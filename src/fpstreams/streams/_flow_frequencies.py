"""Frequency counting across retained, NumPy and Python iterator paths."""

from __future__ import annotations

from collections.abc import Callable, Generator, Iterable, Iterator
from contextlib import AbstractContextManager, contextmanager
from typing import Any, Protocol, cast

from ..physical.plan import (
    BackendPayload,
    PhysicalPlan,
)
from ..planning.logical import Pipeline
from ..runtime.iterators import closing_iterators
from ..runtime.report import (
    _record_direct_strategy,
)


class FrequencyHooks(Protocol):
    """Keep terminal iterator selection visible at its original owner."""

    _open_python_pipeline_values: Callable[[Pipeline], AbstractContextManager[Iterator[Any]]]
    _open_terminal_values: Callable[
        [PhysicalPlan | None, Pipeline | None], AbstractContextManager[Iterator[Any]]
    ]
    execute_physical: Callable[[PhysicalPlan], Iterator[Any]]
    has_active_failpoints: Callable[[], bool]


@contextmanager
def _open_frequency_values(
    physical: PhysicalPlan,
    pipeline: Pipeline | None,
    *,
    direct_range: bool,
    has_key: bool,
    hooks: FrequencyHooks,
) -> Generator[Iterator[Any], None, None]:
    """Preserve key callbacks before reading later inputs in automatic linear plans."""
    from ..runtime.failpoints import has_active_failpoints

    if (
        has_key
        and pipeline is not None
        and pipeline.engine == "auto"
        and physical.root is None
        and physical.parallel is None
    ):
        with hooks._open_python_pipeline_values(pipeline) as iterator:
            _record_direct_strategy(
                physical,
                "python_frequency",
                "Python frequency counting preserves key callbacks before later input reads",
            )
            yield iterator
        return

    payload = physical.backend_payload
    python_selected = (
        physical.root is None
        and physical.parallel is None
        and pipeline is not None
        and isinstance(payload, BackendPayload)
        and payload.arrow_prefix is None
        and payload.native_decision is not None
        and payload.native_decision.engine == "python"
    )
    instrumented_auto = (
        has_active_failpoints() and pipeline is not None and pipeline.engine != "native"
    )
    if direct_range or python_selected or instrumented_auto:
        with hooks._open_terminal_values(physical, pipeline) as iterator:
            yield iterator
        return

    iterator = hooks.execute_physical(physical)
    with closing_iterators((iterator,)):
        yield iterator


def _update_identity_frequency_counts(
    values: Iterable[Any],
    counts: dict[Any, int],
) -> dict[Any, int]:
    get_count = counts.get
    for selected in values:
        try:
            count = get_count(selected, 0)
        except TypeError:
            raise TypeError("reduce_by() keys must be hashable") from None
        counts[selected] = count + 1
    return counts


def _try_native_identity_frequencies(
    pipeline: Pipeline | None,
    physical: PhysicalPlan | None = None,
    *,
    hooks: FrequencyHooks,
) -> tuple[bool, dict[Any, int] | None]:
    """Count retained builtin keys without observing custom key protocols."""
    if (
        pipeline is None
        or pipeline.engine == "python"
        or pipeline.operations
        or hooks.has_active_failpoints()
        or type(source := pipeline.source.native_data) not in (list, tuple)
    ):
        return False, None
    try:
        from .. import _native
    except ImportError:
        return False, None

    kernel = getattr(_native, "frequencies_i64_exact_v1", None)
    if kernel is None:
        return False, None
    result = kernel(source)
    if result is None:
        return False, None
    if type(result) is dict:
        _record_direct_strategy(
            physical, "rust_direct", "native integer frequency counting completed"
        )
        return True, result
    counts, remainder = cast(tuple[dict[Any, int], Iterator[Any]], result)
    continue_native = getattr(_native, "frequencies_exact_prefix_v1", None)
    if continue_native is not None:
        outcome = continue_native(counts, remainder)
        if outcome is not None:
            boundary, exhausted = outcome
            if exhausted:
                _record_direct_strategy(
                    physical,
                    "rust_direct",
                    "native frequency counting completed for exact builtin keys",
                )
                return True, counts
            remainder = _prepend_frequency_boundary(boundary, remainder)
            del boundary, outcome
    counts = _update_identity_frequency_counts(remainder, counts)
    _record_direct_strategy(
        physical, "python_frequency", "Python counting completed after the native frequency prefix"
    )
    return True, counts


def _prepend_frequency_boundary(first: Any, remainder: Iterator[Any]) -> Iterator[Any]:
    """Pass a native boundary to Python once, retaining normal loop-variable lifetimes."""
    yield first
    del first
    yield from remainder


def _try_numpy_frequency_iterator(
    iterator: Iterator[Any],
    physical: PhysicalPlan,
    pipeline: Pipeline | None,
    *,
    hooks: FrequencyHooks,
) -> dict[Any, int] | None:
    """Count a selected NumPy stream through its existing runtime-owned generator."""
    payload = physical.backend_payload
    if (
        pipeline is None
        or pipeline.engine != "auto"
        or pipeline.operations
        or physical.root is not None
        or physical.parallel is not None
        or not isinstance(payload, BackendPayload)
        or payload.arrow_prefix is not None
        or payload.native_decision is None
        or payload.native_decision.engine != "native"
        or hooks.has_active_failpoints()
    ):
        return None
    from ..tabular.numpy import NumpyColumnSource

    if type(pipeline.source.native_data) is not NumpyColumnSource:
        return None
    try:
        from .. import _native
    except ImportError:
        return None
    kernel = getattr(_native, "frequencies_iter_prefix_v1", None)
    if kernel is None or (outcome := kernel(iterator)) is None:
        return None
    counts, boundary, exhausted = outcome
    if exhausted:
        _record_direct_strategy(
            physical,
            "rust_direct",
            "native frequency counting consumed the selected NumPy iterator",
        )
        return cast(dict[Any, int], counts)
    remainder = _prepend_frequency_boundary(boundary, iterator)
    del boundary, outcome
    counts = _update_identity_frequency_counts(remainder, counts)
    _record_direct_strategy(
        physical, "python_frequency", "Python counting completed after the native iterator prefix"
    )
    return counts
