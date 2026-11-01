"""Numeric terminal execution and aggregation compatibility paths."""

from __future__ import annotations

import builtins
from collections.abc import Callable, Generator, Iterable, Iterator
from contextlib import AbstractContextManager, contextmanager
from typing import Any, Protocol, cast

from ...collecting.aggregate_program import (
    AggregationProgram,
    NativeAggregateField,
    NativeAggregateMask,
)
from ...collecting.aggregation import AggregationItems, finish_native_aggregations
from ...collecting.program import run_collector_program
from ...errors import EmptyFlowError, NativeUnsupportedError
from ...execution import execute
from ...physical.plan import BackendPayload, PhysicalPlan
from ...planning.logical import Pipeline
from ...planning.native import EngineDecision
from ...planning.source import Source
from ...runtime._distinct import reraise_key_type_error
from ...runtime.iterators import close_iterators, closing_iterators
from ...runtime.report import _record_direct_strategy


class NumericHooks(Protocol):
    """Read live terminal bindings without taking snapshots across callbacks."""

    open_operations: Callable[..., AbstractContextManager[Iterator[Any]]]
    _LIST_ITERATOR_TYPE: type[Iterator[Any]]
    _TUPLE_ITERATOR_TYPE: type[Iterator[Any]]
    _RANGE_ITERATOR_TYPE: type[Iterator[Any]]
    _LONG_RANGE_ITERATOR_TYPE: type[Iterator[Any]]
    _LIST_ITERATOR_LENGTH_HINT: Callable[[Iterator[Any]], int]
    _TUPLE_ITERATOR_LENGTH_HINT: Callable[[Iterator[Any]], int]
    _RANGE_ITERATOR_LENGTH_HINT: Callable[[Iterator[Any]], int]
    _LONG_RANGE_ITERATOR_LENGTH_HINT: Callable[[Iterator[Any]], int]
    _NATIVE_ITERATOR_MEAN_MIN_ROWS: int
    _auto_mean_fastpath_is_safe: Callable[[], bool]
    _canonical_mean_source_boundaries_are_live: Callable[[], bool]
    _iterator_mean_continuation_is_safe: Callable[[], bool]
    _CANONICAL_MEAN_NATIVE_BINDINGS: Any
    _CANONICAL_COMPENSATED_MEAN: Any
    _CANONICAL_COMPENSATED_MEAN_CODE: Any
    _CANONICAL_MEAN_ISFINITE: Any
    _continue_compensated_mean: Callable[..., float | None]
    compensated_mean: Callable[[Iterable[Any]], float | None]
    try_native_mean: Callable[..., tuple[bool, float | None]]
    try_native_statistics: Callable[..., tuple[bool, Any]]
    has_active_failpoints: Callable[[], bool]
    _iterator_starts_at_retained_sequence: Callable[
        [Iterator[Any], list[Any] | tuple[Any, ...] | range], bool
    ]


_BUILTIN_TYPE = type


def _try_direct_numeric_iterator_mean(
    pipeline: Pipeline,
    iterator: Iterator[Any],
    *,
    hooks: NumericHooks,
) -> tuple[bool, float | None, bool]:
    """Consume a callback-free exact prefix, then resume custom values in Python."""
    iterator_type = _BUILTIN_TYPE(iterator)
    try:
        if iterator_type is hooks._LIST_ITERATOR_TYPE:
            estimated_rows = hooks._LIST_ITERATOR_LENGTH_HINT(iterator)
        elif iterator_type is hooks._TUPLE_ITERATOR_TYPE:
            estimated_rows = hooks._TUPLE_ITERATOR_LENGTH_HINT(iterator)
        elif iterator_type is hooks._RANGE_ITERATOR_TYPE:
            estimated_rows = hooks._RANGE_ITERATOR_LENGTH_HINT(iterator)
        elif iterator_type is hooks._LONG_RANGE_ITERATOR_TYPE:
            estimated_rows = hooks._LONG_RANGE_ITERATOR_LENGTH_HINT(iterator)
        else:
            return False, None, False
    except OverflowError:
        return False, None, False
    if (
        estimated_rows < hooks._NATIVE_ITERATOR_MEAN_MIN_ROWS
        or pipeline.engine != "auto"
        or hooks.has_active_failpoints()
        or not hooks._auto_mean_fastpath_is_safe()
    ):
        return False, None, False
    from ...execution.native import execute_numeric_iterator_mean

    outcome = execute_numeric_iterator_mean(
        iterator,
        hooks._CANONICAL_MEAN_NATIVE_BINDINGS,
        hooks._CANONICAL_COMPENSATED_MEAN,
        hooks._CANONICAL_COMPENSATED_MEAN_CODE,
        hooks._iterator_mean_continuation_is_safe,
    )
    if outcome is None:
        return False, None, False
    status, count, total, compensation, boundary = outcome
    if status == 0:
        return True, (total + compensation) / count if count else None, False
    if status == 3 and count == 0:
        return False, None, False
    if status == 2:
        if boundary is None:
            raise RuntimeError("native iterator mean omitted its Python boundary")
        boundary_holder = [boundary]
        del outcome, boundary
        return (
            True,
            hooks._continue_compensated_mean(
                iterator,
                count,
                total,
                compensation,
                hooks._CANONICAL_MEAN_ISFINITE,
                boundary_holder,
            ),
            True,
        )
    if status == 3:
        return (
            True,
            hooks._continue_compensated_mean(
                iterator,
                count,
                total,
                compensation,
                hooks._CANONICAL_MEAN_ISFINITE,
            ),
            True,
        )
    raise RuntimeError(f"native iterator mean returned unknown status {status}")


def _run_opened_auto_numeric_mean(
    pipeline: Pipeline,
    native_decision: EngineDecision | None,
    arrow_prefix: Any | None = None,
    *,
    hooks: NumericHooks,
) -> tuple[float | None, str, str]:
    """Open the live source once, then use Rust only when that iterator proves retained data."""
    source_iterator = pipeline.source.open()
    native_safe = (
        hooks._canonical_mean_source_boundaries_are_live() and not hooks.has_active_failpoints()
    )
    if native_safe and type(pipeline.source) is Source:
        retained = pipeline.source.retained_sequence()
        if type(retained) in (list, tuple, range):
            retained_sequence = cast(list[Any] | tuple[Any, ...] | range, retained)
        else:
            retained_sequence = None
        retained_iterator_matches = (
            retained_sequence is not None
            and hooks._iterator_starts_at_retained_sequence(source_iterator, retained_sequence)
        )
        from ...tabular.numpy import guarded_numpy_f64_column, guarded_numpy_i64_column

        native_kind = (
            native_decision.program.kind
            if native_decision is not None and native_decision.program is not None
            else None
        )
        numpy_source_matches = retained_sequence is None and (
            (native_kind == "i64" and guarded_numpy_i64_column(pipeline.source) is not None)
            or (native_kind == "f64" and guarded_numpy_f64_column(pipeline.source) is not None)
        )
        if (
            (retained_iterator_matches or numpy_source_matches)
            and not hooks.has_active_failpoints()
            and hooks._auto_mean_fastpath_is_safe()
        ):
            try:
                native, result = hooks.try_native_mean(pipeline, decision=native_decision)
            except BaseException as error:
                close_iterators((source_iterator,), active_error=error)
                raise
            if native:
                close_iterators((source_iterator,))
                return (
                    result,
                    "rust_direct",
                    "the live source opened before its proven numeric storage was reduced in Rust",
                )
        if arrow_prefix is not None:
            from ...execution.arrow import try_arrow_numeric_field_mean
            from ...tabular.arrow import guarded_arrow_mean_source

            if (
                guarded_arrow_mean_source(pipeline.source) is not None
                and not hooks.has_active_failpoints()
                and hooks._auto_mean_fastpath_is_safe()
            ):
                try:
                    handled, result = try_arrow_numeric_field_mean(pipeline, arrow_prefix)
                except BaseException as error:
                    close_iterators((source_iterator,), active_error=error)
                    raise
                if handled:
                    close_iterators((source_iterator,))
                    return (
                        result,
                        "arrow_direct",
                        "the live source opened before a compensated Arrow column reduction",
                    )

    with hooks.open_operations(source_iterator, pipeline.operations) as iterator:
        handled, result, hybrid = _try_direct_numeric_iterator_mean(pipeline, iterator, hooks=hooks)
        if handled:
            return (
                result,
                "rust_python_hybrid" if hybrid else "rust_direct",
                (
                    "a callback-free numeric prefix continued through the canonical Python "
                    "compensated mean loop"
                    if hybrid
                    else "the opened numeric iterator was reduced by a callback-free "
                    "compensated Rust loop"
                ),
            )
        return (
            hooks.compensated_mean(iterator),
            "python_direct",
            "the live opened source required the canonical Python compensated mean loop",
        )


def _sum_python_pipeline(pipeline: Pipeline, start: Any, *, hooks: NumericHooks) -> Any:
    """Reduce a Python iterator chain in CPython's C loop under query-scoped ownership."""
    with hooks.open_operations(
        pipeline.source.open(),
        pipeline.operations,
        fuse_callable_map_filter=True,
    ) as iterator:
        return builtins.sum(iterator, start)


def _run_statistics_aggregations(
    pipeline: Pipeline,
    items: AggregationItems,
    program: AggregationProgram,
    native_decision: EngineDecision | None,
    *,
    hooks: NumericHooks,
) -> dict[str, Any]:
    """Run one statistics snapshot or enter the canonical collectors once."""
    native, statistics = hooks.try_native_statistics(pipeline, decision=native_decision)
    if native:
        if statistics is None:
            raise RuntimeError("native statistics result is missing")
        count, mean, squared_deviations = statistics
        return finish_native_aggregations(
            items,
            (
                count,
                0,
                None,
                None,
                None,
                None,
                mean,
                squared_deviations,
            ),
        )
    # A rejected or failed statistics attempt may already have touched
    # conversion protocols. Do not observe user state through another ABI.
    return run_collector_program(execute(pipeline, auto_native=False), program.collectors)


def _run_mean_only_aggregations(
    physical: PhysicalPlan,
    pipeline: Pipeline,
    items: AggregationItems,
    program: AggregationProgram,
    native_decision: EngineDecision | None,
    *,
    hooks: NumericHooks,
) -> dict[str, Any]:
    """Run repeated mean aggregators without bypassing automatic source-open semantics."""
    if pipeline.engine == "auto" and physical.parallel is None and pipeline.parallel is None:
        payload = physical.backend_payload
        arrow_prefix = payload.arrow_prefix if isinstance(payload, BackendPayload) else None
        result, strategy, reason = _run_opened_auto_numeric_mean(
            pipeline,
            native_decision,
            arrow_prefix,
            hooks=hooks,
        )
        _record_direct_strategy(physical, strategy, reason)
        return {name: result for name, _aggregation in items}
    if pipeline.engine == "native":
        native, result = hooks.try_native_mean(pipeline, decision=native_decision)
        if native:
            return {name: result for name, _aggregation in items}
    return run_collector_program(execute(pipeline, auto_native=False), program.collectors)


def _requires_sequential_f64_total(
    mask: NativeAggregateMask | None,
    decision: EngineDecision | None,
) -> bool:
    """Return whether an aggregate plan depends on the versioned float-total ABI."""
    return (
        mask is not None
        and NativeAggregateField.TOTAL in mask.fields
        and decision is not None
        and decision.program is not None
        and decision.program.kind == "f64"
    )


def _f64_total_compatibility_fallback(
    pipeline: Pipeline,
    program: AggregationProgram,
    decision: EngineDecision | None,
) -> dict[str, Any] | None:
    """Fall back or reject when an older wheel cannot preserve aggregate sum order."""
    if not _requires_sequential_f64_total(program.native_mask, decision):
        return None
    from ...execution.native import sequential_f64_aggregate_total_available

    if sequential_f64_aggregate_total_available():
        return None
    reason = "native extension lacks sequential f64 aggregate total support"
    if pipeline.engine == "native":
        raise NativeUnsupportedError(reason)
    return run_collector_program(execute(pipeline, auto_native=False), program.collectors)


def _minmax_python_values(iterator: Iterator[Any]) -> tuple[Any, Any]:
    """Compare keyless values directly while their caller retains iterator ownership."""
    try:
        minimum = maximum = next(iterator)
    except StopIteration:
        raise EmptyFlowError("minmax() requires at least one item") from None
    for item in iterator:
        if item < minimum:
            minimum = item
        if item > maximum:
            maximum = item
    return minimum, maximum


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
    from ...runtime.failpoints import has_active_failpoints

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
            reraise_key_type_error(selected, "reduce_by() keys must be hashable")
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
        from fpstreams import _native
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
    from ...tabular.numpy import NumpyColumnSource

    if type(pipeline.source.native_data) is not NumpyColumnSource:
        return None
    try:
        from fpstreams import _native
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
