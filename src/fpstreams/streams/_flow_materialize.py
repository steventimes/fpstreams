"""Direct Python and columnar materialization with source-owned cleanup."""

from __future__ import annotations

import builtins
from collections.abc import Callable, Generator, Iterable, Iterator
from contextlib import AbstractContextManager, contextmanager
from typing import Any, Protocol, cast

from ..execution.physical import operations_from_physical_nodes
from ..physical.plan import (
    BackendPayload,
    PhysicalPlan,
    SortPhysicalNode,
    SortStrategy,
)
from ..planning.logical import Pipeline
from ..planning.source import (
    _CANONICAL_RETAINED_SEQUENCE,
    _CANONICAL_SOURCE_OPEN,
    _CANONICAL_SOURCE_OPEN_NATIVE,
    Source,
)
from ..planning.sync import (
    Operation,
    ParallelMapOp,
    SortOp,
)
from ..runtime import failpoints as _failpoints
from ..runtime.iterators import close_iterators, closing_iterators
from ..runtime.query import QueryRuntime
from ..runtime.report import (
    _record_direct_strategy,
)
from . import _flow_structural_list


class MaterializeHooks(Protocol):
    """Share live failpoint and source-iterator proofs with terminal dispatch."""

    open_operations: Callable[..., AbstractContextManager[Iterator[Any]]]
    _CANONICAL_HAS_ACTIVE_FAILPOINTS: Callable[[], bool]
    _CANONICAL_HAS_ACTIVE_FAILPOINTS_CODE: Any
    _READ_FUNCTION_CODE: Callable[[Any], Any]
    has_active_failpoints: Callable[[], bool]
    _iterator_starts_at_retained_sequence: Callable[
        [Iterator[Any], list[Any] | tuple[Any, ...] | range], bool
    ]


_BUILTIN_LIST: type[list[Any]] = builtins.list
_BUILTIN_TUPLE: type[tuple[Any, ...]] = builtins.tuple
_BUILTIN_SET: type[set[Any]] = builtins.set
_BUILTIN_SORTED = builtins.sorted
_SCALAR_FUSION_MIN_ROWS = 4_096
_NATIVE_IDENTITY_SORT_MIN_ROWS = 32_768


def _try_direct_python_materialize(
    physical: PhysicalPlan,
    pipeline: Pipeline | None,
    target: str,
    materializer: Callable[[Iterable[Any]], Any],
    *,
    hooks: MaterializeHooks,
) -> tuple[bool, Any | None]:
    """Drain one selected linear Python plan without executor forwarding."""
    payload = physical.backend_payload
    python_selected = payload is None or (
        isinstance(payload, BackendPayload)
        and payload.arrow_prefix is None
        and payload.native_decision is not None
        and payload.native_decision.engine == "python"
    )
    if (
        physical.root is not None
        or physical.parallel is not None
        or pipeline is None
        or _failpoints.has_active_failpoints is not hooks._CANONICAL_HAS_ACTIVE_FAILPOINTS
        or hooks._READ_FUNCTION_CODE(hooks._CANONICAL_HAS_ACTIVE_FAILPOINTS)
        is not hooks._CANONICAL_HAS_ACTIVE_FAILPOINTS_CODE
        or hooks._CANONICAL_HAS_ACTIVE_FAILPOINTS()
        or not python_selected
        or not _canonical_python_materializer(target, materializer)
    ):
        return False, None

    retained_source = _flow_structural_list._retained_sequence(pipeline.source)
    if retained_source is not None:
        scalar_loop = _direct_python_scalar_loop(physical, pipeline, retained_source)
        if scalar_loop is not None:
            with _open_direct_python_scalar_values(pipeline, scalar_loop) as iterator:
                return True, materializer(iterator)
    elif target not in ("list", "tuple"):
        return False, None

    operations = operations_from_physical_nodes(physical.nodes)
    # An opaque identity iterator may expose a user-defined __length_hint__ that the
    # canonical executor's forwarding generators intentionally hide. Every planned
    # operation produces an fpstreams-owned iterator boundary, while exact retained
    # sources have only side-effect-free built-in length hints.
    if not operations and retained_source is None:
        return False, None
    if any(isinstance(operation, ParallelMapOp) for operation in operations):
        return False, None
    if any(
        isinstance(node, SortPhysicalNode) and node.strategy is SortStrategy.ARROW_STABLE
        for node in physical.nodes
    ):
        return False, None

    with _open_direct_python_materialize_values(pipeline, operations, hooks=hooks) as iterator:
        return True, materializer(iterator)


@contextmanager
def _open_direct_python_materialize_values(
    pipeline: Pipeline,
    operations: tuple[Operation, ...],
    *,
    hooks: MaterializeHooks,
) -> Generator[Iterator[Any], None, None]:
    """Own one real source open and its canonical linear iterator chain."""
    runtime = QueryRuntime()
    active_error: BaseException | None = None
    try:
        source_iterator = pipeline.source.open()
        with hooks.open_operations(
            source_iterator,
            operations,
            runtime=runtime,
            fuse_callable_map_filter=True,
        ) as iterator:
            yield iterator
    except BaseException as error:
        active_error = error
        runtime.close(None if isinstance(error, GeneratorExit) else error)
        raise
    finally:
        if active_error is None:
            runtime.close()


def _canonical_python_materializer(
    target: str,
    materializer: Callable[[Iterable[Any]], Any],
) -> bool:
    """Require the exact constructor whose eager source ownership the fast path models."""
    if target == "list":
        return materializer is _BUILTIN_LIST
    if target == "tuple":
        return materializer is _BUILTIN_TUPLE
    if target == "set":
        return materializer is _BUILTIN_SET
    raise RuntimeError(f"unknown materialization target {target!r}")


def _direct_python_scalar_loop(
    physical: PhysicalPlan,
    pipeline: Pipeline | None,
    retained_source: object | None,
) -> Callable[[Iterator[Any]], Iterator[Any]] | None:
    """Return a bounded scalar loop only for a Python-selected container query."""
    from ..runtime.failpoints import has_active_failpoints

    exact_size = None if pipeline is None else pipeline.source.current_exact_size()
    python_selected = pipeline is not None and (
        pipeline.engine == "python"
        or (pipeline.engine == "auto" and physical.decision.selected_engine == "python")
    )
    if (
        physical.root is not None
        or pipeline is None
        or not python_selected
        or has_active_failpoints()
        or retained_source is None
        or exact_size is None
        or exact_size < _SCALAR_FUSION_MIN_ROWS
    ):
        return None
    from ..execution._scalar_fusion import compile_scalar_fusion

    return compile_scalar_fusion(physical.nodes)


@contextmanager
def _open_direct_python_scalar_values(
    pipeline: Pipeline,
    scalar_loop: Callable[[Iterator[Any]], Iterator[Any]],
) -> Generator[Iterator[Any], None, None]:
    """Open and close the exact-container source around one generated scalar loop."""
    with QueryRuntime():
        source_iterator = pipeline.source.open()
        iterator = scalar_loop(source_iterator)
        with closing_iterators((iterator, source_iterator)):
            yield iterator


def _try_direct_python_scalar_sum(
    physical: PhysicalPlan,
    pipeline: Pipeline | None,
    start: Any,
) -> tuple[bool, Any | None]:
    """Reduce one large closed scalar Python plan without per-stage callbacks."""
    retained_source = (
        None if pipeline is None else _flow_structural_list._retained_sequence(pipeline.source)
    )
    scalar_loop = _direct_python_scalar_loop(physical, pipeline, retained_source)
    if scalar_loop is None or pipeline is None:
        return False, None
    with _open_direct_python_scalar_values(pipeline, scalar_loop) as iterator:
        return True, builtins.sum(iterator, start)


def _try_direct_numpy_list(
    owner: Any,
    physical: PhysicalPlan,
    pipeline: Pipeline | None,
) -> tuple[bool, list[Any] | None]:
    """Collect exact identity NumPy rows without executor generator forwarding."""
    from .flow import Flow

    if (
        type(owner) is not Flow
        or physical.root is not None
        or physical.parallel is not None
        or pipeline is None
        or pipeline.engine != "auto"
        or pipeline.parallel is not None
        or pipeline.operations
        or type(pipeline.source) is not Source
        or Source.__dict__.get("open") is not _CANONICAL_SOURCE_OPEN
        or Source.__dict__.get("open_native") is not _CANONICAL_SOURCE_OPEN_NATIVE
    ):
        return False, None

    from ..runtime.failpoints import hit
    from ..tabular.numpy import (
        NumpyRowSource,
        guarded_numpy_identity_source,
        numpy_identity_rows,
    )

    descriptor = guarded_numpy_identity_source(pipeline.source)
    if descriptor is None or not 1 <= len(descriptor.columns) <= 8:
        return False, None

    opened = pipeline.source.open_native(NumpyRowSource)
    hit("source.open.after")
    result = numpy_identity_rows(opened)
    _record_direct_strategy(
        physical,
        "numpy_direct",
        "identity NumPy rows were collected at their retained source boundary",
    )
    return True, result


def _try_direct_arrow_list(
    physical: PhysicalPlan,
    pipeline: Pipeline | None,
) -> tuple[bool, list[Any] | None]:
    """Collect a replayable identity Arrow source without per-row generator forwarding."""
    from ..runtime.failpoints import has_active_failpoints, hit

    if (
        physical.root is not None
        or pipeline is None
        or pipeline.engine != "auto"
        or pipeline.parallel is not None
        or pipeline.operations
    ):
        return False, None

    from ..planning.arrow_source import ArrowBatchSource
    from ..tabular import arrow as arrow_adapter

    descriptor = pipeline.source.native_data
    if not isinstance(descriptor, ArrowBatchSource) or descriptor.kind not in {
        "table",
        "record_batch",
    }:
        return False, None
    if has_active_failpoints():
        return False, None

    pipeline.source.open_native(ArrowBatchSource)
    hit("source.open.after")
    batches = descriptor.open_batches()
    result: list[Any] = []
    try:
        for batch in batches:
            result.extend(arrow_adapter.batch_to_rows(batch))
    finally:
        arrow_adapter._close(batches)
    return True, result


def _materialize_opened_python_sort(
    source_iterator: Iterator[Any],
    operation: SortOp,
    *,
    hooks: MaterializeHooks,
) -> list[Any]:
    """Finish one already-opened source through the canonical sort boundary."""
    with (
        QueryRuntime() as runtime,
        hooks.open_operations(source_iterator, (operation,), runtime=runtime) as iterator,
    ):
        return _BUILTIN_LIST(iterator)


def _try_direct_sort_list(
    physical: PhysicalPlan,
    pipeline: Pipeline | None,
    *,
    hooks: MaterializeHooks,
) -> tuple[bool, list[Any] | None]:
    """Materialize one planner-selected direct Arrow or retained Python sort."""
    if len(physical.nodes) != 1 or not isinstance(node := physical.nodes[0], SortPhysicalNode):
        return False, None
    if node.strategy is SortStrategy.ARROW_STABLE:
        from ..execution.arrow import materialize_retained_arrow_stable_sort

        result = materialize_retained_arrow_stable_sort(physical)
        return result is not None, result
    operation = node.operation
    payload = physical.backend_payload
    python_selected = isinstance(payload, BackendPayload) and (
        payload.arrow_prefix is None
        and payload.native_decision is not None
        and payload.native_decision.engine == "python"
    )
    if (
        pipeline is None
        or node.strategy is not SortStrategy.IN_MEMORY
        or type(operation) is not SortOp
        or physical.parallel is not None
        or physical.engine != "auto"
        or pipeline.engine != "auto"
        or physical.source is not pipeline.source
        or type(pipeline.source) is not Source
        or Source.__dict__.get("open") is not _CANONICAL_SOURCE_OPEN
        or Source.__dict__.get("retained_sequence") is not _CANONICAL_RETAINED_SEQUENCE
        or len(pipeline.operations) != 1
        or pipeline.operations[0] is not operation
        or operation.key is not None
        or operation.buffer_size is not None
        or not python_selected
        or hooks.has_active_failpoints()
        or builtins.sorted is not _BUILTIN_SORTED
    ):
        return False, None
    retained = pipeline.source.retained_sequence()
    if type(retained) not in (list, tuple):
        return False, None
    source = cast(list[Any] | tuple[Any, ...], retained)
    if len(source) < _NATIVE_IDENTITY_SORT_MIN_ROWS or type(operation.reverse) is not bool:
        return False, None
    try:
        source_iterator = pipeline.source.open()
    except StopIteration as error:
        raise RuntimeError("generator raised StopIteration") from error

    try:
        from fpstreams import _native
    except ImportError:
        return True, _materialize_opened_python_sort(source_iterator, operation, hooks=hooks)
    native_sort = getattr(_native, "sort_i64_exact_sequence_v1", None)
    if (
        not callable(native_sort)
        or hooks.has_active_failpoints()
        or builtins.sorted is not _BUILTIN_SORTED
        or Source.__dict__.get("open") is not _CANONICAL_SOURCE_OPEN
        or Source.__dict__.get("retained_sequence") is not _CANONICAL_RETAINED_SEQUENCE
        or pipeline.source.retained_sequence() is not source
        or not hooks._iterator_starts_at_retained_sequence(source_iterator, source)
    ):
        result = _materialize_opened_python_sort(source_iterator, operation, hooks=hooks)
        _record_direct_strategy(
            physical,
            "python_direct",
            "the opened sort source continued through canonical Python execution",
        )
        return True, result

    try:
        result = native_sort(source, operation.reverse)
    except BaseException as error:
        close_iterators((source_iterator,), active_error=error)
        raise
    if result is None:
        result = _materialize_opened_python_sort(source_iterator, operation, hooks=hooks)
        _record_direct_strategy(
            physical,
            "python_direct",
            "the adaptive integer sort retained canonical Python execution",
        )
        return True, result
    close_iterators((source_iterator,))
    _record_direct_strategy(
        physical,
        "rust_direct",
        "retained exact integers were stably sorted by the Rust direct sink",
    )
    return True, cast(list[Any], result)
