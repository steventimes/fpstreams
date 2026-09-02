"""Terminal operations shared by synchronous Flow instances."""

from __future__ import annotations

import builtins
import csv
import dis as _dis
import heapq
import json
import math
import operator
import os
from collections import Counter, deque
from collections.abc import Callable, Generator, Iterable, Iterator, Mapping
from contextlib import contextmanager
from numbers import Real
from time import perf_counter_ns
from typing import Any, Generic, TypeVar, cast

from ..collecting.aggregate_program import (
    AggregationProgram,
    NativeAggregateField,
    NativeAggregateMask,
    compile_aggregations,
    native_mean_only,
)
from ..collecting.aggregation import (
    AggregationItems,
    Aggregator,
    finish_native_aggregations,
    prepare_aggregations,
)
from ..collecting.collector import Collector, prepare_collectors, run_collectors
from ..collecting.program import run_collector_program
from ..collecting.statistics import (
    OnlineStatistics,
    StatisticsSnapshot,
    _continue_compensated_mean,
    compensated_mean,
    std_from,
    validate_ddof,
    variance_from,
)
from ..errors import BufferLimitError, EmptyFlowError, NativeUnsupportedError
from ..execution import (
    exact_count,
    execute,
    try_native_aggregate,
    try_native_materialize,
    try_native_mean,
    try_native_statistics,
    try_native_terminal,
)
from ..execution.physical import execute_physical, operations_from_physical_nodes
from ..execution.sync import open_operations
from ..expressions.scalar import Expr, FExpr
from ..expressions.selectors import Selector, compile_selector
from ..io_safety import spreadsheet_safe_cell
from ..physical.plan import (
    BackendPayload,
    PhysicalPlan,
    SortPhysicalNode,
    SortStrategy,
)
from ..planning.compiler import compile_query
from ..planning.logical import Pipeline, Query, SourceNode, linear_pipeline
from ..planning.native import _AUTO_THRESHOLD as _NATIVE_AUTO_THRESHOLD
from ..planning.native import EngineDecision, TerminalName
from ..planning.source import (
    _CANONICAL_RETAINED_SEQUENCE,
    _CANONICAL_SOURCE_CLAIM,
    _CANONICAL_SOURCE_CLAIM_CODE,
    _CANONICAL_SOURCE_CURRENT_EXACT_SIZE,
    _CANONICAL_SOURCE_CURRENT_EXACT_SIZE_CODE,
    _CANONICAL_SOURCE_NATIVE_DATA,
    _CANONICAL_SOURCE_OPEN,
    _CANONICAL_SOURCE_OPEN_CODE,
    _CANONICAL_SOURCE_OPEN_NATIVE,
    _CANONICAL_SOURCE_OPEN_NATIVE_CODE,
    Source,
)
from ..planning.source import (
    _function_code as _function_code_no_audit,
)
from ..planning.sync import (
    DropOp,
    Operation,
    ParallelMapOp,
    SortOp,
    TakeOp,
)
from ..primitives.result import Err, Ok
from ..runtime import failpoints as _failpoints
from ..runtime.failpoints import has_active_failpoints
from ..runtime.iterators import close_iterators, closing_iterators
from ..runtime.query import QueryRuntime
from ..runtime.report import (
    ExecutionResult,
    _record_direct_strategy,
    _record_sync_plan,
    _start_recording,
    _stop_recording,
)
from . import _flow_structural_list

T = TypeVar("T")
R = TypeVar("R")
C = TypeVar("C")
_MISSING = object()
_BUILTIN_LIST: type[list[Any]] = builtins.list
_BUILTIN_INT = builtins.int
_BUILTIN_MAX = builtins.max
_BUILTIN_MIN = builtins.min
_BUILTIN_TUPLE: type[tuple[Any, ...]] = builtins.tuple
_BUILTIN_SET: type[set[Any]] = builtins.set
_BUILTIN_TYPE = builtins.type
_BUILTIN_ITER = builtins.iter
_BUILTIN_SORTED = builtins.sorted
_LIST_ITERATOR_TYPE: type[Iterator[Any]] = type(iter([]))
_TUPLE_ITERATOR_TYPE: type[Iterator[Any]] = type(iter(()))
_RANGE_ITERATOR_TYPE: type[Iterator[Any]] = type(iter(range(0)))
_LONG_RANGE_ITERATOR_TYPE: type[Iterator[Any]] = type(iter(range(1 << 100, (1 << 100) + 1)))
_LIST_ITERATOR_LENGTH_HINT = cast(
    Callable[[Iterator[Any]], int],
    vars(_LIST_ITERATOR_TYPE)["__length_hint__"],
)
_TUPLE_ITERATOR_LENGTH_HINT = cast(
    Callable[[Iterator[Any]], int],
    vars(_TUPLE_ITERATOR_TYPE)["__length_hint__"],
)
_RANGE_ITERATOR_LENGTH_HINT = cast(
    Callable[[Iterator[Any]], int],
    vars(_RANGE_ITERATOR_TYPE)["__length_hint__"],
)
_LONG_RANGE_ITERATOR_LENGTH_HINT = cast(
    Callable[[Iterator[Any]], int],
    vars(_LONG_RANGE_ITERATOR_TYPE)["__length_hint__"],
)


def _resolve_load_global(
    global_namespace: dict[str, Any],
    builtin_namespace: dict[str, Any],
    name: str,
) -> Any:
    """Resolve one function global with CPython's globals-before-builtins order."""
    if name in global_namespace:
        return global_namespace[name]
    return builtin_namespace[name]


_READ_FUNCTION_CODE = cast(Callable[[Any], Any], _function_code_no_audit)
_CANONICAL_COMPILE_QUERY = compile_query
_CANONICAL_COMPILE_QUERY_CODE = compile_query.__code__
_CANONICAL_FAILPOINT_HIT = _failpoints.hit
_CANONICAL_FAILPOINT_HIT_CODE = _READ_FUNCTION_CODE(_failpoints.hit)
_CANONICAL_HAS_ACTIVE_FAILPOINTS = has_active_failpoints
_CANONICAL_HAS_ACTIVE_FAILPOINTS_CODE = _READ_FUNCTION_CODE(has_active_failpoints)
_CANONICAL_TRY_NATIVE_TERMINAL = try_native_terminal
_CANONICAL_TRY_NATIVE_TERMINAL_CODE = try_native_terminal.__code__
_CANONICAL_COMPENSATED_MEAN = compensated_mean
_CANONICAL_COMPENSATED_MEAN_CODE = _READ_FUNCTION_CODE(compensated_mean)
_CANONICAL_CONTINUE_COMPENSATED_MEAN = _continue_compensated_mean
_CANONICAL_CONTINUE_COMPENSATED_MEAN_CODE = _READ_FUNCTION_CODE(_continue_compensated_mean)
_CANONICAL_RETAINED_SEQUENCE_CODE = _READ_FUNCTION_CODE(_CANONICAL_RETAINED_SEQUENCE)
_CANONICAL_SOURCE_FACTORY_IS_PRISTINE = Source._factory_is_pristine
_CANONICAL_SOURCE_FACTORY_IS_PRISTINE_CODE = _READ_FUNCTION_CODE(
    _CANONICAL_SOURCE_FACTORY_IS_PRISTINE
)
_CANONICAL_SOURCE_NATIVE_DATA_GETTER = cast(Any, _CANONICAL_SOURCE_NATIVE_DATA).fget
_CANONICAL_SOURCE_NATIVE_DATA_GETTER_CODE = _READ_FUNCTION_CODE(
    _CANONICAL_SOURCE_NATIVE_DATA_GETTER
)
_CANONICAL_COMPENSATED_MEAN_GLOBALS = compensated_mean.__globals__
_CANONICAL_COMPENSATED_MEAN_BUILTINS = cast(Any, compensated_mean).__builtins__
_CANONICAL_MEAN_LOAD_GLOBAL_NAMES = tuple(
    dict.fromkeys(
        cast(str, instruction.argval)
        for instruction in _dis.get_instructions(_CANONICAL_COMPENSATED_MEAN_CODE)
        if instruction.opname == "LOAD_GLOBAL"
    )
)
_CANONICAL_MEAN_GLOBAL_BINDINGS = tuple(
    (
        name,
        _resolve_load_global(
            _CANONICAL_COMPENSATED_MEAN_GLOBALS,
            _CANONICAL_COMPENSATED_MEAN_BUILTINS,
            name,
        ),
    )
    for name in _CANONICAL_MEAN_LOAD_GLOBAL_NAMES
)
_CANONICAL_MEAN_MATH = cast(
    Any,
    dict(_CANONICAL_MEAN_GLOBAL_BINDINGS)["math"],
)
_CANONICAL_MEAN_MATH_DICT = cast(dict[str, Any], _CANONICAL_MEAN_MATH.__dict__)
_CANONICAL_MEAN_ISFINITE = _CANONICAL_MEAN_MATH_DICT["isfinite"]
_FLOW_TERMINALS_GLOBALS = globals()
_CANONICAL_MEAN_NATIVE_BINDINGS = (
    *(
        (
            _CANONICAL_COMPENSATED_MEAN_GLOBALS,
            _CANONICAL_COMPENSATED_MEAN_BUILTINS,
            name,
            expected,
        )
        for name, expected in _CANONICAL_MEAN_GLOBAL_BINDINGS
    ),
    (
        _CANONICAL_MEAN_MATH_DICT,
        None,
        "isfinite",
        _CANONICAL_MEAN_ISFINITE,
    ),
    (
        _FLOW_TERMINALS_GLOBALS,
        _CANONICAL_COMPENSATED_MEAN_BUILTINS,
        "compensated_mean",
        _CANONICAL_COMPENSATED_MEAN,
    ),
    (
        _FLOW_TERMINALS_GLOBALS,
        _CANONICAL_COMPENSATED_MEAN_BUILTINS,
        "_continue_compensated_mean",
        _CANONICAL_CONTINUE_COMPENSATED_MEAN,
    ),
)
_CANONICAL_MEAN_FUNCTION_BINDINGS = (
    (
        "compensated_mean",
        _CANONICAL_COMPENSATED_MEAN,
        _CANONICAL_COMPENSATED_MEAN_CODE,
    ),
    (
        "_continue_compensated_mean",
        _CANONICAL_CONTINUE_COMPENSATED_MEAN,
        _CANONICAL_CONTINUE_COMPENSATED_MEAN_CODE,
    ),
)
_CANONICAL_MEAN_SOURCE_METHOD_BINDINGS = (
    ("open", _CANONICAL_SOURCE_OPEN, _CANONICAL_SOURCE_OPEN_CODE),
    (
        "open_native",
        _CANONICAL_SOURCE_OPEN_NATIVE,
        _CANONICAL_SOURCE_OPEN_NATIVE_CODE,
    ),
    ("_claim", _CANONICAL_SOURCE_CLAIM, _CANONICAL_SOURCE_CLAIM_CODE),
    (
        "retained_sequence",
        _CANONICAL_RETAINED_SEQUENCE,
        _CANONICAL_RETAINED_SEQUENCE_CODE,
    ),
    (
        "_factory_is_pristine",
        _CANONICAL_SOURCE_FACTORY_IS_PRISTINE,
        _CANONICAL_SOURCE_FACTORY_IS_PRISTINE_CODE,
    ),
)
_MINMAX_NATIVE_MASK = NativeAggregateMask(
    frozenset({NativeAggregateField.MINIMUM, NativeAggregateField.MAXIMUM})
).bits
_SCALAR_FUSION_MIN_ROWS = 4_096
_NATIVE_IDENTITY_SORT_MIN_ROWS = 32_768
_NATIVE_NUMPY_I64_MIN_ROWS = 4_096
_NATIVE_ITERATOR_MEAN_MIN_ROWS = 128
_I64_MIN = -(1 << 63)
_I64_MAX = (1 << 63) - 1
_REPORTABLE_SYNC_TERMINALS = frozenset(
    {
        "aggregate",
        "all",
        "any",
        "average",
        "bottom",
        "collect",
        "count",
        "count_by",
        "describe",
        "find",
        "find_index",
        "first",
        "fold_by",
        "fold_right",
        "for_each",
        "frequencies",
        "greatest",
        "index_of",
        "join",
        "last",
        "least",
        "max",
        "mean",
        "min",
        "minmax",
        "none",
        "nth",
        "partition",
        "partition_results",
        "reduce",
        "reduce_by",
        "reduce_right",
        "std",
        "sum",
        "summarize",
        "to_csv",
        "to_df",
        "to_json",
        "to_list",
        "to_np",
        "to_numpy",
        "to_pandas",
        "to_set",
        "to_tuple",
        "top",
        "variance",
    }
)


def _is_exact_i64(value: object) -> bool:
    return _BUILTIN_TYPE(value) is _BUILTIN_INT and _I64_MIN <= value <= _I64_MAX


def _compensated_mean_dependencies_are_live(
    _mean: Any = _CANONICAL_COMPENSATED_MEAN,
    _mean_globals: dict[str, Any] = _CANONICAL_COMPENSATED_MEAN_GLOBALS,
    _mean_builtins: dict[str, Any] = _CANONICAL_COMPENSATED_MEAN_BUILTINS,
    _bindings: tuple[tuple[str, object], ...] = _CANONICAL_MEAN_GLOBAL_BINDINGS,
    _math_dict: dict[str, Any] = _CANONICAL_MEAN_MATH_DICT,
    _isfinite: object = _CANONICAL_MEAN_ISFINITE,
    _key_error: type[KeyError] = builtins.KeyError,
) -> bool:
    """Return whether native code may bypass every live global used by the mean loop."""
    if _mean.__globals__ is not _mean_globals or _mean.__builtins__ is not _mean_builtins:
        return False
    try:
        for name, expected in _bindings:
            live = _mean_globals[name] if name in _mean_globals else _mean_builtins[name]
            if live is not expected:
                return False
    except _key_error:
        return False
    return _math_dict.get("isfinite") is _isfinite


def _try_direct_numeric_iterator_mean(
    pipeline: Pipeline,
    iterator: Iterator[Any],
) -> tuple[bool, float | None, bool]:
    """Consume a callback-free exact prefix, then resume custom values in Python."""
    iterator_type = _BUILTIN_TYPE(iterator)
    try:
        if iterator_type is _LIST_ITERATOR_TYPE:
            estimated_rows = _LIST_ITERATOR_LENGTH_HINT(iterator)
        elif iterator_type is _TUPLE_ITERATOR_TYPE:
            estimated_rows = _TUPLE_ITERATOR_LENGTH_HINT(iterator)
        elif iterator_type is _RANGE_ITERATOR_TYPE:
            estimated_rows = _RANGE_ITERATOR_LENGTH_HINT(iterator)
        elif iterator_type is _LONG_RANGE_ITERATOR_TYPE:
            estimated_rows = _LONG_RANGE_ITERATOR_LENGTH_HINT(iterator)
        else:
            return False, None, False
    except OverflowError:
        return False, None, False
    if (
        estimated_rows < _NATIVE_ITERATOR_MEAN_MIN_ROWS
        or pipeline.engine != "auto"
        or has_active_failpoints()
        or not _auto_mean_fastpath_is_safe()
    ):
        return False, None, False
    from ..execution.native import execute_numeric_iterator_mean

    outcome = execute_numeric_iterator_mean(
        iterator,
        _CANONICAL_MEAN_NATIVE_BINDINGS,
        _CANONICAL_COMPENSATED_MEAN,
        _CANONICAL_COMPENSATED_MEAN_CODE,
        _iterator_mean_continuation_is_safe,
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
            _continue_compensated_mean(
                iterator,
                count,
                total,
                compensation,
                _CANONICAL_MEAN_ISFINITE,
                boundary_holder,
            ),
            True,
        )
    if status == 3:
        return (
            True,
            _continue_compensated_mean(
                iterator,
                count,
                total,
                compensation,
                _CANONICAL_MEAN_ISFINITE,
            ),
            True,
        )
    raise RuntimeError(f"native iterator mean returned unknown status {status}")


def _run_opened_auto_numeric_mean(
    pipeline: Pipeline,
    native_decision: EngineDecision | None,
    arrow_prefix: Any | None = None,
) -> tuple[float | None, str, str]:
    """Open the live source once, then use Rust only when that iterator proves retained data."""
    source_iterator = pipeline.source.open()
    native_safe = _canonical_mean_source_boundaries_are_live() and not has_active_failpoints()
    if native_safe and type(pipeline.source) is Source:
        retained = pipeline.source.retained_sequence()
        if type(retained) in (list, tuple, range):
            retained_sequence = cast(list[Any] | tuple[Any, ...] | range, retained)
        else:
            retained_sequence = None
        retained_iterator_matches = (
            retained_sequence is not None
            and _iterator_starts_at_retained_sequence(source_iterator, retained_sequence)
        )
        from ..tabular.numpy import guarded_numpy_f64_column, guarded_numpy_i64_column

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
            and not has_active_failpoints()
            and _auto_mean_fastpath_is_safe()
        ):
            try:
                native, result = try_native_mean(pipeline, decision=native_decision)
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
            from ..execution.arrow import try_arrow_numeric_field_mean
            from ..tabular.arrow import guarded_arrow_mean_source

            if (
                guarded_arrow_mean_source(pipeline.source) is not None
                and not has_active_failpoints()
                and _auto_mean_fastpath_is_safe()
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

    with open_operations(source_iterator, pipeline.operations) as iterator:
        handled, result, hybrid = _try_direct_numeric_iterator_mean(pipeline, iterator)
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
            compensated_mean(iterator),
            "python_direct",
            "the live opened source required the canonical Python compensated mean loop",
        )


def _sum_python_pipeline(pipeline: Pipeline, start: Any) -> Any:
    """Reduce a Python iterator chain in CPython's C loop under query-scoped ownership."""
    with open_operations(
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
) -> dict[str, Any]:
    """Run one statistics snapshot or enter the canonical collectors once."""
    native, statistics = try_native_statistics(pipeline, decision=native_decision)
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
) -> dict[str, Any]:
    """Run repeated mean aggregators without bypassing automatic source-open semantics."""
    if pipeline.engine == "auto" and physical.parallel is None and pipeline.parallel is None:
        payload = physical.backend_payload
        arrow_prefix = payload.arrow_prefix if isinstance(payload, BackendPayload) else None
        result, strategy, reason = _run_opened_auto_numeric_mean(
            pipeline,
            native_decision,
            arrow_prefix,
        )
        _record_direct_strategy(physical, strategy, reason)
        return {name: result for name, _aggregation in items}
    if pipeline.engine == "native":
        native, result = try_native_mean(pipeline, decision=native_decision)
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
    from ..execution.native import sequential_f64_aggregate_total_available

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


@contextmanager
def _open_python_pipeline_values(
    pipeline: Pipeline,
) -> Generator[Iterator[Any], None, None]:
    """Open one linear Python plan inside the canonical generator boundary."""
    with open_operations(pipeline.source.open(), pipeline.operations) as iterator:
        yield iterator


@contextmanager
def _open_terminal_values(
    physical: PhysicalPlan | None, pipeline: Pipeline | None
) -> Generator[Iterator[Any], None, None]:
    """Open one iterator selected by a compiled plan or safe identity shortcut.

    A linear query can enter the compatibility Python loop after its scalar native
    attempt declines. A relational query has no equivalent linear view: it must
    execute the recursive physical root so joins and aggregates are not bypassed.
    """
    if physical is None:
        if pipeline is None:
            raise RuntimeError("direct terminal plan is missing its canonical pipeline")
        with _open_python_pipeline_values(pipeline) as iterator:
            yield iterator
        return

    payload = physical.backend_payload
    if physical.root is not None or (
        isinstance(payload, BackendPayload) and payload.arrow_prefix is not None
    ):
        if isinstance(payload, BackendPayload) and payload.arrow_prefix is not None:
            _record_direct_strategy(
                physical,
                "arrow_direct",
                "the planner executed a proven Arrow prefix without Python row iteration",
            )
        iterator = execute_physical(physical)
    elif pipeline is None:
        raise RuntimeError("linear terminal plan is missing its canonical pipeline")
    elif pipeline.engine != "native":
        # The terminal has already attempted its selected native/Arrow route. Enter the
        # canonical Python operation chain directly instead of forwarding every item through
        # execute()'s generator frame; this matters for built-in reductions over large lists.
        with _open_python_pipeline_values(pipeline) as iterator:
            yield iterator
        return
    else:
        # Forced-native iteration remains an end-to-end contract even when the terminal itself
        # has an opaque callback that cannot be compiled into the scalar kernel.
        iterator = execute(pipeline, auto_native=False)
    with closing_iterators((iterator,)):
        yield iterator


@contextmanager
def _open_frequency_values(
    physical: PhysicalPlan,
    pipeline: Pipeline | None,
    *,
    direct_range: bool,
) -> Generator[Iterator[Any], None, None]:
    """Use a direct Python loop only when the compiled iterate route selected Python."""
    from ..runtime.failpoints import has_active_failpoints

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
        with _open_terminal_values(physical, pipeline) as iterator:
            yield iterator
        return

    iterator = execute_physical(physical)
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
        except KeyError:
            count = 0
        except TypeError:
            raise TypeError("reduce_by() keys must be hashable") from None
        counts[selected] = count + 1
    return counts


def _try_native_identity_frequencies(
    pipeline: Pipeline | None,
) -> tuple[bool, dict[Any, int] | None]:
    """Count retained exact i64 containers without observing custom key protocols."""
    if (
        pipeline is None
        or pipeline.engine == "python"
        or pipeline.operations
        or has_active_failpoints()
        or type(source := pipeline.source.native_data) not in (list, tuple)
    ):
        return False, None
    from .. import _native

    kernel = getattr(_native, "frequencies_i64_exact_v1", None)
    if kernel is None:
        return False, None
    result = kernel(source)
    if result is None:
        return False, None
    if type(result) is dict:
        return True, result
    counts, remainder = cast(tuple[dict[Any, int], Iterator[Any]], result)
    return True, _update_identity_frequency_counts(remainder, counts)


def _try_direct_python_materialize(
    physical: PhysicalPlan,
    pipeline: Pipeline | None,
    target: str,
    materializer: Callable[[Iterable[Any]], Any],
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
        or _failpoints.has_active_failpoints is not _CANONICAL_HAS_ACTIVE_FAILPOINTS
        or _READ_FUNCTION_CODE(_CANONICAL_HAS_ACTIVE_FAILPOINTS)
        is not _CANONICAL_HAS_ACTIVE_FAILPOINTS_CODE
        or _CANONICAL_HAS_ACTIVE_FAILPOINTS()
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

    with _open_direct_python_materialize_values(pipeline, operations) as iterator:
        return True, materializer(iterator)


@contextmanager
def _open_direct_python_materialize_values(
    pipeline: Pipeline,
    operations: tuple[Operation, ...],
) -> Generator[Iterator[Any], None, None]:
    """Own one real source open and its canonical linear iterator chain."""
    runtime = QueryRuntime()
    active_error: BaseException | None = None
    try:
        source_iterator = pipeline.source.open()
        with open_operations(
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


def _canonical_mean_functions_are_live(
    _flow_globals: dict[str, Any] = _FLOW_TERMINALS_GLOBALS,
    _bindings: tuple[tuple[str, Any, Any], ...] = _CANONICAL_MEAN_FUNCTION_BINDINGS,
    _read_code: Callable[[Any], Any] = _READ_FUNCTION_CODE,
) -> bool:
    """Check every Python call boundary bypassed or resumed by native mean chunks."""
    for name, function, code in _bindings:
        if _flow_globals.get(name) is not function or _read_code(function) is not code:
            return False
    return True


def _canonical_mean_source_boundaries_are_live(
    _flow_globals: dict[str, Any] = _FLOW_TERMINALS_GLOBALS,
    _source_type: type[Source[Any]] = Source,
    _source_methods: tuple[tuple[str, Any, Any], ...] = _CANONICAL_MEAN_SOURCE_METHOD_BINDINGS,
    _source_native_data: Any = _CANONICAL_SOURCE_NATIVE_DATA,
    _source_native_data_getter: Any = _CANONICAL_SOURCE_NATIVE_DATA_GETTER,
    _source_native_data_getter_code: Any = _CANONICAL_SOURCE_NATIVE_DATA_GETTER_CODE,
    _failpoint_module: Any = _failpoints,
    _failpoint_hit: Any = _CANONICAL_FAILPOINT_HIT,
    _failpoint_hit_code: Any = _CANONICAL_FAILPOINT_HIT_CODE,
    _active_failpoints: Any = _CANONICAL_HAS_ACTIVE_FAILPOINTS,
    _active_failpoints_code: Any = _CANONICAL_HAS_ACTIVE_FAILPOINTS_CODE,
    _read_code: Callable[[Any], Any] = _READ_FUNCTION_CODE,
) -> bool:
    """Check source opening and failpoint boundaries before an automatic native mean."""
    if (
        _flow_globals.get("Source") is not _source_type
        or _flow_globals.get("has_active_failpoints") is not _active_failpoints
        or _read_code(_active_failpoints) is not _active_failpoints_code
        or _flow_globals.get("_failpoints") is not _failpoint_module
        or cast(Any, _failpoint_module).has_active_failpoints is not _active_failpoints
        or cast(Any, _failpoint_module).hit is not _failpoint_hit
        or _read_code(_failpoint_hit) is not _failpoint_hit_code
    ):
        return False
    namespace = _source_type.__dict__
    if (
        namespace.get("native_data") is not _source_native_data
        or cast(Any, _source_native_data).fget is not _source_native_data_getter
        or _read_code(_source_native_data_getter) is not _source_native_data_getter_code
    ):
        return False
    for name, function, code in _source_methods:
        if namespace.get(name) is not function or _read_code(function) is not code:
            return False
    return True


def _auto_mean_fastpath_is_safe(
    _functions_are_live: Callable[[], bool] = _canonical_mean_functions_are_live,
    _source_boundaries_are_live: Callable[[], bool] = _canonical_mean_source_boundaries_are_live,
    _dependencies_are_live: Callable[[], bool] = _compensated_mean_dependencies_are_live,
) -> bool:
    """Protect every automatic mean shortcut with the canonical Python call boundary."""
    return _functions_are_live() and _source_boundaries_are_live() and _dependencies_are_live()


def _iterator_mean_continuation_is_safe(
    _failpoints_active: Callable[[], bool] = _CANONICAL_HAS_ACTIVE_FAILPOINTS,
    _mean_fastpath_is_safe: Callable[[], bool] = _auto_mean_fastpath_is_safe,
) -> bool:
    """Recheck callbacks and instrumentation before every native iterator chunk."""
    return not _failpoints_active() and _mean_fastpath_is_safe()


def _retained_identity_sequence(
    owner: Any,
    terminal: str,
    *arguments: Any,
) -> list[Any] | tuple[Any, ...] | range | None:
    """Return an exact identity sequence only when direct access is unobservable."""
    from .flow import (
        _CANONICAL_FLOW_DROP,
        _CANONICAL_FLOW_FIRST,
        _CANONICAL_FLOW_LAST,
        _CANONICAL_FLOW_TERMINAL_CONTEXT,
        Flow,
    )

    if type(owner) is not Flow:
        return None
    if terminal == "last" and Flow._terminal_context is not _CANONICAL_FLOW_TERMINAL_CONTEXT:
        return None
    if terminal == "nth":
        position = cast(int, arguments[0])
        if position >= 0 and (
            Flow.drop is not _CANONICAL_FLOW_DROP or Flow.first is not _CANONICAL_FLOW_FIRST
        ):
            return None
        if position == -1 and Flow.last is not _CANONICAL_FLOW_LAST:
            return None

    if (
        has_active_failpoints()
        or Source.__dict__.get("open") is not _CANONICAL_SOURCE_OPEN
        or Source.__dict__.get("retained_sequence") is not _CANONICAL_RETAINED_SEQUENCE
    ):
        return None
    pipeline = owner._uncompiled_python_identity_pipeline(owner._query(terminal, *arguments))
    if pipeline is None or pipeline.parallel is not None or pipeline.operations:
        return None
    retained = pipeline.source.retained_sequence()
    if type(retained) not in (list, tuple, range):
        return None
    return cast(list[Any] | tuple[Any, ...] | range, retained)


def _try_direct_numpy_i64_extreme(
    owner: Any,
    terminal: str,
    key: Callable[[Any], Any] | None,
) -> tuple[bool, Any | None]:
    """Reduce one exact identity i64 ndarray before constructing a physical plan."""
    from .flow import (
        _CANONICAL_FLOW_COMPILED_TERMINAL_CONTEXT,
        _CANONICAL_FLOW_COMPILED_TERMINAL_CONTEXT_CODE,
        _CANONICAL_FLOW_NATIVE_DECISION,
        _CANONICAL_FLOW_NATIVE_DECISION_CODE,
        _CANONICAL_FLOW_QUERY,
        _CANONICAL_FLOW_QUERY_CODE,
        _CANONICAL_FLOW_TERMINAL_CONTEXT,
        _CANONICAL_FLOW_TERMINAL_CONTEXT_CODE,
        Flow,
    )

    if (
        key is not None
        or terminal not in {"min", "max"}
        or type(owner) is not Flow
        or Flow._query is not _CANONICAL_FLOW_QUERY
        or _CANONICAL_FLOW_QUERY.__code__ is not _CANONICAL_FLOW_QUERY_CODE
        or Flow._terminal_context is not _CANONICAL_FLOW_TERMINAL_CONTEXT
        or _CANONICAL_FLOW_TERMINAL_CONTEXT.__code__ is not _CANONICAL_FLOW_TERMINAL_CONTEXT_CODE
        or Flow._compiled_terminal_context is not _CANONICAL_FLOW_COMPILED_TERMINAL_CONTEXT
        or _CANONICAL_FLOW_COMPILED_TERMINAL_CONTEXT.__code__
        is not _CANONICAL_FLOW_COMPILED_TERMINAL_CONTEXT_CODE
        or Flow._native_decision is not _CANONICAL_FLOW_NATIVE_DECISION
        or _CANONICAL_FLOW_NATIVE_DECISION.__code__ is not _CANONICAL_FLOW_NATIVE_DECISION_CODE
        or globals().get("compile_query") is not _CANONICAL_COMPILE_QUERY
        or _CANONICAL_COMPILE_QUERY.__code__ is not _CANONICAL_COMPILE_QUERY_CODE
        or globals().get("has_active_failpoints") is not _CANONICAL_HAS_ACTIVE_FAILPOINTS
        or globals().get("try_native_terminal") is not _CANONICAL_TRY_NATIVE_TERMINAL
        or _CANONICAL_TRY_NATIVE_TERMINAL.__code__ is not _CANONICAL_TRY_NATIVE_TERMINAL_CODE
        or (builtins.min if terminal == "min" else builtins.max)
        is not (_BUILTIN_MIN if terminal == "min" else _BUILTIN_MAX)
        or _failpoints.hit is not _CANONICAL_FAILPOINT_HIT
        or _CANONICAL_FAILPOINT_HIT.__code__ is not _CANONICAL_FAILPOINT_HIT_CODE
        or Source.__dict__.get("open") is not _CANONICAL_SOURCE_OPEN
        or _CANONICAL_SOURCE_OPEN.__code__ is not _CANONICAL_SOURCE_OPEN_CODE
        or Source.__dict__.get("open_native") is not _CANONICAL_SOURCE_OPEN_NATIVE
        or _CANONICAL_SOURCE_OPEN_NATIVE.__code__ is not _CANONICAL_SOURCE_OPEN_NATIVE_CODE
        or Source.__dict__.get("_claim") is not _CANONICAL_SOURCE_CLAIM
        or _CANONICAL_SOURCE_CLAIM.__code__ is not _CANONICAL_SOURCE_CLAIM_CODE
        or Source.__dict__.get("current_exact_size") is not _CANONICAL_SOURCE_CURRENT_EXACT_SIZE
        or _CANONICAL_SOURCE_CURRENT_EXACT_SIZE.__code__
        is not _CANONICAL_SOURCE_CURRENT_EXACT_SIZE_CODE
        or _CANONICAL_HAS_ACTIVE_FAILPOINTS()
    ):
        return False, None
    logical = owner._logical_plan
    if (
        logical.engine != "auto"
        or logical.parallel is not None
        or type(logical.root) is not SourceNode
    ):
        return False, None

    from ..tabular.numpy import guarded_numpy_i64_column

    values = guarded_numpy_i64_column(logical.root.source)
    if values is None or values.size < _NATIVE_AUTO_THRESHOLD:
        return False, None
    reduced = values.min() if terminal == "min" else values.max()
    result = reduced.item()
    _record_direct_strategy(
        None,
        "numpy_direct",
        f"an exact identity NumPy int64 column used ndarray.{terminal}()",
    )
    return True, result


def _retained_identity_range(owner: Any) -> range | None:
    """Return a canonical Flow range without constructing a discarded query."""
    from .flow import _CANONICAL_FLOW_QUERY, Flow

    if (
        type(owner) is not Flow
        or Flow._query is not _CANONICAL_FLOW_QUERY
        or Flow.find_index is not FlowTerminalsMixin.find_index
        or has_active_failpoints()
        or Source.__dict__.get("open") is not _CANONICAL_SOURCE_OPEN
        or Source.__dict__.get("retained_sequence") is not _CANONICAL_RETAINED_SEQUENCE
    ):
        return None
    logical = owner._logical_plan
    root = logical.root
    if (
        logical.engine not in {"auto", "python"}
        or logical.parallel is not None
        or not isinstance(root, SourceNode)
        or type(root.source) is not Source
        or type(root.source.native_data) is not range
    ):
        return None
    retained = root.source.retained_sequence()
    return retained if type(retained) is range else None


def _iterator_starts_at_retained_sequence(
    iterator: Iterator[Any],
    source: list[Any] | tuple[Any, ...] | range,
) -> bool:
    """Prove an exact built-in iterator was opened over this retained source at offset zero."""
    source_type = type(source)
    iterator_type = type(iterator)
    expected_iterators: tuple[type[Iterator[Any]], ...]
    if source_type is list:
        expected_iterators = (_LIST_ITERATOR_TYPE,)
    elif source_type is tuple:
        expected_iterators = (_TUPLE_ITERATOR_TYPE,)
    else:
        expected_iterators = (_RANGE_ITERATOR_TYPE, _LONG_RANGE_ITERATOR_TYPE)
    if iterator_type not in expected_iterators:
        return False
    reduced = iterator.__reduce__()
    if type(reduced) is not tuple or len(reduced) != 3:
        return False
    constructor, arguments, offset = reduced
    if constructor is not _BUILTIN_ITER or type(arguments) is not tuple or len(arguments) != 1:
        return False
    remaining = arguments[0]
    if source_type is range:
        range_source = cast(range, source)
        return (
            type(remaining) is range
            and remaining.start == range_source.start
            and remaining.stop == range_source.stop
            and remaining.step == range_source.step
            and offset is None
        )
    return remaining is source and type(offset) is int and offset == 0


def _materialize_opened_python_sort(
    source_iterator: Iterator[Any],
    operation: SortOp,
) -> list[Any]:
    """Finish one already-opened source through the canonical sort boundary."""
    with (
        QueryRuntime() as runtime,
        open_operations(source_iterator, (operation,), runtime=runtime) as iterator,
    ):
        return _BUILTIN_LIST(iterator)


def _try_direct_sort_list(
    physical: PhysicalPlan,
    pipeline: Pipeline | None,
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
        or has_active_failpoints()
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
        return True, _materialize_opened_python_sort(source_iterator, operation)
    native_sort = getattr(_native, "sort_i64_exact_sequence_v1", None)
    if (
        not callable(native_sort)
        or has_active_failpoints()
        or builtins.sorted is not _BUILTIN_SORTED
        or Source.__dict__.get("open") is not _CANONICAL_SOURCE_OPEN
        or Source.__dict__.get("retained_sequence") is not _CANONICAL_RETAINED_SEQUENCE
        or pipeline.source.retained_sequence() is not source
        or not _iterator_starts_at_retained_sequence(source_iterator, source)
    ):
        result = _materialize_opened_python_sort(source_iterator, operation)
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
        result = _materialize_opened_python_sort(source_iterator, operation)
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


class FlowTerminalsMixin(Generic[T]):
    """Terminal and reduction methods mixed into the public Flow class."""

    @property
    def _pipeline(self) -> Pipeline:
        """Return the canonical unopened linear view for backend selection."""
        raise NotImplementedError

    def _uncompiled_exact_count(self) -> int | None:
        """Return safe identity cardinality without constructing a physical plan."""
        raise NotImplementedError

    def _uncompiled_python_identity_pipeline(self, query: Query) -> Pipeline | None:
        """Return an identity auto/Python plan that does not require backend selection."""
        raise NotImplementedError

    def _query(self, name: str, *arguments: Any, **options: Any) -> Query:
        """Describe one terminal request without consuming the stream."""
        raise NotImplementedError

    def _physical_query(self, name: str, *arguments: Any, **options: Any) -> PhysicalPlan:
        """Compile one terminal request once into its physical compatibility plan."""
        physical = compile_query(self._query(name, *arguments, **options))
        _record_sync_plan(physical)
        return physical

    def _terminal_context(
        self, name: str, *arguments: Any, **options: Any
    ) -> tuple[PhysicalPlan, Pipeline | None]:
        """Compile once and return a linear view only when the physical plan is linear."""
        return self._compiled_terminal_context(self._query(name, *arguments, **options))

    def _compiled_terminal_context(self, query: Query) -> tuple[PhysicalPlan, Pipeline | None]:
        """Compile one already-constructed terminal query into its execution context."""
        physical = compile_query(query)
        _record_sync_plan(physical)
        if physical.root is not None:
            return physical, None
        return physical, linear_pipeline(query.logical)

    @staticmethod
    def _native_decision(physical: PhysicalPlan) -> Any:
        """Extract the compiler's native decision without allowing a reselection."""
        payload = physical.backend_payload
        if not isinstance(payload, BackendPayload):
            return None
        return payload.native_decision

    def _try_native_materialize(
        self, physical: PhysicalPlan, pipeline: Pipeline | None, target: str
    ) -> tuple[bool, Any | None]:
        """Use the compiler's complete native decision only when no other route owns it."""
        payload = physical.backend_payload
        if (
            physical.root is not None
            or pipeline is None
            or not isinstance(payload, BackendPayload)
            or payload.arrow_prefix is not None
            or payload.native_decision is None
            or payload.native_decision.engine != "native"
        ):
            return False, None
        return try_native_materialize(pipeline, target, payload.native_decision)

    def run_with_report(
        self,
        terminal: str,
        /,
        *args: Any,
        **kwargs: Any,
    ) -> ExecutionResult[Any]:
        """Run one eager terminal normally and return its value with a read-only report.

        ``terminal`` names an existing eager method such as ``"to_list"`` or
        ``"sum"``. Lazy transformations and iteration are deliberately excluded.
        """
        if terminal not in _REPORTABLE_SYNC_TERMINALS:
            raise ValueError(f"{terminal!r} is not a reportable eager terminal")
        method = getattr(self, terminal)
        recorder, token = _start_recording(
            terminal,
            str(self._query("iterate").logical.engine),
        )
        started = perf_counter_ns()
        try:
            value = method(*args, **kwargs)
            return recorder.finish(value, perf_counter_ns() - started)
        finally:
            _stop_recording(token)

    def __iter__(self) -> Iterator[T]:
        """Yield items from the concrete Flow implementation."""
        raise NotImplementedError

    def _open(self) -> Any:
        """Return a context manager that closes the active pipeline iterator."""
        raise NotImplementedError

    def filter(self, predicate: Callable[[T], Any]) -> FlowTerminalsMixin[T]:
        """Build a lazy pipeline that keeps items satisfying `predicate`."""
        raise NotImplementedError

    def reject(self, predicate: Callable[[T], Any]) -> FlowTerminalsMixin[T]:
        """Build a lazy pipeline that drops items satisfying `predicate`."""
        raise NotImplementedError

    def drop(self, count: int) -> FlowTerminalsMixin[T]:
        """Build a lazy pipeline that skips the first `count` items."""
        raise NotImplementedError

    def to_list(self) -> list[T]:
        """Execute the pipeline and collect its items in a list.

        Returns:
            All emitted items in encounter order.
        """
        physical, pipeline = self._terminal_context("list")
        materializer = list
        if materializer is not _BUILTIN_LIST:
            return materializer(execute_physical(physical))
        if physical.root is not None:
            from ..execution.relational import (
                try_direct_global_list,
                try_direct_group_list,
                try_native_record_join,
                try_retained_arrow_unique_join,
            )

            arrow_records = try_retained_arrow_unique_join(physical)
            if arrow_records is not None:
                return cast(list[T], arrow_records)
            native_records = try_native_record_join(physical)
            if native_records is not None:
                return cast(list[T], native_records)
            direct_global, physical = try_direct_global_list(physical)
            if direct_global is not None:
                return cast(list[T], direct_global)
            direct_groups, physical = try_direct_group_list(physical)
            if direct_groups is not None:
                return cast(list[T], direct_groups)
        handled, value = _try_direct_sort_list(physical, pipeline)
        if handled:
            return cast(list[T], value)
        from ..execution.numpy_prefix import try_numpy_prefix_list

        handled, value = try_numpy_prefix_list(self, physical, pipeline)
        if handled:
            return cast(list[T], value)
        handled, value = _try_direct_numpy_list(self, physical, pipeline)
        if handled:
            return cast(list[T], value)
        handled, value = _try_direct_arrow_list(physical, pipeline)
        if handled:
            return cast(list[T], value)
        handled, value = _flow_structural_list.try_direct_python_structural_list(
            physical,
            pipeline,
        )
        if handled:
            return cast("list[T]", value)
        handled, value = self._try_native_materialize(physical, pipeline, "list")
        if handled:
            return cast(list[T], value)
        handled, value = _try_direct_python_materialize(
            physical,
            pipeline,
            "list",
            materializer,
        )
        return cast("list[T]", value) if handled else materializer(execute_physical(physical))

    def to_tuple(self) -> tuple[T, ...]:
        """Execute the pipeline and collect its items in a tuple.

        Returns:
            All emitted items in encounter order as a tuple.
        """
        physical, pipeline = self._terminal_context("tuple")
        materializer = tuple
        if (
            materializer is _BUILTIN_TUPLE
            and pipeline is not None
            and pipeline.operations
            and type(pipeline.operations[0]) in (TakeOp, DropOp)
        ):
            handled, value = _flow_structural_list.try_direct_retained_sequence_window(
                physical,
                pipeline,
                "tuple",
            )
            if handled:
                return cast("tuple[T, ...]", value)
        handled, value = self._try_native_materialize(physical, pipeline, "tuple")
        if handled:
            return cast(tuple[T, ...], value)
        handled, value = _try_direct_python_materialize(
            physical,
            pipeline,
            "tuple",
            materializer,
        )
        return cast("tuple[T, ...]", value) if handled else materializer(execute_physical(physical))

    def to_set(self) -> set[T]:
        """Execute the pipeline and collect distinct hashable items.

        Returns:
            The distinct emitted items; every item must be hashable.
        """
        physical, pipeline = self._terminal_context("set")
        handled, value = self._try_native_materialize(physical, pipeline, "set")
        if handled:
            return cast(set[T], value)
        materializer = set
        handled, value = _try_direct_python_materialize(
            physical,
            pipeline,
            "set",
            materializer,
        )
        return cast("set[T]", value) if handled else materializer(execute_physical(physical))

    def to_pandas(self, columns: Iterable[str] | None = None) -> Any:
        """Execute the pipeline and build a pandas DataFrame.

        Args:
            columns: Optional column labels passed to `pandas.DataFrame`.

        Returns:
            A pandas `DataFrame` containing the emitted items.
        """
        try:
            import pandas as pd  # type: ignore[import-untyped]
        except ImportError:
            raise ImportError(
                "to_pandas() requires the 'data' extra: pip install fpstreams[data]"
            ) from None
        names = list(columns) if columns is not None else None
        return pd.DataFrame(self.to_list(), columns=names)

    to_df = to_pandas

    def to_numpy(self, dtype: Any = None) -> Any:
        """Execute the pipeline and build a NumPy array.

        Args:
            dtype: The optional NumPy data type used for the resulting array.

        Returns:
            A NumPy array containing the emitted items.
        """
        try:
            import numpy as np
        except ImportError:
            raise ImportError(
                "to_numpy() requires the 'data' extra: pip install fpstreams[data]"
            ) from None
        values = self.to_list()
        resolved_dtype = np.dtype(dtype) if dtype is not None else np.dtype(int)
        if (
            len(values) >= _NATIVE_NUMPY_I64_MIN_ROWS
            and resolved_dtype == np.dtype(np.int64)
            and resolved_dtype.isnative
            and _is_exact_i64(values[0])
            and _is_exact_i64(values[-1])
        ):
            try:
                from .. import _native
            except ImportError:
                pass
            else:
                pack = getattr(_native, "pack_i64_exact_sequence_v1", None)
                if callable(pack) and (packed := pack(values)) is not None:
                    return np.frombuffer(packed, dtype=resolved_dtype).copy()
        return np.asarray(values, dtype=resolved_dtype if dtype is not None else None)

    to_np = to_numpy

    def to_csv(
        self,
        path: str | os.PathLike[str],
        *,
        header: Iterable[str] | None = None,
        encoding: str = "utf-8",
        spreadsheet_safe: bool = False,
    ) -> None:
        """Execute the pipeline and stream its items to a CSV file.

        Args:
            path: Destination file, opened in text write mode.
            header: Optional header row. For mapping items, these names also select and order cells.
            encoding: Encoding used when opening the destination.
            spreadsheet_safe: Prefix formula-like string cells so spreadsheet software treats them
                as text.
        """
        columns = tuple(header) if header is not None else None
        with (
            self._open() as iterator,
            open(path, "w", encoding=encoding, newline="") as handle,
        ):
            writer = csv.writer(handle, lineterminator="\n")
            if columns is not None:
                writer.writerow(columns)
            for item in iterator:
                values: Iterable[Any]
                if isinstance(item, Mapping):
                    values = (
                        [item.get(column) for column in columns]
                        if columns is not None
                        else item.values()
                    )
                elif isinstance(item, (list, tuple)):
                    values = item
                else:
                    values = (item,)
                writer.writerow(
                    [spreadsheet_safe_cell(value) for value in values]
                    if spreadsheet_safe
                    else values
                )

    def to_json(
        self,
        path: str | os.PathLike[str],
        *,
        encoding: str = "utf-8",
        ensure_ascii: bool = False,
        default: Callable[[Any], Any] | None = None,
    ) -> None:
        """Execute the pipeline and stream its items to one JSON array.

        Args:
            path: Destination file, replaced with one streamed JSON array.
            encoding: Encoding used when opening the destination.
            ensure_ascii: Whether JSON output escapes non-ASCII characters.
            default: Optional serializer called for objects the JSON encoder cannot handle.
        """
        encoder = (
            json.JSONEncoder(ensure_ascii=ensure_ascii)
            if default is None
            else json.JSONEncoder(ensure_ascii=ensure_ascii, default=default)
        )
        with self._open() as iterator, open(path, "w", encoding=encoding) as handle:
            handle.write("[")
            first = True
            for item in iterator:
                if not first:
                    handle.write(",")
                first = False
                for chunk in encoder.iterencode(item):
                    handle.write(chunk)
            handle.write("]")

    def describe(self) -> dict[str, int | float]:
        """Return count and one-pass summary statistics for numeric items.

        Returns:
            An empty dictionary for an empty flow; otherwise `count` plus numeric `sum`, `min`,
            `max`, and `mean` when real values occur. Mixed input also includes
            `numeric_count`, and two or more numeric values add sample `std`.
        """
        count = 0
        numeric_count = 0
        total = 0.0
        compensation = 0.0
        mean = 0.0
        squared_deviations = 0.0
        minimum = maximum = 0.0

        with self._open() as iterator:
            for item in iterator:
                count += 1
                if not isinstance(item, Real):
                    continue
                value = float(item)
                numeric_count += 1
                combined = total + value
                if math.isfinite(total) and math.isfinite(value) and math.isfinite(combined):
                    compensation += (
                        (total - combined) + value
                        if abs(total) >= abs(value)
                        else (value - combined) + total
                    )
                else:
                    compensation = 0.0
                total = combined

                delta = value - mean
                mean += delta / numeric_count
                squared_deviations += delta * (value - mean)
                if numeric_count == 1:
                    minimum = maximum = value
                else:
                    minimum = value if value < minimum else minimum
                    maximum = value if value > maximum else maximum

        if count == 0:
            return {}
        result: dict[str, int | float] = {"count": count}
        if numeric_count == 0:
            return result
        if numeric_count != count:
            result["numeric_count"] = numeric_count
        result.update(
            {
                "sum": total + compensation,
                "min": minimum,
                "max": maximum,
                "mean": mean,
            }
        )
        if numeric_count > 1:
            result["std"] = math.sqrt(max(squared_deviations, 0.0) / (numeric_count - 1))
        return result

    def aggregate(self, **aggregations: Aggregator) -> dict[str, Any]:
        """Compute several named aggregations while traversing the flow once.

        All named aggregators are updated during the same source traversal.

        Args:
            **aggregations: Result names mapped to aggregators updated in one traversal.

        Returns:
            Finished aggregation values keyed by the supplied argument names.
        """
        physical, pipeline = self._terminal_context("aggregate", **aggregations)
        items = prepare_aggregations(aggregations)
        program = compile_aggregations(items)
        if physical.root is not None:
            # CollectorProgram owns and closes this iterator, including when all
            # collectors finish before the relational source is exhausted.
            return run_collector_program(execute_physical(physical), program.collectors)
        if pipeline is None:
            raise RuntimeError("linear aggregate plan is missing its canonical pipeline")
        native_decision = self._native_decision(physical)
        mask = program.native_mask
        mean_only = native_mean_only(items)
        terminal: TerminalName | None = None if mask is None else mask.scalar_terminal
        specialized_allowed = pipeline.engine == "native" or not has_active_failpoints()
        if not specialized_allowed:
            return run_collector_program(execute(pipeline, auto_native=False), program.collectors)
        if (
            compatibility_result := _f64_total_compatibility_fallback(
                pipeline, program, native_decision
            )
        ) is not None:
            return compatibility_result
        if mean_only:
            return _run_mean_only_aggregations(
                physical,
                pipeline,
                items,
                program,
                native_decision,
            )
        if terminal == "count" and (known_size := exact_count(pipeline)) is not None:
            return {name: known_size for name, _aggregation in items}
        if terminal is not None:
            native, result = try_native_terminal(pipeline, terminal, decision=native_decision)
            if native:
                # A one-field mask can only contain repetitions of the matching
                # native aggregation kind, so one scalar result projects to all
                # requested names without another traversal.
                return {name: result for name, _aggregation in items}
            return run_collector_program(execute(pipeline, auto_native=False), program.collectors)
        if mask is not None and mask.statistics_only:
            return _run_statistics_aggregations(pipeline, items, program, native_decision)
        if mask is not None:
            masked_bits: int | None = mask.bits
            if (
                native_decision is not None
                and native_decision.program is not None
                and not mask.prefers_masked_kernel(native_decision.program.kind)
            ):
                masked_bits = None
            native, snapshot = try_native_aggregate(
                pipeline, decision=native_decision, mask=masked_bits
            )
            if native:
                if snapshot is None:
                    raise RuntimeError("native aggregate result is missing")
                return finish_native_aggregations(items, snapshot)
        return run_collector_program(execute(pipeline, auto_native=False), program.collectors)

    summarize = aggregate

    def collect(
        self,
        collector: Callable[[Iterable[T]], C] | Collector[T, Any, C] | None = None,
        /,
        **collectors: Collector[T, Any, Any],
    ) -> C | dict[str, Any]:
        """Reduce the pipeline with one Collector or named Collectors.

        One collector returns its finished value. Named collectors share one source traversal
        and return a dictionary.

        Args:
            collector: One `Collector` or callable invoked with this flow to produce the result.
            **collectors: Named streaming collectors updated together in one traversal.

        Returns:
            The collector result, or a dictionary of results for named collectors.
        """
        if collector is not None and collectors:
            raise TypeError("collect accepts either one collector or named collectors, not both")
        if collector is not None:
            _record_direct_strategy(
                None,
                "dynamic_collector",
                "callable collector controls whether and how the input pipeline is consumed",
            )
            return collector(self)
        return run_collectors(self, prepare_collectors(cast(Any, collectors)))

    def join(self, separator: str = "") -> str:
        """Convert items to strings and join them with separator.

        This is a string terminal operation; it consumes the flow and does not perform a
        relational join.

        Args:
            separator: String placed between consecutive `str(item)` values.

        Returns:
            One string containing every item separated by `separator`.
        """
        with self._open() as iterator:
            return separator.join(str(item) for item in iterator)

    def for_each(self, action: Callable[[T], Any]) -> None:
        """Execute action once for every item.

        Args:
            action: Called once for each emitted item; its return value is ignored.
        """
        with self._open() as iterator:
            for item in iterator:
                action(item)

    def partition(self, predicate: Callable[[T], bool]) -> tuple[list[T], list[T]]:
        """Collect matching and non-matching items in separate lists.

        Args:
            predicate: Called once per item; truthy items enter the first returned list.

        Returns:
            `(matches, misses)`, preserving encounter order within both lists.
        """
        matches: list[T] = []
        misses: list[T] = []
        with self._open() as iterator:
            for item in iterator:
                (matches if predicate(item) else misses).append(item)
        return matches, misses

    def partition_results(self) -> tuple[list[Any], list[Exception]]:
        """Separate Result values into successes and failures.

        Returns:
            `(success_values, exceptions)`, with `Ok` and `Err` payloads unwrapped in their
            respective encounter orders.

        Raises:
            TypeError: If any emitted item is neither `Ok` nor `Err`.
        """
        successes: list[Any] = []
        failures: list[Exception] = []
        with self._open() as iterator:
            for result in iterator:
                if isinstance(result, Ok):
                    successes.append(result.value)
                elif isinstance(result, Err):
                    failures.append(result.error)
                else:
                    raise TypeError("partition_results() requires Result values")
        return successes, failures

    def to_async(self) -> Any:
        """View this synchronous pipeline as an AsyncFlow.

        Returns:
            An `AsyncFlow` that yields the same items.
        """
        from .async_flow import aflow

        return aflow(self)

    def first(self, default: Any = _MISSING) -> T | Any:
        """Return the first item without consuming an unnecessary tail.

        Args:
            default: Returned only when the flow is empty.

        Returns:
            The first item, or `default` when the flow is empty.

        Raises:
            EmptyFlowError: If the flow is empty and no default is supplied.
        """
        physical, pipeline = self._terminal_context("first", default)
        if pipeline is not None:
            native, result = try_native_terminal(
                pipeline, "first", decision=self._native_decision(physical)
            )
            if native:
                if result is not None:
                    return cast(T, result)
                if default is _MISSING:
                    raise EmptyFlowError("first() requires at least one item")
                return default
        with _open_terminal_values(physical, pipeline) as iterator:
            try:
                return next(iterator)
            except StopIteration:
                if default is _MISSING:
                    raise EmptyFlowError("first() requires at least one item") from None
                return default

    def last(self, default: Any = _MISSING) -> T | Any:
        """Return the last item, or default when the flow is empty.

        Args:
            default: Returned only when the flow is empty.

        Returns:
            The last item, or `default` when the flow is empty.

        Raises:
            EmptyFlowError: If the flow is empty and no default is supplied.
        """
        retained = _retained_identity_sequence(self, "last", default)
        if retained is not None:
            _record_direct_strategy(
                None,
                "python_direct",
                "an exact retained identity sequence answered last() by direct indexing",
            )
            try:
                return retained[-1]
            except IndexError:
                if default is _MISSING:
                    raise EmptyFlowError("last() requires at least one item") from None
                return default
        physical, pipeline = self._terminal_context("last", default)
        if pipeline is not None:
            native, result = try_native_terminal(
                pipeline, "last", decision=self._native_decision(physical)
            )
            if native:
                if result is not None:
                    return cast(T, result)
                if default is _MISSING:
                    raise EmptyFlowError("last() requires at least one item")
                return default
        found = default
        with _open_terminal_values(physical, pipeline) as iterator:
            for item in iterator:
                found = item
        if found is _MISSING:
            raise EmptyFlowError("last() requires at least one item")
        return found

    def find(self, predicate: Callable[[T], Any], default: Any = _MISSING) -> T | Any:
        """Return the first matching item, a default, or raise EmptyFlowError.

        Args:
            predicate: Called in order until its first truthy result.
            default: Returned when no predicate result is truthy.

        Returns:
            The first matching item, or `default` when no item matches.

        Raises:
            EmptyFlowError: If no item matches and no default is supplied.
        """
        matches = self.filter(predicate)
        if default is not _MISSING:
            return matches.first(default)
        try:
            return matches.first()
        except EmptyFlowError:
            raise EmptyFlowError("find() found no matching item") from None

    def find_index(self, predicate: Callable[[T], Any]) -> int | None:
        """Return the index of the first matching item, or None.

        Args:
            predicate: Called with each item in order until its first truthy result.

        Returns:
            The zero-based position of the first truthy predicate result, or `None`.
        """
        if not callable(predicate):
            raise TypeError("predicate must be callable")
        with self._open() as iterator:
            for position, item in enumerate(iterator):
                if predicate(item):
                    return position
        return None

    def index_of(self, value: T) -> int | None:
        """Return the index of the first equal value, or None.

        Args:
            value: Target compared to each source item with equality.

        Returns:
            The zero-based position of the first item equal to `value`, or `None`.
        """
        retained = _retained_identity_range(self) if type(value) in (int, bool) else None
        if type(retained) is range and type(value) in (int, bool):
            _record_direct_strategy(
                None,
                "python_direct",
                "an exact retained range answered index_of() arithmetically",
            )
            try:
                return retained.index(cast(int, value))
            except ValueError:
                return None
        return self.find_index(lambda item: item == value)

    def nth(self, index: int, default: Any = _MISSING) -> T | Any:
        """Return the item at a positive or negative index.

        Args:
            index: Zero-based position; negative values count backward from the end.
            default: Returned when `index` is outside the flow.

        Returns:
            The selected item, or `default` when the index is out of range.

        Raises:
            EmptyFlowError: If the index is out of range and no default is supplied.
        """
        position = operator.index(index)
        retained = _retained_identity_sequence(self, "nth", position, default)
        if retained is not None:
            _record_direct_strategy(
                None,
                "python_direct",
                "an exact retained identity sequence answered nth() by direct indexing",
            )
            try:
                return retained[position]
            except IndexError:
                if default is not _MISSING:
                    return default
                raise EmptyFlowError(f"nth({position}) is out of range") from None
        if position >= 0:
            candidate = self.drop(position)
            if default is not _MISSING:
                return candidate.first(default)
            try:
                return candidate.first()
            except EmptyFlowError:
                raise EmptyFlowError(f"nth({position}) is out of range") from None
        if position == -1:
            if default is not _MISSING:
                return self.last(default)
            try:
                return self.last()
            except EmptyFlowError:
                raise EmptyFlowError("nth(-1) is out of range") from None

        width = -position
        with self._open() as iterator:
            tail: deque[T] = deque(iterator, maxlen=width)
        if len(tail) == width:
            return tail[0]
        if default is not _MISSING:
            return default
        raise EmptyFlowError(f"nth({position}) is out of range")

    def count(self) -> int:
        """Count all items produced by the pipeline.

        Returns:
            The total number of emitted items.
        """
        instrumented = has_active_failpoints()
        if not instrumented and (known_size := self._uncompiled_exact_count()) is not None:
            return known_size
        physical, pipeline = self._terminal_context("count")
        if pipeline is not None:
            specialized_allowed = not instrumented or pipeline.engine == "native"
            if specialized_allowed:
                known_size = exact_count(pipeline)
                if known_size is not None:
                    return known_size
                native, result = try_native_terminal(
                    pipeline, "count", decision=self._native_decision(physical)
                )
                if native:
                    return cast(int, result)
                payload = physical.backend_payload
                if isinstance(payload, BackendPayload) and payload.arrow_prefix is not None:
                    from ..execution.arrow import try_arrow_count

                    handled, result = try_arrow_count(pipeline, prefix=payload.arrow_prefix)
                    if handled:
                        return result
        with _open_terminal_values(physical, pipeline) as iterator:
            return builtins.sum(1 for _ in iterator)

    def sum(self, start: Any = 0) -> Any:
        """Add all items, starting with start.

        Args:
            start: Value added before all emitted items, matching Python's built-in `sum`.

        Returns:
            The total of `start` and all emitted items.
        """
        physical, pipeline = self._terminal_context("sum", start)
        native_allowed = (
            pipeline is not None
            and type(start) is int
            and start == 0
            and (pipeline.engine != "auto" or not has_active_failpoints())
        )
        if native_allowed:
            assert pipeline is not None
            native, result = try_native_terminal(
                pipeline, "sum", decision=self._native_decision(physical)
            )
            if native:
                return result
        # Consuming the selected iterator directly avoids two generator forwarding layers.
        # open_operations retains source ownership and automatically restores the precise
        # pull/callback boundaries whenever a failpoint is active.
        if pipeline is not None:
            payload = physical.backend_payload
            arrow_prefix = payload.arrow_prefix if isinstance(payload, BackendPayload) else None
            if arrow_prefix is not None and type(start) is int:
                from ..execution.arrow import try_arrow_i64_field_reduction

                reduction = try_arrow_i64_field_reduction(
                    pipeline,
                    arrow_prefix,
                    "sum",
                )
                if reduction is not None:
                    if reduction.source_value_error is not None:
                        raise reduction.source_value_error
                    _record_direct_strategy(
                        physical,
                        "arrow_direct",
                        "an exact int64 Arrow field was reduced by a columnar kernel",
                    )
                    return start if not reduction.seen else start + cast(int, reduction.value)
            if arrow_prefix is None or type(start) is not int:
                handled, result = _try_direct_python_scalar_sum(physical, pipeline, start)
                if handled:
                    return result
                return _sum_python_pipeline(pipeline, start)
        with _open_terminal_values(physical, pipeline) as iterator:
            return builtins.sum(iterator, start)

    def _statistics_snapshot(self) -> StatisticsSnapshot:
        """Compute one-pass numeric statistics, using the native terminal when supported."""
        physical, pipeline = self._terminal_context("statistics")
        if pipeline is not None:
            from ..runtime.failpoints import has_active_failpoints

            native_allowed = pipeline.engine != "auto" or not has_active_failpoints()
        else:
            native_allowed = False
        if pipeline is not None and native_allowed:
            native, snapshot = try_native_statistics(
                pipeline, decision=self._native_decision(physical)
            )
            if native:
                if snapshot is None:
                    raise RuntimeError("native statistics result is missing")
                return snapshot
        statistics = OnlineStatistics()
        with _open_terminal_values(physical, pipeline) as iterator:
            for item in iterator:
                statistics.accept(item)
        return statistics.snapshot()

    def mean(self) -> float | None:
        """Return the arithmetic mean, or None for an empty flow.

        Returns:
            The compensated floating-point mean, or `None` when no items are emitted.

        Raises:
            TypeError: If an emitted item is not a real number.
        """
        physical, pipeline = self._terminal_context("mean")
        if pipeline is not None:
            native_decision = self._native_decision(physical)
            payload = physical.backend_payload
            arrow_prefix = payload.arrow_prefix if isinstance(payload, BackendPayload) else None
            if (
                pipeline.engine == "auto"
                and physical.root is None
                and physical.parallel is None
                and pipeline.parallel is None
            ):
                result, strategy, reason = _run_opened_auto_numeric_mean(
                    pipeline,
                    native_decision,
                    arrow_prefix,
                )
                _record_direct_strategy(physical, strategy, reason)
                return result
            if pipeline.engine == "native":
                native, result = try_native_mean(pipeline, decision=self._native_decision(physical))
                if native:
                    return result
            if arrow_prefix is not None and pipeline.engine == "native":
                from ..execution.arrow import try_arrow_numeric_field_mean

                handled, result = try_arrow_numeric_field_mean(pipeline, arrow_prefix)
                if handled:
                    _record_direct_strategy(
                        physical,
                        "arrow_direct",
                        "an exact numeric Arrow field was reduced by a compensated batch kernel",
                    )
                    return result
        with _open_terminal_values(physical, pipeline) as iterator:
            if pipeline is not None:
                handled, result, hybrid = _try_direct_numeric_iterator_mean(pipeline, iterator)
                if handled:
                    _record_direct_strategy(
                        physical,
                        "rust_python_hybrid" if hybrid else "rust_direct",
                        (
                            "a callback-free numeric prefix continued through the canonical "
                            "Python compensated mean loop"
                            if hybrid
                            else "the opened numeric iterator was reduced by a callback-free "
                            "compensated Rust loop"
                        ),
                    )
                    return result
            return compensated_mean(iterator)

    average = mean

    def variance(self, *, ddof: int = 1) -> float | None:
        """Return the variance, or None when too few values are available.

        Args:
            ddof: Non-negative adjustment in the divisor `count - ddof`.

        Returns:
            The floating-point variance, or `None` when `count <= ddof`.

        Raises:
            TypeError: If an emitted item is not a real number.
            ValueError: If `ddof` is negative.
        """
        validate_ddof(ddof)
        return variance_from(self._statistics_snapshot(), ddof)

    def std(self, *, ddof: int = 1) -> float | None:
        """Return the standard deviation, or None when too few values are available.

        Args:
            ddof: Non-negative adjustment in the variance divisor `count - ddof`.

        Returns:
            The square root of the variance, or `None` when `count <= ddof`.

        Raises:
            TypeError: If an emitted item is not a real number.
            ValueError: If `ddof` is negative.
        """
        validate_ddof(ddof)
        return std_from(self._statistics_snapshot(), ddof)

    def min(self, *, key: Callable[[T], Any] | None = None) -> T:
        """Return the smallest item and raise on an empty flow.

        Args:
            key: Optional callable whose result is compared instead of the item.

        Returns:
            The smallest item according to `key`.
        """
        direct, direct_result = _try_direct_numpy_i64_extreme(self, "min", key)
        if direct:
            if direct_result is None:
                raise EmptyFlowError("min() requires at least one item")
            return cast(T, direct_result)
        physical, pipeline = self._terminal_context("min", key=key)
        native_allowed = (
            key is None
            and pipeline is not None
            and (pipeline.engine != "auto" or not has_active_failpoints())
        )
        if native_allowed:
            assert pipeline is not None
            native, result = try_native_terminal(
                pipeline, "min", decision=self._native_decision(physical)
            )
            if native:
                if result is None:
                    raise EmptyFlowError("min() requires at least one item")
                return cast(T, result)
        payload = physical.backend_payload
        if (
            key is None
            and pipeline is not None
            and isinstance(payload, BackendPayload)
            and payload.arrow_prefix is not None
        ):
            from ..execution.arrow import try_arrow_i64_field_reduction

            reduction = try_arrow_i64_field_reduction(pipeline, payload.arrow_prefix, "min")
            if reduction is not None:
                if reduction.source_value_error is not None:
                    raise EmptyFlowError("min() requires at least one item") from None
                if not reduction.seen:
                    raise EmptyFlowError("min() requires at least one item")
                _record_direct_strategy(
                    physical,
                    "arrow_direct",
                    "an exact int64 Arrow field was reduced by a columnar kernel",
                )
                return cast(T, reduction.value)
        try:
            with _open_terminal_values(physical, pipeline) as iterator:
                return cast(T, builtins.min(iterator, key=key))
        except ValueError:
            raise EmptyFlowError("min() requires at least one item") from None

    def max(self, *, key: Callable[[T], Any] | None = None) -> T:
        """Return the largest item and raise on an empty flow.

        Args:
            key: Optional callable whose result is compared instead of the item.

        Returns:
            The largest item according to `key`.
        """
        direct, direct_result = _try_direct_numpy_i64_extreme(self, "max", key)
        if direct:
            if direct_result is None:
                raise EmptyFlowError("max() requires at least one item")
            return cast(T, direct_result)
        physical, pipeline = self._terminal_context("max", key=key)
        native_allowed = (
            key is None
            and pipeline is not None
            and (pipeline.engine != "auto" or not has_active_failpoints())
        )
        if native_allowed:
            assert pipeline is not None
            native, result = try_native_terminal(
                pipeline, "max", decision=self._native_decision(physical)
            )
            if native:
                if result is None:
                    raise EmptyFlowError("max() requires at least one item")
                return cast(T, result)
        payload = physical.backend_payload
        if (
            key is None
            and pipeline is not None
            and isinstance(payload, BackendPayload)
            and payload.arrow_prefix is not None
        ):
            from ..execution.arrow import try_arrow_i64_field_reduction

            reduction = try_arrow_i64_field_reduction(pipeline, payload.arrow_prefix, "max")
            if reduction is not None:
                if reduction.source_value_error is not None:
                    raise EmptyFlowError("max() requires at least one item") from None
                if not reduction.seen:
                    raise EmptyFlowError("max() requires at least one item")
                _record_direct_strategy(
                    physical,
                    "arrow_direct",
                    "an exact int64 Arrow field was reduced by a columnar kernel",
                )
                return cast(T, reduction.value)
        try:
            with _open_terminal_values(physical, pipeline) as iterator:
                return cast(T, builtins.max(iterator, key=key))
        except ValueError:
            raise EmptyFlowError("max() requires at least one item") from None

    def top(self, count: int, *, key: Selector | None = None) -> list[T]:
        """Return up to count largest items without sorting the entire result.

        Args:
            count: Maximum number of items to return.
            key: Optional callable, field name, index, path, or expression used for ranking.

        Returns:
            Up to `count` items ordered from largest to smallest selected value.
        """
        if count < 0:
            raise ValueError("top count must be non-negative")
        select = None if key is None else cast(Callable[[T], Any], compile_selector(key))
        with self._open() as iterator:
            return heapq.nlargest(count, iterator, key=cast(Any, select))

    greatest = top

    def bottom(self, count: int, *, key: Selector | None = None) -> list[T]:
        """Return up to count smallest items without sorting the entire result.

        Args:
            count: Maximum number of items to return.
            key: Optional callable, field name, index, path, or expression used for ranking.

        Returns:
            Up to `count` items ordered from smallest to largest selected value.
        """
        if count < 0:
            raise ValueError("bottom count must be non-negative")
        select = None if key is None else cast(Callable[[T], Any], compile_selector(key))
        with self._open() as iterator:
            return heapq.nsmallest(count, iterator, key=cast(Any, select))

    least = bottom

    def minmax(self, *, key: Selector | None = None) -> tuple[T, T]:
        """Return the smallest and largest items in one traversal.

        Args:
            key: Optional callable, field name, index, path, or expression used for comparison.

        Returns:
            `(minimum_item, maximum_item)` according to the selected values.

        Raises:
            EmptyFlowError: If the flow emits no items.
        """
        if key is not None:
            select = cast(Callable[[T], Any], compile_selector(key))
            with self._open() as iterator:
                try:
                    minimum = maximum = next(iterator)
                except StopIteration:
                    raise EmptyFlowError("minmax() requires at least one item") from None
                minimum_key = maximum_key = select(minimum)
                for item in iterator:
                    item_key = select(item)
                    if item_key < minimum_key:
                        minimum = item
                        minimum_key = item_key
                    if item_key > maximum_key:
                        maximum = item
                        maximum_key = item_key
            return minimum, maximum

        physical, pipeline = self._terminal_context("minmax")
        if pipeline is not None:
            from ..runtime.failpoints import has_active_failpoints

            if pipeline.engine != "auto" or not has_active_failpoints():
                native, snapshot = try_native_aggregate(
                    pipeline,
                    decision=self._native_decision(physical),
                    mask=_MINMAX_NATIVE_MASK,
                )
                if native:
                    if snapshot is None:
                        raise RuntimeError("native aggregate result is missing")
                    minimum, maximum = snapshot[2], snapshot[3]
                    if minimum is None:
                        raise EmptyFlowError("minmax() requires at least one item")
                    return cast(tuple[T, T], (minimum, maximum))
            with open_operations(pipeline.source.open(), pipeline.operations) as iterator:
                return cast(tuple[T, T], _minmax_python_values(iterator))

        with _open_terminal_values(physical, pipeline) as iterator:
            return cast(tuple[T, T], _minmax_python_values(iterator))

    def any(self, predicate: Callable[[T], bool] = bool) -> bool:
        """Return whether at least one item satisfies predicate.

        Args:
            predicate: Tested in order until one result is truthy; defaults to `bool`.

        Returns:
            `True` when any item satisfies `predicate`; `False` for an empty flow.
        """
        query = self._query("any", predicate)
        direct_pipeline: Pipeline | None = None
        if (
            predicate is not bool
            and not isinstance(predicate, (Expr, FExpr))
            and not has_active_failpoints()
        ):
            direct_pipeline = self._uncompiled_python_identity_pipeline(query)
        if direct_pipeline is not None:
            _record_direct_strategy(
                None,
                "python_direct",
                "opaque predicate uses the direct Python identity pipeline",
            )
            values = _open_terminal_values(None, direct_pipeline)
        else:
            physical, pipeline = self._compiled_terminal_context(query)
            if predicate is bool and pipeline is not None:
                native, result = try_native_terminal(
                    pipeline, "any", decision=self._native_decision(physical)
                )
                if native:
                    return bool(result)
            elif pipeline is not None and isinstance(predicate, (Expr, FExpr)):
                filtered = self.filter(cast(Callable[[T], Any], predicate))
                native, result = try_native_terminal(filtered._pipeline, "first")
                if native:
                    return result is not None
            values = _open_terminal_values(physical, pipeline)
        with values as iterator:
            return builtins.any(predicate(item) for item in iterator)

    def all(self, predicate: Callable[[T], bool] = bool) -> bool:
        """Return whether every item satisfies predicate.

        Args:
            predicate: Tested in order until one result is falsey; defaults to `bool`.

        Returns:
            `True` when every item satisfies `predicate`, including for an empty flow.
        """
        physical, pipeline = self._terminal_context("all", predicate)
        if predicate is bool and pipeline is not None:
            native, result = try_native_terminal(
                pipeline, "all", decision=self._native_decision(physical)
            )
            if native:
                return bool(result)
        elif pipeline is not None and isinstance(predicate, (Expr, FExpr)):
            rejected = self.reject(cast(Callable[[T], Any], predicate))
            native, result = try_native_terminal(rejected._pipeline, "first")
            if native:
                return result is None
        with _open_terminal_values(physical, pipeline) as iterator:
            return builtins.all(map(predicate, iterator))

    def none(self, predicate: Callable[[T], bool] = bool) -> bool:
        """Return whether no item satisfies predicate.

        Args:
            predicate: Tested in order until one result is truthy; defaults to `bool`.

        Returns:
            `True` only when no item satisfies `predicate`, including for an empty flow.
        """
        return not self.any(predicate)

    def reduce(
        self,
        function: Callable[[Any, T], Any],
        initial: Any = _MISSING,
    ) -> Any:
        """Combine items from left to right with an optional initial value.

        Args:
            function: Called as `function(accumulator, item)` from left to right.
            initial: Starting accumulator; when omitted, the first item becomes the accumulator.

        Returns:
            The final left-to-right accumulator.

        Raises:
            EmptyFlowError: If the flow is empty and `initial` is omitted.
        """
        with self._open() as iterator:
            if initial is _MISSING:
                try:
                    accumulator = next(iterator)
                except StopIteration:
                    raise EmptyFlowError("reduce() requires at least one item") from None
            else:
                accumulator = initial
            for item in iterator:
                accumulator = function(accumulator, item)
        return accumulator

    def reduce_right(
        self,
        function: Callable[[T, Any], Any],
        initial: Any = _MISSING,
        *,
        max_items: int | None = None,
    ) -> Any:
        """Combine buffered items from right to left.

        Args:
            function: Called as `function(item, accumulator)` from right to left.
            initial: Starting accumulator; when omitted, the last item becomes the accumulator.
            max_items: Optional maximum number of source items that may be buffered.

        Returns:
            The final right-to-left accumulator.

        Raises:
            BufferLimitError: If the source contains more than `max_items` items.
            EmptyFlowError: If the flow is empty and `initial` is omitted.
        """
        if not callable(function):
            raise TypeError("function must be callable")
        if max_items is not None:
            max_items = operator.index(max_items)
            if max_items < 0:
                raise ValueError("max_items must be non-negative")
        values: list[T] = []
        with self._open() as iterator:
            for item in iterator:
                if max_items is not None and len(values) >= max_items:
                    raise BufferLimitError(f"reduce_right() exceeded max_items={max_items}")
                values.append(item)
        if initial is _MISSING:
            if not values:
                raise EmptyFlowError("reduce_right() requires at least one item")
            accumulator = values.pop()
        else:
            accumulator = initial
        for item in reversed(values):
            accumulator = function(item, accumulator)
        return accumulator

    fold_right = reduce_right

    def reduce_by(
        self,
        key: Selector,
        function: Callable[[R, T], R],
        *,
        initializer: Callable[[], R],
    ) -> dict[Any, R]:
        """Reduce items independently for each selected key.

        Args:
            key: Callable, field name, index, path, or expression selecting each group key.
            function: Called as `function(group_state, item)` for items in that group.
            initializer: Called once when each distinct group is first encountered.

        Returns:
            Final accumulator state for each hashable key, in first-key encounter order.
        """
        if not callable(initializer):
            raise TypeError("initializer must be callable")
        select = compile_selector(key)
        states: dict[Any, R] = {}
        with self._open() as iterator:
            for item in iterator:
                group = select(item)
                try:
                    state = states[group]
                except KeyError:
                    state = initializer()
                except TypeError:
                    raise TypeError("reduce_by() keys must be hashable") from None
                states[group] = function(state, item)
        return states

    fold_by = reduce_by

    def frequencies(self, key: Selector | None = None) -> dict[Any, int]:
        """Count occurrences of values or selected keys.

        Args:
            key: Optional callable, field name, index, path, or expression selecting the value to
                count; the item itself is counted when omitted.

        Returns:
            Occurrence counts keyed by each hashable selected value.
        """
        physical, pipeline = self._terminal_context("iterate")
        select = None if key is None else compile_selector(key)

        if select is None:
            handled, native_counts = _try_native_identity_frequencies(pipeline)
            if handled:
                return cast(dict[Any, int], native_counts)

        direct_range = (
            select is None
            and pipeline is not None
            and not pipeline.operations
            and not has_active_failpoints()
            # A range proves every key is an exact integer without a full prepass.
            # Retained lists and tuples may contain protocol-bearing custom keys;
            # their stable one-pass dictionary loop also wins on repeated keys.
            and type(pipeline.source.native_data) is range
        )
        with _open_frequency_values(
            physical,
            pipeline,
            direct_range=direct_range,
        ) as iterator:
            if direct_range:
                try:
                    return dict(Counter(iterator))
                except TypeError:
                    raise TypeError("reduce_by() keys must be hashable") from None

            if select is None:
                return _update_identity_frequency_counts(iterator, {})

            counts: dict[Any, int] = {}
            get_count = counts.get
            for item in iterator:
                selected = select(item)
                try:
                    count = get_count(selected, 0)
                except KeyError:
                    count = 0
                except TypeError:
                    raise TypeError("reduce_by() keys must be hashable") from None
                counts[selected] = count + 1
            return counts

    count_by = frequencies
