"""Direct list sinks with source-independent ownership proofs."""

from __future__ import annotations

import itertools
import operator
import sys
from collections.abc import Callable, Iterator, Sequence
from types import FunctionType
from typing import Any, cast

from ..execution._rows_fusion import (
    I64RowFilterPlan,
    RowsFilterSink,
    compile_rows_filter_sink,
    lower_i64_row_filter,
    rows_filter_sink_eligible,
)
from ..execution.physical import operations_from_physical_nodes
from ..execution.sync import open_operations
from ..expressions.row import RowExpr
from ..expressions.scalar import Expr
from ..physical.plan import BackendPayload, PhysicalPlan
from ..planning.logical import Pipeline
from ..planning.native import _AUTO_I64_EXTERNAL_IDENTITY_REASON
from ..planning.source import (
    Source,
)
from ..planning.sync import (
    ChunkOp,
    DropOp,
    FilterOp,
    FlatMapOp,
    MapOp,
    ScanOp,
    TakeOp,
    UniqueOp,
    WindowOp,
)
from ..runtime.iterators import closing_iterators
from ..runtime.query import QueryRuntime
from . import _flow_unique_list

_BUILTIN_LIST: type[list[Any]] = list
_BUILTIN_TUPLE: type[tuple[Any, ...]] = tuple
_BUILTIN_TYPE: Callable[[Any], type[Any]] = type
_BUILTIN_LEN: Callable[[Any], int] = len
_OPERATOR_ADD = operator.add
_OPERATOR_LENGTH_HINT = operator.length_hint
_LIST_ITERATOR_TYPE = _BUILTIN_TYPE(iter([]))
_TUPLE_ITERATOR_TYPE = _BUILTIN_TYPE(iter(()))
_CPYTHON_312_DIRECT_FILTER_LOOP = sys.implementation.name == "cpython" and sys.version_info[:2] == (
    3,
    12,
)
_BATCHED = getattr(itertools, "batched", None)
_NATIVE_ROWS_DROP_NULLS_MIN_ROWS = 512
_NATIVE_ROWS_SELECT_MIN_ROWS = 2_048
_PYTHON_ROWS_FILTER_MIN_ROWS = 16_384
_NATIVE_I64_FILTER_MIN_ITEMS = 8
# Keep this measured crossover synchronized with ``I64_ROW_FILTER_MAX_FIELDS`` in select.rs.
_NATIVE_I64_ROW_FILTER_MAX_FIELDS = 2
_DropNullsEndpoint = Callable[
    [list[Any], Iterator[Any], str],
    tuple[Any | None, bool] | None,
]
_I64RowFilterEndpoint = Callable[
    [list[Any], Iterator[Any], str, tuple[tuple[int, int], ...], bool],
    tuple[Any | None, bool] | None,
]
_ROWS_FILTER_STRATEGY_REASONS = {
    ("rust_direct", True): (
        "retained exact dictionary rows used the Rust i64 expression filter sink"
    ),
    ("rust_python_hybrid", True): (
        "a Rust-filtered exact dictionary prefix continued through the Python RowExpr sink"
    ),
    ("rust_python_hybrid", False): (
        "a Rust-filtered exact dictionary prefix continued through Python"
    ),
    ("python_direct", True): ("retained exact dictionary rows used the direct RowExpr filter sink"),
    ("python_direct", False): "an exact dictionary prefix used the direct RowExpr filter sink",
}


def _native_i64_row_filter_endpoint(
    plan: I64RowFilterPlan | None,
) -> _I64RowFilterEndpoint | None:
    """Resolve the optional Rust row-expression endpoint before the source is opened."""
    if plan is None:
        return None
    try:
        from .. import _native
    except ImportError:
        return None
    raw_endpoint = getattr(_native, "filter_i64_expr_exact_dict_prefix_v1", None)
    if not callable(raw_endpoint):
        return None
    return cast(_I64RowFilterEndpoint, raw_endpoint)


def _invoke_native_i64_row_filter_prefix(
    output: list[Any],
    source: Iterator[Any],
    plan: I64RowFilterPlan | None,
    endpoint: _I64RowFilterEndpoint | None,
) -> tuple[Any | None, bool] | None:
    """Run the optional native prefix for one already-lowered row expression."""
    if plan is None or endpoint is None:
        return None
    field, instructions, negate = plan
    return endpoint(output, source, field, instructions, negate)


def _drain_i64_then_python_row_filter(
    output: list[Any],
    source: Iterator[Any],
    physical_sink: RowsFilterSink,
    plan: I64RowFilterPlan | None,
    endpoint: _I64RowFilterEndpoint | None,
) -> tuple[Any | None, bool, str]:
    """Run the optional Rust prefix, then resume the generated Python sink at its boundary."""
    rows_before_native = _OPERATOR_LENGTH_HINT(source, -1)
    try:
        native = _invoke_native_i64_row_filter_prefix(
            output,
            source,
            plan,
            endpoint,
        )
    except StopIteration:
        return None, True, "rust_direct"

    values = source
    strategy = "python_direct"
    if native is not None:
        first_incompatible, completed = native
        del native
        if completed:
            return None, True, "rust_direct"
        rows_after_native = _OPERATOR_LENGTH_HINT(source, -1)
        # The returned boundary itself accounts for one consumed iterator row.
        if rows_before_native - rows_after_native > 1:
            strategy = "rust_python_hybrid"
        values = _CANONICAL_PREPEND_ROWS_FILTER_BOUNDARY(first_incompatible, source)
        del first_incompatible

    first_incompatible, completed = physical_sink(output, values)
    return first_incompatible, completed, strategy


def _retained_sequence(
    source: object,
) -> list[Any] | tuple[Any, ...] | range | None:
    """Return exact retained data owned by the standard source implementation."""
    if _BUILTIN_TYPE(source) is not Source:
        return None
    return cast(Source[Any], source).retained_sequence()


def _append_python_filter_values(
    output: list[Any],
    source: Iterator[Any],
    predicate: Callable[[Any], Any],
) -> None:
    """Drain one Python predicate into a list with builtin-filter exhaustion semantics."""
    for item in source:
        try:
            if predicate(item):
                output.append(item)
        except StopIteration:
            # CPython's filter treats predicate and truth-test exhaustion as the end of the
            # filter iterator, so the list terminal returns the prefix collected so far.
            del item
            return
        except BaseException:
            del item
            raise
        del item


def _try_direct_i64_expr_filter_list(
    physical: PhysicalPlan,
    pipeline: Pipeline | None,
) -> tuple[bool, list[Any] | None]:
    """Run one scalar expression filter while retaining the selected Python objects."""
    from ..execution.native import (
        direct_i64_filter_list_endpoint,
        direct_i64_filter_list_expression,
    )
    from ..runtime.failpoints import has_active_failpoints
    from ..runtime.report import _record_direct_strategy

    if (
        physical.root is not None
        or pipeline is None
        or physical.parallel is not None
        or pipeline.engine != "auto"
        or has_active_failpoints()
    ):
        return False, None

    operations = operations_from_physical_nodes(physical.nodes)
    if len(operations) != 1 or len(pipeline.operations) != 1:
        return False, None
    raw_operation = operations[0]
    raw_logical = pipeline.operations[0]
    if _BUILTIN_TYPE(raw_operation) is not FilterOp or _BUILTIN_TYPE(raw_logical) is not FilterOp:
        return False, None
    operation = cast(FilterOp, raw_operation)
    logical = cast(FilterOp, raw_logical)
    if _BUILTIN_TYPE(logical.predicate) is not Expr or operation.negate != logical.negate:
        return False, None
    predicate = cast(Expr, logical.predicate)

    source = _retained_sequence(pipeline.source)
    if (
        _BUILTIN_TYPE(source) not in (_BUILTIN_LIST, _BUILTIN_TUPLE)
        or _BUILTIN_LEN(source) < _NATIVE_I64_FILTER_MIN_ITEMS
    ):
        return False, None
    exact_source = cast("list[Any] | tuple[Any, ...]", source)
    payload = physical.backend_payload
    decision = payload.native_decision if isinstance(payload, BackendPayload) else None
    if not isinstance(payload, BackendPayload) or (
        payload.arrow_prefix is not None
        or decision is None
        or decision.engine != "python"
        or decision.reason != _AUTO_I64_EXTERNAL_IDENTITY_REASON
    ):
        return False, None

    expression_plan = direct_i64_filter_list_expression(predicate)
    endpoint = direct_i64_filter_list_endpoint()
    if expression_plan is None or endpoint is None:
        return False, None
    instructions, evaluator = expression_plan
    if operation.predicate is not evaluator:
        return False, None
    if (
        len(pipeline.operations) != 1
        or pipeline.operations[0] is not logical
        or logical.predicate is not predicate
        or _retained_sequence(pipeline.source) is not exact_source
        or has_active_failpoints()
    ):
        return False, None
    output = endpoint(exact_source, instructions, logical.negate)
    if _BUILTIN_TYPE(output) is not _BUILTIN_LIST:
        return False, None
    _record_direct_strategy(
        physical,
        "rust_direct",
        "an exact integer expression filtered a retained list/tuple without re-boxing values",
    )
    return True, cast(list[Any], output)


def _prepend_rows_filter_boundary(first: Any, source: Iterator[Any]) -> Iterator[Any]:
    """Yield a guarded first row without retaining it across the following pull."""
    yield first
    del first
    yield from source


_CANONICAL_PREPEND_ROWS_FILTER_BOUNDARY = _prepend_rows_filter_boundary


def _try_direct_python_filter_list(
    physical: PhysicalPlan,
    pipeline: Pipeline | None,
) -> tuple[bool, list[Any] | None]:
    """Collect one exact Python function filter from a retained builtin sequence."""
    from ..runtime.failpoints import has_active_failpoints

    if (
        physical.root is not None
        or pipeline is None
        or physical.parallel is not None
        or pipeline.engine == "native"
        or not _CPYTHON_312_DIRECT_FILTER_LOOP
        or has_active_failpoints()
    ):
        return False, None

    operations = operations_from_physical_nodes(physical.nodes)
    if (
        len(operations) != 1
        or type(operation := operations[0]) is not FilterOp
        or operation.negate
        or _BUILTIN_TYPE(predicate := operation.predicate) is not FunctionType
        or len(pipeline.operations) != 1
        or pipeline.operations[0] is not operation
    ):
        return False, None

    if _BUILTIN_TYPE(pipeline.source) is not Source:
        return False, None
    source = _retained_sequence(pipeline.source)
    if _BUILTIN_TYPE(source) not in (_BUILTIN_LIST, _BUILTIN_TUPLE):
        return False, None
    source_size = _BUILTIN_LEN(source)

    payload = physical.backend_payload
    if not isinstance(payload, BackendPayload) or (
        payload.arrow_prefix is not None
        or payload.native_decision is None
        or payload.native_decision.engine != "python"
    ):
        return False, None

    output: list[Any] = []
    try:
        with QueryRuntime() as runtime:
            source_iterator = pipeline.source.open()
            live_operations = operations_from_physical_nodes(physical.nodes)
            direct = (
                len(live_operations) == 1
                and live_operations[0] is operation
                and type(operation) is FilterOp
                and not operation.negate
                and operation.predicate is predicate
                and len(pipeline.operations) == 1
                and pipeline.operations[0] is operation
                and _BUILTIN_TYPE(pipeline.source) is Source
                and pipeline.source.retained_sequence() is source
                and _BUILTIN_LEN(source) == source_size
                and _BUILTIN_TYPE(source_iterator) in (_LIST_ITERATOR_TYPE, _TUPLE_ITERATOR_TYPE)
                and not has_active_failpoints()
            )
            if not direct:
                with open_operations(
                    source_iterator,
                    operations,
                    runtime=runtime,
                ) as iterator:
                    try:
                        output.extend(iterator)
                    except BaseException:
                        output.clear()
                        raise
            else:
                with closing_iterators((source_iterator,)):
                    try:
                        _append_python_filter_values(output, source_iterator, predicate)
                    except BaseException:
                        output.clear()
                        raise
    except StopIteration as error:
        output.clear()
        raise RuntimeError("generator raised StopIteration") from error
    except BaseException:
        output.clear()
        raise
    return True, output


def _single_nonnegated_filter(operations: Sequence[Any]) -> FilterOp | None:
    """Return the sole non-negated filter, if the stage has exactly one."""
    if len(operations) != 1:
        return None
    candidate = operations[0]
    if _BUILTIN_TYPE(candidate) is not FilterOp:
        return None
    operation = cast(FilterOp, candidate)
    if operation.negate:
        return None
    return operation


def _first_row_filter_shape(
    source: list[Any] | tuple[Any, ...],
) -> tuple[bool, bool]:
    """Return exact-dict and profitable-native facts without invoking row protocols."""
    try:
        first_row = source[0]
    except IndexError:
        return False, False
    if _BUILTIN_TYPE(first_row) is not dict:
        return False, False
    return True, _BUILTIN_LEN(first_row) <= _NATIVE_I64_ROW_FILTER_MAX_FIELDS


def _try_direct_rows_filter_list(
    physical: PhysicalPlan,
    pipeline: Pipeline | None,
) -> tuple[bool, list[Any] | None]:
    """Drain one large closed RowExpr filter over an exact retained container."""
    from ..runtime.failpoints import has_active_failpoints
    from ..runtime.report import _record_direct_strategy

    if (
        physical.root is not None
        or pipeline is None
        or physical.parallel is not None
        or pipeline.engine != "auto"
        or has_active_failpoints()
    ):
        return False, None
    logical = _single_nonnegated_filter(pipeline.operations)
    if logical is None or not isinstance(logical.predicate, RowExpr):
        return False, None

    if _BUILTIN_TYPE(pipeline.source) is not Source:
        return False, None
    retained = _retained_sequence(pipeline.source)
    if _BUILTIN_TYPE(retained) not in (_BUILTIN_LIST, _BUILTIN_TUPLE):
        return False, None
    source = cast(list[Any] | tuple[Any, ...], retained)
    source_size = _BUILTIN_LEN(source)
    if source_size < _PYTHON_ROWS_FILTER_MIN_ROWS:
        return False, None

    payload = physical.backend_payload
    if not isinstance(payload, BackendPayload) or (
        payload.arrow_prefix is not None
        or payload.native_decision is None
        or payload.native_decision.engine != "python"
    ):
        return False, None
    operations = operations_from_physical_nodes(physical.nodes)
    operation = _single_nonnegated_filter(operations)
    if operation is None or not rows_filter_sink_eligible(operation):
        return False, None

    i64_plan = lower_i64_row_filter(logical)
    native_endpoint = _native_i64_row_filter_endpoint(i64_plan)

    output: list[Any] = []
    try:
        with QueryRuntime() as runtime:
            source_iterator = pipeline.source.open()
            direct = (
                len(pipeline.operations) == 1
                and pipeline.operations[0] is logical
                and not logical.negate
                and isinstance(logical.predicate, RowExpr)
                and _BUILTIN_TYPE(pipeline.source) is Source
                and pipeline.source.retained_sequence() is source
                and _BUILTIN_LEN(source) == source_size
                and _BUILTIN_TYPE(source_iterator) in (_LIST_ITERATOR_TYPE, _TUPLE_ITERATOR_TYPE)
                and not has_active_failpoints()
            )
            if direct:
                exact_first, native_first_row = _first_row_filter_shape(source)
                if not exact_first:
                    with open_operations(source_iterator, operations, runtime=runtime) as iterator:
                        output.extend(iterator)
                    return True, output
                sink = compile_rows_filter_sink(operation)
                if sink is None:
                    with open_operations(source_iterator, operations, runtime=runtime) as iterator:
                        output.extend(iterator)
                    return True, output
                with closing_iterators((source_iterator,)):
                    first_incompatible, completed, strategy = _drain_i64_then_python_row_filter(
                        output,
                        source_iterator,
                        sink,
                        i64_plan if native_first_row else None,
                        native_endpoint,
                    )
                    if completed:
                        _record_direct_strategy(
                            physical,
                            strategy,
                            _ROWS_FILTER_STRATEGY_REASONS[(strategy, True)],
                        )
                        return True, output
                    values = _CANONICAL_PREPEND_ROWS_FILTER_BOUNDARY(
                        first_incompatible, source_iterator
                    )
                    del first_incompatible
                    with open_operations(values, operations, runtime=runtime) as iterator:
                        output.extend(iterator)
                    _record_direct_strategy(
                        physical,
                        strategy,
                        _ROWS_FILTER_STRATEGY_REASONS[(strategy, False)],
                    )
            else:
                with open_operations(source_iterator, operations, runtime=runtime) as iterator:
                    output.extend(iterator)
    except StopIteration as error:
        output.clear()
        raise RuntimeError("generator raised StopIteration") from error
    except BaseException:
        output.clear()
        raise
    return True, output


def _append_flat_map_values(
    output: list[Any],
    source: Iterator[Any],
    function: Callable[[Any], Any],
) -> None:
    """Drain one flat-map directly while preserving general iterable protocols."""
    from ..planning._pair_stages import PairFlatMapDescriptor

    extend = output.extend
    exact_type = _BUILTIN_TYPE
    tuple_type = _BUILTIN_TUPLE
    list_type = _BUILTIN_LIST
    if exact_type(function) is PairFlatMapDescriptor:
        callback = cast(PairFlatMapDescriptor, function).callback
        for pair in source:
            nested = callback(pair[0], pair[1])
            if exact_type(nested) is tuple_type:
                extend(nested)
            else:
                nested_value = None
                for nested_value in nested:
                    output.append(nested_value)
                del nested_value
            del nested
        return

    for item in source:
        nested = function(item)
        if exact_type(nested) is tuple_type:
            extend(nested)
            del nested
            continue
        if exact_type(nested) is list_type:
            extend(nested)
            del nested
            for item in source:
                nested = function(item)
                if exact_type(nested) is tuple_type or exact_type(nested) is list_type:
                    extend(nested)
                else:
                    nested_value = None
                    for nested_value in nested:
                        output.append(nested_value)
                    del nested_value
                del nested
            return
        nested_value = None
        for nested_value in nested:
            output.append(nested_value)
        del nested_value, nested
        for item in source:
            nested = function(item)
            if exact_type(nested) is tuple_type:
                extend(nested)
            else:
                nested_value = None
                for nested_value in nested:
                    output.append(nested_value)
                del nested_value
            del nested
        return


def _try_direct_flat_map_list(
    physical: PhysicalPlan,
    pipeline: Pipeline | None,
) -> tuple[bool, list[Any] | None]:
    """Drain one flat-map into a list without an intermediate generator layer."""
    from ..runtime.failpoints import has_active_failpoints
    from ..tabular.rows import _materialized_row_appender

    if (
        physical.root is not None
        or pipeline is None
        or physical.parallel is not None
        or pipeline.engine != "auto"
        or has_active_failpoints()
    ):
        return False, None
    operations = operations_from_physical_nodes(physical.nodes)
    if len(operations) != 1 or type(operation := operations[0]) is not FlatMapOp:
        return False, None
    payload = physical.backend_payload
    if not isinstance(payload, BackendPayload):
        return False, None
    if (
        payload.arrow_prefix is not None
        or payload.native_decision is None
        or payload.native_decision.engine != "python"
    ):
        return False, None

    output: list[Any] = []
    with QueryRuntime():
        try:
            source_iterator = pipeline.source.open()
        except StopIteration as error:
            raise RuntimeError("generator raised StopIteration") from error
        append_materialized = _materialized_row_appender(operation.function)
        closed_by_context = False

        def locally_owned_source() -> Iterator[Iterator[Any]]:
            if not closed_by_context:
                yield source_iterator

        with closing_iterators(locally_owned_source()):
            try:
                if has_active_failpoints():
                    closed_by_context = True
                    with open_operations(source_iterator, operations) as iterator:
                        output.extend(iterator)
                elif append_materialized is not None:
                    append_materialized(output, source_iterator)
                else:
                    _append_flat_map_values(output, source_iterator, operation.function)
            except StopIteration as error:
                output.clear()
                raise RuntimeError("generator raised StopIteration") from error
            except BaseException:
                output.clear()
                raise
    return True, output


def _try_direct_scan_list(
    physical: PhysicalPlan,
    pipeline: Pipeline | None,
) -> tuple[bool, list[Any] | None]:
    """Accumulate one Python scan directly into its list terminal."""
    from ..runtime.failpoints import has_active_failpoints

    if (
        physical.root is not None
        or pipeline is None
        or physical.parallel is not None
        or pipeline.engine != "auto"
        or has_active_failpoints()
    ):
        return False, None
    operations = operations_from_physical_nodes(physical.nodes)
    if len(operations) != 1 or type(operation := operations[0]) is not ScanOp:
        return False, None

    payload = physical.backend_payload
    if not isinstance(payload, BackendPayload):
        return False, None
    if (
        payload.arrow_prefix is not None
        or payload.native_decision is None
        or payload.native_decision.engine != "python"
    ):
        return False, None

    output: list[Any] = []
    with QueryRuntime() as runtime:
        try:
            source_iterator = pipeline.source.open()
        except StopIteration as error:
            raise RuntimeError("generator raised StopIteration") from error
        if has_active_failpoints():
            with open_operations(
                source_iterator,
                operations,
                runtime=runtime,
            ) as iterator:
                try:
                    output.extend(iterator)
                except BaseException:
                    output.clear()
                    raise
            return True, output

        state = operation.initial
        function = operation.function
        with closing_iterators((source_iterator,)):
            try:
                if function is _OPERATOR_ADD:
                    output = [state := state + item for item in source_iterator]
                else:
                    output = [state := function(state, item) for item in source_iterator]
            except StopIteration as error:
                output.clear()
                raise RuntimeError("generator raised StopIteration") from error
            except BaseException:
                output.clear()
                raise
    return True, output


def _native_drop_nulls_endpoint(
    pipeline: Pipeline,
    source: list[Any] | tuple[Any, ...],
    predicate: object,
) -> tuple[_DropNullsEndpoint | None, str | None]:
    """Resolve one guarded native endpoint without weakening the Python direct sink."""
    if pipeline.engine != "auto" or _BUILTIN_LEN(source) < _NATIVE_ROWS_DROP_NULLS_MIN_ROWS:
        return None, None
    fields = getattr(predicate, "fields", None)
    if _BUILTIN_TYPE(fields) is not _BUILTIN_TUPLE:
        return None, None
    direct_fields = cast(tuple[Any, ...], fields)
    if _BUILTIN_LEN(direct_fields) != 1 or _BUILTIN_TYPE(direct_fields[0]) is not str:
        return None, None
    try:
        from .. import _native
    except ImportError:
        return None, None
    raw_endpoint = getattr(_native, "drop_nulls_exact_dict_prefix_v1", None)
    if not callable(raw_endpoint):
        return None, None
    return cast(_DropNullsEndpoint, raw_endpoint), cast(str, direct_fields[0])


def _invoke_native_drop_nulls_prefix(
    output: list[Any],
    source: Iterator[Any],
    endpoint: _DropNullsEndpoint | None,
    field: str | None,
) -> tuple[Any | None, bool] | None:
    """Run the optional native exact-dictionary prefix."""
    if endpoint is None or field is None:
        return None
    return endpoint(output, source, field)


def _try_direct_drop_nulls_list(
    physical: PhysicalPlan,
    pipeline: Pipeline | None,
) -> tuple[bool, list[Any] | None]:
    """Collect one sealed field-null filter directly from a retained sequence."""
    from ..runtime.failpoints import has_active_failpoints
    from ..runtime.report import _record_direct_strategy
    from ..tabular.rows import _materialized_drop_nulls_appender

    if (
        physical.root is not None
        or pipeline is None
        or physical.parallel is not None
        or has_active_failpoints()
    ):
        return False, None
    operations = operations_from_physical_nodes(physical.nodes)
    if len(operations) != 1 or type(operation := operations[0]) is not FilterOp or operation.negate:
        return False, None
    source = _retained_sequence(pipeline.source)
    if _BUILTIN_TYPE(source) not in (_BUILTIN_LIST, _BUILTIN_TUPLE):
        return False, None

    payload = physical.backend_payload
    if not isinstance(payload, BackendPayload):
        return False, None
    if (
        payload.arrow_prefix is not None
        or payload.native_decision is None
        or payload.native_decision.engine != "python"
    ):
        return False, None
    append_materialized = _materialized_drop_nulls_appender(operation.predicate)
    if append_materialized is None:
        return False, None

    endpoint, field = _native_drop_nulls_endpoint(
        pipeline,
        cast(list[Any] | tuple[Any, ...], source),
        operation.predicate,
    )

    output: list[Any] = []
    try:
        with QueryRuntime() as runtime:
            source_iterator = pipeline.source.open()
            if has_active_failpoints() or _BUILTIN_TYPE(source_iterator) not in (
                _LIST_ITERATOR_TYPE,
                _TUPLE_ITERATOR_TYPE,
            ):
                with open_operations(
                    source_iterator,
                    operations,
                    runtime=runtime,
                ) as iterator:
                    try:
                        output.extend(iterator)
                    except BaseException:
                        output.clear()
                        raise
            else:
                with closing_iterators((source_iterator,)):
                    native = _invoke_native_drop_nulls_prefix(
                        output,
                        source_iterator,
                        endpoint,
                        field,
                    )
                    if native is None:
                        append_materialized(output, source_iterator)
                    else:
                        first_incompatible, completed = native
                        if completed:
                            _record_direct_strategy(
                                physical,
                                "rust_direct",
                                "retained exact dictionary rows were null-filtered by the Rust "
                                "direct sink",
                            )
                        else:
                            _record_direct_strategy(
                                physical,
                                "rust_python_hybrid",
                                "a Rust-filtered exact dictionary prefix continued through Python",
                            )
                            append_materialized(
                                output,
                                itertools.chain((first_incompatible,), source_iterator),
                            )
    except StopIteration as error:
        output.clear()
        raise RuntimeError("generator raised StopIteration") from error
    except BaseException:
        output.clear()
        raise
    return True, output


def _try_direct_rows_map_list(
    physical: PhysicalPlan,
    pipeline: Pipeline,
) -> tuple[bool, list[Any] | None]:
    """Try sealed row-map sinks in semantic priority order."""
    handled, output = _try_direct_rows_unnest_list(physical, pipeline)
    if handled:
        return True, output
    return _try_direct_rows_select_list(physical, pipeline)


def _try_direct_rows_unnest_list(
    physical: PhysicalPlan,
    pipeline: Pipeline | None,
) -> tuple[bool, list[Any] | None]:
    """Materialize one owned unnest transform directly over a retained sequence."""
    from ..runtime.failpoints import has_active_failpoints
    from ..tabular.rows import _materialized_unnest_appender

    if (
        physical.root is not None
        or pipeline is None
        or physical.parallel is not None
        or pipeline.engine != "auto"
        or has_active_failpoints()
    ):
        return False, None
    source = _retained_sequence(pipeline.source)
    if _BUILTIN_TYPE(source) not in (_BUILTIN_LIST, _BUILTIN_TUPLE):
        return False, None
    operations = operations_from_physical_nodes(physical.nodes)
    if len(operations) != 1 or _BUILTIN_TYPE(candidate := operations[0]) is not MapOp:
        return False, None
    operation = cast(MapOp, candidate)
    payload = physical.backend_payload
    if (
        not isinstance(payload, BackendPayload)
        or payload.arrow_prefix is not None
        or payload.native_decision is None
        or payload.native_decision.engine != "python"
    ):
        return False, None

    allow_native = pipeline.engine == "auto"
    append_materialized = _materialized_unnest_appender(operation.function, allow_native)
    if append_materialized is None:
        return False, None

    output: list[Any] = []
    with QueryRuntime():
        try:
            source_iterator = pipeline.source.open()
        except StopIteration as error:
            raise RuntimeError("generator raised StopIteration") from error
        closed_by_context = False

        def locally_owned_source() -> Iterator[Iterator[Any]]:
            if not closed_by_context:
                yield source_iterator

        with closing_iterators(locally_owned_source()):
            try:
                if has_active_failpoints() or _BUILTIN_TYPE(source_iterator) not in (
                    _LIST_ITERATOR_TYPE,
                    _TUPLE_ITERATOR_TYPE,
                ):
                    closed_by_context = True
                    with open_operations(source_iterator, operations) as iterator:
                        output.extend(iterator)
                else:
                    remaining = append_materialized(output, source_iterator)
                    if remaining is not None:
                        closed_by_context = True
                        with open_operations(remaining, operations) as iterator:
                            output.extend(iterator)
            except BaseException:
                output.clear()
                raise
    return True, output


def _try_direct_rows_select_list(
    physical: PhysicalPlan,
    pipeline: Pipeline | None,
) -> tuple[bool, list[Any] | None]:
    """Project one retained direct-field Rows stage in a single guarded native traversal."""
    from ..errors import SelectionError
    from ..runtime.failpoints import has_active_failpoints
    from ..runtime.report import _record_direct_strategy
    from ..tabular.rows import _materialized_select_spec

    if (
        physical.root is not None
        or pipeline is None
        or physical.parallel is not None
        or pipeline.engine != "auto"
        or has_active_failpoints()
    ):
        return False, None
    source = _retained_sequence(pipeline.source)
    if (
        _BUILTIN_TYPE(source) not in (_BUILTIN_LIST, _BUILTIN_TUPLE)
        or _BUILTIN_LEN(source) < _NATIVE_ROWS_SELECT_MIN_ROWS
        or _BUILTIN_TYPE(pipeline.source) is not Source
    ):
        return False, None
    operations = operations_from_physical_nodes(physical.nodes)
    if len(operations) != 1 or _BUILTIN_TYPE(candidate := operations[0]) is not MapOp:
        return False, None
    operation = cast(MapOp, candidate)
    payload = physical.backend_payload
    if (
        not isinstance(payload, BackendPayload)
        or payload.arrow_prefix is not None
        or payload.native_decision is None
        or payload.native_decision.engine != "python"
    ):
        return False, None

    spec = _materialized_select_spec(operation.function)
    if spec is None:
        return False, None

    try:
        from .. import _native
    except ImportError:
        return False, None
    raw_endpoint = getattr(_native, "select_exact_dict_prefix_v1", None)
    if not callable(raw_endpoint):
        return False, None
    endpoint = cast(
        Callable[
            [list[Any], Iterator[Any], tuple[str, ...], tuple[str, ...], type[BaseException]],
            tuple[Any | None, bool] | None,
        ],
        raw_endpoint,
    )
    output_names = _BUILTIN_TUPLE(name for name, _field in spec)
    input_fields = _BUILTIN_TUPLE(field for _name, field in spec)
    output: list[Any] = []

    try:
        with QueryRuntime() as runtime:
            source_iterator = pipeline.source.open()
            with closing_iterators((source_iterator,)):
                native: tuple[Any | None, bool] | None = None
                if not has_active_failpoints() and _BUILTIN_TYPE(source_iterator) in (
                    _LIST_ITERATOR_TYPE,
                    _TUPLE_ITERATOR_TYPE,
                ):
                    try:
                        native = endpoint(
                            output,
                            source_iterator,
                            output_names,
                            input_fields,
                            SelectionError,
                        )
                    except StopIteration:
                        _record_direct_strategy(
                            physical,
                            "rust_direct",
                            "retained exact dictionary rows were projected by the Rust direct sink",
                        )
                        return True, output

                if native is not None:
                    first_incompatible, completed = native
                    if completed:
                        _record_direct_strategy(
                            physical,
                            "rust_direct",
                            "retained exact dictionary rows were projected by the Rust direct sink",
                        )
                        return True, output
                    _record_direct_strategy(
                        physical,
                        "rust_python_hybrid",
                        "a Rust-projected exact dictionary prefix continued through Python",
                    )
                    values: Iterator[Any] = itertools.chain((first_incompatible,), source_iterator)
                else:
                    values = source_iterator
                with open_operations(values, operations, runtime=runtime) as iterator:
                    output.extend(iterator)

    except StopIteration as error:
        output.clear()
        raise RuntimeError("generator raised StopIteration") from error
    except BaseException:
        output.clear()
        raise
    return True, output


def _retained_grouping_operation(
    physical: PhysicalPlan,
    pipeline: Pipeline | None,
) -> tuple[list[Any] | tuple[Any, ...] | range, ChunkOp | WindowOp] | None:
    """Return one callback-free grouping operation over an exact retained sequence."""
    from ..runtime.failpoints import has_active_failpoints

    payload = physical.backend_payload
    python_selected = isinstance(payload, BackendPayload) and (
        payload.arrow_prefix is None
        and payload.native_decision is not None
        and payload.native_decision.engine == "python"
    )
    if (
        physical.root is not None
        or pipeline is None
        or physical.parallel is not None
        or has_active_failpoints()
        or not python_selected
    ):
        return None
    operations = operations_from_physical_nodes(physical.nodes)
    if len(operations) != 1:
        return None
    operation = operations[0]
    if type(operation) not in (ChunkOp, WindowOp):
        return None
    operation = cast("ChunkOp | WindowOp", operation)
    if type(operation.size) is not int or operation.size > sys.maxsize:
        return None
    if type(operation) is WindowOp:
        step = operation.step
        if type(step) is not int:
            return None
    source = _retained_sequence(pipeline.source)
    if source is None:
        return None
    return source, operation


def _materialize_retained_grouping(
    source: list[Any] | tuple[Any, ...] | range,
    operation: ChunkOp | WindowOp,
) -> list[Any]:
    """Use exact sequence slices without callback, iterator, or protocol speculation."""
    if type(operation) is ChunkOp:
        if _BATCHED is not None and type(source) is _BUILTIN_LIST:
            return _BUILTIN_LIST(_BATCHED(source, operation.size))
        return [
            tuple(source[index : index + operation.size])
            for index in range(0, len(source), operation.size)
        ]
    operation = cast(WindowOp, operation)
    if not source:
        return []
    if len(source) < operation.size:
        return [tuple(source)]
    return [
        tuple(source[index : index + operation.size])
        for index in range(0, len(source) - operation.size + 1, operation.step)
    ]


def try_direct_retained_sequence_window(
    physical: PhysicalPlan,
    pipeline: Pipeline | None,
    target: str,
) -> tuple[bool, Any | None]:
    """Fold a pure bounds chain into one exact built-in sequence slice."""
    from ..runtime.failpoints import has_active_failpoints

    if (
        physical.root is not None
        or pipeline is None
        or physical.parallel is not None
        or pipeline.engine == "native"
        or has_active_failpoints()
        or target not in {"list", "tuple"}
        or not pipeline.operations
    ):
        return False, None

    start = 0
    window_operations: list[DropOp | TakeOp] = []
    for raw_operation in pipeline.operations:
        if type(raw_operation) not in (DropOp, TakeOp):
            return False, None
        operation = cast("DropOp | TakeOp", raw_operation)
        if type(operation.count) is not int or operation.count > sys.maxsize:
            return False, None
        window_operations.append(operation)

    source = _retained_sequence(pipeline.source)
    if source is None:
        return False, None
    stop = _BUILTIN_LEN(source)
    for operation in window_operations:
        if type(operation) is DropOp:
            start += operation.count
            if start > stop:
                start = stop
        else:
            candidate = start + operation.count
            if candidate < stop:
                stop = candidate

    window = source[start:stop]
    if target == "list":
        return True, window if type(window) is list else _BUILTIN_LIST(window)
    return True, window if type(window) is tuple else _BUILTIN_TUPLE(window)


def _try_direct_filter_list(
    physical: PhysicalPlan,
    pipeline: Pipeline,
    operation: FilterOp,
) -> tuple[bool, list[Any] | None]:
    """Route one filter shape to its smallest semantics-preserving direct sink."""
    if isinstance(operation.predicate, RowExpr) and not operation.negate:
        return _try_direct_rows_filter_list(physical, pipeline)
    if type(operation.predicate) is Expr:
        return _try_direct_i64_expr_filter_list(physical, pipeline)
    if type(operation.predicate) is FunctionType and not operation.negate:
        if not _CPYTHON_312_DIRECT_FILTER_LOOP:
            return False, None
        return _try_direct_python_filter_list(physical, pipeline)
    return _try_direct_drop_nulls_list(physical, pipeline)


def try_direct_python_structural_list(
    physical: PhysicalPlan,
    pipeline: Pipeline | None,
) -> tuple[bool, list[Any] | None]:
    """Dispatch only operation families with a proved direct structural list sink."""
    if pipeline is None or not pipeline.operations:
        return False, None
    operation = pipeline.operations[0]
    operation_type = type(operation)
    if operation_type in (TakeOp, DropOp):
        windowed, output = try_direct_retained_sequence_window(physical, pipeline, "list")
        if windowed:
            return True, output
        return False, None
    if operation_type is UniqueOp:
        return _flow_unique_list.try_direct_unique_list(physical, pipeline)
    if len(pipeline.operations) != 1:
        return False, None
    if operation_type is MapOp:
        return _try_direct_rows_map_list(physical, pipeline)
    if operation_type is FilterOp:
        return _try_direct_filter_list(physical, pipeline, cast(FilterOp, operation))
    if operation_type is FlatMapOp:
        return _try_direct_flat_map_list(physical, pipeline)
    if operation_type is ScanOp:
        return _try_direct_scan_list(physical, pipeline)
    if operation_type in (ChunkOp, WindowOp):
        grouping = _retained_grouping_operation(physical, pipeline)
        if grouping is not None:
            return True, _materialize_retained_grouping(*grouping)
    return False, None
