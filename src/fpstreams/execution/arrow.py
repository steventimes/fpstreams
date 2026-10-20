"""Execute guarded RowExpr map/filter prefixes over Arrow batches."""

from __future__ import annotations

import sys
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, cast
from typing import Literal as TypeLiteral

from ..expressions.row_ir import (
    Binary,
    Call,
    Cast,
    Coalesce,
    Field,
    IsNull,
    Literal,
    PythonUDF,
    Unary,
)
from ..planning.arrow import (
    ArrowPrefixPlan,
    ArrowProjectionSpec,
    _direct_primitive_filter,
    _direct_projection,
    _projection_is_current,
    direct_exact_equality,
    direct_exact_i64_range,
    plan_arrow_prefix,
    supports_arrow_table_materialization,
)
from ..planning.arrow_source import ArrowBatchSource, RangePredicate, batch_to_rows
from ..planning.logical import Pipeline
from ..planning.source import Source
from ..planning.sync import FilterOp, MapOp
from ..runtime.iterators import closing_iterators
from ..runtime.query import QueryRuntime
from . import arrow_numeric as _arrow_numeric
from . import arrow_sort as _arrow_sort
from .arrow_numeric import ArrowNumericHooks
from .arrow_sort import ArrowSortHooks
from .sync import execute_operations

_ARROW_NUMERIC_HOOKS = cast(ArrowNumericHooks, sys.modules[__name__])
_ARROW_SORT_HOOKS = cast(ArrowSortHooks, sys.modules[__name__])

_EXPECTED_ARROW_ERRORS = (ArithmeticError, NotImplementedError, TypeError, ValueError)
_ARROW_PYTHON_EXTREME_MAX_ROWS = 128
_ARROW_REDUCTION_MISSING = object()
_COMBINED_FILTER_MIN_ROWS = 0
_COMBINED_FILTER_MIN_COLUMNS = 1


class BatchFallbackReason(StrEnum):
    """Classify why a batch must use row-wise Python instead of Arrow kernels."""

    OPAQUE_EXPRESSION = "opaque_expression"
    MISSING_FIELD = "missing_field"
    INCOMPATIBLE_TYPE = "incompatible_type"
    NULL_SEMANTICS = "null_semantics"
    INTEGER_OVERFLOW = "integer_overflow"
    ZERO_DIVISOR = "zero_divisor"
    UNSAFE_CAST = "unsafe_cast"
    KERNEL_ERROR = "kernel_error"


@dataclass(frozen=True, slots=True)
class BatchSafety:
    """Record a batch-kernel safety verdict and its fallback reason, if unsafe."""

    safe: bool
    reason: BatchFallbackReason | None = None


@dataclass(frozen=True, slots=True)
class ArrowI64Reduction:
    """Carry one exact reduction while distinguishing empty input from a null value."""

    seen: bool
    value: int | None
    source_value_error: ValueError | None = None


def _arrow_modules() -> tuple[Any, Any]:
    """Import optional PyArrow modules only after an Arrow path is selected."""
    import pyarrow as pa  # type: ignore[import-untyped]
    import pyarrow.compute as pc  # type: ignore[import-untyped]

    return pa, pc


def _expr_of(operation: Any) -> Any | None:
    """Return the private row-expression node carried by a map or filter."""
    candidate = getattr(operation, "function", getattr(operation, "predicate", None))
    return getattr(candidate, "_node", None)


def _nodes(root: Any) -> Iterator[Any]:
    """Walk an expression tree depth-first in left-to-right operand order."""
    stack = [root]
    while stack:
        node = stack.pop()
        yield node
        if isinstance(node, Binary):
            stack.extend((node.right, node.left))
        elif isinstance(node, Unary):
            stack.append(node.operand)
        elif isinstance(node, (Cast, IsNull)):
            stack.append(node.value)
        elif isinstance(node, Coalesce):
            stack.extend(reversed(node.values))
        elif isinstance(node, (Call, PythonUDF)):
            stack.extend(reversed(node.arguments))


def _has_primitive_builtin_schema(pa: Any, schema: Any) -> bool:
    """Return whether validated fields convert only to built-in scalars and lists."""
    return arrow_schema_has_primitive_rows(pa, schema)


def _retained_arrow_stable_sort_result(plan: Any) -> tuple[Any, ArrowBatchSource] | None:
    """Return one guarded stable Arrow sort result before Python row conversion."""
    return _arrow_sort._retained_arrow_stable_sort_result(plan, hooks=_ARROW_SORT_HOOKS)


def _retained_arrow_sorted_batches(ordered: Any, descriptor: ArrowBatchSource) -> Iterator[Any]:
    """Yield one ordered retained result in the adapter's canonical batch bounds."""
    return _arrow_sort._retained_arrow_sorted_batches(ordered, descriptor, hooks=_ARROW_SORT_HOOKS)


def materialize_retained_arrow_stable_sort(plan: Any) -> list[Any] | None:
    """Collect a guarded retained Arrow sort without per-row generator forwarding."""
    return _arrow_sort.materialize_retained_arrow_stable_sort(plan, hooks=_ARROW_SORT_HOOKS)


def try_retained_arrow_stable_sort(plan: Any) -> Iterator[Any] | None:
    """Sort one proven retained Arrow source without first constructing row dictionaries."""
    return _arrow_sort.try_retained_arrow_stable_sort(plan, hooks=_ARROW_SORT_HOOKS)


def _projection_safety(
    pa: Any,
    schema: Any,
    names: set[str],
    projection: ArrowProjectionSpec | None,
) -> BatchSafety:
    """Guard an optional direct projection with field and whole-schema proofs."""
    if projection is None:
        return BatchSafety(True)
    if any(name not in names for name in projection.inputs):
        return BatchSafety(False, BatchFallbackReason.MISSING_FIELD)
    if not _has_primitive_builtin_schema(pa, schema):
        return BatchSafety(False, BatchFallbackReason.INCOMPATIBLE_TYPE)
    return BatchSafety(True)


def _direct_filter_safety(pa: Any, batch: Any, operation: Any) -> BatchSafety:
    """Prove that one closed field/literal comparison has Python-equivalent scalar types."""
    if not _direct_primitive_filter(operation):
        return BatchSafety(False, BatchFallbackReason.INCOMPATIBLE_TYPE)
    root = _expr_of(operation)
    if not isinstance(root, Binary):
        return BatchSafety(False, BatchFallbackReason.INCOMPATIBLE_TYPE)
    if isinstance(root.left, Field):
        field, literal = root.left, root.right
    else:
        field, literal = root.right, root.left
    field_index = batch.schema.get_field_index(field.name)
    if field_index < 0:
        return BatchSafety(False, BatchFallbackReason.MISSING_FIELD)
    column = batch.column(field_index)
    if getattr(column, "null_count", 0):
        return BatchSafety(False, BatchFallbackReason.NULL_SEMANTICS)
    arrow_type = batch.schema.field(field_index).type
    value_type = type(literal.value)
    compatible = (
        (value_type is int and pa.types.is_int64(arrow_type))
        or (value_type is bool and pa.types.is_boolean(arrow_type))
        or (
            value_type is str
            and (pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type))
        )
        or (
            value_type is bytes
            and (
                pa.types.is_binary(arrow_type)
                or pa.types.is_large_binary(arrow_type)
                or pa.types.is_fixed_size_binary(arrow_type)
            )
        )
    )
    return (
        BatchSafety(True)
        if compatible
        else BatchSafety(False, BatchFallbackReason.INCOMPATIBLE_TYPE)
    )


def _direct_filter_program(operations: tuple[Any, ...]) -> bool:
    """Recognize a nonempty sequence of side-effect-free primitive comparisons."""
    return bool(operations) and all(_direct_primitive_filter(operation) for operation in operations)


def _projection_pipeline_safety(
    pa: Any,
    batch: Any,
    operations: tuple[Any, ...],
    projection: ArrowProjectionSpec | None,
) -> BatchSafety:
    """Guard a direct primitive filter, with or without a terminal projection."""
    if not operations:
        return BatchSafety(True)
    if _direct_filter_program(operations):
        for operation in operations:
            safety = _direct_filter_safety(pa, batch, operation)
            if not safety.safe:
                return safety
        return BatchSafety(True)
    if projection is not None or _direct_primitive_filter(operations[0]):
        return _direct_filter_safety(pa, batch, operations[0])
    return BatchSafety(True)


def _batch_schema_context(batch: Any) -> tuple[Any, Any, set[str]] | None:
    """Load Arrow schema helpers while preserving resource exhaustion."""
    try:
        pa, _pc = _arrow_modules()
        schema = batch.schema
        return pa, schema, set(schema.names)
    except MemoryError:
        raise
    except Exception:
        return None


def prove_batch_safe(
    batch: Any,
    operations: tuple[Any, ...],
    *,
    projection: ArrowProjectionSpec | None = None,
) -> BatchSafety:
    """Conservatively decide whether Arrow may evaluate every operation on a batch.

    The guard rejects opaque or unsupported nodes, missing fields, unhandled nulls,
    literal division by zero, and unsupported casts. Import or schema inspection
    failures are treated as kernel failures; runtime kernel errors still trigger
    the row-wise fallback in execute_arrow_prefix.
    """
    context = _batch_schema_context(batch)
    if context is None:
        return BatchSafety(False, BatchFallbackReason.KERNEL_ERROR)
    _pa, schema, names = context
    projection_safety = _projection_safety(_pa, schema, names, projection)
    if not projection_safety.safe:
        return projection_safety
    pipeline_safety = _projection_pipeline_safety(_pa, batch, operations, projection)
    if not pipeline_safety.safe:
        return pipeline_safety
    allowed_binary = {"+", "-", "*", "/", "==", "!=", "<", "<=", ">", ">=", "and", "or"}
    for operation in operations:
        root = _expr_of(operation)
        if root is None:
            return BatchSafety(False, BatchFallbackReason.OPAQUE_EXPRESSION)
        for node in _nodes(root):
            if isinstance(node, PythonUDF):
                return BatchSafety(False, BatchFallbackReason.INCOMPATIBLE_TYPE)
            if isinstance(node, Field) and ("." in node.name or node.name not in names):
                return BatchSafety(False, BatchFallbackReason.MISSING_FIELD)
            if isinstance(node, Field):
                column = batch.column(batch.schema.get_field_index(node.name))
                if getattr(column, "null_count", 0) and not any(
                    isinstance(parent, (IsNull, Coalesce)) for parent in _nodes(root)
                ):
                    return BatchSafety(False, BatchFallbackReason.NULL_SEMANTICS)
            if isinstance(node, Binary) and node.kind not in allowed_binary:
                return BatchSafety(False, BatchFallbackReason.INCOMPATIBLE_TYPE)
            if (
                isinstance(node, Binary)
                and node.kind == "/"
                and isinstance(node.right, Literal)
                and node.right.value == 0
            ):
                return BatchSafety(False, BatchFallbackReason.ZERO_DIVISOR)
            if isinstance(node, (Call,)):
                return BatchSafety(False, BatchFallbackReason.INCOMPATIBLE_TYPE)
            if isinstance(node, Cast) and node.target not in (int, float, bool):
                return BatchSafety(False, BatchFallbackReason.UNSAFE_CAST)
    return BatchSafety(True)


def lower_row_expression(node: Any, batch: Any) -> Any:
    """Evaluate a supported row-expression node as an Arrow scalar or array."""
    pa, pc = _arrow_modules()
    if isinstance(node, Field):
        return batch.column(batch.schema.get_field_index(node.name))
    if isinstance(node, Literal):
        return pa.scalar(node.value)
    if isinstance(node, Binary):
        left, right = (
            lower_row_expression(node.left, batch),
            lower_row_expression(node.right, batch),
        )
        if node.kind == "and":
            return pc.and_kleene(left, right)
        if node.kind == "or":
            return pc.or_kleene(left, right)
        return {
            "+": pc.add,
            "-": pc.subtract,
            "*": pc.multiply,
            "/": pc.divide,
            "==": pc.equal,
            "!=": pc.not_equal,
            "<": pc.less,
            "<=": pc.less_equal,
            ">": pc.greater,
            ">=": pc.greater_equal,
        }[node.kind](left, right)
    if isinstance(node, Unary):
        value = lower_row_expression(node.operand, batch)
        return (
            pc.invert(value)
            if node.kind == "not"
            else pc.negate(value)
            if node.kind == "neg"
            else pc.abs(value)
        )
    if isinstance(node, IsNull):
        result = pc.is_null(lower_row_expression(node.value, batch))
        return pc.invert(result) if node.negate else result
    if isinstance(node, Coalesce):
        result = lower_row_expression(node.values[-1], batch)
        for child in reversed(node.values[:-1]):
            result = pc.coalesce(lower_row_expression(child, batch), result)
        return result
    if isinstance(node, Cast):
        value = lower_row_expression(node.value, batch)
        target = {int: pa.int64(), float: pa.float64(), bool: pa.bool_()}[node.target]  # type: ignore[index]
        return pc.cast(value, target)
    raise ValueError(f"unsupported Arrow row node: {type(node).__name__}")


def _can_execute_batch_program(
    operations: tuple[Any, ...], projection: ArrowProjectionSpec | None
) -> bool:
    """Accept one expression stage or a proven primitive filter sequence.

    Combining RowExpr stages requires a substantially stronger proof for Python overflow,
    division, null, cast, and literal-protocol semantics. Only direct primitive comparisons may
    form a multi-stage program; all other multi-stage shapes stay on the canonical row path.
    """
    if projection is not None:
        if not operations or _direct_projection(operations[-1]) != projection:
            return False
        return isinstance(operations[-1], MapOp) and all(
            _direct_primitive_filter(operation) for operation in operations[:-1]
        )
    return (len(operations) == 1 and isinstance(operations[0], (FilterOp, MapOp))) or (
        len(operations) > 1 and _direct_filter_program(operations)
    )


def _project_batch_rows(batch: Any, projection: ArrowProjectionSpec) -> Iterator[dict[str, Any]]:
    """Convert each unique input once, then rebuild aliases in declaration order."""
    rows = batch.select(list(projection.inputs)).to_pylist()
    direct = projection.selectors == tuple((name, name) for name in projection.inputs)
    if direct:
        yield from rows
        return
    for row in rows:
        yield {output: row[source] for output, source in projection.selectors}


def _combined_filter_mask(
    batch: Any,
    operations: tuple[Any, ...],
    projection: ArrowProjectionSpec | None,
) -> Any | None:
    """Share one selection mask across already-proven primitive comparisons.

    Callers retain the source-conversion and batch-safety guards. Only closed,
    non-null primitive comparisons can be evaluated on the original batch; an
    opaque callback or a potentially failing expression keeps sequential filtering.
    """
    filter_count = len(operations) - (projection is not None)
    if (
        filter_count < 2
        or batch.num_rows == 0
        or batch.num_rows < _COMBINED_FILTER_MIN_ROWS
        or batch.num_columns < _COMBINED_FILTER_MIN_COLUMNS
    ):
        return None
    filters = operations[:filter_count]
    if not _direct_filter_program(filters):
        return None
    _pa, pc = _arrow_modules()
    mask = lower_row_expression(_expr_of(filters[0]), batch)
    for operation in filters[1:]:
        mask = pc.and_(mask, lower_row_expression(_expr_of(operation), batch))
    return mask


def _execute_batch_program(
    batch: Any,
    operations: tuple[Any, ...],
    projection: ArrowProjectionSpec | None,
) -> Iterator[Any]:
    """Apply a shape-preserving Arrow filter sequence and an optional final map."""
    _pa, pc = _arrow_modules()
    current = batch
    mask = _combined_filter_mask(batch, operations, projection)
    if mask is not None:
        current = pc.filter(batch, mask)
        operations = operations[-1:] if projection is not None else ()
    for index, operation in enumerate(operations):
        if projection is not None and index == len(operations) - 1:
            yield from _project_batch_rows(current, projection)
            return
        result = lower_row_expression(_expr_of(operation), current)
        if isinstance(operation, FilterOp):
            current = pc.filter(current, result)
            continue
        if isinstance(operation, MapOp) and index == len(operations) - 1:
            yield from result.to_pylist()
            return
        raise ValueError("Arrow map must be the final shape-changing stage")
    yield from batch_to_rows(current)


def _projection_scan_columns(
    prefix: ArrowPrefixPlan, *, descriptor: ArrowBatchSource | None = None
) -> tuple[str, ...] | None:
    """Return source fields needed by a closed projection and its optional filter."""
    if prefix.projection is None:
        return None
    if descriptor is not None and not descriptor.projection_safe:
        # A batch opener can change the accessor. Dropped fields cannot be recovered
        # when that batch needs Python fallback, even if the opener is reusable.
        return None
    required = list(prefix.projection.inputs)
    seen = set(required)
    for operation in prefix.operations[:-1]:
        for node in _nodes(_expr_of(operation)):
            if isinstance(node, Field) and node.name not in seen:
                seen.add(node.name)
                required.append(node.name)
    return tuple(required)


def _count_scan_columns(
    prefix: ArrowPrefixPlan, *, descriptor: ArrowBatchSource | None = None
) -> tuple[str, ...] | None:
    """Return direct source fields needed to evaluate a count's complete batch program."""
    if prefix.projection is not None and descriptor is not None and not descriptor.projection_safe:
        return None
    required: list[str] = []
    seen: set[str] = set()
    if prefix.projection is not None:
        for name in prefix.projection.inputs:
            if name not in seen:
                seen.add(name)
                required.append(name)
    for operation in prefix.operations:
        for node in _nodes(_expr_of(operation)):
            if isinstance(node, Field):
                if "." in node.name:
                    return None
                if node.name not in seen:
                    seen.add(node.name)
                    required.append(node.name)
    return tuple(required) if required else None


def _scan_equality(prefix: ArrowPrefixPlan) -> tuple[str, object] | None:
    """Return one exact built-in equality that a guarded source may use as an I/O hint."""
    operations = prefix.operations
    if (
        not operations
        or not isinstance(operations[0], FilterOp)
        or not _can_execute_batch_program(operations, prefix.projection)
    ):
        return None
    return direct_exact_equality(operations[0])


def _scan_range_predicate(prefix: ArrowPrefixPlan) -> RangePredicate | None:
    """Return one exact i64 range that a guarded source may use as an I/O hint."""
    operations = prefix.operations
    if (
        not operations
        or not isinstance(operations[0], FilterOp)
        or not _can_execute_batch_program(operations, prefix.projection)
    ):
        return None
    return direct_exact_i64_range(operations[0])


def _execute_first_batch_program(
    batch: Any,
    operations: tuple[Any, ...],
    projection: ArrowProjectionSpec | None,
) -> tuple[bool, Any | None]:
    """Run a proven complete prefix and box at most its first surviving result."""
    _pa, pc = _arrow_modules()
    current = batch
    for index, operation in enumerate(operations):
        if projection is not None and index == len(operations) - 1:
            if current.num_rows == 0:
                return False, None
            return True, next(_project_batch_rows(current.slice(0, 1), projection))
        result = lower_row_expression(_expr_of(operation), current)
        if isinstance(operation, FilterOp):
            current = pc.filter(current, result)
            continue
        raise ValueError("early Arrow map must be a direct projection")
    if current.num_rows == 0:
        return False, None
    return True, batch_to_rows(current.slice(0, 1))[0]


def _execute_first_python_batch(
    batch: Any,
    operations: tuple[Any, ...],
    *,
    rows: list[Any] | None = None,
) -> tuple[bool, Any | None]:
    """Apply a guarded batch prefix row-by-row until its first accepted result."""
    for item in batch_to_rows(batch) if rows is None else rows:
        current = item
        accepted = True
        for operation in operations:
            if isinstance(operation, MapOp):
                current = operation.function(current)
            elif bool(operation.predicate(current)) is operation.negate:
                accepted = False
                break
        if accepted:
            return True, current
    return False, None


def _concat_record_batches(pa: Any, batches: list[Any]) -> Any:
    """Concatenate batches once, including on Arrow releases without ``concat_batches``."""
    concat_batches = getattr(pa, "concat_batches", None)
    if callable(concat_batches):
        return concat_batches(batches)
    table = pa.Table.from_batches(batches).combine_chunks()
    return table.to_batches()[0]


def _rechunk_batches(pa: Any, batches: Iterator[Any], batch_size: int) -> Iterator[Any]:
    """Reblock native batches with at most one output chunk of buffered columnar data."""
    iterator = iter(batches)
    pending: list[Any] = []
    pending_rows = 0
    with closing_iterators((iterator,)):
        for batch in iterator:
            offset = 0
            rows = batch.num_rows
            if pending_rows:
                taken = min(batch_size - pending_rows, rows)
                if taken:
                    pending.append(batch.slice(0, taken))
                    pending_rows += taken
                    offset = taken
                if pending_rows == batch_size:
                    yield (pending[0] if len(pending) == 1 else _concat_record_batches(pa, pending))
                    pending.clear()
                    pending_rows = 0
            while rows - offset >= batch_size:
                yield batch.slice(offset, batch_size)
                offset += batch_size
            if offset < rows:
                pending.append(batch.slice(offset))
                pending_rows = rows - offset
        if pending:
            yield pending[0] if len(pending) == 1 else _concat_record_batches(pa, pending)


def _execute_python_batch(
    batch: Any, operations: tuple[Any, ...], *, rows: list[Any] | None = None
) -> list[Any]:
    """Run one unsafe Arrow batch through the established Python row operations."""
    return list(
        execute_operations(iter(batch_to_rows(batch) if rows is None else rows), operations)
    )


def _count_python_batch(batch: Any, operations: tuple[Any, ...]) -> int:
    """Count one unsafe batch through the established Python row operations."""
    return len(_execute_python_batch(batch, operations))


def _count_batch_program(
    batch: Any,
    operations: tuple[Any, ...],
    projection: ArrowProjectionSpec | None,
) -> int:
    """Evaluate one proven complete batch program and return its output cardinality."""
    _pa, pc = _arrow_modules()
    mask = _combined_filter_mask(batch, operations, projection)
    if mask is not None:
        return int(pc.sum(mask).as_py())
    current = batch
    for index, operation in enumerate(operations):
        if projection is not None and index == len(operations) - 1:
            return int(current.num_rows)
        result = lower_row_expression(_expr_of(operation), current)
        if isinstance(operation, FilterOp):
            current = pc.filter(current, result)
            continue
        if isinstance(operation, MapOp) and index == len(operations) - 1:
            return int(current.num_rows)
        raise ValueError("Arrow map must be the final shape-changing stage")
    return int(current.num_rows)


def try_arrow_count(
    plan: Pipeline,
    *,
    prefix: ArrowPrefixPlan | None = None,
) -> tuple[bool, int]:
    """Count one complete guarded Arrow program without materializing accepted rows."""
    prefix = plan_arrow_prefix(plan) if prefix is None else prefix
    if (
        prefix is None
        or prefix.operation_count != len(plan.operations)
        or not _projection_is_current(prefix)
    ):
        return False, 0

    from ..runtime.failpoints import has_active_failpoints

    if has_active_failpoints():
        values = execute_operations(plan.source.open(), plan.operations)
        with closing_iterators((values,)):
            return True, sum(1 for _value in values)

    descriptor = plan.source.native_data
    if not isinstance(descriptor, ArrowBatchSource):
        return False, 0
    equality = _scan_equality(prefix) if descriptor.kind == "parquet" else None
    range_predicate = _scan_range_predicate(prefix) if descriptor.kind == "parquet" else None
    columns = _count_scan_columns(prefix, descriptor=descriptor)
    expression_operations = (
        prefix.operations[:-1] if prefix.projection is not None else prefix.operations
    )

    plan.source.open_native(ArrowBatchSource)
    if not _projection_is_current(prefix):
        columns = None
    if not prefix.operations and descriptor.count_opener is not None:
        count = descriptor.count_opener()
        if count is not None:
            return True, count
    batches = descriptor.open_batches(
        columns=columns,
        projection_check=lambda: _projection_is_current(prefix),
        equality=equality,
        range_predicate=range_predicate,
    )
    total = 0
    with closing_iterators((batches,)):
        for batch in batches:
            rows = observe_arrow_batch_rows(batch)
            if rows is not None:
                total += sum(1 for _ in execute_operations(iter(rows), prefix.operations))
                continue
            if prefix.operation_count == 0:
                total += batch.num_rows
                continue
            safety = prove_batch_safe(
                batch,
                expression_operations,
                projection=prefix.projection,
            )
            if safety.safe and _can_execute_batch_program(prefix.operations, prefix.projection):
                try:
                    total += _count_batch_program(batch, prefix.operations, prefix.projection)
                    continue
                except _EXPECTED_ARROW_ERRORS:
                    pass
            total += _count_python_batch(batch, prefix.operations)
    return True, total


def _can_materialize_table_program(prefix: ArrowPrefixPlan) -> bool:
    """Accept native identity, safe filter-only, or direct projection programs."""
    return supports_arrow_table_materialization(prefix)


def _native_batches_match_size(batches: list[Any], batch_size: int) -> bool:
    """Return whether native batch boundaries already match canonical terminal chunks."""
    return (
        bool(batches)
        and all(batch.num_rows == batch_size for batch in batches[:-1])
        and 0 < batches[-1].num_rows <= batch_size
    )


def _identity_table(
    pa: Any,
    batches: list[Any],
    *,
    schema: Any | None,
    batch_size: int,
) -> Any:
    """Preserve a native identity schema while enforcing terminal batch boundaries."""
    if not batches:
        return pa.Table.from_batches([], schema=schema) if schema is not None else pa.table({})
    table = pa.Table.from_batches(batches, schema=schema)
    if _native_batches_match_size(batches, batch_size):
        return table
    table = table.combine_chunks()
    return pa.Table.from_batches(
        table.to_batches(max_chunksize=batch_size),
        schema=table.schema,
    )


def _canonical_arrow_type(pa: Any, value_type: Any) -> bool:
    """Return whether Python-row inference recreates this primitive Arrow type exactly."""
    types = pa.types
    return bool(
        types.is_null(value_type)
        or types.is_boolean(value_type)
        or types.is_int64(value_type)
        or types.is_float64(value_type)
        or types.is_string(value_type)
        or types.is_binary(value_type)
    )


def _table_output_types_are_canonical(
    pa: Any,
    batch: Any,
    projection: ArrowProjectionSpec | None,
) -> bool:
    """Guard physical dtypes that survive the existing Python-row Arrow round trip."""
    names = (
        batch.schema.names
        if projection is None
        else [source for _output, source in projection.selectors]
    )
    return all(_canonical_arrow_type(pa, batch.schema.field(name).type) for name in names)


def retained_table_rows_are_canonical(table: Any) -> bool:
    """Prove full retained-table row conversion is valid and round-trip canonical."""
    pa, _pc = _arrow_modules()
    try:
        if not all(_canonical_arrow_type(pa, field.type) for field in table.schema):
            return False
        table.validate(full=True)
    except MemoryError:
        raise
    except _EXPECTED_ARROW_ERRORS:
        return False
    return True


def retained_table_rows_are_valid(table: Any) -> bool:
    """Prove a retained primitive table crosses the canonical Python row boundary."""
    pa, _pc = _arrow_modules()
    if not arrow_schema_has_primitive_rows(pa, table.schema):
        return False
    try:
        table.validate(full=True)
    except MemoryError:
        raise
    except _EXPECTED_ARROW_ERRORS:
        return False
    return True


def arrow_schema_has_primitive_rows(pa: Any, schema: Any) -> bool:
    """Prove scalar/list conversion without extension, struct, map, or temporal hooks.

    The caller still validates all buffers before projecting away an unused field. A list
    is safe only when its entire child type tree has the same closed built-in proof.
    """
    types = pa.types
    pending = [field.type for field in schema]
    while pending:
        value_type = pending.pop()
        if (
            types.is_dictionary(value_type)
            or types.is_list(value_type)
            or types.is_large_list(value_type)
            or types.is_fixed_size_list(value_type)
        ):
            pending.append(value_type.value_type)
        elif not (
            types.is_null(value_type)
            or types.is_boolean(value_type)
            or types.is_integer(value_type)
            or types.is_floating(value_type)
            or types.is_string(value_type)
            or types.is_large_string(value_type)
            or types.is_binary(value_type)
            or types.is_large_binary(value_type)
            or types.is_fixed_size_binary(value_type)
        ):
            return False
    return True


def arrow_batch_rows_are_valid(batch: Any) -> bool:
    """Prove one primitive Arrow batch can cross the canonical Python row boundary."""
    pa, _pc = _arrow_modules()
    if not arrow_schema_has_primitive_rows(pa, batch.schema):
        return False
    try:
        batch.validate(full=True)
    except MemoryError:
        raise
    except _EXPECTED_ARROW_ERRORS:
        return False
    return True


def observe_arrow_batch_rows(batch: Any) -> list[dict[str, Any]] | None:
    """Observe the canonical row boundary before a kernel can discard source values.

    Full Arrow validation keeps ordinary primitive batches columnar.  Unsupported or
    invalid batches cross the established adapter conversion instead, which either
    proves the conversion is valid or raises the same Python-facing error as the
    non-optimized path. Return converted rows so a fallback can reuse that boundary
    without invoking extension scalar conversion a second time.
    """
    if arrow_batch_rows_are_valid(batch):
        return None
    from ..tabular.arrow import batch_to_rows as canonical_batch_to_rows

    return canonical_batch_to_rows(batch)


def arrow_i64_sum(
    values: Any, row_count: int, pa: Any, pc: Any, *, bounds: Any | None = None
) -> int:
    """Return an exact Python integer sum for one nonempty int64 Arrow array."""
    return _arrow_numeric.arrow_i64_sum(
        values, row_count, pa, pc, bounds=bounds, hooks=_ARROW_NUMERIC_HOOKS
    )


def arrow_i64_extreme(
    current: Any,
    values: Any,
    kind: TypeLiteral["min", "max"],
    pc: Any,
    *,
    missing: object = _ARROW_REDUCTION_MISSING,
) -> Any:
    """Fold one Arrow array while retaining Python's ordered null comparisons."""
    return _arrow_numeric.arrow_i64_extreme(
        current, values, kind, pc, missing=missing, hooks=_ARROW_NUMERIC_HOOKS
    )


def _reduce_arrow_i64_extreme_batches(
    iterator: Iterator[Any],
    field_index: int,
    kind: TypeLiteral["min", "max"],
    pc: Any,
    *,
    capture_source_value_error: bool,
) -> ArrowI64Reduction:
    """Reduce one exact int64 extreme while keeping source failures distinct."""
    return _arrow_numeric._reduce_arrow_i64_extreme_batches(
        iterator,
        field_index,
        kind,
        pc,
        capture_source_value_error=capture_source_value_error,
        hooks=_ARROW_NUMERIC_HOOKS,
    )


def reduce_arrow_i64_batches(
    batches: Iterator[Any],
    field_index: int,
    kind: TypeLiteral["sum", "min", "max"],
    pa: Any,
    pc: Any,
    *,
    capture_source_value_error: bool = False,
) -> ArrowI64Reduction:
    """Reduce exact int64 columns without losing empty or ordered-null semantics."""
    return _arrow_numeric.reduce_arrow_i64_batches(
        batches,
        field_index,
        kind,
        pa,
        pc,
        capture_source_value_error=capture_source_value_error,
        hooks=_ARROW_NUMERIC_HOOKS,
    )


def try_arrow_i64_field_reduction(
    plan: Pipeline, prefix: ArrowPrefixPlan, kind: TypeLiteral["sum", "min", "max"]
) -> ArrowI64Reduction | None:
    """Reduce one complete direct-field Arrow plan or decline before unsafe consumption."""
    return _arrow_numeric.try_arrow_i64_field_reduction(
        plan, prefix, kind, hooks=_ARROW_NUMERIC_HOOKS
    )


def _guarded_arrow_direct_field(
    plan: Pipeline, prefix: ArrowPrefixPlan
) -> tuple[ArrowBatchSource, int, Any, Any] | None:
    """Resolve one closed direct field without claiming or opening its Arrow source."""
    return _arrow_numeric._guarded_arrow_direct_field(plan, prefix, hooks=_ARROW_NUMERIC_HOOKS)


def _arrow_numeric_buffer(
    values: Any, format_code: TypeLiteral["q", "d"]
) -> memoryview[int] | memoryview[float]:
    """Borrow one nonempty primitive Arrow value buffer at its logical array offset."""
    return _arrow_numeric._arrow_numeric_buffer(values, format_code, hooks=_ARROW_NUMERIC_HOOKS)


def try_arrow_numeric_field_mean(
    plan: Pipeline, prefix: ArrowPrefixPlan
) -> tuple[bool, float | None]:
    """Compute a Python-compatible compensated mean over one direct Arrow field."""
    return _arrow_numeric.try_arrow_numeric_field_mean(plan, prefix, hooks=_ARROW_NUMERIC_HOOKS)


@dataclass(slots=True)
class _OutputSchemaInference:
    """Track whether Python would infer every native type from its first output batch."""

    batch_size: int
    anchors: list[bool] | None = None
    rows: int = 0
    complete: bool = False

    def observe(self, pa: Any, output: Any) -> bool:
        """Record output values and decline once a full first batch leaves a type unanchored."""
        if self.complete or not output.num_rows:
            return True
        if self.anchors is None:
            self.anchors = [pa.types.is_null(field.type) for field in output.schema]
        inspected = min(self.batch_size - self.rows, output.num_rows)
        for index, anchored in enumerate(self.anchors):
            if not anchored:
                values = output.column(index).slice(0, inspected)
                self.anchors[index] = values.null_count < inspected
        self.rows += inspected
        if self.rows < self.batch_size:
            return True
        self.complete = all(self.anchors)
        return self.complete

    def requires_fallback(self) -> bool:
        """Return whether an exhausted short first batch still lacks a type anchor."""
        return not self.complete and self.anchors is not None and not all(self.anchors)


def _materialize_table_batch(
    batch: Any,
    operations: tuple[Any, ...],
    projection: ArrowProjectionSpec | None,
) -> Any:
    """Apply one proven batch program and retain its result as a metadata-free RecordBatch."""
    pa, pc = _arrow_modules()
    current = batch
    mask = _combined_filter_mask(batch, operations, projection)
    if mask is not None:
        current = pc.filter(batch, mask)
        operations = operations[-1:] if projection is not None else ()
    for index, operation in enumerate(operations):
        if projection is not None and index == len(operations) - 1:
            arrays = [
                current.column(current.schema.get_field_index(source))
                for _output, source in projection.selectors
            ]
            return pa.RecordBatch.from_arrays(
                arrays,
                names=[output for output, _source in projection.selectors],
            )
        result = lower_row_expression(_expr_of(operation), current)
        if isinstance(operation, FilterOp):
            current = pc.filter(current, result)
            continue
        raise ValueError("Arrow table materialization supports only a final direct projection")
    return pa.RecordBatch.from_arrays(
        [current.column(index) for index in range(current.num_columns)],
        names=current.schema.names,
    )


def _fallback_table_from_open_batches(
    batches: Any,
    current_batch: Any,
    prior_outputs: list[Any],
    operations: tuple[Any, ...],
    *,
    batch_size: int,
    schema: Any | None,
    current_rows: list[Any] | None = None,
) -> Any:
    """Finish an already-opened speculative scan through the canonical row conversion."""
    from ..tabular.arrow import table_from_rows
    from ..tabular.records import _record_view

    def rows() -> Iterator[Any]:
        for output in prior_outputs:
            yield from batch_to_rows(output)
        yield from _execute_python_batch(current_batch, operations, rows=current_rows)
        for batch in batches:
            yield from _execute_python_batch(batch, operations)

    return table_from_rows(
        rows(),
        batch_size=batch_size,
        schema=schema,
        as_record=_record_view,
    )


def _canonical_table_from_outputs(
    outputs: list[Any],
    *,
    batch_size: int,
    schema: Any | None,
) -> Any:
    """Rebuild already-materialized outputs when first-batch schema inference differs."""
    from ..tabular.arrow import table_from_rows
    from ..tabular.records import _record_view

    def rows() -> Iterator[Any]:
        for output in outputs:
            yield from batch_to_rows(output)

    return table_from_rows(
        rows(),
        batch_size=batch_size,
        schema=schema,
        as_record=_record_view,
    )


def _table_program_output_schema(
    pa: Any,
    source_schema: Any | None,
    projection: ArrowProjectionSpec | None,
) -> Any | None:
    """Derive the unchanged or directly projected schema of a table-safe program."""
    if source_schema is None or projection is None:
        return source_schema
    names = tuple(source_schema.names)
    fields = []
    for output, source in projection.selectors:
        if names.count(source) != 1:
            return None
        field = source_schema.field(names.index(source))
        fields.append(
            pa.field(
                output,
                field.type,
                nullable=field.nullable,
                metadata=field.metadata,
            )
        )
    return pa.schema(fields)


def try_validated_retained_arrow_table(
    plan: Pipeline,
    *,
    prefix: ArrowPrefixPlan,
) -> tuple[bool, Any | None]:
    """Execute one already row-validated retained prefix as a whole Arrow table.

    Relational callers validate the retained source before entering this helper.  Since
    the input is already fully resident, avoiding adapter-sized slices does not increase
    peak source memory and removes repeated proof, kernel, and table-combine work.
    """
    if prefix.operation_count != len(plan.operations) or not _can_materialize_table_program(prefix):
        return False, None

    from ..runtime.failpoints import has_active_failpoints

    if has_active_failpoints():
        return False, None
    descriptor = plan.source.native_data
    if not isinstance(descriptor, ArrowBatchSource):
        return False, None
    pa, pc = _arrow_modules()
    retained = descriptor.materialized_data
    if descriptor.kind == "table" and isinstance(retained, pa.Table):
        current = retained
    elif descriptor.kind == "record_batch" and isinstance(retained, pa.RecordBatch):
        try:
            current = pa.Table.from_batches([retained], schema=descriptor.schema_hint)
        except _EXPECTED_ARROW_ERRORS:
            return False, None
    else:
        return False, None

    expression_operations = (
        prefix.operations[:-1] if prefix.projection is not None else prefix.operations
    )
    safety = prove_batch_safe(
        current,
        expression_operations,
        projection=prefix.projection,
    )
    if not safety.safe or not _table_output_types_are_canonical(pa, current, prefix.projection):
        return False, None

    try:
        operations = prefix.operations
        mask = _combined_filter_mask(current, operations, prefix.projection)
        if mask is not None:
            current = pc.filter(current, mask)
            operations = operations[-1:] if prefix.projection is not None else ()
        for index, operation in enumerate(operations):
            if prefix.projection is not None and index == len(operations) - 1:
                current = current.select(
                    [source for _output, source in prefix.projection.selectors]
                ).rename_columns([output for output, _source in prefix.projection.selectors])
                break
            result = lower_row_expression(_expr_of(operation), current)
            if isinstance(operation, FilterOp):
                current = pc.filter(current, result)
                continue
            return False, None
    except MemoryError:
        raise
    except _EXPECTED_ARROW_ERRORS:
        return False, None

    plan.source.open_native(ArrowBatchSource)
    return True, current


def try_arrow_batch_factory(  # noqa: C901 - one ownership/fallback state machine
    plan: Pipeline,
    *,
    batch_size: int,
    prefix: ArrowPrefixPlan | None = None,
    schema: Any = None,
) -> Callable[[], Iterator[Any]] | None:
    """Return a lazy native-batch opener for one complete guarded record plan.

    Plans rejected before opening retain the established row-to-batch adapter. A
    per-batch safety or kernel decline finishes the already-claimed source through
    that adapter, preserving one-shot ownership without reopening it.
    """
    explicit_schema = schema is not None
    if schema is not None:
        from ..tabular.arrow import guarded_arrow_mean_source

        if guarded_arrow_mean_source(plan.source) is None:
            return None
    prefix = plan_arrow_prefix(plan) if prefix is None else prefix
    if (
        prefix is None
        or prefix.operation_count != len(plan.operations)
        or not _can_materialize_table_program(prefix)
    ):
        return None

    from ..runtime.failpoints import has_active_failpoints

    if has_active_failpoints():
        return None
    descriptor = plan.source.native_data
    if not isinstance(descriptor, ArrowBatchSource):
        return None
    pa, _pc = _arrow_modules()
    if schema is not None:
        retained = descriptor.materialized_data
        if (
            descriptor.kind not in {"table", "record_batch"}
            or not isinstance(retained, (pa.Table, pa.RecordBatch))
            or type(schema) is not pa.Schema
            or len(schema) == 0
        ):
            return None
        expected_schema = _table_program_output_schema(pa, retained.schema, prefix.projection)
        if (
            expected_schema is None
            or not expected_schema.equals(schema, check_metadata=True)
            or not _table_output_types_are_canonical(pa, retained, prefix.projection)
        ):
            return None
    equality = _scan_equality(prefix) if descriptor.kind == "parquet" else None
    range_predicate = _scan_range_predicate(prefix) if descriptor.kind == "parquet" else None
    columns = _projection_scan_columns(prefix, descriptor=descriptor)
    expression_operations = (
        prefix.operations[:-1] if prefix.projection is not None else prefix.operations
    )

    def planned_batches(  # noqa: C901 - keep claim and close paths together
    ) -> Iterator[Any]:
        """Open once, emit bounded native batches, and own every fallback/close path."""

        def slices(batch: Any) -> Iterator[Any]:
            for offset in range(0, batch.num_rows, batch_size):
                yield batch.slice(offset, batch_size)

        def converted(
            prior_outputs: list[Any],
            current_batch: Any | None,
            remaining: Iterator[Any],
            *,
            schema: Any | None,
            current_rows: list[Any] | None = None,
        ) -> Iterator[Any]:
            from ..tabular.arrow import arrow_batch_factory
            from ..tabular.records import _record_view

            def rows() -> Iterator[Any]:
                if explicit_schema:
                    from ..tabular import arrow as arrow_adapter

                    def converted_batch(
                        batch: Any, prepared: list[Any] | None = None
                    ) -> Iterator[Any]:
                        for row in (
                            arrow_adapter.batch_to_rows(batch) if prepared is None else prepared
                        ):
                            for operation in prefix.operations:
                                if isinstance(operation, MapOp):
                                    row = operation.function(row)
                                elif bool(operation.predicate(row)) is operation.negate:
                                    break
                            else:
                                yield row

                    for output in prior_outputs:
                        yield from batch_to_rows(output)
                    if current_batch is not None:
                        yield from converted_batch(current_batch, current_rows)
                    for later_batch in remaining:
                        yield from converted_batch(later_batch)
                    return
                for output in prior_outputs:
                    yield from batch_to_rows(output)
                if current_batch is not None:
                    yield from _execute_python_batch(
                        current_batch, prefix.operations, rows=current_rows
                    )
                for later_batch in remaining:
                    yield from _execute_python_batch(later_batch, prefix.operations)

            yield from arrow_batch_factory(
                rows(),
                batch_size=batch_size,
                schema=schema,
                as_record=_record_view,
            )()

        plan.source.open_native(ArrowBatchSource)
        opened = descriptor.open_batches(
            columns=columns if _projection_is_current(prefix) else None,
            projection_check=lambda: _projection_is_current(prefix),
            equality=equality,
            range_predicate=range_predicate,
        )
        with closing_iterators((opened,)):
            if not prefix.operations:
                for batch in opened:
                    if schema is not None:
                        from ..tabular.arrow import guarded_arrow_mean_source

                        if (
                            guarded_arrow_mean_source(plan.source) is not descriptor
                            or not batch.schema.equals(schema, check_metadata=True)
                            or not arrow_batch_rows_are_valid(batch)
                        ):
                            yield from converted([], batch, opened, schema=schema)
                            return
                    yield from slices(batch)
                return

            inference = _OutputSchemaInference(batch_size)
            pending: list[Any] = []
            output_schema = schema
            for batch in opened:
                if schema is not None:
                    from ..tabular.arrow import guarded_arrow_mean_source

                    batch_output_schema = _table_program_output_schema(
                        pa, batch.schema, prefix.projection
                    )
                    if (
                        guarded_arrow_mean_source(plan.source) is not descriptor
                        or batch_output_schema is None
                        or not batch_output_schema.equals(schema, check_metadata=True)
                    ):
                        yield from converted(pending, batch, opened, schema=schema)
                        return
                rows = observe_arrow_batch_rows(batch)
                if rows is not None:
                    yield from converted(
                        pending, batch, opened, schema=output_schema, current_rows=rows
                    )
                    return
                safety = prove_batch_safe(
                    batch,
                    expression_operations,
                    projection=prefix.projection,
                )
                if (
                    safety.safe
                    and _projection_is_current(prefix)
                    and _table_output_types_are_canonical(pa, batch, prefix.projection)
                ):
                    try:
                        output = _materialize_table_batch(
                            batch, prefix.operations, prefix.projection
                        )
                        if schema is not None:
                            output = pa.RecordBatch.from_arrays(output.columns, schema=schema)
                    except _EXPECTED_ARROW_ERRORS:
                        pass
                    else:
                        pieces = list(slices(output))
                        for index, piece in enumerate(pieces):
                            if output_schema is not None:
                                yield piece
                                continue
                            pending.append(piece)
                            if not inference.observe(pa, piece):
                                pending.extend(pieces[index + 1 :])
                                yield from converted(pending, None, opened, schema=None)
                                return
                            if inference.complete:
                                output_schema = pending[0].schema
                                yield from pending
                                pending.clear()
                        continue
                yield from converted(pending, batch, opened, schema=output_schema)
                return

            if pending:
                if inference.requires_fallback():
                    yield from converted(pending, None, iter(()), schema=None)
                else:
                    yield from pending

    def native_batches() -> Iterator[Any]:
        yield from _rechunk_batches(pa, planned_batches(), batch_size)

    return native_batches


def try_arrow_table(
    plan: Pipeline,
    *,
    batch_size: int,
    prefix: ArrowPrefixPlan | None = None,
    preserve_source_schema: bool = False,
) -> tuple[bool, Any | None]:
    """Materialize one complete guarded record prefix without boxing safe Arrow batches."""
    prefix = plan_arrow_prefix(plan) if prefix is None else prefix
    if (
        prefix is None
        or prefix.operation_count != len(plan.operations)
        or not _can_materialize_table_program(prefix)
    ):
        return False, None

    from ..runtime.failpoints import has_active_failpoints

    if has_active_failpoints():
        return False, None

    descriptor = plan.source.native_data
    if not isinstance(descriptor, ArrowBatchSource):
        return False, None
    pa, _pc = _arrow_modules()
    equality = _scan_equality(prefix) if descriptor.kind == "parquet" else None
    range_predicate = _scan_range_predicate(prefix) if descriptor.kind == "parquet" else None
    columns = _projection_scan_columns(prefix, descriptor=descriptor)
    outputs: list[Any] = []
    inference = _OutputSchemaInference(batch_size)
    fallback_schema = (
        _table_program_output_schema(pa, descriptor.schema_hint, prefix.projection)
        if preserve_source_schema
        else None
    )
    fixed_output_schema = fallback_schema if preserve_source_schema else None
    expression_operations = (
        prefix.operations[:-1] if prefix.projection is not None else prefix.operations
    )

    plan.source.open_native(ArrowBatchSource)
    if not _projection_is_current(prefix):
        fallback_schema = fixed_output_schema = None
    batches = descriptor.open_batches(
        columns=columns if _projection_is_current(prefix) else None,
        projection_check=lambda: _projection_is_current(prefix),
        equality=equality,
        range_predicate=range_predicate,
    )
    with closing_iterators((batches,)):
        if not prefix.operations:
            return True, _identity_table(
                pa,
                list(batches),
                schema=descriptor.schema_hint,
                batch_size=batch_size,
            )
        for batch in batches:
            if not _projection_is_current(prefix):
                # The former selected field no longer determines the output dtype.
                fallback_schema = fixed_output_schema = None
            rows = observe_arrow_batch_rows(batch)
            safety = prove_batch_safe(
                batch,
                expression_operations,
                projection=prefix.projection,
            )
            if (
                rows is None
                and safety.safe
                and _projection_is_current(prefix)
                and _table_output_types_are_canonical(pa, batch, prefix.projection)
            ):
                try:
                    output = _materialize_table_batch(batch, prefix.operations, prefix.projection)
                except _EXPECTED_ARROW_ERRORS:
                    pass
                else:
                    if output.num_rows:
                        if fixed_output_schema is None and not inference.observe(pa, output):
                            return True, _fallback_table_from_open_batches(
                                batches,
                                batch,
                                outputs,
                                prefix.operations,
                                batch_size=batch_size,
                                schema=fallback_schema,
                            )
                        outputs.append(output)
                    continue
            return True, _fallback_table_from_open_batches(
                batches,
                batch,
                outputs,
                prefix.operations,
                batch_size=batch_size,
                schema=fallback_schema,
                current_rows=rows,
            )

    if not outputs:
        empty = (
            pa.Table.from_batches([], schema=fallback_schema)
            if fallback_schema is not None
            else pa.table({})
        )
        return True, empty
    if fixed_output_schema is None and inference.requires_fallback():
        return True, _canonical_table_from_outputs(
            outputs,
            batch_size=batch_size,
            schema=fallback_schema,
        )
    table = pa.Table.from_batches(outputs, schema=fixed_output_schema).combine_chunks()
    return True, pa.Table.from_batches(table.to_batches(max_chunksize=batch_size))


def _execute_arrow_first(
    descriptor: ArrowBatchSource,
    prefix: ArrowPrefixPlan,
    *,
    columns: tuple[str, ...] | None,
    equality: tuple[str, object] | None,
) -> Iterator[Any]:
    """Yield at most one row from a complete, short-circuit-safe Arrow program."""
    batches = descriptor.open_batches(
        columns=columns,
        projection_check=lambda: _projection_is_current(prefix),
        equality=equality,
        first_only=True,
    )
    with closing_iterators((batches,)):
        for batch in batches:
            rows = observe_arrow_batch_rows(batch)
            if rows is not None:
                found, value = _execute_first_python_batch(batch, prefix.operations, rows=rows)
                if found:
                    yield value
                    return
                continue
            if prefix.operation_count == 0:
                if batch.num_rows:
                    yield batch_to_rows(batch.slice(0, 1))[0]
                    return
                continue
            expression_operations = (
                prefix.operations[:-1] if prefix.projection is not None else prefix.operations
            )
            safety = prove_batch_safe(
                batch,
                expression_operations,
                projection=prefix.projection,
            )
            if safety.safe and _can_execute_batch_program(prefix.operations, prefix.projection):
                try:
                    found, value = _execute_first_batch_program(
                        batch, prefix.operations, prefix.projection
                    )
                except _EXPECTED_ARROW_ERRORS:
                    pass
                else:
                    if found:
                        yield value
                        return
                    continue
            found, value = _execute_first_python_batch(batch, prefix.operations)
            if found:
                yield value
                return


def execute_arrow_prefix(
    plan: Pipeline, *, prefix: ArrowPrefixPlan | None = None
) -> Iterator[Any] | None:
    """Return a lazy iterator for the plan's Arrow-compatible prefix, if nonempty.

    Each input batch gets an independent safety verdict. A single safe map or
    filter uses Arrow compute; guarded batches, multi-operation prefixes, and
    recoverable kernel failures run the same prefix over Python row dictionaries.
    The returned iterator owns and closes the descriptor's batch iterator.
    """
    # Physical execution may reconstruct compiled row predicates as ordinary callables.  Reuse
    # the prefix selected from the original logical operations when supplied; recomputing it on
    # that compatibility pipeline can shorten the prefix and make suffix slicing skip work.
    prefix = plan_arrow_prefix(plan) if prefix is None else prefix
    if (
        prefix is None
        or (prefix.operation_count == 0 and not prefix.first_only)
        or not _projection_is_current(prefix)
    ):
        return None
    if prefix.first_only and prefix.operation_count != len(plan.operations):
        return None
    from ..runtime.failpoints import has_active_failpoints

    if has_active_failpoints():
        return None
    descriptor = plan.source.native_data
    if not isinstance(descriptor, ArrowBatchSource):
        return None
    equality = _scan_equality(prefix) if descriptor.kind == "parquet" else None
    range_predicate = _scan_range_predicate(prefix) if descriptor.kind == "parquet" else None
    columns = _projection_scan_columns(prefix, descriptor=descriptor)
    expression_operations = (
        prefix.operations[:-1] if prefix.projection is not None else prefix.operations
    )

    def values() -> Iterator[Any]:
        """Process batches lazily and close the opened batch stream on every exit."""
        plan.source.open_native(ArrowBatchSource)
        current_columns = columns if _projection_is_current(prefix) else None
        if prefix.first_only:
            yield from _execute_arrow_first(
                descriptor,
                prefix,
                columns=current_columns,
                equality=equality,
            )
            return
        batches = descriptor.open_batches(
            columns=current_columns,
            projection_check=lambda: _projection_is_current(prefix),
            equality=equality,
            range_predicate=range_predicate,
        )
        with closing_iterators((batches,)):
            for batch in batches:
                rows = observe_arrow_batch_rows(batch)
                if rows is not None:
                    yield from execute_operations(iter(rows), prefix.operations)
                    continue
                safety = prove_batch_safe(
                    batch,
                    expression_operations,
                    projection=prefix.projection,
                )
                if safety.safe and _can_execute_batch_program(prefix.operations, prefix.projection):
                    try:
                        yield from _execute_batch_program(
                            batch, prefix.operations, prefix.projection
                        )
                        continue
                    except _EXPECTED_ARROW_ERRORS:
                        pass
                yield from execute_operations(iter(batch_to_rows(batch)), prefix.operations)

    return values()


def execute_with_arrow_prefix(
    plan: Pipeline,
    *,
    prefix: ArrowPrefixPlan | None = None,
    runtime: QueryRuntime | None = None,
) -> Iterator[Any]:
    """Execute an Arrow prefix and pass its values through the Python suffix.

    Plans without a usable prefix run wholly in Python. The Python executor owns
    the prefix iterator as its source, so early termination closes the Arrow batch
    stream through the normal iterator-cleanup chain.
    """
    prefix = plan_arrow_prefix(plan) if prefix is None else prefix
    if prefix is None or (prefix.operation_count == 0 and not prefix.first_only):
        yield from execute_operations(plan.source.open(), plan.operations, runtime=runtime)
        return
    values = execute_arrow_prefix(plan, prefix=prefix)
    if values is None:
        yield from execute_operations(plan.source.open(), plan.operations, runtime=runtime)
        return
    suffix = Pipeline(
        Source.from_iterable(values),
        plan.operations[prefix.operation_count :],
        "python",
    )
    yield from execute_operations(suffix.source.open(), suffix.operations, runtime=runtime)
