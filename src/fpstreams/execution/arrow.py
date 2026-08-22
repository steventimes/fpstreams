"""Execute guarded RowExpr map/filter prefixes over Arrow batches."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import suppress
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

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
    direct_exact_equality,
    direct_exact_i64_range,
    plan_arrow_prefix,
)
from ..planning.arrow_source import ArrowBatchSource, RangePredicate, batch_to_rows
from ..planning.logical import Pipeline
from ..planning.source import Source
from ..planning.sync import FilterOp, MapOp
from ..runtime.query import QueryRuntime
from .sync import execute_operations

_EXPECTED_ARROW_ERRORS = (ArithmeticError, NotImplementedError, TypeError, ValueError)


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
    """Return whether every Arrow field converts to a primitive built-in Python value."""
    checks = tuple(
        predicate
        for name in (
            "is_null",
            "is_boolean",
            "is_integer",
            "is_floating",
            "is_string",
            "is_large_string",
            "is_binary",
            "is_large_binary",
            "is_fixed_size_binary",
        )
        if callable(predicate := getattr(pa.types, name, None))
    )
    return all(any(check(field.type) for check in checks) for field in schema)


def _retained_arrow_stable_sort_result(
    plan: Any,
) -> tuple[Any, ArrowBatchSource] | None:
    """Return one guarded stable Arrow sort result before Python row conversion."""
    from ..expressions.selectors import _direct_field
    from ..physical.plan import SortPhysicalNode, SortStrategy
    from ..runtime.failpoints import has_active_failpoints, hit

    if (
        plan.root is not None
        or plan.engine != "auto"
        or plan.parallel is not None
        or len(plan.nodes) != 1
        or not isinstance(node := plan.nodes[0], SortPhysicalNode)
        or node.strategy is not SortStrategy.ARROW_STABLE
        or node.operation.buffer_size is not None
        or type(node.operation.reverse) is not bool
        or (field := _direct_field(node.operation.key)) is None
        or has_active_failpoints()
        or not plan.source.capabilities.reiterable
    ):
        return None
    descriptor = plan.source.native_data
    if (
        not isinstance(descriptor, ArrowBatchSource)
        or descriptor.kind not in {"table", "record_batch"}
        or descriptor.materialized_data is None
    ):
        return None

    pa, pc = _arrow_modules()
    retained = descriptor.materialized_data
    if descriptor.kind == "table":
        if not isinstance(retained, pa.Table):
            return None
    elif not isinstance(retained, pa.RecordBatch):
        return None
    schema = retained.schema
    names = tuple(schema.names)
    if names.count(field) != 1 or not _has_primitive_builtin_schema(pa, schema):
        return None
    field_index = names.index(field)
    key_values = retained.column(field_index)
    types = pa.types
    if key_values.null_count or not (
        types.is_boolean(key_values.type)
        or types.is_integer(key_values.type)
        or types.is_string(key_values.type)
        or types.is_large_string(key_values.type)
        or types.is_binary(key_values.type)
        or types.is_large_binary(key_values.type)
        or types.is_fixed_size_binary(key_values.type)
    ):
        return None

    try:
        retained.validate(full=True)
    except _EXPECTED_ARROW_ERRORS:
        return None

    plan.source.open_native(ArrowBatchSource)
    hit("source.open.after")
    order = "descending" if node.operation.reverse else "ascending"
    try:
        indices = pc.sort_indices(retained, sort_keys=[(field, order)])
        ordered = pc.take(retained, indices)
    except _EXPECTED_ARROW_ERRORS:
        return None
    return ordered, descriptor


def _retained_arrow_sorted_batches(ordered: Any, descriptor: ArrowBatchSource) -> Iterator[Any]:
    """Yield one ordered retained result in the adapter's canonical batch bounds."""
    if descriptor.kind == "table":
        yield from ordered.to_batches(max_chunksize=descriptor.batch_size)
        return
    for offset in range(0, ordered.num_rows, descriptor.batch_size):
        yield ordered.slice(offset, descriptor.batch_size)


def materialize_retained_arrow_stable_sort(plan: Any) -> list[Any] | None:
    """Collect a guarded retained Arrow sort without per-row generator forwarding."""
    result = _retained_arrow_stable_sort_result(plan)
    if result is None:
        return None
    ordered, descriptor = result
    rows: list[Any] = []
    for batch in _retained_arrow_sorted_batches(ordered, descriptor):
        rows.extend(batch_to_rows(batch))
    return rows


def try_retained_arrow_stable_sort(plan: Any) -> Iterator[Any] | None:
    """Sort one proven retained Arrow source without first constructing row dictionaries."""
    result = _retained_arrow_stable_sort_result(plan)
    if result is None:
        return None
    ordered, descriptor = result

    def rows() -> Iterator[Any]:
        """Convert the already ordered primitive Arrow values in canonical batch bounds."""
        for batch in _retained_arrow_sorted_batches(ordered, descriptor):
            yield from batch_to_rows(batch)

    return rows()


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
        return (len(operations) == 1 and isinstance(operations[0], MapOp)) or (
            len(operations) == 2
            and _direct_primitive_filter(operations[0])
            and isinstance(operations[1], MapOp)
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


def _execute_batch_program(
    batch: Any,
    operations: tuple[Any, ...],
    projection: ArrowProjectionSpec | None,
) -> Iterator[Any]:
    """Apply a shape-preserving Arrow filter sequence and an optional final map."""
    _pa, pc = _arrow_modules()
    current = batch
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


def _projection_scan_columns(prefix: ArrowPrefixPlan) -> tuple[str, ...] | None:
    """Return source fields needed by a closed projection and its optional filter."""
    if prefix.projection is None:
        return None
    required = list(prefix.projection.inputs)
    seen = set(required)
    for operation in prefix.operations[:-1]:
        for node in _nodes(_expr_of(operation)):
            if isinstance(node, Field) and node.name not in seen:
                seen.add(node.name)
                required.append(node.name)
    return tuple(required)


def _count_scan_columns(prefix: ArrowPrefixPlan) -> tuple[str, ...] | None:
    """Return direct source fields needed to evaluate a count's complete batch program."""
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
    if len(operations) not in {1, 2} or not isinstance(operations[0], FilterOp):
        return None
    if len(operations) == 2 and (prefix.projection is None or not isinstance(operations[1], MapOp)):
        return None
    return direct_exact_equality(operations[0])


def _scan_range_predicate(prefix: ArrowPrefixPlan) -> RangePredicate | None:
    """Return one exact i64 range that a guarded source may use as an I/O hint."""
    operations = prefix.operations
    if len(operations) not in {1, 2} or not isinstance(operations[0], FilterOp):
        return None
    if len(operations) == 2 and (prefix.projection is None or not isinstance(operations[1], MapOp)):
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
) -> tuple[bool, Any | None]:
    """Apply a guarded batch prefix row-by-row until its first accepted result."""
    for item in batch_to_rows(batch):
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


def _close_batches(batches: Any) -> None:
    """Best-effort close owned Arrow batches without replacing query outcomes."""
    close = getattr(batches, "close", None)
    if callable(close):
        with suppress(Exception):
            close()


def _rechunk_batches(pa: Any, batches: Iterator[Any], batch_size: int) -> Iterator[Any]:
    """Reblock native batches with at most one output chunk of buffered columnar data."""
    iterator = iter(batches)
    pending: list[Any] = []
    pending_rows = 0
    try:
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
                    yield (pending[0] if len(pending) == 1 else pa.concat_batches(pending))
                    pending.clear()
                    pending_rows = 0
            while rows - offset >= batch_size:
                yield batch.slice(offset, batch_size)
                offset += batch_size
            if offset < rows:
                pending.append(batch.slice(offset))
                pending_rows = rows - offset
        if pending:
            yield pending[0] if len(pending) == 1 else pa.concat_batches(pending)
    finally:
        _close_batches(iterator)


def _execute_python_batch(batch: Any, operations: tuple[Any, ...]) -> list[Any]:
    """Run one unsafe Arrow batch through the established Python row operations."""
    current: list[Any] = batch_to_rows(batch)
    for operation in operations:
        if isinstance(operation, MapOp):
            current = [operation.function(item) for item in current]
        else:
            current = [
                item for item in current if bool(operation.predicate(item)) is not operation.negate
            ]
    return current


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
    if prefix is None or prefix.operation_count != len(plan.operations):
        return False, 0

    from ..runtime.failpoints import has_active_failpoints

    if has_active_failpoints():
        values = execute_operations(plan.source.open(), plan.operations)
        try:
            return True, sum(1 for _value in values)
        finally:
            close = getattr(values, "close", None)
            if callable(close):
                close()

    descriptor = plan.source.native_data
    if not isinstance(descriptor, ArrowBatchSource):
        return False, 0
    equality = _scan_equality(prefix) if descriptor.kind == "parquet" else None
    range_predicate = _scan_range_predicate(prefix) if descriptor.kind == "parquet" else None
    columns = _count_scan_columns(prefix)
    expression_operations = (
        prefix.operations[:-1] if prefix.projection is not None else prefix.operations
    )

    plan.source.open_native(ArrowBatchSource)
    if not prefix.operations and descriptor.count_opener is not None:
        count = descriptor.count_opener()
        if count is not None:
            return True, count
    batches = descriptor.open_batches(
        columns=columns,
        equality=equality,
        range_predicate=range_predicate,
    )
    total = 0
    try:
        for batch in batches:
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
    finally:
        _close_batches(batches)
    return True, total


def _can_materialize_table_program(prefix: ArrowPrefixPlan) -> bool:
    """Accept native identity, safe filter-only, or direct projection programs."""
    operations = prefix.operations
    if not operations:
        return True
    if prefix.projection is None:
        return _direct_filter_program(operations)
    return bool(prefix.projection.selectors) and _can_execute_batch_program(
        operations, prefix.projection
    )


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
) -> Any:
    """Finish an already-opened speculative scan through the canonical row conversion."""
    from ..tabular.arrow import table_from_rows
    from ..tabular.records import _record_view

    def rows() -> Iterator[Any]:
        for output in prior_outputs:
            yield from batch_to_rows(output)
        yield from _execute_python_batch(current_batch, operations)
        for batch in batches:
            yield from _execute_python_batch(batch, operations)

    return table_from_rows(
        rows(),
        batch_size=batch_size,
        schema=None,
        as_record=_record_view,
    )


def _canonical_table_from_outputs(outputs: list[Any], *, batch_size: int) -> Any:
    """Rebuild already-materialized outputs when first-batch schema inference differs."""
    from ..tabular.arrow import table_from_rows
    from ..tabular.records import _record_view

    def rows() -> Iterator[Any]:
        for output in outputs:
            yield from batch_to_rows(output)

    return table_from_rows(
        rows(),
        batch_size=batch_size,
        schema=None,
        as_record=_record_view,
    )


def try_arrow_batch_factory(  # noqa: C901 - one ownership/fallback state machine
    plan: Pipeline,
    *,
    batch_size: int,
    prefix: ArrowPrefixPlan | None = None,
) -> Callable[[], Iterator[Any]] | None:
    """Return a lazy native-batch opener for one complete guarded record plan.

    Plans rejected before opening retain the established row-to-batch adapter. A
    per-batch safety or kernel decline finishes the already-claimed source through
    that adapter, preserving one-shot ownership without reopening it.
    """
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
    equality = _scan_equality(prefix) if descriptor.kind == "parquet" else None
    range_predicate = _scan_range_predicate(prefix) if descriptor.kind == "parquet" else None
    columns = _projection_scan_columns(prefix)
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
        ) -> Iterator[Any]:
            from ..tabular.arrow import arrow_batch_factory
            from ..tabular.records import _record_view

            def rows() -> Iterator[Any]:
                for output in prior_outputs:
                    yield from batch_to_rows(output)
                if current_batch is not None:
                    yield from _execute_python_batch(current_batch, prefix.operations)
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
            columns=columns,
            equality=equality,
            range_predicate=range_predicate,
        )
        try:
            if not prefix.operations:
                for batch in opened:
                    yield from slices(batch)
                return

            inference = _OutputSchemaInference(batch_size)
            pending: list[Any] = []
            output_schema = None
            for batch in opened:
                safety = prove_batch_safe(
                    batch,
                    expression_operations,
                    projection=prefix.projection,
                )
                if safety.safe and _table_output_types_are_canonical(pa, batch, prefix.projection):
                    try:
                        output = _materialize_table_batch(
                            batch, prefix.operations, prefix.projection
                        )
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
        finally:
            _close_batches(opened)

    def native_batches() -> Iterator[Any]:
        yield from _rechunk_batches(pa, planned_batches(), batch_size)

    return native_batches


def try_arrow_table(
    plan: Pipeline,
    *,
    batch_size: int,
    prefix: ArrowPrefixPlan | None = None,
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
    columns = _projection_scan_columns(prefix)
    outputs: list[Any] = []
    inference = _OutputSchemaInference(batch_size)
    expression_operations = (
        prefix.operations[:-1] if prefix.projection is not None else prefix.operations
    )

    plan.source.open_native(ArrowBatchSource)
    batches = descriptor.open_batches(
        columns=columns,
        equality=equality,
        range_predicate=range_predicate,
    )
    try:
        if not prefix.operations:
            return True, _identity_table(
                pa,
                list(batches),
                schema=descriptor.schema_hint,
                batch_size=batch_size,
            )
        for batch in batches:
            safety = prove_batch_safe(
                batch,
                expression_operations,
                projection=prefix.projection,
            )
            if safety.safe and _table_output_types_are_canonical(pa, batch, prefix.projection):
                try:
                    output = _materialize_table_batch(batch, prefix.operations, prefix.projection)
                except _EXPECTED_ARROW_ERRORS:
                    pass
                else:
                    if output.num_rows:
                        if not inference.observe(pa, output):
                            return True, _fallback_table_from_open_batches(
                                batches,
                                batch,
                                outputs,
                                prefix.operations,
                                batch_size=batch_size,
                            )
                        outputs.append(output)
                    continue
            return True, _fallback_table_from_open_batches(
                batches,
                batch,
                outputs,
                prefix.operations,
                batch_size=batch_size,
            )
    finally:
        _close_batches(batches)

    if not outputs:
        return True, pa.table({})
    if inference.requires_fallback():
        return True, _canonical_table_from_outputs(outputs, batch_size=batch_size)
    table = pa.Table.from_batches(outputs).combine_chunks()
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
        equality=equality,
        first_only=True,
    )
    try:
        for batch in batches:
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
    finally:
        _close_batches(batches)


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
    if prefix is None or (prefix.operation_count == 0 and not prefix.first_only):
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
    columns = _projection_scan_columns(prefix)
    expression_operations = (
        prefix.operations[:-1] if prefix.projection is not None else prefix.operations
    )

    def values() -> Iterator[Any]:
        """Process batches lazily and close the opened batch stream on every exit."""
        plan.source.open_native(ArrowBatchSource)
        if prefix.first_only:
            yield from _execute_arrow_first(
                descriptor,
                prefix,
                columns=columns,
                equality=equality,
            )
            return
        batches = descriptor.open_batches(
            columns=columns,
            equality=equality,
            range_predicate=range_predicate,
        )
        try:
            for batch in batches:
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
                rows = batch_to_rows(batch)
                current: list[Any] = rows
                for operation in prefix.operations:
                    if isinstance(operation, MapOp):
                        current = [operation.function(item) for item in current]
                    else:
                        current = [
                            item
                            for item in current
                            if bool(operation.predicate(item)) is not operation.negate
                        ]
                yield from current
        finally:
            _close_batches(batches)

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
