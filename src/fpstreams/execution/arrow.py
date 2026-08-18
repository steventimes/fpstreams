"""Execute guarded RowExpr map/filter prefixes over Arrow batches."""

from __future__ import annotations

from collections.abc import Iterator
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
from ..planning.arrow import plan_arrow_prefix
from ..planning.arrow_source import ArrowBatchSource, batch_to_rows
from ..planning.source import Source
from ..planning.sync import FilterOp, MapOp, Plan
from .sync import execute as execute_python


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


def prove_batch_safe(batch: Any, operations: tuple[Any, ...]) -> BatchSafety:
    """Conservatively decide whether Arrow may evaluate every operation on a batch.

    The guard rejects opaque or unsupported nodes, missing fields, unhandled nulls,
    literal division by zero, and unsupported casts. Import or schema inspection
    failures are treated as kernel failures; runtime kernel errors still trigger
    the row-wise fallback in execute_arrow_prefix.
    """
    try:
        _pa, _pc = _arrow_modules()
        names = set(batch.schema.names)
    except Exception:
        return BatchSafety(False, BatchFallbackReason.KERNEL_ERROR)
    allowed_binary = {"+", "-", "*", "/", "==", "!=", "<", "<=", ">", ">=", "and", "or"}
    for operation in operations:
        root = _expr_of(operation)
        if root is None:
            return BatchSafety(False, BatchFallbackReason.OPAQUE_EXPRESSION)
        for node in _nodes(root):
            if isinstance(node, PythonUDF):
                return BatchSafety(False, BatchFallbackReason.INCOMPATIBLE_TYPE)
            if isinstance(node, Field) and node.name not in names:
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


def execute_arrow_prefix(plan: Plan) -> Iterator[Any] | None:
    """Return a lazy iterator for the plan's Arrow-compatible prefix, if nonempty.

    Each input batch gets an independent safety verdict. A single safe map or
    filter uses Arrow compute; guarded batches, multi-operation prefixes, and
    recoverable kernel failures run the same prefix over Python row dictionaries.
    The returned iterator owns and closes the descriptor's batch iterator.
    """
    prefix = plan_arrow_prefix(plan)
    if prefix is None or prefix.operation_count == 0:
        return None
    descriptor = plan.source.open_native(ArrowBatchSource)

    def values() -> Iterator[Any]:
        """Process batches lazily and close the opened batch stream on every exit."""
        batches = descriptor.open_batches()
        try:
            for batch in batches:
                safety = prove_batch_safe(batch, prefix.operations)
                if safety.safe and len(prefix.operations) == 1:
                    operation = prefix.operations[0]
                    try:
                        _pa, pc = _arrow_modules()

                        array = lower_row_expression(_expr_of(operation), batch)
                        if isinstance(operation, FilterOp):
                            filtered = pc.filter(batch, array)
                            yield from batch_to_rows(filtered)
                        else:
                            yield from array.to_pylist()
                        continue
                    except (TypeError, ValueError, ArithmeticError):
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
            close = getattr(batches, "close", None)
            if callable(close):
                close()

    return values()


def execute_with_arrow_prefix(plan: Plan) -> Iterator[Any]:
    """Execute an Arrow prefix and pass its values through the Python suffix.

    Plans without a usable prefix run wholly in Python. The Python executor owns
    the prefix iterator as its source, so early termination closes the Arrow batch
    stream through the normal iterator-cleanup chain.
    """
    prefix = plan_arrow_prefix(plan)
    if prefix is None or prefix.operation_count == 0:
        yield from execute_python(plan)
        return
    values = execute_arrow_prefix(plan)
    if values is None:
        yield from execute_python(plan)
        return
    suffix = Plan(Source.from_iterable(values), plan.operations[prefix.operation_count :], "python")
    yield from execute_python(suffix)
