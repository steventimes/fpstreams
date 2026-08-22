"""Find Arrow-executable map/filter prefixes without importing optional PyArrow."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Literal, cast

from ..expressions.row_ir import Binary, Field
from ..expressions.row_ir import Literal as RowLiteral
from .arrow_source import ArrowBatchSource, RangePredicate
from .logical import Pipeline
from .sync import FilterOp, MapOp


class ArrowBoundaryReason(StrEnum):
    """Classify why Arrow execution is unavailable, stops, or covers the full plan."""

    FORCED_PYTHON = "forced_python"
    NON_ARROW_SOURCE = "non_arrow_source"
    UNSUPPORTED_OPERATION = "unsupported_operation"
    OPAQUE_EXPRESSION = "opaque_expression"
    UNSUPPORTED_EXPRESSION = "unsupported_expression"
    FULL_PREFIX = "full_prefix"


@dataclass(frozen=True, slots=True)
class ArrowProjectionSpec:
    """Describe one exact direct-field projection without retaining compiled accessors."""

    selectors: tuple[tuple[str, str], ...]
    inputs: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ArrowPrefixPlan:
    """Describe the leading operations retained in Arrow and the following boundary."""

    operation_count: int
    operations: tuple[MapOp | FilterOp, ...]
    boundary_reason: ArrowBoundaryReason
    guarded: bool
    projection: ArrowProjectionSpec | None = None
    first_only: bool = False


@dataclass(frozen=True, slots=True)
class RowStageDescriptor:
    """Retain query-bound structure for an exact Rows transformation.

    The descriptor stores original selector and literal objects rather than a structural cache
    key.  Executors may therefore specialize a single query without accidentally borrowing a
    selector, Python callable, or literal identity from another query.
    """

    kind: Literal["with_columns", "where", "select"]
    selectors: tuple[tuple[str, object], ...] = ()
    predicate: object | None = None
    equalities: tuple[tuple[str, object], ...] = ()


@dataclass(frozen=True, slots=True)
class PlannedRowCallable:
    """Wrap a row callable so structural planning can recognize a closed projection."""

    function: Callable[[Any], Any]
    role: str = "projection"
    descriptor: RowStageDescriptor | None = None

    def __call__(self, row: Any) -> Any:
        """Delegate row evaluation to the wrapped callable."""
        return self.function(row)


def _direct_projection(operation: MapOp | FilterOp) -> ArrowProjectionSpec | None:
    """Return an ordered direct-field spec for an exact internal ``Rows.select`` map."""
    if not isinstance(operation, MapOp) or type(operation.function) is not PlannedRowCallable:
        return None
    descriptor = operation.function.descriptor
    if descriptor is None or descriptor.kind != "select":
        return None
    direct_selectors: list[tuple[str, str]] = []
    for output, selector in descriptor.selectors:
        if (
            type(output) is not str
            or not isinstance(selector, str)
            or type(selector) is not str
            or "." in selector
        ):
            return None
        direct_selectors.append((output, selector))
    selectors = tuple(direct_selectors)
    outputs = tuple(output for output, _selector in selectors)
    if len(set(outputs)) != len(outputs):
        return None
    inputs = tuple(dict.fromkeys(selector for _output, selector in selectors))
    return ArrowProjectionSpec(selectors, inputs)


def _direct_primitive_filter(operation: MapOp | FilterOp) -> bool:
    """Recognize one side-effect-free field/literal comparison for a batch pipeline."""
    if not isinstance(operation, FilterOp) or operation.negate:
        return False
    root = getattr(operation.predicate, "_node", None)
    if not isinstance(root, Binary) or root.kind not in {
        "==",
        "!=",
        "<",
        "<=",
        ">",
        ">=",
    }:
        return False
    if isinstance(root.left, Field) and isinstance(root.right, RowLiteral):
        field, literal = root.left, root.right
    elif isinstance(root.left, RowLiteral) and isinstance(root.right, Field):
        field, literal = root.right, root.left
    else:
        return False
    if type(field.name) is not str or "." in field.name:
        return False
    value_type = type(literal.value)
    if value_type is int:
        return -(1 << 63) <= cast(int, literal.value) < (1 << 63)
    if value_type in {bool, str, bytes}:
        return root.kind in {"==", "!="}
    return False


def direct_exact_equality(operation: MapOp | FilterOp) -> tuple[str, object] | None:
    """Return one side-effect-free field/builtin equality usable by early Arrow execution."""
    if not isinstance(operation, FilterOp) or operation.negate:
        return None
    root = getattr(operation.predicate, "_node", None)
    if not isinstance(root, Binary) or root.kind != "==":
        return None
    if isinstance(root.left, Field) and isinstance(root.right, RowLiteral):
        field, literal = root.left, root.right
    elif isinstance(root.left, RowLiteral) and isinstance(root.right, Field):
        field, literal = root.right, root.left
    else:
        return None
    if type(field.name) is not str or "." in field.name:
        return None
    value = literal.value
    value_type = type(value)
    if value_type is int:
        if not -(1 << 63) <= cast(int, value) < (1 << 63):
            return None
    elif value_type not in {bool, str, bytes}:
        return None
    return field.name, value


def direct_exact_i64_range(operation: MapOp | FilterOp) -> RangePredicate | None:
    """Normalize one exact field/i64 range comparison into field-left form."""
    if not isinstance(operation, FilterOp) or operation.negate:
        return None
    root = getattr(operation.predicate, "_node", None)
    if not isinstance(root, Binary) or root.kind not in {"<", "<=", ">", ">="}:
        return None
    if isinstance(root.left, Field) and isinstance(root.right, RowLiteral):
        field, literal, operator = root.left, root.right, root.kind
    elif isinstance(root.left, RowLiteral) and isinstance(root.right, Field):
        field, literal = root.right, root.left
        operator = {"<": ">", "<=": ">=", ">": "<", ">=": "<="}[root.kind]
    else:
        return None
    value = literal.value
    if (
        type(field.name) is not str
        or "." in field.name
        or type(value) is not int
        or not -(1 << 63) <= value < (1 << 63)
    ):
        return None
    normalized = cast(Literal["<", "<=", ">", ">="], operator)
    return field.name, normalized, value


def plan_arrow_first_prefix(plan: Pipeline) -> ArrowPrefixPlan | None:
    """Select only Arrow shapes whose complete result can stop after one surviving row."""
    if plan.engine != "auto" or not isinstance(
        descriptor := plan.source.native_data, ArrowBatchSource
    ):
        return None
    if not plan.operations:
        if descriptor.kind not in {"csv", "parquet"}:
            return None
        return ArrowPrefixPlan(
            0,
            (),
            ArrowBoundaryReason.FULL_PREFIX,
            True,
            first_only=True,
        )

    prefix = plan_arrow_prefix(plan)
    if prefix is None or prefix.operation_count != len(plan.operations):
        return None
    operations = prefix.operations
    if len(operations) == 1:
        operation = operations[0]
        if not (
            (isinstance(operation, MapOp) and prefix.projection is not None)
            or direct_exact_equality(operation) is not None
        ):
            return None
    elif not (
        len(operations) == 2
        and direct_exact_equality(operations[0]) is not None
        and isinstance(operations[1], MapOp)
        and prefix.projection is not None
    ):
        return None
    return ArrowPrefixPlan(
        prefix.operation_count,
        prefix.operations,
        prefix.boundary_reason,
        prefix.guarded,
        prefix.projection,
        True,
    )


def plan_arrow_prefix(plan: Pipeline) -> ArrowPrefixPlan | None:
    """Return a conservative Arrow-safe leading map/filter segment for an automatic plan.

    Forced engines and non-Arrow sources return ``None``. Planning stops at the first
    unsupported operation or opaque Python callable; accepted expression nodes remain guarded
    because runtime schema and kernel support are checked by the Arrow executor. A direct
    projection is eligible only as the plan's sole operation; finding one after another stage
    discards that tentative prefix so Arrow expression semantics cannot leak into the result.
    """
    if plan.engine != "auto":
        return None
    if not isinstance(plan.source.native_data, ArrowBatchSource):
        return None
    accepted: list[MapOp | FilterOp] = []
    for index, operation in enumerate(plan.operations):
        if not isinstance(operation, (MapOp, FilterOp)):
            return ArrowPrefixPlan(
                len(accepted),
                tuple(accepted),
                ArrowBoundaryReason.UNSUPPORTED_OPERATION,
                True,
            )
        # Only a lone exact direct select may cross the row-wrapper boundary.  Combining it
        # with an expression stage would make Arrow's overflow, null, cast, and operator
        # protocols observable before the canonical Python projection.
        callable_node = getattr(operation, "function", getattr(operation, "predicate", None))
        if callable_node.__class__.__name__ == "RowExpr":
            if isinstance(operation, FilterOp) and operation.negate:
                return ArrowPrefixPlan(
                    0,
                    (),
                    ArrowBoundaryReason.UNSUPPORTED_EXPRESSION,
                    True,
                )
            accepted.append(operation)
            continue
        projection = _direct_projection(operation)
        if projection is not None and len(plan.operations) == 1:
            accepted.append(operation)
            return ArrowPrefixPlan(
                len(accepted),
                tuple(accepted),
                ArrowBoundaryReason.FULL_PREFIX,
                True,
                projection,
            )
        if (
            projection is not None
            and index == 1
            and len(plan.operations) == 2
            and len(accepted) == 1
            and _direct_primitive_filter(accepted[0])
        ):
            accepted.append(operation)
            return ArrowPrefixPlan(
                len(accepted),
                tuple(accepted),
                ArrowBoundaryReason.FULL_PREFIX,
                True,
                projection,
            )
        if projection is not None:
            return ArrowPrefixPlan(0, (), ArrowBoundaryReason.OPAQUE_EXPRESSION, True)
        # Internal Rows wrappers carry Python selection, copy, and equality semantics.  If one
        # follows a tentative RowExpr prefix, executing only that prefix would expose Arrow's
        # arithmetic/null behavior before the wrapper falls back.  Discard the whole tentative
        # batch program; ordinary opaque callbacks keep the established prefix behavior.
        if type(callable_node) is PlannedRowCallable:
            return ArrowPrefixPlan(0, (), ArrowBoundaryReason.OPAQUE_EXPRESSION, True)
        return ArrowPrefixPlan(
            len(accepted), tuple(accepted), ArrowBoundaryReason.OPAQUE_EXPRESSION, True
        )
    return ArrowPrefixPlan(len(accepted), tuple(accepted), ArrowBoundaryReason.FULL_PREFIX, True)
