"""Find Arrow-executable map/filter prefixes without importing optional PyArrow."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from threading import RLock
from types import CodeType, FunctionType
from typing import Any, Literal, TypeVar, cast
from weakref import WeakKeyDictionary

from ..expressions.row import RowExpr
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

    kind: Literal["with_columns", "where", "select", "rename", "cast", "fill_nulls"]
    selectors: tuple[tuple[str, object], ...] = ()
    predicate: object | None = None
    equalities: tuple[tuple[str, object], ...] = ()


@dataclass(frozen=True, slots=True)
class _RowStageMetadata:
    """Bind one trusted function identity to its query-local structural descriptor."""

    token: object
    code: CodeType
    descriptor: RowStageDescriptor


_RowFunction = TypeVar("_RowFunction", bound=Callable[..., Any])


def _row_stage_registry() -> tuple[
    Callable[[_RowFunction, RowStageDescriptor], _RowFunction],
    Callable[[object], RowStageDescriptor | None],
]:
    """Create private identity metadata helpers without exposing the registry token."""
    token = object()
    metadata: WeakKeyDictionary[FunctionType, _RowStageMetadata] = WeakKeyDictionary()
    lock = RLock()

    def register(function: _RowFunction, descriptor: RowStageDescriptor) -> _RowFunction:
        """Register an exact function and return that same callable unchanged."""
        if type(function) is not FunctionType:
            raise TypeError("planned row stages require an exact Python function")
        with lock:
            metadata[cast(FunctionType, function)] = _RowStageMetadata(
                token,
                cast(FunctionType, function).__code__,
                descriptor,
            )
        return function

    def descriptor(function: object) -> RowStageDescriptor | None:
        """Read only metadata installed for this exact function identity."""
        if type(function) is not FunctionType:
            return None
        with lock:
            registered = metadata.get(function)
        if (
            registered is None
            or registered.token is not token
            or function.__code__ is not registered.code
        ):
            return None
        return registered.descriptor

    return register, descriptor


_register_row_stage, _row_stage_descriptor = _row_stage_registry()
del _row_stage_registry


def _direct_projection(operation: MapOp | FilterOp) -> ArrowProjectionSpec | None:
    """Return an ordered direct-field spec for an exact internal ``Rows.select`` map."""
    if not isinstance(operation, MapOp):
        return None
    descriptor = _row_stage_descriptor(operation.function)
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
    predicate = operation.predicate
    if type(predicate) is not RowExpr:
        return False
    root = predicate._node
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
    predicate = operation.predicate
    if type(predicate) is not RowExpr:
        return None
    root = predicate._node
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
    predicate = operation.predicate
    if type(predicate) is not RowExpr:
        return None
    root = predicate._node
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


def plan_arrow_reduction_prefix(plan: Pipeline) -> ArrowPrefixPlan | None:
    """Select a total scalar projection that is safe to evaluate before a reduction.

    Reduction callbacks and comparisons must retain their Python order.  A lone direct field
    map is the only initial shape whose batch evaluation cannot run user code or introduce a
    later expression error before the terminal observes an earlier value.
    """
    if plan.engine != "auto" or not isinstance(plan.source.native_data, ArrowBatchSource):
        return None
    operations = plan.operations
    if plan.parallel is not None or not operations or operations[1:]:
        return None
    operation = operations[0]
    if not isinstance(operation, MapOp):
        return None
    function = operation.function
    if type(function) is not RowExpr:
        return None
    root = function._node
    if type(root) is not Field or type(root.name) is not str or "." in root.name:
        return None
    prefix = plan_arrow_prefix(plan)
    if prefix is None or prefix.operation_count != 1 or prefix.operations[0] is not operation:
        return None
    return prefix


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
        if type(callable_node) is RowExpr:
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
        if _row_stage_descriptor(callable_node) is not None:
            return ArrowPrefixPlan(0, (), ArrowBoundaryReason.OPAQUE_EXPRESSION, True)
        return ArrowPrefixPlan(
            len(accepted), tuple(accepted), ArrowBoundaryReason.OPAQUE_EXPRESSION, True
        )
    return ArrowPrefixPlan(len(accepted), tuple(accepted), ArrowBoundaryReason.FULL_PREFIX, True)


def supports_arrow_table_materialization(prefix: ArrowPrefixPlan) -> bool:
    """Return whether a complete prefix can stay columnar through table materialization."""
    operations = prefix.operations
    if not operations:
        return True
    if prefix.projection is None:
        return all(_direct_primitive_filter(operation) for operation in operations)
    return bool(prefix.projection.selectors) and (
        (len(operations) == 1 and isinstance(operations[0], MapOp))
        or (
            len(operations) == 2
            and _direct_primitive_filter(operations[0])
            and isinstance(operations[1], MapOp)
        )
    )


def plan_arrow_table_prefix(plan: Pipeline) -> ArrowPrefixPlan | None:
    """Return a full Arrow prefix whose output can remain a native table."""
    prefix = plan_arrow_prefix(plan)
    if (
        prefix is None
        or prefix.operation_count != len(plan.operations)
        or not supports_arrow_table_materialization(prefix)
    ):
        return None
    return prefix
