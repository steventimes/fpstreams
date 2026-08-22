"""Conservative typed/effect metadata for existing public expression objects."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from .row import RowExpr
from .row_ir import PythonUDF, _children
from .scalar import Expr, FExpr


class Effect(StrEnum):
    """Whether evaluating an expression is structurally pure or callback-bound."""

    PURE = "pure"
    MAY_RAISE = "may_raise"
    PYTHON_CALLBACK = "python_callback"


class ValueType(StrEnum):
    """A deliberately small, conservative value-type lattice."""

    INT64 = "int64"
    FLOAT64 = "float64"
    BOOL = "bool"
    STRING = "string"
    OBJECT = "object"
    UNKNOWN = "unknown"


class ExpressionSource(StrEnum):
    """Public family from which a typed expression was lowered."""

    INTEGER = "integer"
    FLOAT = "float"
    ROW = "row"
    SELECTOR = "selector"
    CALLBACK = "callback"


@dataclass(frozen=True, slots=True)
class TypedExpr:
    """Existing expression root paired with only safe planning metadata."""

    root: object
    value_type: ValueType
    effect: Effect
    backends: frozenset[str]
    source: ExpressionSource


def _row_effect(root: object) -> Effect:
    """Find a Python UDF barrier iteratively without evaluating the expression tree."""
    stack = [root]
    while stack:
        node = stack.pop()
        if isinstance(node, PythonUDF):
            return Effect.PYTHON_CALLBACK
        stack.extend(_children(node))
    return Effect.MAY_RAISE


def lower_expression(value: object) -> TypedExpr:
    """Classify an existing public expression without invoking user code."""
    if isinstance(value, Expr):
        return TypedExpr(
            value,
            ValueType.INT64,
            Effect.MAY_RAISE,
            frozenset({"python", "rust"}),
            ExpressionSource.INTEGER,
        )
    if isinstance(value, FExpr):
        return TypedExpr(
            value,
            ValueType.FLOAT64,
            Effect.MAY_RAISE,
            frozenset({"python", "rust"}),
            ExpressionSource.FLOAT,
        )
    if isinstance(value, RowExpr):
        effect = _row_effect(value._node)
        return TypedExpr(
            value._node,
            ValueType.UNKNOWN,
            effect,
            (
                frozenset({"python"})
                if effect is Effect.PYTHON_CALLBACK
                else frozenset({"python", "arrow"})
            ),
            ExpressionSource.ROW,
        )
    if isinstance(value, (str, int)):
        return TypedExpr(
            value,
            ValueType.UNKNOWN,
            Effect.MAY_RAISE,
            frozenset({"python", "arrow"}),
            ExpressionSource.SELECTOR,
        )
    if callable(value):
        return TypedExpr(
            value,
            ValueType.UNKNOWN,
            Effect.PYTHON_CALLBACK,
            frozenset({"python"}),
            ExpressionSource.CALLBACK,
        )
    raise TypeError(f"unsupported expression: {type(value).__name__}")
