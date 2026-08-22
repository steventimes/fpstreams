"""Build composable row expressions backed by explicit IR nodes."""

from __future__ import annotations

import operator
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

from .row_eval import LazyRowEvaluator
from .row_ir import (
    Binary,
    Call,
    Cast,
    Coalesce,
    Field,
    GetItem,
    IfElse,
    Index,
    InputRow,
    IsNull,
    Literal,
    Path,
    PythonUDF,
    Unary,
    analyze_row_node,
)
from .selectors import Selector, compile_selector


@dataclass(frozen=True, slots=True, eq=False)
class RowExpr:
    """Represent a labeled row expression as both a callable and an inspectable IR graph.

    Direct construction wraps the supplied evaluator as an opaque PythonUDF. Expressions
    created by this module retain their specific nodes and compile lazily on first evaluation.
    """

    _evaluate: Callable[[Any], Any]
    label: str
    _node: Any = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        """Wrap a directly supplied evaluator as a PythonUDF receiving the complete input row."""
        object.__setattr__(self, "_node", PythonUDF(self._evaluate, (InputRow(),)))

    @classmethod
    def _from_node(cls, node: Any, label: str) -> RowExpr:
        """Construct an expression around an existing IR node and attach a lazy row evaluator.

        Bypassing the dataclass initializer preserves the supplied node instead of wrapping the
        evaluator as an opaque PythonUDF.
        """
        evaluator = LazyRowEvaluator(node)
        instance = cls.__new__(cls)
        object.__setattr__(instance, "_evaluate", evaluator)
        object.__setattr__(instance, "label", label)
        object.__setattr__(instance, "_node", node)
        return instance

    def __call__(self, row: Any) -> Any:
        """Evaluate this expression for one row.

        Selector failures, operator errors, and exceptions from user callables propagate from
        the underlying evaluator.
        """
        return self._evaluate(row)

    def inspect(self) -> Any:
        """Analyze field use and execution properties without evaluating the expression."""
        return analyze_row_node(self._node)

    @staticmethod
    def _coerce(value: object) -> RowExpr:
        """Return a RowExpr unchanged or wrap any other object in a Literal node."""
        if isinstance(value, RowExpr):
            return value
        return RowExpr._from_node(Literal(value), repr(value))

    def _binary(
        self,
        other: object,
        function: Callable[[Any, Any], Any],
        symbol: str,
    ) -> RowExpr:
        """Build a Binary node with this expression on the left and a coerced right operand.

        Evaluation dispatches from symbol stored in the IR. function is accepted by this shared
        overload helper but is not stored in the resulting node.
        """
        right = self._coerce(other)
        return RowExpr._from_node(
            Binary(symbol, self._node, right._node),
            f"({self.label} {symbol} {right.label})",
        )

    def _reverse(
        self,
        other: object,
        function: Callable[[Any, Any], Any],
        symbol: str,
    ) -> RowExpr:
        """Build a Binary node with a coerced left operand and this expression on the right.

        Evaluation dispatches from symbol stored in the IR. function is accepted by this shared
        reflected-overload helper but is not stored in the resulting node.
        """
        left = self._coerce(other)
        return RowExpr._from_node(
            Binary(symbol, left._node, self._node),
            f"({left.label} {symbol} {self.label})",
        )

    def __add__(self, other: object) -> RowExpr:
        """Build an expression adding the right operand to this expression's value."""
        return self._binary(other, operator.add, "+")

    def __radd__(self, other: object) -> RowExpr:
        """Build an expression adding this expression's value to the left operand."""
        return self._reverse(other, operator.add, "+")

    def __sub__(self, other: object) -> RowExpr:
        """Build an expression subtracting the right operand from this expression's value."""
        return self._binary(other, operator.sub, "-")

    def __rsub__(self, other: object) -> RowExpr:
        """Build an expression subtracting this expression's value from the left operand."""
        return self._reverse(other, operator.sub, "-")

    def __mul__(self, other: object) -> RowExpr:
        """Build an expression multiplying this expression's value by the right operand."""
        return self._binary(other, operator.mul, "*")

    def __rmul__(self, other: object) -> RowExpr:
        """Build an expression multiplying the left operand by this expression's value."""
        return self._reverse(other, operator.mul, "*")

    def __truediv__(self, other: object) -> RowExpr:
        """Build an expression dividing this expression's value by the right operand."""
        return self._binary(other, operator.truediv, "/")

    def __rtruediv__(self, other: object) -> RowExpr:
        """Build an expression dividing the left operand by this expression's value."""
        return self._reverse(other, operator.truediv, "/")

    def __floordiv__(self, other: object) -> RowExpr:
        """Build an expression floor-dividing this expression's value by the right operand."""
        return self._binary(other, operator.floordiv, "//")

    def __rfloordiv__(self, other: object) -> RowExpr:
        """Build an expression floor-dividing the left operand by this expression's value."""
        return self._reverse(other, operator.floordiv, "//")

    def __mod__(self, other: object) -> RowExpr:
        """Build an expression taking this expression's value modulo the right operand."""
        return self._binary(other, operator.mod, "%")

    def __rmod__(self, other: object) -> RowExpr:
        """Build an expression taking the left operand modulo this expression's value."""
        return self._reverse(other, operator.mod, "%")

    def __pow__(self, other: object) -> RowExpr:
        """Build an expression raising this expression's value to the right operand."""
        return self._binary(other, operator.pow, "**")

    def __rpow__(self, other: object) -> RowExpr:
        """Build an expression raising the left operand to this expression's value."""
        return self._reverse(other, operator.pow, "**")

    def __eq__(self, other: object) -> RowExpr:  # type: ignore[override]  # builds a RowExpr
        """Build an expression comparing this expression's value equal to the right operand."""
        return self._binary(other, operator.eq, "==")

    def __ne__(self, other: object) -> RowExpr:  # type: ignore[override]  # builds a RowExpr
        """Build an expression comparing this expression's value unequal to the right operand."""
        return self._binary(other, operator.ne, "!=")

    def __lt__(self, other: object) -> RowExpr:
        """Build an expression testing whether this expression's value is less than the right."""
        return self._binary(other, operator.lt, "<")

    def __le__(self, other: object) -> RowExpr:
        """Build an expression testing whether this expression's value is at most the right."""
        return self._binary(other, operator.le, "<=")

    def __gt__(self, other: object) -> RowExpr:
        """Build an expression testing whether this expression's value is greater than the right."""
        return self._binary(other, operator.gt, ">")

    def __ge__(self, other: object) -> RowExpr:
        """Build an expression testing whether this expression's value is at least the right."""
        return self._binary(other, operator.ge, ">=")

    def __and__(self, other: object) -> RowExpr:
        """Build a Boolean conjunction that skips the right node when the left is falsy.

        Evaluated operands are converted to bool, so the expression returns True or False rather
        than one of the original operand values.
        """
        right = self._coerce(other)
        return RowExpr._from_node(
            Binary("and", self._node, right._node), f"({self.label} & {right.label})"
        )

    def __or__(self, other: object) -> RowExpr:
        """Build a Boolean disjunction that skips the right node when the left is truthy.

        Evaluated operands are converted to bool, so the expression returns True or False rather
        than one of the original operand values.
        """
        right = self._coerce(other)
        return RowExpr._from_node(
            Binary("or", self._node, right._node), f"({self.label} | {right.label})"
        )

    def __invert__(self) -> RowExpr:
        """Build an expression returning not bool(value)."""
        return RowExpr._from_node(Unary("not", self._node), f"~{self.label}")

    def __neg__(self) -> RowExpr:
        """Build an expression applying arithmetic negation to the produced value."""
        return RowExpr._from_node(Unary("neg", self._node), f"-{self.label}")

    def __abs__(self) -> RowExpr:
        """Build an expression applying abs to the produced value."""
        return RowExpr._from_node(Unary("abs", self._node), f"abs({self.label})")

    def __getitem__(self, key: object) -> RowExpr:
        """Build a GetItem node that indexes the produced value with the supplied literal key."""
        return RowExpr._from_node(GetItem(self._node, Literal(key)), f"{self.label}[{key!r}]")

    def __bool__(self) -> bool:
        """Reject eager truth testing so expressions must be combined with ampersand or pipe."""
        raise TypeError("combine row expressions with '&' or '|', not 'and' or 'or'")

    def map(self, function: Callable[[Any], Any]) -> RowExpr:
        """Apply a Python callable to this expression's value during row evaluation.

        The callable is stored as a PythonUDF, so structural inspection marks the resulting graph
        opaque and cannot infer its field dependencies.
        """
        return RowExpr._from_node(PythonUDF(function, (self._node,)), f"map({self.label})")

    def cast(self, target: Callable[[Any], Any]) -> RowExpr:
        """Call target with this expression's value during row evaluation.

        target may be a type or any one-argument callable; its normal return value and exceptions
        are preserved.
        """
        return RowExpr._from_node(Cast(self._node, target), f"cast({self.label})")

    def isin(self, values: Iterable[Any]) -> RowExpr:
        """Materialize values as a tuple immediately, then test membership during row evaluation.

        Consuming the iterable at construction makes later evaluations reuse the same choices.
        """
        choices = tuple(values)
        return RowExpr._from_node(
            Call("isin", (self._node, Literal(choices))), f"{self.label}.isin(...)"
        )

    def is_null(self) -> RowExpr:
        """Build an identity test for whether the produced value is None."""
        return RowExpr._from_node(IsNull(self._node), f"{self.label}.is_null()")

    def is_not_null(self) -> RowExpr:
        """Build a non-null test by logically inverting this expression's IsNull node."""
        return ~self.is_null()

    def fill_null(self, value: object) -> RowExpr:
        """Return this expression's value unless it is None, then evaluate the replacement.

        The replacement is coerced to a row expression at construction and remains unevaluated
        for rows whose primary value is non-null.
        """
        replacement = self._coerce(value)

        return RowExpr._from_node(
            Coalesce((self._node, replacement._node)),
            f"{self.label}.fill_null({replacement.label})",
        )

    def coalesce(self, *fallbacks: object) -> RowExpr:
        """Delegate to coalesce with this expression as the first candidate."""
        return coalesce(self, *fallbacks)

    def lower(self) -> RowExpr:
        """Call the produced value's lower method when the expression is evaluated."""
        return RowExpr._from_node(Call("lower", (self._node,)), f"lower({self.label})")

    def upper(self) -> RowExpr:
        """Call the produced value's upper method when the expression is evaluated."""
        return RowExpr._from_node(Call("upper", (self._node,)), f"upper({self.label})")

    def strip(self) -> RowExpr:
        """Call the produced value's strip method when the expression is evaluated."""
        return RowExpr._from_node(Call("strip", (self._node,)), f"strip({self.label})")

    def contains(self, value: object) -> RowExpr:
        """Test whether the supplied literal value belongs to the produced container."""
        return RowExpr._from_node(
            Call("contains", (self._node, Literal(value))),
            f"{self.label}.contains({value!r})",
        )


def col(selector: Selector) -> RowExpr:
    """Build a row expression from a callable, integer index, or string selector.

    A one-part string becomes a Field node, a dotted string becomes a Path, and an integer
    becomes an Index. A callable, including another callable expression, is wrapped as an
    opaque PythonUDF receiving the entire row. Selector lookup errors become SelectionError
    when the expression is evaluated.
    """
    select = compile_selector(selector)
    if isinstance(selector, str):
        parts = tuple(selector.split("."))
        node: Any = Field(selector) if len(parts) == 1 else Path(parts, selector)
    elif isinstance(selector, int):
        node = Index(selector)
    else:
        node = PythonUDF(select, (InputRow(),))
    return RowExpr._from_node(node, str(selector))


def lit(value: object) -> RowExpr:
    """Coerce a value into a row expression.

    Existing RowExpr instances are returned unchanged; every other object becomes a Literal
    that returns the same object for each row.
    """
    return RowExpr._coerce(value)


def coalesce(*values: object) -> RowExpr:
    """Build a short-circuiting expression that returns the first non-None candidate.

    Values are coerced to expressions at construction and evaluated from left to right for
    each row. Later candidates are skipped after a non-null result. Calling without values
    raises ValueError immediately.
    """
    if not values:
        raise ValueError("coalesce requires at least one value")
    expressions = tuple(RowExpr._coerce(value) for value in values)

    return RowExpr._from_node(
        Coalesce(tuple(expression._node for expression in expressions)),
        f"coalesce({', '.join(expression.label for expression in expressions)})",
    )


def when(condition: RowExpr, then: object, otherwise: object = None) -> RowExpr:
    """Choose between two expressions from the truth value of a row condition.

    then and otherwise are coerced at construction. Evaluation computes the condition first
    and evaluates only the selected branch; otherwise defaults to a None literal.
    """
    positive = RowExpr._coerce(then)
    negative = RowExpr._coerce(otherwise)
    return RowExpr._from_node(
        IfElse(condition._node, positive._node, negative._node),
        f"when({condition.label})",
    )
