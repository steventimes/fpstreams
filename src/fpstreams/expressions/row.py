"""Composable expressions evaluated against record-like rows."""

from __future__ import annotations

import operator
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from .selectors import Selector, compile_selector


@dataclass(frozen=True, slots=True, eq=False)
class RowExpr:
    """A callable expression evaluated against one row."""

    _evaluate: Callable[[Any], Any]
    label: str

    def __call__(self, row: Any) -> Any:
        """Evaluate this expression for one row.

        Args:
            row: The record-like value used to resolve selectors and fields.

        Returns:
            The value produced by this expression.
        """
        return self._evaluate(row)

    @staticmethod
    def _coerce(value: object) -> RowExpr:
        if isinstance(value, RowExpr):
            return value
        return RowExpr(lambda _row: value, repr(value))

    def _binary(
        self,
        other: object,
        function: Callable[[Any, Any], Any],
        symbol: str,
    ) -> RowExpr:
        right = self._coerce(other)
        return RowExpr(
            lambda row: function(self(row), right(row)),
            f"({self.label} {symbol} {right.label})",
        )

    def _reverse(
        self,
        other: object,
        function: Callable[[Any, Any], Any],
        symbol: str,
    ) -> RowExpr:
        left = self._coerce(other)
        return RowExpr(
            lambda row: function(left(row), self(row)),
            f"({left.label} {symbol} {self.label})",
        )

    def __add__(self, other: object) -> RowExpr:
        return self._binary(other, operator.add, "+")

    def __radd__(self, other: object) -> RowExpr:
        return self._reverse(other, operator.add, "+")

    def __sub__(self, other: object) -> RowExpr:
        return self._binary(other, operator.sub, "-")

    def __rsub__(self, other: object) -> RowExpr:
        return self._reverse(other, operator.sub, "-")

    def __mul__(self, other: object) -> RowExpr:
        return self._binary(other, operator.mul, "*")

    def __rmul__(self, other: object) -> RowExpr:
        return self._reverse(other, operator.mul, "*")

    def __truediv__(self, other: object) -> RowExpr:
        return self._binary(other, operator.truediv, "/")

    def __rtruediv__(self, other: object) -> RowExpr:
        return self._reverse(other, operator.truediv, "/")

    def __floordiv__(self, other: object) -> RowExpr:
        return self._binary(other, operator.floordiv, "//")

    def __rfloordiv__(self, other: object) -> RowExpr:
        return self._reverse(other, operator.floordiv, "//")

    def __mod__(self, other: object) -> RowExpr:
        return self._binary(other, operator.mod, "%")

    def __rmod__(self, other: object) -> RowExpr:
        return self._reverse(other, operator.mod, "%")

    def __pow__(self, other: object) -> RowExpr:
        return self._binary(other, operator.pow, "**")

    def __rpow__(self, other: object) -> RowExpr:
        return self._reverse(other, operator.pow, "**")

    def __eq__(self, other: object) -> RowExpr:  # type: ignore[override]
        return self._binary(other, operator.eq, "==")

    def __ne__(self, other: object) -> RowExpr:  # type: ignore[override]
        return self._binary(other, operator.ne, "!=")

    def __lt__(self, other: object) -> RowExpr:
        return self._binary(other, operator.lt, "<")

    def __le__(self, other: object) -> RowExpr:
        return self._binary(other, operator.le, "<=")

    def __gt__(self, other: object) -> RowExpr:
        return self._binary(other, operator.gt, ">")

    def __ge__(self, other: object) -> RowExpr:
        return self._binary(other, operator.ge, ">=")

    def __and__(self, other: object) -> RowExpr:
        right = self._coerce(other)
        return RowExpr(
            lambda row: bool(self(row)) and bool(right(row)),
            f"({self.label} & {right.label})",
        )

    def __or__(self, other: object) -> RowExpr:
        right = self._coerce(other)
        return RowExpr(
            lambda row: bool(self(row)) or bool(right(row)),
            f"({self.label} | {right.label})",
        )

    def __invert__(self) -> RowExpr:
        return RowExpr(lambda row: not bool(self(row)), f"~{self.label}")

    def __neg__(self) -> RowExpr:
        return RowExpr(lambda row: -self(row), f"-{self.label}")

    def __abs__(self) -> RowExpr:
        return RowExpr(lambda row: abs(self(row)), f"abs({self.label})")

    def __getitem__(self, key: object) -> RowExpr:
        return RowExpr(lambda row: self(row)[key], f"{self.label}[{key!r}]")

    def __bool__(self) -> bool:
        raise TypeError("combine row expressions with '&' or '|', not 'and' or 'or'")

    def map(self, function: Callable[[Any], Any]) -> RowExpr:
        """Transform the value produced by this row expression.

        The callable runs when the expression is evaluated for a row, not when the expression is
        created.

        Args:
            function: The callable applied by this operation.

        Returns:
            A new row expression that applies the callable when evaluated.
        """
        return RowExpr(lambda row: function(self(row)), f"map({self.label})")

    def cast(self, target: Callable[[Any], Any]) -> RowExpr:
        """Convert the produced value with target.

        Args:
            target: The callable or type used to convert a value.

        Returns:
            A new row expression that converts the selected value.
        """
        return self.map(target)

    def isin(self, values: Iterable[Any]) -> RowExpr:
        """Test whether the produced value belongs to values.

        Args:
            values: The values consumed by this operation.

        Returns:
            A boolean row expression testing membership.
        """
        choices = tuple(values)
        return RowExpr(lambda row: self(row) in choices, f"{self.label}.isin(...)")

    def is_null(self) -> RowExpr:
        """Test whether the produced value is None.

        Returns:
            A boolean row expression testing for `None`.
        """
        return RowExpr(lambda row: self(row) is None, f"{self.label}.is_null()")

    def is_not_null(self) -> RowExpr:
        """Test whether the produced value is not None.

        Returns:
            A boolean row expression testing for a non-`None` value.
        """
        return ~self.is_null()

    def fill_null(self, value: object) -> RowExpr:
        """Replace a None result with value.

        Args:
            value: The value consumed by this operation.

        Returns:
            A new row expression with the fallback value.
        """
        replacement = self._coerce(value)

        def evaluate(row: Any) -> Any:
            current = self(row)
            return replacement(row) if current is None else current

        return RowExpr(
            evaluate,
            f"{self.label}.fill_null({replacement.label})",
        )

    def coalesce(self, *fallbacks: object) -> RowExpr:
        """Return the first non-None result from this expression and fallbacks.

        Args:
            *fallbacks: Fallback expressions checked from left to right.

        Returns:
            A new row expression returning the first non-`None` value.
        """
        return coalesce(self, *fallbacks)

    def lower(self) -> RowExpr:
        """Convert the produced string to lowercase.

        Returns:
            A new row expression producing lowercase strings.
        """
        return self.map(lambda value: value.lower())

    def upper(self) -> RowExpr:
        """Convert the produced string to uppercase.

        Returns:
            A new row expression producing uppercase strings.
        """
        return self.map(lambda value: value.upper())

    def strip(self) -> RowExpr:
        """Remove leading and trailing whitespace from the produced string.

        Returns:
            A new row expression producing stripped strings.
        """
        return self.map(lambda value: value.strip())

    def contains(self, value: object) -> RowExpr:
        """Test whether value occurs in the produced container.

        Args:
            value: The value consumed by this operation.

        Returns:
            A boolean row expression testing containment.
        """
        return RowExpr(lambda row: value in self(row), f"{self.label}.contains({value!r})")


def col(selector: Selector) -> RowExpr:
    """Build a row expression from a field, index, attribute, path, or callable.

    Args:
        selector: A callable, field name, index, path, or expression used to select a value.

    Returns:
        A row expression that reads the selected value when evaluated.
    """
    select = compile_selector(selector)
    return RowExpr(select, str(selector))


def lit(value: object) -> RowExpr:
    """Build a row expression that returns the same literal value for every row.

    Args:
        value: The value consumed by this operation.

    Returns:
        A row expression that returns the literal value when evaluated.
    """
    return RowExpr._coerce(value)


def coalesce(*values: object) -> RowExpr:
    """Build a row expression returning the first non-None value.

    Args:
        *values: Values supplied to this operation in encounter order.

    Returns:
        A row expression that returns the first non-`None` value.
    """
    if not values:
        raise ValueError("coalesce requires at least one value")
    expressions = tuple(RowExpr._coerce(value) for value in values)

    def evaluate(row: Any) -> Any:
        for expression in expressions:
            value = expression(row)
            if value is not None:
                return value
        return None

    return RowExpr(
        evaluate,
        f"coalesce({', '.join(expression.label for expression in expressions)})",
    )


def when(condition: RowExpr, then: object, otherwise: object = None) -> RowExpr:
    """Build a conditional row expression.

    Args:
        condition: The condition evaluated for each row.
        then: The value or expression returned when the condition is true.
        otherwise: The value or expression returned when the condition is false.

    Returns:
        A row expression that chooses between the two branches.
    """
    positive = RowExpr._coerce(then)
    negative = RowExpr._coerce(otherwise)
    return RowExpr(
        lambda row: positive(row) if bool(condition(row)) else negative(row),
        f"when({condition.label})",
    )
