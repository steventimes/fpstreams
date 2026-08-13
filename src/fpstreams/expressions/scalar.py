"""Scalar expression trees shared by Python and native execution."""

from __future__ import annotations

import operator
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

ExpressionKind = Literal["int", "bool"]

_OPCODES = {
    "item": 0,
    "const": 1,
    "add": 2,
    "sub": 3,
    "mul": 4,
    "floordiv": 5,
    "mod": 6,
    "neg": 7,
    "eq": 8,
    "ne": 9,
    "lt": 10,
    "le": 11,
    "gt": 12,
    "ge": 13,
    "and": 14,
    "or": 15,
    "not": 16,
    "abs": 17,
    "truediv": 18,
}

_BINARY: dict[str, Callable[[int | bool, int | bool], int | bool]] = {
    "add": operator.add,
    "sub": operator.sub,
    "mul": operator.mul,
    "floordiv": operator.floordiv,
    "mod": operator.mod,
    "eq": operator.eq,
    "ne": operator.ne,
    "lt": operator.lt,
    "le": operator.le,
    "gt": operator.gt,
    "ge": operator.ge,
    "and": lambda left, right: bool(left) and bool(right),
    "or": lambda left, right: bool(left) or bool(right),
}


@dataclass(frozen=True, slots=True, eq=False)
class Expr:
    """A callable expression that can also be compiled by a native executor."""

    operation: str
    left: Expr | None = None
    right: Expr | None = None
    value: int | None = None
    kind: ExpressionKind = "int"

    @staticmethod
    def constant(value: int) -> Expr:
        """Build an integer expression that always returns value.

        Args:
            value: The value consumed by this operation.

        Returns:
            A composable scalar expression.
        """
        if type(value) is not int:
            raise TypeError("native expressions currently accept integer constants")
        return Expr("const", value=value)

    @staticmethod
    def _coerce(value: object) -> Expr:
        if isinstance(value, Expr):
            return value
        if type(value) is int:
            return Expr.constant(value)
        raise TypeError(f"unsupported expression operand: {type(value).__name__}")

    def _binary(self, operation: str, other: object, *, kind: ExpressionKind = "int") -> Expr:
        return Expr(operation, self, self._coerce(other), kind=kind)

    def _reverse(self, operation: str, other: object) -> Expr:
        return Expr(operation, self._coerce(other), self)

    def __add__(self, other: object) -> Expr:
        return self._binary("add", other)

    def __radd__(self, other: object) -> Expr:
        return self._reverse("add", other)

    def __sub__(self, other: object) -> Expr:
        return self._binary("sub", other)

    def __rsub__(self, other: object) -> Expr:
        return self._reverse("sub", other)

    def __mul__(self, other: object) -> Expr:
        return self._binary("mul", other)

    def __rmul__(self, other: object) -> Expr:
        return self._reverse("mul", other)

    def __floordiv__(self, other: object) -> Expr:
        return self._binary("floordiv", other)

    def __rfloordiv__(self, other: object) -> Expr:
        return self._reverse("floordiv", other)

    def __mod__(self, other: object) -> Expr:
        return self._binary("mod", other)

    def __rmod__(self, other: object) -> Expr:
        return self._reverse("mod", other)

    def __neg__(self) -> Expr:
        return Expr("neg", self)

    def __abs__(self) -> Expr:
        return Expr("abs", self)

    def __and__(self, other: object) -> Expr:
        return self._binary("and", other, kind="bool")

    def __or__(self, other: object) -> Expr:
        return self._binary("or", other, kind="bool")

    def __invert__(self) -> Expr:
        return Expr("not", self, kind="bool")

    def __eq__(self, other: object) -> Expr:  # type: ignore[override]
        return self._binary("eq", other, kind="bool")

    def __ne__(self, other: object) -> Expr:  # type: ignore[override]
        return self._binary("ne", other, kind="bool")

    def __lt__(self, other: object) -> Expr:
        return self._binary("lt", other, kind="bool")

    def __le__(self, other: object) -> Expr:
        return self._binary("le", other, kind="bool")

    def __gt__(self, other: object) -> Expr:
        return self._binary("gt", other, kind="bool")

    def __ge__(self, other: object) -> Expr:
        return self._binary("ge", other, kind="bool")

    def __bool__(self) -> bool:
        raise TypeError("expressions cannot be used as booleans before evaluation")

    def __call__(self, item: int) -> int | bool:
        """Evaluate this expression for one integer item.

        Args:
            item: The integer bound to `item` while evaluating the expression.

        Returns:
            The computed integer or boolean result.

        Raises:
            RuntimeError: If the expression tree is malformed.
        """
        if self.operation == "item":
            return item
        if self.operation == "const":
            return self.value  # type: ignore[return-value]
        if self.left is None:
            raise RuntimeError("malformed expression: missing operand")
        left = self.left(item)
        if self.operation == "neg":
            return -left
        if self.operation == "abs":
            return abs(left)
        if self.operation == "not":
            return not bool(left)
        if self.right is None:
            raise RuntimeError("malformed expression: missing right operand")
        return _BINARY[self.operation](left, self.right(item))

    def native_instructions(self) -> tuple[tuple[int, int], ...]:
        """Compile this expression into native executor instructions.

        Returns:
            A tuple containing the resulting values.
        """
        instructions: list[tuple[int, int]] = []

        def emit(expression: Expr) -> None:
            if expression.left is not None:
                emit(expression.left)
            if expression.right is not None:
                emit(expression.right)
            operand = expression.value if expression.operation == "const" else 0
            instructions.append((_OPCODES[expression.operation], operand or 0))

        emit(self)
        return tuple(instructions)


item = Expr("item")


FloatExpressionKind = Literal["float", "bool"]
_FLOAT_BINARY: dict[str, Callable[[float | bool, float | bool], float | bool]] = {
    "add": operator.add,
    "sub": operator.sub,
    "mul": operator.mul,
    "truediv": operator.truediv,
    "eq": operator.eq,
    "ne": operator.ne,
    "lt": operator.lt,
    "le": operator.le,
    "gt": operator.gt,
    "ge": operator.ge,
    "and": lambda left, right: bool(left) and bool(right),
    "or": lambda left, right: bool(left) or bool(right),
}


@dataclass(frozen=True, slots=True, eq=False)
class FExpr:
    """A callable floating-point expression compiled by the f64 executor."""

    operation: str
    left: FExpr | None = None
    right: FExpr | None = None
    value: float | None = None
    kind: FloatExpressionKind = "float"

    @staticmethod
    def constant(value: int | float) -> FExpr:
        """Build a floating-point expression that always returns value.

        Args:
            value: The value consumed by this operation.

        Returns:
            A composable scalar expression.
        """
        if type(value) not in (int, float):
            raise TypeError("native float expressions accept int or float constants")
        return FExpr("const", value=float(value))

    @staticmethod
    def _coerce(value: object) -> FExpr:
        if isinstance(value, FExpr):
            return value
        if type(value) in (int, float):
            return FExpr.constant(value)  # type: ignore[arg-type]
        raise TypeError(f"unsupported float expression operand: {type(value).__name__}")

    def _binary(
        self,
        operation: str,
        other: object,
        *,
        kind: FloatExpressionKind = "float",
    ) -> FExpr:
        return FExpr(operation, self, self._coerce(other), kind=kind)

    def _reverse(self, operation: str, other: object) -> FExpr:
        return FExpr(operation, self._coerce(other), self)

    def __add__(self, other: object) -> FExpr:
        return self._binary("add", other)

    def __radd__(self, other: object) -> FExpr:
        return self._reverse("add", other)

    def __sub__(self, other: object) -> FExpr:
        return self._binary("sub", other)

    def __rsub__(self, other: object) -> FExpr:
        return self._reverse("sub", other)

    def __mul__(self, other: object) -> FExpr:
        return self._binary("mul", other)

    def __rmul__(self, other: object) -> FExpr:
        return self._reverse("mul", other)

    def __truediv__(self, other: object) -> FExpr:
        return self._binary("truediv", other)

    def __rtruediv__(self, other: object) -> FExpr:
        return self._reverse("truediv", other)

    def __neg__(self) -> FExpr:
        return FExpr("neg", self)

    def __abs__(self) -> FExpr:
        return FExpr("abs", self)

    def __and__(self, other: object) -> FExpr:
        return self._binary("and", other, kind="bool")

    def __or__(self, other: object) -> FExpr:
        return self._binary("or", other, kind="bool")

    def __invert__(self) -> FExpr:
        return FExpr("not", self, kind="bool")

    def __eq__(self, other: object) -> FExpr:  # type: ignore[override]
        return self._binary("eq", other, kind="bool")

    def __ne__(self, other: object) -> FExpr:  # type: ignore[override]
        return self._binary("ne", other, kind="bool")

    def __lt__(self, other: object) -> FExpr:
        return self._binary("lt", other, kind="bool")

    def __le__(self, other: object) -> FExpr:
        return self._binary("le", other, kind="bool")

    def __gt__(self, other: object) -> FExpr:
        return self._binary("gt", other, kind="bool")

    def __ge__(self, other: object) -> FExpr:
        return self._binary("ge", other, kind="bool")

    def __bool__(self) -> bool:
        raise TypeError("float expressions cannot be used as booleans before evaluation")

    def __call__(self, item: float) -> float | bool:
        """Evaluate this expression for one numeric item.

        Args:
            item: The number bound to `fitem` while evaluating the expression.

        Returns:
            The computed floating-point or boolean result.

        Raises:
            RuntimeError: If the expression tree is malformed.
        """
        if self.operation == "item":
            return float(item)
        if self.operation == "const":
            return self.value  # type: ignore[return-value]
        if self.left is None:
            raise RuntimeError("malformed float expression: missing operand")
        left = self.left(item)
        if self.operation == "neg":
            return -left
        if self.operation == "abs":
            return abs(left)
        if self.operation == "not":
            return not bool(left)
        if self.right is None:
            raise RuntimeError("malformed float expression: missing right operand")
        return _FLOAT_BINARY[self.operation](left, self.right(item))

    def native_instructions(self) -> tuple[tuple[int, float], ...]:
        """Compile this expression into native executor instructions.

        Returns:
            A tuple containing the resulting values.
        """
        instructions: list[tuple[int, float]] = []

        def emit(expression: FExpr) -> None:
            if expression.left is not None:
                emit(expression.left)
            if expression.right is not None:
                emit(expression.right)
            operand = expression.value if expression.operation == "const" else 0.0
            instructions.append((_OPCODES[expression.operation], operand or 0.0))

        emit(self)
        return tuple(instructions)


fitem = FExpr("item")
