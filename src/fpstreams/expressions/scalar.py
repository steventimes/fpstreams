"""Scalar expression trees shared by Python and native execution."""

from __future__ import annotations

import operator
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any, Generic, Literal, TypeVar, cast

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
_OPCODE_NAMES = {opcode: name for name, opcode in _OPCODES.items()}
_UNARY_OPCODES = {_OPCODES["neg"], _OPCODES["not"], _OPCODES["abs"]}
_COMPOSED_EVALUATOR_LIMIT = 128

InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")


@dataclass(frozen=True, slots=True)
class _EvaluatorNode(Generic[InputT, OutputT]):
    kind: Literal["item", "const", "call"]
    value: OutputT | Callable[[InputT], OutputT] | None = None


def _binary_node(
    operation: Callable[[OutputT, OutputT], OutputT],
    left: _EvaluatorNode[InputT, OutputT],
    right: _EvaluatorNode[InputT, OutputT],
    *,
    item_converter: Callable[[InputT], OutputT] | None,
) -> _EvaluatorNode[InputT, OutputT]:
    if left.kind == "const" and right.kind == "const":
        return _EvaluatorNode(
            "const",
            operation(cast(OutputT, left.value), cast(OutputT, right.value)),
        )
    if left.kind == "item" and right.kind == "item":
        if item_converter is None:
            return _EvaluatorNode(
                "call",
                lambda item: operation(cast(OutputT, item), cast(OutputT, item)),
            )
        return _EvaluatorNode(
            "call",
            lambda item: operation(item_converter(item), item_converter(item)),
        )
    if left.kind == "item" and right.kind == "const":
        right_constant = cast(OutputT, right.value)
        if item_converter is None:
            return _EvaluatorNode(
                "call",
                lambda item: operation(cast(OutputT, item), right_constant),
            )
        return _EvaluatorNode(
            "call",
            lambda item: operation(item_converter(item), right_constant),
        )
    if left.kind == "const" and right.kind == "item":
        left_constant = cast(OutputT, left.value)
        if item_converter is None:
            return _EvaluatorNode(
                "call",
                lambda item: operation(left_constant, cast(OutputT, item)),
            )
        return _EvaluatorNode(
            "call",
            lambda item: operation(left_constant, item_converter(item)),
        )
    if left.kind == "call" and right.kind == "const":
        left_evaluator = cast(Callable[[InputT], OutputT], left.value)
        right_constant = cast(OutputT, right.value)
        return _EvaluatorNode(
            "call",
            lambda item: operation(left_evaluator(item), right_constant),
        )
    if left.kind == "const" and right.kind == "call":
        left_constant = cast(OutputT, left.value)
        right_evaluator = cast(Callable[[InputT], OutputT], right.value)
        return _EvaluatorNode(
            "call",
            lambda item: operation(left_constant, right_evaluator(item)),
        )
    if left.kind == "item" and right.kind == "call":
        right_evaluator = cast(Callable[[InputT], OutputT], right.value)
        if item_converter is None:
            return _EvaluatorNode(
                "call",
                lambda item: operation(cast(OutputT, item), right_evaluator(item)),
            )
        return _EvaluatorNode(
            "call",
            lambda item: operation(item_converter(item), right_evaluator(item)),
        )
    if left.kind == "call" and right.kind == "item":
        left_evaluator = cast(Callable[[InputT], OutputT], left.value)
        if item_converter is None:
            return _EvaluatorNode(
                "call",
                lambda item: operation(left_evaluator(item), cast(OutputT, item)),
            )
        return _EvaluatorNode(
            "call",
            lambda item: operation(left_evaluator(item), item_converter(item)),
        )
    left_evaluator = cast(Callable[[InputT], OutputT], left.value)
    right_evaluator = cast(Callable[[InputT], OutputT], right.value)
    return _EvaluatorNode(
        "call",
        lambda item: operation(left_evaluator(item), right_evaluator(item)),
    )


def _unary_node(
    operation: Callable[[OutputT], OutputT],
    operand: _EvaluatorNode[InputT, OutputT],
    *,
    item_converter: Callable[[InputT], OutputT] | None,
) -> _EvaluatorNode[InputT, OutputT]:
    if operand.kind == "const":
        return _EvaluatorNode("const", operation(cast(OutputT, operand.value)))
    if operand.kind == "item":
        if item_converter is None:
            return _EvaluatorNode("call", lambda item: operation(cast(OutputT, item)))
        return _EvaluatorNode("call", lambda item: operation(item_converter(item)))
    evaluator = cast(Callable[[InputT], OutputT], operand.value)
    return _EvaluatorNode("call", lambda item: operation(evaluator(item)))


def _node_evaluator(
    node: _EvaluatorNode[InputT, OutputT],
    *,
    item_converter: Callable[[InputT], OutputT] | None,
) -> Callable[[InputT], OutputT]:
    if node.kind == "call":
        return cast(Callable[[InputT], OutputT], node.value)
    if node.kind == "item":
        if item_converter is not None:
            return item_converter
        return lambda item: cast(OutputT, item)
    constant = cast(OutputT, node.value)
    return lambda _item: constant


def _compose_evaluator(
    instructions: tuple[tuple[int, OutputT], ...],
    *,
    binary_operations: Mapping[str, Callable[[OutputT, OutputT], OutputT]],
    unary_operations: Mapping[int, Callable[[OutputT], OutputT]],
    item_converter: Callable[[InputT], OutputT] | None,
    description: str,
) -> Callable[[InputT], OutputT]:
    values: list[_EvaluatorNode[InputT, OutputT]] = []
    for opcode, operand in instructions:
        if opcode == _OPCODES["item"]:
            values.append(_EvaluatorNode("item"))
            continue
        if opcode == _OPCODES["const"]:
            values.append(_EvaluatorNode("const", operand))
            continue
        if opcode in _UNARY_OPCODES:
            if not values:
                raise RuntimeError(f"malformed {description}: missing operand")
            values.append(
                _unary_node(
                    unary_operations[opcode],
                    values.pop(),
                    item_converter=item_converter,
                )
            )
            continue
        if len(values) < 2:
            raise RuntimeError(f"malformed {description}: missing right operand")
        right = values.pop()
        left = values.pop()
        values.append(
            _binary_node(
                binary_operations[_OPCODE_NAMES[opcode]],
                left,
                right,
                item_converter=item_converter,
            )
        )
    if len(values) != 1:
        raise RuntimeError(f"malformed {description}: unexpected operands")
    return _node_evaluator(values[0], item_converter=item_converter)


def _postorder_instructions(
    expression: Any,
    *,
    default_operand: int | float,
) -> tuple[tuple[int, int | float], ...]:
    instructions: list[tuple[int, int | float]] = []
    pending = [(expression, False)]
    while pending:
        current, visited = pending.pop()
        if not visited:
            pending.append((current, True))
            if current.right is not None:
                pending.append((current.right, False))
            if current.left is not None:
                pending.append((current.left, False))
            continue
        operand = current.value if current.operation == "const" else default_operand
        instructions.append((_OPCODES[current.operation], operand or default_operand))
    return tuple(instructions)


def _flat_int_evaluator(
    instructions: tuple[tuple[int, int], ...],
) -> Callable[[int], int | bool]:
    def evaluate(item: int) -> int | bool:
        values: list[int | bool] = []
        for opcode, operand in instructions:
            if opcode == _OPCODES["item"]:
                values.append(item)
                continue
            if opcode == _OPCODES["const"]:
                values.append(operand)
                continue
            if opcode in _UNARY_OPCODES:
                if not values:
                    raise RuntimeError("malformed expression: missing operand")
                value = values.pop()
                if opcode == _OPCODES["neg"]:
                    values.append(-value)
                elif opcode == _OPCODES["abs"]:
                    values.append(abs(value))
                else:
                    values.append(not bool(value))
                continue
            if len(values) < 2:
                raise RuntimeError("malformed expression: missing right operand")
            right = values.pop()
            left = values.pop()
            operation = _OPCODE_NAMES[opcode]
            values.append(_BINARY[operation](left, right))
        if len(values) != 1:
            raise RuntimeError("malformed expression: unexpected operands")
        return values[0]

    return evaluate


def _int_neg(value: int | bool) -> int | bool:
    return -value


def _int_abs(value: int | bool) -> int | bool:
    return abs(value)


def _int_not(value: int | bool) -> int | bool:
    return not bool(value)


_INT_UNARY: dict[int, Callable[[int | bool], int | bool]] = {
    _OPCODES["neg"]: _int_neg,
    _OPCODES["abs"]: _int_abs,
    _OPCODES["not"]: _int_not,
}


@lru_cache(maxsize=1_024)
def _compile_int_evaluator(
    instructions: tuple[tuple[int, int], ...],
) -> Callable[[int], int | bool]:
    if len(instructions) > _COMPOSED_EVALUATOR_LIMIT:
        return _flat_int_evaluator(instructions)
    return _compose_evaluator(
        instructions,
        binary_operations=_BINARY,
        unary_operations=_INT_UNARY,
        item_converter=None,
        description="expression",
    )


@dataclass(frozen=True, slots=True, eq=False)
class Expr:
    """A callable expression that can also be compiled by a native executor."""

    operation: str
    left: Expr | None = None
    right: Expr | None = None
    value: int | None = None
    kind: ExpressionKind = "int"
    _instructions: tuple[tuple[int, int], ...] | None = field(
        default=None, init=False, repr=False, compare=False
    )
    _evaluator: Callable[[int], int | bool] | None = field(
        default=None, init=False, repr=False, compare=False
    )

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
        return self._python_evaluator()(item)

    def _python_evaluator(self) -> Callable[[int], int | bool]:
        """Return the cached callable used by Python execution loops."""
        evaluator = self._evaluator
        if evaluator is None:
            evaluator = _compile_int_evaluator(self.native_instructions())
            object.__setattr__(self, "_evaluator", evaluator)
        return evaluator

    def native_instructions(self) -> tuple[tuple[int, int], ...]:
        """Compile this expression into native executor instructions.

        Returns:
            A tuple containing the resulting values.
        """
        instructions = self._instructions
        if instructions is None:
            instructions = cast(
                tuple[tuple[int, int], ...],
                _postorder_instructions(self, default_operand=0),
            )
            object.__setattr__(self, "_instructions", instructions)
        return instructions


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


def _flat_float_evaluator(
    instructions: tuple[tuple[int, float], ...],
) -> Callable[[float], float | bool]:
    def evaluate(item: float) -> float | bool:
        values: list[float | bool] = []
        for opcode, operand in instructions:
            if opcode == _OPCODES["item"]:
                values.append(float(item))
                continue
            if opcode == _OPCODES["const"]:
                values.append(operand)
                continue
            if opcode in _UNARY_OPCODES:
                if not values:
                    raise RuntimeError("malformed float expression: missing operand")
                value = values.pop()
                if opcode == _OPCODES["neg"]:
                    values.append(-value)
                elif opcode == _OPCODES["abs"]:
                    values.append(abs(value))
                else:
                    values.append(not bool(value))
                continue
            if len(values) < 2:
                raise RuntimeError("malformed float expression: missing right operand")
            right = values.pop()
            left = values.pop()
            operation = _OPCODE_NAMES[opcode]
            values.append(_FLOAT_BINARY[operation](left, right))
        if len(values) != 1:
            raise RuntimeError("malformed float expression: unexpected operands")
        return values[0]

    return evaluate


def _float_neg(value: float | bool) -> float | bool:
    return -value


def _float_abs(value: float | bool) -> float | bool:
    return abs(value)


def _float_not(value: float | bool) -> float | bool:
    return not bool(value)


_FLOAT_UNARY: dict[int, Callable[[float | bool], float | bool]] = {
    _OPCODES["neg"]: _float_neg,
    _OPCODES["abs"]: _float_abs,
    _OPCODES["not"]: _float_not,
}


@lru_cache(maxsize=1_024)
def _compile_float_evaluator(
    instructions: tuple[tuple[int, float], ...],
) -> Callable[[float], float | bool]:
    if len(instructions) > _COMPOSED_EVALUATOR_LIMIT:
        return _flat_float_evaluator(instructions)
    return _compose_evaluator(
        instructions,
        binary_operations=_FLOAT_BINARY,
        unary_operations=_FLOAT_UNARY,
        item_converter=float,
        description="float expression",
    )


@dataclass(frozen=True, slots=True, eq=False)
class FExpr:
    """A callable floating-point expression compiled by the f64 executor."""

    operation: str
    left: FExpr | None = None
    right: FExpr | None = None
    value: float | None = None
    kind: FloatExpressionKind = "float"
    _instructions: tuple[tuple[int, float], ...] | None = field(
        default=None, init=False, repr=False, compare=False
    )
    _evaluator: Callable[[float], float | bool] | None = field(
        default=None, init=False, repr=False, compare=False
    )

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
        return self._python_evaluator()(item)

    def _python_evaluator(self) -> Callable[[float], float | bool]:
        """Return the cached callable used by Python execution loops."""
        evaluator = self._evaluator
        if evaluator is None:
            evaluator = _compile_float_evaluator(self.native_instructions())
            object.__setattr__(self, "_evaluator", evaluator)
        return evaluator

    def native_instructions(self) -> tuple[tuple[int, float], ...]:
        """Compile this expression into native executor instructions.

        Returns:
            A tuple containing the resulting values.
        """
        instructions = self._instructions
        if instructions is None:
            instructions = cast(
                tuple[tuple[int, float], ...],
                _postorder_instructions(self, default_operand=0.0),
            )
            object.__setattr__(self, "_instructions", instructions)
        return instructions


fitem = FExpr("item")
