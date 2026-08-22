"""Compile integer and float expression trees for Python and native numeric execution."""

from __future__ import annotations

import ast
import operator
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any, Literal, cast

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
_AST_EVALUATOR_LIMIT = 128


_AST_BINARY_OPERATORS: dict[str, type[ast.operator]] = {
    "add": ast.Add,
    "sub": ast.Sub,
    "mul": ast.Mult,
    "floordiv": ast.FloorDiv,
    "mod": ast.Mod,
    "truediv": ast.Div,
}
_AST_COMPARISON_OPERATORS: dict[str, type[ast.cmpop]] = {
    "eq": ast.Eq,
    "ne": ast.NotEq,
    "lt": ast.Lt,
    "le": ast.LtE,
    "gt": ast.Gt,
    "ge": ast.GtE,
}


def _ast_binary(kind: str, left: ast.expr, right: ast.expr) -> ast.expr:
    """Build one controlled Python operator node from a scalar opcode name."""
    binary = _AST_BINARY_OPERATORS.get(kind)
    if binary is not None:
        return ast.BinOp(left=left, op=binary(), right=right)
    comparison = _AST_COMPARISON_OPERATORS.get(kind)
    if comparison is not None:
        return ast.Compare(left=left, ops=[comparison()], comparators=[right])
    if kind in {"and", "or"}:
        # Scalar expressions historically evaluate both operands and normalize both to
        # bool. Bitwise combination of those booleans retains that eager behavior.
        boolean_left = ast.Call(func=ast.Name("_bool", ast.Load()), args=[left], keywords=[])
        boolean_right = ast.Call(func=ast.Name("_bool", ast.Load()), args=[right], keywords=[])
        operator_node: ast.operator = ast.BitAnd() if kind == "and" else ast.BitOr()
        return ast.BinOp(left=boolean_left, op=operator_node, right=boolean_right)
    raise RuntimeError(f"unknown scalar expression operation: {kind}")


def _ast_evaluator(
    instructions: tuple[tuple[int, int | float], ...],
    *,
    convert_item_to_float: bool,
    description: str,
) -> Callable[[Any], Any]:
    """Compile a short, validated postfix program into one restricted Python lambda.

    Every AST node comes from the closed opcode vocabulary below; values can only enter as
    numeric constants or the lambda argument. The restricted globals therefore contain no
    user-controlled names or general builtins.
    """
    values: list[ast.expr] = []
    for opcode, operand in instructions:
        if opcode == _OPCODES["item"]:
            item_node: ast.expr = ast.Name("_item", ast.Load())
            if convert_item_to_float:
                item_node = ast.Call(
                    func=ast.Name("_float", ast.Load()), args=[item_node], keywords=[]
                )
            values.append(item_node)
            continue
        if opcode == _OPCODES["const"]:
            values.append(ast.Constant(operand))
            continue
        if opcode in _UNARY_OPCODES:
            if not values:
                raise RuntimeError(f"malformed {description}: missing operand")
            value = values.pop()
            if opcode == _OPCODES["neg"]:
                values.append(ast.UnaryOp(op=ast.USub(), operand=value))
            elif opcode == _OPCODES["abs"]:
                values.append(
                    ast.Call(func=ast.Name("_abs", ast.Load()), args=[value], keywords=[])
                )
            else:
                values.append(ast.UnaryOp(op=ast.Not(), operand=value))
            continue
        if len(values) < 2:
            raise RuntimeError(f"malformed {description}: missing right operand")
        right = values.pop()
        left = values.pop()
        operation = _OPCODE_NAMES.get(opcode)
        if operation is None:
            raise RuntimeError(f"malformed {description}: unknown opcode {opcode}")
        values.append(_ast_binary(operation, left, right))
    if len(values) != 1:
        raise RuntimeError(f"malformed {description}: unexpected operands")

    expression = ast.Expression(
        body=ast.Lambda(
            args=ast.arguments(
                posonlyargs=[],
                args=[ast.arg(arg="_item")],
                kwonlyargs=[],
                kw_defaults=[],
                defaults=[],
            ),
            body=values[0],
        )
    )
    code = compile(ast.fix_missing_locations(expression), "<fpstreams-expression>", "eval")
    namespace = {"__builtins__": {}, "_abs": abs, "_bool": bool, "_float": float}
    return cast(Callable[[Any], Any], eval(code, namespace))


def _postorder_instructions(
    expression: Any,
    *,
    default_operand: int | float,
) -> tuple[tuple[int, int | float], ...]:
    """Flatten an expression tree iteratively into postfix opcode/operand instructions.

    Constant instructions carry their value. Every other opcode carries default_operand,
    which is ignored by both Python evaluators and the native executor for non-constants.
    Unknown operation names fail through the opcode lookup.
    """
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
    """Build the explicit value-stack interpreter used for long integer programs."""

    def evaluate(item: int) -> int | bool:
        """Execute the integer postfix program with item bound to each item instruction.

        Malformed operand counts raise RuntimeError; arithmetic and comparison failures propagate
        from their Python operators.
        """
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


@lru_cache(maxsize=1_024)
def _compile_int_evaluator(
    instructions: tuple[tuple[int, int], ...],
) -> Callable[[int], int | bool]:
    """Compile and LRU-cache an integer evaluator for one instruction tuple.

    Programs of at most 128 instructions use one restricted generated Python function.
    Longer programs use the explicit value stack to avoid a large chain of closures. The
    global cache retains up to 1,024 compiled evaluators.
    """
    if len(instructions) > _AST_EVALUATOR_LIMIT:
        return _flat_int_evaluator(instructions)
    return cast(
        Callable[[int], int | bool],
        _ast_evaluator(
            instructions,
            convert_item_to_float=False,
            description="expression",
        ),
    )


@dataclass(frozen=True, slots=True, eq=False)
class Expr:
    """Represent an integer expression for Python evaluation and native i64 execution.

    operation, left, right, and value form the tree; kind records whether the result is
    integer or Boolean for planning. Postfix instructions and the Python evaluator are
    populated lazily in per-instance cache fields.
    """

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
        """Return a constant integer expression after exact-type validation.

        bool and all non-int objects raise TypeError even though bool is an int subclass.
        """
        if type(value) is not int:
            raise TypeError("native expressions currently accept integer constants")
        return Expr("const", value=value)

    @staticmethod
    def _coerce(value: object) -> Expr:
        """Keep an Expr operand or convert an exact int into a constant expression.

        Unsupported operand types raise TypeError naming their runtime type.
        """
        if isinstance(value, Expr):
            return value
        if type(value) is int:
            return Expr.constant(value)
        raise TypeError(f"unsupported expression operand: {type(value).__name__}")

    def _binary(self, operation: str, other: object, *, kind: ExpressionKind = "int") -> Expr:
        """Create a binary node with this expression on the left and a coerced right operand.

        kind records whether the new node produces an integer or Boolean result.
        """
        return Expr(operation, self, self._coerce(other), kind=kind)

    def _reverse(self, operation: str, other: object) -> Expr:
        """Build an arithmetic node with a coerced left operand and this value on the right."""
        return Expr(operation, self._coerce(other), self)

    def __add__(self, other: object) -> Expr:
        """Build an integer addition node with this expression on the left."""
        return self._binary("add", other)

    def __radd__(self, other: object) -> Expr:
        """Build an integer addition node with this expression on the right."""
        return self._reverse("add", other)

    def __sub__(self, other: object) -> Expr:
        """Build an integer subtraction node with this expression on the left."""
        return self._binary("sub", other)

    def __rsub__(self, other: object) -> Expr:
        """Build an integer subtraction node with this expression on the right."""
        return self._reverse("sub", other)

    def __mul__(self, other: object) -> Expr:
        """Build an integer multiplication node with this expression on the left."""
        return self._binary("mul", other)

    def __rmul__(self, other: object) -> Expr:
        """Build an integer multiplication node with this expression on the right."""
        return self._reverse("mul", other)

    def __floordiv__(self, other: object) -> Expr:
        """Build an integer floor-division node with this expression on the left."""
        return self._binary("floordiv", other)

    def __rfloordiv__(self, other: object) -> Expr:
        """Build an integer floor-division node with this expression on the right."""
        return self._reverse("floordiv", other)

    def __mod__(self, other: object) -> Expr:
        """Build an integer remainder node with this expression on the left."""
        return self._binary("mod", other)

    def __rmod__(self, other: object) -> Expr:
        """Build an integer remainder node with this expression on the right."""
        return self._reverse("mod", other)

    def __neg__(self) -> Expr:
        """Build an arithmetic-negation node for this expression."""
        return Expr("neg", self)

    def __abs__(self) -> Expr:
        """Build an absolute-value node for this expression."""
        return Expr("abs", self)

    def __and__(self, other: object) -> Expr:
        """Build a Boolean conjunction expression.

        Both scalar operand subtrees are evaluated by the postfix program before their truth
        values are combined; unlike row-expression and, this operation does not short-circuit.
        """
        return self._binary("and", other, kind="bool")

    def __or__(self, other: object) -> Expr:
        """Build a Boolean disjunction expression.

        Both scalar operand subtrees are evaluated by the postfix program before their truth
        values are combined; unlike row-expression or, this operation does not short-circuit.
        """
        return self._binary("or", other, kind="bool")

    def __invert__(self) -> Expr:
        """Build a logical-not expression; tilde is not treated as integer bitwise complement."""
        return Expr("not", self, kind="bool")

    def __eq__(self, other: object) -> Expr:  # type: ignore[override]  # builds an Expr
        """Build a Boolean node comparing this expression equal to a coerced operand."""
        return self._binary("eq", other, kind="bool")

    def __ne__(self, other: object) -> Expr:  # type: ignore[override]  # builds an Expr
        """Build a Boolean node comparing this expression unequal to a coerced operand."""
        return self._binary("ne", other, kind="bool")

    def __lt__(self, other: object) -> Expr:
        """Build a Boolean less-than node with this expression on the left."""
        return self._binary("lt", other, kind="bool")

    def __le__(self, other: object) -> Expr:
        """Build a Boolean less-than-or-equal node with this expression on the left."""
        return self._binary("le", other, kind="bool")

    def __gt__(self, other: object) -> Expr:
        """Build a Boolean greater-than node with this expression on the left."""
        return self._binary("gt", other, kind="bool")

    def __ge__(self, other: object) -> Expr:
        """Build a Boolean greater-than-or-equal node with this expression on the left."""
        return self._binary("ge", other, kind="bool")

    def __bool__(self) -> bool:
        """Reject truth-testing an unevaluated integer expression with TypeError."""
        raise TypeError("expressions cannot be used as booleans before evaluation")

    def __call__(self, item: int) -> int | bool:
        """Evaluate the expression with one input bound to item.

        Python evaluation does not enforce the int annotation at runtime. The lazily compiled
        evaluator is reused on later calls. Malformed trees raise
        RuntimeError during compilation or stack execution; operator exceptions propagate.
        """
        return self._python_evaluator()(item)

    def _python_evaluator(self) -> Callable[[int], int | bool]:
        """Return the per-instance Python evaluator, compiling and caching it on first use.

        Compilation uses the module-level LRU cache, so identical instruction tuples can share
        the same callable across different Expr instances.
        """
        evaluator = self._evaluator
        if evaluator is None:
            evaluator = _compile_int_evaluator(self.native_instructions())
            object.__setattr__(self, "_evaluator", evaluator)
        return evaluator

    def native_instructions(self) -> tuple[tuple[int, int], ...]:
        """Return and cache this tree's postfix instructions for Python or native execution.

        Each pair contains a numeric opcode and an integer operand. Only const opcodes consume
        the operand; other instructions store zero as a placeholder.
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
    """Build the explicit value-stack interpreter used for long float programs."""

    def evaluate(item: float) -> float | bool:
        """Execute the float postfix program, converting item instructions to float.

        Malformed operand counts raise RuntimeError; arithmetic and comparison failures propagate
        from their Python operators.
        """
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


@lru_cache(maxsize=1_024)
def _compile_float_evaluator(
    instructions: tuple[tuple[int, float], ...],
) -> Callable[[float], float | bool]:
    """Compile and LRU-cache a float evaluator for one instruction tuple.

    Programs of at most 128 instructions use one restricted function and convert each item to
    float. Longer programs use the explicit value stack. The global cache retains up to 1,024
    compiled evaluators.
    """
    if len(instructions) > _AST_EVALUATOR_LIMIT:
        return _flat_float_evaluator(instructions)
    return cast(
        Callable[[float], float | bool],
        _ast_evaluator(
            instructions,
            convert_item_to_float=True,
            description="float expression",
        ),
    )


@dataclass(frozen=True, slots=True, eq=False)
class FExpr:
    """Represent a float expression for Python evaluation and native f64 execution.

    operation, left, right, and value form the tree; kind records whether the result is float
    or Boolean for planning. Postfix instructions and the Python evaluator are populated
    lazily in per-instance cache fields.
    """

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
        """Return a float constant expression from an exact int or float.

        The value is converted immediately to float. bool and every other type raise TypeError.
        """
        if type(value) not in (int, float):
            raise TypeError("native float expressions accept int or float constants")
        return FExpr("const", value=float(value))

    @staticmethod
    def _coerce(value: object) -> FExpr:
        """Keep an FExpr operand or convert an exact int/float into a constant expression.

        Unsupported operand types raise TypeError naming their runtime type.
        """
        if isinstance(value, FExpr):
            return value
        if type(value) in (int, float):
            return FExpr.constant(value)  # type: ignore[arg-type]  # narrowed above
        raise TypeError(f"unsupported float expression operand: {type(value).__name__}")

    def _binary(
        self,
        operation: str,
        other: object,
        *,
        kind: FloatExpressionKind = "float",
    ) -> FExpr:
        """Create a binary node with this expression on the left and a coerced right operand.

        kind records whether the new node produces a float or Boolean result.
        """
        return FExpr(operation, self, self._coerce(other), kind=kind)

    def _reverse(self, operation: str, other: object) -> FExpr:
        """Build an arithmetic node with a coerced left operand and this value on the right."""
        return FExpr(operation, self._coerce(other), self)

    def __add__(self, other: object) -> FExpr:
        """Build a float addition node with this expression on the left."""
        return self._binary("add", other)

    def __radd__(self, other: object) -> FExpr:
        """Build a float addition node with this expression on the right."""
        return self._reverse("add", other)

    def __sub__(self, other: object) -> FExpr:
        """Build a float subtraction node with this expression on the left."""
        return self._binary("sub", other)

    def __rsub__(self, other: object) -> FExpr:
        """Build a float subtraction node with this expression on the right."""
        return self._reverse("sub", other)

    def __mul__(self, other: object) -> FExpr:
        """Build a float multiplication node with this expression on the left."""
        return self._binary("mul", other)

    def __rmul__(self, other: object) -> FExpr:
        """Build a float multiplication node with this expression on the right."""
        return self._reverse("mul", other)

    def __truediv__(self, other: object) -> FExpr:
        """Build a float true-division node with this expression on the left."""
        return self._binary("truediv", other)

    def __rtruediv__(self, other: object) -> FExpr:
        """Build a float true-division node with this expression on the right."""
        return self._reverse("truediv", other)

    def __neg__(self) -> FExpr:
        """Build an arithmetic-negation node for this float expression."""
        return FExpr("neg", self)

    def __abs__(self) -> FExpr:
        """Build an absolute-value node for this float expression."""
        return FExpr("abs", self)

    def __and__(self, other: object) -> FExpr:
        """Build a Boolean conjunction expression.

        Both scalar operand subtrees are evaluated by the postfix program before their truth
        values are combined; this operation does not short-circuit.
        """
        return self._binary("and", other, kind="bool")

    def __or__(self, other: object) -> FExpr:
        """Build a Boolean disjunction expression.

        Both scalar operand subtrees are evaluated by the postfix program before their truth
        values are combined; this operation does not short-circuit.
        """
        return self._binary("or", other, kind="bool")

    def __invert__(self) -> FExpr:
        """Build a logical-not expression; tilde is not a numeric bitwise operation."""
        return FExpr("not", self, kind="bool")

    def __eq__(self, other: object) -> FExpr:  # type: ignore[override]  # builds an FExpr
        """Build a Boolean node comparing this expression equal to a coerced operand."""
        return self._binary("eq", other, kind="bool")

    def __ne__(self, other: object) -> FExpr:  # type: ignore[override]  # builds an FExpr
        """Build a Boolean node comparing this expression unequal to a coerced operand."""
        return self._binary("ne", other, kind="bool")

    def __lt__(self, other: object) -> FExpr:
        """Build a Boolean less-than node with this expression on the left."""
        return self._binary("lt", other, kind="bool")

    def __le__(self, other: object) -> FExpr:
        """Build a Boolean less-than-or-equal node with this expression on the left."""
        return self._binary("le", other, kind="bool")

    def __gt__(self, other: object) -> FExpr:
        """Build a Boolean greater-than node with this expression on the left."""
        return self._binary("gt", other, kind="bool")

    def __ge__(self, other: object) -> FExpr:
        """Build a Boolean greater-than-or-equal node with this expression on the left."""
        return self._binary("ge", other, kind="bool")

    def __bool__(self) -> bool:
        """Reject truth-testing an unevaluated float expression with TypeError."""
        raise TypeError("float expressions cannot be used as booleans before evaluation")

    def __call__(self, item: float) -> float | bool:
        """Evaluate the expression with one numeric input bound to fitem.

        Item instructions convert the input to float, and the lazily compiled evaluator is reused
        on later calls. Malformed trees raise RuntimeError; operator exceptions propagate.
        """
        return self._python_evaluator()(item)

    def _python_evaluator(self) -> Callable[[float], float | bool]:
        """Return the per-instance float evaluator, compiling and caching it on first use.

        Compilation uses the module-level LRU cache, so identical instruction tuples can share
        the same callable across different FExpr instances.
        """
        evaluator = self._evaluator
        if evaluator is None:
            evaluator = _compile_float_evaluator(self.native_instructions())
            object.__setattr__(self, "_evaluator", evaluator)
        return evaluator

    def native_instructions(self) -> tuple[tuple[int, float], ...]:
        """Return and cache this tree's postfix instructions for Python or native execution.

        Each pair contains a numeric opcode and a float operand. Only const opcodes consume the
        operand; other instructions store 0.0 as a placeholder.
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
