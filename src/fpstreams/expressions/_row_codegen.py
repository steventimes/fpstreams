"""Build safe, direct evaluators for the closed row-expression IR subset."""

from __future__ import annotations

import ast
from collections.abc import Callable, Mapping
from typing import Any, cast

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
)

_MAX_DEPTH = 128
_MAX_OCCURRENCES = 512

_ARITHMETIC_OPERATORS: dict[str, ast.operator] = {
    "+": ast.Add(),
    "-": ast.Sub(),
    "*": ast.Mult(),
    "/": ast.Div(),
    "//": ast.FloorDiv(),
    "%": ast.Mod(),
    "**": ast.Pow(),
}
_COMPARISON_OPERATORS: dict[str, ast.cmpop] = {
    "==": ast.Eq(),
    "!=": ast.NotEq(),
    "<": ast.Lt(),
    "<=": ast.LtE(),
    ">": ast.Gt(),
    ">=": ast.GtE(),
}
_EXACT_CASTS = (float, int, str, bool)


def _contains(container: Any, value: Any) -> bool:
    """Evaluate membership after both row-expression arguments have been evaluated."""
    return value in container


def _isin(value: Any, choices: Any) -> bool:
    """Evaluate membership after both row-expression arguments have been evaluated."""
    return value in choices


def compile_row_evaluator(
    root: object, selectors: Mapping[int, Callable[[Any], Any]]
) -> Callable[[Any], Any] | None:
    """Return an AST evaluator for closed IR, or ``None`` for the reference fallback.

    The inventory pass is iterative and counts occurrences rather than identities.  This
    bounds AST expansion for shared graphs and breaks cycles without executing any row data.
    """
    try:
        if not _is_bounded_closed_graph(root):
            return None
        builder = _ExpressionBuilder(selectors)
        expression = builder.build(root)
        tree = ast.Expression(ast.Lambda(_lambda_arguments(builder.row_name), expression))
        code = compile(ast.fix_missing_locations(tree), "<fpstreams-row-expr>", "eval")
        globals_: dict[str, Any] = {
            "__builtins__": {},
            builder.slots_name: tuple(builder.slots),
        }
        return cast(Callable[[Any], Any], eval(code, globals_))
    except Exception:
        # Malformed third-party IR and implementation limits must retain the iterative path.
        return None


def _lambda_arguments(row_name: str) -> ast.arguments:
    return ast.arguments(
        posonlyargs=[],
        args=[ast.arg(arg=row_name)],
        vararg=None,
        kwonlyargs=[],
        kw_defaults=[],
        defaults=[],
    )


def _is_bounded_closed_graph(root: object) -> bool:
    """Accept only supported nodes whose occurrence expansion remains bounded."""
    occurrences = 0
    stack: list[tuple[object, int]] = [(root, 1)]
    while stack:
        node, depth = stack.pop()
        occurrences += 1
        if depth > _MAX_DEPTH or occurrences > _MAX_OCCURRENCES:
            return False
        children = _closed_children(node)
        if children is None:
            return False
        stack.extend((child, depth + 1) for child in reversed(children))
    return True


def _closed_children(node: object) -> tuple[object, ...] | None:
    """Return children for the direct subset, rejecting opaque and unknown constructs."""
    if isinstance(node, (InputRow, Field, Path, Index, Literal)):
        return ()
    if isinstance(node, GetItem):
        return (node.value, node.key)
    if isinstance(node, Unary):
        return (node.operand,) if node.kind in {"not", "neg", "abs"} else None
    if isinstance(node, Binary):
        if node.kind in _ARITHMETIC_OPERATORS or node.kind in _COMPARISON_OPERATORS:
            return (node.left, node.right)
        if node.kind in {"and", "or"}:
            return (node.left, node.right)
        return None
    if isinstance(node, Cast):
        return (node.value,) if any(node.target is target for target in _EXACT_CASTS) else None
    if isinstance(node, IsNull):
        return (node.value,)
    if isinstance(node, Coalesce):
        return node.values
    if isinstance(node, Call):
        expected_arguments = {
            "lower": 1,
            "upper": 1,
            "strip": 1,
            "contains": 2,
            "isin": 2,
        }
        return node.arguments if expected_arguments.get(node.kind) == len(node.arguments) else None
    if isinstance(node, IfElse):
        return (node.condition, node.yes, node.no)
    if isinstance(node, PythonUDF):
        return None
    return None


class _ExpressionBuilder:
    """Translate only trusted IR kinds into AST while binding all external values as slots."""

    def __init__(self, selectors: Mapping[int, Callable[[Any], Any]]) -> None:
        self._selectors = selectors
        identity = f"{id(self):x}"
        self.row_name = f"_fpstreams_row_{identity}"
        self.slots_name = f"_fpstreams_slots_{identity}"
        self._temporary = 0
        self.slots: list[Any] = []

    def build(self, node: object) -> ast.expr:
        """Build one expression, preserving source-order child evaluation in every node."""
        if isinstance(node, InputRow):
            return ast.Name(id=self.row_name, ctx=ast.Load())
        if isinstance(node, Literal):
            return self._slot(node.value)
        if isinstance(node, (Field, Path, Index)):
            return ast.Call(self._slot(self._selectors[id(node)]), [self._row()], [])
        if isinstance(node, GetItem):
            return ast.Subscript(self.build(node.value), self.build(node.key), ast.Load())
        if isinstance(node, Unary):
            return self._unary(node)
        if isinstance(node, Binary):
            return self._binary(node)
        if isinstance(node, Cast):
            return ast.Call(self._slot(node.target), [self.build(node.value)], [])
        if isinstance(node, IsNull):
            return ast.Compare(
                self.build(node.value),
                [ast.IsNot() if node.negate else ast.Is()],
                [ast.Constant(None)],
            )
        if isinstance(node, Coalesce):
            return self._coalesce(node.values)
        if isinstance(node, Call):
            return self._call(node)
        if isinstance(node, IfElse):
            return ast.IfExp(
                ast.Call(self._slot(bool), [self.build(node.condition)], []),
                self.build(node.yes),
                self.build(node.no),
            )
        raise TypeError(f"unsupported direct row node: {type(node).__name__}")

    def _row(self) -> ast.Name:
        return ast.Name(id=self.row_name, ctx=ast.Load())

    def _slot(self, value: Any) -> ast.Subscript:
        index = len(self.slots)
        self.slots.append(value)
        return ast.Subscript(
            value=ast.Name(id=self.slots_name, ctx=ast.Load()),
            slice=ast.Constant(index),
            ctx=ast.Load(),
        )

    def _unary(self, node: Unary) -> ast.expr:
        operand = self.build(node.operand)
        if node.kind == "not":
            return ast.UnaryOp(ast.Not(), operand)
        if node.kind == "neg":
            return ast.UnaryOp(ast.USub(), operand)
        return ast.Call(self._slot(abs), [operand], [])

    def _binary(self, node: Binary) -> ast.expr:
        left = self.build(node.left)
        right = self.build(node.right)
        if node.kind in _ARITHMETIC_OPERATORS:
            return ast.BinOp(left, _ARITHMETIC_OPERATORS[node.kind], right)
        if node.kind in _COMPARISON_OPERATORS:
            return ast.Compare(left, [_COMPARISON_OPERATORS[node.kind]], [right])
        return ast.BoolOp(
            ast.And() if node.kind == "and" else ast.Or(),
            [
                ast.Call(self._slot(bool), [left], []),
                ast.Call(self._slot(bool), [right], []),
            ],
        )

    def _coalesce(self, values: tuple[Any, ...]) -> ast.expr:
        if len(values) == 1:
            return self.build(values[0])
        temporary = f"_fpstreams_value_{self._temporary}"
        self._temporary += 1
        first = ast.NamedExpr(ast.Name(id=temporary, ctx=ast.Store()), self.build(values[0]))
        saved = ast.Name(id=temporary, ctx=ast.Load())
        return ast.IfExp(
            ast.Compare(first, [ast.IsNot()], [ast.Constant(None)]),
            saved,
            self._coalesce(values[1:]),
        )

    def _call(self, node: Call) -> ast.expr:
        arguments = [self.build(argument) for argument in node.arguments]
        if node.kind in {"lower", "upper", "strip"}:
            return ast.Call(ast.Attribute(arguments[0], node.kind, ast.Load()), [], [])
        helper = _contains if node.kind == "contains" else _isin
        return ast.Call(self._slot(helper), arguments, [])
