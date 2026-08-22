"""Reusable programs for lowered expressions with direct row fast paths."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, cast

from ._row_codegen import compile_row_evaluator
from .row_eval import _BINARY, _UNARY, _children
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
from .scalar import Expr, FExpr
from .selectors import compile_selector
from .typed_ir import Effect, ExpressionSource, TypedExpr


@dataclass(frozen=True, slots=True)
class ExprProgram:
    """A compiled row root with precompiled leaf selectors."""

    root: object
    selectors: dict[int, Callable[[Any], Any]]
    effect: Effect
    scalar_evaluator: Callable[[Any], Any] | None = None
    row_evaluator: Callable[[Any], Any] | None = None

    def evaluator(self) -> Callable[[Any], Any]:
        """Create an evaluator that can be reused across arbitrarily many rows."""
        if self.scalar_evaluator is not None:
            return self.scalar_evaluator
        return self.row_evaluator if self.row_evaluator is not None else ExprEvaluator(self)


class ExprEvaluator:
    """Evaluate one compiled program with the iterative semantic fallback."""

    def __init__(self, program: ExprProgram) -> None:
        self._program = program

    def __call__(self, row: Any) -> Any:  # noqa: C901
        """Evaluate all phases inline so rows do not pay one Python call per IR node."""
        values: dict[int, Any] = {}
        root = self._program.root
        stack: list[tuple[Any, int]] = [(root, 0)]
        while stack:
            node, phase = stack.pop()
            key = id(node)
            if phase == 0:
                if isinstance(node, InputRow):
                    values[key] = row
                    continue
                if isinstance(node, Literal):
                    values[key] = node.value
                    continue
                if isinstance(node, (Field, Path, Index)):
                    values[key] = self._program.selectors[key](row)
                    continue
                if isinstance(node, Binary) and node.kind in {"and", "or"}:
                    stack.append((node, 2))
                    stack.append((node.left, 0))
                    continue
                if isinstance(node, IfElse):
                    stack.append((node, 3))
                    stack.append((node.condition, 0))
                    continue
                if isinstance(node, Coalesce):
                    stack.append((node, 1000))
                    stack.append((node.values[0], 0))
                    continue
            if phase == 2:
                left = values[id(node.left)]
                if (node.kind == "and" and not bool(left)) or (node.kind == "or" and bool(left)):
                    values[key] = node.kind == "or"
                else:
                    stack.append((node, 5))
                    stack.append((node.right, 0))
                continue
            if phase == 3:
                branch = node.yes if bool(values[id(node.condition)]) else node.no
                stack.append((node, 6 if branch is node.yes else 7))
                stack.append((branch, 0))
                continue
            if phase >= 1000 and isinstance(node, Coalesce):
                index = phase - 1000
                result = values[id(node.values[index])]
                if result is not None or index == len(node.values) - 1:
                    values[key] = result
                else:
                    stack.append((node, phase + 1))
                    stack.append((node.values[index + 1], 0))
                continue
            if phase == 5:
                values[key] = bool(values[id(node.right)])
                continue
            if phase == 6:
                values[key] = values[id(node.yes)]
                continue
            if phase == 7:
                values[key] = values[id(node.no)]
                continue
            if phase == 0:
                children = _children(node)
                stack.append((node, 1))
                stack.extend((child, 0) for child in reversed(children))
                continue
            arguments = [values[id(child)] for child in _children(node)]
            if isinstance(node, GetItem):
                result = arguments[0][arguments[1]]
            elif isinstance(node, Unary):
                result = cast(Callable[[Any], Any], _UNARY[node.kind])(arguments[0])
            elif isinstance(node, Binary):
                result = _BINARY[node.kind](arguments[0], arguments[1])
            elif isinstance(node, Cast):
                result = node.target(arguments[0])
            elif isinstance(node, IsNull):
                result = arguments[0] is not None if node.negate else arguments[0] is None
            elif isinstance(node, Call):
                result = _call(node.kind, arguments)
            elif isinstance(node, PythonUDF):
                result = node.function(*arguments)
            else:
                raise TypeError(f"unsupported row node: {type(node).__name__}")
            values[key] = result
        return values[id(root)]


def _call(kind: str, arguments: list[Any]) -> Any:
    """Dispatch the small callback-free row-call vocabulary by its lowered name."""
    if kind == "lower":
        return arguments[0].lower()
    if kind == "upper":
        return arguments[0].upper()
    if kind == "strip":
        return arguments[0].strip()
    if kind == "contains":
        return arguments[1] in arguments[0]
    if kind == "isin":
        return arguments[0] in arguments[1]
    raise ValueError(f"unknown row call {kind!r}")


def compile_expression(expression: TypedExpr) -> ExprProgram:
    """Compile a lowered row expression without evaluating a row or user callback."""
    if expression.source in {ExpressionSource.INTEGER, ExpressionSource.FLOAT}:
        scalar = cast(Expr | FExpr, expression.root)
        return ExprProgram(
            expression.root,
            {},
            expression.effect,
            cast(Callable[[Any], Any], scalar._python_evaluator()),
        )
    if expression.source is not ExpressionSource.ROW:
        raise TypeError(f"expression source is not a compilable program: {expression.source}")
    selectors: dict[int, Callable[[Any], Any]] = {}
    seen: set[int] = set()
    stack = [expression.root]
    while stack:
        node = stack.pop()
        if id(node) in seen:
            continue
        seen.add(id(node))
        if isinstance(node, Field):
            selectors[id(node)] = compile_selector(node.name)
        elif isinstance(node, Path):
            selectors[id(node)] = compile_selector(node.selector or ".".join(node.parts))
        elif isinstance(node, Index):
            selectors[id(node)] = compile_selector(node.index)
        try:
            children = _children(node)
        except TypeError:
            # Keep unknown third-party IR on the established row-time error path.
            continue
        stack.extend(reversed(children))
    return ExprProgram(
        expression.root,
        selectors,
        expression.effect,
        row_evaluator=compile_row_evaluator(expression.root, selectors),
    )
