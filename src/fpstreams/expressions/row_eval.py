"""Evaluate row-expression IR iteratively and cache its reusable program handle."""

from __future__ import annotations

import operator
from collections.abc import Callable
from dataclasses import dataclass
from threading import Lock
from typing import Any

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

_BINARY = {
    "+": operator.add,
    "-": operator.sub,
    "*": operator.mul,
    "/": operator.truediv,
    "//": operator.floordiv,
    "%": operator.mod,
    "**": operator.pow,
    "==": operator.eq,
    "!=": operator.ne,
    "<": operator.lt,
    "<=": operator.le,
    ">": operator.gt,
    ">=": operator.ge,
}
_UNARY = {"not": operator.not_, "neg": operator.neg, "abs": abs}


def _children(node: Any) -> tuple[Any, ...]:
    """Return a row IR node's direct operands in source order.

    Unsupported node types raise TypeError before evaluation can continue.
    """
    if isinstance(node, (InputRow, Field, Path, Index, Literal)):
        return ()
    if isinstance(node, GetItem):
        return (node.value, node.key)
    if isinstance(node, Unary):
        return (node.operand,)
    if isinstance(node, Binary):
        return (node.left, node.right)
    if isinstance(node, Cast):
        return (node.value,)
    if isinstance(node, IsNull):
        return (node.value,)
    if isinstance(node, Coalesce):
        return node.values
    if isinstance(node, Call):
        return node.arguments
    if isinstance(node, IfElse):
        return (node.condition, node.yes, node.no)
    if isinstance(node, PythonUDF):
        return node.arguments
    raise TypeError(f"unsupported row node: {type(node).__name__}")


@dataclass(frozen=True, slots=True)
class RowProgram:
    """Hold a deduplicated node inventory and the root used for repeated row evaluation.

    instructions records the depth-first graph collected at compile time; calls evaluate
    root with the iterative reference evaluator.
    """

    instructions: tuple[Any, ...]
    root: Any
    expression: Callable[[Any], Any]

    def __call__(self, row: Any) -> Any:
        """Evaluate the stored root against one row."""
        return self.expression(row)


def compile_row_node(node: Any) -> RowProgram:
    """Collect each node identity once with an iterative depth-first traversal.

    The resulting RowProgram stores nodes in parent-before-children order together with
    the original root.
    """
    ordered: list[Any] = []
    seen: set[int] = set()
    stack = [node]
    while stack:
        current = stack.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        ordered.append(current)
        stack.extend(reversed(_children(current)))
    from .program import compile_expression
    from .typed_ir import Effect, ExpressionSource, TypedExpr, ValueType

    expression = TypedExpr(
        node,
        ValueType.UNKNOWN,
        Effect.MAY_RAISE,
        frozenset({"python"}),
        ExpressionSource.ROW,
    )
    return RowProgram(tuple(ordered), node, compile_expression(expression).evaluator())


class LazyRowEvaluator:
    """Compile one row graph on first use and reuse its RowProgram across calls."""

    def __init__(self, node: Any) -> None:
        """Store the root and initialize an empty program cache guarded by a lock."""
        self.node = node
        self._program: RowProgram | None = None
        self._lock = Lock()

    def __call__(self, row: Any) -> Any:
        """Compile once under a double-checked lock, then evaluate the cached program."""
        program = self._program
        if program is None:
            with self._lock:
                if self._program is None:
                    self._program = compile_row_node(self.node)
                program = self._program
        return program(row)
