"""Evaluate row-expression IR iteratively and cache its reusable program handle."""

from __future__ import annotations

import operator
from dataclasses import dataclass
from threading import Lock
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
from .selectors import compile_selector

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


def _load(node: Any, row: Any) -> Any:
    """Resolve an input, literal, field, path, or index leaf for the current row.

    Selector-backed leaves delegate to compile_selector, so failed lookups surface as
    SelectionError. Passing any non-leaf node raises TypeError.
    """
    if isinstance(node, InputRow):
        return row
    if isinstance(node, Literal):
        return node.value
    if isinstance(node, Field):
        return compile_selector(node.name)(row)
    if isinstance(node, Path):
        return compile_selector(node.selector or ".".join(node.parts))(row)
    if isinstance(node, Index):
        return compile_selector(node.index)(row)
    raise TypeError


def evaluate_row_node(root: Any, row: Any) -> Any:
    """Evaluate a row-expression graph with an explicit stack of node phases.

    Phase zero loads leaves or schedules work. Phase two decides whether Boolean and/or
    needs its right operand, phase five stores that operand's truth value, phases three
    and six choose and collect one conditional branch, and phases 1000 and above advance
    through coalesce candidates. Consequently, unused Boolean operands, conditional
    branches, and later coalesce values are never evaluated.

    Supported primitive operations propagate their normal Python exceptions. Unknown
    Call kinds raise ValueError, and unsupported node types raise TypeError.
    """
    values: dict[int, Any] = {}
    stack: list[tuple[Any, int]] = [(root, 0)]
    while stack:
        node, phase = stack.pop()
        key = id(node)
        if phase == 0:
            if isinstance(node, (InputRow, Literal, Field, Path, Index)):
                values[key] = _load(node, row)
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
                if not node.values:
                    values[key] = None
                    continue
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
            stack.append((node, 6))
            stack.append((branch, 0))
            continue
        if phase >= 1000 and isinstance(node, Coalesce):
            index = phase - 1000
            result = values[id(node.values[index])]
            if result is not None or index == len(node.values) - 1:
                values[key] = result
            else:
                stack.append((node, 1000 + index + 1))
                stack.append((node.values[index + 1], 0))
            continue
        if phase == 5:
            values[key] = bool(values[id(node.right)])
            continue
        if phase == 6:
            values[key] = values[id(node.yes if bool(values[id(node.condition)]) else node.no)]
            continue
        if phase == 0:
            children = _children(node)
            stack.append((node, 1))
            stack.extend((child, 0) for child in reversed(children))
            continue
        args = [values[id(child)] for child in _children(node)]
        if isinstance(node, GetItem):
            result = args[0][args[1]]
        elif isinstance(node, Unary):
            result = cast(Any, _UNARY[node.kind])(args[0])
        elif isinstance(node, Binary):
            result = _BINARY[node.kind](args[0], args[1])
        elif isinstance(node, Cast):
            result = node.target(args[0])
        elif isinstance(node, IsNull):
            result = args[0] is None if not node.negate else args[0] is not None
        elif isinstance(node, Call):
            if node.kind == "lower":
                result = args[0].lower()
            elif node.kind == "upper":
                result = args[0].upper()
            elif node.kind == "strip":
                result = args[0].strip()
            elif node.kind == "contains":
                result = args[1] in args[0]
            elif node.kind == "isin":
                result = args[0] in args[1]
            else:
                raise ValueError(f"unknown row call {node.kind!r}")
        elif isinstance(node, PythonUDF):
            result = node.function(*args)
        else:
            raise TypeError(f"unsupported row node: {type(node).__name__}")
        values[key] = result
    return values[id(root)]


@dataclass(frozen=True, slots=True)
class RowProgram:
    """Hold a deduplicated node inventory and the root used for repeated row evaluation.

    instructions records the depth-first graph collected at compile time; calls evaluate
    root with the iterative reference evaluator.
    """

    instructions: tuple[Any, ...]
    root: Any

    def __call__(self, row: Any) -> Any:
        """Evaluate the stored root against one row."""
        return evaluate_row_node(self.root, row)


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
    return RowProgram(tuple(ordered), node)


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
