"""Evaluate row-expression IR iteratively and cache its reusable program handle."""

from __future__ import annotations

import operator
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from threading import Lock
from types import CodeType, FunctionType
from typing import Any

from .._provenance import (
    row_expression_environment_is_current,
    row_expression_environment_snapshot,
)
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

_BUILTIN_ANY = any
_BUILTIN_ID = id
_BUILTIN_LEN = len
_BUILTIN_NEXT = next
_BUILTIN_REVERSED = reversed
_BUILTIN_TUPLE: type[tuple[Any, ...]] = tuple
_BUILTIN_TYPE = type
_BUILTIN_ZIP = zip
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
_ROW_PROGRAM_TRUST_TOKEN = object()


def _row_node_boundary(
    node_type: type[object],
    names: tuple[str, ...],
) -> tuple[type[object], object, object, tuple[tuple[str, object], ...]]:
    """Capture exact object-model and slot boundaries for one frozen IR class."""
    namespace = node_type.__dict__
    return (
        node_type,
        node_type.__getattribute__,
        node_type.__setattr__,
        _BUILTIN_TUPLE((name, namespace[name]) for name in names),
    )


_ROW_NODE_BOUNDARIES = _BUILTIN_TUPLE(
    _row_node_boundary(node_type, names)
    for node_type, names in (
        (InputRow, ()),
        (Field, ("name",)),
        (Path, ("parts", "selector")),
        (Index, ("index",)),
        (Literal, ("value",)),
        (GetItem, ("value", "key")),
        (Unary, ("kind", "operand")),
        (Binary, ("kind", "left", "right")),
        (Cast, ("value", "target")),
        (IsNull, ("value", "negate")),
        (Coalesce, ("values",)),
        (Call, ("kind", "arguments")),
        (IfElse, ("condition", "yes", "no")),
        (PythonUDF, ("function", "arguments")),
    )
)
_CANONICAL_ROW_NODE_BOUNDARIES = _ROW_NODE_BOUNDARIES


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
    trust_token: object | None = dataclass_field(default=None, repr=False, compare=False)
    graph_manifest: tuple[Any, ...] | None = dataclass_field(
        default=None,
        repr=False,
        compare=False,
    )

    def __call__(self, row: Any) -> Any:
        """Evaluate the stored root against one row."""
        return self.expression(row)


def _collect_row_nodes(node: object) -> tuple[object, ...]:
    """Collect each reachable row node identity once in parent-first order."""
    ordered: list[object] = []
    seen: set[int] = set()
    stack = [node]
    while stack:
        current = stack.pop()
        if _BUILTIN_ID(current) in seen:
            continue
        seen.add(_BUILTIN_ID(current))
        ordered.append(current)
        stack.extend(_BUILTIN_REVERSED(_children(current)))
    return _BUILTIN_TUPLE(ordered)


def _row_graph_snapshot(instructions: tuple[object, ...]) -> tuple[Any, ...] | None:
    """Capture every exact IR field by identity without traversing user values."""
    entries: list[tuple[object, type[object], tuple[tuple[str, object], ...]]] = []
    for node in instructions:
        node_type = _BUILTIN_TYPE(node)
        boundary = _BUILTIN_NEXT(
            (candidate for candidate in _ROW_NODE_BOUNDARIES if node_type is candidate[0]),
            None,
        )
        if boundary is None:
            return None
        _trusted_type, getattribute, setattr, fields = boundary
        if (
            node_type.__getattribute__ is not getattribute
            or node_type.__setattr__ is not setattr
            or _BUILTIN_ANY(
                node_type.__dict__.get(name) is not descriptor for name, descriptor in fields
            )
        ):
            return None
        attributes = _BUILTIN_TUPLE(
            (name, descriptor.__get__(node, node_type)) for name, descriptor in fields
        )
        for name, value in attributes:
            if node_type is Path and name == "parts":
                if _BUILTIN_TYPE(value) is not _BUILTIN_TUPLE:
                    return None
                for part in value:
                    if _BUILTIN_TYPE(part) is not str:
                        return None
            elif node_type in (Coalesce, Call, PythonUDF) and name in {
                "values",
                "arguments",
            }:
                if _BUILTIN_TYPE(value) is not _BUILTIN_TUPLE:
                    return None
        entries.append(
            (
                node,
                node_type,
                attributes,
            )
        )
    return instructions, _BUILTIN_TUPLE(entries)


def _row_program_graph_snapshot(program: RowProgram) -> tuple[Any, ...] | None:
    """Capture the exact graph inventory retained by one compiled program."""
    return _row_graph_snapshot(program.instructions)


def _row_graph_snapshots_agree(left: object, right: object) -> bool:
    """Compare two trusted snapshots only by node, type, and field identities."""
    if (
        _BUILTIN_TYPE(left) is not _BUILTIN_TUPLE
        or _BUILTIN_LEN(left) != 2
        or _BUILTIN_TYPE(right) is not _BUILTIN_TUPLE
        or _BUILTIN_LEN(right) != 2
    ):
        return False
    left_instructions, left_entries = left
    right_instructions, right_entries = right
    if (
        _BUILTIN_TYPE(left_instructions) is not _BUILTIN_TUPLE
        or _BUILTIN_TYPE(right_instructions) is not _BUILTIN_TUPLE
        or _BUILTIN_TYPE(left_entries) is not _BUILTIN_TUPLE
        or _BUILTIN_TYPE(right_entries) is not _BUILTIN_TUPLE
        or _BUILTIN_LEN(left_instructions) != _BUILTIN_LEN(right_instructions)
        or _BUILTIN_LEN(left_entries) != _BUILTIN_LEN(right_entries)
    ):
        return False
    for left_node, right_node in _BUILTIN_ZIP(left_instructions, right_instructions, strict=True):
        if left_node is not right_node:
            return False
    for left_entry, right_entry in _BUILTIN_ZIP(left_entries, right_entries, strict=True):
        if (
            _BUILTIN_TYPE(left_entry) is not _BUILTIN_TUPLE
            or _BUILTIN_LEN(left_entry) != 3
            or _BUILTIN_TYPE(right_entry) is not _BUILTIN_TUPLE
            or _BUILTIN_LEN(right_entry) != 3
        ):
            return False
        left_node, left_type, left_attributes = left_entry
        right_node, right_type, right_attributes = right_entry
        if (
            left_node is not right_node
            or left_type is not right_type
            or _BUILTIN_TYPE(left_attributes) is not _BUILTIN_TUPLE
            or _BUILTIN_TYPE(right_attributes) is not _BUILTIN_TUPLE
            or _BUILTIN_LEN(left_attributes) != _BUILTIN_LEN(right_attributes)
        ):
            return False
        for left_attribute, right_attribute in _BUILTIN_ZIP(
            left_attributes, right_attributes, strict=True
        ):
            if (
                _BUILTIN_TYPE(left_attribute) is not _BUILTIN_TUPLE
                or _BUILTIN_LEN(left_attribute) != 2
                or _BUILTIN_TYPE(right_attribute) is not _BUILTIN_TUPLE
                or _BUILTIN_LEN(right_attribute) != 2
                or left_attribute[0] is not right_attribute[0]
                or left_attribute[1] is not right_attribute[1]
            ):
                return False
    return True


def _row_program_graph_is_current(program: RowProgram, snapshot: object) -> bool:
    """Reject replaced nodes, child links, or scalar fields in one cached graph."""
    if _BUILTIN_TYPE(snapshot) is not _BUILTIN_TUPLE or _BUILTIN_LEN(snapshot) != 2:
        return False
    trusted_instructions, entries = snapshot
    instructions = program.instructions
    if (
        _BUILTIN_TYPE(instructions) is not _BUILTIN_TUPLE
        or instructions is not trusted_instructions
        or _BUILTIN_TYPE(entries) is not _BUILTIN_TUPLE
        or _BUILTIN_LEN(entries) != _BUILTIN_LEN(instructions)
    ):
        return False
    for node, entry in _BUILTIN_ZIP(instructions, entries, strict=True):
        if _BUILTIN_TYPE(entry) is not _BUILTIN_TUPLE or _BUILTIN_LEN(entry) != 3:
            return False
        trusted_node, trusted_type, attributes = entry
        node_type = _BUILTIN_TYPE(node)
        if (
            node is not trusted_node
            or node_type is not trusted_type
            or _BUILTIN_TYPE(attributes) is not _BUILTIN_TUPLE
        ):
            return False
        boundary = _BUILTIN_NEXT(
            (candidate for candidate in _ROW_NODE_BOUNDARIES if node_type is candidate[0]),
            None,
        )
        if boundary is None:
            return False
        _trusted_type, getattribute, setattr, fields = boundary
        if (
            node_type.__getattribute__ is not getattribute
            or node_type.__setattr__ is not setattr
            or _BUILTIN_LEN(attributes) != _BUILTIN_LEN(fields)
        ):
            return False
        namespace = node_type.__dict__
        for (expected_name, descriptor), attribute in _BUILTIN_ZIP(fields, attributes, strict=True):
            if (
                _BUILTIN_TYPE(attribute) is not _BUILTIN_TUPLE
                or _BUILTIN_LEN(attribute) != 2
                or namespace.get(expected_name) is not descriptor
            ):
                return False
            name, trusted_value = attribute
            if (
                _BUILTIN_TYPE(name) is not str
                or name != expected_name
                or descriptor.__get__(node, node_type) is not trusted_value
            ):
                return False
    return True


def compile_row_node(node: Any) -> RowProgram:
    """Collect each node identity once with an iterative depth-first traversal.

    The resulting RowProgram stores nodes in parent-before-children order together with
    the original root.
    """
    ordered = _collect_row_nodes(node)
    from .program import compile_expression
    from .typed_ir import Effect, ExpressionSource, TypedExpr, ValueType

    expression = TypedExpr(
        node,
        ValueType.UNKNOWN,
        Effect.MAY_RAISE,
        frozenset({"python"}),
        ExpressionSource.ROW,
    )
    return RowProgram(
        ordered,
        node,
        compile_expression(expression).evaluator(),
        _ROW_PROGRAM_TRUST_TOKEN,
    )


class LazyRowEvaluator:
    """Compile one row graph on first use and reuse its RowProgram across calls."""

    def __init__(self, node: Any) -> None:
        """Store the root and initialize an empty program cache guarded by a lock."""
        self.node = node
        self._program: RowProgram | None = None
        self._program_provenance: (
            tuple[
                RowProgram,
                Any,
                Callable[[Any], Any],
                CodeType | None,
                tuple[Any, ...] | None,
                tuple[Any, ...] | None,
            ]
            | None
        ) = None
        self._lock = Lock()

    def __call__(self, row: Any) -> Any:
        """Compile once under a double-checked lock, then evaluate the cached program."""
        program = self._program
        if program is None:
            with self._lock:
                if self._program is None:
                    graph_before = _row_graph_snapshot(_collect_row_nodes(self.node))
                    compiled = compile_row_node(self.node)
                    graph_after = _row_program_graph_snapshot(compiled)
                    graph = (
                        graph_after
                        if _row_graph_snapshots_agree(graph_before, graph_after)
                        else None
                    )
                    object.__setattr__(compiled, "graph_manifest", graph)
                    self._program = compiled
                    self._program_provenance = (
                        compiled,
                        compiled.root,
                        compiled.expression,
                        (
                            compiled.expression.__code__
                            if _BUILTIN_TYPE(compiled.expression) is FunctionType
                            else None
                        ),
                        row_expression_environment_snapshot(compiled.expression)
                        if _BUILTIN_TYPE(compiled.expression) is FunctionType
                        else None,
                        graph,
                    )
                program = self._program
        return program(row)


def cached_row_program(evaluator: object) -> RowProgram | None:
    """Return a canonical cached program only while code, environment, and IR agree."""
    if _BUILTIN_TYPE(evaluator) is not LazyRowEvaluator:
        return None
    program = evaluator._program
    provenance = evaluator._program_provenance
    if (
        _BUILTIN_TYPE(program) is not RowProgram
        or _BUILTIN_TYPE(provenance) is not _BUILTIN_TUPLE
        or _BUILTIN_LEN(provenance) != 6
    ):
        return None
    (
        trusted_program,
        trusted_root,
        trusted_expression,
        trusted_code,
        environment,
        graph,
    ) = provenance
    expression = program.expression
    if not (
        program is trusted_program
        and program.trust_token is _ROW_PROGRAM_TRUST_TOKEN
        and program.graph_manifest is graph
        and program.root is trusted_root
        and trusted_root is evaluator.node
        and expression is trusted_expression
        and _BUILTIN_TYPE(expression) is FunctionType
        and expression.__code__ is trusted_code
        and _BUILTIN_TYPE(environment) is _BUILTIN_TUPLE
        and _BUILTIN_TYPE(graph) is _BUILTIN_TUPLE
        and _ROW_NODE_BOUNDARIES is _CANONICAL_ROW_NODE_BOUNDARIES
    ):
        return None
    if not row_expression_environment_is_current(environment):
        return None
    return program if _row_program_graph_is_current(program, graph) else None
