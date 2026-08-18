"""Define immutable row-expression IR nodes and conservative structural analysis."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class AnalysisTruth(StrEnum):
    """Represent yes, no, or unknown results for determinism and purity analysis."""

    YES = "yes"
    NO = "no"
    UNKNOWN = "unknown"


class NullBehavior(StrEnum):
    """Classify the root node's relationship to Python-style null handling."""

    PYTHON = "python"
    TESTS_NULL = "tests_null"
    COALESCES = "coalesces"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class InputRow:
    """Leaf whose evaluated value is the entire current input row."""

    pass


@dataclass(frozen=True, slots=True)
class Field:
    """Leaf selecting one top-level mapping key or object attribute by name."""

    name: str

    def __post_init__(self) -> None:
        """Reject an empty top-level field name."""
        if not self.name:
            raise ValueError("field name cannot be empty")


@dataclass(frozen=True, slots=True)
class Path:
    """Leaf selecting a dotted sequence of mapping keys and object attributes."""

    parts: tuple[str, ...]
    selector: str | None = None

    def __post_init__(self) -> None:
        """Require at least one path component and reject empty components."""
        if not self.parts or any(not part for part in self.parts):
            raise ValueError("path parts cannot be empty")


@dataclass(frozen=True, slots=True)
class Index:
    """Leaf selecting row[index], including integer sequence or mapping access."""

    index: int


@dataclass(frozen=True, slots=True)
class Literal:
    """Leaf returning its stored object unchanged for every input row."""

    value: Any


@dataclass(frozen=True, slots=True)
class GetItem:
    """Node evaluating a container and key before applying container[key]."""

    value: Any
    key: Any


@dataclass(frozen=True, slots=True)
class Unary:
    """Node dispatching logical not, arithmetic negation, or absolute value by kind."""

    kind: str
    operand: Any


@dataclass(frozen=True, slots=True)
class Binary:
    """Node for an arithmetic, comparison, or Boolean binary operation.

    The and and or kinds are evaluated as short-circuiting Boolean operations; all
    other kinds dispatch through the row evaluator's binary operator table.
    """

    kind: str
    left: Any
    right: Any


@dataclass(frozen=True, slots=True)
class Cast:
    """Node calling target with the value produced by its child node."""

    value: Any
    target: Callable[[Any], Any]


@dataclass(frozen=True, slots=True)
class IsNull:
    """Node testing whether a child is None, or is not None when negate is true."""

    value: Any
    negate: bool = False


@dataclass(frozen=True, slots=True)
class Coalesce:
    """Node evaluating children left to right until one returns a non-None value."""

    values: tuple[Any, ...]

    def __post_init__(self) -> None:
        """Reject a coalesce node with no candidate values."""
        if not self.values:
            raise ValueError("coalesce requires at least one value")


@dataclass(frozen=True, slots=True)
class Call:
    """Node dispatching a supported lower, upper, strip, contains, or isin operation."""

    kind: str
    arguments: tuple[Any, ...]


@dataclass(frozen=True, slots=True)
class IfElse:
    """Conditional node that truth-tests condition and evaluates only the selected branch."""

    condition: Any
    yes: Any
    no: Any


@dataclass(frozen=True, slots=True)
class PythonUDF:
    """Opaque node calling a Python function with its evaluated argument values."""

    function: Callable[..., Any]
    arguments: tuple[Any, ...]

    def __post_init__(self) -> None:
        """Reject a non-callable PythonUDF function."""
        if not callable(self.function):
            raise TypeError("PythonUDF function must be callable")


@dataclass(frozen=True, slots=True)
class RowExprAnalysis:
    """Store conservative metadata inferred from a row-expression graph.

    fields is None when dependencies cannot be known; otherwise it contains top-level
    field names. deterministic and pure use three-valued results. backends lists the
    reported execution targets, and opaque marks graphs containing a PythonUDF.
    """

    fields: frozenset[str] | None
    deterministic: AnalysisTruth
    pure: AnalysisTruth
    null_behavior: NullBehavior
    backends: frozenset[str]
    opaque: bool

    def to_dict(self) -> dict[str, Any]:
        """Convert analysis metadata to plain values with fields and backends sorted."""
        return {
            "fields": None if self.fields is None else sorted(self.fields),
            "deterministic": self.deterministic.value,
            "pure": self.pure.value,
            "null_behavior": self.null_behavior.value,
            "backends": sorted(self.backends),
            "opaque": self.opaque,
        }


def analyze_row_node(root: Any) -> RowExprAnalysis:
    """Derive structural metadata without evaluating selectors or user functions.

    The iterative traversal unions top-level Field and Path dependencies. A PythonUDF
    makes fields unknown and marks the graph opaque. Only a Literal root is positively
    identified as deterministic and pure; non-literal roots remain unknown unless a
    child is explicitly negative. Every result currently reports only the Python
    backend, and only a Coalesce root reports coalescing null behavior.
    """
    results: dict[int, RowExprAnalysis] = {}
    stack: list[tuple[Any, bool]] = [(root, False)]
    while stack:
        node, visited = stack.pop()
        key = id(node)
        if visited:
            children = _children(node)
            analyses = [results[id(child)] for child in children]
            fields: frozenset[str] | None = frozenset()
            for child in analyses:
                fields = None if fields is None or child.fields is None else fields | child.fields
            opaque = any(child.opaque for child in analyses)
            if isinstance(node, Field):
                fields = frozenset({node.name})
            elif isinstance(node, Path):
                fields = frozenset({node.parts[0]})
            elif isinstance(node, (InputRow, PythonUDF)):
                fields = None if isinstance(node, PythonUDF) else frozenset()
            if isinstance(node, PythonUDF):
                fields, opaque = None, True
            deterministic = (
                AnalysisTruth.YES if isinstance(node, Literal) else AnalysisTruth.UNKNOWN
            )
            pure = AnalysisTruth.YES if isinstance(node, Literal) else AnalysisTruth.UNKNOWN
            if any(child.deterministic is AnalysisTruth.NO for child in analyses):
                deterministic = AnalysisTruth.NO
            if any(child.pure is AnalysisTruth.NO for child in analyses):
                pure = AnalysisTruth.NO
            null_behavior = (
                NullBehavior.COALESCES if isinstance(node, Coalesce) else NullBehavior.PYTHON
            )
            results[key] = RowExprAnalysis(
                fields, deterministic, pure, null_behavior, frozenset({"python"}), opaque
            )
        elif key not in results:
            stack.append((node, True))
            stack.extend((child, False) for child in reversed(_children(node)))
    return results[id(root)]


def _children(node: Any) -> tuple[Any, ...]:
    """Return an IR node's operands in evaluation order.

    Leaves return an empty tuple; an unsupported object raises TypeError naming its
    runtime type.
    """
    if isinstance(node, (InputRow, Field, Path, Index, Literal)):
        return ()
    if isinstance(node, PythonUDF):
        return node.arguments
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
    raise TypeError(f"unsupported row node: {type(node).__name__}")
