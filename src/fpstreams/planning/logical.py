"""Immutable logical nodes for synchronous streams."""

from __future__ import annotations

import os
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, TypeAlias

from .source import Source
from .sync import Engine, Operation, ParallelSettings

if TYPE_CHECKING:
    from ..collecting.aggregation import AggregationItems
    from ..tabular.spill_limits import SpillLimits

JoinSelector: TypeAlias = Any
JoinValidation: TypeAlias = str


def merge_engine_requests(left: Engine, right: Engine, *, operation: str) -> Engine:
    """Combine two query-wide engine requests without discarding either contract."""
    if left == right:
        return left
    if left == "auto":
        return right
    if right == "auto":
        return left
    raise ValueError(
        f"conflicting {operation} engine requests: left={left!r}, right={right!r}; "
        "set both inputs to the same engine or leave one as 'auto'"
    )


@dataclass(frozen=True, slots=True)
class LogicalNode:
    """Base class for immutable logical nodes."""


@dataclass(frozen=True, slots=True)
class SourceNode(LogicalNode):
    """A leaf that owns the unopened source of a logical pipeline."""

    source: Source[Any]


@dataclass(frozen=True, slots=True)
class UnaryNode(LogicalNode):
    """An operation applied to one logical input."""

    input: LogicalNode
    operation: Operation


@dataclass(frozen=True, slots=True)
class JoinSpec:
    """Validated, source-independent configuration for a binary record join."""

    left_on: JoinSelector
    right_on: JoinSelector
    how: str
    suffix: str
    validate: JoinValidation
    partitions: int | None
    tempdir: str | os.PathLike[str] | None
    limits: SpillLimits | None


@dataclass(frozen=True, slots=True)
class JoinNode(LogicalNode):
    """A binary record join whose two inputs remain unopened."""

    left: LogicalNode
    right: LogicalNode
    spec: JoinSpec


@dataclass(frozen=True, slots=True)
class GroupAggregateSpec:
    """Validated grouping keys and aggregations for a grouped result."""

    keys: tuple[tuple[str, JoinSelector], ...]
    aggregations: AggregationItems
    partitions: int | None
    tempdir: str | os.PathLike[str] | None
    limits: SpillLimits | None


@dataclass(frozen=True, slots=True)
class GroupAggregateNode(LogicalNode):
    """A grouped aggregate that consumes its input only at execution time."""

    input: LogicalNode
    spec: GroupAggregateSpec


@dataclass(frozen=True, slots=True)
class GlobalAggregateNode(LogicalNode):
    """A one-row aggregate over an unopened input."""

    input: LogicalNode
    aggregations: AggregationItems


@dataclass(frozen=True, slots=True)
class LogicalPlan:
    """An immutable logical pipeline and its execution preferences."""

    root: LogicalNode
    engine: Engine = "auto"
    parallel: ParallelSettings | None = None

    def append(self, operation: Operation) -> LogicalPlan:
        """Return a plan with ``operation`` added after the current root."""
        return replace(self, root=UnaryNode(self.root, operation))

    def with_root(self, root: LogicalNode) -> LogicalPlan:
        """Return a plan with its logical root replaced."""
        return replace(self, root=root)

    def with_engine(self, engine: Engine) -> LogicalPlan:
        """Return a plan requesting the selected execution engine."""
        if engine not in {"auto", "python", "native"}:
            raise ValueError("engine must be 'auto', 'python', or 'native'")
        return replace(self, engine=engine)

    def with_parallel(self, settings: ParallelSettings | None) -> LogicalPlan:
        """Return a plan with plan-level parallel settings replaced or cleared."""
        return replace(self, parallel=settings)


@dataclass(frozen=True, slots=True)
class Pipeline:
    """Canonical linear view consumed by sync backend selection and execution.

    It is derived from a ``LogicalPlan`` and therefore never owns a second
    planning history or reopens a source while being constructed.
    """

    source: Source[Any]
    operations: tuple[Operation, ...]
    engine: Engine = "auto"
    parallel: ParallelSettings | None = None

    def append(self, operation: Operation) -> Pipeline:
        """Return an equivalent view with one appended operation."""
        return replace(self, operations=(*self.operations, operation))


@dataclass(frozen=True, slots=True)
class TerminalSpec:
    """The terminal operation requested for a logical pipeline."""

    name: str
    arguments: tuple[Any, ...] = ()
    options: tuple[tuple[str, Any], ...] = ()


@dataclass(frozen=True, slots=True)
class Query:
    """Pair a logical pipeline with immutable terminal metadata."""

    logical: LogicalPlan
    terminal: TerminalSpec


def unary_chain(root: LogicalNode) -> tuple[SourceNode, tuple[UnaryNode, ...]]:
    """Return the source and execution-order unary nodes of a linear tree."""
    reversed_nodes: list[UnaryNode] = []
    current = root
    while isinstance(current, UnaryNode):
        reversed_nodes.append(current)
        current = current.input
    if not isinstance(current, SourceNode):
        raise TypeError(f"unsupported logical node: {type(current).__name__}")
    reversed_nodes.reverse()
    return current, tuple(reversed_nodes)


def linear_pipeline(logical: LogicalPlan) -> Pipeline:
    """Flatten a unary logical tree into the canonical execution view."""
    source, nodes = unary_chain(logical.root)
    return Pipeline(
        source.source,
        tuple(node.operation for node in nodes),
        logical.engine,
        logical.parallel,
    )


def walk_logical(root: LogicalNode) -> tuple[LogicalNode, ...]:
    """Return logical nodes in deterministic parent-first, left-first order."""
    result: list[LogicalNode] = []
    pending = [root]
    while pending:
        node = pending.pop()
        result.append(node)
        if isinstance(node, (UnaryNode, GroupAggregateNode, GlobalAggregateNode)):
            pending.append(node.input)
        elif isinstance(node, JoinNode):
            pending.append(node.right)
            pending.append(node.left)
    return tuple(result)
