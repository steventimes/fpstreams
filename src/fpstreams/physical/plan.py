"""Immutable physical-plan values shared by the synchronous compiler."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from ..expressions.program import ExprProgram
from ..expressions.typed_ir import TypedExpr
from ..planning.arrow import ArrowPrefixPlan
from ..planning.logical import TerminalSpec
from ..planning.native import EngineDecision
from ..planning.numpy import NumpyPrefixPlan
from ..planning.source import Source
from ..planning.sync import Engine, Operation, ParallelSettings, SortOp


@dataclass(frozen=True, slots=True)
class PhysicalNode:
    """A physical representation of one or more logical nodes."""

    logical_ids: tuple[int, ...]
    engine: str


@dataclass(frozen=True, slots=True)
class RowPhysicalNode(PhysicalNode):
    """One canonical Python row operation stage."""

    operation: Operation


@dataclass(frozen=True, slots=True)
class CompiledExpressionPhysicalNode(PhysicalNode):
    """A map/filter whose analyzable expression was compiled once per physical plan."""

    operation: Operation
    expression: TypedExpr
    program: ExprProgram


class SortStrategy(StrEnum):
    """The source-safe execution strategy chosen for one sort node."""

    IN_MEMORY = "in_memory"
    ARROW_STABLE = "arrow_stable"
    CACHED_EXTERNAL_MERGE = "cached_external_merge"
    OPAQUE_CALLBACK = "opaque_callback"


@dataclass(frozen=True, slots=True)
class SortPhysicalNode(PhysicalNode):
    """An explicit sort operation retaining the original public semantics."""

    operation: SortOp
    strategy: SortStrategy


@dataclass(frozen=True, slots=True)
class PlanDecision:
    """A stable presentation of the already selected backend decision."""

    selected_engine: str
    reason: str
    estimated_rows: int | None = None
    estimated_bytes: int | None = None
    guards: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class BackendPayload:
    """Backend results selected once by compilation and consumed by execution."""

    native_decision: EngineDecision | None = None
    arrow_prefix: ArrowPrefixPlan | None = None
    numpy_prefix: NumpyPrefixPlan | None = None


@dataclass(frozen=True, slots=True)
class PhysicalPlan:
    """Compiled query with one decision and selected backend payload."""

    source: Source[Any]
    nodes: tuple[PhysicalNode, ...]
    terminal: TerminalSpec
    decision: PlanDecision
    engine: Engine = "auto"
    parallel: ParallelSettings | None = None
    backend_payload: BackendPayload | None = None
    root: Any | None = None
    cache_hit: bool = False
    cacheable: bool = False
    cache_reason: str = "not evaluated"
