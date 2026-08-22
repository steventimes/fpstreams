"""Serialize planner semantics, execution stages, boundaries, and engine cost decisions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .arrow import ArrowPrefixPlan, plan_arrow_prefix
from .async_ import AsyncLogicalPlan
from .logical import Pipeline, Query
from .native import (
    EngineDecision,
    TerminalName,
    select_materializing_engine,
    select_terminal_engine,
    validate_terminal,
)
from .semantic_analyzer import analyze_async_plan, analyze_sync_plan
from .semantics import AsyncTerminalName
from .sync import FilterOp, MapOp, Operation, ParallelMapOp, TapOp

_FUSABLE = (MapOp, FilterOp, TapOp)


def _python_stages(operations: tuple[Operation, ...]) -> list[dict[str, Any]]:
    """Return Python stages, fusing adjacent map/filter/tap nodes."""
    stages: list[dict[str, Any]] = []
    pending: list[str] = []

    def flush_fused() -> None:
        """Emit the pending fusible Python node names as one stage and clear the buffer."""
        if pending:
            stages.append(
                {
                    "engine": "python",
                    "operations": pending.copy(),
                    "fused": len(pending) > 1,
                }
            )
            pending.clear()

    for operation in operations:
        if isinstance(operation, _FUSABLE):
            pending.append(operation.name)
        else:
            flush_fused()
            stages.append(
                {
                    "engine": (
                        operation.backend if isinstance(operation, ParallelMapOp) else "python"
                    ),
                    "operations": [operation.name],
                    "fused": False,
                }
            )
    flush_fused()
    return stages


def _selected_engine_stages(
    plan: Pipeline,
    decision: EngineDecision,
) -> list[dict[str, Any]]:
    """Return the stage layout implied by one selected engine decision."""
    if decision.engine not in {"native", "hybrid"}:
        return _python_stages(plan.operations)

    native_operations = plan.operations[: decision.native_operation_count]
    stages = [
        {
            "engine": "native",
            "operations": [operation.name for operation in native_operations],
            "fused": len(native_operations) > 1,
        }
    ]
    if decision.engine == "hybrid":
        stages.extend(_python_stages(plan.operations[decision.native_operation_count :]))
    return stages


def _with_arrow_prefix(
    plan: Pipeline,
    stages: list[dict[str, Any]],
    arrow_prefix: ArrowPrefixPlan | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return stages and boundaries after an eligible Arrow prefix supersedes selection."""
    if arrow_prefix is None or not arrow_prefix.operation_count:
        return stages, []

    arrow_operations = plan.operations[: arrow_prefix.operation_count]
    arrow_stages = [
        {
            "engine": "arrow",
            "operations": [operation.name for operation in arrow_operations],
            "fused": len(arrow_operations) > 1,
        }
    ]
    boundaries: list[dict[str, Any]] = []
    if arrow_prefix.operation_count < len(plan.operations):
        arrow_stages.extend(_python_stages(plan.operations[arrow_prefix.operation_count :]))
        boundaries.append(
            {
                "from": "arrow",
                "to": "python",
                "after_operation": arrow_prefix.operation_count,
                "materializes_rows": True,
                "guarded": arrow_prefix.guarded,
            }
        )
    return arrow_stages, boundaries


def _with_physical_engines(
    stages: list[dict[str, Any]],
    physical_nodes: tuple[Any, ...],
) -> list[dict[str, Any]]:
    """Return stage copies annotated with physical sort engines."""
    physical_stages = [stage.copy() for stage in stages]
    search_from = 0
    for node in physical_nodes:
        from ..physical.plan import SortPhysicalNode

        if isinstance(node, SortPhysicalNode):
            for index, stage in enumerate(physical_stages[search_from:], start=search_from):
                if node.operation.name in stage["operations"]:
                    stage["engine"] = node.engine
                    search_from = index + 1
                    break
    return physical_stages


def _explanation_stages(
    plan: Pipeline,
    decision: EngineDecision,
    arrow_prefix: ArrowPrefixPlan | None,
    physical_nodes: tuple[Any, ...],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return serialized stages and materialization boundaries for one explanation."""
    stages = _selected_engine_stages(plan, decision)
    stages, boundaries = _with_arrow_prefix(plan, stages, arrow_prefix)
    return _with_physical_engines(stages, physical_nodes), boundaries


def _explanation_data_movement(
    plan: Pipeline,
    terminal: TerminalName,
    decision: EngineDecision,
) -> tuple[dict[str, bool], str]:
    """Return data-movement flags and complexity for one terminal decision."""
    if terminal in {"iterate", "list"}:
        source = plan.source.native_data
        crosses_native_boundary = decision.engine in {"native", "hybrid"}
        container_source = isinstance(source, (list, tuple))
        return (
            {
                "scans_source": crosses_native_boundary and container_source,
                "copies_source": crosses_native_boundary and container_source,
                "materializes": terminal == "list" or decision.engine == "hybrid",
            },
            "O(n)",
        )
    return (
        {
            "scans_source": decision.scans_source,
            "copies_source": decision.copies_source,
            "materializes": decision.materializes,
        },
        decision.complexity,
    )


def _serialize_arrow_prefix(
    arrow_prefix: ArrowPrefixPlan | None,
) -> dict[str, Any] | None:
    """Return the stable JSON-ready representation of an Arrow prefix."""
    if arrow_prefix is None:
        return None
    return {
        "operation_count": arrow_prefix.operation_count,
        "boundary_reason": arrow_prefix.boundary_reason.value,
        "guarded": arrow_prefix.guarded,
    }


@dataclass(frozen=True, slots=True)
class PlanExplanation:
    """Pair a synchronous plan and terminal for deferred explanation serialization."""

    plan: Pipeline
    terminal: TerminalName = "iterate"
    decision: EngineDecision | None = None
    arrow_prefix: ArrowPrefixPlan | None = None
    relations: dict[str, Any] | None = None
    physical_nodes: tuple[Any, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        """Analyze the plan and serialize engine choice, costs, stages, semantics, and boundaries.

        Native or hybrid decisions determine the initial stage layout. An eligible Arrow prefix
        supersedes that layout and records where materialized Python rows begin.
        """
        semantics = analyze_sync_plan(self.plan, self.terminal)
        compiled_decision = self.decision
        decision = compiled_decision or (
            select_materializing_engine(self.plan)
            if self.terminal in {"iterate", "list"}
            else select_terminal_engine(self.plan, self.terminal)
        )
        data_movement, complexity = _explanation_data_movement(
            self.plan,
            self.terminal,
            decision,
        )
        capabilities = self.plan.source.capabilities
        arrow_prefix = self.arrow_prefix
        if arrow_prefix is None and compiled_decision is None:
            arrow_prefix = plan_arrow_prefix(self.plan)
        stages, boundaries = _explanation_stages(
            self.plan,
            decision,
            arrow_prefix,
            self.physical_nodes,
        )

        result = {
            "terminal": self.terminal,
            "source": {
                "reiterable": capabilities.reiterable,
                "exact_size": capabilities.exact_size,
                "ordered": capabilities.ordered,
            },
            "requested_engine": self.plan.engine,
            "selected_engine": decision.engine,
            "streaming_engine": ("python" if self.plan.engine == "auto" else decision.engine),
            "materializing_engine": decision.engine,
            "selection_reason": decision.reason,
            "data_movement": data_movement,
            "complexity": complexity,
            "operations": [{"name": operation.name} for operation in self.plan.operations],
            "stages": stages,
            "semantics": semantics.to_dict(include_diagnostics=False),
            "diagnostics": [item.to_dict() for item in semantics.diagnostics],
            "arrow_prefix": _serialize_arrow_prefix(arrow_prefix),
            "boundaries": boundaries,
        }
        if self.relations is not None:
            result["relations"] = self.relations
        return result


def explain_physical(physical: Any) -> PlanExplanation:
    """Serialize an already compiled physical plan without rerunning backend selection."""
    from ..execution.physical import operations_from_physical_nodes
    from ..physical.plan import BackendPayload

    physical_nodes = physical.nodes
    if physical.root is not None:
        from ..physical.relational import PipelinePhysicalNode

        if isinstance(physical.root, PipelinePhysicalNode):
            physical_nodes = physical.root.stages
    pipeline = Pipeline(
        physical.source,
        operations_from_physical_nodes(physical_nodes),
        physical.engine,
        physical.parallel,
    )
    if physical.root is not None:
        if physical.decision.selected_engine != "python":
            raise ValueError("relational physical plans require a Python-owned executor")
        return PlanExplanation(
            pipeline,
            validate_terminal(physical.terminal.name),
            decision=EngineDecision("python", physical.decision.reason),
            arrow_prefix=None,
            relations=_explain_relation(physical.root),
            physical_nodes=physical_nodes,
        )
    payload = physical.backend_payload
    if not isinstance(payload, BackendPayload) or payload.native_decision is None:
        raise ValueError(f"terminal {physical.terminal.name!r} cannot be explained physically")
    return PlanExplanation(
        pipeline,
        validate_terminal(physical.terminal.name),
        payload.native_decision,
        payload.arrow_prefix,
        physical_nodes=physical.nodes,
    )


def _explain_relation(root: Any) -> dict[str, Any]:
    """Serialize physical relation choices without inspecting sources or callbacks."""
    from ..physical.relational import (
        GlobalAggregatePhysicalNode,
        GroupAggregatePhysicalNode,
        JoinPhysicalNode,
        PipelinePhysicalNode,
        SourcePhysicalNode,
    )

    if isinstance(root, SourcePhysicalNode):
        return {"node": "source", "children": []}
    if isinstance(root, PipelinePhysicalNode):
        return {
            "node": "pipeline",
            "operations": [stage.engine for stage in root.stages],
            "children": [_explain_relation(root.input)],
        }
    if isinstance(root, JoinPhysicalNode):
        return {
            "node": "join",
            "strategy": root.strategy.value,
            "reason": root.reason,
            "children": [_explain_relation(root.left), _explain_relation(root.right)],
        }
    if isinstance(root, GroupAggregatePhysicalNode):
        result: dict[str, Any] = {
            "node": "group_aggregate",
            "strategy": "grace_hash" if root.partitions is not None else "hash",
            "children": [_explain_relation(root.input)],
        }
        if root.arrow_i64_sum is not None:
            result.update(candidate="arrow_hash", guarded=True)
        return result
    if isinstance(root, GlobalAggregatePhysicalNode):
        result = {
            "node": "global_aggregate",
            "children": [_explain_relation(root.input)],
        }
        if root.exact_count_name is not None:
            result.update(candidate="exact_size", guarded=True)
        elif root.arrow_i64_sum is not None:
            result.update(candidate="arrow_reduce", guarded=True)
        return result
    raise TypeError(f"unsupported physical relation: {type(root).__name__}")


def explain_query(query: Query) -> PlanExplanation:
    """Compile then explain a logical query through the single M2 planning path."""
    from .compiler import compile_query

    return explain_physical(compile_query(query))


@dataclass(frozen=True, slots=True)
class AsyncPlanExplanation:
    """Pair an asynchronous plan and terminal for semantic explanation serialization."""

    plan: AsyncLogicalPlan[Any]
    terminal: AsyncTerminalName = "iterate"

    def to_dict(self) -> dict[str, Any]:
        """Serialize async source facts, node analyses, completion semantics, and
        diagnostics."""
        semantics = analyze_async_plan(self.plan, self.terminal)
        return {
            "terminal": self.terminal,
            "source": self.plan.source.facts.to_dict(),
            "operations": [item.to_dict() for item in semantics.operations],
            "semantics": semantics.to_dict(include_diagnostics=False),
            "diagnostics": [item.to_dict() for item in semantics.diagnostics],
        }
