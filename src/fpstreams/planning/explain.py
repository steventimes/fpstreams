"""Serialize planner semantics, execution stages, boundaries, and engine cost decisions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .arrow import plan_arrow_prefix
from .async_ import _AsyncPlan
from .native import TerminalName, select_materializing_engine, select_terminal_engine
from .semantic_analyzer import analyze_async_plan, analyze_sync_plan
from .semantics import AsyncTerminalName
from .sync import FilterOp, MapOp, Operation, ParallelMapOp, Plan, TapOp

_FUSABLE = (MapOp, FilterOp, TapOp)


def _append_python_stages(stages: list[dict[str, Any]], operations: tuple[Operation, ...]) -> None:
    """Append Python stages, fusing adjacent map/filter/tap nodes in the explanation."""
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


@dataclass(frozen=True, slots=True)
class PlanExplanation:
    """Pair a synchronous plan and terminal for deferred explanation serialization."""

    plan: Plan
    terminal: TerminalName = "iterate"

    def to_dict(self) -> dict[str, Any]:
        """Analyze the plan and serialize engine choice, costs, stages, semantics, and boundaries.

        Native or hybrid decisions determine the initial stage layout. An eligible Arrow prefix
        supersedes that layout and records where materialized Python rows begin.
        """
        semantics = analyze_sync_plan(self.plan, self.terminal)
        decision = (
            select_materializing_engine(self.plan)
            if self.terminal in {"iterate", "list"}
            else select_terminal_engine(self.plan, self.terminal)
        )
        if self.terminal in {"iterate", "list"}:
            source = self.plan.source.native_data
            crosses_native_boundary = decision.engine in {"native", "hybrid"}
            container_source = isinstance(source, (list, tuple))
            data_movement = {
                "scans_source": crosses_native_boundary and container_source,
                "copies_source": crosses_native_boundary and container_source,
                "materializes": self.terminal == "list" or decision.engine == "hybrid",
            }
            complexity = "O(n)"
        else:
            data_movement = {
                "scans_source": decision.scans_source,
                "copies_source": decision.copies_source,
                "materializes": decision.materializes,
            }
            complexity = decision.complexity
        capabilities = self.plan.source.capabilities
        stages: list[dict[str, Any]] = []

        if decision.engine in {"native", "hybrid"}:
            native_operations = self.plan.operations[: decision.native_operation_count]
            stages.append(
                {
                    "engine": "native",
                    "operations": [operation.name for operation in native_operations],
                    "fused": len(native_operations) > 1,
                }
            )
            if decision.engine == "hybrid":
                _append_python_stages(
                    stages,
                    self.plan.operations[decision.native_operation_count :],
                )
        else:
            _append_python_stages(stages, self.plan.operations)

        arrow_prefix = plan_arrow_prefix(self.plan)
        boundaries: list[dict[str, Any]] = []
        if arrow_prefix is not None and arrow_prefix.operation_count:
            arrow_operations = self.plan.operations[: arrow_prefix.operation_count]
            stages = [
                {
                    "engine": "arrow",
                    "operations": [operation.name for operation in arrow_operations],
                    "fused": len(arrow_operations) > 1,
                }
            ]
            if arrow_prefix.operation_count < len(self.plan.operations):
                _append_python_stages(stages, self.plan.operations[arrow_prefix.operation_count :])
                boundaries.append(
                    {
                        "from": "arrow",
                        "to": "python",
                        "after_operation": arrow_prefix.operation_count,
                        "materializes_rows": True,
                        "guarded": arrow_prefix.guarded,
                    }
                )

        return {
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
            "arrow_prefix": None
            if (arrow := plan_arrow_prefix(self.plan)) is None
            else {
                "operation_count": arrow.operation_count,
                "boundary_reason": arrow.boundary_reason.value,
                "guarded": arrow.guarded,
            },
            "boundaries": boundaries,
        }


@dataclass(frozen=True, slots=True)
class AsyncPlanExplanation:
    """Pair an asynchronous plan and terminal for semantic explanation serialization."""

    plan: _AsyncPlan[Any]
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
