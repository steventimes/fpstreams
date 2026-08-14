"""Human-readable explanations of planner and engine decisions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .native import TerminalName, select_materializing_engine, select_terminal_engine
from .sync import FilterOp, MapOp, Operation, ParallelMapOp, Plan, TapOp

_FUSABLE = (MapOp, FilterOp, TapOp)


def _append_python_stages(stages: list[dict[str, Any]], operations: tuple[Operation, ...]) -> None:
    pending: list[str] = []

    def flush_fused() -> None:
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
    plan: Plan
    terminal: TerminalName = "iterate"

    def to_dict(self) -> dict[str, Any]:
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
        }
