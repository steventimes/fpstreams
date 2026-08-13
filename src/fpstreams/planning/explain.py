"""Human-readable explanations of planner and engine decisions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .native import select_materializing_engine
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

    def to_dict(self) -> dict[str, Any]:
        decision = select_materializing_engine(self.plan)
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
            "operations": [{"name": operation.name} for operation in self.plan.operations],
            "stages": stages,
        }
