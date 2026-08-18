"""Find Arrow-executable map/filter prefixes without importing optional PyArrow."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from .arrow_source import ArrowBatchSource
from .sync import FilterOp, MapOp, Plan


class ArrowBoundaryReason(StrEnum):
    """Classify why Arrow execution is unavailable, stops, or covers the full plan."""

    FORCED_PYTHON = "forced_python"
    NON_ARROW_SOURCE = "non_arrow_source"
    UNSUPPORTED_OPERATION = "unsupported_operation"
    OPAQUE_EXPRESSION = "opaque_expression"
    UNSUPPORTED_EXPRESSION = "unsupported_expression"
    FULL_PREFIX = "full_prefix"


@dataclass(frozen=True, slots=True)
class ArrowPrefixPlan:
    """Describe the leading operations retained in Arrow and the following boundary."""

    operation_count: int
    operations: tuple[MapOp | FilterOp, ...]
    boundary_reason: ArrowBoundaryReason
    guarded: bool


@dataclass(frozen=True, slots=True)
class PlannedRowCallable:
    """Wrap a row callable so structural planning can recognize a closed projection."""

    function: Callable[[Any], Any]
    role: str = "projection"

    def __call__(self, row: Any) -> Any:
        """Delegate row evaluation to the wrapped callable."""
        return self.function(row)


def plan_arrow_prefix(plan: Plan) -> ArrowPrefixPlan | None:
    """Return the longest Arrow-safe leading map/filter segment for an automatic plan.

    Forced engines and non-Arrow sources return ``None``. Planning stops at the first
    unsupported operation or opaque Python callable; accepted expression nodes remain guarded
    because runtime schema and kernel support are checked by the Arrow executor.
    """
    if plan.engine != "auto":
        return None
    if not isinstance(plan.source.native_data, ArrowBatchSource):
        return None
    accepted: list[MapOp | FilterOp] = []
    for operation in plan.operations:
        if not isinstance(operation, (MapOp, FilterOp)):
            return ArrowPrefixPlan(
                len(accepted), tuple(accepted), ArrowBoundaryReason.UNSUPPORTED_OPERATION, True
            )
        # RowExpr and the internal projection wrapper are structurally closed. An arbitrary
        # Python callable is opaque to Arrow and starts the Python suffix.
        callable_node = getattr(operation, "function", getattr(operation, "predicate", None))
        if callable_node.__class__.__name__ == "RowExpr" or isinstance(
            callable_node, PlannedRowCallable
        ):
            accepted.append(operation)
        else:
            return ArrowPrefixPlan(
                len(accepted), tuple(accepted), ArrowBoundaryReason.OPAQUE_EXPRESSION, True
            )
    return ArrowPrefixPlan(len(accepted), tuple(accepted), ArrowBoundaryReason.FULL_PREFIX, True)
