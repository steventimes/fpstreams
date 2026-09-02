"""Closed project-owned aggregation lanes for key/value pipelines."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, TypeAlias

from ..collecting.aggregation import (
    AggregationItems,
    native_group_aggregation,
    project_count_aggregation,
)
from ..runtime.failpoints import has_active_failpoints

PairAggregationKind: TypeAlias = Literal["count", "sum", "min", "max"]

_BUILTIN_STR = str
_BUILTIN_TYPE = type


@dataclass(frozen=True, slots=True)
class ClosedPairAggregations:
    """One revalidated native-compatible set of whole-value aggregation lanes."""

    items: AggregationItems
    kinds: tuple[PairAggregationKind, ...]

    def is_live(self) -> bool:
        """Revalidate public collector lifecycle state after the source has opened."""
        return not has_active_failpoints() and all(
            _closed_kind(aggregation) == expected
            for (_name, aggregation), expected in zip(self.items, self.kinds, strict=True)
        )


def _closed_kind(aggregation: Any) -> PairAggregationKind | None:
    if project_count_aggregation(aggregation):
        return "count"
    native = native_group_aggregation(aggregation)
    if native is None or native.selector is not None:
        return None
    match native.kind:
        case "sum":
            return "sum"
        case "min":
            return "min"
        case "max":
            return "max"
        case _:
            return None


def compile_closed_pair_aggregations(
    items: AggregationItems,
) -> ClosedPairAggregations | None:
    """Compile unchanged project factories or decline to the canonical collectors."""
    kinds: list[PairAggregationKind] = []
    for name, aggregation in items:
        # Generic states index every lane by its output name on every row. A string
        # subclass can therefore observe hashes that compact list lanes deliberately omit.
        if _BUILTIN_TYPE(name) is not _BUILTIN_STR:
            return None
        kind = _closed_kind(aggregation)
        if kind is None:
            return None
        kinds.append(kind)
    frozen_kinds = tuple(kinds)
    return ClosedPairAggregations(items, frozen_kinds)
