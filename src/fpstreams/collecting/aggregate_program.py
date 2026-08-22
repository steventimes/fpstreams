"""Exact native snapshot masks for compiled existing aggregation collectors."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Literal, TypeAlias

from .aggregation import AggregationItems, NativeAggregation
from .program import CollectorProgram, compile_collectors

ScalarAggregateTerminal: TypeAlias = Literal["count", "min", "max", "first", "last"]


class NativeAggregateField(StrEnum):
    """Individual fields supplied by the existing native aggregate snapshot."""

    COUNT = "count"
    TOTAL = "total"
    MINIMUM = "minimum"
    MAXIMUM = "maximum"
    FIRST = "first"
    LAST = "last"
    MEAN = "mean"
    M2 = "m2"


@dataclass(frozen=True, slots=True)
class NativeAggregateMask:
    """The exact snapshot fields required by every requested native aggregation."""

    fields: frozenset[NativeAggregateField]

    @property
    def bits(self) -> int:
        """Encode the stable Rust ABI bitset without exposing it in the public API."""
        return sum(_FIELD_BITS[field] for field in self.fields)

    @property
    def scalar_terminal(self) -> ScalarAggregateTerminal | None:
        """Return the existing scalar kernel when every result needs one field."""
        if len(self.fields) != 1:
            return None
        return _SCALAR_TERMINALS.get(next(iter(self.fields)))

    @property
    def statistics_only(self) -> bool:
        """Return whether the established statistics kernel supplies every field."""
        return bool(self.fields & _STATISTIC_VALUES) and self.fields <= _STATISTICS_FIELDS

    @property
    def total_only(self) -> bool:
        """Return whether a wide or compensated sum is the sole requested field."""
        return self.fields == _TOTAL_ONLY

    def prefers_masked_kernel(self, kind: str) -> bool:
        """Choose masks only for field mixes that beat the branch-free full loop.

        Online statistics dominate the arithmetic cost and the established full
        loop is faster than checking masks around that state. Float totals have
        the same result: compensation plus mask branches costs more than the
        compact full loop. Integer totals and extrema-only mixes retain clear
        measured wins from skipping Welford statistics.
        """
        if self.fields & _STATISTIC_VALUES:
            return False
        return kind != "f64" or NativeAggregateField.TOTAL not in self.fields


@dataclass(frozen=True, slots=True)
class AggregationProgram:
    """Indexed collector program plus optional all-native snapshot metadata."""

    collectors: CollectorProgram
    native_mask: NativeAggregateMask | None
    native_only: bool


_FIELDS = {
    "count": frozenset({NativeAggregateField.COUNT}),
    "sum": frozenset({NativeAggregateField.TOTAL}),
    "min": frozenset({NativeAggregateField.MINIMUM}),
    "max": frozenset({NativeAggregateField.MAXIMUM}),
    "first": frozenset({NativeAggregateField.FIRST}),
    "last": frozenset({NativeAggregateField.LAST}),
    "mean": frozenset({NativeAggregateField.COUNT, NativeAggregateField.MEAN}),
    "variance": frozenset(
        {NativeAggregateField.COUNT, NativeAggregateField.MEAN, NativeAggregateField.M2}
    ),
    "std": frozenset(
        {NativeAggregateField.COUNT, NativeAggregateField.MEAN, NativeAggregateField.M2}
    ),
}

# The numeric values are an internal ABI shared with ``rust/src/common.rs``.
# Keeping field names as strings above makes planner diagnostics readable while
# this private table provides a compact call boundary for the native extension.
_FIELD_BITS = {
    NativeAggregateField.COUNT: 1 << 0,
    NativeAggregateField.TOTAL: 1 << 1,
    NativeAggregateField.MINIMUM: 1 << 2,
    NativeAggregateField.MAXIMUM: 1 << 3,
    NativeAggregateField.FIRST: 1 << 4,
    NativeAggregateField.LAST: 1 << 5,
    NativeAggregateField.MEAN: 1 << 6,
    NativeAggregateField.M2: 1 << 7,
}
_SCALAR_TERMINALS: dict[NativeAggregateField, ScalarAggregateTerminal] = {
    NativeAggregateField.COUNT: "count",
    NativeAggregateField.MINIMUM: "min",
    NativeAggregateField.MAXIMUM: "max",
    NativeAggregateField.FIRST: "first",
    NativeAggregateField.LAST: "last",
}
_STATISTIC_VALUES = frozenset({NativeAggregateField.MEAN, NativeAggregateField.M2})
_STATISTICS_FIELDS = frozenset({NativeAggregateField.COUNT, *_STATISTIC_VALUES})
_TOTAL_ONLY = frozenset({NativeAggregateField.TOTAL})


def compile_aggregations(items: AggregationItems) -> AggregationProgram:
    """Compile aggregators and derive a mask only when every one is native-backed."""
    collectors = compile_collectors(items)
    native_items = [aggregation.native for _name, aggregation in items]
    if not all(isinstance(value, NativeAggregation) for value in native_items):
        return AggregationProgram(collectors, None, False)
    fields: set[NativeAggregateField] = set()
    for native in native_items:
        assert isinstance(native, NativeAggregation)
        fields.update(_FIELDS[native.kind])
    return AggregationProgram(collectors, NativeAggregateMask(frozenset(fields)), True)
