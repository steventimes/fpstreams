"""Named single-pass aggregations with optional native metadata."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Literal

from ..expressions.selectors import Selector, compile_selector
from .collector import (
    Collector,
    finish_collectors,
    initialize_collectors,
    run_collectors,
    step_collectors,
)
from .statistics import OnlineStatistics, mean_from, std_from, variance_from

_MISSING = object()

NativeAggregationKind = Literal[
    "count", "sum", "min", "max", "first", "last", "mean", "variance", "std"
]
NativeAggregateSnapshot = tuple[
    int,
    int | float,
    int | float | None,
    int | float | None,
    int | float | None,
    int | float | None,
    float,
    float,
]


def _identity(value: Any) -> Any:
    return value


class Aggregator(Collector[Any, Any, Any]):
    """A named Collector that may carry metadata for fused native terminals."""

    __slots__ = ()


@dataclass(frozen=True, slots=True)
class NativeAggregation:
    """Planner metadata describing a Rust-compatible aggregation."""

    kind: NativeAggregationKind
    ddof: int = 0


class _DistinctState:
    __slots__ = ("hashable", "unhashable")

    def __init__(self) -> None:
        self.hashable: set[Any] = set()
        self.unhashable: list[Any] = []

    def add(self, value: Any) -> None:
        try:
            self.hashable.add(value)
        except TypeError:
            if value not in self.unhashable:
                self.unhashable.append(value)

    def count(self) -> int:
        return len(self.hashable) + len(self.unhashable)


AggregationItems = tuple[tuple[str, Aggregator], ...]


def prepare_aggregations(aggregations: Mapping[str, Aggregator]) -> AggregationItems:
    if not aggregations:
        raise ValueError("aggregate requires at least one named aggregation")
    items = tuple(aggregations.items())
    for name, aggregation in items:
        if not name:
            raise ValueError("aggregate names cannot be empty")
        if not isinstance(aggregation, Aggregator):
            raise TypeError(f"aggregate {name!r} must be an Aggregator")
    return items


def initialize_aggregations(items: AggregationItems) -> dict[str, Any]:
    return initialize_collectors(items)


def step_aggregations(states: dict[str, Any], items: AggregationItems, value: Any) -> None:
    step_collectors(states, items, value)


def finish_aggregations(states: Mapping[str, Any], items: AggregationItems) -> dict[str, Any]:
    return finish_collectors(states, items)


def run_aggregations(values: Iterable[Any], items: AggregationItems) -> dict[str, Any]:
    return run_collectors(values, items)


def native_aggregation_items(items: AggregationItems) -> bool:
    return all(isinstance(aggregation.native, NativeAggregation) for _name, aggregation in items)


def native_first_only(items: AggregationItems) -> str | None:
    if len(items) != 1:
        return None
    name, aggregation = items[0]
    native = aggregation.native
    return name if isinstance(native, NativeAggregation) and native.kind == "first" else None


def finish_native_aggregations(
    items: AggregationItems, snapshot: NativeAggregateSnapshot
) -> dict[str, Any]:
    count, total, minimum, maximum, first, last, mean, squared_deviations = snapshot
    statistics = (count, mean, squared_deviations)
    values: dict[NativeAggregationKind, Any] = {
        "count": count,
        "sum": total,
        "min": minimum,
        "max": maximum,
        "first": first,
        "last": last,
        "mean": mean_from(statistics),
    }
    result: dict[str, Any] = {}
    for name, aggregation in items:
        native = aggregation.native
        if not isinstance(native, NativeAggregation):
            raise RuntimeError("native aggregation metadata is missing")
        if native.kind == "variance":
            result[name] = variance_from(statistics, native.ddof)
        elif native.kind == "std":
            result[name] = std_from(statistics, native.ddof)
        else:
            result[name] = values[native.kind]
    return result


class _AggFactory:
    """Factory for built-in single-pass aggregators."""

    __slots__ = ()

    def count(self) -> Aggregator:
        """Count all input items.

        Returns:
            A reusable `Aggregator` implementing the described calculation.
        """
        return Aggregator(
            lambda: 0,
            lambda count, _row: count + 1,
            combine=lambda left, right: left + right,
            native=NativeAggregation("count"),
        )

    def count_where(self, predicate: Selector) -> Aggregator:
        """Count items that satisfy predicate.

        Args:
            predicate: A callable that decides whether an item matches.

        Returns:
            A reusable `Aggregator` implementing the described calculation.
        """
        test = compile_selector(predicate)
        return Aggregator(
            lambda: 0,
            lambda count, row: count + bool(test(row)),
        )

    def any(self, predicate: Selector | None = None) -> Aggregator:
        """Return whether at least one item satisfies predicate.

        Args:
            predicate: A callable that decides whether an item matches.

        Returns:
            A reusable `Aggregator` implementing the described calculation.
        """
        test = bool if predicate is None else compile_selector(predicate)
        return Aggregator(
            lambda: False,
            lambda matched, row: matched or bool(test(row)),
            combine=lambda left, right: left or right,
            done=bool,
        )

    def all(self, predicate: Selector | None = None) -> Aggregator:
        """Return whether every item satisfies predicate.

        Args:
            predicate: A callable that decides whether an item matches.

        Returns:
            A reusable `Aggregator` implementing the described calculation.
        """
        test = bool if predicate is None else compile_selector(predicate)
        return Aggregator(
            lambda: True,
            lambda matched, row: matched and bool(test(row)),
            combine=lambda left, right: left and right,
            done=lambda matched: not matched,
        )

    def sum(self, selector: Selector | None = None) -> Aggregator:
        """Sum input items or selected values.

        Args:
            selector: A callable, field name, index, path, or expression used to select a value.

        Returns:
            A reusable `Aggregator` implementing the described calculation.
        """
        select = _identity if selector is None else compile_selector(selector)
        return Aggregator(
            lambda: 0,
            lambda total, row: total + select(row),
            combine=lambda left, right: left + right,
            native=NativeAggregation("sum") if selector is None else None,
        )

    def mean(self, selector: Selector | None = None) -> Aggregator:
        """Return the arithmetic mean of items or selected values.

        Args:
            selector: A callable, field name, index, path, or expression used to select a value.

        Returns:
            A reusable `Aggregator` implementing the described calculation.
        """
        select = _identity if selector is None else compile_selector(selector)

        def step(state: OnlineStatistics, row: Any) -> OnlineStatistics:
            state.accept(select(row))
            return state

        return Aggregator(
            OnlineStatistics,
            step,
            lambda state: mean_from(state.snapshot()),
            native=NativeAggregation("mean") if selector is None else None,
        )

    def variance(self, selector: Selector | None = None, *, ddof: int = 1) -> Aggregator:
        """Return variance using the requested delta degrees of freedom.

        Args:
            selector: A callable, field name, index, path, or expression used to select a value.
            ddof: Delta degrees of freedom used in the variance divisor.

        Returns:
            A reusable `Aggregator` implementing the described calculation.
        """
        return self._variance(selector, ddof=ddof, square_root=False)

    def std(self, selector: Selector | None = None, *, ddof: int = 1) -> Aggregator:
        """Return standard deviation using the requested delta degrees of freedom.

        Args:
            selector: A callable, field name, index, path, or expression used to select a value.
            ddof: Delta degrees of freedom used in the variance divisor.

        Returns:
            A reusable `Aggregator` implementing the described calculation.
        """
        return self._variance(selector, ddof=ddof, square_root=True)

    def _variance(self, selector: Selector | None, *, ddof: int, square_root: bool) -> Aggregator:
        if ddof < 0:
            raise ValueError("ddof must be non-negative")
        select = _identity if selector is None else compile_selector(selector)

        def step(state: OnlineStatistics, row: Any) -> OnlineStatistics:
            state.accept(select(row))
            return state

        def finish(state: OnlineStatistics) -> float | None:
            snapshot = state.snapshot()
            return std_from(snapshot, ddof) if square_root else variance_from(snapshot, ddof)

        kind: Literal["std", "variance"] = "std" if square_root else "variance"
        return Aggregator(
            OnlineStatistics,
            step,
            finish,
            native=NativeAggregation(kind, ddof) if selector is None else None,
        )

    def count_distinct(self, selector: Selector | None = None) -> Aggregator:
        """Count distinct items or selected values.

        Args:
            selector: A callable, field name, index, path, or expression used to select a value.

        Returns:
            A reusable `Aggregator` implementing the described calculation.
        """
        select = _identity if selector is None else compile_selector(selector)

        def step(state: _DistinctState, row: Any) -> _DistinctState:
            state.add(select(row))
            return state

        return Aggregator(_DistinctState, step, _DistinctState.count)

    def min(self, selector: Selector | None = None) -> Aggregator:
        """Return the smallest item or selected value.

        Args:
            selector: A callable, field name, index, path, or expression used to select a value.

        Returns:
            A reusable `Aggregator` implementing the described calculation.
        """
        select = _identity if selector is None else compile_selector(selector)

        def step(current: Any, row: Any) -> Any:
            value = select(row)
            return value if current is _MISSING or value < current else current

        return Aggregator(
            lambda: _MISSING,
            step,
            lambda value: None if value is _MISSING else value,
            native=NativeAggregation("min") if selector is None else None,
        )

    def max(self, selector: Selector | None = None) -> Aggregator:
        """Return the largest item or selected value.

        Args:
            selector: A callable, field name, index, path, or expression used to select a value.

        Returns:
            A reusable `Aggregator` implementing the described calculation.
        """
        select = _identity if selector is None else compile_selector(selector)

        def step(current: Any, row: Any) -> Any:
            value = select(row)
            return value if current is _MISSING or value > current else current

        return Aggregator(
            lambda: _MISSING,
            step,
            lambda value: None if value is _MISSING else value,
            native=NativeAggregation("max") if selector is None else None,
        )

    def first(self, selector: Selector | None = None) -> Aggregator:
        """Return the first item or selected value.

        Args:
            selector: A callable, field name, index, path, or expression used to select a value.

        Returns:
            A reusable `Aggregator` implementing the described calculation.
        """
        select = _identity if selector is None else compile_selector(selector)
        return Aggregator(
            lambda: _MISSING,
            lambda current, row: select(row) if current is _MISSING else current,
            lambda value: None if value is _MISSING else value,
            done=lambda value: value is not _MISSING,
            native=NativeAggregation("first") if selector is None else None,
        )

    def last(self, selector: Selector | None = None) -> Aggregator:
        """Return the last item or selected value.

        Args:
            selector: A callable, field name, index, path, or expression used to select a value.

        Returns:
            A reusable `Aggregator` implementing the described calculation.
        """
        select = _identity if selector is None else compile_selector(selector)
        return Aggregator(
            lambda: _MISSING,
            lambda _current, row: select(row),
            lambda value: None if value is _MISSING else value,
            native=NativeAggregation("last") if selector is None else None,
        )

    def collect(
        self, selector: Selector | None = None, *, into: Callable[[Any], Any] = list
    ) -> Aggregator:
        """Collect items or selected values with into.

        This creates an aggregator that accumulates selected values; it does not execute a flow
        immediately.

        Args:
            selector: A callable, field name, index, path, or expression used to select a value.
            into: A collector or container factory used for the final values.

        Returns:
            An aggregator that collects the selected values.
        """
        select = _identity if selector is None else compile_selector(selector)

        def append(values: list[Any], row: Any) -> list[Any]:
            values.append(select(row))
            return values

        return Aggregator(
            list,
            append,
            into,
        )


agg = _AggFactory()
