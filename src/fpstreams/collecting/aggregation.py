"""Build named single-pass aggregators and finalize fused native snapshots."""

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
    """Select an entire input row when an aggregation has no explicit selector."""
    return value


class Aggregator(Collector[Any, Any, Any]):
    """A collector accepted by named aggregate terminals and optional native fusion."""

    __slots__ = ()


@dataclass(frozen=True, slots=True)
class NativeAggregation:
    """Identify a whole-value aggregation supported by the fused native snapshot.

    `ddof` is consulted only for variance and standard-deviation kinds.
    """

    kind: NativeAggregationKind
    ddof: int = 0


class _DistinctState:
    """Track distinct hashable values in a set and unhashable values in a list."""

    __slots__ = ("hashable", "unhashable")

    def __init__(self) -> None:
        """Initialize empty storage for both hashability categories."""
        self.hashable: set[Any] = set()
        self.unhashable: list[Any] = []

    def add(self, value: Any) -> None:
        """Add a value once, falling back to equality-based list membership if unhashable."""
        try:
            self.hashable.add(value)
        except TypeError:
            if value not in self.unhashable:
                self.unhashable.append(value)

    def count(self) -> int:
        """Return the combined number of hashable and unhashable distinct values."""
        return len(self.hashable) + len(self.unhashable)


AggregationItems = tuple[tuple[str, Aggregator], ...]


def prepare_aggregations(aggregations: Mapping[str, Aggregator]) -> AggregationItems:
    """Validate named aggregators and freeze mapping order as `(name, aggregator)` pairs.

    The mapping must be nonempty, names must be truthy, and every value must be an
    :class:`Aggregator` rather than an arbitrary collector.
    """
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
    """Initialize one independent state per named aggregator."""
    return initialize_collectors(items)


def step_aggregations(states: dict[str, Any], items: AggregationItems, value: Any) -> None:
    """Offer one input value to every named aggregator that is not complete."""
    step_collectors(states, items, value)


def finish_aggregations(states: Mapping[str, Any], items: AggregationItems) -> dict[str, Any]:
    """Finish named aggregation states into an insertion-ordered result dictionary."""
    return finish_collectors(states, items)


def run_aggregations(values: Iterable[Any], items: AggregationItems) -> dict[str, Any]:
    """Run named aggregators in one traversal that closes its iterator and can stop early."""
    return run_collectors(values, items)


def native_aggregation_items(items: AggregationItems) -> bool:
    """Return whether every aggregator carries fused-native metadata."""
    return all(isinstance(aggregation.native, NativeAggregation) for _name, aggregation in items)


def native_first_only(items: AggregationItems) -> str | None:
    """Return the result name for exactly one native `first` aggregation, else `None`."""
    if len(items) != 1:
        return None
    name, aggregation = items[0]
    native = aggregation.native
    return name if isinstance(native, NativeAggregation) and native.kind == "first" else None


def finish_native_aggregations(
    items: AggregationItems, snapshot: NativeAggregateSnapshot
) -> dict[str, Any]:
    """Project one fused native snapshot into each named aggregation result.

    Count, sum, extrema, endpoints, and mean are read directly from the snapshot. Variance and
    standard deviation use each aggregator's `ddof`. Missing native metadata raises
    `RuntimeError` because the caller selected a native-only finalization path.
    """
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
    """Construct built-in aggregators for `Flow.aggregate` and tabular grouping."""

    __slots__ = ()

    def count(self) -> Aggregator:
        """Build a mergeable whole-value counter with native metadata.

        Returns:
            An aggregator returning zero for empty input.
        """
        from .reducer import COUNT_LAWS, ReducerAggregator

        return ReducerAggregator(
            lambda: 0,
            lambda count, _row: count + 1,
            merge=lambda left, right: left + right,
            laws=COUNT_LAWS,
            native=NativeAggregation("count"),
        )

    def count_where(self, predicate: Selector) -> Aggregator:
        """Build an aggregator that counts truthy selector results.

        Args:
            predicate: Selector evaluated once per input item and converted to `bool`.

        Returns:
            An aggregator returning zero for empty input.
        """
        test = compile_selector(predicate)
        return Aggregator(
            lambda: 0,
            lambda count, row: count + bool(test(row)),
        )

    def any(self, predicate: Selector | None = None) -> Aggregator:
        """Build an aggregator that stops after the first truthy selected value.

        With no predicate, each whole item is tested for truth. Empty input returns `False`.

        Args:
            predicate: Optional selector whose result is converted to `bool`.

        Returns:
            An OR-combinable, early-stopping Boolean aggregator.
        """
        test = bool if predicate is None else compile_selector(predicate)
        return Aggregator(
            lambda: False,
            lambda matched, row: matched or bool(test(row)),
            combine=lambda left, right: left or right,
            done=bool,
        )

    def all(self, predicate: Selector | None = None) -> Aggregator:
        """Build an aggregator that stops after the first false selected value.

        With no predicate, each whole item is tested for truth. Empty input returns `True`.

        Args:
            predicate: Optional selector whose result is converted to `bool`.

        Returns:
            An AND-combinable, early-stopping Boolean aggregator.
        """
        test = bool if predicate is None else compile_selector(predicate)
        return Aggregator(
            lambda: True,
            lambda matched, row: matched and bool(test(row)),
            combine=lambda left, right: left and right,
            done=lambda matched: not matched,
        )

    def sum(self, selector: Selector | None = None) -> Aggregator:
        """Build an aggregator that adds selected values from a zero identity.

        Whole-value sums carry native metadata; selected sums execute through Python selector
        evaluation. Empty input returns zero.

        Args:
            selector: Value selector; `None` adds each whole input item.

        Returns:
            An addition-combinable aggregator.
        """
        select = _identity if selector is None else compile_selector(selector)
        return Aggregator(
            lambda: 0,
            lambda total, row: total + select(row),
            combine=lambda left, right: left + right,
            native=NativeAggregation("sum") if selector is None else None,
        )

    def mean(self, selector: Selector | None = None) -> Aggregator:
        """Build an aggregator for a compensated one-pass arithmetic mean.

        Selected values must be real numbers accepted by :class:`OnlineStatistics`. Empty
        input returns `None`; whole-value means carry native metadata.

        Args:
            selector: Value selector; `None` averages each whole input item.

        Returns:
            An aggregator finishing online statistics as `float` or `None`.
        """
        select = _identity if selector is None else compile_selector(selector)

        def step(state: OnlineStatistics, row: Any) -> OnlineStatistics:
            """Select one real value and update the mutable online-statistics state."""
            state.accept(select(row))
            return state

        return Aggregator(
            OnlineStatistics,
            step,
            lambda state: mean_from(state.snapshot()),
            native=NativeAggregation("mean") if selector is None else None,
        )

    def variance(self, selector: Selector | None = None, *, ddof: int = 1) -> Aggregator:
        """Build an aggregator for one-pass variance with divisor `count - ddof`.

        The default computes sample variance. Results are `None` when `count <= ddof`.

        Args:
            selector: Value selector; `None` consumes each whole input item.
            ddof: Non-negative delta degrees of freedom.

        Returns:
            An online-statistics aggregator returning variance or `None`.

        Raises:
            ValueError: If `ddof` is negative.
        """
        return self._variance(selector, ddof=ddof, square_root=False)

    def std(self, selector: Selector | None = None, *, ddof: int = 1) -> Aggregator:
        """Build an aggregator for the square root of one-pass variance.

        The default computes sample standard deviation. Results are `None` when
        `count <= ddof`.

        Args:
            selector: Value selector; `None` consumes each whole input item.
            ddof: Non-negative delta degrees of freedom.

        Returns:
            An online-statistics aggregator returning standard deviation or `None`.

        Raises:
            ValueError: If `ddof` is negative.
        """
        return self._variance(selector, ddof=ddof, square_root=True)

    def _variance(self, selector: Selector | None, *, ddof: int, square_root: bool) -> Aggregator:
        """Construct the shared online state machine for variance or standard deviation."""
        if ddof < 0:
            raise ValueError("ddof must be non-negative")
        select = _identity if selector is None else compile_selector(selector)

        def step(state: OnlineStatistics, row: Any) -> OnlineStatistics:
            """Select one real value and update the mutable online-statistics state."""
            state.accept(select(row))
            return state

        def finish(state: OnlineStatistics) -> float | None:
            """Finish the state as variance or its square root under the captured `ddof`."""
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
        """Build an aggregator that counts distinct selected values.

        Hashable values use set semantics. Unhashable values are retained once according to
        equality-based list membership. Empty input returns zero.

        Args:
            selector: Value selector; `None` compares each whole input item.

        Returns:
            An aggregator backed by separate hashable and unhashable state.
        """
        select = _identity if selector is None else compile_selector(selector)

        def step(state: _DistinctState, row: Any) -> _DistinctState:
            """Select one value, add it if distinct, and preserve the mutable state."""
            state.add(select(row))
            return state

        return Aggregator(_DistinctState, step, _DistinctState.count)

    def min(self, selector: Selector | None = None) -> Aggregator:
        """Build an aggregator that retains the first smallest selected value.

        Values are compared with `<`; equal values keep the earlier representative. Empty
        input returns `None`, and whole-value minima carry native metadata.

        Args:
            selector: Value selector; `None` compares each whole input item.

        Returns:
            An aggregator returning the minimum selected value or `None`.
        """
        select = _identity if selector is None else compile_selector(selector)

        def step(current: Any, row: Any) -> Any:
            """Replace missing or larger state with the newly selected value."""
            value = select(row)
            return value if current is _MISSING or value < current else current

        return Aggregator(
            lambda: _MISSING,
            step,
            lambda value: None if value is _MISSING else value,
            native=NativeAggregation("min") if selector is None else None,
        )

    def max(self, selector: Selector | None = None) -> Aggregator:
        """Build an aggregator that retains the first largest selected value.

        Values are compared with `>`; equal values keep the earlier representative. Empty
        input returns `None`, and whole-value maxima carry native metadata.

        Args:
            selector: Value selector; `None` compares each whole input item.

        Returns:
            An aggregator returning the maximum selected value or `None`.
        """
        select = _identity if selector is None else compile_selector(selector)

        def step(current: Any, row: Any) -> Any:
            """Replace missing or smaller state with the newly selected value."""
            value = select(row)
            return value if current is _MISSING or value > current else current

        return Aggregator(
            lambda: _MISSING,
            step,
            lambda value: None if value is _MISSING else value,
            native=NativeAggregation("max") if selector is None else None,
        )

    def first(self, selector: Selector | None = None) -> Aggregator:
        """Build an aggregator that stops after the first selected value.

        Empty input returns `None`. A selected `None` is still a completed first value, and a
        whole-value aggregation carries native short-circuit metadata.

        Args:
            selector: Value selector; `None` returns the whole first item.

        Returns:
            An aggregator consuming at most one item.
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
        """Build an aggregator that consumes all input and retains its last selected value.

        Empty input returns `None`; whole-value aggregation carries native metadata.

        Args:
            selector: Value selector; `None` returns the whole last item.

        Returns:
            An aggregator retaining one current value.
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
        """Build an aggregator that buffers selected values and calls `into` at finish.

        `list` and `tuple` finishers produce lawful mergeable reducer aggregators. Other
        callable finishers still receive the full encounter-ordered list but do not declare a
        state merger. This factory does not execute a flow immediately.

        Args:
            selector: Value selector; `None` collects each whole input item.
            into: Callable invoked once with the accumulated list.

        Returns:
            An aggregator returning `into(selected_values)`.

        Raises:
            TypeError: If `into` is not callable.
        """
        select = _identity if selector is None else compile_selector(selector)

        def append(values: list[Any], row: Any) -> list[Any]:
            """Append one selected value and preserve the mutable list state."""
            values.append(select(row))
            return values

        from .reducer import LIST_LAWS, ReducerAggregator

        if into in (list, tuple):
            return ReducerAggregator(
                list,
                append,
                into,
                merge=lambda left, right: left.extend(right) or left,
                laws=LIST_LAWS,
            )
        return Aggregator(
            list,
            append,
            into,
        )


agg = _AggFactory()
