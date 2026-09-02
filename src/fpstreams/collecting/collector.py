"""Run composable reduction state machines together with optional early stopping."""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Generic, Literal, TypeVar

from ..errors import DuplicateKeyError
from ..expressions.selectors import Selector, compile_selector
from ..runtime.iterators import closing_iterators
from ._collector_base import (
    Collector as Collector,
)
from ._collector_base import (
    CollectorItems as CollectorItems,
)
from ._collector_base import (
    _identity as _identity,
)
from ._collector_base import (
    _never_done as _never_done,
)

T = TypeVar("T")
K = TypeVar("K")
R = TypeVar("R")
U = TypeVar("U")
V = TypeVar("V")

_MISSING = object()


def prepare_collectors(
    collectors: Mapping[str, Collector[Any, Any, Any]],
) -> CollectorItems:
    """Validate named collectors and freeze their mapping order as `(name, collector)` pairs.

    The mapping must be nonempty, every name must be truthy, and every value must be a
    :class:`Collector`.
    """
    if not collectors:
        raise ValueError("collect requires a Collector or at least one named Collector")
    items = tuple(collectors.items())
    for name, collector in items:
        if not name:
            raise ValueError("collector names cannot be empty")
        if not isinstance(collector, Collector):
            raise TypeError(f"collector {name!r} must be a Collector")
    return items


def initialize_collectors(items: CollectorItems) -> dict[str, Any]:
    """Call each collector initializer and index the independent states by name."""
    return {name: collector.initializer() for name, collector in items}


def collectors_done(states: Mapping[str, Any], items: CollectorItems) -> bool:
    """Return whether every named collector reports its current state complete."""
    return all(collector.done(states[name]) for name, collector in items)


def step_collectors(states: dict[str, Any], items: CollectorItems, value: Any) -> None:
    """Offer one input value to each collector that has not completed.

    State entries are replaced with each `step` return value. The `_never_done` sentinel is
    recognized directly so the default predicate is not called on the hot path.
    """
    for name, collector in items:
        state = states[name]
        if collector.done is _never_done or not collector.done(state):
            states[name] = collector.step(state, value)


def finish_collectors(states: Mapping[str, Any], items: CollectorItems) -> dict[str, Any]:
    """Finish every named state and return results in collector order."""
    return {name: collector.finish(states[name]) for name, collector in items}


def run_collectors(values: Iterable[Any], items: CollectorItems) -> dict[str, Any]:
    """Run the canonical fixed-layout collector program in one traversal."""
    from .program import compile_collectors, run_collector_program

    return run_collector_program(values, compile_collectors(items))


def _append(values: list[Any], value: Any) -> list[Any]:
    """Append one value to list state and return that same mutable list."""
    values.append(value)
    return values


def _add(values: set[Any], value: Any) -> set[Any]:
    """Add one hashable value to set state and return that same mutable set."""
    values.add(value)
    return values


def _extend(left: list[Any], right: list[Any]) -> list[Any]:
    """Append a right partial list to a left partial list and return the mutated left list."""
    left.extend(right)
    return left


def _update(left: set[Any], right: set[Any]) -> set[Any]:
    """Union a right partial set into a left partial set and return the mutated left set."""
    left.update(right)
    return left


def _deque_add(values: deque[Any], value: Any) -> deque[Any]:
    """Append to bounded deque state, automatically discarding its oldest value when full."""
    values.append(value)
    return values


def _selector(selector: Selector | None) -> Callable[[Any], Any]:
    """Compile a selector, or return identity selection when no selector was supplied."""
    return _identity if selector is None else compile_selector(selector)


def _as_collector(
    downstream: Collector[Any, Any, Any] | Callable[[Iterable[Any]], Any],
) -> Collector[Any, Any, Any]:
    """Return a collector unchanged or adapt a callable into a list-finishing collector.

    Callable adapters buffer all downstream items in order, invoke the callable once during
    finishing, and expose list concatenation for combining partial state. Other values raise
    `TypeError`.
    """
    if isinstance(downstream, Collector):
        return downstream
    if not callable(downstream):
        raise TypeError("downstream must be a Collector or callable")
    return Collector(list, _append, downstream, _extend)


@dataclass(slots=True)
class SummaryStatistics:
    """Mutable count, numeric sum, minimum, maximum, and derived average state."""

    count: int = 0
    sum: float = 0.0
    min: float = float("inf")
    max: float = float("-inf")

    @property
    def average(self) -> float:
        """Divide the running sum by count, returning `0.0` for empty state.

        Returns:
            `sum / count` when at least one value was accepted, otherwise `0.0`.
        """
        return self.sum / self.count if self.count else 0.0

    def accept(self, value: float) -> None:
        """Increment count and update sum, minimum, and maximum in place.

        Args:
            value: Numeric value supporting addition and ordering with prior values.
        """
        self.count += 1
        self.sum += value
        self.min = value if value < self.min else self.min
        self.max = value if value > self.max else self.max


@dataclass(slots=True)
class _ColumnsState:
    """Track column lists and the row count needed to backfill newly seen fields."""

    columns: dict[str, list[Any]]
    count: int = 0


@dataclass(slots=True)
class _TeeState:
    """Hold the independent mutable states of two collectors sharing one source."""

    left: Any
    right: Any


def _collect_columns(values: Iterable[Mapping[str, Any]]) -> dict[str, list[Any]]:
    """Transpose mapping rows into encounter-ordered, `None`-padded columns.

    A field first seen after earlier rows receives one leading `None` per prior row. Every
    established column receives `row.get(name)` for each later row, so missing fields are also
    represented by `None`.
    """
    state = _ColumnsState({})
    for row in values:
        for name in row:
            if name not in state.columns:
                state.columns[name] = [None] * state.count
        for name, column in state.columns.items():
            column.append(row.get(name))
        state.count += 1
    return state.columns


class Collectors(Generic[T]):
    """Build reusable collectors for containers, grouping, adaptation, and summaries."""

    @staticmethod
    def to_list() -> Collector[T, list[T], list[T]]:
        """Build an order-preserving, mergeable collector for all input items.

        Returns:
            A reducer whose finished list is also its accumulated state.
        """
        from .reducer import LIST_LAWS, Reducer

        return Reducer(list, _append, merge=_extend, laws=LIST_LAWS)

    @staticmethod
    def to_set() -> Collector[T, set[T], set[T]]:
        """Build a collector that retains each distinct hashable input item.

        Returns:
            A collector with set-union support for partial states.
        """
        return Collector(set, _add, combine=_update)

    @staticmethod
    def to_tuple() -> Collector[T, list[T], tuple[T, ...]]:
        """Build an order-preserving collector that finishes list state as a tuple.

        Returns:
            A mergeable reducer returning an immutable tuple.
        """
        from .reducer import LIST_LAWS, Reducer

        return Reducer(list, _append, tuple, merge=_extend, laws=LIST_LAWS)

    @staticmethod
    def joining(delimiter: str = "") -> Collector[Any, list[str], str]:
        """Build an order-preserving collector that joins each item's `str` value.

        Conversion occurs as items are stepped. Empty input finishes as an empty string.

        Args:
            delimiter: The string inserted between collected values.

        Returns:
            A mergeable reducer that joins its accumulated strings with `delimiter`.
        """
        from .reducer import LIST_LAWS, Reducer

        return Reducer(
            list,
            lambda values, value: _append(values, str(value)),
            lambda values: delimiter.join(values),
            merge=_extend,
            laws=LIST_LAWS,
        )

    @staticmethod
    def grouping_by(
        classifier: Selector,
        downstream: Collector[T, Any, R] | Callable[[Iterable[T]], R] | None = None,
    ) -> Collector[T, dict[Any, Any], dict[Any, R | list[T]]]:
        """Build a collector that maintains one downstream state per selected key.

        Only encountered keys appear in the result, in first-encounter order. Each item is
        stepped into its group even if the downstream collector has an early-completion
        predicate. A callable downstream is invoked at finish time with a materialized list
        for each group; omitting it collects group members into lists.

        Args:
            classifier: Selector producing a hashable group key for each item.
            downstream: Per-group collector or callable, defaulting to list collection.

        Returns:
            A collector that finishes every encountered group's state into a dictionary.
        """
        classify = compile_selector(classifier)
        reduction = _as_collector(Collectors.to_list() if downstream is None else downstream)

        def step(groups: dict[Any, Any], value: T) -> dict[Any, Any]:
            """Initialize the selected group on first use and step its state with `value`."""
            key = classify(value)
            try:
                state = groups[key]
            except KeyError:
                state = reduction.initializer()
            groups[key] = reduction.step(state, value)
            return groups

        def finish(groups: dict[Any, Any]) -> dict[Any, Any]:
            """Finish each encountered group state without creating absent groups."""
            return {key: reduction.finish(state) for key, state in groups.items()}

        return Collector(dict, step, finish)

    @staticmethod
    def partitioning_by(
        predicate: Selector,
        downstream: Collector[T, Any, R] | Callable[[Iterable[T]], R] | None = None,
    ) -> Collector[T, dict[bool, Any], dict[bool, R | list[T]]]:
        """Build a collector with independently reduced false and true partitions.

        Both Boolean keys are always present, even when one or both partitions receive no
        items. Each source item is stepped into one partition without consulting the
        downstream collector's early-completion predicate. Omitting `downstream` collects
        partition members into lists.

        Args:
            predicate: Selector whose truth value chooses the partition.
            downstream: Collector or callable applied independently to both partitions.

        Returns:
            A collector returning `{False: false_result, True: true_result}`.
        """
        test = compile_selector(predicate)
        reduction = _as_collector(Collectors.to_list() if downstream is None else downstream)

        def initialize() -> dict[bool, Any]:
            """Create separate downstream identity states for both Boolean keys."""
            return {False: reduction.initializer(), True: reduction.initializer()}

        def step(groups: dict[bool, Any], value: T) -> dict[bool, Any]:
            """Evaluate the predicate and step only the corresponding partition state."""
            key = bool(test(value))
            groups[key] = reduction.step(groups[key], value)
            return groups

        def finish(groups: dict[bool, Any]) -> dict[bool, Any]:
            """Finish false and true states in that deterministic key order."""
            return {key: reduction.finish(groups[key]) for key in (False, True)}

        return Collector(initialize, step, finish)

    @staticmethod
    def mapping(
        mapper: Selector,
        downstream: Collector[U, Any, R] | Callable[[Iterable[U]], R],
    ) -> Collector[T, Any, R]:
        """Build a collector that selects a new value before each downstream step.

        The adapter preserves downstream initialization, finishing, combining, and early
        completion, but does not carry downstream native metadata or reducer-law type.

        Args:
            mapper: Selector applied once to each consumed source item.
            downstream: Collector or callable receiving mapped values.

        Returns:
            A collector that maps each source item before returning the downstream result.
        """
        transform = compile_selector(mapper)
        reduction = _as_collector(downstream)
        return Collector(
            reduction.initializer,
            lambda state, value: reduction.step(state, transform(value)),
            reduction.finish,
            reduction.combine,
            reduction.done,
        )

    @staticmethod
    def filtering(
        predicate: Selector,
        downstream: Collector[T, Any, R] | Callable[[Iterable[T]], R],
    ) -> Collector[T, Any, R]:
        """Build a collector that steps downstream only for predicate matches.

        Rejected items leave downstream state unchanged. Initialization, finishing, combining,
        and early completion are preserved; native metadata and reducer-law type are not.

        Args:
            predicate: Selector evaluated for truth against each item.
            downstream: Collector or callable receiving matching items.

        Returns:
            A collector that filters source items before returning the downstream result.
        """
        test = compile_selector(predicate)
        reduction = _as_collector(downstream)

        def step(state: Any, value: T) -> Any:
            """Step downstream for a match, otherwise return its state unchanged."""
            return reduction.step(state, value) if test(value) else state

        return Collector(
            reduction.initializer,
            step,
            reduction.finish,
            reduction.combine,
            reduction.done,
        )

    @staticmethod
    def flat_mapping(
        mapper: Selector,
        downstream: Collector[U, Any, R] | Callable[[Iterable[U]], R],
    ) -> Collector[T, Any, R]:
        """Build a collector that steps downstream over each item's expanded iterable.

        Expansion stops as soon as the downstream state reports completion. A nested iterator
        exposing `close` is closed after exhaustion, early completion, or error. Downstream
        combining and early completion are preserved, while native metadata and reducer-law
        type are not.

        Args:
            mapper: Selector returning an iterable for each source item.
            downstream: Collector or callable receiving nested items.

        Returns:
            A collector that flattens source items before returning the downstream result.
        """
        expand = compile_selector(mapper)
        reduction = _as_collector(downstream)

        def step(state: Any, value: T) -> Any:
            """Step nested values until their iterator ends or downstream is complete."""
            iterator = iter(expand(value))
            with closing_iterators((iterator,)):
                while not reduction.done(state):
                    try:
                        nested = next(iterator)
                    except StopIteration:
                        break
                    state = reduction.step(state, nested)
            return state

        return Collector(
            reduction.initializer,
            step,
            reduction.finish,
            reduction.combine,
            reduction.done,
        )

    @staticmethod
    def collecting_and_then(
        downstream: Collector[T, Any, R] | Callable[[Iterable[T]], R],
        finisher: Callable[[R], U],
    ) -> Collector[T, Any, U]:
        """Build a collector that applies one more transformation after downstream finish.

        The downstream state machine, combiner, and early-completion behavior are preserved.
        `finisher` is called exactly once with the downstream public result.

        Args:
            downstream: Collector or callable that produces the intermediate result.
            finisher: Callable converting that intermediate result to the final value.

        Returns:
            An adapted collector returning `finisher(downstream_result)`.

        Raises:
            TypeError: If `finisher` is not callable.
        """
        if not callable(finisher):
            raise TypeError("finisher must be callable")
        reduction = _as_collector(downstream)
        return Collector(
            reduction.initializer,
            reduction.step,
            lambda state: finisher(reduction.finish(state)),
            reduction.combine,
            reduction.done,
        )

    @staticmethod
    def teeing(
        left: Collector[T, Any, R] | Callable[[Iterable[T]], R],
        right: Collector[T, Any, U] | Callable[[Iterable[T]], U],
        merger: Callable[[R, U], V],
    ) -> Collector[T, _TeeState, V]:
        """Build a collector that shares one source between two downstream collectors.

        Each input is offered only to downstream states that are not already complete. Source
        consumption stops when both are complete, then `merger` receives their finished
        results in left-to-right order. Callable downstreams buffer their respective items.

        Args:
            left: First collector or iterable-consuming callable.
            right: Second collector or iterable-consuming callable.
            merger: A callable that merges two downstream results.

        Returns:
            A short-circuiting collector returning the merger result.

        Raises:
            TypeError: If either downstream or `merger` is not callable as required.
        """
        if not callable(merger):
            raise TypeError("merger must be callable")
        left_reduction = _as_collector(left)
        right_reduction = _as_collector(right)

        def initialize() -> _TeeState:
            """Initialize independent left and right downstream states."""
            return _TeeState(left_reduction.initializer(), right_reduction.initializer())

        def step(state: _TeeState, value: T) -> _TeeState:
            """Offer `value` to each downstream state that is not complete."""
            if not left_reduction.done(state.left):
                state.left = left_reduction.step(state.left, value)
            if not right_reduction.done(state.right):
                state.right = right_reduction.step(state.right, value)
            return state

        def finish(state: _TeeState) -> V:
            """Finish both states and merge their public results."""
            return merger(
                left_reduction.finish(state.left),
                right_reduction.finish(state.right),
            )

        def done(state: _TeeState) -> bool:
            """Report completion only after both downstream collectors are complete."""
            return left_reduction.done(state.left) and right_reduction.done(state.right)

        return Collector(initialize, step, finish, done=done)

    @staticmethod
    def counting() -> Collector[T, int, int]:
        """Build a constant-state reducer that increments once per input item.

        Returns:
            A commutative reducer returning zero for empty input.
        """
        from .reducer import COUNT_LAWS, Reducer

        return Reducer(
            lambda: 0,
            lambda count, _value: count + 1,
            merge=lambda a, b: a + b,
            laws=COUNT_LAWS,
        )

    @staticmethod
    def summing(selector: Selector | None = None) -> Collector[T, Any, Any]:
        """Build a collector that adds selected values from a zero identity.

        Args:
            selector: Value selector; `None` adds each whole input item.

        Returns:
            A collector returning zero for empty input and supporting partial-state addition.
        """
        select = _selector(selector)
        return Collector(
            lambda: 0,
            lambda total, value: total + select(value),
            combine=lambda a, b: a + b,
        )

    @staticmethod
    def averaging(
        selector: Selector | None = None,
    ) -> Collector[T, tuple[Any, int], float]:
        """Build a collector that tracks selected-value sum and count for a mean.

        Empty input returns `0.0`. Nonempty input divides the accumulated sum by count during
        finishing, and partial `(sum, count)` states can be combined component-wise.

        Args:
            selector: Value selector; `None` averages each whole input item.

        Returns:
            A collector returning the arithmetic mean or `0.0` for no values.
        """
        select = _selector(selector)
        return Collector(
            lambda: (0, 0),
            lambda state, value: (state[0] + select(value), state[1] + 1),
            lambda state: state[0] / state[1] if state[1] else 0.0,
            lambda a, b: (a[0] + b[0], a[1] + b[1]),
        )

    @staticmethod
    def summarizing(
        selector: Selector | None = None,
    ) -> Collector[T, SummaryStatistics, SummaryStatistics]:
        """Build a collector returning mutable count, sum, extrema, and average state.

        Selected values update :class:`SummaryStatistics` directly. Empty input normalizes
        minimum and maximum from infinities to `0.0`; the sum, count, and derived average are
        already zero. The returned statistics object is the final mutable state.

        Args:
            selector: Value selector; `None` summarizes each whole input item.

        Returns:
            A one-pass collector returning a :class:`SummaryStatistics` instance.
        """
        select = _selector(selector)

        def step(statistics: SummaryStatistics, value: T) -> SummaryStatistics:
            """Select one value, update statistics in place, and preserve that state object."""
            statistics.accept(select(value))
            return statistics

        def finish(statistics: SummaryStatistics) -> SummaryStatistics:
            """Normalize empty extrema to zero and return the mutable statistics state."""
            if not statistics.count:
                statistics.min = statistics.max = 0.0
            return statistics

        return Collector(SummaryStatistics, step, finish)

    @staticmethod
    def first() -> Collector[T, Any, T | None]:
        """Build a collector that stops after and returns the first input item.

        Empty input returns `None`. A first item whose value is itself `None` is still treated
        as a completed result.

        Returns:
            A collector that consumes at most one item.
        """
        return Collector(
            lambda: _MISSING,
            lambda _state, value: value,
            lambda value: None if value is _MISSING else value,
            done=lambda value: value is not _MISSING,
        )

    @staticmethod
    def last() -> Collector[T, Any, T | None]:
        """Build a collector that consumes all input and returns its last item.

        Empty input returns `None`.

        Returns:
            A collector retaining only the most recently consumed item.
        """
        return Collector(
            lambda: _MISSING,
            lambda _state, value: value,
            lambda value: None if value is _MISSING else value,
        )

    @staticmethod
    def head(count: int) -> Collector[T, list[T], list[T]]:
        """Build a collector that stops after retaining the first `count` items.

        A zero count completes before pulling from the source.

        Args:
            count: Non-negative maximum number of items to retain.

        Returns:
            An early-stopping collector returning an encounter-ordered list.

        Raises:
            ValueError: If `count` is negative.
        """
        if count < 0:
            raise ValueError("head count must be non-negative")
        return Collector(list, _append, done=lambda values: len(values) >= count)

    @staticmethod
    def tail(count: int) -> Collector[T, deque[T], list[T]]:
        """Build a bounded-state collector for the final `count` input items.

        The source is fully consumed. Once the deque is full, each new item discards the
        oldest; a zero count retains nothing.

        Args:
            count: Non-negative maximum number of trailing items to retain.

        Returns:
            A collector finishing its bounded deque as an encounter-ordered list.

        Raises:
            ValueError: If `count` is negative.
        """
        if count < 0:
            raise ValueError("tail count must be non-negative")
        return Collector(
            lambda: deque(maxlen=count),
            lambda values, value: _deque_add(values, value),
            list,
        )

    @staticmethod
    def only() -> Collector[T, list[T], T | None]:
        """Build a collector that enforces zero-or-one input cardinality.

        Empty input returns `None`, one item is returned directly, and a second item completes
        collection early so finishing can raise without pulling a third.

        Returns:
            A collector retaining no more than two items.

        Raises:
            ValueError: If the input contains more than one item.
        """

        def finish(values: list[T]) -> T | None:
            """Return zero-or-one state and reject the two-item overflow marker."""
            if not values:
                return None
            if len(values) == 1:
                return values[0]
            raise ValueError("only() requires exactly one item")

        return Collector(list, _append, finish, done=lambda values: len(values) >= 2)

    @staticmethod
    def to_dict(
        key: Selector,
        value: Selector,
        *,
        on_duplicate: Literal["error", "first", "last"] = "error",
    ) -> Collector[T, dict[K, V], dict[K, V]]:
        """Build a dictionary collector with an explicit duplicate-key policy.

        On duplicates, `"error"` raises before selecting the duplicate value, `"first"`
        preserves the existing entry without selecting another value, and `"last"` replaces
        it. Selector errors and unhashable keys propagate.

        Args:
            key: Selector deriving each dictionary key.
            value: Selector deriving each dictionary value.
            on_duplicate: One of `"error"`, `"first"`, or `"last"`.

        Returns:
            A collector preserving key insertion order in its result dictionary.

        Raises:
            ValueError: If `on_duplicate` is not a supported policy.
            DuplicateKeyError: If a repeated key is encountered under `"error"`.
        """
        if on_duplicate not in {"error", "first", "last"}:
            raise ValueError("on_duplicate must be 'error', 'first', or 'last'")
        select_key = compile_selector(key)
        select_value = compile_selector(value)

        def step(result: dict[K, V], item: T) -> dict[K, V]:
            """Select one key and apply the configured duplicate policy before its value."""
            item_key = select_key(item)
            if item_key in result:
                if on_duplicate == "error":
                    raise DuplicateKeyError(f"Duplicate key: {item_key!r}")
                if on_duplicate == "first":
                    return result
            result[item_key] = select_value(item)
            return result

        return Collector(dict, step)

    @staticmethod
    def to_columns() -> Collector[Mapping[str, Any], _ColumnsState, dict[str, list[Any]]]:
        """Build a collector that transposes variably shaped mapping rows into columns.

        Columns retain field encounter order. Fields introduced by later rows are backfilled
        with `None`, and fields absent from later rows append `None`, so every column has one
        entry per input row.

        Returns:
            A collector returning a dictionary of equally sized column lists.
        """

        def initialize() -> _ColumnsState:
            """Create empty columns with a zero processed-row count."""
            return _ColumnsState({})

        def step(state: _ColumnsState, row: Mapping[str, Any]) -> _ColumnsState:
            """Backfill new fields, append this row's values, and increment row count."""
            for name in row:
                if name not in state.columns:
                    state.columns[name] = [None] * state.count
            for name, column in state.columns.items():
                column.append(row.get(name))
            state.count += 1
            return state

        return Collector(initialize, step, lambda state: state.columns)
