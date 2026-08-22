"""Key-value pipelines with per-key collection and aggregation."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar, cast

from ..collecting.aggregation import (
    Aggregator,
    finish_aggregations,
    initialize_aggregations,
    prepare_aggregations,
    step_aggregations,
)
from ..collecting.collector import Collector
from ..errors import DuplicateKeyError

if TYPE_CHECKING:
    from .flow import Flow

K = TypeVar("K")
V = TypeVar("V")
R = TypeVar("R")
U = TypeVar("U")


class Pairs(Generic[K, V]):
    """A lazy key-value view backed by a synchronous Flow."""

    __slots__ = ("_flow",)

    def __init__(self, source: Flow[tuple[K, V]]) -> None:
        """Wrap a flow whose items are interpreted as key/value pairs."""
        self._flow = source

    def __iter__(self) -> Iterator[tuple[K, V]]:
        """Execute the underlying flow and yield its key/value pairs lazily."""
        return iter(self._flow)

    def to_flow(self) -> Flow[tuple[K, V]]:
        """Return the underlying flow of key/value tuples.

        Returns:
            The same underlying `Flow`; no copy or additional operation is created.
        """
        return self._flow

    items = to_flow

    def keys(self) -> Flow[K]:
        """Select only keys as a Flow.

        Returns:
            A flow containing the first element of every pair.
        """
        return self._flow.map(lambda pair: pair[0])

    def values(self) -> Flow[V]:
        """Select only values as a Flow.

        Returns:
            A flow containing the second element of every pair.
        """
        return self._flow.map(lambda pair: pair[1])

    def map_pairs(self, function: Callable[[K, V], tuple[R, U]]) -> Pairs[R, U]:
        """Transform each key/value pair into a new pair.

        Args:
            function: Called as `function(key, value)` and returns the replacement pair.

        Returns:
            A lazy pair pipeline containing each returned `(new_key, new_value)`.
        """
        return Pairs(self._flow.map(lambda pair: function(pair[0], pair[1])))

    def flat_map_pairs(self, function: Callable[[K, V], Iterable[tuple[R, U]]]) -> Pairs[R, U]:
        """Transform each pair into zero or more pairs.

        Args:
            function: Called as `function(key, value)` and returns an iterable of replacement
                pairs.

        Returns:
            A lazy pair pipeline that emits every returned iterable in source order.
        """
        return Pairs(self._flow.flat_map(lambda pair: function(pair[0], pair[1])))

    def filter_pairs(self, predicate: Callable[[K, V], bool]) -> Pairs[K, V]:
        """Keep pairs for which predicate returns true.

        Args:
            predicate: Called as `predicate(key, value)` for each pair.

        Returns:
            A lazy pair pipeline containing only pairs with truthy predicate results.
        """
        return Pairs(self._flow.filter(lambda pair: predicate(pair[0], pair[1])))

    def tap(self, action: Callable[[K, V], object]) -> Pairs[K, V]:
        """Run a side effect for each pair while passing the pair through.

        Args:
            action: Called as `action(key, value)` before the original pair is emitted.

        Returns:
            A lazy pair pipeline that passes every original pair through unchanged.
        """

        def invoke(pair: tuple[K, V]) -> None:
            """Expand one pair into the two arguments expected by the user action."""
            action(pair[0], pair[1])

        return Pairs(self._flow.tap(invoke))

    def take(self, count: int) -> Pairs[K, V]:
        """Emit at most count pairs.

        Args:
            count: Maximum number of leading pairs to emit.

        Returns:
            A lazy pair pipeline containing only the first `count` pairs.
        """
        return Pairs(self._flow.take(count))

    def drop(self, count: int) -> Pairs[K, V]:
        """Skip count pairs before yielding the remainder.

        Args:
            count: Number of leading pairs to consume without emitting.

        Returns:
            A lazy pair pipeline containing every pair after the first `count`.
        """
        return Pairs(self._flow.drop(count))

    def sort_by_key(self, *, reverse: bool = False) -> Pairs[K, V]:
        """Sort pairs by key.

        Args:
            reverse: Sort keys descending when true.

        Returns:
            A lazy pair pipeline globally ordered by key.
        """
        return Pairs(self._flow.sort_by(lambda pair: pair[0], reverse=reverse))

    def sort_by_value(self, *, reverse: bool = False) -> Pairs[K, V]:
        """Sort pairs by value.

        Args:
            reverse: Sort values descending when true.

        Returns:
            A lazy pair pipeline globally ordered by value.
        """
        return Pairs(self._flow.sort_by(lambda pair: pair[1], reverse=reverse))

    def unique_keys(self) -> Pairs[K, V]:
        """Keep the first pair for each key.

        Returns:
            A lazy pair pipeline containing the earliest pair for each distinct key.
        """
        return Pairs(self._flow.unique_by(lambda pair: pair[0]))

    def map_keys(self, function: Callable[[K], R]) -> Pairs[R, V]:
        """Transform keys while leaving values unchanged.

        Args:
            function: Maps each key to its replacement key.

        Returns:
            A lazy pair pipeline of `(function(key), value)` pairs.
        """
        return Pairs(self._flow.map(lambda pair: (function(pair[0]), pair[1])))

    def map_values(self, function: Callable[[V], R]) -> Pairs[K, R]:
        """Transform values while leaving keys unchanged.

        Args:
            function: Maps each value to its replacement value.

        Returns:
            A lazy pair pipeline of `(key, function(value))` pairs.
        """
        return Pairs(self._flow.map(lambda pair: (pair[0], function(pair[1]))))

    def filter_keys(self, predicate: Callable[[K], bool]) -> Pairs[K, V]:
        """Keep pairs whose key satisfies predicate.

        Args:
            predicate: Called with each key to decide whether its pair is retained.

        Returns:
            A lazy pair pipeline containing pairs whose keys satisfy `predicate`.
        """
        return Pairs(self._flow.filter(lambda pair: predicate(pair[0])))

    def filter_values(self, predicate: Callable[[V], bool]) -> Pairs[K, V]:
        """Keep pairs whose value satisfies predicate.

        Args:
            predicate: Called with each value to decide whether its pair is retained.

        Returns:
            A lazy pair pipeline containing pairs whose values satisfy `predicate`.
        """
        return Pairs(self._flow.filter(lambda pair: predicate(pair[1])))

    def invert(self) -> Pairs[V, K]:
        """Swap the key and value in every pair.

        Returns:
            A lazy pair pipeline containing `(value, key)` for each source pair.
        """
        return Pairs(self._flow.map(lambda pair: (pair[1], pair[0])))

    def to_dict(
        self,
        *,
        on_duplicate: Literal["error", "first", "last"] = "error",
    ) -> dict[K, V]:
        """Collect pairs into a dictionary using the duplicate-key policy.

        Args:
            on_duplicate: `error` raises, `first` keeps the earliest value, and `last` keeps the
                latest value for a repeated key.

        Returns:
            One selected value for each hashable key, in first-key encounter order.

        Raises:
            DuplicateKeyError: If a key repeats under the `error` policy.
            ValueError: If `on_duplicate` is not `error`, `first`, or `last`.
        """
        if on_duplicate not in {"error", "first", "last"}:
            raise ValueError("on_duplicate must be 'error', 'first', or 'last'")
        result: dict[K, V] = {}
        for key, value in self:
            if key in result:
                if on_duplicate == "error":
                    raise DuplicateKeyError(f"Duplicate key: {key!r}")
                if on_duplicate == "first":
                    continue
            result[key] = value
        return result

    def group_values(self) -> dict[K, list[V]]:
        """Collect all values for each key in encounter order.

        Returns:
            Lists of values keyed by each hashable key, preserving key and value encounter order.
        """
        result: dict[K, list[V]] = {}
        for key, value in self:
            result.setdefault(key, []).append(value)
        return result

    def collect_values(
        self,
        collector: Collector[V, Any, R] | Callable[[Iterable[V]], R],
    ) -> dict[K, R]:
        """Run one Collector or callable independently for every key.

        Args:
            collector: A streaming `Collector` applied independently per key, or a callable that
                receives that key's complete value list.

        Returns:
            Each hashable key mapped to its independently finished collector result.
        """
        if isinstance(collector, Collector):
            states: dict[K, Any] = {}
            iterator = iter(self)
            try:
                for key, value in iterator:
                    try:
                        state = states[key]
                    except KeyError:
                        state = collector.initializer()
                    except TypeError:
                        raise TypeError("pair keys must be hashable") from None
                    if not collector.done(state):
                        state = collector.step(state, value)
                    try:
                        states[key] = state
                    except TypeError:
                        raise TypeError("pair keys must be hashable") from None
            finally:
                close = getattr(iterator, "close", None)
                if callable(close):
                    close()
            return {key: collector.finish(state) for key, state in states.items()}

        if not callable(collector):
            raise TypeError("collector must be a Collector or callable")
        groups: dict[K, list[V]] = {}
        iterator = iter(self)
        try:
            for key, value in iterator:
                try:
                    groups.setdefault(key, []).append(value)
                except TypeError:
                    raise TypeError("pair keys must be hashable") from None
        finally:
            close = getattr(iterator, "close", None)
            if callable(close):
                close()
        return {key: collector(values) for key, values in groups.items()}

    def aggregate_values(self, **aggregations: Aggregator) -> dict[K, dict[str, Any]]:
        """Run named Aggregators independently for every key.

        Args:
            **aggregations: Result names mapped to aggregators maintained independently per key.

        Returns:
            Each hashable key mapped to a dictionary of its named finished aggregation values.
        """
        items = prepare_aggregations(aggregations)
        from ..execution.relational import try_native_pair_sum

        native = try_native_pair_sum(self._flow._logical_plan, items)
        if native is not None:
            return cast(dict[K, dict[str, Any]], native)
        states_by_key: dict[K, dict[str, Any]] = {}
        iterator = iter(self)
        try:
            for key, value in iterator:
                try:
                    states = states_by_key[key]
                except KeyError:
                    states = initialize_aggregations(items)
                except TypeError:
                    raise TypeError("pair keys must be hashable") from None
                step_aggregations(states, items, value)
                try:
                    states_by_key[key] = states
                except TypeError:
                    raise TypeError("pair keys must be hashable") from None
        finally:
            close = getattr(iterator, "close", None)
            if callable(close):
                close()
        return {key: finish_aggregations(states, items) for key, states in states_by_key.items()}


def pairs(source: Mapping[K, V] | Iterable[tuple[K, V]]) -> Pairs[K, V]:
    """Create a lazy Pairs pipeline from a mapping or key/value iterable.

    Args:
        source: A mapping, whose `items()` are used, or a synchronous iterable of two-tuples.

    Returns:
        A lazy pair pipeline over the mapping entries or supplied tuples.
    """
    # Flow exposes ``Flow.pairs`` in the other direction. Delaying this runtime
    # import avoids partially initialized modules while preserving precise types.
    from .flow import Flow

    values = source.items() if isinstance(source, Mapping) else source
    return Pairs(Flow.from_iterable(cast(Iterable[tuple[K, V]], values)))
