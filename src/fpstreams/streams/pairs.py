"""Key-value pipelines with per-key collection and aggregation."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping
from typing import Any, Generic, Literal, TypeVar, cast

from ..collecting.aggregation import (
    Aggregator,
    finish_aggregations,
    initialize_aggregations,
    prepare_aggregations,
    step_aggregations,
)
from ..collecting.collector import Collector
from ..errors import DuplicateKeyError
from .flow import Flow

K = TypeVar("K")
V = TypeVar("V")
R = TypeVar("R")
U = TypeVar("U")


class Pairs(Generic[K, V]):
    """A lazy key-value view backed by a synchronous Flow."""

    __slots__ = ("_flow",)

    def __init__(self, source: Flow[tuple[K, V]]) -> None:
        self._flow = source

    def __iter__(self) -> Iterator[tuple[K, V]]:
        return iter(self._flow)

    def to_flow(self) -> Flow[tuple[K, V]]:
        """Return the underlying flow of key/value tuples.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        return self._flow

    items = to_flow

    def keys(self) -> Flow[K]:
        """Select only keys as a Flow.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        return self._flow.map(lambda pair: pair[0])

    def values(self) -> Flow[V]:
        """Select only values as a Flow.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        return self._flow.map(lambda pair: pair[1])

    def map_pairs(self, function: Callable[[K, V], tuple[R, U]]) -> Pairs[R, U]:
        """Transform each key/value pair into a new pair.

        Args:
            function: The callable applied by this operation.

        Returns:
            A new lazy `Pairs` pipeline representing this operation.
        """
        return Pairs(self._flow.map(lambda pair: function(pair[0], pair[1])))

    def flat_map_pairs(self, function: Callable[[K, V], Iterable[tuple[R, U]]]) -> Pairs[R, U]:
        """Transform each pair into zero or more pairs.

        Args:
            function: The callable applied by this operation.

        Returns:
            A new lazy `Pairs` pipeline representing this operation.
        """
        return Pairs(self._flow.flat_map(lambda pair: function(pair[0], pair[1])))

    def filter_pairs(self, predicate: Callable[[K, V], bool]) -> Pairs[K, V]:
        """Keep pairs for which predicate returns true.

        Args:
            predicate: A callable that decides whether an item matches.

        Returns:
            A new lazy `Pairs` pipeline representing this operation.
        """
        return Pairs(self._flow.filter(lambda pair: predicate(pair[0], pair[1])))

    def tap(self, action: Callable[[K, V], object]) -> Pairs[K, V]:
        """Run a side effect for each pair while passing the pair through.

        Args:
            action: The side-effecting callable invoked for each matching item.

        Returns:
            A new lazy `Pairs` pipeline representing this operation.
        """

        def invoke(pair: tuple[K, V]) -> None:
            action(pair[0], pair[1])

        return Pairs(self._flow.tap(invoke))

    def take(self, count: int) -> Pairs[K, V]:
        """Emit at most count pairs.

        Args:
            count: The requested number of items.

        Returns:
            A new lazy `Pairs` pipeline representing this operation.
        """
        return Pairs(self._flow.take(count))

    def drop(self, count: int) -> Pairs[K, V]:
        """Skip count pairs before yielding the remainder.

        Args:
            count: The requested number of items.

        Returns:
            A new lazy `Pairs` pipeline representing this operation.
        """
        return Pairs(self._flow.drop(count))

    def sort_by_key(self, *, reverse: bool = False) -> Pairs[K, V]:
        """Sort pairs by key.

        Args:
            reverse: If true, produce values in descending order.

        Returns:
            A new lazy `Pairs` pipeline representing this operation.
        """
        return Pairs(self._flow.sort_by(lambda pair: pair[0], reverse=reverse))

    def sort_by_value(self, *, reverse: bool = False) -> Pairs[K, V]:
        """Sort pairs by value.

        Args:
            reverse: If true, produce values in descending order.

        Returns:
            A new lazy `Pairs` pipeline representing this operation.
        """
        return Pairs(self._flow.sort_by(lambda pair: pair[1], reverse=reverse))

    def unique_keys(self) -> Pairs[K, V]:
        """Keep the first pair for each key.

        Returns:
            A new lazy `Pairs` pipeline representing this operation.
        """
        return Pairs(self._flow.unique_by(lambda pair: pair[0]))

    def map_keys(self, function: Callable[[K], R]) -> Pairs[R, V]:
        """Transform keys while leaving values unchanged.

        Args:
            function: The callable applied by this operation.

        Returns:
            A new lazy `Pairs` pipeline representing this operation.
        """
        return Pairs(self._flow.map(lambda pair: (function(pair[0]), pair[1])))

    def map_values(self, function: Callable[[V], R]) -> Pairs[K, R]:
        """Transform values while leaving keys unchanged.

        Args:
            function: The callable applied by this operation.

        Returns:
            A new lazy `Pairs` pipeline representing this operation.
        """
        return Pairs(self._flow.map(lambda pair: (pair[0], function(pair[1]))))

    def filter_keys(self, predicate: Callable[[K], bool]) -> Pairs[K, V]:
        """Keep pairs whose key satisfies predicate.

        Args:
            predicate: A callable that decides whether an item matches.

        Returns:
            A new lazy `Pairs` pipeline representing this operation.
        """
        return Pairs(self._flow.filter(lambda pair: predicate(pair[0])))

    def filter_values(self, predicate: Callable[[V], bool]) -> Pairs[K, V]:
        """Keep pairs whose value satisfies predicate.

        Args:
            predicate: A callable that decides whether an item matches.

        Returns:
            A new lazy `Pairs` pipeline representing this operation.
        """
        return Pairs(self._flow.filter(lambda pair: predicate(pair[1])))

    def invert(self) -> Pairs[V, K]:
        """Swap the key and value in every pair.

        Returns:
            A new lazy `Pairs` pipeline representing this operation.
        """
        return Pairs(self._flow.map(lambda pair: (pair[1], pair[0])))

    def to_dict(
        self,
        *,
        on_duplicate: Literal["error", "first", "last"] = "error",
    ) -> dict[K, V]:
        """Collect pairs into a dictionary using the duplicate-key policy.

        Args:
            on_duplicate: The policy used when the same key appears more than once.

        Returns:
            A dictionary containing the computed keys and values.
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
            A dictionary containing the computed keys and values.
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
            collector: The collector used to reduce input items.

        Returns:
            A dictionary containing the computed keys and values.
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
            **aggregations: Named aggregators evaluated during the same traversal.

        Returns:
            A dictionary containing the computed keys and values.
        """
        items = prepare_aggregations(aggregations)
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
        source: The iterable, async iterable, or data source to read lazily.

    Returns:
        A new lazy `Pairs` pipeline representing this operation.
    """
    values = source.items() if isinstance(source, Mapping) else source
    return Pairs(Flow.from_iterable(cast(Iterable[tuple[K, V]], values)))
