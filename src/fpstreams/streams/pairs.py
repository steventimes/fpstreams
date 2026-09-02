"""Key-value pipelines with per-key collection and aggregation."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping
from operator import itemgetter
from types import FunctionType
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar, cast

from ..collecting.aggregation import (
    Aggregator,
    finish_aggregations,
    initialize_aggregations,
    prepare_aggregations,
    step_aggregations,
)
from ..collecting.collector import Collector
from ..expressions.row import RowExpr
from ..planning._pair_stages import (
    PAIR_KEY_SELECTOR,
    PairFilterDescriptor,
    PairFlatMapDescriptor,
    PairMapDescriptor,
)
from ..planning.sync import Engine
from ..runtime.iterators import closing_iterators

if TYPE_CHECKING:
    from .flow import Flow

K = TypeVar("K")
V = TypeVar("V")
R = TypeVar("R")
U = TypeVar("U")

_PAIR_KEY = PAIR_KEY_SELECTOR
_PAIR_VALUE = itemgetter(1)
_PAIR_INVERT = itemgetter(1, 0)
_CANONICAL_FINISH_AGGREGATIONS = finish_aggregations
_CANONICAL_INITIALIZE_AGGREGATIONS = initialize_aggregations
_CANONICAL_STEP_AGGREGATIONS = step_aggregations
_CANONICAL_FINISH_AGGREGATIONS_CODE = finish_aggregations.__code__
_CANONICAL_INITIALIZE_AGGREGATIONS_CODE = initialize_aggregations.__code__
_CANONICAL_STEP_AGGREGATIONS_CODE = step_aggregations.__code__
_CANONICAL_FINISH_COLLECTORS = cast(
    FunctionType, finish_aggregations.__globals__["finish_collectors"]
)
_CANONICAL_INITIALIZE_COLLECTORS = cast(
    FunctionType, initialize_aggregations.__globals__["initialize_collectors"]
)
_CANONICAL_STEP_COLLECTORS = cast(FunctionType, step_aggregations.__globals__["step_collectors"])
_CANONICAL_FINISH_COLLECTORS_CODE = _CANONICAL_FINISH_COLLECTORS.__code__
_CANONICAL_INITIALIZE_COLLECTORS_CODE = _CANONICAL_INITIALIZE_COLLECTORS.__code__
_CANONICAL_STEP_COLLECTORS_CODE = _CANONICAL_STEP_COLLECTORS.__code__


def _aggregation_helpers_are_live() -> bool:
    """Keep replaced public aggregation helpers authoritative over compact lanes."""
    namespace = globals()
    return bool(
        namespace.get("initialize_aggregations") is _CANONICAL_INITIALIZE_AGGREGATIONS
        and type(_CANONICAL_INITIALIZE_AGGREGATIONS) is FunctionType
        and _CANONICAL_INITIALIZE_AGGREGATIONS.__code__ is _CANONICAL_INITIALIZE_AGGREGATIONS_CODE
        and _CANONICAL_INITIALIZE_AGGREGATIONS.__globals__.get("initialize_collectors")
        is _CANONICAL_INITIALIZE_COLLECTORS
        and _CANONICAL_INITIALIZE_COLLECTORS.__code__ is _CANONICAL_INITIALIZE_COLLECTORS_CODE
        and namespace.get("step_aggregations") is _CANONICAL_STEP_AGGREGATIONS
        and type(_CANONICAL_STEP_AGGREGATIONS) is FunctionType
        and _CANONICAL_STEP_AGGREGATIONS.__code__ is _CANONICAL_STEP_AGGREGATIONS_CODE
        and _CANONICAL_STEP_AGGREGATIONS.__globals__.get("step_collectors")
        is _CANONICAL_STEP_COLLECTORS
        and _CANONICAL_STEP_COLLECTORS.__code__ is _CANONICAL_STEP_COLLECTORS_CODE
        and namespace.get("finish_aggregations") is _CANONICAL_FINISH_AGGREGATIONS
        and type(_CANONICAL_FINISH_AGGREGATIONS) is FunctionType
        and _CANONICAL_FINISH_AGGREGATIONS.__code__ is _CANONICAL_FINISH_AGGREGATIONS_CODE
        and _CANONICAL_FINISH_AGGREGATIONS.__globals__.get("finish_collectors")
        is _CANONICAL_FINISH_COLLECTORS
        and _CANONICAL_FINISH_COLLECTORS.__code__ is _CANONICAL_FINISH_COLLECTORS_CODE
    )


_CANONICAL_AGGREGATION_HELPERS_ARE_LIVE = _aggregation_helpers_are_live
_CANONICAL_AGGREGATION_HELPERS_ARE_LIVE_CODE = _aggregation_helpers_are_live.__code__


class Pairs(Generic[K, V]):
    """A lazy key-value view backed by a synchronous Flow."""

    __slots__ = ("_flow",)

    def __init__(self, source: Flow[tuple[K, V]]) -> None:
        """Wrap a flow whose items are interpreted as key/value pairs."""
        self._flow = source

    def __iter__(self) -> Iterator[tuple[K, V]]:
        """Execute the underlying flow and yield its key/value pairs lazily."""
        return iter(self._flow)

    def _consume(self, consumer: Callable[[Iterator[tuple[K, V]]], U]) -> U:
        """Consume canonical pairs directly while preserving custom public iteration."""
        if self._can_consume_flow_directly():
            return self._flow._consume(consumer)
        iterator = iter(self)
        with closing_iterators((iterator,)):
            return consumer(iterator)

    def _can_consume_flow_directly(self) -> bool:
        """Return whether neither public iteration layer has been customized."""
        from .flow import Flow

        return type(self) is Pairs and type(self._flow) is Flow

    def to_flow(self) -> Flow[tuple[K, V]]:
        """Return the underlying flow of key/value tuples.

        Returns:
            The same underlying `Flow`; no copy or additional operation is created.
        """
        return self._flow

    items = to_flow

    def with_engine(self, engine: Engine) -> Pairs[K, V]:
        """Return an equivalent pair view requesting one Flow execution engine.

        Args:
            engine: ``"auto"``, ``"python"``, or ``"native"``.

        Returns:
            Lazy Pairs backed by the engine-adjusted Flow.
        """
        return Pairs(self._flow.with_engine(engine))

    def keys(self) -> Flow[K]:
        """Select only keys as a Flow.

        Returns:
            A flow containing the first element of every pair.
        """
        return self._flow.map(_PAIR_KEY)

    def values(self) -> Flow[V]:
        """Select only values as a Flow.

        Returns:
            A flow containing the second element of every pair.
        """
        return self._flow.map(_PAIR_VALUE)

    def map_pairs(self, function: Callable[[K, V], tuple[R, U]]) -> Pairs[R, U]:
        """Transform each key/value pair into a new pair.

        Args:
            function: Called as `function(key, value)` and returns the replacement pair.

        Returns:
            A lazy pair pipeline containing each returned `(new_key, new_value)`.
        """
        return Pairs(self._flow.map(PairMapDescriptor("pair", function)))

    def flat_map_pairs(self, function: Callable[[K, V], Iterable[tuple[R, U]]]) -> Pairs[R, U]:
        """Transform each pair into zero or more pairs.

        Args:
            function: Called as `function(key, value)` and returns an iterable of replacement
                pairs.

        Returns:
            A lazy pair pipeline that emits every returned iterable in source order.
        """
        return Pairs(self._flow.flat_map(PairFlatMapDescriptor(function)))

    def filter_pairs(self, predicate: Callable[[K, V], bool] | RowExpr) -> Pairs[K, V]:
        """Keep pairs for which predicate returns true.

        Args:
            predicate: A callable invoked as `predicate(key, value)`, or a RowExpr evaluated
                against the complete pair so `col(0)` selects the key and `col(1)` the value.

        Returns:
            A lazy pair pipeline containing only pairs with truthy predicate results.
        """
        target: Literal["row", "pair"] = "row" if isinstance(predicate, RowExpr) else "pair"
        return Pairs(self._flow.filter(PairFilterDescriptor(target, predicate)))

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
        return Pairs(self._flow.sort_by(_PAIR_KEY, reverse=reverse))

    def sort_by_value(self, *, reverse: bool = False) -> Pairs[K, V]:
        """Sort pairs by value.

        Args:
            reverse: Sort values descending when true.

        Returns:
            A lazy pair pipeline globally ordered by value.
        """
        return Pairs(self._flow.sort_by(_PAIR_VALUE, reverse=reverse))

    def unique_keys(self) -> Pairs[K, V]:
        """Keep the first pair for each key.

        Returns:
            A lazy pair pipeline containing the earliest pair for each distinct key.
        """
        return Pairs(self._flow.unique_by(_PAIR_KEY))

    def map_keys(self, function: Callable[[K], R]) -> Pairs[R, V]:
        """Transform keys while leaving values unchanged.

        Args:
            function: Maps each key to its replacement key.

        Returns:
            A lazy pair pipeline of `(function(key), value)` pairs.
        """
        return Pairs(self._flow.map(PairMapDescriptor("key", function)))

    def map_values(self, function: Callable[[V], R]) -> Pairs[K, R]:
        """Transform values while leaving keys unchanged.

        Args:
            function: Maps each value to its replacement value.

        Returns:
            A lazy pair pipeline of `(key, function(value))` pairs.
        """
        return Pairs(self._flow.map(PairMapDescriptor("value", function)))

    def filter_keys(self, predicate: Callable[[K], bool]) -> Pairs[K, V]:
        """Keep pairs whose key satisfies predicate.

        Args:
            predicate: Called with each key to decide whether its pair is retained.

        Returns:
            A lazy pair pipeline containing pairs whose keys satisfy `predicate`.
        """
        return Pairs(self._flow.filter(PairFilterDescriptor("key", predicate)))

    def filter_values(self, predicate: Callable[[V], bool]) -> Pairs[K, V]:
        """Keep pairs whose value satisfies predicate.

        Args:
            predicate: Called with each value to decide whether its pair is retained.

        Returns:
            A lazy pair pipeline containing pairs whose values satisfy `predicate`.
        """
        return Pairs(self._flow.filter(PairFilterDescriptor("value", predicate)))

    def invert(self) -> Pairs[V, K]:
        """Swap the key and value in every pair.

        Returns:
            A lazy pair pipeline containing `(value, key)` for each source pair.
        """
        return Pairs(self._flow.map(_PAIR_INVERT))

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
        from ..execution._pair_dict import PairDictConsumer

        return cast(dict[K, V], self._consume(PairDictConsumer(on_duplicate)))

    def group_values(self) -> dict[K, list[V]]:
        """Collect all values for each key in encounter order.

        Returns:
            Lists of values keyed by each hashable key, preserving key and value encounter order.
        """

        def consume(iterator: Iterator[tuple[K, V]]) -> dict[K, list[V]]:
            result: dict[K, list[V]] = {}
            for key, value in iterator:
                result.setdefault(key, []).append(value)
            return result

        return self._consume(consume)

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

            def consume_streaming(iterator: Iterator[tuple[K, V]]) -> dict[K, Any]:
                states: dict[K, Any] = {}
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
                return states

            states = self._consume(consume_streaming)
            return {key: collector.finish(state) for key, state in states.items()}

        if not callable(collector):
            raise TypeError("collector must be a Collector or callable")

        def consume_groups(iterator: Iterator[tuple[K, V]]) -> dict[K, list[V]]:
            groups: dict[K, list[V]] = {}
            for key, value in iterator:
                try:
                    groups.setdefault(key, []).append(value)
                except TypeError:
                    raise TypeError("pair keys must be hashable") from None
            return groups

        groups = self._consume(consume_groups)
        return {key: collector(values) for key, values in groups.items()}

    def aggregate_values(self, **aggregations: Aggregator) -> dict[K, dict[str, Any]]:
        """Run named Aggregators independently for every key.

        Args:
            **aggregations: Result names mapped to aggregators maintained independently per key.

        Returns:
            Each hashable key mapped to a dictionary of its named finished aggregation values.
        """
        items = prepare_aggregations(aggregations)
        canonical_helpers_live = (
            _aggregation_helpers_are_live is _CANONICAL_AGGREGATION_HELPERS_ARE_LIVE
            and _CANONICAL_AGGREGATION_HELPERS_ARE_LIVE.__code__
            is _CANONICAL_AGGREGATION_HELPERS_ARE_LIVE_CODE
            and _CANONICAL_AGGREGATION_HELPERS_ARE_LIVE()
        )
        direct = self._can_consume_flow_directly()
        from ..execution._pair_aggregate import compile_closed_pair_aggregations

        closed = (
            compile_closed_pair_aggregations(items) if direct and canonical_helpers_live else None
        )
        if closed is not None and closed.is_live():
            from ..execution.relational import try_native_pair_aggregations

            native = try_native_pair_aggregations(
                self._flow._logical_plan,
                items,
                closed.kinds,
            )
            if native is not None:
                return cast(dict[K, dict[str, Any]], native)

        def consume(iterator: Iterator[tuple[K, V]]) -> dict[K, dict[str, Any]]:
            states_by_key: dict[K, dict[str, Any]] = {}
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
            return states_by_key

        states_by_key = self._consume(consume)
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
