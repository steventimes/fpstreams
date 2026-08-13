"""Composable state machines for streaming collection."""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Generic, Literal, TypeVar, cast

from ..errors import DuplicateKeyError
from ..expressions.selectors import Selector, compile_selector

T = TypeVar("T")
K = TypeVar("K")
R = TypeVar("R")
S = TypeVar("S")
U = TypeVar("U")
V = TypeVar("V")

_MISSING = object()


def _identity(value: Any) -> Any:
    return value


def _never_done(_state: Any) -> bool:
    return False


@dataclass(frozen=True, slots=True)
class Collector(Generic[T, S, R]):
    """An immutable, composable description of a streaming reduction."""

    initializer: Callable[[], S]
    step: Callable[[S, T], S]
    finish: Callable[[S], R] = _identity
    combine: Callable[[S, S], S] | None = None
    done: Callable[[S], bool] = _never_done
    native: Any | None = None

    def __post_init__(self) -> None:
        for name in ("initializer", "step", "finish", "done"):
            if not callable(getattr(self, name)):
                raise TypeError(f"Collector {name} must be callable")
        if self.combine is not None and not callable(self.combine):
            raise TypeError("Collector combine must be callable or None")

    def __call__(self, values: Iterable[T]) -> R:
        """Execute this collector over an iterable.

        Args:
            values: The input values consumed by the collector.

        Returns:
            The collector's finished result.
        """
        return cast(R, run_collectors(values, (("result", self),))["result"])


CollectorItems = tuple[tuple[str, Collector[Any, Any, Any]], ...]


def prepare_collectors(
    collectors: Mapping[str, Collector[Any, Any, Any]],
) -> CollectorItems:
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
    return {name: collector.initializer() for name, collector in items}


def collectors_done(states: Mapping[str, Any], items: CollectorItems) -> bool:
    return all(collector.done(states[name]) for name, collector in items)


def step_collectors(states: dict[str, Any], items: CollectorItems, value: Any) -> None:
    for name, collector in items:
        state = states[name]
        if collector.done is _never_done or not collector.done(state):
            states[name] = collector.step(state, value)


def finish_collectors(states: Mapping[str, Any], items: CollectorItems) -> dict[str, Any]:
    return {name: collector.finish(states[name]) for name, collector in items}


def run_collectors(values: Iterable[Any], items: CollectorItems) -> dict[str, Any]:
    states = initialize_collectors(items)
    iterator = iter(values)
    try:
        # Skip completion checks in the common case; they are observable hot-path overhead.
        if all(collector.done is _never_done for _name, collector in items):
            for value in iterator:
                step_collectors(states, items, value)
        else:
            while not collectors_done(states, items):
                try:
                    value = next(iterator)
                except StopIteration:
                    break
                step_collectors(states, items, value)
    finally:
        close = getattr(iterator, "close", None)
        if callable(close):
            close()
    return finish_collectors(states, items)


def _append(values: list[Any], value: Any) -> list[Any]:
    values.append(value)
    return values


def _add(values: set[Any], value: Any) -> set[Any]:
    values.add(value)
    return values


def _extend(left: list[Any], right: list[Any]) -> list[Any]:
    left.extend(right)
    return left


def _update(left: set[Any], right: set[Any]) -> set[Any]:
    left.update(right)
    return left


def _deque_add(values: deque[Any], value: Any) -> deque[Any]:
    values.append(value)
    return values


def _selector(selector: Selector | None) -> Callable[[Any], Any]:
    return _identity if selector is None else compile_selector(selector)


def _as_collector(
    downstream: Collector[Any, Any, Any] | Callable[[Iterable[Any]], Any],
) -> Collector[Any, Any, Any]:
    if isinstance(downstream, Collector):
        return downstream
    if not callable(downstream):
        raise TypeError("downstream must be a Collector or callable")
    return Collector(list, _append, downstream, _extend)


@dataclass(slots=True)
class SummaryStatistics:
    """Mutable one-pass count, sum, minimum, maximum, and average."""

    count: int = 0
    sum: float = 0.0
    min: float = float("inf")
    max: float = float("-inf")

    @property
    def average(self) -> float:
        """Return the arithmetic mean, or 0.0 when no values were accepted.

        Returns:
            The computed floating-point value.
        """
        return self.sum / self.count if self.count else 0.0

    def accept(self, value: float) -> None:
        """Add one numeric value to the running statistics.

        Args:
            value: The value consumed by this operation.
        """
        self.count += 1
        self.sum += value
        self.min = value if value < self.min else self.min
        self.max = value if value > self.max else self.max


@dataclass(slots=True)
class _ColumnsState:
    columns: dict[str, list[Any]]
    count: int = 0


@dataclass(slots=True)
class _TeeState:
    left: Any
    right: Any


def _collect_columns(values: Iterable[Mapping[str, Any]]) -> dict[str, list[Any]]:
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
    """Factories for composable streaming collectors."""

    @staticmethod
    def to_list() -> Collector[T, list[T], list[T]]:
        """Collect all items into a list.

        Returns:
            A reusable `Collector` implementing the described reduction.
        """
        return Collector(list, _append, combine=_extend)

    @staticmethod
    def to_set() -> Collector[T, set[T], set[T]]:
        """Collect distinct hashable items into a set.

        Returns:
            A reusable `Collector` implementing the described reduction.
        """
        return Collector(set, _add, combine=_update)

    @staticmethod
    def to_tuple() -> Collector[T, list[T], tuple[T, ...]]:
        """Collect all items into a tuple.

        Returns:
            A reusable `Collector` implementing the described reduction.
        """
        return Collector(list, _append, tuple, _extend)

    @staticmethod
    def joining(delimiter: str = "") -> Collector[Any, list[str], str]:
        """Convert items to strings and join them with delimiter.

        Args:
            delimiter: The string inserted between collected values.

        Returns:
            A reusable `Collector` implementing the described reduction.
        """
        return Collector(
            list,
            lambda values, value: _append(values, str(value)),
            lambda values: delimiter.join(values),
            _extend,
        )

    @staticmethod
    def grouping_by(
        classifier: Selector,
        downstream: Collector[T, Any, R] | Callable[[Iterable[T]], R] | None = None,
    ) -> Collector[T, dict[Any, Any], dict[Any, R | list[T]]]:
        """Group items by classifier and reduce each group.

        Args:
            classifier: A callable that selects the group for each input item.
            downstream: The collector or gatherer that receives transformed items.

        Returns:
            A reusable `Collector` implementing the described reduction.
        """
        classify = compile_selector(classifier)
        reduction = _as_collector(Collectors.to_list() if downstream is None else downstream)

        def step(groups: dict[Any, Any], value: T) -> dict[Any, Any]:
            key = classify(value)
            try:
                state = groups[key]
            except KeyError:
                state = reduction.initializer()
            groups[key] = reduction.step(state, value)
            return groups

        def finish(groups: dict[Any, Any]) -> dict[Any, Any]:
            return {key: reduction.finish(state) for key, state in groups.items()}

        return Collector(dict, step, finish)

    @staticmethod
    def partitioning_by(
        predicate: Selector,
        downstream: Collector[T, Any, R] | Callable[[Iterable[T]], R] | None = None,
    ) -> Collector[T, dict[bool, Any], dict[bool, R | list[T]]]:
        """Split items by predicate and reduce both partitions.

        Args:
            predicate: A callable that decides whether an item matches.
            downstream: The collector or gatherer that receives transformed items.

        Returns:
            A reusable `Collector` implementing the described reduction.
        """
        test = compile_selector(predicate)
        reduction = _as_collector(Collectors.to_list() if downstream is None else downstream)

        def initialize() -> dict[bool, Any]:
            return {False: reduction.initializer(), True: reduction.initializer()}

        def step(groups: dict[bool, Any], value: T) -> dict[bool, Any]:
            key = bool(test(value))
            groups[key] = reduction.step(groups[key], value)
            return groups

        def finish(groups: dict[bool, Any]) -> dict[bool, Any]:
            return {key: reduction.finish(groups[key]) for key in (False, True)}

        return Collector(initialize, step, finish)

    @staticmethod
    def mapping(
        mapper: Selector,
        downstream: Collector[U, Any, R] | Callable[[Iterable[U]], R],
    ) -> Collector[T, Any, R]:
        """Transform each item before passing it to downstream.

        Args:
            mapper: The callable used to transform each selected value.
            downstream: The collector or gatherer that receives transformed items.

        Returns:
            A reusable `Collector` implementing the described reduction.
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
        """Pass only matching items to downstream.

        Args:
            predicate: A callable that decides whether an item matches.
            downstream: The collector or gatherer that receives transformed items.

        Returns:
            A reusable `Collector` implementing the described reduction.
        """
        test = compile_selector(predicate)
        reduction = _as_collector(downstream)

        def step(state: Any, value: T) -> Any:
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
        """Expand each item and pass every nested item to downstream.

        Args:
            mapper: The callable used to transform each selected value.
            downstream: The collector or gatherer that receives transformed items.

        Returns:
            A reusable `Collector` implementing the described reduction.
        """
        expand = compile_selector(mapper)
        reduction = _as_collector(downstream)

        def step(state: Any, value: T) -> Any:
            iterator = iter(expand(value))
            try:
                while not reduction.done(state):
                    try:
                        nested = next(iterator)
                    except StopIteration:
                        break
                    state = reduction.step(state, nested)
            finally:
                close = getattr(iterator, "close", None)
                if callable(close):
                    close()
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
        """Apply finisher to the result produced by downstream.

        Args:
            downstream: The collector or gatherer that receives transformed items.
            finisher: A callable that converts accumulated state into the final result.

        Returns:
            A reusable `Collector` implementing the described reduction.
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
        """Run two collectors in one traversal and merge their results.

        Args:
            left: The first collector, result, or value to combine.
            right: The second collector, result, or value to combine.
            merger: A callable that merges two downstream results.

        Returns:
            A reusable `Collector` implementing the described reduction.
        """
        if not callable(merger):
            raise TypeError("merger must be callable")
        left_reduction = _as_collector(left)
        right_reduction = _as_collector(right)

        def initialize() -> _TeeState:
            return _TeeState(left_reduction.initializer(), right_reduction.initializer())

        def step(state: _TeeState, value: T) -> _TeeState:
            if not left_reduction.done(state.left):
                state.left = left_reduction.step(state.left, value)
            if not right_reduction.done(state.right):
                state.right = right_reduction.step(state.right, value)
            return state

        def finish(state: _TeeState) -> V:
            return merger(
                left_reduction.finish(state.left),
                right_reduction.finish(state.right),
            )

        def done(state: _TeeState) -> bool:
            return left_reduction.done(state.left) and right_reduction.done(state.right)

        return Collector(initialize, step, finish, done=done)

    @staticmethod
    def counting() -> Collector[T, int, int]:
        """Count all input items.

        Returns:
            A reusable `Collector` implementing the described reduction.
        """
        return Collector(lambda: 0, lambda count, _value: count + 1, combine=lambda a, b: a + b)

    @staticmethod
    def summing(selector: Selector | None = None) -> Collector[T, Any, Any]:
        """Sum input items or selected values.

        Args:
            selector: A callable, field name, index, path, or expression used to select a value.

        Returns:
            A reusable `Collector` implementing the described reduction.
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
        """Return the arithmetic mean of items or selected values.

        Args:
            selector: A callable, field name, index, path, or expression used to select a value.

        Returns:
            A reusable `Collector` implementing the described reduction.
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
        """Return count, sum, minimum, maximum, and average in one traversal.

        Args:
            selector: A callable, field name, index, path, or expression used to select a value.

        Returns:
            A reusable `Collector` implementing the described reduction.
        """
        select = _selector(selector)

        def step(statistics: SummaryStatistics, value: T) -> SummaryStatistics:
            statistics.accept(select(value))
            return statistics

        def finish(statistics: SummaryStatistics) -> SummaryStatistics:
            if not statistics.count:
                statistics.min = statistics.max = 0.0
            return statistics

        return Collector(SummaryStatistics, step, finish)

    @staticmethod
    def first() -> Collector[T, Any, T | None]:
        """Return the first item, or None when input is empty.

        Returns:
            A reusable `Collector` implementing the described reduction.
        """
        return Collector(
            lambda: _MISSING,
            lambda _state, value: value,
            lambda value: None if value is _MISSING else value,
            done=lambda value: value is not _MISSING,
        )

    @staticmethod
    def last() -> Collector[T, Any, T | None]:
        """Return the last item, or None when input is empty.

        Returns:
            A reusable `Collector` implementing the described reduction.
        """
        return Collector(
            lambda: _MISSING,
            lambda _state, value: value,
            lambda value: None if value is _MISSING else value,
        )

    @staticmethod
    def head(count: int) -> Collector[T, list[T], list[T]]:
        """Collect at most the first count items.

        Args:
            count: The requested number of items.

        Returns:
            A reusable `Collector` implementing the described reduction.
        """
        if count < 0:
            raise ValueError("head count must be non-negative")
        return Collector(list, _append, done=lambda values: len(values) >= count)

    @staticmethod
    def tail(count: int) -> Collector[T, deque[T], list[T]]:
        """Collect at most the last count items.

        Args:
            count: The requested number of items.

        Returns:
            A reusable `Collector` implementing the described reduction.
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
        """Return the sole item, None for empty input, or raise for multiple items.

        Returns:
            A reusable `Collector` implementing the described reduction.

        Raises:
            OnlyElementError: If the input contains more than one item.
        """

        def finish(values: list[T]) -> T | None:
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
        """Collect selected keys and values into a dictionary.

        Args:
            key: The callable or selector used to derive a key.
            value: The value consumed by this operation.
            on_duplicate: The policy used when the same key appears more than once.

        Returns:
            A reusable `Collector` implementing the described reduction.
        """
        if on_duplicate not in {"error", "first", "last"}:
            raise ValueError("on_duplicate must be 'error', 'first', or 'last'")
        select_key = compile_selector(key)
        select_value = compile_selector(value)

        def step(result: dict[K, V], item: T) -> dict[K, V]:
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
        """Collect mapping rows into column-oriented lists.

        Returns:
            A reusable `Collector` implementing the described reduction.
        """

        def initialize() -> _ColumnsState:
            return _ColumnsState({})

        def step(state: _ColumnsState, row: Mapping[str, Any]) -> _ColumnsState:
            for name in row:
                if name not in state.columns:
                    state.columns[name] = [None] * state.count
            for name, column in state.columns.items():
                column.append(row.get(name))
            state.count += 1
            return state

        return Collector(initialize, step, lambda state: state.columns)
