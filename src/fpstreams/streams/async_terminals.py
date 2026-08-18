"""Terminal operations shared by asynchronous Flow instances."""

from __future__ import annotations

import operator
from collections import deque
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable
from typing import Any, Generic, TypeVar, cast

from ..collecting.aggregation import Aggregator, prepare_aggregations
from ..collecting.collector import (
    Collector,
    CollectorItems,
    collectors_done,
    finish_collectors,
    initialize_collectors,
    prepare_collectors,
    step_collectors,
)
from ..collecting.statistics import (
    OnlineStatistics,
    StatisticsSnapshot,
    mean_from,
    std_from,
    validate_ddof,
    variance_from,
)
from ..errors import BufferLimitError, EmptyFlowError
from ..expressions.selectors import Selector, compile_selector
from ..planning.async_utils import _MISSING, _close, _resolve
from ..primitives.result import Err, Ok

T = TypeVar("T")
R = TypeVar("R")
C = TypeVar("C")


async def _run_async_collectors(
    values: AsyncIterable[Any], items: CollectorItems
) -> dict[str, Any]:
    """Update all collectors in one async traversal and close the source iterator.

    Consumption stops as soon as every collector reports completion; each finalizer
    then converts its retained state into the named result.
    """
    states = initialize_collectors(items)
    iterator = values.__aiter__()
    try:
        while not collectors_done(states, items):
            try:
                value = await anext(iterator)
            except StopAsyncIteration:
                break
            step_collectors(states, items, value)
    finally:
        await _close(iterator)
    return finish_collectors(states, items)


class AsyncFlowTerminalsMixin(Generic[T]):
    """Terminal and reduction methods mixed into the public AsyncFlow class."""

    def __aiter__(self) -> AsyncIterator[T]:
        """Yield items from the concrete AsyncFlow implementation."""
        raise NotImplementedError

    def filter(
        self, predicate: Callable[[T], bool | Awaitable[bool]]
    ) -> AsyncFlowTerminalsMixin[T]:
        """Build a lazy async pipeline that keeps items satisfying `predicate`."""
        raise NotImplementedError

    def drop(self, count: int) -> AsyncFlowTerminalsMixin[T]:
        """Build a lazy async pipeline that skips the first `count` items."""
        raise NotImplementedError

    async def to_list(self) -> list[T]:
        """Consume the async flow and collect its items in a list.

        Returns:
            All emitted items in encounter order.
        """
        return [item async for item in self]

    async def to_tuple(self) -> tuple[T, ...]:
        """Execute the async pipeline and collect its items in a tuple.

        Returns:
            All emitted items in encounter order as a tuple.
        """
        return tuple([item async for item in self])

    async def to_set(self) -> set[T]:
        """Execute the async pipeline and collect distinct hashable items.

        Returns:
            The distinct emitted items; every item must be hashable.
        """
        return {item async for item in self}

    async def join(self, separator: str = "") -> str:
        """Convert items to strings and join them with separator.

        This is a string terminal operation; it consumes the flow and does not perform a
        relational join.

        Args:
            separator: String placed between consecutive `str(item)` values.

        Returns:
            One string containing every item separated by `separator`.
        """
        values: list[str] = []
        iterator = self.__aiter__()
        try:
            async for item in iterator:
                values.append(str(item))
        finally:
            await _close(iterator)
        return separator.join(values)

    async def partition(
        self,
        predicate: Callable[[T], bool | Awaitable[bool]],
    ) -> tuple[list[T], list[T]]:
        """Collect matching and non-matching items in separate lists.

        Args:
            predicate: Sync or async callable resolved once per item; truthy items enter the first
                returned list.

        Returns:
            `(matches, misses)`, preserving encounter order within both lists.
        """
        matches: list[T] = []
        misses: list[T] = []
        iterator = self.__aiter__()
        try:
            async for item in iterator:
                (matches if await _resolve(predicate(item)) else misses).append(item)
        finally:
            await _close(iterator)
        return matches, misses

    async def partition_results(self) -> tuple[list[Any], list[Exception]]:
        """Separate Result values into successes and failures.

        Returns:
            `(success_values, exceptions)`, with `Ok` and `Err` payloads unwrapped in their
            respective encounter orders.

        Raises:
            TypeError: If any emitted item is neither `Ok` nor `Err`.
        """
        successes: list[Any] = []
        failures: list[Exception] = []
        iterator = self.__aiter__()
        try:
            async for result in iterator:
                if isinstance(result, Ok):
                    successes.append(result.value)
                elif isinstance(result, Err):
                    failures.append(result.error)
                else:
                    raise TypeError("partition_results() requires Result values")
        finally:
            await _close(iterator)
        return successes, failures

    async def first(self, default: Any = _MISSING) -> T | Any:
        """Return the first item and close the async source immediately.

        Args:
            default: Returned only when the async flow is empty.

        Returns:
            The first item, or `default` when the flow is empty.

        Raises:
            EmptyFlowError: If the flow is empty and no default is supplied.
        """
        iterator = self.__aiter__()
        try:
            return await anext(iterator)
        except StopAsyncIteration:
            if default is _MISSING:
                raise EmptyFlowError("first() called on an empty async flow") from None
            return default
        finally:
            await _close(iterator)

    async def last(self, default: Any = _MISSING) -> T | Any:
        """Return the last item, or default when the flow is empty.

        Args:
            default: Returned only when the async flow is empty.

        Returns:
            The last item, or `default` when the flow is empty.

        Raises:
            EmptyFlowError: If the async flow is empty and no default is supplied.
        """
        result: Any = _MISSING
        async for item in self:
            result = item
        if result is _MISSING:
            if default is _MISSING:
                raise EmptyFlowError("last() called on an empty async flow")
            return default
        return result

    async def find(
        self,
        predicate: Callable[[T], bool | Awaitable[bool]],
        default: Any = _MISSING,
    ) -> T | Any:
        """Return the first matching item, a default, or raise EmptyFlowError.

        Args:
            predicate: Sync or async callable resolved in order until its first truthy result.
            default: Returned when no resolved predicate result is truthy.

        Returns:
            The first matching item, or `default` when no item matches.

        Raises:
            EmptyFlowError: If no item matches and no default is supplied.
        """
        matches = self.filter(predicate)
        if default is not _MISSING:
            return await matches.first(default)
        try:
            return await matches.first()
        except EmptyFlowError:
            raise EmptyFlowError("find() found no matching item") from None

    async def find_index(self, predicate: Callable[[T], bool | Awaitable[bool]]) -> int | None:
        """Return the index of the first matching item, or None.

        Args:
            predicate: Sync or async callable resolved in order until its first truthy result.

        Returns:
            The zero-based position of the first truthy resolved predicate result, or `None`.
        """
        if not callable(predicate):
            raise TypeError("predicate must be callable")
        iterator = self.__aiter__()
        position = 0
        try:
            async for item in iterator:
                if await _resolve(predicate(item)):
                    return position
                position += 1
        finally:
            await _close(iterator)
        return None

    async def index_of(self, value: T) -> int | None:
        """Return the index of the first equal value, or None.

        Args:
            value: Target compared to each source item with equality.

        Returns:
            The zero-based position of the first item equal to `value`, or `None`.
        """
        return await self.find_index(lambda item: item == value)

    async def nth(self, index: int, default: Any = _MISSING) -> T | Any:
        """Return the item at a positive or negative index.

        Args:
            index: Zero-based position; negative values count backward from the end.
            default: Returned when `index` is outside the async flow.

        Returns:
            The selected item, or `default` when the index is out of range.

        Raises:
            EmptyFlowError: If the index is out of range and no default is supplied.
        """
        position = operator.index(index)
        if position >= 0:
            candidate = self.drop(position)
            if default is not _MISSING:
                return await candidate.first(default)
            try:
                return await candidate.first()
            except EmptyFlowError:
                raise EmptyFlowError(f"nth({position}) is out of range") from None
        if position == -1:
            if default is not _MISSING:
                return await self.last(default)
            try:
                return await self.last()
            except EmptyFlowError:
                raise EmptyFlowError("nth(-1) is out of range") from None

        width = -position
        tail: deque[T] = deque(maxlen=width)
        iterator = self.__aiter__()
        try:
            async for item in iterator:
                tail.append(item)
        finally:
            await _close(iterator)
        if len(tail) == width:
            return tail[0]
        if default is not _MISSING:
            return default
        raise EmptyFlowError(f"nth({position}) is out of range")

    async def count(self) -> int:
        """Count all items produced by the async flow.

        Returns:
            The total number of emitted items.
        """
        count = 0
        async for _item in self:
            count += 1
        return count

    async def collect(
        self,
        collector: (Callable[[Iterable[T]], C | Awaitable[C]] | Collector[T, Any, C] | None) = None,
        /,
        **collectors: Collector[T, Any, Any],
    ) -> C | dict[str, Any]:
        """Reduce the flow with one Collector or named Collectors.

        One collector returns its finished value. Named collectors share one async traversal and
        return a dictionary.

        Args:
            collector: A streaming `Collector`, or a sync or async callable that receives a list
                containing the entire consumed flow.
            **collectors: Named streaming collectors updated in one async traversal.

        Returns:
            The collector result, or a dictionary of results for named collectors.
        """
        if collector is not None and collectors:
            raise TypeError("collect accepts either one collector or named collectors, not both")
        if isinstance(collector, Collector):
            result = await _run_async_collectors(self, (("result", collector),))
            return cast(C, result["result"])
        if collector is not None:
            return cast(C, await _resolve(collector(await self.to_list())))
        return await _run_async_collectors(self, prepare_collectors(cast(Any, collectors)))

    async def aggregate(self, **aggregations: Aggregator) -> dict[str, Any]:
        """Compute several named aggregations while traversing the async flow once.

        All named aggregators are updated during the same asynchronous traversal.

        Args:
            **aggregations: Result names mapped to aggregators updated in one async traversal.

        Returns:
            Finished aggregation values keyed by the supplied argument names.
        """
        items = prepare_aggregations(aggregations)
        return await _run_async_collectors(self, items)

    summarize = aggregate

    async def _statistics_snapshot(self) -> StatisticsSnapshot:
        """Consume numeric items into a stable one-pass statistics snapshot."""
        statistics = OnlineStatistics()
        iterator = self.__aiter__()
        try:
            async for item in iterator:
                statistics.accept(item)
        finally:
            await _close(iterator)
        return statistics.snapshot()

    async def mean(self) -> float | None:
        """Return the arithmetic mean, or None for an empty flow.

        Returns:
            The compensated floating-point mean, or `None` when no items are emitted.

        Raises:
            TypeError: If an emitted item is not a real number.
        """
        return mean_from(await self._statistics_snapshot())

    average = mean

    async def variance(self, *, ddof: int = 1) -> float | None:
        """Return the variance, or None when too few values are available.

        Args:
            ddof: Non-negative adjustment in the divisor `count - ddof`.

        Returns:
            The floating-point variance, or `None` when `count <= ddof`.

        Raises:
            TypeError: If an emitted item is not a real number.
            ValueError: If `ddof` is negative.
        """
        validate_ddof(ddof)
        return variance_from(await self._statistics_snapshot(), ddof)

    async def std(self, *, ddof: int = 1) -> float | None:
        """Return the standard deviation, or None when too few values are available.

        Args:
            ddof: Non-negative adjustment in the variance divisor `count - ddof`.

        Returns:
            The square root of the variance, or `None` when `count <= ddof`.

        Raises:
            TypeError: If an emitted item is not a real number.
            ValueError: If `ddof` is negative.
        """
        validate_ddof(ddof)
        return std_from(await self._statistics_snapshot(), ddof)

    async def reduce(
        self,
        function: Callable[[Any, T], Any | Awaitable[Any]],
        initial: Any = _MISSING,
    ) -> Any:
        """Combine items from left to right with an optional initial value.

        Args:
            function: Sync or async callback invoked as `function(accumulator, item)` from left to
                right.
            initial: Starting accumulator; when omitted, the first item becomes the accumulator.

        Returns:
            The final left-to-right accumulator.

        Raises:
            EmptyFlowError: If the async flow is empty and `initial` is omitted.
        """
        accumulator = initial
        async for item in self:
            if accumulator is _MISSING:
                accumulator = item
            else:
                accumulator = await _resolve(function(accumulator, item))
        if accumulator is _MISSING:
            raise EmptyFlowError("reduce() called on an empty async flow")
        return accumulator

    async def reduce_right(
        self,
        function: Callable[[T, Any], Any | Awaitable[Any]],
        initial: Any = _MISSING,
        *,
        max_items: int | None = None,
    ) -> Any:
        """Combine buffered items from right to left.

        Args:
            function: Sync or async callback invoked as `function(item, accumulator)` from right
                to left.
            initial: Starting accumulator; when omitted, the last item becomes the accumulator.
            max_items: Optional maximum number of source items that may be buffered.

        Returns:
            The final right-to-left accumulator.

        Raises:
            BufferLimitError: If the source contains more than `max_items` items.
            EmptyFlowError: If the async flow is empty and `initial` is omitted.
        """
        if not callable(function):
            raise TypeError("function must be callable")
        if max_items is not None:
            max_items = operator.index(max_items)
            if max_items < 0:
                raise ValueError("max_items must be non-negative")
        values: list[T] = []
        iterator = self.__aiter__()
        try:
            async for item in iterator:
                if max_items is not None and len(values) >= max_items:
                    raise BufferLimitError(f"reduce_right() exceeded max_items={max_items}")
                values.append(item)
        finally:
            await _close(iterator)
        if initial is _MISSING:
            if not values:
                raise EmptyFlowError("reduce_right() requires at least one item")
            accumulator = values.pop()
        else:
            accumulator = initial
        for item in reversed(values):
            accumulator = await _resolve(function(item, accumulator))
        return accumulator

    fold_right = reduce_right

    async def reduce_by(
        self,
        key: Selector,
        function: Callable[[R, T], R | Awaitable[R]],
        *,
        initializer: Callable[[], R | Awaitable[R]],
    ) -> dict[Any, R]:
        """Reduce items independently for each selected key.

        Args:
            key: Callable, field name, index, path, or expression selecting each group key; callable
                results may be awaitable.
            function: Sync or async callback invoked as `function(group_state, item)`.
            initializer: Sync or async callable invoked when each distinct group is first seen.

        Returns:
            Final resolved accumulator for each hashable key, in first-key encounter order.
        """
        if not callable(initializer):
            raise TypeError("initializer must be callable")
        select = compile_selector(key)
        states: dict[Any, R] = {}
        async for item in self:
            group = await _resolve(select(item))
            try:
                state = states[group]
            except KeyError:
                state = cast(R, await _resolve(initializer()))
            except TypeError:
                raise TypeError("reduce_by() keys must be hashable") from None
            states[group] = cast(R, await _resolve(function(state, item)))
        return states

    fold_by = reduce_by

    async def frequencies(self, key: Selector | None = None) -> dict[Any, int]:
        """Count occurrences of values or selected keys.

        Args:
            key: Optional callable, field name, index, path, or expression selecting the value to
                count; the item itself is counted when omitted.

        Returns:
            Occurrence counts keyed by each hashable resolved selected value.
        """
        selector: Selector = (lambda item: item) if key is None else key
        return await self.reduce_by(
            selector,
            lambda count, _item: count + 1,
            initializer=lambda: 0,
        )

    count_by = frequencies

    async def any(self, predicate: Callable[[T], bool | Awaitable[bool]] = bool) -> bool:
        """Return whether at least one item satisfies the predicate.

        Args:
            predicate: Sync or async predicate resolved in order until one result is truthy;
                defaults to `bool`.

        Returns:
            `True` when any item satisfies `predicate`; `False` for an empty flow.
        """
        iterator = self.__aiter__()
        try:
            async for item in iterator:
                if await _resolve(predicate(item)):
                    return True
            return False
        finally:
            await _close(iterator)

    async def all(self, predicate: Callable[[T], bool | Awaitable[bool]] = bool) -> bool:
        """Return whether every item satisfies the predicate.

        Args:
            predicate: Sync or async predicate resolved in order until one result is falsey;
                defaults to `bool`.

        Returns:
            `True` when every item satisfies `predicate`, including for an empty flow.
        """
        iterator = self.__aiter__()
        try:
            async for item in iterator:
                if not await _resolve(predicate(item)):
                    return False
            return True
        finally:
            await _close(iterator)

    async def none(self, predicate: Callable[[T], bool | Awaitable[bool]] = bool) -> bool:
        """Return whether no item satisfies predicate.

        Args:
            predicate: Sync or async predicate resolved in order until one result is truthy;
                defaults to `bool`.

        Returns:
            `True` only when no item satisfies `predicate`, including for an empty flow.
        """
        return not await self.any(predicate)

    async def for_each(self, action: Callable[[T], Any]) -> None:
        """Run an action for every item and return after completion.

        Args:
            action: Sync or async callable resolved once for every emitted item; return values are
                ignored.
        """
        async for item in self:
            await _resolve(action(item))
