"""Lazy asynchronous pipelines and real-time terminal operations."""

from __future__ import annotations

import asyncio
import operator
import os
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable
from typing import Any, Generic, TypeVar, cast

from ..execution.async_ import _execute
from ..expressions.selectors import Selector, compile_selector
from ..planning.async_ import (
    _Append,
    _AsyncOperation,
    _AsyncPlan,
    _AsyncSource,
    _BatchBySize,
    _BufferTimeout,
    _Chunk,
    _Collapse,
    _CombineLatest,
    _Concat,
    _Cross,
    _Debounce,
    _Delay,
    _Drop,
    _DropWhile,
    _Enumerate,
    _Filter,
    _FlatMap,
    _Fold,
    _GroupRuns,
    _Intersperse,
    _MapAsync,
    _MapFirst,
    _MapLast,
    _Merge,
    _MergeMap,
    _Pairwise,
    _Prepend,
    _Scan,
    _ScanRight,
    _SwitchMap,
    _Take,
    _TakeWhile,
    _TakeWhileInclusive,
    _Tap,
    _Throttle,
    _Timeout,
    _Unique,
    _Window,
    _Zip,
    _ZipLongest,
)
from ..planning.async_utils import _resolve
from ..primitives.result import Err, Ok, Result
from .async_terminals import AsyncFlowTerminalsMixin

T = TypeVar("T")
R = TypeVar("R")
C = TypeVar("C")
U = TypeVar("U")


def _default_item_size(value: Any) -> int:
    return len(value)


class AsyncFlow(AsyncFlowTerminalsMixin[T], Generic[T]):
    """A lazy pipeline over synchronous or asynchronous iterable sources."""

    __slots__ = ("_plan",)

    def __init__(self, source: AsyncIterable[T] | Iterable[T]) -> None:
        self._plan: _AsyncPlan[T] = _AsyncPlan(_AsyncSource.from_value(source))

    @staticmethod
    def _from_plan(plan: _AsyncPlan[R]) -> AsyncFlow[R]:
        instance: AsyncFlow[R] = object.__new__(AsyncFlow)
        instance._plan = plan
        return instance

    @staticmethod
    def of(*items: R) -> AsyncFlow[R]:
        """Create an async flow from positional items.

        Args:
            *items: Items emitted by the new pipeline.

        Returns:
            A new lazy `AsyncFlow`.
        """
        return AsyncFlow[R](items)

    @staticmethod
    def from_iterable(source: Iterable[R]) -> AsyncFlow[R]:
        """Create an async flow over a synchronous iterable.

        Args:
            source: The iterable, async iterable, or data source to read lazily.

        Returns:
            A new lazy `AsyncFlow`.
        """
        return AsyncFlow[R](source)

    @staticmethod
    def from_aiterable(source: AsyncIterable[R]) -> AsyncFlow[R]:
        """Create an async flow over an asynchronous iterable.

        Args:
            source: The iterable, async iterable, or data source to read lazily.

        Returns:
            A new lazy `AsyncFlow`.
        """
        return AsyncFlow[R](source)

    @staticmethod
    def from_file(path: str | os.PathLike[str], *, encoding: str = "utf-8") -> AsyncFlow[str]:
        """Read a text file asynchronously and emit lines without trailing newlines.

        Args:
            path: The filesystem path to read from or write to.
            encoding: The text encoding used to open the file.

        Returns:
            A new lazy `AsyncFlow`.
        """

        async def lines() -> AsyncIterator[str]:
            try:
                import aiofiles  # type: ignore[import-untyped]
            except ImportError:
                raise ImportError("AsyncFlow.from_file() requires the 'async' extra") from None
            async with aiofiles.open(path, encoding=encoding) as file:
                async for line in file:
                    yield line.rstrip("\r\n")

        return AsyncFlow._from_plan(_AsyncPlan(_AsyncSource.defer(lines)))

    @staticmethod
    def interval(seconds: float) -> AsyncFlow[int]:
        """Emit increasing integers separated by seconds.

        Args:
            seconds: The duration in seconds used by the timing operation.

        Returns:
            A new lazy `AsyncFlow`.
        """
        if seconds < 0:
            raise ValueError("seconds cannot be negative")

        async def ticks() -> AsyncIterator[int]:
            current = 0
            while True:
                await asyncio.sleep(seconds)
                yield current
                current += 1

        return AsyncFlow._from_plan(_AsyncPlan(_AsyncSource.defer(ticks)))

    @staticmethod
    def paginate(
        fetch_page: Callable[
            [C | None],
            tuple[AsyncIterable[R] | Iterable[R], C | None]
            | Awaitable[tuple[AsyncIterable[R] | Iterable[R], C | None]],
        ],
        *,
        start: C | None = None,
    ) -> AsyncFlow[R]:
        """Fetch pages lazily until the returned cursor is None.

        Args:
            fetch_page: An async callable that returns page items and the next cursor.
            start: The first index, numeric value, or additive identity to use.

        Returns:
            A new lazy `AsyncFlow`.
        """

        async def items() -> AsyncIterator[R]:
            cursor = start
            while True:
                page, next_cursor = await _resolve(fetch_page(cursor))
                if isinstance(page, AsyncIterable):
                    async for item in page:
                        yield item
                else:
                    for item in page:
                        yield item
                if next_cursor is None:
                    return
                cursor = next_cursor

        return AsyncFlow._from_plan(_AsyncPlan(_AsyncSource.defer(items)))

    def __aiter__(self) -> AsyncIterator[T]:
        return cast(AsyncIterator[T], _execute(self._plan))

    def _append(self, operation: _AsyncOperation) -> AsyncFlow[Any]:
        return self._from_plan(_AsyncPlan(self._plan.source, (*self._plan.operations, operation)))

    def map_async(
        self,
        function: Callable[[T], R | Awaitable[R]],
        *,
        concurrency: int = 8,
        ordered: bool = True,
        timeout: float | None = None,
    ) -> AsyncFlow[R]:
        """Map items with bounded concurrency and optional ordering and timeout.

        Concurrency is bounded. Ordered mode delays later completed results until earlier inputs
        finish.

        Args:
            function: The callable applied by this operation.
            concurrency: The maximum number of operations or inner sources active at once.
            ordered: Whether results must preserve source encounter order.
            timeout: The optional maximum duration in seconds before the operation fails.

        Returns:
            A new lazy `AsyncFlow` representing this operation.

        Raises:
            ValueError: If concurrency is less than one.
        """
        # Concurrency is stored in the plan and enforced when the flow is consumed.
        if concurrency < 1:
            raise ValueError("concurrency must be at least 1")
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be positive")
        operation = _MapAsync(function, concurrency, ordered, timeout)
        return cast(AsyncFlow[R], self._append(operation))

    def map(self, function: Callable[[T], R | Awaitable[R]]) -> AsyncFlow[R]:
        """Apply a synchronous or asynchronous function to each item in order.

        `map` preserves encounter order and awaits awaitable results; work starts only when the
        flow is consumed.

        Args:
            function: The callable applied by this operation.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        return self.map_async(function, concurrency=1)

    def filter(self, predicate: Callable[[T], bool | Awaitable[bool]]) -> AsyncFlow[T]:
        """Keep items for which the sync or async predicate is truthy.

        The predicate may be synchronous or asynchronous and is evaluated in encounter order.

        Args:
            predicate: A callable that decides whether an item matches.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        # The predicate is resolved during async iteration, not while building the flow.
        return cast(AsyncFlow[T], self._append(_Filter(predicate)))

    def where(self, predicate: Callable[[T], bool | Awaitable[bool]]) -> AsyncFlow[T]:
        """Keep items for which predicate returns a truthy value.

        This is an alias-style filtering operation for a synchronous or asynchronous predicate.

        Args:
            predicate: A callable that decides whether an item matches.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        return self.filter(predicate)

    def reject(self, predicate: Callable[[T], bool | Awaitable[bool]]) -> AsyncFlow[T]:
        """Drop items for which the sync or async predicate is truthy.

        Args:
            predicate: A callable that decides whether an item matches.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """

        async def inverse(item: T) -> bool:
            return not await _resolve(predicate(item))

        return self.filter(inverse)

    def tap(self, action: Callable[[T], Any]) -> AsyncFlow[T]:
        """Run a sync or async side effect while passing each item through.

        Args:
            action: The side-effecting callable invoked for each matching item.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        return cast(AsyncFlow[T], self._append(_Tap(action)))

    def flat_map(
        self,
        function: Callable[
            [T],
            AsyncIterable[R] | Iterable[R] | Awaitable[AsyncIterable[R] | Iterable[R]],
        ],
    ) -> AsyncFlow[R]:
        """Map each item to an iterable and emit its contents.

        The mapper may be synchronous or asynchronous; each returned iterable is flattened in
        encounter order.

        Args:
            function: The callable applied by this operation.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        return cast(AsyncFlow[R], self._append(_FlatMap(function)))

    def merge(
        self,
        *others: AsyncIterable[T] | Iterable[T],
    ) -> AsyncFlow[T]:
        """Merge this flow with other sources in completion order.

        Args:
            *others: Additional sources combined with this pipeline.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        if not others:
            return self
        sources = tuple(_AsyncSource.from_value(other) for other in others)
        return cast(AsyncFlow[T], self._append(_Merge(sources)))

    def combine_latest(
        self,
        *others: AsyncIterable[Any] | Iterable[Any],
    ) -> AsyncFlow[tuple[Any, ...]]:
        """Emit the latest value from every source after all have produced once.

        Args:
            *others: Additional sources combined with this pipeline.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        sources = tuple(_AsyncSource.from_value(other) for other in others)
        return cast(AsyncFlow[tuple[Any, ...]], self._append(_CombineLatest(sources)))

    def merge_map(
        self,
        function: Callable[
            [T],
            AsyncIterable[R] | Iterable[R] | Awaitable[AsyncIterable[R] | Iterable[R]],
        ],
        *,
        concurrency: int = 8,
    ) -> AsyncFlow[R]:
        """Map items to inner sources and merge them with bounded concurrency.

        Args:
            function: The callable applied by this operation.
            concurrency: The maximum number of operations or inner sources active at once.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        if not callable(function):
            raise TypeError("function must be callable")
        if concurrency < 1:
            raise ValueError("concurrency must be at least 1")
        return cast(AsyncFlow[R], self._append(_MergeMap(function, concurrency)))

    flat_map_merge = merge_map

    def switch_map(
        self,
        function: Callable[
            [T],
            AsyncIterable[R] | Iterable[R] | Awaitable[AsyncIterable[R] | Iterable[R]],
        ],
    ) -> AsyncFlow[R]:
        """Map each item to a source and emit only from the latest source.

        When a new outer item arrives, the previous mapper or inner source is cancelled and
        closed. After the outer source completes, the latest inner source may finish normally.

        Args:
            function: A sync or async callable returning a sync or async iterable.

        Returns:
            A new lazy `AsyncFlow` that emits items from the most recent inner source.

        Raises:
            TypeError: If function is not callable.
        """
        if not callable(function):
            raise TypeError("function must be callable")
        return cast(AsyncFlow[R], self._append(_SwitchMap(function)))

    def delay(self, seconds: float) -> AsyncFlow[T]:
        """Wait before requesting the first item from this flow.

        The delay is applied once per evaluation. No upstream item is requested while waiting,
        and cancellation during the delay still closes the source.

        Args:
            seconds: The positive delay in seconds before the first upstream request.

        Returns:
            A new lazy `AsyncFlow` with delayed consumption.

        Raises:
            ValueError: If seconds is not positive.
        """
        if seconds <= 0:
            raise ValueError("seconds must be positive")
        return cast(AsyncFlow[T], self._append(_Delay(seconds)))

    def throttle(self, max_count: int, *, per: float) -> AsyncFlow[T]:
        """Limit emissions within a sliding time window.

        Up to max_count items may be emitted immediately. Later items wait until the oldest
        emission leaves the monotonic window; encounter order and pull-based backpressure are
        preserved.

        Args:
            max_count: The maximum number of emissions allowed in one window.
            per: The positive sliding-window duration in seconds.

        Returns:
            A new lazy `AsyncFlow` with bounded emission rate.

        Raises:
            TypeError: If max_count is not an integer.
            ValueError: If max_count is below one or per is not positive.
        """
        try:
            count = operator.index(max_count)
        except TypeError:
            raise TypeError("max_count must be an integer") from None
        if count < 1:
            raise ValueError("max_count must be at least 1")
        if per <= 0:
            raise ValueError("per must be positive")
        return cast(AsyncFlow[T], self._append(_Throttle(count, per)))

    def spaceout(self, seconds: float) -> AsyncFlow[T]:
        """Separate consecutive emissions by at least seconds.

        The first item is emitted immediately. Each later item waits only for the remainder of
        the requested interval, using the event loop's monotonic clock.

        Args:
            seconds: The positive minimum interval between emissions.

        Returns:
            A new lazy `AsyncFlow` with evenly spaced emissions.

        Raises:
            ValueError: If seconds is not positive.
        """
        if seconds <= 0:
            raise ValueError("seconds must be positive")
        return self.throttle(1, per=seconds)

    def timeout(self, seconds: float) -> AsyncFlow[T]:
        """Fail when the next item takes longer than seconds.

        Args:
            seconds: The duration in seconds used by the timing operation.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        if seconds <= 0:
            raise ValueError("seconds must be positive")
        return cast(AsyncFlow[T], self._append(_Timeout(seconds)))

    def debounce(self, seconds: float) -> AsyncFlow[T]:
        """Emit an item only after the source stays quiet for seconds.

        Args:
            seconds: The duration in seconds used by the timing operation.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        if seconds < 0:
            raise ValueError("seconds cannot be negative")
        return cast(AsyncFlow[T], self._append(_Debounce(seconds)))

    def buffer_timeout(self, max_count: int, seconds: float) -> AsyncFlow[tuple[T, ...]]:
        """Flush a tuple when it reaches max_count or seconds elapse.

        Args:
            max_count: The maximum number of items emitted in one batch.
            seconds: The duration in seconds used by the timing operation.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        try:
            count = operator.index(max_count)
        except TypeError:
            raise TypeError("max_count must be an integer") from None
        if count < 1:
            raise ValueError("max_count must be at least 1")
        if seconds <= 0:
            raise ValueError("seconds must be positive")
        return cast(
            AsyncFlow[tuple[T, ...]],
            self._append(_BufferTimeout(count, seconds)),
        )

    batch_timeout = buffer_timeout

    def filter_map(
        self,
        function: Callable[[T], R | Awaitable[R | None] | None],
    ) -> AsyncFlow[R]:
        """Map items and discard results equal to None.

        Args:
            function: The callable applied by this operation.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        mapped = self.map(function)
        return cast(AsyncFlow[R], mapped.filter(lambda item: item is not None))

    def pluck(self, selector: Selector) -> AsyncFlow[Any]:
        """Select one field, index, attribute, or nested path from each item.

        Args:
            selector: A callable, field name, index, path, or expression used to select a value.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        return self.map(compile_selector(selector))

    pick = pluck

    def compact(self, selector: Selector | None = None) -> AsyncFlow[T]:
        """Drop None values, optionally selected from each item.

        Args:
            selector: A callable, field name, index, path, or expression used to select a value.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        select = bool if selector is None else compile_selector(selector)
        return self.filter(select)

    def filter_none(self) -> AsyncFlow[T]:
        """Drop None values, optionally selected from each item.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        return self.filter(lambda item: item is not None)

    def unique(self) -> AsyncFlow[T]:
        """Keep the first occurrence of each hashable value.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        return cast(AsyncFlow[T], self._append(_Unique(lambda item: item)))

    distinct = unique

    def unique_by(self, selector: Selector) -> AsyncFlow[T]:
        """Keep the first item for each selected key.

        Args:
            selector: A callable, field name, index, path, or expression used to select a value.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        return cast(AsyncFlow[T], self._append(_Unique(compile_selector(selector))))

    distinct_by = unique_by

    def attempt(self, function: Callable[[T], R | Awaitable[R]]) -> AsyncFlow[Result[R]]:
        """Map each item and wrap success or failure in a Result.

        Args:
            function: The callable applied by this operation.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """

        async def capture(item: T) -> Result[R]:
            try:
                return Ok(cast(R, await _resolve(function(item))))
            except Exception as error:
                return Err(error)

        return self.map(capture)

    def take(self, count: int) -> AsyncFlow[T]:
        """Emit at most count items and cancel pending upstream work.

        Args:
            count: The requested number of items.

        Returns:
            A new lazy `AsyncFlow` representing this operation.

        Raises:
            ValueError: If count is negative.
        """
        if count < 0:
            raise ValueError("count cannot be negative")
        return cast(AsyncFlow[T], self._append(_Take(count)))

    limit = take

    def drop(self, count: int) -> AsyncFlow[T]:
        """Skip count items before yielding the remainder.

        Args:
            count: The requested number of items.

        Returns:
            A new lazy `AsyncFlow` representing this operation.

        Raises:
            ValueError: If count is negative.
        """
        if count < 0:
            raise ValueError("count cannot be negative")
        return cast(AsyncFlow[T], self._append(_Drop(count)))

    skip = drop

    def take_while(self, predicate: Callable[[T], bool | Awaitable[bool]]) -> AsyncFlow[T]:
        """Emit the longest prefix that satisfies predicate.

        Args:
            predicate: A callable that decides whether an item matches.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        return cast(AsyncFlow[T], self._append(_TakeWhile(predicate)))

    def take_while_inclusive(
        self, predicate: Callable[[T], bool | Awaitable[bool]]
    ) -> AsyncFlow[T]:
        """Emit through the first item that fails predicate.

        Args:
            predicate: A callable that decides whether an item matches.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        return cast(AsyncFlow[T], self._append(_TakeWhileInclusive(predicate)))

    def drop_while(self, predicate: Callable[[T], bool | Awaitable[bool]]) -> AsyncFlow[T]:
        """Skip the longest prefix that satisfies predicate.

        Args:
            predicate: A callable that decides whether an item matches.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        return cast(AsyncFlow[T], self._append(_DropWhile(predicate)))

    def chunk(self, size: int) -> AsyncFlow[tuple[T, ...]]:
        """Group consecutive items into fixed-size tuples.

        Args:
            size: The requested window, chunk, or batch size.

        Returns:
            A new lazy `AsyncFlow` representing this operation.

        Raises:
            ValueError: If size is less than one.
        """
        if size < 1:
            raise ValueError("size must be at least 1")
        return cast(AsyncFlow[tuple[T, ...]], self._append(_Chunk(size)))

    batch = chunk

    def window(self, size: int, *, step: int = 1) -> AsyncFlow[tuple[T, ...]]:
        """Emit sliding tuples of size with the requested step.

        Args:
            size: The requested window, chunk, or batch size.
            step: The distance between consecutive windows or numeric increments.

        Returns:
            A new lazy `AsyncFlow` representing this operation.

        Raises:
            ValueError: If size or step is less than one.
        """
        if size <= 0:
            raise ValueError("window size must be positive")
        if step <= 0:
            raise ValueError("window step must be positive")
        return cast(AsyncFlow[tuple[T, ...]], self._append(_Window(size, step)))

    def pairwise(self) -> AsyncFlow[tuple[T, T]]:
        """Emit each adjacent pair of items.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        return cast(AsyncFlow[tuple[T, T]], self._append(_Pairwise()))

    def pair_map(
        self,
        function: Callable[[T, T], R | Awaitable[R]],
    ) -> AsyncFlow[R]:
        """Apply a two-argument function to each adjacent pair.

        Args:
            function: The callable applied by this operation.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        return self.pairwise().map(lambda pair: function(pair[0], pair[1]))

    def group_runs(self, key: Selector | None = None) -> AsyncFlow[tuple[T, ...]]:
        """Group consecutive items that share the same key.

        Args:
            key: The callable or selector used to derive a key.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        select = (lambda item: item) if key is None else compile_selector(key)
        return cast(AsyncFlow[tuple[T, ...]], self._append(_GroupRuns(select)))

    chunk_by = group_runs

    def enumerate(self, start: int = 0) -> AsyncFlow[tuple[int, T]]:
        """Pair each item with a consecutive index starting at start.

        Args:
            start: The first index, numeric value, or additive identity to use.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        return cast(
            AsyncFlow[tuple[int, T]],
            self._append(_Enumerate(operator.index(start))),
        )

    zip_with_index = enumerate

    def zip(
        self,
        other: AsyncIterable[U] | Iterable[U],
        *,
        strict: bool = False,
    ) -> AsyncFlow[tuple[T, U]]:
        """Pair items with another source until one side ends.

        Args:
            other: The other iterable, flow, value, or fallback used by the operation.
            strict: Whether invalid or empty input should raise instead of returning a fallback.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        operation = _Zip(_AsyncSource.from_value(other), strict)
        return cast(AsyncFlow[tuple[T, U]], self._append(operation))

    def zip_longest(
        self,
        other: AsyncIterable[U] | Iterable[U],
        *,
        fillvalue: Any = None,
    ) -> AsyncFlow[tuple[T | Any, U | Any]]:
        """Pair with another source until both sides end, filling missing values.

        Args:
            other: The other iterable, flow, value, or fallback used by the operation.
            fillvalue: The value used when one side of a longest zip is exhausted.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        operation = _ZipLongest(_AsyncSource.from_value(other), fillvalue)
        return cast(AsyncFlow[tuple[T | Any, U | Any]], self._append(operation))

    def intersperse(self, separator: T) -> AsyncFlow[T]:
        """Insert separator between consecutive items.

        Args:
            separator: The string inserted between adjacent string representations.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        return cast(AsyncFlow[T], self._append(_Intersperse(separator)))

    def concat(
        self,
        *others: AsyncIterable[T] | Iterable[T],
    ) -> AsyncFlow[T]:
        """Emit this flow followed by each supplied source.

        Args:
            *others: Additional sources combined with this pipeline.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        sources = tuple(_AsyncSource.from_value(other) for other in others)
        return cast(AsyncFlow[T], self._append(_Concat(sources)))

    def cross(
        self,
        other: AsyncIterable[U] | Iterable[U],
        *,
        max_right: int | None = None,
    ) -> AsyncFlow[tuple[T, U]]:
        """Emit the Cartesian product with a bounded or reiterable right side.

        Args:
            other: The other iterable, flow, value, or fallback used by the operation.
            max_right: The maximum right-side size allowed when buffering is required.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        if max_right is not None:
            max_right = operator.index(max_right)
            if max_right < 0:
                raise ValueError("max_right must be non-negative")
        return cast(
            AsyncFlow[tuple[T, U]],
            self._append(_Cross(_AsyncSource.from_value(other), max_right)),
        )

    cartesian = cross

    def scan(
        self,
        initial: R,
        function: Callable[[R, T], R | Awaitable[R]],
    ) -> AsyncFlow[R]:
        """Emit each left-to-right accumulator state.

        Unlike `reduce`, `scan` emits every intermediate accumulator state.

        Args:
            initial: The initial accumulator value. When omitted, the first item is used where
                supported.
            function: The callable applied by this operation.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        if not callable(function):
            raise TypeError("function must be callable")
        return cast(AsyncFlow[R], self._append(_Scan(initial, function)))

    def scan_right(
        self,
        initial: R,
        function: Callable[[T, R], R | Awaitable[R]],
        *,
        max_items: int | None = None,
    ) -> AsyncFlow[R]:
        """Emit accumulator states while combining items from right to left.

        Args:
            initial: The initial accumulator value. When omitted, the first item is used where
                supported.
            function: The callable applied by this operation.
            max_items: The maximum number of source items allowed in the right-side buffer.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        if not callable(function):
            raise TypeError("function must be callable")
        if max_items is not None:
            max_items = operator.index(max_items)
            if max_items < 0:
                raise ValueError("max_items must be non-negative")
        return cast(AsyncFlow[R], self._append(_ScanRight(initial, function, max_items)))

    def prepend(self, *values: T) -> AsyncFlow[T]:
        """Emit values before the items from this flow.

        Args:
            *values: Values supplied to this operation in encounter order.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        return cast(AsyncFlow[T], self._append(_Prepend(values)))

    def append(self, *values: T) -> AsyncFlow[T]:
        """Emit values after the items from this flow.

        Args:
            *values: Values supplied to this operation in encounter order.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        return cast(AsyncFlow[T], self._append(_Append(values)))

    def map_first(self, function: Callable[[T], T | Awaitable[T]]) -> AsyncFlow[T]:
        """Transform only the first item, if one exists.

        Args:
            function: The callable applied by this operation.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        return cast(AsyncFlow[T], self._append(_MapFirst(function)))

    def map_last(self, function: Callable[[T], T | Awaitable[T]]) -> AsyncFlow[T]:
        """Transform only the last item, if one exists.

        Args:
            function: The callable applied by this operation.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        return cast(AsyncFlow[T], self._append(_MapLast(function)))

    def collapse(
        self,
        collapsible: Callable[[T, T], bool | Awaitable[bool]],
        merger: Callable[[T, T], T | Awaitable[T]],
    ) -> AsyncFlow[T]:
        """Merge adjacent items while collapsible returns true.

        Args:
            collapsible: A callable deciding whether two adjacent items should be combined.
            merger: A callable that merges two downstream results.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        if not callable(collapsible) or not callable(merger):
            raise TypeError("collapsible and merger must be callable")
        return cast(AsyncFlow[T], self._append(_Collapse(collapsible, merger)))

    def fold(
        self,
        initializer: Callable[[], R | Awaitable[R]],
        function: Callable[[R, T], R | Awaitable[R]],
    ) -> AsyncFlow[R]:
        """Apply a Gatherer-compatible stateful fold.

        Args:
            initializer: A zero-argument callable that creates fresh mutable state.
            function: The callable applied by this operation.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        if not callable(initializer):
            raise TypeError("initializer must be callable")
        if not callable(function):
            raise TypeError("function must be callable")
        return cast(AsyncFlow[R], self._append(_Fold(initializer, function)))

    def batch_by_size(
        self,
        max_size: int,
        *,
        max_count: int | None = None,
        get_size: Callable[[T], int | Awaitable[int]] = _default_item_size,
        strict: bool = True,
    ) -> AsyncFlow[tuple[T, ...]]:
        """Build batches constrained by count and measured size.

        Args:
            max_size: The maximum measured size allowed in one batch.
            max_count: The maximum number of items emitted in one batch.
            get_size: A callable that returns the measured size of one item.
            strict: Whether invalid or empty input should raise instead of returning a fallback.

        Returns:
            A new lazy `AsyncFlow` representing this operation.
        """
        if max_size <= 0:
            raise ValueError("max_size must be positive")
        if max_count is not None and max_count <= 0:
            raise ValueError("max_count must be positive")
        if not callable(get_size):
            raise TypeError("get_size must be callable")
        operation = _BatchBySize(max_size, max_count, get_size, strict)
        return cast(AsyncFlow[tuple[T, ...]], self._append(operation))

    constrained_batches = batch_by_size


class _AsyncFlowFactory:
    """Callable factory for creating asynchronous flows and async sources."""

    __slots__ = ()

    def __call__(self, source: AsyncIterable[T] | Iterable[T]) -> AsyncFlow[T]:
        """Create a lazy async flow over a synchronous or asynchronous source.

        Args:
            source: The synchronous or asynchronous iterable consumed by the flow.

        Returns:
            A new lazy `AsyncFlow`.
        """
        return AsyncFlow(source)

    def defer(self, factory: Callable[[], AsyncIterable[T] | Iterable[T]]) -> AsyncFlow[T]:
        """Create a reusable async flow that calls factory for each iteration.

        Args:
            factory: A callable that opens a fresh source for every iteration.

        Returns:
            A new reusable lazy `AsyncFlow`.
        """
        return AsyncFlow._from_plan(_AsyncPlan(_AsyncSource.defer(factory)))

    def from_file(self, path: str | os.PathLike[str], *, encoding: str = "utf-8") -> AsyncFlow[str]:
        """Read a text file asynchronously and emit its lines.

        Args:
            path: The filesystem path to read from or write to.
            encoding: The text encoding used to open the file.

        Returns:
            A new reusable lazy `AsyncFlow`.
        """
        return AsyncFlow.from_file(path, encoding=encoding)

    def interval(self, seconds: float) -> AsyncFlow[int]:
        """Emit increasing integers separated by seconds.

        Args:
            seconds: The duration in seconds used by the timing operation.

        Returns:
            A new reusable lazy `AsyncFlow`.
        """
        return AsyncFlow.interval(seconds)

    def paginate(
        self,
        fetch_page: Callable[
            [C | None],
            tuple[AsyncIterable[R] | Iterable[R], C | None]
            | Awaitable[tuple[AsyncIterable[R] | Iterable[R], C | None]],
        ],
        *,
        start: C | None = None,
    ) -> AsyncFlow[R]:
        """Fetch pages lazily until the returned cursor is None.

        Args:
            fetch_page: An async callable that returns page items and the next cursor.
            start: The first index, numeric value, or additive identity to use.

        Returns:
            A new reusable lazy `AsyncFlow`.
        """
        return AsyncFlow.paginate(fetch_page, start=start)


aflow = _AsyncFlowFactory()
AsyncStream = AsyncFlow
