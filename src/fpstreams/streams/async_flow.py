"""Lazy asynchronous pipelines and real-time terminal operations."""

from __future__ import annotations

import asyncio
import operator
import os
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable
from typing import Any, Generic, TypeVar, cast

from ..expressions.selectors import Selector, compile_selector
from ..planning.async_ import (
    AsyncLogicalPlan,
    AsyncOperation,
    _Append,
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
    _Prefetch,
    _Prepend,
    _Scan,
    _ScanRight,
    _SessionWindow,
    _SwitchMap,
    _Take,
    _TakeWhile,
    _TakeWhileInclusive,
    _Tap,
    _Throttle,
    _Timeout,
    _to_async_iterator,
    _Unique,
    _Window,
    _Zip,
    _ZipLongest,
)
from ..planning.async_utils import _resolve, closing_async_iterators
from ..planning.explain import AsyncPlanExplanation
from ..planning.semantics import (
    AsyncTerminalName,
    Cardinality,
    OrderingGuarantee,
    Replayability,
    StreamFacts,
    TerminationEvidence,
)
from ..primitives.result import Err, Ok, Result
from .async_terminals import AsyncFlowTerminalsMixin

T = TypeVar("T")
R = TypeVar("R")
C = TypeVar("C")
U = TypeVar("U")
_NO_QUEUE_STOP = object()


def _default_item_size(value: Any) -> int:
    """Measure an item with `len()` for size-constrained batching."""
    return len(value)


class AsyncFlow(AsyncFlowTerminalsMixin[T], Generic[T]):
    """An async pipeline that opens its sync or async source when consumption begins."""

    __slots__ = ("_logical_plan",)

    def __init__(self, source: AsyncIterable[T] | Iterable[T]) -> None:
        """Own a sync or async iterable, preserving one-shot iterator consumption semantics."""
        self._logical_plan: AsyncLogicalPlan[T] = AsyncLogicalPlan(_AsyncSource.from_value(source))

    @staticmethod
    def _from_logical(plan: AsyncLogicalPlan[R]) -> AsyncFlow[R]:
        """Construct an `AsyncFlow` around an existing immutable plan without rewrapping it."""
        instance: AsyncFlow[R] = object.__new__(AsyncFlow)
        instance._logical_plan = plan
        return instance

    def _retained_identity_terminal(
        self,
        terminal: str,
    ) -> Awaitable[Any] | None:
        """Return deferred direct work for this exact retained synchronous sequence."""
        from ._async_identity import try_retained_identity_terminal

        if type(self) is not AsyncFlow:
            return None
        return try_retained_identity_terminal(self, terminal)

    def explain(self, terminal: AsyncTerminalName = "iterate") -> AsyncPlanExplanation:
        """Describe stream facts and terminal completion risks without consuming the source.

        Args:
            terminal: Async terminal whose completion requirements should be analyzed.

        Returns:
            A lazy explanation view over the current plan and selected terminal.

        Raises:
            ValueError: If `terminal` is not a supported async terminal name.
        """
        if terminal not in {
            "iterate",
            "list",
            "count",
            "sum",
            "min",
            "max",
            "minmax",
            "statistics",
            "aggregate",
            "first",
            "last",
            "any",
            "all",
        }:
            raise ValueError(f"unknown async terminal: {terminal!r}")
        return AsyncPlanExplanation(self._logical_plan, terminal)

    @staticmethod
    def of(*items: R) -> AsyncFlow[R]:
        """Create an async flow from positional items.

        Args:
            *items: Positional values to emit in argument order.

        Returns:
            A reusable async flow that emits `items` in argument order.
        """
        return AsyncFlow[R](items)

    @staticmethod
    def from_iterable(source: Iterable[R]) -> AsyncFlow[R]:
        """Create an async flow over a synchronous iterable.

        Args:
            source: Synchronous iterable adapted to async iteration when consumed.

        Returns:
            An async flow that emits each item from `source`; iterator instances are one-shot.
        """
        return AsyncFlow[R](source)

    @staticmethod
    def from_aiterable(source: AsyncIterable[R]) -> AsyncFlow[R]:
        """Create an async flow over an asynchronous iterable.

        Args:
            source: Asynchronous iterable opened when consumption begins.

        Returns:
            An async flow over `source`; async iterator instances are one-shot.
        """
        return AsyncFlow[R](source)

    @staticmethod
    def from_queue(
        queue: asyncio.Queue[R],
        *,
        stop: object = _NO_QUEUE_STOP,
    ) -> AsyncFlow[R]:
        """Create a non-owning, one-shot flow over an asyncio queue.

        Values are requested lazily and emitted in ``queue.get()`` return order.
        When provided, ``stop`` ends the flow by identity and is not emitted.
        On Python 3.13+, queue shutdown ends the flow normally.
        Values already removed from the queue are not returned when downstream
        stops early. This adapter never calls ``task_done()``, including for the
        hidden ``stop``; do not use pull-ahead ``prefetch()`` when relying on
        ``Queue.join()`` or per-item acknowledgements. Queue and producer ownership
        remain with the caller.
        """

        async def items() -> AsyncIterator[R]:
            """Receive until the optional identity sentinel is observed."""
            queue_shutdown = getattr(asyncio, "QueueShutDown", None)
            while True:
                try:
                    item = await queue.get()
                except Exception as error:
                    if isinstance(queue_shutdown, type) and isinstance(error, queue_shutdown):
                        return
                    raise
                if stop is not _NO_QUEUE_STOP and item is stop:
                    return
                yield item

        return AsyncFlow._from_logical(
            AsyncLogicalPlan(
                _AsyncSource(
                    items,
                    reiterable=False,
                    facts=StreamFacts(
                        TerminationEvidence.UNKNOWN,
                        Cardinality.unknown(),
                        Replayability.ONE_SHOT,
                        OrderingGuarantee.ORDERED,
                    ),
                )
            )
        )

    @staticmethod
    def from_file(path: str | os.PathLike[str], *, encoding: str = "utf-8") -> AsyncFlow[str]:
        """Read a text file asynchronously and emit lines without trailing newlines.

        Args:
            path: Text file opened only when the returned flow is consumed.
            encoding: Encoding used by `aiofiles.open`.

        Returns:
            A reusable async flow of lines with trailing CR and LF characters removed.
        """

        async def lines() -> AsyncIterator[str]:
            """Open the file lazily and yield lines with newline terminators removed."""
            try:
                import aiofiles  # type: ignore[import-untyped]
            except ImportError:
                raise ImportError("AsyncFlow.from_file() requires the 'async' extra") from None
            async with aiofiles.open(path, encoding=encoding) as file:
                async for line in file:
                    yield line.rstrip("\r\n")

        return AsyncFlow._from_logical(AsyncLogicalPlan(_AsyncSource.defer(lines)))

    @staticmethod
    def interval(seconds: float) -> AsyncFlow[int]:
        """Emit increasing integers separated by seconds.

        Args:
            seconds: Delay before the first integer and between later integers.

        Returns:
            A reusable, infinite flow emitting `0, 1, 2, ...` at the requested interval.
        """
        if seconds < 0:
            raise ValueError("seconds cannot be negative")

        async def ticks() -> AsyncIterator[int]:
            """Sleep before each tick and emit increasing integers indefinitely."""
            current = 0
            while True:
                await asyncio.sleep(seconds)
                yield current
                current += 1

        return AsyncFlow._from_logical(
            AsyncLogicalPlan(
                _AsyncSource.defer(
                    ticks,
                    facts=StreamFacts(
                        TerminationEvidence.PROVEN_INFINITE,
                        Cardinality.unknown(),
                        Replayability.REOPENABLE,
                        OrderingGuarantee.ORDERED,
                    ),
                )
            )
        )

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
            fetch_page: Sync or async callable receiving the current cursor and returning
                `(page, next_cursor)`; each page may be sync or async iterable.
            start: Cursor passed to the first `fetch_page` call.

        Returns:
            A reusable async flow that flattens each page and stops after a `None` next cursor.
        """

        async def items() -> AsyncIterator[R]:
            """Fetch and flatten pages until the page function returns a None cursor."""
            cursor = start
            while True:
                page, next_cursor = await _resolve(fetch_page(cursor))
                page_iterator = _to_async_iterator(page)
                async with closing_async_iterators((page_iterator,)):
                    async for item in page_iterator:
                        yield item
                if next_cursor is None:
                    return
                cursor = next_cursor

        return AsyncFlow._from_logical(AsyncLogicalPlan(_AsyncSource.defer(items)))

    def __aiter__(self) -> AsyncIterator[T]:
        """Execute the stored async plan and return its managed output iterator."""
        from ..execution.async_scheduler import execute_async_physical
        from ..physical.async_plan import compile_async_query
        from ..runtime.report import _record_async_plan

        physical = compile_async_query(self._query("iterate"))
        _record_async_plan()
        return cast(AsyncIterator[T], execute_async_physical(physical))

    def _query(self, terminal: AsyncTerminalName) -> Any:
        """Pair the immutable logical plan with one validated async terminal name."""
        from ..physical.async_plan import AsyncQuery

        return AsyncQuery(self._logical_plan, terminal)

    def _append(self, operation: AsyncOperation) -> AsyncFlow[Any]:
        """Return a new AsyncFlow whose immutable plan includes `operation`."""
        return self._from_logical(
            AsyncLogicalPlan(
                self._logical_plan.source,
                (*self._logical_plan.operations, operation),
            )
        )

    def map_async(
        self,
        function: Callable[[T], R | Awaitable[R]],
        *,
        concurrency: int = 8,
        ordered: bool = True,
        timeout: float | None = None,
        buffer: int | None = None,
    ) -> AsyncFlow[R]:
        """Map items with bounded concurrency, buffering, ordering, and timeout.

        Concurrency is bounded. Ordered mode delays later completed results until earlier inputs
        finish, while the buffer permits completed work to refill active mapper slots.

        Args:
            function: Sync or async mapper called once for each source item.
            concurrency: Maximum mapper calls in flight.
            ordered: Emit in source order when true, otherwise in task-completion order.
            timeout: Optional per-item deadline covering mapper invocation and awaiting its result.
            buffer: Maximum submitted results not yet emitted, or twice ``concurrency`` by default.

        Returns:
            An async flow of mapped values with bounded work and cleanup on early exit.

        Raises:
            ValueError: If concurrency or buffer is less than one.
        """
        # Concurrency is stored in the plan and enforced when the flow is consumed.
        if concurrency < 1:
            raise ValueError("concurrency must be at least 1")
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be positive")
        if buffer is None:
            buffer = 2 * concurrency
        if buffer < 1:
            raise ValueError("buffer must be at least 1")
        operation = _MapAsync(function, concurrency, ordered, timeout, buffer)
        return cast(AsyncFlow[R], self._append(operation))

    def map(self, function: Callable[[T], R | Awaitable[R]]) -> AsyncFlow[R]:
        """Apply a synchronous or asynchronous function to each item in order.

        `map` preserves encounter order and awaits awaitable results; work starts only when the
        flow is consumed.

        Args:
            function: Sync or async mapper called once per item; awaitable results are resolved.

        Returns:
            An async flow of mapped values in source order, with one mapper call in flight.
        """
        return self.map_async(function, concurrency=1)

    def filter(self, predicate: Callable[[T], bool | Awaitable[bool]]) -> AsyncFlow[T]:
        """Keep items for which the sync or async predicate is truthy.

        The predicate may be synchronous or asynchronous and is evaluated in encounter order.

        Args:
            predicate: Sync or async callable resolved for each item; truthy results retain it.

        Returns:
            An async flow containing only items whose resolved predicate result is truthy.
        """
        # The predicate is resolved during async iteration, not while building the flow.
        return cast(AsyncFlow[T], self._append(_Filter(predicate)))

    def where(self, predicate: Callable[[T], bool | Awaitable[bool]]) -> AsyncFlow[T]:
        """Keep items for which predicate returns a truthy value.

        This is an alias-style filtering operation for a synchronous or asynchronous predicate.

        Args:
            predicate: Sync or async callable resolved for each item; truthy results retain it.

        Returns:
            The same ordered async filtering pipeline produced by `filter(predicate)`.
        """
        return self.filter(predicate)

    def reject(self, predicate: Callable[[T], bool | Awaitable[bool]]) -> AsyncFlow[T]:
        """Drop items for which the sync or async predicate is truthy.

        Args:
            predicate: Sync or async callable resolved for each item; truthy results drop it.

        Returns:
            An async flow containing only items whose resolved predicate result is falsey.
        """

        async def inverse(item: T) -> bool:
            """Await the predicate when needed and negate its truth value."""
            return not await _resolve(predicate(item))

        return self.filter(inverse)

    def tap(self, action: Callable[[T], Any]) -> AsyncFlow[T]:
        """Run a sync or async side effect while passing each item through.

        Args:
            action: Sync or async side effect resolved before the original item is emitted.

        Returns:
            An async flow that passes every item through unchanged after running `action`.
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
            function: Sync or async mapper returning a sync or async iterable for each item.

        Returns:
            An async flow that drains each mapped iterable in source order before mapping the next
            item.
        """
        return cast(AsyncFlow[R], self._append(_FlatMap(function)))

    def merge(
        self,
        *others: AsyncIterable[T] | Iterable[T],
    ) -> AsyncFlow[T]:
        """Merge this flow with other sources in completion order.

        Args:
            *others: Sync or async sources to interleave with this flow.

        Returns:
            An async flow that emits each source's next item as its pull completes.
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
            *others: Sync or async sources whose latest values join this flow's latest value.

        Returns:
            A flow of latest-value tuples in source argument order. Emission starts only after every
            source has produced once; completed sources retain their final value.
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
            function: Sync or async mapper returning a sync or async inner source.
            concurrency: Shared maximum for inner sources being opened or actively consumed.

        Returns:
            An async flow interleaving inner items in completion order under the concurrency cap.
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
            An async flow that emits only from the most recently mapped inner source.

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
            An async flow that waits once before its first upstream pull, then forwards normally.

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
            max_count: Maximum emissions permitted in each rolling `per`-second window.
            per: The positive sliding-window duration in seconds.

        Returns:
            An async flow that delays, but never drops, items to enforce the rolling rate limit.

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
            An async flow that emits the first item immediately and delays later items as needed.

        Raises:
            ValueError: If seconds is not positive.
        """
        if seconds <= 0:
            raise ValueError("seconds must be positive")
        return self.throttle(1, per=seconds)

    def timeout(self, seconds: float) -> AsyncFlow[T]:
        """Fail when the next item takes longer than seconds.

        Args:
            seconds: Maximum wait for each individual upstream `anext` call.

        Returns:
            An async flow that raises `TimeoutError` and cancels an overdue item pull.
        """
        if seconds <= 0:
            raise ValueError("seconds must be positive")
        return cast(AsyncFlow[T], self._append(_Timeout(seconds)))

    def debounce(self, seconds: float) -> AsyncFlow[T]:
        """Emit an item only after the source stays quiet for seconds.

        Args:
            seconds: Quiet interval required before the latest pending item is emitted.

        Returns:
            An async flow that drops superseded pending items and flushes the latest item when the
            source completes.
        """
        if seconds < 0:
            raise ValueError("seconds cannot be negative")
        return cast(AsyncFlow[T], self._append(_Debounce(seconds)))

    def buffer_timeout(self, max_count: int, seconds: float) -> AsyncFlow[tuple[T, ...]]:
        """Flush a tuple when it reaches max_count or seconds elapse.

        Args:
            max_count: Item count that flushes the current batch immediately.
            seconds: Maximum time from a batch's first item until that batch is flushed.

        Returns:
            An async flow of non-empty tuples flushed by count, timeout, or source completion.
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

    def session_window(
        self,
        idle_for: float,
        *,
        max_count: int,
    ) -> AsyncFlow[tuple[T, ...]]:
        """Group consecutive items until the source stays idle or the hard count cap is reached.

        The processing-time idle timer is reset after every accepted item. Source completion
        flushes the final non-empty tuple.

        Args:
            idle_for: Positive quiet interval that closes the current session.
            max_count: Required maximum number of items retained in one session.

        Returns:
            An async flow of non-empty session tuples in encounter order.
        """
        try:
            count = operator.index(max_count)
        except TypeError:
            raise TypeError("max_count must be an integer") from None
        if count < 1:
            raise ValueError("max_count must be at least 1")
        if not idle_for > 0:
            raise ValueError("idle_for must be positive")
        return cast(
            AsyncFlow[tuple[T, ...]],
            self._append(_SessionWindow(idle_for, count)),
        )

    def prefetch(self, capacity: int) -> AsyncFlow[T]:
        """Pull upstream values ahead under an explicit bounded buffer.

        Args:
            capacity: Maximum accepted upstream values not yet handed to downstream.

        Returns:
            An async flow preserving every value in encounter order.

        Raises:
            TypeError: If capacity does not implement the integer index protocol.
            ValueError: If capacity is less than one.
        """
        try:
            bound = operator.index(capacity)
        except TypeError:
            raise TypeError("capacity must be an integer") from None
        if bound < 1:
            raise ValueError("capacity must be at least 1")
        return cast(AsyncFlow[T], self._append(_Prefetch(bound)))

    def filter_map(
        self,
        function: Callable[[T], R | Awaitable[R | None] | None],
    ) -> AsyncFlow[R]:
        """Map items and discard results equal to None.

        Args:
            function: Sync or async mapper returning one output value or `None`.

        Returns:
            An async flow of non-`None` mapped results; falsey values such as `0` are retained.
        """
        mapped = self.map(function)
        return cast(AsyncFlow[R], mapped.filter(lambda item: item is not None))

    def pluck(self, selector: Selector) -> AsyncFlow[Any]:
        """Select one field, index, attribute, or nested path from each item.

        Args:
            selector: Callable, field name, index, path, or expression evaluated for each output.

        Returns:
            An async flow containing the value selected from each source item.
        """
        return self.map(compile_selector(selector))

    pick = pluck

    def compact(self, selector: Selector | None = None) -> AsyncFlow[T]:
        """Drop items whose own or selected value is falsey.

        Args:
            selector: Optional callable, field name, index, path, or expression whose truth value
                determines whether the original item is retained.

        Returns:
            An async flow retaining only items with truthy selected values; without a selector,
            falsey items such as `None`, `0`, and empty containers are omitted.
        """
        select = bool if selector is None else compile_selector(selector)
        return self.filter(select)

    def filter_none(self) -> AsyncFlow[T]:
        """Drop only items equal to `None`, retaining every other falsey value.

        Returns:
            An async flow containing every non-`None` source item.
        """
        return self.filter(lambda item: item is not None)

    def unique(self) -> AsyncFlow[T]:
        """Keep the first occurrence of each value in source order.

        Returns:
            An async flow containing the first occurrence of each distinct value; unhashable values
            are compared by equality.
        """
        return cast(AsyncFlow[T], self._append(_Unique(lambda item: item)))

    distinct = unique

    def unique_by(self, selector: Selector) -> AsyncFlow[T]:
        """Keep the first item for each selected key.

        Args:
            selector: Callable, field name, index, path, or expression producing each uniqueness
                key; callable results may be awaitable.

        Returns:
            An async flow containing the first item for each distinct resolved selected key.
        """
        return cast(AsyncFlow[T], self._append(_Unique(compile_selector(selector))))

    distinct_by = unique_by

    def attempt(self, function: Callable[[T], R | Awaitable[R]]) -> AsyncFlow[Result[R]]:
        """Map each item and wrap success or failure in a Result.

        Args:
            function: Sync or async mapper that may raise an `Exception`.

        Returns:
            An async flow of `Ok` mapped values and `Err` objects for raised exceptions.
        """

        async def capture(item: T) -> Result[R]:
            """Await the mapping and wrap success in Ok or a raised exception in Err."""
            try:
                return Ok(cast(R, await _resolve(function(item))))
            except Exception as error:
                return Err(error)

        return self.map(capture)

    def take(self, count: int) -> AsyncFlow[T]:
        """Emit at most count items and cancel pending upstream work.

        Args:
            count: Maximum number of leading items to emit.

        Returns:
            An async flow containing only the first `count` items, with upstream work cancelled
            and closed after the limit.

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
            count: Number of leading source items to consume without emitting.

        Returns:
            An async flow containing every source item after the first `count`.

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
            predicate: Sync or async callable resolved on leading items until its first falsey
                result.

        Returns:
            An async flow ending before the first item whose resolved predicate is falsey.
        """
        return cast(AsyncFlow[T], self._append(_TakeWhile(predicate)))

    def take_while_inclusive(
        self, predicate: Callable[[T], bool | Awaitable[bool]]
    ) -> AsyncFlow[T]:
        """Emit through the first item that fails predicate.

        Args:
            predicate: Sync or async callable resolved on leading items through its first falsey
                result.

        Returns:
            An async flow ending after emitting the first item whose resolved predicate is falsey.
        """
        return cast(AsyncFlow[T], self._append(_TakeWhileInclusive(predicate)))

    def drop_while(self, predicate: Callable[[T], bool | Awaitable[bool]]) -> AsyncFlow[T]:
        """Skip the longest prefix that satisfies predicate.

        Args:
            predicate: Sync or async callable resolved only while leading items are truthy.

        Returns:
            An async flow beginning with the first falsey-predicate item; later items are emitted
            without further predicate calls.
        """
        return cast(AsyncFlow[T], self._append(_DropWhile(predicate)))

    def chunk(self, size: int) -> AsyncFlow[tuple[T, ...]]:
        """Group consecutive items into fixed-size tuples.

        Args:
            size: Maximum number of consecutive items in each tuple.

        Returns:
            An async flow of non-overlapping tuples, including a final shorter tuple when needed.

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
            size: Number of items in each full window.
            step: Number of source items consumed between successive windows.

        Returns:
            A flow of full sliding windows; a non-empty source shorter than `size` produces one
            partial window, but no trailing partial window is emitted otherwise.

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
            An async flow of overlapping `(previous, current)` pairs; fewer than two items emit
            nothing.
        """
        return cast(AsyncFlow[tuple[T, T]], self._append(_Pairwise()))

    def pair_map(
        self,
        function: Callable[[T, T], R | Awaitable[R]],
    ) -> AsyncFlow[R]:
        """Apply a two-argument function to each adjacent pair.

        Args:
            function: Sync or async mapper called as `function(previous, current)`.

        Returns:
            An async flow containing one resolved mapped result per adjacent pair.
        """
        return self.pairwise().map(lambda pair: function(pair[0], pair[1]))

    def group_runs(self, key: Selector | None = None) -> AsyncFlow[tuple[T, ...]]:
        """Group consecutive items that share the same key.

        Args:
            key: Optional selector for run identity; adjacent items themselves are compared when
                omitted. Callable results may be awaitable.

        Returns:
            An async flow of non-empty tuples, one for each contiguous run of equal resolved keys.
        """
        select = (lambda item: item) if key is None else compile_selector(key)
        return cast(AsyncFlow[tuple[T, ...]], self._append(_GroupRuns(select)))

    chunk_by = group_runs

    def enumerate(self, start: int = 0) -> AsyncFlow[tuple[int, T]]:
        """Pair each item with a consecutive index starting at start.

        Args:
            start: Integer-compatible index paired with the first source item.

        Returns:
            An async flow of `(index, item)` pairs with consecutive integer indices.
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
            other: Sync or async source providing the right-hand item in each pair.
            strict: Raise `ValueError` during consumption when the two sources have different
                lengths.

        Returns:
            An async flow of pairs ending with the shorter source unless `strict` is true.
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
            other: Sync or async source providing right-hand values.
            fillvalue: Substitute used on whichever source is exhausted first.

        Returns:
            An async flow of pairs whose length matches the longer source.
        """
        operation = _ZipLongest(_AsyncSource.from_value(other), fillvalue)
        return cast(AsyncFlow[tuple[T | Any, U | Any]], self._append(operation))

    def intersperse(self, separator: T) -> AsyncFlow[T]:
        """Insert separator between consecutive items.

        Args:
            separator: Item emitted once between each pair of adjacent source items.

        Returns:
            An async flow with `separator` between source items and never at either boundary.
        """
        return cast(AsyncFlow[T], self._append(_Intersperse(separator)))

    def concat(
        self,
        *others: AsyncIterable[T] | Iterable[T],
    ) -> AsyncFlow[T]:
        """Emit this flow followed by each supplied source.

        Args:
            *others: Sync or async sources opened and drained after this flow, in argument order.

        Returns:
            An async flow that drains this source and then each additional source in order.
        """
        sources = tuple(_AsyncSource.from_value(other) for other in others)
        return cast(AsyncFlow[T], self._append(_Concat(sources)))

    def cross(
        self,
        other: AsyncIterable[U] | Iterable[U],
        *,
        max_right: int | None = None,
    ) -> AsyncFlow[tuple[T, U]]:
        """Buffer another source once and emit a left-major Cartesian product.

        Args:
            other: Sync or async source buffered after the first left item arrives.
            max_right: Optional maximum number of right-side items that may be buffered.

        Returns:
            An async flow of `(left, right)` pairs with every right item repeated for each left
            item.

        Raises:
            BufferLimitError: During consumption if `other` contains more than `max_right` items.
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
        """Emit each resolved left-to-right accumulator state after consuming one item.

        Unlike `reduce`, `scan` emits every intermediate accumulator state.

        Args:
            initial: Accumulator passed to the first callback; it is not emitted by itself.
            function: Sync or async callback invoked as `function(state, item)`.

        Returns:
            An async flow with one accumulated state for every source item.
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
        """Buffer the source, fold right, and emit resolved states in source order.

        Args:
            initial: Accumulator passed to the rightmost callback; it is not emitted by itself.
            function: Sync or async callback invoked as `function(item, state)` from right to
                left.
            max_items: Optional maximum number of source items that may be buffered.

        Returns:
            An async flow with one right-fold state per source item, ordered like the source.

        Raises:
            BufferLimitError: During consumption if the source exceeds `max_items`.
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
            *values: Items emitted before the first source item, in argument order.

        Returns:
            An async flow containing `values` followed by every source item.
        """
        return cast(AsyncFlow[T], self._append(_Prepend(values)))

    def append(self, *values: T) -> AsyncFlow[T]:
        """Emit values after the items from this flow.

        Args:
            *values: Items emitted after the source completes, in argument order.

        Returns:
            An async flow containing every source item followed by `values`.
        """
        return cast(AsyncFlow[T], self._append(_Append(values)))

    def map_first(self, function: Callable[[T], T | Awaitable[T]]) -> AsyncFlow[T]:
        """Transform only the first item, if one exists.

        Args:
            function: Sync or async mapper for the first item; never called for an empty source.

        Returns:
            An async flow with only its first item replaced by the resolved mapped value.
        """
        return cast(AsyncFlow[T], self._append(_MapFirst(function)))

    def map_last(self, function: Callable[[T], T | Awaitable[T]]) -> AsyncFlow[T]:
        """Transform only the last item, if one exists.

        Args:
            function: Sync or async mapper for the final item; never called for an empty source.

        Returns:
            An async flow with only its final item replaced by the resolved mapped value.
        """
        return cast(AsyncFlow[T], self._append(_MapLast(function)))

    def collapse(
        self,
        collapsible: Callable[[T, T], bool | Awaitable[bool]],
        merger: Callable[[T, T], T | Awaitable[T]],
    ) -> AsyncFlow[T]:
        """Merge adjacent items while collapsible returns true.

        Args:
            collapsible: Sync or async predicate called on neighboring original items.
            merger: Sync or async callback combining the current run aggregate with the next item.

        Returns:
            An async flow containing one resolved aggregate per contiguous collapsible run.
        """
        if not callable(collapsible) or not callable(merger):
            raise TypeError("collapsible and merger must be callable")
        return cast(AsyncFlow[T], self._append(_Collapse(collapsible, merger)))

    def fold(
        self,
        initializer: Callable[[], R | Awaitable[R]],
        function: Callable[[R, T], R | Awaitable[R]],
    ) -> AsyncFlow[R]:
        """Reduce the whole source and emit one resolved accumulator.

        Args:
            initializer: Sync or async callable invoked once per evaluation for initial state.
            function: Sync or async callback invoked as `function(state, item)`.

        Returns:
            An async flow emitting exactly one final state, including the initializer for an empty
            source.
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
            max_size: Maximum sum of resolved item sizes in a normal batch.
            max_count: Optional maximum item count per batch.
            get_size: Sync or async callable returning a non-negative integer size; defaults to
                `len`.
            strict: Raise when one item exceeds `max_size`; when false, emit it in an oversized
                singleton batch.

        Returns:
            An async flow of non-empty tuples packed within both limits, except for non-strict
            oversized singleton items.
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


_CANONICAL_ASYNC_FLOW_AITER = AsyncFlow.__aiter__


class _AsyncFlowFactory:
    """Callable factory for creating asynchronous flows and async sources."""

    __slots__ = ()

    def __call__(self, source: AsyncIterable[T] | Iterable[T]) -> AsyncFlow[T]:
        """Create a lazy async flow over a synchronous or asynchronous source.

        Args:
            source: The synchronous or asynchronous iterable consumed by the flow.

        Returns:
            An async flow that adapts and emits items from `source` when consumed.
        """
        return AsyncFlow(source)

    def defer(self, factory: Callable[[], AsyncIterable[T] | Iterable[T]]) -> AsyncFlow[T]:
        """Create a reusable async flow that calls factory for each iteration.

        Args:
            factory: Called for each evaluation and returns that evaluation's sync or async source.

        Returns:
            A reusable async flow that invokes `factory` separately for each evaluation.
        """
        return AsyncFlow._from_logical(AsyncLogicalPlan(_AsyncSource.defer(factory)))

    def from_queue(
        self,
        queue: asyncio.Queue[R],
        *,
        stop: object = _NO_QUEUE_STOP,
    ) -> AsyncFlow[R]:
        """Create a non-owning, one-shot flow emitting raw values in ``queue.get()`` order.

        Python 3.13+ queue shutdown ends normally; neither raw values nor the hidden ``stop``
        call ``task_done()``; avoid ``prefetch()`` with ``Queue.join()`` or per-item
        acknowledgements.
        """
        return AsyncFlow.from_queue(queue, stop=stop)

    def from_file(self, path: str | os.PathLike[str], *, encoding: str = "utf-8") -> AsyncFlow[str]:
        """Read a text file asynchronously and emit its lines.

        Args:
            path: Text file opened when the returned flow is consumed.
            encoding: Encoding used by `aiofiles.open`.

        Returns:
            A reusable async flow of lines with trailing CR and LF characters removed.
        """
        return AsyncFlow.from_file(path, encoding=encoding)

    def interval(self, seconds: float) -> AsyncFlow[int]:
        """Emit increasing integers separated by seconds.

        Args:
            seconds: Delay before the first integer and between later integers.

        Returns:
            A reusable, infinite async flow emitting `0, 1, 2, ...`.
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
            fetch_page: Sync or async callable returning `(page, next_cursor)` for each cursor.
            start: Cursor passed to the first page request.

        Returns:
            A reusable async flow that flattens pages through the first `None` next cursor.
        """
        return AsyncFlow.paginate(fetch_page, start=start)


aflow = _AsyncFlowFactory()
AsyncStream = AsyncFlow
