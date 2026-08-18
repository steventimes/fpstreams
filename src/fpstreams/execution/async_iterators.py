"""Sequential async transformations with explicit upstream ownership.

Each generator closes the source iterator in a finally block, so exhaustion,
failure, cancellation, and early downstream close all propagate cleanup upstream.
Operators that open nested sources also close those iterators at their local scope.
"""

from __future__ import annotations

import operator
import sys
from collections import deque
from collections.abc import AsyncIterable, AsyncIterator
from typing import Any

from ..errors import BufferLimitError
from ..planning.async_ import (
    _Append,
    _BatchBySize,
    _Chunk,
    _Collapse,
    _Concat,
    _Cross,
    _Drop,
    _DropWhile,
    _Enumerate,
    _Filter,
    _FlatMap,
    _Fold,
    _GroupRuns,
    _Intersperse,
    _MapFirst,
    _MapLast,
    _Prepend,
    _Scan,
    _ScanRight,
    _Take,
    _TakeWhile,
    _TakeWhileInclusive,
    _Tap,
    _Unique,
    _Window,
    _Zip,
    _ZipLongest,
)
from ..planning.async_utils import _resolve, close_async_iterators


async def _filter(source: AsyncIterator[Any], operation: _Filter) -> AsyncIterator[Any]:
    """Yield items whose possibly awaitable predicate resolves truthy."""
    try:
        async for item in source:
            if await _resolve(operation.predicate(item)):
                yield item
    finally:
        await close_async_iterators((source,), active_error=sys.exception())


async def _tap(source: AsyncIterator[Any], operation: _Tap) -> AsyncIterator[Any]:
    """Resolve the side-effect action before yielding each original item unchanged."""
    try:
        async for item in source:
            await _resolve(operation.action(item))
            yield item
    finally:
        await close_async_iterators((source,), active_error=sys.exception())


async def _flat_map(source: AsyncIterator[Any], operation: _FlatMap) -> AsyncIterator[Any]:
    """Flatten each possibly awaitable mapped iterable in source order.

    Async nested iterators are awaited and closed with async cleanup; synchronous
    nested iterators are iterated directly and closed when they support close.
    """
    try:
        async for item in source:
            nested = await _resolve(operation.function(item))
            if isinstance(nested, AsyncIterable):
                nested_iterator = nested.__aiter__()
                try:
                    async for nested_item in nested_iterator:
                        yield nested_item
                finally:
                    await close_async_iterators((nested_iterator,), active_error=sys.exception())
            else:
                nested_iterator = iter(nested)
                try:
                    yield_from = nested_iterator
                    for nested_item in yield_from:
                        yield nested_item
                finally:
                    close = getattr(nested_iterator, "close", None)
                    if callable(close):
                        close()
    finally:
        await close_async_iterators((source,), active_error=sys.exception())


async def _take(source: AsyncIterator[Any], operation: _Take) -> AsyncIterator[Any]:
    """Yield at most count items, consuming nothing when count is zero."""
    try:
        if operation.count == 0:
            return
        seen = 0
        async for item in source:
            yield item
            seen += 1
            if seen >= operation.count:
                break
    finally:
        await close_async_iterators((source,), active_error=sys.exception())


async def _drop(source: AsyncIterator[Any], operation: _Drop) -> AsyncIterator[Any]:
    """Consume the first count items, then yield the remainder unchanged."""
    try:
        seen = 0
        async for item in source:
            if seen < operation.count:
                seen += 1
                continue
            yield item
    finally:
        await close_async_iterators((source,), active_error=sys.exception())


async def _take_while(source: AsyncIterator[Any], operation: _TakeWhile) -> AsyncIterator[Any]:
    """Yield the leading truthy-predicate run, consuming but omitting its first failure."""
    try:
        async for item in source:
            if not await _resolve(operation.predicate(item)):
                return
            yield item
    finally:
        await close_async_iterators((source,), active_error=sys.exception())


async def _take_while_inclusive(
    source: AsyncIterator[Any], operation: _TakeWhileInclusive
) -> AsyncIterator[Any]:
    """Yield through the first predicate failure, including that terminating item."""
    try:
        async for item in source:
            yield item
            if not await _resolve(operation.predicate(item)):
                return
    finally:
        await close_async_iterators((source,), active_error=sys.exception())


async def _drop_while(source: AsyncIterator[Any], operation: _DropWhile) -> AsyncIterator[Any]:
    """Discard the leading truthy-predicate run, then yield all remaining items.

    The predicate is no longer called after the first false result.
    """
    try:
        dropping = True
        async for item in source:
            if dropping:
                if await _resolve(operation.predicate(item)):
                    continue
                dropping = False
            yield item
    finally:
        await close_async_iterators((source,), active_error=sys.exception())


async def _chunk(source: AsyncIterator[Any], operation: _Chunk) -> AsyncIterator[Any]:
    """Group consecutive items into tuples, including a final tuple shorter than size."""
    try:
        batch: list[Any] = []
        async for item in source:
            batch.append(item)
            if len(batch) == operation.size:
                yield tuple(batch)
                batch.clear()
        if batch:
            yield tuple(batch)
    finally:
        await close_async_iterators((source,), active_error=sys.exception())


async def _batch_by_size(source: AsyncIterator[Any], operation: _BatchBySize) -> AsyncIterator[Any]:
    """Pack items into tuples bounded by cumulative size and optional item count.

    The possibly awaitable size callback must resolve to a non-negative integer.
    A batch flushes before adding an item that would exceed max_size. Strict mode
    rejects an individually oversized item; non-strict mode permits it as an
    oversized batch of its own.
    """
    try:
        batch: list[Any] = []
        batch_size = 0
        async for item in source:
            raw_size = await _resolve(operation.get_size(item))
            try:
                item_size = operator.index(raw_size)
            except TypeError:
                raise TypeError("get_size must return an integer") from None
            if item_size < 0:
                raise ValueError("item sizes must be non-negative")
            if operation.strict and item_size > operation.max_size:
                raise ValueError(f"item size {item_size} exceeds max_size {operation.max_size}")

            size_exceeded = batch_size + item_size > operation.max_size
            count_reached = operation.max_count is not None and len(batch) >= operation.max_count
            if batch and (size_exceeded or count_reached):
                yield tuple(batch)
                batch.clear()
                batch_size = 0

            batch.append(item)
            batch_size += item_size
        if batch:
            yield tuple(batch)
    finally:
        await close_async_iterators((source,), active_error=sys.exception())


async def _window(source: AsyncIterator[Any], operation: _Window) -> AsyncIterator[Any]:
    """Yield fixed-size sliding windows, advancing step source items each time.

    If the entire source is shorter than size, one partial window is yielded.
    Exhaustion while advancing ends the stream without a trailing partial window.
    """
    try:
        current: deque[Any] = deque(maxlen=operation.size)
        while len(current) < operation.size:
            try:
                current.append(await anext(source))
            except StopAsyncIteration:
                if current:
                    yield tuple(current)
                return

        while True:
            yield tuple(current)
            for _ in range(operation.step):
                try:
                    current.append(await anext(source))
                except StopAsyncIteration:
                    return
    finally:
        await close_async_iterators((source,), active_error=sys.exception())


async def _pairwise(source: AsyncIterator[Any]) -> AsyncIterator[Any]:
    """Yield overlapping adjacent pairs; empty and singleton sources emit nothing."""
    try:
        try:
            previous = await anext(source)
        except StopAsyncIteration:
            return
        async for item in source:
            yield previous, item
            previous = item
    finally:
        await close_async_iterators((source,), active_error=sys.exception())


async def _group_runs(source: AsyncIterator[Any], operation: _GroupRuns) -> AsyncIterator[Any]:
    """Group consecutive items with equal possibly awaitable keys into tuples."""
    try:
        try:
            first = await anext(source)
        except StopAsyncIteration:
            return
        current = [first]
        current_key = await _resolve(operation.key(first))
        async for item in source:
            item_key = await _resolve(operation.key(item))
            if item_key == current_key:
                current.append(item)
                continue
            yield tuple(current)
            current = [item]
            current_key = item_key
        yield tuple(current)
    finally:
        await close_async_iterators((source,), active_error=sys.exception())


async def _fold(source: AsyncIterator[Any], operation: _Fold) -> AsyncIterator[Any]:
    """Reduce the whole source and emit exactly one final state.

    Both initializer and step results may be awaitable; an empty source emits the
    resolved initializer value.
    """
    try:
        state = await _resolve(operation.initializer())
        async for item in source:
            state = await _resolve(operation.function(state, item))
        yield state
    finally:
        await close_async_iterators((source,), active_error=sys.exception())


async def _unique(source: AsyncIterator[Any], operation: _Unique) -> AsyncIterator[Any]:
    """Yield the first item for each resolved key while preserving source order.

    Hashable keys use a set; unhashable keys use equality against a linear list.
    """
    hashable: set[Any] = set()
    unhashable: list[Any] = []
    try:
        async for item in source:
            key = await _resolve(operation.key(item))
            try:
                if key in hashable:
                    continue
                hashable.add(key)
            except TypeError:
                if any(key == seen for seen in unhashable):
                    continue
                unhashable.append(key)
            yield item
    finally:
        await close_async_iterators((source,), active_error=sys.exception())


async def _enumerate(source: AsyncIterator[Any], operation: _Enumerate) -> AsyncIterator[Any]:
    """Pair each item with consecutive integers beginning at operation.start."""
    position = operation.start
    try:
        async for item in source:
            yield position, item
            position += 1
    finally:
        await close_async_iterators((source,), active_error=sys.exception())


async def _zip(source: AsyncIterator[Any], operation: _Zip) -> AsyncIterator[Any]:
    """Pull and pair the left and opened right source until either is exhausted.

    Strict mode raises when lengths differ, matching built-in zip diagnostics. The
    locally opened right iterator and the upstream left iterator are both closed.
    """
    other = operation.source.open()
    try:
        while True:
            try:
                left = await anext(source)
            except StopAsyncIteration:
                if not operation.strict:
                    return
                try:
                    await anext(other)
                except StopAsyncIteration:
                    return
                raise ValueError("zip() argument 2 is longer than argument 1") from None

            try:
                right = await anext(other)
            except StopAsyncIteration:
                if operation.strict:
                    raise ValueError("zip() argument 2 is shorter than argument 1") from None
                return
            yield left, right
    finally:
        await close_async_iterators((other, source), active_error=sys.exception())


async def _zip_longest(source: AsyncIterator[Any], operation: _ZipLongest) -> AsyncIterator[Any]:
    """Pair inputs to the longer length, substituting fillvalue on the exhausted side.

    The operator opens and owns the right iterator and closes both inputs on exit.
    """
    other = operation.source.open()
    left_done = right_done = False
    try:
        while not (left_done and right_done):
            if left_done:
                left = operation.fillvalue
            else:
                try:
                    left = await anext(source)
                except StopAsyncIteration:
                    left_done = True
                    left = operation.fillvalue

            if right_done:
                right = operation.fillvalue
            else:
                try:
                    right = await anext(other)
                except StopAsyncIteration:
                    right_done = True
                    right = operation.fillvalue

            if not (left_done and right_done):
                yield left, right
    finally:
        await close_async_iterators((other, source), active_error=sys.exception())


async def _intersperse(source: AsyncIterator[Any], operation: _Intersperse) -> AsyncIterator[Any]:
    """Insert one separator between adjacent items, with none at either boundary."""
    try:
        try:
            first = await anext(source)
        except StopAsyncIteration:
            return
        yield first
        async for item in source:
            yield operation.separator
            yield item
    finally:
        await close_async_iterators((source,), active_error=sys.exception())


async def _concat(source: AsyncIterator[Any], operation: _Concat) -> AsyncIterator[Any]:
    """Drain the upstream source, then each additional source in declaration order.

    Each additional iterator is opened only when reached and closed before moving
    to the next; the original source is closed by the outer cleanup block.
    """
    try:
        async for item in source:
            yield item
        for additional_source in operation.sources:
            other = additional_source.open()
            try:
                async for item in other:
                    yield item
            finally:
                await close_async_iterators((other,), active_error=sys.exception())
    finally:
        await close_async_iterators((source,), active_error=sys.exception())


async def _cross(source: AsyncIterator[Any], operation: _Cross) -> AsyncIterator[Any]:
    """Emit the left-major Cartesian product after caching the right source once.

    The right side is opened only after the first left item. max_right bounds its
    cache and raises before retaining an excess item. The right iterator is closed
    after loading or on failure, and the left source is always closed on exit.
    """
    right_values: list[Any] = []
    right_iterator: AsyncIterator[Any] | None = None
    initialized = False
    try:
        async for left in source:
            if not initialized:
                initialized = True
                right_iterator = operation.source.open()
                try:
                    async for right in right_iterator:
                        if (
                            operation.max_right is not None
                            and len(right_values) >= operation.max_right
                        ):
                            raise BufferLimitError(
                                f"cross() exceeded max_right={operation.max_right}"
                            )
                        right_values.append(right)
                finally:
                    await close_async_iterators((right_iterator,), active_error=sys.exception())
                    right_iterator = None
            for right in right_values:
                yield left, right
    finally:
        owned_iterators = (right_iterator, source) if right_iterator is not None else (source,)
        await close_async_iterators(owned_iterators, active_error=sys.exception())


async def _scan(source: AsyncIterator[Any], operation: _Scan) -> AsyncIterator[Any]:
    """Emit each left-to-right accumulated state after incorporating one item.

    The initial value itself is not emitted, and each step result may be awaitable.
    """
    state = operation.initial
    try:
        async for item in source:
            state = await _resolve(operation.function(state, item))
            yield state
    finally:
        await close_async_iterators((source,), active_error=sys.exception())


async def _scan_right(source: AsyncIterator[Any], operation: _ScanRight) -> AsyncIterator[Any]:
    """Buffer the source, accumulate from right to left, then emit in source order.

    The callback receives (item, state); the initial state is not emitted.
    max_items raises before buffering an excess source item.
    """
    values: list[Any] = []
    try:
        async for item in source:
            if operation.max_items is not None and len(values) >= operation.max_items:
                raise BufferLimitError(f"scan_right() exceeded max_items={operation.max_items}")
            values.append(item)
        state = operation.initial
        for index in range(len(values) - 1, -1, -1):
            state = await _resolve(operation.function(values[index], state))
            values[index] = state
        for value in values:
            yield value
    finally:
        await close_async_iterators((source,), active_error=sys.exception())


async def _prepend(source: AsyncIterator[Any], operation: _Prepend) -> AsyncIterator[Any]:
    """Yield configured values before pulling and forwarding the source."""
    try:
        for item in operation.values:
            yield item
        async for item in source:
            yield item
    finally:
        await close_async_iterators((source,), active_error=sys.exception())


async def _append(source: AsyncIterator[Any], operation: _Append) -> AsyncIterator[Any]:
    """Drain the source before yielding the configured trailing values."""
    try:
        async for item in source:
            yield item
        for item in operation.values:
            yield item
    finally:
        await close_async_iterators((source,), active_error=sys.exception())


async def _map_first(source: AsyncIterator[Any], operation: _MapFirst) -> AsyncIterator[Any]:
    """Map only the first item, awaiting its result, and pass later items unchanged."""
    try:
        try:
            first = await anext(source)
        except StopAsyncIteration:
            return
        yield await _resolve(operation.function(first))
        async for item in source:
            yield item
    finally:
        await close_async_iterators((source,), active_error=sys.exception())


async def _map_last(source: AsyncIterator[Any], operation: _MapLast) -> AsyncIterator[Any]:
    """Pass all but the final item unchanged, then emit its resolved mapped value.

    One item is held pending so an empty source remains empty.
    """
    try:
        try:
            pending = await anext(source)
        except StopAsyncIteration:
            return
        async for item in source:
            yield pending
            pending = item
        yield await _resolve(operation.function(pending))
    finally:
        await close_async_iterators((source,), active_error=sys.exception())


async def _collapse(source: AsyncIterator[Any], operation: _Collapse) -> AsyncIterator[Any]:
    """Merge adjacent collapsible runs and emit one aggregate per run.

    Collapsibility is tested on neighboring original items, while merger combines
    the current run aggregate with the new item. Both callbacks may be awaitable.
    """
    try:
        try:
            previous = aggregate = await anext(source)
        except StopAsyncIteration:
            return
        async for item in source:
            if await _resolve(operation.collapsible(previous, item)):
                aggregate = await _resolve(operation.merger(aggregate, item))
            else:
                yield aggregate
                aggregate = item
            previous = item
        yield aggregate
    finally:
        await close_async_iterators((source,), active_error=sys.exception())
