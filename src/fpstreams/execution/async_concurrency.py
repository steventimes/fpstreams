"""Run task-based async operators with bounded work and deterministic cleanup."""

from __future__ import annotations

import asyncio
import inspect
import sys
from collections import deque
from collections.abc import AsyncIterator, Iterable
from typing import Any, cast

from ..planning.async_ import (
    _BufferTimeout,
    _CombineLatest,
    _Debounce,
    _Delay,
    _MapAsync,
    _Merge,
    _MergeMap,
    _SwitchMap,
    _Throttle,
    _Timeout,
    _to_async_iterator,
)
from ..planning.async_utils import _MISSING, _resolve, close_async_iterators
from .async_runtime import AsyncRuntime, TaskRole


async def _call(operation: _MapAsync, item: Any) -> Any:
    """Invoke one map callback and await either its value or awaitable result.

    A configured timeout wraps the normalized coroutine with asyncio.wait_for, so
    an overdue awaitable portion is cancelled. A synchronous callback still runs
    directly on the event-loop thread and cannot be preempted by that timeout.
    """

    async def invoke() -> Any:
        """Normalize synchronous and awaitable callback results to one coroutine."""
        result = operation.function(item)
        return await result if inspect.isawaitable(result) else result

    if operation.timeout is None:
        return await invoke()
    return await asyncio.wait_for(invoke(), operation.timeout)


async def _cancel(tasks: Iterable[asyncio.Task[Any]]) -> None:
    # Draining every cancellation suppresses orphan-task warnings and background work.
    """Request cancellation for every task and await all outcomes without re-raising them."""
    tasks = tuple(tasks)
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def _pull(iterator: AsyncIterator[Any]) -> Any:
    """Pull one item, allowing StopAsyncIteration and source errors to reach the owner."""
    return await anext(iterator)


async def map_concurrent(source: AsyncIterator[Any], operation: _MapAsync) -> AsyncIterator[Any]:
    """Map items with at most operation.concurrency callback tasks in flight.

    Concurrency one invokes callbacks directly. Ordered mode awaits queued tasks in
    source order; unordered mode emits each completed set without restoring source
    order. Per-item timeouts are enforced by _call. The local runtime owns the
    source and every callback task, cancelling and awaiting unfinished work when
    the consumer stops or any task fails.
    """
    runtime = AsyncRuntime()
    runtime.own_iterator(source)
    try:
        if operation.concurrency == 1:
            async for item in source:
                yield await _call(operation, item)
            return

        if operation.ordered:
            pending: deque[asyncio.Task[Any]] = deque()
            try:
                async for item in source:
                    pending.append(
                        runtime.create_task(_call(operation, item), role=TaskRole.USER_CALL)
                    )
                    if len(pending) >= operation.concurrency:
                        yield await pending.popleft()
                while pending:
                    yield await pending.popleft()
            finally:
                pass
            return

        pending_set: set[asyncio.Task[Any]] = set()
        try:
            async for item in source:
                pending_set.add(
                    runtime.create_task(_call(operation, item), role=TaskRole.USER_CALL)
                )
                if len(pending_set) >= operation.concurrency:
                    done, pending_set = await asyncio.wait(
                        pending_set, return_when=asyncio.FIRST_COMPLETED
                    )
                    for task in done:
                        yield task.result()
            while pending_set:
                done, pending_set = await asyncio.wait(
                    pending_set, return_when=asyncio.FIRST_COMPLETED
                )
                for task in done:
                    yield task.result()
        finally:
            pass
    finally:
        await runtime.aclose(active_error=sys.exception())


async def merge(source: AsyncIterator[Any], operation: _Merge) -> AsyncIterator[Any]:
    """Interleave values from all sources as their one-item pull tasks complete.

    One pull is outstanding per active iterator. Exhausted inputs are closed
    immediately; cancellation, failure, or early downstream close cancels pending
    pulls and closes every remaining iterator through the local runtime.
    """
    runtime = AsyncRuntime()
    runtime.own_iterator(source)
    active: dict[int, AsyncIterator[Any]] = {0: source}
    pending: dict[asyncio.Task[Any], int] = {}
    try:
        for position, additional_source in enumerate(operation.sources, start=1):
            active[position] = additional_source.open()
            runtime.own_iterator(active[position])
        for position, iterator in active.items():
            pending[runtime.create_task(_pull(iterator), role=TaskRole.SOURCE_PULL)] = position

        while pending:
            done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                position = pending.pop(task)
                iterator = active[position]
                try:
                    item = task.result()
                except StopAsyncIteration:
                    await runtime.release_iterator(iterator, close=True)
                    active.pop(position)
                    continue
                yield item
                pending[runtime.create_task(_pull(iterator), role=TaskRole.SOURCE_PULL)] = position
    finally:
        await runtime.aclose(active_error=sys.exception())


async def combine_latest(
    source: AsyncIterator[Any],
    operation: _CombineLatest,
) -> AsyncIterator[Any]:
    """Emit the latest value from every source whenever any source advances.

    No tuple is emitted until each input has produced once. An input that completes
    before its first value ends the operator; after producing, its final value is
    retained while other inputs continue. One owned pull task is maintained for
    each active iterator and all remaining work is cancelled on exit.
    """
    runtime = AsyncRuntime()
    runtime.own_iterator(source)
    active: dict[int, AsyncIterator[Any]] = {0: source}
    pending: dict[asyncio.Task[Any], int] = {}
    latest = [_MISSING] * (len(operation.sources) + 1)
    ready = 0
    try:
        for position, additional_source in enumerate(operation.sources, start=1):
            active[position] = additional_source.open()
            runtime.own_iterator(active[position])
        for position, iterator in active.items():
            pending[runtime.create_task(_pull(iterator), role=TaskRole.SOURCE_PULL)] = position

        while pending:
            done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                position = pending.pop(task)
                iterator = active[position]
                try:
                    item = task.result()
                except StopAsyncIteration:
                    await runtime.release_iterator(iterator, close=True)
                    active.pop(position)
                    if latest[position] is _MISSING:
                        return
                    continue

                if latest[position] is _MISSING:
                    ready += 1
                latest[position] = item
                if ready == len(latest):
                    yield tuple(latest)
                pending[runtime.create_task(_pull(iterator), role=TaskRole.SOURCE_PULL)] = position
    finally:
        await runtime.aclose(active_error=sys.exception())


async def _open_mapped(operation: _MergeMap, item: Any) -> AsyncIterator[Any]:
    """Resolve a merge-map callback and normalize its iterable to an async iterator."""
    nested = await _resolve(operation.function(item))
    return _to_async_iterator(nested)


async def merge_map(source: AsyncIterator[Any], operation: _MergeMap) -> AsyncIterator[Any]:
    """Map outer items to inner streams and interleave their values concurrently.

    Pending mapping calls and open inner streams share one concurrency budget, so
    the outer source is pulled only when a slot is free. Each open inner has one
    pull task and values are emitted in completion order. The runtime adopts
    iterators returned by racing mapping tasks, ensuring cancellation cannot leak
    an inner stream that finished opening during cleanup.
    """
    runtime = AsyncRuntime()
    runtime.own_iterator(source)
    outer_pull: asyncio.Task[Any] | None = None
    outer_done = False
    mappings: set[asyncio.Task[AsyncIterator[Any]]] = set()
    inners: dict[int, AsyncIterator[Any]] = {}
    inner_pulls: dict[asyncio.Task[Any], int] = {}
    next_inner_id = 0

    try:
        while True:
            # Count both opening and active inners before pulling another outer item.
            occupied = len(mappings) + len(inners)
            if outer_pull is None and not outer_done and occupied < operation.concurrency:
                outer_pull = runtime.create_task(_pull(source), role=TaskRole.SOURCE_PULL)

            waiting: set[asyncio.Task[Any]] = set(mappings)
            waiting.update(inner_pulls)
            if outer_pull is not None:
                waiting.add(outer_pull)
            if not waiting:
                return

            done, _ = await asyncio.wait(waiting, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if task is outer_pull:
                    outer_pull = None
                    try:
                        outer_item = task.result()
                    except StopAsyncIteration:
                        outer_done = True
                    else:
                        mappings.add(
                            runtime.create_task(
                                _open_mapped(operation, outer_item),
                                role=TaskRole.INNER_OPEN,
                                returns_iterator=True,
                            )
                        )
                    continue

                if task in mappings:
                    mapping = cast(asyncio.Task[AsyncIterator[Any]], task)
                    mappings.remove(mapping)
                    nested = runtime.finish_task(mapping)
                    runtime.own_iterator(nested)
                    position = next_inner_id
                    next_inner_id += 1
                    inners[position] = nested
                    inner_pulls[runtime.create_task(_pull(nested), role=TaskRole.INNER_PULL)] = (
                        position
                    )
                    continue

                position = inner_pulls.pop(task)
                nested = inners[position]
                try:
                    item = task.result()
                except StopAsyncIteration:
                    await runtime.release_iterator(nested, close=True)
                    inners.pop(position)
                    continue
                yield item
                inner_pulls[runtime.create_task(_pull(nested), role=TaskRole.INNER_PULL)] = position
    finally:
        await runtime.aclose(active_error=sys.exception())


async def _open_switched(operation: _SwitchMap, item: Any) -> AsyncIterator[Any]:
    """Resolve a switch-map callback and normalize its iterable to an async iterator."""
    nested = await _resolve(operation.function(item))
    return _to_async_iterator(nested)


async def _finished_mapping(
    task: asyncio.Task[AsyncIterator[Any]],
) -> AsyncIterator[Any] | None:
    """Cancel a superseded mapping task and recover any iterator it already returned.

    A successfully completed iterator is returned so switch_map can close it;
    cancelled and failed mappings have no iterator to release here.
    """
    await _cancel((task,))
    if task.cancelled() or task.exception() is not None:
        return None
    return task.result()


async def switch_map(source: AsyncIterator[Any], operation: _SwitchMap) -> AsyncIterator[Any]:
    """Emit only from the inner stream associated with the latest outer value.

    The next outer pull races the current mapping and inner pull. A newer outer
    value cancels an unfinished mapping or pull and closes the superseded inner
    before opening its replacement. Outer completion lets the current inner drain;
    runtime cleanup owns every remaining task and iterator.
    """
    runtime = AsyncRuntime()
    runtime.own_iterator(source)
    outer_pull: asyncio.Task[Any] | None = runtime.create_task(
        _pull(source), role=TaskRole.SOURCE_PULL
    )
    mapping: asyncio.Task[AsyncIterator[Any]] | None = None
    inner: AsyncIterator[Any] | None = None
    inner_pull: asyncio.Task[Any] | None = None

    try:
        while True:
            waiting = {task for task in (outer_pull, mapping, inner_pull) if task is not None}
            if not waiting:
                return
            done, _ = await asyncio.wait(waiting, return_when=asyncio.FIRST_COMPLETED)

            if outer_pull is not None and outer_pull in done:
                completed_outer = outer_pull
                outer_pull = None
                try:
                    outer_item = completed_outer.result()
                except StopAsyncIteration:
                    pass
                else:
                    outer_pull = runtime.create_task(_pull(source), role=TaskRole.SOURCE_PULL)
                    stale_iterators: list[AsyncIterator[Any]] = []
                    if mapping is not None:
                        stale_mapping = mapping
                        mapping = None
                        mapped_iterator = await _finished_mapping(stale_mapping)
                        if mapped_iterator is not None:
                            stale_iterators.append(mapped_iterator)
                    if inner_pull is not None:
                        await _cancel((inner_pull,))
                        inner_pull = None
                    if inner is not None:
                        stale_iterators.append(inner)
                        inner = None
                    for stale in stale_iterators:
                        await runtime.release_iterator(stale, close=True)
                    mapping = runtime.create_task(
                        _open_switched(operation, outer_item),
                        role=TaskRole.INNER_OPEN,
                        returns_iterator=True,
                    )
                    continue

            if mapping is not None and mapping in done:
                completed_mapping = mapping
                mapping = None
                inner = runtime.finish_task(completed_mapping)
                runtime.own_iterator(inner)
                inner_pull = runtime.create_task(_pull(inner), role=TaskRole.INNER_PULL)

            if inner_pull is not None and inner_pull in done:
                completed_inner_pull = inner_pull
                inner_pull = None
                try:
                    item = completed_inner_pull.result()
                except StopAsyncIteration:
                    if inner is not None:
                        await runtime.release_iterator(inner, close=True)
                        inner = None
                else:
                    yield item
                    if inner is not None:
                        inner_pull = runtime.create_task(_pull(inner), role=TaskRole.INNER_PULL)
    finally:
        await runtime.aclose(active_error=sys.exception())


async def delay(source: AsyncIterator[Any], operation: _Delay) -> AsyncIterator[Any]:
    """Wait once before the first source pull, then forward all values unchanged."""
    try:
        await asyncio.sleep(operation.seconds)
        async for item in source:
            yield item
    finally:
        await close_async_iterators((source,), active_error=sys.exception())


async def throttle(source: AsyncIterator[Any], operation: _Throttle) -> AsyncIterator[Any]:
    """Delay, but never drop, items to enforce a sliding-window rate limit.

    At most max_count emission timestamps may fall within the preceding
    operation.per seconds; when the window is full, the operator sleeps until its
    oldest timestamp expires.
    """
    emitted_at: deque[float] = deque(maxlen=operation.max_count)
    loop = asyncio.get_running_loop()
    try:
        async for item in source:
            now = loop.time()
            while emitted_at and now - emitted_at[0] >= operation.per:
                emitted_at.popleft()
            while len(emitted_at) >= operation.max_count:
                await asyncio.sleep(max(0.0, emitted_at[0] + operation.per - now))
                now = loop.time()
                while emitted_at and now - emitted_at[0] >= operation.per:
                    emitted_at.popleft()
            emitted_at.append(now)
            yield item
    finally:
        await close_async_iterators((source,), active_error=sys.exception())


async def timeout(source: AsyncIterator[Any], operation: _Timeout) -> AsyncIterator[Any]:
    """Require each individual source pull to finish within the configured interval.

    asyncio.wait_for cancels an overdue pull. Normal source exhaustion ends the
    stream, and the source iterator is closed for every exit path.
    """
    try:
        while True:
            try:
                item = await asyncio.wait_for(_pull(source), operation.seconds)
            except StopAsyncIteration:
                return
            yield item
    finally:
        await close_async_iterators((source,), active_error=sys.exception())


async def debounce(source: AsyncIterator[Any], operation: _Debounce) -> AsyncIterator[Any]:
    """Emit the latest item after a quiet interval, resetting the timer on input.

    Pulling continues while the timer runs. Source completion cancels the timer and
    flushes the pending latest item immediately. The runtime cancels both timer and
    pull tasks when downstream stops.
    """
    runtime = AsyncRuntime()
    runtime.own_iterator(source)
    pull: asyncio.Task[Any] | None = runtime.create_task(_pull(source), role=TaskRole.SOURCE_PULL)
    timer: asyncio.Task[None] | None = None
    latest: Any = _MISSING
    try:
        while pull is not None:
            waiting: set[asyncio.Task[Any]] = {pull}
            if timer is not None:
                waiting.add(timer)
            done, _ = await asyncio.wait(waiting, return_when=asyncio.FIRST_COMPLETED)

            if pull in done:
                try:
                    item = pull.result()
                except StopAsyncIteration:
                    pull = None
                    if timer is not None:
                        await runtime.cancel_task(timer)
                        timer = None
                    if latest is not _MISSING:
                        yield latest
                    return

                latest = item
                if timer is not None:
                    await runtime.cancel_task(timer)
                timer = runtime.create_task(asyncio.sleep(operation.seconds), role=TaskRole.TIMER)
                pull = runtime.create_task(_pull(source), role=TaskRole.SOURCE_PULL)
                continue

            if timer is not None and timer in done:
                timer.result()
                timer = None
                item = latest
                latest = _MISSING
                yield item
    finally:
        await runtime.aclose(active_error=sys.exception())


async def buffer_timeout(
    source: AsyncIterator[Any],
    operation: _BufferTimeout,
) -> AsyncIterator[Any]:
    """Collect tuples until max_count or the first-item timer wins.

    The timer starts when an empty batch receives its first item and is cancelled
    when the count limit flushes that batch. Source completion flushes a final
    nonempty batch immediately; runtime cleanup cancels outstanding pulls and
    timers.
    """
    runtime = AsyncRuntime()
    runtime.own_iterator(source)
    pull: asyncio.Task[Any] | None = runtime.create_task(_pull(source), role=TaskRole.SOURCE_PULL)
    timer: asyncio.Task[None] | None = None
    batch: list[Any] = []
    try:
        while pull is not None:
            waiting: set[asyncio.Task[Any]] = {pull}
            if timer is not None:
                waiting.add(timer)
            done, _ = await asyncio.wait(waiting, return_when=asyncio.FIRST_COMPLETED)

            if timer is not None and timer in done:
                timer.result()
                timer = None
                output = tuple(batch)
                batch.clear()
                yield output
                continue

            try:
                item = pull.result()
            except StopAsyncIteration:
                pull = None
                if timer is not None:
                    await runtime.cancel_task(timer)
                    timer = None
                if batch:
                    yield tuple(batch)
                return

            pull = None
            batch.append(item)
            if len(batch) == 1:
                timer = runtime.create_task(asyncio.sleep(operation.seconds), role=TaskRole.TIMER)
            if len(batch) == operation.max_count:
                if timer is not None:
                    await runtime.cancel_task(timer)
                    timer = None
                output = tuple(batch)
                batch.clear()
                yield output
            pull = runtime.create_task(_pull(source), role=TaskRole.SOURCE_PULL)
    finally:
        await runtime.aclose(active_error=sys.exception())
