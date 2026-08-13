"""Concurrent and realtime operators for asynchronous streams."""

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
from ..planning.async_utils import _MISSING, _close, _resolve, close_async_iterators


async def _call(operation: _MapAsync, item: Any) -> Any:
    async def invoke() -> Any:
        result = operation.function(item)
        return await result if inspect.isawaitable(result) else result

    if operation.timeout is None:
        return await invoke()
    return await asyncio.wait_for(invoke(), operation.timeout)


async def _cancel(tasks: Iterable[asyncio.Task[Any]]) -> None:
    # Always await cancelled tasks so no background work or warning escapes the pipeline.
    tasks = tuple(tasks)
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def _pull(iterator: AsyncIterator[Any]) -> Any:
    return await anext(iterator)


async def map_concurrent(source: AsyncIterator[Any], operation: _MapAsync) -> AsyncIterator[Any]:
    try:
        if operation.concurrency == 1:
            async for item in source:
                yield await _call(operation, item)
            return

        if operation.ordered:
            pending: deque[asyncio.Task[Any]] = deque()
            try:
                async for item in source:
                    pending.append(asyncio.create_task(_call(operation, item)))
                    if len(pending) >= operation.concurrency:
                        yield await pending.popleft()
                while pending:
                    yield await pending.popleft()
            finally:
                await _cancel(pending)
            return

        pending_set: set[asyncio.Task[Any]] = set()
        try:
            async for item in source:
                pending_set.add(asyncio.create_task(_call(operation, item)))
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
            await _cancel(pending_set)
    finally:
        await close_async_iterators((source,), active_error=sys.exception())


async def merge(source: AsyncIterator[Any], operation: _Merge) -> AsyncIterator[Any]:
    active: dict[int, AsyncIterator[Any]] = {0: source}
    pending: dict[asyncio.Task[Any], int] = {}
    try:
        for position, additional_source in enumerate(operation.sources, start=1):
            active[position] = additional_source.open()
        for position, iterator in active.items():
            pending[asyncio.create_task(_pull(iterator))] = position

        while pending:
            done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                position = pending.pop(task)
                iterator = active[position]
                try:
                    item = task.result()
                except StopAsyncIteration:
                    await _close(iterator)
                    active.pop(position)
                    continue
                yield item
                pending[asyncio.create_task(_pull(iterator))] = position
    finally:
        await _cancel(pending)
        await close_async_iterators(reversed(tuple(active.values())), active_error=sys.exception())


async def combine_latest(
    source: AsyncIterator[Any],
    operation: _CombineLatest,
) -> AsyncIterator[Any]:
    active: dict[int, AsyncIterator[Any]] = {0: source}
    pending: dict[asyncio.Task[Any], int] = {}
    latest = [_MISSING] * (len(operation.sources) + 1)
    ready = 0
    try:
        for position, additional_source in enumerate(operation.sources, start=1):
            active[position] = additional_source.open()
        for position, iterator in active.items():
            pending[asyncio.create_task(_pull(iterator))] = position

        while pending:
            done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                position = pending.pop(task)
                iterator = active[position]
                try:
                    item = task.result()
                except StopAsyncIteration:
                    await _close(iterator)
                    active.pop(position)
                    if latest[position] is _MISSING:
                        return
                    continue

                if latest[position] is _MISSING:
                    ready += 1
                latest[position] = item
                if ready == len(latest):
                    yield tuple(latest)
                pending[asyncio.create_task(_pull(iterator))] = position
    finally:
        await _cancel(pending)
        await close_async_iterators(reversed(tuple(active.values())), active_error=sys.exception())


async def _open_mapped(operation: _MergeMap, item: Any) -> AsyncIterator[Any]:
    nested = await _resolve(operation.function(item))
    return _to_async_iterator(nested)


async def merge_map(source: AsyncIterator[Any], operation: _MergeMap) -> AsyncIterator[Any]:
    outer_pull: asyncio.Task[Any] | None = None
    outer_done = False
    mappings: set[asyncio.Task[AsyncIterator[Any]]] = set()
    inners: dict[int, AsyncIterator[Any]] = {}
    inner_pulls: dict[asyncio.Task[Any], int] = {}
    next_inner_id = 0

    try:
        while True:
            # Mapping tasks and open inner streams share one concurrency budget.
            occupied = len(mappings) + len(inners)
            if outer_pull is None and not outer_done and occupied < operation.concurrency:
                outer_pull = asyncio.create_task(_pull(source))

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
                        mappings.add(asyncio.create_task(_open_mapped(operation, outer_item)))
                    continue

                if task in mappings:
                    mapping = cast(asyncio.Task[AsyncIterator[Any]], task)
                    mappings.remove(mapping)
                    nested = mapping.result()
                    position = next_inner_id
                    next_inner_id += 1
                    inners[position] = nested
                    inner_pulls[asyncio.create_task(_pull(nested))] = position
                    continue

                position = inner_pulls.pop(task)
                nested = inners[position]
                try:
                    item = task.result()
                except StopAsyncIteration:
                    await _close(nested)
                    inners.pop(position)
                    continue
                yield item
                inner_pulls[asyncio.create_task(_pull(nested))] = position
    finally:
        tasks: list[asyncio.Task[Any]] = [*mappings, *inner_pulls]
        if outer_pull is not None:
            tasks.append(outer_pull)
        await _cancel(tasks)

        owned_iterators: list[AsyncIterator[Any]] = []
        for mapping in mappings:
            if mapping.cancelled() or not mapping.done() or mapping.exception() is not None:
                continue
            owned_iterators.append(mapping.result())
        owned_iterators.extend(reversed(tuple(inners.values())))
        owned_iterators.append(source)
        await close_async_iterators(owned_iterators, active_error=sys.exception())


async def _open_switched(operation: _SwitchMap, item: Any) -> AsyncIterator[Any]:
    nested = await _resolve(operation.function(item))
    return _to_async_iterator(nested)


async def _finished_mapping(
    task: asyncio.Task[AsyncIterator[Any]],
) -> AsyncIterator[Any] | None:
    await _cancel((task,))
    if task.cancelled() or task.exception() is not None:
        return None
    return task.result()


async def switch_map(source: AsyncIterator[Any], operation: _SwitchMap) -> AsyncIterator[Any]:
    outer_pull: asyncio.Task[Any] | None = asyncio.create_task(_pull(source))
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
                    outer_pull = asyncio.create_task(_pull(source))
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
                    await close_async_iterators(stale_iterators)
                    mapping = asyncio.create_task(_open_switched(operation, outer_item))
                    continue

            if mapping is not None and mapping in done:
                completed_mapping = mapping
                mapping = None
                inner = completed_mapping.result()
                inner_pull = asyncio.create_task(_pull(inner))

            if inner_pull is not None and inner_pull in done:
                completed_inner_pull = inner_pull
                inner_pull = None
                try:
                    item = completed_inner_pull.result()
                except StopAsyncIteration:
                    if inner is not None:
                        await close_async_iterators((inner,))
                        inner = None
                else:
                    yield item
                    if inner is not None:
                        inner_pull = asyncio.create_task(_pull(inner))
    finally:
        tasks = tuple(task for task in (outer_pull, mapping, inner_pull) if task is not None)
        await _cancel(tasks)

        owned_iterators: list[AsyncIterator[Any]] = []
        if mapping is not None:
            mapped_iterator = await _finished_mapping(mapping)
            if mapped_iterator is not None:
                owned_iterators.append(mapped_iterator)
        if inner is not None:
            owned_iterators.append(inner)
        owned_iterators.append(source)
        await close_async_iterators(owned_iterators, active_error=sys.exception())


async def delay(source: AsyncIterator[Any], operation: _Delay) -> AsyncIterator[Any]:
    try:
        await asyncio.sleep(operation.seconds)
        async for item in source:
            yield item
    finally:
        await close_async_iterators((source,), active_error=sys.exception())


async def throttle(source: AsyncIterator[Any], operation: _Throttle) -> AsyncIterator[Any]:
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
    pull: asyncio.Task[Any] | None = asyncio.create_task(_pull(source))
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
                        await _cancel((timer,))
                        timer = None
                    if latest is not _MISSING:
                        yield latest
                    return

                latest = item
                if timer is not None:
                    await _cancel((timer,))
                timer = asyncio.create_task(asyncio.sleep(operation.seconds))
                pull = asyncio.create_task(_pull(source))
                continue

            if timer is not None and timer in done:
                timer.result()
                timer = None
                item = latest
                latest = _MISSING
                yield item
    finally:
        tasks: list[asyncio.Task[Any]] = []
        if pull is not None:
            tasks.append(pull)
        if timer is not None:
            tasks.append(timer)
        await _cancel(tasks)
        await close_async_iterators((source,), active_error=sys.exception())


async def buffer_timeout(
    source: AsyncIterator[Any],
    operation: _BufferTimeout,
) -> AsyncIterator[Any]:
    pull: asyncio.Task[Any] | None = asyncio.create_task(_pull(source))
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
                    await _cancel((timer,))
                    timer = None
                if batch:
                    yield tuple(batch)
                return

            pull = None
            batch.append(item)
            if len(batch) == 1:
                timer = asyncio.create_task(asyncio.sleep(operation.seconds))
            if len(batch) == operation.max_count:
                if timer is not None:
                    await _cancel((timer,))
                    timer = None
                output = tuple(batch)
                batch.clear()
                yield output
            pull = asyncio.create_task(_pull(source))
    finally:
        tasks: list[asyncio.Task[Any]] = []
        if pull is not None:
            tasks.append(pull)
        if timer is not None:
            tasks.append(timer)
        await _cancel(tasks)
        await close_async_iterators((source,), active_error=sys.exception())
