import asyncio
import time
from typing import AsyncIterator, Callable, Awaitable, Iterable
from .common import T, R

async def _ensure_coroutine(awaitable: Awaitable[R]) -> R:
    """
    Helper to wrap any Awaitable into a Coroutine.
    Required because asyncio.create_task() demands a Coroutine, not just an Awaitable.
    """
    return await awaitable

async def from_iterable_async(iterable: Iterable[T]) -> AsyncIterator[T]:
    """Converts a standard sync iterable into an async generator."""
    for item in iterable:
        yield item
        await asyncio.sleep(0)

async def map_async_gen(
    iterator: AsyncIterator[T], 
    func: Callable[[T], Awaitable[R]]
) -> AsyncIterator[Awaitable[R]]:
    """
    Maps elements to coroutines (Does not await them yet).
    Returns a stream of 'pending tasks'.
    """
    async for item in iterator:
        yield func(item)

async def gather_gen(
    iterator: AsyncIterator[Awaitable[R]], 
    limit: int | None = None
) -> AsyncIterator[R]:
    """
    Executes pending coroutines.
    """
    if limit is None:
        async for coro in iterator:
            yield await coro
    else:
        sem = asyncio.Semaphore(limit)
        
        async def protected_exec(c: Awaitable[R]) -> R:
            async with sem:
                return await c

        tasks = []
        async for coro in iterator:
            tasks.append(asyncio.create_task(protected_exec(coro)))

        for task in tasks:
            yield await task

async def unordered_gather_gen(
    iterator: AsyncIterator[Awaitable[R]]
) -> AsyncIterator[R]:
    """
    Yields results as soon as they finish, ignoring input order.
    """
    tasks = [
        asyncio.create_task(_ensure_coroutine(c)) 
        async for c in iterator
    ]

    for finished_task in asyncio.as_completed(tasks):
        yield await finished_task

async def filter_gen(
    iterator: AsyncIterator[T], 
    predicate: Callable[[T], bool]
) -> AsyncIterator[T]:
    async for item in iterator:
        if predicate(item):
            yield item

async def limit_gen(iterator: AsyncIterator[T], n: int) -> AsyncIterator[T]:
    count = 0
    async for item in iterator:
        if count >= n:
            break
        yield item
        count += 1
        
async def debounce_gen(
    iterator: AsyncIterator[T], 
    wait_ms: float
) -> AsyncIterator[T]:
    """
    Emits an item only if 'wait_ms' has passed since the last item.
    Note: In a pull-based stream, this acts more like a rate-limiter 
    that drops fast-incoming items from the source.
    """
    last_emit_time = 0.0
    wait_sec = wait_ms / 1000.0

    async for item in iterator:
        current_time = time.monotonic()
        if (current_time - last_emit_time) >= wait_sec:
            yield item
            last_emit_time = current_time