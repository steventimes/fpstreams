"""One-task timer ownership for async physical timer nodes."""

from __future__ import annotations

import asyncio
import sys
from collections import deque
from collections.abc import AsyncIterator
from typing import Any

from ..physical.async_plan import AsyncTimerNode
from ..planning.async_ import _BufferTimeout, _Debounce, _Delay, _Throttle, _Timeout
from ..planning.async_utils import _MISSING, close_async_iterators
from ..runtime.query import QueryRuntime
from ..runtime.tasks import TaskRole, TaskScope


class TimerHandle:
    """Own at most one cancellable delay task within one child task scope."""

    def __init__(self, scope: TaskScope) -> None:
        self._scope = scope
        self._task: asyncio.Task[None] | None = None

    @property
    def armed(self) -> bool:
        """Return whether this handle currently owns a live or completed delay task."""
        return self._task is not None

    @property
    def task(self) -> asyncio.Task[None] | None:
        """Expose the single owned task for a scheduler wait set."""
        return self._task

    async def arm(self, delay: float) -> None:
        """Replace the current timer after draining it, then arm one new delay."""
        from ..runtime.failpoints import hit

        if delay < 0:
            raise ValueError("timer delay cannot be negative")
        await self.cancel()
        self._task = self._scope.create_task(asyncio.sleep(delay), role=TaskRole.OPERATOR)
        hit("timer.arm.after")

    async def take(self) -> None:
        """Wait for the armed timer and make the handle reusable."""
        from ..runtime.failpoints import hit

        if self._task is None:
            raise RuntimeError("timer is not armed")
        task = self._task
        self._task = None
        await self._scope.take_result(task)
        hit("timer.fire.before_publish")

    async def cancel(self) -> None:
        """Cancel and drain the active timer, if any."""
        if self._task is None:
            return
        task = self._task
        self._task = None
        await self._scope.cancel(task)

    async def aclose(self) -> None:
        """Idempotently release the one owned timer task."""
        await self.cancel()


async def _pull(iterator: AsyncIterator[Any]) -> Any:
    """Represent one upstream request as a query-owned task."""
    return await anext(iterator)


async def _execute_timeout(
    source: AsyncIterator[Any],
    operation: _Timeout,
    scope: TaskScope,
    timer: TimerHandle,
) -> AsyncIterator[Any]:
    """Race each source pull against one reset timer and cancel the loser."""
    while True:
        pull = scope.create_task(_pull(source), role=TaskRole.SOURCE)
        await timer.arm(operation.seconds)
        assert timer.task is not None
        done, _ = await asyncio.wait((pull, timer.task), return_when=asyncio.FIRST_COMPLETED)
        if pull in done:
            await timer.cancel()
            try:
                yield await scope.take_result(pull)
            except StopAsyncIteration:
                return
            continue
        await timer.take()
        await scope.cancel(pull)
        raise TimeoutError(f"async stream timed out after {operation.seconds} seconds")


async def _execute_debounce(
    source: AsyncIterator[Any],
    operation: _Debounce,
    scope: TaskScope,
    timer: TimerHandle,
) -> AsyncIterator[Any]:
    """Publish only the latest value after one quiet interval or source completion."""
    pull: asyncio.Task[Any] | None = scope.create_task(_pull(source), role=TaskRole.SOURCE)
    latest: Any = _MISSING
    while pull is not None:
        waiting = {pull}
        if timer.task is not None:
            waiting.add(timer.task)
        done, _ = await asyncio.wait(waiting, return_when=asyncio.FIRST_COMPLETED)
        if pull in done:
            completed_pull = pull
            pull = None
            try:
                latest = await scope.take_result(completed_pull)
            except StopAsyncIteration:
                await timer.cancel()
                if latest is not _MISSING:
                    yield latest
                return
            await timer.arm(operation.seconds)
            pull = scope.create_task(_pull(source), role=TaskRole.SOURCE)
            continue
        await timer.take()
        if latest is not _MISSING:
            output = latest
            latest = _MISSING
            yield output


async def _execute_buffer_timeout(
    source: AsyncIterator[Any],
    operation: _BufferTimeout,
    scope: TaskScope,
    timer: TimerHandle,
) -> AsyncIterator[Any]:
    """Flush a bounded batch when either its count or first-item timer wins."""
    pull: asyncio.Task[Any] | None = scope.create_task(_pull(source), role=TaskRole.SOURCE)
    batch: list[Any] = []
    while pull is not None:
        waiting = {pull}
        if timer.task is not None:
            waiting.add(timer.task)
        done, _ = await asyncio.wait(waiting, return_when=asyncio.FIRST_COMPLETED)
        if pull in done:
            completed_pull = pull
            pull = None
            try:
                item = await scope.take_result(completed_pull)
            except StopAsyncIteration:
                await timer.cancel()
                if batch:
                    yield tuple(batch)
                return
            batch.append(item)
            if len(batch) == 1:
                await timer.arm(operation.seconds)
            if len(batch) == operation.max_count:
                await timer.cancel()
                output = tuple(batch)
                batch.clear()
                yield output
            pull = scope.create_task(_pull(source), role=TaskRole.SOURCE)
            continue
        await timer.take()
        if batch:
            output = tuple(batch)
            batch.clear()
            yield output


async def _execute_delay(
    source: AsyncIterator[Any], operation: _Delay, timer: TimerHandle
) -> AsyncIterator[Any]:
    """Delay subscription once, then preserve upstream timing and order."""
    await timer.arm(operation.seconds)
    await timer.take()
    async for item in source:
        yield item


async def _execute_throttle(
    source: AsyncIterator[Any], operation: _Throttle, timer: TimerHandle
) -> AsyncIterator[Any]:
    """Enforce a sliding-window emission budget with one reusable timer."""
    emitted_at: deque[float] = deque(maxlen=operation.max_count)
    loop = asyncio.get_running_loop()
    async for item in source:
        now = loop.time()
        while emitted_at and now - emitted_at[0] >= operation.per:
            emitted_at.popleft()
        if len(emitted_at) >= operation.max_count:
            await timer.arm(max(0.0, emitted_at[0] + operation.per - now))
            await timer.take()
            now = loop.time()
            while emitted_at and now - emitted_at[0] >= operation.per:
                emitted_at.popleft()
        emitted_at.append(now)
        yield item


async def execute_async_timer(
    source: AsyncIterator[Any], node: AsyncTimerNode, runtime: QueryRuntime
) -> AsyncIterator[Any]:
    """Run one timer physical node with one timer task and one child task scope."""
    scope = runtime.tasks.scope(f"timer:{node.logical_ids[0]}")
    timer = TimerHandle(scope)
    operation = node.operation
    try:
        if isinstance(operation, _Timeout):
            async for item in _execute_timeout(source, operation, scope, timer):
                yield item
        elif isinstance(operation, _Debounce):
            async for item in _execute_debounce(source, operation, scope, timer):
                yield item
        elif isinstance(operation, _BufferTimeout):
            async for item in _execute_buffer_timeout(source, operation, scope, timer):
                yield item
        elif isinstance(operation, _Delay):
            async for item in _execute_delay(source, operation, timer):
                yield item
        elif isinstance(operation, _Throttle):
            async for item in _execute_throttle(source, operation, timer):
                yield item
        else:  # pragma: no cover - the physical compiler constrains this union.
            raise TypeError(f"unsupported async timer operation: {type(operation).__name__}")
    finally:
        await timer.aclose()
        await scope.aclose()
        await close_async_iterators((source,), active_error=sys.exception())
