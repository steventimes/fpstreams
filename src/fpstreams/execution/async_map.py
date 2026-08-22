"""Bounded physical executor for concurrent async map nodes."""

from __future__ import annotations

import inspect
import sys
from collections.abc import AsyncIterator
from contextlib import suppress
from typing import Any

from ..physical.async_plan import AsyncMapNode
from ..planning.async_utils import close_async_iterators
from ..runtime.query import QueryRuntime
from ..runtime.tasks import TaskRole
from .async_queue import CompletionQueue, OrderedResultRing


async def _call(node: AsyncMapNode, item: Any) -> Any:
    """Resolve one synchronous-or-awaitable mapper call under its optional timeout."""

    async def invoke() -> Any:
        """Normalize a mapper's immediate value or awaitable to one awaited result."""
        result = node.operation.function(item)
        return await result if inspect.isawaitable(result) else result

    if node.operation.timeout is None:
        return await invoke()
    import asyncio

    return await asyncio.wait_for(invoke(), node.operation.timeout)


async def _observe(scope: Any, task: Any) -> None:
    """Drain a completed task after its done callback has captured the public outcome."""
    with suppress(BaseException):
        await scope.take_result(task)


async def execute_async_map(
    source: AsyncIterator[Any], node: AsyncMapNode, runtime: QueryRuntime
) -> AsyncIterator[Any]:
    """Execute bounded map work with exact ordered or completion-order semantics."""
    operation = node.operation
    scope = runtime.tasks.scope(f"map:{node.logical_ids[0]}", max_tasks=operation.concurrency)
    try:
        if operation.concurrency == 1:
            async for item in source:
                yield await _call(node, item)
            return

        completions = CompletionQueue()
        pending: dict[int, Any] = {}
        next_sequence = 0
        source_done = False
        ring = OrderedResultRing(operation.concurrency) if operation.ordered else None

        async def fill() -> None:
            """Submit source values only until the public concurrency budget is full."""
            nonlocal next_sequence, source_done
            while not source_done and len(pending) < operation.concurrency:
                try:
                    item = await anext(source)
                except StopAsyncIteration:
                    source_done = True
                    return
                task = scope.create_task(_call(node, item), role=TaskRole.USER_CALL)
                pending[next_sequence] = task
                completions.watch(task, sequence=next_sequence)
                next_sequence += 1

        while pending or not source_done:
            await fill()
            if not pending:
                continue
            completion = await completions.get()
            task = pending[completion.sequence]
            await _observe(scope, task)
            if ring is None:
                pending.pop(completion.sequence)
                yield completion.result()
                continue
            ring.put(completion.sequence, completion)
            while (ready := ring.pop_next()) is not None:
                pending.pop(ready.sequence)
                yield ready.result()
    finally:
        await scope.aclose()
        await close_async_iterators((source,), active_error=sys.exception())
