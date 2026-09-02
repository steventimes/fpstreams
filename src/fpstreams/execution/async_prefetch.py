"""Strictly bounded pull-ahead execution for async prefetch nodes."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

from ..physical.async_plan import AsyncPrefetchNode
from ..planning.async_utils import close_async_iterators
from ..runtime.query import QueryRuntime
from ..runtime.resources import _add_cleanup_failure
from ..runtime.tasks import TaskRole, TaskScope

_END = object()


@dataclass(frozen=True, slots=True)
class _Value:
    """Carry one accepted value while its capacity permit remains held."""

    value: Any


@dataclass(frozen=True, slots=True)
class _Failure:
    """Carry a producer failure behind every value accepted before it."""

    error: BaseException


async def _produce(
    source: AsyncIterator[Any],
    messages: asyncio.Queue[object],
    permits: asyncio.Semaphore,
) -> None:
    """Pull only after reserving room for the value that may be accepted."""
    while True:
        await permits.acquire()
        try:
            value = await anext(source)
        except StopAsyncIteration:
            permits.release()
            messages.put_nowait(_END)
            return
        except asyncio.CancelledError as error:
            permits.release()
            task = asyncio.current_task()
            if task is not None and task.cancelling():
                raise
            messages.put_nowait(_Failure(error))
            return
        except BaseException as error:
            permits.release()
            messages.put_nowait(_Failure(error))
            return
        messages.put_nowait(_Value(value))


async def _close_prefetch(
    source: AsyncIterator[Any],
    scope: TaskScope,
    active_error: BaseException | None,
) -> None:
    """Run task and source cleanup while preserving the primary failure."""
    if isinstance(active_error, GeneratorExit):
        active_error = None
    cleanup_errors: list[BaseException] = []
    try:
        await scope.aclose()
    except BaseException as error:
        cleanup_errors.append(error)
    try:
        await close_async_iterators((source,))
    except BaseException as error:
        cleanup_errors.append(error)
    _add_cleanup_failure(active_error, cleanup_errors)


async def execute_async_prefetch(
    source: AsyncIterator[Any], node: AsyncPrefetchNode, runtime: QueryRuntime
) -> AsyncIterator[Any]:
    """Preserve source order while one query-owned producer overlaps downstream work."""
    messages: asyncio.Queue[object] = asyncio.Queue(maxsize=node.operation.capacity)
    permits = asyncio.Semaphore(node.operation.capacity)
    scope = runtime.tasks.scope(f"prefetch:{node.logical_ids[0]}", max_tasks=1)
    active_error: BaseException | None = None
    try:
        producer = scope.create_task(_produce(source, messages, permits), role=TaskRole.SOURCE)
        while True:
            message = await messages.get()
            if isinstance(message, _Value):
                permits.release()
                yield message.value
                continue
            await scope.take_result(producer)
            if isinstance(message, _Failure):
                raise message.error
            return
    except BaseException as error:
        active_error = error
        raise
    finally:
        await _close_prefetch(source, scope, active_error)
