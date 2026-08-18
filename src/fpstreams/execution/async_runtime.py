"""Track tasks and iterators owned by one concurrent async operator."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, TypeVar

from ..planning.async_utils import _close, close_async_iterators

T = TypeVar("T")


class TaskRole(StrEnum):
    """Identify the work performed by a task owned by an async operator."""

    SOURCE_PULL = "source_pull"
    INNER_OPEN = "inner_open"
    INNER_PULL = "inner_pull"
    USER_CALL = "user_call"
    TIMER = "timer"


@dataclass(slots=True)
class _TaskRecord:
    """Pair an owned task with its role and iterator-returning cleanup contract."""

    task: asyncio.Task[Any]
    role: TaskRole
    returns_iterator: bool = False


class AsyncRuntime:
    """Own the tasks and iterators created inside one concurrent operator.

    Closing cancels and awaits unfinished tasks, adopts iterators returned by tasks
    that completed successfully, and then closes all iterators in reverse
    registration order. This also covers an inner iterator created concurrently
    just before its mapping task was cancelled by downstream cleanup.
    """

    def __init__(self) -> None:
        """Initialize an open runtime with no owned tasks or iterators."""
        self._iterators: list[Any] = []
        self._tasks: dict[asyncio.Task[Any], _TaskRecord] = {}
        self._closed = False

    def own_iterator(self, iterator: Any) -> Any:
        """Register an iterator once and return it for inline assignment."""
        if self._closed:
            raise RuntimeError("async runtime is closed")
        if iterator not in self._iterators:
            self._iterators.append(iterator)
        return iterator

    async def release_iterator(self, iterator: Any, *, close: bool) -> None:
        """Release a registered iterator, optionally awaiting its close immediately."""
        if iterator in self._iterators:
            self._iterators.remove(iterator)
        if close:
            await _close(iterator)

    def create_task(
        self, awaitable: Awaitable[T], *, role: TaskRole, returns_iterator: bool = False
    ) -> asyncio.Task[T]:
        """Schedule an awaitable and record how its result must be cleaned up.

        Set returns_iterator for tasks whose successful result becomes an owned
        iterator even if the operator exits before it can consume that result.
        """
        if self._closed:
            raise RuntimeError("async runtime is closed")
        task = asyncio.ensure_future(awaitable)
        self._tasks[task] = _TaskRecord(task, role, returns_iterator)
        return task

    def finish_task(self, task: asyncio.Task[T]) -> T:
        """Remove a completed task from the registry and return or raise its result."""
        self._tasks.pop(task, None)
        return task.result()

    async def cancel_task(self, task: asyncio.Task[Any]) -> None:
        """Cancel a task if needed, await it without leaking its exception, and forget it."""
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self._tasks.pop(task, None)

    async def aclose(self, *, active_error: BaseException | None = None) -> None:
        """Idempotently cancel tasks, adopt successful iterator results, and close.

        Task exceptions are drained during cleanup. Iterator close failures are
        delegated to close_async_iterators, which annotates active_error or raises
        the first cleanup failure when no earlier exception is in flight.
        """
        if self._closed:
            return
        records = list(self._tasks.values())
        for record in records:
            if not record.task.done():
                record.task.cancel()
        if records:
            await asyncio.gather(*(record.task for record in records), return_exceptions=True)
        for record in records:
            if (
                not record.returns_iterator
                or record.task.cancelled()
                or record.task.exception() is not None
            ):
                continue
            result = record.task.result()
            if result is not None:
                self.own_iterator(result)
        self._closed = True
        self._tasks.clear()
        await close_async_iterators(reversed(self._iterators), active_error=active_error)
        self._iterators.clear()
