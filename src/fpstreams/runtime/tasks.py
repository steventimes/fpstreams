"""Bounded ownership of ordinary asyncio tasks created by one query."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Coroutine
from enum import StrEnum
from typing import Any, TypeVar

from . import failpoints
from .limits import QueryLimits
from .metrics import QueryMetrics
from .resources import _add_cleanup_failure

T = TypeVar("T")


def _dispose_unscheduled(
    awaitable: Awaitable[Any], active_error: BaseException | None = None
) -> None:
    """Retire a rejected awaitable without masking its admission failure."""
    try:
        if isinstance(awaitable, asyncio.Future):
            awaitable.cancel()
            return
        close = getattr(awaitable, "close", None)
        if callable(close):
            close()
    except BaseException as error:
        _add_cleanup_failure(active_error, [error])


async def _await_cleanup_task(task: asyncio.Future[None]) -> None:
    """Finish one shared cleanup task before propagating caller cancellation."""
    if asyncio.current_task() is task:
        return
    cancellation: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as error:
            if cancellation is None:
                cancellation = error
        except BaseException:
            break

    try:
        task.result()
    except BaseException as error:
        if cancellation is None:
            raise
        _add_cleanup_failure(cancellation, [error])
    if cancellation is not None:
        raise cancellation


def _start_cleanup_task(coroutine: Coroutine[Any, Any, None]) -> asyncio.Task[None]:
    """Submit cleanup without leaking its coroutine or committing a half-closed owner."""
    try:
        return asyncio.create_task(coroutine)
    except BaseException as error:
        _dispose_unscheduled(coroutine, error)
        raise


class TaskRole(StrEnum):
    """Describe why a query owns a task for diagnostics and later scheduling policies."""

    USER_CALL = "user_call"
    SOURCE = "source"
    OPERATOR = "operator"


class TaskRuntime:
    """Create bounded tasks and retire them as soon as completion is observed."""

    def __init__(self, limits: QueryLimits, metrics: QueryMetrics) -> None:
        self._limits = limits
        self._metrics = metrics
        self._live: dict[asyncio.Task[Any], TaskRole] = {}
        self._scopes: list[TaskScope] = []
        self._admitting = 0
        self._closed = False
        self._close_task: asyncio.Future[None] | None = None

    @property
    def live_count(self) -> int:
        """Return currently owned tasks that have not retired."""
        return len(self._live)

    def create_task(self, awaitable: Awaitable[T], *, role: TaskRole) -> asyncio.Task[T]:
        """Schedule an awaitable only after the task limit is checked."""
        return self._create_task(awaitable, role=role, dispose_on_submit_failure=False)

    def _create_task(
        self,
        awaitable: Awaitable[T],
        *,
        role: TaskRole,
        dispose_on_submit_failure: bool,
        owner: TaskScope | None = None,
    ) -> asyncio.Task[T]:
        """Schedule after admission, optionally retiring an internally owned awaitable."""
        from .failpoints import hit

        self._check_admission()
        self._admitting += 1
        if owner is not None:
            owner._admitting += 1
        try:
            try:
                task = asyncio.ensure_future(awaitable)
            except BaseException as error:
                if dispose_on_submit_failure:
                    _dispose_unscheduled(awaitable, error)
                raise
            self._live[task] = role
            self._metrics.live_tasks = len(self._live)
            self._metrics.high_water_tasks = max(
                self._metrics.high_water_tasks, self._metrics.live_tasks
            )
            task.add_done_callback(self._retire_live)
            if owner is not None:
                owner._tasks.add(task)
        finally:
            self._admitting -= 1
            if owner is not None:
                owner._admitting -= 1
        try:
            hit("task.create.after")
        except BaseException:
            task.cancel()
            raise
        return task

    def _check_admission(self) -> None:
        """Reject task creation before ownership transfers from the caller."""
        if self._closed:
            raise RuntimeError("task runtime is closed")
        if len(self._live) + self._admitting >= self._limits.max_tasks:
            raise RuntimeError(f"task limit exceeded: max_tasks={self._limits.max_tasks}")

    def scope(self, name: str, *, max_tasks: int | None = None) -> TaskScope:
        """Create a child ownership scope while retaining the query-wide task limit."""
        if self._closed:
            raise RuntimeError("task runtime is closed")
        scope = TaskScope(self, name, max_tasks=max_tasks)
        self._scopes.append(scope)
        return scope

    def _retire_live(self, task: asyncio.Task[Any]) -> None:
        """Forget a settled task idempotently and refresh the observable live count."""
        self._live.pop(task, None)
        self._metrics.live_tasks = len(self._live)

    def _owns(self, task: asyncio.Task[Any] | None) -> bool:
        """Return whether ``task`` is currently query-owned."""
        return task is not None and task in self._live

    async def take_result(self, task: asyncio.Task[T]) -> T:
        """Await a task and retire it even when it raises or is cancelled."""
        from .failpoints import hit

        try:
            result = await task
            hit("task.complete.before_publish")
            return result
        finally:
            self._retire_live(task)

    async def cancel(self, task: asyncio.Task[Any]) -> None:
        """Cancel one task and wait for it to settle without leaking ownership."""
        from .failpoints import hit

        hit("task.cancel.before")
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self._retire_live(task)

    async def aclose(self) -> None:
        """Cancel and retire all still-live tasks exactly once."""
        if self._admitting or self._owns(asyncio.current_task()):
            raise RuntimeError("owned task cannot close its task runtime")
        if self._close_task is None:
            if self._closed:
                return
            close_task = _start_cleanup_task(self._aclose_impl())
            self._close_task = close_task
            self._closed = True
        await _await_cleanup_task(self._close_task)

    async def _aclose_impl(self) -> None:
        """Run the one-way task cleanup body under a cancellation-safe owner."""
        for scope in reversed(self._scopes):
            await scope._aclose_from_owner()
        self._scopes.clear()
        tasks = tuple(self._live)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for task in tasks:
            self._retire_live(task)

    async def _aclose_from_owner(self) -> None:
        """Run inline when a parent cleanup task already shields cancellation."""
        if self._close_task is not None:
            await _await_cleanup_task(self._close_task)
            return
        if self._closed:
            return
        completion = asyncio.get_running_loop().create_future()
        self._close_task = completion
        self._closed = True
        try:
            await self._aclose_impl()
        except BaseException as error:
            completion.set_exception(error)
            completion.exception()
            raise
        else:
            completion.set_result(None)


class TaskScope:
    """Independently cancellable child ownership under a query-wide :class:`TaskRuntime`."""

    def __init__(self, runtime: TaskRuntime, name: str, *, max_tasks: int | None) -> None:
        if not name:
            raise ValueError("task scope name cannot be empty")
        if max_tasks is not None and max_tasks < 1:
            raise ValueError("task scope max_tasks must be at least 1")
        self._runtime = runtime
        self.name = name
        self._max_tasks = max_tasks
        self._tasks: set[asyncio.Task[Any]] = set()
        self._admitting = 0
        self._closed = False
        self._close_task: asyncio.Future[None] | None = None

    @property
    def live_count(self) -> int:
        """Return the tasks still owned by this scope."""
        return len(self._tasks)

    def create_task(self, awaitable: Awaitable[T], *, role: TaskRole) -> asyncio.Task[T]:
        """Take ownership and create a task subject to scope and query-wide limits."""
        try:
            if self._closed:
                raise RuntimeError("task scope is closed")
            if (
                self._max_tasks is not None
                and len(self._tasks) + self._admitting >= self._max_tasks
            ):
                raise RuntimeError(f"task scope limit exceeded: {self.name}")
            self._runtime._check_admission()
        except BaseException as error:
            _dispose_unscheduled(awaitable, error)
            raise
        task = self._runtime._create_task(
            awaitable,
            role=role,
            dispose_on_submit_failure=True,
            owner=self,
        )
        return task

    async def take_result(self, task: asyncio.Task[T]) -> T:
        """Observe one owned task result and release scope ownership."""
        if task not in self._tasks:
            raise ValueError("task is not owned by this scope")
        try:
            return await self._runtime.take_result(task)
        finally:
            self._tasks.discard(task)

    def release_observed(self, task: asyncio.Task[Any], *, successful: bool) -> None:
        """Release a completed task whose outcome another owner has already captured."""
        if task not in self._tasks:
            raise ValueError("task is not owned by this scope")
        if not task.done():
            raise ValueError("task is not complete")
        try:
            if successful:
                failpoints.hit("task.complete.before_publish")
        finally:
            self._runtime._retire_live(task)
            self._tasks.discard(task)

    async def cancel(self, task: asyncio.Task[Any]) -> None:
        """Cancel and drain one task that this scope owns."""
        if task not in self._tasks:
            raise ValueError("task is not owned by this scope")
        await self._runtime.cancel(task)
        self._tasks.discard(task)

    async def aclose(self) -> None:
        """Cancel and drain only this scope's tasks, leaving siblings active."""
        if self._admitting or asyncio.current_task() in self._tasks:
            raise RuntimeError("owned task cannot close its task scope")
        if self._close_task is None:
            if self._closed:
                return
            close_task = _start_cleanup_task(self._aclose_impl())
            self._close_task = close_task
            self._closed = True
        await _await_cleanup_task(self._close_task)

    async def _aclose_impl(self) -> None:
        """Drain this scope once under a cancellation-safe cleanup owner."""
        tasks = tuple(self._tasks)
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for task in tasks:
            self._runtime._retire_live(task)
        self._tasks.clear()

    async def _aclose_from_owner(self) -> None:
        """Run inline when the task runtime already owns cancellation shielding."""
        if self._close_task is not None:
            await _await_cleanup_task(self._close_task)
            return
        if self._closed:
            return
        completion = asyncio.get_running_loop().create_future()
        self._close_task = completion
        self._closed = True
        try:
            await self._aclose_impl()
        except BaseException as error:
            completion.set_exception(error)
            completion.exception()
            raise
        else:
            completion.set_result(None)
