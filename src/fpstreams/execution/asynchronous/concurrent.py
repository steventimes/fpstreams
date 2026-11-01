"""Bounded completion primitives used by async physical schedulers."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

from ...physical.async_plan import AsyncMapNode, AsyncPrefetchNode
from ...planning.async_utils import close_async_iterators
from ...runtime.query import QueryRuntime
from ...runtime.resources import _add_cleanup_failure, run_async_cleanup
from ...runtime.tasks import TaskRole, TaskScope

_MISSING = object()


@dataclass(frozen=True, slots=True)
class Completion:
    """The value or error associated with one logical sequence number."""

    sequence: int
    value: object = _MISSING
    error: BaseException | None = None

    @classmethod
    def from_task(cls, sequence: int, task: asyncio.Task[Any]) -> Completion:
        """Capture a completed task outcome without raising from a done callback."""
        if task.cancelled():
            return cls(sequence, error=asyncio.CancelledError())
        try:
            return cls(sequence, value=task.result())
        except BaseException as error:
            return cls(sequence, error=error)

    @classmethod
    def value_of(cls, sequence: int, value: object) -> Completion:
        """Create a successful completion without allocating a temporary task."""
        return cls(sequence, value=value)

    def result(self) -> object:
        """Return the completed value or raise its captured error."""
        if self.error is not None:
            raise self.error
        return self.value


class OrderedResultRing:
    """Fixed-capacity sequence slots that release only the next logical completion."""

    def __init__(self, capacity: int) -> None:
        if capacity < 1:
            raise ValueError("ordered ring capacity must be at least 1")
        self._slots: list[Completion | None] = [None] * capacity
        self._next = 0

    def put(self, sequence: int, completion: Completion) -> None:
        """Store a completion unless its fixed slot is still occupied by another sequence."""
        if sequence != completion.sequence:
            raise ValueError("completion sequence does not match slot sequence")
        slot = sequence % len(self._slots)
        occupied = self._slots[slot]
        if occupied is not None:
            raise RuntimeError(f"ordered result ring slot {slot} is occupied")
        self._slots[slot] = completion

    def pop_next(self) -> Completion | None:
        """Release exactly the next sequence when it has completed."""
        slot = self._next % len(self._slots)
        completion = self._slots[slot]
        if completion is None or completion.sequence != self._next:
            return None
        self._slots[slot] = None
        self._next += 1
        return completion


class CompletionQueue:
    """An unbounded completion-order queue populated by task done callbacks."""

    def __init__(self) -> None:
        self._queue: asyncio.Queue[Completion] = asyncio.Queue()

    def publish(self, completion: Completion) -> None:
        """Enqueue a captured task outcome without blocking its done callback."""
        self._queue.put_nowait(completion)

    def watch(self, task: asyncio.Task[Any], *, sequence: int) -> None:
        """Publish one captured outcome when task settles without invoking user callbacks."""
        task.add_done_callback(
            lambda settled: self.publish(Completion.from_task(sequence, settled))
        )

    async def get(self) -> Completion:
        """Wait for the next completion in physical finish order."""
        return await self._queue.get()

    def empty(self) -> bool:
        """Report whether a completion is already available without waiting."""
        return self._queue.empty()


async def _call(node: AsyncMapNode, item: Any) -> Any:
    """Resolve one synchronous-or-awaitable mapper call under its optional timeout."""

    async def invoke() -> Any:
        """Normalize a mapper's immediate value or awaitable to one awaited result."""
        result = node.operation.function(item)
        return await result if inspect.isawaitable(result) else result

    if node.operation.timeout is None:
        return await invoke()
    return await asyncio.wait_for(invoke(), node.operation.timeout)


async def execute_async_map(
    source: AsyncIterator[Any], node: AsyncMapNode, runtime: QueryRuntime
) -> AsyncIterator[Any]:
    """Execute bounded map work with exact ordered or completion-order semantics."""
    operation = node.operation
    # One owner cancels both kinds of work before awaiting either cleanup.
    # Mapper admission below retains its own concurrency and result limits.
    scope = runtime.tasks.scope(f"map:{node.logical_ids[0]}", max_tasks=operation.concurrency + 1)
    active_error: BaseException | None = None
    try:
        if operation.concurrency == 1:
            async for item in source:
                yield await _call(node, item)
            return

        completions = CompletionQueue()
        pending: dict[int, Any] = {}
        source_pull: asyncio.Task[Any] | None = None
        next_sequence = 0
        emitted = 0
        source_done = False
        ring = OrderedResultRing(operation.buffer) if operation.ordered else None

        async def fill() -> None:
            """Fill ready inputs in one burst while slow pulls remain independently owned."""
            nonlocal next_sequence, source_done
            while (
                len(pending) < operation.concurrency and next_sequence - emitted < operation.buffer
            ):
                try:
                    item = await anext(source)
                except StopAsyncIteration:
                    source_done = True
                    return
                task = scope.create_task(_call(node, item), role=TaskRole.USER_CALL)
                pending[next_sequence] = task
                completions.watch(task, sequence=next_sequence)
                next_sequence += 1

        while pending or not source_done or source_pull is not None:
            # One producer reserves a result slot before each pull; mapper
            # admission and the query-wide task budget remain separate limits.
            if (
                not source_done
                and source_pull is None
                and len(pending) < operation.concurrency
                and next_sequence - emitted < operation.buffer
            ):
                source_pull = scope.create_task(fill(), role=TaskRole.SOURCE)
                completions.watch(source_pull, sequence=-1)

            completion = await completions.get()
            if completion.sequence == -1:
                assert source_pull is not None
                try:
                    # Await the settled pull to preserve an upstream
                    # CancelledError's payload as well as its exception type.
                    await scope.take_result(source_pull)
                finally:
                    source_pull = None
                continue
            task = pending.pop(completion.sequence)
            scope.release_observed(task, successful=completion.error is None)
            if ring is None:
                emitted += 1
                yield completion.result()
                continue
            ring.put(completion.sequence, completion)
            while (ready := ring.pop_next()) is not None:
                emitted += 1
                yield ready.result()
    except BaseException as error:
        active_error = error
        raise
    finally:
        await run_async_cleanup(
            (scope.aclose, lambda: close_async_iterators((source,))),
            active_error,
        )


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
