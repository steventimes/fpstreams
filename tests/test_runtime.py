# ruff: noqa: E402
"""Query runtime, task scheduling, resource ownership, and spill contracts."""

from __future__ import annotations

# --- Consolidated from runtime/test_async_scheduler.py ---

"""Contracts for bounded async scheduler primitives."""


import asyncio

import pytest

from fpstreams.execution.async_queue import Completion, CompletionQueue, OrderedResultRing
from fpstreams.execution.async_timers import TimerHandle
from fpstreams.runtime import QueryLimits
from fpstreams.runtime.metrics import QueryMetrics
from fpstreams.runtime.tasks import TaskRole, TaskRuntime


def test_free_threaded_flow_smoke() -> None:
    """Exercise shared Flow plans only on a GIL-disabled interpreter."""
    import os
    import sys
    from concurrent.futures import ThreadPoolExecutor

    import fpstreams

    if getattr(sys, "_is_gil_enabled", lambda: True)():
        pytest.skip("requires a free-threaded interpreter")

    thread_count = int(os.environ.get("FPSTREAMS_FT_SMOKE_THREADS", "16"))
    iterations = int(os.environ.get("FPSTREAMS_FT_SMOKE_ITERATIONS", "200"))
    values = list(range(100))
    expected = sum(value * 2 for value in values if value % 3 == 0)

    def work(_worker: int) -> int:
        result = 0
        for _ in range(iterations):
            result += (
                fpstreams.Flow.of(*values)
                .map(lambda value: value * 2)
                .filter(lambda value: value % 3 == 0)
                .sum()
            )
        return result

    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        results = list(executor.map(work, range(thread_count)))

    assert results == [expected * iterations] * thread_count
    assert asyncio.run(fpstreams.AsyncFlow.of(1, 2, 3).count()) == 3


@pytest.mark.asyncio
async def test_ordered_ring_releases_only_next_sequence() -> None:
    ring = OrderedResultRing(capacity=3)
    ring.put(1, Completion.value_of(1, "one"))
    assert ring.pop_next() is None
    ring.put(0, Completion.value_of(0, "zero"))
    assert ring.pop_next().result() == "zero"  # type: ignore[union-attr]
    assert ring.pop_next().result() == "one"  # type: ignore[union-attr]


def test_ordered_ring_rejects_slot_overwrite() -> None:
    ring = OrderedResultRing(capacity=2)
    ring.put(0, Completion.value_of(0, 0))
    with pytest.raises(RuntimeError, match="occupied"):
        ring.put(2, Completion.value_of(2, 2))


@pytest.mark.asyncio
async def test_completion_queue_publishes_done_task_outcome() -> None:
    queue = CompletionQueue()
    task = asyncio.create_task(asyncio.sleep(0, result=3))
    queue.watch(task, sequence=7)
    assert (await queue.get()).result() == 3


@pytest.mark.asyncio
async def test_task_scope_cancel_does_not_cancel_sibling_scope() -> None:
    runtime = TaskRuntime(QueryLimits(max_tasks=10), QueryMetrics())
    left = runtime.scope("left", max_tasks=2)
    right = runtime.scope("right", max_tasks=2)
    left_task = left.create_task(asyncio.sleep(10), role=TaskRole.USER_CALL)
    right_task = right.create_task(asyncio.sleep(0, result=3), role=TaskRole.USER_CALL)

    await left.aclose()

    assert left_task.cancelled()
    assert await right.take_result(right_task) == 3
    await runtime.aclose()


@pytest.mark.asyncio
async def test_timer_handle_replaces_and_owns_only_one_timer() -> None:
    runtime = TaskRuntime(QueryLimits(max_tasks=3), QueryMetrics())
    scope = runtime.scope("timer", max_tasks=1)
    timer = TimerHandle(scope)
    await timer.arm(10)
    assert timer.armed
    await timer.arm(0)
    assert scope.live_count == 1
    await timer.take()
    assert not timer.armed
    await runtime.aclose()


# --- Consolidated from runtime/test_task_runtime.py ---

"""Task and spill ownership contracts for one query runtime."""


import pytest

from fpstreams.runtime import (
    ResourceRegistry,
    SpillFileRegistry,
)


@pytest.mark.asyncio
async def test_completed_tasks_leave_live_registry_before_query_close() -> None:
    """Awaited tasks retire immediately rather than accumulating until query shutdown."""
    metrics = QueryMetrics()
    tasks = TaskRuntime(QueryLimits(max_tasks=2), metrics)
    for value in range(100):
        task = tasks.create_task(asyncio.sleep(0, result=value), role=TaskRole.USER_CALL)
        assert await tasks.take_result(task) == value
        assert tasks.live_count == 0
    assert metrics.high_water_tasks <= 1


@pytest.mark.asyncio
async def test_task_scope_close_drains_completed_unobserved_failure() -> None:
    """A settled failure stays scope-owned until its result is observed or drained."""
    tasks = TaskRuntime(QueryLimits(max_tasks=2), QueryMetrics())
    scope = tasks.scope("failure", max_tasks=1)

    async def fail() -> None:
        raise RuntimeError("task failed")

    task = scope.create_task(fail(), role=TaskRole.USER_CALL)
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert task.done()
    assert scope.live_count == 1
    assert task._log_traceback  # type: ignore[attr-defined]

    await scope.aclose()

    assert not task._log_traceback  # type: ignore[attr-defined]
    assert scope.live_count == 0
    await tasks.aclose()


@pytest.mark.asyncio
async def test_task_scope_cancel_failure_retains_ownership_for_close() -> None:
    tasks = TaskRuntime(QueryLimits(max_tasks=2), QueryMetrics())
    scope = tasks.scope("cancel", max_tasks=1)
    task = scope.create_task(asyncio.sleep(10), role=TaskRole.USER_CALL)

    with (
        failpoint("task.cancel.before", OSError("cancel failed")),
        pytest.raises(OSError, match="cancel failed"),
    ):
        await scope.cancel(task)

    assert scope.live_count == 1
    assert tasks.live_count == 1
    assert not task.done()

    await scope.aclose()

    assert task.cancelled()
    assert scope.live_count == 0
    assert tasks.live_count == 0
    await tasks.aclose()


@pytest.mark.asyncio
async def test_task_runtime_close_finishes_all_scopes_before_propagating_cancellation() -> None:
    tasks = TaskRuntime(QueryLimits(max_tasks=3), QueryMetrics())
    sibling = tasks.scope("sibling", max_tasks=1)
    stubborn = tasks.scope("stubborn", max_tasks=1)
    sibling_task = sibling.create_task(asyncio.sleep(10), role=TaskRole.OPERATOR)
    cancellation_seen = asyncio.Event()
    release = asyncio.Event()

    async def resist_one_cancellation() -> None:
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancellation_seen.set()
            await release.wait()

    stubborn_task = stubborn.create_task(resist_one_cancellation(), role=TaskRole.OPERATOR)
    close_task = asyncio.create_task(tasks.aclose())
    await cancellation_seen.wait()

    close_task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await close_task

    assert stubborn_task.done()
    assert sibling_task.cancelled()
    assert tasks.live_count == 0
    await tasks.aclose()


@pytest.mark.asyncio
async def test_query_runtime_close_releases_resources_before_propagating_cancellation() -> None:
    runtime = QueryRuntime(QueryLimits(max_tasks=2))
    scope = runtime.tasks.scope("stubborn", max_tasks=1)
    cancellation_seen = asyncio.Event()
    release = asyncio.Event()
    cleanup_calls: list[str] = []

    async def resist_one_cancellation() -> None:
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancellation_seen.set()
            await release.wait()

    task = scope.create_task(resist_one_cancellation(), role=TaskRole.OPERATOR)
    runtime.resources.own("resource", cleanup_calls.append)
    close_task = asyncio.create_task(runtime.aclose())
    await cancellation_seen.wait()

    close_task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await close_task

    assert task.done()
    assert runtime.tasks.live_count == 0
    assert runtime.resources.closed
    assert cleanup_calls == ["resource"]
    await runtime.aclose()


@pytest.mark.asyncio
async def test_query_runtime_resource_closer_can_reenter_close() -> None:
    runtime = QueryRuntime()
    cleanup_calls = 0

    async def close(_value: str) -> None:
        nonlocal cleanup_calls
        cleanup_calls += 1
        await runtime.aclose()

    await runtime.resources.aown("resource", close)

    await runtime.aclose()

    assert cleanup_calls == 1
    assert runtime.resources.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("owner", ["scope", "tasks", "query"])
async def test_owned_task_cannot_close_its_own_owner(owner: str) -> None:
    runtime = QueryRuntime(QueryLimits(max_tasks=2))
    scope = runtime.tasks.scope("owned", max_tasks=1)

    async def close_owner() -> None:
        with pytest.raises(RuntimeError, match="owned task cannot close"):
            if owner == "scope":
                await scope.aclose()
            elif owner == "tasks":
                await runtime.tasks.aclose()
            else:
                await runtime.aclose()

    task = scope.create_task(close_owner(), role=TaskRole.OPERATOR)

    await scope.take_result(task)
    await runtime.aclose()


@pytest.mark.asyncio
@pytest.mark.skipif(
    not hasattr(asyncio, "eager_task_factory"),
    reason="asyncio eager task factory requires Python 3.12+",
)
@pytest.mark.parametrize("owner", ["scope", "tasks", "query"])
async def test_eager_owned_task_cannot_close_its_own_owner(owner: str) -> None:
    runtime = QueryRuntime(QueryLimits(max_tasks=2))
    scope = runtime.tasks.scope("eager-owned", max_tasks=1)

    async def close_owner() -> None:
        with pytest.raises(RuntimeError, match="owned task cannot close"):
            if owner == "scope":
                await scope.aclose()
            elif owner == "tasks":
                await runtime.tasks.aclose()
            else:
                await runtime.aclose()

    loop = asyncio.get_running_loop()
    previous_factory = loop.get_task_factory()
    factory = asyncio.eager_task_factory  # type: ignore[attr-defined]
    loop.set_task_factory(factory)
    try:
        task = scope.create_task(close_owner(), role=TaskRole.OPERATOR)
    finally:
        loop.set_task_factory(previous_factory)

    await scope.take_result(task)
    await runtime.aclose()


@pytest.mark.asyncio
@pytest.mark.skipif(
    not hasattr(asyncio, "eager_task_factory"),
    reason="asyncio eager task factory requires Python 3.12+",
)
async def test_eager_task_creation_counts_inflight_runtime_admission() -> None:
    runtime = QueryRuntime(QueryLimits(max_tasks=1))
    scope = runtime.tasks.scope("eager-limit")

    async def create_nested() -> None:
        with pytest.raises(RuntimeError, match="max_tasks=1"):
            scope.create_task(asyncio.sleep(0), role=TaskRole.OPERATOR)

    loop = asyncio.get_running_loop()
    previous_factory = loop.get_task_factory()
    factory = asyncio.eager_task_factory  # type: ignore[attr-defined]
    loop.set_task_factory(factory)
    try:
        task = scope.create_task(create_nested(), role=TaskRole.OPERATOR)
    finally:
        loop.set_task_factory(previous_factory)

    await scope.take_result(task)
    await runtime.aclose()


@pytest.mark.asyncio
async def test_concurrent_query_close_waiters_observe_the_same_cleanup_failure() -> None:
    runtime = QueryRuntime()
    entered = asyncio.Event()
    release = asyncio.Event()
    failure = OSError("resource close failed")
    failure.add_note("resource cleanup detail")

    async def fail_close(_value: str) -> None:
        entered.set()
        await release.wait()
        raise failure

    await runtime.resources.aown("resource", fail_close)
    left_error = ValueError("left failed")
    right_error = LookupError("right failed")
    left = asyncio.create_task(runtime.aclose(left_error))
    await entered.wait()
    right = asyncio.create_task(runtime.aclose(right_error))
    plain = asyncio.create_task(runtime.aclose())
    release.set()

    results = await asyncio.gather(left, right, plain, return_exceptions=True)

    assert results[:2] == [None, None]
    assert results[2] is failure
    expected_notes = [
        "cleanup failed: OSError: resource close failed",
        "resource cleanup detail",
    ]
    assert left_error.__notes__ == expected_notes
    assert right_error.__notes__ == expected_notes


@pytest.mark.asyncio
async def test_task_scope_disposes_awaitable_when_submission_fails(monkeypatch) -> None:
    import fpstreams.runtime.tasks as tasks_module

    class TrackedAwaitable:
        def __init__(self) -> None:
            self.close_calls = 0

        def __await__(self):
            if False:
                yield None
            return None

        def close(self) -> None:
            self.close_calls += 1

    def reject_submission(_awaitable):
        raise OSError("task submission failed")

    tasks = TaskRuntime(QueryLimits(max_tasks=2), QueryMetrics())
    scope = tasks.scope("submit")
    awaitable = TrackedAwaitable()
    monkeypatch.setattr(tasks_module.asyncio, "ensure_future", reject_submission)

    with pytest.raises(OSError, match="task submission failed"):
        scope.create_task(awaitable, role=TaskRole.USER_CALL)

    assert awaitable.close_calls == 1
    assert scope.live_count == 0
    assert tasks.live_count == 0
    await tasks.aclose()


@pytest.mark.asyncio
async def test_post_schedule_failure_remains_scope_owned_until_close() -> None:
    tasks = TaskRuntime(QueryLimits(max_tasks=2), QueryMetrics())
    scope = tasks.scope("post-schedule", max_tasks=1)

    with (
        failpoint("task.create.after", OSError("create failed")),
        pytest.raises(OSError, match="create failed"),
    ):
        scope.create_task(asyncio.sleep(10), role=TaskRole.OPERATOR)

    assert scope.live_count == 1
    assert tasks.live_count == 1
    await scope.aclose()
    assert scope.live_count == 0
    assert tasks.live_count == 0
    await tasks.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("owner", ["scope", "tasks", "query"])
async def test_cleanup_task_submission_failure_can_be_retried(owner: str, monkeypatch) -> None:
    import fpstreams.runtime.tasks as tasks_module

    runtime = QueryRuntime(QueryLimits(max_tasks=2))
    scope = runtime.tasks.scope("retry", max_tasks=1)
    task = scope.create_task(asyncio.sleep(10), role=TaskRole.OPERATOR)
    cleanup_calls: list[str] = []
    runtime.resources.own("resource", cleanup_calls.append)
    original_create_task = asyncio.create_task

    def reject_cleanup_task(*_args, **_kwargs):
        raise OSError("cleanup task submission failed")

    monkeypatch.setattr(tasks_module.asyncio, "create_task", reject_cleanup_task)
    target = scope if owner == "scope" else runtime.tasks if owner == "tasks" else runtime

    with pytest.raises(OSError, match="cleanup task submission failed"):
        await target.aclose()

    monkeypatch.setattr(tasks_module.asyncio, "create_task", original_create_task)
    await target.aclose()

    if owner in {"scope", "tasks"}:
        assert task.cancelled()
        await runtime.aclose()
    else:
        assert task.cancelled()
        assert cleanup_calls == ["resource"]


@pytest.mark.asyncio
async def test_query_runtime_cleanup_submits_only_one_owner_task() -> None:
    runtime = QueryRuntime(QueryLimits(max_tasks=2))
    scope = runtime.tasks.scope("single-owner", max_tasks=1)
    task = scope.create_task(asyncio.sleep(10), role=TaskRole.OPERATOR)
    cleanup_calls: list[str] = []
    runtime.resources.own("resource", cleanup_calls.append)
    loop = asyncio.get_running_loop()
    previous_factory = loop.get_task_factory()
    factory_calls = 0

    def reject_nested_factory(loop, coroutine, **kwargs):
        nonlocal factory_calls
        factory_calls += 1
        if factory_calls > 1:
            raise OSError("nested cleanup submission failed")
        return asyncio.Task(coroutine, loop=loop, **kwargs)

    loop.set_task_factory(reject_nested_factory)
    failure: BaseException | None = None
    try:
        await runtime.aclose()
    except BaseException as error:
        failure = error
    finally:
        loop.set_task_factory(previous_factory)

    if failure is not None:
        await runtime.tasks.aclose()
        await runtime.resources.aclose()

    assert failure is None
    assert factory_calls == 1
    assert task.cancelled()
    assert cleanup_calls == ["resource"]


@pytest.mark.asyncio
async def test_task_scope_disposal_failure_does_not_hide_admission_error() -> None:
    class FailingCloseAwaitable:
        def __await__(self):
            if False:
                yield None
            return None

        def close(self) -> None:
            raise OSError("awaitable close failed")

    tasks = TaskRuntime(QueryLimits(max_tasks=2), QueryMetrics())
    scope = tasks.scope("closed")
    await scope.aclose()

    with pytest.raises(RuntimeError, match="task scope is closed") as captured:
        scope.create_task(FailingCloseAwaitable(), role=TaskRole.USER_CALL)

    assert captured.value.__notes__ == ["cleanup failed: OSError: awaitable close failed"]
    await tasks.aclose()


@pytest.mark.asyncio
async def test_task_limit_rejects_before_scheduling_coroutine() -> None:
    """A rejected coroutine remains caller-owned and can be closed without a warning."""
    gate = asyncio.Event()
    tasks = TaskRuntime(QueryLimits(max_tasks=1), QueryMetrics())
    first = tasks.create_task(gate.wait(), role=TaskRole.USER_CALL)
    coroutine = asyncio.sleep(0)
    with pytest.raises(RuntimeError, match="max_tasks=1"):
        tasks.create_task(coroutine, role=TaskRole.USER_CALL)
    coroutine.close()
    gate.set()
    await tasks.take_result(first)


def test_spill_registry_removes_registered_directory(tmp_path) -> None:
    """A query-owned spill directory is removed on resource cleanup, not its parent."""
    resources = ResourceRegistry()
    spills = SpillFileRegistry(resources, QueryMetrics())
    directory = spills.create_directory(tmp_path)
    path = spills.register(directory / "run-000001.bin")
    path.write_bytes(b"abc")
    spills.record_write(3)
    resources.close()

    assert not directory.exists()


# --- Consolidated from runtime/test_query_runtime.py ---

"""Unit contracts for query-scoped resource ownership."""


from collections.abc import Iterator
from dataclasses import replace
from typing import Any

import pytest

from fpstreams import flow
from fpstreams.execution.physical import PhysicalExecutionError, execute_physical
from fpstreams.physical.plan import PhysicalNode
from fpstreams.planning.compiler import compile_query
from fpstreams.runtime import (
    FileLimitError,
    QueryRuntime,
)
from fpstreams.runtime.failpoints import failpoint
from fpstreams.storage import SpillStore


def test_file_manager_enforces_budget_and_releases_capacity(tmp_path) -> None:
    """Tracked files obey the hard query limit and retire metrics on early close."""
    runtime = QueryRuntime(QueryLimits(max_open_files=2))
    first = runtime.files.open(tmp_path / "first.bin", "wb")
    second = runtime.files.open(tmp_path / "second.bin", "wb")

    assert runtime.metrics.open_files == 2
    assert runtime.metrics.high_water_open_files == 2
    with pytest.raises(FileLimitError, match=r"current=2, limit=2"):
        runtime.files.open(tmp_path / "rejected.bin", "wb")

    first.close()
    replacement = runtime.files.open(tmp_path / "replacement.bin", "wb")
    assert runtime.metrics.open_files == 2
    runtime.close()

    assert first.closed and second.closed and replacement.closed
    assert runtime.metrics.open_files == 0


def test_released_file_leases_do_not_accumulate_registry_references(tmp_path) -> None:
    """Sequential spill I/O retains neither descriptors nor closed proxy objects."""
    runtime = QueryRuntime(QueryLimits(max_open_files=1))

    for position in range(1_000):
        runtime.files.open(tmp_path / f"lease-{position}.bin", "wb").close()

    assert runtime.metrics.open_files == 0
    assert runtime.resources._records == []
    runtime.close()


def test_file_registration_failure_returns_descriptor_lease(tmp_path) -> None:
    """A failure after ownership registration cannot strand an open descriptor."""
    runtime = QueryRuntime(QueryLimits(max_open_files=1))

    with (
        failpoint("resource.register.after", OSError("registration failed")),
        pytest.raises(OSError, match="registration failed"),
    ):
        runtime.files.open(tmp_path / "failed.bin", "wb")

    assert runtime.metrics.open_files == 0
    assert runtime.metrics.high_water_open_files == 1
    runtime.close()


def test_file_manager_serializes_concurrent_limit_admission(tmp_path, monkeypatch) -> None:
    """Two simultaneous opens cannot both pass a one-descriptor query limit."""
    from concurrent.futures import ThreadPoolExecutor
    from contextlib import suppress
    from pathlib import Path
    from threading import Barrier, BrokenBarrierError

    barrier = Barrier(2)
    original_open = Path.open

    def synchronized_open(path: Path, mode: str):
        # Without one admission lock both callers reach this barrier after the
        # same stale limit check. With the lock, the first times out and commits
        # its lease before the second caller can perform its check.
        with suppress(BrokenBarrierError):
            barrier.wait(timeout=0.2)
        return original_open(path, mode)

    monkeypatch.setattr(Path, "open", synchronized_open)
    runtime = QueryRuntime(QueryLimits(max_open_files=1))

    def acquire(position: int):
        try:
            return runtime.files.open(tmp_path / f"concurrent-{position}.bin", "wb")
        except FileLimitError:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        opened = [handle for handle in pool.map(acquire, range(2)) if handle is not None]

    assert len(opened) == 1
    assert runtime.metrics.open_files == 1
    opened[0].close()
    runtime.close()


def test_closed_runtime_rejects_new_file_and_spill_ownership(tmp_path) -> None:
    """Cleanup is one-way: reopening after close must not create an unowned lease."""
    runtime = QueryRuntime(QueryLimits(max_open_files=1))
    runtime.close()

    with pytest.raises(RuntimeError, match="closed"):
        runtime.files.open(tmp_path / "leaked.bin", "wb")
    with pytest.raises(RuntimeError, match="closed"):
        SpillStore(runtime, parent=tmp_path, operation="closed")

    assert runtime.metrics.open_files == 0
    assert list(tmp_path.iterdir()) == []


def test_resource_registry_closes_lifo_and_is_idempotent() -> None:
    """Owned synchronous resources close in reverse acquisition order exactly once."""
    events: list[str] = []
    resources = ResourceRegistry()
    resources.own("first", lambda value: events.append(value))
    resources.own("second", lambda value: events.append(value))
    resources.close()
    resources.close()

    assert events == ["second", "first"]


def test_cleanup_errors_are_notes_on_active_error() -> None:
    """Pipeline errors remain primary when cleanup has independent failures."""
    resources = ResourceRegistry()

    def fail(_value: object) -> None:
        raise OSError("close failed")

    resources.own(object(), fail)
    active = ValueError("pipeline failed")
    resources.close(active)

    assert active.__notes__ == ["cleanup failed: OSError: close failed"]


@pytest.mark.asyncio
async def test_async_registry_awaits_every_closer_in_lifo_order() -> None:
    """Async owners preserve the same LIFO semantics."""
    events: list[str] = []
    resources = ResourceRegistry()

    async def close(value: str) -> None:
        events.append(value)

    await resources.aown("first", close)
    await resources.aown("second", close)
    await resources.aclose()

    assert events == ["second", "first"]


@pytest.mark.asyncio
async def test_async_registry_early_release_forgets_owner_and_closes_once() -> None:
    """An explicitly retired async owner is neither retained nor closed again."""

    class NonIdempotentOwner:
        def __init__(self) -> None:
            self.close_calls = 0

        async def aclose(self) -> None:
            self.close_calls += 1
            if self.close_calls > 1:
                raise RuntimeError("async owner closed twice")

    resources = ResourceRegistry()
    owner = await resources.aown(NonIdempotentOwner())

    await resources.arelease(owner)
    await resources.arelease(owner)
    await resources.aclose()

    assert owner.close_calls == 1
    assert resources._records == []


@pytest.mark.asyncio
async def test_async_registry_release_keeps_active_error_primary() -> None:
    """Early async cleanup annotates, rather than replaces, an active failure."""
    close_calls = 0

    async def fail_close(_value: object) -> None:
        nonlocal close_calls
        close_calls += 1
        raise OSError("inner close failed")

    resources = ResourceRegistry()
    owner = await resources.aown(object(), fail_close)
    active = asyncio.CancelledError("query cancelled")

    await resources.arelease(owner, active)
    await resources.aclose()

    assert close_calls == 1
    assert active.__notes__ == ["cleanup failed: OSError: inner close failed"]


def test_query_runtime_closes_owned_resources_on_execution_error() -> None:
    """Injected runtimes close query-owned resources while keeping the pipeline error primary."""
    events: list[str] = []
    runtime = QueryRuntime()
    runtime.resources.own("closed", lambda value: events.append(value))
    pipeline = flow([1]).map(lambda _value: (_ for _ in ()).throw(ValueError("boom")))
    physical = compile_query(pipeline._query("list"))

    with pytest.raises(ValueError, match="boom"):
        list(execute_physical(physical, runtime=runtime))

    assert events == ["closed"]


def test_execute_physical_keeps_runtime_alive_until_iterator_close() -> None:
    """Query-owned resources stay live until an iterator completes or is closed."""
    cleanup_calls: list[str] = []
    runtime = QueryRuntime()
    runtime.resources.own("runtime", lambda value: cleanup_calls.append(value))
    physical = compile_query(flow(range(100))._query("iterate"))
    iterator = execute_physical(physical, runtime=runtime)

    assert next(iterator) == 0
    assert cleanup_calls == []
    close = getattr(iterator, "close", None)
    assert callable(close)
    close()
    assert cleanup_calls == ["runtime"]


def test_execute_physical_close_surfaces_runtime_cleanup_failure() -> None:
    """An explicit iterator close exposes a failing query-owned resource cleanup."""
    cleanup_calls: list[str] = []
    runtime = QueryRuntime()
    failure = OSError("runtime close failed")

    def fail_close(value: str) -> None:
        cleanup_calls.append(value)
        raise failure

    runtime.resources.own("runtime", fail_close)
    physical = compile_query(flow(range(100))._query("iterate"))
    iterator = execute_physical(physical, runtime=runtime)

    assert next(iterator) == 0
    close = getattr(iterator, "close", None)
    assert callable(close)
    with pytest.raises(OSError, match="runtime close failed") as captured:
        close()

    assert captured.value is failure
    assert cleanup_calls == ["runtime"]


def test_execute_physical_close_before_first_next_is_lazy_and_surfaces_cleanup_failure() -> None:
    """Closing an unstarted iterator retires its runtime without opening its source."""
    source_opens: list[str] = []
    cleanup_calls: list[str] = []
    runtime = QueryRuntime()
    failure = OSError("runtime close failed")

    def values() -> Iterator[int]:
        source_opens.append("source")
        return iter(range(3))

    def fail_close(value: str) -> None:
        cleanup_calls.append(value)
        raise failure

    runtime.resources.own("runtime", fail_close)
    physical = compile_query(flow.defer(values)._query("iterate"))
    iterator = execute_physical(physical, runtime=runtime)
    close = getattr(iterator, "close", None)

    assert callable(close)
    assert source_opens == []
    with pytest.raises(OSError, match="runtime close failed") as captured:
        close()

    assert captured.value is failure
    assert cleanup_calls == ["runtime"]
    assert source_opens == []


def test_execute_physical_setup_failure_closes_runtime_and_keeps_primary_error() -> None:
    """Physical setup errors remain primary while query-owned resources are closed."""
    cleanup_calls: list[str] = []
    runtime = QueryRuntime()

    def fail_close(value: str) -> None:
        cleanup_calls.append(value)
        raise OSError("runtime close failed")

    runtime.resources.own("runtime", fail_close)
    physical = compile_query(flow([1])._query("list"))
    invalid = replace(physical, nodes=(PhysicalNode((0,), "unknown"),))

    with pytest.raises(PhysicalExecutionError, match="unknown physical node") as captured:
        execute_physical(invalid, runtime=runtime)

    assert cleanup_calls == ["runtime"]
    assert captured.value.__notes__ == ["cleanup failed: OSError: runtime close failed"]


def test_execute_physical_invalid_backend_payload_closes_runtime() -> None:
    """Backend validation still retires resources owned by an injected runtime."""
    cleanup_calls: list[str] = []
    runtime = QueryRuntime()
    runtime.resources.own("runtime", lambda value: cleanup_calls.append(value))
    physical = compile_query(flow([1])._query("list"))
    invalid = replace(physical, backend_payload=object())  # type: ignore[arg-type]

    with pytest.raises(PhysicalExecutionError, match="unknown backend payload"):
        execute_physical(invalid, runtime=runtime)

    assert cleanup_calls == ["runtime"]


# --- Consolidated from runtime/test_spill_store.py ---

"""Versioned query-scoped SpillStore format and lifecycle contracts."""


import pytest

from fpstreams.execution.sorting import (
    PositionRecord,
    SortRecord,
    _collapse_sort_runs,
    _merge_position_runs,
    _merge_sort_runs,
    _sort_merge_fan_in,
)
from fpstreams.planning.sync import SortOp
from fpstreams.storage import SpillFormatError, SpillGeneration
from fpstreams.tabular.spill_io import (
    PartitionFile,
    SpillLazyWriter,
    SpillPartitionWriters,
    merge_ordered,
    read,
    repartition,
)


def test_spilled_count_preaggregation_planner_accepts_only_the_closed_count_shape() -> None:
    """A broader selector or collector must never acquire the count-only spill strategy."""
    import pickle

    import fpstreams

    class OutputName(str):
        pass

    records = [{"id": 1, "band": "a", "value": 2}]
    canonical_count = fpstreams.agg.count()
    cloned_count = fpstreams.ReducerAggregator(
        canonical_count.initializer,
        canonical_count.step,
        lambda count: count + 100,
        merge=canonical_count.combine,  # type: ignore[arg-type]
        laws=canonical_count.laws,
        done=canonical_count.done,
        native=canonical_count.native,
    )
    mutated_finish = fpstreams.agg.count()
    object.__setattr__(mutated_finish, "finish", lambda count: count + 100)
    mutated_done = fpstreams.agg.count()
    object.__setattr__(mutated_done, "done", lambda _count: False)
    pickled_count = pickle.loads(pickle.dumps(fpstreams.agg.count()))
    candidates = (
        fpstreams.rows(records).group_by("id").spill(2).aggregate(total=fpstreams.agg.count()),
        fpstreams.rows(records).group_by("id").spill(2).aggregate(total=fpstreams.agg.sum("value")),
        fpstreams.rows(records)
        .group_by("id")
        .spill(2)
        .aggregate(total=fpstreams.agg.collect("value")),
        fpstreams.rows(records)
        .group_by("id")
        .spill(2)
        .aggregate(total=fpstreams.Aggregator(lambda: 0, lambda state, _row: state + 1)),
        fpstreams.rows(records).group_by("id").spill(2).aggregate(total=cloned_count),
        fpstreams.rows(records).group_by("id").spill(2).aggregate(total=mutated_finish),
        fpstreams.rows(records).group_by("id").spill(2).aggregate(total=mutated_done),
        fpstreams.rows(records).group_by("id").spill(2).aggregate(total=pickled_count),
        fpstreams.rows(records)
        .group_by("id", "band")
        .spill(2)
        .aggregate(total=fpstreams.agg.count()),
        fpstreams.rows([{"nested": {"id": 1}}])
        .group_by("nested.id")
        .spill(2)
        .aggregate(total=fpstreams.agg.count()),
        fpstreams.rows(records)
        .group_by(lambda row: row["id"])
        .spill(2)
        .aggregate(total=fpstreams.agg.count()),
        fpstreams.rows(records)
        .group_by("id")
        .spill(2)
        .aggregate(**{OutputName("total"): fpstreams.agg.count()}),
        fpstreams.rows(records)
        .group_by("id")
        .spill(2)
        .aggregate(total=fpstreams.agg.count(), other=fpstreams.agg.count()),
    )

    planned = [
        getattr(compile_query(rows._flow._query("list")).root, "spill_count", None) is not None
        for rows in candidates
    ]

    assert planned == [
        True,
        False,
        False,
        False,
        False,
        False,
        False,
        False,
        False,
        False,
        False,
        False,
        False,
    ]


def _execute_spilled_count(
    records: list[dict[str, Any]],
    *,
    key_field: str,
    output_name: str,
    partitions: int = 8,
    instrumented: bool,
) -> tuple[list[dict[str, Any]], int]:
    """Execute one real spill-count plan and return its result and physical bytes."""
    import fpstreams

    grouped = (
        fpstreams.rows(records)
        .group_by(key_field)
        .spill(partitions)
        .aggregate(**{output_name: fpstreams.agg.count()})
    )
    physical = compile_query(grouped._flow._query("list"))
    runtime = QueryRuntime()
    if instrumented:
        with failpoint("unrelated.transition", RuntimeError("unused")):
            result = list(execute_physical(physical, runtime=runtime))
    else:
        result = list(execute_physical(physical, runtime=runtime))
    physical_bytes = runtime.metrics.spill_bytes
    runtime.close()
    return result, physical_bytes


def test_spilled_count_preaggregation_reduces_physical_spill_without_changing_results() -> None:
    """A repeated pure record key becomes one partial while an instrumented run stays raw."""
    records = [
        {"id": "hot", "position": position, "payload": "x" * 96} for position in range(4_096)
    ]

    optimized, optimized_bytes = _execute_spilled_count(
        records, key_field="id", output_name="total", instrumented=False
    )
    canonical, canonical_bytes = _execute_spilled_count(
        records, key_field="id", output_name="total", instrumented=True
    )

    assert optimized == canonical == [{"id": "hot", "total": len(records)}]
    assert optimized_bytes * 4 < canonical_bytes


def test_spilled_count_preaggregation_auto_colds_a_uniform_stream() -> None:
    """A bounded prefix with no frequency signal switches permanently to raw spilling."""
    records = [{"id": position, "payload": position} for position in range(20_000)]

    optimized, optimized_bytes = _execute_spilled_count(
        records, key_field="id", output_name="total", partitions=32, instrumented=False
    )
    canonical, canonical_bytes = _execute_spilled_count(
        records, key_field="id", output_name="total", partitions=32, instrumented=True
    )

    assert optimized == canonical
    assert optimized_bytes == canonical_bytes


def test_spilled_count_gate_rejects_many_shallow_repeats_without_a_hot_key(
    monkeypatch,
) -> None:
    """Aggregate repeat evidence alone must not admit hundreds of one-row partials."""
    import fpstreams
    from fpstreams.tabular import spill

    keys = [key for _round in range(2) for key in range(128)]
    keys.extend(key for _round in range(6) for key in range(128))
    keys.extend(key for _round in range(8) for key in range(128, 512))
    records = [
        {
            "segment": key,
            **{f"measure_{field}": position + field for field in range(4)},
        }
        for position, key in enumerate(keys)
    ]
    partials = 0
    original_partial = spill.group_count_partial

    def tracked_partial(*args, **kwargs):
        nonlocal partials
        partials += 1
        return original_partial(*args, **kwargs)

    monkeypatch.setattr(spill, "group_count_partial", tracked_partial)

    result = (
        fpstreams.rows(records)
        .group_by("segment")
        .spill(8)
        .aggregate(observations=fpstreams.agg.count())
        .to_list()
    )

    assert result == [{"segment": key, "observations": 8} for key in range(512)]
    assert partials == 0


def test_spilled_count_gate_extends_evidence_for_stable_low_cardinality() -> None:
    """Strong recurrence may earn bounded extra sampling before count admission."""
    keys = [key for _round in range(32) for key in range(256)]
    records = [
        {
            "segment": key,
            **{f"measure_{field}": position + field for field in range(36)},
        }
        for position, key in enumerate(keys)
    ]

    optimized, optimized_bytes = _execute_spilled_count(
        records, key_field="segment", output_name="observations", instrumented=False
    )
    canonical, canonical_bytes = _execute_spilled_count(
        records, key_field="segment", output_name="observations", instrumented=True
    )

    assert optimized == canonical == [{"segment": key, "observations": 32} for key in range(256)]
    assert optimized_bytes * 2 < canonical_bytes


def test_spilled_count_gate_rechecks_a_cold_prefix_for_later_hot_data() -> None:
    """One cold sample must not permanently hide a sustained later hot key."""
    keys = [*range(1, 257), *([0] * 4_096)]
    records = [
        {
            "cohort": key,
            **{f"reading_{field}": position + field for field in range(10)},
        }
        for position, key in enumerate(keys)
    ]

    optimized, optimized_bytes = _execute_spilled_count(
        records, key_field="cohort", output_name="observations", instrumented=False
    )
    canonical, canonical_bytes = _execute_spilled_count(
        records, key_field="cohort", output_name="observations", instrumented=True
    )

    assert optimized == canonical
    assert optimized_bytes * 2 < canonical_bytes


def test_spilled_count_gate_retires_after_a_hot_prefix_turns_cold(monkeypatch) -> None:
    """A stale prefix signal must not keep scanning an arbitrarily long unique tail."""
    import fpstreams
    from fpstreams.tabular import spill

    keys = [*([0] * 230), *range(1, 27), *range(1_000, 5_096)]
    records = [
        {
            "cohort": key,
            **{f"reading_{field}": position + field for field in range(30)},
        }
        for position, key in enumerate(keys)
    ]
    purity_checks = 0
    original_proof = spill._pickle_pure_row

    def tracked_proof(row, key):
        nonlocal purity_checks
        purity_checks += 1
        return original_proof(row, key)

    monkeypatch.setattr(spill, "_pickle_pure_row", tracked_proof)

    result = (
        fpstreams.rows(records)
        .group_by("cohort")
        .spill(8)
        .aggregate(observations=fpstreams.agg.count())
        .to_list()
    )

    assert result[0] == {"cohort": 0, "observations": 230}
    assert len(result) == 4_123
    assert result[-1] == {"cohort": 5_095, "observations": 1}
    assert purity_checks < len(records) // 2


def test_spilled_count_gate_stops_rechecking_a_still_unique_stream(monkeypatch) -> None:
    """A second cold window is enough when recurrence evidence remains absent."""
    import fpstreams
    from fpstreams.tabular import spill

    records = [
        {
            "cohort": position,
            **{f"reading_{field}": position + field for field in range(2)},
        }
        for position in range(2_048)
    ]
    purity_checks = 0
    original_proof = spill._pickle_pure_row

    def tracked_proof(row, key):
        nonlocal purity_checks
        purity_checks += 1
        return original_proof(row, key)

    monkeypatch.setattr(spill, "_pickle_pure_row", tracked_proof)

    result = (
        fpstreams.rows(records)
        .group_by("cohort")
        .spill(8)
        .aggregate(observations=fpstreams.agg.count())
        .to_list()
    )

    assert result == [{"cohort": position, "observations": 1} for position in range(2_048)]
    assert purity_checks <= 512


class _SpillPickleObserved:
    calls = 0

    def __reduce__(self):
        type(self).calls += 1
        return type(self), ()


class _SpillRedirectedRow(dict[str, object]):
    def __getitem__(self, key: str) -> object:
        if key == "id":
            return True
        return super().__getitem__(key)


class _SpillCollisionKey:
    comparisons = 0

    def __init__(self, value: int) -> None:
        self.value = value

    def __hash__(self) -> int:
        return -2

    def __eq__(self, other: object) -> bool:
        type(self).comparisons += 1
        if isinstance(other, _SpillCollisionKey):
            return self.value == other.value
        return self.value == other


class _SpillStatefulCollisionKey:
    comparisons = 0

    def __init__(self, value: int) -> None:
        self.value = value

    def __hash__(self) -> int:
        return -2

    def __eq__(self, other: object) -> bool:
        type(self).comparisons += 1
        value = other.value if isinstance(other, _SpillStatefulCollisionKey) else other
        return type(self).comparisons <= 8 and self.value == value


class _SpillStatefulFieldKey:
    comparisons = 0

    def __hash__(self) -> int:
        return hash("id")

    def __eq__(self, other: object) -> bool:
        type(self).comparisons += 1
        # CPython may compare one colliding dict key more than once after the
        # subscription bytecode specializes.  Keep the result stable so this
        # probe measures executor retries rather than hash-table internals.
        return False


def test_spilled_count_preaggregation_keeps_mixed_protocol_and_key_semantics() -> None:
    """Only pure exact rows compact; cold rows and numeric aliases merge canonically."""
    import math

    import fpstreams

    _SpillPickleObserved.calls = 0
    nan = float("nan")
    records: list[dict[str, object]] = [
        {"id": "first", "payload": 0},
        _SpillRedirectedRow(id="not-the-selected-key", payload=1),
        {"id": 1, "payload": _SpillPickleObserved()},
        *({"id": key, "payload": position} for position, key in enumerate((1.0, 1, True) * 64)),
        *({"id": "first", "payload": position} for position in range(32)),
        {"id": nan, "payload": "left"},
        {"id": nan, "payload": "right"},
    ]

    result = (
        fpstreams.rows(records)
        .group_by("id")
        .spill(8)
        .aggregate(total=fpstreams.agg.count())
        .to_list()
    )

    assert result[0] == {"id": "first", "total": 33}
    assert result[1]["id"] is True
    assert result[1]["total"] == 194
    assert [row["total"] for row in result[2:]] == [1, 1]
    assert all(math.isnan(row["id"]) for row in result[2:])
    assert _SpillPickleObserved.calls == 1


def test_spilled_count_preaggregation_requires_exact_evidence_before_admission() -> None:
    """Sketch collisions cannot let a later custom-equal key replace the first built-in key."""
    import fpstreams

    def execute(*, instrumented: bool) -> tuple[list[dict[str, object]], int]:
        _SpillCollisionKey.comparisons = 0
        records = [
            *({"id": -1, "position": position} for position in range(256)),
            *({"id": -2, "position": 256 + position} for position in range(44)),
            {"id": _SpillCollisionKey(-2), "position": 300},
            *({"id": -2, "position": 301 + position} for position in range(29)),
        ]
        grouped = (
            fpstreams.rows(records).group_by("id").spill(2).aggregate(total=fpstreams.agg.count())
        )
        runtime = QueryRuntime()
        physical = compile_query(grouped._flow._query("list"))
        if instrumented:
            with failpoint("unrelated.transition", RuntimeError("unused")):
                result = list(execute_physical(physical, runtime=runtime))
        else:
            result = list(execute_physical(physical, runtime=runtime))
        comparisons = _SpillCollisionKey.comparisons
        runtime.close()
        return result, comparisons

    optimized, optimized_comparisons = execute(instrumented=False)
    canonical, canonical_comparisons = execute(instrumented=True)

    assert (
        optimized
        == canonical
        == [
            {"id": -1, "total": 256},
            {"id": -2, "total": 74},
        ]
    )
    assert type(optimized[1]["id"]) is int
    assert optimized_comparisons == canonical_comparisons


def test_spilled_count_preaggregation_turns_permanently_cold_at_custom_protocol_rows() -> None:
    """A custom key is written after earlier partials and before all later canonical raw rows."""
    import fpstreams

    def execute(*, instrumented: bool) -> tuple[list[tuple[str, int, int]], int]:
        _SpillStatefulCollisionKey.comparisons = 0
        records = [
            *({"id": -1, "position": position} for position in range(256)),
            {"id": _SpillStatefulCollisionKey(-2), "position": 256},
            *({"id": -2, "position": 257 + position} for position in range(73)),
        ]
        grouped = (
            fpstreams.rows(records).group_by("id").spill(2).aggregate(total=fpstreams.agg.count())
        )
        runtime = QueryRuntime()
        physical = compile_query(grouped._flow._query("list"))
        if instrumented:
            with failpoint("unrelated.transition", RuntimeError("unused")):
                result = list(execute_physical(physical, runtime=runtime))
        else:
            result = list(execute_physical(physical, runtime=runtime))
        comparisons = _SpillStatefulCollisionKey.comparisons
        runtime.close()
        normalized = [
            (
                "custom" if type(row["id"]) is _SpillStatefulCollisionKey else "int",
                row["id"].value if type(row["id"]) is _SpillStatefulCollisionKey else row["id"],
                row["total"],
            )
            for row in result
        ]
        return normalized, comparisons

    optimized, optimized_comparisons = execute(instrumented=False)
    canonical, canonical_comparisons = execute(instrumented=True)

    assert optimized == canonical
    assert optimized_comparisons == canonical_comparisons


def test_spilled_count_preaggregation_does_not_retry_direct_field_selection() -> None:
    """A failed exact-dict field selection has the canonical observable lookup count."""
    from contextlib import nullcontext

    import fpstreams

    def execute(*, instrumented: bool) -> tuple[str, type[BaseException] | None, int]:
        _SpillStatefulFieldKey.comparisons = 0
        grouped = (
            fpstreams.rows([{_SpillStatefulFieldKey(): "hot"}])
            .group_by("id")
            .spill(2)
            .aggregate(total=fpstreams.agg.count())
        )
        context = (
            failpoint("unrelated.transition", RuntimeError("unused"))
            if instrumented
            else nullcontext()
        )
        with context, pytest.raises(fpstreams.SelectionError) as caught:
            grouped.to_list()
        return (
            str(caught.value),
            type(caught.value.__cause__) if caught.value.__cause__ is not None else None,
            _SpillStatefulFieldKey.comparisons,
        )

    assert execute(instrumented=False) == execute(instrumented=True)


def test_spilled_count_preaggregation_keeps_wide_row_graphs_out_of_bounded_hot_state() -> None:
    """Purity proof scratch space is capped instead of retaining an arbitrarily wide row."""
    import fpstreams

    record = {"id": "hot", **{f"field_{position}": position for position in range(512)}}
    records = [dict(record) for _position in range(32)]

    def spill_bytes(*, instrumented: bool) -> int:
        grouped = (
            fpstreams.rows(records).group_by("id").spill(2).aggregate(total=fpstreams.agg.count())
        )
        runtime = QueryRuntime()
        physical = compile_query(grouped._flow._query("list"))
        if instrumented:
            with failpoint("unrelated.transition", RuntimeError("unused")):
                assert list(execute_physical(physical, runtime=runtime)) == [
                    {"id": "hot", "total": len(records)}
                ]
        else:
            assert list(execute_physical(physical, runtime=runtime)) == [
                {"id": "hot", "total": len(records)}
            ]
        result = runtime.metrics.spill_bytes
        runtime.close()
        return result

    assert spill_bytes(instrumented=False) == spill_bytes(instrumented=True)


def test_spilled_count_preaggregation_does_not_retain_unbounded_primitive_keys() -> None:
    """An exact built-in key larger than the cache byte cap remains canonical cold data."""
    import fpstreams

    key = "x" * 8_192
    records = [{"id": key, "position": position} for position in range(32)]

    def execute(*, instrumented: bool) -> tuple[list[dict[str, object]], int]:
        grouped = (
            fpstreams.rows(records).group_by("id").spill(2).aggregate(total=fpstreams.agg.count())
        )
        runtime = QueryRuntime()
        physical = compile_query(grouped._flow._query("list"))
        if instrumented:
            with failpoint("unrelated.transition", RuntimeError("unused")):
                result = list(execute_physical(physical, runtime=runtime))
        else:
            result = list(execute_physical(physical, runtime=runtime))
        physical_bytes = runtime.metrics.spill_bytes
        runtime.close()
        return result, physical_bytes

    optimized, optimized_bytes = execute(instrumented=False)
    canonical, canonical_bytes = execute(instrumented=True)

    assert optimized == canonical == [{"id": key, "total": len(records)}]
    assert optimized_bytes == canonical_bytes


def test_spilled_count_preaggregation_uses_exact_virtual_partition_limits(tmp_path) -> None:
    """Hot partials retain canonical raw row and framed-byte limits through repartition."""
    import fpstreams
    from fpstreams.storage.codec import SpillCodec

    records = [{"id": "hot", "payload": "x" * 64} for _position in range(512)]
    codec = SpillCodec()
    canonical_bytes = len(b"FPSTRM\x00\x01") + sum(
        len(codec.encode_record((position, "hot", row))) for position, row in enumerate(records)
    )

    exact = fpstreams.SpillLimits(
        max_partition_rows=len(records),
        max_partition_bytes=canonical_bytes,
        max_matches_per_key=1,
        max_output_rows=1,
        max_repartition_depth=0,
    )
    assert fpstreams.rows(records).group_by("id").spill(
        2, tempdir=tmp_path, limits=exact
    ).aggregate(total=fpstreams.agg.count()).to_list() == [{"id": "hot", "total": len(records)}]

    too_few_rows = fpstreams.SpillLimits(
        max_partition_rows=len(records) - 1,
        max_partition_bytes=canonical_bytes,
        max_matches_per_key=1,
        max_output_rows=1,
        max_repartition_depth=1,
    )
    with pytest.raises(fpstreams.BufferLimitError, match=r"max_partition_rows.*1 repartition"):
        (
            fpstreams.rows(records)
            .group_by("id")
            .spill(2, tempdir=tmp_path, limits=too_few_rows)
            .aggregate(total=fpstreams.agg.count())
            .to_list()
        )

    too_few_bytes = fpstreams.SpillLimits(
        max_partition_rows=len(records),
        max_partition_bytes=canonical_bytes - 1,
        max_matches_per_key=1,
        max_output_rows=1,
        max_repartition_depth=0,
    )
    with pytest.raises(fpstreams.BufferLimitError, match="max_partition_bytes"):
        (
            fpstreams.rows(records)
            .group_by("id")
            .spill(2, tempdir=tmp_path, limits=too_few_bytes)
            .aggregate(total=fpstreams.agg.count())
            .to_list()
        )

    assert list(tmp_path.iterdir()) == []


def test_spilled_count_preaggregation_keeps_repartition_bytes_canonical(tmp_path) -> None:
    """Nested built-ins stay raw when pickle roundtrips change their framed byte size."""
    import pickle
    from contextlib import nullcontext

    import fpstreams
    from fpstreams.storage.codec import SpillCodec

    size = 600
    records = [{"key": 0, "payload": {str(0): {str(0): position % 3}}} for position in range(size)]
    codec = SpillCodec()
    initial_bytes = stable_bytes = len(b"FPSTRM\x00\x01")
    for position, row in enumerate(records):
        frame = codec.encode_record((position, 0, row))
        initial_bytes += len(frame)
        stable_bytes += len(codec.encode_record(pickle.loads(frame[4:])))
    assert (initial_bytes, stable_bytes) == (36_352, 35_152)
    limits = fpstreams.SpillLimits(
        max_partition_rows=size,
        max_partition_bytes=stable_bytes,
        max_matches_per_key=1,
        max_output_rows=1,
        max_repartition_depth=1,
    )

    def execute(*, instrumented: bool) -> list[dict[str, int]]:
        grouped = (
            fpstreams.rows(records)
            .group_by("key")
            .spill(2, tempdir=tmp_path, limits=limits)
            .aggregate(n=fpstreams.agg.count())
        )
        context = (
            failpoint("unrelated.transition", RuntimeError("unused"))
            if instrumented
            else nullcontext()
        )
        with context:
            return grouped.to_list()

    assert execute(instrumented=False) == execute(instrumented=True) == [{"key": 0, "n": size}]
    assert list(tmp_path.iterdir()) == []


def test_spilled_count_preaggregation_colds_distinct_equal_flat_strings_and_bytes() -> None:
    """Identity-sensitive flat values stay raw before pickle can canonicalize aliases."""
    import pickle

    import fpstreams
    from fpstreams.storage.codec import SpillCodec

    builders = (
        (lambda: {"key": str(0), "payload": str(0)}, True),
        (lambda: {"key": 0, "a": str(0), "b": str(0)}, True),
        (lambda: {"key": 0, "a": bytes([0]) * 0, "b": bytes([1]) * 0}, True),
        (
            lambda: {
                "key": 0,
                "a": bytes(bytearray(b"x" * 30)),
                "b": bytes(bytearray(b"x" * 30)),
            },
            False,
        ),
    )
    codec = SpillCodec()

    def execute(
        records: list[dict[str, object]], *, instrumented: bool
    ) -> tuple[list[dict[str, object]], int]:
        grouped = (
            fpstreams.rows(records).group_by("key").spill(2).aggregate(n=fpstreams.agg.count())
        )
        runtime = QueryRuntime()
        physical = compile_query(grouped._flow._query("list"))
        if instrumented:
            with failpoint("unrelated.transition", RuntimeError("unused")):
                result = list(execute_physical(physical, runtime=runtime))
        else:
            result = list(execute_physical(physical, runtime=runtime))
        spill_bytes = runtime.metrics.spill_bytes
        runtime.close()
        return result, spill_bytes

    for build, size_drifts in builders:
        first = build()
        record = (0, first["key"], first)
        sizes: list[int] = []
        for _generation in range(4):
            frame = codec.encode_record(record)
            sizes.append(len(frame))
            record = pickle.loads(frame[4:])
        assert (len(set(sizes)) > 1) is size_drifts
        records = [build() for _position in range(600)]

        assert execute(records, instrumented=False) == execute(records, instrumented=True)


def test_active_failpoint_keeps_spilled_count_on_canonical_source_and_cleanup_path(
    tmp_path, monkeypatch
) -> None:
    """Instrumentation disables partial creation before the one-shot source is opened."""
    import fpstreams
    from fpstreams.tabular import spill

    opened = False
    specialized = False

    def records():
        nonlocal opened
        opened = True
        yield from ({"id": "hot", "value": value} for value in range(32))

    original = spill._partition_count_rows

    def tracked(*args, **kwargs):
        nonlocal specialized
        specialized = True
        return original(*args, **kwargs)

    monkeypatch.setattr(spill, "_partition_count_rows", tracked)
    grouped = (
        fpstreams.rows(records())
        .group_by("id")
        .spill(2, tempdir=tmp_path)
        .aggregate(total=fpstreams.agg.count())
    )

    with (
        failpoint("source.open.after", RuntimeError("instrumented open")),
        pytest.raises(RuntimeError, match="instrumented open"),
    ):
        grouped.to_list()

    assert not opened
    assert not specialized
    assert list(tmp_path.iterdir()) == []


def test_spill_store_round_trips_arbitrary_records(tmp_path) -> None:
    """One versioned run preserves heterogeneous picklable Python records."""
    runtime = QueryRuntime(QueryLimits(max_open_files=4))
    store = SpillStore(runtime, parent=tmp_path, operation="test")
    writer = store.create_writer(generation=0, partition=0)
    writer.write((1, "a"))
    writer.write({"values": (2, 3)})
    run = writer.close()

    assert list(store.read(run)) == [(1, "a"), {"values": (2, 3)}]
    runtime.close()
    assert not store.directory.exists()


def test_spill_store_batches_large_runs_without_changing_logical_metrics(
    tmp_path, monkeypatch
) -> None:
    """Large runs amortize pickle framing while retaining rows, bytes, and values."""
    from fpstreams.storage import codec as codec_module

    calls = 0
    pickle_dumps = codec_module.pickle.dumps

    def counted_dumps(value, *, protocol):
        nonlocal calls
        calls += 1
        return pickle_dumps(value, protocol=protocol)

    monkeypatch.setattr(codec_module.pickle, "dumps", counted_dumps)
    runtime = QueryRuntime()
    store = SpillStore(runtime, parent=tmp_path, operation="batched")
    values = list(range(4_097))

    run = store.write_run(0, 0, values)

    assert list(store.read(run)) == values
    assert run.rows == len(values)
    assert run.bytes == runtime.metrics.spill_bytes + len(b"FPSTRM\x00\x01")
    assert 1 < calls < 100
    runtime.close()


def test_spill_writer_open_failpoint_releases_its_lease_immediately(tmp_path) -> None:
    """A constructor failure after open cannot poison the remaining query budget."""
    runtime = QueryRuntime(QueryLimits(max_open_files=1))
    store = SpillStore(runtime, parent=tmp_path, operation="open-failure")

    with (
        failpoint("spill.open.after", OSError("open transition failed")),
        pytest.raises(OSError, match="open transition failed"),
    ):
        store.create_writer(generation=0, partition=0)

    assert runtime.metrics.open_files == 0
    replacement = store.create_writer(generation=0, partition=1)
    replacement.write("still usable")
    replacement.close()
    assert runtime.metrics.open_files == 0
    runtime.close()


def test_spill_run_keeps_source_error_primary_when_iterator_close_fails(tmp_path) -> None:
    """Writer and iterator cleanup failures become notes on the pipeline error."""
    primary = ValueError("primary source failure")

    class FailingValues:
        def __iter__(self):
            return self

        def __next__(self):
            raise primary

        def close(self) -> None:
            raise OSError("secondary iterator close")

    runtime = QueryRuntime(QueryLimits(max_open_files=1))
    store = SpillStore(runtime, parent=tmp_path, operation="source-failure")

    with pytest.raises(ValueError, match="primary source failure") as caught:
        store.write_run(0, 0, FailingValues())

    assert caught.value is primary
    assert caught.value.__notes__ == ["cleanup failed: OSError: secondary iterator close"]
    assert runtime.metrics.open_files == 0
    runtime.close()


def test_spill_sort_records_avoid_per_row_dataclass_reflection(tmp_path, monkeypatch) -> None:
    """Record framing must not rediscover dataclass fields for every spilled row."""
    import dataclasses

    calls = 0
    fields = dataclasses.fields

    def counted_fields(value):
        nonlocal calls
        calls += 1
        return fields(value)

    monkeypatch.setattr(dataclasses, "fields", counted_fields)
    runtime = QueryRuntime()
    store = SpillStore(runtime, parent=tmp_path, operation="sort-records")
    records = [SortRecord(value, value, value) for value in range(128)]

    run = store.write_run(0, 0, records)

    assert list(store.read(run)) == records
    assert calls <= 2
    runtime.close()


def test_value_only_merge_evaluates_each_current_record_key_once(tmp_path) -> None:
    """Heap comparisons reuse one key per cursor instead of re-running user code."""
    runtime = QueryRuntime()
    store = SpillStore(runtime, parent=tmp_path, operation="position-sort")
    runs = [
        store.write_run(
            0,
            partition,
            [PositionRecord(position, value) for position, value in records],
        )
        for partition, records in enumerate(
            (
                ((0, 1), (3, 4), (6, 7)),
                ((1, 2), (4, 5), (7, 8)),
                ((2, 3), (5, 6), (8, 9)),
            )
        )
    ]
    calls = 0

    def key(value: int) -> int:
        nonlocal calls
        calls += 1
        return value

    merged = list(_merge_position_runs(store, runs, key=key, reverse=False))

    assert [record.value for record in merged] == list(range(1, 10))
    assert calls == len(merged)
    runtime.close()


def test_ephemeral_spill_skips_fsync_without_weakening_durable_default(
    tmp_path, monkeypatch
) -> None:
    """Query-temporary runs may skip crash durability; ordinary stores still sync."""
    calls: list[int] = []
    monkeypatch.setattr("fpstreams.storage.spill_store.os.fsync", calls.append)

    durable_runtime = QueryRuntime()
    durable = SpillStore(durable_runtime, parent=tmp_path, operation="durable")
    durable.write_run(0, 0, ["kept"])
    durable_runtime.close()

    ephemeral_runtime = QueryRuntime()
    ephemeral = SpillStore(
        ephemeral_runtime,
        parent=tmp_path,
        operation="ephemeral",
        durable=False,
    )
    run = ephemeral.write_run(0, 0, ["temporary"])

    assert len(calls) == 1
    assert list(ephemeral.read(run)) == ["temporary"]
    ephemeral_runtime.close()


def test_truncated_run_fails_and_cleans_up(tmp_path) -> None:
    """Malformed frame input raises a normalized format error before runtime cleanup."""
    runtime = QueryRuntime()
    store = SpillStore(runtime, parent=tmp_path, operation="test")
    run = store.write_run(0, 0, [1, 2, 3])
    run.path.write_bytes(run.path.read_bytes()[:-2])

    with pytest.raises(SpillFormatError, match="truncated"):
        list(store.read(run))

    runtime.close()
    assert not store.directory.exists()


def test_generation_replacement_publishes_new_runs_before_removing_old(tmp_path) -> None:
    runtime = QueryRuntime()
    store = SpillStore(runtime, parent=tmp_path, operation="test")
    old_run = store.write_run(0, 0, ["old"])
    old = SpillGeneration(0, (old_run,))
    store.commit_generation(old)
    new_run = store.write_run(1, 0, ["new"])
    new = SpillGeneration(1, (new_run,))

    store.replace_generation(old, new)

    assert not old_run.path.exists()
    assert new_run.path.exists()
    assert list(store.read(new_run)) == ["new"]
    runtime.close()


def test_generation_replacement_keeps_durable_metadata_if_unlink_fails(tmp_path) -> None:
    runtime = QueryRuntime()
    store = SpillStore(runtime, parent=tmp_path, operation="test")
    old_run = store.write_run(0, 0, ["old"])
    old = SpillGeneration(0, (old_run,))
    store.commit_generation(old)
    new_run = store.write_run(1, 0, ["new"])
    new = SpillGeneration(1, (new_run,))

    with (
        failpoint("spill.unlink.before", OSError("injected unlink failure")),
        pytest.raises(OSError, match="injected unlink failure"),
    ):
        store.replace_generation(old, new)

    assert store._generations == {0: old, 1: new}
    assert old_run.path.exists()
    assert new_run.path.exists()
    runtime.close()


def test_spill_writer_can_append_a_closed_partition_run(tmp_path) -> None:
    runtime = QueryRuntime()
    store = SpillStore(runtime, parent=tmp_path, operation="test")
    writer = store.create_writer(generation=0, partition=0)
    writer.write("first")
    first = writer.close()
    resumed = store.create_writer(generation=0, partition=0, path=first.path, append=True)
    resumed.write("second")
    resumed.close()

    assert list(store.read(first)) == ["first", "second"]
    runtime.close()


def test_partition_writers_can_use_framed_store_runs(tmp_path) -> None:
    runtime = QueryRuntime()
    store = SpillStore(runtime, parent=tmp_path, operation="partition")
    writers = SpillPartitionWriters(store.directory, "input", 2, operation="group_by", store=store)
    writers.dump(0, (0, "left"))
    writers.dump(1, (1, "right"))
    writers.close()

    assert list(read(writers.files()[0].path, store=store)) == [(0, "left")]
    assert list(read(writers.files()[1].path, store=store)) == [(1, "right")]
    runtime.close()


def test_buffered_partition_write_failpoint_releases_file_lease(tmp_path) -> None:
    """The encoded-byte flush retains the common spill write failure boundary."""
    runtime = QueryRuntime(QueryLimits(max_open_files=1))
    store = SpillStore(runtime, parent=tmp_path, operation="partition-failure", durable=False)
    writers = SpillPartitionWriters(
        store.directory,
        "input",
        64,
        operation="group_by",
        store=store,
    )
    writers.dump(0, (0, "value"))

    with (
        failpoint("spill.write.before", OSError("disk full")),
        pytest.raises(OSError, match="disk full"),
    ):
        writers.close()

    assert runtime.metrics.open_files == 0
    runtime.close()


def test_partition_read_uses_the_shared_failpoint_and_releases_its_lease(tmp_path) -> None:
    """Tabular readers use the same tracked read boundary as ordinary spill runs."""
    runtime = QueryRuntime(QueryLimits(max_open_files=1))
    store = SpillStore(runtime, parent=tmp_path, operation="partition-read", durable=False)
    run = store.write_run(0, 0, ["value"])
    values = read(run.path, store=store)

    with (
        failpoint("spill.read.before", OSError("read failed")),
        pytest.raises(OSError, match="read failed"),
    ):
        next(values)

    assert runtime.metrics.open_files == 0
    runtime.close()


def test_store_partition_writer_appends_after_a_bounded_buffer_flush(tmp_path, monkeypatch) -> None:
    from fpstreams.tabular import spill_io

    monkeypatch.setattr(spill_io, "_PARTITION_BUFFER_BYTES", 1)
    runtime = QueryRuntime()
    store = SpillStore(runtime, parent=tmp_path, operation="partition")
    writers = SpillPartitionWriters(store.directory, "input", 33, operation="group_by", store=store)
    writers.dump(0, "before")
    for partition in range(1, 33):
        writers.dump(partition, partition)
    writers.dump(0, "after")
    writers.close()

    assert list(read(writers.files()[0].path, store=store)) == ["before", "after"]
    runtime.close()


def test_wide_partition_writer_and_repartition_share_the_query_file_budget(tmp_path) -> None:
    """A source reader plus wide repartition never relies on a stale FD snapshot."""
    runtime = QueryRuntime(QueryLimits(max_open_files=2))
    store = SpillStore(runtime, parent=tmp_path, operation="repartition", durable=False)
    values = [(position, position, {"id": position}) for position in range(80)]
    run = store.write_run(0, 0, values)
    source = PartitionFile(run.path, run.rows, run.bytes)

    outputs = repartition(
        source,
        store.directory,
        "child",
        64,
        operation="group_by",
        salt=7,
        store=store,
    )
    actual = [value for output in outputs for value in read(output.path, store=store)]

    assert sorted(actual) == values
    assert runtime.metrics.high_water_open_files <= 2
    assert runtime.metrics.open_files == 0
    runtime.close()


def test_ordered_spill_merge_collapses_256_paths_within_file_budget(tmp_path) -> None:
    """Result ordering uses bounded merge generations rather than 256 simultaneous readers."""
    runtime = QueryRuntime(QueryLimits(max_open_files=3))
    store = SpillStore(runtime, parent=tmp_path, operation="ordered-merge", durable=False)
    paths = []
    for position in reversed(range(256)):
        path = store.directory / f"result-{position}.bin"
        writer = SpillLazyWriter(path, operation="ordered-merge", store=store)
        writer.dump((position, position))
        writer.close()
        paths.append(path)

    with pytest.raises(RuntimeError, match="closed"):
        writer.dump((999, 999))

    values = merge_ordered(paths, store=store)
    assert next(values) == 0
    assert [0, *values] == list(range(256))
    assert runtime.metrics.high_water_open_files <= 3
    assert runtime.metrics.open_files == 0

    early_paths = []
    for position in range(8):
        path = store.directory / f"early-{position}.bin"
        early_writer = SpillLazyWriter(path, operation="ordered-merge", store=store)
        early_writer.dump((position, position))
        early_writer.close()
        early_paths.append(path)
    early = merge_ordered(early_paths, store=store)
    assert next(early) == 0
    early.close()
    assert runtime.metrics.open_files == 0
    runtime.close()


def test_ordered_spill_merge_early_close_surfaces_reader_cleanup_failures(
    tmp_path, monkeypatch
) -> None:
    """Early consumer close reports reader failures after attempting every close."""
    from fpstreams.runtime import TrackedBinaryFile

    runtime = QueryRuntime(QueryLimits(max_open_files=4))
    store = SpillStore(runtime, parent=tmp_path, operation="ordered-close", durable=False)
    paths = [store.write_run(0, position, [(position, position)]).path for position in range(2)]
    close_calls = 0
    original_close = TrackedBinaryFile._close_owned

    def fail_after_reader_close(handle: TrackedBinaryFile) -> None:
        nonlocal close_calls
        is_reader = handle.readable()
        original_close(handle)
        if is_reader:
            close_calls += 1
            raise OSError(f"reader close failure {close_calls}")

    monkeypatch.setattr(TrackedBinaryFile, "_close_owned", fail_after_reader_close)
    values = merge_ordered(paths, store=store)
    assert next(values) == 0

    with pytest.raises(OSError, match="reader close failure 1") as captured:
        values.close()

    assert captured.value.__notes__ == ["cleanup failed: OSError: reader close failure 2"]
    assert close_calls == len(paths)
    assert runtime.metrics.open_files == 0
    runtime.close()


def test_nested_join_and_group_spills_compose_under_one_file_budget() -> None:
    """A downstream spill may consume an upstream spill without descriptor deadlock."""
    import fpstreams

    left = [{"id": position % 8, "left": position} for position in range(64)]
    right = [{"id": position, "right": position * 10} for position in range(8)]
    rows = (
        fpstreams.rows(left)
        .join(right, on="id", partitions=4)
        .group_by("id")
        .spill(4)
        .aggregate(count=fpstreams.agg.count())
    )
    physical = compile_query(rows._flow._query("list"))
    runtime = QueryRuntime(QueryLimits(max_open_files=4))

    actual = list(execute_physical(physical, runtime=runtime))

    assert sorted(actual, key=lambda row: row["id"]) == [
        {"id": position, "count": 8} for position in range(8)
    ]
    assert runtime.metrics.high_water_open_files <= 4
    assert runtime.metrics.open_files == 0
    assert runtime.metrics.spill_bytes > 0


def test_spilled_relational_file_minimum_is_checked_before_one_shot_input(tmp_path) -> None:
    """An unusable file budget fails before claiming or advancing a source generator."""
    import fpstreams

    opened = False

    def records():
        nonlocal opened
        opened = True
        yield {"id": 1}

    rows = fpstreams.rows(records()).group_by("id").spill(2).aggregate(count=fpstreams.agg.count())
    physical = compile_query(rows._flow._query("list"))
    runtime = QueryRuntime(QueryLimits(max_open_files=3))
    runtime.files.open(tmp_path / "held.bin", "wb")

    with pytest.raises(RuntimeError, match=r"at least 3.*available=2"):
        list(execute_physical(physical, runtime=runtime))

    assert not opened
    assert runtime.metrics.open_files == 0
    assert rows.to_list() == [{"id": 1, "count": 1}]
    assert opened


def test_sort_merge_fan_in_obeys_the_query_file_limit_during_compaction(tmp_path) -> None:
    runtime = QueryRuntime(QueryLimits(max_open_files=4))
    store = SpillStore(runtime, parent=tmp_path, operation="sort")
    runs = [
        store.write_run(0, position, [SortRecord(position, position, position)])
        for position in range(5)
    ]
    operation = SortOp(None, False, 1)
    fan_in = _sort_merge_fan_in(runtime)

    collapsed, _generation = _collapse_sort_runs(store, runs, operation, 0, fan_in)

    assert fan_in == 2
    assert len(collapsed) <= fan_in
    assert [record.value for record in _merge_sort_runs(store, collapsed, operation)] == list(
        range(5)
    )
    runtime.close()


def test_physical_external_sort_uses_injected_runtime_budget_and_metrics() -> None:
    """The physical sort must not replace the caller's limits with a nested runtime."""
    physical = compile_query(
        flow(range(20, 0, -1)).external_sort(buffer_size=1).with_engine("python")._query("list")
    )
    rejected = QueryRuntime(QueryLimits(max_open_files=1))
    with pytest.raises(RuntimeError, match="at least 3"):
        list(execute_physical(physical, runtime=rejected))

    runtime = QueryRuntime(QueryLimits(max_open_files=4))
    assert list(execute_physical(physical, runtime=runtime)) == list(range(1, 21))
    assert 0 < runtime.metrics.high_water_open_files <= 4
    assert runtime.metrics.open_files == 0
    assert runtime.metrics.spill_bytes > 0
