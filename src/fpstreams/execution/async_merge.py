"""Shared-query-runtime physical executors for async multi-source nodes."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterable
from typing import Any

from ..physical.async_plan import (
    AsyncCombineLatestNode,
    AsyncMergeMapNode,
    AsyncMergeNode,
    AsyncSwitchMapNode,
)
from ..planning.async_ import _to_async_iterator
from ..planning.async_utils import _MISSING, _resolve, close_async_iterators
from ..runtime.query import QueryRuntime
from ..runtime.resources import run_async_cleanup
from ..runtime.tasks import TaskRole, TaskScope


async def _pull(iterator: AsyncIterator[Any]) -> Any:
    """Represent one owned upstream request as a schedulable coroutine."""
    return await anext(iterator)


async def execute_merge(
    source: AsyncIterator[Any], node: AsyncMergeNode, runtime: QueryRuntime
) -> AsyncIterator[Any]:
    """Interleave sources with exactly one query-owned pull task per live input."""
    scope = runtime.tasks.scope(f"merge:{node.logical_ids[0]}")
    active: dict[int, AsyncIterator[Any]] = {0: source}
    pending: dict[asyncio.Task[Any], int] = {}
    active_error: BaseException | None = None
    try:
        for position, additional in enumerate(node.operation.sources, start=1):
            active[position] = additional.open()
        for position, iterator in active.items():
            pending[scope.create_task(_pull(iterator), role=TaskRole.SOURCE)] = position
        while pending:
            done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            ordered_done = sorted(done, key=pending.__getitem__)
            for task in ordered_done:
                failure = task.exception() if not task.cancelled() else asyncio.CancelledError()
                if failure is not None and not isinstance(failure, StopAsyncIteration):
                    pending.pop(task)
                    await scope.take_result(task)
                    raise AssertionError("unreachable")
            for task in ordered_done:
                position = pending.pop(task)
                iterator = active[position]
                try:
                    item = await scope.take_result(task)
                except StopAsyncIteration:
                    active.pop(position)
                    await close_async_iterators((iterator,))
                    continue
                yield item
                pending[scope.create_task(_pull(iterator), role=TaskRole.SOURCE)] = position
    except BaseException as error:
        active_error = error
        raise
    finally:
        await run_async_cleanup(
            (scope.aclose, lambda: close_async_iterators(tuple(active.values()))),
            active_error,
        )


async def execute_combine_latest(
    source: AsyncIterator[Any], node: AsyncCombineLatestNode, runtime: QueryRuntime
) -> AsyncIterator[Any]:
    """Combine live sources while keeping their pulls under the query runtime.

    A completed source keeps its last value.  A source that completes before its
    first value makes a complete latest-value tuple impossible, so the remaining
    pulls are cancelled and the operator finishes immediately.
    """
    scope = runtime.tasks.scope(f"combine_latest:{node.logical_ids[0]}")
    active: dict[int, AsyncIterator[Any]] = {0: source}
    pending: dict[asyncio.Task[Any], int] = {}
    latest: list[Any] = [_MISSING] * (len(node.operation.sources) + 1)
    ready = 0
    active_error: BaseException | None = None
    try:
        for position, additional in enumerate(node.operation.sources, start=1):
            active[position] = additional.open()
        for position, iterator in active.items():
            pending[scope.create_task(_pull(iterator), role=TaskRole.SOURCE)] = position
        while pending:
            done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            ordered_done = sorted(done, key=pending.__getitem__)
            # An empty source makes future tuples impossible, but every pull that
            # completed in the same scheduler turn must be observed first. Otherwise
            # set iteration order can silently discard a concurrent source failure.
            for task in ordered_done:
                failure = task.exception() if not task.cancelled() else asyncio.CancelledError()
                if failure is not None and not isinstance(failure, StopAsyncIteration):
                    pending.pop(task)
                    await scope.take_result(task)
                    raise AssertionError("unreachable")

            impossible = False
            reschedule: list[tuple[int, AsyncIterator[Any]]] = []
            for task in ordered_done:
                position = pending.pop(task)
                iterator = active[position]
                try:
                    item = await scope.take_result(task)
                except StopAsyncIteration:
                    active.pop(position)
                    await close_async_iterators((iterator,))
                    if latest[position] is _MISSING:
                        impossible = True
                    continue

                if latest[position] is _MISSING:
                    ready += 1
                latest[position] = item
                if ready == len(latest):
                    yield tuple(latest)
                reschedule.append((position, iterator))
            if impossible:
                return
            for position, iterator in reschedule:
                pending[scope.create_task(_pull(iterator), role=TaskRole.SOURCE)] = position
    except BaseException as error:
        active_error = error
        raise
    finally:
        await run_async_cleanup(
            (scope.aclose, lambda: close_async_iterators(tuple(active.values()))),
            active_error,
        )


async def _open_inner(operation: Any, item: Any, runtime: QueryRuntime) -> AsyncIterator[Any]:
    """Call a mapper once, normalize its result, and register its inner immediately."""
    mapped = await _resolve(operation.function(item))
    inner = _to_async_iterator(mapped)
    return await runtime.resources.aown(inner)


async def _release_inners(
    runtime: QueryRuntime,
    inners: Iterable[AsyncIterator[Any]],
    *,
    active_error: BaseException | None = None,
) -> None:
    """Retire registered inners without letting one close failure skip later owners."""
    first_cleanup_error: BaseException | None = None
    for inner in inners:
        cleanup_primary = active_error if active_error is not None else first_cleanup_error
        try:
            await runtime.resources.arelease(
                inner,
                cleanup_primary,
                cleanup_boundary=False,
            )
        except BaseException as error:
            first_cleanup_error = error
    if first_cleanup_error is not None:
        raise first_cleanup_error


async def _close_inner_operator_ownership(
    runtime: QueryRuntime,
    sources: Iterable[AsyncIterator[Any]],
    inners: Iterable[AsyncIterator[Any]],
    *,
    active_error: BaseException | None,
) -> None:
    """Close ordinary sources and registered inners under one error boundary."""
    first_cleanup_error: BaseException | None = None
    try:
        await close_async_iterators(sources, active_error=active_error)
    except BaseException as error:
        first_cleanup_error = error
    cleanup_primary = active_error if active_error is not None else first_cleanup_error
    try:
        await _release_inners(
            runtime,
            inners,
            active_error=cleanup_primary,
        )
    except BaseException as error:
        first_cleanup_error = error
    if first_cleanup_error is not None:
        raise first_cleanup_error


async def execute_merge_map(
    source: AsyncIterator[Any], node: AsyncMergeMapNode, runtime: QueryRuntime
) -> AsyncIterator[Any]:
    """Merge mapped inners with the public concurrency limit as one shared budget."""
    scope = runtime.tasks.scope(f"merge_map:{node.logical_ids[0]}")
    outer_pull: asyncio.Task[Any] | None = None
    outer_done = False
    mappings: dict[asyncio.Task[AsyncIterator[Any]], int] = {}
    inners: dict[int, AsyncIterator[Any]] = {}
    inner_pulls: dict[asyncio.Task[Any], int] = {}
    next_mapping_id = 0
    next_inner_id = 0
    active_error: BaseException | None = None
    try:
        while True:
            occupied = len(mappings) + len(inners)
            if outer_pull is None and not outer_done and occupied < node.operation.concurrency:
                outer_pull = scope.create_task(_pull(source), role=TaskRole.SOURCE)

            waiting: set[asyncio.Task[Any]] = set(mappings)
            waiting.update(inner_pulls)
            if outer_pull is not None:
                waiting.add(outer_pull)
            if not waiting:
                return

            done, _ = await asyncio.wait(waiting, return_when=asyncio.FIRST_COMPLETED)
            ordered_done = sorted(
                done,
                key=lambda task: (
                    (0, 0)
                    if task is outer_pull
                    else (1, mappings[task])
                    if task in mappings
                    else (2, inner_pulls[task])
                ),
            )
            for task in ordered_done:
                failure = task.exception() if not task.cancelled() else asyncio.CancelledError()
                stop_is_end = task is outer_pull or task in inner_pulls
                if failure is not None and (
                    not stop_is_end or not isinstance(failure, StopAsyncIteration)
                ):
                    await scope.take_result(task)
                    raise AssertionError("unreachable")

            for task in ordered_done:
                if task is outer_pull:
                    outer_pull = None
                    try:
                        outer_item = await scope.take_result(task)
                    except StopAsyncIteration:
                        outer_done = True
                    else:
                        mapping = scope.create_task(
                            _open_inner(node.operation, outer_item, runtime),
                            role=TaskRole.USER_CALL,
                        )
                        mappings[mapping] = next_mapping_id
                        next_mapping_id += 1
                    continue

                if task in mappings:
                    mapping = task
                    mappings.pop(mapping)
                    nested = await scope.take_result(mapping)
                    position = next_inner_id
                    next_inner_id += 1
                    inners[position] = nested
                    inner_pulls[scope.create_task(_pull(nested), role=TaskRole.SOURCE)] = position
                    continue

                position = inner_pulls.pop(task)
                nested = inners[position]
                try:
                    item = await scope.take_result(task)
                except StopAsyncIteration:
                    inners.pop(position)
                    await _release_inners(runtime, (nested,))
                    continue
                yield item
                inner_pulls[scope.create_task(_pull(nested), role=TaskRole.SOURCE)] = position
    except BaseException as error:
        active_error = error
        raise
    finally:
        await run_async_cleanup(
            (
                scope.aclose,
                lambda: _close_inner_operator_ownership(
                    runtime,
                    (source,),
                    tuple(inners.values()),
                    active_error=None,
                ),
            ),
            active_error,
        )


async def _discard_mapping(
    scope: TaskScope, task: asyncio.Task[AsyncIterator[Any]]
) -> AsyncIterator[Any] | None:
    """Cancel or drain a superseded mapper and recover a successfully opened inner."""
    if not task.done():
        await scope.cancel(task)
        return None
    try:
        return await scope.take_result(task)
    except (asyncio.CancelledError, Exception):
        return None


async def execute_switch_map(  # noqa: C901
    source: AsyncIterator[Any], node: AsyncSwitchMapNode, runtime: QueryRuntime
) -> AsyncIterator[Any]:
    """Keep the latest-only cancellation state machine under one query-owned task scope."""
    scope = runtime.tasks.scope(f"switch_map:{node.logical_ids[0]}")
    outer_pull: asyncio.Task[Any] | None = scope.create_task(_pull(source), role=TaskRole.SOURCE)
    mapping: asyncio.Task[AsyncIterator[Any]] | None = None
    inner: AsyncIterator[Any] | None = None
    inner_pull: asyncio.Task[Any] | None = None
    active_error: BaseException | None = None
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
                    outer_item = await scope.take_result(completed_outer)
                except StopAsyncIteration:
                    pass
                else:
                    outer_pull = scope.create_task(_pull(source), role=TaskRole.SOURCE)
                    stale: list[AsyncIterator[Any]] = []
                    if mapping is not None:
                        stale_mapping = mapping
                        mapping = None
                        mapped_inner = await _discard_mapping(scope, stale_mapping)
                        if mapped_inner is not None:
                            stale.append(mapped_inner)
                    if inner_pull is not None:
                        await scope.cancel(inner_pull)
                        inner_pull = None
                    if inner is not None:
                        stale.append(inner)
                        inner = None
                    await _release_inners(runtime, stale)
                    mapping = scope.create_task(
                        _open_inner(node.operation, outer_item, runtime),
                        role=TaskRole.USER_CALL,
                    )
                    continue

            if mapping is not None and mapping in done:
                completed_mapping = mapping
                mapping = None
                inner = await scope.take_result(completed_mapping)
                inner_pull = scope.create_task(_pull(inner), role=TaskRole.SOURCE)

            if inner_pull is not None and inner_pull in done:
                completed_inner_pull = inner_pull
                inner_pull = None
                try:
                    item = await scope.take_result(completed_inner_pull)
                except StopAsyncIteration:
                    if inner is not None:
                        await _release_inners(runtime, (inner,))
                        inner = None
                else:
                    yield item
                    if inner is not None:
                        inner_pull = scope.create_task(_pull(inner), role=TaskRole.SOURCE)
    except BaseException as error:
        active_error = error
        raise
    finally:
        current_inners = () if inner is None else (inner,)
        await run_async_cleanup(
            (
                scope.aclose,
                lambda: _close_inner_operator_ownership(
                    runtime,
                    (source,),
                    current_inners,
                    active_error=None,
                ),
            ),
            active_error,
        )
