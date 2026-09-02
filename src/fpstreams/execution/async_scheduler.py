"""Compose source-safe async physical nodes under one query runtime."""

from __future__ import annotations

import inspect
from collections.abc import AsyncIterator
from typing import Any

from ..physical.async_plan import (
    AsyncCombineLatestNode,
    AsyncMapNode,
    AsyncMergeMapNode,
    AsyncMergeNode,
    AsyncPhysicalPlan,
    AsyncPrefetchNode,
    AsyncSerialOperationNode,
    AsyncSerialStage,
    AsyncSwitchMapNode,
    AsyncTimerNode,
)
from ..planning.async_ import AsyncOperation, _Filter, _MapAsync, _Tap
from ..planning.async_utils import close_async_iterators, closing_async_iterators
from ..runtime.query import QueryRuntime
from ..runtime.resources import _add_cleanup_failure
from .async_map import execute_async_map
from .async_merge import (
    execute_combine_latest,
    execute_merge,
    execute_merge_map,
    execute_switch_map,
)
from .async_ops import apply_async_operation
from .async_prefetch import execute_async_prefetch
from .async_timers import execute_async_timer


async def _execute_serial_stage(
    source: AsyncIterator[Any], operations: tuple[AsyncOperation, ...]
) -> AsyncIterator[Any]:
    """Fuse adjacent serial map, filter, and tap operations in physical order."""
    async with closing_async_iterators((source,)):
        async for item in source:
            current = item
            emit = True
            for operation in operations:
                if isinstance(operation, _MapAsync):
                    mapped = operation.function(current)
                    if inspect.isawaitable(mapped):
                        mapped = await mapped
                    current = mapped
                    del mapped
                elif isinstance(operation, _Filter):
                    accepted = operation.predicate(current)
                    if inspect.isawaitable(accepted):
                        accepted = await accepted
                    try:
                        if not accepted:
                            emit = False
                            break
                    finally:
                        del accepted
                elif isinstance(operation, _Tap):
                    action_result = operation.action(current)
                    if inspect.isawaitable(action_result):
                        action_result = await action_result
                    del action_result
            if emit:
                yield current


async def _close_async_execution(
    iterator: AsyncIterator[Any] | None,
    runtime: QueryRuntime,
    active_error: BaseException | None,
) -> None:
    """Close the opened stage chain and runtime without masking an active failure."""
    # Explicit ``aclose()`` reaches async-generator finally blocks as
    # GeneratorExit. Cleanup failures must remain observable to the caller.
    if isinstance(active_error, GeneratorExit):
        active_error = None

    cleanup_error: BaseException | None = None
    if iterator is not None:
        try:
            # Every physical stage closes its direct upstream in turn. Closing
            # only the outermost iterator therefore walks the ownership chain
            # once; closing the root again would duplicate that walk.
            await close_async_iterators((iterator,), active_error=active_error)
        except BaseException as error:
            cleanup_error = error

    cleanup_primary = active_error if active_error is not None else cleanup_error
    try:
        # Runtime ownership is independent and must close even when the physical
        # iterator failed to open or its ``aclose`` has already failed.
        await runtime.aclose(cleanup_primary)
    except BaseException as error:
        if cleanup_primary is None:
            cleanup_error = error
        else:
            _add_cleanup_failure(cleanup_primary, [error])

    if active_error is None and cleanup_error is not None:
        raise cleanup_error


async def _iterate_async_physical(
    plan: AsyncPhysicalPlan,
    query_runtime: QueryRuntime,
) -> AsyncIterator[Any]:
    """Open and execute a compiled plan after the first downstream pull."""
    iterator: AsyncIterator[Any] | None = None
    active_error: BaseException | None = None
    try:
        iterator = plan.source.open()
        for node in plan.nodes:
            if isinstance(node, AsyncSerialStage):
                iterator = _execute_serial_stage(iterator, node.operations)
            elif isinstance(node, AsyncMapNode):
                iterator = execute_async_map(iterator, node, query_runtime)
            elif isinstance(node, AsyncMergeNode):
                iterator = execute_merge(iterator, node, query_runtime)
            elif isinstance(node, AsyncCombineLatestNode):
                iterator = execute_combine_latest(iterator, node, query_runtime)
            elif isinstance(node, AsyncMergeMapNode):
                iterator = execute_merge_map(iterator, node, query_runtime)
            elif isinstance(node, AsyncSwitchMapNode):
                iterator = execute_switch_map(iterator, node, query_runtime)
            elif isinstance(node, AsyncTimerNode):
                iterator = execute_async_timer(iterator, node, query_runtime)
            elif isinstance(node, AsyncPrefetchNode):
                iterator = execute_async_prefetch(iterator, node, query_runtime)
            elif isinstance(node, AsyncSerialOperationNode):
                iterator = apply_async_operation(iterator, node.operation)
            else:
                raise TypeError(f"unsupported async physical node: {type(node).__name__}")
        async for item in iterator:
            yield item
    except BaseException as error:
        active_error = error
        raise
    finally:
        await _close_async_execution(iterator, query_runtime, active_error)


class _RuntimeOwnedAsyncIterator:
    """Keep runtime cleanup reachable before the lazy executor has started."""

    __slots__ = ("_closed", "_inner", "_plan", "_runtime")

    def __init__(self, plan: AsyncPhysicalPlan, runtime: QueryRuntime) -> None:
        self._plan = plan
        self._runtime = runtime
        self._inner: AsyncIterator[Any] | None = None
        self._closed = False

    def __aiter__(self) -> _RuntimeOwnedAsyncIterator:
        return self

    def _open(self) -> AsyncIterator[Any]:
        if self._inner is None:
            self._inner = _iterate_async_physical(self._plan, self._runtime)
        return self._inner

    async def __anext__(self) -> Any:
        if self._closed:
            raise StopAsyncIteration
        try:
            return await anext(self._open())
        except BaseException:
            self._closed = True
            raise

    async def aclose(self) -> None:
        """Close an opened stage chain or just the still-unstarted runtime."""
        if self._closed:
            return
        self._closed = True
        await _close_async_execution(self._inner, self._runtime, None)


def execute_async_physical(
    plan: AsyncPhysicalPlan, runtime: QueryRuntime | None = None
) -> AsyncIterator[Any]:
    """Return a lazy async execution whose runtime can always be closed."""
    return _RuntimeOwnedAsyncIterator(plan, runtime or QueryRuntime())
