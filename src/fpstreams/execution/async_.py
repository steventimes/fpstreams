"""Build and clean up the lazy iterator chain for an asynchronous plan."""

from __future__ import annotations

import sys
from collections.abc import AsyncIterator
from typing import Any

from ..planning.async_ import _AsyncOperation, _AsyncPlan, _Filter, _MapAsync, _Tap
from ..planning.async_utils import _resolve
from .async_ops import apply_async_operation, close_async_iterators


def _is_fusable(operation: _AsyncOperation) -> bool:
    """Return whether an operation can share the serial fused loop.

    Filters and taps are always serial. A map is eligible only when it requests one
    worker and no per-call timeout, because fusion must not erase its concurrency or
    cancellation semantics.
    """
    return isinstance(operation, (_Filter, _Tap)) or (
        isinstance(operation, _MapAsync)
        and operation.concurrency == 1
        and operation.timeout is None
    )


async def _fused(
    source: AsyncIterator[Any], operations: tuple[_AsyncOperation, ...]
) -> AsyncIterator[Any]:
    """Apply adjacent serial maps, filters, and taps in one async iteration loop.

    Callbacks may return immediate or awaitable values. A rejected filter stops the
    remaining operations for that item, and closing this generator closes its
    upstream iterator while preserving any active exception.
    """
    try:
        async for item in source:
            current = item
            emit = True
            for operation in operations:
                if isinstance(operation, _MapAsync):
                    current = await _resolve(operation.function(current))
                elif isinstance(operation, _Filter):
                    if not await _resolve(operation.predicate(current)):
                        emit = False
                        break
                elif isinstance(operation, _Tap):
                    await _resolve(operation.action(current))
            if emit:
                yield current
    finally:
        await close_async_iterators((source,), active_error=sys.exception())


async def _execute(plan: _AsyncPlan[Any]) -> AsyncIterator[Any]:
    """Open a plan, fuse eligible runs, and yield from the final iterator layer.

    Non-fusible operations retain their dedicated iterator or task implementation.
    On completion, error, cancellation, or consumer close, cleanup starts at the
    outer layer and also closes the root source if it is distinct.
    """
    root = plan.source.open()
    iterator = root
    try:
        index = 0
        while index < len(plan.operations):
            operation = plan.operations[index]
            if _is_fusable(operation):
                end = index + 1
                while end < len(plan.operations) and _is_fusable(plan.operations[end]):
                    end += 1
                iterator = _fused(iterator, plan.operations[index:end])
                index = end
                continue
            iterator = apply_async_operation(iterator, operation)
            index += 1
        async for item in iterator:
            yield item
    finally:
        owned_iterators = (iterator, root) if iterator is not root else (root,)
        await close_async_iterators(owned_iterators, active_error=sys.exception())
