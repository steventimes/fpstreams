"""Pure-Python orchestration for asynchronous operation plans."""

from __future__ import annotations

import sys
from collections.abc import AsyncIterator
from typing import Any

from ..planning.async_ import _AsyncOperation, _AsyncPlan, _Filter, _MapAsync, _Tap
from ..planning.async_utils import _resolve
from .async_ops import apply_async_operation, close_async_iterators


def _is_fusable(operation: _AsyncOperation) -> bool:
    return isinstance(operation, (_Filter, _Tap)) or (
        isinstance(operation, _MapAsync)
        and operation.concurrency == 1
        and operation.timeout is None
    )


async def _fused(
    source: AsyncIterator[Any], operations: tuple[_AsyncOperation, ...]
) -> AsyncIterator[Any]:
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
