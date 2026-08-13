"""Pure-Python orchestration for synchronous operation plans."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, cast

from ..planning.sync import FilterOp, GatherOp, MapOp, Plan, TapOp
from .sync_ops import apply_operation, close_iterators


def _fused(
    iterator: Iterator[Any],
    operations: tuple[MapOp | FilterOp | TapOp, ...],
) -> Iterator[Any]:
    # Keep adjacent stateless stages in one Python loop without materializing intermediates.
    for item in iterator:
        current = item
        accepted = True
        for operation in operations:
            if isinstance(operation, MapOp):
                current = operation.function(current)
            elif isinstance(operation, TapOp):
                operation.function(current)
            elif bool(operation.predicate(current)) is operation.negate:
                accepted = False
                break
        if accepted:
            yield current


def _remember_iterator(stack: list[Iterator[Any]], iterator: Iterator[Any]) -> None:
    if callable(getattr(iterator, "close", None)) and all(
        iterator is not existing for existing in stack
    ):
        stack.append(iterator)


def execute(plan: Plan) -> Iterator[Any]:
    source_iterator = plan.source.open()
    iterator: Iterator[Any] = source_iterator
    managed_iterators = [source_iterator]
    operations = plan.operations
    index = 0
    while index < len(operations):
        operation = operations[index]
        # Coalesce compatible neighbors before constructing the next lazy iterator layer.
        if isinstance(operation, (MapOp, FilterOp, TapOp)):
            end = index + 1
            while end < len(operations) and isinstance(operations[end], (MapOp, FilterOp, TapOp)):
                end += 1
            fused_operations = cast(tuple[MapOp | FilterOp | TapOp, ...], operations[index:end])
            iterator = _fused(iterator, fused_operations)
            _remember_iterator(managed_iterators, iterator)
            index = end
            continue
        if isinstance(operation, GatherOp):
            gatherer = operation.gatherer
            end = index + 1
            while end < len(operations) and isinstance(operations[end], GatherOp):
                next_operation = cast(GatherOp, operations[end])
                gatherer = gatherer.and_then(next_operation.gatherer)
                end += 1
            iterator = apply_operation(iterator, GatherOp(gatherer))
            _remember_iterator(managed_iterators, iterator)
            index = end
            continue
        iterator = apply_operation(iterator, operation)
        _remember_iterator(managed_iterators, iterator)
        index += 1

    try:
        yield from iterator
    finally:
        close_iterators(reversed(managed_iterators))
