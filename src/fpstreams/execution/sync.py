"""Build, fuse, and close the lazy Python iterator chain for a sync plan."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any, cast

from ..expressions.scalar import Expr, FExpr
from ..planning.sync import FilterOp, GatherOp, MapOp, Plan, TapOp
from .sync_ops import apply_operation, close_iterators


def _fused(
    iterator: Iterator[Any],
    operations: tuple[MapOp | FilterOp | TapOp, ...],
) -> Iterator[Any]:
    """Apply an adjacent map/filter/tap run in one lazy Python loop.

    Operation callables are compiled once before iteration. Each item then visits
    the operations in plan order; a rejected filter skips the remainder of its run
    for that item, while maps and taps preserve encounter order.
    """
    compiled = tuple((operation, _operation_function(operation)) for operation in operations)
    for item in iterator:
        current = item
        accepted = True
        for operation, function in compiled:
            if isinstance(operation, MapOp):
                current = function(current)
            elif isinstance(operation, TapOp):
                function(current)
            elif bool(function(current)) is operation.negate:
                accepted = False
                break
        if accepted:
            yield current


def _operation_function(
    operation: MapOp | FilterOp | TapOp,
) -> Callable[[Any], Any]:
    """Return an operation callback, compiling Expr and FExpr evaluators once."""
    function = operation.predicate if isinstance(operation, FilterOp) else operation.function
    if isinstance(function, (Expr, FExpr)):
        return cast(Callable[[Any], Any], function._python_evaluator())
    return function


def _remember_iterator(stack: list[Iterator[Any]], iterator: Iterator[Any]) -> None:
    """Register each closeable iterator identity once for outer-to-inner cleanup."""
    if callable(getattr(iterator, "close", None)) and all(
        iterator is not existing for existing in stack
    ):
        stack.append(iterator)


def execute(plan: Plan) -> Iterator[Any]:
    """Interpret a plan lazily and close its iterator layers from outermost to source.

    Maximal map/filter/tap runs share one generator, and adjacent gatherers compose
    into one stateful traversal. Other nodes create their normal iterator layers.
    Completion, failure, or early consumer close triggers cleanup of every
    registered closeable layer without replacing an active pipeline exception.
    """
    source_iterator = plan.source.open()
    iterator: Iterator[Any] = source_iterator
    managed_iterators = [source_iterator]
    operations = plan.operations
    index = 0
    while index < len(operations):
        operation = operations[index]
        # Build one generator for the maximal adjacent map/filter/tap run.
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
