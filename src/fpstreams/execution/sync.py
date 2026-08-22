"""Build, fuse, and close the lazy Python iterator chain for a sync plan."""

from __future__ import annotations

from collections.abc import Callable, Generator, Iterator
from contextlib import contextmanager
from itertools import filterfalse
from operator import length_hint
from typing import Any, cast

from ..expressions.scalar import Expr, FExpr
from ..planning.sync import FilterOp, GatherOp, MapOp, Operation, TakeOp, TapOp
from ..runtime.query import QueryRuntime
from ._rows_fusion import execute_rows_fusion
from .sync_ops import apply_operation, close_iterators

_EXACT_SIZED_ITERATORS: tuple[type[Any], ...] = (
    type(iter([])),
    type(iter(())),
    type(iter(range(0))),
)


def _fused(
    iterator: Iterator[Any],
    operations: tuple[MapOp | FilterOp | TapOp, ...],
    *,
    instrumented: bool,
) -> Iterator[Any]:
    """Apply an adjacent map/filter/tap run in one lazy Python loop.

    Operation callables are compiled once before iteration. Each item then visits
    the operations in plan order; a rejected filter skips the remainder of its run
    for that item, while maps and taps preserve encounter order.
    """
    from ..runtime.failpoints import hit

    compiled = tuple((operation, _operation_function(operation)) for operation in operations)
    for item in iterator:
        current = item
        accepted = True
        for operation, function in compiled:
            if instrumented:
                hit("callback.before")
            if isinstance(operation, MapOp):
                current = function(current)
            elif isinstance(operation, TapOp):
                function(current)
            else:
                rejected = bool(function(current)) is operation.negate
                if instrumented:
                    hit("callback.after")
                if rejected:
                    accepted = False
                    break
                continue
            if instrumented:
                hit("callback.after")
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


def _after_pull(iterator: Iterator[Any]) -> Iterator[Any]:
    """Expose a test-only boundary immediately after each actual upstream pull."""
    from ..runtime.failpoints import hit

    for item in iterator:
        hit("iterator.pull.after")
        yield item


def _map_filter_chain(
    iterator: Iterator[Any],
    operations: tuple[MapOp | FilterOp, ...],
    *,
    eager: bool,
) -> Iterator[Any]:
    """Compose callback-only map/filter stages from CPython's lazy C iterators."""
    exact_rows = length_hint(iterator) if type(iterator) in _EXACT_SIZED_ITERATORS else None
    rows = execute_rows_fusion(iterator, operations, exact_rows=exact_rows, eager=eager)
    if rows is not None:
        return rows
    for operation in operations:
        function = _operation_function(operation)
        if isinstance(operation, MapOp):
            iterator = map(function, iterator)
        elif operation.negate:
            iterator = filterfalse(function, iterator)
        else:
            iterator = filter(function, iterator)
    return iterator


@contextmanager
def open_operations(
    source_iterator: Iterator[Any],
    operations: tuple[Operation, ...],
    *,
    runtime: QueryRuntime | None = None,
) -> Generator[Iterator[Any], None, None]:
    """Build one canonical iterator chain and retain ownership until the caller exits."""
    from ..runtime.failpoints import has_active_failpoints

    instrumented = has_active_failpoints()
    iterator: Iterator[Any] = _after_pull(source_iterator) if instrumented else source_iterator
    managed_iterators = [source_iterator]
    _remember_iterator(managed_iterators, iterator)
    try:
        index = 0
        while index < len(operations):
            operation = operations[index]
            # Build one lazy iterator for the maximal adjacent map/filter/tap run.
            if isinstance(operation, (MapOp, FilterOp, TapOp)):
                end = index + 1
                while end < len(operations) and isinstance(
                    operations[end], (MapOp, FilterOp, TapOp)
                ):
                    end += 1
                fused_operations = cast(tuple[MapOp | FilterOp | TapOp, ...], operations[index:end])
                if not instrumented and all(
                    isinstance(candidate, (MapOp, FilterOp)) for candidate in fused_operations
                ):
                    iterator = _map_filter_chain(
                        iterator,
                        cast(tuple[MapOp | FilterOp, ...], fused_operations),
                        eager=not (end < len(operations) and isinstance(operations[end], TakeOp)),
                    )
                else:
                    iterator = _fused(iterator, fused_operations, instrumented=instrumented)
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
                iterator = apply_operation(iterator, GatherOp(gatherer), runtime=runtime)
                _remember_iterator(managed_iterators, iterator)
                index = end
                continue
            iterator = apply_operation(iterator, operation, runtime=runtime)
            _remember_iterator(managed_iterators, iterator)
            index += 1
        yield iterator
    finally:
        close_iterators(reversed(managed_iterators))


def execute_operations(
    source_iterator: Iterator[Any],
    operations: tuple[Operation, ...],
    *,
    runtime: QueryRuntime | None = None,
) -> Iterator[Any]:
    """Execute canonical physical operations without reconstructing a planning object."""
    with open_operations(source_iterator, operations, runtime=runtime) as iterator:
        yield from iterator
