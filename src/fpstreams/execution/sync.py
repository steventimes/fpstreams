"""Build, fuse, and close the lazy Python iterator chain for a sync plan."""

from __future__ import annotations

from collections.abc import Callable, Generator, Iterator
from contextlib import contextmanager
from itertools import filterfalse
from operator import length_hint
from typing import Any, cast

from ..expressions.scalar import _compile_scalar_callable
from ..planning._pair_stages import PairFilterDescriptor, PairMapDescriptor
from ..planning.sync import FilterOp, GatherOp, MapOp, Operation, TakeOp, TapOp
from ..runtime.iterators import closing_iterators
from ..runtime.query import QueryRuntime
from ._rows_fusion import execute_rows_fusion
from .sync_ops import apply_operation

_EXACT_SIZED_ITERATORS: tuple[type[Any], ...] = (
    type(iter([])),
    type(iter(())),
    type(iter(range(0))),
)


class _NonClosingIterator:
    """Delegate pulls without exposing the owned iterator's ``close`` method.

    ``yield from`` otherwise forwards ``close`` to its delegate.  The surrounding
    ownership context already closes every layer, so this view avoids both a second
    close and a forwarding generator frame retaining the last emitted value.
    """

    __slots__ = ("_iterator",)

    def __init__(self, iterator: Iterator[Any]) -> None:
        self._iterator = iterator

    def __iter__(self) -> _NonClosingIterator:
        return self

    def __next__(self) -> Any:
        return next(self._iterator)


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
    return cast(Callable[[Any], Any], _compile_scalar_callable(function))


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
    fuse_callable_map_filter: bool,
) -> Iterator[Any]:
    """Compose callback-only map/filter stages from CPython's lazy C iterators."""
    exact_rows = length_hint(iterator) if type(iterator) in _EXACT_SIZED_ITERATORS else None
    rows = execute_rows_fusion(iterator, operations, exact_rows=exact_rows, eager=eager)
    if rows is not None:
        return rows
    if (
        fuse_callable_map_filter
        and len(operations) == 2
        and type(first := operations[0]) is MapOp
        and type(second := operations[1]) is FilterOp
    ):
        from ..planning.arrow import _row_stage_descriptor
        from ..tabular.rows import _DropNullsPlan

        transform = _operation_function(first)
        predicate = _operation_function(second)
        if (
            type(transform) is not PairMapDescriptor
            and type(predicate) is not PairFilterDescriptor
            and type(second.predicate) is not _DropNullsPlan
            and _row_stage_descriptor(first.function) is None
            and _row_stage_descriptor(second.predicate) is None
        ):
            return _map_then_filter(iterator, transform, predicate, second.negate)
    for operation in operations:
        function = _operation_function(operation)
        if isinstance(operation, MapOp):
            iterator = (
                _map_pair_side(iterator, function)
                if type(function) is PairMapDescriptor
                else map(function, iterator)
            )
        elif type(function) is PairFilterDescriptor:
            iterator = _filter_pair_target(iterator, function, operation.negate)
        elif operation.negate:
            iterator = filterfalse(function, iterator)
        else:
            from ..tabular.rows import _planned_drop_nulls_filter

            planned = _planned_drop_nulls_filter(function, iterator)
            iterator = filter(function, iterator) if planned is None else planned
    return iterator


def _map_then_filter(
    iterator: Iterator[Any],
    transform: Callable[[Any], Any],
    predicate: Callable[[Any], Any],
    negate: bool,
) -> Iterator[Any]:
    """Execute one ordinary map/filter pair without stacked iterator frames."""
    if negate:
        for item in iterator:
            try:
                current = transform(item)
            except StopIteration:
                return
            except BaseException:
                del item
                raise
            del item
            try:
                if predicate(current):
                    del current
                    continue
            except StopIteration:
                return
            except BaseException:
                del current
                raise
            yield current
            del current
        return

    for item in iterator:
        try:
            current = transform(item)
        except StopIteration:
            return
        except BaseException:
            del item
            raise
        del item
        try:
            if not predicate(current):
                del current
                continue
        except StopIteration:
            return
        except BaseException:
            del current
            raise
        yield current
        del current


def _map_pair_side(
    iterator: Iterator[Any],
    descriptor: PairMapDescriptor,
) -> Iterator[tuple[Any, Any]]:
    """Map selected pair fields without calling the internal adapter function."""
    callback = descriptor.callback
    if descriptor.side == "pair":
        for pair in iterator:
            try:
                mapped = callback(pair[0], pair[1])
            except StopIteration:
                return
            del pair
            yield mapped
            del mapped
        return

    callback = _compile_scalar_callable(callback)
    if descriptor.side == "value":
        for pair in iterator:
            try:
                key = pair[0]
                value = callback(pair[1])
            except StopIteration:
                return
            del pair
            yield key, value
            del key, value
        return

    for pair in iterator:
        try:
            key = callback(pair[0])
            value = pair[1]
        except StopIteration:
            return
        del pair
        yield key, value
        del key, value


def _filter_pair_target(  # noqa: C901 - target-local loops remove per-item adapter calls
    iterator: Iterator[Any],
    descriptor: PairFilterDescriptor,
    negate: bool,
) -> Iterator[Any]:
    """Filter pairs by selected fields without calling the internal adapter."""
    callback = descriptor.callback
    if descriptor.target == "pair":
        for pair in iterator:
            try:
                result = callback(pair[0], pair[1])
            except StopIteration:
                return
            try:
                if negate if result else not negate:
                    del result, pair
                    continue
            except StopIteration:
                return
            except BaseException:
                del pair, result
                raise
            del result
            yield pair
            del pair
        return

    if descriptor.target == "row":
        for pair in iterator:
            try:
                result = callback(pair)
            except StopIteration:
                return
            try:
                if negate if result else not negate:
                    del result, pair
                    continue
            except StopIteration:
                return
            except BaseException:
                del pair, result
                raise
            del result
            yield pair
            del pair
        return

    callback = _compile_scalar_callable(callback)
    if descriptor.target == "key":
        for pair in iterator:
            try:
                result = callback(pair[0])
            except StopIteration:
                return
            try:
                if negate if result else not negate:
                    del result, pair
                    continue
            except StopIteration:
                return
            except BaseException:
                del pair, result
                raise
            del result
            yield pair
            del pair
        return

    for pair in iterator:
        try:
            result = callback(pair[1])
        except StopIteration:
            return
        try:
            if negate if result else not negate:
                del result, pair
                continue
        except StopIteration:
            return
        except BaseException:
            del pair, result
            raise
        del result
        yield pair
        del pair


@contextmanager
def open_operations(
    source_iterator: Iterator[Any],
    operations: tuple[Operation, ...],
    *,
    runtime: QueryRuntime | None = None,
    fuse_callable_map_filter: bool = False,
) -> Generator[Iterator[Any], None, None]:
    """Build one canonical iterator chain and retain ownership until the caller exits."""
    from ..runtime.failpoints import has_active_failpoints

    instrumented = has_active_failpoints()
    iterator: Iterator[Any] = _after_pull(source_iterator) if instrumented else source_iterator
    managed_iterators = [source_iterator]
    _remember_iterator(managed_iterators, iterator)

    def managed_in_reverse() -> Iterator[Iterator[Any]]:
        """Read the complete dynamically built ownership stack only at context exit."""
        yield from reversed(managed_iterators)

    with closing_iterators(managed_in_reverse()):
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
                        fuse_callable_map_filter=fuse_callable_map_filter,
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


def execute_operations(
    source_iterator: Iterator[Any],
    operations: tuple[Operation, ...],
    *,
    runtime: QueryRuntime | None = None,
) -> Iterator[Any]:
    """Execute canonical physical operations without reconstructing a planning object."""
    with open_operations(source_iterator, operations, runtime=runtime) as iterator:
        yield from _NonClosingIterator(iterator)
