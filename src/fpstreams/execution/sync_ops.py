"""Construct the synchronous iterator layer for each plan operation.

The plan executor owns and closes the passed upstream iterator and every closeable
layer it creates. Operators that open additional sources or executors own and clean
up those local resources themselves.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterator
from concurrent.futures import (
    FIRST_COMPLETED,
    Future,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    wait,
)
from itertools import dropwhile, islice, pairwise, takewhile, zip_longest
from multiprocessing import get_context
from typing import Any

from ..errors import BufferLimitError, SelectionError
from ..expressions.selectors import _direct_field
from ..planning._pair_stages import PAIR_KEY_SELECTOR, PairFlatMapDescriptor
from ..planning.gather import Downstream
from ..planning.sync import (
    AppendOp,
    ChunkOp,
    CollapseOp,
    ConcatOp,
    CrossOp,
    DropOp,
    DropWhileOp,
    EnumerateOp,
    FilterOp,
    FlatMapOp,
    GatherOp,
    GroupRunsOp,
    IntersperseOp,
    MapFirstOp,
    MapLastOp,
    MapOp,
    Operation,
    PairwiseOp,
    ParallelMapOp,
    PrependOp,
    ScanOp,
    ScanRightOp,
    SortOp,
    TakeOp,
    TakeWhileInclusiveOp,
    TakeWhileOp,
    TapOp,
    UniqueOp,
    WindowOp,
    ZipLongestOp,
    ZipOp,
)
from ..runtime.iterators import close_iterators as close_iterators
from ..runtime.iterators import closing_iterators
from ..runtime.query import QueryRuntime
from .sorting import external_sort


def _map(iterator: Iterator[Any], operation: MapOp) -> Iterator[Any]:
    """Apply the mapping function lazily to each input item in encounter order."""
    for item in iterator:
        yield operation.function(item)


def _take_while_inclusive(
    iterator: Iterator[Any], operation: TakeWhileInclusiveOp
) -> Iterator[Any]:
    """Yield through the first predicate failure, including that terminating item."""
    for item in iterator:
        yield item
        if not operation.predicate(item):
            return


def _map_parallel(iterator: Iterator[Any], operation: ParallelMapOp) -> Iterator[Any]:
    """Map with a bounded thread pool or spawn-based process pool.

    At most buffer futures remain queued. Ordered mode awaits futures in submission
    order; unordered mode yields completed sets as they become available. Callback
    failures propagate through Future.result, and early exit cancels queued work
    before waiting for executor shutdown.
    """
    executor = (
        ThreadPoolExecutor(max_workers=operation.workers)
        if operation.backend == "thread"
        else ProcessPoolExecutor(
            max_workers=operation.workers,
            mp_context=get_context("spawn"),
        )
    )
    if operation.ordered:
        pending: deque[Future[Any]] = deque()
        try:
            for item in iterator:
                pending.append(executor.submit(operation.function, item))
                if len(pending) >= operation.buffer:
                    yield pending.popleft().result()
            while pending:
                yield pending.popleft().result()
        finally:
            for future in pending:
                future.cancel()
            executor.shutdown(wait=True, cancel_futures=True)
        return

    pending_set: set[Future[Any]] = set()
    try:
        for item in iterator:
            pending_set.add(executor.submit(operation.function, item))
            if len(pending_set) >= operation.buffer:
                done, pending_set = wait(pending_set, return_when=FIRST_COMPLETED)
                for future in done:
                    yield future.result()
        while pending_set:
            done, pending_set = wait(pending_set, return_when=FIRST_COMPLETED)
            for future in done:
                yield future.result()
    finally:
        for future in pending_set:
            future.cancel()
        executor.shutdown(wait=True, cancel_futures=True)


def _tap(iterator: Iterator[Any], operation: TapOp) -> Iterator[Any]:
    """Run the side-effect callback before yielding each original item unchanged."""
    for item in iterator:
        operation.function(item)
        yield item


def _filter(iterator: Iterator[Any], operation: FilterOp) -> Iterator[Any]:
    """Yield items whose predicate truth value differs from the negate flag."""
    for item in iterator:
        if bool(operation.predicate(item)) is not operation.negate:
            yield item


def _flat_map(iterator: Iterator[Any], operation: FlatMapOp) -> Iterator[Any]:
    """Map each item to an iterable and flatten those iterables in source order."""
    function = operation.function
    if type(function) is PairFlatMapDescriptor:
        callback = function.callback
        for pair in iterator:
            yield from callback(pair[0], pair[1])
        return

    for item in iterator:
        yield from function(item)


def _unique(iterator: Iterator[Any], operation: UniqueOp) -> Iterator[Any]:
    """Yield the first item for each key while preserving source order.

    The item itself is the key when no key function is configured. Hashable keys
    use a set; unhashable keys fall back to equality against a linear list.
    """
    hashable: set[Any] = set()
    unhashable: list[Any] = []
    if operation.key is PAIR_KEY_SELECTOR:
        for item in iterator:
            key = item[0]
            try:
                if key in hashable:
                    continue
                hashable.add(key)
            except TypeError:
                if any(key == seen for seen in unhashable):
                    continue
                unhashable.append(key)
            yield item
        return

    key_function = operation.key
    for item in iterator:
        key = key_function(item) if key_function is not None else item
        try:
            if key in hashable:
                continue
            hashable.add(key)
        except TypeError:
            if any(key == seen for seen in unhashable):
                continue
            unhashable.append(key)
        yield item


def _chunk(iterator: Iterator[Any], operation: ChunkOp) -> Iterator[tuple[Any, ...]]:
    """Group consecutive items into tuples, including a final short chunk."""
    while chunk := tuple(islice(iterator, operation.size)):
        yield chunk


def _window(iterator: Iterator[Any], operation: WindowOp) -> Iterator[tuple[Any, ...]]:
    """Yield fixed-size sliding windows, advancing step source items each time.

    A nonempty source shorter than size produces one partial window. Exhaustion
    while advancing a full window ends iteration without a trailing partial one.
    """
    current = deque(islice(iterator, operation.size), maxlen=operation.size)
    if not current:
        return
    if len(current) < operation.size:
        yield tuple(current)
        return
    while True:
        yield tuple(current)
        for _ in range(operation.step):
            try:
                current.append(next(iterator))
            except StopIteration:
                return


def _group_runs(iterator: Iterator[Any], operation: GroupRunsOp) -> Iterator[tuple[Any, ...]]:
    """Group consecutive items with equal keys into nonempty tuples."""
    try:
        first = next(iterator)
    except StopIteration:
        return
    current = [first]
    current_key = operation.key(first)
    for item in iterator:
        item_key = operation.key(item)
        if item_key == current_key:
            current.append(item)
            continue
        yield tuple(current)
        current = [item]
        current_key = item_key
    yield tuple(current)


def _zip(iterator: Iterator[Any], operation: ZipOp) -> Iterator[tuple[Any, Any]]:
    """Zip with a locally opened right source, optionally requiring equal lengths.

    Built-in strict zip supplies mismatch diagnostics. This layer closes the right
    iterator; the plan executor owns the passed left iterator.
    """
    other = operation.source.open()
    with closing_iterators((other,)):
        yield from zip(iterator, other, strict=operation.strict)


def _zip_longest(iterator: Iterator[Any], operation: ZipLongestOp) -> Iterator[tuple[Any, Any]]:
    """Zip to the longer input with fillvalue and close the locally opened right side."""
    other = operation.source.open()
    with closing_iterators((other,)):
        yield from zip_longest(iterator, other, fillvalue=operation.fillvalue)


def _intersperse(iterator: Iterator[Any], operation: IntersperseOp) -> Iterator[Any]:
    """Insert one separator between adjacent items, with none at either boundary."""
    try:
        first = next(iterator)
    except StopIteration:
        return
    yield first
    for item in iterator:
        yield operation.separator
        yield item


def _concat(iterator: Iterator[Any], operation: ConcatOp) -> Iterator[Any]:
    """Drain upstream, then lazily open and drain each additional source in order.

    Every additional iterator is closed before advancing to the next source.
    """
    yield from iterator
    for source in operation.sources:
        other = source.open()
        with closing_iterators((other,)):
            yield from other


def _scan(iterator: Iterator[Any], operation: ScanOp) -> Iterator[Any]:
    """Emit each left-to-right accumulated state after incorporating one item."""
    state = operation.initial
    for item in iterator:
        state = operation.function(state, item)
        yield state


def _scan_right(iterator: Iterator[Any], operation: ScanRightOp) -> Iterator[Any]:
    """Buffer input, accumulate from right to left, then emit states in source order.

    The callback receives (item, state), and the initial state is not emitted.
    max_items raises before retaining an excess item.
    """
    values: list[Any] = []
    for item in iterator:
        if operation.max_items is not None and len(values) >= operation.max_items:
            raise BufferLimitError(f"scan_right() exceeded max_items={operation.max_items}")
        values.append(item)
    state = operation.initial
    for index in range(len(values) - 1, -1, -1):
        state = operation.function(values[index], state)
        values[index] = state
    yield from values


def _cross(iterator: Iterator[Any], operation: CrossOp) -> Iterator[tuple[Any, Any]]:
    """Emit a left-major Cartesian product after caching the right source once.

    The right side is opened only when a first left item exists. max_right bounds
    its cache and raises before retaining an excess item. This layer closes the
    right iterator; the plan executor owns the left iterator.
    """
    right_values: list[Any] = []
    right_iterator: Iterator[Any] | None = None
    initialized = False
    active_error: BaseException | None = None
    try:
        for left in iterator:
            if not initialized:
                initialized = True
                right_iterator = operation.source.open()
                right_error: BaseException | None = None
                try:
                    for right in right_iterator:
                        if (
                            operation.max_right is not None
                            and len(right_values) >= operation.max_right
                        ):
                            raise BufferLimitError(
                                f"cross() exceeded max_right={operation.max_right}"
                            )
                        right_values.append(right)
                except BaseException as error:
                    right_error = error
                    raise
                finally:
                    owned_right = right_iterator
                    right_iterator = None
                    close_iterators((owned_right,), active_error=right_error)
            for right in right_values:
                yield left, right
    except BaseException as error:
        active_error = error
        raise
    finally:
        if right_iterator is not None:
            close_iterators((right_iterator,), active_error=active_error)


def _gather(iterator: Iterator[Any], operation: GatherOp) -> Iterator[Any]:
    """Drive one stateful gatherer and yield values pushed for each input.

    Integration may emit zero or more buffered values and may stop source
    consumption by returning false. The finisher runs after normal exhaustion or
    gatherer-directed stop. If downstream closes this generator, the finisher
    receives a rejecting channel so it can clean up without emitting.
    """
    gatherer = operation.gatherer
    state = gatherer.initializer()
    emitted: list[Any] = []

    def emit(value: Any) -> bool:
        """Buffer one gatherer output for emission after the current callback returns."""
        emitted.append(value)
        return True

    downstream = Downstream(emit)
    finished = False
    try:
        for item in iterator:
            emitted.clear()
            proceed = gatherer._integrate(state, item, downstream)
            yield from emitted
            if not proceed:
                break
        emitted.clear()
        gatherer._finish(state, downstream)
        finished = True
        yield from emitted
    except GeneratorExit:
        if not finished:
            rejecting: Downstream[Any] = Downstream(lambda _value: False, rejecting=True)
            gatherer._finish(state, rejecting)
        raise


def _prepend(iterator: Iterator[Any], operation: PrependOp) -> Iterator[Any]:
    """Yield configured leading values before consuming the upstream iterator."""
    yield from operation.values
    yield from iterator


def _append(iterator: Iterator[Any], operation: AppendOp) -> Iterator[Any]:
    """Drain upstream before yielding the configured trailing values."""
    yield from iterator
    yield from operation.values


def _map_first(iterator: Iterator[Any], operation: MapFirstOp) -> Iterator[Any]:
    """Map only the first item and pass every later item through unchanged."""
    try:
        first = next(iterator)
    except StopIteration:
        return
    yield operation.function(first)
    yield from iterator


def _map_last(iterator: Iterator[Any], operation: MapLastOp) -> Iterator[Any]:
    """Pass all but the final item unchanged, then map and emit that final item.

    One item is held pending so an empty input remains empty.
    """
    try:
        pending = next(iterator)
    except StopIteration:
        return
    for item in iterator:
        yield pending
        pending = item
    yield operation.function(pending)


def _collapse(iterator: Iterator[Any], operation: CollapseOp) -> Iterator[Any]:
    """Merge adjacent collapsible runs and emit one aggregate per run.

    Collapsibility is tested on neighboring original items, while merger combines
    the current run aggregate with the new item.
    """
    try:
        previous = aggregate = next(iterator)
    except StopIteration:
        return
    for item in iterator:
        if operation.collapsible(previous, item):
            aggregate = operation.merger(aggregate, item)
        else:
            yield aggregate
            aggregate = item
        previous = item
    yield aggregate


def _take(iterator: Iterator[Any], operation: TakeOp) -> Iterator[Any]:
    """Return an islice that yields at most the first count items."""
    return islice(iterator, operation.count)


def _drop(iterator: Iterator[Any], operation: DropOp) -> Iterator[Any]:
    """Return an islice that skips count items before yielding the remainder."""
    return islice(iterator, operation.count, None)


def _take_while(iterator: Iterator[Any], operation: TakeWhileOp) -> Iterator[Any]:
    """Yield the leading truthy-predicate run and consume its first failing item."""
    return takewhile(operation.predicate, iterator)


def _drop_while(iterator: Iterator[Any], operation: DropWhileOp) -> Iterator[Any]:
    """Discard the leading truthy-predicate run, then stop testing and yield the rest."""
    return dropwhile(operation.predicate, iterator)


def _pairwise(iterator: Iterator[Any], _operation: PairwiseOp) -> Iterator[Any]:
    """Return overlapping adjacent pairs; the operation node carries no settings."""
    return pairwise(iterator)


def _enumerate(iterator: Iterator[Any], operation: EnumerateOp) -> Iterator[Any]:
    """Pair items with consecutive integers beginning at operation.start."""
    return enumerate(iterator, operation.start)


def _sort(
    iterator: Iterator[Any],
    operation: SortOp,
    *,
    runtime: QueryRuntime | None = None,
) -> Iterator[Any]:
    """Select in-memory sorted or bounded external sorting.

    A missing buffer_size materializes directly with built-in sorted; otherwise
    external_sort spills sorted runs under the configured temporary directory.
    """
    if operation.buffer_size is not None:
        return external_sort(iterator, operation, runtime=runtime)
    field = _direct_field(operation.key)
    try:
        from .. import _native
    except ImportError:
        exact_dict_rows = None
        direct_dict_field_key = None
    else:
        exact_dict_rows = getattr(_native, "all_exact_dict_rows_v1", None)
        direct_dict_field_key = getattr(_native, "direct_dict_field_key_v1", None)
    if field is None or exact_dict_rows is None or direct_dict_field_key is None:
        return iter(sorted(iterator, key=operation.key, reverse=operation.reverse))
    values = list(iterator)
    if not exact_dict_rows(values):
        values.sort(key=operation.key, reverse=operation.reverse)
        return iter(values)
    values.sort(key=direct_dict_field_key(field, SelectionError), reverse=operation.reverse)
    return iter(values)


OperationHandler = Callable[..., Iterator[Any]]
OPERATION_HANDLERS: dict[type[object], OperationHandler] = {
    MapOp: _map,
    ParallelMapOp: _map_parallel,
    TapOp: _tap,
    FilterOp: _filter,
    FlatMapOp: _flat_map,
    TakeOp: _take,
    DropOp: _drop,
    TakeWhileOp: _take_while,
    TakeWhileInclusiveOp: _take_while_inclusive,
    DropWhileOp: _drop_while,
    UniqueOp: _unique,
    ChunkOp: _chunk,
    WindowOp: _window,
    GroupRunsOp: _group_runs,
    PairwiseOp: _pairwise,
    EnumerateOp: _enumerate,
    ZipOp: _zip,
    ZipLongestOp: _zip_longest,
    IntersperseOp: _intersperse,
    ConcatOp: _concat,
    CrossOp: _cross,
    ScanOp: _scan,
    ScanRightOp: _scan_right,
    SortOp: _sort,
    GatherOp: _gather,
    PrependOp: _prepend,
    AppendOp: _append,
    MapFirstOp: _map_first,
    MapLastOp: _map_last,
    CollapseOp: _collapse,
}
SUPPORTED_OPERATION_TYPES: tuple[type[object], ...] = tuple(OPERATION_HANDLERS)


def apply_operation(
    iterator: Iterator[Any],
    operation: Operation,
    *,
    runtime: QueryRuntime | None = None,
) -> Iterator[Any]:
    """Construct the iterator layer registered for the operation's exact type.

    Unknown subclasses are rejected instead of implicitly reusing a base handler.
    """
    handler = OPERATION_HANDLERS.get(type(operation))
    if handler is None:
        raise TypeError(f"unsupported synchronous operation: {type(operation).__name__}")
    if type(operation) is SortOp:
        return _sort(iterator, operation, runtime=runtime)
    return handler(iterator, operation)
