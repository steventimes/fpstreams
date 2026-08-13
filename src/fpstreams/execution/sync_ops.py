"""Iterator transformations for one synchronous plan operation."""

from __future__ import annotations

import sys
from collections import deque
from collections.abc import Iterable, Iterator
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

from ..errors import BufferLimitError
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
from .sorting import external_sort

SUPPORTED_OPERATION_TYPES: tuple[type[object], ...] = (
    MapOp,
    ParallelMapOp,
    TapOp,
    FilterOp,
    FlatMapOp,
    TakeOp,
    DropOp,
    TakeWhileOp,
    TakeWhileInclusiveOp,
    DropWhileOp,
    UniqueOp,
    ChunkOp,
    WindowOp,
    GroupRunsOp,
    PairwiseOp,
    EnumerateOp,
    ZipOp,
    ZipLongestOp,
    IntersperseOp,
    ConcatOp,
    CrossOp,
    ScanOp,
    ScanRightOp,
    SortOp,
    GatherOp,
    PrependOp,
    AppendOp,
    MapFirstOp,
    MapLastOp,
    CollapseOp,
)


def _map(iterator: Iterator[Any], operation: MapOp) -> Iterator[Any]:
    for item in iterator:
        yield operation.function(item)


def _take_while_inclusive(
    iterator: Iterator[Any], operation: TakeWhileInclusiveOp
) -> Iterator[Any]:
    for item in iterator:
        yield item
        if not operation.predicate(item):
            return


def _map_parallel(iterator: Iterator[Any], operation: ParallelMapOp) -> Iterator[Any]:
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
    for item in iterator:
        operation.function(item)
        yield item


def _filter(iterator: Iterator[Any], operation: FilterOp) -> Iterator[Any]:
    for item in iterator:
        if bool(operation.predicate(item)) is not operation.negate:
            yield item


def _flat_map(iterator: Iterator[Any], operation: FlatMapOp) -> Iterator[Any]:
    for item in iterator:
        yield from operation.function(item)


def _unique(iterator: Iterator[Any], operation: UniqueOp) -> Iterator[Any]:
    hashable: set[Any] = set()
    unhashable: list[Any] = []
    for item in iterator:
        key = operation.key(item) if operation.key is not None else item
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
    while chunk := tuple(islice(iterator, operation.size)):
        yield chunk


def _window(iterator: Iterator[Any], operation: WindowOp) -> Iterator[tuple[Any, ...]]:
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


def close_iterator(iterator: Iterator[Any]) -> None:
    close = getattr(iterator, "close", None)
    if callable(close):
        close()


def close_iterators(iterators: Iterable[Iterator[Any]]) -> None:
    active_error = sys.exception()
    first_cleanup_error: BaseException | None = None

    for iterator in iterators:
        try:
            close_iterator(iterator)
        except BaseException as error:
            note = f"cleanup failed with {type(error).__name__}: {error}"
            if active_error is not None:
                active_error.add_note(note)
            elif first_cleanup_error is None:
                first_cleanup_error = error
            else:
                first_cleanup_error.add_note(note)

    if first_cleanup_error is not None:
        raise first_cleanup_error


def _zip(iterator: Iterator[Any], operation: ZipOp) -> Iterator[tuple[Any, Any]]:
    other = operation.source.open()
    try:
        yield from zip(iterator, other, strict=operation.strict)
    finally:
        close_iterator(other)


def _zip_longest(iterator: Iterator[Any], operation: ZipLongestOp) -> Iterator[tuple[Any, Any]]:
    other = operation.source.open()
    try:
        yield from zip_longest(iterator, other, fillvalue=operation.fillvalue)
    finally:
        close_iterator(other)


def _intersperse(iterator: Iterator[Any], operation: IntersperseOp) -> Iterator[Any]:
    try:
        first = next(iterator)
    except StopIteration:
        return
    yield first
    for item in iterator:
        yield operation.separator
        yield item


def _concat(iterator: Iterator[Any], operation: ConcatOp) -> Iterator[Any]:
    yield from iterator
    for source in operation.sources:
        other = source.open()
        try:
            yield from other
        finally:
            close_iterator(other)


def _scan(iterator: Iterator[Any], operation: ScanOp) -> Iterator[Any]:
    state = operation.initial
    for item in iterator:
        state = operation.function(state, item)
        yield state


def _scan_right(iterator: Iterator[Any], operation: ScanRightOp) -> Iterator[Any]:
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
    right_values: list[Any] = []
    right_iterator: Iterator[Any] | None = None
    initialized = False
    try:
        for left in iterator:
            if not initialized:
                initialized = True
                right_iterator = operation.source.open()
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
                finally:
                    close_iterator(right_iterator)
                    right_iterator = None
            for right in right_values:
                yield left, right
    finally:
        if right_iterator is not None:
            close_iterator(right_iterator)


def _gather(iterator: Iterator[Any], operation: GatherOp) -> Iterator[Any]:
    gatherer = operation.gatherer
    state = gatherer.initializer()
    emitted: list[Any] = []

    def emit(value: Any) -> bool:
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
    yield from operation.values
    yield from iterator


def _append(iterator: Iterator[Any], operation: AppendOp) -> Iterator[Any]:
    yield from iterator
    yield from operation.values


def _map_first(iterator: Iterator[Any], operation: MapFirstOp) -> Iterator[Any]:
    try:
        first = next(iterator)
    except StopIteration:
        return
    yield operation.function(first)
    yield from iterator


def _map_last(iterator: Iterator[Any], operation: MapLastOp) -> Iterator[Any]:
    try:
        pending = next(iterator)
    except StopIteration:
        return
    for item in iterator:
        yield pending
        pending = item
    yield operation.function(pending)


def _collapse(iterator: Iterator[Any], operation: CollapseOp) -> Iterator[Any]:
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


def apply_operation(iterator: Iterator[Any], operation: Operation) -> Iterator[Any]:
    if isinstance(operation, MapOp):
        return _map(iterator, operation)
    if isinstance(operation, ParallelMapOp):
        return _map_parallel(iterator, operation)
    if isinstance(operation, TapOp):
        return _tap(iterator, operation)
    if isinstance(operation, FilterOp):
        return _filter(iterator, operation)
    if isinstance(operation, FlatMapOp):
        return _flat_map(iterator, operation)
    if isinstance(operation, TakeOp):
        return islice(iterator, operation.count)
    if isinstance(operation, DropOp):
        return islice(iterator, operation.count, None)
    if isinstance(operation, TakeWhileOp):
        return takewhile(operation.predicate, iterator)
    if isinstance(operation, TakeWhileInclusiveOp):
        return _take_while_inclusive(iterator, operation)
    if isinstance(operation, DropWhileOp):
        return dropwhile(operation.predicate, iterator)
    if isinstance(operation, UniqueOp):
        return _unique(iterator, operation)
    if isinstance(operation, ChunkOp):
        return _chunk(iterator, operation)
    if isinstance(operation, WindowOp):
        return _window(iterator, operation)
    if isinstance(operation, GroupRunsOp):
        return _group_runs(iterator, operation)
    if isinstance(operation, PairwiseOp):
        return pairwise(iterator)
    if isinstance(operation, EnumerateOp):
        return enumerate(iterator, operation.start)
    if isinstance(operation, ZipOp):
        return _zip(iterator, operation)
    if isinstance(operation, ZipLongestOp):
        return _zip_longest(iterator, operation)
    if isinstance(operation, IntersperseOp):
        return _intersperse(iterator, operation)
    if isinstance(operation, ConcatOp):
        return _concat(iterator, operation)
    if isinstance(operation, CrossOp):
        return _cross(iterator, operation)
    if isinstance(operation, ScanOp):
        return _scan(iterator, operation)
    if isinstance(operation, ScanRightOp):
        return _scan_right(iterator, operation)
    if isinstance(operation, SortOp):
        if operation.buffer_size is None:
            return iter(sorted(iterator, key=operation.key, reverse=operation.reverse))
        return external_sort(iterator, operation)
    if isinstance(operation, GatherOp):
        return _gather(iterator, operation)
    if isinstance(operation, PrependOp):
        return _prepend(iterator, operation)
    if isinstance(operation, AppendOp):
        return _append(iterator, operation)
    if isinstance(operation, MapFirstOp):
        return _map_first(iterator, operation)
    if isinstance(operation, MapLastOp):
        return _map_last(iterator, operation)
    if isinstance(operation, CollapseOp):
        return _collapse(iterator, operation)
    raise TypeError(f"unsupported synchronous operation: {type(operation).__name__}")
