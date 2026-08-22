"""Sort synchronous streams with bounded runs and temporary pickle files."""

from __future__ import annotations

import heapq
import pickle
from collections.abc import Generator, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from itertools import islice
from pathlib import Path
from typing import Any, SupportsIndex, cast

from ..planning.sync import SortOp
from ..runtime.query import QueryRuntime
from ..storage import SpillGeneration, SpillRun, SpillStore
from ..storage.codec import _CodecSerializationError


@dataclass(frozen=True, slots=True)
class SortRecord:
    """A cached sort key and global input position preserving stable ties."""

    key: object
    position: int
    value: object

    def __reduce_ex__(self, _protocol: SupportsIndex) -> tuple[object, tuple[object, ...]]:
        """Serialize constructor arguments without per-record dataclass reflection."""
        return type(self), (self.key, self.position, self.value)


@dataclass(frozen=True, slots=True)
class PositionRecord:
    """A spillable value plus its stable encounter position, without a cached key."""

    position: int
    value: object

    def __reduce_ex__(self, _protocol: SupportsIndex) -> tuple[object, tuple[object, ...]]:
        """Serialize constructor arguments without per-record dataclass reflection."""
        return type(self), (self.position, self.value)


def sort_records(records: list[SortRecord], *, reverse: bool) -> list[SortRecord]:
    """Sort one cached run in place with CPython's optimized stable Timsort.

    Timsort already preserves encounter order for equal keys in both directions.
    Keeping the key separate from ``position`` also means valid ordering objects only
    need ``__lt__``; sorting never adds an equality requirement.  Returning the same
    list lets the spill loop reuse one bounded buffer instead of allocating a second
    full run for a custom counting or radix pass.
    """
    records.sort(key=lambda record: cast(Any, record.key), reverse=reverse)
    return records


@dataclass(slots=True)
class _HeapEntry:
    """One merge cursor ordered with ``__lt__`` alone and a stable position tie.

    Python tuple comparison probes equality before ordering later fields.  That is an
    invalid extra requirement for sort keys, so the heap owns comparison explicitly.
    """

    record: SortRecord
    run_index: int
    reverse: bool

    def __lt__(self, other: _HeapEntry) -> bool:
        left = cast(Any, self.record.key)
        right = cast(Any, other.record.key)
        if self.reverse:
            if bool(right < left):
                return True
            if bool(left < right):
                return False
        else:
            if bool(left < right):
                return True
            if bool(right < left):
                return False
        if self.record.position != other.record.position:
            return self.record.position < other.record.position
        return self.run_index < other.run_index


@dataclass(slots=True)
class _IdentityHeapEntry:
    """One compact identity-sort cursor ordered without calling user equality.

    Run indices are a stable tie-break because every run covers a contiguous input
    range and compaction always groups runs in encounter order.  A global position
    therefore need not be serialized beside every value on the identity fast path.
    """

    value: object
    run_index: int
    reverse: bool

    def __lt__(self, other: _IdentityHeapEntry) -> bool:
        left = cast(Any, self.value)
        right = cast(Any, other.value)
        if self.reverse:
            if bool(right < left):
                return True
            if bool(left < right):
                return False
        else:
            if bool(left < right):
                return True
            if bool(right < left):
                return False
        return self.run_index < other.run_index


@dataclass(slots=True)
class _PositionHeapEntry:
    """One value-only merge cursor with a key cached for its current record."""

    record: PositionRecord
    run_index: int
    reverse: bool
    sort_key: object

    def __lt__(self, other: _PositionHeapEntry) -> bool:
        left = cast(Any, self.sort_key)
        right = cast(Any, other.sort_key)
        if self.reverse:
            if bool(right < left):
                return True
            if bool(left < right):
                return False
        else:
            if bool(left < right):
                return True
            if bool(right < left):
                return False
        if self.record.position != other.record.position:
            return self.record.position < other.record.position
        return self.run_index < other.run_index


def _close_iterator(iterator: Iterator[Any]) -> None:
    """Close an owned run iterator when it exposes a close method."""
    close = getattr(iterator, "close", None)
    if callable(close):
        close()


def _close_iterators(
    iterators: Iterable[Iterator[Any]], active_error: BaseException | None
) -> None:
    """Close every run reader and retain an active comparison or codec failure."""
    from ..runtime.resources import _add_cleanup_failure

    errors: list[BaseException] = []
    for iterator in iterators:
        try:
            _close_iterator(iterator)
        except BaseException as error:
            errors.append(error)
    # GeneratorExit is an internal close signal and its notes are not observable
    # by the caller. Propagate cleanup failures to the outer owner, which can then
    # attach them to the actual comparison, codec, or destination-write error.
    effective_active = None if isinstance(active_error, GeneratorExit) else active_error
    _add_cleanup_failure(effective_active, errors)


def _merge_sort_runs(
    store: SpillStore, runs: list[SpillRun], operation: SortOp
) -> Iterator[SortRecord]:
    """Lazily heap-merge sorted run files with the requested key and direction.

    Every run reader is closed if merging finishes, fails, or is stopped early.
    """
    from ..runtime.failpoints import hit

    readers = [store.read(run) for run in runs]
    heap: list[_HeapEntry] = []
    active_error: BaseException | None = None
    try:
        for index, reader in enumerate(readers):
            try:
                record = cast(SortRecord, next(reader))
            except StopIteration:
                continue
            heapq.heappush(heap, _HeapEntry(record, index, operation.reverse))
        while heap:
            entry = heapq.heappop(heap)
            index = entry.run_index
            record = entry.record
            yield record
            try:
                successor = cast(SortRecord, next(readers[index]))
            except StopIteration:
                continue
            hit("sort.merge.pull.after")
            heapq.heappush(heap, _HeapEntry(successor, index, operation.reverse))
    except BaseException as error:
        active_error = error
        raise
    finally:
        _close_iterators(readers, active_error)


def _merge_identity_runs(
    store: SpillStore, runs: list[SpillRun], *, reverse: bool
) -> Iterator[Any]:
    """Heap-merge raw identity values while preserving less-than-only semantics."""
    from ..runtime.failpoints import hit

    readers = [store.read(run) for run in runs]
    heap: list[_IdentityHeapEntry] = []
    active_error: BaseException | None = None
    try:
        for index, reader in enumerate(readers):
            try:
                value = next(reader)
            except StopIteration:
                continue
            heapq.heappush(heap, _IdentityHeapEntry(value, index, reverse))
        while heap:
            entry = heapq.heappop(heap)
            index = entry.run_index
            yield entry.value
            try:
                successor = next(readers[index])
            except StopIteration:
                continue
            hit("sort.merge.pull.after")
            heapq.heappush(heap, _IdentityHeapEntry(successor, index, reverse))
    except BaseException as error:
        active_error = error
        raise
    finally:
        _close_iterators(readers, active_error)


def _sort_position_records(
    records: list[PositionRecord], *, key: Any, reverse: bool
) -> list[PositionRecord]:
    """Sort one fallback run while retaining no key result beyond this in-memory sort."""
    records.sort(key=lambda record: cast(Any, key(record.value)), reverse=reverse)
    return records


def _merge_position_runs(
    store: SpillStore,
    runs: list[SpillRun],
    *,
    key: Any,
    reverse: bool,
) -> Iterator[PositionRecord]:
    """Heap-merge value-only runs, evaluating each current record key once.

    Key results live only for the at-most-fan-in heap cursors, so process-local keys
    never cross the pickle boundary and memory remains bounded.  Positions remain
    durable so stable ties do not depend on re-evaluated keys comparing equal.
    """
    from ..runtime.failpoints import hit

    readers = [store.read(run) for run in runs]
    heap: list[_PositionHeapEntry] = []
    active_error: BaseException | None = None
    try:
        for index, reader in enumerate(readers):
            try:
                record = cast(PositionRecord, next(reader))
            except StopIteration:
                continue
            heapq.heappush(
                heap,
                _PositionHeapEntry(record, index, reverse, key(record.value)),
            )
        while heap:
            entry = heapq.heappop(heap)
            index = entry.run_index
            record = entry.record
            yield record
            try:
                successor = cast(PositionRecord, next(readers[index]))
            except StopIteration:
                continue
            hit("sort.merge.pull.after")
            heapq.heappush(
                heap,
                _PositionHeapEntry(successor, index, reverse, key(successor.value)),
            )
    except BaseException as error:
        active_error = error
        raise
    finally:
        _close_iterators(readers, active_error)


def _collapse_sort_runs(
    store: SpillStore,
    runs: list[SpillRun],
    operation: SortOp,
    generation: int,
    fan_in: int,
) -> tuple[list[SpillRun], int]:
    """Merge run generations until the current query's legal fan-in remains.

    Each generation groups only the descriptor-safe number of inputs, then unlinks
    the prior generation before continuing. It never samples or reopens the source.
    """
    current = SpillGeneration(generation, tuple(runs))
    store.commit_generation(current)
    while len(current.runs) > fan_in:
        generation += 1
        merged = [
            store.write_run(generation, index, _merge_sort_runs(store, list(group), operation))
            for index, group in enumerate(
                current.runs[offset : offset + fan_in]
                for offset in range(0, len(current.runs), fan_in)
            )
        ]
        replacement = SpillGeneration(generation, tuple(merged))
        store.replace_generation(current, replacement)
        current = replacement
    return list(current.runs), generation


def _write_identity_run(
    store: SpillStore,
    generation: int,
    partition: int,
    values: Iterator[Any] | list[Any],
) -> SpillRun:
    """Write one raw-value run and translate only a proven codec failure."""
    try:
        return store.write_run(generation, partition, values)
    except _CodecSerializationError as error:
        raise TypeError(
            "external sort values must be picklable; "
            "use sorted() without buffer_size for process-local objects"
        ) from error


def _collapse_identity_runs(
    store: SpillStore,
    runs: list[SpillRun],
    *,
    reverse: bool,
    generation: int,
    fan_in: int,
) -> tuple[list[SpillRun], int]:
    """Compact raw-value generations without adding key/position payload fields."""
    current = SpillGeneration(generation, tuple(runs))
    store.commit_generation(current)
    while len(current.runs) > fan_in:
        generation += 1
        merged = [
            _write_identity_run(
                store,
                generation,
                index,
                _merge_identity_runs(store, list(group), reverse=reverse),
            )
            for index, group in enumerate(
                current.runs[offset : offset + fan_in]
                for offset in range(0, len(current.runs), fan_in)
            )
        ]
        replacement = SpillGeneration(generation, tuple(merged))
        store.replace_generation(current, replacement)
        current = replacement
    return list(current.runs), generation


def _collapse_position_runs(
    store: SpillStore,
    runs: list[SpillRun],
    *,
    key: Any,
    reverse: bool,
    generation: int,
    fan_in: int,
) -> tuple[list[SpillRun], int]:
    """Apply the descriptor-safe fan-in to value-only fallback runs."""
    current = SpillGeneration(generation, tuple(runs))
    store.commit_generation(current)
    while len(current.runs) > fan_in:
        generation += 1
        merged = [
            store.write_run(
                generation,
                index,
                _merge_position_runs(store, list(group), key=key, reverse=reverse),
            )
            for index, group in enumerate(
                current.runs[offset : offset + fan_in]
                for offset in range(0, len(current.runs), fan_in)
            )
        ]
        replacement = SpillGeneration(generation, tuple(merged))
        store.replace_generation(current, replacement)
        current = replacement
    return list(current.runs), generation


def _sort_merge_fan_in(runtime: QueryRuntime) -> int:
    """Reserve writer/output descriptors before giving merge readers their fan-in."""
    maximum = runtime.limits.max_open_files
    if maximum < 3:
        raise RuntimeError("external sort requires max_open_files of at least 3")
    reserve_files = min(2, maximum - 2)
    return min(32, maximum - reserve_files)


@contextmanager
def _sort_runtime(runtime: QueryRuntime | None) -> Generator[QueryRuntime, None, None]:
    """Reuse a caller runtime or own one for the exact generator lifetime.

    Physical execution supplies a query-wide runtime so file limits and metrics span
    every operator. Direct internal callers retain the historical self-contained
    behavior, including preserving a pipeline error over cleanup failures.
    """
    owned = runtime is None
    active = QueryRuntime() if runtime is None else runtime
    try:
        yield active
    except BaseException as error:
        if owned:
            active.close(error)
        raise
    else:
        if owned:
            active.close()


def _is_picklable(value: object) -> bool:
    """Check a value only after a real spill serialization failure occurred."""
    try:
        pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
    except (pickle.PicklingError, AttributeError, TypeError, ValueError):
        return False
    return True


def _can_fall_back_to_value_only_runs(failed: list[SortRecord], pending: SortRecord | None) -> bool:
    """Identify an actual cached-key serialization failure without normal-path probing."""
    values = [record.value for record in failed]
    if pending is not None:
        values.append(pending.value)
    return (
        bool(failed)
        and all(_is_picklable(value) for value in values)
        and any(not _is_picklable(record.key) for record in failed)
    )


def _rewrite_keyed_runs_as_position_runs(
    store: SpillStore, runs: list[SpillRun], generation: int
) -> list[SpillRun]:
    """Convert completed key-cached runs without reopening the source iterator."""
    from ..runtime.failpoints import hit

    rewritten: list[SpillRun] = []
    for partition, run in enumerate(runs):
        reader = store.read(run)
        active_error: BaseException | None = None
        try:
            rewritten.append(
                store.write_run(
                    generation,
                    partition,
                    (
                        PositionRecord(record.position, record.value)
                        for record in cast(Iterator[SortRecord], reader)
                    ),
                )
            )
        except BaseException as error:
            active_error = error
            raise
        finally:
            _close_iterators((reader,), active_error)
    for run in runs:
        hit("spill.unlink.before")
        run.path.unlink(missing_ok=True)
    return rewritten


def _continue_with_value_only_runs(
    iterator: Iterator[Any],
    operation: SortOp,
    *,
    key: Any,
    position: int,
    store: SpillStore,
    completed_keyed_runs: list[SpillRun],
    failed_buffer: list[SortRecord],
    pending: SortRecord | None,
    fan_in: int,
) -> Iterator[Any]:
    """Finish one consumed sort source using only position/value spill records.

    The normal path retains cached keys and therefore invokes the key exactly once
    per input.  This compatibility path begins only after a proved key-result
    serialization failure, rewrites completed runs without touching the source,
    and recomputes keys for every merge pass.
    """
    generation = (
        max(
            (*store._generations, *(run.generation for run in completed_keyed_runs)),
            default=0,
        )
        + 1
    )
    runs = _rewrite_keyed_runs_as_position_runs(store, completed_keyed_runs, generation)
    buffer = [PositionRecord(record.position, record.value) for record in failed_buffer]
    deferred = None if pending is None else PositionRecord(pending.position, pending.value)
    run_index = len(runs)

    def fill_buffer(limit: int) -> None:
        """Read later source values exactly once and assign their durable position."""
        nonlocal position
        while len(buffer) < limit:
            try:
                value = next(iterator)
            except StopIteration:
                return
            buffer.append(PositionRecord(position, value))
            position += 1

    while buffer:
        runs.append(
            store.write_run(
                generation,
                run_index,
                _sort_position_records(buffer, key=key, reverse=operation.reverse),
            )
        )
        buffer.clear()
        if deferred is not None:
            buffer.append(deferred)
            deferred = None
        fill_buffer(cast(int, operation.buffer_size))
        run_index += 1

    runs, _generation = _collapse_position_runs(
        store,
        runs,
        key=key,
        reverse=operation.reverse,
        generation=generation,
        fan_in=fan_in,
    )
    yield from (
        record.value
        for record in _merge_position_runs(store, runs, key=key, reverse=operation.reverse)
    )


def _recover_from_keyed_spill_failure(
    error: _CodecSerializationError,
    iterator: Iterator[Any],
    operation: SortOp,
    *,
    key: Any,
    position: int,
    store: SpillStore,
    completed_keyed_runs: list[SpillRun],
    failed_buffer: list[SortRecord],
    pending: SortRecord | None,
    fan_in: int,
) -> Iterator[Any]:
    """Resume only the proven cached-key failure with position/value spill records."""
    if _can_fall_back_to_value_only_runs(failed_buffer, pending):
        try:
            yield from _continue_with_value_only_runs(
                iterator,
                operation,
                key=key,
                position=position,
                store=store,
                completed_keyed_runs=completed_keyed_runs,
                failed_buffer=failed_buffer,
                pending=pending,
                fan_in=fan_in,
            )
            return
        except _CodecSerializationError as fallback_error:
            raise TypeError(
                "external sort values must be picklable; "
                "use sorted() without buffer_size for process-local objects"
            ) from fallback_error
    if not all(
        _is_picklable(record.value)
        for record in (*failed_buffer, *((pending,) if pending is not None else ()))
    ):
        raise TypeError(
            "external sort values must be picklable; "
            "use sorted() without buffer_size for process-local objects"
        ) from error
    raise TypeError(
        "external sort cached key results must be picklable; "
        "use sorted() without buffer_size for process-local objects"
    ) from error


def _external_identity_sort(
    iterator: Iterator[Any],
    operation: SortOp,
    raw_buffer: list[Any],
    carry_value: Any,
    runtime: QueryRuntime | None,
) -> Iterator[Any]:
    """Spill an identity sort as raw values with no duplicate cached-key payload.

    The initial runs and every compaction generation preserve contiguous source-run
    order.  Their run index is therefore sufficient for stable equal-key ties, while
    omitting ``SortRecord(value, position, value)`` cuts serialization work and disk
    volume substantially for the common ``key=None`` case.
    """
    from ..runtime.failpoints import hit

    buffer_size = cast(int, operation.buffer_size)
    parent = None if operation.tempdir is None else Path(operation.tempdir)
    with _sort_runtime(runtime) as active_runtime:
        fan_in = _sort_merge_fan_in(active_runtime)
        store = SpillStore(active_runtime, parent=parent, operation="sort", durable=False)
        runs: list[SpillRun] = []
        buffer = raw_buffer
        pending = carry_value
        has_pending = True
        run_index = 0
        while buffer:
            buffer.sort(reverse=operation.reverse)
            runs.append(_write_identity_run(store, 0, run_index, buffer))
            hit("sort.run.flush.after")
            buffer.clear()
            if has_pending:
                buffer.append(pending)
                has_pending = False
            buffer.extend(islice(iterator, buffer_size - len(buffer)))
            run_index += 1
        runs, _generation = _collapse_identity_runs(
            store,
            runs,
            reverse=operation.reverse,
            generation=0,
            fan_in=fan_in,
        )
        yield from _merge_identity_runs(store, runs, reverse=operation.reverse)


def external_sort(
    iterator: Iterator[Any],
    operation: SortOp,
    *,
    runtime: QueryRuntime | None = None,
) -> Iterator[Any]:
    """Sort an input with at most buffer_size values per initial in-memory run.

    Inputs that fit in the first buffer are sorted and yielded without pickling.
    Larger inputs are fully consumed into temporary sorted runs, reduced to at most
    32 files by merge passes, and lazily heap-merged. The temporary directory owns
    every run file and is removed when iteration ends; the enclosing executor
    remains responsible for closing the source iterator.
    """
    from ..runtime.failpoints import hit

    buffer_size = operation.buffer_size
    if buffer_size is None:
        raise RuntimeError("external sort requires a buffer size")

    raw_buffer = list(islice(iterator, buffer_size))
    if not raw_buffer:
        return
    try:
        carry_value = next(iterator)
    except StopIteration:
        # Match built-in ``sorted`` for the common no-spill case.  Deferring key
        # decoration until the lookahead proves spilling necessary avoids one
        # SortRecord allocation per value and keeps this path to a single list.
        raw_buffer.sort(key=cast(Any, operation.key), reverse=operation.reverse)
        yield from raw_buffer
        return

    if operation.key is None:
        yield from _external_identity_sort(
            iterator,
            operation,
            raw_buffer,
            carry_value,
            runtime,
        )
        return

    key = operation.key
    position = 0

    def make_record(value: Any) -> SortRecord:
        """Evaluate one key and claim its unique global encounter position."""
        nonlocal position
        record = SortRecord(key(value), position, value)
        position += 1
        return record

    def fill_buffer(buffer: list[SortRecord], limit: int) -> None:
        """Append up to ``limit`` records while advancing positions before return."""
        while len(buffer) < limit:
            try:
                value = next(iterator)
            except StopIteration:
                return
            buffer.append(make_record(value))

    # Reuse the raw list as the first decorated run once spill is unavoidable.
    # Replacing entries in place keeps peak live storage to B records plus the
    # one lookahead value instead of retaining parallel raw and decorated lists.
    for index, value in enumerate(raw_buffer):
        raw_buffer[index] = make_record(value)
    first = cast(list[SortRecord], raw_buffer)
    carry = make_record(carry_value)

    parent = None if operation.tempdir is None else Path(operation.tempdir)
    # The context begins before store construction so failpoints during directory
    # registration still release the newly-owned path.  It also passes an active
    # pipeline error to cleanup, preserving that error and attaching close failures.
    with _sort_runtime(runtime) as active_runtime:
        fan_in = _sort_merge_fan_in(active_runtime)
        # Runs are query-temporary. They need flush/close visibility for merging,
        # not crash durability across process failure.
        store = SpillStore(active_runtime, parent=parent, operation="sort", durable=False)
        runs: list[SpillRun] = []
        buffer = first
        run_index = 0
        pending: SortRecord | None = carry
        try:
            while buffer:
                runs.append(
                    store.write_run(0, run_index, sort_records(buffer, reverse=operation.reverse))
                )
                hit("sort.run.flush.after")
                buffer.clear()
                if pending is not None:
                    buffer.append(pending)
                    pending = None
                fill_buffer(buffer, buffer_size)
                run_index += 1
            runs, _generation = _collapse_sort_runs(store, runs, operation, 0, fan_in)
            yield from (record.value for record in _merge_sort_runs(store, runs, operation))
        except _CodecSerializationError as error:
            yield from _recover_from_keyed_spill_failure(
                error,
                iterator,
                operation,
                key=key,
                position=position,
                store=store,
                completed_keyed_runs=runs,
                failed_buffer=buffer,
                pending=pending,
                fan_in=fan_in,
            )
