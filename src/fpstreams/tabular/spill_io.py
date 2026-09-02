"""Temporary partition file primitives shared by spilled tabular operations."""

from __future__ import annotations

import heapq
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from itertools import islice
from pathlib import Path
from typing import Any

from ..runtime.iterators import closing_iterators
from ..storage import SpillStore, SpillWriter

_PARTITION_BUFFER_BYTES = 4 * 1024 * 1024
_SPILL_HEADER_BYTES = len(b"FPSTRM\x00\x01")
_GROUP_COUNT_PARTIAL_TAG = "fpstreams.group-count.partial.v1"

GroupCountPartial = tuple[int, Any, str, int, int, int]


def group_count_partial(
    first_position: int,
    key: Any,
    count: int,
    virtual_rows: int,
    virtual_frame_bytes: int,
) -> GroupCountPartial:
    """Build one compact count state with its canonical raw spill accounting."""
    return (
        first_position,
        key,
        _GROUP_COUNT_PARTIAL_TAG,
        count,
        virtual_rows,
        virtual_frame_bytes,
    )


def group_count_partial_stats(value: Any) -> tuple[int, int] | None:
    """Return virtual row/frame counts for an internal count partial record."""
    if (
        type(value) is tuple
        and len(value) == 6
        and value[2] == _GROUP_COUNT_PARTIAL_TAG
        and type(value[4]) is int
        and type(value[5]) is int
    ):
        return value[4], value[5]
    return None


def partition(key: Any, count: int, *, operation: str, salt: int = 0) -> int:
    """Map a hashable key to a partition, optionally using a repartition salt."""
    try:
        hashed = hash(key) if salt == 0 else hash((salt, key))
    except TypeError:
        raise TypeError(f"{operation} keys must be hashable") from None
    return hashed % count


@dataclass(frozen=True, slots=True)
class PartitionFile:
    """Metadata for one temporary partition file."""

    path: Path
    rows: int
    bytes: int


class SpillPartitionWriters:
    """Write query-owned partitions without reserving long-lived descriptors.

    Rows are encoded when accepted, retained under one small global byte budget,
    and flushed one partition at a time. The single-writer policy lets a spilled
    operator safely consume another spilled operator upstream.
    """

    __slots__ = (
        "_buffer_rows",
        "_buffered_bytes",
        "_buffers",
        "_closed",
        "_operation",
        "_rows",
        "_store",
        "_virtual_frame_bytes",
        "paths",
    )

    def __init__(
        self,
        directory: Path,
        prefix: str,
        count: int,
        *,
        operation: str,
        store: SpillStore,
    ) -> None:
        """Prepare partition paths and per-file counters without opening any files."""
        self.paths = tuple(directory / f"{prefix}-{position}.bin" for position in range(count))
        self._operation = operation
        self._rows = [0] * count
        self._virtual_frame_bytes = [0] * count
        self._store = store
        self._closed = False
        self._buffers = [bytearray() for _position in range(count)]
        self._buffer_rows = [0] * count
        self._buffered_bytes = 0

    def dump(
        self,
        position: int,
        value: Any,
        *,
        virtual_rows: int = 1,
        virtual_frame_bytes: int | None = None,
    ) -> None:
        """Encode and buffer one record with optional canonical virtual accounting."""
        if self._closed:
            raise RuntimeError("partition writers are closed")
        try:
            frame = self._store._codec.encode_record(value)
        except TypeError as error:
            raise TypeError(f"{self._operation} spill data must be picklable") from error
        self.dump_encoded(
            position,
            frame,
            virtual_rows=virtual_rows,
            virtual_frame_bytes=virtual_frame_bytes,
        )

    def encode(self, value: Any) -> bytes:
        """Encode through the canonical spill boundary without buffering the frame."""
        if self._closed:
            raise RuntimeError("partition writers are closed")
        try:
            return self._store._codec.encode_record(value)
        except TypeError as error:
            raise TypeError(f"{self._operation} spill data must be picklable") from error

    def encoded_size(self, value: Any) -> int:
        """Measure a real canonical frame without retaining its physical bytes."""
        if self._closed:
            raise RuntimeError("partition writers are closed")
        try:
            return self._store._codec.encoded_size(value)
        except TypeError as error:
            raise TypeError(f"{self._operation} spill data must be picklable") from error

    def dump_encoded(
        self,
        position: int,
        frame: bytes,
        *,
        virtual_rows: int = 1,
        virtual_frame_bytes: int | None = None,
    ) -> None:
        """Buffer a canonical frame while tracking its raw logical size separately."""
        if self._closed:
            raise RuntimeError("partition writers are closed")
        if type(virtual_rows) is not int or virtual_rows < 1:
            raise ValueError("virtual_rows must be a positive integer")
        accounted_bytes = len(frame) if virtual_frame_bytes is None else virtual_frame_bytes
        if type(accounted_bytes) is not int or accounted_bytes < 1:
            raise ValueError("virtual_frame_bytes must be a positive integer")
        self._buffers[position].extend(frame)
        self._buffer_rows[position] += 1
        self._rows[position] += virtual_rows
        self._virtual_frame_bytes[position] += accounted_bytes
        self._buffered_bytes += len(frame)
        if self._buffered_bytes >= _PARTITION_BUFFER_BYTES:
            self._flush_buffers()

    def _flush_buffers(self) -> None:
        """Append every non-empty byte buffer while opening only one file at a time."""
        for position, buffered in enumerate(self._buffers):
            if not buffered:
                continue
            path = self.paths[position]
            writer = self._store.create_writer(
                generation=0,
                partition=position,
                path=path,
                append=path.exists(),
            )
            active_error: BaseException | None = None
            try:
                writer.write_encoded(bytes(buffered), rows=self._buffer_rows[position])
            except BaseException as error:
                active_error = error
                raise
            finally:
                _close_writer(writer, active_error)
            self._buffered_bytes -= len(buffered)
            buffered.clear()
            self._buffer_rows[position] = 0

    def close(self, active_error: BaseException | None = None) -> None:
        """Flush on success, close handles, and preserve an active pipeline error."""
        from ..runtime.resources import _add_cleanup_failure

        if self._closed:
            return
        self._closed = True
        errors: list[BaseException] = []
        if active_error is None:
            try:
                self._flush_buffers()
            except BaseException as error:
                errors.append(error)
        elif active_error is not None:
            # The query will delete its temporary directory; writing buffered rows
            # after a primary failure only creates a chance to mask that failure.
            for buffered in self._buffers:
                buffered.clear()
            self._buffer_rows[:] = [0] * len(self._buffer_rows)
            self._buffered_bytes = 0
        _add_cleanup_failure(active_error, errors)

    def files(self) -> tuple[PartitionFile, ...]:
        """Return row and byte metadata after all partition handles have closed."""
        if not self._closed or self._buffered_bytes:
            raise RuntimeError("partition writers must be closed before reading file statistics")
        return tuple(
            PartitionFile(
                path,
                rows,
                (_SPILL_HEADER_BYTES + frame_bytes if rows else 0),
            )
            for path, rows, frame_bytes in zip(
                self.paths,
                self._rows,
                self._virtual_frame_bytes,
                strict=True,
            )
        )


def _close_writer(writer: SpillWriter, active_error: BaseException | None) -> None:
    """Close one spill writer without replacing an earlier write failure."""
    from ..runtime.resources import _add_cleanup_failure

    if writer._handle is None:
        return
    try:
        writer.close()
    except BaseException as error:
        _add_cleanup_failure(active_error, [error])


class SpillLazyWriter:
    """Open a single spill output only when its first record is written."""

    __slots__ = ("_closed", "_handle", "_operation", "_path", "_store")

    def __init__(
        self,
        path: Path,
        *,
        operation: str,
        store: SpillStore,
    ) -> None:
        """Configure an output path that remains unopened until the first record."""
        self._path = path
        self._operation = operation
        self._store = store
        self._closed = False
        self._handle: SpillWriter | None = None

    def dump(self, value: Any) -> None:
        """Open the output lazily and serialize one record to it."""
        if self._closed:
            raise RuntimeError("spill output writer is closed")
        if self._handle is None:
            self._handle = self._store.create_writer(
                generation=0,
                partition=0,
                path=self._path,
            )
        try:
            self._handle.write(value)
        except TypeError as error:
            raise TypeError(f"{self._operation} spill data must be picklable") from error

    def close(self, active_error: BaseException | None = None) -> None:
        """Close the lazily opened output while preserving an active error."""
        from ..runtime.resources import _add_cleanup_failure

        if self._closed:
            return
        self._closed = True
        handle = self._handle
        self._handle = None
        if handle is None:
            return
        try:
            handle.close()
        except BaseException as error:
            _add_cleanup_failure(active_error, [error])


def read(path: Path, *, store: SpillStore) -> Iterator[Any]:
    """Read one framed partition through the query-owned spill store."""
    if not path.exists():
        return
    yield from store.read_path(path)


def _merge_ordered_records(
    paths: Iterable[Path],
    *,
    store: SpillStore,
) -> Iterator[tuple[Any, Any]]:
    """Merge a bounded set of ordered files and retain each record's ordering tag."""
    readers: list[Iterator[Any]] = []
    heap: list[tuple[Any, int, Any, Iterator[Any]]] = []
    active_error: BaseException | None = None
    try:
        for serial, path in enumerate(paths):
            reader = read(path, store=store)
            readers.append(reader)
            try:
                order, value = next(reader)
            except StopIteration:
                continue
            heapq.heappush(heap, (order, serial, value, reader))

        while heap:
            order, serial, value, reader = heapq.heappop(heap)
            yield order, value
            try:
                order, next_value = next(reader)
            except StopIteration:
                continue
            heapq.heappush(heap, (order, serial, next_value, reader))
    except BaseException as error:
        active_error = error
        raise
    finally:
        from ..runtime.resources import _add_cleanup_failure

        errors: list[BaseException] = []
        for reader in readers:
            close = getattr(reader, "close", None)
            if callable(close):
                try:
                    close()
                except BaseException as error:
                    errors.append(error)
        # ``generator.close()`` injects GeneratorExit, but callers cannot inspect
        # notes attached to that internal signal.  Treat early close as having no
        # primary failure so the first reader-close error remains observable.
        effective_active = None if isinstance(active_error, GeneratorExit) else active_error
        _add_cleanup_failure(effective_active, errors)


def _batched_paths(paths: list[Path], size: int) -> Iterator[list[Path]]:
    """Yield contiguous path groups so equal ordering tags keep file stability."""
    iterator = iter(paths)
    while batch := list(islice(iterator, size)):
        yield batch


def merge_ordered(
    paths: Iterable[Path],
    *,
    store: SpillStore,
) -> Iterator[Any]:
    """Merge ordered partitions under the active query's descriptor budget.

    Contiguous groups are collapsed when the final fan-in would exceed the
    remaining descriptor budget. Each collapse uses at most ``limit - 1`` readers
    plus one writer and deletes its inputs only after the replacement is complete.
    """
    current = [path for path in paths if path.exists()]
    if not current:
        return
    from ..runtime.failpoints import hit

    available = store._runtime.limits.max_open_files - store._runtime.metrics.open_files
    # A streaming consumer may need one descriptor while this merge is still
    # yielding. Keeping that slot free makes nested spill pipelines composable.
    final_fan_in = available - 1
    if final_fan_in < 1:
        raise RuntimeError("ordered spill merge requires max_open_files of at least 2")
    generation = 1
    while len(current) > final_fan_in:
        fan_in = available - 1
        if fan_in < 2:
            raise RuntimeError("ordered spill merge requires max_open_files of at least 3")
        replacement: list[Path] = []
        for serial, batch in enumerate(_batched_paths(current, fan_in)):
            if len(batch) == 1:
                replacement.extend(batch)
                continue
            run = store.write_run(
                generation,
                serial,
                _merge_ordered_records(batch, store=store),
            )
            replacement.append(run.path)
            for path in batch:
                hit("spill.unlink.before")
                path.unlink(missing_ok=True)
        current = replacement
        generation += 1

    records = _merge_ordered_records(current, store=store)
    with closing_iterators((records,)):
        for _order, value in records:
            yield value


def repartition(
    source: PartitionFile,
    directory: Path,
    prefix: str,
    count: int,
    *,
    operation: str,
    salt: int,
    store: SpillStore,
) -> tuple[PartitionFile, ...]:
    """Redistribute one oversized partition using a new hash salt."""
    writers = SpillPartitionWriters(
        directory,
        prefix,
        count,
        operation=operation,
        store=store,
    )
    values = read(source.path, store=store)
    active_error: BaseException | None = None
    try:
        for value in values:
            key = value[1]
            bucket = partition(key, count, operation=operation, salt=salt)
            accounting = group_count_partial_stats(value)
            if accounting is None:
                writers.dump(bucket, value)
            else:
                virtual_rows, virtual_frame_bytes = accounting
                writers.dump(
                    bucket,
                    value,
                    virtual_rows=virtual_rows,
                    virtual_frame_bytes=virtual_frame_bytes,
                )
    except BaseException as error:
        active_error = error
        raise
    finally:
        from ..runtime.resources import _add_cleanup_failure

        errors: list[BaseException] = []
        effective_active = None if isinstance(active_error, GeneratorExit) else active_error
        try:
            writers.close(active_error)
        except BaseException as error:
            errors.append(error)
        close = getattr(values, "close", None)
        if callable(close):
            try:
                close()
            except BaseException as error:
                errors.append(error)
        _add_cleanup_failure(effective_active, errors)
    return writers.files()
