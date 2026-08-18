"""Temporary partition file primitives shared by spilled tabular operations."""

from __future__ import annotations

import heapq
import pickle
from collections import OrderedDict
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO

_MAX_OPEN_WRITERS = 32


def dump(handle: BinaryIO, value: Any, *, operation: str) -> None:
    """Serialize one spill record and normalize pickle failures."""
    try:
        pickle.dump(value, handle, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception as error:
        raise TypeError(f"{operation} spill data must be picklable") from error


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


class PartitionWriters:
    """Write partitions while keeping only a bounded number of files open."""

    __slots__ = ("_handles", "_operation", "_rows", "paths")

    def __init__(self, directory: Path, prefix: str, count: int, *, operation: str) -> None:
        """Prepare partition paths and per-file counters without opening any files."""
        self.paths = tuple(directory / f"{prefix}-{position}.bin" for position in range(count))
        self._operation = operation
        self._rows = [0] * count
        self._handles: OrderedDict[int, BinaryIO] = OrderedDict()

    def dump(self, position: int, value: Any) -> None:
        """Append a record and evict the least-recently-used open handle if needed."""
        handle = self._handles.pop(position, None)
        if handle is None:
            if len(self._handles) >= _MAX_OPEN_WRITERS:
                _old_position, old_handle = self._handles.popitem(last=False)
                old_handle.close()
            handle = self.paths[position].open("ab")
        self._handles[position] = handle
        dump(handle, value, operation=self._operation)
        self._rows[position] += 1

    def close(self) -> None:
        """Close every cached partition handle in least-recently-used order."""
        while self._handles:
            _position, handle = self._handles.popitem(last=False)
            handle.close()

    def files(self) -> tuple[PartitionFile, ...]:
        """Return row and byte metadata after all partition handles have closed."""
        if self._handles:
            raise RuntimeError("partition writers must be closed before reading file statistics")
        return tuple(
            PartitionFile(path, rows, path.stat().st_size if path.exists() else 0)
            for path, rows in zip(self.paths, self._rows, strict=True)
        )


class LazyWriter:
    """Open a single spill output only when its first record is written."""

    __slots__ = ("_handle", "_operation", "_path")

    def __init__(self, path: Path, *, operation: str) -> None:
        """Configure an output path that remains unopened until the first record."""
        self._path = path
        self._operation = operation
        self._handle: BinaryIO | None = None

    def dump(self, value: Any) -> None:
        """Open the output lazily and serialize one record to it."""
        if self._handle is None:
            self._handle = self._path.open("wb")
        dump(self._handle, value, operation=self._operation)

    def close(self) -> None:
        """Close the lazily opened output handle when present."""
        if self._handle is not None:
            self._handle.close()
            self._handle = None


def read(path: Path) -> Iterator[Any]:
    """Yield pickled records from a partition and stop cleanly at EOF."""
    if not path.exists():
        return
    with path.open("rb") as handle:
        while True:
            try:
                yield pickle.load(handle)
            except EOFError:
                return


def merge_ordered(paths: Iterable[Path]) -> Iterator[Any]:
    """Perform a k-way merge of partitions whose records start with sort keys."""
    readers: list[Iterator[Any]] = []
    heap: list[tuple[Any, int, Any, Iterator[Any]]] = []
    try:
        for serial, path in enumerate(paths):
            reader = read(path)
            readers.append(reader)
            try:
                order, value = next(reader)
            except StopIteration:
                continue
            heapq.heappush(heap, (order, serial, value, reader))

        while heap:
            _order, serial, value, reader = heapq.heappop(heap)
            yield value
            try:
                order, next_value = next(reader)
            except StopIteration:
                continue
            heapq.heappush(heap, (order, serial, next_value, reader))
    finally:
        for reader in readers:
            close = getattr(reader, "close", None)
            if callable(close):
                close()


def repartition(
    source: PartitionFile,
    directory: Path,
    prefix: str,
    count: int,
    *,
    operation: str,
    salt: int,
) -> tuple[PartitionFile, ...]:
    """Redistribute one oversized partition using a new hash salt."""
    writers = PartitionWriters(directory, prefix, count, operation=operation)
    try:
        for value in read(source.path):
            key = value[1]
            bucket = partition(key, count, operation=operation, salt=salt)
            writers.dump(bucket, value)
    finally:
        writers.close()
    return writers.files()
