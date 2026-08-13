"""Bounded-memory external sorting for synchronous streams."""

from __future__ import annotations

import heapq
import pickle
import tempfile
from collections.abc import Iterator
from itertools import islice
from pathlib import Path
from typing import Any

from ..planning.sync import SortOp

_EXTERNAL_SORT_MERGE_FAN_IN = 32


def _close_iterator(iterator: Iterator[Any]) -> None:
    close = getattr(iterator, "close", None)
    if callable(close):
        close()


def _read_sort_run(path: Path) -> Iterator[Any]:
    with path.open("rb") as handle:
        while True:
            try:
                yield pickle.load(handle)
            except EOFError:
                return


def _write_sort_run(path: Path, values: Iterator[Any]) -> None:
    try:
        with path.open("wb") as handle:
            for value in values:
                try:
                    pickle.dump(value, handle, protocol=pickle.HIGHEST_PROTOCOL)
                except (pickle.PickleError, AttributeError, TypeError) as error:
                    raise TypeError(
                        "external sort values must be picklable; "
                        "use sorted() without buffer_size for process-local objects"
                    ) from error
    finally:
        _close_iterator(values)


def _merge_sort_runs(paths: list[Path], operation: SortOp) -> Iterator[Any]:
    readers = [_read_sort_run(path) for path in paths]
    try:
        yield from heapq.merge(
            *readers,
            key=operation.key,
            reverse=operation.reverse,
        )
    finally:
        for reader in readers:
            _close_iterator(reader)


def _collapse_sort_runs(paths: list[Path], directory: Path, operation: SortOp) -> list[Path]:
    generation = 0
    while len(paths) > _EXTERNAL_SORT_MERGE_FAN_IN:
        merged_paths: list[Path] = []
        for offset in range(0, len(paths), _EXTERNAL_SORT_MERGE_FAN_IN):
            group = paths[offset : offset + _EXTERNAL_SORT_MERGE_FAN_IN]
            destination = directory / f"merge-{generation}-{len(merged_paths)}.bin"
            _write_sort_run(destination, _merge_sort_runs(group, operation))
            merged_paths.append(destination)
        for path in paths:
            path.unlink()
        paths = merged_paths
        generation += 1
    return paths


def external_sort(iterator: Iterator[Any], operation: SortOp) -> Iterator[Any]:
    """Sort values using bounded in-memory runs and temporary files.

    Args:
        iterator: The iterator consumed by the execution operation.
        operation: The planned operation appended to the pipeline.

    Returns:
        An iterator that produces values as they are requested.
    """
    buffer_size = operation.buffer_size
    if buffer_size is None:
        raise RuntimeError("external sort requires a buffer size")

    first = list(islice(iterator, buffer_size))
    if not first:
        return
    try:
        carry = next(iterator)
    except StopIteration:
        first.sort(key=operation.key, reverse=operation.reverse)
        yield from first
        return

    with tempfile.TemporaryDirectory(
        prefix="fpstreams-sort-", dir=operation.tempdir
    ) as temporary_directory:
        directory = Path(temporary_directory)
        paths: list[Path] = []

        first.sort(key=operation.key, reverse=operation.reverse)
        first_path = directory / "run-0.bin"
        _write_sort_run(first_path, iter(first))
        paths.append(first_path)

        buffer = [carry, *islice(iterator, buffer_size - 1)]
        while buffer:
            buffer.sort(key=operation.key, reverse=operation.reverse)
            path = directory / f"run-{len(paths)}.bin"
            _write_sort_run(path, iter(buffer))
            paths.append(path)
            buffer = list(islice(iterator, buffer_size))

        paths = _collapse_sort_runs(paths, directory, operation)
        yield from _merge_sort_runs(paths, operation)
