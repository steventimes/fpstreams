"""Sort synchronous streams with bounded runs and temporary pickle files."""

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
    """Close an owned run iterator when it exposes a close method."""
    close = getattr(iterator, "close", None)
    if callable(close):
        close()


def _read_sort_run(path: Path) -> Iterator[Any]:
    """Yield the consecutive pickle records in one run file until clean EOF."""
    with path.open("rb") as handle:
        while True:
            try:
                yield pickle.load(handle)
            except EOFError:
                return


def _write_sort_run(path: Path, values: Iterator[Any]) -> None:
    """Serialize one sorted run and close its input iterator on every exit.

    Common pickling failures are translated to the external-sort API error that
    recommends in-memory sorting for process-local objects.
    """
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
    """Lazily heap-merge sorted run files with the requested key and direction.

    Every run reader is closed if merging finishes, fails, or is stopped early.
    """
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
    """Merge run generations until at most the configured fan-in remain.

    Each generation groups up to 32 inputs into a new run, then unlinks the prior
    generation before continuing.
    """
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
    """Sort an input with at most buffer_size values per initial in-memory run.

    Inputs that fit in the first buffer are sorted and yielded without pickling.
    Larger inputs are fully consumed into temporary sorted runs, reduced to at most
    32 files by merge passes, and lazily heap-merged. The temporary directory owns
    every run file and is removed when iteration ends; the enclosing executor
    remains responsible for closing the source iterator.
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
