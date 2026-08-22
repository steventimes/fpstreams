"""Query-scoped spill run ownership on top of the M3 runtime registry."""

from __future__ import annotations

import os
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from itertools import islice
from pathlib import Path
from typing import BinaryIO, cast

from ..runtime.query import QueryRuntime
from .codec import SpillCodec

# Small record objects reach pickle's best throughput before very large batches;
# 256 also caps the transient reference tuple at roughly 2 KiB on 64-bit CPython.
_SPILL_WRITE_BATCH_SIZE = 256


@dataclass(frozen=True, slots=True)
class SpillRun:
    """One closed, versioned run file and its immutable metadata."""

    path: Path
    generation: int
    partition: int
    rows: int
    bytes: int


@dataclass(frozen=True, slots=True)
class SpillGeneration:
    """A completed generation of run files safe to replace atomically."""

    number: int
    runs: tuple[SpillRun, ...]


class SpillWriter:
    """Append framed values to one query-owned run and close it exactly once."""

    def __init__(
        self,
        store: SpillStore,
        generation: int,
        partition: int,
        path: Path,
        *,
        append: bool = False,
    ) -> None:
        from ..runtime.failpoints import hit

        self._store = store
        self._generation = generation
        self._partition = partition
        self._path = path
        existing = append and path.exists() and path.stat().st_size > 0
        self._handle: BinaryIO | None = None
        try:
            self._handle = cast(
                BinaryIO,
                self._store._runtime.files.open(path, "ab" if append else "wb"),
            )
            hit("spill.open.after")
            if not existing:
                self._store._codec.write_header(self._handle)
        except BaseException as error:
            from ..runtime.resources import _add_cleanup_failure

            errors: list[BaseException] = []
            if self._handle is not None:
                try:
                    self._handle.close()
                except BaseException as close_error:
                    errors.append(close_error)
                self._handle = None
            _add_cleanup_failure(error, errors)
            raise
        self._rows = 0
        self._bytes = 0 if existing else len(b"FPSTRM\x00\x01")

    def write(self, value: object) -> None:
        """Write one serializable record while accounting its framed bytes."""
        from ..runtime.failpoints import hit

        if self._handle is None:
            raise RuntimeError("spill writer is closed")
        hit("spill.write.before")
        written = self._store._codec.write_record(self._handle, value)
        self._rows += 1
        self._bytes += written
        self._store._runtime.spills.record_write(written)

    def write_many(self, values: tuple[object, ...]) -> None:
        """Write a bounded logical batch with one serialization and frame operation.

        The tuple is capped by :func:`SpillStore.write_run`, so the optimization
        amortizes Python/pickle overhead without turning a bounded spill algorithm
        into a full-run materialization.  Public row and byte accounting remains in
        logical-record and physical-byte units respectively.
        """
        from ..runtime.failpoints import hit

        if self._handle is None:
            raise RuntimeError("spill writer is closed")
        if not values:
            return
        hit("spill.write.before")
        written = self._store._codec.write_records(self._handle, values)
        self._rows += len(values)
        self._bytes += written
        self._store._runtime.spills.record_write(written)

    def write_encoded(self, frame: bytes, *, rows: int = 1) -> None:
        """Append already validated framed bytes from a bounded partition buffer.

        Encoding happens when the source row is accepted, so a bad pickle cannot
        be delayed past later user callbacks. This method only performs the later
        I/O flush and updates the same logical-row/physical-byte accounting.
        """
        from ..runtime.failpoints import hit

        if self._handle is None:
            raise RuntimeError("spill writer is closed")
        if rows < 1:
            raise ValueError("encoded spill writes require at least one logical row")
        hit("spill.write.before")
        self._handle.write(frame)
        self._rows += rows
        self._bytes += len(frame)
        self._store._runtime.spills.record_write(len(frame))

    def close(self) -> SpillRun:
        """Flush, optionally fsync, close, and return immutable run metadata."""
        from ..runtime.resources import _add_cleanup_failure

        if self._handle is None:
            raise RuntimeError("spill writer is already closed")
        handle = self._handle
        self._handle = None
        active_error: BaseException | None = None
        try:
            handle.flush()
            if self._store._durable:
                os.fsync(handle.fileno())
        except BaseException as error:
            active_error = error
            raise
        finally:
            try:
                handle.close()
            except BaseException as error:
                _add_cleanup_failure(active_error, [error])
        return SpillRun(self._path, self._generation, self._partition, self._rows, self._bytes)


class SpillStore:
    """Create query-owned run files and expose deterministic framed read/write operations."""

    def __init__(
        self,
        runtime: QueryRuntime,
        *,
        parent: str | Path | None = None,
        operation: str,
        durable: bool = True,
    ) -> None:
        """Create a query-owned store, syncing closed runs when durability is requested."""
        from ..runtime.failpoints import hit

        self._runtime = runtime
        self._durable = durable
        self.operation = operation
        self.directory = runtime.spills.create_directory(parent)
        hit("spill.mkdir.after")
        self._codec = SpillCodec()
        self._generations: dict[int, SpillGeneration] = {}
        self._next = 0

    def create_writer(
        self,
        *,
        generation: int,
        partition: int,
        path: Path | None = None,
        append: bool = False,
    ) -> SpillWriter:
        """Create one owned run writer with deterministic generation/partition naming."""
        run_path = (
            self.runtime_path(generation, partition)
            if path is None
            else self._runtime.spills.register(path)
        )
        return SpillWriter(self, generation, partition, run_path, append=append)

    def runtime_path(self, generation: int, partition: int) -> Path:
        """Allocate a collision-free run path below this store's owned directory."""
        path = (
            self.directory
            / f"{self.operation}-g{generation:04d}-p{partition:04d}-{self._next:06d}.run"
        )
        self._next += 1
        return self._runtime.spills.register(path)

    def write_run(self, generation: int, partition: int, values: Iterable[object]) -> SpillRun:
        """Write one complete run and return it only after its writer is closed."""
        from ..runtime.resources import _add_cleanup_failure

        writer = self.create_writer(generation=generation, partition=partition)
        iterator = iter(values)
        active_error: BaseException | None = None
        try:
            while batch := tuple(islice(iterator, _SPILL_WRITE_BATCH_SIZE)):
                writer.write_many(batch)
            return writer.close()
        except BaseException as error:
            active_error = error
            raise
        finally:
            errors: list[BaseException] = []
            if writer._handle is not None:
                handle = writer._handle
                writer._handle = None
                try:
                    handle.close()
                except BaseException as error:
                    errors.append(error)
            # Merge generators own open run readers.  Closing eagerly on a failed
            # batch avoids waiting for reference-counting to release descriptors.
            close = getattr(iterator, "close", None)
            if callable(close):
                try:
                    close()
                except BaseException as error:
                    errors.append(error)
            _add_cleanup_failure(active_error, errors)

    def commit_generation(self, generation: SpillGeneration) -> None:
        """Record a completed generation after verifying every run belongs to this query store."""
        if generation.number in self._generations:
            raise ValueError(f"generation {generation.number} is already committed")
        for run in generation.runs:
            if run.generation != generation.number:
                raise ValueError("run generation does not match generation metadata")
            self._runtime.spills.register(run.path)
        self._generations[generation.number] = generation

    def replace_generation(self, previous: SpillGeneration, replacement: SpillGeneration) -> None:
        """Publish completed replacement runs before deleting the superseded owned generation."""
        from ..runtime.failpoints import hit

        if self._generations.get(previous.number) != previous:
            raise ValueError("previous generation is not the currently committed generation")
        if replacement.number == previous.number:
            raise ValueError("replacement generation must have a different number")
        self.commit_generation(replacement)
        try:
            hit("spill.generation.replace.before")
            for run in previous.runs:
                hit("spill.unlink.before")
                run.path.unlink(missing_ok=True)
        except BaseException:
            # A partially completed unlink cannot be rolled back.  Keep both
            # metadata records so the fully durable replacement remains
            # reachable and a later cleanup can still identify the old runs.
            raise
        self._generations.pop(previous.number, None)

    def read(self, run: SpillRun) -> Iterator[object]:
        """Lazily read a closed run and close its handle when iteration ends or fails."""
        yield from self.read_path(run.path)

    def read_path(self, path: Path) -> Iterator[object]:
        """Read a registered framed path through the query's tracked I/O boundary."""
        from ..runtime.failpoints import hit
        from ..runtime.resources import _add_cleanup_failure

        handle = self._runtime.files.open(path, "rb")
        active_error: BaseException | None = None
        try:
            hit("spill.read.before")
            yield from self._codec.read_records(cast(BinaryIO, handle))
        except BaseException as error:
            active_error = error
            raise
        finally:
            try:
                handle.close()
            except BaseException as error:
                # ``generator.close()`` injects GeneratorExit into this reader.
                # Let a close failure escape so the owning merge can attach it to
                # the real comparison/codec error that triggered reader cleanup.
                if isinstance(active_error, GeneratorExit):
                    raise
                _add_cleanup_failure(active_error, [error])
