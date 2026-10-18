"""Persist verified outputs for stable, explicitly versioned file partitions."""

from __future__ import annotations

import errno
import hashlib
import json
import os
import sys
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from ..runtime.resources import _add_cleanup_failure


def _digest(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


@contextmanager
def _partition_lock(path: Path) -> Iterator[None]:
    """Use an OS lock that is released on process exit; keep the lock inode stable."""
    if path.is_symlink() or (path.exists() and not path.is_file()):
        raise ValueError("checkpoint lock must be a regular file")
    with path.open("a+b") as handle:
        try:
            if sys.platform == "win32":
                import msvcrt

                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            if error.errno in {errno.EACCES, errno.EAGAIN, errno.EDEADLK}:
                raise BlockingIOError(errno.EWOULDBLOCK, "checkpoint partition is busy") from error
            raise
        active_error: BaseException | None = None
        try:
            yield
        except BaseException as error:
            active_error = error
            raise
        finally:
            # Closing the descriptor also releases the OS lock after exceptions.
            try:
                if sys.platform == "win32":
                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(handle, fcntl.LOCK_UN)
            except BaseException as error:
                _add_cleanup_failure(active_error, [error])


class FileCheckpoint:
    """Reuse completed file partitions after verifying their input and output bytes.

    A callback receives the input path and a temporary output path. It must close
    one regular output file before returning. A successful run publishes that file
    and JSON metadata together by renaming their directory on the same filesystem.
    Failed runs publish nothing. Publication guarantees atomic visibility, not
    durability after power loss. The directory must be trusted local storage.

    Inputs must remain stable during execution. Use a new version whenever code,
    options, or external dependencies can change the result. Only the callback's
    file output is checkpointed; external side effects need their own idempotency.
    Concurrent runs of different partitions are independent. A duplicate running
    partition raises BlockingIOError instead of repeating its callback.

    Args:
        directory: Directory in which completed partitions and persistent lock files live.
        version: Nonempty processing-version token covering callback logic and options.
    """

    def __init__(self, directory: str | os.PathLike[str], *, version: str) -> None:
        if not isinstance(version, str):
            raise TypeError("version must be a string")
        if not version:
            raise ValueError("version must not be empty")
        self._directory = Path(directory).resolve()
        self._directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._version = version

    def _completed(self, entry: Path, expected: dict[str, Any]) -> Path:
        """Reject incomplete or modified entries without replaying their callbacks."""
        result = entry / "output"
        metadata = entry / "metadata.json"
        try:
            if (
                entry.is_symlink()
                or metadata.is_symlink()
                or result.is_symlink()
                or not result.is_file()
                or not metadata.is_file()
                or metadata.stat().st_size > len(json.dumps(expected).encode()) + 256
            ):
                raise ValueError("invalid checkpoint files")
            saved = json.loads(metadata.read_text(encoding="utf-8"))
            if not isinstance(saved, dict) or saved != {
                **expected,
                "output_sha256": _digest(result),
            }:
                raise ValueError("checkpoint metadata or output changed")
        except (OSError, ValueError, TypeError) as error:
            raise ValueError(f"invalid checkpoint: {entry}") from error
        return result

    def run(
        self,
        source: str | os.PathLike[str],
        function: Callable[[Path, Path], object],
    ) -> Path:
        """Return a verified output path, computing and publishing it only on a miss.

        Args:
            source: Stable regular input file; its resolved path and SHA256 identify a partition.
            function: Receives input and temporary output paths; its return value is ignored.

        Returns:
            The completed output file, ready to pass to another file-based stage.

        Raises:
            BlockingIOError: If this partition is already being computed.
            ValueError: If input changes, output is missing, or a completed entry is corrupt.
            OSError: If input or checkpoint storage cannot be read or written.
        """
        if not callable(function):
            raise TypeError("function must be callable")
        input_path = Path(source).resolve(strict=True)
        if not input_path.is_file():
            raise ValueError("checkpoint input must be a regular file")
        expected = {
            "format": 1,
            "source": str(input_path),
            "input_sha256": _digest(input_path),
            "version": self._version,
        }
        key = hashlib.sha256(json.dumps(expected, sort_keys=True).encode()).hexdigest()
        entry = self._directory / key
        if entry.exists() or entry.is_symlink():
            return self._completed(entry, expected)
        with _partition_lock(self._directory / f"{key}.lock"):
            if entry.exists() or entry.is_symlink():
                return self._completed(entry, expected)
            with TemporaryDirectory(prefix=".stage-", dir=self._directory) as name:
                staging = Path(name)
                output = staging / "output"
                function(input_path, output)
                if output.is_symlink() or not output.is_file():
                    raise ValueError("checkpoint callback must create a regular output file")
                if _digest(input_path) != expected["input_sha256"]:
                    raise ValueError("checkpoint input changed during processing")
                metadata = {**expected, "output_sha256": _digest(output)}
                (staging / "metadata.json").write_text(
                    json.dumps(metadata, sort_keys=True) + "\n", encoding="utf-8"
                )
                staging.rename(entry)
        return entry / "output"
