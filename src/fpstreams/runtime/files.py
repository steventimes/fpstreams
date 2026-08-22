"""Query-scoped binary file leases with an enforced descriptor budget."""

from __future__ import annotations

import io
from pathlib import Path
from threading import RLock
from typing import Any, BinaryIO, Literal

from .limits import QueryLimits
from .metrics import QueryMetrics
from .resources import ResourceRegistry

BinaryMode = Literal["rb", "wb", "ab"]


class FileLimitError(RuntimeError):
    """Raised before a query would exceed its configured open-file budget."""


class TrackedBinaryFile(io.BufferedIOBase):
    """Proxy one binary handle and return its lease exactly once on close.

    The resource registry owns this proxy, so abandoning an iterator cannot leak
    the underlying descriptor.  Explicit ``close()`` releases it early and makes
    capacity immediately available to later spill readers or writers.
    """

    __slots__ = ("_manager", "_raw", "_released", "_resources")

    def __init__(
        self,
        raw: BinaryIO,
        manager: FileManager,
        resources: ResourceRegistry,
    ) -> None:
        self._raw = raw
        self._manager = manager
        self._resources = resources
        self._released = False

    @property
    def closed(self) -> bool:
        """Report the underlying handle state rather than BufferedIOBase state."""
        return self._released or self._raw.closed

    def close(self) -> None:
        """Release this exact lease; repeated closes are harmless."""
        if not self._released:
            # An ordinary operator close is not query-cleanup. Keeping that
            # failpoint boundary distinct lets primary pipeline failures remain
            # primary when the eventual runtime cleanup is independently faulty.
            self._resources.release(self, cleanup_boundary=False)

    def _close_owned(self) -> None:
        """Close the raw handle and decrement accounting even if close fails."""
        if self._released:
            return
        self._released = True
        try:
            self._raw.close()
        finally:
            self._manager._release()

    def readable(self) -> bool:
        return self._raw.readable()

    def writable(self) -> bool:
        return self._raw.writable()

    def seekable(self) -> bool:
        return self._raw.seekable()

    def read(self, size: int | None = -1) -> bytes:
        return self._raw.read(-1 if size is None else size)

    def readline(self, size: int | None = -1) -> bytes:
        return self._raw.readline(-1 if size is None else size)

    def write(self, value: Any) -> int:
        return self._raw.write(value)

    def seek(self, offset: int, whence: int = 0) -> int:
        return self._raw.seek(offset, whence)

    def tell(self) -> int:
        return self._raw.tell()

    def flush(self) -> None:
        self._raw.flush()

    def fileno(self) -> int:
        return self._raw.fileno()


class FileManager:
    """Acquire binary files under one query's hard descriptor limit."""

    def __init__(
        self,
        limits: QueryLimits,
        metrics: QueryMetrics,
        resources: ResourceRegistry,
    ) -> None:
        self._limits = limits
        self._metrics = metrics
        self._resources = resources
        self._closed = False
        self._lock = RLock()

    def open(self, path: str | Path, mode: BinaryMode) -> TrackedBinaryFile:
        """Open and own one binary handle, rejecting before the limit is crossed."""
        with self._lock:
            if self._closed:
                raise RuntimeError("query file manager is closed")
            if self._metrics.open_files >= self._limits.max_open_files:
                raise FileLimitError(
                    "open file limit exceeded: "
                    f"current={self._metrics.open_files}, limit={self._limits.max_open_files}"
                )
            # Deliberately outlives this method; QueryRuntime owns the returned proxy.
            raw: BinaryIO = Path(path).open(mode)  # noqa: SIM115
            self._metrics.open_files += 1
            self._metrics.high_water_open_files = max(
                self._metrics.high_water_open_files,
                self._metrics.open_files,
            )
            tracked = TrackedBinaryFile(raw, self, self._resources)
            try:
                return self._resources.own(tracked, lambda value: value._close_owned())
            except BaseException as error:
                # Registration failpoints happen after the record is appended. A
                # closed-registry race may happen before it is appended. Handle
                # both cases and make the raw descriptor rollback idempotent.
                self._resources.release(tracked, error, cleanup_boundary=False)
                tracked._close_owned()
                raise

    def close(self) -> None:
        """Prevent new leases before the runtime starts closing existing owners."""
        with self._lock:
            self._closed = True

    def _release(self) -> None:
        """Return one previously acquired descriptor slot."""
        with self._lock:
            self._metrics.open_files -= 1
