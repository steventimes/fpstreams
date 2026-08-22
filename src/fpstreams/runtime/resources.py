"""Explicit query-scoped synchronous and asynchronous resource ownership."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from threading import RLock
from typing import TypeVar, cast

T = TypeVar("T")


@dataclass(slots=True)
class _ResourceRecord:
    """A closer selected at acquisition time, never guessed again during cleanup."""

    resource: object
    close: Callable[[object], None] | None
    aclose: Callable[[object], Awaitable[None]] | None
    released: bool = False


def _add_cleanup_failure(active: BaseException | None, errors: list[BaseException]) -> None:
    """Preserve the active error and attach cleanup failures in encounter order."""
    if not errors:
        return
    if active is not None:
        for error in errors:
            _append_cleanup_failure(active, error)
        return
    primary = errors[0]
    for error in errors[1:]:
        _append_cleanup_failure(primary, error)
    raise primary


def _append_cleanup_failure(target: BaseException, error: BaseException) -> None:
    """Add one cleanup summary and preserve diagnostics nested on that failure."""
    nested_notes = tuple(getattr(error, "__notes__", ()) or ())
    target.add_note(f"cleanup failed: {type(error).__name__}: {error}")
    if target is not error:
        for note in nested_notes:
            target.add_note(note)


class ResourceRegistry:
    """Own resources for one query and clean them up LIFO with idempotent release."""

    def __init__(self) -> None:
        self._records: list[_ResourceRecord] = []
        self._closed = False
        self._lock = RLock()

    @property
    def closed(self) -> bool:
        """Report whether this registry has begun its one-way cleanup transition."""
        with self._lock:
            return self._closed

    def own(
        self,
        resource: T,
        close: Callable[[T], None] | None = None,
    ) -> T:
        """Register a synchronous closer or the resource's callable ``close`` method."""
        from .failpoints import hit

        closer = cast(Callable[[object], None] | None, close)
        if closer is None:
            candidate = getattr(resource, "close", None)
            if callable(candidate):
                closer = cast(Callable[[object], None], lambda value: candidate())
        if closer is None:
            raise TypeError("resource requires an explicit close callback or close() method")
        with self._lock:
            if self._closed:
                raise RuntimeError("query resource registry is closed")
            self._records.append(_ResourceRecord(resource, closer, None))
        hit("resource.register.after")
        return resource

    async def aown(
        self,
        resource: T,
        close: Callable[[T], Awaitable[None]] | None = None,
    ) -> T:
        """Register an asynchronous closer or the resource's callable ``aclose`` method."""
        closer = cast(Callable[[object], Awaitable[None]] | None, close)
        if closer is None:
            candidate = getattr(resource, "aclose", None)
            if callable(candidate):
                closer = cast(Callable[[object], Awaitable[None]], lambda value: candidate())
        if closer is None:
            raise TypeError("resource requires an explicit async close callback or aclose() method")
        with self._lock:
            if self._closed:
                raise RuntimeError("query resource registry is closed")
            self._records.append(_ResourceRecord(resource, None, closer))
        return resource

    def release(
        self,
        resource: object,
        active_error: BaseException | None = None,
        *,
        cleanup_boundary: bool = True,
    ) -> None:
        """Close one exact resource identity early, preserving an active error."""
        selected = self._take_record(resource)
        if selected is not None:
            self._close_record(selected, active_error, cleanup_boundary=cleanup_boundary)

    async def arelease(
        self,
        resource: object,
        active_error: BaseException | None = None,
        *,
        cleanup_boundary: bool = True,
    ) -> None:
        """Asynchronously close and forget one exact owner, preserving an active error."""
        selected = self._take_record(resource)
        if selected is not None:
            await self._aclose_record(
                selected,
                active_error,
                cleanup_boundary=cleanup_boundary,
            )

    def _take_record(self, resource: object) -> _ResourceRecord | None:
        """Atomically transfer one registered identity to an early-release caller."""
        with self._lock:
            for index in range(len(self._records) - 1, -1, -1):
                record = self._records[index]
                if record.resource is resource:
                    if record.released:
                        return None
                    record.released = True
                    # Explicitly released files are the hot path and normally sit
                    # at the tail, making this an O(1) pop. Removing the record is
                    # also essential for async inner streams: a long query must
                    # not retain every completed iterator until runtime cleanup.
                    return self._records.pop(index)
        return None

    def _close_record(
        self,
        record: _ResourceRecord,
        active: BaseException | None,
        *,
        cleanup_boundary: bool,
    ) -> None:
        """Release one record once and attach cleanup failure to any active exception."""
        if record.close is None:
            return
        try:
            if cleanup_boundary:
                from .failpoints import hit

                hit("resource.close.before")
            record.close(record.resource)
        except BaseException as error:
            _add_cleanup_failure(active, [error])

    async def _aclose_record(
        self,
        record: _ResourceRecord,
        active: BaseException | None,
        *,
        cleanup_boundary: bool,
    ) -> None:
        """Release one async-capable record once, including synchronous fallbacks."""
        try:
            if cleanup_boundary:
                from .failpoints import hit

                hit("resource.close.before")
            if record.aclose is not None:
                await record.aclose(record.resource)
            elif record.close is not None:
                record.close(record.resource)
        except BaseException as error:
            _add_cleanup_failure(active, [error])

    def close(self, active_error: BaseException | None = None) -> None:
        """Synchronously close every synchronous owner in LIFO order."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            records: list[_ResourceRecord] = []
            for record in reversed(self._records):
                if record.released:
                    continue
                record.released = True
                if record.close is not None:
                    records.append(record)
            self._records.clear()
        errors: list[BaseException] = []
        for record in records:
            try:
                from .failpoints import hit

                hit("resource.close.before")
                closer = record.close
                if closer is None:
                    raise RuntimeError("synchronous resource lost its registered closer")
                closer(record.resource)
            except BaseException as error:
                errors.append(error)
        _add_cleanup_failure(active_error, errors)

    async def aclose(self, active_error: BaseException | None = None) -> None:
        """Close every async or sync owner in LIFO order."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            records: list[_ResourceRecord] = []
            for record in reversed(self._records):
                if record.released:
                    continue
                record.released = True
                records.append(record)
            self._records.clear()
        errors: list[BaseException] = []
        for record in records:
            try:
                from .failpoints import hit

                hit("resource.close.before")
                if record.aclose is not None:
                    await record.aclose(record.resource)
                elif record.close is not None:
                    record.close(record.resource)
            except BaseException as error:
                errors.append(error)
        _add_cleanup_failure(active_error, errors)
