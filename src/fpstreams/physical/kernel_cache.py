"""Thread-safe bounded LRU cache for compiled backend metadata."""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable
from threading import RLock
from typing import TypeVar, cast

from .compiled import ProgramFingerprint

T = TypeVar("T")


class KernelCache:
    """Cache compiled kernel descriptors by structural fingerprint, never source data."""

    __slots__ = ("_entries", "_lock", "_max_entries")

    def __init__(self, max_entries: int = 128) -> None:
        """Create a bounded cache with positive capacity."""
        if max_entries <= 0:
            raise ValueError("max_entries must be positive")
        self._max_entries = max_entries
        self._entries: OrderedDict[ProgramFingerprint, object] = OrderedDict()
        self._lock = RLock()

    def __len__(self) -> int:
        """Return the current number of cached compiled descriptors."""
        with self._lock:
            return len(self._entries)

    def get_or_compile(self, fingerprint: ProgramFingerprint, compiler: Callable[[], T]) -> T:
        """Return a cached descriptor or compile it once while holding the cache lock."""
        with self._lock:
            try:
                value = self._entries.pop(fingerprint)
            except KeyError:
                value = compiler()
                self._entries[fingerprint] = value
                if len(self._entries) > self._max_entries:
                    self._entries.popitem(last=False)
                return value
            self._entries[fingerprint] = value
            return cast(T, value)
