"""Bounded structural template cache that never retains source data."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from threading import RLock
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class PlanCacheKey:
    """Source-free identity of a cacheable physical plan template."""

    logical_fingerprint: str
    terminal: str
    capabilities: tuple[bool, int | None, bool]


@dataclass(frozen=True, slots=True)
class PhysicalPlanTemplate(Generic[T]):
    """A source-free immutable payload plus cache diagnostic metadata."""

    payload: T
    cacheable: bool = True


class PlanCache(Generic[T]):
    """Thread-safe bounded LRU cache for source-free physical-plan templates."""

    __slots__ = ("_entries", "_lock", "_max_entries")

    def __init__(self, max_entries: int = 128) -> None:
        if max_entries <= 0:
            raise ValueError("max_entries must be positive")
        self._max_entries = max_entries
        self._entries: OrderedDict[PlanCacheKey, PhysicalPlanTemplate[T]] = OrderedDict()
        self._lock = RLock()

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)

    def get(self, key: PlanCacheKey) -> PhysicalPlanTemplate[T] | None:
        """Retrieve a template and update its LRU position."""
        with self._lock:
            try:
                template = self._entries.pop(key)
            except KeyError:
                return None
            self._entries[key] = template
            return template

    def put(self, key: PlanCacheKey, template: PhysicalPlanTemplate[T]) -> None:
        """Store a cacheable template, evicting only the oldest descriptor if necessary."""
        if not template.cacheable:
            return
        with self._lock:
            self._entries.pop(key, None)
            self._entries[key] = template
            if len(self._entries) > self._max_entries:
                self._entries.popitem(last=False)
