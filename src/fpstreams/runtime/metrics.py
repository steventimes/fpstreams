"""Mutable query-scoped counters with monotonic high-water marks."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class QueryMetrics:
    """Runtime accounting that is reset for each independently compiled query."""

    live_tasks: int = 0
    high_water_tasks: int = 0
    open_files: int = 0
    high_water_open_files: int = 0
    spill_bytes: int = 0
