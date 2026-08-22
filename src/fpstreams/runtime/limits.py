"""Hard limits enforced by query-owned runtime components."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class QueryLimits:
    """Limits enforced before the runtime acquires tasks or file descriptors."""

    max_tasks: int = 1024
    max_open_files: int = 64

    def __post_init__(self) -> None:
        if self.max_tasks < 1:
            raise ValueError("max_tasks must be at least 1")
        if self.max_open_files < 1:
            raise ValueError("max_open_files must be at least 1")
