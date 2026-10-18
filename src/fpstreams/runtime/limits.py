"""Hard limits enforced by query-owned runtime components."""

from __future__ import annotations

from dataclasses import dataclass
from operator import index


@dataclass(frozen=True, slots=True)
class QueryLimits:
    """Limits enforced before the runtime acquires tasks or file descriptors."""

    max_tasks: int = 1024
    max_open_files: int = 64

    def __post_init__(self) -> None:
        max_tasks = index(self.max_tasks)
        if max_tasks < 1:
            raise ValueError("max_tasks must be at least 1")
        max_open_files = index(self.max_open_files)
        if max_open_files < 1:
            raise ValueError("max_open_files must be at least 1")
        object.__setattr__(self, "max_tasks", max_tasks)
        object.__setattr__(self, "max_open_files", max_open_files)
