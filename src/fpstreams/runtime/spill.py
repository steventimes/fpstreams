"""Query-owned spill paths that never remove caller-owned parent directories."""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

from .metrics import QueryMetrics
from .resources import ResourceRegistry


class SpillFileRegistry:
    """Create temporary query directories and validate registered child paths."""

    def __init__(self, resources: ResourceRegistry, metrics: QueryMetrics) -> None:
        self._resources = resources
        self._metrics = metrics
        self._directories: list[Path] = []

    def create_directory(self, parent: str | Path | None = None) -> Path:
        """Create and own a fresh spill directory below an optional caller parent."""
        if self._resources.closed:
            raise RuntimeError("query resource registry is closed")
        directory = Path(tempfile.mkdtemp(prefix="fpstreams-", dir=parent))
        try:
            self._resources.own(directory, lambda value: shutil.rmtree(value, ignore_errors=True))
        except BaseException:
            # The registry may close between the preflight check and registration.
            shutil.rmtree(directory, ignore_errors=True)
            raise
        self._directories.append(directory)
        return directory

    def register(self, path: str | Path) -> Path:
        """Validate that a spill file is below a directory this query created."""
        candidate = Path(path)
        resolved = candidate.resolve(strict=False)
        if not any(resolved.is_relative_to(directory.resolve()) for directory in self._directories):
            raise ValueError("spill path must be below a query-owned spill directory")
        return candidate

    def record_write(self, size: int) -> None:
        """Record bytes written by an opted-in spill implementation."""
        if type(size) is not int or size < 0:
            raise TypeError("spill write size must be a non-negative int")
        self._metrics.spill_bytes += size
