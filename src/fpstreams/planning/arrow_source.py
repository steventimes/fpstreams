"""Describe lazily opened Arrow batches without importing optional PyArrow while planning."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any, Literal


@dataclass(frozen=True, slots=True)
class ArrowBatchSource:
    """Record a batch opener, source kind, sizing/schema hints, and replay metadata."""

    opener: Callable[[], Iterator[Any]]
    kind: Literal["table", "record_batch", "reader", "parquet"]
    batch_size: int
    schema_hint: Any | None = None
    reiterable: bool = True

    def open_batches(self) -> Iterator[Any]:
        """Create an iterator over Arrow tables or record batches with the configured opener."""
        return self.opener()


def batch_to_rows(batch: Any) -> list[dict[str, Any]]:
    """Materialize an Arrow batch as independent Python row dictionaries."""
    return [dict(row) for row in batch.to_pylist()]
