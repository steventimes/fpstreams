"""Describe lazily opened Arrow batches without importing optional PyArrow while planning."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any, Literal, TypeAlias, cast

RangePredicate: TypeAlias = tuple[str, Literal["<", "<=", ">", ">="], int]


@dataclass(frozen=True, slots=True)
class ArrowScanRequest:
    """Carry optional source-level projection and exact comparison scan hints."""

    columns: tuple[str, ...] | None = None
    equality: tuple[str, object] | None = None
    first_only: bool = False
    range_predicate: RangePredicate | None = None


@dataclass(frozen=True, slots=True)
class ArrowBatchSource:
    """Record a batch opener, source kind, sizing/schema hints, and replay metadata."""

    opener: Callable[[], Iterator[Any]]
    kind: Literal["table", "record_batch", "reader", "csv", "parquet", "dataframe", "polars"]
    batch_size: int
    schema_hint: Any | None = None
    reiterable: bool = True
    projection_opener: Callable[[tuple[str, ...]], Iterator[Any]] | None = None
    request_opener: Callable[[ArrowScanRequest], Iterator[Any]] | None = None
    materialized_data: Any | None = None
    columnar_opener: Callable[[], Any] | None = None
    count_opener: Callable[[], int | None] | None = None
    byte_size_opener: Callable[[], int | None] | None = None

    def open_batches(
        self,
        *,
        columns: tuple[str, ...] | None = None,
        equality: tuple[str, object] | None = None,
        first_only: bool = False,
        range_predicate: RangePredicate | None = None,
    ) -> Iterator[Any]:
        """Open batches, passing only explicitly supported source-level scan hints."""
        if self.request_opener is not None and (
            columns is not None or equality is not None or first_only or range_predicate is not None
        ):
            return self.request_opener(
                ArrowScanRequest(
                    columns=columns,
                    equality=equality,
                    first_only=first_only,
                    range_predicate=range_predicate,
                )
            )
        if columns is not None and self.projection_opener is not None:
            return self.projection_opener(columns)
        return self.opener()


def batch_to_rows(batch: Any) -> list[dict[str, Any]]:
    """Materialize an Arrow batch as independent Python row dictionaries."""
    return cast(list[dict[str, Any]], batch.to_pylist())
