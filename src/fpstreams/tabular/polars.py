"""Lazy and eager Polars row adapters."""

from __future__ import annotations

import operator
from collections.abc import Callable, Iterator
from importlib import import_module
from typing import Any, cast

from ..planning.arrow_source import ArrowBatchSource
from ..planning.source import Source
from .arrow import _close, _deferred_arrow_source, _schema_names


def polars_module() -> Any:
    """Import optional Polars support or raise the package installation hint."""
    try:
        return cast(Any, import_module("polars"))
    except ModuleNotFoundError as error:
        if error.name == "polars" or (error.name or "").startswith("polars."):
            raise ImportError(
                "Polars support requires the 'polars' extra: pip install fpstreams[polars]"
            ) from None
        raise


def _positive_size(value: int) -> int:
    """Validate a positive integer-like Polars batch size."""
    try:
        size = operator.index(value)
    except TypeError:
        raise TypeError("batch_size must be an integer") from None
    if size <= 0:
        raise ValueError("batch_size must be positive")
    return size


def is_polars_frame(source: Any) -> bool:
    """Recognize Polars DataFrame and LazyFrame objects without importing Polars unnecessarily."""
    if type(source).__module__.partition(".")[0] != "polars":
        return False
    pl = polars_module()
    return isinstance(source, (pl.DataFrame, pl.LazyFrame))


def polars_row_factory(
    frame: Any,
    *,
    batch_size: int = 65_536,
    maintain_order: bool = True,
    engine: Any = "auto",
) -> Callable[[], Iterator[dict[str, Any]]]:
    """Build a row opener for an eager or lazily collected Polars frame."""
    pl = polars_module()
    size = _positive_size(batch_size)

    if isinstance(frame, pl.DataFrame):

        def eager_records() -> Iterator[dict[str, Any]]:
            """Slice an eager DataFrame into bounded batches and yield dictionaries."""
            for batch in frame.iter_slices(n_rows=size):
                yield from batch.to_dicts()

        return eager_records

    if isinstance(frame, pl.LazyFrame):

        def lazy_records() -> Iterator[dict[str, Any]]:
            """Collect a LazyFrame in bounded batches and preserve requested row order."""
            batches = frame.collect_batches(
                chunk_size=size,
                maintain_order=maintain_order,
                lazy=True,
                engine=engine,
            )
            for batch in batches:
                yield from batch.to_dicts()

        return lazy_records

    raise TypeError("from_polars() expects a polars DataFrame or LazyFrame")


def polars_source(
    frame: Any,
    *,
    batch_size: int = 65_536,
    maintain_order: bool = True,
    engine: Any = "auto",
) -> Source[dict[str, Any]]:
    """Retain lazy Arrow batches beside the established Polars dictionary-row opener."""
    pl = polars_module()
    size = _positive_size(batch_size)
    rows = polars_row_factory(
        frame,
        batch_size=size,
        maintain_order=maintain_order,
        engine=engine,
    )

    if isinstance(frame, pl.DataFrame):

        def eager_table() -> Any:
            table = frame.to_arrow()
            _schema_names(table.schema)
            return table

        def eager_batches() -> Iterator[Any]:
            yield from eager_table().to_batches(max_chunksize=size)

        descriptor = ArrowBatchSource(
            eager_batches,
            "polars",
            size,
            columnar_opener=eager_table,
        )
        return _deferred_arrow_source(rows, descriptor)

    if isinstance(frame, pl.LazyFrame):

        def lazy_batches() -> Iterator[Any]:
            collected = frame.collect_batches(
                chunk_size=size,
                maintain_order=maintain_order,
                lazy=True,
                engine=engine,
            )
            iterator = iter(collected)
            try:
                for batch in iterator:
                    table = batch.to_arrow()
                    _schema_names(table.schema)
                    yield from table.to_batches(max_chunksize=size)
            finally:
                _close(iterator)

        descriptor = ArrowBatchSource(lazy_batches, "polars", size)
        return _deferred_arrow_source(rows, descriptor)

    raise TypeError("from_polars() expects a polars DataFrame or LazyFrame")
