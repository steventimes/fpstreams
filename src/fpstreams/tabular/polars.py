"""Lazy and eager Polars row adapters."""

from __future__ import annotations

import operator
from collections.abc import Callable, Iterator
from importlib import import_module
from typing import Any, cast


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
