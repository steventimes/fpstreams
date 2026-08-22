"""Dataframe interchange adapters built on Arrow."""

from __future__ import annotations

import json
import warnings
from collections.abc import Callable, Iterator
from importlib import import_module
from typing import Any, cast

from ..planning.arrow_source import ArrowBatchSource
from ..planning.source import Source
from .arrow import (
    _arrow_modules,
    _deferred_arrow_source,
    _positive_size,
    _schema_names,
    arrow_row_source,
)


def _without_pandas_index(table: Any) -> Any:
    """Drop physical pandas index columns and remove pandas schema metadata."""
    metadata = table.schema.metadata or {}
    pandas_metadata = metadata.get(b"pandas")
    if pandas_metadata is None:
        return table
    description = json.loads(pandas_metadata)
    index_columns = tuple(
        name for name in description.get("index_columns", ()) if isinstance(name, str)
    )
    if index_columns:
        table = table.drop(index_columns)
    remaining = {key: value for key, value in metadata.items() if key != b"pandas"}
    return table.replace_schema_metadata(remaining or None)


def _dataframe_table_factory(
    frame: Any,
    *,
    batch_size: int = 65_536,
    allow_copy: bool = True,
) -> tuple[Callable[[], Any], int]:
    """Build one lazy, validated dataframe-to-Arrow table opener."""
    pa, _dataset, _parquet = _arrow_modules()
    size = _positive_size(batch_size)
    if not callable(getattr(frame, "__dataframe__", None)):
        raise TypeError("from_dataframe() expects an object implementing __dataframe__()")

    def table() -> Any:
        """Convert on consumption while preserving the existing copy policy and index rules."""
        if allow_copy and callable(getattr(frame, "__arrow_c_stream__", None)):
            result = _without_pandas_index(pa.table(frame))
            _schema_names(result.schema)
            return result
        interchange = cast(Any, import_module("pyarrow.interchange"))
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="The Dataframe Interchange Protocol is deprecated.*",
            )
            result = interchange.from_dataframe(frame, allow_copy=allow_copy)
        _schema_names(result.schema)
        return result

    return table, size


def _dataframe_rows(table: Callable[[], Any], size: int) -> Callable[[], Iterator[dict[str, Any]]]:
    """Keep the established row conversion available for Python execution and iteration."""

    def records() -> Iterator[dict[str, Any]]:
        factory, _reiterable = arrow_row_source(table(), batch_size=size)
        yield from factory()

    return records


def _stable_columnar_opener(frame: Any, table: Callable[[], Any]) -> Callable[[], Any] | None:
    """Expose whole-table speculation only for the concrete pandas frame implementation."""
    if type(frame).__module__.partition(".")[0] != "pandas":
        return None
    pandas = import_module("pandas")
    return table if type(frame) is pandas.DataFrame else None


def dataframe_row_factory(
    frame: Any,
    *,
    batch_size: int = 65_536,
    allow_copy: bool = True,
) -> Callable[[], Iterator[dict[str, Any]]]:
    """Build a row opener for an object implementing the dataframe interchange protocol."""
    table, size = _dataframe_table_factory(
        frame,
        batch_size=batch_size,
        allow_copy=allow_copy,
    )
    return _dataframe_rows(table, size)


def dataframe_source(
    frame: Any,
    *,
    batch_size: int = 65_536,
    allow_copy: bool = True,
) -> Source[dict[str, Any]]:
    """Retain lazy Arrow batches while preserving the canonical dataframe row fallback."""
    table, size = _dataframe_table_factory(
        frame,
        batch_size=batch_size,
        allow_copy=allow_copy,
    )

    def batches() -> Iterator[Any]:
        yield from table().to_batches(max_chunksize=size)

    descriptor = ArrowBatchSource(
        batches,
        "dataframe",
        size,
        columnar_opener=_stable_columnar_opener(frame, table),
    )
    return _deferred_arrow_source(_dataframe_rows(table, size), descriptor)
