"""Dataframe interchange adapters built on Arrow."""

from __future__ import annotations

import json
import warnings
from collections.abc import Callable, Iterator
from importlib import import_module
from typing import Any, cast

from .arrow import _arrow_modules, _positive_size, arrow_row_source


def _without_pandas_index(table: Any) -> Any:
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


def dataframe_row_factory(
    frame: Any,
    *,
    batch_size: int = 65_536,
    allow_copy: bool = True,
) -> Callable[[], Iterator[dict[str, Any]]]:
    pa, _dataset, _parquet = _arrow_modules()
    size = _positive_size(batch_size)
    if not callable(getattr(frame, "__dataframe__", None)):
        raise TypeError("from_dataframe() expects an object implementing __dataframe__()")

    def records() -> Iterator[dict[str, Any]]:
        if allow_copy and callable(getattr(frame, "__arrow_c_stream__", None)):
            table = _without_pandas_index(pa.table(frame))
        else:
            interchange = cast(Any, import_module("pyarrow.interchange"))
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    message="The Dataframe Interchange Protocol is deprecated.*",
                )
                table = interchange.from_dataframe(frame, allow_copy=allow_copy)
        factory, _reiterable = arrow_row_source(table, batch_size=size)
        yield from factory()

    return records
