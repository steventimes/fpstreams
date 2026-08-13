"""Output adapters and writers shared by record pipelines."""

from __future__ import annotations

import csv
import json
import os
from collections.abc import Callable, Iterable, Iterator, Mapping
from typing import Any, Generic, Literal, TypeAlias, TypeVar

from ..collecting.collector import _collect_columns
from ..expressions.selectors import Selector
from ..streams.flow import Flow, flow
from .arrow import (
    arrow_batch_factory,
    table_from_rows,
    write_parquet_rows,
)
from .polars import polars_module
from .records import (
    _as_record,
    _record_view,
    _require_unique_names,
)
from .sql import ConnectionFactory, write_db_rows
from .sqlite_sink import write_sqlite_rows

T = TypeVar("T")
JoinSelector: TypeAlias = Selector | tuple[Selector, ...]


class RowsIOMixin(Generic[T]):
    """Typed output operations implemented by concrete Rows pipelines."""

    _flow: Flow[T]

    def __iter__(self) -> Iterator[T]:
        raise NotImplementedError

    def to_columns(self) -> dict[str, list[Any]]:
        """Collect records into a dictionary of column lists.

        Returns:
            A dictionary containing the computed keys and values.
        """
        return _collect_columns(_as_record(row) for row in self)

    def to_pandas(self, *, batch_size: int = 65_536, schema: Any = None) -> Any:
        """Collect rows into a pandas DataFrame through Arrow.

        Args:
            batch_size: The maximum number of rows processed in each batch.
            schema: The explicit schema used to interpret or write records.

        Returns:
            A pandas `DataFrame` containing the rows.
        """
        try:
            import pandas as pd  # type: ignore[import-untyped]
        except ImportError:
            raise ImportError(
                "to_pandas() requires the 'data' extra: pip install fpstreams[data]"
            ) from None
        _ = pd
        return self.to_arrow(batch_size=batch_size, schema=schema).to_pandas()

    to_df = to_pandas

    def arrow_batches(self, *, batch_size: int = 65_536, schema: Any = None) -> Flow[Any]:
        """Stream PyArrow RecordBatch objects.

        Args:
            batch_size: The maximum number of rows processed in each batch.
            schema: The explicit schema used to interpret or write records.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        return flow.defer(
            arrow_batch_factory(
                self,
                batch_size=batch_size,
                schema=schema,
                as_record=_record_view,
            )
        )

    to_arrow_batches = arrow_batches

    def to_arrow(self, *, batch_size: int = 65_536, schema: Any = None) -> Any:
        """Collect rows into a PyArrow Table.

        Args:
            batch_size: The maximum number of rows processed in each batch.
            schema: The explicit schema used to interpret or write records.

        Returns:
            A PyArrow `Table` containing the rows.
        """
        return table_from_rows(
            self,
            batch_size=batch_size,
            schema=schema,
            as_record=_record_view,
        )

    def polars_batches(self, *, batch_size: int = 65_536, schema: Any = None) -> Flow[Any]:
        """Stream Polars DataFrames converted from Arrow batches.

        Args:
            batch_size: The maximum number of rows processed in each batch.
            schema: The explicit schema used to interpret or write records.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        pl = polars_module()
        return self.arrow_batches(batch_size=batch_size, schema=schema).map(
            lambda batch: pl.from_arrow(batch, rechunk=False)
        )

    to_polars_batches = polars_batches

    def to_polars(self, *, batch_size: int = 65_536, schema: Any = None) -> Any:
        """Collect rows into a Polars DataFrame.

        Args:
            batch_size: The maximum number of rows processed in each batch.
            schema: The explicit schema used to interpret or write records.

        Returns:
            A Polars `DataFrame` containing the rows.
        """
        pl = polars_module()
        return pl.from_arrow(
            self.to_arrow(batch_size=batch_size, schema=schema),
            rechunk=False,
        )

    def to_csv(
        self,
        path: str | os.PathLike[str],
        *,
        fieldnames: Iterable[str] | None = None,
        encoding: str = "utf-8",
        include_header: bool = True,
        extrasaction: Literal["raise", "ignore"] = "raise",
    ) -> None:
        """Write rows to a CSV file without retaining the full input.

        Args:
            path: The filesystem path to read from or write to.
            fieldnames: The CSV field order. Required when it cannot be inferred.
            encoding: The text encoding used to open the file.
            include_header: Whether to write a CSV header row.
            extrasaction: How CSV writing handles fields absent from fieldnames.

        Returns:
            The number of rows written.
        """
        if extrasaction not in {"raise", "ignore"}:
            raise ValueError("extrasaction must be 'raise' or 'ignore'")
        names = tuple(fieldnames) if fieldnames is not None else None
        if names is not None and not names:
            raise ValueError("fieldnames cannot be empty")
        if names is not None:
            _require_unique_names(names, operation="to_csv")

        iterator = iter(self)
        try:
            with open(path, "w", encoding=encoding, newline="") as handle:
                try:
                    first = _as_record(next(iterator))
                except StopIteration:
                    if names is not None and include_header:
                        csv.DictWriter(handle, fieldnames=names).writeheader()
                    return
                output_names = names or tuple(first)
                if not output_names:
                    raise ValueError("cannot infer CSV columns from an empty record")
                writer = csv.DictWriter(
                    handle,
                    fieldnames=output_names,
                    extrasaction=extrasaction,
                )
                if include_header:
                    writer.writeheader()
                writer.writerow(first)
                for row in iterator:
                    writer.writerow(_as_record(row))
        finally:
            close = getattr(iterator, "close", None)
            if callable(close):
                close()

    def to_jsonl(
        self,
        path: str | os.PathLike[str],
        *,
        encoding: str = "utf-8",
        ensure_ascii: bool = False,
    ) -> None:
        """Write one JSON record per line.

        Args:
            path: The filesystem path to read from or write to.
            encoding: The text encoding used to open the file.
            ensure_ascii: Whether JSON output escapes non-ASCII characters.

        Returns:
            The number of rows written.
        """
        with open(path, "w", encoding=encoding) as handle:
            for row in self:
                json.dump(_as_record(row), handle, ensure_ascii=ensure_ascii)
                handle.write("\n")

    def to_parquet(
        self,
        path: str | os.PathLike[str],
        *,
        if_exists: Literal["error", "replace"] = "error",
        batch_size: int = 65_536,
        schema: Any = None,
        compression: Any = "zstd",
        use_dictionary: Any = True,
        write_statistics: Any = True,
        writer_options: Mapping[str, Any] | None = None,
    ) -> int:
        """Write rows to Parquet in bounded Arrow batches.

        Rows are converted and written in bounded batches rather than materialized as one table.

        Args:
            path: The filesystem path to read from or write to.
            if_exists: How to handle an existing destination table.
            batch_size: The maximum number of rows processed in each batch.
            schema: The explicit schema used to interpret or write records.
            compression: The compression codec requested by the output format.
            use_dictionary: Whether Parquet writing should use dictionary encoding.
            write_statistics: Whether Parquet metadata should include column statistics.
            writer_options: Additional options passed to the underlying writer.

        Returns:
            The number of rows written.
        """
        return write_parquet_rows(
            self,
            path,
            if_exists=if_exists,
            batch_size=batch_size,
            schema=schema,
            compression=compression,
            use_dictionary=use_dictionary,
            write_statistics=write_statistics,
            writer_options=writer_options,
            as_record=_record_view,
        )

    def to_db(
        self,
        connect: ConnectionFactory,
        statement: str,
        *,
        parameters: Callable[[T], Any] | None = None,
        batch_size: int = 1_000,
    ) -> int:
        """Execute a DB-API statement for rows in batches.

        Rows are submitted in bounded batches inside one transaction.

        Args:
            connect: A zero-argument callable that opens a new database connection.
            statement: The SQL statement executed for each batch of rows.
            parameters: Parameters passed to the database query or statement.
            batch_size: The maximum number of rows processed in each batch.

        Returns:
            The number of rows submitted to the database.
        """
        return write_db_rows(
            self,
            connect,
            statement,
            parameters=parameters,
            batch_size=batch_size,
        )

    def to_sqlite(
        self,
        database: str | os.PathLike[str],
        table: str,
        *,
        if_exists: Literal["append", "fail", "replace"] = "append",
        conflict: Literal["error", "ignore", "replace"] = "error",
        columns: Iterable[str] | None = None,
        schema: Mapping[str, str] | None = None,
        batch_size: int = 1_000,
        timeout: float = 5.0,
        uri: bool = False,
    ) -> int:
        """Insert rows into a SQLite table in batches.

        Rows are submitted in bounded batches inside one SQLite transaction.

        Args:
            database: The SQLite database path or connection target.
            table: The destination table name.
            if_exists: How to handle an existing destination table.
            conflict: The duplicate-key or name conflict policy.
            columns: The columns or column mapping used by the operation.
            schema: The explicit schema used to interpret or write records.
            batch_size: The maximum number of rows processed in each batch.
            timeout: The optional maximum duration in seconds before the operation fails.
            uri: Whether the SQLite database string should be interpreted as a URI.

        Returns:
            The number of rows written to SQLite.
        """
        return write_sqlite_rows(
            self,
            database,
            table,
            if_exists=if_exists,
            conflict=conflict,
            columns=columns,
            schema=schema,
            batch_size=batch_size,
            timeout=timeout,
            uri=uri,
            as_record=_record_view,
        )
