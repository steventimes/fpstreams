"""Factory entry point for record-oriented pipelines."""

from __future__ import annotations

import os
from collections.abc import Iterable
from typing import Any, TypeAlias, TypeVar, cast

from ..expressions.selectors import Selector
from ..streams.flow import Flow
from .polars import is_polars_frame
from .rows import Rows
from .sql import (
    ConnectionFactory,
    DBParameters,
)

T = TypeVar("T")
JoinSelector: TypeAlias = Selector | tuple[Selector, ...]


class _RowsFactory:
    """Callable factory for creating record pipelines from supported sources."""

    __slots__ = ()

    def __call__(self, source: Iterable[T] | Flow[T]) -> Rows[T]:
        """Create a lazy row pipeline from a supported source.

        Args:
            source: Records, a `Flow`, or a supported dataframe object.

        Returns:
            A lazy `Rows` pipeline over the source.
        """
        if is_polars_frame(source):
            return cast(Rows[T], Rows.from_polars(source))
        if callable(getattr(source, "__dataframe__", None)):
            return cast(Rows[T], Rows.from_dataframe(source))
        return Rows(source)

    def from_csv(
        self,
        path: str | os.PathLike[str],
        *,
        encoding: str = "utf-8",
        **format_parameters: Any,
    ) -> Rows[dict[str, Any]]:
        """Read a CSV file lazily and emit rows as dictionaries.

        Args:
            path: The filesystem path to read from or write to.
            encoding: The text encoding used to open the file.
            **format_parameters: Additional keyword arguments passed to the underlying
                file-format reader.

        Returns:
            A new lazy `Rows` pipeline representing this operation.
        """
        return Rows.from_csv(path, encoding=encoding, **format_parameters)

    def from_jsonl(
        self,
        path: str | os.PathLike[str],
        *,
        encoding: str = "utf-8",
        max_record_bytes: int | None = 8 * 1024 * 1024,
    ) -> Rows[dict[str, Any]]:
        """Read a JSON Lines file lazily and emit decoded objects.

        Args:
            path: The filesystem path to read from or write to.
            encoding: The text encoding used to open the file.
            max_record_bytes: Maximum encoded bytes per physical record, or None to disable.

        Returns:
            A new lazy `Rows` pipeline representing this operation.
        """
        return Rows.from_jsonl(
            path,
            encoding=encoding,
            max_record_bytes=max_record_bytes,
        )

    def from_arrow(self, source: Any, *, batch_size: int = 65_536) -> Rows[dict[str, Any]]:
        """Create rows from an Arrow-compatible source.

        Args:
            source: The iterable, async iterable, or data source to read lazily.
            batch_size: The maximum number of rows processed in each batch.

        Returns:
            A new lazy `Rows` pipeline representing this operation.
        """
        return Rows.from_arrow(source, batch_size=batch_size)

    def from_dataframe(
        self,
        frame: Any,
        *,
        batch_size: int = 65_536,
        allow_copy: bool = True,
    ) -> Rows[dict[str, Any]]:
        """Create rows through the dataframe interchange protocol.

        Args:
            frame: The dataframe-like object used as the row source.
            batch_size: The maximum number of rows processed in each batch.
            allow_copy: Whether an adapter may copy data when zero-copy conversion is unavailable.

        Returns:
            A new lazy `Rows` pipeline representing this operation.
        """
        return Rows.from_dataframe(
            frame,
            batch_size=batch_size,
            allow_copy=allow_copy,
        )

    from_pandas = from_dataframe

    def from_polars(
        self,
        frame: Any,
        *,
        batch_size: int = 65_536,
        maintain_order: bool = True,
        engine: Any = "auto",
    ) -> Rows[dict[str, Any]]:
        """Create rows from a Polars DataFrame or LazyFrame.

        Args:
            frame: The dataframe-like object used as the row source.
            batch_size: The maximum number of rows processed in each batch.
            maintain_order: Whether output must preserve the source row order.
            engine: The execution engine requested for this pipeline.

        Returns:
            A new lazy `Rows` pipeline representing this operation.
        """
        return Rows.from_polars(
            frame,
            batch_size=batch_size,
            maintain_order=maintain_order,
            engine=engine,
        )

    def from_parquet(
        self,
        source: Any,
        *,
        columns: Iterable[str] | None = None,
        filter: Any = None,
        batch_size: int = 65_536,
        use_threads: bool = True,
        filesystem: Any = None,
        partitioning: Any = None,
    ) -> Rows[dict[str, Any]]:
        """Scan Parquet data lazily with optional projection and filtering.

        Args:
            source: The iterable, async iterable, or data source to read lazily.
            columns: The columns or column mapping used by the operation.
            filter: An optional dataset filter pushed into the underlying reader.
            batch_size: The maximum number of rows processed in each batch.
            use_threads: Whether the underlying Arrow operation may use worker threads.
            filesystem: The optional filesystem implementation used to access the data source.
            partitioning: The partitioning scheme used to interpret a dataset.

        Returns:
            A new lazy `Rows` pipeline representing this operation.
        """
        return Rows.from_parquet(
            source,
            columns=columns,
            filter=filter,
            batch_size=batch_size,
            use_threads=use_threads,
            filesystem=filesystem,
            partitioning=partitioning,
        )

    def from_db(
        self,
        connect: ConnectionFactory,
        query: str,
        parameters: DBParameters = None,
        *,
        batch_size: int = 1_000,
    ) -> Rows[dict[str, Any]]:
        """Execute a DB-API query lazily and emit rows as dictionaries.

        Args:
            connect: A zero-argument callable that opens a new database connection.
            query: The SQL query executed for each fresh iteration.
            parameters: Parameters passed to the database query or statement.
            batch_size: The maximum number of rows processed in each batch.

        Returns:
            A new lazy `Rows` pipeline representing this operation.
        """
        return Rows.from_db(connect, query, parameters, batch_size=batch_size)

    def from_sqlite(
        self,
        database: str | os.PathLike[str],
        query: str,
        parameters: DBParameters = None,
        *,
        batch_size: int = 1_000,
        timeout: float = 5.0,
        uri: bool = False,
    ) -> Rows[dict[str, Any]]:
        """Execute a SQLite query lazily and emit rows as dictionaries.

        Args:
            database: The SQLite database path or connection target.
            query: The SQL query executed for each fresh iteration.
            parameters: Parameters passed to the database query or statement.
            batch_size: The maximum number of rows processed in each batch.
            timeout: The optional maximum duration in seconds before the operation fails.
            uri: Whether the SQLite database string should be interpreted as a URI.

        Returns:
            A new lazy `Rows` pipeline representing this operation.
        """
        return Rows.from_sqlite(
            database,
            query,
            parameters,
            batch_size=batch_size,
            timeout=timeout,
            uri=uri,
        )


rows = _RowsFactory()
