"""Factory entry point for record-oriented pipelines."""

from __future__ import annotations

import os
from collections.abc import Callable, Iterable, Mapping
from typing import TYPE_CHECKING, Any, BinaryIO, TextIO, TypeAlias, TypeVar, overload

if TYPE_CHECKING:
    import polars as pl

from ..expressions.selectors import Selector
from ..streams.flow import Flow, _ArrowCStreamProvider, _DataFrameProvider
from .rows import Rows
from .sql import (
    ConnectionFactory,
    DBParameters,
)

T = TypeVar("T")
JoinSelector: TypeAlias = Selector | tuple[Selector, ...]


class _RowsFactory:
    """Callable namespace exposed as `rows` for constructing lazy record pipelines."""

    __slots__ = ()

    if TYPE_CHECKING:

        @overload
        def __call__(
            self,
            source: pl.Series,
        ) -> Rows[Any]: ...

    @overload
    def __call__(self, source: Flow[T]) -> Rows[T]: ...

    @overload
    # Rows also exports Arrow, but the compatibility factory must preserve its record type.
    def __call__(self, source: Rows[T]) -> Rows[T]: ...  # type: ignore[overload-overlap]

    @overload
    def __call__(
        self,
        source: _ArrowCStreamProvider | _DataFrameProvider,
    ) -> Rows[dict[str, Any]]: ...

    @overload
    def __call__(self, source: Iterable[T]) -> Rows[T]: ...

    @overload
    def __call__(self, source: Any) -> Rows[Any]: ...

    def __call__(self, source: Any) -> Rows[Any]:
        """Wrap records in Rows, reusing Flow's single tabular dispatch path.

        Args:
            source: A Flow or iterable of records, Polars frame, or __dataframe__ provider.

        Returns:
            A lazy Rows wrapper; construction does not iterate record sources.
        """
        return Rows(source)

    def from_csv(
        self,
        path: str | os.PathLike[str] | TextIO | Callable[[], TextIO],
        *,
        encoding: str = "utf-8",
        **format_parameters: Any,
    ) -> Rows[dict[str, Any]]:
        """Read CSV rows lazily from a path, caller-owned handle, or owned opener.

        Args:
            path: A replayable path, a one-shot caller-owned text handle, or a replayable
                zero-argument opener whose returned handle fpstreams closes.
            encoding: Text encoding used only when fpstreams opens a path.
            **format_parameters: Keyword options forwarded to csv.DictReader, such as
                dialect, delimiter, or quoting.

        Returns:
            A lazy Rows pipeline of dictionaries keyed by the unique CSV header.
        """
        return Rows.from_csv(path, encoding=encoding, **format_parameters)

    def scan_csv(
        self,
        path: str | os.PathLike[str],
        *,
        batch_size: int = 65_536,
        read_options: Any = None,
        parse_options: Any = None,
        convert_options: Any = None,
        memory_pool: Any = None,
    ) -> Rows[dict[str, Any]]:
        """Lazily scan typed CSV batches with optional query-level column pruning.

        Unlike :meth:`from_csv`, this explicit Arrow path infers non-string scalar types.
        PyArrow options can fix parsing and conversion behavior when inference is unsuitable.
        The incremental Arrow reader is single-threaded and freezes inferred types after its
        first byte block; use ``read_options`` or ``convert_options`` to control those choices.
        Query column pruning intentionally avoids conversion work, including conversion errors,
        in columns the query does not read.

        Args:
            path: Local CSV path reopened for each iteration.
            batch_size: Maximum rows exposed by each retained Arrow batch.
            read_options: Optional ``pyarrow.csv.ReadOptions``.
            parse_options: Optional ``pyarrow.csv.ParseOptions``.
            convert_options: Optional ``pyarrow.csv.ConvertOptions``.
            memory_pool: Optional PyArrow memory pool used by the reader.

        Returns:
            Reusable typed rows. This adapter requires the ``arrow`` extra.
        """
        return Rows.scan_csv(
            path,
            batch_size=batch_size,
            read_options=read_options,
            parse_options=parse_options,
            convert_options=convert_options,
            memory_pool=memory_pool,
        )

    def from_jsonl(
        self,
        path: (str | os.PathLike[str] | TextIO | BinaryIO | Callable[[], TextIO | BinaryIO]),
        *,
        encoding: str = "utf-8",
        max_record_bytes: int | None = 8 * 1024 * 1024,
    ) -> Rows[dict[str, Any]]:
        """Read JSON objects lazily from a path, caller-owned handle, or owned opener.

        Args:
            path: A replayable path, a one-shot caller-owned text or binary handle, or a
                replayable zero-argument opener whose returned handle fpstreams closes.
            encoding: Encoding used for paths and binary handles, and for byte accounting on text
                handles.
            max_record_bytes: Encoded-byte limit per line, or None for no limit.

        Returns:
            Lazy dictionary rows; duplicate keys and non-object records fail when consumed.
        """
        return Rows.from_jsonl(
            path,
            encoding=encoding,
            max_record_bytes=max_record_bytes,
        )

    def from_arrow(self, source: Any, *, batch_size: int = 65_536) -> Rows[dict[str, Any]]:
        """Adapt an Arrow object or C Stream provider to dictionary rows.

        Args:
            source: Reusable PyArrow Table/RecordBatch, one-shot RecordBatchReader, or an
                object implementing ``__arrow_c_stream__``. A C Stream provider is imported
                once at construction and treated as one-shot.
            batch_size: Maximum rows converted from each Arrow batch slice.

        Returns:
            Lazy Rows; reader-backed inputs may be consumed only once and are closed afterward.
        """
        return Rows.from_arrow(source, batch_size=batch_size)

    def from_columns(
        self,
        columns: Mapping[str, Any],
        *,
        batch_size: int = 65_536,
    ) -> Rows[dict[str, Any]]:
        """Adapt an explicit mapping of independent columns through Arrow."""
        return Rows.from_columns(columns, batch_size=batch_size)

    def from_numpy(
        self,
        array: Any,
        *,
        columns: Iterable[str] | None = None,
    ) -> Rows[dict[str, Any]]:
        """Adapt a two-dimensional NumPy array to replayable dictionary rows.

        Args:
            array: Two-dimensional ndarray or array-like input accepted by ``numpy.asarray``.
            columns: Unique non-empty string names matching the array width. Defaults to
                stringified integer positions.

        Returns:
            Lazy rows that convert one retained array row at a time when consumed.
        """
        return Rows.from_numpy(array, columns=columns)

    def from_dataframe(
        self,
        frame: Any,
        *,
        batch_size: int = 65_536,
        allow_copy: bool = True,
    ) -> Rows[dict[str, Any]]:
        """Adapt an object implementing the dataframe interchange protocol through PyArrow.

        Args:
            frame: Object providing __dataframe__(), optionally with an Arrow C stream.
            batch_size: Maximum rows converted from each Arrow batch.
            allow_copy: Permit interchange conversion to allocate copied buffers.

        Returns:
            Lazy Rows that perform dataframe-to-Arrow conversion when iterated.
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
        """Adapt an eager Polars DataFrame or batch-collected LazyFrame to dictionary rows.

        Args:
            frame: Polars DataFrame or LazyFrame to slice or collect.
            batch_size: Rows requested per eager slice or lazy collection batch.
            maintain_order: Preserve LazyFrame row order while collecting batches.
            engine: Polars engine used only for LazyFrame batch collection.

        Returns:
            Lazy reusable Rows; a LazyFrame is collected again for each iteration.
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
        """Build reusable rows from a fresh PyArrow dataset scanner per iteration.

        Args:
            source: PyArrow Dataset or dataset source accepted by pyarrow.dataset().
            columns: Unique projected column names, or None for all columns.
            filter: PyArrow dataset expression pushed into the scanner.
            batch_size: Maximum rows requested from each scanner batch.
            use_threads: Allow the PyArrow scanner to use worker threads.
            filesystem: Optional PyArrow filesystem for resolving the source.
            partitioning: Optional dataset partitioning specification.

        Returns:
            Lazy dictionary rows with projection and filtering performed by PyArrow.
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
        """Build a reiterable DB-API query source that owns its connections and cursors.

        Args:
            connect: Zero-argument factory called once per iteration for a new connection.
            query: Statement executed by each newly opened cursor.
            parameters: Optional mapping or positional values passed to cursor.execute().
            batch_size: Maximum rows requested by each cursor.fetchmany() call.

        Returns:
            Lazy rows that close the cursor and connection on exhaustion, error, or early stop.
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
        """Build a reiterable SQLite query source that owns one connection per iteration.

        Args:
            database: SQLite path or URI passed to sqlite3.connect().
            query: Statement executed by each newly opened cursor.
            parameters: Optional mapping or positional values passed to cursor.execute().
            batch_size: Maximum rows requested by each cursor.fetchmany() call.
            timeout: Seconds sqlite3 waits for a locked database.
            uri: Interpret database as a SQLite URI when true.

        Returns:
            Lazy dictionary rows that close their cursor and connection after iteration.
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
