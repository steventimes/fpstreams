"""Output adapters and writers shared by record pipelines."""

from __future__ import annotations

import csv
import json
import os
from collections.abc import Callable, Iterable, Iterator, Mapping
from typing import Any, Generic, Literal, TypeAlias, TypeVar, cast

from ..collecting.collector import _collect_columns
from ..errors import SelectionError
from ..expressions.selectors import Selector, compile_selector
from ..io_safety import spreadsheet_safe_cell
from ..runtime.iterators import closing_iterators
from ..streams.flow import Flow, flow
from .arrow import (
    _arrow_modules,
    _close,
    _positive_size,
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


def _direct_numpy_field_values(
    iterator: Iterator[Any],
    field_names: tuple[str, ...],
    compiled: tuple[Callable[[Any], Any], ...],
) -> tuple[list[Any], int]:
    """Select exact dictionary fields into one row-major flat list."""
    if len(field_names) == 1:
        name = field_names[0]
        select = compiled[0]
        selected_rows: list[Any] = []
        for row in iterator:
            if type(row) is not dict:
                selected_rows.append(select(row))
                continue
            try:
                selected_rows.append(row[name])
            except (AttributeError, KeyError, TypeError) as error:
                raise SelectionError(
                    f"Could not resolve selector {name!r}; failed at {name!r}"
                ) from error
        return selected_rows, 1

    if len(field_names) == 2:
        first_name, second_name = field_names
        first_select, second_select = compiled
        selected_rows = []
        for row in iterator:
            if type(row) is not dict:
                selected_rows.append(first_select(row))
                selected_rows.append(second_select(row))
                continue
            try:
                first = row[first_name]
            except (AttributeError, KeyError, TypeError) as error:
                raise SelectionError(
                    f"Could not resolve selector {first_name!r}; failed at {first_name!r}"
                ) from error
            try:
                second = row[second_name]
            except (AttributeError, KeyError, TypeError) as error:
                raise SelectionError(
                    f"Could not resolve selector {second_name!r}; failed at {second_name!r}"
                ) from error
            selected_rows.append(first)
            selected_rows.append(second)
        return selected_rows, 2

    selected_rows = []
    for row in iterator:
        if type(row) is not dict:
            for select in compiled:
                selected_rows.append(select(row))
            continue
        for name in field_names:
            try:
                selected_rows.append(row[name])
            except (AttributeError, KeyError, TypeError) as error:
                raise SelectionError(
                    f"Could not resolve selector {name!r}; failed at {name!r}"
                ) from error
    return selected_rows, len(field_names)


def _numpy_matrix_values_from_iterator(
    iterator: Iterator[Any],
    selectors: tuple[Selector, ...],
) -> tuple[list[Any], int]:
    """Materialize matrix values while the caller owns the opened iterator."""
    if selectors:
        compiled = tuple(compile_selector(selector) for selector in selectors)
        if all(type(selector) is str and "." not in selector for selector in selectors):
            field_names = cast(tuple[str, ...], selectors)
            return _direct_numpy_field_values(iterator, field_names, compiled)
        selected_values = []
        for row in iterator:
            for select in compiled:
                selected_values.append(select(row))
        return selected_values, len(compiled)

    names: list[str] = []
    seen: set[str] = set()
    output: list[Any] = []
    for row in iterator:
        record = _record_view(row)
        previous_width = len(names)
        for name in record:
            if name not in seen:
                seen.add(name)
                names.append(name)
        if len(names) != previous_width:
            missing = [None] * (len(names) - previous_width)
            for previous in output:
                previous.extend(missing)
        output.append([record.get(name) for name in names])
    return output, len(names)


def _numpy_matrix_values(
    rows: Iterable[Any],
    selectors: tuple[Selector, ...],
) -> tuple[list[Any], int]:
    """Materialize matrix values and close an iterator opened from public row iteration."""
    iterator = iter(rows)
    with closing_iterators((iterator,)):
        return _numpy_matrix_values_from_iterator(iterator, selectors)


class RowsIOMixin(Generic[T]):
    """Collection and streaming sink operations mixed into Rows; calls consume the pipeline."""

    _flow: Flow[T]

    def __iter__(self) -> Iterator[T]:
        """Yield records from the concrete Rows implementation."""
        raise NotImplementedError

    def to_columns(self) -> dict[str, list[Any]]:
        """Transpose rows into encounter-ordered, None-padded column lists.

        Returns:
            A field-to-list mapping with one aligned entry for every consumed row.
        """
        from .rows import Rows

        direct_flow = type(self) is Rows and type(self._flow) is Flow
        if not direct_flow:
            return _collect_columns(_as_record(row) for row in self)

        def collect_python_rows() -> dict[str, list[Any]]:
            return self._flow._consume(
                lambda iterator: _collect_columns(_as_record(row) for row in iterator)
            )

        try:
            pipeline = self._flow._pipeline
        except TypeError:
            pipeline = None
        if direct_flow and pipeline is not None:
            from ..execution.numpy_prefix import try_numpy_prefix_columns

            handled, numpy_columns = try_numpy_prefix_columns(pipeline)
            if handled:
                return cast(dict[str, list[Any]], numpy_columns)
        if (
            direct_flow
            and pipeline is not None
            and pipeline.engine == "auto"
            and pipeline.parallel is None
            and not pipeline.operations
        ):
            descriptor = pipeline.source.native_data
            from ..runtime.failpoints import has_active_failpoints, hit
            from .numpy import (
                NumpyRowSource,
                guarded_numpy_identity_source,
                numpy_identity_columns,
            )

            if isinstance(descriptor, NumpyRowSource):
                guarded = guarded_numpy_identity_source(pipeline.source)
                if guarded is None:
                    return collect_python_rows()
                opened = pipeline.source.open_native(NumpyRowSource)
                hit("source.open.after")
                return numpy_identity_columns(opened)

            from ..planning.arrow_source import ArrowBatchSource

            if has_active_failpoints():
                return collect_python_rows()
            if (
                isinstance(descriptor, ArrowBatchSource)
                and descriptor.materialized_data is not None
            ):
                pipeline.source.open_native(ArrowBatchSource)
                columns: dict[str, list[Any]] = {}
                row_count = 0
                batches = descriptor.open_batches()
                try:
                    for batch in batches:
                        if batch.num_rows == 0:
                            continue
                        converted = cast(dict[str, list[Any]], batch.to_pydict())
                        if row_count == 0:
                            columns = converted
                        else:
                            for name, values in converted.items():
                                columns[name].extend(values)
                        row_count += batch.num_rows
                finally:
                    _close(batches)
                return columns if row_count else {}
        return collect_python_rows()

    def to_pandas(self, *, batch_size: int = 65_536, schema: Any = None) -> Any:
        """Materialize all rows as a pandas DataFrame through bounded Arrow conversion.

        Args:
            batch_size: Maximum source rows converted in each intermediate Arrow batch.
            schema: Optional Arrow schema fixing field order, types, and allowed columns.

        Returns:
            A pandas DataFrame containing all rows; the Rows pipeline is fully consumed.
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

    def to_numpy(
        self,
        *selectors: Selector,
        dtype: Any = None,
        copy: bool | None = None,
    ) -> Any:
        """Materialize selected record values as a two-dimensional NumPy array.

        Without selectors, mapping fields are aligned in first-seen order and missing fields
        become ``None``. With selectors, every selector follows the same field, index, path,
        expression, and ``SelectionError`` behavior as the rest of Rows.

        Args:
            *selectors: Optional selectors defining output columns in encounter order.
            dtype: Optional dtype forwarded to NumPy conversion.
            copy: NumPy copy policy: ``None`` copies only as needed, ``True`` always copies,
                and ``False`` requests no copy. NumPy 2.x raises when that request cannot be
                honored; NumPy 1.x treats it as a best-effort preference.

        Returns:
            A two-dimensional ndarray with one row per consumed record.
        """
        from .numpy import (
            NumpyRowSource,
            guarded_numpy_identity_source,
            numpy_array,
            numpy_identity_array,
            numpy_module,
        )
        from .rows import Rows

        np = numpy_module("to_numpy()")
        direct_flow = type(self) is Rows and type(self._flow) is Flow
        if direct_flow and not selectors:
            try:
                pipeline = self._flow._pipeline
            except TypeError:
                pipeline = None
            if (
                pipeline is not None
                and pipeline.engine == "auto"
                and pipeline.parallel is None
                and not pipeline.operations
                and isinstance(pipeline.source.native_data, NumpyRowSource)
            ):
                from ..runtime.failpoints import hit

                guarded = guarded_numpy_identity_source(
                    pipeline.source,
                    observers=False,
                    exact_names=False,
                )
                if guarded is not None:
                    descriptor = pipeline.source.open_native(NumpyRowSource)
                    hit("source.open.after")
                    return numpy_identity_array(
                        np,
                        descriptor,
                        dtype=dtype,
                        copy=copy,
                    )

        if direct_flow:
            values, width = self._flow._consume(
                lambda iterator: _numpy_matrix_values_from_iterator(iterator, selectors)
            )
        else:
            values, width = _numpy_matrix_values(self, selectors)
        if not values:
            empty = np.empty((0, width), dtype=dtype)
            return numpy_array(np, empty, dtype=dtype, copy=copy)
        array = numpy_array(np, values, dtype=dtype, copy=copy)
        if selectors:
            row_count = len(values) // width
            return array.reshape((row_count, width, *array.shape[1:]))
        return array

    def arrow_batches(self, *, batch_size: int = 65_536, schema: Any = None) -> Flow[Any]:
        """Return a lazy Flow of bounded PyArrow RecordBatch objects.

        Args:
            batch_size: Maximum source rows retained for one emitted RecordBatch.
            schema: Optional fixed schema; later fields outside it are rejected.

        Returns:
            A Flow that closes its upstream iterator when exhausted, failed, or short-circuited.
        """
        size = _positive_size(batch_size)
        if schema is None:
            try:
                pipeline = self._flow._pipeline
            except TypeError:
                pipeline = None
            if pipeline is not None:
                from ..execution.arrow import try_arrow_batch_factory

                native_factory = try_arrow_batch_factory(pipeline, batch_size=size)
                if native_factory is not None:
                    return flow.defer(native_factory)
        return flow.defer(
            arrow_batch_factory(
                self,
                batch_size=size,
                schema=schema,
                as_record=_record_view,
            )
        )

    to_arrow_batches = arrow_batches

    def to_arrow(self, *, batch_size: int = 65_536, schema: Any = None) -> Any:
        """Materialize all rows as one PyArrow Table.

        Args:
            batch_size: Maximum rows converted in each intermediate RecordBatch.
            schema: Optional schema used for conversion and for an empty result.

        Returns:
            A Table containing every row; direct Arrow sources may reuse native batches.
        """
        size = _positive_size(batch_size)
        from .rows import Rows

        direct_flow = type(self) is Rows and type(self._flow) is Flow
        try:
            pipeline = self._flow._pipeline
        except TypeError:
            # Joins and aggregates have no source-equivalent linear view. They
            # must be evaluated before Arrow sees their result records.
            pipeline = None
        if (
            schema is None
            and direct_flow
            and pipeline is not None
            and pipeline.engine == "auto"
            and pipeline.parallel is None
            and not pipeline.operations
        ):
            from ..runtime.failpoints import hit
            from .numpy import (
                NumpyRowSource,
                guarded_numpy_identity_source,
                numpy_identity_arrow_table,
            )

            descriptor = pipeline.source.native_data
            if (
                isinstance(descriptor, NumpyRowSource)
                and guarded_numpy_identity_source(pipeline.source) is not None
            ):
                pa, _dataset, _parquet = _arrow_modules()
                opened = pipeline.source.open_native(NumpyRowSource)
                hit("source.open.after")
                return numpy_identity_arrow_table(pa, opened, batch_size=size)
        if schema is None and pipeline is not None:
            from ..execution.arrow import try_arrow_table

            handled, table = try_arrow_table(pipeline, batch_size=size)
            if handled:
                return table
        return table_from_rows(
            self,
            batch_size=size,
            schema=schema,
            as_record=_record_view,
        )

    def __arrow_c_stream__(self, requested_schema: Any = None) -> Any:
        """Export this Rows pipeline through Arrow's standard PyCapsule stream protocol."""
        try:
            pipeline = self._flow._pipeline
        except TypeError:
            pipeline = None
        if (
            pipeline is not None
            and pipeline.engine == "auto"
            and pipeline.parallel is None
            and not pipeline.operations
        ):
            from ..planning.arrow_source import ArrowBatchSource
            from ..runtime.failpoints import has_active_failpoints

            descriptor = pipeline.source.native_data
            pa, _dataset, _parquet = _arrow_modules()
            # PyArrow 25 does not close the Python iterable held by ``from_batches`` when
            # RecordBatchReader.close() is called. Restrict lazy export to inputs whose
            # abandonment cannot leave an external reader, file, or callback source open.
            if (
                not has_active_failpoints()
                and isinstance(descriptor, ArrowBatchSource)
                and descriptor.kind in {"table", "record_batch"}
                and isinstance(descriptor.materialized_data, (pa.Table, pa.RecordBatch))
                and descriptor.schema_hint is not None
            ):

                def batches() -> Iterator[Any]:
                    pipeline.source.open_native(ArrowBatchSource)
                    opened = descriptor.open_batches()
                    try:
                        yield from opened
                    finally:
                        _close(opened)

                reader = pa.RecordBatchReader.from_batches(descriptor.schema_hint, batches())
                try:
                    return reader.__arrow_c_stream__(requested_schema)
                except BaseException:
                    _close(reader)
                    raise
            if (
                not has_active_failpoints()
                and requested_schema is None
                and type(descriptor) in {list, tuple}
            ):
                opened = iter(self.arrow_batches())
                try:
                    first_batch = next(opened)
                except StopIteration:
                    first_batch = None
                except BaseException:
                    _close(opened)
                    raise

                def batches() -> Iterator[Any]:
                    try:
                        if first_batch is not None:
                            yield first_batch
                        yield from opened
                    finally:
                        _close(opened)

                schema = pa.schema([]) if first_batch is None else first_batch.schema
                reader = pa.RecordBatchReader.from_batches(schema, batches())
                try:
                    return reader.__arrow_c_stream__(requested_schema)
                except BaseException:
                    _close(opened)
                    _close(reader)
                    raise
        return self.to_arrow().__arrow_c_stream__(requested_schema)

    def polars_batches(self, *, batch_size: int = 65_536, schema: Any = None) -> Flow[Any]:
        """Return a lazy Flow of Polars DataFrames converted from bounded Arrow batches.

        Args:
            batch_size: Maximum source rows represented by each emitted DataFrame.
            schema: Optional Arrow schema fixing conversion fields and types.

        Returns:
            A Flow that preserves batch order and inherits upstream cleanup behavior.
        """
        pl = polars_module()
        return self.arrow_batches(batch_size=batch_size, schema=schema).map(
            lambda batch: pl.from_arrow(batch, rechunk=False)
        )

    to_polars_batches = polars_batches

    def to_polars(self, *, batch_size: int = 65_536, schema: Any = None) -> Any:
        """Materialize all rows as one Polars DataFrame through Arrow.

        Args:
            batch_size: Maximum rows converted in each intermediate Arrow batch.
            schema: Optional Arrow schema fixing conversion fields and types.

        Returns:
            A non-rechunked Polars DataFrame containing the fully consumed pipeline.
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
        spreadsheet_safe: bool = False,
    ) -> None:
        """Consume rows into a CSV file incrementally and return None.

        Args:
            path: Destination file opened for text overwrite.
            fieldnames: Output order; inferred from the first row when omitted.
            encoding: Text encoding used to write the destination.
            include_header: Write fieldnames before data rows when true.
            extrasaction: Raise or ignore fields absent from fieldnames.
            spreadsheet_safe: Prefix cells that spreadsheet software may execute.
        """
        if extrasaction not in {"raise", "ignore"}:
            raise ValueError("extrasaction must be 'raise' or 'ignore'")
        names = tuple(fieldnames) if fieldnames is not None else None
        if names is not None and not names:
            raise ValueError("fieldnames cannot be empty")
        if names is not None:
            _require_unique_names(names, operation="to_csv")

        iterator = iter(self)
        with (
            closing_iterators((iterator,)),
            open(path, "w", encoding=encoding, newline="") as handle,
        ):
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
            writer.writerow(
                {name: spreadsheet_safe_cell(value) for name, value in first.items()}
                if spreadsheet_safe
                else first
            )
            for row in iterator:
                record = _as_record(row)
                writer.writerow(
                    {name: spreadsheet_safe_cell(value) for name, value in record.items()}
                    if spreadsheet_safe
                    else record
                )

    def to_jsonl(
        self,
        path: str | os.PathLike[str],
        *,
        encoding: str = "utf-8",
        ensure_ascii: bool = False,
    ) -> None:
        """Consume rows into an overwritten file as one JSON object per line.

        Args:
            path: Destination JSON Lines file.
            encoding: Text encoding used to write the destination.
            ensure_ascii: Escape non-ASCII code points when true.
        """
        iterator = iter(self)
        with closing_iterators((iterator,)), open(path, "w", encoding=encoding) as handle:
            for row in iterator:
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
        """Stream rows to a local temporary Parquet file and publish it atomically.

        The destination changes only after all bounded row groups are written;
        failures remove the temporary file.

        Args:
            path: Local destination path; URI-style paths are rejected.
            if_exists: Error before consuming rows, or atomically replace the target.
            batch_size: Maximum rows converted and written per row group.
            schema: Optional Arrow schema, required to write an empty pipeline.
            compression: Compression setting passed to PyArrow ParquetWriter.
            use_dictionary: Dictionary-encoding setting passed to ParquetWriter.
            write_statistics: Column-statistics setting passed to ParquetWriter.
            writer_options: Extra ParquetWriter options except reserved arguments.

        Returns:
            The number of source rows published.
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
        """Submit mapped rows with DB-API executemany batches in one transaction.

        Commit after all rows succeed or roll back on error; always close the
        iterator, cursor, and connection.

        Args:
            connect: Zero-argument factory for the transaction's connection.
            statement: Statement passed to cursor.executemany().
            parameters: Optional callable mapping each row to bound parameters.
            batch_size: Maximum parameter sets in each executemany() call.

        Returns:
            The number of source rows submitted, independent of cursor.rowcount.
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
        """Insert rows into a SQLite table in bounded batches and one transaction.

        Replace-mode DDL and inserts roll back together; all owned resources
        close on every exit path.

        Args:
            database: SQLite path or URI passed to sqlite3.connect().
            table: Destination table identifier, validated and quoted as data.
            if_exists: Append, fail if present, or transactionally replace the table.
            conflict: Use ordinary INSERT, INSERT OR IGNORE, or INSERT OR REPLACE.
            columns: Optional ordered projection; extra source fields are omitted.
            schema: Optional mapping from field names to supported SQLite type names.
            batch_size: Maximum bindings in each executemany() call.
            timeout: Seconds sqlite3 waits for a locked database.
            uri: Interpret database as a SQLite URI when true.

        Returns:
            Source records submitted, including records ignored by SQLite conflicts.
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
