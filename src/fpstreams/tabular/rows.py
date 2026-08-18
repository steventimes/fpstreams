"""Record-oriented pipelines and tabular data operations."""

from __future__ import annotations

import csv
import json
import os
from collections.abc import Callable, Iterable, Iterator, Mapping
from typing import Any, Generic, Literal, TypeVar

from ..collecting.aggregation import (
    Aggregator,
    prepare_aggregations,
    run_aggregations,
)
from ..errors import BufferLimitError, DuplicateKeyError, SelectionError
from ..expressions.row import RowExpr, lit
from ..expressions.selectors import Selector, compile_selector
from ..io_safety import validate_max_record_bytes
from ..planning.arrow import PlannedRowCallable
from ..planning.sync import Engine
from ..streams.flow import Flow, flow
from .arrow import (
    arrow_source,
    parquet_row_factory,
)
from .dataframe import dataframe_row_factory
from .grouped import GroupedRows
from .io import RowsIOMixin
from .join import JoinSelector, JoinValidation, _build_join
from .polars import polars_row_factory
from .records import _as_record, _require_unique_names
from .spill_limits import SpillLimits
from .sql import (
    ConnectionFactory,
    DBParameters,
    db_row_factory,
    sqlite_row_factory,
)

T = TypeVar("T")


class Rows(RowsIOMixin[T], Generic[T]):
    """A lazy record pipeline with joins, grouping, and data-system adapters."""

    __slots__ = ("_flow",)

    def __init__(self, source: Iterable[T] | Flow[T]) -> None:
        """Wrap an existing Flow or convert an iterable into a lazy row pipeline."""
        self._flow = source if isinstance(source, Flow) else flow(source)

    @staticmethod
    def from_csv(
        path: str | os.PathLike[str],
        *,
        encoding: str = "utf-8",
        **format_parameters: Any,
    ) -> Rows[dict[str, Any]]:
        """Return reusable rows that reopen a CSV file and validate its header when iterated.

        Args:
            path: CSV input file opened on each iteration.
            encoding: Text encoding used by the CSV reader.
            **format_parameters: Keyword options forwarded to csv.DictReader, such as
                dialect, delimiter, or quoting.

        Returns:
            A lazy Rows pipeline of dictionaries keyed by the unique CSV header.
        """

        def records() -> Iterator[dict[str, Any]]:
            """Open the CSV on each iteration and yield dictionaries after validating its header."""
            with open(path, encoding=encoding, newline="") as handle:
                reader = csv.DictReader(handle, **format_parameters)
                _require_unique_names(reader.fieldnames or (), operation="CSV header")
                for record in reader:
                    yield dict(record)

        return Rows(flow.defer(records))

    @staticmethod
    def from_jsonl(
        path: str | os.PathLike[str],
        *,
        encoding: str = "utf-8",
        max_record_bytes: int | None = 8 * 1024 * 1024,
    ) -> Rows[dict[str, Any]]:
        """Return reusable rows that decode nonblank JSON objects from a file on demand.

        Args:
            path: JSON Lines input file reopened for each iteration.
            encoding: Encoding applied after each physical line passes its byte limit.
            max_record_bytes: Encoded-byte limit per line, or None for no limit.

        Returns:
            Lazy dictionary rows; duplicate keys and non-object records fail when consumed.
        """

        record_limit = validate_max_record_bytes(max_record_bytes)

        def records() -> Iterator[dict[str, Any]]:
            """Read bounded lines, decode JSON objects, and yield validated dictionaries."""
            with open(path, "rb") as handle:
                line_number = 0
                while True:
                    encoded = (
                        handle.readline()
                        if record_limit is None
                        else handle.readline(record_limit + 1)
                    )
                    if not encoded:
                        return
                    line_number += 1
                    if record_limit is not None and len(encoded) > record_limit:
                        raise BufferLimitError(
                            f"JSON Lines record {line_number} bytes {len(encoded)} exceed "
                            f"max_record_bytes={record_limit}"
                        )
                    line = encoded.decode(encoding)
                    if not line.strip():
                        continue

                    def unique_object(
                        pairs: list[tuple[str, Any]],
                        *,
                        record_number: int = line_number,
                    ) -> dict[str, Any]:
                        """Build one JSON object while rejecting duplicate keys before loss."""
                        value: dict[str, Any] = {}
                        for name, item in pairs:
                            if name in value:
                                raise DuplicateKeyError(
                                    "JSON Lines record "
                                    f"{record_number} contains duplicate key {name!r}"
                                )
                            value[name] = item
                        return value

                    value = json.loads(line, object_pairs_hook=unique_object)
                    if not isinstance(value, Mapping):
                        raise SelectionError(f"JSON Lines record {line_number} is not an object")
                    yield dict(value)

        return Rows(flow.defer(records))

    @staticmethod
    def from_arrow(source: Any, *, batch_size: int = 65_536) -> Rows[dict[str, Any]]:
        """Adapt a PyArrow Table, RecordBatch, or RecordBatchReader to dictionary rows.

        Args:
            source: Reusable Table/RecordBatch or one-shot RecordBatchReader.
            batch_size: Maximum rows converted from each Arrow batch slice.

        Returns:
            Lazy Rows; a RecordBatchReader may be consumed only once and is closed afterward.
        """
        return Rows(Flow(arrow_source(source, batch_size=batch_size)))

    @staticmethod
    def from_dataframe(
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
        return Rows(
            flow.defer(
                dataframe_row_factory(
                    frame,
                    batch_size=batch_size,
                    allow_copy=allow_copy,
                )
            )
        )

    from_pandas = from_dataframe

    @staticmethod
    def from_polars(
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
        return Rows(
            flow.defer(
                polars_row_factory(
                    frame,
                    batch_size=batch_size,
                    maintain_order=maintain_order,
                    engine=engine,
                )
            )
        )

    @staticmethod
    def from_parquet(
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
        return Rows(
            flow.defer(
                parquet_row_factory(
                    source,
                    columns=columns,
                    filter=filter,
                    batch_size=batch_size,
                    use_threads=use_threads,
                    filesystem=filesystem,
                    partitioning=partitioning,
                )
            )
        )

    @staticmethod
    def from_db(
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
        return Rows(
            flow.defer(
                db_row_factory(
                    connect,
                    query,
                    parameters,
                    batch_size=batch_size,
                )
            )
        )

    @staticmethod
    def from_sqlite(
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
        return Rows(
            flow.defer(
                sqlite_row_factory(
                    database,
                    query,
                    parameters,
                    batch_size=batch_size,
                    timeout=timeout,
                    uri=uri,
                )
            )
        )

    def __iter__(self) -> Iterator[T]:
        """Execute the underlying Flow and yield records lazily."""
        return iter(self._flow)

    def to_list(self) -> list[T]:
        """Execute the record pipeline and collect its rows.

        Returns:
            A list containing the consumed results in encounter order.
        """
        return self._flow.to_list()

    def count(self) -> int:
        """Consume the pipeline and count every emitted row.

        Returns:
            The number of rows remaining after all lazy transformations.
        """
        return self._flow.count()

    def with_engine(self, engine: Engine) -> Rows[T]:
        """Return equivalent lazy Rows requesting auto, Python, or native Flow execution."""
        return Rows(self._flow.with_engine(engine))

    def first(self) -> T:
        """Return the first row and close upstream without requesting an unnecessary tail.

        Returns:
            The first emitted row.

        Raises:
            EmptyFlowError: If the pipeline emits no rows.
        """
        return self._flow.first()

    def last(self) -> T:
        """Consume the pipeline and return its final row, raising EmptyFlowError when empty.

        Returns:
            The last emitted row.
        """
        return self._flow.last()

    def take(self, count: int) -> Rows[T]:
        """Return a lazy prefix that stops and closes upstream after at most count rows.

        Args:
            count: Nonnegative maximum number of rows to emit.

        Returns:
            New Rows preserving encounter order; zero emits nothing.
        """
        return Rows(self._flow.take(count))

    limit = take
    head = take

    def skip(self, count: int) -> Rows[T]:
        """Return lazy rows after discarding the first count upstream items.

        Args:
            count: Nonnegative number of rows to consume without emitting.

        Returns:
            New Rows containing the remaining encounter-ordered rows.
        """
        return Rows(self._flow.drop(count))

    offset = skip

    def unique_by(self, selector: Selector) -> Rows[T]:
        """Keep the first row for each distinct selected key in encounter order.

        Args:
            selector: Field, path, index, expression, or callable producing a hashable key.

        Returns:
            Lazy Rows whose later duplicate keys are omitted.
        """
        return Rows(self._flow.unique_by(selector))

    distinct_by = unique_by

    def filter(self, predicate: Callable[[T], bool]) -> Rows[T]:
        """Keep rows for which predicate returns a truthy result.

        The predicate runs lazily in encounter order, and the parent Rows pipeline remains
        unchanged.

        Args:
            predicate: Callable evaluated once for each upstream row reached.

        Returns:
            New lazy Rows containing only matching rows.
        """
        return Rows(self._flow.filter(predicate))

    def where(self, predicate: Callable[[T], bool] | None = None, **equalities: Any) -> Rows[T]:
        """Require the optional predicate and every named field equality.

        Named fields are compiled once, then selected lazily from each consumed row.

        Args:
            predicate: Optional callable that must return truthy for a row.
            **equalities: Top-level or dotted field paths mapped to required values.

        Returns:
            New lazy Rows containing rows that satisfy all supplied conditions.
        """
        # Compile equality selectors once; evaluation still happens lazily per row.
        selectors = [(compile_selector(name), expected) for name, expected in equalities.items()]

        def matches(row: T) -> bool:
            """Require both the optional predicate and every named equality to match."""
            if predicate is not None and not predicate(row):
                return False
            return all(select(row) == expected for select, expected in selectors)

        return self.filter(matches)

    def with_columns(self, **columns: Selector) -> Rows[dict[str, Any]]:
        """Copy each row and add or replace fields evaluated against the original row.

        Args:
            **columns: Output field names mapped to selectors or RowExpr values.

        Returns:
            Lazy dictionary Rows; computed columns do not observe earlier additions.
        """
        selectors = [(name, compile_selector(selector)) for name, selector in columns.items()]

        def enrich(row: T) -> dict[str, Any]:
            """Copy a row to a dictionary and evaluate each new column against the original row."""
            record = _as_record(row)
            for name, select in selectors:
                record[name] = select(row)
            return record

        return Rows(self._flow.map(PlannedRowCallable(enrich)))

    def rename(self, **columns: str) -> Rows[dict[str, Any]]:
        """Rename top-level fields while rejecting collisions in each output record.

        Args:
            **columns: Existing field names mapped to nonempty destination names.

        Returns:
            Lazy copied dictionaries; unmapped fields retain their names and order.
        """
        if any(not name for name in columns.values()):
            raise ValueError("renamed columns cannot be empty")

        def transform(row: T) -> dict[str, Any]:
            """Rename fields while detecting collisions in the resulting record."""
            renamed: dict[str, Any] = {}
            for name, value in _as_record(row).items():
                target = columns.get(name, name)
                if target in renamed:
                    raise ValueError(f"rename creates duplicate column {target!r}")
                renamed[target] = value
            return renamed

        return Rows(self._flow.map(transform))

    def drop(self, *columns: str) -> Rows[dict[str, Any]]:
        """Copy each row without the named top-level fields.

        Args:
            *columns: Field names to omit; absent names are ignored.

        Returns:
            Lazy dictionary Rows preserving the order of retained fields.
        """
        names = frozenset(columns)
        return Rows(
            self._flow.map(
                lambda row: {
                    name: value for name, value in _as_record(row).items() if name not in names
                }
            )
        )

    def cast(self, **columns: Callable[[Any], Any]) -> Rows[dict[str, Any]]:
        """Convert existing named fields with one callable per field.

        Args:
            **columns: Field names mapped to callable value converters.

        Returns:
            Lazy copied dictionaries; a missing field raises SelectionError when consumed.
        """
        if not columns:
            raise ValueError("cast requires at least one named converter")
        if any(not name for name in columns):
            raise ValueError("cast column names cannot be empty")
        for name, converter in columns.items():
            if not callable(converter):
                raise TypeError(f"cast converter for {name!r} must be callable")

        def transform(row: T) -> dict[str, Any]:
            """Apply each converter to its named field and reject missing columns."""
            record = _as_record(row)
            for name, converter in columns.items():
                if name not in record:
                    raise SelectionError(f"cast column {name!r} is missing")
                record[name] = converter(record[name])
            return record

        return Rows(self._flow.map(transform))

    parse = cast

    def fill_nulls(self, **replacements: object) -> Rows[dict[str, Any]]:
        """Replace missing or None named fields with constants or RowExpr results.

        Args:
            **replacements: Field names mapped to literal values or row expressions.

        Returns:
            Lazy copied dictionaries; non-None existing values are preserved.
        """
        if not replacements:
            raise ValueError("fill_nulls requires at least one named replacement")
        if any(not name for name in replacements):
            raise ValueError("fill_nulls column names cannot be empty")
        expressions = tuple(
            (
                name,
                value if isinstance(value, RowExpr) else lit(value),
            )
            for name, value in replacements.items()
        )

        def transform(row: T) -> dict[str, Any]:
            """Replace selected None fields by evaluating their replacement expressions."""
            record = _as_record(row)
            for name, replacement in expressions:
                if record.get(name) is None:
                    record[name] = replacement(row)
            return record

        return Rows(self._flow.map(transform))

    fillna = fill_nulls

    def drop_nulls(
        self,
        *selectors: Selector,
        how: Literal["any", "all"] = "any",
    ) -> Rows[T]:
        """Drop rows according to None values in selected fields or the whole record.

        Args:
            *selectors: Fields to inspect; omitted means every field in each record.
            how: "any" drops on one null; "all" requires every inspected value to be null.

        Returns:
            Lazy Rows; a missing selected field is treated as None.
        """
        if how not in {"any", "all"}:
            raise ValueError("drop_nulls how must be 'any' or 'all'")
        selected = tuple(compile_selector(selector) for selector in selectors)

        def select_or_none(select: Callable[[T], Any], row: T) -> Any:
            """Treat a missing selected field as None for null filtering."""
            try:
                return select(row)
            except SelectionError:
                return None

        def keep(row: T) -> bool:
            """Keep a row unless its selected values satisfy the requested null policy."""
            values = (
                (select_or_none(select, row) for select in selected)
                if selected
                else _as_record(row).values()
            )
            missing = (value is None for value in values)
            return not (any(missing) if how == "any" else all(missing))

        return self.filter(keep)

    dropna = drop_nulls

    def explode(
        self,
        selector: Selector,
        *,
        into: str | None = None,
        outer: bool = False,
    ) -> Rows[dict[str, Any]]:
        """Expand a selected iterable into one copied row per element.

        Args:
            selector: Selector returning a non-string iterable or None.
            into: Output field name; required for non-top-level selectors.
            outer: Emit one row with None when the selected value is None or empty.

        Returns:
            Lazy flattened dictionary Rows that close upstream on downstream stop.
        """
        if into is None:
            if not isinstance(selector, str) or not selector or "." in selector:
                raise ValueError("explode into is required for non-top-level selectors")
            output_name = selector
        else:
            if not into:
                raise ValueError("explode output name cannot be empty")
            output_name = into
        select = compile_selector(selector)

        def expand(row: T) -> Iterator[dict[str, Any]]:
            """Yield one record per selected element, optionally preserving empty rows."""
            record = _as_record(row)
            values = select(row)
            if values is None:
                if outer:
                    record[output_name] = None
                    yield record
                return
            if isinstance(values, (str, bytes, bytearray, Mapping)) or not isinstance(
                values, Iterable
            ):
                raise TypeError("explode selector must return a non-string iterable or None")

            emitted = False
            for value in values:
                emitted = True
                expanded = record.copy()
                expanded[output_name] = value
                yield expanded
            if outer and not emitted:
                record[output_name] = None
                yield record

        return Rows(self._flow.flat_map(expand))

    def unnest(self, column: str, *, prefix: str = "") -> Rows[dict[str, Any]]:
        """Replace one top-level nested record with its fields.

        Args:
            column: Non-dotted field name containing a supported record-like value.
            prefix: Text prepended to every promoted nested field.

        Returns:
            Lazy copied dictionaries; output-name collisions raise DuplicateKeyError.
        """
        if not column or "." in column:
            raise ValueError("unnest column must be a top-level name")

        def expand(row: T) -> dict[str, Any]:
            """Remove the nested column and merge its fields into the top-level record."""
            record = _as_record(row)
            try:
                nested_value = record.pop(column)
            except KeyError:
                raise SelectionError(f"unnest column {column!r} is missing") from None
            nested = _as_record(nested_value)
            for name, value in nested.items():
                target = f"{prefix}{name}"
                if target in record:
                    raise DuplicateKeyError(
                        f"unnest output column {target!r} collides with an existing column"
                    )
                record[target] = value
            return record

        return Rows(self._flow.map(expand))

    def unpivot(
        self,
        *columns: str,
        names_to: str = "variable",
        values_to: str = "value",
    ) -> Rows[dict[str, Any]]:
        """Convert selected top-level fields from wide form into name/value rows.

        Args:
            *columns: Unique fields expanded in the given order.
            names_to: Noncolliding output field for each former column name.
            values_to: Noncolliding output field for each former column value.

        Returns:
            Lazy Rows emitting len(columns) records per input row.
        """
        if not columns:
            raise ValueError("unpivot requires at least one column")
        _require_unique_names(columns, operation="unpivot")
        _require_unique_names((names_to, values_to), operation="unpivot")

        def reshape(row: T) -> Iterable[dict[str, Any]]:
            """Yield one name/value record for each column selected from a wide row."""
            record = _as_record(row)
            missing = [name for name in columns if name not in record]
            if missing:
                raise SelectionError(f"missing columns for unpivot: {missing!r}")
            base = {name: value for name, value in record.items() if name not in columns}
            if names_to in base or values_to in base:
                raise DuplicateKeyError("unpivot output names collide with existing columns")
            return ({**base, names_to: name, values_to: record[name]} for name in columns)

        return Rows(self._flow.flat_map(reshape))

    def pivot(
        self,
        *,
        index: Selector | tuple[Selector, ...],
        columns: Selector,
        values: Selector,
        aggregate: str | Callable[[Any, Any], Any] = "error",
        fill: Any = None,
    ) -> Rows[dict[str, Any]]:
        """Materialize long-form rows into encounter-ordered wide records.

        Args:
            index: Selector or selector tuple defining each output row and its key fields.
            columns: Selector whose values become dynamic output field names.
            values: Selector producing each pivot cell value.
            aggregate: Duplicate-cell policy: error, first, last, sum, or a reducer callable.
            fill: Value inserted for missing cells among discovered columns.

        Returns:
            Lazy pipeline that builds the full pivot only when consumed.
        """
        reducers = {"error", "first", "last", "sum"}
        if not callable(aggregate) and aggregate not in reducers:
            raise ValueError(f"aggregate must be callable or one of {sorted(reducers)!r}")

        index_fields = index if isinstance(index, tuple) else (index,)
        if not index_fields:
            raise ValueError("pivot index requires at least one selector")
        key_selectors = [compile_selector(selector) for selector in index_fields]
        key_names = [
            selector.split(".")[-1] if isinstance(selector, str) else f"key_{position}"
            for position, selector in enumerate(index_fields)
        ]
        _require_unique_names(key_names, operation="pivot")
        column_selector = compile_selector(columns)
        value_selector = compile_selector(values)

        def evaluate() -> Iterator[dict[str, Any]]:
            """Build pivot cells by index key, apply duplicate policy, and emit wide rows."""
            groups: dict[tuple[Any, ...], dict[str, Any]] = {}
            column_names: list[str] = []
            for row in self:
                key = tuple(select(row) for select in key_selectors)
                column = str(column_selector(row))
                if column in key_names:
                    raise ValueError(f"pivot column {column!r} collides with an index column")
                if column not in column_names:
                    column_names.append(column)
                cells = groups.setdefault(key, {})
                value = value_selector(row)
                if column not in cells:
                    cells[column] = value
                elif callable(aggregate):
                    cells[column] = aggregate(cells[column], value)
                elif aggregate == "error":
                    raise DuplicateKeyError(
                        f"multiple values for pivot key {key!r}, column {column!r}"
                    )
                elif aggregate == "last":
                    cells[column] = value
                elif aggregate == "sum":
                    cells[column] += value

            for key, cells in groups.items():
                record = dict(zip(key_names, key, strict=True))
                record.update({name: cells.get(name, fill) for name in column_names})
                yield record

        return Rows(flow.defer(evaluate))

    def select(self, *selectors: str | int, **named: Selector) -> Rows[dict[str, Any]]:
        """Project positional and named selectors into new dictionaries.

        Args:
            *selectors: String paths or integer indexes; output names are derived automatically.
            **named: Explicit output names mapped to any supported selector.

        Returns:
            Lazy projected Rows; duplicate derived or explicit names are rejected immediately.
        """
        positional = [
            (
                selector.split(".")[-1] if isinstance(selector, str) else str(selector),
                compile_selector(selector),
            )
            for selector in selectors
        ]
        aliases = [(name, compile_selector(selector)) for name, selector in named.items()]
        _require_unique_names(
            (name for name, _select in (*positional, *aliases)),
            operation="select",
        )

        def project(row: T) -> dict[str, Any]:
            """Evaluate positional and named selectors into a new projected dictionary."""
            return {name: select(row) for name, select in (*positional, *aliases)}

        return Rows(self._flow.map(PlannedRowCallable(project)))

    def sort_by(
        self,
        selector: Selector,
        *,
        reverse: bool = False,
        buffer_size: int | None = None,
        tempdir: str | os.PathLike[str] | None = None,
    ) -> Rows[T]:
        """Sort rows by a selected key, in memory or through bounded external runs.

        Args:
            selector: Field, path, index, expression, or callable producing the sort key.
            reverse: Emit descending order when true.
            buffer_size: None for full in-memory sort, or positive rows per spilled run.
            tempdir: Parent directory for automatically cleaned external-sort files.

        Returns:
            Lazy stably sorted Rows.
        """
        return Rows(
            self._flow.sort_by(
                selector,
                reverse=reverse,
                buffer_size=buffer_size,
                tempdir=tempdir,
            )
        )

    def external_sort_by(
        self,
        selector: Selector,
        *,
        reverse: bool = False,
        buffer_size: int = 100_000,
        tempdir: str | os.PathLike[str] | None = None,
    ) -> Rows[T]:
        """Sort rows stably with bounded in-memory runs and temporary files.

        Each run holds at most buffer_size rows; the lazy merge closes upstream and removes
        temporary files after completion, failure, or downstream short-circuit.

        Args:
            selector: Field, path, index, expression, or callable producing the sort key.
            reverse: Emit descending order when true.
            buffer_size: Positive maximum rows held in each sorted run.
            tempdir: Parent directory for automatically cleaned run files.

        Returns:
            Lazy externally sorted Rows.
        """
        return self.sort_by(
            selector,
            reverse=reverse,
            buffer_size=buffer_size,
            tempdir=tempdir,
        )

    def aggregate(self, **aggregations: Aggregator) -> Rows[dict[str, Any]]:
        """Run named Aggregators and return a one-row pipeline.

        The computation is deferred and produces a `Rows` pipeline containing one result record.

        Args:
            **aggregations: Named aggregators evaluated during the same traversal.

        Returns:
            A lazy one-row pipeline containing the named results.
        """
        aggregation_items = prepare_aggregations(aggregations)

        def evaluate() -> Iterator[dict[str, Any]]:
            """Run all named aggregators in one pass and emit their results as one row."""
            yield run_aggregations(self, aggregation_items)

        return Rows(flow.defer(evaluate))

    def group_by(self, *selectors: Selector, **named: Selector) -> GroupedRows[T]:
        """Describe grouped aggregation by positional and/or explicitly named selectors.

        Args:
            *selectors: Keys named from field paths or as key_N for other selector types.
            **named: Explicit output key names mapped to supported selectors.

        Returns:
            GroupedRows configuration; no source rows are read until aggregate() is consumed.
        """
        # Grouping remains deferred; rows are read only when aggregate() is consumed.
        if not selectors and not named:
            raise ValueError("group_by requires at least one selector")
        positional = tuple(
            (
                (selector.split(".")[-1] if isinstance(selector, str) else f"key_{position}"),
                selector,
            )
            for position, selector in enumerate(selectors)
        )
        keys = (*positional, *named.items())
        if any(not name for name, _selector in keys):
            raise ValueError("group_by names cannot be empty")
        _require_unique_names(
            (name for name, _selector in keys),
            operation="group_by",
        )
        return GroupedRows(self, keys)

    def join(
        self,
        other: Iterable[Any] | Rows[Any],
        *,
        on: JoinSelector | None = None,
        left_on: JoinSelector | None = None,
        right_on: JoinSelector | None = None,
        how: str = "inner",
        suffix: str = "_right",
        validate: JoinValidation = "m:m",
        partitions: int | None = None,
        tempdir: str | os.PathLike[str] | None = None,
        limits: SpillLimits | None = None,
    ) -> Rows[dict[str, Any]]:
        """Join this record pipeline with another source.

        Joins are lazy and preserve stable input order. Inner, left, semi, and anti joins stream
        the left source after indexing the right source. Right and full joins materialize both
        sides. Set partitions to use bounded-memory hash partitioning through temporary files.

        Args:
            other: The record iterable or Rows pipeline to join.
            on: A selector used for both left and right keys.
            left_on: The left key selector when the two sides use different fields.
            right_on: The right key selector when the two sides use different fields.
            how: One of inner, left, right, full, semi, or anti.
            suffix: Text appended to conflicting right-side field names.
            validate: Expected key cardinality. 1:m requires unique left keys, m:1 requires
                unique right keys, 1:1 requires both, and m:m permits duplicates on both sides.
            partitions: Number of hash partitions for bounded-memory execution. Must be between
                2 and 256.
            tempdir: Parent directory for temporary partition files. Requires partitions.
            limits: Finite partition, match, and output budgets for spilled execution.

        Returns:
            Lazy dictionary Rows that execute the selected in-memory or spilled join when consumed.

        Raises:
            ValueError: If selectors, modes, partition options, or key cardinality are invalid.
            TypeError: If a key is unhashable or spilled data cannot be serialized.
            DuplicateKeyError: If suffixing would create an ambiguous output field.
            BufferLimitError: If spilled execution exceeds a configured resource budget.
        """
        return Rows(
            flow.defer(
                _build_join(
                    self,
                    other,
                    on=on,
                    left_on=left_on,
                    right_on=right_on,
                    how=how,
                    suffix=suffix,
                    validate=validate,
                    partitions=partitions,
                    tempdir=tempdir,
                    limits=limits,
                )
            )
        )
