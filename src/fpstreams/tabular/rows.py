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
from ..errors import DuplicateKeyError, SelectionError
from ..expressions.row import RowExpr, lit
from ..expressions.selectors import Selector, compile_selector
from ..streams.flow import Flow, flow
from .arrow import (
    arrow_row_source,
    parquet_row_factory,
)
from .dataframe import dataframe_row_factory
from .grouped import GroupedRows
from .io import RowsIOMixin
from .join import JoinSelector, JoinValidation, _build_join
from .polars import polars_row_factory
from .records import _as_record, _require_unique_names
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
        self._flow = source if isinstance(source, Flow) else flow(source)

    @staticmethod
    def from_csv(
        path: str | os.PathLike[str],
        *,
        encoding: str = "utf-8",
        **format_parameters: Any,
    ) -> Rows[dict[str, Any]]:
        """Read CSV rows lazily as dictionaries.

        Args:
            path: The filesystem path to read from or write to.
            encoding: The text encoding used to open the file.
            **format_parameters: Additional keyword arguments passed to the underlying
                file-format reader.

        Returns:
            A new lazy `Rows` pipeline representing this operation.
        """

        def records() -> Iterator[dict[str, Any]]:
            with open(path, encoding=encoding, newline="") as handle:
                reader = csv.DictReader(handle, **format_parameters)
                _require_unique_names(reader.fieldnames or (), operation="CSV header")
                for record in reader:
                    yield dict(record)

        return Rows(flow.defer(records))

    @staticmethod
    def from_jsonl(
        path: str | os.PathLike[str], *, encoding: str = "utf-8"
    ) -> Rows[dict[str, Any]]:
        """Read one JSON object per line lazily.

        Args:
            path: The filesystem path to read from or write to.
            encoding: The text encoding used to open the file.

        Returns:
            A new lazy `Rows` pipeline representing this operation.
        """

        def records() -> Iterator[dict[str, Any]]:
            with open(path, encoding=encoding) as handle:
                for line_number, line in enumerate(handle, 1):
                    if not line.strip():
                        continue

                    def unique_object(
                        pairs: list[tuple[str, Any]],
                        *,
                        record_number: int = line_number,
                    ) -> dict[str, Any]:
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
        """Create rows from an Arrow table, batch, reader, dataset, or stream.

        Args:
            source: The iterable, async iterable, or data source to read lazily.
            batch_size: The maximum number of rows processed in each batch.

        Returns:
            A new lazy `Rows` pipeline representing this operation.
        """
        factory, reiterable = arrow_row_source(source, batch_size=batch_size)
        return Rows(flow.defer(factory) if reiterable else flow(factory()))

    @staticmethod
    def from_dataframe(
        frame: Any,
        *,
        batch_size: int = 65_536,
        allow_copy: bool = True,
    ) -> Rows[dict[str, Any]]:
        """Create rows from an object implementing the dataframe interchange protocol.

        Args:
            frame: The dataframe-like object used as the row source.
            batch_size: The maximum number of rows processed in each batch.
            allow_copy: Whether an adapter may copy data when zero-copy conversion is unavailable.

        Returns:
            A new lazy `Rows` pipeline representing this operation.
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
        """Create rows from a Polars DataFrame or LazyFrame.

        Args:
            frame: The dataframe-like object used as the row source.
            batch_size: The maximum number of rows processed in each batch.
            maintain_order: Whether output must preserve the source row order.
            engine: The execution engine requested for this pipeline.

        Returns:
            A new lazy `Rows` pipeline representing this operation.
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
        """Read Parquet batches lazily through PyArrow.

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
        """Run a DB-API query and stream rows in batches.

        Args:
            connect: A zero-argument callable that opens a new database connection.
            query: The SQL query executed for each fresh iteration.
            parameters: Parameters passed to the database query or statement.
            batch_size: The maximum number of rows processed in each batch.

        Returns:
            A new lazy `Rows` pipeline representing this operation.
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
        return iter(self._flow)

    def to_list(self) -> list[T]:
        """Execute the record pipeline and collect its rows.

        Returns:
            A list containing the consumed results in encounter order.
        """
        return self._flow.to_list()

    def count(self) -> int:
        """Count all rows produced by the pipeline.

        Returns:
            The number of matching input items.
        """
        return self._flow.count()

    def first(self) -> T:
        """Return the first row, a default, or raise EmptyFlowError.

        Returns:
            The first row.

        Raises:
            EmptyFlowError: If the pipeline is empty and no default is supplied.
        """
        return self._flow.first()

    def last(self) -> T:
        """Return the last row, a default, or raise EmptyFlowError.

        Returns:
            The last row.
        """
        return self._flow.last()

    def take(self, count: int) -> Rows[T]:
        """Keep at most count rows.

        Args:
            count: The requested number of items.

        Returns:
            A new lazy `Rows` pipeline representing this operation.
        """
        return Rows(self._flow.take(count))

    limit = take
    head = take

    def skip(self, count: int) -> Rows[T]:
        """Skip count rows; alias of drop.

        Args:
            count: The requested number of items.

        Returns:
            A new lazy `Rows` pipeline representing this operation.
        """
        return Rows(self._flow.drop(count))

    offset = skip

    def unique_by(self, selector: Selector) -> Rows[T]:
        """Keep the first row for each selected key.

        Args:
            selector: A callable, field name, index, path, or expression used to select a value.

        Returns:
            A new lazy `Rows` pipeline representing this operation.
        """
        return Rows(self._flow.unique_by(selector))

    distinct_by = unique_by

    def filter(self, predicate: Callable[[T], bool]) -> Rows[T]:
        """Keep rows for which predicate returns true.

        Rows remain lazy; the predicate is evaluated only while the returned pipeline is
        consumed.

        Args:
            predicate: A callable that decides whether an item matches.

        Returns:
            A new lazy `Rows` pipeline representing this operation.
        """
        return Rows(self._flow.filter(predicate))

    def where(self, predicate: Callable[[T], bool] | None = None, **equalities: Any) -> Rows[T]:
        """Filter rows with a predicate and/or field equalities.

        A row must satisfy the optional predicate and every supplied field equality.

        Args:
            predicate: A callable that decides whether an item matches.
            **equalities: Field names mapped to values that rows must equal.

        Returns:
            A new lazy `Rows` pipeline representing this operation.
        """
        # Compile equality selectors once; evaluation still happens lazily per row.
        selectors = [(compile_selector(name), expected) for name, expected in equalities.items()]

        def matches(row: T) -> bool:
            if predicate is not None and not predicate(row):
                return False
            return all(select(row) == expected for select, expected in selectors)

        return self.filter(matches)

    def with_columns(self, **columns: Selector) -> Rows[dict[str, Any]]:
        """Add or replace fields using selectors and row expressions.

        Args:
            **columns: Names mapped to selectors, expressions, or replacement values.

        Returns:
            A new lazy `Rows` pipeline representing this operation.
        """
        selectors = [(name, compile_selector(selector)) for name, selector in columns.items()]

        def enrich(row: T) -> dict[str, Any]:
            record = _as_record(row)
            for name, select in selectors:
                record[name] = select(row)
            return record

        return Rows(self._flow.map(enrich))

    def rename(self, **columns: str) -> Rows[dict[str, Any]]:
        """Rename fields and reject duplicate output names.

        Args:
            **columns: Names mapped to selectors, expressions, or replacement values.

        Returns:
            A new lazy `Rows` pipeline representing this operation.
        """
        if any(not name for name in columns.values()):
            raise ValueError("renamed columns cannot be empty")

        def transform(row: T) -> dict[str, Any]:
            renamed: dict[str, Any] = {}
            for name, value in _as_record(row).items():
                target = columns.get(name, name)
                if target in renamed:
                    raise ValueError(f"rename creates duplicate column {target!r}")
                renamed[target] = value
            return renamed

        return Rows(self._flow.map(transform))

    def drop(self, *columns: str) -> Rows[dict[str, Any]]:
        """Remove the named fields from each row.

        Args:
            *columns: Column names selected by the operation.

        Returns:
            A new lazy `Rows` pipeline representing this operation.
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
        """Convert selected field values with the supplied callables.

        Args:
            **columns: Names mapped to selectors, expressions, or replacement values.

        Returns:
            A new lazy `Rows` pipeline representing this operation.
        """
        if not columns:
            raise ValueError("cast requires at least one named converter")
        if any(not name for name in columns):
            raise ValueError("cast column names cannot be empty")
        for name, converter in columns.items():
            if not callable(converter):
                raise TypeError(f"cast converter for {name!r} must be callable")

        def transform(row: T) -> dict[str, Any]:
            record = _as_record(row)
            for name, converter in columns.items():
                if name not in record:
                    raise SelectionError(f"cast column {name!r} is missing")
                record[name] = converter(record[name])
            return record

        return Rows(self._flow.map(transform))

    parse = cast

    def fill_nulls(self, **replacements: object) -> Rows[dict[str, Any]]:
        """Replace None values with constants, selectors, or row expressions.

        Args:
            **replacements: Field names mapped to their new names.

        Returns:
            A new lazy `Rows` pipeline representing this operation.
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
        """Drop rows containing None in selected fields.

        Args:
            *selectors: Selectors that define projected fields or grouping keys.
            how: The join mode or operation policy to apply.

        Returns:
            A new lazy `Rows` pipeline representing this operation.
        """
        if how not in {"any", "all"}:
            raise ValueError("drop_nulls how must be 'any' or 'all'")
        selected = tuple(compile_selector(selector) for selector in selectors)

        def select_or_none(select: Callable[[T], Any], row: T) -> Any:
            try:
                return select(row)
            except SelectionError:
                return None

        def keep(row: T) -> bool:
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
        """Emit one row for each element of a selected nested iterable.

        Args:
            selector: A callable, field name, index, path, or expression used to select a value.
            into: A collector or container factory used for the final values.
            outer: Whether unnesting should retain the original nested field.

        Returns:
            A new lazy `Rows` pipeline representing this operation.
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
        """Expand a nested record into top-level fields.

        Args:
            column: The field whose nested values should be expanded.
            prefix: The prefix added to fields expanded from a nested record.

        Returns:
            A new lazy `Rows` pipeline representing this operation.
        """
        if not column or "." in column:
            raise ValueError("unnest column must be a top-level name")

        def expand(row: T) -> dict[str, Any]:
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
        """Convert selected columns from wide form to name/value rows.

        Args:
            *columns: Column names selected by the operation.
            names_to: The output field that stores former column names.
            values_to: The output field that stores former column values.

        Returns:
            A new lazy `Rows` pipeline representing this operation.
        """
        if not columns:
            raise ValueError("unpivot requires at least one column")
        _require_unique_names(columns, operation="unpivot")
        _require_unique_names((names_to, values_to), operation="unpivot")

        def reshape(row: T) -> Iterable[dict[str, Any]]:
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
        """Convert long-form rows to columns with an explicit duplicate policy.

        Args:
            index: The zero-based item or field position to select.
            columns: The columns or column mapping used by the operation.
            values: The values consumed by this operation.
            aggregate: The aggregator used to combine duplicate pivot cells.
            fill: The value used for pivot cells with no matching input row.

        Returns:
            A new lazy `Rows` pipeline representing this operation.
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
        """Project fields and computed selectors into new dictionaries.

        Args:
            *selectors: Selectors that define projected fields or grouping keys.
            **named: Names mapped to collectors or expressions.

        Returns:
            A new lazy `Rows` pipeline representing this operation.
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
            return {name: select(row) for name, select in (*positional, *aliases)}

        return Rows(self._flow.map(project))

    def sort_by(
        self,
        selector: Selector,
        *,
        reverse: bool = False,
        buffer_size: int | None = None,
        tempdir: str | os.PathLike[str] | None = None,
    ) -> Rows[T]:
        """Sort rows by a selector, optionally with a bounded buffer.

        Args:
            selector: A callable, field name, index, path, or expression used to select a value.
            reverse: If true, produce values in descending order.
            buffer_size: The maximum number of items held in an in-memory buffer or batch.
            tempdir: The directory used for temporary spill files.

        Returns:
            A new lazy `Rows` pipeline representing this operation.
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
        """Sort rows with bounded in-memory runs and temporary files.

        Sorted runs are written to temporary files and merged lazily, keeping peak in-memory
        rows bounded.

        Args:
            selector: A callable, field name, index, path, or expression used to select a value.
            reverse: If true, produce values in descending order.
            buffer_size: The maximum number of items held in an in-memory buffer or batch.
            tempdir: The directory used for temporary spill files.

        Returns:
            A new lazy `Rows` pipeline representing this operation.
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
            yield run_aggregations(self, aggregation_items)

        return Rows(flow.defer(evaluate))

    def group_by(self, *selectors: Selector, **named: Selector) -> GroupedRows[T]:
        """Create a deferred grouped aggregation by one or more selectors.

        Args:
            *selectors: Selectors that define projected fields or grouping keys.
            **named: Names mapped to collectors or expressions.

        Returns:
            A new lazy `Rows` pipeline representing this operation.
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

        Returns:
            A new lazy Rows pipeline containing the joined records.

        Raises:
            ValueError: If selectors, modes, partition options, or key cardinality are invalid.
            TypeError: If a key is unhashable or spilled data cannot be serialized.
            DuplicateKeyError: If suffixing would create an ambiguous output field.
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
                )
            )
        )
