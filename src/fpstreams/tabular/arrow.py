"""Arrow and Parquet adapters with bounded streaming buffers."""

from __future__ import annotations

import operator
import os
import tempfile
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import suppress
from importlib import import_module
from itertools import islice
from pathlib import Path
from types import FunctionType
from typing import Any, TypeAlias, cast

from ..errors import DuplicateKeyError
from ..planning.arrow_source import (
    ArrowBatchSource,
    ArrowScanRequest,
    RangePredicate,
)
from ..planning.arrow_source import batch_to_rows as batch_to_rows
from ..planning.semantics import facts_from_capabilities
from ..planning.source import Source, SourceCapabilities, _function_code

RecordConverter: TypeAlias = Callable[[Any], Mapping[str, Any]]
_CSV_PROJECTION_PROBE_BYTES = 64 * 1024
_MAX_PARQUET_PRUNING_PROBE_ROW_GROUPS = 512
_MAX_PARQUET_PRUNING_PROBE_FRAGMENTS = 512
_PARQUET_METADATA_COUNT_TOKEN = object()
_PARQUET_METADATA_COUNT_MARKER = "__fpstreams_guarded_parquet_metadata_count__"


def _arrow_modules() -> tuple[Any, Any, Any]:
    """Import the optional PyArrow modules or raise the installation-specific error."""
    try:
        pa = cast(Any, import_module("pyarrow"))
        dataset = cast(Any, import_module("pyarrow.dataset"))
        parquet = cast(Any, import_module("pyarrow.parquet"))
    except ModuleNotFoundError as error:
        if error.name == "pyarrow" or (error.name or "").startswith("pyarrow."):
            raise ImportError(
                "Arrow/Parquet support requires the 'arrow' extra: pip install fpstreams[arrow]"
            ) from None
        raise
    return pa, dataset, parquet


def _positive_size(value: int, *, name: str = "batch_size") -> int:
    """Coerce an integer-like size and require it to be greater than zero."""
    try:
        size = operator.index(value)
    except TypeError:
        raise TypeError(f"{name} must be an integer") from None
    if size <= 0:
        raise ValueError(f"{name} must be positive")
    return size


def _close(resource: Any) -> None:
    """Best-effort close an Arrow resource without masking the active error."""
    close = getattr(resource, "close", None)
    if callable(close):
        with suppress(Exception):
            close()


_CANONICAL_ARROW_BATCH_TO_ROWS = cast(FunctionType, batch_to_rows)
_CANONICAL_ARROW_BATCH_TO_ROWS_CODE = _function_code(_CANONICAL_ARROW_BATCH_TO_ROWS)
_CANONICAL_ARROW_CLOSE = cast(FunctionType, _close)
_CANONICAL_ARROW_CLOSE_CODE = _function_code(_CANONICAL_ARROW_CLOSE)
_CANONICAL_ARROW_OPEN_BATCHES = cast(FunctionType, ArrowBatchSource.open_batches)
_CANONICAL_ARROW_OPEN_BATCHES_CODE = _function_code(_CANONICAL_ARROW_OPEN_BATCHES)


class _OwnedReaderRows(Iterator[dict[str, Any]]):
    """Close a one-shot Arrow reader even when its row generator never starts."""

    __slots__ = ("_closed", "_iterator", "_reader")

    def __init__(self, iterator: Iterator[dict[str, Any]], reader: Any) -> None:
        self._iterator = iterator
        self._reader = reader
        self._closed = False

    def __iter__(self) -> _OwnedReaderRows:
        return self

    def __next__(self) -> dict[str, Any]:
        return next(self._iterator)

    def close(self) -> None:
        """Release both layers idempotently, including before the first ``next``."""
        if self._closed:
            return
        self._closed = True
        _close(self._iterator)
        _close(self._reader)


def _bounded_batches(source: Iterable[Any], size: int) -> Iterator[Any]:
    """Slice an Arrow batch stream to the configured row bound and own its iterator."""
    iterator = iter(source)
    try:
        for batch in iterator:
            for offset in range(0, batch.num_rows, size):
                yield batch.slice(offset, size)
    finally:
        _close(iterator)


def _column_names(names: Iterable[str], *, operation: str) -> tuple[str, ...]:
    """Materialize and validate unique, non-empty string column names."""
    result = tuple(names)
    seen: set[str] = set()
    for name in result:
        if not isinstance(name, str):
            raise TypeError(f"{operation} column names must be strings")
        if not name:
            raise ValueError(f"{operation} column names cannot be empty")
        if name in seen:
            raise DuplicateKeyError(f"{operation} contains duplicate column {name!r}")
        seen.add(name)
    return result


def _schema_names(schema: Any) -> tuple[str, ...]:
    """Return validated field names from an Arrow schema."""
    return _column_names(schema.names, operation="Arrow schema")


def _batch_from_records(pa: Any, records: list[Mapping[str, Any]], schema: Any) -> Any:
    """Convert records to one RecordBatch while preserving or enforcing its schema."""
    if schema is None:
        names: list[str] = []
        seen: set[str] = set()
        for record in records:
            for name in record:
                if not isinstance(name, str):
                    raise TypeError("Arrow row column names must be strings")
                if not name:
                    raise ValueError("Arrow row column names cannot be empty")
                if name not in seen:
                    seen.add(name)
                    names.append(name)
        if not names:
            raise ValueError("cannot infer an Arrow schema from records without columns")
    else:
        names = list(_schema_names(schema))
        target = set(names)
        for record in records:
            extras = [name for name in record if name not in target]
            if extras:
                raise ValueError(f"Arrow rows introduced columns {extras!r} outside the schema")

    columns = {name: [record.get(name) for record in records] for name in names}
    return pa.RecordBatch.from_pydict(columns, schema=schema)


def arrow_batch_factory(
    source: Iterable[Any],
    *,
    batch_size: int = 65_536,
    schema: Any = None,
    as_record: RecordConverter,
) -> Callable[[], Iterator[Any]]:
    """Build a reusable opener that converts source rows into bounded Arrow batches."""
    pa, _dataset, _parquet = _arrow_modules()
    size = _positive_size(batch_size)
    if schema is not None:
        _schema_names(schema)

    def batches() -> Iterator[Any]:
        """Read at most `batch_size` rows at a time and close the source on exit."""
        current_schema = schema
        iterator = iter(source)
        try:
            while records := [as_record(row) for row in islice(iterator, size)]:
                batch = _batch_from_records(pa, records, current_schema)
                if current_schema is None:
                    current_schema = batch.schema
                yield batch
        finally:
            _close(iterator)

    return batches


def arrow_row_source(
    source: Any,
    *,
    batch_size: int = 65_536,
) -> tuple[Callable[[], Iterator[dict[str, Any]]], bool]:
    """Adapt an Arrow table, batch, or reader to a row opener and replayability flag."""
    pa, _dataset, _parquet = _arrow_modules()
    size = _positive_size(batch_size)

    def batch_rows(batch: Any) -> Iterator[dict[str, Any]]:
        """Slice a RecordBatch into bounded pieces and yield Python record dictionaries."""
        for offset in range(0, batch.num_rows, size):
            yield from batch.slice(offset, size).to_pylist()

    if isinstance(source, pa.Table):
        _schema_names(source.schema)

        def table_rows() -> Iterator[dict[str, Any]]:
            """Yield dictionaries from reusable bounded batches of the Arrow table."""
            for batch in source.to_batches(max_chunksize=size):
                yield from batch_rows(batch)

        return table_rows, True

    if isinstance(source, pa.RecordBatch):
        _schema_names(source.schema)

        def record_batch_rows() -> Iterator[dict[str, Any]]:
            """Yield dictionaries from the reusable RecordBatch in bounded slices."""
            yield from batch_rows(source)

        return record_batch_rows, True

    if isinstance(source, pa.RecordBatchReader):
        try:
            _schema_names(source.schema)
        except BaseException:
            _close(source)
            raise

        def reader_rows() -> Iterator[dict[str, Any]]:
            """Consume the one-shot RecordBatchReader and always close it afterward."""
            try:
                for batch in source:
                    yield from batch_rows(batch)
            finally:
                _close(source)

        return reader_rows, False

    raise TypeError("from_arrow() expects a pyarrow Table, RecordBatch, or RecordBatchReader")


def arrow_source(source: Any, *, batch_size: int = 65_536) -> Source[dict[str, Any]]:
    """Build a row Source retaining its Arrow batch opener for planning."""
    pa, _dataset, _parquet = _arrow_modules()
    size = _positive_size(batch_size)
    if not isinstance(source, (pa.Table, pa.RecordBatch, pa.RecordBatchReader)):
        if not callable(getattr(source, "__arrow_c_stream__", None)):
            raise TypeError(
                "from_arrow() expects a pyarrow Table, RecordBatch, RecordBatchReader, "
                "or __arrow_c_stream__ provider"
            )
        source = pa.RecordBatchReader.from_stream(source)
    if isinstance(source, pa.Table):
        _schema_names(source.schema)
        descriptor = ArrowBatchSource(
            lambda: iter(source.to_batches(max_chunksize=size)),
            "table",
            size,
            source.schema,
            materialized_data=source,
        )
        exact = source.num_rows
    elif isinstance(source, pa.RecordBatch):
        _schema_names(source.schema)
        descriptor = ArrowBatchSource(
            lambda: _bounded_batches((source,), size),
            "record_batch",
            size,
            source.schema,
            materialized_data=source,
        )
        exact = source.num_rows
    elif isinstance(source, pa.RecordBatchReader):
        try:
            _schema_names(source.schema)
        except BaseException:
            _close(source)
            raise
        descriptor = ArrowBatchSource(
            lambda: _bounded_batches(source, size),
            "reader",
            size,
            source.schema,
            False,
        )
        exact = None
    else:  # pragma: no cover - the protocol normalization above closes the type union.
        raise AssertionError("unreachable Arrow source type")

    def rows() -> Iterator[dict[str, Any]]:
        """Open planned Arrow batches, convert them to rows, and close one-shot readers."""
        from ..runtime.failpoints import has_active_failpoints, hit

        instrumented = has_active_failpoints()
        batches = descriptor.open_batches()
        try:
            if instrumented:
                hit("arrow.reader.after")
            for batch in batches:
                for row in batch_to_rows(batch):
                    if instrumented:
                        hit("arrow.batch.after")
                    yield row
        finally:
            _close(batches)
            if descriptor.kind == "reader":
                _close(source)

    def open_rows() -> Iterator[dict[str, Any]]:
        """Wrap one-shot readers so close-before-first-pull still owns the native handle."""
        iterator = rows()
        return _OwnedReaderRows(iterator, source) if descriptor.kind == "reader" else iterator

    return Source(
        open_rows,
        SourceCapabilities(descriptor.reiterable, exact, True),
        native_data=descriptor,
    )


def guarded_arrow_mean_source(source: object) -> ArrowBatchSource | None:
    """Return a replayable in-memory Arrow descriptor with its skipped row loop intact."""
    if (
        type(source) is not Source
        or not source.capabilities.reiterable
        or not source._factory_is_pristine()
        or globals().get("batch_to_rows") is not _CANONICAL_ARROW_BATCH_TO_ROWS
        or _function_code(_CANONICAL_ARROW_BATCH_TO_ROWS) is not _CANONICAL_ARROW_BATCH_TO_ROWS_CODE
        or globals().get("_close") is not _CANONICAL_ARROW_CLOSE
        or _function_code(_CANONICAL_ARROW_CLOSE) is not _CANONICAL_ARROW_CLOSE_CODE
        or ArrowBatchSource.__dict__.get("open_batches") is not _CANONICAL_ARROW_OPEN_BATCHES
        or _function_code(_CANONICAL_ARROW_OPEN_BATCHES) is not _CANONICAL_ARROW_OPEN_BATCHES_CODE
    ):
        return None
    descriptor = source.native_data
    if (
        type(descriptor) is not ArrowBatchSource
        or descriptor.kind not in {"table", "record_batch"}
        or descriptor.materialized_data is None
    ):
        return None
    for captured in source._initial_factory_closure:
        if captured is descriptor:
            return cast(ArrowBatchSource, descriptor)
    return None


def guarded_parquet_count_opener(source: object) -> Callable[[], int | None] | None:
    """Return a local metadata counter still bound to its canonical row source."""
    if type(source) is not Source or not source._factory_is_pristine():
        return None
    descriptor = source.native_data
    if type(descriptor) is not ArrowBatchSource or descriptor.kind != "parquet":
        return None
    count_opener = descriptor.count_opener
    if type(count_opener) is not FunctionType:
        return None
    marker = getattr(count_opener, _PARQUET_METADATA_COUNT_MARKER, None)
    if type(marker) is not tuple or len(marker) != 5:
        return None
    token, batches, dataset_module, dataset_factory, metadata_guarded = marker
    if (
        token is not _PARQUET_METADATA_COUNT_TOKEN
        or metadata_guarded is not True
        or descriptor.opener is not batches
        or getattr(dataset_module, "dataset", None) is not dataset_factory
        or not any(captured is batches for captured in source._initial_factory_closure)
    ):
        return None
    return count_opener


def columns_source(
    columns: Mapping[str, Any],
    *,
    batch_size: int = 65_536,
) -> Source[dict[str, Any]]:
    """Build a retained Arrow source directly from an explicit mapping of columns."""
    pa, _dataset, _parquet = _arrow_modules()
    size = _positive_size(batch_size)
    if not isinstance(columns, Mapping):
        raise TypeError("from_columns() expects a mapping")
    _column_names(columns, operation="from_columns")
    return arrow_source(pa.table(columns), batch_size=size)


def _deferred_arrow_source(
    rows: Callable[[], Iterator[dict[str, Any]]],
    descriptor: ArrowBatchSource,
) -> Source[dict[str, Any]]:
    """Retain a reusable columnar opener beside a canonical deferred row opener."""
    capabilities = SourceCapabilities(reiterable=True, exact_size=None, ordered=True)
    return Source(
        rows,
        capabilities,
        native_data=descriptor,
        facts=facts_from_capabilities(
            reiterable=True,
            exact_size=None,
            ordered=True,
            reopenable=True,
        ),
    )


def csv_source(
    path: str | os.PathLike[str],
    *,
    batch_size: int = 65_536,
    read_options: Any = None,
    parse_options: Any = None,
    convert_options: Any = None,
    memory_pool: Any = None,
) -> Source[dict[str, Any]]:
    """Build a reusable typed CSV stream backed by PyArrow's incremental reader."""
    _pa, _dataset, _parquet = _arrow_modules()
    csv_module = cast(Any, import_module("pyarrow.csv"))
    size = _positive_size(batch_size)

    def open_reader(
        options: Any,
        *,
        input_read_options: Any = read_options,
    ) -> tuple[Any, Any]:
        """Open one owned stream/reader pair and validate its output field names."""
        input_stream = _pa.input_stream(path, compression="detect")
        reader = None
        try:
            reader = csv_module.open_csv(
                input_stream,
                read_options=input_read_options,
                parse_options=parse_options,
                convert_options=options,
                memory_pool=memory_pool,
            )
            _schema_names(reader.schema)
        except BaseException:
            _close(reader)
            _close(input_stream)
            raise
        return reader, input_stream

    def stream(options: Any) -> Iterator[Any]:
        """Own one incremental reader and bound every emitted batch by row count."""
        reader, input_stream = open_reader(options)
        try:
            yield from _bounded_batches(reader, size)
        finally:
            _close(reader)
            _close(input_stream)

    def batches() -> Iterator[Any]:
        """Read the complete caller-visible CSV schema."""
        yield from stream(convert_options)

    def projected_batches(requested: tuple[str, ...]) -> Iterator[Any]:
        """Reopen a default CSV reader with only proven-present query fields."""
        if not requested:
            yield from batches()
            return
        if read_options is None and parse_options is None and convert_options is None:
            try:
                probe, input_stream = open_reader(
                    None,
                    input_read_options=csv_module.ReadOptions(
                        block_size=_CSV_PROJECTION_PROBE_BYTES
                    ),
                )
            except (TypeError, ValueError):
                # A header/record may straddle the bounded probe, or its partial view may expose
                # a schema-validation error before a later parse error. Reopen with Arrow's
                # established default so both successful inference and public errors remain
                # identical to the pre-optimization path. Allocation and non-validation errors
                # deliberately propagate instead of being mistaken for a speculative decline.
                probe, input_stream = open_reader(None)
        else:
            probe, input_stream = open_reader(None)
        try:
            available = set(probe.schema.names)
        finally:
            _close(probe)
            _close(input_stream)
        if any(name not in available for name in requested):
            # Preserve lazy Rows selection semantics for missing fields: header-only inputs
            # remain empty, while nonempty inputs fail through the canonical selector path.
            yield from batches()
            return
        options = csv_module.ConvertOptions(include_columns=list(requested))
        yield from stream(options)

    def records() -> Iterator[dict[str, Any]]:
        iterator = batches()
        try:
            for batch in iterator:
                yield from batch_to_rows(batch)
        finally:
            _close(iterator)

    def byte_size() -> int | None:
        """Return cheap local-file evidence without changing an eventual open failure."""
        if type(path) is not str and not isinstance(path, Path):
            return None
        try:
            size_bytes = os.stat(path).st_size
        except (OSError, TypeError, ValueError):
            return None
        return size_bytes if type(size_bytes) is int and size_bytes >= 0 else None

    descriptor = ArrowBatchSource(
        batches,
        "csv",
        size,
        # ParseOptions is mutable.  Supplying one disables the two-open projection route so a
        # callback installed after construction can never observe the same invalid row twice.
        projection_opener=(
            projected_batches if convert_options is None and parse_options is None else None
        ),
        byte_size_opener=byte_size,
    )
    return _deferred_arrow_source(records, descriptor)


def _parquet_equality_expression(
    pa: Any,
    dataset_module: Any,
    dataset: Any,
    available: set[str],
    equality: tuple[str, object],
) -> Any | None:
    """Build an Arrow equality only for a caller-visible field with an exact scalar type."""
    field_name, value = equality
    if field_name not in available:
        return None
    arrow_type = dataset.schema.field(field_name).type
    value_type = type(value)
    compatible = (
        (value_type is int and pa.types.is_int64(arrow_type))
        or (value_type is bool and pa.types.is_boolean(arrow_type))
        or (
            value_type is str
            and (pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type))
        )
        or (
            value_type is bytes
            and (pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type))
        )
    )
    return dataset_module.field(field_name) == value if compatible else None


def _parquet_range_expression(
    pa: Any,
    dataset_module: Any,
    dataset: Any,
    available: set[str],
    predicate: RangePredicate,
) -> Any | None:
    """Build a null-preserving scanner superset for one exact int64 range."""
    field_name, operator, value = predicate
    if (
        type(field_name) is not str
        or "." in field_name
        or operator not in {"<", "<=", ">", ">="}
        or type(value) is not int
        or not -(1 << 63) <= value < (1 << 63)
        or field_name not in available
        or not pa.types.is_int64(dataset.schema.field(field_name).type)
    ):
        return None
    field = dataset_module.field(field_name)
    match operator:
        case "<":
            comparison = field < value
        case "<=":
            comparison = field <= value
        case ">":
            comparison = field > value
        case ">=":
            comparison = field >= value
        case _:
            return None
    return field.is_null() | comparison


def _parquet_first_predicate_can_prune(
    dataset: Any,
    predicate: Any,
    dataset_module: Any,
    iterator: Iterator[Any],
    *,
    single_fragment: bool,
) -> bool:
    """Probe only metadata needed to preserve a first-match query's source order."""
    try:
        first = next(iterator)
    except StopIteration:
        return False
    if not isinstance(first, dataset_module.ParquetFileFragment):
        return False
    first_row_groups = first.num_row_groups
    if (
        type(first_row_groups) is not int
        or first_row_groups <= 0
        or first_row_groups > _MAX_PARQUET_PRUNING_PROBE_ROW_GROUPS
    ):
        return False
    kept_first = len(first.split_by_row_group(predicate, schema=dataset.schema))
    return kept_first == 0 or (single_fragment and kept_first < first_row_groups)


def _parquet_predicate_can_prune(
    dataset: Any,
    predicate: Any,
    dataset_module: Any,
    *,
    probe_statistics: bool,
    first_only: bool = False,
    single_fragment: bool = False,
) -> bool:
    """Ask Arrow whether bounded local row-group statistics can exclude any data."""
    if not probe_statistics:
        return not first_only
    fragments: list[Any] = []
    total = 0
    try:
        iterator = iter(dataset.get_fragments())
        if first_only:
            return _parquet_first_predicate_can_prune(
                dataset,
                predicate,
                dataset_module,
                iterator,
                single_fragment=single_fragment,
            )
        for fragment in iterator:
            if len(fragments) >= _MAX_PARQUET_PRUNING_PROBE_FRAGMENTS:
                return False
            if not isinstance(fragment, dataset_module.ParquetFileFragment):
                return False
            row_groups = fragment.num_row_groups
            if type(row_groups) is not int or row_groups < 0:
                return False
            total += row_groups
            if total > _MAX_PARQUET_PRUNING_PROBE_ROW_GROUPS:
                return False
            fragments.append(fragment)
        if total == 0:
            return False
        kept = sum(
            len(fragment.split_by_row_group(predicate, schema=dataset.schema))
            for fragment in fragments
        )
    except MemoryError:
        raise
    except Exception:
        # A failed speculative metadata probe must not turn an otherwise valid residual filter
        # into a scanner error. Unknown local layouts simply keep the canonical unfiltered scan.
        return False
    return kept < total


def _guarded_local_parquet_metadata_count(
    source: Any,
    filesystem: Any,
    partitioning: Any,
) -> bool:
    """Prove metadata counting has no caller-defined source or filesystem callback."""
    return bool(
        filesystem is None
        and partitioning is None
        and ((type(source) is str and "://" not in source) or type(source) is type(Path()))
    )


def _parquet_batch_factory(  # noqa: C901 - shared scan/count opener construction
    source: Any,
    *,
    columns: Iterable[str] | None = None,
    filter: Any = None,
    batch_size: int = 65_536,
    use_threads: bool = True,
    filesystem: Any = None,
    partitioning: Any = None,
) -> tuple[
    Callable[[], Iterator[Any]],
    int,
    Callable[[tuple[str, ...]], Iterator[Any]],
    Callable[[ArrowScanRequest], Iterator[Any]],
    Callable[[], int | None],
]:
    """Build Parquet batch and metadata-count openers with a row bound."""
    _pa, dataset_module, _parquet = _arrow_modules()
    size = _positive_size(batch_size)
    projected = None if columns is None else list(_column_names(columns, operation="Parquet scan"))
    dataset_factory = getattr(dataset_module, "dataset", None)
    metadata_count_is_guarded = bool(
        dataset_factory is not None
        and _guarded_local_parquet_metadata_count(
            source,
            filesystem,
            partitioning,
        )
    )
    adaptive_pruning = filesystem is None and (
        (type(source) is str and "://" not in source) or isinstance(source, Path)
    )

    def open_dataset() -> Any:
        """Create or reuse the dataset object without opening a scanner eagerly."""
        if isinstance(source, dataset_module.Dataset):
            return source
        options: dict[str, Any] = {"format": "parquet"}
        if filesystem is not None:
            options["filesystem"] = filesystem
        if partitioning is not None:
            options["partitioning"] = partitioning
        return dataset_module.dataset(source, **options)

    def scan(
        requested: tuple[str, ...] | None,
        equality: tuple[str, object] | None = None,
        range_predicate: RangePredicate | None = None,
        *,
        first_only: bool = False,
    ) -> Iterator[Any]:
        """Yield batches, narrowing only within the caller-visible source schema."""
        dataset = open_dataset()
        scan_columns = projected
        scan_filter = filter
        dataset_names = set(_schema_names(dataset.schema))
        available = set(projected) if projected is not None else dataset_names
        base_projection_is_proven = projected is None or all(
            name in dataset_names for name in projected
        )
        requested_is_proven = base_projection_is_proven and (
            requested is None or all(name in available for name in requested)
        )
        # A missing query field must retain the canonical row-wise error timing.  Reading
        # the original column set lets an empty source remain empty and a nonempty source
        # fail through the normal selector instead of raising eagerly in the scanner.
        # Likewise, an invalid public columns= request must still reach the base scanner;
        # a narrower downstream select cannot be allowed to hide its schema error.
        if requested is not None and requested_is_proven:
            scan_columns = list(requested)
        if (
            filter is None
            and requested_is_proven
            and (equality is not None or range_predicate is not None)
        ):
            candidate = (
                _parquet_equality_expression(_pa, dataset_module, dataset, available, equality)
                if equality is not None
                else _parquet_range_expression(
                    _pa,
                    dataset_module,
                    dataset,
                    available,
                    cast(RangePredicate, range_predicate),
                )
            )
            if candidate is not None and _parquet_predicate_can_prune(
                dataset,
                candidate,
                dataset_module,
                probe_statistics=adaptive_pruning,
                first_only=first_only,
                single_fragment=adaptive_pruning and Path(source).is_file(),
            ):
                scan_filter = candidate
        scanner_options: dict[str, Any] = {
            "columns": scan_columns,
            "filter": scan_filter,
            "batch_size": size,
            "use_threads": use_threads,
        }
        if first_only:
            scanner_options["batch_readahead"] = 0
            scanner_options["fragment_readahead"] = 0
        scanner = dataset.scanner(
            **scanner_options,
        )
        _schema_names(scanner.projected_schema)
        iterator = iter(scanner.to_batches())
        try:
            yield from iterator
        finally:
            _close(iterator)

    def batches() -> Iterator[Any]:
        """Create a fresh scanner using the public source projection."""
        yield from scan(None)

    def projected_batches(requested: tuple[str, ...]) -> Iterator[Any]:
        """Create a fresh scanner containing only fields required by a closed query."""
        yield from scan(requested)

    def requested_batches(request: ArrowScanRequest) -> Iterator[Any]:
        """Create a scanner from schema-guarded projection and comparison hints."""
        yield from scan(
            request.columns,
            request.equality,
            request.range_predicate,
            first_only=request.first_only,
        )

    def count_rows() -> int | None:
        """Count the public scan through Arrow's metadata-aware scanner terminal."""
        if (
            not metadata_count_is_guarded
            or getattr(dataset_module, "dataset", None) is not dataset_factory
        ):
            return None
        dataset = open_dataset()
        _schema_names(dataset.schema)
        scanner = dataset.scanner(
            columns=projected,
            filter=filter,
            batch_size=size,
            use_threads=use_threads,
        )
        _schema_names(scanner.projected_schema)
        count = getattr(scanner, "count_rows", None)
        if not callable(count):
            return None
        result = count()
        return result if type(result) is int and result >= 0 else None

    setattr(
        count_rows,
        _PARQUET_METADATA_COUNT_MARKER,
        (
            _PARQUET_METADATA_COUNT_TOKEN,
            batches,
            dataset_module,
            dataset_factory,
            metadata_count_is_guarded,
        ),
    )
    return batches, size, projected_batches, requested_batches, count_rows


def parquet_source(
    source: Any,
    *,
    columns: Iterable[str] | None = None,
    filter: Any = None,
    batch_size: int = 65_536,
    use_threads: bool = True,
    filesystem: Any = None,
    partitioning: Any = None,
) -> Source[dict[str, Any]]:
    """Retain Parquet scanner batches so relational projections stay columnar."""
    batches, size, projected_batches, requested_batches, count_rows = _parquet_batch_factory(
        source,
        columns=columns,
        filter=filter,
        batch_size=batch_size,
        use_threads=use_threads,
        filesystem=filesystem,
        partitioning=partitioning,
    )

    def records() -> Iterator[dict[str, Any]]:
        iterator = batches()
        try:
            for batch in iterator:
                yield from batch_to_rows(batch)
        finally:
            _close(iterator)

    descriptor = ArrowBatchSource(
        batches,
        "parquet",
        size,
        projection_opener=projected_batches,
        request_opener=requested_batches if filter is None else None,
        count_opener=count_rows,
    )
    return _deferred_arrow_source(records, descriptor)


def table_from_rows(
    source: Iterable[Any],
    *,
    batch_size: int = 65_536,
    schema: Any = None,
    as_record: RecordConverter,
) -> Any:
    """Materialize row batches as one Arrow Table, retaining an explicit empty schema."""
    pa, _dataset, _parquet = _arrow_modules()
    factory = arrow_batch_factory(
        source,
        batch_size=batch_size,
        schema=schema,
        as_record=as_record,
    )
    batches = list(factory())
    if batches:
        return pa.Table.from_batches(batches)
    if schema is not None:
        return pa.Table.from_batches([], schema=schema)
    return pa.table({})


def write_parquet_rows(
    source: Iterable[Any],
    path: str | os.PathLike[str],
    *,
    if_exists: str = "error",
    batch_size: int = 65_536,
    schema: Any = None,
    compression: Any = "zstd",
    use_dictionary: Any = True,
    write_statistics: Any = True,
    writer_options: Mapping[str, Any] | None = None,
    as_record: RecordConverter,
) -> int:
    """Stream rows to a temporary Parquet file and atomically publish it on success."""
    _pa, _dataset, parquet = _arrow_modules()
    modes = {"error", "replace"}
    if if_exists not in modes:
        raise ValueError(f"if_exists must be one of {sorted(modes)!r}")
    path_value = os.fspath(path)
    if "://" in path_value:
        raise ValueError("to_parquet() atomic writes currently require a local path")
    target = Path(path_value)
    if target.exists() and if_exists == "error":
        raise FileExistsError(f"Parquet target already exists: {target}")

    options = dict(writer_options or {})
    reserved = {"where", "schema", "compression", "use_dictionary", "write_statistics"}
    overlap = reserved & options.keys()
    if overlap:
        name = next(iter(overlap))
        raise TypeError(f"writer_options cannot override {name!r}")
    factory = arrow_batch_factory(
        source,
        batch_size=batch_size,
        schema=schema,
        as_record=as_record,
    )

    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.",
        suffix=".tmp",
        dir=target.parent,
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    iterator = factory()
    writer: Any = None
    published = False
    count = 0
    try:
        for batch in iterator:
            if writer is None:
                writer = parquet.ParquetWriter(
                    temporary,
                    batch.schema,
                    compression=compression,
                    use_dictionary=use_dictionary,
                    write_statistics=write_statistics,
                    **options,
                )
            writer.write_batch(batch, row_group_size=batch.num_rows)
            count += batch.num_rows

        if writer is None:
            if schema is None:
                raise ValueError("cannot write empty rows to Parquet without an Arrow schema")
            writer = parquet.ParquetWriter(
                temporary,
                schema,
                compression=compression,
                use_dictionary=use_dictionary,
                write_statistics=write_statistics,
                **options,
            )

        _close(iterator)
        writer.close()
        writer = None
        if target.exists() and if_exists == "error":
            raise FileExistsError(f"Parquet target already exists: {target}")
        os.replace(temporary, target)
        published = True
        return count
    finally:
        _close(iterator)
        _close(writer)
        if not published:
            temporary.unlink(missing_ok=True)
