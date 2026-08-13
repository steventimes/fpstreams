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
from typing import Any, TypeAlias, cast

from ..errors import DuplicateKeyError

RecordConverter: TypeAlias = Callable[[Any], Mapping[str, Any]]


def _arrow_modules() -> tuple[Any, Any, Any]:
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
    try:
        size = operator.index(value)
    except TypeError:
        raise TypeError(f"{name} must be an integer") from None
    if size <= 0:
        raise ValueError(f"{name} must be positive")
    return size


def _close(resource: Any) -> None:
    close = getattr(resource, "close", None)
    if callable(close):
        with suppress(Exception):
            close()


def _column_names(names: Iterable[str], *, operation: str) -> tuple[str, ...]:
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
    return _column_names(schema.names, operation="Arrow schema")


def _batch_from_records(pa: Any, records: list[Mapping[str, Any]], schema: Any) -> Any:
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
    pa, _dataset, _parquet = _arrow_modules()
    size = _positive_size(batch_size)
    if schema is not None:
        _schema_names(schema)

    def batches() -> Iterator[Any]:
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
    pa, _dataset, _parquet = _arrow_modules()
    size = _positive_size(batch_size)

    def batch_rows(batch: Any) -> Iterator[dict[str, Any]]:
        for offset in range(0, batch.num_rows, size):
            yield from batch.slice(offset, size).to_pylist()

    if isinstance(source, pa.Table):
        _schema_names(source.schema)

        def table_rows() -> Iterator[dict[str, Any]]:
            for batch in source.to_batches(max_chunksize=size):
                yield from batch_rows(batch)

        return table_rows, True

    if isinstance(source, pa.RecordBatch):
        _schema_names(source.schema)

        def record_batch_rows() -> Iterator[dict[str, Any]]:
            yield from batch_rows(source)

        return record_batch_rows, True

    if isinstance(source, pa.RecordBatchReader):
        try:
            _schema_names(source.schema)
        except BaseException:
            _close(source)
            raise

        def reader_rows() -> Iterator[dict[str, Any]]:
            try:
                for batch in source:
                    yield from batch_rows(batch)
            finally:
                _close(source)

        return reader_rows, False

    raise TypeError("from_arrow() expects a pyarrow Table, RecordBatch, or RecordBatchReader")


def parquet_row_factory(
    source: Any,
    *,
    columns: Iterable[str] | None = None,
    filter: Any = None,
    batch_size: int = 65_536,
    use_threads: bool = True,
    filesystem: Any = None,
    partitioning: Any = None,
) -> Callable[[], Iterator[dict[str, Any]]]:
    _pa, dataset_module, _parquet = _arrow_modules()
    size = _positive_size(batch_size)
    projected = None if columns is None else list(_column_names(columns, operation="Parquet scan"))

    def records() -> Iterator[dict[str, Any]]:
        if isinstance(source, dataset_module.Dataset):
            dataset = source
        else:
            options: dict[str, Any] = {"format": "parquet"}
            if filesystem is not None:
                options["filesystem"] = filesystem
            if partitioning is not None:
                options["partitioning"] = partitioning
            dataset = dataset_module.dataset(source, **options)
        scanner = dataset.scanner(
            columns=projected,
            filter=filter,
            batch_size=size,
            use_threads=use_threads,
        )
        _schema_names(scanner.projected_schema)
        for batch in scanner.to_batches():
            yield from batch.to_pylist()

    return records


def table_from_rows(
    source: Iterable[Any],
    *,
    batch_size: int = 65_536,
    schema: Any = None,
    as_record: RecordConverter,
) -> Any:
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
