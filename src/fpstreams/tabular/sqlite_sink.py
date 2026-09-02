"""Transactional SQLite output for record pipelines."""

from __future__ import annotations

import os
import sqlite3
from collections.abc import Callable, Iterable, Iterator, Mapping
from itertools import islice
from typing import Any, TypeAlias

from ..runtime.resources import _add_cleanup_failure
from .sql import _batch_size, _close_resources, _rollback, _validate_names

RecordConverter: TypeAlias = Callable[[Any], Mapping[str, Any]]

_SQLITE_TYPES = frozenset({"ANY", "BLOB", "INTEGER", "NUMERIC", "REAL", "TEXT"})
_IF_EXISTS_MODES = frozenset({"append", "fail", "replace"})
_CONFLICT_MODES = frozenset({"error", "ignore", "replace"})


def _identifier(name: str, *, operation: str) -> str:
    """Quote a validated SQLite identifier without interpreting it as SQL."""
    if not isinstance(name, str):
        raise TypeError(f"{operation} must be a string")
    if not name or "\x00" in name:
        raise ValueError(f"{operation} cannot be empty or contain NUL")
    return '"' + name.replace('"', '""') + '"'


def _schema_types(schema: Mapping[str, str] | None) -> dict[str, str] | None:
    """Normalize the intentionally small set of accepted SQLite type names."""
    if schema is None:
        return None
    names = _validate_names(schema, operation="SQLite schema")
    declarations: dict[str, str] = {}
    for name in names:
        declaration = schema[name]
        if not isinstance(declaration, str):
            raise TypeError("SQLite schema declarations must be strings")
        normalized = declaration.strip().upper()
        if normalized not in _SQLITE_TYPES:
            allowed = ", ".join(sorted(_SQLITE_TYPES))
            raise ValueError(f"SQLite column type must be one of {allowed}")
        declarations[name] = normalized
    return declarations


def _resolve_schema(
    columns: Iterable[str] | None,
    schema: Mapping[str, str] | None,
) -> tuple[tuple[str, ...] | None, dict[str, str] | None]:
    """Resolve explicit columns and schema declarations before opening SQLite."""
    requested = None
    if columns is not None:
        if isinstance(columns, str):
            raise TypeError("columns must be an iterable of names, not a string")
        requested = _validate_names(columns, operation="SQLite sink")

    declarations = _schema_types(schema)
    if declarations is None:
        return requested, None

    schema_names = tuple(declarations)
    if requested is None:
        return schema_names, declarations
    if set(requested) != set(schema_names):
        raise ValueError("columns and schema must name the same SQLite columns")
    return requested, declarations


def _inferred_type(value: Any) -> str:
    """Map a Python value to the SQLite storage class used for inference."""
    if isinstance(value, (bool, int)):
        return "INTEGER"
    if isinstance(value, float):
        return "REAL"
    if isinstance(value, str):
        return "TEXT"
    return "BLOB"


def _table_kind(cursor: sqlite3.Cursor, table: str) -> str | None:
    """Return whether a named SQLite object is a table, view, or absent."""
    cursor.execute(
        "SELECT type FROM sqlite_master "
        "WHERE name = ? AND type IN ('table', 'view') ORDER BY type LIMIT 1",
        (table,),
    )
    row = cursor.fetchone()
    return None if row is None else str(row[0])


def _existing_columns(cursor: sqlite3.Cursor, quoted_table: str) -> set[str]:
    """Return the field names reported by SQLite for an existing table."""
    cursor.execute(f"PRAGMA table_info({quoted_table})")
    return {str(row[1]) for row in cursor.fetchall()}


def _inspect_destination(
    cursor: sqlite3.Cursor,
    *,
    table: str,
    if_exists: str,
) -> bool:
    """Validate the destination object and report whether its table exists."""
    kind = _table_kind(cursor, table)
    if kind == "view":
        raise ValueError(f"SQLite object {table!r} is a view, not a table")
    exists = kind == "table"
    if exists and if_exists == "fail":
        raise ValueError(f"SQLite table {table!r} already exists")
    return exists


def _first_record(
    iterator: Iterator[Any],
    as_record: RecordConverter,
) -> Mapping[str, Any] | None:
    """Read and convert the first source item, or return None for an empty source."""
    try:
        return as_record(next(iterator))
    except StopIteration:
        return None


def _output_names(
    requested: tuple[str, ...] | None,
    first_record: Mapping[str, Any] | None,
    *,
    creating: bool,
) -> tuple[str, ...] | None:
    """Resolve sink fields from explicit configuration or the first record."""
    if requested is not None:
        return requested
    if first_record is not None:
        return _validate_names(first_record, operation="SQLite sink")
    if creating:
        raise ValueError("cannot create a SQLite table from empty rows without columns or schema")
    return None


def _validate_append_columns(
    cursor: sqlite3.Cursor,
    *,
    table: str,
    quoted_table: str,
    names: tuple[str, ...],
) -> None:
    """Ensure every requested append field exists in the destination table."""
    existing = _existing_columns(cursor, quoted_table)
    missing = [name for name in names if name not in existing]
    if missing:
        raise ValueError(f"SQLite table {table!r} has no columns {missing!r}")


def _create_table(
    cursor: sqlite3.Cursor,
    *,
    quoted_table: str,
    names: tuple[str, ...],
    declarations: Mapping[str, str] | None,
    first_record: Mapping[str, Any] | None,
) -> None:
    """Create a SQLite table from explicit declarations or first-row inference."""
    inferred = first_record or {}
    definitions = ", ".join(
        f"{_identifier(name, operation='SQLite column name')} "
        f"{declarations[name] if declarations is not None else _inferred_type(inferred.get(name))}"
        for name in names
    )
    cursor.execute(f"CREATE TABLE {quoted_table} ({definitions})")


def _record_values(
    record: Mapping[str, Any],
    *,
    names: tuple[str, ...],
    target_names: set[str],
    explicit_projection: bool,
) -> tuple[Any, ...]:
    """Project a record to bound values, rejecting implicit schema drift."""
    if not explicit_projection:
        if isinstance(record, dict):
            has_extras = not record.keys() <= target_names
        else:
            has_extras = any(name not in target_names for name in record)
        if has_extras:
            extras = [name for name in record if name not in target_names]
            raise ValueError(
                f"SQLite rows introduced columns {extras!r}; "
                "pass columns explicitly to project them"
            )
    return tuple(record.get(name) for name in names)


def _insert_statement(
    quoted_table: str,
    names: tuple[str, ...],
    *,
    conflict: str,
) -> str:
    """Build an INSERT statement with quoted identifiers and value placeholders."""
    quoted_columns = ", ".join(_identifier(name, operation="SQLite column name") for name in names)
    placeholders = ", ".join("?" for _name in names)
    conflict_sql = "" if conflict == "error" else f" OR {conflict.upper()}"
    return f"INSERT{conflict_sql} INTO {quoted_table} ({quoted_columns}) VALUES ({placeholders})"


def _bindings(
    first_record: Mapping[str, Any],
    iterator: Iterator[Any],
    *,
    names: tuple[str, ...],
    explicit_projection: bool,
    as_record: RecordConverter,
) -> Iterator[tuple[Any, ...]]:
    """Convert the first and remaining source records into SQLite bindings."""
    target_names = set(names)
    yield _record_values(
        first_record,
        names=names,
        target_names=target_names,
        explicit_projection=explicit_projection,
    )
    for row in iterator:
        yield _record_values(
            as_record(row),
            names=names,
            target_names=target_names,
            explicit_projection=explicit_projection,
        )


def _write_batches(
    cursor: sqlite3.Cursor,
    statement: str,
    bindings: Iterator[tuple[Any, ...]],
    *,
    batch_size: int,
) -> int:
    """Submit binding tuples in bounded batches and return the number written."""
    count = 0
    while batch := tuple(islice(bindings, batch_size)):
        cursor.executemany(statement, batch)
        count += len(batch)
    return count


def write_sqlite_rows(
    source: Iterable[Any],
    database: str | os.PathLike[str],
    table: str,
    *,
    if_exists: str = "append",
    conflict: str = "error",
    columns: Iterable[str] | None = None,
    schema: Mapping[str, str] | None = None,
    batch_size: int = 1_000,
    timeout: float = 5.0,
    uri: bool = False,
    as_record: RecordConverter,
) -> int:
    """Write mapping-like rows to a SQLite table in one transaction.

    Values are always passed as SQLite bindings. Table and column names are validated and quoted
    separately, and every opened iterator, cursor, and connection is closed on success or failure.

    Args:
        source: The iterable of mapping-like rows to write.
        database: The SQLite database path or URI.
        table: The destination table name.
        if_exists: One of append, fail, or replace.
        conflict: One of error, ignore, or replace for SQLite INSERT conflicts.
        columns: Optional ordered fields to write. Extra record fields are projected away.
        schema: Optional field-to-SQLite-type mapping.
        batch_size: Maximum records submitted by each executemany call.
        timeout: Seconds SQLite waits for a locked database.
        uri: Whether database should be interpreted as a SQLite URI.
        as_record: Callable that converts each source item into a mapping.

    Returns:
        The number of source records written or ignored by SQLite.

    Raises:
        ValueError: If configuration, schema, destination, or implicit fields are invalid.
        TypeError: If configuration types or record conversion are invalid.
        sqlite3.Error: If SQLite cannot complete the transaction.
    """
    if if_exists not in _IF_EXISTS_MODES:
        raise ValueError(f"if_exists must be one of {sorted(_IF_EXISTS_MODES)!r}")
    if conflict not in _CONFLICT_MODES:
        raise ValueError(f"conflict must be one of {sorted(_CONFLICT_MODES)!r}")
    if not callable(as_record):
        raise TypeError("as_record must be callable")

    size = _batch_size(batch_size)
    quoted_table = _identifier(table, operation="SQLite table name")
    requested, declarations = _resolve_schema(columns, schema)
    explicit_projection = requested is not None

    connection = sqlite3.connect(os.fspath(database), timeout=timeout, uri=uri)
    cursor: sqlite3.Cursor | None = None
    iterator: Iterator[Any] | None = None
    transaction_started = False
    active_error: BaseException | None = None
    try:
        cursor = connection.cursor()
        exists = _inspect_destination(cursor, table=table, if_exists=if_exists)
        creating = not exists or if_exists == "replace"

        iterator = iter(source)
        first_record = _first_record(iterator, as_record)
        names = _output_names(requested, first_record, creating=creating)
        if names is None:
            return 0

        if exists and if_exists == "append":
            _validate_append_columns(
                cursor,
                table=table,
                quoted_table=quoted_table,
                names=names,
            )

        cursor.execute("BEGIN")
        transaction_started = True
        if exists and if_exists == "replace":
            cursor.execute(f"DROP TABLE {quoted_table}")
        if creating:
            _create_table(
                cursor,
                quoted_table=quoted_table,
                names=names,
                declarations=declarations,
                first_record=first_record,
            )

        if first_record is None:
            connection.commit()
            transaction_started = False
            return 0

        statement = _insert_statement(quoted_table, names, conflict=conflict)
        count = _write_batches(
            cursor,
            statement,
            _bindings(
                first_record,
                iterator,
                names=names,
                explicit_projection=explicit_projection,
                as_record=as_record,
            ),
            batch_size=size,
        )
        connection.commit()
        transaction_started = False
        return count
    except BaseException as error:
        active_error = error
        if transaction_started:
            try:
                _rollback(connection)
            except BaseException as rollback_error:
                _add_cleanup_failure(error, [rollback_error])
        raise
    finally:
        _close_resources((iterator, cursor, connection), active_error)
