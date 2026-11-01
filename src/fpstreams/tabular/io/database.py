"""Resource-owning DB-API and SQLite sources and sinks."""

from __future__ import annotations

import operator
import os
import sqlite3
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from itertools import islice
from typing import Any, TypeAlias

from ...errors import DuplicateKeyError
from ...runtime.resources import _add_cleanup_failure

ConnectionFactory: TypeAlias = Callable[[], Any]
DBParameters: TypeAlias = Mapping[str, Any] | Sequence[Any] | None
ParameterMapper: TypeAlias = Callable[[Any], Any]


def _batch_size(value: int) -> int:
    """Validate a positive integer-like DB-API batch size."""
    try:
        size = operator.index(value)
    except TypeError:
        raise TypeError("batch_size must be an integer") from None
    if size <= 0:
        raise ValueError("batch_size must be positive")
    return size


def _close(resource: Any) -> None:
    """Close a cursor, connection, or iterator when it exposes a close hook."""
    if resource is None:
        return
    close = getattr(resource, "close", None)
    if callable(close):
        close()


def _close_resources(resources: Iterable[Any], active_error: BaseException | None) -> None:
    """Attempt every close and attach failures to the active database error."""
    if isinstance(active_error, GeneratorExit):
        active_error = None
    errors: list[BaseException] = []
    for resource in resources:
        try:
            _close(resource)
        except BaseException as error:
            errors.append(error)
    _add_cleanup_failure(active_error, errors)


def _rollback(connection: Any) -> None:
    """Roll back a connection when it exposes a rollback hook."""
    rollback = getattr(connection, "rollback", None)
    if callable(rollback):
        rollback()


def _column_names(description: Any) -> tuple[str, ...]:
    """Extract and validate result-column names from a DB-API cursor description."""
    if description is None:
        raise ValueError("database query did not produce a row set")
    names = tuple(str(column[0]) for column in description)
    _validate_names(names, operation="database query")
    return names


def _validate_names(names: Iterable[str], *, operation: str) -> tuple[str, ...]:
    """Require at least one unique, non-empty, NUL-free string column name."""
    result = tuple(names)
    if not result:
        raise ValueError(f"{operation} requires at least one column")
    seen: set[str] = set()
    for name in result:
        if not isinstance(name, str):
            raise TypeError(f"{operation} column names must be strings")
        if not name or "\x00" in name:
            raise ValueError(f"{operation} column names cannot be empty or contain NUL")
        if name in seen:
            raise DuplicateKeyError(f"{operation} contains duplicate column {name!r}")
        seen.add(name)
    return result


def _record_from_row(row: Any, names: tuple[str, ...]) -> dict[str, Any]:
    """Project a mapping or positional DB row into the described column dictionary."""
    if isinstance(row, Mapping):
        try:
            return {name: row[name] for name in names}
        except KeyError as error:
            raise ValueError(f"database row is missing column {error.args[0]!r}") from None
    values = tuple(row)
    if len(values) != len(names):
        raise ValueError(
            f"database row has {len(values)} values but the query describes {len(names)} columns"
        )
    return dict(zip(names, values, strict=True))


def db_row_factory(
    connect: ConnectionFactory,
    query: str,
    parameters: DBParameters = None,
    *,
    batch_size: int = 1_000,
) -> Callable[[], Iterator[dict[str, Any]]]:
    """Return a factory that runs a DB-API query with fresh resources per iteration.

    Args:
        connect: Zero-argument factory called once for each new iterator.
        query: Statement executed by the iterator's newly opened cursor.
        parameters: Mapping or positional values passed to cursor.execute(), or None.
        batch_size: Maximum rows requested by each cursor.fetchmany() call.

    Returns:
        A zero-argument iterator factory that closes its cursor and connection when iteration ends.
    """

    if not callable(connect):
        raise TypeError("connect must be a zero-argument callable")
    size = _batch_size(batch_size)

    def records() -> Iterator[dict[str, Any]]:
        """Open a fresh connection, fetch query rows in bounded batches, and close resources."""
        from ...runtime.failpoints import hit

        connection: Any = None
        cursor: Any = None
        active_error: BaseException | None = None
        try:
            connection = connect()
            hit("db.connect.after")
            cursor = connection.cursor()
            hit("db.cursor.after")
            if parameters is None:
                cursor.execute(query)
            else:
                cursor.execute(query, parameters)
            hit("db.execute.after")
            names = _column_names(cursor.description)
            while batch := cursor.fetchmany(size):
                hit("db.fetch.after")
                for row in batch:
                    yield _record_from_row(row, names)
        except BaseException as error:
            active_error = error
            raise
        finally:
            _close_resources((cursor, connection), active_error)

    return records


def sqlite_row_factory(
    database: str | os.PathLike[str],
    query: str,
    parameters: DBParameters = None,
    *,
    batch_size: int = 1_000,
    timeout: float = 5.0,
    uri: bool = False,
) -> Callable[[], Iterator[dict[str, Any]]]:
    """Build a reusable SQLite query source that opens a connection per iteration."""
    path = os.fspath(database)

    def connect() -> sqlite3.Connection:
        """Open one SQLite connection with the configured path, timeout, and URI mode."""
        return sqlite3.connect(path, timeout=timeout, uri=uri)

    return db_row_factory(connect, query, parameters, batch_size=batch_size)


def write_db_rows(
    source: Iterable[Any],
    connect: ConnectionFactory,
    statement: str,
    *,
    parameters: ParameterMapper | None = None,
    batch_size: int = 1_000,
) -> int:
    """Consume rows in DB-API batches, committing once or rolling back on failure.

    Args:
        source: Synchronous iterable consumed once and closed after the write attempt.
        connect: Zero-argument factory for the transaction's connection.
        statement: Statement passed to cursor.executemany() for each batch.
        parameters: Optional callable mapping each source row to one parameter set.
        batch_size: Maximum parameter sets submitted by each executemany() call.

    Returns:
        Number of source rows submitted after the transaction commits successfully.
    """

    from ...runtime.failpoints import hit

    if not callable(connect):
        raise TypeError("connect must be a zero-argument callable")
    if parameters is not None and not callable(parameters):
        raise TypeError("parameters must be callable")
    size = _batch_size(batch_size)
    select: ParameterMapper = (lambda row: row) if parameters is None else parameters
    connection: Any = None
    cursor: Any = None
    iterator: Iterator[Any] | None = None
    active_error: BaseException | None = None
    try:
        connection = connect()
        hit("db.connect.after")
        cursor = connection.cursor()
        hit("db.cursor.after")
        iterator = iter(source)
        batch: list[Any] = []
        count = 0
        for row in iterator:
            batch.append(select(row))
            count += 1
            if len(batch) == size:
                cursor.executemany(statement, batch)
                hit("db.execute.after")
                batch.clear()
        if batch:
            cursor.executemany(statement, batch)
            hit("db.execute.after")
        hit("db.commit.before")
        connection.commit()
        return count
    except BaseException as error:
        active_error = error
        if connection is not None:
            try:
                _rollback(connection)
            except BaseException as rollback_error:
                _add_cleanup_failure(error, [rollback_error])
        raise
    finally:
        _close_resources((iterator, cursor, connection), active_error)


RecordConverter: TypeAlias = Callable[[Any], Mapping[str, Any]]

_SQLITE_TYPES = frozenset({"ANY", "BLOB", "INTEGER", "NUMERIC", "REAL", "TEXT"})
_IF_EXISTS_MODES = frozenset({"append", "fail", "replace"})
_CONFLICT_MODES = frozenset({"error", "ignore", "replace"})
_IDENTIFIER_CASE = str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz")


def _quote_identifier(name: str) -> str:
    """Quote an already validated SQLite identifier without interpreting it as SQL."""
    return '"' + name.replace('"', '""') + '"'


def _validate_sqlite_names(names: Iterable[str], *, operation: str) -> tuple[str, ...]:
    """Validate record names and reject aliases of the same SQLite column."""
    validated = _validate_names(names, operation=operation)
    seen: set[str] = set()
    for name in validated:
        folded = str.translate(name, _IDENTIFIER_CASE)
        if folded in seen:
            raise DuplicateKeyError(f"{operation} contains duplicate column {folded!r}")
        seen.add(folded)
    return validated


def _schema_types(schema: Mapping[str, str] | None) -> dict[str, str] | None:
    """Normalize the intentionally small set of accepted SQLite type names."""
    if schema is None:
        return None
    names = _validate_sqlite_names(schema, operation="SQLite schema")
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


def _sqlite_configuration(
    table: str,
    *,
    if_exists: str,
    conflict: str,
    columns: Iterable[str] | None,
    schema: Mapping[str, str] | None,
    batch_size: int,
    as_record: RecordConverter,
) -> tuple[int, str, tuple[str, ...] | None, dict[str, str] | None]:
    """Validate and freeze sink configuration before opening a connection or source."""
    if if_exists not in _IF_EXISTS_MODES:
        raise ValueError(f"if_exists must be one of {sorted(_IF_EXISTS_MODES)!r}")
    if conflict not in _CONFLICT_MODES:
        raise ValueError(f"conflict must be one of {sorted(_CONFLICT_MODES)!r}")
    if not callable(as_record):
        raise TypeError("as_record must be callable")
    size = _batch_size(batch_size)
    if not isinstance(table, str):
        raise TypeError("SQLite table name must be a string")
    if not table or "\x00" in table:
        raise ValueError("SQLite table name cannot be empty or contain NUL")
    quoted_table = _quote_identifier(table)
    requested = None
    if columns is not None:
        if isinstance(columns, str):
            raise TypeError("columns must be an iterable of names, not a string")
        requested = _validate_sqlite_names(columns, operation="SQLite sink")

    declarations = _schema_types(schema)
    if declarations is None:
        return size, quoted_table, requested, None
    schema_names = tuple(declarations)
    if requested is None:
        return size, quoted_table, schema_names, declarations
    if set(requested) != set(schema_names):
        raise ValueError("columns and schema must name the same SQLite columns")
    return size, quoted_table, requested, declarations


def _inferred_type(value: Any) -> str:
    """Map a Python value to the SQLite storage class used for inference."""
    if isinstance(value, (bool, int)):
        return "INTEGER"
    if isinstance(value, float):
        return "REAL"
    if isinstance(value, str):
        return "TEXT"
    return "BLOB"


def _validate_append_columns(
    cursor: sqlite3.Cursor,
    *,
    table: str,
    quoted_table: str,
    names: tuple[str, ...],
) -> None:
    """Ensure every requested append field exists in the destination table."""
    cursor.execute(f"PRAGMA table_info({quoted_table})")
    existing = {str.translate(str(row[1]), _IDENTIFIER_CASE) for row in cursor.fetchall()}
    missing = [name for name in names if str.translate(name, _IDENTIFIER_CASE) not in existing]
    if missing:
        raise ValueError(f"SQLite table {table!r} has no columns {missing!r}")


def _create_table(
    cursor: sqlite3.Cursor,
    *,
    quoted_table: str,
    names: tuple[str, ...],
    quoted_names: tuple[str, ...],
    declarations: Mapping[str, str] | None,
    first_record: Mapping[str, Any] | None,
) -> None:
    """Create a SQLite table from explicit declarations or first-row inference."""
    inferred = first_record or {}
    definitions = ", ".join(
        f"{quoted_name} "
        f"{declarations[name] if declarations is not None else _inferred_type(inferred.get(name))}"
        for name, quoted_name in zip(names, quoted_names, strict=True)
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
    size, quoted_table, requested, declarations = _sqlite_configuration(
        table,
        if_exists=if_exists,
        conflict=conflict,
        columns=columns,
        schema=schema,
        batch_size=batch_size,
        as_record=as_record,
    )
    explicit_projection = requested is not None

    connection = sqlite3.connect(os.fspath(database), timeout=timeout, uri=uri)
    cursor: sqlite3.Cursor | None = None
    iterator: Iterator[Any] | None = None
    transaction_started = False
    active_error: BaseException | None = None
    try:
        cursor = connection.cursor()
        cursor.execute(
            "SELECT type FROM sqlite_master "
            "WHERE name = ? COLLATE NOCASE AND type IN ('table', 'view') ORDER BY type LIMIT 1",
            (table,),
        )
        destination = cursor.fetchone()
        kind = None if destination is None else str(destination[0])
        if kind == "view":
            raise ValueError(f"SQLite object {table!r} is a view, not a table")
        exists = kind == "table"
        if exists and if_exists == "fail":
            raise ValueError(f"SQLite table {table!r} already exists")
        creating = not exists or if_exists == "replace"

        iterator = iter(source)
        sentinel = object()
        first = next(iterator, sentinel)
        first_record = None if first is sentinel else as_record(first)
        if requested is not None:
            names = requested
        elif first_record is not None:
            names = _validate_sqlite_names(first_record, operation="SQLite sink")
        elif creating:
            raise ValueError(
                "cannot create a SQLite table from empty rows without columns or schema"
            )
        else:
            return 0
        quoted_names = tuple(_quote_identifier(name) for name in names)

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
                quoted_names=quoted_names,
                declarations=declarations,
                first_record=first_record,
            )

        if first_record is None:
            connection.commit()
            transaction_started = False
            return 0

        quoted_columns = ", ".join(quoted_names)
        placeholders = ", ".join("?" for _name in names)
        conflict_sql = "" if conflict == "error" else f" OR {conflict.upper()}"
        statement = (
            f"INSERT{conflict_sql} INTO {quoted_table} ({quoted_columns}) VALUES ({placeholders})"
        )
        bindings = _bindings(
            first_record,
            iterator,
            names=names,
            explicit_projection=explicit_projection,
            as_record=as_record,
        )
        count = 0
        while batch := tuple(islice(bindings, size)):
            cursor.executemany(statement, batch)
            count += len(batch)
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
