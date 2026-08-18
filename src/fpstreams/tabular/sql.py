"""Resource-owning DB-API and SQLite sources and sinks."""

from __future__ import annotations

import operator
import os
import sqlite3
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import suppress
from typing import Any, TypeAlias

from ..errors import DuplicateKeyError

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
    """Best-effort close a cursor or connection without masking an active error."""
    if resource is None:
        return
    close = getattr(resource, "close", None)
    if callable(close):
        with suppress(Exception):
            close()


def _rollback(connection: Any) -> None:
    """Best-effort roll back a connection after a failed write."""
    rollback = getattr(connection, "rollback", None)
    if callable(rollback):
        with suppress(Exception):
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
        connection: Any = None
        cursor: Any = None
        try:
            connection = connect()
            cursor = connection.cursor()
            if parameters is None:
                cursor.execute(query)
            else:
                cursor.execute(query, parameters)
            names = _column_names(cursor.description)
            while batch := cursor.fetchmany(size):
                for row in batch:
                    yield _record_from_row(row, names)
        finally:
            _close(cursor)
            _close(connection)

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

    if not callable(connect):
        raise TypeError("connect must be a zero-argument callable")
    if parameters is not None and not callable(parameters):
        raise TypeError("parameters must be callable")
    size = _batch_size(batch_size)
    select: ParameterMapper = (lambda row: row) if parameters is None else parameters
    connection: Any = None
    cursor: Any = None
    iterator: Iterator[Any] | None = None
    try:
        connection = connect()
        cursor = connection.cursor()
        iterator = iter(source)
        batch: list[Any] = []
        count = 0
        for row in iterator:
            batch.append(select(row))
            count += 1
            if len(batch) == size:
                cursor.executemany(statement, batch)
                batch.clear()
        if batch:
            cursor.executemany(statement, batch)
        connection.commit()
        return count
    except BaseException:
        if connection is not None:
            _rollback(connection)
        raise
    finally:
        _close(iterator)
        _close(cursor)
        _close(connection)
