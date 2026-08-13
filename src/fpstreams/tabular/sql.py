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
    try:
        size = operator.index(value)
    except TypeError:
        raise TypeError("batch_size must be an integer") from None
    if size <= 0:
        raise ValueError("batch_size must be positive")
    return size


def _close(resource: Any) -> None:
    if resource is None:
        return
    close = getattr(resource, "close", None)
    if callable(close):
        with suppress(Exception):
            close()


def _rollback(connection: Any) -> None:
    rollback = getattr(connection, "rollback", None)
    if callable(rollback):
        with suppress(Exception):
            rollback()


def _column_names(description: Any) -> tuple[str, ...]:
    if description is None:
        raise ValueError("database query did not produce a row set")
    names = tuple(str(column[0]) for column in description)
    _validate_names(names, operation="database query")
    return names


def _validate_names(names: Iterable[str], *, operation: str) -> tuple[str, ...]:
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
    """Build a reusable, resource-owning DB-API row source.

    Args:
        connect: A zero-argument callable that opens a new database connection.
        query: The SQL query executed for each fresh iteration.
        parameters: Parameters passed to the database query or statement.
        batch_size: The maximum number of rows processed in each batch.

    Returns:
        An iterator that produces values as they are requested.
    """

    if not callable(connect):
        raise TypeError("connect must be a zero-argument callable")
    size = _batch_size(batch_size)

    def records() -> Iterator[dict[str, Any]]:
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
    path = os.fspath(database)

    def connect() -> sqlite3.Connection:
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
    """Execute a DB-API statement in bounded batches and one transaction.

    Args:
        source: The iterable, async iterable, or data source to read lazily.
        connect: A zero-argument callable that opens a new database connection.
        statement: The SQL statement executed for each batch of rows.
        parameters: Parameters passed to the database query or statement.
        batch_size: The maximum number of rows processed in each batch.

    Returns:
        The computed integer value.
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
