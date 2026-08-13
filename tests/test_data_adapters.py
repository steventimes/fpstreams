"""Arrow, dataframe, Polars, Parquet, and SQL adapters."""

from __future__ import annotations

import sqlite3
import subprocess
import sys
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

import fpstreams


def test_process_parallel_is_safe_after_arrow_initializes_threads(tmp_path: Path) -> None:
    program = tmp_path / "parallel_after_arrow.py"
    program.write_text(
        """
import pyarrow
from fpstreams import flow

def square(value):
    return value * value

if __name__ == "__main__":
    print(flow(range(4)).parallel(workers=2).map(square).to_list())
""".lstrip(),
        encoding="utf-8",
    )
    completed = subprocess.run(
        [sys.executable, "-W", "always", program],
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == "[0, 1, 4, 9]"
    assert "multi-threaded, use of fork()" not in completed.stderr


def test_arrow_batches_preserve_sparse_rows_schema_and_reuse() -> None:
    values = [{"id": 1, "group": "a"}, {"id": 2}, {"id": 3, "group": "b"}]
    source = fpstreams.rows(values)
    batches = source.arrow_batches(batch_size=2).to_list()

    assert [batch.num_rows for batch in batches] == [2, 1]
    assert all(batch.schema.names == ["id", "group"] for batch in batches)
    expected = [{"id": 1, "group": "a"}, {"id": 2, "group": None}, values[2]]
    assert source.to_arrow(batch_size=2).to_pylist() == expected

    restored = fpstreams.rows.from_arrow(pa.Table.from_batches(batches), batch_size=1)
    assert restored.to_list() == expected
    assert restored.to_list() == expected

    schema = pa.schema([("id", pa.int64()), ("group", pa.string())])
    assert fpstreams.rows([{"id": 4}]).to_arrow(schema=schema).to_pylist() == [
        {"id": 4, "group": None}
    ]


def test_arrow_reader_stays_one_shot_and_closes_after_short_circuit() -> None:
    batch = pa.RecordBatch.from_pydict({"id": [1, 2, 3]})
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    source = fpstreams.rows.from_arrow(reader, batch_size=1)

    assert source.take(1).to_list() == [{"id": 1}]
    with pytest.raises(fpstreams.FlowConsumedError):
        source.to_list()


def test_parquet_sink_streams_row_groups_and_scan_pushes_projection_filter(tmp_path: Path) -> None:
    target = tmp_path / "events.parquet"
    events: list[str] = []

    def records() -> Iterator[dict[str, object]]:
        events.append("open")
        try:
            for value in range(7):
                yield {"id": value, "group": value % 2, "payload": f"value-{value}"}
        finally:
            events.append("close")

    assert fpstreams.rows(records()).to_parquet(target, batch_size=3) == 7
    assert events == ["open", "close"]
    assert pq.ParquetFile(target).metadata.num_row_groups == 3

    scanned = fpstreams.rows.from_parquet(
        target,
        columns=("id", "group"),
        filter=ds.field("id") >= 4,
        batch_size=2,
    )
    expected = [
        {"id": 4, "group": 0},
        {"id": 5, "group": 1},
        {"id": 6, "group": 0},
    ]
    assert scanned.to_list() == expected
    assert scanned.to_list() == expected


def test_parquet_publish_is_atomic_and_never_consumes_on_existing_error(tmp_path: Path) -> None:
    target = tmp_path / "atomic.parquet"
    assert fpstreams.rows([{"id": 10, "value": "kept"}]).to_parquet(target) == 1

    opened = False

    def unused() -> Iterator[dict[str, int]]:
        nonlocal opened
        opened = True
        yield {"id": 11}

    with pytest.raises(FileExistsError):
        fpstreams.rows(unused()).to_parquet(target)
    assert not opened

    with pytest.raises(ValueError, match="introduced columns"):
        fpstreams.rows([{"id": 1, "value": "new"}, {"id": 2, "extra": True}]).to_parquet(
            target,
            if_exists="replace",
            batch_size=1,
        )
    assert fpstreams.rows.from_parquet(target).to_list() == [{"id": 10, "value": "kept"}]
    assert list(tmp_path.iterdir()) == [target]

    empty = tmp_path / "empty.parquet"
    schema = pa.schema([("id", pa.int64())])
    assert fpstreams.rows([]).to_parquet(empty, schema=schema) == 0
    assert pq.read_table(empty).schema == schema
    assert pq.read_table(empty).num_rows == 0


def test_arrow_configuration_rejects_silent_schema_loss(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="outside the schema"):
        fpstreams.rows([{"a": 1}, {"b": 2}]).arrow_batches(batch_size=1).to_list()
    with pytest.raises(ValueError, match="without columns"):
        fpstreams.rows([{}]).to_arrow()
    with pytest.raises(ValueError, match="without an Arrow schema"):
        fpstreams.rows([]).to_parquet(tmp_path / "missing-schema.parquet")
    assert not (tmp_path / "missing-schema.parquet").exists()

    duplicate = pa.schema([("id", pa.int64()), ("id", pa.int64())])
    with pytest.raises(fpstreams.DuplicateKeyError, match="duplicate column"):
        fpstreams.rows([]).arrow_batches(schema=duplicate)
    duplicate_batch = pa.RecordBatch.from_arrays(
        [pa.array([1]), pa.array([2])],
        names=("id", "id"),
    )
    with pytest.raises(fpstreams.DuplicateKeyError, match="duplicate column"):
        fpstreams.rows.from_arrow(duplicate_batch)
    with pytest.raises(ValueError, match="batch_size"):
        fpstreams.rows.from_arrow(pa.table({"id": [1]}), batch_size=0)
    with pytest.raises(ValueError, match="local path"):
        fpstreams.rows([{"id": 1}]).to_parquet("s3://bucket/events.parquet")


def test_rows_factory_recognizes_eager_dataframe_protocols() -> None:
    pandas_frame = pd.DataFrame(
        {"id": [1, 2], "label": ["a", "b"]},
        index=pd.Index(["left", "right"], name="row"),
    )
    polars_frame = pl.DataFrame({"id": [3, 4], "label": ["c", "d"]})

    assert fpstreams.rows(pandas_frame).to_list() == [
        {"id": 1, "label": "a"},
        {"id": 2, "label": "b"},
    ]
    assert fpstreams.rows(polars_frame).to_list() == [
        {"id": 3, "label": "c"},
        {"id": 4, "label": "d"},
    ]
    strict_frame = pd.DataFrame({"id": [1, 2]})
    assert fpstreams.rows.from_dataframe(strict_frame, allow_copy=False).count() == 2
    assert fpstreams.rows.from_polars(polars_frame, batch_size=1).count() == 2


def test_polars_lazyframe_stays_lazy_batched_and_reiterable() -> None:
    executions: list[int] = []

    def observe(batch: pl.DataFrame) -> pl.DataFrame:
        executions.append(batch.height)
        return batch

    lazy_frame = (
        pl.LazyFrame({"id": range(5)})
        .with_columns((pl.col("id") * 10).alias("value"))
        .map_batches(observe)
    )
    source = fpstreams.rows.from_polars(lazy_frame, batch_size=2)

    assert executions == []
    assert source.take(1).to_list() == [{"id": 0, "value": 0}]
    assert executions
    executions.clear()
    assert source.to_list() == [
        {"id": 0, "value": 0},
        {"id": 1, "value": 10},
        {"id": 2, "value": 20},
        {"id": 3, "value": 30},
        {"id": 4, "value": 40},
    ]
    assert executions


def test_rows_outputs_polars_batches_and_eager_dataframe() -> None:
    values: list[dict[str, Any]] = [
        {"id": 1},
        {"id": 2, "label": "b"},
        {"id": 3, "label": "c"},
        {"id": 4},
        {"id": 5, "label": "e"},
    ]
    source = fpstreams.rows(values)

    batches = source.polars_batches(batch_size=2).to_list()
    assert [batch.height for batch in batches] == [2, 2, 1]
    assert all(batch.columns == ["id", "label"] for batch in batches)
    expected = [dict(row, label=row.get("label")) for row in values]
    assert [row for batch in batches for row in batch.to_dicts()] == expected

    restored = source.to_polars(batch_size=2)
    assert isinstance(restored, pl.DataFrame)
    assert restored.to_dicts() == expected
    pandas_records = source.to_pandas(batch_size=2).to_dict("records")
    assert [record["id"] for record in pandas_records] == [1, 2, 3, 4, 5]
    assert pd.isna(pandas_records[0]["label"])
    assert pandas_records[1]["label"] == "b"


class _ReadCursor:
    def __init__(self, columns: Sequence[str], values: Sequence[tuple[Any, ...]]) -> None:
        self.description = tuple((name,) for name in columns)
        self._values = list(values)
        self.executed: tuple[str, object] | None = None
        self.fetch_sizes: list[int] = []
        self.closed = False

    def execute(self, query: str, parameters: object = None) -> None:
        self.executed = (query, parameters)

    def fetchmany(self, size: int) -> list[tuple[Any, ...]]:
        self.fetch_sizes.append(size)
        batch, self._values = self._values[:size], self._values[size:]
        return batch

    def close(self) -> None:
        self.closed = True


class _ReadConnection:
    def __init__(self, cursor: _ReadCursor) -> None:
        self._cursor = cursor
        self.closed = False

    def cursor(self) -> _ReadCursor:
        return self._cursor

    def close(self) -> None:
        self.closed = True


class _WriteCursor:
    def __init__(self, *, fail_on: int | None = None) -> None:
        self.batches: list[list[object]] = []
        self.fail_on = fail_on
        self.closed = False

    def executemany(self, _statement: str, values: Sequence[object]) -> None:
        self.batches.append(list(values))
        if len(self.batches) == self.fail_on:
            raise RuntimeError("database write failed")

    def close(self) -> None:
        self.closed = True


class _WriteConnection:
    def __init__(self, cursor: _WriteCursor) -> None:
        self._cursor = cursor
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self) -> _WriteCursor:
        return self._cursor

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


def test_db_source_is_lazy_reusable_batched_and_closes_on_take() -> None:
    connections: list[_ReadConnection] = []

    def connect() -> _ReadConnection:
        connection = _ReadConnection(_ReadCursor(("id", "name"), ((1, "a"), (2, "b"))))
        connections.append(connection)
        return connection

    source = fpstreams.rows.from_db(
        connect,
        "select id, name from events where id >= :minimum",
        {"minimum": 1},
        batch_size=2,
    )
    assert connections == []
    assert source.take(1).to_list() == [{"id": 1, "name": "a"}]
    assert connections[0]._cursor.executed == (
        "select id, name from events where id >= :minimum",
        {"minimum": 1},
    )
    assert connections[0]._cursor.fetch_sizes == [2]
    assert connections[0]._cursor.closed and connections[0].closed

    assert source.to_list() == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    assert len(connections) == 2
    assert connections[1]._cursor.closed and connections[1].closed


def test_db_source_rejects_ambiguous_columns_and_still_closes() -> None:
    connection = _ReadConnection(_ReadCursor(("value", "value"), ((1, 2),)))
    with pytest.raises(fpstreams.DuplicateKeyError, match="duplicate column"):
        fpstreams.rows.from_db(lambda: connection, "select 1, 2").to_list()
    assert connection._cursor.closed and connection.closed


def test_db_sink_batches_commits_rolls_back_and_closes_upstream() -> None:
    cursor = _WriteCursor()
    connection = _WriteConnection(cursor)
    assert (
        fpstreams.rows([{"id": 1}, {"id": 2}, {"id": 3}]).to_db(
            lambda: connection,
            "insert into events values (?)",
            parameters=lambda row: (row["id"],),
            batch_size=2,
        )
        == 3
    )
    assert cursor.batches == [[(1,), (2,)], [(3,)]]
    assert connection.commits == 1 and connection.rollbacks == 0
    assert cursor.closed and connection.closed

    events: list[str] = []

    def records() -> Iterator[dict[str, int]]:
        events.append("open")
        try:
            for value in range(5):
                yield {"id": value}
        finally:
            events.append("close")

    failed_cursor = _WriteCursor(fail_on=2)
    failed = _WriteConnection(failed_cursor)
    with pytest.raises(RuntimeError, match="write failed"):
        fpstreams.rows(records()).to_db(
            lambda: failed,
            "insert into events values (?)",
            parameters=lambda row: (row["id"],),
            batch_size=2,
        )
    assert failed.commits == 0 and failed.rollbacks == 1
    assert failed_cursor.closed and failed.closed
    assert events == ["open", "close"]


def test_sqlite_table_sink_and_query_source_form_a_concise_round_trip(tmp_path: Path) -> None:
    database = tmp_path / "events.db"
    values = [
        {"id": 1, "select": "a", "amount": 2.0},
        {"id": 2, "select": "b", "amount": 3.5},
        {"id": 3, "select": "a", "amount": 4.0},
    ]
    assert fpstreams.rows(values).to_sqlite(
        database, 'event "log"', if_exists="replace", batch_size=1
    ) == len(values)

    selected = fpstreams.rows.from_sqlite(
        database,
        'select id, "select", amount from "event ""log""" where amount >= ? order by id',
        (3.0,),
        batch_size=1,
    )
    expected = [values[1], values[2]]
    assert selected.to_list() == expected
    assert selected.to_list() == expected
    assert selected.where(select="a").select("id", "amount").to_list() == [{"id": 3, "amount": 4.0}]


def test_sqlite_schema_projection_conflicts_and_fail_mode_are_explicit(tmp_path: Path) -> None:
    database = tmp_path / "schema.db"
    assert (
        fpstreams.rows([]).to_sqlite(
            database,
            "events",
            if_exists="replace",
            schema={"id": "INTEGER", "value": "TEXT"},
        )
        == 0
    )
    assert (
        fpstreams.rows([{"id": 1, "value": "a", "ignored": True}]).to_sqlite(
            database,
            "events",
            columns=("id", "value"),
        )
        == 1
    )
    assert fpstreams.rows.from_sqlite(database, "select * from events").to_list() == [
        {"id": 1, "value": "a"}
    ]

    opened = False

    def unused() -> Iterator[dict[str, int]]:
        nonlocal opened
        opened = True
        yield {"id": 2}

    with pytest.raises(ValueError, match="already exists"):
        fpstreams.rows(unused()).to_sqlite(database, "events", if_exists="fail")
    assert not opened

    with sqlite3.connect(database) as connection:
        connection.execute("create table unique_events (id integer primary key, value text)")
    assert (
        fpstreams.rows([{"id": 1, "value": "a"}, {"id": 1, "value": "b"}]).to_sqlite(
            database,
            "unique_events",
            conflict="ignore",
            batch_size=1,
        )
        == 2
    )
    assert fpstreams.rows.from_sqlite(database, "select * from unique_events").to_list() == [
        {"id": 1, "value": "a"}
    ]


def test_sqlite_sink_rolls_back_batches_and_schema_changes_on_error(tmp_path: Path) -> None:
    database = tmp_path / "atomic.db"
    with sqlite3.connect(database) as connection:
        connection.execute("create table events (id integer, value text)")
        connection.execute("insert into events values (10, 'kept')")

    with pytest.raises(sqlite3.ProgrammingError):
        fpstreams.rows([{"id": 1, "value": "ok"}, {"id": 2, "value": object()}]).to_sqlite(
            database, "events", batch_size=1
        )
    assert fpstreams.rows.from_sqlite(database, "select * from events").to_list() == [
        {"id": 10, "value": "kept"}
    ]

    with pytest.raises(ValueError, match="introduced columns"):
        fpstreams.rows([{"a": 1}, {"a": 2, "b": 3}]).to_sqlite(
            database, "drift", if_exists="replace", batch_size=1
        )
    with sqlite3.connect(database) as connection:
        assert connection.execute(
            "select count(*) from sqlite_master where type = 'table' and name = 'drift'"
        ).fetchone() == (0,)


def test_sql_interfaces_reject_ambiguous_or_unbounded_configuration(tmp_path: Path) -> None:
    database = tmp_path / "invalid.db"
    with pytest.raises(ValueError, match="batch_size"):
        fpstreams.rows.from_sqlite(database, "select 1", batch_size=0)
    with pytest.raises(ValueError, match="empty rows"):
        fpstreams.rows([]).to_sqlite(database, "empty")
    with pytest.raises(ValueError, match="column type"):
        fpstreams.rows([]).to_sqlite(database, "bad", schema={"id": "DROP TABLE x"})
    with pytest.raises(TypeError, match="not a string"):
        fpstreams.rows([]).to_sqlite(database, "bad", columns="id")

    with sqlite3.connect(database) as connection:
        connection.execute("create table source (value integer)")
        connection.execute("insert into source values (1)")
    with pytest.raises(fpstreams.DuplicateKeyError, match="duplicate column"):
        fpstreams.rows.from_sqlite(
            database, "select value as duplicate, value as duplicate from source"
        ).to_list()
