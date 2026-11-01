"""tabular sql regression contracts."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import closing
from pathlib import Path
from typing import Any

import pytest

import fpstreams


@pytest.mark.parametrize("column", ["select", 'se"lect'])
def test_sqlite_table_sink_and_query_source_form_a_concise_round_trip(
    tmp_path: Path, column: str
) -> None:
    database = tmp_path / "events.db"
    values = [
        {"id": 1, column: "a", "amount": 2.0},
        {"id": 2, column: "b", "amount": 3.5},
        {"id": 3, column: "a", "amount": 4.0},
    ]
    assert fpstreams.rows(values).to_sqlite(
        database, 'event "log"', if_exists="replace", batch_size=1
    ) == len(values)

    selected = fpstreams.rows.from_sqlite(
        database,
        'select * from "event ""log""" where amount >= ? order by id',
        (3.0,),
        batch_size=1,
    )
    expected = [values[1], values[2]]
    assert selected.to_list() == expected
    assert selected.to_list() == expected
    assert selected.where(**{column: "a"}).select("id", "amount").to_list() == [
        {"id": 3, "amount": 4.0}
    ]


@pytest.mark.parametrize("exists", [False, True])
@pytest.mark.parametrize("mode", ["append", "fail", "replace"])
@pytest.mark.parametrize("configuration", ["inferred", "columns", "schema"])
def test_sqlite_empty_sink_preserves_destination_policy(
    tmp_path: Path, exists: bool, mode: str, configuration: str
) -> None:
    database = tmp_path / "empty.db"
    if exists:
        with closing(sqlite3.connect(database)) as connection, connection:
            connection.execute("CREATE TABLE events (id INTEGER)")
            connection.execute("INSERT INTO events VALUES (42)")
    options: dict[str, Any] = {}
    if configuration == "columns":
        options["columns"] = ("id",)
    elif configuration == "schema":
        options["schema"] = {"id": "INTEGER"}

    creating = not exists or mode == "replace"
    fails = exists and mode == "fail"
    missing_names = creating and configuration == "inferred"
    if fails or missing_names:
        with pytest.raises(ValueError, match="already exists" if fails else "empty rows"):
            fpstreams.rows([]).to_sqlite(database, "events", if_exists=mode, **options)
    else:
        assert fpstreams.rows([]).to_sqlite(database, "events", if_exists=mode, **options) == 0

    with closing(sqlite3.connect(database)) as connection:
        found = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'events'"
        ).fetchone()
        if not exists and missing_names:
            assert found is None
        else:
            assert found == ("events",)
            kept = exists and (mode != "replace" or missing_names)
            assert connection.execute("SELECT id FROM events").fetchall() == (
                [(42,)] if kept else []
            )


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

    # sqlite3.Connection commits on context exit but does not close itself. Pair
    # transaction ownership with ``closing`` so newer CPython versions see no leak.
    with closing(sqlite3.connect(database)) as connection, connection:
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


@pytest.mark.parametrize("mode", ["append", "replace"])
def test_sqlite_sink_matches_existing_table_names_without_ascii_case(
    tmp_path: Path, mode: str
) -> None:
    database = tmp_path / "case.db"
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute('CREATE TABLE "Events" (id INTEGER)')
        connection.execute('INSERT INTO "Events" VALUES (10)')

    assert fpstreams.rows([{"id": 2}]).to_sqlite(database, "events", if_exists=mode) == 1
    expected = [{"id": 10}, {"id": 2}] if mode == "append" else [{"id": 2}]
    assert fpstreams.rows.from_sqlite(database, 'SELECT * FROM "Events"').to_list() == expected


@pytest.mark.parametrize(
    ("kind", "mode"),
    [("table", "fail"), ("view", "append"), ("view", "fail"), ("view", "replace")],
)
def test_sqlite_sink_rejects_existing_case_variant_before_reading(
    tmp_path: Path, kind: str, mode: str
) -> None:
    database = tmp_path / "case-error.db"
    with closing(sqlite3.connect(database)) as connection, connection:
        statement = (
            'CREATE TABLE "Events" (id INTEGER)'
            if kind == "table"
            else 'CREATE VIEW "Events" AS SELECT 10 AS id'
        )
        connection.execute(statement)

    pulled: list[bool] = []

    def source() -> Iterator[dict[str, int]]:
        pulled.append(True)
        yield {"id": 2}

    message = "already exists" if kind == "table" else "view, not a table"
    with pytest.raises(ValueError, match=message):
        fpstreams.rows(source()).to_sqlite(database, "events", if_exists=mode)
    assert pulled == []


def test_sqlite_sink_matches_existing_column_names_without_ascii_case(tmp_path: Path) -> None:
    database = tmp_path / "column-case.db"
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute('CREATE TABLE events ("ID" INTEGER, "Value" TEXT)')

    assert fpstreams.rows([{"id": 2, "value": "written"}]).to_sqlite(database, "events") == 1
    assert fpstreams.rows.from_sqlite(database, "SELECT * FROM events").to_list() == [
        {"ID": 2, "Value": "written"}
    ]


@pytest.mark.parametrize("configuration", ["inferred", "columns", "schema"])
def test_sqlite_sink_rejects_ascii_aliases_for_the_same_column(
    tmp_path: Path, configuration: str
) -> None:
    database = tmp_path / "duplicate-columns.db"
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute("CREATE TABLE events (id INTEGER)")
        connection.execute("INSERT INTO events VALUES (10)")

    options: dict[str, Any] = {}
    if configuration == "columns":
        options["columns"] = ("id", "ID")
    elif configuration == "schema":
        options["schema"] = {"id": "INTEGER", "ID": "INTEGER"}
        options["if_exists"] = "replace"
    pulled: list[bool] = []

    def source() -> Iterator[dict[str, int]]:
        pulled.append(True)
        yield {"id": 2, "ID": 3}

    with pytest.raises(fpstreams.DuplicateKeyError):
        fpstreams.rows(source()).to_sqlite(database, "events", **options)
    assert pulled == ([True] if configuration == "inferred" else [])
    assert fpstreams.rows.from_sqlite(database, "SELECT * FROM events").to_list() == [{"id": 10}]


def test_sqlite_sink_keeps_non_ascii_identifier_case_distinct(tmp_path: Path) -> None:
    database = tmp_path / "unicode-case.db"
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute('CREATE TABLE "Ävents" ("Ä" INTEGER)')

    assert fpstreams.rows([{"ä": 2}]).to_sqlite(database, "ävents", if_exists="fail") == 1
    with pytest.raises(ValueError, match="has no columns"):
        fpstreams.rows([{"ä": 3}]).to_sqlite(database, "Ävents")
    assert fpstreams.rows.from_sqlite(database, 'SELECT * FROM "ävents"').to_list() == [{"ä": 2}]
    assert fpstreams.rows.from_sqlite(database, 'SELECT * FROM "Ävents"').to_list() == []
    assert fpstreams.rows([{"Ä": 4, "ä": 5}]).to_sqlite(database, "mixed") == 1
    assert fpstreams.rows.from_sqlite(database, "SELECT * FROM mixed").to_list() == [
        {"Ä": 4, "ä": 5}
    ]


def test_sqlite_sink_rolls_back_batches_and_schema_changes_on_error(tmp_path: Path) -> None:
    database = tmp_path / "atomic.db"
    with closing(sqlite3.connect(database)) as connection, connection:
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
    with closing(sqlite3.connect(database)) as connection, connection:
        assert connection.execute(
            "select count(*) from sqlite_master where type = 'table' and name = 'drift'"
        ).fetchone() == (0,)


def test_sqlite_sink_preserves_primary_error_and_attempts_every_cleanup(  # noqa: C901
    tmp_path: Path, monkeypatch
) -> None:
    primary = ValueError("row pull failed")
    events: list[str] = []

    class Source(Iterator[dict[str, int]]):
        def __init__(self) -> None:
            self.emitted = False
            self.close_calls = 0

        def __next__(self) -> dict[str, int]:
            if self.emitted:
                raise primary
            self.emitted = True
            return {"id": 1}

        def close(self) -> None:
            self.close_calls += 1
            events.append("source.close")
            raise OSError("source close failed")

    class Cursor:
        def __init__(self) -> None:
            self.close_calls = 0

        def execute(self, _statement: str, _parameters: object = None) -> None:
            return None

        def fetchone(self) -> None:
            return None

        def executemany(self, _statement: str, _values: object) -> None:
            return None

        def close(self) -> None:
            self.close_calls += 1
            events.append("cursor.close")
            raise KeyError("cursor close failed")

    class Connection:
        def __init__(self, cursor: Cursor) -> None:
            self._cursor = cursor
            self.rollback_calls = 0
            self.close_calls = 0

        def cursor(self) -> Cursor:
            return self._cursor

        def commit(self) -> None:
            return None

        def rollback(self) -> None:
            self.rollback_calls += 1
            events.append("rollback")
            raise RuntimeError("rollback failed")

        def close(self) -> None:
            self.close_calls += 1
            events.append("connection.close")
            raise LookupError("connection close failed")

    from fpstreams.tabular.io import database as sqlite_sink

    source = Source()
    cursor = Cursor()
    connection = Connection(cursor)
    monkeypatch.setattr(sqlite_sink.sqlite3, "connect", lambda *_args, **_kwargs: connection)

    class DirectRows(fpstreams.Rows[dict[str, int]]):
        def __init__(self) -> None:
            pass

        def __iter__(self) -> Iterator[dict[str, int]]:
            return source

    with pytest.raises(ValueError) as captured:
        DirectRows().to_sqlite(tmp_path / "unused.db", "events", batch_size=1)

    assert captured.value is primary
    assert captured.value.__notes__ == [
        "cleanup failed: RuntimeError: rollback failed",
        "cleanup failed: OSError: source close failed",
        "cleanup failed: KeyError: 'cursor close failed'",
        "cleanup failed: LookupError: connection close failed",
    ]
    assert events == ["rollback", "source.close", "cursor.close", "connection.close"]
    assert source.close_calls == cursor.close_calls == connection.close_calls == 1
    assert connection.rollback_calls == 1


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

    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute("create table source (value integer)")
        connection.execute("insert into source values (1)")
    with pytest.raises(fpstreams.DuplicateKeyError, match="duplicate column"):
        fpstreams.rows.from_sqlite(
            database, "select value as duplicate, value as duplicate from source"
        ).to_list()


def test_feedback_sqlite_filtering_and_single_transaction_batches(tmp_path):
    import sqlite3

    database = tmp_path / "analysis.sqlite"
    records = [
        {"source": "tracking", "version": 1, "team": 7, "points": 3},
        {"source": "tracking", "version": 2, "team": 7, "points": 1},
        {"source": "tracking", "version": 2, "team": 8, "points": 3},
        {"source": "other", "version": 2, "team": 7, "points": 99},
    ]
    assert fpstreams.rows(records).to_sqlite(database, "matches", batch_size=1) == 4
    selected = fpstreams.Rows.from_sqlite(
        database,
        "SELECT team, points FROM matches WHERE source = ? AND version = ? ORDER BY team",
        ("tracking", 2),
        batch_size=1,
    )
    expected = [{"team": 7, "total": 1}, {"team": 8, "total": 3}]
    assert (
        selected.group_by("team").aggregate(total=fpstreams.agg.sum("points")).to_list() == expected
    )
    assert (
        selected.group_by("team").aggregate(total=fpstreams.agg.sum("points")).to_list() == expected
    )
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute("CREATE TABLE results (id INTEGER PRIMARY KEY, points INTEGER)")
        connection.execute("INSERT INTO results VALUES (0, 10)")
    with pytest.raises(sqlite3.IntegrityError):
        fpstreams.rows([{"id": 1, "points": 3}, {"id": 0, "points": 99}]).to_sqlite(
            database, "results", batch_size=1
        )
    with closing(sqlite3.connect(database)) as connection, connection:
        assert connection.execute("SELECT * FROM results").fetchall() == [(0, 10)]
