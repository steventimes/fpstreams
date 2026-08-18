"""Rows expressions, joins, aggregation, reshaping, text I/O, cleaning, and spilling."""

from __future__ import annotations

import gc
import sqlite3
import subprocess
import sys
import weakref
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

import fpstreams
from fpstreams import flow

# --- Tests consolidated from test_rows_api.py ---


def _square(value: int) -> int:
    return value * value


@dataclass(frozen=True)
class _CollidingKey:
    value: int

    def __hash__(self) -> int:
        return 1


def test_rows_turns_record_etl_into_one_readable_pipeline() -> None:
    orders = [
        {"customer": "Ada", "country": "UK", "status": "paid", "amount": 20},
        {"customer": "Lin", "country": "US", "status": "open", "amount": 90},
        {"customer": "Ada", "country": "UK", "status": "paid", "amount": 30},
        {"customer": "Max", "country": "US", "status": "paid", "amount": 80},
    ]

    report = (
        fpstreams.rows(orders)
        .where(status="paid")
        .with_columns(net=lambda row: row["amount"] * 0.8)
        .group_by("country")
        .aggregate(orders=fpstreams.agg.count(), revenue=fpstreams.agg.sum("net"))
        .sort_by("revenue", reverse=True)
        .to_list()
    )

    assert report == [
        {"country": "US", "orders": 1, "revenue": 64.0},
        {"country": "UK", "orders": 2, "revenue": 40.0},
    ]


def test_rows_keeps_basic_table_navigation_in_the_same_chain() -> None:
    table = fpstreams.rows([{"id": 1}, {"id": 1}, {"id": 2}]).unique_by("id")

    assert table.count() == 2
    assert table.skip(1).take(1).first() == {"id": 2}
    assert fpstreams.rows([{"a": 1}, {"a": 2, "b": 3}]).to_columns() == {
        "a": [1, 2],
        "b": [None, 3],
    }


def test_to_columns_consumes_evolving_rows_once_without_retaining_them() -> None:
    shared: dict[str, int] = {}

    def records():
        for record in ({"a": 1}, {"a": 2, "b": 3}, {"b": 4, "c": 5}):
            shared.clear()
            shared.update(record)
            yield shared

    expected = {
        "a": [1, 2, None],
        "b": [None, 3, 4],
        "c": [None, None, 5],
    }
    assert flow(records()).collect(fpstreams.Collectors.to_columns()) == expected
    assert fpstreams.rows(records()).to_columns() == expected


def test_column_expressions_remove_row_lambda_noise() -> None:
    orders = [
        {"customer": "Ada", "status": "paid", "amount": 20},
        {"customer": "Lin", "status": "open", "amount": 90},
        {"customer": "Max", "status": "paid", "amount": 80},
    ]

    result = (
        fpstreams.rows(orders)
        .where((fpstreams.col("status") == "paid") & (fpstreams.col("amount") > 25))
        .with_columns(
            net=fpstreams.col("amount") * 0.8,
            band=fpstreams.when(fpstreams.col("amount") >= 75, "large", "small"),
        )
        .select("customer", "net", "band")
        .to_list()
    )

    assert result == [{"customer": "Max", "net": 64.0, "band": "large"}]


def test_row_expression_fill_null_evaluates_its_input_once() -> None:
    evaluations = 0

    def evaluate(row: dict[str, str | None]) -> str | None:
        nonlocal evaluations
        evaluations += 1
        return row["value"]

    expression = fpstreams.col("value").map(lambda value: evaluate({"value": value}))

    assert expression.fill_null("missing")({"value": "present"}) == "present"
    assert evaluations == 1
    assert (
        expression.fill_null(fpstreams.col("fallback"))({"value": None, "fallback": "used"})
        == "used"
    )
    assert evaluations == 2


def test_rows_join_and_select_replace_nested_lookup_loops() -> None:
    orders = [
        {"order_id": 10, "user_id": 1, "amount": 50},
        {"order_id": 11, "user_id": 3, "amount": 20},
    ]
    users = [
        {"user_id": 1, "name": "Ada"},
        {"user_id": 2, "name": "Lin"},
    ]

    joined = (
        fpstreams.rows(orders)
        .join(users, on="user_id", how="left")
        .select("order_id", "name", total="amount")
        .to_list()
    )

    assert joined == [
        {"order_id": 10, "name": "Ada", "total": 50},
        {"order_id": 11, "name": None, "total": 20},
    ]


def test_common_rows_joins_stream_the_left_side() -> None:
    opened: list[int] = []

    def left_rows():
        for value in range(100):
            opened.append(value)
            yield {"id": value}

    joined = fpstreams.rows(left_rows()).join([{"id": 0, "name": "zero"}], on="id")
    iterator = iter(joined)

    assert next(iterator) == {"id": 0, "name": "zero"}
    assert opened == [0]
    iterator.close()


def test_semi_join_indexes_only_keys_without_retaining_right_payloads() -> None:
    class Payload:
        pass

    references: list[weakref.ReferenceType[Payload]] = []

    def right_rows():
        for value in range(20):
            payload = Payload()
            references.append(weakref.ref(payload))
            yield {"id": value, "payload": payload}

    joined = fpstreams.rows([{"id": 0}]).join(right_rows(), on="id", how="semi")
    iterator = iter(joined)
    assert next(iterator) == {"id": 0}
    gc.collect()
    assert all(reference() is None for reference in references)
    iterator.close()


def test_join_modes_preserve_duplicate_key_order_and_cardinality() -> None:
    left = [
        {"id": 1, "left": "a"},
        {"id": 1, "left": "b"},
        {"id": 3, "left": "c"},
    ]
    right = [
        {"id": 1, "right": "x"},
        {"id": 1, "right": "y"},
        {"id": 2, "right": "z"},
    ]
    matched = [
        {"id": 1, "left": "a", "right": "x"},
        {"id": 1, "left": "a", "right": "y"},
        {"id": 1, "left": "b", "right": "x"},
        {"id": 1, "left": "b", "right": "y"},
    ]

    def join(how: str):
        return fpstreams.rows(left).join(right, on="id", how=how).to_list()

    assert join("inner") == matched
    assert join("left") == [*matched, {"id": 3, "left": "c", "right": None}]
    assert join("right") == [*matched, {"id": 2, "left": None, "right": "z"}]
    assert join("full") == [
        *matched,
        {"id": 3, "left": "c", "right": None},
        {"id": 2, "left": None, "right": "z"},
    ]
    assert join("semi") == left[:2]
    assert join("anti") == left[2:]


def test_join_validation_is_checked_before_sources_are_opened() -> None:
    opened = False

    def records():
        nonlocal opened
        opened = True
        yield {"id": 1}

    with pytest.raises(ValueError, match="validate must be one of"):
        fpstreams.rows(records()).join(records(), on="id", validate="one-to-one")

    assert not opened


def test_join_cardinality_contracts_accept_matching_inputs() -> None:
    left_unique = [{"id": 1, "left": "a"}]
    left_duplicate = [{"id": 1, "left": "a"}, {"id": 1, "left": "b"}]
    right_unique = [{"id": 1, "right": "x"}]
    right_duplicate = [{"id": 1, "right": "x"}, {"id": 1, "right": "y"}]

    assert (
        len(fpstreams.rows(left_unique).join(right_duplicate, on="id", validate="1:m").to_list())
        == 2
    )
    assert (
        len(fpstreams.rows(left_duplicate).join(right_unique, on="id", validate="m:1").to_list())
        == 2
    )
    assert (
        len(fpstreams.rows(left_unique).join(right_unique, on="id", validate="1:1").to_list()) == 1
    )
    assert (
        len(fpstreams.rows(left_duplicate).join(right_duplicate, on="id", validate="m:m").to_list())
        == 4
    )


@pytest.mark.parametrize(
    ("validate", "duplicate_side"),
    [("1:m", "left"), ("1:1", "left"), ("m:1", "right"), ("1:1", "right")],
)
def test_join_cardinality_contracts_reject_duplicate_keys(
    validate: str, duplicate_side: str
) -> None:
    unique = [{"id": 1}]
    duplicate = [{"id": 1}, {"id": 1}]
    left = duplicate if duplicate_side == "left" else unique
    right = duplicate if duplicate_side == "right" else unique

    with pytest.raises(
        ValueError,
        match=rf"validate={validate!r} requires unique {duplicate_side} keys.*1",
    ):
        fpstreams.rows(left).join(right, on="id", validate=validate).to_list()


@pytest.mark.parametrize(
    ("how", "validate", "duplicate_side"),
    [("semi", "1:m", "left"), ("anti", "m:1", "right")],
)
def test_semi_and_anti_joins_honor_cardinality_contracts(
    how: str, validate: str, duplicate_side: str
) -> None:
    unique = [{"id": 1}]
    duplicate = [{"id": 1}, {"id": 1}]
    left = duplicate if duplicate_side == "left" else unique
    right = duplicate if duplicate_side == "right" else unique

    with pytest.raises(ValueError, match=rf"unique {duplicate_side} keys"):
        fpstreams.rows(left).join(
            right,
            on="id",
            how=how,
            validate=validate,
        ).to_list()


def test_rows_supports_relational_join_modes_and_different_keys() -> None:
    left = [{"left_id": 1, "value": "a"}, {"left_id": 3, "value": "c"}]
    right = [{"right_id": 1, "value": "A"}, {"right_id": 2, "value": "B"}]

    assert fpstreams.rows(left).join(
        right, left_on="left_id", right_on="right_id", how="full"
    ).to_list() == [
        {"left_id": 1, "value": "a", "right_id": 1, "value_right": "A"},
        {"left_id": 3, "value": "c", "right_id": None, "value_right": None},
        {"left_id": None, "value": None, "right_id": 2, "value_right": "B"},
    ]
    assert fpstreams.rows(left).join(
        right, left_on="left_id", right_on="right_id", how="semi"
    ).to_list() == [left[0]]
    assert fpstreams.rows(left).join(
        right, left_on="left_id", right_on="right_id", how="anti"
    ).to_list() == [left[1]]


def test_rows_join_accepts_readable_composite_keys() -> None:
    events = [
        {"tenant": "a", "user_id": 1, "event": "open"},
        {"tenant": "b", "user_id": 1, "event": "click"},
    ]
    users = [
        {"tenant": "a", "id": 1, "name": "Ada"},
        {"tenant": "b", "id": 2, "name": "Lin"},
    ]

    assert fpstreams.rows(events).join(
        users,
        left_on=("tenant", "user_id"),
        right_on=("tenant", "id"),
        how="left",
    ).select("event", "name").to_list() == [
        {"event": "open", "name": "Ada"},
        {"event": "click", "name": None},
    ]


def test_rows_reshapes_columns_and_has_practical_aggregators() -> None:
    wide = [{"id": 1, "q1": 10, "q2": 20}]

    assert fpstreams.rows(wide).rename(id="account").drop("q2").to_list() == [
        {"account": 1, "q1": 10}
    ]
    assert fpstreams.rows(wide).unpivot(
        "q1", "q2", names_to="quarter", values_to="sales"
    ).to_list() == [
        {"id": 1, "quarter": "q1", "sales": 10},
        {"id": 1, "quarter": "q2", "sales": 20},
    ]

    summary = (
        fpstreams.rows([{"team": "x", "score": score} for score in (3, 1, 4)])
        .group_by("team")
        .aggregate(
            low=fpstreams.agg.min("score"),
            high=fpstreams.agg.max("score"),
            first=fpstreams.agg.first("score"),
            last=fpstreams.agg.last("score"),
            scores=fpstreams.agg.collect("score", into=tuple),
        )
        .to_list()
    )
    assert summary == [
        {"team": "x", "low": 1, "high": 4, "first": 3, "last": 4, "scores": (3, 1, 4)}
    ]

    with pytest.raises(fpstreams.DuplicateKeyError):
        fpstreams.rows(wide).unpivot("q1", names_to="value", values_to="value")
    with pytest.raises(fpstreams.DuplicateKeyError):
        fpstreams.rows(wide).unpivot("q1", "q1")


def test_rows_computes_stable_online_statistics_and_distinct_counts() -> None:
    records = [
        {"team": "a", "score": 1_000_000_000_001.0, "tags": ["x"]},
        {"team": "a", "score": 1_000_000_000_002.0, "tags": ["x"]},
        {"team": "a", "score": 1_000_000_000_003.0, "tags": ["y"]},
        {"team": "b", "score": 7.0, "tags": ["z"]},
    ]

    summary = (
        fpstreams.rows(records)
        .group_by("team")
        .aggregate(
            sample_variance=fpstreams.agg.variance("score"),
            population_variance=fpstreams.agg.variance("score", ddof=0),
            sample_std=fpstreams.agg.std("score"),
            distinct_tags=fpstreams.agg.count_distinct("tags"),
        )
        .to_list()
    )

    assert summary == [
        {
            "team": "a",
            "sample_variance": 1.0,
            "population_variance": pytest.approx(2 / 3),
            "sample_std": 1.0,
            "distinct_tags": 2,
        },
        {
            "team": "b",
            "sample_variance": None,
            "population_variance": 0.0,
            "sample_std": None,
            "distinct_tags": 1,
        },
    ]
    with pytest.raises(ValueError, match="ddof"):
        fpstreams.agg.variance("score", ddof=-1)


def test_grouped_rows_initializes_aggregations_once_per_group() -> None:
    initializer_calls = 0

    def initializer() -> int:
        nonlocal initializer_calls
        initializer_calls += 1
        return 0

    total = fpstreams.Aggregator(
        initializer,
        lambda state, row: state + row["score"],
    )
    result = (
        fpstreams.rows(
            [
                {"team": "a", "score": 1},
                {"team": "a", "score": 2},
                {"team": "b", "score": 3},
            ]
        )
        .group_by("team")
        .aggregate(total=total)
        .to_list()
    )

    assert result == [{"team": "a", "total": 3}, {"team": "b", "total": 3}]
    assert initializer_calls == 2


def test_rows_names_computed_group_keys_and_supports_boolean_aggregations() -> None:
    records = [
        {"team": "a", "amount": 10, "paid": True},
        {"team": "a", "amount": 15, "paid": False},
        {"team": "a", "amount": 30, "paid": True},
        {"team": "b", "amount": 5, "paid": True},
    ]
    result = (
        fpstreams.rows(records)
        .group_by("team", band=lambda row: "high" if row["amount"] >= 20 else "low")
        .aggregate(
            rows=fpstreams.agg.count(),
            paid=fpstreams.agg.count_where("paid"),
            any_large=fpstreams.agg.any(lambda row: row["amount"] >= 30),
            all_positive=fpstreams.agg.all(lambda row: row["amount"] > 0),
        )
        .to_list()
    )

    assert result == [
        {
            "team": "a",
            "band": "low",
            "rows": 2,
            "paid": 1,
            "any_large": False,
            "all_positive": True,
        },
        {
            "team": "a",
            "band": "high",
            "rows": 1,
            "paid": 1,
            "any_large": True,
            "all_positive": True,
        },
        {
            "team": "b",
            "band": "low",
            "rows": 1,
            "paid": 1,
            "any_large": False,
            "all_positive": True,
        },
    ]
    assert fpstreams.rows([]).aggregate(
        any_paid=fpstreams.agg.any("paid"),
        all_paid=fpstreams.agg.all("paid"),
        paid=fpstreams.agg.count_where("paid"),
    ).first() == {"any_paid": False, "all_paid": True, "paid": 0}
    with pytest.raises(fpstreams.DuplicateKeyError):
        fpstreams.rows(records).group_by("team", team="paid")
    with pytest.raises(TypeError, match="hashable"):
        fpstreams.rows([{"tags": ["x"]}]).group_by("tags").aggregate(
            rows=fpstreams.agg.count()
        ).to_list()


def test_rows_global_aggregate_is_chainable_and_handles_empty_input() -> None:
    summary = fpstreams.rows(
        [
            {"score": 1.0, "tag": ["x"]},
            {"score": 2.0, "tag": ["x"]},
            {"score": 3.0, "tag": ["y"]},
        ]
    ).aggregate(
        rows=fpstreams.agg.count(),
        total=fpstreams.agg.sum("score"),
        variance=fpstreams.agg.variance("score"),
        tags=fpstreams.agg.count_distinct("tag"),
    )

    expected = [{"rows": 3, "total": 6.0, "variance": 1.0, "tags": 2}]
    assert summary.to_list() == expected
    assert summary.to_list() == expected
    assert fpstreams.rows([]).aggregate(
        rows=fpstreams.agg.count(), mean=fpstreams.agg.mean("score")
    ).to_list() == [{"rows": 0, "mean": None}]
    with pytest.raises(ValueError, match="aggregate"):
        fpstreams.rows([]).aggregate()


def test_rows_pivots_long_data_with_explicit_duplicate_policy() -> None:
    sales = [
        {"store": "a", "quarter": "q1", "amount": 10},
        {"store": "a", "quarter": "q1", "amount": 2},
        {"store": "a", "quarter": "q2", "amount": 20},
        {"store": "b", "quarter": "q1", "amount": 5},
    ]

    result = (
        fpstreams.rows(sales)
        .pivot(
            index="store",
            columns="quarter",
            values="amount",
            aggregate="sum",
            fill=0,
        )
        .to_list()
    )

    assert result == [
        {"store": "a", "q1": 12, "q2": 20},
        {"store": "b", "q1": 5, "q2": 0},
    ]


def test_rows_rejects_ambiguous_output_columns() -> None:
    assert (
        fpstreams.rows([{"id": 1, "value": "left", "value_right": "unused"}])
        .join([{"id": 2, "value": "right"}], on="id", how="inner")
        .to_list()
        == []
    )

    with pytest.raises(fpstreams.DuplicateKeyError):
        fpstreams.rows([{"id": 1, "value": "left", "value_right": "preserve"}]).join(
            [{"id": 1, "value": "right"}], on="id"
        ).to_list()

    with pytest.raises(fpstreams.DuplicateKeyError):
        fpstreams.rows([{"a": 1, "b": 2}]).select("a", a="b")

    with pytest.raises(fpstreams.DuplicateKeyError):
        fpstreams.rows([{"team": "x", "score": 1}]).group_by("team").aggregate(
            team=fpstreams.agg.count()
        )


def test_rows_rejects_duplicate_derived_group_and_pivot_keys() -> None:
    records = [{"left": {"id": 1}, "right": {"id": 2}, "value": 3}]

    with pytest.raises(fpstreams.DuplicateKeyError):
        fpstreams.rows(records).group_by("left.id", "right.id").aggregate(
            total=fpstreams.agg.sum("value")
        )

    with pytest.raises(fpstreams.DuplicateKeyError):
        fpstreams.rows(records).pivot(
            index=("left.id", "right.id"),
            columns="value",
            values="value",
        )


def test_rows_stream_csv_and_json_lines_round_trips(tmp_path) -> None:
    records = [{"name": "Ada", "score": 10}, {"name": "Lin", "score": 20}]
    csv_path = tmp_path / "scores.csv"
    jsonl_path = tmp_path / "scores.jsonl"

    fpstreams.rows(records).to_csv(csv_path)
    csv_rows = fpstreams.rows.from_csv(csv_path)
    assert csv_rows.to_list() == [
        {"name": "Ada", "score": "10"},
        {"name": "Lin", "score": "20"},
    ]
    assert csv_rows.to_list()[0]["name"] == "Ada"

    fpstreams.rows(records).to_jsonl(jsonl_path)
    assert fpstreams.rows.from_jsonl(jsonl_path).to_list() == records


def test_rows_csv_can_neutralize_spreadsheet_formula_cells(tmp_path) -> None:
    path = tmp_path / "safe-rows.csv"

    fpstreams.rows([{"formula": "+1", "spaced": "  -2", "command": "@run", "number": -3}]).to_csv(
        path, spreadsheet_safe=True
    )

    assert path.read_text(encoding="utf-8") == (
        "formula,spaced,command,number\n'+1,'  -2,'@run,-3\n"
    )


def test_jsonl_record_limit_counts_bytes_before_decoding(tmp_path) -> None:
    path = tmp_path / "bounded.jsonl"
    payload = '{"word":"雪"}\n'.encode()
    path.write_bytes(payload)

    assert fpstreams.rows.from_jsonl(path, max_record_bytes=len(payload)).to_list() == [
        {"word": "雪"}
    ]
    assert fpstreams.rows.from_jsonl(path, max_record_bytes=None).to_list() == [{"word": "雪"}]
    with pytest.raises(fpstreams.BufferLimitError, match="max_record_bytes"):
        fpstreams.rows.from_jsonl(path, max_record_bytes=len(payload) - 1).to_list()


def test_jsonl_record_limit_is_validated_before_opening_the_file(tmp_path) -> None:
    missing = tmp_path / "missing.jsonl"

    with pytest.raises(ValueError, match="max_record_bytes"):
        fpstreams.rows.from_jsonl(missing, max_record_bytes=0)
    with pytest.raises(TypeError, match="max_record_bytes"):
        fpstreams.rows.from_jsonl(missing, max_record_bytes=2.5)  # type: ignore[arg-type]

    deferred = fpstreams.rows.from_jsonl(missing)
    with pytest.raises(FileNotFoundError):
        deferred.to_list()


def test_rows_io_rejects_duplicate_fields_instead_of_losing_data(tmp_path) -> None:
    csv_path = tmp_path / "duplicate.csv"
    jsonl_path = tmp_path / "duplicate.jsonl"
    csv_path.write_text("id,id\n1,2\n", encoding="utf-8")
    jsonl_path.write_text('{"user":{"id":1,"id":2}}\n', encoding="utf-8")

    with pytest.raises(fpstreams.DuplicateKeyError, match="CSV"):
        fpstreams.rows.from_csv(csv_path).to_list()
    with pytest.raises(fpstreams.DuplicateKeyError, match="JSON Lines"):
        fpstreams.rows.from_jsonl(jsonl_path).to_list()
    with pytest.raises(fpstreams.DuplicateKeyError, match="to_csv"):
        fpstreams.rows([{"id": 1}]).to_csv(csv_path, fieldnames=("id", "id"))


def test_rows_clean_nested_records_in_one_readable_pipeline() -> None:
    records = [
        {
            "id": "1",
            "amount": "2.5",
            "country": None,
            "fallback": "US",
            "tags": ["new", "paid"],
            "profile": {"age": "30", "city": "Boston"},
        },
        {
            "id": "2",
            "amount": None,
            "country": "CA",
            "fallback": None,
            "tags": ["ignored"],
            "profile": {"age": "40", "city": "Toronto"},
        },
        {
            "id": "3",
            "amount": "7",
            "country": None,
            "fallback": None,
            "tags": [],
            "profile": {"age": "25", "city": "Paris"},
        },
    ]

    cleaned = (
        fpstreams.rows(records)
        .drop_nulls("amount")
        .cast(id=int, amount=float)
        .fill_nulls(country=fpstreams.coalesce(fpstreams.col("fallback"), "unknown"))
        .explode("tags", outer=True)
        .unnest("profile", prefix="profile_")
        .drop("fallback")
        .to_list()
    )

    assert cleaned == [
        {
            "id": 1,
            "amount": 2.5,
            "country": "US",
            "tags": "new",
            "profile_age": "30",
            "profile_city": "Boston",
        },
        {
            "id": 1,
            "amount": 2.5,
            "country": "US",
            "tags": "paid",
            "profile_age": "30",
            "profile_city": "Boston",
        },
        {
            "id": 3,
            "amount": 7.0,
            "country": "unknown",
            "tags": None,
            "profile_age": "25",
            "profile_city": "Paris",
        },
    ]
    assert records[0]["id"] == "1"
    assert records[0]["tags"] == ["new", "paid"]


def test_drop_nulls_supports_any_all_and_heterogeneous_rows() -> None:
    records = [
        {"a": 1, "b": 2},
        {"a": 1, "b": None},
        {"a": None, "b": None},
        {},
    ]

    assert fpstreams.rows(records).drop_nulls("a", "b").to_list() == [records[0]]
    assert fpstreams.rows(records[:-1]).drop_nulls("a", "b", how="all").to_list() == records[:2]
    assert fpstreams.rows(records).drop_nulls(how="all").to_list() == records[:2]

    with pytest.raises(ValueError, match="how"):
        fpstreams.rows(records).drop_nulls(how="some")  # type: ignore[arg-type]


def test_explode_is_lazy_closes_upstream_and_rejects_ambiguous_values() -> None:
    closed = False

    def source() -> Iterator[dict[str, object]]:
        nonlocal closed
        try:
            yield {"id": 1, "tags": ["a", "b"]}
            yield {"id": 2, "tags": ["unused"]}
        finally:
            closed = True

    assert fpstreams.rows(source()).explode("tags").take(1).to_list() == [{"id": 1, "tags": "a"}]
    assert closed
    assert fpstreams.rows([{"tags": None}, {"tags": []}]).explode("tags").to_list() == []
    assert fpstreams.rows([{"tags": None}, {"tags": []}]).explode("tags", outer=True).to_list() == [
        {"tags": None},
        {"tags": None},
    ]
    assert fpstreams.rows([{"payload": {"tags": [1, 2]}}]).explode(
        "payload.tags", into="tag"
    ).to_list() == [
        {"payload": {"tags": [1, 2]}, "tag": 1},
        {"payload": {"tags": [1, 2]}, "tag": 2},
    ]

    with pytest.raises(ValueError, match="into"):
        fpstreams.rows([]).explode("payload.tags")
    with pytest.raises(TypeError, match="non-string iterable"):
        fpstreams.rows([{"tags": "abc"}]).explode("tags").to_list()


def test_unnest_and_coalesce_never_hide_collisions_or_extra_work() -> None:
    calls: list[str] = []
    first = fpstreams.RowExpr(lambda row: calls.append("first") or row["first"], "first")
    second = fpstreams.RowExpr(lambda row: calls.append("second") or row["second"], "second")
    expression = fpstreams.coalesce(first, second, "fallback")

    assert expression({"first": "ready", "second": "unused"}) == "ready"
    assert calls == ["first"]
    calls.clear()
    assert expression({"first": None, "second": "used"}) == "used"
    assert calls == ["first", "second"]
    with pytest.raises(ValueError, match="at least one"):
        fpstreams.coalesce()

    with pytest.raises(fpstreams.DuplicateKeyError, match="collides"):
        fpstreams.rows([{"name": "outer", "profile": {"name": "inner"}}]).unnest(
            "profile"
        ).to_list()
    with pytest.raises(fpstreams.SelectionError, match="missing"):
        fpstreams.rows([{"id": 1}]).unnest("profile").to_list()


@pytest.mark.parametrize("how", ["inner", "left", "right", "full", "semi", "anti"])
def test_partitioned_join_matches_stable_in_memory_semantics(how: str, tmp_path) -> None:
    left = [
        {"id": 1, "left": "a"},
        {"id": 1, "left": "b"},
        {"id": 3, "left": "c"},
    ]
    right = [
        {"id": 1, "right": "x"},
        {"id": 1, "right": "y"},
        {"id": 2, "right": "z"},
    ]

    expected = fpstreams.rows(left).join(right, on="id", how=how).to_list()
    actual = (
        fpstreams.rows(left)
        .join(
            right,
            on="id",
            how=how,
            partitions=3,
            tempdir=tmp_path,
        )
        .to_list()
    )

    assert actual == expected
    assert list(tmp_path.iterdir()) == []


def test_partitioned_join_preserves_composite_keys_suffixes_and_sparse_columns() -> None:
    left = [
        {"tenant": "a", "user": 1, "value": "left-a"},
        {"tenant": "b", "user": 1, "value": "left-b", "extra": 9},
    ]
    right = [
        {"tenant": "a", "id": 1, "value": "right-a"},
        {"tenant": "b", "id": 2, "value": "right-b"},
    ]

    regular = (
        fpstreams.rows(left)
        .join(
            right,
            left_on=("tenant", "user"),
            right_on=("tenant", "id"),
            how="full",
            suffix="_lookup",
        )
        .to_list()
    )
    spilled = (
        fpstreams.rows(left)
        .join(
            right,
            left_on=("tenant", "user"),
            right_on=("tenant", "id"),
            how="full",
            suffix="_lookup",
            partitions=4,
        )
        .to_list()
    )

    assert spilled == regular


@pytest.mark.parametrize(
    ("validate", "duplicate_side"),
    [("1:m", "left"), ("m:1", "right")],
)
def test_partitioned_join_validates_cardinality_and_cleans_up(
    validate: str, duplicate_side: str, tmp_path
) -> None:
    unique = [{"id": 1}]
    duplicate = [{"id": 1}, {"id": 1}]
    left = duplicate if duplicate_side == "left" else unique
    right = duplicate if duplicate_side == "right" else unique

    with pytest.raises(ValueError, match=rf"unique {duplicate_side} keys"):
        fpstreams.rows(left).join(
            right,
            on="id",
            validate=validate,
            partitions=3,
            tempdir=tmp_path,
        ).to_list()

    assert list(tmp_path.iterdir()) == []


def test_spilled_group_by_preserves_first_group_and_row_order(tmp_path) -> None:
    records = [
        {"team": "b", "band": 1, "score": 4},
        {"team": "a", "band": 1, "score": 3},
        {"team": "b", "band": 1, "score": 2},
        {"team": "a", "band": 2, "score": 8},
        {"team": "a", "band": 1, "score": 1},
    ]

    expected = (
        fpstreams.rows(records)
        .group_by("team", "band")
        .aggregate(
            count=fpstreams.agg.count(),
            total=fpstreams.agg.sum("score"),
            scores=fpstreams.agg.collect("score", into=tuple),
        )
        .to_list()
    )
    actual = (
        fpstreams.rows(records)
        .group_by("team", "band")
        .spill(3, tempdir=tmp_path)
        .aggregate(
            count=fpstreams.agg.count(),
            total=fpstreams.agg.sum("score"),
            scores=fpstreams.agg.collect("score", into=tuple),
        )
        .to_list()
    )

    assert actual == expected
    assert list(tmp_path.iterdir()) == []


def test_spill_closes_sources_and_temp_files_after_short_circuit(tmp_path) -> None:
    left_closed = right_closed = False

    def left() -> Iterator[dict[str, int]]:
        nonlocal left_closed
        try:
            for value in range(20):
                yield {"id": value}
        finally:
            left_closed = True

    def right() -> Iterator[dict[str, int]]:
        nonlocal right_closed
        try:
            for value in range(20):
                yield {"id": value}
        finally:
            right_closed = True

    iterator = iter(fpstreams.rows(left()).join(right(), on="id", partitions=3, tempdir=tmp_path))
    assert next(iterator) == {"id": 0}
    assert left_closed and right_closed
    assert list(tmp_path.iterdir())
    iterator.close()
    assert list(tmp_path.iterdir()) == []


def test_spill_errors_are_clear_and_cleanup_is_transactional(tmp_path) -> None:
    with pytest.raises(TypeError, match="hashable"):
        (
            fpstreams.rows([{"key": [1], "value": 2}])
            .group_by("key")
            .spill(2, tempdir=tmp_path)
            .aggregate(total=fpstreams.agg.sum("value"))
            .to_list()
        )
    assert list(tmp_path.iterdir()) == []

    assert fpstreams.rows([{"id": 1}]).join(
        [{"id": 1, "payload": lambda: None}],
        on="id",
        how="semi",
        partitions=2,
        tempdir=tmp_path,
    ).to_list() == [{"id": 1}]
    assert list(tmp_path.iterdir()) == []

    with pytest.raises(TypeError, match="picklable"):
        fpstreams.rows([{"id": 1, "value": lambda: None}]).join(
            [{"id": 1}],
            on="id",
            partitions=2,
            tempdir=tmp_path,
        ).to_list()
    assert list(tmp_path.iterdir()) == []


def test_spill_partition_configuration_is_validated() -> None:
    grouped = fpstreams.rows([{"id": 1}]).group_by("id")
    with pytest.raises(ValueError, match="between"):
        grouped.spill(1)
    with pytest.raises(TypeError, match="integer"):
        grouped.spill(2.5)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="tempdir requires"):
        fpstreams.rows([{"id": 1}]).join([{"id": 1}], on="id", tempdir="unused")


def test_spill_limits_are_validated_before_sources_are_consumed() -> None:
    with pytest.raises(ValueError, match="max_partition_rows"):
        fpstreams.SpillLimits(max_partition_rows=0)
    with pytest.raises(TypeError, match="max_output_rows"):
        fpstreams.SpillLimits(max_output_rows=2.5)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="limits requires partitions"):
        fpstreams.rows([{"id": 1}]).join(
            [{"id": 1}],
            on="id",
            limits=fpstreams.SpillLimits(),
        )


def test_spilled_join_rejects_an_oversized_skewed_partition_and_cleans_up(tmp_path) -> None:
    limits = fpstreams.SpillLimits(
        max_partition_rows=2,
        max_partition_bytes=64 * 1024,
        max_matches_per_key=10,
        max_output_rows=10,
        max_repartition_depth=1,
    )

    with pytest.raises(fpstreams.BufferLimitError, match="max_partition_rows"):
        fpstreams.rows([{"id": 1}]).join(
            [{"id": 1, "right": value} for value in range(3)],
            on="id",
            partitions=2,
            tempdir=tmp_path,
            limits=limits,
        ).to_list()

    assert list(tmp_path.iterdir()) == []


def test_spilled_group_rejects_distinct_colliding_keys_without_unbounded_load(tmp_path) -> None:
    limits = fpstreams.SpillLimits(
        max_partition_rows=2,
        max_partition_bytes=64 * 1024,
        max_matches_per_key=10,
        max_output_rows=10,
        max_repartition_depth=1,
    )
    records = [{"key": _CollidingKey(value)} for value in range(3)]

    with pytest.raises(fpstreams.BufferLimitError, match="max_partition_rows"):
        (
            fpstreams.rows(records)
            .group_by("key")
            .spill(2, tempdir=tmp_path, limits=limits)
            .aggregate(count=fpstreams.agg.count())
            .to_list()
        )

    assert list(tmp_path.iterdir()) == []


def test_spill_write_failure_closes_source_and_removes_temporary_files(
    tmp_path, monkeypatch
) -> None:
    from fpstreams.tabular import spill_io

    closed = False

    def records() -> Iterator[dict[str, int]]:
        nonlocal closed
        try:
            yield {"key": 1}
        finally:
            closed = True

    def fail_write(*_args, **_kwargs) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(spill_io, "dump", fail_write)
    with pytest.raises(OSError, match="disk full"):
        (
            fpstreams.rows(records())
            .group_by("key")
            .spill(2, tempdir=tmp_path)
            .aggregate(count=fpstreams.agg.count())
            .to_list()
        )

    assert closed
    assert list(tmp_path.iterdir()) == []


def test_spilled_group_rejects_an_oversized_serialized_partition(tmp_path) -> None:
    limits = fpstreams.SpillLimits(
        max_partition_rows=10,
        max_partition_bytes=512,
        max_matches_per_key=10,
        max_output_rows=10,
        max_repartition_depth=0,
    )

    with pytest.raises(fpstreams.BufferLimitError, match="max_partition_bytes"):
        (
            fpstreams.rows([{"key": 1, "payload": "x" * 5_000}])
            .group_by("key")
            .spill(2, tempdir=tmp_path, limits=limits)
            .aggregate(count=fpstreams.agg.count())
            .to_list()
        )

    assert list(tmp_path.iterdir()) == []


def test_spilled_join_guards_matches_and_total_output(tmp_path) -> None:
    matches_limited = fpstreams.SpillLimits(
        max_partition_rows=10,
        max_partition_bytes=64 * 1024,
        max_matches_per_key=2,
        max_output_rows=10,
        max_repartition_depth=0,
    )
    with pytest.raises(fpstreams.BufferLimitError, match="max_matches_per_key"):
        fpstreams.rows([{"id": 1, "left": "a"}]).join(
            [{"id": 1, "right": value} for value in range(3)],
            on="id",
            partitions=2,
            tempdir=tmp_path,
            limits=matches_limited,
        ).to_list()
    assert list(tmp_path.iterdir()) == []

    output_limited = fpstreams.SpillLimits(
        max_partition_rows=10,
        max_partition_bytes=64 * 1024,
        max_matches_per_key=10,
        max_output_rows=3,
        max_repartition_depth=0,
    )
    left = [{"id": 1, "left": value} for value in range(2)]
    right = [{"id": 1, "right": value} for value in range(2)]
    with pytest.raises(fpstreams.BufferLimitError, match="max_output_rows"):
        fpstreams.rows(left).join(
            right,
            on="id",
            partitions=2,
            tempdir=tmp_path,
            limits=output_limited,
        ).to_list()
    assert list(tmp_path.iterdir()) == []

    exact_limit = fpstreams.SpillLimits(
        max_partition_rows=10,
        max_partition_bytes=64 * 1024,
        max_matches_per_key=10,
        max_output_rows=4,
        max_repartition_depth=0,
    )
    assert (
        len(
            fpstreams.rows(left)
            .join(
                right,
                on="id",
                partitions=2,
                tempdir=tmp_path,
                limits=exact_limit,
            )
            .to_list()
        )
        == 4
    )
    assert list(tmp_path.iterdir()) == []


# --- Tests consolidated from test_data_adapters.py ---

"""Dataframe protocols plus Arrow, Polars, Parquet, and SQL source and sink adapters."""


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


def test_row_expression_ir_covers_operator_and_call_families() -> None:
    row = {
        "value": 3,
        "negative": -4,
        "text": " AbC ",
        "items": [10, 20],
        "nested": {"amount": 5},
        "missing": None,
    }
    expressions = [
        (10 + fpstreams.col("value"), 13),
        (10 - fpstreams.col("value"), 7),
        (4 * fpstreams.col("value"), 12),
        (12 / fpstreams.col("value"), 4),
        (7 // fpstreams.col("value"), 2),
        (7 % fpstreams.col("value"), 1),
        (2 ** fpstreams.col("value"), 8),
        (-fpstreams.col("value"), -3),
        (abs(fpstreams.col("negative")), 4),
        (fpstreams.col("items")[1], 20),
        (fpstreams.col("value").cast(float), 3.0),
        (fpstreams.col("value").isin([1, 3]), True),
        (fpstreams.col("missing").is_null(), True),
        (fpstreams.col("value").is_not_null(), True),
        (fpstreams.col("text").lower(), " abc "),
        (fpstreams.col("text").upper(), " ABC "),
        (fpstreams.col("text").strip(), "AbC"),
        (fpstreams.col("text").contains("b"), True),
        (fpstreams.when(fpstreams.col("value") >= 3, "yes", "no"), "yes"),
        ((fpstreams.col("value") > 0) & (fpstreams.col("missing").is_null()), True),
        ((fpstreams.col("value") < 0) | (fpstreams.col("value") == 3), True),
    ]

    assert [expression(row) for expression, _expected in expressions] == [
        expected for _expression, expected in expressions
    ]
    assert fpstreams.col("nested.amount").inspect().to_dict()["fields"] == ["nested"]
    assert fpstreams.lit(1).inspect().to_dict()["deterministic"] == "yes"
    assert fpstreams.col("missing").coalesce(0).inspect().to_dict()["null_behavior"] == (
        "coalesces"
    )
    assert fpstreams.col("value").map(str).inspect().to_dict() == {
        "fields": None,
        "deterministic": "unknown",
        "pure": "unknown",
        "null_behavior": "python",
        "backends": ["python"],
        "opaque": True,
    }
    with pytest.raises(TypeError, match="'&' or '\\|'"):
        bool(fpstreams.col("value"))


def test_arrow_row_expression_prefix_preserves_python_boundaries() -> None:
    table = pa.table({"value": [1, 2, 3], "enabled": [True, False, True]})
    source = fpstreams.rows.from_arrow(table, batch_size=2)._flow

    assert source.filter(fpstreams.col("value") > 1).to_list() == [
        {"value": 2, "enabled": False},
        {"value": 3, "enabled": True},
    ]
    assert source.map(fpstreams.col("value") + 10).to_list() == [11, 12, 13]
    assert source.filter(fpstreams.col("value") > 1).map(fpstreams.col("value") * 10).to_list() == [
        20,
        30,
    ]
    assert source.filter((fpstreams.col("value") % 2) == 0).to_list() == [
        {"value": 2, "enabled": False}
    ]
    assert source.filter(fpstreams.col("enabled") | (fpstreams.col("value") > 1)).count() == 3

    nullable = fpstreams.rows.from_arrow(pa.table({"value": [None, 1]}))._flow
    assert nullable.filter(fpstreams.col("value").is_null()).to_list() == [{"value": None}]
    assert nullable.map(fpstreams.col("value").fill_null(5).cast(float)).to_list() == [5.0, 1.0]

    explanation = source.filter(fpstreams.col("value") > 0).take(1).explain().to_dict()
    assert explanation["arrow_prefix"] == {
        "operation_count": 1,
        "boundary_reason": "unsupported_operation",
        "guarded": True,
    }
    assert explanation["boundaries"] == [
        {
            "from": "arrow",
            "to": "python",
            "after_operation": 1,
            "materializes_rows": True,
            "guarded": True,
        }
    ]


def test_arrow_batch_guard_names_each_fallback_reason() -> None:
    from fpstreams.execution.arrow import prove_batch_safe
    from fpstreams.planning.sync import FilterOp

    batch = pa.record_batch({"value": [1, 2]})
    nullable = pa.record_batch({"value": [None, 1]})
    operations = [
        (batch, FilterOp(lambda _row: True), "opaque_expression"),
        (batch, FilterOp(fpstreams.col("missing") > 0), "missing_field"),
        (batch, FilterOp(fpstreams.col("value").map(bool)), "incompatible_type"),
        (batch, FilterOp((fpstreams.col("value") // 2) == 1), "incompatible_type"),
        (batch, FilterOp((fpstreams.col("value") / 0) > 1), "zero_divisor"),
        (batch, FilterOp(fpstreams.col("value").lower() == "1"), "incompatible_type"),
        (batch, FilterOp(fpstreams.col("value").cast(str) == "1"), "unsafe_cast"),
        (nullable, FilterOp(fpstreams.col("value") == 1), "null_semantics"),
    ]

    assert [
        prove_batch_safe(item, (operation,)).reason.value for item, operation, _reason in operations
    ] == [reason for _item, _operation, reason in operations]
    assert prove_batch_safe(object(), ()).reason.value == "kernel_error"


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
