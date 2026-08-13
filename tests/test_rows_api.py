"""Rows expressions, joins, reshaping, cleaning, and spilling."""

from __future__ import annotations

import gc
import weakref
from collections.abc import Iterator

import pytest

import fpstreams
from fpstreams import flow


def _square(value: int) -> int:
    return value * value


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
