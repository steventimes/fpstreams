"""Collectors, aggregators, statistics, and duplicate policies."""

from __future__ import annotations

import json
import random
from collections.abc import AsyncIterator, Iterator

import pytest

import fpstreams
from fpstreams import flow


def _square(value: int) -> int:
    return value * value


def test_flow_conveniences_and_collectors_stay_thin_and_pythonic() -> None:
    values = flow([3, 1, 2, 4])

    assert values.sorted().batch(2).to_list() == [(1, 2), (3, 4)]
    assert flow(["a", "b"]).join("|") == "a|b"
    assert flow([1, 2, 3, 4]).collect(
        fpstreams.Collectors.grouping_by(lambda value: value % 2)
    ) == {1: [1, 3], 0: [2, 4]}

    successes, failures = flow(
        [fpstreams.Ok(1), fpstreams.Err(ValueError("bad")), fpstreams.Ok(2)]
    ).partition_results()
    assert successes == [1, 2]
    assert len(failures) == 1
    assert isinstance(failures[0], ValueError)


def test_to_dict_collector_never_overwrites_without_an_explicit_policy() -> None:
    records = [("a", 1), ("a", 2)]

    with pytest.raises(fpstreams.DuplicateKeyError):
        flow(records).collect(
            fpstreams.Collectors.to_dict(lambda item: item[0], lambda item: item[1])
        )

    assert flow(records).collect(
        fpstreams.Collectors.to_dict(
            lambda item: item[0],
            lambda item: item[1],
            on_duplicate="first",
        )
    ) == {"a": 1}
    assert flow(records).collect(
        fpstreams.Collectors.to_dict(
            lambda item: item[0],
            lambda item: item[1],
            on_duplicate="last",
        )
    ) == {"a": 2}


def test_flow_streams_generic_exports_and_online_statistics(tmp_path) -> None:
    csv_path = tmp_path / "people.csv"
    json_path = tmp_path / "values.json"

    flow([(1, "Ada"), (2, "Lin")]).to_csv(csv_path, header=("id", "name"))
    flow([{"id": 1}, {"id": 2}]).to_json(json_path)

    assert csv_path.read_text(encoding="utf-8") == "id,name\n1,Ada\n2,Lin\n"
    assert json.loads(json_path.read_text(encoding="utf-8")) == [{"id": 1}, {"id": 2}]
    assert flow(iter([1, 2, 3, 4])).describe() == {
        "count": 4,
        "sum": 10.0,
        "min": 1.0,
        "max": 4.0,
        "mean": 2.5,
        "std": pytest.approx(1.2909944487358056),
    }


def test_collectors_remain_directly_callable() -> None:
    joining = fpstreams.Collectors.joining(",")

    assert joining([1, 2, 3]) == "1,2,3"


def test_named_collectors_share_one_pass() -> None:
    pulls: list[int] = []

    def values() -> Iterator[int]:
        for value in (1, 2, 3):
            pulls.append(value)
            yield value

    result = fpstreams.flow.defer(values).collect(
        count=fpstreams.Collectors.counting(),
        total=fpstreams.Collectors.summing(),
        last=fpstreams.Collectors.last(),
    )

    assert result == {"count": 3, "total": 6, "last": 3}
    assert pulls == [1, 2, 3]


def test_named_collectors_stop_and_close_when_every_result_is_complete() -> None:
    pulls: list[int] = []
    closed = False

    def values() -> Iterator[int]:
        nonlocal closed
        try:
            value = 0
            while True:
                pulls.append(value)
                yield value
                value += 1
        finally:
            closed = True

    result = fpstreams.flow.defer(values).collect(
        first=fpstreams.Collectors.first(),
        head=fpstreams.Collectors.head(2),
    )

    assert result == {"first": 0, "head": [0, 1]}
    assert pulls == [0, 1]
    assert closed


def test_grouping_by_steps_its_downstream_while_the_source_is_consumed() -> None:
    events: list[tuple[str, int]] = []

    def classifier(value: int) -> int:
        events.append(("source", value))
        return value % 2

    def add(total: int, value: int) -> int:
        events.append(("step", value))
        return total + value

    downstream = fpstreams.Collector(lambda: 0, add)
    grouped = fpstreams.Collectors.grouping_by(classifier, downstream)([1, 2, 3])

    assert grouped == {1: 4, 0: 2}
    assert events == [
        ("source", 1),
        ("step", 1),
        ("source", 2),
        ("step", 2),
        ("source", 3),
        ("step", 3),
    ]


def test_aggregators_are_collectors_and_default_to_the_whole_value() -> None:
    total = fpstreams.agg.sum()

    assert isinstance(total, fpstreams.Collector)
    assert total([1, 2, 3]) == 6
    assert fpstreams.flow([1, 2, 3]).aggregate(
        count=fpstreams.agg.count(),
        total=total,
        first=fpstreams.agg.first(),
    ) == {"count": 3, "total": 6, "first": 1}


def test_short_circuiting_aggregate_does_not_pull_an_unneeded_item() -> None:
    closed = False

    def values() -> Iterator[int]:
        nonlocal closed
        try:
            yield 10
            raise AssertionError("aggregate over-pulled its source")
        finally:
            closed = True

    assert fpstreams.flow.defer(values).aggregate(first=fpstreams.agg.first()) == {"first": 10}
    assert closed


def test_nested_grouping_reduces_each_group_without_materializing_members() -> None:
    events = [
        {"category": "food", "amount": 4},
        {"category": "books", "amount": 10},
        {"category": "food", "amount": 3},
    ]

    totals = fpstreams.flow(events).collect(
        fpstreams.Collectors.grouping_by(
            "category",
            fpstreams.Collectors.summing("amount"),
        )
    )

    assert totals == {"food": 7, "books": 10}


def test_collector_adapters_compose_without_intermediate_collections() -> None:
    collector = fpstreams.Collectors.collecting_and_then(
        fpstreams.Collectors.flat_mapping(
            lambda value: range(value),
            fpstreams.Collectors.filtering(
                lambda value: value % 2 == 0,
                fpstreams.Collectors.summing(),
            ),
        ),
        lambda total: f"total={total}",
    )

    assert collector([4, 3]) == "total=4"


def test_teeing_stops_when_both_downstreams_are_complete() -> None:
    pulls: list[int] = []

    def values() -> Iterator[int]:
        value = 1
        while True:
            pulls.append(value)
            yield value
            value += 1

    collector = fpstreams.Collectors.teeing(
        fpstreams.Collectors.first(),
        fpstreams.Collectors.head(3),
        lambda first, head: (first, head),
    )

    assert collector(values()) == (1, [1, 2, 3])
    assert pulls == [1, 2, 3]


def test_tail_and_only_have_bounded_state_and_explicit_cardinality() -> None:
    assert fpstreams.Collectors.tail(2)(range(5)) == [3, 4]
    assert fpstreams.Collectors.only()([7]) == 7
    assert fpstreams.Collectors.only()([]) is None
    with pytest.raises(ValueError, match="exactly one"):
        fpstreams.Collectors.only()([1, 2, 3])


@pytest.mark.asyncio
async def test_async_collect_accepts_one_or_many_collectors() -> None:
    assert await fpstreams.aflow([1, 2, 3]).collect(fpstreams.Collectors.summing()) == 6
    assert await fpstreams.aflow([1, 2, 3]).collect(
        count=fpstreams.Collectors.counting(),
        tail=fpstreams.Collectors.tail(2),
    ) == {"count": 3, "tail": [2, 3]}


@pytest.mark.asyncio
async def test_async_collect_and_aggregate_short_circuit_and_close() -> None:
    async def source(events: list[str]) -> AsyncIterator[int]:
        try:
            yield 10
            events.append("over-pulled")
            raise AssertionError("async reduction over-pulled its source")
        finally:
            events.append("closed")

    collect_events: list[str] = []
    aggregate_events: list[str] = []

    assert await fpstreams.aflow(source(collect_events)).collect(
        first=fpstreams.Collectors.first()
    ) == {"first": 10}
    assert await fpstreams.aflow(source(aggregate_events)).aggregate(
        first=fpstreams.agg.first()
    ) == {"first": 10}
    assert collect_events == ["closed"]
    assert aggregate_events == ["closed"]


def test_native_whole_value_aggregations_share_one_fused_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fpstreams import _native

    def reject_materialization(*_args: object) -> None:
        raise AssertionError("aggregate materialized the native pipeline")

    monkeypatch.setattr(_native, "execute_i64_range", reject_materialization)
    result = (
        fpstreams.flow(range(1, 6))
        .map(fpstreams.item * 2)
        .with_engine("native")
        .aggregate(
            count=fpstreams.agg.count(),
            total=fpstreams.agg.sum(),
            minimum=fpstreams.agg.min(),
            maximum=fpstreams.agg.max(),
            first=fpstreams.agg.first(),
            last=fpstreams.agg.last(),
            mean=fpstreams.agg.mean(),
            variance=fpstreams.agg.variance(ddof=0),
            std=fpstreams.agg.std(ddof=1),
        )
    )

    assert result == {
        "count": 5,
        "total": 30,
        "minimum": 2,
        "maximum": 10,
        "first": 2,
        "last": 10,
        "mean": 6.0,
        "variance": 8.0,
        "std": pytest.approx(10**0.5),
    }


def test_native_empty_multi_aggregation_matches_python_semantics() -> None:
    result = (
        fpstreams.flow(range(5))
        .filter(fpstreams.item < 0)
        .with_engine("native")
        .aggregate(
            count=fpstreams.agg.count(),
            total=fpstreams.agg.sum(),
            first=fpstreams.agg.first(),
            last=fpstreams.agg.last(),
            mean=fpstreams.agg.mean(),
            variance=fpstreams.agg.variance(),
        )
    )

    assert result == {
        "count": 0,
        "total": 0,
        "first": None,
        "last": None,
        "mean": None,
        "variance": None,
    }


def test_native_first_only_aggregate_uses_the_short_circuit_terminal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fpstreams import _native

    def reject_full_snapshot(*_args: object) -> None:
        raise AssertionError("first-only aggregate scanned the complete pipeline")

    monkeypatch.setattr(_native, "aggregate_i64_range", reject_full_snapshot)

    assert (
        fpstreams.flow(range(1_000_000))
        .map(fpstreams.item + 1)
        .with_engine("native")
        .aggregate(first=fpstreams.agg.first())
    ) == {"first": 1}


@pytest.mark.parametrize("numeric_kind", ["i64", "f64"])
def test_native_multi_aggregation_matches_python_for_generated_values(
    numeric_kind: str,
) -> None:
    randomizer = random.Random(1489)

    for _case in range(25):
        integers = [randomizer.randrange(-10_000, 10_001) for _ in range(randomizer.randrange(40))]
        values = integers if numeric_kind == "i64" else [value / 7 for value in integers]
        pipeline = (
            fpstreams.flow(values).map(fpstreams.item + 0)
            if numeric_kind == "i64"
            else fpstreams.flow(values).map(fpstreams.fitem + 0.0)
        )
        aggregations = {
            "count": fpstreams.agg.count(),
            "total": fpstreams.agg.sum(),
            "minimum": fpstreams.agg.min(),
            "maximum": fpstreams.agg.max(),
            "mean": fpstreams.agg.mean(),
            "variance": fpstreams.agg.variance(ddof=0),
        }
        native = pipeline.with_engine("native").aggregate(**aggregations)
        python = pipeline.with_engine("python").aggregate(**aggregations)

        for name, expected in python.items():
            assert native[name] == (
                pytest.approx(expected) if isinstance(expected, float) else expected
            )
