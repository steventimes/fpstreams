from __future__ import annotations


# --- Tests consolidated from test_collecting_api.py ---

"""Flow collectors, one-pass aggregations, short-circuiting, and duplicate policies."""


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


def test_flow_csv_can_neutralize_spreadsheet_formula_cells(tmp_path) -> None:
    safe_path = tmp_path / "safe.csv"
    raw_path = tmp_path / "raw.csv"
    values = [("=1+1", "  @command", "plain", -4)]

    flow(values).to_csv(
        safe_path,
        header=("formula", "spaced", "text", "number"),
        spreadsheet_safe=True,
    )
    flow(values).to_csv(raw_path, header=("formula", "spaced", "text", "number"))

    assert safe_path.read_text(encoding="utf-8") == (
        "formula,spaced,text,number\n'=1+1,'  @command,plain,-4\n"
    )
    assert raw_path.read_text(encoding="utf-8") == (
        "formula,spaced,text,number\n=1+1,  @command,plain,-4\n"
    )


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


# --- Tests consolidated from test_pairs_api.py ---

"""Pairs transforms, per-key collection and aggregation, validation, and cleanup."""


from collections.abc import Iterator
from typing import Any

import pytest

import fpstreams
from fpstreams import flow


def _pairs_square(value: int) -> int:
    return value * value


def test_pairs_makes_key_value_transforms_explicit() -> None:
    result = (
        flow({"ada": 2, "lin": 1}.items())
        .pairs()
        .filter_values(lambda value: value > 1)
        .map_values(lambda value: value * 10)
        .to_dict()
    )

    assert result == {"ada": 20}
    with pytest.raises(fpstreams.DuplicateKeyError):
        flow([("x", 1), ("x", 2)]).pairs().to_dict()


def test_pairs_factory_exposes_keys_values_and_the_underlying_flow() -> None:
    entries = fpstreams.pairs({"ada": 2, "lin": 1})

    assert entries.keys().to_list() == ["ada", "lin"]
    assert entries.values().to_list() == [2, 1]
    assert entries.to_flow().to_list() == [("ada", 2), ("lin", 1)]
    assert (
        entries.flat_map_pairs(lambda key, value: [(key, value), (key, value * 10)])
        .drop(1)
        .take(2)
        .to_flow()
        .to_list()
    ) == [("ada", 20), ("lin", 1)]


def test_pair_transforms_stay_lazy_and_close_after_downstream_short_circuit() -> None:
    events: list[str] = []

    def source() -> Iterator[tuple[str, int]]:
        try:
            for pair in (("skip", 0), ("a", 2), ("b", 3)):
                events.append(f"pull:{pair[0]}")
                yield pair
        finally:
            events.append("closed")

    entries = (
        fpstreams.pairs(source())
        .filter_pairs(lambda _key, value: value > 0)
        .map_pairs(lambda key, value: (key.upper(), value * 10))
    )
    assert events == []
    assert entries.take(1).to_dict() == {"A": 20}
    assert events == ["pull:skip", "pull:a", "closed"]

    assert fpstreams.pairs(
        [("b", 2), ("a", 3), ("a", 1)]
    ).sort_by_key().unique_keys().to_flow().to_list() == [
        ("a", 3),
        ("b", 2),
    ]


def test_collect_values_steps_each_key_downstream_as_pairs_arrive() -> None:
    events: list[tuple[str, int]] = []

    def source() -> Iterator[tuple[str, int]]:
        for key, value in (("a", 1), ("b", 2), ("a", 3)):
            events.append(("source", value))
            yield key, value

    def add(total: int, value: int) -> int:
        events.append(("step", value))
        return total + value

    result = fpstreams.pairs(source()).collect_values(fpstreams.Collector(lambda: 0, add))

    assert result == {"a": 4, "b": 2}
    assert events == [
        ("source", 1),
        ("step", 1),
        ("source", 2),
        ("step", 2),
        ("source", 3),
        ("step", 3),
    ]


def test_collect_values_tracks_completion_per_key_and_accepts_legacy_callables() -> None:
    steps: list[int] = []
    first = fpstreams.Collector(
        lambda: [],
        lambda state, value: _append_step(state, value, steps),
        lambda state: state[0],
        done=bool,
    )

    assert fpstreams.pairs([("a", 1), ("a", 2), ("b", 3), ("b", 4)]).collect_values(first) == {
        "a": 1,
        "b": 3,
    }
    assert steps == [1, 3]
    assert fpstreams.pairs([("a", 1), ("a", 2)]).collect_values(sum) == {"a": 3}


def _append_step(state: list[int], value: int, steps: list[int]) -> list[int]:
    steps.append(value)
    state.append(value)
    return state


def test_collect_values_closes_the_source_when_a_downstream_step_fails() -> None:
    closed = False

    def source() -> Iterator[tuple[str, int]]:
        nonlocal closed
        try:
            yield "a", 1
            yield "a", 2
        finally:
            closed = True

    def fail(_state: Any, _value: int) -> Any:
        raise RuntimeError("stop")

    with pytest.raises(RuntimeError, match="stop"):
        fpstreams.pairs(source()).collect_values(fpstreams.Collector(lambda: None, fail))
    assert closed


def test_aggregate_values_computes_named_reductions_per_key_in_one_pass() -> None:
    closed = False

    def source() -> Iterator[tuple[str, dict[str, int]]]:
        nonlocal closed
        try:
            yield "a", {"amount": 2}
            yield "b", {"amount": 5}
            yield "a", {"amount": 3}
        finally:
            closed = True

    result = fpstreams.pairs(source()).aggregate_values(
        count=fpstreams.agg.count(),
        total=fpstreams.agg.sum("amount"),
        first=fpstreams.agg.first("amount"),
        last=fpstreams.agg.last("amount"),
    )

    assert result == {
        "a": {"count": 2, "total": 5, "first": 2, "last": 3},
        "b": {"count": 1, "total": 5, "first": 5, "last": 5},
    }
    assert closed


def test_aggregate_values_validates_before_opening_the_source() -> None:
    opened = False

    def source() -> Iterator[tuple[str, int]]:
        nonlocal opened
        opened = True
        yield "a", 1

    with pytest.raises(TypeError, match="must be an Aggregator"):
        fpstreams.pairs(source()).aggregate_values(invalid=object())  # type: ignore[arg-type]
    assert not opened
