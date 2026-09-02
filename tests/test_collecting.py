"""Flow collectors, one-pass aggregations, short-circuiting, and duplicate policies."""

from __future__ import annotations

import json
import random
import sys
from collections.abc import AsyncIterator, Callable, Iterable, Iterator
from typing import Any

import pytest

import fpstreams
from fpstreams import flow

_NATIVE_PAIR_TEST_SIZE = 140_000


def _sequential_native_pairs() -> list[tuple[int, int]]:
    """Build enough exact integer pairs to cross retained native thresholds."""
    return [(index, index) for index in range(_NATIVE_PAIR_TEST_SIZE)]


# --- Tests consolidated from test_collecting_api.py ---


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


@pytest.mark.parametrize("adapter", ["collector", "flat_mapping"])
def test_public_collectors_keep_step_failure_primary_when_input_close_fails(
    adapter: str,
) -> None:
    primary = ValueError("collector step failed")
    opened: list[ClosingValues] = []

    class ClosingValues(Iterator[int]):
        def __init__(self) -> None:
            self.emitted = False
            self.close_calls = 0

        def __next__(self) -> int:
            if self.emitted:
                raise StopIteration
            self.emitted = True
            return 1

        def close(self) -> None:
            self.close_calls += 1
            raise OSError("collector input close failed")

    def values() -> ClosingValues:
        iterator = ClosingValues()
        opened.append(iterator)
        return iterator

    def fail(_state: None, _value: int) -> None:
        raise primary

    downstream = fpstreams.Collector(lambda: None, fail)
    collector = (
        downstream
        if adapter == "collector"
        else fpstreams.Collectors.flat_mapping(lambda _value: values(), downstream)
    )
    source: Iterable[int] = values() if adapter == "collector" else [0]

    with pytest.raises(ValueError) as captured:
        collector(source)

    assert captured.value is primary
    assert captured.value.__notes__ == ["cleanup failed with OSError: collector input close failed"]
    assert opened[0].close_calls == 1


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


def test_pairs_to_dict_last_uses_one_assignment_lookup_per_item() -> None:
    """Last-wins collection must not probe membership before assigning a key."""
    events: list[tuple[str, ...]] = []

    class Key:
        def __init__(self, name: str) -> None:
            self.name = name

        def __hash__(self) -> int:
            events.append(("hash", self.name))
            return 1

        def __eq__(self, other: object) -> bool:
            assert isinstance(other, Key)
            events.append(("eq", self.name, other.name))
            return True

    first = Key("first")
    duplicate = Key("duplicate")

    result = fpstreams.pairs([(first, 1), (duplicate, 2)]).to_dict(on_duplicate="last")

    assert next(iter(result)) is first
    assert list(result.values()) == [2]
    assert events == [
        ("hash", "first"),
        ("hash", "duplicate"),
        ("eq", "first", "duplicate"),
    ]


def test_pair_side_map_to_dict_consumes_the_structured_tail_directly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pair key/value maps can feed every dictionary policy without an intermediate tuple."""
    from fpstreams.execution import sync

    def unexpected_pair_iterator(*_arguments: object) -> Iterator[object]:
        raise AssertionError("the final structured pair map must be consumed directly")

    monkeypatch.setattr(sync, "_map_pair_side", unexpected_pair_iterator)

    values = (
        fpstreams.pairs([("a", 1), ("skip", 2), ("b", 3)])
        .filter_keys(lambda key: key != "skip")
        .map_values(lambda value: value * 10)
    )
    assert values.to_dict(on_duplicate="last") == {"a": 10, "b": 30}

    duplicate_keys = fpstreams.pairs((("a", 1), ("b", 2))).map_keys(lambda _key: "same")
    assert duplicate_keys.to_dict(on_duplicate="first") == {"same": 1}
    assert duplicate_keys.to_dict(on_duplicate="last") == {"same": 2}
    with pytest.raises(fpstreams.DuplicateKeyError, match=r"^Duplicate key: 'same'$"):
        duplicate_keys.to_dict()

    calls: list[int] = []

    def stop_on_two(value: int) -> int:
        calls.append(value)
        if value == 2:
            raise StopIteration
        return value * 10

    assert fpstreams.pairs(iter((("a", 1), ("b", 2), ("c", 3)))).map_values(stop_on_two).to_dict(
        on_duplicate="last"
    ) == {"a": 10}
    assert calls == [1, 2]


@pytest.mark.parametrize("policy", ["first", "last"])
def test_pair_map_to_dict_consumes_first_and_last_directly(
    monkeypatch: pytest.MonkeyPatch,
    policy: str,
) -> None:
    """A final map_pairs callback feeds common dictionary policies once per source item."""
    from fpstreams.execution import sync

    def unexpected_pair_iterator(*_arguments: object) -> Iterator[object]:
        raise AssertionError("the final structured pair map must be consumed directly")

    monkeypatch.setattr(sync, "_map_pair_side", unexpected_pair_iterator)
    events: list[object] = []

    def source() -> Iterator[tuple[str, int]]:
        events.append("opened")
        try:
            yield "b", 2
            yield "a", 4
            yield "b", 6
        finally:
            events.append("closed")

    def transform(key: str, value: int) -> tuple[str, int]:
        events.append((key, value))
        return key, value * 10

    mapped = fpstreams.flow.defer(source).pairs().map_pairs(transform)

    result = mapped.to_dict(on_duplicate=policy)  # type: ignore[arg-type]

    expected_b = 20 if policy == "first" else 60
    assert list(result.items()) == [("b", expected_b), ("a", 40)]
    assert events == ["opened", ("b", 2), ("a", 4), ("b", 6), "closed"]


@pytest.mark.parametrize("failure", ["stop", "invalid", "error"])
def test_pair_map_to_dict_preserves_callback_and_pair_failures(
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    """Direct map_pairs collection validates output pairs and always closes its source."""
    from fpstreams.execution import sync

    def unexpected_pair_iterator(*_arguments: object) -> Iterator[object]:
        raise AssertionError("the final structured pair map must be consumed directly")

    monkeypatch.setattr(sync, "_map_pair_side", unexpected_pair_iterator)
    events: list[object] = []
    primary = RuntimeError("mapping failed")

    def source() -> Iterator[tuple[str, int]]:
        try:
            yield "a", 1
            yield "b", 2
            yield "c", 3
        finally:
            events.append("closed")

    def transform(key: str, value: int) -> tuple[object, ...]:
        events.append((key, value))
        if value != 2:
            return key, value * 10
        if failure == "stop":
            raise StopIteration
        if failure == "error":
            raise primary
        return key, value, "extra"

    mapped = fpstreams.flow.defer(source).pairs().map_pairs(transform)

    if failure == "stop":
        assert mapped.to_dict(on_duplicate="last") == {"a": 10}
    elif failure == "error":
        with pytest.raises(RuntimeError) as raised:
            mapped.to_dict(on_duplicate="last")
        assert raised.value is primary
    else:
        with pytest.raises(ValueError, match="too many values to unpack"):
            mapped.to_dict(on_duplicate="last")
    assert events == [("a", 1), ("b", 2), "closed"]


def test_pair_map_to_dict_error_keeps_the_canonical_iterator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Duplicate-error map_pairs collection retains the established iterator path."""
    from fpstreams.execution import sync

    original = sync._map_pair_side
    observed: list[str] = []

    def observe_pair_iterator(*arguments: object) -> Iterator[tuple[object, object]]:
        descriptor = arguments[1]
        observed.append(descriptor.side)  # type: ignore[attr-defined]
        return original(*arguments)  # type: ignore[arg-type]

    monkeypatch.setattr(sync, "_map_pair_side", observe_pair_iterator)
    mapped = fpstreams.pairs([("a", 1), ("a", 2)]).map_pairs(lambda key, value: (key, value))

    with pytest.raises(fpstreams.DuplicateKeyError, match=r"^Duplicate key: 'a'$"):
        mapped.to_dict()
    assert observed == ["pair"]


@pytest.mark.parametrize("tail", ["map", "filter"])
def test_pair_tail_fusion_checks_exact_policy_before_membership(tail: str) -> None:
    """A str-subclass policy reaches the canonical consumer without a fast-path probe."""
    events: list[object] = []

    class Policy(str):
        def __hash__(self) -> int:
            events.append("hash")
            return super().__hash__()

        def __eq__(self, other: object) -> bool:
            events.append(("eq", other))
            return super().__eq__(other)

    def values() -> Iterator[tuple[str, int]]:
        try:
            yield "a", 1
        finally:
            events.append("closed")

    class Source:
        def __iter__(self) -> Iterator[tuple[str, int]]:
            events.clear()
            events.append("opened")
            return values()

    pairs = fpstreams.pairs(Source())
    pipeline = (
        pairs.map_pairs(lambda key, value: (key, value * 10))
        if tail == "map"
        else pairs.filter_values(lambda value: value > 0)
    )

    result = pipeline.to_dict(on_duplicate=Policy("last"))  # type: ignore[arg-type]

    assert result == ({"a": 10} if tail == "map" else {"a": 1})
    assert events == ["opened", ("eq", "last"), "closed"]


@pytest.mark.parametrize("tail", ["map_value", "map_pair", "filter_value", "filter_pair"])
def test_pair_tail_fusion_rechecks_consumer_after_source_open(
    monkeypatch: pytest.MonkeyPatch,
    tail: str,
) -> None:
    """A source-open mutation of PairDictConsumer.__call__ disables every tail fusion."""
    from fpstreams.execution._pair_dict import PairDictConsumer

    events: list[str] = []
    marker: dict[object, object] = {"sentinel": object()}

    def replacement_consumer(
        _self: PairDictConsumer,
        _iterator: Iterator[object],
    ) -> dict[object, object]:
        events.append("consumer")
        return marker

    class Source:
        def __iter__(self) -> Iterator[tuple[str, int]]:
            events.append("opened")
            monkeypatch.setattr(PairDictConsumer, "__call__", replacement_consumer)
            return iter((("a", 1),))

    pairs = fpstreams.pairs(Source())
    if tail == "map_value":
        pipeline = pairs.map_values(lambda value: value * 10)
    elif tail == "map_pair":
        pipeline = pairs.map_pairs(lambda key, value: (key, value * 10))
    elif tail == "filter_value":
        pipeline = pairs.filter_values(lambda value: value > 0)
    else:
        pipeline = pairs.filter_pairs(lambda _key, value: value > 0)

    result = pipeline.to_dict(on_duplicate="last")

    assert result is marker
    assert events == ["opened", "consumer"]


def test_pair_filter_to_dict_consumes_the_structured_tail_directly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A final two-argument pair filter feeds each dictionary policy directly."""
    from fpstreams.execution import sync

    def unexpected_pair_iterator(*_arguments: object) -> Iterator[object]:
        raise AssertionError("the final structured pair filter must be consumed directly")

    monkeypatch.setattr(sync, "_filter_pair_target", unexpected_pair_iterator)
    calls: list[int] = []

    def accepted(_key: str, value: int) -> bool:
        calls.append(value)
        return value != 2

    entries = fpstreams.pairs((("a", 1), ("a", 2), ("a", 3), ("b", 4))).filter_pairs(accepted)

    assert entries.to_dict(on_duplicate="first") == {"a": 1, "b": 4}
    assert calls == [1, 2, 3, 4]
    calls.clear()
    assert entries.to_dict(on_duplicate="last") == {"a": 3, "b": 4}
    assert calls == [1, 2, 3, 4]
    calls.clear()
    with pytest.raises(fpstreams.DuplicateKeyError, match=r"^Duplicate key: 'a'$"):
        entries.to_dict()
    assert calls == [1, 2, 3]


@pytest.mark.parametrize("target", ["key", "value"])
@pytest.mark.parametrize("policy", ["first", "last"])
def test_pair_side_filter_to_dict_consumes_first_and_last_directly(
    monkeypatch: pytest.MonkeyPatch,
    target: str,
    policy: str,
) -> None:
    """Final key/value filters feed common dictionary policies without a generator layer."""
    from fpstreams.execution import sync

    def unexpected_pair_iterator(*_arguments: object) -> Iterator[object]:
        raise AssertionError("the final structured pair filter must be consumed directly")

    monkeypatch.setattr(sync, "_filter_pair_target", unexpected_pair_iterator)
    events: list[object] = []

    def source() -> Iterator[tuple[str, int]]:
        events.append("opened")
        try:
            yield "b", 2
            yield "a", 4
            yield "b", 6
            yield "c", 3
        finally:
            events.append("closed")

    def accepted_key(key: str) -> bool:
        events.append(("key", key))
        return key != "c"

    def accepted_value(value: int) -> bool:
        events.append(("value", value))
        return value % 2 == 0

    entries = fpstreams.flow.defer(source).pairs()
    filtered = (
        entries.filter_keys(accepted_key)
        if target == "key"
        else entries.filter_values(accepted_value)
    )

    result = filtered.to_dict(on_duplicate=policy)  # type: ignore[arg-type]

    expected_b = 2 if policy == "first" else 6
    assert list(result.items()) == [("b", expected_b), ("a", 4)]
    expected_arguments = ["b", "a", "b", "c"] if target == "key" else [2, 4, 6, 3]
    assert events == [
        "opened",
        *((target, argument) for argument in expected_arguments),
        "closed",
    ]


@pytest.mark.parametrize(("target", "boundary"), [("key", "b"), ("value", 2)])
def test_pair_side_filter_to_dict_preserves_callback_exhaustion(
    monkeypatch: pytest.MonkeyPatch,
    target: str,
    boundary: object,
) -> None:
    """Direct key/value filter collection returns its prefix and closes on StopIteration."""
    from fpstreams.execution import sync

    def unexpected_pair_iterator(*_arguments: object) -> Iterator[object]:
        raise AssertionError("the final structured pair filter must be consumed directly")

    monkeypatch.setattr(sync, "_filter_pair_target", unexpected_pair_iterator)
    events: list[object] = []

    def source() -> Iterator[tuple[str, int]]:
        try:
            yield "a", 1
            yield "b", 2
            yield "c", 3
        finally:
            events.append("closed")

    def accepted(value: object) -> bool:
        events.append(value)
        if value == boundary:
            raise StopIteration
        return True

    entries = fpstreams.flow.defer(source).pairs()
    filtered = entries.filter_keys(accepted) if target == "key" else entries.filter_values(accepted)

    assert filtered.to_dict(on_duplicate="last") == {"a": 1}
    assert events == (["a", "b", "closed"] if target == "key" else [1, 2, "closed"])


@pytest.mark.parametrize("target", ["key", "value"])
def test_pair_side_filter_to_dict_error_keeps_the_canonical_iterator(
    monkeypatch: pytest.MonkeyPatch,
    target: str,
) -> None:
    """Duplicate-error collection retains the established structured iterator path."""
    from fpstreams.execution import sync

    original = sync._filter_pair_target
    observed: list[str] = []

    def observe_pair_iterator(*arguments: object) -> Iterator[object]:
        descriptor = arguments[1]
        observed.append(descriptor.target)  # type: ignore[attr-defined]
        return original(*arguments)  # type: ignore[arg-type]

    monkeypatch.setattr(sync, "_filter_pair_target", observe_pair_iterator)
    entries = fpstreams.pairs([("a", 1), ("a", 2)])
    filtered = (
        entries.filter_keys(lambda _key: True)
        if target == "key"
        else entries.filter_values(lambda _value: True)
    )

    with pytest.raises(fpstreams.DuplicateKeyError, match=r"^Duplicate key: 'a'$"):
        filtered.to_dict()
    assert observed == [target]


def test_pair_filter_to_dict_truth_exhaustion_closes_the_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Truth-test exhaustion returns the accepted prefix and closes a one-shot source."""
    from fpstreams.execution import sync

    def unexpected_pair_iterator(*_arguments: object) -> Iterator[object]:
        raise AssertionError("the structured filter iterator must stay fused")

    monkeypatch.setattr(sync, "_filter_pair_target", unexpected_pair_iterator)
    closed = False

    def source() -> Iterator[tuple[str, int]]:
        nonlocal closed
        try:
            yield "a", 1
            yield "b", 2
            yield "c", 3
        finally:
            closed = True

    class ExhaustedTruth:
        def __bool__(self) -> bool:
            raise StopIteration

    calls: list[int] = []

    def accepted(_key: str, value: int) -> object:
        calls.append(value)
        return True if value == 1 else ExhaustedTruth()

    result = fpstreams.pairs(source()).filter_pairs(accepted).to_dict(on_duplicate="last")

    assert result == {"a": 1}
    assert calls == [1, 2]
    assert closed


@pytest.mark.parametrize("side", ["value", "pair"])
def test_pair_side_map_to_dict_retains_active_failpoint_boundaries(side: str) -> None:
    """Instrumentation keeps the canonical callback transition instead of using fusion."""
    from fpstreams.runtime.failpoints import failpoint

    error = RuntimeError("stop before callback")
    pairs = fpstreams.pairs([("a", 1)])
    values = (
        pairs.map_values(lambda value: value * 10)
        if side == "value"
        else pairs.map_pairs(lambda key, value: (key, value * 10))
    )

    with failpoint("callback.before", error), pytest.raises(RuntimeError) as raised:
        values.to_dict(on_duplicate="last")

    assert raised.value is error


@pytest.mark.parametrize("tail", ["map", "map_pair", "filter"])
def test_pair_structured_to_dict_rechecks_failpoints_activated_while_opening_source(
    tail: str,
) -> None:
    """A source-open side effect can require instrumentation before the first callback."""
    from contextlib import AbstractContextManager

    from fpstreams.runtime.failpoints import failpoint

    error = RuntimeError("stop before callback")
    callbacks: list[int] = []
    closed = False

    class OpenedIterator(Iterator[tuple[str, int]]):
        def __init__(self, scope: AbstractContextManager[None]) -> None:
            self._values = iter((("a", 1),))
            self._scope = scope

        def __next__(self) -> tuple[str, int]:
            return next(self._values)

        def close(self) -> None:
            nonlocal closed
            closed = True
            self._scope.__exit__(None, None, None)

    class ActivatingSource:
        def __iter__(self) -> Iterator[tuple[str, int]]:
            scope = failpoint("callback.before", error)
            scope.__enter__()
            return OpenedIterator(scope)

    def transform(value: int) -> int:
        callbacks.append(value)
        return value * 10

    pairs = fpstreams.pairs(ActivatingSource())
    if tail == "map":
        values = pairs.map_values(transform)
    elif tail == "map_pair":
        values = pairs.map_pairs(lambda key, value: (key, transform(value)))
    else:
        values = pairs.filter_pairs(lambda _key, value: bool(transform(value)))
    with pytest.raises(RuntimeError) as raised:
        values.to_dict(on_duplicate="last")

    assert raised.value is error
    assert callbacks == []
    assert closed


def test_pair_materializers_preserve_custom_iteration() -> None:
    """Only canonical Pairs may bypass the public iterator forwarding layer."""
    from fpstreams.streams.flow import Flow
    from fpstreams.streams.pairs import Pairs

    class CustomPairs(Pairs[int, int]):
        def __iter__(self) -> Iterator[tuple[int, int]]:
            return iter(((9, 9),))

    values = CustomPairs(fpstreams.flow([(1, 2)]))

    assert values.to_dict() == {9: 9}
    assert values.group_values() == {9: [9]}
    assert values.collect_values(sum) == {9: 9}
    assert values.aggregate_values(total=fpstreams.agg.sum()) == {9: {"total": 9}}

    class CustomFlow(Flow[tuple[int, int]]):
        def __iter__(self) -> Iterator[tuple[int, int]]:
            return iter(((9, 9),))

    flow_values = Pairs(CustomFlow([(1, 2)]))
    assert flow_values.to_dict() == {9: 9}
    assert flow_values.group_values() == {9: [9]}
    assert flow_values.collect_values(sum) == {9: 9}
    assert flow_values.aggregate_values(total=fpstreams.agg.sum()) == {9: {"total": 9}}


def test_pair_value_expr_filter_to_dict_uses_the_retained_native_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A large retained Expr value filter reaches the native prefix endpoint."""
    from fpstreams import _native

    calls: list[int] = []

    def observed(*_arguments: object) -> None:
        calls.append(1)
        return None

    monkeypatch.setattr(
        _native,
        "pair_i64_value_filter_to_dict_exact_prefix_v1",
        observed,
        raising=False,
    )
    values = _sequential_native_pairs()

    result = (
        fpstreams.pairs(values).filter_values(fpstreams.item % 2 == 0).to_dict(on_duplicate="first")
    )

    assert result == {index: index for index in range(0, 140_000, 2)}
    assert calls == [1]


@pytest.mark.parametrize("policy", ["first", "last"])
def test_pair_row_expression_filter_to_dict_uses_the_native_prefix(
    monkeypatch: pytest.MonkeyPatch,
    policy: str,
) -> None:
    """A retained i64 pair expression should fuse filtering with dictionary collection."""
    from fpstreams import _native

    endpoint = _native.pair_i64_row_filter_to_dict_exact_prefix_v1
    calls = 0

    def observed(*arguments: object) -> object:
        nonlocal calls
        calls += 1
        return endpoint(*arguments)

    monkeypatch.setattr(_native, "pair_i64_row_filter_to_dict_exact_prefix_v1", observed)
    values = [(index % 10_007, index) for index in range(140_000)]
    expression = (fpstreams.col(0) + fpstreams.col(1)) % 3 == 0
    expected: dict[int, int] = {}
    for key, value in values:
        if (key + value) % 3 != 0:
            continue
        if policy == "first":
            expected.setdefault(key, value)
        else:
            expected[key] = value

    result = (
        fpstreams.pairs(values)
        .filter_pairs(expression)
        .to_dict(
            on_duplicate=policy  # type: ignore[arg-type]
        )
    )

    assert result == expected
    assert calls == 1


def test_pair_row_expression_filter_reuses_a_trusted_python_program(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A canonical RowProgram remains eligible after the same expression runs in Python."""
    from fpstreams import _native

    endpoint = _native.pair_i64_row_filter_to_dict_exact_prefix_v1
    calls = 0

    def observed(*arguments: object) -> object:
        nonlocal calls
        calls += 1
        return endpoint(*arguments)

    monkeypatch.setattr(_native, "pair_i64_row_filter_to_dict_exact_prefix_v1", observed)
    values = [(index % 37, index) for index in range(256)]
    expression = (fpstreams.col(0) + fpstreams.col(1)) % 3 == 0
    filtered = fpstreams.pairs(values).filter_pairs(expression)

    expected = filtered.with_engine("python").to_dict(on_duplicate="last")
    result = filtered.to_dict(on_duplicate="last")

    assert result == expected
    assert calls == 1


@pytest.mark.parametrize(
    "expression_factory",
    [
        lambda: fpstreams.col(1) > 10_000,
        lambda: fpstreams.col(1) == -191,
        lambda: fpstreams.col(1) != 0,
        lambda: (fpstreams.col(1) // -7) % 5 == -1,
        lambda: -fpstreams.col(0) >= 120,
    ],
    ids=("no_hits", "one_hit", "dense", "negative_floor_mod", "unary_single_column"),
)
def test_pair_row_expression_filter_matches_python_across_general_shapes(
    monkeypatch: pytest.MonkeyPatch,
    expression_factory: Callable[[], Any],
) -> None:
    """Native pair filtering is not tied to the competitive benchmark expression."""
    from fpstreams import _native

    endpoint = _native.pair_i64_row_filter_to_dict_exact_prefix_v1
    calls = 0

    def observed(*arguments: object) -> object:
        nonlocal calls
        calls += 1
        return endpoint(*arguments)

    monkeypatch.setattr(_native, "pair_i64_row_filter_to_dict_exact_prefix_v1", observed)
    values = [(index - 128, index - 192) for index in range(257)]
    expression = expression_factory()
    filtered = fpstreams.pairs(values).filter_pairs(expression)

    expected = filtered.with_engine("python").to_dict(on_duplicate="last")
    actual = filtered.to_dict(on_duplicate="last")

    assert actual == expected
    assert calls == 1


@pytest.mark.parametrize("source_kind", ["list", "tuple"])
def test_pair_row_expression_filter_native_threshold_is_128_rows(
    monkeypatch: pytest.MonkeyPatch,
    source_kind: str,
) -> None:
    """The measured crossover selects 128 retained rows without timing assertions."""
    from fpstreams import _native

    endpoint = _native.pair_i64_row_filter_to_dict_exact_prefix_v1
    native_sizes: list[int] = []
    current_size = 0

    def observed(*arguments: object) -> object:
        native_sizes.append(current_size)
        return endpoint(*arguments)

    monkeypatch.setattr(_native, "pair_i64_row_filter_to_dict_exact_prefix_v1", observed)
    for size in (127, 128):
        current_size = size
        rows = [(index, index + 1) for index in range(size)]
        values = rows if source_kind == "list" else tuple(rows)
        expression = (fpstreams.col(0) + fpstreams.col(1)) % 3 == 0

        result = fpstreams.pairs(values).filter_pairs(expression).to_dict(on_duplicate="last")

        assert result == {key: value for key, value in values if (key + value) % 3 == 0}

    assert native_sizes == [128]


@pytest.mark.parametrize(
    "mutation",
    ["program", "root", "expression", "row_program_call"],
)
def test_pair_row_expression_filter_rejects_mutated_cached_program(
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    """Every live RowProgram component stays authoritative over native lowering."""
    from fpstreams import _native
    from fpstreams.execution import _pair_row_filter  # noqa: F401 - freeze canonical snapshots
    from fpstreams.expressions.row_eval import RowProgram

    def unexpected(*_arguments: object) -> object:
        raise AssertionError("a mutated RowProgram reached native execution")

    expression = (fpstreams.col(0) + fpstreams.col(1)) % 3 == 0
    expression((0, 0))
    evaluator = expression._evaluate
    program = evaluator._program
    assert type(program) is RowProgram

    if mutation == "program":
        evaluator._program = lambda _row: False  # type: ignore[assignment]
    elif mutation == "root":
        object.__setattr__(program, "root", object())
    elif mutation == "expression":
        object.__setattr__(program, "expression", lambda _row: False)
    else:
        monkeypatch.setattr(RowProgram, "__call__", lambda _self, _row: False)
    monkeypatch.setattr(_native, "pair_i64_row_filter_to_dict_exact_prefix_v1", unexpected)

    values = [(index, index + 1) for index in range(128)]
    result = fpstreams.pairs(values).filter_pairs(expression).to_dict(on_duplicate="last")

    expected = (
        {key: value for key, value in values if (key + value) % 3 == 0}
        if mutation == "root"
        else {}
    )
    assert result == expected


def test_pair_row_expression_native_boundary_resumes_python_on_the_same_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An i64 boundary must be evaluated once by Python before the untouched suffix."""
    from fpstreams import _native

    endpoint = _native.pair_i64_row_filter_to_dict_exact_prefix_v1
    boundaries: list[object] = []

    def observed(*arguments: object) -> object:
        result = endpoint(*arguments)
        if result is not None:
            boundaries.append(result[0])
        return result

    monkeypatch.setattr(_native, "pair_i64_row_filter_to_dict_exact_prefix_v1", observed)
    values = [(index, index + 1) for index in range(2_048)]
    boundary = (2**70, 5)
    values.extend((boundary, (7, 9), (11, 13)))
    expression = (fpstreams.col(0) + fpstreams.col(1)) % 5 == 0

    automatic = fpstreams.pairs(values).filter_pairs(expression).to_dict(on_duplicate="last")
    canonical = (
        fpstreams.pairs(values)
        .filter_pairs(expression)
        .with_engine("python")
        .to_dict(on_duplicate="last")
    )

    assert automatic == canonical
    assert boundaries == [boundary]


def test_pair_row_expression_duplicate_error_stays_on_the_python_sink(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The error duplicate policy must preserve its ordered Python membership checks."""
    from fpstreams import _native

    def unexpected(*_arguments: object) -> object:
        raise AssertionError("duplicate-error collection must not enter the native sink")

    monkeypatch.setattr(_native, "pair_i64_row_filter_to_dict_exact_prefix_v1", unexpected)
    values = [(index, index) for index in range(1_024)] + [(0, 2)]
    expression = (fpstreams.col(0) + fpstreams.col(1)) % 2 == 0

    with pytest.raises(fpstreams.DuplicateKeyError, match=r"^Duplicate key: 0$"):
        fpstreams.pairs(values).filter_pairs(expression).to_dict()


def test_pair_row_expression_rechecks_source_methods_before_live_retained_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A post-open class mutation must resume without calling the replaced lookup."""
    from fpstreams.planning.source import Source
    from fpstreams.runtime import failpoints

    values = [(index, index + 1) for index in range(2_048)]
    expression = (fpstreams.col(0) + fpstreams.col(1)) % 3 == 0
    original_hit = failpoints.hit

    def replacement_retained(_source: Source[object]) -> object:
        raise AssertionError("a post-open guard must not call a replaced source method")

    def replace_after_open(name: str) -> None:
        if name == "source.open.after":
            monkeypatch.setattr(Source, "retained_sequence", replacement_retained)
        original_hit(name)

    monkeypatch.setattr(failpoints, "hit", replace_after_open)

    assert fpstreams.pairs(values).filter_pairs(expression).to_dict(on_duplicate="last") == {
        key: value for key, value in values if (key + value) % 3 == 0
    }


def test_pair_value_fexpr_filter_to_dict_uses_the_retained_native_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A large retained FExpr value filter reaches its native prefix endpoint."""
    from fpstreams import _native

    calls: list[int] = []

    def observed(*_arguments: object) -> None:
        calls.append(1)
        return None

    monkeypatch.setattr(
        _native,
        "pair_f64_value_filter_to_dict_exact_prefix_v1",
        observed,
        raising=False,
    )
    values = [(str(index % 32), index + 0.5) for index in range(140_000)]

    result = (
        fpstreams.pairs(values)
        .filter_values(fpstreams.fitem >= 139_990.0)
        .to_dict(on_duplicate="last")
    )

    assert result == {str(index % 32): index + 0.5 for index in range(139_990, 140_000)}
    assert calls == [1]


def test_pair_value_filter_rechecks_source_methods_after_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A source-open class mutation resumes without calling the replaced inspection hook."""
    from fpstreams.planning.source import Source

    values = _sequential_native_pairs()
    entries = fpstreams.pairs(values).filter_values(fpstreams.item >= 139_995)
    source = entries.to_flow()._pipeline.source

    def replacement_retained(_source: Source[object]) -> object:
        raise AssertionError("a post-open guard must not call a replaced source method")

    def open_and_replace_source_method() -> Iterator[tuple[int, int]]:
        monkeypatch.setattr(Source, "retained_sequence", replacement_retained)
        return iter(values)

    source._factory = open_and_replace_source_method

    assert entries.to_dict(on_duplicate="last") == {
        index: index for index in range(139_995, 140_000)
    }


@pytest.mark.parametrize(
    ("expression", "values"),
    [
        (fpstreams.item >= 139_995, _sequential_native_pairs()),
        (
            fpstreams.fitem >= 139_995.0,
            [(index, index + 0.5) for index in range(140_000)],
        ),
    ],
)
def test_pair_value_filter_rejects_a_mismatched_cached_evaluator(
    monkeypatch: pytest.MonkeyPatch,
    expression: object,
    values: list[tuple[int, object]],
) -> None:
    """Native instructions cannot bypass the evaluator used by the Python sink."""
    from fpstreams import _native
    from fpstreams.expressions.scalar import Expr, FExpr

    assert isinstance(expression, (Expr, FExpr))
    object.__setattr__(expression, "_evaluator", lambda _value: False)

    def unexpected(*_arguments: object) -> object:
        raise AssertionError("a mismatched evaluator must retain canonical Python execution")

    endpoint = (
        "pair_i64_value_filter_to_dict_exact_prefix_v1"
        if isinstance(expression, Expr)
        else "pair_f64_value_filter_to_dict_exact_prefix_v1"
    )
    monkeypatch.setattr(_native, endpoint, unexpected)

    entries = fpstreams.pairs(values).filter_values(expression)
    assert entries.to_dict(on_duplicate="last") == {}


def test_pair_value_filter_reuses_the_predicate_snapshotted_before_native_entry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A native boundary resumes with the predicate fixed before the first source row."""
    from fpstreams import _native

    expression = fpstreams.item >= 0
    values = _sequential_native_pairs()
    calls = 0

    def mutate_at_boundary(
        _output: dict[object, object],
        source: Iterator[object],
        _instructions: object,
        _keep_first: bool,
    ) -> tuple[object, bool]:
        nonlocal calls
        calls += 1
        first = next(source)
        object.__setattr__(expression, "_evaluator", lambda _value: False)
        return first, False

    monkeypatch.setattr(
        _native,
        "pair_i64_value_filter_to_dict_exact_prefix_v1",
        mutate_at_boundary,
    )

    assert fpstreams.pairs(values).filter_values(expression).to_dict(on_duplicate="last") == dict(
        values
    )
    assert calls == 1


@pytest.mark.parametrize("policy", ["first", "last"])
def test_pair_value_filter_native_preserves_first_key_and_selected_value_identity(
    monkeypatch: pytest.MonkeyPatch,
    policy: str,
) -> None:
    """Native duplicate handling retains the canonical key and selected value objects."""
    from fpstreams import _native

    endpoint = _native.pair_i64_value_filter_to_dict_exact_prefix_v1
    calls = 0

    def observed(*arguments: object) -> object:
        nonlocal calls
        calls += 1
        return endpoint(*arguments)

    monkeypatch.setattr(
        _native,
        "pair_i64_value_filter_to_dict_exact_prefix_v1",
        observed,
    )
    first_key = "".join(("identity-", "key"))
    equal_key = "".join(("identity", "-key"))
    assert first_key == equal_key and first_key is not equal_key
    first_value = int("1000000000000000001")
    last_value = int("1000000000000000002")
    values = [
        (first_key, first_value),
        (equal_key, last_value),
        *((index, index + 1) for index in range(140_000 - 2)),
    ]

    result = (
        fpstreams.pairs(values).filter_values(fpstreams.item > 0).to_dict(on_duplicate=policy)  # type: ignore[arg-type]
    )

    retained_key, retained_value = next(iter(result.items()))
    assert retained_key is first_key
    assert retained_value is (first_value if policy == "first" else last_value)
    assert calls == 1


def test_pair_value_filter_native_boundary_resumes_bool_subclass_and_bigint_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An incompatible value starts one Python suffix without replaying its row."""
    from fpstreams import _native

    endpoint = _native.pair_i64_value_filter_to_dict_exact_prefix_v1
    calls = 0
    events: list[str] = []

    class IntSubclass(int):
        def __mod__(self, other: object) -> object:
            events.append("mod")
            return super().__mod__(other)

    subclass_value = IntSubclass(6)
    bigint_value = 1 << 100
    values: list[tuple[object, object]] = [
        *((index % 16, index * 2) for index in range(131_072)),
        ("bool", True),
        ("subclass", subclass_value),
        ("bigint", bigint_value),
        ("tail", 8),
    ]

    def observed(*arguments: object) -> object:
        nonlocal calls
        calls += 1
        return endpoint(*arguments)

    monkeypatch.setattr(
        _native,
        "pair_i64_value_filter_to_dict_exact_prefix_v1",
        observed,
    )
    result = (
        fpstreams.pairs(values).filter_values(fpstreams.item % 2 == 0).to_dict(on_duplicate="last")
    )

    assert "bool" not in result
    assert result["subclass"] is subclass_value
    assert result["bigint"] is bigint_value
    assert result["tail"] == 8
    assert events == ["mod"]
    assert calls == 1


def test_pair_value_filter_native_arithmetic_boundaries_preserve_python_errors() -> None:
    """Checked overflow resumes successfully and zero-divide errors keep Python parity."""
    maximum = (1 << 63) - 1
    maximum_values = [
        *((index % 8, 0) for index in range(131_072)),
        ("maximum", maximum),
    ]
    maximum_result = (
        fpstreams.pairs(maximum_values)
        .filter_values(fpstreams.item + 1 > 0)
        .to_dict(on_duplicate="last")
    )
    assert maximum_result["maximum"] is maximum

    values = _sequential_native_pairs()
    expression = fpstreams.item // 0
    messages: list[str] = []
    for engine in ("auto", "python"):
        entries = fpstreams.pairs(values).filter_values(expression)
        if engine == "python":
            entries = entries.with_engine("python")
        with pytest.raises(ZeroDivisionError) as raised:
            entries.to_dict(on_duplicate="first")
        messages.append(str(raised.value))
    assert messages[0] == messages[1]

    huge_values = [
        *((index % 8, 1.0) for index in range(131_072)),
        ("huge", 1 << 10_000),
    ]
    float_messages: list[str] = []
    for engine in ("auto", "python"):
        entries = fpstreams.pairs(huge_values).filter_values(fpstreams.fitem > 0.0)
        if engine == "python":
            entries = entries.with_engine("python")
        with pytest.raises(OverflowError) as raised:
            entries.to_dict(on_duplicate="last")
        float_messages.append(str(raised.value))
    assert float_messages[0] == float_messages[1]


def test_pair_value_filter_consumes_factory_drift_and_live_list_iterator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The endpoint consumes the opened iterator, including drift and a post-open append."""
    from fpstreams import _native

    i64_endpoint = _native.pair_i64_value_filter_to_dict_exact_prefix_v1
    retained = [(index, -1) for index in range(140_000)]
    opened = tuple((str(index % 32), index) for index in range(140_000))
    entries = fpstreams.pairs(retained).filter_values(fpstreams.item >= 139_995)
    entries.to_flow()._pipeline.source._factory = lambda: iter(opened)
    assert entries.to_dict(on_duplicate="last") == {
        str(index % 32): index for index in range(139_995, 140_000)
    }

    live = [(index % 8, 0) for index in range(131_072)]
    late = ("late", 2)
    calls = 0

    def append_then_consume(*arguments: object) -> object:
        nonlocal calls
        calls += 1
        live.append(late)
        return i64_endpoint(*arguments)

    monkeypatch.setattr(
        _native,
        "pair_i64_value_filter_to_dict_exact_prefix_v1",
        append_then_consume,
    )
    result = fpstreams.pairs(live).filter_values(fpstreams.item > 0).to_dict(on_duplicate="first")
    assert result == {"late": 2}
    assert calls == 1


@pytest.mark.skipif(sys.platform == "win32", reason="setitimer is Unix-only")
def test_pair_value_filter_native_signal_mutation_is_seen_by_the_opened_list_iterator() -> None:
    """Signal handling can append a row that the already-open native iterator then consumes."""
    import signal

    from fpstreams.execution._pair_dict import try_consume_pair_value_filter_to_dict

    values: list[tuple[object, int]] = [(index % 8, 0) for index in range(500_000)]
    late = ("late", 2)
    previous = signal.getsignal(signal.SIGALRM)
    appended = False

    def append_once(_signum: int, frame: object) -> None:
        nonlocal appended
        if (
            not appended
            and getattr(frame, "f_code", None) is try_consume_pair_value_filter_to_dict.__code__
        ):
            values.append(late)
            appended = True

    signal.signal(signal.SIGALRM, append_once)
    signal.setitimer(signal.ITIMER_REAL, 0.001, 0.001)
    try:
        result = (
            fpstreams.pairs(values).filter_values(fpstreams.item > 0).to_dict(on_duplicate="first")
        )
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)

    assert appended
    assert result == {"late": 2}


def test_pair_value_filter_post_open_failpoint_closes_the_source_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failpoint activated while opening deopts and leaves one cleanup owner."""
    from contextlib import AbstractContextManager

    from fpstreams import _native
    from fpstreams.runtime.failpoints import failpoint

    values = _sequential_native_pairs()
    entries = fpstreams.pairs(values).filter_values(fpstreams.item >= 0)
    source = entries.to_flow()._pipeline.source
    error = RuntimeError("stop after pull")
    closes = 0

    def unexpected(*_arguments: object) -> object:
        raise AssertionError("an active failpoint must disable the native filter sink")

    class OpenedIterator(Iterator[tuple[int, int]]):
        def __init__(self, scope: AbstractContextManager[None]) -> None:
            self._values = iter(values)
            self._scope = scope

        def __next__(self) -> tuple[int, int]:
            return next(self._values)

        def close(self) -> None:
            nonlocal closes
            closes += 1
            if closes != 1:
                raise AssertionError("source iterator was closed more than once")
            self._scope.__exit__(None, None, None)

    def open_with_failpoint() -> Iterator[tuple[int, int]]:
        scope = failpoint("iterator.pull.after", error)
        scope.__enter__()
        return OpenedIterator(scope)

    monkeypatch.setattr(
        _native,
        "pair_i64_value_filter_to_dict_exact_prefix_v1",
        unexpected,
    )
    source._factory = open_with_failpoint
    with pytest.raises(RuntimeError) as raised:
        entries.to_dict(on_duplicate="last")

    assert raised.value is error
    assert closes == 1


@pytest.mark.parametrize("guard", ["endpoint", "operation"])
def test_pair_value_filter_rechecks_live_endpoint_and_operation(
    monkeypatch: pytest.MonkeyPatch,
    guard: str,
) -> None:
    """Source opening cannot leave a stale endpoint or stale predicate in native execution."""
    from fpstreams import _native
    from fpstreams.planning._pair_stages import PairFilterDescriptor

    values = [(index % 8, index) for index in range(140_000)]
    entries = fpstreams.pairs(values).filter_values(fpstreams.item >= 139_995)
    source = entries.to_flow()._pipeline.source

    def unexpected(*_arguments: object) -> object:
        raise AssertionError("a changed live guard must retain Python execution")

    if guard == "endpoint":

        def open_and_replace() -> Iterator[tuple[int, int]]:
            monkeypatch.setattr(
                _native,
                "pair_i64_value_filter_to_dict_exact_prefix_v1",
                unexpected,
            )
            return iter(values)

        expected = {index % 8: index for index in range(139_995, 140_000)}
    else:
        operation = entries.to_flow()._pipeline.operations[0]
        assert isinstance(operation.predicate, PairFilterDescriptor)

        def open_and_replace() -> Iterator[tuple[int, int]]:
            object.__setattr__(operation.predicate, "callback", lambda _value: False)
            return iter(values)

        expected = {}
    source._factory = open_and_replace

    assert entries.to_dict(on_duplicate="last") == expected


def test_pair_value_filter_native_sink_stays_off_for_unsupported_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Explicit Python, one-shot, lambda-filter, and map paths remain unchanged."""
    from fpstreams import _native

    def unexpected(*_arguments: object) -> object:
        raise AssertionError("an unsupported path must not enter the native value-filter sink")

    monkeypatch.setattr(
        _native,
        "pair_i64_value_filter_to_dict_exact_prefix_v1",
        unexpected,
    )
    values = _sequential_native_pairs()

    explicit = (
        fpstreams.pairs(values)
        .filter_values(fpstreams.item >= 139_999)
        .with_engine("python")
        .to_dict(on_duplicate="first")
    )
    one_shot = (
        fpstreams.pairs(iter(values))
        .filter_values(fpstreams.item >= 139_999)
        .to_dict(on_duplicate="first")
    )
    callback_filter = (
        fpstreams.pairs(values)
        .filter_values(lambda value: value >= 139_999)
        .to_dict(on_duplicate="first")
    )
    mapped = fpstreams.pairs(values).map_values(fpstreams.item + 1).to_dict(on_duplicate="last")

    assert explicit == one_shot == callback_filter == {139_999: 139_999}
    assert mapped[0] == 1 and mapped[139_999] == 140_000


def test_pair_unique_to_dict_adapts_only_high_cardinality_auto_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The retained Rust sink stays off for low cardinality and explicit engines."""
    from fpstreams import _native

    endpoint = _native.pair_unique_exact_prefix_v1
    calls: list[int] = []

    def observed(output: dict[object, object], source: Iterator[object]) -> object:
        calls.append(1)
        return endpoint(output, source)

    monkeypatch.setattr(_native, "pair_unique_exact_prefix_v1", observed)
    high = _sequential_native_pairs()
    low = [(index % 16, index) for index in range(140_000)]

    for policy in ("error", "first", "last"):
        assert fpstreams.pairs(high).unique_keys().to_dict(on_duplicate=policy) == dict(high)
    assert calls == [1, 1, 1]
    assert fpstreams.pairs(low).unique_keys().to_dict() == {index: index for index in range(16)}
    assert calls == [1, 1, 1]
    assert fpstreams.pairs(high).unique_keys().with_engine("python").to_dict() == dict(high)
    assert calls == [1, 1, 1]
    with_none = [(None, -1), *high]
    assert fpstreams.pairs(with_none).unique_keys().to_dict() == dict(with_none)
    assert calls == [1, 1, 1]


def test_pair_unique_native_prefix_resumes_custom_keys_with_canonical_trace() -> None:
    """A custom-key boundary is not hashed until seeded Python uniqueness resumes."""
    prefix = _sequential_native_pairs()

    def run(engine: str) -> tuple[list[tuple[object, ...]], bool, int]:
        events: list[tuple[object, ...]] = []

        class Key:
            def __init__(self, name: str) -> None:
                self.name = name

            def __hash__(self) -> int:
                events.append(("hash", self.name))
                return 1_000_003

            def __eq__(self, other: object) -> bool:
                assert isinstance(other, Key)
                events.append(("eq", self.name, other.name))
                return True

        first = Key("first")
        duplicate = Key("duplicate")
        values = [*prefix, (first, 1), (duplicate, 2)]
        pairs = fpstreams.pairs(values).unique_keys()
        if engine == "python":
            pairs = pairs.with_engine("python")
        result = pairs.to_dict()
        observed = list(events)
        retained_key, retained_value = list(result.items())[-1]
        return observed, retained_key is first, retained_value

    automatic = run("auto")
    canonical = run("python")

    assert automatic == canonical
    assert automatic[1:] == (True, 1)


@pytest.mark.parametrize("policy", ["error", "first", "last"])
def test_pair_unique_hybrid_rebuilds_canonical_collision_layout(policy: str) -> None:
    """Incremental seen reconstruction preserves custom equality order after collisions."""
    modulus = sys.hash_info.modulus
    colliding = [(42 + index * modulus, index) for index in range(12)]
    filler = [(1_000_000 + index, index) for index in range(140_000 - len(colliding))]

    def run(engine: str) -> tuple[list[tuple[object, ...]], int, int]:
        events: list[tuple[object, ...]] = []

        class Key:
            def __hash__(self) -> int:
                events.append(("hash",))
                return 42

            def __eq__(self, other: object) -> bool:
                events.append(("eq", other))
                return False

        key = Key()
        pairs = fpstreams.pairs([*colliding, *filler, (key, 999)]).unique_keys()
        if engine == "python":
            pairs = pairs.with_engine("python")
        result = pairs.to_dict(on_duplicate=policy)  # type: ignore[arg-type]
        return list(events), len(result), list(result.values())[-1]

    automatic = run("auto")
    canonical = run("python")

    assert automatic == canonical
    assert any(event[0] == "eq" for event in automatic[0])
    assert automatic[1:] == (140_001, 999)


def test_pair_unique_to_dict_preserves_string_subclass_policy_trace() -> None:
    """A validated policy subclass still executes the canonical consumer comparisons."""
    values = _sequential_native_pairs()

    def run(engine: str) -> tuple[list[object], int]:
        events: list[object] = []

        class Policy(str):
            __hash__ = str.__hash__

            def __eq__(self, other: object) -> bool:
                events.append(other)
                return super().__eq__(other)

        pairs = fpstreams.pairs(values).unique_keys()
        if engine == "python":
            pairs = pairs.with_engine("python")
        result = pairs.to_dict(on_duplicate=Policy("error"))  # type: ignore[arg-type]
        return events, len(result)

    assert run("auto") == run("python")


def test_pair_unique_to_dict_does_not_compare_engine_subclass_during_eligibility(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fast-path eligibility deopts an engine subclass without invoking its comparisons."""
    values = _sequential_native_pairs()

    from fpstreams.execution import _pair_dict

    def run() -> tuple[list[tuple[str, object, object]], dict[int, int]]:
        events: list[tuple[str, object, object]] = []

        class Engine(str):
            def __eq__(self, other: object) -> bool:
                events.append(("eq", str(self), str(other)))
                return super().__eq__(other)

            def __ne__(self, other: object) -> bool:
                events.append(("ne", str(self), str(other)))
                return super().__ne__(other)

            __hash__ = str.__hash__

        entries = fpstreams.pairs(values).unique_keys().with_engine(Engine("auto"))  # type: ignore[arg-type]
        events.clear()
        return events, entries.to_dict()

    candidate = run()

    def disabled(*_arguments: object) -> tuple[bool, None]:
        return False, None

    with monkeypatch.context() as context:
        context.setattr(_pair_dict, "try_consume_pair_unique_to_dict", disabled)
        canonical = run()

    assert candidate == canonical


def test_pair_unique_to_dict_rechecks_policy_mutated_during_source_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A live policy subclass deopts before its observable comparisons are skipped."""
    from fpstreams.execution._pair_dict import PairDictConsumer

    values = _sequential_native_pairs()
    entries = fpstreams.pairs(values).unique_keys()
    source = entries.to_flow()._pipeline.source
    captured: list[PairDictConsumer] = []
    events: list[object] = []
    original_init = PairDictConsumer.__init__

    class Policy(str):
        __hash__ = str.__hash__

        def __eq__(self, other: object) -> bool:
            events.append(other)
            return super().__eq__(other)

    def capture_consumer(self: PairDictConsumer, policy: object) -> None:
        original_init(self, policy)  # type: ignore[arg-type]
        captured.append(self)

    def open_and_mutate_policy() -> Iterator[tuple[int, int]]:
        object.__setattr__(captured[0], "policy", Policy("error"))
        return iter(values)

    monkeypatch.setattr(PairDictConsumer, "__init__", capture_consumer)
    source._factory = open_and_mutate_policy

    assert entries.to_dict() == dict(values)
    assert events == ["last", "first"]


def test_pair_unique_to_dict_rechecks_consumer_class_mutated_during_source_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A live same-layout class replacement retains canonical dynamic dispatch."""
    from fpstreams.execution._pair_dict import PairDictConsumer

    values = _sequential_native_pairs()
    captured: list[PairDictConsumer] = []
    original_init = PairDictConsumer.__init__

    class Replacement(PairDictConsumer):
        __slots__ = ()

        def __call__(self, _iterator: Iterator[object]) -> dict[str, int]:
            return {"replacement": 1}

    def capture_consumer(self: PairDictConsumer, policy: object) -> None:
        original_init(self, policy)  # type: ignore[arg-type]
        captured.append(self)

    monkeypatch.setattr(PairDictConsumer, "__init__", capture_consumer)

    def run(engine: str) -> dict[object, object]:
        entries = fpstreams.pairs(values).unique_keys()
        source = entries.to_flow()._pipeline.source

        def open_and_mutate_class() -> Iterator[tuple[int, int]]:
            object.__setattr__(captured[-1], "__class__", Replacement)
            return iter(values)

        source._factory = open_and_mutate_class
        if engine == "python":
            entries = entries.with_engine("python")
        return entries.to_dict()

    assert run("auto") == run("python") == {"replacement": 1}


def test_pair_unique_native_boundary_preserves_pep479_stop_iteration() -> None:
    """A custom hash exhaustion at the hybrid boundary remains a generator error."""

    class Key:
        def __hash__(self) -> int:
            raise StopIteration("stop hashing")

    prefix = _sequential_native_pairs()
    values = [*prefix, (Key(), 1)]

    with pytest.raises(RuntimeError, match="generator raised StopIteration"):
        fpstreams.pairs(values).unique_keys().to_dict()
    with pytest.raises(RuntimeError, match="generator raised StopIteration"):
        fpstreams.pairs(values).unique_keys().with_engine("python").to_dict()


@pytest.mark.skipif(sys.platform == "win32", reason="setitimer is Unix-only")
@pytest.mark.parametrize("engine", ["auto", "python"])
def test_pair_unique_native_signal_stop_iteration_is_not_pep479(engine: str) -> None:
    """StopIteration raised by a signal handler remains a normal terminal exception."""
    import signal

    from fpstreams.execution._pair_dict import (
        PairDictConsumer,
        try_consume_pair_unique_to_dict,
    )

    values = [(index, index) for index in range(1_000_000)]
    entries = fpstreams.pairs(values).unique_keys()
    if engine == "python":
        entries = entries.with_engine("python")
    previous = signal.getsignal(signal.SIGALRM)
    terminal_codes = {
        PairDictConsumer.__call__.__code__,
        try_consume_pair_unique_to_dict.__code__,
    }

    def stop(_signum: int, _frame: object) -> None:
        # Repeating alarms ignore the UniqueOp generator frame until Python is in the normal
        # terminal frame that corresponds to the native endpoint call.
        if getattr(_frame, "f_code", None) in terminal_codes:
            raise StopIteration("timer stop")

    signal.signal(signal.SIGALRM, stop)
    signal.setitimer(signal.ITIMER_REAL, 0.005, 0.001)
    try:
        with pytest.raises(StopIteration, match="timer stop") as raised:
            entries.to_dict()
        assert raised.value.__cause__ is None
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def test_pair_unique_native_boundary_preserves_malformed_pair_errors() -> None:
    """A malformed suffix row is validated by the same canonical dictionary consumer."""
    values: list[object] = [(index, index) for index in range(140_000)]
    values.append(("too", "many", "items"))

    with pytest.raises(ValueError, match="too many values to unpack"):
        fpstreams.pairs(values).unique_keys().to_dict()  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="too many values to unpack"):
        fpstreams.pairs(values).unique_keys().with_engine("python").to_dict()  # type: ignore[arg-type]


def test_pair_unique_to_dict_rechecks_failpoints_activated_during_source_open() -> None:
    """A post-open failpoint resumes the instrumented canonical iterator exactly once."""
    from contextlib import AbstractContextManager

    from fpstreams.runtime.failpoints import failpoint

    values = _sequential_native_pairs()
    entries = fpstreams.pairs(values).unique_keys()
    source = entries.to_flow()._pipeline.source
    error = RuntimeError("stop after pull")
    closed = False

    class OpenedIterator(Iterator[tuple[int, int]]):
        def __init__(self, scope: AbstractContextManager[None]) -> None:
            self._values = iter(values)
            self._scope = scope

        def __next__(self) -> tuple[int, int]:
            return next(self._values)

        def close(self) -> None:
            nonlocal closed
            closed = True
            self._scope.__exit__(None, None, None)

    def open_with_failpoint() -> Iterator[tuple[int, int]]:
        scope = failpoint("iterator.pull.after", error)
        scope.__enter__()
        return OpenedIterator(scope)

    source._factory = open_with_failpoint
    with pytest.raises(RuntimeError) as raised:
        entries.to_dict()

    assert raised.value is error
    assert closed


@pytest.mark.parametrize("engine", ["auto", "python"])
def test_pair_unique_post_open_fallback_closes_source_once(engine: str) -> None:
    """The canonical fallback exclusively owns a non-idempotent opened iterator."""
    values = _sequential_native_pairs()
    entries = fpstreams.pairs(values).unique_keys()
    source = entries.to_flow()._pipeline.source
    closes = 0

    class ClosingIterator(Iterator[tuple[int, int]]):
        def __init__(self) -> None:
            self._values = iter(values)

        def __next__(self) -> tuple[int, int]:
            return next(self._values)

        def close(self) -> None:
            nonlocal closes
            closes += 1
            if closes != 1:
                raise AssertionError("source iterator was closed more than once")

    source._factory = ClosingIterator
    if engine == "python":
        entries = entries.with_engine("python")

    assert entries.to_dict() == dict(values)
    assert closes == 1


@pytest.mark.parametrize("engine", ["auto", "python"])
def test_pair_unique_post_open_respects_noncallable_execution_hook(
    monkeypatch: pytest.MonkeyPatch,
    engine: str,
) -> None:
    """A source-open replacement of the live executor fails on both engine paths."""
    from fpstreams.execution import physical

    values = _sequential_native_pairs()
    entries = fpstreams.pairs(values).unique_keys()
    source = entries.to_flow()._pipeline.source

    def open_and_remove_hook() -> Iterator[tuple[int, int]]:
        monkeypatch.setattr(physical, "open_operations", None)
        return iter(values)

    source._factory = open_and_remove_hook
    if engine == "python":
        entries = entries.with_engine("python")

    with pytest.raises(TypeError, match="not callable"):
        entries.to_dict()


@pytest.mark.parametrize(
    "guard",
    [
        "source_open",
        "endpoint_live",
        "consumer_live",
        "handler_live",
        "operation_live",
    ],
)
def test_pair_unique_to_dict_rechecks_live_native_guards(  # noqa: C901 - guard-specific drift
    monkeypatch: pytest.MonkeyPatch,
    guard: str,
) -> None:
    """Source opening cannot leave a stale endpoint or bypass a replaced opener."""
    from fpstreams import _native
    from fpstreams.execution import sync_ops
    from fpstreams.execution._pair_dict import PairDictConsumer
    from fpstreams.planning.source import Source
    from fpstreams.planning.sync import UniqueOp

    values = _sequential_native_pairs()
    entries = fpstreams.pairs(values).unique_keys()
    source = entries.to_flow()._pipeline.source
    endpoint = _native.pair_unique_exact_prefix_v1
    opens = 0

    def unexpected(*_arguments: object) -> object:
        raise AssertionError("a changed live guard must keep the canonical Python path")

    if guard == "source_open":
        original_open = Source.open

        def wrapped_open(self: Source[object]) -> Iterator[object]:
            nonlocal opens
            opens += 1
            return original_open(self)

        monkeypatch.setattr(Source, "open", wrapped_open)
        monkeypatch.setattr(_native, "pair_unique_exact_prefix_v1", unexpected)
    elif guard == "endpoint_live":

        def open_and_replace_endpoint() -> Iterator[tuple[int, int]]:
            nonlocal opens
            opens += 1
            monkeypatch.setattr(_native, "pair_unique_exact_prefix_v1", unexpected)
            return iter(values)

        source._factory = open_and_replace_endpoint
    elif guard == "consumer_live":

        def replacement_consumer(
            _consumer: PairDictConsumer,
            _iterator: Iterator[object],
        ) -> dict[str, int]:
            return {"replacement": 1}

        def open_and_replace_consumer() -> Iterator[tuple[int, int]]:
            nonlocal opens
            opens += 1
            monkeypatch.setattr(PairDictConsumer, "__call__", replacement_consumer)
            return iter(values)

        source._factory = open_and_replace_consumer
    elif guard == "handler_live":

        def empty_unique(
            _iterator: Iterator[object],
            _operation: UniqueOp,
        ) -> Iterator[object]:
            return iter(())

        def open_and_replace_handler() -> Iterator[tuple[int, int]]:
            nonlocal opens
            opens += 1
            monkeypatch.setitem(sync_ops.OPERATION_HANDLERS, UniqueOp, empty_unique)
            return iter(values)

        source._factory = open_and_replace_handler
    else:
        operation = entries.to_flow()._pipeline.operations[0]

        def open_and_replace_operation() -> Iterator[tuple[int, int]]:
            nonlocal opens
            opens += 1
            object.__setattr__(operation, "key", lambda _pair: 0)
            return iter(values)

        source._factory = open_and_replace_operation

    expected: dict[object, object]
    if guard == "consumer_live":
        expected = {"replacement": 1}
    elif guard == "handler_live":
        expected = {}
    elif guard == "operation_live":
        expected = {0: 0}
    else:
        expected = dict(values)
    assert entries.to_dict() == expected
    assert opens == 1
    if guard == "endpoint_live":
        assert endpoint is not _native.pair_unique_exact_prefix_v1


def test_pairs_exposes_engine_control_without_consuming_input() -> None:
    entries = fpstreams.pairs(iter([("a", 1), ("b", 2)])).map_values(lambda value: value * 10)
    underlying = entries.to_flow()
    forced_python = entries.with_engine("python")

    assert forced_python.to_flow().explain().to_dict()["requested_engine"] == "python"
    assert entries.to_dict() == {"a": 10, "b": 20}

    with pytest.raises(fpstreams.FlowConsumedError):
        underlying.to_list()


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


def test_pair_selectors_reuse_one_callable_per_side() -> None:
    """Pair projections, uniqueness, and sorting share cached field selectors."""
    entries = fpstreams.pairs([("a", 1)])

    key_selectors = (
        entries.keys()._pipeline.operations[0].function,
        entries.unique_keys().to_flow()._pipeline.operations[0].key,
        entries.sort_by_key().to_flow()._pipeline.operations[0].key,
    )
    value_selectors = (
        entries.values()._pipeline.operations[0].function,
        entries.sort_by_value().to_flow()._pipeline.operations[0].key,
    )

    assert key_selectors[0] is key_selectors[1] is key_selectors[2]
    assert value_selectors[0] is value_selectors[1]
    assert key_selectors[0] is not value_selectors[0]

    first_invert = entries.invert().to_flow()._pipeline.operations[0].function
    second_invert = entries.invert().to_flow()._pipeline.operations[0].function
    assert first_invert is second_invert
    assert entries.invert().to_flow().to_list() == [(1, "a")]


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


def test_collect_values_closes_source_before_finishing_states() -> None:
    """Per-key finish callbacks retain the established after-source-close ordering."""

    class Source(Iterator[tuple[str, int]]):
        def __init__(self) -> None:
            self._values = iter((("a", 1),))
            self.closed = False

        def __next__(self) -> tuple[str, int]:
            return next(self._values)

        def close(self) -> None:
            self.closed = True

    source = Source()
    observed: list[bool] = []
    collector = fpstreams.Collector(
        lambda: 0,
        lambda total, value: total + value,
        lambda total: (observed.append(source.closed), total)[1],
    )

    assert fpstreams.pairs(source).collect_values(collector) == {"a": 1}
    assert observed == [True]


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


def test_pair_reducer_keeps_step_error_primary_when_source_close_fails() -> None:
    """Reducer failure remains primary while source cleanup is recorded as a note."""
    primary = ValueError("step failed")
    close_calls = 0

    def source() -> Iterator[tuple[str, int]]:
        nonlocal close_calls
        try:
            yield "a", 1
        finally:
            close_calls += 1
            raise OSError("source close failed")

    def fail(_state: Any, _value: int) -> Any:
        raise primary

    with pytest.raises(ValueError) as captured:
        fpstreams.pairs(source()).collect_values(fpstreams.Collector(lambda: None, fail))

    assert captured.value is primary
    assert captured.value.__notes__ == ["cleanup failed with OSError: source close failed"]
    assert close_calls == 1


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


def test_aggregate_values_revalidates_closed_lanes_after_source_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Source-open mutations must deopt on the same, still-unconsumed iterator."""
    total = fpstreams.agg.sum()

    def replacement_factory() -> Callable[[int, int], int]:
        def select(value: int) -> int:
            return value

        def replacement(current: int, value: int) -> int:
            return current + select(value) * 10

        return replacement

    replacement = replacement_factory()
    opens = 0

    class Source:
        def __iter__(self) -> Iterator[tuple[str, int]]:
            nonlocal opens
            opens += 1
            monkeypatch.setattr(total.step, "__code__", replacement.__code__)
            return iter((("a", 1), ("a", 2)))

    assert fpstreams.pairs(Source()).aggregate_values(total=total) == {"a": {"total": 30}}
    assert opens == 1


@pytest.mark.parametrize("guard", ["helpers", "closed"])
def test_aggregate_values_freezes_guards_before_direct_source_open(
    monkeypatch: pytest.MonkeyPatch,
    guard: str,
) -> None:
    """Direct source opening cannot replace the guard that validates its own mutations."""
    pairs_module = sys.modules["fpstreams.streams.pairs"]
    total = fpstreams.agg.sum()

    def replacement_step(
        states: dict[str, int], items: tuple[tuple[str, object], ...], value: int
    ) -> None:
        for name, _aggregation in items:
            states[name] += value * 10

    def replacement_factory() -> Callable[[int, int], int]:
        def select(value: int) -> int:
            return value

        def replacement(current: int, value: int) -> int:
            return current + select(value) * 10

        return replacement

    class Source:
        def __iter__(self) -> Iterator[tuple[str, int]]:
            if guard == "helpers":
                monkeypatch.setattr(pairs_module, "_aggregation_helpers_are_live", lambda: True)
                monkeypatch.setattr(pairs_module, "step_aggregations", replacement_step)
            else:
                from fpstreams.execution._pair_aggregate import ClosedPairAggregations

                monkeypatch.setattr(ClosedPairAggregations, "is_live", lambda _self: True)
                monkeypatch.setattr(
                    total.step,
                    "__code__",
                    replacement_factory().__code__,
                )
            return iter((("a", 1), ("a", 2)))

    assert fpstreams.pairs(Source()).aggregate_values(total=total) == {"a": {"total": 30}}


def test_aggregate_values_revalidates_after_the_first_lazy_source_pull(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A generator body can change a retained lane before yielding its first pair."""
    total = fpstreams.agg.sum()

    def replacement_factory() -> Callable[[int, int], int]:
        def select(value: int) -> int:
            return value

        def replacement(current: int, value: int) -> int:
            return current + select(value) * 10

        return replacement

    def source() -> Iterator[tuple[str, int]]:
        monkeypatch.setattr(total.step, "__code__", replacement_factory().__code__)
        yield "a", 1
        yield "a", 2

    assert fpstreams.pairs(source()).aggregate_values(total=total) == {"a": {"total": 30}}


def test_aggregate_values_revalidates_helpers_after_the_first_lazy_pull(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """In-place helper code changes remain authoritative over compact lanes."""
    pairs_module = sys.modules["fpstreams.streams.pairs"]

    def replacement(_states: object, _items: object, _value: object) -> None:
        raise RuntimeError("live step helper")

    def source() -> Iterator[tuple[str, int]]:
        monkeypatch.setattr(
            pairs_module.step_aggregations,
            "__code__",
            replacement.__code__,
        )
        yield "a", 1

    with pytest.raises(RuntimeError, match="live step helper"):
        fpstreams.pairs(source()).aggregate_values(total=fpstreams.agg.sum())


def test_aggregate_values_observes_step_mutation_after_the_first_value() -> None:
    """A value callback can replace the lifecycle used by the next pair."""
    total = fpstreams.agg.sum()
    events: list[str] = []

    def replacement(state: int, _value: object) -> int:
        events.append("replacement")
        return state + 100

    class Value:
        def __radd__(self, state: int) -> int:
            events.append("radd")
            object.__setattr__(total, "step", replacement)
            return state + 2

    result = (
        fpstreams.pairs([("key", Value()), ("key", 3)])
        .with_engine("python")
        .aggregate_values(total=total)
    )

    assert result == {"key": {"total": 102}}
    assert events == ["radd", "replacement"]


def test_aggregate_values_freezes_helper_guard_code_before_lazy_pull(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A source cannot forge the post-pull public-helper guard itself."""
    pairs_module = sys.modules["fpstreams.streams.pairs"]

    def always_live() -> bool:
        return True

    def replacement(
        states: dict[str, int], items: tuple[tuple[str, object], ...], value: int
    ) -> None:
        for name, _aggregation in items:
            states[name] += value * 10

    def source() -> Iterator[tuple[str, int]]:
        monkeypatch.setattr(
            pairs_module._aggregation_helpers_are_live,
            "__code__",
            always_live.__code__,
        )
        monkeypatch.setattr(
            pairs_module.step_aggregations,
            "__code__",
            replacement.__code__,
        )
        yield "a", 1
        yield "a", 2

    assert fpstreams.pairs(source()).aggregate_values(total=fpstreams.agg.sum()) == {
        "a": {"total": 30}
    }


def test_aggregate_values_observes_initializer_mutation_from_key_hash() -> None:
    """A key callback can replace the initializer used for its first state."""
    total = fpstreams.agg.sum()
    events: list[str] = []

    def initializer() -> int:
        events.append("init")
        return 100

    class Key:
        def __hash__(self) -> int:
            events.append("hash")
            object.__setattr__(total, "initializer", initializer)
            return 1

    key = Key()
    result = fpstreams.pairs([(key, 2)]).with_engine("python").aggregate_values(total=total)

    assert next(iter(result)) is key
    assert list(result.values()) == [{"total": 102}]
    assert events == ["hash", "init", "hash", "hash"]


def test_aggregate_values_observes_finish_helper_mutation_after_later_pull(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A lazy source can replace the authoritative finishing helper after grouping starts."""
    pairs_module = sys.modules["fpstreams.streams.pairs"]
    events: list[str] = []

    def replacement(_states: object, _items: object) -> dict[str, int]:
        events.append("finish")
        return {"changed": 999}

    def source() -> Iterator[tuple[str, int]]:
        events.append("yield:1")
        yield "key", 1
        events.append("replace")
        monkeypatch.setattr(pairs_module, "finish_aggregations", replacement)
        events.append("yield:2")
        yield "key", 2
        events.append("stop")

    result = (
        fpstreams.pairs(source()).with_engine("python").aggregate_values(total=fpstreams.agg.sum())
    )

    assert result == {"key": {"changed": 999}}
    assert events == ["yield:1", "replace", "yield:2", "stop", "finish"]


def test_aggregate_values_releases_seed_at_the_canonical_iteration_boundary() -> None:
    """The closed lane must not extend the lifetime of its unexamined first value."""
    released = False

    class FirstValue:
        def __radd__(self, _other: object) -> int:
            return 0

        def __del__(self) -> None:
            nonlocal released
            released = True

    class LastValue:
        def __radd__(self, _other: object) -> int:
            return 100 if released else 1

    def source() -> Iterator[tuple[str, object]]:
        yield "a", FirstValue()
        yield "a", 0
        yield "a", LastValue()

    assert fpstreams.pairs(source()).aggregate_values(total=fpstreams.agg.sum()) == {
        "a": {"total": 100}
    }


def test_aggregate_values_revalidates_after_lazy_relational_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A relational upstream may not run its source body until the first terminal pull."""
    from fpstreams.streams.pairs import Pairs

    total = fpstreams.agg.sum()

    def source() -> Iterator[dict[str, int]]:
        object.__setattr__(total, "initializer", lambda: 10)
        yield {"key": 1, "value": 5}

    pair_flow = (
        fpstreams.rows(source())
        .group_by("key")
        .aggregate(value=fpstreams.agg.sum("value"))
        .map(lambda row: (row["key"], row["value"]))
    )

    assert Pairs(pair_flow).aggregate_values(total=total) == {1: {"total": 15}}


def test_aggregate_values_closes_source_before_live_finish() -> None:
    """Closed lane states are finalized only after the live source has closed."""
    total = fpstreams.agg.sum()
    observed: list[bool] = []
    closed = False

    def source() -> Iterator[tuple[str, int]]:
        nonlocal closed
        try:
            yield "a", 1
            yield "a", 2
        finally:
            closed = True

            def finish(value: int) -> int:
                observed.append(closed)
                return value + 100

            object.__setattr__(total, "finish", finish)

    assert fpstreams.pairs(source()).aggregate_values(total=total) == {"a": {"total": 103}}
    assert observed == [True]


def test_aggregate_values_keeps_step_error_primary_when_source_close_fails() -> None:
    """A closed-lane arithmetic error stays primary when source cleanup also fails."""
    primary = ValueError("addition failed")
    close_calls = 0

    class BrokenValue:
        def __radd__(self, _other: object) -> object:
            raise primary

    def source() -> Iterator[tuple[str, BrokenValue]]:
        nonlocal close_calls
        try:
            yield "a", BrokenValue()
        finally:
            close_calls += 1
            raise OSError("source close failed")

    with pytest.raises(ValueError) as captured:
        fpstreams.pairs(source()).aggregate_values(total=fpstreams.agg.sum())

    assert captured.value is primary
    assert captured.value.__notes__ == ["cleanup failed with OSError: source close failed"]
    assert close_calls == 1


def test_reducer_metadata_and_partitioned_execution_are_observable() -> None:
    from fpstreams.collecting.reducer import merge_reducer_states, run_partitioned_reducer
    from fpstreams.planning.semantics import StateProfile
    from fpstreams.testing import ReducerLawReport, assert_reducer_laws, check_reducer_laws

    laws = fpstreams.ReducerLaws(
        True,
        True,
        False,
        True,
        fpstreams.EmptyInputPolicy.FINISH_IDENTITY,
        StateProfile.constant(),
        fpstreams.LawProvenance.USER_ASSERTED,
    )
    reducer = fpstreams.Reducer(
        lambda: 0,
        lambda total, value: total + value,
        merge=lambda left, right: left + right,
        laws=laws,
    )

    assert reducer.reduce([1, 2, 3, 4, 5]) == 15
    assert merge_reducer_states([], reducer) == 0
    assert merge_reducer_states([7], reducer) == 7
    assert merge_reducer_states([1, 2, 3, 4, 5], reducer) == 15
    assert run_partitioned_reducer([], reducer, partition_size=2) == 0
    assert run_partitioned_reducer(range(1, 6), reducer, partition_size=2) == 15
    report = check_reducer_laws(reducer, range(1, 6), partitions=[(2, 3), (0, 5)])
    assert report == ReducerLawReport(True, True, True, ((2, 3), (0, 5)))
    assert_reducer_laws(reducer, range(1, 6), partitions=[(2, 3), (0, 5)])
    assert fpstreams.explain_reduction(reducer).to_dict() == {
        "mergeable": True,
        "combine_declared": True,
        "laws": {
            "associative": True,
            "commutative": True,
            "order_sensitive": False,
            "identity": True,
            "empty_input": "finish_identity",
            "state": {"kind": "constant", "bound": None, "spillable": False},
            "provenance": "user_asserted",
        },
    }

    ordinary = fpstreams.Collector(list, lambda state, value: [*state, value])
    assert fpstreams.explain_reduction(ordinary).to_dict() == {
        "mergeable": False,
        "combine_declared": False,
        "laws": None,
    }
    with pytest.raises(TypeError, match="collector must be a Collector"):
        fpstreams.explain_reduction(object())  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="partition_size"):
        run_partitioned_reducer([1], reducer, partition_size=0)
    with pytest.raises(TypeError, match="reducer must be a Reducer"):
        merge_reducer_states([1], ordinary)  # type: ignore[arg-type]


def test_reducer_law_declarations_reject_invalid_contracts() -> None:
    from fpstreams.planning.semantics import StateProfile

    fields = (
        fpstreams.EmptyInputPolicy.FINISH_IDENTITY,
        StateProfile.constant(),
        fpstreams.LawProvenance.USER_ASSERTED,
    )
    with pytest.raises(ValueError, match="associative=True"):
        fpstreams.ReducerLaws(False, True, False, True, *fields)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="order-sensitive"):
        fpstreams.ReducerLaws(True, True, True, True, *fields)
    with pytest.raises(TypeError, match="EmptyInputPolicy"):
        fpstreams.ReducerLaws(True, True, False, True, "invalid", *fields[1:])  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="LawProvenance"):
        fpstreams.ReducerLaws(True, True, False, True, fields[0], fields[1], "invalid")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="StateProfile"):
        fpstreams.ReducerLaws(True, True, False, True, fields[0], object(), fields[2])  # type: ignore[arg-type]

    laws = fpstreams.ReducerLaws(True, True, False, True, *fields)
    with pytest.raises(TypeError, match="merge must be callable"):
        fpstreams.Reducer(
            lambda: 0,
            lambda state, _value: state,
            merge=None,  # type: ignore[arg-type]
            laws=laws,
        )
    with pytest.raises(TypeError, match="laws must be"):
        fpstreams.Reducer(
            lambda: 0,
            lambda state, _value: state,
            merge=lambda left, _right: left,
            laws=object(),  # type: ignore[arg-type]
        )
    with pytest.raises(TypeError, match="merge must be callable"):
        fpstreams.ReducerAggregator(
            lambda: 0,
            lambda state, _value: state,
            merge=None,  # type: ignore[arg-type]
            laws=laws,
        )
    with pytest.raises(TypeError, match="laws must be"):
        fpstreams.ReducerAggregator(
            lambda: 0,
            lambda state, _value: state,
            merge=lambda left, _right: left,
            laws=object(),  # type: ignore[arg-type]
        )
