"""Pairs transformations, collection, and aggregation."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest

import fpstreams
from fpstreams import flow


def _square(value: int) -> int:
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
