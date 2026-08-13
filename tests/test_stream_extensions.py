"""Gatherer and StreamEx-inspired extension behavior."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from typing import Any

import pytest

import fpstreams
from fpstreams import Downstream, Gatherer, flow


def test_downstream_rejection_is_monotonic() -> None:
    received: list[int] = []

    def accept_one(value: int) -> bool:
        received.append(value)
        return False

    downstream = Downstream(accept_one)

    assert downstream.is_rejecting() is False
    assert downstream.push(1) is False
    assert downstream.is_rejecting() is True
    assert downstream.push(2) is False
    assert received == [1]


def test_push_gatherer_supports_state_and_finisher() -> None:
    def integrate(state: list[int], item: int, downstream: Downstream[int]) -> bool:
        state.append(item)
        return downstream.push(item * 2)

    def finish(state: list[int], downstream: Downstream[int]) -> None:
        downstream.push(sum(state))

    gatherer = Gatherer.of_sequential(list, integrate, finisher=finish)

    assert flow([1, 2, 3]).gather(gatherer).to_list() == [2, 4, 6, 6]


def test_stateless_push_gatherer_factory() -> None:
    gatherer = Gatherer.of_sequential(lambda _state, item, downstream: downstream.push(item + 1))

    assert flow([1, 2]).gather(gatherer).to_list() == [2, 3]


def test_integrator_short_circuits_source_and_still_finishes() -> None:
    events: list[object] = []

    def source() -> Iterator[int]:
        try:
            for item in range(1, 10):
                events.append(("source", item))
                yield item
        finally:
            events.append("closed")

    def integrate(state: list[int], item: int, downstream: Downstream[int]) -> bool:
        state.append(item)
        downstream.push(item)
        return item < 3

    def finish(state: list[int], downstream: Downstream[Any]) -> None:
        events.append(("finish", tuple(state), downstream.is_rejecting()))
        downstream.push(sum(state))

    gatherer = Gatherer.of_sequential(list, integrate, finisher=finish)

    assert flow(source()).gather(gatherer).to_list() == [1, 2, 3, 6]
    assert events == [
        ("source", 1),
        ("source", 2),
        ("source", 3),
        ("finish", (1, 2, 3), False),
        "closed",
    ]


def test_and_then_feeds_left_finisher_before_right_finisher() -> None:
    def group(state: list[int], item: int, downstream: Downstream[tuple[int, ...]]) -> bool:
        state.append(item)
        if len(state) < 2:
            return True
        batch = tuple(state)
        state.clear()
        return downstream.push(batch)

    def finish_group(state: list[int], downstream: Downstream[tuple[int, ...]]) -> None:
        if state:
            downstream.push(tuple(state))

    def total(
        state: list[tuple[int, ...]],
        item: tuple[int, ...],
        downstream: Downstream[int],
    ) -> bool:
        state.append(item)
        return downstream.push(sum(item))

    def finish_total(state: list[tuple[int, ...]], downstream: Downstream[int]) -> None:
        downstream.push(len(state))

    left = Gatherer.of_sequential(list, group, finisher=finish_group)
    right = Gatherer.of_sequential(list, total, finisher=finish_total)

    assert flow([1, 2, 3, 4, 5]).gather(left.and_then(right)).to_list() == [3, 7, 5, 3]


def test_and_then_propagates_right_rejection_to_source_and_left_finisher() -> None:
    events: list[object] = []

    def source() -> Iterator[int]:
        try:
            for item in range(1, 10):
                events.append(("source", item))
                yield item
        finally:
            events.append("closed")

    def pass_through(
        state: None,
        item: int,
        downstream: Downstream[int],
    ) -> bool:
        return downstream.push(item)

    def finish_left(state: None, downstream: Downstream[int]) -> None:
        events.append(("left_finish_rejecting", downstream.is_rejecting()))

    def take_two(state: list[int], item: int, downstream: Downstream[int]) -> bool:
        state.append(item)
        downstream.push(item * 10)
        return len(state) < 2

    def finish_right(state: list[int], downstream: Downstream[int]) -> None:
        events.append(("right_finish", tuple(state), downstream.is_rejecting()))

    left = Gatherer.of_sequential(pass_through, finisher=finish_left)
    right = Gatherer.of_sequential(list, take_two, finisher=finish_right)

    assert flow(source()).gather(left.and_then(right)).to_list() == [10, 20]
    assert events == [
        ("source", 1),
        ("source", 2),
        ("left_finish_rejecting", True),
        ("right_finish", (1, 2), False),
        "closed",
    ]


def test_adjacent_gatherers_are_fused_with_composed_finisher_order() -> None:
    events: list[object] = []

    def pass_through(
        state: None,
        item: int,
        downstream: Downstream[int],
    ) -> bool:
        return downstream.push(item)

    def finish_left(state: None, downstream: Downstream[int]) -> None:
        events.append(("left", downstream.is_rejecting()))

    def stop(state: list[int], item: int, downstream: Downstream[int]) -> bool:
        state.append(item)
        downstream.push(item)
        return False

    def finish_right(state: list[int], downstream: Downstream[int]) -> None:
        events.append(("right", tuple(state), downstream.is_rejecting()))

    left = Gatherer.of_sequential(pass_through, finisher=finish_left)
    right = Gatherer.of_sequential(list, stop, finisher=finish_right)

    assert flow([1, 2, 3]).gather(left).gather(right).to_list() == [1]
    assert events == [("left", True), ("right", (1,), False)]


def test_downstream_cancellation_runs_finisher_in_rejecting_mode() -> None:
    events: list[object] = []

    def source() -> Iterator[int]:
        try:
            yield from range(10)
        finally:
            events.append("closed")

    def integrate(state: list[int], item: int, downstream: Downstream[int]) -> bool:
        state.append(item)
        return downstream.push(item)

    def finish(state: list[int], downstream: Downstream[int]) -> None:
        events.append(("finish", tuple(state), downstream.is_rejecting()))
        assert downstream.push(99) is False

    gatherer = Gatherer.of_sequential(list, integrate, finisher=finish)

    assert flow(source()).gather(gatherer).take(1).to_list() == [0]
    assert events == [("finish", (0,), True), "closed"]


def test_push_contract_rejects_non_boolean_results() -> None:
    downstream: Downstream[int] = Downstream(lambda _value: None)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="push callback must return a bool"):
        downstream.push(1)

    gatherer = Gatherer.of_sequential(
        lambda _state, _item, _downstream: None  # type: ignore[arg-type,return-value]
    )
    with pytest.raises(TypeError, match="integrator must return a bool"):
        flow([1]).gather(gatherer).to_list()


def test_take_while_inclusive_emits_the_boundary_then_closes() -> None:
    pulls: list[int] = []
    closed = False

    def values() -> Iterator[int]:
        nonlocal closed
        try:
            for value in (1, 2, 3, 4):
                pulls.append(value)
                yield value
        finally:
            closed = True

    pipeline = fpstreams.flow.defer(values).take_while_inclusive(lambda value: value < 3)

    assert pipeline.to_list() == [1, 2, 3]
    assert pulls == [1, 2, 3]
    assert closed
    assert pipeline.explain().to_dict()["operations"] == [{"name": "take_while_inclusive"}]


def test_find_index_short_circuits_and_index_of_uses_none_when_missing() -> None:
    pulls: list[int] = []
    closed = False

    def values() -> Iterator[int]:
        nonlocal closed
        try:
            for value in (10, 20, 30, 40):
                pulls.append(value)
                yield value
        finally:
            closed = True

    assert fpstreams.flow.defer(values).find_index(lambda value: value == 30) == 2
    assert pulls == [10, 20, 30]
    assert closed
    assert fpstreams.flow([10, 20, 30]).index_of(20) == 1
    assert fpstreams.flow([10, 20, 30]).index_of(99) is None


def test_cross_opens_and_caches_the_right_side_only_when_needed() -> None:
    events: list[str] = []

    def left() -> Iterator[int]:
        try:
            for value in (1, 2, 3):
                events.append(f"left:{value}")
                yield value
        finally:
            events.append("left:closed")

    def right() -> Iterator[str]:
        try:
            for value in ("a", "b"):
                events.append(f"right:{value}")
                yield value
        finally:
            events.append("right:closed")

    pipeline = fpstreams.flow.defer(left).cross(right())
    assert events == []
    assert pipeline.take(3).to_list() == [(1, "a"), (1, "b"), (2, "a")]
    assert events == [
        "left:1",
        "right:a",
        "right:b",
        "right:closed",
        "left:2",
        "left:closed",
    ]

    unopened: list[str] = []

    def unused_right() -> Iterator[int]:
        unopened.append("opened")
        yield 1

    assert fpstreams.flow([]).cartesian(unused_right()).to_list() == []
    assert unopened == []


def test_cross_enforces_the_explicit_right_cache_limit() -> None:
    with pytest.raises(fpstreams.BufferLimitError, match="max_right=2"):
        fpstreams.flow([1]).cross(range(3), max_right=2).to_list()


def test_scan_right_and_reduce_right_use_right_associative_order() -> None:
    assert fpstreams.flow([1, 2, 3]).scan_right(
        0, lambda value, total: value + total
    ).to_list() == [6, 5, 3]
    assert fpstreams.flow([1, 2, 3]).reduce_right(lambda left, right: left - right) == 2
    assert fpstreams.flow([]).reduce_right(lambda value, total: value + total, 10) == 10
    with pytest.raises(fpstreams.EmptyFlowError):
        fpstreams.flow([]).reduce_right(lambda left, right: left + right)


def test_right_operations_are_lazy_bounded_and_close_on_limit_errors() -> None:
    events: list[str] = []

    def values() -> Iterator[int]:
        try:
            for value in (1, 2, 3):
                events.append(f"pull:{value}")
                yield value
        finally:
            events.append("closed")

    pipeline = fpstreams.flow.defer(values).scan_right(
        0,
        lambda value, total: value + total,
        max_items=2,
    )
    assert events == []
    with pytest.raises(fpstreams.BufferLimitError, match="max_items=2"):
        pipeline.to_list()
    assert events == ["pull:1", "pull:2", "pull:3", "closed"]


@pytest.mark.asyncio
async def test_async_inclusive_take_and_index_search_short_circuit() -> None:
    events: list[str] = []

    async def values() -> AsyncIterator[int]:
        try:
            for value in (1, 2, 3, 4):
                events.append(f"pull:{value}")
                yield value
        finally:
            events.append("closed")

    async def before_three(value: int) -> bool:
        return value < 3

    assert await fpstreams.aflow(values()).take_while_inclusive(before_three).to_list() == [1, 2, 3]
    assert events == ["pull:1", "pull:2", "pull:3", "closed"]
    assert await fpstreams.aflow([10, 20, 30]).find_index(lambda value: value == 20) == 1
    assert await fpstreams.aflow([10, 20, 30]).index_of(99) is None


@pytest.mark.asyncio
async def test_async_cross_and_right_reductions_match_sync_semantics() -> None:
    async def right() -> AsyncIterator[str]:
        for value in ("a", "b"):
            yield value

    assert await fpstreams.aflow([1, 2]).cross(right()).to_list() == [
        (1, "a"),
        (1, "b"),
        (2, "a"),
        (2, "b"),
    ]

    async def add(value: int, total: int) -> int:
        return value + total

    assert await fpstreams.aflow([1, 2, 3]).scan_right(0, add).to_list() == [6, 5, 3]
    assert await fpstreams.aflow([1, 2, 3]).reduce_right(add, 0) == 6
    with pytest.raises(fpstreams.BufferLimitError, match="max_right=1"):
        await fpstreams.aflow([1]).cross(["a", "b"], max_right=1).to_list()


def test_native_inclusive_take_fuses_with_downstream_i64_operations() -> None:
    pipeline = (
        fpstreams.flow(range(100))
        .map(fpstreams.item * 2)
        .take_while_inclusive(fpstreams.item < 6)
        .filter(fpstreams.item % 4 == 0)
        .with_engine("native")
    )

    assert pipeline.to_list() == [0, 4]
    assert pipeline.count() == 2


def test_native_inclusive_take_fuses_in_f64_pipelines() -> None:
    result = (
        fpstreams.flow(range(10))
        .map(fpstreams.fitem / 2)
        .take_while_inclusive(fpstreams.fitem < 1.0)
        .map(fpstreams.fitem * 2)
        .with_engine("native")
        .to_list()
    )

    assert result == [0.0, 1.0, 2.0]
