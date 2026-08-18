"""Synchronous Flow sources, lazy transforms, selectors, gatherers, and terminals."""

from __future__ import annotations

import json
import subprocess
import sys
import threading
import time
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import pytest

import benchmark
import fpstreams
from fpstreams import Downstream, Gatherer, NativeUnsupportedError, SelectionError, flow

# --- Tests consolidated from test_flow_api.py ---


def _square(value: int) -> int:
    return value * value


def test_flow_collects_any_iterable() -> None:
    assert flow(range(4)).to_list() == [0, 1, 2, 3]


def test_one_shot_source_fails_instead_of_silently_returning_empty() -> None:
    values = flow(iter([1, 2]))

    assert values.to_list() == [1, 2]
    with pytest.raises(Exception) as captured:
        values.to_list()

    assert type(captured.value).__name__ == "FlowConsumedError"


def test_intermediate_operations_are_lazy_and_do_not_mutate_the_parent() -> None:
    seen: list[int] = []
    base = flow([1, 2, 3]).tap(seen.append)
    doubled = base.map(lambda value: value * 2)

    assert seen == []
    assert doubled.to_list() == [2, 4, 6]
    assert seen == [1, 2, 3]
    assert base.to_list() == [1, 2, 3]


def test_creating_a_flow_does_not_open_or_prescan_a_reiterable_source() -> None:
    class CountedList(list[int]):
        opens = 0
        length_checks = 0

        def __iter__(self):
            self.opens += 1
            return super().__iter__()

        def __len__(self):
            self.length_checks += 1
            return super().__len__()

    source = CountedList([1, 2, 3])
    pipeline = flow(source).map(lambda value: value + 1).take(1)

    assert source.opens == 0
    assert source.length_checks == 0
    assert pipeline.to_list() == [2]
    assert source.opens == 1


def test_stateless_transformations_compose_in_encounter_order() -> None:
    result = (
        flow(range(6))
        .filter(lambda value: value % 2 == 0)
        .flat_map(lambda value: (value, -value))
        .reject(lambda value: value == 0)
        .to_list()
    )

    assert result == [2, -2, 4, -4]


def test_short_circuit_closes_the_upstream_iterator() -> None:
    closed = False

    def values():
        nonlocal closed
        try:
            yield from range(100)
        finally:
            closed = True

    assert flow(values()).take(2).to_list() == [0, 1]
    assert closed


def test_cleanup_error_does_not_hide_pipeline_error() -> None:
    def values():
        try:
            yield 1
        finally:
            raise RuntimeError("cleanup failed")

    def fail(_value: int) -> int:
        raise ValueError("transform failed")

    with pytest.raises(ValueError, match="transform failed") as captured:
        flow.defer(values).map(fail).to_list()

    assert any("cleanup failed" in note for note in captured.value.__notes__)


def test_cleanup_attempts_every_owned_iterator() -> None:
    import fpstreams.execution.sync_ops as sync_ops

    events: list[str] = []

    class Closeable:
        def __init__(self, name: str, *, fails: bool = False) -> None:
            self.name = name
            self.fails = fails

        def __iter__(self):
            return self

        def __next__(self):
            raise StopIteration

        def close(self) -> None:
            events.append(self.name)
            if self.fails:
                raise RuntimeError(f"{self.name} cleanup failed")

    assert hasattr(sync_ops, "close_iterators"), "batch cleanup must be available"
    with pytest.raises(RuntimeError, match="first cleanup failed"):
        sync_ops.close_iterators([Closeable("first", fails=True), Closeable("second")])

    assert events == ["first", "second"]


def test_deferred_infinite_source_can_be_evaluated_repeatedly() -> None:
    powers = flow.iterate(1, lambda value: value * 2).take(5)

    assert powers.to_list() == [1, 2, 4, 8, 16]
    assert powers.to_list() == [1, 2, 4, 8, 16]


def test_stream_class_factories_remain_useful_v2_entry_points() -> None:
    calls = 0

    def supply() -> int:
        nonlocal calls
        calls += 1
        return calls

    assert fpstreams.Stream.iterate(1, lambda value: value * 2).take(4).to_list() == [
        1,
        2,
        4,
        8,
    ]
    assert fpstreams.Stream.generate(supply).take(3).to_list() == [1, 2, 3]
    assert fpstreams.Flow.of_nullable(None).to_list() == []
    assert fpstreams.Flow.of_nullable(4).to_list() == [4]


def test_reiterable_flow_supports_pythonic_terminals() -> None:
    values = flow([1, 2, 3, 4])

    assert values.first() == 1
    assert values.last() == 4
    assert values.count() == 4
    assert values.sum() == 10
    assert values.min() == 1
    assert values.max() == 4
    assert values.any(lambda value: value > 3)
    assert values.all(lambda value: value > 0)
    assert values.none(lambda value: value < 0)
    assert values.reduce(lambda left, right: left + right) == 10
    assert flow([]).reduce(lambda left, right: left + right, 10) == 10

    def user_error(_left: int, _right: int) -> int:
        raise TypeError("empty iterable is domain data")

    with pytest.raises(TypeError, match="domain data"):
        flow([1, 2]).reduce(user_error)


def test_prefix_operations_preserve_boundaries() -> None:
    result = (
        flow(range(10))
        .drop(2)
        .drop_while(lambda value: value < 4)
        .take_while(lambda value: value < 8)
        .to_list()
    )

    assert result == [4, 5, 6, 7]
    with pytest.raises(ValueError):
        flow([1]).drop(-1)


def test_selectors_resolve_paths_and_fail_loudly_when_missing() -> None:
    records = [
        {"user": {"name": "Ada"}},
        {"user": {"name": "Grace"}},
    ]

    assert flow(records).pluck("user.name").to_list() == ["Ada", "Grace"]
    with pytest.raises(SelectionError, match=r"user\.email"):
        flow(records).pluck("user.email").to_list()


def test_unique_is_stable_for_hashable_and_unhashable_values() -> None:
    values = [1, 1, [2], [2], 3, 1]

    assert flow(values).unique().to_list() == [1, [2], 3]


def test_structural_operations_stream_immutable_groups() -> None:
    assert flow(range(5)).chunk(2).to_list() == [(0, 1), (2, 3), (4,)]
    assert flow(range(4)).window(3).to_list() == [(0, 1, 2), (1, 2, 3)]
    assert flow([1, 4, 9]).pairwise().to_list() == [(1, 4), (4, 9)]
    assert flow([1, 2, 3]).scan(0, lambda total, value: total + value).to_list() == [
        1,
        3,
        6,
    ]


def test_latest_jdk_gatherer_semantics_and_streamex_conveniences() -> None:
    initializer_calls = 0

    def initialize() -> list[int]:
        nonlocal initializer_calls
        initializer_calls += 1
        return []

    folded = flow([1, 2, 3]).fold(
        initialize,
        lambda values, item: [*values, item],
    )

    assert flow([1, 2]).window(3).to_list() == [(1, 2)]
    assert flow([]).window(3).to_list() == []
    assert folded.map(tuple).to_list() == [(1, 2, 3)]
    assert folded.map(tuple).to_list() == [(1, 2, 3)]
    assert initializer_calls == 2
    assert flow([]).fold(list, lambda values, item: [*values, item]).to_list() == [[]]
    assert flow([1, 2, 3, 4]).filter_map(
        lambda item: str(item) if item % 2 == 0 else None
    ).to_list() == ["2", "4"]
    assert flow([1, 4, 9]).pair_map(lambda left, right: right - left).to_list() == [
        3,
        5,
    ]
    assert flow([1, 1, 2, 4, 3, 3]).group_runs(lambda item: item % 2).to_list() == [
        (1, 1),
        (2, 4),
        (3, 3),
    ]


def test_bounded_selection_and_minmax_avoid_full_sorting_api_noise() -> None:
    records = [
        {"id": "a", "score": 9},
        {"id": "b", "score": 4},
        {"id": "c", "score": 9},
        {"id": "d", "score": 7},
    ]

    assert flow(records).top(2, key="score") == [records[0], records[2]]
    assert flow(records).bottom(2, key="score") == [records[1], records[3]]
    assert flow([3, 1, 4, 2]).minmax() == (1, 4)
    assert flow(records).minmax(key="score") == (records[1], records[0])
    assert flow(records).top(0, key="score") == []
    with pytest.raises(fpstreams.EmptyFlowError):
        flow([]).minmax()
    with pytest.raises(ValueError):
        flow([1]).bottom(-1)


def test_size_constrained_batches_are_lazy_and_enforce_both_limits() -> None:
    opened = False

    def payloads():
        nonlocal opened
        opened = True
        yield from (b"12345", b"123", b"12345678", b"1", b"1", b"12", b"1")

    batches = flow.defer(payloads).batch_by_size(10, max_count=2)

    assert not opened
    assert batches.to_list() == [
        (b"12345", b"123"),
        (b"12345678", b"1"),
        (b"1", b"12"),
        (b"1",),
    ]
    assert opened
    assert flow(["oversized"]).batch_by_size(3, strict=False).to_list() == [("oversized",)]
    with pytest.raises(ValueError, match="exceeds max_size"):
        flow(["oversized"]).batch_by_size(3).to_list()
    with pytest.raises(ValueError, match="non-negative"):
        flow([1]).batch_by_size(3, get_size=lambda _item: -1).to_list()


def test_reduce_by_keeps_only_an_independent_accumulator_per_key() -> None:
    initializer_calls = 0

    def initialize() -> list[int]:
        nonlocal initializer_calls
        initializer_calls += 1
        return []

    def append(values: list[int], item: int) -> list[int]:
        values.append(item)
        return values

    grouped = flow(range(6)).reduce_by(
        lambda value: value % 2,
        append,
        initializer=initialize,
    )

    assert grouped == {0: [0, 2, 4], 1: [1, 3, 5]}
    assert initializer_calls == 2
    assert grouped[0] is not grouped[1]
    assert flow("abaca").frequencies() == {"a": 3, "b": 1, "c": 1}


def test_pythonic_combinators_cover_common_stream_bookkeeping() -> None:
    values = flow([10, None, 20]).compact()

    assert values.enumerate(start=1).to_list() == [(1, 10), (2, 20)]
    assert values.intersperse(0).to_list() == [10, 0, 20]
    assert values.zip("ab", strict=True).to_list() == [(10, "a"), (20, "b")]
    assert flow([1]).zip_longest([2, 3], fillvalue=0).to_list() == [
        (1, 2),
        (0, 3),
    ]
    assert flow([1]).concat([2, 3], (4,)).to_list() == [1, 2, 3, 4]


def test_one_shot_zip_source_never_silently_turns_empty() -> None:
    values = flow([1, 2]).zip(iter([3, 4]))

    assert values.to_list() == [(1, 3), (2, 4)]
    with pytest.raises(fpstreams.FlowConsumedError):
        values.to_list()


def test_selector_driven_unique_and_sort_are_concise() -> None:
    records = [
        {"id": 1, "score": 8},
        {"id": 2, "score": 5},
        {"id": 1, "score": 99},
    ]

    assert flow(records).unique_by("id").sort_by("score", reverse=True).pluck("id").to_list() == [
        1,
        2,
    ]


def test_custom_gatherer_emits_zero_or_more_values() -> None:
    def integrate(state: list[int], item: int):
        state.append(item)
        if len(state) < 2:
            return ()
        group = tuple(state)
        state.clear()
        return (group,)

    gatherer = fpstreams.Gatherer(
        initializer=list,
        integrator=integrate,
        finisher=lambda state: (tuple(state),) if state else (),
    )

    assert flow([1, 2, 3]).gather(gatherer).to_list() == [(1, 2), (3,)]


def test_stream_ex_inspired_operations_avoid_manual_bookkeeping() -> None:
    collapsed = (
        flow([1, 1, 2, 3, 3])
        .collapse(lambda left, right: left == right, lambda left, right: left + right)
        .prepend(0)
        .append(9)
        .to_list()
    )
    ends = (
        flow([1, 2, 3])
        .map_first(lambda value: value * 10)
        .map_last(lambda value: value * 100)
        .to_list()
    )

    assert collapsed == [0, 2, 2, 6, 9]
    assert ends == [10, 2, 300]


def test_explain_reports_fusion_and_forced_engine_rejections() -> None:
    explanation = flow(range(10)).map(str).filter(bool).take(2).explain().to_dict()

    assert explanation["source"] == {
        "reiterable": True,
        "exact_size": 10,
        "ordered": True,
    }
    assert [operation["name"] for operation in explanation["operations"]] == [
        "map",
        "filter",
        "take",
    ]
    assert explanation["stages"][0] == {
        "engine": "python",
        "operations": ["map", "filter"],
        "fused": True,
    }
    with pytest.raises(NativeUnsupportedError, match="map"):
        flow([1, 2]).map(lambda value: value + 1).with_engine("native").to_list()


def _semantic_output(pipeline: fpstreams.Flow[object]) -> dict[str, object]:
    return pipeline.explain().to_dict()["semantics"]["output"]


@pytest.mark.parametrize(
    ("pipeline", "termination", "cardinality", "value"),
    [
        (flow([]).filter(bool), "proven_finite", "exact", 0),
        (flow([]).flat_map(lambda value: [value]), "proven_finite", "exact", 0),
        (flow([1, 2, 3]).take(0), "proven_finite", "exact", 0),
        (flow([1, 2, 3]).drop(1), "proven_finite", "exact", 2),
        (flow([1, 2, 3]).filter(bool).drop(1), "proven_finite", "upper_bound", 2),
        (flow([1, 2, 3]).filter(bool).pairwise(), "proven_finite", "upper_bound", 2),
        (flow([1, 2, 3]).filter(bool).chunk(2), "proven_finite", "upper_bound", 2),
        (flow(range(5)).chunk(2), "proven_finite", "exact", 3),
        (flow(range(4)).window(3, step=2), "proven_finite", "exact", 1),
        (flow([1]).window(3), "proven_finite", "exact", 1),
        (flow([]).window(3), "proven_finite", "exact", 0),
        (flow(iter([1, 2])).window(2), "unknown", "unknown", None),
        (flow([1]).concat([2, 3]), "proven_finite", "exact", 3),
        (flow.iterate(0, lambda value: value + 1).concat([1]), "proven_infinite", "unknown", None),
        (flow(iter([1])).concat([2]), "unknown", "unknown", None),
        (flow([1, 2, 3]).zip([4, 5]), "proven_finite", "exact", 2),
        (
            flow([1, 2, 3]).filter(bool).zip([4, 5]),
            "proven_finite",
            "upper_bound",
            2,
        ),
        (
            flow.iterate(0, lambda value: value + 1).zip([4, 5]),
            "proven_finite",
            "unknown",
            None,
        ),
        (flow([1]).zip_longest([2, 3]), "proven_finite", "unknown", None),
        (
            flow.iterate(0, lambda value: value + 1).zip_longest([1]),
            "proven_infinite",
            "unknown",
            None,
        ),
        (flow([]).cross(iter([1])), "proven_finite", "exact", 0),
        (flow([1, 2]).cross(range(3)), "proven_finite", "exact", 6),
        (flow([1, 2]).cross(iter([3])), "unknown", "unknown", None),
    ],
)
def test_explain_propagates_cardinality_and_termination(
    pipeline: fpstreams.Flow[object],
    termination: str,
    cardinality: str,
    value: int | None,
) -> None:
    output = _semantic_output(pipeline)

    assert output["termination"] == termination
    assert output["cardinality"] == {"kind": cardinality, "value": value}


def test_explain_reports_order_state_and_completion_risks() -> None:
    unordered = flow({1, 2}).scan(0, lambda total, value: total + value).explain("list").to_dict()
    infinite = flow.iterate(0, lambda value: value + 1).sorted().explain("list").to_dict()
    unknown = flow.defer(lambda: iter([2, 1])).sorted().explain("list").to_dict()

    assert {item["code"] for item in unordered["diagnostics"]} == {"ORDER_NOT_PRESERVED"}
    assert [item["code"] for item in infinite["diagnostics"]] == [
        "STATE_MAY_GROW",
        "NON_TERMINATING_PLAN",
        "NON_TERMINATING_PLAN",
    ]
    assert [item["code"] for item in unknown["diagnostics"]] == [
        "STATE_MAY_GROW",
        "COMPLETION_NOT_PROVEN",
        "COMPLETION_NOT_PROVEN",
    ]
    assert flow([1]).explain("first").to_dict()["semantics"]["completion"] == (
        "first_item_or_source_end"
    )
    assert flow([1]).explain("all").to_dict()["semantics"]["completion"] == (
        "witness_or_source_end"
    )
    assert flow([1]).explain().to_dict()["semantics"]["completion"] == "consumer_stop"


def test_attempt_turns_exceptions_into_composable_values() -> None:
    results = flow([2, 0]).attempt(lambda value: 10 // value).to_list()

    assert results[0] == fpstreams.Ok(5)
    assert results[0].map(lambda value: value * 2) == fpstreams.Ok(10)
    assert isinstance(results[1], fpstreams.Err)
    assert isinstance(results[1].error, ZeroDivisionError)


def test_stream_is_a_thin_v2_compatibility_alias() -> None:
    assert fpstreams.Stream is fpstreams.Flow
    assert fpstreams.ParallelStream is fpstreams.Flow
    assert fpstreams.Stream([1, 1, 2, 3]).distinct().skip(1).limit(2).to_list() == [2, 3]
    assert fpstreams.ParallelStream.of(1, 2, 3).map(lambda value: value + 1).to_list() == [
        2,
        3,
        4,
    ]


# --- Tests consolidated from test_stream_extensions.py ---

"""Gatherer contracts and synchronous, asynchronous, and native stream extensions."""


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


# --- Tests consolidated from test_execution_engines.py ---

"""Parallel mapping, engine planning, fused terminals, source metadata, and external sorting."""


def _engine_square(value: int) -> int:
    return value * value


def test_parallel_map_is_a_bounded_ordered_flow_operation() -> None:
    active = 0
    peak = 0
    lock = threading.Lock()

    def work(value: int) -> int:
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        time.sleep((5 - value) * 0.002)
        with lock:
            active -= 1
        return value * value

    values = flow([1, 2, 3, 4]).map_parallel(work, workers=2, buffer=2, ordered=True)

    assert values.to_list() == [1, 4, 9, 16]
    assert peak == 2
    assert values.explain().to_dict()["stages"] == [
        {"engine": "thread", "operations": ["map_parallel"], "fused": False}
    ]


def test_parallel_is_an_immutable_strategy_for_following_maps() -> None:
    base = flow(range(6)).parallel(workers=2, backend="thread", buffer=2)
    parallel = base.map(lambda value: value * 2)
    sequential = base.sequential().map(lambda value: value * 2)

    assert parallel.to_list() == sequential.to_list() == [0, 2, 4, 6, 8, 10]
    assert parallel.explain().to_dict()["stages"][0]["engine"] == "thread"
    assert sequential.explain().to_dict()["stages"][0]["engine"] == "python"


def test_parallel_default_process_backend_executes_picklable_work() -> None:
    assert flow(range(6)).parallel(workers=2, buffer=2).map(_engine_square).to_list() == [
        0,
        1,
        4,
        9,
        16,
        25,
    ]


def test_native_expression_pipeline_matches_python_and_is_auto_planned() -> None:
    pipeline = (
        flow(range(10_000)).map(fpstreams.item * 3 + 1).filter(fpstreams.item % 2 == 0).take(4)
    )

    expected = [4, 10, 16, 22]
    assert pipeline.with_engine("python").to_list() == expected
    assert pipeline.with_engine("native").to_list() == expected
    assert pipeline.to_list() == expected

    explanation = pipeline.explain().to_dict()
    assert explanation["selected_engine"] == "native"
    assert explanation["stages"] == [
        {
            "engine": "native",
            "operations": ["map", "filter", "take"],
            "fused": True,
        }
    ]


def test_auto_planner_keeps_tiny_list_short_circuits_in_python() -> None:
    pipeline = flow(list(range(10_000))).map(fpstreams.item + 1).take(1)
    explanation = pipeline.explain().to_dict()

    assert pipeline.to_list() == [1]
    assert explanation["selected_engine"] == "python"
    assert "copy" in explanation["selection_reason"]
    assert pipeline.with_engine("native").to_list() == [1]


def test_auto_native_never_breaks_streaming_iteration(monkeypatch) -> None:
    from fpstreams import _native

    original = _native.execute_i64_range
    calls = 0

    def tracked(*args):
        nonlocal calls
        calls += 1
        return original(*args)

    monkeypatch.setattr(_native, "execute_i64_range", tracked)
    pipeline = flow(range(10)).map(fpstreams.item + 1)
    explanation = pipeline.explain().to_dict()
    iterator = iter(pipeline)

    assert explanation["streaming_engine"] == "python"
    assert explanation["materializing_engine"] == "native"
    assert next(iterator) == 1
    iterator.close()
    assert calls == 0
    assert pipeline.to_list() == list(range(1, 11))
    assert calls == 1


def test_auto_native_conversion_falls_back_without_changing_semantics() -> None:
    values = [1, 2.5, *range(3, 10)]
    pipeline = flow(values).map(fpstreams.item + 1)

    assert pipeline.explain().to_dict()["selected_engine"] == "native"
    assert pipeline.to_list() == [value + 1 for value in values]
    with pytest.raises(fpstreams.NativeUnsupportedError):
        pipeline.with_engine("native").to_list()


def test_native_kernel_preserves_python_integer_pipeline_semantics() -> None:
    pipeline = (
        flow(list(range(-20, 21)))
        .map((fpstreams.item - 1) // -3)
        .reject(fpstreams.item % -4 == 0)
        .drop(2)
        .take(7)
    )

    assert pipeline.with_engine("native").to_list() == pipeline.with_engine("python").to_list()
    native = pipeline.with_engine("native")
    python = pipeline.with_engine("python")
    assert (native.count(), native.sum(), native.min(), native.max()) == (
        python.count(),
        python.sum(),
        python.min(),
        python.max(),
    )

    overflowing = flow(range(5000)).map((fpstreams.item + 2**62) * 4).take(1)
    assert overflowing.to_list() == [2**64]
    assert overflowing.explain().to_dict()["selected_engine"] == "native"


def test_native_expressions_compose_boolean_conditions_and_abs() -> None:
    condition = ((fpstreams.item >= -5) & (fpstreams.item < 8)) | (fpstreams.item == 20)
    pipeline = flow(range(-30, 30)).filter(condition & ~(abs(fpstreams.item) == 3))

    assert pipeline.with_engine("native").to_list() == pipeline.with_engine("python").to_list()


def test_deep_scalar_expressions_compile_without_python_recursion() -> None:
    integer_expression = fpstreams.item
    float_expression = fpstreams.fitem
    for _ in range(2_000):
        integer_expression = integer_expression + 1
        float_expression = float_expression + 0.5

    assert integer_expression(3) == 2_003
    assert float_expression(3.0) == pytest.approx(1_003.0)
    assert len(integer_expression.native_instructions()) == 4_001
    assert len(float_expression.native_instructions()) == 4_001


def test_structurally_equal_scalar_expressions_share_compiled_evaluators() -> None:
    from fpstreams.expressions.scalar import (
        _compile_float_evaluator,
        _compile_int_evaluator,
    )

    _compile_int_evaluator.cache_clear()
    _compile_float_evaluator.cache_clear()

    assert ((fpstreams.item + 2) * 3)(4) == 18
    assert ((fpstreams.item + 2) * 3)(5) == 21
    assert ((fpstreams.fitem + 2.0) * 3.0)(4.0) == pytest.approx(18.0)
    assert ((fpstreams.fitem + 2.0) * 3.0)(5.0) == pytest.approx(21.0)

    assert _compile_int_evaluator.cache_info().misses == 1
    assert _compile_int_evaluator.cache_info().hits == 1
    assert _compile_float_evaluator.cache_info().misses == 1
    assert _compile_float_evaluator.cache_info().hits == 1


def test_python_executor_unwraps_compiled_expression_once(monkeypatch) -> None:
    from fpstreams.expressions.scalar import Expr

    expression = fpstreams.item * 3 + 1
    predicate = fpstreams.item % 2 == 0

    def reject_per_item_dispatch(_expression: Expr, _item: int) -> int:
        raise AssertionError("Expr.__call__ should not run inside the fused loop")

    monkeypatch.setattr(Expr, "__call__", reject_per_item_dispatch)
    result = (
        fpstreams.flow(range(10)).map(expression).filter(predicate).with_engine("python").to_list()
    )

    assert result == [4, 10, 16, 22, 28]


def test_hybrid_native_prefix_analysis_does_not_recompile_each_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fpstreams.planning import native

    pipeline = fpstreams.flow(range(100))
    for _ in range(30):
        pipeline = pipeline.map(fpstreams.item + 1)
    for _ in range(30):
        pipeline = pipeline.map(str)

    compile_calls = 0
    original_compile = native._compile

    def tracked_compile(plan):
        nonlocal compile_calls
        compile_calls += 1
        return original_compile(plan)

    monkeypatch.setattr(native, "_compile", tracked_compile)
    program, prefix_length = native._longest_native_prefix(pipeline._plan)

    assert program is not None
    assert prefix_length == 30
    assert compile_calls <= 1


def test_extension_capability_cache_is_reused_but_tracks_module_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fpstreams import _native as _installed_native
    from fpstreams.planning import native

    assert _installed_native is not None

    required = {
        "execute_i64",
        "execute_i64_range",
        "terminal_i64",
        "terminal_i64_range",
        "statistics_i64",
        "statistics_i64_range",
        "aggregate_i64",
        "aggregate_i64_range",
    }

    class Extension:
        def __init__(self) -> None:
            self.lookups = 0

        def __getattr__(self, name: str):
            if name in required:
                self.lookups += 1
                return lambda: None
            raise AttributeError(name)

    first = Extension()
    monkeypatch.setattr(fpstreams, "_native", first)
    assert native._extension_available("i64")
    first_pass_lookups = first.lookups
    assert native._extension_available("i64")
    assert first.lookups == first_pass_lookups

    replacement = Extension()
    monkeypatch.setattr(fpstreams, "_native", replacement)
    assert native._extension_available("i64")
    assert replacement.lookups == first_pass_lookups


def test_native_distinct_is_stable_fused_and_available_to_terminals() -> None:
    pipeline = (
        flow([8, 3, 8, 5, 3, 2, 5, 9, 2, 9, 1, 8])
        .map(fpstreams.item % 5)
        .unique()
        .filter(fpstreams.item > 0)
    )

    native = pipeline.with_engine("native")
    python = pipeline.with_engine("python")
    assert native.to_list() == python.to_list() == [3, 2, 4, 1]
    assert (native.count(), native.sum(), native.min(), native.max()) == (4, 10, 1, 4)
    assert native.explain().to_dict()["selected_engine"] == "native"

    float_distinct = flow([1.0, 1.0, 2.0] * 4).map(fpstreams.fitem + 0.0).unique()
    assert float_distinct.to_list() == [1.0, 2.0]
    assert float_distinct.explain().to_dict()["selected_engine"] == "hybrid"
    with pytest.raises(fpstreams.NativeUnsupportedError, match="f64 distinct"):
        float_distinct.with_engine("native").to_list()


def test_native_while_stages_short_circuit_i64_and_f64_pipelines() -> None:
    integers = (
        flow(range(100_000))
        .drop_while(fpstreams.item < 100)
        .take_while(fpstreams.item < 110)
        .map(fpstreams.item * 2)
    )
    native_integers = integers.with_engine("native")
    python_integers = integers.with_engine("python")
    assert native_integers.to_list() == python_integers.to_list() == list(range(200, 220, 2))
    assert (native_integers.count(), native_integers.sum()) == (10, 2090)
    assert native_integers.explain().to_dict()["selected_engine"] == "native"

    floats = (
        flow([value / 2 for value in range(40)])
        .drop_while(fpstreams.fitem < 2.0)
        .take_while(fpstreams.fitem < 5.0)
        .map(fpstreams.fitem * 1.5)
    )
    assert floats.with_engine("native").to_list() == pytest.approx(
        floats.with_engine("python").to_list()
    )
    assert floats.with_engine("native").sum() == pytest.approx(floats.with_engine("python").sum())

    filter_only = flow(range(10)).take_while(fpstreams.fitem < 4.0)
    assert filter_only.to_list() == [0, 1, 2, 3]
    with pytest.raises(fpstreams.NativeUnsupportedError, match="float source"):
        filter_only.with_engine("native").to_list()


def test_native_short_circuit_terminals_do_not_evaluate_the_tail() -> None:
    guarded = flow([1, 2**62]).map(fpstreams.item * 4).with_engine("native")

    assert guarded.first() == 4
    assert guarded.any()
    assert guarded.any(fpstreams.item == 4)
    assert not guarded.all(fpstreams.item < 0)
    assert not guarded.none(fpstreams.item == 4)

    false_first = flow([0, 2**62]).map(fpstreams.item * 4).with_engine("native")
    assert not false_first.all()

    complete = flow(range(10)).map(fpstreams.item * 2).filter(fpstreams.item > 4)
    assert complete.with_engine("native").first() == 6
    assert complete.with_engine("native").last() == 18

    float_guarded = (
        flow([1.0, 2.0]).map(fpstreams.fitem / (fpstreams.fitem - 2.0)).with_engine("native")
    )
    assert float_guarded.first() == pytest.approx(-1.0)

    empty = flow(range(10)).filter(fpstreams.item < 0).with_engine("native")
    assert empty.first("missing") == "missing"
    assert empty.last("missing") == "missing"
    assert not empty.any()
    assert empty.all()


def test_flow_statistics_fuse_i64_and_f64_pipelines_with_stable_means() -> None:
    integers = flow(range(1, 5)).map(fpstreams.item + 0)
    native_integers = integers.with_engine("native")
    python_integers = integers.with_engine("python")

    assert native_integers.mean() == python_integers.mean() == 2.5
    assert native_integers.average() == 2.5
    assert native_integers.variance() == pytest.approx(5 / 3)
    assert native_integers.variance(ddof=0) == 1.25
    assert native_integers.std() == pytest.approx((5 / 3) ** 0.5)

    floats = flow([1.0, 2.0, 3.0, 4.0]).map(fpstreams.fitem + 0.0)
    assert floats.with_engine("native").mean() == 2.5
    assert floats.with_engine("native").variance() == pytest.approx(5 / 3)
    cancellation = flow([1e16, 1.0, -1e16]).map(fpstreams.fitem + 0.0)
    assert cancellation.with_engine("native").mean() == pytest.approx(1 / 3)
    assert cancellation.with_engine("python").mean() == pytest.approx(1 / 3)

    empty = flow(range(3)).filter(fpstreams.item < 0).with_engine("native")
    assert empty.mean() is None
    assert empty.variance() is None
    assert flow([1]).variance() is None
    with pytest.raises(ValueError, match="ddof"):
        integers.std(ddof=-1)


def test_find_and_nth_are_concise_short_circuit_terminals() -> None:
    values = flow(range(10)).map(fpstreams.item * 2).with_engine("native")
    guarded = flow([1, 2**62]).map(fpstreams.item * 4).with_engine("native")

    assert guarded.find(fpstreams.item == 4) == 4
    assert guarded.nth(0) == 4
    assert values.find(fpstreams.item >= 12) == 12
    assert values.nth(3) == 6
    assert values.nth(-1) == 18
    assert values.nth(-2) == 16
    assert values.find(fpstreams.item > 100, "missing") == "missing"
    assert values.nth(100, "missing") == "missing"
    with pytest.raises(fpstreams.EmptyFlowError, match="find"):
        values.find(fpstreams.item > 100)
    with pytest.raises(fpstreams.EmptyFlowError, match="nth"):
        values.nth(-100)


def test_native_float_expressions_fuse_data_pipelines_and_terminals() -> None:
    values = [value / 2 for value in range(-20, 21)]
    condition = (fpstreams.fitem >= -4.0) & (fpstreams.fitem < 7.0)
    pipeline = (
        flow(values)
        .map(fpstreams.fitem * 1.25 + 0.5)
        .filter(condition & ~(abs(fpstreams.fitem) < 0.01))
        .drop(1)
        .take(12)
    )

    native = pipeline.with_engine("native")
    python = pipeline.with_engine("python")
    assert native.to_list() == pytest.approx(python.to_list())
    assert native.count() == python.count()
    assert native.sum() == pytest.approx(python.sum())
    assert native.min() == pytest.approx(python.min())
    assert native.max() == pytest.approx(python.max())
    assert flow(range(4)).map(fpstreams.fitem / 2).with_engine("native").to_list() == [
        0.0,
        0.5,
        1.0,
        1.5,
    ]
    with pytest.raises(ZeroDivisionError):
        flow([1.0] * 10).map(fpstreams.fitem / 0).with_engine("native").to_list()

    cancellation = flow([1e16, 1.0, -1e16]).map(fpstreams.fitem + 0.0)
    assert cancellation.with_engine("native").sum() == cancellation.with_engine("python").sum()
    assert cancellation.with_engine("native").sum(10**16) == cancellation.with_engine("python").sum(
        10**16
    )

    large_integer = 2**53 + 1
    precision = (
        flow([large_integer] * 10)
        .map(fpstreams.fitem + 0.0)
        .filter(fpstreams.fitem == large_integer)
    )
    assert precision.with_engine("native").to_list() == precision.with_engine("python").to_list()

    filter_only = flow(range(10)).filter(fpstreams.fitem > 4.0)
    assert filter_only.to_list() == [5, 6, 7, 8, 9]
    with pytest.raises(fpstreams.NativeUnsupportedError, match="float source"):
        filter_only.with_engine("native").to_list()


def test_hybrid_materializers_run_native_prefixes_without_changing_iteration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fpstreams import _native

    original = _native.execute_i64_range
    calls = 0

    def tracked(*args: object) -> list[int]:
        nonlocal calls
        calls += 1
        return original(*args)

    monkeypatch.setattr(_native, "execute_i64_range", tracked)
    pipeline = (
        fpstreams.flow(range(20)).map(fpstreams.item * 2).map(lambda value: f"v{value}").chunk(3)
    )
    explanation = pipeline.explain().to_dict()

    assert explanation["selected_engine"] == "hybrid"
    assert explanation["streaming_engine"] == "python"
    assert explanation["stages"] == [
        {"engine": "native", "operations": ["map"], "fused": False},
        {"engine": "python", "operations": ["map"], "fused": False},
        {"engine": "python", "operations": ["chunk"], "fused": False},
    ]

    iterator = iter(pipeline)
    assert next(iterator) == ("v0", "v2", "v4")
    iterator.close()
    assert calls == 0
    assert pipeline.to_list()[-1] == ("v36", "v38")
    assert calls == 1


def test_hybrid_planner_preserves_short_circuit_and_external_sort_costs(
    tmp_path: Path,
) -> None:
    bounded = fpstreams.flow(range(10_000)).map(fpstreams.item + 1).map(str).take(1)
    external = (
        fpstreams.flow(range(100, 0, -1))
        .map(fpstreams.item + 1)
        .external_sort(buffer_size=4, tempdir=tmp_path)
    )
    native_bounded = fpstreams.flow(range(10_000)).take(4).map(str)

    assert bounded.explain().to_dict()["selected_engine"] == "python"
    assert bounded.to_list() == ["1"]
    assert external.explain().to_dict()["selected_engine"] == "python"
    assert external.take(2).to_list() == [2, 3]
    assert list(tmp_path.iterdir()) == []
    assert native_bounded.explain().to_dict()["selected_engine"] == "hybrid"
    assert native_bounded.to_list() == ["0", "1", "2", "3"]


def test_hybrid_conversion_failure_restarts_the_whole_plan_in_python() -> None:
    values = [1, 2.5, *range(3, 12)]
    pipeline = fpstreams.flow(values).map(fpstreams.item + 1).map(str)

    assert pipeline.explain().to_dict()["selected_engine"] == "hybrid"
    assert pipeline.to_list() == [str(value + 1) for value in values]
    with pytest.raises(fpstreams.NativeUnsupportedError, match="map"):
        pipeline.with_engine("native").to_list()


@pytest.mark.parametrize(
    "pipeline",
    [
        fpstreams.flow(range(40)).map(fpstreams.item * 3).map(lambda value: -value).sorted(),
        fpstreams.flow([1.0, 1.0, 2.0, 3.0] * 4).map(fpstreams.fitem / 2).unique(),
        fpstreams.flow([4, 2, 4, 1, 2] * 4).unique().map(lambda value: f"n={value}"),
        fpstreams.flow(range(20)).map(fpstreams.item + 1).map(fpstreams.fitem / 2),
    ],
)
def test_hybrid_pipelines_match_the_python_engine(pipeline: fpstreams.Flow[object]) -> None:
    assert pipeline.explain().to_dict()["selected_engine"] == "hybrid"
    assert pipeline.to_list() == pipeline.with_engine("python").to_list()


def test_direct_range_uses_native_terminals_without_a_synthetic_map() -> None:
    values = fpstreams.flow(range(1, 6)).with_engine("native")

    assert values.count() == 5
    assert values.sum() == 15
    assert values.min() == 1
    assert values.max() == 5
    assert values.mean() == 3.0
    assert values.aggregate(
        count=fpstreams.agg.count(),
        total=fpstreams.agg.sum(),
        first=fpstreams.agg.first(),
        last=fpstreams.agg.last(),
        variance=fpstreams.agg.variance(ddof=0),
    ) == {"count": 5, "total": 15, "first": 1, "last": 5, "variance": 2.0}


@pytest.mark.parametrize("source", [list(range(32)), tuple(range(32))])
@pytest.mark.parametrize("terminal", ["list", "count", "sum", "statistics"])
def test_identity_container_auto_terminals_avoid_native_copy(
    source: list[int] | tuple[int, ...], terminal: str
) -> None:
    explanation = fpstreams.flow(source).explain(terminal).to_dict()

    assert explanation["terminal"] == terminal
    assert explanation["selected_engine"] == "python"
    assert explanation["data_movement"] == {
        "scans_source": False,
        "copies_source": False,
        "materializes": terminal == "list",
    }


def test_terminal_explain_matches_forced_native_and_range_execution() -> None:
    forced = fpstreams.flow([1, 2, 3]).with_engine("native").explain("sum").to_dict()
    ranged = fpstreams.flow(range(1, 33)).explain("sum").to_dict()

    assert forced["selected_engine"] == "native"
    assert forced["data_movement"] == {
        "scans_source": True,
        "copies_source": True,
        "materializes": False,
    }
    assert ranged["selected_engine"] == "native"
    assert ranged["data_movement"] == {
        "scans_source": False,
        "copies_source": False,
        "materializes": False,
    }
    assert ranged["complexity"] == "O(n)"


def test_exact_size_count_does_not_open_an_identity_source() -> None:
    from fpstreams.planning.source import Source, SourceCapabilities
    from fpstreams.planning.sync import Plan

    def fail_if_opened() -> Iterator[int]:
        raise AssertionError("exact-size source was opened")
        yield

    source = Source(
        fail_if_opened,
        SourceCapabilities(reiterable=True, exact_size=7),
    )

    assert fpstreams.Flow(Plan(source)).count() == 7


def test_cardinality_changing_plan_does_not_use_source_exact_size() -> None:
    opened = 0

    def values() -> Iterator[int]:
        nonlocal opened
        opened += 1
        yield from range(7)

    from fpstreams.planning.source import Source, SourceCapabilities
    from fpstreams.planning.sync import Plan

    source = Source(values, SourceCapabilities(reiterable=True, exact_size=7))
    pipeline = fpstreams.Flow(Plan(source)).filter(lambda value: value % 2 == 0)

    assert pipeline.count() == 4
    assert opened == 1


def test_direct_homogeneous_numeric_sequences_infer_the_native_kind() -> None:
    assert fpstreams.flow([1, 2, 3]).with_engine("native").aggregate(
        total=fpstreams.agg.sum(), mean=fpstreams.agg.mean()
    ) == {"total": 6, "mean": 2.0}
    assert fpstreams.flow((1.5, 2.5, 3.5)).with_engine("native").aggregate(
        total=fpstreams.agg.sum(), mean=fpstreams.agg.mean()
    ) == {"total": pytest.approx(7.5), "mean": pytest.approx(2.5)}


def test_native_adapter_covers_float_range_and_integer_list_terminals(monkeypatch) -> None:
    from fpstreams.execution import native
    from fpstreams.planning.native import NativeProgram

    float_range = NativeProgram(
        range(1, 4),
        ((0, (fpstreams.fitem + 0.5).native_instructions()),),
        "f64",
    )
    integer_list = NativeProgram([1, 2, 3], (), "i64")

    assert native.execute_terminal(float_range, "count") == 3
    assert native.execute_terminal(float_range, "sum") == pytest.approx(7.5)
    assert native.execute_statistics(float_range)[0] == 3
    assert native.execute_aggregate(float_range)[0] == 3
    assert native.execute_statistics(integer_list)[0] == 3

    monkeypatch.setattr(native.sys, "version_info", (3, 11))
    float_list = NativeProgram([1.0, 2.0, 3.0], (), "f64")
    assert native.execute_terminal(float_list, "sum") == pytest.approx(6.0)


def test_identity_terminals_fallback_safely_and_preserve_empty_semantics() -> None:
    assert fpstreams.flow([1, 2.5]).aggregate(
        total=fpstreams.agg.sum(), mean=fpstreams.agg.mean()
    ) == {"total": 3.5, "mean": 1.75}
    with pytest.raises(fpstreams.NativeUnsupportedError, match="homogeneous"):
        fpstreams.flow([1, 2.5]).with_engine("native").sum()

    assert fpstreams.flow([]).with_engine("native").aggregate(
        count=fpstreams.agg.count(),
        total=fpstreams.agg.sum(),
        first=fpstreams.agg.first(),
        mean=fpstreams.agg.mean(),
    ) == {"count": 0, "total": 0, "first": None, "mean": None}

    with pytest.raises(fpstreams.NativeUnsupportedError, match="no native-compilable"):
        fpstreams.flow(range(10)).with_engine("native").to_list()


def test_named_aggregations_share_one_stable_pass() -> None:
    events: list[str] = []

    def values() -> Iterator[float]:
        events.append("open")
        try:
            yield from (1e16, 1.0, -1e16)
        finally:
            events.append("close")

    summary = fpstreams.flow.defer(values).aggregate(
        count=fpstreams.agg.count(),
        mean=fpstreams.agg.mean(fpstreams.fitem),
        variance=fpstreams.agg.variance(fpstreams.fitem, ddof=0),
        first=fpstreams.agg.first(fpstreams.fitem),
        last=fpstreams.agg.last(fpstreams.fitem),
    )

    assert summary == {
        "count": 3,
        "mean": pytest.approx(1 / 3),
        "variance": pytest.approx(2e32 / 3),
        "first": 1e16,
        "last": -1e16,
    }
    assert events == ["open", "close"]

    with pytest.raises(TypeError, match="must be an Aggregator"):
        fpstreams.flow.defer(values).aggregate(invalid=object())  # type: ignore[arg-type]
    assert events == ["open", "close"]


@pytest.mark.asyncio
async def test_async_named_aggregations_close_the_source() -> None:
    closed = False

    async def values() -> AsyncIterator[int]:
        nonlocal closed
        try:
            for value in (1, 2, 2, 3):
                yield value
        finally:
            closed = True

    summary = await fpstreams.aflow(values()).summarize(
        count=fpstreams.agg.count(),
        total=fpstreams.agg.sum(fpstreams.item),
        distinct=fpstreams.agg.count_distinct(fpstreams.item),
    )

    assert summary == {"count": 4, "total": 8, "distinct": 3}
    assert closed

    closed = False

    def fail(_state: Any, _value: Any) -> None:
        raise RuntimeError("stop")

    with pytest.raises(RuntimeError, match="stop"):
        await fpstreams.aflow(values()).aggregate(broken=fpstreams.Aggregator(lambda: None, fail))
    assert closed


def test_external_sort_is_stable_and_bounds_open_runs(tmp_path: Path) -> None:
    records = [{"group": position % 5, "position": position} for position in range(79, -1, -1)]
    for reverse in (False, True):
        expected = sorted(records, key=lambda row: row["group"], reverse=reverse)
        sorted_rows = fpstreams.rows(records).external_sort_by(
            "group",
            reverse=reverse,
            buffer_size=2,
            tempdir=tmp_path,
        )

        assert sorted_rows.to_list() == expected
        assert list(tmp_path.iterdir()) == []

    explanation = (
        fpstreams.flow(records).external_sort_by("group", buffer_size=2).explain().to_dict()
    )
    assert explanation["operations"] == [{"name": "external_sort"}]


def test_external_sort_cleans_up_after_short_circuit_and_errors(tmp_path: Path) -> None:
    first = (
        fpstreams.flow(range(100, -1, -1))
        .external_sort(buffer_size=3, tempdir=tmp_path)
        .take(1)
        .to_list()
    )
    assert first == [0]
    assert list(tmp_path.iterdir()) == []

    local_values = [(1, lambda: 1), (0, lambda: 0)]
    assert fpstreams.flow(local_values).external_sort(
        key=lambda row: row[0], buffer_size=2, tempdir=tmp_path
    ).to_list() == list(reversed(local_values))
    with pytest.raises(TypeError, match="must be picklable"):
        (
            fpstreams.flow(local_values)
            .external_sort(key=lambda row: row[0], buffer_size=1, tempdir=tmp_path)
            .to_list()
        )
    assert list(tmp_path.iterdir()) == []

    with pytest.raises(ValueError, match="buffer_size"):
        fpstreams.flow([1]).external_sort(buffer_size=0)


# --- Tests consolidated from test_benchmark.py ---


ROOT = Path(__file__).resolve().parents[1]
REQUIRED_RESULT_KEYS = {
    "name",
    "samples_seconds",
    "median_seconds",
    "stdev_seconds",
    "backend",
    "source_kind",
    "terminal",
    "baseline",
}


def test_quick_benchmark_emits_machine_readable_identity_baselines() -> None:
    report = benchmark.run(size=32, repeats=2, domain="int", quick=True)

    assert report["schema_version"] == 1
    assert report["metadata"]["size"] == 32
    assert report["metadata"]["repeats"] == 2
    results = report["results"]
    names = {result["name"] for result in results}
    assert {
        "python_builtin/list/identity/sum",
        "fpstreams_python/list/identity/sum",
        "fpstreams_auto/list/identity/sum",
        "python_builtin/range/identity/count",
        "fpstreams_python/range/identity/count",
        "fpstreams_auto/range/identity/count",
    } <= names
    for result in results:
        assert result.keys() >= REQUIRED_RESULT_KEYS
        assert len(result["samples_seconds"]) == 2
        assert result["median_seconds"] >= 0
        assert result["stdev_seconds"] >= 0


def test_regression_gate_compares_auto_identity_to_same_run_python() -> None:
    records: list[dict[str, Any]] = [
        {
            "name": "fpstreams_python/list/identity/sum",
            "median_seconds": 1.0,
            "backend": "python",
            "source_kind": "list",
            "terminal": "sum",
            "baseline": "python_builtin/list/identity/sum",
        },
        {
            "name": "fpstreams_auto/list/identity/sum",
            "median_seconds": 1.11,
            "backend": "auto",
            "source_kind": "list",
            "terminal": "sum",
            "baseline": "fpstreams_python/list/identity/sum",
        },
    ]

    regressions = benchmark.find_regressions(records, maximum_ratio=1.10)

    assert len(regressions) == 1
    assert regressions[0]["ratio"] == pytest.approx(1.11)


def test_regression_gate_compares_expression_fallback_to_lambda() -> None:
    records: list[dict[str, Any]] = [
        {
            "name": "fpstreams_lambda/list/map_filter/sum",
            "median_seconds": 1.0,
            "backend": "python",
            "source_kind": "list",
            "terminal": "sum",
            "baseline": "python_builtin/list/map_filter/sum",
        },
        {
            "name": "fpstreams_python/list/map_filter/sum",
            "median_seconds": 2.01,
            "backend": "python",
            "source_kind": "list",
            "terminal": "sum",
            "baseline": "fpstreams_lambda/list/map_filter/sum",
        },
    ]

    regressions = benchmark.find_regressions(records)

    assert len(regressions) == 1
    assert regressions[0]["maximum_ratio"] == 2.0


def _coverage_file(percent: float) -> dict[str, Any]:
    covered = round(percent)
    return {
        "summary": {
            "covered_lines": covered,
            "num_statements": 100,
            "covered_branches": 0,
            "num_branches": 0,
        }
    }


def _coverage_payload(
    *, total: float = 90, native: float = 95, spill: float = 95
) -> dict[str, Any]:
    return {
        "totals": {"percent_covered": total},
        "files": {
            "src/fpstreams/planning/native.py": _coverage_file(95),
            "src/fpstreams/planning/source.py": _coverage_file(95),
            "src/fpstreams/execution/native.py": _coverage_file(native),
            "src/fpstreams/tabular/spill.py": _coverage_file(spill),
            "src/fpstreams/tabular/spill_io.py": _coverage_file(spill),
            "src/fpstreams/tabular/spill_limits.py": _coverage_file(spill),
            "src/fpstreams/execution/async_.py": _coverage_file(90),
            "src/fpstreams/execution/async_concurrency.py": _coverage_file(90),
            "src/fpstreams/execution/async_iterators.py": _coverage_file(90),
            "src/fpstreams/execution/async_ops.py": _coverage_file(90),
        },
    }


def _run_coverage_check(
    tmp_path: Path, payload: dict[str, Any]
) -> subprocess.CompletedProcess[str]:
    report = tmp_path / "coverage.json"
    report.write_text(json.dumps(payload), encoding="utf-8")
    return subprocess.run(
        [sys.executable, str(ROOT / "tools" / "check_coverage.py"), str(report)],
        check=False,
        capture_output=True,
        text=True,
    )


def test_coverage_gate_accepts_all_thresholds(tmp_path: Path) -> None:
    result = _run_coverage_check(tmp_path, _coverage_payload())

    assert result.returncode == 0, result.stderr
    assert "coverage thresholds passed" in result.stdout


def test_coverage_gate_rejects_low_total(tmp_path: Path) -> None:
    result = _run_coverage_check(tmp_path, _coverage_payload(total=84.99))

    assert result.returncode == 1
    assert "total: 84.99% < 85.00%" in result.stderr


def test_coverage_gate_rejects_low_focus_group(tmp_path: Path) -> None:
    result = _run_coverage_check(tmp_path, _coverage_payload(spill=89))

    assert result.returncode == 1
    assert "spill: 89.00% < 90.00%" in result.stderr


def test_coverage_gate_checks_native_execution_separately(tmp_path: Path) -> None:
    result = _run_coverage_check(tmp_path, _coverage_payload(native=89))

    assert result.returncode == 1
    assert "native execution: 89.00% < 90.00%" in result.stderr
