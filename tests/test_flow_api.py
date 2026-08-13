"""Synchronous Flow behavior, laziness, selectors, and terminals."""

from __future__ import annotations

import pytest

import fpstreams
from fpstreams import NativeUnsupportedError, SelectionError, flow


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
