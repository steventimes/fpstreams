"""Parallel, native, hybrid, resource, and external-sort execution."""

from __future__ import annotations

import threading
import time
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import pytest

import fpstreams
from fpstreams import flow


def _square(value: int) -> int:
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
    assert flow(range(6)).parallel(workers=2, buffer=2).map(_square).to_list() == [
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


def test_direct_homogeneous_numeric_sequences_infer_the_native_kind() -> None:
    assert fpstreams.flow([1, 2, 3]).with_engine("native").aggregate(
        total=fpstreams.agg.sum(), mean=fpstreams.agg.mean()
    ) == {"total": 6, "mean": 2.0}
    assert fpstreams.flow((1.5, 2.5, 3.5)).with_engine("native").aggregate(
        total=fpstreams.agg.sum(), mean=fpstreams.agg.mean()
    ) == {"total": pytest.approx(7.5), "mean": pytest.approx(2.5)}


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
