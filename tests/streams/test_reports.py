"""streams reports regression contracts."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import fpstreams
from fpstreams import NativeUnsupportedError, flow


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


def test_identity_index_of_reports_only_the_range_direct_path() -> None:
    """Inspecting a list must not report the range-only arithmetic shortcut."""
    direct = flow(range(5)).run_with_report("index_of", 3)
    scanned = flow([1, 2, 3]).run_with_report("index_of", 3)

    assert direct.value == 3
    assert direct.report.compiler_engine == "not_compiled"
    assert direct.report.strategy == "python_direct"
    assert scanned.value == 2
    assert scanned.report.compiler_engine == "python"
    assert scanned.report.strategy == "planned:python"


def test_numeric_iterator_mean_reports_direct_and_hybrid_execution() -> None:
    """Reports distinguish an all-Rust reduction from a Python suffix continuation."""
    from fractions import Fraction

    from fpstreams.collecting.statistics import compensated_mean

    direct = fpstreams.flow(iter(range(1_000))).run_with_report("mean")
    mixed_values = [*range(128), Fraction(1, 3), 7]
    mixed = fpstreams.flow(iter(mixed_values)).run_with_report("mean")

    assert direct.value == 499.5
    assert direct.report.strategy == "rust_direct"
    assert mixed.value == compensated_mean(mixed_values)
    assert mixed.report.strategy == "rust_python_hybrid"


def test_run_with_report_runs_one_real_terminal_and_returns_immutable_metrics() -> None:
    calls: list[int] = []

    def double(value: int) -> int:
        calls.append(value)
        return value * 2

    execution = fpstreams.flow([1, 2, 3]).with_engine("python").map(double).run_with_report("sum")

    assert execution.value == 12
    assert calls == [1, 2, 3]
    assert execution.report.terminal == "sum"
    assert execution.report.requested_engine == "python"
    assert execution.report.compiler_engine == "python"
    assert execution.report.strategy == "planned:python"
    assert execution.report.elapsed_ns >= 0
    assert execution.report.peak_owned_async_tasks == 0
    assert execution.report.peak_spill_files == 0
    assert execution.report.spill_bytes_written == 0
    assert isinstance(execution.report.events, tuple)
    with pytest.raises(AttributeError):
        execution.report.compiler_engine = "native"  # type: ignore[misc]
    with pytest.raises(AttributeError):
        execution.report.events[0].reason = "changed"  # type: ignore[misc]


def test_run_with_report_preserves_exact_count_metadata_shortcut() -> None:
    execution = fpstreams.flow([1, 2, 3]).run_with_report("count")

    assert execution.value == 3
    assert execution.report.terminal == "count"
    assert execution.report.compiler_engine == "not_compiled"
    assert execution.report.strategy == "metadata"
    assert "cardinality" in execution.report.reason
    assert len(execution.report.events) == 1
    assert execution.report.events[0].kind == "metadata"
    assert execution.report.events[0].compiler_engine == "not_compiled"
    assert execution.report.events_truncated is False


def test_run_with_report_captures_external_sort_spill_metrics(tmp_path: Path) -> None:
    execution = (
        fpstreams.flow(range(20, -1, -1))
        .sort_by(lambda value: value, buffer_size=3, tempdir=tmp_path)
        .run_with_report("to_list")
    )

    assert execution.value == list(range(21))
    assert execution.report.peak_spill_files > 0
    assert execution.report.spill_bytes_written > 0
    assert list(tmp_path.iterdir()) == []


def test_run_with_report_preserves_terminal_exception_and_resets_scope() -> None:
    failure = ValueError("boom")

    def fail(_value: int) -> int:
        raise failure

    with pytest.raises(ValueError) as captured:
        fpstreams.flow([1]).map(fail).run_with_report("to_list")
    assert captured.value is failure

    assert fpstreams.flow([1]).run_with_report("to_list").value == [1]


@pytest.mark.parametrize(
    "outer_engine,child_engine,mapper",
    [
        pytest.param("auto", "auto", lambda value: value * 10, id="python-callback"),
        pytest.param("python", "native", fpstreams.item * 10, id="native-child"),
    ],
)
def test_run_with_report_allows_composite_child_plans(outer_engine, child_engine, mapper) -> None:
    child = fpstreams.flow([2, 3]).with_engine(child_engine).map(mapper)
    query = fpstreams.flow([1]).with_engine(outer_engine).concat(child)

    execution = query.run_with_report("to_list")

    assert execution.value == [1, 20, 30]
    assert execution.report.terminal == "to_list"
    assert execution.report.requested_engine == outer_engine
    assert execution.report.compiler_engine == "python"
    assert execution.report.strategy == "planned:python"
    assert len(execution.report.events) >= 2
    assert execution.report.events[0].compiler_engine == "python"
    expected_child_engine = "native" if child_engine == "native" else "python"
    assert any(
        event.kind == "plan" and event.compiler_engine == expected_child_engine
        for event in execution.report.events
    )
    assert execution.report.events_truncated is False


@pytest.mark.parametrize("count", [64, 65, 80])
def test_run_with_report_events_are_bounded_for_each_terminal(count: int) -> None:
    query = fpstreams.flow([0]).concat(*(fpstreams.flow([value]) for value in range(1, count)))

    execution = query.run_with_report("to_list")

    assert execution.value == list(range(count))
    assert len(execution.report.events) == 64
    assert execution.report.events_truncated is (count > 64)
    subsequent = fpstreams.flow([1]).run_with_report("count")
    assert len(subsequent.report.events) == 1
    assert subsequent.report.events_truncated is False


def test_execution_report_keeps_the_existing_positional_constructor() -> None:
    report = fpstreams.ExecutionReport(
        "count", "auto", "not_compiled", "metadata", "size", 0, 0, 0, 0
    )

    assert report.events == ()
    assert report.events_truncated is False


def test_run_with_report_events_do_not_replay_or_preconsume_a_short_circuited_source() -> None:
    calls = []

    def source():
        try:
            calls.append("first")
            yield 1
            calls.append("second")
            yield 2
        finally:
            calls.append("close")

    execution = fpstreams.flow(source()).take(1).run_with_report("to_list")

    assert execution.value == [1]
    assert calls == ["first", "close"]
    assert execution.report.events


def test_run_with_report_events_do_not_retain_input_objects() -> None:
    import gc
    import weakref

    class Payload:
        pass

    value = Payload()
    reference = weakref.ref(value)
    execution = fpstreams.flow(iter([value])).map(lambda _value: 1).run_with_report("sum")
    del value
    gc.collect()

    assert execution.value == 1
    assert reference() is None


@pytest.mark.parametrize(
    "inner_engine,mapper,expected",
    [
        pytest.param("auto", None, [1, 2], id="auto-identity"),
        pytest.param("native", fpstreams.item + 1, [2, 3], id="native-mapped"),
    ],
)
def test_run_with_report_allows_nested_reported_terminals_in_callbacks(
    inner_engine, mapper, expected
) -> None:
    reports = []

    def inner_sum(value):
        child = fpstreams.flow([value]).with_engine(inner_engine)
        if mapper is not None:
            child = child.map(mapper)
        inner = child.run_with_report("sum")
        reports.append(inner.report)
        return inner.value

    execution = (
        fpstreams.flow([1, 2]).with_engine("python").map(inner_sum).run_with_report("to_list")
    )

    assert execution.value == expected
    assert execution.report.terminal == "to_list"
    assert all(event.compiler_engine == "python" for event in execution.report.events)
    assert len(reports) == 2
    assert all(report.requested_engine == inner_engine for report in reports)
    expected_inner_engine = "native" if inner_engine == "native" else "python"
    assert all(
        any(event.compiler_engine == expected_inner_engine for event in report.events)
        for report in reports
    )


def test_run_with_report_events_leave_named_pair_aggregation_arguments_available() -> None:
    execution = fpstreams.pairs([("a", 2), ("a", 3)]).run_with_report(
        "aggregate_values", diagnostics=fpstreams.agg.sum(), max_events=fpstreams.agg.count()
    )

    assert execution.value == {"a": {"diagnostics": 5, "max_events": 2}}
    assert any(event.strategy == execution.report.strategy for event in execution.report.events)
    assert not any(event.strategy == "pairs_direct" for event in execution.report.events)


def test_run_with_report_preserves_outer_engine_for_iteration_terminals() -> None:
    execution = fpstreams.flow([1, 2, 3]).run_with_report(
        "reduce", lambda left, right: left + right
    )

    assert execution.value == 6
    assert execution.report.requested_engine == "auto"


def test_run_with_report_handles_direct_none_and_non_consuming_collector() -> None:
    none_result = fpstreams.flow([1, 2, 3]).run_with_report("none", lambda value: value > 3)
    collect_result = fpstreams.flow([1, 2, 3]).run_with_report("collect", lambda _values: 42)

    assert none_result.value is True
    assert none_result.report.compiler_engine == "not_compiled"
    assert none_result.report.strategy == "python_direct"
    assert collect_result.value == 42
    assert collect_result.report.compiler_engine == "not_compiled"
    assert collect_result.report.strategy == "dynamic_collector"


def test_run_with_report_rejects_lazy_operations_before_execution() -> None:
    with pytest.raises(ValueError, match="eager terminal"):
        fpstreams.flow([1, 2, 3]).run_with_report("map", lambda value: value)


@pytest.mark.parametrize(
    "terminal", ["to_dict", "group_values", "collect_values", "aggregate_values"]
)
def test_pairs_run_with_report_preserves_single_consumption_and_arguments(terminal) -> None:
    events = []
    marker = object()

    def source():
        try:
            events.append("yield")
            yield ("a", marker)
        finally:
            events.append("close")

    pipeline = fpstreams.pairs(source())
    kwargs = (
        {"on_duplicate": "first"}
        if terminal == "to_dict"
        else {"total": fpstreams.agg.count()}
        if terminal == "aggregate_values"
        else {}
    )
    args = (list,) if terminal == "collect_values" else ()
    execution = pipeline.run_with_report(terminal, *args, **kwargs)
    assert events == ["yield", "close"]
    expected = (
        {"a": marker}
        if terminal == "to_dict"
        else {"a": {"total": 1}}
        if terminal == "aggregate_values"
        else {"a": [marker]}
    )
    assert execution.value == expected
    assert execution.report.terminal == terminal
    assert execution.report.requested_engine == "auto"
    with pytest.raises(fpstreams.FlowConsumedError):
        pipeline.to_dict()


def test_pairs_run_with_report_native_and_python_aggregation_routes(monkeypatch) -> None:
    from fpstreams.execution import relational

    values = [(1, 1), (1, 2), (2, 4)]
    native = fpstreams.pairs(values).run_with_report("aggregate_values", total=fpstreams.agg.sum())
    assert native.value == {1: {"total": 3}, 2: {"total": 4}}
    assert native.report.strategy == "rust_direct"
    monkeypatch.setattr(relational, "try_native_pair_aggregations", lambda *_args: None)
    fallback = fpstreams.pairs(values).run_with_report(
        "aggregate_values", total=fpstreams.agg.sum()
    )
    assert fallback.value == native.value
    assert fallback.report.strategy == "planned:python"


def test_pairs_run_with_report_errors_and_nested_reports_leave_outer_context_intact() -> None:
    from fpstreams.runtime.report import _current_recorder

    failure = RuntimeError("callback failed")

    def fail(_key, _value):
        raise failure

    with pytest.raises(RuntimeError) as captured:
        fpstreams.pairs([("a", 1)]).map_pairs(fail).run_with_report("to_dict")
    assert captured.value is failure
    assert _current_recorder() is None
    pipeline = fpstreams.pairs(iter([("a", 2)]))
    with pytest.raises(ValueError, match="Pairs terminal"):
        pipeline.run_with_report("map_values", lambda v: v)
    assert pipeline.to_dict() == {"a": 2}
    outer = (
        fpstreams.flow([1, 2])
        .map(lambda value: fpstreams.pairs([("a", value)]).run_with_report("to_dict").value)
        .run_with_report("to_list")
    )
    assert outer.value == [{"a": 1}, {"a": 2}]
    assert outer.report.terminal == "to_list"
    assert _current_recorder() is None


def test_window_reduce_validates_before_opening_and_reports_real_plan_bounds():
    import operator

    indexed = []
    opened = []

    class Count:
        def __index__(self):
            indexed.append(True)
            return 4

    def source():
        opened.append(True)
        return iter(range(10))

    for kwargs in (
        {"size": 2.5},
        {"size": 0},
        {"size": 4, "step": float("nan")},
        {"size": 4, "step": -1},
    ):
        with pytest.raises((TypeError, ValueError)):
            flow.defer(source).window_reduce(combine=operator.add, **kwargs)
    assert opened == []
    query = flow(range(10)).window_reduce(Count(), operator.add, step=3)
    assert indexed == [True]
    operation = query.explain("list").to_dict()["semantics"]["operations"][0]
    assert operation["name"] == "window_reduce"
    assert operation["progress"] == "prefix_emitting"
    assert operation["state"] == {"kind": "bounded", "bound": 5, "spillable": False}
    assert operation["output"]["cardinality"] == {"kind": "exact", "value": len(query.to_list())}
    assert indexed == [True]
    with pytest.raises(NativeUnsupportedError):
        flow.defer(source).window_reduce(4, operator.add).with_engine("native").to_list()
    assert opened == []


def test_report_metadata_export_is_detached_and_never_inspects_terminal_values():
    class PrivateValue:
        def __repr__(self):
            raise AssertionError("metadata must not inspect terminal values")

    calls = []
    value = PrivateValue()
    query = flow([value]).tap(lambda item: calls.append(id(item)))
    planned = query.explain().to_dict()
    assert calls == []
    assert (planned["schema_version"], planned["kind"], planned["mode"]) == (
        1,
        "fpstreams.plan_explanation",
        "sync",
    )
    observed = query.run_with_report("to_list")
    metadata = observed.report.to_dict()
    assert json.loads(json.dumps(metadata)) == metadata
    assert metadata["kind"] == "fpstreams.execution_report"
    assert metadata["schema_version"] == 1
    assert metadata["resource_scope"] == "query_owned"
    assert metadata["metric_units"]["elapsed_ns"] == "nanoseconds"
    assert "value" not in metadata and calls == [id(value)]
    metadata["events"][0]["reason"] = "changed"
    metadata["metric_units"].clear()
    assert observed.report.to_dict()["events"][0]["reason"] != "changed"
    assert observed.report.to_dict()["metric_units"]["elapsed_ns"] == "nanoseconds"
