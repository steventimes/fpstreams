"""Rows pivot, unpivot, explode, and unnest contracts."""

from __future__ import annotations

import gc
import json
import os
import signal
import sys
import weakref
from collections.abc import Callable, Iterable, Iterator, Mapping
from typing import Any, cast

import pytest

import fpstreams

from ._tabular_support import (
    _run_inline_python as _run_inline_python,
)


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


def test_rows_unpivot_preserves_exact_record_snapshots_and_protocol_fallback() -> None:
    source = {
        "account": 7,
        "region": "west",
        "january": 10,
        "february": 20,
        "march": 30,
    }
    reshaped = fpstreams.rows([source]).unpivot(
        "january",
        "february",
        "march",
        names_to="month",
        values_to="sales",
    )
    iterator = iter(reshaped)

    assert next(iterator) == {
        "account": 7,
        "region": "west",
        "month": "january",
        "sales": 10,
    }
    source["region"] = "changed"
    source["february"] = 999
    del source["march"]
    assert list(iterator) == [
        {"account": 7, "region": "west", "month": "february", "sales": 20},
        {"account": 7, "region": "west", "month": "march", "sales": 30},
    ]

    events: list[str] = []

    class ProtocolMapping(Mapping[str, object]):
        def __init__(self) -> None:
            self.values = {"account": 9, "january": 3, "february": 4, "march": 5}

        def __getitem__(self, key: str) -> object:
            events.append(f"get:{key}")
            return self.values[key]

        def __iter__(self) -> Iterator[str]:
            events.append("iter")
            return iter(self.values)

        def __len__(self) -> int:
            return len(self.values)

    assert fpstreams.rows([ProtocolMapping()]).unpivot(
        "january", "february", "march"
    ).to_list() == [
        {"account": 9, "variable": "january", "value": 3},
        {"account": 9, "variable": "february", "value": 4},
        {"account": 9, "variable": "march", "value": 5},
    ]
    assert events == [
        "iter",
        "get:account",
        "get:january",
        "get:february",
        "get:march",
    ]


def test_row_expansions_share_one_sealed_terminal_plan_type() -> None:
    """The list terminal trusts one project plan, not an API-specific type allowlist."""
    rows_module = sys.modules["fpstreams.tabular.rows"]

    exploded = fpstreams.rows([{"labels": [1, 2]}]).explode("labels", into="individual_label")
    unpivoted = fpstreams.rows([{"north": 1, "south": 2, "east": 3}]).unpivot(
        "north",
        "south",
        "east",
        names_to="territory_axis",
        values_to="gross_measure",
    )
    explode_function = exploded._flow._pipeline.operations[-1].function
    unpivot_function = unpivoted._flow._pipeline.operations[-1].function

    assert type(explode_function) is type(unpivot_function)
    assert rows_module._materialized_row_appender(explode_function) is not None
    assert rows_module._materialized_row_appender(unpivot_function) is not None

    class UntrustedExpansion(type(explode_function)):
        pass

    spoof = object.__new__(UntrustedExpansion)
    assert rows_module._materialized_row_appender(spoof) is None


def test_materialized_unpivot_keeps_exact_string_rows_on_the_direct_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Row width does not reject proven dictionaries; protocol keys still fall back."""
    rows_module = sys.modules["fpstreams.tabular.rows"]
    canonical_calls: list[tuple[str, int]] = []
    original_explode = rows_module._ExplodeExpansion.__call__
    original_unpivot = rows_module._UnpivotExpansion.__call__

    def tracked_explode(expansion: object, row: object) -> Iterator[dict[str, object]]:
        canonical_calls.append(("explode", len(cast(dict[object, object], row))))
        return original_explode(expansion, row)

    def tracked_unpivot(expansion: object, row: object) -> Iterator[dict[str, object]]:
        canonical_calls.append(("unpivot", len(cast(dict[object, object], row))))
        return original_unpivot(expansion, row)

    monkeypatch.setattr(rows_module._ExplodeExpansion, "__call__", tracked_explode)
    monkeypatch.setattr(rows_module._UnpivotExpansion, "__call__", tracked_unpivot)

    def record(width: int, required: dict[str, object]) -> dict[str, object]:
        extras = {f"extra_{position}": position for position in range(width - len(required))}
        return {**required, **extras}

    assert fpstreams.rows([record(16, {"id": 1, "tags": [1, 2]})]).explode("tags").to_list() == [
        {**record(16, {"id": 1, "tags": [1, 2]}), "tags": 1},
        {**record(16, {"id": 1, "tags": [1, 2]}), "tags": 2},
    ]
    fpstreams.rows([record(17, {"id": 2, "tags": [3]})]).explode("tags").to_list()

    selected = tuple(f"value_{position}" for position in range(8))
    required = {"id": 3, **{name: position for position, name in enumerate(selected)}}
    fpstreams.rows([record(16, required)]).unpivot(*selected).to_list()
    fpstreams.rows([record(17, required)]).unpivot(*selected).to_list()

    many_selected = tuple(f"measure_{position}" for position in range(20))
    many_required = {
        "id": 4,
        **{name: position for position, name in enumerate(many_selected)},
    }
    fpstreams.rows([record(25, many_required)]).unpivot(*many_selected).to_list()
    fpstreams.rows([record(26, many_required)]).unpivot(*many_selected).to_list()

    protocol_key = object()
    fpstreams.rows([{"id": 5, "left": 10, "right": 20, protocol_key: "preserved"}]).unpivot(
        "left", "right"
    ).to_list()

    assert fpstreams.rows([{"id": 7, "left.amount": 50, "right.amount": 60}]).unpivot(
        "left.amount", "right.amount"
    ).to_list() == [
        {"id": 7, "variable": "left.amount", "value": 50},
        {"id": 7, "variable": "right.amount", "value": 60},
    ]

    assert canonical_calls == [("unpivot", 4)]

    materialized = (
        fpstreams.rows([{"id": 6, "left": 30, "right": 40}]).unpivot("left", "right").to_list()
    )
    materialized[0]["id"] = "changed"
    assert materialized[1] == {"id": 6, "variable": "right", "value": 40}


def test_materialized_unpivot_sealed_appender_owns_one_shot_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A list terminal can batch-expand one claimed source without reopening it."""
    from fpstreams.runtime.failpoints import failpoint

    rows_module = sys.modules["fpstreams.tabular.rows"]
    original = rows_module._UnpivotExpansion.__call__
    canonical_calls = 0

    def tracked(expansion: object, row: object) -> Iterator[dict[str, object]]:
        nonlocal canonical_calls
        canonical_calls += 1
        return original(expansion, row)

    monkeypatch.setattr(rows_module._UnpivotExpansion, "__call__", tracked)
    events: list[str] = []

    def source() -> Iterator[dict[str, int]]:
        events.append("open")
        try:
            yield {"id": 1, "left": 10, "right": 20}
            yield {"id": 2, "left": 30, "right": 40}
        finally:
            events.append("close")

    query = fpstreams.rows(source()).unpivot("left", "right")
    assert query.to_list() == [
        {"id": 1, "variable": "left", "value": 10},
        {"id": 1, "variable": "right", "value": 20},
        {"id": 2, "variable": "left", "value": 30},
        {"id": 2, "variable": "right", "value": 40},
    ]
    assert events == ["open", "close"]
    assert canonical_calls == 0
    with pytest.raises(fpstreams.FlowConsumedError):
        query.to_list()

    with failpoint("inactive.unpivot.boundary", RuntimeError("unused")):
        fpstreams.rows([{"id": 3, "left": 50, "right": 60}]).unpivot("left", "right").to_list()
    assert canonical_calls == 1


def test_unpivot_native_prefix_admits_only_safe_materialized_source_shapes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The generic prefix hook sees retained sequences but never claims a generator."""
    from fpstreams import _native

    calls: list[type[object]] = []

    def decline(
        _output: list[object],
        source: Iterator[object],
        _columns: tuple[str, ...],
        _names_to: str,
        _values_to: str,
    ) -> None:
        calls.append(type(source))
        return None

    monkeypatch.setattr(_native, "unpivot_exact_dict_prefix_v1", decline, raising=False)
    records = [
        {"account": 1, "january": 10, "february": 20, "march": 30},
        {"account": 2, "january": 40, "february": 50, "march": 60},
    ]
    expected = [
        {"account": 1, "month": "january", "amount": 10},
        {"account": 1, "month": "february", "amount": 20},
        {"account": 1, "month": "march", "amount": 30},
        {"account": 2, "month": "january", "amount": 40},
        {"account": 2, "month": "february", "amount": 50},
        {"account": 2, "month": "march", "amount": 60},
    ]

    for source in (records, tuple(records), iter(records), (row for row in records)):
        assert (
            fpstreams.rows(source)
            .unpivot(
                "january",
                "february",
                "march",
                names_to="month",
                values_to="amount",
            )
            .to_list()
            == expected
        )

    assert [kind.__name__ for kind in calls] == [
        "list_iterator",
        "tuple_iterator",
        "list_iterator",
    ]


def test_unpivot_native_decline_resumes_the_same_opened_iterator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A native boundary is handed once to Python without reopening or skipping rows."""
    from fpstreams import _native

    events: list[str] = []

    def boundary(
        _output: list[object],
        source: Iterator[object],
        _columns: tuple[str, ...],
        _names_to: str,
        _values_to: str,
    ) -> tuple[object, bool]:
        events.append("native")
        return next(source), False

    monkeypatch.setattr(_native, "unpivot_exact_dict_prefix_v1", boundary, raising=False)
    records = [
        {"account": 1, "january": 10, "february": 20},
        {"account": 2, "january": 30, "february": 40},
    ]

    assert fpstreams.rows(records).unpivot(
        "january", "february", names_to="month", values_to="amount"
    ).to_list() == [
        {"account": 1, "month": "january", "amount": 10},
        {"account": 1, "month": "february", "amount": 20},
        {"account": 2, "month": "january", "amount": 30},
        {"account": 2, "month": "february", "amount": 40},
    ]
    assert events == ["native"]


def test_unpivot_native_stop_iteration_keeps_the_generator_error_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An optimization callback cannot leak StopIteration out of a materialized flat-map."""
    from fpstreams import _native

    failure = StopIteration("signal stop")

    def interrupted(*_arguments: object) -> None:
        raise failure

    monkeypatch.setattr(_native, "unpivot_exact_dict_prefix_v1", interrupted)

    with pytest.raises(RuntimeError, match=r"^generator raised StopIteration$") as caught:
        fpstreams.rows([{"account": 1, "january": 10}]).unpivot("january").to_list()

    assert caught.value.__cause__ is failure


def test_unpivot_native_prefix_treats_none_as_a_real_python_boundary() -> None:
    records: list[object] = [
        {"account": 1, "january": 10},
        None,
        {"account": 2, "january": 20},
    ]

    with pytest.raises(fpstreams.SelectionError, match="NoneType cannot be represented"):
        fpstreams.rows(records).unpivot("january").to_list()


def test_rows_unpivot_reports_missing_and_colliding_nonbenchmark_columns() -> None:
    with pytest.raises(fpstreams.SelectionError, match=r"\['february', 'march'\]"):
        fpstreams.rows([{"account": 1, "january": 10}]).unpivot(
            "january", "february", "march"
        ).to_list()

    with pytest.raises(fpstreams.DuplicateKeyError, match="output names collide"):
        fpstreams.rows(
            [{"account": 1, "month": "existing", "january": 10, "february": 20}]
        ).unpivot("january", "february", names_to="month").to_list()


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


def test_rows_pivot_sparse_columns_preserve_order_and_fill_identity() -> None:
    """Template-backed sparse output keeps the public index/column layout and fill object."""
    fill = object()

    result = (
        fpstreams.rows(
            [
                {"site": "north", "rack": 1, "metric": "cpu", "value": 10},
                {"site": "south", "rack": 2, "metric": "memory", "value": 20},
                {"site": "north", "rack": 1, "metric": "disk", "value": 30},
            ]
        )
        .pivot(
            index=("site", "rack"),
            columns="metric",
            values="value",
            fill=fill,
        )
        .to_list()
    )

    assert [tuple(row) for row in result] == [
        ("site", "rack", "cpu", "memory", "disk"),
        ("site", "rack", "cpu", "memory", "disk"),
    ]
    assert result[0]["memory"] is fill
    assert result[1]["cpu"] is fill
    assert result[1]["disk"] is fill


def test_rows_pivot_uses_retained_exact_native_kernel_lazily(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A direct replayable source reaches the exact kernel only when pivot execution starts."""
    from fpstreams import _native

    records = [
        {"site": 1, "metric": "cpu", "reading": 2},
        {"site": 1, "metric": "memory", "reading": 3},
    ]
    fill = object()
    result_marker = object()
    calls: list[tuple[object, ...]] = []

    def native_pivot(*arguments: object) -> list[dict[str, object]]:
        calls.append(arguments)
        return [{"site": 1, "cpu": result_marker, "memory": fill}]

    monkeypatch.setattr(_native, "pivot_exact_dict_rows_v1", native_pivot, raising=False)
    pivoted = fpstreams.rows(records).pivot(
        index="site",
        columns="metric",
        values="reading",
        fill=fill,
    )

    assert calls == []
    assert pivoted.to_list() == [{"site": 1, "cpu": result_marker, "memory": fill}]
    assert len(calls) == 1
    assert calls[0][0] is records
    assert calls[0][1:6] == (("site",), "metric", "reading", ("site",), fill)
    assert calls[0][6] is fpstreams.DuplicateKeyError


def test_rows_pivot_native_decline_reuses_the_unchanged_python_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A speculative exact-kernel decline leaves the replayable input for canonical pivot."""
    from fpstreams import _native

    records = [
        {"site": 1, "metric": "cpu", "reading": 2},
        {"site": 2, "metric": "memory", "reading": 3},
    ]
    calls = 0

    def decline(*_arguments: object) -> None:
        nonlocal calls
        calls += 1
        return None

    monkeypatch.setattr(_native, "pivot_exact_dict_rows_v1", decline, raising=False)

    assert fpstreams.rows(records).pivot(
        index="site",
        columns="metric",
        values="reading",
        fill=0,
    ).to_list() == [
        {"site": 1, "cpu": 2, "memory": 0},
        {"site": 2, "cpu": 0, "memory": 3},
    ]
    assert calls == 1
    assert records == [
        {"site": 1, "metric": "cpu", "reading": 2},
        {"site": 2, "metric": "memory", "reading": 3},
    ]


def test_rows_pivot_rechecks_a_live_retained_source_on_every_execution() -> None:
    """Replayable list pivots observe later row edits and appends instead of caching a snapshot."""
    records = [{"site": 1, "metric": "cpu", "reading": 2}]
    pivoted = fpstreams.rows(records).pivot(
        index="site", columns="metric", values="reading", fill=0
    )

    assert pivoted.to_list() == [{"site": 1, "cpu": 2}]
    records[0]["reading"] = 5
    records.append({"site": 2, "metric": "memory", "reading": 7})

    assert pivoted.to_list() == [
        {"site": 1, "cpu": 5, "memory": 0},
        {"site": 2, "cpu": 0, "memory": 7},
    ]


def test_rows_pivot_native_gate_preserves_dynamic_and_instrumented_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Python-only requests, upstream work, dynamic globals, and failpoints bypass native pivot."""
    from fpstreams import _native
    from fpstreams.runtime.failpoints import failpoint

    records = [{"site": 1, "metric": "cpu", "reading": 2}]

    def unexpected(*_arguments: object) -> object:
        raise AssertionError("guarded pivot must retain Python execution")

    monkeypatch.setattr(_native, "pivot_exact_dict_rows_v1", unexpected, raising=False)

    assert fpstreams.rows(records).with_engine("python").pivot(
        index="site", columns="metric", values="reading"
    ).to_list() == [{"site": 1, "cpu": 2}]
    assert fpstreams.rows(records).where(lambda _row: True).pivot(
        index="site", columns="metric", values="reading"
    ).to_list() == [{"site": 1, "cpu": 2}]

    from fpstreams.streams.flow import Flow
    from fpstreams.tabular.rows import Rows

    class CustomRows(Rows[dict[str, object]]):
        def __iter__(self) -> Iterator[dict[str, object]]:
            return iter([{"site": 2, "metric": "memory", "reading": 3}])

    class CustomFlow(Flow[dict[str, object]]):
        def __iter__(self) -> Iterator[dict[str, object]]:
            return iter([{"site": 3, "metric": "disk", "reading": 4}])

    assert CustomRows(records).pivot(
        index="site", columns="metric", values="reading"
    ).to_list() == [{"site": 2, "memory": 3}]
    assert Rows(CustomFlow(records)).pivot(
        index="site", columns="metric", values="reading"
    ).to_list() == [{"site": 3, "disk": 4}]

    pivoted = fpstreams.rows(records).pivot(index="site", columns="metric", values="reading")
    rows_module = sys.modules["fpstreams.tabular.rows"]
    monkeypatch.setattr(rows_module, "str", lambda _value: "renamed", raising=False)
    assert pivoted.to_list() == [{"site": 1, "renamed": 2}]

    monkeypatch.delattr(rows_module, "str")
    failure = RuntimeError("observed source pull")
    with failpoint("iterator.pull.after", failure), pytest.raises(RuntimeError) as raised:
        fpstreams.rows(records).pivot(index="site", columns="metric", values="reading").to_list()
    assert raised.value is failure


@pytest.mark.parametrize("engine", ["python", "auto"])
@pytest.mark.parametrize("field", ["id", "column", "value"])
@pytest.mark.parametrize("when", ["before", "open", "second_row"])
@pytest.mark.parametrize("mutation", ["closure", "code"])
def test_rows_pivot_calls_live_field_selectors(
    monkeypatch: pytest.MonkeyPatch, engine: str, field: str, when: str, mutation: str
) -> None:
    """Direct dictionaries use the same live selector functions as other mappings."""
    rows_module = sys.modules["fpstreams.tabular.rows"]
    original_compile = rows_module.compile_selector
    captured = {}
    closed = []
    records = [
        {"id": 1, "column": "x", "value": 2, "new_id": 10, "new_column": "u", "new_value": 30},
        {"id": 1, "column": "y", "value": 3, "new_id": 20, "new_column": "v", "new_value": 40},
    ]

    def capture(selector):
        function = original_compile(selector)
        captured[selector] = function
        return function

    def change():
        function = captured[field]
        if mutation == "closure":
            cells = dict(zip(function.__code__.co_freevars, function.__closure__, strict=True))
            cells["selector"].cell_contents = f"new_{field}"
        else:
            selector = field

            def replacement(row):
                return row[f"new_{selector}"]

            function.__code__ = replacement.__code__

    def source():
        try:
            if when == "open":
                change()
            yield records[0]
            if when == "second_row":
                change()
            yield records[1]
        finally:
            closed.append(True)

    monkeypatch.setattr(rows_module, "compile_selector", capture)
    pipeline = (
        fpstreams.rows(records if when == "before" else source())
        .with_engine(engine)
        .pivot(index="id", columns="column", values="value", fill=-1)
    )
    if when == "before":
        change()

    changed_first = when != "second_row"
    if field == "id":
        expected = [
            {"id": 10 if changed_first else 1, "x": 2, "y": -1},
            {"id": 20, "x": -1, "y": 3},
        ]
    elif field == "column":
        expected = [{"id": 1, "u" if changed_first else "x": 2, "v": 3}]
    else:
        expected = [{"id": 1, "x": 30 if changed_first else 2, "y": 40}]
    assert pipeline.to_list() == expected
    assert closed == ([] if when == "before" else [True])


@pytest.mark.parametrize("engine", ["python", "auto"])
@pytest.mark.parametrize("raises", [False, True])
def test_rows_pivot_keeps_selector_global_callbacks(
    monkeypatch: pytest.MonkeyPatch, engine: str, raises: bool
) -> None:
    """Selector environment changes remain visible, including their original exceptions."""
    rows_module = sys.modules["fpstreams.tabular.rows"]
    record = {"id": 1, "column": "x", "value": 2}
    pipeline = (
        fpstreams.rows([record])
        .with_engine(engine)
        .pivot(index="id", columns="column", values="value")
    )
    calls = []
    failure = RuntimeError("selector environment changed")

    def live_type(value):
        calls.append(value)
        if raises:
            raise failure
        record["value"] = 30
        return type(value)

    monkeypatch.setitem(rows_module.compile_selector.__globals__, "type", live_type)
    if raises:
        with pytest.raises(RuntimeError) as captured:
            pipeline.to_list()
        assert captured.value is failure
        assert len(calls) == 1
    else:
        assert pipeline.to_list() == [{"id": 1, "x": 30}]
        assert len(calls) == 3
    assert all(value is record for value in calls)


@pytest.mark.parametrize("engine", ["python", "auto"])
@pytest.mark.parametrize("boundary", ["rows", "flow", "source"])
@pytest.mark.parametrize("raise_on_open", [False, True])
@pytest.mark.parametrize("replacement_kind", ["method", "code"])
def test_rows_pivot_keeps_replaced_iteration_boundaries(
    engine: str,
    boundary: str,
    raise_on_open: bool,
    replacement_kind: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pivot must consume the live opener, including its errors and iterator cleanup."""
    from fpstreams.planning.source import Source
    from fpstreams.streams.flow import Flow
    from fpstreams.tabular.rows import Rows

    rows = fpstreams.rows([{"id": 1, "metric": "reading", "value": 2}]).with_engine(engine)
    source = rows._flow._logical_plan.root.source
    owner, target, method = {
        "rows": (Rows, rows, "__iter__"),
        "flow": (Flow, rows._flow, "__iter__"),
        "source": (Source, source, "open"),
    }[boundary]
    original = getattr(owner, method)
    events = []
    failure = RuntimeError("replacement opener failed")

    def replacement_rows():
        try:
            yield {"id": 1, "metric": "reading", "value": 30}
        finally:
            events.append("close")

    def replacement(instance):
        if instance is target:
            events.append("open")
            if raise_on_open:
                raise failure
            return replacement_rows()
        return original(instance)

    if replacement_kind == "code":
        from types import FunctionType

        target_function = original
        original = FunctionType(
            original.__code__, original.__globals__, original.__name__, original.__defaults__
        )

        def replacement_code(instance):
            return _pivot_replacement(instance)  # noqa: F821 - installed in the target globals

        monkeypatch.setitem(target_function.__globals__, "_pivot_replacement", replacement)
        monkeypatch.setattr(target_function, "__code__", replacement_code.__code__)
    else:
        monkeypatch.setattr(owner, method, replacement)
    pivoted = rows.pivot(index="id", columns="metric", values="value")
    assert events == []
    if raise_on_open:
        with pytest.raises(RuntimeError) as raised:
            pivoted.to_list()
        assert raised.value is failure
        assert events == ["open"]
    else:
        assert pivoted.to_list() == [{"id": 1, "reading": 30}]
        assert events == ["open", "close"]


def test_rows_pivot_handles_general_columns_and_every_duplicate_policy() -> None:
    readings = [
        {"device": "alpha", "metric": "cpu", "reading": 3},
        {"device": "alpha", "metric": "memory", "reading": 5},
        {"device": "alpha", "metric": "disk", "reading": 7},
        {"device": "alpha", "metric": "cpu", "reading": 11},
        {"device": "beta", "metric": "memory", "reading": 13},
    ]
    original = [row.copy() for row in readings]

    def pivot(policy: str | Any) -> list[dict[str, object]]:
        return (
            fpstreams.rows(readings)
            .pivot(
                index="device",
                columns="metric",
                values="reading",
                aggregate=policy,
                fill=-1,
            )
            .to_list()
        )

    assert pivot("first") == [
        {"device": "alpha", "cpu": 3, "memory": 5, "disk": 7},
        {"device": "beta", "cpu": -1, "memory": 13, "disk": -1},
    ]
    assert pivot("last")[0]["cpu"] == 11
    assert pivot("sum")[0]["cpu"] == 14
    assert pivot(lambda left, right: left * right)[0]["cpu"] == 33
    with pytest.raises(fpstreams.DuplicateKeyError, match="multiple values for pivot key"):
        pivot("error")
    assert readings == original


def test_rows_pivot_rechecks_dynamic_aggregate_objects_per_duplicate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Construction checks once, while duplicate cells observe later __call__ drift."""
    events: list[str] = []

    class DriftingReducer:
        def __call__(self, left: int, right: int) -> int:
            events.append("reduce")
            del type(self).__call__
            return left + right

    reducer = DriftingReducer()
    canonical_callable = callable

    def tracked_callable(value: object) -> bool:
        events.append("callable")
        return canonical_callable(value)

    rows_module = sys.modules["fpstreams.tabular.rows"]
    monkeypatch.setattr(rows_module, "callable", tracked_callable, raising=False)
    pivoted = fpstreams.rows(
        [
            {"sensor": 1, "channel": "heat", "reading": 2},
            {"sensor": 1, "channel": "heat", "reading": 3},
            {"sensor": 1, "channel": "heat", "reading": 5},
        ]
    ).pivot(
        index="sensor",
        columns="channel",
        values="reading",
        aggregate=reducer,
    )

    assert events == ["callable"]
    assert pivoted.to_list() == [{"sensor": 1, "heat": 5}]
    assert events == ["callable", "callable", "reduce", "callable"]


def test_rows_pivot_truth_tests_callable_validation_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The constructor normalizes one dynamic callable result without testing it twice."""
    events: list[str] = []

    class CallableDecision:
        def __bool__(self) -> bool:
            events.append("bool")
            if len(events) > 1:
                raise RuntimeError("callable result tested twice")
            return True

    decision = CallableDecision()
    rows_module = sys.modules["fpstreams.tabular.rows"]
    monkeypatch.setattr(rows_module, "callable", lambda _value: decision, raising=False)

    fpstreams.rows([{"sensor": 1, "channel": "heat", "reading": 2}]).pivot(
        index="sensor",
        columns="channel",
        values="reading",
        aggregate=lambda left, right: left + right,
    )

    assert events == ["bool"]


@pytest.mark.parametrize(
    ("index_selector", "key_name"),
    [("sensor", "sensor"), (lambda row: row["sensor"], "key_0")],
    ids=["direct", "compatible"],
)
def test_rows_pivot_exact_reducer_observes_dynamic_callable_global(
    monkeypatch: pytest.MonkeyPatch,
    index_selector: object,
    key_name: str,
) -> None:
    """A cached exact function is used only while the module callable remains canonical."""
    pivoted = fpstreams.rows(
        [
            {"sensor": 1, "channel": "heat", "reading": 2},
            {"sensor": 1, "channel": "heat", "reading": 3},
        ]
    ).pivot(
        index=index_selector,  # type: ignore[arg-type]
        columns="channel",
        values="reading",
        aggregate=lambda left, right: left + right,
    )
    rows_module = sys.modules["fpstreams.tabular.rows"]
    monkeypatch.setattr(rows_module, "callable", lambda _value: False, raising=False)

    assert pivoted.to_list() == [{key_name: 1, "heat": 2}]


def test_rows_pivot_nonexact_key_names_keep_construction_and_runtime_hash_order() -> None:
    """A derived str subclass stays out of the seen-column set specialization."""
    events: list[str] = []

    class DerivedName(str):
        def __hash__(self) -> int:
            events.append("hash")
            if events.count("hash") == 3:
                raise RuntimeError("third derived-name hash")
            return str.__hash__(self)

        def __eq__(self, other: object) -> bool:
            events.append("eq")
            return str.__eq__(self, other)

    derived_name = DerivedName("sensor")

    class Selector(str):
        def split(self, *_args: object, **_options: object) -> list[DerivedName]:
            return [derived_name]

    def select_value(row: Mapping[str, object]) -> object:
        events.append("value")
        return row["reading"]

    pivoted = fpstreams.rows([{"sensor": 1, "channel": "heat", "reading": 2}]).pivot(
        index=Selector("sensor"),
        columns="channel",
        values=select_value,
    )

    assert events == ["hash", "hash"]
    with pytest.raises(RuntimeError, match="third derived-name hash"):
        pivoted.to_list()
    assert events == ["hash", "hash", "eq", "value", "hash"]


def test_rows_pivot_direct_gate_uses_saved_builtin_str_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A replacement module str cannot admit its instances to direct-field containment checks."""

    class Selector(str):
        def __contains__(self, _value: object) -> bool:
            raise RuntimeError("replacement selector containment")

    rows_module = sys.modules["fpstreams.tabular.rows"]
    monkeypatch.setattr(rows_module, "str", Selector, raising=False)

    fpstreams.rows([{"sensor": 1, "channel": "heat", "reading": 2}]).pivot(
        index=Selector("sensor"),
        columns=Selector("channel"),
        values=Selector("reading"),
    )


@pytest.mark.parametrize("direct_selectors", [False, True], ids=["compatible", "direct"])
def test_rows_pivot_uses_saved_builtin_str_identity_for_seen_columns(
    monkeypatch: pytest.MonkeyPatch,
    direct_selectors: bool,
) -> None:
    """A dynamic str result stays on the ordered list path before value selection and hashing."""
    events: list[str] = []

    class ColumnName(str):
        def __eq__(self, _other: object) -> bool:
            events.append("eq")
            return False

        def __hash__(self) -> int:
            events.append("hash")
            raise RuntimeError("column hash")

    class DirectRecord(Mapping[str, object]):
        def __init__(self) -> None:
            self.values = {"sensor": 1, "channel": "heat", "reading": 2}

        def __getitem__(self, key: str) -> object:
            if key == "reading":
                events.append("value")
            return self.values[key]

        def __iter__(self) -> Iterator[str]:
            return iter(self.values)

        def __len__(self) -> int:
            return len(self.values)

    if direct_selectors:
        pivoted = fpstreams.rows([DirectRecord()]).pivot(
            index="sensor",
            columns="channel",
            values="reading",
        )
    else:
        record = {"sensor": 1, "channel": "heat", "reading": 2}

        def select_value(row: Mapping[str, object]) -> object:
            events.append("value")
            return row["reading"]

        pivoted = fpstreams.rows([record]).pivot(
            index=lambda row: row["sensor"],
            columns=lambda row: row["channel"],
            values=select_value,
        )

    rows_module = sys.modules["fpstreams.tabular.rows"]
    monkeypatch.setattr(rows_module, "str", ColumnName, raising=False)
    with pytest.raises(RuntimeError, match="column hash"):
        pivoted.to_list()

    assert events == ["eq", "value", "hash"]


def test_rows_pivot_preserves_non_exact_column_name_protocols() -> None:
    """A str subclass returned by __str__ keeps canonical equality and hash probes."""
    events: list[str] = []

    class ProtocolColumn(str):
        def __hash__(self) -> int:
            events.append("hash")
            return str.__hash__(self)

        def __eq__(self, other: object) -> bool:
            events.append(f"eq:{other}")
            return str.__eq__(self, other)

    class ColumnValue:
        def __str__(self) -> str:
            events.append("str")
            return ProtocolColumn("metric")

    def select_column(row: Mapping[str, object]) -> object:
        return row["column"]

    for column_selector in ("column", select_column):
        events.clear()
        result = (
            fpstreams.rows([{"id": 1, "column": ColumnValue(), "value": 2}])
            .pivot(
                index="id",
                columns=column_selector,
                values="value",
            )
            .to_list()
        )

        assert events == ["str", "eq:id", "hash", "hash", "hash", "hash"]
        assert result == [{"id": 1, "metric": 2}]


@pytest.mark.parametrize(
    ("collision_field", "index"),
    [
        ("site", "site"),
        ("rack", ("site", "rack")),
        ("metric", ("site", "rack")),
        ("reading", ("site", "rack")),
    ],
)
def test_rows_pivot_wraps_exact_dict_key_protocol_errors(
    collision_field: str,
    index: str | tuple[str, ...],
) -> None:
    """Direct field lookup keeps compile_selector's exception translation and order."""

    class CollidingKey:
        armed = False

        def __hash__(self) -> int:
            return hash(collision_field)

        def __eq__(self, _other: object) -> bool:
            if self.armed:
                raise TypeError(f"collision at {collision_field}")
            return False

    collision = CollidingKey()
    record: dict[object, object] = {collision: "unselected"}
    record.update({"site": "north", "rack": 1, "metric": "cpu", "reading": 2})
    collision.armed = True

    with pytest.raises(fpstreams.SelectionError, match=rf"failed at '{collision_field}'") as error:
        fpstreams.rows([record]).pivot(
            index=index,
            columns="metric",
            values="reading",
        ).to_list()

    assert isinstance(error.value.__cause__, TypeError)
    assert str(error.value.__cause__) == f"collision at {collision_field}"


def test_rows_pivot_does_not_translate_column_stringification_errors() -> None:
    """Only selector lookup failures become SelectionError, not a value's __str__ error."""
    failure = KeyError("column formatting failed")

    class ColumnValue:
        def __str__(self) -> str:
            raise failure

    with pytest.raises(KeyError) as error:
        fpstreams.rows([{"id": 1, "metric": ColumnValue(), "reading": 2}]).pivot(
            index="id",
            columns="metric",
            values="reading",
        ).to_list()

    assert error.value is failure


def test_rows_pivot_single_index_preserves_tuple_hash_collisions() -> None:
    """The direct field path keeps canonical tuple hashing and equality callbacks."""
    failure = RuntimeError("pivot index equality")

    class CollidingTupleHash:
        def __init__(self, value_hash: int) -> None:
            self.value_hash = value_hash

        def __hash__(self) -> int:
            return self.value_hash

        def __eq__(self, _other: object) -> bool:
            raise failure

    first = CollidingTupleHash(-8_496_733_470_247_235_670)
    second = CollidingTupleHash(1_137_828_717_814_758_821)
    assert hash(first) != hash(second)
    assert hash((first,)) == hash((second,))

    with pytest.raises(RuntimeError) as error:
        fpstreams.rows(
            [
                {"group": first, "metric": "left", "reading": 1},
                {"group": second, "metric": "right", "reading": 2},
            ]
        ).pivot(index="group", columns="metric", values="reading").to_list()

    assert error.value is failure


@pytest.mark.parametrize(
    ("index_selector", "key_name"),
    [("habitat", "habitat"), (lambda row: row["habitat"], "key_0")],
    ids=["direct-field", "callable"],
)
def test_rows_pivot_single_index_preserves_dynamic_constructors_and_mapping_protocols(
    monkeypatch: pytest.MonkeyPatch,
    index_selector: object,
    key_name: str,
) -> None:
    """Both pivot evaluators keep dynamic globals and Mapping access order."""
    events: list[str] = []

    class HabitatRecord(Mapping[str, object]):
        def __init__(self, values: dict[str, object]) -> None:
            self.values = values

        def __getitem__(self, key: str) -> object:
            events.append(key)
            return self.values[key]

        def __iter__(self) -> Iterator[str]:
            return iter(self.values)

        def __len__(self) -> int:
            return len(self.values)

    canonical_dict = dict
    canonical_zip = zip

    def tracked_dict(*args: object, **options: object) -> dict[object, object]:
        events.append("dict")
        return canonical_dict(*args, **options)

    def tracked_zip(*iterables: object, **options: object) -> object:
        events.append("zip")
        return canonical_zip(*iterables, **options)

    rows_module = sys.modules["fpstreams.tabular.rows"]
    monkeypatch.setattr(rows_module, "dict", tracked_dict, raising=False)
    monkeypatch.setattr(rows_module, "zip", tracked_zip, raising=False)
    records = [
        HabitatRecord({"habitat": "marsh", "season": "spring", "population": 4}),
        HabitatRecord({"habitat": "marsh", "season": "autumn", "population": 7}),
    ]

    assert fpstreams.rows(records).pivot(
        index=index_selector,  # type: ignore[arg-type]
        columns="season",
        values="population",
        fill=0,
    ).to_list() == [{key_name: "marsh", "spring": 4, "autumn": 7}]
    assert events == [
        "habitat",
        "season",
        "population",
        "habitat",
        "season",
        "population",
        "zip",
        "dict",
    ]


def test_rows_pivot_preserves_selector_protocols_and_multi_index_order() -> None:
    events: list[str] = []

    class VirtualDict(dict[str, object]):
        def __getitem__(self, key: str) -> object:
            events.append(f"dict:{key}")
            return super().__getitem__(key)

    class ProtocolMapping(Mapping[str, object]):
        def __init__(self, values: dict[str, object]) -> None:
            self.values = values

        def __getitem__(self, key: str) -> object:
            events.append(f"mapping:{key}")
            return self.values[key]

        def __iter__(self) -> Iterator[str]:
            return iter(self.values)

        def __len__(self) -> int:
            return len(self.values)

    records: list[Mapping[str, object]] = [
        {"site": "north", "rack": 1, "metric": "cpu", "reading": 2},
        VirtualDict(site="north", rack=1, metric="memory", reading=4),
        ProtocolMapping({"site": "south", "rack": 2, "metric": "disk", "reading": 8}),
    ]

    assert fpstreams.rows(records).pivot(
        index=("site", "rack"),
        columns="metric",
        values="reading",
        fill=0,
    ).to_list() == [
        {"site": "north", "rack": 1, "cpu": 2, "memory": 4, "disk": 0},
        {"site": "south", "rack": 2, "cpu": 0, "memory": 0, "disk": 8},
    ]
    assert events == [
        "dict:site",
        "dict:rack",
        "dict:metric",
        "dict:reading",
        "mapping:site",
        "mapping:rack",
        "mapping:metric",
        "mapping:reading",
    ]

    events.clear()
    mixed_records: list[Mapping[str, object]] = [
        {"site": "shared", "rack": 1, "metric": "cpu", "reading": 2},
        VirtualDict(site="shared", rack=9, metric="memory", reading=4),
        ProtocolMapping({"site": "other", "rack": 3, "metric": "disk", "reading": 8}),
    ]
    assert fpstreams.rows(mixed_records).pivot(
        index="site", columns="metric", values="reading", fill=0
    ).to_list() == [
        {"site": "shared", "cpu": 2, "memory": 4, "disk": 0},
        {"site": "other", "cpu": 0, "memory": 0, "disk": 8},
    ]
    assert events == [
        "dict:site",
        "dict:metric",
        "dict:reading",
        "mapping:site",
        "mapping:metric",
        "mapping:reading",
    ]

    with pytest.raises(fpstreams.SelectionError, match="failed at 'reading'") as captured:
        fpstreams.rows([{"site": "north", "metric": "cpu"}]).pivot(
            index="site", columns="metric", values="reading"
        ).to_list()
    assert isinstance(captured.value.__cause__, KeyError)

    with pytest.raises(ValueError, match="collides with an index column"):
        fpstreams.rows([{"site": "north", "metric": "site", "reading": 1}]).pivot(
            index="site", columns="metric", values="reading"
        ).to_list()


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


def test_explode_exact_dict_sink_preserves_owned_outputs_and_record_protocol_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exact dictionaries use owned snapshots while other records retain conversion."""
    rows_module = sys.modules["fpstreams.tabular.rows"]
    canonical_as_record = rows_module._as_record
    events: list[tuple[str, object]] = []

    def tracked_as_record(row: object) -> dict[str, object]:
        events.append(("convert", type(row)))
        return canonical_as_record(row)

    monkeypatch.setattr(rows_module, "_as_record", tracked_as_record)

    source = {"id": 1, "tags": [10, 20]}
    result = fpstreams.rows([source]).explode("tags").to_list()

    assert result == [{"id": 1, "tags": 10}, {"id": 1, "tags": 20}]
    assert source == {"id": 1, "tags": [10, 20]}
    assert events == []
    assert result[0] is not source
    assert result[0] is not result[1]
    result[0]["id"] = 99
    assert result[1]["id"] == 1

    events.clear()
    mutating_source: dict[str, object] = {"state": "initial"}

    class MutatingValues:
        def __iter__(self) -> Iterator[int]:
            events.append(("iterate:first", mutating_source["state"]))
            mutating_source["state"] = "after-first"
            yield 1
            events.append(("iterate:second", mutating_source["state"]))
            mutating_source["state"] = "after-second"
            yield 2

    mutating_source["tags"] = MutatingValues()
    assert fpstreams.rows([mutating_source]).explode("tags").to_list() == [
        {"state": "initial", "tags": 1},
        {"state": "initial", "tags": 2},
    ]
    assert events == [
        ("iterate:first", "initial"),
        ("iterate:second", "after-first"),
    ]
    assert mutating_source["state"] == "after-second"

    events.clear()
    nested = {"payload": {"tags": [1, 2]}}
    assert fpstreams.rows([nested]).explode("payload.tags", into="tag").to_list() == [
        {"payload": {"tags": [1, 2]}, "tag": 1},
        {"payload": {"tags": [1, 2]}, "tag": 2},
    ]
    assert events == []

    events.clear()

    def select_tags(row: Mapping[str, object]) -> object:
        events.append(("select", type(row)))
        return row["tags"]

    assert fpstreams.rows([source]).explode(select_tags, into="tag").to_list() == [
        {"id": 1, "tags": [10, 20], "tag": 10},
        {"id": 1, "tags": [10, 20], "tag": 20},
    ]
    assert events == [("select", dict)]

    class DictSubclass(dict[str, object]):
        pass

    class CustomMapping(Mapping[str, object]):
        def __init__(self, values: dict[str, object]) -> None:
            self.values = values

        def __getitem__(self, key: str) -> object:
            return self.values[key]

        def __iter__(self) -> Iterator[str]:
            return iter(self.values)

        def __len__(self) -> int:
            return len(self.values)

    events.clear()
    subclass = DictSubclass(tags=[1])
    mapping = CustomMapping({"tags": [2]})
    assert fpstreams.rows([subclass, mapping]).explode("tags").to_list() == [
        {"tags": 1},
        {"tags": 2},
    ]
    assert events == [("convert", DictSubclass), ("convert", CustomMapping)]


def test_materialized_explode_reuses_its_snapshot_without_aliasing_outputs() -> None:
    """The exact-dict sink avoids a redundant copy while retaining owned results."""
    source = {"id": 1, "tags": [10, 20]}
    query = fpstreams.rows([source]).explode("tags")
    expansion = query._flow._pipeline.operations[-1].function
    output: list[dict[str, object]] = []
    copy_calls = 0

    def profile(_frame: object, event: str, argument: object) -> None:
        nonlocal copy_calls
        if (
            event == "c_call"
            and getattr(argument, "__name__", None) == "copy"
            and type(getattr(argument, "__self__", None)) is dict
        ):
            copy_calls += 1

    previous_profile = sys.getprofile()
    try:
        sys.setprofile(profile)
        expansion.extend_materialized(output, iter([source]))
    finally:
        sys.setprofile(previous_profile)

    assert copy_calls == 2
    assert output == [{"id": 1, "tags": 10}, {"id": 1, "tags": 20}]
    assert output[0] is not source
    assert output[0] is not output[1]
    output[0]["id"] = 99
    assert output[1]["id"] == 1


def test_materialized_explode_keeps_partial_output_when_field_assignment_raises() -> None:
    """A later output-field error preserves earlier owned rows and the original exception."""
    failure = RuntimeError("second output failed")

    class RaisingName(str):
        calls = 0

        def __hash__(self) -> int:
            self.calls += 1
            if self.calls == 2:
                raise failure
            return super().__hash__()

    source = {"id": 1, "tags": [10, 20]}
    query = fpstreams.rows([source]).explode("tags", into=RaisingName("tag"))
    expansion = query._flow._pipeline.operations[-1].function
    output: list[dict[str, object]] = []

    with pytest.raises(RuntimeError) as captured:
        expansion.extend_materialized(output, iter([source]))

    assert captured.value is failure
    assert output == [{"id": 1, "tags": [10, 20], "tag": 10}]
    assert output[0] is not source
    source["id"] = 99
    assert output[0]["id"] == 1


def test_materialized_explode_keeps_overwritten_snapshot_values_alive() -> None:
    """An `into` overwrite retains the original snapshot until its row finishes expanding."""

    class Marker:
        pass

    marker = Marker()
    reference = weakref.ref(marker)
    source: dict[str, object] = {"replaced": marker}
    del marker
    retained_during_iteration: list[bool] = []

    class Values:
        def __iter__(self) -> Iterator[int]:
            yield 10
            source.pop("replaced")
            gc.collect()
            retained_during_iteration.append(reference() is not None)
            yield 20

    source["tags"] = Values()
    assert fpstreams.rows([source]).explode("tags", into="replaced").to_list() == [
        {"tags": source["tags"], "replaced": 10},
        {"tags": source["tags"], "replaced": 20},
    ]

    assert retained_during_iteration == [True]
    gc.collect()
    assert reference() is None


def test_explode_direct_field_bypasses_generated_selector_for_exact_dicts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fully materialized exact dictionaries read top-level fields without a callback frame."""
    rows_module = sys.modules["fpstreams.tabular.rows"]
    original_compile = rows_module.compile_selector
    selected_rows: list[object] = []

    def compile_tracked(selector: object) -> Any:
        compiled = original_compile(selector)

        def tracked(row: object) -> object:
            selected_rows.append(row)
            return compiled(row)

        return tracked

    class DictSubclass(dict[str, object]):
        pass

    exact = {"id": 1, "tags": [10]}
    fallback = DictSubclass(id=2, tags=[20])
    monkeypatch.setattr(rows_module, "compile_selector", compile_tracked)

    assert fpstreams.rows([exact, fallback]).explode("tags").to_list() == [
        {"id": 1, "tags": 10},
        {"id": 2, "tags": 20},
    ]
    assert selected_rows == [fallback]


def test_explode_snapshots_an_input_row_before_yielding_any_outputs() -> None:
    """Mutating the source between pulls cannot leak into later expanded records."""
    source = {"id": 1, "tags": [10, 20]}
    expanded = iter(fpstreams.rows([source]).explode("tags"))

    assert next(expanded) == {"id": 1, "tags": 10}
    source["id"] = 99
    assert next(expanded) == {"id": 1, "tags": 20}


def test_explode_custom_output_name_cannot_mutate_between_materialized_copies() -> None:
    source = {"id": 1, "tags": [10, 20]}

    class MutatingName(str):
        def __hash__(self) -> int:
            source["id"] = 99
            return super().__hash__()

    assert fpstreams.rows([source]).explode("tags", into=MutatingName("tag")).to_list() == [
        {"id": 1, "tags": [10, 20], "tag": 10},
        {"id": 1, "tags": [10, 20], "tag": 20},
    ]

    holder: dict[str, dict[object, object]] = {}

    class MutatingKey(str):
        __hash__ = str.__hash__

        def __eq__(self, other: object) -> bool:
            holder["row"]["id"] = 99
            return super().__eq__(other)

    protocol_key = MutatingKey("tags")
    protocol_row: dict[object, object] = {"id": 1, protocol_key: [30, 40]}
    holder["row"] = protocol_row
    assert fpstreams.rows([protocol_row]).explode("tags").to_list() == [
        {"id": 1, "tags": 30},
        {"id": 1, "tags": 40},
    ]


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
        fpstreams.rows([{"name": "outer", "profile": {"name": "inner"}}] * 64).unnest(
            "profile"
        ).to_list()
    with pytest.raises(fpstreams.SelectionError, match="missing"):
        fpstreams.rows([{"id": 1}] * 64).unnest("profile").to_list()


def test_unnest_native_prefix_replays_one_protocol_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A compatible prefix stays native while the first protocol row is processed once."""
    from types import MappingProxyType

    from fpstreams import _native

    endpoint = _native.unnest_exact_dict_prefix_v1
    boundaries: list[tuple[int, object, bool]] = []

    def tracked(
        output: list[object],
        source: Iterator[object],
        column: str,
        prefix: str,
    ) -> tuple[object | None, bool] | None:
        result = endpoint(output, source, column, prefix)
        if result is not None:
            boundaries.append((len(output), result[0], result[1]))
        return result

    monkeypatch.setattr(_native, "unnest_exact_dict_prefix_v1", tracked)
    assert fpstreams.rows([{"id": 0, "profile": {"label": "small"}}]).unnest(
        "profile", prefix="nested_"
    ).to_list() == [{"id": 0, "nested_label": "small"}]
    assert boundaries == []

    boundary = MappingProxyType({"id": 2, "profile": {"label": "second"}})
    prefix_rows: list[object] = [
        {"id": index, "profile": {"label": f"prefix-{index}"}} for index in range(64)
    ]
    records: list[object] = [
        *prefix_rows,
        boundary,
        {"id": 3, "profile": {"label": "third"}},
    ]

    assert fpstreams.rows(records).unnest("profile", prefix="nested_").to_list() == [
        *[{"id": index, "nested_label": f"prefix-{index}"} for index in range(64)],
        {"id": 2, "nested_label": "second"},
        {"id": 3, "nested_label": "third"},
    ]
    assert boundaries == [(64, boundary, False)]


def test_unnest_native_prefix_is_used_only_by_auto(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Forced Python keeps the canonical iterator without crossing into Rust."""
    from fpstreams import _native

    endpoint = _native.unnest_exact_dict_prefix_v1
    native_calls = 0

    def tracked(*arguments: object) -> object:
        nonlocal native_calls
        native_calls += 1
        return endpoint(*arguments)

    monkeypatch.setattr(_native, "unnest_exact_dict_prefix_v1", tracked)
    records = [
        {"id": index, "profile": {"left": index + 1, "right": index + 2}} for index in range(256)
    ]
    query = fpstreams.rows(records).unnest("profile", prefix="nested_")

    automatic = query.to_list()
    canonical = query.with_engine("python").to_list()

    assert automatic == canonical
    assert native_calls == 1


def test_unnest_materialization_releases_protocol_values_before_next_source_pull() -> None:
    """A callback temporary must die before a live list iterator asks for its next row."""

    def consume(*, materialized: bool) -> tuple[list[dict[str, Any]], list[str]]:
        source: list[dict[str, Any]] = []
        events: list[str] = []
        row: dict[str, Any] = {}

        class NestedRecord:
            def _asdict(self) -> dict[str, str]:
                events.append("asdict")
                row.pop("profile")
                return {"label": "first"}

            def __del__(self) -> None:
                events.append("del")
                source.append({"id": 2, "profile": {"label": "appended"}})

        row.update(id=1, profile=NestedRecord())
        source.append(row)
        query = fpstreams.rows(source).unnest("profile", prefix="nested_").with_engine("python")
        result = query.to_list() if materialized else list(query)
        return result, events

    expected = [
        {"id": 1, "nested_label": "first"},
        {"id": 2, "nested_label": "appended"},
    ]
    assert consume(materialized=False) == (expected, ["asdict", "del"])
    assert consume(materialized=True) == (expected, ["asdict", "del"])


def test_unnest_materialization_preserves_freed_callback_local_events() -> None:
    """A freed PEP 669 local event on the canonical callback must force canonical execution."""
    monitoring = getattr(sys, "monitoring", None)
    if monitoring is None:
        pytest.skip("sys.monitoring requires Python 3.12+")
    if sys.version_info[:2] not in {(3, 12), (3, 13)}:
        pytest.skip("free_tool_id preserves local monitoring only on Python 3.12/3.13")

    tool_id = next(
        (
            candidate
            for candidate in range(monitoring.OPTIMIZER_ID + 1)
            if monitoring.get_tool(candidate) is None
        ),
        None,
    )
    if tool_id is None:
        pytest.skip("no free sys.monitoring tool id")

    query = (
        fpstreams.rows([{"id": 1, "profile": {"label": "first"}}])
        .unnest("profile", prefix="nested_")
        .with_engine("python")
    )
    callback = query._flow._pipeline.operations[0].function
    callback_code = callback.__code__
    observed: list[str] = []

    def observe(code: object, _instruction_offset: int) -> None:
        if code is callback_code:
            observed.append("PY_START")

    event = monitoring.events.PY_START
    monitoring.use_tool_id(tool_id, "fpstreams unnest materialization regression")
    monitoring.register_callback(tool_id, event, observe)
    monitoring.set_local_events(tool_id, callback_code, event)
    monitoring.free_tool_id(tool_id)
    try:
        assert list(query) == [{"id": 1, "nested_label": "first"}]
        assert observed == ["PY_START"]
        observed.clear()
        assert query.to_list() == [{"id": 1, "nested_label": "first"}]
    finally:
        monitoring.use_tool_id(tool_id, "fpstreams unnest materialization cleanup")
        monitoring.set_local_events(tool_id, callback_code, 0)
        monitoring.register_callback(tool_id, event, None)
        monitoring.free_tool_id(tool_id)

    assert observed == ["PY_START"]


@pytest.mark.parametrize(
    "boundary",
    [
        {"id": 2},
        {"id": 2, "nested_label": "outer", "profile": {"label": "inner"}},
        {"id": 2, "profile": {object(): "protocol key"}},
        {object(): "protocol key", "profile": {"label": "inner"}},
        {"id": 2, "profile": [("label", "mapping protocol")]},
    ],
)
@pytest.mark.parametrize("container", [list, tuple])
def test_unnest_native_prefix_returns_atomic_unprocessed_boundary(
    boundary: object,
    container: Callable[[Iterable[object]], Iterable[object]],
) -> None:
    """Missing, colliding, and noncanonical rows leave no partial native result."""
    from fpstreams import _native

    first = {"id": 1, "profile": {"label": "first"}}
    tail = {"id": 3, "profile": {"label": "third"}}
    source = iter(container([first, boundary, tail]))
    output: list[object] = []

    result = _native.unnest_exact_dict_prefix_v1(output, source, "profile", "nested_")

    assert result is not None
    assert result[0] is boundary
    assert result[1] is False
    assert output == [{"id": 1, "nested_label": "first"}]
    assert next(source) is tail


@pytest.mark.skipif(not hasattr(signal, "SIGALRM"), reason="SIGALRM is unavailable")
@pytest.mark.parametrize("wide_part", ["outer", "nested"])
@pytest.mark.parametrize("first_signal", ["normal", "before-pull", "after-append"])
def test_unnest_native_wide_row_checks_signals_before_append(
    wide_part: str, first_signal: str
) -> None:
    """A pending signal interrupts either dictionary scan while its private row is atomic."""
    import dis
    from types import FrameType

    from fpstreams import _native

    wide = {f"field_{index}": index for index in range(250_000)}
    row = {"profile": wide} if wide_part == "nested" else {"profile": {}, **wide}
    tail = object()
    previous_handler = signal.getsignal(signal.SIGALRM)
    source = iter([row, tail])
    output: list[object] = []

    def invoke() -> tuple[object | None, bool] | None:
        return _native.unnest_exact_dict_prefix_v1(output, source, "profile", "")

    # CPython 3.11 can execute a warmed built-in call at PRECALL.
    call_offsets = {
        instruction.offset
        for instruction in dis.get_instructions(invoke)
        if instruction.opname in {"PRECALL", "CALL"}
    }

    class WideRowSignal(Exception):
        pass

    def interrupt(_signum: int, frame: FrameType | None) -> None:
        if (
            frame is None
            or frame.f_code is not invoke.__code__
            or frame.f_lasti not in call_offsets
            or source.__length_hint__() != 1
            or output
        ):
            return
        raise WideRowSignal

    signal.signal(signal.SIGALRM, interrupt)
    try:
        for attempt in range(10):
            source = iter([row, tail])
            output = []
            missed_control = attempt == 0 and first_signal != "normal"
            if missed_control and first_signal == "before-pull":
                signal.raise_signal(signal.SIGALRM)
            delay = (
                0.0 if missed_control else (0.001 if wide_part == "outer" else 0.005) / (2**attempt)
            )
            signal.setitimer(signal.ITIMER_REAL, delay)
            try:
                outcome = invoke()
                if missed_control and first_signal == "after-append":
                    signal.raise_signal(signal.SIGALRM)
            except WideRowSignal:
                assert output == []
                assert next(source) is tail
                break
            finally:
                signal.setitimer(signal.ITIMER_REAL, 0.0)
            # Retry only a missed row window, never an incorrect result or source position.
            assert output == [wide]
            assert outcome is not None
            assert outcome[0] is tail
            assert outcome[1] is False
            assert source.__length_hint__() == 0
        else:
            pytest.fail("signal missed the private unnest row in all 10 attempts")
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0.0)
        signal.signal(signal.SIGALRM, previous_handler)


@pytest.mark.parametrize("container", [list, tuple])
@pytest.mark.parametrize(
    ("prefix", "name"),
    [
        ("", ""),
        ("", "x"),
        ("", "é"),
        ("", "\0"),
        ("", "漢"),
        ("", "💩"),
        ("", "\ud800"),
        ("", "x" * 31),
        ("", "漢" * 32),
        ("", "💩" * 33),
        ("", "\ud800" * 127),
        ("é", ""),
        ("nested_", "label"),
    ],
)
def test_unnest_native_keys_match_fstring_storage_and_identity(
    prefix: str,
    name: str,
    container: Callable[[Iterable[object]], Iterable[object]],
) -> None:
    """Generated keys keep BUILD_STRING value, width, and fresh-object behavior."""
    from fpstreams import _native

    rows = container({"profile": {name: index}} for index in range(3))
    output: list[dict[str, int]] = []

    result = _native.unnest_exact_dict_prefix_v1(output, iter(rows), "profile", prefix)

    assert result == (None, True)
    actual = [next(iter(row)) for row in output]
    expected = [f"{prefix}{name}" for _index in output]
    assert actual == expected
    assert [type(key) for key in actual] == [type(key) for key in expected]
    assert [sys.getsizeof(key) for key in actual] == [sys.getsizeof(key) for key in expected]
    assert [key is prefix for key in actual] == [key is prefix for key in expected]
    assert [key is name for key in actual] == [key is name for key in expected]
    assert [left is right for left in actual for right in actual] == [
        left is right for left in expected for right in expected
    ]


class _UnnestEffectMapping(Mapping[str, int]):
    """Expose one observable callback while converting a nested Mapping."""

    def __init__(self, effect: Callable[[], None]) -> None:
        self._effect = effect

    def __iter__(self) -> Iterator[str]:
        self._effect()
        return iter(("x",))

    def __len__(self) -> int:
        return 1

    def __getitem__(self, key: str) -> int:
        if key != "x":
            raise KeyError(key)
        return 1


def _unnest_test_callback(query: object) -> Any:
    """Read the retained callback from one test query."""
    return cast(Any, query)._flow._pipeline.operations[0].function


def _unnest_prefix_cell(callback: Any) -> Any:
    """Find the prefix cell without depending on closure ordering."""
    return dict(
        zip(
            callback.__code__.co_freevars,
            callback.__closure__ or (),
            strict=True,
        )
    )["prefix"]


@pytest.mark.parametrize("engine", ["python", "auto"])
def test_unnest_protocol_boundary_preserves_live_closure(engine: str) -> None:
    """A protocol row can change the prefix observed by itself and the canonical tail."""
    source: list[dict[str, Any]] = [
        {"id": index, "profile": {"x": index}} for index in range(1_024)
    ]
    query = fpstreams.rows(source).unnest("profile", prefix="old_").with_engine(engine)
    prefix_cell = _unnest_prefix_cell(_unnest_test_callback(query))

    def mutate_prefix() -> None:
        prefix_cell.cell_contents = "new_"

    source.extend(
        [
            {"id": 1_024, "profile": _UnnestEffectMapping(mutate_prefix)},
            {"id": 1_025, "profile": {"x": 1_025}},
        ]
    )
    result = query.to_list()

    assert result[:2] == [{"id": 0, "old_x": 0}, {"id": 1, "old_x": 1}]
    assert result[-2:] == [
        {"id": 1_024, "new_x": 1},
        {"id": 1_025, "new_x": 1_025},
    ]


@pytest.mark.parametrize("engine", ["python", "auto"])
def test_unnest_protocol_boundary_preserves_new_trace_observer(engine: str) -> None:
    """Tracing enabled by a Mapping observes every later canonical callback."""
    source: list[dict[str, Any]] = [
        {"id": index, "profile": {"x": index}} for index in range(1_024)
    ]
    query = fpstreams.rows(source).unnest("profile", prefix="nested_").with_engine(engine)
    callback_code = _unnest_test_callback(query).__code__
    observed: list[int] = []
    previous_trace = sys.gettrace()

    def trace(frame: Any, event: str, _argument: object) -> Any:
        if event == "call" and frame.f_code is callback_code:
            observed.append(cast(dict[str, int], frame.f_locals["row"])["id"])
        return trace

    def enable_trace() -> None:
        sys.settrace(trace)

    source.append({"id": 1_024, "profile": _UnnestEffectMapping(enable_trace)})
    source.extend({"id": index, "profile": {"x": index}} for index in range(1_025, 1_088))
    try:
        result = query.to_list()
    finally:
        sys.settrace(previous_trace)

    assert len(result) == 1_088
    assert observed == list(range(1_025, 1_088))


def test_unnest_direct_sink_emits_no_function_code_audit_events() -> None:
    """Runtime provenance uses the repository's non-audited function-code reader."""
    program = r"""
import json
import os
import sys
from importlib import import_module

import fpstreams

rows_module = import_module("fpstreams.tabular.rows")

query = fpstreams.rows([{"profile": {"x": index}} for index in range(1_024)]).unnest("profile")
events = []
targets = {
    id(query._flow._pipeline.operations[0].function),
    id(rows_module._append_materialized_unnest),
    id(rows_module._materialized_unnest_spec),
}

def audit(name, arguments):
    if (
        name == "object.__getattr__"
        and len(arguments) > 1
        and arguments[1] == "__code__"
        and id(arguments[0]) in targets
    ):
        events.append(name)

sys.addaudithook(audit)
if os.environ["FPSTREAMS_AUDIT_MODE"] == "direct":
    query.to_list()
else:
    list(query)
print(json.dumps(events))
"""
    observed: dict[str, list[str]] = {}
    for mode in ("canonical", "direct"):
        environment = dict(os.environ)
        environment["FPSTREAMS_AUDIT_MODE"] = mode
        completed = _run_inline_python(program, check=True, env=environment)
        observed[mode] = cast(list[str], json.loads(completed.stdout))

    assert observed == {"canonical": [], "direct": []}


@pytest.mark.parametrize("source_size", [2, 64])
def test_unnest_materialization_preserves_map_stop_iteration(source_size: int) -> None:
    """Protocol StopIteration must end a map and retain its already-emitted prefix."""

    class StoppingRecord:
        def _asdict(self) -> dict[str, object]:
            raise StopIteration("record conversion stopped")

    source: list[object] = [
        {"id": 0, "profile": {"label": "first"}},
        StoppingRecord(),
    ]
    source.extend(
        {"id": index, "profile": {"label": f"tail-{index}"}} for index in range(2, source_size)
    )
    expected = [{"id": 0, "nested_label": "first"}]
    query = fpstreams.rows(source).unnest("profile", prefix="nested_")

    assert list(query) == expected
    assert query.to_list() == expected
