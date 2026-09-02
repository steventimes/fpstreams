"""Rows expressions, joins, aggregation, reshaping, text I/O, cleaning, and spilling."""

from __future__ import annotations

import gc
import io
import json
import math
import os
import random
import signal
import sqlite3
import subprocess
import sys
import traceback
import weakref
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

import fpstreams
from fpstreams import flow

PROJECT_ROOT = Path(__file__).parents[1]


def _run_inline_python(
    script: str,
    *arguments: str,
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
    check: bool = False,
) -> subprocess.CompletedProcess[str]:
    """Run an isolated Python snippet with consistent text output capture."""
    return subprocess.run(
        [sys.executable, "-c", script, *arguments],
        cwd=cwd,
        env=env,
        check=check,
        capture_output=True,
        text=True,
    )


def _capture_rows_error(query: fpstreams.Rows[object]) -> BaseException:
    """Materialize one rows query and return its required failure."""
    try:
        query.to_list()
    except BaseException as error:
        return error
    raise AssertionError("a missing selector unexpectedly succeeded")


# --- Tests consolidated from test_rows_api.py ---


@dataclass(frozen=True)
class _CollidingKey:
    value: int

    def __hash__(self) -> int:
        return 1


class _TrackedArrowBatches(Iterator[pa.RecordBatch]):
    """Record deterministic pulls and explicit close for one custom Arrow reader source."""

    def __init__(self, batches: Sequence[pa.RecordBatch], events: list[str]) -> None:
        self._batches = tuple(batches)
        self._events = events
        self._index = 0
        self._closed = False

    def __iter__(self) -> _TrackedArrowBatches:
        return self

    def __next__(self) -> pa.RecordBatch:
        if self._index == len(self._batches):
            self._events.append("stop")
            raise StopIteration
        index = self._index
        self._index += 1
        self._events.append(f"pull:{index}")
        return self._batches[index]

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._events.append("close")


def test_rows_turns_record_etl_into_one_readable_pipeline() -> None:
    orders = [
        {"customer": "Ada", "country": "UK", "status": "paid", "amount": 20},
        {"customer": "Lin", "country": "US", "status": "open", "amount": 90},
        {"customer": "Ada", "country": "UK", "status": "paid", "amount": 30},
        {"customer": "Max", "country": "US", "status": "paid", "amount": 80},
    ]

    report = (
        fpstreams.rows(orders)
        .where(status="paid")
        .with_columns(net=lambda row: row["amount"] * 0.8)
        .group_by("country")
        .aggregate(orders=fpstreams.agg.count(), revenue=fpstreams.agg.sum("net"))
        .sort_by("revenue", reverse=True)
        .to_list()
    )

    assert report == [
        {"country": "US", "orders": 1, "revenue": 64.0},
        {"country": "UK", "orders": 2, "revenue": 40.0},
    ]


def test_with_columns_copies_exact_dicts_without_the_generic_record_adapter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from importlib import import_module

    rows_module = import_module("fpstreams.tabular.rows")

    adapted: list[object] = []
    original_as_record = rows_module._as_record

    def tracked_as_record(row: object) -> dict[str, Any]:
        adapted.append(row)
        return original_as_record(row)

    class DictSubclass(dict[str, int]):
        pass

    exact = {"value": 1}
    fallback = DictSubclass(value=2)
    monkeypatch.setattr(rows_module, "_as_record", tracked_as_record)

    result = (
        fpstreams.rows([exact, fallback])
        .with_columns(doubled=lambda row: row["value"] * 2)
        .to_list()
    )

    assert result == [
        {"value": 1, "doubled": 2},
        {"value": 2, "doubled": 4},
    ]
    assert adapted == [fallback]
    assert exact == {"value": 1}
    assert fallback == {"value": 2}


def test_rows_keeps_basic_table_navigation_in_the_same_chain() -> None:
    table = fpstreams.rows([{"id": 1}, {"id": 1}, {"id": 2}]).unique_by("id")

    assert table.count() == 2
    assert table.skip(1).take(1).first() == {"id": 2}
    assert fpstreams.rows([{"a": 1}, {"a": 2, "b": 3}]).to_columns() == {
        "a": [1, 2],
        "b": [None, 3],
    }


def test_rows_exposes_its_flow_and_plan_without_consuming_one_shot_input() -> None:
    table = fpstreams.rows(iter([{"id": 1}, {"id": 2}])).select("id")
    underlying = table.to_flow()

    assert table.to_flow() is underlying
    assert table.explain("list").to_dict() == underlying.explain("list").to_dict()
    assert table.to_list() == [{"id": 1}, {"id": 2}]

    with pytest.raises(fpstreams.FlowConsumedError):
        underlying.to_list()


def test_rows_concat_is_lazy_ordered_and_never_aligns_record_schemas() -> None:
    events: list[str] = []

    def middle() -> Iterator[dict[str, int]]:
        events.append("middle:open")
        try:
            yield {"middle": 2}
            events.append("middle:tail")
        finally:
            events.append("middle:close")

    def last() -> Iterator[dict[str, int]]:
        events.append("last:open")
        try:
            yield {"last": 3}
        finally:
            events.append("last:close")

    first = fpstreams.rows([{"first": 1}])
    query = first.concat(fpstreams.rows(middle()), last()).take(2)

    assert first.concat() is first
    assert events == []
    assert query.to_list() == [{"first": 1}, {"middle": 2}]
    assert events == ["middle:open", "middle:close"]

    with pytest.raises(fpstreams.FlowConsumedError):
        query.to_list()

    class CustomRows(fpstreams.Rows[dict[str, int]]):
        def __iter__(self) -> Iterator[dict[str, int]]:
            yield {"custom": 9}

    custom = CustomRows([{"hidden": 2}])
    assert first.concat(custom).to_list() == [{"first": 1}, {"custom": 9}]


def test_to_columns_consumes_evolving_rows_once_without_retaining_them() -> None:
    shared: dict[str, int] = {}

    def records():
        for record in ({"a": 1}, {"a": 2, "b": 3}, {"b": 4, "c": 5}):
            shared.clear()
            shared.update(record)
            yield shared

    expected = {
        "a": [1, 2, None],
        "b": [None, 3, 4],
        "c": [None, None, 5],
    }
    assert flow(records()).collect(fpstreams.Collectors.to_columns()) == expected
    assert fpstreams.rows(records()).to_columns() == expected


def test_identity_arrow_to_columns_never_boxes_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A retained Arrow table should transpose columns without constructing row dicts."""
    from fpstreams.planning import arrow_source as arrow_planning
    from fpstreams.tabular import arrow as arrow_adapter

    def reject_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("identity Arrow to_columns must not box rows")

    monkeypatch.setattr(arrow_planning, "batch_to_rows", reject_rows)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", reject_rows)
    table = pa.table(
        {
            "id": pa.chunked_array([[1, 2], [3]]),
            "payload": pa.chunked_array([["a"], ["b", None]]),
        }
    )

    assert fpstreams.rows.from_arrow(table, batch_size=1).to_columns() == {
        "id": [1, 2, 3],
        "payload": ["a", "b", None],
    }
    assert fpstreams.rows.from_arrow(table.slice(0, 0)).to_columns() == {}


def test_identity_arrow_to_columns_preserves_engine_and_failpoint_boundaries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Forced Python and instrumentation keep the canonical row-opening path."""
    from fpstreams.runtime.failpoints import failpoint
    from fpstreams.tabular import arrow as arrow_adapter

    converted: list[int] = []
    convert_rows = arrow_adapter.batch_to_rows

    def tracked(batch: object) -> list[dict[str, object]]:
        converted.append(batch.num_rows)  # type: ignore[attr-defined]
        return convert_rows(batch)

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", tracked)
    source = fpstreams.rows.from_arrow(pa.table({"id": [1, 2]}), batch_size=1)

    assert source.with_engine("python").to_columns() == {"id": [1, 2]}
    assert converted == [1, 1]
    with (
        failpoint("source.open.after", RuntimeError("canonical columns")),
        pytest.raises(RuntimeError, match="canonical columns"),
    ):
        source.to_columns()


def test_identity_arrow_to_columns_preserves_batch_conversion_error_order() -> None:
    """The columnar fast path must observe conversion failures one source batch at a time."""
    timestamps = pa.array([0, 10**12], type=pa.timestamp("s"))
    invalid_utf8 = pa.array([b"\xff", b"ok"], type=pa.binary()).view(pa.string())
    source = fpstreams.rows.from_arrow(
        pa.Table.from_arrays([timestamps, invalid_utf8], names=["timestamp", "text"]),
        batch_size=1,
    )

    with pytest.raises(UnicodeDecodeError):
        source.to_columns()
    with pytest.raises(UnicodeDecodeError):
        source.with_engine("python").to_columns()


def test_from_columns_is_explicit_across_rows_and_flow_without_reinterpreting_mappings() -> None:
    """Column mappings opt in explicitly while ordinary mappings keep iterable semantics."""
    columns = {"id": [1, 2], "label": ["a", "b"]}
    expected = [{"id": 1, "label": "a"}, {"id": 2, "label": "b"}]

    assert fpstreams.Rows.from_columns(columns).to_list() == expected
    assert fpstreams.rows.from_columns(columns).to_list() == expected
    assert fpstreams.flow.from_columns(columns).to_list() == expected
    assert fpstreams.rows(columns).to_list() == ["id", "label"]
    assert fpstreams.flow(columns).to_list() == ["id", "label"]


def test_from_columns_retains_numpy_buffers_for_existing_arrow_plans() -> None:
    """Independent columns should enter the mature Arrow planner without a stacked copy."""
    import numpy as np

    from fpstreams.physical.relational import GlobalAggregatePhysicalNode, SourcePhysicalNode
    from fpstreams.planning.compiler import compile_query

    keys = np.arange(32, dtype=np.int64) % 4
    values = np.arange(32, dtype=np.int64)
    source = fpstreams.rows.from_columns({"key": keys, "value": values}, batch_size=7)
    query = source.aggregate(
        rows=fpstreams.agg.count(),
        total=fpstreams.agg.sum("value"),
        low=fpstreams.agg.min("value"),
        high=fpstreams.agg.max("value"),
    )
    physical = compile_query(query._flow._query("list"))

    assert isinstance(physical.root, GlobalAggregatePhysicalNode)
    assert isinstance(physical.root.input, SourcePhysicalNode)
    table = physical.root.input.source.native_data.materialized_data
    assert table["value"].chunk(0).buffers()[1].address == values.__array_interface__["data"][0]
    assert query.explain("list").to_dict()["relations"]["candidate"] == "arrow_multi_reduce"
    assert query.to_list() == [{"rows": 32, "total": 496, "low": 0, "high": 31}]


def test_from_columns_validates_its_boundary_before_arrow_planning() -> None:
    with pytest.raises(TypeError, match=r"from_columns\(\) expects a mapping"):
        fpstreams.rows.from_columns([("value", [1])])
    with pytest.raises(TypeError, match="from_columns column names must be strings"):
        fpstreams.rows.from_columns({0: [1]})
    with pytest.raises(ValueError, match="from_columns column names cannot be empty"):
        fpstreams.rows.from_columns({"": [1]})
    with pytest.raises(pa.ArrowInvalid, match="expected length"):
        fpstreams.rows.from_columns({"left": [1], "right": [2, 3]})
    with pytest.raises(ValueError, match="batch_size must be positive"):
        fpstreams.rows.from_columns({"value": [1]}, batch_size=0)


def test_column_expressions_remove_row_lambda_noise() -> None:
    orders = [
        {"customer": "Ada", "status": "paid", "amount": 20},
        {"customer": "Lin", "status": "open", "amount": 90},
        {"customer": "Max", "status": "paid", "amount": 80},
    ]

    result = (
        fpstreams.rows(orders)
        .where((fpstreams.col("status") == "paid") & (fpstreams.col("amount") > 25))
        .with_columns(
            net=fpstreams.col("amount") * 0.8,
            band=fpstreams.when(fpstreams.col("amount") >= 75, "large", "small"),
        )
        .select("customer", "net", "band")
        .to_list()
    )

    assert result == [{"customer": "Max", "net": 64.0, "band": "large"}]


def test_structured_rows_fusion_preserves_copy_sibling_and_literal_identity() -> None:
    """Fast exact-dict stages keep the documented shallow-copy and sibling-read contract."""
    marker = object()
    payload: list[int] = [1]
    source = {"x": 2, "payload": payload}

    result = (
        fpstreams.rows([source])
        .with_columns(
            x=fpstreams.col("x") + 1,
            sibling=fpstreams.col("x"),
            marker=fpstreams.lit(marker),
        )
        .select("x", "sibling", "payload", "marker")
        .to_list()
    )

    assert source == {"x": 2, "payload": payload}
    assert result[0]["x"] == 3
    assert result[0]["sibling"] == 2
    assert result[0]["payload"] is payload
    assert result[0]["marker"] is marker


def test_structured_rows_fusion_inlines_callable_with_columns_for_exact_dicts() -> None:
    """Opaque selectors keep their callback while the surrounding row update is fused."""
    from fpstreams.execution._rows_fusion import compile_rows_fusion
    from fpstreams.execution.physical import operations_from_physical_nodes
    from fpstreams.planning.compiler import compile_query

    calls: list[int] = []

    def next_value(row: dict[str, int]) -> int:
        calls.append(row["value"])
        if row["value"] == 2:
            raise StopIteration("stop callable map")
        return row["value"] + 10

    query = fpstreams.rows([{"value": 1}]).with_columns(next_value=next_value)._flow
    physical = compile_query(query._query("list"))
    operations = operations_from_physical_nodes(physical.nodes)
    fused = compile_rows_fusion(operations)

    assert fused is not None
    source = [{"value": 1}, {"value": 2}, {"value": 3}]
    assert list(fused(iter(source))) == [{"value": 1, "next_value": 11}]
    assert source == [{"value": 1}, {"value": 2}, {"value": 3}]
    assert calls == [1, 2]

    class DictSubclass(dict[str, int]):
        pass

    calls.clear()
    assert list(fused(iter([DictSubclass(value=2), DictSubclass(value=3)]))) == []
    assert calls == [2]


def test_structured_rows_fusion_binds_external_slots_as_loop_locals() -> None:
    """Generated hot loops must not index the query slot tuple for every row."""
    import dis

    from fpstreams.execution._rows_fusion import compile_rows_fusion
    from fpstreams.execution.physical import operations_from_physical_nodes
    from fpstreams.planning.compiler import compile_query

    query = fpstreams.rows([{"value": 1}]).select("value")._flow
    physical = compile_query(query._query("list"))
    fused = compile_rows_fusion(operations_from_physical_nodes(physical.nodes))

    assert fused is not None
    assert not any(
        instruction.opname == "LOAD_GLOBAL" and instruction.argval == "_fpstreams_slots"
        for instruction in dis.get_instructions(fused)
    )


def test_structured_rows_fusion_preserves_map_exhaustion_and_lookup_translation() -> None:
    """Exact-row lowering keeps builtin map exhaustion and selector error boundaries."""

    class StopsDuringAdd:
        def __add__(self, _other: object) -> object:
            raise StopIteration("operator stopped map")

    stopped = [{"value": StopsDuringAdd()} for _index in range(384)]
    assert (
        fpstreams.rows(stopped).with_columns(next_value=fpstreams.col("value") + 1).to_list() == []
    )

    class CollidingKey:
        def __hash__(self) -> int:
            return hash("id")

        def __eq__(self, _other: object) -> bool:
            raise TypeError("collision equality")

    collision = {CollidingKey(): 1}
    with pytest.raises(fpstreams.SelectionError) as captured:
        fpstreams.rows([collision] * 384).select("id").to_list()
    assert isinstance(captured.value.__cause__, TypeError)
    assert str(captured.value.__cause__) == "collision equality"


def test_retained_direct_select_uses_one_native_pass_only_for_auto(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Auto projects exact rows natively while forced Python and source records stay untouched."""
    from fpstreams import _native

    records = [{"id": index, "value": index * 2, "hidden": index + 1} for index in range(2_500)]
    snapshot = [record.copy() for record in records]
    query = fpstreams.rows(records).select("id", amount="value")
    endpoint = _native.select_exact_dict_prefix_v1
    native_calls = 0

    def tracked(*arguments: object) -> object:
        nonlocal native_calls
        native_calls += 1
        return endpoint(*arguments)

    monkeypatch.setattr(_native, "select_exact_dict_prefix_v1", tracked)

    automatic = query.to_list()
    canonical = query.with_engine("python").to_list()
    tuple_result = fpstreams.rows(tuple(records)).select("id", amount="value").to_list()

    assert automatic == canonical
    assert tuple_result == canonical
    assert automatic[-1] == {"id": 2_499, "amount": 4_998}
    assert native_calls == 2
    assert records == snapshot

    def stopped(*_arguments: object) -> object:
        raise StopIteration("native select stopped")

    monkeypatch.setattr(_native, "select_exact_dict_prefix_v1", stopped)
    assert query.to_list() == []


def test_retained_direct_select_wraps_source_open_stop_iteration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The direct select sink preserves the canonical generator exhaustion boundary."""
    records = [{"value": index} for index in range(2_100)]
    query = fpstreams.rows(records).select("value")
    source = query._flow._pipeline.source
    stopped = StopIteration("source opener stopped")

    def stop_opening() -> Iterator[dict[str, int]]:
        raise stopped

    monkeypatch.setattr(source, "_factory", stop_opening)

    with pytest.raises(RuntimeError, match=r"^generator raised StopIteration$") as captured:
        query.to_list()

    assert captured.value.__cause__ is stopped


def test_retained_direct_select_preserves_boundary_and_missing_field_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A mixed row resumes in Python once; exact missing fields keep SelectionError and cause."""
    from fpstreams import _native

    class DictSubclass(dict[str, int]):
        def __getitem__(self, field: str) -> int:
            fallback_lookups.append(field)
            return super().__getitem__(field)

    fallback_lookups: list[str] = []
    mixed: list[dict[str, int]] = [
        *({"id": index, "value": index + 1} for index in range(2_100)),
        DictSubclass(id=2_100, value=2_101),
        {"id": 2_101, "value": 2_102},
    ]
    result = fpstreams.rows(mixed).select("id", "value").to_list()

    assert result[-2:] == [
        {"id": 2_100, "value": 2_101},
        {"id": 2_101, "value": 2_102},
    ]
    assert fallback_lookups == ["id", "value"]

    records = [{"id": index, "value": index} for index in range(2_100)]
    records[-1] = {"id": 2_099}
    query = fpstreams.rows(records).select("id", "value")
    with pytest.raises(fpstreams.SelectionError) as canonical:
        query.with_engine("python").to_list()
    with pytest.raises(fpstreams.SelectionError) as automatic:
        query.to_list()

    assert str(automatic.value) == str(canonical.value)
    assert type(automatic.value.__cause__) is type(canonical.value.__cause__) is KeyError
    assert records[-1] == {"id": 2_099}
    assert callable(_native.select_exact_dict_prefix_v1)

    class StopsLookup:
        def __hash__(self) -> int:
            return hash("value")

        def __eq__(self, _other: object) -> bool:
            raise StopIteration("mapping stopped")

    stopped = [{StopsLookup(): 1} for _index in range(2_100)]
    assert fpstreams.rows(stopped).select("value").to_list() == []

    class BrokenLookup:
        def __hash__(self) -> int:
            return hash("value")

        def __eq__(self, _other: object) -> bool:
            raise TypeError("broken equality")

    collision = {BrokenLookup(): 1}
    collision_query = fpstreams.rows([collision] * 2_100).select("value")
    with pytest.raises(fpstreams.SelectionError) as collision_error:
        collision_query.to_list()
    assert isinstance(collision_error.value.__cause__, TypeError)
    assert str(collision_error.value.__cause__) == "broken equality"


def test_retained_direct_select_deopts_for_failpoints_and_reports(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Failpoint instrumentation keeps the ordinary Python projection loop active."""
    from fpstreams import _native
    from fpstreams.runtime.failpoints import failpoint

    query = fpstreams.rows([{"id": index} for index in range(2_100)]).select("id")

    def forbidden(*_arguments: object) -> object:
        raise AssertionError("instrumented select must stay on Python")

    monkeypatch.setattr(_native, "select_exact_dict_prefix_v1", forbidden)
    with failpoint("unrelated.select.transition", RuntimeError("unused")):
        assert len(query.to_list()) == 2_100

    monkeypatch.undo()
    execution = query.run_with_report("to_list")
    assert len(execution.value) == 2_100
    assert execution.report.compiler_engine == "python"
    assert execution.report.strategy == "rust_direct"


def test_structured_rows_fusion_inlines_cast_and_fill_without_changing_value_rules() -> None:
    """Cast sees evolving copies while fill keeps callable literals and row expressions distinct."""
    from fpstreams.execution._rows_fusion import compile_rows_fusion
    from fpstreams.execution.physical import operations_from_physical_nodes
    from fpstreams.planning.compiler import compile_query

    def marker() -> str:
        return "literal callable"

    query = (
        fpstreams.rows([{"value": "2", "fallback": None}])
        .cast(value=int)
        .fill_nulls(fallback=fpstreams.col("value"), marker=marker)
    )
    physical = compile_query(query._flow._query("list"))
    fused = compile_rows_fusion(operations_from_physical_nodes(physical.nodes))

    assert fused is not None
    source = [{"value": "2", "fallback": None}]
    assert list(fused(iter(source))) == [{"value": 2, "fallback": 2, "marker": marker}]
    assert source == [{"value": "2", "fallback": None}]

    class DictSubclass(dict[str, object]):
        pass

    assert list(fused(iter([DictSubclass(value="3", fallback=None)]))) == [
        {"value": 3, "fallback": 3, "marker": marker}
    ]

    calls: list[str] = []

    def convert(value: object) -> int:
        calls.append(str(value))
        return int(value)

    with pytest.raises(fpstreams.SelectionError, match="cast column 'missing' is missing"):
        fpstreams.rows([{"value": "4"}] * 384).cast(
            value=convert,
            missing=int,
        ).to_list()
    assert calls == ["4"]


def test_structured_rows_fusion_preserves_lookup_and_operator_error_boundaries() -> None:
    """Only selector lookup failures become SelectionError; user operator KeyError stays raw."""
    with pytest.raises(fpstreams.SelectionError) as missing:
        (
            fpstreams.rows([{"x": 1}])
            .with_columns(score=fpstreams.col("missing") + 1)
            .select("score")
            .to_list()
        )
    assert isinstance(missing.value.__cause__, KeyError)

    class RaisesKeyError:
        def __mul__(self, _other: object) -> object:
            raise KeyError("operator-owned")

    with pytest.raises(KeyError, match="operator-owned") as operator_error:
        (
            fpstreams.rows([{"x": RaisesKeyError()}])
            .with_columns(score=fpstreams.col("x") * 3)
            .select("score")
            .to_list()
        )
    assert operator_error.value.__cause__ is None


def test_structured_rows_fusion_falls_back_for_protocol_sensitive_selectors() -> None:
    """Mapping subclasses, paths, indexes, and Python UDFs retain canonical protocols."""

    class OffsetMapping(dict[str, int]):
        def __getitem__(self, key: str) -> int:
            return super().__getitem__(key) + 100

    subclass_result = (
        fpstreams.rows([OffsetMapping(x=1, y=2)])
        .with_columns(score=fpstreams.col("x") + fpstreams.col("y"))
        .where(fpstreams.col("score") > 0)
        .select("x", "score")
        .to_list()
    )
    assert subclass_result == [{"x": 1, "score": 203}]

    assert fpstreams.rows([{"nested": {"value": 2}}]).with_columns(
        score=fpstreams.col("nested.value") + 1
    ).select("score").to_list() == [{"score": 3}]
    assert fpstreams.rows([[2, 3]]).select(0).select("0").to_list() == [{"0": 2}]

    calls: list[int] = []
    result = (
        fpstreams.rows([{"x": 2}])
        .with_columns(score=fpstreams.col("x").map(lambda value: calls.append(value) or value + 1))
        .select("score")
        .to_list()
    )
    assert result == [{"score": 3}]
    assert calls == [2]


def test_structured_rows_fusion_short_circuits_and_closes_a_one_shot_source() -> None:
    """Taking one fused result must neither prefetch row two nor leak the source generator."""
    events: list[tuple[str, int | None]] = []

    def records() -> Iterator[dict[str, int]]:
        try:
            for value in range(3):
                events.append(("pull", value))
                yield {"x": value, "y": 1}
        finally:
            events.append(("close", None))

    result = (
        fpstreams.rows(records())
        .with_columns(score=fpstreams.col("x") + fpstreams.col("y"))
        .where(fpstreams.col("score") > 0)
        .select("x", "score")
        .take(1)
        .to_list()
    )

    assert result == [{"x": 0, "score": 1}]
    assert events == [("pull", 0), ("close", None)]


def test_structured_rows_fusion_cannot_bypass_a_forced_native_boundary() -> None:
    """Unsupported forced-native Rows plans fail during selection instead of running Python."""
    query = (
        fpstreams.rows([{"x": 1, "y": 2}])
        .with_columns(score=fpstreams.col("x") + fpstreams.col("y"))
        .where(fpstreams.col("score") > 0)
        .select("score")
        .with_engine("native")
    )

    with pytest.raises(fpstreams.NativeUnsupportedError, match="not native-compilable"):
        query.to_list()


def test_row_expression_fill_null_evaluates_its_input_once() -> None:
    evaluations = 0

    def evaluate(row: dict[str, str | None]) -> str | None:
        nonlocal evaluations
        evaluations += 1
        return row["value"]

    expression = fpstreams.col("value").map(lambda value: evaluate({"value": value}))

    assert expression.fill_null("missing")({"value": "present"}) == "present"
    assert evaluations == 1
    assert (
        expression.fill_null(fpstreams.col("fallback"))({"value": None, "fallback": "used"})
        == "used"
    )
    assert evaluations == 2


def test_rows_join_and_select_replace_nested_lookup_loops() -> None:
    orders = [
        {"order_id": 10, "user_id": 1, "amount": 50},
        {"order_id": 11, "user_id": 3, "amount": 20},
    ]
    users = [
        {"user_id": 1, "name": "Ada"},
        {"user_id": 2, "name": "Lin"},
    ]

    joined = (
        fpstreams.rows(orders)
        .join(users, on="user_id", how="left")
        .select("order_id", "name", total="amount")
        .to_list()
    )

    assert joined == [
        {"order_id": 10, "name": "Ada", "total": 50},
        {"order_id": 11, "name": None, "total": 20},
    ]


def test_common_rows_joins_stream_the_left_side() -> None:
    opened: list[int] = []

    def left_rows():
        for value in range(100):
            opened.append(value)
            yield {"id": value}

    joined = fpstreams.rows(left_rows()).join([{"id": 0, "name": "zero"}], on="id")
    iterator = iter(joined)

    assert next(iterator) == {"id": 0, "name": "zero"}
    assert opened == [0]
    iterator.close()


def test_join_keeps_a_right_flow_root_and_its_one_shot_native_source() -> None:
    from fpstreams.planning.logical import JoinNode, SourceNode
    from fpstreams.planning.source import Source, SourceCapabilities

    right_records = [{"id": 1, "right": "A"}]
    native_source = Source(
        lambda: iter(right_records),
        SourceCapabilities(reiterable=False, exact_size=1),
        native_data=right_records,
    )
    right = fpstreams.Flow(native_source)
    right_root = right._logical_plan.root

    joined = fpstreams.rows([{"id": 1, "left": "a"}]).join(right, on="id")
    join_root = joined._flow._logical_plan.root

    assert isinstance(join_root, JoinNode)
    assert join_root.right is right_root
    assert isinstance(join_root.right, SourceNode)
    assert join_root.right.source is native_source
    assert join_root.right.source.native_data is right_records
    assert joined.to_list() == [{"id": 1, "left": "a", "right": "A"}]
    with pytest.raises(fpstreams.FlowConsumedError):
        right.to_list()
    with pytest.raises(fpstreams.FlowConsumedError):
        joined.to_list()


def test_semi_join_indexes_only_keys_without_retaining_right_payloads() -> None:
    class Payload:
        pass

    references: list[weakref.ReferenceType[Payload]] = []

    def right_rows():
        for value in range(20):
            payload = Payload()
            references.append(weakref.ref(payload))
            yield {"id": value, "payload": payload}

    joined = fpstreams.rows([{"id": 0}]).join(right_rows(), on="id", how="semi")
    iterator = iter(joined)
    assert next(iterator) == {"id": 0}
    gc.collect()
    assert all(reference() is None for reference in references)
    iterator.close()


def test_join_modes_preserve_duplicate_key_order_and_cardinality() -> None:
    left = [
        {"id": 1, "left": "a"},
        {"id": 1, "left": "b"},
        {"id": 3, "left": "c"},
    ]
    right = [
        {"id": 1, "right": "x"},
        {"id": 1, "right": "y"},
        {"id": 2, "right": "z"},
    ]
    matched = [
        {"id": 1, "left": "a", "right": "x"},
        {"id": 1, "left": "a", "right": "y"},
        {"id": 1, "left": "b", "right": "x"},
        {"id": 1, "left": "b", "right": "y"},
    ]

    def join(how: str):
        return fpstreams.rows(left).join(right, on="id", how=how).to_list()

    assert join("inner") == matched
    assert join("left") == [*matched, {"id": 3, "left": "c", "right": None}]
    assert join("right") == [*matched, {"id": 2, "left": None, "right": "z"}]
    assert join("full") == [
        *matched,
        {"id": 3, "left": "c", "right": None},
        {"id": 2, "left": None, "right": "z"},
    ]
    assert join("semi") == left[:2]
    assert join("anti") == left[2:]


def test_left_driven_join_outputs_own_independent_snapshots() -> None:
    """Snapshot reuse must never expose an input dict or alias duplicate outputs."""
    payload: list[int] = [1]
    source = {"id": 1, "left": payload}

    singleton = fpstreams.rows([source]).join([{"id": 1, "right": "x"}], on="id").to_list()
    duplicates = (
        fpstreams.rows([source])
        .join(
            [{"id": 1, "right": "x"}, {"id": 1, "right": "y"}],
            on="id",
        )
        .to_list()
    )
    semi = fpstreams.rows([source]).join([{"id": 1}], on="id", how="semi").to_list()

    assert singleton == [{"id": 1, "left": payload, "right": "x"}]
    assert singleton[0] is not source
    assert singleton[0]["left"] is payload
    assert duplicates[0] is not duplicates[1]
    duplicates[0]["right"] = "changed"
    assert duplicates[1]["right"] == "y"
    assert semi == [source]
    assert semi[0] is not source
    assert source == {"id": 1, "left": payload}


def test_mapping_proxy_records_keep_dict_conversion_protocol_and_owned_join_outputs() -> None:
    """The exact proxy fast path remains equivalent to dict(proxy), including callbacks."""
    from types import MappingProxyType

    from fpstreams.tabular.records import _as_record

    events: list[tuple[str, object]] = []

    class TracedMapping(Mapping[str, object]):
        def __init__(self, values: dict[str, object]) -> None:
            self.values = values

        def __iter__(self) -> Iterator[str]:
            events.append(("iter", None))
            return iter(self.values)

        def __len__(self) -> int:
            events.append(("len", None))
            return len(self.values)

        def __getitem__(self, key: str) -> object:
            events.append(("getitem", key))
            return self.values[key]

    traced = MappingProxyType(TracedMapping({"id": 1, "left": "L"}))
    events.clear()
    expected = dict(traced)
    expected_events = list(events)
    events.clear()

    assert _as_record(traced) == expected
    assert events == expected_events

    left_values = {"id": 1, "left": "L"}
    right_values = {"id": 1, "right": "R"}
    left = MappingProxyType(left_values)
    result = (
        fpstreams.rows([left])
        .join(
            [MappingProxyType(right_values)],
            left_on=lambda row: row["id"],
            right_on=lambda row: row["id"],
        )
        .to_list()
    )

    assert result == [{"id": 1, "left": "L", "id_right": 1, "right": "R"}]
    assert type(result[0]) is dict
    result[0]["left"] = "changed"
    assert left_values == {"id": 1, "left": "L"}


def test_join_mapping_classification_cache_keeps_abc_guard_boundaries(  # noqa: C901
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only an ordinary non-virtual Mapping class may skip repeated ABC classification."""
    from abc import ABCMeta

    from fpstreams.tabular import join as join_module

    class DirectMapping(Mapping[str, object]):
        def __init__(self, identifier: int) -> None:
            self.values = {"id": identifier, "payload": identifier}

        def __iter__(self) -> Iterator[str]:
            return iter(self.values)

        def __len__(self) -> int:
            return len(self.values)

        def __getitem__(self, key: str) -> object:
            return self.values[key]

    class VirtualMapping:
        def __init__(self, identifier: int) -> None:
            self.values = {"id": identifier, "payload": identifier}

        def __iter__(self) -> Iterator[str]:
            return iter(self.values)

        def __len__(self) -> int:
            return len(self.values)

        def __getitem__(self, key: str) -> object:
            return self.values[key]

        def keys(self) -> object:
            return self.values.keys()

    Mapping.register(VirtualMapping)

    class CustomMeta(ABCMeta):
        pass

    class CustomMetaMapping(Mapping[str, object], metaclass=CustomMeta):
        def __init__(self, identifier: int) -> None:
            self.values = {"id": identifier, "payload": identifier}

        def __iter__(self) -> Iterator[str]:
            return iter(self.values)

        def __len__(self) -> int:
            return len(self.values)

        def __getitem__(self, key: str) -> object:
            return self.values[key]

    canonical = join_module._as_record
    classified: list[type[object]] = []

    def tracked(row: object) -> dict[str, Any]:
        classified.append(type(row))
        return canonical(row)

    monkeypatch.setattr(join_module, "_as_record", tracked)

    direct = [DirectMapping(index) for index in range(3)]
    _columns, _index, slots = join_module._join_record_index(
        direct, lambda row: row["id"], validate="m:m"
    )
    assert classified == [DirectMapping]
    assert [record["payload"] for record in slots] == [0, 1, 2]

    for row_type in (VirtualMapping, CustomMetaMapping):
        classified.clear()
        values = [row_type(index) for index in range(3)]
        join_module._join_record_index(values, lambda row: row["id"], validate="m:m")
        assert classified == [row_type, row_type, row_type]


def test_plain_field_join_reuses_proven_nominal_mapping_classification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A proven nominal Mapping should not repeat the selector's Mapping ABC check."""
    from fpstreams.expressions import selectors

    class DirectMapping(Mapping[str, object]):
        def __init__(self, **values: object) -> None:
            self.values = values

        def __iter__(self) -> Iterator[str]:
            return iter(self.values)

        def __len__(self) -> int:
            return len(self.values)

        def __getitem__(self, key: str) -> object:
            return self.values[key]

    class RejectingMeta(type):
        def __instancecheck__(cls, instance: object) -> bool:
            raise AssertionError(f"repeated Mapping check for {type(instance).__name__}")

    class RejectingMapping(metaclass=RejectingMeta):
        pass

    left = [DirectMapping(id=1, left="a"), DirectMapping(id=2, left="b")]
    right = [DirectMapping(id=1, right="x"), DirectMapping(id=2, right="y")]
    joined = fpstreams.rows(left).join(right, on="id", how="inner").with_engine("python")
    monkeypatch.setattr(selectors, "Mapping", RejectingMapping)

    assert joined.to_list() == [
        {"id": 1, "left": "a", "right": "x"},
        {"id": 2, "left": "b", "right": "y"},
    ]


def test_unique_right_plain_field_join_reuses_proven_nominal_mapping_classification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The m:1 strategy should share the proven nominal-Mapping field fast path."""
    from fpstreams.expressions import selectors

    class DirectMapping(Mapping[str, object]):
        def __init__(self, **values: object) -> None:
            self.values = values

        def __iter__(self) -> Iterator[str]:
            return iter(self.values)

        def __len__(self) -> int:
            return len(self.values)

        def __getitem__(self, key: str) -> object:
            return self.values[key]

    class RejectingMeta(type):
        def __instancecheck__(cls, instance: object) -> bool:
            raise AssertionError(f"repeated Mapping check for {type(instance).__name__}")

    class RejectingMapping(metaclass=RejectingMeta):
        pass

    left = [DirectMapping(id=1, left="a"), DirectMapping(id=2, left="b")]
    right = [DirectMapping(id=1, right="x"), DirectMapping(id=2, right="y")]
    joined = (
        fpstreams.rows(left).join(right, on="id", how="inner", validate="m:1").with_engine("python")
    )
    monkeypatch.setattr(selectors, "Mapping", RejectingMapping)

    assert joined.to_list() == [
        {"id": 1, "left": "a", "right": "x"},
        {"id": 2, "left": "b", "right": "y"},
    ]


def test_join_target_cache_preserves_original_and_generated_key_identity() -> None:
    """Only original right names may be shared; generated suffix keys remain per output."""
    safe_name = "".join(("right", "_payload"))
    safe = (
        fpstreams.rows([{"id": 1}, {"id": 1}])
        .join(
            [{"id": 1, safe_name: True}],
            on="id",
        )
        .to_list()
    )
    safe_keys = [next(name for name in row if name == safe_name) for row in safe]
    assert safe_keys[0] is safe_name
    assert safe_keys[1] is safe_name

    collided = (
        fpstreams.rows([{"id": 1, "value": "a"}, {"id": 1, "value": "b"}])
        .join(
            [{"id": 1, "value": "right"}],
            on="id",
        )
        .to_list()
    )
    generated = [next(name for name in row if name == "value_right") for row in collided]
    assert generated[0] is not generated[1]


def test_join_target_plan_reuses_one_generated_key_for_duplicate_matches() -> None:
    """All outputs from one left row retain the same per-row generated suffix key."""

    def select_id(row: dict[str, Any]) -> Any:
        return row["id"]

    result = (
        fpstreams.rows([{"id": 1, "value": "left"}])
        .join(
            [{"id": 1, "value": "a"}, {"id": 1, "value": "b"}],
            left_on=select_id,
            right_on=select_id,
        )
        .to_list()
    )
    generated = [next(name for name in row if name == "value_right") for row in result]

    assert result == [
        {"id": 1, "value": "left", "id_right": 1, "value_right": "a"},
        {"id": 1, "value": "left", "id_right": 1, "value_right": "b"},
    ]
    assert generated[0] is generated[1]


def test_join_target_cache_does_not_rehash_protocol_sensitive_field_names() -> None:
    """Unsafe field shapes fall back through the exact-dict canonical path."""
    from fpstreams.tabular.join import _JoinTargetCache

    hash_calls: list[str] = []

    class FieldName(str):
        def __hash__(self) -> int:
            hash_calls.append(str(self))
            return super().__hash__()

    field = FieldName("payload")
    left = [{"id": 1, field: "a"}, {"id": 2, field: "b"}]
    right = [{"id": 1, "right": "x"}, {"id": 2, "right": "y"}]
    expected = [
        {"id": 1, field: "a", "right": "x"},
        {"id": 2, field: "b", "right": "y"},
    ]
    hash_calls.clear()

    result = fpstreams.rows(left).join(right, on="id").to_list()
    assert hash_calls == []
    assert result == expected

    cache = _JoinTargetCache()
    cache.target_plan(left[0], ("id", "right"), shared_names={"id"}, suffix="_right")
    assert not cache.enabled

    collided_cache = _JoinTargetCache()
    collided_cache.target_plan(
        {"id": 1, "value": "left"},
        ("id", "value"),
        shared_names={"id"},
        suffix="_right",
    )
    assert collided_cache.enabled


def test_hash_join_bucket_promotion_does_not_repeat_key_protocols() -> None:
    """Promoting a repeated right key must not perform another index assignment."""
    from fpstreams.tabular.join import _join_record_index

    events: list[str] = []

    class Key:
        def __init__(self, label: str) -> None:
            self.label = label

        def __hash__(self) -> int:
            events.append(f"hash:{self.label}")
            return 0

        def __eq__(self, other: object) -> bool:
            assert isinstance(other, Key)
            events.append(f"eq:{self.label}:{other.label}")
            return True

    first, second, third = Key("first"), Key("second"), Key("third")
    rows = [
        {"key": first, "value": "a"},
        {"key": second, "value": "b"},
        {"key": third, "value": "c"},
    ]
    events.clear()

    _columns, index, slots = _join_record_index(rows, lambda row: row["key"], validate="m:m")

    assert events == [
        "hash:first",
        "hash:first",
        "hash:second",
        "eq:first:second",
        "hash:third",
        "eq:first:third",
    ]
    bucket = slots[next(iter(index.values()))]
    assert isinstance(bucket, list)
    assert [row["value"] for row in bucket] == [
        "a",
        "b",
        "c",
    ]

    mixed_events: list[str] = []

    class EqualToOne:
        def __hash__(self) -> int:
            mixed_events.append("hash")
            return hash(1)

        def __eq__(self, other: object) -> bool:
            mixed_events.append(f"eq:{other!r}")
            return other == 1

    mixed_key = EqualToOne()
    _columns, mixed_index, mixed_slots = _join_record_index(
        [{"key": 1, "value": "first"}, {"key": mixed_key, "value": "second"}],
        lambda row: row["key"],
        validate="m:m",
    )
    assert mixed_events == ["hash", "eq:1"]
    mixed_bucket = mixed_slots[next(iter(mixed_index.values()))]
    assert isinstance(mixed_bucket, list)
    assert [row["value"] for row in mixed_bucket] == [
        "first",
        "second",
    ]

    metaclass_events: list[str] = []

    class KeyType(type):
        def __eq__(cls, other: object) -> bool:
            metaclass_events.append(f"eq:{other!r}")
            return False

        __hash__ = type.__hash__

    class MetaclassKey(metaclass=KeyType):
        pass

    _join_record_index(
        [{"key": MetaclassKey(), "value": 1}],
        lambda row: row["key"],
        validate="m:m",
    )
    assert metaclass_events == []


def test_hash_join_slots_promote_dense_keys_without_reindexing() -> None:
    """Dense keys should promote their stable slots without rewriting hash entries."""
    from fpstreams.tabular.join import _join_record_index

    rows = [{"key": index % 8, "value": index} for index in range(80)]

    _columns, index, slots = _join_record_index(
        rows,
        lambda row: row["key"],
        validate="m:m",
    )

    assert all(isinstance(slots[position], list) for position in index.values())
    key_three = slots[index[3]]
    assert isinstance(key_three, list)
    assert [row["value"] for row in key_three] == list(range(3, 80, 8))


@pytest.mark.parametrize(
    ("how", "right", "expected"),
    [
        (
            "inner",
            [{"id": 1, "right": "matched"}],
            [{"id": 1, "left": "before", "id_right": 1, "right": "matched"}],
        ),
        (
            "left",
            [{"id": 2, "right": "unmatched"}],
            [{"id": 1, "left": "before", "id_right": None, "right": None}],
        ),
        ("semi", [{"id": 1}], [{"id": 1, "left": "before"}]),
        ("anti", [{"id": 2}], [{"id": 1, "left": "before"}]),
    ],
)
def test_callable_join_key_mutation_does_not_change_left_snapshot(
    how: str,
    right: list[dict[str, Any]],
    expected: list[dict[str, Any]],
) -> None:
    """Join output snapshots a left dictionary before invoking an opaque key callback."""
    source = {"id": 1, "left": "before"}

    def mutating_key(row: dict[str, Any]) -> int:
        key = row["id"]
        row["left"] = "after"
        row["added"] = True
        return key

    result = (
        fpstreams.rows([source])
        .join(
            right,
            left_on=mutating_key,
            right_on="id",
            how=how,
        )
        .to_list()
    )

    assert result == expected
    assert source == {"id": 1, "left": "after", "added": True}


def test_unique_right_join_snapshots_left_before_callable_key_mutation() -> None:
    """The unique-right fast path keeps the same pre-callback snapshot contract."""
    source = {"id": 1, "left": "before"}

    def mutating_key(row: dict[str, Any]) -> int:
        key = row["id"]
        row["left"] = "after"
        row["added"] = True
        return key

    result = (
        fpstreams.rows([source])
        .join(
            [{"id": 1, "right": "matched"}],
            left_on=mutating_key,
            right_on="id",
            validate="m:1",
        )
        .to_list()
    )

    assert result == [{"id": 1, "left": "before", "id_right": 1, "right": "matched"}]
    assert source == {"id": 1, "left": "after", "added": True}


def test_unique_right_inner_skips_target_collisions_for_an_unmatched_left_row() -> None:
    """An inner join must establish that a row matches before validating its output layout."""
    left = [{"id": 2, "value": "left", "value_right": "occupied"}]
    right = [{"id": 1, "value": "right"}]

    def select_id(row: dict[str, Any]) -> Any:
        return row["id"]

    assert (
        fpstreams.rows(left)
        .join(
            right,
            left_on=select_id,
            right_on=select_id,
            validate="m:1",
        )
        .to_list()
        == []
    )


def test_unique_right_probe_reports_unhashable_key_before_output_collision() -> None:
    """Probe errors retain the hash-join ordering even when the row layout also collides."""
    left = [{"id": [], "value": "left", "value_right": "occupied"}]
    right = [{"id": 1, "value": "right"}]

    def select_id(row: dict[str, Any]) -> Any:
        return row["id"]

    with pytest.raises(TypeError, match="join keys must be hashable"):
        fpstreams.rows(left).join(
            right,
            left_on=select_id,
            right_on=select_id,
            validate="m:1",
        ).to_list()


def test_unique_right_join_matches_canonical_key_hash_trace() -> None:
    """The m:1 specialization must not add speculative user hash calls."""
    events: list[str] = []

    class Key:
        def __init__(self, label: str) -> None:
            self.label = label

        def __hash__(self) -> int:
            events.append(f"hash:{self.label}")
            return 0

        def __eq__(self, other: object) -> bool:
            assert isinstance(other, Key)
            events.append(f"eq:{self.label}:{other.label}")
            return True

    left_key = Key("left")
    right_key = Key("right")
    result = (
        fpstreams.rows([{"id": left_key, "left": True}])
        .join(
            [{"id": right_key, "right": True}],
            on="id",
            validate="m:1",
        )
        .with_engine("python")
        .to_list()
    )

    assert result == [{"id": left_key, "left": True, "right": True}]
    assert events == [
        "hash:right",
        "hash:right",
        "hash:left",
        "eq:right:left",
    ]


def test_unique_right_join_discovers_columns_before_hashing_the_key() -> None:
    """Right-column discovery retains the canonical pre-index protocol order."""
    events: list[str] = []

    class Field(str):
        def __hash__(self) -> int:
            events.append("field")
            return super().__hash__()

    class Key:
        def __hash__(self) -> int:
            events.append("key")
            return 1

    payload = Field("payload")
    right = {"id": Key(), payload: True}
    events.clear()

    assert (
        fpstreams.rows([{"id": 2}])
        .join([right], on="id", validate="m:1")
        .with_engine("python")
        .to_list()
        == []
    )
    assert events.index("field") < events.index("key")


@pytest.mark.parametrize(
    ("how", "validate"),
    [("inner", "m:m"), ("semi", "m:m"), ("inner", "m:1")],
)
def test_exact_field_join_snapshots_before_colliding_key_equality(how: str, validate: str) -> None:
    """A string lookup may run a colliding key's ``__eq__`` and mutate its source."""

    class CollidingKey:
        def __init__(self, source: dict[Any, Any]) -> None:
            self.source = source

        def __hash__(self) -> int:
            return hash("id")

        def __eq__(self, other: object) -> bool:
            self.source["value"] = "after-equality"
            return False

    source: dict[Any, Any] = {"value": "placeholder"}
    source[CollidingKey(source)] = "collision"
    source["id"] = 1
    # Inserting the real field may already compare it with the colliding key.
    source["value"] = "before"

    result = (
        fpstreams.rows([source])
        .join(
            [{"id": 1, "right": True}],
            on="id",
            how=how,
            validate=validate,
        )
        .to_list()
    )

    assert result[0]["value"] == "before"
    assert source["value"] == "after-equality"


def test_join_validation_is_checked_before_sources_are_opened() -> None:
    opened = False

    def records():
        nonlocal opened
        opened = True
        yield {"id": 1}

    with pytest.raises(ValueError, match="validate must be one of"):
        fpstreams.rows(records()).join(records(), on="id", validate="one-to-one")

    assert not opened


def test_join_cardinality_contracts_accept_matching_inputs() -> None:
    left_unique = [{"id": 1, "left": "a"}]
    left_duplicate = [{"id": 1, "left": "a"}, {"id": 1, "left": "b"}]
    right_unique = [{"id": 1, "right": "x"}]
    right_duplicate = [{"id": 1, "right": "x"}, {"id": 1, "right": "y"}]

    assert (
        len(fpstreams.rows(left_unique).join(right_duplicate, on="id", validate="1:m").to_list())
        == 2
    )
    assert (
        len(fpstreams.rows(left_duplicate).join(right_unique, on="id", validate="m:1").to_list())
        == 2
    )
    assert (
        len(fpstreams.rows(left_unique).join(right_unique, on="id", validate="1:1").to_list()) == 1
    )
    assert (
        len(fpstreams.rows(left_duplicate).join(right_duplicate, on="id", validate="m:m").to_list())
        == 4
    )


@pytest.mark.parametrize(
    ("validate", "duplicate_side"),
    [("1:m", "left"), ("1:1", "left"), ("m:1", "right"), ("1:1", "right")],
)
def test_join_cardinality_contracts_reject_duplicate_keys(
    validate: str, duplicate_side: str
) -> None:
    unique = [{"id": 1}]
    duplicate = [{"id": 1}, {"id": 1}]
    left = duplicate if duplicate_side == "left" else unique
    right = duplicate if duplicate_side == "right" else unique

    with pytest.raises(
        ValueError,
        match=rf"validate={validate!r} requires unique {duplicate_side} keys.*1",
    ):
        fpstreams.rows(left).join(right, on="id", validate=validate).to_list()


@pytest.mark.parametrize(
    ("how", "validate", "duplicate_side"),
    [("semi", "1:m", "left"), ("anti", "m:1", "right")],
)
def test_semi_and_anti_joins_honor_cardinality_contracts(
    how: str, validate: str, duplicate_side: str
) -> None:
    unique = [{"id": 1}]
    duplicate = [{"id": 1}, {"id": 1}]
    left = duplicate if duplicate_side == "left" else unique
    right = duplicate if duplicate_side == "right" else unique

    with pytest.raises(ValueError, match=rf"unique {duplicate_side} keys"):
        fpstreams.rows(left).join(
            right,
            on="id",
            how=how,
            validate=validate,
        ).to_list()


def test_rows_supports_relational_join_modes_and_different_keys() -> None:
    left = [{"left_id": 1, "value": "a"}, {"left_id": 3, "value": "c"}]
    right = [{"right_id": 1, "value": "A"}, {"right_id": 2, "value": "B"}]

    assert fpstreams.rows(left).join(
        right, left_on="left_id", right_on="right_id", how="full"
    ).to_list() == [
        {"left_id": 1, "value": "a", "right_id": 1, "value_right": "A"},
        {"left_id": 3, "value": "c", "right_id": None, "value_right": None},
        {"left_id": None, "value": None, "right_id": 2, "value_right": "B"},
    ]
    assert fpstreams.rows(left).join(
        right, left_on="left_id", right_on="right_id", how="semi"
    ).to_list() == [left[0]]
    assert fpstreams.rows(left).join(
        right, left_on="left_id", right_on="right_id", how="anti"
    ).to_list() == [left[1]]


def test_rows_join_accepts_readable_composite_keys() -> None:
    events = [
        {"tenant": "a", "user_id": 1, "event": "open"},
        {"tenant": "b", "user_id": 1, "event": "click"},
    ]
    users = [
        {"tenant": "a", "id": 1, "name": "Ada"},
        {"tenant": "b", "id": 2, "name": "Lin"},
    ]

    assert fpstreams.rows(events).join(
        users,
        left_on=("tenant", "user_id"),
        right_on=("tenant", "id"),
        how="left",
    ).select("event", "name").to_list() == [
        {"event": "open", "name": "Ada"},
        {"event": "click", "name": None},
    ]


def test_composite_selector_fast_path_is_exact_and_keeps_canonical_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only two or three exact direct parts may bypass their compiled selectors."""
    from fpstreams.tabular import join as join_module

    canonical_compile = join_module.compile_selector

    class FieldName(str):
        pass

    class Composite(tuple[object, ...]):
        pass

    def select_a(row: Mapping[str, int]) -> int:
        return row["a"]

    def tracking_compiler(call_log: list[object]):
        def tracked_compile(part: object):
            select = canonical_compile(part)

            def tracked_select(value: object) -> object:
                call_log.append(part)
                return select(value)

            return tracked_select

        return tracked_compile

    cases = (
        (("a", "b"), {"a": 1, "b": 2}, (1, 2), False),
        ((0, 1, 2), {0: "a", 1: "b", 2: "c"}, ("a", "b", "c"), False),
        (
            ("a", "b", "c", "d"),
            {"a": 1, "b": 2, "c": 3, "d": 4},
            (1, 2, 3, 4),
            True,
        ),
        (("nested.a", "b"), {"nested": {"a": 1}, "b": 2}, (1, 2), True),
        ((FieldName("a"), "b"), {"a": 1, "b": 2}, (1, 2), True),
        ((True, "b"), {True: 1, "b": 2}, (1, 2), True),
        (("a", 0), {"a": 1, 0: 2}, (1, 2), False),
        ((select_a, "b"), {"a": 1, "b": 2}, (1, 2), True),
        (Composite(("a", "b")), {"a": 1, "b": 2}, (1, 2), True),
    )
    for parts, row, expected, uses_fallback in cases:
        calls: list[object] = []
        monkeypatch.setattr(join_module, "compile_selector", tracking_compiler(calls))
        select_key = join_module._compile_join_selector(parts)

        assert select_key(row) == expected
        assert calls == (list(parts) if uses_fallback else [])

    class LoggedMapping(Mapping[str, int]):
        def __init__(self) -> None:
            self.values = {"a": 1, "b": 2}

        def __getitem__(self, key: str) -> int:
            return self.values[key]

        def __iter__(self) -> Iterator[str]:
            return iter(self.values)

        def __len__(self) -> int:
            return len(self.values)

    calls = []

    monkeypatch.setattr(join_module, "compile_selector", tracking_compiler(calls))
    select_mapping_key = join_module._compile_join_selector(("a", "b"))

    assert select_mapping_key(LoggedMapping()) == (1, 2)
    assert calls == ["a", "b"]


@pytest.mark.parametrize(
    ("part", "error_type", "wrapped"),
    [
        ("missing", AttributeError, True),
        ("missing", IndexError, False),
        (7, AttributeError, False),
        (7, IndexError, True),
        ("missing", KeyError, True),
        (7, TypeError, True),
    ],
)
def test_composite_direct_selector_matches_component_exception_boundaries(
    part: str | int,
    error_type: type[Exception],
    wrapped: bool,
) -> None:
    """A direct part retains the exact exception set, message, and cause of its selector."""
    from fpstreams.expressions.selectors import compile_selector
    from fpstreams.tabular.join import _compile_join_selector

    first = 101 if type(part) is str else "first"

    class CollidingKey:
        def __hash__(self) -> int:
            return hash(part)

        def __eq__(self, other: object) -> bool:
            if other == part:
                raise error_type("collision")
            return False

    def record() -> dict[object, object]:
        return {first: "first", CollidingKey(): "collision"}

    component_selectors = (compile_selector(first), compile_selector(part))

    def canonical(row: object) -> tuple[object, ...]:
        return tuple(select(row) for select in component_selectors)

    direct = _compile_join_selector((first, part))

    def capture(select: object) -> tuple[type[BaseException], str, object, object]:
        try:
            select(record())  # type: ignore[operator]
        except BaseException as error:
            cause = error.__cause__
            return (
                type(error),
                str(error),
                None if cause is None else type(cause),
                None if cause is None else str(cause),
            )
        raise AssertionError("selector unexpectedly succeeded")

    expected_type = fpstreams.SelectionError if wrapped else error_type
    canonical_error = capture(canonical)
    direct_error = capture(direct)

    assert canonical_error[0] is expected_type
    assert direct_error == canonical_error


def test_generic_multi_key_group_uses_one_composite_selector_and_keeps_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The generic in-memory loop reads exact dict keys without component callables."""
    from fpstreams.physical.relational import GroupAggregatePhysicalNode
    from fpstreams.planning.compiler import compile_query
    from fpstreams.tabular import join as join_module

    canonical_compile = join_module.compile_selector
    calls: list[object] = []

    def tracked_compile(part: object):
        select = canonical_compile(part)

        def tracked_select(value: object) -> object:
            calls.append(part)
            return select(value)

        return tracked_select

    monkeypatch.setattr(join_module, "compile_selector", tracked_compile)
    first_key = int("1000")
    equal_key = int("1000")
    first_band = "".join(("band", "-x"))
    equal_band = "".join(("band", "-x"))
    assert first_key == equal_key and first_key is not equal_key
    assert first_band == equal_band and first_band is not equal_band
    count = fpstreams.Aggregator(lambda: 0, lambda state, _row: state + 1)
    grouped = (
        fpstreams.rows(
            [
                {"key": first_key, "band": first_band},
                {"key": equal_key, "band": equal_band},
                {"key": 2000, "band": "band-y"},
            ]
        )
        .with_engine("python")
        .group_by("key", "band")
        .aggregate(rows=count)
    )

    physical = compile_query(grouped._flow._query("list"))
    assert isinstance(physical.root, GroupAggregatePhysicalNode)
    assert physical.root.select_key({"key": 1, "band": "x"}) == (1, "x")
    calls.clear()

    result = grouped.to_list()

    assert calls == []
    assert result == [
        {"key": first_key, "band": first_band, "rows": 2},
        {"key": 2000, "band": "band-y", "rows": 1},
    ]
    assert result[0]["key"] is first_key
    assert result[0]["band"] is first_band

    class LoggedMapping(Mapping[str, object]):
        def __getitem__(self, key: str) -> object:
            return {"key": 1, "band": "mapped"}[key]

        def __iter__(self) -> Iterator[str]:
            return iter(("key", "band"))

        def __len__(self) -> int:
            return 2

    calls.clear()
    assert fpstreams.rows([LoggedMapping()]).with_engine("python").group_by(
        "key", "band"
    ).aggregate(rows=count).to_list() == [{"key": 1, "band": "mapped", "rows": 1}]
    assert calls == ["key", "band"]


def test_generic_composite_group_closes_one_shot_on_selection_and_failpoint() -> None:
    """The composite key path keeps canonical source ownership and state transitions."""
    from fpstreams.runtime.failpoints import failpoint

    count = fpstreams.Aggregator(lambda: 0, lambda state, _row: state + 1)
    events: list[str] = []

    def missing_key() -> Iterator[dict[str, int]]:
        events.append("open:missing")
        try:
            yield {"left": 1}
        finally:
            events.append("close:missing")

    with pytest.raises(fpstreams.SelectionError) as captured:
        (
            fpstreams.rows(missing_key())
            .with_engine("python")
            .group_by("left", "right")
            .aggregate(rows=count)
            .to_list()
        )
    assert isinstance(captured.value.__cause__, KeyError)
    assert events == ["open:missing", "close:missing"]

    def instrumented() -> Iterator[dict[str, int]]:
        events.append("open:failpoint")
        try:
            yield {"left": 1, "right": 2}
        finally:
            events.append("close:failpoint")

    with (
        failpoint("group.state.create.after", RuntimeError("group transition")),
        pytest.raises(RuntimeError, match="group transition"),
    ):
        (
            fpstreams.rows(instrumented())
            .with_engine("python")
            .group_by("left", "right")
            .aggregate(rows=count)
            .to_list()
        )
    assert events == [
        "open:missing",
        "close:missing",
        "open:failpoint",
        "close:failpoint",
    ]


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


def test_rows_min_max_fixed_lane_preserves_protocol_order() -> None:
    """Two extrema lanes keep distinct reads and canonical protocol ordering."""

    events: list[str] = []

    class Key:
        def __init__(self, label: str) -> None:
            self.label = label

        def __hash__(self) -> int:
            events.append(f"hash:{self.label}")
            return 1

        def __eq__(self, other: object) -> bool:
            events.append(f"eq:{self.label}:{getattr(other, 'label', '?')}")
            return isinstance(other, Key)

    class Value:
        def __init__(self, label: str, number: int) -> None:
            self.label = label
            self.number = number

        def __lt__(self, other: Value) -> bool:
            events.append(f"lt:{self.label}:{other.label}")
            return self.number < other.number

        def __gt__(self, other: Value) -> bool:
            events.append(f"gt:{self.label}:{other.label}")
            return self.number > other.number

    class LoggedRow(Mapping[str, object]):
        def __init__(self, label: str, key: Key, value: Value) -> None:
            self.label = label
            self.values = {"key": key, "value": value}

        def __getitem__(self, name: str) -> object:
            events.append(f"get:{self.label}:{name}")
            return self.values[name]

        def __iter__(self) -> Iterator[str]:
            return iter(self.values)

        def __len__(self) -> int:
            return len(self.values)

    first_key = Key("k1")
    equal_key = Key("k2")
    first_value = Value("v1", 5)
    second_value = Value("v2", 3)
    result = (
        fpstreams.rows(
            [
                LoggedRow("r1", first_key, first_value),
                LoggedRow("r2", equal_key, second_value),
            ]
        )
        .with_engine("python")
        .group_by("key")
        .aggregate(low=fpstreams.agg.min("value"), high=fpstreams.agg.max("value"))
        .to_list()
    )

    assert len(result) == 1
    assert result[0]["key"] is first_key
    assert result[0]["low"] is second_value
    assert result[0]["high"] is first_value
    assert events == [
        "get:r1:key",
        "hash:k1",
        "hash:k1",
        "hash:k1",
        "get:r1:value",
        "get:r1:value",
        "get:r2:key",
        "hash:k2",
        "hash:k2",
        "eq:k1:k2",
        "get:r2:value",
        "lt:v2:v1",
        "get:r2:value",
        "gt:v2:v1",
    ]


@pytest.mark.parametrize("lane_order", ["min_max", "max_min"])
def test_rows_min_max_fixed_lane_is_independent_of_keyword_order(
    lane_order: str,
) -> None:
    """Equivalent extrema requests retain results and field order."""
    aggregations = (
        {
            "low": fpstreams.agg.min(1),
            "high": fpstreams.agg.max(1),
        }
        if lane_order == "min_max"
        else {
            "high": fpstreams.agg.max(1),
            "low": fpstreams.agg.min(1),
        }
    )

    result = (
        fpstreams.rows([(1, 5), (1, 3), (2, 7)])
        .with_engine("python")
        .group_by(0)
        .aggregate(**aggregations)
        .to_list()
    )

    assert result == [
        {"key_0": 1, "low": 3, "high": 5},
        {"key_0": 2, "low": 7, "high": 7},
    ]
    assert [*result[0]] == ["key_0", *aggregations]


def test_rows_max_min_fixed_lane_preserves_requested_comparison_order() -> None:
    """The reverse keyword order must evaluate maximum before minimum on every row."""
    events: list[str] = []

    class Value:
        def __init__(self, label: str, number: int) -> None:
            self.label = label
            self.number = number

        def __lt__(self, other: Value) -> bool:
            events.append(f"lt:{self.label}:{other.label}")
            return self.number < other.number

        def __gt__(self, other: Value) -> bool:
            events.append(f"gt:{self.label}:{other.label}")
            return self.number > other.number

    class Row(Mapping[str, object]):
        def __init__(self, label: str, value: Value) -> None:
            self.label = label
            self.value = value

        def __getitem__(self, name: str) -> object:
            events.append(f"get:{self.label}:{name}")
            return 1 if name == "key" else self.value

        def __iter__(self) -> Iterator[str]:
            return iter(("key", "value"))

        def __len__(self) -> int:
            return 2

    first = Value("first", 5)
    second = Value("second", 3)
    result = (
        fpstreams.rows([Row("first", first), Row("second", second)])
        .with_engine("python")
        .group_by("key")
        .aggregate(high=fpstreams.agg.max("value"), low=fpstreams.agg.min("value"))
        .to_list()
    )

    assert result == [{"key": 1, "high": first, "low": second}]
    assert events == [
        "get:first:key",
        "get:first:value",
        "get:first:value",
        "get:second:key",
        "get:second:value",
        "gt:second:first",
        "get:second:value",
        "lt:second:first",
    ]


def test_rows_min_max_fixed_lane_closes_on_error_and_failpoint() -> None:
    """Comparison failure and state failpoints stop later reads and close the source."""
    from fpstreams.runtime.failpoints import failpoint

    events: list[str] = []

    class Value:
        def __init__(self, label: str, *, fail: bool = False) -> None:
            self.label = label
            self.fail = fail

        def __lt__(self, other: Value) -> bool:
            events.append(f"lt:{self.label}:{other.label}")
            if self.fail:
                raise RuntimeError("min comparison")
            return False

        def __gt__(self, other: Value) -> bool:
            events.append(f"gt:{self.label}:{other.label}")
            return False

    class LoggedRow(Mapping[str, object]):
        def __init__(self, label: str, value: Value) -> None:
            self.label = label
            self.value = value

        def __getitem__(self, name: str) -> object:
            events.append(f"get:{self.label}:{name}")
            return 1 if name == "key" else self.value

        def __iter__(self) -> Iterator[str]:
            return iter(("key", "value"))

        def __len__(self) -> int:
            return 2

    def failing_comparison() -> Iterator[Mapping[str, object]]:
        events.append("open:comparison")
        try:
            yield LoggedRow("first", Value("first"))
            yield LoggedRow("second", Value("second", fail=True))
        finally:
            events.append("close:comparison")

    with pytest.raises(RuntimeError, match="min comparison"):
        (
            fpstreams.rows(failing_comparison())
            .with_engine("python")
            .group_by("key")
            .aggregate(low=fpstreams.agg.min("value"), high=fpstreams.agg.max("value"))
            .to_list()
        )
    assert events == [
        "open:comparison",
        "get:first:key",
        "get:first:value",
        "get:first:value",
        "get:second:key",
        "get:second:value",
        "lt:second:first",
        "close:comparison",
    ]

    events.clear()

    def instrumented() -> Iterator[Mapping[str, object]]:
        events.append("open:failpoint")
        try:
            yield LoggedRow("instrumented", Value("unused"))
        finally:
            events.append("close:failpoint")

    with (
        failpoint("group.state.create.after", RuntimeError("group transition")),
        pytest.raises(RuntimeError, match="group transition"),
    ):
        (
            fpstreams.rows(instrumented())
            .with_engine("python")
            .group_by("key")
            .aggregate(low=fpstreams.agg.min("value"), high=fpstreams.agg.max("value"))
            .to_list()
        )
    assert events == [
        "open:failpoint",
        "get:instrumented:key",
        "close:failpoint",
    ]


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


def test_rows_computes_stable_online_statistics_and_distinct_counts() -> None:
    records = [
        {"team": "a", "score": 1_000_000_000_001.0, "tags": ["x"]},
        {"team": "a", "score": 1_000_000_000_002.0, "tags": ["x"]},
        {"team": "a", "score": 1_000_000_000_003.0, "tags": ["y"]},
        {"team": "b", "score": 7.0, "tags": ["z"]},
    ]

    summary = (
        fpstreams.rows(records)
        .group_by("team")
        .aggregate(
            sample_variance=fpstreams.agg.variance("score"),
            population_variance=fpstreams.agg.variance("score", ddof=0),
            sample_std=fpstreams.agg.std("score"),
            distinct_tags=fpstreams.agg.count_distinct("tags"),
        )
        .to_list()
    )

    assert summary == [
        {
            "team": "a",
            "sample_variance": 1.0,
            "population_variance": pytest.approx(2 / 3),
            "sample_std": 1.0,
            "distinct_tags": 2,
        },
        {
            "team": "b",
            "sample_variance": None,
            "population_variance": 0.0,
            "sample_std": None,
            "distinct_tags": 1,
        },
    ]
    with pytest.raises(ValueError, match="ddof"):
        fpstreams.agg.variance("score", ddof=-1)


def test_grouped_rows_initializes_aggregations_once_per_group() -> None:
    initializer_calls = 0

    def initializer() -> int:
        nonlocal initializer_calls
        initializer_calls += 1
        return 0

    total = fpstreams.Aggregator(
        initializer,
        lambda state, row: state + row["score"],
    )
    result = (
        fpstreams.rows(
            [
                {"team": "a", "score": 1},
                {"team": "a", "score": 2},
                {"team": "b", "score": 3},
            ]
        )
        .group_by("team")
        .aggregate(total=total)
        .to_list()
    )

    assert result == [{"team": "a", "total": 3}, {"team": "b", "total": 3}]
    assert initializer_calls == 2


@pytest.mark.parametrize("position", ["key", "value"])
@pytest.mark.parametrize(
    ("selector", "error_type", "wrapped"),
    [
        (0, AttributeError, False),
        (0, IndexError, True),
        ("field", IndexError, False),
        ("field", AttributeError, True),
    ],
)
def test_selected_group_sum_keeps_selector_exception_boundaries(
    position: str,
    selector: str | int,
    error_type: type[Exception],
    wrapped: bool,
) -> None:
    """Direct lookup translates exactly the exception set of each canonical selector."""

    class CollidingKey:
        def __hash__(self) -> int:
            return hash(selector)

        def __eq__(self, _other: object) -> bool:
            raise error_type("collision equality")

    if position == "key":
        record = {CollidingKey(): "unrelated", "safe_value": 4}
        grouped = (
            fpstreams.rows([record])
            .group_by(key=selector)
            .aggregate(total=fpstreams.agg.sum("safe_value"))
        )
    else:
        record = {"safe_key": "a", CollidingKey(): "unrelated"}
        grouped = (
            fpstreams.rows([record])
            .group_by("safe_key")
            .aggregate(total=fpstreams.agg.sum(selector))
        )

    expected_error = fpstreams.SelectionError if wrapped else error_type
    with pytest.raises(
        expected_error, match="collision equality" if not wrapped else None
    ) as captured:
        grouped.to_list()
    if wrapped:
        assert isinstance(captured.value.__cause__, error_type)
    else:
        assert captured.value.__cause__ is None


def test_selected_group_sum_matches_builtin_and_callable_oracles() -> None:
    """Both engines preserve ordered dict grouping across a deterministic random sample."""
    generator = random.Random(0xF57EA)
    records = [
        {"key": generator.randrange(41), "value": generator.randrange(-100, 101)}
        for _ in range(4_000)
    ]
    totals: dict[int, int] = {}
    for record in records:
        key = record["key"]
        totals[key] = totals.get(key, 0) + record["value"]
    expected = [{"key": key, "total": total} for key, total in totals.items()]

    callable_oracle = (
        fpstreams.rows(records)
        .with_engine("python")
        .group_by(key=lambda row: row["key"])
        .aggregate(total=fpstreams.agg.sum(lambda row: row["value"]))
        .to_list()
    )
    assert callable_oracle == expected
    for engine in ("auto", "python"):
        actual = (
            fpstreams.rows(records)
            .with_engine(engine)
            .group_by("key")
            .aggregate(total=fpstreams.agg.sum("value"))
            .to_list()
        )
        assert actual == expected
        assert (
            fpstreams.rows([])
            .with_engine(engine)
            .group_by("key")
            .aggregate(total=fpstreams.agg.sum("value"))
            .to_list()
            == []
        )


@pytest.mark.parametrize("scenario", ["pair_reversed", "group_enumerate"])
def test_closed_kernels_reject_preimport_builtin_pollution(scenario: str) -> None:
    """A polluted implementation primitive must keep execution on Python semantics."""
    if scenario == "pair_reversed":
        body = """
import builtins
import numpy  # Load optional dependency internals before applying the narrow pollution.

builtins.reversed = lambda values: values
import fpstreams
from fpstreams.planning.pair_i64_expression import lower_pair_i64_row_filter

values = [(index, index % 4) for index in range(128)]
expression = (fpstreams.col(1) - 1) == 1
assert lower_pair_i64_row_filter(expression) is None
automatic = fpstreams.pairs(values).filter_pairs(expression).to_dict(on_duplicate="last")
canonical = (
    fpstreams.pairs(values)
    .filter_pairs(expression)
    .with_engine("python")
    .to_dict(on_duplicate="last")
)
assert automatic == canonical
"""
    else:
        body = """
import builtins
import numpy as np

original_enumerate = builtins.enumerate

def selective_enumerate(values, start=0):
    if (
        type(values) is tuple
        and values
        and type(values[0]).__module__.startswith("fpstreams.collecting.")
    ):
        return original_enumerate((), start)
    return original_enumerate(values, start)

builtins.enumerate = selective_enumerate
import fpstreams

query = (
    fpstreams.rows.from_numpy(
        np.asarray([[1, 10], [1, 20], [2, 30]], dtype=np.int64),
        columns=("key", "value"),
    )
    .group_by("key")
    .aggregate(rows=fpstreams.agg.count(), total=fpstreams.agg.sum("value"))
)
automatic = query.run_with_report("to_list")
canonical = query.with_engine("python").to_list()
assert automatic.report.strategy == "planned:python"
assert automatic.value == canonical == [
    {"key": 1, "rows": 0, "total": 0},
    {"key": 2, "rows": 0, "total": 0},
]
"""
    completed = _run_inline_python(body, cwd=PROJECT_ROOT)

    assert completed.returncode == 0, completed.stderr


def test_selected_tuple_group_sum_native_decline_replays_protocol_rows_once() -> None:
    """A late exact-shape decline adds no selector, hash, or addition observations."""

    def evaluate(engine: str) -> tuple[list[dict[str, object]], list[str]]:
        events: list[str] = []

        class Integer(int):
            def __hash__(self) -> int:
                events.append("hash")
                return int.__hash__(self)

        class Number:
            def __init__(self, value: int) -> None:
                self.value = value

            def __radd__(self, left: object) -> int:
                events.append(f"radd:{left!r}+{self.value}")
                assert type(left) is int
                return left + self.value

        class Row(tuple[object, ...]):
            def __getitem__(self, index: object) -> object:
                events.append(f"get:{index!r}")
                return super().__getitem__(index)  # type: ignore[index]

        query = (
            fpstreams.rows([(1, 2), Row((Integer(1), Number(3)))])
            .with_engine(engine)
            .group_by(key=0)
            .aggregate(total=fpstreams.agg.sum(1))
        )
        return query.to_list(), events

    expected, expected_events = evaluate("python")
    actual, actual_events = evaluate("auto")

    assert actual == expected == [{"key": 1, "total": 5}]
    assert (
        actual_events
        == expected_events
        == [
            "get:0",
            "hash",
            "hash",
            "get:1",
            "radd:2+3",
        ]
    )


def test_selected_tuple_group_sum_short_row_replays_and_closes_once() -> None:
    """A late short exact tuple enters one canonical selection-error lifecycle."""
    from fpstreams.planning.source import Source, SourceCapabilities
    from fpstreams.streams.flow import Flow

    retained = [(1, 2), (1,)]
    events: list[str] = []

    def open_rows() -> Iterator[tuple[int, ...]]:
        events.append("open")
        try:
            for index, row in enumerate(retained):
                events.append(f"pull:{index}")
                yield row
        finally:
            events.append("close")

    source = Source(
        open_rows,
        SourceCapabilities(reiterable=True, exact_size=2, ordered=True),
        native_data=retained,
    )
    grouped = fpstreams.Rows(Flow(source)).group_by(key=0).aggregate(total=fpstreams.agg.sum(1))

    with pytest.raises(fpstreams.SelectionError) as captured:
        grouped.to_list()

    assert isinstance(captured.value.__cause__, IndexError)
    assert events == ["open", "pull:0", "pull:1", "close"]


def test_selected_tuple_group_sum_rejects_nonexact_index_selectors_before_native(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bool and int-subclass selectors retain Python selection and report policy."""
    from fpstreams import _native

    class Index(int):
        calls = 0

        def __index__(self) -> int:
            type(self).calls += 1
            return int(self)

    def unexpected_native(*_arguments: object) -> object:
        raise AssertionError("a nonexact selector entered fixed-index native grouping")

    monkeypatch.setattr(_native, "group_sum_i64_rows_v1", unexpected_native)
    candidates = (
        (True, 0, [{"key": 1, "total": 30}]),
        (Index(0), Index(1), [{"key": 10, "total": 1}, {"key": 20, "total": 1}]),
    )
    for key_selector, value_selector, expected in candidates:
        query = (
            fpstreams.rows(((10, 1), (20, 1)))
            .group_by(key=key_selector)
            .aggregate(total=fpstreams.agg.sum(value_selector))
        )
        execution = query.run_with_report("to_list")
        assert execution.value == expected
        assert execution.report.strategy == "planned:python"
    assert Index.calls == 0


def test_selected_group_sum_rechecks_the_exact_fast_shape_per_row() -> None:
    """A built-in first key never weakens later mapping or custom-key protocols."""
    events: list[str] = []

    class CustomKey:
        def __hash__(self) -> int:
            events.append("hash")
            return 101

    class LoggedMapping(Mapping[str, object]):
        def __getitem__(self, key: str) -> object:
            events.append(f"mapping:{key}")
            return {"key": "safe", "value": 4}[key]

        def __iter__(self) -> Iterator[str]:
            return iter(("key", "value"))

        def __len__(self) -> int:
            return 2

    custom = CustomKey()
    result = (
        fpstreams.rows(
            [
                {"key": "safe", "value": 1},
                {"key": custom, "value": 2},
                {"key": custom, "value": 3},
                LoggedMapping(),
            ]
        )
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
        .to_list()
    )

    assert result[0] == {"key": "safe", "total": 5}
    assert result[1]["key"] is custom
    assert result[1]["total"] == 5
    # First custom insertion performs explicit hash, lookup, and insertion;
    # its second row performs explicit hash and lookup, exactly as before.
    assert events == ["hash"] * 5 + ["mapping:key", "mapping:value"]


def test_selected_group_sum_normalizes_fast_lookup_collision_type_error() -> None:
    """A later built-in key keeps canonical errors when it collides with a custom key."""

    class CollidingKey:
        def __hash__(self) -> int:
            return hash(1)

        def __eq__(self, other: object) -> bool:
            if other == 1:
                raise TypeError("equality-owned")
            return self is other

    records = [
        {"key": 0, "value": 1},
        {"key": CollidingKey(), "value": 2},
        {"key": 1, "value": 3},
    ]

    with pytest.raises(TypeError, match="group_by keys must be hashable") as captured:
        fpstreams.rows(records).group_by("key").aggregate(
            total=fpstreams.agg.sum("value")
        ).to_list()
    assert captured.value.__cause__ is None


def test_selected_group_sum_preserves_mapping_addition_and_failure_order() -> None:
    """Fallback protocols, ``0 + value``, dotted paths, and close remain observable."""
    events: list[str] = []

    class LoggedDict(dict[str, object]):
        def __getitem__(self, key: str) -> object:
            events.append(f"dict:{key}")
            return super().__getitem__(key)

    class LoggedMapping(Mapping[str, object]):
        def __init__(self, **values: object) -> None:
            self.values = values

        def __getitem__(self, key: str) -> object:
            events.append(f"mapping:{key}")
            return self.values[key]

        def __iter__(self) -> Iterator[str]:
            return iter(self.values)

        def __len__(self) -> int:
            return len(self.values)

    class StatefulNumber:
        def __init__(self, value: int) -> None:
            self.value = value

        def __radd__(self, left: object) -> StatefulNumber:
            events.append(f"radd:{left!r}+{self.value}")
            assert left == 0
            return StatefulNumber(self.value)

        def __add__(self, right: object) -> StatefulNumber:
            assert isinstance(right, StatefulNumber)
            events.append(f"add:{self.value}+{right.value}")
            return StatefulNumber(self.value + right.value)

    result = (
        fpstreams.rows(
            [
                LoggedDict(key="a", value=StatefulNumber(2)),
                LoggedMapping(key="a", value=StatefulNumber(3)),
            ]
        )
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
        .to_list()
    )
    assert result[0]["key"] == "a"
    assert result[0]["total"].value == 5
    assert events == [
        "dict:key",
        "dict:value",
        "radd:0+2",
        "mapping:key",
        "mapping:value",
        "add:2+3",
    ]
    assert fpstreams.rows([{"outer": {"key": "x"}, "payload": {"value": 4}}]).group_by(
        "outer.key"
    ).aggregate(total=fpstreams.agg.sum("payload.value")).to_list() == [{"key": "x", "total": 4}]

    closed: list[bool] = []

    class ExplodingNumber:
        def __radd__(self, _left: object) -> object:
            raise RuntimeError("addition failed")

    def values() -> Iterator[dict[str, object]]:
        try:
            yield {"key": "x", "value": ExplodingNumber()}
        finally:
            closed.append(True)

    with pytest.raises(RuntimeError, match="addition failed"):
        fpstreams.rows(values()).group_by("key").aggregate(
            total=fpstreams.agg.sum("value")
        ).to_list()
    assert closed == [True]

    lookup_events: list[str] = []

    class UnhashableKeyRow(dict[str, object]):
        def __getitem__(self, key: str) -> object:
            lookup_events.append(key)
            return super().__getitem__(key)

    with pytest.raises(TypeError, match="hashable"):
        fpstreams.rows([UnhashableKeyRow(key=[], value=1)]).group_by("key").aggregate(
            total=fpstreams.agg.sum("value")
        ).to_list()
    assert lookup_events == ["key"]

    nan_result = (
        fpstreams.rows([{"key": "x", "value": float("nan")}])
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
        .first()["total"]
    )
    assert nan_result != nan_result
    with pytest.raises(TypeError):
        fpstreams.rows([{"key": "x", "value": None}]).group_by("key").aggregate(
            total=fpstreams.agg.sum("value")
        ).to_list()


def test_rows_names_computed_group_keys_and_supports_boolean_aggregations() -> None:
    records = [
        {"team": "a", "amount": 10, "paid": True},
        {"team": "a", "amount": 15, "paid": False},
        {"team": "a", "amount": 30, "paid": True},
        {"team": "b", "amount": 5, "paid": True},
    ]
    result = (
        fpstreams.rows(records)
        .group_by("team", band=lambda row: "high" if row["amount"] >= 20 else "low")
        .aggregate(
            rows=fpstreams.agg.count(),
            paid=fpstreams.agg.count_where("paid"),
            any_large=fpstreams.agg.any(lambda row: row["amount"] >= 30),
            all_positive=fpstreams.agg.all(lambda row: row["amount"] > 0),
        )
        .to_list()
    )

    assert result == [
        {
            "team": "a",
            "band": "low",
            "rows": 2,
            "paid": 1,
            "any_large": False,
            "all_positive": True,
        },
        {
            "team": "a",
            "band": "high",
            "rows": 1,
            "paid": 1,
            "any_large": True,
            "all_positive": True,
        },
        {
            "team": "b",
            "band": "low",
            "rows": 1,
            "paid": 1,
            "any_large": False,
            "all_positive": True,
        },
    ]
    assert fpstreams.rows([]).aggregate(
        any_paid=fpstreams.agg.any("paid"),
        all_paid=fpstreams.agg.all("paid"),
        paid=fpstreams.agg.count_where("paid"),
    ).first() == {"any_paid": False, "all_paid": True, "paid": 0}
    with pytest.raises(fpstreams.DuplicateKeyError):
        fpstreams.rows(records).group_by("team", team="paid")
    with pytest.raises(TypeError, match="hashable"):
        fpstreams.rows([{"tags": ["x"]}]).group_by("tags").aggregate(
            rows=fpstreams.agg.count()
        ).to_list()


def test_rows_global_aggregate_is_chainable_and_handles_empty_input() -> None:
    summary = fpstreams.rows(
        [
            {"score": 1.0, "tag": ["x"]},
            {"score": 2.0, "tag": ["x"]},
            {"score": 3.0, "tag": ["y"]},
        ]
    ).aggregate(
        rows=fpstreams.agg.count(),
        total=fpstreams.agg.sum("score"),
        variance=fpstreams.agg.variance("score"),
        tags=fpstreams.agg.count_distinct("tag"),
    )

    expected = [{"rows": 3, "total": 6.0, "variance": 1.0, "tags": 2}]
    assert summary.to_list() == expected
    assert summary.to_list() == expected
    assert fpstreams.rows([]).aggregate(
        rows=fpstreams.agg.count(), mean=fpstreams.agg.mean("score")
    ).to_list() == [{"rows": 0, "mean": None}]
    with pytest.raises(ValueError, match="aggregate"):
        fpstreams.rows([]).aggregate()


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


def test_rows_rejects_ambiguous_output_columns() -> None:
    assert (
        fpstreams.rows([{"id": 1, "value": "left", "value_right": "unused"}])
        .join([{"id": 2, "value": "right"}], on="id", how="inner")
        .to_list()
        == []
    )

    with pytest.raises(fpstreams.DuplicateKeyError):
        fpstreams.rows([{"id": 1, "value": "left", "value_right": "preserve"}]).join(
            [{"id": 1, "value": "right"}], on="id"
        ).to_list()

    with pytest.raises(fpstreams.DuplicateKeyError):
        fpstreams.rows([{"a": 1, "b": 2}]).select("a", a="b")

    with pytest.raises(fpstreams.DuplicateKeyError):
        fpstreams.rows([{"team": "x", "score": 1}]).group_by("team").aggregate(
            team=fpstreams.agg.count()
        )


def test_in_memory_join_keeps_source_failure_primary_and_closes_that_source() -> None:
    primary = ValueError("left source failed")

    class Source(Iterator[dict[str, int]]):
        def __init__(self) -> None:
            self.pulls = 0
            self.close_calls = 0

        def __next__(self) -> dict[str, int]:
            self.pulls += 1
            if self.pulls == 1:
                return {"id": 1}
            raise primary

        def close(self) -> None:
            self.close_calls += 1
            raise OSError("left source close failed")

    source = Source()
    with pytest.raises(ValueError) as captured:
        fpstreams.rows(source).join([{"id": 1}], on="id").to_list()

    assert source.pulls == 2
    assert captured.value is primary
    assert captured.value.__notes__ == ["cleanup failed with OSError: left source close failed"]
    assert source.close_calls == 1


@pytest.mark.parametrize("failing_side", ["left", "right"])
def test_unique_right_join_keeps_key_failure_primary_when_source_close_fails(
    failing_side: str,
) -> None:
    primary = ValueError(f"{failing_side} key failed")

    class Source(Iterator[dict[str, int]]):
        def __init__(self) -> None:
            self.emitted = False
            self.close_calls = 0

        def __next__(self) -> dict[str, int]:
            if self.emitted:
                raise StopIteration
            self.emitted = True
            return {"id": 1}

        def close(self) -> None:
            self.close_calls += 1
            raise OSError(f"{failing_side} source close failed")

    def fail_key(_row: object) -> int:
        raise primary

    def direct_key(row: dict[str, int]) -> int:
        return row["id"]

    source = Source()
    joined = (
        fpstreams.rows(source).join(
            [{"id": 1}],
            left_on=fail_key,
            right_on=direct_key,
            validate="m:1",
        )
        if failing_side == "left"
        else fpstreams.rows([{"id": 1}]).join(
            source,
            left_on=direct_key,
            right_on=fail_key,
            validate="m:1",
        )
    ).with_engine("python")

    with pytest.raises(ValueError) as captured:
        joined.to_list()

    assert captured.value is primary
    assert captured.value.__notes__ == [
        f"cleanup failed with OSError: {failing_side} source close failed"
    ]
    assert source.close_calls == 1


def test_rows_rejects_duplicate_derived_group_and_pivot_keys() -> None:
    records = [{"left": {"id": 1}, "right": {"id": 2}, "value": 3}]

    with pytest.raises(fpstreams.DuplicateKeyError):
        fpstreams.rows(records).group_by("left.id", "right.id").aggregate(
            total=fpstreams.agg.sum("value")
        )

    with pytest.raises(fpstreams.DuplicateKeyError):
        fpstreams.rows(records).pivot(
            index=("left.id", "right.id"),
            columns="value",
            values="value",
        )


def test_rows_stream_csv_and_json_lines_round_trips(tmp_path) -> None:
    records = [{"name": "Ada", "score": 10}, {"name": "Lin", "score": 20}]
    csv_path = tmp_path / "scores.csv"
    jsonl_path = tmp_path / "scores.jsonl"

    fpstreams.rows(records).to_csv(csv_path)
    csv_rows = fpstreams.rows.from_csv(csv_path)
    assert csv_rows.to_list() == [
        {"name": "Ada", "score": "10"},
        {"name": "Lin", "score": "20"},
    ]
    assert csv_rows.to_list()[0]["name"] == "Ada"

    fpstreams.rows(records).to_jsonl(jsonl_path)
    assert fpstreams.rows.from_jsonl(jsonl_path).to_list() == records


@pytest.mark.parametrize("sink", ["to_csv", "to_jsonl"])
def test_rows_text_sinks_keep_record_error_primary_when_generator_close_fails(
    tmp_path: Path,
    sink: str,
) -> None:
    def records() -> Iterator[object]:
        try:
            yield object()
        finally:
            raise OSError("row source close failed")

    with pytest.raises(fpstreams.SelectionError) as captured:
        getattr(fpstreams.rows(records()), sink)(tmp_path / sink)

    assert captured.value.__notes__ == ["cleanup failed with OSError: row source close failed"]


def test_rows_text_factories_keep_the_v2_path_keyword(tmp_path: Path) -> None:
    csv_path = tmp_path / "records.csv"
    csv_path.write_text("id\n1\n", encoding="utf-8")
    jsonl_path = tmp_path / "records.jsonl"
    jsonl_path.write_text('{"id": 1}\n', encoding="utf-8")

    assert fpstreams.rows.from_csv(path=csv_path).to_list() == [{"id": "1"}]
    assert fpstreams.Rows.from_csv(path=csv_path).to_list() == [{"id": "1"}]
    assert fpstreams.rows.from_jsonl(path=jsonl_path).to_list() == [{"id": 1}]
    assert fpstreams.Rows.from_jsonl(path=jsonl_path).to_list() == [{"id": 1}]


def test_rows_csv_file_handles_and_openers_have_explicit_ownership() -> None:
    handle = io.StringIO("ignored prefix\nid,name\n1,Ada\n2,Lin\n")
    assert handle.readline() == "ignored prefix\n"
    one_shot = fpstreams.rows.from_csv(handle)

    assert one_shot.first() == {"id": "1", "name": "Ada"}
    assert not handle.closed
    with pytest.raises(fpstreams.FlowConsumedError):
        one_shot.to_list()

    opened: list[io.StringIO] = []

    def opener() -> io.StringIO:
        source = io.StringIO("id,name\n1,Ada\n2,Lin\n")
        opened.append(source)
        return source

    replayable = fpstreams.rows.from_csv(opener)
    expected = [{"id": "1", "name": "Ada"}, {"id": "2", "name": "Lin"}]
    assert replayable.first() == expected[0]
    assert opened[-1].closed
    assert replayable.to_list() == expected
    assert replayable.to_list() == expected
    assert len(opened) == 3
    assert all(source.closed for source in opened)


def test_rows_csv_identity_list_direct_sink_is_narrow_and_preserves_pep479(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fpstreams.tabular import _text_sources

    path = tmp_path / "identity.csv"
    path.write_text("id,name\n1,Ada\n2,Lin\n", encoding="utf-8")
    calls: list[str] = []
    original = _text_sources.CSVRowSource.materialize

    def tracked(source: _text_sources.CSVRowSource) -> list[dict[str, Any]]:
        calls.append(source.kind)
        return original(source)

    monkeypatch.setattr(_text_sources.CSVRowSource, "materialize", tracked)
    expected = [{"id": "1", "name": "Ada"}, {"id": "2", "name": "Lin"}]

    assert fpstreams.rows.from_csv(path).to_list() == expected
    assert calls == ["path"]
    calls.clear()
    assert fpstreams.rows.from_csv(path).select("id").to_list() == [
        {"id": "1"},
        {"id": "2"},
    ]
    assert fpstreams.rows.from_csv(path).with_engine("python").to_list() == expected
    assert calls == []

    def stopped_opener() -> io.StringIO:
        raise StopIteration("stopped")

    with pytest.raises(RuntimeError, match="generator raised StopIteration"):
        fpstreams.rows.from_csv(stopped_opener).to_list()


def test_rows_csv_direct_sink_revalidates_bound_source_method_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retained CSV metadata must not outlive its bound Python opener method."""
    rows = fpstreams.rows.from_csv(lambda: io.StringIO("value\n1\n"))
    from fpstreams.tabular import _text_sources

    def replacement(_source: object) -> Iterator[dict[str, str]]:
        yield {"value": "9"}

    monkeypatch.setattr(
        _text_sources.CSVRowSource.open_records,
        "__code__",
        replacement.__code__,
    )
    expected = [{"value": "9"}]

    assert rows.with_engine("python").to_list() == expected
    assert rows.to_list() == expected


def test_rows_csv_identity_list_declines_failpoints(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fpstreams.runtime.failpoints import failpoint
    from fpstreams.tabular import _text_sources

    path = tmp_path / "observed.csv"
    path.write_text("id\n1\n", encoding="utf-8")

    def unexpected_materialization(_source: _text_sources.CSVRowSource) -> list[dict[str, Any]]:
        raise AssertionError("instrumented execution must use the canonical pipeline")

    monkeypatch.setattr(_text_sources.CSVRowSource, "materialize", unexpected_materialization)
    with failpoint("unrelated.transition", RuntimeError("unused")):
        assert fpstreams.rows.from_csv(path).to_list() == [{"id": "1"}]

    failure = RuntimeError("observed source open")
    with failpoint("source.open.after", failure), pytest.raises(RuntimeError) as raised:
        fpstreams.rows.from_csv(path).to_list()
    assert raised.value is failure

    monkeypatch.undo()
    execution = fpstreams.rows.from_csv(path).run_with_report("to_list")
    assert execution.value == [{"id": "1"}]
    assert execution.report.terminal == "to_list"
    assert execution.report.compiler_engine == "not_compiled"
    assert execution.report.strategy == "csv_direct"


def test_rows_jsonl_file_handles_and_openers_preserve_byte_limits() -> None:
    payload = b'{"id":1}\n{"id":2}\n'
    handle = io.BytesIO(payload)
    one_shot = fpstreams.rows.from_jsonl(handle, max_record_bytes=9)

    assert one_shot.first() == {"id": 1}
    assert not handle.closed
    with pytest.raises(fpstreams.FlowConsumedError):
        one_shot.to_list()

    opened: list[io.StringIO] = []

    def opener() -> io.StringIO:
        source = io.StringIO('{"word":"雪"}\n')
        opened.append(source)
        return source

    replayable = fpstreams.rows.from_jsonl(opener, max_record_bytes=17)
    assert replayable.first() == {"word": "雪"}
    assert opened[-1].closed
    assert replayable.to_list() == [{"word": "雪"}]
    assert replayable.to_list() == [{"word": "雪"}]
    assert len(opened) == 3
    assert all(source.closed for source in opened)


@pytest.mark.parametrize(
    ("kind", "payload", "error_type"),
    [
        ("csv", "id,id\n1,2\n", fpstreams.DuplicateKeyError),
        ("jsonl", "{invalid}\n", json.JSONDecodeError),
    ],
)
def test_text_opener_close_failure_is_a_note_on_the_parse_error(
    kind: str,
    payload: str,
    error_type: type[BaseException],
) -> None:
    class Handle(io.StringIO):
        def __init__(self, value: str) -> None:
            super().__init__(value)
            self.close_calls = 0

        def close(self) -> None:
            self.close_calls += 1
            super().close()
            raise OSError("text handle close failed")

    opened: list[Handle] = []

    def opener() -> Handle:
        handle = Handle(payload)
        opened.append(handle)
        return handle

    values = fpstreams.rows.from_csv(opener) if kind == "csv" else fpstreams.rows.from_jsonl(opener)
    with pytest.raises(error_type) as captured:
        values.to_list()

    assert captured.value.__notes__ == ["cleanup failed with OSError: text handle close failed"]
    assert len(opened) == 1
    assert opened[0].close_calls == 1

    truncated_unicode = io.BytesIO('{"word":"雪"}\n'.encode())
    with pytest.raises(fpstreams.BufferLimitError, match="max_record_bytes"):
        fpstreams.rows.from_jsonl(truncated_unicode, max_record_bytes=10).to_list()
    assert not truncated_unicode.closed


def test_scan_csv_is_reusable_typed_and_keeps_from_csv_string_semantics(tmp_path: Path) -> None:
    """The Arrow scanner is explicit, so the established CSV adapter stays compatible."""
    import pyarrow.csv as pacsv

    path = tmp_path / "typed.csv"
    path.write_text("id,name,active\n1,Ada,true\n2,Lin,false\n", encoding="utf-8")

    scanned = fpstreams.rows.scan_csv(path, batch_size=1)
    expected = [
        {"id": 1, "name": "Ada", "active": True},
        {"id": 2, "name": "Lin", "active": False},
    ]
    assert scanned.to_list() == expected
    assert scanned.to_list() == expected
    assert fpstreams.rows.from_csv(path).first() == {
        "id": "1",
        "name": "Ada",
        "active": "true",
    }
    # Explicit column types exercise the same option path on every supported PyArrow release.
    strings = pacsv.ConvertOptions(
        column_types={name: pa.string() for name in ("id", "name", "active")}
    )
    assert fpstreams.rows.scan_csv(path, convert_options=strings).first() == {
        "id": "1",
        "name": "Ada",
        "active": "true",
    }


def test_scan_csv_pushes_query_projection_with_filter_dependencies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Default scans parse only output and filter fields once a closed query is known."""
    from fpstreams.execution import arrow as arrow_execution

    path = tmp_path / "wide.csv"
    path.write_text(
        "id,payload,unused_a,unused_b\n1,one,10,100\n2,two,20,200\n3,three,30,300\n",
        encoding="utf-8",
    )
    schemas: list[tuple[str, ...]] = []
    execute_batch_program = arrow_execution._execute_batch_program

    def traced_batch_program(
        batch: object, operations: tuple[object, ...], projection: object
    ) -> Iterator[object]:
        schemas.append(tuple(batch.schema.names))  # type: ignore[attr-defined]
        yield from execute_batch_program(batch, operations, projection)

    monkeypatch.setattr(arrow_execution, "_execute_batch_program", traced_batch_program)

    result = (
        fpstreams.rows.scan_csv(path, batch_size=2)
        .where(fpstreams.col("id") >= 2)
        .select("payload")
        .to_list()
    )

    assert result == [{"payload": "two"}, {"payload": "three"}]
    assert schemas == [("payload", "id"), ("payload", "id")]


def test_scan_csv_uses_a_bounded_default_projection_probe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Projection discovery reads a small header block before the ordinary typed stream."""
    import pyarrow.csv as pacsv

    path = tmp_path / "probe.csv"
    path.write_text("id,unused\n1,10\n2,20\n", encoding="utf-8")
    block_sizes: list[int | None] = []
    open_csv = pacsv.open_csv

    def traced_open_csv(*args: object, **options: object) -> object:
        configured = options.get("read_options")
        block_sizes.append(None if configured is None else configured.block_size)  # type: ignore[attr-defined]
        return open_csv(*args, **options)

    monkeypatch.setattr(pacsv, "open_csv", traced_open_csv)

    assert fpstreams.rows.scan_csv(path).select("id").to_list() == [{"id": 1}, {"id": 2}]
    assert block_sizes == [64 * 1024, None]


@pytest.mark.parametrize("long_header", [False, True])
def test_scan_csv_projection_probe_retries_records_larger_than_its_block(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, long_header: bool
) -> None:
    """A straddling header or first record falls back to Arrow's default inference block."""
    import pyarrow.csv as pacsv

    path = tmp_path / "long.csv"
    long_value = "x" * 150_000
    if long_header:
        path.write_text(f"{long_value},id\nignored,1\n", encoding="utf-8")
    else:
        path.write_text(f"payload,id\n{long_value},1\n", encoding="utf-8")
    block_sizes: list[int | None] = []
    open_csv = pacsv.open_csv

    def traced_open_csv(*args: object, **options: object) -> object:
        configured = options.get("read_options")
        block_sizes.append(None if configured is None else configured.block_size)  # type: ignore[attr-defined]
        return open_csv(*args, **options)

    monkeypatch.setattr(pacsv, "open_csv", traced_open_csv)

    assert fpstreams.rows.scan_csv(path).select("id").to_list() == [{"id": 1}]
    assert block_sizes == [64 * 1024, None, None]


def test_scan_csv_projection_probe_preserves_later_parse_error_precedence(tmp_path: Path) -> None:
    """A small schema probe cannot displace the default reader's later parse failure."""
    import pyarrow as pa

    path = tmp_path / "duplicate-header-late-ragged.csv"
    rows = ["id,id", *(f"{index},{index}" for index in range(9000)), "only-one"]
    path.write_text("\n".join(rows), encoding="utf-8")
    assert path.stat().st_size > 64 * 1024

    with pytest.raises(pa.ArrowInvalid) as baseline:
        fpstreams.rows.scan_csv(path).to_list()
    with pytest.raises(pa.ArrowInvalid) as projected:
        fpstreams.rows.scan_csv(path).select("id").to_list()

    assert str(projected.value) == str(baseline.value)


def test_scan_csv_preserves_deferred_and_missing_field_behavior(tmp_path: Path) -> None:
    """Projection discovery must not turn missing fields into eager Arrow schema errors."""
    path = tmp_path / "scope.csv"
    path.write_text("present,hidden\n1,7\n", encoding="utf-8")

    with pytest.raises(fpstreams.SelectionError) as canonical:
        fpstreams.rows([{"present": 1, "hidden": 7}]).select("missing").to_list()
    with pytest.raises(fpstreams.SelectionError) as projected:
        fpstreams.rows.scan_csv(path).select("missing").to_list()
    assert str(projected.value) == str(canonical.value)
    assert type(projected.value.__cause__) is type(canonical.value.__cause__) is KeyError

    header_only = tmp_path / "header-only.csv"
    header_only.write_text("present\n", encoding="utf-8")
    assert fpstreams.rows.scan_csv(header_only).select("missing").to_list() == []

    missing = tmp_path / "missing.csv"
    deferred = fpstreams.rows.scan_csv(missing)
    with pytest.raises(FileNotFoundError):
        deferred.to_list()


def test_scan_csv_closes_early_terminal_and_projected_streams(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Both short-circuited reading and projected probe/data pairs release their streams."""
    path = tmp_path / "close.csv"
    path.write_text("id,unused\n1,10\n2,20\n", encoding="utf-8")
    opened: list[object] = []
    input_stream = pa.input_stream

    def tracked_input_stream(*args: object, **kwargs: object) -> object:
        stream = input_stream(*args, **kwargs)
        opened.append(stream)
        return stream

    monkeypatch.setattr(pa, "input_stream", tracked_input_stream)

    assert fpstreams.rows.scan_csv(path).first() == {"id": 1, "unused": 10}
    assert len(opened) == 1
    assert opened[0].closed  # type: ignore[attr-defined]

    opened.clear()
    assert fpstreams.rows.scan_csv(path).select("id").to_list() == [{"id": 1}, {"id": 2}]
    assert len(opened) == 2
    assert all(stream.closed for stream in opened)  # type: ignore[attr-defined]


def test_scan_csv_does_not_repeat_invalid_row_callbacks_for_projection(tmp_path: Path) -> None:
    """A user parse callback disables the schema-probe optimization to remain exactly-once."""
    import pyarrow.csv as pacsv

    path = tmp_path / "invalid.csv"
    path.write_text("id,value\n1,one\n2,two,extra\n3,three\n", encoding="utf-8")
    invalid_rows: list[str] = []

    def skip_invalid(row: object) -> str:
        invalid_rows.append(row.text)  # type: ignore[attr-defined]
        return "skip"

    options = pacsv.ParseOptions(invalid_row_handler=skip_invalid)
    assert fpstreams.rows.scan_csv(path, parse_options=options).select("id").to_list() == [
        {"id": 1},
        {"id": 3},
    ]
    assert invalid_rows == ["2,two,extra"]

    invalid_rows.clear()
    assert fpstreams.rows.scan_csv(path, parse_options=options).select("id").first() == {"id": 1}
    assert invalid_rows == ["2,two,extra"]


def test_scan_csv_parse_options_mutation_cannot_enable_duplicate_callbacks(tmp_path: Path) -> None:
    """A supplied mutable ParseOptions keeps the query on the one-open batch projection path."""
    import pyarrow.csv as pacsv

    path = tmp_path / "mutable-invalid.csv"
    path.write_text("id,value\n1,one\n2,two,extra\n3,three\n", encoding="utf-8")
    options = pacsv.ParseOptions()
    query = fpstreams.rows.scan_csv(path, parse_options=options).select("id")
    invalid_rows: list[str] = []

    def skip_invalid(row: object) -> str:
        invalid_rows.append(row.text)  # type: ignore[attr-defined]
        return "skip"

    options.invalid_row_handler = skip_invalid

    assert query.to_list() == [{"id": 1}, {"id": 3}]
    assert invalid_rows == ["2,two,extra"]


def test_scan_csv_keeps_arrow_empty_ragged_and_missing_column_contracts(tmp_path: Path) -> None:
    """The typed scanner intentionally exposes Arrow's stricter structural CSV semantics."""
    import pyarrow.csv as pacsv

    empty = tmp_path / "empty.csv"
    empty.write_bytes(b"")
    with pytest.raises(pa.ArrowInvalid, match="Empty CSV"):
        fpstreams.rows.scan_csv(empty).to_list()

    ragged = tmp_path / "ragged.csv"
    ragged.write_text("id,value\n1\n", encoding="utf-8")
    with pytest.raises(pa.ArrowInvalid, match="Expected 2 columns"):
        fpstreams.rows.scan_csv(ragged).to_list()

    regular = tmp_path / "regular.csv"
    regular.write_text("id\n1\n", encoding="utf-8")
    strict = pacsv.ConvertOptions(include_columns=["id", "missing"])
    with pytest.raises(pa.ArrowKeyError, match="missing"):
        fpstreams.rows.scan_csv(regular, convert_options=strict).to_list()
    nullable = pacsv.ConvertOptions(include_columns=["id", "missing"], include_missing_columns=True)
    assert fpstreams.rows.scan_csv(regular, convert_options=nullable).to_list() == [
        {"id": 1, "missing": None}
    ]


def test_rows_csv_can_neutralize_spreadsheet_formula_cells(tmp_path) -> None:
    path = tmp_path / "safe-rows.csv"

    fpstreams.rows([{"formula": "+1", "spaced": "  -2", "command": "@run", "number": -3}]).to_csv(
        path, spreadsheet_safe=True
    )

    assert path.read_text(encoding="utf-8") == (
        "formula,spaced,command,number\n'+1,'  -2,'@run,-3\n"
    )


def test_jsonl_record_limit_counts_bytes_before_decoding(tmp_path) -> None:
    path = tmp_path / "bounded.jsonl"
    payload = '{"word":"雪"}\n'.encode()
    path.write_bytes(payload)

    assert fpstreams.rows.from_jsonl(path, max_record_bytes=len(payload)).to_list() == [
        {"word": "雪"}
    ]
    assert fpstreams.rows.from_jsonl(path, max_record_bytes=None).to_list() == [{"word": "雪"}]
    with pytest.raises(fpstreams.BufferLimitError, match="max_record_bytes"):
        fpstreams.rows.from_jsonl(path, max_record_bytes=len(payload) - 1).to_list()


def test_unbounded_jsonl_path_uses_incremental_text_decoding(tmp_path: Path) -> None:
    """Multibyte newline encodings must not be split by a binary readline boundary."""
    path = tmp_path / "utf16.jsonl"
    path.write_text('{"word":"雪"}\n{"word":"月"}\n', encoding="utf-16")

    assert fpstreams.rows.from_jsonl(path, encoding="utf-16", max_record_bytes=None).to_list() == [
        {"word": "雪"},
        {"word": "月"},
    ]


def test_jsonl_multibyte_newlines_work_for_binary_sources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Wide binary sources preserve byte bounds, record edges, and handle ownership."""
    first_line = '{"word":"雪\u010a\u0a01"}\n'
    second_line = '{"word":"月"}\n'
    expected = [{"word": "雪\u010a\u0a01"}, {"word": "月"}]
    original_open = open
    path_handles: list[Any] = []

    def tracked_open(*args: Any, **kwargs: Any) -> Any:
        handle = original_open(*args, **kwargs)
        path_handles.append(handle)
        return handle

    monkeypatch.setattr("builtins.open", tracked_open)

    for encoding, width in (("utf-16", 2), ("utf-32", 4)):
        payload = (first_line + second_line).encode(encoding)
        first_record_bytes = width + len(first_line.encode(f"{encoding}-le"))
        second_record_bytes = len(second_line.encode(f"{encoding}-le"))
        assert len(payload) == first_record_bytes + second_record_bytes
        assert first_record_bytes > second_record_bytes
        path = tmp_path / f"{encoding}.jsonl"
        path.write_bytes(payload)

        for record_limit in (None, first_record_bytes):
            assert (
                fpstreams.rows.from_jsonl(
                    path,
                    encoding=encoding,
                    max_record_bytes=record_limit,
                ).to_list()
                == expected
            )

            direct = io.BytesIO(payload)
            assert (
                fpstreams.rows.from_jsonl(
                    direct,
                    encoding=encoding,
                    max_record_bytes=record_limit,
                ).to_list()
                == expected
            )
            assert not direct.closed

            opened: list[io.BytesIO] = []

            def opener(
                payload: bytes = payload,
                opened: list[io.BytesIO] = opened,
            ) -> io.BytesIO:
                handle = io.BytesIO(payload)
                opened.append(handle)
                return handle

            assert (
                fpstreams.rows.from_jsonl(
                    opener,
                    encoding=encoding,
                    max_record_bytes=record_limit,
                ).to_list()
                == expected
            )
            assert len(opened) == 1
            assert opened[0].closed

            short_path_handle_count = len(path_handles)
            assert (
                fpstreams.rows.from_jsonl(
                    path,
                    encoding=encoding,
                    max_record_bytes=record_limit,
                )
                .take(1)
                .to_list()
                == expected[:1]
            )
            assert len(path_handles) == short_path_handle_count + 1
            assert path_handles[-1].closed

            direct = io.BytesIO(payload)
            assert (
                fpstreams.rows.from_jsonl(
                    direct,
                    encoding=encoding,
                    max_record_bytes=record_limit,
                )
                .take(1)
                .to_list()
                == expected[:1]
            )
            assert not direct.closed
            assert direct.tell() == first_record_bytes

            assert (
                fpstreams.rows.from_jsonl(
                    opener,
                    encoding=encoding,
                    max_record_bytes=record_limit,
                )
                .take(1)
                .to_list()
                == expected[:1]
            )
            assert len(opened) == 2
            assert opened[-1].closed

        with pytest.raises(fpstreams.BufferLimitError, match="max_record_bytes"):
            fpstreams.rows.from_jsonl(
                path,
                encoding=encoding,
                max_record_bytes=first_record_bytes - 1,
            ).to_list()

        direct = io.BytesIO(payload)
        with pytest.raises(fpstreams.BufferLimitError, match="max_record_bytes"):
            fpstreams.rows.from_jsonl(
                direct,
                encoding=encoding,
                max_record_bytes=first_record_bytes - 1,
            ).to_list()
        assert not direct.closed

    assert path_handles
    assert all(handle.closed for handle in path_handles)


def test_jsonl_extra_data_error_matches_the_standard_decoder() -> None:
    """A line fast path must preserve JSON's narrow trailing-whitespace rules."""
    line = '{"id":1} \v\n'
    with pytest.raises(json.JSONDecodeError) as expected:
        json.JSONDecoder().decode(line)
    with pytest.raises(json.JSONDecodeError) as actual:
        fpstreams.rows.from_jsonl(io.StringIO(line), max_record_bytes=None).to_list()

    assert (actual.value.msg, actual.value.pos, actual.value.lineno, actual.value.colno) == (
        expected.value.msg,
        expected.value.pos,
        expected.value.lineno,
        expected.value.colno,
    )


def test_unbounded_jsonl_text_handle_does_not_reencode_records() -> None:
    """Without a byte limit, decoded text should go directly to the JSON decoder."""

    class DecodedLine(str):
        def encode(self, *_args: object, **_kwargs: object) -> bytes:
            raise AssertionError("an unbounded text record must not be encoded again")

    class TextHandle:
        def __init__(self) -> None:
            self.lines = [DecodedLine('{"id":1}\n')]

        def __iter__(self) -> Iterator[str]:
            return iter(self.lines)

        def readline(self, _size: int = -1) -> str:
            return self.lines.pop(0) if self.lines else ""

    assert fpstreams.rows.from_jsonl(TextHandle(), max_record_bytes=None).to_list() == [{"id": 1}]


def test_jsonl_reuses_one_strict_decoder_per_iteration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Decoder setup is per file scan, not repeated for every physical record."""
    path = tmp_path / "decoder.jsonl"
    path.write_text('\n{"word":"café"}\n{"nested":{"id":2}}\n', encoding="latin-1")
    original_decoder = json.JSONDecoder
    constructed: list[None] = []

    class TrackingDecoder(original_decoder):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            constructed.append(None)
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(json, "JSONDecoder", TrackingDecoder)
    source = fpstreams.rows.from_jsonl(path, encoding="latin-1")

    assert source.to_list() == [{"word": "café"}, {"nested": {"id": 2}}]
    assert len(constructed) == 1
    assert source.to_list() == [{"word": "café"}, {"nested": {"id": 2}}]
    assert len(constructed) == 2


def test_jsonl_short_circuit_closes_the_bounded_binary_reader(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "close.jsonl"
    path.write_text('{"id":1}\n{"id":2}\n', encoding="utf-8")
    original_open = open
    opened: list[Any] = []

    def tracked_open(*args: Any, **kwargs: Any) -> Any:
        handle = original_open(*args, **kwargs)
        opened.append(handle)
        return handle

    monkeypatch.setattr("builtins.open", tracked_open)

    assert fpstreams.rows.from_jsonl(path).take(1).to_list() == [{"id": 1}]
    assert len(opened) == 1
    assert opened[0].closed


def test_jsonl_record_limit_is_validated_before_opening_the_file(tmp_path) -> None:
    missing = tmp_path / "missing.jsonl"

    with pytest.raises(ValueError, match="max_record_bytes"):
        fpstreams.rows.from_jsonl(missing, max_record_bytes=0)
    with pytest.raises(TypeError, match="max_record_bytes"):
        fpstreams.rows.from_jsonl(missing, max_record_bytes=2.5)  # type: ignore[arg-type]

    deferred = fpstreams.rows.from_jsonl(missing)
    with pytest.raises(FileNotFoundError):
        deferred.to_list()


def test_rows_io_rejects_duplicate_fields_instead_of_losing_data(tmp_path) -> None:
    csv_path = tmp_path / "duplicate.csv"
    jsonl_path = tmp_path / "duplicate.jsonl"
    csv_path.write_text("id,id\n1,2\n", encoding="utf-8")
    jsonl_path.write_text(
        '\n{"ok":true}\n{"user":{"id":1,"id":2}}\n',
        encoding="utf-8",
    )

    with pytest.raises(fpstreams.DuplicateKeyError, match="CSV"):
        fpstreams.rows.from_csv(csv_path).to_list()
    with pytest.raises(fpstreams.DuplicateKeyError, match="Arrow schema"):
        fpstreams.rows.scan_csv(csv_path).to_list()
    with pytest.raises(
        fpstreams.DuplicateKeyError,
        match="JSON Lines record 3 contains duplicate key 'id'",
    ):
        fpstreams.rows.from_jsonl(jsonl_path).to_list()
    with pytest.raises(fpstreams.DuplicateKeyError, match="to_csv"):
        fpstreams.rows([{"id": 1}]).to_csv(csv_path, fieldnames=("id", "id"))


def test_rows_clean_nested_records_in_one_readable_pipeline() -> None:
    records = [
        {
            "id": "1",
            "amount": "2.5",
            "country": None,
            "fallback": "US",
            "tags": ["new", "paid"],
            "profile": {"age": "30", "city": "Boston"},
        },
        {
            "id": "2",
            "amount": None,
            "country": "CA",
            "fallback": None,
            "tags": ["ignored"],
            "profile": {"age": "40", "city": "Toronto"},
        },
        {
            "id": "3",
            "amount": "7",
            "country": None,
            "fallback": None,
            "tags": [],
            "profile": {"age": "25", "city": "Paris"},
        },
    ]

    cleaned = (
        fpstreams.rows(records)
        .drop_nulls("amount")
        .cast(id=int, amount=float)
        .fill_nulls(country=fpstreams.coalesce(fpstreams.col("fallback"), "unknown"))
        .explode("tags", outer=True)
        .unnest("profile", prefix="profile_")
        .drop("fallback")
        .to_list()
    )

    assert cleaned == [
        {
            "id": 1,
            "amount": 2.5,
            "country": "US",
            "tags": "new",
            "profile_age": "30",
            "profile_city": "Boston",
        },
        {
            "id": 1,
            "amount": 2.5,
            "country": "US",
            "tags": "paid",
            "profile_age": "30",
            "profile_city": "Boston",
        },
        {
            "id": 3,
            "amount": 7.0,
            "country": "unknown",
            "tags": None,
            "profile_age": "25",
            "profile_city": "Paris",
        },
    ]
    assert records[0]["id"] == "1"
    assert records[0]["tags"] == ["new", "paid"]


def test_rows_cast_copies_exact_dicts_without_the_generic_record_adapter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The common dictionary path stays direct while record subclasses retain protocol handling."""
    from importlib import import_module

    rows_module = import_module("fpstreams.tabular.rows")

    original_as_record = rows_module._as_record
    adapted: list[object] = []

    def tracked_as_record(row: object) -> dict[str, Any]:
        adapted.append(row)
        return original_as_record(row)

    class DictSubclass(dict[str, object]):
        pass

    exact: dict[str, object] = {"id": "1", "amount": "2.5"}
    fallback = DictSubclass(id="2", amount="3.5")
    monkeypatch.setattr(rows_module, "_as_record", tracked_as_record)

    result = fpstreams.rows([exact, fallback]).cast(id=int, amount=float).to_list()

    assert result == [{"id": 1, "amount": 2.5}, {"id": 2, "amount": 3.5}]
    assert adapted == [fallback]
    assert exact == {"id": "1", "amount": "2.5"}
    assert fallback == {"id": "2", "amount": "3.5"}


def test_rows_fill_nulls_keeps_literals_direct_and_expressions_dynamic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Literal replacements avoid expression dispatch while RowExpr values remain dynamic."""
    from importlib import import_module

    from fpstreams.expressions.row import RowExpr

    rows_module = import_module("fpstreams.tabular.rows")
    original_as_record = rows_module._as_record
    original_call = RowExpr.__call__
    adapted: list[object] = []
    evaluated: list[RowExpr] = []

    def tracked_as_record(row: object) -> dict[str, Any]:
        adapted.append(row)
        return original_as_record(row)

    def tracked_call(expression: RowExpr, row: object) -> object:
        evaluated.append(expression)
        return original_call(expression, row)

    class DictSubclass(dict[str, object]):
        pass

    marker: list[int] = []

    def callable_literal() -> str:
        return "literal"

    dynamic = fpstreams.col("seed") + 1
    exact: dict[str, object] = {
        "seed": 1,
        "literal": None,
        "callable": None,
        "dynamic": None,
    }
    fallback = DictSubclass(seed=2, literal=None, callable=None, dynamic=None)
    monkeypatch.setattr(rows_module, "_as_record", tracked_as_record)
    monkeypatch.setattr(RowExpr, "__call__", tracked_call)

    result = (
        fpstreams.rows([exact, fallback])
        .fill_nulls(
            literal=marker,
            callable=callable_literal,
            dynamic=dynamic,
        )
        .to_list()
    )

    assert result == [
        {"seed": 1, "literal": marker, "callable": callable_literal, "dynamic": 2},
        {"seed": 2, "literal": marker, "callable": callable_literal, "dynamic": 3},
    ]
    assert result[0]["literal"] is result[1]["literal"] is marker
    assert result[0]["callable"] is result[1]["callable"] is callable_literal
    assert adapted == [fallback]
    assert evaluated == [dynamic, dynamic]
    assert exact["literal"] is None
    assert fallback["literal"] is None


def test_drop_nulls_supports_any_all_and_heterogeneous_rows() -> None:
    records = [
        {"a": 1, "b": 2},
        {"a": 1, "b": None},
        {"a": None, "b": None},
        {},
    ]

    assert fpstreams.rows(records).drop_nulls("a", "b").to_list() == [records[0]]
    assert fpstreams.rows(records[:-1]).drop_nulls("a", "b", how="all").to_list() == records[:2]
    assert fpstreams.rows(records).drop_nulls(how="all").to_list() == records[:2]

    with pytest.raises(ValueError, match="how"):
        fpstreams.rows(records).drop_nulls(how="some")  # type: ignore[arg-type]


@pytest.mark.parametrize("field_count", [2, 3, 4, 5])
@pytest.mark.parametrize("how", ["any", "all"])
def test_drop_nulls_direct_field_counts_preserve_policy(
    field_count: int,
    how: str,
) -> None:
    """Specialized common widths and the generic wider loop share one null policy."""
    fields = tuple(f"field_{index}" for index in range(field_count))
    complete = dict.fromkeys(fields, 1)
    first_missing = {**complete, fields[0]: None}
    last_missing = {**complete, fields[-1]: None}
    all_missing = dict.fromkeys(fields)
    records = [complete, first_missing, last_missing, all_missing]
    expected = [complete] if how == "any" else records[:-1]

    assert (
        fpstreams.rows(records)
        .drop_nulls(
            *fields,
            how=cast(Any, how),
        )
        .to_list()
        == expected
    )


def test_drop_nulls_direct_field_fast_path_preserves_protocol_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only exact dictionaries with exact top-level strings bypass selector dispatch."""
    rows_module = sys.modules["fpstreams.tabular.rows"]
    canonical_compile = rows_module.compile_selector
    calls: list[tuple[object, type[object]]] = []

    def tracked_compile(selector: object):
        select = canonical_compile(selector)

        def tracked_select(row: object) -> object:
            calls.append((selector, type(row)))
            return select(row)

        return tracked_select

    class VirtualDict(dict[str, object]):
        def __getitem__(self, key: str) -> object:
            if key == "nullable":
                return 1
            return super().__getitem__(key)

    class LoggedMapping(Mapping[str, object]):
        def __init__(self, value: object) -> None:
            self.value = value

        def __getitem__(self, key: str) -> object:
            if key != "nullable":
                raise KeyError(key)
            return self.value

        def __iter__(self) -> Iterator[str]:
            return iter(("nullable",))

        def __len__(self) -> int:
            return 1

    monkeypatch.setattr(rows_module, "compile_selector", tracked_compile)
    exact_kept = {"nullable": 1}
    exact_missing: dict[str, object] = {}
    virtual = VirtualDict(nullable=None)
    mapping = LoggedMapping(None)

    result = (
        fpstreams.rows([exact_kept, exact_missing, virtual, mapping])
        .drop_nulls("nullable")
        .to_list()
    )

    assert result == [exact_kept, virtual]
    assert calls == [("nullable", VirtualDict), ("nullable", LoggedMapping)]

    class FieldName(str):
        pass

    calls.clear()
    field = FieldName("nullable")
    assert fpstreams.rows([exact_kept]).drop_nulls(field).to_list() == [exact_kept]
    assert calls == [(field, dict)]

    calls.clear()
    nested_rows = [{"nested": {}}, {"nested": {"nullable": 1}}]
    assert fpstreams.rows(nested_rows).drop_nulls("nested.nullable").to_list() == [nested_rows[1]]
    assert calls == [("nested.nullable", dict), ("nested.nullable", dict)]

    calls.clear()
    selected_values: list[object] = []

    def custom_selector(row: Mapping[str, object]) -> object:
        value = row.get("nullable")
        selected_values.append(value)
        return value

    assert fpstreams.rows([exact_missing, exact_kept]).drop_nulls(custom_selector).to_list() == [
        exact_kept
    ]
    assert calls == [(custom_selector, dict), (custom_selector, dict)]
    assert selected_values == [None, 1]


def test_drop_nulls_single_field_exposes_only_its_sealed_materialized_sink() -> None:
    """Only the project-owned one-field plan may bypass the lazy filter iterator."""
    rows_module = sys.modules["fpstreams.tabular.rows"]

    single = fpstreams.rows([{"nullable": 1}]).drop_nulls("nullable")
    multiple = fpstreams.rows([{"left": 1, "right": 2}]).drop_nulls("left", "right")
    dynamic = fpstreams.rows([{"nullable": 1}]).drop_nulls(lambda row: row["nullable"])

    single_predicate = single.to_flow()._pipeline.operations[0].predicate
    multiple_predicate = multiple.to_flow()._pipeline.operations[0].predicate
    dynamic_predicate = dynamic.to_flow()._pipeline.operations[0].predicate

    assert rows_module._materialized_drop_nulls_appender(single_predicate) is not None
    assert rows_module._materialized_drop_nulls_appender(multiple_predicate) is None
    assert rows_module._materialized_drop_nulls_appender(dynamic_predicate) is None


def test_retained_drop_nulls_uses_native_prefix_only_for_auto_and_reports(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Auto filters retained exact rows natively without changing forced-Python execution."""
    from fpstreams import _native

    records = [{"id": index, "nullable": None if index % 5 == 0 else index} for index in range(600)]
    expected = [row for row in records if row["nullable"] is not None]
    query = fpstreams.rows(records).drop_nulls("nullable")
    endpoint = _native.drop_nulls_exact_dict_prefix_v1
    native_calls = 0

    def tracked(*arguments: object) -> object:
        nonlocal native_calls
        native_calls += 1
        return endpoint(*arguments)

    monkeypatch.setattr(_native, "drop_nulls_exact_dict_prefix_v1", tracked)

    automatic = query.to_list()
    canonical = query.with_engine("python").to_list()
    tuple_result = fpstreams.rows(tuple(records)).drop_nulls("nullable").to_list()
    small = fpstreams.rows(records[:128]).drop_nulls("nullable").to_list()
    execution = query.run_with_report("to_list")

    assert automatic == canonical == tuple_result == expected
    assert small == [row for row in records[:128] if row["nullable"] is not None]
    assert all(actual is original for actual, original in zip(automatic, expected, strict=True))
    assert native_calls == 3
    assert execution.value == expected
    assert execution.report.compiler_engine == "python"
    assert execution.report.strategy == "rust_direct"


def test_retained_drop_nulls_native_prefix_preserves_mixed_row_protocol_and_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The first non-exact dictionary resumes one canonical Python suffix."""
    from fpstreams import _native

    lookups: list[str] = []

    class VirtualDict(dict[str, object]):
        def __getitem__(self, field: str) -> object:
            lookups.append(field)
            if field == "nullable":
                return 1
            return super().__getitem__(field)

    prefix = [{"id": index, "nullable": None if index % 5 == 0 else index} for index in range(512)]
    boundary = VirtualDict(id=512, nullable=None)
    missing: dict[str, object] = {"id": 513}
    tail = {"id": 514, "nullable": 514}
    records = [*prefix, boundary, missing, tail]
    endpoint = _native.drop_nulls_exact_dict_prefix_v1
    native_calls = 0

    def tracked(*arguments: object) -> object:
        nonlocal native_calls
        native_calls += 1
        return endpoint(*arguments)

    monkeypatch.setattr(_native, "drop_nulls_exact_dict_prefix_v1", tracked)

    execution = fpstreams.rows(records).drop_nulls("nullable").run_with_report("to_list")
    expected = [row for row in prefix if row["nullable"] is not None] + [boundary, tail]

    assert execution.value == expected
    assert all(
        actual is original for actual, original in zip(execution.value, expected, strict=True)
    )
    assert lookups == ["nullable"]
    assert native_calls == 1
    assert execution.report.strategy == "rust_python_hybrid"


def test_retained_drop_nulls_native_prefix_matches_lookup_exception_boundaries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Suppressed lookup failures, PEP 479, and ordinary errors match the Python sink."""
    from fpstreams import _native

    class Trap:
        def __init__(self, failure_type: type[BaseException]) -> None:
            self.failure_type = failure_type

        def __hash__(self) -> int:
            return hash("nullable")

        def __eq__(self, _other: object) -> bool:
            raise self.failure_type("lookup failed")

    endpoint = _native.drop_nulls_exact_dict_prefix_v1
    native_calls = 0

    def tracked(*arguments: object) -> object:
        nonlocal native_calls
        native_calls += 1
        return endpoint(*arguments)

    monkeypatch.setattr(_native, "drop_nulls_exact_dict_prefix_v1", tracked)

    suppressed = {Trap(TypeError): 1}
    assert fpstreams.rows([suppressed] * 512).drop_nulls("nullable").to_list() == []

    stopped = {Trap(StopIteration): 1}
    with pytest.raises(RuntimeError, match="generator raised StopIteration") as converted:
        fpstreams.rows([stopped] * 512).drop_nulls("nullable").to_list()
    assert isinstance(converted.value.__cause__, StopIteration)

    failed = {Trap(ValueError): 1}
    with pytest.raises(ValueError, match="lookup failed"):
        fpstreams.rows([failed] * 512).drop_nulls("nullable").to_list()

    assert native_calls == 3


def test_retained_drop_nulls_native_prefix_deopts_for_failpoints(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Active failpoints retain the canonical Python drop-null loop."""
    from fpstreams import _native
    from fpstreams.runtime.failpoints import failpoint

    records = [{"nullable": index} for index in range(600)]
    query = fpstreams.rows(records).drop_nulls("nullable")

    def forbidden(*_arguments: object) -> object:
        raise AssertionError("instrumented drop_nulls must stay on Python")

    monkeypatch.setattr(_native, "drop_nulls_exact_dict_prefix_v1", forbidden)
    with failpoint("unrelated.drop_nulls.transition", RuntimeError("unused")):
        assert query.to_list() == records


def test_drop_nulls_materialized_sink_ignores_a_replaced_iter_builtin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The canonical for-loop does not consult a module-level iter replacement."""
    rows_module = sys.modules["fpstreams.tabular.rows"]
    records = [{"nullable": 1}]
    query = fpstreams.rows(records).drop_nulls("nullable")

    monkeypatch.setattr(rows_module, "iter", lambda _source: iter(()), raising=False)

    assert query.to_list() == records


def test_drop_nulls_cleanup_stop_iteration_keeps_generator_conversion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cleanup StopIteration remains the canonical PEP 479 RuntimeError."""
    from fpstreams.planning.source import Source

    query = fpstreams.rows([{"nullable": 1}]).drop_nulls("nullable")

    class ClosingIterator(Iterator[dict[str, int]]):
        def __init__(self) -> None:
            self._source = iter(({"nullable": 1},))

        def __next__(self) -> dict[str, int]:
            return next(self._source)

        def close(self) -> None:
            raise StopIteration("close stopped")

    monkeypatch.setattr(Source, "open", lambda _source: ClosingIterator())

    with pytest.raises(RuntimeError, match="generator raised StopIteration") as caught:
        query.to_list()
    assert isinstance(caught.value.__cause__, StopIteration)


def test_drop_nulls_uses_one_sealed_lazy_filter_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Canonical execution recognizes the project plan without one call per exact row."""
    rows_module = sys.modules["fpstreams.tabular.rows"]
    calls = 0
    original = rows_module._DropNullsPlan.__call__

    def tracked(plan: object, row: object) -> bool:
        nonlocal calls
        calls += 1
        return original(plan, row)

    monkeypatch.setattr(rows_module._DropNullsPlan, "__call__", tracked)
    records = [
        {"left": 1, "right": 2},
        {"left": None, "right": 3},
        {"left": 4, "right": None},
    ]

    assert fpstreams.rows(records).drop_nulls("left", "right").select("left").to_list() == [
        {"left": 1}
    ]
    assert calls == 0


def test_select_then_drop_nulls_keeps_the_sealed_filter_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A preceding row map must not turn the sealed null plan back into a callback."""
    rows_module = sys.modules["fpstreams.tabular.rows"]

    def unexpected_callback(_plan: object, _row: object) -> bool:
        raise AssertionError("sealed drop_nulls plan ran through its adapter")

    monkeypatch.setattr(rows_module._DropNullsPlan, "__call__", unexpected_callback)
    query = fpstreams.rows([{"value": 1}, {"value": None}]).select("value").drop_nulls("value")

    assert query.to_list() == [{"value": 1}]


def test_drop_nulls_whole_record_uses_one_sealed_lazy_filter_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Whole-record null policies avoid one callback and two generators per exact row."""
    rows_module = sys.modules["fpstreams.tabular.rows"]
    records = [{"left": 1, "right": 2}, {"left": None, "right": 3}]
    query = fpstreams.rows(records).drop_nulls()
    operation = query.to_flow()._pipeline.operations[0]

    assert type(operation.predicate) is rows_module._DropNullsPlan

    calls = 0
    original = rows_module._DropNullsPlan.__call__

    def tracked(plan: object, row: object) -> bool:
        nonlocal calls
        calls += 1
        return original(plan, row)

    monkeypatch.setattr(rows_module._DropNullsPlan, "__call__", tracked)

    assert query.to_list() == [records[0]]
    assert calls == 0


def test_drop_nulls_whole_record_rechecks_any_after_source_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A source-time replacement keeps ownership of the whole-record null policy."""
    from fpstreams.planning.source import Source, SourceCapabilities

    rows_module = sys.modules["fpstreams.tabular.rows"]
    calls = 0

    def replacement(_values: Iterable[object]) -> bool:
        nonlocal calls
        calls += 1
        return False

    def open_source() -> Iterator[dict[str, object]]:
        monkeypatch.setattr(rows_module, "any", replacement, raising=False)
        return iter(({"nullable": None},))

    source = Source(open_source, SourceCapabilities(reiterable=True, exact_size=1))

    assert fpstreams.Flow(source).rows().drop_nulls().to_list() == [{"nullable": None}]
    assert calls == 1


def test_drop_nulls_whole_record_rechecks_record_conversion_before_iteration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A live record converter remains authoritative over whole-record filtering."""
    rows_module = sys.modules["fpstreams.tabular.rows"]
    calls: list[object] = []
    query = fpstreams.rows([{"kept": 1}]).drop_nulls()

    def replacement(row: object) -> dict[str, object]:
        calls.append(row)
        return {"replaced": None}

    monkeypatch.setattr(rows_module, "_as_record", replacement)

    assert query.to_list() == []
    assert calls == [{"kept": 1}]


def test_drop_nulls_direct_fields_preserve_observable_dictionary_lookup() -> None:
    """A custom key reaches the established lookup path exactly once."""
    events: list[str] = []

    class Trap:
        def __hash__(self) -> int:
            return hash("selected")

        def __eq__(self, other: object) -> bool:
            events.append(f"eq:{other}")
            return other == "selected"

    first = {"selected": 1}
    custom_key = Trap()
    observed = {custom_key: 2}
    last = {"selected": 3}

    result = fpstreams.rows([first, observed, last]).drop_nulls("selected").to_list()

    assert len(result) == 3
    assert result[0] is first
    assert result[1] is observed
    assert result[2] is last
    assert events == ["eq:selected"]


def test_drop_nulls_rechecks_operation_opener_after_source_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A live canonical operation opener remains authoritative after source effects."""
    from contextlib import contextmanager

    from fpstreams.planning.source import Source, SourceCapabilities

    terminal_module = sys.modules["fpstreams.streams.flow_terminals"]
    calls = 0

    @contextmanager
    def replacement(
        source: Iterator[object],
        operations: tuple[object, ...],
        **keywords: object,
    ) -> Iterator[Iterator[object]]:
        nonlocal calls
        calls += 1
        del source, operations, keywords
        yield iter(())

    def open_source() -> Iterator[dict[str, int]]:
        monkeypatch.setattr(terminal_module, "open_operations", replacement)
        return iter(({"a": 1},))

    source = Source(open_source, SourceCapabilities(reiterable=True, exact_size=1))

    assert fpstreams.Flow(source).rows().drop_nulls("a").to_list() == []
    assert calls == 1


@pytest.mark.parametrize("failure_type", [AttributeError, KeyError, TypeError])
def test_drop_nulls_direct_fields_treat_selector_lookup_failures_as_missing(
    failure_type: type[Exception],
) -> None:
    """Exact-dict lookup matches compile_selector when a stored key owns equality."""

    class FieldName(str):
        pass

    def evaluate(
        names: tuple[str, ...], how: str, *, direct: bool
    ) -> tuple[list[dict[object, object]], list[str], dict[object, object]]:
        events: list[str] = []

        class Trap:
            def __hash__(self) -> int:
                return hash("missing")

            def __eq__(self, other: object) -> bool:
                events.append(f"eq:{other}")
                raise failure_type("trap equality")

        row: dict[object, object] = {Trap(): 1, "present": 2}
        selectors = names if direct else tuple(FieldName(name) for name in names)
        result = (
            fpstreams.rows([row])
            .drop_nulls(
                *selectors,
                how=cast(Any, how),
            )
            .to_list()
        )
        return result, events, row

    for names, how, retained, expected_events in (
        (("missing",), "any", False, ["eq:missing"]),
        (("missing", "present"), "any", False, ["eq:missing"]),
        (("missing", "present"), "all", True, ["eq:missing"]),
        (("present", "missing"), "all", True, []),
    ):
        for direct in (True, False):
            result, events, row = evaluate(names, how, direct=direct)
            assert bool(result) is retained
            if retained:
                assert result[0] is row
            assert events == expected_events


@pytest.mark.parametrize("selectors", [(), ("nullable",)])
def test_drop_nulls_fast_path_stays_lazy_and_closes_after_take(
    selectors: tuple[str, ...],
) -> None:
    events: list[tuple[str, int | None]] = []

    def records() -> Iterator[dict[str, int | None]]:
        try:
            for value in (None, 1, 2):
                events.append(("pull", value))
                yield {"nullable": value}
        finally:
            events.append(("close", None))

    query = fpstreams.rows(records()).drop_nulls(*selectors).take(1)

    assert events == []
    assert query.to_list() == [{"nullable": 1}]
    assert events == [("pull", None), ("pull", 1), ("close", None)]


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
def test_unnest_native_wide_row_checks_signals_before_append(wide_part: str) -> None:
    """A pending signal interrupts either dictionary scan while its private row is atomic."""
    from fpstreams import _native

    wide = {f"field_{index}": index for index in range(250_000)}
    row = {"profile": wide} if wide_part == "nested" else {"profile": {}, **wide}
    tail = object()
    source = iter([row, tail])
    output: list[object] = []
    previous_handler = signal.getsignal(signal.SIGALRM)

    class WideRowSignal(Exception):
        pass

    def interrupt(_signum: int, _frame: object) -> None:
        raise WideRowSignal

    signal.signal(signal.SIGALRM, interrupt)
    signal.setitimer(signal.ITIMER_REAL, 0.001 if wide_part == "outer" else 0.005)
    try:
        with pytest.raises(WideRowSignal):
            _native.unnest_exact_dict_prefix_v1(output, source, "profile", "")
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0.0)
        signal.signal(signal.SIGALRM, previous_handler)

    assert output == []
    assert next(source) is tail


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


@pytest.mark.parametrize("how", ["inner", "left", "right", "full", "semi", "anti"])
def test_partitioned_join_matches_stable_in_memory_semantics(how: str, tmp_path) -> None:
    left = [
        {"id": 1, "left": "a"},
        {"id": 1, "left": "b"},
        {"id": 3, "left": "c"},
    ]
    right = [
        {"id": 1, "right": "x"},
        {"id": 1, "right": "y"},
        {"id": 2, "right": "z"},
    ]

    expected = fpstreams.rows(left).join(right, on="id", how=how).to_list()
    actual = (
        fpstreams.rows(left)
        .join(
            right,
            on="id",
            how=how,
            partitions=3,
            tempdir=tmp_path,
        )
        .to_list()
    )

    assert actual == expected
    assert list(tmp_path.iterdir()) == []


def test_partitioned_join_preserves_composite_keys_suffixes_and_sparse_columns() -> None:
    left = [
        {"tenant": "a", "user": 1, "value": "left-a"},
        {"tenant": "b", "user": 1, "value": "left-b", "extra": 9},
    ]
    right = [
        {"tenant": "a", "id": 1, "value": "right-a"},
        {"tenant": "b", "id": 2, "value": "right-b"},
    ]

    regular = (
        fpstreams.rows(left)
        .join(
            right,
            left_on=("tenant", "user"),
            right_on=("tenant", "id"),
            how="full",
            suffix="_lookup",
        )
        .to_list()
    )
    spilled = (
        fpstreams.rows(left)
        .join(
            right,
            left_on=("tenant", "user"),
            right_on=("tenant", "id"),
            how="full",
            suffix="_lookup",
            partitions=4,
        )
        .to_list()
    )

    assert spilled == regular


@pytest.mark.parametrize(
    ("validate", "duplicate_side"),
    [("1:m", "left"), ("m:1", "right")],
)
def test_partitioned_join_validates_cardinality_and_cleans_up(
    validate: str, duplicate_side: str, tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fpstreams.tabular import spill

    unique = [{"id": 1}]
    duplicate = [{"id": 1}, {"id": 1}]
    left = duplicate if duplicate_side == "left" else unique
    right = duplicate if duplicate_side == "right" else unique
    retained_readers: list[Iterator[Any]] = []
    closed_paths: list[Path] = []
    original_read = spill._read

    def tracked_read(path: Path, *, store=None) -> Iterator[Any]:
        """Retain each reader so only explicit executor ownership can close it."""
        upstream = original_read(path, store=store)

        def values() -> Iterator[Any]:
            try:
                yield from upstream
            finally:
                upstream.close()
                closed_paths.append(path)

        reader = values()
        retained_readers.append(reader)
        return reader

    monkeypatch.setattr(spill, "_read", tracked_read)

    with pytest.raises(ValueError, match=rf"unique {duplicate_side} keys"):
        fpstreams.rows(left).join(
            right,
            on="id",
            validate=validate,
            partitions=3,
            tempdir=tmp_path,
        ).to_list()

    assert retained_readers
    assert len(closed_paths) == len(retained_readers)
    assert list(tmp_path.iterdir()) == []


def test_spilled_group_by_preserves_first_group_and_row_order(tmp_path) -> None:
    records = [
        {"team": "b", "band": 1, "score": 4},
        {"team": "a", "band": 1, "score": 3},
        {"team": "b", "band": 1, "score": 2},
        {"team": "a", "band": 2, "score": 8},
        {"team": "a", "band": 1, "score": 1},
    ]

    expected = (
        fpstreams.rows(records)
        .group_by("team", "band")
        .aggregate(
            count=fpstreams.agg.count(),
            total=fpstreams.agg.sum("score"),
            scores=fpstreams.agg.collect("score", into=tuple),
        )
        .to_list()
    )
    actual = (
        fpstreams.rows(records)
        .group_by("team", "band")
        .spill(3, tempdir=tmp_path)
        .aggregate(
            count=fpstreams.agg.count(),
            total=fpstreams.agg.sum("score"),
            scores=fpstreams.agg.collect("score", into=tuple),
        )
        .to_list()
    )

    assert actual == expected
    assert list(tmp_path.iterdir()) == []


def test_spill_closes_sources_and_temp_files_after_short_circuit(tmp_path) -> None:
    left_closed = right_closed = False

    def left() -> Iterator[dict[str, int]]:
        nonlocal left_closed
        try:
            for value in range(20):
                yield {"id": value}
        finally:
            left_closed = True

    def right() -> Iterator[dict[str, int]]:
        nonlocal right_closed
        try:
            for value in range(20):
                yield {"id": value}
        finally:
            right_closed = True

    iterator = iter(fpstreams.rows(left()).join(right(), on="id", partitions=3, tempdir=tmp_path))
    assert next(iterator) == {"id": 0}
    assert left_closed and right_closed
    assert list(tmp_path.iterdir())
    iterator.close()
    assert list(tmp_path.iterdir()) == []


def test_spill_errors_are_clear_and_cleanup_is_transactional(tmp_path) -> None:
    with pytest.raises(TypeError, match="hashable"):
        (
            fpstreams.rows([{"key": [1], "value": 2}])
            .group_by("key")
            .spill(2, tempdir=tmp_path)
            .aggregate(total=fpstreams.agg.sum("value"))
            .to_list()
        )
    assert list(tmp_path.iterdir()) == []

    assert fpstreams.rows([{"id": 1}]).join(
        [{"id": 1, "payload": lambda: None}],
        on="id",
        how="semi",
        partitions=2,
        tempdir=tmp_path,
    ).to_list() == [{"id": 1}]
    assert list(tmp_path.iterdir()) == []

    with pytest.raises(TypeError, match="picklable"):
        fpstreams.rows([{"id": 1, "value": lambda: None}]).join(
            [{"id": 1}],
            on="id",
            partitions=2,
            tempdir=tmp_path,
        ).to_list()
    assert list(tmp_path.iterdir()) == []


def test_spill_partition_configuration_is_validated() -> None:
    grouped = fpstreams.rows([{"id": 1}]).group_by("id")
    with pytest.raises(ValueError, match="between"):
        grouped.spill(1)
    with pytest.raises(TypeError, match="integer"):
        grouped.spill(2.5)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="tempdir requires"):
        fpstreams.rows([{"id": 1}]).join([{"id": 1}], on="id", tempdir="unused")


def test_spill_limits_are_validated_before_sources_are_consumed() -> None:
    with pytest.raises(ValueError, match="max_partition_rows"):
        fpstreams.SpillLimits(max_partition_rows=0)
    with pytest.raises(TypeError, match="max_output_rows"):
        fpstreams.SpillLimits(max_output_rows=2.5)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="limits requires partitions"):
        fpstreams.rows([{"id": 1}]).join(
            [{"id": 1}],
            on="id",
            limits=fpstreams.SpillLimits(),
        )


def test_spilled_join_rejects_an_oversized_skewed_partition_and_cleans_up(tmp_path) -> None:
    limits = fpstreams.SpillLimits(
        max_partition_rows=2,
        max_partition_bytes=64 * 1024,
        max_matches_per_key=10,
        max_output_rows=10,
        max_repartition_depth=1,
    )

    with pytest.raises(fpstreams.BufferLimitError, match="max_partition_rows"):
        fpstreams.rows([{"id": 1}]).join(
            [{"id": 1, "right": value} for value in range(3)],
            on="id",
            partitions=2,
            tempdir=tmp_path,
            limits=limits,
        ).to_list()

    assert list(tmp_path.iterdir()) == []


def test_spilled_group_rejects_distinct_colliding_keys_without_unbounded_load(tmp_path) -> None:
    limits = fpstreams.SpillLimits(
        max_partition_rows=2,
        max_partition_bytes=64 * 1024,
        max_matches_per_key=10,
        max_output_rows=10,
        max_repartition_depth=1,
    )
    records = [{"key": _CollidingKey(value)} for value in range(3)]

    with pytest.raises(fpstreams.BufferLimitError, match="max_partition_rows"):
        (
            fpstreams.rows(records)
            .group_by("key")
            .spill(2, tempdir=tmp_path, limits=limits)
            .aggregate(count=fpstreams.agg.count())
            .to_list()
        )

    assert list(tmp_path.iterdir()) == []


def test_spill_write_failure_closes_source_and_removes_temporary_files(
    tmp_path, monkeypatch
) -> None:
    from fpstreams.tabular import spill_io

    closed = False

    def records() -> Iterator[dict[str, int]]:
        nonlocal closed
        try:
            yield {"key": 1}
        finally:
            closed = True

    def fail_write(*_args, **_kwargs) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(spill_io.SpillWriter, "write_encoded", fail_write)
    with pytest.raises(OSError, match="disk full"):
        (
            fpstreams.rows(records())
            .group_by("key")
            .spill(2, tempdir=tmp_path)
            .aggregate(count=fpstreams.agg.count())
            .to_list()
        )

    assert closed
    assert list(tmp_path.iterdir()) == []


def test_spilled_group_rejects_an_oversized_serialized_partition(tmp_path) -> None:
    limits = fpstreams.SpillLimits(
        max_partition_rows=10,
        max_partition_bytes=512,
        max_matches_per_key=10,
        max_output_rows=10,
        max_repartition_depth=0,
    )

    with pytest.raises(fpstreams.BufferLimitError, match="max_partition_bytes"):
        (
            fpstreams.rows([{"key": 1, "payload": "x" * 5_000}])
            .group_by("key")
            .spill(2, tempdir=tmp_path, limits=limits)
            .aggregate(count=fpstreams.agg.count())
            .to_list()
        )

    assert list(tmp_path.iterdir()) == []


def test_spilled_join_guards_matches_and_total_output(tmp_path) -> None:
    matches_limited = fpstreams.SpillLimits(
        max_partition_rows=10,
        max_partition_bytes=64 * 1024,
        max_matches_per_key=2,
        max_output_rows=10,
        max_repartition_depth=0,
    )
    with pytest.raises(fpstreams.BufferLimitError, match="max_matches_per_key"):
        fpstreams.rows([{"id": 1, "left": "a"}]).join(
            [{"id": 1, "right": value} for value in range(3)],
            on="id",
            partitions=2,
            tempdir=tmp_path,
            limits=matches_limited,
        ).to_list()
    assert list(tmp_path.iterdir()) == []

    output_limited = fpstreams.SpillLimits(
        max_partition_rows=10,
        max_partition_bytes=64 * 1024,
        max_matches_per_key=10,
        max_output_rows=3,
        max_repartition_depth=0,
    )
    left = [{"id": 1, "left": value} for value in range(2)]
    right = [{"id": 1, "right": value} for value in range(2)]
    with pytest.raises(fpstreams.BufferLimitError, match="max_output_rows"):
        fpstreams.rows(left).join(
            right,
            on="id",
            partitions=2,
            tempdir=tmp_path,
            limits=output_limited,
        ).to_list()
    assert list(tmp_path.iterdir()) == []

    exact_limit = fpstreams.SpillLimits(
        max_partition_rows=10,
        max_partition_bytes=64 * 1024,
        max_matches_per_key=10,
        max_output_rows=4,
        max_repartition_depth=0,
    )
    assert (
        len(
            fpstreams.rows(left)
            .join(
                right,
                on="id",
                partitions=2,
                tempdir=tmp_path,
                limits=exact_limit,
            )
            .to_list()
        )
        == 4
    )
    assert list(tmp_path.iterdir()) == []


# --- Tests consolidated from test_data_adapters.py ---

"""Dataframe protocols plus Arrow, Polars, Parquet, and SQL source and sink adapters."""


def test_process_parallel_is_safe_after_arrow_initializes_threads(tmp_path: Path) -> None:
    program = tmp_path / "parallel_after_arrow.py"
    program.write_text(
        """
import pyarrow
from fpstreams import flow

def square(value):
    return value * value

if __name__ == "__main__":
    print(flow(range(4)).parallel(workers=2).map(square).to_list())
""".lstrip(),
        encoding="utf-8",
    )
    completed = subprocess.run(
        [sys.executable, "-W", "always", program],
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == "[0, 1, 4, 9]"
    assert "multi-threaded, use of fork()" not in completed.stderr


def test_arrow_batches_preserve_sparse_rows_schema_and_reuse() -> None:
    values = [{"id": 1, "group": "a"}, {"id": 2}, {"id": 3, "group": "b"}]
    source = fpstreams.rows(values)
    batches = source.arrow_batches(batch_size=2).to_list()

    assert [batch.num_rows for batch in batches] == [2, 1]
    assert all(batch.schema.names == ["id", "group"] for batch in batches)
    expected = [{"id": 1, "group": "a"}, {"id": 2, "group": None}, values[2]]
    assert source.to_arrow(batch_size=2).to_pylist() == expected

    restored = fpstreams.rows.from_arrow(pa.Table.from_batches(batches), batch_size=1)
    assert restored.to_list() == expected
    assert restored.to_list() == expected

    schema = pa.schema([("id", pa.int64()), ("group", pa.string())])
    assert fpstreams.rows([{"id": 4}]).to_arrow(schema=schema).to_pylist() == [
        {"id": 4, "group": None}
    ]


def test_safe_arrow_batches_never_box_or_rebuild_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A complete safe Arrow plan should emit native batches without Python records."""
    from fpstreams.execution import arrow as arrow_execution
    from fpstreams.tabular import arrow as arrow_adapter

    def reject_projected_rows(*_args: object) -> Iterator[dict[str, object]]:
        raise AssertionError("native arrow_batches must not box projected rows")
        yield

    def reject_rebuild(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("native arrow_batches must not rebuild Python records")

    monkeypatch.setattr(arrow_execution, "_project_batch_rows", reject_projected_rows)
    monkeypatch.setattr(arrow_adapter, "_batch_from_records", reject_rebuild)
    source = fpstreams.rows.from_arrow(
        pa.table(
            {
                "value": [1, 3, 5, 2, 7],
                "payload": [10, 30, 50, 20, 70],
                "unused": [100, 300, 500, 200, 700],
            }
        ),
        batch_size=2,
    )

    batches = (
        source.where(fpstreams.col("value") >= 3)
        .select(score="value", payload="payload")
        .arrow_batches(batch_size=2)
        .to_list()
    )

    assert [batch.num_rows for batch in batches] == [2, 1]
    assert pa.Table.from_batches(batches).to_pylist() == [
        {"score": 3, "payload": 30},
        {"score": 5, "payload": 50},
        {"score": 7, "payload": 70},
    ]


def test_native_arrow_batches_close_one_shot_stream_after_short_circuit() -> None:
    """Closing the emitted Flow must release a claimed native batch stream immediately."""
    from fpstreams.planning.arrow_source import ArrowBatchSource
    from fpstreams.planning.source import Source, SourceCapabilities

    events: list[str] = []
    batches = [
        pa.record_batch({"value": pa.array([1], type=pa.int64())}),
        pa.record_batch({"value": pa.array([2], type=pa.int64())}),
    ]
    descriptor = ArrowBatchSource(
        lambda: _TrackedArrowBatches(batches, events),
        "reader",
        1,
        batches[0].schema,
        False,
    )

    def forbidden_rows() -> Iterator[dict[str, int]]:
        raise AssertionError("native arrow_batches must not open the row source")
        yield

    source = Source(
        forbidden_rows,
        SourceCapabilities(reiterable=False, exact_size=None),
        native_data=descriptor,
    )
    query = (
        fpstreams.Rows(fpstreams.Flow(source)).where(fpstreams.col("value") >= 1).select("value")
    )

    result = query.arrow_batches(batch_size=1).take(1).to_list()

    assert result[0].to_pylist() == [{"value": 1}]
    assert events == ["pull:0", "close"]
    with pytest.raises(fpstreams.FlowConsumedError):
        query.arrow_batches(batch_size=1).to_list()
    assert events == ["pull:0", "close"]


def test_arrow_batches_keep_noncanonical_projection_on_row_converter() -> None:
    """A dtype changed by Python inference must stay on the established fallback path."""
    source = fpstreams.rows.from_arrow(
        pa.table({"value": pa.array([1, 2], type=pa.uint8())})
    ).select("value")

    batches = source.arrow_batches().to_list()

    assert len(batches) == 1
    assert batches[0].schema == pa.schema([("value", pa.int64())])
    assert batches[0].to_pylist() == [{"value": 1}, {"value": 2}]


def test_rows_exports_arrow_c_stream_and_honors_requested_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PyArrow should consume a safe Rows plan directly and negotiate matching field types."""
    from fpstreams.execution import arrow as arrow_execution
    from fpstreams.tabular import arrow as arrow_adapter

    def reject_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("Rows C Stream export must not box a safe Arrow plan")

    monkeypatch.setattr(arrow_execution, "batch_to_rows", reject_rows)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", reject_rows)
    query = (
        fpstreams.rows.from_arrow(pa.table({"value": [1, 3], "unused": [10, 30]}))
        .where(fpstreams.col("value") >= 2)
        .select("value")
    )

    assert pa.table(query).to_pylist() == [{"value": 3}]
    requested = pa.schema([("value", pa.float64())])
    converted = pa.table(query, schema=requested)
    assert converted.schema == requested
    assert converted.to_pylist() == [{"value": 3.0}]


def test_rows_arrow_c_stream_closes_and_spends_one_shot_source() -> None:
    """Export claims a one-shot source once and closes its native iterator on completion."""
    from fpstreams.planning.arrow_source import ArrowBatchSource
    from fpstreams.planning.source import Source, SourceCapabilities

    events: list[str] = []
    batches = [
        pa.record_batch({"value": pa.array([1], type=pa.int64())}),
        pa.record_batch({"value": pa.array([2], type=pa.int64())}),
    ]
    descriptor = ArrowBatchSource(
        lambda: _TrackedArrowBatches(batches, events),
        "reader",
        1,
        batches[0].schema,
        False,
    )

    def forbidden_rows() -> Iterator[dict[str, int]]:
        raise AssertionError("Rows C Stream export must not open the row source")
        yield

    source = Source(
        forbidden_rows,
        SourceCapabilities(reiterable=False, exact_size=None),
        native_data=descriptor,
    )
    query = fpstreams.Rows(fpstreams.Flow(source)).select("value")

    reader = pa.RecordBatchReader.from_stream(query)
    assert reader.read_all().to_pylist() == [{"value": 1}, {"value": 2}]
    assert events == ["pull:0", "pull:1", "stop", "close"]
    with pytest.raises(fpstreams.FlowConsumedError):
        pa.RecordBatchReader.from_stream(query)
    assert events == ["pull:0", "pull:1", "stop", "close"]


def test_complete_arrow_filter_projection_to_arrow_never_boxes_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A proven complete batch program should return an Arrow table without Python records."""
    from fpstreams.execution import arrow as arrow_execution
    from fpstreams.tabular import arrow as arrow_adapter

    def reject_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("batch-native to_arrow must not box input or projected rows")

    monkeypatch.setattr(arrow_execution, "batch_to_rows", reject_rows)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", reject_rows)
    source = fpstreams.rows.from_arrow(
        pa.table({"value": [1, 3, 5, 2], "payload": [10, 30, 50, 20]}),
        batch_size=1,
    )
    result = (
        source.where(fpstreams.col("value") >= 3)
        .select(score="value", copied="value", payload="payload")
        .to_arrow(batch_size=2)
    )

    assert result.to_pylist() == [
        {"score": 3, "copied": 3, "payload": 30},
        {"score": 5, "copied": 5, "payload": 50},
    ]


def test_complete_arrow_filter_only_to_arrow_never_boxes_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A proven primitive filter should keep every source column in Arrow form."""
    from fpstreams.execution import arrow as arrow_execution
    from fpstreams.tabular import arrow as arrow_adapter

    def reject_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("batch-native filter-only to_arrow must not box rows")

    monkeypatch.setattr(arrow_execution, "batch_to_rows", reject_rows)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", reject_rows)
    source = fpstreams.rows.from_arrow(
        pa.table(
            {
                "value": [1, 3, 5, 2, 7],
                "payload": [10, 30, 50, 20, 70],
                "group": [100, 300, 500, 200, 700],
            }
        ),
        batch_size=2,
    )

    result = source.where(fpstreams.col("value") >= 3).to_arrow(batch_size=2)

    assert result.to_pylist() == [
        {"value": 3, "payload": 30, "group": 300},
        {"value": 5, "payload": 50, "group": 500},
        {"value": 7, "payload": 70, "group": 700},
    ]
    assert [batch.num_rows for batch in result.to_batches()] == [2, 1]


def test_complete_arrow_multiple_direct_filters_never_box_input_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A proven primitive filter sequence stays columnar for count and Arrow output."""
    from fpstreams.execution import arrow as arrow_execution
    from fpstreams.tabular import arrow as arrow_adapter

    def reject_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("batch-native multiple filters must not box input rows")

    monkeypatch.setattr(arrow_execution, "batch_to_rows", reject_rows)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", reject_rows)
    source = fpstreams.rows.from_arrow(
        pa.table(
            {
                "value": [1, 3, 5, 2, 7],
                "payload": [10, 30, 50, 20, 70],
                "group": [100, 300, 500, 200, 700],
            }
        ),
        batch_size=2,
    )
    query = source.where(fpstreams.col("value") >= 3).where(fpstreams.col("payload") < 60)

    assert query.count() == 2
    result = query.to_arrow(batch_size=2)
    assert result.to_pylist() == [
        {"value": 3, "payload": 30, "group": 300},
        {"value": 5, "payload": 50, "group": 500},
    ]


def test_batch_native_to_arrow_preserves_alias_chunking_and_empty_schema() -> None:
    """Direct aliases retain order while output chunks follow the requested row bound."""
    source = fpstreams.rows.from_arrow(
        pa.table({"value": list(range(7)), "payload": list(range(10, 17))}),
        batch_size=2,
    )
    result = (
        source.where(fpstreams.col("value") >= 0)
        .select(left="value", right="value", payload="payload")
        .to_arrow(batch_size=3)
    )

    assert result.to_pylist() == [
        {"left": value, "right": value, "payload": value + 10} for value in range(7)
    ]
    assert [batch.num_rows for batch in result.to_batches()] == [3, 3, 1]

    empty = source.where(fpstreams.col("value") > 100).select("value").to_arrow(batch_size=3)
    assert empty.num_rows == 0
    assert empty.column_names == []


@pytest.mark.parametrize(
    "values",
    [
        pa.array([1, 2], type=pa.uint8()),
        pa.array([1.25, 2.5], type=pa.float32()),
        pa.array(["a", "b"], type=pa.large_string()),
        pa.array([0, 1], type=pa.timestamp("s")),
        pa.array(["a", "b"]).dictionary_encode(),
    ],
)
def test_batch_native_to_arrow_declines_dtypes_changed_by_python_inference(
    values: pa.Array,
) -> None:
    """Physical dtypes outside the canonical row round trip retain the old output schema."""
    source = fpstreams.rows.from_arrow(pa.table({"value": values})).select("value")

    automatic = source.to_arrow()
    canonical = source.with_engine("python").to_arrow()

    assert automatic.schema == canonical.schema
    assert automatic.to_pylist() == canonical.to_pylist()


def test_batch_native_to_arrow_fallback_uses_one_stateful_dataframe_snapshot() -> None:
    """A later unsafe batch must not reopen a generic dataframe provider."""

    class StatefulFrame:
        calls = 0

        def __dataframe__(self, **_options: object) -> object:
            return self

        def __arrow_c_stream__(self, requested_schema: object = None) -> object:
            self.calls += 1
            table = pa.table({"value": pa.array([1, None], type=pa.int64())})
            return table.__arrow_c_stream__(requested_schema)

    frame = StatefulFrame()
    query = fpstreams.rows.from_dataframe(frame, batch_size=1).where(fpstreams.col("value") >= 1)

    with pytest.raises(TypeError):
        query.to_arrow()
    assert frame.calls == 1


@pytest.mark.parametrize(
    "error_type", [ArithmeticError, NotImplementedError, TypeError, ValueError]
)
def test_batch_native_to_arrow_backend_decline_falls_back_without_reopening(
    monkeypatch: pytest.MonkeyPatch, error_type: type[Exception]
) -> None:
    """Recoverable lowering errors finish from the already-opened batch iterator."""
    from fpstreams.execution import arrow as arrow_execution
    from fpstreams.planning.source import Source

    source = fpstreams.rows.from_arrow(pa.table({"value": [1, 3]}))
    query = source.where(fpstreams.col("value") >= 2).select("value")
    input_source = source._flow._pipeline.source
    open_source = Source.open

    def reject_lowering(_node: object, _batch: object) -> object:
        raise error_type("decline")

    def reject_row_source(candidate: Source[object]) -> Iterator[object]:
        if candidate is input_source:
            raise AssertionError("batch fallback must not reopen its source")
        return open_source(candidate)

    monkeypatch.setattr(arrow_execution, "lower_row_expression", reject_lowering)
    monkeypatch.setattr(Source, "open", reject_row_source)

    assert query.to_arrow().to_pylist() == [{"value": 3}]


@pytest.mark.parametrize("error_type", [MemoryError, RuntimeError])
def test_batch_native_to_arrow_unexpected_errors_propagate_without_row_fallback(
    monkeypatch: pytest.MonkeyPatch, error_type: type[Exception]
) -> None:
    """Resource and unexpected backend failures are never hidden by row materialization."""
    from fpstreams.execution import arrow as arrow_execution

    def fail_lowering(_node: object, _batch: object) -> object:
        raise error_type("table backend")

    def forbidden_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("unexpected failure must not box rows")

    monkeypatch.setattr(arrow_execution, "lower_row_expression", fail_lowering)
    monkeypatch.setattr(arrow_execution, "batch_to_rows", forbidden_rows)
    query = (
        fpstreams.rows.from_arrow(pa.table({"value": [1]}))
        .where(fpstreams.col("value") >= 1)
        .select("value")
    )

    with pytest.raises(error_type, match="table backend"):
        query.to_arrow()


def test_batch_native_to_arrow_failpoints_and_empty_projection_use_canonical_path() -> None:
    """Instrumented execution and uninferrable empty records keep the established path."""
    from fpstreams.runtime.failpoints import failpoint

    query = fpstreams.rows.from_arrow(pa.table({"value": [1]})).where(fpstreams.col("value") >= 1)
    with (
        failpoint("source.open.after", RuntimeError("canonical to_arrow")),
        pytest.raises(RuntimeError, match="canonical to_arrow"),
    ):
        query.to_arrow()

    with pytest.raises(ValueError, match="without columns"):
        fpstreams.rows.from_arrow(pa.table({"value": [1]})).select().to_arrow()


@pytest.mark.parametrize("storage", ["csv", "parquet"])
def test_batch_native_file_to_arrow_uses_projection_without_boxing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, storage: str
) -> None:
    """Typed file scans keep predicate pushdown and projected output in Arrow batches."""
    from fpstreams.execution import arrow as arrow_execution
    from fpstreams.tabular import arrow as arrow_adapter

    target = tmp_path / f"values.{storage}"
    if storage == "csv":
        target.write_text("value,payload,unused\n1,10,a\n3,30,b\n5,50,c\n")
        source = fpstreams.rows.scan_csv(target, batch_size=1)
    else:
        pq.write_table(
            pa.table({"value": [1, 3, 5], "payload": [10, 30, 50], "unused": ["a", "b", "c"]}),
            target,
        )
        source = fpstreams.rows.from_parquet(target, batch_size=1)

    def forbidden_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("safe file to_arrow must not box rows")

    monkeypatch.setattr(arrow_execution, "batch_to_rows", forbidden_rows)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", forbidden_rows)
    result = (
        source.where(fpstreams.col("value") >= 3).select("value", "payload").to_arrow(batch_size=2)
    )

    assert result.to_pylist() == [
        {"value": 3, "payload": 30},
        {"value": 5, "payload": 50},
    ]


def test_batch_native_to_arrow_owns_one_shot_reader_and_preserves_schema_options() -> None:
    """A RecordBatchReader is claimed once, while explicit schemas stay on the old converter."""
    table = pa.table({"value": [1, 3], "payload": [10, 30]})
    reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches(max_chunksize=1))
    query = (
        fpstreams.rows.from_arrow(reader, batch_size=1)
        .where(fpstreams.col("value") >= 2)
        .select("value")
    )

    assert query.to_arrow().to_pylist() == [{"value": 3}]
    with pytest.raises(fpstreams.FlowConsumedError):
        query.to_arrow()

    schema = pa.schema([("value", pa.float64())])
    converted = (
        fpstreams.rows.from_arrow(table)
        .where(fpstreams.col("value") >= 1)
        .select("value")
        .to_arrow(schema=schema)
    )
    assert converted.schema == schema
    assert converted.to_pylist() == [{"value": 1.0}, {"value": 3.0}]


def test_batch_native_to_arrow_matches_canonical_metadata_and_validates_batch_size() -> None:
    """Fast materialization drops source metadata like row rebuilding and validates its bound."""
    schema = pa.schema(
        [pa.field("value", pa.int64(), metadata={b"field": b"source"})],
        metadata={b"schema": b"source"},
    )
    table = pa.Table.from_arrays([pa.array([1, 2])], schema=schema)
    source = fpstreams.rows.from_arrow(table).where(fpstreams.col("value") >= 1).select("value")

    automatic = source.to_arrow()
    canonical = source.with_engine("python").to_arrow()
    assert automatic.schema == canonical.schema
    assert automatic.schema.metadata is None
    assert automatic.schema.field("value").metadata is None

    with pytest.raises(ValueError, match="batch_size"):
        source.to_arrow(batch_size=0)


def test_batch_native_to_arrow_declines_filter_only_integer_overflow() -> None:
    """A filter expression cannot expose Arrow's fixed-width overflow semantics."""
    source = fpstreams.rows.from_arrow(pa.table({"value": [(1 << 63) - 1]}))
    query = source.where(fpstreams.col("value") + 1 > 0)

    assert query.to_arrow().to_pylist() == query.with_engine("python").to_arrow().to_pylist()


def test_batch_native_to_arrow_preserves_first_batch_null_inference_failure() -> None:
    """A leading all-null output chunk cannot borrow the native source's later string type."""
    source = fpstreams.rows.from_arrow(
        pa.table({"value": [1, 2], "payload": pa.array([None, "later"], pa.string())}),
        batch_size=1,
    )
    query = source.where(fpstreams.col("value") >= 1).select("payload")

    with pytest.raises(pa.ArrowInvalid, match="null value"):
        query.to_arrow(batch_size=1)
    with pytest.raises(pa.ArrowInvalid, match="null value"):
        query.with_engine("python").to_arrow(batch_size=1)


def test_batch_native_to_arrow_preserves_all_null_inferred_schema() -> None:
    """An all-null result infers Arrow null instead of retaining its native source type."""
    source = fpstreams.rows.from_arrow(
        pa.table({"value": [1], "payload": pa.array([None], pa.string())})
    )
    query = source.where(fpstreams.col("value") >= 1).select("payload")

    automatic = query.to_arrow(batch_size=1)
    canonical = query.with_engine("python").to_arrow(batch_size=1)

    assert automatic.schema == canonical.schema == pa.schema([("payload", pa.null())])


def test_batch_native_to_arrow_keeps_later_nulls_native(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Once the first terminal chunk anchors a type, later nulls need no row fallback."""
    from fpstreams.execution import arrow as arrow_execution

    source = fpstreams.rows.from_arrow(
        pa.table({"value": [1, 2], "payload": pa.array(["first", None], pa.string())}),
        batch_size=1,
    )

    def forbidden_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("later nulls must remain batch-native")

    monkeypatch.setattr(arrow_execution, "batch_to_rows", forbidden_rows)
    result = source.where(fpstreams.col("value") >= 1).select("payload").to_arrow(batch_size=1)

    assert result.to_pylist() == [{"payload": "first"}, {"payload": None}]


def test_native_identity_to_arrow_rechunks_and_preserves_schema() -> None:
    """Identity materialization honors its requested bound without rebuilding native fields."""
    schema = pa.schema(
        [pa.field("value", pa.int64(), metadata={b"field": b"native"})],
        metadata={b"schema": b"native"},
    )
    table = pa.Table.from_arrays([pa.array([1, 2, 3])], schema=schema)

    result = fpstreams.rows.from_arrow(table, batch_size=3).to_arrow(batch_size=1)

    assert result.schema == schema
    assert [batch.num_rows for batch in result.to_batches()] == [1, 1, 1]


@pytest.mark.parametrize("kind", ["table", "record_batch", "reader"])
def test_empty_native_identity_to_arrow_preserves_schema(kind: str) -> None:
    """Every empty Arrow source kind retains its known schema instead of raising."""
    schema = pa.schema([("value", pa.int64())], metadata={b"source": b"empty"})
    batch = pa.RecordBatch.from_arrays([pa.array([], type=pa.int64())], schema=schema)
    if kind == "table":
        native = pa.Table.from_batches([], schema=schema)
    elif kind == "record_batch":
        native = batch
    else:
        native = pa.RecordBatchReader.from_batches(schema, [])

    result = fpstreams.rows.from_arrow(native).to_arrow()

    assert result.schema == schema
    assert result.num_rows == 0


def test_native_identity_to_arrow_preserves_zero_column_cardinality() -> None:
    """A zero-column Arrow table keeps its rows even though Table cannot encode chunk bounds."""
    schema = pa.schema([], metadata={b"shape": b"zero-column"})
    batch = pa.record_batch([pa.nulls(3)], names=["placeholder"]).select([])
    table = pa.Table.from_batches([batch], schema=schema)

    result = fpstreams.rows.from_arrow(table, batch_size=3).to_arrow(batch_size=1)

    assert result.schema == schema
    assert result.num_columns == 0
    assert result.num_rows == 3


@pytest.mark.parametrize("name", ["source.open.after", "arrow.reader.after"])
def test_native_identity_to_arrow_observes_active_failpoints(name: str) -> None:
    """Instrumentation keeps the canonical open and reader boundaries observable."""
    from fpstreams.runtime.failpoints import failpoint

    query = fpstreams.rows.from_arrow(pa.table({"value": [1]}))
    with (
        failpoint(name, RuntimeError("identity failpoint")),
        pytest.raises(RuntimeError, match="identity failpoint"),
    ):
        query.to_arrow()


def test_native_identity_failpoint_closes_reader_before_first_pull(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A claimed one-shot reader is released even if opening fails before its generator starts."""
    from fpstreams.runtime.failpoints import failpoint
    from fpstreams.tabular import arrow as arrow_adapter

    batch = pa.record_batch({"value": [1]})
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    closed: list[object] = []
    close = arrow_adapter._close

    def observe_close(resource: object) -> None:
        if resource is reader:
            closed.append(resource)
        close(resource)

    monkeypatch.setattr(arrow_adapter, "_close", observe_close)
    query = fpstreams.rows.from_arrow(reader)
    with (
        failpoint("source.open.after", RuntimeError("reader open failed")),
        pytest.raises(RuntimeError, match="reader open failed"),
    ):
        query.to_arrow()

    assert closed == [reader]


def test_native_to_arrow_backend_initialization_fails_before_reader_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Backend setup failure leaves a one-shot reader available for one later evaluation."""
    from fpstreams.execution import arrow as arrow_execution

    batch = pa.record_batch({"value": [1]})
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    query = fpstreams.rows.from_arrow(reader)
    arrow_modules = arrow_execution._arrow_modules

    def fail_modules() -> tuple[object, object]:
        raise MemoryError("backend setup")

    monkeypatch.setattr(arrow_execution, "_arrow_modules", fail_modules)
    with pytest.raises(MemoryError, match="backend setup"):
        query.to_arrow()

    monkeypatch.setattr(arrow_execution, "_arrow_modules", arrow_modules)
    assert query.to_arrow().to_pylist() == [{"value": 1}]


def test_arrow_prefix_suffix_setup_fails_before_reader_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A suffix setup failure leaves a lazy one-shot prefix available for one later run."""
    from fpstreams.execution import arrow as arrow_execution

    batch = pa.record_batch({"value": [1]})
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    query = fpstreams.rows.from_arrow(reader).where(fpstreams.col("value") == 1)
    execute_operations = arrow_execution.execute_operations

    def fail_suffix(*_args: object, **_kwargs: object) -> Iterator[object]:
        raise MemoryError("suffix setup")

    monkeypatch.setattr(arrow_execution, "execute_operations", fail_suffix)
    with pytest.raises(MemoryError, match="suffix setup"):
        query.to_list()

    monkeypatch.setattr(arrow_execution, "execute_operations", execute_operations)
    assert query.to_list() == [{"value": 1}]


def test_arrow_count_scan_setup_fails_before_reader_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Count scan planning can fail without consuming its one-shot Arrow source."""
    from fpstreams.execution import arrow as arrow_execution

    batch = pa.record_batch({"value": [1]})
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    query = fpstreams.rows.from_arrow(reader).where(fpstreams.col("value") == 1)
    count_columns = arrow_execution._count_scan_columns

    def fail_columns(_prefix: object) -> tuple[str, ...] | None:
        raise MemoryError("count scan setup")

    monkeypatch.setattr(arrow_execution, "_count_scan_columns", fail_columns)
    with pytest.raises(MemoryError, match="count scan setup"):
        query.count()

    monkeypatch.setattr(arrow_execution, "_count_scan_columns", count_columns)
    assert query.count() == 1


def test_from_arrow_closes_reader_when_schema_validation_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Construction failure releases a one-shot reader before ownership can be returned."""
    from fpstreams.tabular import arrow as arrow_adapter

    batch = pa.RecordBatch.from_arrays([pa.array([1]), pa.array([2])], names=["id", "id"])
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    closed: list[object] = []
    close = arrow_adapter._close

    def observe_close(resource: object) -> None:
        if resource is reader:
            closed.append(resource)
        close(resource)

    monkeypatch.setattr(arrow_adapter, "_close", observe_close)
    with pytest.raises(fpstreams.DuplicateKeyError, match="duplicate column"):
        fpstreams.rows.from_arrow(reader)

    assert closed == [reader]


def test_relational_rows_to_arrow_materializes_the_relation_result() -> None:
    """Arrow conversion must not try to flatten a join or aggregate into a linear source."""
    source = fpstreams.rows(
        [
            {"id": 1, "team": "a", "score": 2},
            {"id": 2, "team": "a", "score": 3},
        ]
    )
    relations = (
        (
            source.join(fpstreams.rows([{"id": 1, "tag": "x"}]), on="id"),
            [{"id": 1, "team": "a", "score": 2, "tag": "x"}],
        ),
        (
            source.group_by("team").aggregate(total=fpstreams.agg.sum("score")),
            [{"team": "a", "total": 5}],
        ),
        (source.aggregate(total=fpstreams.agg.sum("score")), [{"total": 5}]),
    )

    for relation, expected in relations:
        assert relation.to_arrow(batch_size=1).to_pylist() == expected


@pytest.mark.parametrize("adapter", ["arrow", "record_batch", "dataframe", "polars"])
@pytest.mark.parametrize("kind", ["sum", "min", "max", "first", "last"])
def test_direct_columnar_global_reduction_is_visible_to_the_guarded_planner(
    adapter: str, kind: str
) -> None:
    """A direct reusable int64 scalar reduction retains its guarded marker."""
    from fpstreams.physical.relational import GlobalAggregatePhysicalNode
    from fpstreams.planning.compiler import compile_query

    data = {"value": [2, 3]}
    if adapter == "arrow":
        source = fpstreams.rows.from_arrow(pa.table(data))
    elif adapter == "record_batch":
        source = fpstreams.rows.from_arrow(pa.record_batch(data))
    elif adapter == "dataframe":
        source = fpstreams.rows.from_dataframe(pd.DataFrame(data))
    else:
        source = fpstreams.rows.from_polars(pl.DataFrame(data))
    aggregated = source.aggregate(result=getattr(fpstreams.agg, kind)("value"))

    physical = compile_query(aggregated._flow._query("list"))

    assert isinstance(physical.root, GlobalAggregatePhysicalNode)
    marker = getattr(physical.root, "arrow_i64_sum", None)
    assert marker is not None
    assert marker.value_field == "value"
    assert marker.output_name == "result"
    assert marker.kind == kind
    relation = aggregated._flow.explain("list").to_dict()["relations"]
    assert relation["candidate"] == "arrow_reduce"
    assert relation["guarded"] is True


@pytest.mark.parametrize("adapter", ["arrow", "record_batch", "dataframe", "polars"])
def test_direct_columnar_global_multi_aggregate_never_boxes_rows(
    monkeypatch: pytest.MonkeyPatch, adapter: str
) -> None:
    """Closed global lanes should share one guarded columnar input without row boxing."""
    from fpstreams.physical.relational import GlobalAggregatePhysicalNode, SourcePhysicalNode
    from fpstreams.planning.compiler import compile_query
    from fpstreams.planning.source import Source

    data = {"value": [2, 3, -1], "payload": [20, 30, -10]}
    if adapter == "arrow":
        source = fpstreams.rows.from_arrow(pa.table(data), batch_size=1)
    elif adapter == "record_batch":
        source = fpstreams.rows.from_arrow(pa.record_batch(data), batch_size=1)
    elif adapter == "dataframe":
        source = fpstreams.rows.from_dataframe(pd.DataFrame(data), batch_size=1)
    else:
        source = fpstreams.rows.from_polars(pl.DataFrame(data), batch_size=1)
    aggregated = source.aggregate(
        rows=fpstreams.agg.count(),
        total=fpstreams.agg.sum("value"),
        low=fpstreams.agg.min("value"),
        high=fpstreams.agg.max("value"),
    )
    physical = compile_query(aggregated._flow._query("list"))

    assert isinstance(physical.root, GlobalAggregatePhysicalNode)
    assert isinstance(physical.root.input, SourcePhysicalNode)
    marker = physical.root.arrow_i64_sum
    assert marker is not None
    assert [(lane.output_name, lane.kind, lane.value_field) for lane in marker.lanes] == [
        ("rows", "count", None),
        ("total", "sum", "value"),
        ("low", "min", "value"),
        ("high", "max", "value"),
    ]
    relation = aggregated._flow.explain("list").to_dict()["relations"]
    assert relation["candidate"] == "arrow_multi_reduce"
    assert relation["guarded"] is True
    input_source = physical.root.input.source
    open_source = Source.open

    def reject_row_source(candidate: Source[object]) -> Iterator[object]:
        if candidate is input_source:
            raise AssertionError("columnar global multi aggregate must not open Python rows")
        return open_source(candidate)

    monkeypatch.setattr(Source, "open", reject_row_source)

    assert aggregated.to_list() == [{"rows": 3, "total": 4, "low": -1, "high": 3}]


@pytest.mark.parametrize("adapter", ["table", "reader"])
def test_arrow_global_multi_aggregate_preserves_wide_sums_and_reports_columnar(
    adapter: str,
) -> None:
    """Repeated exact lanes retain Python integers and expose the strategy that ran."""
    maximum = 2**63 - 1
    table = pa.table({"value": [maximum, 1], "other": [-4, 7]})
    source = (
        table
        if adapter == "table"
        else pa.RecordBatchReader.from_batches(table.schema, table.to_batches(max_chunksize=1))
    )
    query = fpstreams.rows.from_arrow(source).aggregate(
        rows=fpstreams.agg.count(),
        total=fpstreams.agg.sum("value"),
        repeated=fpstreams.agg.sum("value"),
        low=fpstreams.agg.min("value"),
        high=fpstreams.agg.max("other"),
    )

    execution = query.run_with_report("to_list")

    assert execution.value == [
        {
            "rows": 2,
            "total": 2**63,
            "repeated": 2**63,
            "low": 1,
            "high": 7,
        }
    ]
    assert execution.report.strategy == "arrow_direct"
    assert "Arrow" in execution.report.reason


def test_eager_dataframe_global_multi_fallback_reuses_one_arrow_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A dtype decline should not convert the same eager frame to Arrow twice."""
    frame = pd.DataFrame({"value": pd.Series([2, 3], dtype="int32")})
    query = fpstreams.rows.from_dataframe(frame).aggregate(
        rows=fpstreams.agg.count(),
        total=fpstreams.agg.sum("value"),
        low=fpstreams.agg.min("value"),
    )
    table = pa.table
    conversions = 0

    def tracked_table(source: object, *args: object, **kwargs: object) -> object:
        nonlocal conversions
        if source is frame:
            conversions += 1
        return table(source, *args, **kwargs)

    monkeypatch.setattr(pa, "table", tracked_table)

    execution = query.run_with_report("to_list")

    assert execution.value == [{"rows": 2, "total": 5, "low": 2}]
    assert conversions == 1
    assert execution.report.strategy == "planned:python"


@pytest.mark.parametrize("adapter", ["table", "reader"])
def test_arrow_global_multi_nulls_preserve_row_major_error_order(adapter: str) -> None:
    """A nullable lane must not overtake an earlier canonical collector failure."""
    table = pa.table(
        {
            "minimum": pa.array([1, None], type=pa.int64()),
            "total": pa.array([None, 2], type=pa.int64()),
        }
    )

    def aggregate(engine: str) -> object:
        source = (
            table
            if adapter == "table"
            else pa.RecordBatchReader.from_batches(table.schema, table.to_batches())
        )
        return (
            fpstreams.rows.from_arrow(source)
            .with_engine(engine)
            .aggregate(
                minimum=fpstreams.agg.min("minimum"),
                total=fpstreams.agg.sum("total"),
            )
            .to_list()
        )

    with pytest.raises(TypeError) as automatic:
        aggregate("auto")
    with pytest.raises(TypeError) as canonical:
        aggregate("python")

    assert str(automatic.value) == str(canonical.value)


@pytest.mark.parametrize("adapter", ["table", "reader"])
def test_arrow_global_multi_preserves_empty_missing_selector_timing(adapter: str) -> None:
    """Missing fields stay harmless on empty input and fail on the first real row."""
    empty = pa.table({"present": pa.array([], type=pa.int64())})
    aggregation = dict(
        rows=fpstreams.agg.count(),
        total=fpstreams.agg.sum("missing"),
        low=fpstreams.agg.min("missing"),
        high=fpstreams.agg.max("missing"),
    )

    empty_source = (
        empty
        if adapter == "table"
        else pa.RecordBatchReader.from_batches(empty.schema, empty.to_batches())
    )
    assert fpstreams.rows.from_arrow(empty_source).aggregate(**aggregation).to_list() == [
        {"rows": 0, "total": 0, "low": None, "high": None}
    ]

    present = pa.table({"present": [1]})
    present_source = (
        present
        if adapter == "table"
        else pa.RecordBatchReader.from_batches(present.schema, present.to_batches())
    )
    with pytest.raises(fpstreams.SelectionError) as error:
        fpstreams.rows.from_arrow(present_source).aggregate(**aggregation).to_list()
    assert str(error.value) == "Could not resolve selector 'missing'; failed at 'missing'"
    assert isinstance(error.value.__cause__, KeyError)


@pytest.mark.parametrize("source_kind", ["list", "tuple", "arrow", "record_batch"])
def test_direct_exact_size_global_count_never_opens_its_source(
    monkeypatch: pytest.MonkeyPatch, source_kind: str
) -> None:
    """A direct project-owned count may use a trusted exact source cardinality."""
    from fpstreams.physical.relational import GlobalAggregatePhysicalNode, SourcePhysicalNode
    from fpstreams.planning.compiler import compile_query
    from fpstreams.planning.source import Source

    records = [{"value": 1}, {"value": 2}, {"value": 3}]
    if source_kind == "list":
        source = fpstreams.rows(records)
    elif source_kind == "tuple":
        source = fpstreams.rows(tuple(records))
    elif source_kind == "arrow":
        source = fpstreams.rows.from_arrow(pa.table({"value": [1, 2, 3]}))
    else:
        source = fpstreams.rows.from_arrow(pa.record_batch({"value": [1, 2, 3]}))
    aggregated = source.aggregate(rows=fpstreams.agg.count())
    physical = compile_query(aggregated._flow._query("list"))
    assert isinstance(physical.root, GlobalAggregatePhysicalNode)
    assert isinstance(physical.root.input, SourcePhysicalNode)
    assert physical.root.exact_count_name == "rows"
    relation = aggregated._flow.explain("list").to_dict()["relations"]
    assert relation["candidate"] == "exact_size"
    assert relation["guarded"] is True
    input_source = physical.root.input.source
    open_source = Source.open

    def reject_source(candidate: Source[object]) -> Iterator[object]:
        if candidate is input_source:
            raise AssertionError("exact global count must not open its source")
        return open_source(candidate)

    monkeypatch.setattr(Source, "open", reject_source)

    assert aggregated.to_list() == [{"rows": 3}]


@pytest.mark.parametrize("source_kind", ["list", "tuple"])
def test_direct_native_record_global_sum_never_opens_python_rows(
    monkeypatch: pytest.MonkeyPatch, source_kind: str
) -> None:
    """A direct exact-record i64 sum should reduce its retained source in native code."""
    from fpstreams.physical.relational import GlobalAggregatePhysicalNode, SourcePhysicalNode
    from fpstreams.planning.compiler import compile_query
    from fpstreams.planning.source import Source

    records = [
        {"gross_total": 2, "label": "a"},
        {"gross_total": 3, "label": "b"},
        {"gross_total": -1, "label": "c"},
    ]
    source = fpstreams.rows(records if source_kind == "list" else tuple(records))
    aggregated = source.aggregate(total=fpstreams.agg.sum("gross_total"))
    physical = compile_query(aggregated._flow._query("list"))

    assert isinstance(physical.root, GlobalAggregatePhysicalNode)
    assert isinstance(physical.root.input, SourcePhysicalNode)
    assert physical.root.native_record_i64_sum is not None
    relation = aggregated._flow.explain("list").to_dict()["relations"]
    assert relation["candidate"] == "native_reduce"
    assert relation["guarded"] is True
    input_source = physical.root.input.source
    open_source = Source.open

    def reject_row_source(candidate: Source[object]) -> Iterator[object]:
        if candidate is input_source:
            raise AssertionError("native global sum must not open Python rows")
        return open_source(candidate)

    monkeypatch.setattr(Source, "open", reject_row_source)

    assert aggregated.to_list() == [{"total": 4}]


@pytest.mark.parametrize("source_kind", ["list", "tuple"])
@pytest.mark.parametrize("row_kind", ["dict", "tuple"])
def test_direct_native_multi_global_aggregate_never_opens_python_rows(
    monkeypatch: pytest.MonkeyPatch,
    source_kind: str,
    row_kind: str,
) -> None:
    """Closed exact-i64 lanes should reduce one retained source in one native scan."""
    from fpstreams.physical.relational import GlobalAggregatePhysicalNode, SourcePhysicalNode
    from fpstreams.planning.compiler import compile_query
    from fpstreams.planning.source import Source

    if row_kind == "dict":
        records: list[object] = [
            {"left": 2, "right": 9},
            {"left": 3, "right": -4},
            {"left": -1, "right": 7},
        ] * 64
        sum_field = bytes((108, 101, 102, 116)).decode()
        max_field = bytes((108, 101, 102, 116)).decode()
        assert sum_field == max_field == "left" and sum_field is not max_field
        total = fpstreams.agg.sum(sum_field)
        low = fpstreams.agg.min("right")
        high = fpstreams.agg.max(max_field)
    else:
        records = [(2, 9), (3, -4), (-1, 7)] * 64
        total = fpstreams.agg.sum(0)
        low = fpstreams.agg.min(1)
        high = fpstreams.agg.max(0)
    retained = records if source_kind == "list" else tuple(records)
    aggregated = fpstreams.rows(retained).aggregate(
        rows=fpstreams.agg.count(),
        total=total,
        low=low,
        high=high,
    )
    physical = compile_query(aggregated._flow._query("list"))

    assert isinstance(physical.root, GlobalAggregatePhysicalNode)
    assert isinstance(physical.root.input, SourcePhysicalNode)
    marker = physical.root.native_multi_i64
    assert marker is not None
    assert marker.row_kind == row_kind
    assert tuple(lane.kind for lane in marker.lanes) == ("count", "sum", "min", "max")
    assert physical.root.native_record_i64_sum is None
    relation = aggregated.explain("list").to_dict()["relations"]
    assert relation["candidate"] == "native_multi_reduce"
    assert relation["guarded"] is True
    input_source = physical.root.input.source
    open_source = Source.open

    def reject_row_source(candidate: Source[object]) -> Iterator[object]:
        if candidate is input_source:
            raise AssertionError("native global aggregate must not open Python rows")
        return open_source(candidate)

    monkeypatch.setattr(Source, "open", reject_row_source)

    execution = aggregated.run_with_report("to_list")

    assert execution.value == [{"rows": 192, "total": 256, "low": -4, "high": 3}]
    assert execution.report.compiler_engine == "python"
    assert execution.report.strategy == "rust_direct"


@pytest.mark.parametrize("size", [127, 128])
def test_native_multi_global_aggregate_uses_a_measured_small_source_threshold(size: int) -> None:
    """Auto planning pays the native fixed cost only after its generic crossover."""
    from fpstreams.physical.relational import GlobalAggregatePhysicalNode
    from fpstreams.planning.compiler import compile_query

    records = [(index, -index) for index in range(size)]
    aggregated = fpstreams.rows(records).aggregate(
        rows=fpstreams.agg.count(),
        total=fpstreams.agg.sum(0),
        low=fpstreams.agg.min(1),
    )
    physical = compile_query(aggregated._flow._query("list"))

    assert isinstance(physical.root, GlobalAggregatePhysicalNode)
    assert (physical.root.native_multi_i64 is not None) is (size == 128)
    assert aggregated.to_list() == [{"rows": size, "total": sum(range(size)), "low": -(size - 1)}]


def test_native_multi_global_aggregate_abi_preserves_closed_lane_semantics() -> None:
    """The optional ABI keeps lane order, wide sums, extrema identity, and empty values."""
    from fpstreams import _native

    first_low = int("-1000")
    equal_low = int("-1000")
    assert first_low is not equal_low
    maximum = 2**63 - 1
    dict_lanes = (
        (0, None, "rows"),
        (1, "amount", "total"),
        (2, "low", "low"),
        (3, "high", "high"),
    )
    dict_result = _native.global_multi_i64_dict_rows_v1(
        [
            {"amount": maximum, "low": first_low, "high": -5},
            {"amount": maximum, "low": equal_low, "high": 8},
        ],
        dict_lanes,
    )
    assert dict_result == {
        "rows": 2,
        "total": maximum * 2,
        "low": first_low,
        "high": 8,
    }
    assert dict_result["low"] is first_low
    assert _native.global_multi_i64_dict_rows_v1([], dict_lanes) == {
        "rows": 0,
        "total": 0,
        "low": None,
        "high": None,
    }

    tuple_lanes = (
        (0, None, "rows"),
        (1, -2, "total"),
        (2, -1, "low"),
        (3, 0, "high"),
    )
    assert _native.global_multi_i64_rows_v1([(2, 9), (3, -4), (-1, 7)], tuple_lanes) == {
        "rows": 3,
        "total": 4,
        "low": -4,
        "high": 3,
    }
    assert _native.global_multi_i64_rows_v1((), tuple_lanes) == {
        "rows": 0,
        "total": 0,
        "low": None,
        "high": None,
    }


def test_native_multi_global_aggregate_abi_declines_without_protocol_dispatch() -> None:
    """Speculative exact-record rejection must not call user equality or integer hooks."""
    from fpstreams import _native

    class Trap:
        calls = 0

        def __hash__(self) -> int:
            return hash("value")

        def __eq__(self, _other: object) -> bool:
            type(self).calls += 1
            raise AssertionError("speculative lookup touched custom equality")

    class Record(dict[str, object]):
        pass

    dict_lanes = ((0, None, "rows"), (1, "value", "total"))
    trap = Trap()
    assert _native.global_multi_i64_dict_rows_v1([{trap: 1}], dict_lanes) is None
    assert Trap.calls == 0
    for source in (
        [{"value": True}],
        [{"value": 2**100}],
        [{"missing": 1}],
        [Record(value=1)],
    ):
        assert _native.global_multi_i64_dict_rows_v1(source, dict_lanes) is None

    class Integer(int):
        pass

    tuple_lanes = ((0, None, "rows"), (1, 0, "total"))
    for source in ([(True,)], [(2**100,)], [(Integer(1),)], [object()]):
        assert _native.global_multi_i64_rows_v1(source, tuple_lanes) is None


def test_native_multi_global_aggregate_decline_reopens_one_canonical_scan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A native type decline leaves the retained input untouched for the Python result."""
    from fpstreams import _native

    records = [{"value": True}, {"value": 2**100}] * 64
    calls: list[tuple[object, object]] = []

    def decline(source: object, lanes: object) -> None:
        calls.append((source, lanes))
        return None

    monkeypatch.setattr(
        _native,
        "global_multi_i64_dict_rows_v1",
        decline,
        raising=False,
    )

    assert fpstreams.rows(records).aggregate(
        rows=fpstreams.agg.count(),
        total=fpstreams.agg.sum("value"),
        low=fpstreams.agg.min("value"),
    ).to_list() == [{"rows": 128, "total": 64 * (1 + 2**100), "low": True}]
    assert len(calls) == 1
    assert calls[0][0] is records


def test_native_multi_global_aggregate_keeps_the_single_sum_abi(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The narrower established single-sum kernel remains the preferred control path."""
    from fpstreams import _native

    records = [{"value": 2}, {"value": 3}]
    calls: list[object] = []
    old_kernel = _native.global_sum_i64_dict_rows_v1

    def tracked_single(source: object, field: str) -> int | None:
        calls.append(source)
        return old_kernel(source, field)

    def unexpected_multi(_source: object, _lanes: object) -> object:
        raise AssertionError("single sum reached the generic global aggregate kernel")

    monkeypatch.setattr(_native, "global_sum_i64_dict_rows_v1", tracked_single)
    monkeypatch.setattr(
        _native,
        "global_multi_i64_dict_rows_v1",
        unexpected_multi,
        raising=False,
    )

    assert fpstreams.rows(records).aggregate(total=fpstreams.agg.sum("value")).to_list() == [
        {"total": 5}
    ]
    assert calls == [records]


def test_native_multi_global_aggregate_revalidates_live_collectors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A compiled shortcut must yield to a collector whose transition code changed."""
    from fpstreams import _native
    from fpstreams.execution.physical import execute_physical
    from fpstreams.planning.compiler import compile_query

    rows = fpstreams.agg.count()
    total = fpstreams.agg.sum("value")
    aggregated = fpstreams.rows([{"value": 2}, {"value": 3}] * 64).aggregate(
        rows=rows,
        total=total,
    )
    physical = compile_query(aggregated._flow._query("list"))

    def step_factory() -> Callable[[int, dict[str, int]], int]:
        def select(row: dict[str, int]) -> int:
            return row["value"]

        def step(current: int, row: dict[str, int]) -> int:
            return current + select(row) * 9

        return step

    def unexpected_kernel(_source: object, _lanes: object) -> object:
        raise AssertionError("a stale global aggregate reached native execution")

    monkeypatch.setattr(total.step, "__code__", step_factory().__code__)
    monkeypatch.setattr(_native, "global_multi_i64_dict_rows_v1", unexpected_kernel)

    assert list(execute_physical(physical)) == [{"rows": 128, "total": 2_880}]


def test_python_global_aggregate_observes_done_mutated_on_first_pull() -> None:
    """The no-early-stop fast path must notice a done hook installed by its source."""
    events: list[str] = []
    total = fpstreams.agg.sum("value")

    def replacement_done(state: int) -> bool:
        events.append(f"done:{state}")
        return state >= 2

    def records() -> Iterator[dict[str, int]]:
        try:
            events.append("mutate:done")
            object.__setattr__(total, "done", replacement_done)
            events.append("pull:2")
            yield {"value": 2}
            events.append("pull:3")
            yield {"value": 3}
        finally:
            events.append("close")

    aggregated = fpstreams.rows(records()).with_engine("python").aggregate(total=total)

    assert aggregated.to_list() == [{"total": 2}]
    assert events == ["mutate:done", "pull:2", "done:2", "close"]


@pytest.mark.parametrize("execution_path", ["direct", "executor", "report"])
@pytest.mark.parametrize(
    ("mutation_boundary", "expected_total", "expected_events"),
    [
        (
            "first_pull",
            50,
            ["mutate:step", "pull:2", "step:2", "pull:3", "step:3", "close"],
        ),
        (
            "between_batches",
            32,
            ["pull:2", "mutate:step", "pull:3", "step:3", "close"],
        ),
        (
            "source_exhaustion",
            50,
            ["pull:2", "pull:3", "mutate:finish", "close", "finish"],
        ),
    ],
)
def test_arrow_reader_global_aggregate_observes_lifecycle_mutations(
    execution_path: str,
    mutation_boundary: str,
    expected_total: int,
    expected_events: list[str],
) -> None:
    """A claimed reader must retain live collector hooks across every lazy pull."""
    events: list[str] = []
    total = fpstreams.agg.sum("value")
    first = pa.record_batch({"value": pa.array([2], type=pa.int64())})
    second = pa.record_batch({"value": pa.array([3], type=pa.int64())})

    def replacement_step(state: int, row: dict[str, int]) -> int:
        value = row["value"]
        events.append(f"step:{value}")
        return state + value * 10

    def replacement_finish(state: int) -> int:
        events.append("finish")
        return state * 10

    def batches() -> Iterator[pa.RecordBatch]:
        try:
            if mutation_boundary == "first_pull":
                events.append("mutate:step")
                object.__setattr__(total, "step", replacement_step)
            events.append("pull:2")
            yield first
            if mutation_boundary == "between_batches":
                events.append("mutate:step")
                object.__setattr__(total, "step", replacement_step)
            events.append("pull:3")
            yield second
            if mutation_boundary == "source_exhaustion":
                events.append("mutate:finish")
                object.__setattr__(total, "finish", replacement_finish)
        finally:
            events.append("close")

    reader = pa.RecordBatchReader.from_batches(first.schema, batches())
    aggregated = fpstreams.rows.from_arrow(reader).aggregate(
        total=total,
        rows=fpstreams.agg.count(),
    )

    if execution_path == "direct":
        result = aggregated.to_list()
    elif execution_path == "executor":
        result = list(aggregated)
    else:
        execution = aggregated.run_with_report("to_list")
        result = execution.value
        assert execution.report.strategy == "planned:python"

    assert result == [{"total": expected_total, "rows": 2}]
    assert events == expected_events
    assert events.count("close") == 1


def test_empty_arrow_reader_global_aggregate_observes_tail_finish() -> None:
    """An empty claimed reader still applies a finisher replaced at source exhaustion."""
    events: list[str] = []
    total = fpstreams.agg.sum("value")
    schema = pa.schema([("value", pa.int64())])

    def replacement_finish(state: int) -> int:
        events.append("finish")
        return state + 7

    def batches() -> Iterator[pa.RecordBatch]:
        try:
            events.append("mutate:finish")
            object.__setattr__(total, "finish", replacement_finish)
            return
            yield  # pragma: no cover - retain the generator's Arrow batch type.
        finally:
            events.append("close")

    reader = pa.RecordBatchReader.from_batches(schema, batches())
    aggregated = fpstreams.rows.from_arrow(reader).aggregate(
        total=total,
        rows=fpstreams.agg.count(),
    )

    assert aggregated.to_list() == [{"total": 7, "rows": 0}]
    assert events == ["mutate:finish", "close", "finish"]


def test_dataframe_global_aggregate_observes_lifecycle_mutated_during_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A delayed dataframe conversion may replace collector hooks before its first row."""
    events: list[str] = []
    total = fpstreams.agg.sum("value")
    original_table = pa.table

    def replacement_step(state: int, row: dict[str, int]) -> int:
        value = row["value"]
        events.append(f"step:{value}")
        return state + value * 10

    def observed_table(*args: object, **kwargs: object) -> pa.Table:
        events.append("open:mutate-step")
        object.__setattr__(total, "step", replacement_step)
        return original_table(*args, **kwargs)

    aggregated = fpstreams.rows.from_dataframe(pd.DataFrame({"value": [2, 3]})).aggregate(
        total=total,
        rows=fpstreams.agg.count(),
    )
    monkeypatch.setattr(pa, "table", observed_table)

    assert aggregated.to_list() == [{"total": 50, "rows": 2}]
    assert events == ["open:mutate-step", "step:2", "step:3"]


def test_parquet_global_count_observes_lifecycle_mutated_during_metadata_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Metadata counting must yield when opening the same source changes its collector."""
    target = tmp_path / "global-count-lifecycle.parquet"
    pq.write_table(pa.table({"value": [2, 3]}), target)
    events: list[str] = []
    rows = fpstreams.agg.count()
    original_dataset = ds.dataset

    def replacement_step(state: int, _row: object) -> int:
        events.append("step")
        return state + 10

    def observed_dataset(*args: object, **kwargs: object) -> ds.Dataset:
        events.append("open:mutate-step")
        object.__setattr__(rows, "step", replacement_step)
        return original_dataset(*args, **kwargs)

    aggregated = fpstreams.rows.from_parquet(target).aggregate(rows=rows)
    monkeypatch.setattr(ds, "dataset", observed_dataset)

    assert aggregated.to_list() == [{"rows": 20}]
    assert events == ["open:mutate-step", "step", "step"]


@pytest.mark.parametrize("replacement", ["callback", "other_source"])
def test_parquet_global_count_declines_unbound_metadata_openers(
    tmp_path: Path,
    replacement: str,
) -> None:
    """A metadata counter must remain bound to the same canonical local-path source."""
    from dataclasses import replace

    from fpstreams.planning.arrow_source import ArrowBatchSource

    target = tmp_path / "global-count-bound.parquet"
    other_target = tmp_path / "global-count-other.parquet"
    pq.write_table(pa.table({"value": [2, 3]}), target)
    pq.write_table(pa.table({"value": [7, 8, 9]}), other_target)
    source = fpstreams.rows.from_parquet(target)
    owner = source._flow._pipeline.source
    descriptor = owner.native_data
    assert isinstance(descriptor, ArrowBatchSource)
    callback_calls: list[None] = []

    if replacement == "callback":

        def unbound_count() -> int:
            callback_calls.append(None)
            return 99

        owner.native_data = replace(descriptor, count_opener=unbound_count)
    else:
        other_descriptor = fpstreams.rows.from_parquet(
            other_target
        )._flow._pipeline.source.native_data
        assert isinstance(other_descriptor, ArrowBatchSource)
        owner.native_data = other_descriptor

    assert source.aggregate(rows=fpstreams.agg.count()).to_list() == [{"rows": 2}]
    assert callback_calls == []


def test_python_global_aggregate_keeps_primary_error_when_close_fails() -> None:
    """Iterator cleanup must annotate rather than replace an active collector failure."""

    class Values(Iterator[dict[str, int]]):
        def __init__(self) -> None:
            self.pulled = False
            self.close_calls = 0

        def __iter__(self) -> Values:
            return self

        def __next__(self) -> dict[str, int]:
            if self.pulled:
                raise StopIteration
            self.pulled = True
            return {"value": 2}

        def close(self) -> None:
            self.close_calls += 1
            raise OSError("secondary close")

    def failing_step(_state: int, _row: object) -> int:
        raise ValueError("primary step")

    values = Values()
    aggregated = (
        fpstreams.rows(values)
        .with_engine("python")
        .aggregate(total=fpstreams.Aggregator(lambda: 0, failing_step))
    )

    with pytest.raises(ValueError, match="primary step") as captured:
        aggregated.to_list()
    assert captured.value.__notes__ == ["cleanup failed with OSError: secondary close"]
    assert values.close_calls == 1


def test_native_multi_global_aggregate_preserves_failpoint_observation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Instrumented execution stays on the canonical source transition path."""
    from fpstreams import _native
    from fpstreams.runtime.failpoints import failpoint

    def unexpected_kernel(_source: object, _lanes: object) -> object:
        raise AssertionError("an instrumented global aggregate reached native execution")

    monkeypatch.setattr(_native, "global_multi_i64_dict_rows_v1", unexpected_kernel)
    aggregated = fpstreams.rows([{"value": 2}, {"value": 3}] * 64).aggregate(
        rows=fpstreams.agg.count(),
        total=fpstreams.agg.sum("value"),
    )

    with (
        failpoint("source.open.after", RuntimeError("canonical global multi aggregate")),
        pytest.raises(RuntimeError, match="canonical global multi aggregate"),
    ):
        aggregated.to_list()


def test_direct_numpy_global_aggregate_never_opens_python_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Direct integer lanes must reduce the retained ndarray without forwarding or boxing."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution import relational
    from fpstreams.streams import flow_terminals

    matrix = np.asarray([[9, 2], [8, -3], [7, 4], [6, 1]] * 16, dtype=np.int64)
    aggregated = fpstreams.rows.from_numpy(matrix, columns=("id", "value")).aggregate(
        rows=fpstreams.agg.count(),
        total=fpstreams.agg.sum("value"),
        low=fpstreams.agg.min("value"),
        high=fpstreams.agg.max("value"),
    )
    relation = aggregated.explain("list").to_dict()["relations"]

    assert relation["candidate"] == "numpy_reduce"
    assert relation["guarded"] is True

    def forbidden_collector(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("NumPy global aggregate must not collect Python rows")

    def forbidden_executor(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("NumPy global aggregate must not enter the forwarding runtime")

    monkeypatch.setattr(relational, "run_collector_program", forbidden_collector)
    monkeypatch.setattr(flow_terminals, "execute_physical", forbidden_executor)

    execution = aggregated.run_with_report("to_list")

    assert execution.value == [{"rows": 64, "total": 64, "low": -3, "high": 4}]
    assert execution.report.strategy == "numpy_direct"


def test_direct_numpy_global_list_preserves_exact_count_priority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A count-only matrix keeps the cheaper trusted-cardinality shortcut."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution import relational

    aggregated = fpstreams.rows.from_numpy(
        np.arange(64, dtype=np.int64).reshape(-1, 1),
        columns=("value",),
    ).aggregate(rows=fpstreams.agg.count())

    def forbidden_numpy(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("exact NumPy count must not enter the reduction kernel")

    monkeypatch.setattr(relational, "_numpy_global_aggregate", forbidden_numpy)

    assert aggregated.to_list() == [{"rows": 64}]


@pytest.mark.parametrize("dtype_name", ["int64", "float64"])
def test_direct_numpy_global_sum_and_mean_preserve_chunked_order(
    monkeypatch: pytest.MonkeyPatch,
    dtype_name: str,
) -> None:
    """Columnar sum/mean lanes must continue one state across bounded chunks."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution import relational
    from fpstreams.physical.relational import GlobalAggregatePhysicalNode
    from fpstreams.planning.compiler import compile_query

    if dtype_name == "int64":
        raw = [2**53, 1, -(2**53), 3, -7, 11] * 11
    else:
        raw = [1e16, 1.0, -1e16, 3.0, -0.0, 5e-324] * 11
    matrix = np.asarray(raw, dtype=dtype_name).reshape(-1, 1)
    source = fpstreams.rows.from_numpy(matrix, columns=("value",))
    automatic = source.aggregate(
        rows=fpstreams.agg.count(),
        total=fpstreams.agg.sum("value"),
        mean=fpstreams.agg.mean("value"),
        duplicate_mean=fpstreams.agg.mean("value"),
    )
    canonical = source.with_engine("python").aggregate(
        rows=fpstreams.agg.count(),
        total=fpstreams.agg.sum("value"),
        mean=fpstreams.agg.mean("value"),
        duplicate_mean=fpstreams.agg.mean("value"),
    )
    plan = compile_query(automatic._flow._query("list"))

    assert isinstance(plan.root, GlobalAggregatePhysicalNode)
    assert plan.root.numpy_global is not None
    expected = canonical.to_list()

    def forbidden_collector(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("NumPy sum/mean must not collect Python rows")

    monkeypatch.setattr(relational, "_NUMPY_GLOBAL_CHUNK_ROWS", 7)
    monkeypatch.setattr(relational, "run_collector_program", forbidden_collector)

    assert automatic.to_list() == expected


def test_compiled_numpy_mean_revalidates_statistics_globals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A compiled mean must deopt when OnlineStatistics would resolve new globals."""
    import builtins

    np = pytest.importorskip("numpy")
    from fpstreams.collecting.aggregation import native_group_aggregation
    from fpstreams.collecting.statistics import OnlineStatistics
    from fpstreams.execution.physical import execute_physical
    from fpstreams.physical.relational import GlobalAggregatePhysicalNode
    from fpstreams.planning.compiler import compile_query

    mean = fpstreams.agg.mean("value")
    source = fpstreams.rows.from_numpy(
        np.arange(64, dtype=np.int64).reshape(-1, 1),
        columns=("value",),
    )
    automatic = source.aggregate(mean=mean)
    canonical = source.with_engine("python").aggregate(mean=mean)
    automatic_plan = compile_query(automatic._flow._query("list"))
    canonical_plan = compile_query(canonical._flow._query("list"))

    assert isinstance(automatic_plan.root, GlobalAggregatePhysicalNode)
    assert automatic_plan.root.numpy_global is not None

    def doubled_float(value: object) -> float:
        return builtins.float(value) * 2.0

    monkeypatch.setitem(OnlineStatistics.accept.__globals__, "float", doubled_float)

    assert native_group_aggregation(mean) is None
    assert (
        list(execute_physical(automatic_plan))
        == list(execute_physical(canonical_plan))
        == [{"mean": 63.0}]
    )


@pytest.mark.parametrize("changed_dtype", ["int32", ">i8"])
def test_compiled_numpy_global_mean_revalidates_live_dtype(
    changed_dtype: str,
) -> None:
    """A retained matrix must still satisfy the planner's exact mean layout at execution."""
    import warnings

    np = pytest.importorskip("numpy")
    from fpstreams.execution.physical import execute_physical
    from fpstreams.planning.compiler import compile_query

    matrix = np.arange(32, dtype=np.int64).reshape(-1, 1)
    source = fpstreams.rows.from_numpy(matrix, columns=("value",))
    automatic = source.aggregate(
        total=fpstreams.agg.sum("value"),
        mean=fpstreams.agg.mean("value"),
    )
    canonical = source.with_engine("python").aggregate(
        total=fpstreams.agg.sum("value"),
        mean=fpstreams.agg.mean("value"),
    )
    automatic_plan = compile_query(automatic._flow._query("list"))
    canonical_plan = compile_query(canonical._flow._query("list"))

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        matrix.dtype = np.dtype(changed_dtype)
        matrix.shape = (-1, 1)

    assert list(execute_physical(automatic_plan)) == list(execute_physical(canonical_plan))


def test_compiled_numpy_integer_extrema_revalidate_float_dtype() -> None:
    """Integer min/max lanes must deopt instead of acquiring NumPy's NaN semantics."""
    import warnings

    np = pytest.importorskip("numpy")
    from fpstreams.execution.physical import execute_physical
    from fpstreams.planning.compiler import compile_query

    matrix = np.arange(32, dtype=np.int64).reshape(-1, 1)
    source = fpstreams.rows.from_numpy(matrix, columns=("value",))
    automatic = source.aggregate(
        low=fpstreams.agg.min("value"),
        high=fpstreams.agg.max("value"),
    )
    canonical = source.with_engine("python").aggregate(
        low=fpstreams.agg.min("value"),
        high=fpstreams.agg.max("value"),
    )
    automatic_plan = compile_query(automatic._flow._query("list"))
    canonical_plan = compile_query(canonical._flow._query("list"))

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        matrix.dtype = np.float64
    matrix[:4, 0] = [1.0, np.nan, -0.0, 2.0]
    matrix[4:, 0] = 1.0

    expected = [{"low": -0.0, "high": 2.0}]
    assert list(execute_physical(canonical_plan)) == expected
    assert list(execute_physical(automatic_plan)) == expected


@pytest.mark.parametrize("mutation", ["kind", "selector"])
def test_group_aggregation_marker_metadata_is_bound_to_its_step(
    mutation: str,
) -> None:
    """Frozen-marker bypasses cannot relabel the operator or its captured selector."""
    np = pytest.importorskip("numpy")
    from fpstreams.collecting.aggregation import native_group_aggregation

    aggregation = fpstreams.agg.mean("value") if mutation == "kind" else fpstreams.agg.sum("value")
    hint = native_group_aggregation(aggregation)
    assert hint is not None
    object.__setattr__(hint, mutation, "max" if mutation == "kind" else "other")

    source = fpstreams.rows.from_numpy(
        np.asarray([[1, 1, 10], [1, 2, 20], [1, 4, 40], [1, 5, 50]] * 8),
        columns=("key", "value", "other"),
    )
    automatic = source.group_by("key").aggregate(result=aggregation)
    canonical = source.with_engine("python").group_by("key").aggregate(result=aggregation)
    expected = [{"key": 1, "result": 3.0 if mutation == "kind" else 96}]

    assert native_group_aggregation(aggregation) is None
    assert canonical.to_list() == expected
    assert automatic.to_list() == expected


@pytest.mark.parametrize("mutation", ["setattr", "slot"])
def test_compiled_numpy_mean_revalidates_statistics_type_layout(
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    """The direct mean lane requires the state type's original write boundaries."""
    np = pytest.importorskip("numpy")
    from fpstreams.collecting.aggregation import native_group_aggregation
    from fpstreams.collecting.statistics import OnlineStatistics
    from fpstreams.execution.physical import execute_physical
    from fpstreams.planning.compiler import compile_query

    mean = fpstreams.agg.mean("value")
    query = fpstreams.rows.from_numpy(
        np.arange(32, dtype=np.int64).reshape(-1, 1),
        columns=("value",),
    ).aggregate(mean=mean)
    plan = compile_query(query._flow._query("list"))

    if mutation == "setattr":

        def observed_setattr(_self: object, _name: str, _value: object) -> None:
            raise RuntimeError("observed statistics setattr")

        monkeypatch.setattr(OnlineStatistics, "__setattr__", observed_setattr)
        error = RuntimeError
    else:
        monkeypatch.setattr(OnlineStatistics, "total", 123.0)
        error = AttributeError

    assert native_group_aggregation(mean) is None
    with pytest.raises(error):
        list(execute_physical(plan))


def test_numpy_global_endpoint_probe_does_not_observe_live_callable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Internal Rust endpoint validation must not expose a new builtin callback boundary."""
    import builtins

    np = pytest.importorskip("numpy")
    from fpstreams import _native
    from fpstreams.execution.physical import execute_physical
    from fpstreams.planning.compiler import compile_query

    query = fpstreams.rows.from_numpy(
        np.arange(32, dtype=np.float64).reshape(-1, 1),
        columns=("value",),
    ).aggregate(total=fpstreams.agg.sum("value"), mean=fpstreams.agg.mean("value"))
    plan = compile_query(query._flow._query("list"))
    canonical_callable = builtins.callable
    endpoints = (_native.update_sum_f64_buffer_v1, _native.update_mean_f64_buffer_v1)

    def observed_callable(value: object) -> bool:
        if any(value is endpoint for endpoint in endpoints):
            raise RuntimeError("endpoint callable observed")
        return canonical_callable(value)

    monkeypatch.setattr(builtins, "callable", observed_callable)

    assert list(execute_physical(plan)) == [{"total": 496.0, "mean": 15.5}]

    def unexpected_endpoint(*_args: object) -> object:
        raise AssertionError("a replacement endpoint must not run")

    monkeypatch.setattr(_native, "update_mean_f64_buffer_v1", unexpected_endpoint)
    assert list(execute_physical(plan)) == [{"total": 496.0, "mean": 15.5}]


def test_numpy_global_aggregate_preserves_live_shape_errors() -> None:
    """Planning must not replace the canonical error for a retained array changed to 0-D."""
    np = pytest.importorskip("numpy")

    matrix = np.asarray([[2]], dtype=np.int64)
    aggregated = fpstreams.rows.from_numpy(matrix, columns=("value",)).aggregate(
        rows=fpstreams.agg.count(),
        total=fpstreams.agg.sum("value"),
    )
    matrix.resize((), refcheck=False)

    with pytest.raises(ValueError, match="changed to 0 dimensions"):
        aggregated.to_list()


@pytest.mark.parametrize("mode", ["global", "grouped"])
@pytest.mark.parametrize("mutation", ["sum", "count"])
def test_compiled_numpy_aggregate_revalidates_live_aggregators(
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    mutation: str,
) -> None:
    """A compiled NumPy choice must decline when a retained collector changes later."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution.physical import execute_physical
    from fpstreams.physical.relational import (
        GlobalAggregatePhysicalNode,
        GroupAggregatePhysicalNode,
    )
    from fpstreams.planning.compiler import compile_query

    rows = fpstreams.agg.count()
    total = fpstreams.agg.sum("value")
    source = fpstreams.rows.from_numpy(
        np.asarray([[index % 2, 1] for index in range(32)], dtype=np.int64),
        columns=("key", "value"),
    )
    if mode == "global":
        automatic = source.aggregate(rows=rows, total=total)
        canonical = source.with_engine("python").aggregate(rows=rows, total=total)
    else:
        automatic = source.group_by("key").aggregate(rows=rows, total=total)
        canonical = source.with_engine("python").group_by("key").aggregate(rows=rows, total=total)
    automatic_plan = compile_query(automatic._flow._query("list"))
    canonical_plan = compile_query(canonical._flow._query("list"))

    node_type = GlobalAggregatePhysicalNode if mode == "global" else GroupAggregatePhysicalNode
    assert isinstance(automatic_plan.root, node_type)
    assert isinstance(canonical_plan.root, node_type)
    if mode == "global":
        assert automatic_plan.root.numpy_global is not None
        assert canonical_plan.root.numpy_global is None
    else:
        assert automatic_plan.root.numpy_group is not None
        assert canonical_plan.root.numpy_group is None

    if mutation == "sum":

        def step_factory() -> Callable[[int, dict[str, int]], int]:
            def select(row: dict[str, int]) -> int:
                return row["value"]

            def step(current: int, row: dict[str, int]) -> int:
                return current + select(row) * 9

            return step

        monkeypatch.setattr(total.step, "__code__", step_factory().__code__)
        expected_global = [{"rows": 32, "total": 288}]
        expected_grouped = [
            {"key": 0, "rows": 16, "total": 144},
            {"key": 1, "rows": 16, "total": 144},
        ]
    else:

        def count_step(current: int, _row: object) -> int:
            return current + 9

        monkeypatch.setattr(rows.step, "__code__", count_step.__code__)
        expected_global = [{"rows": 288, "total": 32}]
        expected_grouped = [
            {"key": 0, "rows": 144, "total": 16},
            {"key": 1, "rows": 144, "total": 16},
        ]

    expected = expected_global if mode == "global" else expected_grouped
    assert list(execute_physical(canonical_plan)) == expected
    assert list(execute_physical(automatic_plan)) == expected


def test_compiled_numpy_count_revalidates_before_exact_size_shortcut(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stale count lifecycle must decline the earlier exact-cardinality shortcut."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution.physical import execute_physical
    from fpstreams.physical.relational import GlobalAggregatePhysicalNode
    from fpstreams.planning.compiler import compile_query

    rows = fpstreams.agg.count()
    source = fpstreams.rows.from_numpy(
        np.ones((32, 1), dtype=np.int64),
        columns=("value",),
    )
    automatic = source.aggregate(rows=rows)
    canonical = source.with_engine("python").aggregate(rows=rows)
    automatic_plan = compile_query(automatic._flow._query("list"))
    canonical_plan = compile_query(canonical._flow._query("list"))

    assert isinstance(automatic_plan.root, GlobalAggregatePhysicalNode)
    assert automatic_plan.root.exact_count_name == "rows"

    def count_step(current: int, _row: object) -> int:
        return current + 9

    monkeypatch.setattr(rows.step, "__code__", count_step.__code__)

    assert list(execute_physical(canonical_plan)) == [{"rows": 288}]
    assert list(execute_physical(automatic_plan)) == [{"rows": 288}]


@pytest.mark.parametrize("kind", ["min", "max"])
def test_compiled_numpy_extreme_revalidates_lifecycle_globals(
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    """A live lifecycle globals change must invalidate a compiled NumPy extreme."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution.physical import execute_physical
    from fpstreams.physical.relational import GlobalAggregatePhysicalNode
    from fpstreams.planning.compiler import compile_query

    extreme = getattr(fpstreams.agg, kind)("value")
    source = fpstreams.rows.from_numpy(
        np.zeros((32, 1), dtype=np.int64),
        columns=("value",),
    )
    automatic = source.aggregate(result=extreme)
    canonical = source.with_engine("python").aggregate(result=extreme)
    automatic_plan = compile_query(automatic._flow._query("list"))
    canonical_plan = compile_query(canonical._flow._query("list"))

    assert isinstance(automatic_plan.root, GlobalAggregatePhysicalNode)
    assert automatic_plan.root.numpy_global is not None

    monkeypatch.setitem(extreme.finish.__globals__, "_MISSING", 0)

    assert list(execute_physical(canonical_plan)) == [{"result": None}]
    assert list(execute_physical(automatic_plan)) == [{"result": None}]


def test_numpy_global_aggregate_deopts_after_source_factory_code_change(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retained NumPy metadata must not outlive the Python opener that describes it."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution.physical import execute_physical
    from fpstreams.physical.relational import GlobalAggregatePhysicalNode
    from fpstreams.planning.compiler import compile_query

    table = fpstreams.rows.from_numpy(
        np.ones((32, 1), dtype=np.int64),
        columns=("value",),
    )
    source = table._flow._pipeline.source
    automatic = table.aggregate(total=fpstreams.agg.sum("value"))
    canonical = table.with_engine("python").aggregate(total=fpstreams.agg.sum("value"))
    automatic_plan = compile_query(automatic._flow._query("list"))
    canonical_plan = compile_query(canonical._flow._query("list"))

    assert isinstance(automatic_plan.root, GlobalAggregatePhysicalNode)
    assert automatic_plan.root.numpy_global is not None

    def replacement_factory() -> Callable[[], Iterator[dict[str, int]]]:
        names = ("value",)
        values = [9] * 32

        def replacement() -> Iterator[dict[str, int]]:
            return iter({names[0]: 9} for _index in range(32 if values is not None else 0))

        return replacement

    replacement = replacement_factory()
    monkeypatch.setattr(source._factory, "__code__", replacement.__code__)

    assert "candidate" not in automatic.explain("list").to_dict()["relations"]
    assert (
        list(execute_physical(automatic_plan))
        == list(execute_physical(canonical_plan))
        == [{"total": 288}]
    )


@pytest.mark.parametrize("mode", ["global", "grouped"])
@pytest.mark.parametrize(
    "mutation",
    ["selector_closure", "selector_code", "selector_inner_closure", "step_code"],
)
def test_numpy_aggregates_deopt_after_mutated_group_sum_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    mutation: str,
) -> None:
    """A mutated factory sum must execute its live Python selector and step."""
    np = pytest.importorskip("numpy")

    total = fpstreams.agg.sum("value")
    closure = dict(
        zip(
            total.step.__code__.co_freevars,
            total.step.__closure__ or (),
            strict=True,
        )
    )
    if mutation == "selector_closure":
        monkeypatch.setattr(
            closure["select"],
            "cell_contents",
            lambda row: row["value"] * 3,
        )
        expected_global = [{"total": 96}]
        expected_grouped = [
            {"key": 0, "total": 48},
            {"key": 1, "total": 48},
        ]
    elif mutation == "selector_code":

        def selector_factory() -> Callable[[dict[str, int]], int]:
            selector = "value"

            def replacement(row: dict[str, int]) -> int:
                return row[selector] * 4

            return replacement

        replacement_selector = selector_factory()
        select = closure["select"].cell_contents
        monkeypatch.setattr(select, "__code__", replacement_selector.__code__)
        expected_global = [{"total": 128}]
        expected_grouped = [
            {"key": 0, "total": 64},
            {"key": 1, "total": 64},
        ]
    elif mutation == "selector_inner_closure":
        select = closure["select"].cell_contents
        select_closure = dict(
            zip(
                select.__code__.co_freevars,
                select.__closure__ or (),
                strict=True,
            )
        )
        monkeypatch.setattr(select_closure["selector"], "cell_contents", "other")
        expected_global = [{"total": 96}]
        expected_grouped = [
            {"key": 0, "total": 48},
            {"key": 1, "total": 48},
        ]
    else:

        def replacement_factory() -> Callable[[int, dict[str, int]], int]:
            def select(row: dict[str, int]) -> int:
                return row["value"]

            def replacement(current: int, row: dict[str, int]) -> int:
                return current + select(row) * 2

            return replacement

        replacement = replacement_factory()
        monkeypatch.setattr(total.step, "__code__", replacement.__code__)
        expected_global = [{"total": 64}]
        expected_grouped = [
            {"key": 0, "total": 32},
            {"key": 1, "total": 32},
        ]

    matrix = np.asarray([[index % 2, 1, 3] for index in range(32)], dtype=np.int64)
    source = fpstreams.rows.from_numpy(matrix, columns=("key", "value", "other"))
    if mode == "global":
        automatic = source.aggregate(total=total).to_list()
        canonical = source.with_engine("python").aggregate(total=total).to_list()
        expected = expected_global
    else:
        automatic = source.group_by("key").aggregate(total=total).to_list()
        canonical = source.with_engine("python").group_by("key").aggregate(total=total).to_list()
        expected = expected_grouped

    assert canonical == expected
    assert automatic == expected


@pytest.mark.parametrize(
    ("lifecycle", "expected_global", "expected_grouped"),
    [
        ("initializer", 42, 26),
        ("step", 64, 32),
        ("finish", 96, 48),
        ("combine", 32, 16),
        ("done", 32, 16),
    ],
)
def test_group_sum_deopts_after_any_lifecycle_code_change(
    monkeypatch: pytest.MonkeyPatch,
    lifecycle: str,
    expected_global: int,
    expected_grouped: int,
) -> None:
    """All five factory lifecycle functions must retain their original Python code."""
    np = pytest.importorskip("numpy")
    from fpstreams.collecting.aggregation import native_group_aggregation

    total = fpstreams.agg.sum("value")

    def initializer() -> int:
        return 10

    def step_factory() -> Callable[[int, dict[str, int]], int]:
        def select(row: dict[str, int]) -> int:
            return row["value"]

        def step(current: int, row: dict[str, int]) -> int:
            return current + select(row) * 2

        return step

    def finish(current: int) -> int:
        return current * 3

    def combine(left: int, right: int) -> int:
        return left + right + 100

    def done(_current: int) -> bool:
        return True

    replacement = {
        "initializer": initializer,
        "step": step_factory(),
        "finish": finish,
        "combine": combine,
        "done": done,
    }[lifecycle]
    target = getattr(total, lifecycle)
    monkeypatch.setattr(target, "__code__", replacement.__code__)

    matrix = np.asarray([[index % 2, 1] for index in range(32)], dtype=np.int64)
    source = fpstreams.rows.from_numpy(matrix, columns=("key", "value"))
    automatic_global = source.aggregate(total=total)
    canonical_global = source.with_engine("python").aggregate(total=total)
    automatic_grouped = source.group_by("key").aggregate(total=total)
    canonical_grouped = source.with_engine("python").group_by("key").aggregate(total=total)

    assert native_group_aggregation(total) is None
    assert "candidate" not in automatic_grouped.explain("list").to_dict()["relations"]
    assert canonical_global.to_list() == [{"total": expected_global}]
    assert automatic_global.to_list() == [{"total": expected_global}]
    expected_groups = [
        {"key": 0, "total": expected_grouped},
        {"key": 1, "total": expected_grouped},
    ]
    assert canonical_grouped.to_list() == expected_groups
    assert automatic_grouped.to_list() == expected_groups


def test_numpy_aggregates_safely_deopt_for_an_empty_selector_cell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An emptied live selector cell must raise canonically after lowering declines."""
    np = pytest.importorskip("numpy")

    total = fpstreams.agg.sum("value")
    closure = dict(
        zip(
            total.step.__code__.co_freevars,
            total.step.__closure__ or (),
            strict=True,
        )
    )
    monkeypatch.delattr(closure["select"], "cell_contents")
    matrix = np.ones((32, 1), dtype=np.int64)
    source = fpstreams.rows.from_numpy(matrix, columns=("value",))

    for engine in ("auto", "python"):
        with pytest.raises(NameError, match="free variable 'select'"):
            source.with_engine(engine).aggregate(total=total).to_list()


@pytest.mark.parametrize(
    ("lifecycle", "expected"),
    [
        ("initializer", 42),
        ("step", 64),
        ("finish", 96),
        ("combine", 32),
        ("done", 32),
    ],
)
def test_project_count_deopts_after_any_lifecycle_code_change(
    monkeypatch: pytest.MonkeyPatch,
    lifecycle: str,
    expected: int,
) -> None:
    """Every project-owned count lifecycle must retain its factory code identity."""
    np = pytest.importorskip("numpy")

    rows = fpstreams.agg.count()

    def initializer() -> int:
        return 10

    def step(current: int, _row: object) -> int:
        return current + 2

    def finish(current: int) -> int:
        return current * 3

    def combine(left: int, right: int) -> int:
        return left + right + 100

    def done(_current: int) -> bool:
        return True

    replacement = {
        "initializer": initializer,
        "step": step,
        "finish": finish,
        "combine": combine,
        "done": done,
    }[lifecycle]
    target = getattr(rows, lifecycle)
    monkeypatch.setattr(target, "__code__", replacement.__code__)

    matrix = np.ones((32, 1), dtype=np.int64)
    source = fpstreams.rows.from_numpy(matrix, columns=("value",))
    automatic = source.aggregate(rows=rows)
    canonical = source.with_engine("python").aggregate(rows=rows)
    relation = automatic.explain("list").to_dict()["relations"]

    assert "candidate" not in relation
    assert canonical.to_list() == [{"rows": expected}]
    assert automatic.to_list() == [{"rows": expected}]


@pytest.mark.parametrize("mode", ["global", "grouped"])
def test_numpy_aggregates_deopt_after_mutated_project_count_code(
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    """A mutated project count step must not retain exact or NumPy lowering."""
    np = pytest.importorskip("numpy")

    rows = fpstreams.agg.count()

    def replacement(current: int, _row: object) -> int:
        return current + 2

    monkeypatch.setattr(rows.step, "__code__", replacement.__code__)
    matrix = np.asarray([[index % 2, 1] for index in range(32)], dtype=np.int64)
    source = fpstreams.rows.from_numpy(matrix, columns=("key", "value"))
    if mode == "global":
        automatic = source.aggregate(rows=rows, total=fpstreams.agg.sum("value")).to_list()
        canonical = (
            source.with_engine("python")
            .aggregate(rows=rows, total=fpstreams.agg.sum("value"))
            .to_list()
        )
        expected = [{"rows": 64, "total": 32}]
    else:
        automatic = (
            source.group_by("key").aggregate(rows=rows, total=fpstreams.agg.sum("value")).to_list()
        )
        canonical = (
            source.with_engine("python")
            .group_by("key")
            .aggregate(rows=rows, total=fpstreams.agg.sum("value"))
            .to_list()
        )
        expected = [
            {"key": 0, "rows": 32, "total": 16},
            {"key": 1, "rows": 32, "total": 16},
        ]

    assert canonical == expected
    assert automatic == expected


def test_native_record_global_sum_decline_reopens_canonical_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A semantics guard rejection must leave the retained source clean for Python fallback."""
    from fpstreams import _native

    calls: list[tuple[object, str]] = []

    def decline(source: object, field: str) -> None:
        calls.append((source, field))
        return None

    records = [{"value": True}, {"value": 2**100}]
    monkeypatch.setattr(_native, "global_sum_i64_dict_rows_v1", decline, raising=False)

    assert fpstreams.rows(records).aggregate(total=fpstreams.agg.sum("value")).to_list() == [
        {"total": 1 + 2**100}
    ]
    assert calls == [(records, "value")]


def test_native_record_global_sum_abi_declines_protocol_sensitive_values() -> None:
    """The optional ABI must reject unsafe shapes before invoking Python protocols."""
    from fpstreams import _native

    class Record(dict[str, object]):
        pass

    class Field(str):
        pass

    class RecordMapping(Mapping[str, object]):
        def __getitem__(self, key: str) -> object:
            return 1

        def __iter__(self) -> Iterator[str]:
            return iter(("value",))

        def __len__(self) -> int:
            return 1

    class Trap:
        calls = 0

        def __hash__(self) -> int:
            return hash("value")

        def __eq__(self, _other: object) -> bool:
            type(self).calls += 1
            raise AssertionError("speculative lookup touched custom equality")

    kernel = _native.global_sum_i64_dict_rows_v1
    maximum = 2**63 - 1
    assert kernel([{"value": 2}, {"value": 3}, {"value": -1}], "value") == 4
    assert kernel(({"value": maximum}, {"value": maximum}), "value") == maximum * 2
    for source in (
        [{"value": True}],
        [{"value": 2**100}],
        [{"value": None}],
        [{"missing": 1}],
        [Record(value=1)],
        [RecordMapping()],
        ({"value": 1} for _ in range(1)),
    ):
        assert kernel(source, "value") is None
    assert kernel([{"value": 1}], Field("value")) is None

    trap = Trap()
    assert kernel([{trap: 1}], "value") is None
    assert Trap.calls == 0


def test_native_record_global_sum_fallback_runs_mapping_protocols_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Speculative rejection must not duplicate Mapping or dict-subclass effects."""
    from fpstreams import _native

    effects: list[tuple[str, str]] = []

    class Record(dict[str, int]):
        def __getitem__(self, field: str) -> int:
            effects.append(("dict", field))
            return super().__getitem__(field)

    class RecordMapping(Mapping[str, int]):
        def __getitem__(self, field: str) -> int:
            effects.append(("mapping", field))
            return 3

        def __iter__(self) -> Iterator[str]:
            return iter(("value",))

        def __len__(self) -> int:
            return 1

    kernel = _native.global_sum_i64_dict_rows_v1
    kernel_calls: list[object] = []

    def tracked_kernel(source: object, field: str) -> int | None:
        kernel_calls.append(source)
        return kernel(source, field)

    monkeypatch.setattr(_native, "global_sum_i64_dict_rows_v1", tracked_kernel)
    dict_records = [Record(value=2)]
    mapping_records = [RecordMapping()]

    assert fpstreams.rows(dict_records).aggregate(total=fpstreams.agg.sum("value")).to_list() == [
        {"total": 2}
    ]
    assert fpstreams.rows(mapping_records).aggregate(
        total=fpstreams.agg.sum("value")
    ).to_list() == [{"total": 3}]
    assert kernel_calls == [dict_records, mapping_records]
    assert effects == [("dict", "value"), ("mapping", "value")]


def test_native_record_global_sum_declines_one_shot_and_instrumented_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One-shot ownership and failpoint observation stay on the canonical row executor."""
    from fpstreams import _native
    from fpstreams.runtime.failpoints import failpoint

    def unexpected_kernel(_source: object, _field: str) -> int | None:
        raise AssertionError("protocol-sensitive source reached the native global sum")

    monkeypatch.setattr(_native, "global_sum_i64_dict_rows_v1", unexpected_kernel)
    one_shot = fpstreams.rows(iter(({"value": 2}, {"value": 3}))).aggregate(
        total=fpstreams.agg.sum("value")
    )
    assert one_shot.to_list() == [{"total": 5}]
    with pytest.raises(fpstreams.FlowConsumedError):
        one_shot.to_list()

    automatic = fpstreams.rows([{"value": 2}, {"value": 3}]).aggregate(
        total=fpstreams.agg.sum("value")
    )
    with (
        failpoint("source.open.after", RuntimeError("canonical record global sum")),
        pytest.raises(RuntimeError, match="canonical record global sum"),
    ):
        automatic.to_list()


def test_direct_parquet_global_count_uses_metadata_terminal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A direct aggregate count should share the source's metadata-aware terminal."""
    from fpstreams.physical.relational import GlobalAggregatePhysicalNode
    from fpstreams.planning.compiler import compile_query
    from fpstreams.tabular import arrow as arrow_adapter

    target = tmp_path / "aggregate-count.parquet"
    pq.write_table(pa.table({"id": list(range(10))}), target, row_group_size=2)
    aggregated = fpstreams.rows.from_parquet(
        target,
        filter=ds.field("id") >= 5,
        batch_size=1,
    ).aggregate(rows=fpstreams.agg.count())
    physical = compile_query(aggregated._flow._query("list"))
    assert isinstance(physical.root, GlobalAggregatePhysicalNode)
    assert getattr(physical.root, "arrow_count_name", None) == "rows"

    def forbidden_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("direct Parquet aggregate count must not box rows")

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", forbidden_rows)

    assert aggregated.to_list() == [{"rows": 5}]


def test_parquet_global_count_declines_transforms_forced_python_and_failpoints(
    tmp_path: Path,
) -> None:
    """Only the direct automatic one-count shape may bypass canonical rows."""
    from fpstreams.physical.relational import GlobalAggregatePhysicalNode
    from fpstreams.planning.compiler import compile_query
    from fpstreams.runtime.failpoints import failpoint

    target = tmp_path / "guarded-aggregate-count.parquet"
    pq.write_table(pa.table({"id": [1, 2]}), target)
    source = fpstreams.rows.from_parquet(target)
    candidates = (
        source.with_engine("python").aggregate(rows=fpstreams.agg.count()),
        source.select("id").aggregate(rows=fpstreams.agg.count()),
        source.aggregate(rows=fpstreams.agg.count(), total=fpstreams.agg.sum("id")),
    )
    for candidate in candidates:
        physical = compile_query(candidate._flow._query("list"))
        assert isinstance(physical.root, GlobalAggregatePhysicalNode)
        assert physical.root.arrow_count_name is None

    automatic = source.aggregate(rows=fpstreams.agg.count())
    with (
        failpoint("source.open.after", RuntimeError("canonical aggregate metadata count")),
        pytest.raises(RuntimeError, match="canonical aggregate metadata count"),
    ):
        automatic.to_list()


def test_exact_size_global_count_declines_transforms_forced_python_and_failpoints() -> None:
    """Only a direct automatic count may bypass canonical source observation."""
    from fpstreams.physical.relational import GlobalAggregatePhysicalNode
    from fpstreams.planning.compiler import compile_query
    from fpstreams.runtime.failpoints import failpoint

    source = fpstreams.rows([1, 2, 3])
    candidates = (
        source.with_engine("python").aggregate(rows=fpstreams.agg.count()),
        source.where(lambda _value: True).aggregate(rows=fpstreams.agg.count()),
        source.aggregate(rows=fpstreams.agg.count(), total=fpstreams.agg.sum()),
    )
    for candidate in candidates:
        physical = compile_query(candidate._flow._query("list"))
        assert isinstance(physical.root, GlobalAggregatePhysicalNode)
        assert physical.root.exact_count_name is None

    automatic = source.aggregate(rows=fpstreams.agg.count())
    with (
        failpoint("source.open.after", RuntimeError("canonical aggregate count")),
        pytest.raises(RuntimeError, match="canonical aggregate count"),
    ):
        automatic.to_list()


@pytest.mark.parametrize("adapter", ["arrow", "record_batch", "dataframe", "polars"])
def test_direct_columnar_global_sum_never_opens_python_rows(
    monkeypatch: pytest.MonkeyPatch, adapter: str
) -> None:
    """A direct global sum must ignore adapter batch slicing and avoid input row boxing."""
    from fpstreams.physical.relational import GlobalAggregatePhysicalNode, SourcePhysicalNode
    from fpstreams.planning.compiler import compile_query
    from fpstreams.planning.source import Source

    data = {"value": [2, 3, -1]}
    if adapter == "arrow":
        source = fpstreams.rows.from_arrow(pa.table(data), batch_size=1)
    elif adapter == "record_batch":
        source = fpstreams.rows.from_arrow(pa.record_batch(data), batch_size=1)
    elif adapter == "dataframe":
        source = fpstreams.rows.from_dataframe(pd.DataFrame(data), batch_size=1)
    else:
        source = fpstreams.rows.from_polars(pl.DataFrame(data), batch_size=1)
    aggregated = source.aggregate(total=fpstreams.agg.sum("value"))
    physical = compile_query(aggregated._flow._query("list"))
    assert isinstance(physical.root, GlobalAggregatePhysicalNode)
    assert isinstance(physical.root.input, SourcePhysicalNode)
    input_source = physical.root.input.source
    open_source = Source.open

    def reject_row_source(candidate: Source[object]) -> Iterator[object]:
        if candidate is input_source:
            raise AssertionError("columnar global sum must not open Python rows")
        return open_source(candidate)

    monkeypatch.setattr(Source, "open", reject_row_source)

    assert aggregated.to_list() == [{"total": 4}]


def test_arrow_reader_global_sum_streams_without_boxing_and_consumes_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A direct reader sum should consume its imported C stream exactly once as columns."""
    from fpstreams.physical.relational import GlobalAggregatePhysicalNode
    from fpstreams.planning.compiler import compile_query
    from fpstreams.tabular import arrow as arrow_adapter

    table = pa.table(
        {
            "value": pa.chunked_array([[2, 3], [-1]]),
            "payload": pa.chunked_array([[20, 30], [-10]]),
        }
    )

    class StreamProvider:
        def __init__(self) -> None:
            self.calls = 0

        def __arrow_c_stream__(self, requested_schema: object = None) -> object:
            self.calls += 1
            return table.__arrow_c_stream__(requested_schema)

    def reject_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("Arrow reader global sum must not box rows")

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", reject_rows)
    provider = StreamProvider()
    aggregated = fpstreams.rows.from_arrow(provider, batch_size=1).aggregate(
        total=fpstreams.agg.sum("value")
    )
    physical = compile_query(aggregated._flow._query("list"))

    assert isinstance(physical.root, GlobalAggregatePhysicalNode)
    assert physical.root.arrow_i64_sum is not None
    assert aggregated.to_list() == [{"total": 4}]
    with pytest.raises(fpstreams.FlowConsumedError):
        aggregated.to_list()
    assert provider.calls == 1


def test_arrow_c_stream_global_multi_aggregate_never_boxes_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A one-shot C stream should reduce closed global lanes directly from its batches."""
    from fpstreams.physical.relational import GlobalAggregatePhysicalNode
    from fpstreams.planning.compiler import compile_query
    from fpstreams.tabular import arrow as arrow_adapter

    table = pa.table(
        {
            "value": pa.chunked_array([[2, 3], [-1, 7]], type=pa.int64()),
            "payload": pa.chunked_array([[20, 30], [-10, 70]], type=pa.int64()),
        }
    )

    class StreamProvider:
        def __init__(self) -> None:
            self.calls = 0

        def __arrow_c_stream__(self, requested_schema: object = None) -> object:
            self.calls += 1
            return table.__arrow_c_stream__(requested_schema)

    def reject_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("Arrow reader global multi aggregate must not box rows")

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", reject_rows)
    provider = StreamProvider()
    aggregated = fpstreams.rows.from_arrow(provider, batch_size=2).aggregate(
        rows=fpstreams.agg.count(),
        total=fpstreams.agg.sum("value"),
        low=fpstreams.agg.min("value"),
        high=fpstreams.agg.max("payload"),
    )
    physical = compile_query(aggregated._flow._query("list"))

    assert isinstance(physical.root, GlobalAggregatePhysicalNode)
    assert physical.root.arrow_i64_sum is not None
    assert aggregated.to_list() == [{"rows": 4, "total": 11, "low": -1, "high": 70}]
    with pytest.raises(fpstreams.FlowConsumedError):
        aggregated.to_list()
    assert provider.calls == 1


def test_arrow_reader_global_multi_owns_batches_and_merges_wide_values() -> None:
    """One-shot global lanes skip empty batches, merge bigints, close, and consume once."""
    from fpstreams.planning.arrow_source import ArrowBatchSource
    from fpstreams.planning.source import Source, SourceCapabilities

    maximum = 2**63 - 1
    empty = pa.record_batch(
        {
            "value": pa.array([], type=pa.int64()),
            "other": pa.array([], type=pa.int64()),
        }
    )
    first = pa.record_batch({"value": [maximum], "other": [4]})
    final = pa.record_batch({"value": [1], "other": [9]})
    events: list[str] = []
    batches = _TrackedArrowBatches((empty, first, empty, final), events)

    def open_batches() -> Iterator[pa.RecordBatch]:
        events.append("open")
        return batches

    descriptor = ArrowBatchSource(
        open_batches,
        "reader",
        65_536,
        first.schema,
        False,
    )

    def forbidden_rows() -> Iterator[dict[str, int]]:
        raise AssertionError("Arrow reader global multi must not open Python rows")
        yield

    source = Source(
        forbidden_rows,
        SourceCapabilities(reiterable=False, exact_size=None),
        native_data=descriptor,
    )
    aggregated = fpstreams.Rows(fpstreams.Flow(source)).aggregate(
        rows=fpstreams.agg.count(),
        total=fpstreams.agg.sum("value"),
        low=fpstreams.agg.min("value"),
        high=fpstreams.agg.max("other"),
    )

    assert aggregated.to_list() == [{"rows": 2, "total": 2**63, "low": 1, "high": 9}]
    assert events == ["open", "pull:0", "pull:1", "pull:2", "pull:3", "stop", "close"]
    with pytest.raises(fpstreams.FlowConsumedError):
        aggregated.to_list()
    assert events == ["open", "pull:0", "pull:1", "pull:2", "pull:3", "stop", "close"]


def test_arrow_reader_global_multi_compute_decline_keeps_claimed_batches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A late kernel decline folds only that batch and continues the claimed reader."""
    from fpstreams.execution import relational as relational_execution
    from fpstreams.planning.arrow_source import ArrowBatchSource
    from fpstreams.planning.source import Source, SourceCapabilities
    from fpstreams.tabular import arrow as arrow_adapter

    imported = relational_execution.import_module
    actual_compute = imported("pyarrow.compute")
    record_batches = (
        pa.record_batch({"value": [2, 3]}),
        pa.record_batch({"value": [5, -1]}),
        pa.record_batch({"value": [7]}),
    )
    events: list[str] = []
    batches = _TrackedArrowBatches(record_batches, events)
    min_max_calls = 0

    class RejectingCompute:
        @staticmethod
        def min_max(values: object) -> object:
            nonlocal min_max_calls
            min_max_calls += 1
            if min_max_calls == 2:
                raise NotImplementedError("incremental global decline")
            return actual_compute.min_max(values)

        def __getattr__(self, name: str) -> object:
            return getattr(actual_compute, name)

    def import_backend(name: str) -> object:
        return RejectingCompute() if name == "pyarrow.compute" else imported(name)

    def open_batches() -> Iterator[pa.RecordBatch]:
        events.append("open")
        return batches

    descriptor = ArrowBatchSource(
        open_batches,
        "reader",
        65_536,
        record_batches[0].schema,
        False,
    )

    def forbidden_rows() -> Iterator[dict[str, int]]:
        raise AssertionError("claimed Arrow reader must not reopen Python rows")
        yield

    def reject_boxing(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("claimed Arrow reader fallback must not box whole rows")

    source = Source(
        forbidden_rows,
        SourceCapabilities(reiterable=False, exact_size=None),
        native_data=descriptor,
    )
    aggregated = fpstreams.Rows(fpstreams.Flow(source)).aggregate(
        rows=fpstreams.agg.count(),
        total=fpstreams.agg.sum("value"),
        low=fpstreams.agg.min("value"),
        high=fpstreams.agg.max("value"),
    )
    monkeypatch.setattr(relational_execution, "import_module", import_backend)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", reject_boxing)

    assert aggregated.to_list() == [{"rows": 5, "total": 16, "low": -1, "high": 7}]
    assert min_max_calls == 3
    assert events == ["open", "pull:0", "pull:1", "pull:2", "stop", "close"]
    with pytest.raises(fpstreams.FlowConsumedError):
        aggregated.to_list()
    assert min_max_calls == 3
    assert events == ["open", "pull:0", "pull:1", "pull:2", "stop", "close"]


@pytest.mark.parametrize("adapter", ["table", "reader"])
@pytest.mark.parametrize(
    ("kind", "expected"),
    [("min", -2), ("max", 9), ("last", 3)],
)
def test_direct_arrow_extrema_and_last_never_box_rows(
    monkeypatch: pytest.MonkeyPatch,
    adapter: str,
    kind: str,
    expected: int,
) -> None:
    """Direct int64 extrema and last-value reductions should stay columnar."""
    from fpstreams.tabular import arrow as arrow_adapter

    batches = [
        pa.record_batch({"value": pa.array([4, -2], type=pa.int64())}),
        pa.record_batch({"value": pa.array([9, 3], type=pa.int64())}),
    ]
    source = (
        fpstreams.rows.from_arrow(pa.Table.from_batches(batches))
        if adapter == "table"
        else fpstreams.rows.from_arrow(
            pa.RecordBatchReader.from_batches(batches[0].schema, batches)
        )
    )

    def reject_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError(f"Arrow {kind} must not box rows")

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", reject_rows)
    aggregation = getattr(fpstreams.agg, kind)("value")

    assert source.aggregate(result=aggregation).to_list() == [{"result": expected}]


def test_arrow_reader_global_first_reads_one_batch_and_closes_without_boxing() -> None:
    """A selected first reduction should close immediately after the first nonempty batch."""
    from fpstreams.planning.arrow_source import ArrowBatchSource
    from fpstreams.planning.source import Source, SourceCapabilities

    events: list[str] = []
    empty = pa.record_batch({"value": pa.array([], type=pa.int64())})
    first = pa.record_batch({"value": pa.array([None, 7], type=pa.int64())})
    unpulled = pa.record_batch({"value": pa.array([9], type=pa.int64())})
    descriptor = ArrowBatchSource(
        lambda: _TrackedArrowBatches([empty, first, unpulled], events),
        "reader",
        65_536,
        first.schema,
        False,
    )

    def forbidden_rows() -> Iterator[dict[str, int | None]]:
        raise AssertionError("Arrow first must not open rows")
        yield

    source = Source(
        forbidden_rows,
        SourceCapabilities(reiterable=False, exact_size=None),
        native_data=descriptor,
    )
    aggregated = fpstreams.Rows(fpstreams.Flow(source)).aggregate(
        result=fpstreams.agg.first("value")
    )

    assert aggregated.to_list() == [{"result": None}]
    assert events == ["pull:0", "pull:1", "close"]
    with pytest.raises(fpstreams.FlowConsumedError):
        aggregated.to_list()
    assert events == ["pull:0", "pull:1", "close"]


def test_arrow_reader_global_last_scans_to_stop_and_retains_a_final_null() -> None:
    """Selected last consumes every batch and treats a final null as a real value."""
    from fpstreams.planning.arrow_source import ArrowBatchSource
    from fpstreams.planning.source import Source, SourceCapabilities

    events: list[str] = []
    first = pa.record_batch({"value": pa.array([4], type=pa.int64())})
    empty = pa.record_batch({"value": pa.array([], type=pa.int64())})
    final = pa.record_batch({"value": pa.array([9, None], type=pa.int64())})
    descriptor = ArrowBatchSource(
        lambda: _TrackedArrowBatches([first, empty, final], events),
        "reader",
        65_536,
        first.schema,
        False,
    )

    def forbidden_rows() -> Iterator[dict[str, int | None]]:
        raise AssertionError("Arrow last must not open rows")
        yield

    source = Source(
        forbidden_rows,
        SourceCapabilities(reiterable=False, exact_size=None),
        native_data=descriptor,
    )
    aggregated = fpstreams.Rows(fpstreams.Flow(source)).aggregate(
        result=fpstreams.agg.last("value")
    )

    assert aggregated.to_list() == [{"result": None}]
    assert events == ["pull:0", "pull:1", "pull:2", "stop", "close"]
    with pytest.raises(fpstreams.FlowConsumedError):
        aggregated.to_list()


@pytest.mark.parametrize(
    ("kind", "values", "expected", "message"),
    [
        ("min", [None], None, None),
        ("max", [None], None, None),
        (
            "min",
            [None, 1],
            None,
            "'<' not supported between instances of 'int' and 'NoneType'",
        ),
        (
            "max",
            [1, None],
            None,
            "'>' not supported between instances of 'NoneType' and 'int'",
        ),
        (
            "min",
            [1, None],
            None,
            "'<' not supported between instances of 'NoneType' and 'int'",
        ),
        (
            "max",
            [None, 1],
            None,
            "'>' not supported between instances of 'int' and 'NoneType'",
        ),
        (
            "min",
            [None, None],
            None,
            "'<' not supported between instances of 'NoneType' and 'NoneType'",
        ),
        (
            "max",
            [None, None],
            None,
            "'>' not supported between instances of 'NoneType' and 'NoneType'",
        ),
    ],
)
@pytest.mark.parametrize("adapter", ["table", "reader"])
def test_arrow_extrema_preserve_null_comparison_order_without_row_boxing(
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    values: list[int | None],
    expected: int | None,
    message: str | None,
    adapter: str,
) -> None:
    """Null extrema should match the canonical ordered Python comparisons column-wise."""
    from fpstreams.tabular import arrow as arrow_adapter

    def reject_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError(f"Arrow {kind} null handling must not box rows")

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", reject_rows)
    batches = [pa.record_batch({"value": pa.array([value], type=pa.int64())}) for value in values]
    source = (
        fpstreams.rows.from_arrow(pa.Table.from_batches(batches))
        if adapter == "table"
        else fpstreams.rows.from_arrow(
            pa.RecordBatchReader.from_batches(batches[0].schema, batches)
        )
    )
    aggregated = source.aggregate(result=getattr(fpstreams.agg, kind)("value"))

    if message is None:
        assert aggregated.to_list() == [{"result": expected}]
    else:
        with pytest.raises(TypeError) as error:
            aggregated.to_list()
        assert str(error.value) == message


@pytest.mark.parametrize(("kind", "expected"), [("min", -2), ("max", 9)])
def test_arrow_reader_short_extrema_avoid_per_batch_compute_dispatch(
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    expected: int,
) -> None:
    """Tiny reader batches fold selected scalars without one kernel call per batch."""
    from fpstreams.execution import relational as relational_execution

    imported = relational_execution.import_module
    batches = [
        pa.record_batch({"value": pa.array([4, -2, 5], type=pa.int64())}),
        pa.record_batch({"value": pa.array([9, 3], type=pa.int64())}),
    ]

    class RejectingCompute:
        @staticmethod
        def min_max(_values: object) -> object:
            raise AssertionError("tiny extrema must not dispatch one Arrow kernel per batch")

    def import_backend(name: str) -> object:
        return RejectingCompute if name == "pyarrow.compute" else imported(name)

    monkeypatch.setattr(relational_execution, "import_module", import_backend)
    reader = pa.RecordBatchReader.from_batches(batches[0].schema, batches)

    assert (
        fpstreams.rows.from_arrow(reader)
        .aggregate(result=getattr(fpstreams.agg, kind)("value"))
        .to_list()
    ) == [{"result": expected}]


@pytest.mark.parametrize(
    ("kind", "identity"),
    [("sum", 0), ("min", None), ("max", None), ("first", None), ("last", None)],
)
def test_arrow_reader_global_reduction_preserves_empty_missing_selector_timing(
    kind: str, identity: int | None
) -> None:
    """A missing field remains harmless for an empty stream and fails on its first row."""
    empty_batch = pa.record_batch({"present": pa.array([], type=pa.int64())})
    empty_reader = pa.RecordBatchReader.from_batches(empty_batch.schema, [empty_batch])

    assert (
        fpstreams.rows.from_arrow(empty_reader)
        .aggregate(result=getattr(fpstreams.agg, kind)("missing"))
        .to_list()
    ) == [{"result": identity}]

    present_batch = pa.record_batch({"present": pa.array([1], type=pa.int64())})
    present_reader = pa.RecordBatchReader.from_batches(present_batch.schema, [present_batch])
    with pytest.raises(fpstreams.SelectionError) as error:
        (
            fpstreams.rows.from_arrow(present_reader)
            .aggregate(result=getattr(fpstreams.agg, kind)("missing"))
            .to_list()
        )
    assert str(error.value) == "Could not resolve selector 'missing'; failed at 'missing'"
    assert isinstance(error.value.__cause__, KeyError)


def test_arrow_reader_global_sum_preserves_null_type_error_without_boxing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A later null should raise the same Python sum error after closing the reader."""
    from fpstreams.tabular import arrow as arrow_adapter

    converted: list[int] = []
    closed: list[object] = []
    close = arrow_adapter._close

    def reject_rows(batch: object) -> list[dict[str, object]]:
        converted.append(batch.num_rows)  # type: ignore[attr-defined]
        raise AssertionError("Arrow reader null handling must not box rows")

    def observe_close(resource: object) -> None:
        if isinstance(resource, pa.RecordBatchReader):
            closed.append(resource)
        close(resource)

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", reject_rows)
    monkeypatch.setattr(arrow_adapter, "_close", observe_close)
    first = pa.record_batch({"value": pa.array([2, 3], type=pa.int64())})
    second = pa.record_batch({"value": pa.array([None], type=pa.int64())})
    reader = pa.RecordBatchReader.from_batches(first.schema, [first, second])
    aggregated = fpstreams.rows.from_arrow(reader).aggregate(total=fpstreams.agg.sum("value"))

    with pytest.raises(
        TypeError,
        match=r"unsupported operand type\(s\) for \+: 'int' and 'NoneType'",
    ):
        aggregated.to_list()
    with pytest.raises(fpstreams.FlowConsumedError):
        aggregated.to_list()
    assert converted == []
    assert closed


def test_arrow_reader_global_sum_preserves_wide_integer_totals_without_boxing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Per-batch and cross-batch overflow must retain Python's exact integer result."""
    from fpstreams.tabular import arrow as arrow_adapter

    def reject_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("Arrow reader wide sum must not box rows")

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", reject_rows)
    maximum = 2**63 - 1
    minimum = -(2**63)
    positive_batches = [
        pa.record_batch({"value": pa.array([maximum, 1], type=pa.int64())}),
        pa.record_batch({"value": pa.array([maximum], type=pa.int64())}),
    ]
    negative_batch = pa.record_batch({"value": pa.array([minimum, -1], type=pa.int64())})
    positive_reader = pa.RecordBatchReader.from_batches(
        positive_batches[0].schema, positive_batches
    )
    negative_reader = pa.RecordBatchReader.from_batches(negative_batch.schema, [negative_batch])

    assert (
        fpstreams.rows.from_arrow(positive_reader)
        .aggregate(total=fpstreams.agg.sum("value"))
        .to_list()
    ) == [{"total": 2**64 - 1}]
    assert (
        fpstreams.rows.from_arrow(negative_reader)
        .aggregate(total=fpstreams.agg.sum("value"))
        .to_list()
    ) == [{"total": -(2**63) - 1}]


@pytest.mark.parametrize(
    ("kind", "expected"),
    [("sum", 5), ("min", 2), ("max", 3), ("first", 2), ("last", 3)],
)
def test_arrow_reader_global_reduction_declines_before_claim_for_noncanonical_dtype(
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    expected: int,
) -> None:
    """An unsupported reader dtype should still get exactly one canonical row evaluation."""
    from fpstreams.tabular import arrow as arrow_adapter

    converted: list[int] = []
    convert_rows = arrow_adapter.batch_to_rows

    def tracked(batch: object) -> list[dict[str, object]]:
        converted.append(batch.num_rows)  # type: ignore[attr-defined]
        return convert_rows(batch)

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", tracked)
    batch = pa.record_batch({"value": pa.array([2, 3], type=pa.int32())})
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    aggregated = fpstreams.rows.from_arrow(reader).aggregate(
        result=getattr(fpstreams.agg, kind)("value")
    )

    assert aggregated.to_list() == [{"result": expected}]
    with pytest.raises(fpstreams.FlowConsumedError):
        aggregated.to_list()
    assert converted == [2]


@pytest.mark.parametrize(
    ("kind", "expected"),
    [("sum", 5), ("min", 2), ("max", 3), ("first", 2), ("last", 3)],
)
def test_arrow_reader_global_reduction_failpoint_keeps_the_canonical_row_path(
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    expected: int,
) -> None:
    """Instrumentation must continue observing row conversion and source transitions."""
    from fpstreams.runtime.failpoints import failpoint
    from fpstreams.tabular import arrow as arrow_adapter

    converted: list[int] = []
    convert_rows = arrow_adapter.batch_to_rows

    def tracked(batch: object) -> list[dict[str, object]]:
        converted.append(batch.num_rows)  # type: ignore[attr-defined]
        return convert_rows(batch)

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", tracked)
    batch = pa.record_batch({"value": pa.array([2, 3], type=pa.int64())})
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    aggregated = fpstreams.rows.from_arrow(reader).aggregate(
        result=getattr(fpstreams.agg, kind)("value")
    )

    with failpoint("unrelated.transition", RuntimeError("unused")):
        assert aggregated.to_list() == [{"result": expected}]
    assert converted == [2]


def test_arrow_reader_global_sum_closes_custom_batches_before_rejecting_second_run() -> None:
    """Successful native consumption closes its iterator and atomically spends the source."""
    from fpstreams.planning.arrow_source import ArrowBatchSource
    from fpstreams.planning.source import Source, SourceCapabilities

    events: list[str] = []
    batch = pa.record_batch({"value": pa.array([2, 3], type=pa.int64())})
    descriptor = ArrowBatchSource(
        lambda: _TrackedArrowBatches([batch], events),
        "reader",
        65_536,
        batch.schema,
        False,
    )

    def forbidden_rows() -> Iterator[dict[str, int]]:
        raise AssertionError("custom Arrow reader sum must not open rows")
        yield

    source = Source(
        forbidden_rows,
        SourceCapabilities(reiterable=False, exact_size=None),
        native_data=descriptor,
    )
    aggregated = fpstreams.Rows(fpstreams.Flow(source)).aggregate(total=fpstreams.agg.sum("value"))

    assert aggregated.to_list() == [{"total": 5}]
    assert events == ["pull:0", "stop", "close"]
    with pytest.raises(fpstreams.FlowConsumedError):
        aggregated.to_list()
    assert events == ["pull:0", "stop", "close"]


@pytest.mark.parametrize("kind", ["sum", "min", "max"])
def test_arrow_reader_global_reduction_closes_custom_batches_on_null_error(
    kind: str,
) -> None:
    """A terminal null error closes the claimed iterator without pulling a later batch."""
    from fpstreams.planning.arrow_source import ArrowBatchSource
    from fpstreams.planning.source import Source, SourceCapabilities

    events: list[str] = []
    first = pa.record_batch({"value": pa.array([2, 3], type=pa.int64())})
    null = pa.record_batch({"value": pa.array([None], type=pa.int64())})
    unpulled = pa.record_batch({"value": pa.array([5], type=pa.int64())})
    descriptor = ArrowBatchSource(
        lambda: _TrackedArrowBatches([first, null, unpulled], events),
        "reader",
        65_536,
        first.schema,
        False,
    )

    def forbidden_rows() -> Iterator[dict[str, int | None]]:
        raise AssertionError("custom Arrow reader null handling must not open rows")
        yield

    source = Source(
        forbidden_rows,
        SourceCapabilities(reiterable=False, exact_size=None),
        native_data=descriptor,
    )
    aggregated = fpstreams.Rows(fpstreams.Flow(source)).aggregate(
        result=getattr(fpstreams.agg, kind)("value")
    )

    with pytest.raises(TypeError, match="NoneType"):
        aggregated.to_list()
    assert events == ["pull:0", "pull:1", "close"]
    with pytest.raises(fpstreams.FlowConsumedError):
        aggregated.to_list()
    assert events == ["pull:0", "pull:1", "close"]


@pytest.mark.parametrize("kind", ["sum", "min", "max"])
def test_arrow_reader_global_reduction_closes_custom_batches_on_compute_error(
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    """A backend failure after claim closes the iterator and cannot speculatively replay it."""
    from fpstreams.execution import relational as relational_execution
    from fpstreams.planning.arrow_source import ArrowBatchSource
    from fpstreams.planning.source import Source, SourceCapabilities

    events: list[str] = []
    batch = pa.record_batch({"value": pa.array(range(129), type=pa.int64())})
    descriptor = ArrowBatchSource(
        lambda: _TrackedArrowBatches([batch], events),
        "reader",
        65_536,
        batch.schema,
        False,
    )

    def forbidden_rows() -> Iterator[dict[str, int]]:
        raise AssertionError("claimed Arrow compute failure must not reopen rows")
        yield

    source = Source(
        forbidden_rows,
        SourceCapabilities(reiterable=False, exact_size=None),
        native_data=descriptor,
    )
    aggregated = fpstreams.Rows(fpstreams.Flow(source)).aggregate(
        result=getattr(fpstreams.agg, kind)("value")
    )
    imported = relational_execution.import_module

    class FailingCompute:
        @staticmethod
        def min_max(_values: object) -> object:
            raise ValueError("stream compute")

    def import_backend(name: str) -> object:
        return FailingCompute if name == "pyarrow.compute" else imported(name)

    monkeypatch.setattr(relational_execution, "import_module", import_backend)

    with pytest.raises(ValueError, match="stream compute"):
        aggregated.to_list()
    assert events == ["pull:0", "close"]
    with pytest.raises(fpstreams.FlowConsumedError):
        aggregated.to_list()
    assert events == ["pull:0", "close"]


def test_arrow_reader_missing_sum_pulls_empty_then_nonempty_and_closes() -> None:
    """Missing-field fallback preserves batch pull order and closes at the first row error."""
    from fpstreams.planning.arrow_source import ArrowBatchSource
    from fpstreams.planning.source import Source, SourceCapabilities

    events: list[str] = []
    empty = pa.record_batch({"present": pa.array([], type=pa.int64())})
    present = pa.record_batch({"present": pa.array([1], type=pa.int64())})
    unpulled = pa.record_batch({"present": pa.array([2], type=pa.int64())})
    descriptor = ArrowBatchSource(
        lambda: _TrackedArrowBatches([empty, present, unpulled], events),
        "reader",
        65_536,
        empty.schema,
        False,
    )

    def rows() -> Iterator[dict[str, int]]:
        batches = descriptor.open_batches()
        try:
            for batch in batches:
                yield from batch.to_pylist()
        finally:
            batches.close()

    source = Source(
        rows,
        SourceCapabilities(reiterable=False, exact_size=None),
        native_data=descriptor,
    )
    aggregated = fpstreams.Rows(fpstreams.Flow(source)).aggregate(
        total=fpstreams.agg.sum("missing")
    )

    with pytest.raises(fpstreams.SelectionError):
        aggregated.to_list()
    assert events == ["pull:0", "pull:1", "close"]
    with pytest.raises(fpstreams.FlowConsumedError):
        aggregated.to_list()
    assert events == ["pull:0", "pull:1", "close"]


def test_stateful_dataframe_provider_is_not_reopened_after_global_sum_decline() -> None:
    """A speculative dtype guard must not observe a generic dataframe provider twice."""

    class StatefulFrame:
        calls = 0

        def __dataframe__(self, **_options: object) -> object:
            return self

        def __arrow_c_stream__(self, requested_schema: object = None) -> object:
            self.calls += 1
            table = pa.table({"value": pa.array([self.calls], type=pa.int32())})
            return table.__arrow_c_stream__(requested_schema)

    from fpstreams.physical.relational import GlobalAggregatePhysicalNode
    from fpstreams.planning.compiler import compile_query

    frame = StatefulFrame()
    aggregated = fpstreams.rows.from_dataframe(frame).aggregate(total=fpstreams.agg.sum("value"))
    physical = compile_query(aggregated._flow._query("list"))
    assert isinstance(physical.root, GlobalAggregatePhysicalNode)
    assert physical.root.arrow_i64_sum is None

    result = aggregated.to_list()

    assert result == [{"total": 1}]
    assert frame.calls == 1


def test_direct_columnar_global_sum_is_rejected_for_protocol_sensitive_shapes() -> None:
    """Only the exact direct, eager, automatic, single-sum shape earns the marker."""
    from fpstreams.physical.relational import GlobalAggregatePhysicalNode
    from fpstreams.planning.compiler import compile_query

    table = pa.table({"value": [2, 3], "nested.value": [4, 5]})
    direct = fpstreams.rows.from_arrow(table)
    candidates = (
        direct.with_engine("python").aggregate(total=fpstreams.agg.sum("value")),
        direct.select("value").aggregate(first=fpstreams.agg.first("value")),
        direct.aggregate(total=fpstreams.agg.sum(lambda row: row["value"])),
        direct.aggregate(total=fpstreams.agg.sum("nested.value")),
        fpstreams.rows.from_polars(pl.DataFrame({"value": [2, 3]}).lazy()).aggregate(
            total=fpstreams.agg.sum("value")
        ),
    )
    for candidate in candidates:
        physical = compile_query(candidate._flow._query("list"))
        assert isinstance(physical.root, GlobalAggregatePhysicalNode)
        assert physical.root.arrow_i64_sum is None


def test_arrow_global_aggregate_revalidates_captured_source_function_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retained Arrow metadata must not outlive a captured row opener function."""
    rows = fpstreams.rows.from_arrow(pa.table({"value": [1] * 32}))
    source_factory = rows._flow._pipeline.source._factory
    closure = dict(
        zip(
            source_factory.__code__.co_freevars,
            source_factory.__closure__ or (),
            strict=True,
        )
    )
    captured_rows = closure["rows"].cell_contents

    def replacement_factory() -> Callable[[], Iterator[dict[str, int]]]:
        descriptor = object()
        source = object()

        def replacement() -> Iterator[dict[str, int]]:
            if descriptor is source:
                return
            yield from ({"value": 9} for _index in range(32))

        return replacement

    monkeypatch.setattr(captured_rows, "__code__", replacement_factory().__code__)
    expected = [{"total": 288}]

    assert (
        rows.with_engine("python").aggregate(total=fpstreams.agg.sum("value")).to_list() == expected
    )
    assert rows.aggregate(total=fpstreams.agg.sum("value")).to_list() == expected


def test_direct_arrow_global_sum_preserves_empty_fallback_and_bigint_semantics() -> None:
    """The columnar reduction keeps Python's selector timing, errors, and exact integers."""
    from fpstreams.runtime.failpoints import failpoint

    maximum = 2**63 - 1
    minimum = -(2**63)

    assert (
        fpstreams.rows.from_arrow(pa.table({"present": pa.array([], type=pa.int64())}))
        .aggregate(total=fpstreams.agg.sum("missing"))
        .to_list()
    ) == [{"total": 0}]
    assert (
        fpstreams.rows.from_arrow(pa.table({"value": [maximum, 1]}))
        .aggregate(total=fpstreams.agg.sum("value"))
        .to_list()
    ) == [{"total": 2**63}]
    assert (
        fpstreams.rows.from_arrow(pa.table({"value": [minimum, -1]}))
        .aggregate(total=fpstreams.agg.sum("value"))
        .to_list()
    ) == [{"total": -(2**63) - 1}]

    with pytest.raises(fpstreams.SelectionError):
        (
            fpstreams.rows.from_arrow(pa.table({"present": [1]}))
            .aggregate(total=fpstreams.agg.sum("missing"))
            .to_list()
        )
    with pytest.raises(TypeError, match="unsupported operand"):
        (
            fpstreams.rows.from_arrow(pa.table({"value": pa.array([None], pa.int64())}))
            .aggregate(total=fpstreams.agg.sum("value"))
            .to_list()
        )
    assert (
        fpstreams.rows.from_arrow(pa.table({"value": pa.array([1.25, 2.5], pa.float64())}))
        .aggregate(total=fpstreams.agg.sum("value"))
        .to_list()
    ) == [{"total": 3.75}]
    with failpoint("unrelated.transition", RuntimeError("unused")):
        assert (
            fpstreams.rows.from_arrow(pa.table({"value": [2, 3]}))
            .aggregate(total=fpstreams.agg.sum("value"))
            .to_list()
        ) == [{"total": 5}]


def test_direct_arrow_global_sum_bigints_do_not_reopen_python_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both decimal128 overflow guards must finish from the retained Arrow table."""
    from fpstreams.planning.source import Source

    maximum = 2**63 - 1
    minimum = -(2**63)
    overflow = fpstreams.rows.from_arrow(pa.table({"value": [maximum, 1]}))
    underflow = fpstreams.rows.from_arrow(pa.table({"value": [minimum, -1]}))
    guarded_sources = {overflow._flow._pipeline.source, underflow._flow._pipeline.source}
    open_source = Source.open

    def reject_row_source(candidate: Source[object]) -> Iterator[object]:
        if candidate in guarded_sources:
            raise AssertionError("wide global sum must not reopen Python rows")
        return open_source(candidate)

    monkeypatch.setattr(Source, "open", reject_row_source)

    assert overflow.aggregate(total=fpstreams.agg.sum("value")).to_list() == [{"total": 2**63}]
    assert underflow.aggregate(total=fpstreams.agg.sum("value")).to_list() == [
        {"total": -(2**63) - 1}
    ]


@pytest.mark.parametrize(
    "error_type", [ArithmeticError, NotImplementedError, TypeError, ValueError]
)
def test_direct_arrow_global_sum_backend_decline_reopens_canonical_rows(
    monkeypatch: pytest.MonkeyPatch, error_type: type[Exception]
) -> None:
    """Expected Arrow compute rejections leave the replayable row fallback clean."""
    from fpstreams.execution import relational as relational_execution
    from fpstreams.tabular import arrow as arrow_adapter

    imported = relational_execution.import_module
    converted: list[int] = []
    convert_rows = arrow_adapter.batch_to_rows

    class RejectingCompute:
        @staticmethod
        def min_max(_values: object) -> object:
            raise error_type("decline")

    def import_backend(name: str) -> object:
        return RejectingCompute if name == "pyarrow.compute" else imported(name)

    def tracked(batch: object) -> list[dict[str, object]]:
        converted.append(batch.num_rows)  # type: ignore[attr-defined]
        return convert_rows(batch)

    monkeypatch.setattr(relational_execution, "import_module", import_backend)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", tracked)
    result = (
        fpstreams.rows.from_arrow(pa.table({"value": [2, 3]}))
        .aggregate(total=fpstreams.agg.sum("value"))
        .to_list()
    )

    assert result == [{"total": 5}]
    assert converted == [2]


@pytest.mark.parametrize(
    "error_type", [ArithmeticError, NotImplementedError, TypeError, ValueError]
)
@pytest.mark.parametrize(("kind", "expected"), [("min", 0), ("max", 255)])
def test_direct_arrow_extrema_backend_decline_reopens_canonical_rows(
    monkeypatch: pytest.MonkeyPatch,
    error_type: type[Exception],
    kind: str,
    expected: int,
) -> None:
    """Replayable extrema fall back only for an expected Arrow kernel rejection."""
    from fpstreams.execution import relational as relational_execution
    from fpstreams.tabular import arrow as arrow_adapter

    imported = relational_execution.import_module
    converted: list[int] = []
    convert_rows = arrow_adapter.batch_to_rows

    class RejectingCompute:
        @staticmethod
        def min_max(_values: object) -> object:
            raise error_type("decline")

    def import_backend(name: str) -> object:
        return RejectingCompute if name == "pyarrow.compute" else imported(name)

    def tracked(batch: object) -> list[dict[str, object]]:
        converted.append(batch.num_rows)  # type: ignore[attr-defined]
        return convert_rows(batch)

    monkeypatch.setattr(relational_execution, "import_module", import_backend)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", tracked)
    result = (
        fpstreams.rows.from_arrow(pa.table({"value": range(256)}))
        .aggregate(result=getattr(fpstreams.agg, kind)("value"))
        .to_list()
    )

    assert result == [{"result": expected}]
    assert converted == [256]


def test_direct_arrow_global_sum_memory_error_propagates_without_row_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Allocation failure must not be hidden by a second canonical scan."""
    from fpstreams.execution import relational as relational_execution
    from fpstreams.tabular import arrow as arrow_adapter

    imported = relational_execution.import_module

    class FailingCompute:
        @staticmethod
        def min_max(_values: object) -> object:
            raise MemoryError("global allocation")

    def import_backend(name: str) -> object:
        return FailingCompute if name == "pyarrow.compute" else imported(name)

    def forbidden_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("MemoryError must not reopen the canonical row source")

    monkeypatch.setattr(relational_execution, "import_module", import_backend)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", forbidden_rows)
    aggregated = fpstreams.rows.from_arrow(pa.table({"value": [2]})).aggregate(
        total=fpstreams.agg.sum("value")
    )

    with pytest.raises(MemoryError, match="global allocation"):
        aggregated.to_list()


def test_direct_arrow_global_sum_unexpected_error_propagates_without_row_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unexpected backend failure is not silently reclassified as an unsupported kernel."""
    from fpstreams.execution import relational as relational_execution
    from fpstreams.tabular import arrow as arrow_adapter

    imported = relational_execution.import_module

    class FailingCompute:
        @staticmethod
        def min_max(_values: object) -> object:
            raise RuntimeError("global backend")

    def import_backend(name: str) -> object:
        return FailingCompute if name == "pyarrow.compute" else imported(name)

    def forbidden_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("unexpected backend error must not reopen rows")

    monkeypatch.setattr(relational_execution, "import_module", import_backend)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", forbidden_rows)
    aggregated = fpstreams.rows.from_arrow(pa.table({"value": [2]})).aggregate(
        total=fpstreams.agg.sum("value")
    )

    with pytest.raises(RuntimeError, match="global backend"):
        aggregated.to_list()


@pytest.mark.parametrize("adapter", ["arrow", "dataframe", "polars"])
def test_guarded_columnar_filter_count_never_opens_python_rows(
    monkeypatch: pytest.MonkeyPatch, adapter: str
) -> None:
    """A complete safe filter should count surviving batch rows without record boxing."""
    from fpstreams.planning.compiler import compile_query
    from fpstreams.planning.source import Source

    data = {"value": [1, 3, 5, 2]}
    if adapter == "arrow":
        source = fpstreams.rows.from_arrow(pa.table(data), batch_size=1)
    elif adapter == "dataframe":
        source = fpstreams.rows.from_dataframe(pd.DataFrame(data), batch_size=1)
    else:
        source = fpstreams.rows.from_polars(pl.DataFrame(data), batch_size=1)
    filtered = source.where(fpstreams.col("value") >= 3)
    physical = compile_query(filtered._flow._query("count"))
    input_source = physical.source
    open_source = Source.open

    def reject_row_source(candidate: Source[object]) -> Iterator[object]:
        if candidate is input_source:
            raise AssertionError("columnar count must not open Python rows")
        return open_source(candidate)

    monkeypatch.setattr(Source, "open", reject_row_source)

    assert filtered.count() == 2


@pytest.mark.parametrize(
    ("prefix_shape", "expected"),
    [
        ("filter", 4),
        ("select", 5),
        ("filter_select", 4),
        ("two_filters", 3),
    ],
)
def test_safe_arrow_prefix_global_count_never_opens_python_input_rows(
    monkeypatch: pytest.MonkeyPatch,
    prefix_shape: str,
    expected: int,
) -> None:
    """A closed count should reuse every table-safe retained Arrow prefix."""
    from fpstreams.execution import arrow as arrow_execution
    from fpstreams.physical.relational import GlobalAggregatePhysicalNode
    from fpstreams.planning.compiler import compile_query
    from fpstreams.planning.source import Source

    source = fpstreams.rows.from_arrow(
        pa.table(
            {
                "key": [2, 1, 2, 3, 1],
                "value": [4, 7, 3, 99, 2],
                "flag": [True, True, True, False, True],
            }
        ),
        batch_size=2,
    )
    input_source = source._flow._pipeline.source
    if prefix_shape != "select":
        source = source.where(fpstreams.col("flag") == True)  # noqa: E712
    if prefix_shape == "two_filters":
        source = source.where(fpstreams.col("value") >= 3)
    if prefix_shape in {"select", "filter_select"}:
        source = source.select("key", "value")
    aggregated = source.aggregate(rows=fpstreams.agg.count())
    physical = compile_query(aggregated._flow._query("list"))
    assert isinstance(physical.root, GlobalAggregatePhysicalNode)
    assert physical.root.arrow_count_name == "rows"
    open_source = Source.open

    def reject_arrow_row_open(candidate: Source[object]) -> Iterator[object]:
        if candidate is input_source:
            raise AssertionError("safe Arrow prefix/global count opened Python input rows")
        return open_source(candidate)

    def reject_batch_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("safe Arrow prefix/global count boxed a Python input row")

    def reject_projected_rows(_batch: object, _projection: object) -> Iterator[object]:
        raise AssertionError("safe Arrow prefix/global count boxed projected Python rows")

    monkeypatch.setattr(Source, "open", reject_arrow_row_open)
    monkeypatch.setattr(arrow_execution, "batch_to_rows", reject_batch_rows)
    monkeypatch.setattr(arrow_execution, "_project_batch_rows", reject_projected_rows)

    assert aggregated.to_list() == [{"rows": expected}]
    assert aggregated.to_list() == [{"rows": expected}]


def test_arrow_prefix_global_count_preserves_projection_and_reader_boundaries() -> None:
    """Count keeps empty projection semantics and never claims a one-shot reader prefix."""
    from fpstreams.physical.relational import GlobalAggregatePhysicalNode
    from fpstreams.planning.compiler import compile_query

    empty = fpstreams.rows.from_arrow(pa.table({"present": pa.array([], pa.int64())}))
    assert empty.select("missing").aggregate(rows=fpstreams.agg.count()).to_list() == [{"rows": 0}]

    nonempty = fpstreams.rows.from_arrow(pa.table({"present": [1]}))
    automatic = nonempty.select("missing").aggregate(rows=fpstreams.agg.count())
    canonical = (
        nonempty.select("missing").with_engine("python").aggregate(rows=fpstreams.agg.count())
    )
    with pytest.raises(fpstreams.SelectionError) as canonical_error:
        canonical.to_list()
    with pytest.raises(fpstreams.SelectionError) as automatic_error:
        automatic.to_list()
    assert str(automatic_error.value) == str(canonical_error.value)

    reader = pa.RecordBatchReader.from_batches(
        pa.schema([("value", pa.int64())]),
        [pa.record_batch({"value": [1, 2, 3]})],
    )
    one_shot = (
        fpstreams.rows.from_arrow(reader)
        .where(fpstreams.col("value") >= 2)
        .aggregate(rows=fpstreams.agg.count())
    )
    physical = compile_query(one_shot._flow._query("list"))
    assert isinstance(physical.root, GlobalAggregatePhysicalNode)
    assert physical.root.arrow_count_name is None
    assert one_shot.to_list() == [{"rows": 2}]
    with pytest.raises(fpstreams.FlowConsumedError):
        one_shot.to_list()


@pytest.mark.parametrize(
    ("kind", "expected"),
    [("sum", 16), ("min", 2), ("max", 7), ("last", 2)],
)
def test_safe_arrow_prefix_global_reduction_never_opens_python_input_rows(
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    expected: int,
) -> None:
    """Total int64 reductions should consume a safe retained prefix columnarly."""
    from fpstreams.execution import arrow as arrow_execution
    from fpstreams.physical.relational import GlobalAggregatePhysicalNode
    from fpstreams.planning.compiler import compile_query
    from fpstreams.planning.source import Source

    source = fpstreams.rows.from_arrow(
        pa.table(
            {
                "value": [4, 7, 3, 99, 2],
                "flag": [True, True, True, False, True],
            }
        ),
        batch_size=2,
    )
    input_source = source._flow._pipeline.source
    aggregated = (
        source.where(fpstreams.col("flag") == True)  # noqa: E712
        .select(amount="value")
        .aggregate(result=getattr(fpstreams.agg, kind)("amount"))
    )
    physical = compile_query(aggregated._flow._query("list"))
    assert isinstance(physical.root, GlobalAggregatePhysicalNode)
    assert physical.root.arrow_i64_sum is not None
    assert physical.root.arrow_i64_sum.kind == kind
    open_source = Source.open

    def reject_arrow_row_open(candidate: Source[object]) -> Iterator[object]:
        if candidate is input_source:
            raise AssertionError("safe Arrow prefix/global reduction opened Python input rows")
        return open_source(candidate)

    def reject_batch_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("safe Arrow prefix/global reduction boxed a Python input row")

    def reject_projected_rows(_batch: object, _projection: object) -> Iterator[object]:
        raise AssertionError("safe Arrow prefix/global reduction boxed projected Python rows")

    monkeypatch.setattr(Source, "open", reject_arrow_row_open)
    monkeypatch.setattr(arrow_execution, "batch_to_rows", reject_batch_rows)
    monkeypatch.setattr(arrow_execution, "_project_batch_rows", reject_projected_rows)

    assert aggregated.to_list() == [{"result": expected}]


def test_arrow_prefix_global_reduction_preserves_earlier_aggregate_error() -> None:
    """A later prefix error must not replace an earlier streaming reduction error."""
    from fpstreams.planning.arrow_source import ArrowBatchSource
    from fpstreams.planning.source import Source, SourceCapabilities
    from fpstreams.tabular import arrow as arrow_adapter

    batches = (
        pa.record_batch(
            {
                "value": pa.array([None], type=pa.int64()),
                "flag": pa.array([1], type=pa.int64()),
            }
        ),
        pa.record_batch(
            {
                "value": pa.array([1], type=pa.int64()),
                "flag": pa.array([None], type=pa.int64()),
            }
        ),
    )
    table = pa.Table.from_batches(batches)

    def build(engine: str) -> tuple[fpstreams.Rows[dict[str, object]], list[str]]:
        events: list[str] = []

        def open_batches() -> Iterator[pa.RecordBatch]:
            events.append("open")
            return _TrackedArrowBatches(batches, events)

        descriptor = ArrowBatchSource(
            open_batches,
            "table",
            1,
            table.schema,
            True,
            materialized_data=table,
        )

        def rows() -> Iterator[dict[str, object]]:
            opened = descriptor.open_batches()
            try:
                for batch in opened:
                    yield from arrow_adapter.batch_to_rows(batch)
            finally:
                opened.close()  # type: ignore[attr-defined]

        source = Source(
            rows,
            SourceCapabilities(reiterable=True, exact_size=2),
            native_data=descriptor,
        )
        values = fpstreams.Rows(fpstreams.Flow(source)).where(fpstreams.col("flag") >= 0)
        if engine == "python":
            values = values.with_engine("python")
        return values.aggregate(total=fpstreams.agg.sum("value")), events

    canonical, canonical_events = build("python")
    with pytest.raises(TypeError) as canonical_error:
        canonical.to_list()

    automatic, automatic_events = build("auto")
    with pytest.raises(TypeError) as automatic_error:
        automatic.to_list()

    assert str(automatic_error.value) == str(canonical_error.value)
    assert canonical_events == ["open", "pull:0", "close"]
    assert automatic_events == canonical_events


def test_arrow_prefix_global_reduction_falls_back_on_its_claimed_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A late kernel decline must not reopen an already materialized prefix."""
    from fpstreams.execution import relational as relational_execution
    from fpstreams.planning.source import Source

    source = fpstreams.rows.from_arrow(pa.table({"value": [2, 3, 4], "flag": [True, False, True]}))
    input_source = source._flow._pipeline.source
    aggregated = (
        source.where(fpstreams.col("flag") == True)  # noqa: E712
        .select(amount="value")
        .aggregate(total=fpstreams.agg.sum("amount"))
    )
    open_source = Source.open

    def reject_arrow_row_open(candidate: Source[object]) -> Iterator[object]:
        if candidate is input_source:
            raise AssertionError("claimed Arrow global reduction reopened its source")
        return open_source(candidate)

    monkeypatch.setattr(Source, "open", reject_arrow_row_open)
    monkeypatch.setattr(relational_execution, "_reduce_arrow_table", lambda *_args: None)

    assert aggregated.to_list() == [{"total": 6}]


@pytest.mark.parametrize("adapter", ["dataframe", "polars"])
def test_guarded_eager_frame_identity_count_never_opens_python_rows(
    monkeypatch: pytest.MonkeyPatch, adapter: str
) -> None:
    """Unknown adapter size may be counted from zero-operation batches without boxing."""
    from fpstreams.planning.source import Source

    data = {"value": [1, 2, 3, 4]}
    source = (
        fpstreams.rows.from_dataframe(pd.DataFrame(data), batch_size=1)
        if adapter == "dataframe"
        else fpstreams.rows.from_polars(pl.DataFrame(data), batch_size=1)
    )
    input_source = source._flow._pipeline.source
    open_source = Source.open

    def reject_row_source(candidate: Source[object]) -> Iterator[object]:
        if candidate is input_source:
            raise AssertionError("identity columnar count must not open Python rows")
        return open_source(candidate)

    monkeypatch.setattr(Source, "open", reject_row_source)

    assert source.count() == 4


def test_guarded_columnar_count_preserves_selector_errors_and_failpoints() -> None:
    """Batch counting keeps canonical empty, missing, null, division, and hook behavior."""
    from fpstreams.physical.plan import BackendPayload
    from fpstreams.planning.compiler import compile_query
    from fpstreams.runtime.failpoints import failpoint

    empty = fpstreams.rows.from_arrow(pa.table({"present": pa.array([], pa.int64())}))
    assert empty.select("missing").count() == 0

    with pytest.raises(fpstreams.SelectionError):
        fpstreams.rows.from_arrow(pa.table({"present": [1]})).select("missing").count()
    with pytest.raises(TypeError):
        (
            fpstreams.rows.from_arrow(pa.table({"value": pa.array([None], pa.int64())}))
            .where(fpstreams.col("value") >= 1)
            .count()
        )
    with pytest.raises(ZeroDivisionError):
        (
            fpstreams.rows.from_arrow(pa.table({"value": [1]}))
            .where((fpstreams.col("value") / 0) > 1)
            .count()
        )

    forced = (
        fpstreams.rows.from_arrow(pa.table({"value": [1, 2]}))
        .with_engine("python")
        .where(fpstreams.col("value") > 1)
    )
    payload = compile_query(forced._flow._query("count")).backend_payload
    assert isinstance(payload, BackendPayload)
    assert payload.arrow_prefix is None

    automatic = fpstreams.rows.from_arrow(pa.table({"value": [1, 2]})).where(
        fpstreams.col("value") > 1
    )
    with (
        failpoint("source.open.after", RuntimeError("canonical count")),
        pytest.raises(RuntimeError, match="canonical count"),
    ):
        automatic.count()


@pytest.mark.parametrize(
    "error_type", [ArithmeticError, NotImplementedError, TypeError, ValueError]
)
def test_guarded_columnar_count_backend_decline_falls_back_per_batch(
    monkeypatch: pytest.MonkeyPatch, error_type: type[Exception]
) -> None:
    """Recoverable compute errors convert only that batch and never reopen the source."""
    from fpstreams.execution import arrow as arrow_execution
    from fpstreams.planning.source import Source

    lowered = arrow_execution.lower_row_expression
    input_rows = fpstreams.rows.from_arrow(pa.table({"value": [1, 3, 5]}), batch_size=1)
    filtered = input_rows.where(fpstreams.col("value") >= 3)
    input_source = input_rows._flow._pipeline.source
    open_source = Source.open

    def reject_lowering(_node: object, _batch: object) -> object:
        raise error_type("decline")

    def reject_row_source(candidate: Source[object]) -> Iterator[object]:
        if candidate is input_source:
            raise AssertionError("batch fallback must not reopen the source")
        return open_source(candidate)

    monkeypatch.setattr(arrow_execution, "lower_row_expression", reject_lowering)
    monkeypatch.setattr(Source, "open", reject_row_source)
    assert filtered.count() == 2
    monkeypatch.setattr(arrow_execution, "lower_row_expression", lowered)


def test_guarded_columnar_count_memory_error_propagates_without_row_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Allocation failure is not mistaken for an unsupported batch kernel."""
    from fpstreams.execution import arrow as arrow_execution

    def fail_lowering(_node: object, _batch: object) -> object:
        raise MemoryError("count allocation")

    def forbidden_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("MemoryError must not materialize rows")

    monkeypatch.setattr(arrow_execution, "lower_row_expression", fail_lowering)
    monkeypatch.setattr(arrow_execution, "batch_to_rows", forbidden_rows)
    filtered = fpstreams.rows.from_arrow(pa.table({"value": [1]})).where(
        fpstreams.col("value") >= 1
    )

    with pytest.raises(MemoryError, match="count allocation"):
        filtered.count()


@pytest.mark.parametrize("storage", ["csv", "parquet"])
def test_guarded_file_filter_count_stays_columnar(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, storage: str
) -> None:
    """CSV and Parquet scans reuse their guarded projection and predicate request path."""
    from fpstreams.tabular import arrow as arrow_adapter

    target = tmp_path / f"values.{storage}"
    if storage == "csv":
        target.write_text("value,unused\n1,10\n3,20\n5,30\n2,40\n")
        source = fpstreams.rows.scan_csv(target, batch_size=1)
    else:
        pq.write_table(pa.table({"value": [1, 3, 5, 2], "unused": [10, 20, 30, 40]}), target)
        source = fpstreams.rows.from_parquet(target, batch_size=1)

    def forbidden_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("safe file count must not materialize rows")

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", forbidden_rows)

    assert source.where(fpstreams.col("value") >= 3).count() == 2


@pytest.mark.parametrize(
    ("prefix_shape", "expected"),
    [
        ("filter", [{"key": 2, "total": 7}, {"key": 1, "total": 9}]),
        (
            "select",
            [
                {"key": 2, "total": 7},
                {"key": 1, "total": 9},
                {"key": 3, "total": 99},
            ],
        ),
        ("filter_select", [{"key": 2, "total": 7}, {"key": 1, "total": 9}]),
        ("two_filters", [{"key": 2, "total": 7}, {"key": 1, "total": 7}]),
    ],
)
def test_safe_arrow_prefix_group_never_opens_python_input_rows(
    monkeypatch: pytest.MonkeyPatch,
    prefix_shape: str,
    expected: list[dict[str, int]],
) -> None:
    """Every table-safe Arrow prefix should hand columns directly to grouping."""
    from fpstreams.execution import arrow as arrow_execution
    from fpstreams.planning.source import Source

    table = pa.table(
        {
            "key": [2, 1, 2, 3, 1],
            "value": [4, 7, 3, 99, 2],
            "flag": [True, True, True, False, True],
        }
    )
    source = fpstreams.rows.from_arrow(table, batch_size=2)
    input_source = source._flow._pipeline.source
    if prefix_shape != "select":
        source = source.where(
            fpstreams.col("flag") == True  # noqa: E712 - builds a primitive RowExpr
        )
    if prefix_shape == "two_filters":
        source = source.where(fpstreams.col("value") >= 3)
    if prefix_shape in {"select", "filter_select"}:
        source = source.select("key", "value")
    grouped = source.group_by("key").aggregate(total=fpstreams.agg.sum("value"))
    open_source = Source.open

    def reject_arrow_row_open(candidate: Source[object]) -> Iterator[object]:
        if candidate is input_source:
            raise AssertionError("safe Arrow filter/select/group opened Python input rows")
        return open_source(candidate)

    def reject_batch_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("safe Arrow filter/select/group boxed a Python input row")

    def reject_projected_rows(_batch: object, _projection: object) -> Iterator[object]:
        raise AssertionError("safe Arrow select/group boxed projected Python rows")

    monkeypatch.setattr(Source, "open", reject_arrow_row_open)
    monkeypatch.setattr(arrow_execution, "batch_to_rows", reject_batch_rows)
    monkeypatch.setattr(arrow_execution, "_project_batch_rows", reject_projected_rows)

    assert grouped.to_list() == expected


def test_safe_arrow_prefix_group_keeps_closed_aggregate_lanes_columnar(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Count, sum, min, and max should consume a safe filtered table without row boxing."""
    from fpstreams.execution import arrow as arrow_execution
    from fpstreams.planning.source import Source

    source = fpstreams.rows.from_arrow(
        pa.table(
            {
                "key": [2, 1, 2, 3, 1],
                "value": [4, 7, 3, 99, 2],
                "flag": [True, True, True, False, True],
            }
        ),
        batch_size=2,
    )
    input_source = source._flow._pipeline.source
    grouped = (
        source.where(fpstreams.col("flag") == True)  # noqa: E712
        .group_by("key")
        .aggregate(
            rows=fpstreams.agg.count(),
            total=fpstreams.agg.sum("value"),
            minimum=fpstreams.agg.min("value"),
            maximum=fpstreams.agg.max("value"),
        )
    )
    open_source = Source.open

    def reject_arrow_row_open(candidate: Source[object]) -> Iterator[object]:
        if candidate is input_source:
            raise AssertionError("safe Arrow aggregate opened Python input rows")
        return open_source(candidate)

    def reject_batch_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("safe Arrow aggregate boxed a Python input row")

    monkeypatch.setattr(Source, "open", reject_arrow_row_open)
    monkeypatch.setattr(arrow_execution, "batch_to_rows", reject_batch_rows)

    assert grouped.to_list() == [
        {"key": 2, "rows": 2, "total": 7, "minimum": 3, "maximum": 4},
        {"key": 1, "rows": 2, "total": 9, "minimum": 2, "maximum": 7},
    ]


def test_arrow_prefix_group_preserves_earlier_group_error_and_source_boundary() -> None:
    """A later prefix error must not replace an earlier streaming aggregate error."""
    from fpstreams.planning.arrow_source import ArrowBatchSource
    from fpstreams.planning.source import Source, SourceCapabilities
    from fpstreams.tabular import arrow as arrow_adapter

    batches = (
        pa.record_batch(
            {
                "key": pa.array([1], type=pa.int64()),
                "value": pa.array([None], type=pa.int64()),
                "flag": pa.array([1], type=pa.int64()),
            }
        ),
        pa.record_batch(
            {
                "key": pa.array([2], type=pa.int64()),
                "value": pa.array([1], type=pa.int64()),
                "flag": pa.array([None], type=pa.int64()),
            }
        ),
    )
    table = pa.Table.from_batches(batches)

    def build(engine: str) -> tuple[fpstreams.Rows[dict[str, object]], list[str]]:
        events: list[str] = []

        def open_batches() -> Iterator[pa.RecordBatch]:
            events.append("open")
            return _TrackedArrowBatches(batches, events)

        descriptor = ArrowBatchSource(
            open_batches,
            "table",
            1,
            table.schema,
            True,
            materialized_data=table,
        )

        def rows() -> Iterator[dict[str, object]]:
            opened = descriptor.open_batches()
            try:
                for batch in opened:
                    yield from arrow_adapter.batch_to_rows(batch)
            finally:
                opened.close()  # type: ignore[attr-defined]

        source = Source(
            rows,
            SourceCapabilities(reiterable=True, exact_size=2),
            native_data=descriptor,
        )
        values = fpstreams.Rows(fpstreams.Flow(source)).where(fpstreams.col("flag") >= 0)
        if engine == "python":
            values = values.with_engine("python")
        grouped = values.group_by("key").aggregate(total=fpstreams.agg.sum("value"))
        return grouped, events

    canonical, canonical_events = build("python")
    with pytest.raises(TypeError) as canonical_error:
        canonical.to_list()

    automatic, automatic_events = build("auto")
    with pytest.raises(TypeError) as automatic_error:
        automatic.to_list()

    assert str(automatic_error.value) == str(canonical_error.value)
    assert canonical_events == ["open", "pull:0", "close"]
    assert automatic_events == canonical_events


def test_arrow_prefix_group_declines_noncanonical_key_schema_before_claim() -> None:
    """A mixed logical Arrow key should keep Python grouping without schema re-inference."""
    keys = pa.UnionArray.from_dense(
        pa.array([0, 1], type=pa.int8()),
        pa.array([0, 0], type=pa.int32()),
        [pa.array([1], type=pa.int64()), pa.array(["x"], type=pa.string())],
        field_names=["integer", "string"],
        type_codes=[0, 1],
    )
    source = fpstreams.rows.from_arrow(
        pa.table({"key": keys, "value": pa.array([2, 3], type=pa.int64())}),
        batch_size=1,
    ).select("key", "value")
    grouped = source.group_by("key").aggregate(total=fpstreams.agg.sum("value"))
    expected = [{"key": 1, "total": 2}, {"key": "x", "total": 3}]

    assert grouped.with_engine("python").to_list() == expected
    assert grouped.to_list() == expected


def test_arrow_prefix_group_preserves_unused_dense_union_rows() -> None:
    """An unused mixed column must not force a claimed table through schema inference."""
    unused = pa.UnionArray.from_dense(
        pa.array([0, 1], type=pa.int8()),
        pa.array([0, 0], type=pa.int32()),
        [pa.array([1], type=pa.int64()), pa.array(["x"], type=pa.string())],
        field_names=["integer", "string"],
        type_codes=[0, 1],
    )
    source = fpstreams.rows.from_arrow(
        pa.table({"key": [1, 1], "value": [2, 3], "unused": unused}),
        batch_size=1,
    )
    grouped = (
        source.where(fpstreams.col("value") >= 0)
        .select("key", "value")
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
    )
    expected = [{"key": 1, "total": 5}]

    assert grouped.with_engine("python").to_list() == expected
    assert grouped.to_list() == expected


@pytest.mark.parametrize("terminal", ["group", "count", "sum"])
def test_arrow_prefix_relations_preserve_unused_invalid_utf8(terminal: str) -> None:
    """A fast relational terminal must still observe source row-conversion errors."""
    invalid_utf8 = pa.array([b"\xff"], type=pa.binary()).view(pa.string())
    table = pa.Table.from_arrays(
        [pa.array([1]), pa.array([2]), invalid_utf8],
        names=["key", "value", "unused"],
    )

    def build(engine: str) -> fpstreams.Rows[dict[str, object]]:
        source = fpstreams.rows.from_arrow(table, batch_size=1)
        if engine == "python":
            source = source.with_engine("python")
        values = source.where(fpstreams.col("value") >= 0)
        if terminal == "group":
            return values.group_by("key").aggregate(total=fpstreams.agg.sum("value"))
        if terminal == "count":
            return values.aggregate(rows=fpstreams.agg.count())
        return values.aggregate(total=fpstreams.agg.sum("value"))

    with pytest.raises(UnicodeDecodeError) as canonical_error:
        build("python").to_list()
    with pytest.raises(UnicodeDecodeError) as automatic_error:
        build("auto").to_list()
    assert str(automatic_error.value) == str(canonical_error.value)


@pytest.mark.parametrize("terminal", ["group", "sum", "last"])
def test_arrow_prefix_relations_preserve_late_non_null_schema_anchor(terminal: str) -> None:
    """A null-only first batch must not trap later values in an inferred null schema."""
    table = pa.table(
        {
            "key": pa.array([None, None, 1], type=pa.int64()),
            "value": [2, 3, 4],
            "flag": [True, True, True],
        }
    )

    def build(engine: str) -> fpstreams.Rows[dict[str, object]]:
        source = fpstreams.rows.from_arrow(table, batch_size=2)
        if engine == "python":
            source = source.with_engine("python")
        values = source.where(fpstreams.col("flag") == True)  # noqa: E712
        if terminal == "group":
            return values.group_by("key").aggregate(total=fpstreams.agg.sum("value"))
        return values.aggregate(result=getattr(fpstreams.agg, terminal)("value"))

    canonical = build("python").to_list()
    assert build("auto").to_list() == canonical


@pytest.mark.parametrize("adapter", ["table", "reader"])
@pytest.mark.parametrize(
    "terminal",
    ["group", "count", "sum", "min", "max", "first", "last"],
)
def test_direct_arrow_relations_preserve_unused_invalid_utf8(
    adapter: str,
    terminal: str,
) -> None:
    """Direct Arrow kernels must observe conversion errors in each consumed source batch."""
    invalid_utf8 = pa.array([b"\xff", b"ok"], type=pa.binary()).view(pa.string())
    table = pa.Table.from_arrays(
        [pa.array([1, 1]), pa.array([2, 3]), invalid_utf8],
        names=["key", "value", "unused"],
    )

    def build(engine: str) -> fpstreams.Rows[dict[str, object]]:
        source_data: object
        if adapter == "table":
            source_data = table
        else:
            source_data = pa.RecordBatchReader.from_batches(table.schema, table.to_batches())
        source = fpstreams.rows.from_arrow(source_data, batch_size=1)
        if engine == "python":
            source = source.with_engine("python")
        if terminal == "group":
            return source.group_by("key").aggregate(total=fpstreams.agg.sum("value"))
        if terminal == "count":
            return source.aggregate(result=fpstreams.agg.count())
        return source.aggregate(result=getattr(fpstreams.agg, terminal)("value"))

    with pytest.raises(UnicodeDecodeError) as canonical_error:
        build("python").to_list()
    with pytest.raises(UnicodeDecodeError) as automatic_error:
        build("auto").to_list()
    assert str(automatic_error.value) == str(canonical_error.value)


@pytest.mark.parametrize("terminal", ["list", "first", "count"])
def test_arrow_projection_preserves_unused_invalid_utf8(terminal: str) -> None:
    """Projection and cardinality kernels must observe source row-conversion errors."""
    invalid_utf8 = pa.array([b"\xff"], type=pa.binary()).view(pa.string())
    table = pa.Table.from_arrays(
        [pa.array([1]), invalid_utf8],
        names=["value", "unused"],
    )

    def run(engine: str) -> object:
        values = fpstreams.rows.from_arrow(table, batch_size=1).select("value")
        if engine == "python":
            values = values.with_engine("python")
        if terminal == "list":
            return values.to_list()
        return getattr(values, terminal)()

    with pytest.raises(UnicodeDecodeError) as canonical_error:
        run("python")
    with pytest.raises(UnicodeDecodeError) as automatic_error:
        run("auto")
    assert str(automatic_error.value) == str(canonical_error.value)


def test_arrow_projection_first_does_not_validate_an_unpulled_later_batch() -> None:
    """A conversion guard must retain first()'s batch-level source short circuit."""
    late_invalid_utf8 = pa.array([b"ok", b"\xff"], type=pa.binary()).view(pa.string())
    table = pa.Table.from_arrays(
        [pa.array([1, 2]), late_invalid_utf8],
        names=["value", "unused"],
    )

    def run(engine: str) -> object:
        values = fpstreams.rows.from_arrow(table, batch_size=1).select("value")
        return values.with_engine(engine).first()

    assert run("python") == {"value": 1}
    assert run("auto") == {"value": 1}


def test_polars_object_group_prefix_keeps_python_key_objects() -> None:
    """Polars Object keys must not become Arrow pointer bytes during grouped execution."""

    @dataclass(frozen=True)
    class Key:
        value: int

    frame = pl.DataFrame(
        {
            "key": pl.Series("key", [Key(1), Key(1), Key(2)], dtype=pl.Object),
            "value": [2, 3, 4],
        }
    )
    grouped = (
        fpstreams.rows.from_polars(frame)
        .select("key", "value")
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
    )
    expected = [{"key": Key(1), "total": 5}, {"key": Key(2), "total": 4}]

    assert grouped.with_engine("python").to_list() == expected
    assert grouped.to_list() == expected


def test_arrow_prefix_group_does_not_swallow_output_name_hash_errors() -> None:
    """Python output-key protocol errors must propagate instead of triggering a retry."""

    class Name(str):
        def __new__(cls, value: str, fail_at: int) -> Name:
            item = super().__new__(cls, value)
            item.calls = 0
            item.fail_at = fail_at
            return item

        def __hash__(self) -> int:
            self.calls += 1
            if self.calls == self.fail_at:
                raise TypeError(f"hash failure {self}")
            return super().__hash__()

    def build(engine: str) -> tuple[fpstreams.Rows[dict[str, object]], Name]:
        key_name = Name("group", 6)
        total_name = Name("total", 99)
        source = fpstreams.rows.from_arrow(pa.table({"key": [1, 1], "value": [2, 3]})).select(
            "key", "value"
        )
        if engine == "python":
            source = source.with_engine("python")
        grouped = source.group_by(**{key_name: "key"}).aggregate(
            **{total_name: fpstreams.agg.sum("value")}
        )
        return grouped, key_name

    canonical, canonical_name = build("python")
    with pytest.raises(TypeError, match="hash failure group"):
        canonical.to_list()
    assert canonical_name.calls == 6

    automatic, automatic_name = build("auto")
    with pytest.raises(TypeError, match="hash failure group"):
        automatic.to_list()
    assert automatic_name.calls == 6


def test_direct_arrow_global_does_not_swallow_output_name_hash_errors() -> None:
    """A result-key error is not an Arrow compute decline and must not replay input."""

    class Name(str):
        def __new__(cls, value: str) -> Name:
            item = super().__new__(cls, value)
            item.calls = 0
            return item

        def __hash__(self) -> int:
            self.calls += 1
            if self.calls == 3:
                raise TypeError("global output hash failure")
            return super().__hash__()

    def build(engine: str) -> tuple[fpstreams.Rows[dict[str, object]], Name]:
        output_name = Name("total")
        source = fpstreams.rows.from_arrow(pa.table({"value": [2, 3]}))
        if engine == "python":
            source = source.with_engine("python")
        return source.aggregate(**{output_name: fpstreams.agg.sum("value")}), output_name

    canonical, canonical_name = build("python")
    with pytest.raises(TypeError, match="global output hash failure"):
        canonical.to_list()
    assert canonical_name.calls == 3

    automatic, automatic_name = build("auto")
    with pytest.raises(TypeError, match="global output hash failure"):
        automatic.to_list()
    assert automatic_name.calls == 3


def test_direct_arrow_group_sum_is_visible_only_for_the_guarded_plan_shape() -> None:
    """Only direct replayable Arrow fields earn the speculative columnar marker."""
    from fpstreams.physical.relational import ArrowGroupSumSpec, GroupAggregatePhysicalNode
    from fpstreams.planning.compiler import compile_query

    table = pa.table({"key": [1, 2], "value": [3, 4]})
    direct = (
        fpstreams.rows.from_arrow(table).group_by("key").aggregate(total=fpstreams.agg.sum("value"))
    )
    physical = compile_query(direct._flow._query("list"))

    assert isinstance(physical.root, GroupAggregatePhysicalNode)
    assert physical.root.arrow_i64_sum == ArrowGroupSumSpec("key", "value", "total")
    relation = direct._flow.explain("list").to_dict()["relations"]
    assert relation["strategy"] == "hash"
    assert relation["candidate"] == "arrow_hash"
    assert relation["guarded"] is True

    reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches())
    reader_direct = (
        fpstreams.rows.from_arrow(reader)
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
    )
    reader_physical = compile_query(reader_direct._flow._query("list"))
    assert isinstance(reader_physical.root, GroupAggregatePhysicalNode)
    assert reader_physical.root.arrow_i64_sum == ArrowGroupSumSpec("key", "value", "total")

    candidates = (
        direct.with_engine("python"),
        fpstreams.rows.from_arrow(table)
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum(lambda row: row["value"])),
        fpstreams.rows.from_arrow(table)
        .group_by("key")
        .spill(2)
        .aggregate(total=fpstreams.agg.sum("value")),
        fpstreams.rows.from_arrow(table)
        .where(fpstreams.col("key") >= 1)
        .select("key", "value")
        .group_by("key")
        .spill(2)
        .aggregate(total=fpstreams.agg.sum("value")),
    )
    try:
        for candidate in candidates:
            candidate_physical = compile_query(candidate._flow._query("list"))
            assert isinstance(candidate_physical.root, GroupAggregatePhysicalNode)
            assert candidate_physical.root.arrow_i64_sum is None
    finally:
        reader.close()


@pytest.mark.parametrize("storage", ["csv", "parquet"])
def test_file_arrow_group_sum_streams_supported_fields_without_row_boxing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    storage: str,
) -> None:
    """Direct file fields should aggregate by batch while retaining first-seen order."""
    from fpstreams.tabular import arrow as arrow_adapter

    target = tmp_path / f"group-sum.{storage}"
    keys = [2, 1, 2, 3, 1]
    values = [5, 7, 11, -4, 2]
    if storage == "csv":
        unused = "x" * 10_000
        target.write_text(
            "key,value,unused\n"
            f"2,5,{unused}\n1,7,{unused}\n2,11,{unused}\n"
            f"3,-4,{unused}\n1,2,{unused}\n",
            encoding="utf-8",
        )
        source = fpstreams.rows.scan_csv(target, batch_size=2)
    else:
        pq.write_table(
            pa.table({"key": keys, "value": values, "unused": ["a", "b", "c", "d", "e"]}),
            target,
            row_group_size=2,
        )
        source = fpstreams.rows.from_parquet(target, batch_size=2)

    def forbidden_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("guarded file grouping must not box row dictionaries")

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", forbidden_rows)

    assert source.group_by("key").aggregate(total=fpstreams.agg.sum("value")).to_list() == [
        {"key": 2, "total": 16},
        {"key": 1, "total": 9},
        {"key": 3, "total": -4},
    ]


def test_file_arrow_group_sum_unsupported_schema_continues_without_reopening() -> None:
    """A speculative file schema decline must consume the already-opened batch stream once."""
    from fpstreams.planning.arrow_source import ArrowBatchSource
    from fpstreams.planning.source import Source, SourceCapabilities
    from fpstreams.tabular import arrow as arrow_adapter

    batch = pa.record_batch({"key": [1.5, 2.5, 1.5], "value": [2, 3, 4]})
    events: list[str] = []

    def open_batches() -> Iterator[pa.RecordBatch]:
        events.append("open")
        return _TrackedArrowBatches((batch,), events)

    descriptor = ArrowBatchSource(open_batches, "csv", 65_536)

    def rows() -> Iterator[dict[str, object]]:
        batches = descriptor.open_batches()
        try:
            for current in batches:
                yield from arrow_adapter.batch_to_rows(current)
        finally:
            batches.close()  # type: ignore[attr-defined]

    source = Source(
        rows,
        SourceCapabilities(reiterable=True, exact_size=None),
        native_data=descriptor,
    )
    grouped = (
        fpstreams.Rows(fpstreams.Flow(source))
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
    )

    assert grouped.to_list() == [
        {"key": 1.5, "total": 6},
        {"key": 2.5, "total": 3},
    ]
    assert events == ["open", "pull:0", "stop", "close"]


@pytest.mark.parametrize("storage", ["csv", "parquet"])
def test_file_arrow_group_sum_preserves_null_keys_and_cross_batch_bigints(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    storage: str,
) -> None:
    """File batches keep Python null grouping and unbounded integer addition."""
    from fpstreams.tabular import arrow as arrow_adapter

    maximum = 2**63 - 1
    target = tmp_path / f"wide-group-sum.{storage}"
    if storage == "csv":
        unused = "x" * 10_000
        target.write_text(
            "key,value,unused\n"
            f",{maximum},{unused}\n1,2,{unused}\n"
            f",1,{unused}\n1,{maximum},{unused}\n",
            encoding="utf-8",
        )
        source = fpstreams.rows.scan_csv(target, batch_size=1)
    else:
        pq.write_table(
            pa.table(
                {
                    "key": pa.array([None, 1, None, 1], type=pa.int64()),
                    "value": [maximum, 2, 1, maximum],
                    "unused": ["a", "b", "c", "d"],
                }
            ),
            target,
            row_group_size=1,
        )
        source = fpstreams.rows.from_parquet(target, batch_size=1)

    def forbidden_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("overflow-safe file grouping must remain columnar")

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", forbidden_rows)

    assert source.group_by("key").aggregate(total=fpstreams.agg.sum("value")).to_list() == [
        {"key": None, "total": maximum + 1},
        {"key": 1, "total": maximum + 2},
    ]


def test_file_arrow_group_sum_null_stops_before_later_batches_and_closes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A null value raises at its row without pulling or reopening the next file batch."""
    from fpstreams.planning.arrow_source import ArrowBatchSource
    from fpstreams.planning.source import Source, SourceCapabilities
    from fpstreams.tabular import arrow as arrow_adapter

    batches = (
        pa.record_batch({"key": [1, 2], "value": [2, 3]}),
        pa.record_batch({"key": [1, 1], "value": pa.array([4, None], type=pa.int64())}),
        pa.record_batch({"key": [1], "value": [99]}),
    )
    events: list[str] = []

    def open_batches() -> Iterator[pa.RecordBatch]:
        events.append("open")
        return _TrackedArrowBatches(batches, events)

    descriptor = ArrowBatchSource(open_batches, "parquet", 65_536)

    def rows() -> Iterator[dict[str, object]]:
        iterator = descriptor.open_batches()
        try:
            for batch in iterator:
                yield from arrow_adapter.batch_to_rows(batch)
        finally:
            iterator.close()  # type: ignore[attr-defined]

    source = Source(
        rows,
        SourceCapabilities(reiterable=True, exact_size=None),
        native_data=descriptor,
    )
    grouped = (
        fpstreams.Rows(fpstreams.Flow(source))
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
    )

    def forbidden_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("guarded file grouping must not box null rows")

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", forbidden_rows)
    with pytest.raises(TypeError, match="unsupported operand type"):
        grouped.to_list()
    assert events == ["open", "pull:0", "pull:1", "close"]


def test_file_arrow_group_sum_memory_error_closes_without_reopening(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Allocation failure propagates unchanged and releases the claimed file iterator."""
    from fpstreams.execution import relational as relational_execution
    from fpstreams.planning.arrow_source import ArrowBatchSource
    from fpstreams.planning.source import Source, SourceCapabilities

    imported = relational_execution.import_module
    actual_compute = imported("pyarrow.compute")
    batch = pa.record_batch(
        {
            "key": pa.array([1] * 1_153, type=pa.int64()),
            "value": pa.array([1] * 1_153, type=pa.int64()),
        }
    )
    events: list[str] = []

    def open_batches() -> Iterator[pa.RecordBatch]:
        events.append("open")
        return _TrackedArrowBatches((batch,), events)

    descriptor = ArrowBatchSource(open_batches, "csv", 65_536)

    class FailingCompute:
        @staticmethod
        def unique(_values: object) -> object:
            raise MemoryError("file group allocation")

        def __getattr__(self, name: str) -> object:
            return getattr(actual_compute, name)

    def import_backend(name: str) -> object:
        return FailingCompute() if name == "pyarrow.compute" else imported(name)

    def forbidden_rows() -> Iterator[dict[str, int]]:
        raise AssertionError("MemoryError must not reopen the file row source")
        yield

    source = Source(
        forbidden_rows,
        SourceCapabilities(reiterable=True, exact_size=None),
        native_data=descriptor,
    )
    grouped = (
        fpstreams.Rows(fpstreams.Flow(source))
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
    )
    monkeypatch.setattr(relational_execution, "import_module", import_backend)

    with pytest.raises(MemoryError, match="file group allocation"):
        grouped.to_list()
    assert events == ["open", "pull:0", "close"]


@pytest.mark.parametrize("storage", ["csv", "parquet"])
def test_file_arrow_group_sum_preserves_empty_and_missing_selector_timing(
    tmp_path: Path,
    storage: str,
) -> None:
    """Empty files skip missing fields; nonempty files raise the canonical SelectionError."""

    def source_for(path: Path) -> fpstreams.Rows[dict[str, object]]:
        if storage == "csv":
            return fpstreams.rows.scan_csv(path, batch_size=1)
        return fpstreams.rows.from_parquet(path, batch_size=1)

    empty = tmp_path / f"empty-missing.{storage}"
    populated = tmp_path / f"populated-missing.{storage}"
    if storage == "csv":
        empty.write_text("present\n", encoding="utf-8")
        populated.write_text("present\n1\n", encoding="utf-8")
    else:
        pq.write_table(pa.table({"present": pa.array([], type=pa.int64())}), empty)
        pq.write_table(pa.table({"present": [1]}), populated)

    assert (
        source_for(empty)
        .group_by("missing")
        .aggregate(total=fpstreams.agg.sum("also_missing"))
        .to_list()
        == []
    )

    def execute(engine: str) -> list[dict[str, object]]:
        source = source_for(populated)
        selected = source.with_engine("python") if engine == "python" else source
        return (
            selected.group_by("missing")
            .aggregate(total=fpstreams.agg.sum("also_missing"))
            .to_list()
        )

    with pytest.raises(fpstreams.SelectionError) as canonical:
        execute("python")
    with pytest.raises(fpstreams.SelectionError) as automatic:
        execute("auto")
    assert str(automatic.value) == str(canonical.value)
    assert automatic.value.__cause__.args == canonical.value.__cause__.args  # type: ignore[union-attr]


def test_file_arrow_group_sum_active_failpoint_uses_the_canonical_row_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Instrumentation bypasses the file specialization before it can claim a native stream."""
    from fpstreams.execution import relational as relational_execution
    from fpstreams.runtime.failpoints import failpoint
    from fpstreams.tabular import arrow as arrow_adapter

    target = tmp_path / "instrumented-group.csv"
    target.write_text("key,value\n1,2\n1,3\n", encoding="utf-8")
    converted: list[int] = []
    convert_rows = arrow_adapter.batch_to_rows

    def tracked(batch: object) -> list[dict[str, object]]:
        converted.append(batch.num_rows)  # type: ignore[attr-defined]
        return convert_rows(batch)

    def forbidden_native(*_arguments: object, **_options: object) -> object:
        raise AssertionError("an active failpoint must bypass file Arrow grouping")

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", tracked)
    monkeypatch.setattr(relational_execution, "_try_arrow_file_group_sum", forbidden_native)
    grouped = (
        fpstreams.rows.scan_csv(target, batch_size=1)
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
    )

    with failpoint("unrelated.transition", RuntimeError("unused")):
        assert grouped.to_list() == [{"key": 1, "total": 5}]
    assert converted == [1, 1]


def test_file_arrow_group_sum_tiny_csv_declines_before_native_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A tiny default CSV avoids both the schema probe and native kernel dispatch."""
    from fpstreams.execution import relational as relational_execution
    from fpstreams.tabular import arrow as arrow_adapter

    target = tmp_path / "tiny-group.csv"
    target.write_text("key,value,unused\n1,2,a\n1,3,b\n", encoding="utf-8")
    converted: list[int] = []
    convert_rows = arrow_adapter.batch_to_rows

    def tracked(batch: object) -> list[dict[str, object]]:
        converted.append(batch.num_rows)  # type: ignore[attr-defined]
        return convert_rows(batch)

    def forbidden_native(*_arguments: object, **_options: object) -> object:
        raise AssertionError("tiny CSV grouping must decline before native file execution")

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", tracked)
    monkeypatch.setattr(relational_execution, "_try_arrow_file_group_sum", forbidden_native)
    result = (
        fpstreams.rows.scan_csv(target, batch_size=1)
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
        .to_list()
    )

    assert result == [{"key": 1, "total": 5}]
    assert converted == [1, 1]


@pytest.mark.parametrize("kind", ["csv", "parquet"])
def test_file_arrow_group_sum_small_inputs_skip_arrow_kernel_setup(
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    """Small files fold the already-opened projected stream without hash-kernel overhead."""
    from fpstreams.execution import relational as relational_execution
    from fpstreams.planning.arrow_source import ArrowBatchSource
    from fpstreams.planning.source import Source, SourceCapabilities

    batch = pa.record_batch(
        {
            "key": pa.array((index % 8 for index in range(300)), type=pa.int64()),
            "value": pa.array(range(300), type=pa.int64()),
        }
    )
    events: list[str] = []

    def open_batches() -> Iterator[pa.RecordBatch]:
        events.append("open")
        return _TrackedArrowBatches((batch,), events)

    descriptor = ArrowBatchSource(open_batches, kind, 65_536)

    def forbidden_rows() -> Iterator[dict[str, int]]:
        raise AssertionError("small guarded file grouping must not reopen Python rows")
        yield

    source = Source(
        forbidden_rows,
        SourceCapabilities(reiterable=True, exact_size=None),
        native_data=descriptor,
    )
    grouped = (
        fpstreams.Rows(fpstreams.Flow(source))
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
    )

    def forbidden_kernel(*_arguments: object, **_options: object) -> object:
        raise AssertionError("small file grouping must stay below the Arrow crossover")

    monkeypatch.setattr(relational_execution, "_arrow_group_batch_totals", forbidden_kernel)

    assert grouped.to_list() == [{"key": key, "total": sum(range(key, 300, 8))} for key in range(8)]
    assert events == ["open", "pull:0", "stop", "close"]


def test_file_arrow_group_sum_many_tiny_batches_skip_arrow_kernel_setup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A large row count cannot make a one-row batch pay one hash kernel per row."""
    from fpstreams.execution import relational as relational_execution
    from fpstreams.planning.arrow_source import ArrowBatchSource
    from fpstreams.planning.source import Source, SourceCapabilities

    unit = pa.record_batch({"key": [1], "value": [1]})
    events: list[str] = []

    def open_batches() -> Iterator[pa.RecordBatch]:
        events.append("open")
        return _TrackedArrowBatches((unit,) * 1_025, events)

    descriptor = ArrowBatchSource(open_batches, "parquet", 1)

    def forbidden_rows() -> Iterator[dict[str, int]]:
        raise AssertionError("tiny-batch file grouping must stay on its native stream")
        yield

    source = Source(
        forbidden_rows,
        SourceCapabilities(reiterable=True, exact_size=None),
        native_data=descriptor,
    )

    def forbidden_kernel(*_arguments: object, **_options: object) -> object:
        raise AssertionError("one-row batches must not dispatch an Arrow hash kernel")

    monkeypatch.setattr(relational_execution, "_arrow_group_batch_totals", forbidden_kernel)
    result = (
        fpstreams.Rows(fpstreams.Flow(source))
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
        .to_list()
    )

    assert result == [{"key": 1, "total": 1_025}]
    assert events[0:2] == ["open", "pull:0"]
    assert events[-2:] == ["stop", "close"]


def test_arrow_reader_group_sum_merges_batches_without_boxing_and_closes() -> None:
    """A one-shot reader keeps global first-seen order and closes its batch iterator."""
    from fpstreams.planning.arrow_source import ArrowBatchSource
    from fpstreams.planning.source import Source, SourceCapabilities

    batches = (
        pa.record_batch({"key": [2, 1, 2], "value": [5, 7, 11]}),
        pa.record_batch({"key": [3, 1, 4], "value": [-4, 2, 8]}),
    )
    events: list[str] = []
    opened = _TrackedArrowBatches(batches, events)
    descriptor = ArrowBatchSource(
        lambda: opened,
        "reader",
        65_536,
        batches[0].schema,
        False,
    )

    def forbidden_rows() -> Iterator[dict[str, int]]:
        raise AssertionError("guarded reader grouping must not box rows")
        yield

    source = Source(
        forbidden_rows,
        SourceCapabilities(reiterable=False, exact_size=None),
        native_data=descriptor,
    )
    grouped = (
        fpstreams.Rows(fpstreams.Flow(source))
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
    )

    assert grouped.to_list() == [
        {"key": 2, "total": 16},
        {"key": 1, "total": 9},
        {"key": 3, "total": -4},
        {"key": 4, "total": 8},
    ]
    assert events == ["pull:0", "pull:1", "stop", "close"]
    with pytest.raises(fpstreams.FlowConsumedError):
        grouped.to_list()


def test_arrow_reader_group_multi_merges_batches_without_boxing_and_closes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Closed reader lanes merge exact partials in global first-seen order."""
    from fpstreams.execution import relational as relational_execution
    from fpstreams.physical.relational import ArrowGroupAggregateSpec, GroupAggregatePhysicalNode
    from fpstreams.planning.arrow_source import ArrowBatchSource
    from fpstreams.planning.compiler import compile_query
    from fpstreams.planning.source import Source, SourceCapabilities
    from fpstreams.tabular import arrow as arrow_adapter

    monkeypatch.setattr(relational_execution, "_ARROW_READER_GROUP_MULTI_MIN_ROWS", 0)
    maximum = 2**63 - 1
    batches = (
        pa.record_batch(
            {
                "key": [2, 1, 2],
                "value": [maximum, 7, 11],
                "other": [5, 9, 4],
            }
        ),
        pa.record_batch(
            {
                "key": [3, 1, 4, 2],
                "value": [-4, 2, 8, maximum],
                "other": [3, 12, 1, 6],
            }
        ),
    )
    events: list[str] = []
    opened = _TrackedArrowBatches(batches, events)
    descriptor = ArrowBatchSource(
        lambda: opened,
        "reader",
        65_536,
        batches[0].schema,
        False,
    )

    def forbidden_rows() -> Iterator[dict[str, int]]:
        raise AssertionError("guarded reader multi-group must not open Python rows")
        yield

    def forbidden_batch_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("guarded reader multi-group must not box Arrow rows")

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", forbidden_batch_rows)
    source = Source(
        forbidden_rows,
        SourceCapabilities(reiterable=False, exact_size=None),
        native_data=descriptor,
    )
    grouped = (
        fpstreams.Rows(fpstreams.Flow(source))
        .group_by("key")
        .aggregate(
            rows=fpstreams.agg.count(),
            total=fpstreams.agg.sum("value"),
            low=fpstreams.agg.min("value"),
            high=fpstreams.agg.max("other"),
        )
    )
    physical = compile_query(grouped._flow._query("list"))
    assert isinstance(physical.root, GroupAggregatePhysicalNode)
    assert isinstance(physical.root.arrow_i64_sum, ArrowGroupAggregateSpec)

    assert grouped.to_list() == [
        {"key": 2, "rows": 3, "total": maximum * 2 + 11, "low": 11, "high": 6},
        {"key": 1, "rows": 2, "total": 9, "low": 2, "high": 12},
        {"key": 3, "rows": 1, "total": -4, "low": -4, "high": 3},
        {"key": 4, "rows": 1, "total": 8, "low": 8, "high": 1},
    ]
    assert events == ["pull:0", "pull:1", "stop", "close"]
    with pytest.raises(fpstreams.FlowConsumedError):
        grouped.to_list()


def test_arrow_reader_group_multi_null_stops_before_later_batches_and_closes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A claimed nullable batch keeps row-major lane errors and stops pulling."""
    from fpstreams.execution import relational as relational_execution
    from fpstreams.planning.arrow_source import ArrowBatchSource
    from fpstreams.planning.source import Source, SourceCapabilities
    from fpstreams.tabular import arrow as arrow_adapter

    monkeypatch.setattr(relational_execution, "_ARROW_READER_GROUP_MULTI_MIN_ROWS", 0)
    batches = (
        pa.record_batch({"key": [1, 2], "value": [2, 3]}),
        pa.record_batch({"key": [1, 1], "value": pa.array([4, None], type=pa.int64())}),
        pa.record_batch({"key": [1], "value": [99]}),
    )
    events: list[str] = []
    opened = _TrackedArrowBatches(batches, events)
    descriptor = ArrowBatchSource(
        lambda: opened,
        "reader",
        65_536,
        batches[0].schema,
        False,
    )

    def rows() -> Iterator[dict[str, object]]:
        iterator = descriptor.open_batches()
        try:
            for batch in iterator:
                yield from arrow_adapter.batch_to_rows(batch)
        finally:
            iterator.close()

    source = Source(
        rows,
        SourceCapabilities(reiterable=False, exact_size=None),
        native_data=descriptor,
    )
    grouped = (
        fpstreams.Rows(fpstreams.Flow(source))
        .group_by("key")
        .aggregate(
            rows=fpstreams.agg.count(),
            total=fpstreams.agg.sum("value"),
            low=fpstreams.agg.min("value"),
        )
    )

    def forbidden_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("nullable reader multi-group must stay inside its claimed batch")

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", forbidden_rows)
    with pytest.raises(TypeError, match="unsupported operand type"):
        grouped.to_list()
    assert events == ["pull:0", "pull:1", "close"]
    with pytest.raises(fpstreams.FlowConsumedError):
        grouped.to_list()


def test_arrow_reader_group_multi_compute_decline_continues_from_claimed_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A recoverable multi-lane kernel decline switches the same reader to scalar folding."""
    from fpstreams.execution import relational as relational_execution
    from fpstreams.tabular import arrow as arrow_adapter

    monkeypatch.setattr(relational_execution, "_ARROW_READER_GROUP_MULTI_MIN_ROWS", 0)
    batches = (
        pa.record_batch({"key": [2, 1, 2], "value": [5, 7, 11]}),
        pa.record_batch({"key": [3, 1], "value": [-4, 2]}),
        pa.record_batch({"key": [4, 2], "value": [8, 3]}),
    )
    reader = pa.RecordBatchReader.from_batches(batches[0].schema, batches)
    original = relational_execution._try_arrow_retained_group_aggregate
    calls = 0

    def decline_second(*args: object, **kwargs: object) -> object:
        nonlocal calls
        calls += 1
        if calls == 2:
            return None
        return original(*args, **kwargs)

    def forbidden_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("claimed reader decline must not reopen Python rows")

    monkeypatch.setattr(
        relational_execution,
        "_try_arrow_retained_group_aggregate",
        decline_second,
    )
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", forbidden_rows)
    grouped = (
        fpstreams.rows.from_arrow(reader)
        .group_by("key")
        .aggregate(
            rows=fpstreams.agg.count(),
            total=fpstreams.agg.sum("value"),
            low=fpstreams.agg.min("value"),
            high=fpstreams.agg.max("value"),
        )
    )

    assert grouped.to_list() == [
        {"key": 2, "rows": 3, "total": 19, "low": 3, "high": 11},
        {"key": 1, "rows": 2, "total": 9, "low": 2, "high": 7},
        {"key": 3, "rows": 1, "total": -4, "low": -4, "high": -4},
        {"key": 4, "rows": 1, "total": 8, "low": 8, "high": 8},
    ]
    assert calls == 2


def test_arrow_reader_group_multi_high_cardinality_resumes_canonical_claimed_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A costly partial-group shape falls back without reopening its one-shot source."""
    from fpstreams.execution import relational as relational_execution
    from fpstreams.planning.arrow_source import ArrowBatchSource
    from fpstreams.planning.source import Source, SourceCapabilities
    from fpstreams.tabular import arrow as arrow_adapter

    monkeypatch.setattr(relational_execution, "_ARROW_READER_GROUP_MULTI_MIN_ROWS", 0)
    monkeypatch.setattr(relational_execution, "_ARROW_READER_GROUP_CARDINALITY_SAMPLE_ROWS", 1)
    monkeypatch.setattr(relational_execution, "_ARROW_READER_GROUP_MAX_DISTINCT_RATIO", 0.0)
    batches = (
        pa.record_batch({"key": [3, 1, 2], "value": [5, 7, 11]}),
        pa.record_batch({"key": [4, 1], "value": [-4, 2]}),
    )
    events: list[str] = []
    opened = _TrackedArrowBatches(batches, events)
    descriptor = ArrowBatchSource(
        lambda: opened,
        "reader",
        65_536,
        batches[0].schema,
        False,
    )
    converted: list[int] = []
    convert_rows = arrow_adapter.batch_to_rows

    def tracked(batch: object) -> list[dict[str, object]]:
        converted.append(batch.num_rows)  # type: ignore[attr-defined]
        return convert_rows(batch)

    def forbidden_partial(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("high-cardinality reader must skip Arrow partial grouping")

    def forbidden_rows() -> Iterator[dict[str, int]]:
        raise AssertionError("claimed reader fallback must not reopen its source")
        yield

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", tracked)
    monkeypatch.setattr(
        relational_execution,
        "_try_arrow_retained_group_aggregate",
        forbidden_partial,
    )
    source = Source(
        forbidden_rows,
        SourceCapabilities(reiterable=False, exact_size=None),
        native_data=descriptor,
    )
    grouped = (
        fpstreams.Rows(fpstreams.Flow(source))
        .group_by("key")
        .aggregate(
            rows=fpstreams.agg.count(),
            total=fpstreams.agg.sum("value"),
            low=fpstreams.agg.min("value"),
        )
    )

    assert grouped.to_list() == [
        {"key": 3, "rows": 1, "total": 5, "low": 5},
        {"key": 1, "rows": 2, "total": 9, "low": 2},
        {"key": 2, "rows": 1, "total": 11, "low": 11},
        {"key": 4, "rows": 1, "total": -4, "low": -4},
    ]
    assert converted == [3, 2]
    assert events == ["pull:0", "pull:1", "stop", "close"]


def test_arrow_reader_group_multi_unsupported_schema_falls_back_before_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-int64 value lane is rejected before the one-shot reader is opened natively."""
    from fpstreams.physical.relational import GroupAggregatePhysicalNode
    from fpstreams.planning.compiler import compile_query
    from fpstreams.tabular import arrow as arrow_adapter

    batch = pa.record_batch(
        {
            "key": pa.array([1, 1], type=pa.int64()),
            "value": pa.array([2, 3], type=pa.int32()),
        }
    )
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    converted: list[int] = []
    convert_rows = arrow_adapter.batch_to_rows

    def tracked(current: object) -> list[dict[str, object]]:
        converted.append(current.num_rows)  # type: ignore[attr-defined]
        return convert_rows(current)

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", tracked)
    grouped = (
        fpstreams.rows.from_arrow(reader)
        .group_by("key")
        .aggregate(
            rows=fpstreams.agg.count(),
            total=fpstreams.agg.sum("value"),
            low=fpstreams.agg.min("value"),
        )
    )
    physical = compile_query(grouped._flow._query("list"))
    assert isinstance(physical.root, GroupAggregatePhysicalNode)
    assert physical.root.arrow_i64_sum is None

    assert grouped.to_list() == [{"key": 1, "rows": 2, "total": 5, "low": 2}]
    assert converted == [2]


def test_arrow_reader_group_sum_preserves_cross_batch_python_bigints(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Batch-local Arrow sums merge into unbounded Python integers."""
    from fpstreams.tabular import arrow as arrow_adapter

    maximum = 2**63 - 1
    batches = (
        pa.record_batch({"key": [1, 1, 2], "value": [maximum, 1, -(2**63)]}),
        pa.record_batch({"key": [1, 2], "value": [maximum, -1]}),
    )
    reader = pa.RecordBatchReader.from_batches(batches[0].schema, batches)

    def forbidden_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("overflow-safe reader grouping must remain columnar")

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", forbidden_rows)
    grouped = (
        fpstreams.rows.from_arrow(reader)
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
    )

    assert grouped.to_list() == [
        {"key": 1, "total": maximum * 2 + 1},
        {"key": 2, "total": -(2**63) - 1},
    ]
    with pytest.raises(fpstreams.FlowConsumedError):
        grouped.to_list()


def test_arrow_reader_group_sum_null_stops_before_later_batches_and_closes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The first null sum value raises canonically without pulling another batch."""
    from fpstreams.planning.arrow_source import ArrowBatchSource
    from fpstreams.planning.source import Source, SourceCapabilities
    from fpstreams.tabular import arrow as arrow_adapter

    batches = (
        pa.record_batch({"key": [1, 2], "value": [2, 3]}),
        pa.record_batch({"key": [1, 1], "value": pa.array([4, None], type=pa.int64())}),
        pa.record_batch({"key": [1], "value": [99]}),
    )
    events: list[str] = []
    opened = _TrackedArrowBatches(batches, events)
    descriptor = ArrowBatchSource(
        lambda: opened,
        "reader",
        65_536,
        batches[0].schema,
        False,
    )

    def rows() -> Iterator[dict[str, object]]:
        iterator = descriptor.open_batches()
        try:
            for batch in iterator:
                yield from arrow_adapter.batch_to_rows(batch)
        finally:
            iterator.close()

    source = Source(
        rows,
        SourceCapabilities(reiterable=False, exact_size=None),
        native_data=descriptor,
    )
    grouped = (
        fpstreams.Rows(fpstreams.Flow(source))
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
    )

    def forbidden_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("guarded reader grouping must not box null rows")

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", forbidden_rows)
    with pytest.raises(TypeError, match="unsupported operand type"):
        grouped.to_list()
    assert events == ["pull:0", "pull:1", "close"]
    with pytest.raises(fpstreams.FlowConsumedError):
        grouped.to_list()


def test_arrow_reader_group_sum_compute_decline_continues_from_claimed_batches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A recoverable Arrow kernel decline falls back inside the claimed reader."""
    from fpstreams.execution import relational as relational_execution
    from fpstreams.tabular import arrow as arrow_adapter

    imported = relational_execution.import_module
    actual_compute = imported("pyarrow.compute")
    monkeypatch.setattr(relational_execution, "_ARROW_GROUP_BATCH_SCALAR_MAX_ROWS", 0)
    batches = (
        pa.record_batch({"key": [2, 1, 2], "value": [5, 7, 11]}),
        pa.record_batch({"key": [3, 1], "value": [-4, 2]}),
        pa.record_batch({"key": [4, 2], "value": [8, 3]}),
    )
    reader = pa.RecordBatchReader.from_batches(batches[0].schema, batches)
    unique_calls = 0

    class RejectingCompute:
        @staticmethod
        def unique(values: object) -> object:
            nonlocal unique_calls
            unique_calls += 1
            if unique_calls == 2:
                raise NotImplementedError("incremental group decline")
            return actual_compute.unique(values)

        def __getattr__(self, name: str) -> object:
            return getattr(actual_compute, name)

    def import_backend(name: str) -> object:
        return RejectingCompute() if name == "pyarrow.compute" else imported(name)

    def forbidden_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("claimed reader fallback must not reopen Python rows")

    monkeypatch.setattr(relational_execution, "import_module", import_backend)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", forbidden_rows)
    grouped = (
        fpstreams.rows.from_arrow(reader)
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
    )

    assert grouped.to_list() == [
        {"key": 2, "total": 19},
        {"key": 1, "total": 9},
        {"key": 3, "total": -4},
        {"key": 4, "total": 8},
    ]
    assert unique_calls == 2
    with pytest.raises(fpstreams.FlowConsumedError):
        grouped.to_list()


def test_arrow_reader_group_sum_memory_error_closes_without_reopening(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Allocation failure propagates from the claimed batch and still closes ownership."""
    from fpstreams.execution import relational as relational_execution
    from fpstreams.planning.arrow_source import ArrowBatchSource
    from fpstreams.planning.source import Source, SourceCapabilities

    imported = relational_execution.import_module
    actual_compute = imported("pyarrow.compute")
    monkeypatch.setattr(relational_execution, "_ARROW_GROUP_BATCH_SCALAR_MAX_ROWS", 0)
    batch = pa.record_batch({"key": [1, 1], "value": [2, 3]})
    events: list[str] = []
    opened = _TrackedArrowBatches((batch,), events)
    descriptor = ArrowBatchSource(
        lambda: opened,
        "reader",
        65_536,
        batch.schema,
        False,
    )

    class FailingCompute:
        @staticmethod
        def unique(_values: object) -> object:
            raise MemoryError("reader group allocation")

        def __getattr__(self, name: str) -> object:
            return getattr(actual_compute, name)

    def import_backend(name: str) -> object:
        return FailingCompute() if name == "pyarrow.compute" else imported(name)

    def forbidden_rows() -> Iterator[dict[str, int]]:
        raise AssertionError("MemoryError must not reopen the claimed row source")
        yield

    source = Source(
        forbidden_rows,
        SourceCapabilities(reiterable=False, exact_size=None),
        native_data=descriptor,
    )
    grouped = (
        fpstreams.Rows(fpstreams.Flow(source))
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
    )
    monkeypatch.setattr(relational_execution, "import_module", import_backend)

    with pytest.raises(MemoryError, match="reader group allocation"):
        grouped.to_list()
    assert events == ["pull:0", "close"]
    with pytest.raises(fpstreams.FlowConsumedError):
        grouped.to_list()


@pytest.mark.parametrize(
    ("key_type", "first_keys", "second_keys", "expected"),
    [
        (
            pa.null(),
            [None, None],
            [None],
            [{"key": None, "total": 6}],
        ),
        (
            pa.string(),
            ["b", "a", "b"],
            ["c", "a"],
            [
                {"key": "b", "total": 4},
                {"key": "a", "total": 7},
                {"key": "c", "total": 4},
            ],
        ),
    ],
)
def test_arrow_reader_group_sum_preserves_supported_key_values_and_order(
    monkeypatch: pytest.MonkeyPatch,
    key_type: pa.DataType,
    first_keys: list[object],
    second_keys: list[object],
    expected: list[dict[str, object]],
) -> None:
    """Supported nullable and textual scalars merge by Python value and first ordinal."""
    from fpstreams.tabular import arrow as arrow_adapter

    split = len(first_keys)
    batches = (
        pa.record_batch(
            {
                "key": pa.array(first_keys, key_type),
                "value": pa.array(range(1, split + 1), pa.int64()),
            }
        ),
        pa.record_batch(
            {
                "key": pa.array(second_keys, key_type),
                "value": pa.array(range(split + 1, split + len(second_keys) + 1), pa.int64()),
            }
        ),
    )
    reader = pa.RecordBatchReader.from_batches(batches[0].schema, batches)

    def forbidden_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("supported reader keys must not materialize row dictionaries")

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", forbidden_rows)
    result = (
        fpstreams.rows.from_arrow(reader)
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
        .to_list()
    )

    assert result == expected


def test_arrow_reader_group_sum_uses_true_first_seen_order_not_hash_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stable Arrow hash order is not necessarily Python's insertion order."""
    from fpstreams.execution import relational as relational_execution
    from fpstreams.tabular import arrow as arrow_adapter

    monkeypatch.setattr(relational_execution, "_ARROW_GROUP_BATCH_SCALAR_MAX_ROWS", 0)

    maximum = 2**63 - 1
    keys = [0, 1, 1, 1, maximum, -1, -2, maximum, maximum, -2, maximum, 1, 2, 0, 1]
    batch = pa.record_batch({"key": keys, "value": [1] * len(keys)})
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])

    def forbidden_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("first-seen reader grouping must stay columnar")

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", forbidden_rows)
    result = (
        fpstreams.rows.from_arrow(reader)
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
        .to_list()
    )

    assert result == [
        {"key": 0, "total": 2},
        {"key": 1, "total": 5},
        {"key": maximum, "total": 4},
        {"key": -1, "total": 1},
        {"key": -2, "total": 2},
        {"key": 2, "total": 1},
    ]


def test_arrow_reader_group_sum_empty_missing_selectors_stay_empty() -> None:
    """A missing field is never inspected when a one-shot reader has no rows."""
    from fpstreams.physical.relational import GroupAggregatePhysicalNode
    from fpstreams.planning.compiler import compile_query

    batch = pa.record_batch({"present": pa.array([], pa.int64())})
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    grouped = (
        fpstreams.rows.from_arrow(reader)
        .group_by("missing")
        .aggregate(total=fpstreams.agg.sum("also_missing"))
    )
    physical = compile_query(grouped._flow._query("list"))
    assert isinstance(physical.root, GroupAggregatePhysicalNode)
    assert physical.root.arrow_i64_sum is None

    assert grouped.to_list() == []
    with pytest.raises(fpstreams.FlowConsumedError):
        grouped.to_list()


def test_arrow_reader_group_sum_unsupported_shapes_fall_back_before_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing, float, dictionary, and non-int64 schemas retain row execution."""
    from fpstreams.physical.relational import GroupAggregatePhysicalNode
    from fpstreams.planning.compiler import compile_query
    from fpstreams.tabular import arrow as arrow_adapter

    converted: list[int] = []
    convert_rows = arrow_adapter.batch_to_rows

    def tracked(batch: object) -> list[dict[str, object]]:
        converted.append(batch.num_rows)  # type: ignore[attr-defined]
        return convert_rows(batch)

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", tracked)
    dictionary = pa.DictionaryArray.from_arrays(pa.array([0, 1]), pa.array(["a", "b"]))
    inputs = (
        pa.record_batch({"present": [1]}),
        pa.record_batch({"key": [1.5, 2.5], "value": [1, 2]}),
        pa.record_batch({"key": dictionary, "value": [1, 2]}),
        pa.record_batch(
            {"key": pa.array([1, 2], pa.int64()), "value": pa.array([1, 2], pa.int32())}
        ),
    )
    tasks = []
    for batch in inputs:
        reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
        task = (
            fpstreams.rows.from_arrow(reader)
            .group_by("key")
            .aggregate(total=fpstreams.agg.sum("value"))
        )
        physical = compile_query(task._flow._query("list"))
        assert isinstance(physical.root, GroupAggregatePhysicalNode)
        assert physical.root.arrow_i64_sum is None
        tasks.append(task)

    with pytest.raises(fpstreams.SelectionError) as missing:
        tasks[0].to_list()
    assert isinstance(missing.value.__cause__, KeyError)
    float_result = tasks[1].to_list()
    assert float_result == [{"key": 1.5, "total": 1}, {"key": 2.5, "total": 2}]
    assert tasks[2].to_list() == [{"key": "a", "total": 1}, {"key": "b", "total": 2}]
    assert tasks[3].to_list() == [{"key": 1, "total": 1}, {"key": 2, "total": 2}]
    assert converted == [1, 2, 2, 2]


def test_arrow_reader_group_sum_failpoint_and_forced_python_keep_row_boundaries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Instrumentation and explicit engine choice bypass the reader specialization."""
    from fpstreams.runtime.failpoints import failpoint
    from fpstreams.tabular import arrow as arrow_adapter

    converted: list[int] = []
    convert_rows = arrow_adapter.batch_to_rows

    def tracked(batch: object) -> list[dict[str, object]]:
        converted.append(batch.num_rows)  # type: ignore[attr-defined]
        return convert_rows(batch)

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", tracked)
    batch = pa.record_batch({"key": [1, 1], "value": [2, 3]})
    forced_reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    forced = (
        fpstreams.rows.from_arrow(forced_reader)
        .with_engine("python")
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
    )
    assert forced.to_list() == [{"key": 1, "total": 5}]

    instrumented_reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    instrumented = (
        fpstreams.rows.from_arrow(instrumented_reader)
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
    )
    with (
        failpoint("arrow.batch.after", RuntimeError("reader row boundary")),
        pytest.raises(RuntimeError, match="reader row boundary"),
    ):
        instrumented.to_list()
    assert converted == [2, 2]


@pytest.mark.parametrize("adapter", ["dataframe", "polars"])
def test_eager_columnar_frames_are_visible_to_the_arrow_group_sum_planner(adapter: str) -> None:
    """Reusable eager dataframe batches should earn the guarded columnar group marker."""
    from fpstreams.physical.relational import ArrowGroupSumSpec, GroupAggregatePhysicalNode
    from fpstreams.planning.compiler import compile_query

    if adapter == "dataframe":
        source = fpstreams.rows.from_dataframe(pd.DataFrame({"key": [1], "value": [2]}))
    else:
        source = fpstreams.rows.from_polars(pl.DataFrame({"key": [1], "value": [2]}))
    grouped = source.group_by("key").aggregate(total=fpstreams.agg.sum("value"))

    physical = compile_query(grouped._flow._query("list"))

    assert isinstance(physical.root, GroupAggregatePhysicalNode)
    assert physical.root.arrow_i64_sum == ArrowGroupSumSpec("key", "value", "total")


def test_lazy_polars_group_sum_does_not_claim_an_eager_table_opener() -> None:
    """A LazyFrame keeps its batch collector until it has a genuine whole-table opener."""
    from fpstreams.physical.relational import GroupAggregatePhysicalNode
    from fpstreams.planning.compiler import compile_query

    grouped = (
        fpstreams.rows.from_polars(pl.DataFrame({"key": [1], "value": [2]}).lazy())
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
    )

    physical = compile_query(grouped._flow._query("list"))

    assert isinstance(physical.root, GroupAggregatePhysicalNode)
    assert physical.root.arrow_i64_sum is None


@pytest.mark.parametrize("adapter", ["dataframe", "polars"])
def test_eager_columnar_frame_group_sum_never_opens_python_rows(
    monkeypatch: pytest.MonkeyPatch, adapter: str
) -> None:
    """Even batch_size=1 must aggregate an eager frame without boxing its input records."""
    from fpstreams.physical.relational import GroupAggregatePhysicalNode, SourcePhysicalNode
    from fpstreams.planning.compiler import compile_query
    from fpstreams.planning.source import Source

    data = {"key": [3, 1, 3, 2, 1], "value": [5, 7, 11, -4, 2]}
    if adapter == "dataframe":
        source = fpstreams.rows.from_dataframe(pd.DataFrame(data), batch_size=1)
    else:
        source = fpstreams.rows.from_polars(pl.DataFrame(data), batch_size=1)
    grouped = source.group_by("key").aggregate(total=fpstreams.agg.sum("value"))
    physical = compile_query(grouped._flow._query("list"))
    assert isinstance(physical.root, GroupAggregatePhysicalNode)
    assert isinstance(physical.root.input, SourcePhysicalNode)
    input_source = physical.root.input.source
    open_source = Source.open

    def reject_row_source(candidate: Source[object]) -> Iterator[object]:
        if candidate is input_source:
            raise AssertionError("columnar frame grouping must not open Python rows")
        return open_source(candidate)

    monkeypatch.setattr(Source, "open", reject_row_source)

    assert grouped.to_list() == [
        {"key": 3, "total": 16},
        {"key": 1, "total": 9},
        {"key": 2, "total": -4},
    ]


@pytest.mark.parametrize("adapter", ["dataframe", "polars"])
def test_eager_columnar_group_decline_reuses_its_canonical_table(
    monkeypatch: pytest.MonkeyPatch, adapter: str
) -> None:
    """A safe table conversion should not be repeated after a group-kernel decline."""
    from fpstreams.planning.source import Source

    data = {"key": [1.5, 2.5, 1.5], "value": [2, 3, 4]}
    source = (
        fpstreams.rows.from_dataframe(pd.DataFrame(data))
        if adapter == "dataframe"
        else fpstreams.rows.from_polars(pl.DataFrame(data))
    )
    input_source = source._flow._pipeline.source
    grouped = source.group_by("key").aggregate(rows=fpstreams.agg.count())
    open_source = Source.open

    def reject_second_conversion(candidate: Source[object]) -> Iterator[object]:
        if candidate is input_source:
            raise AssertionError("declined eager group reopened its converted source")
        return open_source(candidate)

    monkeypatch.setattr(Source, "open", reject_second_conversion)

    assert grouped.to_list() == [{"key": 1.5, "rows": 2}, {"key": 2.5, "rows": 1}]


@pytest.mark.parametrize("adapter", ["dataframe", "polars"])
def test_eager_columnar_global_decline_reuses_its_selected_table(
    monkeypatch: pytest.MonkeyPatch, adapter: str
) -> None:
    """A non-i64 reduction should not reconvert unrelated eager-frame columns."""
    from fpstreams.planning.source import Source

    data = {"value": [1.5, 2.5, -1.0], "unused": ["a", "b", "c"]}
    source = (
        fpstreams.rows.from_dataframe(pd.DataFrame(data), batch_size=2)
        if adapter == "dataframe"
        else fpstreams.rows.from_polars(pl.DataFrame(data), batch_size=2)
    )
    input_source = source._flow._pipeline.source
    aggregated = source.aggregate(total=fpstreams.agg.sum("value"))
    open_source = Source.open

    def reject_second_conversion(candidate: Source[object]) -> Iterator[object]:
        if candidate is input_source:
            raise AssertionError("declined eager reduction reopened its converted source")
        return open_source(candidate)

    monkeypatch.setattr(Source, "open", reject_second_conversion)

    assert aggregated.to_list() == [{"total": 3.0}]


def test_eager_columnar_global_decline_preserves_nested_conversion_errors() -> None:
    """Selected-column reuse must decline when an unused nested row cannot convert."""
    invalid_text = pa.array([b"\xff"], type=pa.binary()).view(pa.string())
    nested = pa.ListArray.from_arrays(pa.array([0, 1], type=pa.int32()), invalid_text)
    frame = pd.DataFrame(
        {
            "value": [7.0],
            "unused": pd.Series(pd.arrays.ArrowExtensionArray(nested)),
        }
    )

    def run(engine: str) -> list[dict[str, object]]:
        values = fpstreams.rows.from_dataframe(frame)
        if engine == "python":
            values = values.with_engine("python")
        return values.aggregate(total=fpstreams.agg.sum("value")).to_list()

    with pytest.raises(UnicodeDecodeError) as canonical_error:
        run("python")
    with pytest.raises(UnicodeDecodeError) as automatic_error:
        run("auto")
    assert str(automatic_error.value) == str(canonical_error.value)


def test_eager_pandas_nested_global_decline_reuses_one_arrow_conversion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A valid nonprimitive pandas column must not be converted again after decline."""
    values = pd.arrays.ArrowExtensionArray(pa.array([[1], [2]], type=pa.list_(pa.int64())))
    frame = pd.DataFrame({"value": pd.Series(values)})
    array_type = type(values)
    arrow_array = array_type.__arrow_array__
    calls = 0

    def tracked(self: object, type: object = None) -> object:
        nonlocal calls
        calls += 1
        return arrow_array(self, type=type)

    monkeypatch.setattr(array_type, "__arrow_array__", tracked)

    result = (
        fpstreams.rows.from_dataframe(frame).aggregate(first=fpstreams.agg.first("value")).to_list()
    )

    assert result == [{"first": [1]}]
    assert calls == 1


def test_eager_pandas_missing_global_selector_reuses_one_arrow_conversion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A missing selector must fail against the first converted pandas snapshot."""
    values = pd.arrays.ArrowExtensionArray(pa.array([[1]], type=pa.list_(pa.int64())))
    frame = pd.DataFrame({"other": pd.Series(values)})
    array_type = type(values)
    arrow_array = array_type.__arrow_array__
    calls = 0

    def tracked(self: object, type: object = None) -> object:
        nonlocal calls
        calls += 1
        return arrow_array(self, type=type)

    monkeypatch.setattr(array_type, "__arrow_array__", tracked)
    aggregated = fpstreams.rows.from_dataframe(frame).aggregate(total=fpstreams.agg.sum("missing"))

    with pytest.raises(fpstreams.SelectionError):
        aggregated.to_list()
    assert calls == 1


@pytest.mark.parametrize("multi_lane", [False, True])
def test_eager_pandas_nested_group_decline_reuses_one_arrow_conversion(
    monkeypatch: pytest.MonkeyPatch,
    multi_lane: bool,
) -> None:
    """Both pandas group plans must retain the first converted table snapshot."""
    unused = pd.arrays.ArrowExtensionArray(pa.array([[1], [2]], type=pa.list_(pa.int64())))
    frame = pd.DataFrame(
        {
            "key": [1, 1],
            "value": [2, 3],
            "unused": pd.Series(unused),
        }
    )
    array_type = type(unused)
    arrow_array = array_type.__arrow_array__
    calls = 0

    def tracked(self: object, type: object = None) -> object:
        nonlocal calls
        calls += 1
        return arrow_array(self, type=type)

    monkeypatch.setattr(array_type, "__arrow_array__", tracked)
    grouped = fpstreams.rows.from_dataframe(frame).group_by("key")
    result = (
        grouped.aggregate(rows=fpstreams.agg.count(), total=fpstreams.agg.sum("value"))
        if multi_lane
        else grouped.aggregate(total=fpstreams.agg.sum("value"))
    ).to_list()

    expected = {"key": 1, "total": 5}
    if multi_lane:
        expected["rows"] = 2
    assert result == [expected]
    assert calls == 1


@pytest.mark.parametrize("multi_lane", [False, True])
def test_eager_pandas_post_guard_group_decline_reuses_one_arrow_conversion(
    monkeypatch: pytest.MonkeyPatch,
    multi_lane: bool,
) -> None:
    """A supported table snapshot remains canonical after a later group guard declines."""
    unused = pd.arrays.ArrowExtensionArray(pa.array([1, 2], type=pa.int64()))
    frame = pd.DataFrame(
        {
            "key": [1.5, 1.5],
            "value": [2, 3],
            "unused": pd.Series(unused),
        }
    )
    array_type = type(unused)
    arrow_array = array_type.__arrow_array__
    calls = 0

    def tracked(self: object, type: object = None) -> object:
        nonlocal calls
        calls += 1
        return arrow_array(self, type=type)

    monkeypatch.setattr(array_type, "__arrow_array__", tracked)
    grouped = fpstreams.rows.from_dataframe(frame).group_by("key")
    result = (
        grouped.aggregate(rows=fpstreams.agg.count(), total=fpstreams.agg.sum("value"))
        if multi_lane
        else grouped.aggregate(total=fpstreams.agg.sum("value"))
    ).to_list()

    expected = {"key": 1.5, "total": 5}
    if multi_lane:
        expected["rows"] = 2
    assert result == [expected]
    assert calls == 1


@pytest.mark.parametrize("adapter", ["dataframe", "polars"])
def test_eager_columnar_frame_group_sum_preserves_float_key_adapter_semantics(adapter: str) -> None:
    """Unsupported float keys retain each adapter's established NaN/null conversion."""
    from math import isnan

    data = {"key": [float("nan"), float("nan")], "value": [1, 2]}
    if adapter == "dataframe":
        source = fpstreams.rows.from_dataframe(pd.DataFrame(data))
    else:
        source = fpstreams.rows.from_polars(pl.DataFrame(data))

    result = source.group_by("key").aggregate(total=fpstreams.agg.sum("value")).to_list()

    if adapter == "dataframe":
        assert result == [{"key": None, "total": 3}]
    else:
        assert len(result) == 2
        assert all(isnan(row["key"]) for row in result)
        assert [row["total"] for row in result] == [1, 2]


@pytest.mark.parametrize("as_batch", [False, True])
def test_arrow_i64_group_sum_stays_columnar_and_preserves_first_seen_order(
    monkeypatch: pytest.MonkeyPatch, as_batch: bool
) -> None:
    """A supported Table or RecordBatch reaches Arrow aggregation without row boxing."""
    from fpstreams.tabular import arrow as arrow_adapter

    def reject_row_boxing(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("guarded Arrow grouping must not materialize input rows")

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", reject_row_boxing)
    table = pa.table({"key": [3, 1, 3, 2, 1], "value": [5, 7, 11, -4, 2]})
    source = table.to_batches()[0] if as_batch else table

    assert (
        fpstreams.rows.from_arrow(source, batch_size=2)
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
        .to_list()
    ) == [
        {"key": 3, "total": 16},
        {"key": 1, "total": 9},
        {"key": 2, "total": -4},
    ]


@pytest.mark.parametrize("as_batch", [False, True])
def test_arrow_i64_group_multi_aggregate_stays_columnar_and_preserves_order(
    monkeypatch: pytest.MonkeyPatch, as_batch: bool
) -> None:
    """Removing multi-lane Arrow grouping must expose forbidden input-row boxing."""
    from fpstreams.tabular import arrow as arrow_adapter

    def reject_row_boxing(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("guarded Arrow multi-aggregation must not materialize input rows")

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", reject_row_boxing)
    table = pa.table(
        {
            "key": [3, 1, 3, 2, 1],
            "value": [5, 7, 11, -4, 2],
            "other": [9, 4, 3, 8, 6],
        }
    )
    source = table.to_batches()[0] if as_batch else table

    assert (
        fpstreams.rows.from_arrow(source, batch_size=1)
        .group_by("key")
        .aggregate(
            n=fpstreams.agg.count(),
            total=fpstreams.agg.sum("value"),
            low=fpstreams.agg.min("value"),
            high=fpstreams.agg.max("other"),
        )
        .to_list()
    ) == [
        {"key": 3, "n": 2, "total": 16, "low": 5, "high": 9},
        {"key": 1, "n": 2, "total": 9, "low": 2, "high": 6},
        {"key": 2, "n": 1, "total": -4, "low": -4, "high": 8},
    ]


@pytest.mark.parametrize("adapter", ["dataframe", "polars"])
def test_eager_columnar_frame_group_multi_aggregate_never_boxes_rows(
    monkeypatch: pytest.MonkeyPatch, adapter: str
) -> None:
    """Removing eager-frame planning must expose forbidden Arrow batch conversion."""
    from fpstreams.physical.relational import GroupAggregatePhysicalNode, SourcePhysicalNode
    from fpstreams.planning.compiler import compile_query
    from fpstreams.planning.source import Source

    data = {"key": [2, 1, 2], "value": [4, 7, 3]}
    source = (
        fpstreams.rows.from_dataframe(pd.DataFrame(data), batch_size=1)
        if adapter == "dataframe"
        else fpstreams.rows.from_polars(pl.DataFrame(data), batch_size=1)
    )
    grouped = source.group_by("key").aggregate(
        n=fpstreams.agg.count(),
        low=fpstreams.agg.min("value"),
        high=fpstreams.agg.max("value"),
    )
    physical = compile_query(grouped._flow._query("list"))
    assert isinstance(physical.root, GroupAggregatePhysicalNode)
    assert isinstance(physical.root.input, SourcePhysicalNode)
    input_source = physical.root.input.source
    open_source = Source.open

    def reject_row_source(candidate: Source[object]) -> Iterator[object]:
        if candidate is input_source:
            raise AssertionError("eager frame multi-aggregation entered Python rows")
        return open_source(candidate)

    monkeypatch.setattr(Source, "open", reject_row_source)

    assert grouped.to_list() == [
        {"key": 2, "n": 2, "low": 3, "high": 4},
        {"key": 1, "n": 1, "low": 7, "high": 7},
    ]


def test_arrow_group_multi_aggregate_empty_input_skips_missing_selectors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An empty retained table must finish before schema validation or row conversion."""
    from fpstreams.tabular import arrow as arrow_adapter

    def reject_row_boxing(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("empty Arrow multi-aggregation opened Python rows")

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", reject_row_boxing)
    empty = pa.table({"present": pa.array([], type=pa.int64())})

    assert (
        fpstreams.rows.from_arrow(empty)
        .group_by("missing_key")
        .aggregate(
            n=fpstreams.agg.count(),
            total=fpstreams.agg.sum("missing_value"),
            low=fpstreams.agg.min("missing_value"),
            high=fpstreams.agg.max("missing_value"),
        )
        .to_list()
    ) == []


def test_arrow_group_multi_aggregate_reuses_one_value_and_widens_each_sum(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repeated value lanes must stay columnar without exposing int64 sum overflow."""
    from fpstreams.execution import relational as relational_execution
    from fpstreams.tabular import arrow as arrow_adapter

    imported = relational_execution.import_module
    actual_compute = imported("pyarrow.compute")
    reorder_calls: list[str] = []

    class TrackingCompute:
        def index_in(self, *args: object, **kwargs: object) -> object:
            reorder_calls.append("index_in")
            return actual_compute.index_in(*args, **kwargs)

        def take(self, *args: object, **kwargs: object) -> object:
            reorder_calls.append("take")
            return actual_compute.take(*args, **kwargs)

        def __getattr__(self, name: str) -> object:
            return getattr(actual_compute, name)

    def import_backend(name: str) -> object:
        return TrackingCompute() if name == "pyarrow.compute" else imported(name)

    def reject_row_boxing(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("wide Arrow multi-aggregation opened Python rows")

    monkeypatch.setattr(relational_execution, "import_module", import_backend)
    monkeypatch.setattr(relational_execution, "_ARROW_GROUP_TABLE_MIN_GROUPS", 0)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", reject_row_boxing)
    maximum = 2**63 - 1
    table = pa.table(
        {
            "key": pa.array([None, None, 1, 1], type=pa.int64()),
            "value": [maximum, 1, -4, 7],
        }
    )

    assert (
        fpstreams.rows.from_arrow(table)
        .group_by("key")
        .aggregate(
            n=fpstreams.agg.count(),
            total=fpstreams.agg.sum("value"),
            repeated=fpstreams.agg.sum("value"),
            low=fpstreams.agg.min("value"),
            high=fpstreams.agg.max("value"),
        )
        .to_list()
    ) == [
        {
            "key": None,
            "n": 2,
            "total": 2**63,
            "repeated": 2**63,
            "low": 1,
            "high": maximum,
        },
        {"key": 1, "n": 2, "total": 3, "repeated": 3, "low": -4, "high": 7},
    ]
    assert reorder_calls == []


def test_arrow_group_multi_aggregate_keeps_dictionary_key_safety_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Dictionary keys retain their validated logical-key path instead of index matching."""
    from fpstreams.execution import relational as relational_execution
    from fpstreams.tabular import arrow as arrow_adapter

    imported = relational_execution.import_module
    actual_compute = imported("pyarrow.compute")

    class GuardedCompute:
        @staticmethod
        def index_in(*_args: object, **_kwargs: object) -> object:
            raise AssertionError("dictionary keys must not use Arrow index matching")

        def __getattr__(self, name: str) -> object:
            return getattr(actual_compute, name)

    def import_backend(name: str) -> object:
        return GuardedCompute() if name == "pyarrow.compute" else imported(name)

    def reject_row_boxing(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("dictionary grouping must not materialize input rows")

    dictionary_keys = pa.DictionaryArray.from_arrays(
        pa.array([2, 0, 2, 1, 0], type=pa.int8()),
        pa.array(["a", "b", "c"]),
    )
    table = pa.table({"key": dictionary_keys, "value": [4, 7, 3, -2, 5]})
    monkeypatch.setattr(relational_execution, "import_module", import_backend)
    monkeypatch.setattr(relational_execution, "_ARROW_GROUP_TABLE_MIN_GROUPS", 0)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", reject_row_boxing)

    assert (
        fpstreams.rows.from_arrow(table)
        .group_by("key")
        .aggregate(
            n=fpstreams.agg.count(),
            total=fpstreams.agg.sum("value"),
            low=fpstreams.agg.min("value"),
            high=fpstreams.agg.max("value"),
        )
        .to_list()
    ) == [
        {"key": "c", "n": 2, "total": 7, "low": 3, "high": 4},
        {"key": "a", "n": 2, "total": 12, "low": 5, "high": 7},
        {"key": "b", "n": 1, "total": -2, "low": -2, "high": -2},
    ]


def test_arrow_group_multi_aggregate_preserves_nonexact_output_name_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Arrow schema names must not erase observable ``str`` subclass identities."""
    from fpstreams.execution import relational as relational_execution

    class OutputName(str):
        pass

    key_name = OutputName("group")
    total_name = OutputName("total")

    def forbidden_arrays(*_arguments: object, **_options: object) -> list[object]:
        raise AssertionError("non-exact output names must keep Python dictionary materialization")

    monkeypatch.setattr(relational_execution, "_ARROW_GROUP_TABLE_MIN_GROUPS", 0)
    monkeypatch.setattr(relational_execution, "_arrow_group_lane_arrays", forbidden_arrays)
    result = (
        fpstreams.rows.from_arrow(pa.table({"key": [2, 1, 2], "value": [4, 7, 3]}))
        .group_by(**{key_name: "key"})
        .aggregate(**{total_name: fpstreams.agg.sum("value"), "low": fpstreams.agg.min("value")})
        .to_list()
    )

    assert result == [
        {"group": 2, "total": 7, "low": 3},
        {"group": 1, "total": 7, "low": 7},
    ]
    assert next(name for name in result[0] if name == key_name) is key_name
    assert next(name for name in result[0] if name == total_name) is total_name


def test_arrow_group_multi_aggregate_extension_key_keeps_canonical_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Extension keys remain on row execution instead of entering scalar Arrow grouping."""
    from fpstreams.execution import relational as relational_execution
    from fpstreams.tabular import arrow as arrow_adapter

    class WrappedIntType(pa.ExtensionType):
        def __init__(self) -> None:
            super().__init__(pa.int64(), "fpstreams.test.wrapped_int")

        def __arrow_ext_serialize__(self) -> bytes:
            return b""

        @classmethod
        def __arrow_ext_deserialize__(
            cls, _storage_type: pa.DataType, _serialized: bytes
        ) -> WrappedIntType:
            return cls()

    keys = pa.ExtensionArray.from_storage(
        WrappedIntType(),
        pa.array([2, 1, 2], type=pa.int64()),
    )
    converted: list[int] = []
    convert_rows = arrow_adapter.batch_to_rows

    def tracked(batch: object) -> list[dict[str, object]]:
        converted.append(batch.num_rows)  # type: ignore[attr-defined]
        return convert_rows(batch)

    def forbidden_arrays(*_arguments: object, **_options: object) -> list[object]:
        raise AssertionError("extension keys must not reach grouped Arrow materialization")

    monkeypatch.setattr(relational_execution, "_ARROW_GROUP_TABLE_MIN_GROUPS", 0)
    monkeypatch.setattr(relational_execution, "_arrow_group_lane_arrays", forbidden_arrays)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", tracked)

    assert (
        fpstreams.rows.from_arrow(pa.table({"key": keys, "value": [4, 7, 3]}))
        .group_by("key")
        .aggregate(n=fpstreams.agg.count(), total=fpstreams.agg.sum("value"))
        .to_list()
    ) == [
        {"key": 2, "n": 2, "total": 7},
        {"key": 1, "n": 1, "total": 7},
    ]
    assert converted == [3]


def test_arrow_group_multi_aggregate_rejects_non_i64_values_before_compute(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Float lanes retain canonical Python arithmetic rather than Arrow semantics."""
    from fpstreams.tabular import arrow as arrow_adapter

    converted: list[int] = []
    convert_rows = arrow_adapter.batch_to_rows

    def tracked(batch: object) -> list[dict[str, object]]:
        converted.append(batch.num_rows)  # type: ignore[attr-defined]
        return convert_rows(batch)

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", tracked)
    table = pa.table({"key": [1, 1], "value": [2.5, 3.5]})

    assert (
        fpstreams.rows.from_arrow(table)
        .group_by("key")
        .aggregate(
            n=fpstreams.agg.count(),
            total=fpstreams.agg.sum("value"),
            low=fpstreams.agg.min("value"),
            high=fpstreams.agg.max("value"),
        )
        .to_list()
    ) == [{"key": 1, "n": 2, "total": 6.0, "low": 2.5, "high": 3.5}]
    assert converted == [2]


def test_arrow_group_multi_aggregate_rejects_null_values_before_compute(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A null int64 lane must use the canonical selected-value state machine."""
    from fpstreams.tabular import arrow as arrow_adapter

    converted: list[int] = []
    convert_rows = arrow_adapter.batch_to_rows

    def tracked(batch: object) -> list[dict[str, object]]:
        converted.append(batch.num_rows)  # type: ignore[attr-defined]
        return convert_rows(batch)

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", tracked)
    table = pa.table({"key": [1], "value": pa.array([None], type=pa.int64())})

    assert (
        fpstreams.rows.from_arrow(table)
        .group_by("key")
        .aggregate(
            n=fpstreams.agg.count(),
            low=fpstreams.agg.min("value"),
            high=fpstreams.agg.max("value"),
        )
        .to_list()
    ) == [{"key": 1, "n": 1, "low": None, "high": None}]
    assert converted == [1]


def test_arrow_group_multi_aggregate_planner_keeps_strict_source_and_lane_boundaries(
    tmp_path: Path,
) -> None:
    """Only schema-proven readers cross the strict source and lane boundaries."""
    from fpstreams.physical.relational import (
        ArrowGroupAggregateSpec,
        GroupAggregatePhysicalNode,
    )
    from fpstreams.planning.compiler import compile_query

    table = pa.table({"key": [1, 1], "value": [2, 3]})
    reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches())
    csv_path = tmp_path / "multi-group.csv"
    csv_path.write_text("key,value\n1,2\n1,3\n", encoding="utf-8")
    reader_grouped = (
        fpstreams.rows.from_arrow(reader)
        .group_by("key")
        .aggregate(n=fpstreams.agg.count(), low=fpstreams.agg.min("value"))
    )
    reader_physical = compile_query(reader_grouped._flow._query("list"))
    assert isinstance(reader_physical.root, GroupAggregatePhysicalNode)
    assert isinstance(reader_physical.root.arrow_i64_sum, ArrowGroupAggregateSpec)
    assert reader_grouped.to_list() == [{"key": 1, "n": 2, "low": 2}]

    unsupported = (
        fpstreams.rows.scan_csv(csv_path)
        .group_by("key")
        .aggregate(n=fpstreams.agg.count(), low=fpstreams.agg.min("value")),
        fpstreams.rows.from_arrow(table)
        .group_by(lambda row: row["key"])
        .aggregate(n=fpstreams.agg.count(), low=fpstreams.agg.min("value")),
        fpstreams.rows.from_arrow(table)
        .group_by("key")
        .aggregate(n=fpstreams.agg.count(), first=fpstreams.agg.first("value")),
    )

    for grouped in unsupported:
        physical = compile_query(grouped._flow._query("list"))
        assert isinstance(physical.root, GroupAggregatePhysicalNode)
        assert physical.root.arrow_i64_sum is None


@pytest.mark.parametrize(
    "error_type", [ArithmeticError, NotImplementedError, TypeError, ValueError]
)
def test_arrow_group_multi_aggregate_backend_decline_reopens_canonical_rows(
    monkeypatch: pytest.MonkeyPatch, error_type: type[Exception]
) -> None:
    """Expected Arrow rejections leave the retained source clean for row fallback."""
    from fpstreams.execution import relational as relational_execution
    from fpstreams.tabular import arrow as arrow_adapter

    imported = relational_execution.import_module
    actual_compute = imported("pyarrow.compute")
    converted: list[int] = []
    convert_rows = arrow_adapter.batch_to_rows

    class RejectingCompute:
        def min_max(self, _values: object) -> object:
            raise error_type("decline")

        def __getattr__(self, name: str) -> object:
            return getattr(actual_compute, name)

    def import_backend(name: str) -> object:
        return RejectingCompute() if name == "pyarrow.compute" else imported(name)

    def tracked(batch: object) -> list[dict[str, object]]:
        converted.append(batch.num_rows)  # type: ignore[attr-defined]
        return convert_rows(batch)

    monkeypatch.setattr(relational_execution, "import_module", import_backend)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", tracked)

    assert (
        fpstreams.rows.from_arrow(pa.table({"key": [1, 1], "value": [2, 3]}))
        .group_by("key")
        .aggregate(n=fpstreams.agg.count(), total=fpstreams.agg.sum("value"))
        .to_list()
    ) == [{"key": 1, "n": 2, "total": 5}]
    assert converted == [2]


def test_arrow_group_multi_aggregate_memory_error_never_reopens_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Allocation failure propagates instead of masquerading as an unsupported kernel."""
    from fpstreams.execution import relational as relational_execution
    from fpstreams.tabular import arrow as arrow_adapter

    imported = relational_execution.import_module
    actual_compute = imported("pyarrow.compute")

    class FailingCompute:
        def min_max(self, _values: object) -> object:
            raise MemoryError("multi-group allocation")

        def __getattr__(self, name: str) -> object:
            return getattr(actual_compute, name)

    def import_backend(name: str) -> object:
        return FailingCompute() if name == "pyarrow.compute" else imported(name)

    def forbidden_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("MemoryError must not reopen the canonical row source")

    monkeypatch.setattr(relational_execution, "import_module", import_backend)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", forbidden_rows)
    grouped = (
        fpstreams.rows.from_arrow(pa.table({"key": [1], "value": [2]}))
        .group_by("key")
        .aggregate(n=fpstreams.agg.count(), total=fpstreams.agg.sum("value"))
    )

    with pytest.raises(MemoryError, match="multi-group allocation"):
        grouped.to_list()


def test_arrow_group_multi_aggregate_uses_named_columns_across_arrow_orders(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A backend that returns aggregate columns before the key must remain correct."""
    from fpstreams.execution import relational as relational_execution
    from fpstreams.tabular import arrow as arrow_adapter

    imported = relational_execution.import_module
    actual_arrow = imported("pyarrow")
    aggregate_requests: list[tuple[object, str]] = []
    materialized_names: list[tuple[str, ...]] = []

    class ReorderedGroupBy:
        def __init__(self, delegate: object) -> None:
            self._delegate = delegate

        def aggregate(self, requests: list[tuple[object, str]]) -> object:
            aggregate_requests.extend(requests)
            grouped = self._delegate.aggregate(requests)  # type: ignore[attr-defined]
            key_index = grouped.schema.get_field_index("__fpstreams_group_key")
            order = [index for index in range(grouped.num_columns) if index != key_index] + [
                key_index
            ]
            return grouped.select(order)

    class ReorderedTable:
        def __init__(self, delegate: object) -> None:
            self._delegate = delegate

        def group_by(self, *args: object, **kwargs: object) -> ReorderedGroupBy:
            return ReorderedGroupBy(self._delegate.group_by(*args, **kwargs))  # type: ignore[attr-defined]

    class TrackingTableMeta(type):
        def __instancecheck__(cls, instance: object) -> bool:
            return isinstance(instance, actual_arrow.Table)

    class TrackingTable(metaclass=TrackingTableMeta):
        @staticmethod
        def from_arrays(arrays: list[object], *, names: list[str]) -> object:
            materialized_names.append(tuple(names))
            return actual_arrow.Table.from_arrays(arrays, names=names)

    class ReorderedArrow:
        Table = TrackingTable
        RecordBatch = actual_arrow.RecordBatch
        types = actual_arrow.types

        @staticmethod
        def table(*args: object, **kwargs: object) -> ReorderedTable:
            return ReorderedTable(actual_arrow.table(*args, **kwargs))

        def __getattr__(self, name: str) -> object:
            return getattr(actual_arrow, name)

    def import_backend(name: str) -> object:
        return ReorderedArrow() if name == "pyarrow" else imported(name)

    def forbidden_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("version-compatible Arrow grouping must not box rows")

    monkeypatch.setattr(relational_execution, "import_module", import_backend)
    monkeypatch.setattr(relational_execution, "_ARROW_GROUP_TABLE_MIN_GROUPS", 0)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", forbidden_rows)
    table = pa.table({"key": [2, 1, 2], "value": [4, 7, 3]})

    assert (
        fpstreams.rows.from_arrow(table)
        .group_by("key")
        .aggregate(
            n=fpstreams.agg.count(),
            rows=fpstreams.agg.count(),
            total=fpstreams.agg.sum("value"),
            low=fpstreams.agg.min("value"),
            high=fpstreams.agg.max("value"),
        )
        .to_list()
    ) == [
        {"key": 2, "n": 2, "rows": 2, "total": 7, "low": 3, "high": 4},
        {"key": 1, "n": 1, "rows": 1, "total": 7, "low": 7, "high": 7},
    ]
    assert aggregate_requests.count(([], "count_all")) == 1
    assert materialized_names == [("key", "n", "rows", "total", "low", "high")]


@pytest.mark.parametrize("as_batch", [False, True])
def test_arrow_i64_group_sum_uses_true_first_seen_order_not_hash_order(
    monkeypatch: pytest.MonkeyPatch, as_batch: bool
) -> None:
    """Retained Arrow grouping must not expose the hash table's output order."""
    from fpstreams.tabular import arrow as arrow_adapter

    def reject_row_boxing(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("first-seen Arrow grouping must stay columnar")

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", reject_row_boxing)
    table = pa.table({"key": ["k66", "k43", "k3"], "value": [1, 2, 3]})
    source = table.to_batches()[0] if as_batch else table

    assert (
        fpstreams.rows.from_arrow(source)
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
        .to_list()
    ) == [
        {"key": "k66", "total": 1},
        {"key": "k43", "total": 2},
        {"key": "k3", "total": 3},
    ]


@pytest.mark.parametrize("as_batch", [False, True])
def test_arrow_group_sum_uses_the_retained_table_without_batch_size_slicing(
    monkeypatch: pytest.MonkeyPatch, as_batch: bool
) -> None:
    """Tiny adapter batches cannot create O(row_count) batch objects before grouping."""
    from fpstreams.planning.arrow_source import ArrowBatchSource

    def forbidden_batches(self: ArrowBatchSource, **_options: object) -> Iterator[object]:
        raise AssertionError("eager Arrow grouping must use its retained columnar object")

    monkeypatch.setattr(ArrowBatchSource, "open_batches", forbidden_batches)
    table = pa.table({"key": [1, 1, 2], "value": [3, 4, 5]})
    source = table.to_batches()[0] if as_batch else table

    assert (
        fpstreams.rows.from_arrow(source, batch_size=1)
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
        .to_list()
    ) == [{"key": 1, "total": 7}, {"key": 2, "total": 5}]


def test_arrow_i64_group_sum_uses_exact_bigints_when_a_sum_may_overflow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The columnar path must not expose Arrow's wrapping int64 sum semantics."""
    from fpstreams.tabular import arrow as arrow_adapter

    def reject_row_boxing(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("overflow-safe Arrow grouping must remain columnar")

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", reject_row_boxing)
    maximum = 2**63 - 1
    minimum = -(2**63)
    table = pa.table(
        {
            "key": ["small", "overflow", "underflow", "small", "overflow", "underflow"],
            "value": [2, maximum, minimum, 3, 1, -1],
        }
    )

    assert (
        fpstreams.rows.from_arrow(table)
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
        .to_list()
    ) == [
        {"key": "small", "total": 5},
        {"key": "overflow", "total": 2**63},
        {"key": "underflow", "total": -(2**63) - 1},
    ]


def test_arrow_group_sum_declines_float_keys_null_values_and_failpoints(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Protocol-sensitive cases reopen the canonical row executor without semantic drift."""
    from math import isnan

    from fpstreams.runtime.failpoints import failpoint
    from fpstreams.tabular import arrow as arrow_adapter

    converted: list[int] = []
    convert_rows = arrow_adapter.batch_to_rows

    def tracked(batch: object) -> list[dict[str, object]]:
        converted.append(batch.num_rows)  # type: ignore[attr-defined]
        return convert_rows(batch)

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", tracked)
    nan_groups = (
        fpstreams.rows.from_arrow(pa.table({"key": [float("nan"), float("nan")], "value": [1, 2]}))
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
        .to_list()
    )
    assert len(nan_groups) == 2
    assert all(isnan(row["key"]) for row in nan_groups)
    assert [row["total"] for row in nan_groups] == [1, 2]

    with pytest.raises(TypeError, match="unsupported operand"):
        (
            fpstreams.rows.from_arrow(pa.table({"key": [1], "value": pa.array([None], pa.int64())}))
            .group_by("key")
            .aggregate(total=fpstreams.agg.sum("value"))
            .to_list()
        )

    with failpoint("unrelated.transition", RuntimeError("unused")):
        assert (
            fpstreams.rows.from_arrow(pa.table({"key": [1, 1], "value": [2, 3]}))
            .group_by("key")
            .aggregate(total=fpstreams.agg.sum("value"))
            .to_list()
        ) == [{"key": 1, "total": 5}]
    assert converted == [2, 1, 2]


@pytest.mark.parametrize(
    ("keys", "expected"),
    [
        (pa.array([None, None, None]), [{"key": None, "total": 6}]),
        (pa.array([True, False, True]), [{"key": True, "total": 4}, {"key": False, "total": 2}]),
        (pa.array([b"a", b"b", b"a"]), [{"key": b"a", "total": 4}, {"key": b"b", "total": 2}]),
    ],
)
def test_arrow_group_sum_supports_python_equivalent_scalar_key_types(
    keys: pa.Array, expected: list[dict[str, object]]
) -> None:
    """Null, boolean, and binary keys retain Python values and encounter order."""
    result = (
        fpstreams.rows.from_arrow(pa.table({"key": keys, "value": [1, 2, 3]}), batch_size=1)
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
        .to_list()
    )

    assert result == expected


def test_arrow_group_sum_preserves_empty_selector_timing_and_same_field_sum() -> None:
    """Empty inputs skip selectors and a key field may also be the summed value field."""
    empty = pa.table({"present": pa.array([], type=pa.int64())})
    assert (
        fpstreams.rows.from_arrow(empty)
        .group_by("missing")
        .aggregate(total=fpstreams.agg.sum("also_missing"))
        .to_list()
    ) == []

    maximum = 2**63 - 1
    result = (
        fpstreams.rows.from_arrow(pa.table({"value": [maximum, maximum]}))
        .group_by(bucket="value")
        .aggregate(total=fpstreams.agg.sum("value"))
        .to_list()
    )
    assert result == [{"bucket": maximum, "total": maximum * 2}]

    with pytest.raises(fpstreams.SelectionError):
        (
            fpstreams.rows.from_arrow(pa.table({"present": [1]}))
            .group_by("missing")
            .aggregate(total=fpstreams.agg.sum("present"))
            .to_list()
        )


def test_arrow_group_sum_closes_its_retained_batch_iterator() -> None:
    """The speculative columnar scan owns and closes the descriptor iterator."""
    from fpstreams.planning.arrow_source import ArrowBatchSource
    from fpstreams.planning.source import Source, SourceCapabilities

    batch = pa.record_batch({"key": [1, 1], "value": [2, 3]})
    events: list[str] = []

    class Batches(Iterator[object]):
        emitted = False

        def __next__(self) -> object:
            if self.emitted:
                raise StopIteration
            self.emitted = True
            events.append("batch")
            return batch

        def close(self) -> None:
            events.append("close")

    descriptor = ArrowBatchSource(Batches, "table", 65_536, batch.schema)

    def forbidden_rows() -> Iterator[dict[str, int]]:
        raise AssertionError("columnar grouping must not open the row source")
        yield

    source = Source(
        forbidden_rows,
        SourceCapabilities(reiterable=True, exact_size=2),
        native_data=descriptor,
    )
    grouped = (
        fpstreams.Rows(fpstreams.Flow(source))
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
    )

    assert grouped.to_list() == [{"key": 1, "total": 5}]
    assert events == ["batch", "close"]


@pytest.mark.parametrize(
    "error_type", [ArithmeticError, NotImplementedError, TypeError, ValueError]
)
def test_arrow_group_sum_backend_decline_reopens_the_canonical_rows(
    monkeypatch: pytest.MonkeyPatch, error_type: type[Exception]
) -> None:
    """Expected backend rejections are speculative and leave a clean replayable fallback."""
    from fpstreams.execution import relational as relational_execution
    from fpstreams.tabular import arrow as arrow_adapter

    imported = relational_execution.import_module
    actual_compute = imported("pyarrow.compute")
    converted: list[int] = []
    convert_rows = arrow_adapter.batch_to_rows

    class RejectingCompute:
        def min_max(self, _values: object) -> object:
            raise error_type("decline")

        def __getattr__(self, name: str) -> object:
            return getattr(actual_compute, name)

    rejecting_compute = RejectingCompute()

    def import_backend(name: str) -> object:
        return rejecting_compute if name == "pyarrow.compute" else imported(name)

    def tracked(batch: object) -> list[dict[str, object]]:
        converted.append(batch.num_rows)  # type: ignore[attr-defined]
        return convert_rows(batch)

    monkeypatch.setattr(relational_execution, "import_module", import_backend)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", tracked)
    result = (
        fpstreams.rows.from_arrow(pa.table({"key": [1, 1], "value": [2, 3]}))
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
        .to_list()
    )

    assert result == [{"key": 1, "total": 5}]
    assert converted == [2]


def test_arrow_group_sum_memory_error_propagates_without_row_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Allocation failure is never mistaken for an unsupported Arrow specialization."""
    from fpstreams.execution import relational as relational_execution
    from fpstreams.tabular import arrow as arrow_adapter

    imported = relational_execution.import_module
    actual_compute = imported("pyarrow.compute")

    class FailingCompute:
        def min_max(self, _values: object) -> object:
            raise MemoryError("group allocation")

        def __getattr__(self, name: str) -> object:
            return getattr(actual_compute, name)

    failing_compute = FailingCompute()

    def import_backend(name: str) -> object:
        return failing_compute if name == "pyarrow.compute" else imported(name)

    def forbidden_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("MemoryError must not reopen the canonical row source")

    monkeypatch.setattr(relational_execution, "import_module", import_backend)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", forbidden_rows)
    grouped = (
        fpstreams.rows.from_arrow(pa.table({"key": [1], "value": [2]}))
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
    )

    with pytest.raises(MemoryError, match="group allocation"):
        grouped.to_list()


def test_arrow_reader_stays_one_shot_and_closes_after_short_circuit() -> None:
    batch = pa.RecordBatch.from_pydict({"id": [1, 2, 3]})
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    source = fpstreams.rows.from_arrow(reader, batch_size=1)

    assert source.take(1).to_list() == [{"id": 1}]
    with pytest.raises(fpstreams.FlowConsumedError):
        source.to_list()


@pytest.mark.parametrize("kind", ["table", "record_batch"])
def test_rows_arrow_c_stream_defers_known_native_batches_until_pull(kind: Any) -> None:
    """Importing a known-schema in-memory stream must not request its first batch."""
    from fpstreams.planning.arrow_source import ArrowBatchSource
    from fpstreams.planning.source import Source, SourceCapabilities
    from fpstreams.streams.flow import Flow
    from fpstreams.tabular.rows import Rows

    batch = pa.record_batch({"id": [1, 2]})
    events: list[str] = []

    def open_batches() -> Iterator[pa.RecordBatch]:
        events.append("open")
        return _TrackedArrowBatches([batch], events)

    descriptor = ArrowBatchSource(
        open_batches,
        kind,
        65_536,
        batch.schema,
        materialized_data=pa.Table.from_batches([batch]) if kind == "table" else batch,
    )
    source = Source(
        lambda: iter(batch.to_pylist()),
        SourceCapabilities(reiterable=True, exact_size=2),
        native_data=descriptor,
    )

    reader = pa.RecordBatchReader.from_stream(Rows(Flow(source)))
    assert events == []
    assert reader.read_all().to_pylist() == [{"id": 1}, {"id": 2}]
    assert events == ["open", "pull:0", "stop", "close"]


def test_rows_arrow_c_stream_claims_known_native_source_on_first_pull() -> None:
    """The lazy native opener must cross the source claim boundary exactly once."""
    from fpstreams.planning.arrow_source import ArrowBatchSource
    from fpstreams.planning.source import Source, SourceCapabilities
    from fpstreams.streams.flow import Flow
    from fpstreams.tabular.rows import Rows

    batch = pa.record_batch({"id": [1]})
    descriptor = ArrowBatchSource(
        lambda: iter((batch,)),
        "table",
        65_536,
        batch.schema,
        materialized_data=pa.Table.from_batches([batch]),
    )
    source = Source(
        lambda: iter(batch.to_pylist()),
        SourceCapabilities(reiterable=False, exact_size=1),
        native_data=descriptor,
    )
    reader = pa.RecordBatchReader.from_stream(Rows(Flow(source)))

    assert reader.read_next_batch().to_pylist() == [{"id": 1}]
    with pytest.raises(fpstreams.FlowConsumedError):
        source.open_native(ArrowBatchSource)


@pytest.mark.parametrize("container", ["list", "tuple"])
def test_rows_arrow_c_stream_prefetches_only_one_unknown_schema_batch(container: str) -> None:
    """A plain in-memory row list must not be converted beyond schema inference."""

    class CountingRecord(Mapping[str, int]):
        def __init__(self) -> None:
            self.lookups = 0

        def __getitem__(self, key: str) -> int:
            assert key == "id"
            self.lookups += 1
            return 1

        def __iter__(self) -> Iterator[str]:
            return iter(("id",))

        def __len__(self) -> int:
            return 1

    record = CountingRecord()
    records = [record] * 65_537
    reader = pa.RecordBatchReader.from_stream(
        fpstreams.rows(records if container == "list" else tuple(records))
    )

    assert record.lookups == 65_536
    assert reader.read_next_batch().num_rows == 65_536
    reader.close()


def test_rows_arrow_c_stream_closes_unknown_schema_iterator_on_first_batch_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A schema-inference failure must close the opened batch iterator before escaping."""
    from fpstreams.tabular.rows import Rows

    events: list[str] = []

    class FailingBatches:
        def __iter__(self) -> FailingBatches:
            return self

        def __next__(self) -> pa.RecordBatch:
            events.append("next")
            raise RuntimeError("first batch failed")

        def close(self) -> None:
            events.append("close")

    monkeypatch.setattr(Rows, "arrow_batches", lambda self: FailingBatches())

    with pytest.raises(RuntimeError, match="first batch failed"):
        fpstreams.rows([]).__arrow_c_stream__()

    assert events == ["next", "close"]


def test_rows_arrow_c_stream_preserves_requested_schema_casts() -> None:
    """A compatible requested schema keeps PyArrow's lazy reader cast behavior."""
    target = pa.schema([("id", pa.int32())])

    reader = pa.RecordBatchReader.from_stream(
        fpstreams.rows([{"id": 1}, {"id": 2}]),
        schema=target,
    )

    assert reader.schema == target
    assert reader.read_all().to_pylist() == [{"id": 1}, {"id": 2}]


def test_rows_arrow_c_stream_requested_schema_keeps_late_conversion_error_order() -> None:
    """Schema negotiation must not overtake a later canonical row-conversion failure."""
    records: list[object] = [{"id": 1}] * 65_536 + [object()]
    target = pa.schema([("other", pa.int64())])

    with pytest.raises(fpstreams.SelectionError, match="cannot be represented as a record"):
        pa.RecordBatchReader.from_stream(fpstreams.rows(records), schema=target)


def test_rows_arrow_c_stream_preserves_empty_source_schemas() -> None:
    """Unknown empty rows stay schema-less while a retained Arrow schema survives."""
    unknown = pa.RecordBatchReader.from_stream(fpstreams.rows([]))
    schema = pa.schema([("id", pa.int64())])
    known = pa.RecordBatchReader.from_stream(
        fpstreams.rows.from_arrow(pa.Table.from_batches([], schema=schema))
    )

    assert unknown.schema == pa.schema([])
    assert unknown.read_all().to_pylist() == []
    assert known.schema == schema
    assert known.read_all().to_pylist() == []


@pytest.mark.parametrize(
    "rows",
    [
        fpstreams.rows([{"id": 1}]),
        fpstreams.rows.from_arrow(pa.table({"id": [1]})),
    ],
)
def test_rows_arrow_c_stream_rejects_incompatible_requested_schema(rows: object) -> None:
    """A renamed requested field retains PyArrow's eager schema validation error."""
    target = pa.schema([("other", pa.int64())])

    with pytest.raises(ValueError, match="field names are not matching"):
        pa.RecordBatchReader.from_stream(rows, schema=target)


def test_rows_arrow_c_stream_keeps_resource_backed_readers_eager_and_one_shot() -> None:
    """Unsupported resource-backed streams close before export and are claimed once."""
    batch = pa.record_batch({"id": [1, 2]})
    events: list[str] = []

    def batches() -> Iterator[pa.RecordBatch]:
        events.append("open")
        try:
            events.append("pull:0")
            yield batch
        finally:
            events.append("close")

    upstream = pa.RecordBatchReader.from_batches(batch.schema, batches())
    rows = fpstreams.rows.from_arrow(upstream)

    exported = pa.RecordBatchReader.from_stream(rows)
    assert events == ["open", "pull:0", "close"]
    exported.close()
    assert events == ["open", "pull:0", "close"]
    with pytest.raises(fpstreams.FlowConsumedError):
        pa.RecordBatchReader.from_stream(rows)
    assert events == ["open", "pull:0", "close"]


def test_rows_arrow_c_stream_capsule_abandonment_leaves_no_resource_source_open() -> None:
    """A capsule can be released after the conservative path has closed its reader source."""
    batch = pa.record_batch({"id": [1]})
    events: list[str] = []

    def batches() -> Iterator[pa.RecordBatch]:
        events.append("open")
        try:
            yield batch
        finally:
            events.append("close")

    upstream = pa.RecordBatchReader.from_batches(batch.schema, batches())
    capsule = fpstreams.rows.from_arrow(upstream).__arrow_c_stream__()

    assert events == ["open", "close"]
    del capsule
    gc.collect()
    assert events == ["open", "close"]


def test_rows_arrow_c_stream_closes_resource_source_on_conversion_error() -> None:
    """A non-memory source keeps eager conversion so its primary error closes upstream."""
    events: list[str] = []

    def records() -> Iterator[object]:
        events.append("open")
        try:
            yield {"id": 1}
            yield object()
        finally:
            events.append("close")

    with pytest.raises(fpstreams.SelectionError, match="cannot be represented as a record"):
        pa.RecordBatchReader.from_stream(fpstreams.rows(records()))

    assert events == ["open", "close"]


def test_from_arrow_imports_arrow_c_stream_provider_once_as_one_shot() -> None:
    """Removing the protocol branch must make this real capsule provider unsupported."""
    batch = pa.record_batch({"id": [1, 2], "label": ["a", "b"]})

    class StreamProvider:
        def __init__(self) -> None:
            self.calls = 0
            self.reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])

        def __arrow_c_stream__(self, requested_schema: object = None) -> object:
            self.calls += 1
            return self.reader.__arrow_c_stream__(requested_schema)

    provider = StreamProvider()
    source = fpstreams.rows.from_arrow(provider, batch_size=1)

    assert provider.calls == 1
    assert source.to_list() == [
        {"id": 1, "label": "a"},
        {"id": 2, "label": "b"},
    ]
    with pytest.raises(fpstreams.FlowConsumedError):
        source.to_list()
    assert provider.calls == 1


def test_arrow_c_stream_provider_closes_imported_reader_before_first_pull(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A source-open failure must release the reader imported from the C stream capsule."""
    from fpstreams.runtime.failpoints import failpoint
    from fpstreams.tabular import arrow as arrow_adapter

    batch = pa.record_batch({"id": [1]})

    class StreamProvider:
        def __arrow_c_stream__(self, requested_schema: object = None) -> object:
            reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
            return reader.__arrow_c_stream__(requested_schema)

    closed: list[object] = []
    close = arrow_adapter._close

    def observe_close(resource: object) -> None:
        if isinstance(resource, pa.RecordBatchReader):
            closed.append(resource)
        close(resource)

    monkeypatch.setattr(arrow_adapter, "_close", observe_close)
    source = fpstreams.rows.from_arrow(StreamProvider())

    with (
        failpoint("source.open.after", RuntimeError("stream provider open failed")),
        pytest.raises(RuntimeError, match="stream provider open failed"),
    ):
        source.to_list()

    assert len(closed) == 1


def test_arrow_record_batches_honor_batch_size_before_row_conversion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RecordBatch and reader sources bound first-row conversion just like Tables."""
    from fpstreams.planning.arrow_source import batch_to_rows as convert_rows
    from fpstreams.tabular import arrow as arrow_adapter

    converted_sizes: list[int] = []

    def tracked(batch: object) -> list[dict[str, object]]:
        converted_sizes.append(batch.num_rows)  # type: ignore[attr-defined]
        return convert_rows(batch)

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", tracked)
    batch = pa.record_batch({"id": list(range(5))})
    reader = pa.RecordBatchReader.from_batches(batch.schema, (batch,))

    assert fpstreams.rows.from_arrow(batch, batch_size=2).take(1).to_list() == [{"id": 0}]
    assert converted_sizes == [2]
    converted_sizes.clear()
    assert fpstreams.rows.from_arrow(reader, batch_size=2).take(1).to_list() == [{"id": 0}]
    assert converted_sizes == [2]


def test_arrow_row_conversion_uses_the_owned_pylist_dictionaries_directly() -> None:
    """The adapter does not duplicate dictionaries already materialized by Arrow."""
    from fpstreams.planning.arrow_source import batch_to_rows

    record = {"id": 1}

    class Batch:
        def to_pylist(self) -> list[dict[str, int]]:
            return [record]

    converted = batch_to_rows(Batch())

    assert converted == [record]
    assert converted[0] is record


@pytest.mark.parametrize("as_record_batch", [False, True])
def test_arrow_identity_list_collects_batches_without_opening_the_row_iterator(
    monkeypatch: pytest.MonkeyPatch,
    as_record_batch: bool,
) -> None:
    """Identity list materialization should not forward every converted row through Flow."""
    from fpstreams.planning.arrow_source import ArrowBatchSource
    from fpstreams.planning.source import Source
    from fpstreams.tabular import arrow as arrow_adapter

    table = pa.table({"id": list(range(5)), "value": list(range(10, 15))})
    source_value = table.to_batches()[0] if as_record_batch else table
    converted_sizes: list[int] = []
    convert_rows = arrow_adapter.batch_to_rows
    open_rows = Source.open

    def tracked(batch: object) -> list[dict[str, object]]:
        converted_sizes.append(batch.num_rows)  # type: ignore[attr-defined]
        return convert_rows(batch)

    def reject_arrow_rows(source: Source[object]) -> Iterator[object]:
        if isinstance(source.native_data, ArrowBatchSource):
            raise AssertionError("identity Arrow to_list must not open the row iterator")
        return open_rows(source)

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", tracked)
    monkeypatch.setattr(Source, "open", reject_arrow_rows)

    assert fpstreams.rows.from_arrow(source_value, batch_size=2).to_list() == [
        {"id": index, "value": index + 10} for index in range(5)
    ]
    assert converted_sizes == [2, 2, 1]


def test_arrow_identity_list_forced_python_uses_the_canonical_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Forced Python must open rows instead of claiming the direct Arrow list path."""
    from fpstreams.planning.arrow_source import ArrowBatchSource
    from fpstreams.planning.source import Source

    canonical_opens = 0
    open_rows = Source.open
    open_native = Source.open_native

    def tracked_open(source: Source[object]) -> Iterator[object]:
        nonlocal canonical_opens
        if isinstance(source.native_data, ArrowBatchSource):
            canonical_opens += 1
        return open_rows(source)

    def reject_direct_arrow(source: Source[object], expected_type: type[object]) -> object:
        if isinstance(source.native_data, ArrowBatchSource):
            raise AssertionError("forced Python must not claim the direct Arrow list path")
        return open_native(source, expected_type)

    monkeypatch.setattr(Source, "open", tracked_open)
    monkeypatch.setattr(Source, "open_native", reject_direct_arrow)

    assert fpstreams.rows.from_arrow(pa.table({"id": [1, 2]})).with_engine("python").to_list() == [
        {"id": 1},
        {"id": 2},
    ]
    assert canonical_opens == 1


def test_arrow_identity_list_closes_batches_when_row_conversion_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A conversion error must not leak the direct Arrow batch iterator."""
    from fpstreams.planning.arrow_source import ArrowBatchSource
    from fpstreams.tabular import arrow as arrow_adapter

    events: list[str] = []
    batches = _TrackedArrowBatches([pa.record_batch({"id": [1]})], events)

    def open_tracked_batches(
        _source: ArrowBatchSource,
        *,
        columns: tuple[str, ...] | None = None,
        equality: tuple[str, object] | None = None,
        first_only: bool = False,
        range_predicate: tuple[str, object, int] | None = None,
    ) -> Iterator[pa.RecordBatch]:
        del columns, equality, first_only, range_predicate
        return batches

    def fail_conversion(_batch: object) -> list[dict[str, object]]:
        raise ValueError("row conversion failed")

    monkeypatch.setattr(ArrowBatchSource, "open_batches", open_tracked_batches)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", fail_conversion)

    with pytest.raises(ValueError, match="row conversion failed"):
        fpstreams.rows.from_arrow(pa.table({"id": [1]})).to_list()

    assert events == ["pull:0", "close"]


def test_arrow_row_source_skips_per_batch_failpoint_calls_when_uninstrumented(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ordinary Arrow iteration must not call the no-op transition hook per batch or row."""
    from fpstreams.runtime import failpoints

    observed: list[str] = []

    def tracked(name: str) -> None:
        observed.append(name)
        if name.startswith("arrow."):
            raise AssertionError("inactive Arrow transitions must skip hit() entirely")

    monkeypatch.setattr(failpoints, "hit", tracked)

    assert fpstreams.rows.from_arrow(pa.table({"id": [1, 2]}), batch_size=1).to_list() == [
        {"id": 1},
        {"id": 2},
    ]
    assert observed == ["source.open.after"]


def test_arrow_batch_failpoint_remains_reachable_when_instrumented() -> None:
    """Caching instrumentation state must retain the active per-row Arrow boundary."""
    from fpstreams.runtime.failpoints import failpoint

    with (
        failpoint("arrow.batch.after", OSError("batch")),
        pytest.raises(OSError, match="batch"),
    ):
        fpstreams.rows.from_arrow(pa.table({"id": [1]})).to_list()


def test_arrow_exact_select_projects_before_full_python_row_conversion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Removing direct-select pushdown must expose the forbidden full-batch conversion."""
    from fpstreams.execution import arrow as arrow_execution

    def reject_full_batch(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("full Arrow rows must not be materialized before exact select")

    monkeypatch.setattr(arrow_execution, "batch_to_rows", reject_full_batch)
    result = (
        fpstreams.rows.from_arrow(
            pa.table(
                {
                    "left": [1, 2],
                    "right": [3, 4],
                    "unused": [5, 6],
                }
            )
        )
        .select("right", renamed="left")
        .to_list()
    )

    assert result == [
        {"right": 3, "renamed": 1},
        {"right": 4, "renamed": 2},
    ]


@pytest.mark.parametrize("terminal", ["to_list", "first"])
def test_arrow_prefix_not_implemented_falls_back_per_batch(
    monkeypatch: pytest.MonkeyPatch, terminal: str
) -> None:
    """An unavailable Arrow kernel keeps ordinary and early terminals on Python semantics."""
    from fpstreams.execution import arrow as arrow_execution

    def reject_lowering(_node: object, _batch: object) -> object:
        raise NotImplementedError("kernel unavailable")

    monkeypatch.setattr(arrow_execution, "lower_row_expression", reject_lowering)
    query = fpstreams.rows.from_arrow(pa.table({"value": [1, 3, 5]}), batch_size=1).where(
        fpstreams.col("value") == 3
    )

    result = query.to_list() if terminal == "to_list" else query.first()

    assert result == ([{"value": 3}] if terminal == "to_list" else {"value": 3})


def test_arrow_prefix_memory_error_still_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Resource exhaustion is not reclassified as an unsupported Arrow kernel."""
    from fpstreams.execution import arrow as arrow_execution

    def fail_lowering(_node: object, _batch: object) -> object:
        raise MemoryError("prefix allocation")

    def forbidden_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("MemoryError must not fall back to rows")

    monkeypatch.setattr(arrow_execution, "lower_row_expression", fail_lowering)
    monkeypatch.setattr(arrow_execution, "batch_to_rows", forbidden_rows)
    query = fpstreams.rows.from_arrow(pa.table({"value": [1]})).where(fpstreams.col("value") == 1)

    with pytest.raises(MemoryError, match="prefix allocation"):
        query.to_list()


@pytest.mark.parametrize(
    ("terminal", "expected"),
    [
        ("sum", 10),
        ("min", 1),
        ("max", 4),
        ("mean", 2.5),
        ("variance", 5 / 3),
        ("std", math.sqrt(5 / 3)),
    ],
)
def test_arrow_scalar_reductions_consume_a_safe_columnar_prefix(
    monkeypatch: pytest.MonkeyPatch,
    terminal: str,
    expected: object,
) -> None:
    """Full-scan scalar terminals should keep a proven Arrow map on its columnar path."""
    from fpstreams.execution import arrow as arrow_execution
    from fpstreams.planning.compiler import compile_query
    from fpstreams.planning.source import Source

    query = fpstreams.flow.from_arrow(
        pa.table({"value": [1, 2, 3, 4], "unused": [9, 8, 7, 6]}),
        batch_size=2,
    ).map(fpstreams.col("value"))
    input_source = query._pipeline.source
    compiled_terminal = "statistics" if terminal in {"mean", "variance", "std"} else terminal
    physical = compile_query(
        query._query(compiled_terminal, 0) if terminal == "sum" else query._query(compiled_terminal)
    )
    assert physical.backend_payload is not None
    assert physical.backend_payload.arrow_prefix is not None

    source_open = Source.open

    def reject_python_rows(candidate: Source[object]) -> Iterator[object]:
        if candidate is input_source:
            raise AssertionError("safe scalar reduction opened Python Arrow rows")
        return source_open(candidate)

    def reject_full_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("safe scalar reduction boxed full Arrow rows")

    if terminal != "mean":
        monkeypatch.setattr(Source, "open", reject_python_rows)
    monkeypatch.setattr(arrow_execution, "batch_to_rows", reject_full_rows)

    if terminal == "mean":
        execution = query.run_with_report("mean")
        result = execution.value
        assert execution.report.strategy == "arrow_direct"
    else:
        result = getattr(query, terminal)()
    if isinstance(expected, float):
        assert result == pytest.approx(expected)
    else:
        assert result == expected


@pytest.mark.parametrize(
    ("terminal", "expected"),
    [("sum", 4), ("min", -1), ("max", 3)],
)
def test_arrow_i64_scalar_reductions_do_not_stream_python_scalars(
    monkeypatch: pytest.MonkeyPatch,
    terminal: str,
    expected: int,
) -> None:
    """Exact i64 terminals should reduce Arrow batches without yielding each scalar."""
    from fpstreams.execution import arrow as arrow_execution
    from fpstreams.tabular import arrow as arrow_adapter

    def reject_scalar_prefix(*_args: object) -> Iterator[object]:
        raise AssertionError("exact i64 reduction streamed a Python scalar prefix")

    def reject_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("exact i64 reduction boxed Python rows")

    monkeypatch.setattr(arrow_execution, "_execute_batch_program", reject_scalar_prefix)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", reject_rows)
    query = fpstreams.flow.from_arrow(
        pa.table(
            {
                "value": pa.array([2, 3, -1], type=pa.int64()),
                "unused": [20, 30, -10],
            }
        ),
        batch_size=1,
    ).map(fpstreams.col("value"))

    assert getattr(query, terminal)() == expected


@pytest.mark.parametrize("terminal", ["sum", "min", "max", "mean", "variance", "std"])
def test_arrow_scalar_reductions_report_their_columnar_strategy(terminal: str) -> None:
    """Public execution reports must identify the Arrow route that actually ran."""
    query = fpstreams.flow.from_arrow(
        pa.table({"value": pa.array([2, 3, -1], type=pa.int64())})
    ).map(fpstreams.col("value"))

    execution = query.run_with_report(terminal)

    assert execution.report.strategy == "arrow_direct"
    assert "Arrow" in execution.report.reason


@pytest.mark.parametrize("adapter", ["record_batch", "reader"])
@pytest.mark.parametrize(
    ("terminal", "expected"),
    [("sum", 4), ("min", -1), ("max", 3)],
)
def test_arrow_i64_scalar_reductions_cover_record_batches_and_readers(
    monkeypatch: pytest.MonkeyPatch,
    adapter: str,
    terminal: str,
    expected: int,
) -> None:
    """Record batches and one-shot readers should use the same exact i64 reduction."""
    from fpstreams.execution import arrow as arrow_execution
    from fpstreams.tabular import arrow as arrow_adapter

    batch = pa.record_batch({"value": pa.array([2, 3, -1], type=pa.int64())})
    source = (
        batch
        if adapter == "record_batch"
        else pa.RecordBatchReader.from_batches(batch.schema, [batch])
    )

    def reject_scalar_prefix(*_args: object) -> Iterator[object]:
        raise AssertionError("exact i64 reduction streamed a Python scalar prefix")

    def reject_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("exact i64 reduction boxed Python rows")

    monkeypatch.setattr(arrow_execution, "_execute_batch_program", reject_scalar_prefix)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", reject_rows)
    query = fpstreams.flow.from_arrow(source, batch_size=1).map(fpstreams.col("value"))

    assert getattr(query, terminal)() == expected


@pytest.mark.parametrize(
    ("terminal", "expected"),
    [("sum", 8_256), ("min", 0), ("max", 128)],
)
def test_arrow_i64_scalar_reduction_backend_decline_reopens_retained_rows(
    monkeypatch: pytest.MonkeyPatch,
    terminal: str,
    expected: int,
) -> None:
    """A recoverable retained-table kernel error should use the canonical prefix once."""
    from fpstreams.execution import arrow as arrow_execution

    pa_module, pc_module = arrow_execution._arrow_modules()

    class RejectingCompute:
        def min_max(self, _values: object) -> object:
            raise ValueError("retained reduction decline")

        def __getattr__(self, name: str) -> object:
            return getattr(pc_module, name)

    monkeypatch.setattr(arrow_execution, "_arrow_modules", lambda: (pa_module, RejectingCompute()))
    query = fpstreams.flow.from_arrow(
        pa.table({"value": pa.array(range(129), type=pa.int64())})
    ).map(fpstreams.col("value"))

    assert getattr(query, terminal)() == expected


@pytest.mark.parametrize("terminal", ["sum", "min", "max"])
def test_arrow_i64_reader_reduction_propagates_after_claim_and_closes(
    monkeypatch: pytest.MonkeyPatch,
    terminal: str,
) -> None:
    """A claimed one-shot reader must close and expose its reduction backend error."""
    from fpstreams.execution import arrow as arrow_execution
    from fpstreams.planning.arrow_source import ArrowBatchSource
    from fpstreams.planning.source import Source, SourceCapabilities

    events: list[str] = []
    batch = pa.record_batch({"value": pa.array(range(129), type=pa.int64())})
    descriptor = ArrowBatchSource(
        lambda: _TrackedArrowBatches([batch], events),
        "reader",
        65_536,
        batch.schema,
        False,
    )

    def forbidden_rows() -> Iterator[dict[str, int]]:
        raise AssertionError("claimed reader reduction reopened Python rows")
        yield

    pa_module, pc_module = arrow_execution._arrow_modules()

    class RejectingCompute:
        def min_max(self, _values: object) -> object:
            raise ValueError("reader reduction failure")

        def __getattr__(self, name: str) -> object:
            return getattr(pc_module, name)

    monkeypatch.setattr(arrow_execution, "_arrow_modules", lambda: (pa_module, RejectingCompute()))
    query = fpstreams.Flow(
        Source(
            forbidden_rows,
            SourceCapabilities(reiterable=False, exact_size=None),
            native_data=descriptor,
        )
    ).map(fpstreams.col("value"))

    with pytest.raises(ValueError, match="reader reduction failure"):
        getattr(query, terminal)()
    assert events == ["pull:0", "close"]
    with pytest.raises(fpstreams.FlowConsumedError):
        getattr(query, terminal)()


@pytest.mark.parametrize(
    ("terminal", "error_type"),
    [
        ("sum", UnicodeDecodeError),
        ("min", fpstreams.EmptyFlowError),
        ("max", fpstreams.EmptyFlowError),
        ("mean", UnicodeDecodeError),
    ],
)
def test_arrow_i64_reader_reduction_preserves_source_value_error_handling(
    terminal: str,
    error_type: type[Exception],
) -> None:
    """Claimed reduction must retain each terminal's public source-error translation."""
    invalid_utf8 = pa.array([b"\xff"], type=pa.binary()).view(pa.string())
    batch = pa.record_batch({"value": pa.array([7], type=pa.int64()), "unused": invalid_utf8})

    def run(engine: str) -> object:
        reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
        query = fpstreams.flow.from_arrow(reader).map(fpstreams.col("value"))
        return getattr(query.with_engine(engine), terminal)()

    with pytest.raises(error_type) as canonical_error:
        run("python")
    with pytest.raises(error_type) as automatic_error:
        run("auto")
    assert str(automatic_error.value) == str(canonical_error.value)


@pytest.mark.parametrize(
    ("arrow_type", "items", "expected"),
    [
        (pa.int64(), [2**53 + 1, -(2**53)], 0.0),
        (pa.float64(), [1e16, 1.0, -1e16], 1.0 / 3.0),
    ],
)
def test_direct_arrow_mean_preserves_cross_batch_compensation_without_row_execution(
    monkeypatch: pytest.MonkeyPatch,
    arrow_type: pa.DataType,
    items: list[int] | list[float],
    expected: float,
) -> None:
    """A direct numeric field mean should retain one compensated state across batches."""
    from fpstreams.execution import arrow as arrow_execution

    batches = [pa.record_batch({"value": pa.array([value], type=arrow_type)}) for value in items]

    def reject_row_execution(*_args: object, **_kwargs: object) -> Iterator[object]:
        raise AssertionError("direct Arrow mean must not execute the row-oriented prefix")

    monkeypatch.setattr(arrow_execution, "execute_with_arrow_prefix", reject_row_execution)
    values = fpstreams.flow.from_arrow(pa.Table.from_batches(batches), batch_size=1).map(
        fpstreams.col("value")
    )

    assert values.mean() == expected


@pytest.mark.parametrize("arrow_type", [pa.int64(), pa.float64()])
def test_direct_arrow_mean_preserves_empty_and_null_contracts(
    monkeypatch: pytest.MonkeyPatch,
    arrow_type: pa.DataType,
) -> None:
    """The batch kernel must retain the public empty and null-value outcomes."""
    from fpstreams.execution import arrow as arrow_execution

    def reject_row_execution(*_args: object, **_kwargs: object) -> Iterator[object]:
        raise AssertionError("direct Arrow mean must not execute the row-oriented prefix")

    monkeypatch.setattr(arrow_execution, "execute_with_arrow_prefix", reject_row_execution)
    empty = fpstreams.flow.from_arrow(pa.table({"value": pa.array([], type=arrow_type)})).map(
        fpstreams.col("value")
    )
    containing_null = fpstreams.flow.from_arrow(
        pa.table({"value": pa.array([1, None], type=arrow_type)})
    ).map(fpstreams.col("value"))

    assert empty.mean() is None
    with pytest.raises(TypeError, match="statistics require real numeric values"):
        containing_null.mean()


def test_direct_arrow_mean_missing_native_endpoint_falls_back_before_reader_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An older native module must leave a one-shot Arrow reader available to Python."""
    from fpstreams import _native

    monkeypatch.setattr(_native, "update_mean_i64_buffer_v1", None)
    batch = pa.record_batch({"value": pa.array([1, 2, 3], type=pa.int64())})
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    values = fpstreams.flow.from_arrow(reader).map(fpstreams.col("value"))

    assert values.mean() == 2.0


@pytest.mark.parametrize("dependency", ["abs", "isfinite"])
def test_direct_arrow_mean_preserves_dynamic_compensated_mean_dependencies(
    monkeypatch: pytest.MonkeyPatch,
    dependency: str,
) -> None:
    """Replacing a Python mean dependency must deopt the direct Arrow kernel."""
    import builtins
    from types import SimpleNamespace

    from fpstreams.collecting import statistics as statistics_module

    table = pa.table({"value": pa.array([1.0, 2.0], type=pa.float64())})
    automatic = fpstreams.flow.from_arrow(table).map(fpstreams.col("value"))
    canonical = automatic.with_engine("python")

    def observed(_value: object) -> object:
        raise RuntimeError(f"observed {dependency}")

    if dependency == "abs":
        monkeypatch.setattr(builtins, "abs", observed)
    else:
        monkeypatch.setattr(statistics_module, "math", SimpleNamespace(isfinite=observed))

    with pytest.raises(RuntimeError, match=f"observed {dependency}"):
        canonical.mean()
    with pytest.raises(RuntimeError, match=f"observed {dependency}"):
        automatic.mean()


@pytest.mark.parametrize(
    ("terminal", "error_type"),
    [
        ("sum", ValueError),
        ("min", fpstreams.EmptyFlowError),
        ("max", fpstreams.EmptyFlowError),
        ("mean", ValueError),
    ],
)
def test_arrow_i64_reader_reduction_preserves_late_pull_value_error(
    terminal: str,
    error_type: type[Exception],
) -> None:
    """A later reader pull failure keeps terminal translation, closure, and consumption."""
    from fpstreams.planning.arrow_source import ArrowBatchSource, batch_to_rows
    from fpstreams.planning.source import Source, SourceCapabilities

    batch = pa.record_batch({"value": pa.array([7], type=pa.int64())})

    def build(engine: str) -> tuple[fpstreams.Flow[object], list[str]]:
        events: list[str] = []

        class LatePullFailure:
            def __init__(self) -> None:
                self.pulls = 0

            def __iter__(self) -> LatePullFailure:
                return self

            def __next__(self) -> object:
                index = self.pulls
                self.pulls += 1
                events.append(f"pull:{index}")
                if index == 0:
                    return batch
                raise ValueError("reader pull failure")

            def close(self) -> None:
                events.append("close")

        descriptor = ArrowBatchSource(
            LatePullFailure,
            "reader",
            65_536,
            batch.schema,
            False,
        )

        def rows() -> Iterator[dict[str, object]]:
            batches = descriptor.open_batches()
            try:
                for current in batches:
                    yield from batch_to_rows(current)
            finally:
                batches.close()  # type: ignore[attr-defined]

        source = Source(
            rows,
            SourceCapabilities(reiterable=False, exact_size=None),
            native_data=descriptor,
        )
        return fpstreams.Flow(source).map(fpstreams.col("value")).with_engine(engine), events

    canonical, canonical_events = build("python")
    with pytest.raises(error_type) as canonical_error:
        getattr(canonical, terminal)()
    assert canonical_events == ["pull:0", "pull:1", "close"]

    automatic, automatic_events = build("auto")
    with pytest.raises(error_type) as automatic_error:
        getattr(automatic, terminal)()
    assert str(automatic_error.value) == str(canonical_error.value)
    assert automatic_events == canonical_events
    with pytest.raises(fpstreams.FlowConsumedError):
        getattr(automatic, terminal)()
    assert automatic_events == canonical_events


@pytest.mark.parametrize(
    ("terminal", "error_type"),
    [
        ("sum", ValueError),
        ("min", fpstreams.EmptyFlowError),
        ("max", fpstreams.EmptyFlowError),
        ("mean", ValueError),
    ],
)
def test_arrow_i64_reader_reduction_preserves_synchronous_opener_value_error(
    terminal: str,
    error_type: type[Exception],
) -> None:
    """A synchronous reader opener failure retains translation and spends the source once."""
    from fpstreams.planning.arrow_source import ArrowBatchSource, batch_to_rows
    from fpstreams.planning.source import Source, SourceCapabilities

    schema = pa.schema([pa.field("value", pa.int64())])

    def build(engine: str) -> tuple[fpstreams.Flow[object], list[str]]:
        events: list[str] = []

        def open_batches() -> Iterator[object]:
            events.append("open")
            raise ValueError("reader opener failure")

        descriptor = ArrowBatchSource(
            open_batches,
            "reader",
            65_536,
            schema,
            False,
        )

        def rows() -> Iterator[dict[str, object]]:
            batches = descriptor.open_batches()
            try:
                for batch in batches:
                    yield from batch_to_rows(batch)
            finally:
                batches.close()  # type: ignore[attr-defined]

        source = Source(
            rows,
            SourceCapabilities(reiterable=False, exact_size=None),
            native_data=descriptor,
        )
        return fpstreams.Flow(source).map(fpstreams.col("value")).with_engine(engine), events

    canonical, canonical_events = build("python")
    with pytest.raises(error_type) as canonical_error:
        getattr(canonical, terminal)()
    assert canonical_events == ["open"]

    automatic, automatic_events = build("auto")
    with pytest.raises(error_type) as automatic_error:
        getattr(automatic, terminal)()
    assert str(automatic_error.value) == str(canonical_error.value)
    assert automatic_events == canonical_events
    with pytest.raises(fpstreams.FlowConsumedError):
        getattr(automatic, terminal)()
    assert automatic_events == canonical_events


def test_arrow_scalar_reduction_prefix_rejects_non_total_or_parallel_shapes() -> None:
    """Reduction pushdown must not batch-evaluate filters, arithmetic, or parallel work."""
    from fpstreams.planning.compiler import compile_query

    source = fpstreams.flow.from_arrow(pa.table({"value": [1, 2, 3]}))
    rejected = (
        source.map(fpstreams.col("value") + 1),
        source.filter(fpstreams.col("value") > 1).map(fpstreams.col("value")),
        source.parallel(backend="thread", workers=1).map(fpstreams.col("value")),
        source.with_engine("python").map(fpstreams.col("value")),
    )

    for query in rejected:
        physical = compile_query(query._query("sum", 0))
        assert physical.backend_payload is not None
        assert physical.backend_payload.arrow_prefix is None

    direct = source.map(fpstreams.col("value"))
    invalid_start = compile_query(direct._query("sum", ""))
    assert invalid_start.backend_payload is not None
    assert invalid_start.backend_payload.arrow_prefix is None


@pytest.mark.parametrize("source_kind", ["list", "forced_list", "arrow", "forced_arrow"])
def test_arrow_scalar_reduction_planning_does_not_probe_opaque_non_arrow_callables(
    source_kind: str,
) -> None:
    """Declined reduction planning must not execute user-defined attribute access."""

    class Probe:
        def __getattribute__(self, name: str) -> object:
            if name == "_node":
                raise RuntimeError("planning touched _node")
            return object.__getattribute__(self, name)

        def __call__(self, _value: object) -> object:
            raise AssertionError("an empty flow must not invoke its mapper")

    if source_kind in {"list", "forced_list"}:
        source = fpstreams.flow([])
        if source_kind == "forced_list":
            source = source.with_engine("python")
    else:
        source = fpstreams.flow.from_arrow(pa.table({"value": pa.array([], type=pa.int64())}))
        if source_kind == "forced_arrow":
            source = source.with_engine("python")

    assert source.map(Probe()).sum() == 0


def test_arrow_scalar_reduction_prefix_preserves_empty_missing_and_null_semantics() -> None:
    """Runtime guards keep direct-field reduction behavior identical to Python rows."""
    empty = fpstreams.flow.from_arrow(pa.table({"present": pa.array([], type=pa.int64())})).map(
        fpstreams.col("missing")
    )
    assert empty.sum() == 0
    assert empty.mean() is None

    missing = fpstreams.flow.from_arrow(pa.table({"present": [1]})).map(fpstreams.col("missing"))
    with pytest.raises(fpstreams.SelectionError) as automatic_error:
        missing.sum()
    with pytest.raises(fpstreams.SelectionError) as python_error:
        missing.with_engine("python").sum()
    assert str(automatic_error.value) == str(python_error.value)

    nullable = fpstreams.flow.from_arrow(
        pa.table({"value": pa.array([None, 1], type=pa.int64())})
    ).map(fpstreams.col("value"))
    with pytest.raises(TypeError) as automatic_null_error:
        nullable.sum()
    with pytest.raises(TypeError) as python_null_error:
        nullable.with_engine("python").sum()
    assert str(automatic_null_error.value) == str(python_null_error.value)


@pytest.mark.parametrize("name", ["arrow.reader.after", "arrow.batch.after"])
def test_transformed_arrow_prefix_observes_active_reader_failpoints(name: str) -> None:
    """A transformed query keeps the canonical reader instrumentation boundaries visible."""
    from fpstreams.runtime.failpoints import failpoint

    query = fpstreams.rows.from_arrow(pa.table({"value": [1]})).where(fpstreams.col("value") == 1)

    with (
        failpoint(name, RuntimeError("transformed reader")),
        pytest.raises(RuntimeError, match="transformed reader"),
    ):
        query.to_list()


def test_arrow_direct_filter_then_select_stays_columnar_until_projection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A proven int64 comparison filters the batch before selected rows are boxed."""
    from fpstreams.execution import arrow as arrow_execution

    def reject_full_batch(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("filter/select must not materialize full Arrow rows")

    monkeypatch.setattr(arrow_execution, "batch_to_rows", reject_full_batch)
    result = (
        fpstreams.rows.from_arrow(
            pa.table(
                {
                    "id": [0, 1, 2],
                    "payload": ["zero", "one", "two"],
                    "unused": [10, 20, 30],
                }
            )
        )
        .where(fpstreams.col("id") >= 1)
        .select("payload")
        .to_list()
    )

    assert result == [{"payload": "one"}, {"payload": "two"}]


def test_arrow_filter_then_select_preserves_python_arithmetic_and_cast_semantics() -> None:
    """A projection must not expose Arrow overflow, integer division, or cast differences."""
    maximum = 2**63 - 1
    minimum = -(2**63)
    cases = (
        ({"x": [maximum]}, fpstreams.col("x") + 1 > 0, [{"x": maximum}]),
        ({"x": [minimum]}, fpstreams.col("x") - 1 < 0, [{"x": minimum}]),
        ({"x": [maximum]}, fpstreams.col("x") * 2 > 0, [{"x": maximum}]),
        ({"x": [minimum]}, -fpstreams.col("x") > 0, [{"x": minimum}]),
        ({"x": [minimum]}, abs(fpstreams.col("x")) > 0, [{"x": minimum}]),
        ({"x": [3]}, fpstreams.col("x") / 2 > 1.4, [{"x": 3}]),
        ({"x": ["false"]}, fpstreams.col("x").cast(bool), [{"x": "false"}]),
    )

    for columns, predicate, expected in cases:
        assert (
            fpstreams.rows.from_arrow(pa.table(columns)).where(predicate).select("x").to_list()
        ) == expected


def test_arrow_filter_then_select_keeps_null_errors_scoped_to_each_field() -> None:
    """A null test or coalesce on y cannot make a nullable x comparison Arrow-safe."""
    table = pa.table({"x": [None], "y": [None]})
    predicates = (
        (fpstreams.col("x") > 0) & fpstreams.col("y").is_null(),
        (fpstreams.col("x") > 0) & (fpstreams.col("y").fill_null(0) == 0),
    )

    for predicate in predicates:
        with pytest.raises(TypeError, match="not supported"):
            fpstreams.rows.from_arrow(table).where(predicate).select("x").to_list()


def test_arrow_filter_then_select_keeps_custom_literal_operator_protocol() -> None:
    """A direct projection cannot scalarize a literal whose subclass owns comparison."""

    class WeirdInt(int):
        def __new__(cls, value: int) -> WeirdInt:
            instance = super().__new__(cls, value)
            instance.seen = []
            return instance

        def __eq__(self, other: object) -> bool:
            self.seen.append(other)
            return True

    literal = WeirdInt(999)
    result = (
        fpstreams.rows.from_arrow(pa.table({"x": [1]}))
        .where(fpstreams.col("x") == literal)
        .select("x")
        .to_list()
    )

    assert result == [{"x": 1}]
    assert literal.seen == [1]


def test_arrow_multiple_filters_keep_python_integer_overflow_semantics() -> None:
    """Adding another RowExpr filter must not enable unchecked Arrow arithmetic."""
    maximum = 2**63 - 1
    rows = fpstreams.rows.from_arrow(pa.table({"x": [maximum]}))

    assert rows.where(fpstreams.col("x") > 0).where(fpstreams.col("x") + 1 > 0).to_list() == [
        {"x": maximum}
    ]


def test_arrow_multiple_direct_filters_validate_every_filter_before_batch_execution() -> None:
    """A later unsafe primitive comparison sends the original batch through Python."""
    nullable = fpstreams.rows.from_arrow(
        pa.table({"keep": [1, 1], "value": pa.array([None, 1], type=pa.int64())})
    )
    with pytest.raises(TypeError, match="not supported"):
        nullable.where(fpstreams.col("keep") == 1).where(fpstreams.col("value") > 0).count()

    missing_after_no_survivors = fpstreams.rows.from_arrow(pa.table({"value": [0, 1]}))
    assert (
        missing_after_no_survivors.where(fpstreams.col("value") < 0)
        .where(fpstreams.col("missing") == 1)
        .count()
        == 0
    )


def test_arrow_negated_filter_discards_every_tentative_batch_prefix() -> None:
    """A later reject cannot expose an earlier Arrow expression's overflow semantics."""
    maximum = 2**63 - 1
    source = fpstreams.rows.from_arrow(pa.table({"x": [maximum], "enabled": [False]}))._flow
    query = fpstreams.Rows(
        source.filter(fpstreams.col("x") + 1 > 0).reject(fpstreams.col("enabled"))
    ).select("x")

    assert query.to_list() == [{"x": maximum}]


def test_arrow_rows_wrappers_discard_every_tentative_batch_prefix() -> None:
    """A Rows copy, equality, or path selector keeps its leading RowExpr on Python."""
    maximum = 2**63 - 1
    source = fpstreams.rows.from_arrow(pa.table({"x": [maximum], "nested": [{"value": 7}]})).where(
        fpstreams.col("x") + 1 > 0
    )

    assert source.with_columns(copy="x").to_list() == [
        {"x": maximum, "nested": {"value": 7}, "copy": maximum}
    ]
    assert source.where(x=maximum).to_list() == [{"x": maximum, "nested": {"value": 7}}]
    assert source.select(value="nested.value").to_list() == [{"value": 7}]


def test_arrow_exact_select_preserves_alias_order_and_converted_value_identity() -> None:
    """Repeated inputs are converted once and reused in declaration-ordered output fields."""
    payload = b"payload-" * 256
    result = (
        fpstreams.rows.from_arrow(pa.table({"payload": [payload], "unused": [1]}))
        .select(first="payload", second="payload")
        .to_list()
    )

    assert list(result[0]) == ["first", "second"]
    assert result[0]["first"] == payload
    assert result[0]["first"] is result[0]["second"]


def test_arrow_exact_select_keeps_missing_field_timing_and_canonical_error() -> None:
    """Schema knowledge cannot make an empty select fail or replace SelectionError on rows."""
    with pytest.raises(fpstreams.SelectionError) as canonical:
        fpstreams.rows([{"present": 1}]).select("missing").to_list()
    with pytest.raises(fpstreams.SelectionError) as projected:
        fpstreams.rows.from_arrow(pa.table({"present": [1]})).select("missing").to_list()

    assert str(projected.value) == str(canonical.value)
    assert type(projected.value.__cause__) is type(canonical.value.__cause__) is KeyError
    empty = pa.table({"present": pa.array([], type=pa.int64())})
    assert fpstreams.rows.from_arrow(empty).select("missing").to_list() == []


def test_arrow_projection_closes_and_claims_its_one_shot_batch_source_on_error() -> None:
    """A projection fallback error closes the opened batches and consumes the source once."""
    from fpstreams.planning.arrow_source import ArrowBatchSource
    from fpstreams.planning.source import Source, SourceCapabilities
    from fpstreams.streams.flow import Flow

    batch = pa.record_batch({"present": [1]})
    events: list[str] = []

    def batches() -> Iterator[object]:
        events.append("open")
        try:
            events.append("batch")
            yield batch
            events.append("unexpected-tail")
            yield batch
        finally:
            events.append("close")

    descriptor = ArrowBatchSource(batches, "reader", 1, batch.schema, False)
    source = Source(
        lambda: iter(()),
        SourceCapabilities(reiterable=False, exact_size=None),
        native_data=descriptor,
    )
    projected = fpstreams.Rows(Flow(source)).select("missing")

    with pytest.raises(fpstreams.SelectionError):
        projected.to_list()
    assert events == ["open", "batch", "close"]
    with pytest.raises(fpstreams.FlowConsumedError):
        projected.to_list()


def test_arrow_select_iteration_and_early_terminals_keep_canonical_batch_pulls() -> None:
    """Iteration stays row-latency-owned, while first/take close before a second batch."""
    from fpstreams.planning.arrow_source import ArrowBatchSource
    from fpstreams.planning.source import Source, SourceCapabilities
    from fpstreams.streams.flow import Flow

    batches = (
        pa.record_batch({"id": [1], "unused": [10]}),
        pa.record_batch({"id": [2], "unused": [20]}),
    )

    def tracked_rows() -> tuple[fpstreams.Rows[dict[str, int]], list[str]]:
        events: list[str] = []

        def open_batches() -> Iterator[object]:
            events.append("open")
            try:
                for index, batch in enumerate(batches):
                    events.append(f"batch:{index}")
                    yield batch
            finally:
                events.append("close")

        descriptor = ArrowBatchSource(open_batches, "reader", 1, batches[0].schema, False)

        def python_rows() -> Iterator[dict[str, int]]:
            values = descriptor.open_batches()
            try:
                for batch in values:
                    yield from batch.to_pylist()
            finally:
                values.close()

        source = Source(
            python_rows,
            SourceCapabilities(reiterable=False, exact_size=None),
            native_data=descriptor,
        )
        return fpstreams.Rows(Flow(source)).select("id"), events

    iterated, events = tracked_rows()
    assert list(iterated) == [{"id": 1}, {"id": 2}]
    assert events == ["open", "batch:0", "batch:1", "close"]
    with pytest.raises(fpstreams.FlowConsumedError):
        list(iterated)

    first, events = tracked_rows()
    assert first.first() == {"id": 1}
    assert events == ["open", "batch:0", "close"]

    taken, events = tracked_rows()
    assert taken.take(1).to_list() == [{"id": 1}]
    assert events == ["open", "batch:0", "close"]


def test_arrow_first_boxes_only_one_csv_or_parquet_row(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The specialized terminal slices its first surviving batch before Python conversion."""
    from fpstreams.execution import arrow as arrow_execution
    from fpstreams.planning.arrow_source import batch_to_rows as convert_rows

    csv_path = tmp_path / "first.csv"
    csv_path.write_text("id,payload\n1,one\n2,two\n3,three\n", encoding="utf-8")
    parquet_path = tmp_path / "first.parquet"
    pq.write_table(
        pa.table({"id": list(range(100)), "payload": [f"value-{i}" for i in range(100)]}),
        parquet_path,
    )
    converted_sizes: list[int] = []

    def tracked(batch: object) -> list[dict[str, object]]:
        converted_sizes.append(batch.num_rows)  # type: ignore[attr-defined]
        return convert_rows(batch)

    monkeypatch.setattr(arrow_execution, "batch_to_rows", tracked)

    assert fpstreams.rows.scan_csv(csv_path).first() == {"id": 1, "payload": "one"}
    assert converted_sizes == [1]
    converted_sizes.clear()
    assert fpstreams.rows.from_parquet(parquet_path).first() == {
        "id": 0,
        "payload": "value-0",
    }
    assert converted_sizes == [1]


def test_arrow_first_close_failure_does_not_replace_result_or_primary_error() -> None:
    """Best-effort cleanup preserves both a found row and the query's own exception."""
    from fpstreams.planning.arrow_source import ArrowBatchSource
    from fpstreams.planning.source import Source, SourceCapabilities
    from fpstreams.streams.flow import Flow

    batch = pa.record_batch({"id": [1]})
    closed: list[str] = []

    class ClosingBatches(Iterator[object]):
        def __init__(self) -> None:
            self._yielded = False

        def __next__(self) -> object:
            if self._yielded:
                raise StopIteration
            self._yielded = True
            return batch

        def close(self) -> None:
            closed.append("close")
            raise RuntimeError("close failed")

    def query(field: str) -> fpstreams.Rows[dict[str, object]]:
        descriptor = ArrowBatchSource(
            ClosingBatches,
            "reader",
            1,
            batch.schema,
            reiterable=True,
        )
        source = Source(
            lambda: iter(({"id": 1},)),
            SourceCapabilities(reiterable=True, exact_size=1),
            native_data=descriptor,
        )
        return fpstreams.Rows(Flow(source)).select(field)

    assert query("id").first() == {"id": 1}
    with pytest.raises(fpstreams.SelectionError):
        query("missing").first()
    assert closed == ["close", "close"]


def test_arrow_first_preserves_memory_error_from_batch_safety(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Resource exhaustion cannot be mistaken for an ordinary unsupported kernel."""
    from fpstreams.execution import arrow as arrow_execution

    query = fpstreams.rows.from_arrow(pa.table({"id": [1]})).where(fpstreams.col("id") == 1)

    def out_of_memory() -> object:
        raise MemoryError("batch safety allocation failed")

    monkeypatch.setattr(arrow_execution, "_arrow_modules", out_of_memory)

    with pytest.raises(MemoryError, match="batch safety allocation failed"):
        query.first()


def test_arrow_first_keeps_equality_projection_and_python_fallback_semantics(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Exact equality may stay columnar, while null and cross-type cases remain Python-owned."""
    from fpstreams.execution import arrow as arrow_execution

    path = tmp_path / "equality-first.parquet"
    pq.write_table(
        pa.table({"id": [0, 1, 2], "payload": ["zero", "one", "two"]}),
        path,
    )
    projected_sizes: list[int] = []
    project_rows = arrow_execution._project_batch_rows

    def tracked_projection(batch: object, projection: object) -> Iterator[dict[str, object]]:
        projected_sizes.append(batch.num_rows)  # type: ignore[attr-defined]
        yield from project_rows(batch, projection)  # type: ignore[arg-type]

    monkeypatch.setattr(arrow_execution, "_project_batch_rows", tracked_projection)

    assert fpstreams.rows.from_parquet(path).where(fpstreams.col("id") == 2).select(
        "payload"
    ).first() == {"payload": "two"}
    assert projected_sizes == [1]
    assert (
        fpstreams.rows.from_parquet(path)
        .where(fpstreams.col("id") == True)  # noqa: E712 - intentional Python bool/int equality
        .select("payload")
        .first()
        == {"payload": "one"}
    )
    nullable = pa.table({"id": [None, 1], "payload": ["null", "one"]})
    assert fpstreams.rows.from_arrow(nullable).where(fpstreams.col("id") == 1).select(
        "payload"
    ).first() == {"payload": "one"}


def test_arrow_first_preserves_missing_fields_public_columns_and_unsafe_short_circuit(
    tmp_path: Path,
) -> None:
    """Terminal specialization cannot make source errors eager or evaluate a later unsafe row."""
    table = pa.table({"x": [1, 0]})
    assert fpstreams.rows.from_arrow(table).where((10 / fpstreams.col("x")) > 0).first() == {"x": 1}

    with pytest.raises(fpstreams.SelectionError) as canonical:
        fpstreams.rows([{"present": 1}]).select("missing").first()
    with pytest.raises(fpstreams.SelectionError) as projected:
        fpstreams.rows.from_arrow(pa.table({"present": [1]})).select("missing").first()
    assert str(projected.value) == str(canonical.value)
    assert type(projected.value.__cause__) is type(canonical.value.__cause__) is KeyError

    path = tmp_path / "invalid-first-columns.parquet"
    pq.write_table(pa.table({"present": [1]}), path)
    invalid = fpstreams.rows.from_parquet(path, columns=("present", "missing"))
    with pytest.raises(pa.ArrowInvalid, match=r"No match for FieldRef.Name\(missing\)"):
        invalid.select("present").first()


def test_arrow_first_bypasses_native_batches_while_a_failpoint_is_active() -> None:
    """The terminal retains canonical transition injection before claiming an Arrow source."""
    from fpstreams.planning.arrow_source import ArrowBatchSource
    from fpstreams.planning.source import Source, SourceCapabilities
    from fpstreams.runtime.failpoints import failpoint
    from fpstreams.streams.flow import Flow

    opened: list[str] = []

    def forbidden_batches() -> Iterator[object]:
        opened.append("arrow")
        raise AssertionError("an active failpoint must bypass the Arrow terminal path")
        yield

    def python_rows() -> Iterator[dict[str, int]]:
        opened.append("python")
        yield {"id": 1, "unused": 10}

    descriptor = ArrowBatchSource(forbidden_batches, "reader", 1, reiterable=False)
    source = Source(
        python_rows,
        SourceCapabilities(reiterable=False, exact_size=None),
        native_data=descriptor,
    )
    query = fpstreams.Rows(Flow(source)).select("id")

    with failpoint("unrelated.transition", RuntimeError("unused")):
        assert query.first() == {"id": 1}
    assert opened == ["python"]


def test_row_expression_ir_covers_operator_and_call_families() -> None:
    row = {
        "value": 3,
        "negative": -4,
        "text": " AbC ",
        "items": [10, 20],
        "nested": {"amount": 5},
        "missing": None,
    }
    expressions = [
        (10 + fpstreams.col("value"), 13),
        (10 - fpstreams.col("value"), 7),
        (4 * fpstreams.col("value"), 12),
        (12 / fpstreams.col("value"), 4),
        (7 // fpstreams.col("value"), 2),
        (7 % fpstreams.col("value"), 1),
        (2 ** fpstreams.col("value"), 8),
        (-fpstreams.col("value"), -3),
        (abs(fpstreams.col("negative")), 4),
        (fpstreams.col("items")[1], 20),
        (fpstreams.col("value").cast(float), 3.0),
        (fpstreams.col("value").isin([1, 3]), True),
        (fpstreams.col("missing").is_null(), True),
        (fpstreams.col("value").is_not_null(), True),
        (fpstreams.col("text").lower(), " abc "),
        (fpstreams.col("text").upper(), " ABC "),
        (fpstreams.col("text").strip(), "AbC"),
        (fpstreams.col("text").contains("b"), True),
        (fpstreams.when(fpstreams.col("value") >= 3, "yes", "no"), "yes"),
        ((fpstreams.col("value") > 0) & (fpstreams.col("missing").is_null()), True),
        ((fpstreams.col("value") < 0) | (fpstreams.col("value") == 3), True),
    ]

    assert [expression(row) for expression, _expected in expressions] == [
        expected for _expression, expected in expressions
    ]
    assert fpstreams.col("nested.amount").inspect().to_dict()["fields"] == ["nested"]
    assert fpstreams.lit(1).inspect().to_dict()["deterministic"] == "yes"
    assert fpstreams.col("missing").coalesce(0).inspect().to_dict()["null_behavior"] == (
        "coalesces"
    )
    assert fpstreams.col("value").map(str).inspect().to_dict() == {
        "fields": None,
        "deterministic": "unknown",
        "pure": "unknown",
        "null_behavior": "python",
        "backends": ["python"],
        "opaque": True,
    }
    with pytest.raises(TypeError, match="'&' or '\\|'"):
        bool(fpstreams.col("value"))


def test_arrow_row_expression_prefix_preserves_python_boundaries() -> None:
    table = pa.table({"value": [1, 2, 3], "enabled": [True, False, True]})
    source = fpstreams.rows.from_arrow(table, batch_size=2)._flow

    assert source.filter(fpstreams.col("value") > 1).to_list() == [
        {"value": 2, "enabled": False},
        {"value": 3, "enabled": True},
    ]
    assert source.filter(fpstreams.col("value") > 1).filter(fpstreams.col("enabled")).to_list() == [
        {"value": 3, "enabled": True}
    ]
    assert source.reject(fpstreams.col("enabled")).to_list() == [{"value": 2, "enabled": False}]
    assert fpstreams.Rows(source.reject(fpstreams.col("enabled"))).select("value").to_list() == [
        {"value": 2}
    ]
    assert source.map(fpstreams.col("value") + 10).to_list() == [11, 12, 13]
    assert source.filter(fpstreams.col("value") > 1).map(fpstreams.col("value") * 10).to_list() == [
        20,
        30,
    ]
    assert source.filter((fpstreams.col("value") % 2) == 0).to_list() == [
        {"value": 2, "enabled": False}
    ]
    assert source.filter(fpstreams.col("enabled") | (fpstreams.col("value") > 1)).count() == 3

    nullable = fpstreams.rows.from_arrow(pa.table({"value": [None, 1]}))._flow
    assert nullable.filter(fpstreams.col("value").is_null()).to_list() == [{"value": None}]
    assert nullable.map(fpstreams.col("value").fill_null(5).cast(float)).to_list() == [5.0, 1.0]

    explanation = source.filter(fpstreams.col("value") > 0).take(1).explain().to_dict()
    assert explanation["arrow_prefix"] == {
        "operation_count": 1,
        "boundary_reason": "unsupported_operation",
        "guarded": True,
    }
    assert explanation["boundaries"] == [
        {
            "from": "arrow",
            "to": "python",
            "after_operation": 1,
            "materializes_rows": True,
            "guarded": True,
        }
    ]


def test_structured_rows_fusion_never_skips_an_arrow_prefix_stage() -> None:
    """A reconstructed Python suffix must use the exact Arrow prefix selected by compilation."""
    result = (
        fpstreams.rows.from_arrow(pa.table({"x": [1, 2], "y": [3, 1]}))
        .with_columns(score=fpstreams.col("x") * 3 + fpstreams.col("y") - 1)
        .where(fpstreams.col("score") % 5 != 0)
        .select("x", "score")
        .to_list()
    )

    assert result == [{"x": 2, "score": 6}]


def test_arrow_batch_guard_names_each_fallback_reason() -> None:
    from fpstreams.execution.arrow import prove_batch_safe
    from fpstreams.planning.arrow import ArrowProjectionSpec
    from fpstreams.planning.sync import FilterOp

    batch = pa.record_batch({"value": [1, 2]})
    nullable = pa.record_batch({"value": [None, 1]})
    operations = [
        (batch, FilterOp(lambda _row: True), "opaque_expression"),
        (batch, FilterOp(fpstreams.col("missing") > 0), "missing_field"),
        (batch, FilterOp(fpstreams.col("value").map(bool)), "incompatible_type"),
        (batch, FilterOp((fpstreams.col("value") // 2) == 1), "incompatible_type"),
        (batch, FilterOp((fpstreams.col("value") / 0) > 1), "zero_divisor"),
        (batch, FilterOp(fpstreams.col("value").lower() == "1"), "incompatible_type"),
        (batch, FilterOp(fpstreams.col("value").cast(str) == "1"), "unsafe_cast"),
        (nullable, FilterOp(fpstreams.col("value") == 1), "null_semantics"),
    ]

    assert [
        prove_batch_safe(item, (operation,)).reason.value for item, operation, _reason in operations
    ] == [reason for _item, _operation, reason in operations]
    projection = ArrowProjectionSpec((("value", "value"),), ("value",))
    nested = pa.record_batch({"value": [[1], [2]]})
    assert prove_batch_safe(nested, (), projection=projection).reason.value == "incompatible_type"
    assert prove_batch_safe(object(), ()).reason.value == "kernel_error"


def test_parquet_sink_streams_row_groups_and_scan_pushes_projection_filter(tmp_path: Path) -> None:
    target = tmp_path / "events.parquet"
    events: list[str] = []

    def records() -> Iterator[dict[str, object]]:
        events.append("open")
        try:
            for value in range(7):
                yield {"id": value, "group": value % 2, "payload": f"value-{value}"}
        finally:
            events.append("close")

    assert fpstreams.rows(records()).to_parquet(target, batch_size=3) == 7
    assert events == ["open", "close"]
    assert pq.ParquetFile(target).metadata.num_row_groups == 3

    scanned = fpstreams.rows.from_parquet(
        target,
        columns=("id", "group"),
        filter=ds.field("id") >= 4,
        batch_size=2,
    )
    expected = [
        {"id": 4, "group": 0},
        {"id": 5, "group": 1},
        {"id": 6, "group": 0},
    ]
    assert scanned.to_list() == expected
    assert scanned.to_list() == expected


def test_parquet_source_keeps_batches_columnar_through_filter_and_projection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A chainable scan must not box every unselected Parquet field into Python rows."""
    from fpstreams.execution import arrow as arrow_execution
    from fpstreams.planning.arrow_source import ArrowBatchSource
    from fpstreams.tabular import arrow as arrow_adapter

    target = tmp_path / "columnar.parquet"
    pq.write_table(
        pa.table(
            {
                "id": [0, 1, 2],
                "payload": ["zero", "one", "two"],
                "unused": [10, 20, 30],
            }
        ),
        target,
    )
    source = fpstreams.rows.from_parquet(target, batch_size=2)
    descriptor = source._flow._pipeline.source.native_data
    assert isinstance(descriptor, ArrowBatchSource)
    assert descriptor.kind == "parquet"

    def reject_full_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("Parquet filter/select must stay columnar until projection")

    monkeypatch.setattr(arrow_execution, "batch_to_rows", reject_full_rows)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", reject_full_rows)

    assert source.where(fpstreams.col("id") >= 1).select("payload").to_list() == [
        {"payload": "one"},
        {"payload": "two"},
    ]


def test_parquet_identity_count_uses_scanner_count_rows_without_batches(
    tmp_path: Path,
) -> None:
    """An identity count should use Parquet metadata instead of opening scan batches."""
    from dataclasses import replace

    from fpstreams.planning.arrow_source import ArrowBatchSource

    target = tmp_path / "metadata-count.parquet"
    pq.write_table(
        pa.table({"id": list(range(10)), "unused": list(range(10, 20))}),
        target,
        row_group_size=2,
    )
    source = fpstreams.rows.from_parquet(
        target,
        columns=("id",),
        filter=ds.field("id") >= 5,
        batch_size=1,
    )
    pipeline_source = source._flow._pipeline.source
    descriptor = pipeline_source.native_data
    assert isinstance(descriptor, ArrowBatchSource)
    count_opener = getattr(descriptor, "count_opener", None)
    assert callable(count_opener)

    def forbidden_batches() -> Iterator[object]:
        raise AssertionError("metadata count must not open Parquet scan batches")
        yield

    pipeline_source.native_data = replace(descriptor, opener=forbidden_batches)

    assert source.count() == 5


def test_parquet_transformed_count_keeps_batch_semantic_guards(tmp_path: Path) -> None:
    """A query filter must not be replaced by the source-only metadata terminal."""
    from dataclasses import replace

    from fpstreams.planning.arrow_source import ArrowBatchSource

    target = tmp_path / "guarded-count.parquet"
    pq.write_table(pa.table({"id": [1, 2, 3]}), target)
    source = fpstreams.rows.from_parquet(target)
    pipeline_source = source._flow._pipeline.source
    descriptor = pipeline_source.native_data
    assert isinstance(descriptor, ArrowBatchSource)

    def forbidden_count() -> int:
        raise AssertionError("transformed count must retain per-batch semantic guards")

    pipeline_source.native_data = replace(descriptor, count_opener=forbidden_count)

    assert source.where(fpstreams.col("id") >= 2).count() == 2
    assert source.select("id").count() == 3
    assert source.with_engine("python").count() == 3


def test_parquet_count_callback_decline_falls_back_to_batches(tmp_path: Path) -> None:
    """An unavailable scanner terminal should retain the established batch count."""
    from dataclasses import replace

    from fpstreams.planning.arrow_source import ArrowBatchSource

    target = tmp_path / "declined-count.parquet"
    pq.write_table(pa.table({"id": [1, 2, 3]}), target)
    source = fpstreams.rows.from_parquet(target, batch_size=1)
    pipeline_source = source._flow._pipeline.source
    descriptor = pipeline_source.native_data
    assert isinstance(descriptor, ArrowBatchSource)
    events: list[str] = []

    def decline_count() -> None:
        events.append("count")

    def tracked_batches() -> Iterator[object]:
        events.append("batches")
        yield from descriptor.opener()

    pipeline_source.native_data = replace(
        descriptor,
        opener=tracked_batches,
        count_opener=decline_count,
    )

    assert source.count() == 3
    assert events == ["count", "batches"]


def test_parquet_metadata_count_respects_failpoints(tmp_path: Path) -> None:
    """Instrumentation forces the canonical source boundary before metadata counting."""
    from dataclasses import replace

    from fpstreams.planning.arrow_source import ArrowBatchSource
    from fpstreams.runtime.failpoints import failpoint

    target = tmp_path / "instrumented-count.parquet"
    pq.write_table(pa.table({"id": [1]}), target)
    source = fpstreams.rows.from_parquet(target)
    pipeline_source = source._flow._pipeline.source
    descriptor = pipeline_source.native_data
    assert isinstance(descriptor, ArrowBatchSource)
    metadata_calls: list[None] = []

    def forbidden_count() -> int:
        metadata_calls.append(None)
        raise AssertionError("active failpoints must bypass metadata counting")

    pipeline_source.native_data = replace(descriptor, count_opener=forbidden_count)

    with (
        failpoint("source.open.after", RuntimeError("canonical parquet count")),
        pytest.raises(RuntimeError, match="canonical parquet count"),
    ):
        source.count()
    assert metadata_calls == []


def test_parquet_metadata_count_preserves_duplicate_schema_error() -> None:
    """Metadata counting must validate duplicate fields like the canonical scanner."""
    table = pa.Table.from_arrays([pa.array([1]), pa.array([2])], names=["dup", "dup"])
    dataset = ds.dataset(table)
    source = fpstreams.rows.from_parquet(dataset)

    with pytest.raises(fpstreams.DuplicateKeyError) as automatic:
        source.count()
    with pytest.raises(fpstreams.DuplicateKeyError) as canonical:
        source.with_engine("python").count()

    assert str(automatic.value) == str(canonical.value)


def test_parquet_query_projection_reaches_scanner_with_filter_dependencies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The scanner reads output and filter fields, but not unrelated Parquet columns."""
    from fpstreams.execution import arrow as arrow_execution

    target = tmp_path / "wide.parquet"
    pq.write_table(
        pa.table(
            {
                "id": [0, 1, 2],
                "payload": ["zero", "one", "two"],
                "unused_a": [10, 20, 30],
                "unused_b": [100, 200, 300],
            }
        ),
        target,
    )
    schemas: list[tuple[str, ...]] = []
    execute_batch_program = arrow_execution._execute_batch_program

    def traced_batch_program(
        batch: object, operations: tuple[object, ...], projection: object
    ) -> Iterator[object]:
        schemas.append(tuple(batch.schema.names))  # type: ignore[attr-defined]
        yield from execute_batch_program(batch, operations, projection)

    monkeypatch.setattr(arrow_execution, "_execute_batch_program", traced_batch_program)

    result = (
        fpstreams.rows.from_parquet(target, batch_size=2)
        .where(fpstreams.col("id") >= 1)
        .select("payload")
        .to_list()
    )

    assert result == [{"payload": "one"}, {"payload": "two"}]
    assert schemas == [("payload", "id"), ("payload", "id")]


def test_parquet_exact_equality_reaches_scanner_and_keeps_the_python_residual(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A proven equality may prune at scan time while the original filter still executes."""
    from fpstreams.execution import arrow as arrow_execution

    target = tmp_path / "equality.parquet"
    pq.write_table(
        pa.table(
            {
                "id": list(range(40)),
                "payload": [f"value-{value}" for value in range(40)],
                "unused": list(range(40)),
            }
        ),
        target,
        row_group_size=10,
    )
    observed_rows: list[int] = []
    predicates: list[object] = []
    execute_batch_program = arrow_execution._execute_batch_program

    def traced_batch_program(
        batch: object, operations: tuple[object, ...], projection: object
    ) -> Iterator[object]:
        observed_rows.append(batch.num_rows)  # type: ignore[attr-defined]
        predicates.extend(operations[:-1])
        yield from execute_batch_program(batch, operations, projection)

    monkeypatch.setattr(arrow_execution, "_execute_batch_program", traced_batch_program)

    result = (
        fpstreams.rows.from_parquet(target)
        .where(fpstreams.lit(39) == fpstreams.col("id"))
        .select("payload")
        .to_list()
    )

    assert result == [{"payload": "value-39"}]
    assert observed_rows == [1]
    assert len(predicates) == 1


@pytest.mark.parametrize(
    ("predicate", "expected"),
    [
        (fpstreams.col("id") < 5, [0, 1, 2, 3, 4]),
        (fpstreams.col("id") <= 4, [0, 1, 2, 3, 4]),
        (fpstreams.col("id") > 34, [35, 36, 37, 38, 39]),
        (fpstreams.col("id") >= 35, [35, 36, 37, 38, 39]),
        (fpstreams.lit(5) > fpstreams.col("id"), [0, 1, 2, 3, 4]),
        (fpstreams.lit(4) >= fpstreams.col("id"), [0, 1, 2, 3, 4]),
        (fpstreams.lit(34) < fpstreams.col("id"), [35, 36, 37, 38, 39]),
        (fpstreams.lit(35) <= fpstreams.col("id"), [35, 36, 37, 38, 39]),
    ],
)
def test_parquet_exact_i64_range_prunes_before_the_python_residual(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    predicate: object,
    expected: list[int],
) -> None:
    """Four direct range operators, including literal-left forms, prune clustered row groups."""
    from fpstreams.execution import arrow as arrow_execution

    target = tmp_path / "range.parquet"
    pq.write_table(
        pa.table({"id": list(range(40)), "unused": list(range(40))}), target, row_group_size=10
    )
    observed_rows: list[int] = []
    execute_batch_program = arrow_execution._execute_batch_program

    def traced_batch_program(
        batch: object, operations: tuple[object, ...], projection: object
    ) -> Iterator[object]:
        observed_rows.append(batch.num_rows)  # type: ignore[attr-defined]
        yield from execute_batch_program(batch, operations, projection)

    monkeypatch.setattr(arrow_execution, "_execute_batch_program", traced_batch_program)
    result = (
        fpstreams.rows.from_parquet(target)
        .where(predicate)  # type: ignore[arg-type]
        .select("id")
        .to_list()
    )

    assert result == [{"id": value} for value in expected]
    assert sum(observed_rows) == 5


def test_parquet_range_hint_keeps_null_rows_for_python_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The scan hint is a superset: a leading null still reaches and fails in Python."""
    from fpstreams.execution import arrow as arrow_execution

    target = tmp_path / "nullable-range.parquet"
    pq.write_table(
        pa.table({"id": [None, *range(9), *range(20, 30), *range(10, 20)]}),
        target,
        row_group_size=10,
    )
    observed_rows: list[int] = []
    convert_rows = arrow_execution.batch_to_rows

    def tracked(batch: object) -> list[dict[str, object]]:
        observed_rows.append(batch.num_rows)  # type: ignore[attr-defined]
        return convert_rows(batch)

    monkeypatch.setattr(arrow_execution, "batch_to_rows", tracked)

    with pytest.raises(TypeError):
        fpstreams.rows.from_parquet(target).where(fpstreams.col("id") > 25).to_list()
    assert observed_rows == [1]


@pytest.mark.parametrize(
    ("predicate", "expected"),
    [
        (fpstreams.col("id") < -(1 << 63), []),
        (fpstreams.col("id") <= -(1 << 63), [-(1 << 63)]),
        (fpstreams.col("id") > (1 << 63) - 1, []),
        (fpstreams.col("id") >= (1 << 63) - 1, [(1 << 63) - 1]),
    ],
)
def test_parquet_range_hint_preserves_i64_boundaries_without_adjustment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    predicate: object,
    expected: list[int],
) -> None:
    """Strict and inclusive bounds at both i64 extremes need no overflow-prone +/- 1."""
    from fpstreams.execution import arrow as arrow_execution

    target = tmp_path / "range-boundaries.parquet"
    pq.write_table(
        pa.table(
            {
                "id": [
                    -(1 << 63),
                    -(1 << 63) + 1,
                    (1 << 63) - 2,
                    (1 << 63) - 1,
                ]
            }
        ),
        target,
        row_group_size=1,
    )
    observed_rows: list[int] = []
    execute_batch_program = arrow_execution._execute_batch_program

    def traced_batch_program(
        batch: object, operations: tuple[object, ...], projection: object
    ) -> Iterator[object]:
        observed_rows.append(batch.num_rows)  # type: ignore[attr-defined]
        yield from execute_batch_program(batch, operations, projection)

    monkeypatch.setattr(arrow_execution, "_execute_batch_program", traced_batch_program)

    result = (
        fpstreams.rows.from_parquet(target)
        .where(predicate)  # type: ignore[arg-type]
        .select("id")
        .to_list()
    )

    assert result == [{"id": value} for value in expected]
    assert sum(observed_rows) == len(expected)


def test_parquet_range_first_keeps_python_short_circuit_and_error_order(tmp_path: Path) -> None:
    """Range hints stay out of first(), where source order controls whether a null is observed."""
    accepted_first = tmp_path / "accepted-first.parquet"
    pq.write_table(pa.table({"id": [10, None]}), accepted_first)
    assert fpstreams.rows.from_parquet(accepted_first).where(fpstreams.col("id") > 5).first() == {
        "id": 10
    }

    error_first = tmp_path / "error-first.parquet"
    pq.write_table(pa.table({"id": [0, None, 10]}), error_first)
    with pytest.raises(TypeError):
        fpstreams.rows.from_parquet(error_first).where(fpstreams.col("id") > 5).first()


def test_parquet_equality_hint_rejects_an_internal_dotted_field_node(tmp_path: Path) -> None:
    """A dotted Field is a Python path, not a top-level Parquet column with the same name."""
    from fpstreams.expressions.row import RowExpr
    from fpstreams.expressions.row_ir import Field

    target = tmp_path / "dotted-field.parquet"
    pq.write_table(
        pa.table(
            {
                "a.b": [0],
                "a": pa.array([{"b": 2}], type=pa.struct([("b", pa.int64())])),
            }
        ),
        target,
    )
    dotted = RowExpr._from_node(Field("a.b"), "a.b")
    query = fpstreams.rows.from_parquet(target).where(dotted == 2)
    expected = [{"a.b": 0, "a": {"b": 2}}]

    assert query.with_engine("python").to_list() == expected
    assert query.to_list() == expected


def test_parquet_range_hint_rejects_an_internal_dotted_field_node(tmp_path: Path) -> None:
    """A dotted Python path cannot prune by an unrelated top-level Parquet field."""
    from fpstreams.expressions.row import RowExpr
    from fpstreams.expressions.row_ir import Field

    target = tmp_path / "dotted-range-field.parquet"
    pq.write_table(
        pa.table(
            {
                "a.b": [0],
                "a": pa.array([{"b": 2}], type=pa.struct([("b", pa.int64())])),
            }
        ),
        target,
    )
    dotted = RowExpr._from_node(Field("a.b"), "a.b")
    query = fpstreams.rows.from_parquet(target).where(dotted > 1)
    expected = [{"a.b": 0, "a": {"b": 2}}]

    assert query.with_engine("python").to_list() == expected
    assert query.to_list() == expected


def test_parquet_equality_pushdown_prunes_across_multiple_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The bounded metadata probe considers every fragment before enabling its hint."""
    from fpstreams.execution import arrow as arrow_execution

    target = tmp_path / "multi-file"
    target.mkdir()
    for part, start in enumerate((0, 20)):
        pq.write_table(
            pa.table(
                {
                    "id": list(range(start, start + 20)),
                    "payload": [f"value-{value}" for value in range(start, start + 20)],
                }
            ),
            target / f"part-{part}.parquet",
            row_group_size=10,
        )
    observed_rows: list[int] = []
    execute_batch_program = arrow_execution._execute_batch_program

    def traced_batch_program(
        batch: object, operations: tuple[object, ...], projection: object
    ) -> Iterator[object]:
        observed_rows.append(batch.num_rows)  # type: ignore[attr-defined]
        yield from execute_batch_program(batch, operations, projection)

    monkeypatch.setattr(arrow_execution, "_execute_batch_program", traced_batch_program)

    result = (
        fpstreams.rows.from_parquet(target)
        .where(fpstreams.col("id") == 39)
        .select("payload")
        .to_list()
    )

    assert result == [{"payload": "value-39"}]
    assert [rows for rows in observed_rows if rows] == [1]


def test_parquet_equality_pushdown_handles_hive_partition_fields(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A virtual partition field may eliminate whole files without changing the residual."""
    from fpstreams.execution import arrow as arrow_execution

    target = tmp_path / "partitioned"
    for group in ("alpha", "beta"):
        directory = target / f"group={group}"
        directory.mkdir(parents=True)
        pq.write_table(
            pa.table({"payload": [f"{group}-0", f"{group}-1"]}),
            directory / "data.parquet",
        )
    observed_rows: list[int] = []
    execute_batch_program = arrow_execution._execute_batch_program

    def traced_batch_program(
        batch: object, operations: tuple[object, ...], projection: object
    ) -> Iterator[object]:
        observed_rows.append(batch.num_rows)  # type: ignore[attr-defined]
        yield from execute_batch_program(batch, operations, projection)

    monkeypatch.setattr(arrow_execution, "_execute_batch_program", traced_batch_program)

    result = (
        fpstreams.rows.from_parquet(target, partitioning="hive")
        .where(fpstreams.col("group") == "beta")
        .select("payload")
        .to_list()
    )

    assert result == [{"payload": "beta-0"}, {"payload": "beta-1"}]
    assert sum(observed_rows) == 2


def test_parquet_equality_pushdown_declines_without_pruning_statistics(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Absent row-group statistics retain the ordinary residual scan."""
    from fpstreams.execution import arrow as arrow_execution

    target = tmp_path / "no-statistics.parquet"
    pq.write_table(
        pa.table({"id": list(range(40)), "payload": [f"value-{i}" for i in range(40)]}),
        target,
        row_group_size=10,
        write_statistics=False,
    )
    observed_rows: list[int] = []
    execute_batch_program = arrow_execution._execute_batch_program

    def traced_batch_program(
        batch: object, operations: tuple[object, ...], projection: object
    ) -> Iterator[object]:
        observed_rows.append(batch.num_rows)  # type: ignore[attr-defined]
        yield from execute_batch_program(batch, operations, projection)

    monkeypatch.setattr(arrow_execution, "_execute_batch_program", traced_batch_program)

    result = (
        fpstreams.rows.from_parquet(target)
        .where(fpstreams.col("id") == 39)
        .select("payload")
        .to_list()
    )

    assert result == [{"payload": "value-39"}]
    assert sum(observed_rows) == 40


def test_parquet_equality_metadata_probe_declines_on_unknown_local_layout() -> None:
    """A speculative metadata failure cannot become a new scanner failure mode."""
    from types import SimpleNamespace

    from fpstreams.tabular import arrow as arrow_adapter

    class Fragment:
        num_row_groups = 1

        def split_by_row_group(self, *_args: object, **_options: object) -> object:
            raise RuntimeError("unsupported metadata")

    class Dataset:
        schema = object()

        def get_fragments(self) -> list[Fragment]:
            return [Fragment()]

    dataset_module = SimpleNamespace(ParquetFileFragment=Fragment)

    assert not arrow_adapter._parquet_predicate_can_prune(
        Dataset(), object(), dataset_module, probe_statistics=True
    )


@pytest.mark.parametrize(("first_kept", "expected"), [(1, False), (0, True)])
def test_parquet_first_equality_probe_only_consults_the_leading_fragment(
    first_kept: int, expected: bool
) -> None:
    """A multi-file first() pushes only when the first fragment is proven impossible."""
    from types import SimpleNamespace

    from fpstreams.tabular import arrow as arrow_adapter

    calls: list[str] = []

    class Fragment:
        num_row_groups = 1

        def __init__(self, name: str, kept: int) -> None:
            self.name = name
            self.kept = kept

        def split_by_row_group(self, *_args: object, **_options: object) -> list[object]:
            calls.append(self.name)
            return [object()] * self.kept

    class Dataset:
        schema = object()

        def get_fragments(self) -> list[Fragment]:
            return [Fragment("first", first_kept), Fragment("second", 0)]

    dataset_module = SimpleNamespace(ParquetFileFragment=Fragment)

    assert (
        arrow_adapter._parquet_predicate_can_prune(
            Dataset(),
            object(),
            dataset_module,
            probe_statistics=True,
            first_only=True,
        )
        is expected
    )
    assert calls == ["first"]


def test_parquet_first_equality_probe_does_not_pull_a_tail_after_proven_pruning() -> None:
    """A leading impossible fragment decides the safe first() hint before tail access."""
    from types import SimpleNamespace

    from fpstreams.tabular import arrow as arrow_adapter

    calls: list[str] = []

    class Fragment:
        num_row_groups = 1

        def split_by_row_group(self, *_args: object, **_options: object) -> list[object]:
            calls.append("split:first")
            return []

    def fragments() -> Iterator[Fragment]:
        calls.append("next:first")
        yield Fragment()
        calls.append("next:tail")
        raise MemoryError("tail metadata must stay cold")

    class Dataset:
        schema = object()

        def get_fragments(self) -> Iterator[Fragment]:
            return fragments()

    dataset_module = SimpleNamespace(ParquetFileFragment=Fragment)

    assert arrow_adapter._parquet_predicate_can_prune(
        Dataset(),
        object(),
        dataset_module,
        probe_statistics=True,
        first_only=True,
    )
    assert calls == ["next:first", "split:first"]


def test_parquet_first_equality_probe_does_not_pull_a_tail_after_possible_match() -> None:
    """A retained leading row group also decides before inaccessible tail metadata."""
    from types import SimpleNamespace

    from fpstreams.tabular import arrow as arrow_adapter

    calls: list[str] = []

    class Fragment:
        num_row_groups = 1

        def split_by_row_group(self, *_args: object, **_options: object) -> list[object]:
            calls.append("split:first")
            return [object()]

    def fragments() -> Iterator[Fragment]:
        calls.append("next:first")
        yield Fragment()
        calls.append("next:tail")
        raise MemoryError("tail metadata must stay cold")

    class Dataset:
        schema = object()

        def get_fragments(self) -> Iterator[Fragment]:
            return fragments()

    dataset_module = SimpleNamespace(ParquetFileFragment=Fragment)

    assert not arrow_adapter._parquet_predicate_can_prune(
        Dataset(),
        object(),
        dataset_module,
        probe_statistics=True,
        first_only=True,
    )
    assert calls == ["next:first", "split:first"]


def test_parquet_first_equality_probe_keeps_known_single_file_partial_pruning() -> None:
    """A proven single fragment can prune row groups without consulting an iterator tail."""
    from types import SimpleNamespace

    from fpstreams.tabular import arrow as arrow_adapter

    calls: list[str] = []

    class Fragment:
        num_row_groups = 2

        def split_by_row_group(self, *_args: object, **_options: object) -> list[object]:
            calls.append("split:first")
            return [object()]

    def fragments() -> Iterator[Fragment]:
        calls.append("next:first")
        yield Fragment()
        calls.append("next:tail")
        raise MemoryError("a known single file cannot have a fragment tail")

    class Dataset:
        schema = object()

        def get_fragments(self) -> Iterator[Fragment]:
            return fragments()

    dataset_module = SimpleNamespace(ParquetFileFragment=Fragment)

    assert arrow_adapter._parquet_predicate_can_prune(
        Dataset(),
        object(),
        dataset_module,
        probe_statistics=True,
        first_only=True,
        single_fragment=True,
    )
    assert calls == ["next:first", "split:first"]


def test_parquet_first_known_single_file_prunes_late_row_groups(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The no-tail metadata policy retains selective first() scans for one local file."""
    from fpstreams.execution import arrow as arrow_execution

    target = tmp_path / "first-late.parquet"
    pq.write_table(
        pa.table({"id": list(range(40)), "payload": [f"value-{i}" for i in range(40)]}),
        target,
        row_group_size=10,
    )
    observed_rows: list[int] = []
    execute_first = arrow_execution._execute_first_batch_program

    def traced_first(
        batch: object, operations: tuple[object, ...], projection: object
    ) -> tuple[bool, object | None]:
        observed_rows.append(batch.num_rows)  # type: ignore[attr-defined]
        return execute_first(batch, operations, projection)  # type: ignore[arg-type]

    monkeypatch.setattr(arrow_execution, "_execute_first_batch_program", traced_first)

    result = (
        fpstreams.rows.from_parquet(target)
        .where(fpstreams.col("id") == 39)
        .select("payload")
        .first()
    )

    assert result == {"payload": "value-39"}
    assert sum(observed_rows) == 1


def test_parquet_pruning_probe_bounds_zero_row_group_fragments() -> None:
    """The speculative metadata buffer is bounded even when row-group counts stay zero."""
    from types import SimpleNamespace

    from fpstreams.tabular import arrow as arrow_adapter

    pulls = 0

    class Fragment:
        num_row_groups = 0

        def split_by_row_group(self, *_args: object, **_options: object) -> list[object]:
            raise AssertionError("a declined bounded probe must not split fragments")

    def fragments() -> Iterator[Fragment]:
        nonlocal pulls
        for _index in range(600):
            pulls += 1
            yield Fragment()

    class Dataset:
        schema = object()

        def get_fragments(self) -> Iterator[Fragment]:
            return fragments()

    dataset_module = SimpleNamespace(ParquetFileFragment=Fragment)

    assert not arrow_adapter._parquet_predicate_can_prune(
        Dataset(), object(), dataset_module, probe_statistics=True
    )
    assert pulls <= 513


def test_parquet_first_scanner_disables_arrow_readahead(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A first-only request must not ask Arrow to prefetch later batches or files."""
    from types import SimpleNamespace

    from fpstreams.planning.arrow_source import ArrowScanRequest
    from fpstreams.tabular import arrow as arrow_adapter

    options: list[dict[str, object]] = []
    schema = pa.schema([("id", pa.int64())])

    class Scanner:
        projected_schema = schema

        def to_batches(self) -> Iterator[object]:
            yield pa.record_batch({"id": [1]})

    class Dataset:
        def __init__(self) -> None:
            self.schema = schema

        def scanner(self, **scan_options: object) -> Scanner:
            options.append(scan_options)
            return Scanner()

    dataset_module = SimpleNamespace(Dataset=Dataset)
    monkeypatch.setattr(
        arrow_adapter,
        "_arrow_modules",
        lambda: (pa, dataset_module, object()),
    )
    _batches, _size, _projected, requested, _count_rows = arrow_adapter._parquet_batch_factory(
        Dataset()
    )

    assert [batch.to_pylist() for batch in requested(ArrowScanRequest(first_only=True))] == [
        [{"id": 1}]
    ]
    assert options == [
        {
            "columns": None,
            "filter": None,
            "batch_size": 65_536,
            "use_threads": True,
            "batch_readahead": 0,
            "fragment_readahead": 0,
        }
    ]


@pytest.mark.parametrize(
    ("values", "literal", "expected"),
    [
        (pa.array([1, 2], type=pa.int64()), True, [1]),
        (pa.array([True, False], type=pa.bool_()), 1, [True]),
        (pa.array(["1", "2"], type=pa.string()), b"1", []),
        (pa.array([1, 2**63 + 1], type=pa.uint64()), 1, [1]),
    ],
)
def test_parquet_equality_pushdown_declines_cross_type_python_comparisons(
    tmp_path: Path,
    values: object,
    literal: object,
    expected: list[object],
) -> None:
    """Scanner hints never replace Python's bool/int equality or raise Arrow type errors."""
    target = tmp_path / "cross-type.parquet"
    pq.write_table(pa.table({"value": values}), target)

    result = (
        fpstreams.rows.from_parquet(target)
        .where(fpstreams.col("value") == literal)
        .select("value")
        .to_list()
    )

    assert result == [{"value": value} for value in expected]


def test_parquet_range_filter_declines_an_incompatible_arrow_kernel(tmp_path: Path) -> None:
    """A standalone direct filter must keep Python's timestamp/int comparison error."""
    target = tmp_path / "timestamp-range.parquet"
    pq.write_table(pa.table({"value": pa.array([0], type=pa.timestamp("s"))}), target)
    query = fpstreams.rows.from_parquet(target).where(fpstreams.col("value") > 1)

    with pytest.raises(TypeError) as canonical:
        query.with_engine("python").to_list()
    with pytest.raises(TypeError) as automatic:
        query.to_list()

    assert str(automatic.value) == str(canonical.value)


def test_parquet_pushdown_preserves_null_and_source_filter_semantics(
    tmp_path: Path,
) -> None:
    """Nulls and an explicit dataset filter retain their established observable results."""
    target = tmp_path / "nullable.parquet"
    pq.write_table(
        pa.table({"id": [None, 1, 2], "payload": ["null", "one", "two"]}),
        target,
    )

    assert fpstreams.rows.from_parquet(target).where(fpstreams.col("id") == 1).select(
        "payload"
    ).to_list() == [{"payload": "one"}]
    assert (
        fpstreams.rows.from_parquet(target, filter=ds.field("id") >= 2)
        .where(fpstreams.col("id") == 1)
        .select("payload")
        .to_list()
        == []
    )
    assert (
        fpstreams.rows.from_parquet(target, filter=ds.field("id") >= 2)
        .where(fpstreams.col("id") < 2)
        .select("payload")
        .to_list()
        == []
    )


def test_parquet_query_projection_preserves_missing_field_and_public_column_scope(
    tmp_path: Path,
) -> None:
    """Scanner pruning must not eagerly fail or recover fields hidden by ``columns``."""
    target = tmp_path / "scope.parquet"
    pq.write_table(pa.table({"present": [1], "hidden": [7]}), target)

    with pytest.raises(fpstreams.SelectionError) as canonical:
        fpstreams.rows([{"present": 1, "hidden": 7}]).select("missing").to_list()
    with pytest.raises(fpstreams.SelectionError) as projected:
        fpstreams.rows.from_parquet(target).select("missing").to_list()
    assert str(projected.value) == str(canonical.value)
    assert type(projected.value.__cause__) is type(canonical.value.__cause__) is KeyError

    with pytest.raises(fpstreams.SelectionError):
        (
            fpstreams.rows.from_parquet(target, columns=("present",))
            .where(fpstreams.col("hidden") == 7)
            .select("present")
            .to_list()
        )
    with pytest.raises(fpstreams.SelectionError):
        (
            fpstreams.rows.from_parquet(target, columns=("present",))
            .where(fpstreams.col("hidden") > 0)
            .select("present")
            .to_list()
        )

    empty = tmp_path / "empty.parquet"
    pq.write_table(pa.table({"present": pa.array([], type=pa.int64())}), empty)
    assert fpstreams.rows.from_parquet(empty).select("missing").to_list() == []
    assert (
        fpstreams.rows.from_parquet(empty)
        .where(fpstreams.col("missing") > 0)
        .select("present")
        .to_list()
        == []
    )


def test_parquet_query_projection_cannot_hide_invalid_public_columns(tmp_path: Path) -> None:
    """A downstream projection must retain errors from the explicit source column list."""
    target = tmp_path / "invalid-columns.parquet"
    pq.write_table(pa.table({"present": [1]}), target)
    source = fpstreams.rows.from_parquet(target, columns=("present", "missing"))

    with pytest.raises(pa.ArrowInvalid, match=r"No match for FieldRef.Name\(missing\)"):
        source.select("present").to_list()
    with pytest.raises(pa.ArrowInvalid, match=r"No match for FieldRef.Name\(missing\)"):
        source.select().to_list()


def test_parquet_publish_is_atomic_and_never_consumes_on_existing_error(tmp_path: Path) -> None:
    target = tmp_path / "atomic.parquet"
    assert fpstreams.rows([{"id": 10, "value": "kept"}]).to_parquet(target) == 1

    opened = False

    def unused() -> Iterator[dict[str, int]]:
        nonlocal opened
        opened = True
        yield {"id": 11}

    with pytest.raises(FileExistsError):
        fpstreams.rows(unused()).to_parquet(target)
    assert not opened

    with pytest.raises(ValueError, match="introduced columns"):
        fpstreams.rows([{"id": 1, "value": "new"}, {"id": 2, "extra": True}]).to_parquet(
            target,
            if_exists="replace",
            batch_size=1,
        )
    assert fpstreams.rows.from_parquet(target).to_list() == [{"id": 10, "value": "kept"}]
    assert list(tmp_path.iterdir()) == [target]

    empty = tmp_path / "empty.parquet"
    schema = pa.schema([("id", pa.int64())])
    assert fpstreams.rows([]).to_parquet(empty, schema=schema) == 0
    assert pq.read_table(empty).schema == schema
    assert pq.read_table(empty).num_rows == 0


def test_arrow_configuration_rejects_silent_schema_loss(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="outside the schema"):
        fpstreams.rows([{"a": 1}, {"b": 2}]).arrow_batches(batch_size=1).to_list()
    with pytest.raises(ValueError, match="without columns"):
        fpstreams.rows([{}]).to_arrow()
    with pytest.raises(ValueError, match="without an Arrow schema"):
        fpstreams.rows([]).to_parquet(tmp_path / "missing-schema.parquet")
    assert not (tmp_path / "missing-schema.parquet").exists()

    duplicate = pa.schema([("id", pa.int64()), ("id", pa.int64())])
    with pytest.raises(fpstreams.DuplicateKeyError, match="duplicate column"):
        fpstreams.rows([]).arrow_batches(schema=duplicate)
    duplicate_batch = pa.RecordBatch.from_arrays(
        [pa.array([1]), pa.array([2])],
        names=("id", "id"),
    )
    with pytest.raises(fpstreams.DuplicateKeyError, match="duplicate column"):
        fpstreams.rows.from_arrow(duplicate_batch)
    duplicate_table = pa.Table.from_arrays(
        [pa.array([1]), pa.array([2])],
        names=("id", "id"),
    )
    with pytest.raises(fpstreams.DuplicateKeyError, match="duplicate column"):
        fpstreams.rows.from_arrow(duplicate_table)
    with pytest.raises(ValueError, match="cannot be empty"):
        fpstreams.rows.from_arrow(pa.Table.from_arrays([pa.array([1])], names=("",)))
    with pytest.raises(ValueError, match="batch_size"):
        fpstreams.rows.from_arrow(pa.table({"id": [1]}), batch_size=0)
    with pytest.raises(ValueError, match="local path"):
        fpstreams.rows([{"id": 1}]).to_parquet("s3://bucket/events.parquet")


def test_rows_factory_recognizes_eager_dataframe_protocols() -> None:
    pandas_frame = pd.DataFrame(
        {"id": [1, 2], "label": ["a", "b"]},
        index=pd.Index(["left", "right"], name="row"),
    )
    polars_frame = pl.DataFrame({"id": [3, 4], "label": ["c", "d"]})

    assert fpstreams.rows(pandas_frame).to_list() == [
        {"id": 1, "label": "a"},
        {"id": 2, "label": "b"},
    ]
    assert fpstreams.rows(polars_frame).to_list() == [
        {"id": 3, "label": "c"},
        {"id": 4, "label": "d"},
    ]
    strict_frame = pd.DataFrame({"id": [1, 2]})
    assert fpstreams.rows.from_dataframe(strict_frame, allow_copy=False).count() == 2
    assert fpstreams.rows.from_polars(polars_frame, batch_size=1).count() == 2

    pandas_source = fpstreams.rows.from_dataframe(pandas_frame)
    polars_source = fpstreams.rows.from_polars(polars_frame)
    assert pandas_source.select("label").to_list() == [{"label": "a"}, {"label": "b"}]
    assert polars_source.select("label").to_list() == [{"label": "c"}, {"label": "d"}]
    assert pandas_source.to_arrow().column_names == ["id", "label"]


def test_numpy_adapter_is_explicit_lazy_and_replayable() -> None:
    np = pytest.importorskip("numpy")

    source = np.asarray([[1, 2], [3, 4]])
    table = fpstreams.rows.from_numpy(source)

    source[0, 0] = 10
    assert table.to_list() == [
        {"0": 10, "1": 2},
        {"0": 3, "1": 4},
    ]
    source[1, 1] = 40
    assert table.to_list() == [
        {"0": 10, "1": 2},
        {"0": 3, "1": 40},
    ]
    assert table._flow._pipeline.source.capabilities.exact_size == 2
    assert table._flow._pipeline.source.capabilities.reiterable is True

    nested = [[1, 2]]
    converted = fpstreams.flow.from_numpy(nested, columns=["left", "right"])
    nested[0][0] = 99
    assert converted.select("left", "right").to_list() == [{"left": 1, "right": 2}]


def test_identity_numpy_to_list_skips_executor_forwarding_with_fresh_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exact Rows and Flow collect canonical fresh dictionaries at the source boundary."""
    np = pytest.importorskip("numpy")
    from fpstreams.streams import flow_terminals

    values = np.asarray([[1, 2], [1, 2]], dtype=np.int64)
    source = fpstreams.rows.from_numpy(values, columns=("left", "right"))

    def reject_forwarding(*_arguments: object, **_options: object) -> Iterator[object]:
        raise AssertionError("identity NumPy to_list must not enter executor forwarding")

    monkeypatch.setattr(flow_terminals, "execute_physical", reject_forwarding)

    row_values = source.to_list()
    flow_values = source.to_flow().to_list()

    assert (
        row_values
        == flow_values
        == [
            {"left": 1, "right": 2},
            {"left": 1, "right": 2},
        ]
    )
    assert row_values[0] is not row_values[1]
    assert row_values[0] is not flow_values[0]


@pytest.mark.parametrize("width", [2, 8])
def test_numpy_identity_rows_boxes_values_in_bounded_matrix_batches(width: int) -> None:
    """The eager identity sink avoids both per-row ndarray calls and one giant snapshot."""
    np = pytest.importorskip("numpy")
    from fpstreams.tabular.numpy import (
        _NUMPY_IDENTITY_ROW_BATCH_SIZE,
        NumpyRowSource,
        numpy_identity_rows,
    )

    batch_size = _NUMPY_IDENTITY_ROW_BATCH_SIZE
    values = np.arange((batch_size + 3) * width, dtype=np.int64).reshape(-1, width)
    converted_batches: list[int] = []

    class TrackingMatrix:
        ndim = 2
        shape = values.shape

        def __iter__(self) -> Iterator[object]:
            raise AssertionError("identity collection must not convert one ndarray row at a time")

        def __getitem__(self, selected: slice) -> object:
            assert isinstance(selected, slice)
            batch = values[selected]
            converted_batches.append(len(batch))
            return batch

    names = tuple(f"c{index}" for index in range(width))
    result = numpy_identity_rows(NumpyRowSource(TrackingMatrix(), names))

    assert converted_batches == [batch_size, 3]
    assert len(result) == batch_size + 3
    assert result[0] == dict(zip(names, range(width), strict=True))
    last_start = (batch_size + 2) * width
    assert result[-1] == dict(zip(names, range(last_start, last_start + width), strict=True))

    class EmptyBatch:
        def tolist(self) -> list[object]:
            return []

    class StalledMatrix:
        ndim = 2
        shape = (1, 1)

        def __getitem__(self, _selected: slice) -> EmptyBatch:
            return EmptyBatch()

    with pytest.raises(ValueError, match="row count changed during iteration"):
        numpy_identity_rows(NumpyRowSource(StalledMatrix(), ("value",)))


def test_numpy_identity_rows_preserves_layout_dtype_and_object_identity() -> None:
    """Batched boxing keeps ndarray ordering, scalar conversion, and opaque object identity."""
    np = pytest.importorskip("numpy")

    for dtype in (np.int64, np.float64):
        base = np.arange(80, dtype=dtype).reshape(10, 8)
        if dtype is np.float64:
            base = base / 3
        matrices = (
            base.copy(order="C"),
            np.asfortranarray(base),
            base[:, ::2],
            base[::-1, 3::-1],
        )
        for matrix in matrices:
            for width in range(1, matrix.shape[1] + 1):
                values = matrix[:, :width]
                names = tuple(f"c{index}" for index in range(width))
                expected = [dict(zip(names, row, strict=True)) for row in values.tolist()]
                source = fpstreams.rows.from_numpy(values, columns=names)
                assert source.to_list() == expected
                assert source.with_engine("python").to_list() == expected

    protocol_calls: list[str] = []

    class OpaqueValue:
        def __int__(self) -> int:
            protocol_calls.append("int")
            raise AssertionError("object ndarray values must not be coerced")

        def __float__(self) -> float:
            protocol_calls.append("float")
            raise AssertionError("object ndarray values must not be coerced")

        def __index__(self) -> int:
            protocol_calls.append("index")
            raise AssertionError("object ndarray values must not be coerced")

        def __iter__(self) -> Iterator[object]:
            protocol_calls.append("iter")
            raise AssertionError("object ndarray values must stay opaque")

    markers = [[OpaqueValue() for _ in range(8)] for _ in range(3)]
    objects = np.empty((3, 8), dtype=object)
    for row_index, row in enumerate(markers):
        objects[row_index] = row
    names = tuple(f"c{index}" for index in range(8))
    records = fpstreams.rows.from_numpy(objects, columns=names).to_list()

    assert protocol_calls == []
    for row_index, record in enumerate(records):
        assert all(
            record[name] is markers[row_index][column_index]
            for column_index, name in enumerate(names)
        )
    for width in range(1, 9):
        assert fpstreams.rows.from_numpy(np.empty((0, width))).to_list() == []


def test_numpy_native_record_assembly_covers_identity_and_projected_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Private column batches bypass both Python record builders when the ABI is genuine."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution import numpy_prefix
    from fpstreams.tabular import numpy as numpy_adapter

    values = np.arange(520 * 5, dtype=np.int64).reshape(520, 5)
    names = tuple(f"c{index}" for index in range(5))
    source = fpstreams.rows.from_numpy(values, columns=names)

    def reject_python_builder(*_arguments: object, **_options: object) -> object:
        raise AssertionError("genuine native record assembly must own this private batch")

    monkeypatch.setattr(numpy_adapter, "_materialize_numpy_identity_batch", reject_python_builder)
    monkeypatch.setattr(numpy_prefix, "_records_from_columns", reject_python_builder)

    assert source.to_list() == [dict(zip(names, row, strict=True)) for row in values.tolist()]
    assert source.select("c0", "c2", "c4").to_list() == [
        {"c0": row[0], "c2": row[2], "c4": row[4]} for row in values.tolist()
    ]


def test_numpy_record_assembly_rejects_replaced_native_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Python replacement cannot inherit the trusted private-column boundary."""
    np = pytest.importorskip("numpy")
    from fpstreams import _native

    calls = 0

    def replaced(*_arguments: object) -> list[dict[str, object]]:
        nonlocal calls
        calls += 1
        return []

    monkeypatch.setattr(_native, "records_from_exact_columns_v1", replaced)
    values = np.arange(30, dtype=np.int64).reshape(10, 3)
    source = fpstreams.rows.from_numpy(values, columns=("left", "middle", "right"))

    assert source.to_list() == [
        {"left": row[0], "middle": row[1], "right": row[2]} for row in values.tolist()
    ]
    assert source.select("left", "right").to_list() == [
        {"left": row[0], "right": row[2]} for row in values.tolist()
    ]
    assert calls == 0


def test_numpy_identity_rows_releases_partial_batches_after_memory_error() -> None:
    """A failed later batch cannot leak the already boxed partial result."""
    from fpstreams.tabular.numpy import (
        _NUMPY_IDENTITY_ROW_BATCH_SIZE,
        NumpyRowSource,
        numpy_identity_rows,
    )

    references: list[weakref.ReferenceType[object]] = []
    calls = 0

    class Marker:
        pass

    class ConvertedBatch:
        def tolist(self) -> list[list[Marker]]:
            nonlocal calls
            calls += 1
            if calls == 2:
                raise MemoryError("second NumPy row batch")
            rows = [[Marker(), Marker()] for _ in range(_NUMPY_IDENTITY_ROW_BATCH_SIZE)]
            references.extend(weakref.ref(value) for row in rows for value in row)
            return rows

    class FailingMatrix:
        ndim = 2
        shape = (_NUMPY_IDENTITY_ROW_BATCH_SIZE + 1, 2)

        def __getitem__(self, _selected: slice) -> ConvertedBatch:
            return ConvertedBatch()

    try:
        numpy_identity_rows(NumpyRowSource(FailingMatrix(), ("left", "right")))
    except MemoryError as error:
        assert str(error) == "second NumPy row batch"
        frames = error.__traceback__
        error.__traceback__ = None
        if frames is not None:
            traceback.clear_frames(frames)
        del frames
    else:  # pragma: no cover - the synthetic second batch always fails
        raise AssertionError("the synthetic second batch did not fail")

    gc.collect()
    assert references
    assert all(reference() is None for reference in references)


@pytest.mark.skipif(not hasattr(signal, "setitimer"), reason="requires POSIX interval timers")
def test_numpy_to_list_matches_live_signal_mutation_and_resize_boundaries() -> None:
    """Auto batching and canonical iteration agree on signal-time growth and shape changes."""
    row_count = 100_000

    def consume(
        engine: str, action: str
    ) -> tuple[list[dict[str, int]] | None, BaseException | None]:
        np = pytest.importorskip("numpy")
        values = np.zeros((row_count, 2), dtype=np.int64)
        source = fpstreams.rows.from_numpy(values, columns=("left", "right")).with_engine(engine)
        fired = False

        def mutate(_signal_number: int, _frame: object) -> None:
            nonlocal fired
            fired = True
            if action == "mutate":
                values[-1] = (7, 9)
            elif action == "grow":
                values.resize((row_count + 1, 2), refcheck=False)
                values[-1] = (7, 9)
            elif action == "shrink":
                values.resize((row_count - 1, 2), refcheck=False)
            else:
                values.resize((values.size,), refcheck=False)

        previous_handler = signal.signal(signal.SIGALRM, mutate)
        previous_timer = signal.getitimer(signal.ITIMER_REAL)
        signal.setitimer(signal.ITIMER_REAL, 0.001)
        try:
            try:
                result = source.to_list()
            except BaseException as error:
                result = None
                captured = error
            else:
                captured = None
        finally:
            signal.setitimer(signal.ITIMER_REAL, *previous_timer)
            signal.signal(signal.SIGALRM, previous_handler)
        assert fired
        return result, captured

    for action in ("mutate", "grow", "shrink", "reshape"):
        canonical = consume("python", action)
        automatic = consume("auto", action)
        if action == "reshape":
            assert canonical[0] is automatic[0] is None
            assert [type(canonical[1]), type(automatic[1])] == [ValueError, ValueError]
            assert (
                str(canonical[1])
                == str(automatic[1])
                == ("from_numpy() retained array changed to 1 dimensions during iteration")
            )
        else:
            assert canonical[1] is automatic[1] is None
            assert automatic[0] == canonical[0]
            expected_rows = row_count + (1 if action == "grow" else -1 if action == "shrink" else 0)
            assert automatic[0] is not None
            assert len(automatic[0]) == expected_rows
            if action in {"mutate", "grow"}:
                assert automatic[0][-1] == {"left": 7, "right": 9}


def test_numpy_identity_sinks_deopt_on_free_threaded_python(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mutable ndarray batching stays behind the GIL-build boundary."""
    np = pytest.importorskip("numpy")
    from fpstreams.tabular import numpy as numpy_adapter

    monkeypatch.setattr(sys, "_is_gil_enabled", lambda: False, raising=False)

    def reject_direct(_source: object) -> list[dict[str, object]]:
        raise AssertionError("free-threaded Python must retain canonical ndarray iteration")

    monkeypatch.setattr(numpy_adapter, "numpy_identity_rows", reject_direct)
    names = tuple(f"c{index}" for index in range(8))
    values = np.arange(8, dtype=np.int64).reshape(1, 8)
    source = fpstreams.rows.from_numpy(values, columns=names)
    execution = source.run_with_report("to_list")

    assert execution.value == [dict(zip(names, range(8), strict=True))]
    assert execution.report.strategy == "planned:python"


@pytest.mark.skipif(not hasattr(signal, "setitimer"), reason="requires POSIX interval timers")
def test_numpy_direct_row_paths_deopt_while_an_interval_timer_is_active() -> None:
    """Identity, safe-prefix, and grouped sinks share one conservative timer gate."""
    np = pytest.importorskip("numpy")

    source = fpstreams.rows.from_numpy(
        np.asarray([[1, 2], [1, 3]], dtype=np.int64),
        columns=("key", "value"),
    )
    queries = (
        source,
        source.where(fpstreams.col("value") >= 2).select("key"),
        source.group_by("key").aggregate(total=fpstreams.agg.sum("value")),
    )
    previous_timer = signal.getitimer(signal.ITIMER_REAL)
    signal.setitimer(signal.ITIMER_REAL, 60.0)
    try:
        executions = tuple(query.run_with_report("to_list") for query in queries)
    finally:
        signal.setitimer(signal.ITIMER_REAL, *previous_timer)

    assert [execution.value for execution in executions] == [
        [{"key": 1, "value": 2}, {"key": 1, "value": 3}],
        [{"key": 1}, {"key": 1}],
        [{"key": 1, "total": 5}],
    ]
    assert [execution.report.strategy for execution in executions] == [
        "planned:python",
        "planned:python",
        "planned:python",
    ]


@pytest.mark.skipif(not hasattr(signal, "SIGUSR1"), reason="requires a user signal")
def test_numpy_direct_row_paths_ignore_an_idle_custom_signal_handler() -> None:
    """Installing an unrelated handler alone must not disable columnar execution."""
    np = pytest.importorskip("numpy")

    source = fpstreams.rows.from_numpy(
        np.asarray([[1, 2], [1, 3]], dtype=np.int64),
        columns=("key", "value"),
    )
    queries = (
        source,
        source.where(fpstreams.col("value") >= 2).select("key"),
        source.group_by("key").aggregate(total=fpstreams.agg.sum("value")),
    )
    previous_handler = signal.signal(signal.SIGUSR1, lambda *_arguments: None)
    try:
        executions = tuple(query.run_with_report("to_list") for query in queries)
    finally:
        signal.signal(signal.SIGUSR1, previous_handler)

    assert [execution.value for execution in executions] == [
        [{"key": 1, "value": 2}, {"key": 1, "value": 3}],
        [{"key": 1}, {"key": 1}],
        [{"key": 1, "total": 5}],
    ]
    assert [execution.report.strategy for execution in executions] == [
        "numpy_direct",
        "numpy_direct",
        "numpy_direct",
    ]


def test_identity_numpy_to_list_reports_its_real_direct_strategy() -> None:
    """Reported NumPy identity collection names the source-level route it executed."""
    np = pytest.importorskip("numpy")

    source = fpstreams.rows.from_numpy(
        np.asarray([[1, 2], [3, 4]], dtype=np.int64),
        columns=("left", "right"),
    )

    rows_execution = source.run_with_report("to_list")
    flow_execution = source.to_flow().run_with_report("to_list")

    assert (
        rows_execution.value
        == flow_execution.value
        == [
            {"left": 1, "right": 2},
            {"left": 3, "right": 4},
        ]
    )
    assert rows_execution.report.compiler_engine == "python"
    assert flow_execution.report.compiler_engine == "python"
    assert rows_execution.report.strategy == "numpy_direct"
    assert flow_execution.report.strategy == "numpy_direct"


@pytest.mark.parametrize("width", [5, 6, 7, 8])
def test_wide_identity_numpy_to_list_uses_the_bounded_direct_strategy(width: int) -> None:
    """Common wider records share the bounded source-level identity collector."""
    np = pytest.importorskip("numpy")

    names = tuple(f"c{index}" for index in range(width))
    values = np.arange(width * 2, dtype=np.int64).reshape(2, width)
    execution = fpstreams.rows.from_numpy(values, columns=names).run_with_report("to_list")

    assert execution.value == [dict(zip(names, row, strict=True)) for row in values.tolist()]
    assert execution.report.strategy == "numpy_direct"


def test_wider_identity_numpy_to_list_keeps_the_canonical_fallback() -> None:
    """Unbounded schema specialization stops after the common eight-column range."""
    np = pytest.importorskip("numpy")

    names = tuple(f"c{index}" for index in range(9))
    values = np.arange(18, dtype=np.int64).reshape(2, 9)
    execution = fpstreams.rows.from_numpy(values, columns=names).run_with_report("to_list")

    assert execution.value == [dict(zip(names, row, strict=True)) for row in values.tolist()]
    assert execution.report.strategy == "planned:python"


def test_numpy_to_list_subclasses_keep_their_canonical_boundaries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Rows iteration and Flow execution subclasses both bypass the retained direct sink."""
    np = pytest.importorskip("numpy")
    from fpstreams.streams.flow import Flow
    from fpstreams.tabular import numpy as numpy_adapter
    from fpstreams.tabular.rows import Rows

    marker = {"left": 9, "right": 10}

    class CustomRows(Rows[dict[str, int]]):
        def __iter__(self) -> Iterator[dict[str, int]]:
            return iter((marker,))

    class CustomFlow(Flow[dict[str, int]]):
        pass

    source = fpstreams.rows.from_numpy(
        np.asarray([[1, 2]], dtype=np.int64),
        columns=("left", "right"),
    )
    custom_rows = CustomRows(source._flow)
    custom_flow = CustomFlow(source._flow._pipeline.source)

    def reject_direct(_source: object) -> list[dict[str, object]]:
        raise AssertionError("Flow subclasses must stay on canonical physical execution")

    monkeypatch.setattr(numpy_adapter, "numpy_identity_rows", reject_direct)

    assert custom_rows.to_list() == [marker]
    assert custom_flow.to_list() == [{"left": 1, "right": 2}]


def test_identity_numpy_to_list_declines_python_operations_and_failpoints() -> None:
    """Only an uninstrumented auto identity plan may collect retained NumPy rows directly."""
    np = pytest.importorskip("numpy")
    from fpstreams.runtime.failpoints import failpoint

    values = np.asarray([[1, 2], [3, 4]], dtype=np.int64)
    source = fpstreams.rows.from_numpy(values, columns=("left", "right"))

    forced = source.with_engine("python").run_with_report("to_list")
    assert forced.value == [{"left": 1, "right": 2}, {"left": 3, "right": 4}]
    assert forced.report.strategy == "planned:python"
    assert source.select("left").to_list() == [{"left": 1}, {"left": 3}]

    failure = RuntimeError("observed NumPy source open")
    with (
        failpoint("source.open.after", failure),
        pytest.raises(RuntimeError, match="observed NumPy source open") as captured,
    ):
        source.to_list()
    assert captured.value is failure


@pytest.mark.parametrize("name", ["min", "max"])
def test_numpy_identity_rows_keep_internal_extrema_stable(name: str) -> None:
    """Batch bounds use canonical helpers that the Python row source never resolves."""
    import builtins

    np = pytest.importorskip("numpy")
    rows = fpstreams.rows.from_numpy(
        np.asarray([[1, 2], [3, 4]], dtype=np.int64),
        columns=("left", "right"),
    )
    original = getattr(builtins, name)
    try:
        setattr(builtins, name, lambda *_args, **_kwargs: False)
        execution = rows.run_with_report("to_list")
    finally:
        setattr(builtins, name, original)

    assert execution.value == [{"left": 1, "right": 2}, {"left": 3, "right": 4}]
    assert execution.report.strategy == "numpy_direct"


def test_numpy_prefix_snapshots_selection_error_before_lazy_import() -> None:
    """Lazy prefix loading must keep the exception class bound by eager selectors."""
    completed = _run_inline_python(
        """
import sys
import fpstreams
import fpstreams.errors as errors

selection_error_type = errors.SelectionError
assert "fpstreams.execution.numpy_prefix" not in sys.modules

class ReplacedSelectionError(selection_error_type):
    pass

errors.SelectionError = ReplacedSelectionError

import numpy as np
rows = fpstreams.rows.from_numpy(
    np.asarray([[1]], dtype=np.int64),
    columns=("value",),
)
automatic = rows.select("missing")
canonical = rows.with_engine("python").select("missing")

def capture_error(query):
    try:
        query.to_list()
    except BaseException as error:
        return error
    raise AssertionError("missing selector unexpectedly succeeded")

canonical_error = capture_error(canonical)
automatic_error = capture_error(automatic)
assert type(automatic_error) is type(canonical_error) is selection_error_type
assert str(automatic_error) == str(canonical_error)
"""
    )

    assert completed.returncode == 0, completed.stderr


def test_numpy_kernels_do_not_execute_lazy_typing_casts() -> None:
    """Runtime kernels should not depend on a typing-only helper captured during lazy import."""
    completed = _run_inline_python(
        """
import sys
import typing
import fpstreams
import numpy as np

rows = fpstreams.rows.from_numpy(
    np.asarray([[1, 2], [1, 3]], dtype=np.int64),
    columns=("key", "value"),
)
computed_auto = rows.with_columns(next=fpstreams.col("value") + 1)
computed_python = rows.with_engine("python").with_columns(next=fpstreams.col("value") + 1)
group_auto = rows.group_by("key").aggregate(total=fpstreams.agg.sum("value"))
group_python = rows.with_engine("python").group_by("key").aggregate(
    total=fpstreams.agg.sum("value")
)
assert "fpstreams.execution.numpy_prefix" not in sys.modules
assert "fpstreams.execution.numpy_group" not in sys.modules

original_cast = typing.cast
def changed_cast(target, value):
    if getattr(target, "__origin__", None) is list:
        return []
    if getattr(target, "__name__", None) == "NumpyGroupAggregateSpec":
        raise RuntimeError("typing.cast reached grouped execution")
    return original_cast(target, value)

typing.cast = changed_cast
try:
    expected_computed = computed_python.to_list()
    expected_grouped = group_python.to_list()
    first_computed = computed_auto.run_with_report("to_list")
    computed = computed_auto.run_with_report("to_list")
    grouped = group_auto.run_with_report("to_list")
finally:
    typing.cast = original_cast

assert computed.value == expected_computed
assert grouped.value == expected_grouped
assert first_computed.report.strategy == "planned:python"
assert computed.report.strategy == "numpy_direct"
assert grouped.report.strategy == "numpy_direct"
"""
    )

    assert completed.returncode == 0, completed.stderr


def test_numpy_identity_source_declines_replaced_instance_factory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retained NumPy metadata cannot outrank a replaced live source factory."""
    np = pytest.importorskip("numpy")

    query = fpstreams.rows.from_numpy(
        np.asarray([[1], [2]], dtype=np.int64),
        columns=("value",),
    )
    source = query._flow._pipeline.source
    opens = 0

    def replacement() -> Iterator[dict[str, int]]:
        nonlocal opens
        opens += 1
        return iter(({"value": 9},))

    monkeypatch.setattr(source, "_factory", replacement)
    execution = query.run_with_report("to_list")

    assert execution.value == [{"value": 9}]
    assert execution.report.strategy == "planned:python"
    assert opens == 1


@pytest.mark.skipif(not hasattr(signal, "setitimer"), reason="requires POSIX interval timers")
def test_numpy_identity_to_numpy_copy_false_ignores_row_observers() -> None:
    """Row observers and unused column protocols cannot change ndarray copy semantics."""
    np = pytest.importorskip("numpy")

    class ArmedColumn(str):
        calls = 0

        def __hash__(self) -> int:
            type(self).calls += 1
            return super().__hash__()

    values = np.asarray([[1, 2], [3, 4]], dtype=np.int64)
    name = ArmedColumn("left")
    rows = fpstreams.rows.from_numpy(values, columns=(name, "right"))
    ArmedColumn.calls = 0

    def trace(_frame: object, _event: str, _argument: object) -> object:
        return trace

    previous_trace = sys.gettrace()
    previous_timer = signal.getitimer(signal.ITIMER_REAL)
    sys.settrace(trace)
    signal.setitimer(signal.ITIMER_REAL, 60.0)
    try:
        result = rows.to_numpy(copy=False)
    finally:
        signal.setitimer(signal.ITIMER_REAL, *previous_timer)
        sys.settrace(previous_trace)

    assert np.shares_memory(result, values)
    assert ArmedColumn.calls == 0


def test_identity_numpy_to_columns_never_boxes_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A retained matrix should transpose its Python values without row dictionaries."""
    np = pytest.importorskip("numpy")
    from fpstreams.tabular import io as tabular_io

    values = np.asarray(
        [[1, float("nan"), "first"], [2, -0.0, None]],
        dtype=object,
    )
    source = fpstreams.rows.from_numpy(values, columns=("id", "score", "label"))

    def reject_rows(_row: object) -> dict[str, object]:
        raise AssertionError("identity NumPy to_columns must not box rows")

    monkeypatch.setattr(tabular_io, "_as_record", reject_rows)

    columns = source.to_columns()

    assert list(columns) == ["id", "score", "label"]
    assert columns["id"] == [1, 2]
    assert math.isnan(columns["score"][0])
    assert math.copysign(1.0, columns["score"][1]) == -1.0
    assert columns["label"] == ["first", None]


def test_identity_numpy_arrow_materializers_never_box_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Arrow and pandas sinks should share the retained matrix column conversion."""
    np = pytest.importorskip("numpy")
    from fpstreams.tabular import io as tabular_io

    values = np.asarray([[1.25, 2.5], [3.5, float("nan")]], dtype=np.float32)
    source = fpstreams.rows.from_numpy(values, columns=("left", "right"))

    def reject_rows(_row: object) -> Mapping[str, object]:
        raise AssertionError("identity NumPy Arrow sinks must not box rows")

    monkeypatch.setattr(tabular_io, "_record_view", reject_rows)

    table = source.to_arrow(batch_size=1)
    frame = source.to_pandas(batch_size=1)

    assert table.schema == pa.schema([("left", pa.float64()), ("right", pa.float64())])
    assert [batch.num_rows for batch in table.to_batches()] == [1, 1]
    assert table.column("left").to_pylist() == [1.25, 3.5]
    assert table.column("right")[0].as_py() == 2.5
    assert math.isnan(table.column("right")[1].as_py())
    assert list(frame.columns) == ["left", "right"]
    assert frame["left"].tolist() == [1.25, 3.5]
    assert math.isnan(frame["right"].iloc[1])


def test_numpy_arrow_resize_after_first_batch_has_one_adapter_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Auto and Python scans should reject a live dimensionality change identically."""
    np = pytest.importorskip("numpy")
    from fpstreams.tabular import arrow as arrow_adapter
    from fpstreams.tabular import io as tabular_io

    def resizing_arrow_modules(values: object) -> Callable[[], tuple[object, object, object]]:
        resized = False

        class RecordBatchProxy:
            @staticmethod
            def from_pydict(columns: object, schema: object = None) -> pa.RecordBatch:
                nonlocal resized
                batch = pa.RecordBatch.from_pydict(columns, schema=schema)
                if not resized:
                    resized = True
                    values.resize((values.size,), refcheck=False)  # type: ignore[attr-defined]
                return batch

        class PyArrowProxy:
            RecordBatch = RecordBatchProxy

            def __getattr__(self, name: str) -> object:
                return getattr(pa, name)

        proxy = PyArrowProxy()

        def modules() -> tuple[object, object, object]:
            return proxy, ds, pq

        return modules

    errors: list[BaseException] = []
    for engine in ("auto", "python"):
        values = np.asarray([[1, 2], [3, 4]], dtype=np.int64)
        source = fpstreams.rows.from_numpy(values, columns=("left", "right")).with_engine(engine)
        arrow_modules = resizing_arrow_modules(values)

        monkeypatch.setattr(tabular_io, "_arrow_modules", arrow_modules)
        monkeypatch.setattr(arrow_adapter, "_arrow_modules", arrow_modules)

        try:
            source.to_arrow(batch_size=1)
        except BaseException as error:
            errors.append(error)
        else:
            raise AssertionError(f"{engine} scan accepted a retained 2D-to-1D resize")

        values.resize((2, 2), refcheck=False)
        assert source.to_arrow(batch_size=1).to_pylist() == [
            {"left": 1, "right": 2},
            {"left": 3, "right": 4},
        ]

    assert [type(error) for error in errors] == [ValueError, ValueError]
    assert [str(error) for error in errors] == [
        "from_numpy() retained array changed to 1 dimensions during iteration",
        "from_numpy() retained array changed to 1 dimensions during iteration",
    ]


def test_numpy_to_list_resize_after_sink_entry_has_one_adapter_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Direct and forced-Python list sinks reject the same live dimensionality change."""
    np = pytest.importorskip("numpy")
    from fpstreams.tabular import numpy as numpy_adapter

    canonical_width = numpy_adapter._retained_numpy_width
    errors: list[BaseException] = []
    for engine in ("auto", "python"):
        names = tuple(f"c{index}" for index in range(8))
        values = np.arange(16, dtype=np.int64).reshape(2, 8)
        source = fpstreams.rows.from_numpy(values, columns=names).with_engine(engine)
        resized = False

        def resize_after_entry(matrix: object, names: tuple[str, ...]) -> int:
            nonlocal resized
            width = canonical_width(matrix, names)
            if not resized:
                resized = True
                matrix.resize((matrix.size,), refcheck=False)  # type: ignore[attr-defined]
            return width

        monkeypatch.setattr(numpy_adapter, "_retained_numpy_width", resize_after_entry)
        try:
            source.to_list()
        except BaseException as error:
            errors.append(error)
        else:
            raise AssertionError(f"{engine} list accepted a retained 2D-to-1D resize")

        monkeypatch.setattr(numpy_adapter, "_retained_numpy_width", canonical_width)
        values.resize((2, 8), refcheck=False)
        assert source.to_list() == [dict(zip(names, row, strict=True)) for row in values.tolist()]

    assert [type(error) for error in errors] == [ValueError, ValueError]
    assert [str(error) for error in errors] == [
        "from_numpy() retained array changed to 1 dimensions during iteration",
        "from_numpy() retained array changed to 1 dimensions during iteration",
    ]


def test_numpy_group_deopts_after_executor_builtin_alias_replacement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Group kernels cannot observe a replaced executor integer constructor."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution import numpy_group

    source = (
        fpstreams.rows.from_numpy(
            np.asarray([[1, 10], [1, 20], [2, 30]], dtype=np.int64),
            columns=("key", "value"),
        )
        .group_by("key")
        .aggregate(rows=fpstreams.agg.count(), total=fpstreams.agg.sum("value"))
    )
    first = source.run_with_report("to_list")
    assert first.report.strategy == "numpy_direct"

    def unexpected(*_arguments: object, **_keywords: object) -> object:
        raise AssertionError("replaced executor int reached NumPy grouping")

    monkeypatch.setattr(numpy_group, "_BUILTIN_INT", unexpected)
    execution = source.run_with_report("to_list")

    assert execution.value == [
        {"key": 1, "rows": 2, "total": 30},
        {"key": 2, "rows": 1, "total": 30},
    ]
    assert execution.report.strategy == "planned:python"


def test_numpy_with_columns_compiles_before_reusing_its_cached_direct_plan() -> None:
    """Fresh lazy expressions compile canonically once before a later direct execution."""
    np = pytest.importorskip("numpy")
    source = fpstreams.rows.from_numpy(
        np.asarray([[1], [2]], dtype=np.int64),
        columns=("value",),
    ).with_columns(score=fpstreams.col("value") + 1)

    first = source.run_with_report("to_list")
    second = source.run_with_report("to_list")

    assert (
        first.value
        == second.value
        == [
            {"value": 1, "score": 2},
            {"value": 2, "score": 3},
        ]
    )
    assert first.report.strategy == "planned:python"
    assert second.report.strategy == "numpy_direct"


def test_flow_from_numpy_adapts_one_dimensional_scalars_lazily() -> None:
    """A 1D explicit adapter must stay live and emit ordinary Python scalar values."""
    np = pytest.importorskip("numpy")

    source = np.asarray([1, 2, 3], dtype=np.int64)
    pipeline = fpstreams.flow.from_numpy(source)

    source[1] = 20
    assert pipeline.to_list() == [1, 20, 3]
    assert all(type(value) is int for value in pipeline)
    source.resize((4,), refcheck=False)
    source[3] = 40
    assert pipeline.count() == 4
    assert pipeline.with_engine("python").to_list() == [1, 20, 3, 40]

    source.resize((2, 2), refcheck=False)
    with pytest.raises(ValueError, match="changed to 2 dimensions"):
        pipeline.count()

    with pytest.raises(ValueError, match=r"columns.*two-dimensional"):
        fpstreams.flow.from_numpy(np.arange(2), columns=("value",))


def test_flow_from_numpy_i64_supports_exact_native_full_scan_terminals() -> None:
    """The buffer kernel must preserve Python integers and statistical results."""
    np = pytest.importorskip("numpy")

    maximum = 2**63 - 1
    wide_source = np.asarray([maximum, maximum], dtype=np.int64)
    wide = fpstreams.flow.from_numpy(wide_source).with_engine("native")
    assert wide.sum() == 2 * maximum
    assert type(wide.sum()) is int
    assert wide.aggregate(rows=fpstreams.agg.count(), total=fpstreams.agg.sum()) == {
        "rows": 2,
        "total": 2 * maximum,
    }
    assert wide.min() == maximum
    assert wide.max() == maximum
    wide_source[0] = -maximum
    assert wide.sum() == 0

    statistics = fpstreams.flow.from_numpy(np.asarray([1, 2, 3, 4], dtype=np.int64)).with_engine(
        "native"
    )
    assert statistics.mean() == 2.5
    assert statistics.variance() == pytest.approx(5 / 3)
    assert statistics.std() == pytest.approx((5 / 3) ** 0.5)

    empty = fpstreams.flow.from_numpy(np.asarray([], dtype=np.int64)).with_engine("native")
    assert empty.sum() == 0
    assert empty.mean() is None
    assert empty.variance() is None
    with pytest.raises(fpstreams.EmptyFlowError):
        empty.min()

    automatic = fpstreams.flow.from_numpy(np.arange(32, dtype=np.int64))
    assert automatic.explain("sum").to_dict()["selected_engine"] == "native"
    assert automatic.explain("list").to_dict()["selected_engine"] == "native"
    assert automatic.to_list() == list(range(32))

    from fpstreams.runtime.failpoints import failpoint

    failure = RuntimeError("instrumented NumPy source")
    with failpoint("source.open.after", failure), pytest.raises(RuntimeError) as captured:
        automatic.sum()
    assert captured.value is failure


@pytest.mark.parametrize("dtype", ["int64", "float64"])
def test_flow_from_numpy_terminal_planning_classifies_buffer_once(
    monkeypatch: pytest.MonkeyPatch,
    dtype: str,
) -> None:
    """One engine decision should not repeatedly revalidate one retained buffer."""
    np = pytest.importorskip("numpy")
    from fpstreams.planning import native

    pipeline = fpstreams.flow.from_numpy(np.arange(32, dtype=dtype))
    original = native._numpy_buffer_kind
    calls = 0

    def counted_buffer_kind(source: object) -> object:
        nonlocal calls
        calls += 1
        return original(source)

    monkeypatch.setattr(native, "_numpy_buffer_kind", counted_buffer_kind)

    decision = native.select_terminal_engine(pipeline._pipeline, "sum")

    assert decision.engine == "native"
    assert calls == 1


@pytest.mark.parametrize("engine", ["auto", "native"])
@pytest.mark.parametrize("dtype", ["int64", "float64"])
def test_flow_from_numpy_operation_planning_classifies_buffer_once(
    monkeypatch: pytest.MonkeyPatch,
    engine: str,
    dtype: str,
) -> None:
    """A fused decision should reuse its validated buffer kind throughout planning."""
    np = pytest.importorskip("numpy")
    from fpstreams.planning import native

    expression = fpstreams.item + 1 if dtype == "int64" else fpstreams.fitem + 1.0
    pipeline = (
        fpstreams.flow.from_numpy(np.arange(32, dtype=dtype)).map(expression).with_engine(engine)
    )
    original = native._numpy_buffer_kind
    calls = 0

    def counted_buffer_kind(source: object) -> object:
        nonlocal calls
        calls += 1
        return original(source)

    monkeypatch.setattr(native, "_numpy_buffer_kind", counted_buffer_kind)

    decision = native.select_terminal_engine(pipeline._pipeline, "sum")

    assert decision.engine == "native"
    assert calls == 1


def test_flow_from_numpy_i64_identity_min_max_use_numpy_direct_reduction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exact identity extrema may skip planning without widening NumPy semantics."""
    np = pytest.importorskip("numpy")

    values = np.asarray(
        [2**63 - 1, -7, -(2**63), 11, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12],
        dtype=np.int64,
    )
    pipeline = fpstreams.flow.from_numpy(values)
    tiny = fpstreams.flow.from_numpy(np.asarray([2, 1], dtype=np.int64))

    minimum = pipeline.run_with_report("min")
    maximum = pipeline.run_with_report("max")

    assert minimum.value == -(2**63)
    assert maximum.value == 2**63 - 1
    assert type(minimum.value) is type(maximum.value) is int
    assert minimum.report.compiler_engine == maximum.report.compiler_engine == "not_compiled"
    assert minimum.report.strategy == maximum.report.strategy == "numpy_direct"

    forced = pipeline.with_engine("python").run_with_report("min")
    transformed = pipeline.map(fpstreams.item + 0).run_with_report("max")
    strided = fpstreams.flow.from_numpy(values[::2]).run_with_report("min")

    assert forced.value == strided.value == -(2**63)
    assert transformed.value == 2**63 - 1
    assert forced.report.strategy != "numpy_direct"
    assert transformed.report.strategy != "numpy_direct"
    assert strided.report.strategy != "numpy_direct"

    empty = fpstreams.flow.from_numpy(np.asarray([], dtype=np.int64))
    with pytest.raises(fpstreams.EmptyFlowError, match=r"min\(\)") as empty_failure:
        empty.min()
    assert isinstance(empty_failure.value.__context__, ValueError)
    assert empty_failure.value.__suppress_context__ is True

    from fpstreams.runtime.failpoints import failpoint

    failure = RuntimeError("instrumented NumPy min")
    with failpoint("source.open.after", failure), pytest.raises(RuntimeError) as captured:
        pipeline.min()
    assert captured.value is failure

    from fpstreams.runtime import failpoints as failpoints_module

    def replaced_hit(name: str) -> None:
        raise RuntimeError(f"replaced hit:{name}")

    with monkeypatch.context() as scoped:
        scoped.setattr(failpoints_module, "hit", replaced_hit)
        with pytest.raises(RuntimeError, match=r"replaced hit:source.open.after"):
            tiny.min()

    import builtins

    for terminal in ("min", "max"):
        message = f"replaced builtin {terminal}"

        def replaced(*_args: object, _message: str = message, **_kwargs: object) -> object:
            raise RuntimeError(_message)

        captured_builtin: BaseException | None = None
        with monkeypatch.context() as scoped:
            scoped.setattr(builtins, terminal, replaced)
            try:
                getattr(tiny, terminal)()
            except BaseException as error:
                captured_builtin = error
        assert type(captured_builtin) is RuntimeError
        assert str(captured_builtin) == message

    from fpstreams.tabular.numpy import NumpyColumnSource

    def replaced_len(_source: object) -> int:
        raise RuntimeError("replaced NumPy column length")

    with monkeypatch.context() as scoped:
        scoped.setattr(NumpyColumnSource, "__len__", replaced_len)
        with pytest.raises(RuntimeError, match="replaced NumPy column length"):
            pipeline.min()

    def replaced_size(_source: object) -> int:
        raise RuntimeError("replaced exact size")

    from fpstreams.planning.source import Source

    with monkeypatch.context() as scoped:
        scoped.setattr(Source, "current_exact_size", replaced_size)
        with pytest.raises(RuntimeError, match="replaced exact size"):
            pipeline.min()

    rebound = fpstreams.flow.from_numpy(np.asarray([0, 1], dtype=np.int64))
    rebound_source = rebound._logical_plan.root.source  # type: ignore[attr-defined]
    rebound_source.native_data = NumpyColumnSource(np.asarray([101, 102], dtype=np.int64))
    rebound_execution = rebound.run_with_report("min")
    assert rebound_execution.value == 0
    assert rebound_execution.report.strategy != "numpy_direct"

    from fpstreams.streams import flow_terminals

    with (
        monkeypatch.context() as scoped,
        failpoint("source.open.after", RuntimeError("active patched failpoint")),
    ):
        scoped.setattr(flow_terminals, "has_active_failpoints", lambda: False)
        with pytest.raises(RuntimeError, match="active patched failpoint"):
            tiny.min()


def test_flow_from_numpy_i64_fuses_expression_pipelines() -> None:
    """A numeric buffer source should compose with ordinary native map/filter stages."""
    np = pytest.importorskip("numpy")

    source = np.arange(16, dtype=np.int64)
    pipeline = (
        fpstreams.flow.from_numpy(source)
        .map(fpstreams.item * 3 + 1)
        .filter(fpstreams.item % 2 == 0)
    )

    expected = [4, 10, 16, 22, 28, 34, 40, 46]
    assert pipeline.with_engine("native").to_list() == expected
    assert pipeline.with_engine("native").sum() == 200
    assert pipeline.explain("sum").to_dict()["selected_engine"] == "native"
    movement = pipeline.explain("list").to_dict()["data_movement"]
    assert movement["scans_source"] is True
    assert movement["copies_source"] is True

    source[0] = 3
    assert pipeline.with_engine("native").to_list() == [10, *expected]

    overflowing = fpstreams.flow.from_numpy(np.asarray([2**62, *([0] * 7)], dtype=np.int64)).map(
        fpstreams.item * 4
    )
    assert overflowing.to_list() == [2**64, *([0] * 7)]
    with pytest.raises(OverflowError):
        overflowing.with_engine("native").to_list()

    short_circuiting = fpstreams.flow.from_numpy(np.asarray([2, 0], dtype=np.int64)).map(
        10 // fpstreams.item
    )
    assert short_circuiting.first() == 5
    assert short_circuiting.aggregate(first=fpstreams.agg.first()) == {"first": 5}
    with pytest.raises(fpstreams.NativeUnsupportedError, match="short-circuiting"):
        short_circuiting.with_engine("native").first()
    with pytest.raises(fpstreams.NativeUnsupportedError, match="short-circuiting"):
        short_circuiting.with_engine("native").aggregate(first=fpstreams.agg.first())


def test_flow_from_numpy_non_native_i64_layout_stays_on_the_python_path() -> None:
    """Strided and opposite-endian arrays must not be silently copied into the buffer kernel."""
    np = pytest.importorskip("numpy")

    strided = np.arange(64, dtype=np.int64)[::2]
    pipeline = fpstreams.flow.from_numpy(strided)
    assert pipeline.to_list() == list(range(0, 64, 2))
    assert pipeline.explain("sum").to_dict()["selected_engine"] == "python"
    with pytest.raises(fpstreams.NativeUnsupportedError):
        pipeline.with_engine("native").sum()

    storage = np.arange(8 * 33 + 1, dtype=np.uint8)
    unaligned = storage[1:].view(np.int64)
    unaligned_flow = fpstreams.flow.from_numpy(unaligned)
    assert unaligned.flags.c_contiguous and not unaligned.flags.aligned
    assert unaligned_flow.explain("sum").to_dict()["selected_engine"] == "python"
    with pytest.raises(fpstreams.NativeUnsupportedError):
        unaligned_flow.with_engine("native").sum()

    opposite_dtype = ">i8" if sys.byteorder == "little" else "<i8"
    opposite = np.asarray([1, 2, 3], dtype=opposite_dtype)
    assert fpstreams.flow.from_numpy(opposite).sum() == 6

    from fpstreams import _native

    with pytest.raises(TypeError, match="native-endian"):
        _native.aggregate_i64_buffer_masked_v1(opposite, [], 1 << 1)


def test_flow_from_numpy_f64_preserves_python_full_scan_terminal_semantics() -> None:
    """A float buffer fast path must keep Python numeric and empty-flow contracts."""
    np = pytest.importorskip("numpy")

    cancellation_values = np.asarray([1e16, 1.0, -1e16], dtype=np.float64)
    cancellation = fpstreams.flow.from_numpy(cancellation_values)
    expected_sum = cancellation.with_engine("python").sum()
    assert cancellation.explain("list").to_dict()["selected_engine"] == "python"
    assert cancellation.with_engine("native").to_list() == cancellation_values.tolist()
    assert cancellation.with_engine("native").sum() == expected_sum
    assert cancellation.with_engine("native").mean() == cancellation.with_engine("python").mean()
    assert cancellation.with_engine("native").variance() == pytest.approx(
        cancellation.with_engine("python").variance()
    )

    aggregate_cancellation = fpstreams.flow.from_numpy(
        np.asarray([1e16, 1.0, -1e16, 0.0, 0.0, 0.0, 0.0, 0.0], dtype=np.float64)
    )
    expected_total = aggregate_cancellation.with_engine("python").aggregate(
        total=fpstreams.agg.sum()
    )
    automatic_total = aggregate_cancellation.aggregate(total=fpstreams.agg.sum())
    assert automatic_total == expected_total
    assert type(automatic_total["total"]) is float
    mixed_total = aggregate_cancellation.with_engine("native").aggregate(
        total=fpstreams.agg.sum(), minimum=fpstreams.agg.min()
    )
    assert mixed_total == aggregate_cancellation.with_engine("python").aggregate(
        total=fpstreams.agg.sum(), minimum=fpstreams.agg.min()
    )
    assert type(mixed_total["total"]) is float

    automatic = fpstreams.flow.from_numpy(np.arange(32, dtype=np.float64))
    assert automatic.explain("list").to_dict()["selected_engine"] == "native"
    assert automatic.to_list() == np.arange(32, dtype=np.float64).tolist()

    empty = fpstreams.flow.from_numpy(np.asarray([], dtype=np.float64)).with_engine("native")
    assert empty.sum() == 0
    assert type(empty.sum()) is int
    assert empty.aggregate(total=fpstreams.agg.sum()) == {"total": 0}
    assert type(empty.aggregate(total=fpstreams.agg.sum())["total"]) is int
    assert empty.mean() is None
    assert empty.variance() is None

    filtered_empty = fpstreams.flow.from_numpy(np.arange(16, dtype=np.float64)).filter(
        fpstreams.fitem < 0.0
    )
    assert filtered_empty.with_engine("native").sum() == 0
    assert type(filtered_empty.with_engine("native").sum()) is int
    assert filtered_empty.with_engine("native").aggregate(total=fpstreams.agg.sum()) == {"total": 0}
    assert (
        type(filtered_empty.with_engine("native").aggregate(total=fpstreams.agg.sum())["total"])
        is int
    )

    nan = float("nan")
    leading_nan = fpstreams.flow.from_numpy(np.asarray([nan, 1.0], dtype=np.float64)).with_engine(
        "native"
    )
    assert math.isnan(leading_nan.min())
    assert math.isnan(leading_nan.max())

    signed_zero = fpstreams.flow.from_numpy(np.asarray([-0.0, 0.0], dtype=np.float64)).with_engine(
        "native"
    )
    assert math.copysign(1.0, signed_zero.min()) == -1.0
    assert math.copysign(1.0, signed_zero.max()) == -1.0


def test_flow_from_numpy_f64_fuses_expression_pipelines_without_layout_coercion() -> None:
    """Float expressions should reuse the generic kernel only for exact contiguous buffers."""
    np = pytest.importorskip("numpy")

    source = np.arange(32, dtype=np.float64)
    pipeline = (
        fpstreams.flow.from_numpy(source)
        .map(fpstreams.fitem * 1.5 + 0.25)
        .filter(fpstreams.fitem > 20.0)
    )
    expected = pipeline.with_engine("python").to_list()
    assert pipeline.with_engine("native").to_list() == expected
    assert pipeline.count() == len(expected)
    assert pipeline.with_engine("native").count() == len(expected)
    assert pipeline.aggregate(rows=fpstreams.agg.count()) == {"rows": len(expected)}
    assert pipeline.with_engine("native").sum() == pipeline.with_engine("python").sum()
    assert pipeline.explain("sum").to_dict()["selected_engine"] == "native"

    source[31] = 64.0
    assert pipeline.with_engine("native").to_list() == pipeline.with_engine("python").to_list()

    strided = fpstreams.flow.from_numpy(np.arange(64, dtype=np.float64)[::2])
    assert strided.explain("sum").to_dict()["selected_engine"] == "python"
    with pytest.raises(fpstreams.NativeUnsupportedError):
        strided.with_engine("native").sum()

    opposite_dtype = ">f8" if sys.byteorder == "little" else "<f8"
    opposite = fpstreams.flow.from_numpy(np.asarray([1.0, 2.0, 3.0], dtype=opposite_dtype))
    assert opposite.sum() == 6.0
    assert opposite.explain("sum").to_dict()["selected_engine"] == "python"


def test_numpy_adapter_count_tracks_a_resized_retained_array() -> None:
    """Live ndarray row cardinality stays aligned with lazy iteration and explain output."""
    np = pytest.importorskip("numpy")

    source = np.asarray([[1], [2]])
    table = fpstreams.rows.from_numpy(source, columns=["value"])
    assert table.count() == 2

    source.resize((3, 1), refcheck=False)
    source[2, 0] = 3

    assert table.count() == 3
    assert table.to_list() == [{"value": 1}, {"value": 2}, {"value": 3}]
    explanation = table.explain("count").to_dict()
    assert explanation["source"]["exact_size"] == 3
    assert explanation["semantics"]["output"]["cardinality"] == {
        "kind": "exact",
        "value": 3,
    }

    source.resize((1, 3), refcheck=False)
    with pytest.raises(ValueError, match="retained array width changed"):
        table.count()
    with pytest.raises(ValueError, match="retained array width changed"):
        table.explain("count")
    with pytest.raises(ValueError, match="retained array width changed"):
        table.to_list()

    changing = np.array([[0, 1], [2, 3]])
    iterator = iter(fpstreams.rows.from_numpy(changing, columns=("left", "right")))
    assert next(iterator) == {"left": 0, "right": 1}
    changing.resize((2, 3), refcheck=False)
    with pytest.raises(ValueError, match="width changed during iteration"):
        next(iterator)


def test_numpy_adapter_validates_shape_and_column_names() -> None:
    np = pytest.importorskip("numpy")

    with pytest.raises(ValueError, match="two-dimensional"):
        fpstreams.rows.from_numpy(np.asarray([1, 2]))
    with pytest.raises(ValueError, match="2 columns"):
        fpstreams.rows.from_numpy(np.asarray([[1, 2]]), columns=["only"])
    with pytest.raises(fpstreams.DuplicateKeyError, match="duplicate column 'value'"):
        fpstreams.rows.from_numpy(
            np.asarray([[1, 2]]),
            columns=["value", "value"],
        )
    with pytest.raises(TypeError, match="column names must be strings"):
        fpstreams.rows.from_numpy(np.asarray([[1]]), columns=[0])
    with pytest.raises(ValueError, match="column names cannot be empty"):
        fpstreams.rows.from_numpy(np.asarray([[1]]), columns=[""])


def test_rows_to_numpy_preserves_selectors_dtype_and_empty_shapes() -> None:
    np = pytest.importorskip("numpy")

    table = fpstreams.rows([{"a": 1}, {"a": 2, "b": 3}])
    matrix = table.to_numpy(dtype=object)
    selected = fpstreams.rows([{"a": 1, "b": None}, {"a": 2, "b": 3}]).to_numpy(
        "b",
        lambda row: row["a"] * 10,
        dtype=object,
    )

    assert matrix.shape == (2, 2)
    assert matrix.tolist() == [[1, None], [2, 3]]
    assert selected.shape == (2, 2)
    assert selected.dtype == np.dtype("object")
    assert selected.tolist() == [[None, 10], [3, 20]]
    empty = fpstreams.rows([]).to_numpy()
    typed_empty = fpstreams.rows([]).to_numpy("a", "b", dtype=np.int16)
    retained_empty = fpstreams.rows.from_numpy(np.empty((0, 3), dtype=np.float32)).to_numpy()
    assert empty.shape == (0, 0)
    assert empty.dtype == np.dtype("float64")
    assert typed_empty.shape == (0, 2)
    assert typed_empty.dtype == np.dtype("int16")
    assert retained_empty.shape == (0, 3)
    assert retained_empty.dtype == np.dtype("float32")

    with pytest.raises(fpstreams.SelectionError, match="missing"):
        fpstreams.rows([{"a": 1}]).to_numpy("missing")


def test_rows_materializers_consume_the_flow_directly_without_bypassing_subclasses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Canonical Rows skip iterator forwarding while custom iteration remains authoritative."""
    from fpstreams.streams.flow import Flow
    from fpstreams.tabular.rows import Rows

    np = pytest.importorskip("numpy")

    consume_calls = 0
    original_consume = Flow._consume

    def tracked_consume(self: Flow[Any], consumer: Any) -> Any:
        nonlocal consume_calls
        consume_calls += 1
        return original_consume(self, consumer)

    monkeypatch.setattr(Flow, "_consume", tracked_consume)

    regular = fpstreams.rows([{"a": 1}])
    assert regular.to_numpy("a").tolist() == [[1]]
    assert consume_calls == 1
    assert regular.to_columns() == {"a": [1]}
    assert consume_calls == 2

    class CustomRows(Rows[dict[str, int]]):
        def __iter__(self) -> Iterator[dict[str, int]]:
            return iter(({"a": 9},))

    class CustomFlow(Flow[dict[str, int]]):
        def __iter__(self) -> Iterator[dict[str, int]]:
            return iter(({"a": 9},))

    assert CustomRows([{"a": 2}]).to_numpy("a").tolist() == [[9]]
    assert CustomRows([{"a": 2}]).to_columns() == {"a": [9]}

    numpy_backed = fpstreams.rows.from_numpy(np.asarray([[1]]), columns=("a",))
    assert CustomRows(numpy_backed._flow).to_numpy().tolist() == [[9]]

    pa = pytest.importorskip("pyarrow")
    arrow_backed = fpstreams.rows.from_arrow(pa.table({"a": [1]}))
    assert CustomRows(arrow_backed._flow).to_columns() == {"a": [9]}

    flow_backed = Rows(CustomFlow([{"a": 1}]))
    assert flow_backed.to_numpy("a").tolist() == [[9]]
    assert flow_backed.to_columns() == {"a": [9]}

    class OpaqueRows(CustomRows):
        def __getattribute__(self, name: str) -> Any:
            if name == "_flow":
                raise AssertionError("custom iteration must not inspect the backing flow")
            return super().__getattribute__(name)

    opaque = OpaqueRows([{"a": 1}])
    assert opaque.to_numpy("a").tolist() == [[9]]
    assert opaque.to_columns() == {"a": [9]}
    assert consume_calls == 2


@pytest.mark.parametrize("materializer", ["pairs", "rows_numpy"])
def test_custom_materializers_keep_primary_failure_when_iterator_close_fails(
    materializer: str,
) -> None:
    """Custom public iteration remains owned without replacing its primary failure."""
    from fpstreams.streams.pairs import Pairs
    from fpstreams.tabular.rows import Rows

    primary = ValueError("materializer failed")

    class FailingIterator(Iterator[Any]):
        def __init__(self) -> None:
            self.close_calls = 0

        def __next__(self) -> Any:
            raise primary

        def close(self) -> None:
            self.close_calls += 1
            raise OSError("custom iterator close failed")

    iterator = FailingIterator()
    if materializer == "pairs":

        class CustomPairs(Pairs[int, int]):
            def __iter__(self) -> Iterator[tuple[int, int]]:
                return cast(Iterator[tuple[int, int]], iterator)

        materialize = CustomPairs(fpstreams.flow([])).to_dict
    else:
        pytest.importorskip("numpy")

        class CustomRows(Rows[dict[str, int]]):
            def __iter__(self) -> Iterator[dict[str, int]]:
                return cast(Iterator[dict[str, int]], iterator)

        def materialize_rows() -> Any:
            return CustomRows([]).to_numpy("value")

        materialize = materialize_rows

    with pytest.raises(ValueError) as captured:
        materialize()

    assert captured.value is primary
    assert captured.value.__notes__ == ["cleanup failed with OSError: custom iterator close failed"]
    assert iterator.close_calls == 1


def test_rows_to_numpy_direct_fields_bypass_generated_selectors_for_exact_dicts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exact dictionaries use the direct field path while mixed rows retain fallback semantics."""
    from fpstreams.tabular import io as tabular_io

    original_compile = tabular_io.compile_selector
    selected_rows: list[object] = []

    def compile_tracked(selector: object) -> Any:
        compiled = original_compile(selector)  # type: ignore[arg-type]

        def tracked(row: object) -> object:
            selected_rows.append(row)
            return compiled(row)

        return tracked

    class DictSubclass(dict[str, int]):
        pass

    exact = {"a": 1, "b": 2, "c": 3}
    fallback = DictSubclass(a=4, b=5, c=6)
    monkeypatch.setattr(tabular_io, "compile_selector", compile_tracked)

    matrix = fpstreams.rows([exact, fallback]).to_numpy("a", "b", "c")

    assert matrix.tolist() == [[1, 2, 3], [4, 5, 6]]
    assert selected_rows == [fallback, fallback, fallback]

    class CollidingKey:
        def __hash__(self) -> int:
            return hash("b")

        def __eq__(self, _other: object) -> bool:
            raise TypeError("broken key equality")

    with pytest.raises(fpstreams.SelectionError, match="'b'") as captured:
        fpstreams.rows([{"a": 1, CollidingKey(): 2}]).to_numpy("a", "b")
    assert isinstance(captured.value.__cause__, TypeError)


def test_rows_to_numpy_direct_fields_preserve_nested_dtype_shapes() -> None:
    """Direct selectors retain the list-of-rows input contract passed to NumPy."""
    np = pytest.importorskip("numpy")
    records = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    structured_dtype = np.dtype([("left", "i4"), ("right", "i4")])
    subarray_dtype = np.dtype(("i4", (2,)))

    direct_structured = fpstreams.rows(records).to_numpy("a", "b", dtype=structured_dtype)
    fallback_structured = fpstreams.rows(records).to_numpy(
        lambda row: row["a"], lambda row: row["b"], dtype=structured_dtype
    )
    direct_subarray = fpstreams.rows(records).to_numpy("a", dtype=subarray_dtype)
    fallback_subarray = fpstreams.rows(records).to_numpy(lambda row: row["a"], dtype=subarray_dtype)

    assert direct_structured.shape == fallback_structured.shape == (2, 2)
    assert direct_structured.dtype == fallback_structured.dtype
    assert direct_structured.tolist() == fallback_structured.tolist()
    assert direct_subarray.shape == fallback_subarray.shape == (2, 1, 2)
    assert direct_subarray.dtype == fallback_subarray.dtype
    assert direct_subarray.tolist() == fallback_subarray.tolist()


def test_rows_to_numpy_obeys_numpy_two_copy_modes() -> None:
    np = pytest.importorskip("numpy")

    source = np.asarray([[1, 2], [3, 4]], dtype=np.int64)
    table = fpstreams.rows.from_numpy(source, columns=["a", "b"])

    reused = table.to_numpy(copy=None)
    required_view = table.to_numpy(copy=False)
    copied = table.to_numpy(copy=True)

    assert np.shares_memory(reused, source)
    assert np.shares_memory(required_view, source)
    assert not np.shares_memory(copied, source)
    with pytest.raises(ValueError, match="copy"):
        table.to_numpy(dtype=np.float64, copy=False)
    with pytest.raises(ValueError, match="copy"):
        fpstreams.rows([{"a": 1}]).to_numpy(copy=False)


def test_polars_lazyframe_stays_lazy_batched_and_reiterable() -> None:
    executions: list[int] = []

    def observe(batch: pl.DataFrame) -> pl.DataFrame:
        executions.append(batch.height)
        return batch

    lazy_frame = (
        pl.LazyFrame({"id": range(5)})
        .with_columns((pl.col("id") * 10).alias("value"))
        .map_batches(observe)
    )
    source = fpstreams.rows.from_polars(lazy_frame, batch_size=2)

    assert executions == []
    assert source.take(1).to_list() == [{"id": 0, "value": 0}]
    assert executions
    executions.clear()
    assert source.to_list() == [
        {"id": 0, "value": 0},
        {"id": 1, "value": 10},
        {"id": 2, "value": 20},
        {"id": 3, "value": 30},
        {"id": 4, "value": 40},
    ]
    assert executions


def test_rows_outputs_polars_batches_and_eager_dataframe() -> None:
    values: list[dict[str, Any]] = [
        {"id": 1},
        {"id": 2, "label": "b"},
        {"id": 3, "label": "c"},
        {"id": 4},
        {"id": 5, "label": "e"},
    ]
    source = fpstreams.rows(values)

    batches = source.polars_batches(batch_size=2).to_list()
    assert [batch.height for batch in batches] == [2, 2, 1]
    assert all(batch.columns == ["id", "label"] for batch in batches)
    expected = [dict(row, label=row.get("label")) for row in values]
    assert [row for batch in batches for row in batch.to_dicts()] == expected

    restored = source.to_polars(batch_size=2)
    assert isinstance(restored, pl.DataFrame)
    assert restored.to_dicts() == expected
    pandas_records = source.to_pandas(batch_size=2).to_dict("records")
    assert [record["id"] for record in pandas_records] == [1, 2, 3, 4, 5]
    assert pd.isna(pandas_records[0]["label"])
    assert pandas_records[1]["label"] == "b"


class _ReadCursor:
    def __init__(self, columns: Sequence[str], values: Sequence[tuple[Any, ...]]) -> None:
        self.description = tuple((name,) for name in columns)
        self._values = list(values)
        self.executed: tuple[str, object] | None = None
        self.fetch_sizes: list[int] = []
        self.closed = False

    def execute(self, query: str, parameters: object = None) -> None:
        self.executed = (query, parameters)

    def fetchmany(self, size: int) -> list[tuple[Any, ...]]:
        self.fetch_sizes.append(size)
        batch, self._values = self._values[:size], self._values[size:]
        return batch

    def close(self) -> None:
        self.closed = True


class _ReadConnection:
    def __init__(self, cursor: _ReadCursor) -> None:
        self._cursor = cursor
        self.closed = False

    def cursor(self) -> _ReadCursor:
        return self._cursor

    def close(self) -> None:
        self.closed = True


class _WriteCursor:
    def __init__(self, *, fail_on: int | None = None) -> None:
        self.batches: list[list[object]] = []
        self.fail_on = fail_on
        self.closed = False

    def executemany(self, _statement: str, values: Sequence[object]) -> None:
        self.batches.append(list(values))
        if len(self.batches) == self.fail_on:
            raise RuntimeError("database write failed")

    def close(self) -> None:
        self.closed = True


class _WriteConnection:
    def __init__(self, cursor: _WriteCursor) -> None:
        self._cursor = cursor
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self) -> _WriteCursor:
        return self._cursor

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


def test_db_source_is_lazy_reusable_batched_and_closes_on_take() -> None:
    connections: list[_ReadConnection] = []

    def connect() -> _ReadConnection:
        connection = _ReadConnection(_ReadCursor(("id", "name"), ((1, "a"), (2, "b"))))
        connections.append(connection)
        return connection

    source = fpstreams.rows.from_db(
        connect,
        "select id, name from events where id >= :minimum",
        {"minimum": 1},
        batch_size=2,
    )
    assert connections == []
    assert source.take(1).to_list() == [{"id": 1, "name": "a"}]
    assert connections[0]._cursor.executed == (
        "select id, name from events where id >= :minimum",
        {"minimum": 1},
    )
    assert connections[0]._cursor.fetch_sizes == [2]
    assert connections[0]._cursor.closed and connections[0].closed

    assert source.to_list() == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    assert len(connections) == 2
    assert connections[1]._cursor.closed and connections[1].closed


def test_db_source_rejects_ambiguous_columns_and_still_closes() -> None:
    connection = _ReadConnection(_ReadCursor(("value", "value"), ((1, 2),)))
    with pytest.raises(fpstreams.DuplicateKeyError, match="duplicate column"):
        fpstreams.rows.from_db(lambda: connection, "select 1, 2").to_list()
    assert connection._cursor.closed and connection.closed


def test_db_source_cleanup_failure_ignores_an_ambient_outer_exception() -> None:
    class FailingCursor(_ReadCursor):
        def __init__(self) -> None:
            super().__init__(("value",), ((1,),))
            self.close_calls = 0

        def close(self) -> None:
            self.close_calls += 1
            raise OSError("cursor close failed")

    cursor = FailingCursor()
    connection = _ReadConnection(cursor)
    try:
        raise ValueError("outer")
    except ValueError as outer:
        with pytest.raises(OSError, match="cursor close failed"):
            fpstreams.rows.from_db(lambda: connection, "select 1").take(1).to_list()
        assert getattr(outer, "__notes__", ()) == ()

    assert cursor.close_calls == 1
    assert connection.closed


def test_db_sink_batches_commits_rolls_back_and_closes_upstream() -> None:
    cursor = _WriteCursor()
    connection = _WriteConnection(cursor)
    assert (
        fpstreams.rows([{"id": 1}, {"id": 2}, {"id": 3}]).to_db(
            lambda: connection,
            "insert into events values (?)",
            parameters=lambda row: (row["id"],),
            batch_size=2,
        )
        == 3
    )
    assert cursor.batches == [[(1,), (2,)], [(3,)]]
    assert connection.commits == 1 and connection.rollbacks == 0
    assert cursor.closed and connection.closed

    events: list[str] = []

    def records() -> Iterator[dict[str, int]]:
        events.append("open")
        try:
            for value in range(5):
                yield {"id": value}
        finally:
            events.append("close")

    failed_cursor = _WriteCursor(fail_on=2)
    failed = _WriteConnection(failed_cursor)
    with pytest.raises(RuntimeError, match="write failed"):
        fpstreams.rows(records()).to_db(
            lambda: failed,
            "insert into events values (?)",
            parameters=lambda row: (row["id"],),
            batch_size=2,
        )
    assert failed.commits == 0 and failed.rollbacks == 1
    assert failed_cursor.closed and failed.closed
    assert events == ["open", "close"]


def test_rows_to_db_closes_database_handles_when_source_close_raises_base_exception() -> None:
    class Source(Iterator[dict[str, int]]):
        def __init__(self) -> None:
            self.emitted = False
            self.close_calls = 0

        def __next__(self) -> dict[str, int]:
            if self.emitted:
                raise StopIteration
            self.emitted = True
            return {"id": 1}

        def close(self) -> None:
            self.close_calls += 1
            raise KeyboardInterrupt("source close interrupted")

    class DirectRows(fpstreams.Rows[dict[str, int]]):
        def __init__(self, source: Source) -> None:
            self.source = source

        def __iter__(self) -> Iterator[dict[str, int]]:
            return self.source

    source = Source()
    cursor = _WriteCursor()
    connection = _WriteConnection(cursor)

    try:
        raise ValueError("outer")
    except ValueError as outer:
        with pytest.raises(KeyboardInterrupt, match="source close interrupted"):
            DirectRows(source).to_db(
                lambda: connection,
                "insert into events values (?)",
                parameters=lambda row: (row["id"],),
                batch_size=1,
            )
        assert getattr(outer, "__notes__", ()) == ()

    assert source.close_calls == 1
    assert cursor.closed and connection.closed


def test_sqlite_table_sink_and_query_source_form_a_concise_round_trip(tmp_path: Path) -> None:
    database = tmp_path / "events.db"
    values = [
        {"id": 1, "select": "a", "amount": 2.0},
        {"id": 2, "select": "b", "amount": 3.5},
        {"id": 3, "select": "a", "amount": 4.0},
    ]
    assert fpstreams.rows(values).to_sqlite(
        database, 'event "log"', if_exists="replace", batch_size=1
    ) == len(values)

    selected = fpstreams.rows.from_sqlite(
        database,
        'select id, "select", amount from "event ""log""" where amount >= ? order by id',
        (3.0,),
        batch_size=1,
    )
    expected = [values[1], values[2]]
    assert selected.to_list() == expected
    assert selected.to_list() == expected
    assert selected.where(select="a").select("id", "amount").to_list() == [{"id": 3, "amount": 4.0}]


def test_sqlite_schema_projection_conflicts_and_fail_mode_are_explicit(tmp_path: Path) -> None:
    database = tmp_path / "schema.db"
    assert (
        fpstreams.rows([]).to_sqlite(
            database,
            "events",
            if_exists="replace",
            schema={"id": "INTEGER", "value": "TEXT"},
        )
        == 0
    )
    assert (
        fpstreams.rows([{"id": 1, "value": "a", "ignored": True}]).to_sqlite(
            database,
            "events",
            columns=("id", "value"),
        )
        == 1
    )
    assert fpstreams.rows.from_sqlite(database, "select * from events").to_list() == [
        {"id": 1, "value": "a"}
    ]

    opened = False

    def unused() -> Iterator[dict[str, int]]:
        nonlocal opened
        opened = True
        yield {"id": 2}

    with pytest.raises(ValueError, match="already exists"):
        fpstreams.rows(unused()).to_sqlite(database, "events", if_exists="fail")
    assert not opened

    # sqlite3.Connection commits on context exit but does not close itself. Pair
    # transaction ownership with ``closing`` so newer CPython versions see no leak.
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute("create table unique_events (id integer primary key, value text)")
    assert (
        fpstreams.rows([{"id": 1, "value": "a"}, {"id": 1, "value": "b"}]).to_sqlite(
            database,
            "unique_events",
            conflict="ignore",
            batch_size=1,
        )
        == 2
    )
    assert fpstreams.rows.from_sqlite(database, "select * from unique_events").to_list() == [
        {"id": 1, "value": "a"}
    ]


def test_sqlite_sink_rolls_back_batches_and_schema_changes_on_error(tmp_path: Path) -> None:
    database = tmp_path / "atomic.db"
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute("create table events (id integer, value text)")
        connection.execute("insert into events values (10, 'kept')")

    with pytest.raises(sqlite3.ProgrammingError):
        fpstreams.rows([{"id": 1, "value": "ok"}, {"id": 2, "value": object()}]).to_sqlite(
            database, "events", batch_size=1
        )
    assert fpstreams.rows.from_sqlite(database, "select * from events").to_list() == [
        {"id": 10, "value": "kept"}
    ]

    with pytest.raises(ValueError, match="introduced columns"):
        fpstreams.rows([{"a": 1}, {"a": 2, "b": 3}]).to_sqlite(
            database, "drift", if_exists="replace", batch_size=1
        )
    with closing(sqlite3.connect(database)) as connection, connection:
        assert connection.execute(
            "select count(*) from sqlite_master where type = 'table' and name = 'drift'"
        ).fetchone() == (0,)


def test_sqlite_sink_preserves_primary_error_and_attempts_every_cleanup(  # noqa: C901
    tmp_path: Path, monkeypatch
) -> None:
    primary = ValueError("row pull failed")
    events: list[str] = []

    class Source(Iterator[dict[str, int]]):
        def __init__(self) -> None:
            self.emitted = False
            self.close_calls = 0

        def __next__(self) -> dict[str, int]:
            if self.emitted:
                raise primary
            self.emitted = True
            return {"id": 1}

        def close(self) -> None:
            self.close_calls += 1
            events.append("source.close")
            raise OSError("source close failed")

    class Cursor:
        def __init__(self) -> None:
            self.close_calls = 0

        def execute(self, _statement: str, _parameters: object = None) -> None:
            return None

        def fetchone(self) -> None:
            return None

        def executemany(self, _statement: str, _values: object) -> None:
            return None

        def close(self) -> None:
            self.close_calls += 1
            events.append("cursor.close")
            raise KeyError("cursor close failed")

    class Connection:
        def __init__(self, cursor: Cursor) -> None:
            self._cursor = cursor
            self.rollback_calls = 0
            self.close_calls = 0

        def cursor(self) -> Cursor:
            return self._cursor

        def commit(self) -> None:
            return None

        def rollback(self) -> None:
            self.rollback_calls += 1
            events.append("rollback")
            raise RuntimeError("rollback failed")

        def close(self) -> None:
            self.close_calls += 1
            events.append("connection.close")
            raise LookupError("connection close failed")

    from fpstreams.tabular import sqlite_sink

    source = Source()
    cursor = Cursor()
    connection = Connection(cursor)
    monkeypatch.setattr(sqlite_sink.sqlite3, "connect", lambda *_args, **_kwargs: connection)

    class DirectRows(fpstreams.Rows[dict[str, int]]):
        def __init__(self) -> None:
            pass

        def __iter__(self) -> Iterator[dict[str, int]]:
            return source

    with pytest.raises(ValueError) as captured:
        DirectRows().to_sqlite(tmp_path / "unused.db", "events", batch_size=1)

    assert captured.value is primary
    assert captured.value.__notes__ == [
        "cleanup failed: RuntimeError: rollback failed",
        "cleanup failed: OSError: source close failed",
        "cleanup failed: KeyError: 'cursor close failed'",
        "cleanup failed: LookupError: connection close failed",
    ]
    assert events == ["rollback", "source.close", "cursor.close", "connection.close"]
    assert source.close_calls == cursor.close_calls == connection.close_calls == 1
    assert connection.rollback_calls == 1


def test_sql_interfaces_reject_ambiguous_or_unbounded_configuration(tmp_path: Path) -> None:
    database = tmp_path / "invalid.db"
    with pytest.raises(ValueError, match="batch_size"):
        fpstreams.rows.from_sqlite(database, "select 1", batch_size=0)
    with pytest.raises(ValueError, match="empty rows"):
        fpstreams.rows([]).to_sqlite(database, "empty")
    with pytest.raises(ValueError, match="column type"):
        fpstreams.rows([]).to_sqlite(database, "bad", schema={"id": "DROP TABLE x"})
    with pytest.raises(TypeError, match="not a string"):
        fpstreams.rows([]).to_sqlite(database, "bad", columns="id")

    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute("create table source (value integer)")
        connection.execute("insert into source values (1)")
    with pytest.raises(fpstreams.DuplicateKeyError, match="duplicate column"):
        fpstreams.rows.from_sqlite(
            database, "select value as duplicate, value as duplicate from source"
        ).to_list()


@pytest.mark.parametrize("as_record_batch", [False, True])
@pytest.mark.parametrize(
    ("how", "expected"),
    [
        (
            "inner",
            [
                {"left_id": 2, "left_value": 20, "right_id": 2, "right_value": 200},
                {"left_id": 1, "left_value": 10, "right_id": 1, "right_value": 100},
                {"left_id": 4, "left_value": 11, "right_id": 4, "right_value": 400},
            ],
        ),
        (
            "left",
            [
                {"left_id": 2, "left_value": 20, "right_id": 2, "right_value": 200},
                {"left_id": 1, "left_value": 10, "right_id": 1, "right_value": 100},
                {"left_id": 3, "left_value": 30, "right_id": None, "right_value": None},
                {"left_id": 4, "left_value": 11, "right_id": 4, "right_value": 400},
            ],
        ),
    ],
)
def test_retained_arrow_unique_join_keeps_left_order_without_boxing_inputs(
    monkeypatch: pytest.MonkeyPatch,
    as_record_batch: bool,
    how: str,
    expected: list[dict[str, object]],
) -> None:
    """A direct m:1 join keeps distinct key names and canonical field order."""
    from fpstreams.tabular import arrow as arrow_adapter

    left_table = pa.table({"left_id": [2, 1, 3, 4], "left_value": [20, 10, 30, 11]})
    unused = list(range(1_000, 1_125))
    right_table = pa.table(
        {
            "right_id": [1, 2, 4, *unused],
            "right_value": [100, 200, 400, *unused],
        }
    )
    left = left_table.to_batches()[0] if as_record_batch else left_table
    right = right_table.to_batches()[0] if as_record_batch else right_table

    def forbidden_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("guarded retained Arrow join must not box either input")

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", forbidden_rows)

    result = (
        fpstreams.rows.from_arrow(left)
        .join(
            fpstreams.rows.from_arrow(right),
            left_on="left_id",
            right_on="right_id",
            how=how,
            validate="m:1",
        )
        .to_list()
    )

    assert result == expected


def test_retained_arrow_unique_join_mints_suffix_keys_per_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Generated right-column keys retain canonical per-output string identity."""
    from fpstreams.tabular import arrow as arrow_adapter

    left = pa.table({"id": [1, 1], "value": [10, 11]})
    unused = list(range(1_000, 1_125))
    right = pa.table({"id": [1, *unused], "value": [20, *unused]})
    canonical = (
        fpstreams.rows.from_arrow(left)
        .with_engine("python")
        .join(
            fpstreams.rows.from_arrow(right).with_engine("python"),
            on="id",
            validate="m:1",
        )
        .to_list()
    )

    def forbidden_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("safe suffix joins must stay columnar")

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", forbidden_rows)
    automatic = (
        fpstreams.rows.from_arrow(left)
        .join(fpstreams.rows.from_arrow(right), on="id", validate="m:1")
        .to_list()
    )

    assert (
        automatic
        == canonical
        == [
            {"id": 1, "value": 10, "value_right": 20},
            {"id": 1, "value": 11, "value_right": 20},
        ]
    )
    automatic_keys = [next(key for key in row if key == "value_right") for row in automatic]
    canonical_keys = [next(key for key in row if key == "value_right") for row in canonical]
    assert automatic_keys[0] is not automatic_keys[1]
    assert canonical_keys[0] is not canonical_keys[1]


def test_retained_arrow_unique_join_preserves_source_batch_key_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Left keys keep source batch boundaries while right target keys stay global."""
    from fpstreams.tabular import arrow as arrow_adapter

    left = pa.table({"left_id": [1, 2, 99, 3], "left_value": [10, 20, 990, 30]})
    unused = list(range(1_000, 1_125))
    right = pa.table(
        {
            "right_id": [1, 2, 3, *unused],
            "right_value": [100, 200, 300, *unused],
        }
    )

    def forbidden_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("guarded retained Arrow join must not box either input")

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", forbidden_rows)
    result = (
        fpstreams.rows.from_arrow(left, batch_size=2)
        .join(
            fpstreams.rows.from_arrow(right, batch_size=2),
            left_on="left_id",
            right_on="right_id",
            how="inner",
            validate="m:1",
        )
        .to_list()
    )

    left_keys = [next(key for key in row if key == "left_value") for row in result]
    right_keys = [next(key for key in row if key == "right_value") for row in result]
    assert left_keys[0] is left_keys[1]
    assert left_keys[0] is not left_keys[2]
    assert right_keys[0] is right_keys[1] is right_keys[2]


def test_retained_arrow_unique_join_repeated_match_reuses_right_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repeated matches retain canonical right-row payload object identity."""
    from fpstreams.tabular import arrow as arrow_adapter

    payload = "right-payload-that-is-long-enough-to-avoid-interning"
    left = pa.table({"left_id": [1, 1], "left_value": [10, 11]})
    unused = list(range(1_000, 1_125))
    right = pa.table(
        {
            "right_id": [1, *unused],
            "right_value": [payload, *(f"unused-{value}" for value in unused)],
        }
    )

    def forbidden_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("repeated retained Arrow matches must stay columnar")

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", forbidden_rows)

    result = (
        fpstreams.rows.from_arrow(left)
        .join(
            fpstreams.rows.from_arrow(right),
            left_on="left_id",
            right_on="right_id",
            validate="m:1",
        )
        .to_list()
    )

    assert result[0]["right_value"] is result[1]["right_value"]


def test_retained_arrow_unique_join_planner_marks_only_the_exact_top_level_shape() -> None:
    """The columnar marker excludes modes whose protocol timing is still observable."""
    from fpstreams.physical.relational import ArrowUniqueJoinSpec, JoinPhysicalNode
    from fpstreams.planning.compiler import compile_query

    left = fpstreams.rows.from_arrow(pa.table({"id": range(70), "left": range(70)}))
    right = fpstreams.rows.from_arrow(pa.table({"id": range(70), "right": range(70)}))
    supported = left.join(right, on="id", how="left", validate="m:1")._flow
    physical = compile_query(supported._query("list"))
    assert isinstance(physical.root, JoinPhysicalNode)
    assert physical.root.arrow_unique == ArrowUniqueJoinSpec("id", "id")

    class StatefulMode(str):
        comparisons = 0
        __hash__ = str.__hash__

        def __eq__(self, other: object) -> bool:
            type(self).comparisons += 1
            return super().__eq__(other)

    unsupported = (
        left.with_engine("python").join(right, on="id", validate="m:1")._flow,
        left.join(right, on="id", validate="m:m")._flow,
        left.join(right, on="id", validate="m:1", partitions=2)._flow,
        left.where(lambda _row: True).join(right, on="id", validate="m:1")._flow,
        left.join(
            right,
            on="id",
            how=StatefulMode("inner"),
            validate="m:1",
        )._flow,
        left.join(
            right,
            on="id",
            validate=StatefulMode("m:1"),
        )._flow,
    )
    for query in unsupported:
        plan = compile_query(query._query("list"))
        assert isinstance(plan.root, JoinPhysicalNode)
        assert plan.root.arrow_unique is None
    non_list = compile_query(supported._query("tuple"))
    assert isinstance(non_list.root, JoinPhysicalNode)
    assert non_list.root.arrow_unique is None


def test_retained_arrow_unique_join_small_tables_use_the_python_crossover(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Arrow kernel setup stays off the measured losing side of the crossover."""
    from fpstreams.tabular import arrow as arrow_adapter

    converted = 0
    original = arrow_adapter.batch_to_rows

    def tracked(batch: object) -> list[dict[str, object]]:
        nonlocal converted
        converted += 1
        return original(batch)

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", tracked)
    result = (
        fpstreams.rows.from_arrow(pa.table({"id": [1], "left": [10]}))
        .join(
            fpstreams.rows.from_arrow(pa.table({"id": [1], "right": [20]})),
            on="id",
            validate="m:1",
        )
        .to_list()
    )

    assert result == [{"id": 1, "left": 10, "right": 20}]
    assert converted == 2


@pytest.mark.parametrize("case", ["float_nan", "null", "cross_type", "dictionary", "nested"])
def test_retained_arrow_unique_join_falls_back_for_unproven_arrow_semantics(
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    """Unsupported equality and mutable payload families retain canonical row behavior."""
    from fpstreams.tabular import arrow as arrow_adapter

    filler = list(range(1_000, 1_130))
    if case == "float_nan":
        nan = float("nan")
        left = pa.table({"left_id": [nan, 1.0], "left": [0, 1]})
        right = pa.table(
            {"right_id": [nan, 1.0, *(float(value) for value in filler)], "right": range(132)}
        )
    elif case == "null":
        left = pa.table({"left_id": [None, 1], "left": [0, 1]})
        right = pa.table({"right_id": [None, 1, *filler], "right": range(132)})
    elif case == "cross_type":
        left = pa.table({"left_id": ["1", "2"], "left": [0, 1]})
        right = pa.table(
            {
                "right_id": pa.array([b"1", b"2", *(str(value).encode() for value in filler)]),
                "right": range(132),
            }
        )
    elif case == "dictionary":
        left = pa.table(
            {
                "left_id": pa.array(["a", "b"]).dictionary_encode(),
                "left": [0, 1],
            }
        )
        right_keys = pa.array(["a", "b", *(str(value) for value in filler)]).dictionary_encode()
        right = pa.table({"right_id": right_keys, "right": range(132)})
    else:
        left = pa.table({"left_id": [1, 2], "left": [[1], [2]]})
        right = pa.table({"right_id": [1, 2, *filler], "right": range(132)})

    canonical = (
        fpstreams.rows.from_arrow(left)
        .with_engine("python")
        .join(
            fpstreams.rows.from_arrow(right).with_engine("python"),
            left_on="left_id",
            right_on="right_id",
            validate="m:1",
        )
        .to_list()
    )
    converted = 0
    original = arrow_adapter.batch_to_rows

    def tracked(batch: object) -> list[dict[str, object]]:
        nonlocal converted
        converted += 1
        return original(batch)

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", tracked)
    automatic = (
        fpstreams.rows.from_arrow(left)
        .join(
            fpstreams.rows.from_arrow(right),
            left_on="left_id",
            right_on="right_id",
            validate="m:1",
        )
        .to_list()
    )

    assert automatic == canonical
    assert converted >= 2


def test_retained_arrow_unique_join_keeps_empty_and_duplicate_right_semantics() -> None:
    """Schema-only right fields and duplicate validation stay on canonical discovery rules."""
    left = pa.table({"id": [1], "left": [10]})
    empty_right = pa.table(
        {
            "id": pa.array([], type=pa.int64()),
            "right": pa.array([], type=pa.int64()),
        }
    )
    assert fpstreams.rows.from_arrow(left).join(
        fpstreams.rows.from_arrow(empty_right),
        on="id",
        how="left",
        validate="m:1",
    ).to_list() == [{"id": 1, "left": 10}]

    empty_left = pa.table(
        {"id": pa.array([], type=pa.int64()), "left": pa.array([], type=pa.int64())}
    )
    duplicate_right = pa.table({"id": [1, 1, *range(1_000, 1_130)], "right": range(132)})
    with pytest.raises(ValueError) as canonical:
        (
            fpstreams.rows.from_arrow(empty_left)
            .with_engine("python")
            .join(
                fpstreams.rows.from_arrow(duplicate_right).with_engine("python"),
                on="id",
                validate="m:1",
            )
            .to_list()
        )
    with pytest.raises(ValueError) as automatic:
        (
            fpstreams.rows.from_arrow(empty_left)
            .join(
                fpstreams.rows.from_arrow(duplicate_right),
                on="id",
                validate="m:1",
            )
            .to_list()
        )
    assert str(automatic.value) == str(canonical.value)


def test_retained_arrow_unique_join_kernel_decline_and_memory_error_boundaries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Expected Arrow errors replay canonically; allocation failure propagates unchanged."""
    from fpstreams.execution import relational
    from fpstreams.tabular import arrow as arrow_adapter

    left = pa.table({"left_id": list(range(130)), "left": list(range(130))})
    right = pa.table({"right_id": list(range(130)), "right": list(range(130))})
    query = fpstreams.rows.from_arrow(left).join(
        fpstreams.rows.from_arrow(right),
        left_on="left_id",
        right_on="right_id",
        validate="m:1",
    )
    original_import = relational.import_module
    real_compute = original_import("pyarrow.compute")

    class ComputeProxy:
        calls = 0

        def __getattr__(self, name: str) -> object:
            if name != "index_in":
                return getattr(real_compute, name)

            def decline(*_args: object, **_kwargs: object) -> object:
                type(self).calls += 1
                raise ValueError("declined index lookup")

            return decline

    proxy = ComputeProxy()
    monkeypatch.setattr(
        relational,
        "import_module",
        lambda name: proxy if name == "pyarrow.compute" else original_import(name),
    )
    assert query.to_list() == [
        {"left_id": value, "left": value, "right_id": value, "right": value} for value in range(130)
    ]
    assert ComputeProxy.calls == 1

    failure = MemoryError("Arrow position allocation failed")

    class MemoryProxy:
        def __getattr__(self, name: str) -> object:
            if name != "index_in":
                return getattr(real_compute, name)

            def fail(*_args: object, **_kwargs: object) -> object:
                raise failure

            return fail

    converted = 0
    original_rows = arrow_adapter.batch_to_rows

    def tracked(batch: object) -> list[dict[str, object]]:
        nonlocal converted
        converted += 1
        return original_rows(batch)

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", tracked)
    memory_proxy = MemoryProxy()
    monkeypatch.setattr(
        relational,
        "import_module",
        lambda name: memory_proxy if name == "pyarrow.compute" else original_import(name),
    )
    with pytest.raises(MemoryError) as captured:
        query.to_list()
    assert captured.value is failure
    assert converted == 0

    monkeypatch.setattr(relational, "import_module", original_import)
    assert query.to_list()[0] == {"left_id": 0, "left": 0, "right_id": 0, "right": 0}


def test_retained_arrow_unique_join_invalid_utf8_keeps_canonical_error_priority() -> None:
    """Full validation may decline, but it never replaces later row-conversion ordering."""
    offsets = pa.array([0, 1, 2, 3], type=pa.int32()).buffers()[1]
    malformed = pa.Array.from_buffers(
        pa.string(),
        3,
        [None, offsets, pa.py_buffer(b"ab\xff")],
    )
    left = pa.table({"id": list(range(130)), "left": list(range(130))})
    right = pa.table({"id": [1, 1, 2], "right": malformed})

    def execute(engine: str) -> list[dict[str, object]]:
        left_rows = fpstreams.rows.from_arrow(left, batch_size=1)
        right_rows = fpstreams.rows.from_arrow(right, batch_size=1)
        if engine == "python":
            left_rows = left_rows.with_engine("python")
            right_rows = right_rows.with_engine("python")
        return left_rows.join(right_rows, on="id", validate="m:1").to_list()

    with pytest.raises(ValueError) as canonical:
        execute("python")
    with pytest.raises(ValueError) as automatic:
        execute("auto")
    assert str(automatic.value) == str(canonical.value)
    assert "found duplicate 1" in str(automatic.value)


def test_rows_run_with_report_keeps_the_rows_terminal_surface() -> None:
    source = fpstreams.rows([{"value": 1}, {"value": 2}])

    execution = source.select("value").run_with_report("to_list")

    assert execution.value == [{"value": 1}, {"value": 2}]
    assert execution.report.terminal == "to_list"
    assert execution.report.strategy == "planned:python"


def test_numpy_group_planner_marks_only_direct_integer_columnar_shapes() -> None:
    """Planning should expose the bounded NumPy candidate without widening its contract."""
    np = pytest.importorskip("numpy")
    from fpstreams.physical.relational import (
        GroupAggregatePhysicalNode,
        NumpyGroupAggregateSpec,
    )
    from fpstreams.planning.compiler import compile_query

    source = fpstreams.rows.from_numpy(
        np.asarray([[1, 2], [1, 3]], dtype=np.int64),
        columns=("key", "value"),
    )
    grouped = source.group_by("key").aggregate(
        rows=fpstreams.agg.count(),
        total=fpstreams.agg.sum("value"),
        low=fpstreams.agg.min("value"),
        high=fpstreams.agg.max("value"),
    )
    physical = compile_query(grouped._flow._query("list"))
    assert isinstance(physical.root, GroupAggregatePhysicalNode)
    assert isinstance(physical.root.numpy_group, NumpyGroupAggregateSpec)
    assert tuple(lane.kind for lane in physical.root.numpy_group.lanes) == (
        "count",
        "sum",
        "min",
        "max",
    )

    unsupported = (
        source.with_engine("python").group_by("key").aggregate(total=fpstreams.agg.sum("value")),
        source.group_by(lambda row: row["key"]).aggregate(total=fpstreams.agg.sum("value")),
        fpstreams.rows.from_numpy(
            np.asarray([[1.0, 2.0]], dtype=np.float64),
            columns=("key", "value"),
        )
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value")),
    )
    for query in unsupported:
        fallback = compile_query(query._flow._query("list"))
        assert isinstance(fallback.root, GroupAggregatePhysicalNode)
        assert fallback.root.numpy_group is None


def test_numpy_group_aggregate_is_bounded_columnar_single_pass_and_reported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One chunk factorization should feed every lane and preserve first-seen order."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution import numpy_group, relational

    matrix = np.asarray(
        [[2, 4], [1, -7], [2, 3], [3, 9], [1, 5], [2, -8], [3, 1]],
        dtype=np.int64,
    )
    source = fpstreams.rows.from_numpy(matrix, columns=("key", "value"))
    query = source.group_by("key").aggregate(
        rows=fpstreams.agg.count(),
        total=fpstreams.agg.sum("value"),
        total_again=fpstreams.agg.sum("value"),
        low=fpstreams.agg.min("value"),
        high=fpstreams.agg.max("value"),
    )
    original_factorization = numpy_group._factorize_numpy_group_keys
    chunk_sizes: list[int] = []

    def bounded_factorization(module: object, values: object) -> object:
        chunk_sizes.append(len(values))  # type: ignore[arg-type]
        assert len(values) <= 3  # type: ignore[arg-type]
        return original_factorization(module, values)

    def forbidden_rows(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("supported NumPy grouping must not enter the row executor")

    monkeypatch.setattr(relational, "_NUMPY_GROUP_CHUNK_ROWS", 3)
    monkeypatch.setattr(relational, "_execute_python_group_values", forbidden_rows)
    monkeypatch.setattr(numpy_group, "_native_numpy_group_endpoints", lambda: None)
    monkeypatch.setattr(numpy_group, "_factorize_numpy_group_keys", bounded_factorization)
    execution = query.run_with_report("to_list")

    expected = [
        {"key": 2, "rows": 3, "total": -1, "total_again": -1, "low": -8, "high": 4},
        {"key": 1, "rows": 2, "total": -2, "total_again": -2, "low": -7, "high": 5},
        {"key": 3, "rows": 2, "total": 10, "total_again": 10, "low": 1, "high": 9},
    ]
    assert execution.value == expected
    assert chunk_sizes == [3, 3]
    assert execution.report.strategy == "numpy_direct"
    chunk_sizes.clear()
    assert list(query) == expected
    assert chunk_sizes == [3, 3]


def test_numpy_group_native_state_reduces_i64_chunks_without_numpy_factorization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One stable native scan should serve repeated lanes and multiple bounded chunks."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution import numpy_group, relational

    matrix = np.asarray(
        [
            [2, 4],
            [1, -7],
            [2, 3],
            [3, 9],
            [1, 5],
            [2, -8],
            [3, 1],
        ],
        dtype=np.int64,
    )
    query = (
        fpstreams.rows.from_numpy(matrix, columns=("key", "value"))
        .group_by("key")
        .aggregate(
            rows=fpstreams.agg.count(),
            total=fpstreams.agg.sum("value"),
            total_again=fpstreams.agg.sum("value"),
            low=fpstreams.agg.min("value"),
            high=fpstreams.agg.max("value"),
        )
    )

    def forbidden_factorization(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("exact i64 chunks should use the native grouped state")

    monkeypatch.setattr(relational, "_NUMPY_GROUP_CHUNK_ROWS", 3)
    monkeypatch.setattr(
        numpy_group,
        "_factorize_numpy_group_keys",
        forbidden_factorization,
    )

    execution = query.run_with_report("to_list")

    assert execution.value == [
        {"key": 2, "rows": 3, "total": -1, "total_again": -1, "low": -8, "high": 4},
        {"key": 1, "rows": 2, "total": -2, "total_again": -2, "low": -7, "high": 5},
        {"key": 3, "rows": 2, "total": 10, "total_again": 10, "low": 1, "high": 9},
    ]
    assert execution.report.strategy == "numpy_direct"


def test_numpy_group_native_state_reads_strided_source_columns_without_v1_copy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The GIL build should aggregate direct column views without a packed intermediate."""
    np = pytest.importorskip("numpy")
    from fpstreams import _native
    from fpstreams.execution import relational

    matrix = np.asarray(
        [
            [2, 40, 4],
            [1, 70, -7],
            [2, 30, 3],
            [3, 90, 9],
            [1, 50, 5],
            [2, 80, -8],
            [3, 10, 1],
        ],
        dtype=np.int64,
    )
    query = (
        fpstreams.rows.from_numpy(matrix, columns=("key", "payload", "value"))
        .group_by("key")
        .aggregate(
            rows=fpstreams.agg.count(),
            total=fpstreams.agg.sum("value"),
            low=fpstreams.agg.min("value"),
            high=fpstreams.agg.max("value"),
        )
    )
    real_partial = _native.numpy_group_strided_partial_v2
    observed_strides: list[tuple[tuple[int, ...], tuple[int, ...]]] = []

    def tracked_partial(keys: object, values: object, mask: int) -> object:
        observed_strides.append((keys.strides, values.strides))  # type: ignore[attr-defined]
        return real_partial(keys, values, mask)

    def forbidden_v1(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("the GIL build should not repack stable NumPy columns")

    monkeypatch.setattr(relational, "_NUMPY_GROUP_CHUNK_ROWS", 3)
    monkeypatch.setattr(_native, "numpy_group_partial_v1", forbidden_v1)
    monkeypatch.setattr(_native, "numpy_group_strided_partial_v2", tracked_partial)

    assert query.to_list() == [
        {"key": 2, "rows": 3, "total": -1, "low": -8, "high": 4},
        {"key": 1, "rows": 2, "total": -2, "low": -7, "high": 5},
        {"key": 3, "rows": 2, "total": 10, "low": 1, "high": 9},
    ]
    assert observed_strides == [((24,), (24,)), ((24,), (24,)), ((24,), (24,))]


def test_numpy_group_native_partial_commits_only_after_live_revalidation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A shortened retained matrix must discard and recompute its pending native chunk."""
    np = pytest.importorskip("numpy")
    from fpstreams import _native
    from fpstreams.execution import numpy_group, relational

    live = np.asarray([[2, 10], [1, 20], [3, 30]], dtype=np.int64)
    query = (
        fpstreams.rows.from_numpy(live, columns=("key", "value"))
        .group_by("key")
        .aggregate(
            rows=fpstreams.agg.count(),
            total=fpstreams.agg.sum("value"),
            low=fpstreams.agg.min("value"),
            high=fpstreams.agg.max("value"),
        )
    )
    real_partial = _native.numpy_group_strided_partial_v2
    real_commit = _native.numpy_group_commit_v1
    partial_sizes: list[int] = []
    commits = 0

    def shrinking_partial(keys: object, values: object, mask: int) -> object:
        partial = real_partial(keys, values, mask)
        partial_sizes.append(len(keys))  # type: ignore[arg-type]
        if len(partial_sizes) == 1:
            live.resize((2, 2), refcheck=False)
        return partial

    def tracked_commit(state: object, partial: object) -> None:
        nonlocal commits
        commits += 1
        real_commit(state, partial)

    def forbidden_factorization(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("a stable retry should remain in the native grouped state")

    monkeypatch.setattr(relational, "_NUMPY_GROUP_CHUNK_ROWS", 3)
    monkeypatch.setattr(_native, "numpy_group_strided_partial_v2", shrinking_partial)
    monkeypatch.setattr(_native, "numpy_group_commit_v1", tracked_commit)
    monkeypatch.setattr(
        numpy_group,
        "_factorize_numpy_group_keys",
        forbidden_factorization,
    )

    assert query.to_list() == [
        {"key": 2, "rows": 1, "total": 10, "low": 10, "high": 10},
        {"key": 1, "rows": 1, "total": 20, "low": 20, "high": 20},
    ]
    assert partial_sizes == [3, 2]
    assert commits == 1


def test_numpy_group_native_decline_discards_state_and_restarts_numpy_from_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A later ABI decline must not leak already committed native rows into fallback."""
    np = pytest.importorskip("numpy")
    from fpstreams import _native
    from fpstreams.execution import numpy_group, relational

    matrix = np.asarray(
        [[2, 10], [1, 20], [2, 30], [3, 40], [1, 50], [3, 60]],
        dtype=np.int64,
    )
    query = (
        fpstreams.rows.from_numpy(matrix, columns=("key", "value"))
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
    )
    real_partial = _native.numpy_group_strided_partial_v2
    original_factorization = numpy_group._factorize_numpy_group_keys
    partial_sizes: list[int] = []
    fallback_chunks: list[tuple[int, ...]] = []

    def declining_partial(keys: object, values: object, mask: int) -> object:
        partial_sizes.append(len(keys))  # type: ignore[arg-type]
        if len(partial_sizes) == 2:
            return None
        return real_partial(keys, values, mask)

    def tracked_factorization(module: object, keys: object) -> object:
        fallback_chunks.append(tuple(int(key) for key in keys))  # type: ignore[union-attr]
        return original_factorization(module, keys)

    def forbidden_finalize(_state: object) -> object:
        raise AssertionError("a declined native state must never be finalized")

    monkeypatch.setattr(relational, "_NUMPY_GROUP_CHUNK_ROWS", 3)
    monkeypatch.setattr(_native, "numpy_group_strided_partial_v2", declining_partial)
    monkeypatch.setattr(_native, "numpy_group_finalize_v1", forbidden_finalize)
    monkeypatch.setattr(
        numpy_group,
        "_factorize_numpy_group_keys",
        tracked_factorization,
    )

    assert query.to_list() == [
        {"key": 2, "total": 40},
        {"key": 1, "total": 70},
        {"key": 3, "total": 100},
    ]
    assert partial_sizes == [3, 3]
    assert fallback_chunks == [(2, 1, 2), (3, 1, 3)]


def test_numpy_group_reuses_only_a_closed_compact_domain_with_a_count_lane(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Holes, missing counts, and wide domains must retain full factorization."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution import numpy_group, relational

    original_factorization = numpy_group._factorize_numpy_group_keys

    def factorized_chunks(
        keys: list[int],
        *,
        include_count: bool,
        chunk_rows: int = 4,
    ) -> tuple[list[tuple[int, ...]], list[dict[str, object]]]:
        matrix = np.column_stack(
            (np.asarray(keys, dtype=np.int64), np.arange(len(keys), dtype=np.int64))
        )
        source = fpstreams.rows.from_numpy(matrix, columns=("key", "value"))
        if include_count:
            query = source.group_by("key").aggregate(
                rows=fpstreams.agg.count(),
                total=fpstreams.agg.sum("value"),
            )
        else:
            query = source.group_by("key").aggregate(total=fpstreams.agg.sum("value"))
        chunks: list[tuple[int, ...]] = []

        def tracked_factorization(module: object, values: object) -> object:
            chunks.append(tuple(int(value) for value in values))  # type: ignore[union-attr]
            return original_factorization(module, values)

        with monkeypatch.context() as scoped:
            scoped.setattr(relational, "_NUMPY_GROUP_CHUNK_ROWS", chunk_rows)
            scoped.setattr(numpy_group, "_native_numpy_group_endpoints", lambda: None)
            scoped.setattr(
                numpy_group,
                "_factorize_numpy_group_keys",
                tracked_factorization,
            )
            result = query.to_list()
        return chunks, result

    hole_chunks, hole_result = factorized_chunks(
        [0, 2, 0, 2, 0, 1, 2, 0, 2, 1, 0, 2],
        include_count=True,
    )
    assert hole_chunks == [(0, 2, 0, 2), (0, 1, 2, 0)]
    assert hole_result == [
        {"key": 0, "rows": 5, "total": 23},
        {"key": 2, "rows": 5, "total": 29},
        {"key": 1, "rows": 2, "total": 14},
    ]

    no_count_chunks, no_count_result = factorized_chunks(
        [0, 1, 0, 1, 1, 0, 1, 0],
        include_count=False,
    )
    assert no_count_chunks == [(0, 1, 0, 1), (1, 0, 1, 0)]
    assert no_count_result == [
        {"key": 0, "total": 14},
        {"key": 1, "total": 14},
    ]

    high_cardinality_chunks, high_cardinality_result = factorized_chunks(
        [0, 100, 200, 300, 300, 200, 100, 0],
        include_count=True,
    )
    assert high_cardinality_chunks == [
        (0, 100, 200, 300),
        (300, 200, 100, 0),
    ]
    assert high_cardinality_result == [
        {"key": 0, "rows": 2, "total": 7},
        {"key": 100, "rows": 2, "total": 7},
        {"key": 200, "rows": 2, "total": 7},
        {"key": 300, "rows": 2, "total": 7},
    ]

    tail_chunks, tail_result = factorized_chunks(
        [index % 65 for index in range(1_040)] + [0],
        include_count=True,
        chunk_rows=1_040,
    )
    assert [len(chunk) for chunk in tail_chunks] == [1_040, 1]
    assert tail_chunks[-1] == (0,)
    assert tail_result[0] == {"key": 0, "rows": 17, "total": 8_840}


def test_numpy_group_closed_domain_reuse_covers_fixed_width_integer_boundaries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Known dense codes remain exact at signed, unsigned, boolean, and endian edges."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution import numpy_group, relational

    opposite = ">" if sys.byteorder == "little" else "<"
    cases = (
        (np.dtype(np.int64), -(2**63), -(2**63) + 1),
        (np.dtype(np.uint64), 2**64 - 2, 2**64 - 1),
        (np.dtype(np.bool_), False, True),
        (np.dtype(f"{opposite}i8"), -(2**63), -(2**63) + 1),
        (np.dtype(f"{opposite}u8"), 2**64 - 2, 2**64 - 1),
    )
    original_factorization = numpy_group._factorize_numpy_group_keys

    def tracking_factorization(sizes: list[int]) -> object:
        def tracked(module: object, values: object) -> object:
            sizes.append(len(values))  # type: ignore[arg-type]
            return original_factorization(module, values)

        return tracked

    for dtype, lower, upper in cases:
        matrix = np.empty((8, 2), dtype=dtype)
        matrix[:, 0] = [upper, lower, upper, lower, lower, upper, lower, upper]
        matrix[:, 1] = True if dtype.kind == "b" else 1
        query = (
            fpstreams.rows.from_numpy(matrix, columns=("key", "value"))
            .group_by("key")
            .aggregate(
                rows=fpstreams.agg.count(),
                total=fpstreams.agg.sum("value"),
                low=fpstreams.agg.min("value"),
                high=fpstreams.agg.max("value"),
            )
        )
        factorized_sizes: list[int] = []

        with monkeypatch.context() as scoped:
            scoped.setattr(relational, "_NUMPY_GROUP_CHUNK_ROWS", 4)
            scoped.setattr(numpy_group, "_native_numpy_group_endpoints", lambda: None)
            scoped.setattr(
                numpy_group,
                "_factorize_numpy_group_keys",
                tracking_factorization(factorized_sizes),
            )
            result = query.to_list()

        unit = True if dtype.kind == "b" else 1
        assert factorized_sizes == [4]
        assert result == [
            {"key": upper, "rows": 4, "total": 4, "low": unit, "high": unit},
            {"key": lower, "rows": 4, "total": 4, "low": unit, "high": unit},
        ]
        expected_key_type = bool if dtype.kind == "b" else int
        assert all(type(row["key"]) is expected_key_type for row in result)


def test_numpy_group_single_chunk_does_not_build_an_unused_dense_domain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A terminal chunk keeps the original factorizer without a state-only rescan."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution import numpy_group, relational

    matrix = np.asarray([[1, 2], [0, 3], [1, 4], [0, 5]], dtype=np.int64)
    query = (
        fpstreams.rows.from_numpy(matrix, columns=("key", "value"))
        .group_by("key")
        .aggregate(rows=fpstreams.agg.count(), total=fpstreams.agg.sum("value"))
    )

    def forbidden_domain(*_args: object) -> object:
        raise AssertionError("the final chunk cannot reuse newly discovered state")

    monkeypatch.setattr(relational, "_NUMPY_GROUP_CHUNK_ROWS", len(matrix))
    monkeypatch.setattr(numpy_group, "_native_numpy_group_endpoints", lambda: None)
    monkeypatch.setattr(numpy_group, "_closed_numpy_group_domain", forbidden_domain)

    assert query.to_list() == [
        {"key": 1, "rows": 2, "total": 6},
        {"key": 0, "rows": 2, "total": 8},
    ]


def test_numpy_group_closed_domain_rechecks_a_shrinking_live_array(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Dense-state reuse must recompute a partial chunk after retained-array shrinkage."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution import numpy_group, relational

    live = np.asarray(
        [[1, 10], [0, 1], [1, 20], [0, 2], [1, 30], [0, 3], [1, 40], [0, 4]],
        dtype=np.int64,
    )
    query = (
        fpstreams.rows.from_numpy(live, columns=("key", "value"))
        .group_by("key")
        .aggregate(rows=fpstreams.agg.count(), total=fpstreams.agg.sum("value"))
    )
    original_reuse = numpy_group._factorize_closed_numpy_group_domain
    reuse_sizes: list[int] = []

    def shrink_after_reuse(module: object, values: object, domain: object) -> object:
        result = original_reuse(module, values, domain)  # type: ignore[arg-type]
        reuse_sizes.append(len(values))  # type: ignore[arg-type]
        if len(reuse_sizes) == 1:
            live.resize((6, 2), refcheck=False)
        return result

    monkeypatch.setattr(relational, "_NUMPY_GROUP_CHUNK_ROWS", 4)
    monkeypatch.setattr(numpy_group, "_native_numpy_group_endpoints", lambda: None)
    monkeypatch.setattr(
        numpy_group,
        "_factorize_closed_numpy_group_domain",
        shrink_after_reuse,
    )

    assert query.to_list() == [
        {"key": 1, "rows": 3, "total": 60},
        {"key": 0, "rows": 3, "total": 6},
    ]
    assert reuse_sizes == [4, 2]


def test_numpy_group_closed_domain_discards_speculative_state_after_regrowth(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A shrink retry cannot reuse removed keys to reorder a subsequently grown tail."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution import numpy_group, relational

    live = np.asarray(
        [
            [3, 30],
            [0, 0],
            [1, 10],
            [2, 20],
            [3, 31],
            [0, 1],
            [1, 11],
            [2, 21],
        ],
        dtype=np.int64,
    )
    query = (
        fpstreams.rows.from_numpy(live, columns=("key", "value"))
        .group_by("key")
        .aggregate(rows=fpstreams.agg.count(), total=fpstreams.agg.sum("value"))
    )
    original_factorization = numpy_group._factorize_numpy_group_keys
    calls = 0

    def resize_between_retries(module: object, values: object) -> object:
        nonlocal calls
        result = original_factorization(module, values)
        calls += 1
        if calls == 1:
            live.resize((2, 2), refcheck=False)
        elif calls == 2:
            live.resize((4, 2), refcheck=False)
            live[2:] = [[2, 200], [1, 100]]
        return result

    monkeypatch.setattr(relational, "_NUMPY_GROUP_CHUNK_ROWS", 4)
    monkeypatch.setattr(numpy_group, "_native_numpy_group_endpoints", lambda: None)
    monkeypatch.setattr(
        numpy_group,
        "_factorize_numpy_group_keys",
        resize_between_retries,
    )

    assert query.to_list() == [
        {"key": 3, "rows": 1, "total": 30},
        {"key": 0, "rows": 1, "total": 0},
        {"key": 2, "rows": 1, "total": 200},
        {"key": 1, "rows": 1, "total": 100},
    ]


@pytest.mark.parametrize(
    ("names", "values", "expected"),
    [
        (
            ("left", "right"),
            [[10, 20], [30, 40]],
            [
                {"key": 2, "left": 10, "right": 30},
                {"key": 1, "left": 20, "right": 40},
            ],
        ),
        (
            ("first", "second", "third"),
            [[10, 20], [30, 40], [50, 60]],
            [
                {"key": 2, "first": 10, "second": 30, "third": 50},
                {"key": 1, "first": 20, "second": 40, "third": 60},
            ],
        ),
        (
            ("first", "second", "third", "fourth"),
            [[10, 20], [30, 40], [50, 60], [70, 80]],
            [
                {
                    "key": 2,
                    "first": 10,
                    "second": 30,
                    "third": 50,
                    "fourth": 70,
                },
                {
                    "key": 1,
                    "first": 20,
                    "second": 40,
                    "third": 60,
                    "fourth": 80,
                },
            ],
        ),
    ],
)
def test_numpy_group_fixed_width_materialization_reads_lane_metadata_once(
    names: tuple[str, ...],
    values: list[list[int]],
    expected: list[dict[str, int]],
) -> None:
    """Two-to-four-lane output must not rescan lane metadata for every group."""
    from fpstreams.execution.numpy_group import _materialize_numpy_group_rows
    from fpstreams.physical.relational import NumpyGroupLaneSpec

    class CountingLaneStates(list[list[int]]):
        iterations = 0

        def __iter__(self) -> Iterator[list[int]]:
            self.iterations += 1
            return super().__iter__()

    lane_states = CountingLaneStates(values)
    lanes = tuple(NumpyGroupLaneSpec(name, "sum", "value") for name in names)

    result = _materialize_numpy_group_rows([2, 1], lane_states, "key", lanes)

    assert result == expected
    assert lane_states.iterations == 0


def test_numpy_group_aggregate_keeps_python_integer_and_boolean_semantics() -> None:
    """Chunk partials must never wrap, and NumPy scalar keys must become Python scalars."""
    np = pytest.importorskip("numpy")
    maximum = 2**63 - 1
    cases = (
        (
            np.asarray([[1, 120], [1, 120], [-1, -120]], dtype=np.int8),
            [
                {"key": 1, "rows": 2, "total": 240, "low": 120, "high": 120},
                {"key": -1, "rows": 1, "total": -120, "low": -120, "high": -120},
            ],
        ),
        (
            np.asarray([[1, maximum], [1, maximum]], dtype=np.int64),
            [{"key": 1, "rows": 2, "total": maximum * 2, "low": maximum, "high": maximum}],
        ),
        (
            np.asarray([[1, 2**64 - 1], [1, 2**64 - 1]], dtype=np.uint64),
            [
                {
                    "key": 1,
                    "rows": 2,
                    "total": (2**64 - 1) * 2,
                    "low": 2**64 - 1,
                    "high": 2**64 - 1,
                }
            ],
        ),
        (
            np.asarray([[True, True], [False, True], [True, False]], dtype=np.bool_),
            [
                {"key": True, "rows": 2, "total": 1, "low": False, "high": True},
                {"key": False, "rows": 1, "total": 1, "low": True, "high": True},
            ],
        ),
        (
            np.asarray([[2, 4], [1, -3], [2, 5]], dtype=">i8"),
            [
                {"key": 2, "rows": 2, "total": 9, "low": 4, "high": 5},
                {"key": 1, "rows": 1, "total": -3, "low": -3, "high": -3},
            ],
        ),
    )
    for matrix, expected in cases:
        result = (
            fpstreams.rows.from_numpy(matrix, columns=("key", "value"))
            .group_by("key")
            .aggregate(
                rows=fpstreams.agg.count(),
                total=fpstreams.agg.sum("value"),
                low=fpstreams.agg.min("value"),
                high=fpstreams.agg.max("value"),
            )
            .to_list()
        )
        assert result == expected
        assert all(
            type(row["key"]) is type(expected_row["key"])
            for row, expected_row in zip(result, expected, strict=True)
        )
        assert all(
            type(row[name]) is type(expected_row[name])
            for row, expected_row in zip(result, expected, strict=True)
            for name in ("rows", "total", "low", "high")
        )

    empty = np.empty((0, 1), dtype=np.int64)
    assert (
        fpstreams.rows.from_numpy(empty, columns=("present",))
        .group_by("missing")
        .aggregate(total=fpstreams.agg.sum("also_missing"))
        .to_list()
    ) == []
    with pytest.raises(fpstreams.SelectionError) as missing:
        (
            fpstreams.rows.from_numpy(np.asarray([[1]], dtype=np.int64), columns=("present",))
            .group_by("missing")
            .aggregate(total=fpstreams.agg.sum("also_missing"))
            .to_list()
        )
    assert isinstance(missing.value.__cause__, KeyError)

    maximum_key = 2**63 - 1
    assert (
        fpstreams.rows.from_numpy(
            np.asarray([[maximum_key], [maximum_key]], dtype=np.int64),
            columns=("value",),
        )
        .group_by(bucket="value")
        .aggregate(total=fpstreams.agg.sum("value"))
        .to_list()
    ) == [{"bucket": maximum_key, "total": maximum_key * 2}]


def test_numpy_group_low_cardinality_overflow_sum_stays_columnar() -> None:
    """Exact wide totals must not fall back to a Python loop for every source row."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution.numpy_group import _exact_numpy_group_sum

    class NoRowList(np.ndarray):
        def tolist(self) -> list[object]:
            raise AssertionError("low-cardinality exact grouping must remain columnar")

    maximum = 2**63 - 1
    values = np.asarray([maximum, -maximum] * 8, dtype=np.int64).view(NoRowList)
    inverse = np.asarray([0, 1] * 8, dtype=np.intp).view(NoRowList)

    totals = _exact_numpy_group_sum(np, values, inverse, 2)

    assert list(totals) == [maximum * 8, -maximum * 8]
    assert all(type(value) is int for value in totals)


def test_numpy_group_float_exact_integer_sum_uses_bincount(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Safely bounded integer sums should use the faster exact weighted reduction."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution.numpy_group import _exact_numpy_group_sum

    values = np.asarray([100_000, -100_000, 7, -3] * 8, dtype=np.int32)
    inverse = np.arange(32, dtype=np.intp) % 4
    original_bincount = np.bincount
    calls = 0

    def tracked_bincount(*args: object, **kwargs: object) -> object:
        nonlocal calls
        calls += 1
        return original_bincount(*args, **kwargs)

    monkeypatch.setattr(np, "bincount", tracked_bincount)

    totals = _exact_numpy_group_sum(np, values, inverse, 4)

    assert totals.tolist() == [800_000, -800_000, 56, -24]
    assert totals.dtype == np.dtype(np.int64)
    assert calls == 1

    one_group = _exact_numpy_group_sum(np, values, np.zeros(32, dtype=np.intp), 1)
    assert one_group.tolist() == [32]
    assert calls == 1

    native_i64 = values.astype(np.int64)
    assert _exact_numpy_group_sum(np, native_i64, inverse, 4).tolist() == [
        800_000,
        -800_000,
        56,
        -24,
    ]
    assert calls == 1


def test_numpy_group_keeps_internal_list_checks_stable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An implementation-only list lookup must not change public grouped results or routing."""
    import builtins

    np = pytest.importorskip("numpy")
    maximum = 2**63 - 1
    query = (
        fpstreams.rows.from_numpy(
            np.asarray([[1, maximum], [1, maximum]], dtype=np.int64),
            columns=("key", "value"),
        )
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
    )
    from fpstreams.execution import relational

    def forbidden_rows(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("an implementation-only list replacement must stay columnar")

    monkeypatch.setattr(relational, "_execute_python_group_values", forbidden_rows)
    original_list = builtins.list
    try:
        builtins.list = lambda *_args, **_kwargs: ["changed"]
        result = tuple(query)
    finally:
        builtins.list = original_list

    assert result == ({"key": 1, "total": maximum * 2},)


@pytest.mark.parametrize(
    ("dtype_name", "left", "right"),
    [
        ("int64", -(2**63), 2**63 - 1),
        ("uint64", 2**64 - 1, 2**63),
        ("opposite_i64", -(2**63), 2**63 - 1),
        ("opposite_u64", 2**64 - 1, 2**63),
    ],
)
def test_numpy_group_exact_limb_sum_covers_extremes_and_byte_orders(
    dtype_name: str,
    left: int,
    right: int,
) -> None:
    """Both signed limbs and endian conversions must remain exact without row boxing."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution.numpy_group import _exact_numpy_group_sum

    class NoRowList(np.ndarray):
        def tolist(self) -> list[object]:
            raise AssertionError("the exact limb path must not box source rows")

    dtype = {
        "int64": "int64",
        "uint64": "uint64",
        "opposite_i64": ">i8" if sys.byteorder == "little" else "<i8",
        "opposite_u64": ">u8" if sys.byteorder == "little" else "<u8",
    }[dtype_name]
    values = np.asarray([left, right] * 8, dtype=dtype).view(NoRowList)
    inverse = np.asarray([0, 1] * 8, dtype=np.intp).view(NoRowList)

    totals = _exact_numpy_group_sum(np, values, inverse, 2)

    assert totals == [left * 8, right * 8]
    assert all(type(value) is int for value in totals)


def test_numpy_group_exact_limb_sum_keeps_high_cardinality_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The measured low-cardinality kernel must not penalize one-group-per-row data."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution import numpy_group

    def forbidden_limb(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("high-cardinality groups must keep the Python fallback")

    monkeypatch.setattr(numpy_group, "_exact_limb_group_sum", forbidden_limb)
    maximum = 2**63 - 1
    values = np.full(16, maximum, dtype=np.int64)
    inverse = np.arange(16, dtype=np.intp)

    assert numpy_group._exact_numpy_group_sum(np, values, inverse, 16) == [maximum] * 16


def test_numpy_group_exact_limb_partials_merge_across_bounded_chunks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Python integer state must merge limb partials without wrapping or reordering keys."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution import numpy_group, relational

    assert relational._NUMPY_GROUP_CHUNK_ROWS <= numpy_group._MAX_EXACT_LIMB_ROWS
    monkeypatch.setattr(relational, "_NUMPY_GROUP_CHUNK_ROWS", 16)
    maximum = 2**63 - 1
    minimum = -(2**63)
    matrix = np.asarray([[2, maximum], [1, minimum]] * 20, dtype=np.int64)

    result = (
        fpstreams.rows.from_numpy(matrix, columns=("key", "value"))
        .group_by("key")
        .aggregate(
            rows=fpstreams.agg.count(),
            total=fpstreams.agg.sum("value"),
        )
        .to_list()
    )

    assert result == [
        {"key": 2, "rows": 20, "total": maximum * 20},
        {"key": 1, "rows": 20, "total": minimum * 20},
    ]


def test_numpy_group_aggregate_deopts_for_failpoints_and_tracks_source_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Failpoints and live shape changes keep their canonical boundaries."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution import numpy_group, relational
    from fpstreams.runtime.failpoints import failpoint

    matrix = np.asarray([[1, 2], [1, 3]], dtype=np.int64)
    source = fpstreams.rows.from_numpy(matrix, columns=("key", "value"))
    expected = [{"key": 1, "total": 5}]

    def forbidden_columnar(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("observable execution must bypass NumPy grouped columns")

    original_group_aggregate = numpy_group._numpy_group_aggregate
    monkeypatch.setattr(numpy_group, "_numpy_group_aggregate", forbidden_columnar)
    assert (
        source.with_engine("python")
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
        .to_list()
        == expected
    )
    with failpoint("unrelated.numpy.group", RuntimeError("unused")):
        assert (
            source.group_by("key").aggregate(total=fpstreams.agg.sum("value")).to_list() == expected
        )

    monkeypatch.setattr(numpy_group, "_numpy_group_aggregate", original_group_aggregate)

    changing = np.asarray([[1, 2], [1, 3], [2, 4], [2, 5]], dtype=np.int64)
    changing_source = fpstreams.rows.from_numpy(changing, columns=("key", "value"))
    original_factorization = numpy_group._factorize_numpy_group_keys
    calls = 0

    monkeypatch.setattr(numpy_group, "_native_numpy_group_endpoints", lambda: None)

    def reshape_after_first_chunk(module: object, values: object) -> object:
        nonlocal calls
        result = original_factorization(module, values)
        calls += 1
        if calls == 1:
            changing.resize((changing.size,), refcheck=False)
        return result

    monkeypatch.setattr(relational, "_NUMPY_GROUP_CHUNK_ROWS", 2)
    monkeypatch.setattr(numpy_group, "_factorize_numpy_group_keys", reshape_after_first_chunk)
    with pytest.raises(ValueError, match="changed to 1 dimensions during iteration"):
        changing_source.group_by("key").aggregate(total=fpstreams.agg.sum("value")).to_list()

    def aggregate_after_resize(
        initial: list[list[int]],
        resized: list[list[int]],
        *,
        chunk_rows: int,
    ) -> list[dict[str, object]]:
        live = np.asarray(initial, dtype=np.int64)
        live_query = (
            fpstreams.rows.from_numpy(live, columns=("key", "value"))
            .group_by("key")
            .aggregate(total=fpstreams.agg.sum("value"))
        )
        resize_calls = 0

        def resize_after_first_chunk(module: object, values: object) -> object:
            nonlocal resize_calls
            result = original_factorization(module, values)
            resize_calls += 1
            if resize_calls == 1:
                live.resize((len(resized), 2), refcheck=False)
                live[:] = resized
            return result

        monkeypatch.setattr(relational, "_NUMPY_GROUP_CHUNK_ROWS", chunk_rows)
        monkeypatch.setattr(numpy_group, "_factorize_numpy_group_keys", resize_after_first_chunk)
        return live_query.to_list()

    assert aggregate_after_resize(
        [[1, 2], [1, 3], [2, 4], [2, 5]],
        [[1, 2]],
        chunk_rows=3,
    ) == [{"key": 1, "total": 2}]
    assert aggregate_after_resize(
        [[1, 2], [1, 3]],
        [[1, 2], [1, 3], [2, 4], [1, 7]],
        chunk_rows=2,
    ) == [{"key": 1, "total": 12}, {"key": 2, "total": 4}]


@pytest.mark.skipif(not hasattr(signal, "setitimer"), reason="requires POSIX interval timers")
@pytest.mark.parametrize("changed_dtype", ["float64", "uint64"])
def test_numpy_group_matches_python_during_signal_time_same_width_dtype_changes(
    monkeypatch: pytest.MonkeyPatch,
    changed_dtype: str,
) -> None:
    """An active timer keeps public auto grouping on Python's live dtype boundary."""
    import warnings

    np = pytest.importorskip("numpy")
    from fpstreams.execution import numpy_group
    from fpstreams.tabular import numpy as numpy_adapter

    canonical_validation = numpy_adapter._validate_retained_numpy_iteration

    def consume(engine: str) -> tuple[list[dict[str, object]] | None, BaseException | None]:
        matrix = np.zeros((200_000, 2), dtype=np.int64)
        query = (
            fpstreams.rows.from_numpy(matrix, columns=("key", "value"))
            .with_engine(engine)
            .group_by("key")
            .aggregate(total=fpstreams.agg.sum("value"))
        )
        fired = False
        armed = False

        def change_dtype(_signal_number: int, _frame: object) -> None:
            nonlocal fired
            fired = True
            matrix.dtype = np.dtype(changed_dtype)

        def arm_after_validation(values: object, width: int, dtype: object) -> int:
            nonlocal armed
            row_count = canonical_validation(values, width, dtype)
            if values is matrix and not armed:
                armed = True
                signal.setitimer(signal.ITIMER_REAL, 0.0001)
            return row_count

        previous_handler = signal.signal(signal.SIGALRM, change_dtype)
        previous_timer = signal.getitimer(signal.ITIMER_REAL)
        signal.setitimer(signal.ITIMER_REAL, 0)
        try:
            with monkeypatch.context() as scoped, warnings.catch_warnings():
                warnings.simplefilter("ignore", DeprecationWarning)
                scoped.setattr(
                    numpy_adapter,
                    "_validate_retained_numpy_iteration",
                    arm_after_validation,
                )
                scoped.setattr(
                    numpy_group,
                    "_validate_retained_numpy_iteration",
                    arm_after_validation,
                )
                try:
                    result = query.to_list()
                except BaseException as error:
                    result = None
                    captured = error
                else:
                    captured = None
        finally:
            signal.setitimer(signal.ITIMER_REAL, *previous_timer)
            signal.signal(signal.SIGALRM, previous_handler)
        assert fired
        return result, captured

    canonical = consume("python")
    automatic = consume("auto")
    assert canonical[0] is automatic[0] is None
    assert [type(canonical[1]), type(automatic[1])] == [ValueError, ValueError]
    assert (
        str(canonical[1])
        == str(automatic[1])
        == (
            f"from_numpy() retained array dtype changed from int64 to {changed_dtype} "
            "during iteration"
        )
    )


def test_numpy_row_prefix_explain_marks_only_complete_safe_plans() -> None:
    """Explain should expose only full guarded NumPy row prefixes selected by planning."""
    np = pytest.importorskip("numpy")

    integers = fpstreams.rows.from_numpy(
        np.asarray([[1, 4], [2, 3]], dtype=np.int64),
        columns=("left", "right"),
    )
    eligible = (
        integers.where(fpstreams.col("left") >= 1)
        .where(5 > fpstreams.col("right"))  # noqa: SIM300 - exercise literal-left lowering
        .select("left", copied="left")
    )
    explanation = eligible.explain("list").to_dict()

    assert explanation["numpy_prefix"] == {
        "operation_count": 3,
        "guarded": True,
    }
    assert explanation["stages"] == [
        {
            "engine": "numpy",
            "operations": ["filter", "filter", "map"],
            "fused": True,
        }
    ]

    projected_objects = fpstreams.rows.from_numpy(
        np.asarray([[object(), object()]], dtype=object),
        columns=("left", "right"),
    ).select(alias="right")
    assert projected_objects.explain("list").to_dict()["numpy_prefix"] == {
        "operation_count": 1,
        "guarded": True,
    }

    unsupported = (
        integers.with_engine("python").select("left"),
        fpstreams.rows.from_numpy(
            np.asarray([[1.0, 2.0]], dtype=np.float64),
            columns=("left", "right"),
        )
        .where(fpstreams.col("left") >= 1)
        .select("left"),
        integers.where(lambda row: row["left"] >= 1).select("left"),
    )
    for query in unsupported:
        assert query.explain("list").to_dict()["numpy_prefix"] is None


def test_numpy_row_prefix_rejects_rebound_native_descriptor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A replacement native descriptor must not detach execution from the source factory."""
    np = pytest.importorskip("numpy")
    from fpstreams.tabular.numpy import NumpyRowSource

    query = fpstreams.rows.from_numpy(
        np.asarray([[1], [0]], dtype=np.int64),
        columns=("value",),
    ).where(fpstreams.col("value") > 0)
    source = query._flow._pipeline.source
    source.native_data = NumpyRowSource(
        np.asarray([[0], [0]], dtype=np.int64),
        ("value",),
    )

    execution = query.run_with_report("to_list")

    assert execution.value == [{"value": 1}]
    assert execution.report.strategy == "planned:python"


def test_numpy_row_prefix_executes_exact_integer_with_columns_without_input_rows() -> None:
    """A pure computed column should stay columnar until final output dictionaries exist."""
    np = pytest.importorskip("numpy")
    values = np.asarray([[-2, 10], [0, 20], [3, 30]], dtype=np.int64)
    source = fpstreams.rows.from_numpy(values, columns=("left", "right"))
    query = source.with_columns(
        score=fpstreams.col("left") * 3 + 1,
        original=fpstreams.col("left"),
    ).select("score", "original", "right")

    first = query.run_with_report("to_list")
    execution = query.run_with_report("to_list")
    explanation = query.explain("list").to_dict()["numpy_prefix"]

    assert execution.value == [
        {"score": -5, "original": -2, "right": 10},
        {"score": 1, "original": 0, "right": 20},
        {"score": 10, "original": 3, "right": 30},
    ]
    assert execution.value == query.with_engine("python").to_list()
    assert first.value == execution.value
    assert first.report.strategy == "planned:python"
    assert execution.report.strategy == "numpy_direct"
    assert explanation == {
        "operation_count": 2,
        "guarded": True,
    }
    assert query.explain("list").to_dict()["numpy_prefix"] == explanation


def test_numpy_row_prefix_computed_columns_keep_python_integer_and_sibling_semantics() -> None:
    """Computed columns remain unbounded Python integers and read the original sibling values."""
    np = pytest.importorskip("numpy")
    maximum = 2**63 - 1
    minimum = -(2**63)
    source = fpstreams.rows.from_numpy(
        np.asarray([[maximum], [minimum]], dtype=np.int64),
        columns=("value",),
    )
    query = source.with_columns(
        value=fpstreams.col("value") + 1,
        sibling=fpstreams.col("value"),
        tripled=fpstreams.col("value") * 3,
    ).select("value", "sibling", "tripled")

    first = query.run_with_report("to_list")
    execution = query.run_with_report("to_list")

    assert execution.value == [
        {"value": maximum + 1, "sibling": maximum, "tripled": maximum * 3},
        {"value": minimum + 1, "sibling": minimum, "tripled": minimum * 3},
    ]
    assert execution.value == query.with_engine("python").to_list()
    assert first.value == execution.value
    assert first.report.strategy == "planned:python"
    assert execution.report.strategy == "numpy_direct"


def test_numpy_row_prefix_computed_constant_subtrees_use_python_arithmetic() -> None:
    """Literal-only subtrees must not inherit fixed-width NumPy scalar behavior."""
    np = pytest.importorskip("numpy")
    maximum = 2**63 - 1
    source = fpstreams.rows.from_numpy(
        np.asarray([[1]], dtype=np.int64),
        columns=("value",),
    )
    query = source.with_columns(
        small=fpstreams.lit(1) + 2,
        overflow=fpstreams.lit(maximum) + 1,
        multiplied=fpstreams.lit(2**62) * 4,
        booleans=fpstreams.lit(True) + True,
        nested=fpstreams.col("value") + (fpstreams.lit(maximum) + 1),
        negative=-fpstreams.lit(-(2**80)),
        magnitude=abs(fpstreams.lit(-(2**80))),
    )

    first = query.run_with_report("to_list")
    execution = query.run_with_report("to_list")
    expected = query.with_engine("python").to_list()

    assert (
        execution.value
        == expected
        == [
            {
                "value": 1,
                "small": 3,
                "overflow": 2**63,
                "multiplied": 2**64,
                "booleans": 2,
                "nested": 2**63 + 1,
                "negative": 2**80,
                "magnitude": 2**80,
            }
        ]
    )
    assert all(type(value) is int for value in execution.value[0].values())
    assert first.value == execution.value
    assert first.report.strategy == "planned:python"
    assert execution.report.strategy == "numpy_direct"


def test_numpy_row_prefix_deopts_deep_computed_expression_trees() -> None:
    """Columnar evaluation must not recurse beyond Python's iterative RowExpr fallback."""
    np = pytest.importorskip("numpy")
    expression = fpstreams.col("value")
    for _index in range(1_200):
        expression = expression + 1
    query = fpstreams.rows.from_numpy(
        np.asarray([[1]], dtype=np.int64),
        columns=("value",),
    ).with_columns(result=expression)

    execution = query.run_with_report("to_list")

    assert execution.value == [{"value": 1, "result": 1_201}]
    assert execution.report.strategy == "planned:python"


def test_numpy_row_prefix_cached_fallback_ignores_python_recursion_limit() -> None:
    """A low-limit iterative compile remains a safe reusable Python fallback."""
    np = pytest.importorskip("numpy")
    expression = fpstreams.col("value")
    for _index in range(80):
        expression = expression + 1
    query = fpstreams.rows.from_numpy(
        np.asarray([[1]], dtype=np.int64),
        columns=("value",),
    ).with_columns(result=expression)

    previous_limit = sys.getrecursionlimit()
    try:
        sys.setrecursionlimit(100)
        first = query.run_with_report("to_list")
        execution = query.run_with_report("to_list")
    finally:
        sys.setrecursionlimit(previous_limit)

    assert execution.value == [{"value": 1, "result": 81}]
    assert first.value == execution.value
    assert first.report.strategy == "planned:python"
    assert execution.report.strategy == "planned:python"
    assert query.explain("list").to_dict()["numpy_prefix"] is None


def test_numpy_row_prefix_computed_columns_preserve_python_integer_identity() -> None:
    """Bare fields share row objects while evaluated constants remain per-row results."""
    np = pytest.importorskip("numpy")

    def query() -> fpstreams.Rows[dict[str, object]]:
        return fpstreams.rows.from_numpy(
            np.asarray([[1], [2]], dtype=np.int64),
            columns=("row",),
        ).with_columns(
            source=fpstreams.col("row") + 999,
            first=fpstreams.col("row") + 999,
            bare=fpstreams.col("row"),
            calculated=fpstreams.lit(1000) + 1,
            literal=fpstreams.lit(1000),
        )

    automatic_query = query()
    first = automatic_query.run_with_report("to_list")
    automatic = automatic_query.run_with_report("to_list")
    canonical = query().with_engine("python").to_list()

    for rows in (first.value, automatic.value, canonical):
        assert rows[0]["source"] == rows[0]["first"] == 1000
        assert rows[0]["source"] is not rows[0]["first"]
        assert rows[0]["bare"] is rows[0]["row"]
        assert rows[0]["calculated"] == rows[1]["calculated"] == 1001
        assert rows[0]["calculated"] is not rows[1]["calculated"]
        assert rows[0]["literal"] is rows[1]["literal"]
    assert first.report.strategy == "planned:python"
    assert automatic.report.strategy == "numpy_direct"

    def absolute_query() -> fpstreams.Rows[dict[str, object]]:
        return fpstreams.rows.from_numpy(
            np.asarray([[10**12], [-(10**12)]], dtype=np.int64),
            columns=("value",),
        ).with_columns(magnitude=abs(fpstreams.col("value")))

    automatic_absolute_query = absolute_query()
    first_absolute = automatic_absolute_query.run_with_report("to_list")
    automatic_absolute = automatic_absolute_query.run_with_report("to_list")
    canonical_absolute = absolute_query().with_engine("python").to_list()
    for rows in (first_absolute.value, automatic_absolute.value, canonical_absolute):
        assert rows[0]["magnitude"] is rows[0]["value"]
        assert rows[1]["magnitude"] == 10**12
    assert automatic_absolute.value == canonical_absolute
    assert first_absolute.report.strategy == "planned:python"
    assert automatic_absolute.report.strategy == "numpy_direct"


def test_numpy_row_prefix_computed_columns_keep_lazy_missing_field_boundaries() -> None:
    """A missing dependency is observed only when a row reaches its computed stage."""
    np = pytest.importorskip("numpy")
    empty = fpstreams.rows.from_numpy(
        np.empty((0, 1), dtype=np.int64),
        columns=("present",),
    ).with_columns(result=fpstreams.col("missing") + 1)
    skipped = (
        fpstreams.rows.from_numpy(
            np.asarray([[1], [2]], dtype=np.int64),
            columns=("present",),
        )
        .where(fpstreams.col("present") < 0)
        .with_columns(result=fpstreams.col("missing") + 1)
    )

    assert empty.to_list() == []
    assert skipped.to_list() == []

    reached = fpstreams.rows.from_numpy(
        np.asarray([[1]], dtype=np.int64),
        columns=("present",),
    ).with_columns(result=fpstreams.col("missing") + 1)
    with pytest.raises(fpstreams.SelectionError) as automatic:
        reached.to_list()
    with pytest.raises(fpstreams.SelectionError) as canonical:
        reached.with_engine("python").to_list()
    assert str(automatic.value) == str(canonical.value)
    assert type(automatic.value.__cause__) is type(canonical.value.__cause__) is KeyError
    assert automatic.value.__context__ is automatic.value.__cause__
    assert canonical.value.__context__ is canonical.value.__cause__


def test_numpy_row_prefix_executes_one_short_circuit_conjunction_stage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A primitive AND tree should remain one planned filter while evaluating left to right."""
    np = pytest.importorskip("numpy")
    source = fpstreams.rows.from_numpy(
        np.asarray([[0, 5], [1, 4], [2, 3], [3, 2], [4, 1]], dtype=np.int64),
        columns=("left", "right"),
    )
    query = source.where(
        (fpstreams.col("left") >= 1) & (fpstreams.col("right") < 4) & (10 > fpstreams.col("left"))  # noqa: SIM300 - test reversed comparison lowering
    ).select("left", "right")

    execution = query.run_with_report("to_list")

    assert execution.value == [
        {"left": 2, "right": 3},
        {"left": 3, "right": 2},
        {"left": 4, "right": 1},
    ]
    assert execution.value == query.with_engine("python").to_list()
    assert execution.report.strategy == "numpy_direct"
    assert query.explain("list").to_dict()["numpy_prefix"] == {
        "operation_count": 2,
        "guarded": True,
    }

    with monkeypatch.context() as scoped:
        scoped.setattr(np, "greater_equal", lambda _left, _right: False)
        scoped.setattr(np, "logical_and", lambda _left, _right: False)
        ufunc_execution = query.run_with_report("to_list")
    assert ufunc_execution.value == [
        {"left": 2, "right": 3},
        {"left": 3, "right": 2},
        {"left": 4, "right": 1},
    ]
    assert ufunc_execution.report.strategy == "numpy_direct"

    import operator

    stable_helpers = source.where(
        (fpstreams.col("left") < 2**100) & (fpstreams.col("right") > 0)
    ).select("left", "right")
    with monkeypatch.context() as scoped:
        scoped.setattr(operator, "eq", lambda _left, _right: False)
        scoped.setattr(operator, "and_", lambda _left, _right: False)
        scoped.setattr(np, "ascontiguousarray", lambda values: values * 0)
        helper_execution = stable_helpers.run_with_report("to_list")
    assert helper_execution.value == [
        {"left": 0, "right": 5},
        {"left": 1, "right": 4},
        {"left": 2, "right": 3},
        {"left": 3, "right": 2},
        {"left": 4, "right": 1},
    ]
    assert helper_execution.report.strategy == "numpy_direct"


def test_numpy_row_prefix_conjunction_preserves_reachable_missing_rhs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A missing right operand is skipped only when every active left operand is false."""
    np = pytest.importorskip("numpy")
    source = fpstreams.rows.from_numpy(
        np.asarray([[0], [1]], dtype=np.int64),
        columns=("value",),
    )
    skipped = source.where((fpstreams.col("value") < 0) & (fpstreams.col("missing") > 0))
    reached = source.where((fpstreams.col("value") >= 0) & (fpstreams.col("missing") > 0))

    skipped_execution = skipped.run_with_report("to_list")
    assert skipped_execution.value == []
    assert skipped_execution.report.strategy == "numpy_direct"

    with pytest.raises(fpstreams.SelectionError) as automatic:
        reached.to_list()
    with pytest.raises(fpstreams.SelectionError) as canonical:
        reached.with_engine("python").to_list()
    assert str(automatic.value) == str(canonical.value)
    assert type(automatic.value.__cause__) is type(canonical.value.__cause__) is KeyError

    from fpstreams.execution import numpy_prefix

    monkeypatch.setattr(numpy_prefix, "_NUMPY_PREFIX_CHUNK_ROWS", 2)
    later_chunk = fpstreams.rows.from_numpy(
        np.asarray([[0], [0], [1]], dtype=np.int64),
        columns=("value",),
    ).where((fpstreams.col("value") > 0) & (fpstreams.col("missing") > 0))
    with pytest.raises(fpstreams.SelectionError) as later_automatic:
        later_chunk.to_list()
    with pytest.raises(fpstreams.SelectionError) as later_canonical:
        later_chunk.with_engine("python").to_list()
    assert str(later_automatic.value) == str(later_canonical.value)


def test_numpy_row_prefix_rechecks_filter_dtype_at_execution_boundary() -> None:
    """A retained dtype changed after compilation must deopt before NumPy comparison."""
    import warnings

    np = pytest.importorskip("numpy")
    from fpstreams.execution.numpy_prefix import try_numpy_prefix_list

    values = np.asarray([[1], [2]], dtype=np.int64)
    query = fpstreams.rows.from_numpy(values, columns=("value",)).where(fpstreams.col("value") > 0)
    physical, pipeline = query._flow._terminal_context("list")
    assert physical.backend_payload is not None
    assert physical.backend_payload.numpy_prefix is not None

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        values.dtype = np.dtype("float64")
    assert try_numpy_prefix_list(query._flow, physical, pipeline) == (False, None)
    assert query.to_list() == query.with_engine("python").to_list()


def test_numpy_row_prefix_materializes_lists_and_columns_without_python_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A complete primitive prefix should reach both sinks before the row executor."""
    np = pytest.importorskip("numpy")
    from fpstreams.streams import flow_terminals
    from fpstreams.streams.flow import Flow

    values = np.asarray(
        [[0, 9, 100], [1, 8, 101], [2, 7, 102], [3, 6, 103], [4, 5, 104]],
        dtype=np.int64,
    )

    def query() -> fpstreams.Rows[dict[str, object]]:
        return (
            fpstreams.rows.from_numpy(values, columns=("left", "right", "tail"))
            .where(fpstreams.col("left") >= 1)
            .where(4 > fpstreams.col("left"))  # noqa: SIM300 - literal-left comparison
            .select(id="left", copied="left", payload="tail")
        )

    def forbidden_rows(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("a safe NumPy prefix must not enter the Python row executor")

    monkeypatch.setattr(flow_terminals, "execute_physical", forbidden_rows)
    execution = query().run_with_report("to_list")
    assert execution.value == [
        {"id": 1, "copied": 1, "payload": 101},
        {"id": 2, "copied": 2, "payload": 102},
        {"id": 3, "copied": 3, "payload": 103},
    ]
    assert execution.report.strategy == "numpy_direct"

    def forbidden_consume(
        self: Flow[object],
        consumer: object,
    ) -> object:
        del self, consumer
        raise AssertionError("a safe NumPy prefix must not consume Python rows")

    monkeypatch.setattr(Flow, "_consume", forbidden_consume)
    columns = query().to_columns()
    assert columns == {
        "id": [1, 2, 3],
        "copied": [1, 2, 3],
        "payload": [101, 102, 103],
    }
    assert columns["id"] is not columns["copied"]


def test_numpy_row_prefix_preserves_empty_and_nonempty_missing_field_boundaries() -> None:
    """Missing direct fields should stay lazy until the first row reaching their stage."""
    np = pytest.importorskip("numpy")

    empty = fpstreams.rows.from_numpy(
        np.empty((0, 1), dtype=np.int64),
        columns=("present",),
    )
    assert empty.select("missing").to_list() == []
    assert empty.where(fpstreams.col("missing") == 1).select("present").to_columns() == {}

    nonempty = fpstreams.rows.from_numpy(
        np.asarray([[1]], dtype=np.int64),
        columns=("present",),
    )
    queries = (
        nonempty.select("missing"),
        nonempty.where(fpstreams.col("missing") == 1).select("present"),
    )
    for query in queries:
        with pytest.raises(fpstreams.SelectionError) as automatic:
            query.to_list()
        with pytest.raises(fpstreams.SelectionError) as canonical:
            query.with_engine("python").to_list()
        assert str(automatic.value) == str(canonical.value)
        assert type(automatic.value.__cause__) is type(canonical.value.__cause__) is KeyError

    assert nonempty.where(fpstreams.col("present") < 0).select("missing").to_list() == []
    with pytest.raises(fpstreams.SelectionError):
        nonempty.where(fpstreams.col("present") > 0).select("missing").to_list()


@pytest.mark.parametrize(
    ("dtype", "raw_values"),
    [
        ("bool", [False, True]),
        ("int8", [-128, -1, 0, 1, 127]),
        ("uint8", [0, 1, 127, 255]),
        ("int64", [-(2**63), -1, 0, 1, 2**63 - 1]),
        ("uint64", [0, 1, 2**63, 2**64 - 1]),
    ],
)
def test_numpy_row_prefix_compares_exact_python_integer_literals_without_coercion(
    dtype: str,
    raw_values: list[int | bool],
) -> None:
    """Out-of-domain literals fold exactly instead of entering NumPy's coercion rules."""
    import operator

    np = pytest.importorskip("numpy")
    source = fpstreams.rows.from_numpy(
        np.asarray(raw_values, dtype=dtype).reshape(-1, 1),
        columns=("value",),
    )
    comparisons = (
        operator.eq,
        operator.ne,
        operator.lt,
        operator.le,
        operator.gt,
        operator.ge,
    )
    literals = (
        False,
        True,
        -(2**100),
        -1,
        0,
        1,
        2,
        127,
        128,
        255,
        256,
        2**63 - 1,
        2**64 - 1,
        2**100,
    )
    for compare in comparisons:
        for literal in literals:
            for reversed_operands in (False, True):
                field = fpstreams.col("value")
                predicate = (
                    compare(literal, field) if reversed_operands else compare(field, literal)
                )
                automatic = source.where(predicate).select("value").to_list()
                canonical = source.with_engine("python").where(predicate).select("value").to_list()
                assert automatic == canonical


@pytest.mark.parametrize(
    ("predicate", "expected"),
    [
        (fpstreams.lit(2) == fpstreams.col("value"), [2]),
        (fpstreams.lit(2) != fpstreams.col("value"), [1, 3]),
        (fpstreams.lit(2) < fpstreams.col("value"), [3]),
        (fpstreams.lit(2) <= fpstreams.col("value"), [2, 3]),
        (fpstreams.lit(2) > fpstreams.col("value"), [1]),
        (fpstreams.lit(2) >= fpstreams.col("value"), [1, 2]),
    ],
)
def test_numpy_row_prefix_preserves_explicit_literal_left_comparisons(
    predicate: object,
    expected: list[int],
) -> None:
    """Literal expression nodes keep Python ordering when NumPy reverses the comparison."""
    np = pytest.importorskip("numpy")
    source = fpstreams.rows.from_numpy(
        np.asarray([[1], [2], [3]], dtype=np.int64),
        columns=("value",),
    )
    query = source.where(predicate).select("value")

    execution = query.run_with_report("to_list")

    assert execution.value == [{"value": value} for value in expected]
    assert execution.value == query.with_engine("python").to_list()
    assert execution.report.strategy == "numpy_direct"


def test_numpy_row_prefix_declines_an_empty_projection() -> None:
    """Zero-column records retain the canonical row path and their hidden cardinality."""
    np = pytest.importorskip("numpy")

    source = fpstreams.rows.from_numpy(
        np.asarray([[1], [2]], dtype=np.int64),
        columns=("value",),
    ).select()

    assert source.explain("list").to_dict()["numpy_prefix"] is None
    assert source.to_list() == [{}, {}]
    assert source.to_columns() == {}


def test_numpy_row_prefix_declines_zero_width_rename_without_losing_cardinality() -> None:
    """A schema-only rename cannot represent nonzero rows when no output columns exist."""
    np = pytest.importorskip("numpy")
    source = fpstreams.rows.from_numpy(
        np.empty((3, 0), dtype=np.int64),
        columns=(),
    ).rename()

    execution = source.run_with_report("to_list")

    assert execution.value == source.with_engine("python").to_list() == [{}, {}, {}]
    assert execution.report.strategy == "planned:python"
    assert source.explain("list").to_dict()["numpy_prefix"] is None


@pytest.mark.parametrize(
    "route",
    ["identity", "columns", "arrow", "prefix", "group"],
)
def test_numpy_direct_row_paths_deopt_before_hashing_string_subclass_columns(
    route: str,
) -> None:
    """Planning and direct sinks cannot add or remove observable column-name hash calls."""
    np = pytest.importorskip("numpy")

    class ArmedColumn(str):
        armed = False
        calls = 0

        def __hash__(self) -> int:
            if self.armed:
                type(self).calls += 1
            return super().__hash__()

    def consume(engine: str) -> tuple[object, int, str | None]:
        name = ArmedColumn("value")
        rows = fpstreams.rows.from_numpy(
            np.asarray([[1], [2]], dtype=np.int64),
            columns=(name,),
        ).with_engine(engine)
        query: object
        if route == "prefix":
            query = rows.where(fpstreams.col("value") > 0).select("value")
        elif route == "group":
            query = rows.group_by("value").aggregate(rows=fpstreams.agg.count())
        else:
            query = rows
        name.armed = True
        ArmedColumn.calls = 0
        if route == "columns":
            result = rows.to_columns()
            strategy = None
        elif route == "arrow":
            result = rows.to_arrow().to_pylist()
            strategy = None
        else:
            execution = query.run_with_report("to_list")  # type: ignore[attr-defined]
            result = execution.value
            strategy = execution.report.strategy
        return result, ArmedColumn.calls, strategy

    canonical = consume("python")
    automatic = consume("auto")
    assert automatic[:2] == canonical[:2]
    assert canonical[1] > 0
    if route in {"identity", "prefix", "group"}:
        assert automatic[2] == "planned:python"


@pytest.mark.parametrize("changed_dtype", ["float64", "uint64"])
@pytest.mark.parametrize("sink", ["list", "columns", "arrow", "prefix", "group"])
def test_numpy_row_scans_match_python_on_threaded_same_width_dtype_changes(
    monkeypatch: pytest.MonkeyPatch,
    changed_dtype: str,
    sink: str,
) -> None:
    """A real worker transition is detected by every optimized retained-row scan."""
    import warnings
    from threading import Event, Thread

    np = pytest.importorskip("numpy")
    from fpstreams.execution import numpy_group, numpy_prefix
    from fpstreams.tabular import numpy as numpy_adapter

    canonical_validation = numpy_adapter._validate_retained_numpy_iteration

    def consume(engine: str) -> BaseException | None:
        values = np.zeros((20_000, 2), dtype=np.int64)
        rows = fpstreams.rows.from_numpy(values, columns=("key", "value")).with_engine(engine)
        ready = Event()
        fired = Event()
        worker_errors: list[BaseException] = []
        released = False

        def mutate() -> None:
            try:
                if not ready.wait(5):
                    raise TimeoutError("NumPy scan never reached its first validated boundary")
                values.dtype = np.dtype(changed_dtype)
            except BaseException as error:
                worker_errors.append(error)
            finally:
                fired.set()

        def release_after_validation(
            matrix: object,
            width: int,
            dtype: object | None = None,
        ) -> int:
            nonlocal released
            row_count = canonical_validation(matrix, width, dtype)
            if matrix is values and not released:
                released = True
                ready.set()
                if not fired.wait(5):
                    raise TimeoutError("dtype mutation worker did not finish")
            return row_count

        worker = Thread(target=mutate)
        worker.start()
        try:
            with (
                monkeypatch.context() as scoped,
                warnings.catch_warnings(),
            ):
                warnings.simplefilter("ignore", DeprecationWarning)
                for module in (numpy_adapter, numpy_prefix, numpy_group):
                    scoped.setattr(
                        module,
                        "_validate_retained_numpy_iteration",
                        release_after_validation,
                    )
                try:
                    if sink == "list":
                        rows.to_list()
                    elif sink == "columns":
                        rows.to_columns()
                    elif sink == "arrow":
                        rows.to_arrow(batch_size=1_024)
                    elif sink == "prefix":
                        rows.where(fpstreams.col("key") == 0).select("value").to_list()
                    else:
                        rows.group_by("key").aggregate(total=fpstreams.agg.sum("value")).to_list()
                except BaseException as error:
                    captured = error
                else:
                    captured = None
        finally:
            worker.join(5)
        assert not worker.is_alive()
        assert fired.is_set()
        assert worker_errors == []
        return captured

    canonical = consume("python")
    automatic = consume("auto")
    assert [type(canonical), type(automatic)] == [ValueError, ValueError]
    assert (
        str(canonical)
        == str(automatic)
        == (
            f"from_numpy() retained array dtype changed from int64 to {changed_dtype} "
            "during iteration"
        )
    )


@pytest.mark.parametrize("action", ["shrink", "grow", "reshape", "width", "dtype"])
def test_numpy_row_prefix_preserves_live_matrix_boundaries(
    monkeypatch: pytest.MonkeyPatch,
    action: str,
) -> None:
    """Each bounded prefix chunk revalidates live length, width, dimensions, and dtype."""
    import warnings

    np = pytest.importorskip("numpy")
    from fpstreams.execution import numpy_prefix

    initial = (
        [[1, 10], [2, 20]]
        if action == "grow"
        else [
            [1, 10],
            [2, 20],
            [3, 30],
            [4, 40],
        ]
    )
    values = np.asarray(initial, dtype=np.int64)
    original = numpy_prefix._compare_integer_column
    fired = False

    def mutate_after_comparison(column: object, spec: object) -> object:
        nonlocal fired
        result = original(column, spec)  # type: ignore[arg-type]
        if fired:
            return result
        fired = True
        if action == "shrink":
            values.resize((2, 2), refcheck=False)
        elif action == "grow":
            values.resize((4, 2), refcheck=False)
            values[2:] = ((3, 30), (4, 40))
        elif action == "reshape":
            values.resize((values.size,), refcheck=False)
        elif action == "width":
            values.resize((2, 4), refcheck=False)
        else:
            values.dtype = np.dtype("uint64")
        return result

    monkeypatch.setattr(numpy_prefix, "_compare_integer_column", mutate_after_comparison)
    query = (
        fpstreams.rows.from_numpy(values, columns=("key", "value"))
        .where(fpstreams.col("key") > 0)
        .select("key", "value")
    )
    if action in {"reshape", "width", "dtype"}:
        message = {
            "reshape": "changed to 1 dimensions",
            "width": "width changed",
            "dtype": "dtype changed from int64 to uint64",
        }[action]
        with warnings.catch_warnings(), pytest.raises(ValueError, match=message):
            warnings.simplefilter("ignore", DeprecationWarning)
            query.to_list()
        return

    assert query.to_list() == [
        {"key": 1, "value": 10},
        {"key": 2, "value": 20},
        *([{"key": 3, "value": 30}, {"key": 4, "value": 40}] if action == "grow" else []),
    ]


def test_numpy_group_consumes_safe_filtered_projection_and_rename_prefixes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Safe schema-only prefixes should feed the existing bounded NumPy group kernel."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution import relational

    values = np.asarray(
        [[2, 4, 1], [1, 7, 0], [2, 3, 1], [1, 5, 1], [3, 9, 1]],
        dtype=np.int64,
    )

    def forbidden_rows(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("a safe NumPy group prefix must not execute Python rows")

    monkeypatch.setattr(relational, "_execute_python_group_values", forbidden_rows)
    source = fpstreams.rows.from_numpy(values, columns=("key", "value", "keep"))
    grouped = (
        source.where(fpstreams.col("keep") == 1)
        .select(bucket="key", amount="value")
        .group_by("bucket")
        .aggregate(
            rows=fpstreams.agg.count(),
            total=fpstreams.agg.sum("amount"),
            low=fpstreams.agg.min("amount"),
            high=fpstreams.agg.max("amount"),
        )
    )
    renamed = (
        source.where(fpstreams.col("keep") == 1)
        .rename(key="bucket", value="amount")
        .group_by("bucket")
        .aggregate(total=fpstreams.agg.sum("amount"))
    )
    conjoined = (
        source.where((fpstreams.col("keep") == 1) & (fpstreams.col("value") >= 4))
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
    )

    expected = [
        {"bucket": 2, "rows": 2, "total": 7, "low": 3, "high": 4},
        {"bucket": 1, "rows": 1, "total": 5, "low": 5, "high": 5},
        {"bucket": 3, "rows": 1, "total": 9, "low": 9, "high": 9},
    ]
    execution = grouped.run_with_report("to_list")
    assert execution.value == expected
    assert execution.report.strategy == "numpy_direct"
    assert grouped.explain("list").to_dict()["relations"]["candidate"] == "numpy_hash"
    assert renamed.to_list() == [
        {"bucket": 2, "total": 7},
        {"bucket": 1, "total": 5},
        {"bucket": 3, "total": 9},
    ]
    conjoined_execution = conjoined.run_with_report("to_list")
    assert conjoined_execution.value == [
        {"key": 2, "total": 4},
        {"key": 1, "total": 5},
        {"key": 3, "total": 9},
    ]
    assert conjoined_execution.report.strategy == "numpy_direct"


@pytest.mark.parametrize("prefixed", [False, True], ids=["direct", "prefix"])
@pytest.mark.parametrize("replacement", ["global", "builtins"])
def test_numpy_group_deopts_before_replaced_hash_is_observed(
    monkeypatch: pytest.MonkeyPatch,
    prefixed: bool,
    replacement: str,
) -> None:
    """Columnar grouping cannot bypass either dynamic lookup tier of canonical hash()."""
    import builtins

    np = pytest.importorskip("numpy")
    from fpstreams.execution import relational

    source = fpstreams.rows.from_numpy(
        np.asarray([[1, 2, 1], [1, 3, 1], [2, 4, 0]], dtype=np.int64),
        columns=("key", "value", "keep"),
    )
    if prefixed:
        source = source.where(fpstreams.col("keep") == 1).select("key", "value")
    query = source.group_by("key").aggregate(
        rows=fpstreams.agg.count(),
        total=fpstreams.agg.sum("value"),
    )
    original_hash = builtins.hash
    calls: list[object] = []

    def observed_hash(value: object) -> int:
        calls.append(value)
        return original_hash(value)

    with monkeypatch.context() as scoped:
        if replacement == "global":
            scoped.setattr(relational, "hash", observed_hash, raising=False)
        else:
            scoped.setattr(builtins, "hash", observed_hash)
        result = query.to_list()

    expected = (
        [{"key": 1, "rows": 2, "total": 5}]
        if prefixed
        else [
            {"key": 1, "rows": 2, "total": 5},
            {"key": 2, "rows": 1, "total": 4},
        ]
    )
    assert calls
    assert result == expected


@pytest.mark.parametrize("prefixed", [False, True], ids=["direct", "prefix"])
def test_only_a_root_numpy_group_updates_non_list_execution_reports(prefixed: bool) -> None:
    """A root relational hit is real evidence; the same group nested under a stage is not."""
    np = pytest.importorskip("numpy")

    source = fpstreams.rows.from_numpy(
        np.asarray([[1, 2, 1], [1, 3, 1], [2, 4, 0]], dtype=np.int64),
        columns=("key", "value", "keep"),
    )
    if prefixed:
        source = source.where(fpstreams.col("keep") == 1).select("key", "value")
    grouped = source.group_by("key").aggregate(total=fpstreams.agg.sum("value"))

    root_execution = grouped.run_with_report("count")
    nested_execution = grouped.select("key").run_with_report("count")

    assert root_execution.value == (1 if prefixed else 2)
    assert root_execution.report.strategy == "numpy_direct"
    assert nested_execution.value == root_execution.value
    assert nested_execution.report.strategy == "planned:python"
