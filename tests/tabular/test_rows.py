"""Rows selection, expressions, field dependencies, and materialization contracts."""

from __future__ import annotations

import gc
import json
import sys
import weakref
from collections.abc import Iterable, Iterator, Mapping
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pytest

import fpstreams
from fpstreams import flow


def test_static_field_dependencies_track_values_separately_from_row_selection():
    from fpstreams import col, rows

    opened = []

    def source():
        opened.append(True)
        yield {"price": 4, "tax": 2, "active": True}

    query = (
        rows(flow.defer(source))
        .with_columns(total=col("price") + col("tax"))
        .rename(total="amount")
        .where(col("active"))
        .select("amount")
    )
    dependency = query.explain().to_dict(include_field_dependencies=True)["field_dependencies"]
    assert opened == []
    assert dependency["fields"] == {
        "amount": [{"source": "source_0", "field": "price"}, {"source": "source_0", "field": "tax"}]
    }
    assert dependency["row_dependencies"] == [{"source": "source_0", "field": "active"}]
    assert dependency["complete_schema"] is True
    assert query.to_list() == [{"amount": 6}]


def test_static_dependencies_use_live_closures_and_mark_opaque_boundaries_unknown():
    from fpstreams import col, rows

    query = rows([{"x": 2}]).with_columns(y=col("x") * 2).select("y")
    assert query.explain().to_dict(include_field_dependencies=True)["field_dependencies"]["fields"][
        "y"
    ] == [{"source": "source_0", "field": "x"}]
    opaque = rows([{"x": 2}]).with_columns(y=lambda record: record["x"]).select("y")
    assert opaque.explain().to_dict(include_field_dependencies=True)["field_dependencies"][
        "fields"
    ] == {"y": None}
    # Replacing a generated stage's executable code must invalidate its descriptor.
    operation = query.to_flow()._pipeline.operations[-1]

    original = operation.function.__code__

    # The project projection has two closure cells, so use a compatible replacement.
    def factory():
        left, right = 1, 2

        def altered(row):
            return {"y": left + right}

        return altered

    try:
        operation.function.__code__ = factory().__code__
        assert (
            query.explain().to_dict(include_field_dependencies=True)["field_dependencies"]["fields"]
            == {}
        )
    finally:
        operation.function.__code__ = original


def test_row_stage_metadata_requires_the_registered_function_and_current_code():
    from types import FunctionType

    from fpstreams.planning.arrow import (
        RowStageDescriptor,
        _register_row_stage,
        _row_stage_descriptor,
    )

    def stage(record):
        return record["x"]

    descriptor = RowStageDescriptor("select", selectors=(("x", "x"),))
    assert _register_row_stage(stage, descriptor) is stage
    assert _row_stage_descriptor(stage) is descriptor
    clone = FunctionType(stage.__code__, stage.__globals__)
    clone.__dict__.update(stage.__dict__)
    assert _row_stage_descriptor(clone) is None

    def replacement(record):
        return None

    original_code = stage.__code__
    try:
        stage.__code__ = replacement.__code__
        assert _row_stage_descriptor(stage) is None
    finally:
        stage.__code__ = original_code
    assert _row_stage_descriptor(stage) is descriptor
    updated = RowStageDescriptor("rename", selectors=(("x", "y"),))
    _register_row_stage(stage, updated)
    assert _row_stage_descriptor(stage) is updated


def test_row_stage_metadata_releases_query_bound_values_with_its_function():
    import gc
    import weakref

    from fpstreams.planning.arrow import RowStageDescriptor, _register_row_stage

    class QueryValue:
        pass

    def register():
        value = QueryValue()

        def stage(record):
            return record

        _register_row_stage(stage, RowStageDescriptor("select", selectors=(("x", value),)))
        return weakref.ref(stage), weakref.ref(value)

    references = [register() for _ in range(256)]
    gc.collect()
    assert all(function() is None and value() is None for function, value in references)


def test_row_stage_metadata_concurrent_registration_reads_and_reclamation():
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    from fpstreams.planning.arrow import (
        RowStageDescriptor,
        _register_row_stage,
        _row_stage_descriptor,
    )

    barrier = Barrier(4, timeout=30)

    def worker(worker_id):
        barrier.wait()
        for index in range(256):

            def stage(record):
                return record

            descriptor = RowStageDescriptor("select", selectors=(("x", (worker_id, index)),))
            _register_row_stage(stage, descriptor)
            assert _row_stage_descriptor(stage) is descriptor
            # Re-registration drops the preceding weak reference while this owner is live.
            updated = RowStageDescriptor("rename", selectors=(("x", "y"),))
            _register_row_stage(stage, updated)
            assert _row_stage_descriptor(stage) is updated
            del stage
        return worker_id

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(worker, index) for index in range(4)]
        assert sorted(future.result(timeout=30) for future in futures) == list(range(4))


def test_static_dependencies_keep_join_source_names_and_group_keys():
    from fpstreams import agg, rows

    left = rows([{"key": 1, "x": 2}]).select("key", "x")
    right = rows([{"key": 1, "x": 3}]).select("key", "x")
    joined = left.join(right, on="key")
    dependencies = joined.explain().to_dict(include_field_dependencies=True)["field_dependencies"]
    assert dependencies["sources"] == ["source_0", "source_1"]
    assert dependencies["fields"]["x"] == [{"source": "source_0", "field": "x"}]
    assert dependencies["fields"]["x_right"] == [{"source": "source_1", "field": "x"}]
    grouped = left.group_by("key").aggregate(total=agg.sum("x"))
    dependency = grouped.explain().to_dict(include_field_dependencies=True)["field_dependencies"]
    assert dependency["fields"]["total"] == [{"source": "source_0", "field": "x"}]
    assert dependency["row_dependencies"] == [{"source": "source_0", "field": "key"}]


def test_static_dependencies_reject_rebound_expression_execution_without_calling_it():
    from fpstreams import col, rows

    predicate = col("flag")
    query = rows([{"flag": True, "value": 1}]).where(predicate).select("value")
    calls = []
    object.__setattr__(predicate, "_evaluate", lambda row: calls.append(row) or True)
    dependency = query.explain().to_dict(include_field_dependencies=True)["field_dependencies"]
    assert calls == []
    assert dependency["fields"] == {"value": None}
    assert dependency["row_dependencies"] is None


def test_static_dependencies_preserve_rename_schema_and_reintroduced_literal_fields():
    from fpstreams import lit, rows

    known = rows([{"x": 2}]).select("x").rename(missing="ghost")
    assert known.explain().to_dict(include_field_dependencies=True)["field_dependencies"][
        "fields"
    ] == {"x": [{"source": "source_0", "field": "x"}]}
    query = rows([{"x": 2}]).rename(x="y").with_columns(x=lit(3)).select("x", "y")
    dependency = query.explain().to_dict(include_field_dependencies=True)["field_dependencies"]
    assert dependency["fields"] == {"x": [], "y": [{"source": "source_0", "field": "x"}]}
    assert query.to_list() == [{"x": 3, "y": 2}]


def test_static_dependencies_visit_shared_expression_nodes_once_and_detach_exports(monkeypatch):
    from fpstreams import RowExpr, col, rows
    from fpstreams.expressions.row_ir import Binary
    from fpstreams.planning import lineage

    expression = col("x")
    for _ in range(30):
        # Bound display labels so this regression isolates traversal of the shared IR.
        expression = RowExpr._from_node(Binary("+", expression._node, expression._node), "shared")
    visits = []
    children = lineage._children

    def counted(node):
        visits.append(id(node))
        return children(node)

    monkeypatch.setattr(lineage, "_children", counted)
    explanation = rows([{"x": 1}]).with_columns(y=expression).explain()
    assert "field_dependencies" not in explanation.to_dict()
    assert visits == []
    metadata = explanation.to_dict(include_field_dependencies=True)["field_dependencies"]
    assert len(visits) == len(set(visits)) == 31
    assert metadata["fields"]["y"] == [{"source": "source_0", "field": "x"}]
    metadata["fields"]["y"].clear()
    assert explanation.to_dict(include_field_dependencies=True)["field_dependencies"]["fields"][
        "y"
    ] == [{"source": "source_0", "field": "x"}]


def test_static_dependencies_reuse_each_columns_analysis_only_within_the_step(monkeypatch):
    from fpstreams import col, rows
    from fpstreams.planning import lineage

    left, right = col("x") + 1, col("y") + 1
    explanation = rows([]).with_columns(a=left, b=right).with_columns(a=left, b=right).explain()
    analyzed = []
    analyze = lineage.analyze_row_node

    def counted(node):
        analyzed.append(id(node))
        return analyze(node)

    monkeypatch.setattr(lineage, "analyze_row_node", counted)
    assert "field_dependencies" not in explanation.to_dict()
    assert analyzed == []
    metadata = explanation.to_dict(include_field_dependencies=True)["field_dependencies"]
    assert metadata["fields"] == {
        "a": [{"source": "source_0", "field": "x"}],
        "b": [{"source": "source_0", "field": "y"}],
    }
    assert analyzed == [id(left._node), id(right._node)] * 2


def test_static_dependencies_stop_at_opaque_columns_before_a_later_invalid_graph():
    from fpstreams import RowExpr, rows
    from fpstreams.expressions.row_ir import InputRow, PythonUDF

    def unread_source():
        raise AssertionError("dependency export must not open the source")

    def uncalled(record):
        raise AssertionError("dependency export must not call a selector")

    opaque = RowExpr._from_node(PythonUDF(uncalled, (InputRow(),)), "opaque")
    invalid = RowExpr._from_node(object(), "unsupported")
    explanation = rows(flow.defer(unread_source)).with_columns(a=opaque, b=invalid).explain()
    metadata = explanation.to_dict(include_field_dependencies=True)["field_dependencies"]
    assert metadata["fields"] == {}
    assert metadata["row_dependencies"] is None
    assert metadata["passthrough_source"] is None
    # Confirm the later graph would fail if the opaque short circuit were lost.
    with pytest.raises(TypeError, match="unsupported row node"):
        rows([]).with_columns(b=invalid).explain().to_dict(include_field_dependencies=True)


@pytest.mark.parametrize("kind", ["input", "index"])
def test_static_dependencies_keep_unknown_whole_row_and_index_columns(kind):
    from fpstreams import RowExpr, col, rows
    from fpstreams.expressions.row_ir import Index, InputRow

    unknown = RowExpr._from_node(InputRow() if kind == "input" else Index(0), "unknown")
    explanation = rows([]).with_columns(unknown=unknown, known=col("x") + 1).explain()
    metadata = explanation.to_dict(include_field_dependencies=True)["field_dependencies"]
    assert metadata["fields"] == {
        "unknown": None,
        "known": [{"source": "source_0", "field": "x"}],
    }
    assert metadata["row_dependencies"] == []


def test_static_dependencies_release_columns_graph_and_source_after_export():
    from fpstreams import col, lit, rows

    class Source:
        def __call__(self):
            raise AssertionError("dependency export must not open the source")

    class Value:
        pass

    source, value = Source(), Value()
    source_ref, value_ref = weakref.ref(source), weakref.ref(value)
    explanation = rows(flow.defer(source)).with_columns(a=lit(value), b=col("x") + 1).explain()
    metadata = explanation.to_dict(include_field_dependencies=True)["field_dependencies"]
    assert metadata["fields"] == {
        "a": [],
        "b": [{"source": "source_0", "field": "x"}],
    }
    del explanation, source, value
    gc.collect()
    assert source_ref() is None
    assert value_ref() is None


@pytest.mark.parametrize("layout", ["list", "large", "fixed", "nested"])
def test_arrow_projection_handles_validated_builtin_lists_and_detaches_results(layout):
    kind = {
        "list": pa.list_(pa.int64()),
        "large": pa.large_list(pa.int64()),
        "fixed": pa.list_(pa.int64(), 2),
        "nested": pa.list_(pa.list_(pa.int64())),
    }[layout]
    payload = [[[1, None]], [[2, 3]]] if layout == "nested" else [[1, None], [2, 3]]
    table = pa.table({"id": [1, 2], "payload": pa.array(payload, type=kind)})
    query = fpstreams.rows.from_arrow(table)
    assert query.select("id").to_list() == [{"id": 1}, {"id": 2}]
    selected = query.select(copy="payload").to_list()
    assert selected == [{"copy": row} for row in payload]
    selected[0]["copy"].append([] if layout == "nested" else 9)
    assert query.select(copy="payload").to_list() == [{"copy": row} for row in payload]


def test_arrow_projection_does_not_hide_invalid_utf8_in_an_unused_list():
    import struct

    invalid = pa.Array.from_buffers(
        pa.string(),
        1,
        [None, pa.py_buffer(struct.pack("<ii", 0, 1)), pa.py_buffer(b"\xff")],
    )
    payload = pa.ListArray.from_arrays(pa.array([0, 1], type=pa.int32()), invalid)
    query = fpstreams.rows.from_arrow(pa.table({"id": [1], "payload": payload}))
    with pytest.raises(UnicodeDecodeError):
        query.select("id").to_list()


def test_arrow_projection_observes_extension_conversion_inside_an_unused_list():
    conversions = []

    class ObservedScalar(pa.ExtensionScalar):
        def as_py(self, **_kwargs):
            value = self.value.as_py()
            conversions.append(value)
            return value

    class ObservedType(pa.ExtensionType):
        def __init__(self):
            super().__init__(pa.int64(), "fpstreams.test.nested-projection")

        def __arrow_ext_serialize__(self):
            return b""

        @classmethod
        def __arrow_ext_deserialize__(cls, _storage, _metadata):
            return cls()

        def __arrow_ext_scalar_class__(self):
            return ObservedScalar

    children = pa.ExtensionArray.from_storage(ObservedType(), pa.array([3, 4]))
    payload = pa.ListArray.from_arrays(pa.array([0, 2], type=pa.int32()), children)
    query = fpstreams.rows.from_arrow(pa.table({"id": [1], "payload": payload}))
    assert query.select("id").to_list() == [{"id": 1}]
    assert conversions == [3, 4]


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
    from fpstreams.planning.arrow import source as arrow_planning
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


@pytest.mark.parametrize("raises", [False, True])
def test_select_snapshot_retains_replaced_sibling_until_row_finishes(raises: bool) -> None:
    """Selector-list edits affect the next row without releasing a current sibling early."""
    from fpstreams.tabular.rows import _build_select_project

    events = []
    failure = RuntimeError("original selector failure")

    class Sibling:
        def __call__(self, row):
            events.append("original sibling")
            if raises:
                raise failure
            return row["right"]

        def __del__(self):
            events.append("released sibling")

    aliases = [("right", Sibling())]

    def replace_sibling(row):
        events.append("replace sibling")
        aliases[:] = [("replacement", lambda item: item["new"])]
        return row["left"]

    project = _build_select_project([("left", replace_sibling)], aliases)
    record = {"left": 1, "right": 2, "new": 3}
    if raises:
        with pytest.raises(RuntimeError) as caught:
            project(record)
        assert caught.value is failure
        # The exception traceback owns the active selector until it is cleared.
        failure.__traceback__ = None
        del caught
    else:
        assert project(record) == {"left": 1, "right": 2}
    assert events == ["replace sibling", "original sibling", "released sibling"]
    assert project(record) == {"left": 1, "replacement": 3}


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


@pytest.mark.parametrize("engine", ["python", "auto"])
def test_rows_with_columns_keeps_live_sibling_list_and_original_row(monkeypatch, engine):
    """Enrichment copies first, then reads the live selector list against the input row."""
    rows_module = sys.modules["fpstreams.tabular.rows"]
    original_build = rows_module._CANONICAL_BUILD_WITH_COLUMNS_ENRICHER
    captured = []

    def capture(selectors):
        captured.append(selectors)
        return original_build(selectors)

    monkeypatch.setattr(rows_module, "_CANONICAL_BUILD_WITH_COLUMNS_ENRICHER", capture)
    query = fpstreams.rows([{"value": 2, "replacement": 30} for _ in range(600)])
    query = query.with_engine(engine).with_columns(copy="value", second="replacement")
    selectors = captured[0]
    changed = []

    def first(row):
        row["replacement"] = 99
        if not changed:
            changed.append(True)
            selectors[1] = ("second", lambda original: original["replacement"] + 10)
            selectors.append(("third", lambda original: original["value"] + 20))
        return row["value"]

    def replacement(selector):
        def invoke(row):
            return selector(row)

        return invoke

    function = selectors[0][1]
    function.__code__ = replacement(first).__code__
    function.__closure__[0].cell_contents = first
    assert query.to_list() == [
        {"value": 2, "replacement": 30, "copy": 2, "second": 109, "third": 22} for _ in range(600)
    ]


@pytest.mark.parametrize("engine", ["python", "auto"])
@pytest.mark.parametrize("source_kind", ["dict", "numpy"])
@pytest.mark.parametrize("boundary", ["before_execution", "source_open"])
@pytest.mark.parametrize("terminal", ["to_list", "to_columns"])
def test_rows_with_columns_uses_live_expression_evaluator(
    monkeypatch, engine, source_kind, boundary, terminal
):
    """Computed-column terminals recheck evaluators after claiming their source."""
    from fpstreams.planning.source import Source

    size = 600
    calls = []
    opened = []
    expression = fpstreams.col("value") + 1
    assert expression({"value": 2}) == 3
    if source_kind == "numpy":
        np = pytest.importorskip("numpy")
        source = fpstreams.rows.from_numpy(
            np.asarray([[2, 30]] * size), columns=["value", "replacement"]
        )
    else:
        source = fpstreams.rows([{"value": 2, "replacement": 30} for _ in range(size)])
    query = source.with_engine(engine).with_columns(score=expression)

    def evaluate(row):
        calls.append(row["value"])
        return row["replacement"] + 1

    if boundary == "before_execution":
        object.__setattr__(expression, "_evaluate", evaluate)
    else:
        name = "open_native" if source_kind == "numpy" and engine == "auto" else "open"
        original = getattr(Source, name)

        def changed_open(owner, *args, **kwargs):
            opened.append(True)
            object.__setattr__(expression, "_evaluate", evaluate)
            return original(owner, *args, **kwargs)

        monkeypatch.setattr(Source, name, changed_open)
    result = getattr(query, terminal)()
    if terminal == "to_list":
        assert result == [{"value": 2, "replacement": 30, "score": 31} for _ in range(size)]
    else:
        assert result == {"value": [2] * size, "replacement": [30] * size, "score": [31] * size}
    assert calls == [2] * size
    if boundary == "source_open":
        assert opened == [True]


@pytest.mark.parametrize("engine", ["python", "auto"])
@pytest.mark.parametrize("boundary", [0, 512, 550])
@pytest.mark.parametrize("raises", [False, True])
def test_rows_with_columns_observes_accessor_changes_during_consumption(
    monkeypatch, engine, boundary, raises
):
    """A live accessor controls later rows, errors, and the number of source pulls."""
    rows_module = sys.modules["fpstreams.tabular.rows"]
    original_compile = rows_module.compile_selector
    captured = []
    pulled = []
    calls = []
    failure = RuntimeError("changed accessor")

    def capture(selector):
        function = original_compile(selector)
        captured.append(function)
        return function

    def replacement(selector):
        def changed(row):
            return selector(row)

        return changed

    def live(row):
        calls.append(row["index"])
        if raises:
            raise failure
        return row["replacement"]

    def source():
        for index in range(600):
            if index == boundary:
                function = captured[0]
                function.__code__ = replacement(live).__code__
                function.__closure__[0].cell_contents = live
            pulled.append(index)
            yield {"index": index, "value": 2, "replacement": 30}

    monkeypatch.setattr(rows_module, "compile_selector", capture)
    query = fpstreams.rows(source()).with_engine(engine).with_columns(copy="value")
    if raises:
        with pytest.raises(RuntimeError) as caught:
            query.to_list()
        assert caught.value is failure
        assert pulled == list(range(boundary + 1))
        assert calls == [boundary]
    else:
        result = query.to_list()
        assert [row["copy"] for row in result] == [2] * boundary + [30] * (600 - boundary)
        assert pulled == list(range(600))
        assert calls == list(range(boundary, 600))


@pytest.mark.parametrize("engine", ["python", "auto"])
@pytest.mark.parametrize("source_kind", ["dict", "numpy", "arrow"])
@pytest.mark.parametrize("mutation", ["closure", "code"])
@pytest.mark.parametrize("compiled", [False, True])
def test_rows_with_columns_reads_captured_accessor_bindings(
    monkeypatch, engine, source_kind, mutation, compiled
):
    """Computed columns use the current accessor even through a retained plan."""
    rows_module = sys.modules["fpstreams.tabular.rows"]
    original_compile = rows_module.compile_selector
    captured = []

    def capture(selector):
        function = original_compile(selector)
        captured.append(function)
        return function

    size = 600
    if source_kind == "numpy":
        np = pytest.importorskip("numpy")
        source = fpstreams.rows.from_numpy(
            np.asarray([[2, 30]] * size), columns=["value", "replacement"]
        )
    elif source_kind == "arrow":
        source = fpstreams.rows.from_columns(
            {"value": [2] * size, "replacement": [30] * size}, batch_size=7
        )
    else:
        source = fpstreams.rows([{"value": 2, "replacement": 30} for _ in range(size)])
    monkeypatch.setattr(rows_module, "compile_selector", capture)
    query = source.with_engine(engine).with_columns(copy="value")
    if compiled:
        context = query._flow._terminal_context("list")
        owner_type = type(query._flow)
        original_context = owner_type._terminal_context

        def retained_context(owner, terminal):
            if owner is query._flow and terminal == "list":
                return context
            return original_context(owner, terminal)

        monkeypatch.setattr(owner_type, "_terminal_context", retained_context)
    function = captured[0]
    if mutation == "closure":
        function.__closure__[0].cell_contents = "replacement"
    else:

        def replacement(selector):
            def changed(row):
                assert selector == "value"
                return row["replacement"]

            return changed

        function.__code__ = replacement("value").__code__
    assert query.to_list() == [{"value": 2, "replacement": 30, "copy": 30} for _ in range(size)]


@pytest.mark.parametrize("engine", ["python", "auto"])
@pytest.mark.parametrize("source_kind", ["dict", "numpy", "arrow"])
@pytest.mark.parametrize("mutation", ["closure", "code"])
@pytest.mark.parametrize("compiled", [False, True])
def test_rows_select_revalidates_captured_accessors(
    monkeypatch: pytest.MonkeyPatch,
    engine: str,
    source_kind: str,
    mutation: str,
    compiled: bool,
) -> None:
    """Terminal shortcuts and retained physical plans read the live selected field."""
    rows_module = sys.modules["fpstreams.tabular.rows"]
    original_compile = rows_module.compile_selector
    captured = {}

    def capture(selector):
        function = original_compile(selector)
        captured[selector] = function
        return function

    size = 2048 if source_kind == "dict" else 16
    if source_kind == "numpy":
        np = pytest.importorskip("numpy")
        source = fpstreams.rows.from_numpy(
            np.asarray([[2, 30]] * size), columns=["value", "replacement"]
        )
    elif source_kind == "arrow":
        pytest.importorskip("pyarrow")
        source = fpstreams.rows.from_columns({"value": [2] * size, "replacement": [30] * size})
    else:
        source = fpstreams.rows([{"value": 2, "replacement": 30} for _ in range(size)])
    monkeypatch.setattr(rows_module, "compile_selector", capture)
    query = source.with_engine(engine).select(value="value")
    if compiled:
        context = query._flow._terminal_context("list")
        owner_type = type(query._flow)
        original_context = owner_type._terminal_context

        def retained_context(owner, terminal):
            if owner is query._flow and terminal == "list":
                return context
            return original_context(owner, terminal)

        monkeypatch.setattr(owner_type, "_terminal_context", retained_context)

    function = captured["value"]
    if mutation == "closure":
        cells = dict(zip(function.__code__.co_freevars, function.__closure__, strict=True))
        cells["selector"].cell_contents = "replacement"
    else:
        selector = "value"

        def replacement(row):
            return row["replacement"] if selector else None

        function.__code__ = replacement.__code__

    assert query.to_list() == [{"value": 30}] * size


@pytest.mark.parametrize("engine", ["python", "auto"])
@pytest.mark.parametrize("change_at", [0, 1024])
def test_rows_select_keeps_accessor_changes_during_source_reads(
    monkeypatch: pytest.MonkeyPatch, engine: str, change_at: int
) -> None:
    """A one-shot source can change an accessor before or after the generated loop starts."""
    rows_module = sys.modules["fpstreams.tabular.rows"]
    original_compile = rows_module.compile_selector
    captured = {}
    pulls = []
    closed = []

    def capture(selector):
        function = original_compile(selector)
        captured[selector] = function
        return function

    def source():
        try:
            for index in range(2048):
                if index == change_at:
                    function = captured["value"]
                    cells = dict(
                        zip(function.__code__.co_freevars, function.__closure__, strict=True)
                    )
                    cells["selector"].cell_contents = "replacement"
                pulls.append(index)
                yield {"value": 2, "replacement": 30}
        finally:
            closed.append(True)

    monkeypatch.setattr(rows_module, "compile_selector", capture)
    query = fpstreams.rows(source()).with_engine(engine).select(value="value")
    assert query.to_list() == [{"value": 2}] * change_at + [{"value": 30}] * (2048 - change_at)
    assert pulls == list(range(2048))
    assert closed == [True]


@pytest.mark.parametrize("terminal", ["to_list", "to_columns"])
def test_numpy_select_revalidates_after_native_source_open(
    monkeypatch: pytest.MonkeyPatch, terminal: str
) -> None:
    """A native source-open callback cannot leave a cached projection reading an old field."""
    np = pytest.importorskip("numpy")
    from fpstreams.planning.source import Source

    rows_module = sys.modules["fpstreams.tabular.rows"]
    original_compile = rows_module.compile_selector
    captured = {}
    opened = []

    def capture(selector):
        function = original_compile(selector)
        captured[selector] = function
        return function

    monkeypatch.setattr(rows_module, "compile_selector", capture)
    query = fpstreams.rows.from_numpy(
        np.asarray([[2, 30]] * 16), columns=["value", "replacement"]
    ).select(value="value")
    target = query._flow._terminal_context("list")[1].source
    original_open = Source.open_native

    def changing_open(source, expected_type):
        result = original_open(source, expected_type)
        if source is target:
            opened.append(True)
            function = captured["value"]
            function.__closure__[0].cell_contents = "replacement"
        return result

    monkeypatch.setattr(Source, "open_native", changing_open)
    expected = [{"value": 30}] * 16 if terminal == "to_list" else {"value": [30] * 16}
    assert getattr(query, terminal)() == expected
    assert opened == [True]


@pytest.mark.parametrize("terminal", ["to_list", "to_arrow", "arrow_batches", "count"])
@pytest.mark.parametrize("change_at", [0, 1])
@pytest.mark.parametrize("raises", [False, True])
@pytest.mark.parametrize("reiterable", [False, True])
def test_arrow_select_keeps_live_accessors_and_batch_fields(
    monkeypatch: pytest.MonkeyPatch, terminal: str, change_at: int, raises: bool, reiterable: bool
) -> None:
    """Batch callbacks preserve unprojected fields for fallback without reopening the stream."""
    pa = pytest.importorskip("pyarrow")
    from fpstreams.planning.arrow.source import ArrowBatchSource
    from fpstreams.planning.source import Source, SourceCapabilities

    rows_module = sys.modules["fpstreams.tabular.rows"]
    original_compile = rows_module.compile_selector
    captured = {}
    selected = []
    opened = []
    closed = []
    failure = RuntimeError("changed projection accessor")
    selector = {"selected": selected, "raises": raises, "failure": failure}
    batches = [
        pa.record_batch({"value": [2], "replacement": [30]}),
        pa.record_batch({"value": [3], "replacement": [40]}),
    ]

    def capture(field):
        function = original_compile(field)
        captured[field] = function
        return function

    def replacement(row):
        value = row["replacement"]
        selector["selected"].append(value)
        if selector["raises"]:
            raise selector["failure"]
        return value

    def open_batches(columns=None):
        opened.append(columns)
        try:
            for index, batch in enumerate(batches):
                if index == change_at:
                    function = captured["value"]
                    function.__code__ = replacement.__code__
                    function.__closure__[0].cell_contents = selector
                yield batch if columns is None else batch.select(list(columns))
        finally:
            closed.append(True)

    descriptor = ArrowBatchSource(
        open_batches,
        "reader",
        1,
        batches[0].schema,
        reiterable,
        projection_opener=lambda columns: open_batches(columns),
    )

    def row_source():
        yield from (row for batch in open_batches() for row in batch.to_pylist())

    source = Source(
        row_source,
        SourceCapabilities(reiterable=reiterable, exact_size=None),
        native_data=descriptor,
    )
    monkeypatch.setattr(rows_module, "compile_selector", capture)
    query = fpstreams.Rows(fpstreams.Flow(source)).select(value="value")

    def run():
        if terminal == "to_arrow":
            return query.to_arrow(batch_size=1).to_pylist()
        if terminal == "arrow_batches":
            result = query.arrow_batches(batch_size=1).to_list()
            return pa.Table.from_batches(result).to_pylist()
        return getattr(query, terminal)()

    if raises:
        with pytest.raises(RuntimeError) as raised:
            run()
        assert raised.value is failure
        assert selected == [30 if change_at == 0 else 40]
    else:
        result = run()
        assert result == (
            2 if terminal == "count" else [{"value": 30 if change_at == 0 else 2}, {"value": 40}]
        )
        assert selected == ([30, 40] if change_at == 0 else [40])
    assert len(opened) == 1
    assert closed == [True]
    if not reiterable:
        with pytest.raises(fpstreams.FlowConsumedError):
            query.to_list()
        assert len(opened) == 1


@pytest.mark.parametrize(
    "terminal", ["to_list", "to_arrow", "arrow_batches", "count", "preserved_table"]
)
def test_arrow_select_rechecks_projection_after_source_claim(
    monkeypatch: pytest.MonkeyPatch, terminal: str
) -> None:
    """A source claim can change a projected field and its dtype before a scan starts."""
    pa = pytest.importorskip("pyarrow")
    from fpstreams.planning.arrow.source import ArrowBatchSource
    from fpstreams.planning.source import Source, SourceCapabilities

    rows_module = sys.modules["fpstreams.tabular.rows"]
    compile_selector = rows_module.compile_selector
    captured = {}
    opened = []
    closed = []
    batch = pa.record_batch({"value": [2], "replacement": ["changed"]})

    def capture(field):
        accessor = compile_selector(field)
        captured[field] = accessor
        return accessor

    def batches(columns=None):
        opened.append(columns)
        try:
            yield batch if columns is None else batch.select(list(columns))
        finally:
            closed.append(True)

    descriptor = ArrowBatchSource(
        batches,
        "reader",
        1,
        batch.schema,
        False,
        projection_opener=batches,
        projection_safe=True,
    )
    source = Source(
        lambda: iter(batch.to_pylist()),
        SourceCapabilities(reiterable=False, exact_size=None),
        native_data=descriptor,
    )
    monkeypatch.setattr(rows_module, "compile_selector", capture)
    query = fpstreams.Rows(fpstreams.Flow(source)).select(value="value")
    native_open = Source.open_native
    claims = []

    def changing_open(self, expected):
        result = native_open(self, expected)
        if self is source:
            claims.append(True)
            captured["value"].__closure__[0].cell_contents = "replacement"
        return result

    monkeypatch.setattr(Source, "open_native", changing_open)
    if terminal == "preserved_table":
        from fpstreams.execution.arrow import try_arrow_table

        handled, table = try_arrow_table(
            query._flow._terminal_context("list")[1], batch_size=1, preserve_source_schema=True
        )
        assert handled
        result = table.to_pylist()
    elif terminal == "arrow_batches":
        result = pa.Table.from_batches(query.arrow_batches(batch_size=1).to_list()).to_pylist()
    elif terminal == "to_arrow":
        result = query.to_arrow(batch_size=1).to_pylist()
    else:
        result = getattr(query, terminal)()
    assert result == (1 if terminal == "count" else [{"value": "changed"}])
    assert opened == [None]
    assert claims == closed == [True]


@pytest.mark.parametrize("hook", ["csv_reader", "csv_stream", "parquet_dataset", "parquet_code"])
@pytest.mark.parametrize("terminal", ["to_list", "to_arrow", "arrow_batches", "count", "first"])
@pytest.mark.parametrize("raises", [False, True])
@pytest.mark.parametrize("before_query", [False, True])
def test_arrow_projection_preserves_replaced_library_openers(  # noqa: C901 - opener/terminal matrix
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    hook: str,
    terminal: str,
    raises: bool,
    before_query: bool,
    public_filter: bool = False,
) -> None:
    """A changed library opener keeps selector callbacks and their errors observable."""
    pa = pytest.importorskip("pyarrow")
    import pyarrow.csv as csv
    import pyarrow.dataset as dataset
    import pyarrow.parquet as parquet

    rows_module = sys.modules["fpstreams.tabular.rows"]
    compile_selector = rows_module.compile_selector
    captured = {}
    selected = []
    calls = []
    failure = RuntimeError("changed scan selector")
    selector = {"selected": selected, "failure": failure, "raises": raises}

    def capture(field):
        accessor = compile_selector(field)
        captured[field] = accessor
        return accessor

    def replacement(row):
        value = row["replacement"]
        selector["selected"].append(value)
        if selector["raises"]:
            raise selector["failure"]
        return value

    monkeypatch.setattr(rows_module, "compile_selector", capture)
    if hook.startswith("parquet"):
        path = tmp_path / "input.parquet"
        parquet.write_table(pa.table({"value": [2], "replacement": [30]}), path)

        def make_query():
            predicate = dataset.field("value") == 2 if public_filter else None
            return fpstreams.Rows.from_parquet(path, filter=predicate).select(value="value")

        module, name = dataset, "dataset"
    else:
        path = tmp_path / "input.csv"
        path.write_text("value,replacement\n2,30\n")

        def make_query():
            return fpstreams.Rows.scan_csv(path).select(value="value")

        module, name = (csv, "open_csv") if hook == "csv_reader" else (pa, "input_stream")
    query = None if before_query else make_query()
    original = getattr(module, name)
    if hook == "parquet_code":
        from types import FunctionType

        live_function = original
        original = FunctionType(
            live_function.__code__,
            live_function.__globals__,
            live_function.__name__,
            live_function.__defaults__,
            live_function.__closure__,
        )
        original.__kwdefaults__ = live_function.__kwdefaults__

    def changed(*args, **kwargs):
        result = original(*args, **kwargs)
        calls.append(True)
        captured["value"].__code__ = replacement.__code__
        captured["value"].__closure__[0].cell_contents = selector
        return result

    if hook == "parquet_code":

        def changed_code(*args, **kwargs):
            return _fpstreams_test_scan_changed_call(*args, **kwargs)  # noqa: F821

        monkeypatch.setitem(live_function.__globals__, "_fpstreams_test_scan_changed_call", changed)
        monkeypatch.setattr(live_function, "__code__", changed_code.__code__)
    else:
        monkeypatch.setattr(module, name, changed)

    if before_query:
        query = make_query()

    def run():
        if terminal == "to_arrow":
            return query.to_arrow().to_pylist()
        if terminal == "arrow_batches":
            return pa.Table.from_batches(query.arrow_batches().to_list()).to_pylist()
        return getattr(query, terminal)()

    if raises:
        with pytest.raises(RuntimeError) as caught:
            run()
        assert caught.value is failure
    else:
        assert run() == {"count": 1, "first": {"value": 30}}.get(terminal, [{"value": 30}])
    assert selected == [30]
    assert calls == [True]


@pytest.mark.parametrize("terminal", ["to_list", "to_arrow", "arrow_batches", "count", "first"])
def test_changed_parquet_opener_preserves_explicit_source_filter(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, terminal: str
) -> None:
    test_arrow_projection_preserves_replaced_library_openers(
        monkeypatch, tmp_path, "parquet_dataset", terminal, False, True, public_filter=True
    )


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


@pytest.mark.parametrize("how", ["any", "all"])
def test_drop_nulls_keeps_explicit_selector_errors_and_field_type_errors(how: str) -> None:
    """A selector failure is not evidence that a selected field is missing."""
    from fpstreams import SelectionError

    explicit = SelectionError("selector failed")
    explicit.__cause__ = KeyError("not a missing field")

    def select(_row: object) -> object:
        raise explicit

    with pytest.raises(SelectionError) as captured:
        fpstreams.rows([{"other": 1}]).drop_nulls(select, "other", how=cast(Any, how)).to_list()
    assert captured.value is explicit
    assert isinstance(captured.value.__cause__, KeyError)

    stopped = StopIteration("selector stopped")

    def stop(_row: object) -> object:
        raise stopped

    for policy in ("legacy", "raise"):
        with pytest.raises(RuntimeError, match="generator raised StopIteration") as captured:
            fpstreams.rows([{}]).drop_nulls(
                stop, how=cast(Any, how), missing=cast(Any, policy)
            ).to_list()
        assert captured.value.__cause__ is stopped

    failure = TypeError("property failed")
    calls: list[str] = []

    class Broken:
        other = 1

        @property
        def value(self) -> object:
            calls.append("value")
            raise failure

    with pytest.raises(SelectionError) as captured:
        fpstreams.rows([Broken()]).drop_nulls("value", "other", how=cast(Any, how)).to_list()
    assert captured.value.__cause__ is failure
    assert calls == ["value"]

    with pytest.raises(SelectionError) as captured:
        fpstreams.rows([{"nested": Broken(), "other": 1}]).drop_nulls(
            "nested.value", "other", how=cast(Any, how)
        ).to_list()
    assert captured.value.__cause__ is failure
    assert calls == ["value", "value"]

    class Missing:
        other = 1

    missing = Missing()
    assert fpstreams.rows([missing]).drop_nulls("value", "other", how=cast(Any, how)).to_list() == (
        [] if how == "any" else [missing]
    )
    records = [{"value": 1, "other": 2}, {"value": None, "other": 2}, {}]
    expected = records[:1] if how == "any" else records[:2]
    assert (
        fpstreams.rows(records).drop_nulls("value", "other", how=cast(Any, how)).to_list()
        == expected
    )


@pytest.mark.parametrize("how", ["any", "all"])
def test_drop_nulls_rowexpr_and_string_subclass_keep_missing_and_failure_boundaries(
    how: str,
) -> None:
    class Field(str):
        pass

    failure = TypeError("property failed")
    calls = []

    class Broken:
        @property
        def value(self):
            calls.append(True)
            raise failure

    for selector in (fpstreams.col("value"), Field("value")):
        records = [{}, {"value": None}, {"value": 1}]
        assert fpstreams.rows(records).drop_nulls(selector, how=cast(Any, how)).to_list() == [
            records[-1]
        ]
        with pytest.raises(fpstreams.SelectionError) as caught:
            fpstreams.rows([Broken()]).drop_nulls(selector, how=cast(Any, how)).to_list()
        assert caught.value.__cause__ is failure
        multi = [{"other": 1}, {"value": None, "other": None}, {"value": 2, "other": 1}]
        expected = multi[-1:] if how == "any" else [multi[0], multi[2]]
        assert (
            fpstreams.rows(multi).drop_nulls(selector, "other", how=cast(Any, how)).to_list()
            == expected
        )
    assert calls == [True, True]

    nested = fpstreams.col("nested.value")
    assert (
        fpstreams.rows([{}, {"nested": {}}, {"nested": {"value": None}}])
        .drop_nulls(nested, how=cast(Any, how))
        .to_list()
        == []
    )
    with pytest.raises(fpstreams.SelectionError) as caught:
        fpstreams.rows([{"nested": Broken()}]).drop_nulls(nested, how=cast(Any, how)).to_list()
    assert caught.value.__cause__ is failure
    assert calls == [True, True, True]

    composite = fpstreams.col("value") + 1
    assert fpstreams.rows([{}, {"value": 2}]).drop_nulls(
        composite, how=cast(Any, how)
    ).to_list() == [{"value": 2}]
    with pytest.raises(fpstreams.SelectionError) as caught:
        fpstreams.rows([Broken()]).drop_nulls(composite, how=cast(Any, how)).to_list()
    assert caught.value.__cause__ is failure
    assert calls == [True, True, True, True]

    with pytest.raises(fpstreams.SelectionError) as caught:
        fpstreams.rows([Broken()]).drop_nulls(
            fpstreams.col("value").map(lambda value: value), how=cast(Any, how)
        ).to_list()
    assert caught.value.__cause__ is failure
    assert calls == [True] * 5

    nested_errors = []

    class Nested:
        @property
        def value(self):
            try:
                return fpstreams.col("value")(Broken())
            except fpstreams.SelectionError as error:
                nested_errors.append(error)
                raise

    with pytest.raises(fpstreams.SelectionError) as caught:
        fpstreams.rows([Nested()]).drop_nulls(fpstreams.col("value"), how=cast(Any, how)).to_list()
    assert caught.value is nested_errors[0]
    assert caught.value.__cause__ is failure
    assert calls == [True] * 6


@pytest.mark.parametrize("how", ["any", "all"])
def test_drop_nulls_preserves_user_rowexpr_failures_and_tracking(how: str) -> None:
    import asyncio

    explicit = fpstreams.SelectionError("user expression")
    explicit.__cause__ = KeyError("not a missing expression field")

    def fail(_value):
        raise explicit

    expression = fpstreams.col("value").map(fail)
    with pytest.raises(fpstreams.SelectionError) as caught:
        fpstreams.rows([{"value": 1}]).drop_nulls(expression, how=cast(Any, how)).to_list()
    assert caught.value is explicit
    assert vars(explicit) == {}

    class GetterFailure:
        @property
        def value(self):
            raise explicit

    for selector, row in (
        ("value", GetterFailure()),
        (fpstreams.col("value"), GetterFailure()),
        ("nested.value", {"nested": GetterFailure()}),
        (fpstreams.col("nested.value"), {"nested": GetterFailure()}),
    ):
        with pytest.raises(fpstreams.SelectionError) as caught:
            fpstreams.rows([row]).drop_nulls(selector, how=cast(Any, how)).to_list()
        assert caught.value is explicit
        assert vars(explicit) == {}

    class CustomRowExpr(fpstreams.RowExpr):
        def __call__(self, row):
            raise explicit

    overridden = CustomRowExpr(lambda row: row, "custom")
    with pytest.raises(fpstreams.SelectionError) as caught:
        fpstreams.rows([{}]).drop_nulls(overridden).to_list()
    assert caught.value is explicit

    class InheritedRowExpr(fpstreams.RowExpr):
        pass

    inherited = InheritedRowExpr(lambda _row: fpstreams.col("missing")({}), "inherited")
    with pytest.raises(fpstreams.SelectionError):
        fpstreams.rows([{}]).drop_nulls(inherited).to_list()
    inherited_field = InheritedRowExpr._from_node(fpstreams.col("missing")._node, "missing")
    assert fpstreams.rows([{}]).drop_nulls(inherited_field).to_list() == []
    nested_selector = fpstreams.col("value").map(lambda _value: fpstreams.col("missing")({}))
    with pytest.raises(fpstreams.SelectionError):
        fpstreams.rows([{"value": 1}]).drop_nulls(nested_selector, how=cast(Any, how)).to_list()

    # Callback failures do not reclassify later ordinary missing fields, even
    # when the same exception object is reused in nested evaluations.
    for _ in range(2):
        with pytest.raises(fpstreams.SelectionError) as caught:
            fpstreams.rows([{"value": 1}]).drop_nulls(expression, how=cast(Any, how)).to_list()
        assert caught.value is explicit
        assert fpstreams.rows([{}]).drop_nulls(fpstreams.col("missing")).to_list() == []

    def nested(_value):
        return fpstreams.rows([{"value": 1}]).drop_nulls(expression).to_list()

    with pytest.raises(fpstreams.SelectionError) as caught:
        fpstreams.rows([{"value": 1}]).drop_nulls(fpstreams.col("value").map(nested)).to_list()
    assert caught.value is explicit
    assert vars(explicit) == {}

    direct = fpstreams.col("value")
    object.__setattr__(direct, "_evaluate", fail)
    with pytest.raises(fpstreams.SelectionError) as caught:
        fpstreams.rows([{"value": 1}]).drop_nulls(direct).to_list()
    assert caught.value is explicit

    patched = fpstreams.col("value")
    assert patched({"value": 1}) == 1  # Compile the normal evaluator first.
    patched._evaluate._program = fail
    with pytest.raises(fpstreams.SelectionError) as caught:
        fpstreams.rows([{"value": 1}]).drop_nulls(patched).to_list()
    assert caught.value is explicit

    changed_code = fpstreams.col("value")
    assert changed_code({"value": 1}) == 1
    compiled = changed_code._evaluate._program.expression

    def raise_from_globals(_row):
        raise _drop_nulls_test_error  # noqa: F821

    original_code = compiled.__code__
    compiled.__globals__["_drop_nulls_test_error"] = explicit
    try:
        compiled.__code__ = raise_from_globals.__code__
        with pytest.raises(fpstreams.SelectionError) as caught:
            fpstreams.rows([{"value": 1}]).drop_nulls(changed_code).to_list()
        assert caught.value is explicit
    finally:
        compiled.__code__ = original_code
        del compiled.__globals__["_drop_nulls_test_error"]

    with pytest.raises(fpstreams.SelectionError) as caught:
        expression({"value": 1})
    assert caught.value is explicit
    assert vars(explicit) == {}

    class ImmutableError(fpstreams.SelectionError):
        def __setattr__(self, name, value):
            if name not in {"__traceback__", "__context__", "__cause__", "__suppress_context__"}:
                raise AssertionError("the user's exception must not be mutated")
            super().__setattr__(name, value)

    immutable = ImmutableError("user failure")

    def fail_immutable(_value):
        raise immutable

    with pytest.raises(ImmutableError) as caught:
        fpstreams.rows([{"value": 1}]).drop_nulls(
            fpstreams.col("value").map(fail_immutable)
        ).to_list()
    assert caught.value is immutable

    from fpstreams.errors import _NO_USER_SELECTION_TRACKING, _user_selection_error

    def cancel(_value):
        raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        fpstreams.rows([{"value": 1}]).drop_nulls(fpstreams.col("value").map(cancel)).to_list()
    assert _user_selection_error.get() is _NO_USER_SELECTION_TRACKING

    outer = object()
    token = _user_selection_error.set(outer)
    try:
        with pytest.raises(asyncio.CancelledError):
            fpstreams.rows([{"value": 1}]).drop_nulls(fpstreams.col("value").map(cancel)).to_list()
        assert _user_selection_error.get() is outer
    finally:
        _user_selection_error.reset(token)


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


@pytest.mark.parametrize(
    ("selectors", "missing"),
    [
        ((), "legacy"),
        ((), "raise"),
        (("nullable",), "raise"),
        ((lambda row: row["nullable"],), "legacy"),
        ((lambda row: row["nullable"],), "raise"),
    ],
)
def test_drop_nulls_whole_record_rechecks_any_after_source_open(
    monkeypatch: pytest.MonkeyPatch,
    selectors: tuple,
    missing: str,
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

    assert fpstreams.Flow(source).rows().drop_nulls(*selectors, missing=missing).to_list() == [
        {"nullable": None}
    ]
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


@pytest.mark.parametrize("owner", ["rows", "flow"])
@pytest.mark.parametrize("conversion_fails", [False, True])
@pytest.mark.parametrize("close_fails", [False, True])
def test_custom_rows_to_columns_closes_owned_cursor(
    tmp_path: Path, owner: str, conversion_fails: bool, close_fails: bool
) -> None:
    """Custom iteration owns its cursor through conversion and cleanup failures."""
    primary = ValueError("record conversion failed")
    cleanup = OSError("column cursor close failed")
    path = tmp_path / "records.jsonl"
    path.write_text('{"b": 1}\n{"a": 2}\n', encoding="utf-8")
    iter_calls = 0

    class BrokenRecord:
        def _asdict(self) -> dict[str, Any]:
            raise primary

    class Cursor(Iterator[Any]):
        def __init__(self) -> None:
            self.handle = path.open(encoding="utf-8")
            self.close_calls = 0
            self.pulls = 0

        def __next__(self) -> Any:
            self.pulls += 1
            line = next(self.handle)
            return BrokenRecord() if conversion_fails else json.loads(line)

        def close(self) -> None:
            self.close_calls += 1
            self.handle.close()
            if close_fails:
                raise cleanup

    cursor = Cursor()

    class CustomRows(fpstreams.Rows[Any]):
        def __iter__(self) -> Iterator[Any]:
            nonlocal iter_calls
            iter_calls += 1
            return cursor

    class CustomFlow(fpstreams.Flow[Any]):
        def __iter__(self) -> Iterator[Any]:
            nonlocal iter_calls
            iter_calls += 1
            return cursor

    source = (
        CustomRows([{"ignored": 0}])
        if owner == "rows"
        else fpstreams.Rows(CustomFlow([{"ignored": 0}]))
    )
    try:
        if conversion_fails or close_fails:
            with pytest.raises(ValueError if conversion_fails else OSError) as captured:
                source.to_columns()
            assert captured.value is (primary if conversion_fails else cleanup)
            if conversion_fails and close_fails:
                assert getattr(captured.value, "__notes__", ()) == [
                    "cleanup failed with OSError: column cursor close failed"
                ]
            else:
                assert not getattr(captured.value, "__notes__", ())
        else:
            result = source.to_columns()
            assert list(result) == ["b", "a"]
            assert result == {"b": [1, None], "a": [None, 2]}
        assert cursor.close_calls == 1
        assert cursor.handle.closed
        assert iter_calls == 1
        assert cursor.pulls == (1 if conversion_fails else 3)
    finally:
        cursor.handle.close()


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


def test_rows_run_with_report_keeps_the_rows_terminal_surface() -> None:
    source = fpstreams.rows([{"value": 1}, {"value": 2}])

    execution = source.select("value").run_with_report("to_list")

    assert execution.value == [{"value": 1}, {"value": 2}]
    assert execution.report.terminal == "to_list"
    assert execution.report.strategy == "planned:python"
    assert execution.report.events[0].compiler_engine == "python"
    assert execution.report.events_truncated is False


@pytest.mark.parametrize("engine", ["python", "auto"])
def test_optimization_preserves_live_callbacks(engine) -> None:
    events = []

    def selector(row):
        events.append(row["value"])
        return row["value"] + 1

    def replacement(row):
        events.append(row["value"])
        return row["value"] * 10

    def source():
        try:
            yield {"value": 2}
            selector.__code__ = replacement.__code__
            yield {"value": 3}
        finally:
            events.append("closed")

    result = flow(source()).with_engine(engine).rows().with_columns(result=selector).to_list()
    assert result == [{"value": 2, "result": 3}, {"value": 3, "result": 30}]
    assert events == [2, 3, "closed"]


@pytest.mark.parametrize("stage", ["select", "with_columns"])
@pytest.mark.parametrize("mapping", [False, True])
def test_single_row_map_loop_keeps_callback_ownership_separate_between_queries(mapping, stage):
    """Reusing a loop template must not share selectors or keep discarded queries alive."""
    import gc
    import weakref
    from types import MappingProxyType

    from fpstreams.execution._rows_fusion import compile_rows_fusion
    from fpstreams.execution.physical import operations_from_physical_nodes
    from fpstreams.planning.compiler import compile_query

    class Callback:
        def __init__(self, marker):
            self.marker = marker

        def __call__(self, row):
            return self.marker

    def compile_loop(marker):
        callback = Callback(marker)
        reference = weakref.ref(callback)
        query = getattr(fpstreams.rows([{"value": 1}]), stage)(marker=callback)._flow
        physical = compile_query(query._query("list"))
        loop = compile_rows_fusion(operations_from_physical_nodes(physical.nodes))
        assert loop is not None
        return loop, reference

    left_marker, right_marker = object(), object()
    left, left_callback = compile_loop(left_marker)
    right, right_callback = compile_loop(right_marker)
    row = {"value": 1}
    source = MappingProxyType(row) if mapping else row
    left_values = left(iter([source, source]))
    right_values = right(iter([source, source]))
    assert next(left_values)["marker"] is left_marker
    assert next(right_values)["marker"] is right_marker
    assert next(left_values)["marker"] is left_marker
    assert next(right_values)["marker"] is right_marker
    left_values.close()
    right_values.close()
    del left, left_values
    gc.collect()
    assert left_callback() is None
    assert right_callback() is not None
    del right, right_values
    gc.collect()
    assert right_callback() is None
    assert row == {"value": 1}


def test_zero_column_output_adapters_preserve_rows_and_batch_bounds():
    schema = pa.schema([])
    source = fpstreams.rows([{}, {}, {}])
    pytest.importorskip("polars")
    batches = source.polars_batches(schema=schema, batch_size=2).to_list()
    assert [batch.shape for batch in batches] == [(2, 0), (1, 0)]
    retained = pa.table({"value": [1, 2, 3]}).select([])
    assert [
        batch.shape for batch in fpstreams.rows.from_arrow(retained).polars_batches(batch_size=2)
    ] == [(2, 0), (1, 0)]
    assert source.to_polars(schema=schema).shape == (3, 0)
    pytest.importorskip("pandas")
    assert source.to_pandas(schema=schema).shape == (3, 0)
    assert source.to_numpy().shape == (3, 0)


@pytest.mark.parametrize("how", ["any", "all"])
def test_drop_nulls_preserves_operator_selection_errors(how: str) -> None:
    failure = fpstreams.SelectionError("operator failure")
    calls = []

    class Broken:
        def __add__(self, value):
            calls.append(value)
            raise failure

    class Nested:
        def __add__(self, value):
            calls.append(value)
            return fpstreams.col("missing")({})

    expression = fpstreams.col("value") + 1
    assert fpstreams.rows([{}, {"value": 2}]).drop_nulls(
        expression, how=cast(Any, how)
    ).to_list() == [{"value": 2}]
    with pytest.raises(fpstreams.SelectionError) as caught:
        fpstreams.rows([{"value": Broken()}]).drop_nulls(expression, how=cast(Any, how)).to_list()
    assert caught.value is failure
    assert calls == [1]
    with pytest.raises(fpstreams.SelectionError, match="missing"):
        fpstreams.rows([{"value": Nested()}]).drop_nulls(expression, how=cast(Any, how)).to_list()
    assert calls == [1, 1]


@pytest.mark.parametrize("how", ["any", "all"])
@pytest.mark.parametrize("selector", ["value", fpstreams.col("value"), "nested.value"])
def test_drop_nulls_raise_preserves_generated_lookup_failures(how: str, selector: Any) -> None:
    """Strict lookup distinguishes no errors; every generated failure propagates."""
    from fpstreams import SelectionError

    failure = AttributeError("internal attribute failure")
    calls: list[str] = []

    class Broken:
        @property
        def value(self) -> object:
            calls.append("value")
            raise failure

    row: Any = (
        {"nested": Broken()}
        if isinstance(selector, str) and selector == "nested.value"
        else Broken()
    )
    with pytest.raises(SelectionError) as captured:
        fpstreams.rows([row]).drop_nulls(selector, how=cast(Any, how), missing="raise").to_list()
    assert captured.value.__cause__ is failure
    assert calls == ["value"]
    calls.clear()

    def direct(record: Any) -> object:
        target = record["nested"] if type(record) is dict else record
        return target.value

    with pytest.raises(AttributeError) as original:
        fpstreams.rows([row]).drop_nulls(direct, missing="raise").to_list()
    assert original.value is failure
    assert calls == ["value"]
    assert fpstreams.rows([row]).drop_nulls(selector, how=cast(Any, how)).to_list() == []
    with pytest.raises(SelectionError) as absent:
        fpstreams.rows([{}]).drop_nulls(selector, how=cast(Any, how), missing="raise").to_list()
    assert isinstance(absent.value.__cause__, KeyError)
    assert fpstreams.rows([{}]).drop_nulls(selector).to_list() == []


@pytest.mark.parametrize("how", ["any", "all"])
def test_drop_nulls_raise_mapping_and_explicit_missing_signal(how: str) -> None:
    """Only caller-designated absence becomes None; internal failures keep identity."""
    from fpstreams import SelectionError

    class AbsentValue(Exception):
        pass

    failure = KeyError("internal mapping failure")
    calls: list[object] = []

    class Record(dict[str, object]):
        def __getitem__(self, key: str) -> object:
            calls.append(self)
            if dict.__contains__(self, key):
                raise failure
            raise AbsentValue(key)

    def optional_value(row: Record) -> object:
        try:
            return row["value"]
        except AbsentValue:
            return None

    absent = Record()
    broken = Record(value=1)
    assert fpstreams.rows([absent]).drop_nulls(optional_value, missing="raise").to_list() == []
    assert calls == [absent]
    calls.clear()
    with pytest.raises(KeyError) as explicit:
        fpstreams.rows([broken]).drop_nulls(
            optional_value, how=cast(Any, how), missing="raise"
        ).to_list()
    assert explicit.value is failure
    assert len(calls) == 1 and calls[0] is broken
    calls.clear()
    with pytest.raises(SelectionError) as generated:
        fpstreams.rows([broken]).drop_nulls("value", how=cast(Any, how), missing="raise").to_list()
    assert generated.value.__cause__ is failure
    assert len(calls) == 1 and calls[0] is broken
    assert fpstreams.rows([broken]).drop_nulls("value").to_list() == []


@pytest.mark.parametrize("how,first", [("any", None), ("all", 1)])
def test_drop_nulls_raise_short_circuit_and_nested_tracking(how: str, first: object) -> None:
    """The explicit policy retains short circuiting and outer tracking state."""
    from fpstreams import SelectionError

    failure = AttributeError("callable failed")
    calls: list[str] = []

    def fail(_row: object) -> object:
        calls.append("fail")
        raise failure

    row = {"first": first}
    expected = [] if how == "any" else [row]
    assert (
        fpstreams.rows([row])
        .drop_nulls("first", fail, how=cast(Any, how), missing="raise")
        .to_list()
        == expected
    )
    assert calls == []
    with pytest.raises(AttributeError) as direct:
        fpstreams.rows([row]).drop_nulls(fail, missing="raise").to_list()
    assert direct.value is failure

    def nested(_value: object) -> object:
        with pytest.raises(SelectionError):
            fpstreams.rows([{}]).drop_nulls("missing", missing="raise").to_list()
        return None

    expression = fpstreams.col("value").map(nested)
    assert fpstreams.rows([{"value": 1}, {}]).drop_nulls(expression).to_list() == []
    assert fpstreams.rows([{"value": 1}]).drop_nulls(expression, missing="raise").to_list() == []


@pytest.mark.parametrize("size", [2, 512])
@pytest.mark.parametrize("how", ["any", "all"])
def test_drop_nulls_raise_bypasses_missing_suppressing_optimized_paths(size: int, how: str) -> None:
    """Retained sources and selected rows must honor the explicit failure policy."""
    from fpstreams import SelectionError

    records = [{"value": 1}] * size
    nullable = [{"value": None}, {"value": 1}]
    assert fpstreams.rows(nullable).drop_nulls(
        "value", how=cast(Any, how), missing="raise"
    ).to_list() == [nullable[1]]
    assert fpstreams.rows(records).drop_nulls("value", missing="raise").to_list() == records
    with pytest.raises(SelectionError):
        fpstreams.rows([*records, {}]).drop_nulls(
            "other", "value", how=cast(Any, how), missing="raise"
        ).to_list()
    with pytest.raises(SelectionError):
        fpstreams.rows([*records, {}]).select("value").drop_nulls(
            "other", missing="raise"
        ).to_list()
    with pytest.raises(ValueError, match="missing"):
        fpstreams.rows([]).drop_nulls(missing=cast(Any, "ignore"))
