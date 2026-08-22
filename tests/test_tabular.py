"""Rows expressions, joins, aggregation, reshaping, text I/O, cleaning, and spilling."""

from __future__ import annotations

import gc
import random
import sqlite3
import subprocess
import sys
import weakref
from collections.abc import Iterator, Mapping, Sequence
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

import fpstreams
from fpstreams import flow

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


def test_rows_keeps_basic_table_navigation_in_the_same_chain() -> None:
    table = fpstreams.rows([{"id": 1}, {"id": 1}, {"id": 2}]).unique_by("id")

    assert table.count() == 2
    assert table.skip(1).take(1).first() == {"id": 2}
    assert fpstreams.rows([{"a": 1}, {"a": 2, "b": 3}]).to_columns() == {
        "a": [1, 2],
        "b": [None, 3],
    }


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
    strings = pacsv.ConvertOptions(default_column_type=pa.string())
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
        fpstreams.rows([{"name": "outer", "profile": {"name": "inner"}}]).unnest(
            "profile"
        ).to_list()
    with pytest.raises(fpstreams.SelectionError, match="missing"):
        fpstreams.rows([{"id": 1}]).unnest("profile").to_list()


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
        direct.select("value").aggregate(total=fpstreams.agg.sum("value")),
        direct.aggregate(total=fpstreams.agg.sum(lambda row: row["value"])),
        direct.aggregate(total=fpstreams.agg.sum("nested.value")),
        direct.aggregate(total=fpstreams.agg.sum("value"), rows=fpstreams.agg.count()),
        fpstreams.rows.from_polars(pl.DataFrame({"value": [2, 3]}).lazy()).aggregate(
            total=fpstreams.agg.sum("value")
        ),
    )
    for candidate in candidates:
        physical = compile_query(candidate._flow._query("list"))
        assert isinstance(physical.root, GlobalAggregatePhysicalNode)
        assert physical.root.arrow_i64_sum is None


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
        .select("key", "value")
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value")),
        fpstreams.rows.from_arrow(table)
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum(lambda row: row["value"])),
        fpstreams.rows.from_arrow(table)
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
