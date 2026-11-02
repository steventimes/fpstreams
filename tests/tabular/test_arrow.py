"""tabular arrow regression contracts."""

from __future__ import annotations

import gc
import math
import struct
import subprocess
import sys
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import closing
from dataclasses import replace
from math import isnan
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

import fpstreams
from fpstreams.execution import arrow as arrow_execution
from fpstreams.execution import arrow as execution
from fpstreams.execution import relational as relational_execution
from fpstreams.tabular import arrow
from fpstreams.tabular import arrow as adapter
from fpstreams.tabular import arrow as arrow_adapter


def _observe_row_batches(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    sizes: list[int] = []
    convert_rows = arrow_adapter.batch_to_rows

    def tracked(batch: Any) -> list[dict[str, object]]:
        sizes.append(batch.num_rows)
        return convert_rows(batch)

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", tracked)
    return sizes


def _forbid_row_boxing(_batch: object) -> list[dict[str, object]]:
    raise AssertionError("the columnar path must not box or reopen Python rows")


class _ArrowStreamProvider:
    def __init__(self, table: pa.Table) -> None:
        self.table = table
        self.calls = 0

    def __arrow_c_stream__(self, requested_schema: object = None) -> object:
        self.calls += 1
        return self.table.__arrow_c_stream__(requested_schema)


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


def _arrow_reader_source(
    open_batches: Callable[[], Any],
    schema: Any,
    *,
    batch_size: int = 65_536,
    row_converter: Callable[[Any], Any] | None = None,
):
    """Build one-shot Arrow ownership with forbidden or explicitly controlled row fallback."""
    from fpstreams.planning.arrow.source import ArrowBatchSource
    from fpstreams.planning.source import Source, SourceCapabilities

    descriptor = ArrowBatchSource(open_batches, "reader", batch_size, schema, False)

    def rows():
        if row_converter is None:
            raise AssertionError("the guarded Arrow reader must not reopen Python rows")
        batches = descriptor.open_batches()
        try:
            for batch in batches:
                yield from row_converter(batch)
        finally:
            batches.close()

    return Source(
        rows,
        SourceCapabilities(reiterable=False, exact_size=None),
        native_data=descriptor,
    )


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


@pytest.mark.parametrize("override_rows", [False, True])
def test_native_arrow_batches_close_one_shot_stream_after_short_circuit(override_rows) -> None:
    """Closing the emitted Flow must release a claimed native batch stream immediately."""
    from fpstreams.planning.arrow.source import ArrowBatchSource
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

    def row_values() -> Iterator[dict[str, int]]:
        with closing(descriptor.open_batches()) as opened:
            for batch in opened:
                yield from batch.to_pylist()

    source = Source(
        row_values if override_rows else forbidden_rows,
        SourceCapabilities(reiterable=False, exact_size=None),
        native_data=descriptor,
    )
    query = (
        fpstreams.Rows(fpstreams.Flow(source)).where(fpstreams.col("value") >= 1).select("value")
    )

    if override_rows:

        class CustomRows(fpstreams.Rows):
            def __iter__(self):
                with closing(super().__iter__()) as values:
                    for row in values:
                        yield {"value": row["value"] + 10}

        query = CustomRows(query._flow)

    output = query.arrow_batches(batch_size=1)
    assert events == []
    assert output.take(0).to_list() == []
    assert events == []
    result = output.take(1).to_list()

    assert result[0].to_pylist() == [{"value": 11 if override_rows else 1}]
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

    monkeypatch.setattr(arrow_execution, "batch_to_rows", _forbid_row_boxing)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)
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

    events: list[str] = []
    batches = [
        pa.record_batch({"value": pa.array([1], type=pa.int64())}),
        pa.record_batch({"value": pa.array([2], type=pa.int64())}),
    ]

    source = _arrow_reader_source(
        lambda: _TrackedArrowBatches(batches, events), batches[0].schema, batch_size=1
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

    monkeypatch.setattr(arrow_execution, "batch_to_rows", _forbid_row_boxing)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)
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

    monkeypatch.setattr(arrow_execution, "batch_to_rows", _forbid_row_boxing)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)
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

    monkeypatch.setattr(arrow_execution, "batch_to_rows", _forbid_row_boxing)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)
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

    def fail_lowering(_node: object, _batch: object) -> object:
        raise error_type("table backend")

    monkeypatch.setattr(arrow_execution, "lower_row_expression", fail_lowering)
    monkeypatch.setattr(arrow_execution, "batch_to_rows", _forbid_row_boxing)
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

    monkeypatch.setattr(arrow_execution, "batch_to_rows", _forbid_row_boxing)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)
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

    source = fpstreams.rows.from_arrow(
        pa.table({"value": [1, 2], "payload": pa.array(["first", None], pa.string())}),
        batch_size=1,
    )

    monkeypatch.setattr(arrow_execution, "batch_to_rows", _forbid_row_boxing)
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


@pytest.mark.parametrize(
    ("hook", "terminal", "filtered", "message", "expected"),
    [
        ("_arrow_modules", "to_arrow", False, "backend setup", [{"value": 1}]),
        ("execute_operations", "to_list", True, "suffix setup", [{"value": 1}]),
        ("_count_scan_columns", "count", True, "count scan setup", 1),
    ],
)
def test_arrow_setup_failure_preserves_reader_for_later_evaluation(
    monkeypatch: pytest.MonkeyPatch,
    hook: str,
    terminal: str,
    filtered: bool,
    message: str,
    expected: object,
) -> None:
    """Backend, suffix and count planning failures must precede one-shot reader claims."""

    batch = pa.record_batch({"value": [1]})
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    query = fpstreams.rows.from_arrow(reader)
    if filtered:
        query = query.where(fpstreams.col("value") == 1)
    original = getattr(arrow_execution, hook)

    def fail_setup(*_args: object, **_kwargs: object) -> object:
        raise MemoryError(message)

    monkeypatch.setattr(arrow_execution, hook, fail_setup)
    with pytest.raises(MemoryError, match=message):
        getattr(query, terminal)()

    monkeypatch.setattr(arrow_execution, hook, original)
    result = getattr(query, terminal)()
    assert (result.to_pylist() if terminal == "to_arrow" else result) == expected


def test_from_arrow_closes_reader_when_schema_validation_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Construction failure releases a one-shot reader before ownership can be returned."""

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
    from fpstreams.tabular import dataframe

    table = dataframe._without_pandas_index
    conversions = 0

    def tracked_table(source: object, *args: object, **kwargs: object) -> object:
        nonlocal conversions
        conversions += 1
        return table(source, *args, **kwargs)

    monkeypatch.setattr(dataframe, "_without_pandas_index", tracked_table)

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

    from fpstreams.planning.arrow.source import ArrowBatchSource

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


def test_direct_parquet_global_count_uses_metadata_terminal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A direct aggregate count should share the source's metadata-aware terminal."""
    from fpstreams.physical.relational import GlobalAggregatePhysicalNode
    from fpstreams.planning.compiler import compile_query

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

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)

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

    table = pa.table(
        {
            "value": pa.chunked_array([[2, 3], [-1]]),
            "payload": pa.chunked_array([[20, 30], [-10]]),
        }
    )

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)
    provider = _ArrowStreamProvider(table)
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

    table = pa.table(
        {
            "value": pa.chunked_array([[2, 3], [-1, 7]], type=pa.int64()),
            "payload": pa.chunked_array([[20, 30], [-10, 70]], type=pa.int64()),
        }
    )

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)
    provider = _ArrowStreamProvider(table)
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

    source = _arrow_reader_source(open_batches, first.schema, batch_size=65536)
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

    source = _arrow_reader_source(open_batches, record_batches[0].schema, batch_size=65536)
    aggregated = fpstreams.Rows(fpstreams.Flow(source)).aggregate(
        rows=fpstreams.agg.count(),
        total=fpstreams.agg.sum("value"),
        low=fpstreams.agg.min("value"),
        high=fpstreams.agg.max("value"),
    )
    monkeypatch.setattr(relational_execution, "import_module", import_backend)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)

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


@pytest.mark.parametrize(
    ("kind", "items", "expected_events"),
    [
        ("first", [[], [None, 7], [9]], ["pull:0", "pull:1", "close"]),
        ("last", [[4], [], [9, None]], ["pull:0", "pull:1", "pull:2", "stop", "close"]),
    ],
)
def test_arrow_reader_global_endpoint_preserves_nulls_and_batch_consumption(
    kind: str, items: list[list[int | None]], expected_events: list[str]
) -> None:
    """First short-circuits and last scans to EOF, both retaining a selected null."""
    events: list[str] = []
    batches = [pa.record_batch({"value": pa.array(values, type=pa.int64())}) for values in items]
    source = _arrow_reader_source(
        lambda: _TrackedArrowBatches(batches, events), batches[0].schema, batch_size=65536
    )
    aggregated = fpstreams.Rows(fpstreams.Flow(source)).aggregate(
        result=getattr(fpstreams.agg, kind)("value")
    )
    assert aggregated.to_list() == [{"result": None}]
    assert events == expected_events
    with pytest.raises(fpstreams.FlowConsumedError):
        aggregated.to_list()
    assert events == expected_events


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

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)
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

    converted = _observe_row_batches(monkeypatch)
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

    converted = _observe_row_batches(monkeypatch)
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

    events: list[str] = []
    batch = pa.record_batch({"value": pa.array([2, 3], type=pa.int64())})

    source = _arrow_reader_source(
        lambda: _TrackedArrowBatches([batch], events), batch.schema, batch_size=65536
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

    events: list[str] = []
    first = pa.record_batch({"value": pa.array([2, 3], type=pa.int64())})
    null = pa.record_batch({"value": pa.array([None], type=pa.int64())})
    unpulled = pa.record_batch({"value": pa.array([5], type=pa.int64())})

    source = _arrow_reader_source(
        lambda: _TrackedArrowBatches([first, null, unpulled], events),
        first.schema,
        batch_size=65536,
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

    events: list[str] = []
    batch = pa.record_batch({"value": pa.array(range(129), type=pa.int64())})

    source = _arrow_reader_source(
        lambda: _TrackedArrowBatches([batch], events), batch.schema, batch_size=65536
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

    events: list[str] = []
    empty = pa.record_batch({"present": pa.array([], type=pa.int64())})
    present = pa.record_batch({"present": pa.array([1], type=pa.int64())})
    unpulled = pa.record_batch({"present": pa.array([2], type=pa.int64())})

    source = _arrow_reader_source(
        lambda: _TrackedArrowBatches([empty, present, unpulled], events),
        empty.schema,
        batch_size=65536,
        row_converter=lambda batch: batch.to_pylist(),
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
@pytest.mark.parametrize(
    ("kind", "values", "output_name", "expected"),
    [
        ("sum", [2, 3], "total", 5),
        ("min", list(range(256)), "result", 0),
        ("max", list(range(256)), "result", 255),
    ],
)
def test_direct_arrow_global_backend_decline_reopens_canonical_rows(
    monkeypatch: pytest.MonkeyPatch,
    error_type: type[Exception],
    kind: str,
    expected: int,
    values: list[int],
    output_name: str,
) -> None:
    """Replayable extrema fall back only for an expected Arrow kernel rejection."""

    imported = relational_execution.import_module

    class RejectingCompute:
        @staticmethod
        def min_max(_values: object) -> object:
            raise error_type("decline")

    def import_backend(name: str) -> object:
        return RejectingCompute if name == "pyarrow.compute" else imported(name)

    monkeypatch.setattr(relational_execution, "import_module", import_backend)
    converted = _observe_row_batches(monkeypatch)
    result = (
        fpstreams.rows.from_arrow(pa.table({"value": values}))
        .aggregate(**{output_name: getattr(fpstreams.agg, kind)("value")})
        .to_list()
    )

    assert result == [{output_name: expected}]
    assert converted == [len(values)]


@pytest.mark.parametrize(
    ("error_type", "message"),
    [(MemoryError, "global allocation"), (RuntimeError, "global backend")],
)
def test_direct_arrow_global_sum_error_propagates_without_row_fallback(
    monkeypatch: pytest.MonkeyPatch,
    error_type: type[Exception],
    message: str,
) -> None:
    """Allocation and unexpected backend errors must not reopen canonical rows."""

    imported = relational_execution.import_module

    class FailingCompute:
        @staticmethod
        def min_max(_values: object) -> object:
            raise error_type(message)

    def import_backend(name: str) -> object:
        return FailingCompute if name == "pyarrow.compute" else imported(name)

    monkeypatch.setattr(relational_execution, "import_module", import_backend)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)
    aggregated = fpstreams.rows.from_arrow(pa.table({"value": [2]})).aggregate(
        total=fpstreams.agg.sum("value")
    )

    with pytest.raises(error_type, match=message):
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

    def reject_projected_rows(_batch: object, _projection: object) -> Iterator[object]:
        raise AssertionError("safe Arrow prefix/global count boxed projected Python rows")

    monkeypatch.setattr(Source, "open", reject_arrow_row_open)
    monkeypatch.setattr(arrow_execution, "batch_to_rows", _forbid_row_boxing)
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

    def reject_projected_rows(_batch: object, _projection: object) -> Iterator[object]:
        raise AssertionError("safe Arrow prefix/global reduction boxed projected Python rows")

    monkeypatch.setattr(Source, "open", reject_arrow_row_open)
    monkeypatch.setattr(arrow_execution, "batch_to_rows", _forbid_row_boxing)
    monkeypatch.setattr(arrow_execution, "_project_batch_rows", reject_projected_rows)

    assert aggregated.to_list() == [{"result": expected}]


@pytest.mark.parametrize("grouped", [False, True])
def test_arrow_prefix_preserves_earlier_aggregate_error(grouped: bool) -> None:
    """A later prefix error must not replace an earlier streaming reduction error."""
    from fpstreams.planning.arrow.source import ArrowBatchSource
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
    if grouped:
        batches = tuple(
            batch.append_column("key", pa.array([index + 1], type=pa.int64()))
            for index, batch in enumerate(batches)
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
        aggregate = values.group_by("key") if grouped else values
        return aggregate.aggregate(total=fpstreams.agg.sum("value")), events

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

    def fail_lowering(_node: object, _batch: object) -> object:
        raise MemoryError("count allocation")

    monkeypatch.setattr(arrow_execution, "lower_row_expression", fail_lowering)
    monkeypatch.setattr(arrow_execution, "batch_to_rows", _forbid_row_boxing)
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

    target = tmp_path / f"values.{storage}"
    if storage == "csv":
        target.write_text("value,unused\n1,10\n3,20\n5,30\n2,40\n")
        source = fpstreams.rows.scan_csv(target, batch_size=1)
    else:
        pq.write_table(pa.table({"value": [1, 3, 5, 2], "unused": [10, 20, 30, 40]}), target)
        source = fpstreams.rows.from_parquet(target, batch_size=1)

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)

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

    def reject_projected_rows(_batch: object, _projection: object) -> Iterator[object]:
        raise AssertionError("safe Arrow select/group boxed projected Python rows")

    monkeypatch.setattr(Source, "open", reject_arrow_row_open)
    monkeypatch.setattr(arrow_execution, "batch_to_rows", _forbid_row_boxing)
    monkeypatch.setattr(arrow_execution, "_project_batch_rows", reject_projected_rows)

    assert grouped.to_list() == expected


def test_safe_arrow_prefix_group_keeps_closed_aggregate_lanes_columnar(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Count, sum, min, and max should consume a safe filtered table without row boxing."""
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

    monkeypatch.setattr(Source, "open", reject_arrow_row_open)
    monkeypatch.setattr(arrow_execution, "batch_to_rows", _forbid_row_boxing)

    assert grouped.to_list() == [
        {"key": 2, "rows": 2, "total": 7, "minimum": 3, "maximum": 4},
        {"key": 1, "rows": 2, "total": 9, "minimum": 2, "maximum": 7},
    ]


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

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)

    assert source.group_by("key").aggregate(total=fpstreams.agg.sum("value")).to_list() == [
        {"key": 2, "total": 16},
        {"key": 1, "total": 9},
        {"key": 3, "total": -4},
    ]


def test_file_arrow_group_sum_unsupported_schema_continues_without_reopening() -> None:
    """A speculative file schema decline must consume the already-opened batch stream once."""
    from fpstreams.planning.arrow.source import ArrowBatchSource
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

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)

    assert source.group_by("key").aggregate(total=fpstreams.agg.sum("value")).to_list() == [
        {"key": None, "total": maximum + 1},
        {"key": 1, "total": maximum + 2},
    ]


def test_file_arrow_group_sum_null_stops_before_later_batches_and_closes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A null value raises at its row without pulling or reopening the next file batch."""
    from fpstreams.planning.arrow.source import ArrowBatchSource
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

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)
    with pytest.raises(TypeError, match="unsupported operand type"):
        grouped.to_list()
    assert events == ["open", "pull:0", "pull:1", "close"]


def test_file_arrow_group_sum_memory_error_closes_without_reopening(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Allocation failure propagates unchanged and releases the claimed file iterator."""
    from fpstreams.planning.arrow.source import ArrowBatchSource
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
    from fpstreams.runtime.failpoints import failpoint

    target = tmp_path / "instrumented-group.csv"
    target.write_text("key,value\n1,2\n1,3\n", encoding="utf-8")

    def forbidden_native(*_arguments: object, **_options: object) -> object:
        raise AssertionError("an active failpoint must bypass file Arrow grouping")

    converted = _observe_row_batches(monkeypatch)
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

    target = tmp_path / "tiny-group.csv"
    target.write_text("key,value,unused\n1,2,a\n1,3,b\n", encoding="utf-8")

    def forbidden_native(*_arguments: object, **_options: object) -> object:
        raise AssertionError("tiny CSV grouping must decline before native file execution")

    converted = _observe_row_batches(monkeypatch)
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
    from fpstreams.planning.arrow.source import ArrowBatchSource
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
    from fpstreams.planning.arrow.source import ArrowBatchSource
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

    batches = (
        pa.record_batch({"key": [2, 1, 2], "value": [5, 7, 11]}),
        pa.record_batch({"key": [3, 1, 4], "value": [-4, 2, 8]}),
    )
    events: list[str] = []
    opened = _TrackedArrowBatches(batches, events)

    source = _arrow_reader_source(lambda: opened, batches[0].schema, batch_size=65536)
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
    from fpstreams.physical.relational import ArrowGroupAggregateSpec, GroupAggregatePhysicalNode
    from fpstreams.planning.compiler import compile_query

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

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)
    source = _arrow_reader_source(lambda: opened, batches[0].schema, batch_size=65536)
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


@pytest.mark.parametrize("multi_lane", [False, True])
def test_arrow_reader_group_null_stops_before_later_batches_and_closes(
    monkeypatch: pytest.MonkeyPatch,
    multi_lane: bool,
) -> None:
    """A claimed nullable batch keeps row-major lane errors and stops pulling."""
    from fpstreams.tabular import arrow as arrow_adapter

    if multi_lane:
        monkeypatch.setattr(relational_execution, "_ARROW_READER_GROUP_MULTI_MIN_ROWS", 0)
    batches = (
        pa.record_batch({"key": [1, 2], "value": [2, 3]}),
        pa.record_batch({"key": [1, 1], "value": pa.array([4, None], type=pa.int64())}),
        pa.record_batch({"key": [1], "value": [99]}),
    )
    events: list[str] = []
    opened = _TrackedArrowBatches(batches, events)

    source = _arrow_reader_source(
        lambda: opened,
        batches[0].schema,
        batch_size=65536,
        row_converter=lambda batch: arrow_adapter.batch_to_rows(batch),
    )
    collectors = {"total": fpstreams.agg.sum("value")}
    if multi_lane:
        collectors = {
            "rows": fpstreams.agg.count(),
            **collectors,
            "low": fpstreams.agg.min("value"),
        }
    grouped = fpstreams.Rows(fpstreams.Flow(source)).group_by("key").aggregate(**collectors)

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)
    with pytest.raises(TypeError, match="unsupported operand type"):
        grouped.to_list()
    assert events == ["pull:0", "pull:1", "close"]
    with pytest.raises(fpstreams.FlowConsumedError):
        grouped.to_list()


def test_arrow_reader_group_multi_compute_decline_continues_from_claimed_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A recoverable multi-lane kernel decline switches the same reader to scalar folding."""

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

    monkeypatch.setattr(
        relational_execution,
        "_try_arrow_retained_group_aggregate",
        decline_second,
    )
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)
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

    monkeypatch.setattr(relational_execution, "_ARROW_READER_GROUP_MULTI_MIN_ROWS", 0)
    monkeypatch.setattr(relational_execution, "_ARROW_READER_GROUP_CARDINALITY_SAMPLE_ROWS", 1)
    monkeypatch.setattr(relational_execution, "_ARROW_READER_GROUP_MAX_DISTINCT_RATIO", 0.0)
    batches = (
        pa.record_batch({"key": [3, 1, 2], "value": [5, 7, 11]}),
        pa.record_batch({"key": [4, 1], "value": [-4, 2]}),
    )
    events: list[str] = []
    opened = _TrackedArrowBatches(batches, events)

    def forbidden_partial(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("high-cardinality reader must skip Arrow partial grouping")

    converted = _observe_row_batches(monkeypatch)
    monkeypatch.setattr(
        relational_execution,
        "_try_arrow_retained_group_aggregate",
        forbidden_partial,
    )
    source = _arrow_reader_source(lambda: opened, batches[0].schema, batch_size=65536)
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

    batch = pa.record_batch(
        {
            "key": pa.array([1, 1], type=pa.int64()),
            "value": pa.array([2, 3], type=pa.int32()),
        }
    )
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])

    converted = _observe_row_batches(monkeypatch)
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

    maximum = 2**63 - 1
    batches = (
        pa.record_batch({"key": [1, 1, 2], "value": [maximum, 1, -(2**63)]}),
        pa.record_batch({"key": [1, 2], "value": [maximum, -1]}),
    )
    reader = pa.RecordBatchReader.from_batches(batches[0].schema, batches)

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)
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


def test_arrow_reader_group_sum_compute_decline_continues_from_claimed_batches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A recoverable Arrow kernel decline falls back inside the claimed reader."""

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

    monkeypatch.setattr(relational_execution, "import_module", import_backend)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)
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

    imported = relational_execution.import_module
    actual_compute = imported("pyarrow.compute")
    monkeypatch.setattr(relational_execution, "_ARROW_GROUP_BATCH_SCALAR_MAX_ROWS", 0)
    batch = pa.record_batch({"key": [1, 1], "value": [2, 3]})
    events: list[str] = []
    opened = _TrackedArrowBatches((batch,), events)

    class FailingCompute:
        @staticmethod
        def unique(_values: object) -> object:
            raise MemoryError("reader group allocation")

        def __getattr__(self, name: str) -> object:
            return getattr(actual_compute, name)

    def import_backend(name: str) -> object:
        return FailingCompute() if name == "pyarrow.compute" else imported(name)

    source = _arrow_reader_source(lambda: opened, batch.schema, batch_size=65536)
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

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)
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

    monkeypatch.setattr(relational_execution, "_ARROW_GROUP_BATCH_SCALAR_MAX_ROWS", 0)

    maximum = 2**63 - 1
    keys = [0, 1, 1, 1, maximum, -1, -2, maximum, maximum, -2, maximum, 1, 2, 0, 1]
    batch = pa.record_batch({"key": keys, "value": [1] * len(keys)})
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)
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

    converted = _observe_row_batches(monkeypatch)
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

    converted = _observe_row_batches(monkeypatch)
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


def test_dataframe_without_c_stream_preserves_arrow_extension_columns_and_index_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Older concrete pandas frames use Arrow conversion without leaking their index."""
    monkeypatch.setattr(pd.DataFrame, "__arrow_c_stream__", None, raising=False)
    values = pd.arrays.ArrowExtensionArray(pa.array([[1], [2]], type=pa.list_(pa.int64())))
    frame = pd.DataFrame({"value": pd.Series(values)})
    frame.index = pd.Index([7, 8], name="physical_index")
    source = fpstreams.rows.from_dataframe(frame)
    assert source.to_list() == [{"value": [1]}, {"value": [2]}]
    frame["value"] = pd.arrays.ArrowExtensionArray(pa.array([[9], [2]], type=pa.list_(pa.int64())))
    assert source.to_list() == [{"value": [9]}, {"value": [2]}]


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
@pytest.mark.parametrize(
    ("key", "unused_values", "unused_type"),
    [
        pytest.param(1, [[1], [2]], pa.list_(pa.int64()), id="nested-column"),
        pytest.param(1.5, [1, 2], pa.int64(), id="post-guard"),
    ],
)
def test_eager_pandas_group_decline_reuses_one_arrow_conversion(
    monkeypatch: pytest.MonkeyPatch,
    multi_lane: bool,
    key: float,
    unused_values: list[object],
    unused_type: pa.DataType,
) -> None:
    """Both pandas group plans must retain the first converted table snapshot."""
    unused = pd.arrays.ArrowExtensionArray(pa.array(unused_values, type=unused_type))
    frame = pd.DataFrame(
        {
            "key": [key, key],
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

    expected = {"key": key, "total": 5}
    if multi_lane:
        expected["rows"] = 2
    assert result == [expected]
    assert calls == 1


@pytest.mark.parametrize("adapter", ["dataframe", "polars"])
def test_eager_columnar_frame_group_sum_preserves_float_key_adapter_semantics(adapter: str) -> None:
    """Unsupported float keys retain each adapter's established NaN/null conversion."""

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

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)
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

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)
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

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)
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

    monkeypatch.setattr(relational_execution, "import_module", import_backend)
    monkeypatch.setattr(relational_execution, "_ARROW_GROUP_TABLE_MIN_GROUPS", 0)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)
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

    dictionary_keys = pa.DictionaryArray.from_arrays(
        pa.array([2, 0, 2, 1, 0], type=pa.int8()),
        pa.array(["a", "b", "c"]),
    )
    table = pa.table({"key": dictionary_keys, "value": [4, 7, 3, -2, 5]})
    monkeypatch.setattr(relational_execution, "import_module", import_backend)
    monkeypatch.setattr(relational_execution, "_ARROW_GROUP_TABLE_MIN_GROUPS", 0)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)

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

    def forbidden_arrays(*_arguments: object, **_options: object) -> list[object]:
        raise AssertionError("extension keys must not reach grouped Arrow materialization")

    monkeypatch.setattr(relational_execution, "_ARROW_GROUP_TABLE_MIN_GROUPS", 0)
    monkeypatch.setattr(relational_execution, "_arrow_group_lane_arrays", forbidden_arrays)
    converted = _observe_row_batches(monkeypatch)

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

    converted = _observe_row_batches(monkeypatch)
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

    converted = _observe_row_batches(monkeypatch)
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
    from fpstreams.physical.relational import ArrowGroupAggregateSpec, GroupAggregatePhysicalNode
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
@pytest.mark.parametrize("multi_lane", [False, True])
def test_arrow_group_backend_decline_reopens_canonical_rows(
    monkeypatch: pytest.MonkeyPatch, error_type: type[Exception], multi_lane: bool
) -> None:
    """Expected Arrow rejections leave the retained source clean for row fallback."""

    imported = relational_execution.import_module
    actual_compute = imported("pyarrow.compute")

    class RejectingCompute:
        def min_max(self, _values: object) -> object:
            raise error_type("decline")

        def __getattr__(self, name: str) -> object:
            return getattr(actual_compute, name)

    rejecting_compute = RejectingCompute()

    def import_backend(name: str) -> object:
        return (
            (RejectingCompute() if multi_lane else rejecting_compute)
            if name == "pyarrow.compute"
            else imported(name)
        )

    monkeypatch.setattr(relational_execution, "import_module", import_backend)
    converted = _observe_row_batches(monkeypatch)

    collectors = {"total": fpstreams.agg.sum("value")}
    expected = {"key": 1, "total": 5}
    if multi_lane:
        collectors = {"n": fpstreams.agg.count(), **collectors}
        expected["n"] = 2
    assert (
        fpstreams.rows.from_arrow(pa.table({"key": [1, 1], "value": [2, 3]}))
        .group_by("key")
        .aggregate(**collectors)
        .to_list()
    ) == [expected]
    assert converted == [2]


@pytest.mark.parametrize("multi_lane", [False, True])
def test_arrow_group_memory_error_never_reopens_rows(
    monkeypatch: pytest.MonkeyPatch,
    multi_lane: bool,
) -> None:
    """Allocation failure propagates instead of masquerading as an unsupported kernel."""

    message = "multi-group allocation" if multi_lane else "group allocation"
    imported = relational_execution.import_module
    actual_compute = imported("pyarrow.compute")

    class FailingCompute:
        def min_max(self, _values: object) -> object:
            raise MemoryError(message)

        def __getattr__(self, name: str) -> object:
            return getattr(actual_compute, name)

    failing_compute = FailingCompute()

    def import_backend(name: str) -> object:
        return (
            (FailingCompute() if multi_lane else failing_compute)
            if name == "pyarrow.compute"
            else imported(name)
        )

    monkeypatch.setattr(relational_execution, "import_module", import_backend)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)
    collectors = {"total": fpstreams.agg.sum("value")}
    if multi_lane:
        collectors = {"n": fpstreams.agg.count(), **collectors}
    grouped = (
        fpstreams.rows.from_arrow(pa.table({"key": [1], "value": [2]}))
        .group_by("key")
        .aggregate(**collectors)
    )

    with pytest.raises(MemoryError, match=message):
        grouped.to_list()


def test_arrow_group_multi_aggregate_uses_named_columns_across_arrow_orders(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A backend that returns aggregate columns before the key must remain correct."""

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

    monkeypatch.setattr(relational_execution, "import_module", import_backend)
    monkeypatch.setattr(relational_execution, "_ARROW_GROUP_TABLE_MIN_GROUPS", 0)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)
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

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)
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
    from fpstreams.planning.arrow.source import ArrowBatchSource

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

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)
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

    from fpstreams.runtime.failpoints import failpoint

    converted = _observe_row_batches(monkeypatch)
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
    from fpstreams.planning.arrow.source import ArrowBatchSource
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


@pytest.mark.parametrize("kind", ["table", "record_batch"])
def test_rows_arrow_c_stream_defers_known_native_batches_until_pull(kind: Any) -> None:
    """Importing a known-schema in-memory stream must not request its first batch."""
    from fpstreams.planning.arrow.source import ArrowBatchSource
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
    from fpstreams.planning.arrow.source import ArrowBatchSource
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
    from fpstreams.planning.arrow.source import batch_to_rows as convert_rows

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
    source = fpstreams.rows.from_arrow(reader, batch_size=2)
    assert source.take(1).to_list() == [{"id": 0}]
    assert converted_sizes == [2]
    with pytest.raises(fpstreams.FlowConsumedError):
        source.to_list()
    assert converted_sizes == [2]


def test_arrow_row_conversion_uses_the_owned_pylist_dictionaries_directly() -> None:
    """The adapter does not duplicate dictionaries already materialized by Arrow."""
    from fpstreams.planning.arrow.source import batch_to_rows

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
    from fpstreams.planning.arrow.source import ArrowBatchSource
    from fpstreams.planning.source import Source

    table = pa.table({"id": list(range(5)), "value": list(range(10, 15))})
    source_value = table.to_batches()[0] if as_record_batch else table
    open_rows = Source.open

    def reject_arrow_rows(source: Source[object]) -> Iterator[object]:
        if isinstance(source.native_data, ArrowBatchSource):
            raise AssertionError("identity Arrow to_list must not open the row iterator")
        return open_rows(source)

    converted_sizes = _observe_row_batches(monkeypatch)
    monkeypatch.setattr(Source, "open", reject_arrow_rows)

    assert fpstreams.rows.from_arrow(source_value, batch_size=2).to_list() == [
        {"id": index, "value": index + 10} for index in range(5)
    ]
    assert converted_sizes == [2, 2, 1]


def test_arrow_identity_list_forced_python_uses_the_canonical_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Forced Python must open rows instead of claiming the direct Arrow list path."""
    from fpstreams.planning.arrow.source import ArrowBatchSource
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
    from fpstreams.planning.arrow.source import ArrowBatchSource

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

    monkeypatch.setattr(arrow_execution, "batch_to_rows", _forbid_row_boxing)
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

    def fail_lowering(_node: object, _batch: object) -> object:
        raise MemoryError("prefix allocation")

    monkeypatch.setattr(arrow_execution, "lower_row_expression", fail_lowering)
    monkeypatch.setattr(arrow_execution, "batch_to_rows", _forbid_row_boxing)
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

    if terminal != "mean":
        monkeypatch.setattr(Source, "open", reject_python_rows)
    monkeypatch.setattr(arrow_execution, "batch_to_rows", _forbid_row_boxing)

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

    def reject_scalar_prefix(*_args: object) -> Iterator[object]:
        raise AssertionError("exact i64 reduction streamed a Python scalar prefix")

    monkeypatch.setattr(arrow_execution, "_execute_batch_program", reject_scalar_prefix)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)
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

    batch = pa.record_batch({"value": pa.array([2, 3, -1], type=pa.int64())})
    source = (
        batch
        if adapter == "record_batch"
        else pa.RecordBatchReader.from_batches(batch.schema, [batch])
    )

    def reject_scalar_prefix(*_args: object) -> Iterator[object]:
        raise AssertionError("exact i64 reduction streamed a Python scalar prefix")

    monkeypatch.setattr(arrow_execution, "_execute_batch_program", reject_scalar_prefix)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)
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
    from fpstreams.planning.arrow.source import ArrowBatchSource
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
        ("min", UnicodeDecodeError),
        ("max", UnicodeDecodeError),
        ("mean", UnicodeDecodeError),
    ],
)
def test_arrow_i64_reader_reduction_preserves_source_value_error_handling(
    terminal: str,
    error_type: type[Exception],
) -> None:
    """Claimed reduction retains the original source error, as does the Python path."""
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
        ("min", ValueError),
        ("max", ValueError),
        ("mean", ValueError),
    ],
)
def test_arrow_i64_reader_reduction_preserves_late_pull_value_error(
    terminal: str,
    error_type: type[Exception],
) -> None:
    """A later reader pull failure keeps its error, closure, and consumption."""
    from fpstreams.planning.arrow.source import batch_to_rows

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

        source = _arrow_reader_source(
            LatePullFailure,
            batch.schema,
            batch_size=65536,
            row_converter=lambda current: batch_to_rows(current),
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
        ("min", ValueError),
        ("max", ValueError),
        ("mean", ValueError),
    ],
)
def test_arrow_i64_reader_reduction_preserves_synchronous_opener_value_error(
    terminal: str,
    error_type: type[Exception],
) -> None:
    """A synchronous reader opener failure retains its error and spends the source once."""
    from fpstreams.planning.arrow.source import batch_to_rows

    schema = pa.schema([pa.field("value", pa.int64())])

    def build(engine: str) -> tuple[fpstreams.Flow[object], list[str]]:
        events: list[str] = []

        def open_batches() -> Iterator[object]:
            events.append("open")
            raise ValueError("reader opener failure")

        source = _arrow_reader_source(
            open_batches, schema, batch_size=65536, row_converter=lambda batch: batch_to_rows(batch)
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

    monkeypatch.setattr(arrow_execution, "batch_to_rows", _forbid_row_boxing)
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


@pytest.mark.parametrize("terminal", ["to_list", "count", "to_arrow", "arrow_batches"])
@pytest.mark.parametrize("projection", [False, True])
def test_arrow_unsafe_filter_sequence_preserves_row_first_exception_order(terminal, projection):
    """The first row's later filter must fail before the second row's earlier filter."""
    table = pa.table({"x": [1, None], "y": [None, 1]})
    query = (
        fpstreams.rows.from_arrow(table)
        .where(fpstreams.col("x") >= 0)
        .where(fpstreams.col("y") < 2)
    )
    if projection:
        query = query.select("x")
    with pytest.raises(TypeError, match=r"^'<' not supported"):
        result = getattr(query, terminal)()
        if terminal == "arrow_batches":
            list(result)


@pytest.mark.parametrize("flow_view", [False, True])
def test_arrow_unsafe_filter_prefix_does_not_visit_tail_after_take(flow_view):
    """Closing a nullable prefix after its first output never evaluates the tail row."""
    query = (
        fpstreams.rows.from_arrow(pa.table({"x": [1, None], "y": [1, 1]}))
        .where(fpstreams.col("x") >= 0)
        .where(fpstreams.col("y") < 2)
    )
    limited = query.to_flow().take(1) if flow_view else query.limit(1)
    assert limited.to_list() == [{"x": 1, "y": 1}]


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
    from fpstreams.planning.arrow.source import ArrowBatchSource
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

        source = _arrow_reader_source(
            open_batches,
            batches[0].schema,
            batch_size=1,
            row_converter=lambda batch: batch.to_pylist(),
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
    from fpstreams.planning.arrow.source import batch_to_rows as convert_rows

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


def test_arrow_first_close_failure_raises_or_annotates_primary_error() -> None:
    """A found row cannot hide failed cleanup; a query failure remains primary."""
    from fpstreams.planning.arrow.source import ArrowBatchSource
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

    with pytest.raises(RuntimeError, match="close failed"):
        query("id").first()
    with pytest.raises(fpstreams.SelectionError) as caught:
        query("missing").first()
    assert any("close failed" in note for note in caught.value.__notes__)
    assert closed == ["close", "close"]


def test_arrow_first_preserves_memory_error_from_batch_safety(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Resource exhaustion cannot be mistaken for an ordinary unsupported kernel."""

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
    from fpstreams.planning.arrow.source import ArrowBatchSource
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
    nested = pa.record_batch({"value": [{"x": 1}, {"x": 2}]})
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
    from fpstreams.planning.arrow.source import ArrowBatchSource

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

    monkeypatch.setattr(arrow_execution, "batch_to_rows", _forbid_row_boxing)
    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)

    assert source.where(fpstreams.col("id") >= 1).select("payload").to_list() == [
        {"payload": "one"},
        {"payload": "two"},
    ]


def test_parquet_identity_count_uses_scanner_count_rows_without_batches(
    tmp_path: Path,
) -> None:
    """An identity count should use Parquet metadata instead of opening scan batches."""

    from fpstreams.planning.arrow.source import ArrowBatchSource

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

    from fpstreams.planning.arrow.source import ArrowBatchSource

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

    from fpstreams.planning.arrow.source import ArrowBatchSource

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

    from fpstreams.planning.arrow.source import ArrowBatchSource
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


def test_parquet_query_projection_keeps_filter_dependencies_at_executor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Validated scan outputs retain filter fields and drop unrelated Parquet columns."""

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


def test_parquet_exact_equality_narrows_batches_and_keeps_the_python_residual(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A proven equality narrows validated batches while the original filter still executes."""

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


@pytest.mark.parametrize("kind", ["utf8", "date"])
@pytest.mark.parametrize("filtered_out", [False, True])
@pytest.mark.parametrize("filter_count", [1, 2])
@pytest.mark.parametrize("terminal", ["to_list", "count", "first"])
def test_parquet_query_hints_preserve_source_conversion_errors(
    tmp_path: Path, kind: str, filtered_out: bool, filter_count: int, terminal: str
) -> None:
    """Automatic hints cannot hide invalid values in discarded columns or row groups."""
    if kind == "utf8":
        bad = pa.array([b"\xff", b"ok"], type=pa.binary()).view(pa.string())
        error = UnicodeDecodeError
    else:
        bad = pa.array([2**31 - 1, 0], type=pa.int32()).view(pa.date32())
        error = OverflowError
    target = tmp_path / "invalid-unused.parquet"
    pq.write_table(pa.table({"id": [0, 1], "bad": bad}), target, row_group_size=1)
    query = fpstreams.rows.from_parquet(target).where(
        fpstreams.col("id") >= (1 if filtered_out else 0)
    )
    if filter_count == 2:
        query = query.where(fpstreams.col("id") < 2)
    query = query.select("id")
    with pytest.raises(error) as canonical:
        getattr(query.with_engine("python"), terminal)()
    with pytest.raises(error) as automatic:
        getattr(query, terminal)()
    assert automatic.value.args == canonical.value.args


@pytest.mark.parametrize("public_projection", [False, True])
@pytest.mark.parametrize("return_predicate", [False, True])
@pytest.mark.parametrize(
    ("source_filter", "hint"),
    [(False, "range"), (False, "equality"), (False, None), (True, "range")],
)
def test_parquet_conversion_validation_skips_only_safe_unused_fields(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    public_projection: bool,
    return_predicate: bool,
    source_filter: bool,
    hint: str | None,
) -> None:
    """One scan retains all fallible fields in source order and every query dependency."""
    from fpstreams.planning.arrow.source import ArrowScanRequest

    target = tmp_path / "validation-columns.parquet"
    table = pa.table(
        {
            "safe_extra": [0.0, 1.0, 2.0],
            "left": [None, "a", "b"],
            "id": [0, 1, 2],
            "right": ["c", "d", "e"],
            "payload": [10, 20, 30],
            "nullable_safe": pa.array([None, 4, 5], type=pa.int64()),
            "binary_safe": [b"\xff"] * 3,
        }
    )
    pq.write_table(table, target, row_group_size=1)
    source_columns = (
        ["right", "payload", "safe_extra", "left", "id", "nullable_safe"]
        if public_projection
        else None
    )
    visible = source_columns if source_columns is not None else table.schema.names
    options = []
    opened = []
    dataset = ds.dataset

    class ObservedDataset:
        def __init__(self, value):
            self.value = value

        def __getattr__(self, name):
            return getattr(self.value, name)

        def scanner(self, **scan_options):
            options.append(scan_options)
            return self.value.scanner(**scan_options)

    def observed_dataset(*args, **kwargs):
        opened.append(True)
        return ObservedDataset(dataset(*args, **kwargs))

    monkeypatch.setattr(ds, "dataset", observed_dataset)
    _base, _size, _projected, requested, _count = adapter._parquet_batch_factory(
        target, columns=source_columns, filter=ds.field("id") >= 1 if source_filter else None
    )
    wanted = ("payload", "id") if return_predicate else ("payload",)
    request = ArrowScanRequest(
        columns=wanted,
        range_predicate=("id", ">=", 1) if hint == "range" else None,
        equality=("id", 2) if hint == "equality" else None,
    )
    kept = [2] if hint == "equality" else ([1, 2] if hint == "range" else [0, 1, 2])
    expected = [
        {"payload": (index + 1) * 10, **({"id": index} if return_predicate else {})}
        for index in kept
    ]
    assert [row for batch in requested(request) for row in batch.to_pylist()] == expected
    assert opened == [True]
    assert len(options) == 1
    validation_fields = {"left", "right", *wanted}
    if hint is not None and not source_filter:
        validation_fields.add("id")
    assert options[0]["columns"] == [name for name in visible if name in validation_fields]
    assert options[0]["filter"] is not None if source_filter else options[0]["filter"] is None


@pytest.mark.parametrize("public_projection", [False, True])
@pytest.mark.parametrize("terminal", ["to_list", "count", "first"])
def test_parquet_validation_projection_preserves_conversion_error_order(
    tmp_path: Path, public_projection: bool, terminal: str
) -> None:
    """Discarding safe fields never reorders two fallible fields at the row boundary."""
    target = tmp_path / "two-invalid-columns.parquet"
    pq.write_table(
        pa.table(
            {
                "unused": [7, 8],
                "left": pa.array([b"ok", b"\xff"], type=pa.binary()).view(pa.string()),
                "id": [0, 1],
                "right": pa.array([b"\xfe", b"ok"], type=pa.binary()).view(pa.string()),
            }
        ),
        target,
    )
    source_columns = ["right", "id", "left", "unused"] if public_projection else None
    query = (
        fpstreams.rows.from_parquet(target, columns=source_columns)
        .where(fpstreams.col("id") >= 1)
        .where(fpstreams.col("id") < 3)
        .select("id")
    )
    errors = []
    for candidate in (query.with_engine("python"), query):
        with pytest.raises(UnicodeDecodeError) as error:
            getattr(candidate, terminal)()
        errors.append(error.value.args)
    assert errors[0] == errors[1]
    assert errors[0][1] == (b"\xfe" if public_projection else b"\xff")


@pytest.mark.parametrize("text_field", [False, True])
@pytest.mark.parametrize("lower", [0, 1])
@pytest.mark.parametrize("terminal", ["to_list", "count", "to_arrow"])
def test_parquet_query_hints_preserve_fragment_cast_errors(
    tmp_path: Path, text_field: bool, lower: int, terminal: str
) -> None:
    """A logical integer field can still fail while casting a later fragment's strings."""
    first = {"id": [0], "value": [10], "unused": [1]}
    second = {"id": [1], "value": [20], "unused": ["bad"]}
    if text_field:
        first["label"] = ["a"]
        second["label"] = ["b"]
    pq.write_table(pa.table(first), tmp_path / "a.parquet")
    pq.write_table(pa.table(second), tmp_path / "b.parquet")
    query = (
        fpstreams.rows.from_parquet(tmp_path).where(fpstreams.col("id") >= lower).select("value")
    )
    errors = []
    for candidate in (query.with_engine("python"), query):
        with pytest.raises(pa.ArrowInvalid) as error:
            getattr(candidate, terminal)()
        errors.append(str(error.value))
    assert errors[0] == errors[1]


@pytest.mark.parametrize("source_kind", ["fragments", "schema_cast"])
@pytest.mark.parametrize("use_threads", [False, True])
def test_parquet_native_cast_errors_follow_the_explicit_source_projection(
    tmp_path: Path,
    source_kind: str,
    use_threads: bool,
) -> None:
    """Full visible casts must fail; explicit source columns can omit unrelated casts."""
    source: object = tmp_path
    if source_kind == "fragments":
        pq.write_table(pa.table({"id": [0], "value": [10], "unused": [1]}), tmp_path / "a.parquet")
        pq.write_table(
            pa.table({"id": [1], "value": [20], "unused": ["bad"]}), tmp_path / "b.parquet"
        )
    else:
        path = tmp_path / "cast.parquet"
        pq.write_table(
            pa.table({"id": [0, 1], "value": [10, 20], "unused": ["1", "bad"]}),
            path,
            row_group_size=1,
        )
        source = ds.dataset(
            path,
            format="parquet",
            schema=pa.schema([("id", pa.int64()), ("value", pa.int64()), ("unused", pa.int64())]),
        )
    query = (
        fpstreams.rows.from_parquet(source, batch_size=1, use_threads=use_threads)
        .where(fpstreams.col("id") >= 0)
        .select("value")
    )
    for candidate in (query, query.with_engine("python")):
        with pytest.raises(pa.ArrowInvalid, match="bad"):
            candidate.to_list()
    projected = (
        fpstreams.rows.from_parquet(
            source,
            columns=["id", "value"],
            batch_size=1,
            use_threads=use_threads,
        )
        .where(fpstreams.col("id") >= 0)
        .select("value")
    )
    for candidate in (projected, projected.with_engine("python")):
        assert candidate.first() == {"value": 10}
        assert candidate.limit(1).to_list() == [{"value": 10}]
        assert candidate.to_list() == [{"value": 10}, {"value": 20}]


@pytest.mark.parametrize("fail_before_first", [False, True])
def test_parquet_consumption_preserves_delivered_prefixes_and_source_failures(
    monkeypatch: pytest.MonkeyPatch,
    fail_before_first: bool,
) -> None:
    """Do not pull beyond a delivered prefix or replay a source that failed before it."""
    from types import SimpleNamespace

    events = []
    failure = pa.ArrowInvalid("source cast failed")
    schema = pa.schema([("id", pa.int64()), ("value", pa.int64())])

    class Scanner:
        projected_schema = schema

        def to_batches(self) -> Iterator[object]:
            events.append("open")
            try:
                if fail_before_first:
                    raise failure
                events.append("first")
                yield pa.record_batch({"id": [0], "value": [10]})
                events.append("later")
                raise failure
            finally:
                events.append("close")

    class Dataset:
        def __init__(self) -> None:
            self.schema = schema

        def scanner(self, **_options: object) -> Scanner:
            return Scanner()

    monkeypatch.setattr(
        adapter,
        "_arrow_modules",
        lambda: (pa, SimpleNamespace(Dataset=Dataset, field=ds.field), object()),
    )
    query = (
        fpstreams.rows.from_parquet(Dataset(), batch_size=1)
        .where(fpstreams.col("id") >= 0)
        .select("value")
    )
    for candidate in (query, query.with_engine("python")):
        for terminal, expected in (
            (lambda q: q.first(), {"value": 10}),
            (lambda q: q.limit(1).to_list(), [{"value": 10}]),
            (lambda q: q.to_flow().take(1).to_list(), [{"value": 10}]),
        ):
            events.clear()
            if fail_before_first:
                with pytest.raises(pa.ArrowInvalid) as caught:
                    terminal(candidate)
                assert caught.value is failure
                assert events == ["open", "close"]
            else:
                result = terminal(candidate)
                assert result == expected
                assert events == ["open", "first", "close"]
        events.clear()
        with pytest.raises(pa.ArrowInvalid) as caught:
            candidate.to_list()
        assert caught.value is failure
        assert events == (
            ["open", "close"] if fail_before_first else ["open", "first", "later", "close"]
        )


@pytest.mark.parametrize("public_columns", [False, True])
def test_parquet_supplied_dataset_schema_keeps_its_cast_boundary(
    tmp_path: Path, public_columns: bool
) -> None:
    target = tmp_path / "cast.parquet"
    pq.write_table(pa.table({"id": [1], "unused": ["bad"]}), target)
    source = ds.dataset(
        target, format="parquet", schema=pa.schema([("id", pa.int64()), ("unused", pa.int64())])
    )
    query = fpstreams.rows.from_parquet(source, columns=["id"] if public_columns else None).where(
        fpstreams.col("id") >= 0
    )
    if public_columns:
        assert query.count() == query.with_engine("python").count() == 1
    else:
        for candidate in (query.with_engine("python"), query):
            with pytest.raises(pa.ArrowInvalid, match="Failed to parse string"):
                candidate.count()


@pytest.mark.parametrize("count", [0, 1, 2, 3])
def test_parquet_single_fragment_probe_bounds_and_closes_enumeration(count: int) -> None:

    events = []

    class Fragment:
        @property
        def physical_schema(self):
            raise AssertionError("schema proof must not deserialize extension metadata")

        @property
        def metadata(self):
            raise AssertionError("schema proof must not load metadata")

    class Dataset:
        def get_fragments(self):
            try:
                for index in range(count):
                    events.append(index)
                    yield Fragment()
            finally:
                events.append("closed")

    assert adapter._parquet_has_single_fragment(Dataset()) is (count == 1)
    assert events == [*range(min(count, 2)), "closed"]


@pytest.mark.parametrize("adapter", ["retained", "parquet"])
@pytest.mark.parametrize("keep_extension", [False, True])
@pytest.mark.parametrize("filter_count", [1, 2])
@pytest.mark.parametrize("terminal", ["to_list", "count", "to_arrow", "arrow_batches", "first"])
def test_arrow_filter_projection_converts_extension_scalars_once(
    tmp_path: Path, adapter: str, keep_extension: bool, filter_count: int, terminal: str
) -> None:
    """A speculative prefix must not repeat or skip Python extension conversion hooks."""
    conversions: list[int] = []

    class TracedScalar(pa.ExtensionScalar):
        def as_py(self, **_kwargs: object) -> int:
            value = self.value.as_py()
            conversions.append(value)
            return value

    class TracedType(pa.ExtensionType):
        def __init__(self) -> None:
            super().__init__(pa.int64(), "fpstreams.test.prefix-conversion")

        def __arrow_ext_serialize__(self) -> bytes:
            return b""

        @classmethod
        def __arrow_ext_deserialize__(cls, _storage: object, _metadata: bytes) -> TracedType:
            return cls()

        def __arrow_ext_scalar_class__(self) -> type[TracedScalar]:
            return TracedScalar

    extension = TracedType()
    pa.register_extension_type(extension)
    try:
        table = pa.table(
            {"id": [0, 1], "value": pa.ExtensionArray.from_storage(extension, pa.array([10, 20]))}
        )
        if adapter == "parquet":
            target = tmp_path / "extension.parquet"
            pq.write_table(table, target)
            source = fpstreams.rows.from_parquet(target)
        else:
            source = fpstreams.rows.from_arrow(table)
        query = source.where(fpstreams.col("id") >= 0)
        if filter_count == 2:
            query = query.where(fpstreams.col("id") < 2)
        query = query.select("value" if keep_extension else "id")
        outputs = []
        observations = []
        for candidate in (query.with_engine("python"), query):
            conversions.clear()
            result = getattr(candidate, terminal)()
            if terminal == "arrow_batches":
                result = list(result)
            if terminal in {"arrow_batches", "to_arrow"}:
                result = (
                    [row for batch in result for row in batch.to_pylist()]
                    if terminal == "arrow_batches"
                    else result.to_pylist()
                )
            outputs.append(result)
            observations.append(list(conversions))
        assert outputs[0] == outputs[1]
        assert observations == [[10, 20], [10, 20]]
    finally:
        pa.unregister_extension_type(extension.extension_name)


def test_parquet_explicit_source_hints_define_the_conversion_boundary(tmp_path: Path) -> None:
    bad = pa.array([b"\xff", b"ok"], type=pa.binary()).view(pa.string())
    target = tmp_path / "explicit-hints.parquet"
    pq.write_table(pa.table({"id": [0, 1], "bad": bad}), target, row_group_size=1)
    projected = fpstreams.rows.from_parquet(target, columns=["id"])
    filtered = fpstreams.rows.from_parquet(target, filter=ds.field("id") == 1)
    for source, expected in ((projected, [{"id": 0}, {"id": 1}]), (filtered, [{"id": 1}])):
        query = source.where(fpstreams.col("id") >= 0).where(fpstreams.col("id") < 2).select("id")
        assert query.to_list() == query.with_engine("python").to_list() == expected


@pytest.mark.parametrize("replaced_opener", [False, True])
def test_parquet_invalidated_projection_also_discards_automatic_predicate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, replaced_opener: bool
) -> None:
    from fpstreams.planning.arrow.source import ArrowScanRequest

    target = tmp_path / "invalidated-hints.parquet"
    pq.write_table(pa.table({"id": [0, 1], "value": [10, 20]}), target, row_group_size=1)
    _base, _size, _projected, requested, _count = adapter._parquet_batch_factory(target)
    if replaced_opener:
        original = ds.dataset
        monkeypatch.setattr(ds, "dataset", lambda *args, **kwargs: original(*args, **kwargs))
    request = ArrowScanRequest(
        columns=("value",),
        range_predicate=("id", ">=", 1),
        projection_check=lambda: replaced_opener,
    )
    assert [row for batch in requested(request) for row in batch.to_pylist()] == [
        {"id": 0, "value": 10},
        {"id": 1, "value": 20},
    ]


@pytest.mark.parametrize("third_filter", [False, True])
def test_parquet_filter_chain_projection_keeps_only_leading_scan_hint(
    tmp_path, monkeypatch, third_filter
):
    """A longer safe prefix reads required fields, using only the original first range."""

    target = tmp_path / "prefix.parquet"
    pq.write_table(
        pa.table(
            {
                "id": list(range(40)),
                "payload": [f"v{x}" for x in range(40)],
                "unused": list(range(40)),
            }
        ),
        target,
        row_group_size=10,
    )
    batches = []
    execute = execution._execute_batch_program

    def tracked(batch, operations, projection):
        batches.append((batch.num_rows, tuple(batch.schema.names), len(operations)))
        yield from execute(batch, operations, projection)

    monkeypatch.setattr(execution, "_execute_batch_program", tracked)
    query = fpstreams.rows.from_parquet(target).where(fpstreams.col("id") >= 35)
    query = query.where(fpstreams.col("id") < 39)
    if third_filter:
        query = query.where(fpstreams.col("payload") != "v37")
    assert query.select(value="payload").to_list() == (
        [{"value": "v35"}, {"value": "v36"}, {"value": "v38"}]
        if third_filter
        else [{"value": "v35"}, {"value": "v36"}, {"value": "v37"}, {"value": "v38"}]
    )
    assert batches == [(5, ("payload", "id"), 4 if third_filter else 3)]


def test_retained_arrow_filter_chain_projection_stays_columnar_with_explicit_schema(monkeypatch):

    table = pa.table({"id": [0, 1, 2, 3], "value": [10, 20, 30, 40]}).replace_schema_metadata(
        {b"origin": b"test"}
    )
    schema = pa.schema([("chosen", pa.int64())])
    calls = []
    materialize = execution._materialize_table_batch

    def tracked(batch, operations, projection):
        calls.append(batch.num_rows)
        return materialize(batch, operations, projection)

    monkeypatch.setattr(execution, "_materialize_table_batch", tracked)
    batches = (
        fpstreams.rows.from_arrow(table)
        .where(fpstreams.col("id") >= 1)
        .where(fpstreams.col("id") < 3)
        .select(chosen="value")
        .arrow_batches(schema=schema)
        .to_list()
    )
    assert [row for batch in batches for row in batch.to_pylist()] == [
        {"chosen": 20},
        {"chosen": 30},
    ]
    assert all(batch.schema.equals(schema, check_metadata=True) for batch in batches)
    assert calls == [4]


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
def test_parquet_exact_i64_range_narrows_batches_before_the_python_residual(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    predicate: object,
    expected: list[int],
) -> None:
    """Four direct ranges, including literal-left forms, narrow validated scan outputs."""

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


def test_parquet_equality_narrows_validated_batches_across_multiple_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The bounded metadata probe considers every fragment before enabling its hint."""

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


@pytest.mark.parametrize(
    ("row_groups", "retained_groups", "single_fragment", "expected"),
    [
        pytest.param(1, 0, False, True, id="proven-pruning"),
        pytest.param(1, 1, False, False, id="possible-match"),
        pytest.param(2, 1, True, True, id="single-file-partial-pruning"),
    ],
)
def test_parquet_first_equality_probe_does_not_pull_fragment_tail(
    row_groups: int, retained_groups: int, single_fragment: bool, expected: bool
) -> None:
    """A leading fragment decides the first() hint without requesting tail metadata."""
    calls: list[str] = []

    class Fragment:
        num_row_groups = row_groups

        def split_by_row_group(self, *_args: object, **_options: object) -> list[object]:
            calls.append("split:first")
            return [object() for _ in range(retained_groups)]

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
    assert (
        arrow_adapter._parquet_predicate_can_prune(
            Dataset(),
            object(),
            dataset_module,
            probe_statistics=True,
            first_only=True,
            single_fragment=single_fragment,
        )
        is expected
    )
    assert calls == ["next:first", "split:first"]


def test_parquet_first_known_single_file_prunes_late_row_groups(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The no-tail metadata policy retains selective first() scans for one local file."""

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

    from fpstreams.planning.arrow.source import ArrowScanRequest

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

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)

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

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)
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

    left = pa.table({"left_id": [1, 2, 99, 3], "left_value": [10, 20, 990, 30]})
    unused = list(range(1_000, 1_125))
    right = pa.table(
        {
            "right_id": [1, 2, 3, *unused],
            "right_value": [100, 200, 300, *unused],
        }
    )

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)
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

    payload = "right-payload-that-is-long-enough-to-avoid-interning"
    left = pa.table({"left_id": [1, 1], "left_value": [10, 11]})
    unused = list(range(1_000, 1_125))
    right = pa.table(
        {
            "right_id": [1, *unused],
            "right_value": [payload, *(f"unused-{value}" for value in unused)],
        }
    )

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", _forbid_row_boxing)

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


def test_parquet_error_publish_does_not_overwrite_racing_creator(tmp_path, monkeypatch):

    target = tmp_path / "race.parquet"
    sentinel = b"created by another writer"
    replace, link = arrow.os.replace, arrow.os.link

    def raced_replace(source, destination, **kwargs):
        target.write_bytes(sentinel)
        return replace(source, destination, **kwargs)

    def raced_link(source, destination, **kwargs):
        target.write_bytes(sentinel)
        return link(source, destination, **kwargs)

    monkeypatch.setattr(arrow.os, "replace", raced_replace)
    monkeypatch.setattr(arrow.os, "link", raced_link)
    with pytest.raises(FileExistsError):
        fpstreams.rows([{"id": 1}]).to_parquet(target)
    assert target.read_bytes() == sentinel
    assert list(tmp_path.iterdir()) == [target]


@pytest.mark.parametrize("primary", [False, True])
@pytest.mark.parametrize("sink", ["arrow", "parquet"])
def test_arrow_owned_source_close_failure_is_not_silent(tmp_path, primary, sink):

    error = ValueError("source failed")
    cleanup = OSError("source close failed")
    closed = []

    class Source:
        def __iter__(self):
            return self

        def __next__(self):
            if primary:
                raise error
            raise StopIteration

        def close(self):
            closed.append(True)
            raise cleanup

    schema = pa.schema([("id", pa.int64())])
    with pytest.raises(ValueError if primary else OSError) as caught:
        if sink == "arrow":
            arrow.table_from_rows(Source(), schema=schema, as_record=lambda x: x)
        else:
            arrow.write_parquet_rows(
                Source(), tmp_path / "failed.parquet", schema=schema, as_record=lambda x: x
            )
    assert caught.value is (error if primary else cleanup)
    if primary:
        assert any("source close failed" in note for note in error.__notes__)
    assert closed == [True]
    assert list(tmp_path.iterdir()) == []


def test_arrow_cleanup_attempts_all_resources_and_preserves_ambient_exception():

    events = []
    first, second = OSError("first close"), ValueError("second close")

    class Resource:
        def __init__(self, error):
            self.error = error

        def close(self):
            events.append(self.error)
            raise self.error

    try:
        raise RuntimeError("ambient handled error")
    except RuntimeError:
        with pytest.raises(OSError) as caught, arrow._closing(Resource(first), Resource(second)):
            pass
    assert caught.value is first
    assert events == [first, second]
    assert any("second close" in note for note in first.__notes__)


def test_arrow_cleanup_keeps_primary_and_closes_all_resources():

    primary = ValueError("primary query failure")
    closed = []

    class Resource:
        def close(self):
            closed.append(True)
            raise OSError("cleanup failure")

    with pytest.raises(ValueError) as caught, arrow._closing(Resource(), Resource()):
        raise primary
    assert caught.value is primary
    assert closed == [True, True]
    assert len(primary.__notes__) == 2


def test_parquet_error_rejects_dangling_symlink_before_consuming(tmp_path):
    target = tmp_path / "target.parquet"
    try:
        target.symlink_to(tmp_path / "absent")
    except (OSError, NotImplementedError):
        pytest.skip("symlinks unavailable")
    consumed = []

    def source():
        consumed.append(True)
        yield {"id": 1}

    with pytest.raises(FileExistsError):
        fpstreams.rows(source()).to_parquet(target)
    assert consumed == []
    assert target.is_symlink()


def test_parquet_writer_close_failure_prevents_publish_and_removes_temporary(tmp_path, monkeypatch):

    real = pq.ParquetWriter
    failure = OSError("writer finalization failed")
    closes = []

    class Writer:
        def __init__(self, *args, **kwargs):
            self.writer = real(*args, **kwargs)

        def write_batch(self, *args, **kwargs):
            return self.writer.write_batch(*args, **kwargs)

        def close(self):
            closes.append(True)
            self.writer.close()
            raise failure

    monkeypatch.setattr(pq, "ParquetWriter", Writer)
    with pytest.raises(OSError) as caught:
        arrow.write_parquet_rows([{"id": 1}], tmp_path / "target.parquet", as_record=lambda x: x)
    assert caught.value is failure
    assert closes == [True]
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("source_kind", ["table", "record_batch"])
@pytest.mark.parametrize("explicit_schema", [False, True])
def test_matching_arrow_batch_schema_preserves_native_buffers(source_kind, explicit_schema) -> None:
    """An unchanged explicit schema must not force retained Arrow data through Python rows."""
    table = pa.table({"id": [1, 2, 3], "value": [1.5, None, -0.0], "name": ["a", "b", "c"]})
    source = table if source_kind == "table" else table.to_batches()[0]
    batches = (
        fpstreams.rows.from_arrow(source)
        .arrow_batches(batch_size=2, schema=table.schema if explicit_schema else None)
        .to_list()
    )
    assert [batch.num_rows for batch in batches] == [2, 1]
    assert pa.Table.from_batches(batches).equals(table, check_metadata=True)
    assert (
        batches[0].column(0).buffers()[1].address == table.column(0).chunk(0).buffers()[1].address
    )


@pytest.mark.parametrize(
    "override", ["rows_subclass", "flow_subclass", "rows_method", "flow_method"]
)
@pytest.mark.parametrize("explicit_schema", [False, True])
def test_arrow_batches_preserve_overridden_iteration(
    override, explicit_schema, monkeypatch
) -> None:
    table = pa.table({"value": [1, 2, 3]})
    source = fpstreams.rows.from_arrow(table)
    original_rows_iter = fpstreams.Rows.__iter__
    original_flow_iter = fpstreams.Flow.__iter__

    def row_iter(self):
        for row in original_rows_iter(self):
            yield {"value": row["value"] + 10}

    def flow_iter(self):
        for row in original_flow_iter(self):
            yield {"value": row["value"] + 10}

    if override == "rows_subclass":

        class CustomRows(fpstreams.Rows):
            __iter__ = row_iter

        source = CustomRows(source._flow)
    elif override == "flow_subclass":

        class CustomFlow(fpstreams.Flow):
            __iter__ = flow_iter

        source = fpstreams.Rows(CustomFlow._from_logical(source._flow._logical_plan))
    elif override == "rows_method":
        monkeypatch.setattr(fpstreams.Rows, "__iter__", row_iter)
    else:
        monkeypatch.setattr(fpstreams.Flow, "__iter__", flow_iter)
    batches = source.arrow_batches(
        batch_size=2, schema=table.schema if explicit_schema else None
    ).to_list()
    assert pa.Table.from_batches(batches).to_pylist() == [
        {"value": 11},
        {"value": 12},
        {"value": 13},
    ]

    assert [batch.num_rows for batch in batches] == [2, 1]


@pytest.mark.parametrize("mode", ["cast", "projection", "callback", "metadata"])
def test_explicit_arrow_batch_schema_preserves_conversion_and_callback_semantics(mode) -> None:
    table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    source = fpstreams.rows.from_arrow(table)
    schema = table.schema
    calls = []
    if mode == "cast":
        schema = pa.schema([("id", pa.int32()), ("name", pa.string())])
    elif mode == "projection":
        source = source.select("id")
        schema = pa.schema([("id", pa.int64())])
    elif mode == "callback":
        source = source.map(lambda row: calls.append(row["id"]) or row).rows()
    else:
        schema = table.schema.with_metadata({b"example": b"retained"})
    batches = source.arrow_batches(batch_size=2, schema=schema).to_list()
    assert all(batch.schema.equals(schema, check_metadata=True) for batch in batches)
    assert [batch.num_rows for batch in batches] == [2, 1]
    assert [row["id"] for batch in batches for row in batch.to_pylist()] == [1, 2, 3]
    assert calls == ([1, 2, 3] if mode == "callback" else [])


@pytest.mark.parametrize("operation", ["select", "where", "where_select"])
@pytest.mark.parametrize("source_kind", ["table", "record_batch"])
def test_explicit_arrow_batch_schema_executes_safe_prefix_columnarly(
    operation, source_kind, monkeypatch
):

    table = pa.table({"id": [1, 2, 3, 4], "value": [None, 8, 9, 10]})
    data = table if source_kind == "table" else table.to_batches()[0]
    source = fpstreams.rows.from_arrow(data, batch_size=2)
    schema = table.schema
    if "where" in operation:
        source = source.where(fpstreams.col("id") > 1)
    if "select" in operation:
        source = source.select(score="value")
        schema = pa.schema([("score", pa.int64())])
    expected = source.with_engine("python").arrow_batches(batch_size=3, schema=schema).to_list()
    calls = []
    original = execution._materialize_table_batch

    def tracked(*args):
        calls.append(args[0].num_rows)
        return original(*args)

    monkeypatch.setattr(execution, "_materialize_table_batch", tracked)
    actual = source.arrow_batches(batch_size=3, schema=schema).to_list()
    assert calls == [2, 2]
    assert [batch.to_pylist() for batch in actual] == [batch.to_pylist() for batch in expected]
    assert all(batch.schema.equals(schema, check_metadata=True) for batch in actual)


def test_explicit_arrow_prefix_observes_midstream_conversion_hook(monkeypatch):

    table = pa.table({"id": [1, 2, 3, 4], "value": [5, 6, 7, 8]})
    source = fpstreams.rows.from_arrow(table, batch_size=2).select("value")
    schema = pa.schema([("value", pa.int64())])
    iterator = iter(source.arrow_batches(batch_size=2, schema=schema))
    assert next(iterator).to_pylist() == [{"value": 5}, {"value": 6}]
    calls = []
    original = adapter.batch_to_rows

    def changed(batch):
        calls.append(batch.num_rows)
        return [{**row, "value": row["value"] + 10} for row in original(batch)]

    monkeypatch.setattr(adapter, "batch_to_rows", changed)
    assert next(iterator).to_pylist() == [{"value": 17}, {"value": 18}]
    with pytest.raises(StopIteration):
        next(iterator)
    assert calls == [2]


@pytest.mark.parametrize("operation", ["select", "where_select"])
def test_explicit_arrow_prefix_does_not_hide_invalid_dropped_values(operation):

    invalid = pa.Array.from_buffers(
        pa.string(),
        1,
        [None, pa.py_buffer(struct.pack("<ii", 0, 1)), pa.py_buffer(b"\xff")],
    )
    source = fpstreams.rows.from_arrow(pa.table({"id": [1], "bad": invalid}))
    if operation == "where_select":
        source = source.where(fpstreams.col("id") > 2)
    source = source.select("id")
    with pytest.raises(UnicodeDecodeError):
        source.arrow_batches(schema=pa.schema([("id", pa.int64())])).to_list()


def test_explicit_arrow_prefix_preserves_nullable_predicate_errors_and_source_metadata():
    schema = pa.schema(
        [pa.field("id", pa.int64(), metadata={b"unit": b"count"})], metadata={b"source": b"example"}
    )
    table = pa.Table.from_pydict({"id": [1, None, 3]}, schema=schema)
    query = fpstreams.rows.from_arrow(table).where(fpstreams.col("id") > 1)
    with pytest.raises(TypeError):
        query.arrow_batches(schema=schema).to_list()
    selected = fpstreams.rows.from_arrow(table).select("id")
    output_schema = pa.schema([schema.field("id")])
    actual = selected.arrow_batches(batch_size=2, schema=output_schema).to_list()
    assert [batch.to_pylist() for batch in actual] == [[{"id": 1}, {"id": None}], [{"id": 3}]]
    assert all(batch.schema.equals(output_schema, check_metadata=True) for batch in actual)


@pytest.mark.parametrize("mutation", ["reorder", "dtype", "extra"])
def test_explicit_arrow_prefix_revalidates_each_batch_schema(mutation):
    table = pa.table({"id": [1, 2, 3, 4], "value": [10, 20, 30, 40]})
    source = fpstreams.rows.from_arrow(table, batch_size=2)
    first = table.slice(0, 2).to_batches()[0]
    second = table.slice(2, 2)
    if mutation == "reorder":
        second = second.select(["value", "id"])
    elif mutation == "dtype":
        second = second.cast(pa.schema([("id", pa.int64()), ("value", pa.float64())]))
    else:
        second = second.append_column("extra", pa.array([1, 2]))
    descriptor = source._flow._pipeline.source.native_data
    object.__setattr__(descriptor, "opener", lambda: iter((first, second.to_batches()[0])))
    query = source.where(fpstreams.col("id") > 0)
    if mutation == "extra":
        for engine in ("python", "auto"):
            iterator = iter(
                query.with_engine(engine).arrow_batches(schema=table.schema, batch_size=2)
            )
            assert next(iterator).to_pylist() == [{"id": 1, "value": 10}, {"id": 2, "value": 20}]
            with pytest.raises(ValueError, match="outside the schema"):
                next(iterator)
    else:
        for engine in ("python", "auto"):
            actual = (
                query.with_engine(engine).arrow_batches(schema=table.schema, batch_size=2).to_list()
            )
            assert [row for batch in actual for row in batch.to_pylist()] == table.to_pylist()
            assert all(batch.schema.equals(table.schema, check_metadata=True) for batch in actual)


@pytest.mark.parametrize("engine", ["auto", "python"])
@pytest.mark.parametrize("source_kind", ["table", "record_batch", "records"])
def test_explicit_arrow_batch_schema_preserves_zero_column_cardinality(engine, source_kind) -> None:
    schema = pa.schema([], metadata={b"source": b"empty-fields"})
    table = pa.table({"value": [1, 2, 3]}).replace_schema_metadata(schema.metadata).select([])
    source = fpstreams.rows(iter([{}, {}, {}]))
    if source_kind != "records":
        source = fpstreams.rows.from_arrow(
            table if source_kind == "table" else table.to_batches()[0]
        )
    actual = source.with_engine(engine).arrow_batches(batch_size=2, schema=schema).to_list()
    assert [batch.num_rows for batch in actual] == [2, 1]
    assert [row for batch in actual for row in batch.to_pylist()] == [{}, {}, {}]
    assert all(batch.schema.equals(schema, check_metadata=True) for batch in actual)


def test_zero_column_arrow_schema_preserves_table_rows_and_rejects_extra_fields() -> None:
    schema = pa.schema([])
    assert fpstreams.rows([{}, {}, {}]).to_arrow(schema=schema, batch_size=2).num_rows == 3
    assert fpstreams.rows([]).arrow_batches(schema=schema).to_list() == []
    assert fpstreams.rows([]).to_arrow(schema=schema).num_rows == 0
    with pytest.raises(ValueError, match="outside the schema"):
        fpstreams.rows([{}, {"extra": 1}]).arrow_batches(schema=schema).to_list()
    with pytest.raises(ValueError, match="without columns"):
        fpstreams.rows([{}]).arrow_batches().to_list()

    class InvalidSchema:
        names = ()
        metadata = None

    with pytest.raises(TypeError):
        fpstreams.rows([{}]).arrow_batches(schema=InvalidSchema()).to_list()


@pytest.mark.parametrize("existing", [False, True])
def test_zero_column_parquet_write_fails_without_publishing_or_replacing(existing, tmp_path):
    target = tmp_path / "rows.parquet"
    if existing:
        fpstreams.rows([{"id": 7}]).to_parquet(target)
    before = target.read_bytes() if existing else None
    events = []

    def source():
        try:
            yield {}
            yield {}
        finally:
            events.append("closed")

    with pytest.raises(ValueError, match="without columns"):
        fpstreams.rows(source()).to_parquet(target, schema=pa.schema([]), if_exists="replace")
    assert events == ["closed"]
    assert (target.read_bytes() if target.exists() else None) == before
    assert sorted(tmp_path.iterdir()) == ([target] if existing else [])
    empty = tmp_path / "empty.parquet"
    assert fpstreams.rows([]).to_parquet(empty, schema=pa.schema([])) == 0
    assert fpstreams.rows.from_parquet(empty).to_list() == []


@pytest.mark.parametrize("late", [False, True])
def test_explicit_arrow_batch_schema_does_not_skip_invalid_utf8_errors(late) -> None:

    payload = bytearray(b"a" if late else b"\xff")
    invalid = pa.Array.from_buffers(
        pa.string(),
        1,
        [None, pa.py_buffer(struct.pack("<ii", 0, 1)), pa.py_buffer(payload)],
    )
    table = pa.table({"value": invalid})
    batches = fpstreams.rows.from_arrow(table).arrow_batches(schema=table.schema)
    if late:
        payload[0] = 255
    with pytest.raises(UnicodeDecodeError):
        batches.to_list()


@pytest.mark.parametrize("late", [False, True])
def test_explicit_arrow_batch_schema_keeps_changed_row_conversion_observable(
    late, monkeypatch
) -> None:

    table = pa.table({"value": [1, 2, 3]})
    source = fpstreams.rows.from_arrow(table)
    error = ValueError("row conversion hook")
    batches = source.arrow_batches(schema=table.schema) if late else None

    def reject(batch):
        raise error

    monkeypatch.setattr(adapter, "batch_to_rows", reject)
    with pytest.raises(ValueError, match="row conversion hook") as caught:
        (batches if late else source.arrow_batches(schema=table.schema)).to_list()
    assert caught.value is error


@pytest.mark.parametrize("owner", [fpstreams.Rows, fpstreams.Flow])
@pytest.mark.parametrize("explicit_schema", [False, True])
@pytest.mark.parametrize("after_iter", [False, True])
def test_arrow_batches_observe_iteration_changes_before_consumption(
    owner, explicit_schema, after_iter, monkeypatch
) -> None:
    table = pa.table({"value": [1, 2, 3]})
    source = fpstreams.rows.from_arrow(table)
    batches = source.arrow_batches(schema=table.schema if explicit_schema else None)
    iterator = iter(batches) if after_iter else None
    original = owner.__iter__

    def changed(self):
        for row in original(self):
            yield {"value": row["value"] + 10}

    monkeypatch.setattr(owner, "__iter__", changed)
    try:
        actual = list(iterator) if iterator is not None else batches.to_list()
        assert pa.Table.from_batches(actual).to_pylist() == [
            {"value": 11},
            {"value": 12},
            {"value": 13},
        ]
    finally:
        if iterator is not None:
            iterator.close()


@pytest.mark.parametrize(
    "scalar_type",
    [
        pa.int8(),
        pa.int64(),
        pa.uint64(),
        pa.float32(),
        pa.float64(),
        pa.bool_(),
        pa.string(),
        pa.large_string(),
        pa.binary(),
        pa.large_binary(),
        pa.null(),
        pa.list_(pa.int64()),
        pa.dictionary(pa.int8(), pa.string()),
    ],
)
def test_explicit_arrow_batch_schema_matches_python_conversion_for_scalar_families(
    scalar_type,
) -> None:
    if pa.types.is_boolean(scalar_type):
        values = [True, None, False]
    elif (
        pa.types.is_string(scalar_type)
        or pa.types.is_large_string(scalar_type)
        or pa.types.is_dictionary(scalar_type)
    ):
        values = ["one", None, "two"]
    elif pa.types.is_binary(scalar_type) or pa.types.is_large_binary(scalar_type):
        values = [b"one", None, b"two"]
    elif pa.types.is_null(scalar_type):
        values = [None, None, None]
    elif pa.types.is_list(scalar_type):
        values = [[1, 2], None, []]
    elif pa.types.is_floating(scalar_type):
        values = [-0.0, None, 1.25]
    elif pa.types.is_uint64(scalar_type):
        values = [2**64 - 1, None, 0]
    else:
        values = [1, None, 2]
    table = pa.table({"value": pa.array(values, type=scalar_type)})
    source = fpstreams.rows.from_arrow(table)
    expected = (
        source.with_engine("python").arrow_batches(batch_size=2, schema=table.schema).to_list()
    )
    actual = source.arrow_batches(batch_size=2, schema=table.schema).to_list()
    assert [batch.num_rows for batch in actual] == [batch.num_rows for batch in expected]
    assert all(
        a.schema.equals(b.schema, check_metadata=True)
        for a, b in zip(actual, expected, strict=True)
    )
    assert pa.Table.from_batches(actual).to_pylist() == pa.Table.from_batches(expected).to_pylist()


def test_explicit_arrow_batch_schema_observes_conversion_changes_between_source_batches(
    monkeypatch,
) -> None:

    table = pa.table({"value": [1, 2, 3]})
    batches = fpstreams.rows.from_arrow(table, batch_size=1).arrow_batches(
        batch_size=1, schema=table.schema
    )
    iterator = iter(batches)
    try:
        assert next(iterator).to_pylist() == [{"value": 1}]

        def reject(batch):
            raise ValueError("changed conversion between batches")

        monkeypatch.setattr(adapter, "batch_to_rows", reject)
        with pytest.raises(ValueError, match="between batches"):
            next(iterator)
    finally:
        iterator.close()


def test_explicit_arrow_batch_schema_validates_mutable_buffers_between_source_batches() -> None:

    payload = bytearray(b"ab")
    array = pa.Array.from_buffers(
        pa.string(),
        2,
        [None, pa.py_buffer(struct.pack("<iii", 0, 1, 2)), pa.py_buffer(payload)],
    )
    table = pa.table({"value": array})
    batches = fpstreams.rows.from_arrow(table, batch_size=1).arrow_batches(
        batch_size=1, schema=table.schema
    )
    iterator = iter(batches)
    try:
        assert next(iterator).to_pylist() == [{"value": "a"}]
        payload[1] = 255
        with pytest.raises(UnicodeDecodeError):
            next(iterator)
    finally:
        iterator.close()


@pytest.mark.parametrize("retained", ["table", "batch"])
def test_arrow_shared_masks_preserve_results_without_repeated_payload_filters(
    monkeypatch, retained
):

    values = list(range(2048))
    table = pa.table({f"v{column}": [value + column for value in values] for column in range(12)})
    data = table if retained == "table" else table.to_batches()[0]
    query = (
        fpstreams.Rows.from_arrow(data)
        .where(fpstreams.col("v0") >= 17)
        .where(fpstreams.col("v1") < 2000)
    )
    expected = [{f"v{column}": value + column for column in range(12)} for value in range(17, 1999)]
    monkeypatch.setattr(execution, "_COMBINED_FILTER_MIN_ROWS", 1)
    monkeypatch.setattr(execution, "_COMBINED_FILTER_MIN_COLUMNS", 1)
    _pa, pc = execution._arrow_modules()
    original = pc.filter
    filtered = []

    def record_filter(batch, mask, *args, **kwargs):
        filtered.append(batch.num_rows)
        return original(batch, mask, *args, **kwargs)

    monkeypatch.setattr(pc, "filter", record_filter)
    assert query.count() == len(expected)
    assert filtered == []
    assert query.to_arrow().to_pylist() == expected
    assert len(filtered) == 1
    filtered.clear()
    assert query.select(left="v0", right="v0").to_list() == [
        {"left": row["v0"], "right": row["v0"]} for row in expected
    ]
    assert len(filtered) == 1


def test_arrow_shared_mask_thresholds_keep_empty_batches_and_unsafe_callbacks_safe(monkeypatch):

    monkeypatch.setattr(execution, "_COMBINED_FILTER_MIN_ROWS", 0)
    monkeypatch.setattr(execution, "_COMBINED_FILTER_MIN_COLUMNS", 0)
    empty = pa.record_batch({"v": pa.array([], type=pa.int64())})
    query = (
        fpstreams.Rows.from_arrow(empty).where(fpstreams.col("v") > 0).where(fpstreams.col("v") < 9)
    )
    assert query.count() == 0
    assert query.to_list() == []

    calls = []
    failure = LookupError("callback must retain its original row boundary")

    def callback(row):
        calls.append(row["v"])
        if row["v"] == 2:
            raise failure
        return True

    query = (
        fpstreams.Rows.from_arrow(pa.table({"v": [1, 2, 3]}))
        .where(fpstreams.col("v") >= 2)
        .where(callback)
        .where(fpstreams.col("v") < 2)
    )
    with pytest.raises(LookupError) as caught:
        query.to_list()
    assert caught.value is failure and calls == [2]


@pytest.mark.parametrize("retained", ["table", "batch"])
def test_arrow_transformed_list_collects_prefix_once_and_preserves_batch_observers(
    monkeypatch, retained
):
    from contextlib import contextmanager

    from fpstreams.planning.source import Source

    table = pa.table({"x": [1, 2, 3], "unused": [4, 5, 6]})
    data = table if retained == "table" else table.to_batches()[0]
    query = (
        fpstreams.rows.from_arrow(data, batch_size=1)
        .where(fpstreams.col("x") > 0)
        .select(first="x", second="x")
    )
    events = []
    original_prefix = execution.execute_arrow_prefix
    original_open = Source.open_native
    original_observe = execution.observe_arrow_batch_rows
    original_closing = execution.closing_iterators
    error = ValueError("batch observer failed")

    def prefix(*args, **kwargs):
        assert isinstance(kwargs.get("_collect"), list)
        events.append("collect")
        return original_prefix(*args, **kwargs)

    def opened(self, expected_type):
        events.append("open")
        return original_open(self, expected_type)

    def observe(batch):
        events.append("observe")
        if events.count("observe") == 2:
            raise error
        return original_observe(batch)

    @contextmanager
    def closing(iterators):
        try:
            with original_closing(iterators):
                yield
        finally:
            events.append("closed")

    monkeypatch.setattr(execution, "execute_arrow_prefix", prefix)
    monkeypatch.setattr(Source, "open_native", opened)
    monkeypatch.setattr(execution, "observe_arrow_batch_rows", observe)
    monkeypatch.setattr(execution, "closing_iterators", closing)
    with pytest.raises(ValueError, match="batch observer failed") as caught:
        query.to_list()
    assert caught.value is error
    assert events == ["collect", "open", "observe", "observe", "closed"]


@pytest.mark.parametrize("nested", [False, True])
def test_arrow_transformed_lists_keep_alias_identity_and_independent_replays(nested):
    payload = [1, 2] if nested else b"payload-" * 256
    query = fpstreams.rows.from_arrow(pa.table({"x": [payload, payload]})).select(
        first="x", second="x"
    )
    first, second = query.to_list(), query.to_list()
    assert first == second == [{"first": payload, "second": payload}] * 2
    for row in first:
        assert list(row) == ["first", "second"]
        assert row["first"] is row["second"]
    assert first is not second
    assert first[0] is not first[1] and first[0] is not second[0]
    if nested:
        first[0]["first"].append(3)
        assert first[1]["first"] == second[0]["first"] == [1, 2]


def test_arrow_transformed_list_keeps_conversion_error_identity(monkeypatch):

    query = fpstreams.rows.from_arrow(pa.table({"x": [{"a": 1}]})).select("x")
    error = ValueError("conversion observer")

    def reject(batch):
        raise error

    monkeypatch.setattr(adapter, "batch_to_rows", reject)
    with pytest.raises(ValueError, match="conversion observer") as caught:
        query.to_list()
    assert caught.value is error


@pytest.mark.parametrize("operation", ["select", "filter_select"])
def test_arrow_transformed_list_cannot_discard_invalid_dropped_utf8(operation):
    invalid = pa.array([b"\xff"], type=pa.binary()).view(pa.string())
    query = fpstreams.rows.from_arrow(pa.table({"x": [1], "unused": invalid}))
    if operation == "filter_select":
        query = query.where(fpstreams.col("x") > 9)
    with pytest.raises(UnicodeDecodeError):
        query.select("x").to_list()
