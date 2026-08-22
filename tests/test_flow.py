"""Synchronous Flow sources, lazy transforms, selectors, gatherers, and terminals."""

from __future__ import annotations

import json
import math
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


def test_flow_row_bridge_matches_rows_projection_enrichment_and_grouping() -> None:
    records = [
        {"team": "red", "score": 3},
        {"team": "blue", "score": 5},
        {"team": "red", "score": 7},
    ]

    assert (
        flow(records).select("team", points="score").to_list()
        == fpstreams.rows(records).select("team", points="score").to_list()
    )
    assert (
        flow(records).with_columns(doubled=lambda row: row["score"] * 2).to_list()
        == fpstreams.rows(records).with_columns(doubled=lambda row: row["score"] * 2).to_list()
    )
    assert (
        flow(records)
        .group_by("team")
        .aggregate(count=fpstreams.agg.count(), total=fpstreams.agg.sum("score"))
        .to_list()
        == fpstreams.rows(records)
        .group_by("team")
        .aggregate(count=fpstreams.agg.count(), total=fpstreams.agg.sum("score"))
        .to_list()
    )


def test_flow_row_bridge_does_not_probe_a_generator() -> None:
    events: list[str] = []

    def records() -> Iterator[dict[str, int]]:
        events.append("open")
        yield {"id": 1}

    pipeline = flow(records())
    row_view = pipeline.rows()
    projected = pipeline.select("id")
    enriched = pipeline.with_columns(copy="id")
    grouped = pipeline.group_by("id").aggregate(count=fpstreams.agg.count())

    assert row_view is not None
    assert projected is not None
    assert enriched is not None
    assert grouped is not None
    assert events == []


def test_flow_and_rows_bridge_share_one_shot_source_claim() -> None:
    pipeline = flow(iter([{"id": 1}, {"id": 2}]))
    row_view = pipeline.rows()
    projected = pipeline.select("id")

    assert row_view._flow is pipeline
    assert projected._flow._pipeline.source is pipeline._pipeline.source

    assert projected.to_list() == [{"id": 1}, {"id": 2}]
    with pytest.raises(fpstreams.FlowConsumedError):
        pipeline.to_list()

    second = flow(iter([{"id": 3}]))
    row_view = second.rows()

    assert second.to_list() == [{"id": 3}]
    with pytest.raises(fpstreams.FlowConsumedError):
        row_view.to_list()


def test_flow_row_bridge_runtime_type_hints_resolve() -> None:
    from typing import get_type_hints

    for method_name in ("rows", "select", "with_columns", "group_by"):
        hints = get_type_hints(getattr(fpstreams.Flow, method_name))
        assert hints["return"] is not None


def test_rows_map_and_flat_map_exit_to_flow_and_can_reenter_rows() -> None:
    mapped = fpstreams.rows([{"value": 1}, {"value": 2}]).map(
        lambda row: {"value": row["value"] + 10}
    )
    flattened = fpstreams.rows([{"values": (1, 2)}, {"values": (3,)}]).flat_map(
        lambda row: ({"value": value} for value in row["values"])
    )

    assert isinstance(mapped, fpstreams.Flow)
    assert mapped.select("value").to_list() == [{"value": 11}, {"value": 12}]
    assert isinstance(flattened, fpstreams.Flow)
    assert flattened.select("value").to_list() == [
        {"value": 1},
        {"value": 2},
        {"value": 3},
    ]


def test_flow_row_bridge_does_not_replace_existing_terminal_or_transform_semantics() -> None:
    assert flow([1, 2, 3]).drop(1).to_list() == [2, 3]
    assert flow([1, 2, 3]).aggregate(total=fpstreams.agg.sum()) == {"total": 6}
    assert flow(["a", "b"]).join("|") == "a|b"
    assert flow([1, 2, 3]).where(lambda value: value > 1).to_list() == [2, 3]


def test_explicit_rows_view_keeps_relational_versions_of_conflicting_flow_methods() -> None:
    records = [{"id": 1, "value": 2}, {"id": 2, "value": 3}]

    assert flow(records).rows().drop("value").to_list() == [{"id": 1}, {"id": 2}]
    assert flow(records).rows().where(id=2).to_list() == [{"id": 2, "value": 3}]
    assert flow(records).rows().aggregate(total=fpstreams.agg.sum("value")).to_list() == [
        {"total": 5}
    ]
    assert flow(records).rows().join(
        [{"id": 1, "right": "x"}],
        left_on="id",
        right_on="id",
        validate="m:1",
    ).to_list() == [{"id": 1, "value": 2, "right": "x"}]


def test_flow_factory_reuses_existing_flow_and_rows_plan_ownership() -> None:
    pipeline = flow(iter([{"id": 1}]))
    row_view = pipeline.rows()

    assert flow(pipeline) is pipeline
    assert flow(row_view) is pipeline
    assert flow(row_view).to_list() == [{"id": 1}]
    with pytest.raises(fpstreams.FlowConsumedError):
        row_view.to_list()


def test_flow_factory_dispatches_retained_pyarrow_without_row_boxing() -> None:
    pa = pytest.importorskip("pyarrow")
    from fpstreams.planning.arrow_source import ArrowBatchSource

    table = pa.table({"id": [1, 2], "value": [3, 4]})
    pipeline = flow(table)
    descriptor = pipeline._pipeline.source.native_data

    assert isinstance(pipeline, fpstreams.Flow)
    assert isinstance(descriptor, ArrowBatchSource)
    assert descriptor.kind == "table"
    assert descriptor.materialized_data is table
    assert pipeline.select("id").to_list() == [{"id": 1}, {"id": 2}]


def test_flow_factory_dispatches_retained_pyarrow_record_batch() -> None:
    pa = pytest.importorskip("pyarrow")
    from fpstreams.planning.arrow_source import ArrowBatchSource

    batch = pa.record_batch({"id": [1, 2]})
    pipeline = flow(batch)
    descriptor = pipeline._pipeline.source.native_data

    assert isinstance(descriptor, ArrowBatchSource)
    assert descriptor.kind == "record_batch"
    assert descriptor.materialized_data is batch
    assert pipeline.to_list() == [{"id": 1}, {"id": 2}]


def test_flow_factory_keeps_pyarrow_reader_one_shot() -> None:
    pa = pytest.importorskip("pyarrow")
    from fpstreams.planning.arrow_source import ArrowBatchSource

    reader = pa.RecordBatchReader.from_batches(
        pa.schema([("id", pa.int64())]),
        [pa.record_batch({"id": [1, 2]})],
    )
    pipeline = flow(reader)
    descriptor = pipeline._pipeline.source.native_data

    assert isinstance(descriptor, ArrowBatchSource)
    assert descriptor.kind == "reader"
    assert pipeline.to_list() == [{"id": 1}, {"id": 2}]
    with pytest.raises(fpstreams.FlowConsumedError):
        pipeline.to_list()


@pytest.mark.parametrize("vendor", ["pandas", "polars", "pyarrow"])
def test_flow_factory_keeps_vendor_scalar_vectors_as_ordinary_iterables(vendor: str) -> None:
    """Series and Arrow arrays expose protocols but are not record tables."""
    if vendor == "pandas":
        pd = pytest.importorskip("pandas")
        source = pd.Series([1, 2])
    elif vendor == "polars":
        pl = pytest.importorskip("polars")
        source = pl.Series([1, 2])
    else:
        pa = pytest.importorskip("pyarrow")
        source = pa.chunked_array([[1], [2]])

    pipeline = flow(source)

    assert pipeline._pipeline.source.native_data is None
    assert pipeline.to_list() == list(source)


def test_flow_factory_prefers_custom_arrow_stream_over_dataframe_protocol() -> None:
    pa = pytest.importorskip("pyarrow")
    from fpstreams.planning.arrow_source import ArrowBatchSource

    table = pa.table({"id": [1, 2]})

    class Provider:
        arrow_calls = 0
        dataframe_calls = 0

        def __arrow_c_stream__(self, requested_schema: Any = None) -> Any:
            self.arrow_calls += 1
            return table.__arrow_c_stream__(requested_schema)

        def __dataframe__(self, **_options: Any) -> Any:
            self.dataframe_calls += 1
            raise AssertionError("the dataframe protocol must not win")

    provider = Provider()
    pipeline = flow(provider)
    descriptor = pipeline._pipeline.source.native_data

    assert isinstance(descriptor, ArrowBatchSource)
    assert descriptor.kind == "reader"
    assert provider.arrow_calls == 1
    assert provider.dataframe_calls == 0
    assert pipeline.to_list() == [{"id": 1}, {"id": 2}]


def test_rows_factory_reuses_flow_dual_protocol_priority() -> None:
    pa = pytest.importorskip("pyarrow")
    from fpstreams.planning.arrow_source import ArrowBatchSource

    table = pa.table({"id": [1, 2]})

    class Provider:
        arrow_calls = 0
        dataframe_calls = 0

        def __arrow_c_stream__(self, requested_schema: Any = None) -> Any:
            self.arrow_calls += 1
            return table.__arrow_c_stream__(requested_schema)

        def __dataframe__(self, **_options: Any) -> Any:
            self.dataframe_calls += 1
            raise AssertionError("Arrow must keep priority through the Rows compatibility factory")

    provider = Provider()
    row_view = fpstreams.rows(provider)
    descriptor = row_view._flow._pipeline.source.native_data

    assert isinstance(descriptor, ArrowBatchSource)
    assert descriptor.kind == "reader"
    assert provider.arrow_calls == 1
    assert provider.dataframe_calls == 0
    assert row_view.to_list() == [{"id": 1}, {"id": 2}]


def test_rows_factory_preserves_instance_level_dataframe_provider() -> None:
    pd = pytest.importorskip("pandas")

    frame = pd.DataFrame({"id": [1, 2]})

    class DynamicProvider:
        def __init__(self) -> None:
            self.__dataframe__ = frame.__dataframe__

        def __iter__(self) -> Iterator[dict[str, bool]]:
            yield {"fallback": True}

    assert fpstreams.rows(DynamicProvider()).to_list() == [{"id": 1}, {"id": 2}]


def test_rows_factory_types_standard_tabular_protocols_as_record_rows() -> None:
    from typing import get_args, get_overloads, get_type_hints

    from fpstreams.streams.flow import _ArrowCStreamProvider, _DataFrameProvider

    expected_protocols = {_ArrowCStreamProvider, _DataFrameProvider}
    for candidate in get_overloads(type(fpstreams.rows).__call__):
        hints = get_type_hints(candidate)
        if expected_protocols <= set(get_args(hints["source"])):
            assert hints["return"] == fpstreams.Rows[dict[str, Any]]
            break
    else:
        raise AssertionError("rows() is missing its standard tabular protocol overload")


def test_flow_factory_keeps_pandas_index_out_of_record_columns() -> None:
    pd = pytest.importorskip("pandas")
    from fpstreams.planning.arrow_source import ArrowBatchSource

    frame = pd.DataFrame({"id": [1, 2]}, index=pd.Index([10, 20], name="row_id"))
    pipeline = flow(frame)
    descriptor = pipeline._pipeline.source.native_data

    assert isinstance(descriptor, ArrowBatchSource)
    assert descriptor.kind == "dataframe"
    assert pipeline.to_list() == [{"id": 1}, {"id": 2}]


def test_flow_factory_defers_generic_dataframe_conversion_until_consumption() -> None:
    pd = pytest.importorskip("pandas")
    from fpstreams.planning.arrow_source import ArrowBatchSource

    frame = pd.DataFrame({"id": [1, 2]})
    events: list[str] = []

    class Provider:
        def __dataframe__(self, **options: Any) -> Any:
            events.append("convert")
            return frame.__dataframe__(**options)

    pipeline = flow(Provider())
    descriptor = pipeline._pipeline.source.native_data

    assert isinstance(descriptor, ArrowBatchSource)
    assert descriptor.kind == "dataframe"
    assert events == []
    assert pipeline.to_list() == [{"id": 1}, {"id": 2}]
    assert events == ["convert"]


def test_flow_factory_preserves_polars_lazyframe_laziness() -> None:
    pl = pytest.importorskip("polars")
    from fpstreams.planning.arrow_source import ArrowBatchSource

    events: list[str] = []

    def observe(batch: Any) -> Any:
        events.append("collect")
        return batch

    source = (
        pl.DataFrame({"id": [1, 2]})
        .lazy()
        .map_batches(
            observe,
            schema={"id": pl.Int64},
        )
    )
    pipeline = flow(source)
    descriptor = pipeline._pipeline.source.native_data

    assert isinstance(descriptor, ArrowBatchSource)
    assert descriptor.kind == "polars"
    assert events == []
    assert pipeline.to_list() == [{"id": 1}, {"id": 2}]
    assert events == ["collect"]


def test_flow_factory_uses_polars_adapter_for_eager_dataframe() -> None:
    pl = pytest.importorskip("polars")
    from fpstreams.planning.arrow_source import ArrowBatchSource

    frame = pl.DataFrame({"id": [1, 2]})
    pipeline = flow(frame)
    descriptor = pipeline._pipeline.source.native_data

    assert isinstance(descriptor, ArrowBatchSource)
    assert descriptor.kind == "polars"
    assert pipeline.to_list() == [{"id": 1}, {"id": 2}]


def test_flow_factory_recognizes_a_polars_subclass_from_an_application_module() -> None:
    pl = pytest.importorskip("polars")
    from fpstreams.planning.arrow_source import ArrowBatchSource

    class ApplicationLazyFrame(pl.LazyFrame):
        pass

    source = ApplicationLazyFrame({"id": [1, 2]})
    pipeline = flow(source)
    descriptor = pipeline._pipeline.source.native_data

    assert isinstance(descriptor, ArrowBatchSource)
    assert descriptor.kind == "polars"
    assert pipeline.to_list() == [{"id": 1}, {"id": 2}]


def test_flow_factory_does_not_trust_a_spoofed_vendor_module_name() -> None:
    pytest.importorskip("pandas")

    class OrdinaryRecords:
        __module__ = "pandas.application"

        def __iter__(self) -> Iterator[dict[str, int]]:
            yield {"id": 1}

    pipeline = flow(OrdinaryRecords())

    assert pipeline._pipeline.source.native_data is None
    assert pipeline.to_list() == [{"id": 1}]


def test_flow_factory_does_not_probe_an_ordinary_failing_generator() -> None:
    events: list[str] = []

    def records() -> Iterator[dict[str, int]]:
        events.append("open")
        raise RuntimeError("source failed")
        yield {"id": 1}

    pipeline = flow(records())

    assert events == []
    with pytest.raises(RuntimeError, match="source failed"):
        pipeline.to_list()
    assert events == ["open"]


def test_flow_factory_keeps_numpy_arrays_as_ordinary_iterables() -> None:
    np = pytest.importorskip("numpy")

    source = np.asarray([[1, 2], [3, 4]])
    pipeline = flow(source)
    result = pipeline.to_list()

    assert pipeline._pipeline.source.native_data is None
    assert all(isinstance(row, np.ndarray) for row in result)
    assert np.array_equal(result[0], np.asarray([1, 2]))
    assert np.array_equal(result[1], np.asarray([3, 4]))


def test_flow_projects_a_plain_two_dimensional_list_without_source_sniffing() -> None:
    source = [[1, "a"], [2, "b"]]
    pipeline = flow(source)

    assert pipeline._pipeline.source.native_data is source
    assert pipeline.select(0, 1).to_list() == [
        {"0": 1, "1": "a"},
        {"0": 2, "1": "b"},
    ]


@pytest.mark.parametrize(
    ("source", "transform", "expected"),
    [
        ([{"old": 1}], lambda rows: rows.rename(old="new"), [{"new": 1}]),
        ([{"value": "2"}], lambda rows: rows.cast(value=int), [{"value": 2}]),
        (
            [{"value": None}, {"value": 2}],
            lambda rows: rows.fill_nulls(value=1),
            [{"value": 1}, {"value": 2}],
        ),
        (
            [{"value": None}, {"value": 2}],
            lambda rows: rows.drop_nulls("value"),
            [{"value": 2}],
        ),
        (
            [{"id": 1, "values": [2, 3]}],
            lambda rows: rows.explode("values"),
            [{"id": 1, "values": 2}, {"id": 1, "values": 3}],
        ),
        (
            [{"id": 1, "meta": {"score": 2}}],
            lambda rows: rows.unnest("meta"),
            [{"id": 1, "score": 2}],
        ),
        (
            [{"id": 1, "left": 2, "right": 3}],
            lambda rows: rows.unpivot("left", "right"),
            [
                {"id": 1, "variable": "left", "value": 2},
                {"id": 1, "variable": "right", "value": 3},
            ],
        ),
        (
            [
                {"id": 1, "metric": "left", "value": 2},
                {"id": 1, "metric": "right", "value": 3},
            ],
            lambda rows: rows.pivot(index="id", columns="metric", values="value"),
            [{"id": 1, "left": 2, "right": 3}],
        ),
    ],
    ids=(
        "rename",
        "cast",
        "fill_nulls",
        "drop_nulls",
        "explode",
        "unnest",
        "unpivot",
        "pivot",
    ),
)
def test_flow_nonconflicting_row_bridges_remain_lazy_and_usable(
    source: list[dict[str, Any]],
    transform: Any,
    expected: list[dict[str, Any]],
) -> None:
    result = transform(flow(source))

    assert isinstance(result, fpstreams.Rows)
    assert result.to_list() == expected


def test_flow_namespace_reuses_tabular_source_adapters(tmp_path: Path) -> None:
    pa = pytest.importorskip("pyarrow")
    pd = pytest.importorskip("pandas")
    pl = pytest.importorskip("polars")
    parquet = pytest.importorskip("pyarrow.parquet")

    table = pa.table({"id": [1, 2]})
    frame = pd.DataFrame({"id": [1, 2]})
    polars_frame = pl.DataFrame({"id": [1, 2]})
    csv_path = tmp_path / "records.csv"
    parquet_path = tmp_path / "records.parquet"
    csv_path.write_text("id\n1\n2\n", encoding="utf-8")
    parquet.write_table(table, parquet_path)

    pipelines = (
        flow.from_arrow(table),
        flow.from_dataframe(frame),
        flow.from_pandas(frame),
        flow.from_polars(polars_frame),
        flow.scan_csv(csv_path),
        flow.from_parquet(parquet_path),
    )

    assert all(isinstance(pipeline, fpstreams.Flow) for pipeline in pipelines)
    assert [pipeline.to_list() for pipeline in pipelines] == [
        [{"id": 1}, {"id": 2}],
        [{"id": 1}, {"id": 2}],
        [{"id": 1}, {"id": 2}],
        [{"id": 1}, {"id": 2}],
        [{"id": 1}, {"id": 2}],
        [{"id": 1}, {"id": 2}],
    ]


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


def test_identity_sum_closes_its_source_when_addition_fails() -> None:
    """The direct CPython reduction path retains query-scoped source ownership."""
    closed = False

    def values():
        nonlocal closed
        try:
            yield 1
            yield "not-addable"
        finally:
            closed = True

    with pytest.raises(TypeError):
        flow(values()).sum()

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


def test_unique_handles_late_sparse_and_unbounded_integers() -> None:
    """Integer magnitude must not change exact set semantics or allocate by key span."""
    values = [*range(128), 10**10, -(10**10), 2**100, 10**10, 0]

    assert flow(values).with_engine("python").unique().to_list() == list(dict.fromkeys(values))


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


def test_keyless_minmax_uses_one_truthful_query_and_keeps_equal_representatives(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The direct comparison fallback retains the first equal minimum and maximum objects."""

    class Ranked:
        def __init__(self, rank: int, label: str) -> None:
            self.rank = rank
            self.label = label

        def __lt__(self, other: object) -> bool:
            assert isinstance(other, Ranked)
            return self.rank < other.rank

        def __gt__(self, other: object) -> bool:
            assert isinstance(other, Ranked)
            return self.rank > other.rank

    first_low = Ranked(1, "first-low")
    later_low = Ranked(1, "later-low")
    first_high = Ranked(3, "first-high")
    later_high = Ranked(3, "later-high")
    values = flow([first_low, later_low, first_high, later_high])
    seen: list[str] = []
    original = type(values)._query

    def tracked(self, name, *arguments, **options):
        seen.append(name)
        return original(self, name, *arguments, **options)

    monkeypatch.setattr(type(values), "_query", tracked)

    minimum, maximum = values.minmax()

    assert minimum is first_low
    assert maximum is first_high
    assert seen == ["minmax"]


def test_minmax_one_shot_failpoint_closes_the_canonical_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An instrumented callback failure closes one claimed source under the minmax query."""
    from fpstreams.runtime.failpoints import failpoint

    events: list[str] = []

    def source() -> Iterator[int]:
        events.append("open")
        try:
            yield from (3, 1, 4, 2)
        finally:
            events.append("close")

    values = flow(source()).map(lambda value: value)
    seen: list[str] = []
    original = type(values)._query

    def tracked(self, name, *arguments, **options):
        seen.append(name)
        return original(self, name, *arguments, **options)

    monkeypatch.setattr(type(values), "_query", tracked)

    with (
        failpoint("callback.before", RuntimeError("instrumented minmax")),
        pytest.raises(RuntimeError, match="instrumented minmax"),
    ):
        values.minmax()

    assert seen == ["minmax"]
    assert events == ["open", "close"]


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


def test_explain_preserves_serialization_order() -> None:
    payload = flow(range(3)).map(str).filter(bool).take(1).with_engine("python").explain().to_dict()

    assert tuple(payload) == (
        "terminal",
        "source",
        "requested_engine",
        "selected_engine",
        "streaming_engine",
        "materializing_engine",
        "selection_reason",
        "data_movement",
        "complexity",
        "operations",
        "stages",
        "semantics",
        "diagnostics",
        "arrow_prefix",
        "boundaries",
    )
    assert json.dumps(
        {
            "data_movement": payload["data_movement"],
            "operations": payload["operations"],
            "stages": payload["stages"],
            "arrow_prefix": payload["arrow_prefix"],
            "boundaries": payload["boundaries"],
        },
        separators=(",", ":"),
    ) == (
        '{"data_movement":{"scans_source":false,"copies_source":false,'
        '"materializes":false},"operations":[{"name":"map"},{"name":"filter"},'
        '{"name":"take"}],"stages":[{"engine":"python","operations":["map",'
        '"filter"],"fused":true},{"engine":"python","operations":["take"],'
        '"fused":false}],"arrow_prefix":null,"boundaries":[]}'
    )


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

    original = _native.materialize_i64_range
    calls = 0

    def tracked(*args):
        nonlocal calls
        calls += 1
        return original(*args)

    monkeypatch.setattr(_native, "materialize_i64_range", tracked)
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


@pytest.mark.parametrize(
    ("terminal", "expected"),
    [
        ("to_list", [4, 10, 16, 22]),
        ("to_tuple", (4, 10, 16, 22)),
        ("to_set", {4, 10, 16, 22}),
    ],
)
def test_complete_native_materializes_directly_without_the_legacy_iterator(
    monkeypatch: pytest.MonkeyPatch, terminal: str, expected: object
) -> None:
    from fpstreams import _native

    direct = _native.materialize_i64_range

    def reject_legacy(*_args: object) -> None:
        raise AssertionError("direct terminal materialization must not use execute_i64_range")

    calls = 0

    def tracked_direct(*args: object) -> object:
        nonlocal calls
        calls += 1
        return direct(*args)

    monkeypatch.setattr(_native, "execute_i64_range", reject_legacy)
    monkeypatch.setattr(_native, "materialize_i64_range", tracked_direct)
    pipeline = flow(range(100)).map(fpstreams.item * 3 + 1).filter(fpstreams.item % 2 == 0).take(4)

    assert getattr(pipeline.with_engine("native"), terminal)() == expected
    assert calls == 1


@pytest.mark.parametrize(
    ("source", "expression", "expected"),
    [
        (range(8), fpstreams.item * 2, [0, 2, 4, 6, 8, 10, 12, 14]),
        ([0, 1, 2, 3], fpstreams.item + 1, [1, 2, 3, 4]),
        ((0, 1, 2, 3), fpstreams.item + 1, [1, 2, 3, 4]),
        (range(8), fpstreams.fitem / 2.0, [0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5]),
        ([0.0, 1.0, 2.0], fpstreams.fitem + 0.5, [0.5, 1.5, 2.5]),
        ((0.0, 1.0, 2.0), fpstreams.fitem + 0.5, [0.5, 1.5, 2.5]),
    ],
)
def test_native_direct_materialization_matches_all_numeric_source_shapes(
    source: object, expression: object, expected: list[float | int]
) -> None:
    pipeline = flow(source).map(expression).with_engine("native")

    assert pipeline.to_list() == expected
    assert pipeline.to_tuple() == tuple(expected)
    assert pipeline.to_set() == set(expected)


def test_native_direct_materialization_preserves_empty_while_and_nan_set_results() -> None:
    empty = flow(range(10)).filter(fpstreams.item < 0).with_engine("native")
    assert empty.to_list() == []
    assert empty.to_tuple() == ()
    assert empty.to_set() == set()

    bounded = (
        flow(range(20))
        .drop(2)
        .drop_while(fpstreams.item < 5)
        .take_while(fpstreams.item < 10)
        .take(3)
        .with_engine("native")
    )
    assert bounded.to_list() == [5, 6, 7]

    nan = float("nan")
    values = flow([nan, 1.0]).map(fpstreams.fitem + 0.0).with_engine("native").to_set()
    assert len(values) == 2
    assert 1.0 in values
    assert any(value != value for value in values)


def test_legacy_native_extension_falls_back_to_the_existing_iterator_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fpstreams.execution import native as native_execution

    legacy_execute = native_execution._native.execute_i64_range
    calls = 0

    class LegacyExtension:
        def execute_i64_range(self, *args: object) -> list[int]:
            nonlocal calls
            calls += 1
            return legacy_execute(*args)

    monkeypatch.setattr(native_execution, "_native", LegacyExtension())
    assert flow(range(5)).map(fpstreams.item + 1).with_engine("native").to_list() == [1, 2, 3, 4, 5]
    assert calls == 1


def test_direct_materialization_keeps_auto_fallback_and_forced_native_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fpstreams.planning.source import Source

    incompatible = flow([1, 2.5, 3]).map(fpstreams.item + 1)
    source = incompatible._pipeline.source
    original_open = Source.open
    opens = 0

    def tracked_open(self: Source[object]) -> Iterator[object]:
        nonlocal opens
        if self is source:
            opens += 1
        return original_open(self)

    monkeypatch.setattr(Source, "open", tracked_open)
    assert incompatible.to_tuple() == (2, 3.5, 4)
    assert opens == 1
    with pytest.raises(fpstreams.NativeUnsupportedError):
        incompatible.with_engine("native").to_set()

    overflowing = flow(range(2)).map((fpstreams.item + 2**62) * 4).take(1)
    assert overflowing.to_list() == [2**64]
    with pytest.raises(OverflowError):
        overflowing.with_engine("native").to_list()


def test_direct_materialization_failure_does_not_retry_native_conversion() -> None:
    """A failed new endpoint must enter Python without invoking the legacy adapter."""
    index_calls = 0
    add_calls = 0
    values: list[object] = [1, 2, 3, 4, 5, 6, 7]

    class MutableProtocolValue:
        def __index__(self) -> int:
            nonlocal index_calls
            index_calls += 1
            current = values[0]
            assert isinstance(current, int)
            values[0] = current + 1
            raise TypeError("not an exact native integer")

        def __add__(self, other: object) -> int:
            nonlocal add_calls
            add_calls += 1
            assert other == 1
            return 9

    values.append(MutableProtocolValue())

    result = flow(values).map(fpstreams.item + 1).to_list()

    assert index_calls == 0
    assert add_calls == 1
    assert result == [2, 3, 4, 5, 6, 7, 8, 9]


@pytest.mark.parametrize(
    ("terminal", "expected"),
    [
        ("count", 16),
        ("sum", 143),
        ("mean", 143 / 16),
        ("aggregate", {"rows": 16, "total": 143}),
    ],
)
def test_native_terminal_failure_runs_stateful_python_protocol_once(
    terminal: str, expected: object
) -> None:
    """Every terminal enters canonical Python once after exact extraction rejects a value."""
    calls = {"index": 0, "add": 0}

    class StatefulInteger:
        def __index__(self) -> int:
            calls["index"] += 1
            return 7

        def __add__(self, other: object) -> int:
            calls["add"] += 1
            assert other == 1
            return 8

    pipeline = fpstreams.flow([StatefulInteger(), *range(1, 16)]).map(fpstreams.item + 1)
    if terminal == "aggregate":
        result = pipeline.aggregate(rows=fpstreams.agg.count(), total=fpstreams.agg.sum())
    else:
        result = getattr(pipeline, terminal)()

    assert result == expected
    assert calls == {"index": 0, "add": 1}


def test_direct_materialization_excludes_hybrid_python_and_relational_plans(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fpstreams.streams import flow_terminals

    calls = 0

    def reject_direct(*_args: object, **_kwargs: object) -> tuple[bool, None]:
        nonlocal calls
        calls += 1
        raise AssertionError("incomplete native decisions must not enter direct materialization")

    monkeypatch.setattr(flow_terminals, "try_native_materialize", reject_direct)

    hybrid = flow(range(4)).map(fpstreams.item + 1).map(str)
    assert hybrid.to_list() == ["1", "2", "3", "4"]
    assert flow(range(4)).map(fpstreams.item + 1).with_engine("python").to_list() == [1, 2, 3, 4]
    assert flow(range(4)).map(lambda value: value + 1).to_list() == [1, 2, 3, 4]
    assert fpstreams.rows([{"id": 1}]).join([{"id": 1}], on="id").to_list() == [{"id": 1}]
    assert calls == 0


def test_exact_container_python_map_filter_materializes_without_forwarding_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A selected Python map/filter plan can drain its canonical C iterator directly."""
    from fpstreams.streams import flow_terminals

    calls: list[int] = []

    def unexpected_executor(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("direct Python materialization must not forward through generators")

    monkeypatch.setattr(flow_terminals, "execute_physical", unexpected_executor)

    result = (
        flow(list(range(8)))
        .map(lambda value: calls.append(value) or value + 1)
        .filter(lambda value: value % 2 == 0)
        .with_engine("python")
        .to_list()
    )

    assert result == [2, 4, 6, 8]
    assert calls == list(range(8))


def test_python_materialization_does_not_probe_custom_length_hints() -> None:
    """Opaque one-shot sources retain the forwarding layer that hides length hints."""
    hints: list[bool] = []

    class Values:
        def __init__(self) -> None:
            self.current = 0

        def __iter__(self) -> Values:
            return self

        def __next__(self) -> int:
            if self.current == 4:
                raise StopIteration
            value = self.current
            self.current += 1
            return value

        def __length_hint__(self) -> int:
            hints.append(True)
            return 4 - self.current

    assert flow(Values()).map(lambda value: value + 1).with_engine("python").to_list() == [
        1,
        2,
        3,
        4,
    ]
    assert hints == []


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

    expected = [6, 6, 5, 5, 5, 3, 3]

    assert pipeline.with_engine("native").to_list() == expected
    assert pipeline.with_engine("python").to_list() == expected
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


def test_exact_container_python_scalar_materialization_bypasses_stage_callbacks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A large closed scalar program must execute as one callback-free Python loop."""
    from fpstreams.expressions.program import ExprProgram

    evaluator_calls = 0
    original_evaluator = ExprProgram.evaluator

    def tracked_evaluator(self: ExprProgram):
        evaluator = original_evaluator(self)

        def evaluate(value: object) -> object:
            nonlocal evaluator_calls
            evaluator_calls += 1
            return evaluator(value)

        return evaluate

    monkeypatch.setattr(ExprProgram, "evaluator", tracked_evaluator)
    result = (
        fpstreams.flow(range(4_096))
        .map(fpstreams.item * 3 + 1)
        .filter(fpstreams.item % 2 == 0)
        .with_engine("python")
        .to_list()
    )

    assert len(result) == 2_048
    assert result[:3] == [4, 10, 16]
    assert result[-1] == 12_286
    assert evaluator_calls == 0


def test_python_scalar_fusion_keeps_cold_break_even_boundary_canonical(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A unique 2K query must not pay AST compilation before it can amortize it."""
    from fpstreams.expressions.program import ExprProgram

    evaluator_calls = 0
    original_evaluator = ExprProgram.evaluator

    def tracked_evaluator(self: ExprProgram):
        evaluator = original_evaluator(self)

        def evaluate(value: object) -> object:
            nonlocal evaluator_calls
            evaluator_calls += 1
            return evaluator(value)

        return evaluate

    monkeypatch.setattr(ExprProgram, "evaluator", tracked_evaluator)
    result = (
        fpstreams.flow(range(2_048))
        .map(fpstreams.item * 3 + 1)
        .filter(fpstreams.item % 2 == 0)
        .with_engine("python")
        .to_list()
    )

    assert len(result) == 1_024
    assert evaluator_calls == 4_096


def test_exact_container_python_scalar_sum_bypasses_stage_callbacks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Scalar sum must fuse stages while retaining the built-in reducer semantics."""
    from fpstreams.expressions.scalar import Expr

    evaluator_calls = 0
    original_evaluator = Expr._python_evaluator

    def tracked_evaluator(self: Expr):
        evaluator = original_evaluator(self)

        def evaluate(value: object) -> object:
            nonlocal evaluator_calls
            evaluator_calls += 1
            return evaluator(value)

        return evaluate

    monkeypatch.setattr(Expr, "_python_evaluator", tracked_evaluator)
    result = (
        fpstreams.flow(range(4_096))
        .map(fpstreams.item * 3 + 1)
        .filter(fpstreams.item % 2 == 0)
        .with_engine("python")
        .sum(7)
    )

    assert result == 12_584_967
    assert evaluator_calls == 0


def test_scalar_python_fusion_declines_noncanonical_float_constant_without_rounding() -> None:
    from fpstreams.expressions.scalar import FExpr

    exact_integer = 2**53 + 1
    expression = FExpr("const", value=exact_integer)  # type: ignore[arg-type]

    assert fpstreams.flow([0]).map(expression).with_engine("python").to_list() == [exact_integer]
    assert (
        fpstreams.flow([0] * 4_096).map(expression).with_engine("python").to_list()
        == [exact_integer] * 4_096
    )


def test_large_float_scalar_python_fusion_preserves_closed_expression_semantics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fpstreams.expressions.program import ExprProgram

    evaluator_calls = 0
    original_evaluator = ExprProgram.evaluator

    def tracked_evaluator(self: ExprProgram):
        evaluator = original_evaluator(self)

        def evaluate(value: object) -> object:
            nonlocal evaluator_calls
            evaluator_calls += 1
            return evaluator(value)

        return evaluate

    monkeypatch.setattr(ExprProgram, "evaluator", tracked_evaluator)
    transformed = abs(-(fpstreams.fitem / 2.0) + 1.5)
    result = (
        fpstreams.flow([0, 1, 2, 3] * 1_024)
        .map(transformed)
        .filter((fpstreams.fitem >= 0.5) & (fpstreams.fitem < 1.5))
        .with_engine("python")
        .to_list()
    )

    assert len(result) == 2_048
    assert result[:4] == [1.0, 0.5, 1.0, 0.5]
    assert result[-1] == 0.5
    assert evaluator_calls == 0


def test_large_integer_scalar_python_fusion_preserves_reject_and_boolean_order() -> None:
    transformed = abs(1 - fpstreams.item) // 2
    result = (
        fpstreams.flow([-3, -2, -1, 0, 1, 2, 3, 4] * 512)
        .map(transformed)
        .filter(((fpstreams.item >= 1) & (fpstreams.item < 3)) | (fpstreams.item == 4))
        .reject(~(fpstreams.item != 1))
        .with_engine("python")
        .to_list()
    )

    assert result == [2] * 512


def test_scalar_python_fusion_preserves_dunder_order_and_filter_short_circuit() -> None:
    events: list[str] = []

    class Truth:
        def __bool__(self) -> bool:
            events.append("bool")
            return False

    class Predicate:
        def __eq__(self, other: object) -> Truth:
            events.append(f"eq:{other}")
            return Truth()

    class Mapped:
        def __add__(self, other: object) -> Mapped:
            events.append(f"add:{other}")
            return self

        def __mod__(self, other: object) -> Predicate:
            events.append(f"mod:{other}")
            return Predicate()

    class Input:
        def __mul__(self, other: object) -> Mapped:
            events.append(f"mul:{other}")
            return Mapped()

    result = (
        fpstreams.flow([Input(), *range(1, 4_096)])
        .map(fpstreams.item * 3 + 1)
        .filter(fpstreams.item % 2 == 0)
        .map(fpstreams.item + 10)
        .with_engine("python")
        .to_list()
    )

    assert events == ["mul:3", "add:1", "mod:2", "eq:0", "bool"]
    assert len(result) == 2_048
    assert result[:3] == [14, 20, 26]
    assert result[-1] == 12_296


def test_scalar_python_fused_empty_sum_returns_the_original_start() -> None:
    class Start:
        pass

    start = Start()
    result = (
        fpstreams.flow(range(4_096)).filter(fpstreams.item < 0).with_engine("python").sum(start)
    )

    assert result is start


def test_active_failpoint_bypasses_scalar_python_fusion() -> None:
    from fpstreams.runtime.failpoints import failpoint

    query = fpstreams.flow(range(4_096)).map(fpstreams.item + 1).with_engine("python")
    with (
        failpoint("callback.before", RuntimeError("instrumented callback")),
        pytest.raises(RuntimeError, match="instrumented callback"),
    ):
        query.to_list()

    assert query.to_list()[:3] == [1, 2, 3]


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
    program, prefix_length = native._longest_native_prefix(pipeline._pipeline)

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


def test_minmax_uses_the_masked_native_extrema_snapshot_and_truthful_explain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A numeric range requests only minimum and maximum from the existing aggregate ABI."""
    from fpstreams.execution import native

    extension = native._native
    masks: list[int] = []

    class TrackedNative:
        def aggregate_i64_range_masked(self, *arguments: object) -> object:
            mask = arguments[-1]
            assert isinstance(mask, int)
            masks.append(mask)
            return extension.aggregate_i64_range_masked(*arguments)

        def __getattr__(self, name: str) -> object:
            return getattr(extension, name)

    monkeypatch.setattr(native, "_native", TrackedNative())
    values = fpstreams.flow(range(32))

    explanation = values.explain("minmax").to_dict()

    assert explanation["terminal"] == "minmax"
    assert explanation["selected_engine"] == "native"
    assert explanation["data_movement"] == {
        "scans_source": False,
        "copies_source": False,
        "materializes": False,
    }
    assert values.minmax() == (0, 31)
    assert masks == [(1 << 2) | (1 << 3)]


def test_minmax_container_identity_guard_opens_the_python_source_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The representative guard avoids native protocols and opens one canonical fallback."""
    from fpstreams.execution import native
    from fpstreams.planning.source import Source

    calls = {"endpoint": 0, "index": 0, "add": 0, "open": 0}

    class StatefulInteger:
        def __index__(self) -> int:
            calls["index"] += 1
            return 7

        def __add__(self, other: object) -> int:
            calls["add"] += 1
            assert other == 1
            return 8

    source_values: list[object] = [StatefulInteger(), *range(1, 16)]
    values = fpstreams.flow(source_values).map(fpstreams.item + 1)
    source = values._pipeline.source
    original_open = Source.open
    extension = native._native

    def tracked_open(self):
        if self is source:
            calls["open"] += 1
        return original_open(self)

    class TrackedNative:
        def aggregate_i64_masked(self, *arguments: object) -> object:
            calls["endpoint"] += 1
            return extension.aggregate_i64_masked(*arguments)

        def __getattr__(self, name: str) -> object:
            return getattr(extension, name)

    monkeypatch.setattr(Source, "open", tracked_open)
    monkeypatch.setattr(native, "_native", TrackedNative())

    assert values.minmax() == (2, 16)
    assert calls == {"endpoint": 0, "index": 0, "add": 1, "open": 1}


def test_minmax_range_native_decline_opens_the_untouched_source_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A rejected range snapshot leaves its reusable source untouched for Python fallback."""
    from fpstreams.execution import native
    from fpstreams.planning.source import Source

    calls = {"endpoint": 0, "open": 0}
    values = fpstreams.flow(range(1, 16)).map(fpstreams.item + 1)
    source = values._pipeline.source
    original_open = Source.open
    extension = native._native

    def tracked_open(self):
        if self is source:
            calls["open"] += 1
        return original_open(self)

    class RejectingNative:
        def aggregate_i64_range_masked(self, *_arguments: object) -> object:
            calls["endpoint"] += 1
            raise TypeError("decline")

        def __getattr__(self, name: str) -> object:
            return getattr(extension, name)

    monkeypatch.setattr(Source, "open", tracked_open)
    monkeypatch.setattr(native, "_native", RejectingNative())

    assert values.minmax() == (2, 16)
    assert calls == {"endpoint": 1, "open": 1}


@pytest.mark.parametrize("container", [list, tuple])
def test_auto_minmax_preserves_exact_container_representatives(
    container: type[list[int]] | type[tuple[int, ...]],
) -> None:
    """Automatic extrema retain the first exact objects emitted by a stable container."""
    source = container(int(str(1_000 + index)) for index in range(16))
    values = fpstreams.flow(source).filter(fpstreams.item >= 0)

    minimum, maximum = values.minmax()

    assert minimum is source[0]
    assert maximum is source[-1]
    assert values.explain("minmax").to_dict()["selected_engine"] == "python"


def test_native_minmax_declines_when_failpoints_are_active() -> None:
    """Instrumentation keeps the canonical callback boundary visible before native execution."""
    from fpstreams.runtime.failpoints import failpoint

    values = fpstreams.flow(range(16)).map(fpstreams.item + 1)

    with (
        failpoint("callback.before", RuntimeError("instrumented minmax")),
        pytest.raises(RuntimeError, match="instrumented minmax"),
    ):
        values.minmax()


def test_native_minmax_preserves_nan_and_first_signed_zero() -> None:
    """The extrema mask retains Python's ordered-comparison behavior for f64 edges."""
    nan = float("nan")
    with_nan = fpstreams.flow([nan, 1.0, 2.0] * 4).map(fpstreams.fitem * 1.0).with_engine("native")
    negative_zero = fpstreams.flow([-0.0, 0.0] * 6).map(fpstreams.fitem * 1.0).with_engine("native")

    nan_minimum, nan_maximum = with_nan.minmax()
    zero_minimum, zero_maximum = negative_zero.minmax()

    assert math.isnan(nan_minimum)
    assert math.isnan(nan_maximum)
    assert math.copysign(1.0, zero_minimum) == -1.0
    assert math.copysign(1.0, zero_maximum) == -1.0
    assert with_nan.explain("minmax").to_dict()["selected_engine"] == "native"
    assert negative_zero.explain("minmax").to_dict()["selected_engine"] == "native"


def test_forced_native_minmax_handles_range_empty_and_strict_conversion() -> None:
    """Forced native succeeds for numeric ranges and keeps empty and type errors distinct."""
    from fpstreams.runtime.failpoints import failpoint

    assert fpstreams.flow(range(1, 6)).with_engine("native").minmax() == (1, 5)
    with pytest.raises(fpstreams.EmptyFlowError, match=r"minmax\(\)"):
        fpstreams.flow(range(0)).with_engine("native").minmax()
    with pytest.raises(fpstreams.NativeUnsupportedError, match="homogeneous"):
        (
            fpstreams.flow([1, 2.5, *range(2, 10)])
            .map(fpstreams.item + 1)
            .with_engine("native")
            .minmax()
        )
    with (
        failpoint("unrelated.transition", RuntimeError("unused")),
        pytest.raises(fpstreams.NativeUnsupportedError, match="homogeneous"),
    ):
        (
            fpstreams.flow([1, 2.5, *range(2, 10)])
            .map(fpstreams.item + 1)
            .with_engine("native")
            .minmax()
        )


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

    def fail_if_opened() -> Iterator[int]:
        raise AssertionError("exact-size source was opened")
        yield

    source = Source(
        fail_if_opened,
        SourceCapabilities(reiterable=True, exact_size=7),
    )

    assert fpstreams.Flow(source).count() == 7


def test_exact_size_named_count_does_not_open_an_identity_source() -> None:
    """The named count aggregate shares the terminal count's exact-cardinality shortcut."""
    from fpstreams.planning.source import Source, SourceCapabilities

    def fail_if_opened() -> Iterator[int]:
        raise AssertionError("exact-size source was opened")
        yield

    source = Source(
        fail_if_opened,
        SourceCapabilities(reiterable=True, exact_size=7),
    )

    assert fpstreams.Flow(source).aggregate(rows=fpstreams.agg.count()) == {"rows": 7}


def test_cardinality_changing_plan_does_not_use_source_exact_size() -> None:
    opened = 0

    def values() -> Iterator[int]:
        nonlocal opened
        opened += 1
        yield from range(7)

    from fpstreams.planning.source import Source, SourceCapabilities

    source = Source(values, SourceCapabilities(reiterable=True, exact_size=7))
    pipeline = fpstreams.Flow(source).filter(lambda value: value % 2 == 0)

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


_I64_BULK_ENDPOINTS = (
    "execute_i64",
    "materialize_i64",
    "terminal_i64",
    "statistics_i64",
    "aggregate_i64",
    "aggregate_i64_masked",
)
_F64_BULK_ENDPOINTS = (
    "execute_f64",
    "materialize_f64",
    "terminal_f64",
    "statistics_f64",
    "aggregate_f64",
    "aggregate_f64_masked",
    "count_f64",
)


def _call_native_bulk_endpoint(endpoint: str, values: object, program: list[object]) -> object:
    """Invoke one extension endpoint with its endpoint-specific trailing opcode."""
    from fpstreams import _native

    function = getattr(_native, endpoint)
    if endpoint.startswith("materialize_"):
        return function(values, program, 0)
    if endpoint.startswith("terminal_"):
        return function(values, program, 1)
    if endpoint.endswith("_masked"):
        return function(values, program, 1)
    return function(values, program)


@pytest.mark.parametrize("endpoint", _I64_BULK_ENDPOINTS)
@pytest.mark.parametrize("container", [list, tuple], ids=["list", "tuple"])
def test_i64_bulk_endpoints_accept_only_exact_builtin_integers(
    endpoint: str, container: type[list[object]] | type[tuple[object, ...]]
) -> None:
    """No i64 bulk conversion may invoke __index__ or accept bool/subclass values."""
    calls = 0

    class IndexProtocol:
        def __index__(self) -> int:
            nonlocal calls
            calls += 1
            return 7

    class IntegerSubclass(int):
        pass

    with pytest.raises(TypeError, match="exact integers"):
        _call_native_bulk_endpoint(endpoint, container([IndexProtocol()]), [])
    assert calls == 0
    for invalid in (True, IntegerSubclass(1), 1.0):
        with pytest.raises(TypeError, match="exact integers"):
            _call_native_bulk_endpoint(endpoint, container([invalid]), [])
    with pytest.raises(OverflowError):
        _call_native_bulk_endpoint(endpoint, container([2**100]), [])


@pytest.mark.parametrize("endpoint", _I64_BULK_ENDPOINTS)
def test_i64_bulk_endpoints_reject_container_subclasses(endpoint: str) -> None:
    """Exact item checks are paired with an exact list/tuple ownership boundary."""

    class ListSubclass(list[int]):
        pass

    with pytest.raises(TypeError, match="exact list or tuple"):
        _call_native_bulk_endpoint(endpoint, ListSubclass([1]), [])


@pytest.mark.parametrize("endpoint", _F64_BULK_ENDPOINTS)
@pytest.mark.parametrize("container", [list, tuple], ids=["list", "tuple"])
def test_f64_bulk_endpoints_gate_ints_by_the_first_expression_stage(
    endpoint: str, container: type[list[object]] | type[tuple[object, ...]]
) -> None:
    """Identity/predicate sources require floats; a leading map also accepts exact ints."""
    calls = 0

    class FloatProtocol:
        def __float__(self) -> float:
            nonlocal calls
            calls += 1
            return 2.5

    class FloatSubclass(float):
        pass

    map_stage = (0, list((fpstreams.fitem + 0.5).native_instructions()))
    predicate_stage = (1, list((fpstreams.fitem > 0.0).native_instructions()))

    with pytest.raises(TypeError, match="exact floats"):
        _call_native_bulk_endpoint(endpoint, container([FloatProtocol()]), [])
    assert calls == 0
    for invalid in (1, True, FloatSubclass(1.0)):
        with pytest.raises(TypeError, match="exact floats"):
            _call_native_bulk_endpoint(endpoint, container([invalid]), [])

    _call_native_bulk_endpoint(endpoint, container([1, 2.5]), [map_stage])
    with pytest.raises(TypeError, match="exact floats"):
        _call_native_bulk_endpoint(
            endpoint,
            container([1]),
            [predicate_stage, map_stage],
        )
    with pytest.raises(OverflowError):
        _call_native_bulk_endpoint(endpoint, container([2**2000]), [map_stage])


@pytest.mark.parametrize("endpoint", _F64_BULK_ENDPOINTS)
def test_f64_bulk_endpoints_reject_container_subclasses(endpoint: str) -> None:
    """A Python container subclass cannot interpose while Rust snapshots numeric values."""

    class TupleSubclass(tuple[float, ...]):
        pass

    with pytest.raises(TypeError, match="exact list or tuple"):
        _call_native_bulk_endpoint(endpoint, TupleSubclass((1.0,)), [])


def test_f64_bulk_and_probe_allow_ints_only_after_a_leading_map() -> None:
    """Non-expression take/drop stages do not hide which expression first reads an item."""
    from fpstreams import _native

    mapping = list((fpstreams.fitem + 0.5).native_instructions())
    predicate = list((fpstreams.fitem > 0.0).native_instructions())
    take_then_map = [(3, [(1, 2.0)]), (0, mapping)]
    predicate_then_map = [(1, predicate), (0, mapping)]

    assert _native.execute_f64([1, 2], take_then_map) == [1.5, 2.5]
    assert _native.terminal_f64_probe([1, 2], take_then_map, 5, 256) == (True, 1.5)
    with pytest.raises(TypeError, match="exact floats"):
        _native.execute_f64([1], predicate_then_map)
    with pytest.raises(TypeError, match="exact floats"):
        _native.terminal_f64_probe([1], predicate_then_map, 5, 256)


def test_native_planning_never_prescans_numeric_containers() -> None:
    """Rust's exact gate, not Python planning, owns whole-container validation."""
    from fpstreams.planning import native
    from fpstreams.planning.source import Source, SourceCapabilities

    class IterationCountingList(list[float]):
        iterations = 0

        def __iter__(self):
            self.iterations += 1
            return super().__iter__()

    def retained_flow(values: IterationCountingList) -> fpstreams.Flow[float]:
        source = Source(
            lambda: iter(values),
            SourceCapabilities(reiterable=True, exact_size=len(values)),
            native_data=values,
        )
        return fpstreams.Flow(source)

    predicate_source = IterationCountingList(float(value) for value in range(32))
    predicate = retained_flow(predicate_source).filter(fpstreams.fitem >= 0.0)
    assert native.select_terminal_engine(predicate._pipeline, "sum").engine == "native"
    assert predicate_source.iterations == 0

    prefix_source = IterationCountingList(float(value) for value in range(32))
    hybrid = retained_flow(prefix_source).filter(fpstreams.fitem >= 0.0).map(str)
    assert native.select_materializing_engine(hybrid._pipeline).engine == "hybrid"
    assert prefix_source.iterations == 0

    identity_source = IterationCountingList(float(value) for value in range(32))
    identity = retained_flow(identity_source).with_engine("native")
    assert native.select_terminal_engine(identity._pipeline, "sum").engine == "native"
    assert identity_source.iterations == 0


def test_old_native_wheel_never_receives_a_numeric_container(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The positive exact-extraction marker keeps coercive old ABIs on Python."""
    from fpstreams.planning import native

    real_extension = fpstreams._native

    class LegacyExtension:
        def __getattr__(self, name: str) -> object:
            if name == "exact_container_extraction_v1":
                raise AttributeError(name)
            return getattr(real_extension, name)

    monkeypatch.setattr(fpstreams, "_native", LegacyExtension())
    native._exact_container_capability_cache = None

    index_calls = 0

    class ProtocolInteger:
        def __index__(self) -> int:
            nonlocal index_calls
            index_calls += 1
            return 7

        def __add__(self, other: object) -> int:
            assert other == 1
            return 8

    values: list[object] = [ProtocolInteger(), *range(1, 16)]
    pipeline = fpstreams.flow(values).map(fpstreams.item + 1)

    assert native.select_materializing_engine(pipeline._pipeline).engine == "python"
    assert pipeline.to_list() == [8, *range(2, 17)]
    assert index_calls == 0
    with pytest.raises(fpstreams.NativeUnsupportedError, match="exact numeric container"):
        pipeline.with_engine("native").to_list()


@pytest.mark.parametrize(
    ("operation", "expected"),
    [
        ("filter", [0, 1]),
        ("take_while", [0, 1]),
        ("take_while_inclusive", [0, 1, 2]),
        ("drop_while", [2, 3, 4]),
    ],
)
def test_predicate_first_f64_range_preserves_python_integer_values(
    operation: str, expected: list[int]
) -> None:
    """Every predicate form stays Python until a map converts range ints to floats."""
    pipeline = fpstreams.flow(range(5))
    predicate = fpstreams.fitem < 2.0
    if operation == "filter":
        pipeline = pipeline.filter(predicate)
    elif operation == "take_while":
        pipeline = pipeline.take_while(predicate)
    elif operation == "take_while_inclusive":
        pipeline = pipeline.take_while_inclusive(predicate)
    else:
        pipeline = pipeline.drop_while(predicate)

    assert pipeline.to_list() == expected
    assert all(type(value) is int for value in pipeline.to_list())
    with pytest.raises(fpstreams.NativeUnsupportedError, match="preceding fitem map"):
        pipeline.with_engine("native").to_list()


def test_native_masked_aggregate_abi_preserves_snapshot_slots_and_wide_totals() -> None:
    """Optional mask endpoints leave unrequested slots empty without narrowing totals."""
    from fpstreams import _native

    total_mask = 1 << 1
    statistics_mask = (1 << 0) | (1 << 6) | (1 << 7)

    assert _native.aggregate_i64_range_masked(1, 5, 1, [], total_mask) == (
        0,
        10,
        None,
        None,
        None,
        None,
        0.0,
        0.0,
    )
    wide = _native.aggregate_i64_masked([2**63 - 1, 2**63 - 1], [], total_mask)
    assert wide[1] == 2 * (2**63 - 1)

    statistics = _native.aggregate_f64_masked([1e16, 1.0, -1e16], [], statistics_mask)
    assert statistics[0] == 3
    assert statistics[1] == 0.0
    assert statistics[6] == pytest.approx(1 / 3)
    assert statistics[7] > 0.0


def test_named_aggregate_prefers_scalar_and_masked_kernels_over_the_full_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Requested fields alone determine native work; the legacy full kernel stays optional."""
    from fpstreams.execution import native

    extension = native._native

    class NoFullSnapshot:
        def aggregate_i64(self, *_args: object) -> object:
            raise AssertionError("container aggregate must use its field mask")

        def aggregate_i64_range(self, *_args: object) -> object:
            raise AssertionError("range aggregate must use its field mask")

        def aggregate_i64_range_masked(self, *args: object) -> object:
            mask = args[-1]
            assert isinstance(mask, int)
            if mask & ((1 << 6) | (1 << 7)):
                raise AssertionError("statistics-only aggregates must use the statistics kernel")
            return extension.aggregate_i64_range_masked(*args)

        def __getattr__(self, name: str) -> object:
            return getattr(extension, name)

    monkeypatch.setattr(native, "_native", NoFullSnapshot())
    values = fpstreams.flow(range(1, 6)).with_engine("native")

    assert values.aggregate(rows=fpstreams.agg.count()) == {"rows": 5}
    assert values.aggregate(low=fpstreams.agg.min()) == {"low": 1}
    assert values.aggregate(high=fpstreams.agg.max()) == {"high": 5}
    assert values.aggregate(last=fpstreams.agg.last()) == {"last": 5}
    assert values.aggregate(first=fpstreams.agg.first()) == {"first": 1}
    assert values.aggregate(total=fpstreams.agg.sum()) == {"total": 15}
    assert values.aggregate(mean=fpstreams.agg.mean()) == {"mean": 3.0}
    assert values.aggregate(
        rows=fpstreams.agg.count(),
        total=fpstreams.agg.sum(),
        low=fpstreams.agg.min(),
    ) == {"rows": 5, "total": 15, "low": 1}


def test_named_aggregate_compiles_its_collector_program_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The native attempt and Python fallback share one compiled collector program."""
    from fpstreams.streams import flow_terminals

    calls = 0
    compile_program = flow_terminals.compile_aggregations

    def tracked(items):
        nonlocal calls
        calls += 1
        return compile_program(items)

    monkeypatch.setattr(flow_terminals, "compile_aggregations", tracked)
    result = fpstreams.flow([1, 2.5]).aggregate(
        rows=fpstreams.agg.count(), total=fpstreams.agg.sum()
    )

    assert result == {"rows": 2, "total": 3.5}
    assert calls == 1


@pytest.mark.skipif(sys.version_info < (3, 12), reason="3.11 float sum is sequential")
def test_float_single_sum_reuses_the_compensated_scalar_kernel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A total-only f64 aggregate avoids a slower mask without changing sum semantics."""
    from fpstreams.execution import native

    extension = native._native

    class NoMaskedTotal:
        def aggregate_f64_masked(self, *_args: object) -> object:
            raise AssertionError("f64 total should use the compensated scalar terminal")

        def __getattr__(self, name: str) -> object:
            return getattr(extension, name)

    monkeypatch.setattr(native, "_native", NoMaskedTotal())
    values = fpstreams.flow([1e16, 1.0, -1e16]).map(fpstreams.fitem + 0.0).with_engine("native")

    assert values.aggregate(total=fpstreams.agg.sum()) == {"total": 1.0}


def test_statistics_aggregate_failure_falls_back_without_a_second_native_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One failed conversion enters Python directly instead of invoking another Rust ABI."""
    from fpstreams.streams import flow_terminals

    monkeypatch.setattr(
        flow_terminals,
        "try_native_statistics",
        lambda *_args, **_kwargs: (False, None),
    )

    def reject_retry(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("statistics failure must not retry native conversion")

    monkeypatch.setattr(flow_terminals, "try_native_aggregate", reject_retry)

    assert fpstreams.flow(range(1, 4)).aggregate(mean=fpstreams.agg.mean()) == {"mean": 2.0}


def test_scalar_aggregate_failure_falls_back_without_a_second_native_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A rejected scalar aggregate never retries conversion through the snapshot ABI."""
    from fpstreams.streams import flow_terminals

    monkeypatch.setattr(
        flow_terminals,
        "try_native_terminal",
        lambda *_args, **_kwargs: (False, None),
    )

    def reject_retry(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("scalar failure must not retry native conversion")

    monkeypatch.setattr(flow_terminals, "try_native_aggregate", reject_retry)

    assert fpstreams.flow(range(1, 4)).aggregate(low=fpstreams.agg.min()) == {"low": 1}


def test_legacy_native_extension_falls_back_to_the_full_aggregate_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Wheels predating aggregate masks retain the established single-pass result."""
    from fpstreams.execution import native

    extension = native._native
    calls = 0

    class LegacyExtension:
        def aggregate_i64_range(self, *args: object) -> object:
            nonlocal calls
            calls += 1
            return extension.aggregate_i64_range(*args)

        def __getattr__(self, name: str) -> object:
            if name.endswith("_masked"):
                raise AttributeError(name)
            return getattr(extension, name)

    monkeypatch.setattr(native, "_native", LegacyExtension())
    result = (
        fpstreams.flow(range(1, 6))
        .with_engine("native")
        .aggregate(total=fpstreams.agg.sum(), low=fpstreams.agg.min())
    )

    assert result == {"total": 15, "low": 1}
    assert calls == 1


@pytest.mark.parametrize(
    ("source", "kind", "terminal", "expected"),
    [
        ([7, "bad tail"], "i64", 5, (True, 7)),
        ((1.5, "bad tail"), "f64", 6, (True, 1.0)),
        ([0, "bad tail"], "i64", 7, (True, 0)),
    ],
)
def test_native_container_probe_short_circuits_before_extracting_bad_tail(
    source: object, kind: str, terminal: int, expected: tuple[bool, object]
) -> None:
    """A decided terminal must not validate or convert an unreachable container tail."""
    from fpstreams import _native

    probe = getattr(_native, f"terminal_{kind}_probe")
    assert probe(source, [], terminal, 256) == expected


def test_forced_native_identity_first_does_not_scan_a_bad_container_tail() -> None:
    """Identity terminal selection must not pre-scan a tail the terminal cannot reach."""
    assert fpstreams.flow([1, "bad tail"]).with_engine("native").first() == 1


@pytest.mark.parametrize(
    ("values", "terminal"),
    [
        ([1.0, 1], "all"),
        ([0.0, 1], "any"),
        ([0, True], "any"),
        ([1, True], "all"),
    ],
)
def test_forced_native_identity_rejects_mixed_values_when_the_terminal_reaches_them(
    values: list[object], terminal: str
) -> None:
    """A short-circuit identity probe may defer type checks, never coerce them."""
    with pytest.raises(fpstreams.NativeUnsupportedError):
        getattr(fpstreams.flow(values).with_engine("native"), terminal)()


@pytest.mark.parametrize(
    ("values", "terminal", "expected"),
    [
        ([1.0, 1], "first", 1.0),
        ([0.0, 1], "all", False),
        ([1, True], "any", True),
        ([0, True], "first", 0),
    ],
)
def test_forced_native_identity_leaves_an_unreached_mixed_tail_unchecked(
    values: list[object], terminal: str, expected: object
) -> None:
    """The only permitted delayed validation is for a tail the terminal cannot observe."""
    assert getattr(fpstreams.flow(values).with_engine("native"), terminal)() == expected


def test_native_probe_rejects_reentrant_integer_coercion_without_mutating_the_list() -> None:
    """Probe extraction must not invoke __index__ or observe an index shifted by it."""
    from fpstreams import _native

    values: list[object] = [0]

    class MutatingIndex:
        def __index__(self) -> int:
            values.clear()
            return 1

    values.append(MutatingIndex())
    values.extend([0] * 8)
    with pytest.raises(TypeError, match="i64"):
        _native.terminal_i64_probe(values, [], 6, 256)
    assert len(values) == 10

    with pytest.raises(fpstreams.NativeUnsupportedError, match="i64 integers"):
        fpstreams.flow(values).filter(fpstreams.item != 0).with_engine("native").any()
    assert fpstreams.flow(values).filter(fpstreams.item != 0).any() is True
    assert len(values) == 10


def test_forced_native_probe_preserves_stage_state_and_falls_back_after_budget() -> None:
    """A bounded undecided probe restarts the legacy full scan with the same result."""
    values = list(range(300))
    pipeline = (
        fpstreams.flow(values)
        .drop(2)
        .take(260)
        .filter(fpstreams.item % 2 == 0)
        .map(fpstreams.item + 1)
        .with_engine("native")
    )
    assert pipeline.first() == 3
    assert pipeline.all() is True


def test_native_probe_restarts_the_bulk_kernel_only_after_an_undecided_budget(monkeypatch) -> None:
    """An incomplete bounded probe must preserve the legacy full-scan result."""
    from fpstreams.execution import native
    from fpstreams.planning.native import NativeProgram

    calls = {"probe": 0, "bulk": 0}
    probe = native._native.terminal_i64_probe
    bulk = native._native.terminal_i64

    def counted_probe(*args: object) -> object:
        calls["probe"] += 1
        return probe(*args)

    def counted_bulk(*args: object) -> object:
        calls["bulk"] += 1
        return bulk(*args)

    monkeypatch.setattr(native._native, "terminal_i64_probe", counted_probe)
    monkeypatch.setattr(native._native, "terminal_i64", counted_bulk)

    assert native.execute_terminal(NativeProgram([0] * 300 + [1], (), "i64"), "any") == 1
    assert calls == {"probe": 1, "bulk": 1}


def test_native_probe_preserves_forced_errors_and_auto_python_fallback() -> None:
    """A type error before a decision is strict only for forced-native execution."""
    with pytest.raises(fpstreams.NativeUnsupportedError, match="i64 integers"):
        fpstreams.flow([1, "bad tail"]).with_engine("native").all()

    values = [0] * 300 + [1.5]
    assert fpstreams.flow(values).filter(fpstreams.item > 0).any() is True


def test_auto_terminal_avoids_a_legacy_extension_without_container_probes(monkeypatch) -> None:
    """Old wheels must not make auto short-circuit terminals copy a large container."""
    from fpstreams.planning import native

    real_extension = fpstreams._native

    class LegacyExtension:
        def __getattr__(self, name: str) -> object:
            if name == "terminal_i64_probe":
                raise AttributeError(name)
            return getattr(real_extension, name)

    monkeypatch.setattr(fpstreams, "_native", LegacyExtension())
    native._EXTENSION_CAPABILITY_CACHE.clear()
    native._PROBE_CAPABILITY_CACHE.clear()
    decision = native.select_terminal_engine(
        fpstreams.flow(list(range(300))).map(fpstreams.item + 1)._pipeline,
        "first",
    )
    assert decision.engine == "python"


def test_auto_float_terminal_avoids_a_legacy_extension_without_container_probes(
    monkeypatch,
) -> None:
    """The old-wheel guard applies to f64 containers as well as i64 ones."""
    from fpstreams.planning import native

    real_extension = fpstreams._native

    class LegacyExtension:
        def __getattr__(self, name: str) -> object:
            if name == "terminal_f64_probe":
                raise AttributeError(name)
            return getattr(real_extension, name)

    monkeypatch.setattr(fpstreams, "_native", LegacyExtension())
    native._EXTENSION_CAPABILITY_CACHE.clear()
    native._PROBE_CAPABILITY_CACHE.clear()
    decision = native.select_terminal_engine(
        fpstreams.flow([float(value) for value in range(300)]).map(fpstreams.fitem + 1.0)._pipeline,
        "first",
    )
    assert decision.engine == "python"


def test_container_short_circuit_metadata_explains_the_conditional_bulk_copy() -> None:
    """Planning metadata describes the deferred bulk copy without changing its schema."""
    from fpstreams.planning import native

    decision = native.select_terminal_engine(
        fpstreams.flow(list(range(300))).map(fpstreams.item + 1).with_engine("native")._pipeline,
        "first",
    )
    assert decision.scans_source and decision.copies_source
    assert "bounded probe; only undecided fallback bulk-copies" in decision.reason


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
    "sample_count",
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
        assert len(result["samples_seconds"]) == result["sample_count"]
        name = result["name"]
        if name in {
            "fpstreams_operation/sync/map_parallel",
            "fpstreams_operation/rows/group_spill_aggregate",
        }:
            expected_count = 21
        elif name.startswith(
            (
                "fpstreams_group/dict/callable_",
                "fpstreams_group/mappingproxy/callable_",
                "fpstreams_group/nominal_mapping/callable_",
                "fpstreams_join/namedtuple/callable/",
            )
        ):
            expected_count = 15
        else:
            expected_count = 2
        assert result["sample_count"] == expected_count
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


def test_large_python_expression_benchmarks_require_scalar_fusion_speedup() -> None:
    scenarios = benchmark._integer_pipeline_scenarios(4_096, native_available=False)
    candidates = [
        scenario for scenario in scenarios if scenario.name.startswith("fpstreams_python/")
    ]

    assert len(candidates) == 4
    assert all(candidate.baseline.startswith("fpstreams_lambda/") for candidate in candidates)
    assert all(candidate.maximum_ratio == 0.75 for candidate in candidates)


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
            "src/fpstreams/execution/async_scheduler.py": _coverage_file(90),
            "src/fpstreams/execution/async_map.py": _coverage_file(90),
            "src/fpstreams/execution/async_merge.py": _coverage_file(90),
            "src/fpstreams/execution/async_timers.py": _coverage_file(90),
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
