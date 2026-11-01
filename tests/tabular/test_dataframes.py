"""tabular dataframes regression contracts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

import fpstreams


def test_dataframe_global_aggregate_observes_lifecycle_mutated_during_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A delayed dataframe conversion may replace collector hooks before its first row."""
    from fpstreams.tabular import dataframe

    events: list[str] = []
    total = fpstreams.agg.sum("value")
    original_table = dataframe._without_pandas_index

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
    monkeypatch.setattr(dataframe, "_without_pandas_index", observed_table)

    assert aggregated.to_list() == [{"total": 50, "rows": 2}]
    assert events == ["open:mutate-step", "step:2", "step:3"]


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
