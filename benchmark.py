"""Run fpstreams timing scenarios and emit human- and machine-readable regression data."""

from __future__ import annotations

import argparse
import asyncio
import fnmatch
import json
import platform
import statistics
import sys
import time
import tracemalloc
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from tempfile import TemporaryDirectory
from types import MappingProxyType
from typing import Any, Literal, NamedTuple

import fpstreams
from fpstreams import fitem, flow, item
from fpstreams.planning.gather import Gatherer
from fpstreams.planning.logical import Pipeline
from fpstreams.planning.source import Source
from fpstreams.planning.sync import DropOp, FilterOp, MapOp, TakeOp

Backend = Literal["python-builtin", "python", "native", "auto", "numpy", "pandas"]
Task = Callable[[], object]
Normalizer = Callable[[object], object]
_MIN_FIRST_ROW_SAMPLES = 15
_SCALAR_FUSION_GUARD_MIN_SIZE = 4_096


@dataclass(frozen=True, slots=True)
class Scenario:
    """One benchmark workload plus the sampling needed for stable release evidence."""

    name: str
    task: Task
    backend: Backend
    source_kind: str
    terminal: str
    baseline: str | None
    first_row_task: Task | None = None
    minimum_repeats: int = 1
    maximum_ratio: float | None = None


@dataclass(frozen=True, slots=True)
class CompetitivePair:
    """Two equivalent workloads plus their library-specific result normalizers."""

    reference: Scenario
    candidate: Scenario
    normalize_reference: Normalizer
    normalize_candidate: Normalizer


def measure(function: Task, repeats: int) -> list[float]:
    durations: list[float] = []
    for _ in range(repeats):
        started = time.perf_counter()
        function()
        durations.append(time.perf_counter() - started)
    return durations


def _record(scenario: Scenario, repeats: int) -> dict[str, Any]:
    # Thread startup, tiny temporary-file workloads, and allocation-heavy group
    # results can be bimodal on CI, so those scenarios request a larger sample.
    sample_count = max(repeats, scenario.minimum_repeats)
    samples = measure(scenario.task, sample_count)
    tracemalloc.start()
    try:
        scenario.task()
        _, peak_allocation = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    record: dict[str, Any] = {
        "name": scenario.name,
        "sample_count": sample_count,
        "samples_seconds": samples,
        "median_seconds": statistics.median(samples),
        "stdev_seconds": statistics.stdev(samples) if len(samples) > 1 else 0.0,
        "backend": scenario.backend,
        "source_kind": scenario.source_kind,
        "terminal": scenario.terminal,
        "baseline": scenario.baseline,
        "maximum_ratio": scenario.maximum_ratio,
        "resources": {"peak_allocation_bytes": peak_allocation},
    }
    if scenario.first_row_task is not None:
        first_row_samples = measure(
            scenario.first_row_task,
            max(sample_count, _MIN_FIRST_ROW_SAMPLES),
        )
        record["first_row_samples_seconds"] = first_row_samples
        record["first_row_seconds"] = statistics.median(first_row_samples)
    return record


def _builtin_task(source: Sequence[int] | range, terminal: str) -> Task:
    if terminal == "sum":
        return lambda: sum(source)
    if terminal == "count":
        return lambda: len(source)
    raise ValueError(f"unsupported identity terminal {terminal!r}")


def _flow_task(source: Sequence[int] | range, backend: Backend, terminal: str) -> Task:
    pipeline = flow(source)
    selected = pipeline.with_engine(backend) if backend in {"python", "native"} else pipeline
    if terminal == "sum":
        return selected.sum
    if terminal == "count":
        return selected.count
    raise ValueError(f"unsupported identity terminal {terminal!r}")


def _python_map_filter(source: Sequence[int] | range) -> list[int]:
    return [value * 3 + 1 for value in source if (value * 3 + 1) % 2 == 0]


def _sum_task(task: Callable[[], Sequence[int]]) -> int:
    return sum(task())


def _numpy_map_filter_sum(values: Any) -> Any:
    """Run the eager NumPy equivalent of fpstreams' fused numeric expression pipeline."""
    mapped = values * 3 + 1
    return mapped[mapped % 2 == 0].sum()


def _pandas_group_sum(frame: Any) -> Any:
    """Run a stable-order sum against an already-materialized pandas table."""
    return frame.groupby("key", sort=False, as_index=False).agg(total=("value", "sum"))


def _normalize_pandas_groups(value: object) -> tuple[tuple[int, int], ...]:
    """Convert a two-column pandas result to a dependency-neutral ordered value."""
    return tuple((int(key), int(total)) for key, total in value.itertuples(index=False, name=None))  # type: ignore[attr-defined]


def _normalize_fpstreams_groups(value: object) -> tuple[tuple[int, int], ...]:
    """Convert fpstreams group rows to the same ordered value used for correctness checks."""
    return tuple((int(row["key"]), int(row["total"])) for row in value)  # type: ignore[union-attr]


def _competitive_pairs(size: int) -> tuple[CompetitivePair, ...]:
    """Build opt-in compute-only comparisons without extending the frozen release suite."""
    try:
        import numpy as np
        import pandas as pd  # type: ignore[import-untyped]
    except ImportError as error:
        raise RuntimeError(
            "competitive benchmarks require the 'data' extra: pip install fpstreams[data]"
        ) from error

    numeric_values = np.arange(size, dtype=np.int64)
    numeric_pipeline = flow(range(size)).map(item * 3 + 1).filter(item % 2 == 0)

    # Both tabular competitors receive a reusable, already-materialized source. Construction
    # cost is excluded so this pair measures the group operator rather than adapter ingestion.
    group_count = min(64, size)
    records = tuple((index % group_count, index) for index in range(size))
    frame = pd.DataFrame.from_records(records, columns=("key", "value"))
    grouped_rows = fpstreams.rows(records).group_by(key=0).aggregate(total=fpstreams.agg.sum(1))

    return (
        CompetitivePair(
            Scenario(
                "competitive/numpy/array/map_filter/sum",
                partial(_numpy_map_filter_sum, numeric_values),
                "numpy",
                "ndarray",
                "sum",
                None,
            ),
            Scenario(
                "competitive/fpstreams/range/map_filter/sum",
                numeric_pipeline.sum,
                "auto",
                "range",
                "sum",
                "competitive/numpy/array/map_filter/sum",
            ),
            int,
            int,
        ),
        CompetitivePair(
            Scenario(
                "competitive/pandas/dataframe/group_sum",
                partial(_pandas_group_sum, frame),
                "pandas",
                "dataframe",
                "group_sum",
                None,
            ),
            Scenario(
                "competitive/fpstreams/rows/group_sum",
                grouped_rows.to_list,
                "auto",
                "tuple_rows",
                "group_sum",
                "competitive/pandas/dataframe/group_sum",
            ),
            _normalize_pandas_groups,
            _normalize_fpstreams_groups,
        ),
    )


def _identity_scenarios(size: int, *, include_tuple: bool) -> list[Scenario]:
    sources: list[tuple[str, Sequence[int] | range]] = [
        ("list", list(range(size))),
        ("range", range(size)),
    ]
    if include_tuple:
        sources.append(("tuple", tuple(range(size))))

    scenarios: list[Scenario] = []
    for source_kind, source in sources:
        for terminal in ("sum", "count"):
            builtin_name = f"python_builtin/{source_kind}/identity/{terminal}"
            python_name = f"fpstreams_python/{source_kind}/identity/{terminal}"
            scenarios.extend(
                [
                    Scenario(
                        builtin_name,
                        _builtin_task(source, terminal),
                        "python-builtin",
                        source_kind,
                        terminal,
                        None,
                    ),
                    Scenario(
                        python_name,
                        _flow_task(source, "python", terminal),
                        "python",
                        source_kind,
                        terminal,
                        builtin_name,
                    ),
                    Scenario(
                        f"fpstreams_auto/{source_kind}/identity/{terminal}",
                        _flow_task(source, "auto", terminal),
                        "auto",
                        source_kind,
                        terminal,
                        python_name,
                    ),
                ]
            )
    return scenarios


def _integer_pipeline_scenarios(size: int, native_available: bool) -> list[Scenario]:
    scenarios: list[Scenario] = []
    for source_kind, source in (("list", list(range(size))), ("range", range(size))):
        builtin_values = partial(_python_map_filter, source)
        pipeline = flow(source).map(item * 3 + 1).filter(item % 2 == 0)
        lambda_pipeline = (
            flow(source)
            .map(lambda value: value * 3 + 1)
            .filter(lambda value: value % 2 == 0)
            .with_engine("python")
        )
        for terminal in ("list", "sum"):
            builtin_name = f"python_builtin/{source_kind}/map_filter/{terminal}"
            builtin_task: Task = (
                builtin_values if terminal == "list" else partial(_sum_task, builtin_values)
            )
            scenarios.append(
                Scenario(
                    builtin_name,
                    builtin_task,
                    "python-builtin",
                    source_kind,
                    terminal,
                    None,
                )
            )
            lambda_name = f"fpstreams_lambda/{source_kind}/map_filter/{terminal}"
            scenarios.append(
                Scenario(
                    lambda_name,
                    lambda_pipeline.to_list if terminal == "list" else lambda_pipeline.sum,
                    "python",
                    source_kind,
                    terminal,
                    builtin_name,
                )
            )
            python_name = f"fpstreams_python/{source_kind}/map_filter/{terminal}"
            python_pipeline = pipeline.with_engine("python")
            scenarios.append(
                Scenario(
                    python_name,
                    python_pipeline.to_list if terminal == "list" else python_pipeline.sum,
                    "python",
                    source_kind,
                    terminal,
                    lambda_name,
                    maximum_ratio=0.75 if size >= _SCALAR_FUSION_GUARD_MIN_SIZE else None,
                )
            )
            if native_available:
                native_pipeline = pipeline.with_engine("native")
                scenarios.append(
                    Scenario(
                        f"fpstreams_native/{source_kind}/map_filter/{terminal}",
                        native_pipeline.to_list if terminal == "list" else native_pipeline.sum,
                        "native",
                        source_kind,
                        terminal,
                        python_name,
                    )
                )
            scenarios.append(
                Scenario(
                    f"fpstreams_auto/{source_kind}/map_filter/{terminal}",
                    pipeline.to_list if terminal == "list" else pipeline.sum,
                    "auto",
                    source_kind,
                    terminal,
                    python_name,
                )
            )
    return scenarios


def _float_scenarios(size: int, native_available: bool) -> list[Scenario]:
    values = [value / 10 for value in range(size)]
    pipeline = flow(values).map(fitem * 1.25 + 0.5).filter(fitem > 10.0)
    scenarios: list[Scenario] = []
    python_pipeline = pipeline.with_engine("python")
    lambda_pipeline = (
        flow(values)
        .map(lambda value: value * 1.25 + 0.5)
        .filter(lambda value: value > 10.0)
        .with_engine("python")
    )
    for terminal in ("list", "sum"):
        lambda_name = f"fpstreams_lambda/list/float_map_filter/{terminal}"
        scenarios.append(
            Scenario(
                lambda_name,
                lambda_pipeline.to_list if terminal == "list" else lambda_pipeline.sum,
                "python",
                "list",
                terminal,
                None,
            )
        )
        python_name = f"fpstreams_python/list/float_map_filter/{terminal}"
        scenarios.append(
            Scenario(
                python_name,
                python_pipeline.to_list if terminal == "list" else python_pipeline.sum,
                "python",
                "list",
                terminal,
                lambda_name,
            )
        )
        if native_available:
            native_pipeline = pipeline.with_engine("native")
            scenarios.append(
                Scenario(
                    f"fpstreams_native/list/float_map_filter/{terminal}",
                    native_pipeline.to_list if terminal == "list" else native_pipeline.sum,
                    "native",
                    "list",
                    terminal,
                    python_name,
                )
            )
        scenarios.append(
            Scenario(
                f"fpstreams_auto/list/float_map_filter/{terminal}",
                pipeline.to_list if terminal == "list" else pipeline.sum,
                "auto",
                "list",
                terminal,
                python_name,
            )
        )
    return scenarios


def _small_plan_scenario() -> Scenario:
    pipeline = flow(range(4)).map(item + 1)
    return Scenario(
        "fpstreams_auto/range/small_plan/explain",
        lambda: pipeline.explain("list").to_dict(),
        "auto",
        "range",
        "explain",
        None,
    )


def _logical_compile_scenarios() -> list[Scenario]:
    """Measure construction of the canonical immutable logical pipeline."""

    def current_plan_task() -> object:
        return Pipeline(
            Source.from_iterable(range(100)),
            (
                MapOp(lambda value: value + 1),
                FilterOp(lambda value: value > 3),
                DropOp(2),
                TakeOp(10),
            ),
        )

    def logical_task() -> object:
        return (
            flow(range(100))
            .map(lambda value: value + 1)
            .filter(lambda value: value > 3)
            .drop(2)
            .take(10)
            ._logical_plan
        )

    return [
        Scenario(
            "fpstreams_planning/current_plan/iterate",
            current_plan_task,
            "python",
            "range",
            "iterate",
            None,
        ),
        Scenario(
            "fpstreams_planning/logical_compile/iterate",
            logical_task,
            "python",
            "range",
            "iterate",
            "fpstreams_planning/current_plan/iterate",
        ),
    ]


def _sync_operation_scenarios() -> list[Scenario]:
    """Time one public execution for every synchronous operation-union member."""
    source = [1, 2, 3, 4]
    gatherer = Gatherer(lambda: None, lambda _state, value: (value,), lambda _state: ())
    builders: list[tuple[str, Callable[[Any], Any]]] = [
        ("map", lambda stream: stream.map(lambda value: value + 1)),
        (
            "map_parallel",
            lambda stream: stream.map_parallel(lambda value: value + 1, workers=1, buffer=1),
        ),
        ("tap", lambda stream: stream.tap(lambda _value: None)),
        ("filter", lambda stream: stream.filter(lambda value: value % 2 == 0)),
        ("flat_map", lambda stream: stream.flat_map(lambda value: (value,))),
        ("take", lambda stream: stream.take(2)),
        ("drop", lambda stream: stream.drop(1)),
        ("take_while", lambda stream: stream.take_while(lambda value: value < 3)),
        (
            "take_while_inclusive",
            lambda stream: stream.take_while_inclusive(lambda value: value < 3),
        ),
        ("drop_while", lambda stream: stream.drop_while(lambda value: value < 3)),
        ("unique", lambda stream: stream.unique()),
        ("chunk", lambda stream: stream.chunk(2)),
        ("window", lambda stream: stream.window(2, step=1)),
        ("group_runs", lambda stream: stream.group_runs(lambda value: value % 2)),
        ("pairwise", lambda stream: stream.pairwise()),
        ("enumerate", lambda stream: stream.enumerate()),
        ("zip", lambda stream: stream.zip([5, 6, 7, 8])),
        ("zip_longest", lambda stream: stream.zip_longest([5, 6])),
        ("intersperse", lambda stream: stream.intersperse(0)),
        ("concat", lambda stream: stream.concat([5, 6])),
        ("cross", lambda stream: stream.cross([5, 6], max_right=2)),
        ("scan", lambda stream: stream.scan(0, lambda total, value: total + value)),
        ("scan_right", lambda stream: stream.scan_right(0, lambda value, total: value + total)),
        ("sort_by", lambda stream: stream.sort_by(lambda value: -value)),
        ("gather", lambda stream: stream.gather(gatherer)),
        ("prepend", lambda stream: stream.prepend(0)),
        ("append", lambda stream: stream.append(5)),
        ("map_first", lambda stream: stream.map_first(lambda value: value + 1)),
        ("map_last", lambda stream: stream.map_last(lambda value: value + 1)),
        (
            "collapse",
            lambda stream: stream.collapse(
                lambda _left, _right: True, lambda left, right: left + right
            ),
        ),
    ]
    return [
        Scenario(
            f"fpstreams_operation/sync/{name}",
            lambda builder=builder: builder(flow(source)).to_list(),
            "python",
            "operation",
            "list",
            None,
            lambda builder=builder: next(iter(builder(flow(source)))),
            minimum_repeats=21 if name == "map_parallel" else 1,
        )
        for name, builder in builders
    ]


def _async_operation_scenarios() -> list[Scenario]:
    """Time one public execution for every asynchronous operation-union member."""
    source = [1, 2, 3, 4]
    builders: list[tuple[str, Callable[[Any], Any]]] = [
        ("map_async", lambda stream: stream.map_async(lambda value: value + 1, concurrency=2)),
        ("filter", lambda stream: stream.filter(lambda value: value % 2 == 0)),
        ("tap", lambda stream: stream.tap(lambda _value: None)),
        ("flat_map", lambda stream: stream.flat_map(lambda value: (value,))),
        ("merge", lambda stream: stream.merge([5, 6])),
        ("merge_map", lambda stream: stream.merge_map(lambda value: (value,), concurrency=2)),
        ("switch_map", lambda stream: stream.switch_map(lambda value: (value,))),
        ("combine_latest", lambda stream: stream.combine_latest([5, 6])),
        ("timeout", lambda stream: stream.timeout(0.01)),
        ("debounce", lambda stream: stream.debounce(0)),
        ("buffer_timeout", lambda stream: stream.buffer_timeout(2, 0.01)),
        ("delay", lambda stream: stream.delay(0.000001)),
        ("throttle", lambda stream: stream.throttle(4, per=0.01)),
        ("take", lambda stream: stream.take(2)),
        ("drop", lambda stream: stream.drop(1)),
        ("take_while", lambda stream: stream.take_while(lambda value: value < 3)),
        (
            "take_while_inclusive",
            lambda stream: stream.take_while_inclusive(lambda value: value < 3),
        ),
        ("drop_while", lambda stream: stream.drop_while(lambda value: value < 3)),
        ("chunk", lambda stream: stream.chunk(2)),
        ("batch_by_size", lambda stream: stream.batch_by_size(2, get_size=lambda _value: 1)),
        ("window", lambda stream: stream.window(2, step=1)),
        ("pairwise", lambda stream: stream.pairwise()),
        ("group_runs", lambda stream: stream.group_runs(lambda value: value % 2)),
        ("fold", lambda stream: stream.fold(lambda: 0, lambda total, value: total + value)),
        ("unique", lambda stream: stream.unique()),
        ("enumerate", lambda stream: stream.enumerate()),
        ("zip", lambda stream: stream.zip([5, 6, 7, 8])),
        ("zip_longest", lambda stream: stream.zip_longest([5, 6])),
        ("intersperse", lambda stream: stream.intersperse(0)),
        ("concat", lambda stream: stream.concat([5, 6])),
        ("cross", lambda stream: stream.cross([5, 6], max_right=2)),
        ("scan", lambda stream: stream.scan(0, lambda total, value: total + value)),
        ("scan_right", lambda stream: stream.scan_right(0, lambda value, total: value + total)),
        ("prepend", lambda stream: stream.prepend(0)),
        ("append", lambda stream: stream.append(5)),
        ("map_first", lambda stream: stream.map_first(lambda value: value + 1)),
        ("map_last", lambda stream: stream.map_last(lambda value: value + 1)),
        (
            "collapse",
            lambda stream: stream.collapse(
                lambda _left, _right: True, lambda left, right: left + right
            ),
        ),
    ]

    async def first_row(builder: Callable[[Any], Any]) -> Any:
        iterator = builder(fpstreams.aflow(source)).__aiter__()
        try:
            return await anext(iterator)
        finally:
            close = getattr(iterator, "aclose", None)
            if callable(close):
                await close()

    return [
        Scenario(
            f"fpstreams_operation/async/{name}",
            lambda builder=builder: asyncio.run(builder(fpstreams.aflow(source)).to_list()),
            "python",
            "async-operation",
            "list",
            None,
            lambda builder=builder: asyncio.run(first_row(builder)),
        )
        for name, builder in builders
    ]


def _rows_operation_scenarios() -> list[Scenario]:
    """Time public in-memory Rows, grouping, and relational execution paths."""
    records = [
        {
            "id": 1,
            "team": "a",
            "value": 1,
            "nullable": None,
            "tags": ["x", "y"],
            "nested": {"n": 1},
        },
        {"id": 2, "team": "a", "value": 2, "nullable": 2, "tags": ["z"], "nested": {"n": 2}},
        {"id": 3, "team": "b", "value": 3, "nullable": None, "tags": [], "nested": {"n": 3}},
    ]
    # Python deliberately randomizes string hashes between processes.  With only
    # two spill partitions, the two team strings can therefore either collide or
    # open two files, creating a bimodal benchmark unrelated to code performance.
    # Numeric equivalents keep the workload and group cardinality unchanged while
    # making the partition shape reproducible across baseline processes.
    spill_records = [{**record, "team": 0 if record["team"] == "a" else 1} for record in records]

    def rows() -> Any:
        return fpstreams.rows(records)

    builders: list[tuple[str, Callable[[], Any]]] = [
        ("to_list", lambda: rows().to_list()),
        ("count", lambda: rows().count()),
        ("take", lambda: rows().take(2).to_list()),
        ("skip", lambda: rows().skip(1).to_list()),
        ("unique_by", lambda: rows().unique_by("team").to_list()),
        ("filter", lambda: rows().filter(lambda row: row["value"] > 1).to_list()),
        ("where", lambda: rows().where(team="a").to_list()),
        (
            "with_columns",
            lambda: rows().with_columns(next_value=lambda row: row["value"] + 1).to_list(),
        ),
        ("rename", lambda: rows().rename(value="amount").to_list()),
        ("drop", lambda: rows().drop("nested").to_list()),
        ("cast", lambda: rows().cast(value=str).to_list()),
        ("fill_nulls", lambda: rows().fill_nulls(nullable=0).to_list()),
        ("drop_nulls", lambda: rows().drop_nulls("nullable").to_list()),
        ("explode", lambda: rows().explode("tags", outer=True).to_list()),
        ("unnest", lambda: rows().unnest("nested").to_list()),
        ("unpivot", lambda: rows().unpivot("value", "nullable").to_list()),
        (
            "pivot",
            lambda: (
                fpstreams.rows(
                    [{"team": "a", "key": "x", "amount": 1}, {"team": "a", "key": "y", "amount": 2}]
                )
                .pivot(index="team", columns="key", values="amount")
                .to_list()
            ),
        ),
        ("select", lambda: rows().select("id", amount="value").to_list()),
        ("sort_by", lambda: rows().sort_by("value", reverse=True).to_list()),
        ("external_sort_by", lambda: rows().external_sort_by("value", buffer_size=1).to_list()),
        ("aggregate", lambda: rows().aggregate(total=fpstreams.agg.sum("value")).to_list()),
        (
            "group_aggregate",
            lambda: rows().group_by("team").aggregate(total=fpstreams.agg.sum("value")).to_list(),
        ),
        (
            "group_spill_aggregate",
            lambda: (
                fpstreams.rows(spill_records)
                .group_by("team")
                .spill(partitions=2)
                .aggregate(total=fpstreams.agg.sum("value"))
                .to_list()
            ),
        ),
        (
            "join",
            lambda: (
                rows()
                .join([{"id": 1, "label": "one"}, {"id": 2, "label": "two"}], on="id")
                .to_list()
            ),
        ),
    ]
    return [
        Scenario(
            f"fpstreams_operation/rows/{name}",
            task,
            "python",
            "rows-operation",
            "list",
            None,
            minimum_repeats=21 if name == "group_spill_aggregate" else 1,
        )
        for name, task in builders
    ]


def _callable_group_scenarios(size: int) -> list[Scenario]:
    """Guard the fixed callable-key/value group loops with scalable workloads."""
    cardinality = min(size, 64)
    records = [(index % cardinality, index) for index in range(size)]

    def key(row: tuple[int, int]) -> int:
        return row[0]

    def value(row: tuple[int, int]) -> int:
        return row[1]

    source = fpstreams.rows(records).with_engine("python")
    tasks = (
        (
            "callable_key/count",
            source.group_by(key=key).aggregate(count=fpstreams.agg.count()).to_list,
        ),
        (
            "callable_key/count_sum_direct",
            source.group_by(key=key)
            .aggregate(count=fpstreams.agg.count(), total=fpstreams.agg.sum(1))
            .to_list,
        ),
        (
            "callable_key_value/count_sum",
            source.group_by(key=key)
            .aggregate(count=fpstreams.agg.count(), total=fpstreams.agg.sum(value))
            .to_list,
        ),
        (
            "callable_value/count_sum",
            source.group_by(key=0)
            .aggregate(count=fpstreams.agg.count(), total=fpstreams.agg.sum(value))
            .to_list,
        ),
    )
    scenarios = [
        Scenario(
            f"fpstreams_group/tuple/{name}",
            task,
            "python",
            "tuple_rows",
            "group_aggregate",
            None,
        )
        for name, task in tasks
    ]
    high_cardinality_size = min(size, 100_000)
    high_cardinality = min(high_cardinality_size, 30_000)
    dictionary_records = [
        {"key": index % high_cardinality, "value": index} for index in range(high_cardinality_size)
    ]
    dictionary_source = fpstreams.rows(dictionary_records).with_engine("python")
    dictionary_tasks: tuple[tuple[str, Task], ...] = (
        (
            "callable_key/count_sum_direct",
            dictionary_source.group_by(key=lambda row: row["key"])
            .aggregate(count=fpstreams.agg.count(), total=fpstreams.agg.sum("value"))
            .to_list,
        ),
        (
            "callable_value/count_sum",
            dictionary_source.group_by(key="key")
            .aggregate(
                count=fpstreams.agg.count(),
                total=fpstreams.agg.sum(lambda row: row["value"]),
            )
            .to_list,
        ),
    )
    for name, task in dictionary_tasks:
        scenarios.append(
            Scenario(
                f"fpstreams_group/dict/{name}/high_cardinality",
                task,
                "python",
                "dict_rows",
                "group_aggregate",
                None,
                minimum_repeats=15,
            )
        )
    dictionary_baselines = {
        name: f"fpstreams_group/dict/{name}/high_cardinality" for name, _task in dictionary_tasks
    }
    auto_dictionary_source = fpstreams.rows(dictionary_records)
    auto_dictionary_tasks: tuple[tuple[str, Task], ...] = (
        (
            "callable_key/count_sum_direct",
            auto_dictionary_source.group_by(key=lambda row: row["key"])
            .aggregate(count=fpstreams.agg.count(), total=fpstreams.agg.sum("value"))
            .to_list,
        ),
        (
            "callable_value/count_sum",
            auto_dictionary_source.group_by(key="key")
            .aggregate(
                count=fpstreams.agg.count(),
                total=fpstreams.agg.sum(lambda row: row["value"]),
            )
            .to_list,
        ),
    )
    for name, task in auto_dictionary_tasks:
        scenarios.append(
            Scenario(
                f"fpstreams_group/dict/{name}/high_cardinality/auto",
                task,
                "auto",
                "dict_rows",
                "group_aggregate",
                dictionary_baselines[name],
                minimum_repeats=15,
                maximum_ratio=0.98,
            )
        )

    class NominalRecord(Mapping[str, int]):
        """Lightweight benchmark wrapper that shares the canonical dict payload."""

        __slots__ = ("_values",)

        def __init__(self, values: dict[str, int]) -> None:
            self._values = values

        def __getitem__(self, name: str) -> int:
            return self._values[name]

        def __iter__(self) -> Iterator[str]:
            return iter(self._values)

        def __len__(self) -> int:
            return len(self._values)

    mapping_sources = (
        (
            "mappingproxy",
            "mappingproxy_rows",
            fpstreams.rows([MappingProxyType(row) for row in dictionary_records]).with_engine(
                "python"
            ),
            1.65,
        ),
        (
            "nominal_mapping",
            "nominal_mapping_rows",
            fpstreams.rows([NominalRecord(row) for row in dictionary_records]).with_engine(
                "python"
            ),
            1.75,
        ),
    )
    for row_kind, source_kind, mapping_source, maximum_ratio in mapping_sources:
        mapping_tasks: tuple[tuple[str, Task], ...] = (
            (
                "callable_key/count_sum_direct",
                mapping_source.group_by(key=lambda row: row["key"])
                .aggregate(count=fpstreams.agg.count(), total=fpstreams.agg.sum("value"))
                .to_list,
            ),
            (
                "callable_value/count_sum",
                mapping_source.group_by(key="key")
                .aggregate(
                    count=fpstreams.agg.count(),
                    total=fpstreams.agg.sum(lambda row: row["value"]),
                )
                .to_list,
            ),
        )
        for name, task in mapping_tasks:
            scenarios.append(
                Scenario(
                    f"fpstreams_group/{row_kind}/{name}/high_cardinality",
                    task,
                    "python",
                    source_kind,
                    "group_aggregate",
                    dictionary_baselines[name],
                    minimum_repeats=15,
                    maximum_ratio=maximum_ratio,
                )
            )
    return scenarios


def _fixed_sparse_group_scenarios(size: int) -> list[Scenario]:
    """Guard the exact-i64 native HashMap path with negative high-cardinality keys."""
    row_count = min(size, 100_000)
    cardinality = max(1, min(row_count, 30_000))
    tuple_records = tuple((-(index % cardinality) - 1, index) for index in range(row_count))
    dict_records = tuple(
        {"key": -(index % cardinality) - 1, "value": index} for index in range(row_count)
    )
    tasks = (
        (
            "tuple",
            fpstreams.rows(tuple_records)
            .group_by(key=0)
            .aggregate(count=fpstreams.agg.count(), total=fpstreams.agg.sum(1))
            .to_list,
        ),
        (
            "dict",
            fpstreams.rows(dict_records)
            .group_by(key="key")
            .aggregate(count=fpstreams.agg.count(), total=fpstreams.agg.sum("value"))
            .to_list,
        ),
    )
    return [
        Scenario(
            f"fpstreams_group/{row_kind}/fixed_i64/count_sum/sparse_high_cardinality",
            task,
            "auto",
            f"{row_kind}_rows",
            "group_aggregate",
            None,
        )
        for row_kind, task in tasks
    ]


def _composite_group_scenarios(size: int) -> list[Scenario]:
    """Guard the fixed two-key count/sum loop against its callable-key fallback."""
    row_count = min(size, 100_000)
    cardinality = max(1, min(row_count, 30_000))
    records = tuple(
        (index % cardinality, (index % cardinality) % 7, index) for index in range(row_count)
    )

    def first(row: tuple[int, int, int]) -> int:
        return row[0]

    def second(row: tuple[int, int, int]) -> int:
        return row[1]

    source = fpstreams.rows(records)
    python_name = "fpstreams_group/tuple/callable_composite/count_sum/high_cardinality/python"
    return [
        Scenario(
            python_name,
            source.with_engine("python")
            .group_by(first, second)
            .aggregate(count=fpstreams.agg.count(), total=fpstreams.agg.sum(2))
            .to_list,
            "python",
            "tuple_rows",
            "group_aggregate",
            None,
        ),
        Scenario(
            "fpstreams_group/tuple/direct_composite/count_sum/high_cardinality/auto",
            source.group_by(0, 1)
            .aggregate(count=fpstreams.agg.count(), total=fpstreams.agg.sum(2))
            .to_list,
            "auto",
            "tuple_rows",
            "group_aggregate",
            python_name,
            maximum_ratio=0.45,
        ),
    ]


def _mapping_field_join_scenarios(size: int) -> list[Scenario]:
    """Guard direct-field native fallback for non-dict Mapping record types."""
    unique_rows = min(size, 100_000)
    many_rows = min(size, 50_000)
    cardinality = max(1, min(8_192, unique_rows, many_rows))

    def records(count: int, key_name: str, value_name: str) -> tuple[Mapping[str, int], ...]:
        return tuple(
            MappingProxyType({key_name: index % cardinality, value_name: index})
            for index in range(count)
        )

    unique_left = records(unique_rows, "left_id", "left_value")
    unique_right = tuple(
        MappingProxyType({"right_id": key, "right_value": key * 3}) for key in range(cardinality)
    )
    many_left = records(many_rows, "left_id", "left_value")
    many_right = tuple(
        MappingProxyType({"right_id": key, "right_value": key * 3 + repeat})
        for key in range(cardinality)
        for repeat in range(2)
    )
    tasks = (
        (
            "unique",
            fpstreams.rows(unique_left)
            .join(
                unique_right,
                left_on="left_id",
                right_on="right_id",
                validate="m:1",
            )
            .to_list,
        ),
        (
            "many",
            fpstreams.rows(many_left)
            .join(
                many_right,
                left_on="left_id",
                right_on="right_id",
                validate="m:m",
            )
            .to_list,
        ),
    )
    return [
        Scenario(
            f"fpstreams_join/mapping/direct_field/{cardinality_mode}",
            task,
            "auto",
            "mapping_rows",
            "join",
            None,
        )
        for cardinality_mode, task in tasks
    ]


def _namedtuple_callable_join_scenarios(size: int) -> list[Scenario]:
    """Guard conservative NamedTuple admission to callable unique and many v2 ABIs."""

    # Python 3.12/3.13 retain PEP 669 event masks after ``free_tool_id()``.  Preserving
    # observable ``_asdict`` frames therefore needs per-row global and local mask checks;
    # Python 3.11 has no monitoring API and 3.14+ clears masks when a tool is freed.
    maximum_ratios = (
        {"unique": 0.70, "many": 0.66}
        if sys.version_info[:2] in {(3, 12), (3, 13)}
        else {"unique": 0.55, "many": 0.55}
    )

    class LeftRow(NamedTuple):
        id: int
        left: int

    class RightRow(NamedTuple):
        id: int
        right: int

    def left_key(row: LeftRow) -> int:
        return row.id

    def right_key(row: RightRow) -> int:
        return row.id

    unique_left_count = min(size, 80_000)
    unique_cardinality = max(1, min(unique_left_count, 40_000))
    unique_left = [LeftRow(index % unique_cardinality, index) for index in range(unique_left_count)]
    unique_right = tuple(
        RightRow(identifier, identifier) for identifier in range(unique_cardinality)
    )

    many_left_count = min(size, 40_000)
    many_cardinality = max(1, min(many_left_count, 20_000))
    many_left = [LeftRow(index % many_cardinality, index) for index in range(many_left_count)]
    many_right = tuple(
        RightRow(identifier, occurrence)
        for identifier in range(many_cardinality)
        for occurrence in range(2)
    )
    tasks = (
        (
            "unique",
            fpstreams.rows(unique_left).join(
                unique_right,
                left_on=left_key,
                right_on=right_key,
                validate="m:1",
            ),
        ),
        (
            "many",
            fpstreams.rows(many_left).join(
                many_right,
                left_on=left_key,
                right_on=right_key,
                validate="m:m",
            ),
        ),
    )
    scenarios: list[Scenario] = []
    for cardinality, joined in tasks:
        python_name = f"fpstreams_join/namedtuple/callable/{cardinality}/python"
        scenarios.extend(
            (
                Scenario(
                    python_name,
                    joined.with_engine("python").to_list,
                    "python",
                    "namedtuple_rows",
                    "join",
                    None,
                    minimum_repeats=15,
                ),
                Scenario(
                    f"fpstreams_join/namedtuple/callable/{cardinality}",
                    joined.to_list,
                    "auto",
                    "namedtuple_rows",
                    "join",
                    python_name,
                    minimum_repeats=15,
                    maximum_ratio=maximum_ratios[cardinality],
                ),
            )
        )
    return scenarios


def _wide_callable_join_scenarios(size: int) -> list[Scenario]:
    """Guard callable v2 schema caches and the measured wide many-merge layout."""
    right_fields = tuple(f"right_{index}" for index in range(7))
    bulk_right_fields = tuple(f"bulk_right_{index}" for index in range(23))

    def left_key(row: Mapping[str, int]) -> int:
        return row["id"]

    def right_key(row: Mapping[str, int]) -> int:
        return row["id"]

    def right_record(
        identifier: int,
        occurrence: int = 0,
        *,
        fields: tuple[str, ...] = right_fields,
    ) -> Mapping[str, int]:
        row = {"id": identifier}
        row.update(
            (field, identifier * (offset + 1) + occurrence) for offset, field in enumerate(fields)
        )
        return MappingProxyType(row)

    unique_count = max(1, min(size, 40_000))
    unique_left = tuple(
        MappingProxyType({"id": index, "left": index}) for index in range(unique_count)
    )
    unique_right = tuple(right_record(index) for index in range(unique_count))

    many_left_count = max(1, min(size, 20_000))
    many_left = tuple(
        MappingProxyType({"id": index, "left": index}) for index in range(many_left_count)
    )
    many_right = tuple(
        right_record(index, occurrence)
        for index in range(many_left_count)
        for occurrence in range(2)
    )
    bulk_many_right = tuple(
        right_record(index, occurrence, fields=bulk_right_fields)
        for index in range(many_left_count)
        for occurrence in range(2)
    )
    tasks = (
        (
            "unique",
            fpstreams.rows(unique_left)
            .join(
                unique_right,
                left_on=left_key,
                right_on=right_key,
                validate="m:1",
            )
            .to_list,
        ),
        (
            "many",
            fpstreams.rows(many_left)
            .join(
                many_right,
                left_on=left_key,
                right_on=right_key,
                validate="m:m",
            )
            .to_list,
        ),
        (
            "many_bulk_merge",
            fpstreams.rows(many_left)
            .join(
                bulk_many_right,
                left_on=left_key,
                right_on=right_key,
                validate="m:m",
            )
            .to_list,
        ),
    )
    return [
        Scenario(
            f"fpstreams_join/mapping/callable/wide_schema/{cardinality}",
            task,
            "auto",
            "dict_rows",
            "join",
            None,
        )
        for cardinality, task in tasks
    ]


def _value_layout_callable_join_scenarios(size: int) -> list[Scenario]:
    """Guard equal exact-string schemas whose field objects are fresh on every right row."""
    logical_fields = ("id", *(f"right_{index}" for index in range(7)))

    def left_key(row: Mapping[str, int]) -> int:
        return row["id"]

    def right_key(row: Mapping[str, int]) -> int:
        return row["id"]

    def right_record(identifier: int, occurrence: int = 0) -> Mapping[str, int]:
        row: dict[str, int] = {}
        for offset, logical_name in enumerate(logical_fields):
            # Encoding round-trips deliberately mint a non-interned exact string per row.
            name = logical_name.encode("utf-8").decode("utf-8")
            row[name] = identifier if offset == 0 else identifier * offset + occurrence
        return MappingProxyType(row)

    unique_count = max(1, min(size, 40_000))
    unique_left = tuple(
        MappingProxyType({"id": index, "left": index}) for index in range(unique_count)
    )
    unique_right = tuple(right_record(index) for index in range(unique_count))

    many_left_count = max(1, min(size, 10_000))
    many_left = tuple(
        MappingProxyType({"id": index, "left": index}) for index in range(many_left_count)
    )
    many_right = tuple(
        right_record(index, occurrence)
        for index in range(many_left_count)
        for occurrence in range(2)
    )

    for records in (unique_right, many_right):
        if len(records) > 1:
            first_names = tuple(records[0])
            second_names = tuple(records[1])
            assert all(
                first == second and first is not second
                for first, second in zip(first_names, second_names, strict=True)
            )

    tasks = (
        (
            "unique",
            fpstreams.rows(unique_left)
            .join(
                unique_right,
                left_on=left_key,
                right_on=right_key,
                validate="m:1",
            )
            .to_list,
        ),
        (
            "many",
            fpstreams.rows(many_left)
            .join(
                many_right,
                left_on=left_key,
                right_on=right_key,
                validate="m:m",
            )
            .to_list,
        ),
    )
    return [
        Scenario(
            f"fpstreams_join/mapping/callable/value_schema/{cardinality}",
            task,
            "auto",
            "mapping_rows",
            "join",
            None,
        )
        for cardinality, task in tasks
    ]


def _exact_dict_sort_scenarios(size: int) -> list[Scenario]:
    """Compare one direct exact-dict field with its canonical callable selector."""
    row_count = min(size, 300_000)
    records = [
        {"value": (index * 7919) % row_count, "position": index} for index in range(row_count)
    ]
    canonical_name = "fpstreams_sort/exact_dict/callable_field/list"
    canonical = fpstreams.rows(records).with_engine("python").sort_by(lambda row: row["value"])
    direct = fpstreams.rows(records).sort_by("value")
    return [
        Scenario(
            canonical_name,
            canonical.to_list,
            "python",
            "dict_rows",
            "sort",
            None,
        ),
        Scenario(
            "fpstreams_sort/exact_dict/direct_field/list",
            direct.to_list,
            "auto",
            "dict_rows",
            "sort",
            canonical_name,
        ),
    ]


def _arrow_identity_list_scenarios(size: int) -> list[Scenario]:
    """Track batch-wise identity list materialization against the canonical row route."""
    try:
        import pyarrow as pa
    except ImportError:
        return []

    row_count = min(size, 300_000)
    table = pa.table(
        {
            "id": pa.array(range(row_count), type=pa.int64()),
            "group": pa.array((index % 64 for index in range(row_count)), type=pa.int64()),
            "value": pa.array((index * 3 for index in range(row_count)), type=pa.int64()),
        }
    )
    source = fpstreams.rows.from_arrow(table)
    python_name = "fpstreams_arrow/table/identity/list/python"
    return [
        Scenario(
            python_name,
            source.with_engine("python").to_list,
            "python",
            "arrow_table",
            "list",
            None,
        ),
        Scenario(
            "fpstreams_arrow/table/identity/list/auto",
            source.to_list,
            "auto",
            "arrow_table",
            "list",
            python_name,
        ),
    ]


def _arrow_stable_sort_scenarios(size: int) -> list[Scenario]:
    """Compare canonical row sorting with a direct retained-Arrow field sort."""
    try:
        import pyarrow as pa
    except ImportError:
        return []

    row_count = min(size, 300_000)
    cardinality = max(1, row_count // 4)
    table = pa.table(
        {
            "key": pa.array(
                ((index * 7919) % cardinality for index in range(row_count)),
                type=pa.int64(),
            ),
            "position": pa.array(range(row_count), type=pa.int64()),
            "value": pa.array((index * 3 for index in range(row_count)), type=pa.int64()),
        }
    )
    source = fpstreams.rows.from_arrow(table)
    python_name = "fpstreams_arrow/table/stable_sort/python"
    return [
        Scenario(
            python_name,
            source.with_engine("python").sort_by("key").to_list,
            "python",
            "arrow_table",
            "sort",
            None,
        ),
        Scenario(
            "fpstreams_arrow/table/stable_sort/auto",
            source.sort_by("key").to_list,
            "auto",
            "arrow_table",
            "sort",
            python_name,
        ),
    ]


def _arrow_unique_join_scenarios(size: int) -> list[Scenario]:
    """Compare complete public m:1 output for direct and suffix-key Arrow layouts."""
    try:
        import pyarrow as pa
    except ImportError:
        return []

    left_count = min(size, 300_000)
    right_count = max(1, min(100_000, (left_count * 2) // 3))
    key_domain = min(left_count, right_count * 2)
    left_keys = pa.array(
        (index % key_domain for index in range(left_count)),
        type=pa.int64(),
    )
    right_keys = pa.array(range(right_count), type=pa.int64())
    left_values = pa.array(range(left_count), type=pa.int64())
    right_values = pa.array((index * 3 for index in range(right_count)), type=pa.int64())
    layouts = (
        (
            "no_suffix",
            pa.table({"left_id": left_keys, "left_value": left_values}),
            pa.table({"right_id": right_keys, "right_value": right_values}),
            {"left_on": "left_id", "right_on": "right_id"},
        ),
        (
            "suffix",
            pa.table({"id": left_keys, "value": left_values}),
            pa.table({"id": right_keys, "value": right_values}),
            {"on": "id"},
        ),
    )

    scenarios: list[Scenario] = []
    for layout, left_table, right_table, join_keys in layouts:
        for how in ("inner", "left"):
            left = fpstreams.rows.from_arrow(left_table)
            right = fpstreams.rows.from_arrow(right_table)
            canonical = left.with_engine("python").join(
                right.with_engine("python"),
                how=how,
                validate="m:1",
                **join_keys,
            )
            automatic = left.join(
                right,
                how=how,
                validate="m:1",
                **join_keys,
            )
            python_name = f"fpstreams_arrow/table/unique_join/{layout}/{how}/python"
            scenarios.extend(
                (
                    Scenario(
                        python_name,
                        canonical.to_list,
                        "python",
                        "arrow_table",
                        "join",
                        None,
                    ),
                    Scenario(
                        f"fpstreams_arrow/table/unique_join/{layout}/{how}/auto",
                        automatic.to_list,
                        "auto",
                        "arrow_table",
                        "join",
                        python_name,
                    ),
                )
            )
    return scenarios


def _arrow_c_stream_scenarios(size: int) -> list[Scenario]:
    """Compare eager table export with lazy C Stream first-batch and full-scan work."""
    try:
        import pyarrow as pa
    except ImportError:
        return []

    row_count = min(size, 300_000)
    records = [{"id": index, "group": index % 64, "value": index * 3} for index in range(row_count)]
    source = fpstreams.rows(records)

    def eager_full_scan() -> int:
        table = source.to_arrow()
        return sum(batch.num_rows for batch in table.to_batches())

    def eager_first_batch() -> int:
        batches = source.to_arrow().to_batches(max_chunksize=65_536)
        return 0 if not batches else batches[0].num_rows

    def lazy_full_scan() -> int:
        reader = pa.RecordBatchReader.from_stream(source)
        try:
            return sum(batch.num_rows for batch in reader)
        finally:
            reader.close()

    def lazy_first_batch() -> int:
        reader = pa.RecordBatchReader.from_stream(source)
        try:
            try:
                return reader.read_next_batch().num_rows
            except StopIteration:
                return 0
        finally:
            reader.close()

    eager_name = "fpstreams_arrow/rows/c_stream/eager_table"
    return [
        Scenario(
            eager_name,
            eager_full_scan,
            "auto",
            "dict_rows",
            "arrow_stream",
            None,
            eager_first_batch,
        ),
        Scenario(
            "fpstreams_arrow/rows/c_stream/lazy",
            lazy_full_scan,
            "auto",
            "dict_rows",
            "arrow_stream",
            eager_name,
            lazy_first_batch,
        ),
    ]


def _arrow_reader_group_scenarios(size: int) -> list[Scenario]:
    """Compare canonical row grouping with incremental one-shot Arrow batches."""
    try:
        import pyarrow as pa
    except ImportError:
        return []

    row_count = min(size, 300_000)
    table = pa.table(
        {
            "key": pa.array((index % 64 for index in range(row_count)), type=pa.int64()),
            "value": pa.array((index * 3 for index in range(row_count)), type=pa.int64()),
        }
    )
    batches = tuple(table.to_batches(max_chunksize=65_536))

    def group_sum(backend: Literal["python", "auto"]) -> list[dict[str, object]]:
        reader = pa.RecordBatchReader.from_batches(table.schema, batches)
        source = fpstreams.rows.from_arrow(reader)
        selected = source.with_engine("python") if backend == "python" else source
        return selected.group_by("key").aggregate(total=fpstreams.agg.sum("value")).to_list()

    python_name = "fpstreams_arrow/reader/group_sum/python"
    return [
        Scenario(
            python_name,
            partial(group_sum, "python"),
            "python",
            "arrow_reader",
            "group_sum",
            None,
        ),
        Scenario(
            "fpstreams_arrow/reader/group_sum/auto",
            partial(group_sum, "auto"),
            "auto",
            "arrow_reader",
            "group_sum",
            python_name,
        ),
    ]


def _arrow_file_group_scenarios(size: int) -> list[Scenario]:
    """Compare canonical row grouping with reusable streaming CSV and Parquet scans."""
    try:
        import pyarrow as pa
        import pyarrow.csv as pa_csv
        import pyarrow.parquet as pq
    except ImportError:
        return []

    row_count = min(size, 300_000)
    cardinality = max(1, min(row_count, 4_096))
    table = pa.table(
        {
            "key": pa.array((index % cardinality for index in range(row_count)), type=pa.int64()),
            "value": pa.array((index * 3 for index in range(row_count)), type=pa.int64()),
            "unused": pa.array(
                (f"payload-{index % 128:03d}" for index in range(row_count)),
                type=pa.string(),
            ),
        }
    )
    workspace = TemporaryDirectory(prefix="fpstreams-arrow-file-group-")
    csv_path = Path(workspace.name) / "groups.csv"
    parquet_path = Path(workspace.name) / "groups.parquet"
    pa_csv.write_csv(table, csv_path)
    pq.write_table(table, parquet_path, row_group_size=65_536)

    def group_sum(
        storage: Literal["csv", "parquet"], backend: Literal["python", "auto"]
    ) -> list[dict[str, object]]:
        # Resolve through the owner on every sample so its temporary directory remains alive
        # after this scenario builder returns.
        root = Path(workspace.name)
        source = (
            fpstreams.rows.scan_csv(root / "groups.csv", batch_size=65_536)
            if storage == "csv"
            else fpstreams.rows.from_parquet(root / "groups.parquet", batch_size=65_536)
        )
        selected = source.with_engine("python") if backend == "python" else source
        return selected.group_by("key").aggregate(total=fpstreams.agg.sum("value")).to_list()

    scenarios: list[Scenario] = []
    for storage, source_kind in (("csv", "arrow_csv"), ("parquet", "arrow_parquet")):
        python_name = f"fpstreams_arrow/{storage}/group_sum/python"
        scenarios.extend(
            (
                Scenario(
                    python_name,
                    partial(group_sum, storage, "python"),
                    "python",
                    source_kind,
                    "group_sum",
                    None,
                ),
                Scenario(
                    f"fpstreams_arrow/{storage}/group_sum/auto",
                    partial(group_sum, storage, "auto"),
                    "auto",
                    source_kind,
                    "group_sum",
                    python_name,
                    maximum_ratio=0.60 if row_count == 300_000 else None,
                ),
            )
        )
    return scenarios


def _arrow_dictionary_group_scenarios(size: int) -> list[Scenario]:
    """Guard dictionary unification and columnar grouping when Arrow is installed."""
    try:
        import pyarrow as pa
    except ImportError:
        return []

    row_count = min(size, 300_000)
    cardinality = max(1, min(row_count, 64))
    names = [f"g{index:02d}" for index in range(cardinality)]
    split = row_count // 2

    def dictionary_chunk(start: int, count: int, *, reversed_values: bool) -> Any:
        dictionary = list(reversed(names)) if reversed_values else names
        indices: list[int | None] = []
        for absolute in range(start, start + count):
            if absolute % 257 == 0:
                indices.append(None)
                continue
            logical = absolute % cardinality
            indices.append(cardinality - logical - 1 if reversed_values else logical)
        return pa.DictionaryArray.from_arrays(
            pa.array(indices, type=pa.int8()),
            pa.array(dictionary),
        )

    keys = pa.chunked_array(
        [
            dictionary_chunk(0, split, reversed_values=False),
            dictionary_chunk(split, row_count - split, reversed_values=True),
        ]
    )
    table = pa.table({"key": keys, "value": pa.array([1] * row_count, type=pa.int64())})
    task = (
        fpstreams.rows.from_arrow(table)
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
        .to_list
    )
    return [
        Scenario(
            "fpstreams_arrow/dictionary/group_sum",
            task,
            "auto",
            "arrow_dictionary",
            "group_sum",
            None,
        )
    ]


def _one_shot_task(size: int, backend: Backend) -> Task:
    def execute() -> int:
        pipeline = flow(iter(range(size))).map(lambda value: value + 1)
        selected = pipeline.with_engine(backend) if backend == "python" else pipeline
        return selected.sum()

    return execute


def _one_shot_scenarios(size: int) -> list[Scenario]:
    builtin_name = "python_builtin/one-shot/map/sum"
    python_name = "fpstreams_python/one-shot/map/sum"
    return [
        Scenario(
            builtin_name,
            lambda: sum(value + 1 for value in iter(range(size))),
            "python-builtin",
            "one-shot",
            "sum",
            None,
        ),
        Scenario(
            python_name,
            _one_shot_task(size, "python"),
            "python",
            "one-shot",
            "sum",
            builtin_name,
        ),
        Scenario(
            "fpstreams_auto/one-shot/map/sum",
            _one_shot_task(size, "auto"),
            "auto",
            "one-shot",
            "sum",
            python_name,
        ),
    ]


def native_build_metadata() -> dict[str, object]:
    try:
        from fpstreams import _native
    except ImportError:
        return {"available": False, "profile": "unavailable", "path": None}
    profile = _native.build_profile() if hasattr(_native, "build_profile") else "unknown"
    return {
        "available": True,
        "profile": profile,
        "path": str(getattr(_native, "__file__", "")) or None,
    }


def find_regressions(
    records: Sequence[Mapping[str, Any]],
    *,
    maximum_ratio: float = 1.10,
) -> list[dict[str, Any]]:
    by_name = {str(record["name"]): record for record in records}
    regressions: list[dict[str, Any]] = []
    for record in records:
        name = str(record.get("name", ""))
        identity_guard = (
            record.get("backend") == "auto"
            and record.get("source_kind") in {"list", "tuple"}
            and "/identity/" in name
            and record.get("terminal") == "sum"
        )
        expression_guard = name.startswith("fpstreams_python/") and (
            "/map_filter/" in name or "/float_map_filter/" in name
        )
        explicit_limit = record.get("maximum_ratio")
        explicit_guard = type(explicit_limit) is float and explicit_limit > 0
        if not identity_guard and not expression_guard and not explicit_guard:
            continue
        baseline_name = record.get("baseline")
        baseline = by_name.get(str(baseline_name))
        if baseline is None:
            continue
        baseline_seconds = float(baseline["median_seconds"])
        ratio = float(record["median_seconds"]) / baseline_seconds if baseline_seconds else 1.0
        allowed = explicit_limit if explicit_guard else 2.0 if expression_guard else maximum_ratio
        if ratio > allowed:
            regressions.append(
                {
                    "name": record["name"],
                    "baseline": baseline_name,
                    "ratio": ratio,
                    "maximum_ratio": allowed,
                }
            )
    return regressions


def run(
    *,
    size: int,
    repeats: int,
    domain: str = "int",
    quick: bool = False,
    fail_on_regression: bool = False,
    include: Sequence[str] = (),
) -> dict[str, Any]:
    if size < 1:
        raise ValueError("size must be positive")
    if repeats < 1:
        raise ValueError("repeats must be positive")
    if domain not in {"int", "float", "both"}:
        raise ValueError("domain must be 'int', 'float', or 'both'")
    if any(not pattern for pattern in include):
        raise ValueError("benchmark include patterns cannot be empty")

    native = native_build_metadata()
    if fail_on_regression and native["profile"] != "release":
        raise RuntimeError(
            "--fail-on-regression requires a confirmed release native build; "
            f"detected {native['profile']!r}"
        )

    scenarios: list[Scenario] = []
    if domain in {"int", "both"}:
        scenarios.extend(_identity_scenarios(size, include_tuple=not quick))
        scenarios.append(_small_plan_scenario())
        scenarios.extend(_logical_compile_scenarios())
        scenarios.extend(_sync_operation_scenarios())
        scenarios.extend(_async_operation_scenarios())
        scenarios.extend(_rows_operation_scenarios())
        scenarios.extend(_callable_group_scenarios(size))
        scenarios.extend(_fixed_sparse_group_scenarios(size))
        scenarios.extend(_composite_group_scenarios(size))
        scenarios.extend(_mapping_field_join_scenarios(size))
        scenarios.extend(_namedtuple_callable_join_scenarios(size))
        scenarios.extend(_wide_callable_join_scenarios(size))
        scenarios.extend(_value_layout_callable_join_scenarios(size))
        scenarios.extend(_exact_dict_sort_scenarios(size))
        scenarios.extend(_arrow_identity_list_scenarios(size))
        scenarios.extend(_arrow_stable_sort_scenarios(size))
        scenarios.extend(_arrow_unique_join_scenarios(size))
        scenarios.extend(_arrow_c_stream_scenarios(size))
        scenarios.extend(_arrow_reader_group_scenarios(size))
        scenarios.extend(_arrow_file_group_scenarios(size))
        scenarios.extend(_arrow_dictionary_group_scenarios(size))
        scenarios.extend(_one_shot_scenarios(size))
        if not quick:
            scenarios.extend(_integer_pipeline_scenarios(size, bool(native["available"])))
    if domain in {"float", "both"}:
        scenarios.extend(_float_scenarios(size, bool(native["available"])))
    if include:
        scenarios = [
            scenario
            for scenario in scenarios
            if any(fnmatch.fnmatch(scenario.name, pattern) for pattern in include)
        ]
        if not scenarios:
            raise ValueError("benchmark include patterns selected no scenarios")

    results = [_record(scenario, repeats) for scenario in scenarios]
    return {
        "schema_version": 1,
        "metadata": {
            "fpstreams_version": fpstreams.__version__,
            "python_version": platform.python_version(),
            "implementation": platform.python_implementation(),
            "platform": platform.platform(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "native": native,
            "size": size,
            "repeats": repeats,
            "domain": domain,
            "quick": quick,
        },
        "results": results,
        "regressions": find_regressions(results),
    }


def run_competitive(*, size: int, repeats: int) -> dict[str, Any]:
    """Compare equivalent NumPy/Pandas workloads and reject any semantic mismatch.

    These compute-only comparisons deliberately stay outside the immutable release
    scenario set. They report observed ratios instead of imposing a misleading gate:
    an already-materialized ndarray or DataFrame is the specialist libraries' best case.
    """
    if size < 1:
        raise ValueError("size must be positive")
    if repeats < 1:
        raise ValueError("repeats must be positive")

    pairs = _competitive_pairs(size)
    results: list[dict[str, Any]] = []
    comparisons: list[dict[str, Any]] = []
    for pair in pairs:
        expected = pair.normalize_reference(pair.reference.task())
        actual = pair.normalize_candidate(pair.candidate.task())
        if actual != expected:
            raise RuntimeError(
                f"competitive result mismatch: {pair.candidate.name} != {pair.reference.name}"
            )

        reference = _record(pair.reference, repeats)
        candidate = _record(pair.candidate, repeats)
        results.extend((reference, candidate))
        reference_seconds = float(reference["median_seconds"])
        comparisons.append(
            {
                "candidate": pair.candidate.name,
                "baseline": pair.reference.name,
                "outputs_equal": True,
                "ratio": (
                    float(candidate["median_seconds"]) / reference_seconds
                    if reference_seconds
                    else 1.0
                ),
            }
        )

    return {
        "schema_version": 1,
        "metadata": {
            "suite": "competitive",
            "fpstreams_version": fpstreams.__version__,
            "python_version": platform.python_version(),
            "implementation": platform.python_implementation(),
            "platform": platform.platform(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "native": native_build_metadata(),
            "size": size,
            "repeats": repeats,
            "scope": "compute-only on reusable, pre-materialized inputs",
        },
        "results": results,
        "comparisons": comparisons,
        "regressions": [],
    }


def render(report: Mapping[str, Any]) -> None:
    metadata = report["metadata"]
    print(
        "fpstreams benchmark · "
        f"Python {metadata['python_version']} · {metadata['platform']} · "
        f"native {metadata['native']['profile']}"
    )
    for result in report["results"]:
        print(
            f"{result['name']:<52} {result['median_seconds']:>10.6f}s "
            f"± {result['stdev_seconds']:.6f}s"
        )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", type=int, default=1_000_000)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--domain", choices=("int", "float", "both"), default="int")
    parser.add_argument("--json", type=Path)
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--fail-on-regression", action="store_true")
    parser.add_argument("--include", action="append", default=[])
    parser.add_argument("--list-scenarios", action="store_true")
    parser.add_argument("--competitive", action="store_true")
    arguments = parser.parse_args()

    try:
        if arguments.competitive:
            if (
                arguments.domain != "int"
                or arguments.quick
                or arguments.fail_on_regression
                or arguments.include
            ):
                raise ValueError(
                    "--competitive cannot be combined with --domain, --quick, "
                    "--fail-on-regression, or --include"
                )
            report = run_competitive(
                size=1 if arguments.list_scenarios else arguments.size,
                repeats=1 if arguments.list_scenarios else arguments.repeats,
            )
        else:
            report = run(
                size=1 if arguments.list_scenarios else arguments.size,
                repeats=1 if arguments.list_scenarios else arguments.repeats,
                domain=arguments.domain,
                quick=arguments.quick,
                fail_on_regression=(
                    False if arguments.list_scenarios else arguments.fail_on_regression
                ),
                include=arguments.include,
            )
    except (RuntimeError, ValueError) as error:
        parser.error(str(error))
    if arguments.list_scenarios:
        print("\n".join(result["name"] for result in report["results"]))
        return 0
    render(report)
    if arguments.json is not None:
        arguments.json.parent.mkdir(parents=True, exist_ok=True)
        arguments.json.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    if arguments.fail_on_regression and report["regressions"]:
        for regression in report["regressions"]:
            print(
                f"regression: {regression['name']} is {regression['ratio']:.3f}x its "
                f"same-run Python baseline (limit {regression['maximum_ratio']:.2f}x)",
                file=sys.stderr,
            )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
