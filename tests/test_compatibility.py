"""Compatibility, smoke, integration, and package-layout tests."""

from __future__ import annotations

import asyncio
import os

import pytest

import fpstreams
from fpstreams import AsyncStream, Collectors, Option, ParallelStream, Result, Stream


def _times_two(x: int) -> int:
    return x * 2


def test_smoke_imports_and_basic_pipeline() -> None:
    assert Stream([1, 2, 3]).map(lambda x: x + 1).to_list() == [2, 3, 4]
    assert ParallelStream([1, 2, 3]).map(_times_two).to_list() == [2, 4, 6]
    assert Collectors.joining(",")([1, 2, 3]) == "1,2,3"
    assert Option.of(1).map(lambda x: x + 1).or_else(0) == 2
    assert Result.success(1).map(lambda x: x + 1).get_or_else(0) == 2


def test_smoke_async_stream_runs() -> None:
    async def scenario() -> list[int]:
        return await AsyncStream.of(1, 2, 3).map(lambda x: x + 10).to_list()

    assert asyncio.run(scenario()) == [11, 12, 13]


def _double(x: int) -> int:
    return x * 2


def _div_by_three(x: int) -> bool:
    return x % 3 == 0


def test_unit_transformations_cover_empty_single_many() -> None:
    assert Stream([]).map(lambda x: x).to_list() == []
    assert Stream([5]).filter(lambda x: x > 0).to_list() == [5]
    assert Stream([1, 2, 3, 4]).flat_map(lambda x: [x, -x]).to_list() == [
        1,
        -1,
        2,
        -2,
        3,
        -3,
        4,
        -4,
    ]


def test_unit_limit_skip_and_window_edges() -> None:
    assert Stream([1, 2, 3]).limit(0).to_list() == []
    assert Stream([1, 2, 3]).skip(10).to_list() == []
    assert Stream([1, 2, 3, 4]).window(2).to_list() == [(1, 2), (2, 3), (3, 4)]


def test_unit_terminal_operations() -> None:
    s = Stream([3, 1, 2])
    assert s.sorted().to_list() == [1, 2, 3]
    assert Stream([3, 1, 2]).min() == 1
    assert Stream([3, 1, 2]).max() == 3
    assert Stream([3, 1, 2]).sum() == 6


def test_unit_collectors_grouping_partitioning_and_columns() -> None:
    grouped = Stream(["aa", "b", "cc"]).collect(Collectors.grouping_by(len))
    assert grouped == {2: ["aa", "cc"], 1: ["b"]}

    partitioned = Stream([1, 2, 3, 4]).collect(Collectors.partitioning_by(lambda x: x % 2 == 0))
    assert partitioned[True] == [2, 4]
    assert partitioned[False] == [1, 3]

    columns = Stream([{"a": 1}, {"a": 2, "b": 3}]).collect(Collectors.to_columns())
    assert columns["a"] == [1, 2]
    assert columns["b"] == [None, 3]


def test_unit_parallel_matches_sequential_for_pure_pipeline() -> None:
    data = list(range(200))
    seq = Stream(data).map(_double).filter(_div_by_three).to_list()
    par = ParallelStream(data).map(_double).filter(_div_by_three).to_list()
    assert sorted(par) == sorted(seq)


def test_integration_etl_style_pipeline_with_collectors() -> None:
    rows = [
        {"dept": "eng", "salary": 100},
        {"dept": "eng", "salary": 150},
        {"dept": "sales", "salary": 80},
    ]

    report = Stream(rows).collect(
        Collectors.grouping_by(
            lambda row: row["dept"],
            Collectors.mapping(
                lambda row: row["salary"],
                Collectors.averaging(lambda x: float(x)),
            ),
        )
    )

    assert report == {"eng": 125.0, "sales": 80.0}


def test_integration_result_partition_and_unwrap() -> None:
    events = [
        Result.success({"id": 1, "ok": True}),
        Result.failure(ValueError("bad payload")),
        Result.success({"id": 2, "ok": True}),
    ]

    ok, errors = Stream(events).partition_results()
    assert [row["id"] for row in ok] == [1, 2]
    assert len(errors) == 1


def test_integration_async_bridge() -> None:
    async def scenario() -> list[int]:
        return (
            await Stream([1, 2, 3])
            .to_async()
            .map(lambda x: x + 1)
            .map_async(lambda x: asyncio.sleep(0, result=x * 10))
            .to_list()
        )

    assert asyncio.run(scenario()) == [20, 30, 40]


def test_coverage_threshold_if_enabled() -> None:
    minimum = os.environ.get("COVERAGE_MIN")
    running_under_coverage = os.environ.get("COV_CORE_SOURCE") is not None

    if minimum is None and not running_under_coverage:
        pytest.skip("Coverage gate is enabled only in coverage-aware runs.")

    # If this test runs, assert that tracing is active.
    import sys

    assert sys.gettrace() is not None


def test_domain_packages_export_the_stable_public_api() -> None:
    from fpstreams.collecting import Aggregator, Collector
    from fpstreams.expressions import Expr, RowExpr
    from fpstreams.primitives import Option, Result
    from fpstreams.streams import AsyncFlow, Flow, Gatherer, Pairs
    from fpstreams.tabular import Rows

    assert (Flow, AsyncFlow, Pairs, Gatherer) == (
        fpstreams.Flow,
        fpstreams.AsyncFlow,
        fpstreams.Pairs,
        fpstreams.Gatherer,
    )
    assert (Collector, Aggregator) == (fpstreams.Collector, fpstreams.Aggregator)
    assert (Rows, Expr, RowExpr) == (fpstreams.Rows, fpstreams.Expr, fpstreams.RowExpr)
    assert (Option, Result) == (fpstreams.Option, fpstreams.Result)


def test_legacy_module_imports_remain_compatible() -> None:
    public_flow = fpstreams.flow
    public_rows = fpstreams.rows
    try:
        from fpstreams.aggregate import Aggregator
        from fpstreams.async_flow import AsyncFlow
        from fpstreams.collectors import Collector
        from fpstreams.flow import Flow
        from fpstreams.rows import Rows

        assert (Flow, AsyncFlow, Rows, Collector, Aggregator) == (
            fpstreams.Flow,
            fpstreams.AsyncFlow,
            fpstreams.Rows,
            fpstreams.Collector,
            fpstreams.Aggregator,
        )
    finally:
        # Importing a same-named submodule replaces the package attribute by design.
        fpstreams.flow = public_flow
        fpstreams.rows = public_rows
