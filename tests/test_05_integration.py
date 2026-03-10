"""Integration tests: end-to-end behavior across multiple abstractions."""

from __future__ import annotations

import asyncio

from fpstreams import Collectors, Result, Stream


def test_integration_etl_style_pipeline_with_collectors() -> None:
    rows = [
        {"dept": "eng", "salary": 100},
        {"dept": "eng", "salary": 150},
        {"dept": "sales", "salary": 80},
    ]

    report = (
        Stream(rows)
        .collect(
            Collectors.grouping_by(
                lambda row: row["dept"],
                Collectors.mapping(lambda row: row["salary"], Collectors.averaging(lambda x: float(x))),
            )
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
            .gather()
            .to_list()
        )

    assert asyncio.run(scenario()) == [20, 30, 40]
