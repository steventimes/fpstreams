"""Sanity/smoke tests: verify the package boots and core paths run."""

from __future__ import annotations

import asyncio

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
