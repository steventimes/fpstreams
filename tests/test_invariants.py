"""Fuzz, property-based, and finite-state invariant tests."""

from __future__ import annotations

import importlib.util
import itertools
import random
from typing import cast, get_args

import pytest

from fpstreams import Stream, aflow, flow, item
from fpstreams.planning.sync import FilterOp, MapOp, Operation, TapOp


def test_fuzz_random_numeric_pipelines_are_crash_free_and_match_oracle() -> None:
    rng = random.Random(20260101)

    for _ in range(400):
        size = rng.randint(0, 200)
        data = [rng.randint(-10_000, 10_000) for _ in range(size)]

        skip_n = rng.randint(0, 10)
        limit_n = rng.randint(0, 100)

        result = (
            Stream(data)
            .map(lambda x: x * 3 - 1)
            .filter(lambda x: x % 7 != 0)
            .skip(skip_n)
            .limit(limit_n)
            .to_list()
        )

        expected = [x * 3 - 1 for x in data]
        expected = [x for x in expected if x % 7 != 0]
        expected = expected[skip_n:][:limit_n]

        assert result == expected
        assert all((x + 1) % 3 == 0 for x in result)


def test_property_fused_map_filter_matches_python_oracle_and_native() -> None:
    rng = random.Random(20260814)
    for _ in range(100):
        values = [rng.randint(-10_000, 10_000) for _ in range(rng.randint(0, 200))]
        pipeline = flow(values).map(item * 3 - 1).filter(item % 7 != 0)
        expected = [value * 3 - 1 for value in values if (value * 3 - 1) % 7 != 0]

        assert pipeline.with_engine("python").to_list() == expected
        assert pipeline.with_engine("native").to_list() == expected


@pytest.mark.asyncio
async def test_property_common_sync_and_async_transforms_have_parity() -> None:
    rng = random.Random(20260815)
    for _ in range(50):
        values = [rng.randint(-100, 100) for _ in range(rng.randint(0, 100))]
        drop = rng.randint(0, 10)
        take = rng.randint(0, 30)
        expected = (
            flow(values)
            .map(lambda value: value * 2)
            .filter(lambda value: value % 3 != 0)
            .drop(drop)
            .take(take)
            .to_list()
        )
        actual = await (
            aflow(values)
            .map(lambda value: value * 2)
            .filter(lambda value: value % 3 != 0)
            .drop(drop)
            .take(take)
            .to_list()
        )

        assert actual == expected


def test_fuzz_string_joining_never_crashes() -> None:
    rng = random.Random(7)
    for _ in range(300):
        words = [
            "".join(chr(rng.randint(97, 122)) for _ in range(rng.randint(0, 8)))
            for _ in range(rng.randint(0, 60))
        ]
        out = Stream(words).join("|")
        assert isinstance(out, str)


def test_property_distinct_preserves_first_occurrence_order() -> None:
    rng = random.Random(101)
    for _ in range(300):
        values = [rng.randint(-20, 20) for _ in range(rng.randint(0, 120))]
        out = Stream(values).distinct().to_list()
        expected: list[int] = []
        seen: set[int] = set()
        for v in values:
            if v not in seen:
                seen.add(v)
                expected.append(v)
        assert out == expected


def test_property_batch_flattens_back_to_original() -> None:
    rng = random.Random(102)
    for _ in range(250):
        values = [rng.randint(-1_000, 1_000) for _ in range(rng.randint(0, 300))]
        size = rng.randint(1, 25)
        batches = Stream(values).batch(size).to_list()
        flattened = [x for b in batches for x in b]
        assert flattened == values


def test_property_limit_prefix_law() -> None:
    rng = random.Random(103)
    for _ in range(250):
        values = [rng.randint(-50, 50) for _ in range(rng.randint(0, 200))]
        n = rng.randint(0, 100)
        out = Stream(values).limit(n).to_list()
        assert len(out) <= n
        assert out == values[:n]


def _spec_pipeline(
    values: list[int], do_map: bool, do_filter: bool, skip_n: int, limit_n: int
) -> list[int]:
    out = list(values)
    if do_map:
        out = [x + 1 for x in out]
    if do_filter:
        out = [x for x in out if x % 2 == 0]
    out = out[skip_n:]
    out = out[:limit_n]
    return out


def test_model_check_small_pipeline_state_space() -> None:
    universe = [0, 1, 2]
    for length in range(0, 5):
        for tup in itertools.product(universe, repeat=length):
            values = list(tup)
            for do_map, do_filter in itertools.product([False, True], repeat=2):
                for skip_n in range(0, 3):
                    for limit_n in range(0, 4):
                        stream = Stream(values)
                        if do_map:
                            stream = stream.map(lambda x: x + 1)
                        if do_filter:
                            stream = stream.filter(lambda x: x % 2 == 0)
                        out = stream.skip(skip_n).limit(limit_n).to_list()
                        assert out == _spec_pipeline(values, do_map, do_filter, skip_n, limit_n)


def test_sync_operation_dispatch_covers_every_planned_operation() -> None:
    spec = importlib.util.find_spec("fpstreams.execution.sync_ops")
    assert spec is not None, "the focused synchronous dispatch module must exist"

    from fpstreams.execution.sync_ops import OPERATION_HANDLERS, SUPPORTED_OPERATION_TYPES

    assert len(SUPPORTED_OPERATION_TYPES) == len(set(SUPPORTED_OPERATION_TYPES))
    assert tuple(OPERATION_HANDLERS) == SUPPORTED_OPERATION_TYPES
    assert set(SUPPORTED_OPERATION_TYPES) == set(get_args(Operation))


def test_sync_operation_dispatch_applies_map_operation() -> None:
    import fpstreams.execution.sync_ops as sync_ops

    assert hasattr(sync_ops, "apply_operation"), "the dispatcher must be callable directly"
    iterator = sync_ops.apply_operation(iter([1, 2]), MapOp(lambda value: value + 1))

    assert list(iterator) == [2, 3]


def test_sync_operation_dispatch_rejects_unknown_types() -> None:
    import fpstreams.execution.sync_ops as sync_ops

    with pytest.raises(TypeError, match="unsupported synchronous operation: object"):
        sync_ops.apply_operation(iter(()), cast(Operation, object()))


def test_sync_stateless_dispatch_preserves_callable_order() -> None:
    from fpstreams.execution.sync_ops import apply_operation

    events: list[tuple[str, int]] = []

    def mapped(value: int) -> int:
        events.append(("map", value))
        return value + 10

    def tapped(value: int) -> None:
        events.append(("tap", value))

    def accepted(value: int) -> bool:
        events.append(("filter", value))
        return value % 2 == 1

    iterator = apply_operation(iter([1, 2, 3]), MapOp(mapped))
    iterator = apply_operation(iterator, TapOp(tapped))
    iterator = apply_operation(iterator, FilterOp(accepted))

    assert list(iterator) == [11, 13]
    assert events == [
        ("map", 1),
        ("tap", 11),
        ("filter", 11),
        ("map", 2),
        ("tap", 12),
        ("filter", 12),
        ("map", 3),
        ("tap", 13),
        ("filter", 13),
    ]


def test_async_operation_dispatch_covers_every_planned_operation() -> None:
    spec = importlib.util.find_spec("fpstreams.execution.async_ops")
    assert spec is not None, "the focused asynchronous dispatch module must exist"

    from fpstreams.execution.async_ops import (
        ASYNC_OPERATION_HANDLERS,
        SUPPORTED_ASYNC_OPERATION_TYPES,
    )
    from fpstreams.planning.async_ import _AsyncOperation

    assert len(SUPPORTED_ASYNC_OPERATION_TYPES) == len(set(SUPPORTED_ASYNC_OPERATION_TYPES))
    assert tuple(ASYNC_OPERATION_HANDLERS) == SUPPORTED_ASYNC_OPERATION_TYPES
    assert set(SUPPORTED_ASYNC_OPERATION_TYPES) == set(get_args(_AsyncOperation))


@pytest.mark.asyncio
async def test_async_operation_dispatch_applies_take_operation() -> None:
    import fpstreams.execution.async_ops as async_ops
    from fpstreams.planning.async_ import _Take

    async def source():
        for value in (1, 2, 3):
            yield value

    assert hasattr(async_ops, "apply_async_operation"), (
        "the asynchronous dispatcher must be callable directly"
    )
    iterator = async_ops.apply_async_operation(source(), _Take(2))

    assert [item async for item in iterator] == [1, 2]


@pytest.mark.asyncio
async def test_async_operation_dispatch_rejects_unknown_types() -> None:
    from fpstreams.execution.async_ops import apply_async_operation
    from fpstreams.planning.async_ import _AsyncOperation

    async def source():
        if False:
            yield None

    with pytest.raises(TypeError, match="unsupported asynchronous operation: object"):
        apply_async_operation(source(), cast(_AsyncOperation, object()))


@pytest.mark.asyncio
async def test_async_stateless_dispatch_preserves_callable_order() -> None:
    from fpstreams.execution.async_ops import apply_async_operation
    from fpstreams.planning.async_ import _Filter, _MapAsync, _Tap

    events: list[tuple[str, int]] = []

    async def source():
        for value in (1, 2, 3):
            yield value

    async def mapped(value: int) -> int:
        events.append(("map", value))
        return value + 10

    async def tapped(value: int) -> None:
        events.append(("tap", value))

    async def accepted(value: int) -> bool:
        events.append(("filter", value))
        return value % 2 == 1

    iterator = apply_async_operation(
        source(), _MapAsync(mapped, concurrency=1, ordered=True, timeout=None)
    )
    iterator = apply_async_operation(iterator, _Tap(tapped))
    iterator = apply_async_operation(iterator, _Filter(accepted))

    assert [item async for item in iterator] == [11, 13]
    assert events == [
        ("map", 1),
        ("tap", 11),
        ("filter", 11),
        ("map", 2),
        ("tap", 12),
        ("filter", 12),
        ("map", 3),
        ("tap", 13),
        ("filter", 13),
    ]
