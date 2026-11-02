"""AsyncFlow transforms, bounded concurrency, real-time operators, terminals, and cleanup."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator
from itertools import pairwise

import pytest

import fpstreams


class _AsyncCloseFailureSource(AsyncIterator[int]):
    def __init__(self) -> None:
        self.emitted = False
        self.close_calls = 0

    async def __anext__(self) -> int:
        if self.emitted:
            raise StopAsyncIteration
        self.emitted = True
        return 1

    async def aclose(self) -> None:
        self.close_calls += 1
        raise OSError("source close failed")


# --- Statistics, sketches, and cancellation ---


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal", ["reduce_by", "frequencies"])
async def test_async_grouping_preserves_key_equality_failure(terminal: str) -> None:
    failure = TypeError("equality failed")
    events: list[str] = []

    class Key:
        def __hash__(self) -> int:
            events.append("hash")
            return 1

        def __eq__(self, other: object) -> bool:
            events.append("eq")
            raise failure

    async def source():
        try:
            yield Key()
            yield Key()
        finally:
            events.append("close")

    values = fpstreams.aflow(source())
    with pytest.raises(TypeError) as captured:
        if terminal == "reduce_by":
            await values.reduce_by(
                lambda value: value, lambda state, _: state + 1, initializer=lambda: 0
            )
        else:
            await values.frequencies()
    assert captured.value is failure
    assert events == ["hash", "hash", "hash", "eq", "close"]


@pytest.mark.asyncio
async def test_async_grouping_preserves_hash_and_source_errors() -> None:
    failure = TypeError("hash failed")
    source_failure = TypeError("source failed")

    class Key:
        calls = 0

        def __hash__(self) -> int:
            self.calls += 1
            if self.calls == failing_call:
                raise failure
            return 0

    async def source():
        yield "first"
        raise source_failure

    for failing_call in (1, 2):
        key = Key()
        closed = []

        async def keys(value=key, close_log=closed):
            try:
                yield value
            finally:
                close_log.append(True)

        with pytest.raises(TypeError) as captured:
            await fpstreams.aflow(keys()).frequencies()
        assert captured.value is failure
        assert key.calls == failing_call
        assert closed == [True]
    with pytest.raises(TypeError) as captured:
        await fpstreams.aflow(source()).frequencies()
    assert captured.value is source_failure
    with pytest.raises(TypeError, match="keys must be hashable"):
        await fpstreams.aflow([[1]]).frequencies()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["statistics", "rolling"])
async def test_async_statistics_reject_complex_scalars_and_close_upstream(mode):
    np = pytest.importorskip("numpy")
    closed = []

    async def source():
        try:
            yield 1
            yield np.complex64(2 + 3j)
            raise AssertionError("read past invalid data")
        finally:
            closed.append(True)

    query = fpstreams.aflow(source())
    with pytest.raises(TypeError, match="real numeric values"):
        if mode == "rolling":
            await query.rolling_statistics(2).to_list()
        else:
            await query.collect(fpstreams.Collectors.statistics())
    assert closed == [True]


@pytest.mark.asyncio
async def test_async_statistics_match_sync_with_fresh_state_and_cancellation():
    from fpstreams import Collectors, aflow, flow

    values = [None, 10**12, 10**12 + 1, 10**12 + 2, None, 0]
    expected = flow(values).rolling_statistics(3, ddof=1).to_list()
    pipeline = aflow(values).rolling_statistics(3, ddof=1)
    assert await pipeline.to_list() == expected
    assert await pipeline.to_list() == expected
    assert await aflow(values).collect(Collectors.statistics()) == Collectors.statistics()(values)

    closed = asyncio.Event()
    entered = asyncio.Event()

    async def source():
        try:
            yield 1
            entered.set()
            await asyncio.Event().wait()
        finally:
            closed.set()

    task = asyncio.create_task(aflow(source()).rolling_statistics(2).to_list())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert closed.is_set()


@pytest.mark.asyncio
async def test_optional_sketch_collectors_stream_async_without_materializing():
    pytest.importorskip("datasketches")
    from fpstreams import Collectors, aflow

    closed = []

    async def source():
        try:
            for i in range(10_000):
                yield {"value": i % 100}
        finally:
            closed.append(True)

    result = await aflow(source()).collect(
        quantiles=Collectors.approx_quantiles([0, 1], "value"),
        distinct=Collectors.approx_count_distinct("value"),
    )
    assert result["quantiles"] == (0, 99)
    assert result["distinct"] == pytest.approx(100, rel=0.01)
    assert closed == [True]


@pytest.mark.parametrize("method", ["take", "limit", "drop", "skip"])
@pytest.mark.parametrize("count", [float("nan"), float("inf"), 2.5, 2.0])
async def test_async_prefix_counts_reject_nonintegers_before_opening_source(method, count):
    opened = []

    async def source():
        opened.append(True)
        yield 1

    with pytest.raises(TypeError):
        getattr(fpstreams.aflow.defer(source), method)(count)
    assert opened == []


@pytest.mark.parametrize(
    "method,want", [("take", [0, 1]), ("limit", [0, 1]), ("drop", [2, 3, 4]), ("skip", [2, 3, 4])]
)
async def test_async_prefix_counts_normalize_index_once_and_close_source(method, want):
    indexed = []
    closed = []

    class Count:
        def __index__(self):
            indexed.append(True)
            return 2

    async def source():
        try:
            for value in range(5):
                yield value
        finally:
            closed.append(True)

    query = getattr(fpstreams.aflow.defer(source), method)(Count())
    assert indexed == [True]
    assert closed == []
    assert await query.to_list() == want
    assert await query.to_list() == want
    assert indexed == [True]
    assert closed == [True, True]


@pytest.mark.parametrize("method", ["take", "limit", "drop", "skip"])
async def test_async_prefix_counts_preserve_index_error_without_opening_source(method):
    failure = RuntimeError("count conversion failed")
    opened = []

    class Count:
        def __index__(self):
            raise failure

    async def source():
        opened.append(True)
        yield 1

    with pytest.raises(RuntimeError) as caught:
        getattr(fpstreams.aflow.defer(source), method)(Count())
    assert caught.value is failure
    assert opened == []


@pytest.mark.parametrize("constructor", [fpstreams.aflow, fpstreams.AsyncFlow])
@pytest.mark.parametrize("awaitable", [False, True])
async def test_async_unfold_is_lazy_replayable_and_preserves_none(constructor, awaitable):
    calls = []

    def step(state):
        calls.append(state)
        return None if state == 0 else (None if state == 2 else state, state - 1)

    async def async_step(state):
        await asyncio.sleep(0)
        return step(state)

    query = constructor.unfold(2, async_step if awaitable else step)
    assert calls == []
    assert await query.take(0).to_list() == []
    assert calls == []
    assert await query.to_list() == [None, 1]
    assert await query.take(1).to_list() == [None]
    assert calls == [2, 1, 0, 2]
    assert await constructor.unfold(None, lambda state: None).to_list() == []


async def test_async_unfold_propagates_errors_and_cancellation_without_extra_steps():
    started = asyncio.Event()
    cancelled = []

    async def step(state):
        started.set()
        try:
            await asyncio.Future()
        finally:
            cancelled.append(state)

    query = fpstreams.aflow.unfold(7, step)
    task = asyncio.create_task(query.to_list())
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert cancelled == [7]
    with pytest.raises(TypeError, match="function must be callable"):
        fpstreams.aflow.unfold(0, None)
    failure = ValueError("async step failed")

    async def fail(state):
        raise failure

    with pytest.raises(ValueError) as caught:
        await fpstreams.aflow.unfold(0, fail).to_list()
    assert caught.value is failure
    with pytest.raises(ValueError):
        await fpstreams.aflow.unfold(0, lambda state: (1,)).first()


@pytest.mark.parametrize("awaitable", [False, True])
async def test_async_map_accum_orders_steps_and_resets_state(awaitable):
    calls = []

    def step(state, item):
        calls.append((state, item))
        return state + item, f"{state}:{item}"

    async def async_step(state, item):
        await asyncio.sleep(0)
        return step(state, item)

    query = fpstreams.aflow([2, 3]).map_accum(0, async_step if awaitable else step)
    assert calls == []
    assert await query.to_list() == ["0:2", "2:3"]
    assert await query.to_list() == ["0:2", "2:3"]
    assert calls == [(0, 2), (2, 3), (0, 2), (2, 3)]
    assert await fpstreams.aflow([]).map_accum(0, step).to_list() == []
    left, right = query.__aiter__(), query.__aiter__()
    try:
        assert [await anext(left), await anext(right), await anext(left), await anext(right)] == [
            "0:2",
            "0:2",
            "2:3",
            "2:3",
        ]
    finally:
        await left.aclose()
        await right.aclose()


async def test_async_map_accum_preserves_close_errors_and_output_identity():
    seen = []
    closed = []
    value = object()
    failure = ValueError("state callback failed")

    async def source():
        try:
            for item in (value, None):
                seen.append(item)
                yield item
        finally:
            closed.append(True)

    async def step(state, item):
        if state:
            raise failure
        return state + 1, item

    query = fpstreams.aflow.defer(source).map_accum(0, step)
    assert await query.take(1).to_list() == [value]
    assert seen == [value] and closed == [True]
    with pytest.raises(ValueError) as caught:
        await query.to_list()
    assert caught.value is failure and closed == [True, True]
    assert await fpstreams.aflow([None, value]).map_accum(
        None, lambda state, item: (state, item)
    ).to_list() == [None, value]
    with pytest.raises(TypeError, match="function must be callable"):
        fpstreams.aflow([]).map_accum(0, None)
    with pytest.raises(ValueError):
        await fpstreams.aflow([1]).map_accum(0, lambda state, item: (item,)).to_list()


async def test_async_map_accum_cancels_one_transition_and_closes_its_source():
    started = asyncio.Event()
    seen = []
    closed = []
    cancelled = []

    async def source():
        try:
            for value in (1, 2):
                seen.append(value)
                yield value
        finally:
            closed.append(True)

    async def step(state, value):
        started.set()
        try:
            await asyncio.Future()
        finally:
            cancelled.append((state, value))

    query = fpstreams.aflow(source()).map_accum(0, step)
    task = asyncio.create_task(query.to_list())
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert seen == [1] and cancelled == [(0, 1)] and closed == [True]


@pytest.mark.parametrize("async_transition", [False, True])
async def test_async_map_accum_resolves_one_output_awaitable(async_transition):
    loop = asyncio.get_running_loop()
    inner = loop.create_future()
    inner.set_result("value")
    outer = loop.create_future()
    outer.set_result(inner)
    seen = []
    closed = []

    async def source():
        try:
            yield 1
            yield 2
        finally:
            closed.append(True)

    def step(state, item):
        seen.append((state, item))
        return inner, outer

    async def async_step(state, item):
        return step(state, item)

    result = (
        await fpstreams.aflow(source())
        .map_accum("initial", async_step if async_transition else step)
        .to_list()
    )
    assert result == [inner, inner]
    assert result[0] is inner
    assert seen == [("initial", 1), (inner, 2)]
    assert closed == [True]
    assert fpstreams.flow([1]).map_accum("initial", step).to_list()[0] is outer

    failure = ValueError("output failed")
    failed = loop.create_future()
    failed.set_exception(failure)
    closed.clear()

    def fail(state, item):
        return state, failed

    async def async_fail(state, item):
        return fail(state, item)

    with pytest.raises(ValueError) as captured:
        await (
            fpstreams.aflow(source())
            .map_accum(0, async_fail if async_transition else fail)
            .to_list()
        )
    assert captured.value is failure
    assert closed == [True]


@pytest.mark.parametrize("shielded", [False, True])
async def test_async_map_accum_cancels_awaited_output_and_closes_source(shielded):
    future = asyncio.get_running_loop().create_future()
    started = asyncio.Event()
    closed = []

    async def source():
        try:
            yield 1
        finally:
            closed.append(True)

    def step(state, item):
        started.set()
        return state, asyncio.shield(future) if shielded else future

    task = asyncio.create_task(fpstreams.aflow(source()).map_accum(0, step).to_list())
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert closed == [True]
    assert future.cancelled() is not shielded
    if not future.done():
        future.set_result(None)


@pytest.mark.parametrize("keyed", [False, True])
async def test_async_unique_propagates_equality_failure_and_closes_source(keyed):
    failure = TypeError("async equality failed")
    hashes = []
    closed = []

    class Key:
        def __hash__(self):
            hashes.append(self)
            return 0

        def __eq__(self, other):
            raise failure

    first, second = Key(), Key()

    async def source():
        try:
            yield first
            yield second
        finally:
            closed.append(True)

    query = fpstreams.aflow(source())
    with pytest.raises(TypeError) as caught:
        await (query.unique_by(lambda x: x) if keyed else query.unique()).to_list()
    assert caught.value is failure
    assert len(hashes) == 3
    assert hashes[0] is hashes[1] is first and hashes[2] is second
    assert closed == [True]


class _PrefetchBlockingSource:
    """Emit once when requested, then expose producer cancellation and close counts."""

    def __init__(self, *, block_first: bool = False) -> None:
        self.block_first = block_first
        self.emitted = False
        self.pull_started = asyncio.Event()
        self.pull_cancelled = asyncio.Event()
        self.close_calls = 0
        self._never = asyncio.Event()

    def __aiter__(self) -> _PrefetchBlockingSource:
        return self

    async def __anext__(self) -> int:
        if not self.block_first and not self.emitted:
            self.emitted = True
            return 1
        self.pull_started.set()
        try:
            await self._never.wait()
        except asyncio.CancelledError:
            self.pull_cancelled.set()
            raise
        raise AssertionError("unreachable")

    async def aclose(self) -> None:
        self.close_calls += 1


async def test_async_map_batches_is_lazy_awaits_callbacks_and_keeps_tail_results() -> None:
    seen = []
    calls = []
    closed = []

    async def source():
        try:
            for value in range(5):
                seen.append(value)
                yield value
        finally:
            closed.append(True)

    async def work(batch):
        await asyncio.sleep(0)
        calls.append(batch)
        return list(batch)

    query = fpstreams.aflow(source()).map_batches(work, size=2)
    assert seen == calls == closed == []
    assert await query.to_list() == [[0, 1], [2, 3], [4]]
    assert calls == [(0, 1), (2, 3), (4,)]
    assert seen == [0, 1, 2, 3, 4]
    assert closed == [True]
    assert await fpstreams.aflow(range(3)).map_batches(sum, size=2).to_list() == [1, 2]


async def test_async_map_batches_does_not_call_empty_input_or_flatten_results() -> None:
    def reject_empty(batch):
        raise AssertionError("empty input called the batch callback")

    assert await fpstreams.aflow([]).map_batches(reject_empty, size=2).to_list() == []
    assert await fpstreams.aflow([1]).map_batches(lambda batch: None, size=2).to_list() == [None]
    assert await fpstreams.aflow(range(3)).map_batches(list, size=2).flat_map(iter).to_list() == [
        0,
        1,
        2,
    ]


async def test_async_map_batches_early_exit_bounds_pulls_and_cancels_other_batches() -> None:
    started = asyncio.Event()
    cancelled = []
    seen = []
    closed = []

    async def source():
        try:
            for value in range(100):
                seen.append(value)
                yield value
        finally:
            closed.append(True)

    async def work(batch):
        if batch[0] == 0:
            await started.wait()
            return sum(batch)
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.append(batch)
            raise

    query = fpstreams.aflow(source()).map_batches(work, size=2, concurrency=2, buffer=2)
    assert await asyncio.wait_for(query.take(1).to_list(), 5) == [1]
    assert seen == [0, 1, 2, 3]
    assert cancelled == [(2, 3)]
    assert closed == [True]


@pytest.mark.parametrize("ordered", [True, False])
async def test_async_map_batches_callback_failure_preserves_error_and_closes_source(ordered):
    closed = []
    failure = ValueError("async batch failed")

    async def values():
        try:
            for value in range(7):
                yield value
        finally:
            closed.append(True)

    async def work(batch):
        if batch[0] == 2:
            raise failure
        return sum(batch)

    with pytest.raises(ValueError, match="async batch failed") as caught:
        await (
            fpstreams.aflow(values())
            .map_batches(work, size=2, concurrency=2, buffer=2, ordered=ordered)
            .to_list()
        )
    assert caught.value is failure
    assert closed == [True]


async def test_async_map_batches_timeout_cancels_callback_and_closes_source() -> None:
    closed = []
    finished = []

    async def source():
        try:
            for value in range(3):
                yield value
        finally:
            closed.append(True)

    async def work(batch):
        try:
            await asyncio.Event().wait()
        finally:
            finished.append(batch)

    with pytest.raises(TimeoutError):
        await fpstreams.aflow(source()).map_batches(work, size=2, timeout=0.01).to_list()
    assert finished == [(0, 1)]
    assert closed == [True]


def test_async_map_batches_validates_bounds_without_opening_the_source() -> None:
    seen = []

    async def source():
        seen.append(True)
        yield 1

    query = fpstreams.aflow(source())
    for kwargs in (
        {"size": 0},
        {"size": -1},
        {"size": 2, "concurrency": 0},
        {"size": 2, "buffer": 0},
        {"size": 2, "timeout": 0},
    ):
        with pytest.raises(ValueError):
            query.map_batches(sum, **kwargs)
    with pytest.raises(TypeError):
        query.map_batches(sum, size=1.5)
    assert seen == []


# --- AsyncFlow API and concurrency ---


@pytest.mark.asyncio
async def test_async_flow_maps_with_bounded_ordered_concurrency() -> None:
    active = 0
    peak = 0

    async def work(value: int) -> int:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep((4 - value) * 0.001)
        active -= 1
        return value * 10

    result = await fpstreams.aflow([1, 2, 3]).map_async(work, concurrency=2, ordered=True).to_list()

    assert result == [10, 20, 30]
    assert peak == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("ordered", [True, False])
@pytest.mark.parametrize("mapper_fails", [False, True])
async def test_async_map_delivers_completion_while_next_source_pull_waits(
    ordered, mapper_fails
) -> None:
    pulling = asyncio.Event()
    source_closed = []
    mapper_finished = []
    failure = LookupError("mapper failed before next input")

    async def source():
        try:
            yield 1
            pulling.set()
            await asyncio.Event().wait()
        finally:
            source_closed.append(True)

    async def mapper(value):
        try:
            await pulling.wait()
            if mapper_fails:
                raise failure
            return value * 10
        finally:
            mapper_finished.append(value)

    iterator = (
        fpstreams.aflow(source()).map_async(mapper, concurrency=2, ordered=ordered).__aiter__()
    )
    try:
        if mapper_fails:
            with pytest.raises(LookupError) as caught:
                await asyncio.wait_for(anext(iterator), timeout=1)
            assert caught.value is failure
        else:
            assert await asyncio.wait_for(anext(iterator), timeout=1) == 10
    finally:
        await iterator.aclose()

    assert mapper_finished == [1]
    assert source_closed == [True]


@pytest.mark.asyncio
@pytest.mark.parametrize("error_type", [LookupError, asyncio.CancelledError])
async def test_async_map_preserves_upstream_failure_and_drains_mapper(error_type) -> None:
    mapping = asyncio.Event()
    closed = []

    async def source():
        try:
            yield 1
            await mapping.wait()
            raise error_type("upstream stopped")
        finally:
            closed.append("source")

    async def mapper(value):
        try:
            mapping.set()
            await asyncio.Event().wait()
        finally:
            closed.append("mapper")

    with pytest.raises(error_type, match="upstream stopped"):
        await asyncio.wait_for(
            fpstreams.aflow(source()).map_async(mapper, concurrency=2).to_list(), timeout=1
        )
    assert sorted(closed) == ["mapper", "source"]


@pytest.mark.asyncio
@pytest.mark.parametrize("ordered", [True, False])
@pytest.mark.parametrize("cleanup_waiter", ["source", "mapper"])
async def test_async_map_cancellation_drains_waiting_source_pull_and_mapper(
    ordered, cleanup_waiter
) -> None:
    from fpstreams.execution.asynchronous import execute_async_physical
    from fpstreams.physical.async_plan import compile_async_query
    from fpstreams.runtime.query import QueryRuntime

    pulling = asyncio.Event()
    mapping = asyncio.Event()
    source_cancelled = asyncio.Event()
    mapper_cancelled = asyncio.Event()
    closed = []

    async def source():
        try:
            yield 1
            pulling.set()
            await asyncio.Event().wait()
        finally:
            source_cancelled.set()
            if cleanup_waiter == "source":
                await mapper_cancelled.wait()
            closed.append("source")

    async def mapper(value):
        try:
            mapping.set()
            await asyncio.Event().wait()
        finally:
            mapper_cancelled.set()
            if cleanup_waiter == "mapper":
                await source_cancelled.wait()
            closed.append("mapper")

    pipeline = fpstreams.aflow(source()).map_async(mapper, concurrency=2, ordered=ordered)
    runtime = QueryRuntime()

    async def consume():
        return [
            value
            async for value in execute_async_physical(
                compile_async_query(pipeline._query("list")), runtime
            )
        ]

    task = asyncio.create_task(consume())
    try:
        await asyncio.wait_for(pulling.wait(), timeout=1)
        await asyncio.wait_for(mapping.wait(), timeout=1)
        task.cancel()
        # Unlike wait_for, this deadline does not wait forever for broken
        # cancellation cleanup. The finally block releases either dependency.
        done, _ = await asyncio.wait((task,), timeout=1)
        assert task in done
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        source_cancelled.set()
        mapper_cancelled.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert sorted(closed) == ["mapper", "source"]
    assert runtime.metrics.live_tasks == 0
    assert runtime.metrics.high_water_tasks == 2
    assert runtime.resources.closed


@pytest.mark.asyncio
async def test_async_map_publish_failure_propagates_and_closes_source() -> None:
    from fpstreams.runtime.failpoints import failpoint

    class Source:
        def __init__(self) -> None:
            self.position = 0
            self.close_calls = 0

        def __aiter__(self) -> Source:
            return self

        async def __anext__(self) -> int:
            if self.position == 2:
                raise StopAsyncIteration
            self.position += 1
            return self.position

        async def aclose(self) -> None:
            self.close_calls += 1

    async def double(value: int) -> int:
        await asyncio.sleep(0)
        return value * 2

    source = Source()
    with (
        failpoint("task.complete.before_publish", RuntimeError("publish failed")),
        pytest.raises(RuntimeError, match="publish failed"),
    ):
        await fpstreams.aflow(source).map_async(double, concurrency=2).to_list()

    assert source.close_calls == 1


@pytest.mark.asyncio
async def test_async_ordered_map_buffer_refills_without_exceeding_its_window() -> None:
    release_head = asyncio.Event()
    four_started = asyncio.Event()
    started: list[int] = []
    active = 0
    peak = 0

    async def work(value: int) -> int:
        nonlocal active, peak
        started.append(value)
        active += 1
        peak = max(peak, active)
        if len(started) == 4:
            four_started.set()
        try:
            if value == 0:
                await release_head.wait()
            else:
                await asyncio.sleep(0)
            return value
        finally:
            active -= 1

    execution = asyncio.create_task(
        fpstreams.aflow(range(6)).map_async(work, concurrency=2, ordered=True, buffer=4).to_list()
    )
    try:
        await asyncio.wait_for(four_started.wait(), timeout=1)
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        assert started == [0, 1, 2, 3]
        assert peak == 2

        release_head.set()
        assert await asyncio.wait_for(execution, timeout=1) == [0, 1, 2, 3, 4, 5]
    finally:
        release_head.set()
        if not execution.done():
            execution.cancel()
        await asyncio.gather(execution, return_exceptions=True)


def test_async_map_validates_buffer() -> None:
    with pytest.raises(ValueError, match="buffer must be at least 1"):
        fpstreams.aflow([1]).map_async(lambda value: value, buffer=0)


def test_async_map_normalizes_integer_bounds_once_before_opening_source() -> None:
    opened = []

    class Count:
        def __init__(self, value):
            self.value = value
            self.calls = 0

        def __index__(self):
            self.calls += 1
            return self.value

    async def source():
        opened.append(True)
        yield 1

    query = fpstreams.aflow(source())
    for ordered in (True, False):
        for invalid in (float("nan"), float("inf"), 1.5, "2"):
            with pytest.raises(TypeError):
                query.map_async(lambda value: value, buffer=invalid, ordered=ordered)

    concurrency, buffer = Count(2), Count(3)
    operation = (
        query.map_async(lambda value: value, concurrency=concurrency, buffer=buffer)
        .explain("list")
        .to_dict()["operations"][0]
    )
    assert operation["state"]["bound"] == 3
    assert concurrency.calls == buffer.calls == 1
    assert opened == []


@pytest.mark.parametrize(("buffer", "bound"), [(None, 6), (5, 5)])
def test_async_map_explains_its_submitted_result_bound(
    buffer: int | None,
    bound: int,
) -> None:
    operation = (
        fpstreams.aflow([1, 2])
        .map_async(lambda value: value, concurrency=3, buffer=buffer)
        .explain("list")
        .to_dict()["operations"][0]
    )

    assert operation["state"] == {"kind": "bounded", "bound": bound, "spillable": False}


@pytest.mark.asyncio
async def test_async_flow_from_queue_consumes_lazily_in_fifo_order_until_identity_stop() -> None:
    class ObservedQueue(asyncio.Queue[object]):
        def __init__(self) -> None:
            super().__init__()
            self.get_calls = 0

        async def get(self) -> object:
            self.get_calls += 1
            return await super().get()

    stop = {"kind": "stop"}
    equal_but_not_identical = {"kind": "stop"}
    queue = ObservedQueue()
    for value in (3, equal_but_not_identical, 1, stop):
        queue.put_nowait(value)

    values = fpstreams.AsyncFlow.from_queue(queue, stop=stop)

    assert queue.get_calls == 0
    assert await values.to_list() == [3, equal_but_not_identical, 1]
    assert queue.get_calls == 4
    with pytest.raises(fpstreams.FlowConsumedError):
        await values.to_list()
    assert queue.get_calls == 4


def test_async_flow_from_queue_exposes_precise_one_shot_source_facts() -> None:
    default_source = fpstreams.AsyncFlow.from_queue(asyncio.Queue[int]())
    stopped_source = fpstreams.aflow.from_queue(asyncio.Queue[object](), stop=object())

    default_facts = default_source.explain("list").to_dict()["source"]
    stopped_facts = stopped_source.explain("list").to_dict()["source"]
    taken_facts = default_source.take(1).explain("list").to_dict()["semantics"]["output"]

    assert default_facts == {
        "termination": "unknown",
        "cardinality": {"kind": "unknown", "value": None},
        "replayability": "one_shot",
        "ordering": "ordered",
    }
    assert stopped_facts == default_facts
    assert taken_facts["termination"] == "unknown"


@pytest.mark.asyncio
async def test_async_flow_from_queue_treats_queue_shutdown_as_source_end(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeQueueShutDown(Exception):
        pass

    class ShutdownQueue(asyncio.Queue[int]):
        async def get(self) -> int:
            raise FakeQueueShutDown

    monkeypatch.setattr(asyncio, "QueueShutDown", FakeQueueShutDown, raising=False)

    assert await fpstreams.aflow.from_queue(ShutdownQueue()).to_list() == []


@pytest.mark.asyncio
async def test_async_flow_from_queue_early_close_does_not_own_or_acknowledge_queue() -> None:
    class ObservedQueue(asyncio.Queue[object]):
        def __init__(self) -> None:
            super().__init__()
            self.task_done_calls = 0

        def task_done(self) -> None:
            self.task_done_calls += 1
            super().task_done()

    stop = object()
    queue = ObservedQueue()
    for value in (1, 2, stop):
        queue.put_nowait(value)
    iterator = fpstreams.aflow.from_queue(queue, stop=stop).__aiter__()

    assert await anext(iterator) == 1
    await iterator.aclose()

    assert queue.task_done_calls == 0
    assert queue.get_nowait() == 2
    assert queue.get_nowait() is stop


@pytest.mark.asyncio
async def test_async_flow_from_queue_cancellation_cancels_pending_get_without_task_leak() -> None:
    class BlockingQueue(asyncio.Queue[int]):
        def __init__(self) -> None:
            super().__init__()
            self.get_started = asyncio.Event()
            self.get_cancelled = asyncio.Event()

        async def get(self) -> int:
            self.get_started.set()
            try:
                return await super().get()
            except asyncio.CancelledError:
                self.get_cancelled.set()
                raise

    queue = BlockingQueue()
    task = asyncio.create_task(fpstreams.aflow.from_queue(queue).to_list())
    await queue.get_started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert queue.get_cancelled.is_set()
    assert task.done()
    assert task not in asyncio.all_tasks()


@pytest.mark.asyncio
async def test_async_flow_from_queue_prefetch_preserves_complete_fifo_consumption() -> None:
    stop = object()
    queue: asyncio.Queue[object] = asyncio.Queue()
    for value in (1, 2, 3, 4, stop):
        queue.put_nowait(value)

    result = await fpstreams.aflow.from_queue(queue, stop=stop).prefetch(2).to_list()

    assert result == [1, 2, 3, 4]
    assert queue.empty()


@pytest.mark.asyncio
async def test_async_prefetch_is_lazy_and_preserves_order_and_values() -> None:
    opened = False

    async def source() -> AsyncIterator[int]:
        nonlocal opened
        opened = True
        for value in (3, 1, 2):
            yield value

    values = fpstreams.aflow(source()).prefetch(capacity=2)

    assert not opened
    assert await values.to_list() == [3, 1, 2]


@pytest.mark.asyncio
async def test_async_prefetch_propagates_an_upstream_cancelled_error() -> None:
    delivered = asyncio.Event()
    release_downstream = asyncio.Event()

    async def source() -> AsyncIterator[int]:
        yield 1
        raise asyncio.CancelledError("upstream cancelled itself")

    async def hold_first(value: int) -> None:
        assert value == 1
        delivered.set()
        await release_downstream.wait()

    task = asyncio.create_task(fpstreams.aflow(source()).prefetch(2).tap(hold_first).to_list())
    await delivered.wait()
    release_downstream.set()
    with pytest.raises(asyncio.CancelledError, match="upstream cancelled itself"):
        await asyncio.wait_for(task, timeout=1)


@pytest.mark.asyncio
async def test_async_prefetch_closes_upstream_when_producer_task_creation_fails() -> None:
    from fpstreams.runtime.failpoints import failpoint

    class Source:
        def __init__(self) -> None:
            self.close_calls = 0

        def __aiter__(self) -> Source:
            return self

        async def __anext__(self) -> int:
            return 1

        async def aclose(self) -> None:
            self.close_calls += 1

    source = Source()
    with (
        failpoint("task.create.after", RuntimeError("producer admission failed")),
        pytest.raises(RuntimeError, match="producer admission failed"),
    ):
        await fpstreams.aflow(source).prefetch(1).to_list()

    assert source.close_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("operator", ["prefetch", "map_async"])
@pytest.mark.parametrize("ending", ["explicit", "cancel"])
@pytest.mark.parametrize("close_fails", [False, True])
async def test_scope_cleanup_submission_failure_cancels_before_source_close(
    operator, ending, close_fails
) -> None:
    """A rejected cleanup owner cannot let either shutdown path close an active source."""
    events = []
    submission = OSError("scope owner rejected")
    source_failure = OSError("source close failed")

    class Source:
        def __init__(self):
            self.emitted = False
            self.active = False
            self.pulling = asyncio.Event()
            self.closed_while_active = False
            self.close_calls = 0

        def __aiter__(self):
            return self

        async def __anext__(self):
            if not self.emitted:
                self.emitted = True
                return 0
            self.active = True
            self.pulling.set()
            try:
                await asyncio.Event().wait()
            finally:
                self.active = False
                events.append("pull-ended")

        async def aclose(self):
            self.closed_while_active |= self.active
            self.close_calls += 1
            events.append("close")
            if close_fails:
                raise source_failure

    source = Source()

    async def identity(value):
        return value

    values = fpstreams.aflow(source)
    values = (
        values.prefetch(2) if operator == "prefetch" else values.map_async(identity, concurrency=2)
    )
    iterator = values.__aiter__() if ending == "explicit" else None
    consumer = asyncio.create_task(values.to_list()) if ending == "cancel" else None
    loop = asyncio.get_running_loop()
    previous = loop.get_task_factory()
    rejected = []

    def reject_once(loop, coroutine, **kwargs):
        if not rejected:
            rejected.append(coroutine.__qualname__)
            raise submission
        if previous is not None:
            return previous(loop, coroutine, **kwargs)
        return asyncio.Task(coroutine, loop=loop, **kwargs)

    try:
        async with asyncio.timeout(10):
            if iterator is not None:
                assert await anext(iterator) == 0
            await source.pulling.wait()
            loop.set_task_factory(reject_once)
            if consumer is not None:
                consumer.cancel("caller cancelled")
                with pytest.raises(asyncio.CancelledError) as caught:
                    await consumer
                assert consumer.cancelled()
                assert caught.value.args == ("caller cancelled",)
                expected = ["cleanup failed: OSError: scope owner rejected"]
            else:
                with pytest.raises(OSError) as caught:
                    await iterator.aclose()
                assert caught.value is submission
                expected = []
            if close_fails:
                expected.append("cleanup failed: OSError: source close failed")
            assert getattr(caught.value, "__notes__", []) == expected
        assert rejected == ["TaskScope._aclose_impl"]
        assert events == ["pull-ended", "close"]
        assert source.close_calls == 1
        assert not source.closed_while_active
    finally:
        loop.set_task_factory(previous)
        if consumer is not None:
            consumer.cancel()
            cleanup = consumer
        else:
            cleanup = iterator.aclose()
        await asyncio.gather(cleanup, return_exceptions=True)


@pytest.mark.asyncio
async def test_async_prefetch_aclose_runs_both_cleanup_phases(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fpstreams.runtime.tasks import TaskScope

    original_aclose = TaskScope.aclose

    async def fail_after_scope_cleanup(scope: TaskScope) -> None:
        await original_aclose(scope)
        raise RuntimeError("scope cleanup failed")

    class Source:
        def __init__(self) -> None:
            self.emitted = False
            self.close_calls = 0

        def __aiter__(self) -> Source:
            return self

        async def __anext__(self) -> int:
            if self.emitted:
                await asyncio.Event().wait()
            self.emitted = True
            return 1

        async def aclose(self) -> None:
            self.close_calls += 1
            raise OSError("source cleanup failed")

    monkeypatch.setattr(TaskScope, "aclose", fail_after_scope_cleanup)
    source = Source()
    iterator = fpstreams.aflow(source).prefetch(2).__aiter__()
    assert await anext(iterator) == 1

    with pytest.raises(RuntimeError, match="scope cleanup failed") as caught:
        await iterator.aclose()

    assert source.close_calls == 1
    assert caught.value.__notes__ == ["cleanup failed: OSError: source cleanup failed"]


@pytest.mark.asyncio
async def test_async_prefetch_preserves_pipeline_error_over_both_cleanup_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fpstreams.runtime.tasks import TaskScope

    original_aclose = TaskScope.aclose

    async def fail_after_scope_cleanup(scope: TaskScope) -> None:
        await original_aclose(scope)
        raise RuntimeError("scope cleanup failed")

    class Source:
        def __init__(self) -> None:
            self.close_calls = 0

        def __aiter__(self) -> Source:
            return self

        async def __anext__(self) -> int:
            raise ValueError("pipeline failed")

        async def aclose(self) -> None:
            self.close_calls += 1
            raise OSError("source cleanup failed")

    monkeypatch.setattr(TaskScope, "aclose", fail_after_scope_cleanup)
    source = Source()

    with pytest.raises(ValueError, match="pipeline failed") as caught:
        await fpstreams.aflow(source).prefetch(1).to_list()

    assert source.close_calls == 1
    assert caught.value.__notes__ == [
        "cleanup failed: RuntimeError: scope cleanup failed",
        "cleanup failed: OSError: source cleanup failed",
    ]


@pytest.mark.asyncio
async def test_async_prefetch_reserves_capacity_before_pulling_upstream() -> None:
    pulls = 0

    async def source() -> AsyncIterator[int]:
        nonlocal pulls
        for value in range(10):
            pulls += 1
            yield value

    iterator = fpstreams.aflow(source()).prefetch(2).__aiter__()
    try:
        assert pulls == 0
        assert await anext(iterator) == 0

        producer_blocked = asyncio.Event()

        def mark_after_ready_tasks() -> None:
            asyncio.get_running_loop().call_soon(producer_blocked.set)

        asyncio.get_running_loop().call_soon(mark_after_ready_tasks)
        await producer_blocked.wait()

        assert pulls == 3
    finally:
        await iterator.aclose()


@pytest.mark.asyncio
async def test_async_prefetch_drains_accepted_values_before_producer_failure() -> None:
    seen: list[int] = []

    async def source() -> AsyncIterator[int]:
        yield 1
        yield 2
        raise RuntimeError("upstream failed")

    with pytest.raises(RuntimeError, match="upstream failed"):
        await fpstreams.aflow(source()).prefetch(2).tap(seen.append).to_list()

    assert seen == [1, 2]


@pytest.mark.asyncio
async def test_async_prefetch_overlaps_awaited_upstream_and_downstream_work() -> None:
    second_pull_started = asyncio.Event()
    release_second_pull = asyncio.Event()
    downstream_active = False

    async def source() -> AsyncIterator[int]:
        yield 1
        assert downstream_active
        second_pull_started.set()
        await release_second_pull.wait()
        yield 2

    async def downstream(value: int) -> int:
        nonlocal downstream_active
        if value == 1:
            downstream_active = True
            try:
                await second_pull_started.wait()
                release_second_pull.set()
            finally:
                downstream_active = False
        return value

    result = await asyncio.wait_for(
        fpstreams.aflow(source()).prefetch(1).map(downstream).to_list(),
        timeout=1,
    )

    assert result == [1, 2]


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal", ["take", "first"])
async def test_async_prefetch_short_circuits_without_leaving_a_producer(
    terminal: str,
) -> None:
    source = _PrefetchBlockingSource()
    values = fpstreams.aflow(source).prefetch(2)

    result = await values.take(1).to_list() if terminal == "take" else await values.first()

    expected: object = [1] if terminal == "take" else 1
    assert result == expected
    assert source.pull_started.is_set()
    assert source.pull_cancelled.is_set()
    assert source.close_calls == 1


@pytest.mark.asyncio
async def test_async_prefetch_explicit_aclose_cancels_and_closes_once() -> None:
    source = _PrefetchBlockingSource()
    iterator = fpstreams.aflow(source).prefetch(2).__aiter__()

    assert await anext(iterator) == 1
    await iterator.aclose()

    assert source.pull_started.is_set()
    assert source.pull_cancelled.is_set()
    assert source.close_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["prefetch", "session_window"])
async def test_async_consumer_cancellation_closes_pending_work(operation) -> None:
    source = _PrefetchBlockingSource(block_first=operation == "prefetch")
    query = fpstreams.aflow(source)
    query = query.prefetch(1) if operation == "prefetch" else query.session_window(10, max_count=10)
    task = asyncio.create_task(query.to_list())
    await source.pull_started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert source.pull_cancelled.is_set()
    assert source.close_calls == 1


@pytest.mark.asyncio
async def test_async_prefetch_validates_capacity_with_the_index_protocol() -> None:
    class Capacity:
        def __index__(self) -> int:
            return 2

    configured = fpstreams.aflow([1, 2]).prefetch(Capacity())  # type: ignore[arg-type]
    assert await configured.to_list() == [1, 2]
    with pytest.raises(TypeError, match="capacity must be an integer"):
        fpstreams.aflow([1]).prefetch(1.5)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="capacity must be at least 1"):
        fpstreams.aflow([1]).prefetch(0)


@pytest.mark.asyncio
async def test_async_prefetch_preserves_one_shot_source_semantics() -> None:
    values = fpstreams.aflow(iter([1, 2, 3])).prefetch(2)

    assert await values.to_list() == [1, 2, 3]
    with pytest.raises(fpstreams.FlowConsumedError):
        await values.to_list()


@pytest.mark.asyncio
async def test_async_prefetch_uses_physical_semantic_and_report_architecture() -> None:
    from fpstreams.physical.async_plan import AsyncPrefetchNode, compile_async_query

    plain = fpstreams.aflow([1, 2])
    values = plain.prefetch(3)
    physical = compile_async_query(values._query("list"))
    explanation = values.explain("list").to_dict()
    operation = explanation["operations"][0]

    assert compile_async_query(plain._query("list")).nodes == ()
    assert len(physical.nodes) == 1
    assert isinstance(physical.nodes[0], AsyncPrefetchNode)
    assert physical.nodes[0].name == "prefetch"
    assert operation["name"] == "prefetch"
    assert operation["progress"] == "pipelined"
    assert operation["state"] == {"kind": "bounded", "bound": 3, "spillable": False}
    assert operation["input"] == operation["output"] == explanation["source"]
    assert explanation["semantics"]["output"] == explanation["source"]

    execution = await values.run_with_report("to_list")

    assert execution.value == [1, 2]
    assert execution.report.strategy == "async_scheduler"
    assert execution.report.peak_owned_async_tasks == 1


@pytest.mark.asyncio
async def test_concurrent_map_does_not_reawait_completion_queue_observed_tasks() -> None:
    """A captured mapper outcome should need no second await merely to release ownership."""

    class ObservedTask(asyncio.Task):
        def __init__(self, coroutine, *, loop, context=None) -> None:
            self.await_calls = 0
            super().__init__(coroutine, loop=loop, context=context)

        def __await__(self):
            self.await_calls += 1
            return super().__await__()

    loop = asyncio.get_running_loop()
    previous_factory = loop.get_task_factory()
    mapper_tasks: list[ObservedTask] = []

    def observed_task_factory(loop, coroutine, context=None):
        return ObservedTask(coroutine, loop=loop, context=context)

    async def source() -> AsyncIterator[int]:
        for value in range(4):
            await asyncio.sleep(0)
            yield value

    async def mapper(value: int) -> int:
        task = asyncio.current_task()
        assert isinstance(task, ObservedTask)
        mapper_tasks.append(task)
        await asyncio.sleep(0)
        return value + 1

    loop.set_task_factory(observed_task_factory)
    try:
        result = await fpstreams.aflow(source()).map_async(mapper, concurrency=2).to_list()
    finally:
        loop.set_task_factory(previous_factory)

    assert result == [1, 2, 3, 4]
    assert len(mapper_tasks) == 4
    assert [task.await_calls for task in mapper_tasks] == [0, 0, 0, 0]


@pytest.mark.asyncio
async def test_async_flow_has_size_batching_and_incremental_reduce_by_parity() -> None:
    async def size(value: bytes) -> int:
        await asyncio.sleep(0)
        return len(value)

    batches = (
        await fpstreams.aflow([b"12345", b"123", b"12345678", b"1"])
        .batch_by_size(
            10,
            get_size=size,
        )
        .to_list()
    )
    assert batches == [(b"12345", b"123"), (b"12345678", b"1")]

    async def initialize() -> int:
        await asyncio.sleep(0)
        return 0

    async def add(total: int, record: dict[str, int | str]) -> int:
        await asyncio.sleep(0)
        return total + int(record["amount"])

    records = [
        {"team": "a", "amount": 2},
        {"team": "b", "amount": 3},
        {"team": "a", "amount": 5},
    ]
    totals = await fpstreams.aflow(records).reduce_by(
        "team",
        add,
        initializer=initialize,
    )

    assert totals == {"a": 7, "b": 3}
    assert await fpstreams.aflow("abaca").frequencies() == {"a": 3, "b": 1, "c": 1}


@pytest.mark.asyncio
async def test_async_flow_matches_new_jdk_and_streamex_style_transforms() -> None:
    initializer_calls = 0

    async def initialize() -> list[int]:
        nonlocal initializer_calls
        initializer_calls += 1
        await asyncio.sleep(0)
        return []

    async def append(values: list[int], item: int) -> list[int]:
        await asyncio.sleep(0)
        return [*values, item]

    async def even_text(item: int) -> str | None:
        await asyncio.sleep(0)
        return str(item) if item % 2 == 0 else None

    async def parity(item: int) -> int:
        await asyncio.sleep(0)
        return item % 2

    folded = fpstreams.aflow([1, 2, 3]).fold(initialize, append)

    assert await fpstreams.aflow([1, 2]).window(3).to_list() == [(1, 2)]
    assert await fpstreams.aflow([1, 2, 3, 4]).filter_map(even_text).to_list() == [
        "2",
        "4",
    ]
    assert await fpstreams.aflow([1, 4, 9]).pair_map(
        lambda left, right: right - left
    ).to_list() == [3, 5]
    assert await fpstreams.aflow([1, 1, 2, 4, 3, 3]).group_runs(parity).to_list() == [
        (1, 1),
        (2, 4),
        (3, 3),
    ]
    assert await folded.to_list() == [[1, 2, 3]]
    assert await folded.to_list() == [[1, 2, 3]]
    assert initializer_calls == 2


@pytest.mark.asyncio
async def test_async_flow_transforms_lazily_and_closes_on_short_circuit() -> None:
    closed = False

    async def source():
        nonlocal closed
        try:
            for value in range(1, 10):
                yield value
        finally:
            closed = True

    async def after_one(value: int) -> bool:
        await asyncio.sleep(0)
        return value > 1

    values = (
        fpstreams.aflow(source()).filter(after_one).flat_map(lambda value: (value, -value)).take(2)
    )

    assert await values.to_list() == [2, -2]
    assert closed
    with pytest.raises(fpstreams.FlowConsumedError):
        await values.to_list()


@pytest.mark.asyncio
async def test_async_while_operations_use_the_longest_prefix_and_close() -> None:
    closed = False
    drop_calls: list[int] = []
    take_calls: list[int] = []

    async def source():
        nonlocal closed
        try:
            for value in range(10):
                yield value
        finally:
            closed = True

    async def drop_prefix(value: int) -> bool:
        drop_calls.append(value)
        await asyncio.sleep(0)
        return value < 3

    async def take_prefix(value: int) -> bool:
        take_calls.append(value)
        await asyncio.sleep(0)
        return value < 6

    result = (
        await fpstreams.aflow(source()).drop_while(drop_prefix).take_while(take_prefix).to_list()
    )

    assert result == [3, 4, 5]
    assert drop_calls == [0, 1, 2, 3]
    assert take_calls == [3, 4, 5, 6]
    assert closed


@pytest.mark.asyncio
async def test_async_find_and_nth_short_circuit_and_accept_awaitable_predicates() -> None:
    closed = False

    async def source():
        nonlocal closed
        try:
            for value in range(10):
                yield value
        finally:
            closed = True

    async def after_two(value: int) -> bool:
        await asyncio.sleep(0)
        return value > 2

    assert await fpstreams.aflow(source()).find(after_two) == 3
    assert closed
    assert await fpstreams.aflow(range(10)).nth(4) == 4
    assert await fpstreams.aflow(range(10)).nth(-2) == 8
    assert await fpstreams.aflow([]).find(bool, "missing") == "missing"
    assert await fpstreams.aflow([]).nth(0, "missing") == "missing"
    with pytest.raises(fpstreams.EmptyFlowError, match="nth"):
        await fpstreams.aflow([1]).nth(2)


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal", ["find", "nth0", "nth_last"])
@pytest.mark.parametrize("with_default", [False, True])
async def test_async_find_and_nth_preserve_source_empty_error_and_close(terminal, with_default):
    error = fpstreams.EmptyFlowError("from source")
    closed = []

    async def source():
        try:
            yield 1
            raise error
        finally:
            closed.append(True)

    query = fpstreams.aflow(source())
    args = (lambda _: False,) if terminal == "find" else ((2,) if terminal == "nth0" else (-1,))
    if with_default:
        args += (None,)
    with pytest.raises(fpstreams.EmptyFlowError) as caught:
        await getattr(query, "find" if terminal == "find" else "nth")(*args)
    assert caught.value is error
    assert closed == [True]


@pytest.mark.asyncio
async def test_async_find_preserves_predicate_empty_error_and_none_match():
    error = fpstreams.EmptyFlowError("from predicate")

    async def predicate(_):
        raise error

    with pytest.raises(fpstreams.EmptyFlowError) as caught:
        await fpstreams.aflow([None]).find(predicate)
    assert caught.value is error
    assert await fpstreams.aflow([None]).find(lambda value: value is None) is None


@pytest.mark.asyncio
async def test_async_flow_computes_online_statistics_in_one_pass() -> None:
    assert await fpstreams.aflow([1, 2, 3, 4]).mean() == 2.5
    assert await fpstreams.aflow([1, 2, 3, 4]).average() == 2.5
    assert await fpstreams.aflow([1, 2, 3, 4]).variance() == pytest.approx(5 / 3)
    assert await fpstreams.aflow([1, 2, 3, 4]).variance(ddof=0) == 1.25
    assert await fpstreams.aflow([1, 2, 3, 4]).std() == pytest.approx((5 / 3) ** 0.5)
    assert await fpstreams.aflow([]).mean() is None
    assert await fpstreams.aflow([1]).variance() is None
    with pytest.raises(ValueError, match="ddof"):
        await fpstreams.aflow([1]).std(ddof=-1)

    closed = False

    async def invalid_source():
        nonlocal closed
        try:
            yield 1
            yield "not numeric"
        finally:
            closed = True

    with pytest.raises(TypeError, match="numeric"):
        await fpstreams.aflow(invalid_source()).mean()
    assert closed


@pytest.mark.asyncio
async def test_async_flow_streams_sum_and_extrema_with_selector_parity() -> None:
    records = [
        {"name": "first-high", "meta": {"score": 9}},
        {"name": "first-low", "meta": {"score": 2}},
        {"name": "second-high", "meta": {"score": 9}},
        {"name": "second-low", "meta": {"score": 2}},
    ]

    assert await fpstreams.aflow([1, 2, 3]).sum() == 6
    assert await fpstreams.aflow([]).sum(7) == 7
    with pytest.raises(TypeError, match="strings"):
        await fpstreams.aflow([]).sum("")

    assert await fpstreams.aflow(records).min(key="meta.score") is records[1]
    assert await fpstreams.aflow(records).max(key="meta.score") is records[0]
    assert await fpstreams.aflow(records).minmax(key="meta.score") == (records[1], records[0])

    async def negative_score(record: dict[str, object]) -> int:
        await asyncio.sleep(0)
        return -int(record["meta"]["score"])  # type: ignore[index]

    assert await fpstreams.aflow(records).min(key=negative_score) is records[0]
    assert await fpstreams.aflow(records).max(key=negative_score) is records[1]

    for terminal in ("min", "max", "minmax"):
        with pytest.raises(fpstreams.EmptyFlowError, match=terminal):
            await getattr(fpstreams.aflow([]), terminal)()


@pytest.mark.asyncio
async def test_async_extrema_close_the_source_on_key_failure_and_cancellation() -> None:
    failure_closed = False

    async def failing_source():
        nonlocal failure_closed
        try:
            yield {"value": 1}
            yield {"value": 2}
        finally:
            failure_closed = True

    async def fail_on_second(record: dict[str, int]) -> int:
        if record["value"] == 2:
            raise RuntimeError("key failed")
        return record["value"]

    with pytest.raises(RuntimeError, match="key failed"):
        await fpstreams.aflow(failing_source()).minmax(key=fail_on_second)
    assert failure_closed

    cancellation_closed = asyncio.Event()
    key_started = asyncio.Event()

    async def cancellable_source():
        try:
            yield {"value": 1}
        finally:
            cancellation_closed.set()

    async def blocked_key(record: dict[str, int]) -> int:
        key_started.set()
        await asyncio.Event().wait()
        return record["value"]

    task = asyncio.create_task(fpstreams.aflow(cancellable_source()).max(key=blocked_key))
    await key_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert cancellation_closed.is_set()


def test_async_extrema_terminal_explanations_require_source_completion() -> None:
    for terminal in ("sum", "min", "max", "minmax"):
        explanation = fpstreams.aflow([3, 1, 2]).explain(terminal).to_dict()
        assert explanation["terminal"] == terminal
        assert explanation["semantics"]["completion"] == "source_end"


@pytest.mark.asyncio
async def test_async_flow_supports_completion_order_timeouts_and_terminals() -> None:
    async def finish_in_value_order(value: int) -> int:
        await asyncio.sleep(value * 0.002)
        return value

    completed = (
        await fpstreams.aflow([3, 1, 2])
        .map_async(finish_in_value_order, concurrency=3, ordered=False)
        .to_list()
    )
    assert completed == [1, 2, 3]

    values = fpstreams.aflow([1, 2, 3])
    assert await values.first() == 1
    assert await values.last() == 3
    assert await values.count() == 3
    assert await values.reduce(lambda left, right: left + right) == 6

    async def too_slow(value: int) -> int:
        await asyncio.sleep(1)
        return value

    with pytest.raises(TimeoutError):
        await fpstreams.aflow([1]).map_async(too_slow, timeout=0.001).to_list()


@pytest.mark.asyncio
async def test_async_short_circuit_terminal_closes_upstream_immediately() -> None:
    closed = False

    async def source():
        nonlocal closed
        try:
            for value in range(100):
                yield value
        finally:
            closed = True

    assert await fpstreams.aflow(source()).any(lambda value: value == 2)
    assert closed


@pytest.mark.asyncio
async def test_async_concurrent_short_circuit_cancels_pending_work() -> None:
    active: set[int] = set()
    cancelled: set[int] = set()

    async def work(value: int) -> int:
        active.add(value)
        try:
            await asyncio.sleep(0 if value == 0 else 10)
            return value
        except asyncio.CancelledError:
            cancelled.add(value)
            raise
        finally:
            active.remove(value)

    result = await fpstreams.aflow(range(10)).map_async(work, concurrency=4).take(1).to_list()

    assert result == [0]
    assert active == set()
    assert cancelled == {1, 2, 3}


@pytest.mark.asyncio
async def test_async_file_and_interval_sources_are_reusable(tmp_path) -> None:
    path = tmp_path / "values.txt"
    path.write_text("alpha\nbeta\n", encoding="utf-8")

    lines = fpstreams.AsyncFlow.from_file(path)
    assert await lines.to_list() == ["alpha", "beta"]
    assert await lines.to_list() == ["alpha", "beta"]
    assert await fpstreams.AsyncFlow.interval(0).take(3).to_list() == [0, 1, 2]


@pytest.mark.asyncio
@pytest.mark.parametrize("ending", ["read-error", "cancel", "exhausted", "take"])
async def test_async_file_cleanup_preserves_read_error_and_cancel(ending, monkeypatch) -> None:
    """The real aiofiles context must retain primary errors and report lone close errors."""
    import io

    import aiofiles.threadpool
    from aiofiles.threadpool.text import AsyncTextIOWrapper

    primary = ValueError("read failed")
    cleanup = OSError("file close failed")
    events = []
    started = asyncio.Event()
    resume = asyncio.Event()

    class Handle(io.StringIO):
        def readline(self, *args):
            if ending == "read-error":
                raise primary
            return super().readline(*args)

        def close(self):
            super().close()
            events.append("close")
            raise cleanup

    handle = Handle("alpha\nbeta\n")
    opens = []

    def open_handle(*args, **kwargs):
        opens.append((args, kwargs))
        return handle

    monkeypatch.setattr(aiofiles.threadpool, "sync_open", open_handle)
    if ending == "cancel":

        async def pending_readline(self):
            started.set()
            await resume.wait()
            return ""

        monkeypatch.setattr(AsyncTextIOWrapper, "readline", pending_readline)

    values = fpstreams.AsyncFlow.from_file("owned.txt")
    assert opens == []
    if ending == "cancel":
        task = asyncio.create_task(values.to_list())
        try:
            await asyncio.wait_for(started.wait(), timeout=10)
            task.cancel("caller cancelled")
            resume.set()
            with pytest.raises(asyncio.CancelledError) as caught:
                await task
            assert task.cancelled()
            assert caught.value.args == ("caller cancelled",)
        finally:
            resume.set()
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    else:
        expected = ValueError if ending == "read-error" else OSError
        with pytest.raises(expected) as caught:
            await (values.take(1) if ending == "take" else values).to_list()
        assert caught.value is (primary if ending == "read-error" else cleanup)
    if ending in {"read-error", "cancel"}:
        assert caught.value.__notes__ == ["cleanup failed with OSError: file close failed"]
    assert handle.closed
    assert events == ["close"]
    assert len(opens) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["open", "read", "close"])
@pytest.mark.parametrize("operation_fails", [False, True])
async def test_async_file_cancel_drains_inflight_thread_operation(
    phase, operation_fails, monkeypatch
) -> None:
    """Cancellation must retire actual executor work before returning or closing its file."""
    import io
    import threading

    import aiofiles.base
    import aiofiles.threadpool

    loop = asyncio.get_running_loop()
    started = asyncio.Event()
    release = threading.Event()
    events = []
    cleanup = OSError(f"{phase} operation failed")
    exit_started = []

    def gated_operation(name):
        if name != phase:
            return
        events.append(f"{name}:start")
        loop.call_soon_threadsafe(started.set)
        if not release.wait(timeout=10):
            raise TimeoutError("test did not release the file operation")
        events.append(f"{name}:end")
        if operation_fails:
            raise cleanup

    class Handle(io.StringIO):
        def readline(self, *args):
            gated_operation("read")
            return super().readline(*args)

        def close(self):
            try:
                gated_operation("close")
            finally:
                super().close()
                events.append("closed")

    handles = []

    def open_handle(*args, **kwargs):
        gated_operation("open")
        handle = Handle("alpha\n")
        handles.append(handle)
        return handle

    original_exit = aiofiles.base.AiofilesContextManager.__aexit__

    async def recorded_exit(self, *args):
        exit_started.append(True)
        return await original_exit(self, *args)

    monkeypatch.setattr(aiofiles.threadpool, "sync_open", open_handle)
    monkeypatch.setattr(aiofiles.base.AiofilesContextManager, "__aexit__", recorded_exit)
    task = asyncio.create_task(fpstreams.aflow.from_file("owned.txt").take(1).to_list())
    try:
        await asyncio.wait_for(started.wait(), timeout=10)
        task.cancel("first cancellation")
        await asyncio.sleep(0)  # Deliver cancellation while the worker is held at the gate.
        task.cancel("second cancellation")
        await asyncio.sleep(0)
        assert not task.done(), "caller returned while its file operation was still running"
        if phase != "close":
            assert exit_started == [], "close began before the in-flight operation finished"
        release.set()
        with pytest.raises(asyncio.CancelledError) as caught:
            await asyncio.wait_for(task, timeout=10)
        assert task.cancelled()
        assert caught.value.args == ("first cancellation",)
        if operation_fails:
            assert any(f"{phase} operation failed" in note for note in caught.value.__notes__)
        if phase == "open" and operation_fails:
            assert handles == []
            assert exit_started == []
        else:
            assert len(handles) == 1 and handles[0].closed
            assert events.count("closed") == 1
            assert exit_started == [True]
        assert events.index(f"{phase}:start") < events.index(f"{phase}:end")
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_async_cursor_pagination_is_lazy_and_reusable() -> None:
    requested: list[str | None] = []
    pages = {None: ([1, 2], "next"), "next": ([3, 4], None)}

    async def fetch(cursor: str | None):
        requested.append(cursor)
        return pages[cursor]

    values = fpstreams.aflow.paginate(fetch)

    assert await values.take(3).to_list() == [1, 2, 3]
    assert requested == [None, "next"]
    assert await values.to_list() == [1, 2, 3, 4]


@pytest.mark.asyncio
async def test_async_cursor_pagination_closes_the_active_page_on_short_circuit() -> None:
    class Page(AsyncIterator[int]):
        def __init__(self) -> None:
            self._values = iter((1, 2))
            self.closed = 0

        async def __anext__(self) -> int:
            try:
                return next(self._values)
            except StopIteration:
                raise StopAsyncIteration from None

        async def aclose(self) -> None:
            self.closed += 1

    active_page = Page()

    async def fetch(_cursor: None):
        return active_page, None

    assert await fpstreams.aflow.paginate(fetch).take(1).to_list() == [1]
    assert active_page.closed == 1


@pytest.mark.asyncio
async def test_async_merge_uses_completion_order_and_one_pull_per_source() -> None:
    closed: set[str] = set()
    pulled = {"slow": 0, "fast": 0}
    started = {name: [asyncio.Event(), asyncio.Event()] for name in pulled}
    release = {name: [asyncio.Event(), asyncio.Event()] for name in pulled}
    consumed: asyncio.Queue[str] = asyncio.Queue()
    result: list[str] = []

    async def values(name: str) -> AsyncIterator[str]:
        try:
            for position in range(2):
                pulled[name] += 1
                started[name][position].set()
                await release[name][position].wait()
                yield f"{name}-{position}"
        finally:
            closed.add(name)

    async def consume() -> None:
        async for value in fpstreams.aflow(values("slow")).merge(values("fast")):
            result.append(value)
            consumed.put_nowait(value)

    async with asyncio.TaskGroup() as tasks:
        tasks.create_task(consume())
        await asyncio.gather(started["slow"][0].wait(), started["fast"][0].wait())
        for name, position in [("fast", 0), ("fast", 1), ("slow", 0), ("slow", 1)]:
            await started[name][position].wait()
            release[name][position].set()
            assert await consumed.get() == f"{name}-{position}"

    assert result == ["fast-0", "fast-1", "slow-0", "slow-1"]
    assert pulled == {"slow": 2, "fast": 2}
    assert closed == {"slow", "fast"}

    gate = asyncio.Event()
    buffered = {"left": 0, "right": 0}
    buffer_started = {name: asyncio.Event() for name in buffered}

    async def blocked(name: str) -> AsyncIterator[str]:
        try:
            while True:
                buffered[name] += 1
                buffer_started[name].set()
                yield name
                await gate.wait()
        finally:
            closed.add(f"blocked-{name}")

    iterator = fpstreams.aflow(blocked("left")).merge(blocked("right")).__aiter__()
    await anext(iterator)
    await asyncio.gather(*(event.wait() for event in buffer_started.values()))
    assert buffered == {"left": 1, "right": 1}
    await iterator.aclose()
    assert {"blocked-left", "blocked-right"} <= closed


@pytest.mark.asyncio
async def test_async_merge_propagates_errors_and_cancels_siblings() -> None:
    sibling_cancelled = sibling_closed = False

    async def failing() -> AsyncIterator[int]:
        await asyncio.sleep(0)
        raise ValueError("merge failed")
        yield 0

    async def sibling() -> AsyncIterator[int]:
        nonlocal sibling_cancelled, sibling_closed
        try:
            while True:
                try:
                    await asyncio.sleep(10)
                except asyncio.CancelledError:
                    sibling_cancelled = True
                    raise
                yield 1
        finally:
            sibling_closed = True

    with pytest.raises(ValueError, match="merge failed"):
        await fpstreams.aflow(failing()).merge(sibling()).to_list()

    assert sibling_cancelled and sibling_closed


@pytest.mark.asyncio
async def test_async_merge_short_circuit_closes_all_sources() -> None:
    closed: set[int] = set()

    async def source(identity: int) -> AsyncIterator[tuple[int, int]]:
        try:
            for value in range(100):
                yield identity, value
                await asyncio.sleep(10)
        finally:
            closed.add(identity)

    result = await fpstreams.aflow(source(1)).merge(source(2), source(3)).take(1).to_list()

    assert len(result) == 1
    assert closed == {1, 2, 3}


@pytest.mark.asyncio
async def test_async_merge_second_cancellation_still_closes_all_sources() -> None:
    class Source:
        def __init__(self) -> None:
            self.started = asyncio.Event()
            self.cancelled = asyncio.Event()
            self.release = asyncio.Event()
            self.close_calls = 0

        def __aiter__(self) -> Source:
            return self

        async def __anext__(self) -> int:
            self.started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.cancelled.set()
                await self.release.wait()
                raise
            raise AssertionError("unreachable")

        async def aclose(self) -> None:
            self.close_calls += 1

    left, right = Source(), Source()
    task = asyncio.create_task(fpstreams.aflow(left).merge(right).to_list())
    try:
        await asyncio.wait_for(
            asyncio.gather(left.started.wait(), right.started.wait()),
            timeout=1,
        )
        task.cancel()
        await asyncio.wait_for(
            asyncio.gather(left.cancelled.wait(), right.cancelled.wait()),
            timeout=1,
        )
        task.cancel()
        left.release.set()
        right.release.set()

        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=1)
    finally:
        left.release.set()
        right.release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert left.close_calls == right.close_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["merge", "combine_latest"])
async def test_async_multi_source_close_failure_does_not_close_a_source_twice(
    operation: str,
) -> None:
    class EmptySource(AsyncIterator[int]):
        def __init__(self, *, fail_close: bool = False) -> None:
            self.fail_close = fail_close
            self.close_calls = 0

        async def __anext__(self) -> int:
            raise StopAsyncIteration

        async def aclose(self) -> None:
            self.close_calls += 1
            if self.fail_close:
                raise OSError("left close failed")

    left = EmptySource(fail_close=True)
    right = EmptySource()
    values = fpstreams.aflow(left)
    values = values.merge(right) if operation == "merge" else values.combine_latest(right)

    with pytest.raises(OSError, match="left close failed") as captured:
        await values.to_list()

    assert getattr(captured.value, "__notes__", ()) == ()
    assert left.close_calls == right.close_calls == 1


@pytest.mark.asyncio
async def test_async_merge_map_merges_inners_with_a_hard_concurrency_limit() -> None:
    active = maximum = 0
    started = {value: asyncio.Event() for value in (1, 2)}
    release = {value: [asyncio.Event(), asyncio.Event()] for value in started}
    consumed: asyncio.Queue[int] = asyncio.Queue()
    result: list[int] = []

    async def mapper(value: int) -> AsyncIterator[int]:
        await asyncio.sleep(0)

        async def nested() -> AsyncIterator[int]:
            nonlocal active, maximum
            active += 1
            maximum = max(maximum, active)
            started[value].set()
            try:
                await release[value][0].wait()
                yield value
                await release[value][1].wait()
                yield -value
            finally:
                active -= 1

        return nested()

    async def consume(values: fpstreams.AsyncFlow[int]) -> None:
        async for value in values:
            result.append(value)
            consumed.put_nowait(value)

    async with asyncio.TaskGroup() as tasks:
        tasks.create_task(consume(fpstreams.aflow([1, 2]).merge_map(mapper, concurrency=2)))
        await asyncio.gather(*(event.wait() for event in started.values()))
        for value in (2, 1):
            for position, expected in enumerate((value, -value)):
                release[value][position].set()
                assert await consumed.get() == expected

    assert result == [2, -2, 1, -1]
    assert maximum == 2
    assert active == 0

    maximum = 0
    result = []
    started = {value: asyncio.Event() for value in (1, 2)}
    release = {value: [asyncio.Event(), asyncio.Event()] for value in started}
    async with asyncio.TaskGroup() as tasks:
        tasks.create_task(consume(fpstreams.aflow([1, 2]).flat_map_merge(mapper, concurrency=1)))
        for value in (1, 2):
            await started[value].wait()
            for position, expected in enumerate((value, -value)):
                release[value][position].set()
                assert await consumed.get() == expected

    assert result == [1, -1, 2, -2]
    assert maximum == 1
    assert active == 0


@pytest.mark.asyncio
async def test_async_merge_map_backpressures_outer_and_cleans_up_on_take() -> None:
    release = asyncio.Event()
    all_started = asyncio.Event()
    outer_pulled: list[int] = []
    inner_started: set[int] = set()
    inner_closed: set[int] = set()
    outer_closed = False

    async def outer() -> AsyncIterator[int]:
        nonlocal outer_closed
        try:
            for value in range(100):
                outer_pulled.append(value)
                yield value
        finally:
            outer_closed = True

    def mapper(value: int) -> AsyncIterator[int]:
        async def nested() -> AsyncIterator[int]:
            try:
                inner_started.add(value)
                if len(inner_started) == 3:
                    all_started.set()
                await release.wait()
                yield value
            finally:
                inner_closed.add(value)

        return nested()

    iterator = fpstreams.aflow(outer()).merge_map(mapper, concurrency=3).__aiter__()
    try:
        async with asyncio.TaskGroup() as tasks:
            first = tasks.create_task(anext(iterator))
            await all_started.wait()
            assert outer_pulled == [0, 1, 2]
            assert inner_started == {0, 1, 2}

            release.set()
            assert await first in {0, 1, 2}
    finally:
        await iterator.aclose()

    assert outer_closed
    assert inner_closed == {0, 1, 2}


@pytest.mark.asyncio
@pytest.mark.parametrize("failing_position", [0, 1])
async def test_async_merge_map_prioritizes_a_simultaneous_inner_failure(
    failing_position: int,
) -> None:
    gate = asyncio.Event()
    started = 0
    primary = RuntimeError("inner pull failed")

    class Inner(AsyncIterator[int]):
        def __init__(self, position: int) -> None:
            self.position = position
            self.close_calls = 0

        async def __anext__(self) -> int:
            nonlocal started
            started += 1
            if started == 2:
                gate.set()
            await gate.wait()
            if self.position == failing_position:
                raise primary
            raise StopAsyncIteration

        async def aclose(self) -> None:
            self.close_calls += 1
            if self.position != failing_position:
                raise OSError("empty inner close failed")

    inners = [Inner(0), Inner(1)]
    with pytest.raises(RuntimeError) as captured:
        await fpstreams.aflow([0, 1]).merge_map(inners.__getitem__, concurrency=2).to_list()

    assert captured.value is primary
    assert captured.value.__notes__ == ["cleanup failed: OSError: empty inner close failed"]
    assert [inner.close_calls for inner in inners] == [1, 1]


@pytest.mark.asyncio
@pytest.mark.parametrize("operator", ["merge_map", "switch_map"])
@pytest.mark.parametrize("terminal", ["full", "first"])
@pytest.mark.parametrize(
    "hook_kind", ["absent", "none", "not-callable", "sync", "sync-value", "async"]
)
async def test_async_inner_cleanup_continues_with_optional_close_hooks(
    operator, terminal, hook_kind
) -> None:
    """Mapped inners accept the same optional close protocol as serial flat_map."""

    async def evaluate(method):
        closed = []

        class Inner:
            def __init__(self):
                self.values = iter((4, 6))
                self.pulls = 0

            def __aiter__(self):
                return self

            async def __anext__(self):
                self.pulls += 1
                try:
                    return next(self.values)
                except StopIteration:
                    raise StopAsyncIteration from None

        def sync_close():
            closed.append(True)
            return "ignored" if hook_kind == "sync-value" else None

        async def async_close():
            closed.append(True)

        inner = Inner()
        if hook_kind != "absent":
            inner.aclose = {
                "none": None,
                "not-callable": 42,
                "sync": sync_close,
                "sync-value": sync_close,
                "async": async_close,
            }[hook_kind]
        values = getattr(fpstreams.aflow([0]), method)(lambda _value: inner)
        if terminal == "first":
            values = values.take(1)
        return await values.to_list(), inner.pulls, closed

    expected = (
        [4] if terminal == "first" else [4, 6],
        1 if terminal == "first" else 3,
        [True] if hook_kind in {"sync", "sync-value", "async"} else [],
    )
    assert await evaluate("flat_map") == expected
    assert await evaluate(operator) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("operator", ["merge_map", "switch_map"])
@pytest.mark.parametrize("hook_kind", ["none", "sync", "async"])
async def test_async_inner_cleanup_continues_with_acquisition_hook_snapshot(
    operator, hook_kind
) -> None:
    """An inner callback cannot replace the hook selected when it was acquired."""
    closed = []
    reads = []

    def sync_close():
        closed.append(True)

    async def async_close():
        closed.append(True)

    async def replacement():
        raise AssertionError("the replacement hook was not selected at acquisition")

    selected = [{"none": None, "sync": sync_close, "async": async_close}[hook_kind]]

    class Inner:
        emitted = False

        def __aiter__(self):
            return self

        async def __anext__(self):
            selected[0] = replacement
            if self.emitted:
                raise StopAsyncIteration
            self.emitted = True
            return 7

        @property
        def aclose(self):
            reads.append(True)
            return selected[0]

    inner = Inner()
    assert await getattr(fpstreams.aflow([0]), operator)(lambda _value: inner).to_list() == [7]
    assert reads == [True]
    assert closed == ([] if hook_kind == "none" else [True])


@pytest.mark.asyncio
@pytest.mark.parametrize("operator", ["merge_map", "switch_map"])
@pytest.mark.parametrize("pull_fails", [False, True])
async def test_async_inner_cleanup_continues_preserving_sync_close_errors(
    operator, pull_fails
) -> None:
    primary = RuntimeError("inner pull failed")
    cleanup = OSError("synchronous inner close failed")
    closed = []

    class Inner:
        emitted = False

        def __aiter__(self):
            return self

        async def __anext__(self):
            if pull_fails:
                raise primary
            if self.emitted:
                raise StopAsyncIteration
            self.emitted = True
            return 7

        def aclose(self):
            closed.append(True)
            raise cleanup

    inner = Inner()
    expected = primary if pull_fails else cleanup
    with pytest.raises(type(expected)) as caught:
        await getattr(fpstreams.aflow([0]), operator)(lambda _value: inner).to_list()
    assert caught.value is expected
    assert closed == [True]
    if pull_fails:
        assert primary.__notes__ == ["cleanup failed: OSError: synchronous inner close failed"]


@pytest.mark.parametrize(
    "concurrency",
    [float("nan"), float("inf"), 1.5, 2.0],
    ids=["nan", "infinity", "fractional", "integral-float"],
)
def test_async_merge_map_rejects_noninteger_concurrency_before_source_open(concurrency) -> None:
    opened = []
    mapped = []

    def source():
        opened.append(True)
        return [1, 2]

    def mapper(value):
        mapped.append(value)
        return [value]

    values = fpstreams.aflow.defer(source)
    with pytest.raises(TypeError):
        values.merge_map(mapper, concurrency=concurrency)
    assert opened == mapped == []


@pytest.mark.asyncio
async def test_async_merge_map_normalizes_concurrency_before_source_open() -> None:
    import weakref

    calls = []
    opened = []

    class Capacity:
        value = 2

        def __index__(self):
            calls.append(True)
            return self.value

        def __lt__(self, other):
            return self.value < other

        def __gt__(self, other):
            return self.value > other

    def source():
        opened.append(True)
        return [1, 2, 3]

    concurrency = Capacity()
    reference = weakref.ref(concurrency)
    values = fpstreams.aflow.defer(source).merge_map(
        lambda value: [value * 2], concurrency=concurrency
    )
    assert opened == []
    concurrency.value = 0
    del concurrency
    result = await values.to_list()
    assert (sorted(result), calls, reference() is None) == ([2, 4, 6], [True], True)
    assert opened == [True]


def test_async_merge_map_validates_concurrency() -> None:
    with pytest.raises(ValueError, match="at least 1"):
        fpstreams.aflow([1]).merge_map(lambda value: [value], concurrency=0)


@pytest.mark.asyncio
async def test_async_selector_bookkeeping_composes_without_manual_generators() -> None:
    async def add(total: int, value: int) -> int:
        await asyncio.sleep(0)
        return total + value

    result = (
        await fpstreams.aflow(
            [{"value": 1}, {"value": None}, {"value": 2}, {"value": 2}, {"value": 0}]
        )
        .pluck("value")
        .filter_none()
        .compact()
        .unique()
        .scan(10, add)
        .enumerate(5)
        .intersperse((-1, -1))
        .to_list()
    )

    assert result == [(5, 11), (-1, -1), (6, 13)]
    assert await fpstreams.aflow([{"key": [1]}, {"key": [1]}, {"key": [2]}]).unique_by("key").pluck(
        "key"
    ).to_list() == [[1], [2]]


@pytest.mark.asyncio
async def test_async_zip_variants_close_every_source_and_enforce_strict_lengths() -> None:
    left_closed = right_closed = False

    async def left() -> AsyncIterator[int]:
        nonlocal left_closed
        try:
            for value in range(10):
                yield value
        finally:
            left_closed = True

    async def right() -> AsyncIterator[str]:
        nonlocal right_closed
        try:
            for value in ("a", "b", "c"):
                yield value
        finally:
            right_closed = True

    assert await fpstreams.aflow(left()).zip(right()).take(1).to_list() == [(0, "a")]
    assert left_closed and right_closed
    assert await fpstreams.aflow([1]).zip_longest([2, 3], fillvalue=None).to_list() == [
        (1, 2),
        (None, 3),
    ]

    with pytest.raises(ValueError, match="shorter"):
        await fpstreams.aflow([1, 2]).zip([3], strict=True).to_list()
    with pytest.raises(ValueError, match="longer"):
        await fpstreams.aflow([1]).zip([2, 3], strict=True).to_list()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode", ["left", "right", "alias", "mapped-alias", "repeated", "cross", "mapped-cross"]
)
async def test_async_concat_closes_owned_sources_once(mode: str) -> None:
    class Source:
        def __init__(self, values):
            self.values = iter(values)
            self.calls = 0

        def __aiter__(self):
            return self

        async def __anext__(self):
            try:
                return next(self.values)
            except StopIteration:
                raise StopAsyncIteration from None

        async def aclose(self):
            self.calls += 1
            if self.calls > 1:
                raise RuntimeError("closed twice")

    left = Source([] if mode in {"right", "repeated", "cross", "mapped-cross"} else [1, 2])
    right = left if "alias" in mode else Source([3, 4])
    values = fpstreams.aflow(left)
    if mode == "mapped-alias":
        values = values.map(lambda value: value)
    query = values.concat(right, right) if mode == "repeated" else values.concat(right)
    if mode == "mapped-cross":
        query = query.map(lambda value: value)
    if mode in {"cross", "mapped-cross"}:
        query = query.concat(right)
    result = await (
        query.to_list()
        if "alias" in mode or mode in {"repeated", "cross", "mapped-cross"}
        else query.take(1).to_list()
    )
    expected = (
        [1, 2]
        if "alias" in mode
        else [3, 4]
        if mode in {"repeated", "cross", "mapped-cross"}
        else [3]
        if mode == "right"
        else [1]
    )
    assert result == expected
    assert left.calls == 1
    assert right.calls == (0 if mode == "left" else 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["alias", "mapped-alias", "repeated", "cross", "mapped-cross"])
async def test_async_concat_does_not_close_aliased_sync_source_twice(mode: str) -> None:
    class Source:
        def __init__(self):
            self.values = iter([1, 2])
            self.calls = 0

        def __iter__(self):
            return self

        def __next__(self):
            return next(self.values)

        def close(self):
            self.calls += 1
            if self.calls > 1:
                raise RuntimeError("closed twice")

    source = Source()
    values = fpstreams.aflow([] if mode in {"repeated", "cross", "mapped-cross"} else source)
    if mode == "mapped-alias":
        values = values.map(lambda value: value)
    query = values.concat(source, source) if mode == "repeated" else values.concat(source)
    if mode == "mapped-cross":
        query = query.map(lambda value: value)
    if mode in {"cross", "mapped-cross"}:
        query = query.concat(source)
    assert await query.to_list() == [1, 2]
    assert source.calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("adapted", [False, True])
@pytest.mark.parametrize("mapped", [False, True])
@pytest.mark.parametrize("close_fails", [False, True])
async def test_async_concat_short_circuit_reused_source_closes_once(
    adapted: bool, mapped: bool, close_fails: bool
) -> None:
    cleanup = OSError("close failed")

    class Source:
        def __init__(self):
            self.values = iter([1, 2])
            self.calls = 0
            self.pulls = 0

        def __iter__(self):
            return self

        def __next__(self):
            self.pulls += 1
            return next(self.values)

        def close(self):
            self.calls += 1
            if close_fails:
                raise cleanup
            if self.calls > 1:
                raise RuntimeError("closed twice")

    class AsyncSource(Source):
        def __aiter__(self):
            return self

        async def __anext__(self):
            try:
                self.pulls += 1
                return next(self.values)
            except StopIteration:
                raise StopAsyncIteration from None

        async def aclose(self):
            self.close()

    source = Source() if adapted else AsyncSource()
    query = fpstreams.aflow([]).concat(source).take(1)
    if mapped:
        query = query.map(lambda value: value)
    query = query.concat(source)
    if close_fails:
        with pytest.raises(OSError) as captured:
            await query.to_list()
        assert captured.value is cleanup
        assert source.pulls == 1
    else:
        assert await query.to_list() == [1, 2]
    assert source.calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["concat", "append"])
async def test_async_root_cleanup_continues_without_retaining_closed_source(operation) -> None:
    import gc
    import weakref

    references = []
    closed = []

    class Source:
        def __init__(self):
            self.emitted = False

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self.emitted:
                raise StopAsyncIteration
            self.emitted = True
            return 1

        async def aclose(self):
            closed.append(True)

    def factory():
        source = Source()
        references.append(weakref.ref(source))
        return source

    query = fpstreams.aflow.defer(factory).map(lambda value: value)
    query = query.concat([2]) if operation == "concat" else query.append(2)
    iterator = query.__aiter__()
    try:
        assert await anext(iterator) == 1
        assert await anext(iterator) == 2
        assert closed == [True]
        gc.collect()
        assert references[0]() is None
    finally:
        await iterator.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("reverse", [False, True])
async def test_async_distinct_handles_equal_mixed_hashability(reverse: bool) -> None:
    first, second = ({1}, frozenset({1})) if not reverse else (frozenset({1}), {1})
    result = await fpstreams.aflow([first, second, {2}]).distinct().to_list()
    assert len(result) == 2 and result[0] is first and result[1] == {2}
    pairs = [(first, "first"), (second, "second")]
    result_by = await fpstreams.aflow(pairs).distinct_by(lambda item: item[0]).to_list()
    assert result_by == [pairs[0]]

    first_nan, second_nan = float("nan"), float("nan")
    result = (
        await fpstreams.aflow([first, second, first_nan, first_nan, second_nan, True, 1, 1.0])
        .distinct()
        .to_list()
    )
    assert len(result) == 4
    assert result[0] is first and result[1] is first_nan and result[2] is second_nan
    assert result[3] is True


@pytest.mark.asyncio
async def test_async_distinct_mixed_comparison_error_closes_source() -> None:
    failure = ValueError("mixed comparison failed")
    closed = []

    class Unhashable:
        __hash__ = None

        def __eq__(self, other):
            raise failure

    async def source():
        try:
            yield Unhashable()
            yield 1
        finally:
            closed.append(True)

    with pytest.raises(ValueError) as caught:
        await fpstreams.aflow(source()).distinct().to_list()
    assert caught.value is failure
    assert closed == [True]


@pytest.mark.asyncio
async def test_async_concat_preserves_source_and_close_errors() -> None:
    failure = ValueError("source failed")
    cleanup = OSError("close failed")

    class Source:
        def __aiter__(self):
            return self

        async def __anext__(self):
            raise failure

        async def aclose(self):
            raise cleanup

    with pytest.raises(ValueError) as captured:
        await fpstreams.aflow([]).concat(Source()).to_list()
    assert captured.value is failure
    assert any("close failed" in note for note in failure.__notes__)

    class CloseFails(Source):
        async def __anext__(self):
            return 1

    with pytest.raises(OSError) as captured:
        await fpstreams.aflow([]).concat(CloseFails()).take(1).to_list()
    assert captured.value is cleanup


def test_async_source_replaced_opener_does_not_retain_one_shot_value() -> None:
    import gc
    import weakref

    class Source:
        def __aiter__(self):
            return self

        async def __anext__(self):
            raise StopAsyncIteration

    source = Source()
    reference = weakref.ref(source)
    values = fpstreams.aflow(source)
    values._logical_plan.source._opener = lambda: fpstreams.aflow([])._logical_plan.source.open()
    del source
    gc.collect()
    assert reference() is None


@pytest.mark.asyncio
async def test_async_concat_collapse_and_end_transforms_accept_awaitables() -> None:
    other_closed = False

    async def other() -> AsyncIterator[int]:
        nonlocal other_closed
        try:
            yield 7
            yield 8
        finally:
            other_closed = True

    async def same(left: int, right: int) -> bool:
        await asyncio.sleep(0)
        return left == right

    async def merge(left: int, right: int) -> int:
        await asyncio.sleep(0)
        return left + right

    async def times_ten(value: int) -> int:
        await asyncio.sleep(0)
        return value * 10

    result = (
        await fpstreams.aflow([1, 1, 2, 3, 3])
        .collapse(same, merge)
        .prepend(0)
        .concat(other())
        .append(9)
        .map_first(times_ten)
        .map_last(times_ten)
        .to_list()
    )

    assert result == [0, 2, 2, 6, 7, 8, 90]
    assert other_closed
    assert await fpstreams.aflow([]).map_first(times_ten).map_last(times_ten).to_list() == []


async def test_async_collapse_collectors_preserve_adjacent_inputs_and_replay():
    values = [1, 2, 4, 9, 11, 12, 20, 21, 1]
    expected = [[1, 2, 4], [9, 11, 12], [20, 21], [1]]
    pairs = []

    async def adjacent(left, right):
        await asyncio.sleep(0)
        pairs.append((left, right))
        return 0 <= right - left <= 2

    query = fpstreams.aflow(values).collapse(adjacent, fpstreams.Collectors.to_list())
    assert await query.to_list() == expected
    assert pairs == list(pairwise(values))
    pairs.clear()
    assert await query.to_list() == expected
    assert pairs == list(pairwise(values))
    assert await fpstreams.aflow(values).collapse(
        adjacent, fpstreams.Collectors.head(0)
    ).to_list() == [[] for _ in expected]
    initialized = []
    nothing = fpstreams.Collector(lambda: initialized.append(True), lambda state, value: None)
    assert await fpstreams.aflow([]).collapse(adjacent, nothing).to_list() == []
    assert initialized == []
    assert await fpstreams.aflow([None, None, 0, None]).collapse(
        lambda a, b: a == b, fpstreams.Collectors.first()
    ).to_list() == [None, 0, None]


@pytest.mark.parametrize("pending", ("source", "predicate"))
async def test_async_collapse_collector_cancellation_does_not_finish_pending_run(pending):
    waiting = asyncio.Event()
    closed, finished, emitted = [], [], []

    async def source():
        try:
            for value in (1, 2, 10):
                yield value
            waiting.set()
            await asyncio.Event().wait()
        finally:
            closed.append(True)

    async def adjacent(left, right):
        if pending == "predicate" and right == 10:
            waiting.set()
            await asyncio.Event().wait()
        return left // 10 == right // 10

    def finish(state):
        finished.append(tuple(state))
        return tuple(state)

    collector = fpstreams.Collector(list, lambda state, value: [*state, value], finish)
    task = asyncio.create_task(
        fpstreams.aflow(source()).collapse(adjacent, collector).tap(emitted.append).to_list()
    )
    try:
        await asyncio.wait_for(waiting.wait(), timeout=2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert closed == [True]
        assert finished == emitted == ([(1, 2)] if pending == "source" else [])
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("hook", ("predicate", "finish", "source"))
async def test_async_collapse_collector_failures_preserve_primary_error(hook):
    failure = LookupError(hook)
    cleanup = RuntimeError("async collapse cleanup failed")
    emitted, finished = [], []

    class Source:
        def __init__(self):
            self.values = iter((1, 2, 10))
            self.closes = 0

        def __aiter__(self):
            return self

        async def __anext__(self):
            try:
                return next(self.values)
            except StopIteration:
                if hook == "source":
                    raise failure from None
                raise StopAsyncIteration from None

        async def aclose(self):
            self.closes += 1
            raise cleanup

    async def adjacent(left, right):
        await asyncio.sleep(0)
        if hook == "predicate" and right == 10:
            raise failure
        return left // 10 == right // 10

    def finish(state):
        finished.append(tuple(state))
        if hook == "finish":
            raise failure
        return tuple(state)

    source = Source()
    collector = fpstreams.Collector(list, lambda state, value: [*state, value], finish)
    with pytest.raises(LookupError) as caught:
        await fpstreams.aflow(source).collapse(adjacent, collector).tap(emitted.append).to_list()
    assert caught.value is failure and source.closes == 1
    assert any("async collapse cleanup failed" in note for note in failure.__notes__)
    assert emitted == ([(1, 2)] if hook == "source" else [])
    assert finished == ([(1, 2)] if hook in ("finish", "source") else [])


async def test_collapse_collector_awaitable_results_remain_data_on_both_paths():
    awaited = []

    async def finish(state):
        awaited.append(tuple(state))
        return tuple(state)

    collector = fpstreams.Collector(list, lambda state, value: [*state, value], finish)
    for mode in ("sync", "async"):
        query = fpstreams.flow([1, 1, 2]) if mode == "sync" else fpstreams.aflow([1, 1, 2])
        query = query.collapse(lambda a, b: a == b, collector)
        results = query.to_list() if mode == "sync" else await query.to_list()
        assert awaited == []
        try:
            assert [await result for result in results] == [(1, 1), (2,)]
            assert awaited == [(1, 1), (2,)]
        finally:
            for result in results:
                result.close()
        awaited.clear()


async def test_collapse_dispatch_preserves_opaque_callables_and_real_collector_subclasses():
    class Merger:
        def __getattribute__(self, name):
            if name == "__class__":
                raise AssertionError("merger class inspection")
            return object.__getattribute__(self, name)

        def __call__(self, left, right):
            return left + right

    class AsyncMerger(Merger):
        async def __call__(self, left, right):
            return left + right

    class Count(fpstreams.Collector):
        def __getattribute__(self, name):
            if name == "__class__":
                raise AssertionError("collector class inspection")
            return object.__getattribute__(self, name)

    def adjacent(left, right):
        return left == right

    assert fpstreams.flow([1, 1, 2]).collapse(adjacent, Merger()).to_list() == [2, 2]
    assert await fpstreams.aflow([1, 1, 2]).collapse(adjacent, Merger()).to_list() == [2, 2]
    assert await fpstreams.aflow([1, 1, 2]).collapse(adjacent, AsyncMerger()).to_list() == [2, 2]
    collector = Count(lambda: 0, lambda state, value: state + 1)
    assert fpstreams.flow([1, 1, 2]).collapse(adjacent, collector).to_list() == [2, 1]
    assert await fpstreams.aflow([1, 1, 2]).collapse(adjacent, collector).to_list() == [2, 1]


@pytest.mark.asyncio
async def test_async_attempt_and_partition_terminals_preserve_errors_and_cancellation() -> None:
    async def work(value: int) -> int:
        await asyncio.sleep(0)
        if value == 2:
            raise ValueError("bad two")
        return value * 10

    successes, failures = await fpstreams.aflow([1, 2, 3]).attempt(work).partition_results()
    assert successes == [10, 30]
    assert len(failures) == 1 and str(failures[0]) == "bad two"

    async def even(value: int) -> bool:
        await asyncio.sleep(0)
        return value % 2 == 0

    assert await fpstreams.aflow(range(5)).partition(even) == ([0, 2, 4], [1, 3])
    assert await fpstreams.aflow([1, 2, 3]).join("|") == "1|2|3"

    async def cancel(_value: int) -> int:
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await fpstreams.aflow([1]).attempt(cancel).to_list()


@pytest.mark.asyncio
async def test_combine_latest_tracks_each_source_in_completion_order() -> None:
    closed: set[str] = set()
    left_ready = asyncio.Event()
    left_update = asyncio.Event()
    right_updates = [asyncio.Event() for _ in range(3)]
    consumed: asyncio.Queue[tuple[str, str]] = asyncio.Queue()
    result: list[tuple[str, str]] = []

    async def left() -> AsyncIterator[str]:
        try:
            yield "left-0"
            # The next pull confirms that combine_latest accepted the first value.
            left_ready.set()
            await left_update.wait()
            yield "left-1"
        finally:
            closed.add("left")

    async def right() -> AsyncIterator[str]:
        try:
            for position, gate in enumerate(right_updates):
                await gate.wait()
                yield f"right-{position}"
        finally:
            closed.add("right")

    async def consume() -> None:
        async for value in fpstreams.aflow(left()).combine_latest(right()):
            result.append(value)
            consumed.put_nowait(value)

    async with asyncio.TaskGroup() as tasks:
        tasks.create_task(consume())
        await left_ready.wait()
        for gate, expected in [
            (right_updates[0], ("left-0", "right-0")),
            (right_updates[1], ("left-0", "right-1")),
            (left_update, ("left-1", "right-1")),
            (right_updates[2], ("left-1", "right-2")),
        ]:
            gate.set()
            assert await consumed.get() == expected

    assert result == [
        ("left-0", "right-0"),
        ("left-0", "right-1"),
        ("left-1", "right-1"),
        ("left-1", "right-2"),
    ]
    assert closed == {"left", "right"}
    assert await fpstreams.aflow([1, 2]).combine_latest().to_list() == [(1,), (2,)]


@pytest.mark.asyncio
async def test_combine_latest_empty_source_cancels_other_sources() -> None:
    sibling_cancelled = sibling_closed = False

    async def empty() -> AsyncIterator[int]:
        if False:
            yield 0

    async def sibling() -> AsyncIterator[int]:
        nonlocal sibling_cancelled, sibling_closed
        try:
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                sibling_cancelled = True
                raise
            yield 1
        finally:
            sibling_closed = True

    assert await fpstreams.aflow(empty()).combine_latest(sibling()).to_list() == []
    assert sibling_cancelled and sibling_closed


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["merge", "combine_latest"])
@pytest.mark.parametrize("empty_position", [0, 1])
async def test_async_multi_source_does_not_hide_a_simultaneous_source_failure(
    operation: str,
    empty_position: int,
) -> None:
    gate = asyncio.Event()
    started = 0

    class Source(AsyncIterator[int]):
        def __init__(
            self,
            failure: BaseException | None,
            close_failure: BaseException | None = None,
        ) -> None:
            self.failure = failure
            self.close_failure = close_failure
            self.close_calls = 0

        async def __anext__(self) -> int:
            nonlocal started
            started += 1
            if started == 2:
                gate.set()
            await gate.wait()
            if self.failure is None:
                raise StopAsyncIteration
            raise self.failure

        async def aclose(self) -> None:
            self.close_calls += 1
            if self.close_failure is not None:
                raise self.close_failure

    primary = RuntimeError("concurrent source failed")
    sources = [Source(None, OSError("empty source close failed")), Source(primary)]
    if empty_position == 1:
        sources.reverse()

    values = fpstreams.aflow(sources[0])
    values = values.merge(sources[1]) if operation == "merge" else values.combine_latest(sources[1])
    with pytest.raises(RuntimeError) as captured:
        await values.to_list()

    assert captured.value is primary
    assert captured.value.__notes__ == ["cleanup failed: OSError: empty source close failed"]
    assert [source.close_calls for source in sources] == [1, 1]


@pytest.mark.asyncio
async def test_timeout_limits_wait_between_elements_and_closes_source() -> None:
    closed = False

    async def source() -> AsyncIterator[int]:
        nonlocal closed
        try:
            yield 1
            await asyncio.sleep(10)
            yield 2
        finally:
            closed = True

    iterator = fpstreams.aflow(source()).timeout(0.005).__aiter__()
    assert await anext(iterator) == 1
    with pytest.raises(TimeoutError):
        await anext(iterator)
    assert closed


@pytest.mark.asyncio
async def test_timeout_caller_cancellation_waits_for_pull_cleanup_before_source_close() -> None:
    """A timeout's cancelled pull finishes cleanup before the source is closed."""
    cleaning = asyncio.Event()
    release = asyncio.Event()
    events: list[str] = []

    class Source:
        def __aiter__(self):
            return self

        async def __anext__(self):
            try:
                await asyncio.Event().wait()
            finally:
                events.append("pull-cleanup-start")
                cleaning.set()
                await release.wait()
                events.append("pull-cleanup-done")

        async def aclose(self):
            events.append("source-close")

    consume = asyncio.create_task(fpstreams.aflow(Source()).timeout(0.001).to_list())
    try:
        async with asyncio.timeout(5):
            await cleaning.wait()
            for message in ("first-delivered", "later-cancel"):
                consume.cancel(message)
                for _ in range(4):
                    await asyncio.sleep(0)
                assert not consume.done()
                assert events == ["pull-cleanup-start"]
            release.set()
            with pytest.raises(asyncio.CancelledError) as caught:
                await consume
            assert caught.value.args == ("first-delivered",)
        assert events == ["pull-cleanup-start", "pull-cleanup-done", "source-close"]
    finally:
        release.set()
        await asyncio.gather(consume, return_exceptions=True)


@pytest.mark.asyncio
async def test_timeout_cancel_failure_still_closes_source() -> None:
    from fpstreams.runtime.failpoints import failpoint

    source = _PrefetchBlockingSource(block_first=True)
    with failpoint("task.cancel.before", RuntimeError("timer cancellation failed")):
        task = asyncio.create_task(fpstreams.aflow(source).timeout(30).to_list())
    await asyncio.wait_for(source.pull_started.wait(), timeout=1)

    task.cancel()
    with pytest.raises(asyncio.CancelledError) as captured:
        await asyncio.wait_for(task, timeout=1)

    assert captured.value.__notes__ == ["cleanup failed: RuntimeError: timer cancellation failed"]
    assert source.close_calls == 1


@pytest.mark.asyncio
async def test_debounce_is_trailing_edge_and_flushes_on_completion() -> None:
    closed = False

    async def source() -> AsyncIterator[int]:
        nonlocal closed
        try:
            yield 1
            await asyncio.sleep(0.002)
            yield 2
            await asyncio.sleep(0.015)
            yield 3
        finally:
            closed = True

    assert await fpstreams.aflow(source()).debounce(0.008).to_list() == [2, 3]
    assert closed
    assert await fpstreams.aflow([1, 2, 3]).debounce(0).to_list() == [3]


@pytest.mark.asyncio
async def test_debounce_short_circuit_cancels_pending_pull() -> None:
    closed = False

    async def source() -> AsyncIterator[int]:
        nonlocal closed
        try:
            yield 1
            await asyncio.sleep(10)
            yield 2
        finally:
            closed = True

    assert await fpstreams.aflow(source()).debounce(0.001).take(1).to_list() == [1]
    assert closed


@pytest.mark.asyncio
async def test_buffer_timeout_flushes_on_count_time_and_completion() -> None:
    closed = False

    async def source() -> AsyncIterator[int]:
        nonlocal closed
        try:
            yield 1
            yield 2
            yield 3
            await asyncio.sleep(0.02)
            yield 4
        finally:
            closed = True

    result = await fpstreams.aflow(source()).buffer_timeout(2, 0.005).to_list()

    assert result == [(1, 2), (3,), (4,)]
    assert closed
    assert await fpstreams.aflow([1, 2, 3]).batch_timeout(10, 1).to_list() == [(1, 2, 3)]


@pytest.mark.parametrize(
    "configure",
    [
        pytest.param(lambda values, seconds: fpstreams.aflow.interval(seconds), id="interval"),
        pytest.param(lambda values, seconds: values.delay(seconds), id="delay"),
        pytest.param(lambda values, seconds: values.throttle(1, per=seconds), id="throttle"),
        pytest.param(lambda values, seconds: values.spaceout(seconds), id="spaceout"),
        pytest.param(lambda values, seconds: values.timeout(seconds), id="timeout"),
        pytest.param(lambda values, seconds: values.debounce(seconds), id="debounce"),
        pytest.param(
            lambda values, seconds: values.buffer_timeout(2, seconds), id="buffer_timeout"
        ),
        pytest.param(lambda values, seconds: values.batch_timeout(2, seconds), id="batch_timeout"),
        pytest.param(
            lambda values, seconds: values.session_window(seconds, max_count=2), id="session_window"
        ),
        pytest.param(
            lambda values, seconds: values.map_async(lambda item: item, timeout=seconds),
            id="map_async",
        ),
        pytest.param(
            lambda values, seconds: values.map_batches(
                lambda batch: batch, size=2, timeout=seconds
            ),
            id="map_batches",
        ),
    ],
)
@pytest.mark.parametrize("seconds", [float("nan"), float("inf")], ids=["nan", "infinity"])
def test_async_duration_configuration_checks_before_source_open(configure, seconds) -> None:
    opened = []

    def source():
        opened.append(True)
        return [1]

    values = fpstreams.aflow.defer(source)
    if seconds != seconds:
        with pytest.raises(ValueError):
            configure(values, seconds)
    else:
        assert isinstance(configure(values, seconds), fpstreams.AsyncFlow)
    assert opened == []


@pytest.mark.asyncio
@pytest.mark.parametrize("seconds", [0.0, -0.0], ids=["zero", "negative_zero"])
async def test_interval_and_debounce_keep_zero_duration_semantics(seconds) -> None:
    assert await asyncio.wait_for(fpstreams.aflow.interval(seconds).take(1).to_list(), 1) == [0]
    assert await asyncio.wait_for(fpstreams.aflow([1]).debounce(seconds).to_list(), 1) == [1]


def test_realtime_operators_validate_durations_and_counts() -> None:
    values = fpstreams.aflow([1])
    with pytest.raises(ValueError, match="positive"):
        values.timeout(0)
    with pytest.raises(ValueError, match="negative"):
        values.debounce(-1)
    with pytest.raises(ValueError, match="at least 1"):
        values.buffer_timeout(0, 1)
    with pytest.raises(TypeError, match="integer"):
        values.buffer_timeout(1.5, 1)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="positive"):
        values.buffer_timeout(1, 0)


@pytest.mark.asyncio
async def test_async_cleanup_error_does_not_hide_pipeline_error() -> None:
    async def source():
        try:
            yield 1
        finally:
            raise RuntimeError("cleanup failed")

    def explode(_: int) -> int:
        raise ValueError("transform failed")

    with pytest.raises(ValueError, match="transform failed") as captured:
        await fpstreams.aflow(source()).map(explode).to_list()

    assert any("cleanup failed" in note for note in captured.value.__notes__)


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal", ["for_each", "any"])
async def test_async_callable_terminals_keep_primary_failure_when_source_close_fails(
    terminal: str,
) -> None:
    """Action and predicate failures stay primary while their source is closed once."""
    primary = ValueError("callback failed")

    source = _AsyncCloseFailureSource()

    def fail(_value: int) -> bool:
        raise primary

    values = fpstreams.aflow(source)
    with pytest.raises(ValueError) as captured:
        if terminal == "for_each":
            await values.for_each(fail)
        else:
            await values.any(fail)

    assert captured.value is primary
    assert captured.value.__notes__ == ["cleanup failed with OSError: source close failed"]
    assert source.close_calls == 1


@pytest.mark.asyncio
async def test_async_terminal_cleanup_failure_is_not_hidden_by_an_outer_exception() -> None:
    """A normally completing terminal must not treat an ambient handler as its own failure."""
    outer = RuntimeError("outer failure")

    source = _AsyncCloseFailureSource()
    try:
        raise outer
    except RuntimeError:
        with pytest.raises(OSError, match="source close failed"):
            await fpstreams.aflow(source).first()

    assert getattr(outer, "__notes__", ()) == ()
    assert source.close_calls == 1


@pytest.mark.asyncio
async def test_async_sync_adapter_keeps_its_own_error_boundary() -> None:
    class Source(Iterator[int]):
        def __init__(self, failure: BaseException | None) -> None:
            self.failure = failure
            self.emitted = False
            self.close_calls = 0

        def __next__(self) -> int:
            if self.failure is not None:
                raise self.failure
            if self.emitted:
                raise StopIteration
            self.emitted = True
            return 1

        def close(self) -> None:
            self.close_calls += 1
            raise OSError("sync source close failed")

    primary = ValueError("sync pull failed")
    failed = Source(primary)
    with pytest.raises(ValueError) as captured:
        await fpstreams.aflow(failed).to_list()
    assert captured.value is primary
    assert captured.value.__notes__ == ["cleanup failed with OSError: sync source close failed"]
    assert failed.close_calls == 1

    completed = Source(None)
    outer = RuntimeError("outer")
    try:
        raise outer
    except RuntimeError:
        with pytest.raises(OSError, match="sync source close failed"):
            await fpstreams.aflow(completed).to_list()
    assert getattr(outer, "__notes__", ()) == ()
    assert completed.close_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal", ["to_set", "reduce", "reduce_by", "for_each"])
async def test_async_terminal_body_failure_closes_source_immediately(terminal: str) -> None:
    close_calls = 0

    class FailingHash:
        def __hash__(self) -> int:
            raise RuntimeError("terminal body failed")

    async def source():
        nonlocal close_calls
        try:
            yield 1
            yield FailingHash() if terminal == "to_set" else 2
        finally:
            close_calls += 1

    def fail(*_args):
        raise RuntimeError("terminal body failed")

    values = fpstreams.aflow(source())
    if terminal == "to_set":
        result = values.to_set()
    elif terminal == "reduce":
        result = values.reduce(fail)
    elif terminal == "reduce_by":
        result = values.reduce_by(fail, lambda state, _item: state, initializer=lambda: 0)
    else:
        result = values.for_each(fail)

    with pytest.raises(RuntimeError, match="terminal body failed"):
        await result

    assert close_calls == 1


@pytest.mark.asyncio
async def test_async_cleanup_attempts_every_owned_iterator() -> None:
    import fpstreams.execution.asynchronous.operators as async_ops

    events: list[str] = []

    class ClosingIterator:
        def __init__(self, name: str, *, fail: bool) -> None:
            self.name = name
            self.fail = fail

        def __aiter__(self):
            return self

        async def __anext__(self):
            raise StopAsyncIteration

        async def aclose(self) -> None:
            events.append(self.name)
            if self.fail:
                raise RuntimeError(f"{self.name} cleanup failed")

    first = ClosingIterator("first", fail=True)
    second = ClosingIterator("second", fail=False)

    assert hasattr(async_ops, "close_async_iterators"), (
        "batch asynchronous cleanup must be available"
    )
    with pytest.raises(RuntimeError, match="first cleanup failed"):
        await async_ops.close_async_iterators((first, second))

    assert events == ["first", "second"]


def test_async_flow_control_validates_before_opening_source() -> None:
    opened = False

    async def source():
        nonlocal opened
        opened = True
        yield 1

    values = fpstreams.aflow(source())

    with pytest.raises(ValueError, match="positive"):
        values.delay(0)
    with pytest.raises(TypeError, match="integer"):
        values.throttle(1.5, per=1)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="at least 1"):
        values.throttle(0, per=1)
    with pytest.raises(ValueError, match="positive"):
        values.throttle(1, per=0)
    with pytest.raises(ValueError, match="positive"):
        values.spaceout(0)
    with pytest.raises(TypeError, match="callable"):
        values.switch_map(None)  # type: ignore[arg-type]

    assert not opened


@pytest.mark.asyncio
async def test_async_delay_throttle_and_spaceout_use_monotonic_backpressure() -> None:
    loop = asyncio.get_running_loop()
    source_pulls: list[float] = []

    async def source():
        source_pulls.append(loop.time())
        yield 1
        yield 2

    started = loop.time()
    assert await fpstreams.aflow(source()).delay(0.02).to_list() == [1, 2]
    assert source_pulls[0] - started >= 0.012

    throttle_times: list[float] = []
    assert (
        await fpstreams.aflow(range(5))
        .throttle(2, per=0.02)
        .tap(lambda _item: throttle_times.append(loop.time()))
        .to_list()
    ) == [0, 1, 2, 3, 4]
    assert throttle_times[2] - throttle_times[0] >= 0.012
    assert throttle_times[4] - throttle_times[2] >= 0.012

    spaced_times: list[float] = []
    assert (
        await fpstreams.aflow(range(3))
        .spaceout(0.02)
        .tap(lambda _item: spaced_times.append(loop.time()))
        .to_list()
    ) == [0, 1, 2]
    assert all(right - left >= 0.012 for left, right in pairwise(spaced_times))


@pytest.mark.asyncio
async def test_switch_map_closes_superseded_inner_and_finishes_latest() -> None:
    closed: list[int] = []

    async def source():
        yield 1
        await asyncio.sleep(0.01)
        yield 2

    async def inner(value: int):
        try:
            yield value * 10
            await asyncio.sleep(0.03)
            yield value * 10 + 1
        finally:
            closed.append(value)

    result = await fpstreams.aflow(source()).switch_map(inner).to_list()

    assert result == [10, 20, 21]
    assert closed == [1, 2]


@pytest.mark.asyncio
async def test_switch_map_prefers_new_outer_value_and_accepts_awaitable_mapper() -> None:
    async def mapper(value: int):
        await asyncio.sleep(0)
        return [value, value + 10]

    result = await fpstreams.aflow([1, 2]).switch_map(mapper).to_list()

    assert result == [2, 12]


@pytest.mark.asyncio
async def test_switch_map_short_circuit_closes_outer_and_current_inner() -> None:
    outer_closed = False
    inner_closed = False
    never = asyncio.Event()

    async def source():
        nonlocal outer_closed
        try:
            yield 1
            await never.wait()
        finally:
            outer_closed = True

    async def inner(value: int):
        nonlocal inner_closed
        try:
            yield value * 10
            await never.wait()
        finally:
            inner_closed = True

    assert await fpstreams.aflow(source()).switch_map(inner).take(1).to_list() == [10]
    assert outer_closed
    assert inner_closed


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_stage", ["factory", "registered"])
@pytest.mark.parametrize("close_fails", [False, True])
@pytest.mark.parametrize("upstream_stage", [False, True])
async def test_switch_map_initial_task_failure_cancels_and_closes_owned_source(
    tmp_path, failure_stage, close_fails, upstream_stage
) -> None:
    """An unstarted outer source still owns a real file that must close on admission failure."""
    from contextlib import nullcontext

    from fpstreams.runtime.failpoints import failpoint

    path = tmp_path / "outer.txt"
    path.write_text("1\n", encoding="utf-8")
    admission = OSError("initial pull rejected")
    cleanup = OSError("outer close failed")

    class Source:
        def __init__(self):
            self.handle = path.open(encoding="utf-8")
            self.pulls = self.close_calls = 0

        def __aiter__(self):
            return self

        async def __anext__(self):
            self.pulls += 1
            line = self.handle.readline()
            if not line:
                raise StopAsyncIteration
            return int(line)

        async def aclose(self):
            self.close_calls += 1
            self.handle.close()
            if close_fails:
                raise cleanup

    source = Source()
    mapped = []

    def mapper(value):
        mapped.append(value)
        return [value]

    loop = asyncio.get_running_loop()
    previous = loop.get_task_factory()
    rejected = []

    def reject_once(loop, coroutine, **kwargs):
        if not rejected:
            rejected.append(coroutine.__qualname__)
            raise admission
        if previous is not None:
            return previous(loop, coroutine, **kwargs)
        return asyncio.Task(coroutine, loop=loop, **kwargs)

    boundary = (
        failpoint("task.create.after", admission)
        if failure_stage == "registered"
        else nullcontext()
    )
    try:
        values = fpstreams.aflow(source)
        if upstream_stage:
            values = values.map(lambda value: value)
        values = values.switch_map(mapper)
        if failure_stage == "factory":
            loop.set_task_factory(reject_once)
        async with asyncio.timeout(10):
            with boundary, pytest.raises(OSError) as caught:
                await values.to_list()
        assert caught.value is admission
        assert source.close_calls == 1
        assert source.handle.closed
        assert source.pulls == 0
        assert mapped == []
        assert rejected == (["_pull"] if failure_stage == "factory" else [])
        assert getattr(admission, "__notes__", []) == (
            ["cleanup failed: OSError: outer close failed"] if close_fails else []
        )
    finally:
        loop.set_task_factory(previous)
        source.handle.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal", ["zero", "first", "full"])
@pytest.mark.parametrize("close_fails", [False, True])
@pytest.mark.parametrize("protocol", ["sync", "async"])
async def test_async_root_cleanup_continues_across_unstarted_and_completed_stages(
    tmp_path, terminal, close_fails, protocol
) -> None:
    """Root ownership covers both input protocols without duplicating source close."""
    from contextlib import ExitStack

    path = tmp_path / "root.txt"
    path.write_text("1\n2\n", encoding="utf-8")
    opened = []
    failure = OSError("root close failed")
    cleanup = ExitStack()

    class SyncSource:
        def __init__(self):
            self.handle = cleanup.enter_context(path.open(encoding="utf-8"))
            self.pulls = self.closes = 0
            self.iterations = self.async_iterations = 0

        def __getattribute__(self, name):
            if name in {"__anext__", "__next__"}:
                raise AssertionError("iteration must use special-method lookup")
            return object.__getattribute__(self, name)

        def __iter__(self):
            self.iterations += 1
            return self

        def __next__(self):
            self.pulls += 1
            line = self.handle.readline()
            if not line:
                raise StopIteration
            return int(line)

        def close(self):
            self.closes += 1
            self.handle.close()
            if close_fails:
                raise failure

    class AsyncSource(SyncSource):
        def __aiter__(self):
            self.async_iterations += 1
            return self

        async def __anext__(self):
            try:
                return next(self)
            except StopIteration:
                raise StopAsyncIteration from None

        async def aclose(self):
            self.close()

    source_type = {"sync": SyncSource, "async": AsyncSource}[protocol]

    def factory():
        source = source_type()
        opened.append(source)
        return source

    values = fpstreams.aflow.defer(factory).map(lambda value: value * 2)
    values = (
        values.take(0) if terminal == "zero" else values.take(1) if terminal == "first" else values
    )
    try:
        if close_fails:
            with pytest.raises(OSError) as caught:
                await values.to_list()
            assert caught.value is failure
        else:
            assert (
                await values.to_list()
                == {
                    "zero": [],
                    "first": [2],
                    "full": [2, 4],
                }[terminal]
            )
        assert len(opened) == 1
        assert opened[0].closes == 1
        assert opened[0].handle.closed
        assert opened[0].pulls == {"zero": 0, "first": 1, "full": 3}[terminal]
        expected_iterations = {"zero": 1, "first": 2, "full": 2}[terminal]
        assert (opened[0].iterations, opened[0].async_iterations) == (
            (expected_iterations, 0) if protocol == "sync" else (0, expected_iterations)
        )
    finally:
        cleanup.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("protocol", ["sync", "async"])
async def test_async_root_cleanup_continues_with_separate_traversal(protocol) -> None:
    """Iterating may select another iterator while the acquired owner stays responsible."""
    events = []

    class SyncSource:
        iterations = 0

        def __iter__(self):
            self.iterations += 1
            events.append("iter")
            return self if self.iterations == 1 else iter((3, 5))

        def __next__(self):
            raise AssertionError("the acquired owner is not the traversal iterator")

        def close(self):
            events.append("close")

    class AsyncTraversal:
        values = iter((3, 5))

        def __aiter__(self):
            return self

        async def __anext__(self):
            try:
                return next(self.values)
            except StopIteration:
                raise StopAsyncIteration from None

        async def aclose(self):
            raise AssertionError("the acquired source owns traversal cleanup")

    class AsyncSource:
        iterations = 0

        def __aiter__(self):
            self.iterations += 1
            events.append("iter")
            return self if self.iterations == 1 else AsyncTraversal()

        async def __anext__(self):
            raise AssertionError("the acquired owner is not the traversal iterator")

        async def aclose(self):
            events.append("close")

    source = SyncSource() if protocol == "sync" else AsyncSource()
    values = fpstreams.aflow(source).map(lambda value: value * 2)
    assert events == []
    assert await values.to_list() == [6, 10]
    assert events == ["iter", "iter", "close"]


@pytest.mark.asyncio
@pytest.mark.parametrize("upstream_stage", [False, True])
async def test_async_root_cleanup_continues_after_runtime_closes_root(upstream_stage) -> None:
    """A suspended execution cannot retain or pull its root after runtime shutdown."""
    import gc
    import weakref

    from fpstreams.execution.asynchronous import execute_async_physical
    from fpstreams.physical.async_plan import compile_async_query
    from fpstreams.runtime.query import QueryRuntime

    references = []
    pulls = []
    closed = []

    class Source:
        def __aiter__(self):
            return self

        async def __anext__(self):
            pulls.append(len(pulls) + 1)
            return pulls[-1]

        async def aclose(self):
            closed.append(True)

    def factory():
        source = Source()
        references.append(weakref.ref(source))
        return source

    values = fpstreams.aflow.defer(factory)
    if upstream_stage:
        values = values.map(lambda value: value)
    runtime = QueryRuntime()
    iterator = execute_async_physical(compile_async_query(values._query("iterate")), runtime)
    try:
        assert await anext(iterator) == 1
        await runtime.aclose()
        gc.collect()
        retained = references[0]() is not None
        next_value = await anext(iterator, None)
        assert (next_value, retained, pulls, closed) == (None, False, [1], [True])
        assert runtime.resources._records == []
        assert runtime.metrics.live_tasks == 0
    finally:
        await iterator.aclose()


@pytest.mark.asyncio
async def test_async_sync_adapter_cleanup_continues_releasing_traversal_before_owner() -> None:
    """Exhaustion releases a separate traversal before invoking the owner's close hook."""
    import weakref

    from fpstreams.planning.async_ import _to_async_iterator

    references = []
    closed = []

    class Traversal:
        emitted = False

        def __iter__(self):
            return self

        def __next__(self):
            if self.emitted:
                raise StopIteration
            self.emitted = True
            return 1

    class Owner:
        iterations = 0

        def __iter__(self):
            self.iterations += 1
            if self.iterations == 1:
                return self
            traversal = Traversal()
            references.append(weakref.ref(traversal))
            return traversal

        def __next__(self):
            raise AssertionError("traversal must come from the second iter call")

        def close(self):
            closed.append(references[0]() is None)

    iterator = _to_async_iterator(Owner())
    try:
        assert await anext(iterator) == 1
        with pytest.raises(StopAsyncIteration):
            await anext(iterator)
        assert closed == [True]
    finally:
        await iterator.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["iter", "next", "close-eof", "close-explicit"])
@pytest.mark.parametrize("error_type", ["stop", "async-stop", "exit", "cancel", "group"])
async def test_async_sync_adapter_cancellation_and_stop_errors_keep_cleanup(
    mode, error_type
) -> None:
    """An eager close owner must retain the old generator boundary's error semantics."""
    from fpstreams.planning.async_ import _to_async_iterator

    error = {
        "stop": StopIteration,
        "async-stop": StopAsyncIteration,
        "exit": GeneratorExit,
        "cancel": asyncio.CancelledError,
        "group": lambda message: BaseExceptionGroup(
            message, [GeneratorExit("grouped exit"), OSError("grouped failure")]
        ),
    }[error_type]("injected source control error")

    class Source:
        closes = 0
        iterations = 0

        def __iter__(self):
            self.iterations += 1
            if mode == "iter" and self.iterations == 2:
                raise error
            return self

        def __next__(self):
            if mode == "next":
                raise error
            if mode == "close-explicit":
                return 0
            raise StopIteration

        def close(self):
            self.closes += 1
            if mode not in {"iter", "next"}:
                raise error

    source = Source()
    iterator = _to_async_iterator(source)
    if mode == "close-explicit":
        assert await anext(iterator) == 0

    def action():
        if mode == "close-explicit":
            return iterator.aclose()
        return anext(iterator)

    try:
        if mode == "close-explicit" and error_type == "exit":
            await action()
        elif mode == "next" and error_type == "stop":
            with pytest.raises(StopAsyncIteration):
                await action()
        elif error_type in {"stop", "async-stop"}:
            message = (
                (
                    "async generator raised StopIteration"
                    if mode == "iter"
                    else "generator raised StopIteration"
                )
                if error_type == "stop"
                else "async generator raised StopAsyncIteration"
            )
            with pytest.raises(RuntimeError, match=message) as caught:
                await action()
            assert caught.value.__cause__ is error
        else:
            with pytest.raises(type(error)) as caught:
                await action()
            assert caught.value is error
        assert source.closes == 1
    finally:
        await iterator.aclose()


@pytest.mark.asyncio
async def test_switch_map_mapper_error_closes_the_outer_source() -> None:
    closed = False

    async def source():
        nonlocal closed
        try:
            yield 1
        finally:
            closed = True

    def mapper(_value: int):
        raise RuntimeError("mapping failed")

    with pytest.raises(RuntimeError, match="mapping failed"):
        await fpstreams.aflow(source()).switch_map(mapper).to_list()

    assert closed


@pytest.mark.asyncio
async def test_async_run_with_report_captures_query_owned_task_high_water() -> None:
    async def double(value: int) -> int:
        await asyncio.sleep(0)
        return value * 2

    execution = await (
        fpstreams.aflow(range(12)).map_async(double, concurrency=3).run_with_report("to_list")
    )

    assert execution.value == [value * 2 for value in range(12)]
    assert execution.report.terminal == "to_list"
    assert execution.report.requested_engine == "async"
    assert execution.report.compiler_engine == "async"
    assert execution.report.strategy == "async_scheduler"
    assert 1 <= execution.report.peak_owned_async_tasks <= 4


@pytest.mark.asyncio
@pytest.mark.parametrize("source", [[1, 2, 3], (1, 2, 3), range(1, 4)])
async def test_async_retained_identity_terminals_preserve_values_and_report_strategy(
    source: list[int] | tuple[int, ...] | range,
) -> None:
    values = fpstreams.aflow(source)

    listed = await values.run_with_report("to_list")
    tupled = await values.to_tuple()

    assert listed.value == [1, 2, 3]
    assert tupled == (1, 2, 3)
    assert await values.count() == 3
    assert listed.report.compiler_engine == "not_compiled"
    assert listed.report.strategy == "python_direct"
    if type(source) is tuple:
        assert tupled is not source


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("source", "size", "prefix"),
    [
        (range(1 << 65), 1 << 65, [0, 1, 2]),
        (
            range(1 << 65, 1, -4),
            1 << 63,
            [1 << 65, (1 << 65) - 4, (1 << 65) - 8],
        ),
    ],
)
async def test_async_flow_supports_ranges_larger_than_ssize_t(
    source: range, size: int, prefix: list[int]
) -> None:
    values = fpstreams.aflow(source)

    assert await values.count() == size
    assert await values.take(3).to_list() == prefix
    assert await values.take(3).to_tuple() == tuple(prefix)
    assert await values.map(lambda value: value + 1).take(3).to_list() == [
        value + 1 for value in prefix
    ]


@pytest.mark.asyncio
async def test_async_retained_list_terminals_read_live_length_and_values() -> None:
    source = [1, 2]
    values = fpstreams.aflow(source)
    source.append(3)

    assert await values.count() == 3
    assert await values.to_list() == [1, 2, 3]

    source.pop(0)
    assert await values.to_tuple() == (2, 3)


@pytest.mark.asyncio
async def test_async_retained_identity_deopts_for_failpoints() -> None:
    from fpstreams.runtime.failpoints import failpoint

    with failpoint("unrelated.transition", RuntimeError("unused")):
        instrumented = await fpstreams.aflow([1, 2]).run_with_report("count")
    assert instrumented.value == 2
    assert instrumented.report.strategy == "async_scheduler"


@pytest.mark.asyncio
async def test_async_identity_iterator_source_remains_one_shot() -> None:
    values = fpstreams.aflow(iter([1, 2, 3]))

    assert await values.count() == 3
    with pytest.raises(fpstreams.FlowConsumedError):
        await values.to_list()


@pytest.mark.asyncio
async def test_async_run_with_report_allows_nested_sync_source_plans() -> None:
    source = fpstreams.flow([1, 2]).map(lambda value: value * 2)

    execution = await fpstreams.aflow(source).run_with_report("to_list")

    assert execution.value == [2, 4]
    assert execution.report.compiler_engine == "async"
    assert execution.report.events[0].compiler_engine == "async"
    assert any(event.compiler_engine == "python" for event in execution.report.events)


@pytest.mark.asyncio
async def test_run_with_report_events_keep_concurrent_queries_separate() -> None:
    async def double(value):
        await asyncio.sleep(0)
        return value * 2

    async def observed():
        await asyncio.sleep(0)
        source = fpstreams.flow([1, 2]).with_engine("python").map(lambda value: value)
        return await fpstreams.aflow(source).map_async(double).run_with_report("to_list")

    async def ordinary():
        await asyncio.sleep(0)
        return await fpstreams.aflow([3, 4]).map_async(double).run_with_report("to_list")

    diagnostic, plain = await asyncio.gather(observed(), ordinary())

    assert diagnostic.value == [2, 4]
    assert plain.value == [6, 8]
    assert any(event.compiler_engine == "python" for event in diagnostic.report.events)
    assert plain.report.events
    assert all(event.compiler_engine == "async" for event in plain.report.events)
    assert plain.report.events_truncated is False


@pytest.mark.asyncio
async def test_async_run_with_report_deactivates_inherited_background_context() -> None:
    release = asyncio.Event()
    background: list[asyncio.Task[object]] = []

    async def later() -> object:
        await release.wait()
        return await fpstreams.aflow([2]).run_with_report("to_list")

    def spawn(_value: int) -> None:
        background.append(asyncio.create_task(later()))

    outer = await fpstreams.aflow([1]).run_with_report("for_each", spawn)
    release.set()
    inner = await background[0]

    assert outer.value is None
    assert isinstance(inner, fpstreams.ExecutionResult)
    assert inner.value == [2]


@pytest.mark.asyncio
async def test_async_run_with_report_allows_nested_reported_mapper_terminal() -> None:
    async def inner(value: int) -> int:
        result = await fpstreams.aflow([value]).run_with_report("sum")
        return result.value

    execution = await fpstreams.aflow([1, 2]).map_async(inner).run_with_report("to_list")

    assert execution.value == [1, 2]
    assert execution.report.terminal == "to_list"


@pytest.mark.asyncio
async def test_session_window_flushes_on_idle_count_and_completion() -> None:
    async def source() -> AsyncIterator[int]:
        yield 1
        yield 2
        yield 3
        await asyncio.sleep(0.02)
        yield 4

    result = await fpstreams.aflow(source()).session_window(0.005, max_count=2).to_list()

    assert result == [(1, 2), (3,), (4,)]


@pytest.mark.asyncio
async def test_session_window_resets_the_idle_timer_after_every_item() -> None:
    async def source() -> AsyncIterator[int]:
        yield 1
        await asyncio.sleep(0.02)
        yield 2
        await asyncio.sleep(0.02)
        yield 3
        await asyncio.sleep(0.05)

    result = await fpstreams.aflow(source()).session_window(0.03, max_count=10).to_list()

    assert result == [(1, 2, 3)]


@pytest.mark.asyncio
async def test_session_window_prefers_a_pull_when_pull_and_timer_are_both_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fpstreams.execution.asynchronous import timers as async_timers

    async def wait_for_all(
        futures: set[asyncio.Task[object]],
        *,
        return_when: str,
    ) -> tuple[set[asyncio.Task[object]], set[asyncio.Task[object]]]:
        assert return_when == asyncio.FIRST_COMPLETED
        await asyncio.gather(*futures, return_exceptions=True)
        return futures, set()

    monkeypatch.setattr(async_timers.asyncio, "wait", wait_for_all)

    result = await fpstreams.aflow([1, 2]).session_window(0.001, max_count=10).to_list()

    assert result == [(1, 2)]


@pytest.mark.asyncio
async def test_session_window_does_not_flush_after_upstream_failure() -> None:
    emitted: list[tuple[int, ...]] = []
    closed = False

    async def source() -> AsyncIterator[int]:
        nonlocal closed
        try:
            yield 1
            raise RuntimeError("upstream failed")
        finally:
            closed = True

    with pytest.raises(RuntimeError, match="upstream failed"):
        await (
            fpstreams.aflow(source()).session_window(1, max_count=10).tap(emitted.append).to_list()
        )

    assert emitted == []
    assert closed


@pytest.mark.asyncio
async def test_session_window_short_circuit_cancels_pull_and_closes_upstream() -> None:
    source = _PrefetchBlockingSource()

    result = await fpstreams.aflow(source).session_window(0.001, max_count=10).take(1).to_list()

    assert result == [(1,)]
    assert source.pull_started.is_set()
    assert source.pull_cancelled.is_set()
    assert source.close_calls == 1


def test_session_window_validates_and_uses_bounded_timer_planning() -> None:
    from fpstreams.physical.async_plan import AsyncTimerNode, compile_async_query

    class Count:
        def __index__(self) -> int:
            return 3

    values = fpstreams.aflow([1, 2]).session_window(1, max_count=Count())  # type: ignore[arg-type]
    physical = compile_async_query(values._query("list"))
    operation = values.explain("list").to_dict()["operations"][0]

    assert len(physical.nodes) == 1
    assert isinstance(physical.nodes[0], AsyncTimerNode)
    assert physical.nodes[0].name == "session_window"
    assert operation["name"] == "session_window"
    assert operation["progress"] == "prefix_emitting"
    assert operation["state"] == {"kind": "bounded", "bound": 3, "spillable": False}
    assert operation["output"]["cardinality"] == {"kind": "unknown", "value": None}

    with pytest.raises(TypeError, match="max_count must be an integer"):
        fpstreams.aflow([1]).session_window(1, max_count=1.5)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="max_count must be at least 1"):
        fpstreams.aflow([1]).session_window(1, max_count=0)
    with pytest.raises(ValueError, match="idle_for must be positive"):
        fpstreams.aflow([1]).session_window(0, max_count=1)
    with pytest.raises(ValueError, match="idle_for must be positive"):
        fpstreams.aflow([1]).session_window(float("nan"), max_count=1)


@pytest.mark.asyncio
@pytest.mark.parametrize("ordered", [True, False])
async def test_async_backpressure_cancel_contract(ordered) -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    active = set()
    created = set()
    source_closed = []

    async def source():
        try:
            for value in range(20):
                yield value
        finally:
            source_closed.append(True)

    async def mapper(value):
        task = asyncio.current_task()
        created.add(task)
        active.add(value)
        assert len(active) <= 2
        if len(active) == 2:
            started.set()
        try:
            await release.wait()
            return value
        finally:
            active.remove(value)

    task = asyncio.create_task(
        fpstreams.aflow(source())
        .map_async(mapper, concurrency=2, buffer=2, ordered=ordered)
        .take(1)
        .to_list()
    )
    try:
        await asyncio.wait_for(started.wait(), timeout=2)
        assert len(active) == 2
        release.set()
        result = await asyncio.wait_for(task, timeout=2)
        assert len(result) == 1
        if ordered:
            assert result == [0]
        assert not active
        assert all(worker.done() for worker in created)
        assert source_closed == [True]
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_async_window_reduce_preserves_order_boundaries_and_replay():
    from functools import reduce
    from operator import add

    values = [(value,) for value in range(47)]

    async def source():
        for value in values:
            await asyncio.sleep(0)
            yield value

    for size, step in ((1, 1), (7, 1), (7, 3), (7, 7), (7, 11), (63, 3)):
        starts = range(0, max(1, len(values) - size + 1), step)
        expected = [reduce(add, values[start : start + size]) for start in starts]
        query = fpstreams.aflow.defer(source).window_reduce(size, add, step=step)
        assert await query.to_list() == expected
        assert await query.to_list() == expected
    assert await fpstreams.aflow([]).window_reduce(3, add).to_list() == []
    assert (
        await fpstreams.aflow([None] * 8).window_reduce(3, lambda a, b: None).to_list()
        == [None] * 6
    )
    operation = query.explain("list").to_dict()["operations"][0]
    assert operation["name"] == "window_reduce"
    assert operation["state"] == {"kind": "bounded", "bound": 64, "spillable": False}


async def test_async_window_reduce_short_circuit_and_failures_close_source():
    from operator import add

    closed = []
    pulled = []
    failure = LookupError("combine failed")
    source_failure = ValueError("source failed")

    async def source():
        try:
            for value in range(10):
                pulled.append(value)
                yield value
            raise source_failure
        finally:
            closed.append(True)

    query = fpstreams.aflow.defer(source).window_reduce(3, add)
    assert await query.take(1).to_list() == [3]
    assert pulled == [0, 1, 2] and closed == [True]

    def combine(left, right):
        raise failure

    with pytest.raises(LookupError) as caught:
        await fpstreams.aflow.defer(source).window_reduce(3, combine).to_list()
    assert caught.value is failure
    assert closed == [True, True]
    emitted = []
    with pytest.raises(ValueError) as caught:
        await query.tap(emitted.append).to_list()
    assert caught.value is source_failure and emitted == list(range(3, 25, 3))
    assert closed == [True, True, True]


async def test_async_window_reduce_cancellation_closes_pending_pull():
    from operator import add

    waiting = asyncio.Event()
    closed = asyncio.Event()
    emitted = []

    async def source():
        try:
            for value in (1, 2, 3):
                yield value
            waiting.set()
            await asyncio.Event().wait()
        finally:
            closed.set()

    task = asyncio.create_task(
        fpstreams.aflow(source()).window_reduce(3, add).tap(emitted.append).to_list()
    )
    await asyncio.wait_for(waiting.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert emitted == [6] and closed.is_set()


async def test_async_disjoint_window_reduce_keeps_tail_errors_identity_and_large_counts():
    import sys
    from operator import add

    huge = sys.maxsize + 17
    for size, step, expected in (
        (3, 5, [3, 18]),
        (3, 11, [3]),
        (1, huge, [0]),
        (huge, huge, [36]),
        (3, huge, [3]),
    ):
        assert (
            await fpstreams.aflow(range(9)).window_reduce(size, add, step=step).to_list()
            == expected
        )
    left, right = object(), object()
    values = [None, left, right, None, right]
    assert await fpstreams.aflow(values).window_reduce(2, lambda a, b: a, step=2).to_list() == [
        None,
        right,
    ]

    calls, emitted, pulled = [], [], []
    failure = LookupError("incomplete tail failed")
    cleanup = RuntimeError("window source cleanup failed")

    async def source():
        try:
            for value in range(7):
                pulled.append(value)
                yield value
        finally:
            raise cleanup

    def combine(left, right):
        calls.append((left, right))
        if right == 6:
            raise failure
        return left + right

    with pytest.raises(LookupError) as caught:
        await (
            fpstreams.aflow(source())
            .window_reduce(3, combine, step=5)
            .tap(emitted.append)
            .to_list()
        )
    assert caught.value is failure
    assert pulled == list(range(7)) and emitted == [3]
    assert calls == [(0, 1), (1, 2), (5, 6)]
    assert any("window source cleanup failed" in note for note in failure.__notes__)


async def test_async_disjoint_window_reduce_cancels_pending_gap_without_combining():
    waiting, closed = asyncio.Event(), asyncio.Event()
    pulled, calls, emitted = [], [], []

    async def source():
        try:
            for value in range(4):
                pulled.append(value)
                yield value
            waiting.set()
            await asyncio.Event().wait()
        finally:
            closed.set()

    def combine(left, right):
        calls.append((left, right))
        return left + right

    task = asyncio.create_task(
        fpstreams.aflow(source()).window_reduce(3, combine, step=10).tap(emitted.append).to_list()
    )
    try:
        await asyncio.wait_for(waiting.wait(), timeout=2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert pulled == [0, 1, 2, 3] and emitted == [3] and closed.is_set()
        assert calls == [(0, 1), (1, 2)]
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("method", ["filter", "tap"])
@pytest.mark.parametrize("await_result", [False, True])
async def test_async_serial_stage_releases_temporaries_before_next_callback(method, await_result):
    import gc
    import weakref

    references = []
    truth_tests = []
    closed = []

    class Temporary:
        def __init__(self, value):
            self.value = value

        def __bool__(self):
            truth_tests.append(self.value)
            return True

    async def source():
        try:
            yield 1
            yield 2
        finally:
            closed.append(True)

    def callback(value):
        temporary = Temporary(value)
        references.append(weakref.ref(temporary))

        async def resolve():
            await asyncio.sleep(0)
            return temporary

        return resolve() if await_result else temporary

    def next_callback(value):
        gc.collect()
        assert references[-1]() is None
        return value

    values = getattr(fpstreams.aflow(source()), method)(callback).map(next_callback)
    assert await values.to_list() == [1, 2]
    assert truth_tests == ([1, 2] if method == "filter" else [])
    assert closed == [True]
    assert all(reference() is None for reference in references)


@pytest.mark.parametrize("method", ["map", "filter", "filter_map", "tap"])
async def test_async_serial_stage_reads_live_callback_fields(method):
    events = []
    closed = []
    field = {"map": "function", "filter": "predicate", "filter_map": "function", "tap": "action"}[
        method
    ]

    async def source():
        try:
            yield 1
            yield 2
        finally:
            closed.append(True)

    def replacement(value):
        events.append(("replacement", value))
        return {"map": value + 100, "filter": False, "filter_map": None, "tap": None}[method]

    def first(value):
        events.append(("first", value))
        object.__setattr__(operation, field, replacement)
        return True if method == "filter" else value

    pipeline = getattr(fpstreams.aflow(source()), method)(first)
    operation = pipeline._logical_plan.operations[0]
    expected = {
        "map": [1, 102],
        "filter": [1],
        "filter_map": [1],
        "tap": [1, 2],
    }
    assert await pipeline.to_list() == expected[method]
    assert events == [("first", 1), ("replacement", 2)]
    assert closed == [True]


@pytest.mark.parametrize("ending", ["limit", "failure", "cancel"])
async def test_async_filter_map_fusion_handles_mixed_results_and_cleanup(ending):
    events = []
    empty = []
    started = asyncio.Event()
    failure = LookupError("async filter_map failed")

    async def source():
        try:
            for value in range(8):
                events.append(("pull", value))
                yield value
        finally:
            events.append(("closed",))

    async def resolve(value):
        await asyncio.sleep(0)
        return value

    async def pending():
        started.set()
        await asyncio.Event().wait()

    def mapper(value):
        events.append(("map", value))
        if value == 3:
            if ending == "failure":
                raise failure
            return pending()
        return resolve(empty) if value == 2 else 0 if value == 1 else None

    emitted = []
    query = (
        fpstreams.aflow(source()).map(lambda value: value).filter_map(mapper).tap(emitted.append)
    )
    plan = query.explain().to_dict()
    assert plan["mode"] == "async" and plan["schema_version"] == 1
    if ending == "limit":
        observed = await query.take(2).run_with_report("to_list")
        assert observed.value == [0, []] and observed.value[1] is empty
        assert observed.report.to_dict()["resource_scope"] == "query_owned"
        last = 2
    elif ending == "failure":
        with pytest.raises(LookupError) as caught:
            await query.to_list()
        assert caught.value is failure
        last = 3
    else:
        task = asyncio.create_task(query.to_list())
        await asyncio.wait_for(started.wait(), timeout=2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        last = 3
    assert emitted == [0, []] and emitted[1] is empty
    assert events == [
        event for value in range(last + 1) for event in (("pull", value), ("map", value))
    ] + [("closed",)]


@pytest.mark.asyncio
async def test_cancellation_survives_unprintable_source_cleanup_error() -> None:
    """A real caller cancellation still propagates after the owned source is closed."""
    from fpstreams import aflow

    class BadMessage(Exception):
        def __str__(self) -> str:
            raise RuntimeError("message formatting failed")

    started = asyncio.Event()
    closed = 0

    class Source:
        def __aiter__(self):
            return self

        async def __anext__(self):
            started.set()
            await asyncio.Event().wait()
            raise StopAsyncIteration

        async def aclose(self) -> None:
            nonlocal closed
            closed += 1
            raise BadMessage()

    task = asyncio.create_task(aflow(Source()).to_list())
    await asyncio.wait_for(started.wait(), timeout=5)
    task.cancel("caller cancellation")
    with pytest.raises(asyncio.CancelledError) as captured:
        await asyncio.wait_for(task, timeout=5)
    assert captured.value.args == ("caller cancellation",)
    assert closed == 1
    assert task.cancelled()
    assert any(
        "BadMessage: <exception message unavailable>" in note for note in captured.value.__notes__
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal", ["reduce", "reduce_right", "fold_right"])
async def test_async_reduction_resolves_callbacks_but_preserves_seed_and_singleton(
    terminal: str,
) -> None:
    """Only callback results are awaited, preserving empty and singleton value identity."""
    future = asyncio.get_running_loop().create_future()
    future.set_result("resolved reducer result")
    calls: list[tuple[object, object]] = []

    def combine(left: object, right: object):
        calls.append((left, right))
        return future

    result = await getattr(fpstreams.aflow([]), terminal)(combine, future)
    assert result is future
    assert calls == []
    result = await getattr(fpstreams.aflow([future]), terminal)(combine)
    assert result is future
    assert calls == []
    result = await getattr(fpstreams.aflow([1]), terminal)(combine, future)
    assert result == "resolved reducer result"
    assert calls == ([(future, 1)] if terminal == "reduce" else [(1, future)])


async def test_async_chunk_strict_rejects_incomplete_final_batch_and_closes_source() -> None:
    closed = []

    async def source():
        try:
            for value in range(5):
                yield value
        finally:
            closed.append(True)

    assert await fpstreams.aflow([]).chunk(2, strict=True).to_list() == []
    assert await fpstreams.aflow(range(4)).batch(2, strict=True).to_list() == [(0, 1), (2, 3)]
    assert await fpstreams.aflow(range(5)).chunk(2).to_list() == [(0, 1), (2, 3), (4,)]
    assert await fpstreams.aflow(source()).chunk(2, strict=True).take(1).to_list() == [(0, 1)]
    assert closed == [True]
    with pytest.raises(ValueError, match="incomplete final chunk"):
        await fpstreams.aflow(source()).chunk(2, strict=True).to_list()
    assert closed == [True, True]


def test_async_chunk_explain_uses_batch_counts_and_strict_short_tail() -> None:
    def cardinality(values):
        return values.explain().to_dict()["semantics"]["output"]["cardinality"]

    assert cardinality(fpstreams.aflow(range(5)).chunk(2)) == {"kind": "exact", "value": 3}
    assert cardinality(fpstreams.aflow(range(4)).chunk(2, strict=True)) == {
        "kind": "exact",
        "value": 2,
    }
    assert cardinality(fpstreams.aflow(range(5)).chunk(2, strict=True)) == {
        "kind": "upper_bound",
        "value": 2,
    }
    assert cardinality(fpstreams.aflow(range(5)).filter(bool).chunk(2, strict=True)) == {
        "kind": "upper_bound",
        "value": 2,
    }
    assert cardinality(fpstreams.aflow(range(5)).chunk(2, strict=True).take(1)) == {
        "kind": "upper_bound",
        "value": 1,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "name", ["concat", "zip", "zip_longest", "merge", "combine_latest", "cross", "cartesian"]
)
async def test_async_multisource_explain_uses_live_source_facts(name: str) -> None:
    left = [1]
    right = [2]
    query = getattr(fpstreams.aflow(left), name)(right)
    left.append(3)
    right.append(4)
    explanation = query.explain().to_dict()
    assert explanation["source"]["cardinality"] == {"kind": "exact", "value": 2}
    assert explanation["semantics"]["source"]["cardinality"] == {"kind": "exact", "value": 2}
    if name == "concat":
        assert explanation["semantics"]["output"]["cardinality"] == {"kind": "exact", "value": 4}
        assert await query.to_list() == [1, 3, 2, 4]
    elif name in ("cross", "cartesian"):
        assert explanation["semantics"]["output"]["cardinality"] == {"kind": "exact", "value": 4}
        assert await query.to_list() == [(1, 2), (1, 4), (3, 2), (3, 4)]
    elif name in ("zip", "zip_longest"):
        assert await query.to_list() == [(1, 2), (3, 4)]
    else:
        assert await query.to_list()
    left.clear()
    right.clear()
    empty = query.explain().to_dict()
    assert empty["source"]["cardinality"] == {"kind": "exact", "value": 0}
    assert await query.to_list() == []


def test_async_explain_refreshes_mutable_builtins_and_preserves_unknown_iterators() -> None:
    values = [1, 2]
    query = fpstreams.aflow(values).map(lambda item: item).take(3)
    values.append(3)
    explanation = query.explain().to_dict()
    assert explanation["source"]["cardinality"] == {"kind": "exact", "value": 3}
    assert explanation["semantics"]["output"]["cardinality"] == {"kind": "exact", "value": 3}
    assert fpstreams.flow(values).explain().to_dict()["semantics"]["source"]["cardinality"] == {
        "kind": "exact",
        "value": 3,
    }
    mapping = {"a": 1}
    mapping_query = fpstreams.aflow(mapping)
    mapping["b"] = 2
    assert mapping_query.explain().to_dict()["source"]["cardinality"] == {
        "kind": "exact",
        "value": 2,
    }
    iterator = iter(values)
    assert fpstreams.aflow(iterator).explain().to_dict()["source"]["cardinality"] == {
        "kind": "unknown",
        "value": None,
    }
    assert next(iterator) == 1

    query._logical_plan.source._opener = lambda: fpstreams.aflow([1])._logical_plan.source.open()
    source = query.explain().to_dict()["source"]
    assert source["cardinality"] == {"kind": "unknown", "value": None}
    assert source["ordering"] == "unknown"


@pytest.mark.asyncio
@pytest.mark.parametrize("left,right", [(range(3), range(2)), (range(2), range(3)), ([], [1])])
async def test_strict_zip_explain_bounds_failed_consumption(left, right) -> None:
    expected = {"kind": "upper_bound", "value": min(len(left), len(right))}
    for make in (fpstreams.flow, fpstreams.aflow):
        query = make(left).zip(right, strict=True)
        assert query.explain().to_dict()["semantics"]["output"]["cardinality"] == expected
        assert query.take(1).explain().to_dict()["semantics"]["output"]["cardinality"] == {
            "kind": "upper_bound",
            "value": min(expected["value"], 1),
        }
    with pytest.raises(ValueError):
        fpstreams.flow(left).zip(right, strict=True).to_list()
    with pytest.raises(ValueError):
        await fpstreams.aflow(left).zip(right, strict=True).to_list()
    ordinary = fpstreams.flow(left).zip(right).explain().to_dict()
    assert ordinary["semantics"]["output"]["cardinality"] == {
        "kind": "exact",
        "value": expected["value"],
    }
    equal = fpstreams.aflow([1, 2]).zip([3, 4], strict=True).explain().to_dict()
    assert equal["semantics"]["output"]["cardinality"] == {"kind": "exact", "value": 2}
    empty_equal = fpstreams.flow([]).zip([], strict=True).explain().to_dict()
    assert empty_equal["semantics"]["output"]["cardinality"] == {"kind": "exact", "value": 0}
    if left and right:
        assert fpstreams.flow(left).zip(right, strict=True).take(1).to_list() == [(0, 0)]
        assert await fpstreams.aflow(left).zip(right, strict=True).take(1).to_list() == [(0, 0)]


@pytest.mark.asyncio
@pytest.mark.parametrize("weak", [False, True])
@pytest.mark.parametrize("adapted", [False, True])
@pytest.mark.parametrize("mode", ["direct", "chain", "root", "short"])
async def test_async_concat_custom_iterables_share_cursor_once(weak, adapted, mode):
    class Cursor:
        __slots__ = ("calls", "values")

        def __init__(self):
            self.values, self.calls = iter([1, 2]), 0

        def __iter__(self):
            return self

        def __next__(self):
            return next(self.values)

        def close(self):
            self.calls += 1

    class AsyncCursor(Cursor):
        __slots__ = ()

        def __aiter__(self):
            return self

        async def __anext__(self):
            try:
                return next(self.values)
            except StopIteration:
                raise StopAsyncIteration from None

        async def aclose(self):
            self.close()

    class Source:
        def __init__(self, cursor):
            self.cursor = cursor

        def __iter__(self):
            return self.cursor

    class AsyncSource(Source):
        def __aiter__(self):
            return self.cursor

    cursor_type = Cursor if adapted else AsyncCursor
    if weak:
        cursor_type = type("WeakCursor", (cursor_type,), {"__slots__": ("__weakref__",)})
    cursor = cursor_type()
    source = (Source if adapted else AsyncSource)(cursor)
    if mode == "direct":
        query = fpstreams.aflow([]).concat(source, source)
    else:
        query = fpstreams.aflow(source) if mode == "root" else fpstreams.aflow([]).concat(source)
        if mode == "short":
            query = query.take(1)
        query = query.concat(source)
    assert await query.to_list() == [1, 2]
    assert cursor.calls == 1


@pytest.mark.asyncio
async def test_async_concat_custom_cursor_preserves_close_error():
    failure = OSError("close failed")
    calls = []

    class Cursor:
        def __aiter__(self):
            return self

        async def __anext__(self):
            raise StopAsyncIteration

        async def aclose(self):
            calls.append(True)
            raise failure

    class Source:
        def __init__(self):
            self.cursor = Cursor()

        def __aiter__(self):
            return self.cursor

    source = Source()
    with pytest.raises(OSError) as captured:
        await fpstreams.aflow([]).concat(source, source).to_list()
    assert captured.value is failure
    assert calls == [True]


@pytest.mark.asyncio
async def test_async_concat_nonweak_cursor_is_retained_until_query_finishes():
    events = []

    class Cursor:
        __slots__ = ("done",)

        def __init__(self):
            self.done = False

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self.done:
                raise StopAsyncIteration
            self.done = True
            return 1

        async def aclose(self):
            events.append("close")

        def __del__(self):
            events.append("release")

    class Source:
        def __aiter__(self):
            return Cursor()

    iterator = fpstreams.aflow([]).concat(Source(), [2]).__aiter__()
    assert await anext(iterator) == 1
    assert await anext(iterator) == 2
    assert events == ["close"]
    await iterator.aclose()
    assert events == ["close", "release"]


@pytest.mark.asyncio
@pytest.mark.parametrize("wrapped", [False, True])
async def test_async_concat_releases_exhausted_builtin_repeat_payload(wrapped):
    import itertools
    import weakref

    class Payload:
        pass

    references = []

    class First:
        def __iter__(self):
            payload = Payload()
            references.append(weakref.ref(payload))
            cursor = itertools.repeat(payload, 0)
            return itertools.accumulate(cursor) if wrapped else cursor

    class Last:
        def __iter__(self):
            assert references[-1]() is None
            return iter([2])

    assert await fpstreams.aflow([]).concat(First(), [], Last()).to_list() == [2]


@pytest.mark.asyncio
async def test_async_concat_does_not_probe_custom_close_before_cleanup():
    events = []

    class Cursor:
        def __iter__(self):
            return self

        def __next__(self):
            events.append("pull")
            raise StopIteration

        def __getattribute__(self, name):
            if name == "close":
                events.append("get close")
                return lambda: events.append("close")
            return object.__getattribute__(self, name)

    class Source:
        def __iter__(self):
            return Cursor()

    assert await fpstreams.aflow([]).concat(Source()).to_list() == []
    assert events == ["pull", "get close", "close"]


@pytest.mark.asyncio
async def test_async_concat_does_not_compare_or_hash_custom_cursor_type():
    class Meta(type):
        def __eq__(cls, other):
            raise RuntimeError("type equality")

        def __hash__(cls):
            raise RuntimeError("type hash")

    class Cursor(metaclass=Meta):
        def __init__(self):
            self.closed = 0

        def __iter__(self):
            return self

        def __next__(self):
            raise StopIteration

        def close(self):
            self.closed += 1

    class Source:
        def __init__(self):
            self.cursor = Cursor()

        def __iter__(self):
            return self.cursor

    source = Source()
    assert await fpstreams.aflow([]).concat(source, source).to_list() == []
    assert source.cursor.closed == 1


@pytest.mark.asyncio
async def test_async_concat_registration_failure_closes_opened_cursor(monkeypatch):
    from fpstreams.runtime.iterators import _IteratorIdentities

    failure = MemoryError("registration failed")
    closed = []
    original_add = _IteratorIdentities.add

    class Cursor:
        def __aiter__(self):
            return self

        async def __anext__(self):
            raise StopAsyncIteration

        async def aclose(self):
            closed.append(True)

    class Source:
        def __aiter__(self):
            return Cursor()

    def fail(self, value):
        if isinstance(value, Cursor):
            raise failure
        return original_add(self, value)

    monkeypatch.setattr(_IteratorIdentities, "add", fail)
    with pytest.raises(MemoryError) as captured:
        await fpstreams.aflow([]).concat(Source()).to_list()
    assert captured.value is failure
    assert closed == [True]


@pytest.mark.asyncio
async def test_async_concat_root_registration_failure_closes_root(monkeypatch):
    from fpstreams.runtime.iterators import _IteratorIdentities

    failure = MemoryError("root registration failed")
    closed = []

    class Cursor:
        def __aiter__(self):
            return self

        async def __anext__(self):
            raise StopAsyncIteration

        async def aclose(self):
            closed.append(True)

    def fail(*args):
        raise failure

    monkeypatch.setattr(_IteratorIdentities, "add", fail)
    with pytest.raises(MemoryError) as captured:
        await fpstreams.aflow(Cursor()).concat([]).to_list()
    assert captured.value is failure
    assert closed == [True]


@pytest.mark.asyncio
@pytest.mark.parametrize("items", [[], [1], [1, 2]])
async def test_fold_async_preserves_awaitable_state_and_resolves_each_hook_once(items):
    events = []
    state = asyncio.get_running_loop().create_future()
    state.set_result(10)

    class HookResult:
        def __init__(self, name):
            self.name = name
            self.awaits = 0

        def __await__(self):
            self.awaits += 1
            assert self.awaits == 1
            events.append(self.name)
            if False:
                yield
            return state

    results = []

    def initial():
        result = HookResult("initial")
        results.append(result)
        return result

    def step(accumulator, item):
        assert accumulator is state
        result = HookResult(item)
        results.append(result)
        return result

    query = fpstreams.aflow(items).fold_async(initial, step)
    assert events == []
    assert await query.first() is state
    assert events == ["initial", *items]
    assert all(result.awaits == 1 for result in results)
    assert await query.first() is state
    assert events == ["initial", *items, "initial", *items]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [ValueError("step failed"), asyncio.CancelledError()])
async def test_fold_async_step_failure_closes_source_and_preserves_error(failure):
    events = []

    async def source():
        try:
            yield 1
            events.append("unexpected second pull")
            yield 2
        finally:
            events.append("closed")

    async def initial() -> int:
        return 0

    async def step(state: int, item: int) -> int:
        raise failure

    with pytest.raises(type(failure)) as captured:
        await fpstreams.aflow(source()).fold_async(initial, step).first()
    assert captured.value is failure
    assert events == ["closed"]
