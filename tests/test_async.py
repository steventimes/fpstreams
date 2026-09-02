"""AsyncFlow transforms, bounded concurrency, real-time operators, terminals, and cleanup."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator
from itertools import pairwise

import pytest

import fpstreams


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


# --- Tests consolidated from test_async_api.py ---


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
async def test_async_flow_from_queue_preserves_one_shot_consumption() -> None:
    stop = object()
    queue: asyncio.Queue[object] = asyncio.Queue()
    queue.put_nowait(1)
    queue.put_nowait(stop)
    values = fpstreams.aflow.from_queue(queue, stop=stop)

    assert await values.to_list() == [1]
    with pytest.raises(fpstreams.FlowConsumedError):
        await values.to_list()


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
async def test_async_prefetch_consumer_cancellation_cancels_and_closes_once() -> None:
    source = _PrefetchBlockingSource(block_first=True)
    task = asyncio.create_task(fpstreams.aflow(source).prefetch(1).to_list())
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

    async def values(name: str, delay: float) -> AsyncIterator[str]:
        try:
            for position in range(2):
                pulled[name] += 1
                await asyncio.sleep(delay)
                yield f"{name}-{position}"
        finally:
            closed.add(name)

    result = await fpstreams.aflow(values("slow", 0.02)).merge(values("fast", 0.001)).to_list()

    assert result == ["fast-0", "fast-1", "slow-0", "slow-1"]
    assert pulled == {"slow": 2, "fast": 2}
    assert closed == {"slow", "fast"}

    gate = asyncio.Event()
    buffered = {"left": 0, "right": 0}

    async def blocked(name: str) -> AsyncIterator[str]:
        try:
            while True:
                buffered[name] += 1
                yield name
                await gate.wait()
        finally:
            closed.add(f"blocked-{name}")

    iterator = fpstreams.aflow(blocked("left")).merge(blocked("right")).__aiter__()
    await anext(iterator)
    await asyncio.sleep(0)
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

    async def mapper(value: int) -> AsyncIterator[int]:
        await asyncio.sleep(0)

        async def nested() -> AsyncIterator[int]:
            nonlocal active, maximum
            active += 1
            maximum = max(maximum, active)
            try:
                await asyncio.sleep(0.02 if value == 1 else 0.001)
                yield value
                yield -value
            finally:
                active -= 1

        return nested()

    result = await fpstreams.aflow([1, 2]).merge_map(mapper, concurrency=2).to_list()

    assert result == [2, -2, 1, -1]
    assert maximum == 2
    assert active == 0
    assert await fpstreams.aflow([1, 2]).flat_map_merge(mapper, concurrency=1).to_list() == [
        1,
        -1,
        2,
        -2,
    ]


@pytest.mark.asyncio
async def test_async_merge_map_backpressures_outer_and_cleans_up_on_take() -> None:
    release = asyncio.Event()
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
                await release.wait()
                yield value
            finally:
                inner_closed.add(value)

        return nested()

    iterator = fpstreams.aflow(outer()).merge_map(mapper, concurrency=3).__aiter__()
    first = asyncio.create_task(anext(iterator))
    for _ in range(20):
        if inner_started == {0, 1, 2}:
            break
        await asyncio.sleep(0)
    assert outer_pulled == [0, 1, 2]
    assert inner_started == {0, 1, 2}

    release.set()
    assert await first in {0, 1, 2}
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

    async def left() -> AsyncIterator[str]:
        try:
            yield "left-0"
            await asyncio.sleep(0.03)
            yield "left-1"
        finally:
            closed.add("left")

    async def right() -> AsyncIterator[str]:
        try:
            await asyncio.sleep(0.01)
            yield "right-0"
            await asyncio.sleep(0.01)
            yield "right-1"
            await asyncio.sleep(0.02)
            yield "right-2"
        finally:
            closed.add("right")

    result = await fpstreams.aflow(left()).combine_latest(right()).to_list()

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

    class Source(AsyncIterator[int]):
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

    source = Source()

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

    class Source(AsyncIterator[int]):
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

    source = Source()
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
    import fpstreams.execution.async_ops as async_ops

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
    assert 1 <= execution.report.peak_owned_async_tasks <= 3


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
    from fpstreams.execution import async_timers

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


@pytest.mark.asyncio
async def test_session_window_consumer_cancellation_closes_pending_work() -> None:
    source = _PrefetchBlockingSource()
    task = asyncio.create_task(fpstreams.aflow(source).session_window(10, max_count=10).to_list())
    await source.pull_started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

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
