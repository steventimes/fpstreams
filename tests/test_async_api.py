"""AsyncFlow transformations, concurrency, timing, and cleanup."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from itertools import pairwise

import pytest

import fpstreams


def _square(value: int) -> int:
    return value * value


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
