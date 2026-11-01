"""streams flow engines regression contracts."""

from __future__ import annotations

import math
import subprocess
import sys
import threading
import time
from collections.abc import AsyncIterator, Callable, Iterator
from pathlib import Path
from types import FrameType
from typing import Any

import pytest

import fpstreams
from fpstreams import flow


def _engine_square(value: int) -> int:
    return value * value


def test_map_batches_is_lazy_preserves_items_and_includes_the_short_tail() -> None:
    first, second, third = object(), object(), object()
    seen = []
    calls = []

    def source():
        for value in (first, second, third):
            seen.append(value)
            yield value

    def summarize(batch):
        assert type(batch) is tuple
        calls.append(batch)
        return {"items": batch}

    query = flow(source()).map_batches(summarize, size=2)
    assert seen == calls == []
    assert query.to_list() == [{"items": (first, second)}, {"items": (third,)}]
    assert seen == [first, second, third]
    assert calls == [(first, second), (third,)]


def test_map_batches_keeps_each_result_whole_and_does_not_call_empty_input() -> None:
    assert flow(range(5)).map_batches(list, size=2).to_list() == [[0, 1], [2, 3], [4]]
    assert flow(range(5)).map_batches(list, size=2).flat_map(iter).to_list() == [0, 1, 2, 3, 4]
    assert flow(range(3)).map_batches(lambda batch: None, size=2).to_list() == [None, None]

    def reject_empty(batch):
        raise AssertionError("empty input called the batch callback")

    assert flow([]).map_batches(reject_empty, size=2).to_list() == []


def test_map_batches_validates_size_without_opening_the_source() -> None:
    seen = []

    def source():
        seen.append(True)
        yield 1

    query = flow(source())
    for size in (0, -1):
        with pytest.raises(ValueError):
            query.map_batches(sum, size=size)
    for size in (1.5, "2", None):
        with pytest.raises(TypeError):
            query.map_batches(sum, size=size)
    assert seen == []

    class Size:
        def __index__(self):
            return 2

    assert query.map_batches(sum, size=Size()).to_list() == [1]


def test_map_batches_callback_failure_preserves_error_and_closes_source() -> None:
    closed = []
    failure = ValueError("batch failed")

    def source():
        try:
            yield from range(7)
        finally:
            closed.append(True)

    def work(batch):
        if batch[0] == 2:
            raise failure
        return sum(batch)

    with pytest.raises(ValueError, match="batch failed") as caught:
        flow(source()).map_batches(work, size=2).to_list()
    assert caught.value is failure
    assert closed == [True]


def test_map_batches_parallel_buffer_counts_batches_and_borrows_the_pool() -> None:
    from concurrent.futures import ThreadPoolExecutor

    seen = []
    closed = []

    def source():
        try:
            for value in range(100):
                seen.append(value)
                yield value
        finally:
            closed.append(True)

    with ThreadPoolExecutor(max_workers=2) as pool:
        query = flow(source()).parallel(executor=pool, buffer=2).map_batches(sum, size=3)
        assert seen == []
        assert query.take(1).to_list() == [3]
        assert seen == [0, 1, 2, 3, 4, 5]
        assert closed == [True]
        assert pool.submit(sum, (4, 5)).result(timeout=5) == 9
        assert flow(range(5)).parallel(executor=pool, buffer=1).sequential().map_batches(
            sum, size=2
        ).to_list() == [1, 5, 4]


def test_map_batches_spawn_pool_preserves_batch_order_and_can_be_reused() -> None:
    from concurrent.futures import ProcessPoolExecutor
    from multiprocessing import get_context

    with ProcessPoolExecutor(max_workers=2, mp_context=get_context("spawn")) as pool:
        query = flow(range(7)).parallel(executor=pool, buffer=2).map_batches(sum, size=3)
        assert query.to_list() == [3, 12, 6]
        assert query.to_list() == [3, 12, 6]
        assert pool.submit(sum, (10, 11)).result(timeout=5) == 21


@pytest.mark.parametrize("method", ["map_parallel", "parallel"])
@pytest.mark.parametrize("field", ["workers", "buffer"])
@pytest.mark.parametrize(
    "value",
    [float("nan"), float("inf"), 1.5, 2.0],
    ids=["nan", "infinity", "fractional", "integral-float"],
)
def test_parallel_configuration_rejects_noninteger_bounds_before_source_open(
    method, field, value
) -> None:
    opened = []
    mapped = []

    def source():
        opened.append(True)
        return [1, 2]

    def mapper(value):
        mapped.append(value)
        return value * 2

    values = flow.defer(source)
    arguments = {"backend": "thread", field: value}
    positional = (mapper,) if method == "map_parallel" else ()
    with pytest.raises(TypeError):
        getattr(values, method)(*positional, **arguments)
    assert opened == mapped == []


@pytest.mark.parametrize("method", ["map_parallel", "parallel"])
@pytest.mark.parametrize("field", ["workers", "buffer"])
def test_parallel_configuration_normalizes_integer_bounds_once_before_source_open(
    method, field
) -> None:
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

        def __le__(self, other):
            return self.value <= other

        def __rmul__(self, other):
            return other * self.value

    def source():
        opened.append(True)
        return [1, 2, 3]

    bound = Capacity()
    reference = weakref.ref(bound)
    arguments = {"backend": "thread", "workers": 1, field: bound}
    values = flow.defer(source)
    if method == "map_parallel":
        values = values.map_parallel(lambda value: value * 2, **arguments)
    else:
        values = values.parallel(**arguments).map(lambda value: value * 2)
    del arguments
    bound.value = 0
    del bound
    assert (calls, reference() is None, opened) == ([True], True, [])
    assert values.to_list() == [2, 4, 6]
    assert opened == [True]


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


@pytest.mark.parametrize("ordered", [True, False])
def test_parallel_maps_borrow_an_executor_without_shutting_it_down(ordered) -> None:
    from concurrent.futures import ThreadPoolExecutor

    class TrackedPool(ThreadPoolExecutor):
        shutdowns = 0

        def shutdown(self, *args, **kwargs):
            self.shutdowns += 1
            return super().shutdown(*args, **kwargs)

    with TrackedPool(max_workers=2) as pool:
        query = flow(range(8)).map_parallel(
            _engine_square, executor=pool, buffer=3, ordered=ordered
        )
        assert query.explain().to_dict()["stages"][0]["engine"] == "thread"
        expected = [value * value for value in range(8)]
        result = query.to_list()
        assert (result if ordered else sorted(result)) == expected
        repeated = query.to_list()
        assert (repeated if ordered else sorted(repeated)) == expected
        following = flow(range(8)).parallel(executor=pool, buffer=3, ordered=ordered)
        result = following.map(_engine_square).to_list()
        assert (result if ordered else sorted(result)) == expected
        assert following.sequential().map(_engine_square).to_list() == expected
        assert pool.shutdowns == 0
        assert pool.submit(_engine_square, 9).result() == 81


def test_borrowed_executor_cleanup_waits_for_own_work_only() -> None:
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    blocked = Event()
    started = Event()

    def unrelated_work():
        started.set()
        return blocked.wait(5)

    with ThreadPoolExecutor(max_workers=2) as pool:
        unrelated = pool.submit(unrelated_work)
        assert started.wait(5)
        try:
            assert flow(range(10)).map_parallel(_engine_square, executor=pool, buffer=2).take(
                1
            ).to_list() == [0]
            assert not unrelated.done()
            assert pool.submit(_engine_square, 3).result(timeout=5) == 9
        finally:
            blocked.set()


@pytest.mark.parametrize("ordered", [True, False])
def test_borrowed_executor_mapping_failure_closes_source_and_preserves_pool(ordered) -> None:
    from concurrent.futures import ThreadPoolExecutor

    closed = []
    failure = ValueError("borrowed mapper failed")

    def source():
        try:
            yield from range(6)
        finally:
            closed.append(True)

    def work(value):
        if value == 1:
            raise failure
        return value

    with ThreadPoolExecutor(max_workers=2) as pool:
        with pytest.raises(ValueError, match="borrowed mapper failed") as caught:
            flow(source()).map_parallel(work, executor=pool, buffer=2, ordered=ordered).to_list()
        assert caught.value is failure
        assert closed == [True]
        assert pool.submit(_engine_square, 3).result() == 9


@pytest.mark.parametrize("method", ["map_parallel", "parallel"])
def test_borrowed_executor_settings_are_validated_and_remain_lazy(method, monkeypatch) -> None:
    from concurrent.futures import Future, ThreadPoolExecutor
    from importlib import import_module

    flow_module = import_module("fpstreams.streams.flow")

    monkeypatch.setattr(flow_module.os, "cpu_count", lambda: 3)
    with ThreadPoolExecutor(max_workers=2) as pool:
        arguments = (_engine_square,) if method == "map_parallel" else ()
        configure = getattr(flow(range(4)), method)
        with pytest.raises(ValueError, match=r"workers.*executor"):
            configure(*arguments, executor=pool, workers=2)
        with pytest.raises(TypeError, match="executor"):
            configure(*arguments, executor=object())
        with pytest.raises(ValueError, match="buffer"):
            configure(*arguments, executor=pool, buffer=0)
        submitted = []

        def submit(function, *args, **kwargs):
            submitted.append(args)
            result = Future()
            result.set_result(function(*args, **kwargs))
            return result

        monkeypatch.setattr(pool, "submit", submit)
        configured = configure(*arguments, executor=pool)
        query = configured if method == "map_parallel" else configured.map(_engine_square)
        assert submitted == []
        assert query._pipeline.operations[-1].buffer == 6
        assert query.to_list() == [0, 1, 4, 9]
        assert len(submitted) == 4


def test_borrowed_spawn_executor_keeps_process_backend_and_remains_reusable() -> None:
    from concurrent.futures import ProcessPoolExecutor
    from multiprocessing import get_context

    with ProcessPoolExecutor(max_workers=2, mp_context=get_context("spawn")) as pool:
        query = flow(range(6)).map_parallel(_engine_square, executor=pool, buffer=2)
        assert query.explain().to_dict()["stages"][0]["engine"] == "process"
        assert query.to_list() == [0, 1, 4, 9, 16, 25]
        assert flow(range(3)).parallel(executor=pool, buffer=2).map(_engine_square).to_list() == [
            0,
            1,
            4,
        ]
        assert pool.submit(_engine_square, 8).result() == 64


def test_borrowed_executor_early_exit_cancels_queued_work_within_buffer() -> None:
    from concurrent.futures import Executor, Future

    class PendingPool(Executor):
        def __init__(self):
            self.futures = []

        def submit(self, function, *args, **kwargs):
            result = Future()
            if not self.futures:
                result.set_result(function(*args, **kwargs))
            self.futures.append(result)
            return result

        def shutdown(self, *args, **kwargs):
            raise AssertionError("borrowed executor was shut down")

    seen = []
    closed = []

    def source():
        try:
            for value in range(20):
                seen.append(value)
                yield value
        finally:
            closed.append(True)

    pool = PendingPool()
    query = flow(source()).map_parallel(_engine_square, executor=pool, buffer=2)
    assert query.explain().to_dict()["stages"][0]["engine"] == "executor"
    assert query.take(1).to_list() == [0]
    assert seen == [0, 1]
    assert closed == [True]
    assert len(pool.futures) == 2
    assert pool.futures[1].cancelled()


def test_borrowed_executor_early_exit_waits_for_own_running_callback() -> None:
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event, Thread

    started = Event()
    release = Event()
    finished = Event()
    closed = []
    result = []

    def source():
        try:
            yield from (0, 1)
        finally:
            closed.append(True)

    def work(value):
        if value == 0:
            assert started.wait(5)
        else:
            started.set()
            assert release.wait(5)
        return value

    with ThreadPoolExecutor(max_workers=2) as pool:

        def consume():
            result.extend(
                flow(source()).map_parallel(work, executor=pool, buffer=2).take(1).to_list()
            )
            finished.set()

        consumer = Thread(target=consume)
        consumer.start()
        try:
            assert started.wait(5)
            assert not finished.wait(0.05)
            assert closed == []
        finally:
            release.set()
            consumer.join(timeout=5)
        assert not consumer.is_alive()
        assert finished.is_set()
        assert result == [0]
        assert closed == [True]


@pytest.mark.parametrize("buffer", [1, 2])
def test_borrowed_executor_interrupted_result_waits_before_closing_source(
    buffer, monkeypatch
) -> None:
    from concurrent.futures import Future, ThreadPoolExecutor
    from threading import Event, Thread

    from fpstreams.execution import sync_ops

    started = Event()
    release = Event()
    cleanup_started = Event()
    callback_finished = Event()
    closed = []
    errors = []
    interrupt = KeyboardInterrupt("interrupted result wait")
    original_result = Future.result
    original_wait = sync_ops.wait
    interrupted = False

    class Source:
        emitted = False

        def __iter__(self):
            return self

        def __next__(self):
            if self.emitted:
                raise StopIteration
            self.emitted = True
            return 1

        def close(self):
            closed.append(callback_finished.is_set())

    def work(value):
        started.set()
        assert release.wait(5)
        callback_finished.set()
        return value

    def result(future, timeout=None):
        nonlocal interrupted
        if not interrupted:
            interrupted = True
            assert started.wait(5)
            raise interrupt
        return original_result(future, timeout=timeout)

    def wait(futures, *args, **kwargs):
        cleanup_started.set()
        return original_wait(futures, *args, **kwargs)

    monkeypatch.setattr(Future, "result", result)
    monkeypatch.setattr(sync_ops, "wait", wait)
    with ThreadPoolExecutor(max_workers=1) as pool:

        def consume():
            try:
                flow(Source()).map_parallel(work, executor=pool, buffer=buffer).to_list()
            except BaseException as error:
                errors.append(error)

        consumer = Thread(target=consume)
        consumer.start()
        try:
            assert cleanup_started.wait(5)
            assert closed == []
            assert consumer.is_alive()
        finally:
            release.set()
            consumer.join(timeout=5)
        assert not consumer.is_alive()
        assert errors == [interrupt]
        assert closed == [True]
        assert pool.submit(_engine_square, 3).result(timeout=5) == 9


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
    """Opaque one-shot sources never expose their optional hint to a materializer."""
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

    assert flow(Values()).with_engine("python").to_list() == [0, 1, 2, 3]
    assert hints == []
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
    from fpstreams.expressions.scalar import _compile_float_evaluator, _compile_int_evaluator

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


def test_auto_selected_python_scalar_materialization_bypasses_stage_callbacks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Auto plans kept in Python must reuse the closed scalar loop."""
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
    query = fpstreams.flow(range(4_096)).filter(fpstreams.fitem >= 2.0).map(fpstreams.fitem * 0.5)

    assert query.explain(terminal="list").to_dict()["selected_engine"] == "python"
    result = query.to_list()

    assert len(result) == 4_094
    assert result[:3] == [1.0, 1.5, 2.0]
    assert result[-1] == 2_047.5
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


@pytest.mark.parametrize("coerce", [False, True])
def test_float_expression_rejects_custom_metaclass_before_conversion(coerce: bool) -> None:
    from fpstreams.expressions.scalar import FExpr

    calls: list[str] = []

    class Meta(type):
        def __eq__(cls, other: object) -> bool:
            calls.append("class equality")
            return True

        __hash__ = type.__hash__

    class Numeric(metaclass=Meta):
        def __float__(self) -> float:
            calls.append("float")
            return 7.0

    value = Numeric()
    message = "unsupported float expression operand" if coerce else "accept int or float constants"
    with pytest.raises(TypeError, match=message):
        if coerce:
            fpstreams.fitem + value
        else:
            FExpr.constant(value)  # type: ignore[arg-type]
    assert calls == []


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
    assert fpstreams.Flow.average is fpstreams.Flow.mean

    empty = flow(range(3)).filter(fpstreams.item < 0).with_engine("native")
    assert empty.mean() is None
    assert empty.variance() is None
    assert flow([1]).variance() is None
    with pytest.raises(ValueError, match="ddof"):
        integers.std(ddof=-1)


def test_compensated_mean_values_preserve_numeric_and_nonfinite_contracts() -> None:
    """The mean-only Python reducer accepts the same values as online statistics."""
    from decimal import Decimal
    from fractions import Fraction

    from fpstreams.collecting.statistics import compensated_mean

    class FloatOnly:
        def __float__(self) -> float:
            return 2.5

    assert compensated_mean([]) is None
    assert compensated_mean([1e16, 1.0, -1e16]) == pytest.approx(1 / 3)
    assert compensated_mean([Fraction(1, 3), Decimal("0.2")]) == pytest.approx(4 / 15)
    assert math.isnan(compensated_mean([math.inf, -math.inf]))
    with pytest.raises(TypeError, match="real numeric values"):
        compensated_mean([FloatOnly()])
    with pytest.raises(TypeError, match="real numeric values"):
        compensated_mean([1 + 0j])


def test_flow_mean_rejects_non_numeric_items_and_closes_the_source() -> None:
    """A failed one-shot mean retains the terminal's iterator ownership contract."""
    closed = False

    def values() -> Iterator[object]:
        nonlocal closed
        try:
            yield 1
            yield "not numeric"
        finally:
            closed = True

    with pytest.raises(TypeError, match="numeric"):
        fpstreams.flow(values()).mean()
    assert closed is True


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


@pytest.mark.parametrize("terminal", ["find", "nth0", "nth_last"])
@pytest.mark.parametrize("with_default", [False, True])
def test_find_and_nth_preserve_source_empty_error_and_close(terminal, with_default):
    error = fpstreams.EmptyFlowError("from source")
    closed = []

    def source():
        try:
            yield 1
            raise error
        finally:
            closed.append(True)

    query = flow(source()).with_engine("python")
    args = (lambda _: False,) if terminal == "find" else ((2,) if terminal == "nth0" else (-1,))
    if with_default:
        args += (None,)
    with pytest.raises(fpstreams.EmptyFlowError) as caught:
        getattr(query, "find" if terminal == "find" else "nth")(*args)
    assert caught.value is error
    assert closed == [True]


def test_find_preserves_predicate_empty_error_and_none_match():
    error = fpstreams.EmptyFlowError("from predicate")

    def predicate(_):
        raise error

    with pytest.raises(fpstreams.EmptyFlowError) as caught:
        flow([None]).find(predicate)
    assert caught.value is error
    assert flow([None]).find(lambda value: value is None) is None


@pytest.mark.parametrize("terminal", ["min", "max"])
@pytest.mark.parametrize("failure", ["source", "key", "comparison"])
def test_extremes_preserve_value_errors_and_close(terminal, failure):
    error = ValueError(failure)
    closed = []

    class BadComparison:
        def __lt__(self, other):
            raise error

        def __gt__(self, other):
            raise error

    def source():
        try:
            yield BadComparison() if failure == "comparison" else 1
            if failure == "source":
                raise error
            yield BadComparison() if failure == "comparison" else 2
        finally:
            closed.append(True)

    def key(value):
        if failure == "key":
            raise error
        return value

    with pytest.raises(ValueError) as caught:
        getattr(flow(source()).with_engine("python"), terminal)(key=key)
    assert caught.value is error
    assert closed == [True]
    with pytest.raises(fpstreams.EmptyFlowError, match=terminal):
        getattr(flow(iter([])).with_engine("python"), terminal)()
    assert getattr(flow([3, 1, 2]), terminal)() == (1 if terminal == "min" else 3)


@pytest.mark.parametrize("engine", ["auto", "python"])
def test_identity_sequences_use_direct_indexed_terminals(
    engine: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exact retained sequences should answer indexed terminals without a linear executor."""
    from fpstreams.streams import flow_terminals

    def reject_open(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("identity indexed terminal opened a linear executor")

    monkeypatch.setattr(fpstreams.Flow, "_open", reject_open)
    monkeypatch.setattr(flow_terminals, "_open_terminal_values", reject_open)

    marker = object()
    for source in ([0, 1, marker], (0, 1, marker)):
        values = flow(source).with_engine(engine)
        assert values.last() is marker
        assert values.nth(0) == 0
        assert values.nth(-2) == 1
        assert values.nth(99, "missing") == "missing"
        with pytest.raises(fpstreams.EmptyFlowError, match=r"nth\(99\)"):
            values.nth(99)

    numbers = flow(range(10, 30, 2)).with_engine(engine)
    assert numbers.nth(3) == 16
    assert numbers.nth(-2) == 26
    assert numbers.index_of(24) == 7
    assert numbers.index_of(25) is None

    empty = flow([]).with_engine(engine)
    assert empty.last("missing") == "missing"
    assert empty.nth(-2, "missing") == "missing"
    with pytest.raises(fpstreams.EmptyFlowError, match="last"):
        empty.last()

    reported = flow((1, 2, 3)).with_engine(engine).run_with_report("nth", -2)
    assert reported.value == 2
    assert reported.report.compiler_engine == "not_compiled"
    assert reported.report.strategy == "python_direct"


def test_identity_indexed_shortcuts_preserve_flow_subclass_overrides() -> None:
    """Retained indexing must not bypass dynamically dispatched Flow methods."""

    class CustomFlow(fpstreams.Flow[int]):
        def drop(self, count: int) -> fpstreams.Flow[int]:
            del count
            return flow([99])

        def last(self, default: object = None) -> int:
            del default
            return 77

    values = CustomFlow([1, 2, 3])

    assert values.nth(0) == 99
    assert values.nth(-1) == 77


def test_identity_nth_preserves_exact_flow_method_monkeypatches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Direct nth must retain runtime replacement of its public delegation points."""
    with monkeypatch.context() as patch:
        patch.setattr(fpstreams.Flow, "drop", lambda _self, _count: flow([99]))
        assert flow([1, 2, 3]).nth(0) == 99

    with monkeypatch.context() as patch:
        patch.setattr(fpstreams.Flow, "last", lambda _self, _default=None: 77)
        assert flow([1, 2, 3]).nth(-1) == 77


def test_identity_indexed_terminals_preserve_active_failpoints() -> None:
    """Instrumentation must restore the canonical source-open boundary."""
    from fpstreams.runtime.failpoints import failpoint

    with (
        failpoint("source.open.after", RuntimeError("canonical indexed terminal")),
        pytest.raises(RuntimeError, match="canonical indexed terminal"),
    ):
        flow([1, 2, 3]).last()


def test_index_of_custom_metaclass_uses_value_equality() -> None:
    calls: list[int] = []

    class Meta(type):
        def __eq__(cls, other: object) -> bool:
            raise RuntimeError("index_of compared a metaclass")

        __hash__ = type.__hash__

    class Target(metaclass=Meta):
        def __eq__(self, other: object) -> bool:
            calls.append(other)  # type: ignore[arg-type]
            return other == 2

    target = Target()
    assert flow(range(4)).index_of(target) == 2  # type: ignore[arg-type]
    assert calls == [0, 1, 2]


def test_identity_index_of_builds_one_iteration_query(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Range preflight must not add a discarded query to canonical sequence scans."""
    original_query = fpstreams.Flow._query
    queries: list[str] = []

    def observed_query(
        instance: fpstreams.Flow[object],
        name: str,
        *arguments: object,
        **options: object,
    ) -> object:
        queries.append(name)
        return original_query(instance, name, *arguments, **options)

    monkeypatch.setattr(fpstreams.Flow, "_query", observed_query)
    for source in ([1, 2, 3], (1, 2, 3), range(1, 4)):
        queries.clear()
        assert flow(source).index_of(2) == 1
        assert queries == ["iterate"]


def test_parallel_identity_indexed_terminal_keeps_its_compiled_plan() -> None:
    """A parallel request must not be erased by retained direct indexing."""
    result = flow(range(5)).parallel(backend="thread", workers=1).run_with_report("nth", 2)

    assert result.value == 2
    assert result.report.compiler_engine != "not_compiled"
    assert result.report.strategy != "python_direct"


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


def test_to_numpy_int64_uses_native_exact_integer_pack_and_returns_owned_array(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The scalar NumPy boundary packs exact integers without changing array ownership."""
    np = pytest.importorskip("numpy")
    from fpstreams import _native

    endpoint = _native.pack_i64_exact_sequence_v1
    calls = 0

    def tracked(values: object) -> object:
        nonlocal calls
        calls += 1
        return endpoint(values)

    monkeypatch.setattr(_native, "pack_i64_exact_sequence_v1", tracked)
    source = list(range(4_096))
    result = flow(source).to_numpy(dtype=np.int64)

    assert result.dtype == np.dtype(np.int64)
    assert result.tolist() == source
    assert result.flags.owndata is True
    assert result.flags.writeable is True
    result[0] = 99
    assert source[0] == 0
    assert calls == 1


def test_to_numpy_int64_native_pack_declines_incompatible_values_atomically(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Speculative packing falls back to NumPy without invoking integer protocols twice."""
    np = pytest.importorskip("numpy")
    from fpstreams import _native

    endpoint = _native.pack_i64_exact_sequence_v1
    calls = 0

    class IntegerSubclass(int):
        index_calls = 0

        def __index__(self) -> int:
            type(self).index_calls += 1
            return int(self)

    def tracked(values: object) -> object:
        nonlocal calls
        calls += 1
        return endpoint(values)

    monkeypatch.setattr(_native, "pack_i64_exact_sequence_v1", tracked)
    values: list[object] = [1] * 2_048 + [IntegerSubclass(7)] + [1] * 2_047
    expected = np.asarray(values, dtype=np.int64)
    result = flow(values).to_numpy(dtype=np.int64)

    assert np.array_equal(result, expected)
    assert calls == 1
    assert IntegerSubclass.index_calls == 0


def test_to_numpy_int64_skips_a_guaranteed_native_boundary_decline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Known-incompatible edge values avoid a redundant whole-list native scan."""
    np = pytest.importorskip("numpy")
    from fpstreams import _native

    def unexpected(_values: object) -> object:
        raise AssertionError("an incompatible boundary reached the exact-integer packer")

    monkeypatch.setattr(_native, "pack_i64_exact_sequence_v1", unexpected)
    values: list[object] = [1] * 4_095 + [2.5]

    result = flow(values).to_numpy(dtype=np.int64)

    assert np.array_equal(result, np.asarray(values, dtype=np.int64))


def test_to_numpy_skips_unavailable_or_irrelevant_native_integer_pack(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Other dtypes and older extension modules retain the canonical NumPy conversion."""
    np = pytest.importorskip("numpy")
    from fpstreams import _native

    def unexpected(_values: object) -> object:
        raise AssertionError("a non-int64 conversion reached the native packer")

    monkeypatch.setattr(_native, "pack_i64_exact_sequence_v1", unexpected)
    values = list(range(4_096))
    floats = flow(values).to_numpy(dtype=np.float32)
    assert np.array_equal(floats, np.asarray(values, dtype=np.float32))

    monkeypatch.delattr(_native, "pack_i64_exact_sequence_v1")
    integers = flow(values).to_numpy(dtype=np.int64)
    assert np.array_equal(integers, np.asarray(values, dtype=np.int64))


def test_to_numpy_parses_an_explicit_dtype_descriptor_once() -> None:
    """Fallback reuses NumPy's resolved dtype instead of replaying descriptor hooks."""
    np = pytest.importorskip("numpy")

    class OneShotDType(dict[str, object]):
        def __init__(self) -> None:
            super().__init__(names=["value"], formats=["i8"])
            self.calls = 0

        def __getitem__(self, key: str) -> object:
            self.calls += 1
            if self.calls > 7:
                raise RuntimeError("dtype parsed twice")
            return super().__getitem__(key)

    dtype = OneShotDType()
    result = flow([(1,), (2,)]).to_numpy(dtype=dtype)

    assert result.dtype == np.dtype([("value", np.int64)])
    assert result["value"].tolist() == [1, 2]
    assert dtype.calls == 7


def test_numpy_affine_comparison_pair_sum_preserves_float_edges_and_fallbacks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The direct f64-buffer sum loop keeps public comparison and terminal contracts."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution import native
    from fpstreams.planning.native import select_terminal_engine

    cancellation = np.array(
        [1e16, 1.0, -1e16, math.nan, math.inf, -math.inf],
        dtype=np.float64,
    )
    finite = (
        fpstreams.flow.from_numpy(cancellation)
        .map(fpstreams.fitem * 1.0 + 0.0)
        .filter((fpstreams.fitem > -math.inf) & (fpstreams.fitem < math.inf))
        .with_engine("native")
    )
    assert finite.sum() == finite.with_engine("python").sum()

    values = np.array([-2.0, -1.0, -0.0, 0.0, 1.0, 2.0, math.nan], dtype=np.float64)
    reversed_bounds = (
        fpstreams.flow.from_numpy(values)
        .map(fpstreams.fitem * 1.0 + 0.0)
        .filter((-1.0 < fpstreams.fitem) & (2.0 > fpstreams.fitem))  # noqa: SIM300
        .with_engine("native")
    )
    assert reversed_bounds.sum() == 1.0
    inclusive_bounds = (
        fpstreams.flow.from_numpy(values)
        .map(fpstreams.fitem * 1.0 + 0.0)
        .filter((-1.0 <= fpstreams.fitem) & (2.0 >= fpstreams.fitem))  # noqa: SIM300
        .with_engine("native")
    )
    assert inclusive_bounds.sum() == 2.0
    equality = (
        fpstreams.flow.from_numpy(values)
        .map(fpstreams.fitem * 1.0 + 0.0)
        .filter((0.0 != fpstreams.fitem) & (1.0 == fpstreams.fitem))  # noqa: SIM300
        .with_engine("native")
    )
    assert equality.sum() == 1.0

    zeros = (
        fpstreams.flow.from_numpy(np.array([-0.0, 0.0], dtype=np.float64))
        .map(fpstreams.fitem * 1.0 + 0.0)
        .filter((fpstreams.fitem == 0.0) & (fpstreams.fitem <= 0.0))
        .with_engine("native")
        .sum()
    )
    assert math.copysign(1.0, zeros) == 1.0
    empty = (
        fpstreams.flow.from_numpy(values)
        .map(fpstreams.fitem * 1.0 + 0.0)
        .filter((fpstreams.fitem < 0.0) & (fpstreams.fitem > 0.0))
        .with_engine("native")
        .sum()
    )
    assert type(empty) is int
    assert empty == 0
    rejected = (
        fpstreams.flow.from_numpy(values)
        .map(fpstreams.fitem * 1.0 + 0.0)
        .reject((fpstreams.fitem > -1.0) & (fpstreams.fitem < 2.0))
        .with_engine("native")
        .sum()
    )
    assert math.isnan(rejected)

    unmatched = (
        fpstreams.flow.from_numpy(values[:-1])
        .map(fpstreams.fitem * 1.0 + 0.0)
        .filter((fpstreams.fitem < -1.0) | (fpstreams.fitem > 1.0))
        .with_engine("native")
    )
    assert unmatched.sum() == 0.0
    with pytest.raises(ZeroDivisionError):
        (
            fpstreams.flow.from_numpy(np.array([1.0, 2.0], dtype=np.float64))
            .map(fpstreams.fitem / (fpstreams.fitem - 2.0))
            .filter((fpstreams.fitem > -math.inf) & (fpstreams.fitem < math.inf))
            .with_engine("native")
            .sum()
        )

    monkeypatch.setattr(native.sys, "version_info", (3, 11))
    decision = select_terminal_engine(finite._pipeline, "sum")
    assert decision.program is not None
    assert native.execute_terminal(decision.program, "sum") == 0.0


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


@pytest.mark.parametrize("engine", ["auto", "native"])
@pytest.mark.parametrize(
    "source",
    [
        range(0),
        range(7),
        range(9, -8, -3),
        range(-11, 14, 4),
        range(2**62, 2**62 + 10),
        range(-(2**63), -(2**63) + 10),
    ],
)
def test_identity_range_metadata_terminals_preserve_exact_results(
    source: range,
    engine: str,
) -> None:
    """Constant-time range terminals retain arbitrary-width Python totals and empty results."""
    values = fpstreams.flow(source).with_engine(engine)

    assert values.sum() == sum(source)
    assert values.last(None) == (source[-1] if source else None)
    if source:
        assert values.min() == min(source)
        assert values.max() == max(source)
        assert values.aggregate(total=fpstreams.agg.sum()) == {"total": sum(source)}
        assert values.aggregate(low=fpstreams.agg.min()) == {"low": min(source)}
        assert values.aggregate(high=fpstreams.agg.max()) == {"high": max(source)}
        assert values.aggregate(tail=fpstreams.agg.last()) == {"tail": source[-1]}
    else:
        assert values.aggregate(total=fpstreams.agg.sum()) == {"total": 0}
        assert values.aggregate(low=fpstreams.agg.min()) == {"low": None}
        assert values.aggregate(high=fpstreams.agg.max()) == {"high": None}
        assert values.aggregate(tail=fpstreams.agg.last()) == {"tail": None}
        with pytest.raises(fpstreams.EmptyFlowError):
            values.min()
        with pytest.raises(fpstreams.EmptyFlowError):
            values.max()


def test_identity_range_metadata_terminals_do_not_enter_the_linear_rust_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Scalar and one-field aggregate terminals share one non-scanning range path."""
    from fpstreams.execution import native

    extension = native._native
    linear_calls = 0
    aggregate_calls = 0

    class TrackedNative:
        def terminal_i64_range(self, *args: object) -> object:
            nonlocal linear_calls
            linear_calls += 1
            return extension.terminal_i64_range(*args)

        def aggregate_i64_range_masked(self, *args: object) -> object:
            nonlocal aggregate_calls
            aggregate_calls += 1
            return extension.aggregate_i64_range_masked(*args)

        def __getattr__(self, name: str) -> object:
            return getattr(extension, name)

    monkeypatch.setattr(native, "_native", TrackedNative())
    source = range(2**62, 2**62 + 10)

    for engine in ("auto", "native"):
        values = fpstreams.flow(source).with_engine(engine)
        assert values.sum() == sum(source)
        assert values.min() == min(source)
        assert values.max() == max(source)
        assert values.last() == source[-1]
        assert values.aggregate(total=fpstreams.agg.sum()) == {"total": sum(source)}
        assert values.aggregate(low=fpstreams.agg.min()) == {"low": min(source)}
        assert values.aggregate(high=fpstreams.agg.max()) == {"high": max(source)}
        assert values.aggregate(tail=fpstreams.agg.last()) == {"tail": source[-1]}

    assert linear_calls == 0
    assert aggregate_calls == 0


def test_identity_range_metadata_ignores_a_replaced_builtin_len(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A metadata shortcut follows the exact range, not mutable builtins state."""
    import builtins

    source = range(1, 33)
    values = fpstreams.flow(source).with_engine("native")
    with monkeypatch.context() as patch:
        patch.setattr(builtins, "len", lambda _value: 1)
        total = values.sum()
        aggregate = values.aggregate(total=fpstreams.agg.sum())

    assert total == 528
    assert aggregate == {"total": 528}


def test_range_metadata_terminal_leaves_transformed_and_instrumented_plans_canonical(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Operations and failpoints retain their established pull and execution boundaries."""
    from fpstreams.execution import native
    from fpstreams.runtime.failpoints import failpoint

    extension = native._native
    linear_calls = 0

    class TrackedNative:
        def terminal_i64_range(self, *args: object) -> object:
            nonlocal linear_calls
            linear_calls += 1
            return extension.terminal_i64_range(*args)

        def __getattr__(self, name: str) -> object:
            return getattr(extension, name)

    monkeypatch.setattr(native, "_native", TrackedNative())

    transformed = fpstreams.flow(range(16)).map(fpstreams.item + 1)
    assert transformed.sum() == sum(range(1, 17))
    assert linear_calls == 1

    failure = RuntimeError("instrumented range")
    with failpoint("source.open.after", failure), pytest.raises(RuntimeError) as captured:
        fpstreams.flow(range(16)).sum()
    assert captured.value is failure


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
    source = container(int(str(1_000_000 + index)) for index in range(16))
    assert type(source[0]) is int and source[0] is not int("1000000")
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


@pytest.mark.parametrize("dtype,kind", [("int64", "i64"), ("float64", "f64")])
def test_native_buffer_detection_preserves_custom_descriptor_types(dtype: str, kind: str) -> None:
    """Class equality cannot hide a numeric column or skip its live shape validation."""
    from fpstreams.planning.native import _numpy_buffer_kind
    from fpstreams.tabular.numpy import NumpyColumnSource

    np = pytest.importorskip("numpy")

    class ColumnMeta(type):
        def __eq__(self, other: object) -> bool:
            raise AssertionError("buffer detection must not compare custom classes")

    class Column(NumpyColumnSource, metaclass=ColumnMeta):
        pass

    values = np.arange(8, dtype=dtype)
    descriptor = Column(values)
    assert _numpy_buffer_kind(descriptor) == kind

    values.resize((2, 4), refcheck=False)
    with pytest.raises(ValueError, match="retained array changed to 2 dimensions"):
        _numpy_buffer_kind(descriptor)


@pytest.mark.parametrize("terminal", ["first", "any", "all"])
def test_identity_container_planning_keeps_numpy_short_circuit_precedence(terminal: str) -> None:
    """A descriptor reporting a list class still needs the buffer streaming decision."""
    from fpstreams.planning import native
    from fpstreams.planning.logical import Pipeline
    from fpstreams.planning.source import Source, SourceCapabilities
    from fpstreams.tabular.numpy import NumpyColumnSource

    np = pytest.importorskip("numpy")

    class Column(NumpyColumnSource):
        @property
        def __class__(self) -> type:
            return list

    def unopened():
        raise AssertionError("engine selection must not open the source")

    source = Source(unopened, SourceCapabilities(True, 8), Column(np.arange(8, dtype="int64")))
    decision = native.select_terminal_engine(Pipeline(source, ()), terminal)

    assert decision.engine == "python"
    assert decision.reason == (
        "NumPy buffer first stays in Python to preserve short-circuiting"
        if terminal == "first"
        else "NumPy buffer any/all stays in Python to preserve short-circuiting"
    )


@pytest.mark.parametrize("source", [list(range(32)), tuple(range(32))])
@pytest.mark.parametrize("terminal", ["list", "count", "sum"])
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


@pytest.mark.parametrize("container", [list, tuple])
def test_large_identity_i64_container_sum_uses_the_native_terminal(
    container: type[list[int]] | type[tuple[int, ...]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A large exact sequence reaches the allocation-free Rust sum terminal once."""
    from fpstreams.execution import native

    extension = native._native
    calls = 0

    class TrackedNative:
        def terminal_i64(self, *arguments: object) -> object:
            nonlocal calls
            calls += 1
            return extension.terminal_i64(*arguments)

        def __getattr__(self, name: str) -> object:
            return getattr(extension, name)

    monkeypatch.setattr(native, "_native", TrackedNative())
    source = container(range(100_000))
    values = fpstreams.flow(source)
    explanation = values.explain("sum").to_dict()

    assert values.sum() == 4_999_950_000
    assert calls == 1
    assert explanation["selected_engine"] == "native"
    assert explanation["data_movement"] == {
        "scans_source": True,
        "copies_source": False,
        "materializes": False,
    }


@pytest.mark.parametrize("container", [list, tuple])
def test_forced_native_identity_i64_container_sum_widens_past_i64(
    container: type[list[int]] | type[tuple[int, ...]],
) -> None:
    """The direct terminal accumulates exact i64 items into one Python-width result."""
    maximum = 2**63 - 1

    assert fpstreams.flow(container([maximum, maximum])).with_engine("native").sum() == 2 * maximum


@pytest.mark.parametrize("container", [list, tuple])
def test_auto_identity_i64_container_sum_replays_after_a_late_exact_type_decline(
    container: type[list[object]] | type[tuple[object, ...]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A speculative exact scan leaves the whole reusable sequence for Python fallback."""
    from fpstreams.execution import native

    extension = native._native
    native_calls = 0
    subclass_additions: list[int] = []

    class TrackedNative:
        def terminal_i64(self, *arguments: object) -> object:
            nonlocal native_calls
            native_calls += 1
            return extension.terminal_i64(*arguments)

        def __getattr__(self, name: str) -> object:
            return getattr(extension, name)

    class IntegerSubclass(int):
        def __radd__(self, other: int) -> int:
            subclass_additions.append(other)
            return other + int(self)

    monkeypatch.setattr(native, "_native", TrackedNative())
    prefix_size = 65_536
    cases = (
        (2.5, 65_539.5),
        (True, 65_538),
        (2**100, 2**100 + 65_537),
        (IntegerSubclass(7), 65_544),
    )

    for tail, expected in cases:
        values = fpstreams.flow(container([1] * prefix_size + [tail, 1]))
        assert values.explain("sum").to_dict()["selected_engine"] == "native"
        assert values.sum() == expected

    assert native_calls == len(cases)
    assert subclass_additions == [prefix_size]


@pytest.mark.parametrize("container", [list, tuple])
def test_auto_identity_i64_container_sum_rejects_incompatible_boundary_items_before_rust(
    container: type[list[object]] | type[tuple[object, ...]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """O(1) boundary guards avoid a guaranteed whole-container native decline."""
    from fpstreams.execution import native

    extension = native._native

    class RejectingNative:
        def terminal_i64(self, *_arguments: object) -> object:
            raise AssertionError("a known-incompatible boundary item reached Rust")

        def __getattr__(self, name: str) -> object:
            return getattr(extension, name)

    class IntegerSubclass(int):
        pass

    monkeypatch.setattr(native, "_native", RejectingNative())
    cases = (
        (2.5, 65_538.5),
        (True, 65_537),
        (2**100, 2**100 + 65_536),
        (IntegerSubclass(7), 65_543),
    )

    for tail, expected in cases:
        for source in (
            container([tail] + [1] * 65_536),
            container([1] * 65_536 + [tail]),
        ):
            values = fpstreams.flow(source)
            assert values.explain("sum").to_dict()["selected_engine"] == "python"
            assert values.sum() == expected


@pytest.mark.parametrize("container", [list, tuple])
def test_large_identity_i64_container_sum_with_start_stays_in_python(
    container: type[list[int]] | type[tuple[int, ...]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A nonzero start retains built-in addition order and never enters the zero-start ABI."""
    from fpstreams.execution import native

    extension = native._native

    class RejectingNative:
        def terminal_i64(self, *_arguments: object) -> object:
            raise AssertionError("a nonzero start reached the zero-start native sum")

        def __getattr__(self, name: str) -> object:
            return getattr(extension, name)

    monkeypatch.setattr(native, "_native", RejectingNative())

    assert fpstreams.flow(container([1] * 65_536)).sum(7) == 65_543


@pytest.mark.parametrize("container", [list, tuple])
def test_auto_identity_i64_container_sum_preserves_active_source_failpoints(
    container: type[list[int]] | type[tuple[int, ...]],
) -> None:
    """Instrumentation keeps the canonical source-open boundary ahead of summation."""
    from fpstreams.runtime.failpoints import failpoint

    failure = RuntimeError("instrumented identity sum")
    with failpoint("source.open.after", failure), pytest.raises(RuntimeError) as captured:
        fpstreams.flow(container([1] * 65_536)).sum()

    assert captured.value is failure


@pytest.mark.parametrize("source", [list(range(32)), tuple(range(32))])
def test_identity_numeric_container_auto_statistics_use_native(
    source: list[int] | tuple[int, ...],
) -> None:
    explanation = fpstreams.flow(source).explain("statistics").to_dict()

    assert explanation["selected_engine"] == "native"
    assert explanation["data_movement"] == {
        "scans_source": True,
        "copies_source": True,
        "materializes": False,
    }
    assert fpstreams.flow(source).mean() == 15.5


def test_terminal_explain_matches_forced_native_and_range_execution() -> None:
    forced = fpstreams.flow([1, 2, 3]).with_engine("native").explain("sum").to_dict()
    ranged = fpstreams.flow(range(1, 33)).explain("sum").to_dict()

    assert forced["selected_engine"] == "native"
    assert forced["data_movement"] == {
        "scans_source": True,
        "copies_source": False,
        "materializes": False,
    }
    assert ranged["selected_engine"] == "native"
    assert ranged["data_movement"] == {
        "scans_source": False,
        "copies_source": False,
        "materializes": False,
    }
    assert ranged["complexity"] == "O(1)"


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


@pytest.mark.parametrize(
    "source",
    [
        [],
        list(range(32)),
        tuple(range(32)),
        range(32),
        "countable",
        b"countable",
        {1, 2, 3},
        frozenset({1, 2, 3}),
        {"one": 1, "two": 2},
    ],
    ids=lambda source: type(source).__name__,
)
@pytest.mark.parametrize("engine", ["auto", "python"])
def test_identity_exact_size_count_skips_physical_compilation(
    source: object,
    engine: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Safe built-in cardinality metadata is a terminal result, not a backend query."""
    from fpstreams.streams import flow_terminals

    def fail_if_compiled(_query: object) -> object:
        raise AssertionError("identity exact-size count must not compile a physical plan")

    monkeypatch.setattr(flow_terminals, "compile_query", fail_if_compiled)

    assert fpstreams.flow(source).with_engine(engine).count() == len(source)  # type: ignore[arg-type]


def test_forced_native_identity_count_still_validates_its_backend(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The metadata shortcut must not silently weaken an explicit native request."""
    from fpstreams.streams import flow_terminals

    compile_query = flow_terminals.compile_query
    calls = 0

    def tracked_compile(query: object) -> object:
        nonlocal calls
        calls += 1
        return compile_query(query)  # type: ignore[arg-type]

    monkeypatch.setattr(flow_terminals, "compile_query", tracked_compile)

    assert fpstreams.flow(range(32)).with_engine("native").count() == 32
    for values in (["bad"], [1, "bad"], [True], [1.0, "bad"]):
        assert fpstreams.flow(values).with_engine("native").count() == len(values)
    assert calls == 5
    explanation = fpstreams.flow([1, "bad"]).with_engine("native").explain("count").to_dict()
    assert explanation["complexity"] == "O(1)"
    assert explanation["data_movement"] == {
        "scans_source": False,
        "copies_source": False,
        "materializes": False,
    }
    with pytest.raises(fpstreams.NativeUnsupportedError):
        fpstreams.flow({1, 2, 3}).with_engine("native").count()


def test_forced_native_count_never_bypasses_a_retained_one_shot_claim() -> None:
    """Raw native_data cannot turn a custom one-shot Source into a replayable count."""
    from fpstreams.planning.source import Source, SourceCapabilities

    retained = list(range(32))
    source = Source(
        lambda: iter(retained),
        SourceCapabilities(reiterable=False, exact_size=len(retained)),
        native_data=retained,
    )

    with pytest.raises(fpstreams.NativeUnsupportedError, match="one-shot"):
        fpstreams.Flow(source).with_engine("native").count()

    replay_guard = Source(
        lambda: iter(retained),
        SourceCapabilities(reiterable=False, exact_size=len(retained)),
        native_data=retained,
    )
    automatic = fpstreams.Flow(replay_guard)
    assert automatic.count() == len(retained)
    with pytest.raises(fpstreams.FlowConsumedError):
        automatic.count()


@pytest.mark.parametrize("engine", ["auto", "python"])
def test_identity_count_reads_the_current_size_of_mutable_builtin_sources(
    engine: str,
) -> None:
    """Count metadata remains live when a retained list, dict, or set changes size."""
    sources: list[tuple[object, Callable[[], None]]] = []

    values = [1]
    sources.append((values, lambda: values.append(2)))
    mapping = {"one": 1}
    sources.append((mapping, lambda: mapping.__setitem__("two", 2)))
    members = {1}
    sources.append((members, lambda: members.add(2)))

    for source, mutate in sources:
        candidate = fpstreams.flow(source).with_engine(engine)
        assert candidate.count() == 1
        mutate()
        assert candidate.count() == 2
        assert candidate.aggregate(rows=fpstreams.agg.count()) == {"rows": 2}
        explanation = candidate.explain("count").to_dict()
        assert explanation["source"]["exact_size"] == 2
        assert explanation["semantics"]["output"]["cardinality"] == {
            "kind": "exact",
            "value": 2,
        }


@pytest.mark.parametrize("engine", ["auto", "python"])
def test_identity_exact_count_preserves_active_source_failpoints(engine: str) -> None:
    """Instrumentation observes canonical opening even when cardinality metadata is exact."""
    from fpstreams.runtime.failpoints import failpoint

    for source in ([1, 2, 3], range(32)):
        failure = RuntimeError("instrumented count source")
        with (
            failpoint("source.open.after", failure),
            pytest.raises(RuntimeError) as captured,
        ):
            fpstreams.flow(source).with_engine(engine).count()

        assert captured.value is failure

        aggregate_failure = RuntimeError("instrumented named count source")
        with (
            failpoint("source.open.after", aggregate_failure),
            pytest.raises(RuntimeError) as aggregate_captured,
        ):
            fpstreams.flow(source).with_engine(engine).aggregate(rows=fpstreams.agg.count())

        assert aggregate_captured.value is aggregate_failure


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


def test_count_consumes_effectful_and_one_shot_plans() -> None:
    """Only a true identity source may skip iteration, callbacks, closing, and claiming."""
    seen: list[int] = []
    assert fpstreams.flow([1, 2, 3]).tap(seen.append).count() == 3
    assert seen == [1, 2, 3]

    closed = False

    def values() -> Iterator[int]:
        nonlocal closed
        try:
            yield from range(3)
        finally:
            closed = True

    one_shot = fpstreams.flow(values())
    assert one_shot.count() == 3
    assert closed is True
    with pytest.raises(fpstreams.FlowConsumedError):
        one_shot.count()


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
    "mean_i64",
    "aggregate_i64",
    "aggregate_i64_masked",
)


_F64_BULK_ENDPOINTS = (
    "execute_f64",
    "materialize_f64",
    "terminal_f64",
    "statistics_f64",
    "mean_f64",
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


def test_native_mean_abi_preserves_empty_range_and_cancellation_semantics() -> None:
    """Mean-only kernels retain public results without computing a variance snapshot."""
    from fpstreams import _native

    assert _native.mean_i64([], []) is None
    assert _native.mean_i64([1, 2, 3], []) == 2.0
    assert _native.mean_i64_range(1, 4, 1, []) == 2.0
    assert _native.mean_f64([], []) is None
    assert _native.mean_f64([1e16, 1.0, -1e16], []) == pytest.approx(1 / 3)
    assert math.isnan(_native.mean_f64([math.inf, -math.inf], []))
    assert _native.mean_f64_range(1, 4, 1, []) == 2.0


def test_numeric_iterator_mean_abi_matches_python_types_errors_and_lifetime() -> None:
    """The chunk ABI returns custom boundaries before invoking any numeric protocol."""
    from fractions import Fraction

    from fpstreams import _native
    from fpstreams.streams import flow_terminals

    def chunk(source: object) -> tuple[int, int, float, float, object | None]:
        return _native.mean_exact_iterator_chunk_v1(
            source,
            0,
            0.0,
            0.0,
            flow_terminals._CANONICAL_MEAN_NATIVE_BINDINGS,
            flow_terminals._CANONICAL_COMPENSATED_MEAN,
            flow_terminals._CANONICAL_COMPENSATED_MEAN_CODE,
        )

    boundary = Fraction(1, 3)
    status, count, _total, _compensation, returned = chunk(
        iter([1, True, 2.5, 2**100, boundary, 5])
    )
    assert (status, count, returned) == (2, 4, boundary)

    events: list[str] = []

    class TrackedFloat(float):
        def __float__(self) -> float:
            events.append("float")
            return 4.5

    tracked = TrackedFloat(1.0)
    status, count, _total, _compensation, returned = chunk(iter([tracked, 5]))
    assert (status, count, returned) == (2, 0, tracked)
    assert events == []

    with pytest.raises(OverflowError, match="too large to convert to float"):
        chunk(iter([2**2000]))
    with pytest.raises(TypeError, match="exact built-in"):
        chunk([1, 2, 3])
    with pytest.raises(TypeError, match="exact built-in"):
        chunk(iter(value for value in [1, 2, 3]))


def test_one_shot_flow_mean_uses_one_consuming_native_loop_and_closes_on_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Automatic iterator means never pre-scan or replay and retain source error ownership."""
    from fpstreams.execution import native

    extension = native._native
    calls = 0

    class TrackedNative:
        def mean_exact_iterator_chunk_v1(self, *arguments: object) -> object:
            nonlocal calls
            calls += 1
            return extension.mean_exact_iterator_chunk_v1(*arguments)

        def __getattr__(self, name: str) -> object:
            return getattr(extension, name)

    monkeypatch.setattr(native, "_native", TrackedNative())
    assert fpstreams.flow(iter(range(1_000))).mean() == 499.5
    large_start = 1 << 100
    assert fpstreams.flow(iter(range(large_start, large_start + 1_000))).mean() == pytest.approx(
        float(large_start)
    )
    assert calls == 2

    class ConversionFailure(RuntimeError):
        pass

    class FailingFloat(float):
        def __float__(self) -> float:
            raise ConversionFailure("conversion failed")

    class SourceFailure(RuntimeError):
        pass

    for failing_value, error_type in (
        ("not numeric", TypeError),
        (2**2000, OverflowError),
        (FailingFloat(1.0), ConversionFailure),
    ):
        with pytest.raises(error_type):
            fpstreams.flow(iter([*range(128), failing_value])).mean()

    closed = False

    def failing_source() -> Iterator[int]:
        nonlocal closed
        try:
            yield 1
            raise SourceFailure("source failed")
        finally:
            closed = True

    with pytest.raises(SourceFailure, match="source failed"):
        fpstreams.flow(failing_source()).mean()
    assert closed is True
    assert calls == 5


@pytest.mark.parametrize("mutation", ["identity", "code"])
def test_numeric_iterator_mean_rechecks_live_continuation_between_chunks(
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    """Changing the Python continuation stops Rust before its next pull."""
    from fpstreams.execution import native
    from fpstreams.streams import flow_terminals

    extension = native._native
    helper_name = "_continue_compensated_mean"
    canonical = flow_terminals._continue_compensated_mean
    original_code = canonical.__code__
    calls = 0
    sentinel = -12_345.0

    def replacement(*_arguments: object) -> float:
        return -12_345.0

    class MutatingNative:
        def mean_exact_iterator_chunk_v1(self, *arguments: object) -> object:
            nonlocal calls
            calls += 1
            outcome = extension.mean_exact_iterator_chunk_v1(*arguments)
            if calls == 1:
                if mutation == "identity":
                    monkeypatch.setattr(flow_terminals, helper_name, replacement)
                else:
                    canonical.__code__ = replacement.__code__
            return outcome

        def __getattr__(self, name: str) -> object:
            return getattr(extension, name)

    monkeypatch.setattr(native, "_native", MutatingNative())
    values = list(range(5_000))
    try:
        execution = fpstreams.flow(iter(values)).run_with_report("mean")
    finally:
        canonical.__code__ = original_code

    assert execution.value == sentinel
    assert execution.report.strategy == "rust_python_hybrid"
    assert calls == 1


@pytest.mark.parametrize("source", [list(range(32)), range(32)], ids=["list", "range"])
def test_auto_mean_opens_the_live_source_before_native_reduction(
    monkeypatch: pytest.MonkeyPatch,
    source: list[int] | range,
) -> None:
    """Automatic mean and mean-only aggregation obey a replaced source opener."""
    from fpstreams.planning.source import Source

    monkeypatch.setattr(Source, "open", lambda _self: iter((10, 20)))

    mean = fpstreams.flow(source).mean()
    aggregate = fpstreams.flow(source).aggregate(mean=fpstreams.agg.mean(fpstreams.fitem))

    assert mean == 15.0
    assert aggregate == {"mean": 15.0}


def test_auto_mean_observes_the_live_retained_factory_iter_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retained metadata cannot outrank the iterator actually returned by Source.open."""
    import builtins

    from fpstreams.planning import source as source_module

    values = list(range(32))
    original_iter = builtins.iter

    def replacement(source: object, *arguments: object) -> Iterator[object]:
        if source is values:
            return original_iter((10, 20))
        return original_iter(source, *arguments)  # type: ignore[call-overload]

    monkeypatch.setattr(source_module, "iter", replacement, raising=False)

    assert fpstreams.flow(values).mean() == 15.0
    assert fpstreams.flow(values).aggregate(mean=fpstreams.agg.mean(fpstreams.fitem)) == {
        "mean": 15.0
    }


def test_auto_mean_executes_the_live_source_open_failpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mean shortcuts never skip the failpoint owned by canonical Source.open."""
    from fpstreams.runtime import failpoints

    original_hit = failpoints.hit

    def fail_source_open(name: str) -> None:
        if name == "source.open.after":
            raise RuntimeError("source open failed")
        original_hit(name)

    monkeypatch.setattr(failpoints, "hit", fail_source_open)

    with pytest.raises(RuntimeError, match="source open failed"):
        fpstreams.flow(list(range(32))).mean()
    with pytest.raises(RuntimeError, match="source open failed"):
        fpstreams.flow(list(range(32))).aggregate(mean=fpstreams.agg.mean(fpstreams.fitem))


def test_auto_numpy_mean_opens_the_live_source_before_columnar_reduction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A NumPy buffer remains fast only after the authoritative source opener runs."""
    np = pytest.importorskip("numpy")
    from fpstreams.planning.source import Source

    query = fpstreams.flow.from_numpy(np.arange(32, dtype=np.int64))
    monkeypatch.setattr(Source, "open", lambda _self: iter((10, 20)))

    assert query.mean() == 15.0


def test_auto_arrow_mean_opens_the_live_source_before_columnar_reduction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An Arrow buffer remains fast only after the authoritative row source opens."""
    pa = pytest.importorskip("pyarrow")
    from fpstreams.planning.source import Source

    table = pa.table({"value": pa.array(range(32), type=pa.int64())})
    query = fpstreams.flow.from_arrow(table).map(fpstreams.col("value"))
    monkeypatch.setattr(Source, "open", lambda _self: iter(({"value": 10}, {"value": 20})))

    assert query.mean() == 15.0


def test_auto_arrow_mean_observes_the_live_row_conversion_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retained Arrow storage cannot bypass the row factory's live converter."""
    pa = pytest.importorskip("pyarrow")
    from fpstreams.tabular import arrow as arrow_adapter

    table = pa.table({"value": pa.array(range(64), type=pa.int64())})
    query = fpstreams.flow.from_arrow(table).map(fpstreams.col("value"))
    canonical = query.with_engine("python")
    monkeypatch.setattr(
        arrow_adapter,
        "batch_to_rows",
        lambda _batch: iter(({"value": 10}, {"value": 20})),
    )

    assert query.mean() == canonical.mean() == 15.0


def test_mean_guards_do_not_emit_function_code_audit_events() -> None:
    """Forced Python and automatic iterator means add no audited ``__code__`` lookup."""
    script = """
import sys
import fpstreams
from fpstreams.collecting import statistics

def reject_code_lookup(event, arguments):
    if (
        event == "object.__getattr__"
        and len(arguments) == 2
        and arguments[0] is statistics.compensated_mean
        and arguments[1] == "__code__"
    ):
        raise RuntimeError("unexpected compensated_mean code lookup")

sys.addaudithook(reject_code_lookup)
assert fpstreams.flow(iter(range(256))).with_engine("python").mean() == 127.5
assert fpstreams.flow(iter(range(256))).mean() == 127.5
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("container", [list, tuple], ids=["list", "tuple"])
def test_exact_numeric_mean_abi_handles_mixed_builtins_without_protocol_dispatch(
    container: type[list[object]] | type[tuple[object, ...]],
) -> None:
    """The identity fast path covers real built-ins and safely declines user objects."""
    from fpstreams import _native

    calls = 0

    class IntegerSubclass(int):
        def __float__(self) -> float:
            nonlocal calls
            calls += 1
            return 99.0

    assert _native.mean_exact_numbers_v1(container([])) == (True, None)
    handled, result = _native.mean_exact_numbers_v1(container([1, 2.5, True, 2**100]))
    assert handled is True
    assert result == pytest.approx((1.0 + 2.5 + 1.0 + float(2**100)) / 4)
    assert _native.mean_exact_numbers_v1(container([1, IntegerSubclass(2)])) == (
        False,
        None,
    )
    assert calls == 0
    assert _native.mean_exact_numbers_v1(container([1, 2 + 0j])) == (False, None)
    with pytest.raises(OverflowError):
        _native.mean_exact_numbers_v1(container([2**2000]))


@pytest.mark.skipif(sys.platform == "win32", reason="setitimer is Unix-only")
@pytest.mark.parametrize("initial_delay", [0.0, 0.001], ids=["missed-first-alarm", "normal-alarm"])
def test_exact_numeric_mean_decline_is_atomic_before_signal_mutation(
    monkeypatch: pytest.MonkeyPatch,
    initial_delay: float,
) -> None:
    """A late decline never replays a prefix mutated by a mid-probe signal callback."""
    import dis
    import signal
    from fractions import Fraction

    from fpstreams.collecting.statistics import compensated_mean
    from fpstreams.execution import native

    original_size = 600_000
    original: list[object] = list(range(original_size))
    original[400_000] = Fraction(1, 3)
    original_mean = compensated_mean(original)
    expected_values = original[100_000:]
    expected_values[300_000] = 7
    expected = compensated_mean(expected_values)
    previous = signal.getsignal(signal.SIGALRM)
    extension = native._native

    def invoke() -> object:
        return extension.mean_exact_numbers_v1(values)

    # CPython 3.11 can execute a specialized built-in call at PRECALL after warmup.
    call_offsets = {
        instruction.offset
        for instruction in dis.get_instructions(invoke)
        if instruction.opname in {"PRECALL", "CALL"}
    }

    class AlarmNative:
        def mean_exact_numbers_v1(self, source: object) -> object:
            assert source is values
            signal.setitimer(signal.ITIMER_REAL, delay, 0)
            try:
                outcome = invoke()
            finally:
                # Never let a missed alarm mutate the subsequent Python fallback.
                signal.setitimer(signal.ITIMER_REAL, 0)
            assert outcome == (False, None)
            return outcome

        def __getattr__(self, name: str) -> object:
            return getattr(extension, name)

    def shrink_front_and_replace_late_value(_signum: int, frame: FrameType | None) -> None:
        nonlocal handled_signal
        if (
            frame is None
            or frame.f_code is not invoke.__code__
            or frame.f_lasti not in call_offsets
        ):
            return
        handled_signal = True
        del values[:100_000]
        values[300_000] = 7

    signal.signal(signal.SIGALRM, shrink_front_and_replace_late_value)
    monkeypatch.setattr(native, "_native", AlarmNative())
    try:
        for attempt in range(10):
            values = original.copy()
            handled_signal = False
            # Retry only missed signal windows, never an incorrect native or fallback result.
            # A zero initial delay exercises a missed window without depending on CPU speed.
            delay = initial_delay if attempt == 0 else 0.001 / (2**attempt)
            result = fpstreams.flow(values).mean()
            assert values == (expected_values if handled_signal else original)
            assert result == (expected if handled_signal else original_mean)
            if handled_signal:
                break
        else:
            pytest.fail("signal missed the native mean call in all 10 attempts")
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


@pytest.mark.skipif(sys.platform == "win32", reason="setitimer is Unix-only")
def test_exact_numeric_mean_delivers_pending_keyboard_interrupt_at_return_boundary() -> None:
    """An atomic speculative scan delays but never loses a pending KeyboardInterrupt."""
    import signal

    from fpstreams import _native

    values = list(range(2_000_000))
    previous = signal.getsignal(signal.SIGALRM)

    def interrupt(_signum: int, _frame: object) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGALRM, interrupt)
    signal.setitimer(signal.ITIMER_REAL, 0.001, 0)
    try:
        with pytest.raises(KeyboardInterrupt):
            _native.mean_exact_numbers_v1(values)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


@pytest.mark.skipif(sys.platform == "win32", reason="setitimer is Unix-only")
@pytest.mark.parametrize("kind", ["i64", "f64"])
@pytest.mark.parametrize("initial_delay", [0.0, 0.001], ids=["missed-first-alarm", "normal-alarm"])
def test_typed_mean_late_mismatch_is_atomic_before_signal_mutation(
    kind: str,
    initial_delay: float,
) -> None:
    """Typed compatibility endpoints cannot turn a late mismatch into a replayed success."""
    import dis
    import signal

    from fpstreams import _native

    if kind == "i64":
        original: list[object] = [1] * 600_000
        original[400_000] = 1.5
        replacement: object = 7
        endpoint = _native.mean_i64
    else:
        original = [1.0] * 600_000
        original[400_000] = 7
        replacement = 7.0
        endpoint = _native.mean_f64
    previous = signal.getsignal(signal.SIGALRM)

    def invoke() -> object:
        try:
            return endpoint(values, [])
        finally:
            # Disarm before propagating TypeError; this also delivers any signal
            # still pending after the native exception, within the observed frame.
            signal.setitimer(signal.ITIMER_REAL, 0)

    call_offsets = {
        instruction.offset
        for instruction in dis.get_instructions(invoke)
        if instruction.opname in {"PRECALL", "CALL"}
    }

    def shrink_front_and_repair_mismatch(_signum: int, frame: FrameType | None) -> None:
        nonlocal handled_signal
        if (
            frame is None
            or frame.f_code is not invoke.__code__
            or frame.f_lasti not in call_offsets
        ):
            return
        handled_signal = True
        del values[:100_000]
        values[300_000] = replacement

    signal.signal(signal.SIGALRM, shrink_front_and_repair_mismatch)
    try:
        for attempt in range(10):
            values = original.copy()
            handled_signal = False
            delay = initial_delay if attempt == 0 else 0.001 / (2**attempt)
            try:
                with pytest.raises(TypeError):
                    signal.setitimer(signal.ITIMER_REAL, delay, 0)
                    invoke()
            finally:
                signal.setitimer(signal.ITIMER_REAL, 0)
            if handled_signal:
                assert len(values) == 500_000
                assert values[300_000] == replacement
                break
            assert values == original
        else:
            pytest.fail("signal missed the native mean call in all 10 attempts")
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def test_numpy_f64_identity_terminals_prefer_borrowed_v2_endpoints(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Identity float terminals use v2 while legacy v1 symbols remain compatible fallbacks."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution import native

    extension = native._native
    calls: list[str] = []

    class TrackedNative:
        def terminal_f64_buffer_v2(self, *args: object) -> object:
            calls.append("terminal")
            return extension.terminal_f64_buffer_v2(*args)

        def mean_f64_buffer_v2(self, *args: object) -> object:
            calls.append("mean")
            return extension.mean_f64_buffer_v2(*args)

        def aggregate_f64_buffer_masked_v2(self, *args: object) -> object:
            calls.append("aggregate")
            return extension.aggregate_f64_buffer_masked_v2(*args)

        def __getattr__(self, name: str) -> object:
            return getattr(extension, name)

    monkeypatch.setattr(native, "_native", TrackedNative())
    values = fpstreams.flow.from_numpy(np.arange(32, dtype=np.float64))

    assert values.sum() == 496.0
    assert values.mean() == 15.5
    assert values.variance() == 88.0
    assert values.aggregate(total=fpstreams.agg.sum(), low=fpstreams.agg.min()) == {
        "total": 496.0,
        "low": 0.0,
    }
    assert calls == ["terminal", "mean", "aggregate", "aggregate"]

    calls.clear()

    class LegacyNative:
        def terminal_f64_buffer_v1(self, *args: object) -> object:
            calls.append("terminal_v1")
            return extension.terminal_f64_buffer_v1(*args)

        def mean_f64_buffer_v1(self, *args: object) -> object:
            calls.append("mean_v1")
            return extension.mean_f64_buffer_v1(*args)

        def aggregate_f64_buffer_masked_v1(self, *args: object) -> object:
            calls.append("aggregate_v1")
            return extension.aggregate_f64_buffer_masked_v1(*args)

        def __getattr__(self, name: str) -> object:
            if name in {
                "terminal_f64_buffer_v2",
                "mean_f64_buffer_v2",
                "aggregate_f64_buffer_masked_v2",
            }:
                raise AttributeError(name)
            return getattr(extension, name)

    monkeypatch.setattr(native, "_native", LegacyNative())

    assert values.sum() == 496.0
    assert values.mean() == 15.5
    assert values.variance() == 88.0
    assert calls == ["terminal_v1", "mean_v1", "aggregate_v1"]


@pytest.mark.skipif(sys.platform == "win32", reason="setitimer is Unix-only")
@pytest.mark.parametrize(
    "endpoint_name",
    [
        "terminal_f64_buffer_v2",
        "terminal_f64_buffer_v2_staged",
        "mean_f64_buffer_v2",
        "mean_f64_buffer_v2_staged",
        "aggregate_f64_buffer_masked_v2",
    ],
)
def test_borrowed_f64_buffer_releases_export_before_signal_resize(endpoint_name: str) -> None:
    """Every borrowed float reducer finishes its stable scan before a handler can resize."""
    pytest.importorskip("numpy")
    code = f"""
import signal

import numpy as np

from fpstreams import _native

values = np.arange(4_000_000, dtype=np.float64)
original_size = values.size
expected_total = float(original_size * (original_size - 1) // 2)
handled_signal = False


def resize_exporter(_signum, _frame):
    global handled_signal
    handled_signal = True
    values.resize(1, refcheck=False)


signal.signal(signal.SIGALRM, resize_exporter)
signal.setitimer(signal.ITIMER_REAL, 0.0005, 0)
try:
    endpoint_name = {endpoint_name!r}
    if endpoint_name == "terminal_f64_buffer_v2":
        result = _native.terminal_f64_buffer_v2(values, [], 1)
        assert result == (original_size, expected_total)
    elif endpoint_name == "terminal_f64_buffer_v2_staged":
        identity_map = [(0, [(0, 0.0)])]
        result = _native.terminal_f64_buffer_v2(values, identity_map, 1)
        assert result == (original_size, expected_total)
    elif endpoint_name == "mean_f64_buffer_v2":
        result = _native.mean_f64_buffer_v2(values, [])
        assert result == (original_size - 1) / 2
    elif endpoint_name == "mean_f64_buffer_v2_staged":
        identity_map = [(0, [(0, 0.0)])]
        result = _native.mean_f64_buffer_v2(values, identity_map)
        assert result == (original_size - 1) / 2
    else:
        result = _native.aggregate_f64_buffer_masked_v2(values, [], 1 << 1)
        assert result[1] == expected_total
finally:
    signal.setitimer(signal.ITIMER_REAL, 0)

assert handled_signal
assert values.size == 1
"""

    completed = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )

    assert completed.returncode == 0, completed.stderr


@pytest.mark.skipif(sys.platform == "win32", reason="setitimer is Unix-only")
@pytest.mark.parametrize("endpoint_name", ["mean_i64_buffer_v1", "mean_i64_buffer_v2"])
@pytest.mark.parametrize("size", [1_000_000, 4_000_000])
def test_numeric_buffer_reduction_survives_signal_handler_resize(
    size: int,
    endpoint_name: str,
) -> None:
    """A signal may resize the exporter only after Rust finishes with its stable storage."""
    pytest.importorskip("numpy")
    code = f"""
import dis
import signal

import numpy as np

from fpstreams import _native

endpoint = getattr(_native, {endpoint_name!r})


def invoke():
    return endpoint(values, [])


call_offset = next(
    instruction.offset for instruction in dis.get_instructions(invoke)
    if instruction.opname == "CALL"
)


def resize_exporter(_signum, frame):
    global handled_signal
    # A signal before entering the native call is not a buffer-safety probe.
    if frame.f_code is invoke.__code__ and frame.f_lasti == call_offset:
        handled_signal = True
        values.resize(1, refcheck=False)


signal.signal(signal.SIGALRM, resize_exporter)
for attempt in range(10):
    values = np.arange({size}, dtype=np.int64)
    original_size = values.size
    handled_signal = False
    # Fast release builds can finish before 0.5 ms. Retry only missed signal
    # windows with a shorter delay; every computed result must still be correct.
    signal.setitimer(signal.ITIMER_REAL, 0.0005 / (2 ** attempt), 0)
    try:
        result = invoke()
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)

    assert result == (original_size - 1) / 2
    if handled_signal:
        assert values.size == 1
        break
    assert values.size == original_size
else:
    raise AssertionError("signal missed the native call in all 10 attempts")

"""

    completed = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )

    assert completed.returncode == 0, completed.stderr


@pytest.mark.skipif(sys.platform == "win32", reason="setitimer is Unix-only")
@pytest.mark.parametrize("kind", ["i64", "f64"])
def test_borrowed_numeric_buffer_delivers_pending_keyboard_interrupt(kind: str) -> None:
    """The v2 scan releases its export before delivering a pending signal exception."""
    import signal

    from fpstreams import _native

    np = pytest.importorskip("numpy")
    dtype = np.int64 if kind == "i64" else np.float64
    endpoint = _native.mean_i64_buffer_v2 if kind == "i64" else _native.mean_f64_buffer_v2
    values = np.arange(4_000_000, dtype=dtype)
    previous = signal.getsignal(signal.SIGALRM)

    def interrupt(_signum: int, _frame: object) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGALRM, interrupt)
    signal.setitimer(signal.ITIMER_REAL, 0.0005, 0)
    try:
        with pytest.raises(KeyboardInterrupt):
            endpoint(values, [])
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)

    values.resize(1, refcheck=False)


def test_flow_mean_dispatches_to_the_exact_mean_only_kernel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The public terminal avoids both variance work and identity-container copying."""
    from fpstreams.execution import native

    extension = native._native
    calls: list[str] = []

    class TrackedNative:
        def mean_exact_numbers_v1(self, *args: object) -> object:
            calls.append("mean_exact_numbers_v1")
            return extension.mean_exact_numbers_v1(*args)

        def statistics_i64(self, *_args: object) -> object:
            raise AssertionError("Flow.mean() must not compute variance state")

        def mean_i64(self, *_args: object) -> object:
            raise AssertionError("identity mean must not copy into the typed bulk kernel")

        def __getattr__(self, name: str) -> object:
            return getattr(extension, name)

    monkeypatch.setattr(native, "_native", TrackedNative())

    values: list[int | float] = [*range(31), 31.5]
    assert fpstreams.flow(values).mean() == pytest.approx(sum(values) / len(values))
    assert calls == ["mean_exact_numbers_v1"]


@pytest.mark.parametrize("container", [list, tuple], ids=["list", "tuple"])
@pytest.mark.parametrize(
    "first",
    [2**100, True, 0.5],
    ids=["bigint", "bool", "float"],
)
def test_flow_mean_exact_container_candidate_does_not_require_an_i64_first_item(
    monkeypatch: pytest.MonkeyPatch,
    container: type[list[object]] | type[tuple[object, ...]],
    first: object,
) -> None:
    """The mixed-number mean endpoint, not the shared i64 planner, owns item validation."""
    from fpstreams.collecting.statistics import compensated_mean
    from fpstreams.execution import native

    extension = native._native
    calls = 0

    class TrackedNative:
        def mean_exact_numbers_v1(self, source: object) -> object:
            nonlocal calls
            calls += 1
            return extension.mean_exact_numbers_v1(source)

        def __getattr__(self, name: str) -> object:
            return getattr(extension, name)

    monkeypatch.setattr(native, "_native", TrackedNative())
    values = container([first, *range(1, 32)])

    assert fpstreams.flow(values).explain("mean").to_dict()["selected_engine"] == "native"
    assert fpstreams.flow(values).mean() == compensated_mean(values)
    assert calls == 1


@pytest.mark.parametrize("container", [list, tuple], ids=["list", "tuple"])
def test_flow_mean_exact_container_decline_replays_python_once(
    monkeypatch: pytest.MonkeyPatch,
    container: type[list[object]] | type[tuple[object, ...]],
) -> None:
    """A user numeric subclass is declined without conversion before canonical replay."""
    from fpstreams.execution import native

    extension = native._native
    endpoint_calls = 0
    protocol_calls = 0

    class IntegerSubclass(int):
        def __float__(self) -> float:
            nonlocal protocol_calls
            protocol_calls += 1
            return 10.0

    class TrackedNative:
        def mean_exact_numbers_v1(self, source: object) -> object:
            nonlocal endpoint_calls
            endpoint_calls += 1
            return extension.mean_exact_numbers_v1(source)

        def mean_i64(self, *_arguments: object) -> object:
            raise AssertionError("an exact-number decline must not retry the typed i64 adapter")

        def __getattr__(self, name: str) -> object:
            return getattr(extension, name)

    monkeypatch.setattr(native, "_native", TrackedNative())
    values = container([IntegerSubclass(1), *range(1, 32)])

    assert fpstreams.flow(values).mean() == pytest.approx((10.0 + sum(range(1, 32))) / 32)
    assert endpoint_calls == protocol_calls == 1


@pytest.mark.parametrize("container", [list, tuple], ids=["list", "tuple"])
def test_flow_mean_exact_container_decline_preserves_non_numeric_error(
    monkeypatch: pytest.MonkeyPatch,
    container: type[list[object]] | type[tuple[object, ...]],
) -> None:
    """Unsupported objects are untouched by Rust and rejected by the canonical reducer."""
    from fpstreams.execution import native

    extension = native._native
    calls = 0

    class TrackedNative:
        def mean_exact_numbers_v1(self, source: object) -> object:
            nonlocal calls
            calls += 1
            return extension.mean_exact_numbers_v1(source)

        def __getattr__(self, name: str) -> object:
            return getattr(extension, name)

    monkeypatch.setattr(native, "_native", TrackedNative())
    values = container(["not numeric", *range(1, 32)])

    with pytest.raises(TypeError, match="real numeric values"):
        fpstreams.flow(values).mean()
    assert calls == 1


@pytest.mark.parametrize("container", [list, tuple], ids=["list", "tuple"])
def test_flow_mean_exact_container_overflow_is_not_replayed_in_python(
    monkeypatch: pytest.MonkeyPatch,
    container: type[list[object]] | type[tuple[object, ...]],
) -> None:
    """A proven exact-int conversion overflow is already the canonical public failure."""
    from fpstreams.execution import native
    from fpstreams.streams import flow_terminals

    extension = native._native
    calls = 0

    def reject_replay(_values: object) -> None:
        pytest.fail("exact-container overflow must not replay Python")

    class TrackedNative:
        def mean_exact_numbers_v1(self, source: object) -> object:
            nonlocal calls
            calls += 1
            monkeypatch.setattr(flow_terminals, "compensated_mean", reject_replay)
            return extension.mean_exact_numbers_v1(source)

        def __getattr__(self, name: str) -> object:
            return getattr(extension, name)

    monkeypatch.setattr(native, "_native", TrackedNative())

    with pytest.raises(OverflowError, match="too large to convert to float"):
        fpstreams.flow(container([2**2000, *range(1, 32)])).mean()
    assert calls == 1


def test_flow_mean_supports_extensions_without_mean_only_symbols(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """New Python code remains compatible with wheels exposing statistics kernels only."""
    from fpstreams.execution import native

    extension = native._native
    calls: list[str] = []

    class LegacyNative:
        def statistics_i64(self, *args: object) -> object:
            calls.append("statistics_i64")
            return extension.statistics_i64(*args)

        def __getattr__(self, name: str) -> object:
            if name == "mean_exact_numbers_v1" or name.startswith("mean_"):
                raise AttributeError(name)
            return getattr(extension, name)

    monkeypatch.setattr(native, "_native", LegacyNative())

    assert fpstreams.flow(list(range(32))).mean() == 15.5
    assert calls == ["statistics_i64"]


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


def test_linear_named_aggregate_revalidates_mutated_factory_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A changed factory reducer must not retain its whole-value native shortcut."""
    total = fpstreams.agg.sum()
    automatic = fpstreams.flow(range(32))
    canonical = automatic.with_engine("python")

    def replacement_factory() -> Callable[[int, int], int]:
        def select(value: int) -> int:
            return value

        def step(current: int, value: int) -> int:
            return current + select(value) * 9

        return step

    monkeypatch.setattr(total.step, "__code__", replacement_factory().__code__)
    expected = {"total": sum(range(32)) * 9}

    assert canonical.aggregate(total=total) == expected
    assert automatic.aggregate(total=total) == expected


def test_mean_only_named_aggregate_reuses_the_mean_kernel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repeated whole-value means share the same mean-only execution as Flow.mean()."""
    from fpstreams.streams import flow_terminals

    original = flow_terminals.try_native_mean
    calls = 0

    def tracked_mean(plan, *, decision=None):
        nonlocal calls
        calls += 1
        assert decision is not None
        assert decision.engine == "native"
        return original(plan, decision=decision)

    def reject_statistics(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("mean-only aggregation must not compute variance state")

    monkeypatch.setattr(flow_terminals, "try_native_mean", tracked_mean)
    monkeypatch.setattr(flow_terminals, "try_native_statistics", reject_statistics)
    values = [1, 2.5, True, 4] * 4

    assert fpstreams.flow(values).aggregate(
        first_mean=fpstreams.agg.mean(), second_mean=fpstreams.agg.mean()
    ) == {"first_mean": 2.125, "second_mean": 2.125}
    assert calls == 1


def test_mean_plus_count_named_aggregate_keeps_the_shared_statistics_kernel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A count plus mean cannot be projected from the scalar mean result alone."""
    from fpstreams.streams import flow_terminals

    def reject_mean(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("multi-field statistics must retain the count")

    monkeypatch.setattr(flow_terminals, "try_native_mean", reject_mean)

    assert fpstreams.flow(range(1, 5)).aggregate(
        rows=fpstreams.agg.count(), mean=fpstreams.agg.mean()
    ) == {"rows": 4, "mean": 2.5}


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


def test_float_single_sum_preserves_the_sequential_aggregate_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A total-only f64 aggregate must match its left-to-right Python collector."""
    from fpstreams.execution import native

    extension = native._native

    class NoScalarTotal:
        def terminal_f64(self, *_args: object) -> object:
            raise AssertionError("f64 aggregate totals must not reuse scalar compensated sum")

        def __getattr__(self, name: str) -> object:
            return getattr(extension, name)

    monkeypatch.setattr(native, "_native", NoScalarTotal())
    values = fpstreams.flow([1e16, 1.0, -1e16]).map(fpstreams.fitem + 0.0).with_engine("native")

    assert values.aggregate(total=fpstreams.agg.sum()) == {"total": 0.0}


def test_float_aggregate_total_requires_the_sequential_native_capability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A new Python planner must not send sequential totals to an older compensated ABI."""
    from fpstreams.execution import native

    extension = native._native

    class LegacyCompensatedExtension:
        def __getattr__(self, name: str) -> object:
            if name == "sequential_f64_aggregate_total_v1":
                raise AttributeError(name)
            if name.startswith("aggregate_f64"):
                raise AssertionError("legacy compensated aggregate must not run")
            return getattr(extension, name)

    monkeypatch.setattr(native, "_native", LegacyCompensatedExtension())
    automatic = fpstreams.flow([1e16, 1.0, -1e16]).map(fpstreams.fitem + 0.0)
    assert automatic.aggregate(total=fpstreams.agg.sum()) == {"total": 0.0}
    with pytest.raises(fpstreams.NativeUnsupportedError, match="sequential f64 aggregate"):
        automatic.with_engine("native").aggregate(total=fpstreams.agg.sum())


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

    assert fpstreams.flow(range(1, 4)).aggregate(
        rows=fpstreams.agg.count(), mean=fpstreams.agg.mean()
    ) == {"rows": 3, "mean": 2.0}


def test_mean_only_aggregate_failure_falls_back_without_another_native_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A rejected mean-only conversion enters its canonical collector exactly once."""
    from fpstreams.streams import flow_terminals

    monkeypatch.setattr(
        flow_terminals,
        "try_native_mean",
        lambda *_args, **_kwargs: (False, None),
    )

    def reject_retry(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("mean-only failure must not retry another native ABI")

    monkeypatch.setattr(flow_terminals, "try_native_statistics", reject_retry)
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
    assert fpstreams.flow([1, 2.5]).with_engine("native").aggregate(mean=fpstreams.agg.mean()) == {
        "mean": 1.75
    }
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


def test_large_auto_identity_sort_uses_guarded_native_integer_kernel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Large shuffled integers use the registered kernel and report their real strategy."""
    from fpstreams import _native

    values = [(position * 48_271) % 32_768 for position in range(32_768)]
    calls: list[tuple[object, bool]] = []
    native_sort = _native.sort_i64_exact_sequence_v1

    def tracked(source: object, reverse: bool) -> list[object] | None:
        calls.append((source, reverse))
        return native_sort(source, reverse)

    monkeypatch.setattr(_native, "sort_i64_exact_sequence_v1", tracked)

    execution = fpstreams.flow(values).sorted().run_with_report("to_list")

    assert execution.value == sorted(values)
    assert execution.report.strategy == "rust_direct"
    assert calls == [(values, False)]

    calls.clear()
    assert fpstreams.flow(values).with_engine("python").sorted(reverse=True).to_list() == sorted(
        values, reverse=True
    )
    assert calls == []


def test_direct_identity_sort_preserves_canonical_stop_iteration_and_live_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A native probe must not bypass source opening or the canonical generator boundary."""

    class StopsComparison:
        def __lt__(self, _other: object) -> bool:
            raise StopIteration("comparison stopped")

    item = StopsComparison()
    with pytest.raises(RuntimeError, match="generator raised StopIteration") as captured:
        fpstreams.flow([item] * 32_768).sorted().to_list()
    assert isinstance(captured.value.__cause__, StopIteration)

    retained = [(position * 48_271) % 32_768 for position in range(32_768)]
    values = fpstreams.flow(retained).sorted()
    source = values._pipeline.source
    replacement = [9, 8]
    monkeypatch.setattr(source, "_factory", lambda: iter(replacement))

    assert values.to_list() == [8, 9]


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


def test_auto_exact_i64_map_list_accepts_the_canonical_warmed_evaluator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Normal physical compilation may warm Expr's canonical evaluator before dispatch."""
    from fpstreams import _native

    values = list(range(16))
    expression = fpstreams.item + 987_654_321
    calls: list[tuple[object, object]] = []

    def direct(source: object, instructions: object) -> list[int]:
        assert object.__getattribute__(expression, "_evaluator") is not None
        calls.append((source, instructions))
        return [value + 987_654_321 for value in values]

    monkeypatch.setattr(
        _native,
        "materialize_i64_map_exact_list_v1",
        direct,
        raising=False,
    )
    monkeypatch.setattr(
        _native,
        "materialize_i64",
        lambda *_arguments: pytest.fail("a canonical warm evaluator reached generic native"),
    )

    assert fpstreams.flow(values).map(expression).to_list() == [
        value + 987_654_321 for value in values
    ]
    assert calls == [(values, expression.native_instructions())]


@pytest.mark.parametrize(
    ("pipeline", "terminal", "expected"),
    [
        (fpstreams.flow(list(range(16))).map(fpstreams.item + 1), "to_tuple", tuple(range(1, 17))),
        (
            fpstreams.flow(list(range(16))).map(fpstreams.item + 1).with_engine("native"),
            "to_list",
            list(range(1, 17)),
        ),
        (
            fpstreams.flow([float(value) for value in range(16)]).map(fpstreams.fitem + 0.5),
            "to_list",
            [value + 0.5 for value in range(16)],
        ),
        (
            fpstreams.flow(list(range(16))).map(fpstreams.item + 1).filter(fpstreams.item > 0),
            "to_list",
            list(range(1, 17)),
        ),
        (fpstreams.flow(range(16)).map(fpstreams.item + 1), "to_list", list(range(1, 17))),
    ],
    ids=["tuple-terminal", "forced-native", "fexpr", "multiple-stages", "range-source"],
)
def test_direct_i64_map_list_endpoint_has_a_closed_phase_a_admission(
    monkeypatch: pytest.MonkeyPatch,
    pipeline: fpstreams.Flow[object],
    terminal: str,
    expected: object,
) -> None:
    """Other targets, engines, expressions, graphs, and sources keep their old routes."""
    from fpstreams import _native

    def reject(*_arguments: object) -> object:
        raise AssertionError("an out-of-scope pipeline reached the Phase A endpoint")

    monkeypatch.setattr(
        _native,
        "materialize_i64_map_exact_list_v1",
        reject,
        raising=False,
    )

    assert getattr(pipeline, terminal)() == expected


def test_direct_i64_map_list_none_and_late_bigint_replay_python(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A late atomic decline never enters the legacy generic native adapter."""
    from fpstreams import _native

    values = [*range(15), 2**100]
    calls = 0

    def decline(source: object, _instructions: object) -> None:
        nonlocal calls
        calls += 1
        assert source is values
        return None

    def reject_generic(*_arguments: object) -> object:
        raise AssertionError("a direct-endpoint decline must replay Python")

    monkeypatch.setattr(
        _native,
        "materialize_i64_map_exact_list_v1",
        decline,
        raising=False,
    )
    monkeypatch.setattr(_native, "materialize_i64", reject_generic)

    assert fpstreams.flow(values).map(fpstreams.item + 1).to_list() == [
        *range(1, 16),
        2**100 + 1,
    ]
    assert calls == 1


def test_direct_i64_map_list_late_mixed_value_replays_python(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fpstreams import _native

    values: list[object] = [*range(15), 2.5]

    calls = 0

    def decline(_source: object, _instructions: object) -> None:
        nonlocal calls
        calls += 1
        return None

    monkeypatch.setattr(
        _native,
        "materialize_i64_map_exact_list_v1",
        decline,
        raising=False,
    )

    assert fpstreams.flow(values).map(fpstreams.item + 1).to_list() == [
        *range(1, 16),
        3.5,
    ]
    assert calls == 1


def test_direct_i64_map_list_division_by_zero_replays_the_python_exception() -> None:
    pipeline = fpstreams.flow(list(range(16))).map(100 // (fpstreams.item - 7))

    with pytest.raises(ZeroDivisionError, match="by zero"):
        pipeline.to_list()


@pytest.mark.parametrize("failure", [MemoryError("allocation"), RuntimeError("native defect")])
def test_direct_i64_map_list_unexpected_failures_propagate(
    monkeypatch: pytest.MonkeyPatch,
    failure: BaseException,
) -> None:
    from fpstreams import _native

    def fail(_source: object, _instructions: object) -> list[int]:
        raise failure

    monkeypatch.setattr(
        _native,
        "materialize_i64_map_exact_list_v1",
        fail,
        raising=False,
    )

    with pytest.raises(type(failure)) as captured:
        fpstreams.flow(list(range(16))).map(fpstreams.item + 1).to_list()
    assert captured.value is failure


def test_auto_i64_materialization_preserves_externally_owned_integer_identity() -> None:
    """Identity-exposing i64 graphs stay Python while an allocating map remains native."""
    from fpstreams.expressions.scalar import Expr
    from fpstreams.planning.native import select_materializing_engine

    values = [int(str(10_000 + index)) for index in range(16)]
    identity = fpstreams.flow(values).map(fpstreams.item)
    filtered = fpstreams.flow(values).filter(fpstreams.item >= 0).take(15).drop(1).unique()
    absolute = fpstreams.flow(values).map(abs(fpstreams.item))
    modulo = fpstreams.flow(values).map(fpstreams.item % (10**20))
    constant = fpstreams.flow(values).map(Expr.constant(int("12345678901234567890")))
    allocating = fpstreams.flow(values).filter(fpstreams.item >= 0).map(fpstreams.item + 0)

    for pipeline in (identity, filtered, absolute, modulo, constant):
        assert select_materializing_engine(pipeline._pipeline).engine == "python"
    assert select_materializing_engine(allocating._pipeline).engine == "native"
    assert (
        select_materializing_engine(allocating.with_engine("native")._pipeline).engine == "native"
    )
    assert (
        select_materializing_engine(
            fpstreams.flow([float(value) for value in range(16)])
            .map(fpstreams.fitem + 0.5)
            ._pipeline
        ).engine
        == "native"
    )

    identity_result = identity.to_list()
    filtered_result = filtered.to_list()
    assert all(result is source for result, source in zip(identity_result, values, strict=True))
    assert all(
        result is source for result, source in zip(filtered_result, values[1:15], strict=True)
    )
    constant_result = constant.to_list()
    assert all(result is constant_result[0] for result in constant_result)


def test_direct_i64_map_list_revalidates_retained_source_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fpstreams import _native
    from fpstreams.execution import try_native_materialize
    from fpstreams.planning.native import select_materializing_engine

    values = list(range(16))
    pipeline = fpstreams.flow(values).map(fpstreams.item + 1)._pipeline
    decision = select_materializing_engine(pipeline)
    pipeline.source.native_data = list(reversed(values))

    monkeypatch.setattr(
        _native,
        "materialize_i64_map_exact_list_v1",
        lambda *_arguments: pytest.fail("a stale retained source reached native execution"),
        raising=False,
    )
    monkeypatch.setattr(
        _native,
        "materialize_i64",
        lambda *_arguments: pytest.fail("a stale retained source reached generic native"),
    )

    assert try_native_materialize(pipeline, "list", decision) == (True, list(range(1, 17)))


def test_old_wheel_without_direct_i64_map_list_symbol_keeps_the_generic_adapter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A missing optional symbol does not disable the established safe allocating map path."""
    from fpstreams import _native
    from fpstreams.execution import native as native_execution

    extension = _native
    generic = extension.materialize_i64
    calls = 0

    class LegacyExtension:
        def __getattr__(self, name: str) -> object:
            nonlocal calls
            if name == "materialize_i64_map_exact_list_v1":
                raise AttributeError(name)
            if name == "materialize_i64":

                def tracked(*arguments: object) -> object:
                    nonlocal calls
                    calls += 1
                    return generic(*arguments)

                return tracked
            return getattr(extension, name)

    legacy = LegacyExtension()
    monkeypatch.setattr(fpstreams, "_native", legacy)
    monkeypatch.setattr(native_execution, "_native", legacy)

    assert fpstreams.flow(list(range(16))).map(fpstreams.item + 1).to_list() == list(range(1, 17))
    assert calls == 1


def test_noncallable_direct_i64_map_list_symbol_keeps_old_wheel_behavior(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Free-threaded/compatibility builds may expose the optional capability as None."""
    from fpstreams.execution import native as native_execution

    generic = native_execution._native.materialize_i64
    calls = 0

    def tracked(*arguments: object) -> object:
        nonlocal calls
        calls += 1
        return generic(*arguments)

    monkeypatch.setattr(
        native_execution._native,
        "materialize_i64_map_exact_list_v1",
        None,
        raising=False,
    )
    monkeypatch.setattr(native_execution._native, "materialize_i64", tracked)

    assert fpstreams.flow(list(range(16))).map(fpstreams.item + 1).to_list() == list(range(1, 17))
    assert calls == 1


@pytest.mark.parametrize("container", [list, tuple], ids=["list", "tuple"])
@pytest.mark.parametrize("negated", [False, True], ids=["filter", "reject"])
def test_auto_exact_i64_filter_list_retains_selected_object_identity(
    monkeypatch: pytest.MonkeyPatch,
    container: type[list[int]] | type[tuple[int, ...]],
    negated: bool,
) -> None:
    from fpstreams import _native

    values = container(int(str(10_000 + index)) for index in range(1_024))
    expression = fpstreams.item % 3 == 0
    direct = _native.materialize_i64_filter_exact_list_v1
    calls: list[tuple[object, object, bool]] = []

    def tracked(source: object, instructions: object, reject: bool) -> object:
        calls.append((source, instructions, reject))
        return direct(source, instructions, reject)

    monkeypatch.setattr(_native, "materialize_i64_filter_exact_list_v1", tracked)
    source_flow = fpstreams.flow(values)
    result = (
        source_flow.reject(expression).to_list()
        if negated
        else source_flow.filter(expression).to_list()
    )
    expected = [value for value in values if bool(expression(value)) is not negated]

    assert result == expected
    assert all(actual is source for actual, source in zip(result, expected, strict=True))
    assert calls == [(values, expression.native_instructions(), negated)]


def test_direct_i64_filter_list_late_decline_replays_python_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fpstreams import _native

    values: list[object] = [*range(511), 2.5]
    direct = _native.materialize_i64_filter_exact_list_v1
    calls = 0

    def tracked(source: object, instructions: object, negated: bool) -> object:
        nonlocal calls
        calls += 1
        return direct(source, instructions, negated)

    monkeypatch.setattr(_native, "materialize_i64_filter_exact_list_v1", tracked)
    result = fpstreams.flow(values).filter(fpstreams.item >= 0).to_list()

    assert result == values
    assert all(actual is source for actual, source in zip(result, values, strict=True))
    assert calls == 1


def test_direct_i64_filter_list_respects_the_measured_crossover(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fpstreams import _native

    values = list(range(7))
    monkeypatch.setattr(
        _native,
        "materialize_i64_filter_exact_list_v1",
        lambda *_arguments: pytest.fail("sub-threshold filters must remain on the Python path"),
    )

    assert fpstreams.flow(values).filter(fpstreams.item % 2 == 0).to_list() == values[::2]


def test_missing_direct_i64_filter_symbol_keeps_python_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fpstreams.execution import native as native_execution

    values = [int(str(10_000 + index)) for index in range(512)]
    monkeypatch.setattr(
        native_execution._native,
        "materialize_i64_filter_exact_list_v1",
        None,
        raising=False,
    )

    result = fpstreams.flow(values).filter(fpstreams.item % 2 == 0).to_list()
    expected = values[::2]
    assert all(actual is source for actual, source in zip(result, expected, strict=True))


@pytest.mark.parametrize("hook_site", ["builtins", "source_globals"])
def test_direct_i64_filter_preserves_retained_source_iter_hooks(
    monkeypatch: pytest.MonkeyPatch,
    hook_site: str,
) -> None:
    """A retained list still opens through the live iterator binding before filtering."""
    import builtins

    from fpstreams.planning import source as source_module

    values = list(range(8))
    query = fpstreams.flow(values).filter(fpstreams.item % 2 == 0)
    original_iter = iter
    calls = 0

    def replacement(source: object, *arguments: object) -> Iterator[object]:
        nonlocal calls
        if source is values:
            calls += 1
            return original_iter([1_001, 1_002, 1_003])
        return original_iter(source, *arguments)  # type: ignore[call-overload]

    if hook_site == "builtins":
        monkeypatch.setattr(builtins, "iter", replacement)
    else:
        monkeypatch.setattr(source_module, "iter", replacement, raising=False)

    assert query.to_list() == [1_002]
    assert calls == 1
