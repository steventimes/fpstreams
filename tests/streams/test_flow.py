"""streams flow regression contracts."""

from __future__ import annotations

import json
import math
import sys
import threading
from collections.abc import AsyncIterator, Callable, Iterable, Iterator
from contextlib import closing
from pathlib import Path
from typing import Any

import pytest

import fpstreams
from fpstreams import Downstream, Gatherer, NativeUnsupportedError, SelectionError, flow


@pytest.mark.parametrize("size", [1, 7, 127, 1024])
@pytest.mark.parametrize("engine", ["python", "auto"])
def test_rolling_statistics_match_independent_recomputation_with_outlier_eviction(size, engine):
    import statistics

    values = [10**12 + (i % 13) / 8 for i in range(600)]
    values[200] = 10**15
    values[300:320] = [None] * 20
    pipeline = flow(values).with_engine(engine).rolling_statistics(size, ddof=1)
    first = pipeline.to_list()
    assert pipeline.to_list() == first
    for index, (count, mean, variance) in enumerate(first):
        window = [v for v in values[max(0, index - size + 1) : index + 1] if v is not None]
        assert count == len(window)
        if not window:
            assert mean is variance is None
        else:
            expected_mean = statistics.mean(window)
            assert mean == pytest.approx(expected_mean, abs=max(0.001, 4 * math.ulp(expected_mean)))
            if len(window) == 1:
                assert variance is None
            else:
                assert variance == pytest.approx(statistics.variance(window), rel=2e-5, abs=1e-6)


def test_rolling_statistics_is_lazy_reusable_and_closes_on_early_stop_and_error(monkeypatch):
    events = []

    def source():
        try:
            for value in [2, None, 4, float("nan")]:
                events.append(value)
                yield {"value": value}
        finally:
            events.append("close")

    pipeline = flow.defer(source).rolling_statistics(2, selector="value")
    assert not events
    assert pipeline.take(3).to_list() == [(1, 2, 0), (1, 2, 0), (1, 4, 0)]
    assert events == [2, None, 4, "close"]
    with pytest.raises(ValueError, match="finite"):
        pipeline.to_list()
    assert events[-1] == "close"
    assert flow([]).rolling_statistics(2).to_list() == []

    native = getattr(fpstreams, "_native", None)
    if native is not None and hasattr(native, "RollingStatistics"):

        def forbidden(*args):
            raise AssertionError("native state should not open")

        monkeypatch.setattr(native, "RollingStatistics", forbidden)
        expected = [(1, 1.0, 0.0), (2, 1.5, 0.25)]
        assert flow([1, 2]).with_engine("python").rolling_statistics(2).to_list() == expected
        late_python = flow([1, 2]).rolling_statistics(2).with_engine("python")
        assert late_python.to_list() == expected
        assert list(late_python) == expected
        assert late_python.to_tuple() == tuple(expected)
        assert late_python.count() == 2
        assert late_python.map_accum(0, lambda count, item: (count + 1, item)).to_list() == expected
        with pytest.raises(AssertionError, match="native state should not open"):
            late_python.with_engine("auto").to_list()
        with pytest.raises(AssertionError, match="native state should not open"):
            flow([1, 2]).with_engine("python").rolling_statistics(2).with_engine("auto").to_list()
        assert flow([1, 2]).rolling_statistics(sys.maxsize + 1).to_list() == expected
        monkeypatch.setattr(native, "RollingStatistics", None)
        assert flow([1, 2]).rolling_statistics(2).to_list() == expected


def test_flow_collects_any_iterable() -> None:
    assert flow(range(4)).to_list() == [0, 1, 2, 3]


@pytest.mark.parametrize("method", ["take", "limit", "drop", "skip"])
@pytest.mark.parametrize("count", [float("nan"), float("inf"), 2.5, 2.0])
def test_prefix_counts_reject_nonintegers_before_opening_source(method, count):
    opened = []

    def source():
        opened.append(True)
        return iter(range(5))

    with pytest.raises(TypeError):
        getattr(flow.defer(source), method)(count)
    assert opened == []


@pytest.mark.parametrize("engine", ["auto", "python"])
@pytest.mark.parametrize(
    "method,want", [("take", [0, 1]), ("limit", [0, 1]), ("drop", [2, 3, 4]), ("skip", [2, 3, 4])]
)
def test_prefix_counts_normalize_index_once_and_close_source(engine, method, want):
    indexed = []
    closed = []

    class Count:
        def __index__(self):
            indexed.append(True)
            return 2

    def source():
        try:
            yield from range(5)
        finally:
            closed.append(True)

    query = getattr(flow.defer(source).with_engine(engine), method)(Count())
    assert indexed == [True]
    assert closed == []
    assert query.to_list() == want
    assert query.to_list() == want
    assert indexed == [True]
    assert closed == [True, True]


@pytest.mark.parametrize("method", ["take", "limit", "drop", "skip"])
def test_prefix_counts_preserve_index_error_without_opening_source(method):
    failure = RuntimeError("count conversion failed")
    opened = []

    class Count:
        def __index__(self):
            raise failure

    def source():
        opened.append(True)
        return iter(range(5))

    with pytest.raises(RuntimeError) as caught:
        getattr(flow.defer(source), method)(Count())
    assert caught.value is failure
    assert opened == []


@pytest.mark.parametrize("constructor", [flow, fpstreams.Flow])
def test_unfold_is_lazy_restarts_from_seed_and_keeps_none_values(constructor):
    calls = []

    def step(state):
        calls.append(state)
        return None if state == 0 else (None if state == 2 else state, state - 1)

    query = constructor.unfold(2, step)
    assert calls == []
    assert query.take(0).to_list() == []
    assert calls == []
    assert query.to_list() == [None, 1]
    assert query.to_list() == [None, 1]
    assert calls == [2, 1, 0, 2, 1, 0]
    assert constructor.unfold(None, lambda state: None).to_list() == []


@pytest.mark.parametrize("engine", ["auto", "python"])
def test_unfold_stops_without_calling_the_next_step_and_preserves_output_identity(engine):
    value = object()
    calls = []

    def step(state):
        calls.append(state)
        return value, state + 1

    query = flow.unfold(0, step).with_engine(engine)
    assert query.take(1).to_list() == [value]
    assert calls == [0]
    observed = query.run_with_report("first")
    assert observed.value is value
    assert calls == [0, 0]


def test_unfold_validates_callbacks_and_propagates_step_errors():
    with pytest.raises(TypeError, match="function must be callable"):
        flow.unfold(0, None)
    failure = ValueError("step failed")

    def fail(state):
        raise failure

    with pytest.raises(ValueError) as caught:
        flow.unfold(0, fail).to_list()
    assert caught.value is failure
    for malformed in (False, (1,), (1, 2, 3)):
        with pytest.raises((TypeError, ValueError)):
            flow.unfold(0, lambda state, result=malformed: result).first()


@pytest.mark.parametrize("engine", ["auto", "python"])
def test_map_accum_emits_outputs_and_keeps_per_iteration_state(engine):
    calls = []

    def step(state, item):
        calls.append((state, item))
        return state + item, f"{state}:{item}"

    query = flow([2, 3]).with_engine(engine).map_accum(0, step)
    assert calls == []
    assert query.to_list() == ["0:2", "2:3"]
    assert query.to_list() == ["0:2", "2:3"]
    assert calls == [(0, 2), (2, 3), (0, 2), (2, 3)]
    assert flow([]).map_accum(0, step).to_list() == []
    with closing(iter(query)) as left, closing(iter(query)) as right:
        assert [next(left), next(right), next(left), next(right)] == ["0:2", "0:2", "2:3", "2:3"]


def test_map_accum_preserves_none_identity_early_close_and_exception_identity():
    seen = []
    closed = []
    value = object()
    failure = ValueError("state step failed")

    def source():
        try:
            for item in (value, None):
                seen.append(item)
                yield item
        finally:
            closed.append(True)

    def step(state, item):
        if state:
            raise failure
        return state + 1, item

    assert flow.defer(source).map_accum(0, step).take(1).to_list() == [value]
    assert seen == [value] and closed == [True]
    assert flow([None, value]).map_accum(None, lambda state, item: (state, item)).to_list() == [
        None,
        value,
    ]
    with pytest.raises(ValueError) as caught:
        flow.defer(source).map_accum(0, step).to_list()
    assert caught.value is failure
    assert closed == [True, True]
    with pytest.raises(TypeError, match="function must be callable"):
        flow([]).map_accum(0, None)
    with pytest.raises(ValueError):
        flow([1]).map_accum(0, lambda state, item: (item,)).to_list()


def test_map_accum_does_not_parallelize_state_transitions():
    caller = threading.get_ident()

    def step(total, value):
        assert threading.get_ident() == caller
        total += value
        return total, total

    query = flow([1, 2, 3]).parallel(workers=2, backend="thread", buffer=2)
    assert query.map_accum(0, step).to_list() == [1, 3, 6]


@pytest.mark.parametrize("engine", ["auto", "python"])
@pytest.mark.parametrize(
    ("source", "size", "prefix", "tail"),
    [
        (range(1 << 65), 1 << 65, [0, 1, 2], (1 << 65) - 1),
        (range(4, (1 << 65) + 1, 4), 1 << 63, [4, 8, 12], 1 << 65),
        (
            range(1 << 65, 1, -4),
            1 << 63,
            [1 << 65, (1 << 65) - 4, (1 << 65) - 8],
            4,
        ),
        (
            range(-(1 << 63), (1 << 63) - 1),
            (1 << 64) - 1,
            [-(1 << 63), -(1 << 63) + 1, -(1 << 63) + 2],
            (1 << 63) - 2,
        ),
    ],
)
def test_flow_supports_ranges_larger_than_ssize_t(
    source: range, size: int, prefix: list[int], tail: int, engine: str
) -> None:
    values = flow(source).with_engine(engine)

    assert values.count() == size
    assert values.last() == tail
    assert values.nth(-1) == tail
    assert values.take(3).to_list() == prefix
    assert values.take(3).to_tuple() == tuple(prefix)
    assert values.map(lambda value: value + 1).take(3).to_list() == [value + 1 for value in prefix]
    assert values.chunk(3).take(1).to_list() == [tuple(prefix)]


def test_one_shot_source_fails_instead_of_silently_returning_empty() -> None:
    values = flow(iter([1, 2]))

    assert values.to_list() == [1, 2]
    with pytest.raises(Exception) as captured:
        values.to_list()

    assert type(captured.value).__name__ == "FlowConsumedError"


def test_retained_source_metadata_declines_replaced_instance_factory() -> None:
    """Cached size and native data cannot outrank the live source factory."""
    values = flow([1, 2, 3])
    values._pipeline.source._factory = lambda: iter((9, 8))

    assert values.to_list() == [9, 8]
    assert values.count() == 2
    assert values.last() == 8
    assert values.nth(0) == 9
    assert values.frequencies() == {9: 1, 8: 1}


def test_intermediate_operations_are_lazy_and_do_not_mutate_the_parent() -> None:
    seen: list[int] = []
    base = flow([1, 2, 3]).tap(seen.append)
    doubled = base.map(lambda value: value * 2)

    assert seen == []
    assert doubled.to_list() == [2, 4, 6]
    assert seen == [1, 2, 3]
    assert base.to_list() == [1, 2, 3]


def test_creating_a_flow_does_not_open_or_prescan_a_reiterable_source() -> None:
    class CountedList(list[int]):
        opens = 0
        length_checks = 0

        def __iter__(self):
            self.opens += 1
            return super().__iter__()

        def __len__(self):
            self.length_checks += 1
            return super().__len__()

    source = CountedList([1, 2, 3])
    pipeline = flow(source).map(lambda value: value + 1).take(1)

    assert source.opens == 0
    assert source.length_checks == 0
    assert pipeline.to_list() == [2]
    assert source.opens == 1


def test_stateless_transformations_compose_in_encounter_order() -> None:
    result = (
        flow(range(6))
        .filter(lambda value: value % 2 == 0)
        .flat_map(lambda value: (value, -value))
        .reject(lambda value: value == 0)
        .to_list()
    )

    assert result == [2, -2, 4, -4]


def test_direct_flat_map_materialization_does_not_probe_length_hint() -> None:
    """The list fast path preserves flat_map's iterator protocol without extra probes."""

    class Values:
        def __init__(self, value: int) -> None:
            self.value = value

        def __iter__(self) -> Iterator[int]:
            return iter((self.value, -self.value))

        def __length_hint__(self) -> int:
            raise AssertionError("flat_map must not request an optional length hint")

    assert flow([1, 2]).flat_map(Values).to_list() == [1, -1, 2, -2]


def test_direct_flat_map_materialization_skips_the_intermediate_iterator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A lone flat-map can append its values directly into the built-in list sink."""
    from fpstreams.streams._terminals import structural as _flow_structural_list

    append = _flow_structural_list._append_flat_map_values
    calls = 0

    def tracked(*arguments: object) -> None:
        nonlocal calls
        calls += 1
        append(*arguments)  # type: ignore[arg-type]

    monkeypatch.setattr(_flow_structural_list, "_append_flat_map_values", tracked)

    assert flow([1, 2]).flat_map(lambda value: (value, -value)).to_list() == [1, -1, 2, -2]
    assert calls == 1


def test_direct_flat_map_rechecks_materialized_appender_after_source_open() -> None:
    """Source opening cannot leave a stale explode or unpivot appender snapshot."""
    query = fpstreams.rows([{"id": 1, "tags": ["a", "b"]}]).explode("tags")
    source = query._flow._pipeline.source
    operation = query._flow._pipeline.operations[0]
    original_factory = source._factory

    def replacement(row: dict[str, object]) -> tuple[dict[str, object]]:
        return ({"replacement": row["id"]},)

    def open_source() -> Iterator[object]:
        object.__setattr__(operation, "function", replacement)
        return original_factory()

    source._factory = open_source

    assert query.to_list() == [{"replacement": 1}]


def test_direct_flat_map_materialization_handles_mixed_iterable_protocols_once() -> None:
    """Adaptive list extension rechecks every callback result without reiterating custom values."""
    iterations = 0

    class Values:
        def __iter__(self) -> Iterator[int]:
            nonlocal iterations
            iterations += 1
            yield 4
            yield 5

    nested: list[Iterable[int]] = [(0,), [1, 2], (value for value in (3,)), Values(), range(6, 8)]

    assert flow(range(len(nested))).flat_map(nested.__getitem__).to_list() == list(range(8))
    assert iterations == 1


def test_direct_flat_map_callback_exhaustion_and_failure_close_the_source() -> None:
    """Callback StopIteration keeps PEP 479 chaining and every failure releases upstream."""
    exhausted = StopIteration("flat-map stopped")
    failure = RuntimeError("flat-map failed")

    def evaluate(error: BaseException) -> None:
        closed = False

        def source() -> Iterator[int]:
            nonlocal closed
            try:
                yield 1
                yield 2
            finally:
                closed = True

        def callback(value: int) -> list[int]:
            if value == 2:
                raise error
            return [value]

        try:
            flow(source()).flat_map(callback).to_list()
        finally:
            assert closed

    with pytest.raises(RuntimeError, match=r"^flat-map failed$") as captured:
        evaluate(failure)
    assert captured.value is failure
    with pytest.raises(RuntimeError, match=r"^generator raised StopIteration$") as wrapped:
        evaluate(exhausted)
    assert wrapped.value.__cause__ is exhausted


def test_direct_flat_map_releases_partial_output_on_error() -> None:
    """A callback failure cannot retain values already appended to the private result list."""
    import gc
    from weakref import ref

    failure = ValueError("flat-map failed")
    references: list[ref[object]] = []

    class Marker:
        pass

    def callback(value: int) -> list[Marker]:
        if value:
            raise failure
        marker = Marker()
        references.append(ref(marker))
        return [marker]

    with pytest.raises(ValueError) as captured:
        flow([0, 1]).flat_map(callback).to_list()

    gc.collect()
    assert captured.value is failure
    assert references[0]() is None


def test_scan_materialization_preserves_callback_order() -> None:
    """Scan invokes the live reducer once per source item in encounter order."""
    calls: list[tuple[int, int]] = []

    def add(total: int, value: int) -> int:
        calls.append((total, value))
        return total + value

    assert flow([1, 2, 3]).with_engine("python").scan(0, add).to_list() == [1, 3, 6]
    assert calls == [(0, 1), (1, 2), (3, 3)]


def test_direct_scan_materialization_preserves_callback_exhaustion_and_close() -> None:
    """Reducer exhaustion keeps PEP 479 error chaining and releases a one-shot source."""
    exhausted = StopIteration("reducer stopped")
    closed = False

    def source() -> Iterator[int]:
        nonlocal closed
        try:
            yield 1
            yield 2
            yield 3
        finally:
            closed = True

    def reducer(total: int, value: int) -> int:
        if value == 2:
            raise exhausted
        return total + value

    with pytest.raises(RuntimeError, match=r"^generator raised StopIteration$") as captured:
        flow(source()).with_engine("python").scan(0, reducer).to_list()

    assert captured.value.__cause__ is exhausted
    assert closed


def test_direct_operator_add_scan_preserves_numeric_and_custom_add_semantics() -> None:
    """The exact add shortcut keeps arbitrary integers, floats, mutation, and add dispatch."""
    import operator

    source = [1, 2]
    calls: list[int] = []

    class Total:
        def __init__(self, value: int) -> None:
            self.value = value

        def __add__(self, value: int) -> Total:
            calls.append(value)
            if value == 1:
                source.append(3)
            return Total(self.value + value)

    states = flow(source).scan(Total(0), operator.add).to_list()

    assert [state.value for state in states] == [1, 3, 6]
    assert calls == [1, 2, 3]
    assert flow([10**80, 10**90]).scan(10**100, operator.add).to_list() == [
        10**100 + 10**80,
        10**100 + 10**80 + 10**90,
    ]
    assert flow([0.25, 0.5]).scan(0.0, operator.add).to_list() == [0.25, 0.75]


def test_direct_operator_add_scan_preserves_stop_iteration_chaining() -> None:
    """A custom add exhaustion remains a PEP 479 RuntimeError with the original cause."""
    import operator

    exhausted = StopIteration("add stopped")

    class Total:
        def __add__(self, _value: object) -> Total:
            raise exhausted

    with pytest.raises(RuntimeError, match=r"^generator raised StopIteration$") as captured:
        flow([1]).scan(Total(), operator.add).to_list()

    assert captured.value.__cause__ is exhausted


def test_direct_operator_add_scan_respects_the_live_operator_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Existing plans retain their callable while new plans see a replaced public binding."""
    import operator

    original = operator.add
    existing = flow([1, 2]).scan(0, original)
    replacement_calls = 0

    def replacement(left: int, right: int) -> int:
        nonlocal replacement_calls
        replacement_calls += 1
        return left + right + 10

    monkeypatch.setattr(operator, "add", replacement)

    assert existing.to_list() == [1, 3]
    assert flow([1, 2]).scan(0, operator.add).to_list() == [11, 23]
    assert replacement_calls == 2


def test_direct_scan_materialization_declines_active_failpoints(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Instrumentation retains the canonical scan iterator boundary."""
    from fpstreams.execution import sync_ops
    from fpstreams.planning.sync import ScanOp
    from fpstreams.runtime.failpoints import failpoint

    handler = sync_ops.OPERATION_HANDLERS[ScanOp]
    calls = 0

    def tracked(*arguments: object, **keywords: object) -> Iterator[object]:
        nonlocal calls
        calls += 1
        return handler(*arguments, **keywords)  # type: ignore[arg-type]

    monkeypatch.setitem(sync_ops.OPERATION_HANDLERS, ScanOp, tracked)
    with failpoint("unrelated.transition", RuntimeError("unused")):
        result = (
            flow([1, 2, 3])
            .with_engine("python")
            .scan(0, lambda total, value: total + value)
            .to_list()
        )

    assert result == [1, 3, 6]
    assert calls == 1


def test_direct_flat_map_respects_a_replaced_list_terminal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A live replacement for list remains the terminal owner instead of the direct sink."""
    from fpstreams.streams import flow_terminals

    canonical_list = list
    calls = 0

    def replacement(values: Iterable[object]) -> list[object]:
        nonlocal calls
        calls += 1
        return canonical_list(values)

    monkeypatch.setattr(flow_terminals, "list", replacement, raising=False)

    assert flow([1, 2]).flat_map(lambda value: [value, -value]).to_list() == [1, -1, 2, -2]
    assert calls == 1


def test_direct_flat_map_materialization_does_not_reiterate_nested_iterators() -> None:
    """Collecting preserves yield-from's single iter() call on each returned iterator."""

    class Values:
        def __init__(self) -> None:
            self.index = 0
            self.iterations = 0

        def __iter__(self) -> Values:
            self.iterations += 1
            if self.iterations > 1:
                raise RuntimeError("nested iterator reopened")
            return self

        def __next__(self) -> int:
            if self.index == 2:
                raise StopIteration
            self.index += 1
            return self.index

    nested = Values()
    assert flow([None]).flat_map(lambda _value: nested).to_list() == [1, 2]
    assert nested.iterations == 1


def test_direct_flat_map_propagates_nested_next_failures_without_extra_close() -> None:
    """A failing nested iterator keeps canonical yield-from ownership semantics."""
    failure = RuntimeError("nested next failed")

    class Source(list[None]):
        pass

    def evaluate(source: list[None]) -> list[str]:
        events: list[str] = []

        class Values:
            def __iter__(self) -> Values:
                events.append("iter")
                return self

            def __next__(self) -> int:
                events.append("next")
                raise failure

            def close(self) -> None:
                events.append("close")

        with pytest.raises(RuntimeError) as captured:
            flow(source).flat_map(lambda _value: Values()).to_list()
        assert captured.value is failure
        return events

    assert evaluate([None]) == ["iter", "next"]
    assert evaluate(Source([None])) == ["iter", "next"]


def test_direct_linear_sink_composes_surrounding_map_filter_stages() -> None:
    """A structural stage retains callback order in a real multi-stage user pipeline."""
    events: list[tuple[str, int]] = []

    pipeline = (
        flow(list(range(8)))
        .map(lambda value: events.append(("before", value)) or value + 1)
        .filter(lambda value: value % 2 == 0)
        .flat_map(lambda value: (value, -value))
        .filter(lambda value: value > 0)
        .map(lambda value: events.append(("after", value)) or value * 10)
    )
    result = pipeline.to_list()

    assert result == [20, 40, 60, 80]
    assert events == [
        ("before", 0),
        ("before", 1),
        ("after", 2),
        ("before", 2),
        ("before", 3),
        ("after", 4),
        ("before", 4),
        ("before", 5),
        ("after", 6),
        ("before", 6),
        ("before", 7),
        ("after", 8),
    ]


def test_direct_linear_sink_closes_an_active_flat_map_iterator() -> None:
    """A downstream callback failure closes flat_map's currently delegated iterator."""
    closed: list[bool] = []
    failure = RuntimeError("downstream failed")

    def nested(value: int) -> Iterator[int]:
        try:
            yield value
            yield value + 1
        finally:
            closed.append(True)

    def fail(_value: int) -> int:
        raise failure

    pipeline = (
        flow([1, 2]).with_engine("python").map(lambda value: value).flat_map(nested).map(fail)
    )
    with pytest.raises(RuntimeError) as captured:
        pipeline.to_list()

    assert captured.value is failure
    assert closed == [True]


@pytest.mark.parametrize("terminal", ["to_list", "to_tuple"])
@pytest.mark.parametrize("source_kind", ["generator", "defer", "source"])
def test_direct_map_filter_materialization_opens_any_python_source_once(
    terminal: str,
    source_kind: str,
) -> None:
    """List and tuple sinks own one real open for every linear Python source."""
    from fpstreams.planning.source import Source, SourceCapabilities

    events: list[str] = []

    def values() -> Iterator[int]:
        events.append("open")
        try:
            yield from range(6)
        finally:
            events.append("close")

    if source_kind == "generator":
        candidate = flow(values())
    elif source_kind == "defer":
        candidate = flow.defer(values)
    else:
        opens = 0

        def open_source() -> Iterator[int]:
            nonlocal opens
            opens += 1
            return values()

        candidate = fpstreams.Flow(
            Source(open_source, SourceCapabilities(reiterable=True, exact_size=None))
        )

    pipeline = (
        candidate.with_engine("python")
        .map(lambda value: value + 1)
        .filter(lambda value: value % 2 == 0)
    )

    assert getattr(pipeline, terminal)() == ([2, 4, 6] if terminal == "to_list" else (2, 4, 6))
    assert events == ["open", "close"]
    if source_kind == "source":
        assert opens == 1
    if source_kind == "generator":
        with pytest.raises(fpstreams.FlowConsumedError):
            getattr(pipeline, terminal)()


@pytest.mark.parametrize("terminal", ["to_list", "to_tuple"])
@pytest.mark.parametrize(
    ("build", "expected"),
    [
        (
            lambda pipeline: pipeline.take(7).drop(2).take_while(lambda value: value < 6),
            [2, 3, 4, 5],
        ),
        (
            lambda pipeline: pipeline.scan(0, lambda total, value: total + value).chunk(2).map(sum),
            [1, 9, 25, 49, 81],
        ),
        (
            lambda pipeline: pipeline.zip(range(10, 20), strict=True).map(sum),
            [10, 12, 14, 16, 18, 20, 22, 24, 26, 28],
        ),
        (
            lambda pipeline: (
                pipeline.map(lambda value: 9 - value).sorted().unique().map(lambda value: value * 2)
            ),
            [0, 2, 4, 6, 8, 10, 12, 14, 16, 18],
        ),
    ],
)
def test_direct_linear_materialization_covers_operation_families(
    terminal: str,
    build: Callable[[Any], Any],
    expected: list[int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One canonical sink drains streaming, stateful, secondary, and barrier stages."""
    from fpstreams.streams import flow_terminals

    def unexpected_executor(*_args: object, **_kwargs: object) -> Iterator[object]:
        raise AssertionError("the direct Python materializer must own this linear plan")
        yield

    monkeypatch.setattr(flow_terminals, "execute_physical", unexpected_executor)
    pipeline = build(flow(value for value in range(10)).with_engine("python"))

    result = getattr(pipeline, terminal)()

    assert result == (expected if terminal == "to_list" else tuple(expected))


@pytest.mark.parametrize(
    ("terminal", "global_name", "expected"),
    [
        ("to_list", "list", [2, 3, 4]),
        ("to_tuple", "tuple", (2, 3, 4)),
    ],
)
def test_direct_materialization_captures_constructor_before_source_open(
    monkeypatch: pytest.MonkeyPatch,
    terminal: str,
    global_name: str,
    expected: object,
) -> None:
    """An opener cannot replace the terminal constructor after canonical lookup."""
    from fpstreams.planning.source import Source, SourceCapabilities
    from fpstreams.streams import flow_terminals

    events: list[str] = []

    def replacement(_values: Iterable[object]) -> object:
        events.append("replacement")
        raise AssertionError("the constructor was resolved before Source.open")

    def open_source() -> Iterator[int]:
        events.append("open")
        monkeypatch.setattr(flow_terminals, global_name, replacement, raising=False)
        return iter((1, 2, 3))

    source = Source(open_source, SourceCapabilities(reiterable=True, exact_size=None))
    pipeline = fpstreams.Flow(source).with_engine("python").map(lambda value: value + 1)

    assert getattr(pipeline, terminal)() == expected
    assert events == ["open"]


@pytest.mark.parametrize(
    ("terminal", "global_name"),
    [("to_list", "list"), ("to_tuple", "tuple")],
)
def test_replaced_materializer_keeps_source_lazy(
    monkeypatch: pytest.MonkeyPatch,
    terminal: str,
    global_name: str,
) -> None:
    """A noncanonical constructor receives the canonical lazy executor without an eager open."""
    from fpstreams.planning.source import Source, SourceCapabilities
    from fpstreams.streams import flow_terminals

    opens = 0
    marker = object()
    received: list[object] = []

    def open_source() -> Iterator[int]:
        nonlocal opens
        opens += 1
        return iter((1, 2, 3))

    def replacement(values: Iterable[object]) -> object:
        received.append(values)
        return marker

    monkeypatch.setattr(flow_terminals, global_name, replacement, raising=False)
    source = Source(open_source, SourceCapabilities(reiterable=True, exact_size=None))
    pipeline = fpstreams.Flow(source).with_engine("python").map(lambda value: value + 1)

    assert getattr(pipeline, terminal)() is marker
    assert opens == 0
    assert len(received) == 1


def test_direct_materialization_converts_source_open_stop_iteration() -> None:
    """Opening remains inside a generator boundary, preserving PEP 479 translation."""
    from fpstreams.planning.source import Source, SourceCapabilities

    failure = StopIteration("source open stopped")

    def open_source() -> Iterator[int]:
        raise failure

    pipeline = fpstreams.Flow(
        Source(open_source, SourceCapabilities(reiterable=True, exact_size=None))
    ).map(lambda value: value)

    with pytest.raises(RuntimeError, match=r"^generator raised StopIteration$") as captured:
        pipeline.with_engine("python").to_list()

    assert captured.value.__cause__ is failure


@pytest.mark.parametrize("boundary", ["map", "bool"])
def test_direct_materialization_preserves_callback_stop_iteration_exhaustion(
    boundary: str,
) -> None:
    """Map callbacks and filter truth tests retain CPython iterator exhaustion semantics."""
    from fpstreams.planning.source import Source, SourceCapabilities

    closes = 0

    class Values:
        def __init__(self) -> None:
            self.index = 0

        def __iter__(self) -> Values:
            return self

        def __next__(self) -> int:
            self.index += 1
            if self.index > 3:
                raise StopIteration
            return self.index

        def close(self) -> None:
            nonlocal closes
            closes += 1

    class StopOnTruth:
        def __bool__(self) -> bool:
            raise StopIteration("truth test stopped")

    def map_value(value: int) -> int:
        if value == 2:
            raise StopIteration("map stopped")
        return value

    def keep_value(value: int) -> object:
        return True if value == 1 else StopOnTruth()

    source = Source(Values, SourceCapabilities(reiterable=True, exact_size=None))
    candidate = fpstreams.Flow(source).with_engine("python")
    pipeline = candidate.map(map_value) if boundary == "map" else candidate.filter(keep_value)

    assert pipeline.to_tuple() == (1,)
    assert closes == 1


@pytest.fixture
def direct_python_filter_loop_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exercise direct-loop semantics independently from the measured runtime gate."""
    from fpstreams.streams._terminals import structural as _flow_structural_list

    monkeypatch.setattr(_flow_structural_list, "_CPYTHON_312_DIRECT_FILTER_LOOP", True)


@pytest.mark.parametrize("engine", ["auto", "python"])
@pytest.mark.parametrize("container", [list, tuple], ids=["list", "tuple"])
def test_direct_python_filter_list_uses_only_the_supported_runtime(
    monkeypatch: pytest.MonkeyPatch,
    engine: str,
    container: Callable[[Iterable[int]], Iterable[int]],
) -> None:
    """Production dispatch selects the measured CPython 3.12 loop and no other runtime."""
    from fpstreams.streams._terminals import structural as _flow_structural_list

    calls = 0
    original = _flow_structural_list._append_python_filter_values

    def tracked(*arguments: object) -> None:
        nonlocal calls
        calls += 1
        original(*arguments)  # type: ignore[arg-type]

    monkeypatch.setattr(_flow_structural_list, "_append_python_filter_values", tracked)

    assert flow(container((0, 1, 2))).with_engine(engine).filter(
        lambda value: value % 2 == 0
    ).to_list() == [0, 2]
    assert calls == int(_flow_structural_list._CPYTHON_312_DIRECT_FILTER_LOOP)


@pytest.mark.parametrize("engine", ["auto", "python"])
@pytest.mark.parametrize("container", [list, tuple], ids=["list", "tuple"])
def test_direct_python_filter_list_preserves_rows_and_identity(
    monkeypatch: pytest.MonkeyPatch,
    direct_python_filter_loop_enabled: None,
    engine: str,
    container: Callable[[Iterable[object]], Iterable[object]],
) -> None:
    """A retained builtin sequence enters the direct sink without normalizing its rows."""
    from fpstreams.streams._terminals import structural as _flow_structural_list

    calls = 0
    original = _flow_structural_list._append_python_filter_values

    def tracked(*arguments: object) -> None:
        nonlocal calls
        calls += 1
        original(*arguments)  # type: ignore[arg-type]

    monkeypatch.setattr(_flow_structural_list, "_append_python_filter_values", tracked)
    first = {"value": 1}
    second = {"value": 2}
    values = container((first, second))

    result = flow(values).with_engine(engine).filter(lambda row: row["value"] == 2).to_list()

    assert result == [second]
    assert result[0] is second
    assert calls == 1


def test_direct_python_filter_list_preserves_live_list_mutation(
    direct_python_filter_loop_enabled: None,
) -> None:
    """The direct sink consumes the same live list iterator as CPython filter."""

    def evaluate(candidate: bool) -> tuple[list[int], list[int]]:
        values = [0, 1, 2]

        def keep(value: int) -> bool:
            if value == 0:
                values.append(3)
            return value % 2 == 0

        result = flow(values).filter(keep).to_list() if candidate else list(filter(keep, values))
        return result, values

    assert evaluate(True) == evaluate(False) == ([0, 2], [0, 1, 2, 3])


@pytest.mark.parametrize("boundary", ["callback", "truth"])
def test_direct_python_filter_list_preserves_filter_stop_iteration_prefix(
    boundary: str,
    direct_python_filter_loop_enabled: None,
) -> None:
    """Predicate and truth-test exhaustion complete with CPython filter's retained prefix."""
    exhausted = StopIteration(f"{boundary} stopped")

    class StopsDuringTruth:
        def __bool__(self) -> bool:
            raise exhausted

    def keep(value: int) -> object:
        if value == 2:
            if boundary == "callback":
                raise exhausted
            return StopsDuringTruth()
        return True

    assert flow([1, 2, 3]).filter(keep).to_list() == [1]


def test_direct_python_filter_list_clears_partial_output_on_error(
    direct_python_filter_loop_enabled: None,
) -> None:
    """A callback failure releases already accepted values before propagating unchanged."""
    import gc
    from weakref import ref

    failure = ValueError("predicate failed")

    class Item:
        def __init__(self, value: int) -> None:
            self.value = value

    values: list[Item | None] = [Item(0), Item(1)]
    accepted = ref(values[0])  # type: ignore[arg-type]

    def keep(item: Item | None) -> bool:
        assert item is not None
        if item.value == 0:
            values[0] = None
            return True
        raise failure

    with pytest.raises(ValueError) as captured:
        flow(values).filter(keep).to_list()

    gc.collect()
    assert captured.value is failure
    assert accepted() is None


def test_direct_python_filter_list_rechecks_predicate_after_source_open(
    monkeypatch: pytest.MonkeyPatch,
    direct_python_filter_loop_enabled: None,
) -> None:
    """A source-time operation mutation deopts before invoking the stale predicate."""
    import builtins

    values = [1, 2, 3]
    query = flow(values).with_engine("python").filter(lambda _value: True)
    operation = query._pipeline.operations[0]
    original_iter = iter
    opens = 0

    def replacement(_value: int) -> bool:
        return False

    def open_iterator(source: object) -> Iterator[object]:
        nonlocal opens
        if source is values:
            opens += 1
            object.__setattr__(operation, "predicate", replacement)
        return original_iter(source)  # type: ignore[arg-type]

    monkeypatch.setattr(builtins, "iter", open_iterator)

    assert query.to_list() == []
    assert opens == 1


def test_direct_python_filter_list_rechecks_retained_size_after_open(
    monkeypatch: pytest.MonkeyPatch,
    direct_python_filter_loop_enabled: None,
) -> None:
    """An opener mutation deopts on the same live list iterator without losing appended rows."""
    import builtins

    from fpstreams.streams._terminals import structural as _flow_structural_list

    values = [1, 2, 3]
    query = flow(values).with_engine("python").filter(lambda value: value > 1)
    original_iter = iter
    opens = 0
    direct_calls = 0
    original_direct = _flow_structural_list._append_python_filter_values

    def tracked_direct(*arguments: object) -> None:
        nonlocal direct_calls
        direct_calls += 1
        original_direct(*arguments)  # type: ignore[arg-type]

    def open_iterator(source: object) -> Iterator[object]:
        nonlocal opens
        if source is values:
            opens += 1
            values.append(4)
        return original_iter(source)  # type: ignore[arg-type]

    monkeypatch.setattr(_flow_structural_list, "_append_python_filter_values", tracked_direct)
    monkeypatch.setattr(builtins, "iter", open_iterator)

    assert query.to_list() == [2, 3, 4]
    assert (opens, direct_calls) == (1, 0)


def test_direct_python_filter_list_declines_non_function_callables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Builtin, bound, callable-object, and forced-native predicates keep their executors."""
    from fpstreams.streams._terminals import structural as _flow_structural_list

    calls = 0
    original = _flow_structural_list._append_python_filter_values

    def tracked(*arguments: object) -> None:
        nonlocal calls
        calls += 1
        original(*arguments)  # type: ignore[arg-type]

    class Keeper:
        def keep(self, value: int) -> bool:
            return bool(value)

        def __call__(self, value: int) -> bool:
            return bool(value)

    monkeypatch.setattr(_flow_structural_list, "_append_python_filter_values", tracked)
    keeper = Keeper()
    for predicate in (bool, keeper.keep, keeper):
        assert flow([0, 1]).filter(predicate).to_list() == [1]
    with pytest.raises(NativeUnsupportedError):
        flow([0, 1]).with_engine("native").filter(lambda value: value > 0).to_list()

    assert calls == 0


@pytest.mark.parametrize("boundary", ["normal", "callback_stop", "callback_error"])
def test_direct_materialization_preserves_close_failure_boundaries(
    boundary: str,
) -> None:
    """Cleanup keeps the canonical primary error, cause, and note boundaries."""
    from fpstreams.planning.source import Source, SourceCapabilities

    callback_failure = ValueError("callback failed")
    close_failure: BaseException = (
        StopIteration("close stopped") if boundary == "normal" else OSError("close failed")
    )
    closes = 0

    class Values:
        def __init__(self) -> None:
            self.index = 0

        def __iter__(self) -> Values:
            return self

        def __next__(self) -> int:
            self.index += 1
            if self.index > 2:
                raise StopIteration
            return self.index

        def close(self) -> None:
            nonlocal closes
            closes += 1
            raise close_failure

    def transform(value: int) -> int:
        if value == 2 and boundary == "callback_stop":
            raise StopIteration("callback stopped")
        if value == 2 and boundary == "callback_error":
            raise callback_failure
        return value

    source = Source(Values, SourceCapabilities(reiterable=True, exact_size=None))
    pipeline = fpstreams.Flow(source).with_engine("python").map(transform)

    if boundary == "normal":
        with pytest.raises(RuntimeError, match=r"^generator raised StopIteration$") as captured:
            pipeline.to_list()
        assert captured.value.__cause__ is close_failure
    elif boundary == "callback_stop":
        with pytest.raises(OSError, match=r"^close failed$") as captured:
            pipeline.to_list()
        assert captured.value is close_failure
    else:
        with pytest.raises(ValueError, match=r"^callback failed") as captured:
            pipeline.to_list()
        assert captured.value is callback_failure
        assert captured.value.__notes__ == ["cleanup failed with OSError: close failed"]
    assert closes == 1


def test_non_retained_set_materialization_stays_on_the_canonical_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The generalized linear source sink is intentionally limited to list and tuple."""
    from fpstreams.streams import flow_terminals

    executions = 0
    execute_physical = flow_terminals.execute_physical

    def tracked_executor(plan: object) -> Iterator[Any]:
        nonlocal executions
        executions += 1
        return execute_physical(plan)  # type: ignore[arg-type]

    monkeypatch.setattr(flow_terminals, "execute_physical", tracked_executor)

    assert flow(value for value in range(4)).with_engine("python").map(
        lambda value: value % 2
    ).to_set() == {0, 1}
    assert executions == 1


def test_direct_linear_materialization_leaves_parallel_scheduling_to_the_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Removing forwarding frames must not alter explicit worker scheduling semantics."""
    from fpstreams.streams import flow_terminals

    executions = 0
    execute_physical = flow_terminals.execute_physical

    def tracked_executor(plan: object) -> Iterator[Any]:
        nonlocal executions
        executions += 1
        return execute_physical(plan)  # type: ignore[arg-type]

    monkeypatch.setattr(flow_terminals, "execute_physical", tracked_executor)
    result = (
        flow(value for value in range(8))
        .with_engine("python")
        .map_parallel(lambda value: value + 1, workers=2, backend="thread", buffer=2)
        .to_list()
    )

    assert result == list(range(1, 9))
    assert executions == 1


def test_direct_linear_sink_preserves_live_list_pulls_and_take_while_boundary() -> None:
    """Callbacks can still affect later list pulls, and take_while stops at its boundary."""
    values = [0, 1, 2, 3]
    pulled: list[int] = []

    def observe(value: int) -> int:
        pulled.append(value)
        if value == 0:
            values[1] = 10
        return value

    result = (
        flow(values)
        .with_engine("python")
        .map(observe)
        .take_while(lambda value: value < 3 or value == 10)
        .map(lambda value: value + 1)
        .to_list()
    )

    assert result == [1, 11, 3]
    assert pulled == [0, 10, 2, 3]


@pytest.mark.parametrize("terminal", ["to_list", "to_tuple"])
@pytest.mark.parametrize("source_type", [list, tuple, range])
@pytest.mark.parametrize("engine", ["auto", "python"])
@pytest.mark.parametrize(
    ("build", "expected"),
    [
        (lambda values: values.take(6).drop(2), [2, 3, 4, 5]),
        (lambda values: values.drop(2).take(4), [2, 3, 4, 5]),
        (lambda values: values.drop(2).take(5).drop(2).take(2), [4, 5]),
        (lambda values: values.take(0).drop(10), []),
        (lambda values: values.drop(20).take(3), []),
    ],
)
def test_retained_sequence_windows_preserve_operation_order(
    terminal: str,
    source_type: type,
    engine: str,
    build: Callable[[Any], Any],
    expected: list[int],
) -> None:
    """Pure bounds compose identically across exact sequence sources and sinks."""
    source = range(8) if source_type is range else source_type(range(8))
    result = getattr(build(flow(source).with_engine(engine)), terminal)()

    assert result == (expected if terminal == "to_list" else tuple(expected))


@pytest.mark.parametrize("terminal", ["to_list", "to_tuple"])
def test_retained_sequence_windows_leave_active_failpoints_observable(terminal: str) -> None:
    """A structural sink must not bypass an instrumented source-open boundary."""
    from fpstreams.runtime.failpoints import failpoint

    failure = RuntimeError("instrumented source")
    pipeline = flow(list(range(8))).with_engine("python").drop(2).take(4)

    with failpoint("source.open.after", failure), pytest.raises(RuntimeError) as captured:
        getattr(pipeline, terminal)()

    assert captured.value is failure


@pytest.mark.parametrize("terminal", ["to_list", "to_tuple"])
def test_retained_sequence_windows_use_custom_source_factory(terminal: str) -> None:
    """Native data alone cannot replace the factory owned by a custom Source."""
    from fpstreams.planning.source import Source, SourceCapabilities

    opens = 0

    def open_source() -> Iterator[int]:
        nonlocal opens
        opens += 1
        return iter((7, 8, 9))

    retained = [1, 2, 3]
    source = Source(
        open_source,
        SourceCapabilities(reiterable=True, exact_size=3),
        native_data=retained,
        live_size_data=retained,
    )
    pipeline = fpstreams.Flow(source).with_engine("python").drop(1).take(1)

    assert getattr(pipeline, terminal)() == ([8] if terminal == "to_list" else (8,))
    assert opens == 1


@pytest.mark.parametrize(
    ("build", "expected"),
    [
        (lambda values: values.chunk(2), [(9, 7), (8,)]),
        (lambda values: values.window(2), [(9, 7), (7, 8)]),
        (lambda values: values.sorted(), [7, 8, 9]),
    ],
)
def test_retained_structural_routes_use_custom_source_factory(
    build: Callable[[Any], Any],
    expected: list[Any],
) -> None:
    """Every structural fast path requires from_iterable provenance, not native metadata."""
    from fpstreams.planning.source import Source, SourceCapabilities

    opens = 0

    def open_source() -> Iterator[int]:
        nonlocal opens
        opens += 1
        return iter((9, 7, 8))

    retained = [1, 2, 3]
    source = Source(
        open_source,
        SourceCapabilities(reiterable=True, exact_size=3),
        native_data=retained,
        live_size_data=retained,
    )

    assert build(fpstreams.Flow(source).with_engine("python")).to_list() == expected
    assert opens == 1


@pytest.mark.parametrize("operation_name", ["chunk", "window"])
def test_retained_grouping_preserves_oversized_size_errors(operation_name: str) -> None:
    """Direct grouping keeps the canonical islice bound error."""
    pipeline = getattr(flow([1, 2, 3]).with_engine("python"), operation_name)(sys.maxsize + 1)

    with pytest.raises(ValueError):
        pipeline.to_list()


@pytest.mark.parametrize("strict", [False, True])
def test_retained_list_chunks_use_direct_tuple_batching(
    monkeypatch: pytest.MonkeyPatch,
    strict: bool,
) -> None:
    """Exact lists avoid allocating an intermediate list slice for every chunk."""
    from fpstreams.streams._terminals import structural as structural

    batched_sources: list[object] = []

    def tracked_batched(values: list[object], size: int) -> Iterator[tuple[object, ...]]:
        batched_sources.append(values)
        for index in range(0, len(values), size):
            yield tuple(values[index : index + size])

    monkeypatch.setattr(structural, "_BATCHED", tracked_batched)

    retained_list = [1, 2, 3, 4] if strict else [1, 2, 3, 4, 5]
    retained_tuple = tuple(retained_list)
    expected = [(1, 2), (3, 4)] if strict else [(1, 2), (3, 4), (5,)]
    assert flow(retained_list).with_engine("python").chunk(2, strict=strict).to_list() == expected
    assert flow(retained_tuple).with_engine("python").chunk(2, strict=strict).to_list() == expected
    if strict:
        with pytest.raises(ValueError, match="incomplete final chunk"):
            flow([1, 2, 3]).with_engine("python").chunk(2, strict=True).to_list()
        assert batched_sources.pop() == [1, 2, 3]
    assert batched_sources == [retained_list]


def test_structural_dispatch_declines_unknown_operation_subclasses() -> None:
    """Only canonical exact operation nodes may enter a structural fast path."""
    from fpstreams.planning.sync import ChunkOp, FlatMapOp, SortOp, WindowOp

    class CustomSort(SortOp):
        pass

    class CustomFlatMap(FlatMapOp):
        pass

    class CustomChunk(ChunkOp):
        pass

    class CustomWindow(WindowOp):
        pass

    expansion = (
        fpstreams.rows([{"labels": [1, 2]}])
        .explode("labels")
        ._flow._pipeline.operations[-1]
        .function
    )
    cases = (
        (CustomSort(None, False), [3, 1, 2]),
        (CustomFlatMap(expansion), [{"labels": [1, 2]}]),
        (CustomChunk(2), [3, 1, 2]),
        (CustomWindow(2, 1), [3, 1, 2]),
    )
    for operation, source in cases:
        pipeline = flow(source).with_engine("python")._append(operation)
        with pytest.raises(TypeError):
            pipeline.to_list()


@pytest.mark.parametrize("terminal", ["to_list", "to_tuple"])
@pytest.mark.parametrize("operation", ["take", "drop"])
def test_retained_sequence_windows_preserve_oversized_count_errors(
    terminal: str,
    operation: str,
) -> None:
    """Bounds outside islice's accepted range retain the canonical ValueError."""
    pipeline = getattr(flow([1, 2, 3]).with_engine("python"), operation)(sys.maxsize + 1)

    with pytest.raises(ValueError):
        getattr(pipeline, terminal)()


@pytest.mark.parametrize(
    ("terminal", "global_name", "build"),
    [
        ("to_list", "list", lambda values: values.drop(1).take(1)),
        ("to_list", "list", lambda values: values.sorted()),
        ("to_tuple", "tuple", lambda values: values.drop(1).take(1)),
    ],
)
def test_direct_structural_routes_decline_replaced_materializers(
    monkeypatch: pytest.MonkeyPatch,
    terminal: str,
    global_name: str,
    build: Callable[[Any], Any],
) -> None:
    """A replaced terminal constructor still receives the canonical lazy executor."""
    from fpstreams.streams import flow_terminals

    marker = object()
    received: list[Iterable[object]] = []

    def replacement(values: Iterable[object]) -> object:
        received.append(values)
        return marker

    monkeypatch.setattr(flow_terminals, global_name, replacement, raising=False)
    pipeline = build(flow([3, 1, 2]).with_engine("python"))

    assert getattr(pipeline, terminal)() is marker
    assert len(received) == 1


def test_direct_linear_sink_preserves_container_subclasses_and_declines_failpoints() -> None:
    """Container protocols remain live while instrumentation keeps canonical ownership."""
    from fpstreams.runtime.failpoints import failpoint

    class ObservableList(list[int]):
        opens = 0

        def __iter__(self) -> Iterator[int]:
            self.opens += 1
            return super().__iter__()

    source = ObservableList(range(6))
    pipeline = (
        flow(source)
        .with_engine("python")
        .map(lambda value: value + 1)
        .scan(0, lambda total, value: total + value)
        .filter(lambda value: value % 2 == 1)
    )
    assert pipeline.to_list() == [1, 3, 15, 21]
    assert source.opens == 1

    instrumented = (
        flow(list(range(6)))
        .with_engine("python")
        .map(lambda value: value + 1)
        .scan(0, lambda total, value: total + value)
        .filter(bool)
    )
    with (
        failpoint("callback.before", RuntimeError("instrumented callback")),
        pytest.raises(RuntimeError, match="instrumented callback"),
    ):
        instrumented.to_list()


def test_flow_row_bridge_matches_rows_projection_enrichment_and_grouping() -> None:
    records = [
        {"team": "red", "score": 3},
        {"team": "blue", "score": 5},
        {"team": "red", "score": 7},
    ]

    assert (
        flow(records).select("team", points="score").to_list()
        == fpstreams.rows(records).select("team", points="score").to_list()
    )
    assert (
        flow(records).with_columns(doubled=lambda row: row["score"] * 2).to_list()
        == fpstreams.rows(records).with_columns(doubled=lambda row: row["score"] * 2).to_list()
    )
    assert (
        flow(records)
        .group_by("team")
        .aggregate(count=fpstreams.agg.count(), total=fpstreams.agg.sum("score"))
        .to_list()
        == fpstreams.rows(records)
        .group_by("team")
        .aggregate(count=fpstreams.agg.count(), total=fpstreams.agg.sum("score"))
        .to_list()
    )


def test_flow_row_bridge_does_not_probe_a_generator() -> None:
    events: list[str] = []

    def records() -> Iterator[dict[str, int]]:
        events.append("open")
        yield {"id": 1}

    pipeline = flow(records())
    row_view = pipeline.rows()
    projected = pipeline.select("id")
    enriched = pipeline.with_columns(copy="id")
    grouped = pipeline.group_by("id").aggregate(count=fpstreams.agg.count())

    assert row_view is not None
    assert projected is not None
    assert enriched is not None
    assert grouped is not None
    assert events == []


def test_flow_and_rows_bridge_share_one_shot_source_claim() -> None:
    pipeline = flow(iter([{"id": 1}, {"id": 2}]))
    row_view = pipeline.rows()
    projected = pipeline.select("id")

    assert row_view._flow is pipeline
    assert projected._flow._pipeline.source is pipeline._pipeline.source

    assert projected.to_list() == [{"id": 1}, {"id": 2}]
    with pytest.raises(fpstreams.FlowConsumedError):
        pipeline.to_list()

    second = flow(iter([{"id": 3}]))
    row_view = second.rows()

    assert second.to_list() == [{"id": 3}]
    with pytest.raises(fpstreams.FlowConsumedError):
        row_view.to_list()


def test_lazy_callable_map_filter_does_not_retain_an_emitted_value() -> None:
    """Manual iteration releases one emitted value when the caller drops its reference."""
    import gc
    from weakref import ref

    class Value:
        pass

    class Source(Iterator[Value]):
        def __init__(self, value: Value) -> None:
            self.value: Value | None = value

        def __next__(self) -> Value:
            if self.value is None:
                raise StopIteration
            value = self.value
            self.value = None
            return value

    value = Value()
    reference = ref(value)
    iterator = iter(
        flow(Source(value)).with_engine("python").map(lambda item: item).filter(lambda _item: True)
    )
    del value

    emitted = next(iterator)
    del emitted
    gc.collect()

    assert reference() is None
    iterator.close()


def test_flow_row_bridge_runtime_type_hints_resolve() -> None:
    from typing import get_type_hints

    for method_name in ("rows", "select", "with_columns", "group_by"):
        hints = get_type_hints(getattr(fpstreams.Flow, method_name))
        assert hints["return"] is not None


def test_rows_map_and_flat_map_exit_to_flow_and_can_reenter_rows() -> None:
    mapped = fpstreams.rows([{"value": 1}, {"value": 2}]).map(
        lambda row: {"value": row["value"] + 10}
    )
    flattened = fpstreams.rows([{"values": (1, 2)}, {"values": (3,)}]).flat_map(
        lambda row: ({"value": value} for value in row["values"])
    )

    assert isinstance(mapped, fpstreams.Flow)
    assert mapped.select("value").to_list() == [{"value": 11}, {"value": 12}]
    assert isinstance(flattened, fpstreams.Flow)
    assert flattened.select("value").to_list() == [
        {"value": 1},
        {"value": 2},
        {"value": 3},
    ]


def test_flow_row_bridge_does_not_replace_existing_terminal_or_transform_semantics() -> None:
    assert flow([1, 2, 3]).drop(1).to_list() == [2, 3]
    assert flow([1, 2, 3]).aggregate(total=fpstreams.agg.sum()) == {"total": 6}
    assert flow(["a", "b"]).join("|") == "a|b"
    assert flow([1, 2, 3]).where(lambda value: value > 1).to_list() == [2, 3]


def test_explicit_rows_view_keeps_relational_versions_of_conflicting_flow_methods() -> None:
    records = [{"id": 1, "value": 2}, {"id": 2, "value": 3}]

    assert flow(records).rows().drop("value").to_list() == [{"id": 1}, {"id": 2}]
    assert flow(records).rows().where(id=2).to_list() == [{"id": 2, "value": 3}]
    assert flow(records).rows().aggregate(total=fpstreams.agg.sum("value")).to_list() == [
        {"total": 5}
    ]
    assert flow(records).rows().join(
        [{"id": 1, "right": "x"}],
        left_on="id",
        right_on="id",
        validate="m:1",
    ).to_list() == [{"id": 1, "value": 2, "right": "x"}]


def test_flow_factory_reuses_existing_flow_and_rows_plan_ownership() -> None:
    pipeline = flow(iter([{"id": 1}]))
    row_view = pipeline.rows()

    assert flow(pipeline) is pipeline
    assert flow(row_view) is pipeline
    assert flow(row_view).to_list() == [{"id": 1}]
    with pytest.raises(fpstreams.FlowConsumedError):
        row_view.to_list()


def test_flow_factory_dispatches_retained_pyarrow_without_row_boxing() -> None:
    pa = pytest.importorskip("pyarrow")
    from fpstreams.planning.arrow.source import ArrowBatchSource

    table = pa.table({"id": [1, 2], "value": [3, 4]})
    pipeline = flow(table)
    descriptor = pipeline._pipeline.source.native_data

    assert isinstance(pipeline, fpstreams.Flow)
    assert isinstance(descriptor, ArrowBatchSource)
    assert descriptor.kind == "table"
    assert descriptor.materialized_data is table
    assert pipeline.select("id").to_list() == [{"id": 1}, {"id": 2}]


def test_flow_factory_dispatches_retained_pyarrow_record_batch() -> None:
    pa = pytest.importorskip("pyarrow")
    from fpstreams.planning.arrow.source import ArrowBatchSource

    batch = pa.record_batch({"id": [1, 2]})
    pipeline = flow(batch)
    descriptor = pipeline._pipeline.source.native_data

    assert isinstance(descriptor, ArrowBatchSource)
    assert descriptor.kind == "record_batch"
    assert descriptor.materialized_data is batch
    assert pipeline.to_list() == [{"id": 1}, {"id": 2}]


def test_flow_factory_keeps_pyarrow_reader_one_shot() -> None:
    pa = pytest.importorskip("pyarrow")
    from fpstreams.planning.arrow.source import ArrowBatchSource

    reader = pa.RecordBatchReader.from_batches(
        pa.schema([("id", pa.int64())]),
        [pa.record_batch({"id": [1, 2]})],
    )
    pipeline = flow(reader)
    descriptor = pipeline._pipeline.source.native_data

    assert isinstance(descriptor, ArrowBatchSource)
    assert descriptor.kind == "reader"
    assert pipeline.to_list() == [{"id": 1}, {"id": 2}]
    with pytest.raises(fpstreams.FlowConsumedError):
        pipeline.to_list()


@pytest.mark.parametrize("vendor", ["pandas", "polars", "pyarrow"])
def test_flow_factory_keeps_vendor_scalar_vectors_as_ordinary_iterables(vendor: str) -> None:
    """Series and Arrow arrays expose protocols but are not record tables."""
    if vendor == "pandas":
        pd = pytest.importorskip("pandas")
        source = pd.Series([1, 2])
    elif vendor == "polars":
        pl = pytest.importorskip("polars")
        source = pl.Series([1, 2])
    else:
        pa = pytest.importorskip("pyarrow")
        source = pa.chunked_array([[1], [2]])

    pipeline = flow(source)

    assert pipeline._pipeline.source.native_data is None
    assert pipeline.to_list() == list(source)


def test_flow_factory_prefers_custom_arrow_stream_over_dataframe_protocol() -> None:
    pa = pytest.importorskip("pyarrow")
    from fpstreams.planning.arrow.source import ArrowBatchSource

    table = pa.table({"id": [1, 2]})

    class Provider:
        arrow_calls = 0
        dataframe_calls = 0

        def __arrow_c_stream__(self, requested_schema: Any = None) -> Any:
            self.arrow_calls += 1
            return table.__arrow_c_stream__(requested_schema)

        def __dataframe__(self, **_options: Any) -> Any:
            self.dataframe_calls += 1
            raise AssertionError("the dataframe protocol must not win")

    provider = Provider()
    pipeline = flow(provider)
    descriptor = pipeline._pipeline.source.native_data

    assert isinstance(descriptor, ArrowBatchSource)
    assert descriptor.kind == "reader"
    assert provider.arrow_calls == 1
    assert provider.dataframe_calls == 0
    assert pipeline.to_list() == [{"id": 1}, {"id": 2}]


def test_rows_factory_reuses_flow_dual_protocol_priority() -> None:
    pa = pytest.importorskip("pyarrow")
    from fpstreams.planning.arrow.source import ArrowBatchSource

    table = pa.table({"id": [1, 2]})

    class Provider:
        arrow_calls = 0
        dataframe_calls = 0

        def __arrow_c_stream__(self, requested_schema: Any = None) -> Any:
            self.arrow_calls += 1
            return table.__arrow_c_stream__(requested_schema)

        def __dataframe__(self, **_options: Any) -> Any:
            self.dataframe_calls += 1
            raise AssertionError("Arrow must keep priority through the Rows compatibility factory")

    provider = Provider()
    row_view = fpstreams.rows(provider)
    descriptor = row_view._flow._pipeline.source.native_data

    assert isinstance(descriptor, ArrowBatchSource)
    assert descriptor.kind == "reader"
    assert provider.arrow_calls == 1
    assert provider.dataframe_calls == 0
    assert row_view.to_list() == [{"id": 1}, {"id": 2}]


def test_rows_factory_preserves_instance_level_dataframe_provider() -> None:
    pd = pytest.importorskip("pandas")

    frame = pd.DataFrame({"id": [1, 2]})

    class DynamicProvider:
        def __init__(self) -> None:
            self.__dataframe__ = frame.__dataframe__

        def __iter__(self) -> Iterator[dict[str, bool]]:
            yield {"fallback": True}

    assert fpstreams.rows(DynamicProvider()).to_list() == [{"id": 1}, {"id": 2}]


@pytest.mark.parametrize("factory", [flow, fpstreams.rows])
def test_flow_and_rows_factories_preserve_instance_level_arrow_provider(
    factory: Callable[[Any], Any],
) -> None:
    pa = pytest.importorskip("pyarrow")

    table = pa.table({"id": [1, 2]})

    class DynamicProvider:
        def __init__(self) -> None:
            self.__arrow_c_stream__ = table.__arrow_c_stream__
            self.__dataframe__ = lambda **_options: pytest.fail("Arrow must keep priority")

        def __iter__(self) -> Iterator[dict[str, bool]]:
            yield {"fallback": True}

    assert factory(DynamicProvider()).to_list() == [{"id": 1}, {"id": 2}]


def test_rows_factory_types_standard_tabular_protocols_as_record_rows() -> None:
    from typing import get_args, get_overloads, get_type_hints

    from fpstreams.streams.flow import _ArrowCStreamProvider, _DataFrameProvider

    expected_protocols = {_ArrowCStreamProvider, _DataFrameProvider}
    for candidate in get_overloads(type(fpstreams.rows).__call__):
        hints = get_type_hints(candidate)
        if expected_protocols <= set(get_args(hints["source"])):
            assert hints["return"] == fpstreams.Rows[dict[str, Any]]
            break
    else:
        raise AssertionError("rows() is missing its standard tabular protocol overload")


def test_flow_factory_keeps_pandas_index_out_of_record_columns() -> None:
    pd = pytest.importorskip("pandas")
    from fpstreams.planning.arrow.source import ArrowBatchSource

    frame = pd.DataFrame({"id": [1, 2]}, index=pd.Index([10, 20], name="row_id"))
    pipeline = flow(frame)
    descriptor = pipeline._pipeline.source.native_data

    assert isinstance(descriptor, ArrowBatchSource)
    assert descriptor.kind == "dataframe"
    assert pipeline.to_list() == [{"id": 1}, {"id": 2}]


def test_flow_factory_defers_generic_dataframe_conversion_until_consumption() -> None:
    pd = pytest.importorskip("pandas")
    from fpstreams.planning.arrow.source import ArrowBatchSource

    frame = pd.DataFrame({"id": [1, 2]})
    events: list[str] = []

    class Provider:
        def __dataframe__(self, **options: Any) -> Any:
            events.append("convert")
            return frame.__dataframe__(**options)

    pipeline = flow(Provider())
    descriptor = pipeline._pipeline.source.native_data

    assert isinstance(descriptor, ArrowBatchSource)
    assert descriptor.kind == "dataframe"
    assert events == []
    assert pipeline.to_list() == [{"id": 1}, {"id": 2}]
    assert events == ["convert"]


def test_flow_factory_preserves_polars_lazyframe_laziness() -> None:
    pl = pytest.importorskip("polars")
    from fpstreams.planning.arrow.source import ArrowBatchSource

    events: list[str] = []

    def observe(batch: Any) -> Any:
        events.append("collect")
        return batch

    source = (
        pl.DataFrame({"id": [1, 2]})
        .lazy()
        .map_batches(
            observe,
            schema={"id": pl.Int64},
        )
    )
    pipeline = flow(source)
    descriptor = pipeline._pipeline.source.native_data

    assert isinstance(descriptor, ArrowBatchSource)
    assert descriptor.kind == "polars"
    assert events == []
    assert pipeline.to_list() == [{"id": 1}, {"id": 2}]
    assert events == ["collect"]


def test_flow_factory_uses_polars_adapter_for_eager_dataframe() -> None:
    pl = pytest.importorskip("polars")
    from fpstreams.planning.arrow.source import ArrowBatchSource

    frame = pl.DataFrame({"id": [1, 2]})
    pipeline = flow(frame)
    descriptor = pipeline._pipeline.source.native_data

    assert isinstance(descriptor, ArrowBatchSource)
    assert descriptor.kind == "polars"
    assert pipeline.to_list() == [{"id": 1}, {"id": 2}]


def test_flow_factory_recognizes_a_polars_subclass_from_an_application_module() -> None:
    pl = pytest.importorskip("polars")
    from fpstreams.planning.arrow.source import ArrowBatchSource

    class ApplicationLazyFrame(pl.LazyFrame):
        pass

    source = ApplicationLazyFrame({"id": [1, 2]})
    pipeline = flow(source)
    descriptor = pipeline._pipeline.source.native_data

    assert isinstance(descriptor, ArrowBatchSource)
    assert descriptor.kind == "polars"
    assert pipeline.to_list() == [{"id": 1}, {"id": 2}]


def test_flow_factory_does_not_trust_a_spoofed_vendor_module_name() -> None:
    pytest.importorskip("pandas")

    class OrdinaryRecords:
        __module__ = "pandas.application"

        def __iter__(self) -> Iterator[dict[str, int]]:
            yield {"id": 1}

    pipeline = flow(OrdinaryRecords())

    assert pipeline._pipeline.source.native_data is None
    assert pipeline.to_list() == [{"id": 1}]


def test_flow_factory_does_not_probe_an_ordinary_failing_generator() -> None:
    events: list[str] = []

    def records() -> Iterator[dict[str, int]]:
        events.append("open")
        raise RuntimeError("source failed")
        yield {"id": 1}

    pipeline = flow(records())

    assert events == []
    with pytest.raises(RuntimeError, match="source failed"):
        pipeline.to_list()
    assert events == ["open"]


def test_flow_factory_keeps_numpy_arrays_as_ordinary_iterables() -> None:
    np = pytest.importorskip("numpy")

    source = np.asarray([[1, 2], [3, 4]])
    pipeline = flow(source)
    result = pipeline.to_list()

    assert pipeline._pipeline.source.native_data is None
    assert all(isinstance(row, np.ndarray) for row in result)
    assert np.array_equal(result[0], np.asarray([1, 2]))
    assert np.array_equal(result[1], np.asarray([3, 4]))


def test_flow_projects_a_plain_two_dimensional_list_without_source_sniffing() -> None:
    source = [[1, "a"], [2, "b"]]
    pipeline = flow(source)

    assert pipeline._pipeline.source.native_data is source
    assert pipeline.select(0, 1).to_list() == [
        {"0": 1, "1": "a"},
        {"0": 2, "1": "b"},
    ]


@pytest.mark.parametrize(
    ("source", "transform", "expected"),
    [
        ([{"old": 1}], lambda rows: rows.rename(old="new"), [{"new": 1}]),
        ([{"value": "2"}], lambda rows: rows.cast(value=int), [{"value": 2}]),
        (
            [{"value": None}, {"value": 2}],
            lambda rows: rows.fill_nulls(value=1),
            [{"value": 1}, {"value": 2}],
        ),
        (
            [{"value": None}, {"value": 2}],
            lambda rows: rows.drop_nulls("value"),
            [{"value": 2}],
        ),
        (
            [{"id": 1, "values": [2, 3]}],
            lambda rows: rows.explode("values"),
            [{"id": 1, "values": 2}, {"id": 1, "values": 3}],
        ),
        (
            [{"id": 1, "meta": {"score": 2}}],
            lambda rows: rows.unnest("meta"),
            [{"id": 1, "score": 2}],
        ),
        (
            [{"id": 1, "left": 2, "right": 3}],
            lambda rows: rows.unpivot("left", "right"),
            [
                {"id": 1, "variable": "left", "value": 2},
                {"id": 1, "variable": "right", "value": 3},
            ],
        ),
        (
            [
                {"id": 1, "metric": "left", "value": 2},
                {"id": 1, "metric": "right", "value": 3},
            ],
            lambda rows: rows.pivot(index="id", columns="metric", values="value"),
            [{"id": 1, "left": 2, "right": 3}],
        ),
    ],
    ids=(
        "rename",
        "cast",
        "fill_nulls",
        "drop_nulls",
        "explode",
        "unnest",
        "unpivot",
        "pivot",
    ),
)
def test_flow_nonconflicting_row_bridges_remain_lazy_and_usable(
    source: list[dict[str, Any]],
    transform: Any,
    expected: list[dict[str, Any]],
) -> None:
    result = transform(flow(source))

    assert isinstance(result, fpstreams.Rows)
    assert result.to_list() == expected


def test_flow_namespace_reuses_tabular_source_adapters(tmp_path: Path) -> None:
    pa = pytest.importorskip("pyarrow")
    pd = pytest.importorskip("pandas")
    pl = pytest.importorskip("polars")
    parquet = pytest.importorskip("pyarrow.parquet")

    table = pa.table({"id": [1, 2]})
    frame = pd.DataFrame({"id": [1, 2]})
    polars_frame = pl.DataFrame({"id": [1, 2]})
    csv_path = tmp_path / "records.csv"
    parquet_path = tmp_path / "records.parquet"
    csv_path.write_text("id\n1\n2\n", encoding="utf-8")
    parquet.write_table(table, parquet_path)

    pipelines = (
        flow.from_arrow(table),
        flow.from_dataframe(frame),
        flow.from_pandas(frame),
        flow.from_polars(polars_frame),
        flow.scan_csv(csv_path),
        flow.from_parquet(parquet_path),
    )

    assert all(isinstance(pipeline, fpstreams.Flow) for pipeline in pipelines)
    assert [pipeline.to_list() for pipeline in pipelines] == [
        [{"id": 1}, {"id": 2}],
        [{"id": 1}, {"id": 2}],
        [{"id": 1}, {"id": 2}],
        [{"id": 1}, {"id": 2}],
        [{"id": 1}, {"id": 2}],
        [{"id": 1}, {"id": 2}],
    ]


def test_short_circuit_closes_the_upstream_iterator() -> None:
    closed = False

    def values():
        nonlocal closed
        try:
            yield from range(100)
        finally:
            closed = True

    assert flow(values()).take(2).to_list() == [0, 1]
    assert closed


def test_identity_sum_closes_its_source_when_addition_fails() -> None:
    """The direct CPython reduction path retains query-scoped source ownership."""
    closed = False

    def values():
        nonlocal closed
        try:
            yield 1
            yield "not-addable"
        finally:
            closed = True

    with pytest.raises(TypeError):
        flow(values()).sum()

    assert closed


def test_cleanup_error_does_not_hide_pipeline_error() -> None:
    def values():
        try:
            yield 1
        finally:
            raise RuntimeError("cleanup failed")

    def fail(_value: int) -> int:
        raise ValueError("transform failed")

    with pytest.raises(ValueError, match="transform failed") as captured:
        flow.defer(values).map(fail).to_list()

    assert any("cleanup failed" in note for note in captured.value.__notes__)


def test_early_close_cleanup_failure_ignores_an_ambient_outer_exception() -> None:
    """A normal early close owns no error from an already-active outer handler."""
    outer = ValueError("outer failure")
    cleanup = OSError("source close failed")

    class Source(Iterator[int]):
        def __init__(self) -> None:
            self.pulled = False
            self.close_calls = 0

        def __next__(self) -> int:
            if self.pulled:
                raise StopIteration
            self.pulled = True
            return 1

        def close(self) -> None:
            self.close_calls += 1
            raise cleanup

    source = Source()
    try:
        raise outer
    except ValueError:
        with pytest.raises(OSError) as captured:
            flow(source).first()

    assert captured.value is cleanup
    assert getattr(outer, "__notes__", None) is None
    assert source.close_calls == 1


def test_cleanup_attempts_every_owned_iterator() -> None:
    import fpstreams.execution.sync_ops as sync_ops

    events: list[str] = []

    class Closeable:
        def __init__(self, name: str, *, fails: bool = False) -> None:
            self.name = name
            self.fails = fails

        def __iter__(self):
            return self

        def __next__(self):
            raise StopIteration

        def close(self) -> None:
            events.append(self.name)
            if self.fails:
                raise RuntimeError(f"{self.name} cleanup failed")

    assert hasattr(sync_ops, "close_iterators"), "batch cleanup must be available"
    with pytest.raises(RuntimeError, match="first cleanup failed"):
        sync_ops.close_iterators([Closeable("first", fails=True), Closeable("second")])

    assert events == ["first", "second"]


def test_deferred_infinite_source_can_be_evaluated_repeatedly() -> None:
    powers = flow.iterate(1, lambda value: value * 2).take(5)

    assert powers.to_list() == [1, 2, 4, 8, 16]
    assert powers.to_list() == [1, 2, 4, 8, 16]


def test_stream_class_factories_remain_useful_v2_entry_points() -> None:
    calls = 0

    def supply() -> int:
        nonlocal calls
        calls += 1
        return calls

    assert fpstreams.Stream.iterate(1, lambda value: value * 2).take(4).to_list() == [
        1,
        2,
        4,
        8,
    ]
    assert fpstreams.Stream.generate(supply).take(3).to_list() == [1, 2, 3]
    assert fpstreams.Flow.of_nullable(None).to_list() == []
    assert fpstreams.Flow.of_nullable(4).to_list() == [4]


def test_reiterable_flow_supports_pythonic_terminals() -> None:
    values = flow([1, 2, 3, 4])

    assert values.first() == 1
    assert values.last() == 4
    assert values.count() == 4
    assert values.sum() == 10
    assert values.min() == 1
    assert values.max() == 4
    assert values.any(lambda value: value > 3)
    assert values.all(lambda value: value > 0)
    assert values.none(lambda value: value < 0)
    assert values.reduce(lambda left, right: left + right) == 10
    assert flow([]).reduce(lambda left, right: left + right, 10) == 10

    def user_error(_left: int, _right: int) -> int:
        raise TypeError("empty iterable is domain data")

    with pytest.raises(TypeError, match="domain data"):
        flow([1, 2]).reduce(user_error)


@pytest.mark.parametrize(
    "terminal",
    ["reduce", "join", "for_each", "partition", "partition_results"],
)
def test_public_terminals_keep_primary_error_when_source_close_fails(terminal: str) -> None:
    primary = ValueError(f"{terminal} failed")

    class Unstringable:
        def __str__(self) -> str:
            raise primary

    class Source(Iterator[Any]):
        def __init__(self, values: Iterable[Any]) -> None:
            self.values = iter(values)
            self.close_calls = 0

        def __next__(self) -> Any:
            return next(self.values)

        def close(self) -> None:
            self.close_calls += 1
            raise OSError("source close failed")

    def fail(*_arguments: object) -> Any:
        raise primary

    values = {
        "reduce": [1, 2],
        "join": [Unstringable()],
        "for_each": [1],
        "partition": [1],
        "partition_results": [object()],
    }[terminal]
    source = Source(values)
    pipeline = flow(source)

    with pytest.raises((ValueError, TypeError)) as captured:
        if terminal == "reduce":
            pipeline.reduce(fail)
        elif terminal == "join":
            pipeline.join()
        elif terminal == "for_each":
            pipeline.for_each(fail)
        elif terminal == "partition":
            pipeline.partition(fail)
        else:
            pipeline.partition_results()

    if terminal == "partition_results":
        assert str(captured.value) == "partition_results() requires Result values"
    else:
        assert captured.value is primary
    assert captured.value.__notes__ == ["cleanup failed with OSError: source close failed"]
    assert source.close_calls == 1


def test_any_distinguishes_predicate_and_truth_test_stop_iteration() -> None:
    """Predicate exhaustion is PEP 479-converted; result truth testing is not."""
    predicate_failure = StopIteration("predicate stopped")

    def stopped_predicate(_value: int) -> bool:
        raise predicate_failure

    with pytest.raises(RuntimeError, match=r"^generator raised StopIteration$") as captured:
        flow([1]).with_engine("python").any(stopped_predicate)

    assert captured.value.__cause__ is predicate_failure
    assert captured.value.__context__ is predicate_failure

    truth_failure = StopIteration("truth test stopped")

    class Truth:
        def __bool__(self) -> bool:
            raise truth_failure

    def stopped_truth_test(_value: int) -> object:
        return Truth()

    with pytest.raises(StopIteration, match=r"^truth test stopped$") as captured:
        flow([1]).with_engine("python").any(stopped_truth_test)  # type: ignore[arg-type]

    assert captured.value is truth_failure


@pytest.mark.parametrize("engine", ["auto", "python"])
def test_identity_callable_any_skips_unused_physical_compilation(
    engine: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Opaque terminal callbacks consume operation-free source shapes without backend planning."""
    from fpstreams.streams import flow_terminals

    def fail_if_compiled(_query: object) -> object:
        raise AssertionError("opaque linear any() must not build an unused physical plan")

    def matches_one(value: int) -> bool:
        return value == 1

    queries: list[object] = []
    original_query = fpstreams.Flow._query

    def tracked_query(
        candidate: fpstreams.Flow[object],
        name: str,
        *arguments: object,
        **options: object,
    ) -> object:
        query = original_query(candidate, name, *arguments, **options)
        queries.append(query)
        return query

    monkeypatch.setattr(flow_terminals, "compile_query", fail_if_compiled)
    monkeypatch.setattr(fpstreams.Flow, "_query", tracked_query)
    one_shot = flow(iter((0, 1)))
    candidates = (
        flow([0, 1]),
        flow(range(2)),
        flow((0, 1)),
        flow.defer(lambda: iter((0, 1))),
        one_shot,
    )

    for candidate in candidates:
        assert candidate.with_engine(engine).any(matches_one)

    assert len(queries) == len(candidates)
    with pytest.raises(fpstreams.FlowConsumedError):
        one_shot.with_engine(engine).any(matches_one)


def test_identity_callable_any_executes_the_constructed_query() -> None:
    """A Flow extension may redirect M1; the shortcut must consume that query's source."""
    from fpstreams.planning.logical import Query

    class RedirectedFlow(fpstreams.Flow[int]):
        def _query(self, name: str, *arguments: Any, **options: Any) -> Query:
            return flow([1]).with_engine("python")._query(name, *arguments, **options)

    assert RedirectedFlow([0]).any(lambda value: value == 1)


def test_callable_any_shortcut_preserves_open_and_instrumentation_boundaries() -> None:
    """The planning shortcut keeps source PEP 479 and active compiler failpoints intact."""
    from fpstreams.planning.source import Source, SourceCapabilities
    from fpstreams.runtime.failpoints import failpoint

    open_failure = StopIteration("source open stopped")

    def stopped_open() -> Iterator[int]:
        raise open_failure

    stopped_source = Source(
        stopped_open,
        SourceCapabilities(reiterable=True, exact_size=None),
    )
    with pytest.raises(RuntimeError, match=r"^generator raised StopIteration$") as captured:
        fpstreams.Flow(stopped_source).with_engine("python").any(lambda value: value > 0)
    assert captured.value.__cause__ is open_failure

    compile_failure = RuntimeError("instrumented expression compilation")
    with (
        failpoint("expression.guard.before", compile_failure),
        pytest.raises(RuntimeError) as instrumented,
    ):
        flow(range(4)).map(fpstreams.item + 1).any(lambda value: value > 0)
    assert instrumented.value is compile_failure


def test_any_resolves_live_builtin_after_open_with_a_generator_argument(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A source may replace ``any`` before its canonical generator is constructed."""
    from types import GeneratorType

    from fpstreams.planning.source import Source, SourceCapabilities
    from fpstreams.streams import flow_terminals

    received: list[object] = []
    marker = object()

    def replacement(values: object) -> object:
        received.append(values)
        return marker

    def open_source() -> Iterator[int]:
        monkeypatch.setattr(flow_terminals.builtins, "any", replacement)
        return iter((0, 1))

    source = Source(open_source, SourceCapabilities(reiterable=True, exact_size=None))
    result = fpstreams.Flow(source).with_engine("python").any(lambda value: value == 1)

    assert result is marker
    assert len(received) == 1
    assert isinstance(received[0], GeneratorType)


def test_any_exposes_the_generator_when_source_enables_trace() -> None:
    """Tracing enabled during open still sees the canonical generator and predicate frames."""
    from fpstreams.planning.source import Source, SourceCapabilities

    calls: list[str] = []
    previous_trace = sys.gettrace()

    def trace(frame: object, event: str, _argument: object) -> object:
        if event == "call":
            name = frame.f_code.co_name  # type: ignore[attr-defined]
            if name in {"<genexpr>", "predicate"}:
                calls.append(name)
        return trace

    def open_source() -> Iterator[int]:
        sys.settrace(trace)
        return iter((0, 1))

    def predicate(value: int) -> bool:
        return value == 1

    source = Source(open_source, SourceCapabilities(reiterable=True, exact_size=None))
    try:
        assert fpstreams.Flow(source).with_engine("python").any(predicate)
    finally:
        sys.settrace(previous_trace)

    assert "<genexpr>" in calls
    assert calls.count("predicate") == 2


def test_any_releases_the_current_item_before_closing_after_truth_failure() -> None:
    """A truth-test error unwinds the terminal item before source cleanup begins."""
    from fpstreams.planning.source import Source, SourceCapabilities

    events: list[str] = []
    failure = ValueError("truth failed")

    class Item:
        def __del__(self) -> None:
            events.append("item:del")

    class Truth:
        def __bool__(self) -> bool:
            events.append("truth")
            raise failure

    class Values:
        def __init__(self) -> None:
            self.emitted = False

        def __iter__(self) -> Values:
            events.append("iter")
            return self

        def __next__(self) -> Item:
            if self.emitted:
                raise StopIteration
            self.emitted = True
            events.append("next")
            return Item()

        def close(self) -> None:
            events.append("close")

    def predicate(_item: Item) -> Truth:
        events.append("predicate")
        return Truth()

    source = Source(Values, SourceCapabilities(reiterable=True, exact_size=None))
    with pytest.raises(ValueError, match=r"^truth failed$") as captured:
        fpstreams.Flow(source).with_engine("python").any(predicate)

    assert captured.value is failure
    assert events[-5:] == ["next", "predicate", "truth", "item:del", "close"]


def test_any_preserves_local_pep669_generator_monitoring() -> None:
    """Local PEP 669 events remain visible on the canonical generator."""
    from types import CodeType

    from fpstreams.streams.flow_terminals import FlowTerminalsMixin

    monitoring = getattr(sys, "monitoring", None)
    if monitoring is None:
        pytest.skip("sys.monitoring requires Python 3.12+")
    tool_id = next(
        (
            candidate
            for candidate in range(monitoring.OPTIMIZER_ID + 1)
            if monitoring.get_tool(candidate) is None
        ),
        None,
    )
    if tool_id is None:
        pytest.skip("no free sys.monitoring tool id")

    monitored_code = next(
        constant
        for constant in FlowTerminalsMixin.any.__code__.co_consts
        if isinstance(constant, CodeType) and constant.co_name == "<genexpr>"
    )
    observed: list[str] = []

    def observe(code: object, _instruction_offset: int) -> None:
        if code is monitored_code:
            observed.append("generator")

    event = monitoring.events.PY_START
    monitoring.use_tool_id(tool_id, "fpstreams Flow.any regression")
    try:
        monitoring.register_callback(tool_id, event, observe)
        monitoring.set_local_events(tool_id, monitored_code, event)

        assert flow([0, 1]).with_engine("python").any(lambda value: value == 1)
    finally:
        monitoring.set_local_events(tool_id, monitored_code, 0)
        monitoring.register_callback(tool_id, event, None)
        monitoring.free_tool_id(tool_id)

    assert observed


def test_prefix_operations_preserve_boundaries() -> None:
    result = (
        flow(range(10))
        .drop(2)
        .drop_while(lambda value: value < 4)
        .take_while(lambda value: value < 8)
        .to_list()
    )

    assert result == [4, 5, 6, 7]
    with pytest.raises(ValueError):
        flow([1]).drop(-1)


def test_selectors_resolve_paths_and_fail_loudly_when_missing() -> None:
    records = [
        {"user": {"name": "Ada"}},
        {"user": {"name": "Grace"}},
    ]

    assert flow(records).pluck("user.name").to_list() == ["Ada", "Grace"]
    with pytest.raises(SelectionError, match=r"user\.email"):
        flow(records).pluck("user.email").to_list()


def test_unique_is_stable_for_hashable_and_unhashable_values() -> None:
    values = [1, 1, [2], [2], 3, 1]

    assert flow(values).unique().to_list() == [1, [2], 3]


def test_unique_handles_late_sparse_and_unbounded_integers() -> None:
    """Integer magnitude must not change exact set semantics or allocate by key span."""
    values = [*range(128), 10**10, -(10**10), 2**100, 10**10, 0]

    assert flow(values).with_engine("python").unique().to_list() == list(dict.fromkeys(values))


@pytest.mark.parametrize(
    "values",
    [
        range(20_000, -1, -3),
        range(1 << 100, (1 << 100) + 4_096),
    ],
    ids=["range_iterator", "longrange_iterator"],
)
def test_auto_unique_materializes_proven_distinct_ranges_without_hashing(values: range) -> None:
    """Both exact range iterator implementations preserve identity uniqueness directly."""
    execution = flow(values).unique().run_with_report("to_list")

    assert execution.value == list(values)
    assert execution.report.strategy == "python_direct"
    assert "without hashing" in execution.report.reason


def test_auto_unique_range_metadata_validates_the_live_iterator() -> None:
    """A changed retained-source closure cannot make duplicate values bypass uniqueness."""
    query = flow(range(4_096)).unique()
    source = query._pipeline.source
    factory_cell = source._factory.__closure__[0]
    factory_cell.cell_contents = [1, 1, 2, 1]

    execution = query.run_with_report("to_list")

    assert execution.value == [1, 2]
    assert execution.report.strategy == "planned:native"


def test_auto_unique_range_fallback_closes_a_changed_iterator_once() -> None:
    """Falling back after opening changed source data must retain single close ownership."""

    class Values(Iterator[int]):
        def __init__(self) -> None:
            self.values = iter((1, 1, 2, 1))
            self.close_calls = 0

        def __next__(self) -> int:
            return next(self.values)

        def close(self) -> None:
            self.close_calls += 1

    query = flow(range(4_096)).unique()
    values = Values()
    query._pipeline.source._factory.__closure__[0].cell_contents = values

    assert query.to_list() == [1, 2]
    assert values.close_calls == 1


@pytest.mark.parametrize(
    ("terminal", "engine", "expected"),
    [("sum", "python", 9), ("mean", "auto", 3.0), ("to_tuple", "python", (2, 3, 4))],
)
def test_terminal_helpers_keep_live_operation_opener(
    monkeypatch: pytest.MonkeyPatch, terminal: str, engine: str, expected: object
) -> None:
    """Helper extraction must keep the terminal owner's opener and single close observable."""
    from fpstreams.streams import flow_terminals

    events: list[str] = []
    original = flow_terminals.open_operations

    def tracked(*args, **kwargs):
        events.append("operations")
        return original(*args, **kwargs)

    def source() -> Iterator[int]:
        events.append("open")
        try:
            yield from (1, 2, 3)
        finally:
            events.append("close")

    monkeypatch.setattr(flow_terminals, "open_operations", tracked)
    query = flow(source()).with_engine(engine).map(lambda value: value + 1)

    assert getattr(query, terminal)() == expected
    assert events == ["operations", "open", "close"]


def test_auto_unique_range_deopts_when_wrapped_iterator_builder_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mutating contextmanager internals cannot leave the direct range sink eligible."""
    from fpstreams.streams import flow_terminals

    wrapped = flow_terminals.open_operations.__wrapped__

    def replacement(
        source_iterator: Iterator[object],
        operations: tuple[object, ...],
        *,
        runtime: object = None,
        fuse_callable_map_filter: bool = False,
    ) -> Iterator[Iterator[object]]:
        del source_iterator, operations, runtime, fuse_callable_map_filter
        yield iter(())

    monkeypatch.setattr(wrapped, "__code__", replacement.__code__)

    execution = flow(range(4_096)).unique().run_with_report("to_list")

    assert execution.value == list(range(4_096))
    assert execution.report.strategy != "python_direct"


def test_auto_unique_range_checks_retained_method_code_before_calling_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Changed retained metadata access must be declined without executing injected code."""
    from fpstreams.planning.source import Source

    def injected(_source: Source[object]) -> None:
        raise AssertionError("changed retained_sequence code executed")

    monkeypatch.setattr(Source.retained_sequence, "__code__", injected.__code__)

    execution = flow(range(4_096)).unique().run_with_report("to_list")

    assert execution.value == list(range(4_096))
    assert execution.report.strategy != "python_direct"


def test_auto_unique_high_cardinality_uses_exact_i64_prefix_sink(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A retained integer list is deduplicated once without replacing first objects."""
    from fpstreams import _native

    values = list(range(1_000, 132_072))
    endpoint = _native.unique_i64_exact_prefix_v1
    calls = 0

    def tracked(output: list[object], source: Iterator[object]) -> object:
        nonlocal calls
        calls += 1
        return endpoint(output, source)

    monkeypatch.setattr(_native, "unique_i64_exact_prefix_v1", tracked)
    result = flow(values).unique().run_with_report("to_list")

    assert result.value == values
    assert result.value[0] is values[0]
    assert result.value[-1] is values[-1]
    assert result.report.strategy == "rust_direct"
    assert calls == 1


@pytest.mark.parametrize("container", [list, tuple], ids=["list", "tuple"])
def test_auto_unique_low_cardinality_uses_exact_i64_prefix_sink(
    monkeypatch: pytest.MonkeyPatch,
    container: Callable[[Iterable[int]], list[int] | tuple[int, ...]],
) -> None:
    """A large repeated integer sequence reuses the direct sink and its first objects."""
    from fpstreams import _native

    # Equal values must be separate objects to select the value-cache endpoint.
    values = container(int(str(1_000_000 + index % 16)) for index in range(131_072))
    assert type(values[0]) is type(values[16]) is int
    assert values[0] == values[16] and values[0] is not values[16]
    endpoint = _native.unique_i64_exact_prefix_cached_v1
    calls = 0

    def tracked(output: list[object], source: Iterator[object]) -> object:
        nonlocal calls
        calls += 1
        return endpoint(output, source)

    monkeypatch.setattr(_native, "unique_i64_exact_prefix_cached_v1", tracked)
    result = flow(values).unique().run_with_report("to_list")

    assert result.value == list(range(1_000_000, 1_000_016))
    assert all(result.value[index] is values[index] for index in range(16))
    assert result.report.strategy == "rust_direct"
    assert calls == 1


def test_auto_unique_low_cardinality_selects_the_sampled_cached_sink(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A conservatively sampled repeated source avoids the baseline hash probe loop."""
    from fpstreams import _native

    values = [int(str(1_000_000 + index % 32)) for index in range(131_072)]
    assert type(values[0]) is type(values[32]) is int
    assert values[0] == values[32] and values[0] is not values[32]
    endpoint = _native.unique_i64_exact_prefix_cached_v1
    cached_calls = 0

    def reject_baseline(*_arguments: object) -> None:
        raise AssertionError("a sampled low-cardinality source must use the cached sink")

    def tracked_cached(output: list[object], source: Iterator[object]) -> object:
        nonlocal cached_calls
        cached_calls += 1
        return endpoint(output, source)

    monkeypatch.setattr(_native, "unique_i64_exact_prefix_v1", reject_baseline)
    monkeypatch.setattr(
        _native,
        "unique_i64_exact_prefix_cached_v1",
        tracked_cached,
    )

    result = flow(values).unique().to_list()

    assert result == list(range(1_000_000, 1_000_032))
    assert cached_calls == 1


def test_auto_unique_reused_objects_select_the_identity_cached_sink(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repeated exact objects skip extraction while preserving their first identities."""
    from fpstreams import _native
    from fpstreams.streams._terminals import unique as _flow_unique_list

    first_objects = [int(str(1_000_000 + index)) for index in range(16)]
    assert type(first_objects[0]) is int and first_objects[0] is not int("1000000")
    values = (first_objects * 8_192)[:131_072]
    endpoint = _native.unique_i64_exact_prefix_identity_cached_v1
    identity_calls = 0

    def reject_other(*_arguments: object) -> None:
        raise AssertionError("an identity-stable sample must use its bounded pointer cache")

    def tracked_identity(output: list[object], source: Iterator[object]) -> object:
        nonlocal identity_calls
        identity_calls += 1
        return endpoint(output, source)

    assert (
        _flow_unique_list._sample_exact_i64(values)
        == _flow_unique_list._UNIQUE_SAMPLE_IDENTITY_CACHED
    )
    monkeypatch.setattr(_native, "unique_i64_exact_prefix_v1", reject_other)
    monkeypatch.setattr(_native, "unique_i64_exact_prefix_cached_v1", reject_other)
    monkeypatch.setattr(
        _native,
        "unique_i64_exact_prefix_identity_cached_v1",
        tracked_identity,
    )

    result = flow(values).unique().run_with_report("to_list")

    assert result.value == first_objects
    assert all(result.value[index] is first_objects[index] for index in range(16))
    assert result.report.strategy == "rust_direct"
    assert identity_calls == 1


def test_auto_unique_samples_once_after_opening_the_live_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Endpoint selection uses one post-open sample and does not scan retained data twice."""
    from fpstreams.planning import source as source_module
    from fpstreams.streams._terminals import unique as _flow_unique_list

    values = [int(str(1_000 + index % 16)) for index in range(131_072)]
    query = flow(values).unique()
    canonical_iter = iter
    canonical_sample = _flow_unique_list._ORIGINAL_SAMPLE_EXACT_I64
    opened = False
    sample_calls = 0

    def tracked_iter(value: Iterable[object]) -> Iterator[object]:
        nonlocal opened
        opened = True
        return canonical_iter(value)

    def tracked_sample(source: list[object] | tuple[object, ...]) -> int:
        nonlocal sample_calls
        assert opened
        sample_calls += 1
        return canonical_sample(source)

    monkeypatch.setattr(source_module, "iter", tracked_iter, raising=False)
    monkeypatch.setattr(_flow_unique_list, "_CANONICAL_SAMPLE_EXACT_I64", tracked_sample)
    monkeypatch.setattr(_flow_unique_list, "_ORIGINAL_SAMPLE_EXACT_I64", tracked_sample)
    monkeypatch.setattr(
        _flow_unique_list,
        "_CANONICAL_SAMPLE_EXACT_I64_CODE",
        tracked_sample.__code__,
    )

    assert query.to_list() == list(range(1_000, 1_016))
    assert sample_calls == 1


def test_auto_unique_replaced_sampler_alias_deopts_before_sampling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A replaced private sampler cannot run only on the automatic engine."""
    from fpstreams import _native
    from fpstreams.streams._terminals import unique as _flow_unique_list

    values = [int(str(1_000 + index % 16)) for index in range(131_072)]
    sample_calls = 0

    def sample(_source: object) -> int:
        nonlocal sample_calls
        sample_calls += 1
        return _flow_unique_list._UNIQUE_SAMPLE_CACHED

    def reject_direct(*_arguments: object) -> None:
        raise AssertionError("a replaced sampler alias must bypass the direct sink")

    monkeypatch.setattr(_flow_unique_list, "_CANONICAL_SAMPLE_EXACT_I64", sample)
    monkeypatch.setattr(_native, "unique_i64_exact_prefix_v1", reject_direct)
    monkeypatch.setattr(
        _native,
        "unique_i64_exact_prefix_cached_v1",
        reject_direct,
    )
    monkeypatch.setattr(
        _native,
        "unique_i64_exact_prefix_identity_cached_v1",
        reject_direct,
    )

    result = flow(values).unique().to_list()

    assert result == list(range(1_000, 1_016))
    assert sample_calls == 0


def test_auto_unique_cached_sink_falls_back_for_an_older_native_module(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A wheel without the optional cached symbol retains the baseline v1 path."""
    from fpstreams import _native

    values = [int(str(1_000_000 + index % 16)) for index in range(131_072)]
    assert type(values[0]) is type(values[16]) is int
    assert values[0] == values[16] and values[0] is not values[16]
    endpoint = _native.unique_i64_exact_prefix_v1
    baseline_calls = 0

    def tracked_baseline(output: list[object], source: Iterator[object]) -> object:
        nonlocal baseline_calls
        baseline_calls += 1
        return endpoint(output, source)

    monkeypatch.setattr(_native, "unique_i64_exact_prefix_v1", tracked_baseline)
    monkeypatch.delattr(_native, "unique_i64_exact_prefix_cached_v1")

    result = flow(values).unique().to_list()

    assert result == list(range(1_000_000, 1_000_016))
    assert baseline_calls == 1


def test_auto_unique_sample_allocation_failure_keeps_the_baseline_route(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Classifier bookkeeping cannot introduce a new allocation failure."""
    from fpstreams.streams._terminals import unique as _flow_unique_list

    values = [index % 16 for index in range(131_072)]

    def fail_dict(*_arguments: object) -> dict[object, object]:
        raise MemoryError("sample bookkeeping")

    monkeypatch.setattr(_flow_unique_list, "_BUILTIN_DICT", fail_dict)

    assert _flow_unique_list._sample_exact_i64(values) == _flow_unique_list._UNIQUE_SAMPLE_BASELINE


def test_auto_unique_i64_prefix_normalizes_booleans_without_replacing_them() -> None:
    """Boolean keys share 0/1 with integers while retaining the first Python object."""
    values: list[object] = [int(str(1_000 + index % 16)) for index in range(131_072)]
    values[500:504] = [True, 1, False, 0]

    result = flow(values).unique().run_with_report("to_list")

    assert result.value == [*range(1_000, 1_016), True, False]
    assert result.value[-2] is True
    assert result.value[-1] is False
    assert result.report.strategy == "rust_direct"


def test_auto_unique_sampler_accepts_exact_booleans() -> None:
    """Exact booleans may use the integer sink without losing their first identities."""
    from fpstreams.streams._terminals import unique as _flow_unique_list

    values = [True, False] * 65_536

    assert (
        _flow_unique_list._sample_exact_i64(values)
        == _flow_unique_list._UNIQUE_SAMPLE_IDENTITY_CACHED
    )
    result = flow(values).unique().run_with_report("to_list")

    assert result.value == [True, False]
    assert result.value[0] is True
    assert result.value[1] is False
    assert result.report.strategy == "rust_direct"


def test_auto_unique_direct_sink_respects_threshold_and_forced_engines(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only a sufficiently large auto plan may select the source-opening direct sink."""
    from fpstreams import _native

    endpoint = _native.unique_i64_exact_prefix_v1
    calls = 0

    def tracked(output: list[object], source: Iterator[object]) -> object:
        nonlocal calls
        calls += 1
        return endpoint(output, source)

    monkeypatch.setattr(_native, "unique_i64_exact_prefix_v1", tracked)

    below_threshold = list(range(4_095))
    assert flow(below_threshold).unique().to_list() == below_threshold
    assert calls == 0

    values = list(range(4_096))
    assert flow(values).unique().to_list() == values
    assert calls == 1

    assert flow(values).with_engine("python").unique().to_list() == values
    assert flow(values).with_engine("native").unique().to_list() == values
    assert calls == 1


def test_unique_closes_source_when_hashing_fails() -> None:
    """A public value-protocol failure cannot leak an already-opened unique source."""
    closed = 0
    failure = RuntimeError("hash failed")

    class BadHash:
        def __hash__(self) -> int:
            raise failure

    def values() -> Iterator[BadHash]:
        nonlocal closed
        try:
            yield BadHash()
        finally:
            closed += 1

    with pytest.raises(RuntimeError) as captured:
        flow(values()).unique().to_list()

    assert captured.value is failure
    assert closed == 1


@pytest.mark.skipif(sys.platform == "win32", reason="setitimer is Unix-only")
def test_auto_unique_identity_cache_keeps_first_objects_alive_during_signal_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A source shrink cannot invalidate pointer-cache entries owned by the output."""
    import signal

    from fpstreams import _native

    first_objects = [int(str(1_000_000 + index)) for index in range(16)]
    assert type(first_objects[0]) is int and first_objects[0] is not int("1000000")
    first_ids = [id(value) for value in first_objects]
    values = first_objects * 131_072
    del first_objects
    endpoint = _native.unique_i64_exact_prefix_identity_cached_v1
    handled_signal = False
    previous = signal.getsignal(signal.SIGALRM)

    def start_alarm(output: list[object], source: Iterator[object]) -> object:
        signal.setitimer(signal.ITIMER_REAL, 0.001, 0)
        return endpoint(output, source)

    def replace_source(_signum: int, _frame: object) -> None:
        nonlocal handled_signal
        handled_signal = True
        values[:] = [10_000]

    monkeypatch.setattr(
        _native,
        "unique_i64_exact_prefix_identity_cached_v1",
        start_alarm,
    )
    signal.signal(signal.SIGALRM, replace_source)
    try:
        result = flow(values).unique().to_list()
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)

    assert handled_signal is True
    assert result == list(range(1_000_000, 1_000_016))
    assert [id(value) for value in result] == first_ids


@pytest.mark.skipif(sys.platform == "win32", reason="setitimer is Unix-only")
def test_unique_identity_cache_owns_slots_when_direct_caller_clears_output() -> None:
    """Cache referents survive direct-ABI output and source mutation between signal checks."""
    import signal

    from fpstreams import _native

    first_objects = [int(str(1_000_000 + index)) for index in range(16)]
    assert type(first_objects[0]) is int and first_objects[0] is not int("1000000")
    values = first_objects * 131_072
    del first_objects
    output: list[object] = []
    handled_signal = False
    previous = signal.getsignal(signal.SIGALRM)

    def clear_output_and_replace_source(_signum: int, _frame: object) -> None:
        nonlocal handled_signal
        handled_signal = True
        output.clear()
        replacement = int("2000")
        values[:] = [replacement] * len(values)

    signal.signal(signal.SIGALRM, clear_output_and_replace_source)
    signal.setitimer(signal.ITIMER_REAL, 0.001, 0)
    try:
        completed = _native.unique_i64_exact_prefix_identity_cached_v1(
            output,
            iter(values),
        )
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)

    assert handled_signal is True
    assert completed == (None, True)
    assert output == [2_000]


def test_unique_identity_cached_endpoint_declines_on_free_threaded_python() -> None:
    """The identity cache never consumes a mutable sequence without the GIL."""
    is_gil_enabled = getattr(sys, "_is_gil_enabled", None)
    if is_gil_enabled is None or is_gil_enabled():
        pytest.skip("requires a free-threaded interpreter")

    from fpstreams import _native

    values = [1, 1]
    source = iter(values)
    output: list[object] = []

    assert _native.unique_i64_exact_prefix_identity_cached_v1(output, source) is None
    assert output == []
    assert next(source) == 1


def test_auto_unique_native_decline_reuses_the_open_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unconsumed optional-endpoint decline must not reopen a reiterable source."""
    from fpstreams import _native
    from fpstreams.planning import source as source_module

    values = [index % 16 for index in range(131_072)]
    query = flow(values).unique()
    canonical_iter = iter
    opens = 0

    def tracked_iter(value: Iterable[object]) -> Iterator[object]:
        nonlocal opens
        opens += 1
        return canonical_iter(value)

    def decline(_output: list[object], _source: Iterator[object]) -> None:
        return None

    monkeypatch.setattr(source_module, "iter", tracked_iter, raising=False)
    monkeypatch.setattr(_native, "unique_i64_exact_prefix_v1", decline)
    monkeypatch.setattr(_native, "unique_i64_exact_prefix_cached_v1", decline)
    monkeypatch.setattr(_native, "unique_i64_exact_prefix_identity_cached_v1", decline)

    execution = query.run_with_report("to_list")

    assert execution.value == list(range(16))
    assert execution.report.strategy == "python_direct"
    assert opens == 1


def test_auto_unique_i64_prefix_resumes_mixed_python_semantics() -> None:
    """The first incompatible value crosses once into the canonical seeded loop."""
    size = 131_072
    huge = 2**100
    unhashable = ["value"]
    values: list[object] = [*range(size), huge, True, 1.0, unhashable, ["value"], size - 1]

    result = flow(values).unique().run_with_report("to_list")

    assert result.value == [*range(size), huge, unhashable]
    assert result.value[-1] is unhashable
    assert result.report.strategy == "rust_python_hybrid"


def test_auto_unique_i64_prefix_keeps_the_generator_stop_iteration_boundary() -> None:
    """A suffix protocol cannot turn generator StopIteration into terminal success."""

    class StopsHashing:
        def __hash__(self) -> int:
            raise StopIteration("hash stopped")

    values: list[object] = [*range(131_072), StopsHashing()]

    with pytest.raises(RuntimeError, match="generator raised StopIteration"):
        flow(values).unique().to_list()


def test_auto_unique_i64_prefix_declines_a_replaced_unique_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A customized mixed-value operation remains owned by canonical execution."""
    from fpstreams import _native
    from fpstreams.execution import sync_ops
    from fpstreams.planning.sync import UniqueOp

    handler = sync_ops.OPERATION_HANDLERS[UniqueOp]
    calls = 0

    def tracked(*arguments: object, **keywords: object) -> Iterator[object]:
        nonlocal calls
        calls += 1
        return handler(*arguments, **keywords)  # type: ignore[arg-type]

    def reject_direct(*_arguments: object) -> None:
        raise AssertionError("a replaced operation handler must bypass the direct sink")

    monkeypatch.setitem(sync_ops.OPERATION_HANDLERS, UniqueOp, tracked)
    monkeypatch.setattr(_native, "unique_i64_exact_prefix_v1", reject_direct)
    marker: list[str] = ["mixed"]
    result = flow([*range(131_072), marker, marker]).unique().to_list()

    assert result == [*range(131_072), marker]
    assert calls == 1


@pytest.mark.parametrize("scope", ["globals", "builtins"])
def test_auto_unique_i64_prefix_declines_a_replaced_set_protocol(
    scope: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A dynamically replaced canonical set constructor remains observable."""
    import builtins

    from fpstreams import _native
    from fpstreams.execution import sync_ops
    from fpstreams.streams._terminals import unique as _flow_unique_list

    marker: list[str] = ["mixed"]
    values: list[object] = [index % 16 for index in range(131_072)]
    values.append(marker)
    python_plan = flow(values).with_engine("python").unique()
    automatic_plan = flow(values).unique()

    def reject_direct(*_arguments: object) -> None:
        raise AssertionError("a replaced set constructor must bypass the direct sink")

    monkeypatch.setattr(_native, "unique_i64_exact_prefix_v1", reject_direct)
    monkeypatch.setattr(_native, "unique_i64_exact_prefix_cached_v1", reject_direct)
    monkeypatch.setattr(
        _native,
        "unique_i64_exact_prefix_identity_cached_v1",
        reject_direct,
    )

    with monkeypatch.context() as protocol_patch:
        owner = sync_ops if scope == "globals" else builtins
        protocol_patch.setattr(owner, "set", lambda: {0}, raising=False)
        assert not _flow_unique_list._canonical_unique_start_intact()
        expected = python_plan.to_list()
        result = automatic_plan.to_list()

    assert result == expected == [*range(1, 16), marker]


def test_auto_unique_i64_prefix_declines_replaced_handler_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mutating the canonical function object cannot bypass its code guard."""
    from fpstreams import _native
    from fpstreams.execution import sync_ops
    from fpstreams.planning.sync import UniqueOp

    marker: list[str] = ["mixed"]
    values: list[object] = [index % 16 for index in range(131_072)]
    values.append(marker)
    handler = sync_ops.OPERATION_HANDLERS[UniqueOp]

    def empty_unique(_iterator: Iterator[object], _operation: object) -> Iterator[object]:
        yield from ()

    def reject_direct(*_arguments: object) -> None:
        raise AssertionError("replaced handler code must bypass the direct sink")

    monkeypatch.setattr(handler, "__code__", empty_unique.__code__)
    monkeypatch.setattr(_native, "unique_i64_exact_prefix_v1", reject_direct)
    monkeypatch.setattr(_native, "unique_i64_exact_prefix_cached_v1", reject_direct)
    monkeypatch.setattr(
        _native,
        "unique_i64_exact_prefix_identity_cached_v1",
        reject_direct,
    )

    assert flow(values).unique().to_list() == []


def test_auto_unique_i64_prefix_declines_replaced_pair_selector_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The canonical handler's startup branch remains dynamically observable."""
    from fpstreams import _native
    from fpstreams.execution import sync_ops

    marker: list[str] = ["mixed"]
    values: list[object] = [index % 16 for index in range(131_072)]
    values.append(marker)
    python_plan = flow(values).with_engine("python").unique()
    automatic_plan = flow(values).unique()

    def reject_direct(*_arguments: object) -> None:
        raise AssertionError("a replaced pair selector must bypass the direct sink")

    monkeypatch.setattr(sync_ops, "PAIR_KEY_SELECTOR", None)
    monkeypatch.setattr(_native, "unique_i64_exact_prefix_v1", reject_direct)
    monkeypatch.setattr(_native, "unique_i64_exact_prefix_cached_v1", reject_direct)
    monkeypatch.setattr(
        _native,
        "unique_i64_exact_prefix_identity_cached_v1",
        reject_direct,
    )

    with pytest.raises(TypeError) as expected:
        python_plan.to_list()
    with pytest.raises(TypeError) as actual:
        automatic_plan.to_list()

    assert str(actual.value) == str(expected.value)


def test_direct_unique_dispatch_observes_the_live_module_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The eager canonical snapshot must not freeze the public optimizer hook."""
    from fpstreams.streams._terminals import unique as _flow_unique_list

    def replacement(*_arguments: object) -> tuple[bool, list[str]]:
        return True, ["replacement"]

    monkeypatch.setattr(_flow_unique_list, "try_direct_unique_list", replacement)

    assert flow([1, 1]).unique().to_list() == ["replacement"]


def test_auto_unique_sampler_code_is_guarded_before_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Replacing the sampler function's code deopts without running injected code."""
    from fpstreams import _native
    from fpstreams.streams._terminals import unique as _flow_unique_list

    marker: list[str] = ["mixed"]
    values: list[object] = [index % 16 for index in range(131_072)]
    values.append(marker)
    sampler = _flow_unique_list._ORIGINAL_SAMPLE_EXACT_I64

    def injected_sampler(_source: object) -> int:
        raise AssertionError("injected sampler code executed")

    def reject_direct(*_arguments: object) -> None:
        raise AssertionError("replaced sampler code must bypass the direct sink")

    monkeypatch.setattr(sampler, "__code__", injected_sampler.__code__)
    monkeypatch.setattr(_native, "unique_i64_exact_prefix_v1", reject_direct)
    monkeypatch.setattr(_native, "unique_i64_exact_prefix_cached_v1", reject_direct)
    monkeypatch.setattr(
        _native,
        "unique_i64_exact_prefix_identity_cached_v1",
        reject_direct,
    )

    assert flow(values).unique().to_list() == [*range(16), marker]


def test_unique_live_binding_resolver_preserves_missing_name_metadata() -> None:
    """A missing canonical LOAD_GLOBAL retains the name on its NameError."""
    from fpstreams.streams._terminals import unique as _flow_unique_list

    handler = _flow_unique_list._CANONICAL_UNIQUE_HANDLER
    globals_namespace = handler.__globals__
    builtins_namespace = handler.__builtins__
    missing = object()
    previous_global = globals_namespace.pop("any", missing)
    previous_builtin = builtins_namespace.pop("any")
    captured: NameError | None = None
    try:
        try:
            _flow_unique_list._resolved_unique_binding("any")
        except NameError as error:
            captured = error
    finally:
        builtins_namespace["any"] = previous_builtin
        if previous_global is not missing:
            globals_namespace["any"] = previous_global

    assert captured is not None
    assert captured.name == "any"


@pytest.mark.skipif(sys.platform == "win32", reason="setitimer is Unix-only")
@pytest.mark.parametrize("binding", ["any", "TypeError"])
def test_auto_unique_hybrid_suffix_resolves_late_exception_bindings(
    binding: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Signal-time changes affect the suffix at the same boundary as canonical Python."""
    import signal

    from fpstreams import _native
    from fpstreams.execution import sync_ops

    shared = [int(str(1_000 + index)) for index in range(16)]
    marker: list[str] = ["mixed"]
    values: list[object] = [*(shared * 131_072), marker, marker]
    endpoint = _native.unique_i64_exact_prefix_identity_cached_v1
    previous_signal = signal.getsignal(signal.SIGALRM)
    missing = object()
    previous_binding = sync_ops.__dict__.get(binding, missing)
    handled_signal = False

    def start_alarm(output: list[object], source: Iterator[object]) -> object:
        signal.setitimer(signal.ITIMER_REAL, 0.001, 0)
        return endpoint(output, source)

    def replace_binding(_signum: int, _frame: object) -> None:
        nonlocal handled_signal
        handled_signal = True
        if binding == "any":
            sync_ops.any = lambda _values: False
        else:
            sync_ops.TypeError = ValueError

    monkeypatch.setattr(
        _native,
        "unique_i64_exact_prefix_identity_cached_v1",
        start_alarm,
    )
    signal.signal(signal.SIGALRM, replace_binding)
    try:
        if binding == "TypeError":
            with pytest.raises(TypeError, match="unhashable type"):
                flow(values).unique().to_list()
        else:
            result = flow(values).unique().to_list()
            assert result == [*shared, marker, marker]
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_signal)
        if previous_binding is missing:
            sync_ops.__dict__.pop(binding, None)
        else:
            sync_ops.__dict__[binding] = previous_binding

    assert handled_signal is True


def test_auto_unique_direct_sink_declines_active_failpoints(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Instrumentation declines the direct sink and remains visible to Python execution."""
    from fpstreams import _native
    from fpstreams.runtime.failpoints import failpoint

    def reject_direct(*_arguments: object) -> None:
        raise AssertionError("the new source-opening sink must decline instrumentation")

    monkeypatch.setattr(_native, "unique_i64_exact_prefix_v1", reject_direct)
    with (
        failpoint("source.open.after", RuntimeError("unopened native source")),
        pytest.raises(RuntimeError, match="unopened native source"),
    ):
        flow(list(range(131_072))).unique().to_list()


def test_structural_operations_stream_immutable_groups() -> None:
    assert flow(range(5)).chunk(2).to_list() == [(0, 1), (2, 3), (4,)]
    assert flow(range(4)).window(3).to_list() == [(0, 1, 2), (1, 2, 3)]
    assert flow([1, 4, 9]).pairwise().to_list() == [(1, 4), (4, 9)]
    assert flow([1, 2, 3]).scan(0, lambda total, value: total + value).to_list() == [
        1,
        3,
        6,
    ]


@pytest.mark.parametrize("batched", [False, True])
def test_chunk_strict_rejects_only_an_incomplete_final_batch_and_closes_source(
    monkeypatch: pytest.MonkeyPatch, batched: bool
) -> None:
    if not batched:
        from fpstreams.execution import sync_ops

        monkeypatch.setattr(sync_ops, "_BATCHED", None)
    closed = []

    def source():
        try:
            yield from range(5)
        finally:
            closed.append(True)

    assert flow([]).chunk(2, strict=True).to_list() == []
    assert flow(range(4)).batch(2, strict=True).to_list() == [(0, 1), (2, 3)]
    assert flow(range(5)).chunk(2).to_list() == [(0, 1), (2, 3), (4,)]
    for retained in (list(range(5)), tuple(range(5)), range(5)):
        for terminal in ("to_list", "to_tuple", "count"):
            with pytest.raises(ValueError, match="incomplete final chunk"):
                getattr(flow(retained).chunk(2, strict=True), terminal)()
    assert flow(source()).chunk(2, strict=True).take(1).to_list() == [(0, 1)]
    assert closed == [True]
    with pytest.raises(ValueError, match="incomplete final chunk"):
        flow(source()).chunk(2, strict=True).to_list()
    assert closed == [True, True]


def test_chunk_explain_distinguishes_strict_short_tail_and_short_circuit() -> None:
    def cardinality(values):
        return values.explain().to_dict()["semantics"]["output"]["cardinality"]

    assert cardinality(flow(range(5)).chunk(2)) == {"kind": "exact", "value": 3}
    assert cardinality(flow(range(4)).chunk(2, strict=True)) == {"kind": "exact", "value": 2}
    assert cardinality(flow(range(5)).chunk(2, strict=True)) == {"kind": "upper_bound", "value": 2}
    assert cardinality(flow(range(5)).filter(bool).chunk(2, strict=True)) == {
        "kind": "upper_bound",
        "value": 2,
    }
    assert cardinality(flow(range(5)).chunk(2, strict=True).take(1)) == {
        "kind": "upper_bound",
        "value": 1,
    }
    assert flow(range(5)).chunk(2, strict=True).take(1).to_list() == [(0, 1)]


def test_latest_jdk_gatherer_semantics_and_streamex_conveniences() -> None:
    initializer_calls = 0

    def initialize() -> list[int]:
        nonlocal initializer_calls
        initializer_calls += 1
        return []

    folded = flow([1, 2, 3]).fold(
        initialize,
        lambda values, item: [*values, item],
    )

    assert flow([1, 2]).window(3).to_list() == [(1, 2)]
    assert flow([]).window(3).to_list() == []
    assert folded.map(tuple).to_list() == [(1, 2, 3)]
    assert folded.map(tuple).to_list() == [(1, 2, 3)]
    assert initializer_calls == 2
    assert flow([]).fold(list, lambda values, item: [*values, item]).to_list() == [[]]
    assert flow([1, 2, 3, 4]).filter_map(
        lambda item: str(item) if item % 2 == 0 else None
    ).to_list() == ["2", "4"]
    assert flow([1, 4, 9]).pair_map(lambda left, right: right - left).to_list() == [
        3,
        5,
    ]
    assert flow([1, 1, 2, 4, 3, 3]).group_runs(lambda item: item % 2).to_list() == [
        (1, 1),
        (2, 4),
        (3, 3),
    ]


def test_bounded_selection_and_minmax_avoid_full_sorting_api_noise() -> None:
    records = [
        {"id": "a", "score": 9},
        {"id": "b", "score": 4},
        {"id": "c", "score": 9},
        {"id": "d", "score": 7},
    ]

    assert flow(records).top(2, key="score") == [records[0], records[2]]
    assert flow(records).bottom(2, key="score") == [records[1], records[3]]
    assert flow([3, 1, 4, 2]).minmax() == (1, 4)
    assert flow(records).minmax(key="score") == (records[1], records[0])
    assert flow(records).top(0, key="score") == []
    with pytest.raises(fpstreams.EmptyFlowError):
        flow([]).minmax()
    with pytest.raises(ValueError):
        flow([1]).bottom(-1)


def test_keyless_minmax_uses_one_truthful_query_and_keeps_equal_representatives(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The direct comparison fallback retains the first equal minimum and maximum objects."""

    class Ranked:
        def __init__(self, rank: int, label: str) -> None:
            self.rank = rank
            self.label = label

        def __lt__(self, other: object) -> bool:
            assert isinstance(other, Ranked)
            return self.rank < other.rank

        def __gt__(self, other: object) -> bool:
            assert isinstance(other, Ranked)
            return self.rank > other.rank

    first_low = Ranked(1, "first-low")
    later_low = Ranked(1, "later-low")
    first_high = Ranked(3, "first-high")
    later_high = Ranked(3, "later-high")
    values = flow([first_low, later_low, first_high, later_high])
    seen: list[str] = []
    original = type(values)._query

    def tracked(self, name, *arguments, **options):
        seen.append(name)
        return original(self, name, *arguments, **options)

    monkeypatch.setattr(type(values), "_query", tracked)

    minimum, maximum = values.minmax()

    assert minimum is first_low
    assert maximum is first_high
    assert seen == ["minmax"]


def test_minmax_one_shot_failpoint_closes_the_canonical_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An instrumented callback failure closes one claimed source under the minmax query."""
    from fpstreams.runtime.failpoints import failpoint

    events: list[str] = []

    def source() -> Iterator[int]:
        events.append("open")
        try:
            yield from (3, 1, 4, 2)
        finally:
            events.append("close")

    values = flow(source()).map(lambda value: value)
    seen: list[str] = []
    original = type(values)._query

    def tracked(self, name, *arguments, **options):
        seen.append(name)
        return original(self, name, *arguments, **options)

    monkeypatch.setattr(type(values), "_query", tracked)

    with (
        failpoint("callback.before", RuntimeError("instrumented minmax")),
        pytest.raises(RuntimeError, match="instrumented minmax"),
    ):
        values.minmax()

    assert seen == ["minmax"]
    assert events == ["open", "close"]


def test_size_constrained_batches_are_lazy_and_enforce_both_limits() -> None:
    opened = False

    def payloads():
        nonlocal opened
        opened = True
        yield from (b"12345", b"123", b"12345678", b"1", b"1", b"12", b"1")

    batches = flow.defer(payloads).batch_by_size(10, max_count=2)

    assert not opened
    assert batches.to_list() == [
        (b"12345", b"123"),
        (b"12345678", b"1"),
        (b"1", b"12"),
        (b"1",),
    ]
    assert opened
    assert flow(["oversized"]).batch_by_size(3, strict=False).to_list() == [("oversized",)]
    with pytest.raises(ValueError, match="exceeds max_size"):
        flow(["oversized"]).batch_by_size(3).to_list()
    with pytest.raises(ValueError, match="non-negative"):
        flow([1]).batch_by_size(3, get_size=lambda _item: -1).to_list()


def test_reduce_by_keeps_only_an_independent_accumulator_per_key() -> None:
    initializer_calls = 0

    def initialize() -> list[int]:
        nonlocal initializer_calls
        initializer_calls += 1
        return []

    def append(values: list[int], item: int) -> list[int]:
        values.append(item)
        return values

    grouped = flow(range(6)).reduce_by(
        lambda value: value % 2,
        append,
        initializer=initialize,
    )

    assert grouped == {0: [0, 2, 4], 1: [1, 3, 5]}
    assert initializer_calls == 2
    assert grouped[0] is not grouped[1]
    assert flow("abaca").frequencies() == {"a": 3, "b": 1, "c": 1}


def test_frequencies_does_not_relabel_selector_type_errors_as_unhashable_keys() -> None:
    """Selector errors propagate; an unhashable list retains the historical message."""

    def fail(_value: int) -> int:
        raise TypeError("selector failed")

    with pytest.raises(TypeError, match=r"^selector failed$"):
        flow([1]).frequencies(fail)

    def misleading_failure(_value: int) -> int:
        raise TypeError("unhashable type: domain validator")

    with pytest.raises(TypeError, match=r"^unhashable type: domain validator$"):
        flow([1]).frequencies(misleading_failure)
    with pytest.raises(TypeError, match=r"^reduce_by\(\) keys must be hashable$"):
        flow([[1]]).frequencies()


@pytest.mark.parametrize(
    "operation",
    ["reduce", "frequency", "selected", "pair_collector", "pair_callable", "pair_aggregate"],
)
def test_grouping_preserves_key_equality_failure_and_closes(operation: str) -> None:
    failure = TypeError("equality failed")
    events: list[str] = []

    class Key:
        def __hash__(self) -> int:
            events.append("hash")
            return 1

        def __eq__(self, other: object) -> bool:
            events.append("eq")
            raise failure

    def source():
        try:
            yield Key()
            yield Key()
        finally:
            events.append("close")

    values = flow(source()).with_engine("python")
    with pytest.raises(TypeError) as captured:
        if operation == "reduce":
            values.reduce_by(lambda value: value, lambda state, _: state + 1, initializer=lambda: 0)
        elif operation == "frequency":
            values.frequencies()
        elif operation == "selected":
            values.frequencies(lambda value: value)
        elif operation == "pair_collector":
            values.map(lambda value: (value, 1)).pairs().collect_values(
                fpstreams.Collectors.counting()
            )
        elif operation == "pair_callable":
            values.map(lambda value: (value, 1)).pairs().collect_values(list)
        else:
            values.map(lambda value: (value, 1)).pairs().aggregate_values(
                count=fpstreams.Collectors.counting()
            )
    assert captured.value is failure
    assert events[-2:] == ["eq", "close"]
    assert events.count("hash") == (2 if operation == "pair_callable" else 3)


def test_grouping_preserves_custom_hash_type_error() -> None:
    failure = TypeError("hash failed")
    source_failure = TypeError("source failed")

    class Key:
        def __hash__(self) -> int:
            raise failure

    with pytest.raises(TypeError) as captured:
        flow([Key()]).frequencies()
    assert captured.value is failure
    with pytest.raises(TypeError, match="unhashable"):
        flow([([],)]).frequencies()

    def source():
        yield 1
        raise source_failure

    with pytest.raises(TypeError) as captured:
        flow(source()).frequencies()
    assert captured.value is source_failure


@pytest.mark.parametrize("engine", ["python", "auto"])
@pytest.mark.parametrize("reverse", [False, True])
def test_distinct_handles_equal_hashable_and_unhashable_values(engine, reverse):
    first, second = ({1}, frozenset({1})) if not reverse else (frozenset({1}), {1})
    result = flow([first, second, {2}]).with_engine(engine).distinct().to_list()
    assert len(result) == 2
    assert result[0] is first
    assert result[1] == {2}
    rows = [(first, "first"), (second, "second")]
    result_by = flow(rows).with_engine(engine).distinct_by(lambda pair: pair[0]).to_list()
    assert result_by == [rows[0]]
    pairs = list(fpstreams.pairs(rows).with_engine(engine).unique_keys())
    assert pairs == [rows[0]]


def test_distinct_mixed_comparison_failure_preserves_exception_and_closes_source():
    failure = ValueError("mixed comparison failed")
    closed = []

    class Unhashable:
        __hash__ = None

        def __eq__(self, other):
            raise failure

    def source():
        try:
            yield Unhashable()
            yield 1
        finally:
            closed.append(True)

    with pytest.raises(ValueError) as caught:
        flow(source()).distinct().to_list()
    assert caught.value is failure
    assert closed == [True]


def test_distinct_preserves_nan_identity_and_numeric_equality_with_mixed_keys():
    first_nan, second_nan = float("nan"), float("nan")
    result = (
        flow([{1}, frozenset({1}), first_nan, first_nan, second_nan, True, 1, 1.0])
        .distinct()
        .to_list()
    )
    assert len(result) == 4
    assert result[1] is first_nan and result[2] is second_nan and result[3] is True


@pytest.mark.parametrize("reverse", [False, True])
def test_distinct_cross_category_comparison_direction(reverse):
    events = []

    class Hashable:
        def __hash__(self):
            return 1

        def __eq__(self, other):
            events.append("hashable-first")
            return False

    class Unhashable:
        __hash__ = None

        def __eq__(self, other):
            events.append("unhashable-first")
            return False

    hashable, unhashable = Hashable(), Unhashable()
    items = [hashable, unhashable] if reverse else [unhashable, hashable]
    assert len(flow(items).distinct().to_list()) == 2
    assert events == ["unhashable-first"]


def test_distinct_native_prefix_matches_later_unhashable_value(monkeypatch):
    from fpstreams import _native

    endpoint = _native.unique_i64_exact_prefix_v1
    called = []

    def tracked(output, source):
        called.append(True)
        return endpoint(output, source)

    class EqualZero:
        __hash__ = None

        def __eq__(self, other):
            return other == 0

    monkeypatch.setattr(_native, "unique_i64_exact_prefix_v1", tracked)
    values = list(range(4_096))
    result = flow([*values, EqualZero()]).distinct().run_with_report("to_list")
    assert called and result.report.strategy == "rust_python_hybrid"
    assert result.value == values


def test_grouping_preserves_equality_error_with_custom_metaclass() -> None:
    failure = TypeError("equality failed")
    events: list[str] = []

    class Meta(type):
        def __getattribute__(cls, name):
            if name == "__hash__":
                raise RuntimeError("metaclass hash lookup")
            return super().__getattribute__(name)

    class Key(metaclass=Meta):
        def __hash__(self):
            events.append("hash")
            return 1

        def __eq__(self, other):
            events.append("eq")
            raise failure

    with pytest.raises(TypeError) as captured:
        flow([Key(), Key()]).frequencies()
    assert captured.value is failure
    assert events == ["hash", "hash", "hash", "eq"]

    class Unhashable:
        __hash__ = None

    class Child(Unhashable):
        pass

    with pytest.raises(TypeError, match="keys must be hashable"):
        flow([Child()]).frequencies()

    class DescriptorMeta(type):
        @property
        def __hash__(cls):
            raise RuntimeError("metaclass descriptor")

    class DescriptorKey(metaclass=DescriptorMeta):
        def __hash__(self):
            return 1

        def __eq__(self, other):
            raise failure

    with pytest.raises(TypeError) as captured:
        flow([DescriptorKey(), DescriptorKey()]).frequencies()
    assert captured.value is failure


def test_frequencies_selector_runs_once_per_item_and_closes_after_failure() -> None:
    """Selector errors stay outside dictionary handling and release a one-shot source."""
    events: list[str] = []

    def values() -> Iterator[int]:
        try:
            yield from (1, 2, 3)
        finally:
            events.append("close")

    def select(value: int) -> int:
        events.append(f"select:{value}")
        if value == 2:
            raise ValueError("selector stopped")
        return value

    with pytest.raises(ValueError, match="selector stopped"):
        flow(values()).frequencies(select)

    assert events == ["select:1", "select:2", "close"]


@pytest.mark.parametrize("engine", ["python", "auto"])
@pytest.mark.parametrize("keyed", [False, True])
def test_frequencies_preserves_custom_key_hashing(engine: str, keyed: bool) -> None:
    """Retained sources keep the canonical lookup-then-assignment key protocol."""

    class FailsOnSecondHash:
        def __init__(self) -> None:
            self.calls = 0

        def __hash__(self) -> int:
            self.calls += 1
            if self.calls == 2:
                raise RuntimeError("second hash")
            return 1

    key = FailsOnSecondHash()
    with pytest.raises(RuntimeError, match=r"^second hash$"):
        flow([key]).with_engine(engine).frequencies((lambda value: value) if keyed else None)
    assert key.calls == 2


def test_identity_frequencies_uses_the_exact_i64_native_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A retained exact-int container counts once without entering the Python item loop."""
    from fpstreams import _native

    kernel = _native.frequencies_i64_exact_v1
    calls: list[object] = []

    def tracked(source: object) -> object:
        calls.append(source)
        return kernel(source)

    monkeypatch.setattr(_native, "frequencies_i64_exact_v1", tracked)
    first = int("1000000")
    equal = int("1000000")
    values = [first, -7, equal]

    counts = flow(values).frequencies()

    assert calls == [values]
    assert list(counts.values()) == [2, 1]
    assert next(iter(counts)) is first


def test_identity_frequencies_resumes_after_a_bounded_native_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A wider distribution resumes from native state instead of replaying the source."""
    from fpstreams import _native

    kernel = _native.frequencies_i64_exact_v1
    results: list[object] = []

    def tracked(source: object) -> object:
        result = kernel(source)
        results.append(result)
        return result

    monkeypatch.setattr(_native, "frequencies_i64_exact_v1", tracked)
    values = [*range(258), 256]

    counts = flow(values).frequencies()

    assert results and type(results[0]) is tuple
    assert counts == {value: 2 if value == 256 else 1 for value in range(258)}


@pytest.mark.parametrize("container", [list, tuple])
def test_frequencies_native_continuation_preserves_builtin_key_identity(container) -> None:
    first = int("100000000000000000000000000000")
    equal = int("100000000000000000000000000000")
    nan = float("nan")
    values = container(
        [
            first,
            equal,
            "中文",
            b"a\0b",
            True,
            1,
            1.0,
            None,
            nan,
            nan,
            float("inf"),
            *range(600),
            "中文",
            b"a\0b",
            -0.0,
        ]
    )
    expected = flow(values).with_engine("python").frequencies()
    observed = flow(values).run_with_report("frequencies")
    assert observed.value == expected
    assert all(left is right for left, right in zip(observed.value, expected, strict=True))
    assert next(iter(observed.value)) is first
    if not hasattr(sys, "_is_gil_enabled") or sys._is_gil_enabled():
        assert observed.report.strategy == "rust_direct"


@pytest.mark.parametrize("failure", [None, KeyError, TypeError, RuntimeError])
def test_frequencies_native_boundary_preserves_custom_hashes_and_live_tail(failure) -> None:
    def run(engine):
        events = []
        values = list(range(600))

        class Key:
            calls = 0

            def __hash__(self):
                self.calls += 1
                events.append(("hash", self.calls))
                if self.calls == 1:
                    values[-1] = "replaced"
                if failure is not None and self.calls == 2:
                    raise failure("second hash")
                return 300

            def __eq__(self, other):
                events.append(("equal", type(other).__name__))
                return other == 300

        key = Key()
        values.extend((key, 999))
        try:
            observed = flow(values).with_engine(engine).run_with_report("frequencies")
            if engine == "auto":
                assert observed.report.strategy == "python_frequency"
            outcome = list(observed.value.items())
        except (KeyError, TypeError, RuntimeError) as error:
            outcome = (type(error), str(error), type(error.__cause__))
        return outcome, events

    assert run("auto") == run("python")


def test_frequencies_continuation_preserves_one_shot_cleanup_and_older_extensions(monkeypatch):
    from fpstreams import _native

    def forbidden(*args):
        raise AssertionError("a one-shot source must not enter retained native frequency counting")

    closed = []

    def source():
        try:
            yield "a"
            yield "b"
            yield "a"
        finally:
            closed.append(True)

    with monkeypatch.context() as patch:
        patch.setattr(_native, "frequencies_i64_exact_v1", forbidden)
        patch.setattr(_native, "frequencies_exact_prefix_v1", forbidden)
        assert flow(source()).frequencies() == {"a": 2, "b": 1}
    assert closed == [True]
    monkeypatch.delattr(_native, "frequencies_exact_prefix_v1")
    values = [*range(600), "a", "a", 400]
    assert flow(values).frequencies() == flow(values).with_engine("python").frequencies()


def test_operated_frequencies_uses_the_compiled_native_iterate_route(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Counting consumes a selected fused backend instead of reopening its Python operations."""
    from fpstreams.streams import flow_terminals

    values = flow(range(4_096)).map(fpstreams.item + 1)
    physical, _pipeline = values._terminal_context("iterate")
    payload = physical.backend_payload
    assert payload is not None
    assert payload.native_decision is not None
    assert payload.native_decision.engine == "native"

    original_execute = flow_terminals.execute_physical
    executions = 0

    def tracked_execute(plan: object) -> Iterator[Any]:
        nonlocal executions
        executions += 1
        return original_execute(plan)  # type: ignore[arg-type]

    monkeypatch.setattr(flow_terminals, "execute_physical", tracked_execute)

    counts = values.frequencies()
    assert len(counts) == 4_096
    assert counts[1] == counts[4_096] == 1
    assert executions == 1


def test_keyed_frequencies_keeps_forced_native_execution(monkeypatch: pytest.MonkeyPatch) -> None:
    """A terminal callback does not turn an explicitly native pipeline into Python."""
    from fpstreams.streams import flow_terminals

    def unexpected(_pipeline):
        raise AssertionError("forced native pipeline was reopened as Python")

    monkeypatch.setattr(flow_terminals, "_open_python_pipeline_values", unexpected)
    values = flow(range(16)).map(fpstreams.item + 1).with_engine("native")
    assert values.frequencies(lambda value: value % 3) == {1: 6, 2: 5, 0: 5}


def test_keyed_frequencies_consumes_its_relational_input() -> None:
    """Frequency counting must execute the grouped plan before selecting result keys."""
    values = (
        flow([{"group": "a", "value": 1}, {"group": "a", "value": 2}])
        .rows()
        .group_by("group")
        .aggregate(total=fpstreams.agg.sum("value"))
        .to_flow()
    )
    assert values.frequencies("total") == {3: 1}


def test_operated_frequencies_preserves_active_source_failpoints() -> None:
    """Instrumentation owns auto execution even when iterate selected a native prefix."""
    from fpstreams.runtime.failpoints import failpoint

    failure = RuntimeError("instrumented frequencies source")
    with (
        failpoint("source.open.after", failure),
        pytest.raises(RuntimeError) as captured,
    ):
        flow(range(32)).map(fpstreams.item + 1).frequencies()

    assert captured.value is failure


def test_auto_identity_statistics_preserves_active_source_failpoints() -> None:
    """Instrumentation owns automatic execution even when identity statistics can use Rust."""
    from fpstreams.runtime.failpoints import failpoint

    failure = RuntimeError("instrumented statistics source")
    with (
        failpoint("source.open.after", failure),
        pytest.raises(RuntimeError) as captured,
    ):
        flow(list(range(4_096))).mean()

    assert captured.value is failure

    aggregate_failure = RuntimeError("instrumented mean aggregate source")
    with (
        failpoint("source.open.after", aggregate_failure),
        pytest.raises(RuntimeError) as aggregate_captured,
    ):
        flow(list(range(4_096))).aggregate(mean=fpstreams.agg.mean())

    assert aggregate_captured.value is aggregate_failure


def test_pythonic_combinators_cover_common_stream_bookkeeping() -> None:
    values = flow([10, None, 20]).compact()

    assert values.enumerate(start=1).to_list() == [(1, 10), (2, 20)]
    assert values.intersperse(0).to_list() == [10, 0, 20]
    assert values.zip("ab", strict=True).to_list() == [(10, "a"), (20, "b")]
    assert flow([1]).zip_longest([2, 3], fillvalue=0).to_list() == [
        (1, 2),
        (0, 3),
    ]
    assert flow([1]).concat([2, 3], (4,)).to_list() == [1, 2, 3, 4]


@pytest.mark.parametrize("left_values,limit", [([1, 2], 1), ([], 1), ([1, 2], None)])
def test_concat_closes_each_opened_source_once(left_values, limit) -> None:
    events = []

    class Source:
        def __init__(self, name, values):
            self.name = name
            self.values = iter(values)
            self.calls = 0

        def __iter__(self):
            return self

        def __next__(self):
            return next(self.values)

        def close(self):
            self.calls += 1
            events.append(self.name)
            if self.calls > 1:
                raise RuntimeError("closed twice")

    left = Source("left", left_values)
    right = Source("right", [3, 4])
    unused = Source("unused", [5])
    query = flow(left).concat(right, unused)
    result = query.take(limit).to_list() if limit is not None else query.to_list()
    if left_values and limit == 1:
        assert result == [1]
        assert (left.calls, right.calls, unused.calls) == (1, 0, 0)
        assert events == ["left"]
    elif limit == 1:
        assert result == [3]
        assert (left.calls, right.calls, unused.calls) == (1, 1, 0)
        assert events == ["right", "left"]
    else:
        assert result == [1, 2, 3, 4, 5]
        assert (left.calls, right.calls, unused.calls) == (1, 1, 1)
        assert events == ["right", "unused", "left"]


@pytest.mark.parametrize("transformed", [False, True])
def test_concat_does_not_close_aliased_upstream_twice(transformed: bool) -> None:
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
    values = flow(source)
    if transformed:
        values = values.map(lambda value: value)
    assert values.concat(source).to_list() == [1, 2]
    assert source.calls == 1


@pytest.mark.parametrize(
    "mode", ["same", "cross", "mapped-cross", "short-cross", "mapped-short-cross"]
)
def test_concat_does_not_close_repeated_one_shot_source_twice(mode: str) -> None:
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
    query = flow([]).concat(source)
    if "short" in mode:
        query = query.take(1)
    if "mapped" in mode:
        query = query.map(lambda value: value)
    query = flow([]).concat(source, source) if mode == "same" else query.concat(source)
    assert query.to_list() == [1, 2]
    assert source.calls == 1


@pytest.mark.parametrize("cross", [False, True])
def test_concat_releases_completed_reiterable_cursor(cross: bool) -> None:
    import weakref

    references = []

    class Cursor:
        def __init__(self):
            self.values = iter([1])

        def __iter__(self):
            return self

        def __next__(self):
            return next(self.values)

        def close(self):
            pass

    class First:
        def __iter__(self):
            cursor = Cursor()
            references.append(weakref.ref(cursor))
            return cursor

    class Second:
        def __iter__(self):
            def values():
                assert references[0]() is None
                yield 2

            return values()

    query = flow([]).concat(First())
    query = query.concat(Second()) if cross else flow([]).concat(First(), Second())
    assert query.to_list() == [1, 2]


def test_concat_preserves_source_error_and_close_failure() -> None:
    failure = ValueError("source failed")
    cleanup = OSError("close failed")

    class Source:
        def __iter__(self):
            return self

        def __next__(self):
            raise failure

        def close(self):
            raise cleanup

    with pytest.raises(ValueError) as captured:
        flow([]).concat(Source()).to_list()
    assert captured.value is failure
    assert any("close failed" in note for note in failure.__notes__)

    class CloseFails(Source):
        def __next__(self):
            return 1

    with pytest.raises(OSError) as captured:
        flow([]).concat(CloseFails()).take(1).to_list()
    assert captured.value is cleanup

    class CloseOnExhaustion(Source):
        def __init__(self):
            self.values = iter([1])
            self.calls = 0

        def __next__(self):
            return next(self.values)

        def close(self):
            self.calls += 1
            raise cleanup

    source = CloseOnExhaustion()
    with pytest.raises(OSError) as captured:
        flow([]).concat(source).concat(source).to_list()
    assert captured.value is cleanup
    assert source.calls == 1


@pytest.mark.parametrize("operation", ["zip", "zip_longest", "concat", "cross"])
def test_public_multi_source_operations_preserve_pull_and_cleanup_failures(
    operation: str,
) -> None:
    primary = ValueError(f"{operation} pull failed")

    class Source(Iterator[int]):
        def __init__(self, label: str, values: Iterable[int], *, fail: bool) -> None:
            self.label = label
            self.values = iter(values)
            self.fail = fail
            self.close_calls = 0

        def __next__(self) -> int:
            try:
                return next(self.values)
            except StopIteration:
                if self.fail:
                    self.fail = False
                    raise primary from None
                raise

        def close(self) -> None:
            self.close_calls += 1
            raise OSError(f"{self.label} close failed")

    fail_left = operation in {"zip", "zip_longest"}
    left = Source("left", [1], fail=fail_left)
    right = Source("right", [2, 3], fail=not fail_left)
    pipeline = getattr(flow(left), operation)(right)

    with pytest.raises(ValueError) as captured:
        pipeline.to_list()

    assert captured.value is primary
    assert captured.value.__notes__ == [
        "cleanup failed with OSError: right close failed",
        "cleanup failed with OSError: left close failed",
    ]
    assert left.close_calls == right.close_calls == 1


def test_one_shot_zip_source_never_silently_turns_empty() -> None:
    values = flow([1, 2]).zip(iter([3, 4]))

    assert values.to_list() == [(1, 3), (2, 4)]
    with pytest.raises(fpstreams.FlowConsumedError):
        values.to_list()


def test_selector_driven_unique_and_sort_are_concise() -> None:
    records = [
        {"id": 1, "score": 8},
        {"id": 2, "score": 5},
        {"id": 1, "score": 99},
    ]

    assert flow(records).unique_by("id").sort_by("score", reverse=True).pluck("id").to_list() == [
        1,
        2,
    ]


def test_custom_gatherer_emits_zero_or_more_values() -> None:
    def integrate(state: list[int], item: int):
        state.append(item)
        if len(state) < 2:
            return ()
        group = tuple(state)
        state.clear()
        return (group,)

    gatherer = fpstreams.Gatherer(
        initializer=list,
        integrator=integrate,
        finisher=lambda state: (tuple(state),) if state else (),
    )

    assert flow([1, 2, 3]).gather(gatherer).to_list() == [(1, 2), (3,)]


def test_stream_ex_inspired_operations_avoid_manual_bookkeeping() -> None:
    collapsed = (
        flow([1, 1, 2, 3, 3])
        .collapse(lambda left, right: left == right, lambda left, right: left + right)
        .prepend(0)
        .append(9)
        .to_list()
    )
    ends = (
        flow([1, 2, 3])
        .map_first(lambda value: value * 10)
        .map_last(lambda value: value * 100)
        .to_list()
    )

    assert collapsed == [0, 2, 2, 6, 9]
    assert ends == [10, 2, 300]


def test_collapse_collectors_match_independent_adjacent_runs_and_replay():
    from itertools import pairwise
    from random import Random

    random = Random(9267)
    time = 0
    data = []
    for index in range(1003):
        time += random.choice((0, 1, 2, 7))
        data.append({"at": time, "value": None if index % 7 == 0 else index % 11})
    boundaries = (
        [0]
        + [i for i in range(1, len(data)) if data[i]["at"] - data[i - 1]["at"] > 2]
        + [len(data)]
    )
    groups = [data[start:end] for start, end in pairwise(boundaries)]

    def adjacent(left, right):
        return right["at"] - left["at"] <= 2

    collector = fpstreams.Collectors.teeing(
        fpstreams.Collectors.filtering(
            lambda row: row["value"] is not None,
            fpstreams.Collectors.mapping("value", fpstreams.Collectors.summing()),
        ),
        fpstreams.Collectors.counting(),
        lambda total, count: {"total": total, "count": count},
    )
    expected = [
        {
            "total": sum(row["value"] for row in group if row["value"] is not None),
            "count": len(group),
        }
        for group in groups
    ]
    for engine in ("auto", "python"):
        source = flow(data).with_engine(engine)
        query = source.collapse(adjacent, merger=collector)
        assert query.to_list() == query.to_list() == expected
        lists = source.collapse(adjacent, fpstreams.Collectors.to_list()).to_list()
        assert lists == groups and lists[0][0] is data[0] and lists[0] is not lists[1]
        assert source.collapse(adjacent, fpstreams.Collectors.head(0)).to_list() == [
            [] for _ in groups
        ]
        nothing = fpstreams.Collector(lambda: None, lambda state, value: None)
        assert source.collapse(adjacent, nothing).to_list() == [None] * len(groups)
    assert flow([None, None, 0, 0, None]).collapse(
        lambda a, b: a == b, fpstreams.Collectors.first()
    ).to_list() == [None, 0, None]


def test_collapse_collector_done_preserves_boundaries_and_defers_next_run():
    from itertools import count

    events = []

    def source():
        events.append("open")
        try:
            for index in count():
                value = index // 3 * 10 + index % 3 + 1
                events.append(("pull", value))
                yield value
        finally:
            events.append("close")

    def initialize():
        events.append("initialize")
        return []

    def step(state, value):
        events.append(("step", value))
        state.append(value)
        return state

    def done(state):
        events.append(("done", len(state)))
        return len(state) == 1

    def finish(state):
        events.append(("finish", state[0]))
        return tuple(state)

    def adjacent(left, right):
        events.append(("predicate", left, right))
        return left // 10 == right // 10

    collector = fpstreams.Collector(initialize, step, finish, done=done)
    query = flow.defer(source).collapse(adjacent, collector)
    assert events == []
    assert query.take(2).to_list() == [(1,), (11,)]
    assert events == [
        "open",
        ("pull", 1),
        "initialize",
        ("done", 0),
        ("step", 1),
        ("done", 1),
        ("pull", 2),
        ("predicate", 1, 2),
        ("pull", 3),
        ("predicate", 2, 3),
        ("pull", 11),
        ("predicate", 3, 11),
        ("finish", 1),
        "initialize",
        ("done", 0),
        ("step", 11),
        ("done", 1),
        ("pull", 12),
        ("predicate", 11, 12),
        ("pull", 13),
        ("predicate", 12, 13),
        ("pull", 21),
        ("predicate", 13, 21),
        ("finish", 11),
        "close",
    ]
    events.clear()
    assert query.take(0).to_list() == [] and events == []
    assert flow([]).collapse(adjacent, collector).to_list() == [] and events == []


@pytest.mark.parametrize(
    "hook", ("initialize", "initial_done", "step", "done", "predicate", "finish", "source")
)
def test_collapse_collector_failure_stops_hooks_and_preserves_cleanup_notes(hook):
    failure = LookupError(hook)
    cleanup = RuntimeError("collapse source cleanup failed")
    emitted, finished = [], []

    def fail(name):
        if hook == name:
            raise failure

    class Source:
        def __init__(self):
            self.values = iter((1, 2, 10, 11))
            self.closes = 0

        def __iter__(self):
            return self

        def __next__(self):
            try:
                return next(self.values)
            except StopIteration:
                fail("source")
                raise

        def close(self):
            self.closes += 1
            raise cleanup

    def initialize():
        fail("initialize")
        return []

    def step(state, value):
        if value == 2:
            fail("step")
        state.append(value)
        return state

    def done(state):
        fail({0: "initial_done", 2: "done"}.get(len(state)))
        return False

    def finish(state):
        finished.append(tuple(state))
        fail("finish")
        return tuple(state)

    def adjacent(left, right):
        if right == 10:
            fail("predicate")
        return left // 10 == right // 10

    source = Source()
    collector = fpstreams.Collector(initialize, step, finish, done=done)
    with pytest.raises(LookupError) as caught:
        flow.defer(lambda source=source: source).collapse(adjacent, collector).tap(
            emitted.append
        ).to_list()
    assert caught.value is failure and source.closes == 1
    assert any("collapse source cleanup failed" in note for note in failure.__notes__)
    assert emitted == ([(1, 2)] if hook == "source" else [])
    assert finished == ([(1, 2)] if hook in ("finish", "source") else [])


def test_explain_preserves_serialization_order() -> None:
    payload = flow(range(3)).map(str).filter(bool).take(1).with_engine("python").explain().to_dict()

    assert tuple(payload) == (
        "schema_version",
        "kind",
        "mode",
        "terminal",
        "source",
        "requested_engine",
        "selected_engine",
        "streaming_engine",
        "materializing_engine",
        "selection_reason",
        "data_movement",
        "complexity",
        "operations",
        "stages",
        "semantics",
        "diagnostics",
        "arrow_prefix",
        "numpy_prefix",
        "boundaries",
    )
    assert json.dumps(
        {
            "data_movement": payload["data_movement"],
            "operations": payload["operations"],
            "stages": payload["stages"],
            "arrow_prefix": payload["arrow_prefix"],
            "numpy_prefix": payload["numpy_prefix"],
            "boundaries": payload["boundaries"],
        },
        separators=(",", ":"),
    ) == (
        '{"data_movement":{"scans_source":false,"copies_source":false,'
        '"materializes":false},"operations":[{"name":"map"},{"name":"filter"},'
        '{"name":"take"}],"stages":[{"engine":"python","operations":["map",'
        '"filter"],"fused":true},{"engine":"python","operations":["take"],'
        '"fused":false}],"arrow_prefix":null,"numpy_prefix":null,"boundaries":[]}'
    )


def _semantic_output(pipeline: fpstreams.Flow[object]) -> dict[str, object]:
    return pipeline.explain().to_dict()["semantics"]["output"]


@pytest.mark.parametrize(
    ("pipeline", "termination", "cardinality", "value"),
    [
        (flow([]).filter(bool), "proven_finite", "exact", 0),
        (flow([]).flat_map(lambda value: [value]), "proven_finite", "exact", 0),
        (flow([1, 2, 3]).take(0), "proven_finite", "exact", 0),
        (flow([1, 2, 3]).drop(1), "proven_finite", "exact", 2),
        (flow([1, 2, 3]).filter(bool).drop(1), "proven_finite", "upper_bound", 2),
        (flow([1, 2, 3]).filter(bool).pairwise(), "proven_finite", "upper_bound", 2),
        (flow([1, 2, 3]).filter(bool).chunk(2), "proven_finite", "upper_bound", 2),
        (flow(range(5)).chunk(2), "proven_finite", "exact", 3),
        (flow(range(4)).window(3, step=2), "proven_finite", "exact", 1),
        (flow([1]).window(3), "proven_finite", "exact", 1),
        (flow([]).window(3), "proven_finite", "exact", 0),
        (flow(iter([1, 2])).window(2), "unknown", "unknown", None),
        (flow([1]).concat([2, 3]), "proven_finite", "exact", 3),
        (flow.iterate(0, lambda value: value + 1).concat([1]), "proven_infinite", "unknown", None),
        (flow(iter([1])).concat([2]), "unknown", "unknown", None),
        (flow([1, 2, 3]).zip([4, 5]), "proven_finite", "exact", 2),
        (
            flow([1, 2, 3]).filter(bool).zip([4, 5]),
            "proven_finite",
            "upper_bound",
            2,
        ),
        (
            flow.iterate(0, lambda value: value + 1).zip([4, 5]),
            "proven_finite",
            "unknown",
            None,
        ),
        (flow([1]).zip_longest([2, 3]), "proven_finite", "unknown", None),
        (
            flow.iterate(0, lambda value: value + 1).zip_longest([1]),
            "proven_infinite",
            "unknown",
            None,
        ),
        (flow([]).cross(iter([1])), "proven_finite", "exact", 0),
        (flow([1, 2]).cross(range(3)), "proven_finite", "exact", 6),
        (flow([1, 2]).cross(iter([3])), "unknown", "unknown", None),
    ],
)
def test_explain_propagates_cardinality_and_termination(
    pipeline: fpstreams.Flow[object],
    termination: str,
    cardinality: str,
    value: int | None,
) -> None:
    output = _semantic_output(pipeline)

    assert output["termination"] == termination
    assert output["cardinality"] == {"kind": cardinality, "value": value}


def test_attempt_turns_exceptions_into_composable_values() -> None:
    results = flow([2, 0]).attempt(lambda value: 10 // value).to_list()

    assert results[0] == fpstreams.Ok(5)
    assert results[0].map(lambda value: value * 2) == fpstreams.Ok(10)
    assert isinstance(results[1], fpstreams.Err)
    assert isinstance(results[1].error, ZeroDivisionError)


def test_stream_is_a_thin_v2_compatibility_alias() -> None:
    assert fpstreams.Stream is fpstreams.Flow
    assert fpstreams.ParallelStream is fpstreams.Flow
    assert fpstreams.Stream([1, 1, 2, 3]).distinct().skip(1).limit(2).to_list() == [2, 3]
    assert fpstreams.ParallelStream.of(1, 2, 3).map(lambda value: value + 1).to_list() == [
        2,
        3,
        4,
    ]


def test_downstream_rejection_is_monotonic() -> None:
    received: list[int] = []

    def accept_one(value: int) -> bool:
        received.append(value)
        return False

    downstream = Downstream(accept_one)

    assert downstream.is_rejecting() is False
    assert downstream.push(1) is False
    assert downstream.is_rejecting() is True
    assert downstream.push(2) is False
    assert received == [1]


def test_push_gatherer_supports_state_and_finisher() -> None:
    def integrate(state: list[int], item: int, downstream: Downstream[int]) -> bool:
        state.append(item)
        return downstream.push(item * 2)

    def finish(state: list[int], downstream: Downstream[int]) -> None:
        downstream.push(sum(state))

    gatherer = Gatherer.of_sequential(list, integrate, finisher=finish)

    assert flow([1, 2, 3]).gather(gatherer).to_list() == [2, 4, 6, 6]


def test_stateless_push_gatherer_factory() -> None:
    gatherer = Gatherer.of_sequential(lambda _state, item, downstream: downstream.push(item + 1))

    assert flow([1, 2]).gather(gatherer).to_list() == [2, 3]


def test_integrator_short_circuits_source_and_still_finishes() -> None:
    events: list[object] = []

    def source() -> Iterator[int]:
        try:
            for item in range(1, 10):
                events.append(("source", item))
                yield item
        finally:
            events.append("closed")

    def integrate(state: list[int], item: int, downstream: Downstream[int]) -> bool:
        state.append(item)
        downstream.push(item)
        return item < 3

    def finish(state: list[int], downstream: Downstream[Any]) -> None:
        events.append(("finish", tuple(state), downstream.is_rejecting()))
        downstream.push(sum(state))

    gatherer = Gatherer.of_sequential(list, integrate, finisher=finish)

    assert flow(source()).gather(gatherer).to_list() == [1, 2, 3, 6]
    assert events == [
        ("source", 1),
        ("source", 2),
        ("source", 3),
        ("finish", (1, 2, 3), False),
        "closed",
    ]


def test_and_then_feeds_left_finisher_before_right_finisher() -> None:
    def group(state: list[int], item: int, downstream: Downstream[tuple[int, ...]]) -> bool:
        state.append(item)
        if len(state) < 2:
            return True
        batch = tuple(state)
        state.clear()
        return downstream.push(batch)

    def finish_group(state: list[int], downstream: Downstream[tuple[int, ...]]) -> None:
        if state:
            downstream.push(tuple(state))

    def total(
        state: list[tuple[int, ...]],
        item: tuple[int, ...],
        downstream: Downstream[int],
    ) -> bool:
        state.append(item)
        return downstream.push(sum(item))

    def finish_total(state: list[tuple[int, ...]], downstream: Downstream[int]) -> None:
        downstream.push(len(state))

    left = Gatherer.of_sequential(list, group, finisher=finish_group)
    right = Gatherer.of_sequential(list, total, finisher=finish_total)

    assert flow([1, 2, 3, 4, 5]).gather(left.and_then(right)).to_list() == [3, 7, 5, 3]


def test_and_then_propagates_right_rejection_to_source_and_left_finisher() -> None:
    events: list[object] = []

    def source() -> Iterator[int]:
        try:
            for item in range(1, 10):
                events.append(("source", item))
                yield item
        finally:
            events.append("closed")

    def pass_through(
        state: None,
        item: int,
        downstream: Downstream[int],
    ) -> bool:
        return downstream.push(item)

    def finish_left(state: None, downstream: Downstream[int]) -> None:
        events.append(("left_finish_rejecting", downstream.is_rejecting()))

    def take_two(state: list[int], item: int, downstream: Downstream[int]) -> bool:
        state.append(item)
        downstream.push(item * 10)
        return len(state) < 2

    def finish_right(state: list[int], downstream: Downstream[int]) -> None:
        events.append(("right_finish", tuple(state), downstream.is_rejecting()))

    left = Gatherer.of_sequential(pass_through, finisher=finish_left)
    right = Gatherer.of_sequential(list, take_two, finisher=finish_right)

    assert flow(source()).gather(left.and_then(right)).to_list() == [10, 20]
    assert events == [
        ("source", 1),
        ("source", 2),
        ("left_finish_rejecting", True),
        ("right_finish", (1, 2), False),
        "closed",
    ]


def test_adjacent_gatherers_are_fused_with_composed_finisher_order() -> None:
    events: list[object] = []

    def pass_through(
        state: None,
        item: int,
        downstream: Downstream[int],
    ) -> bool:
        return downstream.push(item)

    def finish_left(state: None, downstream: Downstream[int]) -> None:
        events.append(("left", downstream.is_rejecting()))

    def stop(state: list[int], item: int, downstream: Downstream[int]) -> bool:
        state.append(item)
        downstream.push(item)
        return False

    def finish_right(state: list[int], downstream: Downstream[int]) -> None:
        events.append(("right", tuple(state), downstream.is_rejecting()))

    left = Gatherer.of_sequential(pass_through, finisher=finish_left)
    right = Gatherer.of_sequential(list, stop, finisher=finish_right)

    assert flow([1, 2, 3]).gather(left).gather(right).to_list() == [1]
    assert events == [("left", True), ("right", (1,), False)]


def test_downstream_cancellation_runs_finisher_in_rejecting_mode() -> None:
    events: list[object] = []

    def source() -> Iterator[int]:
        try:
            yield from range(10)
        finally:
            events.append("closed")

    def integrate(state: list[int], item: int, downstream: Downstream[int]) -> bool:
        state.append(item)
        return downstream.push(item)

    def finish(state: list[int], downstream: Downstream[int]) -> None:
        events.append(("finish", tuple(state), downstream.is_rejecting()))
        assert downstream.push(99) is False

    gatherer = Gatherer.of_sequential(list, integrate, finisher=finish)

    assert flow(source()).gather(gatherer).take(1).to_list() == [0]
    assert events == [("finish", (0,), True), "closed"]


def test_push_contract_rejects_non_boolean_results() -> None:
    downstream: Downstream[int] = Downstream(lambda _value: None)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="push callback must return a bool"):
        downstream.push(1)

    gatherer = Gatherer.of_sequential(
        lambda _state, _item, _downstream: None  # type: ignore[arg-type,return-value]
    )
    with pytest.raises(TypeError, match="integrator must return a bool"):
        flow([1]).gather(gatherer).to_list()


def test_take_while_inclusive_emits_the_boundary_then_closes() -> None:
    pulls: list[int] = []
    closed = False

    def values() -> Iterator[int]:
        nonlocal closed
        try:
            for value in (1, 2, 3, 4):
                pulls.append(value)
                yield value
        finally:
            closed = True

    pipeline = fpstreams.flow.defer(values).take_while_inclusive(lambda value: value < 3)

    assert pipeline.to_list() == [1, 2, 3]
    assert pulls == [1, 2, 3]
    assert closed
    assert pipeline.explain().to_dict()["operations"] == [{"name": "take_while_inclusive"}]


def test_find_index_short_circuits_and_index_of_uses_none_when_missing() -> None:
    pulls: list[int] = []
    closed = False

    def values() -> Iterator[int]:
        nonlocal closed
        try:
            for value in (10, 20, 30, 40):
                pulls.append(value)
                yield value
        finally:
            closed = True

    assert fpstreams.flow.defer(values).find_index(lambda value: value == 30) == 2
    assert pulls == [10, 20, 30]
    assert closed
    assert fpstreams.flow([10, 20, 30]).index_of(20) == 1
    assert fpstreams.flow([10, 20, 30]).index_of(99) is None


def test_cross_opens_and_caches_the_right_side_only_when_needed() -> None:
    events: list[str] = []

    def left() -> Iterator[int]:
        try:
            for value in (1, 2, 3):
                events.append(f"left:{value}")
                yield value
        finally:
            events.append("left:closed")

    def right() -> Iterator[str]:
        try:
            for value in ("a", "b"):
                events.append(f"right:{value}")
                yield value
        finally:
            events.append("right:closed")

    pipeline = fpstreams.flow.defer(left).cross(right())
    assert events == []
    assert pipeline.take(3).to_list() == [(1, "a"), (1, "b"), (2, "a")]
    assert events == [
        "left:1",
        "right:a",
        "right:b",
        "right:closed",
        "left:2",
        "left:closed",
    ]

    unopened: list[str] = []

    def unused_right() -> Iterator[int]:
        unopened.append("opened")
        yield 1

    assert fpstreams.flow([]).cartesian(unused_right()).to_list() == []
    assert unopened == []


def test_cross_enforces_the_explicit_right_cache_limit() -> None:
    with pytest.raises(fpstreams.BufferLimitError, match="max_right=2"):
        fpstreams.flow([1]).cross(range(3), max_right=2).to_list()


def test_scan_right_and_reduce_right_use_right_associative_order() -> None:
    assert fpstreams.flow([1, 2, 3]).scan_right(
        0, lambda value, total: value + total
    ).to_list() == [6, 5, 3]
    assert fpstreams.flow([1, 2, 3]).reduce_right(lambda left, right: left - right) == 2
    assert fpstreams.flow([]).reduce_right(lambda value, total: value + total, 10) == 10
    with pytest.raises(fpstreams.EmptyFlowError):
        fpstreams.flow([]).reduce_right(lambda left, right: left + right)


def test_right_operations_are_lazy_bounded_and_close_on_limit_errors() -> None:
    events: list[str] = []

    def values() -> Iterator[int]:
        try:
            for value in (1, 2, 3):
                events.append(f"pull:{value}")
                yield value
        finally:
            events.append("closed")

    pipeline = fpstreams.flow.defer(values).scan_right(
        0,
        lambda value, total: value + total,
        max_items=2,
    )
    assert events == []
    with pytest.raises(fpstreams.BufferLimitError, match="max_items=2"):
        pipeline.to_list()
    assert events == ["pull:1", "pull:2", "pull:3", "closed"]


@pytest.mark.asyncio
async def test_async_inclusive_take_and_index_search_short_circuit() -> None:
    events: list[str] = []

    async def values() -> AsyncIterator[int]:
        try:
            for value in (1, 2, 3, 4):
                events.append(f"pull:{value}")
                yield value
        finally:
            events.append("closed")

    async def before_three(value: int) -> bool:
        return value < 3

    assert await fpstreams.aflow(values()).take_while_inclusive(before_three).to_list() == [1, 2, 3]
    assert events == ["pull:1", "pull:2", "pull:3", "closed"]
    assert await fpstreams.aflow([10, 20, 30]).find_index(lambda value: value == 20) == 1
    assert await fpstreams.aflow([10, 20, 30]).index_of(99) is None


@pytest.mark.asyncio
async def test_async_cross_and_right_reductions_match_sync_semantics() -> None:
    async def right() -> AsyncIterator[str]:
        for value in ("a", "b"):
            yield value

    assert await fpstreams.aflow([1, 2]).cross(right()).to_list() == [
        (1, "a"),
        (1, "b"),
        (2, "a"),
        (2, "b"),
    ]

    async def add(value: int, total: int) -> int:
        return value + total

    assert await fpstreams.aflow([1, 2, 3]).scan_right(0, add).to_list() == [6, 5, 3]
    assert await fpstreams.aflow([1, 2, 3]).reduce_right(add, 0) == 6
    with pytest.raises(fpstreams.BufferLimitError, match="max_right=1"):
        await fpstreams.aflow([1]).cross(["a", "b"], max_right=1).to_list()


def test_native_inclusive_take_fuses_with_downstream_i64_operations() -> None:
    pipeline = (
        fpstreams.flow(range(100))
        .map(fpstreams.item * 2)
        .take_while_inclusive(fpstreams.item < 6)
        .filter(fpstreams.item % 4 == 0)
        .with_engine("native")
    )

    assert pipeline.to_list() == [0, 4]
    assert pipeline.count() == 2


def test_native_inclusive_take_fuses_in_f64_pipelines() -> None:
    result = (
        fpstreams.flow(range(10))
        .map(fpstreams.fitem / 2)
        .take_while_inclusive(fpstreams.fitem < 1.0)
        .map(fpstreams.fitem * 2)
        .with_engine("native")
        .to_list()
    )

    assert result == [0.0, 1.0, 2.0]


@pytest.mark.parametrize("engine", ["python", "auto"])
@pytest.mark.parametrize("keyed", [False, True])
def test_unique_propagates_equality_type_error_without_rehash(engine, keyed):
    failure = TypeError("equality failed")
    hashes = []
    closed = []

    class Key:
        def __hash__(self):
            hashes.append(self)
            return 0

        def __eq__(self, other):
            raise failure

    first, second = Key(), Key()

    def source():
        try:
            yield first
            yield second
        finally:
            closed.append(True)

    query = flow(source()).with_engine(engine)
    with pytest.raises(TypeError) as caught:
        (query.unique_by(lambda x: x) if keyed else query.unique()).to_list()
    assert caught.value is failure
    assert len(hashes) == 3
    assert hashes[0] is hashes[1] is first and hashes[2] is second
    assert closed == [True]


def test_unique_native_suffix_propagates_equality_type_error():
    from fpstreams.streams._terminals.unique import _seeded_unique_suffix

    failure = TypeError("suffix equality failed")

    class Key:
        def __hash__(self):
            return 1

        def __eq__(self, other):
            raise failure

    with pytest.raises(TypeError) as caught:
        list(_seeded_unique_suffix(iter([Key()]), {1}, [1]))
    assert caught.value is failure


def test_unique_hash_protocol_and_unhashable_fallback_are_preserved():
    calls = []

    class Key:
        def __hash__(self):
            calls.append("hash")
            return 0

        def __eq__(self, other):
            calls.append("eq")
            return True

    first, second = Key(), Key()
    result = flow([first, second]).with_engine("python").unique().to_list()
    assert len(result) == 1 and result[0] is first
    assert calls == ["hash", "hash", "hash", "eq"]
    assert flow([([1],), ([1],), ([2],)]).unique().to_list() == [([1],), ([2],)]


def test_unique_pair_keys_does_not_swallow_equality_type_error():
    failure = TypeError("pair key equality")

    class Key:
        def __hash__(self):
            return 0

        def __eq__(self, other):
            raise failure

    with pytest.raises(TypeError) as caught:
        list(flow([(Key(), 1), (Key(), 2)]).pairs().unique_keys())
    assert caught.value is failure


@pytest.mark.parametrize("values", [[0, "key"], ["key", 0]])
def test_unique_integer_and_custom_collision_preserves_comparison_operands(values):
    events = []

    class Key:
        def __hash__(self):
            return 0

        def __eq__(self, other):
            events.append(other)
            return False

    key = Key()
    data = [key if value == "key" else value for value in values]
    expected = set()
    for value in data:
        if value not in expected:
            expected.add(value)
    baseline = list(events)
    events.clear()
    result = flow(data).with_engine("python").unique().to_list()
    assert len(result) == 2
    assert events == baseline
    assert all(value is original for value, original in zip(result, data, strict=True))


@pytest.mark.parametrize("primary_failure", [False, True])
def test_iterator_cleanup_preserves_nested_failure_notes(primary_failure):
    from fpstreams.runtime.iterators import closing_iterators

    primary = ValueError("query failed")
    first = OSError("first close failed")
    nested = OSError("nested close failed")
    nested.add_note("another owned resource also failed")
    events = []

    class Resource:
        def __init__(self, error):
            self.error = error

        def close(self):
            events.append(self.error)
            raise self.error

    with (
        pytest.raises(ValueError if primary_failure else OSError) as caught,
        closing_iterators((Resource(first), Resource(nested))),
    ):
        if primary_failure:
            raise primary
    assert caught.value is (primary if primary_failure else first)
    assert events == [first, nested]
    assert "another owned resource also failed" in caught.value.__notes__


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(
    ("operation", "parameter", "options"),
    [
        ("chunk", "size", {}),
        ("batch", "size", {}),
        ("window", "size", {}),
        ("window", "step", {"size": 2}),
        ("batch_by_size", "max_size", {}),
        ("batch_by_size", "max_count", {"max_size": 4}),
        ("constrained_batches", "max_size", {}),
    ],
)
@pytest.mark.parametrize("invalid", [1.5, 2.0, float("nan"), float("inf"), "2", None])
def test_batch_bounds_reject_non_integers_before_opening_source(
    asynchronous, operation, parameter, options, invalid
):
    events = []

    def source():
        events.append("opened")
        yield b"a"

    query = fpstreams.aflow(source()) if asynchronous else flow(source())
    if parameter == "max_count" and invalid is None:
        assert getattr(query, operation)(**options, max_count=None) is not query
    else:
        with pytest.raises(TypeError):
            getattr(query, operation)(**options, **{parameter: invalid})
    assert events == []


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(
    "operation", ["chunk", "batch", "window", "batch_by_size", "constrained_batches"]
)
async def test_batch_bounds_normalize_index_once(asynchronous, operation):
    events = []

    class Bound:
        def __index__(self):
            events.append("index")
            return 2

    def source():
        events.append("opened")
        yield from (b"a", b"b", b"c")

    query = fpstreams.aflow(source()) if asynchronous else flow(source())
    query = getattr(query, operation)(Bound())
    assert events == ["index"]
    actual = await query.to_list() if asynchronous else query.to_list()
    expected = [(b"a", b"b"), (b"b", b"c")] if operation == "window" else [(b"a", b"b"), (b"c",)]
    assert actual == expected
    assert events == ["index", "opened"]


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("parameter", ["size", "step", "max_size", "max_count"])
@pytest.mark.parametrize("bound", [0, -1])
def test_batch_bounds_reject_nonpositive_integers(asynchronous, parameter, bound):
    query = fpstreams.aflow([]) if asynchronous else flow([])
    with pytest.raises(ValueError):
        if parameter == "size":
            query.chunk(bound)
        elif parameter == "step":
            query.window(2, step=bound)
        elif parameter == "max_size":
            query.batch_by_size(bound)
        else:
            query.batch_by_size(4, max_count=bound)


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("parameter", ["step", "max_count"])
async def test_batch_secondary_bounds_normalize_index_once(asynchronous, parameter):
    calls = []

    class Bound:
        def __index__(self):
            calls.append("index")
            return 2

    query = fpstreams.aflow([b"a", b"b", b"c"]) if asynchronous else flow([b"a", b"b", b"c"])
    if parameter == "step":
        query = query.window(2, step=Bound())
        expected = [(b"a", b"b")]
    else:
        query = query.batch_by_size(10, max_count=Bound())
        expected = [(b"a", b"b"), (b"c",)]
    assert calls == ["index"]
    actual = await query.to_list() if asynchronous else query.to_list()
    assert actual == expected
    assert calls == ["index"]


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("operation", ["chunk", "window", "batch_by_size"])
def test_batch_bounds_preserve_index_exception(asynchronous, operation):
    failure = TypeError("user index failed")

    class Bound:
        def __index__(self):
            raise failure

    query = fpstreams.aflow([]) if asynchronous else flow([])
    with pytest.raises(TypeError) as caught:
        getattr(query, operation)(Bound())
    assert caught.value is failure


@pytest.mark.parametrize("engine", ["python", "auto"])
def test_merge_sorted_stable_contract(engine) -> None:
    from fpstreams import NativeUnsupportedError, flow
    from fpstreams.planning.logical import MergeSortedNode, walk_logical

    left = [{"k": 1, "v": "a"}, {"k": 1, "v": "b"}, {"k": 3, "v": "c"}]
    right = [{"k": 1, "v": "d"}, {"k": 2, "v": "e"}]
    merged = flow(left).with_engine(engine).merge_sorted(flow(right), key="k")
    assert isinstance(merged._logical_plan.root, MergeSortedNode)
    assert len(walk_logical(merged._logical_plan.root)) == 3
    assert merged.explain().to_dict()["relations"]["node"] == "merge_sorted"
    result = merged.to_list()
    assert [row["v"] for row in result] == ["a", "b", "d", "e", "c"]
    assert all(
        actual is expected
        for actual, expected in zip(
            result, [left[0], left[1], right[0], right[1], left[2]], strict=True
        )
    )
    assert flow([]).merge_sorted([]).to_list() == []
    assert flow([]).merge_sorted([1, 2]).to_list() == [1, 2]
    assert flow([1, 2]).merge_sorted([]).to_list() == [1, 2]
    assert flow([1, 2]).merge_sorted([1, 2]).sum() == 6
    same_list = flow([1, 2])
    assert same_list.merge_sorted(same_list).to_list() == [1, 1, 2, 2]
    with pytest.raises(ValueError, match=r"right.*index 2"):
        flow([1]).merge_sorted([1, 3, 2]).to_list()
    with pytest.raises(TypeError):
        flow([1]).merge_sorted(["a"]).to_list()
    with pytest.raises(ValueError, match="conflicting"):
        flow([1]).with_engine("python").merge_sorted(flow([2]).with_engine("native"))
    with pytest.raises(NativeUnsupportedError):
        flow([1]).merge_sorted([2]).with_engine("native").to_list()

    pulled = []
    closed = []

    class Shared:
        def __iter__(self):
            return self

        def __next__(self):
            pulled.append(True)
            return 1

        def close(self):
            closed.append(True)

    shared = Shared()
    with pytest.raises(ValueError, match="same iterator"):
        flow(shared).merge_sorted(flow(shared)).to_list()
    assert pulled == []
    assert closed == [True]
    pulled.clear()
    closed.clear()
    same_flow = flow(Shared())
    with pytest.raises(ValueError, match="same one-shot"):
        same_flow.merge_sorted(same_flow).to_list()
    assert pulled == []
    assert closed == [True]


def test_merge_sorted_composition_contract() -> None:
    from fpstreams import flow

    a = [(1, "a1"), (1, "a2"), (4, "a4")]
    b = [(1, "b1"), (3, "b3")]
    c = [(1, "c1"), (2, "c2")]
    result = flow(a).merge_sorted(b, key=0).merge_sorted(c, key=0).to_list()
    assert result == [a[0], a[1], b[0], c[0], c[1], b[1], a[2]]
    assert flow([1, 3]).map(lambda v: v * 2).merge_sorted([1, 5]).map(
        lambda v: v + 1
    ).to_list() == [2, 3, 6, 7]
    assert flow([{"k": 1}, {"k": 3}]).select("k").to_flow().merge_sorted(
        [{"k": 2}], key="k"
    ).to_list() == [{"k": 1}, {"k": 2}, {"k": 3}]
    events = []

    def source(side, start):
        events.append(("open", side))
        try:
            index = start
            while True:
                events.append(("pull", side, index))
                yield index
                index += 2
        finally:
            events.append(("close", side))

    merged = flow.defer(lambda: source("left", 0)).merge_sorted(
        flow.defer(lambda: source("right", 1))
    )
    assert events == []
    assert merged.take(0).to_list() == []
    assert events == []
    assert merged.take(1).to_list() == [0]
    assert events == [
        ("open", "left"),
        ("pull", "left", 0),
        ("open", "right"),
        ("pull", "right", 1),
        ("close", "left"),
        ("close", "right"),
    ]

    calls = []
    error = RuntimeError("right open failed")

    def fail():
        calls.append("right")
        raise error

    class Left:
        def __iter__(self):
            return self

        def __next__(self):
            calls.append("pull")
            return 1

        def close(self):
            calls.append("close")

    def left():
        calls.append("left")
        return Left()

    with pytest.raises(RuntimeError) as caught:
        flow.defer(left).merge_sorted(flow.defer(fail)).to_list()
    assert caught.value is error
    assert calls == ["left", "right", "close"]

    pulled = []

    def shared_values():
        for value in [1, 2]:
            pulled.append(value)
            yield value

    shared = flow(shared_values())
    with pytest.raises(ValueError, match="same one-shot"):
        shared.map(lambda v: v).merge_sorted(shared.map(lambda v: v)).to_list()
    assert pulled == []
    assert shared.to_list() == [1, 2]


@pytest.mark.parametrize("kind", ["csv", "json", "jsonl"])
def test_atomic_output_contract(tmp_path, kind) -> None:
    import io

    target = tmp_path / f"output.{kind}"
    reference = tmp_path / f"reference.{kind}"
    method = f"to_{kind}"
    data = [{"id": 1, "name": "中文"}, {"id": 2, "name": "a"}]
    assert getattr(flow(data), method)(reference) is None
    target.write_text("old")
    assert getattr(flow(data), method)(target, atomic=True) is None
    assert target.read_bytes() == reference.read_bytes()
    failure = RuntimeError("source failed")

    def bad():
        yield data[0]
        raise failure

    with pytest.raises(RuntimeError) as caught:
        getattr(flow(bad()), method)(target, atomic=True)
    assert caught.value is failure
    assert target.read_bytes() == reference.read_bytes()
    with pytest.raises(FileExistsError):
        getattr(flow(data), method)(target, atomic=True, if_exists="error")
    with pytest.raises(ValueError):
        getattr(flow(data), method)(target, if_exists="error")
    with pytest.raises(TypeError):
        getattr(flow(data), method)(io.StringIO(), atomic=True)
    assert sorted(path.name for path in tmp_path.iterdir()) == [target.name, reference.name]


def test_json_atomic_serialization_failure(tmp_path) -> None:
    target = tmp_path / "output.json"
    target.write_text("old")
    with pytest.raises(TypeError):
        flow([1, object()]).to_json(target, atomic=True)
    assert target.read_text() == "old"
    assert list(tmp_path.iterdir()) == [target]


@pytest.mark.parametrize("kind", ["csv", "json", "jsonl"])
def test_atomic_sink_source_cleanup_before_publication(tmp_path, kind) -> None:
    target = tmp_path / "output"
    target.write_text("old")
    error = RuntimeError("source close failed")

    def source():
        try:
            yield {"id": 1}
        finally:
            assert target.read_text() == "old"
            raise error

    with pytest.raises(RuntimeError) as caught:
        getattr(flow(source()), f"to_{kind}")(target, atomic=True)
    assert caught.value is error
    assert target.read_text() == "old"
    assert list(tmp_path.iterdir()) == [target]


@pytest.mark.parametrize("format_name", ["csv", "json"])
@pytest.mark.parametrize("if_exists", ["replace", "error"])
def test_atomic_output_keeps_relative_destination_when_source_changes_cwd(
    tmp_path, monkeypatch, format_name, if_exists
) -> None:
    initial = tmp_path / "initial"
    unrelated = tmp_path / "unrelated"
    initial.mkdir()
    unrelated.mkdir()
    name = f"result.{format_name}"
    (unrelated / name).write_text("unrelated content")
    monkeypatch.chdir(initial)

    def records():
        monkeypatch.chdir(unrelated)
        yield {"value": 7}

    getattr(flow(records()), f"to_{format_name}")(name, atomic=True, if_exists=if_exists)
    assert (initial / name).read_text() == ("7\n" if format_name == "csv" else '[{"value": 7}]')
    assert (unrelated / name).read_text() == "unrelated content"
    assert list(initial.iterdir()) == [initial / name]
    assert list(unrelated.iterdir()) == [unrelated / name]


@pytest.mark.parametrize("atomic", [False, True])
def test_flow_jsonl_streams_values_and_serializes_custom_objects(tmp_path, atomic) -> None:
    from datetime import date

    target = tmp_path / "events.jsonl"
    seen = []
    stamp = date(2026, 9, 30)

    def serialize(value):
        seen.append(value)
        return value.isoformat()

    values = [1, None, "雪\nline", {"date": stamp}, [True, 2]]
    assert flow(iter(values)).to_jsonl(target, default=serialize, atomic=atomic) is None
    lines = target.read_text().splitlines()
    assert len(lines) == len(values)
    assert [json.loads(line) for line in lines] == [
        1,
        None,
        "雪\nline",
        {"date": "2026-09-30"},
        [True, 2],
    ]
    assert seen == [stamp]
    flow([]).to_jsonl(target, atomic=atomic)
    assert target.read_bytes() == b""


def test_flow_jsonl_serialization_error_preserves_target_and_closes_source(tmp_path) -> None:
    target = tmp_path / "events.jsonl"
    target.write_text("old")
    closed = []

    def source():
        try:
            yield 1
            yield object()
        finally:
            closed.append(True)

    with pytest.raises(TypeError):
        flow(source()).to_jsonl(target, atomic=True)
    assert target.read_text() == "old"
    assert closed == [True]
    assert list(tmp_path.iterdir()) == [target]


@pytest.mark.parametrize("safe", [False, True])
def test_flow_csv_safety_includes_headers_without_changing_lookup(tmp_path, safe) -> None:
    import csv

    target = tmp_path / "headers.csv"
    names = ["=1+1", " \t@command", "normal"]
    flow([dict(zip(names, [3, "-text", -2], strict=True))]).to_csv(
        target, header=names, spreadsheet_safe=safe
    )
    with target.open(newline="") as handle:
        assert list(csv.reader(handle)) == [
            ["'=1+1", "' \t@command", "normal"] if safe else names,
            ["3", "'-text" if safe else "-text", "-2"],
        ]


def test_window_reduce_matches_independent_ordered_window_oracle():
    from functools import reduce
    from random import Random

    rng = Random(7493)
    values = [(rng.randrange(1, 17), rng.randrange(31)) for _ in range(137)]

    # Affine-function composition is associative, order-sensitive and fixed-size.
    def combine(left, right):
        a, b = left
        c, d = right
        return a * c % 1009, (a * d + b) % 1009

    for count in (0, 1, 6, 37, 137):
        for size, step in ((1, 1), (7, 1), (7, 3), (7, 7), (7, 11), (19, 5)):
            data = values[:count]
            starts = range(0, max(1, count - size + 1), step) if count else ()
            expected = [reduce(combine, data[start : start + size]) for start in starts]
            for engine in ("auto", "python"):
                query = flow.defer(lambda data=data: iter(data)).with_engine(engine)
                result = query.window_reduce(size, combine, step=step)
                assert result.to_list() == expected
                assert result.to_list() == expected  # Every execution owns fresh queue state.
    assert flow([None] * 12).window_reduce(3, lambda a, b: None).to_list() == [None] * 10


def test_window_reduce_reuses_state_and_short_circuits_without_extra_callbacks():
    from itertools import count

    calls = []
    pulls = []
    closes = []

    def combine(left, right):
        calls.append((left, right))
        return left + right

    def source():
        try:
            for value in count():
                pulls.append(value)
                yield value
        finally:
            closes.append(True)

    assert flow.defer(source).window_reduce(256, combine).take(1000).to_list() == [
        256 * start + 255 * 256 // 2 for start in range(1000)
    ]
    assert len(pulls) == 1255
    assert len(calls) < 3 * len(pulls)  # Recomputing each window needs 255000 calls.
    assert closes == [True]

    calls.clear()
    pulls.clear()
    closes.clear()
    assert flow.defer(source).window_reduce(3, combine).take(1).to_list() == [3]
    assert pulls == [0, 1, 2] and calls == [(0, 1), (1, 2)] and closes == [True]
    assert flow.defer(source).window_reduce(3, combine).take(0).to_list() == []
    assert pulls == [0, 1, 2] and closes == [True]


def test_window_reduce_transfer_failure_preserves_exception_and_closes_source():
    failure = LookupError("aggregate transfer failed")
    cleanup = RuntimeError("source cleanup failed")
    pulls = []
    emitted = []

    def source():
        try:
            for value in (1, 2, 3, 4, 5):
                pulls.append(value)
                yield value
        finally:
            raise cleanup

    def combine(left, right):
        if (left, right) == (2, 3):
            raise failure
        return left + right

    with pytest.raises(LookupError) as caught:
        flow.defer(source).window_reduce(3, combine).tap(emitted.append).to_list()
    assert caught.value is failure
    assert pulls == [1, 2, 3, 4] and emitted == [6]
    assert any("source cleanup failed" in note for note in failure.__notes__)


def test_disjoint_window_reduce_keeps_iterator_identity_eof_and_large_counts():
    import operator

    class Source:
        def __init__(self, values):
            self.values = iter(values)
            self.iterations = self.ends = self.closes = 0

        def __iter__(self):
            self.iterations += 1
            return self

        def __next__(self):
            try:
                return next(self.values)
            except StopIteration:
                self.ends += 1
                if self.ends == 1:
                    raise
                raise AssertionError("pulled after exhaustion") from None

        def close(self):
            self.closes += 1

    huge = sys.maxsize + 17
    # Exercise partial windows, partial gaps, and counts beyond islice's limit.
    for size, step, expected in (
        (3, 5, [3, 18]),
        (3, 11, [3]),
        (1, huge, [0]),
        (huge, huge, [36]),
        (3, huge, [3]),
    ):
        source = Source(range(9))
        assert (
            flow.defer(lambda source=source: source)
            .window_reduce(size, operator.add, step=step)
            .to_list()
            == expected
        )
        assert source.ends == source.closes == 1
        assert source.iterations == 2  # Source opening, then one operation-level iteration.

    left, right = object(), object()
    values = [None, left, right, None, right]
    assert flow(values).window_reduce(2, lambda a, b: a, step=2).to_list() == [None, right]
    assert flow(values).window_reduce(2, lambda a, b: b, step=3).to_list() == [left, right]


def test_disjoint_window_reduce_folds_suppressed_tail_and_preserves_cleanup_failure():
    failure = LookupError("incomplete tail failed")
    cleanup = RuntimeError("window source cleanup failed")
    pulls, calls, emitted = [], [], []

    def source():
        try:
            for value in range(7):
                pulls.append(value)
                yield value
        finally:
            raise cleanup

    def combine(left, right):
        calls.append((left, right))
        if right == 6:
            raise failure
        return left + right

    with pytest.raises(LookupError) as caught:
        flow.defer(source).window_reduce(3, combine, step=5).tap(emitted.append).to_list()
    assert caught.value is failure
    assert pulls == list(range(7)) and emitted == [3]
    assert calls == [(0, 1), (1, 2), (5, 6)]
    assert any("window source cleanup failed" in note for note in failure.__notes__)


@pytest.mark.parametrize("engine", ["python", "auto"])
def test_filter_map_fusion_preserves_order_identity_and_short_circuit(engine):
    events = []
    empty = []

    def source():
        try:
            for value in range(8):
                events.append(("pull", value))
                yield value
        finally:
            events.append(("close",))

    def mapper(value):
        events.append(("map", value))
        return {1: 0, 3: empty}.get(value)

    result = (
        flow(source())
        .with_engine(engine)
        .map(lambda value: value)
        .filter_map(mapper)
        .tap(lambda value: events.append(("tap", value)))
        .take(2)
        .to_list()
    )
    assert result == [0, []] and result[1] is empty
    assert events == [
        ("pull", 0),
        ("map", 0),
        ("pull", 1),
        ("map", 1),
        ("tap", 0),
        ("pull", 2),
        ("map", 2),
        ("pull", 3),
        ("map", 3),
        ("tap", empty),
        ("close",),
    ]


def test_filter_map_fusion_propagates_failure_and_closes_without_replay():
    events = []
    failure = LookupError("filter_map mapper failed")

    def source():
        try:
            for value in range(6):
                events.append(value)
                yield value
        finally:
            events.append("closed")

    def mapper(value):
        if value == 3:
            raise failure
        return value if value % 2 == 0 else None

    emitted = []
    with pytest.raises(LookupError) as caught:
        flow(source()).filter_map(mapper).tap(emitted.append).to_list()
    assert caught.value is failure
    assert events == [0, 1, 2, 3, "closed"]
    assert emitted == [0, 2]


@pytest.mark.parametrize("weak", [False, True])
@pytest.mark.parametrize("mode", ["direct", "chain", "root", "short"])
def test_concat_custom_iterables_share_cursor_once(weak, mode):
    events = []

    class Cursor:
        __slots__ = ("calls", "values")

        def __init__(self):
            self.values = iter([1, 2])
            self.calls = 0

        def __iter__(self):
            return self

        def __next__(self):
            return next(self.values)

        def close(self):
            self.calls += 1
            events.append("closed")
            if self.calls > 1:
                raise RuntimeError("closed twice")

    class WeakCursor(Cursor):
        __slots__ = ("__weakref__",)

    class Source:
        def __init__(self, cursor):
            self.cursor = cursor

        def __iter__(self):
            events.append("opened")
            return self.cursor

    cursor = (WeakCursor if weak else Cursor)()
    source = Source(cursor)
    if mode == "root":
        query = flow(source).concat(source)
    elif mode == "chain":
        query = flow([]).concat(source).concat(source)
    elif mode == "short":
        query = flow([]).concat(source).take(1).concat(source)
    else:
        query = flow([]).concat(source, source)
    assert query.to_list() == [1, 2]
    assert cursor.calls == 1
    assert events.count("opened") == 2
    assert events.count("closed") == 1


def test_concat_custom_distinct_nonweak_cursors_close_before_next_pull():
    events = []

    class Cursor:
        __slots__ = ("done", "name")

        def __init__(self, name):
            self.name, self.done = name, False

        def __iter__(self):
            return self

        def __next__(self):
            if self.done:
                raise StopIteration
            self.done = True
            events.append("pull " + self.name)
            return self.name

        def close(self):
            events.append("close " + self.name)

    class Source:
        def __init__(self, name):
            self.name = name

        def __iter__(self):
            return Cursor(self.name)

    assert flow([]).concat(Source("a"), Source("b")).to_list() == ["a", "b"]
    assert events == ["pull a", "close a", "pull b", "close b"]


def test_concat_nonweak_cursor_is_retained_until_query_finishes():
    events = []

    class Cursor:
        __slots__ = ("done",)

        def __init__(self):
            self.done = False

        def __iter__(self):
            return self

        def __next__(self):
            if self.done:
                raise StopIteration
            self.done = True
            return 1

        def close(self):
            events.append("close")

        def __del__(self):
            events.append("release")

    class Source:
        def __iter__(self):
            return Cursor()

    iterator = iter(flow([]).concat(Source(), [2]))
    assert next(iterator) == 1
    assert next(iterator) == 2
    assert events == ["close"]
    iterator.close()
    assert events == ["close", "release"]


def test_concat_custom_cursor_preserves_close_error():
    failure = OSError("close failed")
    calls = []

    class Cursor:
        def __iter__(self):
            return self

        def __next__(self):
            raise StopIteration

        def close(self):
            calls.append(True)
            raise failure

    class Source:
        def __init__(self):
            self.cursor = Cursor()

        def __iter__(self):
            return self.cursor

    source = Source()
    with pytest.raises(OSError) as captured:
        flow([]).concat(source, source).to_list()
    assert captured.value is failure
    assert calls == [True]


def test_concat_cursor_identity_reuse_does_not_alias_dead_weak_cursor(monkeypatch):
    import weakref

    from fpstreams.runtime import iterators

    class Cursor:
        __hash__ = None

    monkeypatch.setattr(iterators, "id", lambda _: 7, raising=False)
    seen = iterators._IteratorIdentities()
    first = Cursor()
    first_ref = weakref.ref(first)
    seen.add(first)
    assert seen.contains(first)
    del first
    assert first_ref() is None
    second = Cursor()
    assert not seen.contains(second)
    seen.add(second)
    assert seen.contains(second)


@pytest.mark.parametrize("wrapped", [False, True])
def test_concat_releases_exhausted_builtin_repeat_payload(wrapped):
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

    assert flow([]).concat(First(), [], Last()).to_list() == [2]


def test_concat_builtin_iterator_subclass_with_close_is_owned():
    import itertools

    calls = []

    class Cursor(itertools.repeat):
        def close(self):
            calls.append("closed")

    class Source:
        def __init__(self):
            self.cursor = Cursor(1, 1)

        def __iter__(self):
            return self.cursor

    source = Source()
    assert flow([]).concat(source, source).to_list() == [1]
    assert calls == ["closed"]


def test_concat_does_not_probe_custom_close_before_cleanup():
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

    assert flow([]).concat(Source()).to_list() == []
    assert events == ["pull", "get close", "close"]


def test_concat_does_not_compare_or_hash_custom_cursor_type():
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
    assert flow([]).concat(source, source).to_list() == []
    assert source.cursor.closed == 1


def test_concat_registration_failure_closes_opened_cursor(monkeypatch):
    from fpstreams.runtime.iterators import _IteratorIdentities

    failure = MemoryError("registration failed")
    closed = []

    class Cursor:
        def __iter__(self):
            return self

        def __next__(self):
            raise StopIteration

        def close(self):
            closed.append(True)

    class Source:
        def __iter__(self):
            return Cursor()

    def fail(*args):
        raise failure

    monkeypatch.setattr(_IteratorIdentities, "add", fail)
    with pytest.raises(MemoryError) as captured:
        flow([]).concat(Source()).to_list()
    assert captured.value is failure
    assert closed == [True]
