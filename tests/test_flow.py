"""Synchronous Flow sources, lazy transforms, selectors, gatherers, and terminals."""

from __future__ import annotations

import json
import math
import subprocess
import sys
import threading
import time
from collections.abc import AsyncIterator, Callable, Iterable, Iterator
from pathlib import Path
from typing import Any

import pytest

import benchmark
import fpstreams
from fpstreams import Downstream, Gatherer, NativeUnsupportedError, SelectionError, flow

# --- Tests consolidated from test_flow_api.py ---


def test_flow_collects_any_iterable() -> None:
    assert flow(range(4)).to_list() == [0, 1, 2, 3]


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
    from fpstreams.streams import _flow_structural_list

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
    from fpstreams.streams import _flow_structural_list

    monkeypatch.setattr(_flow_structural_list, "_CPYTHON_312_DIRECT_FILTER_LOOP", True)


@pytest.mark.parametrize("engine", ["auto", "python"])
@pytest.mark.parametrize("container", [list, tuple], ids=["list", "tuple"])
def test_direct_python_filter_list_uses_only_the_supported_runtime(
    monkeypatch: pytest.MonkeyPatch,
    engine: str,
    container: Callable[[Iterable[int]], Iterable[int]],
) -> None:
    """Production dispatch selects the measured CPython 3.12 loop and no other runtime."""
    from fpstreams.streams import _flow_structural_list

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
    from fpstreams.streams import _flow_structural_list

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

    from fpstreams.streams import _flow_structural_list

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
    from fpstreams.streams import _flow_structural_list

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


def test_retained_list_chunks_use_direct_tuple_batching(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exact lists avoid allocating an intermediate list slice for every chunk."""
    from fpstreams.streams import _flow_structural_list as structural

    batched_sources: list[object] = []

    def tracked_batched(values: list[object], size: int) -> Iterator[tuple[object, ...]]:
        batched_sources.append(values)
        for index in range(0, len(values), size):
            yield tuple(values[index : index + size])

    monkeypatch.setattr(structural, "_BATCHED", tracked_batched)

    retained_list = [1, 2, 3, 4, 5]
    retained_tuple = (1, 2, 3, 4, 5)
    assert flow(retained_list).with_engine("python").chunk(2).to_list() == [
        (1, 2),
        (3, 4),
        (5,),
    ]
    assert flow(retained_tuple).with_engine("python").chunk(2).to_list() == [
        (1, 2),
        (3, 4),
        (5,),
    ]
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
    from fpstreams.planning.arrow_source import ArrowBatchSource

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
    from fpstreams.planning.arrow_source import ArrowBatchSource

    batch = pa.record_batch({"id": [1, 2]})
    pipeline = flow(batch)
    descriptor = pipeline._pipeline.source.native_data

    assert isinstance(descriptor, ArrowBatchSource)
    assert descriptor.kind == "record_batch"
    assert descriptor.materialized_data is batch
    assert pipeline.to_list() == [{"id": 1}, {"id": 2}]


def test_flow_factory_keeps_pyarrow_reader_one_shot() -> None:
    pa = pytest.importorskip("pyarrow")
    from fpstreams.planning.arrow_source import ArrowBatchSource

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
    from fpstreams.planning.arrow_source import ArrowBatchSource

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
    from fpstreams.planning.arrow_source import ArrowBatchSource

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
    from fpstreams.planning.arrow_source import ArrowBatchSource

    frame = pd.DataFrame({"id": [1, 2]}, index=pd.Index([10, 20], name="row_id"))
    pipeline = flow(frame)
    descriptor = pipeline._pipeline.source.native_data

    assert isinstance(descriptor, ArrowBatchSource)
    assert descriptor.kind == "dataframe"
    assert pipeline.to_list() == [{"id": 1}, {"id": 2}]


def test_flow_factory_defers_generic_dataframe_conversion_until_consumption() -> None:
    pd = pytest.importorskip("pandas")
    from fpstreams.planning.arrow_source import ArrowBatchSource

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
    from fpstreams.planning.arrow_source import ArrowBatchSource

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
    from fpstreams.planning.arrow_source import ArrowBatchSource

    frame = pl.DataFrame({"id": [1, 2]})
    pipeline = flow(frame)
    descriptor = pipeline._pipeline.source.native_data

    assert isinstance(descriptor, ArrowBatchSource)
    assert descriptor.kind == "polars"
    assert pipeline.to_list() == [{"id": 1}, {"id": 2}]


def test_flow_factory_recognizes_a_polars_subclass_from_an_application_module() -> None:
    pl = pytest.importorskip("polars")
    from fpstreams.planning.arrow_source import ArrowBatchSource

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
    assert events == ["iter", "next", "predicate", "truth", "item:del", "close"]


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

    values = container(int(str(1_000 + index % 16)) for index in range(131_072))
    endpoint = _native.unique_i64_exact_prefix_cached_v1
    calls = 0

    def tracked(output: list[object], source: Iterator[object]) -> object:
        nonlocal calls
        calls += 1
        return endpoint(output, source)

    monkeypatch.setattr(_native, "unique_i64_exact_prefix_cached_v1", tracked)
    result = flow(values).unique().run_with_report("to_list")

    assert result.value == list(range(1_000, 1_016))
    assert all(result.value[index] is values[index] for index in range(16))
    assert result.report.strategy == "rust_direct"
    assert calls == 1


def test_auto_unique_low_cardinality_selects_the_sampled_cached_sink(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A conservatively sampled repeated source avoids the baseline hash probe loop."""
    from fpstreams import _native

    values = [int(str(1_000 + index % 32)) for index in range(131_072)]
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

    assert result == list(range(1_000, 1_032))
    assert cached_calls == 1


def test_auto_unique_reused_objects_select_the_identity_cached_sink(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repeated exact objects skip extraction while preserving their first identities."""
    from fpstreams import _native
    from fpstreams.streams import _flow_unique_list

    first_objects = [int(str(1_000 + index)) for index in range(16)]
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
    from fpstreams.streams import _flow_unique_list

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
    from fpstreams.streams import _flow_unique_list

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

    values = [int(str(1_000 + index % 16)) for index in range(131_072)]
    endpoint = _native.unique_i64_exact_prefix_v1
    baseline_calls = 0

    def tracked_baseline(output: list[object], source: Iterator[object]) -> object:
        nonlocal baseline_calls
        baseline_calls += 1
        return endpoint(output, source)

    monkeypatch.setattr(_native, "unique_i64_exact_prefix_v1", tracked_baseline)
    monkeypatch.delattr(_native, "unique_i64_exact_prefix_cached_v1")

    result = flow(values).unique().to_list()

    assert result == list(range(1_000, 1_016))
    assert baseline_calls == 1


def test_auto_unique_sample_allocation_failure_keeps_the_baseline_route(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Classifier bookkeeping cannot introduce a new allocation failure."""
    from fpstreams.streams import _flow_unique_list

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
    from fpstreams.streams import _flow_unique_list

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

    first_objects = [int(str(1_000 + index)) for index in range(16)]
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
    assert result == list(range(1_000, 1_016))
    assert [id(value) for value in result] == first_ids


@pytest.mark.skipif(sys.platform == "win32", reason="setitimer is Unix-only")
def test_unique_identity_cache_owns_slots_when_direct_caller_clears_output() -> None:
    """Cache referents survive direct-ABI output and source mutation between signal checks."""
    import signal

    from fpstreams import _native

    first_objects = [int(str(1_000 + index)) for index in range(16)]
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
    from fpstreams.streams import _flow_unique_list

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
    from fpstreams.streams import _flow_unique_list

    def replacement(*_arguments: object) -> tuple[bool, list[str]]:
        return True, ["replacement"]

    monkeypatch.setattr(_flow_unique_list, "try_direct_unique_list", replacement)

    assert flow([1, 1]).unique().to_list() == ["replacement"]


def test_auto_unique_sampler_code_is_guarded_before_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Replacing the sampler function's code deopts without running injected code."""
    from fpstreams import _native
    from fpstreams.streams import _flow_unique_list

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
    from fpstreams.streams import _flow_unique_list

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
    """Only key hashing failures receive the historical reduce_by message."""

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


def test_identity_frequencies_preserves_custom_key_hashing() -> None:
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
        flow([key]).frequencies()
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


def test_explain_reports_fusion_and_forced_engine_rejections() -> None:
    explanation = flow(range(10)).map(str).filter(bool).take(2).explain().to_dict()

    assert explanation["source"] == {
        "reiterable": True,
        "exact_size": 10,
        "ordered": True,
    }
    assert [operation["name"] for operation in explanation["operations"]] == [
        "map",
        "filter",
        "take",
    ]
    assert explanation["stages"][0] == {
        "engine": "python",
        "operations": ["map", "filter"],
        "fused": True,
    }
    with pytest.raises(NativeUnsupportedError, match="map"):
        flow([1, 2]).map(lambda value: value + 1).with_engine("native").to_list()


def test_explain_preserves_serialization_order() -> None:
    payload = flow(range(3)).map(str).filter(bool).take(1).with_engine("python").explain().to_dict()

    assert tuple(payload) == (
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


def test_explain_reports_order_state_and_completion_risks() -> None:
    unordered = flow({1, 2}).scan(0, lambda total, value: total + value).explain("list").to_dict()
    infinite = flow.iterate(0, lambda value: value + 1).sorted().explain("list").to_dict()
    unknown = flow.defer(lambda: iter([2, 1])).sorted().explain("list").to_dict()

    assert {item["code"] for item in unordered["diagnostics"]} == {"ORDER_NOT_PRESERVED"}
    assert [item["code"] for item in infinite["diagnostics"]] == [
        "STATE_MAY_GROW",
        "NON_TERMINATING_PLAN",
        "NON_TERMINATING_PLAN",
    ]
    assert [item["code"] for item in unknown["diagnostics"]] == [
        "STATE_MAY_GROW",
        "COMPLETION_NOT_PROVEN",
        "COMPLETION_NOT_PROVEN",
    ]
    assert flow([1]).explain("first").to_dict()["semantics"]["completion"] == (
        "first_item_or_source_end"
    )
    assert flow([1]).explain("all").to_dict()["semantics"]["completion"] == (
        "witness_or_source_end"
    )
    assert flow([1]).explain().to_dict()["semantics"]["completion"] == "consumer_stop"


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


# --- Tests consolidated from test_stream_extensions.py ---

"""Gatherer contracts and synchronous, asynchronous, and native stream extensions."""


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


# --- Tests consolidated from test_execution_engines.py ---

"""Parallel mapping, engine planning, fused terminals, source metadata, and external sorting."""


def _engine_square(value: int) -> int:
    return value * value


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
    from fpstreams.expressions.scalar import (
        _compile_float_evaluator,
        _compile_int_evaluator,
    )

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


def test_identity_index_of_reports_only_the_range_direct_path() -> None:
    """Inspecting a list must not report the range-only arithmetic shortcut."""
    direct = flow(range(5)).run_with_report("index_of", 3)
    scanned = flow([1, 2, 3]).run_with_report("index_of", 3)

    assert direct.value == 3
    assert direct.report.compiler_engine == "not_compiled"
    assert direct.report.strategy == "python_direct"
    assert scanned.value == 2
    assert scanned.report.compiler_engine == "python"
    assert scanned.report.strategy == "planned:python"


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
    assert finite.sum() == 1.0

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
    source = container(int(str(1_000 + index)) for index in range(16))
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


def test_numeric_iterator_mean_reports_direct_and_hybrid_execution() -> None:
    """Reports distinguish an all-Rust reduction from a Python suffix continuation."""
    from fractions import Fraction

    from fpstreams.collecting.statistics import compensated_mean

    direct = fpstreams.flow(iter(range(1_000))).run_with_report("mean")
    mixed_values = [*range(128), Fraction(1, 3), 7]
    mixed = fpstreams.flow(iter(mixed_values)).run_with_report("mean")

    assert direct.value == 499.5
    assert direct.report.strategy == "rust_direct"
    assert mixed.value == compensated_mean(mixed_values)
    assert mixed.report.strategy == "rust_python_hybrid"


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
def test_exact_numeric_mean_decline_is_atomic_before_signal_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A late decline never replays a prefix mutated by a mid-probe signal callback."""
    import signal
    from fractions import Fraction

    from fpstreams.collecting.statistics import compensated_mean
    from fpstreams.execution import native

    original_size = 600_000
    original: list[object] = list(range(original_size))
    original[400_000] = Fraction(1, 3)
    values = original.copy()
    expected_values = original[100_000:]
    expected_values[300_000] = 7
    expected = compensated_mean(expected_values)
    previous = signal.getsignal(signal.SIGALRM)
    handled_signal = False
    extension = native._native

    class AlarmNative:
        def mean_exact_numbers_v1(self, source: object) -> object:
            signal.setitimer(signal.ITIMER_REAL, 0.001, 0)
            return extension.mean_exact_numbers_v1(source)

        def __getattr__(self, name: str) -> object:
            return getattr(extension, name)

    def shrink_front_and_replace_late_value(_signum: int, _frame: object) -> None:
        nonlocal handled_signal
        handled_signal = True
        del values[:100_000]
        values[300_000] = 7

    signal.signal(signal.SIGALRM, shrink_front_and_replace_late_value)
    monkeypatch.setattr(native, "_native", AlarmNative())
    try:
        result = fpstreams.flow(values).mean()
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)

    assert handled_signal is True
    assert values == expected_values
    assert result == expected


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
def test_typed_mean_late_mismatch_is_atomic_before_signal_mutation(kind: str) -> None:
    """Typed compatibility endpoints cannot turn a late mismatch into a replayed success."""
    import signal

    from fpstreams import _native

    if kind == "i64":
        values: list[object] = [1] * 600_000
        values[400_000] = 1.5
        replacement: object = 7
        endpoint = _native.mean_i64
    else:
        values = [1.0] * 600_000
        values[400_000] = 7
        replacement = 7.0
        endpoint = _native.mean_f64
    previous = signal.getsignal(signal.SIGALRM)
    handled_signal = False

    def shrink_front_and_repair_mismatch(_signum: int, _frame: object) -> None:
        nonlocal handled_signal
        handled_signal = True
        del values[:100_000]
        values[300_000] = replacement

    signal.signal(signal.SIGALRM, shrink_front_and_repair_mismatch)
    signal.setitimer(signal.ITIMER_REAL, 0.001, 0)
    try:
        with pytest.raises(TypeError):
            endpoint(values, [])
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)

    assert handled_signal is True


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
import signal

import numpy as np

from fpstreams import _native

values = np.arange({size}, dtype=np.int64)
original_size = values.size
handled_signal = False


def resize_exporter(_signum, _frame):
    global handled_signal
    handled_signal = True
    values.resize(1, refcheck=False)


signal.signal(signal.SIGALRM, resize_exporter)
signal.setitimer(signal.ITIMER_REAL, 0.0005, 0)
try:
    result = getattr(_native, {endpoint_name!r})(values, [])
finally:
    signal.setitimer(signal.ITIMER_REAL, 0)

assert handled_signal
assert values.size == 1
assert result == (original_size - 1) / 2
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


# --- Tests consolidated from test_benchmark.py ---


ROOT = Path(__file__).resolve().parents[1]
REQUIRED_RESULT_KEYS = {
    "name",
    "sample_count",
    "samples_seconds",
    "median_seconds",
    "stdev_seconds",
    "backend",
    "source_kind",
    "terminal",
    "baseline",
}


def test_quick_benchmark_emits_machine_readable_identity_baselines() -> None:
    report = benchmark.run(size=32, repeats=2, domain="int", quick=True)

    assert report["schema_version"] == 1
    assert report["metadata"]["size"] == 32
    assert report["metadata"]["repeats"] == 2
    results = report["results"]
    names = {result["name"] for result in results}
    assert {
        "python_builtin/list/identity/sum",
        "fpstreams_python/list/identity/sum",
        "fpstreams_auto/list/identity/sum",
        "python_builtin/range/identity/count",
        "fpstreams_python/range/identity/count",
        "fpstreams_auto/range/identity/count",
    } <= names
    for result in results:
        assert result.keys() >= REQUIRED_RESULT_KEYS
        assert len(result["samples_seconds"]) == result["sample_count"]
        name = result["name"]
        if name in {
            "fpstreams_operation/sync/map_parallel",
            "fpstreams_operation/rows/group_spill_aggregate",
        }:
            expected_count = 21
        elif name.startswith(
            (
                "fpstreams_group/dict/callable_",
                "fpstreams_group/mappingproxy/callable_",
                "fpstreams_group/nominal_mapping/callable_",
                "fpstreams_join/namedtuple/callable/",
            )
        ):
            expected_count = 15
        else:
            expected_count = 2
        assert result["sample_count"] == expected_count
        assert result["median_seconds"] >= 0
        assert result["stdev_seconds"] >= 0


def test_regression_gate_compares_auto_identity_to_same_run_python() -> None:
    records: list[dict[str, Any]] = [
        {
            "name": "fpstreams_python/list/identity/sum",
            "median_seconds": 1.0,
            "backend": "python",
            "source_kind": "list",
            "terminal": "sum",
            "baseline": "python_builtin/list/identity/sum",
        },
        {
            "name": "fpstreams_auto/list/identity/sum",
            "median_seconds": 1.11,
            "backend": "auto",
            "source_kind": "list",
            "terminal": "sum",
            "baseline": "fpstreams_python/list/identity/sum",
        },
    ]

    regressions = benchmark.find_regressions(records, maximum_ratio=1.10)

    assert len(regressions) == 1
    assert regressions[0]["ratio"] == pytest.approx(1.11)


def test_regression_gate_compares_expression_fallback_to_lambda() -> None:
    records: list[dict[str, Any]] = [
        {
            "name": "fpstreams_lambda/list/map_filter/sum",
            "median_seconds": 1.0,
            "backend": "python",
            "source_kind": "list",
            "terminal": "sum",
            "baseline": "python_builtin/list/map_filter/sum",
        },
        {
            "name": "fpstreams_python/list/map_filter/sum",
            "median_seconds": 2.01,
            "backend": "python",
            "source_kind": "list",
            "terminal": "sum",
            "baseline": "fpstreams_lambda/list/map_filter/sum",
        },
    ]

    regressions = benchmark.find_regressions(records)

    assert len(regressions) == 1
    assert regressions[0]["maximum_ratio"] == 2.0


def test_large_python_expression_benchmarks_require_scalar_fusion_speedup() -> None:
    scenarios = benchmark._integer_pipeline_scenarios(4_096, native_available=False)
    candidates = [
        scenario for scenario in scenarios if scenario.name.startswith("fpstreams_python/")
    ]

    assert len(candidates) == 4
    assert all(candidate.baseline.startswith("fpstreams_lambda/") for candidate in candidates)
    assert all(candidate.maximum_ratio == 0.75 for candidate in candidates)


def test_run_with_report_runs_one_real_terminal_and_returns_immutable_metrics() -> None:
    calls: list[int] = []

    def double(value: int) -> int:
        calls.append(value)
        return value * 2

    execution = fpstreams.flow([1, 2, 3]).with_engine("python").map(double).run_with_report("sum")

    assert execution.value == 12
    assert calls == [1, 2, 3]
    assert execution.report.terminal == "sum"
    assert execution.report.requested_engine == "python"
    assert execution.report.compiler_engine == "python"
    assert execution.report.strategy == "planned:python"
    assert execution.report.elapsed_ns >= 0
    assert execution.report.peak_owned_async_tasks == 0
    assert execution.report.peak_spill_files == 0
    assert execution.report.spill_bytes_written == 0
    with pytest.raises(AttributeError):
        execution.report.compiler_engine = "native"  # type: ignore[misc]


def test_run_with_report_preserves_exact_count_metadata_shortcut() -> None:
    execution = fpstreams.flow([1, 2, 3]).run_with_report("count")

    assert execution.value == 3
    assert execution.report.terminal == "count"
    assert execution.report.compiler_engine == "not_compiled"
    assert execution.report.strategy == "metadata"
    assert "cardinality" in execution.report.reason


def test_run_with_report_captures_external_sort_spill_metrics(tmp_path: Path) -> None:
    execution = (
        fpstreams.flow(range(20, -1, -1))
        .sort_by(lambda value: value, buffer_size=3, tempdir=tmp_path)
        .run_with_report("to_list")
    )

    assert execution.value == list(range(21))
    assert execution.report.peak_spill_files > 0
    assert execution.report.spill_bytes_written > 0
    assert list(tmp_path.iterdir()) == []


def test_run_with_report_preserves_terminal_exception_and_resets_scope() -> None:
    failure = ValueError("boom")

    def fail(_value: int) -> int:
        raise failure

    with pytest.raises(ValueError) as captured:
        fpstreams.flow([1]).map(fail).run_with_report("to_list")
    assert captured.value is failure

    assert fpstreams.flow([1]).run_with_report("to_list").value == [1]


def test_run_with_report_allows_composite_child_plans() -> None:
    execution = (
        fpstreams.flow([1])
        .concat(fpstreams.flow([2]).map(lambda value: value * 10))
        .run_with_report("to_list")
    )

    assert execution.value == [1, 20]
    assert execution.report.terminal == "to_list"


def test_run_with_report_preserves_outer_engine_for_iteration_terminals() -> None:
    execution = fpstreams.flow([1, 2, 3]).run_with_report(
        "reduce", lambda left, right: left + right
    )

    assert execution.value == 6
    assert execution.report.requested_engine == "auto"


def test_run_with_report_handles_direct_none_and_non_consuming_collector() -> None:
    none_result = fpstreams.flow([1, 2, 3]).run_with_report("none", lambda value: value > 3)
    collect_result = fpstreams.flow([1, 2, 3]).run_with_report("collect", lambda _values: 42)

    assert none_result.value is True
    assert none_result.report.compiler_engine == "not_compiled"
    assert none_result.report.strategy == "python_direct"
    assert collect_result.value == 42
    assert collect_result.report.compiler_engine == "not_compiled"
    assert collect_result.report.strategy == "dynamic_collector"


def test_run_with_report_allows_nested_reported_terminals_in_callbacks() -> None:
    execution = (
        fpstreams.flow([1, 2])
        .map(lambda value: fpstreams.flow([value]).run_with_report("sum").value)
        .run_with_report("to_list")
    )

    assert execution.value == [1, 2]
    assert execution.report.terminal == "to_list"


def test_run_with_report_rejects_lazy_operations_before_execution() -> None:
    with pytest.raises(ValueError, match="eager terminal"):
        fpstreams.flow([1, 2, 3]).run_with_report("map", lambda value: value)


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
