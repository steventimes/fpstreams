# ruff: noqa: E402
"""Versioned synchronous, asynchronous, planning, and public API contracts."""

from __future__ import annotations

# --- Consolidated from contracts/test_sync_semantics.py ---
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass
from typing import Any, Literal

import pytest

from fpstreams import FlowConsumedError, flow
from fpstreams.execution.physical import execute_physical
from fpstreams.planning.compiler import compile_query


@dataclass(frozen=True, slots=True)
class ContractOutcome:
    """Store one returned value or one ordinary exception without retrying work."""

    status: Literal["returned", "raised"]
    value: Any = None
    exception_type: str | None = None
    exception_args: tuple[Any, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "value": self.value,
            "exception_type": self.exception_type,
            "exception_args": list(self.exception_args),
        }


def _raised(error: Exception) -> ContractOutcome:
    kind = type(error)
    return ContractOutcome(
        "raised",
        exception_type=f"{kind.__module__}.{kind.__qualname__}",
        exception_args=error.args,
    )


def capture_sync(call: Callable[[], Any]) -> ContractOutcome:
    try:
        return ContractOutcome("returned", value=call())
    except Exception as error:
        return _raised(error)


async def capture_async(call: Callable[[], Awaitable[Any]]) -> ContractOutcome:
    try:
        return ContractOutcome("returned", value=await call())
    except Exception as error:
        return _raised(error)


def execute_sync(pipeline):
    """Run the selected Python physical path for trace-level contracts."""
    return execute_physical(compile_query(pipeline.with_engine("python")._query("list")))


def traced_source(values: tuple[int, ...], events: list[str]) -> Iterator[int]:
    try:
        for value in values:
            events.append(f"pull:{value}")
            yield value
    finally:
        events.append("close")


def test_sync_fused_callbacks_preserve_pull_call_output_and_close_order() -> None:
    events: list[str] = []

    def mapper(value: int) -> int:
        events.append(f"map:{value}")
        return value * 2

    def predicate(value: int) -> bool:
        events.append(f"filter:{value}")
        return value > 2

    pipeline = flow(traced_source((1, 2, 3, 4), events)).map(mapper).filter(predicate).take(2)
    values: list[int] = []
    for value in execute_sync(pipeline):
        events.append(f"output:{value}")
        values.append(value)

    assert values == [4, 6]
    assert events == [
        "pull:1",
        "map:1",
        "filter:2",
        "pull:2",
        "map:2",
        "filter:4",
        "output:4",
        "pull:3",
        "map:3",
        "filter:6",
        "output:6",
        "close",
    ]


def test_sync_early_close_does_not_pull_a_second_item() -> None:
    events: list[str] = []
    pipeline = flow(traced_source((1, 2, 3), events)).map(
        lambda value: events.append(f"map:{value}") or value
    )
    iterator = execute_sync(pipeline)

    assert next(iterator) == 1
    iterator.close()

    assert events == ["pull:1", "map:1", "close"]


def test_sync_failure_reports_the_first_failing_item_and_closes_once() -> None:
    events: list[str] = []

    def mapper(value: int) -> int:
        events.append(f"map:{value}")
        if value == 2:
            raise ValueError("bad row", value)
        return value

    pipeline = flow(traced_source((1, 2, 3), events)).map(mapper)
    outcome = capture_sync(lambda: list(execute_sync(pipeline)))

    assert outcome.to_dict() == {
        "status": "raised",
        "value": None,
        "exception_type": "builtins.ValueError",
        "exception_args": ["bad row", 2],
    }
    assert events == ["pull:1", "map:1", "pull:2", "map:2", "close"]


def test_sync_source_replayability_contract() -> None:
    reiterable = flow([1, 2])
    one_shot = flow(iter([1, 2]))
    reopened = 0

    def factory() -> list[int]:
        nonlocal reopened
        reopened += 1
        return [1, 2]

    deferred = flow.defer(factory)

    assert list(execute_sync(reiterable)) == [1, 2]
    assert list(execute_sync(reiterable)) == [1, 2]
    assert list(execute_sync(one_shot)) == [1, 2]
    with pytest.raises(FlowConsumedError):
        list(execute_sync(one_shot))
    assert list(execute_sync(deferred)) == [1, 2]
    assert list(execute_sync(deferred)) == [1, 2]
    assert reopened == 2


# --- Consolidated from contracts/test_async_semantics.py ---

import asyncio

from fpstreams import aflow


@pytest.mark.asyncio
async def test_async_serial_callbacks_preserve_pull_call_output_and_close_order() -> None:
    events: list[str] = []

    async def source():
        try:
            for value in (1, 2, 3, 4):
                events.append(f"pull:{value}")
                yield value
        finally:
            events.append("close")

    async def mapper(value: int) -> int:
        events.append(f"map:{value}")
        return value * 2

    async def predicate(value: int) -> bool:
        events.append(f"filter:{value}")
        return value > 2

    pipeline = aflow(source()).map_async(mapper, concurrency=1).filter(predicate).take(2)
    values: list[int] = []
    async for value in pipeline:
        events.append(f"output:{value}")
        values.append(value)

    assert values == [4, 6]
    assert events == [
        "pull:1",
        "map:1",
        "filter:2",
        "pull:2",
        "map:2",
        "filter:4",
        "output:4",
        "pull:3",
        "map:3",
        "filter:6",
        "output:6",
        "close",
    ]


@pytest.mark.asyncio
async def test_async_failure_reports_the_first_failing_item_and_closes_once() -> None:
    events: list[str] = []

    async def source():
        try:
            for value in (1, 2, 3):
                events.append(f"pull:{value}")
                yield value
        finally:
            events.append("close")

    async def mapper(value: int) -> int:
        events.append(f"map:{value}")
        if value == 2:
            raise ValueError("bad row", value)
        return value

    pipeline = aflow(source()).map_async(mapper, concurrency=1)

    async def run() -> list[int]:
        return [item async for item in pipeline]

    outcome = await capture_async(run)

    assert outcome.to_dict() == {
        "status": "raised",
        "value": None,
        "exception_type": "builtins.ValueError",
        "exception_args": ["bad row", 2],
    }
    assert events == ["pull:1", "map:1", "pull:2", "map:2", "close"]


@pytest.mark.asyncio
async def test_async_one_shot_source_rejects_a_second_execution() -> None:
    async def source():
        yield 1
        yield 2

    pipeline = aflow(source())

    assert [item async for item in pipeline] == [1, 2]
    with pytest.raises(FlowConsumedError):
        [item async for item in pipeline]


@pytest.mark.asyncio
async def test_async_early_stop_cancels_and_awaits_every_in_flight_mapper() -> None:
    started: set[int] = set()
    cancelled: set[int] = set()
    active: set[int] = set()
    release_first = asyncio.Event()

    async def mapper(value: int) -> int:
        started.add(value)
        active.add(value)
        try:
            if value == 0:
                await release_first.wait()
                return value
            await asyncio.Future()
        except asyncio.CancelledError:
            cancelled.add(value)
            raise
        finally:
            active.remove(value)

    pipeline = aflow(range(4)).map_async(mapper, concurrency=4, ordered=True).take(1)
    iterator = pipeline.__aiter__()
    pending = asyncio.create_task(anext(iterator))
    while started != {0, 1, 2, 3}:
        await asyncio.sleep(0)
    release_first.set()

    assert await pending == 0
    await iterator.aclose()

    assert active == set()
    assert cancelled == {1, 2, 3}


# --- Consolidated from contracts/test_plan_contract.py ---

from typing import get_args

import pytest

from fpstreams.planning.async_ import AsyncOperation
from fpstreams.planning.semantic_rules import ASYNC_OPERATOR_RULES, SYNC_OPERATOR_RULES
from fpstreams.planning.sync import Operation


def test_sync_python_path_executes_the_logical_pipeline() -> None:
    pipeline = flow([1, 2, 3]).map(lambda value: value * 2).filter(lambda value: value > 2)

    assert pipeline.with_engine("python").to_list() == [4, 6]


@pytest.mark.asyncio
async def test_async_physical_executor_preserves_the_logical_plan() -> None:
    pipeline = aflow([1, 2, 3]).map(lambda value: value * 2).filter(lambda value: value > 2)

    assert [item async for item in pipeline] == [4, 6]


def test_semantic_rule_registries_cover_every_operation_union_member() -> None:
    assert set(SYNC_OPERATOR_RULES) == set(get_args(Operation))
    assert set(ASYNC_OPERATOR_RULES) == set(get_args(AsyncOperation))


def test_capture_sync_calls_the_scenario_once_and_records_failures() -> None:
    calls = 0

    def scenario() -> object:
        nonlocal calls
        calls += 1
        raise ValueError("bad row", 2)

    outcome = capture_sync(scenario)

    assert calls == 1
    assert outcome.to_dict() == {
        "status": "raised",
        "value": None,
        "exception_type": "builtins.ValueError",
        "exception_args": ["bad row", 2],
    }


@pytest.mark.asyncio
async def test_capture_async_calls_the_scenario_once_and_records_values() -> None:
    calls = 0

    async def scenario() -> list[int]:
        nonlocal calls
        calls += 1
        return [1, 2]

    outcome = await capture_async(scenario)

    assert calls == 1
    assert outcome.to_dict() == {
        "status": "returned",
        "value": [1, 2],
        "exception_type": None,
        "exception_args": [],
    }


def test_sync_explain_v2_schema_is_frozen() -> None:
    payload = flow.defer(lambda: iter([2, 1])).sorted().explain("list").to_dict()

    assert set(payload) == {
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
        "boundaries",
    }
    assert set(payload["source"]) == {"reiterable", "exact_size", "ordered"}
    assert set(payload["data_movement"]) == {"scans_source", "copies_source", "materializes"}
    assert set(payload["operations"][0]) == {"name"}
    assert set(payload["stages"][0]) == {"engine", "operations", "fused"}
    assert set(payload["semantics"]) == {"source", "operations", "output", "completion"}
    assert set(payload["diagnostics"][0]) == {
        "code",
        "severity",
        "message",
        "operation_index",
    }


def test_async_explain_v2_schema_is_frozen() -> None:
    payload = aflow([1, 2]).map(lambda value: value + 1).explain("list").to_dict()

    assert set(payload) == {"terminal", "source", "operations", "semantics", "diagnostics"}
    assert set(payload["source"]) == {
        "termination",
        "cardinality",
        "replayability",
        "ordering",
    }
    assert set(payload["operations"][0]) == {
        "index",
        "name",
        "input",
        "output",
        "progress",
        "state",
        "completion_dependencies",
        "requires_order",
    }


def test_sync_explain_does_not_open_or_pull_a_deferred_source() -> None:
    events: list[str] = []

    def source() -> list[int]:
        events.append("open")
        return [1, 2]

    flow.defer(source).map(lambda value: value + 1).explain("list").to_dict()

    assert events == []


def test_async_explain_does_not_open_or_pull_a_deferred_source() -> None:
    events: list[str] = []

    async def source():
        events.append("open")
        yield 1

    aflow.defer(source).map(lambda value: value + 1).explain("list").to_dict()

    assert events == []


# --- Consolidated from contracts/test_m1_query_parity.py ---

"""M1 contracts proving terminal execution is described by one Query first."""


import pytest

from fpstreams.planning.logical import Query


@pytest.mark.parametrize(
    ("build", "invoke"),
    [
        (lambda: flow([1, 2]), lambda value: value.to_list()),
        (lambda: flow([1, 2]), lambda value: value.first()),
        (lambda: flow([1, 2]), lambda value: value.sum()),
        (lambda: flow([0, 1]), lambda value: value.any()),
        (lambda: flow([1, 2]), lambda value: iter(value)),
    ],
)
def test_terminal_constructs_one_query(monkeypatch, build, invoke) -> None:
    """Representative terminals must describe exactly one query before execution."""
    seen: list[Query] = []
    original = type(build())._query

    def tracked(self, name, *arguments, **options):
        query = original(self, name, *arguments, **options)
        seen.append(query)
        return query

    monkeypatch.setattr(type(build()), "_query", tracked)
    result = invoke(build())
    if hasattr(result, "__next__"):
        list(result)

    assert len(seen) == 1
    assert seen[0].terminal.name


# --- Consolidated from contracts/test_m2_physical_parity.py ---

"""M2 physical-plan routing and source-safety contracts."""


import pytest

import fpstreams
from fpstreams.planning.explain import explain_physical


@pytest.mark.parametrize(
    ("terminal", "invoke"),
    [
        ("list", lambda value: value.to_list()),
        ("first", lambda value: value.first()),
        ("sum", lambda value: value.sum()),
        ("minmax", lambda value: value.minmax()),
        ("aggregate", lambda value: value.aggregate(total=fpstreams.agg.sum())),
    ],
)
def test_terminal_compiles_one_physical_plan(monkeypatch, terminal, invoke) -> None:
    """Representative direct terminals route through exactly one compiler invocation."""
    from fpstreams.planning import compiler

    calls = []
    original = compiler.compile_query

    def tracked(query):
        calls.append(query)
        return original(query)

    monkeypatch.setattr("fpstreams.streams.flow_terminals.compile_query", tracked)
    invoke(flow(range(5)))

    assert [query.terminal.name for query in calls] == [terminal]


def test_iterate_selects_the_low_latency_python_row_plan() -> None:
    """Streaming iteration skips materializing backend work while retaining a physical plan."""
    physical = compile_query(flow(range(10)).map(abs).with_engine("python")._query("iterate"))

    assert physical.decision.selected_engine == "python"
    assert physical.decision.reason == "streaming iteration uses low-latency Python row execution"
    assert physical.backend_payload.native_decision.engine == "python"
    assert physical.backend_payload.native_decision.program is None
    assert physical.backend_payload.arrow_prefix is None
    assert all(type(node).__name__ == "RowPhysicalNode" for node in physical.nodes)
    assert list(execute_physical(physical)) == list(range(10))
    explained = explain_physical(physical).to_dict()
    assert explained["selected_engine"] == physical.decision.selected_engine


# --- Consolidated from contracts/test_m3_resource_parity.py ---

"""M3 resource and task-retirement parity contracts."""


import pytest

from fpstreams.execution.async_scheduler import execute_async_physical
from fpstreams.physical.async_plan import compile_async_query
from fpstreams.runtime import QueryRuntime


@pytest.mark.asyncio
@pytest.mark.parametrize("ordered", [True, False])
async def test_map_concurrent_task_registry_is_bounded(ordered) -> None:
    """Completed map tasks retire, so input size cannot inflate task ownership."""
    pipeline = aflow(range(1000)).map_async(
        lambda value: asyncio.sleep(0, result=value),
        concurrency=4,
        ordered=ordered,
    )
    runtime = QueryRuntime()
    result = [
        value
        async for value in execute_async_physical(
            compile_async_query(pipeline._query("list")), runtime
        )
    ]

    assert sorted(result) == list(range(1000))
    assert runtime.metrics.high_water_tasks <= 4


@pytest.mark.asyncio
async def test_map_task_limit_rejection_closes_unscheduled_coroutine() -> None:
    """Internal admission failure must not leave a coroutine for the GC to warn about."""
    import gc
    import warnings

    from fpstreams.runtime import QueryLimits

    pipeline = aflow([1, 2]).map_async(
        lambda value: asyncio.sleep(0, result=value),
        concurrency=2,
    )
    runtime = QueryRuntime(QueryLimits(max_tasks=1))

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", RuntimeWarning)
        with pytest.raises(RuntimeError, match="task limit exceeded: max_tasks=1"):
            [
                value
                async for value in execute_async_physical(
                    compile_async_query(pipeline._query("list")), runtime
                )
            ]
        gc.collect()

    assert not [warning for warning in caught if "was never awaited" in str(warning.message)]
    assert runtime.metrics.live_tasks == 0
    assert runtime.resources.closed


# --- Consolidated from contracts/test_m4_expression_parity.py ---

"""M4 physical compiled-expression compatibility contracts."""


from fpstreams import item


def test_analyzable_map_and_filter_compile_once(monkeypatch) -> None:
    """Physical compilation creates one reusable program for each scalar expression node."""
    from fpstreams.physical.kernel_cache import KernelCache
    from fpstreams.planning import compiler

    calls = 0
    original = compiler.compile_expression
    monkeypatch.setattr(compiler, "_EXPRESSION_PROGRAM_CACHE", KernelCache())

    def tracked(expression):
        nonlocal calls
        calls += 1
        return original(expression)

    monkeypatch.setattr(compiler, "compile_expression", tracked)
    pipeline = flow(range(100)).map(item + 1).filter(item % 2 == 0)

    assert pipeline.to_list() == list(range(2, 101, 2))
    assert calls == 2


def test_opaque_callback_remains_row_physical_node() -> None:
    """Arbitrary Python callbacks remain a no-reorder/no-replay compatibility barrier."""
    physical = compile_query(flow([1]).map(lambda value: value + 1)._query("list"))

    assert type(physical.nodes[0]).__name__ == "RowPhysicalNode"


# --- Consolidated from contracts/test_m10_async_parity.py ---

"""Physical async scheduling preserves the established public map semantics."""


from typing import cast

import pytest

from fpstreams.execution.async_map import execute_async_map
from fpstreams.physical.async_plan import AsyncMapNode


@pytest.mark.asyncio
async def test_ordered_map_holds_later_completion_and_error_until_sequence() -> None:
    gates = [asyncio.Event() for _ in range(3)]

    async def mapper(value: int) -> int:
        await gates[value].wait()
        if value == 2:
            raise ValueError("third")
        return value

    physical = compile_async_query(
        cast(Any, aflow(range(3))).map_async(mapper, concurrency=3, ordered=True)._query("iterate")
    )
    node = physical.nodes[0]
    assert isinstance(node, AsyncMapNode)
    runtime = QueryRuntime()
    iterator = execute_async_map(physical.source.open(), node, runtime)
    gates[1].set()
    gates[2].set()
    await asyncio.sleep(0)
    gates[0].set()

    assert await anext(iterator) == 0
    assert await anext(iterator) == 1
    with pytest.raises(ValueError, match="third"):
        await anext(iterator)
    await cast(Any, iterator).aclose()
    await runtime.aclose()


@pytest.mark.asyncio
async def test_unordered_map_emits_completion_queue_order() -> None:
    gates = [asyncio.Event() for _ in range(3)]

    async def mapper(value: int) -> int:
        await gates[value].wait()
        return value

    physical = compile_async_query(
        cast(Any, aflow(range(3))).map_async(mapper, concurrency=3, ordered=False)._query("iterate")
    )
    node = physical.nodes[0]
    assert isinstance(node, AsyncMapNode)
    runtime = QueryRuntime()
    iterator = execute_async_map(physical.source.open(), node, runtime)
    first = anext(iterator)
    await asyncio.sleep(0)
    gates[2].set()
    assert await first == 2
    gates[0].set()
    assert await anext(iterator) == 0
    gates[1].set()
    assert await anext(iterator) == 1
    await cast(Any, iterator).aclose()
    await runtime.aclose()


@pytest.mark.asyncio
async def test_physical_combine_latest_early_empty_closes_sibling_tasks() -> None:
    sibling_closed = False
    never = asyncio.Event()

    async def empty() -> Any:
        if False:
            yield None

    async def sibling() -> Any:
        nonlocal sibling_closed
        try:
            await never.wait()
            yield 1
        finally:
            sibling_closed = True

    physical = compile_async_query(aflow(empty()).combine_latest(sibling())._query("list"))
    runtime = QueryRuntime()
    assert [item async for item in execute_async_physical(physical, runtime)] == []
    assert sibling_closed
    assert runtime.metrics.live_tasks == 0


@pytest.mark.asyncio
async def test_physical_merge_map_uses_shared_runtime_and_cleans_all_inners() -> None:
    closed: set[int] = set()
    started: set[int] = set()
    release = asyncio.Event()

    async def nested(value: int) -> Any:
        try:
            started.add(value)
            await release.wait()
            yield value
        finally:
            closed.add(value)

    physical = compile_async_query(aflow([1, 2]).merge_map(nested, concurrency=2)._query("iterate"))
    runtime = QueryRuntime()
    iterator = execute_async_physical(physical, runtime)
    first = asyncio.create_task(anext(iterator))
    for _ in range(20):
        if started == {1, 2}:
            break
        await asyncio.sleep(0)
    assert started == {1, 2}
    release.set()
    assert await first in {1, 2}
    await iterator.aclose()
    assert closed == {1, 2}
    assert runtime.metrics.live_tasks == 0


@pytest.mark.asyncio
async def test_merge_map_retires_many_short_non_idempotent_inners() -> None:
    """Completed inners leave constant registry state and close exactly once."""

    class OneShotInner:
        def __init__(self, value: int) -> None:
            self.value = value
            self.emitted = False
            self.close_calls = 0

        def __aiter__(self) -> OneShotInner:
            return self

        async def __anext__(self) -> int:
            if self.emitted:
                raise StopAsyncIteration
            self.emitted = True
            return self.value

        async def aclose(self) -> None:
            self.close_calls += 1
            if self.close_calls > 1:
                raise RuntimeError(f"inner {self.value} closed twice")

    inners = [OneShotInner(value) for value in range(128)]
    pipeline = aflow(range(len(inners))).merge_map(inners.__getitem__, concurrency=1)
    physical = compile_async_query(pipeline._query("iterate"))
    runtime = QueryRuntime()
    iterator = execute_async_physical(physical, runtime)
    result: list[int] = []

    async for value in iterator:
        result.append(value)
        assert len(runtime.resources._records) <= 1

    assert result == list(range(len(inners)))
    assert runtime.resources._records == []
    assert [inner.close_calls for inner in inners] == [1] * len(inners)


@pytest.mark.asyncio
async def test_switch_map_registry_closes_superseded_inner_once() -> None:
    """Latest-only replacement retires the previous registered inner immediately."""

    class SupersededInner:
        def __init__(self, value: int) -> None:
            self.value = value
            self.position = 0
            self.close_calls = 0

        def __aiter__(self) -> SupersededInner:
            return self

        async def __anext__(self) -> int:
            if self.position == 0:
                self.position += 1
                return self.value
            await asyncio.Future()
            raise AssertionError("unreachable")

        async def aclose(self) -> None:
            self.close_calls += 1
            if self.close_calls > 1:
                raise RuntimeError(f"inner {self.value} closed twice")

    release_second = asyncio.Event()

    async def source():
        yield 1
        await release_second.wait()
        yield 2

    inners = {value: SupersededInner(value) for value in (1, 2)}
    iterator = aflow(source()).switch_map(inners.__getitem__).__aiter__()

    assert await anext(iterator) == 1
    release_second.set()
    assert await anext(iterator) == 2
    await iterator.aclose()
    assert [inner.close_calls for inner in inners.values()] == [1, 1]


@pytest.mark.asyncio
async def test_switch_map_cancelled_mapper_result_remains_registry_owned() -> None:
    """A mapper that suppresses cancellation cannot strand its unclaimed inner."""
    mapper_started = asyncio.Event()

    class UnclaimedInner:
        def __init__(self) -> None:
            self.close_calls = 0

        def __aiter__(self) -> UnclaimedInner:
            return self

        async def __anext__(self) -> int:
            raise StopAsyncIteration

        async def aclose(self) -> None:
            self.close_calls += 1
            if self.close_calls > 1:
                raise RuntimeError("unclaimed inner closed twice")

    unclaimed = UnclaimedInner()

    async def mapper(value: int):
        if value == 1:
            mapper_started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                return unclaimed
        return [value]

    async def source():
        yield 1
        await mapper_started.wait()
        yield 2

    assert await aflow(source()).switch_map(mapper).to_list() == [2]
    assert unclaimed.close_calls == 1


@pytest.mark.asyncio
async def test_async_scheduler_closes_each_upstream_layer_once() -> None:
    """The outer stage owns upstream cleanup; the scheduler must not close the root again."""

    class NonIdempotentSource:
        def __init__(self) -> None:
            self.position = 0
            self.close_calls = 0

        def __aiter__(self) -> NonIdempotentSource:
            return self

        async def __anext__(self) -> int:
            if self.position == 1:
                raise StopAsyncIteration
            self.position += 1
            return 1

        async def aclose(self) -> None:
            self.close_calls += 1
            if self.close_calls > 1:
                raise RuntimeError("root closed twice")

    source = NonIdempotentSource()

    assert await aflow(source).map(lambda value: value).to_list() == [1]
    assert source.close_calls == 1


@pytest.mark.asyncio
async def test_async_scheduler_cleans_runtime_after_outer_close_failure() -> None:
    """The first close failure stays primary while later runtime cleanup still runs."""

    class FailingSource:
        def __init__(self) -> None:
            self.done = False

        def __aiter__(self) -> FailingSource:
            return self

        async def __anext__(self) -> int:
            if self.done:
                raise StopAsyncIteration
            self.done = True
            return 1

        async def aclose(self) -> None:
            raise RuntimeError("outer close failed")

    cleanup_calls: list[str] = []
    runtime = QueryRuntime()

    async def fail_runtime_close(value: str) -> None:
        cleanup_calls.append(value)
        raise OSError("runtime close failed")

    await runtime.resources.aown("runtime", fail_runtime_close)
    physical = compile_async_query(aflow(FailingSource())._query("iterate"))

    with pytest.raises(RuntimeError, match="outer close failed") as captured:
        [value async for value in execute_async_physical(physical, runtime)]

    assert cleanup_calls == ["runtime"]
    assert captured.value.__notes__ == ["cleanup failed: OSError: runtime close failed"]


@pytest.mark.asyncio
async def test_async_scheduler_cleans_runtime_when_source_open_fails() -> None:
    """A synchronous opener failure stays primary while runtime cleanup still runs."""

    class FailingOpenSource:
        def __aiter__(self):
            raise OSError("source open failed")

    cleanup_calls: list[str] = []
    runtime = QueryRuntime()

    async def fail_runtime_close(value: str) -> None:
        cleanup_calls.append(value)
        raise RuntimeError("runtime close failed")

    await runtime.resources.aown("runtime", fail_runtime_close)
    physical = compile_async_query(aflow(FailingOpenSource())._query("iterate"))

    with pytest.raises(OSError, match="source open failed") as captured:
        [value async for value in execute_async_physical(physical, runtime)]

    assert runtime.resources.closed
    assert runtime.resources._records == []
    assert cleanup_calls == ["runtime"]
    assert captured.value.__notes__ == ["cleanup failed: RuntimeError: runtime close failed"]


@pytest.mark.asyncio
async def test_async_scheduler_close_before_first_pull_keeps_source_lazy_and_closes_runtime() -> (
    None
):
    """Closing an unstarted execution must release ownership without opening its source."""
    opens: list[str] = []

    class DeferredSource:
        def __aiter__(self):
            opens.append("open")

            async def values():
                yield 1

            return values()

    cleanup_calls: list[str] = []
    runtime = QueryRuntime()

    async def fail_runtime_close(value: str) -> None:
        cleanup_calls.append(value)
        raise OSError("runtime close failed")

    await runtime.resources.aown("runtime", fail_runtime_close)
    physical = compile_async_query(aflow(DeferredSource())._query("iterate"))
    iterator = execute_async_physical(physical, runtime)

    with pytest.raises(OSError, match="runtime close failed"):
        await iterator.aclose()

    assert opens == []
    assert cleanup_calls == ["runtime"]
    assert runtime.resources.closed
    assert runtime.resources._records == []


@pytest.mark.asyncio
async def test_async_scheduler_explicit_close_surfaces_source_cleanup_failure() -> None:
    """Closing an opened stage must expose failure from its innermost source."""

    class FailingCloseSource:
        def __init__(self) -> None:
            self.close_calls = 0

        def __aiter__(self):
            return self

        async def __anext__(self) -> int:
            return 1

        async def aclose(self) -> None:
            self.close_calls += 1
            raise OSError("source close failed")

    source = FailingCloseSource()
    physical = compile_async_query(aflow(source).map(abs)._query("iterate"))
    iterator = execute_async_physical(physical)

    assert await anext(iterator) == 1
    with pytest.raises(OSError, match="source close failed"):
        await iterator.aclose()

    assert source.close_calls == 1


@pytest.mark.asyncio
async def test_async_scheduler_explicit_close_surfaces_runtime_cleanup_failure() -> None:
    """Closing an opened execution must expose runtime-owned cleanup failure."""
    runtime = QueryRuntime()
    cleanup_calls: list[str] = []

    async def fail_runtime_close(value: str) -> None:
        cleanup_calls.append(value)
        raise RuntimeError("runtime close failed")

    await runtime.resources.aown("runtime", fail_runtime_close)
    physical = compile_async_query(aflow([1, 2]).map(abs)._query("iterate"))
    iterator = execute_async_physical(physical, runtime)

    assert await anext(iterator) == 1
    with pytest.raises(RuntimeError, match="runtime close failed"):
        await iterator.aclose()

    assert cleanup_calls == ["runtime"]
    assert runtime.resources.closed


@pytest.mark.asyncio
async def test_async_scheduler_preserves_nested_cleanup_failures_on_consumer_error() -> None:
    """Consumer failure stays primary while source and runtime failures remain visible."""

    class FailingCloseSource:
        def __aiter__(self):
            return self

        async def __anext__(self) -> int:
            return 1

        async def aclose(self) -> None:
            raise OSError("source close failed")

    runtime = QueryRuntime()

    async def fail_runtime_close(_value: str) -> None:
        raise RuntimeError("runtime close failed")

    await runtime.resources.aown("runtime", fail_runtime_close)
    physical = compile_async_query(aflow(FailingCloseSource()).map(abs)._query("iterate"))
    iterator = execute_async_physical(physical, runtime)
    consumer_error = ValueError("consumer failed")

    with pytest.raises(ValueError, match="consumer failed") as captured:
        try:
            async for _value in iterator:
                raise consumer_error
        finally:
            await iterator.aclose()

    assert captured.value is consumer_error
    assert captured.value.__notes__ == [
        "cleanup failed with OSError: source close failed",
        "cleanup failed: RuntimeError: runtime close failed",
    ]


# --- Consolidated from contracts/test_m11_adaptive_parity.py ---

"""Public semantic parity for exact operators backed by adaptive state."""


import pytest


def test_adaptive_unique_preserves_first_seen_values_and_selector_calls() -> None:
    seen: list[int] = []
    values = [*range(16), *range(16)]

    def selector(value: int) -> int:
        seen.append(value)
        return value

    result = fpstreams.flow(values).unique_by(selector).to_list()

    assert result == list(range(16))
    assert seen == values


def test_adaptive_group_index_preserves_first_group_order_and_aggregation() -> None:
    result = (
        fpstreams.rows([{"team": value % 10, "value": value} for value in range(20)])
        .group_by("team")
        .aggregate(total=fpstreams.agg.sum("value"))
        .to_list()
    )

    assert result == [{"team": value, "total": value + value + 10} for value in range(10)]


def test_adaptive_unique_right_join_preserves_validation_and_left_order() -> None:
    left = fpstreams.rows([{"id": 2}, {"id": 1}, {"id": 3}])
    right = fpstreams.rows([{"id": value, "name": str(value)} for value in range(10)])

    assert left.join(right, on="id", how="left", validate="m:1").to_list() == [
        {"id": 2, "name": "2"},
        {"id": 1, "name": "1"},
        {"id": 3, "name": "3"},
    ]
    with pytest.raises(ValueError, match="unique right keys"):
        left.join(fpstreams.rows([{"id": 1}, {"id": 1}]), on="id", validate="m:1").to_list()


# --- Consolidated from contracts/test_differential_corpus.py ---

import random

from fpstreams import agg, rows


def test_fixed_seed_linear_plans_match_the_explicit_reference() -> None:
    rng = random.Random(20260819)
    for _ in range(200):
        values = [rng.randint(-100, 100) for _ in range(rng.randint(0, 80))]
        multiplier = rng.choice((-3, -1, 1, 2, 5))
        offset = rng.randint(-5, 5)
        modulus = rng.randint(2, 9)
        drop = rng.randint(0, 8)
        take = rng.randint(0, 20)

        def build(
            values: list[int] = values,
            multiplier: int = multiplier,
            offset: int = offset,
            modulus: int = modulus,
            drop: int = drop,
            take: int = take,
        ):
            return (
                flow(values)
                .map(lambda item, m=multiplier, o=offset: item * m + o)
                .filter(lambda item, mod=modulus: item % mod != 0)
                .drop(drop)
                .take(take)
            )

        expected = [value * multiplier + offset for value in values]
        expected = [value for value in expected if value % modulus != 0]
        expected = expected[drop : drop + take]
        actual = build().with_engine("python").to_list()

        assert actual == expected


def test_stateful_sync_golden_cases_preserve_stability_and_first_occurrence() -> None:
    records = [
        {"key": 1, "position": "a"},
        {"key": 0, "position": "b"},
        {"key": 1, "position": "c"},
    ]

    assert flow(records).sort_by("key").to_list() == [records[1], records[0], records[2]]
    assert flow([[1], [1], [2], [1]]).unique().to_list() == [[1], [2]]


def test_tabular_join_and_group_golden_contracts() -> None:
    left = [
        {"id": 2, "left": "a"},
        {"id": 1, "left": "b"},
        {"id": 2, "left": "c"},
    ]
    right = [
        {"id": 2, "right": "x"},
        {"id": 2, "right": "y"},
    ]

    assert rows(left).join(right, on="id", how="left").to_list() == [
        {"id": 2, "left": "a", "right": "x"},
        {"id": 2, "left": "a", "right": "y"},
        {"id": 1, "left": "b", "right": None},
        {"id": 2, "left": "c", "right": "x"},
        {"id": 2, "left": "c", "right": "y"},
    ]
    assert rows(left).group_by("id").aggregate(count=agg.count()).to_list() == [
        {"id": 2, "count": 2},
        {"id": 1, "count": 1},
    ]


# --- Consolidated from contracts/test_public_api_contract.py ---

import inspect
import json
from enum import Enum
from pathlib import Path

SNAPSHOT = Path(__file__).with_name("public_api_v2.json")
_API_SCALARS = (type(None), bool, int, float, str)


def _api_annotation(value: Any) -> str | None:
    if value is inspect.Signature.empty:
        return None
    if isinstance(value, str):
        return value
    module = getattr(value, "__module__", None)
    name = getattr(value, "__qualname__", None)
    if module is not None and name is not None:
        return f"{module}.{name}"
    return str(value).replace("typing.", "")


def _api_default(value: Any) -> dict[str, Any]:
    if value is inspect.Signature.empty:
        return {"required": True, "value": None}
    if isinstance(value, _API_SCALARS):
        return {"required": False, "value": value}
    if isinstance(value, Enum):
        return {"required": False, "value": value.value}
    kind = type(value)
    return {
        "required": False,
        "value": f"<{kind.__module__}.{kind.__qualname__}>",
    }


def _api_signature(value: Any) -> dict[str, Any] | None:
    if inspect.isclass(value) and issubclass(value, Enum):
        return {
            "parameters": [
                {
                    "name": "values",
                    "kind": "VAR_POSITIONAL",
                    "default": {"required": True, "value": None},
                    "annotation": None,
                }
            ],
            "return": None,
        }
    try:
        signature = inspect.signature(value)
    except (TypeError, ValueError):
        return None
    return {
        "parameters": [
            {
                "name": parameter.name,
                "kind": parameter.kind.name,
                "default": _api_default(parameter.default),
                "annotation": _api_annotation(parameter.annotation),
            }
            for parameter in signature.parameters.values()
        ],
        "return": _api_annotation(signature.return_annotation),
    }


def _api_owned_member(raw: Any, member: Any) -> bool:
    sources = (raw.fget, member) if isinstance(raw, property) else (raw, member)
    return any(
        isinstance(module, str) and (module == "fpstreams" or module.startswith("fpstreams."))
        for source in sources
        if (module := getattr(source, "__module__", None)) is not None
    )


def _api_public_signature(value: Any) -> dict[str, Any] | None:
    if callable(value) and not inspect.isclass(value) and not inspect.isfunction(value):
        call = inspect.getattr_static(type(value), "__call__", None)
        if call is not None and _api_owned_member(call, call):
            signature = _api_signature(call)
            if signature is not None and signature["parameters"]:
                first = signature["parameters"][0]
                if first["name"] in {"self", "cls"}:
                    signature["parameters"] = signature["parameters"][1:]
            return signature
    return _api_signature(value) if callable(value) else None


def _api_descriptor(value: Any) -> dict[str, Any]:
    if isinstance(value, _API_SCALARS):
        return {
            "kind": "constant",
            "target": f"{type(value).__module__}.{type(value).__qualname__}",
            "value": value,
            "signature": None,
            "callables": {},
            "properties": [],
        }
    owner = value if inspect.isclass(value) else type(value)
    callables: dict[str, Any] = {}
    properties: list[str] = []
    for name in dir(value):
        if name.startswith("_"):
            continue
        raw = inspect.getattr_static(owner, name, None)
        member = getattr(value, name)
        if not _api_owned_member(raw, member):
            continue
        if isinstance(raw, property):
            properties.append(name)
        elif callable(member):
            callables[name] = _api_signature(member)
    kind = "object"
    if inspect.isclass(value):
        kind = "class"
    elif inspect.isfunction(value):
        kind = "function"
    return {
        "kind": kind,
        "target": f"{value.__module__}.{value.__qualname__}"
        if hasattr(value, "__qualname__")
        else f"{type(value).__module__}.{type(value).__qualname__}",
        "signature": _api_public_signature(value),
        "callables": dict(sorted(callables.items())),
        "properties": sorted(properties),
    }


def build_snapshot() -> dict[str, Any]:
    exports = sorted(fpstreams.__all__)
    first_name_by_identity: dict[int, str] = {}
    items: dict[str, Any] = {}
    aliases: dict[str, str] = {}
    for name in exports:
        value = getattr(fpstreams, name)
        identity = id(value)
        if identity in first_name_by_identity:
            aliases[name] = first_name_by_identity[identity]
            continue
        first_name_by_identity[identity] = name
        items[name] = _api_descriptor(value)
    return {
        "version": fpstreams.__version__,
        "root_exports": exports,
        "aliases": dict(sorted(aliases.items())),
        "items": items,
    }


def test_public_api_matches_the_checked_in_v2_manifest() -> None:
    expected = json.loads(SNAPSHOT.read_text(encoding="utf-8"))

    assert build_snapshot() == expected
