"""execution planning regression contracts."""

from __future__ import annotations

import builtins
import math
import signal
import struct
import sys
import weakref
from collections.abc import Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from types import FunctionType

import pytest

import fpstreams
from fpstreams import aflow, agg, coalesce, col, fitem, flow, item, lit
from fpstreams.collecting import Collector, Collectors
from fpstreams.collecting.aggregate_program import NativeAggregateField, compile_aggregations
from fpstreams.collecting.aggregation import prepare_aggregations
from fpstreams.collecting.program import compile_collectors, run_collector_program
from fpstreams.execution import physical
from fpstreams.execution import physical as physical_executor
from fpstreams.execution.pairs import dictionary as _pair_dict
from fpstreams.execution.physical import (
    PhysicalExecutionError,
    execute_physical,
    operations_from_physical_nodes,
)
from fpstreams.expressions import program as program_module
from fpstreams.expressions import scalar
from fpstreams.expressions.program import ExprEvaluator, ExprProgram, compile_expression
from fpstreams.expressions.row_ir import (
    Binary,
    Call,
    Cast,
    Coalesce,
    Field,
    GetItem,
    IfElse,
    Literal,
    PythonUDF,
    Unary,
)
from fpstreams.expressions.typed_ir import (
    Effect,
    ExpressionSource,
    TypedExpr,
    ValueType,
    lower_expression,
)
from fpstreams.physical.async_plan import compile_async_query
from fpstreams.physical.compiled import ProgramFingerprint
from fpstreams.physical.kernel_cache import KernelCache
from fpstreams.physical.plan import (
    CompiledExpressionPhysicalNode,
    PhysicalNode,
    PhysicalPlan,
)
from fpstreams.physical.relational import (
    JoinPhysicalNode,
)
from fpstreams.planning.compiler import compile_query
from fpstreams.planning.logical import (
    GlobalAggregateNode,
    GroupAggregateNode,
    JoinNode,
    LogicalPlan,
    Pipeline,
    Query,
    SourceNode,
    TerminalSpec,
    linear_pipeline,
    unary_chain,
)
from fpstreams.planning.plan_cache import PhysicalPlanTemplate, PlanCache, PlanCacheKey
from fpstreams.planning.source import Source, SourceCapabilities
from fpstreams.planning.sync import DropOp, FilterOp, MapOp, TakeOp
from fpstreams.runtime.failpoints import failpoint
from fpstreams.tabular import rows


@pytest.mark.parametrize("raises", [False, True])
def test_native_frequency_iterator_stops_before_custom_key_protocols(raises: bool) -> None:
    from fpstreams import _native

    events = []
    failure = RuntimeError("second hash")

    class Key:
        calls = 0

        def __hash__(self):
            self.calls += 1
            if raises and self.calls == 2:
                raise failure
            return 7

    key = Key()

    def values():
        try:
            events.append("first")
            yield 7
            events.append("boundary")
            yield key
            events.append("tail")
            yield 9
        finally:
            events.append("closed")

    iterator = values()
    counts, boundary, exhausted = _native.frequencies_iter_prefix_v1(iterator)
    assert counts == {7: 1}
    assert boundary is key
    assert not exhausted
    assert key.calls == 0
    assert events == ["first", "boundary"]
    from fpstreams.streams.flow_terminals import (
        _prepend_frequency_boundary,
        _update_identity_frequency_counts,
    )

    try:
        if raises:
            with pytest.raises(RuntimeError) as caught:
                _update_identity_frequency_counts(
                    _prepend_frequency_boundary(boundary, iterator), counts
                )
            assert caught.value is failure
            assert key.calls == 2
            assert events == ["first", "boundary"]
        else:
            result = _update_identity_frequency_counts(
                _prepend_frequency_boundary(boundary, iterator), counts
            )
            assert list(result.values()) == [1, 1, 1]
            assert events == ["first", "boundary", "tail", "closed"]
    finally:
        iterator.close()


@pytest.mark.parametrize("dtype", ["int64", "float64"])
@pytest.mark.parametrize("failure_kind", ["none", "source", "count"])
def test_numpy_frequency_iterator_preserves_runtime_error_ownership(
    dtype: str, failure_kind: str, monkeypatch: pytest.MonkeyPatch
) -> None:

    import numpy as np

    from fpstreams.execution import native
    from fpstreams.runtime.query import QueryRuntime

    failure = RuntimeError(failure_kind)
    closes = []
    events = []
    original_close = QueryRuntime.close

    def close(self, active_error=None):
        closes.append(active_error)
        return original_close(self, active_error)

    class Key:
        def __hash__(self):
            raise failure

    def execute(program):
        def values():
            try:
                events.append("first")
                yield 1
                if failure_kind == "source":
                    raise failure
                events.append("second")
                yield Key() if failure_kind == "count" else 2
                events.append("tail")
            finally:
                events.append("closed")

        return values()

    monkeypatch.setattr(QueryRuntime, "close", close)
    monkeypatch.setattr(native, "execute", execute)
    source = flow.from_numpy(np.arange(1000, dtype=dtype))
    if failure_kind == "none":
        assert source.frequencies() == {1: 1, 2: 1}
        assert closes == [None]
        assert events == ["first", "second", "tail", "closed"]
    else:
        with pytest.raises(RuntimeError) as caught:
            source.frequencies()
        assert caught.value is failure
        assert closes == ([failure] if failure_kind == "source" else [None])
        assert "tail" not in events
        assert events[-1] == "closed"


@pytest.mark.parametrize("dtype", ["int64", "float64"])
@pytest.mark.parametrize("engine", ["python", "auto", "native"])
def test_numpy_frequency_iterator_reports_only_selected_automatic_native_counting(
    dtype: str, engine: str
) -> None:

    import numpy as np

    result = (
        flow.from_numpy(np.arange(1000, dtype=dtype) % 16)
        .with_engine(engine)
        .run_with_report("frequencies")
    )
    assert list(result.value) == list(range(16))
    assert sum(result.value.values()) == 1000
    assert all(type(key) is (int if dtype == "int64" else float) for key in result.value)
    assert (result.report.strategy == "rust_direct") is (engine == "auto")


@pytest.mark.parametrize("unavailable", ["missing", "declined"])
def test_numpy_frequency_iterator_old_wheel_does_not_consume_twice(
    unavailable: str, monkeypatch: pytest.MonkeyPatch
) -> None:

    import numpy as np

    from fpstreams import _native
    from fpstreams.execution import native

    calls = []
    original = native.execute

    def execute(program):
        calls.append("execute")
        return original(program)

    monkeypatch.setattr(native, "execute", execute)
    if unavailable == "missing":
        monkeypatch.delattr(_native, "frequencies_iter_prefix_v1")
    else:
        monkeypatch.setattr(_native, "frequencies_iter_prefix_v1", lambda iterator: None)
    result = flow.from_numpy(np.arange(1000, dtype="int64")).run_with_report("frequencies")
    assert list(result.value.items()) == [(value, 1) for value in range(1000)]
    assert calls == ["execute"]
    assert result.report.strategy != "rust_direct"


@pytest.mark.parametrize("error_type", [TypeError, OverflowError])
@pytest.mark.parametrize("dtype", ["int64", "float64"])
def test_numpy_frequency_iterator_keeps_automatic_source_fallback(
    error_type: type[Exception], dtype: str, monkeypatch: pytest.MonkeyPatch
) -> None:

    import numpy as np

    from fpstreams.execution import native
    from fpstreams.planning.source import Source

    calls = []
    original_open = Source.open

    def execute(program):
        calls.append("native")
        raise error_type("fallback")

    def open_source(self):
        calls.append("python")
        return original_open(self)

    monkeypatch.setattr(native, "execute", execute)
    monkeypatch.setattr(Source, "open", open_source)
    result = flow.from_numpy(np.arange(1000, dtype=dtype)).frequencies()
    assert list(result.items()) == [(value, 1) for value in range(1000)]
    assert calls == ["native", "python"]


@pytest.mark.parametrize("engine", ["python", "auto", "native"])
def test_numpy_frequency_iterator_keeps_nan_payloads_and_signed_zero(engine: str) -> None:

    import numpy as np

    bits = ["8000000000000000", "0000000000000000", "7ff8000000000001", "fff8000000000002"]
    values = [struct.unpack("!d", bytes.fromhex(value))[0] for value in bits] * 300
    result = flow.from_numpy(np.array(values)).with_engine(engine).frequencies()
    actual = [(struct.pack("!d", key).hex(), count) for key, count in result.items()]
    assert actual == [(bits[0], 600)] + [(value, 1) for value in bits[2:] * 300]


@pytest.mark.parametrize("engine", ["python", "native"])
@pytest.mark.parametrize(
    "failure_kind",
    ["factory_stop", "iter_stop", "next_error", "unbound_error", "early_close", "unopened_close"],
)
def test_physical_forwarding_preserves_error_conversion_and_delegate_release(
    engine: str, failure_kind: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fpstreams.runtime.query import QueryRuntime

    failure = StopIteration("factory") if failure_kind.endswith("stop") else RuntimeError("next")
    events = []
    closed_errors = []

    class Delegate:
        def __iter__(self):
            events.append("iter")
            if failure_kind == "iter_stop":
                raise failure
            return self

        def __next__(self):
            events.append("next")
            if failure_kind == "next_error":
                raise failure
            return 1

        def close(self):
            events.append("delegate_close")

        def __del__(self):
            events.append("released")

    if failure_kind == "unbound_error":

        def unbound_next():
            events.append("next")
            raise failure

        Delegate.__next__ = staticmethod(unbound_next)

    def execute(*args, **kwargs):
        events.append("factory")
        if failure_kind == "factory_stop":
            raise failure
        return Delegate()

    original_close = QueryRuntime.close

    def close(self, active_error=None):
        closed_errors.append(active_error)
        events.append("runtime_close")
        return original_close(self, active_error)

    source = flow(range(1000)).map(item + 1).with_engine(engine)
    plan = compile_query(source._query("iterate"))
    monkeypatch.setattr(
        physical, "execute_operations" if engine == "python" else "execute", execute
    )
    monkeypatch.setattr(QueryRuntime, "close", close)
    iterator = physical.execute_physical(plan)
    assert events == []
    if failure_kind == "unopened_close":
        iterator.close()
        assert closed_errors == [None]
    elif failure_kind == "early_close":
        assert next(iterator) == 1
        iterator.close()
        assert closed_errors == [None]
    else:
        with pytest.raises(RuntimeError) as caught:
            next(iterator)
        error = caught.value
        assert closed_errors == [error]
        if failure_kind.endswith("stop"):
            assert str(error) == "generator raised StopIteration"
            assert error.__cause__ is failure
        else:
            assert error is failure
    assert (
        events
        == {
            "factory_stop": ["factory", "runtime_close"],
            "iter_stop": ["factory", "iter", "runtime_close"],
            "next_error": ["factory", "iter", "next", "runtime_close"],
            "unbound_error": ["factory", "iter", "next", "released", "runtime_close"],
            "early_close": [
                "factory",
                "iter",
                "next",
                "delegate_close",
                "released",
                "runtime_close",
            ],
            "unopened_close": ["runtime_close"],
        }[failure_kind]
    )


@pytest.mark.parametrize("kind", ["factory_stop", "next_error", "early_close"])
def test_physical_forwarding_retains_arrow_sort_delegate_on_error(
    kind: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    import pyarrow as pa

    from fpstreams import rows
    from fpstreams.execution import arrow, physical
    from fpstreams.runtime.query import QueryRuntime

    events, errors = [], []
    failure = StopIteration("sort factory") if kind == "factory_stop" else RuntimeError("sort next")

    def advance():
        events.append("next")
        if kind == "next_error":
            raise failure
        return {"key": 1}

    class Delegate:
        __next__ = staticmethod(advance)

        def __iter__(self):
            events.append("iter")
            return self

        def close(self):
            events.append("delegate_close")

        def __del__(self):
            events.append("released")

    def open_sort(plan):
        events.append("factory")
        if kind == "factory_stop":
            raise failure
        return Delegate()

    original_close = QueryRuntime.close

    def close(self, error=None):
        events.append("runtime_close")
        errors.append(error)
        return original_close(self, error)

    plan = compile_query(
        rows.from_arrow(pa.table({"key": [2, 1]})).sort_by("key")._flow._query("list")
    )
    monkeypatch.setattr(arrow, "try_retained_arrow_stable_sort", open_sort)
    monkeypatch.setattr(QueryRuntime, "close", close)
    iterator = physical.execute_physical(plan)
    assert events == []
    if kind == "early_close":
        assert next(iterator) == {"key": 1}
        iterator.close()
        assert errors == [None]
        assert events == ["factory", "iter", "next", "delegate_close", "released", "runtime_close"]
    else:
        with pytest.raises(RuntimeError) as caught:
            next(iterator)
        assert errors == [caught.value]
        if kind == "factory_stop":
            assert caught.value.__cause__ is failure
            assert str(caught.value) == "generator raised StopIteration"
            assert events == ["factory", "runtime_close"]
        else:
            assert caught.value is failure
            assert events == ["factory", "iter", "next", "runtime_close"]


def _key(value: str) -> PlanCacheKey:
    return PlanCacheKey(value, "list", (True, 10, True))


def test_plan_cache_is_bounded_and_uses_structural_keys() -> None:
    cache = PlanCache[str](max_entries=2)
    cache.put(_key("a"), PhysicalPlanTemplate("first"))

    assert cache.get(_key("a")) == PhysicalPlanTemplate("first")
    cache.put(_key("b"), PhysicalPlanTemplate("second"))
    cache.put(_key("c"), PhysicalPlanTemplate("third"))
    assert cache.get(_key("a")) is None
    assert len(cache) == 2

    # A hit must protect the entry when a later insertion exceeds capacity.
    assert cache.get(_key("b")) == PhysicalPlanTemplate("second")
    cache.put(_key("d"), PhysicalPlanTemplate("fourth"))
    assert cache.get(_key("c")) is None
    assert cache.get(_key("b")) == PhysicalPlanTemplate("second")
    assert len(cache) == 2


def test_noncacheable_template_is_not_retained() -> None:
    cache = PlanCache[str]()
    cache.put(_key("callback"), PhysicalPlanTemplate("opaque", cacheable=False))

    assert cache.get(_key("callback")) is None


def test_compiler_binds_cached_expression_template_to_the_current_source() -> None:
    first_flow = flow(range(10)).map(item * 7919 + 123).with_engine("python")
    second_flow = flow(range(20)).map(item * 7919 + 123).with_engine("python")

    first = compile_query(first_flow._query("list"))
    second = compile_query(second_flow._query("list"))

    assert first.cacheable and not first.cache_hit
    assert second.cacheable and second.cache_hit
    assert second.source is second_flow._pipeline.source
    assert second_flow.to_list() == [value * 7919 + 123 for value in range(20)]


def test_consume_physical_directly_owns_streaming_python_iteration(monkeypatch) -> None:
    """A sink can consume a selected Python plan without iterator forwarding layers."""
    from fpstreams.planning.compiler import compile_iteration

    closed = False

    def source() -> Iterator[int]:
        nonlocal closed
        try:
            yield from range(6)
        finally:
            closed = True

    query = flow(source()).map(lambda value: value * 2).filter(lambda value: value % 4 == 0)
    plan = compile_iteration(query._query("iterate"))

    def unexpected_forwarding(*_args: object, **_kwargs: object) -> Iterator[object]:
        raise AssertionError("selected Python iteration must be consumed directly")

    monkeypatch.setattr(physical, "execute_physical", unexpected_forwarding)

    assert physical.consume_physical(plan, list) == [0, 4, 8]
    assert closed


def test_callable_map_filter_run_does_not_stack_builtin_iterators(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A common callable run executes in one loop instead of nested map/filter objects."""
    from fpstreams.execution import sync

    def reject_stacked_iterator(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("callable map/filter run used a stacked builtin iterator")

    monkeypatch.setattr(sync, "map", reject_stacked_iterator, raising=False)
    monkeypatch.setattr(sync, "filter", reject_stacked_iterator, raising=False)

    result = (
        flow(iter(range(8)))
        .with_engine("python")
        .map(lambda value: value * 3 + 1)
        .filter(lambda value: value % 2 == 0)
        .to_list()
    )

    assert result == [4, 10, 16, 22]


def test_consume_physical_falls_back_for_a_non_python_backend(monkeypatch) -> None:
    """A non-Python decision remains owned by the canonical physical executor."""
    from dataclasses import replace

    from fpstreams.physical.plan import BackendPayload
    from fpstreams.planning.compiler import compile_iteration
    from fpstreams.planning.native import EngineDecision

    plan = compile_iteration(flow([1])._query("iterate"))
    plan = replace(
        plan,
        backend_payload=BackendPayload(EngineDecision("native", "test fallback")),
    )
    calls = 0

    def execute(_plan: object) -> Iterator[int]:
        nonlocal calls
        calls += 1
        return iter((7,))

    monkeypatch.setattr(physical, "execute_physical", execute)

    assert physical.consume_physical(plan, list) == [7]
    assert calls == 1


def test_consume_physical_preserves_source_open_stop_iteration_boundary() -> None:
    """Direct sinks retain the generator boundary around a failing source opener."""
    from fpstreams.planning.compiler import compile_iteration
    from fpstreams.planning.source import Source

    failure = StopIteration("source open stopped")

    def stopped_open() -> Iterator[int]:
        raise failure

    query = fpstreams.Flow(
        Source(stopped_open, SourceCapabilities(reiterable=True, exact_size=None))
    ).with_engine("python")
    plan = compile_iteration(query._query("iterate"))

    with pytest.raises(RuntimeError, match=r"^generator raised StopIteration$") as caught:
        physical.consume_physical(plan, list)

    assert caught.value.__cause__ is failure


def test_row_expression_cache_never_reuses_bound_literal_or_source_objects() -> None:
    """Structurally equal row graphs retain the current literal identity and source values."""
    first_literal = ("value",)
    second_literal = tuple(["value"])
    assert first_literal == second_literal and first_literal is not second_literal

    first = flow([{"value": 1}]).map(lit(first_literal)).with_engine("python")
    second = flow([{"value": 2}]).map(lit(second_literal)).with_engine("python")

    first_plan = compile_query(first._query("list"))
    second_plan = compile_query(second._query("list"))

    assert next(iter(execute_physical(first_plan))) is first_literal
    assert next(iter(execute_physical(second_plan))) is second_literal
    assert first.to_list()[0] is first_literal
    assert second.to_list()[0] is second_literal
    assert not first_plan.cacheable
    assert not second_plan.cacheable
    assert flow([{"value": 2}]).map(col("value")).to_list() == [2]


def test_rows_where_keeps_a_pure_row_expression_visible_to_the_compiler() -> None:
    """A predicate-only Rows.where must not hide closed IR behind a Python closure."""
    from fpstreams import rows

    query = rows([{"value": 1}, {"value": 2}]).where(col("value") > 1)._flow
    plan = compile_query(query._query("list"))

    assert isinstance(plan.nodes[0], CompiledExpressionPhysicalNode)
    assert query.to_list() == [{"value": 2}]


def test_rows_exact_map_stages_retain_query_bound_structural_descriptors() -> None:
    """with_columns/select metadata must retain this query's exact selector identities."""

    from fpstreams import rows
    from fpstreams.planning import arrow as arrow_planning

    score = col("x") * 3 + col("y") - 1
    rows_query = rows([{"x": 1, "y": 2}]).with_columns(score=score).where(score=4)
    query = rows_query.select("x", "score")._flow
    first, middle, last = query._pipeline.operations

    assert type(first.function) is FunctionType
    _row_stage_descriptor = arrow_planning._row_stage_descriptor
    first_descriptor = _row_stage_descriptor(first.function)
    assert first_descriptor is not None
    assert first_descriptor.kind == "with_columns"
    assert first_descriptor.selectors[0][0] == "score"
    assert first_descriptor.selectors[0][1] is score
    assert type(middle.predicate) is FunctionType
    middle_descriptor = _row_stage_descriptor(middle.predicate)
    assert middle_descriptor is not None
    assert middle_descriptor.kind == "where"
    assert middle_descriptor.equalities == (("score", 4),)
    assert type(last.function) is FunctionType
    last_descriptor = _row_stage_descriptor(last.function)
    assert last_descriptor is not None
    assert last_descriptor.kind == "select"
    assert last_descriptor.selectors == (("x", "x"), ("score", "score"))

    def user_function(row: object) -> object:
        return row

    user_function.descriptor = first_descriptor  # type: ignore[attr-defined]
    assert _row_stage_descriptor(user_function) is None


def test_pair_side_maps_retain_trusted_query_bound_descriptors() -> None:
    """Internal pair wrappers expose their exact side and original callback by identity."""
    from fpstreams.planning.pairs import PairMapDescriptor

    def map_key(value: str) -> str:
        return value.upper()

    def map_value(value: int) -> int:
        return value * 2

    def map_pair(key: str, value: int) -> tuple[str, int]:
        return key.upper(), value * 3

    key_operation = fpstreams.pairs([("a", 1)]).map_keys(map_key).to_flow()._pipeline.operations[0]
    value_operation = (
        fpstreams.pairs([("a", 1)]).map_values(map_value).to_flow()._pipeline.operations[0]
    )
    pair_operation = (
        fpstreams.pairs([("a", 1)]).map_pairs(map_pair).to_flow()._pipeline.operations[0]
    )
    key_descriptor = key_operation.function
    value_descriptor = value_operation.function
    pair_descriptor = pair_operation.function

    assert type(key_descriptor) is PairMapDescriptor
    assert key_descriptor.side == "key"
    assert key_descriptor.callback is map_key
    assert key_descriptor(("a", 1)) == ("A", 1)
    assert type(value_descriptor) is PairMapDescriptor
    assert value_descriptor.side == "value"
    assert value_descriptor.callback is map_value
    assert value_descriptor(("a", 1)) == ("a", 2)
    assert type(pair_descriptor) is PairMapDescriptor
    assert pair_descriptor.side == "pair"
    assert pair_descriptor.callback is map_pair
    assert pair_descriptor(("a", 1)) == ("A", 3)


def test_pair_side_map_executor_skips_adapter_and_preserves_stop_iteration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Structured execution calls the original callback and retains map exhaustion semantics."""
    from fpstreams.execution.sync import open_operations
    from fpstreams.planning.pairs import PairMapDescriptor
    from fpstreams.planning.sync import MapOp

    calls: list[int] = []

    def callback(value: int) -> int:
        calls.append(value)
        if value == 2:
            raise StopIteration
        return value * 10

    def unexpected_adapter(
        _self: PairMapDescriptor,
        _pair: tuple[str, int],
    ) -> tuple[str, int]:
        raise AssertionError("trusted pair adapter should not run on the Python fast path")

    descriptor = PairMapDescriptor("value", callback)
    monkeypatch.setattr(PairMapDescriptor, "__call__", unexpected_adapter)
    with open_operations(
        iter((("a", 1), ("b", 2), ("c", 3))),
        (MapOp(descriptor),),
    ) as values:
        assert list(values) == [("a", 10)]

    assert calls == [1, 2]

    pair_calls: list[tuple[str, int]] = []

    def pair_callback(key: str, value: int) -> tuple[str, int]:
        pair_calls.append((key, value))
        if value == 2:
            raise StopIteration
        return key.upper(), value * 10

    pair_descriptor = PairMapDescriptor("pair", pair_callback)
    with open_operations(
        iter((("a", 1), ("b", 2), ("c", 3))),
        (MapOp(pair_descriptor),),
    ) as values:
        assert list(values) == [("A", 10)]

    assert pair_calls == [("a", 1), ("b", 2)]


def test_pair_side_expressions_compile_once_per_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pair-side scalar expressions resolve one evaluator instead of one per input row."""
    from fpstreams.expressions.scalar import Expr, FExpr

    integer_compilations = 0
    float_compilations = 0
    original_integer = Expr._python_evaluator
    original_float = FExpr._python_evaluator

    def track_integer(self: Expr) -> object:
        nonlocal integer_compilations
        integer_compilations += 1
        return original_integer(self)

    def track_float(self: FExpr) -> object:
        nonlocal float_compilations
        float_compilations += 1
        return original_float(self)

    monkeypatch.setattr(Expr, "_python_evaluator", track_integer)
    monkeypatch.setattr(FExpr, "_python_evaluator", track_float)

    source = [(0, 1), (1, 2), (2, 3)]
    mapped = fpstreams.pairs(source).map_values(fpstreams.item * 2 + 1).with_engine("python")
    assert list(mapped) == [(0, 3), (1, 5), (2, 7)]
    assert mapped.to_dict(on_duplicate="last") == {0: 3, 1: 5, 2: 7}
    assert integer_compilations == 2

    filtered = fpstreams.pairs(source).filter_values(fpstreams.fitem > 1.5).with_engine("python")
    assert list(filtered) == [(1, 2), (2, 3)]
    assert filtered.to_dict(on_duplicate="last") == {1: 2, 2: 3}
    assert float_compilations == 2


@pytest.mark.parametrize("keep_first, expected", [(False, 6), (True, 4)])
def test_native_pair_value_map_preserves_duplicate_policy_and_first_key_identity(
    keep_first: bool,
    expected: int,
) -> None:
    """The exact-pair map sink evaluates every value and retains Python dict key identity."""
    from fpstreams import _native

    # Keep equal exact integers outside CPython's small-integer cache.
    first_key = int("1000000")
    equal_key = int("1000000")
    assert type(first_key) is type(equal_key) is int
    assert first_key is not equal_key
    output: dict[object, object] = {}

    boundary = _native.pair_i64_value_map_to_dict_exact_prefix_v1(
        output,
        iter(((first_key, 2), (equal_key, 3))),
        ((0, 0), (1, 2), (4, 0)),
        keep_first,
        False,
    )

    assert boundary == (None, True)
    assert output == {1_000_000: expected}
    assert next(iter(output)) is first_key


@pytest.mark.parametrize("container", [list, tuple], ids=["list", "tuple"])
@pytest.mark.parametrize("policy", ["first", "last"])
@pytest.mark.parametrize("expression_kind", ["integer", "float"])
def test_pair_value_expression_map_uses_native_sink_across_supported_shapes(
    monkeypatch: pytest.MonkeyPatch,
    container: type[list[tuple[int, int]]] | type[tuple[tuple[int, int], ...]],
    policy: str,
    expression_kind: str,
) -> None:
    """Both scalar families preserve pair policies for exact retained lists and tuples."""
    from fpstreams import _native, fitem

    endpoint_name = (
        "pair_i64_value_map_to_dict_exact_prefix_v1"
        if expression_kind == "integer"
        else "pair_f64_value_map_to_dict_exact_prefix_v1"
    )
    endpoint = getattr(_native, endpoint_name)
    calls = 0

    def tracked(*arguments: object) -> object:
        nonlocal calls
        calls += 1
        return endpoint(*arguments)

    monkeypatch.setattr(_pair_dict, "_PAIR_VALUE_MAP_MIN_ROWS", 0)
    monkeypatch.setattr(_native, endpoint_name, tracked)
    source = container(((1, 2), (1, 3), (2, 4)))
    expression = item * 2 if expression_kind == "integer" else fitem / 2.0

    result = fpstreams.pairs(source).map_values(expression).to_dict(on_duplicate=policy)

    if expression_kind == "integer":
        assert result == ({1: 4, 2: 8} if policy == "first" else {1: 6, 2: 8})
    else:
        assert result == ({1: 1.0, 2: 2.0} if policy == "first" else {1: 1.5, 2: 2.0})
    assert calls == 1


@pytest.mark.parametrize("container", [list, tuple], ids=["list", "tuple"])
@pytest.mark.parametrize("policy", ["first", "last"])
@pytest.mark.parametrize(
    "boundary",
    ["list-row", "tuple-subclass", "i64-overflow", "f64-overflow"],
)
def test_pair_value_map_declines_known_first_row_boundaries_before_native(
    monkeypatch: pytest.MonkeyPatch,
    container: type[list[object]] | type[tuple[object, ...]],
    policy: str,
    boundary: str,
) -> None:
    """An O(1) exact-row probe avoids a native transition known to stop immediately."""
    from fpstreams import _native, fitem

    class Pair(tuple[object, object]):
        pass

    monkeypatch.setattr(_pair_dict, "_PAIR_VALUE_MAP_MIN_ROWS", 0)
    if boundary == "list-row":
        values = container(([1, 2], (2, 3)))
        expression = item * 2
        expected = {1: 4, 2: 6}
    elif boundary == "tuple-subclass":
        values = container((Pair((1, 2)), (2, 3)))
        expression = item * 2
        expected = {1: 4, 2: 6}
    elif boundary == "i64-overflow":
        values = container(((1, 1 << 70), (2, 3)))
        expression = item * 2
        expected = {1: 1 << 71, 2: 6}
    else:
        values = container(((1, 1 << 1024), (2, 3)))
        expression = fitem * 2.0
        expected = None

    def unexpected(*_arguments: object) -> object:
        raise AssertionError("a known first-row boundary must not enter Rust")

    monkeypatch.setattr(_native, "pair_i64_value_map_to_dict_exact_prefix_v1", unexpected)
    monkeypatch.setattr(_native, "pair_f64_value_map_to_dict_exact_prefix_v1", unexpected)
    entries = fpstreams.pairs(values).map_values(expression)
    if boundary == "f64-overflow":
        with pytest.raises(OverflowError):
            entries.to_dict(on_duplicate=policy)  # type: ignore[arg-type]
    else:
        assert entries.to_dict(on_duplicate=policy) == expected  # type: ignore[arg-type]


def test_pair_value_map_declines_replaced_first_row_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Replacing the internal capability probe cannot authorize native execution."""
    from fpstreams import _native

    def unexpected(*_arguments: object) -> object:
        raise AssertionError("a replaced capability probe must disable the native sink")

    monkeypatch.setattr(_pair_dict, "_PAIR_VALUE_MAP_MIN_ROWS", 0)
    monkeypatch.setattr(_pair_dict, "_pair_value_map_first_row_compatible", lambda *_args: True)
    monkeypatch.setattr(_native, "pair_i64_value_map_to_dict_exact_prefix_v1", unexpected)

    assert fpstreams.pairs([(1, 2), (2, 3)]).map_values(item * 2).to_dict(on_duplicate="last") == {
        1: 4,
        2: 6,
    }


@pytest.mark.parametrize("policy", ["first", "last"])
def test_pair_value_map_resumes_canonical_protocol_at_nonexact_boundary(
    monkeypatch: pytest.MonkeyPatch,
    policy: str,
) -> None:
    """A nonexact row reaches Python before indexing, hashing, or equality can be observed."""
    import fpstreams

    monkeypatch.setattr(_pair_dict, "_PAIR_VALUE_MAP_MIN_ROWS", 0)

    def evaluate(engine: str) -> tuple[list[tuple[str, int]], tuple[object, ...]]:
        events: list[object] = []

        class Key:
            def __hash__(self) -> int:
                events.append("hash")
                return 1

            def __eq__(self, other: object) -> bool:
                events.append(("eq", type(other).__name__, other if type(other) is int else None))
                return False

        class Row:
            def __getitem__(self, index: int) -> object:
                events.append(("get", index))
                return Key() if index == 0 else 3

        query = fpstreams.pairs([(1, 1), (2, 2), Row(), (3, 4)]).map_values(item * 2)
        if engine == "python":
            query = query.with_engine("python")
        output = query.to_dict(on_duplicate=policy)
        normalized = [(type(key).__name__, value) for key, value in output.items()]
        return normalized, tuple(events)

    assert evaluate("auto") == evaluate("python")


def test_pair_value_map_preserves_identity_overflow_and_live_source_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Identity maps retain objects while an i64 boundary and opener mutation resume in Python."""
    from fpstreams import fitem

    monkeypatch.setattr(_pair_dict, "_PAIR_VALUE_MAP_MIN_ROWS", 0)
    integer = int("1000000")
    assert type(integer) is int and integer is not int("1000000")
    floating = float("2.5")
    integers = [("identity", integer), ("overflow", 1 << 70)]
    integer_query = fpstreams.pairs(integers).map_values(item)
    source = integer_query._flow._pipeline.source
    original_factory = source._factory

    def open_source() -> Iterator[tuple[str, int]]:
        integers.append(("appended", 3))
        return original_factory()

    monkeypatch.setattr(source, "_factory", open_source)
    integer_output = integer_query.to_dict(on_duplicate="last")
    float_output = (
        fpstreams.pairs([("float", floating)]).map_values(fitem).to_dict(on_duplicate="last")
    )

    assert integer_output == {"identity": 1_000_000, "overflow": 1 << 70, "appended": 3}
    assert integer_output["identity"] is integer
    assert float_output["float"] is floating


def test_pair_value_map_native_boundary_preserves_numeric_exceptions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Native evaluation failures resume at the untouched row and raise canonically."""
    from fpstreams import _native, fitem

    endpoint_names = (
        "pair_i64_value_map_to_dict_exact_prefix_v1",
        "pair_f64_value_map_to_dict_exact_prefix_v1",
    )
    calls: list[str] = []
    for endpoint_name in endpoint_names:
        endpoint = getattr(_native, endpoint_name)

        def tracked(*arguments: object, _name: str = endpoint_name, _endpoint=endpoint) -> object:
            calls.append(_name)
            return _endpoint(*arguments)

        monkeypatch.setattr(_native, endpoint_name, tracked)

    monkeypatch.setattr(_pair_dict, "_PAIR_VALUE_MAP_MIN_ROWS", 0)
    for expression in (item // 0, fitem / 0.0):
        failures: list[tuple[type[BaseException], str]] = []
        for engine in ("auto", "python"):
            entries = fpstreams.pairs([(1, 2)]).map_values(expression)
            if engine == "python":
                entries = entries.with_engine("python")
            with pytest.raises(ZeroDivisionError) as raised:
                entries.to_dict(on_duplicate="last")
            failures.append((type(raised.value), str(raised.value)))
        assert failures[0] == failures[1]

    assert calls == list(endpoint_names)


def test_pair_value_map_fexpr_boolean_arithmetic_preserves_python_result_types(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Float expressions that canonically produce integers must remain in Python."""
    from fpstreams import _native, fitem

    endpoint = _native.pair_f64_value_map_to_dict_exact_prefix_v1
    calls = 0

    def tracked(*arguments: object) -> object:
        nonlocal calls
        calls += 1
        return endpoint(*arguments)

    monkeypatch.setattr(_pair_dict, "_PAIR_VALUE_MAP_MIN_ROWS", 0)
    monkeypatch.setattr(_native, "pair_f64_value_map_to_dict_exact_prefix_v1", tracked)
    expressions = (
        -(fitem > 0.0),
        abs(fitem > 0.0),
        (fitem > 0.0) + (fitem < 4.0),
        (fitem > 0.0) - (fitem < 4.0),
        (fitem > 0.0) * (fitem < 4.0),
    )

    for expression in expressions:
        automatic = (
            fpstreams.pairs([("value", 2.0)]).map_values(expression).to_dict(on_duplicate="last")
        )
        canonical = (
            fpstreams.pairs([("value", 2.0)])
            .map_values(expression)
            .with_engine("python")
            .to_dict(on_duplicate="last")
        )
        assert automatic == canonical
        assert type(automatic["value"]) is type(canonical["value"]) is int

    assert calls == 0


def test_pair_value_map_fexpr_safe_float_and_boolean_results_stay_native(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Terminal booleans and unambiguous float arithmetic retain native execution."""
    from fpstreams import _native, fitem

    endpoint = _native.pair_f64_value_map_to_dict_exact_prefix_v1
    calls = 0

    def tracked(*arguments: object) -> object:
        nonlocal calls
        calls += 1
        return endpoint(*arguments)

    monkeypatch.setattr(_pair_dict, "_PAIR_VALUE_MAP_MIN_ROWS", 0)
    monkeypatch.setattr(_native, "pair_f64_value_map_to_dict_exact_prefix_v1", tracked)
    lanes = (
        (fitem > 0.0, bool),
        ((fitem > 0.0) & (fitem < 4.0), bool),
        (fitem * 2.0 + 1.0, float),
        ((fitem > 0.0) + fitem, float),
        ((fitem > 0.0) / (fitem < 4.0), float),
    )

    for expression, expected_type in lanes:
        result = (
            fpstreams.pairs([("value", 2.0)]).map_values(expression).to_dict(on_duplicate="last")
        )
        assert type(result["value"]) is expected_type

    assert calls == len(lanes)


def test_pair_value_map_declines_native_sink_for_failpoints(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Failpoints retain the established Python value-map loop."""
    from fpstreams import _native
    from fpstreams.runtime.failpoints import failpoint

    endpoint = _native.pair_i64_value_map_to_dict_exact_prefix_v1

    def unexpected(*_arguments: object) -> object:
        raise AssertionError("runtime observers must bypass the native pair-map sink")

    monkeypatch.setattr(_pair_dict, "_PAIR_VALUE_MAP_MIN_ROWS", 0)
    monkeypatch.setattr(_native, "pair_i64_value_map_to_dict_exact_prefix_v1", unexpected)
    query = fpstreams.pairs([(1, 2), (2, 3)]).map_values(item * 2)

    with failpoint("unrelated.pair-map.transition", RuntimeError("unused")):
        result = query.to_dict(on_duplicate="last")

    monkeypatch.setattr(_native, "pair_i64_value_map_to_dict_exact_prefix_v1", endpoint)
    assert result == {1: 4, 2: 6}


@pytest.mark.parametrize("guard", ["endpoint", "operation"])
def test_pair_value_map_rechecks_live_guards_after_source_open(
    monkeypatch: pytest.MonkeyPatch,
    guard: str,
) -> None:
    """Source opening cannot leave a stale native endpoint or scalar expression snapshot."""
    from fpstreams import _native
    from fpstreams.planning.pairs import PairMapDescriptor

    monkeypatch.setattr(_pair_dict, "_PAIR_VALUE_MAP_MIN_ROWS", 0)
    values = [(1, 2), (2, 3)]
    query = fpstreams.pairs(values).map_values(item * 2)
    source = query._flow._pipeline.source
    operation = query._flow._pipeline.operations[0]
    assert isinstance(operation.function, PairMapDescriptor)

    def unexpected(*_arguments: object) -> object:
        raise AssertionError("a source-time endpoint replacement must not be called")

    def open_source() -> Iterator[tuple[int, int]]:
        if guard == "endpoint":
            monkeypatch.setattr(
                _native,
                "pair_i64_value_map_to_dict_exact_prefix_v1",
                unexpected,
            )
        else:
            object.__setattr__(operation.function, "callback", item * 3)
        return iter(values)

    monkeypatch.setattr(source, "_factory", open_source)

    assert query.to_dict(on_duplicate="last") == (
        {1: 4, 2: 6} if guard == "endpoint" else {1: 6, 2: 9}
    )


def test_pair_value_map_explicitly_declines_duplicate_error_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The native first/last sink cannot pre-empt canonical duplicate errors."""
    from fpstreams import _native

    calls = 0

    def unexpected(*_arguments: object) -> object:
        nonlocal calls
        calls += 1
        raise AssertionError("the error policy must stay on the canonical Python sink")

    monkeypatch.setattr(_pair_dict, "_PAIR_VALUE_MAP_MIN_ROWS", 0)
    monkeypatch.setattr(_native, "pair_i64_value_map_to_dict_exact_prefix_v1", unexpected)

    assert fpstreams.pairs([(1, 2), (2, 3)]).map_values(item * 2).to_dict() == {1: 4, 2: 6}
    with pytest.raises(fpstreams.DuplicateKeyError, match=r"^Duplicate key: 1$"):
        fpstreams.pairs([(1, 2), (1, 3)]).map_values(item * 2).to_dict()
    assert calls == 0


@pytest.mark.skipif(sys.platform == "win32", reason="setitimer is Unix-only")
def test_pair_value_map_native_signal_mutation_reaches_the_open_list_iterator() -> None:
    """A signal-time append remains visible to the already-open native iterator."""

    from fpstreams.execution.pairs.dictionary import try_consume_pair_value_map_to_dict_opened

    values: list[tuple[object, int]] = [(index % 8, 0) for index in range(500_000)]
    late = ("late", 2)
    previous = signal.getsignal(signal.SIGALRM)
    appended = False

    def append_once(_signum: int, frame: object) -> None:
        nonlocal appended
        if (
            not appended
            and getattr(frame, "f_code", None) is try_consume_pair_value_map_to_dict_opened.__code__
        ):
            values.append(late)
            appended = True

    signal.signal(signal.SIGALRM, append_once)
    signal.setitimer(signal.ITIMER_REAL, 0.001, 0.001)
    try:
        result = fpstreams.pairs(values).map_values(item * 2).to_dict(on_duplicate="first")
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)

    assert appended
    assert result == {**{key: 0 for key in range(8)}, "late": 4}


@pytest.mark.skipif(sys.platform == "win32", reason="setitimer is Unix-only")
@pytest.mark.parametrize("engine", ["auto", "python"])
def test_pair_value_map_propagates_signal_keyboard_interrupt(engine: str) -> None:
    """Both native and canonical map sinks propagate asynchronous interruption."""

    from fpstreams.execution.pairs.dictionary import (
        _consume_mapped_values,
        try_consume_pair_value_map_to_dict_opened,
    )

    values = [(index, index) for index in range(1_000_000)]
    entries = fpstreams.pairs(values).map_values(item * 2)
    if engine == "python":
        entries = entries.with_engine("python")
    terminal_codes = {
        _consume_mapped_values.__code__,
        try_consume_pair_value_map_to_dict_opened.__code__,
    }
    previous = signal.getsignal(signal.SIGALRM)

    def interrupt(_signum: int, frame: object) -> None:
        if getattr(frame, "f_code", None) in terminal_codes:
            raise KeyboardInterrupt("timer stop")

    signal.signal(signal.SIGALRM, interrupt)
    signal.setitimer(signal.ITIMER_REAL, 0.002, 0.001)
    try:
        with pytest.raises(KeyboardInterrupt, match="timer stop"):
            entries.to_dict(on_duplicate="last")
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


@pytest.mark.skipif(sys.platform == "win32", reason="setitimer is Unix-only")
@pytest.mark.parametrize(
    ("endpoint", "extra"),
    [
        ("terminal_i64_range", (0,)),
        ("count_f64_range", ()),
        ("mean_i64_range", ()),
        ("mean_f64_range", ()),
        ("statistics_i64_range", ()),
        ("aggregate_f64_range", ()),
    ],
)
def test_native_range_kernels_poll_pending_signals(endpoint: str, extra: tuple[int, ...]) -> None:
    """A bounded subprocess prevents an uninterruptible native regression hanging pytest."""
    import subprocess

    source = str(Path(fpstreams.__file__).resolve().parent.parent)
    code = f"""
import signal
import sys
sys.path.insert(0, {source!r})
from fpstreams import _native
def interrupt(signum, frame):
    raise KeyboardInterrupt('range interrupted')
signal.signal(signal.SIGALRM, interrupt)
signal.setitimer(signal.ITIMER_REAL, 0.02)
try:
    getattr(_native, {endpoint!r})(0, 10**12, 1, [], *{extra!r})
except KeyboardInterrupt as error:
    assert str(error) == 'range interrupted'
    print('interrupted')
else:
    raise AssertionError('range unexpectedly finished without interruption')
finally:
    signal.setitimer(signal.ITIMER_REAL, 0)
"""
    completed = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=10, check=True
    )
    assert completed.stdout.strip() == "interrupted"


def test_pair_value_map_post_open_failpoint_closes_the_source_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A post-open deopt retains exactly one cleanup owner for the live source."""
    from contextlib import AbstractContextManager

    from fpstreams import _native
    from fpstreams.runtime.failpoints import failpoint

    values = [(1, 2), (2, 3)]
    entries = fpstreams.pairs(values).map_values(item * 2)
    source = entries.to_flow()._pipeline.source
    error = RuntimeError("stop after pull")
    closes = 0

    def unexpected(*_arguments: object) -> object:
        raise AssertionError("an active failpoint must disable the native map sink")

    class OpenedIterator(Iterator[tuple[int, int]]):
        def __init__(self, scope: AbstractContextManager[None]) -> None:
            self._values = iter(values)
            self._scope = scope

        def __next__(self) -> tuple[int, int]:
            return next(self._values)

        def close(self) -> None:
            nonlocal closes
            closes += 1
            if closes != 1:
                raise AssertionError("source iterator was closed more than once")
            self._scope.__exit__(None, None, None)

    def open_with_failpoint() -> Iterator[tuple[int, int]]:
        scope = failpoint("iterator.pull.after", error)
        scope.__enter__()
        return OpenedIterator(scope)

    monkeypatch.setattr(_pair_dict, "_PAIR_VALUE_MAP_MIN_ROWS", 0)
    monkeypatch.setattr(_native, "pair_i64_value_map_to_dict_exact_prefix_v1", unexpected)
    monkeypatch.setattr(source, "_factory", open_with_failpoint)
    with pytest.raises(RuntimeError) as raised:
        entries.to_dict(on_duplicate="last")

    assert raised.value is error
    assert closes == 1


def test_pair_value_map_native_endpoint_stays_disabled_without_the_gil() -> None:
    """The shared native endpoint declines without consuming on free-threaded CPython."""
    if getattr(sys, "_is_gil_enabled", lambda: True)():
        pytest.skip("requires a free-threaded interpreter")

    from fpstreams import _native

    output: dict[object, object] = {}
    source = iter(((1, 2),))
    assert (
        _native.pair_i64_value_map_to_dict_exact_prefix_v1(
            output,
            source,
            ((0, 0),),
            False,
            False,
        )
        is None
    )
    assert output == {}
    assert list(source) == [(1, 2)]


def test_pair_side_map_releases_previous_input_before_next_source_pull() -> None:
    """The structured iterator retains no input pair beyond its mapped output."""
    from weakref import ReferenceType, ref

    from fpstreams.execution.sync import open_operations
    from fpstreams.planning.pairs import PairMapDescriptor
    from fpstreams.planning.sync import MapOp

    class Pair:
        def __init__(self, key: str, value: int) -> None:
            self.values = (key, value)

        def __getitem__(self, index: int) -> str | int:
            return self.values[index]

    class Source(Iterator[Pair]):
        def __init__(self) -> None:
            self.index = 0
            self.previous: ReferenceType[Pair] | None = None
            self.retained_before_pull: list[bool] = []

        def __next__(self) -> Pair:
            if self.previous is not None:
                self.retained_before_pull.append(self.previous() is not None)
            if self.index == 2:
                raise StopIteration
            pair = Pair(chr(ord("a") + self.index), self.index)
            self.index += 1
            self.previous = ref(pair)
            return pair

    source = Source()
    operation = MapOp(PairMapDescriptor("value", lambda value: value))
    with open_operations(source, (operation,)) as values:
        first = next(values)
        del first
        assert source.previous is not None
        assert source.previous() is None
        next(values)

    assert source.retained_before_pull == [False]


def test_pair_flat_map_retains_trusted_descriptor_and_skips_adapter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pair flat-map expands arguments directly on its exact internal fast path."""
    from fpstreams.execution.sync import open_operations
    from fpstreams.planning.pairs import PairFlatMapDescriptor
    from fpstreams.planning.sync import FlatMapOp

    calls: list[tuple[str, int]] = []

    def callback(key: str, value: int) -> tuple[tuple[str, int], ...]:
        calls.append((key, value))
        return ((key, value), (key, value * 10))

    operation = (
        fpstreams.pairs([("a", 1)]).flat_map_pairs(callback).to_flow()._pipeline.operations[0]
    )
    descriptor = operation.function
    assert type(descriptor) is PairFlatMapDescriptor
    assert descriptor.callback is callback
    assert tuple(descriptor(("a", 1))) == (("a", 1), ("a", 10))
    calls.clear()

    def unexpected_adapter(
        _self: PairFlatMapDescriptor,
        _pair: tuple[str, int],
    ) -> object:
        raise AssertionError("trusted pair flat-map adapter should not run")

    monkeypatch.setattr(PairFlatMapDescriptor, "__call__", unexpected_adapter)
    with open_operations(iter((("a", 1), ("b", 2))), (FlatMapOp(descriptor),)) as values:
        assert list(values) == [("a", 1), ("a", 10), ("b", 2), ("b", 20)]
    assert calls == [("a", 1), ("b", 2)]


def test_pair_filters_retain_targets_and_preserve_access_order() -> None:
    """Each public pair filter exposes its exact target without touching another side."""
    import fpstreams
    from fpstreams.planning.pairs import PairFilterDescriptor

    class Pair:
        def __init__(self, events: list[str]) -> None:
            self.events = events

        def __getitem__(self, index: int) -> str | int:
            self.events.append(f"get:{index}")
            return "a" if index == 0 else 1

    class Truth:
        def __init__(self, events: list[str]) -> None:
            self.events = events

        def __bool__(self) -> bool:
            self.events.append("bool")
            return True

    def run(target: str) -> tuple[list[str], object, object]:
        events: list[str] = []
        pair = Pair(events)
        if target == "pair":

            def predicate(key: str, value: int) -> Truth:
                events.append(f"predicate:{key}:{value}")
                return Truth(events)

            filtered = fpstreams.pairs([pair]).filter_pairs(predicate)  # type: ignore[arg-type]
            callback: object = predicate
        elif target == "key":

            def predicate(key: str) -> Truth:
                events.append(f"predicate:{key}")
                return Truth(events)

            filtered = fpstreams.pairs([pair]).filter_keys(predicate)  # type: ignore[arg-type]
            callback = predicate
        else:

            def predicate(value: int) -> Truth:
                events.append(f"predicate:{value}")
                return Truth(events)

            filtered = fpstreams.pairs([pair]).filter_values(predicate)  # type: ignore[arg-type]
            callback = predicate

        operation = filtered.to_flow()._pipeline.operations[0]
        descriptor = operation.predicate
        assert type(descriptor) is PairFilterDescriptor
        assert descriptor.target == target
        assert descriptor.callback is callback
        assert bool(descriptor(pair))
        events.clear()
        assert list(filtered) == [pair]
        return events, pair, descriptor

    assert run("pair")[0] == ["get:0", "get:1", "predicate:a:1", "bool"]
    assert run("key")[0] == ["get:0", "predicate:a", "bool"]
    assert run("value")[0] == ["get:1", "predicate:1", "bool"]

    row_events: list[str] = []
    row = Pair(row_events)
    row_descriptor = PairFilterDescriptor("row", lambda candidate: candidate is row)
    assert row_descriptor(row) is True
    assert row_events == []


def test_pair_filter_accepts_a_row_expression_over_key_and_value() -> None:
    """A RowExpr receives the complete pair while ordinary callables keep two arguments."""
    from fpstreams.planning.pairs import PairFilterDescriptor

    expression = (fpstreams.col(0) + fpstreams.col(1)) % 2 == 0
    filtered = fpstreams.pairs([(1, 2), (2, 2), (3, 4)]).filter_pairs(expression)
    operation = filtered.to_flow()._pipeline.operations[0]

    assert type(operation.predicate) is PairFilterDescriptor
    assert operation.predicate.target == "row"
    assert operation.predicate.callback is expression
    assert filtered.with_engine("python").to_flow().to_list() == [(2, 2)]


def test_pair_filter_executor_skips_adapter_and_preserves_filter_exhaustion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Structured filtering bypasses its adapter and keeps truth and StopIteration semantics."""
    from fpstreams.execution.sync import open_operations
    from fpstreams.planning.pairs import PairFilterDescriptor
    from fpstreams.planning.sync import FilterOp

    def unexpected_adapter(
        _self: PairFilterDescriptor,
        _pair: tuple[str, int],
    ) -> object:
        raise AssertionError("trusted pair filter adapter should not run on the Python fast path")

    monkeypatch.setattr(PairFilterDescriptor, "__call__", unexpected_adapter)
    pair = ("a", 1)
    falsey_checks = 0

    class Falsey:
        def __bool__(self) -> bool:
            nonlocal falsey_checks
            falsey_checks += 1
            return False

    negated = FilterOp(PairFilterDescriptor("value", lambda _value: Falsey()), negate=True)
    with open_operations(iter((pair,)), (negated,)) as values:
        result = list(values)
    assert result == [pair]
    assert result[0] is pair
    assert falsey_checks == 1

    stop = StopIteration("predicate truth stopped")

    class StopsDuringTruth:
        def __bool__(self) -> bool:
            raise stop

    operation = FilterOp(PairFilterDescriptor("key", lambda _key: StopsDuringTruth()))
    with open_operations(iter((("a", 1), ("b", 2))), (operation,)) as values:
        assert list(values) == []


def test_pair_filter_truth_error_does_not_retain_its_input_pair() -> None:
    """A truth-test traceback must not keep the already-completed adapter input alive."""
    from weakref import ReferenceType, ref

    from fpstreams.execution.sync import open_operations
    from fpstreams.planning.pairs import PairFilterDescriptor
    from fpstreams.planning.sync import FilterOp

    class Pair:
        def __getitem__(self, index: int) -> str | int:
            return "a" if index == 0 else 1

    class Source(Iterator[Pair]):
        def __init__(self) -> None:
            self.reference: ReferenceType[Pair] | None = None
            self.emitted = False

        def __next__(self) -> Pair:
            if self.emitted:
                raise StopIteration
            self.emitted = True
            pair = Pair()
            self.reference = ref(pair)
            return pair

    primary = ValueError("truth failed")

    class BrokenTruth:
        def __bool__(self) -> bool:
            raise primary

    source = Source()
    operation = FilterOp(PairFilterDescriptor("value", lambda _value: BrokenTruth()))
    with pytest.raises(ValueError) as captured, open_operations(source, (operation,)) as values:
        next(values)

    assert captured.value is primary
    assert source.reference is not None
    assert source.reference() is None


def test_direct_planned_row_function_preserves_mapping_and_selector_order() -> None:
    """Removing the wrapper cannot change Mapping copy or sibling selector semantics."""
    from collections.abc import Mapping

    from fpstreams import rows

    events: list[str] = []
    marker = object()

    class TrackedMapping(Mapping[str, object]):
        def __init__(self) -> None:
            self.values = {"marker": marker, "value": 2}

        def __getitem__(self, key: str) -> object:
            events.append(f"get:{key}")
            return self.values[key]

        def __iter__(self) -> Iterator[str]:
            events.append("iter")
            for key in self.values:
                events.append(f"yield:{key}")
                yield key

        def __len__(self) -> int:
            events.append("len")
            return len(self.values)

    source = TrackedMapping()

    def first(row: Mapping[str, object]) -> int:
        events.append("first")
        return int(row["value"]) + 1

    def second(row: Mapping[str, object]) -> int:
        events.append("second")
        return int(row["value"]) + 2

    query = rows([source]).with_columns(first=first, second=second)
    operation = query._flow._pipeline.operations[0]
    result = query.to_list()

    assert type(operation.function) is FunctionType
    assert result == [{"marker": marker, "value": 2, "first": 3, "second": 4}]
    assert result[0]["marker"] is marker
    assert events == [
        "iter",
        "yield:marker",
        "yield:value",
        "get:marker",
        "get:value",
        "first",
        "get:value",
        "second",
        "get:value",
    ]


def test_direct_planned_row_function_does_not_add_an_exception_frame() -> None:
    """A selector failure should pass directly through the generated transform function."""

    from fpstreams import rows

    class ExpectedFailure(RuntimeError):
        pass

    def fail(_row: object) -> object:
        raise ExpectedFailure("selector failed")

    with pytest.raises(ExpectedFailure, match="selector failed") as captured:
        rows([{"value": 1}]).with_columns(result=fail).to_list()

    frames: list[tuple[str, str]] = []
    traceback = captured.value.__traceback__
    while traceback is not None:
        frames.append(
            (
                Path(traceback.tb_frame.f_code.co_filename).as_posix(),
                traceback.tb_frame.f_code.co_name,
            )
        )
        traceback = traceback.tb_next
    assert not any(
        path.endswith("/planning/arrow.py") and name == "__call__" for path, name in frames
    )


def test_arrow_prefix_retains_an_exact_query_bound_direct_projection() -> None:
    """A lone direct select carries ordered output/input names into Arrow execution."""
    pa = pytest.importorskip("pyarrow")
    from fpstreams import rows
    from fpstreams.planning.arrow import ArrowProjectionSpec

    query = (
        rows.from_arrow(pa.table({"left": [1], "right": [2]}))
        .select("right", renamed="left", repeated="right")
        ._flow
    )
    physical = compile_query(query._query("list"))

    assert physical.backend_payload is not None
    prefix = physical.backend_payload.arrow_prefix
    assert prefix is not None
    assert prefix.operation_count == 1
    assert prefix.projection == ArrowProjectionSpec(
        selectors=(("right", "right"), ("renamed", "left"), ("repeated", "right")),
        inputs=("right", "left"),
    )
    assert prefix.operations[-1] is query._pipeline.operations[-1]


def test_arrow_projection_candidate_crosses_only_a_closed_direct_filter() -> None:
    """A primitive direct comparison may filter before projection; opaque row stages may not."""
    pa = pytest.importorskip("pyarrow")
    from fpstreams import rows

    table = pa.table({"left": [1], "right": [2]})

    class StringSubclass(str):
        pass

    safe = rows.from_arrow(table).where(col("left") > 0).select("left")
    physical = compile_query(safe._flow._query("list"))
    assert physical.backend_payload is not None
    prefix = physical.backend_payload.arrow_prefix
    assert prefix is not None
    assert prefix.operation_count == 2
    assert prefix.projection is not None

    queries = (
        rows.from_arrow(table).with_columns(extra="left").select("left"),
        rows.from_arrow(table).where(left=1).select("left"),
        rows.from_arrow(table).where(lambda row: row["left"] > 0).select("left"),
        rows.from_arrow(table).select("left").select("left"),
        rows.from_arrow(table).select("left.value"),
        rows.from_arrow(table).select(0),
        rows.from_arrow(table).select(col("left")),
        rows.from_arrow(table).select(StringSubclass("left")),
    )

    for query in queries:
        physical = compile_query(query._flow._query("list"))
        assert physical.backend_payload is not None
        prefix = physical.backend_payload.arrow_prefix
        assert prefix is not None
        assert prefix.operation_count == 0
        assert prefix.projection is None


def test_arrow_first_prefix_accepts_only_complete_short_circuit_safe_shapes() -> None:
    """Early Arrow execution is limited to equality and direct projection programs."""
    pa = pytest.importorskip("pyarrow")
    from fpstreams import Rows, rows

    table = pa.table({"id": [1, 2], "payload": ["one", "two"]})
    accepted = (
        rows.from_arrow(table).select("payload"),
        rows.from_arrow(table).where(col("id") == 2),
        rows.from_arrow(table).where(lit(2) == col("id")).select("payload"),
    )
    for query in accepted:
        physical = compile_query(query._flow._query("first"))
        assert physical.backend_payload is not None
        prefix = physical.backend_payload.arrow_prefix
        assert prefix is not None
        assert prefix.first_only
        assert prefix.operation_count == len(query._flow._pipeline.operations)

    rejected = (
        rows.from_arrow(table).where(col("id") >= 1),
        rows.from_arrow(table).where((col("id") + 1) > 0),
        Rows(rows.from_arrow(table)._flow.reject(col("id") == 2)),
        rows.from_arrow(table).where(lambda row: row["id"] == 2),
        rows.from_arrow(table).select(value="payload.missing"),
    )
    for query in rejected:
        physical = compile_query(query._flow._query("first"))
        assert physical.backend_payload is not None
        assert physical.backend_payload.arrow_prefix is None
        assert query._flow.explain(terminal="first").to_dict()["arrow_prefix"] is None


def test_exact_i64_range_hint_normalizes_only_builtin_safe_comparisons() -> None:
    """The Parquet hint accepts exact i64 literals and never rewrites boundary operators."""
    from fpstreams.expressions.row import RowExpr
    from fpstreams.expressions.row_ir import Field
    from fpstreams.planning.arrow import direct_exact_i64_range
    from fpstreams.planning.sync import FilterOp

    minimum = -(1 << 63)
    maximum = (1 << 63) - 1
    assert direct_exact_i64_range(FilterOp(col("id") < minimum)) == ("id", "<", minimum)
    assert direct_exact_i64_range(FilterOp(col("id") <= maximum)) == ("id", "<=", maximum)
    assert direct_exact_i64_range(FilterOp(lit(minimum) < col("id"))) == (
        "id",
        ">",
        minimum,
    )
    assert direct_exact_i64_range(FilterOp(lit(maximum) >= col("id"))) == (
        "id",
        "<=",
        maximum,
    )

    class IntSubclass(int):
        pass

    dotted = RowExpr._from_node(Field("nested.value"), "nested.value")
    rejected = (
        FilterOp(col("id") < True),
        FilterOp(col("id") < IntSubclass(1)),
        FilterOp(col("id") < -(1 << 63) - 1),
        FilterOp(col("id") > 1 << 63),
        FilterOp(col("id") != 1),
        FilterOp(col("id") < 1, negate=True),
        FilterOp(dotted > 1),
        FilterOp(col("nested.value") > 1),
    )
    assert all(direct_exact_i64_range(operation) is None for operation in rejected)


def test_exact_rows_chain_compiles_to_one_query_bound_loop() -> None:
    """The common with_columns/where/select shape must have one statement-level row loop."""
    from fpstreams import rows
    from fpstreams.execution._rows_fusion import compile_rows_fusion
    from fpstreams.execution.physical import operations_from_physical_nodes
    from fpstreams.execution.sync import open_operations

    score = col("x") * 3 + col("y") - 1
    query = (
        rows([{"x": 1, "y": 3}, {"x": 2, "y": 1}])
        .with_columns(score=score)
        .where(col("score") % 5 != 0)
        .select("x", "score")
        ._flow
    )
    plan = compile_query(query._query("list"))
    operations = operations_from_physical_nodes(plan.nodes)
    fused = compile_rows_fusion(operations)

    assert fused is not None
    assert list(fused(iter([{"x": 1, "y": 3}, {"x": 2, "y": 1}]))) == [{"x": 2, "score": 6}]
    with open_operations(iter([{"x": 2, "y": 1}]), operations) as values:
        assert values.gi_code.co_name == "_adaptive_rows_fusion"
        assert list(values) == [{"x": 2, "score": 6}]
    with open_operations(iter([{"x": 2, "y": 1}] * 512), operations) as values:
        assert values.gi_code.co_filename == "<fpstreams-rows-fusion>"


@pytest.mark.parametrize("container", [list, tuple], ids=["list", "tuple"])
@pytest.mark.parametrize(
    ("threshold", "expected_count"),
    [(16_384, 0), (16_221, 163), (8_192, 8_192), (164, 16_220), (0, 16_384)],
    ids=["zero", "one-percent", "half", "ninety-nine-percent", "all"],
)
def test_large_closed_row_filter_uses_direct_eager_list_sink(
    container: type,
    threshold: int,
    expected_count: int,
) -> None:
    """A retained exact container drains one closed i64 RowExpr filter in Rust."""
    from fpstreams import rows

    records = container({"value": index} for index in range(16_384))
    execution = rows(records).where(col("value") >= threshold).run_with_report("to_list")

    assert len(execution.value) == expected_count
    if expected_count:
        assert execution.value[0] is records[threshold]
        assert execution.value[-1] is records[-1]
    assert execution.report.strategy == "rust_direct"


def test_large_exact_i64_row_filter_uses_native_expression_sink() -> None:
    """A closed arithmetic predicate filters exact integer dictionaries in Rust."""
    from fpstreams import rows

    records = [{"value": value} for value in range(-8_192, 8_192)]
    predicate = ((col("value") * 3 + 1) % 4) == 0

    execution = rows(records).where(predicate).run_with_report("to_list")

    assert execution.value == records[1::4]
    assert all(
        actual is expected for actual, expected in zip(execution.value, records[1::4], strict=True)
    )
    assert execution.report.strategy == "rust_direct"


@pytest.mark.parametrize(
    ("width", "strategy"),
    [(2, "rust_direct"), (3, "python_direct")],
    ids=["profitable", "crossover"],
)
def test_native_row_filter_stops_before_wide_dictionary_scan(
    width: int,
    strategy: str,
) -> None:
    """The i64 prefix is limited to record widths with a measured generic speedup."""
    from fpstreams import rows

    extras = tuple(f"extra_{index}" for index in range(width - 1))
    records = [{"value": value, **{name: value for name in extras}} for value in range(16_384)]

    execution = rows(records).where(col("value") >= 8_192).run_with_report("to_list")

    assert execution.value == records[8_192:]
    assert execution.report.strategy == strategy


@pytest.mark.parametrize("container", [list, tuple], ids=["list", "tuple"])
@pytest.mark.parametrize("exact_prefix", [0, 8_192], ids=["mapping", "mixed"])
def test_large_row_filter_preserves_mapping_protocol_at_the_exact_dict_boundary(
    container: type,
    exact_prefix: int,
) -> None:
    """The first non-exact dictionary and its suffix execute once through canonical selectors."""
    from collections.abc import Mapping

    from fpstreams import rows

    lookups: list[int] = []

    class Record(Mapping[str, int]):
        def __init__(self, value: int) -> None:
            self.value = value

        def __getitem__(self, key: str) -> int:
            if key != "value":
                raise KeyError(key)
            lookups.append(self.value)
            return self.value

        def __iter__(self) -> Iterator[str]:
            return iter(("value",))

        def __len__(self) -> int:
            return 1

    records = container(
        [*({"value": index} for index in range(exact_prefix))]
        + [Record(index) for index in range(exact_prefix, 16_384)]
    )
    execution = rows(records).where(col("value") >= 0).run_with_report("to_list")

    assert len(execution.value) == 16_384
    assert all(
        actual is expected for actual, expected in zip(execution.value, records, strict=True)
    )
    assert lookups == list(range(exact_prefix, 16_384))
    assert execution.report.strategy == (
        "planned:python" if exact_prefix == 0 else "rust_python_hybrid"
    )


def test_native_row_filter_does_not_repeat_exact_dict_key_protocols() -> None:
    """A native decline must replay the boundary before any observable dictionary lookup."""
    from fpstreams import rows

    comparisons: list[object] = []

    class FieldKey:
        def __hash__(self) -> int:
            return hash("value")

        def __eq__(self, other: object) -> bool:
            comparisons.append(other)
            return other == "value"

    class Comparable:
        def __ge__(self, other: object) -> bool:
            return other == 0

    first = {FieldKey(): Comparable()}
    records = [first, *({"value": index} for index in range(1, 16_384))]

    execution = rows(records).where(col("value") >= 0).run_with_report("to_list")

    assert execution.value == records
    assert all(
        actual is expected for actual, expected in zip(execution.value, records, strict=True)
    )
    assert comparisons == ["value"]
    assert execution.report.strategy == "python_direct"


def test_direct_row_filter_uses_filter_truth_semantics_when_bool_is_replaced(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The generated sink truth-tests results instead of resolving the live bool name."""

    from fpstreams import rows

    records = [{"value": 1} for _index in range(16_384)]
    predicate = col("value") > 0
    with monkeypatch.context() as context:
        context.setattr(builtins, "bool", lambda _value: False)
        automatic = rows(records).where(predicate).run_with_report("to_list")
        forced = rows(records).where(predicate).with_engine("python").to_list()

    assert automatic.value == forced == records
    assert automatic.report.strategy == "python_direct"


def test_native_row_filter_preserves_live_abs_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An abs expression stays in Python while its captured builtin is noncanonical."""

    from fpstreams import rows

    records = [{"value": 0} for _index in range(16_384)]
    predicate = abs(col("value")) == 77
    with monkeypatch.context() as context:
        context.setattr(builtins, "abs", lambda _value: 77)
        automatic = rows(records).where(predicate).run_with_report("to_list")
        forced = rows(records).where(predicate).with_engine("python").to_list()

    assert automatic.value == forced == records
    assert automatic.report.strategy == "python_direct"


def test_direct_row_filter_rejects_internal_dotted_field_nodes() -> None:
    """A dotted Field keeps canonical path lookup semantics even when manually constructed."""
    from fpstreams import rows
    from fpstreams.expressions.row import RowExpr
    from fpstreams.expressions.row_ir import Field

    records = [{"a": {"b": 0}, "a.b": 2} for _index in range(16_384)]
    dotted = RowExpr._from_node(Field("a.b"), "a.b")

    automatic = rows(records).where(dotted > 1).run_with_report("to_list")
    forced = rows(records).where(dotted > 1).with_engine("python").to_list()

    assert automatic.value == forced == []
    assert automatic.report.strategy == "planned:python"


def test_large_row_filter_preserves_truth_testing_and_stop_iteration_prefix() -> None:
    """The eager sink truth-tests once per reached row and treats exhaustion like filter."""
    from fpstreams import rows

    calls: list[int] = []

    class Truth:
        def __init__(self, value: int) -> None:
            self.value = value

        def __bool__(self) -> bool:
            calls.append(self.value)
            if self.value == 2:
                raise StopIteration("truth stopped")
            return self.value == 0

    records = [
        {"keep": Truth(0)},
        {"keep": Truth(1)},
        {"keep": Truth(2)},
        *({"keep": True} for _index in range(16_381)),
    ]
    execution = rows(records).where(col("keep")).run_with_report("to_list")

    assert execution.value == [records[0]]
    assert execution.value[0] is records[0]
    assert calls == [0, 1, 2]
    assert execution.report.strategy == "python_direct"


def test_large_row_filter_preserves_selector_and_operator_error_boundaries() -> None:
    """Lookup failures are translated, while exceptions owned by operators stay untouched."""

    missing = [{"value": 0}, {}, *({"value": 1} for _index in range(16_382))]
    with pytest.raises(fpstreams.SelectionError) as selection_error:
        fpstreams.rows(missing).where(col("value") >= 0).to_list()
    assert isinstance(selection_error.value.__cause__, KeyError)

    failure = KeyError("operator-owned")

    class BrokenComparison:
        def __ge__(self, _other: object) -> bool:
            raise failure

    broken = [{"value": BrokenComparison()}, *({"value": 1} for _index in range(16_383))]
    with pytest.raises(KeyError, match="operator-owned") as operator_error:
        fpstreams.rows(broken).where(col("value") >= 0).to_list()
    assert operator_error.value is failure
    assert operator_error.value.__cause__ is None


def test_large_row_filter_error_releases_accepted_prefix() -> None:
    """A later operator error clears the eager prefix and releases its accepted rows."""
    import gc

    from fpstreams import rows

    failure = ValueError("comparison failed")
    records: list[dict[str, object] | None] = []

    class Payload:
        pass

    class BrokenComparison:
        def __ge__(self, _other: object) -> bool:
            records[0] = None
            raise failure

    payload = Payload()
    accepted = {"value": 1, "payload": payload}
    reference = weakref.ref(payload)
    records.extend(
        [accepted, {"value": BrokenComparison()}, *({"value": 1} for _index in range(16_382))]
    )
    del accepted, payload

    with pytest.raises(ValueError) as captured:
        rows(records).where(col("value") >= 0).to_list()

    assert captured.value is failure
    gc.collect()
    assert reference() is None


def test_large_row_filter_consumes_live_list_mutations() -> None:
    """Expression protocols mutate the same list iterator that canonical filter would consume."""
    from fpstreams import rows

    records: list[dict[str, object]] = []
    first: dict[str, object] = {}
    appended = {"value": 16_384}
    records.extend([first, *({"value": index} for index in range(1, 16_384))])

    class AppendExpectedRow:
        def __ge__(self, _other: object) -> bool:
            records.append(appended)
            return True

    first["value"] = AppendExpectedRow()
    execution = rows(records).where(col("value") >= 0).run_with_report("to_list")

    assert len(execution.value) == 16_385
    assert execution.value[0] is first
    assert execution.value[-1] is appended
    assert execution.report.strategy == "python_direct"


def test_native_row_filter_boundary_preserves_for_target_lifetime() -> None:
    """A declined row is released before the next live-list pull, like builtin filter."""
    from fpstreams import rows

    records: list[dict[str, object] | None] = []
    appended = {"value": 20_000}

    class Boundary:
        def __ge__(self, _other: object) -> bool:
            records[0] = None
            return False

        def __del__(self) -> None:
            records.append(appended)

    first = {"value": Boundary()}
    records.extend([first, *({"value": index} for index in range(1, 16_384))])
    del first

    execution = rows(records).where(col("value") >= 0).run_with_report("to_list")

    assert len(execution.value) == 16_384
    assert execution.value[-1] is appended
    assert len(records) == 16_385
    assert execution.report.strategy == "python_direct"


def test_large_row_filter_deopt_closes_a_replaced_source_iterator_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-builtin iterator opened at runtime remains solely owned by canonical execution."""
    from fpstreams import rows

    records = [{"value": index} for index in range(16_384)]
    query = rows(records).where(col("value") >= 16_383)
    source = query._flow._pipeline.source
    closed = 0

    class Values:
        def __init__(self) -> None:
            self._iterator = iter(records)

        def __iter__(self) -> Values:
            return self

        def __next__(self) -> dict[str, int]:
            return next(self._iterator)

        def close(self) -> None:
            nonlocal closed
            closed += 1

    monkeypatch.setattr(source, "_factory", Values)

    assert query.to_list() == [records[-1]]
    assert closed == 1


def test_direct_row_filter_scope_keeps_small_and_forced_python_queries_canonical() -> None:
    """The codegen cost is paid only by large automatic list materializations."""
    from fpstreams import rows

    small = rows([{"value": index} for index in range(16_383)]).where(col("value") >= 0)
    forced = (
        rows([{"value": index} for index in range(16_384)])
        .where(col("value") >= 0)
        .with_engine("python")
    )

    assert small.run_with_report("to_list").report.strategy == "planned:python"
    assert forced.run_with_report("to_list").report.strategy == "planned:python"


def test_direct_row_filter_declines_failpoints() -> None:
    """Active failpoints retain the canonical compiled-expression callback path."""
    from fpstreams import rows
    from fpstreams.runtime.failpoints import failpoint

    query = rows([{"value": index} for index in range(16_384)]).where(col("value") >= 8_192)

    with failpoint("unrelated.rows-filter.transition", RuntimeError("unused")):
        execution = query.run_with_report("to_list")

    assert len(execution.value) == 8_192
    assert execution.report.strategy == "planned:python"


def test_single_select_preserves_current_row_until_source_resumes() -> None:
    """An adaptive projection retains dropped values while advancing its source."""
    from fpstreams import rows

    events: list[str] = []

    class Resource:
        def __del__(self) -> None:
            events.append("released")

    def source():
        yield {"value": 1, "unused": Resource()}
        events.append("source resumed")
        yield {"value": 2}

    assert rows(source()).select("value").with_engine("python").to_list() == [
        {"value": 1},
        {"value": 2},
    ]
    assert events == ["source resumed", "released"]


def test_small_direct_select_uses_builtin_map_until_codegen_can_amortize() -> None:
    """A simple projection avoids query-local AST cost while richer expressions still fuse."""
    from fpstreams import rows
    from fpstreams.execution._rows_fusion import execute_rows_fusion
    from fpstreams.execution.physical import operations_from_physical_nodes

    direct = rows([{"id": 1, "value": 2}]).select("id", "value")._flow
    direct_plan = compile_query(direct._query("list"))
    direct_operations = operations_from_physical_nodes(direct_plan.nodes)

    assert (
        execute_rows_fusion(
            iter([{"id": 1, "value": 2}]),
            direct_operations,
            exact_rows=1_000,
        )
        is None
    )
    compiled = execute_rows_fusion(
        iter([{"id": 1, "value": 2}]),
        direct_operations,
        exact_rows=3_000,
    )
    assert compiled is not None
    assert list(compiled) == [{"id": 1, "value": 2}]

    expression = rows([{"value": 2}]).select(score=col("value") * 3 + 1)._flow
    expression_plan = compile_query(expression._query("list"))
    expression_operations = operations_from_physical_nodes(expression_plan.nodes)
    expression_rows = execute_rows_fusion(
        iter([{"value": 2}]),
        expression_operations,
        exact_rows=1_000,
    )
    assert expression_rows is not None
    assert list(expression_rows) == [{"score": 7}]


def test_compiler_cache_keeps_compiled_map_filter_and_negation_semantics_distinct() -> None:
    """Catch a template-key collision that substitutes a map/filter stage or filter polarity."""
    values = [-2, -1, 0, 1]
    shift = item + 1

    map_then_filter = compile_query(
        flow(values).map(shift).filter(shift).with_engine("python")._query("list")
    )
    cached_map_then_filter = compile_query(
        flow(values).map(shift).filter(shift).with_engine("python")._query("list")
    )
    filter_then_map = compile_query(
        flow(values).filter(shift).map(shift).with_engine("python")._query("list")
    )
    keep_matches = compile_query(flow(values).filter(shift).with_engine("python")._query("list"))
    reject_matches = compile_query(flow(values).reject(shift).with_engine("python")._query("list"))

    assert map_then_filter.cacheable and not map_then_filter.cache_hit
    assert cached_map_then_filter.cacheable and cached_map_then_filter.cache_hit
    assert filter_then_map.cacheable and not filter_then_map.cache_hit
    assert keep_matches.cacheable and not keep_matches.cache_hit
    assert reject_matches.cacheable and not reject_matches.cache_hit
    assert list(execute_physical(map_then_filter)) == [0, 1, 2]
    assert list(execute_physical(filter_then_map)) == [-1, 1, 2]
    assert list(execute_physical(keep_matches)) == [-2, 0, 1]
    assert list(execute_physical(reject_matches)) == [-1]


def test_scalar_python_fusion_declines_unbounded_generated_programs() -> None:
    """A long stage chain must stay on the bounded canonical executor."""
    from fpstreams.execution._scalar_fusion import compile_scalar_fusion

    query = flow(range(2_048)).with_engine("python")
    for _ in range(33):
        query = query.map(item + 1)
    plan = compile_query(query._query("list"))

    assert compile_scalar_fusion(plan.nodes) is None
    assert query.to_list()[:2] == [33, 34]


@pytest.mark.parametrize(("first_kind", "offset"), [("arrow", 30_001), ("row", 40_001)])
def test_compiler_cache_keeps_arrow_prefix_payloads_isolated_from_row_sources(
    first_kind: str, offset: int
) -> None:
    """Arrow and ordinary row plans with matching expressions cannot exchange payloads."""
    pa = pytest.importorskip("pyarrow")
    from fpstreams import Flow, col, rows
    from fpstreams.planning.source import Source

    records = [{"value": value} for value in range(64)]
    expression = col("value") + offset
    arrow_flow = rows.from_arrow(pa.table({"value": range(64)}))._flow.map(expression)
    row_flow = Flow(
        Source(lambda: iter(records), SourceCapabilities(reiterable=True, exact_size=64))
    ).map(expression)
    first_flow, second_flow = (
        (arrow_flow, row_flow) if first_kind == "arrow" else (row_flow, arrow_flow)
    )

    first = compile_query(first_flow._query("list"))
    second = compile_query(second_flow._query("list"))
    arrow_plan = first if first_kind == "arrow" else second
    row_plan = second if first_kind == "arrow" else first

    assert not first.cacheable and not first.cache_hit
    assert not second.cacheable and not second.cache_hit
    assert arrow_plan.backend_payload is not None
    assert arrow_plan.backend_payload.arrow_prefix is not None
    assert arrow_plan.backend_payload.arrow_prefix.operation_count == 1
    assert row_plan.backend_payload is not None
    assert row_plan.backend_payload.arrow_prefix is None


def test_compiler_never_caches_an_opaque_callback_template() -> None:
    physical = compile_query(flow([1, 2]).map(lambda value: value + 1)._query("list"))

    assert not physical.cacheable
    assert "RowPhysicalNode" in physical.cache_reason


def test_unique_selector_runs_once_per_source_item() -> None:
    seen: list[int] = []
    result = flow(range(128)).unique_by(lambda value: seen.append(value) or value).to_list()

    assert result == list(range(128))
    assert seen == list(range(128))


def test_program_reuses_precompiled_selector(monkeypatch) -> None:
    """Selectors compile once per program rather than once for every input row."""
    calls = 0
    original = program_module.compile_selector

    def tracked(selector):
        nonlocal calls
        calls += 1
        return original(selector)

    monkeypatch.setattr(program_module, "compile_selector", tracked)
    compiled = compile_expression(lower_expression(col("user.age") + 1))
    evaluator = compiled.evaluator()

    assert evaluator({"user": {"age": 20}}) == 21
    assert evaluator({"user": {"age": 30}}) == 31
    assert calls == 1


def test_and_and_coalesce_skip_unused_callbacks() -> None:
    """Compiled control flow preserves Python short-circuit callback ordering."""
    events: list[str] = []

    def touch(value):
        events.append("called")
        return value

    assert (
        compile_expression(lower_expression(lit(False) & col("x").map(touch))).evaluator()({"x": 1})
        is False
    )
    assert events == []
    assert (
        compile_expression(lower_expression(coalesce(lit(3), col("x").map(touch)))).evaluator()(
            {"x": 1}
        )
        == 3
    )
    assert events == []


def test_if_else_truth_tests_condition_once() -> None:
    """The selected branch is recorded after one truth test of the condition."""

    class TruthyOnce:
        def __init__(self) -> None:
            self.calls = 0

        def __bool__(self) -> bool:
            self.calls += 1
            if self.calls > 1:
                raise AssertionError("condition truth-tested more than once")
            return True

    condition = TruthyOnce()
    program = ExprProgram(
        IfElse(Literal(condition), Literal("yes"), Literal("no")), {}, Effect.MAY_RAISE
    )

    assert ExprEvaluator(program)(object()) == "yes"
    assert condition.calls == 1


def _compiled_row_program(node: object):
    return compile_expression(
        TypedExpr(
            node, ValueType.UNKNOWN, Effect.MAY_RAISE, frozenset({"python"}), ExpressionSource.ROW
        )
    )


def test_row_program_uses_direct_evaluator_and_matches_reference() -> None:
    """Closed row IR gains a direct evaluator without changing its result."""
    program = compile_expression(lower_expression(col("x") * 3 + col("y") - 1))

    assert program.row_evaluator is not None
    assert program.evaluator() is program.row_evaluator
    assert program.evaluator()({"x": 4, "y": 2}) == ExprEvaluator(program)({"x": 4, "y": 2})


def test_direct_row_evaluator_preserves_control_flow_and_evaluation_order() -> None:
    """Boolean, coalesce, conditional, and call nodes keep their one-pass ordering."""
    events: list[str] = []

    class LoggedValue:
        def __init__(self, name: str, value: object) -> None:
            self.name = name
            self.value = value
            self.truth_tests = 0

        def __bool__(self) -> bool:
            self.truth_tests += 1
            events.append(self.name)
            return bool(self.value)

    left = LoggedValue("left", False)
    right = LoggedValue("right", True)
    and_program = _compiled_row_program(Binary("and", Literal(left), Literal(right)))
    assert and_program.row_evaluator is not None
    assert and_program.evaluator()({}) is False
    assert events == ["left"]
    assert right.truth_tests == 0
    events.clear()
    assert ExprEvaluator(and_program)({}) is False
    assert events == ["left"]

    events.clear()
    or_left = LoggedValue("or-left", True)
    or_right = LoggedValue("or-right", False)
    or_program = _compiled_row_program(Binary("or", Literal(or_left), Literal(or_right)))
    assert or_program.row_evaluator is not None
    assert or_program.evaluator()({}) is True
    assert events == ["or-left"]
    assert or_right.truth_tests == 0
    events.clear()
    assert ExprEvaluator(or_program)({}) is True
    assert events == ["or-left"]

    events.clear()
    condition = LoggedValue("condition", True)
    conditional = _compiled_row_program(IfElse(Literal(condition), Literal("yes"), Literal("no")))
    assert conditional.row_evaluator is not None
    assert conditional.evaluator()({}) == ExprEvaluator(conditional)({}) == "yes"
    assert events == ["condition", "condition"]
    assert condition.truth_tests == 2

    class LoggedRow(dict[str, object]):
        def __getitem__(self, key: str) -> object:
            events.append(key)
            return super().__getitem__(key)

    events.clear()
    coalesce_program = _compiled_row_program(Coalesce((Field("first"), Field("second"))))
    assert coalesce_program.row_evaluator is not None
    assert coalesce_program.evaluator()(LoggedRow(first="kept", second="unused")) == "kept"
    assert events == ["first"]

    events.clear()
    call_program = _compiled_row_program(Call("contains", (Field("haystack"), Field("needle"))))
    assert call_program.row_evaluator is not None
    assert call_program.evaluator()(LoggedRow(haystack=("x",), needle="x")) is True
    assert events == ["haystack", "needle"]
    events.clear()
    assert ExprEvaluator(call_program)(LoggedRow(haystack=("x",), needle="x")) is True
    assert events == ["haystack", "needle"]


def test_direct_row_evaluator_preserves_selector_and_operator_failures() -> None:
    """Selector wrapping and left-to-right GetItem evaluation remain reference-identical."""
    missing = _compiled_row_program(Field("missing"))
    assert missing.row_evaluator is not None
    with pytest.raises(fpstreams.SelectionError) as direct_error:
        missing.evaluator()({})
    with pytest.raises(fpstreams.SelectionError) as reference_error:
        ExprEvaluator(missing)({})
    assert str(direct_error.value) == str(reference_error.value)
    assert type(direct_error.value.__cause__) is type(reference_error.value.__cause__)

    events: list[str] = []

    class LoggedRow(dict[str, object]):
        def __getitem__(self, key: str) -> object:
            events.append(key)
            return super().__getitem__(key)

    getitem = _compiled_row_program(GetItem(Field("value"), Field("key")))
    assert getitem.row_evaluator is not None
    assert getitem.evaluator()(LoggedRow(value={"answer": 42}, key="answer")) == 42
    assert events == ["value", "key"]


def test_direct_row_evaluator_uses_slots_without_evaluating_user_representation() -> None:
    """Untrusted selector text and literal reprs never become compiler source."""

    class ExplosiveRepr:
        def __repr__(self) -> str:
            raise AssertionError("repr must not run during compilation")

    value = ExplosiveRepr()
    literal = _compiled_row_program(Literal(value))
    selector = "x'); __import__('os')"
    field = _compiled_row_program(Field(selector))

    assert literal.row_evaluator is not None
    assert literal.evaluator()({}) is value
    assert field.row_evaluator is not None
    assert field.evaluator()({selector: 7}) == 7


def test_non_closed_or_oversized_row_ir_falls_back_to_the_iterative_evaluator() -> None:
    """Opaque, unknown, custom-cast, and oversized IR retain reference timing."""
    calls: list[object] = []

    opaque = _compiled_row_program(
        PythonUDF(lambda value: calls.append(value) or value + 1, (Field("x"),))
    )
    custom_cast = _compiled_row_program(Cast(Literal("2"), lambda value: calls.append(value) or 2))
    unknown_call = _compiled_row_program(Call("unknown", (Literal(1),)))

    class Unknown:
        pass

    unknown_node = _compiled_row_program(Unknown())
    deep: object = Literal(1)
    for _ in range(129):
        deep = Unary("neg", deep)
    wide: object = Literal(1)
    for _ in range(10):
        wide = Binary("+", wide, wide)
    oversized = _compiled_row_program(deep)
    expanded = _compiled_row_program(wide)

    for program in (opaque, custom_cast, unknown_call, unknown_node, oversized, expanded):
        assert program.row_evaluator is None
        assert isinstance(program.evaluator(), ExprEvaluator)

    assert opaque.evaluator()({"x": 1}) == 2
    assert custom_cast.evaluator()({}) == 2
    assert calls == [1, "2"]
    with pytest.raises(ValueError, match="unknown row call"):
        unknown_call.evaluator()({})
    with pytest.raises(TypeError, match="unsupported row node"):
        unknown_node.evaluator()({})
    assert oversized.evaluator()({}) == -1
    assert expanded.evaluator()({}) == 1024


@pytest.mark.parametrize("engine", ["auto", "python"])
@pytest.mark.parametrize("depth", [128, 600])
def test_oversized_row_expression_materializes_through_public_terminals(
    engine: str, depth: int
) -> None:
    """Uncached row programs reach the iterative fallback before recursive fingerprinting."""
    expression = col("value")
    for _ in range(depth):
        expression = expression + 1
    record = {"value": 0}
    query = flow([record]).map(expression).with_engine(engine)

    assert expression(record) == depth
    assert list(query) == [depth]
    assert query.to_list() == [depth]
    assert query.to_tuple() == (depth,)
    assert query.count() == 1
    filtered = flow([record]).filter(expression == depth).with_engine(engine).to_list()
    assert len(filtered) == 1 and filtered[0] is record


def test_exact_builtin_casts_compile_directly_and_preserve_exceptions() -> None:
    """Only exact approved builtins bypass the generic Cast fallback."""
    direct = _compiled_row_program(Cast(Literal("2"), int))
    invalid = _compiled_row_program(Cast(Literal("not-an-int"), int))

    assert direct.row_evaluator is not None
    assert direct.evaluator()({}) == ExprEvaluator(direct)({}) == 2
    assert invalid.row_evaluator is not None
    with pytest.raises(ValueError) as direct_error:
        invalid.evaluator()({})
    with pytest.raises(ValueError) as reference_error:
        ExprEvaluator(invalid)({})
    assert str(direct_error.value) == str(reference_error.value)

    for target, value, expected in ((float, "2.5", 2.5), (str, 2, "2"), (bool, 0, False)):
        program = _compiled_row_program(Cast(Literal(value), target))
        assert program.row_evaluator is not None
        assert program.evaluator()({}) == ExprEvaluator(program)({}) == expected


def test_direct_row_evaluator_preserves_coalesce_and_conditional_single_evaluation() -> None:
    """Direct AST control flow evaluates each selected operand exactly once."""
    direct_events: list[str] = []
    reference_events: list[str] = []

    class LoggedRow(dict[str, object]):
        def __init__(self, events: list[str], **values: object) -> None:
            super().__init__(values)
            self.events = events

        def __getitem__(self, key: str) -> object:
            self.events.append(key)
            return super().__getitem__(key)

    coalesced = _compiled_row_program(Coalesce((Field("first"), Field("second"))))
    conditional = _compiled_row_program(IfElse(Field("condition"), Field("yes"), Field("no")))
    assert coalesced.row_evaluator is not None
    assert conditional.row_evaluator is not None

    assert (
        coalesced.evaluator()(LoggedRow(direct_events, first=None, second="fallback")) == "fallback"
    )
    assert (
        ExprEvaluator(coalesced)(LoggedRow(reference_events, first=None, second="fallback"))
        == "fallback"
    )
    assert direct_events == reference_events == ["first", "second"]

    direct_events.clear()
    reference_events.clear()
    assert conditional.evaluator()(LoggedRow(direct_events, condition=True, yes=1, no=2)) == 1
    assert ExprEvaluator(conditional)(LoggedRow(reference_events, condition=True, yes=1, no=2)) == 1
    assert direct_events == reference_events == ["condition", "yes"]


@pytest.mark.parametrize(("returned", "expected"), [((1, 2), 3), (("a", "b"), "ab")])
def test_row_udf_shared_occurrences_keep_each_returned_value(returned, expected) -> None:
    """Repeated use calls the callback twice and retains both results for its parent."""
    results = iter(returned)
    calls: list[int] = []

    def step(value: int) -> object:
        calls.append(value)
        return next(results)

    shared = col("x").map(step)
    expression = shared + shared

    assert expression({"x": 7}) == expected
    assert calls == [7, 7]


@pytest.mark.parametrize("depth", [0, 130])
def test_row_shared_selector_occurrences_keep_changing_values(depth: int) -> None:
    """Direct and deep fallback evaluation both preserve repeated dynamic lookups."""

    class ChangingRow(dict[str, int]):
        def __init__(self) -> None:
            super().__init__(x=0)
            self.reads = 0

        def __getitem__(self, key: str) -> int:
            assert key == "x"
            self.reads += 1
            return self.reads

    shared = col("x")
    expression = shared + shared
    for _ in range(depth):
        expression = expression + 0
    row = ChangingRow()

    assert expression(row) == 3
    assert row.reads == 2


def test_row_fallback_shared_results_survive_lazy_and_zero_argument_children() -> None:
    """A later lazy operand cannot replace an earlier result of the same node."""
    events: list[str] = []
    results = iter((1, 2))

    def value() -> int:
        result = next(results)
        events.append(f"value:{result}")
        return result

    def condition() -> bool:
        events.append("condition")
        return True

    def missing() -> None:
        events.append("missing")

    def forbidden() -> None:
        raise AssertionError("unselected branch was evaluated")

    shared = PythonUDF(value, ())
    lazy = IfElse(
        PythonUDF(condition, ()),
        Coalesce((PythonUDF(missing, ()), shared)),
        PythonUDF(forbidden, ()),
    )
    program = _compiled_row_program(
        PythonUDF(lambda prefix, left, right: (prefix, left, right), (Literal(10), shared, lazy))
    )
    assert program.row_evaluator is None
    assert program.evaluator()({}) == (10, 1, 2)
    assert events == ["value:1", "condition", "missing", "value:2"]


def test_row_fallback_shared_expression_is_independent_across_threads() -> None:
    """One lazy evaluator keeps per-occurrence values local to each concurrent call."""
    from threading import Barrier, get_ident, local

    state = local()
    barrier = Barrier(8)

    def step(value: int) -> int:
        state.calls += 1
        return value * 10 + state.calls

    shared = col("x").map(step)
    expression = shared + shared

    def work(worker: int) -> int:
        barrier.wait(timeout=10)
        for _ in range(64):
            state.calls = 0
            assert expression({"x": worker}) == worker * 20 + 3
            assert state.calls == 2
        return get_ident()

    with ThreadPoolExecutor(max_workers=8) as pool:
        threads = list(pool.map(work, range(8)))
    assert len(set(threads)) == 8


def test_direct_row_evaluator_preserves_shared_occurrences_and_failure_order() -> None:
    """Shared nodes are re-evaluated per occurrence and operands finish before raising."""
    direct_events: list[str] = []
    reference_events: list[str] = []

    class LoggedRow(dict[str, object]):
        def __init__(self, events: list[str], **values: object) -> None:
            super().__init__(values)
            self.events = events

        def __getitem__(self, key: str) -> object:
            self.events.append(key)
            return super().__getitem__(key)

    shared = Field("x")
    repeated = _compiled_row_program(Binary("+", shared, shared))
    assert repeated.row_evaluator is not None
    assert repeated.evaluator()(LoggedRow(direct_events, x=2)) == 4
    assert ExprEvaluator(repeated)(LoggedRow(reference_events, x=2)) == 4
    assert direct_events == reference_events == ["x", "x"]

    direct_events.clear()
    reference_events.clear()

    class ExplodesOnAdd:
        def __add__(self, other: object) -> object:
            raise RuntimeError("add failed")

    failing = _compiled_row_program(Binary("+", Field("left"), Field("right")))
    assert failing.row_evaluator is not None
    with pytest.raises(RuntimeError, match="add failed"):
        failing.evaluator()(LoggedRow(direct_events, left=ExplodesOnAdd(), right=1))
    with pytest.raises(RuntimeError, match="add failed"):
        ExprEvaluator(failing)(LoggedRow(reference_events, left=ExplodesOnAdd(), right=1))
    assert direct_events == reference_events == ["left", "right"]


def test_direct_row_evaluator_preserves_isin_argument_order() -> None:
    """Membership calls evaluate row-expression arguments in IR source order."""
    direct_events: list[str] = []
    reference_events: list[str] = []

    class LoggedRow(dict[str, object]):
        def __init__(self, events: list[str]) -> None:
            super().__init__(member="x", choices=("x",))
            self.events = events

        def __getitem__(self, key: str) -> object:
            self.events.append(key)
            return super().__getitem__(key)

    program = _compiled_row_program(Call("isin", (Field("member"), Field("choices"))))
    assert program.row_evaluator is not None
    assert program.evaluator()(LoggedRow(direct_events)) is True
    assert ExprEvaluator(program)(LoggedRow(reference_events)) is True
    assert direct_events == reference_events == ["member", "choices"]


def test_lowering_classifies_public_expression_families() -> None:
    """Scalar, row, and callback expression families retain distinct metadata."""
    integer = lower_expression(item * 2 + 1)
    floating = lower_expression(fitem / 2.0)
    row = lower_expression(col("user.age") >= 18)
    callback = lower_expression(lambda value: value)

    assert integer.value_type is ValueType.INT64
    assert floating.value_type is ValueType.FLOAT64
    assert row.value_type is ValueType.UNKNOWN
    assert callback.effect is Effect.PYTHON_CALLBACK
    assert callback.backends == frozenset({"python"})


def test_row_python_udf_propagates_callback_effect() -> None:
    """Opaque user row functions form an explicit Python-only barrier."""
    calls: list[int] = []
    shared = col("value")
    for _ in range(10):
        shared = shared + shared
    assert lower_expression(shared).effect is Effect.MAY_RAISE
    callback = col("value").map(lambda value: calls.append(value) or value + 1)
    lowered = lower_expression(shared + callback + callback)

    assert calls == []
    assert lowered.effect is Effect.PYTHON_CALLBACK
    assert lowered.backends == frozenset({"python"})
    assert flow([{"value": 1}]).map(callback + callback).to_list() == [4]
    assert calls == [1, 1]


async def _identity(value: int) -> int:
    return value


def test_async_plan_compiles_serial_and_concurrent_nodes() -> None:
    pipeline = (
        aflow(range(10))
        .map(lambda value: value + 1)
        .filter(bool)
        .map_async(_identity, concurrency=4, ordered=True)
        .take(3)
    )

    physical = compile_async_query(pipeline._query("list"))

    assert [type(node).__name__ for node in physical.nodes] == [
        "AsyncSerialStage",
        "AsyncMapNode",
        "AsyncSerialOperationNode",
    ]
    assert len(physical.nodes[0].operations) == 2


def test_async_compile_does_not_open_source() -> None:
    opens = 0

    async def values():
        nonlocal opens
        opens += 1
        yield 1

    pipeline = aflow.defer(values).map_async(_identity, concurrency=2)

    compile_async_query(pipeline._query("iterate"))

    assert opens == 0


def test_program_fingerprint_is_structural_and_address_free() -> None:
    left = ProgramFingerprint.from_expression(compile_expression(lower_expression(item * 2 + 1)))
    right = ProgramFingerprint.from_expression(compile_expression(lower_expression(item * 2 + 1)))

    assert left == right
    assert "0x" not in left.value


@pytest.mark.parametrize(
    ("case", "expected"),
    [
        ("int_zero", "e5c1816aaa13725b7cf68103a20056b37ed5be7219dcfd262a09d722e796dfd8"),
        ("int_negative", "43c78be6aeaab50632fd41e26fac2ad0e9fd6e7c34d814aba062e06e0d0ba7a7"),
        ("int_wide", "d5976658ac2aa92b8e809f2e68762aee7090e7643453f6e7773ce08eb66695ff"),
        ("0000000000000000", "c8c0a303faa3c52a78c13120f51da50dfd41622b3247079160440cc78a0e1ba3"),
        ("8000000000000000", "9e6c65801ecf0a39562163f56cce16ba9ebd10d2581ee52fd07e639a47b842d4"),
        ("7ff0000000000000", "ce712151e8fa86842c1b62e5b5e3d5fe617b8b59ebbfe8facbcf4b5e33106c18"),
        ("fff0000000000000", "f848d1da6f767a9bee84da96f15b98af74bce4a60cedd5d700e20f29396ee445"),
        ("7ff80000000000a1", "b805d1fba0a39ee1070bf5bc00588c004fbc313911ecd7afa5b9e99c7688f8d3"),
        ("7ff80000000000b2", "46794adabbc9d7dab2395357e47b077661509f5e748a804b8e3a4b47bca32323"),
        ("int_pipeline", "cb8319c153a80986beb344211e57d5c41675c17a1fa6f8505cbf8ef6037eb88f"),
        ("float_pipeline", "df66a5fbbc436c1a1e33f20260e2df96f02b35bf38fae2b2bd7e171229b984b4"),
    ],
)
def test_scalar_fingerprint_keeps_the_framed_binary_format(case: str, expected: str) -> None:
    """Stable fingerprints preserve integer width and each floating-point payload bit."""

    from fpstreams.expressions.scalar import Expr, FExpr, fitem

    if case == "int_pipeline":
        expression = item * 3 + 1
    elif case == "float_pipeline":
        expression = fitem * 3 + 1
    elif case.startswith("int_"):
        expression = Expr.constant(
            {"int_zero": 0, "int_negative": -129, "int_wide": 1 << 4096}[case]
        )
    else:
        expression = FExpr.constant(struct.unpack("!d", bytes.fromhex(case))[0])
    program = ExprProgram(expression, {}, Effect.MAY_RAISE)
    assert ProgramFingerprint.from_expression(program).value == expected


@pytest.mark.parametrize("family", ["integer", "float"])
@pytest.mark.parametrize(
    ("opcode", "integer_fingerprint", "float_fingerprint"),
    [
        (
            -256,
            "8091d69958100af5a1d350ae3d326b6e54983cbb18a79da4c31aeb4b0f134348",
            "f2dabe4a014cd9978777001e3c1c2f00d86f58958380614aa99f45b136eb442a",
        ),
        (
            -1,
            "037d6807831120d0b38f906cef2d1e3673c5b81f1331d4f283e794d567be5001",
            "570a10bb77466ddbd3d5f1057cf6df8eff26d58fd893ac6cccda4d5b78bc1d76",
        ),
        (
            0,
            "f407abf0af565f8c9fb3ff7cc720e7f3b75ea919bbd69b99c7404f80a534ea9f",
            "c5c92e5b706b177f591d4f7aa17325b5e825cab213f183b08efa528e79090d68",
        ),
        (
            18,
            "6bd706ca3b55c78f07c882545041dd36b6753f0c515213ddb2214491ef8502ff",
            "492dab516aeaa771687ec8545bf0fc7ef384e7a385550ffe0449e32732d4e71c",
        ),
        (
            255,
            "9a52cef4423316bd61931249c8211ecc680681ab6fc533f7f32470d2521f1abc",
            "812d57d1deac31a1888709310fc88458c7a7007cfc159261d656f1dca7cad072",
        ),
        (
            256,
            "e94cc3cabd9d639ae6a5c010ee80d478cb098d02a0692f9cb1f22e1728716c43",
            "8268a030d902079de9b62b6e5a451e809f85181d360e28f245f49ddb6f343c1f",
        ),
        (
            1 << 129,
            "80d8f46c120224ca92ef6f00a71ad935adb3e7c2d5c45697ca676c63aab1d3a1",
            "08978f5ecfe550665aff43616f57704f3dc40f7ccb8b77ecc1acb8f551fe3831",
        ),
    ],
)
def test_scalar_fingerprint_preserves_opcode_sign_and_width(
    family, opcode, integer_fingerprint, float_fingerprint
) -> None:
    """Opcode framing remains stable across the byte boundary and for wider integers."""
    from fpstreams.expressions.scalar import Expr, FExpr

    expression = Expr.constant(1) if family == "integer" else FExpr.constant(1)
    operand = 1 << 257 if family == "integer" else -0.0
    object.__setattr__(expression, "_instructions", ((opcode, operand),))
    program = ExprProgram(expression, {}, Effect.MAY_RAISE)
    expected = integer_fingerprint if family == "integer" else float_fingerprint
    assert ProgramFingerprint.from_expression(program).value == expected


def test_program_fingerprint_frames_row_literals_without_delimiter_collisions() -> None:
    left = ProgramFingerprint.from_expression(
        compile_expression(lower_expression(lit(("a,str:b",))))
    )
    right = ProgramFingerprint.from_expression(
        compile_expression(lower_expression(lit(("a", "b"))))
    )

    assert left != right


def test_program_cache_isolates_float_nan_payloads_and_large_integer_constants() -> None:

    from fpstreams.expressions.scalar import Expr, FExpr

    first_bits = bytes.fromhex("7ff80000000000a1")
    second_bits = bytes.fromhex("7ff80000000000b2")
    first_nan = struct.unpack("!d", first_bits)[0]
    second_nan = struct.unpack("!d", second_bits)[0]
    first_float = compile_query(
        flow([0]).map(FExpr.constant(first_nan)).with_engine("python")._query("list")
    )
    second_float = compile_query(
        flow([0]).map(FExpr.constant(second_nan)).with_engine("python")._query("list")
    )
    first_integer = ProgramFingerprint.from_expression(
        compile_expression(lower_expression(Expr.constant(1 << 4_096)))
    )
    second_integer = ProgramFingerprint.from_expression(
        compile_expression(lower_expression(Expr.constant((1 << 4_096) + 1)))
    )

    assert isinstance(first_float.nodes[0], CompiledExpressionPhysicalNode)
    assert isinstance(second_float.nodes[0], CompiledExpressionPhysicalNode)
    assert first_float.nodes[0].program is not second_float.nodes[0].program
    assert first_integer != second_integer


def test_explicit_python_nan_payload_is_stable_across_scalar_fusion_boundary() -> None:

    from fpstreams.expressions.scalar import FExpr

    cached_bits = bytes.fromhex("7ff80000000000c3")
    expected_bits = bytes.fromhex("7ff80000000000d4")
    cached_nan = struct.unpack("!d", cached_bits)[0]
    expected_nan = struct.unpack("!d", expected_bits)[0]
    flow([0]).map(FExpr.constant(cached_nan)).with_engine("python").to_list()

    below = flow(range(4_095)).map(FExpr.constant(expected_nan)).with_engine("python").to_list()
    at_boundary = (
        flow(range(4_096)).map(FExpr.constant(expected_nan)).with_engine("python").to_list()
    )

    assert len(below) == 4_095
    assert len(at_boundary) == 4_096
    assert struct.pack("!d", below[0]) == expected_bits
    assert struct.pack("!d", below[-1]) == expected_bits
    assert struct.pack("!d", at_boundary[0]) == expected_bits
    assert struct.pack("!d", at_boundary[-1]) == expected_bits


@pytest.mark.parametrize("first_negative", [False, True])
@pytest.mark.parametrize("depth", [0, 70])
def test_float_constant_sign_survives_instruction_and_evaluator_caches(
    first_negative: bool, depth: int
) -> None:

    from fpstreams.expressions.scalar import FExpr, _compile_float_evaluator

    _compile_float_evaluator.cache_clear()
    fingerprints = []
    for negative in (first_negative, not first_negative, first_negative):
        expected_sign = -1.0 if negative else 1.0
        expression = FExpr.constant(-0.0 if negative else 0.0)
        for _ in range(depth):
            expression = expression * 1.0
        instructions = expression.native_instructions()
        assert math.copysign(1.0, instructions[0][1]) == expected_sign
        assert math.copysign(1.0, expression(7.0)) == expected_sign
        fingerprints.append(
            ProgramFingerprint.from_expression(ExprProgram(expression, {}, Effect.MAY_RAISE))
        )
    assert fingerprints[0] != fingerprints[1]
    assert fingerprints[0] == fingerprints[2]


@pytest.mark.parametrize("engine", ["python", "auto", "native"])
@pytest.mark.parametrize("first_negative", [False, True])
def test_float_constant_sign_is_independent_of_previously_compiled_plan(
    engine: str, first_negative: bool
) -> None:

    from fpstreams.expressions.scalar import FExpr, _compile_float_evaluator

    _compile_float_evaluator.cache_clear()
    for negative in (first_negative, not first_negative, first_negative):
        expected_sign = -1.0 if negative else 1.0
        expression = FExpr.constant(-0.0 if negative else 0.0)
        values = flow([1.0, 2.0]).with_engine(engine).map(expression).to_list()
        assert [math.copysign(1.0, value) for value in values] == [expected_sign] * 2


@pytest.mark.parametrize("first_negative", [False, True])
@pytest.mark.parametrize("policy", ["first", "last"])
def test_pair_float_constant_sign_keeps_the_native_value_map(
    monkeypatch: pytest.MonkeyPatch, first_negative: bool, policy: str
) -> None:

    from fpstreams import _native, fitem, pairs
    from fpstreams.expressions.scalar import FExpr, _compile_float_evaluator

    _compile_float_evaluator.cache_clear()
    monkeypatch.setattr(_pair_dict, "_PAIR_VALUE_MAP_MIN_ROWS", 0)
    endpoint = _native.pair_f64_value_map_to_dict_exact_prefix_v1
    calls = []

    def tracked(*arguments: object) -> object:
        calls.append("native")
        return endpoint(*arguments)

    monkeypatch.setattr(_native, "pair_f64_value_map_to_dict_exact_prefix_v1", tracked)
    for negative in (first_negative, not first_negative, first_negative):
        value = -0.0 if negative else 0.0
        result = (
            pairs([(1, 2.0), (1, 3.0)])
            .map_values(FExpr.constant(value) * fitem)
            .to_dict(on_duplicate=policy)
        )
        assert math.copysign(1.0, result[1]) == (-1.0 if negative else 1.0)
    assert calls == ["native", "native", "native"]


def test_float_evaluator_cache_distinguishes_explicit_signed_zero_instructions() -> None:

    from fpstreams.expressions.scalar import FExpr, _compile_float_evaluator

    _compile_float_evaluator.cache_clear()
    first = FExpr.constant(0.0)
    second = FExpr.constant(-0.0)
    # Isolate the evaluator-cache key from the instruction builder's normalization.
    object.__setattr__(first, "_instructions", ((1, 0.0),))
    object.__setattr__(second, "_instructions", ((1, -0.0),))
    assert math.copysign(1.0, first(7.0)) == 1.0
    assert math.copysign(1.0, second(7.0)) == -1.0


@pytest.mark.parametrize("numeric_type", [int, float], ids=["integer", "float"])
def test_generated_expression_preserves_custom_constant_inspection_order(numeric_type) -> None:
    from fpstreams.expressions.scalar import (
        Expr,
        FExpr,
        _compile_float_evaluator,
        _compile_int_evaluator,
    )

    events = []

    class Number(numeric_type):
        def __new__(cls, value, label):
            instance = numeric_type.__new__(cls, value)
            instance.label = label
            return instance

        @property
        def __class__(self):
            events.append(self.label)
            return numeric_type

    _compile_int_evaluator.cache_clear()
    _compile_float_evaluator.cache_clear()
    expression_type = Expr if numeric_type is int else FExpr
    expression = expression_type(
        "add",
        expression_type("const", value=Number(1, "left")),
        expression_type("const", value=Number(2, "right")),
    )

    with pytest.raises(TypeError, match="invalid type in Constant: Number"):
        expression(0)

    assert events == ["left", "left", "right", "right"]


def test_callback_program_has_no_compiled_fingerprint() -> None:
    expression = ExprProgram(lambda value: value + 1, {}, Effect.PYTHON_CALLBACK)

    with pytest.raises(ValueError, match="callback"):
        ProgramFingerprint.from_expression(expression)


@pytest.fixture
def isolated_scalar_caches():
    from fpstreams.expressions import scalar
    from fpstreams.planning import compiler

    def clear():
        scalar._compile_int_evaluator.cache_clear()
        scalar._compile_float_evaluator.cache_clear()
        compiler._EXPRESSION_PROGRAM_CACHE._entries.clear()
        compiler._PHYSICAL_TEMPLATE_CACHE._entries.clear()

    clear()
    yield
    clear()


@pytest.mark.parametrize(
    ("family", "first", "second"),
    [
        ("integer", 1, True),
        ("integer", 1, 1.0),
        ("float", 1.0, 1),
        ("float", float(2**53), 2**53 + 1),
    ],
)
@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("depth", [0, 70])
def test_scalar_evaluator_cache_keeps_direct_constant_types(
    isolated_scalar_caches, family, first, second, reverse, depth
) -> None:

    expression_type = scalar.Expr if family == "integer" else scalar.FExpr
    values = (second, first, second) if reverse else (first, second, first)
    for value in values:
        expression = expression_type("const", value=value)
        expected = value
        for _ in range(depth):
            expression = expression * 1
            expected *= 1 if family == "integer" else 1.0
        result = expression(0)
        assert type(result) is type(expected)
        assert result == expected


@pytest.mark.parametrize(
    ("family", "first", "second"),
    [
        ("integer", 1, True),
        ("integer", 1, 1.0),
        ("float", 1.0, 1),
        ("float", float(2**53), 2**53 + 1),
    ],
)
@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("engine", ["python", "auto"])
@pytest.mark.parametrize("size", [1, 4096])
def test_scalar_program_cache_keeps_direct_constant_types(
    isolated_scalar_caches, family, first, second, reverse, engine, size
) -> None:

    expression_type = scalar.Expr if family == "integer" else scalar.FExpr
    values = (second, first, second) if reverse else (first, second, first)
    for value in values:
        expression = expression_type("const", value=value)
        result = flow([0] * size).map(expression).with_engine(engine).to_list()
        assert len(result) == size
        assert all(type(item) is type(value) and item == value for item in result)


@pytest.mark.parametrize(
    ("family", "value"), [("integer", True), ("integer", 1.0), ("float", 2**53 + 1)]
)
def test_native_rejects_direct_constants_that_change_numeric_representation(
    isolated_scalar_caches, family, value
) -> None:
    from fpstreams.errors import NativeUnsupportedError

    expression_type = scalar.Expr if family == "integer" else scalar.FExpr
    expression = expression_type("const", value=value)
    with pytest.raises(NativeUnsupportedError, match="exact"):
        flow([0] * 4096).map(expression).with_engine("native").to_list()


@pytest.mark.parametrize("family", ["integer", "float"])
def test_scalar_cache_does_not_hide_invalid_custom_constants(
    isolated_scalar_caches, family
) -> None:

    numeric_type = int if family == "integer" else float
    expression_type = scalar.Expr if family == "integer" else scalar.FExpr
    events = []

    class Number(numeric_type):
        @property
        def __class__(self):
            events.append("class")
            return numeric_type

    assert expression_type.constant(1)(0) == 1
    with pytest.raises(TypeError, match="invalid type in Constant: Number"):
        expression_type("const", value=Number(1))(0)
    assert events == ["class", "class"]


@pytest.mark.parametrize("family", ["integer", "float"])
@pytest.mark.parametrize("route", ["direct", "python", "auto"])
def test_scalar_cache_keeps_custom_constant_identity_in_flat_programs(
    isolated_scalar_caches, family, route
) -> None:

    numeric_type = int if family == "integer" else float
    expression_type = scalar.Expr if family == "integer" else scalar.FExpr
    calls = []

    class Number(numeric_type):
        def __abs__(self):
            calls.append(self)
            return self

    first, second = Number(1), Number(1)
    assert first is not second
    for value in (first, second, first):
        calls.clear()
        expression = expression_type("const", value=value)
        for _ in range(140):
            expression = abs(expression)
        result = (
            expression(0)
            if route == "direct"
            else flow([0]).map(expression).with_engine(route).to_list()[0]
        )
        assert result is value
        assert len(calls) == 140
        assert all(called is value for called in calls)


@pytest.mark.parametrize("family", ["integer", "float"])
def test_scalar_fingerprint_declines_custom_numeric_protocols(family) -> None:

    calls = []

    class Meta(type):
        def __eq__(cls, other):
            calls.append("type equality")
            return True

        __hash__ = type.__hash__

    class Numeric(metaclass=Meta):
        def __abs__(self):
            calls.append("abs")
            return 1

        def __float__(self):
            calls.append("float")
            return 1.0

    expression_type = scalar.Expr if family == "integer" else scalar.FExpr
    expression = expression_type.constant(1)
    object.__setattr__(expression, "_instructions", ((1, Numeric()),))
    with pytest.raises(ValueError, match="exact"):
        ProgramFingerprint.from_expression(ExprProgram(expression, {}, Effect.MAY_RAISE))
    assert calls == []


@pytest.mark.parametrize("cache_type", [KernelCache, PlanCache], ids=["kernel", "plan"])
@pytest.mark.parametrize(
    "capacity",
    [float("nan"), float("inf"), 1.5, 2.0],
    ids=["nan", "infinity", "fractional", "integral-float"],
)
def test_cache_capacity_rejects_noninteger_values(cache_type, capacity) -> None:
    """Noninteger limits cannot silently disable or change the configured LRU bound."""
    with pytest.raises(TypeError):
        cache_type(max_entries=capacity)


@pytest.mark.parametrize("cache_type", [KernelCache, PlanCache], ids=["kernel", "plan"])
def test_cache_capacity_snapshots_and_releases_an_integer_protocol_object(cache_type) -> None:
    """A caller's later mutation cannot enlarge the cache or keep its config alive."""

    calls = []

    class Capacity:
        value = 2

        def __index__(self):
            calls.append(True)
            return self.value

        def __le__(self, other):
            return self.value <= other

        def __lt__(self, other):
            return self.value < other

    capacity = Capacity()
    reference = weakref.ref(capacity)
    cache = cache_type(max_entries=capacity)
    capacity.value = 1_000_000
    for label in ("a", "b", "c"):
        if cache_type is KernelCache:
            cache.get_or_compile(ProgramFingerprint(label), object)
        else:
            cache.put(_key(label), PhysicalPlanTemplate(object()))
    del capacity
    assert (len(cache), calls, reference() is None) == (2, [True], True)


def test_kernel_cache_is_structural_and_bounded() -> None:
    cache = KernelCache(max_entries=2)
    first_fingerprint = ProgramFingerprint("a")
    first = cache.get_or_compile(first_fingerprint, object)

    assert cache.get_or_compile(first_fingerprint, object) is first
    second = cache.get_or_compile(ProgramFingerprint("b"), object)
    third = cache.get_or_compile(ProgramFingerprint("c"), object)
    assert len(cache) == 2

    assert cache.get_or_compile(ProgramFingerprint("b"), object) is second
    cache.get_or_compile(ProgramFingerprint("d"), object)
    assert cache.get_or_compile(ProgramFingerprint("b"), object) is second
    assert cache.get_or_compile(ProgramFingerprint("c"), object) is not third
    assert len(cache) == 2


@pytest.mark.parametrize("fail_first", [False, True], ids=["shared-result", "retry-after-failure"])
def test_kernel_cache_coordinates_concurrent_compilation(fail_first: bool) -> None:
    """Concurrent callers share a compiled value; a failed attempt stays retryable."""
    from threading import Barrier, Event, get_ident

    cache = KernelCache()
    fingerprint = ProgramFingerprint("shared-compilation")
    barrier = Barrier(5)
    entered = Event()
    release = Event()
    failure = RuntimeError("first compilation failed")
    descriptor = object()
    calls = 0

    def compile_descriptor() -> object:
        nonlocal calls
        calls += 1
        entered.set()
        assert release.wait(timeout=10), "compiler was not released"
        if fail_first and calls == 1:
            raise failure
        return descriptor

    def request() -> tuple[int, object]:
        thread_id = get_ident()
        barrier.wait(timeout=10)
        try:
            result = cache.get_or_compile(fingerprint, compile_descriptor)
        except RuntimeError as error:
            result = error
        return thread_id, result

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(request) for _ in range(4)]
        try:
            barrier.wait(timeout=10)
            assert entered.wait(timeout=10), "no caller entered the compiler"
        finally:
            release.set()
        outcomes = [future.result(timeout=10) for future in futures]

    assert len({thread_id for thread_id, _ in outcomes}) == 4
    assert sum(value is failure for _, value in outcomes) == int(fail_first)
    assert sum(value is descriptor for _, value in outcomes) == 4 - int(fail_first)
    assert calls == 1 + int(fail_first)
    assert len(cache) == 1
    assert cache.get_or_compile(
        fingerprint, lambda: pytest.fail("cached descriptor recompiled")
    ) is (descriptor)


@pytest.mark.parametrize("family", ["integer", "float"])
def test_compiler_reuses_a_structurally_equal_expression_program(
    family: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each scalar stage builds one fingerprint, shared by program and template caches."""
    original = ProgramFingerprint.from_expression
    calls = 0

    def fingerprint(cls, program):
        nonlocal calls
        calls += 1
        return original(program)

    monkeypatch.setattr(ProgramFingerprint, "from_expression", classmethod(fingerprint))
    first_expression = item * 2 + 1 if family == "integer" else fitem * 2.0 + 1.0
    second_expression = item * 2 + 1 if family == "integer" else fitem * 2.0 + 1.0
    assert first_expression is not second_expression
    first = compile_query(flow([1]).map(first_expression)._query("list"))
    second = compile_query(flow([1]).map(second_expression)._query("list"))

    assert calls == 2
    assert isinstance(first.nodes[0], CompiledExpressionPhysicalNode)
    assert isinstance(second.nodes[0], CompiledExpressionPhysicalNode)
    assert first.nodes[0].program is second.nodes[0].program
    assert first.nodes[0].fingerprint is not None
    assert first.nodes[0].fingerprint == second.nodes[0].fingerprint
    assert list(execute_physical(first)) == list(execute_physical(second)) == [3]


def test_linear_pipeline_preserves_source_operations_and_options() -> None:
    """The canonical linear view preserves a logical pipeline exactly."""
    source = Source.from_iterable([1, 2, 3])
    logical = (
        LogicalPlan(SourceNode(source), "python")
        .append(MapOp(str))
        .append(FilterOp(bool))
        .append(TakeOp(2))
    )

    pipeline = linear_pipeline(logical)

    assert pipeline.source is source
    assert [type(operation) for operation in pipeline.operations] == [MapOp, FilterOp, TakeOp]
    assert pipeline.engine == "python"
    source_node, nodes = unary_chain(logical.root)
    assert source_node.source is source
    assert [type(node.operation) for node in nodes] == [MapOp, FilterOp, TakeOp]


def test_query_freezes_terminal_options_in_order() -> None:
    """Terminal metadata is immutable and preserves caller option ordering."""
    logical = LogicalPlan(SourceNode(Source.from_iterable([1])))
    terminal = TerminalSpec("sum", arguments=(10,), options=(("strict", False),))
    query = Query(logical, terminal)

    assert query.terminal == terminal


def test_flow_owns_logical_plan_and_exposes_canonical_linear_view() -> None:
    """A Flow stores a tree and derives its unopened execution view."""
    pipeline = flow([1, 2, 3]).map(abs).filter(bool).take(1)

    assert isinstance(pipeline._logical_plan, LogicalPlan)
    first = pipeline._pipeline
    second = pipeline._pipeline
    assert first == second
    assert first is not second
    assert [type(operation).__name__ for operation in first.operations] == [
        "MapOp",
        "FilterOp",
        "TakeOp",
    ]


def test_build_compile_and_explain_do_not_open_source() -> None:
    """Tree construction and physical compilation remain purely descriptive."""
    opens = 0

    def factory():
        nonlocal opens
        opens += 1
        return iter([1, 2, 3])

    pipeline = flow.defer(factory).map(abs)
    _ = pipeline._pipeline
    _ = pipeline.explain()

    assert opens == 0


def test_pipeline_append_preserves_immutable_operation_order() -> None:
    """The canonical linear value remains immutable and appends in order."""
    source = Source.from_iterable([1, 2])
    pipeline = Pipeline(source, (MapOp(abs),)).append(DropOp(1)).append(TakeOp(1))

    assert [type(operation) for operation in pipeline.operations] == [MapOp, DropOp, TakeOp]


def test_compile_query_maps_each_unary_node_once_in_order() -> None:
    """Every unary logical node becomes one ordered row physical node."""
    pipeline = flow(range(10)).map(abs).filter(bool).drop(1).take(2)
    physical = compile_query(pipeline._query("list"))

    assert isinstance(physical, PhysicalPlan)
    assert [type(node).__name__ for node in physical.nodes] == [
        "RowPhysicalNode",
        "RowPhysicalNode",
        "RowPhysicalNode",
        "RowPhysicalNode",
    ]
    assert [type(node.operation).__name__ for node in physical.nodes] == [
        "MapOp",
        "FilterOp",
        "DropOp",
        "TakeOp",
    ]
    assert [node.logical_ids for node in physical.nodes] == [(0,), (1,), (2,), (3,)]


def test_compile_query_does_not_open_deferred_source() -> None:
    """Compilation remains source-safe even when choosing a native terminal backend."""
    opens = 0

    def values():
        nonlocal opens
        opens += 1
        return iter([1])

    physical = compile_query(flow.defer(values)._query("first"))

    assert physical.terminal.name == "first"
    assert opens == 0


def test_physical_operations_preserve_the_compiled_linear_pipeline() -> None:
    """Row stage payloads retain source order without rebuilding a plan object."""
    pipeline = flow(range(10)).map(abs).take(3).with_engine("python")
    physical = compile_query(pipeline._query("list"))
    restored = operations_from_physical_nodes(physical.nodes)

    assert restored == pipeline._pipeline.operations


def test_physical_executor_rejects_unknown_physical_node() -> None:
    """Future physical nodes cannot silently run through the row executor."""
    physical = compile_query(flow([1])._query("list"))
    changed = replace(physical, nodes=(PhysicalNode((0,), "unknown"),))

    with pytest.raises(PhysicalExecutionError, match="PhysicalNode"):
        operations_from_physical_nodes(changed.nodes)


def test_python_physical_execution_uses_compiled_operations_directly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The canonical Python physical path executes node operations directly."""
    physical = compile_query(flow([1, 2, 3]).map(lambda value: value + 1)._query("list"))

    def reject_reselection(*_args: object, **_kwargs: object) -> Iterator[object]:
        raise AssertionError("compiled Python operations must not re-enter backend dispatch")

    monkeypatch.setattr(physical_executor, "execute", reject_reselection)

    assert list(physical_executor.execute_physical(physical)) == [2, 3, 4]


def test_native_physical_execution_reuses_compiled_backend_decision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Native dispatch receives the compiler's decision instead of selecting a backend again."""
    from fpstreams import execution

    physical = compile_query(flow(range(20)).map(item + 1)._query("list"))

    def reject_reselection(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("physical execution must not select the native backend again")

    monkeypatch.setattr(execution, "select_materializing_engine", reject_reselection)

    assert list(physical_executor.execute_physical(physical)) == list(range(1, 21))


def test_rows_join_is_a_binary_logical_node_without_opening_sources() -> None:
    opens: list[str] = []

    def source(name: str, values: list[dict[str, int]]):
        def factory():
            opens.append(name)
            return iter(values)

        return rows(flow.defer(factory))

    joined = source("left", [{"id": 1}]).join(source("right", [{"id": 1}]), on="id")

    assert isinstance(joined._flow._logical_plan.root, JoinNode)
    assert opens == []


@pytest.mark.parametrize(
    ("left_engine", "right_engine", "expected"),
    [
        ("auto", "python", "python"),
        ("python", "auto", "python"),
        ("auto", "native", "native"),
        ("native", "auto", "native"),
        ("auto", "auto", "auto"),
        ("python", "python", "python"),
    ],
)
def test_join_merges_compatible_query_wide_engine_requests(
    left_engine: str,
    right_engine: str,
    expected: str,
) -> None:
    left = rows([{"id": 1}]).with_engine(left_engine)
    right = rows([{"id": 1}]).with_engine(right_engine)

    joined = left.join(right, on="id")

    assert joined._flow._logical_plan.engine == expected


@pytest.mark.parametrize(
    ("left_engine", "right_engine"),
    [("python", "native"), ("native", "python")],
)
def test_join_rejects_conflicting_query_wide_engine_requests_without_opening(
    left_engine: str,
    right_engine: str,
) -> None:
    opened: list[str] = []

    def source(name: str):
        return rows(flow.defer(lambda: opened.append(name) or iter([{"id": 1}])))

    with pytest.raises(ValueError, match="conflicting join engine requests"):
        source("left").with_engine(left_engine).join(
            source("right").with_engine(right_engine),
            on="id",
        )

    assert opened == []


def test_right_python_engine_disables_auto_arrow_join_selection() -> None:
    pa = pytest.importorskip("pyarrow")
    table = pa.table({"id": range(128)})
    joined = rows.from_arrow(table).join(
        rows.from_arrow(table).with_engine("python"),
        on="id",
        validate="m:1",
    )

    physical = compile_query(joined._flow._query("list"))

    assert isinstance(physical.root, JoinPhysicalNode)
    assert physical.root.arrow_unique is None


def test_group_and_global_aggregate_are_logical_nodes() -> None:
    source = rows([{"team": "a", "score": 1}])

    grouped = source.group_by("team").aggregate(total=fpstreams.agg.sum("score"))
    global_result = source.aggregate(total=fpstreams.agg.sum("score"))

    assert isinstance(grouped._flow._logical_plan.root, GroupAggregateNode)
    assert isinstance(global_result._flow._logical_plan.root, GlobalAggregateNode)


def test_relational_compile_does_not_open_sources_and_chooses_legal_strategy() -> None:
    opens: list[str] = []

    def factory(name: str):
        def values():
            opens.append(name)
            return iter([{"id": 1}])

        return rows(flow.defer(values))

    physical = compile_query(factory("left").join(factory("right"), on="id")._flow._query("list"))

    assert isinstance(physical.root, JoinPhysicalNode)
    assert physical.root.strategy.value == "hash_right"
    assert opens == []


@pytest.mark.parametrize("relation", ["join", "group", "global"])
@pytest.mark.parametrize("force_position", ["before", "after"])
def test_forced_native_relations_fail_before_claiming_one_shot_sources(
    relation: str, force_position: str
) -> None:
    """Unsupported relational trees must reject forced native before opening either input."""
    left = rows(iter([{"id": 1, "score": 2}]))
    if force_position == "before":
        left = left.with_engine("native")

    if relation == "join":
        query = left.join(rows(iter([{"id": 1, "label": "one"}])), on="id")
        expected = [{"id": 1, "score": 2, "label": "one"}]
    elif relation == "group":
        query = left.group_by("id").aggregate(total=fpstreams.agg.sum("score"))
        expected = [{"id": 1, "total": 2}]
    else:
        query = left.aggregate(total=fpstreams.agg.sum("score"))
        expected = [{"total": 2}]

    if force_position == "after":
        query = query.with_engine("native")

    with (
        failpoint("source.open.after", AssertionError("forced native opened a source")),
        pytest.raises(fpstreams.NativeUnsupportedError, match="not native-compilable"),
    ):
        query.to_list()

    # A source opened before the planning error would already be claimed and this
    # public retry would raise FlowConsumedError instead of producing the records.
    assert query.with_engine("python").to_list() == expected


def test_relational_explain_reports_the_selected_strategy_without_opening() -> None:
    opened: list[bool] = []

    def values():
        opened.append(True)
        return iter([{"id": 1}])

    explanation = (
        rows(flow.defer(values))
        .join(rows([{"id": 1}]), on="id")
        ._flow.map(lambda record: record["id"])
        .explain(terminal="list")
        .to_dict()
    )

    assert explanation["selection_reason"] == (
        "relational physical tree uses the Python record executor"
    )
    assert explanation["operations"] == [{"name": "map"}]
    assert explanation["stages"] == [{"engine": "python", "operations": ["map"], "fused": False}]
    assert explanation["relations"]["node"] == "pipeline"
    assert explanation["relations"]["children"][0]["strategy"] == "hash_right"
    assert opened == []


def _numeric_relational_flow(kind: str):
    """Build one relation whose scalar output has a hand-checked encounter order."""
    if kind == "join":
        return (
            rows([{"id": 1, "value": 0}, {"id": 2, "value": 0}])
            .join(
                rows([{"id": 1, "tag": "a"}, {"id": 1, "tag": "b"}, {"id": 2, "tag": "c"}]),
                on="id",
            )
            ._flow.map(lambda record: record["value"])
        )
    if kind == "group":
        return (
            rows(
                [
                    {"team": "a", "score": 1},
                    {"team": "a", "score": -1},
                    {"team": "b", "score": 2},
                    {"team": "b", "score": -2},
                ]
            )
            .group_by("team")
            .aggregate(total=agg.sum("score"))
            ._flow.map(lambda record: record["total"])
        )
    if kind == "global":
        return (
            rows([{"score": 1}, {"score": -1}])
            .aggregate(total=agg.sum("score"))
            ._flow.map(lambda record: record["total"])
        )
    raise AssertionError(f"unknown relation fixture {kind!r}")


@pytest.mark.parametrize("kind", ["i64", "f64", "buffer"])
def test_statistics_large_offset_native_and_python_match_exact_oracle(kind):
    from fractions import Fraction

    if kind == "i64":
        values = [10**16 + 2 * index for index in [0, 1, 2, 1, 3] * 17]
        source = values
        expression = item + 0
    else:
        values = [1e12 + index / 1024 for index in [0, 1, 2, 1, 3] * 17]
        source = pytest.importorskip("numpy").array(values) if kind == "buffer" else values
        expression = fitem + 0.0
    exact = [Fraction(value) for value in values]
    center = sum(exact) / len(exact)
    squared = sum((value - center) ** 2 for value in exact)
    for engine in ("python", "native"):
        pipeline = (
            (flow.from_numpy(source) if kind == "buffer" else flow(source))
            .map(expression)
            .with_engine(engine)
        )
        for ddof in (0, 1):
            expected = float(squared / (len(values) - ddof))
            assert pipeline.variance(ddof=ddof) == pytest.approx(expected, rel=1e-14)
            assert pipeline.std(ddof=ddof) == pytest.approx(expected**0.5, rel=1e-14)
            result = pipeline.aggregate(variance=agg.variance(ddof=ddof), std=agg.std(ddof=ddof))
            assert result["variance"] == pytest.approx(expected, rel=1e-14)
            assert result["std"] == pytest.approx(expected**0.5, rel=1e-14)


def _run_numeric_terminal(values, terminal: str):
    """Apply one public scalar terminal to a linear or relational Flow."""
    actions = {
        "count": values.count,
        "last": values.last,
        "any": values.any,
        "all": values.all,
        "min": values.min,
        "max": values.max,
        "sum": values.sum,
        "mean": values.mean,
        "variance": values.variance,
        "std": values.std,
        "aggregate_count": lambda: values.aggregate(count=agg.count()),
    }
    return actions[terminal]()


@pytest.mark.parametrize(
    ("relation", "expected_values"),
    [("join", [0, 0, 0]), ("group", [0, 0]), ("global", [0])],
)
@pytest.mark.parametrize(
    "terminal",
    [
        "count",
        "last",
        "any",
        "all",
        "min",
        "max",
        "sum",
        "mean",
        "variance",
        "std",
        "aggregate_count",
    ],
)
def test_relational_terminals_match_the_materialized_linear_oracle(
    relation: str, expected_values: list[int], terminal: str
) -> None:
    """A terminal must consume the relation result, never its leftmost source."""
    assert _numeric_relational_flow(relation).to_list() == expected_values
    expected = _run_numeric_terminal(flow(expected_values), terminal)
    actual = _run_numeric_terminal(_numeric_relational_flow(relation), terminal)

    if isinstance(expected, float):
        assert actual == pytest.approx(expected)
    else:
        assert actual == expected


@pytest.mark.parametrize("relation", ["join", "group", "global"])
@pytest.mark.parametrize("terminal", ["last", "min", "max"])
def test_empty_relational_terminals_raise_the_public_empty_error(
    relation: str, terminal: str
) -> None:
    """An empty relation must not leak linear-plan TypeError from a terminal."""
    empty = _numeric_relational_flow(relation).filter(lambda _value: False)

    with pytest.raises(fpstreams.EmptyFlowError):
        getattr(empty, terminal)()


def test_relational_short_circuit_terminal_compiles_once_and_closes_sources(monkeypatch) -> None:
    """A relation terminal owns one plan and closes both branches after an early answer."""
    from fpstreams.streams import flow_terminals

    events: list[str] = []

    def left_values():
        events.append("left:open")
        try:
            yield {"id": 1, "value": 10}
            yield {"id": 2, "value": 20}
        finally:
            events.append("left:close")

    def right_values():
        events.append("right:open")
        try:
            yield {"id": 1, "tag": "match"}
        finally:
            events.append("right:close")

    query = (
        rows(flow.defer(left_values))
        .join(rows(flow.defer(right_values)), on="id")
        ._flow.map(lambda record: record["value"])
    )
    original_compile = flow_terminals.compile_query
    compile_calls = 0

    def tracked_compile(request):
        nonlocal compile_calls
        compile_calls += 1
        return original_compile(request)

    monkeypatch.setattr(flow_terminals, "compile_query", tracked_compile)

    assert query.any(lambda value: value == 10)
    assert compile_calls == 1
    assert events.count("left:open") == events.count("left:close") == 1
    assert events.count("right:open") == events.count("right:close") == 1


def test_native_mask_contains_only_requested_snapshot_fields() -> None:
    """A native snapshot request exposes just the exact fields finishers need."""
    program = compile_aggregations(
        prepare_aggregations({"rows": agg.count(), "minimum": agg.min(), "average": agg.mean()})
    )

    assert program.native_mask is not None
    assert program.native_mask.fields == frozenset(
        {NativeAggregateField.COUNT, NativeAggregateField.MINIMUM, NativeAggregateField.MEAN}
    )
    assert program.native_mask.bits == (1 << 0) | (1 << 2) | (1 << 6)


def test_merge_rejects_non_mergeable_collector_before_mutation() -> None:
    """Merge validation occurs before a destination state is touched."""
    collector = Collector(list, lambda state, value: [*state, value])
    program = compile_collectors((("items", collector),))
    left = program.initialize()
    right = program.initialize()
    before = left.values.copy()

    with pytest.raises(ValueError, match=r"items.*not mergeable"):
        program.merge(left, right)

    assert left.values == before


@pytest.mark.parametrize("engine", ["python", "native"])
def test_empty_aggregation_finishes_with_existing_values(engine) -> None:
    """M6 program layers preserve native and Python empty aggregation outputs."""
    result = (
        flow([])
        .with_engine(engine)
        .aggregate(count=agg.count(), total=agg.sum(), first=agg.first(), mean=agg.mean())
    )

    assert result == {"count": 0, "total": 0, "first": None, "mean": None}


def test_native_materializer_executes_the_selected_rust_kernel(monkeypatch) -> None:
    """A forced-native multi-stage list terminal must execute its selected Rust kernel."""
    from fpstreams import _native

    original = _native.materialize_i64_range
    calls = 0

    def tracked(*args):
        nonlocal calls
        calls += 1
        return original(*args)

    monkeypatch.setattr(_native, "materialize_i64_range", tracked)
    pipeline = flow(range(64)).map(item * 3 + 1).filter(item % 2 == 0).with_engine("native")

    assert pipeline.to_list() == list(range(4, 64 * 3, 6))
    assert calls == 1


@pytest.mark.parametrize("terminal", ["first", "any", "all", "find", "nth"])
def test_low_latency_terminal_reports_its_streaming_route(terminal) -> None:
    """Short-circuit terminals retain an explicit one-row-at-a-time decision guard."""
    physical = compile_query(flow(range(100)).map(item + 1)._query(terminal))

    assert "row_mode: low_latency_terminal" in physical.decision.guards


def test_single_collector_uses_specialized_state_without_name_lookup() -> None:
    """One collector retains a fixed layout and yields its named result."""
    program = compile_collectors((("total", Collectors.summing()),))

    assert program.layout.names == ("total",)
    assert program.single
    assert run_collector_program([1, 2, 3], program) == {"total": 6}


def test_never_done_collector_program_skips_completion_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exhaustive collectors use their straight-line loop without per-item program calls."""
    program = compile_collectors((("total", Collectors.summing()),))

    def unexpected(*_args: object) -> None:
        raise AssertionError("never-done fast path dispatched through CollectorProgram")

    monkeypatch.setattr(type(program), "done", unexpected)
    monkeypatch.setattr(type(program), "step", unexpected)

    assert run_collector_program([1, 2, 3], program) == {"total": 6}


@pytest.mark.parametrize("collector_count", [1, 2])
def test_never_done_collector_program_does_not_reiterate_a_custom_iterator(
    collector_count: int,
) -> None:
    """The straight-line loop preserves the source's single initial iter() call."""

    class SinglePassIterator:
        def __init__(self) -> None:
            self.index = 0
            self.iterations = 0
            self.closes = 0

        def __iter__(self) -> SinglePassIterator:
            self.iterations += 1
            if self.iterations > 1:
                raise RuntimeError("iter called twice")
            return self

        def __next__(self) -> int:
            if self.index == 3:
                raise StopIteration
            self.index += 1
            return self.index

        def close(self) -> None:
            self.closes += 1

    values = SinglePassIterator()
    items = tuple((f"total_{index}", Collectors.summing()) for index in range(collector_count))

    assert run_collector_program(values, compile_collectors(items)) == {
        f"total_{index}": 6 for index in range(collector_count)
    }
    assert values.iterations == 1
    assert values.closes == 1


def test_single_never_done_collector_releases_replaced_state_during_iteration() -> None:
    """A replaced initializer state can affect a live source at the canonical boundary."""
    values = [1]

    class State:
        def __init__(self, count: int, *, append_on_delete: bool = False) -> None:
            self.count = count
            self.append_on_delete = append_on_delete

        def __del__(self) -> None:
            if self.append_on_delete:
                self.append_on_delete = False
                values.append(2)

    collector = Collector(
        lambda: State(0, append_on_delete=True),
        lambda state, _value: State(state.count + 1),
        lambda state: state.count,
    )

    assert run_collector_program(values, compile_collectors((("count", collector),))) == {
        "count": 2
    }
    assert values == [1, 2]


def test_empty_collector_program_does_not_pull_its_source() -> None:
    """The exhaustive fast path retains vacuous completion for an empty layout."""
    pulls: list[int] = []
    values = (pulls.append(value) or value for value in range(3))

    assert run_collector_program(values, compile_collectors(())) == {}
    assert pulls == []


def test_collector_program_caches_completion_instead_of_rechecking_old_state() -> None:
    """A done predicate runs once per new state and never again after completion."""
    checks: list[int] = []

    def done(total: int) -> bool:
        checks.append(total)
        return total >= 3

    program = compile_collectors(
        (("total", Collector(lambda: 0, lambda total, value: total + value, done=done)),)
    )
    state = program.initialize()

    program.step(state, 1)
    program.step(state, 2)
    program.step(state, 100)

    assert state.values == [3]
    assert checks == [0, 1, 3]


def test_multiple_collectors_step_in_declaration_order_once_per_item() -> None:
    """Every item reaches unfinished collectors in caller-declared keyword order."""
    events: list[tuple[str, int]] = []

    def collector(name):
        return Collector(
            lambda: 0,
            lambda state, value: events.append((name, value)) or state + value,
        )

    program = compile_collectors((("left", collector("left")), ("right", collector("right"))))

    assert run_collector_program([1, 2], program) == {"left": 3, "right": 3}
    assert events == [("left", 1), ("right", 1), ("left", 2), ("right", 2)]


def test_source_stops_when_every_collector_is_done() -> None:
    """Early-done collectors prevent an additional source pull."""
    pulls: list[int] = []
    values = (pulls.append(value) or value for value in range(100))
    program = compile_collectors((("first", Collectors.first()),))

    assert run_collector_program(values, program) == {"first": 0}
    assert pulls == [0]
