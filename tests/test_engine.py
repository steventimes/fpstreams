# ruff: noqa: E402, F811
"""Consolidated fpstreams test cases."""

from __future__ import annotations

import sys
from collections.abc import Iterator, Mapping

import pytest

# --- Consolidated from adaptive/test_plan_cache.py ---

"""Structural plan templates are bounded and never own source objects."""


from fpstreams import col, flow, item, lit
from fpstreams.execution.physical import execute_physical
from fpstreams.planning.compiler import compile_query
from fpstreams.planning.plan_cache import PhysicalPlanTemplate, PlanCache, PlanCacheKey


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
    from fpstreams.execution import physical
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

    from fpstreams.execution import physical
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
    from fpstreams.execution import physical
    from fpstreams.planning.compiler import compile_iteration
    from fpstreams.planning.source import Source, SourceCapabilities

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
    from types import FunctionType

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
    import fpstreams
    from fpstreams.planning._pair_stages import PairMapDescriptor

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
    from fpstreams.planning._pair_stages import PairMapDescriptor
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
    import fpstreams
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

    first_key = int("1000")
    equal_key = int("1000")
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
    assert output == {1000: expected}
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
    import fpstreams
    from fpstreams import _native, fitem
    from fpstreams.execution import _pair_dict

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
    import fpstreams
    from fpstreams import _native, fitem
    from fpstreams.execution import _pair_dict

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
    import fpstreams
    from fpstreams import _native
    from fpstreams.execution import _pair_dict

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
    from fpstreams.execution import _pair_dict

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
    import fpstreams
    from fpstreams import fitem
    from fpstreams.execution import _pair_dict

    monkeypatch.setattr(_pair_dict, "_PAIR_VALUE_MAP_MIN_ROWS", 0)
    integer = int("1000")
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

    assert integer_output == {"identity": 1000, "overflow": 1 << 70, "appended": 3}
    assert integer_output["identity"] is integer
    assert float_output["float"] is floating


def test_pair_value_map_native_boundary_preserves_numeric_exceptions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Native evaluation failures resume at the untouched row and raise canonically."""
    import fpstreams
    from fpstreams import _native, fitem
    from fpstreams.execution import _pair_dict

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
    import fpstreams
    from fpstreams import _native, fitem
    from fpstreams.execution import _pair_dict

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
    import fpstreams
    from fpstreams import _native, fitem
    from fpstreams.execution import _pair_dict

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
    import fpstreams
    from fpstreams import _native
    from fpstreams.execution import _pair_dict
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
    import fpstreams
    from fpstreams import _native
    from fpstreams.execution import _pair_dict
    from fpstreams.planning._pair_stages import PairMapDescriptor

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
    import fpstreams
    from fpstreams import _native
    from fpstreams.execution import _pair_dict

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
    import signal

    import fpstreams
    from fpstreams.execution._pair_dict import try_consume_pair_value_map_to_dict_opened

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
    import signal

    import fpstreams
    from fpstreams.execution._pair_dict import (
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


def test_pair_value_map_post_open_failpoint_closes_the_source_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A post-open deopt retains exactly one cleanup owner for the live source."""
    from contextlib import AbstractContextManager

    import fpstreams
    from fpstreams import _native
    from fpstreams.execution import _pair_dict
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
    from fpstreams.planning._pair_stages import PairMapDescriptor
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
    import fpstreams
    from fpstreams.execution.sync import open_operations
    from fpstreams.planning._pair_stages import PairFlatMapDescriptor
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
    from fpstreams.planning._pair_stages import PairFilterDescriptor

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
    import fpstreams
    from fpstreams.planning._pair_stages import PairFilterDescriptor

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
    from fpstreams.planning._pair_stages import PairFilterDescriptor
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
    from fpstreams.planning._pair_stages import PairFilterDescriptor
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
    from types import FunctionType

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
    from pathlib import Path

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
    import builtins

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
    import builtins

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
    import fpstreams

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
    import weakref

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
    from fpstreams.planning.source import Source, SourceCapabilities

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


# --- Consolidated from expressions/test_expression_program.py ---

"""Reusable, source-safe compiled row-expression program contracts."""


from fpstreams import coalesce
from fpstreams.expressions import program as program_module
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
from fpstreams.expressions.typed_ir import lower_expression


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


# --- Consolidated from expressions/test_typed_ir.py ---

"""Conservative typed-expression lowering contracts."""


from fpstreams import fitem
from fpstreams.expressions.typed_ir import Effect, ExpressionSource, TypedExpr, ValueType


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
    lowered = lower_expression(col("value").map(lambda value: value + 1))

    assert lowered.effect is Effect.PYTHON_CALLBACK
    assert lowered.backends == frozenset({"python"})


# --- Consolidated from planning/test_async_physical.py ---

"""Async physical planning remains lazy and exposes concurrency boundaries."""


from fpstreams import aflow
from fpstreams.physical.async_plan import compile_async_query


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


# --- Consolidated from planning/test_compiled_backends.py ---

"""M9 backend-neutral program descriptors remain deterministic and source-safe."""


import pytest

from fpstreams.physical.compiled import ProgramFingerprint
from fpstreams.physical.kernel_cache import KernelCache
from fpstreams.physical.plan import CompiledExpressionPhysicalNode
from fpstreams.planning.source import Source


def test_program_fingerprint_is_structural_and_address_free() -> None:
    left = ProgramFingerprint.from_expression(compile_expression(lower_expression(item * 2 + 1)))
    right = ProgramFingerprint.from_expression(compile_expression(lower_expression(item * 2 + 1)))

    assert left == right
    assert "0x" not in left.value


def test_program_fingerprint_frames_row_literals_without_delimiter_collisions() -> None:
    left = ProgramFingerprint.from_expression(
        compile_expression(lower_expression(lit(("a,str:b",))))
    )
    right = ProgramFingerprint.from_expression(
        compile_expression(lower_expression(lit(("a", "b"))))
    )

    assert left != right


def test_program_cache_isolates_float_nan_payloads_and_large_integer_constants() -> None:
    import struct

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
    import struct

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


def test_callback_program_has_no_compiled_fingerprint() -> None:
    expression = ExprProgram(lambda value: value + 1, {}, Effect.PYTHON_CALLBACK)

    with pytest.raises(ValueError, match="callback"):
        ProgramFingerprint.from_expression(expression)


def test_kernel_cache_is_structural_and_bounded() -> None:
    cache = KernelCache(max_entries=2)
    first_fingerprint = ProgramFingerprint("a")
    first = cache.get_or_compile(first_fingerprint, object)

    assert cache.get_or_compile(first_fingerprint, object) is first
    cache.get_or_compile(ProgramFingerprint("b"), object)
    cache.get_or_compile(ProgramFingerprint("c"), object)
    assert len(cache) == 2


def test_compiler_reuses_a_structurally_equal_expression_program() -> None:
    first = compile_query(flow([1]).map(item * 2 + 1)._query("list"))
    second = compile_query(flow([1]).map(item * 2 + 1)._query("list"))

    assert isinstance(first.nodes[0], CompiledExpressionPhysicalNode)
    assert isinstance(second.nodes[0], CompiledExpressionPhysicalNode)
    assert first.nodes[0].program is second.nodes[0].program


# --- Consolidated from planning/test_logical_plan.py ---

"""Contracts for the synchronous immutable logical representation."""


from fpstreams.planning.logical import (
    LogicalPlan,
    Pipeline,
    Query,
    SourceNode,
    TerminalSpec,
    linear_pipeline,
    unary_chain,
)
from fpstreams.planning.sync import (
    DropOp,
    FilterOp,
    MapOp,
    TakeOp,
)


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


# --- Consolidated from planning/test_physical_plan.py ---

"""Contracts for compiling logical queries to M2 physical plans."""


from dataclasses import replace

import pytest

from fpstreams.execution import physical as physical_executor
from fpstreams.execution.physical import PhysicalExecutionError, operations_from_physical_nodes
from fpstreams.physical.plan import PhysicalNode, PhysicalPlan


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


# --- Consolidated from planning/test_relational_logical.py ---

"""M8 relational logical plans remain lazy and source-safe."""


import fpstreams
from fpstreams.physical.relational import JoinPhysicalNode
from fpstreams.planning.logical import (
    GlobalAggregateNode,
    GroupAggregateNode,
    JoinNode,
)
from fpstreams.tabular import rows


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


# --- Consolidated from execution/test_aggregate_program.py ---

"""Compiled aggregation masks and merge-safety contracts."""


import pytest

from fpstreams import agg
from fpstreams.collecting import Collector
from fpstreams.collecting.aggregate_program import (
    NativeAggregateField,
    compile_aggregations,
)
from fpstreams.collecting.aggregation import prepare_aggregations
from fpstreams.collecting.program import compile_collectors


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


# --- Consolidated from execution/test_collector_program.py ---

"""Indexed collector-program semantics and ordering contracts."""


from fpstreams.collecting import Collectors
from fpstreams.collecting.program import run_collector_program


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


# --- Consolidated from execution/test_relational_operators.py ---

"""Execution checks for M8 relational physical trees."""


from fpstreams.physical.relational import (
    GroupAggregatePhysicalNode,
    JoinStrategy,
    NativeGroupSumSpec,
)


def test_relational_nodes_preserve_join_and_aggregate_results() -> None:
    left = fpstreams.rows([{"id": 1, "left": "a"}, {"id": 2, "left": "b"}])
    right = fpstreams.rows([{"id": 1, "right": "x"}, {"id": 1, "right": "y"}])

    assert left.join(right, on="id", how="left").to_list() == [
        {"id": 1, "left": "a", "right": "x"},
        {"id": 1, "left": "a", "right": "y"},
        {"id": 2, "left": "b", "right": None},
    ]
    assert left.group_by("id").aggregate(total=fpstreams.agg.count()).to_list() == [
        {"id": 1, "total": 1},
        {"id": 2, "total": 1},
    ]


@pytest.mark.parametrize("engine", ["auto", "python"])
def test_exact_selected_group_sum_is_visible_to_the_relational_planner(engine: str) -> None:
    """One raw field key and selected sum expose the dedicated Python loop contract."""
    grouped = (
        fpstreams.rows([{"key": "a", "value": 2}, {"key": "a", "value": 3}])
        .with_engine(engine)
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
    )

    physical = compile_query(grouped._flow._query("list"))

    assert isinstance(physical.root, GroupAggregatePhysicalNode)
    simple_sum = getattr(physical.root, "simple_sum", None)
    assert simple_sum is not None
    assert simple_sum.key_selector == "key"
    assert simple_sum.value_selector == "value"
    assert simple_sum.output_name == "total"
    native_record = physical.root.native_record_i64_sum
    if engine == "auto":
        assert native_record is not None
        assert native_record.key_field == "key"
        assert native_record.value_field == "value"
        assert native_record.output_name == "total"
    else:
        assert native_record is None
    assert grouped.to_list() == [{"key": "a", "total": 5}]


@pytest.mark.parametrize("shape", ["simple_sum", "closed_group"])
def test_top_level_python_group_list_materializes_directly_and_closes_source(
    monkeypatch: pytest.MonkeyPatch,
    shape: str,
) -> None:
    """A built-in list sink owns canonical Python grouping without executor forwarding."""
    from fpstreams.streams import flow_terminals

    events: list[str] = []
    first_key = int("1000")
    equal_key = int("1000")
    assert first_key == equal_key and first_key is not equal_key

    def source() -> Iterator[dict[str, int]]:
        try:
            for row in (
                {"key": first_key, "value": 2},
                {"key": equal_key, "value": 3},
                {"key": 2, "value": 4},
            ):
                events.append("pull")
                yield row
        finally:
            events.append("close")

    grouped_rows = (
        fpstreams.rows(source()).with_engine("python").group_by(key=lambda row: row["key"])
    )
    if shape == "simple_sum":
        grouped = grouped_rows.aggregate(
            total=fpstreams.agg.sum(lambda row: row["value"]),
        )
        expected = [
            {"key": first_key, "total": 5},
            {"key": 2, "total": 4},
        ]
    else:
        grouped = grouped_rows.aggregate(
            count=fpstreams.agg.count(),
            total=fpstreams.agg.sum(lambda row: row["value"]),
            first=fpstreams.agg.first("value"),
        )
        expected = [
            {"key": first_key, "count": 2, "total": 5, "first": 2},
            {"key": 2, "count": 1, "total": 4, "first": 4},
        ]

    physical = compile_query(grouped._flow._query("list"))
    assert isinstance(physical.root, GroupAggregatePhysicalNode)
    if shape == "simple_sum":
        assert physical.root.simple_sum is not None
    else:
        assert physical.root.simple_sum is None
        assert physical.root.closed_group is not None

    def reject_forwarding(*_args: object, **_kwargs: object) -> Iterator[object]:
        raise AssertionError("top-level Python grouping used executor forwarding")

    monkeypatch.setattr(flow_terminals, "execute_physical", reject_forwarding)

    execution = grouped.run_with_report("to_list")

    assert execution.value == expected
    assert list(execution.value[0]) == list(expected[0])
    assert execution.value[0]["key"] is first_key
    assert execution.report.strategy == "planned:python"
    assert events == ["pull", "pull", "pull", "close"]
    with pytest.raises(fpstreams.FlowConsumedError):
        grouped.to_list()
    assert events == ["pull", "pull", "pull", "close"]


class _SingleCollectorNominalRow(Mapping[str, int]):
    def __init__(self, key: int, value: int) -> None:
        self._values = {"key": key, "value": value}

    def __getitem__(self, name: str) -> int:
        return self._values[name]

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)


def _single_collector_test_value(row: object) -> int:
    if type(row) is tuple:
        return row[1]  # type: ignore[index,return-value]
    return row["value"]  # type: ignore[index,return-value]


def _single_collector_group_case(
    aggregation_kind: str,
    row_kind: str,
    cardinality: str,
) -> tuple[list[object], object, fpstreams.Aggregator, list[dict[str, object]]]:
    keys = [0, 1, 0, 1] if cardinality == "low" else [0, 1, 2, 3]
    values = [5, 2, 3, 7]
    if row_kind == "tuple":
        records: list[object] = list(zip(keys, values, strict=True))
        key_selector: object = 0
        value_selector: object = 1
    elif row_kind == "mapping":
        records = [
            _SingleCollectorNominalRow(key, value) for key, value in zip(keys, values, strict=True)
        ]
        key_selector = "key"
        value_selector = "value"
    else:
        records = [{"key": key, "value": value} for key, value in zip(keys, values, strict=True)]
        key_selector = "key"
        value_selector = "value"

    if aggregation_kind == "sum":
        aggregation = fpstreams.agg.sum(value_selector)
    elif aggregation_kind == "count":
        aggregation = fpstreams.agg.count()
    elif aggregation_kind == "min":
        aggregation = fpstreams.agg.min(value_selector)
    else:
        aggregation = fpstreams.Aggregator(
            tuple,
            lambda state, row: (*state, _single_collector_test_value(row)),
            done=lambda state: len(state) == 1,
        )

    expected_by_kind: dict[str, dict[str, list[dict[str, object]]]] = {
        "sum": {
            "low": [{"group": 0, "result": 8}, {"group": 1, "result": 9}],
            "high": [
                {"group": 0, "result": 5},
                {"group": 1, "result": 2},
                {"group": 2, "result": 3},
                {"group": 3, "result": 7},
            ],
        },
        "count": {
            "low": [{"group": 0, "result": 2}, {"group": 1, "result": 2}],
            "high": [
                {"group": 0, "result": 1},
                {"group": 1, "result": 1},
                {"group": 2, "result": 1},
                {"group": 3, "result": 1},
            ],
        },
        "min": {
            "low": [{"group": 0, "result": 3}, {"group": 1, "result": 2}],
            "high": [
                {"group": 0, "result": 5},
                {"group": 1, "result": 2},
                {"group": 2, "result": 3},
                {"group": 3, "result": 7},
            ],
        },
        "custom": {
            "low": [
                {"group": 0, "result": (5,)},
                {"group": 1, "result": (2,)},
            ],
            "high": [
                {"group": 0, "result": (5,)},
                {"group": 1, "result": (2,)},
                {"group": 2, "result": (3,)},
                {"group": 3, "result": (7,)},
            ],
        },
    }
    return records, key_selector, aggregation, expected_by_kind[aggregation_kind][cardinality]


@pytest.mark.parametrize("execution_path", ["direct", "executor"])
@pytest.mark.parametrize(
    ("source_kind", "cardinality"),
    [
        ("list", "low"),
        ("list", "high"),
        ("generator", "low"),
        ("generator", "high"),
    ],
)
@pytest.mark.parametrize(
    ("aggregation_kind", "row_kind"),
    [
        ("sum", "dict"),
        ("count", "tuple"),
        ("min", "mapping"),
        ("custom", "dict"),
    ],
)
def test_single_collector_group_avoids_collector_program_state_wrappers(
    monkeypatch: pytest.MonkeyPatch,
    execution_path: str,
    source_kind: str,
    cardinality: str,
    aggregation_kind: str,
    row_kind: str,
) -> None:
    """One generic collector lane must not allocate the multi-lane program state."""
    from fpstreams.collecting.program import CollectorProgram
    from fpstreams.execution import relational

    records, key_selector, aggregation, expected = _single_collector_group_case(
        aggregation_kind,
        row_kind,
        cardinality,
    )
    source = records if source_kind == "list" else iter(records)
    grouped = (
        fpstreams.rows(source)
        .with_engine("python")
        .group_by(group=key_selector)
        .aggregate(result=aggregation)
    )

    def reject_program_wrapper(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("single collector grouping used CollectorProgram state wrappers")

    monkeypatch.setattr(CollectorProgram, "initialize", reject_program_wrapper)
    monkeypatch.setattr(CollectorProgram, "step", reject_program_wrapper)
    monkeypatch.setattr(CollectorProgram, "finish", reject_program_wrapper)
    if execution_path == "executor":

        def skip_direct_group_list(plan: object) -> tuple[None, object]:
            return None, plan

        monkeypatch.setattr(relational, "try_direct_group_list", skip_direct_group_list)

    assert grouped.to_list() == expected


def test_single_collector_direct_field_pic_guards_provenance_and_polymorphism(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The field shortcut rejects changed selectors and preserves polymorphic inputs."""
    from fpstreams.expressions import selectors

    class DivergentRow(Mapping[str, int]):
        def __init__(self, attribute_key: int, mapping_key: int) -> None:
            self.key = attribute_key
            self._mapping_key = mapping_key

        def __getitem__(self, name: str) -> int:
            if name != "key":
                raise KeyError(name)
            return self._mapping_key

        def __iter__(self) -> Iterator[str]:
            return iter(("key",))

        def __len__(self) -> int:
            return 1

    count = fpstreams.Aggregator(lambda: 0, lambda total, _row: total + 1)
    changed_selector = (
        fpstreams.rows([DivergentRow(1, 100)])
        .with_engine("python")
        .group_by("key")
        .aggregate(rows=count)
    )
    monkeypatch.setattr(selectors, "isinstance", lambda _value, _kind: False, raising=False)
    assert changed_selector.to_list() == [{"key": 1, "rows": 1}]
    monkeypatch.delattr(selectors, "isinstance")

    class First(DivergentRow):
        pass

    class Second(DivergentRow):
        pass

    class Third(DivergentRow):
        pass

    rows = [
        row_type(index % 2, index % 2) for index, row_type in enumerate((First, Second, Third) * 3)
    ]
    assert fpstreams.rows(rows).with_engine("python").group_by("key").aggregate(
        rows=count
    ).to_list() == [{"key": 0, "rows": 5}, {"key": 1, "rows": 4}]


class _SingleCollectorLifecycleHarness:
    def __init__(self, mutation_boundary: str, source_kind: str) -> None:
        self.mutation_boundary = mutation_boundary
        self.source_kind = source_kind
        self.events: list[str] = []
        self.mutated = False
        self.aggregation = fpstreams.Aggregator(
            self.initializer,
            self.step,
            finish=self.finish,
            done=self.done,
        )
        self.key: object = (
            _SingleCollectorMutatingKey(self)
            if mutation_boundary in {"hash", "insert_hash"}
            else "a"
        )
        self.records = [
            {"key": self.key, "value": 2},
            {
                "key": ("b" if mutation_boundary in {"finish", "step_initializer"} else self.key),
                "value": 3,
            },
        ]

    def replacement_step(self, total: int, row: dict[str, object]) -> int:
        value = row["value"]
        assert isinstance(value, int)
        self.events.append(f"replacement:{value}")
        return total + value * 10

    def replacement_done(self, _state: int) -> bool:
        self.events.append("replacement:done")
        return True

    def replacement_finish(self, total: int) -> int:
        self.events.append("replacement:finish")
        return total * 100

    def replacement_initializer(self) -> int:
        self.events.append("replacement:initializer")
        return 100

    def replace_step(self, event: str) -> None:
        self.mutated = True
        self.events.append(event)
        object.__setattr__(self.aggregation, "step", self.replacement_step)

    def initializer(self) -> int:
        self.events.append("initializer")
        if self.mutation_boundary == "initializer" and not self.mutated:
            self.replace_step("mutate:initializer")
        return 0

    def step(self, total: int, row: dict[str, object]) -> int:
        value = row["value"]
        assert isinstance(value, int)
        self.events.append(f"step:{value}")
        if self.mutation_boundary == "step" and not self.mutated:
            self.replace_step("mutate:step")
        elif self.mutation_boundary == "step_initializer" and not self.mutated:
            self.mutated = True
            self.events.append("mutate:initializer")
            object.__setattr__(
                self.aggregation,
                "initializer",
                self.replacement_initializer,
            )
        elif self.mutation_boundary == "step_done" and not self.mutated:
            self.mutated = True
            self.events.append("mutate:done")
            object.__setattr__(self.aggregation, "done", self.replacement_done)
        return total + value

    def done(self, _state: int) -> bool:
        self.events.append("done")
        if self.mutation_boundary == "done" and not self.mutated:
            self.replace_step("mutate:done")
        return False

    def finish(self, total: int) -> int:
        self.events.append("finish")
        if self.mutation_boundary == "finish" and not self.mutated:
            self.mutated = True
            self.events.append("mutate:finish")
            object.__setattr__(self.aggregation, "finish", self.replacement_finish)
        return total * 10 if self.mutation_boundary == "finish" else total

    def select_key(self, row: dict[str, object]) -> object:
        self.events.append("key")
        if self.mutation_boundary == "key" and not self.mutated:
            self.replace_step("mutate:key")
        elif self.mutation_boundary == "key_initializer" and not self.mutated:
            self.mutated = True
            self.events.append("mutate:initializer")
            object.__setattr__(
                self.aggregation,
                "initializer",
                self.replacement_initializer,
            )
        return row["key"]

    def source(self) -> Iterator[dict[str, object]]:
        try:
            if self.mutation_boundary == "first_pull":
                self.replace_step("mutate:first_pull")
            yield from self.records
        finally:
            self.events.append("close")

    def grouped(self) -> fpstreams.Rows[dict[str, object]]:
        values: object = self.records if self.source_kind == "list" else self.source()
        return (
            fpstreams.rows(values)
            .with_engine("python")
            .group_by(key=self.select_key)
            .aggregate(result=self.aggregation)
        )


class _SingleCollectorMutatingKey:
    def __init__(self, harness: _SingleCollectorLifecycleHarness) -> None:
        self.harness = harness
        self.calls = 0

    def __hash__(self) -> int:
        self.calls += 1
        boundary = self.harness.mutation_boundary
        target = 3 if boundary == "insert_hash" else 1
        if not self.harness.mutated and self.calls == target:
            self.harness.replace_step(f"mutate:hash:{self.calls}")
        return 7


@pytest.mark.parametrize("execution_path", ["direct", "executor"])
@pytest.mark.parametrize(
    ("mutation_boundary", "source_kind", "expected"),
    [
        ("first_pull", "generator", [{"key": "a", "result": 50}]),
        ("key", "list", [{"key": "a", "result": 50}]),
        ("key_initializer", "list", [{"key": "a", "result": 105}]),
        ("hash", "list", None),
        ("insert_hash", "list", None),
        ("initializer", "generator", [{"key": "a", "result": 50}]),
        ("done", "list", [{"key": "a", "result": 50}]),
        ("step", "generator", [{"key": "a", "result": 32}]),
        (
            "step_initializer",
            "generator",
            [{"key": "a", "result": 2}, {"key": "b", "result": 103}],
        ),
        ("step_done", "list", [{"key": "a", "result": 2}]),
        (
            "finish",
            "generator",
            [{"key": "a", "result": 20}, {"key": "b", "result": 300}],
        ),
    ],
)
def test_single_collector_group_refreshes_live_lifecycle_at_callback_boundaries(
    monkeypatch: pytest.MonkeyPatch,
    execution_path: str,
    mutation_boundary: str,
    source_kind: str,
    expected: list[dict[str, object]] | None,
) -> None:
    """Cached hooks retain CollectorProgram's current-row and next-row replacement rules."""
    from fpstreams.execution import relational

    harness = _SingleCollectorLifecycleHarness(mutation_boundary, source_kind)
    grouped = harness.grouped()
    if execution_path == "executor":

        def skip_direct_group_list(plan: object) -> tuple[None, object]:
            return None, plan

        monkeypatch.setattr(relational, "try_direct_group_list", skip_direct_group_list)

    result = grouped.to_list()

    if mutation_boundary in {"hash", "insert_hash"}:
        expected_result = [{"key": harness.key, "result": 50}]
    else:
        assert expected is not None
        expected_result = expected
    assert result == expected_result
    if source_kind == "generator":
        assert harness.events.count("close") == 1


@pytest.mark.parametrize("execution_path", ["direct", "executor"])
def test_single_collector_group_refreshes_after_instrumented_state_boundary(
    monkeypatch: pytest.MonkeyPatch,
    execution_path: str,
) -> None:
    """A returning transition hook may replace the step used for the new group's row."""
    from fpstreams.execution import relational
    from fpstreams.runtime import failpoints
    from fpstreams.runtime.failpoints import failpoint

    harness = _SingleCollectorLifecycleHarness("failpoint", "list")
    grouped = harness.grouped()

    def replace_step(_name: str) -> None:
        harness.replace_step("mutate:failpoint")

    monkeypatch.setattr(failpoints, "hit", replace_step)
    if execution_path == "executor":

        def skip_direct_group_list(plan: object) -> tuple[None, object]:
            return None, plan

        monkeypatch.setattr(relational, "try_direct_group_list", skip_direct_group_list)

    with failpoint("unrelated", RuntimeError("unused")):
        assert grouped.to_list() == [{"key": "a", "result": 50}]


@pytest.mark.parametrize("execution_path", ["direct", "executor"])
@pytest.mark.parametrize(
    ("replacement_kind", "expected", "expected_steps"),
    [("ordinary", 1, [1]), ("never_done", 2, [1, 2])],
)
def test_single_collector_group_preserves_live_done_completion_cache(
    monkeypatch: pytest.MonkeyPatch,
    execution_path: str,
    replacement_kind: str,
    expected: int,
    expected_steps: list[int],
) -> None:
    """A live done replacement keeps the cached bit unless it becomes never-done."""
    from fpstreams.collecting._collector_base import _never_done
    from fpstreams.execution import relational

    steps: list[int] = []
    replaced = False
    aggregation: fpstreams.Aggregator

    def step(state: int, _row: object) -> int:
        state += 1
        steps.append(state)
        return state

    def replacement_done(_state: int) -> bool:
        raise AssertionError("a completed state must not be rechecked")

    def done(state: int) -> bool:
        nonlocal replaced
        if state == 1 and not replaced:
            replaced = True
            object.__setattr__(
                aggregation,
                "done",
                _never_done if replacement_kind == "never_done" else replacement_done,
            )
        return state == 1

    aggregation = fpstreams.Aggregator(lambda: 0, step, done=done)
    grouped = (
        fpstreams.rows([{"key": "a"}, {"key": "a"}])
        .with_engine("python")
        .group_by("key")
        .aggregate(result=aggregation)
    )
    if execution_path == "executor":

        def skip_direct_group_list(plan: object) -> tuple[None, object]:
            return None, plan

        monkeypatch.setattr(relational, "try_direct_group_list", skip_direct_group_list)

    assert grouped.to_list() == [{"key": "a", "result": expected}]
    assert steps == expected_steps


@pytest.mark.parametrize("execution_path", ["direct", "executor"])
@pytest.mark.parametrize(
    ("mutation_boundary", "expected_total", "expected_events"),
    [
        (
            "first_pull",
            50,
            ["mutate:step", "pull:2", "step:2", "pull:3", "step:3", "close"],
        ),
        (
            "value_add",
            32,
            ["pull:value", "mutate:step", "pull:3", "step:3", "close"],
        ),
        (
            "source_exhaustion",
            50,
            ["pull:2", "pull:3", "mutate:finish", "close", "finish"],
        ),
    ],
)
def test_simple_group_sum_observes_lifecycle_mutations_at_source_boundaries(
    monkeypatch: pytest.MonkeyPatch,
    execution_path: str,
    mutation_boundary: str,
    expected_total: int,
    expected_events: list[str],
) -> None:
    """Fixed-state grouping retains dynamic Aggregator lifecycle semantics."""
    from fpstreams.execution import relational

    events: list[str] = []
    aggregation = fpstreams.agg.sum("value")

    def replacement_step(total: int, row: dict[str, object]) -> int:
        value = row["value"]
        assert isinstance(value, int)
        events.append(f"step:{value}")
        return total + value * 10

    def replacement_finish(total: int) -> int:
        events.append("finish")
        return total * 10

    class MutatingValue:
        def __radd__(self, total: int) -> int:
            events.append("mutate:step")
            object.__setattr__(aggregation, "step", replacement_step)
            return total + 2

    def source() -> Iterator[dict[str, object]]:
        try:
            if mutation_boundary == "first_pull":
                events.append("mutate:step")
                object.__setattr__(aggregation, "step", replacement_step)
            if mutation_boundary == "value_add":
                events.append("pull:value")
                yield {"key": "a", "value": MutatingValue()}
            else:
                events.append("pull:2")
                yield {"key": "a", "value": 2}
            events.append("pull:3")
            yield {"key": "a", "value": 3}
            if mutation_boundary == "source_exhaustion":
                events.append("mutate:finish")
                object.__setattr__(aggregation, "finish", replacement_finish)
        finally:
            events.append("close")

    grouped = (
        fpstreams.rows(source()).with_engine("python").group_by("key").aggregate(total=aggregation)
    )
    physical = compile_query(grouped._flow._query("list"))
    assert isinstance(physical.root, GroupAggregatePhysicalNode)
    assert physical.root.simple_sum is not None

    if execution_path == "executor":

        def skip_direct_group_list(plan: object) -> tuple[None, object]:
            return None, plan

        monkeypatch.setattr(relational, "try_direct_group_list", skip_direct_group_list)

    assert grouped.to_list() == [{"key": "a", "total": expected_total}]
    assert events == expected_events
    assert events.count("close") == 1


def _assert_multi_lane_group_observes_sum_lifecycle_mutation(
    monkeypatch: pytest.MonkeyPatch,
    *,
    shape: str,
    execution_path: str,
    mutation_boundary: str,
) -> None:
    from fpstreams.execution import relational

    events: list[str] = []
    aggregation = fpstreams.agg.sum("value")

    def replacement_step(total: int, row: dict[str, object]) -> int:
        value = row["value"]
        assert isinstance(value, int)
        events.append(f"step:{value}")
        return total + value * 10

    def replacement_finish(total: int) -> int:
        events.append("finish")
        return total * 10

    def source() -> Iterator[dict[str, object]]:
        try:
            if mutation_boundary == "first_pull":
                events.append("mutate:step")
                object.__setattr__(aggregation, "step", replacement_step)
            events.append("pull:2")
            yield {"key": "a", "left": "a", "right": 1, "value": 2}
            events.append("pull:3")
            yield {"key": "a", "left": "a", "right": 1, "value": 3}
            if mutation_boundary == "source_exhaustion":
                events.append("mutate:finish")
                object.__setattr__(aggregation, "finish", replacement_finish)
        finally:
            events.append("close")

    rows = fpstreams.rows(source()).with_engine("python")
    grouped = (
        rows.group_by("key").aggregate(rows=fpstreams.agg.count(), total=aggregation)
        if shape == "closed_group"
        else rows.group_by("left", "right").aggregate(
            rows=fpstreams.agg.count(),
            total=aggregation,
        )
    )
    physical = compile_query(grouped._flow._query("list"))
    assert isinstance(physical.root, GroupAggregatePhysicalNode)
    if shape == "closed_group":
        assert physical.root.closed_group is not None
        assert physical.root.composite_count_sum is None
        expected = [{"key": "a", "rows": 2, "total": 50}]
    else:
        assert physical.root.closed_group is None
        assert physical.root.composite_count_sum is not None
        expected = [{"left": "a", "right": 1, "rows": 2, "total": 50}]

    if execution_path == "executor":

        def skip_direct_group_list(plan: object) -> tuple[None, object]:
            return None, plan

        monkeypatch.setattr(relational, "try_direct_group_list", skip_direct_group_list)

    result = grouped.to_list()

    assert result == expected
    assert events == (
        ["mutate:step", "pull:2", "step:2", "pull:3", "step:3", "close"]
        if mutation_boundary == "first_pull"
        else ["pull:2", "pull:3", "mutate:finish", "close", "finish"]
    )
    assert events.count("close") == 1


@pytest.mark.parametrize("execution_path", ["direct", "executor"])
@pytest.mark.parametrize("mutation_boundary", ["first_pull", "source_exhaustion"])
def test_closed_group_observes_sum_lifecycle_mutations_at_source_boundaries(
    monkeypatch: pytest.MonkeyPatch,
    execution_path: str,
    mutation_boundary: str,
) -> None:
    """A fixed one-key multi-lane group retains the live sum lifecycle."""
    _assert_multi_lane_group_observes_sum_lifecycle_mutation(
        monkeypatch,
        shape="closed_group",
        execution_path=execution_path,
        mutation_boundary=mutation_boundary,
    )


@pytest.mark.parametrize("execution_path", ["direct", "executor"])
@pytest.mark.parametrize("mutation_boundary", ["first_pull", "source_exhaustion"])
def test_composite_count_sum_observes_sum_lifecycle_mutations_at_source_boundaries(
    monkeypatch: pytest.MonkeyPatch,
    execution_path: str,
    mutation_boundary: str,
) -> None:
    """A fixed two-key count-plus-sum group retains the live sum lifecycle."""
    _assert_multi_lane_group_observes_sum_lifecycle_mutation(
        monkeypatch,
        shape="composite_count_sum",
        execution_path=execution_path,
        mutation_boundary=mutation_boundary,
    )


@pytest.mark.parametrize("execution_path", ["direct", "executor"])
def test_closed_count_observes_step_replacement_on_first_source_pull(
    monkeypatch: pytest.MonkeyPatch,
    execution_path: str,
) -> None:
    """Every fixed collector lane remains live after its source has been opened."""
    from fpstreams.execution import relational

    events: list[str] = []
    aggregation = fpstreams.agg.count()

    def replacement_step(count: int, _row: object) -> int:
        events.append("step")
        return count + 10

    def source() -> Iterator[dict[str, int]]:
        try:
            events.append("mutate")
            object.__setattr__(aggregation, "step", replacement_step)
            yield {"key": 1}
            yield {"key": 1}
        finally:
            events.append("close")

    grouped = (
        fpstreams.rows(source()).with_engine("python").group_by("key").aggregate(rows=aggregation)
    )
    physical = compile_query(grouped._flow._query("list"))
    assert isinstance(physical.root, GroupAggregatePhysicalNode)
    assert physical.root.closed_group is not None

    if execution_path == "executor":

        def skip_direct_group_list(plan: object) -> tuple[None, object]:
            return None, plan

        monkeypatch.setattr(relational, "try_direct_group_list", skip_direct_group_list)

    assert grouped.to_list() == [{"key": 1, "rows": 20}]
    assert events == ["mutate", "step", "step", "close"]


@pytest.mark.parametrize("shape", ["closed_group", "composite_count_sum"])
@pytest.mark.parametrize("execution_path", ["direct", "executor"])
@pytest.mark.parametrize("mutation_boundary", ["value_add", "last_value_add"])
def test_fixed_multi_lane_group_retained_list_observes_sum_lifecycle_mutation(
    monkeypatch: pytest.MonkeyPatch,
    shape: str,
    execution_path: str,
    mutation_boundary: str,
) -> None:
    """A retained exact list does not freeze the sum lifecycle after planning."""
    from fpstreams.execution import relational

    events: list[str] = []
    aggregation = fpstreams.agg.sum("value")

    def replacement_step(total: int, row: dict[str, object]) -> int:
        value = row["value"]
        assert isinstance(value, int)
        events.append(f"step:{value}")
        return total + value * 10

    class MutatingValue:
        def __radd__(self, total: int) -> int:
            events.append("mutate:step")
            object.__setattr__(aggregation, "step", replacement_step)
            return total + 2

    class FinishingValue:
        def __radd__(self, total: int) -> int:
            events.append("mutate:finish")
            object.__setattr__(aggregation, "finish", replacement_finish)
            return total + 3

    def replacement_finish(total: int) -> int:
        events.append("finish")
        return total * 10

    records = [
        {
            "key": "a",
            "left": "a",
            "right": 1,
            "value": MutatingValue() if mutation_boundary == "value_add" else 2,
        },
        {
            "key": "a",
            "left": "a",
            "right": 1,
            "value": 3 if mutation_boundary == "value_add" else FinishingValue(),
        },
    ]
    rows = fpstreams.rows(records).with_engine("python")
    grouped = (
        rows.group_by("key").aggregate(rows=fpstreams.agg.count(), total=aggregation)
        if shape == "closed_group"
        else rows.group_by("left", "right").aggregate(
            rows=fpstreams.agg.count(),
            total=aggregation,
        )
    )
    physical = compile_query(grouped._flow._query("list"))
    assert isinstance(physical.root, GroupAggregatePhysicalNode)
    if shape == "closed_group":
        assert physical.root.closed_group is not None
        expected = [
            {
                "key": "a",
                "rows": 2,
                "total": 32 if mutation_boundary == "value_add" else 50,
            }
        ]
    else:
        assert physical.root.composite_count_sum is not None
        expected = [
            {
                "left": "a",
                "right": 1,
                "rows": 2,
                "total": 32 if mutation_boundary == "value_add" else 50,
            }
        ]

    if execution_path == "executor":

        def skip_direct_group_list(plan: object) -> tuple[None, object]:
            return None, plan

        monkeypatch.setattr(relational, "try_direct_group_list", skip_direct_group_list)

    assert grouped.to_list() == expected
    assert events == (
        ["mutate:step", "step:3"]
        if mutation_boundary == "value_add"
        else ["mutate:finish", "finish"]
    )


@pytest.mark.parametrize("shape", ["closed_group", "composite_count_sum"])
@pytest.mark.parametrize("execution_path", ["direct", "executor"])
def test_fixed_multi_lane_group_revalidates_sum_after_custom_key_hash(
    monkeypatch: pytest.MonkeyPatch,
    shape: str,
    execution_path: str,
) -> None:
    """A retained row's key protocol may replace the step for that same row."""
    from fpstreams.execution import relational

    events: list[str] = []
    aggregation = fpstreams.agg.sum("value")

    def replacement_step(total: int, row: dict[str, object]) -> int:
        value = row["value"]
        assert isinstance(value, int)
        events.append(f"step:{value}")
        return total + value * 10

    class MutatingKey:
        mutated = False

        def __hash__(self) -> int:
            if not self.mutated:
                self.mutated = True
                events.append("mutate:step")
                object.__setattr__(aggregation, "step", replacement_step)
            return 7

    key = MutatingKey()
    rows = fpstreams.rows([{"key": key, "left": key, "right": 1, "value": 2}]).with_engine("python")
    grouped = (
        rows.group_by("key").aggregate(rows=fpstreams.agg.count(), total=aggregation)
        if shape == "closed_group"
        else rows.group_by("left", "right").aggregate(
            rows=fpstreams.agg.count(),
            total=aggregation,
        )
    )

    if execution_path == "executor":

        def skip_direct_group_list(plan: object) -> tuple[None, object]:
            return None, plan

        monkeypatch.setattr(relational, "try_direct_group_list", skip_direct_group_list)

    result = grouped.to_list()

    assert result[0]["key" if shape == "closed_group" else "left"] is key
    assert result[0]["total"] == 20
    assert events == ["mutate:step", "step:2"]


@pytest.mark.parametrize("execution_path", ["direct", "executor"])
def test_closed_group_selector_step_replacement_starts_on_the_next_row(
    monkeypatch: pytest.MonkeyPatch,
    execution_path: str,
) -> None:
    """Replacing step from inside its selector neither reselects nor restarts that step."""
    from fpstreams.execution import relational

    events: list[str] = []
    mutated = False
    aggregation = fpstreams.agg.sum("value")

    def replacement_step(total: int, row: dict[str, object]) -> int:
        events.append("replacement:step")
        value = row["value"]
        assert isinstance(value, int)
        return total + value * 10

    class MutatingRow(dict[str, object]):
        def __getitem__(self, selector: str) -> object:
            nonlocal mutated
            value = super().__getitem__(selector)
            if selector == "value":
                events.append(f"select:{value}")
                if not mutated:
                    mutated = True
                    events.append("mutate:step")
                    object.__setattr__(aggregation, "step", replacement_step)
            return value

    rows = fpstreams.rows(
        [
            MutatingRow(key="a", value=2),
            MutatingRow(key="a", value=3),
        ]
    ).with_engine("python")
    grouped = rows.group_by("key").aggregate(
        rows=fpstreams.agg.count(),
        total=aggregation,
    )

    if execution_path == "executor":

        def skip_direct_group_list(plan: object) -> tuple[None, object]:
            return None, plan

        monkeypatch.setattr(relational, "try_direct_group_list", skip_direct_group_list)

    assert grouped.to_list() == [{"key": "a", "rows": 2, "total": 32}]
    assert events == [
        "select:2",
        "mutate:step",
        "replacement:step",
        "select:3",
    ]


@pytest.mark.parametrize("shape", ["simple_sum", "closed_group", "composite_count_sum"])
@pytest.mark.parametrize("execution_path", ["direct", "executor"])
def test_group_sum_reads_the_live_finisher_for_each_group(
    monkeypatch: pytest.MonkeyPatch,
    shape: str,
    execution_path: str,
) -> None:
    """One group's finisher replacement is visible while materializing the next group."""
    from fpstreams.execution import relational

    events: list[str] = []
    aggregation = fpstreams.agg.sum("value")

    def second_finish(total: int) -> int:
        events.append("finish:second")
        return total * 100

    def first_finish(total: int) -> int:
        events.append("finish:first")
        object.__setattr__(aggregation, "finish", second_finish)
        return total * 10

    class FinishingValue:
        def __radd__(self, total: int) -> int:
            events.append("install:first")
            object.__setattr__(aggregation, "finish", first_finish)
            return total + 3

    records = [
        {"key": "a", "left": "a", "right": 1, "value": 2},
        {"key": "b", "left": "b", "right": 1, "value": FinishingValue()},
    ]
    rows = fpstreams.rows(records).with_engine("python")
    if shape == "simple_sum":
        grouped = rows.group_by("key").aggregate(total=aggregation)
        expected = [{"key": "a", "total": 20}, {"key": "b", "total": 300}]
    elif shape == "closed_group":
        grouped = rows.group_by("key").aggregate(
            rows=fpstreams.agg.count(),
            total=aggregation,
        )
        expected = [
            {"key": "a", "rows": 1, "total": 20},
            {"key": "b", "rows": 1, "total": 300},
        ]
    else:
        grouped = rows.group_by("left", "right").aggregate(
            rows=fpstreams.agg.count(),
            total=aggregation,
        )
        expected = [
            {"left": "a", "right": 1, "rows": 1, "total": 20},
            {"left": "b", "right": 1, "rows": 1, "total": 300},
        ]

    if execution_path == "executor":

        def skip_direct_group_list(plan: object) -> tuple[None, object]:
            return None, plan

        monkeypatch.setattr(relational, "try_direct_group_list", skip_direct_group_list)

    assert grouped.to_list() == expected
    assert events == ["install:first", "finish:first", "finish:second"]


@pytest.mark.parametrize("keys", [("key",), ("left", "right")])
def test_factory_group_sum_reads_finish_replaced_between_yields(
    keys: tuple[str, ...],
) -> None:
    """A consumer can replace the finisher before requesting the next group."""
    aggregation = fpstreams.agg.sum("value")
    grouped = (
        fpstreams.rows(
            [
                {"key": "a", "left": "a", "right": 1, "value": 1},
                {"key": "b", "left": "b", "right": 2, "value": 2},
            ]
        )
        .with_engine("python")
        .group_by(*keys)
        .aggregate(total=aggregation)
    )
    consumed = 0

    def replace_finish(row: dict[str, object]) -> dict[str, object]:
        nonlocal consumed
        consumed += 1
        if consumed == 1:
            object.__setattr__(aggregation, "finish", lambda total: total * 10)
        return row

    result = grouped.to_flow().map(replace_finish).to_list()

    if len(keys) == 1:
        assert result == [{"key": "a", "total": 1}, {"key": "b", "total": 20}]
    else:
        assert result == [
            {"left": "a", "right": 1, "total": 1},
            {"left": "b", "right": 2, "total": 20},
        ]


@pytest.mark.parametrize("shape", ["single_collector", "closed_group", "composite_count_sum"])
def test_python_group_keeps_step_error_primary_when_close_fails(
    shape: str,
) -> None:
    """A live sum failure remains primary when its generator cleanup also fails."""
    primary = ValueError("step failed")
    close_calls = 0
    aggregation = fpstreams.agg.sum("value")

    def fail_step(_total: int, _row: dict[str, object]) -> int:
        raise primary

    def source() -> Iterator[dict[str, object]]:
        nonlocal close_calls
        try:
            object.__setattr__(aggregation, "step", fail_step)
            yield {"key": "a", "left": "a", "right": 1, "value": 2}
        finally:
            close_calls += 1
            raise OSError("source close failed")

    rows = fpstreams.rows(source()).with_engine("python")
    if shape == "single_collector":
        grouped = rows.group_by("key").aggregate(total=aggregation)
    elif shape == "closed_group":
        grouped = rows.group_by("key").aggregate(rows=fpstreams.agg.count(), total=aggregation)
    else:
        grouped = rows.group_by("left", "right").aggregate(
            rows=fpstreams.agg.count(),
            total=aggregation,
        )

    with pytest.raises(ValueError) as captured:
        grouped.to_list()

    assert captured.value is primary
    assert captured.value.__notes__ == ["cleanup failed with OSError: source close failed"]
    assert close_calls == 1


def test_direct_row_expression_relations_preserve_public_semantics() -> None:
    """Inspectable selector lowering must not leak into names or source ownership."""
    joined = fpstreams.rows(iter([{"id": 1, "left": "L"}])).join(
        ({"id": 1, "right": "R"},),
        on=col("id"),
        validate="m:1",
    )

    assert joined.to_list() == [
        {"id": 1, "left": "L", "id_right": 1, "right": "R"},
    ]
    with pytest.raises(fpstreams.FlowConsumedError):
        joined.to_list()

    grouped = (
        fpstreams.rows([{"key": "a", "value": 2}, {"key": "a", "value": 3}])
        .group_by(col("key"))
        .aggregate(total=fpstreams.agg.sum(col("value")))
    )

    assert grouped.to_list() == [{"key_0": "a", "total": 5}]


@pytest.mark.parametrize("relation", ["join", "group"])
def test_direct_row_expression_relations_preserve_selection_errors(relation: str) -> None:
    """Lowered direct leaves keep the public selector error and its lookup cause."""
    if relation == "join":
        result = fpstreams.rows([{"id": 1}]).join(
            ({"other": 1},),
            on=col("id"),
            validate="m:1",
        )
    else:
        result = (
            fpstreams.rows([{"id": 1}])
            .group_by(col("missing"))
            .aggregate(total=fpstreams.agg.count())
        )

    with pytest.raises(fpstreams.SelectionError) as captured:
        result.to_list()

    assert isinstance(captured.value.__cause__, (AttributeError, KeyError, TypeError))


def test_opaque_row_expression_group_callbacks_keep_order_and_identity() -> None:
    """A PythonUDF RowExpr stays opaque and executes key before value once per row."""
    from fpstreams.expressions.row import RowExpr

    records = ({"key": 2, "value": 3}, {"key": 1, "value": 5}, {"key": 2, "value": 7})
    calls: list[tuple[str, int]] = []
    key = RowExpr(lambda row: calls.append(("key", row["key"])) or row["key"], "key")
    value = RowExpr(
        lambda row: calls.append(("value", row["value"])) or row["value"],
        "value",
    )
    grouped = (
        fpstreams.rows(records)
        .with_engine("python")
        .group_by(key=key)
        .aggregate(total=fpstreams.agg.sum(value))
    )
    physical = compile_query(grouped._flow._query("list"))

    assert isinstance(physical.root, GroupAggregatePhysicalNode)
    assert physical.root.simple_sum is not None
    assert physical.root.simple_sum.key_selector is key
    assert physical.root.simple_sum.value_selector is value
    assert calls == []
    assert grouped.to_list() == [{"key": 2, "total": 10}, {"key": 1, "total": 5}]
    assert calls == [
        ("key", 2),
        ("value", 3),
        ("key", 1),
        ("value", 5),
        ("key", 2),
        ("value", 7),
    ]


def test_group_planner_lowers_only_direct_row_expression_leaves() -> None:
    """Field, index, and path leaves become the equivalent existing selector contract."""
    records = [{"key": "a", "value": 2}, {"key": "a", "value": 3}]
    grouped = (
        fpstreams.rows(records)
        .group_by(col("key"))
        .aggregate(total=fpstreams.agg.sum(col("value")))
    )
    physical = compile_query(grouped._flow._query("list"))

    assert isinstance(physical.root, GroupAggregatePhysicalNode)
    assert physical.root.simple_sum is not None
    assert physical.root.simple_sum.key_selector == "key"
    assert physical.root.simple_sum.value_selector == "value"
    assert physical.root.native_record_i64_sum is not None

    indexed = (
        fpstreams.rows([(1, 2), (1, 3)]).group_by(col(0)).aggregate(total=fpstreams.agg.sum(col(1)))
    )
    indexed_physical = compile_query(indexed._flow._query("list"))
    assert isinstance(indexed_physical.root, GroupAggregatePhysicalNode)
    assert indexed_physical.root.simple_sum is not None
    assert indexed_physical.root.simple_sum.key_selector == 0
    assert indexed_physical.root.simple_sum.value_selector == 1
    assert indexed_physical.root.native_i64_sum is not None

    nested = (
        fpstreams.rows([{"payload": {"key": "a"}, "value": 2}])
        .group_by(col("payload.key"))
        .aggregate(total=fpstreams.agg.sum("value"))
    )
    nested_physical = compile_query(nested._flow._query("list"))
    assert isinstance(nested_physical.root, GroupAggregatePhysicalNode)
    assert nested_physical.root.simple_sum is not None
    assert nested_physical.root.simple_sum.key_selector == "payload.key"
    assert nested_physical.root.native_record_i64_sum is None


def test_callable_key_selected_sum_uses_the_fixed_sum_loop_once_per_row() -> None:
    """An opaque group key keeps callback order without paying generic collector dispatch."""
    records = ({"key": 2, "value": 3}, {"key": 1, "value": 5}, {"key": 2, "value": 7})
    calls: list[dict[str, int]] = []

    def select_key(row: dict[str, int]) -> int:
        calls.append(row)
        return row["key"]

    grouped = (
        fpstreams.rows(records).group_by(key=select_key).aggregate(total=fpstreams.agg.sum("value"))
    )
    physical = compile_query(grouped._flow._query("list"))

    assert isinstance(physical.root, GroupAggregatePhysicalNode)
    assert physical.root.simple_sum is not None
    assert physical.root.native_i64_sum is None
    assert physical.root.native_record_i64_sum is None
    assert calls == []
    assert grouped.to_list() == [{"key": 2, "total": 10}, {"key": 1, "total": 5}]
    assert calls == list(records)


@pytest.mark.parametrize("callable_key", [False, True])
def test_callable_value_selected_sum_uses_the_fixed_sum_loop_once_per_row(
    callable_key: bool,
) -> None:
    """An opaque sum value keeps callback order without generic lane dispatch."""
    records = ({"key": 2, "value": 3}, {"key": 1, "value": 5}, {"key": 2, "value": 7})
    calls: list[tuple[str, dict[str, int]]] = []

    def select_key(row: dict[str, int]) -> int:
        calls.append(("key", row))
        return row["key"]

    def select_value(row: dict[str, int]) -> int:
        calls.append(("value", row))
        return row["value"]

    key_selector: object = select_key if callable_key else "key"
    grouped = (
        fpstreams.rows(records)
        .group_by(key=key_selector)
        .aggregate(total=fpstreams.agg.sum(select_value))
    )
    physical = compile_query(grouped._flow._query("list"))

    assert isinstance(physical.root, GroupAggregatePhysicalNode)
    assert physical.root.simple_sum is not None
    assert physical.root.native_i64_sum is None
    assert physical.root.native_record_i64_sum is None
    assert calls == []
    assert grouped.to_list() == [{"key": 2, "total": 10}, {"key": 1, "total": 5}]
    expected: list[tuple[str, dict[str, int]]] = []
    for record in records:
        if callable_key:
            expected.append(("key", record))
        expected.append(("value", record))
    assert calls == expected


def test_simple_group_sum_treats_callable_string_subclasses_as_callables() -> None:
    """Callable selector subclasses follow compile_selector instead of direct field lookup."""
    records = ({"key": 2, "value": 3}, {"key": 1, "value": 5}, {"key": 2, "value": 7})
    calls: list[tuple[str, dict[str, int]]] = []

    class CallableKey(str):
        def __call__(self, row: dict[str, int]) -> int:
            calls.append(("key", row))
            return row["key"]

    class CallableValue(str):
        def __call__(self, row: dict[str, int]) -> int:
            calls.append(("value", row))
            return row["value"]

    grouped = (
        fpstreams.rows(records)
        .group_by(key=CallableKey("missing_key"))
        .aggregate(total=fpstreams.agg.sum(CallableValue("missing_value")))
    )
    physical = compile_query(grouped._flow._query("list"))

    assert isinstance(physical.root, GroupAggregatePhysicalNode)
    assert physical.root.simple_sum is not None
    assert grouped.to_list() == [{"key": 2, "total": 10}, {"key": 1, "total": 5}]
    assert calls == [call for record in records for call in (("key", record), ("value", record))]


def test_callable_value_simple_group_failpoint_precedes_callback_and_closes() -> None:
    """A new group remains the first state transition before opaque value selection."""
    from fpstreams.runtime.failpoints import failpoint

    events: list[str] = []

    def source():
        try:
            events.append("pull")
            yield {"key": 1, "value": 2}
        finally:
            events.append("close")

    def select_key(row: dict[str, int]) -> int:
        events.append("key")
        return row["key"]

    def select_value(row: dict[str, int]) -> int:
        events.append("value")
        return row["value"]

    grouped = (
        fpstreams.rows(source())
        .group_by(key=select_key)
        .aggregate(total=fpstreams.agg.sum(select_value))
    )

    with (
        failpoint("group.state.create.after", RuntimeError("stop after state")),
        pytest.raises(RuntimeError, match="stop after state"),
    ):
        grouped.to_list()
    assert events == ["pull", "key", "close"]


def test_simple_group_sum_recognizes_mappingproxy_without_an_abc_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The exact immutable proxy type has a direct Mapping capability."""
    from types import MappingProxyType

    from fpstreams.execution import relational

    class RejectingMeta(type):
        def __instancecheck__(cls, instance: object) -> bool:
            raise AssertionError(f"unexpected Mapping check for {type(instance).__name__}")

    class RejectingMapping(metaclass=RejectingMeta):
        pass

    monkeypatch.setattr(relational, "Mapping", RejectingMapping)
    result = (
        fpstreams.rows(
            [
                MappingProxyType({"key": 1, "value": 2}),
                MappingProxyType({"key": 1, "value": 3}),
                MappingProxyType({"key": 2, "value": 4}),
            ]
        )
        .with_engine("python")
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
        .to_list()
    )

    assert result == [{"key": 1, "total": 5}, {"key": 2, "total": 4}]


@pytest.mark.parametrize("engine", ["python", "auto"])
@pytest.mark.parametrize("source_kind", ["list", "generator"])
@pytest.mark.parametrize("callback_side", ["key", "value"])
def test_callable_simple_group_sum_preserves_dynamic_hash_lookup_after_warmup(
    monkeypatch: pytest.MonkeyPatch,
    engine: str,
    source_kind: str,
    callback_side: str,
) -> None:
    """Sum-only grouping keeps its public preliminary hash on every source shape."""
    import builtins

    from fpstreams.execution import relational

    records: list[object]
    if callback_side == "key":
        records = [{"bucket": index % 7, "measure": index} for index in range(33)]
    else:
        records = [(index % 7, index) for index in range(33)]
    key_calls: list[object] = []
    value_calls: list[object] = []

    def select_key(row: object) -> int:
        key_calls.append(row)
        return row["bucket"]  # type: ignore[index]

    def select_value(row: object) -> int:
        value_calls.append(row)
        return row[1]  # type: ignore[index]

    source = records if source_kind == "list" else iter(records)
    key_selector: object = select_key if callback_side == "key" else 0
    value_selector: object = "measure" if callback_side == "key" else select_value
    grouped = (
        fpstreams.rows(source)
        .with_engine(engine)
        .group_by(key=key_selector)
        .aggregate(total=fpstreams.agg.sum(value_selector))
    )
    explicit_hashes: list[object] = []
    original_hash = builtins.hash

    def traced_hash(value: object) -> int:
        explicit_hashes.append(value)
        return original_hash(value)

    monkeypatch.setattr(relational, "hash", traced_hash, raising=False)

    result = grouped.to_list()

    assert sum(row["total"] for row in result) == sum(range(33))
    expected_hashes = [
        record["bucket"] if callback_side == "key" else record[0]  # type: ignore[index]
        for record in records
    ]
    assert explicit_hashes == expected_hashes
    assert len(key_calls) == (33 if callback_side == "key" else 0)
    assert len(value_calls) == (33 if callback_side == "value" else 0)


@pytest.mark.parametrize("engine", ["python", "auto"])
@pytest.mark.parametrize("source_kind", ["list", "generator"])
@pytest.mark.parametrize("row_kind", ["tuple", "list"])
def test_exact_sequence_simple_group_sum_keeps_dynamic_hash_for_exact_int_keys(
    monkeypatch: pytest.MonkeyPatch,
    engine: str,
    source_kind: str,
    row_kind: str,
) -> None:
    """Exact index selectors keep the preliminary hash for tuple and list rows."""
    import builtins

    from fpstreams.execution import relational

    make_row = tuple if row_kind == "tuple" else list
    records = [make_row((index % 5, index)) for index in range(33)]
    source = records if source_kind == "list" else iter(records)
    grouped = (
        fpstreams.rows(source)
        .with_engine(engine)
        .group_by(key=0)
        .aggregate(total=fpstreams.agg.sum(1))
    )
    explicit_hashes: list[object] = []
    original_hash = builtins.hash

    def traced_hash(value: object) -> int:
        explicit_hashes.append(value)
        return original_hash(value)

    monkeypatch.setattr(relational, "hash", traced_hash, raising=False)

    assert grouped.to_list() == [{"key": key, "total": sum(range(key, 33, 5))} for key in range(5)]
    expected_hashes = (
        []
        if engine == "auto" and source_kind == "list" and row_kind == "tuple"
        else [record[0] for record in records]
    )
    assert explicit_hashes == expected_hashes


@pytest.mark.parametrize("callback_side", ["key", "value"])
def test_callable_simple_group_sum_keeps_hash_before_value_and_closes_one_shot(
    callback_side: str,
) -> None:
    """A key hash failure precedes value selection and still closes the claimed generator."""
    events: list[str] = []

    class BrokenHash:
        def __hash__(self) -> int:
            events.append("hash")
            raise RuntimeError("broken hash")

    def source() -> Iterator[dict[str, object]]:
        try:
            events.append("pull")
            yield {"bucket": BrokenHash(), "measure": 4}
        finally:
            events.append("close")

    def select_key(row: dict[str, object]) -> object:
        events.append("key")
        return row["bucket"]

    def select_value(row: dict[str, object]) -> int:
        events.append("value")
        return int(row["measure"])

    grouped = (
        fpstreams.rows(source())
        .with_engine("auto")
        .group_by(key=select_key if callback_side == "key" else "bucket")
        .aggregate(total=fpstreams.agg.sum("measure" if callback_side == "key" else select_value))
    )

    with pytest.raises(RuntimeError, match="broken hash"):
        grouped.to_list()

    expected = ["pull"]
    if callback_side == "key":
        expected.append("key")
    expected.extend(("hash", "close"))
    assert events == expected
    with pytest.raises(fpstreams.FlowConsumedError):
        grouped.to_list()


def test_two_callable_simple_group_sum_keeps_dynamic_hash_before_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The reused loop must still resolve and call the public hash before value selection."""
    import builtins

    from fpstreams.execution import relational

    key_calls = 0
    value_calls = 0

    def broken_hash(_value: object) -> int:
        raise RuntimeError("dynamic hash")

    monkeypatch.setattr(relational, "hash", builtins.hash, raising=False)

    def select_key(row: dict[str, int]) -> int:
        nonlocal key_calls
        key_calls += 1
        if row["value"] == 32:
            relational.hash = broken_hash  # type: ignore[attr-defined]
        return row["bucket"]

    def select_value(row: dict[str, int]) -> int:
        nonlocal value_calls
        value_calls += 1
        return row["value"]

    grouped = (
        fpstreams.rows([{"bucket": index % 5, "value": index} for index in range(34)])
        .group_by(key=select_key)
        .aggregate(total=fpstreams.agg.sum(select_value))
    )

    with pytest.raises(RuntimeError, match="dynamic hash"):
        grouped.to_list()
    assert key_calls == 33
    assert value_calls == 32


@pytest.mark.parametrize("callback_side", ["key", "value"])
def test_callable_simple_group_sum_matches_canonical_iterator_protocol(
    callback_side: str,
) -> None:
    """Shared execution keeps the canonical two ``iter`` calls and one exhaustion probe."""
    from fpstreams.execution import relational

    events: list[str] = []

    class ObservableIterator:
        def __init__(self) -> None:
            self.remaining = [{"bucket": 1, "measure": 2}]

        def __iter__(self) -> ObservableIterator:
            events.append("iter")
            return self

        def __next__(self) -> dict[str, int]:
            events.append("next")
            if not self.remaining:
                raise StopIteration
            return self.remaining.pop()

        def close(self) -> None:
            events.append("close")

    def select_key(row: dict[str, int]) -> int:
        return row["bucket"]

    def select_value(row: dict[str, int]) -> int:
        return row["measure"]

    query = (
        fpstreams.rows([{"bucket": 1, "measure": 2}])
        .with_engine("python")
        .group_by(key=select_key if callback_side == "key" else "bucket")
        .aggregate(total=fpstreams.agg.sum("measure" if callback_side == "key" else select_value))
    )
    physical = compile_query(query._flow._query("list"))
    assert isinstance(physical.root, GroupAggregatePhysicalNode)
    spec = physical.root.simple_sum
    assert spec is not None

    assert list(relational._execute_python_group_values(ObservableIterator(), physical.root)) == [
        {"key": 1, "total": 2}
    ]
    assert events == ["iter", "iter", "next", "next", "close"]


def test_callable_value_simple_group_sum_second_iter_precedes_callbacks() -> None:
    """A failing canonical second ``iter`` happens before peeking or selecting the first row."""
    from fpstreams.execution import relational

    events: list[str] = []
    value_calls = 0

    class FailingSecondIter:
        def __init__(self) -> None:
            self.iterations = 0

        def __iter__(self) -> FailingSecondIter:
            self.iterations += 1
            events.append("iter")
            if self.iterations == 2:
                raise RuntimeError("second iter")
            return self

        def __next__(self) -> dict[str, int]:
            events.append("next")
            return {"bucket": 1, "measure": 2}

        def close(self) -> None:
            events.append("close")

    def select_value(row: dict[str, int]) -> int:
        nonlocal value_calls
        value_calls += 1
        return row["measure"]

    query = (
        fpstreams.rows([{"bucket": 1, "measure": 2}])
        .with_engine("python")
        .group_by(key="bucket")
        .aggregate(total=fpstreams.agg.sum(select_value))
    )
    physical = compile_query(query._flow._query("list"))
    assert isinstance(physical.root, GroupAggregatePhysicalNode)
    spec = physical.root.simple_sum
    assert spec is not None

    with pytest.raises(RuntimeError, match="second iter"):
        list(relational._execute_python_group_values(FailingSecondIter(), physical.root))

    assert events == ["iter", "iter", "close"]
    assert value_calls == 0


@pytest.mark.parametrize("callback_side", ["key", "value"])
def test_callable_simple_group_sum_keeps_preliminary_hash_for_custom_keys(
    callback_side: str,
) -> None:
    """The exact-key tail never hides the public preliminary hash of a user key type."""
    hash_calls = 0

    class Key:
        def __hash__(self) -> int:
            nonlocal hash_calls
            hash_calls += 1
            return 7

    key = Key()
    records = [{"bucket": key, "measure": 1} for _index in range(33)]
    grouped = (
        fpstreams.rows(records)
        .with_engine("python")
        .group_by(key=(lambda row: row["bucket"]) if callback_side == "key" else "bucket")
        .aggregate(
            total=fpstreams.agg.sum(
                "measure" if callback_side == "key" else lambda row: row["measure"]
            )
        )
    )

    assert grouped.to_list() == [{"key": key, "total": 33}]
    assert hash_calls == 67


@pytest.mark.parametrize("callback_side", ["key", "value"])
def test_callable_simple_group_sum_observes_hash_replaced_after_warmup(
    monkeypatch: pytest.MonkeyPatch,
    callback_side: str,
) -> None:
    """A callback can replace the next explicit hash even after the PIC warmup."""
    import builtins

    from fpstreams.execution import relational

    callback_calls = 0
    events: list[str] = []

    def broken_hash(_value: object) -> int:
        events.append("hash")
        raise RuntimeError("dynamic hash")

    monkeypatch.setattr(relational, "hash", builtins.hash, raising=False)

    def source() -> Iterator[object]:
        try:
            for index in range(34):
                events.append(f"pull:{index}")
                if callback_side == "key":
                    yield {"bucket": index % 5, "measure": index}
                else:
                    yield (index % 5, index)
        finally:
            events.append("close")

    def select_key(row: dict[str, int]) -> int:
        nonlocal callback_calls
        callback_calls += 1
        if row["measure"] == 32:
            relational.hash = broken_hash  # type: ignore[attr-defined]
        return row["bucket"]

    def select_value(row: object) -> int:
        nonlocal callback_calls
        callback_calls += 1
        measure = row[1]  # type: ignore[index]
        if measure == 32:
            relational.hash = broken_hash  # type: ignore[attr-defined]
        return measure

    grouped = (
        fpstreams.rows(source())
        .with_engine("python")
        .group_by(key=select_key if callback_side == "key" else 0)
        .aggregate(total=fpstreams.agg.sum("measure" if callback_side == "key" else select_value))
    )

    with pytest.raises(RuntimeError, match="dynamic hash"):
        grouped.to_list()

    assert callback_calls == 33
    assert events[-2:] == ["hash", "close"]


@pytest.mark.parametrize("engine", ["python", "auto"])
@pytest.mark.parametrize("row_kind", ["tuple", "list"])
def test_exact_sequence_simple_group_sum_observes_hash_replaced_by_previous_add(
    monkeypatch: pytest.MonkeyPatch,
    engine: str,
    row_kind: str,
) -> None:
    """A selected value may replace hash, which must affect the following row."""
    import builtins

    from fpstreams.execution import relational

    events: list[str] = []

    def broken_hash(_value: object) -> int:
        events.append("hash")
        raise RuntimeError("dynamic hash")

    monkeypatch.setattr(relational, "hash", builtins.hash, raising=False)

    class ReplaceHash:
        def __radd__(self, current: int) -> int:
            events.append("add")
            relational.hash = broken_hash  # type: ignore[attr-defined]
            return current + 1

    make_row = tuple if row_kind == "tuple" else list
    records = [make_row((1, 1)) for _index in range(32)]
    records.append(make_row((1, ReplaceHash())))
    records.append(make_row((1, 1)))

    def source() -> Iterator[tuple[object, ...] | list[object]]:
        try:
            for index, row in enumerate(records):
                events.append(f"pull:{index}")
                yield row
        finally:
            events.append("close")

    grouped = (
        fpstreams.rows(source())
        .with_engine(engine)
        .group_by(key=0)
        .aggregate(total=fpstreams.agg.sum(1))
    )

    with pytest.raises(RuntimeError, match="dynamic hash"):
        grouped.to_list()

    assert events[-4:] == ["add", "pull:33", "hash", "close"]


@pytest.mark.parametrize("callback_side", ["key", "value"])
@pytest.mark.parametrize("source_kind", ["list", "generator"])
def test_callable_simple_group_sum_observes_selector_mutation_at_canonical_boundary(
    callback_side: str,
    source_kind: str,
) -> None:
    """Key mutation precedes value lookup, while value mutation follows the captured key."""
    records = [
        {"bucket": 2, "measure": 3},
        {"bucket": 1, "measure": 5},
        {"bucket": 2, "measure": 7},
    ]
    calls: list[dict[str, int]] = []

    def select_key(row: dict[str, int]) -> int:
        calls.append(row)
        row["measure"] += 10
        return row["bucket"]

    def select_value(row: dict[str, int]) -> int:
        calls.append(row)
        value = row["measure"]
        row["bucket"] = 99
        return value

    source = records if source_kind == "list" else iter(records)
    grouped = (
        fpstreams.rows(source)
        .with_engine("python")
        .group_by(key=select_key if callback_side == "key" else "bucket")
        .aggregate(total=fpstreams.agg.sum("measure" if callback_side == "key" else select_value))
    )

    if callback_side == "key":
        expected = [{"key": 2, "total": 30}, {"key": 1, "total": 15}]
    else:
        expected = [{"key": 2, "total": 10}, {"key": 1, "total": 5}]
    assert grouped.to_list() == expected
    assert len(calls) == 3


@pytest.mark.parametrize("callback_side", ["key", "value"])
def test_callable_simple_group_sum_invalidates_mapping_pic_after_selector_mro_change(
    callback_side: str,
) -> None:
    """A nominal Mapping proof is rechecked after either opaque selector changes its MRO."""
    from collections.abc import Mapping

    class Root:
        def __init__(self, bucket: int, measure: int) -> None:
            self.bucket = bucket
            self.measure = measure

    class MappingBase(Root, Mapping[str, int]):
        def __getitem__(self, name: str) -> int:
            return getattr(self, name)

        def __iter__(self) -> Iterator[str]:
            return iter(("bucket", "measure"))

        def __len__(self) -> int:
            return 2

    class AttributeBase(Root):
        def __getitem__(self, _name: str) -> int:
            return -100

    class Record(MappingBase):
        pass

    records = [Record(1, 2), Record(1, 3)]
    calls = 0

    if callback_side == "key":

        def select_key(row: Record) -> int:
            nonlocal calls
            calls += 1
            if calls == 2:
                Record.__bases__ = (AttributeBase,)
                Mapping._abc_caches_clear()
            return row.bucket

        query = (
            fpstreams.rows(records)
            .with_engine("python")
            .group_by(key=select_key)
            .aggregate(total=fpstreams.agg.sum("measure"))
        )
    else:

        def select_value(row: Record) -> int:
            nonlocal calls
            calls += 1
            if calls == 1:
                Record.__bases__ = (AttributeBase,)
                Mapping._abc_caches_clear()
            return row.measure

        query = (
            fpstreams.rows(records)
            .with_engine("python")
            .group_by(key="bucket")
            .aggregate(total=fpstreams.agg.sum(select_value))
        )

    assert query.to_list() == [{"key": 1, "total": 5}]


@pytest.mark.parametrize("mutation_boundary", ["hash_global", "equality_mro"])
def test_direct_field_group_sum_rechecks_mapping_after_key_lookup_callbacks(  # noqa: C901
    monkeypatch: pytest.MonkeyPatch,
    mutation_boundary: str,
) -> None:
    """Hash and equality callbacks cannot stale the value field's Mapping proof."""
    from fpstreams.expressions import selectors

    class Root:
        def __init__(self, key: object, value: int, item_value: int) -> None:
            self.key = key
            self.value = value
            self.item_value = item_value

    class MappingBase(Root, Mapping[str, object]):
        def __getitem__(self, name: str) -> object:
            return self.item_value if name == "value" else getattr(self, name)

        def __iter__(self) -> Iterator[str]:
            return iter(("key", "value"))

        def __len__(self) -> int:
            return 2

    class AttributeBase(Root):
        def __getitem__(self, _name: str) -> int:
            return -100

    class Record(MappingBase):
        pass

    class RejectingMeta(type):
        def __instancecheck__(cls, _instance: object) -> bool:
            return False

    class RejectingMapping(metaclass=RejectingMeta):
        pass

    mutated = False

    def mutate_mapping() -> None:
        nonlocal mutated
        if mutated:
            return
        mutated = True
        if mutation_boundary == "hash_global":
            monkeypatch.setattr(selectors, "Mapping", RejectingMapping)
        else:
            Record.__bases__ = (AttributeBase,)
            Mapping._abc_caches_clear()

    class Key:
        def __init__(self, trigger: bool) -> None:
            self.trigger = trigger

        def __hash__(self) -> int:
            if mutation_boundary == "hash_global" and self.trigger:
                mutate_mapping()
            return 7

        def __eq__(self, other: object) -> bool:
            if mutation_boundary == "equality_mro" and type(other) is Key and other.trigger:
                mutate_mapping()
            return type(other) is Key

    original_bases = Record.__bases__
    records = (
        [Record(Key(True), 5, -100)]
        if mutation_boundary == "hash_global"
        else [Record(Key(False), 2, 2), Record(Key(True), 3, 3)]
    )
    try:
        result = (
            fpstreams.rows(records)
            .with_engine("python")
            .group_by("key")
            .aggregate(total=fpstreams.agg.sum("value"))
            .to_list()
        )
    finally:
        if Record.__bases__ != original_bases:
            Record.__bases__ = original_bases
            Mapping._abc_caches_clear()

    assert mutated
    assert len(result) == 1
    assert result[0]["total"] == 5


def test_direct_field_group_sum_keeps_a_live_row_expression_evaluator() -> None:
    """A direct RowExpr leaf remains callable when key hashing replaces its evaluator."""
    value = fpstreams.col("value")

    class MutatingKey:
        def __hash__(self) -> int:
            value._evaluate._program = lambda row: row["value"] * 10  # type: ignore[attr-defined]
            return 11

        def __eq__(self, other: object) -> bool:
            return type(other) is MutatingKey

    result = (
        fpstreams.rows([{"key": MutatingKey(), "value": 3}])
        .with_engine("python")
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum(value))
        .to_list()
    )

    assert result[0]["total"] == 30


@pytest.mark.parametrize("callback_side", ["key", "value"])
def test_callable_simple_group_sum_closes_once_after_late_callback_failure(
    callback_side: str,
) -> None:
    """Once callbacks start, a late failure never replays a one-shot source or its prefix."""
    events: list[str] = []
    callback_calls = 0

    def source() -> Iterator[dict[str, int]]:
        try:
            for index in range(34):
                events.append(f"pull:{index}")
                yield {"bucket": index % 5, "measure": index}
        finally:
            events.append("close")

    def select_key(row: dict[str, int]) -> int:
        nonlocal callback_calls
        callback_calls += 1
        if row["measure"] == 33:
            raise RuntimeError("late key")
        return row["bucket"]

    def select_value(row: dict[str, int]) -> int:
        nonlocal callback_calls
        callback_calls += 1
        if row["measure"] == 33:
            raise RuntimeError("late value")
        return row["measure"]

    grouped = (
        fpstreams.rows(source())
        .with_engine("auto")
        .group_by(key=select_key if callback_side == "key" else "bucket")
        .aggregate(total=fpstreams.agg.sum("measure" if callback_side == "key" else select_value))
    )

    with pytest.raises(RuntimeError, match=f"late {callback_side}"):
        grouped.to_list()

    assert callback_calls == 34
    assert events[-1] == "close"
    assert events.count("close") == 1


def test_project_owned_group_aggregations_compile_to_fixed_state_lanes() -> None:
    """Closed grouped collectors are recognized without evaluating their selectors."""
    calls: list[str] = []

    def select_key(row: dict[str, int]) -> int:
        calls.append("key")
        return row["key"]

    def select_value(row: dict[str, int]) -> int:
        calls.append("value")
        return row["value"]

    grouped = (
        fpstreams.rows([{"key": 1, "value": 3}])
        .group_by(key=select_key)
        .aggregate(
            count=fpstreams.agg.count(),
            total=fpstreams.agg.sum(select_value),
            minimum=fpstreams.agg.min("value"),
            maximum=fpstreams.agg.max("value"),
            first=fpstreams.agg.first("value"),
            last=fpstreams.agg.last("value"),
        )
    )
    physical = compile_query(grouped._flow._query("list"))

    assert isinstance(physical.root, GroupAggregatePhysicalNode)
    assert physical.root.simple_sum is None
    assert physical.root.closed_group is not None
    assert tuple(lane.kind for lane in physical.root.closed_group.lanes) == (
        "count",
        "sum",
        "min",
        "max",
        "first",
        "last",
    )
    assert calls == []
    assert grouped.to_list() == [
        {
            "key": 1,
            "count": 1,
            "total": 3,
            "minimum": 3,
            "maximum": 3,
            "first": 3,
            "last": 3,
        }
    ]
    assert calls == ["key", "value"]


def test_direct_composite_count_sum_preserves_identity_order_and_field_order() -> None:
    """Two direct keys bypass collector objects without changing grouped-row semantics."""
    first_key = int("1000")
    equal_key = int("1000")
    first_band = "".join(("band", "-x"))
    equal_band = "".join(("band", "-x"))
    assert first_key == equal_key and first_key is not equal_key
    assert first_band == equal_band and first_band is not equal_band
    grouped = (
        fpstreams.rows(
            (
                (first_key, first_band, 2),
                (equal_key, equal_band, 3),
                (2000, "band-y", 4),
            )
        )
        .group_by(0, 1)
        .aggregate(rows=fpstreams.agg.count(), total=fpstreams.agg.sum(2))
    )
    physical = compile_query(grouped._flow._query("list"))

    assert isinstance(physical.root, GroupAggregatePhysicalNode)
    marker = physical.root.composite_count_sum
    assert marker is not None
    assert (
        marker.key_selectors,
        marker.value_selector,
        marker.count_name,
        marker.sum_name,
    ) == ((0, 1), 2, "rows", "total")
    assert physical.root.simple_sum is None
    assert physical.root.closed_group is None

    result = grouped.to_list()

    assert result == [
        {"key_0": first_key, "key_1": first_band, "rows": 2, "total": 5},
        {"key_0": 2000, "key_1": "band-y", "rows": 1, "total": 4},
    ]
    assert list(result[0]) == ["key_0", "key_1", "rows", "total"]
    assert result[0]["key_0"] is first_key
    assert result[0]["key_1"] is first_band


@pytest.mark.parametrize("execution_path", ["direct", "executor"])
@pytest.mark.parametrize("late_kind", ["dict", "mapping"])
def test_composite_count_sum_deopts_after_exact_prefix_without_replaying_state(
    monkeypatch: pytest.MonkeyPatch,
    execution_path: str,
    late_kind: str,
) -> None:
    """A late row/lifecycle change continues the authoritative loop from compact state."""
    from fpstreams.execution import relational

    events: list[str] = []
    aggregation = fpstreams.agg.sum(2)

    class IntMapping(Mapping[int, int]):
        def __getitem__(self, key: int) -> int:
            return {0: 1, 1: 2, 2: 4}[key]

        def __iter__(self) -> Iterator[int]:
            return iter((0, 1, 2))

        def __len__(self) -> int:
            return 3

    def replacement_step(total: int, row: object) -> int:
        value = row[2]  # type: ignore[index]
        events.append(f"step:{value}")
        return total + value * 10

    def source() -> Iterator[object]:
        try:
            yield (1, 2, 3)
            events.append("mutate")
            object.__setattr__(aggregation, "step", replacement_step)
            yield {0: 1, 1: 2, 2: 4} if late_kind == "dict" else IntMapping()
        finally:
            events.append("close")

    grouped = (
        fpstreams.rows(source())
        .with_engine("python")
        .group_by(0, 1)
        .aggregate(rows=fpstreams.agg.count(), total=aggregation)
    )
    physical = compile_query(grouped._flow._query("list"))
    assert isinstance(physical.root, GroupAggregatePhysicalNode)
    assert physical.root.composite_count_sum is not None

    if execution_path == "executor":

        def skip_direct_group_list(plan: object) -> tuple[None, object]:
            return None, plan

        monkeypatch.setattr(relational, "try_direct_group_list", skip_direct_group_list)

    assert grouped.to_list() == [{"key_0": 1, "key_1": 2, "rows": 2, "total": 43}]
    assert events == ["mutate", "step:4", "close"]


def test_composite_count_sum_declines_nonfixed_shapes_and_observes_replaced_hash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Admission stays narrow, and a replaced module hash retains canonical call timing."""
    records = ((1, 2, 3),)
    candidates = [
        fpstreams.rows(records)
        .group_by(0, 1, 2)
        .aggregate(rows=fpstreams.agg.count(), total=fpstreams.agg.sum(2)),
        fpstreams.rows(records)
        .group_by(True, 1)
        .aggregate(rows=fpstreams.agg.count(), total=fpstreams.agg.sum(2)),
        fpstreams.rows(records)
        .group_by(0, 1)
        .aggregate(total=fpstreams.agg.sum(2), rows=fpstreams.agg.count()),
        fpstreams.rows(records)
        .group_by(0, 1)
        .spill(partitions=2)
        .aggregate(rows=fpstreams.agg.count(), total=fpstreams.agg.sum(2)),
        fpstreams.rows([{"left": {"nested": 1}, "right": 2, "value": 3}])
        .group_by("left.nested", "right")
        .aggregate(rows=fpstreams.agg.count(), total=fpstreams.agg.sum("value")),
    ]
    for grouped in candidates:
        physical = compile_query(grouped._flow._query("list"))
        assert isinstance(physical.root, GroupAggregatePhysicalNode)
        assert physical.root.composite_count_sum is None

    from fpstreams.execution import relational

    canonical_hash = hash
    hashed: list[object] = []

    def tracked_hash(value: object) -> int:
        hashed.append(value)
        return canonical_hash(value)

    monkeypatch.setattr(relational, "hash", tracked_hash, raising=False)
    assert fpstreams.rows(((1, 2, 3), (1, 2, 4))).group_by(0, 1).aggregate(
        rows=fpstreams.agg.count(), total=fpstreams.agg.sum(2)
    ).to_list() == [{"key_0": 1, "key_1": 2, "rows": 2, "total": 7}]
    assert hashed == [(1, 2), (1, 2)]


def test_composite_count_sum_failpoint_precedes_value_lookup_and_closes_source() -> None:
    """The optimized loop retains the group-transition boundary and iterator ownership."""
    from collections.abc import Mapping

    from fpstreams.runtime.failpoints import failpoint

    events: list[str] = []

    class LoggedRow(Mapping[str, int]):
        def __getitem__(self, name: str) -> int:
            events.append(f"get:{name}")
            return {"left": 1, "right": 2, "value": 3}[name]

        def __iter__(self) -> Iterator[str]:
            return iter(("left", "right", "value"))

        def __len__(self) -> int:
            return 3

    def source() -> Iterator[LoggedRow]:
        events.append("open")
        try:
            yield LoggedRow()
        finally:
            events.append("close")

    with (
        failpoint("group.state.create.after", RuntimeError("group transition")),
        pytest.raises(RuntimeError, match="group transition"),
    ):
        (
            fpstreams.rows(source())
            .group_by("left", "right")
            .aggregate(rows=fpstreams.agg.count(), total=fpstreams.agg.sum("value"))
            .to_list()
        )

    assert events == ["open", "get:left", "get:right", "close"]


def test_exact_fixed_count_groups_compile_to_native_physical_markers() -> None:
    """Exact tuple/dict count shapes expose one closed native ABI description."""
    candidates = [
        (
            fpstreams.rows([(1, 3), (1, 4)]).group_by(key=0).aggregate(rows=fpstreams.agg.count()),
            ("tuple", 0, None, "rows", None),
        ),
        (
            fpstreams.rows([(1, 3), (1, 4)])
            .group_by(key=0)
            .aggregate(rows=fpstreams.agg.count(), total=fpstreams.agg.sum(1)),
            ("tuple", 0, 1, "rows", "total"),
        ),
        (
            fpstreams.rows([{"key": 1, "value": 3}, {"key": 1, "value": 4}])
            .group_by("key")
            .aggregate(rows=fpstreams.agg.count()),
            ("dict", "key", None, "rows", None),
        ),
        (
            fpstreams.rows([{"key": 1, "value": 3}, {"key": 1, "value": 4}])
            .group_by("key")
            .aggregate(rows=fpstreams.agg.count(), total=fpstreams.agg.sum("value")),
            ("dict", "key", "value", "rows", "total"),
        ),
    ]

    for grouped, expected in candidates:
        physical = compile_query(grouped._flow._query("list"))
        assert isinstance(physical.root, GroupAggregatePhysicalNode)
        marker = getattr(physical.root, "native_fixed_i64_group", None)
        assert marker is not None
        assert (
            marker.row_kind,
            marker.key_selector,
            marker.value_selector,
            marker.count_name,
            marker.sum_name,
        ) == expected


@pytest.mark.parametrize("outer_kind", ["list", "tuple"])
@pytest.mark.parametrize("row_kind", ["tuple", "dict"])
def test_exact_record_multi_aggregate_uses_native_without_python_group_fallback(
    monkeypatch: pytest.MonkeyPatch,
    outer_kind: str,
    row_kind: str,
) -> None:
    """Direct exact records keep ordered count/sum/min/max work inside one native scan."""
    from fpstreams.execution import relational

    maximum = 2**63 - 1
    raw_rows: list[object]
    if row_kind == "tuple":
        raw_rows = [(2, maximum), (1, -5), (2, 1), (1, 3)]
    else:
        raw_rows = [
            {"key": 2, "value": maximum},
            {"key": 1, "value": -5},
            {"key": 2, "value": 1},
            {"key": 1, "value": 3},
        ]
    source = raw_rows if outer_kind == "list" else tuple(raw_rows)
    rows = fpstreams.rows(source)
    grouped = (rows.group_by(key=0) if row_kind == "tuple" else rows.group_by("key")).aggregate(
        total=fpstreams.agg.sum(1 if row_kind == "tuple" else "value"),
        repeated=fpstreams.agg.sum(1 if row_kind == "tuple" else "value"),
        low=fpstreams.agg.min(1 if row_kind == "tuple" else "value"),
        rows=fpstreams.agg.count(),
        high=fpstreams.agg.max(1 if row_kind == "tuple" else "value"),
    )

    def reject_python_group(*_arguments: object, **_options: object) -> object:
        raise AssertionError("an exact multi-aggregate source must not enter Python grouping")

    monkeypatch.setattr(relational, "_execute_python_group_values", reject_python_group)

    result = grouped.to_list()
    assert result == [
        {
            "key": 2,
            "total": 2**63,
            "repeated": 2**63,
            "low": 1,
            "rows": 2,
            "high": maximum,
        },
        {
            "key": 1,
            "total": -2,
            "repeated": -2,
            "low": -5,
            "rows": 2,
            "high": 3,
        },
    ]
    assert [tuple(row) for row in result] == [
        ("key", "total", "repeated", "low", "rows", "high"),
        ("key", "total", "repeated", "low", "rows", "high"),
    ]


@pytest.mark.parametrize("row_kind", ["tuple", "dict"])
@pytest.mark.parametrize("wide_field", ["key", "value"])
def test_exact_record_multi_aggregate_declines_non_i64_values(
    row_kind: str,
    wide_field: str,
) -> None:
    """Speculative native grouping leaves Python big integers to canonical aggregation."""
    large = 1 << 80
    keys = (large, large) if wide_field == "key" else (1, 1)
    values = (large, 2) if wide_field == "value" else (3, 2)
    if row_kind == "tuple":
        records: list[object] = list(zip(keys, values, strict=True))
        key_selector: str | int = 0
        value_selector: str | int = 1
    else:
        records = [{"key": key, "value": value} for key, value in zip(keys, values, strict=True)]
        key_selector = "key"
        value_selector = "value"

    result = (
        fpstreams.rows(records)
        .group_by(key=key_selector)
        .aggregate(
            low=fpstreams.agg.min(value_selector),
            total=fpstreams.agg.sum(value_selector),
            high=fpstreams.agg.max(value_selector),
            rows=fpstreams.agg.count(),
        )
        .to_list()
    )

    assert result == [
        {
            "key": keys[0],
            "low": min(values),
            "total": sum(values),
            "high": max(values),
            "rows": 2,
        }
    ]


@pytest.mark.parametrize("error_type", [MemoryError, TypeError, OverflowError])
def test_exact_record_multi_aggregate_does_not_swallow_native_errors(
    monkeypatch: pytest.MonkeyPatch,
    error_type: type[BaseException],
) -> None:
    """Errors raised after the speculative scan starts do not replay the source."""
    from fpstreams import _native

    def fail(*_arguments: object) -> object:
        raise error_type("native grouped multi failure")

    monkeypatch.setattr(_native, "group_multi_i64_rows_v1", fail, raising=False)
    grouped = (
        fpstreams.rows([(1, 2)])
        .group_by(key=0)
        .aggregate(
            low=fpstreams.agg.min(1),
            high=fpstreams.agg.max(1),
        )
    )

    with pytest.raises(error_type, match="native grouped multi failure"):
        grouped.to_list()


def test_exact_record_multi_aggregate_prevalidates_wide_tuple_indexes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Indexes wider than isize decline before entering the optional native ABI."""
    from fpstreams import _native

    def unexpected(*_arguments: object) -> object:
        raise AssertionError("an invalid native index must be rejected before dispatch")

    monkeypatch.setattr(_native, "group_multi_i64_rows_v1", unexpected, raising=False)
    wide_index = 1 << 100
    result = (
        fpstreams.rows([])
        .group_by(key=wide_index)
        .aggregate(low=fpstreams.agg.min(wide_index), high=fpstreams.agg.max(wide_index))
        .to_list()
    )

    assert result == []


def test_exact_record_multi_native_preserves_overwrite_and_lane_order() -> None:
    """The private ABI follows Python dict overwrite order for colliding output names."""
    from fpstreams import _native

    kernel = getattr(_native, "group_multi_i64_rows_v1", None)
    if not callable(kernel):
        pytest.skip("the current optional wheel predates grouped multi aggregation")

    result = kernel(
        [(1, 3), (1, 4)],
        0,
        "key",
        (
            (3, 1, "key"),
            (2, 1, "low"),
            (2, 1, "low_again"),
            (0, None, "rows"),
            (0, None, "rows_again"),
        ),
    )

    assert result == [{"key": 4, "low": 3, "low_again": 3, "rows": 2, "rows_again": 2}]
    assert tuple(result[0]) == ("key", "low", "low_again", "rows", "rows_again")


def test_exact_record_multi_native_preserves_first_winning_value_identity() -> None:
    """Equal min/max values retain the first exact Python integer object."""
    first = int("1000000000000")
    equal = int("1000000000000")
    assert first is not equal

    result = (
        fpstreams.rows([(1, first), (1, equal)])
        .group_by(key=0)
        .aggregate(low=fpstreams.agg.min(1), high=fpstreams.agg.max(1))
        .to_list()
    )

    assert result[0]["low"] is first
    assert result[0]["high"] is first


@pytest.mark.skipif(sys.platform == "win32", reason="setitimer is Unix-only")
def test_exact_record_multi_active_timer_preserves_python_list_growth(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An active timer keeps signal-time list mutation in the canonical Python loop."""
    import signal

    from fpstreams import _native
    from fpstreams.execution.relational import _execute_authoritative_group

    records = [(0, 1)] * 500_000
    previous = signal.getsignal(signal.SIGALRM)
    appended = False

    def unexpected_native(*_arguments: object) -> object:
        raise AssertionError("an active timer must bypass native grouped multi aggregation")

    monkeypatch.setattr(_native, "group_multi_i64_rows_v1", unexpected_native, raising=False)

    def append_once(_signum: int, frame: object) -> None:
        nonlocal appended
        if not appended and getattr(frame, "f_code", None) is _execute_authoritative_group.__code__:
            records.append((0, 999))
            appended = True

    signal.signal(signal.SIGALRM, append_once)
    signal.setitimer(signal.ITIMER_REAL, 0.001, 0.001)
    try:
        result = (
            fpstreams.rows(records)
            .group_by(key=0)
            .aggregate(low=fpstreams.agg.min(1), high=fpstreams.agg.max(1))
            .to_list()
        )
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)

    assert appended
    assert result == [{"key": 0, "low": 1, "high": 999}]


@pytest.mark.skipif(sys.platform == "win32", reason="getitimer is Unix-only")
def test_exact_record_multi_declines_replaced_signal_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Signal introspection replacement conservatively keeps canonical execution."""
    import signal

    from fpstreams import _native

    def unexpected_native(*_arguments: object) -> object:
        raise AssertionError("a replaced signal API must bypass grouped multi native")

    monkeypatch.setattr(_native, "group_multi_i64_rows_v1", unexpected_native, raising=False)
    monkeypatch.setattr(signal, "getitimer", lambda _timer: (0.0, 0.0))

    result = (
        fpstreams.rows([(1, 3), (1, 4)])
        .group_by(key=0)
        .aggregate(low=fpstreams.agg.min(1), high=fpstreams.agg.max(1))
        .to_list()
    )

    assert result == [{"key": 1, "low": 3, "high": 4}]


@pytest.mark.parametrize("callback_side", ["key", "value"])
def test_opaque_callable_count_sum_groups_stay_on_python_physical_path(
    callback_side: str,
) -> None:
    """Opaque callbacks keep the Python frame and generator lifecycle authoritative."""
    records = [{"key": 1, "value": 2}, {"key": 1, "value": 3}]

    def select_key(row: dict[str, int]) -> int:
        return row["key"]

    def select_value(row: dict[str, int]) -> int:
        return row["value"]

    key_selector: object = select_key if callback_side == "key" else "key"
    value_selector: object = "value" if callback_side == "key" else select_value
    grouped = (
        fpstreams.rows(records)
        .group_by(alias=key_selector)
        .aggregate(rows=fpstreams.agg.count(), total=fpstreams.agg.sum(value_selector))
    )
    physical = compile_query(grouped._flow._query("list"))

    assert isinstance(physical.root, GroupAggregatePhysicalNode)
    assert physical.root.native_fixed_i64_group is None
    assert grouped.to_list() == [{"alias": 1, "rows": 2, "total": 5}]


@pytest.mark.parametrize("callback_side", ["key", "value"])
def test_opaque_callable_group_never_enters_private_native_abis(
    monkeypatch: pytest.MonkeyPatch,
    callback_side: str,
) -> None:
    """No private callable ABI may own callbacks without a Python-frame lifecycle."""
    from fpstreams import _native

    def unexpected_native(*_arguments: object) -> object:
        raise AssertionError("opaque callable grouping must remain in Python")

    prefix = f"group_count_sum_callable_{callback_side}_dict_rows"
    for version in (1, 2, 3):
        monkeypatch.setattr(
            _native,
            f"{prefix}_v{version}",
            unexpected_native,
            raising=False,
        )

    records = [{"key": 1, "value": 2}, {"key": 1, "value": 3}]

    def select_key(row: dict[str, int]) -> int:
        return row["key"]

    def select_value(row: dict[str, int]) -> int:
        return row["value"]

    grouped = (
        fpstreams.rows(records)
        .group_by(key=select_key if callback_side == "key" else "key")
        .aggregate(
            count=fpstreams.agg.count(),
            total=fpstreams.agg.sum("value" if callback_side == "key" else select_value),
        )
    )
    expected = [{"key": 1, "count": 2, "total": 5}]

    assert grouped.to_list() == expected
    assert grouped.map(lambda row: row).to_list() == expected


def test_exact_fixed_group_small_native_payloads_materialize_rows(monkeypatch) -> None:
    """Count pairs and count/sum triples become public rows in declaration order."""
    from fpstreams import _native

    calls: list[tuple[object, ...]] = []

    def tuple_result(*arguments: object) -> tuple[bool, list[tuple[int, int]]]:
        calls.append(("tuple", *arguments))
        return False, [(7, 2)]

    def dict_result(*arguments: object) -> tuple[bool, list[tuple[int, int, int]]]:
        calls.append(("dict", *arguments))
        return False, [(9, 3, 17)]

    monkeypatch.setattr(_native, "group_fixed_i64_rows_v1", tuple_result, raising=False)
    monkeypatch.setattr(_native, "group_fixed_i64_dict_rows_v1", dict_result, raising=False)
    tuple_source = [(1, 3), (1, 4)]
    dict_source = [{"group": 1, "value": 3}, {"group": 1, "value": 4}]

    assert fpstreams.rows(tuple_source).group_by(key=0).aggregate(
        rows=fpstreams.agg.count()
    ).to_list() == [{"key": 7, "rows": 2}]
    assert fpstreams.rows(dict_source).group_by(alias="group").aggregate(
        rows=fpstreams.agg.count(), total=fpstreams.agg.sum("value")
    ).to_list() == [{"alias": 9, "rows": 3, "total": 17}]
    assert calls == [
        ("tuple", tuple_source, 0, None, "key", "rows", None),
        ("dict", dict_source, "group", "value", "alias", "rows", "total"),
    ]


@pytest.mark.parametrize(
    ("terminal", "expected"),
    [
        ("to_list", [{"key": 1, "rows": 2, "total": 7}]),
        ("count", 1),
    ],
)
def test_root_native_group_reports_the_rust_route(terminal: str, expected: object) -> None:
    """Both group entry points report a Rust kernel only after it succeeds."""
    grouped = (
        fpstreams.rows([(1, 3), (1, 4)])
        .group_by(key=0)
        .aggregate(
            rows=fpstreams.agg.count(),
            total=fpstreams.agg.sum(1),
        )
    )

    execution = grouped.run_with_report(terminal)

    assert execution.value == expected
    assert execution.report.strategy == "rust_direct"
    assert "Rust" in execution.report.reason


@pytest.mark.parametrize("terminal", ["to_list", "count"])
def test_declined_native_group_keeps_the_planned_python_report(
    monkeypatch: pytest.MonkeyPatch,
    terminal: str,
) -> None:
    """A speculative native decline is not reported as completed Rust work."""
    from fpstreams.execution import relational

    monkeypatch.setattr(relational, "_try_native_group_sum", lambda _node: None)
    grouped = (
        fpstreams.rows([(1, 3), (1, 4)])
        .group_by(key=0)
        .aggregate(
            rows=fpstreams.agg.count(),
            total=fpstreams.agg.sum(1),
        )
    )

    execution = grouped.run_with_report(terminal)

    assert execution.value == ([{"key": 1, "rows": 2, "total": 7}] if terminal == "to_list" else 1)
    assert execution.report.strategy == "planned:python"


def test_fixed_group_native_decline_is_not_retried_by_list_fallback(monkeypatch) -> None:
    """A fixed ABI None sentinel performs exactly one canonical Python replay."""
    from fpstreams import _native

    calls = 0

    def decline(*_arguments: object) -> None:
        nonlocal calls
        calls += 1
        return None

    monkeypatch.setattr(_native, "group_fixed_i64_rows_v1", decline, raising=False)

    assert fpstreams.rows([(1, 3), (1, 4)]).group_by(key=0).aggregate(
        rows=fpstreams.agg.count(), total=fpstreams.agg.sum(1)
    ).to_list() == [{"key": 1, "rows": 2, "total": 7}]
    assert calls == 1


def test_fixed_group_native_index_narrowing_error_falls_back_cleanly(monkeypatch) -> None:
    """An exact Python index wider than isize retains canonical empty-input semantics."""
    from fpstreams import _native

    def reject_wide_index(*_arguments: object) -> object:
        raise OverflowError("Python int too large to convert to C ssize_t")

    monkeypatch.setattr(
        _native,
        "group_fixed_i64_rows_v1",
        reject_wide_index,
        raising=False,
    )

    assert (
        fpstreams.rows([]).group_by(key=1 << 100).aggregate(rows=fpstreams.agg.count()).to_list()
        == []
    )


def test_fixed_group_native_planning_rejects_nonexact_or_indirect_shapes() -> None:
    """Only direct auto source leaves with matching exact selectors receive the marker."""
    records = [{"key": 1, "value": 3}]
    candidates = [
        fpstreams.rows(records)
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"), average=fpstreams.agg.mean("value")),
        fpstreams.rows(records)
        .group_by("key")
        .aggregate(rows=fpstreams.agg.count(), total=fpstreams.agg.sum("payload.value")),
        fpstreams.rows(records)
        .group_by(key=lambda row: row["key"])
        .aggregate(rows=fpstreams.agg.count()),
        fpstreams.rows(records)
        .group_by(key=0)
        .aggregate(rows=fpstreams.agg.count(), total=fpstreams.agg.sum("value")),
        fpstreams.rows(records)
        .with_engine("python")
        .group_by("key")
        .aggregate(rows=fpstreams.agg.count()),
        fpstreams.rows(records)
        .filter(lambda row: bool(row))
        .group_by("key")
        .aggregate(rows=fpstreams.agg.count()),
        fpstreams.rows(iter(records)).group_by("key").aggregate(rows=fpstreams.agg.count()),
    ]

    for grouped in candidates:
        physical = compile_query(grouped._flow._query("list"))
        assert isinstance(physical.root, GroupAggregatePhysicalNode)
        assert getattr(physical.root, "native_fixed_i64_group", None) is None


def test_fixed_group_final_native_rows_are_returned_directly(monkeypatch) -> None:
    """A final fixed-ABI list bypasses Python payload and executor materialization."""
    from fpstreams import _native

    expected = [{"native": True}]

    def native_result(*_arguments: object) -> tuple[bool, list[dict[str, bool]]]:
        return True, expected

    def unexpected_executor(*_arguments: object, **_options: object) -> object:
        raise AssertionError("final native rows must bypass the Python executor")

    monkeypatch.setattr(_native, "group_fixed_i64_rows_v1", native_result, raising=False)
    monkeypatch.setattr(
        "fpstreams.streams.flow_terminals.execute_physical",
        unexpected_executor,
    )

    result = (
        fpstreams.rows([(1, 3), (1, 4)])
        .group_by(key=0)
        .aggregate(rows=fpstreams.agg.count())
        .to_list()
    )
    assert result is expected


def test_fixed_group_missing_native_symbol_falls_back_to_python(monkeypatch) -> None:
    """An older optional wheel without fixed grouping symbols remains compatible."""
    from fpstreams import _native

    monkeypatch.delattr(_native, "group_fixed_i64_rows_v1", raising=False)
    monkeypatch.delattr(_native, "group_fixed_i64_dict_rows_v1", raising=False)
    monkeypatch.delattr(_native, "group_multi_i64_rows_v1", raising=False)
    monkeypatch.delattr(_native, "group_multi_i64_dict_rows_v1", raising=False)

    assert fpstreams.rows([(1, 3), (1, 4)]).group_by(key=0).aggregate(
        rows=fpstreams.agg.count(), total=fpstreams.agg.sum(1)
    ).to_list() == [{"key": 1, "rows": 2, "total": 7}]
    assert fpstreams.rows([{"key": 1}, {"key": 1}]).group_by("key").aggregate(
        rows=fpstreams.agg.count()
    ).to_list() == [{"key": 1, "rows": 2}]
    assert fpstreams.rows([(1, 3), (1, 4)]).group_by(key=0).aggregate(
        low=fpstreams.agg.min(1), high=fpstreams.agg.max(1)
    ).to_list() == [{"key": 1, "low": 3, "high": 4}]


def test_fixed_group_active_failpoint_bypasses_native(monkeypatch) -> None:
    """Instrumented state transitions remain owned by the closed Python loop."""
    from fpstreams import _native
    from fpstreams.runtime.failpoints import failpoint

    def unexpected_native(*_arguments: object) -> object:
        raise AssertionError("an active group failpoint must bypass native execution")

    monkeypatch.setattr(
        _native,
        "group_fixed_i64_rows_v1",
        unexpected_native,
        raising=False,
    )
    grouped = (
        fpstreams.rows([(1, 3)])
        .group_by(key=0)
        .aggregate(rows=fpstreams.agg.count(), total=fpstreams.agg.sum(1))
    )

    with (
        failpoint("group.state.create.after", RuntimeError("stop after state")),
        pytest.raises(RuntimeError, match="stop after state"),
    ):
        grouped.to_list()


@pytest.mark.parametrize(
    ("kernel_name", "source", "arguments", "expected"),
    [
        (
            "group_fixed_i64_rows_v1",
            [(1, 3), (1, 4), (2, 9)],
            (0, None, "key", "rows", None),
            (False, [(1, 2), (2, 1)]),
        ),
        (
            "group_fixed_i64_dict_rows_v1",
            [
                {"key": 1, "value": 3},
                {"key": 1, "value": 4},
                {"key": 2, "value": 9},
            ],
            ("key", "value", "key", "rows", "total"),
            (False, [(1, 2, 7), (2, 1, 9)]),
        ),
    ],
)
def test_real_fixed_group_native_small_payloads_when_available(
    kernel_name: str,
    source: object,
    arguments: tuple[object, ...],
    expected: object,
) -> None:
    """The built extension matches the Python dispatch payload contract."""
    from fpstreams import _native

    kernel = getattr(_native, kernel_name, None)
    if not callable(kernel):
        pytest.skip("the current optional wheel predates fixed grouping")

    assert kernel(source, *arguments) == expected


def test_closed_group_rejects_a_copied_factory_marker_on_a_different_step() -> None:
    """Copying private metadata cannot make an arbitrary Aggregator look project-owned."""
    branded = fpstreams.agg.sum("value")
    calls: list[int] = []

    def replacement(state: int, row: dict[str, int]) -> int:
        calls.append(row["value"])
        return state + 100

    replacement.__dict__.update(branded.step.__dict__)
    copied = fpstreams.Aggregator(
        branded.initializer,
        replacement,
        branded.finish,
        branded.combine,
        branded.done,
        branded.native,
    )
    grouped = fpstreams.rows([{"key": 1, "value": 2}]).group_by("key").aggregate(result=copied)
    physical = compile_query(grouped._flow._query("list"))

    assert isinstance(physical.root, GroupAggregatePhysicalNode)
    assert physical.root.simple_sum is None
    assert physical.root.closed_group is None
    assert grouped.to_list() == [{"key": 1, "result": 100}]
    assert calls == [2]


def test_closed_group_preserves_mapping_selector_order_and_first_short_circuit() -> None:
    """Fixed lanes keep generic per-collector Mapping reads in declaration order."""
    from collections.abc import Mapping

    events: list[str] = []

    class LoggedMapping(Mapping[str, int]):
        def __init__(self, key: int, value: int) -> None:
            self.values = {"key": key, "value": value}

        def __getitem__(self, name: str) -> int:
            events.append(name)
            return self.values[name]

        def __iter__(self) -> Iterator[str]:
            return iter(self.values)

        def __len__(self) -> int:
            return len(self.values)

    result = (
        fpstreams.rows([LoggedMapping(1, 3), LoggedMapping(1, 2)])
        .group_by("key")
        .aggregate(
            count=fpstreams.agg.count(),
            total=fpstreams.agg.sum("value"),
            minimum=fpstreams.agg.min("value"),
            maximum=fpstreams.agg.max("value"),
            first=fpstreams.agg.first("value"),
            last=fpstreams.agg.last("value"),
        )
        .to_list()
    )

    assert result == [
        {
            "key": 1,
            "count": 2,
            "total": 5,
            "minimum": 2,
            "maximum": 3,
            "first": 3,
            "last": 2,
        }
    ]
    assert events == [
        "key",
        "value",
        "value",
        "value",
        "value",
        "value",
        "key",
        "value",
        "value",
        "value",
        "value",
    ]


@pytest.mark.parametrize("engine", ["python", "auto"])
@pytest.mark.parametrize("direction", ["callable_key", "callable_value"])
def test_callable_group_reads_mappingproxy_fields_at_the_canonical_time(
    direction: str,
    engine: str,
) -> None:
    """A callback can mutate proxy backing without fields being prefetched or snapshotted."""
    from types import MappingProxyType

    backing = {"key": 1, "value": 2}
    record = MappingProxyType(backing)

    if direction == "callable_key":

        def select_key(row: object) -> int:
            backing["value"] = 7
            return row["key"]  # type: ignore[index]

        query = (
            fpstreams.rows([record])
            .with_engine(engine)
            .group_by(key=select_key)
            .aggregate(count=fpstreams.agg.count(), total=fpstreams.agg.sum("value"))
        )
        expected = [{"key": 1, "count": 1, "total": 7}]
    else:

        def select_value(row: object) -> int:
            backing["key"] = 9
            return row["value"]  # type: ignore[index]

        query = (
            fpstreams.rows([record])
            .with_engine(engine)
            .group_by(key="key")
            .aggregate(count=fpstreams.agg.count(), total=fpstreams.agg.sum(select_value))
        )
        expected = [{"key": 1, "count": 1, "total": 2}]

    assert query.to_list() == expected


@pytest.mark.parametrize("engine", ["python", "auto"])
@pytest.mark.parametrize("direction", ["callable_key", "callable_value"])
def test_callable_group_invalidates_nominal_mapping_pic_after_mro_change(
    direction: str,
    engine: str,
) -> None:
    """A cached nominal Mapping shape is valid only while its exact MRO object survives."""
    from collections.abc import Mapping

    class Root:
        def __init__(self, key: int, value: int) -> None:
            self.key = key
            self.value = value

    class MappingBase(Root, Mapping[str, int]):
        def __getitem__(self, name: str) -> int:
            return getattr(self, name)

        def __iter__(self) -> Iterator[str]:
            return iter(("key", "value"))

        def __len__(self) -> int:
            return 2

    class AttributeBase(Root):
        def __getitem__(self, _name: str) -> int:
            return -100

    class Record(MappingBase):
        pass

    records = [Record(1, 2), Record(1, 3)]
    calls = 0

    if direction == "callable_key":

        def select_key(row: Record) -> int:
            nonlocal calls
            calls += 1
            if calls == 2:
                Record.__bases__ = (AttributeBase,)
                Mapping._abc_caches_clear()
            return row.key

        query = (
            fpstreams.rows(records)
            .with_engine(engine)
            .group_by(key=select_key)
            .aggregate(count=fpstreams.agg.count(), total=fpstreams.agg.sum("value"))
        )
    else:

        def select_value(row: Record) -> int:
            nonlocal calls
            calls += 1
            if calls == 1:
                Record.__bases__ = (AttributeBase,)
                Mapping._abc_caches_clear()
            return row.value

        query = (
            fpstreams.rows(records)
            .with_engine(engine)
            .group_by(key="key")
            .aggregate(count=fpstreams.agg.count(), total=fpstreams.agg.sum(select_value))
        )

    assert query.to_list() == [{"key": 1, "count": 2, "total": 5}]


@pytest.mark.parametrize(
    "shape",
    (
        "callable_key_count",
        "callable_key_direct_value",
        "callable_key_value",
        "direct_key_callable_value",
    ),
)
def test_callable_group_keeps_live_overridden_hash_after_warmup(
    monkeypatch: pytest.MonkeyPatch,
    shape: str,
) -> None:
    """The exact-key tail calls a dynamically replaced hash on every remaining row."""
    import builtins

    records = [{"key": index % 11, "value": index} for index in range(33)]
    key_calls: list[object] = []
    value_calls: list[object] = []

    def select_key(row: object) -> int:
        key_calls.append(row)
        return row["key"]  # type: ignore[index]

    def select_value(row: object) -> int:
        value_calls.append(row)
        return row["value"]  # type: ignore[index]

    if shape == "callable_key_count":
        query = (
            fpstreams.rows(records)
            .with_engine("python")
            .group_by(key=select_key)
            .aggregate(count=fpstreams.agg.count())
        )
    elif shape == "callable_key_direct_value":
        query = (
            fpstreams.rows(records)
            .with_engine("python")
            .group_by(key=select_key)
            .aggregate(count=fpstreams.agg.count(), total=fpstreams.agg.sum("value"))
        )
    else:
        key_selector: object = select_key if shape == "callable_key_value" else "key"
        query = (
            fpstreams.rows(records)
            .with_engine("python")
            .group_by(key=key_selector)
            .aggregate(count=fpstreams.agg.count(), total=fpstreams.agg.sum(select_value))
        )

    physical = compile_query(query._flow._query("list"))
    assert isinstance(physical.root, GroupAggregatePhysicalNode)
    spec = physical.root.closed_group
    assert spec is not None
    explicit_hashes: list[object] = []
    original_hash = builtins.hash

    def traced_hash(value: object) -> int:
        explicit_hashes.append(value)
        return original_hash(value)

    monkeypatch.setattr(builtins, "hash", traced_hash)
    result = query.to_list()

    assert sum(row["count"] for row in result) == 33
    assert explicit_hashes == [record["key"] for record in records]
    assert len(key_calls) == (0 if shape == "direct_key_callable_value" else 33)
    assert len(value_calls) == (
        33 if shape in {"callable_key_value", "direct_key_callable_value"} else 0
    )


def test_callable_group_short_input_keeps_the_canonical_hash_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A source shorter than the warmup is evaluated once without a tail transition."""
    import builtins

    records = [{"key": index % 3, "value": index} for index in range(31)]
    query = (
        fpstreams.rows(records)
        .with_engine("python")
        .group_by(key=lambda row: row["key"])
        .aggregate(count=fpstreams.agg.count(), total=fpstreams.agg.sum("value"))
    )
    physical = compile_query(query._flow._query("list"))
    assert isinstance(physical.root, GroupAggregatePhysicalNode)
    spec = physical.root.closed_group
    assert spec is not None
    explicit_hashes: list[object] = []
    original_hash = builtins.hash

    def traced_hash(value: object) -> int:
        explicit_hashes.append(value)
        return original_hash(value)

    monkeypatch.setattr(builtins, "hash", traced_hash)

    result = query.to_list()

    assert sum(row["count"] for row in result) == 31
    assert explicit_hashes == [record["key"] for record in records]


def test_callable_group_hash_tail_translates_late_unhashable_key_and_closes() -> None:
    """An unlike key after warmup retains canonical error translation and cleanup."""
    events: list[str] = []

    def source() -> Iterator[dict[str, object]]:
        try:
            for index in range(32):
                events.append(f"pull:{index}")
                yield {"key": index, "value": 1}
            events.append("pull:bad")
            yield {"key": [], "value": 1}
        finally:
            events.append("close")

    def select_key(row: dict[str, object]) -> object:
        events.append("key")
        return row["key"]

    query = (
        fpstreams.rows(source())
        .with_engine("python")
        .group_by(key=select_key)
        .aggregate(count=fpstreams.agg.count(), total=fpstreams.agg.sum("value"))
    )

    with pytest.raises(TypeError, match="group_by keys must be hashable") as captured:
        query.to_list()

    assert captured.value.__cause__ is None
    assert events.count("key") == 33
    assert events[-1] == "close"


def test_callable_key_group_exact_tuple_value_index_preserves_subclass_protocol() -> None:
    """Tuple subclasses remain on compiled accessors with their live subscription callbacks."""
    events: list[int] = []

    class Row(tuple[int, int]):
        def __getitem__(self, index: int) -> int:
            events.append(index)
            return super().__getitem__(index)

    records = [Row((1, 2)), Row((1, 3))]
    query = (
        fpstreams.rows(records)
        .group_by(key=lambda row: row[0])
        .aggregate(count=fpstreams.agg.count(), total=fpstreams.agg.sum(1))
    )

    assert query.to_list() == [{"key": 1, "count": 2, "total": 5}]
    assert events == [0, 1, 0, 1]


def test_callable_value_group_exact_tuple_key_index_preserves_subclass_protocol() -> None:
    """Tuple subclasses keep compiled key access and their live subscription callbacks."""
    events: list[int] = []

    class Row(tuple[int, int]):
        def __getitem__(self, index: int) -> int:
            events.append(index)
            return super().__getitem__(index)

    records = [Row((1, 2)), Row((1, 3))]
    query = (
        fpstreams.rows(records)
        .group_by(key=0)
        .aggregate(count=fpstreams.agg.count(), total=fpstreams.agg.sum(lambda row: row[1]))
    )

    assert query.to_list() == [{"key": 1, "count": 2, "total": 5}]
    assert events == [0, 1, 0, 1]


def test_callable_key_group_exact_tuple_value_index_translates_errors_and_closes() -> None:
    """An out-of-range direct tuple selector retains SelectionError chaining and cleanup."""
    events: list[str] = []

    def source() -> Iterator[tuple[int]]:
        try:
            events.append("pull")
            yield (1,)
        finally:
            events.append("close")

    query = (
        fpstreams.rows(source())
        .group_by(key=lambda row: row[0])
        .aggregate(count=fpstreams.agg.count(), total=fpstreams.agg.sum(1))
    )

    with pytest.raises(fpstreams.SelectionError, match="index selector 1 on tuple") as captured:
        query.to_list()

    assert isinstance(captured.value.__cause__, IndexError)
    assert events == ["pull", "close"]


def test_callable_value_group_exact_tuple_key_index_translates_errors_and_closes() -> None:
    """A failing direct tuple key is translated before the callback and closes its source."""
    events: list[str] = []

    def source() -> Iterator[tuple[int]]:
        try:
            events.append("pull")
            yield (1,)
        finally:
            events.append("close")

    def value(_row: object) -> int:
        events.append("value")
        return 1

    query = (
        fpstreams.rows(source())
        .group_by(key=1)
        .aggregate(count=fpstreams.agg.count(), total=fpstreams.agg.sum(value))
    )

    with pytest.raises(fpstreams.SelectionError, match="index selector 1 on tuple") as captured:
        query.to_list()

    assert isinstance(captured.value.__cause__, IndexError)
    assert events == ["pull", "close"]


def test_callable_value_count_sum_keeps_dotted_keys_on_the_canonical_path() -> None:
    """A callable value cannot turn a dotted key into one literal dictionary lookup."""
    result = (
        fpstreams.rows(
            [
                {"payload": {"key": 1}, "value": 2},
                {"payload": {"key": 1}, "value": 3},
            ]
        )
        .with_engine("python")
        .group_by(key="payload.key")
        .aggregate(
            count=fpstreams.agg.count(),
            total=fpstreams.agg.sum(lambda row: row["value"]),
        )
        .to_list()
    )

    assert result == [{"key": 1, "count": 2, "total": 5}]


def test_common_closed_group_repeats_exact_dict_lookups_for_each_aggregation() -> None:
    """Hand-unrolled shapes cannot merge observably distinct dictionary probes."""
    equality_calls: list[str] = []

    class CollidingField:
        def __hash__(self) -> int:
            return hash("value")

        def __eq__(self, other: object) -> bool:
            equality_calls.append(str(other))
            return False

    collision = CollidingField()
    records = [
        {collision: "trap", "key": 1, "value": 3},
        {collision: "trap", "key": 1, "value": 2},
    ]
    equality_calls.clear()
    for row, lookups in zip(records, (5, 4), strict=True):
        for _index in range(lookups):
            row["value"]
    expected_calls = equality_calls.copy()
    equality_calls.clear()

    result = (
        fpstreams.rows(records)
        .group_by("key")
        .aggregate(
            count=fpstreams.agg.count(),
            total=fpstreams.agg.sum("value"),
            minimum=fpstreams.agg.min("value"),
            maximum=fpstreams.agg.max("value"),
            first=fpstreams.agg.first("value"),
            last=fpstreams.agg.last("value"),
        )
        .to_list()
    )

    assert result == [
        {
            "key": 1,
            "count": 2,
            "total": 5,
            "minimum": 2,
            "maximum": 3,
            "first": 3,
            "last": 2,
        }
    ]
    assert equality_calls == expected_calls


@pytest.mark.parametrize("callable_key", [False, True])
@pytest.mark.parametrize("callable_value", [False, True])
def test_common_closed_group_failpoint_runs_before_value_selection_and_closes(
    callable_key: bool, callable_value: bool
) -> None:
    """State creation remains the first instrumented transition of a new group."""
    from fpstreams.runtime.failpoints import failpoint

    events: list[str] = []

    class LoggedDict(dict[str, int]):
        def __getitem__(self, key: str) -> int:
            events.append(f"get:{key}")
            return super().__getitem__(key)

    def source():
        try:
            events.append("pull")
            yield LoggedDict(key=1, value=2)
        finally:
            events.append("close")

    key_selector: object = (lambda row: row["key"]) if callable_key else "key"
    value_selector: object = (
        (lambda row: events.append("value") or row["value"]) if callable_value else "value"
    )
    grouped = (
        fpstreams.rows(source())
        .group_by(key=key_selector)
        .aggregate(count=fpstreams.agg.count(), total=fpstreams.agg.sum(value_selector))
    )

    with (
        failpoint("group.state.create.after", RuntimeError("stop after state")),
        pytest.raises(RuntimeError, match="stop after state"),
    ):
        grouped.to_list()
    assert events == ["pull", "get:key", "close"]


def test_simple_group_sum_planning_rejects_nonexact_or_more_general_shapes() -> None:
    """Protocol-sensitive selectors and multi-state/spilling plans keep the generic program."""

    class FieldName(str):
        pass

    candidates = [
        fpstreams.rows(((1, 2),)).group_by(key=True).aggregate(total=fpstreams.agg.sum(1)),
        fpstreams.rows(((1, 2),)).group_by(key=0).aggregate(total=fpstreams.agg.sum(True)),
        fpstreams.rows([{"key": 1, "value": 2}])
        .group_by(FieldName("key"))
        .aggregate(total=fpstreams.agg.sum("value")),
        fpstreams.rows([{"key": 1, "value": 2}])
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"), count=fpstreams.agg.count()),
        fpstreams.rows([{"key": 1, "value": 2}])
        .group_by("key")
        .spill(2)
        .aggregate(total=fpstreams.agg.sum("value")),
    ]

    for grouped in candidates:
        physical = compile_query(grouped._flow._query("list"))
        assert isinstance(physical.root, GroupAggregatePhysicalNode)
        assert physical.root.simple_sum is None


@pytest.mark.parametrize(
    ("key_selector", "value_selector"),
    [("payload.key", "value"), ("key", "payload.value")],
)
def test_dotted_group_sum_fields_stay_on_the_python_specialization(
    key_selector: str, value_selector: str
) -> None:
    """The exact-dict ABI accepts field names, not dotted selector traversal."""
    grouped = (
        fpstreams.rows([{"payload": {"key": 1, "value": 2}, "key": 1, "value": 2}])
        .group_by(key_selector)
        .aggregate(total=fpstreams.agg.sum(value_selector))
    )

    physical = compile_query(grouped._flow._query("list"))

    assert isinstance(physical.root, GroupAggregatePhysicalNode)
    assert physical.root.simple_sum is not None
    assert physical.root.native_record_i64_sum is None


def test_tuple_group_sum_compiles_to_the_exact_native_i64_fast_path() -> None:
    """A proven tuple/index/sum shape can bypass per-row Python collector dispatch."""
    total = fpstreams.agg.sum(1)
    grouped = (
        fpstreams.rows(((3, 5), (1, 7), (3, 11), (2, -4), (1, 2)))
        .group_by(key=0)
        .aggregate(total=total)
    )

    physical = compile_query(grouped._flow._query("list"))

    assert isinstance(physical.root, GroupAggregatePhysicalNode)
    assert total.native is None
    assert physical.root.simple_sum is not None
    assert physical.root.native_i64_sum == NativeGroupSumSpec(0, 1, "total")
    assert grouped.to_list() == [
        {"key": 3, "total": 16},
        {"key": 1, "total": 9},
        {"key": 2, "total": -4},
    ]


@pytest.mark.parametrize("container", [list, tuple])
def test_exact_tuple_group_sum_runs_native_before_the_simple_python_loop(
    monkeypatch, container
) -> None:
    """The broader Python specialization must not displace the guarded Rust kernel."""
    from fpstreams import _native

    calls: list[tuple[object, int, int, str, str]] = []
    expected = [{"native": True}]

    def native_result(
        source: object,
        key_index: int,
        value_index: int,
        key_name: str,
        output_name: str,
    ) -> tuple[bool, list[dict[str, bool]]]:
        calls.append((source, key_index, value_index, key_name, output_name))
        return True, expected

    def unexpected_old_kernel(*_arguments: object) -> object:
        raise AssertionError("the v1 result must not rescan through the old pair ABI")

    def unexpected_executor(*_arguments: object, **_options: object) -> object:
        raise AssertionError("a successful native list result must be returned directly")

    monkeypatch.setattr(_native, "group_sum_i64_rows_v1", native_result, raising=False)
    monkeypatch.setattr(_native, "group_sum_i64_pairs", unexpected_old_kernel)
    monkeypatch.setattr(
        "fpstreams.streams.flow_terminals.execute_physical",
        unexpected_executor,
    )
    source = container(((1, 2), (1, 3)))

    result = fpstreams.rows(source).group_by(key=0).aggregate(total=fpstreams.agg.sum(1)).to_list()

    assert result is expected
    assert calls == [(source, 0, 1, "key", "total")]


def test_tuple_group_sum_falls_back_to_the_old_pair_abi_for_an_older_wheel(
    monkeypatch,
) -> None:
    """An editable Python tree remains compatible with an extension predating the v1 symbol."""
    from fpstreams import _native

    calls: list[tuple[object, int, int]] = []

    def old_result(source: object, key_index: int, value_index: int):
        calls.append((source, key_index, value_index))
        return [(7, 99)]

    monkeypatch.delattr(_native, "group_sum_i64_rows_v1", raising=False)
    monkeypatch.setattr(_native, "group_sum_i64_pairs", old_result)
    source = ((1, 2), (1, 3))

    assert fpstreams.rows(source).group_by(key=0).aggregate(
        total=fpstreams.agg.sum(1)
    ).to_list() == [{"key": 7, "total": 99}]
    assert calls == [(source, 0, 1)]


@pytest.mark.parametrize("container", [list, tuple])
def test_tuple_row_group_sum_preserves_first_key_identity_and_encounter_order(container) -> None:
    """Both exact retained containers return the first equal key object in source order."""
    first_key = int("1000")
    equal_key = int("1000")
    later_key = int("2000")
    assert first_key == equal_key and first_key is not equal_key
    source = container(((first_key, 2), (later_key, 7), (equal_key, 3)))

    result = fpstreams.rows(source).group_by(key=0).aggregate(total=fpstreams.agg.sum(1)).to_list()

    assert result == [
        {"key": first_key, "total": 5},
        {"key": later_key, "total": 7},
    ]
    assert result[0]["key"] is first_key
    assert result[1]["key"] is later_key


@pytest.mark.parametrize("container", [list, tuple])
def test_record_group_sum_falls_back_to_the_old_pair_abi_for_an_older_wheel(
    monkeypatch, container
) -> None:
    """An editable Python tree remains compatible with a wheel predating record v1."""
    from fpstreams import _native

    first_key = int("1000")
    source = container(({"key": first_key, "value": 2}, {"key": int("1000"), "value": 3}))
    calls: list[tuple[object, str, str]] = []

    def native_result(source_data: object, key_field: str, value_field: str):
        calls.append((source_data, key_field, value_field))
        return [(first_key, 99)]

    monkeypatch.delattr(_native, "group_sum_i64_dict_rows_v1", raising=False)
    monkeypatch.setattr(_native, "group_sum_i64_dict_rows", native_result)

    result = (
        fpstreams.rows(source).group_by("key").aggregate(total=fpstreams.agg.sum("value")).to_list()
    )

    assert result == [{"key": first_key, "total": 99}]
    assert result[0]["key"] is first_key
    assert calls == [(source, "key", "value")]


@pytest.mark.parametrize("container", [list, tuple])
def test_exact_record_group_sum_accepts_final_rows_from_the_v1_abi(monkeypatch, container) -> None:
    """A high-cardinality v1 result bypasses Python pair-to-row materialization."""
    from fpstreams import _native

    source = container(({"group": 1, "value": 2}, {"group": 1, "value": 3}))
    expected = [{"alias": 1, "total": 99}]
    calls: list[tuple[object, str, str, str, str]] = []

    def native_result(
        source_data: object,
        key_field: str,
        value_field: str,
        key_name: str,
        output_name: str,
    ) -> tuple[bool, list[dict[str, int]]]:
        calls.append((source_data, key_field, value_field, key_name, output_name))
        return True, expected

    def unexpected_old_kernel(*_arguments: object) -> object:
        raise AssertionError("the v1 result must not rescan through the old pair ABI")

    monkeypatch.setattr(
        _native,
        "group_sum_i64_dict_rows_v1",
        native_result,
        raising=False,
    )
    monkeypatch.setattr(_native, "group_sum_i64_dict_rows", unexpected_old_kernel)

    result = (
        fpstreams.rows(source)
        .group_by(alias="group")
        .aggregate(total=fpstreams.agg.sum("value"))
        .to_list()
    )

    assert result is expected
    assert calls == [(source, "group", "value", "alias", "total")]


@pytest.mark.parametrize("container", [list, tuple])
def test_exact_record_group_sum_materializes_small_v1_pair_payloads(monkeypatch, container) -> None:
    """A low-cardinality v1 result retains the cheaper pair payload."""
    from fpstreams import _native

    first_key = int("1000")
    source = container(({"key": first_key, "value": 2},))
    calls: list[tuple[object, str, str, str, str]] = []

    def native_result(
        source_data: object,
        key_field: str,
        value_field: str,
        key_name: str,
        output_name: str,
    ) -> tuple[bool, list[tuple[int, int]]]:
        calls.append((source_data, key_field, value_field, key_name, output_name))
        return False, [(first_key, 99)]

    def unexpected_old_kernel(*_arguments: object) -> object:
        raise AssertionError("the v1 result must not rescan through the old pair ABI")

    monkeypatch.setattr(
        _native,
        "group_sum_i64_dict_rows_v1",
        native_result,
        raising=False,
    )
    monkeypatch.setattr(_native, "group_sum_i64_dict_rows", unexpected_old_kernel)

    result = (
        fpstreams.rows(source).group_by("key").aggregate(total=fpstreams.agg.sum("value")).to_list()
    )

    assert result == [{"key": first_key, "total": 99}]
    assert result[0]["key"] is first_key
    assert calls == [(source, "key", "value", "key", "total")]


@pytest.mark.parametrize("container", [list, tuple])
def test_record_group_sum_preserves_first_key_identity_and_encounter_order(container) -> None:
    """The public native path returns the first equal key object in first-seen order."""
    first_key = int("1000")
    equal_key = int("1000")
    later_key = int("2000")
    assert first_key == equal_key and first_key is not equal_key
    source = container(
        (
            {"key": first_key, "value": 2},
            {"key": later_key, "value": 7},
            {"key": equal_key, "value": 3},
        )
    )

    result = (
        fpstreams.rows(source).group_by("key").aggregate(total=fpstreams.agg.sum("value")).to_list()
    )

    assert result == [
        {"key": first_key, "total": 5},
        {"key": later_key, "total": 7},
    ]
    assert result[0]["key"] is first_key
    assert result[1]["key"] is later_key


def test_record_group_sum_guard_rejections_preserve_python_numeric_and_mapping_semantics() -> None:
    """Bool, bigint, and non-exact records restart through the selected Python sum."""

    class Record(dict):
        pass

    huge = 2**100
    cases = [
        (
            [{"key": True, "value": True}, {"key": 1, "value": 3}],
            [{"key": True, "total": 4}],
        ),
        (
            [{"key": huge, "value": huge}, {"key": huge, "value": 2}],
            [{"key": huge, "total": huge + 2}],
        ),
        (
            [{"key": 1, "value": 2}, Record(key=1, value=3)],
            [{"key": 1, "total": 5}],
        ),
    ]

    for source, expected in cases:
        result = (
            fpstreams.rows(source)
            .group_by("key")
            .aggregate(total=fpstreams.agg.sum("value"))
            .to_list()
        )
        assert result == expected


def test_record_group_sum_non_string_dict_key_does_not_repeat_collision_equality() -> None:
    """The ABI key scan must reject before lookup so only Python fallback compares keys."""

    class CollidingKey:
        def __init__(self) -> None:
            self.equality_calls = 0

        def __hash__(self) -> int:
            return hash("key")

        def __eq__(self, _other: object) -> bool:
            self.equality_calls += 1
            return False

    collision = CollidingKey()
    record = {collision: "trap", "key": 7, "value": 4}

    def grouped(engine: str) -> list[dict[str, object]]:
        return (
            fpstreams.rows([record])
            .with_engine(engine)
            .group_by("key")
            .aggregate(total=fpstreams.agg.sum("value"))
            .to_list()
        )

    collision.equality_calls = 0
    python_result = grouped("python")
    python_calls = collision.equality_calls

    collision.equality_calls = 0
    auto_result = grouped("auto")
    auto_calls = collision.equality_calls

    assert python_result == auto_result == [{"key": 7, "total": 4}]
    assert python_calls > 0
    assert auto_calls == python_calls


@pytest.mark.parametrize("container_type", [list, tuple])
def test_record_group_sum_container_subclasses_bypass_native(monkeypatch, container_type) -> None:
    """Only exact retained containers may cross the native ABI boundary."""
    from fpstreams import _native

    class Container(container_type):
        pass

    def unexpected_native(*_arguments: object) -> object:
        raise AssertionError("container subclasses must remain on Python")

    monkeypatch.setattr(
        _native,
        "group_sum_i64_dict_rows_v1",
        unexpected_native,
        raising=False,
    )
    monkeypatch.setattr(_native, "group_sum_i64_dict_rows", unexpected_native)
    source = Container(({"key": 1, "value": 2}, {"key": 1, "value": 3}))

    result = (
        fpstreams.rows(source).group_by("key").aggregate(total=fpstreams.agg.sum("value")).to_list()
    )

    assert result == [{"key": 1, "total": 5}]


def test_record_group_sum_requires_a_direct_source_leaf(monkeypatch) -> None:
    """A preceding physical row stage keeps grouping on the recursive Python executor."""
    from fpstreams import _native

    def unexpected_native(*_arguments: object) -> object:
        raise AssertionError("nested physical inputs must remain on Python")

    monkeypatch.setattr(
        _native,
        "group_sum_i64_dict_rows_v1",
        unexpected_native,
        raising=False,
    )
    monkeypatch.setattr(_native, "group_sum_i64_dict_rows", unexpected_native)
    grouped = (
        fpstreams.rows([{"key": 1, "value": 2}, {"key": 1, "value": 3}])
        .where(lambda record: record["value"] > 0)
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
    )

    assert grouped.to_list() == [{"key": 1, "total": 5}]


@pytest.mark.parametrize(("container", "width"), [(list, 32), (tuple, 128)])
def test_record_group_sum_preflights_a_wide_first_row_before_native_snapshot(
    monkeypatch, container, width: int
) -> None:
    """A wide exact first record declines native before an exact container is snapshotted."""
    from fpstreams import _native

    record = {f"field_{index}": index for index in range(width - 2)}
    record.update(key=1, value=2)

    def unexpected_native(*_arguments: object) -> object:
        raise AssertionError("wide first rows must decline before the native call")

    monkeypatch.setattr(_native, "record_group_sum_max_fields", 24, raising=False)
    monkeypatch.setattr(
        _native,
        "group_sum_i64_dict_rows_v1",
        unexpected_native,
        raising=False,
    )
    monkeypatch.setattr(_native, "group_sum_i64_dict_rows", unexpected_native)
    grouped = (
        fpstreams.rows(container((record,)))
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
    )

    assert grouped.to_list() == [{"key": 1, "total": 2}]


def test_record_group_sum_uses_the_native_width_capability_value(monkeypatch) -> None:
    """Python preflight follows the native marker instead of duplicating its threshold."""
    from fpstreams import _native

    def unexpected_native(*_arguments: object) -> object:
        raise AssertionError("the marker-specific wide row must stay on Python")

    monkeypatch.setattr(_native, "record_group_sum_max_fields", 3, raising=False)
    monkeypatch.setattr(
        _native,
        "group_sum_i64_dict_rows_v1",
        unexpected_native,
        raising=False,
    )
    monkeypatch.setattr(_native, "group_sum_i64_dict_rows", unexpected_native)
    source = [{"key": 1, "value": 2, "left": 3, "right": 4}]

    result = (
        fpstreams.rows(source).group_by("key").aggregate(total=fpstreams.agg.sum("value")).to_list()
    )

    assert result == [{"key": 1, "total": 2}]


def test_record_group_sum_without_a_width_capability_uses_python(monkeypatch) -> None:
    """An intermediate or older wheel cannot run a kernel with an unknown width policy."""
    from fpstreams import _native

    def unexpected_native(*_arguments: object) -> object:
        raise AssertionError("a missing width marker must disable record native")

    monkeypatch.delattr(_native, "record_group_sum_max_fields", raising=False)
    monkeypatch.setattr(
        _native,
        "group_sum_i64_dict_rows_v1",
        unexpected_native,
        raising=False,
    )
    monkeypatch.setattr(_native, "group_sum_i64_dict_rows", unexpected_native)

    result = (
        fpstreams.rows([{"key": 1, "value": 2}])
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
        .to_list()
    )

    assert result == [{"key": 1, "total": 2}]


def test_record_group_sum_preflight_never_calls_custom_dict_len(monkeypatch) -> None:
    """A dict subclass declines by exact type before its length protocol can run."""
    from fpstreams import _native

    class Record(dict):
        len_calls = 0

        def __len__(self) -> int:
            type(self).len_calls += 1
            return super().__len__()

    def unexpected_native(*_arguments: object) -> object:
        raise AssertionError("dict subclasses must decline before the native call")

    monkeypatch.setattr(_native, "record_group_sum_max_fields", 24, raising=False)
    monkeypatch.setattr(
        _native,
        "group_sum_i64_dict_rows_v1",
        unexpected_native,
        raising=False,
    )
    monkeypatch.setattr(_native, "group_sum_i64_dict_rows", unexpected_native)
    source = [Record(key=1, value=2)]

    result = (
        fpstreams.rows(source).group_by("key").aggregate(total=fpstreams.agg.sum("value")).to_list()
    )

    assert result == [{"key": 1, "total": 2}]
    assert Record.len_calls == 0


def test_record_group_sum_empty_exact_source_still_uses_native(monkeypatch) -> None:
    """The first-row preflight keeps an empty exact source on the zero-group native path."""
    from fpstreams import _native

    calls = 0

    def tracked_native(*arguments: object) -> object:
        nonlocal calls
        calls += 1
        assert arguments == ([], "key", "value", "key", "total")
        return False, []

    def unexpected_old_kernel(*_arguments: object) -> object:
        raise AssertionError("the v1 result must not rescan through the old pair ABI")

    monkeypatch.setattr(_native, "record_group_sum_max_fields", 24, raising=False)
    monkeypatch.setattr(
        _native,
        "group_sum_i64_dict_rows_v1",
        tracked_native,
        raising=False,
    )
    monkeypatch.setattr(_native, "group_sum_i64_dict_rows", unexpected_old_kernel)

    result = (
        fpstreams.rows([]).group_by("key").aggregate(total=fpstreams.agg.sum("value")).to_list()
    )

    assert result == []
    assert calls == 1


def test_record_group_sum_missing_field_restarts_with_canonical_selection_error() -> None:
    """Unsupported native rows restart from the unopened source and preserve selector errors."""
    grouped = (
        fpstreams.rows([{"key": 1, "value": 2}, {"key": 1}])
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
    )

    with pytest.raises(fpstreams.SelectionError, match="Could not resolve selector 'value'"):
        grouped.to_list()


@pytest.mark.parametrize("shape", ["tuple", "record"])
def test_one_shot_retained_group_source_cannot_bypass_claim(monkeypatch, shape: str) -> None:
    """A retained native container must not make a one-shot relational source replayable."""
    from fpstreams import _native
    from fpstreams.planning.source import Source, SourceCapabilities
    from fpstreams.streams.flow import Flow

    values: list[object] | tuple[object, ...]
    if shape == "tuple":
        values = ((1, 2), (1, 3))
    else:
        values = [{"key": 1, "value": 2}, {"key": 1, "value": 3}]
    events: list[str] = []

    def rows():
        events.append("open")
        try:
            yield from values
        finally:
            events.append("close")

    def unexpected_native(*_arguments: object) -> object:
        raise AssertionError("one-shot group sources must remain claim-owned by Python")

    monkeypatch.setattr(_native, "group_sum_i64_rows_v1", unexpected_native, raising=False)
    monkeypatch.setattr(_native, "group_sum_i64_pairs", unexpected_native)
    monkeypatch.setattr(
        _native,
        "group_sum_i64_dict_rows_v1",
        unexpected_native,
        raising=False,
    )
    monkeypatch.setattr(_native, "group_sum_i64_dict_rows", unexpected_native)
    source = Source(
        rows,
        SourceCapabilities(reiterable=False, exact_size=len(values)),
        native_data=values,
    )
    relation = fpstreams.Rows(Flow(source))
    grouped = (
        relation.group_by(key=0).aggregate(total=fpstreams.agg.sum(1))
        if shape == "tuple"
        else relation.group_by("key").aggregate(total=fpstreams.agg.sum("value"))
    )
    physical = compile_query(grouped._flow._query("list"))
    assert isinstance(physical.root, GroupAggregatePhysicalNode)
    assert physical.root.native_i64_sum is None
    assert physical.root.native_record_i64_sum is None

    assert grouped.to_list() == [{"key": 1, "total": 5}]
    assert events == ["open", "close"]
    with pytest.raises(fpstreams.FlowConsumedError):
        grouped.to_list()


def test_record_group_sum_native_none_reopens_and_closes_the_untouched_source(
    monkeypatch,
) -> None:
    """Compilation/native success never open a leaf; ABI rejection performs one clean Python run."""
    from fpstreams import _native
    from fpstreams.planning.source import Source, SourceCapabilities
    from fpstreams.streams.flow import Flow

    records = [{"key": 1, "value": 2}, {"key": 1, "value": 3}]
    events: list[str] = []

    def values():
        events.append("open")
        try:
            yield from records
        finally:
            events.append("close")

    source = Source(
        values,
        SourceCapabilities(reiterable=True, exact_size=len(records)),
        native_data=records,
    )
    grouped = (
        fpstreams.Rows(Flow(source)).group_by("key").aggregate(total=fpstreams.agg.sum("value"))
    )

    compile_query(grouped._flow._query("list"))
    assert events == []
    assert grouped.to_list() == [{"key": 1, "total": 5}]
    assert events == []

    native_calls = 0

    def decline_native(*_arguments: object) -> None:
        nonlocal native_calls
        native_calls += 1
        return None

    def unexpected_old_kernel(*_arguments: object) -> object:
        raise AssertionError("a v1 None sentinel must not trigger an old-ABI rescan")

    monkeypatch.setattr(
        _native,
        "group_sum_i64_dict_rows_v1",
        decline_native,
        raising=False,
    )
    monkeypatch.setattr(_native, "group_sum_i64_dict_rows", unexpected_old_kernel)
    assert grouped.to_list() == [{"key": 1, "total": 5}]
    assert native_calls == 1
    assert events == ["open", "close"]
    assert grouped.to_list() == [{"key": 1, "total": 5}]
    assert events == ["open", "close", "open", "close"]


@pytest.mark.parametrize("error", [MemoryError("allocation failed"), RuntimeError("ABI bug")])
def test_record_group_sum_real_native_errors_propagate_without_opening_source(
    monkeypatch, error: BaseException
) -> None:
    """Only the ABI's explicit None sentinel requests fallback; real failures remain visible."""
    from fpstreams import _native
    from fpstreams.planning.source import Source, SourceCapabilities
    from fpstreams.streams.flow import Flow

    opens: list[bool] = []
    records = [{"key": 1, "value": 2}]
    source = Source(
        lambda: opens.append(True) or iter(records),
        SourceCapabilities(reiterable=True, exact_size=1),
        native_data=records,
    )

    def fail_native(*_arguments: object) -> object:
        raise error

    def unexpected_old_kernel(*_arguments: object) -> object:
        raise AssertionError("a real v1 failure must not enter the old pair ABI")

    monkeypatch.setattr(
        _native,
        "group_sum_i64_dict_rows_v1",
        fail_native,
        raising=False,
    )
    monkeypatch.setattr(_native, "group_sum_i64_dict_rows", unexpected_old_kernel)
    grouped = (
        fpstreams.Rows(Flow(source)).group_by("key").aggregate(total=fpstreams.agg.sum("value"))
    )

    with pytest.raises(type(error), match=str(error)):
        grouped.to_list()
    assert opens == []


def test_tuple_group_sum_restarts_in_python_when_exact_i64_guards_fail() -> None:
    """Speculative native rejection leaves Python bigint and bool semantics unchanged."""
    huge = 2**100
    records = ((0, huge), (0, 2), (True, 3), (1, 4))

    assert fpstreams.rows(records).group_by(key=0).aggregate(
        total=fpstreams.agg.sum(1)
    ).to_list() == [{"key": 0, "total": huge + 2}, {"key": True, "total": 7}]


@pytest.mark.parametrize("container", [list, tuple])
def test_tuple_group_sum_native_decline_is_not_retried_by_direct_list_fallback(
    monkeypatch, container
) -> None:
    """A v1 decline performs one canonical replay without rescanning through the old ABI."""
    from fpstreams import _native

    calls = 0

    def decline(*_arguments: object) -> None:
        nonlocal calls
        calls += 1
        return None

    def unexpected_old_kernel(*_arguments: object) -> object:
        raise AssertionError("a v1 None sentinel must not trigger a second speculative scan")

    monkeypatch.setattr(_native, "group_sum_i64_rows_v1", decline, raising=False)
    monkeypatch.setattr(_native, "group_sum_i64_pairs", unexpected_old_kernel)

    assert fpstreams.rows(container(((1, 2), (1, 3)))).group_by(key=0).aggregate(
        total=fpstreams.agg.sum(1)
    ).to_list() == [{"key": 1, "total": 5}]
    assert calls == 1


def test_tuple_group_sum_v1_memory_error_propagates_without_old_abi_retry(monkeypatch) -> None:
    """A real allocation failure is not an unsupported sentinel and must remain visible."""
    from fpstreams import _native

    def fail(*_arguments: object) -> object:
        raise MemoryError("native grouping allocation failed")

    def unexpected_old_kernel(*_arguments: object) -> object:
        raise AssertionError("a real v1 failure must not enter the old pair ABI")

    monkeypatch.setattr(_native, "group_sum_i64_rows_v1", fail, raising=False)
    monkeypatch.setattr(_native, "group_sum_i64_pairs", unexpected_old_kernel)

    with pytest.raises(MemoryError, match="native grouping allocation failed"):
        fpstreams.rows([(1, 2)]).group_by(key=0).aggregate(total=fpstreams.agg.sum(1)).to_list()


@pytest.mark.parametrize("container_type", [list, tuple])
def test_tuple_row_group_sum_container_subclasses_bypass_native(
    monkeypatch, container_type
) -> None:
    """Container subclasses stay on Python before any speculative ABI call."""
    from fpstreams import _native

    class Container(container_type):
        pass

    def unexpected_native(*_arguments: object) -> object:
        raise AssertionError("container subclasses must remain on Python")

    monkeypatch.setattr(_native, "group_sum_i64_rows_v1", unexpected_native, raising=False)
    monkeypatch.setattr(_native, "group_sum_i64_pairs", unexpected_native)
    source = Container(((1, 2), (1, 3)))

    assert fpstreams.rows(source).group_by(key=0).aggregate(
        total=fpstreams.agg.sum(1)
    ).to_list() == [{"key": 1, "total": 5}]


def test_tuple_row_group_sum_native_guards_do_not_call_subclass_protocols() -> None:
    """A failed native probe observes neither tuple nor int subclass methods."""
    from fpstreams import _native

    class Row(tuple):
        getitem_calls = 0

        def __getitem__(self, index: object) -> object:
            type(self).getitem_calls += 1
            return super().__getitem__(index)  # type: ignore[index]

    class Integer(int):
        index_calls = 0

        def __index__(self) -> int:
            type(self).index_calls += 1
            return int(self)

    assert _native.group_sum_i64_pairs([Row((1, 2))], 0, 1) is None
    assert _native.group_sum_i64_pairs([(Integer(1), 2)], 0, 1) is None
    assert _native.group_sum_i64_pairs([(True, 2)], 0, 1) is None
    assert _native.group_sum_i64_pairs([(1, 2**100)], 0, 1) is None
    assert Row.getitem_calls == 0
    assert Integer.index_calls == 0


def test_tuple_group_sum_falls_back_when_native_extension_is_unavailable(monkeypatch) -> None:
    """The optional acceleration module must never be required for relational execution."""
    import sys

    monkeypatch.delattr(fpstreams, "_native", raising=False)
    monkeypatch.setitem(sys.modules, "fpstreams._native", None)

    assert fpstreams.rows(((1, 2), (1, 3), (2, 4))).group_by(key=0).aggregate(
        total=fpstreams.agg.sum(1)
    ).to_list() == [{"key": 1, "total": 5}, {"key": 2, "total": 4}]


def test_record_group_sum_falls_back_when_an_older_extension_lacks_the_symbol(
    monkeypatch,
) -> None:
    """An editable Python install remains compatible with an older optional wheel."""
    from fpstreams import _native

    monkeypatch.delattr(_native, "group_sum_i64_dict_rows_v1", raising=False)
    monkeypatch.delattr(_native, "group_sum_i64_dict_rows")

    result = (
        fpstreams.rows([{"key": 1, "value": 2}, {"key": 1, "value": 3}])
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
        .to_list()
    )

    assert result == [{"key": 1, "total": 5}]


def test_record_group_sum_bypasses_native_while_any_failpoint_is_active(monkeypatch) -> None:
    """Instrumented runs retain Python transition coverage instead of skipping source.open."""
    from fpstreams import _native
    from fpstreams.runtime.failpoints import failpoint

    def unexpected_native(*_arguments: object) -> object:
        raise AssertionError("active failpoints must disable native record grouping")

    monkeypatch.setattr(
        _native,
        "group_sum_i64_dict_rows_v1",
        unexpected_native,
        raising=False,
    )
    monkeypatch.setattr(_native, "group_sum_i64_dict_rows", unexpected_native)
    grouped = (
        fpstreams.rows([{"key": 1, "value": 2}, {"key": 1, "value": 3}])
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
    )

    with failpoint("unrelated.transition", RuntimeError("unused")):
        assert grouped.to_list() == [{"key": 1, "total": 5}]


def test_tuple_row_group_sum_bypasses_native_while_any_failpoint_is_active(monkeypatch) -> None:
    """Instrumented tuple-row runs must keep their ordinary Python transition coverage."""
    from fpstreams import _native
    from fpstreams.runtime.failpoints import failpoint

    def unexpected_native(*_arguments: object) -> object:
        raise AssertionError("active failpoints must disable native tuple-row grouping")

    monkeypatch.setattr(_native, "group_sum_i64_rows_v1", unexpected_native, raising=False)
    monkeypatch.setattr(_native, "group_sum_i64_pairs", unexpected_native)
    grouped = fpstreams.rows([(1, 2), (1, 3)]).group_by(key=0).aggregate(total=fpstreams.agg.sum(1))

    with failpoint("unrelated.transition", RuntimeError("unused")):
        assert grouped.to_list() == [{"key": 1, "total": 5}]


def test_tuple_group_sum_keeps_boolean_and_callable_selectors_on_python() -> None:
    """Only exact integer selectors populate native key/value index fields."""
    records = ((10, 1), (20, 1))
    boolean_key = fpstreams.rows(records).group_by(key=True).aggregate(total=fpstreams.agg.sum(0))
    callable_value = (
        fpstreams.rows(records)
        .group_by(key=1)
        .aggregate(total=fpstreams.agg.sum(lambda row: row[0]))
    )

    boolean_physical = compile_query(boolean_key._flow._query("list"))
    callable_physical = compile_query(callable_value._flow._query("list"))

    assert isinstance(boolean_physical.root, GroupAggregatePhysicalNode)
    assert isinstance(callable_physical.root, GroupAggregatePhysicalNode)
    assert boolean_physical.root.native_i64_sum is None
    assert callable_physical.root.native_i64_sum is None
    assert boolean_key.to_list() == [{"key": 1, "total": 30}]
    assert callable_value.to_list() == [{"key": 1, "total": 30}]


def test_tuple_group_sum_handles_mixed_widths_negative_indexes_and_sparse_keys() -> None:
    """Adaptive native layouts preserve per-row indexing and avoid dense sparse-key storage."""
    mixed_width = ((10, 1), (99, 10, 2), (10, 3))
    sparse = ((-(2**63), 1), (2**63 - 1, 2), (-(2**63), 3))

    assert fpstreams.rows(mixed_width).group_by(key=-2).aggregate(
        total=fpstreams.agg.sum(-1)
    ).to_list() == [{"key": 10, "total": 6}]
    assert fpstreams.rows(sparse).group_by(key=0).aggregate(
        total=fpstreams.agg.sum(1)
    ).to_list() == [
        {"key": -(2**63), "total": 4},
        {"key": 2**63 - 1, "total": 2},
    ]


def test_unique_right_strategy_uses_the_cardinality_contract() -> None:
    joined = fpstreams.rows([{"id": 1}, {"id": 2}]).join(
        fpstreams.rows([{"id": 1, "name": "one"}]),
        on="id",
        how="left",
        validate="m:1",
    )
    physical = compile_query(joined._flow._query("list"))

    assert isinstance(physical.root, JoinPhysicalNode)
    assert physical.root.strategy is JoinStrategy.UNIQUE_RIGHT
    assert joined.to_list() == [
        {"id": 1, "name": "one"},
        {"id": 2, "name": None},
    ]


def test_eager_exact_record_join_is_visible_to_the_relational_planner() -> None:
    """Only an eager list over direct exact-record leaves earns the guarded ABI marker."""
    joined = fpstreams.rows([{"id": 1, "left": "a"}]).join(
        ({"id": 1, "right": "A"},),
        on="id",
        validate="m:1",
    )

    eager = compile_query(joined._flow._query("list"))
    streaming = compile_query(joined._flow._query("iterate"))

    assert isinstance(eager.root, JoinPhysicalNode)
    native = getattr(eager.root, "native_record_i64", None)
    assert native is not None
    assert native.left_field == "id"
    assert native.right_field == "id"
    assert isinstance(streaming.root, JoinPhysicalNode)
    assert getattr(streaming.root, "native_record_i64", None) is None


def test_join_planner_lowers_direct_row_expression_fields_without_renaming() -> None:
    """A direct Field leaf earns the existing accessor but keeps callable naming."""
    from fpstreams.expressions.selectors import _direct_field

    joined = fpstreams.rows([{"id": 1, "left": "a"}]).join(
        ({"id": 1, "right": "A"},),
        on=col("id"),
        validate="m:1",
    )
    physical = compile_query(joined._flow._query("list"))

    assert isinstance(physical.root, JoinPhysicalNode)
    assert _direct_field(physical.root.spec.left_key) == "id"
    assert _direct_field(physical.root.spec.right_key) == "id"
    assert physical.root.native_direct_fields is not None
    assert physical.root.native_direct_fields.left_field == "id"
    assert physical.root.native_direct_fields.right_field == "id"
    assert physical.root.native_record_i64 is None
    assert physical.root.native_callable_unique is False
    assert physical.root.spec.shared_names == frozenset()
    assert joined.to_list() == [
        {"id": 1, "left": "a", "id_right": 1, "right": "A"},
    ]

    distinct = fpstreams.rows([{"left_id": 1, "left": "a"}]).join(
        ({"right_id": 1, "right": "A"},),
        left_on=col("left_id"),
        right_on=col("right_id"),
        validate="m:1",
    )
    distinct_physical = compile_query(distinct._flow._query("list"))
    assert isinstance(distinct_physical.root, JoinPhysicalNode)
    assert distinct_physical.root.native_direct_fields is not None
    assert distinct_physical.root.native_record_i64 is not None
    assert distinct_physical.root.native_callable_unique is False


@pytest.mark.parametrize(
    ("validate", "direct_kernel"),
    [
        ("m:1", "join_hashable_unique_direct_records_v1"),
        ("m:m", "join_hashable_many_direct_records_v1"),
    ],
)
def test_direct_row_field_join_routes_nonshared_same_fields_to_suffix_aware_native(
    monkeypatch: pytest.MonkeyPatch,
    validate: str,
    direct_kernel: str,
) -> None:
    """Logical id_right naming bypasses the i64 ABI that implicitly shares equal fields."""
    from fpstreams import _native
    from fpstreams.expressions.selectors import _direct_field
    from fpstreams.planning.source import Source
    from fpstreams.tabular.records import _as_record

    left = [{"id": 1, "payload": "left"}]
    right = (
        (
            {"id": 1, "payload": "right-1"},
            {"id": 1, "payload": "right-2"},
        )
        if validate == "m:m"
        else ({"id": 1, "payload": "right-1"},)
    )
    expected = [{"native": validate}]
    calls: list[tuple[object, ...]] = []

    def direct_result(*arguments: object) -> list[dict[str, str]]:
        calls.append(arguments)
        return expected

    def forbidden(*_arguments: object) -> None:
        raise AssertionError("a nonshared equal-field join must bypass i64 and callable ABIs")

    def opened(_source: Source[object]) -> object:
        raise AssertionError("a successful suffix-aware native join must not open its sources")

    monkeypatch.setattr(Source, "open", opened)
    monkeypatch.setattr(_native, direct_kernel, direct_result, raising=False)
    monkeypatch.setattr(_native, "join_i64_unique_dict_rows_v1", forbidden, raising=False)
    monkeypatch.setattr(_native, "join_i64_many_dict_rows_v1", forbidden, raising=False)
    monkeypatch.setattr(_native, "join_hashable_unique_records_v1", forbidden, raising=False)
    monkeypatch.setattr(_native, "join_hashable_many_records_v1", forbidden, raising=False)

    result = fpstreams.rows(left).join(right, on=col("id"), validate=validate).to_list()

    assert result is expected
    assert len(calls) == 1
    arguments = calls[0]
    assert arguments[:4] == (left, right, "id", "id")
    assert arguments[4:7] == (False, "_right", frozenset())
    capabilities = arguments[7]
    assert capabilities[:2] == ((), _as_record)
    assert _direct_field(capabilities[2]) == "id"
    assert _direct_field(capabilities[3]) == "id"


def test_direct_row_field_join_uses_i64_when_physical_and_merge_semantics_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Distinct direct fields with no logical sharing retain the narrow i64 kernel."""
    from fpstreams import _native

    left = [{"left_id": 1, "left": "L"}]
    right = ({"right_id": 1, "right": "R"},)
    expected = [{"native": True}]
    calls: list[tuple[object, ...]] = []

    def i64_result(*arguments: object) -> list[dict[str, bool]]:
        calls.append(arguments)
        return expected

    def forbidden(*_arguments: object) -> None:
        raise AssertionError("a merge-compatible direct pair must prefer the i64 ABI")

    monkeypatch.setattr(_native, "record_join_v1_max_fields", 64, raising=False)
    monkeypatch.setattr(_native, "join_i64_unique_dict_rows_v2", i64_result, raising=False)
    monkeypatch.setattr(_native, "join_i64_unique_dict_rows_v1", forbidden, raising=False)
    monkeypatch.setattr(
        _native,
        "join_hashable_unique_direct_records_v1",
        forbidden,
        raising=False,
    )
    monkeypatch.setattr(_native, "join_hashable_unique_records_v1", forbidden, raising=False)

    result = (
        fpstreams.rows(left)
        .join(
            right,
            left_on=col("left_id"),
            right_on=col("right_id"),
            validate="m:1",
        )
        .to_list()
    )

    assert result is expected
    assert calls == [(left, right, "left_id", "right_id", False)]


def test_direct_row_field_join_old_wheel_falls_back_to_compiled_callable_abi(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A direct-v1 None from an older wheel falls back without losing the fast accessor."""
    from fpstreams import _native
    from fpstreams.expressions.selectors import _direct_field

    expected = [{"fallback": True}]
    calls: list[str] = []

    def decline(*_arguments: object) -> None:
        calls.append("direct")
        return None

    def fallback(*arguments: object) -> list[dict[str, bool]]:
        calls.append("callable")
        assert _direct_field(arguments[2]) == "id"
        assert _direct_field(arguments[3]) == "id"
        return expected

    def i64_forbidden(*_arguments: object) -> None:
        raise AssertionError("logical id_right naming is incompatible with i64 v1")

    monkeypatch.setattr(
        _native,
        "join_hashable_unique_direct_records_v1",
        decline,
        raising=False,
    )
    monkeypatch.setattr(_native, "join_hashable_unique_records_v1", fallback, raising=False)
    monkeypatch.setattr(_native, "join_i64_unique_dict_rows_v1", i64_forbidden, raising=False)

    result = fpstreams.rows([{"id": 1}]).join(({"id": 1},), on=col("id"), validate="m:1").to_list()

    assert result is expected
    assert calls == ["direct", "callable"]


def test_exact_record_join_to_list_runs_the_native_kernel(monkeypatch) -> None:
    """The eager list terminal should consume a successful guarded native result directly."""
    from fpstreams import _native

    left = [{"id": 1, "left": "a"}]
    right = ({"id": 1, "right": "A"},)
    expected = [{"native": True}]
    calls: list[tuple[object, object, str, str, bool]] = []

    def native_result(
        left_source: object,
        right_source: object,
        left_field: str,
        right_field: str,
        left_join: bool,
    ) -> list[dict[str, bool]]:
        calls.append((left_source, right_source, left_field, right_field, left_join))
        return expected

    def broad_forbidden(*_arguments: object) -> None:
        raise AssertionError("a successful direct-i64 join must not probe the hashable ABI")

    monkeypatch.setattr(_native, "record_join_v1_max_fields", 64, raising=False)
    monkeypatch.setattr(_native, "join_i64_unique_dict_rows_v2", native_result, raising=False)
    monkeypatch.setattr(_native, "join_i64_unique_dict_rows_v1", native_result, raising=False)
    monkeypatch.setattr(
        _native,
        "join_hashable_unique_records_v1",
        broad_forbidden,
        raising=False,
    )
    monkeypatch.setattr(
        _native,
        "join_hashable_unique_records_v2",
        broad_forbidden,
        raising=False,
    )

    result = fpstreams.rows(left).join(right, on="id", validate="m:1").to_list()

    assert result is expected
    assert calls == [(left, right, "id", "id", False)]


@pytest.mark.parametrize(
    ("validate", "narrow_name", "broad_name", "right", "expected"),
    [
        (
            "m:1",
            "join_i64_unique_dict_rows_v2",
            "join_hashable_unique_records_v1",
            ({"id": 1, "right": "R1"},),
            [
                {"id": 1, "left": "L1", "right": "R1"},
                {"id": 2, "left": "L2", "right": None},
            ],
        ),
        (
            "m:m",
            "join_i64_many_dict_rows_v1",
            "join_hashable_many_records_v1",
            ({"id": 1, "right": "R1"}, {"id": 1, "right": "R2"}),
            [
                {"id": 1, "left": "L1", "right": "R1"},
                {"id": 1, "left": "L1", "right": "R2"},
                {"id": 2, "left": "L2", "right": None},
            ],
        ),
    ],
)
def test_direct_field_join_i64_decline_keeps_exact_dict_on_canonical_path(
    monkeypatch: pytest.MonkeyPatch,
    validate: str,
    narrow_name: str,
    broad_name: str,
    right: tuple[dict[str, object], ...],
    expected: list[dict[str, object]],
) -> None:
    """A narrow decline cannot broaden exact dicts past canonical field-name semantics."""
    from fpstreams import _native

    left = [{"id": 1, "left": "L1"}, {"id": 2, "left": "L2"}]
    narrow_calls = 0

    def decline(*_arguments: object) -> None:
        nonlocal narrow_calls
        narrow_calls += 1
        return None

    def broad_forbidden(*_arguments: object) -> None:
        raise AssertionError("exact dicts must retain the canonical direct-field executor")

    def wrong_cardinality(*_arguments: object) -> None:
        raise AssertionError("a direct join decline must retain its cardinality")

    monkeypatch.setattr(_native, "record_join_v1_max_fields", 64, raising=False)
    monkeypatch.setattr(_native, narrow_name, decline, raising=False)
    monkeypatch.setattr(_native, broad_name, broad_forbidden, raising=False)
    other_cardinality = "many" if validate == "m:1" else "unique"
    monkeypatch.setattr(
        _native,
        f"join_hashable_{other_cardinality}_records_v1",
        wrong_cardinality,
        raising=False,
    )

    result = (
        fpstreams.rows(left)
        .join(right, on="id", how="left", suffix="_lookup", validate=validate)
        .to_list()
    )

    assert result == expected
    assert narrow_calls == 1


@pytest.mark.parametrize(
    ("record_kind", "validate", "narrow_name", "broad_name", "expected_token"),
    [
        (
            "mappingproxy",
            "m:1",
            "join_i64_unique_dict_rows_v1",
            "join_hashable_unique_direct_records_v1",
            "mappingproxy",
        ),
        (
            "nominal",
            "m:m",
            "join_i64_many_dict_rows_v1",
            "join_hashable_many_direct_records_v1",
            "Record",
        ),
    ],
)
def test_direct_field_mapping_join_i64_decline_uses_direct_exact_type_token(
    monkeypatch: pytest.MonkeyPatch,
    record_kind: str,
    validate: str,
    narrow_name: str,
    broad_name: str,
    expected_token: str,
) -> None:
    """Direct Mapping fields retain exact-type preflight and real left-join output."""
    from collections.abc import Mapping
    from types import MappingProxyType

    from fpstreams import _native
    from fpstreams.expressions.selectors import _direct_field
    from fpstreams.tabular.records import _as_record

    class Record(Mapping[str, object]):
        def __init__(self, values: dict[str, object]) -> None:
            self.values = values

        def __getitem__(self, name: str) -> object:
            return self.values[name]

        def __iter__(self) -> Iterator[str]:
            return iter(self.values)

        def __len__(self) -> int:
            return len(self.values)

    wrap = MappingProxyType if record_kind == "mappingproxy" else Record
    left = [wrap({"id": 1, "left": "L1"}), wrap({"id": 2, "left": "L2"})]
    right_values = (
        [{"id": 1, "right": "R1"}]
        if validate == "m:1"
        else [{"id": 1, "right": "R1"}, {"id": 1, "right": "R2"}]
    )
    right = tuple(wrap(row) for row in right_values)
    native_broad = getattr(_native, broad_name)
    calls: list[tuple[object, ...]] = []

    def decline(*_arguments: object) -> None:
        return None

    def tracked_broad(*arguments: object) -> object:
        calls.append(arguments)
        return native_broad(*arguments)

    def callback_forbidden(*_arguments: object) -> None:
        raise AssertionError("direct Mapping fields must not use callback ABIs")

    monkeypatch.setattr(_native, "record_join_v1_max_fields", 64, raising=False)
    monkeypatch.setattr(_native, narrow_name, decline, raising=False)
    monkeypatch.setattr(_native, broad_name, tracked_broad, raising=False)
    cardinality = "many" if validate == "m:m" else "unique"
    monkeypatch.setattr(
        _native, f"join_hashable_{cardinality}_records_v1", callback_forbidden, raising=False
    )
    monkeypatch.setattr(
        _native, f"join_hashable_{cardinality}_records_v2", callback_forbidden, raising=False
    )

    result = fpstreams.rows(left).join(right, on="id", how="left", validate=validate).to_list()

    expected = (
        [
            {"id": 1, "left": "L1", "right": "R1"},
            {"id": 2, "left": "L2", "right": None},
        ]
        if validate == "m:1"
        else [
            {"id": 1, "left": "L1", "right": "R1"},
            {"id": 1, "left": "L1", "right": "R2"},
            {"id": 2, "left": "L2", "right": None},
        ]
    )
    assert result == expected
    assert len(calls) == 1
    assert calls[0][2:5] == ("id", "id", True)
    capabilities = calls[0][7]
    token_types = tuple(token[0] if type(token) is tuple else token for token in capabilities[0])
    assert tuple(token.__name__ for token in token_types) == (expected_token,)
    if record_kind == "nominal":
        assert capabilities[0] == ((Record, Record.__mro__),)
    assert capabilities[1] is _as_record
    assert _direct_field(capabilities[2]) == "id"
    assert _direct_field(capabilities[3]) == "id"


@pytest.mark.parametrize(
    ("validate", "narrow_name", "direct_name"),
    [
        (
            "m:1",
            "join_i64_unique_dict_rows_v1",
            "join_hashable_unique_direct_records_v1",
        ),
        (
            "m:m",
            "join_i64_many_dict_rows_v1",
            "join_hashable_many_direct_records_v1",
        ),
    ],
)
def test_direct_field_mapping_join_prefers_the_direct_field_abi(
    monkeypatch: pytest.MonkeyPatch,
    validate: str,
    narrow_name: str,
    direct_name: str,
) -> None:
    """A Mapping field plan passes exact strings instead of Python key callbacks."""
    from types import MappingProxyType

    from fpstreams import _native
    from fpstreams.expressions.selectors import _direct_field
    from fpstreams.tabular.records import _as_record

    left = [MappingProxyType({"id": "a", "left": "L"})]
    right = (MappingProxyType({"id": "a", "right": "R"}),)
    expected = [{"native": direct_name}]
    calls: list[tuple[object, ...]] = []

    def decline(*_arguments: object) -> None:
        return None

    def direct(*arguments: object) -> list[dict[str, str]]:
        calls.append(arguments)
        return expected

    def callback_forbidden(*_arguments: object) -> None:
        raise AssertionError("a direct field token must not be wrapped in a Python callback")

    cardinality = "many" if validate == "m:m" else "unique"
    monkeypatch.setattr(_native, narrow_name, decline, raising=False)
    monkeypatch.setattr(_native, direct_name, direct, raising=False)
    monkeypatch.setattr(
        _native,
        f"join_hashable_{cardinality}_records_v2",
        callback_forbidden,
    )

    result = fpstreams.rows(left).join(right, on="id", validate=validate).to_list()

    assert result is expected
    assert len(calls) == 1
    assert calls[0][0:4] == (left, right, "id", "id")
    assert calls[0][4:7] == (False, "_right", frozenset({"id"}))
    capabilities = calls[0][7]
    assert capabilities[:2] == ((MappingProxyType,), _as_record)
    assert _direct_field(capabilities[2]) == "id"
    assert _direct_field(capabilities[3]) == "id"


def test_direct_field_mappingproxy_snapshot_emits_no_gc_or_types_import_audit_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The native proxy snapshot uses a stable type slot without audited introspection."""
    import sys
    from types import MappingProxyType

    from fpstreams import _native

    native = _native.join_hashable_unique_direct_records_v1
    calls = 0

    def tracked(*arguments: object) -> object:
        nonlocal calls
        calls += 1
        return native(*arguments)

    monkeypatch.setattr(_native, "join_hashable_unique_direct_records_v1", tracked)
    observed: list[str] = []
    active = True

    def audit(event: str, arguments: tuple[object, ...]) -> None:
        imports_types = event == "import" and bool(arguments) and arguments[0] == "types"
        if active and (imports_types or event.startswith("gc.")):
            observed.append(event)

    sys.addaudithook(audit)
    try:
        result = (
            fpstreams.rows([MappingProxyType({"id": 1, "left": "L"})])
            .join(
                (MappingProxyType({"id": 1, "right": "R"}),),
                on="id",
                validate="m:1",
            )
            .to_list()
        )
    finally:
        active = False

    assert result == [{"id": 1, "left": "L", "right": "R"}]
    assert calls == 1
    assert observed == []


def test_direct_field_mapping_old_wheel_uses_the_callback_v2_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A wheel without the new symbol retains the guarded generated-selector path."""
    from types import MappingProxyType

    from fpstreams import _native

    native_v2 = _native.join_hashable_unique_records_v2
    calls: list[tuple[object, ...]] = []

    def decline(*_arguments: object) -> None:
        return None

    def tracked_v2(*arguments: object) -> object:
        calls.append(arguments)
        return native_v2(*arguments)

    monkeypatch.setattr(_native, "join_i64_unique_dict_rows_v1", decline)
    monkeypatch.delattr(_native, "join_hashable_unique_direct_records_v1")
    monkeypatch.setattr(_native, "join_hashable_unique_records_v2", tracked_v2)
    left = [MappingProxyType({"id": "a", "left": "L"})]
    right = (MappingProxyType({"id": "a", "right": "R"}),)

    result = fpstreams.rows(left).join(right, on="id", validate="m:1").to_list()

    assert result == [{"id": "a", "left": "L", "right": "R"}]
    assert len(calls) == 1
    assert callable(calls[0][2]) and callable(calls[0][3])
    assert calls[0][8] == (MappingProxyType,)


@pytest.mark.parametrize(
    "kernel_name",
    [
        "join_hashable_unique_direct_records_v1",
        "join_hashable_many_direct_records_v1",
    ],
)
def test_direct_field_mapping_abi_rejects_invalid_shapes_without_protocol_effects(
    kernel_name: str,
) -> None:
    """Every ABI decline completes before record iteration or subscription begins."""
    from collections.abc import Mapping

    from fpstreams import _native

    events: list[str] = []

    class FieldName(str):
        pass

    class Record(Mapping[str, object]):
        def __getitem__(self, name: str) -> object:
            events.append(f"get:{name}")
            return 1

        def __iter__(self) -> Iterator[str]:
            events.append("iter")
            return iter(("id",))

        def __len__(self) -> int:
            return 1

    kernel = getattr(_native, kernel_name)
    left = [Record()]
    right = (Record(),)
    valid_tail: tuple[object, ...] = (
        False,
        "_right",
        frozenset({"id"}),
        (Record,),
    )
    invalid_arguments = [
        (FieldName("id"), "id", *valid_tail),
        ("id", FieldName("id"), *valid_tail),
        ("nested.id", "id", *valid_tail),
        ("id", "id", False, FieldName("_right"), frozenset({"id"}), (Record,)),
        ("id", "id", False, "_right", frozenset({FieldName("id")}), (Record,)),
        ("id", "id", False, "_right", frozenset({"id"}), [Record]),
        ("id", "id", False, "_right", frozenset({"id"}), ()),
        ("id", "id", False, "_right", frozenset({"id"}), (dict,)),
    ]

    for arguments in invalid_arguments:
        assert kernel(left, right, *arguments) is None
        assert events == []


def test_direct_field_join_missing_i64_symbol_keeps_exact_dict_fallback_canonical(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An old wheel cannot broaden exact dicts past direct-field callback semantics."""
    from fpstreams import _native

    left = [{"id": 1, "left": "L"}]
    right = ({"id": 1, "right": "R"},)

    def broad_forbidden(*_arguments: object) -> None:
        raise AssertionError("an old wheel must use canonical exact-dict fallback")

    monkeypatch.delattr(_native, "join_i64_unique_dict_rows_v1", raising=False)
    monkeypatch.delattr(_native, "record_join_v1_max_fields", raising=False)
    monkeypatch.setattr(_native, "join_hashable_unique_records_v1", broad_forbidden)

    result = fpstreams.rows(left).join(right, on="id", validate="m:1").to_list()

    assert result == [{"id": 1, "left": "L", "right": "R"}]


@pytest.mark.parametrize("record_kind", ["dict", "mappingproxy"])
def test_direct_field_join_missing_hashable_symbol_falls_back_canonically(
    monkeypatch: pytest.MonkeyPatch,
    record_kind: str,
) -> None:
    """Missing v1/v2 broad symbols are an unsupported-shape signal, not a data error."""
    from types import MappingProxyType

    from fpstreams import _native

    wrap = dict if record_kind == "dict" else MappingProxyType
    left = [wrap({"id": 1, "left": "L"}), wrap({"id": 2, "left": "U"})]
    right = (wrap({"id": 1, "right": "R"}),)

    def decline(*_arguments: object) -> None:
        return None

    monkeypatch.setattr(_native, "record_join_v1_max_fields", 64, raising=False)
    monkeypatch.setattr(_native, "join_i64_unique_dict_rows_v1", decline, raising=False)
    monkeypatch.delattr(
        _native,
        f"join_hashable_unique_records_v{1 if record_kind == 'dict' else 2}",
        raising=False,
    )
    if record_kind == "mappingproxy":
        monkeypatch.delattr(_native, "join_hashable_unique_direct_records_v1", raising=False)

    result = fpstreams.rows(left).join(right, on="id", how="left", validate="m:1").to_list()

    assert result == [
        {"id": 1, "left": "L", "right": "R"},
        {"id": 2, "left": "U", "right": None},
    ]


def test_direct_field_mapping_hashable_decline_does_not_replay_mapping_callbacks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both speculative declines happen before Mapping subscription and canonical replay."""
    from collections.abc import Mapping

    from fpstreams import _native

    events: list[str] = []

    class Record(Mapping[str, object]):
        def __init__(self, label: str, values: dict[str, object]) -> None:
            self.label = label
            self.values = values

        def __getitem__(self, name: str) -> object:
            events.append(f"get:{self.label}:{name}")
            return self.values[name]

        def __iter__(self) -> Iterator[str]:
            return iter(self.values)

        def __len__(self) -> int:
            return len(self.values)

    class Unlisted(Record):
        pass

    native_direct = _native.join_hashable_unique_direct_records_v1
    native_v2 = _native.join_hashable_unique_records_v2
    direct_calls = 0
    callback_calls = 0

    def tracked_direct(*arguments: object) -> object:
        nonlocal direct_calls
        direct_calls += 1
        return native_direct(*arguments)

    def tracked_v2(*arguments: object) -> object:
        nonlocal callback_calls
        callback_calls += 1
        return native_v2(*arguments)

    def run(engine: str) -> tuple[list[dict[str, object]], list[str]]:
        events.clear()
        left = [Record("left", {"id": 1, "left": True})]
        right = (
            Record("right-1", {"id": 1, "right": "matched"}),
            Unlisted("right-2", {"id": 2, "right": "unlisted"}),
        )
        query = fpstreams.rows(left).join(right, on="id", validate="m:1")
        if engine == "python":
            query = query.with_engine("python")
        return query.to_list(), events.copy()

    monkeypatch.setattr(_native, "join_hashable_unique_direct_records_v1", tracked_direct)
    monkeypatch.setattr(_native, "join_hashable_unique_records_v2", tracked_v2)
    expected, expected_events = run("python")
    actual, actual_events = run("auto")

    assert actual == expected
    assert actual_events == expected_events
    assert direct_calls == callback_calls == 1


@pytest.mark.parametrize("validate", ["m:1", "m:m"])
@pytest.mark.parametrize("replacement_kind", ["pairs", "attributes"])
def test_direct_field_mapping_live_replacement_restores_canonical_record_protocol(
    monkeypatch: pytest.MonkeyPatch,
    validate: str,
    replacement_kind: str,
) -> None:
    """A snapshot callback cannot make a later direct row bypass canonical conversion."""
    from collections.abc import Mapping

    from fpstreams import _native

    cardinality = "unique" if validate == "m:1" else "many"
    kernel_name = f"join_hashable_{cardinality}_direct_records_v1"
    native_kernel = getattr(_native, kernel_name)
    native_calls = 0

    def tracked_kernel(*arguments: object) -> object:
        nonlocal native_calls
        native_calls += 1
        return native_kernel(*arguments)

    monkeypatch.setattr(_native, kernel_name, tracked_kernel)

    def run(engine: str) -> tuple[object, tuple[str, ...]]:
        events: list[str] = []
        right: list[object] = []
        replaced = False

        class Record(Mapping[str, object]):
            def __init__(self, label: str, **values: object) -> None:
                self.label = label
                self.values = values

            def __iter__(self) -> Iterator[str]:
                return iter(self.values)

            def __len__(self) -> int:
                return len(self.values)

            def __getitem__(self, name: str) -> object:
                nonlocal replaced
                events.append(f"get:{self.label}:{name}")
                if self.label == "right:0" and name == "id" and not replaced:
                    replaced = True
                    right[1] = replacement
                    events.append("replace:right:1")
                return self.values[name]

        class AttributeRecord:
            def __init__(self) -> None:
                self.id = 1
                self.right = "R"

            def __getattribute__(self, name: str) -> object:
                if name == "id":
                    events.append("select:replacement:id")
                return object.__getattribute__(self, name)

        replacement: object = (
            [("id", 1), ("right", "R")] if replacement_kind == "pairs" else AttributeRecord()
        )
        right.extend(
            [
                Record("right:0", id=0, skip="S"),
                Record("right:1", id=1, right="original"),
            ]
        )
        left = [Record("left:1", id=1, left="L")]
        joined = fpstreams.rows(left).join(right, on="id", validate=validate)
        if engine == "python":
            joined = joined.with_engine("python")
        try:
            outcome: object = ("result", joined.to_list())
        except BaseException as error:
            outcome = (type(error), str(error), type(error.__cause__))
        return outcome, tuple(events)

    canonical = run("python")
    automatic = run("auto")

    if replacement_kind == "pairs":
        assert canonical[0] == (
            fpstreams.SelectionError,
            "list cannot be represented as a record",
            type(None),
        )
        assert "select:replacement:id" not in canonical[1]
    else:
        assert canonical[0] == (
            "result",
            [{"id": 1, "left": "L", "right": "R"}],
        )
        assert canonical[1].count("select:replacement:id") == 1
        assert canonical[1].index("select:replacement:id") < canonical[1].index("get:left:1:id")
    assert automatic == canonical
    assert native_calls == 1


def test_direct_field_mapping_hashable_path_preserves_canonical_selection_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The generated direct selector translates only the canonical lookup failures."""
    from types import MappingProxyType

    from fpstreams import _native

    native_direct = _native.join_hashable_unique_direct_records_v1
    native_calls = 0

    def tracked_direct(*arguments: object) -> object:
        nonlocal native_calls
        native_calls += 1
        return native_direct(*arguments)

    monkeypatch.setattr(_native, "join_hashable_unique_direct_records_v1", tracked_direct)
    left = [MappingProxyType({"left": "missing id"})]
    right = (MappingProxyType({"id": 1, "right": "R"}),)

    with pytest.raises(fpstreams.SelectionError) as canonical:
        fpstreams.rows(left).with_engine("python").join(right, on="id", validate="m:1").to_list()
    with pytest.raises(fpstreams.SelectionError) as automatic:
        fpstreams.rows(left).join(right, on="id", validate="m:1").to_list()

    assert str(automatic.value) == str(canonical.value)
    assert type(automatic.value.__cause__) is type(canonical.value.__cause__) is KeyError
    assert automatic.value.__suppress_context__ is canonical.value.__suppress_context__ is True
    assert native_calls == 1


@pytest.mark.parametrize(
    ("error_type", "wrapped"),
    [
        (AttributeError, True),
        (KeyError, True),
        (TypeError, True),
        (ValueError, False),
        (RuntimeError, False),
    ],
)
def test_direct_field_mapping_abi_translates_only_selector_boundary_errors(
    error_type: type[Exception], wrapped: bool
) -> None:
    """Snapshot effects stay ordered and only canonical lookup failures are translated."""
    from collections.abc import Mapping
    from types import MappingProxyType

    events: list[str] = []

    class Record(Mapping[str, object]):
        def __init__(self) -> None:
            self.id_reads = 0

        def __getitem__(self, name: str) -> object:
            events.append(f"get:right:{name}")
            if name == "id":
                self.id_reads += 1
                if self.id_reads == 2:
                    raise error_type("selector failed")
                return 1
            return "R"

        def __iter__(self) -> Iterator[str]:
            events.append("iter:right")
            return iter(("id", "right"))

        def __len__(self) -> int:
            return 2

    query = fpstreams.rows([MappingProxyType({"id": 1, "left": "L"})]).join(
        (Record(),), on="id", validate="m:1"
    )

    expected_error = fpstreams.SelectionError if wrapped else error_type
    with pytest.raises(expected_error) as captured:
        query.to_list()

    if wrapped:
        assert str(captured.value) == "Could not resolve selector 'id'; failed at 'id'"
        assert type(captured.value.__cause__) is error_type
        assert captured.value.__suppress_context__ is True
    else:
        assert type(captured.value) is error_type
        assert captured.value.__cause__ is None
    assert events == [
        "iter:right",
        "get:right:id",
        "get:right:right",
        "get:right:id",
    ]


def test_direct_field_mapping_abi_does_not_translate_snapshot_errors() -> None:
    """An adapter failure precedes field selection and propagates without remapping."""
    from collections.abc import Mapping
    from types import MappingProxyType

    events: list[str] = []

    class Record(Mapping[str, object]):
        def __getitem__(self, name: str) -> object:
            events.append(f"get:{name}")
            raise TypeError("snapshot failed")

        def __iter__(self) -> Iterator[str]:
            events.append("iter")
            return iter(("id",))

        def __len__(self) -> int:
            return 1

    query = fpstreams.rows([MappingProxyType({"id": 1})]).join((Record(),), on="id", validate="m:1")

    with pytest.raises(TypeError, match="snapshot failed") as captured:
        query.to_list()

    assert captured.value.__cause__ is None
    assert events == ["iter", "get:id"]


def test_callable_mapping_join_preserves_dict_constructor_dispatch() -> None:
    """Nominal Mapping snapshots retain keys probing and iterable-pair fallback semantics."""
    from collections.abc import Mapping

    events: list[str] = []

    class TrackedRecord(Mapping[str, object]):
        def __init__(self, label: str, **values: object) -> None:
            self.label = label
            self.values = values

        def __getattribute__(self, name: str) -> object:
            if name == "keys":
                label = object.__getattribute__(self, "label")
                events.append(f"keys:{label}")
            return object.__getattribute__(self, name)

        def __getitem__(self, name: str) -> object:
            return self.values[name]

        def __iter__(self) -> Iterator[str]:
            return iter(self.values)

        def __len__(self) -> int:
            return len(self.values)

    def select_id(row: Mapping[str, object]) -> object:
        return row["id"]

    left = [TrackedRecord("left", id=1, left="L")]
    right = [TrackedRecord("right", id=1, right="R")]
    expected = (
        fpstreams.rows(left)
        .with_engine("python")
        .join(right, on=select_id, validate="m:1")
        .to_list()
    )
    expected_events = events.copy()
    events.clear()

    actual = fpstreams.rows(left).join(right, on=select_id, validate="m:1").to_list()

    assert actual == expected
    assert events == expected_events == ["keys:right", "keys:right", "keys:left", "keys:left"]

    class IterablePairRecord(Mapping[str, object]):
        def __init__(self, **values: object) -> None:
            self.values = values

        @property
        def keys(self) -> object:
            raise AttributeError("use iterable pairs")

        def __getitem__(self, name: str) -> object:
            return self.values[name]

        def __iter__(self) -> Iterator[tuple[str, object]]:  # type: ignore[override]
            return iter(self.values.items())

        def __len__(self) -> int:
            return len(self.values)

    pair_left = [IterablePairRecord(id=1, left="L")]
    pair_right = [IterablePairRecord(id=1, right="R")]
    expected = (
        fpstreams.rows(pair_left)
        .with_engine("python")
        .join(pair_right, on=select_id, validate="m:1")
        .to_list()
    )

    assert (
        fpstreams.rows(pair_left).join(pair_right, on=select_id, validate="m:1").to_list()
        == expected
    )


def test_mapping_many_native_paths_preserve_canonical_unhashable_key_error() -> None:
    """Native callable and generated direct keys retain the m:m Python error boundary."""
    from collections.abc import Mapping

    events: list[str] = []

    class Record(Mapping[str, object]):
        def __init__(self, label: str, values: dict[str, object]) -> None:
            self.label = label
            self.values = values

        def __getitem__(self, name: str) -> object:
            events.append(f"get:{self.label}:{name}")
            return self.values[name]

        def __iter__(self) -> Iterator[str]:
            events.append(f"iter:{self.label}")
            return iter(self.values)

        def __len__(self) -> int:
            events.append(f"len:{self.label}")
            return len(self.values)

    def run(mode: str) -> tuple[tuple[type[BaseException], str, object], list[str]]:
        events.clear()
        left = [Record("left", {"id": 1, "left": True})]
        right = (
            Record("right-ok", {"id": 1, "right": "matched"}),
            Record("right-bad", {"id": [], "right": "unhashable"}),
        )
        rows = fpstreams.rows(left)
        if mode == "python":
            joined = rows.with_engine("python").join(right, on="id", validate="m:m")
        elif mode == "callable":

            def select(record: Mapping[str, object]) -> object:
                return record["id"]

            joined = rows.join(
                right,
                left_on=select,
                right_on=select,
                validate="m:m",
            )
        else:
            joined = rows.join(right, on="id", validate="m:m")

        with pytest.raises(TypeError) as captured:
            joined.to_list()
        error = captured.value
        return (type(error), str(error), error.__cause__), events.copy()

    canonical_error, canonical_events = run("python")
    callable_error, callable_events = run("callable")
    direct_error, direct_events = run("direct")

    assert canonical_error[0] is TypeError
    assert "unhashable type: 'list'" in canonical_error[1]
    assert canonical_error[2] is None
    assert canonical_events == [
        "iter:right-ok",
        "get:right-ok:id",
        "get:right-ok:right",
        "get:right-ok:id",
        "iter:right-bad",
        "get:right-bad:id",
        "get:right-bad:right",
        "get:right-bad:id",
    ]
    assert callable_error == direct_error == canonical_error
    assert callable_events == direct_events == canonical_events


def test_direct_field_mapping_hashable_path_declines_mixed_exact_dict_fields() -> None:
    """A Mapping specialization cannot rehash protocol-sensitive fields from later dict rows."""
    from types import MappingProxyType

    hash_calls: list[str] = []

    class FieldName(str):
        def __hash__(self) -> int:
            hash_calls.append(str(self))
            return super().__hash__()

    field = FieldName("payload")
    left = [
        MappingProxyType({"id": 1, "left": "proxy"}),
        {"id": 2, field: "dict"},
    ]
    right = (
        MappingProxyType({"id": 1, "right": "one"}),
        MappingProxyType({"id": 2, "right": "two"}),
    )

    expected = (
        fpstreams.rows(left).with_engine("python").join(right, on="id", validate="m:1").to_list()
    )
    hash_calls.clear()
    actual = fpstreams.rows(left).join(right, on="id", validate="m:1").to_list()

    assert actual == expected
    assert hash_calls == []


def test_eager_callable_unique_join_is_visible_to_the_guarded_planner() -> None:
    """Only an eager direct callable/callable m:1 join earns the callback ABI marker."""

    def left_key(row: dict[str, int]) -> int:
        return row["left_id"]

    def right_key(row: dict[str, int]) -> int:
        return row["right_id"]

    joined = fpstreams.rows([{"left_id": 1}]).join(
        ({"right_id": 1},),
        left_on=left_key,
        right_on=right_key,
        validate="m:1",
    )

    eager = compile_query(joined._flow._query("list"))
    streaming = compile_query(joined._flow._query("iterate"))

    assert isinstance(eager.root, JoinPhysicalNode)
    assert eager.root.native_callable_unique is True
    assert eager.root.native_callable_many is False
    assert eager.root.native_record_i64 is None
    assert isinstance(streaming.root, JoinPhysicalNode)
    assert streaming.root.native_callable_unique is False
    assert streaming.root.native_callable_many is False


def test_eager_callable_many_join_is_visible_to_the_guarded_planner() -> None:
    """An eager callable/callable m:m join earns only the many-right ABI marker."""

    def left_key(row: dict[str, int]) -> int:
        return row["left_id"]

    def right_key(row: dict[str, int]) -> int:
        return row["right_id"]

    joined = fpstreams.rows([{"left_id": 1}]).join(
        ({"right_id": 1}, {"right_id": 1}),
        left_on=left_key,
        right_on=right_key,
        validate="m:m",
    )

    eager = compile_query(joined._flow._query("list"))
    streaming = compile_query(joined._flow._query("iterate"))

    assert isinstance(eager.root, JoinPhysicalNode)
    assert eager.root.native_callable_unique is False
    assert eager.root.native_callable_many is True
    assert eager.root.native_record_i64 is None
    assert isinstance(streaming.root, JoinPhysicalNode)
    assert streaming.root.native_callable_unique is False
    assert streaming.root.native_callable_many is False


def test_callable_unique_join_v2_is_exported_by_current_native_module() -> None:
    """The current wheel exposes the exact-type-token callback ABI."""
    from fpstreams import _native

    assert callable(getattr(_native, "join_hashable_unique_records_v2", None))


def test_standard_namedtuple_snapshot_factory_is_exported_by_current_native_module() -> None:
    """The current wheel exposes the guarded callable adapter factory."""
    from fpstreams import _native

    assert callable(getattr(_native, "standard_namedtuple_record_adapter_v1", None))


def test_standard_namedtuple_snapshot_factory_exposes_live_guard_arity() -> None:
    """The factory exposes only the guard capabilities used by its native implementation."""
    from inspect import signature

    from fpstreams import _native

    assert tuple(signature(_native.standard_namedtuple_record_adapter_v1).parameters) == (
        "record_types",
        "fallback_adapter",
        "get_cache_token",
        "abc_token",
        "canonical_namedtuple_factory",
        "code_type",
        "mapping_abc",
        "record_continuations",
        "record_globals",
    )


def test_callable_many_join_versions_are_exported_by_current_native_module() -> None:
    """The current wheel exposes exact-dict and exact-type-token many callback ABIs."""
    from fpstreams import _native

    assert callable(getattr(_native, "join_hashable_many_records_v1", None))
    assert callable(getattr(_native, "join_hashable_many_records_v2", None))


def test_callable_many_join_to_list_runs_the_native_kernel(monkeypatch) -> None:
    """A successful many callback ABI bypasses unique ABI and canonical source iterators."""
    from fpstreams import _native
    from fpstreams.planning.source import Source
    from fpstreams.tabular.records import _as_record

    left = [{"left_id": 1, "left": "a"}]
    right = ({"right_id": 1, "right": "A"}, {"right_id": 1, "right": "B"})
    expected = [{"native_many": True}]
    calls: list[tuple[object, ...]] = []

    def left_key(row: dict[str, object]) -> object:
        return row["left_id"]

    def right_key(row: dict[str, object]) -> object:
        return row["right_id"]

    def native_result(*arguments: object) -> list[dict[str, bool]]:
        calls.append(arguments)
        return expected

    def unexpected_unique(*_arguments: object) -> None:
        raise AssertionError("an m:m marker must never call the unique-right ABI")

    def opened(_source: Source[object]) -> object:
        raise AssertionError("a successful callable many ABI must not open Python sources")

    monkeypatch.setattr(Source, "open", opened)
    monkeypatch.setattr(
        _native,
        "join_hashable_many_records_v1",
        native_result,
        raising=False,
    )
    monkeypatch.setattr(
        _native,
        "join_hashable_unique_records_v1",
        unexpected_unique,
    )

    result = (
        fpstreams.rows(left)
        .join(
            right,
            left_on=left_key,
            right_on=right_key,
            how="left",
            suffix="_lookup",
            validate="m:m",
        )
        .to_list()
    )

    assert result is expected
    assert len(calls) == 1
    arguments = calls[0]
    assert arguments[:4] == (left, right, left_key, right_key)
    assert arguments[4] == (dict, _as_record)
    assert arguments[5:] == (True, "_lookup", frozenset())


def test_callable_many_join_old_native_falls_back_without_selector_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An older wheel without many ABIs leaves the m:m shape to canonical Python once."""
    from fpstreams import _native

    events: list[str] = []

    def left_key(row: dict[str, int]) -> int:
        events.append(f"left:{row['id']}")
        return row["id"]

    def right_key(row: dict[str, int]) -> int:
        events.append(f"right:{row['id']}")
        return row["id"]

    monkeypatch.delattr(_native, "join_hashable_many_records_v1", raising=False)
    monkeypatch.delattr(_native, "join_hashable_many_records_v2", raising=False)
    result = (
        fpstreams.rows([{"id": 1, "left": 10}])
        .join(
            ({"id": 1, "right": 20}, {"id": 1, "right": 30}),
            left_on=left_key,
            right_on=right_key,
            validate="m:m",
        )
        .to_list()
    )

    assert result == [
        {"id": 1, "left": 10, "id_right": 1, "right": 20},
        {"id": 1, "left": 10, "id_right": 1, "right": 30},
    ]
    assert events == ["right:1", "right:1", "left:1"]


def test_callable_many_join_mappingproxy_uses_v2_exact_type_token(monkeypatch) -> None:
    """Mapping many joins use v2 with the proven builtin adapter and exact type token."""
    from types import MappingProxyType

    from fpstreams import _native
    from fpstreams.tabular.records import _as_record

    left = [MappingProxyType({"id": 1, "left": "L"})]
    right = (
        MappingProxyType({"id": 1, "right": "R1"}),
        MappingProxyType({"id": 1, "right": "R2"}),
    )
    expected = [{"native_many_v2": True}]
    calls: list[tuple[object, ...]] = []

    def select(row: object) -> object:
        return row["id"]  # type: ignore[index]

    def native_v2(*arguments: object) -> list[dict[str, bool]]:
        calls.append(arguments)
        return expected

    def forbidden(*_arguments: object) -> None:
        raise AssertionError("the v2 many shape must not call another callable ABI")

    monkeypatch.setattr(_native, "join_hashable_many_records_v1", forbidden)
    monkeypatch.setattr(_native, "join_hashable_unique_records_v1", forbidden)
    monkeypatch.setattr(_native, "join_hashable_unique_records_v2", forbidden)
    monkeypatch.setattr(_native, "join_hashable_many_records_v2", native_v2)

    result = (
        fpstreams.rows(left).join(right, left_on=select, right_on=select, validate="m:m").to_list()
    )

    assert result is expected
    assert len(calls) == 1
    assert calls[0][4] == (dict, _as_record)
    assert calls[0][8] == (MappingProxyType,)


@pytest.mark.parametrize("record_kind", ["nominal", "mappingproxy"])
@pytest.mark.parametrize(
    ("validate", "kernel_name", "expected_result", "expected_events"),
    [
        pytest.param(
            "m:1",
            "join_hashable_unique_records_v2",
            [[("id", 1), ("payload", "L"), ("id", 1), ("payload", "R1")]],
            [
                "iter:right-normal",
                "get:right-normal:id",
                "get:right-normal:payload",
                "get:right-normal:id",
                "select:1",
                "iter:right-exotic",
                "get:right-exotic:id",
                "hash:id",
                "get:right-exotic:payload",
                "hash:payload",
                "eq:id:payload",
                "get:right-exotic:id",
                "select:2",
                "hash:id",
                "hash:id",
                "hash:payload",
                "eq:id:payload",
                "hash:payload",
                "eq:id:payload",
                "iter:left",
                "get:left:id",
                "hash:id",
                "get:left:payload",
                "hash:payload",
                "eq:id:payload",
                "get:left:id",
                "select:1",
                "eq:id:payload",
                "hash:id",
                "hash:id",
                "eq:id:id",
                "format:id:",
                "hash:payload",
                "hash:payload",
                "eq:id:payload",
                "eq:payload:payload",
                "format:payload:",
                "hash:id",
                "hash:id",
                "hash:payload",
                "hash:payload",
            ],
            id="unique",
        ),
        pytest.param(
            "m:m",
            "join_hashable_many_records_v2",
            [
                [("id", 1), ("payload", "L"), ("id", 1), ("payload", "R1")],
                [("id", 1), ("payload", "L"), ("id_right", 1), ("payload_right", "R2")],
            ],
            [
                "iter:right-normal",
                "get:right-normal:id",
                "get:right-normal:payload",
                "get:right-normal:id",
                "select:1",
                "iter:right-exotic",
                "get:right-exotic:id",
                "hash:id",
                "get:right-exotic:payload",
                "hash:payload",
                "eq:id:payload",
                "get:right-exotic:id",
                "select:1",
                "hash:id",
                "hash:id",
                "hash:payload",
                "eq:id:payload",
                "hash:payload",
                "eq:id:payload",
                "iter:left",
                "get:left:id",
                "hash:id",
                "get:left:payload",
                "hash:payload",
                "eq:id:payload",
                "get:left:id",
                "select:1",
                "eq:id:payload",
                "hash:id",
                "hash:id",
                "eq:id:id",
                "format:id:",
                "hash:payload",
                "hash:payload",
                "eq:id:payload",
                "eq:payload:payload",
                "format:payload:",
                "hash:id",
                "hash:id",
                "hash:payload",
                "hash:payload",
                "hash:id",
                "hash:id",
                "hash:id",
                "hash:payload",
                "hash:payload",
                "eq:id:payload",
                "hash:payload",
                "eq:id:payload",
            ],
            id="many",
        ),
    ],
)
def test_callable_mapping_v2_preserves_protocol_sensitive_field_trace(
    monkeypatch: pytest.MonkeyPatch,
    record_kind: str,
    validate: str,
    kernel_name: str,
    expected_result: list[list[tuple[str, object]]],
    expected_events: list[str],
) -> None:
    """Native target planning and merge lookups retain canonical field-key callbacks."""
    from collections.abc import Mapping
    from types import MappingProxyType

    from fpstreams import _native

    native = getattr(_native, kernel_name)
    native_calls = 0

    def tracked(*arguments: object) -> object:
        nonlocal native_calls
        native_calls += 1
        return native(*arguments)

    monkeypatch.setattr(_native, kernel_name, tracked)

    def run(engine: str) -> tuple[list[list[tuple[str, object]]], list[str]]:
        events: list[str] = []

        class Name:
            def __init__(self, label: str) -> None:
                self.label = label

            def __hash__(self) -> int:
                events.append(f"hash:{self.label}")
                return 0

            def __eq__(self, other: object) -> bool:
                other_label = getattr(other, "label", type(other).__name__)
                events.append(f"eq:{self.label}:{other_label}")
                return isinstance(other, Name) and self.label == other.label

            def __format__(self, format_spec: str) -> str:
                events.append(f"format:{self.label}:{format_spec}")
                return self.label

        class Record(Mapping[object, object]):
            def __init__(
                self,
                label: str,
                identifier: int,
                payload: str,
                *,
                exotic: bool,
            ) -> None:
                self.label = label
                self.identifier = identifier
                self.payload = payload
                self.names = (Name("id"), Name("payload")) if exotic else ("id", "payload")

            def __iter__(self) -> Iterator[object]:
                events.append(f"iter:{self.label}")
                return iter(self.names)

            def __len__(self) -> int:
                events.append(f"len:{self.label}")
                return 2

            def __getitem__(self, name: object) -> object:
                label = name.label if isinstance(name, Name) else name
                events.append(f"get:{self.label}:{label}")
                if label == "id":
                    return self.identifier
                if label == "payload":
                    return self.payload
                raise KeyError(name)

        def wrap(record: Record) -> object:
            return MappingProxyType(record) if record_kind == "mappingproxy" else record

        left = [wrap(Record("left", 1, "L", exotic=True))]
        right = (
            wrap(Record("right-normal", 1, "R1", exotic=False)),
            wrap(
                Record(
                    "right-exotic",
                    1 if validate == "m:m" else 2,
                    "R2",
                    exotic=True,
                )
            ),
        )

        def select(row: object) -> object:
            value = row["id"]  # type: ignore[index]
            events.append(f"select:{value}")
            return value

        result = (
            fpstreams.rows(left)
            .with_engine(engine)
            .join(right, left_on=select, right_on=select, validate=validate)
            .to_list()
        )
        join_events = events.copy()
        normalized = [
            [(key.label if isinstance(key, Name) else key, value) for key, value in row.items()]
            for row in result
        ]
        return normalized, join_events

    canonical_result, canonical_events = run("python")
    automatic_result, automatic_events = run("auto")

    assert canonical_result == expected_result
    assert canonical_events == expected_events
    assert automatic_result == expected_result
    assert automatic_events == expected_events
    assert native_calls == 1


@pytest.mark.parametrize(
    ("validate", "kernel_name"),
    [
        pytest.param("m:1", "join_hashable_unique_records_v2", id="unique"),
        pytest.param("m:m", "join_hashable_many_records_v2", id="many"),
    ],
)
def test_callable_mapping_v2_cached_plan_skips_sparse_right_fields(
    monkeypatch: pytest.MonkeyPatch,
    validate: str,
    kernel_name: str,
) -> None:
    """A cached union-field plan reads only fields present in each right snapshot."""
    from types import MappingProxyType

    from fpstreams import _native

    native = getattr(_native, kernel_name)
    native_calls = 0

    def tracked(*arguments: object) -> object:
        nonlocal native_calls
        native_calls += 1
        result = native(*arguments)
        assert result is not None
        return result

    def v1_forbidden(*_arguments: object) -> None:
        raise AssertionError("Mapping rows require the exact-type-token v2 ABI")

    monkeypatch.setattr(_native, kernel_name, tracked)
    monkeypatch.setattr(_native, kernel_name.replace("_v2", "_v1"), v1_forbidden)

    left = [
        MappingProxyType({"id": 1, "left": "L1"}),
        MappingProxyType({"id": 2, "left": "L2"}),
    ]
    if validate == "m:1":
        right = (
            MappingProxyType({"id": 1, "alpha": "A1"}),
            MappingProxyType({"id": 2, "beta": "B2"}),
        )
        expected = [
            {"id": 1, "left": "L1", "id_right": 1, "alpha": "A1"},
            {"id": 2, "left": "L2", "id_right": 2, "beta": "B2"},
        ]
    else:
        right = (
            MappingProxyType({"id": 1, "alpha": "A1"}),
            MappingProxyType({"id": 1, "beta": "B1"}),
            MappingProxyType({"id": 2, "alpha": "A2"}),
            MappingProxyType({"id": 2, "gamma": "G2"}),
        )
        expected = [
            {"id": 1, "left": "L1", "id_right": 1, "alpha": "A1"},
            {"id": 1, "left": "L1", "id_right": 1, "beta": "B1"},
            {"id": 2, "left": "L2", "id_right": 2, "alpha": "A2"},
            {"id": 2, "left": "L2", "id_right": 2, "gamma": "G2"},
        ]

    def select(row: object) -> object:
        return row["id"]  # type: ignore[index]

    result = (
        fpstreams.rows(left)
        .join(right, left_on=select, right_on=select, validate=validate)
        .to_list()
    )

    assert result == expected
    assert native_calls == 1


@pytest.mark.parametrize(
    "kernel_name",
    ["join_hashable_unique_records_v2", "join_hashable_many_records_v2"],
)
def test_callable_mapping_v2_seen_columns_keeps_later_equal_str_subclass_observable(
    kernel_name: str,
) -> None:
    """Union dedup cannot hide a later non-exact field from the lookup guard."""
    from types import MappingProxyType

    from fpstreams import _native

    events: list[str] = []
    recording = False

    class EqualField(str):
        def __hash__(self) -> int:
            return super().__hash__()

        def __eq__(self, other: object) -> bool:
            if recording:
                events.append(f"eq:{other}")
            return super().__eq__(other)

    later_payload = EqualField("payload")
    left = [
        MappingProxyType({"id": 1, "left": "L1"}),
        MappingProxyType({"id": 2, "left": "L2"}),
    ]
    right = (
        MappingProxyType({"id": 1, "payload": "exact"}),
        MappingProxyType({"id": 2, later_payload: "subclass"}),
    )

    def right_key(row: object) -> object:
        return row["id"]  # type: ignore[index]

    def left_key(row: object) -> object:
        nonlocal recording
        recording = True
        return row["id"]  # type: ignore[index]

    result = getattr(_native, kernel_name)(
        left,
        right,
        left_key,
        right_key,
        dict,
        False,
        "_right",
        frozenset(),
        (MappingProxyType,),
    )

    assert result == [
        {"id": 1, "left": "L1", "id_right": 1, "payload": "exact"},
        {"id": 2, "left": "L2", "id_right": 2, "payload": "subclass"},
    ]
    assert events == ["eq:payload", "eq:payload"]


@pytest.mark.parametrize(
    "kernel_name",
    ["join_hashable_unique_records_v2", "join_hashable_many_records_v2"],
)
def test_callable_v2_custom_adapter_keeps_right_lookup_observable(kernel_name: str) -> None:
    """An untrusted adapter cannot enable the private exact-string lookup shortcut."""
    from collections.abc import Mapping

    from fpstreams import _native

    events: list[str] = []

    class Name(str):
        def __hash__(self) -> int:
            events.append(f"hash:{self}")
            return super().__hash__()

        def __eq__(self, other: object) -> bool:
            events.append(f"eq:{self}:{other}")
            return super().__eq__(other)

    class Record(Mapping[str, object]):
        def __init__(self, side: str) -> None:
            self.side = side

        def __iter__(self) -> Iterator[str]:
            return iter(("id", "payload" if self.side == "right" else "left"))

        def __len__(self) -> int:
            return 2

        def __getitem__(self, name: str) -> object:
            if name == "id":
                return 1
            if name == "payload" and self.side == "right":
                return "R"
            if name == "left" and self.side == "left":
                return "L"
            raise KeyError(name)

    shared_right: dict[object, object] = {"id": 1, "payload": "R"}

    def adapt(row: Record) -> dict[object, object]:
        if row.side == "right":
            return shared_right
        value = shared_right.pop("payload")
        shared_right[Name("payload")] = value
        return {"id": 1, "left": "L"}

    def select(row: Record) -> object:
        return row["id"]

    kernel = getattr(_native, kernel_name)
    result = kernel(
        [Record("left")],
        (Record("right"),),
        select,
        select,
        adapt,
        False,
        "_right",
        frozenset(),
        (Record,),
    )

    assert result == [{"id": 1, "left": "L", "id_right": 1, "payload": "R"}]
    assert events == ["hash:payload", "eq:payload:payload", "eq:payload:payload"]


@pytest.mark.parametrize(
    "kernel_name",
    ["join_hashable_unique_records_v1", "join_hashable_many_records_v1"],
)
def test_callable_v1_live_replacement_rechecks_snapshot_ownership(kernel_name: str) -> None:
    """An earlier selector may invalidate exact-dict preflight for a later right row."""
    from fpstreams import _native

    events: list[str] = []

    class Name(str):
        def __hash__(self) -> int:
            events.append(f"hash:{self}")
            return super().__hash__()

        def __eq__(self, other: object) -> bool:
            events.append(f"eq:{self}:{other}")
            return super().__eq__(other)

    class Replacement(dict[str, object]):
        pass

    left = [{"id": 1, "left": "L"}]
    right: list[dict[str, object]] = [
        {"id": 0, "skip": "S"},
        {"id": 1, "payload": "original"},
    ]
    shared_right: dict[object, object] = {"id": 1, "payload": "R"}

    def adapt(_row: object) -> dict[object, object]:
        return shared_right

    def right_key(row: dict[str, object]) -> object:
        key = row["id"]
        if key == 0:
            right[1] = Replacement(id=1, payload="replacement")
        return key

    def left_key(row: dict[str, object]) -> object:
        value = shared_right.pop("payload")
        shared_right[Name("payload")] = value
        return row["id"]

    kernel = getattr(_native, kernel_name)
    result = kernel(
        left,
        right,
        left_key,
        right_key,
        adapt,
        False,
        "_right",
        frozenset(),
    )

    assert result == [{"id": 1, "left": "L", "id_right": 1, "payload": "R"}]
    assert events == ["hash:payload", "eq:payload:payload", "eq:payload:payload"]


@pytest.mark.parametrize("validate", ["m:1", "m:m"])
@pytest.mark.parametrize("record_kind", ["dict", "mapping"])
@pytest.mark.parametrize("replacement_kind", ["pairs", "attributes"])
def test_callable_join_live_replacement_restores_canonical_record_protocol(  # noqa: C901
    monkeypatch: pytest.MonkeyPatch,
    validate: str,
    record_kind: str,
    replacement_kind: str,
) -> None:
    """A callback-replaced future row must use canonical record conversion before selection."""
    from collections.abc import Mapping

    from fpstreams import _native

    cardinality = "unique" if validate == "m:1" else "many"
    version = 1 if record_kind == "dict" else 2
    kernel_name = f"join_hashable_{cardinality}_records_v{version}"
    native_kernel = getattr(_native, kernel_name)
    native_calls = 0

    def tracked_kernel(*arguments: object) -> object:
        nonlocal native_calls
        native_calls += 1
        return native_kernel(*arguments)

    monkeypatch.setattr(_native, kernel_name, tracked_kernel)

    def run(engine: str) -> tuple[object, tuple[str, ...]]:
        events: list[str] = []

        class Record(Mapping[str, object]):
            def __init__(self, **values: object) -> None:
                self.values = values

            def __iter__(self) -> Iterator[str]:
                return iter(self.values)

            def __len__(self) -> int:
                return len(self.values)

            def __getitem__(self, name: str) -> object:
                return self.values[name]

        class AttributeRecord:
            def __init__(self) -> None:
                self.id = 1
                self.right = "R"

        def record(**values: object) -> object:
            return values if record_kind == "dict" else Record(**values)

        replacement: object = (
            [("id", 1), ("right", "R")] if replacement_kind == "pairs" else AttributeRecord()
        )
        right: list[object] = [record(id=0, skip="S"), record(id=1, right="original")]
        left = [record(id=1, left="L")]

        def identifier(row: object) -> object:
            if isinstance(row, list):
                return dict(row)["id"]
            if isinstance(row, Mapping):
                return row["id"]
            return row.id  # type: ignore[attr-defined]

        def right_key(row: object) -> object:
            key = identifier(row)
            events.append(f"right:{key}")
            if key == 0:
                right[1] = replacement
            return key

        def left_key(row: object) -> object:
            key = identifier(row)
            events.append(f"left:{key}")
            return key

        joined = (
            fpstreams.rows(left)
            .with_engine(engine)
            .join(
                right,
                left_on=left_key,
                right_on=right_key,
                validate=validate,
            )
        )
        try:
            outcome: object = ("result", joined.to_list())
        except BaseException as error:
            outcome = (type(error), str(error), error.__cause__)
        return outcome, tuple(events)

    canonical = run("python")
    automatic = run("auto")

    if replacement_kind == "pairs":
        assert canonical == (
            (fpstreams.SelectionError, "list cannot be represented as a record", None),
            ("right:0",),
        )
    else:
        assert canonical == (
            (
                "result",
                [{"id": 1, "left": "L", "id_right": 1, "right": "R"}],
            ),
            ("right:0", "right:1", "left:1"),
        )
    assert automatic == canonical
    assert native_calls == 1


@pytest.mark.parametrize("validate", ["m:1", "m:m"])
@pytest.mark.parametrize("selector_kind", ["callable", "direct"])
@pytest.mark.parametrize("mutation_row", ["right:0", "right:1"])
def test_native_mapping_join_rechecks_live_mro_without_replay(  # noqa: C901
    monkeypatch: pytest.MonkeyPatch,
    validate: str,
    selector_kind: str,
    mutation_row: str,
) -> None:
    """The same exact row type stops being a Mapping after its retained MRO changes."""
    from collections.abc import Mapping

    from fpstreams import _native

    cardinality = "unique" if validate == "m:1" else "many"
    kernel_name = (
        f"join_hashable_{cardinality}_records_v2"
        if selector_kind == "callable"
        else f"join_hashable_{cardinality}_direct_records_v1"
    )
    native_kernel = getattr(_native, kernel_name)
    calls: list[tuple[object, ...]] = []

    def tracked_kernel(*arguments: object) -> object:
        calls.append(arguments)
        return native_kernel(*arguments)

    monkeypatch.setattr(_native, kernel_name, tracked_kernel)

    def run(
        engine: str,
    ) -> tuple[
        list[dict[str, object]],
        tuple[str, ...],
        type[object],
        tuple[type[object], ...],
    ]:
        events: list[str] = []
        adapted: set[str] = set()
        selected: set[str] = set()

        class Root:
            def __init__(self, label: str, **attributes: object) -> None:
                self.label = label
                self.attributes = attributes
                vars(self).update(attributes)

            def _asdict(self) -> dict[str, object]:
                if self.label in adapted:
                    raise AssertionError(f"snapshot replayed for {self.label}")
                adapted.add(self.label)
                events.append(f"asdict:{self.label}")
                return dict(self.attributes)

        class MappingBase(Root, Mapping[str, object]):
            def __iter__(self) -> Iterator[str]:
                events.append(f"iter:{self.label}")
                # ``id`` is deliberately last: a direct snapshot can finish before its
                # final subscription changes the live MRO seen by key selection.
                return iter(tuple(reversed(self.attributes)))

            def __len__(self) -> int:
                return len(self.attributes)

            def __getitem__(self, name: str) -> object:
                events.append(f"get:{self.label}:{name}")
                value = self.attributes[name]
                if (
                    selector_kind == "direct"
                    and self.label == mutation_row
                    and name == "id"
                    and MappingBase in Record.__mro__
                ):
                    Record.__bases__ = (AttributeBase,)
                    Mapping._abc_caches_clear()
                    events.append(f"mutate:{self.label}:getitem")
                return value

        class AttributeBase(Root):
            pass

        class Record(MappingBase):
            pass

        initial_mro = Record.__mro__
        right = [
            Record("right:0", id="skip", payload="S"),
            Record("right:1", id="match", payload="R1"),
            Record(
                "right:2",
                id="match" if validate == "m:m" else "other",
                payload="R2",
            ),
        ]
        left = [Record("left:0", id="match", left="L")]

        def select(row: Record) -> object:
            if row.label in selected:
                raise AssertionError(f"selector replayed for {row.label}")
            selected.add(row.label)
            value = row["id"] if isinstance(row, Mapping) else row.id
            events.append(f"select:{row.label}:{value}")
            if (
                selector_kind == "callable"
                and row.label == mutation_row
                and MappingBase in Record.__mro__
            ):
                Record.__bases__ = (AttributeBase,)
                Mapping._abc_caches_clear()
                events.append(f"mutate:{row.label}:selector")
            return value

        query = fpstreams.rows(left).with_engine(engine)
        if selector_kind == "callable":
            joined = query.join(
                right,
                left_on=select,
                right_on=select,
                validate=validate,
            )
        else:
            joined = query.join(right, on="id", validate=validate)
        return joined.to_list(), tuple(events), Record, initial_mro

    canonical_result, canonical_events, _canonical_type, _canonical_mro = run("python")
    automatic_result, automatic_events, automatic_type, automatic_mro = run("auto")

    expected = [{"id": "match", "left": "L", "payload": "R1"}]
    if selector_kind == "callable":
        expected[0]["id_right"] = "match"
    if validate == "m:m":
        second = {"id": "match", "left": "L", "payload": "R2"}
        if selector_kind == "callable":
            second["id_right"] = "match"
        expected.append(second)
    assert canonical_result == expected
    assert automatic_result == expected
    assert automatic_events == canonical_events
    assert len(calls) == 1

    token_envelope = calls[0][8] if selector_kind == "callable" else calls[0][7][0]
    ((token_type, token_mro),) = token_envelope
    assert token_type is automatic_type
    assert token_mro is automatic_mro
    assert token_mro is not automatic_type.__mro__


def test_callable_namedtuple_v2_preserves_dynamic_fields_protocol_trace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A standard NamedTuple keeps canonical live ``_fields`` protocol callbacks in v2."""
    from collections import namedtuple

    from fpstreams import _native
    from fpstreams.tabular.records import _as_record

    native = _native.join_hashable_unique_records_v2
    calls: list[tuple[object, ...]] = []

    def tracked(*arguments: object) -> object:
        calls.append(arguments)
        return native(*arguments)

    def v1_forbidden(*_arguments: object) -> None:
        raise AssertionError("NamedTuple rows require v2 exact-type preflight")

    monkeypatch.setattr(_native, "join_hashable_unique_records_v2", tracked)
    monkeypatch.setattr(_native, "join_hashable_unique_records_v1", v1_forbidden)

    def run(
        engine: str,
    ) -> tuple[list[list[tuple[str, object]]], list[str], type[tuple[object, ...]]]:
        events: list[str] = []

        class Name:
            def __init__(self, label: str) -> None:
                self.label = label

            def __hash__(self) -> int:
                events.append(f"hash:{self.label}")
                return 0

            def __eq__(self, other: object) -> bool:
                other_label = getattr(other, "label", type(other).__name__)
                events.append(f"eq:{self.label}:{other_label}")
                return isinstance(other, Name) and self.label == other.label

            def __format__(self, format_spec: str) -> str:
                events.append(f"format:{self.label}:{format_spec}")
                return self.label

        Row = namedtuple("MutableFieldsRow", "id payload")
        left = [Row(1, "L")]
        right = (Row(1, "R1"), Row(2, "R2"))

        def select(row: tuple[object, ...]) -> object:
            events.append(f"select:{row.payload}")  # type: ignore[attr-defined]
            if row.payload == "R1":  # type: ignore[attr-defined]
                Row._fields = (Name("id"), Name("payload"))
            return row.id  # type: ignore[attr-defined]

        result = (
            fpstreams.rows(left)
            .with_engine(engine)
            .join(right, left_on=select, right_on=select, validate="m:1")
            .to_list()
        )
        join_events = events.copy()
        normalized = [
            [(key.label if isinstance(key, Name) else key, value) for key, value in row.items()]
            for row in result
        ]
        return normalized, join_events, Row

    expected_result = [[("id", 1), ("payload", "L"), ("id", 1), ("payload", "R1")]]
    expected_events = [
        "select:R1",
        "hash:id",
        "hash:payload",
        "eq:id:payload",
        "select:R2",
        "hash:id",
        "hash:id",
        "hash:payload",
        "eq:id:payload",
        "hash:payload",
        "eq:id:payload",
        "hash:id",
        "hash:payload",
        "eq:id:payload",
        "select:L",
        "eq:id:payload",
        "hash:id",
        "hash:id",
        "format:id:",
        "hash:payload",
        "hash:payload",
        "eq:id:payload",
        "format:payload:",
        "hash:id",
        "hash:id",
        "hash:payload",
        "hash:payload",
    ]
    canonical_result, canonical_events, _canonical_type = run("python")
    automatic_result, automatic_events, automatic_type = run("auto")

    assert canonical_result == expected_result
    assert canonical_events == expected_events
    assert automatic_result == expected_result
    assert automatic_events == expected_events
    assert len(calls) == 1
    assert calls[0][4] is not _as_record
    assert calls[0][8] == (automatic_type,)


@pytest.mark.parametrize("validate", ["m:1", "m:m"])
def test_callable_namedtuple_adapter_deopts_for_live_replacement_type(validate: str) -> None:
    """A selector replacing a future row must restore canonical protocol priority."""
    from collections import namedtuple
    from collections.abc import Mapping

    def run(engine: str) -> tuple[list[dict[str, object]], list[str]]:
        events: list[str] = []
        Row = namedtuple("LiveReplacementRow", "id payload")

        class Replacement(Mapping[str, object]):
            def __init__(self, payload: str) -> None:
                self.values = {"id": 2, "payload": payload}

            def __iter__(self) -> Iterator[str]:
                events.append("replacement:iter")
                return iter(self.values)

            def __len__(self) -> int:
                return len(self.values)

            def __getitem__(self, key: str) -> object:
                events.append(f"replacement:get:{key}")
                return self.values[key]

            def _asdict(self) -> dict[str, object]:
                events.append("replacement:_asdict")
                return {"id": 2, "wrong": "FAST-WRONG"}

        right = [Row(1, "R1"), Row(2, "R2")]
        if validate == "m:m":
            right.append(Row(2, "R3"))

        def select(row: object) -> object:
            if isinstance(row, Mapping):
                return row["id"]
            if row.id == 1:  # type: ignore[attr-defined]
                for index in range(1, len(right)):
                    right[index] = Replacement(f"R{index + 1}")  # type: ignore[list-item]
            return row.id  # type: ignore[attr-defined]

        result = (
            fpstreams.rows([Row(2, "L")])
            .with_engine(engine)
            .join(right, left_on=select, right_on=select, validate=validate)
            .to_list()
        )
        return result, events

    canonical, canonical_events = run("python")
    automatic, automatic_events = run("auto")

    assert automatic == canonical
    assert automatic_events == canonical_events
    assert "replacement:_asdict" not in automatic_events


def test_standard_namedtuple_adapter_deopts_when_protocol_priority_changes() -> None:
    """ABC registration and dataclass markers return the fast adapter to canonical conversion."""
    from collections import namedtuple
    from collections.abc import Callable, Mapping

    from fpstreams.execution.relational import _make_standard_namedtuple_record_adapter
    from fpstreams.tabular.records import _as_record

    Row = namedtuple("ProtocolPriorityRow", "id payload")
    row = Row(1, "value")
    adapter = _make_standard_namedtuple_record_adapter((Row,))
    assert adapter is not None
    assert adapter(row) == _as_record(row) == {"id": 1, "payload": "value"}

    Row.__dataclass_fields__ = {}
    try:
        assert adapter(row) == _as_record(row) == {}
    finally:
        del Row.__dataclass_fields__

    adapter = _make_standard_namedtuple_record_adapter((Row,))
    assert adapter is not None
    Mapping.register(Row)

    def outcome(function: Callable[[object], object]) -> tuple[type[BaseException], str, object]:
        try:
            function(row)
        except BaseException as error:
            return type(error), str(error), error.__cause__
        raise AssertionError("the virtually registered tuple must fail canonical dict conversion")

    assert outcome(adapter) == outcome(_as_record)
    assert _make_standard_namedtuple_record_adapter((Row,)) is None


def test_direct_field_namedtuple_join_stays_on_attribute_selection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Direct ``on=`` must not reuse the Mapping-only generated subscription selector."""
    from typing import NamedTuple

    from fpstreams import _native

    class Left(NamedTuple):
        id: int
        left: str

    class Right(NamedTuple):
        id: int
        right: str

    def forbidden(*_arguments: object) -> None:
        raise AssertionError("direct NamedTuple joins must stay outside callable native ABIs")

    for version in (1, 2):
        monkeypatch.setattr(_native, f"join_hashable_unique_records_v{version}", forbidden)

    result = (
        fpstreams.rows([Left(1, "L1"), Left(2, "L2")])
        .join((Right(1, "R1"),), on="id", how="left", validate="m:1")
        .to_list()
    )

    assert result == [
        {"id": 1, "left": "L1", "right": "R1"},
        {"id": 2, "left": "L2", "right": None},
    ]


@pytest.mark.parametrize(
    ("validate", "kernel_name", "right_values"),
    [
        ("m:1", "join_hashable_unique_records_v2", ((1, "R1"),)),
        ("m:m", "join_hashable_many_records_v2", ((1, "R1"), (1, "R2"))),
    ],
)
def test_callable_namedtuple_join_uses_v2_with_guarded_record_adapter(
    monkeypatch: pytest.MonkeyPatch,
    validate: str,
    kernel_name: str,
    right_values: tuple[tuple[int, str], ...],
) -> None:
    """Exact NamedTuple sources use v2 through a live, protocol-invalidating record adapter."""
    from typing import NamedTuple

    from fpstreams import _native
    from fpstreams.tabular.records import _as_record

    class Left(NamedTuple):
        id: int
        left: str

    class Right(NamedTuple):
        id: int
        right: str

    left = [Left(1, "L1"), Left(2, "L2")]
    right = tuple(Right(*values) for values in right_values)

    def left_key(row: Left) -> int:
        return row.id

    def right_key(row: Right) -> int:
        return row.id

    canonical = (
        fpstreams.rows(left)
        .with_engine("python")
        .join(
            right,
            left_on=left_key,
            right_on=right_key,
            how="left",
            validate=validate,
        )
        .to_list()
    )
    native = getattr(_native, kernel_name)
    adapter_factory = _native.standard_namedtuple_record_adapter_v1
    calls: list[tuple[object, ...]] = []
    adapters: list[object] = []

    def tracked(*arguments: object) -> object:
        calls.append(arguments)
        return native(*arguments)

    def tracked_factory(*arguments: object) -> object:
        adapter = adapter_factory(*arguments)
        adapters.append(adapter)
        return adapter

    def v1_forbidden(*_arguments: object) -> None:
        raise AssertionError("NamedTuple rows require v2 exact-type preflight")

    monkeypatch.setattr(_native, kernel_name, tracked)
    monkeypatch.setattr(
        _native,
        "standard_namedtuple_record_adapter_v1",
        tracked_factory,
    )
    monkeypatch.setattr(
        _native,
        kernel_name.replace("_v2", "_v1"),
        v1_forbidden,
    )

    automatic = (
        fpstreams.rows(left)
        .join(
            right,
            left_on=left_key,
            right_on=right_key,
            how="left",
            validate=validate,
        )
        .to_list()
    )

    assert automatic == canonical
    assert len(calls) == 1
    assert len(adapters) == 1
    if adapters[0] is not None:
        assert calls[0][4] is adapters[0]
    assert calls[0][4] is not _as_record
    assert calls[0][8] == (Left, Right)


def test_namedtuple_snapshot_factory_receives_closed_guard_capabilities(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Python supplies exact types, fallback, ABC guard, and canonical `_asdict` code once."""
    from collections import namedtuple
    from collections.abc import Mapping
    from types import CodeType
    from typing import NamedTuple

    from fpstreams import _native
    from fpstreams.execution import relational

    class Left(NamedTuple):
        id: int
        left: str

    class Right(NamedTuple):
        id: int
        right: str

    left = [Left(1, "L")]
    right = (Right(1, "R"),)
    expected = [{"native": True}]
    factory_calls: list[tuple[object, ...]] = []
    kernel_calls: list[tuple[object, ...]] = []

    def sentinel_adapter(_row: object) -> dict[str, object]:
        raise AssertionError("the mocked kernel must not invoke its adapter")

    def factory(*arguments: object) -> object:
        factory_calls.append(arguments)
        return sentinel_adapter

    def kernel(*arguments: object) -> object:
        kernel_calls.append(arguments)
        return expected

    monkeypatch.setattr(
        _native,
        "standard_namedtuple_record_adapter_v1",
        factory,
        raising=False,
    )
    monkeypatch.setattr(_native, "join_hashable_unique_records_v2", kernel)

    def select(row: object) -> object:
        return row.id  # type: ignore[attr-defined]

    result = (
        fpstreams.rows(left).join(right, left_on=select, right_on=select, validate="m:1").to_list()
    )

    assert result is expected
    assert len(factory_calls) == len(kernel_calls) == 1
    (
        record_types,
        fallback,
        get_token,
        token,
        canonical_factory,
        code_type,
        mapping_abc,
        record_continuations,
        record_globals,
    ) = factory_calls[0]
    assert record_types == (Left, Right)
    assert callable(fallback)
    assert callable(get_token)
    assert get_token() == token
    assert canonical_factory is namedtuple
    assert code_type is CodeType
    assert mapping_abc is Mapping
    assert record_continuations is relational._CANONICAL_RECORD_CONTINUATIONS
    assert record_globals is relational._RECORD_GLOBALS
    assert kernel_calls[0][4] is sentinel_adapter


def test_namedtuple_snapshot_emits_no_dynamic_code_or_function_code_audit_events() -> None:
    """Adapter admission and row guards avoid observable code compilation/introspection."""
    import sys
    from typing import NamedTuple

    from fpstreams.execution import relational as _relational  # noqa: F401

    class Left(NamedTuple):
        id: int
        left: str

    class Right(NamedTuple):
        id: int
        right: str

    def select(row: object) -> object:
        return row.id  # type: ignore[attr-defined]

    observed: list[tuple[str, tuple[object, ...]]] = []
    active = False

    def audit(event: str, arguments: tuple[object, ...]) -> None:
        sensitive_code = (
            event == "object.__getattr__" and len(arguments) == 2 and arguments[1] == "__code__"
        )
        if active and (event in {"compile", "exec"} or sensitive_code):
            observed.append((event, arguments))
            raise RuntimeError(f"blocked audited event: {event}")

    sys.addaudithook(audit)
    active = True
    try:
        result = (
            fpstreams.rows([Left(1, "L")])
            .join((Right(1, "R"),), left_on=select, right_on=select, validate="m:1")
            .to_list()
        )
    finally:
        active = False

    assert result == [{"id": 1, "left": "L", "id_right": 1, "right": "R"}]
    assert observed == []


def _require_native_namedtuple_snapshot(adapter: object, fallback: object) -> None:
    if adapter is not fallback:
        return
    import sys

    is_gil_enabled = getattr(sys, "_is_gil_enabled", None)
    if callable(is_gil_enabled) and not is_gil_enabled():
        pytest.skip("native NamedTuple snapshots are unavailable on free-threaded CPython")
    pytest.fail("native NamedTuple snapshot capability unexpectedly fell back to Python")


def test_standard_namedtuple_adapter_stays_disabled_without_the_gil() -> None:
    """The guarded native adapter must decline on free-threaded CPython."""
    import sys

    if getattr(sys, "_is_gil_enabled", lambda: True)():
        pytest.skip("requires a free-threaded interpreter")

    from fpstreams import _native

    assert _native.standard_namedtuple_record_adapter_v1(*(None,) * 9) is None


def test_namedtuple_snapshot_keeps_native_capability_opaque() -> None:
    """The exported native factory does not expose a writable type namespace through __self__."""
    from typing import NamedTuple

    from fpstreams import _native
    from fpstreams.execution import relational

    class Row(NamedTuple):
        id: int
        value: str

    fallback = relational._make_standard_namedtuple_record_adapter((Row,))
    assert fallback is not None
    adapter = relational._native_standard_namedtuple_record_adapter(
        _native,
        (Row,),
        fallback,
    )

    _require_native_namedtuple_snapshot(adapter, fallback)
    state = adapter.__self__  # type: ignore[attr-defined]
    assert type(state).__name__ == "_StandardNamedTupleSnapshotState"
    assert not hasattr(state, "__dict__")
    with pytest.raises(TypeError):
        state[0]  # type: ignore[index]


def test_namedtuple_snapshot_state_exposes_cycles_to_python_gc() -> None:
    """Adapter/type/fallback back-references are collected through the private native state."""
    import gc
    import weakref
    from typing import NamedTuple

    from fpstreams import _native
    from fpstreams.execution import relational

    class Row(NamedTuple):
        id: int
        value: str

    fallback = relational._make_standard_namedtuple_record_adapter((Row,))
    assert fallback is not None
    adapter = relational._native_standard_namedtuple_record_adapter(
        _native,
        (Row,),
        fallback,
    )
    _require_native_namedtuple_snapshot(adapter, fallback)
    state = adapter.__self__  # type: ignore[attr-defined]
    assert gc.is_tracked(adapter)
    assert gc.is_tracked(state)
    Row.native_adapter = adapter  # type: ignore[attr-defined]
    fallback.peer = adapter  # type: ignore[attr-defined]
    row_reference = weakref.ref(Row)
    fallback_reference = weakref.ref(fallback)

    del state, adapter, fallback, Row
    gc.collect()

    assert row_reference() is None
    assert fallback_reference() is None


def test_namedtuple_adapters_follow_mapping_subclasshook_without_abc_token_change() -> None:
    """Clearing ABC caches can change Mapping priority without advancing the global token."""
    from abc import get_cache_token
    from collections.abc import Callable, Mapping
    from typing import NamedTuple

    from fpstreams import _native
    from fpstreams.execution import relational
    from fpstreams.tabular.records import _as_record

    class Row(NamedTuple):
        id: int
        value: int

    row = Row(1, 2)
    fallback = relational._make_standard_namedtuple_record_adapter((Row,))
    assert fallback is not None
    adapter = relational._native_standard_namedtuple_record_adapter(
        _native,
        (Row,),
        fallback,
    )
    original = Mapping.__dict__.get("__subclasshook__")
    token = get_cache_token()

    def subclasshook(cls: type[object], candidate: type[object]) -> object:
        del cls
        return True if candidate is Row else NotImplemented

    Mapping.__subclasshook__ = classmethod(subclasshook)  # type: ignore[method-assign]
    Mapping._abc_caches_clear()
    try:
        assert get_cache_token() == token

        def outcome(function: Callable[[object], object]) -> tuple[type[BaseException], str]:
            Mapping._abc_caches_clear()
            try:
                function(row)
            except BaseException as error:
                return type(error), str(error)
            raise AssertionError("a dynamically recognized Mapping must use dict(row)")

        expected = outcome(_as_record)
        assert outcome(fallback) == expected
        assert outcome(adapter) == expected
    finally:
        if original is None:
            del Mapping.__subclasshook__
        else:
            Mapping.__subclasshook__ = original  # type: ignore[method-assign]
        Mapping._abc_caches_clear()


def test_namedtuple_snapshot_rechecks_asdict_closure_after_mapping_subclasshook() -> None:
    """A false Mapping hook may mutate `_asdict` closure before canonical conversion."""
    from collections.abc import Mapping
    from typing import NamedTuple

    from fpstreams import _native
    from fpstreams.execution import relational
    from fpstreams.tabular.records import _as_record

    original = Mapping.__dict__.get("__subclasshook__")

    def run(native: bool) -> tuple[dict[str, object], list[str]]:
        class Row(NamedTuple):
            id: int
            value: int

        row = Row(1, 2)
        fallback = relational._make_standard_namedtuple_record_adapter((Row,))
        assert fallback is not None
        adapter = relational._native_standard_namedtuple_record_adapter(
            _native,
            (Row,),
            fallback,
        )
        if native:
            _require_native_namedtuple_snapshot(adapter, fallback)
        events: list[str] = []
        asdict = vars(Row)["_asdict"]
        freevars = asdict.__code__.co_freevars
        dict_cell = asdict.__closure__[freevars.index("_dict")]

        def replacement(items: Iterator[tuple[str, object]]) -> dict[str, object]:
            events.append("asdict")
            values = dict(items)
            return {"changed": values["id"] + values["value"]}  # type: ignore[operator]

        def subclasshook(cls: type[object], candidate: type[object]) -> object:
            del cls
            events.append("mapping")
            if candidate is Row:
                dict_cell.cell_contents = replacement
                return False
            return NotImplemented

        Mapping.__subclasshook__ = classmethod(subclasshook)  # type: ignore[method-assign]
        Mapping._abc_caches_clear()
        try:
            result = (adapter if native else _as_record)(row)
        finally:
            dict_cell.cell_contents = dict
            if original is None:
                del Mapping.__subclasshook__
            else:
                Mapping.__subclasshook__ = original  # type: ignore[method-assign]
            Mapping._abc_caches_clear()
        return result, events

    expected, expected_events = run(False)
    actual, actual_events = run(True)

    assert expected == actual == {"changed": 3}
    assert expected_events == actual_events == ["mapping", "asdict"]


def test_namedtuple_snapshot_mapping_hook_shares_prefetched_converter() -> None:
    """A true Mapping hook mutating converter code is seen by canonical and native paths."""
    from collections.abc import Callable, Mapping
    from typing import NamedTuple

    from fpstreams import _native
    from fpstreams.execution import relational
    from fpstreams.tabular.records import _as_record

    original_hook = Mapping.__dict__.get("__subclasshook__")
    continuations = relational._RECORD_GLOBALS["_RECORD_CONTINUATIONS"]
    mapping_record = continuations[0]
    original_code = mapping_record.__code__

    def run(native: bool) -> tuple[object, list[str]]:
        class Row(NamedTuple):
            id: int
            value: int

        row = Row(1, 2)
        fallback = relational._make_standard_namedtuple_record_adapter((Row,))
        assert fallback is not None
        adapter = relational._native_standard_namedtuple_record_adapter(
            _native,
            (Row,),
            fallback,
        )
        if native:
            _require_native_namedtuple_snapshot(adapter, fallback)
        events: list[str] = []

        def replacement(_row: object) -> dict[str, object]:
            return {"wrong": True}

        def subclasshook(cls: type[object], candidate: type[object]) -> object:
            del cls
            events.append("mapping")
            if candidate is Row:
                mapping_record.__code__ = replacement.__code__
                return True
            return NotImplemented

        Mapping.__subclasshook__ = classmethod(subclasshook)  # type: ignore[method-assign]
        Mapping._abc_caches_clear()
        try:
            function: Callable[[object], object] = adapter if native else _as_record
            try:
                outcome: object = function(row)
            except BaseException as error:
                outcome = (type(error), str(error))
        finally:
            mapping_record.__code__ = original_code
            if original_hook is None:
                del Mapping.__subclasshook__
            else:
                Mapping.__subclasshook__ = original_hook  # type: ignore[method-assign]
            Mapping._abc_caches_clear()
        return outcome, events

    expected, expected_events = run(False)
    actual, actual_events = run(True)

    assert actual == expected == {"wrong": True}
    assert expected_events == actual_events == ["mapping"]


@pytest.mark.parametrize("mapping_result", [False, True])
def test_namedtuple_snapshot_mapping_hook_rebind_uses_prefetched_continuations(
    mapping_result: bool,
) -> None:
    """A hook rebind affects the next row, not the current canonical/native conversion."""
    from collections.abc import Callable, Mapping
    from typing import NamedTuple

    from fpstreams import _native
    from fpstreams.execution import relational
    from fpstreams.tabular.records import _as_record

    original_hook = Mapping.__dict__.get("__subclasshook__")
    original_continuations = relational._RECORD_GLOBALS["_RECORD_CONTINUATIONS"]

    def run(native: bool) -> tuple[object, list[str]]:
        class Row(NamedTuple):
            id: int
            value: int

        row = Row(1, 2)
        fallback = relational._make_standard_namedtuple_record_adapter((Row,))
        assert fallback is not None
        adapter = relational._native_standard_namedtuple_record_adapter(
            _native,
            (Row,),
            fallback,
        )
        if native:
            _require_native_namedtuple_snapshot(adapter, fallback)
        events: list[str] = []

        def replacement_mapping(_row: object) -> dict[str, object]:
            events.append("replacement:mapping")
            return {"wrong": "mapping"}

        def replacement_after_mapping(_row: object) -> dict[str, object]:
            events.append("replacement:after")
            return {"wrong": "after"}

        replacement_continuations = (replacement_mapping, replacement_after_mapping)

        def subclasshook(cls: type[object], candidate: type[object]) -> object:
            del cls
            events.append("mapping")
            if candidate is Row:
                relational._RECORD_GLOBALS["_RECORD_CONTINUATIONS"] = replacement_continuations
                return mapping_result
            return NotImplemented

        Mapping.__subclasshook__ = classmethod(subclasshook)  # type: ignore[method-assign]
        Mapping._abc_caches_clear()
        try:
            function: Callable[[object], object] = adapter if native else _as_record
            try:
                outcome: object = function(row)
            except BaseException as error:
                outcome = (type(error), str(error))
        finally:
            relational._RECORD_GLOBALS["_RECORD_CONTINUATIONS"] = original_continuations
            if original_hook is None:
                del Mapping.__subclasshook__
            else:
                Mapping.__subclasshook__ = original_hook  # type: ignore[method-assign]
            Mapping._abc_caches_clear()
        return outcome, events

    expected, expected_events = run(False)
    actual, actual_events = run(True)

    assert actual == expected
    assert expected_events == actual_events == ["mapping"]


def test_namedtuple_snapshot_empty_closure_cell_uses_canonical_name_error() -> None:
    """An emptied stdlib closure declines before fallback executes the live `_asdict`."""
    from collections.abc import Callable
    from typing import NamedTuple

    from fpstreams import _native
    from fpstreams.execution import relational
    from fpstreams.tabular.records import _as_record

    class Row(NamedTuple):
        id: int
        value: int

    row = Row(1, 2)
    fallback = relational._make_standard_namedtuple_record_adapter((Row,))
    assert fallback is not None
    adapter = relational._native_standard_namedtuple_record_adapter(
        _native,
        (Row,),
        fallback,
    )
    asdict = vars(Row)["_asdict"]
    freevars = asdict.__code__.co_freevars
    cell = asdict.__closure__[freevars.index("_dict")]
    del cell.cell_contents
    try:

        def outcome(function: Callable[[object], object]) -> tuple[type[BaseException], str]:
            try:
                function(row)
            except BaseException as error:
                return type(error), str(error)
            raise AssertionError("an empty _dict cell must fail")

        expected = outcome(_as_record)
        assert expected[0] is NameError
        assert outcome(fallback) == expected
        assert outcome(adapter) == expected
    finally:
        cell.cell_contents = dict


def test_callable_many_join_v2_unlisted_type_declines_before_callbacks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A later unlisted Mapping type makes v2 fall back before selector ownership."""
    from collections.abc import Mapping

    from fpstreams import _native

    class Record(Mapping[str, object]):
        def __init__(self, values: dict[str, object]) -> None:
            self.values = values

        def __getitem__(self, name: str) -> object:
            return self.values[name]

        def __iter__(self) -> Iterator[str]:
            return iter(self.values)

        def __len__(self) -> int:
            return len(self.values)

    class Unlisted(Record):
        pass

    events: list[str] = []
    native_calls = 0
    native_v2 = _native.join_hashable_many_records_v2

    def select(row: Mapping[str, object]) -> object:
        events.append(f"select:{row['id']}")
        return row["id"]

    def tracked_v2(*arguments: object) -> object:
        nonlocal native_calls
        native_calls += 1
        return native_v2(*arguments)

    monkeypatch.setattr(_native, "join_hashable_many_records_v2", tracked_v2)
    result = (
        fpstreams.rows([Record({"id": 1, "left": True})])
        .join(
            (
                Record({"id": 1, "right": "first"}),
                Unlisted({"id": 1, "right": "second"}),
            ),
            left_on=select,
            right_on=select,
            validate="m:m",
        )
        .to_list()
    )

    assert [row["right"] for row in result] == ["first", "second"]
    assert native_calls == 1
    assert events == ["select:1", "select:1", "select:1"]


def test_callable_many_join_native_decline_runs_canonical_selectors_once(monkeypatch) -> None:
    """Only a pre-callback None may fall back, and it never retries through unique ABI."""
    from fpstreams import _native

    events: list[str] = []
    native_calls = 0

    def left_key(row: dict[str, int]) -> int:
        events.append(f"left:{row['id']}")
        return row["id"]

    def right_key(row: dict[str, int]) -> int:
        events.append(f"right:{row['id']}")
        return row["id"]

    def decline(*_arguments: object) -> None:
        nonlocal native_calls
        native_calls += 1
        return None

    def forbidden(*_arguments: object) -> None:
        raise AssertionError("a many decline must not probe the unique callable ABI")

    monkeypatch.setattr(_native, "join_hashable_many_records_v1", decline)
    monkeypatch.setattr(_native, "join_hashable_unique_records_v1", forbidden)
    result = (
        fpstreams.rows([{"id": 1, "left": 10}])
        .join(
            ({"id": 1, "right": 20}, {"id": 1, "right": 30}),
            left_on=left_key,
            right_on=right_key,
            validate="m:m",
        )
        .to_list()
    )

    assert [row["right"] for row in result] == [20, 30]
    assert native_calls == 1
    assert events == ["right:1", "right:1", "left:1"]


def test_callable_many_join_native_error_propagates_without_opening_sources(monkeypatch) -> None:
    """Once the chosen ABI raises, execution must not replay either source in Python."""
    from fpstreams import _native
    from fpstreams.planning.source import Source

    failure = RuntimeError("many callback failure")

    def select(row: dict[str, int]) -> int:
        return row["id"]

    def explode(*_arguments: object) -> None:
        raise failure

    def opened(_source: Source[object]) -> object:
        raise AssertionError("a native error must not open canonical Python sources")

    monkeypatch.setattr(_native, "join_hashable_many_records_v1", explode)
    monkeypatch.setattr(Source, "open", opened)
    with pytest.raises(RuntimeError, match="many callback failure") as captured:
        (
            fpstreams.rows([{"id": 1}])
            .join(
                ({"id": 1}, {"id": 1}),
                left_on=select,
                right_on=select,
                validate="m:m",
            )
            .to_list()
        )
    assert captured.value is failure


def test_callable_many_join_native_preserves_callbacks_snapshots_and_output_order() -> None:
    """The real ABI keeps right-first callbacks and left-major, right-stable snapshots."""
    events: list[str] = []
    right = [
        {"right_id": 1, "right": "R1"},
        {"right_id": 1, "right": "R2"},
        {"right_id": 2, "tail": "T2"},
    ]
    left = [
        {"left_id": 1, "left": "L1"},
        {"left_id": 1, "left": "L2"},
        {"left_id": 3, "left": "L3"},
    ]

    def right_key(row: dict[str, object]) -> object:
        events.append(f"right:{row['right_id']}")
        key = row["right_id"]
        row["right"] = "mutated-right"
        return key

    def left_key(row: dict[str, object]) -> object:
        events.append(f"left:{row['left_id']}")
        key = row["left_id"]
        row["left"] = "mutated-left"
        return key

    result = (
        fpstreams.rows(left)
        .join(
            right,
            left_on=left_key,
            right_on=right_key,
            how="left",
            validate="m:m",
        )
        .to_list()
    )

    assert events == ["right:1", "right:1", "right:2", "left:1", "left:1", "left:3"]
    assert [(row["left"], row["right"]) for row in result[:4]] == [
        ("L1", "R1"),
        ("L1", "R2"),
        ("L2", "R1"),
        ("L2", "R2"),
    ]
    assert result[4] == {
        "left_id": 3,
        "left": "L3",
        "right_id": None,
        "right": None,
        "tail": None,
    }
    assert result[0] is not result[1]
    result[0]["left"] = "changed-output"
    assert result[1]["left"] == "L1"


def test_callable_many_join_native_matches_python_hash_and_equality_trace() -> None:
    """The private key-to-code index preserves canonical get/set/probe dispatch."""

    def run(engine: str) -> tuple[list[str], list[dict[str, object]]]:
        events: list[str] = []

        class Key:
            def __init__(self, label: str) -> None:
                self.label = label

            def __hash__(self) -> int:
                events.append(f"hash:{self.label}")
                return 0

            def __eq__(self, other: object) -> bool:
                assert isinstance(other, Key)
                events.append(f"eq:{self.label}:{other.label}")
                return True

        right = [
            {"key": Key("right-1"), "right": 1},
            {"key": Key("right-2"), "right": 2},
        ]
        left = [{"key": Key("left"), "left": True}]

        def select(row: dict[str, object]) -> object:
            key = row["key"]
            assert isinstance(key, Key)
            events.append(f"select:{key.label}")
            return key

        result = (
            fpstreams.rows(left)
            .with_engine(engine)
            .join(right, left_on=select, right_on=select, validate="m:m")
            .to_list()
        )
        return events, result

    python_events, python_result = run("python")
    auto_events, auto_result = run("auto")

    assert auto_events == python_events
    assert [row["right"] for row in auto_result] == [1, 2]
    assert [row["right"] for row in python_result] == [1, 2]


def test_callable_unique_join_to_list_runs_the_native_kernel(monkeypatch) -> None:
    """A successful callback ABI result bypasses both canonical source iterators."""
    from fpstreams import _native
    from fpstreams.planning.source import Source
    from fpstreams.tabular.records import _as_record

    left = [{"left_id": 1, "left": "a"}]
    right = ({"right_id": 1, "right": "A"},)
    expected = [{"native": True}]
    calls: list[tuple[object, ...]] = []

    def left_key(row: dict[str, object]) -> object:
        return row["left_id"]

    def right_key(row: dict[str, object]) -> object:
        return row["right_id"]

    def native_result(*arguments: object) -> list[dict[str, bool]]:
        calls.append(arguments)
        return expected

    def opened(_source: Source[object]) -> object:
        raise AssertionError("a successful callable join ABI must not open Python sources")

    monkeypatch.setattr(Source, "open", opened)
    monkeypatch.setattr(
        _native,
        "join_hashable_unique_records_v1",
        native_result,
        raising=False,
    )

    def unexpected_v2(*_arguments: object) -> None:
        raise AssertionError("all-exact-dict joins must retain the v1 ABI")

    monkeypatch.setattr(
        _native,
        "join_hashable_unique_records_v2",
        unexpected_v2,
        raising=False,
    )

    result = (
        fpstreams.rows(left)
        .join(
            right,
            left_on=left_key,
            right_on=right_key,
            how="left",
            suffix="_lookup",
            validate="m:1",
        )
        .to_list()
    )

    assert result is expected
    assert len(calls) == 1
    arguments = calls[0]
    assert arguments[:4] == (left, right, left_key, right_key)
    assert arguments[4] == (dict, _as_record)
    assert arguments[5:] == (True, "_lookup", frozenset())


def test_callable_unique_join_mappingproxy_uses_v2_exact_type_token(monkeypatch) -> None:
    """Initial mappingproxy rows use v2 without broad Mapping protocol admission."""
    from types import MappingProxyType

    from fpstreams import _native
    from fpstreams.tabular.records import _as_record

    left = [MappingProxyType({"left_id": 1, "left": "a"})]
    right = (MappingProxyType({"right_id": 1, "right": "A"}),)
    expected = [{"native_v2": True}]
    calls: list[tuple[object, ...]] = []

    def left_key(row: object) -> object:
        return row["left_id"]  # type: ignore[index]

    def right_key(row: object) -> object:
        return row["right_id"]  # type: ignore[index]

    def unexpected_v1(*_arguments: object) -> None:
        raise AssertionError("mapping rows must not be sent to the exact-dict v1 ABI")

    def native_v2(*arguments: object) -> list[dict[str, bool]]:
        calls.append(arguments)
        return expected

    monkeypatch.setattr(_native, "join_hashable_unique_records_v1", unexpected_v1)
    monkeypatch.setattr(
        _native,
        "join_hashable_unique_records_v2",
        native_v2,
        raising=False,
    )

    result = (
        fpstreams.rows(left)
        .join(
            right,
            left_on=left_key,
            right_on=right_key,
            validate="m:1",
        )
        .to_list()
    )

    assert result is expected
    assert len(calls) == 1
    assert calls[0][:8] == (
        left,
        right,
        left_key,
        right_key,
        calls[0][4],
        False,
        "_right",
        frozenset(),
    )
    assert calls[0][4] == (dict, _as_record)
    assert calls[0][8] == (MappingProxyType,)


def test_callable_unique_join_nominal_mapping_uses_observed_v2_mro_capability(monkeypatch) -> None:
    """A nominal Mapping delegates both its exact type and retained MRO to v2."""
    from collections.abc import Mapping

    from fpstreams import _native
    from fpstreams.tabular.records import _as_record

    class Record(Mapping[str, object]):
        def __init__(self, values: dict[str, object]) -> None:
            self.values = values

        def __getitem__(self, name: str) -> object:
            return self.values[name]

        def __iter__(self) -> Iterator[str]:
            return iter(self.values)

        def __len__(self) -> int:
            return len(self.values)

    left = [Record({"id": 1, "left": "a"})]
    right = (Record({"id": 1, "right": "A"}),)
    expected = [{"native_v2": True}]
    calls: list[tuple[object, ...]] = []

    def select(row: Mapping[str, object]) -> object:
        return row["id"]

    def unexpected_v1(*_arguments: object) -> None:
        raise AssertionError("nominal Mapping rows must not be sent to v1")

    def native_v2(*arguments: object) -> list[dict[str, bool]]:
        calls.append(arguments)
        return expected

    monkeypatch.setattr(_native, "join_hashable_unique_records_v1", unexpected_v1)
    monkeypatch.setattr(
        _native,
        "join_hashable_unique_records_v2",
        native_v2,
        raising=False,
    )

    result = (
        fpstreams.rows(left)
        .join(
            right,
            left_on=select,
            right_on=select,
            validate="m:1",
        )
        .to_list()
    )

    assert result is expected
    assert len(calls) == 1
    assert calls[0][4] == (dict, _as_record)
    assert calls[0][8] == ((Record, Record.__mro__),)


def test_callable_unique_join_old_native_falls_back_for_mapping_without_calling_v1(
    monkeypatch,
) -> None:
    """A wheel without v2 leaves Mapping rows to Python while exact-dict v1 stays untouched."""
    from types import MappingProxyType

    from fpstreams import _native

    events: list[str] = []

    def select(row: object) -> object:
        value = row["id"]  # type: ignore[index]
        events.append(f"select:{value}")
        return value

    def unexpected_v1(*_arguments: object) -> None:
        raise AssertionError("an old wheel's v1 must not receive Mapping rows")

    monkeypatch.delattr(_native, "join_hashable_unique_records_v2", raising=False)
    monkeypatch.setattr(_native, "join_hashable_unique_records_v1", unexpected_v1)

    result = (
        fpstreams.rows([MappingProxyType({"id": 1, "left": True})])
        .join(
            (MappingProxyType({"id": 1, "right": True}),),
            left_on=select,
            right_on=select,
            validate="m:1",
        )
        .to_list()
    )

    assert result == [{"id": 1, "left": True, "id_right": 1, "right": True}]
    assert events == ["select:1", "select:1"]


def test_callable_unique_join_v2_unlisted_later_type_falls_back_without_selector_replay(
    monkeypatch,
) -> None:
    """A heterogeneous type missed by the head token declines before selector ownership."""
    from collections.abc import Mapping

    from fpstreams import _native

    class Record(Mapping[str, object]):
        def __init__(self, values: dict[str, object]) -> None:
            self.values = values

        def __getitem__(self, name: str) -> object:
            return self.values[name]

        def __iter__(self) -> Iterator[str]:
            return iter(self.values)

        def __len__(self) -> int:
            return len(self.values)

    class Unlisted(Record):
        pass

    events: list[str] = []
    native_calls = 0
    native_v2 = _native.join_hashable_unique_records_v2

    def select(row: Mapping[str, object]) -> object:
        value = row["id"]
        events.append(f"select:{value}")
        return value

    def tracked_v2(*arguments: object) -> object:
        nonlocal native_calls
        native_calls += 1
        return native_v2(*arguments)

    monkeypatch.setattr(_native, "join_hashable_unique_records_v2", tracked_v2)

    result = (
        fpstreams.rows([Record({"id": 1, "left": True})])
        .join(
            (
                Record({"id": 1, "right": True}),
                Unlisted({"id": 2, "right": False}),
            ),
            left_on=select,
            right_on=select,
            validate="m:1",
        )
        .to_list()
    )

    assert result == [{"id": 1, "left": True, "id_right": 1, "right": True}]
    assert native_calls == 1
    assert events == ["select:1", "select:2", "select:1"]


def test_callable_unique_join_native_decline_invokes_each_selector_only_once(monkeypatch) -> None:
    """A pre-callback ABI decline may replay sources but must not replay selector effects."""
    from fpstreams import _native

    events: list[str] = []
    native_calls = 0

    def left_key(row: dict[str, int]) -> int:
        events.append(f"left:{row['left_id']}")
        return row["left_id"]

    def right_key(row: dict[str, int]) -> int:
        events.append(f"right:{row['right_id']}")
        return row["right_id"]

    def decline(*_arguments: object) -> None:
        nonlocal native_calls
        native_calls += 1
        return None

    monkeypatch.setattr(
        _native,
        "join_hashable_unique_records_v1",
        decline,
        raising=False,
    )
    result = (
        fpstreams.rows([{"left_id": 1}])
        .join(
            ({"right_id": 1},),
            left_on=left_key,
            right_on=right_key,
            validate="m:1",
        )
        .to_list()
    )

    assert result == [{"left_id": 1, "right_id": 1}]
    assert native_calls == 1
    assert events == ["right:1", "left:1"]


def test_callable_unique_join_native_error_propagates_without_fallback(monkeypatch) -> None:
    """An ABI error after callback ownership transfers cannot reopen canonical sources."""
    from fpstreams import _native
    from fpstreams.planning.source import Source

    error = RuntimeError("callable join failed")

    def key(row: dict[str, int]) -> int:
        return row["id"]

    def fail(*_arguments: object) -> None:
        raise error

    def opened(_source: Source[object]) -> object:
        raise AssertionError("a callable native error must not enter source fallback")

    monkeypatch.setattr(Source, "open", opened)
    monkeypatch.setattr(
        _native,
        "join_hashable_unique_records_v1",
        fail,
        raising=False,
    )
    joined = fpstreams.rows([{"id": 1}]).join(
        ({"id": 1},),
        left_on=key,
        right_on=key,
        validate="m:1",
    )

    with pytest.raises(RuntimeError) as captured:
        joined.to_list()
    assert captured.value is error


def test_callable_unique_join_real_kernel_preserves_order_and_snapshots(monkeypatch) -> None:
    """The compiled callback ABI owns the whole join without changing observable ordering."""
    from fpstreams import _native
    from fpstreams.planning.source import Source

    left = [{"id": 1, "left": "L1"}, {"id": 3, "left": "L3"}]
    right = ({"id": 1, "right": "R1"}, {"id": 2, "right": "R2"})
    events: list[str] = []
    native_calls = 0

    def right_key(row: dict[str, object]) -> object:
        key = row["id"]
        events.append(f"right:{key}")
        row["right"] = f"{row['right']}!"
        return key

    def left_key(row: dict[str, object]) -> object:
        key = row["id"]
        events.append(f"left:{key}")
        row["left"] = f"{row['left']}!"
        return key

    native_kernel = _native.join_hashable_unique_records_v1

    def tracked_kernel(*arguments: object) -> object:
        nonlocal native_calls
        native_calls += 1
        return native_kernel(*arguments)

    def opened(_source: Source[object]) -> object:
        raise AssertionError("the real callable join kernel must not reopen Python sources")

    monkeypatch.setattr(Source, "open", opened)
    monkeypatch.setattr(_native, "join_hashable_unique_records_v1", tracked_kernel)

    result = (
        fpstreams.rows(left)
        .join(
            right,
            left_on=left_key,
            right_on=right_key,
            how="left",
            validate="m:1",
        )
        .to_list()
    )

    assert native_calls == 1
    assert events == ["right:1", "right:2", "left:1", "left:3"]
    assert result == [
        {"id": 1, "left": "L1", "id_right": 1, "right": "R1"},
        {"id": 3, "left": "L3", "id_right": None, "right": None},
    ]
    assert left == [{"id": 1, "left": "L1!"}, {"id": 3, "left": "L3!"}]
    assert right == ({"id": 1, "right": "R1!"}, {"id": 2, "right": "R2!"})


def test_callable_unique_join_real_kernel_matches_python_hash_protocol(monkeypatch) -> None:
    """Native indexing must preserve Python dict hash/equality calls exactly."""
    from fpstreams import _native

    native_kernel = _native.join_hashable_unique_records_v1
    native_calls = 0

    def tracked_kernel(*arguments: object) -> object:
        nonlocal native_calls
        native_calls += 1
        return native_kernel(*arguments)

    monkeypatch.setattr(_native, "join_hashable_unique_records_v1", tracked_kernel)

    def run(engine: str) -> tuple[list[dict[str, object]], list[str]]:
        events: list[str] = []

        class Key:
            def __init__(self, label: str) -> None:
                self.label = label

            def __hash__(self) -> int:
                events.append(f"hash:{self.label}")
                return 0

            def __eq__(self, other: object) -> bool:
                assert isinstance(other, Key)
                events.append(f"eq:{self.label}:{other.label}")
                return True

        def select(row: dict[str, object]) -> object:
            key = row["key"]
            assert isinstance(key, Key)
            events.append(f"select:{key.label}")
            return key

        query = fpstreams.rows([{"key": Key("left"), "left": True}]).join(
            ({"key": Key("right"), "right": True},),
            left_on=select,
            right_on=select,
            validate="m:1",
        )
        if engine == "python":
            query = query.with_engine("python")
        return query.to_list(), events

    expected, python_events = run("python")
    actual, native_events = run("auto")

    def normalized(records: list[dict[str, object]]) -> list[dict[str, object]]:
        return [
            {name: getattr(value, "label", value) for name, value in record.items()}
            for record in records
        ]

    assert native_calls == 1
    assert normalized(actual) == normalized(expected)
    assert native_events == python_events


def test_callable_unique_join_cached_plan_mints_suffix_keys_per_output() -> None:
    """A homogeneous native layout must not reuse generated key objects between rows."""

    def select(row: dict[str, object]) -> object:
        return row["id"]

    result = (
        fpstreams.rows([{"id": 1, "value": "L1"}, {"id": 1, "value": "L2"}])
        .join(
            ({"id": 1, "value": "R"},),
            left_on=select,
            right_on=select,
            validate="m:1",
        )
        .to_list()
    )
    generated = [next(name for name in row if name == "value_right") for row in result]

    assert result == [
        {"id": 1, "value": "L1", "id_right": 1, "value_right": "R"},
        {"id": 1, "value": "L2", "id_right": 1, "value_right": "R"},
    ]
    assert generated[0] is not generated[1]


def test_callable_unique_join_cached_plan_mints_empty_name_suffix_keys() -> None:
    """An empty colliding field still follows two-part f-string identity semantics."""
    suffix = "".join(("__native", "_suffix__"))

    def left_key(row: dict[str, object]) -> object:
        return row["left_id"]

    def right_key(row: dict[str, object]) -> object:
        return row["right_id"]

    result = (
        fpstreams.rows([{"left_id": 1, "": "L1"}, {"left_id": 1, "": "L2"}])
        .join(
            ({"right_id": 1, "": "R"},),
            left_on=left_key,
            right_on=right_key,
            suffix=suffix,
            validate="m:1",
        )
        .to_list()
    )
    generated = [next(name for name in row if name == suffix) for row in result]

    assert generated[0] is not suffix
    assert generated[1] is not suffix
    assert generated[0] is not generated[1]


def test_callable_unique_join_cached_plan_revalidates_changed_left_shapes() -> None:
    """A later layout cannot reuse a collision-safe plan from the preceding left row."""

    def select(row: dict[str, object]) -> object:
        return row["id"]

    joined = fpstreams.rows(
        [
            {"id": 1, "left": "safe"},
            {"id": 1, "value": "L", "value_right": "occupied"},
        ]
    ).join(
        ({"id": 1, "value": "R"},),
        left_on=select,
        right_on=select,
        validate="m:1",
    )

    with pytest.raises(
        fpstreams.DuplicateKeyError,
        match="join maps right column 'value' to existing output column 'value_right'",
    ):
        joined.to_list()


def test_exact_i64_list_uses_a_coherent_snapshot_during_slice_replacement() -> None:
    """A free-threaded list replacement cannot expose a mixed exact-i64 snapshot."""
    import os
    import sys
    import threading
    from concurrent.futures import ThreadPoolExecutor

    from fpstreams import _native

    if getattr(sys, "_is_gil_enabled", lambda: True)():
        pytest.skip("requires a free-threaded interpreter")

    width = 16_384
    first_values = [1] * width
    second_values = [3] * width
    expected = {width, 3 * width}
    source = list(first_values)
    stop = threading.Event()
    kernel_errors: list[str] = []
    iterations = int(os.environ.get("FPSTREAMS_FT_I64_ITERATIONS", "200"))

    def mutate() -> None:
        while not stop.is_set():
            source[:] = second_values
            source[:] = first_values

    def read(_worker: int) -> None:
        for _ in range(iterations):
            if stop.is_set():
                return
            try:
                result = _native.terminal_i64(source, [], 1)
                if result not in expected:
                    kernel_errors.append(repr(result))
                    stop.set()
                    return
            except BaseException as error:  # pragma: no cover - free-threaded diagnostic
                kernel_errors.append(repr(error))
                stop.set()
                return

    with ThreadPoolExecutor(max_workers=9) as executor:
        mutator = executor.submit(mutate)
        list(executor.map(read, range(8)))
        stop.set()
        mutator.result()

    assert kernel_errors == []


def test_exact_list_tuple_row_group_sum_uses_a_coherent_snapshot_during_mutation() -> None:
    """A free-threaded list replacement cannot expose a mixed native row snapshot."""
    import os
    import sys
    import threading
    from concurrent.futures import ThreadPoolExecutor

    from fpstreams import _native

    if getattr(sys, "_is_gil_enabled", lambda: True)():
        pytest.skip("requires a free-threaded interpreter")

    first_rows = [(1000, 1), (1000, 2), (2000, 4)]
    second_rows = [(1000, 8), (3000, 16), (1000, 32)]
    expected = {((1000, 3), (2000, 4)), ((1000, 40), (3000, 16))}
    fixed_expected = {
        ((1000, 2, 3), (2000, 1, 4)),
        ((1000, 2, 40), (3000, 1, 16)),
    }
    source = list(first_rows)
    stop = threading.Event()
    kernel_errors: list[str] = []
    iterations = int(os.environ.get("FPSTREAMS_FT_GROUP_ITERATIONS", "20000"))

    def mutate() -> None:
        while not stop.is_set():
            source[:] = second_rows
            source[:] = first_rows

    def read(_worker: int) -> None:
        for _ in range(iterations):
            if stop.is_set():
                return
            try:
                result = _native.group_sum_i64_rows_v1(source, 0, 1, "key", "total")
                if result is None:
                    normalized = None
                else:
                    is_final_rows, payload = result
                    assert not is_final_rows
                    normalized = tuple(payload)
                if normalized not in expected:
                    kernel_errors.append(repr(normalized))
                    stop.set()
                    return
                fixed_result = _native.group_fixed_i64_rows_v1(
                    source,
                    0,
                    1,
                    "key",
                    "count",
                    "total",
                )
                if fixed_result is None:
                    fixed_normalized = None
                else:
                    fixed_final_rows, fixed_payload = fixed_result
                    assert not fixed_final_rows
                    fixed_normalized = tuple(fixed_payload)
                if fixed_normalized not in fixed_expected:
                    kernel_errors.append(repr(fixed_normalized))
                    stop.set()
                    return
            except BaseException as error:  # pragma: no cover - free-threaded diagnostic
                kernel_errors.append(repr(error))
                stop.set()
                return

    with ThreadPoolExecutor(max_workers=9) as executor:
        mutator = executor.submit(mutate)
        list(executor.map(read, range(8)))
        stop.set()
        mutator.result()

    assert kernel_errors == []


def test_exact_dict_group_sum_locks_shape_before_selector_lookup_during_mutation() -> None:
    """A free-threaded dict mutation cannot race size/iteration or trigger key equality."""
    import os
    import sys
    import threading
    from concurrent.futures import ThreadPoolExecutor

    from fpstreams import _native

    if getattr(sys, "_is_gil_enabled", lambda: True)():
        pytest.skip("requires a free-threaded interpreter")

    stop = threading.Event()
    mutation_context = threading.local()
    observed: list[object] = []
    kernel_errors: list[str] = []
    iterations = int(os.environ.get("FPSTREAMS_FT_DICT_GROUP_ITERATIONS", "20000"))

    class CollidingKey:
        def __hash__(self) -> int:
            return hash("key")

        def __eq__(self, other: object) -> bool:
            if not getattr(mutation_context, "active", False):
                observed.append(other)
                raise AssertionError("unsafe equality inside native group sum")
            return False

    collision = CollidingKey()
    row: dict[object, object] = {"key": 1, "value": 2}
    source = [row]

    def mutate() -> None:
        mutation_context.active = True
        while not stop.is_set():
            row[collision] = 0
            del row[collision]

    def read(_worker: int) -> None:
        for _ in range(iterations):
            if stop.is_set():
                return
            try:
                result = _native.group_sum_i64_dict_rows_v1(
                    source,
                    "key",
                    "value",
                    "key",
                    "total",
                )
                if result is None:
                    normalized = None
                else:
                    is_final_rows, payload = result
                    assert not is_final_rows
                    normalized = payload
                if normalized not in (None, [(1, 2)]):
                    kernel_errors.append(repr(normalized))
                    stop.set()
                    return
                fixed_result = _native.group_fixed_i64_dict_rows_v1(
                    source,
                    "key",
                    "value",
                    "key",
                    "count",
                    "total",
                )
                if fixed_result is None:
                    fixed_normalized = None
                else:
                    fixed_final_rows, fixed_payload = fixed_result
                    assert not fixed_final_rows
                    fixed_normalized = fixed_payload
                if fixed_normalized not in (None, [(1, 1, 2)]):
                    kernel_errors.append(repr(fixed_normalized))
                    stop.set()
                    return
            except BaseException as error:  # pragma: no cover - free-threaded diagnostic
                kernel_errors.append(repr(error))
                stop.set()
                return

    with ThreadPoolExecutor(max_workers=9) as executor:
        mutator = executor.submit(mutate)
        list(executor.map(read, range(8)))
        stop.set()
        mutator.result()

    assert observed == []
    assert kernel_errors == []


def test_exact_record_join_snapshot_never_runs_colliding_key_protocol_during_mutation() -> None:
    """A coherent native snapshot rejects a transient custom key before right-field lookup."""
    import os
    import sys
    import threading
    from concurrent.futures import ThreadPoolExecutor

    from fpstreams import _native

    if getattr(sys, "_is_gil_enabled", lambda: True)():
        pytest.skip("requires a free-threaded interpreter")

    stop = threading.Event()
    observed: list[object] = []
    kernel_errors: list[str] = []
    iterations = int(os.environ.get("FPSTREAMS_FT_JOIN_ITERATIONS", "20000"))

    class CollidingKey:
        def __hash__(self) -> int:
            return hash("right")

        def __eq__(self, other: object) -> bool:
            observed.append(other)
            raise AssertionError("unsafe equality inside native join")

    collision = CollidingKey()
    row: dict[object, object] = {"id": 1, "left": "L"}
    left = [row]
    right = ({"id": 1, "right": "R"},)

    def mutate() -> None:
        while not stop.is_set():
            row[collision] = 0
            del row[collision]

    def read(_worker: int) -> None:
        for _ in range(iterations):
            if stop.is_set():
                return
            try:
                _native.join_i64_unique_dict_rows_v1(left, right, "id", "id", False)
            except AssertionError as error:
                kernel_errors.append(str(error))
                stop.set()
                return

    with ThreadPoolExecutor(max_workers=9) as executor:
        mutator = executor.submit(mutate)
        list(executor.map(read, range(8)))
        stop.set()
        mutator.result()

    assert observed == []
    assert kernel_errors == []


def test_exact_record_join_compact_right_snapshot_is_coherent_during_mutation() -> None:
    """A compact build-side snapshot owns one locked value layout without key callbacks."""
    import os
    import sys
    import threading
    from concurrent.futures import ThreadPoolExecutor

    from fpstreams import _native

    if getattr(sys, "_is_gil_enabled", lambda: True)():
        pytest.skip("requires a free-threaded interpreter")

    stop = threading.Event()
    mutation_context = threading.local()
    observed: list[object] = []
    kernel_errors: list[str] = []
    iterations = int(os.environ.get("FPSTREAMS_FT_JOIN_ITERATIONS", "20000"))

    class CollidingKey:
        def __hash__(self) -> int:
            return hash("right")

        def __eq__(self, other: object) -> bool:
            if not getattr(mutation_context, "active", False):
                observed.append(other)
                raise AssertionError("unsafe equality inside compact native join snapshot")
            return False

    collision = CollidingKey()
    row: dict[object, object] = {"id": 1, "right": "before"}
    left = ({"id": 1, "left": "L"},)
    right = [row]
    expected = (
        None,
        [{"id": 1, "left": "L", "right": "before"}],
        [{"id": 1, "left": "L", "right": "after"}],
    )

    def mutate() -> None:
        mutation_context.active = True
        while not stop.is_set():
            row["right"] = "after"
            row[collision] = 0
            del row[collision]
            row["right"] = "before"

    def read(_worker: int) -> None:
        for _ in range(iterations):
            if stop.is_set():
                return
            try:
                result = _native.join_i64_unique_dict_rows_v1(left, right, "id", "id", False)
                if result not in expected:
                    kernel_errors.append(repr(result))
                    stop.set()
                    return
            except BaseException as error:  # pragma: no cover - free-threaded diagnostic
                kernel_errors.append(repr(error))
                stop.set()
                return

    with ThreadPoolExecutor(max_workers=9) as executor:
        mutator = executor.submit(mutate)
        list(executor.map(read, range(8)))
        stop.set()
        mutator.result()

    assert observed == []
    assert kernel_errors == []


def test_unique_record_join_prefers_v2_and_retries_v1_only_after_decline(
    monkeypatch,
) -> None:
    """The borrowed GIL ABI is preferred, while old-shape declines retain v1 coverage."""
    from fpstreams import _native

    left = [{"id": 1, "left": "L"}]
    right = ({"id": 1, "right": "R"},)
    expected_v2 = [{"native": "v2"}]
    expected_v1 = [{"native": "v1"}]
    calls: list[tuple[str, object, object, str, str, bool]] = []

    def v2_result(
        left_source: object,
        right_source: object,
        left_field: str,
        right_field: str,
        left_join: bool,
    ) -> list[dict[str, str]]:
        calls.append(("v2", left_source, right_source, left_field, right_field, left_join))
        return expected_v2

    def v1_forbidden(*_arguments: object) -> None:
        raise AssertionError("a successful v2 join must bypass v1")

    monkeypatch.setattr(_native, "record_join_v1_max_fields", 64, raising=False)
    monkeypatch.setattr(_native, "join_i64_unique_dict_rows_v2", v2_result, raising=False)
    monkeypatch.setattr(
        _native,
        "join_i64_unique_dict_rows_v1",
        v1_forbidden,
        raising=False,
    )

    result = fpstreams.rows(left).join(right, on="id", validate="m:1").to_list()

    assert result is expected_v2
    assert calls == [("v2", left, right, "id", "id", False)]

    calls.clear()

    def v2_decline(
        left_source: object,
        right_source: object,
        left_field: str,
        right_field: str,
        left_join: bool,
    ) -> None:
        calls.append(("v2", left_source, right_source, left_field, right_field, left_join))
        return None

    def v1_result(
        left_source: object,
        right_source: object,
        left_field: str,
        right_field: str,
        left_join: bool,
    ) -> list[dict[str, str]]:
        calls.append(("v1", left_source, right_source, left_field, right_field, left_join))
        return expected_v1

    monkeypatch.setattr(_native, "join_i64_unique_dict_rows_v2", v2_decline)
    monkeypatch.setattr(_native, "join_i64_unique_dict_rows_v1", v1_result)

    result = fpstreams.rows(left).join(right, on="id", validate="m:1").to_list()

    assert result is expected_v1
    assert calls == [
        ("v2", left, right, "id", "id", False),
        ("v1", left, right, "id", "id", False),
    ]


@pytest.mark.parametrize("join_options", ({}, {"validate": "m:m"}))
def test_many_to_many_record_join_prefers_many_kernel_without_opening_sources(
    monkeypatch,
    join_options: dict[str, str],
) -> None:
    """Default and explicit m:m joins consume duplicate native output before opening sources."""
    from fpstreams import _native
    from fpstreams.planning.source import Source

    left = [{"id": 1, "left": "L"}]
    right = ({"id": 1, "right": "a"}, {"id": 1, "right": "b"})
    expected = [
        {"id": 1, "left": "L", "right": "a"},
        {"id": 1, "left": "L", "right": "b"},
    ]
    calls: list[tuple[object, object, str, str, bool]] = []

    def many_result(
        left_source: object,
        right_source: object,
        left_field: str,
        right_field: str,
        left_join: bool,
    ) -> list[dict[str, str]]:
        calls.append((left_source, right_source, left_field, right_field, left_join))
        return expected

    def unique_forbidden(*_arguments: object) -> None:
        raise AssertionError("a successful m:m kernel must not probe the unique ABI")

    def opened(_source: Source[object]) -> object:
        raise AssertionError("a successful m:m kernel must not open either source")

    monkeypatch.setattr(Source, "open", opened)
    monkeypatch.setattr(_native, "record_join_v1_max_fields", 64, raising=False)
    monkeypatch.setattr(_native, "join_i64_many_dict_rows_v1", many_result, raising=False)
    monkeypatch.setattr(
        _native,
        "join_i64_unique_dict_rows_v1",
        unique_forbidden,
        raising=False,
    )

    result = fpstreams.rows(left).join(right, on="id", **join_options).to_list()

    assert result is expected
    assert calls == [(left, right, "id", "id", False)]


@pytest.mark.parametrize("many_symbol", ["missing", "non-callable"])
def test_many_join_old_wheel_symbol_falls_back_to_unique_kernel(
    monkeypatch,
    many_symbol: str,
) -> None:
    """An old wheel can still accelerate a proven-unique default join with its v1 ABI."""
    from fpstreams import _native

    expected = [{"native": "unique"}]
    calls = 0

    def unique_result(*_arguments: object) -> list[dict[str, str]]:
        nonlocal calls
        calls += 1
        return expected

    monkeypatch.setattr(_native, "record_join_v1_max_fields", 64, raising=False)
    monkeypatch.delattr(_native, "join_i64_unique_dict_rows_v2", raising=False)
    if many_symbol == "missing":
        monkeypatch.delattr(_native, "join_i64_many_dict_rows_v1", raising=False)
    else:
        monkeypatch.setattr(_native, "join_i64_many_dict_rows_v1", object(), raising=False)
    monkeypatch.setattr(
        _native,
        "join_i64_unique_dict_rows_v1",
        unique_result,
        raising=False,
    )

    result = fpstreams.rows([{"id": 1}]).join(({"id": 1},), on="id").to_list()

    assert result is expected
    assert calls == 1


def test_many_record_join_decline_does_not_repeat_the_native_scan(monkeypatch) -> None:
    """A many-kernel decline falls back canonically without rescanning through unique."""
    from fpstreams import _native

    calls: list[str] = []

    def many_decline(*_arguments: object) -> None:
        calls.append("many")
        return None

    def unique_forbidden(*_arguments: object) -> None:
        raise AssertionError("many decline must not repeat the same native source scan")

    monkeypatch.setattr(_native, "record_join_v1_max_fields", 64, raising=False)
    monkeypatch.setattr(_native, "join_i64_many_dict_rows_v1", many_decline, raising=False)
    monkeypatch.setattr(
        _native,
        "join_i64_unique_dict_rows_v1",
        unique_forbidden,
        raising=False,
    )

    result = fpstreams.rows([{"id": 1}]).join(({"id": 1},), on="id").to_list()

    assert result == [{"id": 1}]
    assert calls == ["many"]


@pytest.mark.parametrize(
    ("validate", "expected_error"),
    [("m:m", None), ("m:1", ValueError)],
)
def test_duplicate_right_native_decline_preserves_cardinality_semantics(
    monkeypatch,
    validate: str,
    expected_error: type[Exception] | None,
) -> None:
    """A duplicate-right None replays m:m buckets and m:1 validation in canonical Python."""
    from fpstreams import _native

    calls: list[str] = []

    def many_decline(*_arguments: object) -> None:
        calls.append("many")
        return None

    def unique_decline(*_arguments: object) -> None:
        calls.append("unique")
        return None

    monkeypatch.setattr(_native, "record_join_v1_max_fields", 64, raising=False)
    monkeypatch.setattr(_native, "join_i64_many_dict_rows_v1", many_decline, raising=False)
    monkeypatch.setattr(
        _native,
        "join_i64_unique_dict_rows_v1",
        unique_decline,
        raising=False,
    )
    joined = fpstreams.rows([{"id": 1, "left": "L"}]).join(
        ({"id": 1, "right": "a"}, {"id": 1, "right": "b"}),
        on="id",
        validate=validate,
    )

    if expected_error is None:
        assert joined.to_list() == [
            {"id": 1, "left": "L", "right": "a"},
            {"id": 1, "left": "L", "right": "b"},
        ]
    else:
        with pytest.raises(expected_error):
            joined.to_list()
    assert calls == (["many"] if validate == "m:m" else ["unique"])


def test_exact_record_join_i64_decline_uses_direct_kernel_without_opening_sources(
    monkeypatch,
) -> None:
    """An i64 decline proceeds to suffix-aware direct fields before source fallback."""
    from fpstreams import _native
    from fpstreams.planning.source import Source

    left = [{"id": 1, "left": "a"}, {"id": 2, "left": "b"}]
    right = ({"id": 1, "right": "A"},)
    opened: list[Source[object]] = []
    native_calls: list[str] = []
    original_open = Source.open
    native_direct = _native.join_hashable_unique_direct_records_v1

    def tracked_open(source: Source[object]) -> object:
        opened.append(source)
        return original_open(source)

    def narrow_decline(*_arguments: object) -> None:
        assert opened == []
        native_calls.append("i64")
        return None

    def direct_result(*arguments: object) -> object:
        assert opened == []
        native_calls.append("direct")
        return native_direct(*arguments)

    def callable_forbidden(*_arguments: object) -> None:
        raise AssertionError("a successful direct-field ABI must bypass callable selectors")

    monkeypatch.setattr(Source, "open", tracked_open)
    monkeypatch.setattr(_native, "record_join_v1_max_fields", 64, raising=False)
    monkeypatch.delattr(_native, "join_i64_unique_dict_rows_v2", raising=False)
    monkeypatch.setattr(
        _native,
        "join_i64_unique_dict_rows_v1",
        narrow_decline,
        raising=False,
    )
    monkeypatch.setattr(
        _native,
        "join_hashable_unique_direct_records_v1",
        direct_result,
        raising=False,
    )
    monkeypatch.setattr(
        _native,
        "join_hashable_unique_records_v1",
        callable_forbidden,
        raising=False,
    )

    assert fpstreams.rows(left).join(right, on="id", how="left", validate="m:1").to_list() == [
        {"id": 1, "left": "a", "right": "A"},
        {"id": 2, "left": "b", "right": None},
    ]
    assert native_calls == ["i64", "direct"]
    assert opened == []


def test_exact_record_join_native_errors_propagate_without_opening_sources(monkeypatch) -> None:
    """A real ABI failure is not an unsupported-shape signal and must remain the active error."""
    from fpstreams import _native
    from fpstreams.planning.source import Source

    error = RuntimeError("native join failed")

    def fail(*_arguments: object) -> None:
        raise error

    def opened(_source: Source[object]) -> object:
        raise AssertionError("a real native error must not enter source fallback")

    monkeypatch.setattr(Source, "open", opened)
    monkeypatch.setattr(_native, "record_join_v1_max_fields", 64, raising=False)
    monkeypatch.setattr(_native, "join_i64_unique_dict_rows_v2", fail, raising=False)
    monkeypatch.setattr(_native, "join_i64_unique_dict_rows_v1", fail, raising=False)

    joined = fpstreams.rows([{"id": 1}]).join(({"id": 1},), on="id", validate="m:1")
    with pytest.raises(RuntimeError) as captured:
        joined.to_list()
    assert captured.value is error


def test_many_record_join_native_errors_propagate_without_opening_sources(monkeypatch) -> None:
    """A real many-ABI failure propagates without probing unique or opening sources."""
    from fpstreams import _native
    from fpstreams.planning.source import Source

    error = RuntimeError("native many join failed")

    def fail(*_arguments: object) -> None:
        raise error

    def forbidden(*_arguments: object) -> None:
        raise AssertionError("a real many-ABI error must not enter another join path")

    def opened(_source: Source[object]) -> object:
        raise AssertionError("a real many-ABI error must not enter source fallback")

    monkeypatch.setattr(Source, "open", opened)
    monkeypatch.setattr(_native, "record_join_v1_max_fields", 64, raising=False)
    monkeypatch.setattr(_native, "join_i64_many_dict_rows_v1", fail, raising=False)
    monkeypatch.setattr(
        _native,
        "join_i64_unique_dict_rows_v1",
        forbidden,
        raising=False,
    )

    joined = fpstreams.rows([{"id": 1}]).join(({"id": 1},), on="id")
    with pytest.raises(RuntimeError) as captured:
        joined.to_list()
    assert captured.value is error


def test_exact_record_join_falls_back_for_old_wheels_and_active_failpoints(monkeypatch) -> None:
    """Missing ABI markers and instrumented runs must retain the ordinary executor."""
    from fpstreams import _native
    from fpstreams.runtime.failpoints import failpoint

    left = [{"id": 1, "left": "a"}]
    right = ({"id": 1, "right": "A"},)
    expected = [{"id": 1, "left": "a", "right": "A"}]

    with monkeypatch.context() as old_wheel:
        old_wheel.delattr(_native, "join_i64_unique_dict_rows_v1", raising=False)
        old_wheel.delattr(_native, "record_join_v1_max_fields", raising=False)
        assert fpstreams.rows(left).join(right, on="id", validate="m:1").to_list() == expected

    def forbidden(*_arguments: object) -> None:
        raise AssertionError("active failpoints must disable native record joins")

    monkeypatch.setattr(_native, "record_join_v1_max_fields", 64, raising=False)
    monkeypatch.setattr(_native, "join_i64_many_dict_rows_v1", forbidden, raising=False)
    monkeypatch.setattr(_native, "join_i64_unique_dict_rows_v1", forbidden, raising=False)
    monkeypatch.setattr(
        _native,
        "join_hashable_unique_direct_records_v1",
        forbidden,
        raising=False,
    )
    with failpoint("unrelated.transition", RuntimeError("unused")):
        assert fpstreams.rows(left).join(right, on="id").to_list() == expected
        assert fpstreams.rows(left).join(right, on=col("id"), validate="m:1").to_list() == [
            {"id": 1, "left": "a", "id_right": 1, "right": "A"}
        ]


def test_record_join_terminals_other_than_list_never_invoke_the_native_kernel(monkeypatch) -> None:
    """Iteration and scalar or alternate materializers keep their existing lazy executor."""
    from fpstreams import _native

    def forbidden(*_arguments: object) -> None:
        raise AssertionError("only to_list may invoke the eager record-join ABI")

    monkeypatch.setattr(_native, "record_join_v1_max_fields", 64, raising=False)
    monkeypatch.setattr(_native, "join_i64_many_dict_rows_v1", forbidden, raising=False)
    monkeypatch.setattr(_native, "join_i64_unique_dict_rows_v1", forbidden, raising=False)
    joined = fpstreams.rows([{"id": 1, "left": "a"}]).join(
        ({"id": 1, "right": "A"},),
        on="id",
    )
    expected = {"id": 1, "left": "a", "right": "A"}

    assert list(joined) == [expected]
    assert joined.first() == expected
    assert joined.count() == 1
    assert joined._flow.to_tuple() == (expected,)


def test_record_join_native_marker_rejects_non_direct_or_non_auto_plans() -> None:
    """Planner eligibility is confined to the direct auto-engine list materialization shape."""
    left = fpstreams.rows([{"id": 1, "left": "a"}])
    right = ({"id": 1, "right": "A"},)
    direct = left.join(right, on="id", validate="m:1")
    explicit_python = direct.with_engine("python")
    post_join_pipeline = direct.filter(lambda _row: True)
    pre_join_pipeline = left.filter(lambda _row: True).join(right, on="id", validate="m:1")
    one_to_one = left.join(right, on="id", validate="1:1")
    deferred_leaf = rows(flow.defer(lambda: [{"id": 1, "left": "a"}])).join(
        right,
        on="id",
    )

    for query in (
        explicit_python,
        post_join_pipeline,
        pre_join_pipeline,
        one_to_one,
        deferred_leaf,
    ):
        physical = compile_query(query._flow._query("list"))
        root = physical.root
        if isinstance(root, JoinPhysicalNode):
            assert root.native_record_i64 is None
        else:
            assert getattr(root, "native_record_i64", None) is None


def test_record_join_native_path_never_bypasses_one_shot_source_claims(monkeypatch) -> None:
    """Retained native_data on a custom one-shot Source must still use canonical claiming."""
    from fpstreams import _native
    from fpstreams.planning.source import Source, SourceCapabilities

    left_data = [{"id": 1, "left": "a"}]
    one_shot = Source(
        lambda: iter(left_data),
        SourceCapabilities(reiterable=False, exact_size=1),
        native_data=left_data,
    )
    joined = rows(fpstreams.Flow(one_shot)).join(
        ({"id": 1, "right": "A"},),
        on=col("id"),
        validate="m:1",
    )

    def forbidden(*_arguments: object) -> None:
        raise AssertionError("native join must not bypass a one-shot source claim")

    monkeypatch.setattr(_native, "record_join_v1_max_fields", 64, raising=False)
    monkeypatch.setattr(_native, "join_i64_many_dict_rows_v1", forbidden, raising=False)
    monkeypatch.setattr(_native, "join_i64_unique_dict_rows_v1", forbidden, raising=False)
    monkeypatch.setattr(
        _native,
        "join_hashable_unique_direct_records_v1",
        forbidden,
        raising=False,
    )

    physical = compile_query(joined._flow._query("list"))
    assert isinstance(physical.root, JoinPhysicalNode)
    assert physical.root.native_record_i64 is None
    assert getattr(physical.root, "native_direct_fields", None) is None
    assert joined.to_list() == [{"id": 1, "left": "a", "id_right": 1, "right": "A"}]
    with pytest.raises(fpstreams.FlowConsumedError):
        joined.to_list()


def test_hash_join_index_keeps_singletons_and_promotes_only_duplicates() -> None:
    """A singleton, including an empty record, should not pay for a bucket list."""
    from fpstreams.tabular.join import _join_record_index

    _columns, empty_index, empty_slots = _join_record_index([{}], lambda _row: 0, validate="m:m")
    _columns, duplicate_index, duplicate_slots = _join_record_index(
        [{"id": 1, "value": "a"}, {"id": 1, "value": "b"}],
        lambda row: row["id"],
        validate="m:m",
    )

    assert empty_slots[empty_index[0]] == {}
    duplicate_bucket = duplicate_slots[duplicate_index[1]]
    assert isinstance(duplicate_bucket, list)
    assert duplicate_bucket == [
        {"id": 1, "value": "a"},
        {"id": 1, "value": "b"},
    ]


# --- Consolidated from execution/test_sort_program.py ---

"""Stable cached sort-record and deterministic numeric run-choice contracts."""


from fpstreams.execution.sorting import SortRecord, sort_records
from fpstreams.physical.plan import SortPhysicalNode, SortStrategy
from fpstreams.runtime.failpoints import failpoint


class _LessThanOnlySortKey:
    """Pickleable test key whose valid ordering deliberately omits equality."""

    def __init__(self, value: int) -> None:
        self.value = value

    def __lt__(self, other: object) -> bool:
        assert isinstance(other, _LessThanOnlySortKey)
        return self.value < other.value

    def __eq__(self, other: object) -> bool:
        raise AssertionError("sorting must not compare keys for equality")


class _IdentityLessThanOnlyValue:
    """Pickleable identity value exposing ordering, stability tags, and no equality."""

    def __init__(self, key: int, position: int) -> None:
        self.key = key
        self.position = position

    def __lt__(self, other: object) -> bool:
        assert isinstance(other, _IdentityLessThanOnlyValue)
        return self.key < other.key

    def __eq__(self, other: object) -> bool:
        raise AssertionError("identity external sort must not compare values for equality")


class _FalseySortKey:
    """Callable key whose truth value must not replace it with identity."""

    def __call__(self, value: int) -> int:
        return -value

    def __bool__(self) -> bool:
        return False


class _UnpickleableSortKey:
    """A valid ordering key whose process-local result cannot enter a spill record."""

    def __init__(self, value: int) -> None:
        self.value = value

    def __lt__(self, other: object) -> bool:
        assert isinstance(other, _UnpickleableSortKey)
        return self.value < other.value

    def __reduce_ex__(self, _protocol: int) -> object:
        raise TypeError("sort key result is process-local")


class _LateUnpickleableSortKey:
    """A comparable key whose later records alone fail spill serialization."""

    def __init__(self, value: int) -> None:
        self.value = value

    def __lt__(self, other: object) -> bool:
        assert isinstance(other, _LateUnpickleableSortKey)
        return self.value < other.value

    def __reduce_ex__(self, protocol: int) -> object:
        if self.value == 0:
            return (type(self), (self.value,))
        raise TypeError("later sort key result is process-local")


_sort_merge_error: ValueError | None = None


class _ExplodingMergeValue:
    """Pickleable value that fails only when separate identity runs are merged."""

    def __lt__(self, _other: object) -> bool:
        if _sort_merge_error is None:
            raise AssertionError("merge error must be configured by the test")
        raise _sort_merge_error


def test_equal_keys_keep_global_input_position() -> None:
    """Record position preserves stable ties even when reverse key order is requested."""
    records = [SortRecord(1, position, {"position": position}) for position in range(100)]

    result = sort_records(records, reverse=True)

    assert [record.value["position"] for record in result] == list(range(100))


def test_external_sort_evaluates_key_once_per_value(tmp_path) -> None:
    calls = 0

    def key(value: dict[str, int]) -> int:
        nonlocal calls
        calls += 1
        return value["key"]

    values = [{"key": value} for value in range(64, -1, -1)]

    assert flow(values).external_sort(key=key, buffer_size=1, tempdir=tmp_path).to_list() == sorted(
        values,
        key=lambda value: value["key"],
    )
    assert calls == len(values)


def test_external_sort_preserves_stability_across_one_record_runs(tmp_path) -> None:
    """Global encounter positions must advance before each short-lived run closes."""
    values = [{"key": 0, "position": position} for position in range(80)]

    result = flow(values).external_sort_by("key", buffer_size=1, tempdir=tmp_path).to_list()

    assert result == values


def test_external_sort_uses_only_less_than_for_cross_run_keys(tmp_path) -> None:
    """Valid ordering keys need not provide equality just to enter the merge heap."""
    values = [3, 1, 2, 1]
    for reverse, expected in ((False, [1, 1, 2, 3]), (True, [3, 2, 1, 1])):
        result = (
            flow(values)
            .external_sort(
                key=_LessThanOnlySortKey,
                reverse=reverse,
                buffer_size=1,
                tempdir=tmp_path,
            )
            .to_list()
        )

        assert result == expected


@pytest.mark.parametrize("reverse", [False, True])
def test_external_identity_sort_preserves_less_than_only_stable_ties(
    tmp_path, reverse: bool
) -> None:
    """Raw-value compaction keeps global stability without adding equality probes."""
    values = [_IdentityLessThanOnlyValue(position % 3, position) for position in range(80)]

    result = flow(values).external_sort(reverse=reverse, buffer_size=1, tempdir=tmp_path).to_list()

    expected = sorted(range(80), key=lambda position: position % 3, reverse=reverse)
    assert [value.position for value in result] == expected


def test_external_sort_preserves_a_falsey_callable_key(tmp_path) -> None:
    result = (
        flow([1, 3, 2])
        .external_sort(key=_FalseySortKey(), buffer_size=1, tempdir=tmp_path)
        .to_list()
    )

    assert result == [3, 2, 1]


def test_external_identity_sort_spills_each_value_without_a_duplicate_cached_key(
    tmp_path, monkeypatch
) -> None:
    """Identity runs use the value itself instead of serializing key/value duplicates."""
    from fpstreams.storage.codec import SpillCodec

    stored_types: set[type[object]] = set()
    write_records = SpillCodec.write_records

    def tracked_write_records(self, handle, values):
        stored_types.update(type(value) for value in values)
        return write_records(self, handle, values)

    monkeypatch.setattr(SpillCodec, "write_records", tracked_write_records)

    assert flow([3, 1, 2]).external_sort(buffer_size=1, tempdir=tmp_path).to_list() == [1, 2, 3]
    assert stored_types == {int}


def test_external_sort_falls_back_when_only_cached_key_results_are_unpickleable(tmp_path) -> None:
    values = [3, 1, 2]

    assert flow(values).external_sort(
        key=_UnpickleableSortKey,
        buffer_size=1,
        tempdir=tmp_path,
    ).to_list() == [1, 2, 3]
    assert list(tmp_path.iterdir()) == []


def test_external_sort_value_only_fallback_rewrites_completed_keyed_runs(tmp_path) -> None:
    values = [0, 2, 1]

    assert flow(values).external_sort(
        key=_LateUnpickleableSortKey,
        buffer_size=1,
        tempdir=tmp_path,
    ).to_list() == [0, 1, 2]
    assert list(tmp_path.iterdir()) == []


def test_external_sort_keeps_the_cached_key_path_for_an_unpickleable_callable(tmp_path) -> None:
    calls = 0

    def key(value: int) -> int:
        nonlocal calls
        calls += 1
        return value

    assert flow([3, 1, 2]).external_sort(key=key, buffer_size=1, tempdir=tmp_path).to_list() == [
        1,
        2,
        3,
    ]
    assert calls == 3


@pytest.mark.parametrize("reverse", [False, True])
def test_external_sort_value_only_fallback_preserves_multi_run_stability_and_cleanup(
    tmp_path, reverse: bool
) -> None:
    opened = 0
    values = [{"key": value % 3, "position": value} for value in range(80, 0, -1)]

    def source():
        nonlocal opened
        opened += 1
        yield from values

    def key(value: dict[str, int]) -> _UnpickleableSortKey:
        return _UnpickleableSortKey(value["key"])

    result = (
        flow(source())
        .external_sort(
            key=key,
            reverse=reverse,
            buffer_size=1,
            tempdir=tmp_path,
        )
        .to_list()
    )

    assert result == sorted(values, key=lambda value: value["key"], reverse=reverse)
    assert opened == 1
    assert list(tmp_path.iterdir()) == []


def test_external_sort_value_serialization_error_keeps_the_codec_as_its_cause(tmp_path) -> None:
    from fpstreams.storage import SpillSerializationError

    values = [(2, lambda: None), (1, lambda: None)]

    with pytest.raises(TypeError, match="values must be picklable") as captured:
        result = flow(values).external_sort(
            key=lambda value: value[0],
            buffer_size=1,
            tempdir=tmp_path,
        )
        result.to_list()

    assert isinstance(captured.value.__cause__, SpillSerializationError)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("transition", ["spill.mkdir.after", "resource.register.after"])
def test_external_sort_cleans_up_when_store_initialization_fails(tmp_path, transition: str) -> None:
    with (
        failpoint(transition, RuntimeError("store initialization failed")),
        pytest.raises(RuntimeError, match="store initialization failed"),
    ):
        flow([3, 2, 1]).external_sort(buffer_size=1, tempdir=tmp_path).to_list()

    assert list(tmp_path.iterdir()) == []


def test_external_sort_keeps_a_primary_error_when_cleanup_also_fails(tmp_path) -> None:
    def key(value: int) -> int:
        if value == 1:
            raise ValueError("primary sort failure")
        return value

    with (
        failpoint("resource.close.before", OSError("cleanup failure")),
        pytest.raises(ValueError, match="primary sort failure") as captured,
    ):
        flow([3, 2, 1]).external_sort(key=key, buffer_size=1, tempdir=tmp_path).to_list()

    assert captured.value.__notes__ == ["cleanup failed: OSError: cleanup failure"]


def test_external_sort_reader_cleanup_is_not_allowed_to_mask_comparison(
    tmp_path, monkeypatch
) -> None:
    """Every reader closes, while its failures become notes on the comparison error."""
    from fpstreams.runtime import TrackedBinaryFile

    global _sort_merge_error

    primary = ValueError("comparison primary")
    _sort_merge_error = primary
    close_calls = 0
    original_close = TrackedBinaryFile._close_owned

    def fail_after_reader_close(handle: TrackedBinaryFile) -> None:
        nonlocal close_calls
        is_reader = handle.readable()
        original_close(handle)
        if is_reader:
            close_calls += 1
            raise OSError(f"reader close failure {close_calls}")

    monkeypatch.setattr(TrackedBinaryFile, "_close_owned", fail_after_reader_close)
    try:
        with pytest.raises(ValueError, match="comparison primary") as captured:
            flow([_ExplodingMergeValue() for _position in range(3)]).external_sort(
                buffer_size=1,
                tempdir=tmp_path,
            ).to_list()
    finally:
        _sort_merge_error = None

    assert captured.value is primary
    assert captured.value.__notes__ == [
        "cleanup failed: OSError: reader close failure 1",
        "cleanup failed: OSError: reader close failure 2",
    ]
    assert close_calls == 2
    assert list(tmp_path.iterdir()) == []


def test_external_sort_does_not_relabel_a_user_type_error(tmp_path) -> None:
    primary = TypeError("spill record is not serializable: raised by user key")

    def key(value: int) -> int:
        if value == 1:
            raise primary
        return value

    with pytest.raises(TypeError) as captured:
        flow([3, 2, 1]).external_sort(key=key, buffer_size=1, tempdir=tmp_path).to_list()

    assert captured.value is primary


def test_external_sort_does_not_relabel_a_user_spill_error_from_the_source(tmp_path) -> None:
    """The exported codec error remains user-owned when a source deliberately raises it."""
    from fpstreams.storage import SpillSerializationError

    primary = SpillSerializationError("raised by source")

    def source():
        yield 3
        yield 2
        raise primary

    with pytest.raises(SpillSerializationError) as captured:
        flow(source()).external_sort(buffer_size=1, tempdir=tmp_path).to_list()

    assert captured.value is primary


def test_external_sort_does_not_relabel_a_user_spill_error_from_the_key(tmp_path) -> None:
    """Only errors created by serialization itself may enter codec recovery."""
    from fpstreams.storage import SpillSerializationError

    primary = SpillSerializationError("raised by key")

    def key(value: int) -> int:
        if value == 1:
            raise primary
        return value

    with pytest.raises(SpillSerializationError) as captured:
        flow([3, 2, 1]).external_sort(key=key, buffer_size=1, tempdir=tmp_path).to_list()

    assert captured.value is primary


def test_external_sort_does_not_relabel_a_user_spill_error_from_comparison(tmp_path) -> None:
    """Comparison failures keep their identity even inside a spill-run construction."""
    from fpstreams.storage import SpillSerializationError

    primary = SpillSerializationError("raised by comparison")

    class ExplodingComparison:
        def __lt__(self, _other: object) -> bool:
            raise primary

    values = [ExplodingComparison(), ExplodingComparison(), ExplodingComparison()]
    with pytest.raises(SpillSerializationError) as captured:
        flow(values).external_sort(buffer_size=2, tempdir=tmp_path).to_list()

    assert captured.value is primary


def test_numeric_runs_match_stable_python_order() -> None:
    cases = [
        [3, 1, 2, 1, 0],
        [-(2**63), 9, 0, 2**63 - 1, -1] * 1000,
    ]
    for keys in cases:
        records = [SortRecord(key, position, position) for position, key in enumerate(keys)]
        expected = sorted(records, key=lambda record: (record.key, record.position))
        assert sort_records(records, reverse=False) == expected


def test_external_sort_compiles_an_explicit_physical_node(tmp_path) -> None:
    query = flow([3, 1, 2]).external_sort(buffer_size=1, tempdir=tmp_path)._query("list")
    physical = compile_query(query)

    assert isinstance(physical.nodes[0], SortPhysicalNode)
    assert physical.nodes[0].strategy is SortStrategy.CACHED_EXTERNAL_MERGE


def test_external_selector_sort_explains_cached_strategy(tmp_path) -> None:
    explanation = (
        flow([{"score": 2}, {"score": 1}])
        .external_sort_by("score", buffer_size=1, tempdir=tmp_path)
        .explain()
        .to_dict()
    )

    assert explanation["stages"] == [
        {
            "engine": "python-spill",
            "operations": ["external_sort"],
            "fused": False,
        }
    ]


def test_repeated_external_sort_explain_uses_physical_node_order(tmp_path) -> None:
    """Equal operation names must not send every physical engine to the first stage."""
    from dataclasses import replace

    from fpstreams.planning.explain import explain_physical

    query = (
        flow([3, 1, 2])
        .external_sort(buffer_size=1, tempdir=tmp_path)
        .external_sort(reverse=True, buffer_size=1, tempdir=tmp_path)
        ._query("list")
    )
    physical = compile_query(query)
    first, second = physical.nodes
    assert isinstance(first, SortPhysicalNode)
    assert isinstance(second, SortPhysicalNode)
    assert first.operation.name == second.operation.name == "external_sort"

    distinct_engines = replace(
        physical,
        nodes=(
            replace(first, engine="first-sort-engine"),
            replace(second, engine="second-sort-engine"),
        ),
    )

    assert explain_physical(distinct_engines).to_dict()["stages"] == [
        {
            "engine": "first-sort-engine",
            "operations": ["external_sort"],
            "fused": False,
        },
        {
            "engine": "second-sort-engine",
            "operations": ["external_sort"],
            "fused": False,
        },
    ]


def test_retained_arrow_direct_sort_compiles_a_stable_columnar_strategy() -> None:
    """Removing source-aware sort selection would put both retained sources back on rows."""
    pa = pytest.importorskip("pyarrow")

    sources = (
        pa.table({"key": [2, 1], "position": [0, 1]}),
        pa.record_batch({"key": [2, 1], "position": [0, 1]}),
    )
    for source in sources:
        query = fpstreams.rows.from_arrow(source).sort_by("key")._flow
        physical = compile_query(query._query("list"))

        assert len(physical.nodes) == 1
        assert isinstance(physical.nodes[0], SortPhysicalNode)
        assert physical.nodes[0].strategy is SortStrategy.ARROW_STABLE
        assert physical.nodes[0].engine == "arrow"


@pytest.mark.parametrize("as_batch", [False, True])
@pytest.mark.parametrize(
    ("reverse", "expected"),
    [
        (False, ["b", "d", "a", "c"]),
        (True, ["a", "c", "b", "d"]),
    ],
)
def test_retained_arrow_direct_sort_keeps_python_tie_order(
    as_batch: bool,
    reverse: bool,
    expected: list[str],
) -> None:
    """A descending key must never reverse records that compare equal."""
    pa = pytest.importorskip("pyarrow")
    table = pa.table(
        {
            "key": pa.array([2, 1, 2, 1], type=pa.int64()),
            "id": ["a", "b", "c", "d"],
        }
    )
    source = table.to_batches()[0] if as_batch else table

    result = fpstreams.rows.from_arrow(source).sort_by("key", reverse=reverse).to_list()

    assert [row["id"] for row in result] == expected


@pytest.mark.parametrize(
    ("reverse", "expected"),
    [
        (False, ["b", "d", "a", "c"]),
        (True, ["a", "c", "b", "d"]),
    ],
)
def test_retained_arrow_direct_sort_keeps_ties_stable_across_chunks(
    reverse: bool,
    expected: list[str],
) -> None:
    """The stable order proof must cover Arrow's cross-chunk merge, not only one batch."""
    pa = pytest.importorskip("pyarrow")
    table = pa.table(
        {
            "key": pa.chunked_array([[2, 1], [2, 1]], type=pa.int64()),
            "id": pa.chunked_array([["a", "b"], ["c", "d"]], type=pa.string()),
        }
    )

    result = fpstreams.rows.from_arrow(table).sort_by("key", reverse=reverse).to_list()

    assert [row["id"] for row in result] == expected


@pytest.mark.parametrize(
    ("key_type", "keys", "expected_positions"),
    [
        ("bool", [True, False, True, False], [1, 3, 0, 2]),
        ("int64", [2, 1, 2, 1], [1, 3, 0, 2]),
        ("uint64", [2**64 - 1, 0, 2**64 - 1, 0], [1, 3, 0, 2]),
        ("string", ["b", "a", "b", "a"], [1, 3, 0, 2]),
        ("large_string", ["b", "a", "b", "a"], [1, 3, 0, 2]),
        ("binary", [b"b", b"a", b"b", b"a"], [1, 3, 0, 2]),
        ("large_binary", [b"b", b"a", b"b", b"a"], [1, 3, 0, 2]),
    ],
)
def test_retained_arrow_direct_sort_accepts_only_proven_builtin_key_orders(
    key_type: str,
    keys: list[object],
    expected_positions: list[int],
) -> None:
    """Each admitted Arrow key family must have the same order as builtin Python scalars."""
    pa = pytest.importorskip("pyarrow")
    table = pa.table(
        {
            "key": pa.array(keys, type=pa.type_for_alias(key_type)),
            "position": range(len(keys)),
        }
    )

    result = fpstreams.rows.from_arrow(table).sort_by("key").to_list()

    assert [row["position"] for row in result] == expected_positions


def test_retained_arrow_direct_sort_accepts_fixed_size_binary_without_row_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fixed-width byte keys have the same lexicographic order and need no row path."""
    pa = pytest.importorskip("pyarrow")
    from fpstreams.planning.arrow_source import ArrowBatchSource
    from fpstreams.planning.source import Source

    open_rows = Source.open

    def reject_rows(source: Source[object]):
        if isinstance(source.native_data, ArrowBatchSource):
            raise AssertionError("a proven fixed-size binary key reopened Python rows")
        return open_rows(source)

    monkeypatch.setattr(Source, "open", reject_rows)
    table = pa.table(
        {
            "key": pa.array([b"bb", b"aa", b"bb", b"aa"], type=pa.binary(2)),
            "position": [0, 1, 2, 3],
        }
    )

    result = fpstreams.rows.from_arrow(table).sort_by("key").to_list()

    assert [row["position"] for row in result] == [1, 3, 0, 2]


def test_retained_arrow_sort_list_avoids_physical_row_forwarding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The list terminal should extend converted batches instead of yielding every row."""
    pa = pytest.importorskip("pyarrow")
    from fpstreams.streams import flow_terminals

    def reject_physical_rows(_plan):
        raise AssertionError("direct Arrow sort list entered the row-forwarding executor")

    monkeypatch.setattr(flow_terminals, "execute_physical", reject_physical_rows)
    table = pa.table({"key": [2, 1], "position": [0, 1]})

    assert fpstreams.rows.from_arrow(table).sort_by("key").to_list() == [
        {"key": 1, "position": 1},
        {"key": 2, "position": 0},
    ]


def test_ordinary_list_does_not_load_the_arrow_sort_terminal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-sort list must reject the specialization before importing its Arrow executor."""
    import sys

    monkeypatch.setitem(sys.modules, "fpstreams.execution.arrow", None)

    assert fpstreams.flow([2, 1]).to_list() == [2, 1]


def test_retained_arrow_direct_sort_falls_back_for_null_and_nan_semantics() -> None:
    """Arrow null/NaN placement must not replace Python's comparisons."""
    pa = pytest.importorskip("pyarrow")

    nullable = pa.table({"key": [2, None, 1], "position": [0, 1, 2]})
    with pytest.raises(TypeError) as canonical:
        fpstreams.rows.from_arrow(nullable).with_engine("python").sort_by("key").to_list()
    with pytest.raises(TypeError) as automatic:
        fpstreams.rows.from_arrow(nullable).sort_by("key").to_list()
    assert str(automatic.value) == str(canonical.value)
    assert automatic.value.__cause__ is None
    assert automatic.value.__context__ is None

    nan = float("nan")
    floating = pa.table({"key": [nan, 2.0, 1.0, nan, -1.0], "position": [0, 1, 2, 3, 4]})
    result = fpstreams.rows.from_arrow(floating).sort_by("key").to_list()
    assert [row["position"] for row in result] == [0, 4, 2, 1, 3]


def test_retained_arrow_direct_sort_keeps_missing_and_nested_comparison_errors() -> None:
    """Schema rejection must re-enter selection and comparison at the Python boundary."""
    pa = pytest.importorskip("pyarrow")

    nonempty = fpstreams.rows.from_arrow(pa.table({"present": [2, 1]}))
    with pytest.raises(fpstreams.SelectionError) as captured:
        nonempty.sort_by("missing").to_list()
    assert str(captured.value) == "Could not resolve selector 'missing'; failed at 'missing'"
    assert type(captured.value.__cause__) is KeyError
    assert captured.value.__cause__.args == ("missing",)

    empty = pa.table({"present": pa.array([], type=pa.int64())})
    assert fpstreams.rows.from_arrow(empty).sort_by("missing").to_list() == []

    nested = pa.table(
        {
            "key": pa.array(
                [{"value": 2}, {"value": 1}],
                type=pa.struct([("value", pa.int64())]),
            )
        }
    )
    with pytest.raises(TypeError) as canonical:
        fpstreams.rows.from_arrow(nested).with_engine("python").sort_by("key").to_list()
    with pytest.raises(TypeError) as automatic:
        fpstreams.rows.from_arrow(nested).sort_by("key").to_list()
    assert str(automatic.value) == str(canonical.value)


def test_retained_arrow_direct_sort_validates_before_reordering_row_conversion_errors() -> None:
    """Malformed primitive buffers must fail before sorting changes the first observed error."""
    pa = pytest.importorskip("pyarrow")
    offsets = pa.array([0, 1, 2], type=pa.int32()).buffers()[1]
    malformed = pa.Array.from_buffers(
        pa.string(),
        2,
        [None, offsets, pa.py_buffer(b"\xffa")],
    )
    table = pa.table({"key": [2, 1], "payload": malformed})

    with pytest.raises(UnicodeDecodeError) as canonical:
        (
            fpstreams.rows.from_arrow(table, batch_size=1)
            .with_engine("python")
            .sort_by("key")
            ._flow.to_set()
        )
    with pytest.raises(UnicodeDecodeError) as automatic:
        fpstreams.rows.from_arrow(table, batch_size=1).sort_by("key")._flow.to_set()

    assert automatic.value.args == canonical.value.args


def test_retained_arrow_direct_sort_rejects_forced_callback_pipeline_and_failpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only an uninstrumented auto plan with one exact direct selector may skip row keys."""
    pa = pytest.importorskip("pyarrow")
    from fpstreams.runtime.failpoints import failpoint

    table = pa.table({"key": [2, 1], "position": [0, 1]})
    forced = fpstreams.rows.from_arrow(table).with_engine("python").sort_by("key")._flow
    forced_plan = compile_query(forced._query("list"))
    assert isinstance(forced_plan.nodes[0], SortPhysicalNode)
    assert forced_plan.nodes[0].strategy is SortStrategy.IN_MEMORY

    selected: list[int] = []

    def select(row: dict[str, int]) -> int:
        selected.append(row["key"])
        return row["key"]

    assert fpstreams.rows.from_arrow(table).sort_by(select).to_list() == [
        {"key": 1, "position": 1},
        {"key": 2, "position": 0},
    ]
    assert selected == [2, 1]

    retained: list[int] = []

    def keep(row: dict[str, int]) -> bool:
        retained.append(row["position"])
        return True

    pipeline = fpstreams.rows.from_arrow(table).filter(keep).sort_by("key")
    assert pipeline.to_list() == [
        {"key": 1, "position": 1},
        {"key": 2, "position": 0},
    ]
    assert retained == [0, 1]
    physical = compile_query(pipeline._flow._query("list"))
    assert len(physical.nodes) == 2
    assert isinstance(physical.nodes[-1], SortPhysicalNode)
    assert physical.nodes[-1].strategy is SortStrategy.IN_MEMORY

    primary = RuntimeError("canonical Arrow source open")
    with (
        failpoint("source.open.after", primary),
        pytest.raises(RuntimeError) as captured,
    ):
        fpstreams.rows.from_arrow(table).sort_by("key").to_list()
    assert captured.value is primary

    class Field(str):
        pass

    subclass = fpstreams.rows.from_arrow(table).sort_by(Field("key"))._flow
    subclass_plan = compile_query(subclass._query("list"))
    assert isinstance(subclass_plan.nodes[0], SortPhysicalNode)
    assert subclass_plan.nodes[0].strategy is SortStrategy.IN_MEMORY


def test_retained_arrow_direct_sort_keeps_reverse_truth_after_row_boxing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A protocol-bearing reverse option must retain canonical evaluation order."""
    pa = pytest.importorskip("pyarrow")
    from fpstreams.tabular import arrow as arrow_adapter

    events: list[str] = []
    canonical_batch_to_rows = arrow_adapter.batch_to_rows

    def tracked_batch_to_rows(batch):
        events.append("rows")
        return canonical_batch_to_rows(batch)

    class Reverse:
        def __bool__(self) -> bool:
            events.append("reverse")
            return True

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", tracked_batch_to_rows)
    table = pa.table({"key": [2, 1], "position": [0, 1]})

    result = fpstreams.rows.from_arrow(table).sort_by("key", reverse=Reverse()).to_list()  # type: ignore[arg-type]

    assert result == [{"key": 2, "position": 0}, {"key": 1, "position": 1}]
    assert events == ["rows", "reverse"]


def test_retained_arrow_direct_sort_falls_back_after_expected_kernel_decline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A replayable retained source can safely restart through canonical rows."""
    pa = pytest.importorskip("pyarrow")
    pc = pytest.importorskip("pyarrow.compute")
    table = pa.table({"key": [2, 1], "position": [0, 1]})

    def decline(*_args, **_kwargs):
        raise ValueError("unsupported sort kernel")

    monkeypatch.setattr(pc, "sort_indices", decline)

    assert fpstreams.rows.from_arrow(table).sort_by("key").to_list() == [
        {"key": 1, "position": 1},
        {"key": 2, "position": 0},
    ]


def test_retained_arrow_direct_sort_propagates_memory_error_and_remains_replayable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Resource exhaustion propagates, while successful retained sources remain reusable."""
    pa = pytest.importorskip("pyarrow")
    pc = pytest.importorskip("pyarrow.compute")
    table = pa.table({"key": [2, 1], "position": [0, 1]})
    rows = fpstreams.rows.from_arrow(table)
    primary = MemoryError("sort allocation")

    with monkeypatch.context() as patch:
        patch.setattr(pc, "sort_indices", lambda *_args, **_kwargs: (_ for _ in ()).throw(primary))
        with pytest.raises(MemoryError) as captured:
            rows.sort_by("key").to_list()
        assert captured.value is primary

    expected = [{"key": 1, "position": 1}, {"key": 2, "position": 0}]
    assert rows.sort_by("key").to_list() == expected
    assert rows.with_engine("python").sort_by("key").to_list() == expected


@pytest.mark.parametrize(
    ("reverse", "expected_ids"),
    [(False, ["b", "d", "a", "c"]), (True, ["a", "c", "b", "d"])],
)
def test_in_memory_direct_field_sort_uses_exact_dict_guard_and_keeps_stable_ties(
    monkeypatch: pytest.MonkeyPatch,
    reverse: bool,
    expected_ids: list[str],
) -> None:
    """Removing the guarded native key lane would leave both native ABIs unused."""
    from operator import itemgetter

    from fpstreams import _native

    records = [
        {"key": 2, "id": "a"},
        {"key": 1, "id": "b"},
        {"key": 2, "id": "c"},
        {"key": 1, "id": "d"},
    ]
    guarded: list[tuple[int, ...]] = []
    keyed: list[tuple[str, type[BaseException]]] = []

    def exact_dict_rows(source: list[object]) -> bool:
        guarded.append(tuple(id(row) for row in source))
        return all(type(row) is dict for row in source)

    def direct_key(field: str, error_type: type[BaseException]):
        keyed.append((field, error_type))
        return itemgetter(field)

    monkeypatch.setattr(_native, "all_exact_dict_rows_v1", exact_dict_rows, raising=False)
    monkeypatch.setattr(_native, "direct_dict_field_key_v1", direct_key, raising=False)

    result = fpstreams.rows(records).sort_by("key", reverse=reverse).to_list()

    assert [row["id"] for row in result] == expected_ids
    assert guarded == [tuple(id(row) for row in records)]
    assert keyed == [("key", fpstreams.SelectionError)]


def test_in_memory_direct_field_sort_preserves_selection_error_timing_and_cause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed direct lookup still consumes the source first and raises the canonical error."""
    from fpstreams import _native

    pulls: list[int] = []
    guard_calls = 0

    def source():
        for index, row in enumerate([{"key": 2}, {"missing": 0}, {"key": 1}]):
            pulls.append(index)
            yield row

    def exact_dict_rows(_source: list[object]) -> bool:
        nonlocal guard_calls
        guard_calls += 1
        return True

    monkeypatch.setattr(_native, "all_exact_dict_rows_v1", exact_dict_rows, raising=False)

    with pytest.raises(fpstreams.SelectionError) as captured:
        fpstreams.rows(source()).sort_by("key").to_list()

    assert str(captured.value) == "Could not resolve selector 'key'; failed at 'key'"
    assert type(captured.value.__cause__) is KeyError
    assert captured.value.__cause__.args == ("key",)
    assert captured.value.__context__ is captured.value.__cause__
    assert captured.value.__suppress_context__
    assert pulls == [0, 1, 2]
    assert guard_calls == 1


def test_in_memory_direct_field_sort_keeps_comparison_type_error_raw() -> None:
    """The native key lane must not relabel a comparison failure as field selection."""
    records = [{"key": 1}, {"key": "two"}]

    with pytest.raises(TypeError) as canonical:
        fpstreams.rows(records).sort_by(lambda row: row["key"]).to_list()
    with pytest.raises(TypeError) as direct:
        fpstreams.rows(records).sort_by("key").to_list()

    assert str(direct.value) == str(canonical.value)
    assert direct.value.__cause__ is None
    assert direct.value.__context__ is None


def test_in_memory_direct_field_sort_keeps_lookup_type_error_at_selection_boundary() -> None:
    """A colliding custom key still wraps its lookup TypeError inside the native callback."""
    primary = TypeError("lookup equality failed")

    class Collision:
        def __hash__(self) -> int:
            return hash("key")

        def __eq__(self, _other: object) -> bool:
            raise primary

    with pytest.raises(fpstreams.SelectionError) as captured:
        fpstreams.rows([{Collision(): 1}]).sort_by("key").to_list()

    assert str(captured.value) == "Could not resolve selector 'key'; failed at 'key'"
    assert captured.value.__cause__ is primary
    assert captured.value.__context__ is primary
    assert captured.value.__suppress_context__


def test_in_memory_direct_field_sort_declines_mixed_rows_before_selector_effects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed exact-dict proof must retain Mapping and attribute lookup order."""
    from collections.abc import Mapping

    from fpstreams import _native

    effects: list[tuple[object, ...]] = []
    guard_calls = 0

    class LoggedMapping(Mapping[str, int]):
        def __init__(self, value: int) -> None:
            self.value = value

        def __getitem__(self, field: str) -> int:
            effects.append(("mapping", self.value, field))
            return self.value

        def __iter__(self):
            return iter(("key",))

        def __len__(self) -> int:
            return 1

    class LoggedAttribute:
        def __init__(self, value: int) -> None:
            self._value = value

        @property
        def key(self) -> int:
            effects.append(("attribute", self._value))
            return self._value

    def decline(_source: list[object]) -> bool:
        nonlocal guard_calls
        guard_calls += 1
        return False

    mapping = LoggedMapping(1)
    attribute = LoggedAttribute(2)
    exact = {"key": 3}
    monkeypatch.setattr(_native, "all_exact_dict_rows_v1", decline, raising=False)

    result = fpstreams.rows([exact, mapping, attribute]).sort_by("key").to_list()

    assert result == [mapping, attribute, exact]
    assert effects == [("mapping", 1, "key"), ("attribute", 2)]
    assert guard_calls == 1


def test_in_memory_direct_field_sort_declines_dict_subclasses_before_getitem(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A dict subclass keeps the generated selector and its observable protocol calls."""
    from fpstreams import _native

    effects: list[tuple[int, str]] = []
    guard_calls = 0

    class Record(dict[str, int]):
        def __getitem__(self, field: str) -> int:
            value = super().__getitem__(field)
            effects.append((value, field))
            return value

    native_guard = _native.all_exact_dict_rows_v1

    def tracked_guard(source: object) -> bool:
        nonlocal guard_calls
        guard_calls += 1
        return native_guard(source)

    monkeypatch.setattr(_native, "all_exact_dict_rows_v1", tracked_guard)
    records = [Record(key=2), Record(key=1)]

    assert fpstreams.rows(records).sort_by("key").to_list() == [records[1], records[0]]
    assert effects == [(2, "key"), (1, "key")]
    assert guard_calls == 1


def test_direct_field_sort_rejects_string_subclasses_and_forged_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only an exact string compiled with the private identity token may enter the lane."""
    from fpstreams import _native

    class Field(str):
        pass

    def unexpected_guard(_source: object) -> bool:
        raise AssertionError("untrusted selector metadata reached the native guard")

    monkeypatch.setattr(_native, "all_exact_dict_rows_v1", unexpected_guard)
    records = [{"key": 2}, {"key": 1}]
    assert fpstreams.rows(records).sort_by(Field("key")).to_list() == [records[1], records[0]]

    effects: list[int] = []

    def forged(row: dict[str, int]) -> int:
        effects.append(row["key"])
        return row["key"]

    forged.__fpstreams_direct_field_v1__ = (object(), "key")  # type: ignore[attr-defined]
    assert fpstreams.rows(records).sort_by(forged).to_list() == [records[1], records[0]]
    assert effects == [2, 1]


def test_native_exact_dict_sort_guard_checks_types_without_protocol_dispatch() -> None:
    """The speculative ABI proves only exact container types without row protocol calls."""
    from fpstreams import _native

    class Record(dict[str, int]):
        def __getitem__(self, key: str) -> int:
            raise AssertionError(f"record protocol dispatched for {key}")

    class RecordList(list[dict[str, int]]):
        def __iter__(self):
            raise AssertionError("list protocol dispatched")

    assert _native.all_exact_dict_rows_v1([{}, {"key": 1}])
    assert not _native.all_exact_dict_rows_v1(({},))
    assert not _native.all_exact_dict_rows_v1([Record(key=1)])
    assert not _native.all_exact_dict_rows_v1(RecordList([{"key": 1}]))
    assert _native.all_exact_dict_rows_v1([{"key": 1, 2: 3}])


def test_in_memory_direct_field_sort_keeps_old_wheel_and_opaque_selector_fallbacks(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """The optional ABI cannot become required or leak into callable and external sorts."""
    from fpstreams import _native

    records = [{"key": 2}, {"key": 1}]
    for missing_abi in ("all_exact_dict_rows_v1", "direct_dict_field_key_v1"):
        with monkeypatch.context() as patch:
            patch.delattr(_native, missing_abi, raising=False)
            assert fpstreams.rows(records).sort_by("key").to_list() == [records[1], records[0]]

    def unexpected_guard(_source: object) -> bool:
        raise AssertionError("opaque and external sorts must not request an exact-dict proof")

    def unexpected_key(_field: object, _error_type: object) -> object:
        raise AssertionError("opaque and external sorts must not request a direct-field key")

    monkeypatch.setattr(_native, "all_exact_dict_rows_v1", unexpected_guard, raising=False)
    monkeypatch.setattr(_native, "direct_dict_field_key_v1", unexpected_key, raising=False)
    assert fpstreams.rows(records).sort_by(lambda row: row["key"]).to_list() == [
        records[1],
        records[0],
    ]
    assert fpstreams.rows(records).external_sort_by(
        "key", buffer_size=1, tempdir=tmp_path
    ).to_list() == [records[1], records[0]]


# --- Consolidated from test_arrow_dictionary_group_sum.py ---

"""Narrow contracts for dictionary-encoded Arrow group-sum execution."""


from datetime import datetime
from typing import Any

import pytest

import fpstreams

pa = pytest.importorskip("pyarrow")


def _dictionary_array(indices: list[int | None], values: list[object]) -> Any:
    """Build one dictionary array with an intentionally explicit physical dictionary."""
    return pa.DictionaryArray.from_arrays(
        pa.array(indices, type=pa.int8()),
        pa.array(values),
    )


def _supported_table(*, differing_dictionaries: bool) -> Any:
    """Return logical keys a, b, a, null, c in one or two physical dictionaries."""
    first = _dictionary_array([1, 0, 1], ["b", "a", "c"])
    if not differing_dictionaries:
        keys = _dictionary_array([1, 0, 1, None, 2], ["b", "a", "c"])
    else:
        second = _dictionary_array([None, 0], ["c", "a", "b"])
        keys = pa.chunked_array([first, second])
    return pa.table({"key": keys, "value": [1, 2, 3, 4, 5]})


def _group_sum(table: Any) -> list[dict[str, object]]:
    return (
        fpstreams.rows.from_arrow(table)
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
        .to_list()
    )


@pytest.mark.parametrize("differing_dictionaries", [False, True])
def test_arrow_dictionary_group_sum_stays_columnar_across_dictionary_layouts(
    monkeypatch: pytest.MonkeyPatch,
    differing_dictionaries: bool,
) -> None:
    """Canonical dictionaries, including null indices, must never box input rows."""
    from fpstreams.tabular import arrow as arrow_adapter

    def forbidden_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("canonical dictionary grouping must stay columnar")

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", forbidden_rows)

    assert _group_sum(_supported_table(differing_dictionaries=differing_dictionaries)) == [
        {"key": "a", "total": 4},
        {"key": "b", "total": 2},
        {"key": None, "total": 4},
        {"key": "c", "total": 5},
    ]


@pytest.mark.parametrize(
    ("keys", "values", "expected"),
    [
        pytest.param(
            _dictionary_array([0, 1, 0, 1], ["same", "same"]),
            [1, 2, 3, 4],
            [{"key": "same", "total": 10}],
            id="duplicate-dictionary-values",
        ),
        pytest.param(
            _dictionary_array([0, None, 1, 0, None], [None, "value"]),
            [1, 2, 3, 4, 5],
            [{"key": None, "total": 12}, {"key": "value", "total": 3}],
            id="dictionary-null-value",
        ),
        pytest.param(
            pa.DictionaryArray.from_arrays(
                pa.array([1, 0, 1], type=pa.int8()),
                pa.array(
                    [datetime(2024, 1, 1), datetime(2024, 1, 2)],
                    type=pa.timestamp("s"),
                ),
            ),
            [1, 2, 3],
            [
                {"key": datetime(2024, 1, 2), "total": 4},
                {"key": datetime(2024, 1, 1), "total": 2},
            ],
            id="unsupported-dictionary-value-type",
        ),
    ],
)
def test_arrow_dictionary_group_sum_declines_unsafe_dictionary_values(
    monkeypatch: pytest.MonkeyPatch,
    keys: Any,
    values: list[int],
    expected: list[dict[str, object]],
) -> None:
    """Noncanonical or unsupported dictionaries retain canonical Python grouping."""
    from fpstreams.tabular import arrow as arrow_adapter

    converted: list[int] = []
    convert_rows = arrow_adapter.batch_to_rows

    def tracked(batch: Any) -> list[dict[str, object]]:
        converted.append(batch.num_rows)
        return convert_rows(batch)

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", tracked)

    assert _group_sum(pa.table({"key": keys, "value": values})) == expected
    assert converted == [len(values)]


def test_arrow_dictionary_group_sum_active_failpoint_uses_canonical_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Instrumentation disables the dictionary specialization without changing results."""
    from fpstreams.runtime.failpoints import failpoint
    from fpstreams.tabular import arrow as arrow_adapter

    converted: list[int] = []
    convert_rows = arrow_adapter.batch_to_rows

    def tracked(batch: Any) -> list[dict[str, object]]:
        converted.append(batch.num_rows)
        return convert_rows(batch)

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", tracked)
    with failpoint("unrelated.transition", RuntimeError("unused")):
        result = _group_sum(_supported_table(differing_dictionaries=False))

    assert result == [
        {"key": "a", "total": 4},
        {"key": "b", "total": 2},
        {"key": None, "total": 4},
        {"key": "c", "total": 5},
    ]
    assert converted == [5]


def test_arrow_dictionary_guard_decline_does_not_reopen_native_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A speculative dictionary decline performs one native open before row fallback."""
    from fpstreams.planning.source import Source

    native_opens = 0
    open_native = Source.open_native

    def tracked(self: Source[Any], expected_type: type[Any]) -> Any:
        nonlocal native_opens
        native_opens += 1
        return open_native(self, expected_type)

    monkeypatch.setattr(Source, "open_native", tracked)
    duplicate_keys = _dictionary_array([0, 1, 0, 1], ["same", "same"])

    assert _group_sum(pa.table({"key": duplicate_keys, "value": [1, 2, 3, 4]})) == [
        {"key": "same", "total": 10}
    ]
    assert native_opens == 1


# --- Consolidated from test_pairs_native.py ---

"""Guarded native execution for direct ``Pairs.aggregate_values(sum)`` calls."""


import pytest

import fpstreams
from fpstreams.planning.source import SourceCapabilities


@pytest.mark.parametrize("container", [list, tuple])
def test_direct_pair_sum_uses_strict_native_abi_without_opening_source(
    monkeypatch: pytest.MonkeyPatch,
    container: type[list[tuple[int, int]]] | type[tuple[tuple[int, int], ...]],
) -> None:
    """Deleting this fast path must expose an unexpected canonical source open."""
    first_key = int("1000")
    equal_key = int("1000")
    later_key = int("-7")
    assert first_key == equal_key and first_key is not equal_key
    source = container([(first_key, 2), (later_key, 5), (equal_key, 3)])

    def unexpected_open(*_args: object) -> Iterator[Any]:
        raise AssertionError("native pair sum opened its Python source")

    def unexpected_open_native(*_args: object) -> object:
        raise AssertionError("native pair sum entered the source lifecycle")

    monkeypatch.setattr(Source, "open", unexpected_open)
    monkeypatch.setattr(Source, "open_native", unexpected_open_native)

    result = fpstreams.pairs(source).aggregate_values(total=fpstreams.agg.sum())

    keys = list(result)
    assert keys == [1000, -7]
    assert keys[0] is first_key
    assert result == {1000: {"total": 5}, -7: {"total": 5}}


def test_direct_pair_sum_accepts_a_final_native_dictionary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Large native outputs must not be rebuilt from intermediate pairs in Python."""
    from fpstreams import _native

    source = [(1, 2), (1, 3)]
    final = {1: {"total": 5}}

    def final_dictionary(rows: object, output_name: object) -> tuple[bool, object]:
        assert rows is source
        assert output_name == "total"
        return True, final

    def unexpected_legacy_kernel(*_args: object) -> object:
        raise AssertionError("a handled v2 result reached the legacy pair kernel")

    monkeypatch.setattr(
        _native,
        "group_sum_i64_exact_pairs_v2",
        final_dictionary,
        raising=False,
    )
    monkeypatch.setattr(_native, "group_sum_i64_exact_pairs_v1", unexpected_legacy_kernel)

    result = fpstreams.pairs(source).aggregate_values(total=fpstreams.agg.sum())

    assert result is final


def test_direct_pair_multi_aggregate_reuses_native_group_kernel_without_opening_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Canonical whole-value lanes should reuse the exact-record Rust group kernel."""
    from fpstreams import _native

    first_key = int("1000")
    equal_key = int("1000")
    source = [(first_key, 7), (-3, 4), (equal_key, 2)]
    native_kernel = _native.group_multi_i64_exact_pairs_v1
    calls = 0

    def tracked_kernel(
        rows: object,
        key_name: str,
        lanes: tuple[tuple[int, int | None, str], ...],
    ) -> object:
        nonlocal calls
        calls += 1
        assert rows is source
        assert type(key_name) is str
        assert key_name not in {"count", "total", "low", "high"}
        assert lanes == (
            (0, None, "count"),
            (1, 1, "total"),
            (2, 1, "low"),
            (3, 1, "high"),
        )
        return native_kernel(rows, key_name, lanes)

    def unexpected_open(*_args: object) -> Iterator[Any]:
        raise AssertionError("native pair aggregation opened its Python source")

    monkeypatch.setattr(_native, "group_multi_i64_exact_pairs_v1", tracked_kernel)
    monkeypatch.setattr(Source, "open", unexpected_open)

    result = fpstreams.pairs(source).aggregate_values(
        count=fpstreams.agg.count(),
        total=fpstreams.agg.sum(),
        low=fpstreams.agg.min(),
        high=fpstreams.agg.max(),
    )

    assert calls == 1
    keys = list(result)
    assert keys == [1000, -3]
    assert keys[0] is first_key
    assert result == {
        1000: {"count": 2, "total": 9, "low": 2, "high": 7},
        -3: {"count": 1, "total": 4, "low": 4, "high": 4},
    }


def test_direct_pair_multi_native_decline_opens_only_the_canonical_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An incompatible exact container must remain untouched for one Python fallback."""
    from fpstreams import _native

    events: list[str] = []
    original_open = Source.open

    def decline(*_args: object) -> None:
        events.append("native")
        return None

    def tracked_open(source: Source[Any]) -> Iterator[Any]:
        events.append("open")
        return original_open(source)

    monkeypatch.setattr(_native, "group_multi_i64_exact_pairs_v1", decline)
    monkeypatch.setattr(Source, "open", tracked_open)

    result = fpstreams.pairs([(1, 2), (1, 3)]).aggregate_values(
        count=fpstreams.agg.count(),
        minimum=fpstreams.agg.min(),
    )

    assert result == {1: {"count": 2, "minimum": 2}}
    assert events == ["native", "open"]


@pytest.mark.parametrize(
    ("source", "message"),
    [
        ([(1,)], "not enough values to unpack"),
        ([(1, 2, 3)], "too many values to unpack"),
    ],
)
def test_direct_pair_multi_native_declines_non_pair_tuple_width(
    source: list[tuple[int, ...]],
    message: str,
) -> None:
    """The shared record kernel must retain the strict public pair boundary."""
    with pytest.raises(ValueError, match=message):
        fpstreams.pairs(source).aggregate_values(
            count=fpstreams.agg.count(),
            total=fpstreams.agg.sum(),
        )


def test_direct_pair_native_honors_replaced_public_aggregation_helper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Public collector helpers remain authoritative over every native lane shape."""
    pairs_module = sys.modules["fpstreams.streams.pairs"]

    def replacement(
        states: dict[str, int],
        items: tuple[tuple[str, object], ...],
        value: int,
    ) -> None:
        for name, _aggregation in items:
            states[name] += 1 if name == "count" else value * 10

    monkeypatch.setattr(pairs_module, "step_aggregations", replacement)

    assert fpstreams.pairs([(1, 2), (1, 3)]).aggregate_values(
        count=fpstreams.agg.count(),
        total=fpstreams.agg.sum(),
    ) == {1: {"count": 2, "total": 50}}


@pytest.mark.parametrize("source", [[], ()])
def test_direct_empty_pair_sum_is_a_handled_native_result(
    monkeypatch: pytest.MonkeyPatch,
    source: list[tuple[int, int]] | tuple[tuple[int, int], ...],
) -> None:
    """An empty native dictionary must not be confused with the fallback sentinel."""

    def unexpected_open(*_args: object) -> Iterator[Any]:
        raise AssertionError("empty native pair sum opened its Python source")

    monkeypatch.setattr(Source, "open", unexpected_open)

    assert fpstreams.pairs(source).aggregate_values(total=fpstreams.agg.sum()) == {}


@pytest.mark.parametrize("native_mode", ["missing", "noncallable", "decline"])
def test_pair_sum_native_unavailability_opens_only_the_canonical_fallback(
    monkeypatch: pytest.MonkeyPatch,
    native_mode: str,
) -> None:
    """Missing and ``None`` ABI outcomes leave the source unopened until fallback."""
    from fpstreams import _native

    events: list[str] = []
    original_open = Source.open

    def tracked_open(source: Source[Any]) -> Iterator[Any]:
        events.append("open")
        return original_open(source)

    monkeypatch.setattr(Source, "open", tracked_open)
    if native_mode == "missing":
        monkeypatch.delattr(_native, "group_sum_i64_exact_pairs_v2")
        monkeypatch.delattr(_native, "group_sum_i64_exact_pairs_v1")
    elif native_mode == "noncallable":
        monkeypatch.setattr(_native, "group_sum_i64_exact_pairs_v2", None)
        monkeypatch.setattr(_native, "group_sum_i64_exact_pairs_v1", None)
    else:

        def decline(_source: object, _output_name: object) -> None:
            assert events == []
            events.append("native")
            return None

        monkeypatch.setattr(_native, "group_sum_i64_exact_pairs_v2", decline)

    result = fpstreams.pairs([(1, 2), (1, 3)]).aggregate_values(total=fpstreams.agg.sum())

    assert result == {1: {"total": 5}}
    assert events == (["native", "open"] if native_mode == "decline" else ["open"])


def test_pair_sum_native_error_propagates_without_opening_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Catching a real ABI failure would incorrectly turn it into Python fallback."""
    from fpstreams import _native

    events: list[str] = []

    def fail(_source: object, _output_name: object) -> None:
        events.append("native")
        raise MemoryError("native grouping allocation failed")

    def unexpected_open(*_args: object) -> Iterator[Any]:
        events.append("open")
        raise AssertionError("native failure opened the source")

    monkeypatch.setattr(_native, "group_sum_i64_exact_pairs_v2", fail)
    monkeypatch.setattr(Source, "open", unexpected_open)

    with pytest.raises(MemoryError, match="native grouping allocation failed"):
        fpstreams.pairs([(1, 2)]).aggregate_values(total=fpstreams.agg.sum())
    assert events == ["native"]


def test_pair_sum_native_requires_a_direct_replayable_exact_container(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Removing any source-shape guard must invoke the rejecting kernel below."""
    from fpstreams import _native

    def unexpected_native(*_args: object) -> None:
        raise AssertionError("ineligible pair source reached native execution")

    monkeypatch.setattr(_native, "group_sum_i64_exact_pairs_v2", unexpected_native)

    def one_shot() -> Iterator[tuple[int, int]]:
        yield 1, 2
        yield 1, 3

    class PairList(list[tuple[int, int]]):
        pass

    assert fpstreams.pairs(one_shot()).aggregate_values(total=fpstreams.agg.sum()) == {
        1: {"total": 5}
    }
    assert fpstreams.pairs(PairList([(1, 2)])).aggregate_values(total=fpstreams.agg.sum()) == {
        1: {"total": 2}
    }
    assert fpstreams.pairs([(1, 2)]).map_values(lambda value: value + 3).aggregate_values(
        total=fpstreams.agg.sum()
    ) == {1: {"total": 5}}


@pytest.mark.parametrize(
    "capabilities",
    [
        SourceCapabilities(reiterable=False, exact_size=2, ordered=True),
        SourceCapabilities(reiterable=True, exact_size=2, ordered=False),
    ],
)
def test_pair_sum_native_checks_retained_source_replayability_and_order(
    monkeypatch: pytest.MonkeyPatch,
    capabilities: SourceCapabilities,
) -> None:
    """Retained rows alone cannot authorize native execution without both source guarantees."""
    from fpstreams import _native

    rows = [(1, 2), (1, 3)]
    source = Source(lambda: iter(rows), capabilities, native_data=rows)

    def unexpected_native(*_args: object) -> None:
        raise AssertionError("unproven pair source reached native execution")

    monkeypatch.setattr(_native, "group_sum_i64_exact_pairs_v2", unexpected_native)

    assert fpstreams.Flow(source).pairs().aggregate_values(total=fpstreams.agg.sum()) == {
        1: {"total": 5}
    }


def test_pair_sum_native_honors_forced_python_and_active_failpoints(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Execution policy and instrumentation must retain the canonical Python boundaries."""
    from fpstreams import _native

    def unexpected_native(*_args: object) -> None:
        raise AssertionError("execution policy allowed native pair grouping")

    monkeypatch.setattr(_native, "group_sum_i64_exact_pairs_v2", unexpected_native)
    forced_python = fpstreams.pairs([(1, 2), (1, 3)]).to_flow().with_engine("python").pairs()

    assert forced_python.aggregate_values(total=fpstreams.agg.sum()) == {1: {"total": 5}}
    with failpoint("unrelated.transition", RuntimeError("unused")):
        assert fpstreams.pairs([(1, 2)]).aggregate_values(total=fpstreams.agg.sum()) == {
            1: {"total": 2}
        }


def test_narrow_pair_sum_kernel_requires_one_builtin_whole_value_sum(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Other lane shapes must bypass the specialized single-sum ABI."""
    from fpstreams import _native

    def unexpected_native(*_args: object) -> None:
        raise AssertionError("noncanonical aggregation reached native pair grouping")

    monkeypatch.setattr(_native, "group_sum_i64_exact_pairs_v2", unexpected_native)
    source = [(1, 2), (1, 3)]

    assert fpstreams.pairs(source).aggregate_values(
        total=fpstreams.agg.sum(lambda value: value)
    ) == {1: {"total": 5}}
    assert fpstreams.pairs(source).aggregate_values(
        total=fpstreams.agg.sum(), count=fpstreams.agg.count()
    ) == {1: {"total": 5, "count": 2}}
    assert fpstreams.pairs(source).aggregate_values(minimum=fpstreams.agg.min()) == {
        1: {"minimum": 2}
    }

    branded = fpstreams.agg.sum()

    def replacement(total: int, _value: int) -> int:
        return total + 100

    replacement.__dict__.update(branded.step.__dict__)
    counterfeit = fpstreams.Aggregator(
        branded.initializer,
        replacement,
        branded.finish,
        branded.combine,
        branded.done,
        branded.native,
    )
    assert fpstreams.pairs(source).aggregate_values(total=counterfeit) == {1: {"total": 200}}


def test_pair_sum_native_requires_an_exact_output_name(monkeypatch: pytest.MonkeyPatch) -> None:
    """A string subclass can have observable hashing and therefore cannot enter native output."""
    from fpstreams import _native

    class OutputName(str):
        pass

    def unexpected_native(*_args: object) -> None:
        raise AssertionError("nonexact output name reached native pair grouping")

    monkeypatch.setattr(_native, "group_sum_i64_exact_pairs_v2", unexpected_native)
    output_name = OutputName("total")

    result = fpstreams.pairs([(1, 2)]).aggregate_values(**{output_name: fpstreams.agg.sum()})

    assert result == {1: {"total": 2}}
    assert next(iter(result[1])) is output_name


@pytest.mark.parametrize(
    ("source", "message"),
    [
        ([(1,)], "not enough values to unpack"),
        ([(1, 2), (1, 2, 3)], "too many values to unpack"),
    ],
)
def test_pair_sum_native_decline_preserves_pair_unpack_errors(
    monkeypatch: pytest.MonkeyPatch,
    source: list[Any],
    message: str,
) -> None:
    """Strict width rejection must replay the canonical loop and its exact failure class."""
    from fpstreams import _native

    calls = 0
    strict_kernel = _native.group_sum_i64_exact_pairs_v2

    def tracked_kernel(rows: object, output_name: object) -> object:
        nonlocal calls
        calls += 1
        return strict_kernel(rows, output_name)

    monkeypatch.setattr(_native, "group_sum_i64_exact_pairs_v2", tracked_kernel)

    with pytest.raises(ValueError, match=message):
        fpstreams.pairs(source).aggregate_values(total=fpstreams.agg.sum())
    assert calls == 1


def test_pair_aggregation_preserves_builtin_keys_and_multiple_lanes() -> None:
    """The public Python path preserves names, key order, and canonical lane values."""
    result = (
        fpstreams.pairs([("a", 3), ("b", 4), ("a", 2)])
        .with_engine("python")
        .aggregate_values(
            count=fpstreams.agg.count(),
            total=fpstreams.agg.sum(),
            low=fpstreams.agg.min(),
            high=fpstreams.agg.max(),
        )
    )

    assert result == {
        "a": {"count": 2, "total": 5, "low": 2, "high": 3},
        "b": {"count": 1, "total": 4, "low": 4, "high": 4},
    }
    assert list(result) == ["a", "b"]
    assert list(result["a"]) == ["count", "total", "low", "high"]


def test_pair_aggregation_finish_mutated_during_consumption_remains_authoritative() -> None:
    """A value callback can still replace a finisher after aggregation starts."""
    aggregation = fpstreams.agg.sum()

    class Value:
        def __radd__(self, total: int) -> int:
            object.__setattr__(aggregation, "finish", lambda state: state * 10)
            return total + 2

    result = (
        fpstreams.pairs([("key", Value())])
        .with_engine("python")
        .aggregate_values(total=aggregation)
    )

    assert result == {"key": {"total": 20}}


def test_pair_row_expression_group_sum_attempts_native_before_clean_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Eligible retained i64 pair expressions try Rust without consuming Python fallback."""
    from fpstreams import _native

    source = [(3, 7), (1, 11), (4, 13), (8, 17)] * 8
    calls: list[tuple[object, object, object, object, object]] = []

    def decline(
        rows: object,
        key_program: object,
        value_program: object,
        key_name: object,
        output_name: object,
    ) -> None:
        calls.append((rows, key_program, value_program, key_name, output_name))
        return None

    monkeypatch.setattr(_native, "group_sum_i64_pair_expr_rows_v1", decline, raising=False)
    grouped = (
        fpstreams.rows(source)
        .group_by(key=col(0) % 3)
        .aggregate(total=fpstreams.agg.sum(col(1) * 2 - 1))
    )
    constant = (
        fpstreams.rows(source).group_by(key=col(0) % 3).aggregate(total=fpstreams.agg.sum(lit(3)))
    )

    assert grouped.to_list() == [
        {"key": 0, "total": 104},
        {"key": 1, "total": 368},
        {"key": 2, "total": 264},
    ]
    assert constant.to_list() == [
        {"key": 0, "total": 24},
        {"key": 1, "total": 48},
        {"key": 2, "total": 24},
    ]
    assert len(calls) == 2
    for native_source, key_program, value_program, key_name, output_name in calls:
        assert native_source is source
        assert key_program and value_program
        assert (key_name, output_name) == ("key", "total")


def test_pair_row_expression_group_sum_keeps_sub_crossover_sources_in_python(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A retained source below the measured crossover must avoid native startup overhead."""
    from fpstreams import _native

    def unexpected_native(*_arguments: object) -> object:
        raise AssertionError("a sub-crossover RowExpr group reached native execution")

    monkeypatch.setattr(
        _native,
        "group_sum_i64_pair_expr_rows_v1",
        unexpected_native,
        raising=False,
    )
    source = [(value, value + 1) for value in range(31)]

    result = (
        fpstreams.rows(source)
        .group_by(key=col(0) % 2)
        .aggregate(total=fpstreams.agg.sum(col(1) + 1))
        .to_list()
    )

    assert result == [{"key": 0, "total": 272}, {"key": 1, "total": 255}]


@pytest.mark.parametrize("replacement", ["open", "claim", "rowexpr_getattribute"])
def test_pair_row_expression_group_sum_observes_replaced_python_boundaries(
    monkeypatch: pytest.MonkeyPatch,
    replacement: str,
) -> None:
    """Replaced source and expression boundaries remain observable through Python fallback."""
    from fpstreams import _native
    from fpstreams.expressions.row import RowExpr

    source = [(value, value + 1) for value in range(32)]
    key = col(0) % 2
    value = col(1) + 1
    grouped = fpstreams.rows(source).group_by(key=key).aggregate(total=fpstreams.agg.sum(value))
    events: list[str] = []

    def unexpected_native(*_arguments: object) -> object:
        raise AssertionError("a replaced Python boundary reached native execution")

    if replacement == "open":
        original_open = Source.open

        def observed_open(owner: Source[Any]) -> Iterator[Any]:
            events.append("open")
            return original_open(owner)

        monkeypatch.setattr(Source, "open", observed_open)
    elif replacement == "claim":
        original_claim = Source._claim

        def observed_claim(owner: Source[Any]) -> None:
            events.append("claim")
            original_claim(owner)

        monkeypatch.setattr(Source, "_claim", observed_claim)
    else:
        original_getattribute = RowExpr.__getattribute__

        def observed_getattribute(expression: RowExpr, name: str) -> object:
            attribute = original_getattribute(expression, name)
            if name != "_evaluate":
                return attribute

            def observed_evaluate(row: object) -> object:
                events.append("evaluate")
                return attribute(row)

            return observed_evaluate

        monkeypatch.setattr(RowExpr, "__getattribute__", observed_getattribute)
    monkeypatch.setattr(
        _native,
        "group_sum_i64_pair_expr_rows_v1",
        unexpected_native,
        raising=False,
    )

    result = grouped.to_list()

    assert result == [{"key": 0, "total": 272}, {"key": 1, "total": 288}]
    assert events == ([replacement] if replacement != "rowexpr_getattribute" else ["evaluate"] * 64)


def test_pair_row_expression_group_sum_matches_python_negative_floor_division_and_modulo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The native arithmetic vocabulary follows Python for negative // and % operands."""
    from fpstreams import _native

    source = [(-7, -7), (-8, -8), (-1, -1), (7, 7), (8, 8), (1, 1)] * 6
    expected = [
        {"key": -3, "total": -18},
        {"key": -1, "total": -6},
        {"key": 2, "total": -18},
        {"key": 0, "total": -12},
    ]
    native_calls = 0
    native_kernel = _native.group_sum_i64_pair_expr_rows_v1

    def tracked_native(*arguments: object) -> object:
        nonlocal native_calls
        native_calls += 1
        return native_kernel(*arguments)

    monkeypatch.setattr(_native, "group_sum_i64_pair_expr_rows_v1", tracked_native)

    def run(engine: str) -> list[dict[str, int]]:
        return (
            fpstreams.rows(source)
            .with_engine(engine)
            .group_by(key=col(0) // 3)
            .aggregate(total=fpstreams.agg.sum(col(1) % -3))
            .to_list()
        )

    assert run("python") == expected
    assert run("auto") == expected
    assert native_calls == 1


@pytest.mark.parametrize("late_row", [[1, 5], {0: 1, 1: 5}])
def test_pair_row_expression_group_sum_replays_after_a_late_non_tuple_row(
    monkeypatch: pytest.MonkeyPatch,
    late_row: object,
) -> None:
    """A whole-container shape rejection restarts without losing prior groups or order."""
    from fpstreams import _native

    source = [(1, 2), (2, 3)] * 15 + [(1, 2), late_row]
    native_calls = 0
    native_kernel = _native.group_sum_i64_pair_expr_rows_v1

    def tracked_native(*arguments: object) -> object:
        nonlocal native_calls
        native_calls += 1
        return native_kernel(*arguments)

    monkeypatch.setattr(_native, "group_sum_i64_pair_expr_rows_v1", tracked_native)

    assert fpstreams.rows(source).group_by(key=col(0) % 2).aggregate(
        total=fpstreams.agg.sum(col(1) * 2)
    ).to_list() == [{"key": 1, "total": 74}, {"key": 0, "total": 90}]
    assert native_calls == 1


def test_pair_row_expression_group_sum_keeps_generators_and_python_engine_off_native(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One-shot sources and forced Python execution never enter the retained-sequence ABI."""
    from fpstreams import _native

    def unexpected_native(*_arguments: object) -> object:
        raise AssertionError("an ineligible RowExpr group reached native execution")

    monkeypatch.setattr(
        _native,
        "group_sum_i64_pair_expr_rows_v1",
        unexpected_native,
        raising=False,
    )
    generated = ((value, value + 1) for value in range(32))
    generator_result = (
        fpstreams.rows(generated)
        .group_by(key=col(0) % 2)
        .aggregate(total=fpstreams.agg.sum(col(1) + 1))
        .to_list()
    )
    python_result = (
        fpstreams.rows([(value, value + 1) for value in range(32)])
        .with_engine("python")
        .group_by(key=col(0) % 2)
        .aggregate(total=fpstreams.agg.sum(col(1) + 1))
        .to_list()
    )

    assert generator_result == [{"key": 0, "total": 272}, {"key": 1, "total": 288}]
    assert python_result == generator_result


def test_pair_row_expression_group_sum_declines_active_failpoints(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Active failpoints retain the observable Python execution path."""
    from fpstreams import _native
    from fpstreams.runtime.failpoints import failpoint

    def unexpected_native(*_arguments: object) -> object:
        raise AssertionError("an observed RowExpr group reached native execution")

    monkeypatch.setattr(
        _native,
        "group_sum_i64_pair_expr_rows_v1",
        unexpected_native,
        raising=False,
    )
    grouped = (
        fpstreams.rows([(value, value + 1) for value in range(32)])
        .group_by(key=col(0) % 2)
        .aggregate(total=fpstreams.agg.sum(col(1) + 1))
    )
    with failpoint("unrelated.rowexpr-group.transition", RuntimeError("unused")):
        result = grouped.to_list()

    assert result == [{"key": 0, "total": 272}, {"key": 1, "total": 288}]


@pytest.mark.parametrize("boolean_side", ["key", "value"])
def test_pair_row_expression_group_sum_keeps_boolean_roots_in_python(
    monkeypatch: pytest.MonkeyPatch,
    boolean_side: str,
) -> None:
    """Comparison roots retain bool keys/results instead of entering the integer ABI."""
    from fpstreams import _native

    def unexpected_native(*_arguments: object) -> object:
        raise AssertionError("a Boolean RowExpr root reached the i64 arithmetic kernel")

    monkeypatch.setattr(
        _native,
        "group_sum_i64_pair_expr_rows_v1",
        unexpected_native,
        raising=False,
    )
    source = [(-1, -2), (1, 3), (-2, 4)] * 11
    if boolean_side == "key":
        result = (
            fpstreams.rows(source)
            .group_by(key=col(0) > 0)
            .aggregate(total=fpstreams.agg.sum(col(1) + 0))
            .to_list()
        )
        assert result == [{"key": False, "total": 22}, {"key": True, "total": 33}]
        assert all(type(row["key"]) is bool for row in result)
    else:
        result = (
            fpstreams.rows(source)
            .group_by(key=col(0) + 0)
            .aggregate(total=fpstreams.agg.sum(col(1) > 0))
            .to_list()
        )
        assert result == [
            {"key": -1, "total": 0},
            {"key": 1, "total": 11},
            {"key": -2, "total": 11},
        ]
        assert all(type(row["total"]) is int for row in result)


def test_pair_row_expression_group_sum_preserves_non_arithmetic_key_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Direct indexes use the identity-safe kernel, while literal keys remain Python objects."""
    from fpstreams import _native

    def unexpected_expr_native(*_arguments: object) -> object:
        raise AssertionError("a direct or literal RowExpr key reached the arithmetic ABI")

    monkeypatch.setattr(
        _native,
        "group_sum_i64_pair_expr_rows_v1",
        unexpected_expr_native,
        raising=False,
    )
    first_key = int("5000")
    equal_key = int("5000")
    assert first_key is not equal_key
    direct = (
        fpstreams.rows([(first_key, 2), (equal_key, 3)] * 16)
        .group_by(key=col(0))
        .aggregate(total=fpstreams.agg.sum(col(1)))
        .to_list()
    )
    literal_key = int("4000000000000000000")
    literal = (
        fpstreams.rows([(1, 2), (2, 3)] * 16)
        .group_by(key=lit(literal_key))
        .aggregate(total=fpstreams.agg.sum(col(1) + 0))
        .to_list()
    )

    assert direct == [{"key": first_key, "total": 80}]
    assert direct[0]["key"] is first_key
    assert literal == [{"key": literal_key, "total": 80}]
    assert literal[0]["key"] is literal_key


def test_pair_row_expression_group_sum_preserves_modulo_dividend_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Modulo bucketing retains the first source integer when CPython returns it directly."""
    from fpstreams import _native

    first_key = int("5000")
    equal_key = int("5000")
    assert first_key is not equal_key
    source = [(first_key, 2), (equal_key, 3)] * 16
    native_calls = 0
    native_kernel = _native.group_sum_i64_pair_expr_rows_v1

    def tracked_native(*arguments: object) -> object:
        nonlocal native_calls
        native_calls += 1
        return native_kernel(*arguments)

    monkeypatch.setattr(_native, "group_sum_i64_pair_expr_rows_v1", tracked_native)

    def run(engine: str) -> list[dict[str, int]]:
        return (
            fpstreams.rows(source)
            .with_engine(engine)
            .group_by(key=col(0) % 1_000_000_000_000)
            .aggregate(total=fpstreams.agg.sum(col(1) + 0))
            .to_list()
        )

    expected = run("python")
    result = run("auto")

    assert result == expected == [{"key": first_key, "total": 80}]
    assert expected[0]["key"] is first_key
    assert result[0]["key"] is first_key
    assert native_calls == 1


def test_pair_row_expression_group_sum_respects_a_replaced_lazy_program(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A live LazyRowEvaluator cache remains authoritative over its original IR node."""
    from fpstreams import _native

    def unexpected_native(*_arguments: object) -> object:
        raise AssertionError("a replaced RowExpr evaluator program reached native execution")

    def constant_key(_row: object) -> int:
        return 99

    def constant_value(_row: object) -> int:
        return 10

    key = col(0) % 2
    value = col(1) + 0
    monkeypatch.setattr(key._evaluate, "_program", constant_key)
    monkeypatch.setattr(value._evaluate, "_program", constant_value)
    monkeypatch.setattr(
        _native,
        "group_sum_i64_pair_expr_rows_v1",
        unexpected_native,
        raising=False,
    )

    result = (
        fpstreams.rows([(1, 2), (2, 3), (3, 4)] * 11)
        .group_by(key=key)
        .aggregate(total=fpstreams.agg.sum(value))
        .to_list()
    )

    assert result == [{"key": 99, "total": 330}]


def test_pair_row_expression_group_sum_cleanly_replays_python_integer_edges(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bool subclasses, custom ints, bigints, and checked overflow keep Python semantics."""
    from fpstreams import _native

    calls: list[str] = []
    custom_key = object()
    native_calls = 0
    native_kernel = _native.group_sum_i64_pair_expr_rows_v1

    def tracked_native(*arguments: object) -> object:
        nonlocal native_calls
        native_calls += 1
        return native_kernel(*arguments)

    monkeypatch.setattr(_native, "group_sum_i64_pair_expr_rows_v1", tracked_native)

    class CustomInt(int):
        def __mod__(self, _other: object) -> object:
            calls.append("mod")
            return custom_key

    custom_result = (
        fpstreams.rows([(CustomInt(5), 2), (1, 3)] * 16)
        .group_by(key=col(0) % 2)
        .aggregate(total=fpstreams.agg.sum(col(1) + 0))
        .to_list()
    )
    huge = 1 << 80
    huge_result = (
        fpstreams.rows([(huge, huge)] * 32)
        .group_by(key=col(0) + 1)
        .aggregate(total=fpstreams.agg.sum(col(1) * 2))
        .to_list()
    )
    i64_max = (1 << 63) - 1
    overflow_result = (
        fpstreams.rows([(i64_max, 7)] * 32)
        .group_by(key=(col(0) + 1) - 1)
        .aggregate(total=fpstreams.agg.sum(col(1) + 0))
        .to_list()
    )
    bool_result = (
        fpstreams.rows([(True, 2), (1, 3)] * 16)
        .group_by(key=col(0) + 0)
        .aggregate(total=fpstreams.agg.sum(col(1) + 0))
        .to_list()
    )

    assert custom_result == [{"key": custom_key, "total": 32}, {"key": 1, "total": 48}]
    assert custom_result[0]["key"] is custom_key
    assert calls == ["mod"] * 16
    assert huge_result == [{"key": huge + 1, "total": huge * 64}]
    assert overflow_result == [{"key": i64_max, "total": 224}]
    assert bool_result == [{"key": 1, "total": 80}]
    assert type(bool_result[0]["key"]) is int
    assert native_calls == 4


def test_pair_row_expression_group_sum_reports_key_error_before_value_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A declined checked program replays the canonical key-before-value exception order."""
    from fpstreams import _native

    native_calls = 0
    native_kernel = _native.group_sum_i64_pair_expr_rows_v1

    def tracked_native(*arguments: object) -> object:
        nonlocal native_calls
        native_calls += 1
        return native_kernel(*arguments)

    monkeypatch.setattr(_native, "group_sum_i64_pair_expr_rows_v1", tracked_native)
    grouped = (
        fpstreams.rows([(0, 0)] * 32)
        .group_by(key=col(0) % 0)
        .aggregate(total=fpstreams.agg.sum(1 // col(1)))
    )

    with pytest.raises(ZeroDivisionError, match="modulo"):
        grouped.to_list()
    assert native_calls == 1


def test_pair_row_expression_group_sum_keeps_opaque_callbacks_in_python(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Auto execution preserves opaque key-before-value callbacks exactly once per row."""
    from fpstreams import _native
    from fpstreams.expressions.row import RowExpr

    def unexpected_native(*_arguments: object) -> object:
        raise AssertionError("an opaque RowExpr group reached native execution")

    monkeypatch.setattr(
        _native,
        "group_sum_i64_pair_expr_rows_v1",
        unexpected_native,
        raising=False,
    )
    calls: list[tuple[str, int]] = []
    key = RowExpr(lambda row: calls.append(("key", row[0])) or row[0] % 2, "key")
    value = RowExpr(lambda row: calls.append(("value", row[1])) or row[1] * 2, "value")

    result = (
        fpstreams.rows([(1, 2), (2, 3), (3, 4)] * 11)
        .group_by(key=key)
        .aggregate(total=fpstreams.agg.sum(value))
        .to_list()
    )

    assert result == [{"key": 1, "total": 132}, {"key": 0, "total": 66}]
    assert (
        calls
        == [
            ("key", 1),
            ("value", 2),
            ("key", 2),
            ("value", 3),
            ("key", 3),
            ("value", 4),
        ]
        * 11
    )
