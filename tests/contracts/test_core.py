"""Option and Result behavior, functional helpers, and finite-domain algebraic laws."""

from __future__ import annotations

import asyncio
import importlib.util
import itertools
import json
import os
import random
import re
import shlex
import subprocess
import sys
import textwrap
import typing
from pathlib import Path
from typing import Any, cast, get_args

import pytest

import fpstreams
from fpstreams import (
    AsyncStream,
    Collectors,
    Err,
    Option,
    ParallelStream,
    Result,
    Stream,
    aflow,
    curry,
    flow,
    item,
    pipe,
    retry,
)
from fpstreams.planning.sync import FilterOp, MapOp, Operation, TapOp

# --- Core values and source semantics ---


@pytest.mark.asyncio
async def test_async_source_facts_do_not_compare_user_metaclasses_or_call_len() -> None:
    class Meta(type):
        def __eq__(cls, other):
            raise AssertionError("user metaclass equality was called")

        __hash__ = type.__hash__

    class Values(metaclass=Meta):
        def __iter__(self):
            return iter([1, 2])

        def __len__(self):
            raise AssertionError("user length was called")

    query = aflow(Values())
    assert query.explain().to_dict()["semantics"]["source"]["cardinality"]["kind"] == "unknown"

    class MisleadingMeta(type):
        def __eq__(cls, other):
            return other is str

        __hash__ = type.__hash__

    class Misleading(metaclass=MisleadingMeta):
        def __iter__(self):
            return iter([1, 2])

        def __len__(self):
            return 99

    actual = aflow(Misleading())
    assert actual.explain().to_dict()["semantics"]["source"]["cardinality"]["kind"] == "unknown"
    assert await actual.to_list() == [1, 2]

    for value in ({1, 2}, frozenset({1, 2})):
        assert aflow(value).explain().to_dict()["semantics"]["source"]["cardinality"] == {
            "kind": "exact",
            "value": 2,
        }
    mutable = {1}
    query = aflow(mutable)
    mutable.add(2)
    assert query.explain().to_dict()["semantics"]["source"]["cardinality"]["value"] == 2


@pytest.mark.asyncio
async def test_explain_single_source_cardinality_transfers() -> None:
    for factory in (flow, aflow):
        for values, added, interspersed, group_kind, group_bound, taken in (
            ([], 2, 0, "exact", 0, 2),
            ([1], 3, 1, "upper_bound", 1, 2),
            ([1, 1, 2], 5, 5, "upper_bound", 3, 2),
        ):
            for query, count in (
                (factory(values).prepend(7, 8), added),
                (factory(values).append(7, 8), added),
                (factory(values).intersperse(0), interspersed),
            ):
                assert query.explain().to_dict()["semantics"]["output"]["cardinality"] == {
                    "kind": "exact",
                    "value": count,
                }
            groups = factory(values).group_runs(lambda x: x)
            assert groups.explain().to_dict()["semantics"]["output"]["cardinality"] == {
                "kind": group_kind,
                "value": group_bound,
            }
            assert factory(values).append(7, 8).take(2).explain().to_dict()["semantics"]["output"][
                "cardinality"
            ] == {"kind": "exact", "value": taken}
        folded = factory([1, 2]).fold(lambda: 0, lambda a, b: a + b)
        assert folded.explain().to_dict()["semantics"]["output"]["cardinality"]["kind"] != "exact"
    assert await aflow([1, 1, 2]).batch_by_size(3, get_size=lambda _: 1).to_list() == [(1, 1, 2)]
    assert aflow([1, 1, 2]).batch_by_size(3, get_size=lambda _: 1).explain().to_dict()["semantics"][
        "output"
    ]["cardinality"] == {"kind": "upper_bound", "value": 3}
    error = ValueError("bad size")

    def fail_size(_):
        raise error

    with pytest.raises(ValueError) as caught:
        await aflow([1]).batch_by_size(3, get_size=fail_size).to_list()
    assert caught.value is error
    assert flow([1, 1, 2]).group_runs(lambda x: x).to_list() == [(1, 1), (2,)]
    assert await aflow([1, 1, 2]).group_runs(lambda x: x).to_list() == [(1, 1), (2,)]

    gatherer = fpstreams.Gatherer(
        initializer=lambda: None,
        integrator=lambda _state, item: (item, -item),
        finisher=lambda _state: (99,),
    )
    for values, expected in (([], [99]), ([1, 2], [1, -1, 2, -2, 99])):
        query = flow(values).gather(gatherer)
        assert query.explain().to_dict()["semantics"]["output"]["cardinality"] == {
            "kind": "unknown",
            "value": None,
        }
        assert query.to_list() == expected


@pytest.mark.asyncio
async def test_explain_unknown_custom_replay_and_short_circuit() -> None:
    class Shared:
        def __init__(self):
            self.cursor = iter([1, 2])
            self.opens = 0

        def __iter__(self):
            self.opens += 1
            return self.cursor

    for factory in (flow, aflow):
        source = Shared()
        query = factory(source)
        assert query.explain().to_dict()["semantics"]["source"]["replayability"] == "unknown"
        assert source.opens == 0
        assert (
            factory([1]).explain().to_dict()["semantics"]["source"]["replayability"] == "reiterable"
        )
        assert (
            factory(iter([1])).explain().to_dict()["semantics"]["source"]["replayability"]
            == "one_shot"
        )
        assert (
            factory.defer(lambda: [1]).explain().to_dict()["semantics"]["source"]["replayability"]
            == "reopenable"
        )
    shared = Shared()
    assert flow(shared).to_list() == [1, 2]
    assert flow(shared).to_list() == []
    shared = Shared()
    query = aflow(shared)
    assert await query.to_list() == [1, 2]
    assert await query.to_list() == []

    class Fresh:
        def __iter__(self):
            return iter([1, 2])

    for factory in (flow, aflow):
        assert (
            factory(Fresh()).explain().to_dict()["semantics"]["source"]["replayability"]
            == "unknown"
        )
    query = flow(Fresh())
    assert query.to_list() == query.to_list() == [1, 2]
    query_async = aflow(Fresh())
    assert await query_async.to_list() == await query_async.to_list() == [1, 2]
    assert not flow([0, None]).any()
    infinite = Stream.iterate(0, lambda x: x + 1)
    assert (
        infinite.filter(lambda _: False)
        .take(1)
        .explain()
        .to_dict()["semantics"]["output"]["termination"]
        == "unknown"
    )
    assert (
        infinite.filter(bool).take(0).explain().to_dict()["semantics"]["output"]["termination"]
        == "proven_finite"
    )
    assert infinite.filter(lambda x: x == 2).take(1).to_list() == [2]
    assert (
        flow([0, 1]).filter(bool).take(1).explain().to_dict()["semantics"]["output"]["termination"]
        == "proven_finite"
    )
    async_infinite = AsyncStream.interval(0)
    assert (
        async_infinite.filter(lambda _: False)
        .take(1)
        .explain()
        .to_dict()["semantics"]["output"]["termination"]
        == "unknown"
    )
    assert await async_infinite.filter(lambda x: x == 2).take(1).to_list() == [2]
    assert await aflow([0, 1]).filter(bool).take(1).to_list() == [1]


@pytest.mark.asyncio
async def test_explain_exact_count_uses_current_facts_and_does_not_run_callbacks() -> None:
    calls = []
    failure = ValueError("callback failed")

    def mapping(value):
        calls.append(value)
        if value == 2:
            raise failure
        return value

    for factory in (flow, aflow):
        values = [1, 2]
        query = factory(values).map(mapping).append(7)
        original = query.explain().to_dict()["semantics"]["output"]["cardinality"]
        assert original == {"kind": "exact", "value": 3}
        assert calls == []
        values.append(3)
        updated = query.explain().to_dict()["semantics"]["output"]["cardinality"]
        assert updated == {"kind": "exact", "value": 4}
        assert original == {"kind": "exact", "value": 3}
        with pytest.raises(ValueError) as caught:
            if factory is flow:
                query.to_list()
            else:
                await query.to_list()
        assert caught.value is failure
        assert calls == [1, 2]
        calls.clear()


@pytest.mark.asyncio
async def test_explain_zip_with_unproven_peer_termination() -> None:
    started = asyncio.Event()
    released = asyncio.Event()

    async def stalled():
        started.set()
        await released.wait()
        yield 1

    for method in ("zip", "zip_longest"):
        query = getattr(aflow([1]), method)(stalled())
        assert query.explain().to_dict()["semantics"]["output"]["termination"] == "unknown"
        task = asyncio.create_task(query.to_list())
        await started.wait()
        assert not task.done()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        started.clear()

    for factory in (flow, aflow):
        assert (
            factory([]).zip(iter([1])).explain().to_dict()["semantics"]["output"]["termination"]
            == "unknown"
        )
        assert (
            factory([])
            .zip(iter([1]), strict=True)
            .explain()
            .to_dict()["semantics"]["output"]["termination"]
            == "unknown"
        )
        assert (
            factory(iter([1])).zip([]).explain().to_dict()["semantics"]["output"]["termination"]
            == "unknown"
        )
        assert (
            factory([1])
            .zip(iter([2]))
            .take(0)
            .explain()
            .to_dict()["semantics"]["output"]["termination"]
            == "proven_finite"
        )
        assert (
            factory([1]).zip([2]).explain().to_dict()["semantics"]["output"]["termination"]
            == "proven_finite"
        )

    failure = RuntimeError("right opener called")

    class Right:
        def __iter__(self):
            raise failure

    sync = flow([]).zip(Right())
    asynchronous = aflow([]).zip(Right())
    for query in (sync, asynchronous):
        assert query.explain().to_dict()["semantics"]["output"]["termination"] == "unknown"
    with pytest.raises(RuntimeError) as caught:
        sync.to_list()
    assert caught.value is failure
    with pytest.raises(RuntimeError) as caught:
        await asynchronous.to_list()
    assert caught.value is failure

    assert (
        flow.iterate(0, lambda x: x + 1)
        .zip_longest(iter([1]))
        .explain()
        .to_dict()["semantics"]["output"]["termination"]
        == "unknown"
    )


@pytest.mark.asyncio
async def test_explain_cross_depends_on_right_not_infinite_left() -> None:
    from fpstreams.errors import BufferLimitError

    infinite = Stream.iterate(0, lambda x: x + 1)
    query = infinite.cross([3, 4]).take(1)
    explanation = query.explain().to_dict()
    cross = explanation["semantics"]["operations"][0]
    assert cross["completion_dependencies"][0]["label"] == "right (if left is nonempty)"
    assert not any(d["code"] == "NON_TERMINATING_PLAN" for d in explanation["diagnostics"])
    assert not any(d["code"] == "STATE_MAY_GROW" for d in explanation["diagnostics"])
    assert query.to_list() == [(0, 3)]
    for factory in (flow, aflow):
        assert (
            factory([])
            .cross([1])
            .explain()
            .to_dict()["semantics"]["operations"][0]["completion_dependencies"]
            == []
        )
        limited = factory([1]).cross([2, 3], max_right=1)
        assert limited.explain().to_dict()["semantics"]["output"]["cardinality"] == {
            "kind": "upper_bound",
            "value": 0,
        }
    with pytest.raises(BufferLimitError, match="max_right"):
        flow([1]).cross([2, 3], max_right=1).to_list()
    with pytest.raises(BufferLimitError, match="max_right"):
        await aflow([1]).cross([2, 3], max_right=1).to_list()

    opened = []

    def right():
        opened.append("open")
        try:
            yield 2
            yield 3
        finally:
            opened.append("close")

    assert flow([]).cross(right()).to_list() == []
    assert opened == []
    with pytest.raises(BufferLimitError):
        flow([1]).cross(right(), max_right=1).to_list()
    assert opened == ["open", "close"]
    opened.clear()
    assert await aflow([]).cross(right()).to_list() == []
    assert opened == []
    with pytest.raises(BufferLimitError):
        await aflow([1]).cross(right(), max_right=1).to_list()
    assert opened == ["open", "close"]

    opened.clear()

    def infinite_right():
        try:
            for item in itertools.count():
                opened.append(item)
                yield item
        finally:
            opened.append("close")

    limited = flow([1]).cross(infinite_right(), max_right=2)
    assert limited.explain().to_dict()["semantics"]["operations"][0]["state"]["bound"] == 2
    with pytest.raises(BufferLimitError):
        limited.to_list()
    assert opened == [0, 1, 2, "close"]
    opened.clear()
    limited_async = aflow([1]).cross(infinite_right(), max_right=2)
    assert limited_async.explain().to_dict()["semantics"]["operations"][0]["state"]["bound"] == 2
    with pytest.raises(BufferLimitError):
        await limited_async.to_list()
    assert opened == [0, 1, 2, "close"]

    opened.clear()

    async def async_infinite_right():
        try:
            for item in itertools.count():
                opened.append(item)
                yield item
        finally:
            opened.append("close")

    with pytest.raises(BufferLimitError):
        await aflow([1]).cross(async_infinite_right(), max_right=2).to_list()
    assert opened == [0, 1, 2, "close"]


def test_live_source_size_refresh_preserves_explicit_semantic_facts() -> None:
    """Refreshing cardinality must not reconstruct unrelated source guarantees."""
    from dataclasses import replace

    from fpstreams.planning.semantics import (
        Cardinality,
        OrderingGuarantee,
        TerminationEvidence,
        facts_from_capabilities,
    )
    from fpstreams.planning.source import Source, SourceCapabilities

    values = [1, 2, 3]
    configured = facts_from_capabilities(
        reiterable=True,
        exact_size=None,
        ordered=False,
        reopenable=True,
    )
    source = Source(
        lambda: iter(values),
        SourceCapabilities(reiterable=True, exact_size=None),
        facts=configured,
        live_size_data=values,
    )

    assert source.current_facts() == replace(
        configured,
        termination=TerminationEvidence.PROVEN_FINITE,
        cardinality=Cardinality.exact(3),
    )

    source._factory = lambda: iter({3, 2, 1})
    assert source.current_facts() == replace(
        configured,
        termination=TerminationEvidence.UNKNOWN,
        cardinality=Cardinality.unknown(),
        ordering=OrderingGuarantee.UNKNOWN,
    )


def test_option_paths_present_and_empty() -> None:
    assert Option.of_nullable(10).filter(lambda x: x > 3).or_else(0) == 10
    assert Option.empty().map(lambda x: x).or_else(99) == 99


def test_option_of_none_is_rejected() -> None:
    with pytest.raises(ValueError):
        Option.of(None)  # type: ignore[arg-type]


def test_result_success_and_failure_paths() -> None:
    assert Result.success(5).map(lambda x: x + 1).get_or_else(0) == 6
    failure = Result.of(lambda: 1 / 0)
    assert failure.is_failure()


def test_err_requires_an_exception_argument() -> None:
    with pytest.raises(TypeError):
        cast(Any, Err)()


def test_pipe_and_curry_examples() -> None:
    assert pipe(3, lambda x: x + 2, lambda x: x * 4) == 20

    @curry
    def add3(a: int, b: int, c: int) -> int:
        return a + b + c

    assert add3(1)(2)(3) == 6
    assert add3(1, 2, 3) == 6


def test_curry_calls_after_all_required_parameters_are_bound() -> None:
    def scale(value: int, factor: int = 2) -> int:
        return value * factor

    assert curry(scale)(3) == 6


def test_curry_accepts_callables_without_code_objects() -> None:
    assert curry(pow)(2)(3) == 8


async def test_retry_recovers_transient_failure() -> None:
    state = {"attempt": 0}

    @retry(attempts=3, backoff=1.0, jitter=False)
    async def flaky() -> int:
        state["attempt"] += 1
        if state["attempt"] < 2:
            raise RuntimeError("transient")
        return 42

    assert await flaky() == 42
    assert state["attempt"] == 2


def test_theorem_option_functor_identity_and_composition_on_finite_domain() -> None:
    domain = [None, -2, -1, 0, 1, 2]

    def f(value: int) -> int:
        return value + 3

    def g(value: int) -> int:
        return value * 2

    for v in domain:
        opt = Option.of_nullable(v)
        # Identity: map(id) == self
        assert opt.map(lambda x: x).or_else(None) == opt.or_else(None)
        # Composition: map(f).map(g) == map(g∘f)
        lhs = opt.map(f).map(g).or_else(None)
        rhs = opt.map(lambda x: g(f(x))).or_else(None)
        assert lhs == rhs


def test_theorem_result_monad_left_and_right_identity_finite_domain() -> None:
    domain = [-2, -1, 0, 1, 2]

    def f(x: int) -> Result[int]:
        return Result.success(x * 10)

    for x in domain:
        # Left identity: return x >>= f == f x
        assert Result.success(x).flat_map(f).get_or_else(0) == f(x).get_or_else(0)

        # Right identity: m >>= return == m
        m = Result.success(x)
        assert m.flat_map(Result.success).get_or_else(0) == m.get_or_else(0)


def test_theorem_stream_sum_homomorphism_under_concatenation() -> None:
    left = [1, 2, 3]
    right = [4, 5]
    assert Stream(left + right).sum() == Stream(left).sum() + Stream(right).sum()


# --- Public compatibility ---

"""Legacy compatibility, integration behavior, coverage tracing, and package exports."""


def _times_two(x: int) -> int:
    return x * 2


def test_smoke_imports_and_basic_pipeline() -> None:
    assert Stream([1, 2, 3]).map(lambda x: x + 1).to_list() == [2, 3, 4]
    assert ParallelStream([1, 2, 3]).map(_times_two).to_list() == [2, 4, 6]
    assert Collectors.joining(",")([1, 2, 3]) == "1,2,3"
    assert Option.of(1).map(lambda x: x + 1).or_else(0) == 2
    assert Result.success(1).map(lambda x: x + 1).get_or_else(0) == 2


async def test_smoke_async_stream_runs() -> None:
    async def scenario() -> list[int]:
        return await AsyncStream.of(1, 2, 3).map(lambda x: x + 10).to_list()

    assert await scenario() == [11, 12, 13]


def test_unit_transformations_cover_empty_single_many() -> None:
    assert Stream([]).map(lambda x: x).to_list() == []
    assert Stream([5]).filter(lambda x: x > 0).to_list() == [5]
    assert Stream([1, 2, 3, 4]).flat_map(lambda x: [x, -x]).to_list() == [
        1,
        -1,
        2,
        -2,
        3,
        -3,
        4,
        -4,
    ]


def test_unit_limit_skip_and_window_edges() -> None:
    assert Stream([1, 2, 3]).limit(0).to_list() == []
    assert Stream([1, 2, 3]).skip(10).to_list() == []
    assert Stream([1, 2, 3, 4]).window(2).to_list() == [(1, 2), (2, 3), (3, 4)]


def test_unit_terminal_operations() -> None:
    s = Stream([3, 1, 2])
    assert s.sorted().to_list() == [1, 2, 3]
    assert Stream([3, 1, 2]).min() == 1
    assert Stream([3, 1, 2]).max() == 3
    assert Stream([3, 1, 2]).sum() == 6


def test_unit_collectors_grouping_partitioning_and_columns() -> None:
    grouped = Stream(["aa", "b", "cc"]).collect(Collectors.grouping_by(len))
    assert grouped == {2: ["aa", "cc"], 1: ["b"]}

    partitioned = Stream([1, 2, 3, 4]).collect(Collectors.partitioning_by(lambda x: x % 2 == 0))
    assert partitioned[True] == [2, 4]
    assert partitioned[False] == [1, 3]

    columns = Stream([{"a": 1}, {"a": 2, "b": 3}]).collect(Collectors.to_columns())
    assert columns["a"] == [1, 2]
    assert columns["b"] == [None, 3]


def test_integration_etl_style_pipeline_with_collectors() -> None:
    rows = [
        {"dept": "eng", "salary": 100},
        {"dept": "eng", "salary": 150},
        {"dept": "sales", "salary": 80},
    ]

    report = Stream(rows).collect(
        Collectors.grouping_by(
            lambda row: row["dept"],
            Collectors.mapping(
                lambda row: row["salary"],
                Collectors.averaging(lambda x: float(x)),
            ),
        )
    )

    assert report == {"eng": 125.0, "sales": 80.0}


def test_integration_result_partition_and_unwrap() -> None:
    events = [
        Result.success({"id": 1, "ok": True}),
        Result.failure(ValueError("bad payload")),
        Result.success({"id": 2, "ok": True}),
    ]

    ok, errors = Stream(events).partition_results()
    assert [row["id"] for row in ok] == [1, 2]
    assert len(errors) == 1


async def test_integration_async_bridge() -> None:
    async def scenario() -> list[int]:
        return (
            await Stream([1, 2, 3])
            .to_async()
            .map(lambda x: x + 1)
            .map_async(lambda x: asyncio.sleep(0, result=x * 10))
            .to_list()
        )

    assert await scenario() == [20, 30, 40]


def test_coverage_threshold_if_enabled() -> None:
    minimum = os.environ.get("COVERAGE_MIN")
    running_under_coverage = os.environ.get("COV_CORE_SOURCE") is not None

    if minimum is None and not running_under_coverage:
        pytest.skip("Coverage gate is enabled only in coverage-aware runs.")

    # Either coverage signal above must correspond to an installed trace function.
    import sys

    assert sys.gettrace() is not None


def test_domain_packages_export_the_stable_public_api() -> None:
    from fpstreams.collecting import Aggregator, Collector
    from fpstreams.expressions import Expr, RowExpr
    from fpstreams.primitives import Option, Result
    from fpstreams.streams import AsyncFlow, Flow, Gatherer, Pairs
    from fpstreams.tabular import Rows

    assert (Flow, AsyncFlow, Pairs, Gatherer) == (
        fpstreams.Flow,
        fpstreams.AsyncFlow,
        fpstreams.Pairs,
        fpstreams.Gatherer,
    )
    assert (Collector, Aggregator) == (fpstreams.Collector, fpstreams.Aggregator)
    assert (Rows, Expr, RowExpr) == (fpstreams.Rows, fpstreams.Expr, fpstreams.RowExpr)
    assert (Option, Result) == (fpstreams.Option, fpstreams.Result)


def test_public_gatherer_annotations_resolve_at_runtime() -> None:
    hints = typing.get_type_hints(fpstreams.Gatherer)

    assert {
        "initializer",
        "integrator",
        "finisher",
        "combiner",
        "greedy",
    } <= hints.keys()


def test_importing_discoverable_submodules_preserves_root_factories() -> None:
    script = textwrap.dedent(
        """
        import importlib
        import pkgutil

        import fpstreams

        for module in pkgutil.iter_modules(fpstreams.__path__, "fpstreams."):
            importlib.import_module(module.name)

        assert fpstreams.flow([1, 2]).to_list() == [1, 2]
        assert fpstreams.rows([{"id": 1}]).to_list() == [{"id": 1}]
        assert fpstreams.pairs([("id", 1)]).to_dict() == {"id": 1}
        """
    )

    completed = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr


def test_legacy_module_imports_remain_compatible() -> None:
    from fpstreams.aggregate import Aggregator
    from fpstreams.async_flow import AsyncFlow
    from fpstreams.collectors import Collector
    from fpstreams.option import Option
    from fpstreams.result import Result

    assert (AsyncFlow, Collector, Aggregator, Option, Result) == (
        fpstreams.AsyncFlow,
        fpstreams.Collector,
        fpstreams.Aggregator,
        fpstreams.Option,
        fpstreams.Result,
    )


# --- Finite-domain invariants ---

"""Randomized, property, finite-state, and operation-dispatch invariant tests."""


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
    spec = importlib.util.find_spec("fpstreams.execution.asynchronous.operators")
    assert spec is not None, "the focused asynchronous dispatch module must exist"

    from fpstreams.execution.asynchronous.operators import (
        ASYNC_OPERATION_HANDLERS,
        SUPPORTED_ASYNC_OPERATION_TYPES,
    )
    from fpstreams.planning.async_ import (
        AsyncOperation,
        _BufferTimeout,
        _CombineLatest,
        _Debounce,
        _Delay,
        _MapAsync,
        _Merge,
        _MergeMap,
        _Prefetch,
        _SessionWindow,
        _SwitchMap,
        _Throttle,
        _Timeout,
    )

    assert len(SUPPORTED_ASYNC_OPERATION_TYPES) == len(set(SUPPORTED_ASYNC_OPERATION_TYPES))
    assert tuple(ASYNC_OPERATION_HANDLERS) == SUPPORTED_ASYNC_OPERATION_TYPES
    specialized = {
        _MapAsync,
        _Merge,
        _MergeMap,
        _Prefetch,
        _SessionWindow,
        _SwitchMap,
        _CombineLatest,
        _Timeout,
        _Debounce,
        _BufferTimeout,
        _Delay,
        _Throttle,
    }
    assert set(SUPPORTED_ASYNC_OPERATION_TYPES) == set(get_args(AsyncOperation)) - specialized


@pytest.mark.asyncio
async def test_async_operation_dispatch_applies_take_operation() -> None:
    import fpstreams.execution.asynchronous.operators as async_ops
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
    from fpstreams.execution.asynchronous.operators import apply_async_operation
    from fpstreams.planning.async_ import AsyncOperation

    async def source():
        if False:
            yield None

    with pytest.raises(TypeError, match="unsupported asynchronous operation: object"):
        apply_async_operation(source(), cast(AsyncOperation, object()))


@pytest.mark.asyncio
async def test_async_stateless_dispatch_preserves_callable_order() -> None:
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

    iterator = aflow(source()).map_async(mapped, concurrency=1).tap(tapped).filter(accepted)

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


# --- Release workflow safeguards ---

"""Release smoke, action pinning, and publish-workflow ordering safeguards."""


ROOT = Path(__file__).resolve().parents[2]
PINNED_ACTION = re.compile(
    r"^\s*- uses: [^@\s]+@[0-9a-f]{40}\s+#\s+\S+.*$",
    re.MULTILINE,
)


def test_release_smoke_checks_native_and_python_backends() -> None:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT / "src")

    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "smoke_release.py")],
        cwd=ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "native": [1, 3, 5, 7],
        "python": [1, 3, 5, 7],
        "version": fpstreams.__version__,
        "versions": dict.fromkeys(["distribution", "python", "native"], fpstreams.__version__),
        "orders": [
            {"region": "eu", "orders": 2, "revenue": 48},
            {"region": "us", "orders": 1, "revenue": 20},
        ],
        "async": [20, 30, 40],
    }


def test_release_version_markers_are_consistent() -> None:
    script = ROOT / "scripts" / "check_release_version.py"
    version = fpstreams.__version__

    valid = subprocess.run(
        [sys.executable, str(script), "--expected", f"v{version}"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    invalid = subprocess.run(
        [sys.executable, str(script), "--expected", "9.9.9"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert valid.returncode == 0, valid.stderr
    assert valid.stdout.strip() == version
    assert invalid.returncode != 0
    assert "expected 9.9.9" in invalid.stderr


def test_external_github_actions_are_pinned_to_documented_commits() -> None:
    workflows = sorted((ROOT / ".github" / "workflows").glob("*.yml"))
    unpinned: list[str] = []
    for workflow in workflows:
        text = workflow.read_text(encoding="utf-8")
        uses_lines = [line for line in text.splitlines() if "- uses:" in line]
        unpinned.extend(
            f"{workflow.name}: {line.strip()}"
            for line in uses_lines
            if PINNED_ACTION.fullmatch(line) is None
        )

    assert not unpinned, "unpinned actions:\n" + "\n".join(unpinned)


def test_core_engine_cases_run_without_optional_arrow(tmp_path) -> None:
    """Missing Arrow skips its cases while core engine regressions still execute."""
    import os
    import subprocess
    import sys

    script = tmp_path / "without_arrow.py"
    script.write_text(
        """
import sys
import pytest

class WithoutArrow:
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "pyarrow" or fullname.startswith("pyarrow."):
            raise ModuleNotFoundError("Arrow intentionally unavailable", name=fullname)

sys.meta_path.insert(0, WithoutArrow())

class Outcomes:
    def __init__(self):
        self.results = {}
    def pytest_runtest_logreport(self, report):
        if report.when == "call" or (report.when == "setup" and report.skipped):
            self.results[report.nodeid] = report.outcome

outcomes = Outcomes()
status = pytest.main([
    "-q", "-W", "error", "-p", "pytest_asyncio.plugin",
    "tests/execution/test_planning.py::test_cache_capacity_rejects_noninteger_values",
    "tests/execution/test_planning.py::test_cache_capacity_snapshots_and_releases_an_integer_protocol_object",
    "tests/execution/test_native_pairs.py::test_arrow_dictionary_guard_decline_does_not_reopen_native_data",
], plugins=[outcomes])
assert status == 0, status
passed = [node for node, result in outcomes.results.items() if result == "passed"]
skipped = [node for node, result in outcomes.results.items() if result == "skipped"]
assert len(passed) == 10, outcomes.results
assert all("::test_cache_capacity_" in node for node in passed), outcomes.results
assert len(skipped) == 1, outcomes.results
assert skipped[0].endswith("::test_arrow_dictionary_guard_decline_does_not_reopen_native_data")
""",
        encoding="utf-8",
    )
    environment = {**os.environ, "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1"}
    result = subprocess.run(
        [sys.executable, "-I", str(script)],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("job", ["platform-smoke", "free-threaded"])
def test_ci_smoke_selection_covers_cache_coordination_and_cleanup(job: str) -> None:
    """Collect the actual workflow command so renamed tests cannot silently disappear."""
    workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    section = re.search(
        rf"^  {re.escape(job)}:\n(.*?)(?=^  \S|\Z)", workflow, re.MULTILINE | re.DOTALL
    )
    assert section is not None, f"missing CI job: {job}"
    commands = []
    for line in section[1].splitlines():
        command = line.strip().removeprefix("run: ")
        if not command.startswith("uv run pytest "):
            continue
        arguments = shlex.split(command)
        if arguments[:3] == ["uv", "run", "pytest"] and any(
            value.startswith("tests/execution/test_planning.py") for value in arguments[3:]
        ):
            commands.append(arguments[3:])
    assert len(commands) == 1, f"expected one engine contract selection in {job}"
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", *commands[0]],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    selected = {
        line for line in result.stdout.splitlines() if line.startswith("tests/") and "::" in line
    }
    cache = "tests/execution/test_planning.py::test_kernel_cache_coordinates_concurrent_compilation"
    assert {f"{cache}[shared-result]", f"{cache}[retry-after-failure]"} <= selected
    if job == "platform-smoke":
        cleanup = (
            "tests/execution/test_runtime.py::test_cleanup_continues_when_exception_message_raises["
        )
        assert sum(node.startswith(cleanup) for node in selected) == 10
        assert (
            "tests/execution/test_runtime.py::"
            "test_cleanup_diagnostics_preserve_custom_exception_formatting"
        ) in selected
        assert (
            "tests/streams/test_async.py::"
            "test_cancellation_survives_unprintable_source_cleanup_error"
        ) in selected
        checkpoint = "tests/execution/test_runtime.py::test_file_checkpoint_"
        assert {
            checkpoint + "stage_cleanup_keeps_callback_failure[ValueError]",
            checkpoint + "stage_cleanup_keeps_callback_failure[KeyboardInterrupt]",
            checkpoint + "lock_close_keeps_callback_failure",
            checkpoint + "postpublication_cleanup_error_keeps_verified_result",
        } <= selected
        writer = "tests/tabular/test_rows_io.py::test_atomic_text_export_preserves_primary_error["
        assert sum(node.startswith(writer) for node in selected) == 20
        reader = (
            "tests/streams/test_async.py::test_async_file_cleanup_preserves_read_error_and_cancel["
        )
        assert sum(node.startswith(reader) for node in selected) == 4
        file_io = (
            "tests/streams/test_async.py::test_async_file_cancel_drains_inflight_thread_operation["
        )
        assert sum(node.startswith(file_io) for node in selected) == 6
        signature = (
            "tests/tabular/test_rows_io.py::"
            "test_borrowed_jsonl_signature_is_only_consumed_at_stream_start["
        )
        assert sum(node.startswith(signature) for node in selected) == 24
        byteorder = (
            "tests/tabular/test_rows_io.py::"
            "test_borrowed_wide_jsonl_requires_explicit_byte_order_without_bom["
        )
        assert sum(node.startswith(byteorder) for node in selected) == 24
        newlines = (
            "tests/tabular/test_rows_io.py::"
            "test_borrowed_jsonl_line_endings_do_not_depend_on_record_limit["
        )
        assert sum(node.startswith(newlines) for node in selected) == 24
        text_newlines = (
            "tests/tabular/test_rows_io.py::test_borrowed_jsonl_text_keeps_caller_newline_policy["
        )
        assert sum(node.startswith(text_newlines) for node in selected) == 2
        scope_cleanup = (
            "tests/streams/test_async.py::"
            "test_scope_cleanup_submission_failure_cancels_before_source_close["
        )
        assert sum(node.startswith(scope_cleanup) for node in selected) == 8
        shared_drain = (
            "tests/execution/test_runtime.py::"
            "test_scope_cleanup_submission_failure_waits_for_cancellation["
        )
        assert sum(node.startswith(shared_drain) for node in selected) == 2
        switch_admission = (
            "tests/streams/test_async.py::"
            "test_switch_map_initial_task_failure_cancels_and_closes_owned_source["
        )
        assert sum(node.startswith(switch_admission) for node in selected) == 8
        root_cleanup = (
            "tests/streams/test_async.py::"
            "test_async_root_cleanup_continues_across_unstarted_and_completed_stages["
        )
        assert sum(node.startswith(root_cleanup) for node in selected) == 12
        separate_traversal = (
            "tests/streams/test_async.py::"
            "test_async_root_cleanup_continues_with_separate_traversal["
        )
        assert sum(node.startswith(separate_traversal) for node in selected) == 2
        assert (
            "tests/streams/test_async.py::"
            "test_async_root_cleanup_continues_without_retaining_closed_source[append]"
        ) in selected
        sync_controls = (
            "tests/streams/test_async.py::"
            "test_async_sync_adapter_cancellation_and_stop_errors_keep_cleanup["
        )
        assert sum(node.startswith(sync_controls) for node in selected) == 20
        inner_hooks = (
            "tests/streams/test_async.py::"
            "test_async_inner_cleanup_continues_with_optional_close_hooks["
        )
        assert sum(node.startswith(inner_hooks) for node in selected) == 24
        inner_snapshot = (
            "tests/streams/test_async.py::"
            "test_async_inner_cleanup_continues_with_acquisition_hook_snapshot["
        )
        assert sum(node.startswith(inner_snapshot) for node in selected) == 6
        inner_errors = (
            "tests/streams/test_async.py::"
            "test_async_inner_cleanup_continues_preserving_sync_close_errors["
        )
        assert sum(node.startswith(inner_errors) for node in selected) == 4
        parallel_rejection = (
            "tests/streams/test_flow_engines.py::"
            "test_parallel_configuration_rejects_noninteger_bounds_before_source_open["
        )
        assert sum(node.startswith(parallel_rejection) for node in selected) == 16
        parallel_snapshot = (
            "tests/streams/test_flow_engines.py::"
            "test_parallel_configuration_normalizes_integer_bounds_once_before_source_open["
        )
        assert sum(node.startswith(parallel_snapshot) for node in selected) == 4
        root_shutdown = (
            "tests/streams/test_async.py::"
            "test_async_root_cleanup_continues_after_runtime_closes_root["
        )
        assert sum(node.startswith(root_shutdown) for node in selected) == 2
        assert (
            "tests/streams/test_async.py::"
            "test_async_sync_adapter_cleanup_continues_releasing_traversal_before_owner" in selected
        )


@pytest.mark.parametrize("job", ["linux", "musllinux", "windows", "macos", "sdist"])
def test_publish_smoke_checks_isolated_installed_files(job: str) -> None:
    """Every artifact lane verifies the installed files with isolated Python."""
    workflow = (ROOT / ".github/workflows/publish.yml").read_text(encoding="utf-8")
    section = re.search(
        rf"^  {re.escape(job)}:\n(.*?)(?=^  \S|\Z)", workflow, re.MULTILINE | re.DOTALL
    )
    assert section is not None
    calls = [line for line in section[1].splitlines() if "smoke_release.py" in line]
    assert len(calls) == 1
    assert re.search(r"\s-I\s+.*smoke_release\.py.*--require-installed", calls[0])


def test_publish_only_receives_credentials_after_artifact_verification() -> None:
    workflow = (ROOT / ".github" / "workflows" / "publish.yml").read_text(encoding="utf-8")

    assert workflow.count("id-token: write") == 1
    assert "needs: manifest" in workflow
    assert "Smoke-test wheel" in workflow
    assert "Build and smoke-test sdist" in workflow
    qualification = workflow.split("  qualification:\n", 1)[1].split("  validate:\n", 1)[0]
    assert "uses: ./.github/workflows/ci.yml" in qualification
    assert "qualification: true" in qualification
    validation = workflow.split("  validate:\n", 1)[1].split("  linux:\n", 1)[0]
    assert "needs: qualification" in validation
    assert "needs: validate" in workflow.split("  linux:\n", 1)[1].split("  musllinux:\n", 1)[0]
    ci = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    assert "inputs.qualification ||" in ci.split("  quality:\n", 1)[1]
    assert "uv run pytest -W error --cov=src/fpstreams --cov-branch" in ci
    assert "cargo test --manifest-path rust/Cargo.toml" in ci
    assert "sha256sum * > ../SHA256SUMS" in workflow
    assert 'gh release upload "$RELEASE_TAG" dist/SHA256SUMS dist/packages/*' in workflow
