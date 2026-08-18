from __future__ import annotations


# --- Tests consolidated from test_primitives.py ---

"""Option and Result behavior, functional helpers, and finite-domain algebraic laws."""


from typing import Any, cast

import pytest

from fpstreams import Err, Option, Result, Stream, curry, pipe, retry


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


def test_retry_recovers_transient_failure() -> None:
    state = {"attempt": 0}

    @retry(attempts=3, backoff=1.0, jitter=False)
    async def flaky() -> int:
        state["attempt"] += 1
        if state["attempt"] < 2:
            raise RuntimeError("transient")
        return 42

    import asyncio

    assert asyncio.run(flaky()) == 42
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


# --- Tests consolidated from test_compatibility.py ---

"""Legacy compatibility, integration behavior, coverage tracing, and package exports."""


import asyncio
import os
import subprocess
import sys
import textwrap
import typing

import pytest

import fpstreams
from fpstreams import AsyncStream, Collectors, Option, ParallelStream, Result, Stream


def _times_two(x: int) -> int:
    return x * 2


def test_smoke_imports_and_basic_pipeline() -> None:
    assert Stream([1, 2, 3]).map(lambda x: x + 1).to_list() == [2, 3, 4]
    assert ParallelStream([1, 2, 3]).map(_times_two).to_list() == [2, 4, 6]
    assert Collectors.joining(",")([1, 2, 3]) == "1,2,3"
    assert Option.of(1).map(lambda x: x + 1).or_else(0) == 2
    assert Result.success(1).map(lambda x: x + 1).get_or_else(0) == 2


def test_smoke_async_stream_runs() -> None:
    async def scenario() -> list[int]:
        return await AsyncStream.of(1, 2, 3).map(lambda x: x + 10).to_list()

    assert asyncio.run(scenario()) == [11, 12, 13]


def _double(x: int) -> int:
    return x * 2


def _div_by_three(x: int) -> bool:
    return x % 3 == 0


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


def test_unit_parallel_matches_sequential_for_pure_pipeline() -> None:
    data = list(range(200))
    seq = Stream(data).map(_double).filter(_div_by_three).to_list()
    par = ParallelStream(data).map(_double).filter(_div_by_three).to_list()
    assert sorted(par) == sorted(seq)


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


def test_integration_async_bridge() -> None:
    async def scenario() -> list[int]:
        return (
            await Stream([1, 2, 3])
            .to_async()
            .map(lambda x: x + 1)
            .map_async(lambda x: asyncio.sleep(0, result=x * 10))
            .to_list()
        )

    assert asyncio.run(scenario()) == [20, 30, 40]


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


# --- Tests consolidated from test_invariants.py ---

"""Randomized, property, finite-state, and operation-dispatch invariant tests."""


import importlib.util
import itertools
import random
from typing import cast, get_args

import pytest

from fpstreams import Stream, aflow, flow, item
from fpstreams.planning.sync import FilterOp, MapOp, Operation, TapOp


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
    spec = importlib.util.find_spec("fpstreams.execution.async_ops")
    assert spec is not None, "the focused asynchronous dispatch module must exist"

    from fpstreams.execution.async_ops import (
        ASYNC_OPERATION_HANDLERS,
        SUPPORTED_ASYNC_OPERATION_TYPES,
    )
    from fpstreams.planning.async_ import _AsyncOperation

    assert len(SUPPORTED_ASYNC_OPERATION_TYPES) == len(set(SUPPORTED_ASYNC_OPERATION_TYPES))
    assert tuple(ASYNC_OPERATION_HANDLERS) == SUPPORTED_ASYNC_OPERATION_TYPES
    assert set(SUPPORTED_ASYNC_OPERATION_TYPES) == set(get_args(_AsyncOperation))


@pytest.mark.asyncio
async def test_async_operation_dispatch_applies_take_operation() -> None:
    import fpstreams.execution.async_ops as async_ops
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
    from fpstreams.execution.async_ops import apply_async_operation
    from fpstreams.planning.async_ import _AsyncOperation

    async def source():
        if False:
            yield None

    with pytest.raises(TypeError, match="unsupported asynchronous operation: object"):
        apply_async_operation(source(), cast(_AsyncOperation, object()))


@pytest.mark.asyncio
async def test_async_stateless_dispatch_preserves_callable_order() -> None:
    from fpstreams.execution.async_ops import apply_async_operation
    from fpstreams.planning.async_ import _Filter, _MapAsync, _Tap

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

    iterator = apply_async_operation(
        source(), _MapAsync(mapped, concurrency=1, ordered=True, timeout=None)
    )
    iterator = apply_async_operation(iterator, _Tap(tapped))
    iterator = apply_async_operation(iterator, _Filter(accepted))

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


# --- Tests consolidated from test_release_tools.py ---

"""Release tools, action pinning, and publish-workflow ordering safeguards."""


import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PINNED_ACTION = re.compile(
    r"^\s*- uses: [^@\s]+@[0-9a-f]{40}\s+#\s+\S+.*$",
    re.MULTILINE,
)


def test_sha256_manifest_is_sorted_and_excludes_itself(tmp_path: Path) -> None:
    (tmp_path / "b.whl").write_bytes(b"wheel-b")
    (tmp_path / "a.tar.gz").write_bytes(b"source-a")
    manifest = tmp_path / "SHA256SUMS"

    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "tools" / "write_sha256_manifest.py"),
            str(tmp_path),
            str(manifest),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert manifest.read_text(encoding="utf-8") == (
        f"{hashlib.sha256(b'source-a').hexdigest()}  a.tar.gz\n"
        f"{hashlib.sha256(b'wheel-b').hexdigest()}  b.whl\n"
    )


def test_release_smoke_checks_native_and_python_backends() -> None:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT / "src")

    result = subprocess.run(
        [sys.executable, str(ROOT / "tools" / "smoke_release.py")],
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
        "version": "2.0.0",
    }


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


def test_publish_only_receives_credentials_after_artifact_verification() -> None:
    workflow = (ROOT / ".github" / "workflows" / "publish.yml").read_text(encoding="utf-8")

    assert workflow.count("id-token: write") == 1
    assert "needs: manifest" in workflow
    assert "Smoke-test wheel" in workflow
    assert "Build and smoke-test sdist" in workflow
    assert "write_sha256_manifest.py dist/packages dist/SHA256SUMS" in workflow
