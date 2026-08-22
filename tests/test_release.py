# ruff: noqa: E402
"""Consolidated fpstreams test cases."""

from __future__ import annotations

# --- Consolidated from release/test_benchmark_policy.py ---

"""Regression policy for reproducible M12 benchmark baselines."""


import importlib.util
import json
import subprocess
import sys
import tomllib
from fnmatch import fnmatch
from pathlib import Path

ROOT = Path(__file__).parents[1]
COMPARE = ROOT / "tools" / "compare_benchmarks.py"
GROUPS = ROOT / "benchmarks" / "groups.toml"


def _benchmark_module() -> object:
    spec = importlib.util.spec_from_file_location("fpstreams_benchmark", ROOT / "benchmark.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _report(
    seconds: float,
    *,
    first_row_seconds: float | None = None,
    resources: dict[str, float | int] | None = None,
) -> dict[str, object]:
    result: dict[str, object] = {
        "name": "sync.map",
        "median_seconds": seconds,
    }
    if first_row_seconds is not None:
        result["first_row_seconds"] = first_row_seconds
    if resources is not None:
        result["resources"] = resources
    return {
        "metadata": {"python_version": "3.12", "platform": "linux", "machine": "x86_64"},
        "results": [result],
    }


def _create_baseline(tmp_path: Path, reports: list[dict[str, object]]) -> Path:
    inputs: list[Path] = []
    for index, report in enumerate(reports, start=1):
        path = tmp_path / f"run-{index}.json"
        path.write_text(json.dumps(report), encoding="utf-8")
        inputs.append(path)
    baseline = tmp_path / "baseline.json"
    subprocess.run(
        [
            sys.executable,
            str(COMPARE),
            "--create-baseline",
            "--provenance",
            "local_one_shot_unreviewed",
            "--output",
            str(baseline),
            *(str(path) for path in inputs),
        ],
        cwd=ROOT,
        check=True,
    )
    return baseline


def _multi_report(*items: tuple[str, float]) -> dict[str, object]:
    return {
        "metadata": {"python_version": "3.12", "platform": "linux", "machine": "x86_64"},
        "results": [{"name": name, "median_seconds": seconds} for name, seconds in items],
    }


def test_create_baseline_records_median_and_mad_from_three_comparable_runs(tmp_path: Path) -> None:
    """A local baseline records robust timing statistics and explicit unreviewed provenance."""
    inputs: list[Path] = []
    for index, seconds in enumerate((1.0, 1.2, 1.1), start=1):
        path = tmp_path / f"run-{index}.json"
        path.write_text(json.dumps(_report(seconds)), encoding="utf-8")
        inputs.append(path)
    output = tmp_path / "baseline.json"

    result = subprocess.run(
        [
            sys.executable,
            str(COMPARE),
            "--create-baseline",
            "--provenance",
            "local_one_shot_unreviewed",
            "--output",
            str(output),
            *(str(path) for path in inputs),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    baseline = json.loads(output.read_text(encoding="utf-8"))
    assert baseline["provenance"] == "local_one_shot_unreviewed"
    assert baseline["scenarios"]["sync.map"] == {"mad_seconds": 0.1, "median_seconds": 1.1}


def test_compare_rejects_a_single_scenario_over_the_hard_25_percent_cap(tmp_path: Path) -> None:
    """A 25% timing regression fails even when no group can hide it."""
    inputs: list[Path] = []
    for index in range(3):
        path = tmp_path / f"run-{index}.json"
        path.write_text(json.dumps(_report(1.0)), encoding="utf-8")
        inputs.append(path)
    baseline = tmp_path / "baseline.json"
    subprocess.run(
        [
            sys.executable,
            str(COMPARE),
            "--create-baseline",
            "--provenance",
            "local_one_shot_unreviewed",
            "--output",
            str(baseline),
            *(str(path) for path in inputs),
        ],
        cwd=ROOT,
        check=True,
    )
    current = tmp_path / "current.json"
    current.write_text(json.dumps(_report(1.26)), encoding="utf-8")

    result = subprocess.run(
        [sys.executable, str(COMPARE), str(baseline), str(current)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert "hard timing regression: sync.map" in result.stderr


def test_compare_allows_a_noisy_baseline_within_its_mad_tolerance(tmp_path: Path) -> None:
    """A normal fluctuation below the four-MAD threshold is not a regression."""
    baseline = _create_baseline(tmp_path, [_report(0.9), _report(1.0), _report(1.1)])
    current = tmp_path / "current.json"
    current.write_text(json.dumps(_report(1.2)), encoding="utf-8")

    result = subprocess.run(
        [sys.executable, str(COMPARE), str(baseline), str(current)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_compare_rejects_any_resource_count_increase(tmp_path: Path) -> None:
    """Task/file/buffer counters are exact invariants, not noisy timing measurements."""
    baseline = _create_baseline(
        tmp_path,
        [_report(1.0, resources={"live_tasks": 0}) for _ in range(3)],
    )
    current = tmp_path / "current.json"
    current.write_text(json.dumps(_report(1.0, resources={"live_tasks": 1})), encoding="utf-8")

    result = subprocess.run(
        [sys.executable, str(COMPARE), str(baseline), str(current)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert "hard resource invariant: sync.map.live_tasks" in result.stderr


def test_compare_rejects_peak_rss_over_the_hard_30_percent_cap(tmp_path: Path) -> None:
    """Peak allocation has a stricter hard cap than the statistical group gate."""
    baseline = _create_baseline(
        tmp_path,
        [_report(1.0, resources={"peak_rss_bytes": 100}) for _ in range(3)],
    )
    current = tmp_path / "current.json"
    current.write_text(
        json.dumps(_report(1.0, resources={"peak_rss_bytes": 131})), encoding="utf-8"
    )

    result = subprocess.run(
        [sys.executable, str(COMPARE), str(baseline), str(current)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert "hard peak resource regression: sync.map.peak_rss_bytes" in result.stderr


def test_compare_rejects_a_group_wide_regression_over_statistical_tolerance(tmp_path: Path) -> None:
    """Multiple moderately slower scenarios fail when their group also regresses."""
    baseline = _create_baseline(
        tmp_path,
        [_multi_report(("sync.map", 1.0), ("sync.filter", 1.0)) for _ in range(3)],
    )
    groups = tmp_path / "groups.toml"
    groups.write_text('[[group]]\nname = "python_row"\npatterns = ["sync.*"]\n', encoding="utf-8")
    current = tmp_path / "current.json"
    current.write_text(
        json.dumps(_multi_report(("sync.map", 1.12), ("sync.filter", 1.12))), encoding="utf-8"
    )

    result = subprocess.run(
        [sys.executable, str(COMPARE), "--groups", str(groups), str(baseline), str(current)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert "group timing regression: python_row" in result.stderr


def test_compare_rejects_first_row_latency_over_the_hard_30_percent_cap(tmp_path: Path) -> None:
    """First-result latency has its own hard cap, independent of throughput timing."""
    baseline = _create_baseline(
        tmp_path,
        [_report(1.0, first_row_seconds=1.0) for _ in range(3)],
    )
    current = tmp_path / "current.json"
    current.write_text(json.dumps(_report(1.0, first_row_seconds=1.31)), encoding="utf-8")

    result = subprocess.run(
        [sys.executable, str(COMPARE), str(baseline), str(current)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert "hard first-row regression: sync.map" in result.stderr


def test_real_benchmark_records_peak_python_allocation_per_scenario() -> None:
    """A baseline can enforce an observed allocation peak instead of a synthetic field."""
    report = _benchmark_module().run(size=10, repeats=1, domain="int", quick=True)

    assert report["results"]
    for result in report["results"]:
        assert isinstance(result["resources"]["peak_allocation_bytes"], int)
        assert result["resources"]["peak_allocation_bytes"] >= 0


def test_operation_benchmarks_record_real_first_row_latency() -> None:
    """First-result policy is backed by a separate early-consumption measurement."""
    report = _benchmark_module().run(size=10, repeats=1, domain="int", quick=True)
    operation_results = [
        result
        for result in report["results"]
        if result["name"].startswith(("fpstreams_operation/sync/", "fpstreams_operation/async/"))
    ]

    assert operation_results
    assert all(result["first_row_seconds"] >= 0 for result in operation_results)


def test_first_row_latency_uses_enough_samples_to_absorb_startup_jitter() -> None:
    """Thread and timer startup outliers cannot decide a release from one sample."""
    module = _benchmark_module()
    first_row_calls = 0

    def first_row() -> int:
        nonlocal first_row_calls
        first_row_calls += 1
        return first_row_calls

    scenario = module.Scenario(
        "fpstreams_test/first_row/repeated",
        lambda: None,
        "python",
        "test",
        "iterate",
        None,
        first_row,
    )

    record = module._record(scenario, repeats=5)

    assert first_row_calls == 15
    assert len(record["first_row_samples_seconds"]) == 15


def test_logical_compile_keeps_the_pre_deletion_comparison_scenario() -> None:
    """The final benchmark remains comparable with the immutable one-shot baseline."""
    scenarios = _benchmark_module()._logical_compile_scenarios()
    names = {scenario.name for scenario in scenarios}

    assert names == {
        "fpstreams_planning/current_plan/iterate",
        "fpstreams_planning/logical_compile/iterate",
    }
    logical = next(
        scenario
        for scenario in scenarios
        if scenario.name == "fpstreams_planning/logical_compile/iterate"
    )
    assert logical.baseline == "fpstreams_planning/current_plan/iterate"


def test_benchmark_include_partitions_scenarios_without_changing_metadata() -> None:
    """A long local run can be sharded without changing a scenario's workload metadata."""
    module = _benchmark_module()
    full = module.run(size=10, repeats=1, domain="int", quick=True)
    partial = module.run(
        size=10, repeats=1, domain="int", quick=True, include=("fpstreams_operation/sync/*",)
    )

    assert partial["metadata"] == full["metadata"]
    assert partial["results"]
    assert all(item["name"].startswith("fpstreams_operation/sync/") for item in partial["results"])


def test_competitive_benchmark_checks_numpy_and_pandas_results_without_changing_release_suite() -> (
    None
):
    """Optional competitors prove equal answers while the frozen release suite stays unchanged."""
    module = _benchmark_module()
    release_names = {
        result["name"]
        for result in module.run(size=16, repeats=1, domain="int", quick=True)["results"]
    }

    report = module.run_competitive(size=128, repeats=1)

    assert report["metadata"]["suite"] == "competitive"
    assert {result["name"] for result in report["results"]} == {
        "competitive/numpy/array/map_filter/sum",
        "competitive/fpstreams/range/map_filter/sum",
        "competitive/pandas/dataframe/group_sum",
        "competitive/fpstreams/rows/group_sum",
    }
    assert report["comparisons"] == [
        {
            "candidate": "competitive/fpstreams/range/map_filter/sum",
            "baseline": "competitive/numpy/array/map_filter/sum",
            "outputs_equal": True,
            "ratio": report["comparisons"][0]["ratio"],
        },
        {
            "candidate": "competitive/fpstreams/rows/group_sum",
            "baseline": "competitive/pandas/dataframe/group_sum",
            "outputs_equal": True,
            "ratio": report["comparisons"][1]["ratio"],
        },
    ]
    assert all(comparison["ratio"] >= 0 for comparison in report["comparisons"])
    assert not release_names & {result["name"] for result in report["results"]}


def test_real_benchmark_exercises_each_sync_operation_union_member() -> None:
    """M12 matrix claims are backed by named execution scenarios, not placeholder IDs."""
    report = _benchmark_module().run(size=10, repeats=1, domain="int", quick=True)
    names = {result["name"] for result in report["results"]}

    assert "fpstreams_operation/sync/map" in names
    assert "fpstreams_operation/sync/gather" in names
    assert "fpstreams_operation/sync/collapse" in names


def test_real_benchmark_exercises_each_async_operation_union_member() -> None:
    """Async physical operations receive independent executable benchmark evidence."""
    report = _benchmark_module().run(size=10, repeats=1, domain="int", quick=True)
    names = {result["name"] for result in report["results"]}

    assert "fpstreams_operation/async/map_async" in names
    assert "fpstreams_operation/async/merge" in names
    assert "fpstreams_operation/async/collapse" in names


def test_real_benchmark_exercises_core_rows_and_relational_operations() -> None:
    """Rows transformations and relational plans have executable timing evidence too."""
    report = _benchmark_module().run(size=10, repeats=1, domain="int", quick=True)
    names = {result["name"] for result in report["results"]}

    assert "fpstreams_operation/rows/with_columns" in names
    assert "fpstreams_operation/rows/join" in names
    assert "fpstreams_operation/rows/group_aggregate" in names


def test_callable_group_benchmarks_guard_each_fixed_callback_loop() -> None:
    """The release suite retains scalable evidence for callable key and value lanes."""
    scenarios = _benchmark_module()._callable_group_scenarios(10)

    assert {scenario.name for scenario in scenarios} == {
        "fpstreams_group/tuple/callable_key/count",
        "fpstreams_group/tuple/callable_key/count_sum_direct",
        "fpstreams_group/tuple/callable_key_value/count_sum",
        "fpstreams_group/tuple/callable_value/count_sum",
        "fpstreams_group/dict/callable_key/count_sum_direct/high_cardinality",
        "fpstreams_group/dict/callable_value/count_sum/high_cardinality",
        "fpstreams_group/dict/callable_key/count_sum_direct/high_cardinality/auto",
        "fpstreams_group/dict/callable_value/count_sum/high_cardinality/auto",
        "fpstreams_group/mappingproxy/callable_key/count_sum_direct/high_cardinality",
        "fpstreams_group/mappingproxy/callable_value/count_sum/high_cardinality",
        "fpstreams_group/nominal_mapping/callable_key/count_sum_direct/high_cardinality",
        "fpstreams_group/nominal_mapping/callable_value/count_sum/high_cardinality",
    }
    assert all(
        scenario.task()
        == [
            {"key": key, "count": 1, **({"total": key} if "count_sum" in scenario.name else {})}
            for key in range(10)
        ]
        for scenario in scenarios
    )
    guarded = [scenario for scenario in scenarios if scenario.baseline is not None]
    assert [scenario.minimum_repeats for scenario in guarded] == [15, 15, 15, 15, 15, 15]
    assert [scenario.maximum_ratio for scenario in guarded] == [
        0.98,
        0.98,
        1.65,
        1.65,
        1.75,
        1.75,
    ]
    assert [scenario.baseline for scenario in guarded] == [
        "fpstreams_group/dict/callable_key/count_sum_direct/high_cardinality",
        "fpstreams_group/dict/callable_value/count_sum/high_cardinality",
        "fpstreams_group/dict/callable_key/count_sum_direct/high_cardinality",
        "fpstreams_group/dict/callable_value/count_sum/high_cardinality",
        "fpstreams_group/dict/callable_key/count_sum_direct/high_cardinality",
        "fpstreams_group/dict/callable_value/count_sum/high_cardinality",
    ]


def test_fixed_sparse_group_benchmarks_guard_tuple_and_dict_entry_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The release suite retains high-cardinality evidence for the sparse native index."""
    from fpstreams import _native

    calls: list[str] = []
    tuple_kernel = _native.group_fixed_i64_rows_v1
    dict_kernel = _native.group_fixed_i64_dict_rows_v1

    def tracked_tuple(*arguments: object) -> object:
        calls.append("tuple")
        return tuple_kernel(*arguments)

    def tracked_dict(*arguments: object) -> object:
        calls.append("dict")
        return dict_kernel(*arguments)

    monkeypatch.setattr(_native, "group_fixed_i64_rows_v1", tracked_tuple)
    monkeypatch.setattr(_native, "group_fixed_i64_dict_rows_v1", tracked_dict)
    scenarios = _benchmark_module()._fixed_sparse_group_scenarios(10)

    assert {scenario.name for scenario in scenarios} == {
        "fpstreams_group/tuple/fixed_i64/count_sum/sparse_high_cardinality",
        "fpstreams_group/dict/fixed_i64/count_sum/sparse_high_cardinality",
    }
    expected = [{"key": -index - 1, "count": 1, "total": index} for index in range(10)]
    assert all(scenario.task() == expected for scenario in scenarios)
    assert calls == ["tuple", "dict"]


def test_composite_group_benchmark_guards_the_direct_count_sum_loop() -> None:
    """Two-key grouping keeps an equivalent callable fallback and a same-run ratio gate."""
    scenarios = _benchmark_module()._composite_group_scenarios(10)

    assert [scenario.name for scenario in scenarios] == [
        "fpstreams_group/tuple/callable_composite/count_sum/high_cardinality/python",
        "fpstreams_group/tuple/direct_composite/count_sum/high_cardinality/auto",
    ]
    assert scenarios[1].baseline == scenarios[0].name
    assert scenarios[1].maximum_ratio == 0.45
    expected = [
        {"key_0": index, "key_1": index % 7, "count": 1, "total": index} for index in range(10)
    ]
    assert scenarios[0].task() == scenarios[1].task() == expected


def test_mapping_field_join_benchmarks_guard_unique_and_many_native_fallbacks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Direct fields on generic Mapping rows retain scalable unique and many evidence."""
    from types import MappingProxyType

    from fpstreams import _native

    calls: list[tuple[str, tuple[object, ...]]] = []
    unique_kernel = _native.join_hashable_unique_direct_records_v1
    many_kernel = _native.join_hashable_many_direct_records_v1

    def tracked_unique(*arguments: object) -> object:
        result = unique_kernel(*arguments)
        assert result is not None
        calls.append(("unique", arguments))
        return result

    def tracked_many(*arguments: object) -> object:
        result = many_kernel(*arguments)
        assert result is not None
        calls.append(("many", arguments))
        return result

    def callback_forbidden(*_arguments: object) -> None:
        raise AssertionError("direct-field benchmark must not use a callback ABI")

    monkeypatch.setattr(_native, "join_hashable_unique_direct_records_v1", tracked_unique)
    monkeypatch.setattr(_native, "join_hashable_many_direct_records_v1", tracked_many)
    monkeypatch.setattr(_native, "join_hashable_unique_records_v2", callback_forbidden)
    monkeypatch.setattr(_native, "join_hashable_many_records_v2", callback_forbidden)
    scenarios = _benchmark_module()._mapping_field_join_scenarios(10)

    assert {scenario.name for scenario in scenarios} == {
        "fpstreams_join/mapping/direct_field/unique",
        "fpstreams_join/mapping/direct_field/many",
    }
    results = {scenario.name: scenario.task() for scenario in scenarios}
    assert len(results["fpstreams_join/mapping/direct_field/unique"]) == 10
    assert len(results["fpstreams_join/mapping/direct_field/many"]) == 20
    assert [cardinality for cardinality, _arguments in calls] == ["unique", "many"]
    assert [arguments[2:4] for _cardinality, arguments in calls] == [
        ("left_id", "right_id"),
        ("left_id", "right_id"),
    ]
    assert all(arguments[7] == (MappingProxyType,) for _cardinality, arguments in calls)


def test_namedtuple_callable_join_benchmarks_guard_unique_and_many_v2(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The release suite keeps scalable evidence for both NamedTuple v2 cardinalities."""
    from fpstreams import _native
    from fpstreams.tabular.records import _as_record

    calls: list[tuple[str, object]] = []
    unique_kernel = _native.join_hashable_unique_records_v2
    many_kernel = _native.join_hashable_many_records_v2

    def tracked_unique(*arguments: object) -> object:
        result = unique_kernel(*arguments)
        assert result is not None
        calls.append(("unique", arguments[4]))
        return result

    def tracked_many(*arguments: object) -> object:
        result = many_kernel(*arguments)
        assert result is not None
        calls.append(("many", arguments[4]))
        return result

    monkeypatch.setattr(_native, "join_hashable_unique_records_v2", tracked_unique)
    monkeypatch.setattr(_native, "join_hashable_many_records_v2", tracked_many)
    scenarios = _benchmark_module()._namedtuple_callable_join_scenarios(10)

    assert {scenario.name for scenario in scenarios} == {
        "fpstreams_join/namedtuple/callable/unique",
        "fpstreams_join/namedtuple/callable/unique/python",
        "fpstreams_join/namedtuple/callable/many",
        "fpstreams_join/namedtuple/callable/many/python",
    }
    results = {scenario.name: scenario.task() for scenario in scenarios}
    assert len(results["fpstreams_join/namedtuple/callable/unique"]) == 10
    assert len(results["fpstreams_join/namedtuple/callable/unique/python"]) == 10
    assert len(results["fpstreams_join/namedtuple/callable/many"]) == 20
    assert len(results["fpstreams_join/namedtuple/callable/many/python"]) == 20
    assert [cardinality for cardinality, _adapter in calls] == ["unique", "many"]
    assert all(adapter is not _as_record for _cardinality, adapter in calls)
    automatic = {scenario.name: scenario for scenario in scenarios if scenario.backend == "auto"}
    assert {scenario.minimum_repeats for scenario in scenarios} == {15}
    expected_ratios = {0.70, 0.66} if sys.version_info[:2] in {(3, 12), (3, 13)} else {0.55}
    assert {scenario.maximum_ratio for scenario in automatic.values()} == expected_ratios
    assert {scenario.baseline for scenario in automatic.values()} == {
        "fpstreams_join/namedtuple/callable/unique/python",
        "fpstreams_join/namedtuple/callable/many/python",
    }


def test_wide_callable_join_benchmarks_guard_right_schema_cache_workloads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Wide callable joins retain scalable unique and many schema-cache evidence."""
    from fpstreams import _native

    calls: list[str] = []
    unique_kernel = _native.join_hashable_unique_records_v2
    many_kernel = _native.join_hashable_many_records_v2

    def tracked_unique(*arguments: object) -> object:
        calls.append("unique")
        return unique_kernel(*arguments)

    def tracked_many(*arguments: object) -> object:
        calls.append("many")
        return many_kernel(*arguments)

    monkeypatch.setattr(_native, "join_hashable_unique_records_v2", tracked_unique)
    monkeypatch.setattr(_native, "join_hashable_many_records_v2", tracked_many)
    scenarios = _benchmark_module()._wide_callable_join_scenarios(10)

    assert {scenario.name for scenario in scenarios} == {
        "fpstreams_join/mapping/callable/wide_schema/unique",
        "fpstreams_join/mapping/callable/wide_schema/many",
        "fpstreams_join/mapping/callable/wide_schema/many_bulk_merge",
    }
    results = {scenario.name: scenario.task() for scenario in scenarios}
    assert len(results["fpstreams_join/mapping/callable/wide_schema/unique"]) == 10
    assert len(results["fpstreams_join/mapping/callable/wide_schema/many"]) == 20
    assert len(results["fpstreams_join/mapping/callable/wide_schema/many_bulk_merge"]) == 20
    assert calls == ["unique", "many", "many"]


def test_value_layout_join_benchmarks_guard_distinct_exact_string_schemas(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Value-layout benchmarks retain unique and many v2 workloads with fresh field objects."""
    from fpstreams import _native

    calls: list[str] = []
    unique_kernel = _native.join_hashable_unique_records_v2
    many_kernel = _native.join_hashable_many_records_v2

    def tracked_unique(*arguments: object) -> object:
        calls.append("unique")
        result = unique_kernel(*arguments)
        assert result is not None
        return result

    def tracked_many(*arguments: object) -> object:
        calls.append("many")
        result = many_kernel(*arguments)
        assert result is not None
        return result

    monkeypatch.setattr(_native, "join_hashable_unique_records_v2", tracked_unique)
    monkeypatch.setattr(_native, "join_hashable_many_records_v2", tracked_many)
    scenarios = _benchmark_module()._value_layout_callable_join_scenarios(10)

    assert {scenario.name for scenario in scenarios} == {
        "fpstreams_join/mapping/callable/value_schema/unique",
        "fpstreams_join/mapping/callable/value_schema/many",
    }
    results = {scenario.name: scenario.task() for scenario in scenarios}
    assert len(results["fpstreams_join/mapping/callable/value_schema/unique"]) == 10
    assert len(results["fpstreams_join/mapping/callable/value_schema/many"]) == 20
    assert calls == ["unique", "many"]


def test_arrow_dictionary_group_benchmark_guards_nullable_dictionary_unification() -> None:
    """The Arrow suite retains differing dictionaries and nullable encounter-order work."""
    pytest.importorskip("pyarrow")
    scenarios = _benchmark_module()._arrow_dictionary_group_scenarios(10)

    assert [scenario.name for scenario in scenarios] == ["fpstreams_arrow/dictionary/group_sum"]
    result = scenarios[0].task()
    assert sum(row["total"] for row in result) == 10
    assert any(row["key"] is None for row in result)


def test_arrow_identity_list_benchmark_retains_the_canonical_comparison() -> None:
    """The scalable Arrow list scenario keeps its forced-Python comparison workload."""
    pytest.importorskip("pyarrow")
    scenarios = _benchmark_module()._arrow_identity_list_scenarios(10)

    assert [scenario.name for scenario in scenarios] == [
        "fpstreams_arrow/table/identity/list/python",
        "fpstreams_arrow/table/identity/list/auto",
    ]
    assert scenarios[1].baseline == scenarios[0].name
    assert (
        scenarios[0].task()
        == scenarios[1].task()
        == [{"id": index, "group": index % 64, "value": index * 3} for index in range(10)]
    )


def test_arrow_c_stream_benchmark_observes_first_batch_and_full_scan() -> None:
    """The Arrow suite pairs eager and lazy export at both observable boundaries."""
    pytest.importorskip("pyarrow")
    scenarios = _benchmark_module()._arrow_c_stream_scenarios(10)

    assert [scenario.name for scenario in scenarios] == [
        "fpstreams_arrow/rows/c_stream/eager_table",
        "fpstreams_arrow/rows/c_stream/lazy",
    ]
    assert scenarios[1].baseline == scenarios[0].name
    assert [scenario.task() for scenario in scenarios] == [10, 10]
    assert [scenario.first_row_task() for scenario in scenarios] == [10, 10]


def test_arrow_reader_group_benchmark_retains_the_python_comparison() -> None:
    """The one-shot reader scenario compares equivalent fresh sources on every sample."""
    pytest.importorskip("pyarrow")
    scenarios = _benchmark_module()._arrow_reader_group_scenarios(10)

    assert [scenario.name for scenario in scenarios] == [
        "fpstreams_arrow/reader/group_sum/python",
        "fpstreams_arrow/reader/group_sum/auto",
    ]
    assert scenarios[1].baseline == scenarios[0].name
    assert scenarios[0].task() == scenarios[1].task()


def test_arrow_file_group_benchmarks_retain_streaming_python_comparisons() -> None:
    """CSV and Parquet fixtures survive their builder and pair equivalent public scans."""
    pytest.importorskip("pyarrow")
    scenarios = _benchmark_module()._arrow_file_group_scenarios(10)

    assert [scenario.name for scenario in scenarios] == [
        "fpstreams_arrow/csv/group_sum/python",
        "fpstreams_arrow/csv/group_sum/auto",
        "fpstreams_arrow/parquet/group_sum/python",
        "fpstreams_arrow/parquet/group_sum/auto",
    ]
    assert [scenario.backend for scenario in scenarios] == ["python", "auto", "python", "auto"]
    assert [scenario.maximum_ratio for scenario in scenarios] == [None, None, None, None]
    assert scenarios[1].baseline == scenarios[0].name
    assert scenarios[3].baseline == scenarios[2].name
    results = [scenario.task() for scenario in scenarios]
    assert results[0] == results[1]
    assert results[2] == results[3]


def test_arrow_file_group_benchmarks_wire_the_guard_only_to_the_full_workload() -> None:
    """The actual 300k scenarios, rather than only synthetic records, carry the guard."""
    pytest.importorskip("pyarrow")
    scenarios = _benchmark_module()._arrow_file_group_scenarios(300_000)

    assert [scenario.maximum_ratio for scenario in scenarios] == [None, 0.60, None, 0.60]


def test_arrow_file_group_regression_gate_requires_a_sixty_percent_ratio() -> None:
    """A 300k file fast path must remain at least 1.67x faster than forced Python."""
    module = _benchmark_module()
    for storage, source_kind in (("csv", "arrow_csv"), ("parquet", "arrow_parquet")):
        python_name = f"fpstreams_arrow/{storage}/group_sum/python"
        auto_name = f"fpstreams_arrow/{storage}/group_sum/auto"

        def records(
            ratio: float,
            maximum_ratio: float | None,
            python_name: str = python_name,
            auto_name: str = auto_name,
            source_kind: str = source_kind,
        ) -> list[dict[str, object]]:
            return [
                {
                    "name": python_name,
                    "median_seconds": 1.0,
                    "backend": "python",
                    "source_kind": source_kind,
                    "terminal": "group_sum",
                    "baseline": None,
                },
                {
                    "name": auto_name,
                    "median_seconds": ratio,
                    "backend": "auto",
                    "source_kind": source_kind,
                    "terminal": "group_sum",
                    "baseline": python_name,
                    "maximum_ratio": maximum_ratio,
                },
            ]

        regressions = module.find_regressions(records(0.61, 0.60))
        assert regressions == [
            {
                "name": auto_name,
                "baseline": python_name,
                "ratio": 0.61,
                "maximum_ratio": 0.60,
            }
        ]
        assert module.find_regressions(records(0.60, 0.60)) == []
        assert module.find_regressions(records(1.00, None)) == []


def test_exact_dict_sort_benchmark_retains_the_callable_canonical_comparison(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 300k-cap direct-field sort stays paired with its ordinary callable semantics."""
    from fpstreams import _native

    guarded: list[list[object]] = []
    native_guard = _native.all_exact_dict_rows_v1

    def tracked_guard(source: list[object]) -> bool:
        guarded.append(source)
        return native_guard(source)

    monkeypatch.setattr(_native, "all_exact_dict_rows_v1", tracked_guard)
    scenarios = _benchmark_module()._exact_dict_sort_scenarios(10)

    assert [scenario.name for scenario in scenarios] == [
        "fpstreams_sort/exact_dict/callable_field/list",
        "fpstreams_sort/exact_dict/direct_field/list",
    ]
    assert [scenario.backend for scenario in scenarios] == ["python", "auto"]
    assert scenarios[1].baseline == scenarios[0].name
    expected = [
        {"value": 0, "position": 0},
        {"value": 1, "position": 9},
        {"value": 2, "position": 8},
        {"value": 3, "position": 7},
        {"value": 4, "position": 6},
        {"value": 5, "position": 5},
        {"value": 6, "position": 4},
        {"value": 7, "position": 3},
        {"value": 8, "position": 2},
        {"value": 9, "position": 1},
    ]
    assert scenarios[0].task() == scenarios[1].task() == expected
    assert len(guarded) == 1
    assert len(guarded[0]) == 10
    assert all(type(row) is dict for row in guarded[0])


def test_arrow_stable_sort_benchmark_retains_the_forced_python_comparison() -> None:
    """The retained-table sort compares one source under forced Python and auto."""
    pytest.importorskip("pyarrow")
    scenarios = _benchmark_module()._arrow_stable_sort_scenarios(12)

    assert [scenario.name for scenario in scenarios] == [
        "fpstreams_arrow/table/stable_sort/python",
        "fpstreams_arrow/table/stable_sort/auto",
    ]
    assert [scenario.backend for scenario in scenarios] == ["python", "auto"]
    assert scenarios[1].baseline == scenarios[0].name
    expected = scenarios[0].task()
    assert scenarios[1].task() == expected
    assert [row["position"] for row in expected if row["key"] == 0] == [0, 3, 6, 9]


def test_arrow_unique_join_benchmarks_retain_public_python_comparisons() -> None:
    """Both direct layouts compare complete public row output with forced Python."""
    pytest.importorskip("pyarrow")
    scenarios = _benchmark_module()._arrow_unique_join_scenarios(12)

    assert [scenario.name for scenario in scenarios] == [
        f"fpstreams_arrow/table/unique_join/{layout}/{how}/{backend}"
        for layout in ("no_suffix", "suffix")
        for how in ("inner", "left")
        for backend in ("python", "auto")
    ]
    assert [scenario.backend for scenario in scenarios] == ["python", "auto"] * 4
    for canonical, automatic in zip(scenarios[::2], scenarios[1::2], strict=True):
        assert automatic.baseline == canonical.name
        assert automatic.task() == canonical.task()


def test_spill_benchmark_uses_a_reproducible_partition_shape(monkeypatch) -> None:
    """Hash randomization cannot turn the release spill timing into a bimodal sample."""
    from fpstreams.tabular import spill

    observed: list[tuple[object, int]] = []
    original = spill._partition

    def tracked(key, count, *, operation, salt=0):
        bucket = original(key, count, operation=operation, salt=salt)
        observed.append((key, bucket))
        return bucket

    monkeypatch.setattr(spill, "_partition", tracked)
    scenario = next(
        item
        for item in _benchmark_module()._rows_operation_scenarios()
        if item.name == "fpstreams_operation/rows/group_spill_aggregate"
    )

    assert scenario.task() == [{"team": 0, "total": 3}, {"team": 1, "total": 3}]
    assert observed == [(0, 0), (0, 0), (1, 1)]


def test_every_emitted_benchmark_scenario_has_one_statistical_group() -> None:
    """A new scenario cannot silently evade its group-level regression gate."""
    groups = tomllib.loads(GROUPS.read_text(encoding="utf-8"))["group"]
    report = _benchmark_module().run(size=10, repeats=1, domain="int", quick=True)

    for result in report["results"]:
        memberships = [
            group["name"]
            for group in groups
            if any(fnmatch(result["name"], pattern) for pattern in group["patterns"])
        ]
        assert memberships, result["name"]
        assert len(memberships) == 1, (result["name"], memberships)


# --- Consolidated from release/test_failpoint_matrix.py ---

"""The M12 failure-injection surface remains connected to real transition sites."""


import ast
from pathlib import Path

ROOT = Path(__file__).parents[1]
REQUIRED_FAILPOINTS = frozenset(
    {
        "source.open.after",
        "iterator.pull.after",
        "callback.before",
        "callback.after",
        "expression.guard.before",
        "backend.convert.after",
        "resource.register.after",
        "resource.close.before",
        "spill.mkdir.after",
        "spill.open.after",
        "spill.write.before",
        "spill.read.before",
        "spill.generation.replace.before",
        "spill.unlink.before",
        "sort.run.flush.after",
        "sort.merge.pull.after",
        "group.state.create.after",
        "join.build.insert.after",
        "join.probe.match.after",
        "task.create.after",
        "task.complete.before_publish",
        "task.cancel.before",
        "timer.arm.after",
        "timer.fire.before_publish",
        "db.connect.after",
        "db.cursor.after",
        "db.execute.after",
        "db.fetch.after",
        "db.commit.before",
        "arrow.reader.after",
        "arrow.batch.after",
    }
)


def _production_failpoints() -> set[str]:
    """Read literal ``hit(name)`` calls without importing production modules."""
    names: set[str] = set()
    for path in (ROOT / "src" / "fpstreams").rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "hit"
                and len(node.args) == 1
                and isinstance(node.args[0], ast.Constant)
                and isinstance(node.args[0].value, str)
            ):
                names.add(node.args[0].value)
    return names


def test_every_planned_failpoint_is_reachable_from_production_code() -> None:
    """Registry-only names do not count: each one must guard an owned transition."""
    assert _production_failpoints() >= REQUIRED_FAILPOINTS


# --- Consolidated from release/test_failpoints.py ---

"""Test-only failpoints are nested, local, and off unless explicitly scoped."""


import asyncio

import pytest

import fpstreams
from fpstreams.planning.source import Source
from fpstreams.runtime.failpoints import failpoint, hit
from fpstreams.runtime.limits import QueryLimits
from fpstreams.runtime.metrics import QueryMetrics
from fpstreams.runtime.query import QueryRuntime
from fpstreams.runtime.resources import ResourceRegistry
from fpstreams.runtime.tasks import TaskRole, TaskRuntime
from fpstreams.storage.spill_store import SpillStore
from fpstreams.streams.flow import flow


def test_failpoint_is_scoped_nested_and_disabled_by_default() -> None:
    hit("spill.write.before")
    with failpoint("spill.write.before", OSError("disk")):
        with pytest.raises(OSError, match="disk"):
            hit("spill.write.before")
        with (
            failpoint("spill.write.before", RuntimeError("inner")),
            pytest.raises(RuntimeError, match="inner"),
        ):
            hit("spill.write.before")
        with pytest.raises(OSError, match="disk"):
            hit("spill.write.before")
    hit("spill.write.before")


def test_spill_transition_failpoints_are_reachable(tmp_path) -> None:
    with (
        QueryRuntime() as runtime,
        failpoint("spill.mkdir.after", OSError("mkdir")),
        pytest.raises(OSError, match="mkdir"),
    ):
        SpillStore(runtime, parent=tmp_path, operation="test")


def test_resource_transition_failpoints_preserve_ownership() -> None:
    closed: list[bool] = []
    registry = ResourceRegistry()
    resource = object()
    with (
        failpoint("resource.register.after", RuntimeError("registered")),
        pytest.raises(RuntimeError, match="registered"),
    ):
        registry.own(resource, lambda _value: closed.append(True))

    registry.close()
    assert closed == [True]

    with (
        failpoint("resource.close.before", OSError("close")),
        pytest.raises(OSError, match="close"),
    ):
        registry = ResourceRegistry()
        registry.own(object(), lambda _value: None)
        registry.close()


def test_source_open_failpoint_closes_new_iterator() -> None:
    closed: list[bool] = []

    class Values:
        def __iter__(self):
            return self

        def __next__(self):
            raise StopIteration

        def close(self) -> None:
            closed.append(True)

    source = Source.defer(Values)
    with (
        failpoint("source.open.after", OSError("open")),
        pytest.raises(OSError, match="open"),
    ):
        source.open()

    assert closed == [True]


def test_pull_failpoint_stops_before_callback() -> None:
    callbacks: list[int] = []
    pipeline = flow([1, 2]).map(lambda value: callbacks.append(value) or value)

    with (
        failpoint("iterator.pull.after", OSError("pull")),
        pytest.raises(OSError, match="pull"),
    ):
        pipeline.to_list()

    assert callbacks == []

    with (
        failpoint("iterator.pull.after", OSError("identity pull")),
        pytest.raises(OSError, match="identity pull"),
    ):
        flow([1, 2]).sum()


def test_task_create_failpoint_keeps_scheduled_task_owned_until_close() -> None:
    async def scenario() -> None:
        runtime = TaskRuntime(QueryLimits(), QueryMetrics())
        with (
            failpoint("task.create.after", RuntimeError("task")),
            pytest.raises(RuntimeError, match="task"),
        ):
            runtime.create_task(asyncio.sleep(0), role=TaskRole.OPERATOR)

        assert runtime.live_count == 1
        await runtime.aclose()
        assert runtime.live_count == 0

    asyncio.run(scenario())


def test_callback_failpoints_preserve_callback_prefix() -> None:
    seen: list[int] = []
    pipeline = flow([1, 2]).map(lambda value: seen.append(value) or value)

    with (
        failpoint("callback.before", OSError("before")),
        pytest.raises(OSError, match="before"),
    ):
        pipeline.to_list()
    assert seen == []

    pipeline = flow([1, 2]).map(lambda value: seen.append(value) or value)
    with (
        failpoint("callback.after", OSError("after")),
        pytest.raises(OSError, match="after"),
    ):
        pipeline.to_list()
    assert seen == [1]


@pytest.mark.parametrize("validate", ["m:m", "m:1"])
def test_join_build_failpoint_closes_right_source(validate: str) -> None:
    closed: list[bool] = []

    def right_values():
        try:
            yield {"id": 1}
        finally:
            closed.append(True)

    joined = fpstreams.rows([{"id": 1}]).join(
        fpstreams.rows(right_values()), on="id", validate=validate
    )
    with (
        failpoint("join.build.insert.after", OSError("index")),
        pytest.raises(OSError, match="index"),
    ):
        joined.to_list()

    assert closed == [True]


def test_group_state_failpoint_closes_source() -> None:
    closed: list[bool] = []

    def values():
        try:
            yield {"team": "a", "value": 1}
        finally:
            closed.append(True)

    grouped = fpstreams.rows(values()).group_by("team").aggregate(total=fpstreams.agg.sum("value"))
    with (
        failpoint("group.state.create.after", OSError("group")),
        pytest.raises(OSError, match="group"),
    ):
        grouped.to_list()

    assert closed == [True]


@pytest.mark.parametrize("validate", ["m:m", "m:1"])
def test_join_match_failpoint_closes_both_sources(validate: str) -> None:
    closed: list[str] = []

    def source(label: str):
        try:
            yield {"id": 1, label: label}
        finally:
            closed.append(label)

    joined = fpstreams.rows(source("left")).join(
        fpstreams.rows(source("right")), on="id", validate=validate
    )
    with (
        failpoint("join.probe.match.after", OSError("match")),
        pytest.raises(OSError, match="match"),
    ):
        joined.to_list()

    assert sorted(closed) == ["left", "right"]


def test_timer_arm_failpoint_releases_the_owned_task() -> None:
    async def scenario() -> None:
        runtime = TaskRuntime(QueryLimits(max_tasks=2), QueryMetrics())
        scope = runtime.scope("timer")
        from fpstreams.execution.async_timers import TimerHandle

        timer = TimerHandle(scope)
        with (
            failpoint("timer.arm.after", OSError("arm")),
            pytest.raises(OSError, match="arm"),
        ):
            await timer.arm(0)
        await timer.aclose()
        await runtime.aclose()
        assert runtime.live_count == 0

    asyncio.run(scenario())


def test_task_completion_and_cancellation_failpoints_leave_no_live_task() -> None:
    async def scenario() -> None:
        runtime = TaskRuntime(QueryLimits(max_tasks=2), QueryMetrics())
        task = runtime.create_task(asyncio.sleep(0, result=1), role=TaskRole.OPERATOR)
        with (
            failpoint("task.complete.before_publish", OSError("publish")),
            pytest.raises(OSError, match="publish"),
        ):
            await runtime.take_result(task)
        assert runtime.live_count == 0

        task = runtime.create_task(asyncio.sleep(1), role=TaskRole.OPERATOR)
        with (
            failpoint("task.cancel.before", OSError("cancel")),
            pytest.raises(OSError, match="cancel"),
        ):
            await runtime.cancel(task)
        await runtime.aclose()
        assert runtime.live_count == 0

    asyncio.run(scenario())


def test_database_failpoint_closes_query_resources() -> None:
    closed: list[str] = []

    class Cursor:
        description = (("value",),)

        def execute(self, _query: str) -> None:
            return None

        def fetchmany(self, _size: int) -> list[tuple[int]]:
            return [(1,)]

        def close(self) -> None:
            closed.append("cursor")

    class Connection:
        def cursor(self) -> Cursor:
            return Cursor()

        def close(self) -> None:
            closed.append("connection")

    source = fpstreams.rows.from_db(Connection, "select 1", batch_size=1)
    with (
        failpoint("db.fetch.after", OSError("fetch")),
        pytest.raises(OSError, match="fetch"),
    ):
        source.to_list()

    assert closed == ["cursor", "connection"]


def test_database_commit_failpoint_rolls_back_and_closes_resources() -> None:
    events: list[str] = []

    class Cursor:
        def executemany(self, _statement: str, _batch: list[tuple[int]]) -> None:
            events.append("execute")

        def close(self) -> None:
            events.append("cursor.close")

    class Connection:
        def cursor(self) -> Cursor:
            return Cursor()

        def commit(self) -> None:
            events.append("commit")

        def rollback(self) -> None:
            events.append("rollback")

        def close(self) -> None:
            events.append("connection.close")

    with (
        failpoint("db.commit.before", OSError("commit")),
        pytest.raises(OSError, match="commit"),
    ):
        fpstreams.rows([{"value": 1}]).to_db(Connection, "insert", batch_size=1)

    assert events == ["execute", "rollback", "cursor.close", "connection.close"]


def test_sort_flush_failpoint_removes_spill_files(tmp_path) -> None:
    pipeline = flow([4, 3, 2, 1]).external_sort_by(
        lambda value: value, buffer_size=1, tempdir=tmp_path
    )
    with (
        failpoint("sort.run.flush.after", OSError("flush")),
        pytest.raises(OSError, match="flush"),
    ):
        pipeline.to_list()

    assert list(tmp_path.iterdir()) == []


def test_arrow_reader_failpoint_closes_the_one_shot_reader() -> None:
    import pyarrow as pa

    reader = pa.RecordBatchReader.from_batches(
        pa.schema([("value", pa.int64())]), [pa.record_batch([[1]], names=["value"])]
    )
    source = fpstreams.rows.from_arrow(reader)
    with (
        failpoint("arrow.reader.after", OSError("reader")),
        pytest.raises(OSError, match="reader"),
    ):
        source.to_list()


def test_expression_guard_failpoint_prevents_source_consumption() -> None:
    consumed: list[int] = []

    def values():
        for value in range(3):
            consumed.append(value)
            yield value

    pipeline = flow(values()).map(lambda value: value + 1)
    with (
        failpoint("expression.guard.before", OSError("guard")),
        pytest.raises(OSError, match="guard"),
    ):
        pipeline.to_list()

    assert consumed == []


# --- Production legacy-route guards ---


def test_sync_production_tree_has_no_legacy_compatibility_route() -> None:
    """M12 keeps one logical-to-physical sync execution route, with no aliases."""
    source = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (ROOT / "src" / "fpstreams").rglob("*.py")
        if "async" not in path.name
    )
    forbidden = (
        "class Plan:",
        "logical_from_legacy",
        "logical_to_legacy",
        "class LegacyPhysicalNode",
        "class LegacyBackendPayload",
        "legacy_plan_from_physical",
        "def execute(plan:",
        "def select_legacy_",
    )

    assert all(symbol not in source for symbol in forbidden)


def test_async_production_tree_has_no_legacy_executor_or_private_plan_names() -> None:
    """M12 async execution is physical-only and owns tasks through QueryRuntime."""
    package = ROOT / "src" / "fpstreams"
    source = "\n".join(path.read_text(encoding="utf-8") for path in package.rglob("*.py"))

    assert not (package / "execution" / "async_concurrency.py").exists()
    assert not (package / "execution" / "async_.py").exists()
    assert not (package / "execution" / "async_runtime.py").exists()
    for symbol in ("LegacyAsyncNode", "_AsyncPlan", "_AsyncOperation", "finish_task"):
        assert symbol not in source


# --- Consolidated from release/test_optional_backends.py ---

"""Subprocess checks for supported configurations without optional backends."""


from pathlib import Path

ROOT = Path(__file__).parents[1]


def _blocked_import_script(module: str, expression: str) -> str:
    return f'''
import builtins
original_import = builtins.__import__

def blocked(name, *args, **kwargs):
    if name == "{module}" or name.startswith("{module}."):
        raise ImportError("blocked optional dependency: {module}")
    return original_import(name, *args, **kwargs)

builtins.__import__ = blocked
import fpstreams
try:
    {expression}
except ImportError as error:
    print(error)
else:
    raise SystemExit("optional-backend call unexpectedly succeeded")
'''


def test_arrow_adapter_reports_a_clean_missing_extra_error() -> None:
    """An installed developer extra cannot hide the supported no-Arrow configuration."""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            _blocked_import_script(
                "pyarrow",
                "fpstreams.rows([{'id': 1}]).to_arrow()",
            ),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "blocked optional dependency: pyarrow" in result.stdout


def test_pandas_adapter_reports_a_clean_missing_extra_error() -> None:
    """The data adapter preserves its documented missing-extra failure path."""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            _blocked_import_script(
                "pandas",
                "fpstreams.rows([{'id': 1}]).to_pandas()",
            ),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "to_pandas() requires the 'data' extra" in result.stdout


# --- Consolidated from release/test_resource_high_water.py ---

"""End-to-end bounds for query-owned async physical scheduler resources."""


from collections.abc import AsyncIterator
from typing import Any, cast

from fpstreams import aflow
from fpstreams.execution.async_scheduler import execute_async_physical
from fpstreams.physical.async_plan import compile_async_query


@pytest.mark.asyncio
async def test_concurrent_map_task_high_water_is_independent_of_input_length() -> None:
    async def identity(value: int) -> int:
        await asyncio.sleep(0)
        return value

    physical = compile_async_query(
        cast(Any, aflow(range(1_000_000)))
        .map_async(identity, concurrency=4, ordered=False)
        ._query("iterate")
    )
    runtime = QueryRuntime(QueryLimits(max_tasks=16))
    count = 0
    async for _item in execute_async_physical(physical, runtime):
        count += 1

    assert count == 1_000_000
    assert runtime.metrics.high_water_tasks <= 4
    assert runtime.metrics.live_tasks == 0


@pytest.mark.asyncio
async def test_merge_task_high_water_tracks_only_live_sources() -> None:
    async def source(offset: int) -> AsyncIterator[int]:
        for value in range(100):
            yield offset + value
            await asyncio.sleep(0)

    physical = compile_async_query(aflow(source(0)).merge(source(1_000))._query("iterate"))
    runtime = QueryRuntime(QueryLimits(max_tasks=16))
    result = [item async for item in execute_async_physical(physical, runtime)]

    assert sorted(result) == [*range(100), *range(1_000, 1_100)]
    assert runtime.metrics.high_water_tasks <= 2
    assert runtime.metrics.live_tasks == 0


@pytest.mark.asyncio
async def test_timer_node_owns_at_most_one_timer_task() -> None:
    physical = compile_async_query(aflow(range(3)).delay(0.001)._query("iterate"))
    runtime = QueryRuntime(QueryLimits(max_tasks=4))

    assert [item async for item in execute_async_physical(physical, runtime)] == [0, 1, 2]
    assert runtime.metrics.high_water_tasks <= 1
    assert runtime.metrics.live_tasks == 0
