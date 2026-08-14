from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

import benchmark

ROOT = Path(__file__).resolve().parents[1]
REQUIRED_RESULT_KEYS = {
    "name",
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
        assert len(result["samples_seconds"]) == 2
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


def _coverage_file(percent: float) -> dict[str, Any]:
    covered = round(percent)
    return {
        "summary": {
            "covered_lines": covered,
            "num_statements": 100,
            "covered_branches": 0,
            "num_branches": 0,
        }
    }


def _coverage_payload(
    *, total: float = 90, native: float = 95, spill: float = 95
) -> dict[str, Any]:
    return {
        "totals": {"percent_covered": total},
        "files": {
            "src/fpstreams/planning/native.py": _coverage_file(95),
            "src/fpstreams/planning/source.py": _coverage_file(95),
            "src/fpstreams/execution/native.py": _coverage_file(native),
            "src/fpstreams/tabular/spill.py": _coverage_file(spill),
            "src/fpstreams/tabular/spill_io.py": _coverage_file(spill),
            "src/fpstreams/tabular/spill_limits.py": _coverage_file(spill),
            "src/fpstreams/execution/async_.py": _coverage_file(90),
            "src/fpstreams/execution/async_concurrency.py": _coverage_file(90),
            "src/fpstreams/execution/async_iterators.py": _coverage_file(90),
            "src/fpstreams/execution/async_ops.py": _coverage_file(90),
        },
    }


def _run_coverage_check(
    tmp_path: Path, payload: dict[str, Any]
) -> subprocess.CompletedProcess[str]:
    report = tmp_path / "coverage.json"
    report.write_text(json.dumps(payload), encoding="utf-8")
    return subprocess.run(
        [sys.executable, str(ROOT / "tools" / "check_coverage.py"), str(report)],
        check=False,
        capture_output=True,
        text=True,
    )


def test_coverage_gate_accepts_all_thresholds(tmp_path: Path) -> None:
    result = _run_coverage_check(tmp_path, _coverage_payload())

    assert result.returncode == 0, result.stderr
    assert "coverage thresholds passed" in result.stdout


def test_coverage_gate_rejects_low_total(tmp_path: Path) -> None:
    result = _run_coverage_check(tmp_path, _coverage_payload(total=84.99))

    assert result.returncode == 1
    assert "total: 84.99% < 85.00%" in result.stderr


def test_coverage_gate_rejects_low_focus_group(tmp_path: Path) -> None:
    result = _run_coverage_check(tmp_path, _coverage_payload(spill=89))

    assert result.returncode == 1
    assert "spill: 89.00% < 90.00%" in result.stderr


def test_coverage_gate_checks_native_execution_separately(tmp_path: Path) -> None:
    result = _run_coverage_check(tmp_path, _coverage_payload(native=89))

    assert result.returncode == 1
    assert "native execution: 89.00% < 90.00%" in result.stderr
