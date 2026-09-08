"""Create and compare fpstreams benchmark baselines."""

from __future__ import annotations

import argparse
import fnmatch
import json
import math
import statistics
import sys
import tomllib
from pathlib import Path
from typing import Any

if __package__:
    from .evidence import COMPARABLE_FIELDS, REPORT_SCHEMA_VERSION, metadata_errors
else:
    from evidence import COMPARABLE_FIELDS, REPORT_SCHEMA_VERSION, metadata_errors

METADATA_FIELDS = COMPARABLE_FIELDS
PEAK_RESOURCE_FIELDS = frozenset({"peak_rss_bytes", "peak_allocation_bytes"})
ROOT = Path(__file__).parents[1]


def _read(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if (
        not isinstance(data, dict)
        or not isinstance(data.get("metadata"), dict)
        or not isinstance(data.get("results"), list)
    ):
        raise ValueError(f"invalid benchmark report: {path}")
    return data


def _read_baseline(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if (
        not isinstance(data, dict)
        or not isinstance(data.get("metadata"), dict)
        or not isinstance(data.get("scenarios"), dict)
    ):
        raise ValueError(f"invalid benchmark baseline: {path}")
    return data


def _read_groups(path: Path | None) -> dict[str, tuple[str, ...]]:
    if path is None:
        return {}
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    groups = data.get("group", [])
    if not isinstance(groups, list):
        raise ValueError("benchmark groups must be an array of tables")
    result: dict[str, tuple[str, ...]] = {}
    for entry in groups:
        name = entry.get("name") if isinstance(entry, dict) else None
        patterns = entry.get("patterns") if isinstance(entry, dict) else None
        if (
            not isinstance(name, str)
            or not name
            or not isinstance(patterns, list)
            or not patterns
            or any(not isinstance(pattern, str) or not pattern for pattern in patterns)
            or name in result
        ):
            raise ValueError("invalid benchmark group")
        result[name] = tuple(patterns)
    return result


def _group_coverage_errors(
    results: list[dict[str, Any]], groups: dict[str, tuple[str, ...]]
) -> list[str]:
    """Keep emitted benchmark scenarios in one, and only one, statistical group."""
    errors: list[str] = []
    for result in results:
        name = result.get("name")
        if not isinstance(name, str) or not name.startswith(("fpstreams_", "python_builtin/")):
            continue
        memberships = [
            group
            for group, patterns in groups.items()
            if any(fnmatch.fnmatch(name, pattern) for pattern in patterns)
        ]
        if not memberships:
            errors.append(f"benchmark scenario has no group: {name}")
        elif len(memberships) > 1:
            errors.append(f"benchmark scenario has overlapping groups: {name}")
    return errors


def _comparable(reports: list[dict[str, Any]]) -> None:
    if not reports:
        raise ValueError("baseline requires reports")
    for report in reports:
        errors = _report_errors(report)
        if errors:
            raise ValueError(errors[0])
    for report in reports[1:]:
        errors = _metadata_errors(reports[0]["metadata"], report["metadata"])
        if report.get("schema_version") != reports[0].get("schema_version"):
            errors.insert(0, "mixed benchmark report schema")
        if errors:
            raise ValueError(errors[0])
        if {item["name"] for item in report["results"]} != {
            item["name"] for item in reports[0]["results"]
        }:
            raise ValueError("benchmark scenario sets differ")


def _report_errors(report: dict[str, Any]) -> list[str]:
    if report.get("schema_version") != REPORT_SCHEMA_VERSION:
        return ["mixed benchmark report schema; recreate the baseline from current reports"]
    errors = metadata_errors(report["metadata"])
    if errors:
        return errors
    seen = set()
    for item in report["results"]:
        if not isinstance(item, dict):
            return ["invalid benchmark scenario"]
        name = item.get("name")
        seconds = item.get("median_seconds")
        if not isinstance(name, str) or not name or name in seen:
            return ["invalid or duplicate benchmark scenario name"]
        if type(seconds) not in (int, float) or not math.isfinite(seconds) or seconds <= 0:
            return [f"invalid benchmark timing: {name}"]
        if report["metadata"]["suite"] == "competitive":
            warmups = item.get("warmup_runs")
            if (
                not isinstance(warmups, list)
                or not warmups
                or len(warmups) != item.get("sample_count")
                or any(type(count) is not int or count < 1 for count in warmups)
            ):
                return [f"invalid benchmark warmup evidence: {name}"]
        errors = _resource_metric_errors(name, item.get("resources"))
        if errors:
            return errors
        seen.add(name)
    return [] if seen else ["benchmark report contains no scenarios"]


def _metadata_errors(expected: dict[str, Any], current: dict[str, Any]) -> list[str]:
    # 规模和执行模式必须相同, 版本、提交与代码 hash 允许跨提交变化。
    for field in METADATA_FIELDS:
        if (field in current) != (field in expected) or current.get(field) != expected.get(field):
            return [f"mixed benchmark metadata: {field}"]
    expected_native = expected.get("native", {})
    current_native = current.get("native", {})
    if not isinstance(expected_native, dict) or not isinstance(current_native, dict):
        return ["invalid benchmark native metadata"]
    for field in ("available", "profile"):
        if (field in expected_native) != (field in current_native) or expected_native.get(
            field
        ) != current_native.get(field):
            return [f"mixed benchmark metadata: native.{field}"]
    expected_libraries = expected.get("libraries", {})
    current_libraries = current.get("libraries", {})
    if not isinstance(expected_libraries, dict) or not isinstance(current_libraries, dict):
        return ["invalid benchmark libraries metadata"]
    for library in (expected_libraries.keys() | current_libraries.keys()) - {"fpstreams"}:
        if expected_libraries.get(library) != current_libraries.get(library):
            return [f"mixed benchmark metadata: libraries.{library}"]
    return []


def _baseline(reports: list[dict[str, Any]], provenance: str) -> dict[str, Any]:
    _comparable(reports)
    scenarios: dict[str, dict[str, Any]] = {}
    for name in sorted(item["name"] for item in reports[0]["results"]):
        samples = [
            float(
                next(item for item in report["results"] if item["name"] == name)["median_seconds"]
            )
            for report in reports
        ]
        median = statistics.median(samples)
        scenario: dict[str, Any] = {
            "median_seconds": round(median, 12),
            "mad_seconds": round(statistics.median(abs(sample - median) for sample in samples), 12),
        }
        source_items = [
            next(item for item in report["results"] if item["name"] == name) for report in reports
        ]
        if any("execution" in item for item in source_items):
            scenario["executions"] = [
                item.get("execution", {"status": "unknown"}) for item in source_items
            ]
        first_row_samples = [
            float(item["first_row_seconds"]) for item in source_items if "first_row_seconds" in item
        ]
        if first_row_samples:
            if len(first_row_samples) != len(source_items):
                raise ValueError(f"inconsistent first-row metric: {name}")
            first_row_median = statistics.median(first_row_samples)
            scenario["first_row_seconds"] = round(first_row_median, 12)
            scenario["first_row_mad_seconds"] = round(
                statistics.median(abs(sample - first_row_median) for sample in first_row_samples),
                12,
            )
        resource_names = set(source_items[0]["resources"])
        if any(set(item["resources"]) != resource_names for item in source_items[1:]):
            raise ValueError(f"resource metric sets differ: {name}")
        scenario["resources"] = {
            resource: statistics.median(item["resources"][resource] for item in source_items)
            for resource in sorted(resource_names)
        }
        scenarios[name] = scenario
    return {
        "schema_version": 2,
        "report_schema_version": reports[0].get("schema_version"),
        "provenance": provenance,
        "metadata": reports[0]["metadata"],
        "scenarios": scenarios,
        "runs": [report["metadata"] for report in reports],
    }


def _comparison_errors(
    baseline: dict[str, Any], current: dict[str, Any], groups: dict[str, tuple[str, ...]]
) -> list[str]:
    if baseline.get("schema_version") != 2:
        return ["unsupported benchmark baseline schema; recreate the baseline"]
    errors = _report_errors(current) or metadata_errors(baseline["metadata"])
    if errors:
        return errors
    if current.get("schema_version") != baseline.get("report_schema_version"):
        return ["mixed benchmark report schema; recreate the baseline from comparable reports"]
    provenance_errors = _baseline_provenance_errors(baseline)
    if provenance_errors:
        return provenance_errors
    comparison_errors = _metadata_errors(baseline["metadata"], current["metadata"])
    if comparison_errors:
        return comparison_errors
    expected = baseline.get("scenarios", {})
    actual = {item["name"]: float(item["median_seconds"]) for item in current["results"]}
    current_by_name: dict[str, dict[str, Any]] = {}
    for item in current["results"]:
        current_by_name.setdefault(item["name"], item)
    if set(actual) != set(expected):
        return ["benchmark scenario sets differ"]
    errors = _group_coverage_errors(current["results"], groups)
    timing_regressions: set[str] = set()
    for name, values in expected.items():
        scenario_errors, timing_regression = _scenario_errors(
            name,
            values,
            actual[name],
            current_by_name[name],
        )
        errors.extend(scenario_errors)
        if timing_regression:
            timing_regressions.add(name)
    errors.extend(_group_timing_errors(expected, actual, groups, timing_regressions))
    return errors


def _baseline_provenance_errors(baseline: dict[str, Any]) -> list[str]:
    runs = baseline.get("runs")
    if not isinstance(runs, list) or not runs:
        return ["missing baseline run provenance; recreate the baseline"]
    for run in runs:
        if not isinstance(run, dict):
            return ["invalid baseline run provenance"]
        errors = metadata_errors(run) or _metadata_errors(baseline["metadata"], run)
        if errors:
            return errors
    return []


def _scenario_errors(
    name: str,
    expected: dict[str, Any],
    actual_seconds: float,
    current: dict[str, Any],
) -> tuple[list[str], bool]:
    """Compare one scenario while preserving timing, latency, then resource error order."""
    median = float(expected["median_seconds"])
    if not math.isfinite(median) or median <= 0:
        return [f"invalid baseline timing: {name}"], False

    errors: list[str] = []
    ratio = actual_seconds / median
    timing_regression = ratio > max(
        1.10,
        1 + 4 * float(expected["mad_seconds"]) / median,
    )
    if ratio > 1.25:
        errors.append(f"hard timing regression: {name} ({ratio:.3f}x)")
        timing_regression = False
    errors.extend(_first_row_errors(name, expected, current))
    errors.extend(_resource_errors(name, expected, current))
    return errors, timing_regression


def _first_row_errors(name: str, expected: dict[str, Any], current: dict[str, Any]) -> list[str]:
    """Compare optional first-row latency for one scenario."""
    if "first_row_seconds" not in expected:
        return []
    if "first_row_seconds" not in current:
        return [f"missing first-row metric: {name}"]
    ratio = float(current["first_row_seconds"]) / float(expected["first_row_seconds"])
    return [f"hard first-row regression: {name} ({ratio:.3f}x)"] if ratio > 1.30 else []


def _resource_metric_errors(name: str, resources: Any) -> list[str]:
    """Require measured allocation evidence and finite, nonnegative resource values."""
    if not isinstance(resources, dict) or "peak_allocation_bytes" not in resources:
        return [f"missing benchmark resource: {name}.peak_allocation_bytes"]
    for resource, value in resources.items():
        if (
            not isinstance(resource, str)
            or not resource
            or type(value) not in (int, float)
            or not math.isfinite(value)
            or value < 0
        ):
            return [f"invalid benchmark resource: {name}.{resource}"]
    return []


def _resource_errors(name: str, expected: dict[str, Any], current: dict[str, Any]) -> list[str]:
    """Compare exact counters and bounded peak resource metrics for one scenario."""
    errors = _resource_metric_errors(name, expected.get("resources"))
    if errors:
        return errors
    expected_resources = expected["resources"]
    actual_resources = current["resources"]
    if set(actual_resources) != set(expected_resources):
        return [f"resource metric sets differ: {name}"]

    errors: list[str] = []
    for resource, expected_value in expected_resources.items():
        actual_value = float(actual_resources[resource])
        baseline_value = float(expected_value)
        if resource in PEAK_RESOURCE_FIELDS:
            if (
                not math.isfinite(baseline_value)
                or not math.isfinite(actual_value)
                or baseline_value < 0
                or actual_value < 0
                or actual_value > baseline_value * 1.30
            ):
                errors.append(f"hard peak resource regression: {name}.{resource}")
        elif actual_value != baseline_value:
            errors.append(f"hard resource invariant: {name}.{resource}")
    return errors


def _group_timing_errors(
    expected: dict[str, Any],
    actual: dict[str, float],
    groups: dict[str, tuple[str, ...]],
    timing_regressions: set[str],
) -> list[str]:
    """Reject a group only when noisy individual regressions move its geometric mean."""
    errors: list[str] = []
    for group, patterns in groups.items():
        names = [
            name for name in actual if any(fnmatch.fnmatch(name, pattern) for pattern in patterns)
        ]
        if not names or not timing_regressions.intersection(names):
            continue
        ratio = statistics.geometric_mean(
            actual[name] / float(expected[name]["median_seconds"]) for name in names
        )
        if ratio > 1.05:
            errors.append(f"group timing regression: {group} ({ratio:.3f}x)")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--create-baseline", action="store_true")
    parser.add_argument("--provenance", choices=("local_one_shot_unreviewed", "release_approved"))
    parser.add_argument("--output", type=Path)
    parser.add_argument("--groups", type=Path, default=ROOT / "benchmarks" / "groups.toml")
    parser.add_argument("inputs", nargs="+", type=Path)
    arguments = parser.parse_args()
    if not arguments.create_baseline:
        if len(arguments.inputs) != 2:
            parser.error("comparison requires BASELINE.json CURRENT.json")
        try:
            baseline = _read_baseline(arguments.inputs[0])
            errors = _comparison_errors(
                baseline, _read(arguments.inputs[1]), _read_groups(arguments.groups)
            )
        except (OSError, ValueError, json.JSONDecodeError, tomllib.TOMLDecodeError) as error:
            parser.error(str(error))
        if errors:
            print("\n".join(errors), file=sys.stderr)
            return 1
        return 0
    if arguments.provenance is None or arguments.output is None:
        parser.error("--create-baseline requires --provenance and --output")
    if len(arguments.inputs) != 3:
        parser.error("baseline creation requires exactly three reports")
    if arguments.provenance != "release_approved" and "benchmarks/baselines" in str(
        arguments.output
    ):
        parser.error("unreviewed baseline cannot be written under benchmarks/baselines")
    try:
        reports = [_read(path) for path in arguments.inputs]
        groups = _read_groups(arguments.groups)
        coverage_errors = _group_coverage_errors(reports[0]["results"], groups)
        if coverage_errors:
            raise ValueError("; ".join(coverage_errors))
        baseline = _baseline(reports, arguments.provenance)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        parser.error(str(error))
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(
        json.dumps(baseline, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
