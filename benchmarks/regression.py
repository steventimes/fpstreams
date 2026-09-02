"""Create and compare fpstreams benchmark baselines."""

from __future__ import annotations

import argparse
import fnmatch
import json
import statistics
import sys
import tomllib
from pathlib import Path
from typing import Any

METADATA_FIELDS = ("python_version", "platform", "machine")
PEAK_RESOURCE_FIELDS = frozenset({"peak_rss_bytes", "peak_allocation_bytes"})
ROOT = Path(__file__).parents[1]


def _read(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data.get("metadata"), dict) or not isinstance(data.get("results"), list):
        raise ValueError(f"invalid benchmark report: {path}")
    return data


def _read_baseline(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data.get("metadata"), dict) or not isinstance(data.get("scenarios"), dict):
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
    expected = reports[0]["metadata"]
    for report in reports[1:]:
        for field in METADATA_FIELDS:
            if report["metadata"].get(field) != expected.get(field):
                raise ValueError(f"mixed benchmark metadata: {field}")
        if {item["name"] for item in report["results"]} != {
            item["name"] for item in reports[0]["results"]
        }:
            raise ValueError("benchmark scenario sets differ")


def _baseline(reports: list[dict[str, Any]], provenance: str) -> dict[str, Any]:
    _comparable(reports)
    scenarios: dict[str, dict[str, float]] = {}
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
        resource_names = {
            resource for item in source_items for resource in item.get("resources", {})
        }
        if resource_names:
            scenario["resources"] = {
                resource: statistics.median(
                    float(item.get("resources", {}).get(resource, 0)) for item in source_items
                )
                for resource in sorted(resource_names)
            }
        scenarios[name] = scenario
    return {
        "schema_version": 1,
        "provenance": provenance,
        "metadata": reports[0]["metadata"],
        "scenarios": scenarios,
    }


def _comparison_errors(
    baseline: dict[str, Any], current: dict[str, Any], groups: dict[str, tuple[str, ...]]
) -> list[str]:
    for field in METADATA_FIELDS:
        if current["metadata"].get(field) != baseline["metadata"].get(field):
            return [f"mixed benchmark metadata: {field}"]
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


def _scenario_errors(
    name: str,
    expected: dict[str, Any],
    actual_seconds: float,
    current: dict[str, Any],
) -> tuple[list[str], bool]:
    """Compare one scenario while preserving timing, latency, then resource error order."""
    median = float(expected["median_seconds"])
    if median <= 0:
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


def _resource_errors(name: str, expected: dict[str, Any], current: dict[str, Any]) -> list[str]:
    """Compare exact counters and bounded peak resource metrics for one scenario."""
    expected_resources = expected.get("resources", {})
    actual_resources = current.get("resources", {})
    if set(actual_resources) != set(expected_resources):
        return [f"resource metric sets differ: {name}"]

    errors: list[str] = []
    for resource, expected_value in expected_resources.items():
        actual_value = float(actual_resources[resource])
        baseline_value = float(expected_value)
        if resource in PEAK_RESOURCE_FIELDS:
            if baseline_value <= 0 or actual_value / baseline_value > 1.30:
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
