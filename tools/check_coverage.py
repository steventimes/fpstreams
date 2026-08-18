"""Enforce total and focus-area line-plus-branch thresholds from coverage.py JSON."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

TOTAL_THRESHOLD = 85.0
GROUPS: dict[str, tuple[float, Callable[[str], bool]]] = {
    "planning": (90.0, lambda path: path.startswith("src/fpstreams/planning/")),
    "native execution": (90.0, lambda path: path == "src/fpstreams/execution/native.py"),
    "spill": (90.0, lambda path: path.startswith("src/fpstreams/tabular/spill")),
    "async execution": (85.0, lambda path: path.startswith("src/fpstreams/execution/async")),
}


def _combined_percent(summaries: list[Mapping[str, Any]]) -> float:
    covered = sum(
        int(summary["covered_lines"]) + int(summary.get("covered_branches", 0))
        for summary in summaries
    )
    possible = sum(
        int(summary["num_statements"]) + int(summary.get("num_branches", 0))
        for summary in summaries
    )
    return 100.0 if possible == 0 else covered * 100 / possible


def check(report: Mapping[str, Any]) -> tuple[list[str], list[str]]:
    failures: list[str] = []
    errors: list[str] = []
    try:
        total = float(report["totals"]["percent_covered"])
        files = report["files"]
    except (KeyError, TypeError, ValueError):
        return [], ["coverage report is missing totals.percent_covered or files"]
    if not isinstance(files, Mapping):
        return [], ["coverage report files must be an object"]
    if total < TOTAL_THRESHOLD:
        failures.append(f"total: {total:.2f}% < {TOTAL_THRESHOLD:.2f}%")

    normalized = {str(path).replace("\\", "/"): details for path, details in files.items()}
    for name, (threshold, matches) in GROUPS.items():
        summaries: list[Mapping[str, Any]] = []
        for path, details in normalized.items():
            if not matches(path):
                continue
            try:
                summary = details["summary"]
            except (KeyError, TypeError):
                errors.append(f"{name}: {path} is missing summary data")
                continue
            if not isinstance(summary, Mapping):
                errors.append(f"{name}: {path} summary must be an object")
                continue
            summaries.append(summary)
        if not summaries:
            errors.append(f"{name}: no matching files in coverage report")
            continue
        try:
            percent = _combined_percent(summaries)
        except (KeyError, TypeError, ValueError):
            errors.append(f"{name}: invalid line or branch totals")
            continue
        if percent < threshold:
            failures.append(f"{name}: {percent:.2f}% < {threshold:.2f}%")
    return failures, errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("report", type=Path)
    arguments = parser.parse_args()
    try:
        report = json.loads(arguments.report.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        print(f"coverage report error: {error}", file=sys.stderr)
        return 2
    failures, errors = check(report)
    if errors:
        for error in errors:
            print(f"coverage report error: {error}", file=sys.stderr)
        return 2
    if failures:
        for failure in failures:
            print(f"coverage threshold failed: {failure}", file=sys.stderr)
        return 1
    print("coverage thresholds passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
