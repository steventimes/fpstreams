"""Repeatable fpstreams performance baselines with machine-readable output."""

from __future__ import annotations

import argparse
import json
import platform
import statistics
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any, Literal

import fpstreams
from fpstreams import fitem, flow, item

Backend = Literal["python-builtin", "python", "native", "auto"]
Task = Callable[[], object]


@dataclass(frozen=True, slots=True)
class Scenario:
    name: str
    task: Task
    backend: Backend
    source_kind: str
    terminal: str
    baseline: str | None


def measure(function: Task, repeats: int) -> list[float]:
    durations: list[float] = []
    for _ in range(repeats):
        started = time.perf_counter()
        function()
        durations.append(time.perf_counter() - started)
    return durations


def _record(scenario: Scenario, repeats: int) -> dict[str, Any]:
    samples = measure(scenario.task, repeats)
    return {
        "name": scenario.name,
        "samples_seconds": samples,
        "median_seconds": statistics.median(samples),
        "stdev_seconds": statistics.stdev(samples) if len(samples) > 1 else 0.0,
        "backend": scenario.backend,
        "source_kind": scenario.source_kind,
        "terminal": scenario.terminal,
        "baseline": scenario.baseline,
    }


def _builtin_task(source: Sequence[int] | range, terminal: str) -> Task:
    if terminal == "sum":
        return lambda: sum(source)
    if terminal == "count":
        return lambda: len(source)
    raise ValueError(f"unsupported identity terminal {terminal!r}")


def _flow_task(source: Sequence[int] | range, backend: Backend, terminal: str) -> Task:
    pipeline = flow(source)
    selected = pipeline.with_engine(backend) if backend in {"python", "native"} else pipeline
    if terminal == "sum":
        return selected.sum
    if terminal == "count":
        return selected.count
    raise ValueError(f"unsupported identity terminal {terminal!r}")


def _python_map_filter(source: Sequence[int] | range) -> list[int]:
    return [value * 3 + 1 for value in source if (value * 3 + 1) % 2 == 0]


def _sum_task(task: Callable[[], Sequence[int]]) -> int:
    return sum(task())


def _identity_scenarios(size: int, *, include_tuple: bool) -> list[Scenario]:
    sources: list[tuple[str, Sequence[int] | range]] = [
        ("list", list(range(size))),
        ("range", range(size)),
    ]
    if include_tuple:
        sources.append(("tuple", tuple(range(size))))

    scenarios: list[Scenario] = []
    for source_kind, source in sources:
        for terminal in ("sum", "count"):
            builtin_name = f"python_builtin/{source_kind}/identity/{terminal}"
            python_name = f"fpstreams_python/{source_kind}/identity/{terminal}"
            scenarios.extend(
                [
                    Scenario(
                        builtin_name,
                        _builtin_task(source, terminal),
                        "python-builtin",
                        source_kind,
                        terminal,
                        None,
                    ),
                    Scenario(
                        python_name,
                        _flow_task(source, "python", terminal),
                        "python",
                        source_kind,
                        terminal,
                        builtin_name,
                    ),
                    Scenario(
                        f"fpstreams_auto/{source_kind}/identity/{terminal}",
                        _flow_task(source, "auto", terminal),
                        "auto",
                        source_kind,
                        terminal,
                        python_name,
                    ),
                ]
            )
    return scenarios


def _integer_pipeline_scenarios(size: int, native_available: bool) -> list[Scenario]:
    scenarios: list[Scenario] = []
    for source_kind, source in (("list", list(range(size))), ("range", range(size))):
        builtin_values = partial(_python_map_filter, source)
        pipeline = flow(source).map(item * 3 + 1).filter(item % 2 == 0)
        lambda_pipeline = (
            flow(source)
            .map(lambda value: value * 3 + 1)
            .filter(lambda value: value % 2 == 0)
            .with_engine("python")
        )
        for terminal in ("list", "sum"):
            builtin_name = f"python_builtin/{source_kind}/map_filter/{terminal}"
            builtin_task: Task = (
                builtin_values if terminal == "list" else partial(_sum_task, builtin_values)
            )
            scenarios.append(
                Scenario(
                    builtin_name,
                    builtin_task,
                    "python-builtin",
                    source_kind,
                    terminal,
                    None,
                )
            )
            lambda_name = f"fpstreams_lambda/{source_kind}/map_filter/{terminal}"
            scenarios.append(
                Scenario(
                    lambda_name,
                    lambda_pipeline.to_list if terminal == "list" else lambda_pipeline.sum,
                    "python",
                    source_kind,
                    terminal,
                    builtin_name,
                )
            )
            python_name = f"fpstreams_python/{source_kind}/map_filter/{terminal}"
            python_pipeline = pipeline.with_engine("python")
            scenarios.append(
                Scenario(
                    python_name,
                    python_pipeline.to_list if terminal == "list" else python_pipeline.sum,
                    "python",
                    source_kind,
                    terminal,
                    lambda_name,
                )
            )
            if native_available:
                native_pipeline = pipeline.with_engine("native")
                scenarios.append(
                    Scenario(
                        f"fpstreams_native/{source_kind}/map_filter/{terminal}",
                        native_pipeline.to_list if terminal == "list" else native_pipeline.sum,
                        "native",
                        source_kind,
                        terminal,
                        python_name,
                    )
                )
            scenarios.append(
                Scenario(
                    f"fpstreams_auto/{source_kind}/map_filter/{terminal}",
                    pipeline.to_list if terminal == "list" else pipeline.sum,
                    "auto",
                    source_kind,
                    terminal,
                    python_name,
                )
            )
    return scenarios


def _float_scenarios(size: int, native_available: bool) -> list[Scenario]:
    values = [value / 10 for value in range(size)]
    pipeline = flow(values).map(fitem * 1.25 + 0.5).filter(fitem > 10.0)
    scenarios: list[Scenario] = []
    python_pipeline = pipeline.with_engine("python")
    lambda_pipeline = (
        flow(values)
        .map(lambda value: value * 1.25 + 0.5)
        .filter(lambda value: value > 10.0)
        .with_engine("python")
    )
    for terminal in ("list", "sum"):
        lambda_name = f"fpstreams_lambda/list/float_map_filter/{terminal}"
        scenarios.append(
            Scenario(
                lambda_name,
                lambda_pipeline.to_list if terminal == "list" else lambda_pipeline.sum,
                "python",
                "list",
                terminal,
                None,
            )
        )
        python_name = f"fpstreams_python/list/float_map_filter/{terminal}"
        scenarios.append(
            Scenario(
                python_name,
                python_pipeline.to_list if terminal == "list" else python_pipeline.sum,
                "python",
                "list",
                terminal,
                lambda_name,
            )
        )
        if native_available:
            native_pipeline = pipeline.with_engine("native")
            scenarios.append(
                Scenario(
                    f"fpstreams_native/list/float_map_filter/{terminal}",
                    native_pipeline.to_list if terminal == "list" else native_pipeline.sum,
                    "native",
                    "list",
                    terminal,
                    python_name,
                )
            )
        scenarios.append(
            Scenario(
                f"fpstreams_auto/list/float_map_filter/{terminal}",
                pipeline.to_list if terminal == "list" else pipeline.sum,
                "auto",
                "list",
                terminal,
                python_name,
            )
        )
    return scenarios


def _small_plan_scenario() -> Scenario:
    pipeline = flow(range(4)).map(item + 1)
    return Scenario(
        "fpstreams_auto/range/small_plan/explain",
        lambda: pipeline.explain("list").to_dict(),
        "auto",
        "range",
        "explain",
        None,
    )


def _one_shot_task(size: int, backend: Backend) -> Task:
    def execute() -> int:
        pipeline = flow(iter(range(size))).map(lambda value: value + 1)
        selected = pipeline.with_engine(backend) if backend == "python" else pipeline
        return selected.sum()

    return execute


def _one_shot_scenarios(size: int) -> list[Scenario]:
    builtin_name = "python_builtin/one-shot/identity/sum"
    python_name = "fpstreams_python/one-shot/map/sum"
    return [
        Scenario(
            builtin_name,
            lambda: sum(iter(range(size))),
            "python-builtin",
            "one-shot",
            "sum",
            None,
        ),
        Scenario(
            python_name,
            _one_shot_task(size, "python"),
            "python",
            "one-shot",
            "sum",
            builtin_name,
        ),
        Scenario(
            "fpstreams_auto/one-shot/map/sum",
            _one_shot_task(size, "auto"),
            "auto",
            "one-shot",
            "sum",
            python_name,
        ),
    ]


def native_build_metadata() -> dict[str, object]:
    try:
        from fpstreams import _native
    except ImportError:
        return {"available": False, "profile": "unavailable", "path": None}
    profile = _native.build_profile() if hasattr(_native, "build_profile") else "unknown"
    return {
        "available": True,
        "profile": profile,
        "path": str(getattr(_native, "__file__", "")) or None,
    }


def find_regressions(
    records: Sequence[Mapping[str, Any]],
    *,
    maximum_ratio: float = 1.10,
) -> list[dict[str, Any]]:
    by_name = {str(record["name"]): record for record in records}
    regressions: list[dict[str, Any]] = []
    for record in records:
        name = str(record.get("name", ""))
        identity_guard = (
            record.get("backend") == "auto"
            and record.get("source_kind") in {"list", "tuple"}
            and "/identity/" in name
            and record.get("terminal") == "sum"
        )
        expression_guard = name.startswith("fpstreams_python/") and (
            "/map_filter/" in name or "/float_map_filter/" in name
        )
        if not identity_guard and not expression_guard:
            continue
        baseline_name = record.get("baseline")
        baseline = by_name.get(str(baseline_name))
        if baseline is None:
            continue
        baseline_seconds = float(baseline["median_seconds"])
        ratio = float(record["median_seconds"]) / baseline_seconds if baseline_seconds else 1.0
        allowed = 2.0 if expression_guard else maximum_ratio
        if ratio > allowed:
            regressions.append(
                {
                    "name": record["name"],
                    "baseline": baseline_name,
                    "ratio": ratio,
                    "maximum_ratio": allowed,
                }
            )
    return regressions


def run(
    *,
    size: int,
    repeats: int,
    domain: str = "int",
    quick: bool = False,
    fail_on_regression: bool = False,
) -> dict[str, Any]:
    if size < 1:
        raise ValueError("size must be positive")
    if repeats < 1:
        raise ValueError("repeats must be positive")
    if domain not in {"int", "float", "both"}:
        raise ValueError("domain must be 'int', 'float', or 'both'")

    native = native_build_metadata()
    if fail_on_regression and native["profile"] != "release":
        raise RuntimeError(
            "--fail-on-regression requires a confirmed release native build; "
            f"detected {native['profile']!r}"
        )

    scenarios: list[Scenario] = []
    if domain in {"int", "both"}:
        scenarios.extend(_identity_scenarios(size, include_tuple=not quick))
        scenarios.append(_small_plan_scenario())
        scenarios.extend(_one_shot_scenarios(size))
        if not quick:
            scenarios.extend(_integer_pipeline_scenarios(size, bool(native["available"])))
    if domain in {"float", "both"}:
        scenarios.extend(_float_scenarios(size, bool(native["available"])))

    results = [_record(scenario, repeats) for scenario in scenarios]
    return {
        "schema_version": 1,
        "metadata": {
            "fpstreams_version": fpstreams.__version__,
            "python_version": platform.python_version(),
            "implementation": platform.python_implementation(),
            "platform": platform.platform(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "native": native,
            "size": size,
            "repeats": repeats,
            "domain": domain,
            "quick": quick,
        },
        "results": results,
        "regressions": find_regressions(results),
    }


def render(report: Mapping[str, Any]) -> None:
    metadata = report["metadata"]
    print(
        "fpstreams benchmark · "
        f"Python {metadata['python_version']} · {metadata['platform']} · "
        f"native {metadata['native']['profile']}"
    )
    for result in report["results"]:
        print(
            f"{result['name']:<52} {result['median_seconds']:>10.6f}s "
            f"± {result['stdev_seconds']:.6f}s"
        )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", type=int, default=1_000_000)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--domain", choices=("int", "float", "both"), default="int")
    parser.add_argument("--json", type=Path)
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--fail-on-regression", action="store_true")
    arguments = parser.parse_args()

    try:
        report = run(
            size=arguments.size,
            repeats=arguments.repeats,
            domain=arguments.domain,
            quick=arguments.quick,
            fail_on_regression=arguments.fail_on_regression,
        )
    except (RuntimeError, ValueError) as error:
        parser.error(str(error))
    render(report)
    if arguments.json is not None:
        arguments.json.parent.mkdir(parents=True, exist_ok=True)
        arguments.json.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    if arguments.fail_on_regression and report["regressions"]:
        for regression in report["regressions"]:
            print(
                f"regression: {regression['name']} is {regression['ratio']:.3f}x its "
                f"same-run Python baseline (limit {regression['maximum_ratio']:.2f}x)",
                file=sys.stderr,
            )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
