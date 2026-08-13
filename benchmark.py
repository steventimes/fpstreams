from __future__ import annotations

import argparse
import statistics
import time
from collections.abc import Callable
from typing import Any

from fpstreams import Flow, fitem, flow, item


def measure(function: Callable[[], Any], repeats: int) -> float:
    durations: list[float] = []
    for _ in range(repeats):
        started = time.perf_counter()
        function()
        durations.append(time.perf_counter() - started)
    return statistics.median(durations)


def report(label: str, pipeline: Flow[Any], size: int, repeats: int) -> None:
    scenarios = {
        "python/list": lambda: pipeline.with_engine("python").to_list(),
        "native/list": lambda: pipeline.with_engine("native").to_list(),
        "python/sum": lambda: pipeline.with_engine("python").sum(),
        "native/sum": lambda: pipeline.with_engine("native").sum(),
        "python/mean": lambda: pipeline.with_engine("python").mean(),
        "native/mean": lambda: pipeline.with_engine("native").mean(),
        "python/variance": lambda: pipeline.with_engine("python").variance(),
        "native/variance": lambda: pipeline.with_engine("native").variance(),
    }
    results = {name: measure(task, repeats) for name, task in scenarios.items()}

    print(f"fpstreams v2 · {size:,} {label} values · median of {repeats}")
    for pair in (
        ("python/list", "native/list"),
        ("python/sum", "native/sum"),
        ("python/mean", "native/mean"),
        ("python/variance", "native/variance"),
    ):
        python_name, native_name = pair
        python_time = results[python_name]
        native_time = results[native_name]
        print(
            f"{python_name:<15} {python_time:>9.6f}s  "
            f"{native_name:<15} {native_time:>9.6f}s  "
            f"speedup {python_time / native_time:>6.2f}x"
        )


def report_first(label: str, pipeline: Flow[Any], size: int, repeats: int) -> None:
    python_time = measure(lambda: pipeline.with_engine("python").first(), repeats)
    native_time = measure(lambda: pipeline.with_engine("native").first(), repeats)
    print(f"fpstreams v2 · {size:,} {label} values · median of {repeats}")
    print(
        f"{'python/first':<12} {python_time:>9.6f}s  "
        f"{'native/first':<12} {native_time:>9.6f}s  "
        f"speedup {python_time / native_time:>6.2f}x"
    )


def report_hybrid(label: str, pipeline: Flow[Any], size: int, repeats: int) -> None:
    explanation = pipeline.explain().to_dict()
    if explanation["selected_engine"] != "hybrid":
        raise RuntimeError(f"hybrid benchmark was planned as {explanation['selected_engine']!r}")
    python_time = measure(lambda: pipeline.with_engine("python").to_list(), repeats)
    hybrid_time = measure(pipeline.to_list, repeats)
    print(f"fpstreams v2 · {size:,} {label} values · median of {repeats}")
    print(
        f"{'python/list':<12} {python_time:>9.6f}s  "
        f"{'hybrid/list':<12} {hybrid_time:>9.6f}s  "
        f"speedup {python_time / hybrid_time:>6.2f}x"
    )


def run(size: int, repeats: int, domain: str) -> None:
    if domain in {"int", "both"}:
        integers = flow(range(size)).map(item * 3 + 1).filter(item % 2 == 0)
        report("integer", integers, size, repeats)
        report_hybrid("integer hybrid chunk", integers.chunk(64), size, repeats)
        report_hybrid(
            "integer hybrid Python map",
            integers.map(lambda value: value.bit_count()),
            size,
            repeats,
        )
        cardinality = max(1, size // 100)
        distinct = flow(range(size)).map(item % cardinality).unique()
        report(f"integer distinct ({cardinality:,} unique)", distinct, size, repeats)
        start = size // 3
        width = max(1, size // 100)
        bounded = (
            flow(range(size))
            .drop_while(item < start)
            .take_while(item < start + width)
            .map(item * 3 + 1)
        )
        report(f"integer bounded while ({width:,} output)", bounded, size, repeats)
        first_match = flow(range(size)).map(item * 3 + 1).filter(item >= size * 2)
        report_first("integer first-match terminal", first_match, size, repeats)
    if domain in {"float", "both"}:
        values = [value / 10 for value in range(size)]
        floats = flow(values).map(fitem * 1.25 + 0.5).filter((fitem > 100.0) & (fitem < size / 10))
        report("floating-point", floats, size, repeats)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", type=int, default=1_000_000)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--domain", choices=("int", "float", "both"), default="int")
    arguments = parser.parse_args()
    run(arguments.size, arguments.repeats, arguments.domain)
