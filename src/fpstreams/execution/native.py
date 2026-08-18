"""Dispatch compiled numeric programs to type- and source-specific Rust kernels."""

from __future__ import annotations

import sys
from collections.abc import Iterator
from typing import Any

from .. import _native
from ..collecting.statistics import StatisticsSnapshot
from ..planning.native import NativeProgram

NativeAggregateSnapshot = tuple[
    int,
    int | float,
    int | float | None,
    int | float | None,
    int | float | None,
    int | float | None,
    float,
    float,
]

_TERMINALS = {
    "count": 0,
    "sum": 1,
    "min": 2,
    "max": 3,
    "last": 4,
    "first": 5,
    "any": 6,
    "all": 7,
}


def execute(program: NativeProgram) -> Iterator[Any]:
    """Run all fused stages in Rust and iterate the materialized numeric output.

    Float and integer programs use separate kernels, with range sources routed to
    specialized start/stop/step entry points.
    """
    source = program.source
    stages = list(program.stages)
    values: list[int] | list[float]
    if program.kind == "f64":
        if isinstance(source, range):
            values = _native.execute_f64_range(source.start, source.stop, source.step, stages)
        else:
            values = _native.execute_f64(source, stages)
    elif isinstance(source, range):
        values = _native.execute_i64_range(source.start, source.stop, source.step, stages)
    else:
        values = _native.execute_i64(source, stages)
    return iter(values)


def execute_terminal(program: NativeProgram, terminal: str) -> int | float | None:
    """Reduce a fused program with the Rust terminal identified by terminal.

    Float counts use dedicated count kernels; every other terminal is encoded for
    the generic integer or float reducer, including range-specialized variants.
    """
    source = program.source
    stages = list(program.stages)
    code = _TERMINALS[terminal]
    if program.kind == "f64":
        # Opcode 8 matches Python 3.11 sequential float sums; newer Python uses
        # compensated summation.
        if terminal == "sum" and sys.version_info < (3, 12):
            code = 8
        if terminal == "count":
            if isinstance(source, range):
                return _native.count_f64_range(source.start, source.stop, source.step, stages)
            return _native.count_f64(source, stages)
        if isinstance(source, range):
            return _native.terminal_f64_range(source.start, source.stop, source.step, stages, code)
        return _native.terminal_f64(source, stages, code)
    if isinstance(source, range):
        return _native.terminal_i64_range(source.start, source.stop, source.step, stages, code)
    return _native.terminal_i64(source, stages, code)


def execute_statistics(program: NativeProgram) -> StatisticsSnapshot:
    """Compute count, mean, and squared deviations in one Rust traversal.

    The program's numeric kind and whether its source is a range select the concrete
    extension entry point.
    """
    source = program.source
    stages = list(program.stages)
    if program.kind == "f64":
        if isinstance(source, range):
            return _native.statistics_f64_range(source.start, source.stop, source.step, stages)
        return _native.statistics_f64(source, stages)
    if isinstance(source, range):
        return _native.statistics_i64_range(source.start, source.stop, source.step, stages)
    return _native.statistics_i64(source, stages)


def execute_aggregate(program: NativeProgram) -> NativeAggregateSnapshot:
    """Compute all built-in numeric aggregate fields in one fused Rust traversal.

    The returned tuple is count, sum, minimum, maximum, first, last, mean, and
    squared deviations.
    """
    source = program.source
    stages = list(program.stages)
    if program.kind == "f64":
        if isinstance(source, range):
            return _native.aggregate_f64_range(source.start, source.stop, source.step, stages)
        return _native.aggregate_f64(source, stages)
    if isinstance(source, range):
        return _native.aggregate_i64_range(source.start, source.stop, source.step, stages)
    return _native.aggregate_i64(source, stages)
