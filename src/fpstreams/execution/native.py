"""Dispatch compiled numeric programs to type- and source-specific Rust kernels."""

from __future__ import annotations

import sys
from collections.abc import Iterator
from typing import Any, cast

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
_PROBE_MAX_ITEMS = 256
_SHORT_CIRCUIT_TERMINALS = {"first", "any", "all"}
_MATERIALIZE_TARGETS = {"list": 0, "tuple": 1, "set": 2}


def materialize_available(program: NativeProgram) -> bool:
    """Return whether this extension exposes the direct container endpoint.

    This deliberately checks only the new optional capability. Older wheels may
    still execute native programs through their established iterator endpoint.
    """
    suffix = "_range" if isinstance(program.source, range) else ""
    return hasattr(_native, f"materialize_{program.kind}{suffix}")


def execute_materialize(program: NativeProgram, target: str) -> Any:
    """Run one complete numeric program into its final Python collection."""
    target_code = _MATERIALIZE_TARGETS[target]
    source = program.source
    stages = list(program.stages)
    suffix = "_range" if isinstance(source, range) else ""
    materialize = getattr(_native, f"materialize_{program.kind}{suffix}")
    if isinstance(source, range):
        return materialize(source.start, source.stop, source.step, stages, target_code)
    return materialize(source, stages, target_code)


def _probe_container_terminal(
    program: NativeProgram, terminal: str, stages: list[Any]
) -> tuple[bool, int | float | None] | None:
    """Try a bounded, non-copying list/tuple terminal probe when the wheel has it.

    A probe preserves fused Rust stage state while extracting at most a small
    prefix under the GIL. ``None`` means either no probe is applicable or it
    needs the legacy detached bulk kernel to finish a full scan.
    """
    source = program.source
    if terminal not in _SHORT_CIRCUIT_TERMINALS or type(source) not in (list, tuple):
        return None
    probe = getattr(_native, f"terminal_{program.kind}_probe", None)
    if probe is None:
        return None
    completed, result = probe(source, stages, _TERMINALS[terminal], _PROBE_MAX_ITEMS)
    return (True, result) if completed else None


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
    probed = _probe_container_terminal(program, terminal, stages)
    if probed is not None:
        return probed[1]
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


def execute_aggregate(program: NativeProgram, mask: int | None = None) -> NativeAggregateSnapshot:
    """Compute requested built-in fields in one fused Rust traversal.

    New extensions accept an optional field mask while retaining the established
    count/sum/min/max/first/last/mean/M2 tuple. If that endpoint is absent, an
    older wheel safely computes the full snapshot instead.
    """
    source = program.source
    stages = list(program.stages)
    suffix = "_range" if isinstance(source, range) else ""
    if mask is not None:
        masked = getattr(_native, f"aggregate_{program.kind}{suffix}_masked", None)
        if masked is not None:
            if isinstance(source, range):
                return cast(
                    NativeAggregateSnapshot,
                    masked(source.start, source.stop, source.step, stages, mask),
                )
            return cast(NativeAggregateSnapshot, masked(source, stages, mask))
    if program.kind == "f64":
        if isinstance(source, range):
            return _native.aggregate_f64_range(source.start, source.stop, source.step, stages)
        return _native.aggregate_f64(source, stages)
    if isinstance(source, range):
        return _native.aggregate_i64_range(source.start, source.stop, source.step, stages)
    return _native.aggregate_i64(source, stages)
