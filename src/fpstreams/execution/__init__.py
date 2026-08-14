"""Engine routing for materializing and terminal execution."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from ..collecting.statistics import StatisticsSnapshot
from ..errors import NativeUnsupportedError
from ..planning.native import (
    TerminalName,
    select_materializing_engine,
    select_terminal_engine,
)
from ..planning.native import exact_count as exact_count
from ..planning.source import Source
from ..planning.sync import Plan
from .sync import execute as execute_python

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


def execute(plan: Plan, *, auto_native: bool = True) -> Iterator[Any]:
    if not auto_native and plan.engine == "auto":
        yield from execute_python(plan)
        return
    decision = select_materializing_engine(plan)
    if decision.engine in {"native", "hybrid"}:
        if decision.program is None:
            raise RuntimeError("native decision is missing a compiled program")
        from .native import execute as execute_native

        try:
            native_values = execute_native(decision.program)
        except TypeError as error:
            if plan.engine != "auto":
                expected = "real numbers" if decision.program.kind == "f64" else "i64 integers"
                raise NativeUnsupportedError(
                    f"native list and tuple sources must contain {expected}"
                ) from error
        except OverflowError:
            if plan.engine != "auto":
                raise
        else:
            if decision.engine == "hybrid":
                suffix = Plan(
                    Source.from_iterable(native_values),
                    plan.operations[decision.native_operation_count :],
                    "python",
                )
                yield from execute_python(suffix)
            else:
                yield from native_values
            return
    yield from execute_python(plan)


def try_native_terminal(plan: Plan, terminal: TerminalName) -> tuple[bool, int | float | None]:
    decision = select_terminal_engine(plan, terminal)
    if decision.engine != "native":
        return False, None
    if decision.program is None:
        raise RuntimeError("native decision is missing a compiled program")
    from .native import execute_terminal

    try:
        return True, execute_terminal(decision.program, terminal)
    except TypeError as error:
        if plan.engine != "auto":
            expected = "real numbers" if decision.program.kind == "f64" else "i64 integers"
            raise NativeUnsupportedError(
                f"native list and tuple sources must contain {expected}"
            ) from error
        return False, None
    except OverflowError:
        if plan.engine != "auto":
            raise
        return False, None


def try_native_statistics(
    plan: Plan,
) -> tuple[bool, StatisticsSnapshot | None]:
    decision = select_terminal_engine(plan, "statistics")
    if decision.engine != "native":
        return False, None
    if decision.program is None:
        raise RuntimeError("native decision is missing a compiled program")
    from .native import execute_statistics

    try:
        return True, execute_statistics(decision.program)
    except TypeError as error:
        if plan.engine != "auto":
            expected = "real numbers" if decision.program.kind == "f64" else "i64 integers"
            raise NativeUnsupportedError(
                f"native list and tuple sources must contain {expected}"
            ) from error
        return False, None
    except OverflowError:
        if plan.engine != "auto":
            raise
        return False, None


def try_native_aggregate(
    plan: Plan,
) -> tuple[bool, NativeAggregateSnapshot | None]:
    decision = select_terminal_engine(plan, "aggregate")
    if decision.engine != "native":
        return False, None
    if decision.program is None:
        raise RuntimeError("native decision is missing a compiled program")
    from .native import execute_aggregate

    try:
        return True, execute_aggregate(decision.program)
    except TypeError as error:
        if plan.engine != "auto":
            expected = "real numbers" if decision.program.kind == "f64" else "i64 integers"
            raise NativeUnsupportedError(
                f"native list and tuple sources must contain {expected}"
            ) from error
        return False, None
    except OverflowError:
        if plan.engine != "auto":
            raise
        return False, None
