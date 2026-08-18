"""Route materializing and terminal plans to Arrow, Rust, or Python execution."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from ..collecting.statistics import StatisticsSnapshot
from ..errors import NativeUnsupportedError
from ..planning.arrow import plan_arrow_prefix
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
    """Materialize a plan with its selected Python, Arrow, native, or hybrid engine.

    For an automatic plan, a nonempty Arrow-compatible prefix takes precedence;
    otherwise engine selection may run the whole plan in Rust or feed a
    materialized Rust prefix into a Python suffix. Native type and integer-range
    failures restart the full plan in Python only in automatic mode. Setting
    auto_native to false bypasses both Arrow and Rust for automatic plans.
    """
    if not auto_native and plan.engine == "auto":
        yield from execute_python(plan)
        return
    if plan.engine == "auto":
        from .arrow import execute_with_arrow_prefix

        arrow_plan = plan_arrow_prefix(plan)
        if arrow_plan is not None and arrow_plan.operation_count:
            yield from execute_with_arrow_prefix(plan)
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
    """Run a selected scalar terminal in Rust, returning (handled, value).

    Automatic plans return (False, None) when selection rejects Rust or the source
    cannot be converted to the compiled numeric kind. A forced native plan instead
    exposes overflow and wraps incompatible source values as NativeUnsupportedError.
    """
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
    """Try Rust's one-pass (count, mean, squared_deviations) reduction.

    The handled flag distinguishes an unavailable or failed automatic native path
    from a valid snapshot; forced-native conversion and overflow errors propagate.
    """
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
    """Try one Rust traversal for the built-in numeric aggregate snapshot.

    The snapshot contains count, sum, minimum, maximum, first, last, mean, and
    squared deviations. Automatic conversion or overflow failures are reported as
    unhandled so callers can perform the aggregation in Python.
    """
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
