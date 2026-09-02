"""Route materializing and terminal plans to Arrow, Rust, or Python execution."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from ..collecting.statistics import StatisticsSnapshot
from ..errors import NativeUnsupportedError
from ..expressions.scalar import Expr
from ..planning.arrow import ArrowPrefixPlan, plan_arrow_prefix
from ..planning.logical import Pipeline
from ..planning.native import (
    EngineDecision,
    TerminalName,
    select_materializing_engine,
    select_terminal_engine,
)
from ..planning.native import exact_count as exact_count
from ..planning.source import _CANONICAL_RETAINED_SEQUENCE, Source
from ..planning.sync import MapOp
from ..runtime.failpoints import has_active_failpoints
from ..runtime.iterators import closing_iterators
from ..runtime.query import QueryRuntime
from .sync import execute_operations

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


def _direct_i64_map_list_source(plan: Pipeline, decision: EngineDecision) -> object | None:
    """Return the retained exact sequence for one uninstrumented scalar map."""
    program = decision.program
    if (
        plan.engine != "auto"
        or plan.parallel is not None
        or len(plan.operations) != 1
        or type(operation := plan.operations[0]) is not MapOp
        or type(operation.function) is not Expr
        or program is None
        or decision.native_operation_count != 1
        or has_active_failpoints()
    ):
        return None
    retained = _CANONICAL_RETAINED_SEQUENCE(plan.source)
    return retained if type(retained) in (list, tuple) and retained is program.source else None


def _native_source_type_error(kind: str) -> NativeUnsupportedError:
    """Describe a numeric-source rejection without exposing Rust ABI details."""
    expected = "homogeneous real numbers" if kind == "f64" else "homogeneous i64 integers"
    return NativeUnsupportedError(f"native numeric sources must contain {expected}")


def _materialize_python_after_native_failure(plan: Pipeline, target: str) -> Any:
    """Run the canonical Python pipeline once after a new native endpoint failed.

    Falling through to ``execute_physical`` would select another native adapter
    before reaching Python. Current exact extraction rejects user objects without
    invoking their hooks; entering canonical execution directly guarantees each
    stateful Python operation is evaluated exactly once.
    """
    values = execute_operations(plan.source.open(), plan.operations)
    with closing_iterators((values,)):
        if target == "list":
            return list(values)
        if target == "tuple":
            return tuple(values)
        if target == "set":
            return set(values)
        raise RuntimeError(f"unknown materialization target {target!r}")


def try_native_materialize(
    plan: Pipeline,
    target: str,
    decision: EngineDecision,
) -> tuple[bool, Any | None]:
    """Materialize a fully selected native program into its final collection.

    The caller supplies the compiler's exact decision so this terminal never
    replans. Missing direct-materialize symbols are an unhandled result, keeping
    older native extensions on the established execution path.
    """
    if decision.engine != "native" or decision.program is None:
        return False, None
    from .native import (
        direct_i64_map_list_endpoint,
        direct_i64_map_list_program,
        execute_materialize,
        materialize_available,
    )

    if plan.engine == "auto" and target == "list":
        phase_a_owned, expected_instructions = direct_i64_map_list_program(decision.program)
        if phase_a_owned:
            endpoint = direct_i64_map_list_endpoint()
            if endpoint is not None:
                source = _direct_i64_map_list_source(plan, decision)
                if source is None or expected_instructions is None:
                    return True, _materialize_python_after_native_failure(plan, target)
                direct = endpoint(source, expected_instructions)
                if direct is None:
                    return True, _materialize_python_after_native_failure(plan, target)
                return True, direct

    if not materialize_available(decision.program):
        return False, None
    try:
        return True, execute_materialize(decision.program, target)
    except TypeError as error:
        if plan.engine != "auto":
            raise _native_source_type_error(decision.program.kind) from error
        return True, _materialize_python_after_native_failure(plan, target)
    except OverflowError:
        if plan.engine != "auto":
            raise
        return True, _materialize_python_after_native_failure(plan, target)


def execute(
    plan: Pipeline,
    *,
    auto_native: bool = True,
    decision: EngineDecision | None = None,
    arrow_prefix: ArrowPrefixPlan | None = None,
    runtime: QueryRuntime | None = None,
) -> Iterator[Any]:
    """Materialize a plan with its selected Python, Arrow, native, or hybrid engine.

    For an automatic plan, a nonempty Arrow-compatible prefix takes precedence;
    otherwise engine selection may run the whole plan in Rust or feed a
    materialized Rust prefix into a Python suffix. Native type and integer-range
    failures restart the full plan in Python only in automatic mode. Setting
    auto_native to false bypasses both Arrow and Rust for automatic plans.
    """
    if not auto_native and plan.engine == "auto":
        yield from execute_operations(plan.source.open(), plan.operations, runtime=runtime)
        return
    if plan.engine == "auto":
        from .arrow import execute_with_arrow_prefix

        arrow_plan = plan_arrow_prefix(plan) if arrow_prefix is None else arrow_prefix
        if arrow_plan is not None and (arrow_plan.operation_count or arrow_plan.first_only):
            yield from execute_with_arrow_prefix(plan, prefix=arrow_plan, runtime=runtime)
            return
    selected = select_materializing_engine(plan) if decision is None else decision
    if selected.engine in {"native", "hybrid"}:
        if selected.program is None:
            raise RuntimeError("native decision is missing a compiled program")
        from .native import execute as execute_native

        try:
            native_values = execute_native(selected.program)
        except TypeError as error:
            if plan.engine != "auto":
                raise _native_source_type_error(selected.program.kind) from error
        except OverflowError:
            if plan.engine != "auto":
                raise
        else:
            if selected.engine == "hybrid":
                suffix = Pipeline(
                    Source.from_iterable(native_values),
                    plan.operations[selected.native_operation_count :],
                    "python",
                )
                yield from execute_operations(
                    suffix.source.open(),
                    suffix.operations,
                    runtime=runtime,
                )
            else:
                yield from native_values
            return
    yield from execute_operations(plan.source.open(), plan.operations, runtime=runtime)


def try_native_terminal(
    plan: Pipeline,
    terminal: TerminalName,
    *,
    decision: EngineDecision | None = None,
) -> tuple[bool, int | float | None]:
    """Run a selected scalar terminal in Rust, returning (handled, value).

    Automatic plans return (False, None) when selection rejects Rust or the source
    cannot be converted to the compiled numeric kind. A forced native plan instead
    exposes overflow and wraps incompatible source values as NativeUnsupportedError.
    """
    # Aggregate planning initially uses the umbrella ``aggregate`` terminal. Re-select
    # short-circuiting scalar projections so a retained buffer cannot turn ``first``
    # into a full scan that observes an unreachable error or overflow in the tail.
    selected = (
        select_terminal_engine(plan, terminal)
        if decision is None or terminal in {"first", "any", "all"}
        else decision
    )
    if selected.engine != "native":
        return False, None
    if selected.program is None:
        raise RuntimeError("native decision is missing a compiled program")
    from .native import execute_terminal

    try:
        return True, execute_terminal(selected.program, terminal)
    except TypeError as error:
        if plan.engine != "auto":
            raise _native_source_type_error(selected.program.kind) from error
        return False, None
    except OverflowError:
        if plan.engine != "auto":
            raise
        return False, None


def try_native_statistics(
    plan: Pipeline,
    *,
    decision: EngineDecision | None = None,
) -> tuple[bool, StatisticsSnapshot | None]:
    """Try Rust's one-pass (count, mean, squared_deviations) reduction.

    The handled flag distinguishes an unavailable or failed automatic native path
    from a valid snapshot; forced-native conversion and overflow errors propagate.
    """
    selected = select_terminal_engine(plan, "statistics") if decision is None else decision
    if selected.engine != "native":
        return False, None
    if selected.program is None:
        raise RuntimeError("native decision is missing a compiled program")
    from .native import execute_statistics

    try:
        return True, execute_statistics(selected.program)
    except TypeError as error:
        if plan.engine != "auto":
            raise _native_source_type_error(selected.program.kind) from error
        return False, None
    except OverflowError:
        if plan.engine != "auto":
            raise
        return False, None


def try_native_mean(
    plan: Pipeline,
    *,
    decision: EngineDecision | None = None,
) -> tuple[bool, float | None]:
    """Try Rust's mean-only reduction without allocating variance state."""
    selected = select_terminal_engine(plan, "mean") if decision is None else decision
    if selected.engine != "native":
        return False, None
    if selected.program is None:
        raise RuntimeError("native decision is missing a compiled program")
    from .native import execute_exact_container_mean, execute_mean

    try:
        exact_container = execute_exact_container_mean(selected.program)
    except TypeError as error:
        if plan.engine != "auto":
            raise _native_source_type_error(selected.program.kind) from error
        return False, None
    if exact_container is not None:
        handled, result = exact_container
        if handled:
            return True, result
        if plan.engine == "auto":
            return False, None

    try:
        return True, execute_mean(selected.program)
    except TypeError as error:
        if plan.engine != "auto":
            raise _native_source_type_error(selected.program.kind) from error
        return False, None
    except OverflowError:
        if plan.engine != "auto":
            raise
        return False, None


def try_native_aggregate(
    plan: Pipeline,
    *,
    decision: EngineDecision | None = None,
    mask: int | None = None,
) -> tuple[bool, NativeAggregateSnapshot | None]:
    """Try one Rust traversal for a built-in numeric aggregate snapshot.

    A mask lets current wheels skip unused fields; older wheels transparently
    compute the complete snapshot. Automatic conversion or overflow failures are
    reported as unhandled so callers can perform the aggregation in Python.
    """
    selected = select_terminal_engine(plan, "aggregate") if decision is None else decision
    if selected.engine != "native":
        return False, None
    if selected.program is None:
        raise RuntimeError("native decision is missing a compiled program")
    from .native import execute_aggregate

    try:
        return True, execute_aggregate(selected.program, mask)
    except TypeError as error:
        if plan.engine != "auto":
            raise _native_source_type_error(selected.program.kind) from error
        return False, None
    except OverflowError:
        if plan.engine != "auto":
            raise
        return False, None
