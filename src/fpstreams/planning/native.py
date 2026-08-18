"""Compile numeric plan stages and choose Python, Rust, or hybrid execution."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, cast

from ..errors import NativeUnsupportedError
from ..expressions.scalar import Expr, FExpr
from .sync import (
    DropOp,
    DropWhileOp,
    FilterOp,
    MapOp,
    Plan,
    SortOp,
    TakeOp,
    TakeWhileInclusiveOp,
    TakeWhileOp,
    UniqueOp,
    ZipOp,
)

NativeInstruction = tuple[int, int]
NativeStage = tuple[int, tuple[NativeInstruction, ...]]
FloatNativeInstruction = tuple[int, float]
FloatNativeStage = tuple[int, tuple[FloatNativeInstruction, ...]]
NativeKind = Literal["i64", "f64"]
TerminalName = Literal[
    "iterate",
    "list",
    "count",
    "sum",
    "statistics",
    "aggregate",
    "min",
    "max",
    "first",
    "last",
    "any",
    "all",
]

_I64_MIN = -(2**63)
_I64_MAX = 2**63 - 1
_AUTO_THRESHOLD = 8
_COPY_SHORT_CIRCUIT_LIMIT = 1_024
_COPY_SHORT_CIRCUIT_RATIO = 64
_EXTENSION_CAPABILITY_CACHE: dict[NativeKind, tuple[object, bool]] = {}


@dataclass(frozen=True, slots=True)
class NativeProgram:
    """Hold retained numeric source data and fused opcode stages for one Rust kernel kind."""

    source: range | list[Any] | tuple[Any, ...]
    stages: tuple[Any, ...]
    kind: NativeKind = "i64"


@dataclass(frozen=True, slots=True)
class EngineDecision:
    """Report the selected engine, rationale, native prefix, and predicted data movement."""

    engine: Literal["python", "native", "hybrid"]
    reason: str
    program: NativeProgram | None = None
    native_operation_count: int = 0
    scans_source: bool = False
    copies_source: bool = False
    materializes: bool = False
    complexity: Literal["O(1)", "O(n)", "short-circuiting"] = "O(n)"


_TERMINALS = frozenset(
    {
        "iterate",
        "list",
        "count",
        "sum",
        "statistics",
        "aggregate",
        "min",
        "max",
        "first",
        "last",
        "any",
        "all",
    }
)


def validate_terminal(terminal: str) -> TerminalName:
    """Validate and narrow a terminal name before engine selection."""
    if terminal not in _TERMINALS:
        raise ValueError(f"unknown terminal {terminal!r}")
    return cast(TerminalName, terminal)


def _terminal_metadata(
    decision: EngineDecision,
    plan: Plan,
    terminal: TerminalName,
) -> EngineDecision:
    """Attach source scanning, copying, materialization, and complexity metadata to a decision."""
    source = plan.source.native_data
    crosses_native_boundary = decision.engine in {"native", "hybrid"}
    container_source = isinstance(source, (list, tuple))
    return EngineDecision(
        decision.engine,
        decision.reason,
        decision.program,
        decision.native_operation_count,
        scans_source=crosses_native_boundary and container_source,
        copies_source=crosses_native_boundary and container_source,
        materializes=terminal == "list" or decision.engine == "hybrid",
        complexity=(
            "O(1)"
            if terminal == "count" and exact_count(plan) is not None
            else "short-circuiting"
            if terminal in {"first", "any", "all"}
            else "O(n)"
        ),
    )


def exact_count(plan: Plan) -> int | None:
    """Return the unopened exact size only for an operation-free, reiterable source."""
    capabilities = plan.source.capabilities
    if plan.operations or not capabilities.reiterable:
        return None
    return capabilities.exact_size


def _valid_source(source: object) -> bool:
    """Accept list/tuple storage or a range whose endpoints and step fit signed i64."""
    if isinstance(source, range):
        return all(
            _I64_MIN <= value <= _I64_MAX for value in (source.start, source.stop, source.step)
        )
    return isinstance(source, (list, tuple))


def _f64_preserves_filter_type(plan: Plan, source: object) -> bool:
    """Require predicate-only f64 plans to start from homogeneous Python floats."""
    for operation in plan.operations:
        if isinstance(operation, MapOp) and isinstance(operation.function, FExpr):
            return True
        if isinstance(operation, (FilterOp, TakeWhileOp, DropWhileOp)) and isinstance(
            operation.predicate, FExpr
        ):
            return isinstance(source, (list, tuple)) and all(
                type(value) is float for value in source
            )
    return True


def _expression(operation: object) -> Expr | FExpr | None:
    """Extract a native scalar expression from supported map and predicate nodes."""
    if isinstance(operation, MapOp):
        return operation.function if isinstance(operation.function, (Expr, FExpr)) else None
    if isinstance(operation, (FilterOp, TakeWhileOp, TakeWhileInclusiveOp, DropWhileOp)):
        return operation.predicate if isinstance(operation.predicate, (Expr, FExpr)) else None
    return None


def _compile(plan: Plan) -> tuple[NativeProgram | None, str]:
    # One fused instruction stream cannot switch between i64 item and f64 fitem expressions.
    """Compile every operation into one typed native instruction stream.

    The compiler rejects mixed expression kinds, opaque callables, unsupported operations,
    nonnumeric map results, f64 distinct stages, and sources that cannot cross the Rust boundary.
    """
    expression_kinds = {
        "i64" if isinstance(expression, Expr) else "f64"
        for operation in plan.operations
        if (expression := _expression(operation)) is not None
    }
    if len(expression_kinds) > 1:
        return None, "a native pipeline cannot mix item and fitem expressions"
    kind: NativeKind = "f64" if expression_kinds == {"f64"} else "i64"
    stages: list[NativeStage | FloatNativeStage] = []
    for operation in plan.operations:
        if isinstance(operation, MapOp) and isinstance(operation.function, (Expr, FExpr)):
            if operation.function.kind not in ("int", "float"):
                return None, "native map expressions must produce numeric values"
            stages.append((0, operation.function.native_instructions()))
        elif isinstance(operation, FilterOp) and isinstance(operation.predicate, (Expr, FExpr)):
            stages.append(
                (
                    2 if operation.negate else 1,
                    operation.predicate.native_instructions(),
                )
            )
        elif isinstance(operation, TakeOp):
            count = float(operation.count) if kind == "f64" else operation.count
            stages.append((3, ((1, count),)))
        elif isinstance(operation, DropOp):
            count = float(operation.count) if kind == "f64" else operation.count
            stages.append((4, ((1, count),)))
        elif isinstance(operation, UniqueOp) and operation.key is None:
            if kind == "f64":
                return None, "f64 distinct is not native-compilable"
            stages.append((5, ()))
        elif isinstance(operation, TakeWhileOp) and isinstance(operation.predicate, (Expr, FExpr)):
            stages.append((6, operation.predicate.native_instructions()))
        elif isinstance(operation, TakeWhileInclusiveOp) and isinstance(
            operation.predicate, (Expr, FExpr)
        ):
            stages.append((8, operation.predicate.native_instructions()))
        elif isinstance(operation, DropWhileOp) and isinstance(operation.predicate, (Expr, FExpr)):
            stages.append((7, operation.predicate.native_instructions()))
        else:
            return None, f"operation {operation.name!r} is not native-compilable"

    if not stages:
        return None, "pipeline has no native-compilable operations"
    source = plan.source.native_data
    if not _valid_source(source):
        return None, f"source is not a {kind} range, list, or tuple"
    if kind == "f64" and not _f64_preserves_filter_type(plan, source):
        return (
            None,
            "fitem predicate-only pipelines require a float source or a preceding fitem map",
        )
    return (
        NativeProgram(source, tuple(stages), kind),
        f"all operations compile to the fused {kind} kernel",
    )


def _longest_native_prefix(plan: Plan) -> tuple[NativeProgram | None, int]:
    """Compile the longest leading stage sequence that obeys one native numeric representation."""
    source = plan.source.native_data
    if not _valid_source(source):
        return None, 0

    expression_kind: NativeKind | None = None
    has_f64_map = False
    has_unique = False
    prefix_length = 0
    for position, operation in enumerate(plan.operations, 1):
        expression = _expression(operation)
        if isinstance(operation, MapOp) and expression is None:
            break
        if isinstance(operation, (FilterOp, TakeWhileOp, TakeWhileInclusiveOp, DropWhileOp)) and (
            expression is None
        ):
            break
        if not isinstance(
            operation,
            (
                MapOp,
                FilterOp,
                TakeOp,
                DropOp,
                UniqueOp,
                TakeWhileOp,
                TakeWhileInclusiveOp,
                DropWhileOp,
            ),
        ):
            break

        operation_kind: NativeKind | None = None
        if isinstance(expression, Expr):
            operation_kind = "i64"
        elif isinstance(expression, FExpr):
            operation_kind = "f64"

        if operation_kind is not None:
            if expression_kind is not None and expression_kind != operation_kind:
                break
            if operation_kind == "f64":
                if has_unique:
                    break
                if (
                    not isinstance(operation, MapOp)
                    and not has_f64_map
                    and not (
                        isinstance(source, (list, tuple))
                        and all(type(value) is float for value in source)
                    )
                ):
                    break
                if isinstance(operation, MapOp):
                    has_f64_map = True
            expression_kind = operation_kind

        if isinstance(operation, UniqueOp):
            if operation.key is not None or expression_kind == "f64":
                break
            has_unique = True
        prefix_length = position

    if prefix_length == 0:
        return None, 0
    prefix = Plan(
        plan.source,
        plan.operations[:prefix_length],
        plan.engine,
        plan.parallel,
    )
    program, _reason = _compile(prefix)
    return (program, prefix_length) if program is not None else (None, 0)


def _extension_available(kind: NativeKind) -> bool:
    """Check and cache whether the native module exposes every kernel for
    ``kind``."""
    try:
        from .. import _native
    except ImportError:
        return False
    cached = _EXTENSION_CAPABILITY_CACHE.get(kind)
    if cached is not None and cached[0] is _native:
        return cached[1]
    if kind == "i64":
        available = all(
            hasattr(_native, name)
            for name in (
                "execute_i64",
                "execute_i64_range",
                "terminal_i64",
                "terminal_i64_range",
                "statistics_i64",
                "statistics_i64_range",
                "aggregate_i64",
                "aggregate_i64_range",
            )
        )
    else:
        available = all(
            hasattr(_native, name)
            for name in (
                "execute_f64",
                "execute_f64_range",
                "terminal_f64",
                "terminal_f64_range",
                "count_f64",
                "count_f64_range",
                "statistics_f64",
                "statistics_f64_range",
                "aggregate_f64",
                "aggregate_f64_range",
            )
        )
    _EXTENSION_CAPABILITY_CACHE[kind] = (_native, available)
    return available


def _copy_would_dominate_short_circuit(plan: Plan) -> bool:
    """Detect a tiny ``take`` bound whose output does not justify copying a large container."""
    source = plan.source.native_data
    size = plan.source.capabilities.exact_size
    if not isinstance(source, (list, tuple)) or size is None:
        return False
    limits = [operation.count for operation in plan.operations if isinstance(operation, TakeOp)]
    if not limits:
        return False
    smallest_limit = min(limits)
    return (
        smallest_limit <= _COPY_SHORT_CIRCUIT_LIMIT
        and smallest_limit * _COPY_SHORT_CIRCUIT_RATIO < size
    )


def select_engine(plan: Plan) -> EngineDecision:
    """Select Python or native execution for a fully compilable operation pipeline.

    Forced native mode raises on compilation or extension failure. Automatic mode also applies
    source-size and short-circuit copy-cost guards before crossing into Rust.
    """
    if plan.engine == "python":
        return EngineDecision("python", "python engine explicitly requested")
    # Rust adapters copy list/tuple sources in full; a tiny take result cannot amortize that cost.
    if plan.engine == "auto" and _copy_would_dominate_short_circuit(plan):
        return EngineDecision(
            "python",
            "avoided copying the entire list or tuple for a tiny short-circuit output",
        )

    program, reason = _compile(plan)
    if program is None:
        if plan.engine == "native":
            raise NativeUnsupportedError(reason)
        return EngineDecision("python", reason)
    if not _extension_available(program.kind):
        if plan.engine == "native":
            raise NativeUnsupportedError("native extension is not installed")
        return EngineDecision("python", "native extension is not installed")
    if plan.engine == "auto":
        size = plan.source.capabilities.exact_size
        if size is None or size < _AUTO_THRESHOLD:
            return EngineDecision(
                "python",
                f"source size is below native crossover threshold {_AUTO_THRESHOLD}",
            )
    return EngineDecision("native", reason, program, len(plan.operations))


def _identity_program(plan: Plan) -> tuple[NativeProgram | None, str]:
    """Build an operation-free native program for an i64 range or homogeneous numeric container."""
    source = plan.source.native_data
    if isinstance(source, range):
        if not _valid_source(source):
            return None, "range endpoints must fit in signed i64"
        return (
            NativeProgram(source, (), "i64"),
            "numeric range can use the native identity kernel",
        )
    if not isinstance(source, (list, tuple)):
        return None, "identity native terminals require a range, list, or tuple"
    if not source or all(type(value) is int and _I64_MIN <= value <= _I64_MAX for value in source):
        return (
            NativeProgram(source, (), "i64"),
            "homogeneous i64 sequence can use native terminals",
        )
    if all(type(value) is float for value in source):
        return (
            NativeProgram(source, (), "f64"),
            "homogeneous float sequence can use native terminals",
        )
    return None, "identity native terminals require homogeneous i64 integers or floats"


def select_terminal_engine(plan: Plan, terminal: TerminalName) -> EngineDecision:
    """Select an engine for a terminal, including operation-free native identity kernels.

    Automatic list/tuple terminals stay in Python to avoid a type scan and Rust copy; ranges can
    use native identity kernels after capability and crossover checks.
    """
    terminal = validate_terminal(terminal)
    if plan.operations:
        return _terminal_metadata(select_engine(plan), plan, terminal)
    if plan.engine == "python":
        return _terminal_metadata(
            EngineDecision("python", "python engine explicitly requested"), plan, terminal
        )
    source = plan.source.native_data
    if plan.engine == "auto" and isinstance(source, (list, tuple)):
        return _terminal_metadata(
            EngineDecision(
                "python",
                "identity list/tuple stays in Python to avoid a type scan and Rust copy",
            ),
            plan,
            terminal,
        )
    program, reason = _identity_program(plan)
    if program is None:
        if plan.engine == "native":
            raise NativeUnsupportedError(reason)
        return _terminal_metadata(EngineDecision("python", reason), plan, terminal)
    if not _extension_available(program.kind):
        if plan.engine == "native":
            raise NativeUnsupportedError("native extension is not installed")
        return _terminal_metadata(
            EngineDecision("python", "native extension is not installed"), plan, terminal
        )
    if plan.engine == "auto":
        size = plan.source.capabilities.exact_size
        if size is None or size < _AUTO_THRESHOLD:
            return _terminal_metadata(
                EngineDecision(
                    "python",
                    f"source size is below native crossover threshold {_AUTO_THRESHOLD}",
                ),
                plan,
                terminal,
            )
    return _terminal_metadata(EngineDecision("native", reason, program), plan, terminal)


def select_materializing_engine(plan: Plan) -> EngineDecision:
    """Select full native or safe hybrid execution for a terminal that consumes all output.

    Hybrid mode is considered only after automatic full-plan compilation fails. It requires an
    available extension, a source above the crossover threshold, and a Python suffix that does
    not depend on streaming a short-circuit or bounded external-sort stage.
    """
    decision = select_engine(plan)
    if decision.engine != "python" or plan.engine != "auto":
        return decision

    full_program, _reason = _compile(plan)
    if full_program is not None or _copy_would_dominate_short_circuit(plan):
        return decision

    program, prefix_length = _longest_native_prefix(plan)
    if program is None:
        return decision
    if not _extension_available(program.kind):
        return decision

    size = plan.source.capabilities.exact_size
    if size is None or size < _AUTO_THRESHOLD:
        return decision

    suffix = plan.operations[prefix_length:]
    # Executing the prefix eagerly would defeat short-circuit and bounded-spill suffix stages.
    unsafe_suffix = next(
        (
            operation
            for operation in suffix
            if isinstance(operation, (TakeOp, TakeWhileOp, TakeWhileInclusiveOp, ZipOp))
            or (isinstance(operation, SortOp) and operation.buffer_size is not None)
        ),
        None,
    )
    if unsafe_suffix is not None:
        return EngineDecision(
            "python",
            f"kept the pipeline streaming because {unsafe_suffix.name!r} follows "
            "the native-compatible prefix",
        )

    return EngineDecision(
        "hybrid",
        f"first {prefix_length} operations compile to the fused {program.kind} kernel; "
        f"remaining {len(suffix)} run in Python",
        program,
        prefix_length,
    )
