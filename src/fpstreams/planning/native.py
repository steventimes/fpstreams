"""Compile numeric plan stages and choose Python, Rust, or hybrid execution."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, cast

from ..errors import NativeUnsupportedError
from ..expressions.scalar import Expr, FExpr
from .logical import Pipeline
from .sync import (
    DropOp,
    DropWhileOp,
    FilterOp,
    MapOp,
    Operation,
    SortOp,
    TakeOp,
    TakeWhileInclusiveOp,
    TakeWhileOp,
    UniqueOp,
    ZipOp,
)

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
    "minmax",
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
_PROBE_CAPABILITY_CACHE: dict[NativeKind, tuple[object, bool]] = {}
_exact_container_capability_cache: tuple[object, bool] | None = None


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
        "minmax",
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
    plan: Pipeline,
    terminal: TerminalName,
) -> EngineDecision:
    """Attach source scanning, copying, materialization, and complexity metadata to a decision."""
    source = plan.source.native_data
    crosses_native_boundary = decision.engine in {"native", "hybrid"}
    container_source = isinstance(source, (list, tuple))
    probe_path = (
        decision.engine == "native"
        and decision.program is not None
        and type(source) in (list, tuple)
        and terminal in {"first", "any", "all"}
        and _container_probe_available(decision.program.kind)
    )
    reason = (
        f"{decision.reason}; bounded probe; only undecided fallback bulk-copies"
        if probe_path
        else decision.reason
    )
    return EngineDecision(
        decision.engine,
        reason,
        decision.program,
        decision.native_operation_count,
        # These are worst-case flags: an undecided bounded probe restarts the
        # legacy bulk adapter, which scans and copies the whole container.
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


def exact_count(plan: Pipeline) -> int | None:
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


def _expression(operation: object) -> Expr | FExpr | None:
    """Extract a native scalar expression from supported map and predicate nodes."""
    if isinstance(operation, MapOp):
        return operation.function if isinstance(operation.function, (Expr, FExpr)) else None
    if isinstance(operation, (FilterOp, TakeWhileOp, TakeWhileInclusiveOp, DropWhileOp)):
        return operation.predicate if isinstance(operation.predicate, (Expr, FExpr)) else None
    return None


def _f64_range_starts_with_map(plan: Pipeline) -> bool:
    """Keep integer range values in Python until an fitem map changes representation.

    Native f64 range kernels expose every input as a float.  A predicate does
    not transform surviving Python values, so predicate-first range plans would
    otherwise return floats where canonical execution returns integers.  This
    is a structural O(operations) check and never opens or scans source data.
    """
    for operation in plan.operations:
        if isinstance(_expression(operation), FExpr):
            return isinstance(operation, MapOp)
    return True


def _native_kind(plan: Pipeline) -> tuple[NativeKind | None, str | None]:
    """Choose one numeric representation or reject a mixed expression pipeline."""
    expression_kinds = {
        "i64" if isinstance(expression, Expr) else "f64"
        for operation in plan.operations
        if (expression := _expression(operation)) is not None
    }
    if len(expression_kinds) > 1:
        return None, "a native pipeline cannot mix item and fitem expressions"
    return ("f64" if expression_kinds == {"f64"} else "i64"), None


def _compile_stage(operation: Operation, kind: NativeKind) -> tuple[Any | None, str | None]:
    """Translate one supported logical operation into a typed native opcode stage."""
    if isinstance(operation, MapOp) and isinstance(operation.function, (Expr, FExpr)):
        if operation.function.kind not in ("int", "float"):
            return None, "native map expressions must produce numeric values"
        return (0, operation.function.native_instructions()), None
    if isinstance(operation, FilterOp) and isinstance(operation.predicate, (Expr, FExpr)):
        return (
            2 if operation.negate else 1,
            operation.predicate.native_instructions(),
        ), None
    if isinstance(operation, (TakeOp, DropOp)):
        count = float(operation.count) if kind == "f64" else operation.count
        return (3 if isinstance(operation, TakeOp) else 4, ((1, count),)), None
    if isinstance(operation, UniqueOp) and operation.key is None:
        if kind == "f64":
            return None, "f64 distinct is not native-compilable"
        return (5, ()), None
    if isinstance(operation, TakeWhileOp) and isinstance(operation.predicate, (Expr, FExpr)):
        return (6, operation.predicate.native_instructions()), None
    if isinstance(operation, TakeWhileInclusiveOp) and isinstance(
        operation.predicate, (Expr, FExpr)
    ):
        return (8, operation.predicate.native_instructions()), None
    if isinstance(operation, DropWhileOp) and isinstance(operation.predicate, (Expr, FExpr)):
        return (7, operation.predicate.native_instructions()), None
    return None, f"operation {operation.name!r} is not native-compilable"


def _compile(plan: Pipeline) -> tuple[NativeProgram | None, str]:
    """Compile every operation into one typed native instruction stream.

    The compiler rejects mixed expression kinds, opaque callables, unsupported operations,
    nonnumeric map results, f64 distinct stages, and sources that cannot cross the Rust boundary.
    """
    # One fused instruction stream cannot switch between i64 item and f64 fitem expressions.
    kind, reason = _native_kind(plan)
    if kind is None:
        assert reason is not None
        return None, reason

    stages: list[Any] = []
    for operation in plan.operations:
        stage, reason = _compile_stage(operation, kind)
        if stage is None:
            assert reason is not None
            return None, reason
        stages.append(stage)

    if not stages:
        return None, "pipeline has no native-compilable operations"
    source = plan.source.native_data
    if not _valid_source(source):
        return None, f"source is not a {kind} range, list, or tuple"
    if kind == "f64" and isinstance(source, range) and not _f64_range_starts_with_map(plan):
        return (
            None,
            "fitem predicate-only range is not a float source; it requires a preceding fitem map",
        )
    if isinstance(source, (list, tuple)) and not _exact_container_extraction_available():
        return None, "native extension lacks exact numeric container extraction"
    return (
        NativeProgram(source, tuple(stages), kind),
        f"all operations compile to the fused {kind} kernel",
    )


@dataclass(frozen=True, slots=True)
class _NativePrefixState:
    """Track representation constraints while extending one legal native prefix."""

    expression_kind: NativeKind | None = None
    has_f64_map: bool = False
    has_unique: bool = False


_NATIVE_PREFIX_OPERATIONS = (
    MapOp,
    FilterOp,
    TakeOp,
    DropOp,
    UniqueOp,
    TakeWhileOp,
    TakeWhileInclusiveOp,
    DropWhileOp,
)
_NATIVE_PREDICATE_OPERATIONS = (
    FilterOp,
    TakeWhileOp,
    TakeWhileInclusiveOp,
    DropWhileOp,
)


def _extend_native_prefix(
    state: _NativePrefixState,
    operation: Operation,
    source: object,
) -> _NativePrefixState | None:
    """Return updated prefix constraints, or None at the first illegal operation."""
    expression = _expression(operation)
    if isinstance(operation, MapOp) and expression is None:
        return None
    if isinstance(operation, _NATIVE_PREDICATE_OPERATIONS) and expression is None:
        return None
    if not isinstance(operation, _NATIVE_PREFIX_OPERATIONS):
        return None

    operation_kind: NativeKind | None = None
    if isinstance(expression, Expr):
        operation_kind = "i64"
    elif isinstance(expression, FExpr):
        operation_kind = "f64"

    expression_kind = state.expression_kind
    has_f64_map = state.has_f64_map
    has_unique = state.has_unique
    if operation_kind is not None:
        if expression_kind is not None and expression_kind != operation_kind:
            return None
        if operation_kind == "f64":
            if has_unique:
                return None
            if isinstance(source, range) and not isinstance(operation, MapOp) and not has_f64_map:
                return None
            has_f64_map = has_f64_map or isinstance(operation, MapOp)
        expression_kind = operation_kind

    if isinstance(operation, UniqueOp):
        if operation.key is not None or expression_kind == "f64":
            return None
        has_unique = True
    return _NativePrefixState(expression_kind, has_f64_map, has_unique)


def _longest_native_prefix(plan: Pipeline) -> tuple[NativeProgram | None, int]:
    """Compile the longest leading stage sequence that obeys one native numeric representation."""
    source = plan.source.native_data
    if not _valid_source(source):
        return None, 0

    state = _NativePrefixState()
    prefix_length = 0
    for position, operation in enumerate(plan.operations, 1):
        next_state = _extend_native_prefix(state, operation, source)
        if next_state is None:
            break
        state = next_state
        prefix_length = position

    if prefix_length == 0:
        return None, 0
    prefix = Pipeline(
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


def _exact_container_extraction_available() -> bool:
    """Require an explicit current-wheel promise before passing containers to Rust.

    Older extension builds use PyO3 ``Vec`` arguments and may call ``__index__``
    or ``__float__`` while parsing them.  The positive marker prevents a new
    planner paired with such a wheel from invoking user conversion protocols.
    """
    global _exact_container_capability_cache
    try:
        from .. import _native
    except ImportError:
        return False
    cached = _exact_container_capability_cache
    if cached is not None and cached[0] is _native:
        return cached[1]
    marker = getattr(_native, "exact_container_extraction_v1", None)
    available = callable(marker) and marker() is True
    _exact_container_capability_cache = (_native, available)
    return available


def _container_probe_available(kind: NativeKind) -> bool:
    """Return whether this extension can inspect a bounded container prefix.

    Bulk adapters snapshot an entire exact Python container into a Rust vector
    before computing. Keeping this capability separate lets short-circuit
    terminals avoid that copy and preserves compatibility with wheels released
    before the bounded probe API existed.
    """
    try:
        from .. import _native
    except ImportError:
        return False
    cached = _PROBE_CAPABILITY_CACHE.get(kind)
    if cached is not None and cached[0] is _native:
        return cached[1]
    available = hasattr(_native, f"terminal_{kind}_probe")
    _PROBE_CAPABILITY_CACHE[kind] = (_native, available)
    return available


def _copy_would_dominate_short_circuit(plan: Pipeline) -> bool:
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


def select_engine(plan: Pipeline) -> EngineDecision:
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


def _identity_program(plan: Pipeline) -> tuple[NativeProgram | None, str]:
    """Build an operation-free native program without scanning a numeric container."""
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
    if not _exact_container_extraction_available():
        return None, "native extension lacks exact numeric container extraction"
    # A short-circuit terminal only needs the first element to choose a numeric
    # kernel. The bounded native probe validates further elements only if it has
    # to reach them, so a bad tail cannot reject an already-determined result.
    if not source:
        return (
            NativeProgram(source, (), "i64"),
            "empty sequence can use native terminals",
        )
    # Rust validates the remaining elements as it copies for full-scan
    # terminals, while bounded probes validate only the reachable prefix.  The
    # first exact item is therefore sufficient to choose a representation.
    first = source[0]
    if type(first) is int and _I64_MIN <= first <= _I64_MAX:
        return (
            NativeProgram(source, (), "i64"),
            "identity terminal selects the first i64 value without a source scan",
        )
    if type(first) is float:
        return (
            NativeProgram(source, (), "f64"),
            "identity terminal selects the first float value without a source scan",
        )
    return None, "identity native terminals require i64 integers or floats"


def select_terminal_engine(plan: Pipeline, terminal: TerminalName) -> EngineDecision:
    """Select an engine for a terminal, including operation-free native identity kernels.

    Automatic list/tuple terminals stay in Python to avoid a type scan and Rust copy; ranges can
    use native identity kernels after capability and crossover checks.
    """
    terminal = validate_terminal(terminal)
    if plan.operations:
        source = plan.source.native_data
        if plan.engine == "auto" and terminal == "minmax" and type(source) in (list, tuple):
            return _terminal_metadata(
                EngineDecision(
                    "python",
                    "automatic minmax preserves exact container representative identity",
                ),
                plan,
                terminal,
            )
        decision = select_engine(plan)
        if (
            plan.engine == "auto"
            and terminal in {"first", "any", "all"}
            and type(source) in (list, tuple)
            and decision.engine == "native"
            and decision.program is not None
            and not _container_probe_available(decision.program.kind)
        ):
            return _terminal_metadata(
                EngineDecision(
                    "python",
                    "native extension lacks bounded container short-circuit probes",
                ),
                plan,
                terminal,
            )
        return _terminal_metadata(decision, plan, terminal)
    if plan.engine == "python":
        return _terminal_metadata(
            EngineDecision("python", "python engine explicitly requested"),
            plan,
            terminal,
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
            EngineDecision("python", "native extension is not installed"),
            plan,
            terminal,
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


def select_materializing_engine(plan: Pipeline) -> EngineDecision:
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
