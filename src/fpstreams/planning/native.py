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
    "mean",
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
_AUTO_IDENTITY_I64_SUM_THRESHOLD = 65_536
_COPY_SHORT_CIRCUIT_LIMIT = 1_024
_COPY_SHORT_CIRCUIT_RATIO = 64
_ALLOCATING_I64_MAP_ROOTS = frozenset({"add", "sub", "mul", "floordiv", "neg"})
_IDENTITY_PROPAGATING_I64_MAP_ROOTS = frozenset({"item", "abs", "mod"})
_AUTO_I64_EXTERNAL_IDENTITY_REASON = (
    "automatic exact i64 container materialization preserves externally owned values"
)
_EXTENSION_CAPABILITY_CACHE: dict[NativeKind, tuple[object, bool]] = {}
_PROBE_CAPABILITY_CACHE: dict[NativeKind, tuple[object, bool]] = {}
_buffer_capability_cache: dict[NativeKind, tuple[object, bool]] = {}
_exact_container_capability_cache: tuple[object, bool] | None = None


@dataclass(frozen=True, slots=True)
class NativeProgram:
    """Hold retained numeric source data and fused opcode stages for one Rust kernel kind."""

    source: Any
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
        "mean",
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
    *,
    source_is_container: bool | None = None,
) -> EngineDecision:
    """Attach worst-case data movement and complexity metadata to a terminal decision."""
    source = plan.source.native_data
    crosses_native_boundary = decision.engine in {"native", "hybrid"}
    if source_is_container is None:
        source_is_container = (
            isinstance(source, (list, tuple)) or _numpy_buffer_kind(source) is not None
        )
    metadata_count = terminal == "count" and exact_count(plan) is not None
    direct_i64_container_sum = (
        decision.engine == "native"
        and decision.program is not None
        and decision.program.kind == "i64"
        and not decision.program.stages
        and type(source) in (list, tuple)
        and terminal == "sum"
    )
    direct_exact_container_mean = (
        decision.engine == "native"
        and decision.program is not None
        and not decision.program.stages
        and type(source) in (list, tuple)
        and terminal == "mean"
        and _exact_number_mean_available()
    )
    range_metadata = (
        decision.engine == "native"
        and type(source) is range
        and not plan.operations
        and terminal in {"sum", "min", "max", "last"}
    )
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
        else f"{decision.reason}; retained range terminal uses constant-time metadata"
        if range_metadata
        else decision.reason
    )
    return EngineDecision(
        decision.engine,
        reason,
        decision.program,
        decision.native_operation_count,
        # An undecided bounded probe restarts the legacy bulk adapter, and numeric
        # buffer kernels may snapshot exporters before detached computation.
        scans_source=crosses_native_boundary and source_is_container and not metadata_count,
        copies_source=(
            crosses_native_boundary
            and source_is_container
            and not metadata_count
            and not direct_i64_container_sum
            and not direct_exact_container_mean
        ),
        materializes=terminal == "list" or decision.engine == "hybrid",
        complexity=(
            "O(1)"
            if metadata_count or range_metadata
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
    return plan.source.current_exact_size()


def _valid_source(source: object) -> bool:
    """Accept list/tuple storage or a range whose endpoints and step fit signed i64."""
    if isinstance(source, range):
        return all(
            _I64_MIN <= value <= _I64_MAX for value in (source.start, source.stop, source.step)
        )
    return isinstance(source, (list, tuple)) or _numpy_buffer_kind(source) is not None


def _is_exact_i64(value: object) -> bool:
    """Return whether one built-in integer fits the native signed lane."""
    return type(value) is int and _I64_MIN <= value <= _I64_MAX


def _auto_direct_i64_sum_candidate(source: object, terminal: TerminalName) -> bool:
    """Return whether a retained sequence clears the allocation-free sum crossover."""
    if terminal != "sum" or type(source) not in (list, tuple):
        return False
    sequence = cast(list[object] | tuple[object, ...], source)
    return (
        len(sequence) >= _AUTO_IDENTITY_I64_SUM_THRESHOLD
        and _is_exact_i64(sequence[0])
        and _is_exact_i64(sequence[-1])
    )


def _numpy_i64_buffer(source: object) -> Any | None:
    """Return a validated explicit NumPy column buffer without importing NumPy eagerly."""
    from ..tabular.numpy import numpy_i64_buffer

    return numpy_i64_buffer(source)


def _numpy_f64_buffer(source: object) -> Any | None:
    """Return a validated explicit NumPy float column buffer without importing NumPy eagerly."""
    from ..tabular.numpy import numpy_f64_buffer

    return numpy_f64_buffer(source)


def _numpy_buffer_kind(source: object) -> NativeKind | None:
    """Return the exact native representation exposed by an explicit NumPy column."""
    if _numpy_i64_buffer(source) is not None:
        return "i64"
    if _numpy_f64_buffer(source) is not None:
        return "f64"
    return None


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


def _compile(
    plan: Pipeline,
    *,
    known_buffer_kind: NativeKind | None = None,
) -> tuple[NativeProgram | None, str]:
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
    if known_buffer_kind is None:
        if not _valid_source(source):
            return None, f"source is not a {kind} range, list, or tuple"
        buffer_kind = _numpy_buffer_kind(source)
    else:
        buffer_kind = known_buffer_kind
    if buffer_kind is not None and kind != buffer_kind:
        return None, f"a {buffer_kind} buffer cannot enter the native {kind} kernel"
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


def _buffer_extraction_available(kind: NativeKind) -> bool:
    """Require versioned endpoints before selecting an external numeric buffer."""
    try:
        from .. import _native
    except ImportError:
        return False
    cached = _buffer_capability_cache.get(kind)
    if cached is not None and cached[0] is _native:
        return cached[1]
    terminal_name = "aggregate_i64_buffer_masked_v1"
    if kind == "f64":
        terminal_name = "terminal_f64_buffer_v1"
    available = all(
        hasattr(_native, name)
        for name in (
            terminal_name,
            f"execute_{kind}_buffer_v1",
            f"materialize_{kind}_buffer_v1",
            f"mean_{kind}_buffer_v1",
        )
    )
    if kind == "f64":
        available = available and hasattr(_native, "aggregate_f64_buffer_masked_v1")
    _buffer_capability_cache[kind] = (_native, available)
    return available


def _copy_would_dominate_short_circuit(
    plan: Pipeline,
    *,
    known_buffer_kind: NativeKind | None = None,
) -> bool:
    """Detect a tiny ``take`` bound whose output does not justify copying a large container."""
    source = plan.source.native_data
    size = plan.source.current_exact_size()
    if (
        not isinstance(source, (list, tuple))
        and known_buffer_kind is None
        and _numpy_buffer_kind(source) is None
    ) or size is None:
        return False
    limits = [operation.count for operation in plan.operations if isinstance(operation, TakeOp)]
    if not limits:
        return False
    smallest_limit = min(limits)
    return (
        smallest_limit <= _COPY_SHORT_CIRCUIT_LIMIT
        and smallest_limit * _COPY_SHORT_CIRCUIT_RATIO < size
    )


def _retained_one_shot_decision(plan: Pipeline) -> EngineDecision | None:
    """Keep retained one-shot sources on the executor that owns canonical claiming."""
    if plan.source.native_data is None or plan.source.capabilities.reiterable:
        return None
    reason = "retained one-shot sources require canonical claiming"
    if plan.engine == "native":
        raise NativeUnsupportedError(reason)
    return EngineDecision("python", reason)


def select_engine(
    plan: Pipeline,
    *,
    _known_buffer_kind: NativeKind | None = None,
) -> EngineDecision:
    """Select Python or native execution for a fully compilable operation pipeline.

    Forced native mode raises on compilation or extension failure. Automatic mode also applies
    source-size and short-circuit copy-cost guards before crossing into Rust.
    """
    if plan.engine == "python":
        return EngineDecision("python", "python engine explicitly requested")
    if retained_one_shot := _retained_one_shot_decision(plan):
        return retained_one_shot
    # Rust adapters copy list/tuple sources in full; a tiny take result cannot amortize that cost.
    if plan.engine == "auto" and _copy_would_dominate_short_circuit(
        plan,
        known_buffer_kind=_known_buffer_kind,
    ):
        return EngineDecision(
            "python",
            "avoided copying the entire list or tuple for a tiny short-circuit output",
        )

    program, reason = _compile(plan, known_buffer_kind=_known_buffer_kind)
    if program is None:
        if plan.engine == "native":
            raise NativeUnsupportedError(reason)
        return EngineDecision("python", reason)
    buffer_kind = (
        _known_buffer_kind if _known_buffer_kind is not None else _numpy_buffer_kind(program.source)
    )
    if buffer_kind is not None and not _buffer_extraction_available(buffer_kind):
        reason = f"native extension lacks {buffer_kind} buffer snapshot endpoints"
        if plan.engine == "native":
            raise NativeUnsupportedError(reason)
        return EngineDecision("python", reason)
    if not _extension_available(program.kind):
        if plan.engine == "native":
            raise NativeUnsupportedError("native extension is not installed")
        return EngineDecision("python", "native extension is not installed")
    if plan.engine == "auto":
        size = plan.source.current_exact_size()
        if size is None or size < _AUTO_THRESHOLD:
            return EngineDecision(
                "python",
                f"source size is below native crossover threshold {_AUTO_THRESHOLD}",
            )
    return EngineDecision("native", reason, program, len(plan.operations))


def _identity_program(
    plan: Pipeline,
    buffer_kind: NativeKind | None,
) -> tuple[NativeProgram | None, str]:
    """Build an operation-free native program without scanning a numeric container."""
    source = plan.source.native_data
    if isinstance(source, range):
        if not _valid_source(source):
            return None, "range endpoints must fit in signed i64"
        return (
            NativeProgram(source, (), "i64"),
            "numeric range can use the native identity kernel",
        )
    if buffer_kind is not None:
        return (
            NativeProgram(source, (), buffer_kind),
            f"explicit NumPy column can use the native {buffer_kind} buffer kernel",
        )
    if not isinstance(source, (list, tuple)):
        return None, "identity native terminals require a range, list, tuple, or numeric buffer"
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
    if _is_exact_i64(first):
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


def _exact_number_mean_available() -> bool:
    """Return whether the extension can validate a mixed exact-number container itself."""
    try:
        from .. import _native
    except ImportError:
        return False
    return callable(getattr(_native, "mean_exact_numbers_v1", None))


def _auto_exact_container_mean_program(
    plan: Pipeline,
    terminal: TerminalName,
) -> tuple[NativeProgram, str] | None:
    """Build the mean-only mixed-number candidate without inspecting any element.

    The program kind is only the carrier used by the established native decision. Execution
    calls ``mean_exact_numbers_v1`` before every typed adapter, so Rust owns exact built-in
    validation and can safely decline subclasses or unsupported values for Python replay.
    """
    source = plan.source.native_data
    if (
        plan.engine != "auto"
        or terminal != "mean"
        or type(source) not in (list, tuple)
        or not _exact_number_mean_available()
    ):
        return None
    return (
        NativeProgram(source, (), "i64"),
        "exact list or tuple can use the mixed-number mean kernel without a type pre-scan",
    )


def _forced_native_exact_count_decision(
    plan: Pipeline,
    terminal: TerminalName,
    source: object,
    buffer_kind: NativeKind | None,
    *,
    source_is_container: bool,
) -> EngineDecision | None:
    """Select metadata-only count for a supported retained native container."""
    if not (
        plan.engine == "native"
        and terminal == "count"
        and exact_count(plan) is not None
        and (type(source) in (list, tuple, range) or buffer_kind is not None)
    ):
        return None
    if not _extension_available("i64"):
        raise NativeUnsupportedError("native extension is not installed")
    return _terminal_metadata(
        EngineDecision(
            "native",
            "exact retained source cardinality needs no element conversion",
            NativeProgram(source, (), "i64"),
        ),
        plan,
        terminal,
        source_is_container=source_is_container,
    )


def _buffer_short_circuit_decision(
    plan: Pipeline,
    terminal: TerminalName,
    buffer_kind: NativeKind | None,
) -> EngineDecision | None:
    """Keep automatic buffer terminals streaming when a full snapshot defeats short-circuiting."""
    if buffer_kind is None:
        return None
    if terminal in {"any", "all"}:
        reason = "NumPy buffer any/all stays in Python to preserve short-circuiting"
        if plan.engine == "native":
            raise NativeUnsupportedError(reason)
        return EngineDecision("python", reason)
    if terminal == "first":
        reason = "NumPy buffer first stays in Python to preserve short-circuiting"
        if plan.engine == "native":
            raise NativeUnsupportedError(reason)
        return EngineDecision("python", reason)
    return None


def _select_operation_terminal_engine(
    plan: Pipeline,
    terminal: TerminalName,
) -> EngineDecision:
    """Select one terminal engine for a non-identity numeric pipeline."""
    if plan.engine == "python":
        return _terminal_metadata(select_engine(plan), plan, terminal)
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
    buffer_kind = _numpy_buffer_kind(source)
    source_is_container = isinstance(source, (list, tuple)) or buffer_kind is not None
    if buffered := _buffer_short_circuit_decision(plan, terminal, buffer_kind):
        return _terminal_metadata(
            buffered,
            plan,
            terminal,
            source_is_container=source_is_container,
        )
    decision = select_engine(plan, _known_buffer_kind=buffer_kind)
    if (
        plan.engine == "auto"
        and terminal in {"first", "any", "all"}
        and type(source) in (list, tuple)
        and decision.engine == "native"
        and decision.program is not None
        and not _container_probe_available(decision.program.kind)
    ):
        decision = EngineDecision(
            "python",
            "native extension lacks bounded container short-circuit probes",
        )
    return _terminal_metadata(
        decision,
        plan,
        terminal,
        source_is_container=source_is_container,
    )


def select_terminal_engine(plan: Pipeline, terminal: TerminalName) -> EngineDecision:
    """Select an engine for a terminal, including operation-free native identity kernels.

    Automatic list/tuple terminals normally stay in Python to avoid a type scan and Rust copy.
    Statistics are the exception: their Python fallback performs substantially more work per
    item than the exact-container copy, so sufficiently large numeric containers use the native
    identity kernel. Ranges can use native identity kernels after capability and crossover checks.
    """
    terminal = validate_terminal(terminal)
    if plan.operations:
        return _select_operation_terminal_engine(plan, terminal)
    if plan.engine == "python":
        return _terminal_metadata(
            EngineDecision("python", "python engine explicitly requested"),
            plan,
            terminal,
        )
    source = plan.source.native_data
    if retained_one_shot := _retained_one_shot_decision(plan):
        return _terminal_metadata(retained_one_shot, plan, terminal)
    buffer_kind = _numpy_buffer_kind(source)
    source_is_container = isinstance(source, (list, tuple)) or buffer_kind is not None
    metadata_count = _forced_native_exact_count_decision(
        plan,
        terminal,
        source,
        buffer_kind,
        source_is_container=source_is_container,
    )
    if metadata_count is not None:
        return metadata_count
    if buffered := _buffer_short_circuit_decision(plan, terminal, buffer_kind):
        return _terminal_metadata(
            buffered,
            plan,
            terminal,
            source_is_container=source_is_container,
        )
    if (
        plan.engine == "auto"
        and isinstance(source, (list, tuple))
        and terminal not in {"mean", "statistics"}
        and not _auto_direct_i64_sum_candidate(source, terminal)
    ):
        return _terminal_metadata(
            EngineDecision(
                "python",
                "identity list/tuple stays in Python below its direct native crossover",
            ),
            plan,
            terminal,
            source_is_container=source_is_container,
        )
    mean_candidate = _auto_exact_container_mean_program(plan, terminal)
    program, reason = (
        mean_candidate if mean_candidate is not None else _identity_program(plan, buffer_kind)
    )
    if program is None:
        if plan.engine == "native":
            raise NativeUnsupportedError(reason)
        return _terminal_metadata(
            EngineDecision("python", reason),
            plan,
            terminal,
            source_is_container=source_is_container,
        )
    if buffer_kind is not None and not _buffer_extraction_available(buffer_kind):
        reason = f"native extension lacks {buffer_kind} buffer snapshot endpoints"
        if plan.engine == "native":
            raise NativeUnsupportedError(reason)
        return _terminal_metadata(
            EngineDecision("python", reason),
            plan,
            terminal,
            source_is_container=source_is_container,
        )
    if mean_candidate is None and not _extension_available(program.kind):
        if plan.engine == "native":
            raise NativeUnsupportedError("native extension is not installed")
        return _terminal_metadata(
            EngineDecision("python", "native extension is not installed"),
            plan,
            terminal,
            source_is_container=source_is_container,
        )
    if plan.engine == "auto":
        size = plan.source.current_exact_size()
        if size is None or size < _AUTO_THRESHOLD:
            return _terminal_metadata(
                EngineDecision(
                    "python",
                    f"source size is below native crossover threshold {_AUTO_THRESHOLD}",
                ),
                plan,
                terminal,
                source_is_container=source_is_container,
            )
    return _terminal_metadata(
        EngineDecision("native", reason, program),
        plan,
        terminal,
        source_is_container=source_is_container,
    )


def select_materializing_engine(plan: Pipeline) -> EngineDecision:
    """Select full native or safe hybrid execution for a terminal that consumes all output.

    Hybrid mode is considered only after automatic full-plan compilation fails. It requires an
    available extension, a source above the crossover threshold, and a Python suffix that does
    not depend on streaming a short-circuit or bounded external-sort stage.
    """
    if not plan.operations and _numpy_buffer_kind(plan.source.native_data) is not None:
        return select_terminal_engine(plan, "list")
    decision = select_engine(plan)
    if _auto_i64_materialization_exposes_external_identity(plan, decision):
        return EngineDecision(
            "python",
            _AUTO_I64_EXTERNAL_IDENTITY_REASON,
        )
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

    size = plan.source.current_exact_size()
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


def _auto_i64_materialization_exposes_external_identity(
    plan: Pipeline,
    decision: EngineDecision,
) -> bool:
    """Keep automatic exact i64 outputs in Python while they may expose input identities.

    Exact list and tuple elements belong to the caller. Native pass-through stages would
    re-box those integers, so they preserve the externally-owned state. An exact integer
    expression map clears the state only when its root necessarily allocates a result and
    its program actually reads an item. Constant-only maps expose their retained constant,
    while item, abs, and modulo can preserve an input/result identity and therefore propagate
    the incoming state. Forced native and f64 plans deliberately retain their established
    contracts.
    """
    program = decision.program
    if (
        plan.engine != "auto"
        or decision.engine != "native"
        or program is None
        or program.kind != "i64"
        or type(program.source) not in (list, tuple)
        or len(program.stages) != len(plan.operations)
    ):
        return False

    externally_owned = True
    for operation, stage in zip(plan.operations, program.stages, strict=True):
        if not isinstance(operation, MapOp):
            continue
        if type(operation) is not MapOp or type(operation.function) is not Expr:
            return True
        expression = operation.function
        if type(stage) is not tuple or len(stage) != 2:
            return True
        instructions = stage[1]
        contains_item = type(instructions) is tuple and any(
            type(instruction) is tuple
            and len(instruction) == 2
            and type(instruction[0]) is int
            and instruction[0] == 0
            for instruction in instructions
        )
        if not contains_item:
            externally_owned = True
        elif expression.operation in _ALLOCATING_I64_MAP_ROOTS:
            externally_owned = False
        elif expression.operation in _IDENTITY_PROPAGATING_I64_MAP_ROOTS:
            continue
        else:
            return True
    return externally_owned
