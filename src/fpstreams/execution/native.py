"""Dispatch compiled numeric programs to type- and source-specific Rust kernels."""

from __future__ import annotations

import sys
from collections.abc import Callable, Iterator
from typing import Any, TypeAlias, cast

from .. import _native
from ..collecting.statistics import StatisticsSnapshot
from ..expressions.scalar import Expr
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
MeanDependencyBinding: TypeAlias = tuple[
    dict[str, Any],
    dict[str, Any] | None,
    str,
    object,
]
NumericIteratorMeanOutcome: TypeAlias = tuple[int, int, float, float, object | None]

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
_BUFFER_TERMINAL_FIELDS = {
    "count": (1 << 0, 0),
    "sum": (1 << 1, 1),
    "min": (1 << 2, 2),
    "max": (1 << 3, 3),
    "first": (1 << 4, 4),
    "last": (1 << 5, 5),
}
_BUFFER_STATISTICS_MASK = (1 << 0) | (1 << 6) | (1 << 7)
_BUFFER_FULL_AGGREGATE_MASK = (1 << 8) - 1
_AGGREGATE_COUNT = 1 << 0
_AGGREGATE_TOTAL = 1 << 1
_RANGE_METADATA_TERMINALS = frozenset({"sum", "min", "max", "last"})
_I64_MIN = -(2**63)
_I64_MAX = 2**63 - 1
_I64_UNARY_EXPRESSION_OPCODES = frozenset({7, 16, 17})
_I64_BINARY_EXPRESSION_OPCODES = frozenset({*range(2, 7), *range(8, 16)})
_DIRECT_I64_MAP_ROOT_OPCODES = frozenset({2, 3, 4, 5, 7})

I64Instruction: TypeAlias = tuple[int, int]
I64Instructions: TypeAlias = tuple[I64Instruction, ...]
DirectI64MapListEndpoint: TypeAlias = Callable[[object, I64Instructions], list[Any] | None]
DirectI64FilterListEndpoint: TypeAlias = Callable[[object, I64Instructions, bool], list[Any] | None]


def _valid_i64_expression_instructions(instructions: object) -> tuple[bool, bool]:
    """Validate one exact postfix i64 program and report whether it reads the item."""
    if type(instructions) is not tuple or not instructions:
        return False, False
    depth = 0
    contains_item = False
    for instruction in instructions:
        if type(instruction) is not tuple or len(instruction) != 2:
            return False, False
        opcode, operand = instruction
        if type(opcode) is not int or type(operand) is not int:
            return False, False
        if not _I64_MIN <= operand <= _I64_MAX:
            return False, False
        if opcode == 0:
            if operand != 0:
                return False, False
            contains_item = True
            depth += 1
        elif opcode == 1:
            depth += 1
        elif opcode in _I64_UNARY_EXPRESSION_OPCODES:
            if operand != 0 or depth < 1:
                return False, False
        elif opcode in _I64_BINARY_EXPRESSION_OPCODES:
            if operand != 0 or depth < 2:
                return False, False
            depth -= 1
        else:
            return False, False
    return depth == 1, contains_item


def _valid_direct_i64_map_instructions(instructions: object) -> bool:
    """Validate the allocating subset accepted by the direct exact-list map ABI."""
    valid, contains_item = _valid_i64_expression_instructions(instructions)
    if not valid:
        return False
    assert type(instructions) is tuple
    root_opcode = instructions[-1][0]
    return contains_item and root_opcode in _DIRECT_I64_MAP_ROOT_OPCODES


def direct_i64_map_list_program(
    program: NativeProgram,
) -> tuple[bool, I64Instructions | None]:
    """Recognize the decision-owned Phase A shape and validate its frozen instructions.

    The boolean marks an exact one-stage i64 map whose fallback is owned by the guarded
    dispatcher. A ``None`` instruction payload means that shape was retained but corrupted;
    callers must replay Python instead of exposing it to the generic native adapter.
    """
    if (
        type(program) is not NativeProgram
        or program.kind != "i64"
        or type(program.source) not in (list, tuple)
        or type(program.stages) is not tuple
        or len(program.stages) != 1
    ):
        return False, None
    stage = program.stages[0]
    if type(stage) is not tuple or len(stage) != 2 or type(stage[0]) is not int or stage[0] != 0:
        return False, None
    instructions = stage[1]
    if not _valid_direct_i64_map_instructions(instructions):
        return True, None
    return True, cast(I64Instructions, instructions)


def direct_i64_filter_list_expression(
    expression: object,
) -> tuple[I64Instructions, Callable[[int], int | bool]] | None:
    """Compile one frozen scalar expression for the identity-preserving filter sink."""
    if type(expression) is not Expr:
        return None
    instructions = object.__getattribute__(expression, "_instructions")
    if instructions is None:
        instructions = expression.native_instructions()
    if not _valid_i64_expression_instructions(instructions)[0]:
        return None
    evaluator = object.__getattribute__(expression, "_evaluator")
    if evaluator is None:
        evaluator = expression._python_evaluator()
    return instructions, evaluator


def direct_i64_map_list_endpoint() -> DirectI64MapListEndpoint | None:
    """Resolve the optional Phase A symbol without rejecting older extension wheels."""
    endpoint = getattr(_native, "materialize_i64_map_exact_list_v1", None)
    return cast(DirectI64MapListEndpoint, endpoint) if callable(endpoint) else None


def direct_i64_filter_list_endpoint() -> DirectI64FilterListEndpoint | None:
    """Resolve the optional identity-preserving exact-list filter symbol."""
    endpoint = getattr(_native, "materialize_i64_filter_exact_list_v1", None)
    return cast(DirectI64FilterListEndpoint, endpoint) if callable(endpoint) else None


def _i64_buffer_aggregate_endpoint() -> Any:
    """Prefer the allocation-free identity reducer when the wheel provides it."""
    return getattr(
        _native,
        "aggregate_i64_buffer_masked_v2",
        _native.aggregate_i64_buffer_masked_v1,
    )


def _i64_buffer_mean_endpoint() -> Any:
    """Prefer the allocation-free identity mean while retaining old-wheel compatibility."""
    return getattr(_native, "mean_i64_buffer_v2", _native.mean_i64_buffer_v1)


def _f64_buffer_terminal_endpoint() -> Any:
    """Prefer the allocation-free identity terminal while retaining old-wheel compatibility."""
    return getattr(_native, "terminal_f64_buffer_v2", _native.terminal_f64_buffer_v1)


def _f64_buffer_mean_endpoint() -> Any:
    """Prefer the allocation-free identity mean while retaining old-wheel compatibility."""
    return getattr(_native, "mean_f64_buffer_v2", _native.mean_f64_buffer_v1)


def _f64_buffer_aggregate_endpoint() -> Any:
    """Prefer the allocation-free identity aggregate while retaining old-wheel compatibility."""
    return getattr(
        _native,
        "aggregate_f64_buffer_masked_v2",
        _native.aggregate_f64_buffer_masked_v1,
    )


def _i64_buffer(program: NativeProgram) -> Any | None:
    """Return the live ndarray retained by an explicit NumPy column source."""
    from ..tabular.numpy import numpy_i64_buffer

    return numpy_i64_buffer(program.source)


def _f64_buffer(program: NativeProgram) -> Any | None:
    """Return the live ndarray retained by an explicit NumPy float column source."""
    from ..tabular.numpy import numpy_f64_buffer

    return numpy_f64_buffer(program.source)


def sequential_f64_aggregate_total_available() -> bool:
    """Return whether this wheel promises Python-compatible aggregate total order."""
    marker = getattr(_native, "sequential_f64_aggregate_total_v1", None)
    return callable(marker) and marker() is True


def materialize_available(program: NativeProgram) -> bool:
    """Return whether this extension exposes the direct container endpoint.

    This deliberately checks only the new optional capability. Older wheels may
    still execute native programs through their established iterator endpoint.
    """
    if _i64_buffer(program) is not None:
        return hasattr(_native, "materialize_i64_buffer_v1")
    if _f64_buffer(program) is not None:
        return hasattr(_native, "materialize_f64_buffer_v1")
    suffix = "_range" if isinstance(program.source, range) else ""
    return hasattr(_native, f"materialize_{program.kind}{suffix}")


def execute_materialize(program: NativeProgram, target: str) -> Any:
    """Run one complete numeric program into its final Python collection."""
    target_code = _MATERIALIZE_TARGETS[target]
    source = program.source
    stages = list(program.stages)
    if (buffer := _i64_buffer(program)) is not None:
        return _native.materialize_i64_buffer_v1(buffer, stages, target_code)
    if (buffer := _f64_buffer(program)) is not None:
        return _native.materialize_f64_buffer_v1(buffer, stages, target_code)
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


def _identity_i64_range_terminal(
    program: NativeProgram,
    terminal: str,
) -> tuple[bool, int | None]:
    """Resolve exact identity-range reductions without visiting their elements."""
    source = program.source
    if (
        program.kind != "i64"
        or program.stages
        or type(source) is not range
        or terminal not in _RANGE_METADATA_TERMINALS
    ):
        return False, None
    # Call the exact built-in type slot directly. Looking up ``len`` through
    # Python builtins would let a later monkeypatch corrupt this metadata-only
    # result even though iterating the retained range still yields every item.
    size = range.__len__(source)
    if size == 0:
        return True, 0 if terminal == "sum" else None
    first = source.start
    last = source[-1]
    match terminal:
        case "sum":
            endpoints = first + last
            # One of the factors is even. Divide it first so the formula stays narrow even
            # though Python integers already provide arbitrary-width overflow safety.
            result = (size // 2) * endpoints if size % 2 == 0 else size * (endpoints // 2)
        case "min":
            result = first if source.step > 0 else last
        case "max":
            result = last if source.step > 0 else first
        case "last":
            result = last
        case _:  # pragma: no cover - guarded by _RANGE_METADATA_TERMINALS
            raise AssertionError(f"unhandled range terminal {terminal!r}")
    return True, result


def execute(program: NativeProgram) -> Iterator[Any]:
    """Run all fused stages in Rust and iterate the materialized numeric output.

    Float and integer programs use separate kernels, with range sources routed to
    specialized start/stop/step entry points.
    """
    source = program.source
    stages = list(program.stages)
    values: list[int] | list[float]
    if (buffer := _f64_buffer(program)) is not None:
        values = _native.execute_f64_buffer_v1(buffer, stages)
    elif program.kind == "f64":
        if isinstance(source, range):
            values = _native.execute_f64_range(source.start, source.stop, source.step, stages)
        else:
            values = _native.execute_f64(source, stages)
    elif (buffer := _i64_buffer(program)) is not None:
        values = _native.execute_i64_buffer_v1(buffer, stages)
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
    range_metadata, range_result = _identity_i64_range_terminal(program, terminal)
    if range_metadata:
        return range_result
    if (buffer := _i64_buffer(program)) is not None:
        mask_and_index = _BUFFER_TERMINAL_FIELDS.get(terminal)
        if mask_and_index is None:
            raise TypeError(f"terminal {terminal!r} is not supported for an i64 buffer")
        mask, index = mask_and_index
        snapshot = _i64_buffer_aggregate_endpoint()(buffer, stages, mask)
        return cast(int | None, snapshot[index])
    if (buffer := _f64_buffer(program)) is not None:
        code = _TERMINALS[terminal]
        if terminal == "sum" and sys.version_info < (3, 12):
            code = 8
        emitted_count, result = cast(
            tuple[int, float | None],
            _f64_buffer_terminal_endpoint()(buffer, stages, code),
        )
        if terminal == "count":
            return emitted_count
        if terminal == "sum" and emitted_count == 0:
            return 0
        return result
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
    if (buffer := _i64_buffer(program)) is not None:
        snapshot = _i64_buffer_aggregate_endpoint()(
            buffer,
            stages,
            _BUFFER_STATISTICS_MASK,
        )
        return snapshot[0], snapshot[6], snapshot[7]
    if (buffer := _f64_buffer(program)) is not None:
        float_snapshot = _f64_buffer_aggregate_endpoint()(
            buffer,
            stages,
            _BUFFER_STATISTICS_MASK,
        )
        return float_snapshot[0], float_snapshot[6], float_snapshot[7]
    if program.kind == "f64":
        if isinstance(source, range):
            return _native.statistics_f64_range(source.start, source.stop, source.step, stages)
        return _native.statistics_f64(source, stages)
    if isinstance(source, range):
        return _native.statistics_i64_range(source.start, source.stop, source.step, stages)
    return _native.statistics_i64(source, stages)


def execute_exact_container_mean(
    program: NativeProgram,
) -> tuple[bool, float | None] | None:
    """Try the optional mixed exact-number ABI without entering a typed adapter.

    ``None`` means that the program shape or installed wheel does not expose this capability.
    The handled flag belongs to the Rust endpoint and distinguishes an accepted empty container
    from a side-effect-free decline that the automatic engine may replay in Python.
    """
    source = program.source
    if program.stages or type(source) not in (list, tuple):
        return None
    exact_numbers = getattr(_native, "mean_exact_numbers_v1", None)
    if not callable(exact_numbers):
        return None
    return cast(tuple[bool, float | None], exact_numbers(source))


def execute_numeric_iterator_mean(
    values: Iterator[Any],
    dependency_bindings: tuple[MeanDependencyBinding, ...],
    mean_function: Callable[..., object],
    mean_code: object,
    continuation_guard: Callable[[], bool],
) -> NumericIteratorMeanOutcome | None:
    """Run callback-free exact chunks while retaining one stable optional-wheel endpoint."""
    endpoint = getattr(_native, "mean_exact_iterator_chunk_v1", None)
    if not callable(endpoint):
        return None
    count = 0
    total = 0.0
    compensation = 0.0
    while True:
        if not continuation_guard():
            return 3, count, total, compensation, None
        outcome = cast(
            NumericIteratorMeanOutcome,
            endpoint(
                values,
                count,
                total,
                compensation,
                dependency_bindings,
                mean_function,
                mean_code,
            ),
        )
        status, count, total, compensation, _boundary = outcome
        if status != 1:
            return outcome


def execute_mean(program: NativeProgram) -> float | None:
    """Compute a compensated mean without variance state when the wheel supports it."""
    source = program.source
    stages = list(program.stages)
    if (buffer := _i64_buffer(program)) is not None:
        return cast(float | None, _i64_buffer_mean_endpoint()(buffer, stages))
    if (buffer := _f64_buffer(program)) is not None:
        return cast(float | None, _f64_buffer_mean_endpoint()(buffer, stages))
    suffix = "_range" if isinstance(source, range) else ""
    endpoint = getattr(_native, f"mean_{program.kind}{suffix}", None)
    if endpoint is None:
        count, mean, _squared_deviations = execute_statistics(program)
        return mean if count else None
    if isinstance(source, range):
        return cast(float | None, endpoint(source.start, source.stop, source.step, stages))
    return cast(float | None, endpoint(source, stages))


def _normalize_empty_f64_total(snapshot: NativeAggregateSnapshot) -> NativeAggregateSnapshot:
    """Restore the Python aggregator's exact integer zero only for a proven empty result."""
    if snapshot[0] != 0 or snapshot[1] != 0.0:
        return snapshot
    return (
        snapshot[0],
        0,
        snapshot[2],
        snapshot[3],
        snapshot[4],
        snapshot[5],
        snapshot[6],
        snapshot[7],
    )


def execute_aggregate(program: NativeProgram, mask: int | None = None) -> NativeAggregateSnapshot:
    """Compute requested built-in fields in one fused Rust traversal.

    New extensions accept an optional field mask while retaining the established
    count/sum/min/max/first/last/mean/M2 tuple. If that endpoint is absent, an
    older wheel safely computes the full snapshot instead.
    """
    source = program.source
    stages = list(program.stages)
    range_metadata, range_total = _identity_i64_range_terminal(program, "sum")
    if mask == _AGGREGATE_TOTAL and range_metadata:
        return (0, cast(int, range_total), None, None, None, None, 0.0, 0.0)
    if (buffer := _i64_buffer(program)) is not None:
        return cast(
            NativeAggregateSnapshot,
            _i64_buffer_aggregate_endpoint()(
                buffer,
                stages,
                _BUFFER_FULL_AGGREGATE_MASK if mask is None else mask,
            ),
        )
    if (buffer := _f64_buffer(program)) is not None:
        float_mask = _BUFFER_FULL_AGGREGATE_MASK if mask is None else mask
        if float_mask & _AGGREGATE_TOTAL:
            float_mask |= _AGGREGATE_COUNT
        snapshot = cast(
            NativeAggregateSnapshot,
            _f64_buffer_aggregate_endpoint()(
                buffer,
                stages,
                float_mask,
            ),
        )
        return _normalize_empty_f64_total(snapshot)
    suffix = "_range" if isinstance(source, range) else ""
    effective_mask = mask
    if program.kind == "f64" and effective_mask is not None and effective_mask & _AGGREGATE_TOTAL:
        effective_mask |= _AGGREGATE_COUNT
    if effective_mask is not None:
        masked = getattr(_native, f"aggregate_{program.kind}{suffix}_masked", None)
        if masked is not None:
            if isinstance(source, range):
                snapshot = cast(
                    NativeAggregateSnapshot,
                    masked(source.start, source.stop, source.step, stages, effective_mask),
                )
            else:
                snapshot = cast(NativeAggregateSnapshot, masked(source, stages, effective_mask))
            return _normalize_empty_f64_total(snapshot) if program.kind == "f64" else snapshot
    if program.kind == "f64":
        if isinstance(source, range):
            snapshot = _native.aggregate_f64_range(source.start, source.stop, source.step, stages)
        else:
            snapshot = _native.aggregate_f64(source, stages)
        return _normalize_empty_f64_total(snapshot)
    if isinstance(source, range):
        return _native.aggregate_i64_range(source.start, source.stop, source.step, stages)
    return _native.aggregate_i64(source, stages)
