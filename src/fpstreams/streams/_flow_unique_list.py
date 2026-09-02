"""Adaptive stable-unique list materialization for retained numeric sources."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any, cast

from ..execution import sync_ops as _sync_ops
from ..execution.physical import operations_from_physical_nodes
from ..execution.sync import open_operations as _CANONICAL_OPEN_OPERATIONS
from ..execution.sync_ops import close_iterators
from ..physical.plan import BackendPayload, PhysicalPlan
from ..planning.logical import Pipeline
from ..planning.native import _AUTO_I64_EXTERNAL_IDENTITY_REASON
from ..planning.source import (
    _CANONICAL_RETAINED_SEQUENCE,
    _CANONICAL_SOURCE_OPEN,
    _CANONICAL_SOURCE_OPEN_CODE,
    Source,
)
from ..planning.sync import UniqueOp
from ..runtime.query import QueryRuntime

_BUILTIN_INT: type[int] = int
_BUILTIN_BOOL: type[bool] = bool
_BUILTIN_ALL = all
_BUILTIN_DICT: type[dict[Any, Any]] = dict
_BUILTIN_GETATTR = getattr
_BUILTIN_NAME_ERROR: type[NameError] = NameError
_BUILTIN_LIST: type[list[Any]] = list
_BUILTIN_RANGE = range
_BUILTIN_SET: type[set[Any]] = set
_BUILTIN_TUPLE: type[tuple[Any, ...]] = tuple
_BUILTIN_TYPE: Callable[[Any], type[Any]] = type
_BUILTIN_VALUE_ERROR: type[ValueError] = ValueError
_BUILTIN_LEN: Callable[[Any], int] = len
_NATIVE_UNIQUE_MIN_ROWS = 4_096
_RANGE_UNIQUE_MIN_ROWS = 4_096
_NATIVE_UNIQUE_PREFIX_SAMPLE = 128
_NATIVE_UNIQUE_SPREAD_SAMPLE = 128
_NATIVE_UNIQUE_CACHE_MAX_DISTINCT = 32
_UNIQUE_SAMPLE_REJECT = 0
_UNIQUE_SAMPLE_BASELINE = 1
_UNIQUE_SAMPLE_CACHED = 2
_UNIQUE_SAMPLE_IDENTITY_CACHED = 3
_UNIQUE_LOW_CARDINALITY_SAMPLES = frozenset({_UNIQUE_SAMPLE_CACHED, _UNIQUE_SAMPLE_IDENTITY_CACHED})
_MISSING = object()
_CANONICAL_UNIQUE_HANDLER = _sync_ops.OPERATION_HANDLERS[UniqueOp]
_CANONICAL_UNIQUE_CODE = cast(Any, _CANONICAL_UNIQUE_HANDLER).__code__
_CANONICAL_RETAINED_SEQUENCE_CODE = _CANONICAL_RETAINED_SEQUENCE.__code__
_CANONICAL_OPEN_OPERATIONS_CODE = _CANONICAL_OPEN_OPERATIONS.__code__
_CANONICAL_OPEN_OPERATIONS_WRAPPED = _BUILTIN_GETATTR(
    _CANONICAL_OPEN_OPERATIONS, "__wrapped__", None
)
_CANONICAL_OPEN_OPERATIONS_WRAPPED_CODE = _BUILTIN_GETATTR(
    _CANONICAL_OPEN_OPERATIONS_WRAPPED,
    "__code__",
    None,
)
_EMPTY_CLOSURE_CELL = object()


def _closure_values(function: Any) -> tuple[Any, ...]:
    """Snapshot closure-cell identities without invoking user equality protocols."""
    values: list[Any] = []
    for cell in _BUILTIN_GETATTR(function, "__closure__", None) or ():
        try:
            values.append(cell.cell_contents)
        except _BUILTIN_VALUE_ERROR:
            values.append(_EMPTY_CLOSURE_CELL)
    return _BUILTIN_TUPLE(values)


_CANONICAL_OPEN_OPERATIONS_CLOSURE = _closure_values(_CANONICAL_OPEN_OPERATIONS)
_CANONICAL_OPEN_OPERATIONS_WRAPPED_CLOSURE = _closure_values(_CANONICAL_OPEN_OPERATIONS_WRAPPED)
_RANGE_ITERATOR_TYPES = (
    _BUILTIN_TYPE(iter(_BUILTIN_RANGE(0))),
    _BUILTIN_TYPE(iter(_BUILTIN_RANGE(0, 1 << 100))),
)


def _same_closure(function: Any, expected: tuple[Any, ...]) -> bool:
    """Return whether live closure cells still retain the captured objects."""
    current = _closure_values(function)
    if _BUILTIN_LEN(current) != _BUILTIN_LEN(expected):
        return False
    return _BUILTIN_ALL(
        current[index] is expected[index] for index in _BUILTIN_RANGE(_BUILTIN_LEN(current))
    )


def _canonical_open_operations_intact(flow_terminals: Any) -> bool:
    """Validate both contextmanager wrapper and wrapped iterator-chain builder."""
    live = flow_terminals.__dict__.get("open_operations")
    if live is not _CANONICAL_OPEN_OPERATIONS:
        return False
    wrapped = _BUILTIN_GETATTR(_CANONICAL_OPEN_OPERATIONS, "__wrapped__", None)
    return (
        _BUILTIN_GETATTR(live, "__code__", None) is _CANONICAL_OPEN_OPERATIONS_CODE
        and wrapped is _CANONICAL_OPEN_OPERATIONS_WRAPPED
        and _BUILTIN_GETATTR(wrapped, "__code__", None) is _CANONICAL_OPEN_OPERATIONS_WRAPPED_CODE
        and _same_closure(live, _CANONICAL_OPEN_OPERATIONS_CLOSURE)
        and _same_closure(wrapped, _CANONICAL_OPEN_OPERATIONS_WRAPPED_CLOSURE)
    )


def _canonical_source_access_intact() -> bool:
    """Validate source methods before reading retained data or opening iteration."""
    return (
        Source.__dict__.get("open") is _CANONICAL_SOURCE_OPEN
        and _CANONICAL_SOURCE_OPEN.__code__ is _CANONICAL_SOURCE_OPEN_CODE
        and Source.__dict__.get("retained_sequence") is _CANONICAL_RETAINED_SEQUENCE
        and _CANONICAL_RETAINED_SEQUENCE.__code__ is _CANONICAL_RETAINED_SEQUENCE_CODE
    )


def _resolved_unique_binding(name: str) -> Any:
    """Resolve one name exactly as the canonical handler's LOAD_GLOBAL does."""
    handler = cast(Any, _CANONICAL_UNIQUE_HANDLER)
    globals_namespace = handler.__globals__
    if name in globals_namespace:
        return globals_namespace[name]
    builtins_namespace = handler.__builtins__
    if _BUILTIN_TYPE(builtins_namespace) is _BUILTIN_DICT and name in builtins_namespace:
        return builtins_namespace[name]
    raise _BUILTIN_NAME_ERROR(f"name {name!r} is not defined", name=name)


_CANONICAL_UNIQUE_START_BINDINGS = tuple(
    (name, _resolved_unique_binding(name)) for name in ("set", "PAIR_KEY_SELECTOR")
)


def _canonical_unique_start_intact() -> bool:
    """Validate code and globals that canonical unique resolves before its first pull."""
    if (
        _sync_ops.OPERATION_HANDLERS.get(UniqueOp) is not _CANONICAL_UNIQUE_HANDLER
        or cast(Any, _CANONICAL_UNIQUE_HANDLER).__code__ is not _CANONICAL_UNIQUE_CODE
    ):
        return False
    try:
        for name, expected in _CANONICAL_UNIQUE_START_BINDINGS:
            if _resolved_unique_binding(name) is not expected:
                return False
    except _BUILTIN_NAME_ERROR:
        return False
    return True


def _sample_exact_i64(source: list[Any] | tuple[Any, ...]) -> int:
    """Classify one large representative exact-integer source without invoking protocols."""
    size = _BUILTIN_LEN(source)
    if size < _NATIVE_UNIQUE_MIN_ROWS:
        return _UNIQUE_SAMPLE_REJECT

    try:
        representatives: dict[int, Any] | None = _BUILTIN_DICT()
    except MemoryError:
        representatives = None
    duplicate_samples = 0
    identity_hits = 0
    prefix_count = min(_NATIVE_UNIQUE_PREFIX_SAMPLE, size)
    denominator = _NATIVE_UNIQUE_SPREAD_SAMPLE + 1
    sample_count = prefix_count + _NATIVE_UNIQUE_SPREAD_SAMPLE
    for position in _BUILTIN_RANGE(sample_count):
        index = (
            position
            if position < prefix_count
            else ((position - prefix_count + 1) * size) // denominator
        )
        try:
            value = source[index]
        except IndexError:
            return _UNIQUE_SAMPLE_REJECT
        if _BUILTIN_TYPE(value) not in (_BUILTIN_INT, _BUILTIN_BOOL):
            return _UNIQUE_SAMPLE_REJECT
        if representatives is not None:
            try:
                representative = representatives.get(value, _MISSING)
                if representative is _MISSING:
                    representatives[value] = value
                else:
                    duplicate_samples += 1
                    if representative is value:
                        identity_hits += 1
            except MemoryError:
                representatives = None
            else:
                if _BUILTIN_LEN(representatives) > _NATIVE_UNIQUE_CACHE_MAX_DISTINCT:
                    representatives = None
    if representatives is None:
        return _UNIQUE_SAMPLE_BASELINE
    if duplicate_samples and identity_hits * 4 >= duplicate_samples * 3:
        return _UNIQUE_SAMPLE_IDENTITY_CACHED
    return _UNIQUE_SAMPLE_CACHED


_CANONICAL_SAMPLE_EXACT_I64 = _sample_exact_i64
_ORIGINAL_SAMPLE_EXACT_I64 = _CANONICAL_SAMPLE_EXACT_I64
_CANONICAL_SAMPLE_EXACT_I64_CODE = cast(Any, _CANONICAL_SAMPLE_EXACT_I64).__code__


def _canonical_sampler_intact() -> bool:
    """Reject injected sampler code before the representative scan executes."""
    return (
        _CANONICAL_SAMPLE_EXACT_I64 is _ORIGINAL_SAMPLE_EXACT_I64
        and cast(Any, _ORIGINAL_SAMPLE_EXACT_I64).__code__ is _CANONICAL_SAMPLE_EXACT_I64_CODE
    )


def _guarded_sample_exact_i64(source: list[Any] | tuple[Any, ...]) -> int | None:
    """Run the sealed sampler only while both its alias and code stay canonical."""
    if not _canonical_sampler_intact():
        return None
    sample = _CANONICAL_SAMPLE_EXACT_I64(source)
    return sample if _canonical_sampler_intact() else None


_NativeUniqueEndpoint = Callable[
    [list[Any], Iterator[Any]],
    tuple[Any | None, bool] | None,
]


def _select_unique_endpoint(
    sample: int,
    baseline: _NativeUniqueEndpoint,
    cached: _NativeUniqueEndpoint | None,
    identity_cached: _NativeUniqueEndpoint | None,
) -> tuple[_NativeUniqueEndpoint, str]:
    """Select the narrowest endpoint proven by one live post-open sample."""
    if sample == _UNIQUE_SAMPLE_IDENTITY_CACHED and identity_cached is not None:
        return identity_cached, "unique_i64_exact_prefix_identity_cached_v1"
    if sample in _UNIQUE_LOW_CARDINALITY_SAMPLES and cached is not None:
        return cached, "unique_i64_exact_prefix_cached_v1"
    return baseline, "unique_i64_exact_prefix_v1"


def _prepend_unique_boundary(first: Any, source: Iterator[Any]) -> Iterator[Any]:
    """Yield the native incompatibility boundary once, then the still-open source."""
    yield first
    del first
    yield from source


def _seeded_unique_suffix(
    iterator: Iterator[Any],
    hashable: set[Any],
) -> Iterator[Any]:
    """Resume canonical keyless uniqueness inside a PEP 479 generator boundary."""
    unhashable: list[Any] = []
    for item in iterator:
        try:
            if item in hashable:
                continue
            hashable.add(item)
        except _resolved_unique_binding("TypeError"):
            live_any = cast(Callable[[object], bool], _resolved_unique_binding("any"))
            if live_any(item == seen for seen in unhashable):
                continue
            unhashable.append(item)
        yield item


def _append_unique_suffix(output: list[Any], iterator: Iterator[Any]) -> None:
    """Seed the canonical hashable state, then append a mixed suffix once."""
    hashable: set[Any] = _BUILTIN_SET(output)
    unique = _seeded_unique_suffix(iterator, hashable)
    try:
        output.extend(unique)
    finally:
        cast(Any, unique).close()


def _materialize_range_unique(
    physical: PhysicalPlan,
    pipeline: Pipeline,
    operations: tuple[Any, ...],
    operation: UniqueOp,
    flow_terminals: Any,
) -> list[Any]:
    """Consume a proven range iterator directly because every emitted integer is distinct."""
    from ..runtime.failpoints import has_active_failpoints
    from ..runtime.report import _record_direct_strategy

    output: list[Any] = []
    with QueryRuntime() as runtime:
        try:
            source_iterator = pipeline.source.open()
        except StopIteration as error:
            raise RuntimeError("generator raised StopIteration") from error
        closed_by_context = False
        active_error: BaseException | None = None
        try:
            live_open_operations = cast(Any, flow_terminals.__dict__.get("open_operations"))
            if (
                has_active_failpoints()
                or not _canonical_source_access_intact()
                or not _canonical_open_operations_intact(flow_terminals)
                or not _canonical_unique_start_intact()
                or _BUILTIN_TYPE(source_iterator) not in _RANGE_ITERATOR_TYPES
                or _BUILTIN_LEN(pipeline.operations) != 1
                or operation is not pipeline.operations[0]
                or operation.key is not None
            ):
                with live_open_operations(source_iterator, operations, runtime=runtime) as iterator:
                    closed_by_context = True
                    output.extend(iterator)
                return output

            live_retained = pipeline.source.retained_sequence()
            if _BUILTIN_TYPE(live_retained) is not _BUILTIN_RANGE:
                with live_open_operations(source_iterator, operations, runtime=runtime) as iterator:
                    closed_by_context = True
                    output.extend(iterator)
                return output

            output.extend(source_iterator)
            _record_direct_strategy(
                physical,
                "python_direct",
                "an exact range iterator is already distinct and was materialized without hashing",
            )
            return output
        except BaseException as error:
            active_error = error
            output.clear()
            raise
        finally:
            if not closed_by_context:
                close_iterators((source_iterator,), active_error=active_error)


def _direct_unique_plan_is_eligible(
    physical: PhysicalPlan,
    pipeline: Pipeline | None,
    flow_terminals: Any,
) -> bool:
    """Reject plans whose public Python execution surface is not canonical."""
    return (
        physical.root is None
        and pipeline is not None
        and physical.parallel is None
        and _BUILTIN_TYPE(pipeline.engine) is str
        and pipeline.engine == "auto"
        and physical.source is pipeline.source
        and _BUILTIN_TYPE(pipeline.source) is Source
        and _canonical_source_access_intact()
        and _canonical_open_operations_intact(flow_terminals)
        and _sync_ops.OPERATION_HANDLERS.get(UniqueOp) is _CANONICAL_UNIQUE_HANDLER
        and cast(Any, _CANONICAL_UNIQUE_HANDLER).__code__ is _CANONICAL_UNIQUE_CODE
    )


def _single_keyless_unique(
    physical: PhysicalPlan,
    pipeline: Pipeline,
) -> tuple[tuple[Any, ...], UniqueOp] | None:
    """Return the sole canonical keyless unique operation, if present."""
    operations = operations_from_physical_nodes(physical.nodes)
    raw_operation = operations[0] if _BUILTIN_LEN(operations) == 1 else None
    if (
        _BUILTIN_TYPE(operations) is not _BUILTIN_TUPLE
        or _BUILTIN_LEN(operations) != 1
        or _BUILTIN_TYPE(raw_operation) is not UniqueOp
        or _BUILTIN_LEN(pipeline.operations) != 1
        or raw_operation is not pipeline.operations[0]
    ):
        return None
    operation = cast(UniqueOp, raw_operation)
    return None if operation.key is not None else (operations, operation)


def _native_unique_payload_is_eligible(payload: Any) -> bool:
    """Return whether the compiler selected the retained exact-i64 unique sink."""
    if not isinstance(payload, BackendPayload) or payload.arrow_prefix is not None:
        return False
    decision = payload.native_decision
    return decision is not None and (
        decision.engine == "native" or decision.reason == _AUTO_I64_EXTERNAL_IDENTITY_REASON
    )


def _try_range_unique(
    retained: Any,
    physical: PhysicalPlan,
    pipeline: Pipeline,
    operations: tuple[Any, ...],
    operation: UniqueOp,
    flow_terminals: Any,
) -> tuple[bool, list[Any] | None] | None:
    """Handle an exact range, or return None when another source path should decide."""
    if _BUILTIN_TYPE(retained) is not _BUILTIN_RANGE:
        return None
    if _BUILTIN_LEN(cast(range, retained)) < _RANGE_UNIQUE_MIN_ROWS:
        return False, None
    return True, _materialize_range_unique(
        physical,
        pipeline,
        operations,
        operation,
        flow_terminals,
    )


def _load_native_unique_endpoints() -> (
    tuple[Any, _NativeUniqueEndpoint, _NativeUniqueEndpoint | None, _NativeUniqueEndpoint | None]
    | None
):
    """Resolve optional native endpoints without making the direct sink mandatory."""
    try:
        from .. import _native
    except ImportError:
        return None
    raw_baseline = getattr(_native, "unique_i64_exact_prefix_v1", None)
    if not callable(raw_baseline):
        return None
    raw_cached = getattr(_native, "unique_i64_exact_prefix_cached_v1", None)
    raw_identity = getattr(_native, "unique_i64_exact_prefix_identity_cached_v1", None)
    return (
        _native,
        cast(_NativeUniqueEndpoint, raw_baseline),
        cast(_NativeUniqueEndpoint | None, raw_cached if callable(raw_cached) else None),
        cast(_NativeUniqueEndpoint | None, raw_identity if callable(raw_identity) else None),
    )


def try_direct_unique_list(
    physical: PhysicalPlan,
    pipeline: Pipeline | None,
) -> tuple[bool, list[Any] | None]:
    """Push one large stable integer distinct sink into Rust."""
    from ..runtime.failpoints import has_active_failpoints
    from ..runtime.report import _record_direct_strategy
    from . import flow_terminals

    if not _direct_unique_plan_is_eligible(physical, pipeline, flow_terminals):
        return False, None
    assert pipeline is not None
    unique_plan = _single_keyless_unique(physical, pipeline)
    if unique_plan is None:
        return False, None
    operations, operation = unique_plan
    payload = physical.backend_payload
    if (
        not _canonical_source_access_intact()
        or not _canonical_open_operations_intact(flow_terminals)
        or not _canonical_unique_start_intact()
        or not _canonical_sampler_intact()
        or has_active_failpoints()
    ):
        return False, None
    retained = pipeline.source.retained_sequence()
    range_result = _try_range_unique(
        retained,
        physical,
        pipeline,
        operations,
        operation,
        flow_terminals,
    )
    if range_result is not None:
        return range_result
    if not _native_unique_payload_is_eligible(payload):
        return False, None
    if _BUILTIN_TYPE(retained) not in (_BUILTIN_LIST, _BUILTIN_TUPLE):
        return False, None
    source_values = cast(list[Any] | tuple[Any, ...], retained)
    if _BUILTIN_LEN(source_values) < _NATIVE_UNIQUE_MIN_ROWS:
        return False, None

    endpoints = _load_native_unique_endpoints()
    if endpoints is None:
        return False, None
    _native, baseline_endpoint, cached_endpoint, identity_endpoint = endpoints
    output: list[Any] = []
    with QueryRuntime() as runtime:
        try:
            source_iterator = pipeline.source.open()
        except StopIteration as error:
            raise RuntimeError("generator raised StopIteration") from error
        active_error: BaseException | None = None
        try:
            live_open_operations = cast(Any, flow_terminals.__dict__.get("open_operations"))
            guarded_sample = _guarded_sample_exact_i64(source_values)
            sample = guarded_sample if guarded_sample is not None else _UNIQUE_SAMPLE_REJECT
            endpoint, endpoint_name = _select_unique_endpoint(
                sample,
                baseline_endpoint,
                cached_endpoint,
                identity_endpoint,
            )
            live_endpoint = getattr(_native, endpoint_name, None)
            if (
                has_active_failpoints()
                or Source.__dict__.get("open") is not _CANONICAL_SOURCE_OPEN
                or Source.__dict__.get("retained_sequence") is not _CANONICAL_RETAINED_SEQUENCE
                or live_open_operations is not _CANONICAL_OPEN_OPERATIONS
                or not _canonical_unique_start_intact()
                or guarded_sample is None
                or not _canonical_sampler_intact()
                or _BUILTIN_LEN(pipeline.operations) != 1
                or operation is not pipeline.operations[0]
                or operation.key is not None
                or live_endpoint is not endpoint
                or sample == _UNIQUE_SAMPLE_REJECT
            ):
                with live_open_operations(source_iterator, operations, runtime=runtime) as iterator:
                    output.extend(iterator)
                return True, output

            native = endpoint(output, source_iterator)
            if native is None:
                # A native decline is guaranteed to happen before consuming the exact
                # iterator.  Reuse the source we already opened so free-threaded builds and
                # optional older endpoints do not invoke a reiterable source factory twice.
                with live_open_operations(source_iterator, operations, runtime=runtime) as iterator:
                    output.extend(iterator)
                _record_direct_strategy(
                    physical,
                    "python_direct",
                    "the optional Rust unique endpoint declined before consuming the opened source",
                )
                return True, output
            first_incompatible, completed = native
            if completed:
                _record_direct_strategy(
                    physical,
                    "rust_direct",
                    "retained exact integers were deduplicated by the Rust direct sink",
                )
                return True, output
            _record_direct_strategy(
                physical,
                "rust_python_hybrid",
                "a Rust-deduplicated integer prefix continued through Python",
            )
            remaining = _prepend_unique_boundary(first_incompatible, source_iterator)
            try:
                _append_unique_suffix(output, remaining)
            finally:
                cast(Any, remaining).close()
            return True, output
        except BaseException as error:
            active_error = error
            output.clear()
            raise
        finally:
            close_iterators((source_iterator,), active_error=active_error)
