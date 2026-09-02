"""Bounded grouped aggregation over retained two-dimensional NumPy arrays."""

from __future__ import annotations

from types import BuiltinFunctionType
from typing import Any

from .._provenance import (
    builtin_endpoints_are_live,
    capture_builtin_endpoints,
)
from ..physical.plan import CompiledExpressionPhysicalNode, RowPhysicalNode
from ..physical.relational import (
    GlobalAggregatePhysicalNode,
    GroupAggregatePhysicalNode,
    NumpyGlobalAggregateSpec,
    NumpyGlobalLaneSpec,
    NumpyGroupAggregateSpec,
    NumpyGroupLaneSpec,
    PipelinePhysicalNode,
    SourcePhysicalNode,
)
from ..planning.numpy import (
    _BUILTIN_ABS,
    _BUILTIN_ANY,
    _BUILTIN_ENUMERATE,
    _BUILTIN_GETATTR,
    _BUILTIN_INT,
    _BUILTIN_ISINSTANCE,
    _BUILTIN_LEN,
    _BUILTIN_LIST,
    _BUILTIN_MAX,
    _BUILTIN_MIN,
    _BUILTIN_OBJECT,
    _BUILTIN_RANGE,
    _BUILTIN_TUPLE,
    _BUILTIN_TYPE,
    _BUILTIN_ZIP,
    _CANONICAL_BOOL,
    NumpyConjunctionSpec,
    NumpyFilterSpec,
)
from ..runtime import failpoints as _failpoint_module
from ..tabular.numpy import _validate_retained_numpy_iteration

_BUILTIN_ALL = all
_MISSING = _BUILTIN_OBJECT()
_SIGNED_I64_MAX = 2**63 - 1
_UNSIGNED_I64_MAX = 2**64 - 1
_EXACT_LIMB_BASE = 1 << 32
_FLOAT64_EXACT_INTEGER = 1 << 53
_MAX_EXACT_LIMB_ROWS = _FLOAT64_EXACT_INTEGER // (_EXACT_LIMB_BASE - 1)
_LIMB_GROUP_ROW_RATIO = 8
_DENSE_KEY_MIN_SLOTS = 64
_DENSE_KEY_ROW_RATIO = 16
_MODULE_GLOBALS = globals()
_CANONICAL_BUILTIN_ENDPOINTS_ARE_LIVE = builtin_endpoints_are_live
_NUMPY_GROUP_BUILTINS = capture_builtin_endpoints(
    ("_BUILTIN_ABS", _BUILTIN_ABS, "abs"),
    ("_BUILTIN_ALL", _BUILTIN_ALL, "all"),
    ("_BUILTIN_ANY", _BUILTIN_ANY, "any"),
    ("_BUILTIN_ENUMERATE", _BUILTIN_ENUMERATE, "enumerate"),
    ("_BUILTIN_GETATTR", _BUILTIN_GETATTR, "getattr"),
    ("_BUILTIN_INT", _BUILTIN_INT, "int"),
    ("_BUILTIN_ISINSTANCE", _BUILTIN_ISINSTANCE, "isinstance"),
    ("_BUILTIN_LEN", _BUILTIN_LEN, "len"),
    ("_BUILTIN_LIST", _BUILTIN_LIST, "list"),
    ("_BUILTIN_MAX", _BUILTIN_MAX, "max"),
    ("_BUILTIN_MIN", _BUILTIN_MIN, "min"),
    ("_BUILTIN_OBJECT", _BUILTIN_OBJECT, "object"),
    ("_BUILTIN_RANGE", _BUILTIN_RANGE, "range"),
    ("_BUILTIN_TUPLE", _BUILTIN_TUPLE, "tuple"),
    ("_BUILTIN_TYPE", _BUILTIN_TYPE, "type"),
    ("_BUILTIN_ZIP", _BUILTIN_ZIP, "zip"),
    ("_CANONICAL_BOOL", _CANONICAL_BOOL, "bool"),
)
_CANONICAL_NUMPY_GROUP_BUILTINS = _NUMPY_GROUP_BUILTINS
_CANONICAL_FAILPOINT_MODULE = _failpoint_module
_CANONICAL_HAS_ACTIVE_FAILPOINTS = _failpoint_module._CANONICAL_HAS_ACTIVE_FAILPOINTS
_CANONICAL_HAS_ACTIVE_FAILPOINTS_CODE = _failpoint_module._CANONICAL_HAS_ACTIVE_FAILPOINTS_CODE
_CANONICAL_FAILPOINT_HIT = _failpoint_module._CANONICAL_HIT
_CANONICAL_FAILPOINT_HIT_CODE = _failpoint_module._CANONICAL_HIT_CODE


def _numpy_group_builtins_are_live() -> bool:
    """Reject direct aggregation when an implementation primitive was polluted."""
    return (
        _CANONICAL_BUILTIN_ENDPOINTS_ARE_LIVE(
            _MODULE_GLOBALS,
            "_NUMPY_GROUP_BUILTINS",
            _CANONICAL_NUMPY_GROUP_BUILTINS,
        )
        is True
    )


_CANONICAL_NUMPY_GROUP_BUILTINS_ARE_LIVE = _numpy_group_builtins_are_live
_CANONICAL_NUMPY_GROUP_BUILTINS_ARE_LIVE_CODE = _numpy_group_builtins_are_live.__code__


def _failpoint_boundaries_are_live() -> bool:
    """Require the exact transition hooks before bypassing ``Source.open``."""
    return (
        _MODULE_GLOBALS.get("_failpoint_module") is _CANONICAL_FAILPOINT_MODULE
        and _CANONICAL_FAILPOINT_MODULE.__dict__.get("has_active_failpoints")
        is _CANONICAL_HAS_ACTIVE_FAILPOINTS
        and _CANONICAL_HAS_ACTIVE_FAILPOINTS.__code__ is _CANONICAL_HAS_ACTIVE_FAILPOINTS_CODE
        and _CANONICAL_FAILPOINT_MODULE.__dict__.get("hit") is _CANONICAL_FAILPOINT_HIT
        and _CANONICAL_FAILPOINT_HIT.__code__ is _CANONICAL_FAILPOINT_HIT_CODE
    )


def _native_stateful_endpoint(endpoint: Any, name: str) -> Any | None:
    """Accept only the named Rust builtin, never an injected Python callback."""
    return (
        endpoint
        if _BUILTIN_TYPE(endpoint) is BuiltinFunctionType
        and _BUILTIN_GETATTR(endpoint, "__module__", None) == "fpstreams._native"
        and _BUILTIN_GETATTR(endpoint, "__name__", None) == name
        else None
    )


def _selected_field_indexes(
    columns: tuple[str, ...],
    spec: NumpyGroupAggregateSpec,
) -> tuple[tuple[int, ...], dict[str, int]] | None:
    """Resolve every direct field once while retaining the source's column order."""
    selected = [spec.key_field]
    selected.extend(
        lane.value_field
        for lane in spec.lanes
        if lane.value_field is not None and lane.value_field not in selected
    )
    if spec.prefix is not None:
        for stage in spec.prefix.stages:
            if _BUILTIN_ISINSTANCE(stage, NumpyConjunctionSpec):
                predicates = _BUILTIN_GETATTR(stage, "filters")
            elif _BUILTIN_ISINSTANCE(stage, NumpyFilterSpec):
                predicates = (stage,)
            else:
                predicates = ()
            selected.extend(
                predicate.source_field
                for predicate in predicates
                if predicate.source_field is not None and predicate.source_field not in selected
            )
    if _BUILTIN_ANY(columns.count(field) != 1 for field in selected):
        return None
    indexes = _BUILTIN_TUPLE(columns.index(field) for field in selected)
    return indexes, {field: position for position, field in _BUILTIN_ENUMERATE(selected)}


def _global_selected_field_indexes(
    columns: tuple[str, ...],
    spec: NumpyGlobalAggregateSpec,
) -> tuple[tuple[int, ...], dict[str, int]] | None:
    """Resolve unique direct global value fields once in first-lane order."""
    selected: list[str] = []
    for lane in spec.lanes:
        field = lane.value_field
        if field is not None and field not in selected:
            selected.append(field)
    if _BUILTIN_ANY(columns.count(field) != 1 for field in selected):
        return None
    indexes = _BUILTIN_TUPLE(columns.index(field) for field in selected)
    return indexes, {field: position for position, field in _BUILTIN_ENUMERATE(selected)}


def _exact_numpy_global_sum(
    values: Any,
    minimum: Any,
    maximum: Any,
) -> int:
    """Sum one bounded integer chunk exactly without a wrapping fixed-width reduction."""
    row_count = _BUILTIN_INT(values.shape[0])
    kind = values.dtype.kind
    if kind == "u":
        magnitude_bound = _BUILTIN_INT(maximum) * row_count
        safe = magnitude_bound <= _UNSIGNED_I64_MAX
        dtype = "uint64"
    else:
        magnitude_bound = (
            _BUILTIN_MAX(
                _BUILTIN_ABS(_BUILTIN_INT(minimum)),
                _BUILTIN_ABS(_BUILTIN_INT(maximum)),
            )
            * row_count
        )
        safe = magnitude_bound <= _SIGNED_I64_MAX
        dtype = "int64"
    if safe:
        # The absolute bound proves that every mathematical partial fits the selected
        # accumulator, so this reduction cannot acquire NumPy's wrapping semantics.
        return _BUILTIN_INT(values.sum(dtype=dtype).item())

    unsigned = kind == "u"
    wide = values.astype("uint64" if unsigned else "int64", copy=False)
    low = wide & (_EXACT_LIMB_BASE - 1)
    high = wide >> 32
    low_total = _BUILTIN_INT(low.sum(dtype="uint64").item())
    high_dtype = "uint64" if unsigned else "int64"
    high_total = _BUILTIN_INT(high.sum(dtype=high_dtype).item())
    return low_total + high_total * _EXACT_LIMB_BASE


def _numpy_global_chunk_lanes(
    selected: Any,
    row_count: int,
    spec: NumpyGlobalAggregateSpec,
    field_positions: dict[str, int],
    states: dict[tuple[str, str | None], Any],
    update_sum_f64: Any | None,
    update_mean: Any | None,
) -> dict[tuple[str, str | None], Any]:
    """Compute each distinct global lane once from one stable column chunk."""
    partials: dict[tuple[str, str | None], Any] = {}
    extrema: dict[str, tuple[Any, Any]] = {}
    for lane in spec.lanes:
        cache_key = (lane.kind, lane.value_field)
        if cache_key in partials:
            continue
        if lane.kind == "count":
            partials[cache_key] = row_count
            continue
        value_field = lane.value_field
        assert value_field is not None and selected is not None
        values = selected[field_positions[value_field]]
        if lane.kind == "sum":
            if values.dtype.kind == "f":
                current = states.get(cache_key, _MISSING)
                assert update_sum_f64 is not None
                partials[cache_key] = update_sum_f64(
                    values,
                    0.0 if current is _MISSING else current,
                )
            else:
                bounds = extrema.get(value_field)
                if bounds is None:
                    bounds = (values.min().item(), values.max().item())
                    extrema[value_field] = bounds
                partials[cache_key] = _exact_numpy_global_sum(values, *bounds)
        elif lane.kind == "mean":
            current = states.get(cache_key, _MISSING)
            if current is _MISSING:
                count, total, compensation = 0, 0.0, 0.0
            else:
                count, total, compensation = current
            assert update_mean is not None
            partials[cache_key] = update_mean(
                values,
                count,
                total,
                compensation,
            )
        elif lane.kind == "min":
            bounds = extrema.get(value_field)
            if bounds is None:
                minimum = values.min().item()
                partials[cache_key] = minimum
            else:
                partials[cache_key] = bounds[0]
        else:
            bounds = extrema.get(value_field)
            if bounds is None:
                maximum = values.max().item()
                partials[cache_key] = maximum
            else:
                partials[cache_key] = bounds[1]
    return partials


def _merge_numpy_global_partials(
    states: dict[tuple[str, str | None], Any],
    partials: dict[tuple[str, str | None], Any],
    dtype_kind: str,
) -> None:
    """Merge one exact chunk into global Python scalar states."""
    for cache_key, value in partials.items():
        kind = cache_key[0]
        current = states.get(cache_key, _MISSING)
        if kind == "mean" or (kind == "sum" and dtype_kind == "f"):
            states[cache_key] = value
        elif kind in {"count", "sum"}:
            states[cache_key] = value if current is _MISSING else current + value
        elif kind == "min":
            states[cache_key] = value if current is _MISSING or value < current else current
        else:
            states[cache_key] = value if current is _MISSING or value > current else current


def _materialize_numpy_global_result(
    states: dict[tuple[str, str | None], Any],
    lanes: tuple[NumpyGlobalLaneSpec, ...],
) -> dict[str, Any]:
    """Finish exact identities and preserve requested lane order and duplicates."""
    result: dict[str, Any] = {}
    for lane in lanes:
        cache_key = (lane.kind, lane.value_field)
        value = states.get(cache_key, _MISSING)
        if lane.kind == "mean":
            if value is _MISSING:
                value = None
            else:
                count, total, compensation = value
                value = (total + compensation) / count if count else None
        elif value is _MISSING:
            value = 0 if lane.kind in {"count", "sum"} else None
        result[lane.output_name] = value
    return result


def _exact_numpy_group_sum(
    np: Any,
    values: Any,
    inverse: Any,
    group_count: int,
) -> Any:
    """Reduce one integer chunk exactly, using Python integers only when overflow is possible."""
    row_count = _BUILTIN_INT(values.shape[0])
    kind = values.dtype.kind
    if kind == "u":
        maximum = _BUILTIN_INT(values.max().item())
        magnitude_bound = maximum * row_count
        safe = magnitude_bound <= _UNSIGNED_I64_MAX
        dtype = np.uint64
    else:
        minimum = _BUILTIN_INT(values.min().item())
        maximum = _BUILTIN_INT(values.max().item())
        magnitude_bound = _BUILTIN_MAX(_BUILTIN_ABS(minimum), _BUILTIN_ABS(maximum)) * row_count
        safe = magnitude_bound <= _SIGNED_I64_MAX
        dtype = np.int64
    if (
        magnitude_bound <= _FLOAT64_EXACT_INTEGER
        and (values.dtype.itemsize < 8 or not values.dtype.isnative)
        and group_count > 2
        and group_count * _LIMB_GROUP_ROW_RATIO <= row_count
    ):
        # Every input and every encounter-ordered partial sum is an exactly representable
        # binary64 integer under this absolute bound.  ``bincount`` can therefore use its
        # faster dense weighted reduction without changing Python's unbounded-integer result.
        return np.bincount(inverse, weights=values, minlength=group_count).astype(dtype)
    if safe:
        totals = np.zeros(group_count, dtype=dtype)
        np.add.at(totals, inverse, values)
        return totals

    if group_count * _LIMB_GROUP_ROW_RATIO <= row_count and row_count <= _MAX_EXACT_LIMB_ROWS:
        return _exact_limb_group_sum(np, values, inverse, group_count)

    # A chunk can contain enough full-width values to overflow every fixed-width NumPy
    # accumulator. Keep factorization columnar, but merge its rare unsafe values as Python ints.
    totals = [0] * group_count
    for position, value in _BUILTIN_ZIP(inverse.tolist(), values.tolist(), strict=True):
        totals[position] += value
    return totals


def _exact_limb_group_sum(
    np: Any,
    values: Any,
    inverse: Any,
    group_count: int,
) -> list[int]:
    """Sum fixed-width integers exactly through two float-exact 32-bit limbs."""
    unsigned = values.dtype.kind == "u"
    wide = values.astype(np.uint64 if unsigned else np.int64, copy=False)
    mask = np.asarray(_EXACT_LIMB_BASE - 1, dtype=wide.dtype)
    low = np.bitwise_and(wide, mask)
    high = np.right_shift(wide, 32)
    low_totals = np.bincount(inverse, weights=low, minlength=group_count).tolist()
    high_totals = np.bincount(inverse, weights=high, minlength=group_count).tolist()
    return [
        _BUILTIN_INT(low_value) + _BUILTIN_INT(high_value) * _EXACT_LIMB_BASE
        for low_value, high_value in _BUILTIN_ZIP(low_totals, high_totals, strict=True)
    ]


def _factorize_numpy_group_keys(
    np: Any,
    key_values: Any,
) -> tuple[list[Any], Any, int, Any]:
    """Factor one chunk, using bounded dense integer codes only when measurements justify it."""
    row_count = _BUILTIN_INT(key_values.shape[0])
    minimum = _BUILTIN_INT(key_values.min().item())
    maximum = _BUILTIN_INT(key_values.max().item())
    slot_count = maximum - minimum + 1
    dense_limit = _BUILTIN_MAX(_DENSE_KEY_MIN_SLOTS, row_count // _DENSE_KEY_ROW_RATIO)
    if slot_count <= dense_limit:
        # Converting both operands to uint64 makes subtraction exact even for a small range
        # crossing zero or starting at the signed minimum. The proven slot bound then fits intp.
        base = np.asarray(minimum, dtype=key_values.dtype).astype(np.uint64)
        inverse = (key_values.astype(np.uint64, copy=False) - base).astype(np.intp, copy=False)
        first_indexes = np.full(slot_count, row_count, dtype=np.intp)
        np.minimum.at(
            first_indexes,
            inverse,
            np.arange(row_count, dtype=np.intp),
        )
        present = np.flatnonzero(first_indexes < row_count)
        order = present[np.argsort(first_indexes[present])]
        keys: list[Any]
        if key_values.dtype.kind == "b":
            keys = [_CANONICAL_BOOL(minimum + _BUILTIN_INT(position)) for position in order]
        else:
            keys = [minimum + _BUILTIN_INT(position) for position in order]
        return keys, inverse, slot_count, order

    unique, first_indexes, inverse = np.unique(
        key_values,
        return_index=True,
        return_inverse=True,
    )
    order = np.argsort(first_indexes)
    return unique[order].tolist(), inverse, _BUILTIN_LEN(unique), order


def _closed_numpy_group_domain(
    key_values: Any,
    keys: list[Any],
) -> tuple[int, int] | None:
    """Recognize a compact integer domain only after every slot has been observed."""
    row_count = _BUILTIN_INT(key_values.shape[0])
    dense_limit = _BUILTIN_MAX(_DENSE_KEY_MIN_SLOTS, row_count // _DENSE_KEY_ROW_RATIO)
    if _BUILTIN_LEN(keys) > dense_limit:
        return None
    minimum = _BUILTIN_INT(key_values.min().item())
    maximum = _BUILTIN_INT(key_values.max().item())
    slot_count = maximum - minimum + 1
    if slot_count <= dense_limit and _BUILTIN_LEN(keys) == slot_count:
        return minimum, slot_count
    return None


def _factorize_closed_numpy_group_domain(
    np: Any,
    key_values: Any,
    domain: tuple[int, int],
) -> tuple[list[Any], Any, int, Any, Any] | None:
    """Reuse stable dense codes when a chunk cannot introduce a new group."""
    minimum, slot_count = domain
    row_count = _BUILTIN_INT(key_values.shape[0])
    dense_limit = _BUILTIN_MAX(_DENSE_KEY_MIN_SLOTS, row_count // _DENSE_KEY_ROW_RATIO)
    if slot_count > dense_limit:
        return None
    chunk_minimum = _BUILTIN_INT(key_values.min().item())
    chunk_maximum = _BUILTIN_INT(key_values.max().item())
    if chunk_minimum < minimum or chunk_maximum >= minimum + slot_count:
        return None
    # Both inputs are cast to the same fixed-width integer before modular subtraction.
    # Because the proven domain span fits intp, its non-negative dense code is exact even
    # when signed or unsigned values straddle the intp wrap boundary.
    base = np.asarray(minimum, dtype=key_values.dtype)
    inverse = np.subtract(key_values, base, dtype=np.intp)
    counts = np.bincount(inverse, minlength=slot_count)
    present = np.flatnonzero(counts)
    keys: list[Any]
    if key_values.dtype.kind == "b":
        keys = [_CANONICAL_BOOL(minimum + _BUILTIN_INT(position)) for position in present]
    else:
        keys = [minimum + _BUILTIN_INT(position) for position in present]
    return keys, inverse, slot_count, present, counts


def _numpy_group_extreme(
    np: Any,
    values: Any,
    inverse: Any,
    group_count: int,
    kind: str,
) -> Any:
    """Reduce one integer minimum or maximum lane without sentinel collisions."""
    if values.dtype.kind == "b":
        initial = kind == "min"
    else:
        bounds = np.iinfo(values.dtype)
        initial = bounds.max if kind == "min" else bounds.min
    result = np.full(group_count, initial, dtype=values.dtype)
    operation = np.minimum if kind == "min" else np.maximum
    operation.at(result, inverse, values)
    return result


def _ordered_partial_values(values: Any, order: Any) -> list[Any]:
    """Reorder one NumPy array or exact-Python fallback list by first encounter."""
    if _BUILTIN_TYPE(values) is _BUILTIN_LIST:
        return [values[position] for position in order.tolist()]
    ordered: list[Any] = values[order].tolist()
    return ordered


def _numpy_chunk_lanes(
    np: Any,
    selected: Any,
    inverse: Any,
    group_count: int,
    order: Any,
    spec: NumpyGroupAggregateSpec,
    field_positions: dict[str, int],
    counts: Any | None = None,
) -> list[list[Any]]:
    """Compute every lane from one shared key factorization and reuse duplicate lanes."""
    cache: dict[tuple[str, str | None], Any] = {}
    lanes: list[list[Any]] = []
    for lane in spec.lanes:
        cache_key = (lane.kind, lane.value_field)
        partial = cache.get(cache_key, _MISSING)
        if partial is _MISSING:
            if lane.kind == "count":
                partial = (
                    counts if counts is not None else np.bincount(inverse, minlength=group_count)
                )
            else:
                value_field = lane.value_field
                assert value_field is not None
                values = selected[field_positions[value_field]]
                if lane.kind == "sum":
                    partial = _exact_numpy_group_sum(np, values, inverse, group_count)
                else:
                    partial = _numpy_group_extreme(
                        np,
                        values,
                        inverse,
                        group_count,
                        lane.kind,
                    )
            cache[cache_key] = partial
        lanes.append(_ordered_partial_values(partial, order))
    return lanes


def _merge_numpy_group_chunk(
    positions: dict[Any, int],
    keys: list[Any],
    lane_states: list[list[Any]],
    chunk_keys: list[Any],
    chunk_lanes: list[list[Any]],
    lanes: tuple[NumpyGroupLaneSpec, ...],
) -> None:
    """Merge first-seen chunk groups into insertion-ordered exact Python state."""
    if not positions or positions.keys().isdisjoint(chunk_keys):
        start = _BUILTIN_LEN(keys)
        positions.update(
            _BUILTIN_ZIP(
                chunk_keys,
                _BUILTIN_RANGE(start, start + _BUILTIN_LEN(chunk_keys)),
                strict=True,
            )
        )
        keys.extend(chunk_keys)
        for state, values in _BUILTIN_ZIP(lane_states, chunk_lanes, strict=True):
            state.extend(values)
        return
    for chunk_position, key in _BUILTIN_ENUMERATE(chunk_keys):
        position = positions.get(key)
        if position is None:
            positions[key] = _BUILTIN_LEN(keys)
            keys.append(key)
            for state, values in _BUILTIN_ZIP(lane_states, chunk_lanes, strict=True):
                state.append(values[chunk_position])
            continue
        for lane, state, values in _BUILTIN_ZIP(
            lanes,
            lane_states,
            chunk_lanes,
            strict=True,
        ):
            value = values[chunk_position]
            if lane.kind in {"count", "sum"}:
                state[position] += value
            elif lane.kind == "min":
                if value < state[position]:
                    state[position] = value
            elif value > state[position]:
                state[position] = value


def _materialize_numpy_group_rows(
    keys: list[Any],
    lane_states: list[list[Any]],
    key_name: str,
    lanes: tuple[NumpyGroupLaneSpec, ...],
) -> list[dict[str, Any]]:
    """Build canonical key-first record dictionaries from closed group state."""
    lane_count = _BUILTIN_LEN(lanes)
    if lane_count == 1:
        lane = lanes[0]
        return [
            {key_name: key, lane.output_name: value}
            for key, value in _BUILTIN_ZIP(keys, lane_states[0], strict=True)
        ]
    if lane_count == 2 and _BUILTIN_LEN(lane_states) == 2:
        first_name = lanes[0].output_name
        second_name = lanes[1].output_name
        return [
            {key_name: key, first_name: first, second_name: second}
            for key, first, second in _BUILTIN_ZIP(
                keys,
                lane_states[0],
                lane_states[1],
                strict=True,
            )
        ]
    if lane_count == 3 and _BUILTIN_LEN(lane_states) == 3:
        first_name = lanes[0].output_name
        second_name = lanes[1].output_name
        third_name = lanes[2].output_name
        return [
            {
                key_name: key,
                first_name: first,
                second_name: second,
                third_name: third,
            }
            for key, first, second, third in _BUILTIN_ZIP(
                keys,
                lane_states[0],
                lane_states[1],
                lane_states[2],
                strict=True,
            )
        ]
    if lane_count == 4 and _BUILTIN_LEN(lane_states) == 4:
        first_name = lanes[0].output_name
        second_name = lanes[1].output_name
        third_name = lanes[2].output_name
        fourth_name = lanes[3].output_name
        return [
            {
                key_name: key,
                first_name: first,
                second_name: second,
                third_name: third,
                fourth_name: fourth,
            }
            for key, first, second, third, fourth in _BUILTIN_ZIP(
                keys,
                lane_states[0],
                lane_states[1],
                lane_states[2],
                lane_states[3],
                strict=True,
            )
        ]
    result: list[dict[str, Any]] = []
    append = result.append
    for position, key in _BUILTIN_ENUMERATE(keys):
        row: dict[str, Any] = {key_name: key}
        for lane, state in _BUILTIN_ZIP(lanes, lane_states, strict=True):
            row[lane.output_name] = state[position]
        append(row)
    return result


_NATIVE_GROUP_COUNT = 1 << 0
_NATIVE_GROUP_SUM = 1 << 1
_NATIVE_GROUP_MIN = 1 << 2
_NATIVE_GROUP_MAX = 1 << 3


def _native_numpy_group_endpoints() -> tuple[Any, Any, Any | None, Any, Any] | None:
    """Resolve the optional transactional ABI once for one grouped execution."""
    from .. import _native

    create = getattr(_native, "numpy_group_state_v1", None)
    partial = getattr(_native, "numpy_group_partial_v1", None)
    strided_partial = getattr(_native, "numpy_group_strided_partial_v2", None)
    commit = getattr(_native, "numpy_group_commit_v1", None)
    finalize = getattr(_native, "numpy_group_finalize_v1", None)
    if (
        not callable(create)
        or not callable(partial)
        or not callable(commit)
        or not callable(finalize)
    ):
        return None
    return (
        create,
        partial,
        strided_partial if callable(strided_partial) else None,
        commit,
        finalize,
    )


def _native_numpy_group_layout(
    dtype: Any,
    spec: NumpyGroupAggregateSpec,
    field_positions: dict[str, int],
) -> tuple[int, int, int | None] | None:
    """Recognize the first native ABI's homogeneous integer lane boundary."""
    if spec.prefix is not None:
        return None
    if dtype.kind == "b":
        if _BUILTIN_INT(dtype.itemsize) != 1:
            return None
    elif dtype.kind not in {"i", "u"} or _BUILTIN_INT(dtype.itemsize) != 8 or not dtype.isnative:
        return None

    mask = 0
    value_field: str | None = None
    for lane in spec.lanes:
        if lane.kind == "count":
            mask |= _NATIVE_GROUP_COUNT
            continue
        if value_field is None:
            value_field = lane.value_field
        elif lane.value_field != value_field:
            return None
        if lane.kind == "sum":
            mask |= _NATIVE_GROUP_SUM
        elif lane.kind == "min":
            mask |= _NATIVE_GROUP_MIN
        else:
            mask |= _NATIVE_GROUP_MAX
    return (
        mask,
        field_positions[spec.key_field],
        None if value_field is None else field_positions[value_field],
    )


def _try_native_numpy_group(
    values: Any,
    width: int,
    dtype: Any,
    spec: NumpyGroupAggregateSpec,
    key_name: str,
    field_indexes: tuple[int, ...],
    field_positions: dict[str, int],
    *,
    chunk_rows: int,
) -> list[dict[str, Any]] | None:
    """Reduce stable chunks transactionally through the optional native grouped state."""
    endpoints = _native_numpy_group_endpoints()
    layout = _native_numpy_group_layout(dtype, spec, field_positions)
    if endpoints is None or layout is None:
        return None
    create, prepare_partial, prepare_strided_partial, commit, finalize = endpoints
    mask, key_position, value_position = layout
    state = create(mask)
    offset = 0

    while True:
        row_count = _validate_retained_numpy_iteration(values, width, dtype)
        if offset >= row_count:
            break
        stop = _BUILTIN_MIN(offset + chunk_rows, row_count)
        converted_rows = stop - offset
        while converted_rows:
            if prepare_strided_partial is None:
                selected = values[offset : offset + converted_rows].T[_BUILTIN_LIST(field_indexes)]
                if not selected.flags.c_contiguous:
                    selected = selected.copy(order="C")
                keys = selected[key_position]
                lane_values = None if value_position is None else selected[value_position]
            else:
                keys = values[
                    offset : offset + converted_rows,
                    field_indexes[key_position],
                ]
                lane_values = (
                    None
                    if value_position is None
                    else values[
                        offset : offset + converted_rows,
                        field_indexes[value_position],
                    ]
                )
            if _BUILTIN_INT(keys.shape[0]) != converted_rows:
                raise ValueError("from_numpy() retained array length changed during iteration")
            partial = (
                prepare_partial(keys, lane_values, mask)
                if prepare_strided_partial is None
                else prepare_strided_partial(keys, lane_values, mask)
            )
            if partial is None:
                return None
            live_row_count = _validate_retained_numpy_iteration(values, width, dtype)
            available = _BUILTIN_MIN(
                converted_rows,
                _BUILTIN_MAX(0, live_row_count - offset),
            )
            if available == converted_rows:
                commit(state, partial)
                break
            converted_rows = available
        if not converted_rows:
            if offset < live_row_count:
                raise ValueError("from_numpy() retained array length changed during iteration")
            break
        offset += converted_rows

    keys, counts, sums, minima, maxima = finalize(state)
    values_by_kind = {
        "count": counts,
        "sum": sums,
        "min": minima,
        "max": maxima,
    }
    lane_states = [values_by_kind[lane.kind] for lane in spec.lanes]
    return _materialize_numpy_group_rows(keys, lane_states, key_name, spec.lanes)


def _aggregate_numpy_global_chunk(
    values: Any,
    offset: int,
    stop: int,
    width: int,
    dtype: Any,
    spec: NumpyGlobalAggregateSpec,
    field_indexes: tuple[int, ...],
    field_positions: dict[str, int],
    states: dict[tuple[str, str | None], Any],
    update_sum_f64: Any | None,
    update_mean: Any | None,
) -> tuple[int, dict[tuple[str, str | None], Any]]:
    """Copy selected columns and recompute a chunk if its live source shrinks."""
    selected = None
    if _BUILTIN_LEN(field_indexes) == 1:
        column = values[offset:stop, field_indexes[0]]
        column = column.copy(order="C")
        selected = column.reshape(1, -1)
        converted_rows = _BUILTIN_INT(column.shape[0])
    elif field_indexes:
        selected = values[offset:stop].T[_BUILTIN_LIST(field_indexes)]
        if not selected.flags.c_contiguous:
            selected = selected.copy(order="C")
        converted_rows = _BUILTIN_INT(selected.shape[1])
    else:
        converted_rows = stop - offset
    if converted_rows != stop - offset:
        raise ValueError("from_numpy() retained array length changed during iteration")
    live_row_count = _validate_retained_numpy_iteration(values, width, dtype)
    available = _BUILTIN_MIN(
        converted_rows,
        _BUILTIN_MAX(0, live_row_count - offset),
    )
    if available != converted_rows:
        if selected is not None:
            selected = selected[:, :available]
            if not selected.flags.c_contiguous:
                selected = selected.copy(order="C")
        converted_rows = available
    if not converted_rows:
        if offset < live_row_count:
            raise ValueError("from_numpy() retained array length changed during iteration")
        return 0, {}

    while True:
        partials = _numpy_global_chunk_lanes(
            selected,
            converted_rows,
            spec,
            field_positions,
            states,
            update_sum_f64,
            update_mean,
        )
        live_row_count = _validate_retained_numpy_iteration(values, width, dtype)
        available = _BUILTIN_MIN(
            converted_rows,
            _BUILTIN_MAX(0, live_row_count - offset),
        )
        if available == converted_rows:
            return converted_rows, partials
        if selected is not None:
            selected = selected[:, :available]
            if not selected.flags.c_contiguous:
                selected = selected.copy(order="C")
        converted_rows = available
        if not converted_rows:
            return 0, {}


def _numpy_global_update_endpoints(
    dtype: Any,
    spec: NumpyGlobalAggregateSpec,
) -> tuple[Any | None, Any | None] | None:
    """Resolve every optional stateful endpoint before claiming the source."""
    if not _numpy_global_layout_is_supported(dtype, spec):
        return None
    needs_mean = _BUILTIN_ANY(lane.kind == "mean" for lane in spec.lanes)
    needs_f64_sum = dtype.kind == "f" and _BUILTIN_ANY(lane.kind == "sum" for lane in spec.lanes)
    if not needs_mean and not needs_f64_sum:
        return None, None
    try:
        from .. import _native
    except ImportError:
        return None

    update_sum_f64 = None
    if needs_f64_sum:
        update_sum_f64 = _native_stateful_endpoint(
            _BUILTIN_GETATTR(_native, "update_sum_f64_buffer_v1", None),
            "update_sum_f64_buffer_v1",
        )
        if update_sum_f64 is None:
            return None

    update_mean = None
    if needs_mean:
        if dtype.kind == "f":
            endpoint_name = "update_mean_f64_buffer_v1"
        elif dtype.kind == "i":
            endpoint_name = "update_mean_i64_buffer_v1"
        else:
            return None
        update_mean = _native_stateful_endpoint(
            _BUILTIN_GETATTR(_native, endpoint_name, None),
            endpoint_name,
        )
        if update_mean is None:
            return None
    return update_sum_f64, update_mean


def _numpy_global_layout_is_supported(dtype: Any, spec: NumpyGlobalAggregateSpec) -> bool:
    """Reapply planner dtype/lane constraints to a mutable retained array."""
    kind = _BUILTIN_GETATTR(dtype, "kind", None)
    itemsize = _BUILTIN_GETATTR(dtype, "itemsize", 0)
    native = _BUILTIN_GETATTR(dtype, "isnative", False) is True
    integer_dtype = kind in {"b", "i", "u"} and 1 <= itemsize <= 8
    float64_dtype = kind == "f" and itemsize == 8 and native
    if not (integer_dtype or float64_dtype):
        return False
    for lane in spec.lanes:
        if lane.kind == "count":
            continue
        if lane.kind == "sum":
            if not (integer_dtype or float64_dtype):
                return False
            continue
        if lane.kind == "mean":
            if not (float64_dtype or (kind == "i" and itemsize == 8 and native)):
                return False
            continue
        if lane.kind not in {"min", "max"} or not integer_dtype:
            return False
    return True


def _numpy_global_aggregate(
    source: Any,
    spec: NumpyGlobalAggregateSpec,
    endpoints: tuple[Any | None, Any | None],
    *,
    chunk_rows: int,
) -> dict[str, Any] | None:
    """Reduce direct numeric fields in bounded chunks without constructing source rows."""
    from ..tabular.numpy import _retained_numpy_width, numpy_module

    np = numpy_module("aggregate()")
    values = source.array
    width = _retained_numpy_width(values, source.columns)
    dtype = values.dtype
    if _BUILTIN_TYPE(values) is not np.ndarray or not _numpy_global_layout_is_supported(
        dtype, spec
    ):
        return None
    update_sum_f64, update_mean = endpoints
    if (
        dtype.kind == "f"
        and _BUILTIN_ANY(lane.kind == "sum" for lane in spec.lanes)
        and _native_stateful_endpoint(update_sum_f64, "update_sum_f64_buffer_v1") is None
    ):
        return None
    if _BUILTIN_ANY(lane.kind == "mean" for lane in spec.lanes):
        endpoint_name = (
            "update_mean_f64_buffer_v1" if dtype.kind == "f" else "update_mean_i64_buffer_v1"
        )
        if _native_stateful_endpoint(update_mean, endpoint_name) is None:
            return None
    row_count = _validate_retained_numpy_iteration(values, width, dtype)
    if not row_count:
        return _materialize_numpy_global_result({}, spec.lanes)
    selected_fields = _global_selected_field_indexes(source.columns, spec)
    if selected_fields is None:
        return None
    field_indexes, field_positions = selected_fields

    states: dict[tuple[str, str | None], Any] = {}
    bounded_chunk_rows = _BUILTIN_MIN(
        _BUILTIN_MAX(1, _BUILTIN_INT(chunk_rows)),
        _MAX_EXACT_LIMB_ROWS,
    )
    offset = 0
    while True:
        row_count = _validate_retained_numpy_iteration(values, width, dtype)
        if offset >= row_count:
            break
        stop = _BUILTIN_MIN(offset + bounded_chunk_rows, row_count)
        converted_rows, partials = _aggregate_numpy_global_chunk(
            values,
            offset,
            stop,
            width,
            dtype,
            spec,
            field_indexes,
            field_positions,
            states,
            update_sum_f64,
            update_mean,
        )
        if not converted_rows:
            break
        _merge_numpy_global_partials(states, partials, dtype.kind)
        offset += converted_rows

    return _materialize_numpy_global_result(states, spec.lanes)


def _aggregate_numpy_group_chunk(
    np: Any,
    values: Any,
    selected: Any,
    offset: int,
    width: int,
    dtype: Any,
    spec: NumpyGroupAggregateSpec,
    field_positions: dict[str, int],
    dense_domain: tuple[int, int] | None,
    reuse_dense_domain: bool,
) -> tuple[int, list[Any], list[list[Any]], tuple[int, int] | None]:
    """Apply one optional prefix and recompute a chunk if the live source shrinks."""
    converted_rows = _BUILTIN_INT(selected.shape[1])
    chunk_keys: list[Any] = []
    chunk_lanes: list[list[Any]] = []
    next_dense_domain = dense_domain
    while converted_rows:
        # A shrink forces a complete retry. Never carry a domain inferred from the
        # discarded wider snapshot into that recomputation.
        next_dense_domain = dense_domain
        grouped = selected
        if spec.prefix is not None:
            from .numpy_prefix import _execute_chunk

            active = _execute_chunk(selected, field_positions, spec.prefix)
            if active is not None:
                grouped = selected[:, active]
        if _BUILTIN_INT(grouped.shape[1]):
            key_values = grouped[field_positions[spec.key_field]]
            reused = None
            if reuse_dense_domain and dense_domain is not None:
                reused = _factorize_closed_numpy_group_domain(
                    np,
                    key_values,
                    dense_domain,
                )
            counts = None
            if reused is None:
                chunk_keys, inverse, group_count, order = _factorize_numpy_group_keys(
                    np,
                    key_values,
                )
                if reuse_dense_domain:
                    closed_domain = _closed_numpy_group_domain(key_values, chunk_keys)
                    if closed_domain is not None:
                        next_dense_domain = closed_domain
            else:
                chunk_keys, inverse, group_count, order, counts = reused
            chunk_lanes = _numpy_chunk_lanes(
                np,
                grouped,
                inverse,
                group_count,
                order,
                spec,
                field_positions,
                counts,
            )
        else:
            chunk_keys = []
            chunk_lanes = [[] for _lane in spec.lanes]
        live_row_count = _validate_retained_numpy_iteration(values, width, dtype)
        available = _BUILTIN_MIN(
            converted_rows,
            _BUILTIN_MAX(0, live_row_count - offset),
        )
        if available == converted_rows:
            break
        selected = selected[:, :available]
        converted_rows = available
    return converted_rows, chunk_keys, chunk_lanes, next_dense_domain


def _numpy_group_aggregate_chunks(
    np: Any,
    values: Any,
    width: int,
    dtype: Any,
    spec: NumpyGroupAggregateSpec,
    key_name: str,
    field_indexes: tuple[int, ...],
    field_positions: dict[str, int],
    *,
    chunk_rows: int,
) -> list[dict[str, Any]]:
    """Execute the portable NumPy grouping path over validated selected chunks."""
    positions: dict[Any, int] = {}
    keys: list[Any] = []
    lane_states: list[list[Any]] = [[] for _lane in spec.lanes]
    dense_domain_enabled = spec.prefix is None and _BUILTIN_ANY(
        lane.kind == "count" for lane in spec.lanes
    )
    dense_domain: tuple[int, int] | None = None
    offset = 0
    while True:
        row_count = _validate_retained_numpy_iteration(values, width, dtype)
        if offset >= row_count:
            break
        stop = _BUILTIN_MIN(offset + chunk_rows, row_count)
        selected = values[offset:stop].T[_BUILTIN_LIST(field_indexes)]
        if not selected.flags.c_contiguous:
            selected = selected.copy(order="C")
        converted_rows = _BUILTIN_INT(selected.shape[1])
        if converted_rows != stop - offset:
            raise ValueError("from_numpy() retained array length changed during iteration")
        live_row_count = _validate_retained_numpy_iteration(values, width, dtype)
        available = _BUILTIN_MIN(
            converted_rows,
            _BUILTIN_MAX(0, live_row_count - offset),
        )
        if available != converted_rows:
            selected = selected[:, :available]
            converted_rows = available
        if not converted_rows:
            if offset < live_row_count:
                raise ValueError("from_numpy() retained array length changed during iteration")
            break

        reuse_dense_domain = dense_domain_enabled and (
            dense_domain is not None or stop < live_row_count
        )
        converted_rows, chunk_keys, chunk_lanes, dense_domain = _aggregate_numpy_group_chunk(
            np,
            values,
            selected,
            offset,
            width,
            dtype,
            spec,
            field_positions,
            dense_domain,
            reuse_dense_domain,
        )
        if not converted_rows:
            break
        if chunk_keys:
            _merge_numpy_group_chunk(
                positions,
                keys,
                lane_states,
                chunk_keys,
                chunk_lanes,
                spec.lanes,
            )
        offset += converted_rows

    # The position index is no longer needed while output dictionaries are materialized.
    positions.clear()
    return _materialize_numpy_group_rows(keys, lane_states, key_name, spec.lanes)


def _numpy_group_aggregate(
    source: Any,
    spec: NumpyGroupAggregateSpec,
    key_name: str,
    *,
    chunk_rows: int,
) -> list[dict[str, Any]] | None:
    """Run one O(chunk + groups) grouped scan over a live retained integer matrix."""
    from ..tabular.numpy import (
        _retained_numpy_width,
        numpy_module,
    )

    np = numpy_module("group_by()")
    values = source.array
    width = _retained_numpy_width(values, source.columns)
    if not _BUILTIN_INT(values.shape[0]):
        return []
    dtype = values.dtype
    if _BUILTIN_TYPE(values) is not np.ndarray or dtype.kind not in {"b", "i", "u"}:
        return None
    if not 1 <= _BUILTIN_INT(dtype.itemsize) <= 8:
        return None
    _validate_retained_numpy_iteration(values, width, dtype)
    selected_fields = _selected_field_indexes(source.columns, spec)
    if selected_fields is None:
        return None
    field_indexes, field_positions = selected_fields

    native_rows = _try_native_numpy_group(
        values,
        width,
        dtype,
        spec,
        key_name,
        field_indexes,
        field_positions,
        chunk_rows=chunk_rows,
    )
    if native_rows is not None:
        return native_rows
    return _numpy_group_aggregate_chunks(
        np,
        values,
        width,
        dtype,
        spec,
        key_name,
        field_indexes,
        field_positions,
        chunk_rows=chunk_rows,
    )


def try_numpy_global_aggregate(
    node: GlobalAggregatePhysicalNode,
    *,
    chunk_rows: int,
    aggregations_validated: bool = False,
) -> dict[str, Any] | None:
    """Open and reduce only a canonical replayable direct NumPy row source."""
    if (
        _MODULE_GLOBALS.get("_numpy_group_builtins_are_live")
        is not _CANONICAL_NUMPY_GROUP_BUILTINS_ARE_LIVE
        or _CANONICAL_NUMPY_GROUP_BUILTINS_ARE_LIVE.__code__
        is not _CANONICAL_NUMPY_GROUP_BUILTINS_ARE_LIVE_CODE
        or not _CANONICAL_NUMPY_GROUP_BUILTINS_ARE_LIVE()
    ):
        return None
    from ..tabular.numpy import NumpyRowSource, guarded_numpy_identity_source
    from .relational import _retained_aggregations_are_live

    spec = node.numpy_global
    source_node: SourcePhysicalNode | None = None
    if _BUILTIN_ISINSTANCE(node.input, SourcePhysicalNode):
        source_node = node.input  # type: ignore[assignment]
    if (
        not _BUILTIN_ISINSTANCE(spec, NumpyGlobalAggregateSpec)
        or not _failpoint_boundaries_are_live()
        or _CANONICAL_HAS_ACTIVE_FAILPOINTS()
        or source_node is None
    ):
        return None
    assert spec is not None
    collectors = node.aggregations.collectors.layout.collectors
    if _BUILTIN_LEN(collectors) != _BUILTIN_LEN(spec.lanes) or (
        not aggregations_validated and not _retained_aggregations_are_live(collectors)
    ):
        return None
    descriptor = guarded_numpy_identity_source(source_node.source)
    if descriptor is None:
        return None
    endpoints = _numpy_global_update_endpoints(descriptor.array.dtype, spec)
    if endpoints is None:
        return None
    opened = source_node.source.open_native(NumpyRowSource)
    _CANONICAL_FAILPOINT_HIT("source.open.after")
    return _numpy_global_aggregate(opened, spec, endpoints, chunk_rows=chunk_rows)


def try_numpy_group_aggregate(
    node: GroupAggregatePhysicalNode,
    *,
    chunk_rows: int,
    aggregations_validated: bool = False,
) -> list[dict[str, Any]] | None:
    """Open and aggregate only a canonical replayable direct NumPy group source."""
    if (
        _MODULE_GLOBALS.get("_numpy_group_builtins_are_live")
        is not _CANONICAL_NUMPY_GROUP_BUILTINS_ARE_LIVE
        or _CANONICAL_NUMPY_GROUP_BUILTINS_ARE_LIVE.__code__
        is not _CANONICAL_NUMPY_GROUP_BUILTINS_ARE_LIVE_CODE
        or not _CANONICAL_NUMPY_GROUP_BUILTINS_ARE_LIVE()
    ):
        return None
    from ..tabular.numpy import (
        NumpyRowSource,
        guarded_numpy_identity_source,
    )
    from .numpy_prefix import _numpy_prefix_is_live
    from .relational import (
        _group_hash_replaced,
        _retained_aggregations_are_live,
    )

    spec = node.numpy_group
    input_node = node.input
    direct_source: SourcePhysicalNode | None = None
    if _BUILTIN_ISINSTANCE(input_node, SourcePhysicalNode):
        direct_source = input_node  # type: ignore[assignment]
    pipeline_input: PipelinePhysicalNode | None = None
    if _BUILTIN_ISINSTANCE(input_node, PipelinePhysicalNode):
        pipeline_input = input_node  # type: ignore[assignment]
    source_node: SourcePhysicalNode | None
    if spec is not None and spec.prefix is None and direct_source is not None:
        source_node = direct_source
    elif (
        spec is not None
        and spec.prefix is not None
        and pipeline_input is not None
        and pipeline_input.parallel is None
        and spec.prefix.operation_count == _BUILTIN_LEN(pipeline_input.stages)
        and _BUILTIN_ISINSTANCE(pipeline_input.input, SourcePhysicalNode)
    ):
        source_node = pipeline_input.input  # type: ignore[assignment]
    else:
        source_node = None
    if (
        not _BUILTIN_ISINSTANCE(spec, NumpyGroupAggregateSpec)
        or not _failpoint_boundaries_are_live()
        or _CANONICAL_HAS_ACTIVE_FAILPOINTS()
        or source_node is None
        or _group_hash_replaced()
    ):
        return None
    assert spec is not None
    collectors = node.aggregations.collectors.layout.collectors
    if _BUILTIN_LEN(collectors) != _BUILTIN_LEN(spec.lanes) or (
        not aggregations_validated and not _retained_aggregations_are_live(collectors)
    ):
        return None
    if spec.prefix is not None:
        if pipeline_input is None:
            return None
        if _BUILTIN_ANY(
            not _BUILTIN_ISINSTANCE(
                stage,
                (RowPhysicalNode, CompiledExpressionPhysicalNode),
            )
            for stage in pipeline_input.stages
        ):
            return None
        from ..planning.logical import Pipeline

        operations = [_BUILTIN_GETATTR(stage, "operation") for stage in pipeline_input.stages]
        prefix_pipeline = Pipeline(
            source_node.source,
            _BUILTIN_TUPLE(operations),
            pipeline_input.engine,
            pipeline_input.parallel,
        )
        if not _numpy_prefix_is_live(prefix_pipeline, spec.prefix):
            return None
    descriptor = guarded_numpy_identity_source(source_node.source)
    if descriptor is None:
        return None
    opened = source_node.source.open_native(NumpyRowSource)
    _CANONICAL_FAILPOINT_HIT("source.open.after")
    return _numpy_group_aggregate(
        opened,
        spec,
        node.key_names[0],
        chunk_rows=chunk_rows,
    )
