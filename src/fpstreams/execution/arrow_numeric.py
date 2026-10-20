"""Exact Arrow numeric reductions and borrowed-buffer mean execution."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager
from types import ModuleType
from typing import TYPE_CHECKING, Any, Protocol
from typing import Literal as TypeLiteral

from ..expressions.row_ir import Field
from ..planning.arrow import ArrowPrefixPlan
from ..planning.arrow_source import ArrowBatchSource as _ArrowBatchSource
from ..planning.logical import Pipeline
from ..planning.sync import MapOp

if TYPE_CHECKING:
    from .arrow import ArrowI64Reduction as _ArrowI64Reduction


class ArrowNumericHooks(Protocol):
    """Read the facade's live bindings at each source and callback boundary."""

    ArrowI64Reduction: type[_ArrowI64Reduction]
    ArrowBatchSource: type[_ArrowBatchSource]
    Field: type[Field]
    MapOp: type[MapOp]
    sys: ModuleType
    _ARROW_PYTHON_EXTREME_MAX_ROWS: int
    _ARROW_REDUCTION_MISSING: object
    _EXPECTED_ARROW_ERRORS: tuple[type[Exception], ...]
    _arrow_modules: Callable[[], tuple[Any, Any]]
    _expr_of: Callable[[Any], Any | None]
    arrow_schema_has_primitive_rows: Callable[[Any, Any], bool]
    observe_arrow_batch_rows: Callable[[Any], list[dict[str, Any]] | None]
    closing_iterators: Callable[..., AbstractContextManager[Any]]
    arrow_i64_sum: Callable[..., int]
    arrow_i64_extreme: Callable[..., Any]
    _reduce_arrow_i64_extreme_batches: Callable[..., _ArrowI64Reduction]
    reduce_arrow_i64_batches: Callable[..., _ArrowI64Reduction]
    _guarded_arrow_direct_field: Callable[
        [Pipeline, ArrowPrefixPlan], tuple[_ArrowBatchSource, int, Any, Any] | None
    ]
    _arrow_numeric_buffer: Callable[..., memoryview[int] | memoryview[float]]


def arrow_i64_sum(
    values: Any,
    row_count: int,
    pa: Any,
    pc: Any,
    *,
    bounds: Any | None = None,
    hooks: ArrowNumericHooks,
) -> int:
    """Return an exact Python integer sum for one nonempty int64 Arrow array."""
    if values.null_count:
        raise TypeError("unsupported operand type(s) for +: 'int' and 'NoneType'")
    if bounds is None:
        bounds = pc.min_max(values).as_py()
    maximum_absolute = max(abs(bounds["min"]), abs(bounds["max"]))
    if maximum_absolute * row_count > 2**63 - 1:
        values = pc.cast(values, pa.decimal128(38, 0))
    subtotal = pc.sum(values).as_py()
    return 0 if subtotal is None else int(subtotal)


def arrow_i64_extreme(
    current: Any,
    values: Any,
    kind: TypeLiteral["min", "max"],
    pc: Any,
    *,
    missing: object,
    hooks: ArrowNumericHooks,
) -> Any:
    """Fold one Arrow array while retaining Python's ordered null comparisons."""
    candidates = (
        values.to_pylist()
        if values.null_count or len(values) <= hooks._ARROW_PYTHON_EXTREME_MAX_ROWS
        else (pc.min_max(values).as_py()[kind],)
    )
    for candidate in candidates:
        if (
            current is missing
            or (kind == "min" and candidate < current)
            or (kind == "max" and candidate > current)
        ):
            current = candidate
    return current


def _reduce_arrow_i64_extreme_batches(
    iterator: Iterator[Any],
    field_index: int,
    kind: TypeLiteral["min", "max"],
    pc: Any,
    *,
    capture_source_value_error: bool,
    hooks: ArrowNumericHooks,
) -> _ArrowI64Reduction:
    """Reduce one exact int64 extreme while keeping source failures distinct."""
    seen = False
    current: Any = hooks._ARROW_REDUCTION_MISSING
    while True:
        try:
            batch = next(iterator)
        except StopIteration:
            break
        except ValueError as error:
            if capture_source_value_error:
                value = None if current is hooks._ARROW_REDUCTION_MISSING else current
                return hooks.ArrowI64Reduction(seen, value, error)
            raise
        try:
            hooks.observe_arrow_batch_rows(batch)
        except ValueError as error:
            if capture_source_value_error:
                value = None if current is hooks._ARROW_REDUCTION_MISSING else current
                return hooks.ArrowI64Reduction(seen, value, error)
            raise
        if batch.num_rows:
            seen = True
            current = hooks.arrow_i64_extreme(
                current,
                batch.column(field_index),
                kind,
                pc,
            )
    return hooks.ArrowI64Reduction(
        seen,
        None if current is hooks._ARROW_REDUCTION_MISSING else current,
    )


def reduce_arrow_i64_batches(
    batches: Iterator[Any],
    field_index: int,
    kind: TypeLiteral["sum", "min", "max"],
    pa: Any,
    pc: Any,
    *,
    capture_source_value_error: bool = False,
    hooks: ArrowNumericHooks,
) -> _ArrowI64Reduction:
    """Reduce exact int64 columns without losing empty or ordered-null semantics."""
    seen = False
    try:
        iterator = iter(batches)
    except ValueError as error:
        if capture_source_value_error:
            identity = 0 if kind == "sum" else None
            return hooks.ArrowI64Reduction(False, identity, error)
        raise
    if kind == "sum":
        total = 0
        while True:
            try:
                batch = next(iterator)
            except StopIteration:
                break
            except ValueError as error:
                if capture_source_value_error:
                    return hooks.ArrowI64Reduction(seen, total, error)
                raise
            try:
                hooks.observe_arrow_batch_rows(batch)
            except ValueError as error:
                if capture_source_value_error:
                    return hooks.ArrowI64Reduction(seen, total, error)
                raise
            row_count = int(batch.num_rows)
            if row_count:
                seen = True
                total += hooks.arrow_i64_sum(batch.column(field_index), row_count, pa, pc)
        return hooks.ArrowI64Reduction(seen, total)

    return hooks._reduce_arrow_i64_extreme_batches(
        iterator,
        field_index,
        kind,
        pc,
        capture_source_value_error=capture_source_value_error,
    )


def try_arrow_i64_field_reduction(
    plan: Pipeline,
    prefix: ArrowPrefixPlan,
    kind: TypeLiteral["sum", "min", "max"],
    *,
    hooks: ArrowNumericHooks,
) -> _ArrowI64Reduction | None:
    """Reduce one complete direct-field Arrow plan or decline before unsafe consumption."""
    direct = hooks._guarded_arrow_direct_field(plan, prefix)
    if direct is None:
        return None
    descriptor, field_index, pa, pc = direct
    schema = descriptor.schema_hint
    assert schema is not None
    if not pa.types.is_int64(schema.field(field_index).type):
        return None

    retained = descriptor.materialized_data
    selected_has_nulls = bool(
        descriptor.kind in {"table", "record_batch"}
        and retained is not None
        and retained.column(field_index).null_count
    )
    plan.source.open_native(hooks.ArrowBatchSource)
    try:
        batches = descriptor.open_batches()
    except ValueError as error:
        identity = 0 if kind == "sum" else None
        return hooks.ArrowI64Reduction(False, identity, error)
    with hooks.closing_iterators((batches,)):
        if descriptor.kind == "reader" or selected_has_nulls:
            return hooks.reduce_arrow_i64_batches(
                batches,
                field_index,
                kind,
                pa,
                pc,
                capture_source_value_error=True,
            )
        try:
            return hooks.reduce_arrow_i64_batches(
                batches,
                field_index,
                kind,
                pa,
                pc,
                capture_source_value_error=True,
            )
        except hooks._EXPECTED_ARROW_ERRORS:
            return None


def _guarded_arrow_direct_field(
    plan: Pipeline, prefix: ArrowPrefixPlan, *, hooks: ArrowNumericHooks
) -> tuple[_ArrowBatchSource, int, Any, Any] | None:
    """Resolve one closed direct field without claiming or opening its Arrow source."""
    from ..runtime.failpoints import has_active_failpoints

    if (
        has_active_failpoints()
        or plan.engine != "auto"
        or plan.parallel is not None
        or prefix.operation_count != len(plan.operations)
        or len(prefix.operations) != 1
        or not isinstance(operation := prefix.operations[0], hooks.MapOp)
        or type(root := hooks._expr_of(operation)) is not hooks.Field
        or type(root.name) is not str
        or "." in root.name
    ):
        return None
    descriptor = plan.source.native_data
    if not isinstance(descriptor, hooks.ArrowBatchSource) or descriptor.kind not in {
        "table",
        "record_batch",
        "reader",
    }:
        return None
    if descriptor.kind != "reader" and not plan.source.capabilities.reiterable:
        return None
    schema = descriptor.schema_hint
    if schema is None:
        return None
    pa, pc = hooks._arrow_modules()
    names = tuple(schema.names)
    if names.count(root.name) != 1 or not hooks.arrow_schema_has_primitive_rows(pa, schema):
        return None
    return descriptor, names.index(root.name), pa, pc


def _arrow_numeric_buffer(
    values: Any, format_code: TypeLiteral["q", "d"], *, hooks: ArrowNumericHooks
) -> memoryview[int] | memoryview[float]:
    """Borrow one nonempty primitive Arrow value buffer at its logical array offset."""
    data = values.buffers()[1]
    if data is None:
        raise TypeError("nonempty numeric Arrow array is missing its value buffer")
    width = 8
    sliced = data.slice(int(values.offset) * width, len(values) * width)
    return memoryview(sliced).cast(format_code)


def try_arrow_numeric_field_mean(
    plan: Pipeline, prefix: ArrowPrefixPlan, *, hooks: ArrowNumericHooks
) -> tuple[bool, float | None]:
    """Compute a Python-compatible compensated mean over one direct Arrow field."""
    if hooks.sys.byteorder != "little":
        return False, None
    direct = hooks._guarded_arrow_direct_field(plan, prefix)
    if direct is None:
        return False, None
    descriptor, field_index, pa, _pc = direct
    schema = descriptor.schema_hint
    assert schema is not None
    field_type = schema.field(field_index).type
    if pa.types.is_int64(field_type):
        endpoint_name = "update_mean_i64_buffer_v1"
        format_code: TypeLiteral["q", "d"] = "q"
    elif pa.types.is_float64(field_type):
        endpoint_name = "update_mean_f64_buffer_v1"
        format_code = "d"
    else:
        return False, None

    try:
        from .. import _native
    except ImportError:
        return False, None
    endpoint = getattr(_native, endpoint_name, None)
    if not callable(endpoint):
        return False, None

    plan.source.open_native(hooks.ArrowBatchSource)
    batches = descriptor.open_batches()
    state: tuple[int, float, float] = (0, 0.0, 0.0)
    with hooks.closing_iterators((batches,)):
        for batch in batches:
            hooks.observe_arrow_batch_rows(batch)
            values = batch.column(field_index)
            if values.null_count:
                raise TypeError("statistics require real numeric values")
            if not len(values):
                continue
            view = hooks._arrow_numeric_buffer(values, format_code)
            try:
                state = endpoint(view, *state)
            finally:
                view.release()
    count, total, compensation = state
    return True, None if not count else (total + compensation) / count
