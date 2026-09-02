"""Execute guarded row prefixes over retained two-dimensional NumPy arrays."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from ..errors import _CANONICAL_SELECTION_ERROR
from ..physical.plan import (
    BackendPayload,
    CompiledExpressionPhysicalNode,
    PhysicalPlan,
    RowPhysicalNode,
)
from ..planning.logical import Pipeline
from ..planning.numpy import (
    _BUILTIN_ANY,
    _BUILTIN_DICT,
    _BUILTIN_ENUMERATE,
    _BUILTIN_GETATTR,
    _BUILTIN_ID,
    _BUILTIN_INT,
    _BUILTIN_KEY_ERROR,
    _BUILTIN_LEN,
    _BUILTIN_LIST,
    _BUILTIN_MAX,
    _BUILTIN_MIN,
    _BUILTIN_OBJECT,
    _BUILTIN_RANGE,
    _BUILTIN_RUNTIME_ERROR,
    _BUILTIN_STR,
    _BUILTIN_TUPLE,
    _BUILTIN_TYPE,
    _BUILTIN_VALUE_ERROR,
    _BUILTIN_ZIP,
    NumpyComputedSpec,
    NumpyConjunctionSpec,
    NumpyFilterSpec,
    NumpyPrefixPlan,
    NumpyProjectionSpec,
    NumpyRenameSpec,
    NumpyWithColumnsSpec,
    plan_numpy_prefix,
)
from ..planning.source import Source
from ..tabular.numpy import _validate_retained_numpy_iteration

# Balance vectorized prefix work with the cost of boxing surviving rows.  Larger
# chunks make the temporary Python columns and dictionaries cross allocator cliffs.
_NUMPY_PREFIX_CHUNK_ROWS = 4_096
_MISSING = _BUILTIN_OBJECT()


def _computed_binary(kind: str, left: Any, right: Any) -> Any:
    """Apply one closed computed-column binary operation."""
    match kind:
        case "+":
            return left + right
        case "-":
            return left - right
        case "*":
            return left * right
        case _:
            raise _BUILTIN_RUNTIME_ERROR(f"unsupported NumPy binary operation: {kind}")


def _computed_unary(kind: str, value: Any) -> Any:
    """Apply one closed computed-column unary operation."""
    match kind:
        case "neg":
            return -value
        case "abs":
            return value.__abs__()
        case _:
            raise _BUILTIN_RUNTIME_ERROR(f"unsupported NumPy unary operation: {kind}")


def _comparison(kind: str, left: Any, right: Any) -> Any:
    """Apply one closed NumPy comparison without mutable dispatch state."""
    match kind:
        case "==":
            return left == right
        case "!=":
            return left != right
        case "<":
            return left < right
        case "<=":
            return left <= right
        case ">":
            return left > right
        case ">=":
            return left >= right
        case _:
            raise _BUILTIN_RUNTIME_ERROR(f"unsupported NumPy comparison: {kind}")


def _and_masks(left: Any, right: Any) -> Any:
    return left & right


def _selection_error(field: str) -> Exception:
    """Build the canonical direct-field selection error with its KeyError cause."""
    try:
        raise _BUILTIN_KEY_ERROR(field)
    except _BUILTIN_KEY_ERROR as error:
        failure = _CANONICAL_SELECTION_ERROR(
            f"Could not resolve selector {field!r}; failed at {field!r}"
        )
        failure.__context__ = error
        failure.__cause__ = error
        return failure


def _constant_comparison(
    comparison: str,
    literal: int,
    minimum: int,
    maximum: int,
) -> bool | None:
    """Return an all-row answer for an out-of-domain integer literal when possible."""
    if minimum <= literal <= maximum:
        return None
    if comparison == "==":
        return False
    if comparison == "!=":
        return True
    if comparison == "<":
        return literal > maximum
    if comparison == "<=":
        return literal >= maximum
    if comparison == ">":
        return literal < minimum
    if comparison == ">=":
        return literal <= minimum
    raise _BUILTIN_RUNTIME_ERROR(f"unsupported NumPy comparison: {comparison}")


def _compare_integer_column(column: Any, spec: NumpyFilterSpec) -> Any:
    """Evaluate one exact Python integer comparison without fixed-width coercion surprises."""
    dtype = column.dtype
    if dtype.kind == "b":
        minimum, maximum = 0, 1
    elif dtype.kind == "u":
        minimum, maximum = 0, (1 << (_BUILTIN_INT(dtype.itemsize) * 8)) - 1
    else:
        sign_bit = 1 << (_BUILTIN_INT(dtype.itemsize) * 8 - 1)
        minimum, maximum = -sign_bit, sign_bit - 1
    literal = _BUILTIN_INT(spec.literal)
    constant = _constant_comparison(
        spec.comparison,
        literal,
        minimum,
        maximum,
    )
    if constant is not None:
        comparison = "==" if constant else "!="
        return _comparison(comparison, column, column)
    return _comparison(spec.comparison, column, spec.literal)


def _computed_source_fields(expression: NumpyComputedSpec) -> tuple[str, ...]:
    """Return resolved source dependencies in the expression's encounter order."""
    return _BUILTIN_TUPLE(source for _logical, source in expression.fields if source is not None)


def _stage_source_fields(stage: object) -> tuple[str, ...]:
    """Return every source column needed by one executable prefix stage."""
    if isinstance(stage, NumpyFilterSpec):
        return () if stage.source_field is None else (stage.source_field,)
    if isinstance(stage, NumpyConjunctionSpec):
        return _BUILTIN_TUPLE(
            predicate.source_field
            for predicate in stage.filters
            if predicate.source_field is not None
        )
    if isinstance(stage, NumpyWithColumnsSpec):
        return _BUILTIN_TUPLE(
            source
            for _name, expression in stage.fields
            for source in _computed_source_fields(expression)
        )
    return ()


def _required_source_fields(prefix: NumpyPrefixPlan) -> tuple[str, ...]:
    """Return unique stage and final-output dependencies in first-use order."""
    required: dict[str, None] = {}
    for stage in prefix.stages:
        required.update(_BUILTIN_DICT.fromkeys(_stage_source_fields(stage)))
    for _output, output_source in prefix.output_fields:
        if type(output_source) is _BUILTIN_STR:
            required.setdefault(output_source, None)
        elif isinstance(output_source, NumpyComputedSpec):
            required.update(_BUILTIN_DICT.fromkeys(_computed_source_fields(output_source)))
    return _BUILTIN_TUPLE(required)


def _active_count(active: Any | None, row_count: int) -> int:
    """Return the number of rows reaching the current prefix stage."""
    return row_count if active is None else _BUILTIN_INT(active.sum())


def _execute_conjunction(
    selected: Any,
    positions: dict[str, int],
    stage: NumpyConjunctionSpec,
    active: Any | None,
    row_count: int,
) -> Any | None:
    """Apply primitive leaves left to right and stop before an unreachable right side."""
    for predicate in stage.filters:
        if not _active_count(active, row_count):
            break
        source_field = predicate.source_field
        if source_field is None:
            raise _selection_error(predicate.field)
        matches = _compare_integer_column(selected[positions[source_field]], predicate)
        active = matches if active is None else _and_masks(active, matches)
    return active


def _validate_computed_stage(stage: NumpyWithColumnsSpec) -> None:
    """Raise the first canonical missing dependency in selector encounter order."""
    for _name, expression in stage.fields:
        for logical, source in expression.fields:
            if source is None:
                raise _selection_error(logical)


def _execute_chunk(
    selected: Any,
    positions: dict[str, int],
    prefix: NumpyPrefixPlan,
) -> Any | None:
    """Apply ordered total stages and return the surviving-row mask, if filtering."""
    row_count = _BUILTIN_INT(selected.shape[1])
    active = None
    for stage in prefix.stages:
        if not _active_count(active, row_count):
            break
        if isinstance(stage, NumpyProjectionSpec):
            for field in stage.fields:
                if field.source_field is None:
                    raise _selection_error(field.field)
            continue
        if isinstance(stage, NumpyRenameSpec):
            continue
        if isinstance(stage, NumpyWithColumnsSpec):
            _validate_computed_stage(stage)
            continue
        if isinstance(stage, NumpyConjunctionSpec):
            active = _execute_conjunction(
                selected,
                positions,
                stage,
                active,
                row_count,
            )
            continue
        source_field = stage.source_field
        if source_field is None:
            raise _selection_error(stage.field)
        matches = _compare_integer_column(selected[positions[source_field]], stage)
        active = matches if active is None else _and_masks(active, matches)
    return active


def _evaluate_computed_program(
    program: tuple[tuple[str, object], ...],
    field_value: Callable[[str], Any],
) -> Any:
    """Evaluate one immutable postfix program in Python operand order."""
    values: list[Any] = []
    for kind, argument in program:
        match kind:
            case "field":
                values.append(field_value(argument))  # type: ignore[arg-type]
            case "literal":
                values.append(argument)
            case "+" | "-" | "*":
                right = values.pop()
                left = values.pop()
                values.append(_computed_binary(kind, left, right))
            case "neg" | "abs":
                values.append(_computed_unary(kind, values.pop()))
            case _:  # pragma: no cover - planner owns this vocabulary
                raise _BUILTIN_RUNTIME_ERROR(f"unsupported NumPy computed operation: {kind}")
    if _BUILTIN_LEN(values) != 1:  # pragma: no cover - planner owns stack shape
        raise _BUILTIN_RUNTIME_ERROR("invalid NumPy computed program")
    return values[0]


def _evaluate_computed_column(
    selected: Any,
    expression: NumpyComputedSpec,
    active: Any | None,
    object_column: Callable[[str], Any],
) -> list[Any]:
    """Evaluate one closed integer expression with Python-object arithmetic semantics."""
    bindings = _BUILTIN_DICT(expression.fields)
    array_type = type(selected)

    def field_value(name: str) -> Any:
        source = bindings[name]
        if source is None:
            raise _selection_error(name)
        return object_column(source)

    def evaluate() -> Any:
        return _evaluate_computed_program(expression.program, field_value)

    result = evaluate()
    if type(result) is array_type:
        converted: list[Any] = result.tolist()
        return converted
    row_count = _BUILTIN_INT(selected.shape[1]) if active is None else _BUILTIN_INT(active.sum())
    if _BUILTIN_LEN(expression.program) == 1 and expression.program[0][0] == "literal":
        return [result] * row_count
    values = [result]
    for _index in _BUILTIN_RANGE(1, row_count):
        values.append(evaluate())
    return values


def _materialized_columns(
    selected: Any,
    positions: dict[str, int],
    prefix: NumpyPrefixPlan,
    active: Any | None,
) -> tuple[tuple[str, ...], tuple[list[Any], ...]]:
    """Box each unique final source column once and rebuild aliases by identity."""
    names = _BUILTIN_TUPLE(name for name, _field in prefix.output_fields)
    cache: dict[str, list[Any]] = {}
    object_columns: dict[str, Any] = {}
    computed_cache: dict[int, list[Any]] = {}
    columns: list[list[Any]] = []
    computed_sources = {
        source
        for _name, field in prefix.output_fields
        if isinstance(field, NumpyComputedSpec)
        for _logical, source in field.fields
        if source is not None
    }

    def object_column(source: str) -> Any:
        values = object_columns.get(source, _MISSING)
        if values is _MISSING:
            column = selected[positions[source]]
            if active is not None:
                column = column[active]
            values = column.astype(_BUILTIN_OBJECT)
            object_columns[source] = values
        return values

    def source_values(source: str) -> list[Any]:
        values = cache.get(source)
        if values is None:
            if source in computed_sources:
                values = object_column(source).tolist()
            else:
                column = selected[positions[source]]
                values = (column if active is None else column[active]).tolist()
            cache[source] = values
        return values

    for _name, field in prefix.output_fields:
        if field is None:
            # Missing projections are normally raised by their ordered stage. This fallback
            # owns malformed plans without silently manufacturing a value column.
            raise _BUILTIN_RUNTIME_ERROR("unresolved NumPy prefix output field")
        if isinstance(field, NumpyComputedSpec):
            bare_field = (
                field.program[0][1]
                if _BUILTIN_LEN(field.program) == 1 and field.program[0][0] == "field"
                else None
            )
            bare_source = (
                _BUILTIN_DICT(field.fields).get(bare_field)
                if _BUILTIN_TYPE(bare_field) is _BUILTIN_STR
                else None
            )
            if bare_source is not None:
                values = source_values(bare_source)
            else:
                computed_values = computed_cache.get(_BUILTIN_ID(field))
                if computed_values is None:
                    computed_values = _evaluate_computed_column(
                        selected,
                        field,
                        active,
                        object_column,
                    )
                    computed_cache[_BUILTIN_ID(field)] = computed_values
                values = computed_values
        else:
            values = source_values(field)
        columns.append(values)
    return names, _BUILTIN_TUPLE(columns)


def _records_from_columns(
    names: tuple[str, ...],
    columns: tuple[list[Any], ...],
) -> list[dict[str, Any]]:
    """Build common narrow dictionaries without a per-row selector or source record."""
    match names:
        case (first,):
            return [{first: value} for value in columns[0]]
        case (first, second):
            return [
                {first: left, second: right}
                for left, right in _BUILTIN_ZIP(columns[0], columns[1], strict=True)
            ]
        case (first, second, third):
            return [
                {first: first_value, second: second_value, third: third_value}
                for first_value, second_value, third_value in _BUILTIN_ZIP(*columns, strict=True)
            ]
        case (first, second, third, fourth):
            return [
                {
                    first: first_value,
                    second: second_value,
                    third: third_value,
                    fourth: fourth_value,
                }
                for first_value, second_value, third_value, fourth_value in _BUILTIN_ZIP(
                    *columns,
                    strict=True,
                )
            ]
        case _:
            return [
                _BUILTIN_DICT(_BUILTIN_ZIP(names, row, strict=True))
                for row in _BUILTIN_ZIP(*columns, strict=True)
            ]


def _trimmed_survivors(active: Any | None, available: int) -> int:
    """Return surviving rows contained in a possibly shrunken input prefix."""
    return available if active is None else _BUILTIN_INT(active[:available].sum())


def _numpy_prefix_batches(
    source: Any,
    prefix: NumpyPrefixPlan,
    *,
    chunk_rows: int,
    expected_dtype: Any | None = None,
) -> Any:
    """Yield bounded boxed output columns together with their scanned input row count."""
    from ..tabular.numpy import _retained_numpy_width

    values = source.array
    width = _retained_numpy_width(values, source.columns)
    dtype = values.dtype if expected_dtype is None else expected_dtype
    _validate_retained_numpy_iteration(values, width, dtype)
    required = _required_source_fields(prefix)
    indexes = _BUILTIN_TUPLE(source.columns.index(field) for field in required)
    positions = {field: position for position, field in _BUILTIN_ENUMERATE(required)}
    offset = 0
    while True:
        row_count = _validate_retained_numpy_iteration(values, width, dtype)
        if offset >= row_count:
            break
        stop = _BUILTIN_MIN(row_count, offset + chunk_rows)
        selected = (
            values[offset:stop].T[_BUILTIN_LIST(indexes)].copy(order="C")
            if indexes
            else values[offset:stop].T[:0].copy(order="C")
        )
        scanned_rows = _BUILTIN_INT(selected.shape[1])
        live_rows = _validate_retained_numpy_iteration(values, width, dtype)
        available = _BUILTIN_MIN(
            scanned_rows,
            _BUILTIN_MAX(0, live_rows - offset),
        )
        if available != scanned_rows:
            selected = selected[:, :available]
            scanned_rows = available
        if not scanned_rows:
            if offset < live_rows:
                raise _BUILTIN_VALUE_ERROR(
                    "from_numpy() retained array row count changed during iteration"
                )
            break

        active = _execute_chunk(selected, positions, prefix)
        live_rows = _validate_retained_numpy_iteration(values, width, dtype)
        available = _BUILTIN_MIN(
            scanned_rows,
            _BUILTIN_MAX(0, live_rows - offset),
        )
        if available != scanned_rows:
            selected = selected[:, :available]
            if active is not None:
                active = active[:available]
            scanned_rows = available
        if not scanned_rows:
            break
        if not _trimmed_survivors(active, scanned_rows):
            names = _BUILTIN_TUPLE(name for name, _field in prefix.output_fields)
            columns: tuple[list[Any], ...] = _BUILTIN_TUPLE([] for _field in prefix.output_fields)
        else:
            names, columns = _materialized_columns(selected, positions, prefix, active)

        live_rows = _validate_retained_numpy_iteration(values, width, dtype)
        available = _BUILTIN_MIN(
            scanned_rows,
            _BUILTIN_MAX(0, live_rows - offset),
        )
        survivors = _trimmed_survivors(active, available)
        if columns and survivors != _BUILTIN_LEN(columns[0]):
            columns = _BUILTIN_TUPLE(column[:survivors] for column in columns)
        if survivors:
            yield names, columns
        offset += available


def numpy_prefix_rows(
    source: Any,
    prefix: NumpyPrefixPlan,
    *,
    chunk_rows: int = _NUMPY_PREFIX_CHUNK_ROWS,
    expected_dtype: Any | None = None,
    record_assembler: Callable[..., list[dict[str, Any]] | None] | None = None,
) -> list[dict[str, Any]]:
    """Materialize one complete NumPy prefix directly into output dictionaries."""
    output: list[dict[str, Any]] = []
    for names, columns in _numpy_prefix_batches(
        source,
        prefix,
        chunk_rows=chunk_rows,
        expected_dtype=expected_dtype,
    ):
        records = record_assembler(names, columns) if record_assembler is not None else None
        output.extend(_records_from_columns(names, columns) if records is None else records)
    return output


def numpy_prefix_columns(
    source: Any,
    prefix: NumpyPrefixPlan,
    *,
    chunk_rows: int = _NUMPY_PREFIX_CHUNK_ROWS,
    expected_dtype: Any | None = None,
) -> dict[str, list[Any]]:
    """Materialize one complete NumPy prefix directly into aligned output columns."""
    output: dict[str, list[Any]] = {}
    for names, columns in _numpy_prefix_batches(
        source,
        prefix,
        chunk_rows=chunk_rows,
        expected_dtype=expected_dtype,
    ):
        if not output:
            output = {
                name: values.copy() for name, values in _BUILTIN_ZIP(names, columns, strict=True)
            }
            continue
        for name, values in _BUILTIN_ZIP(names, columns, strict=True):
            output[name].extend(values)
    return output


def _guarded_numpy_prefix_source(
    pipeline: Pipeline,
    prefix: NumpyPrefixPlan | None,
) -> tuple[Any, Any] | None:
    """Return an unopened exact retained source only when every runtime guard holds."""
    if (
        prefix is None
        or prefix.operation_count != _BUILTIN_LEN(pipeline.operations)
        or pipeline.engine != "auto"
        or pipeline.parallel is not None
        or type(pipeline.source) is not Source
        or not _numpy_prefix_is_live(pipeline, prefix)
    ):
        return None
    from ..tabular.numpy import guarded_numpy_identity_source

    descriptor = guarded_numpy_identity_source(pipeline.source)
    if descriptor is None:
        return None
    dtype = descriptor.array.dtype
    if _BUILTIN_ANY(
        isinstance(
            stage,
            (NumpyFilterSpec, NumpyConjunctionSpec, NumpyWithColumnsSpec),
        )
        for stage in prefix.stages
    ) and (
        _BUILTIN_GETATTR(dtype, "kind", None) not in {"b", "i", "u"}
        or not 1 <= _BUILTIN_GETATTR(dtype, "itemsize", 0) <= 8
    ):
        return None
    return descriptor, dtype


def _numpy_prefix_is_live(pipeline: Pipeline, prefix: NumpyPrefixPlan) -> bool:
    """Revalidate generated callables and dynamic globals at the execution boundary."""
    live = plan_numpy_prefix(pipeline)
    return live == prefix


def try_numpy_prefix_list(
    owner: Any,
    physical: PhysicalPlan,
    pipeline: Pipeline | None,
) -> tuple[bool, list[Any] | None]:
    """Collect a planned NumPy row prefix before opening the Python row executor."""
    from ..streams.flow import Flow

    payload = physical.backend_payload
    if (
        type(owner) is not Flow
        or physical.root is not None
        or physical.parallel is not None
        or pipeline is None
        or not isinstance(payload, BackendPayload)
    ):
        return False, None
    prefix = payload.numpy_prefix
    if prefix is None:
        return False, None
    if _BUILTIN_LEN(physical.nodes) != prefix.operation_count or _BUILTIN_ANY(
        not isinstance(node, (CompiledExpressionPhysicalNode, RowPhysicalNode))
        for node in physical.nodes
    ):
        return False, None
    guarded = _guarded_numpy_prefix_source(pipeline, prefix)
    if guarded is None:
        return False, None
    from ..runtime.failpoints import hit
    from ..runtime.report import _record_direct_strategy
    from ..tabular.numpy import (
        NumpyRowSource,
        _exact_native_record_assembler,
    )

    _descriptor, expected_dtype = guarded
    opened = pipeline.source.open_native(NumpyRowSource)
    hit("source.open.after")
    result = numpy_prefix_rows(
        opened,
        prefix,
        expected_dtype=expected_dtype,
        record_assembler=_exact_native_record_assembler(),
    )
    _record_direct_strategy(
        physical,
        "numpy_direct",
        "bounded NumPy columns executed a complete row prefix without source row boxing",
    )
    return True, result


def try_numpy_prefix_columns(
    pipeline: Pipeline,
) -> tuple[bool, dict[str, list[Any]] | None]:
    """Transpose a safe complete NumPy row prefix without constructing dictionaries."""
    prefix = plan_numpy_prefix(pipeline)
    guarded = _guarded_numpy_prefix_source(pipeline, prefix)
    if guarded is None or prefix is None:
        return False, None
    from ..runtime.failpoints import hit
    from ..tabular.numpy import NumpyRowSource

    _descriptor, expected_dtype = guarded
    opened = pipeline.source.open_native(NumpyRowSource)
    hit("source.open.after")
    return True, numpy_prefix_columns(opened, prefix, expected_dtype=expected_dtype)
