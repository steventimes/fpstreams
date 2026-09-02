"""Plan conservative columnar prefixes over retained two-dimensional NumPy arrays."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal, TypeAlias

from ..expressions.row import RowExpr
from ..expressions.row_eval import LazyRowEvaluator, RowProgram
from ..expressions.row_ir import Binary, Field, Unary
from ..expressions.row_ir import Literal as RowLiteral
from .arrow import _row_stage_descriptor
from .logical import Pipeline
from .sync import FilterOp, MapOp

_CANONICAL_BOOL = True.__class__
_BUILTIN_ABS = abs
_BUILTIN_ANY = any
_BUILTIN_DICT = dict
_BUILTIN_ENUMERATE = enumerate
_BUILTIN_GETATTR = getattr
_BUILTIN_ID = id
_BUILTIN_INT = int
_BUILTIN_ISINSTANCE = isinstance
_BUILTIN_KEY_ERROR = KeyError
_BUILTIN_LEN = len
_BUILTIN_LIST = list
_BUILTIN_MAX = max
_BUILTIN_MIN = min
_BUILTIN_OBJECT = object
_BUILTIN_RANGE = range
_BUILTIN_RUNTIME_ERROR = RuntimeError
_BUILTIN_STR = str
_BUILTIN_TUPLE: type[tuple[Any, ...]] = tuple
_BUILTIN_TYPE = type
_BUILTIN_VALUE_ERROR = ValueError
_BUILTIN_ZIP = zip
_MAX_COMPUTED_OCCURRENCES = 512

NumpyComparison: TypeAlias = Literal["==", "!=", "<", "<=", ">", ">="]


@dataclass(frozen=True, slots=True)
class NumpyFilterSpec:
    """One exact field/literal comparison resolved against the current row schema."""

    field: str
    source_field: str | None
    comparison: NumpyComparison
    literal: int | bool


@dataclass(frozen=True, slots=True)
class NumpyConjunctionSpec:
    """One left-to-right short-circuit AND tree of primitive comparisons."""

    filters: tuple[NumpyFilterSpec, ...]


@dataclass(frozen=True, slots=True)
class NumpyProjectionField:
    """One output field and the retained source column that supplies it."""

    output: str
    field: str
    source_field: NumpyColumnSource


@dataclass(frozen=True, slots=True)
class NumpyProjectionSpec:
    """One direct top-level projection in declaration order."""

    fields: tuple[NumpyProjectionField, ...]


@dataclass(frozen=True, slots=True)
class NumpyRenameSpec:
    """One collision-free direct rename resolved against the current schema."""

    fields: tuple[tuple[str, NumpyColumnSource], ...]


@dataclass(frozen=True, slots=True)
class NumpyComputedSpec:
    """One pure exact-integer expression and its logical-to-source field bindings."""

    program: tuple[tuple[str, object], ...]
    fields: tuple[tuple[str, str | None], ...]


NumpyColumnSource: TypeAlias = str | NumpyComputedSpec | None


@dataclass(frozen=True, slots=True)
class NumpyWithColumnsSpec:
    """Computed columns whose expressions all read the same original input row."""

    fields: tuple[tuple[str, NumpyComputedSpec], ...]


NumpyStageSpec: TypeAlias = (
    NumpyFilterSpec
    | NumpyConjunctionSpec
    | NumpyProjectionSpec
    | NumpyRenameSpec
    | NumpyWithColumnsSpec
)


@dataclass(frozen=True, slots=True)
class NumpyPrefixPlan:
    """A complete guarded NumPy row prefix and its final symbolic schema."""

    operation_count: int
    stages: tuple[NumpyStageSpec, ...]
    output_fields: tuple[tuple[str, NumpyColumnSource], ...]
    guarded: bool = True


def _reversed_comparison(kind: str) -> NumpyComparison | None:
    """Return the closed reversed comparison without a mutable dispatch table."""
    match kind:
        case "==" | "!=":
            return kind
        case "<":
            return ">"
        case "<=":
            return ">="
        case ">":
            return "<"
        case ">=":
            return "<="
        case _:
            return None


def _live_generated_spec(
    function: object,
    factory_name: str,
) -> tuple[tuple[str, object], ...] | None:
    """Revalidate one generated Rows callable through its live inspector."""
    from importlib import import_module

    rows_module = import_module("fpstreams.tabular.rows")
    factory = getattr(rows_module, factory_name, None)
    if not callable(factory):
        return None
    inspect: Callable[[object], tuple[tuple[str, object], ...] | None] = factory
    return inspect(function)


def _primitive_comparison(
    root: object,
    current_fields: dict[str, NumpyColumnSource],
) -> NumpyFilterSpec | None:
    """Lower one total field/literal comparison without evaluating either operand."""
    if not isinstance(root, Binary):
        return None
    match root.kind:
        case "==" | "!=" | "<" | "<=" | ">" | ">=":
            direct_comparison: NumpyComparison = root.kind
        case _:
            return None
    if isinstance(root.left, Field) and isinstance(root.right, RowLiteral):
        field = root.left.name
        literal = root.right.value
        comparison = direct_comparison
    elif isinstance(root.left, RowLiteral) and isinstance(root.right, Field):
        field = root.right.name
        literal = root.left.value
        reversed_comparison = _reversed_comparison(root.kind)
        if reversed_comparison is None:  # pragma: no cover - match validated above
            return None
        comparison = reversed_comparison
    else:
        return None
    if (
        type(field) is not _BUILTIN_STR
        or "." in field
        or type(literal) not in {_BUILTIN_INT, _CANONICAL_BOOL}
    ):
        return None
    source_field = current_fields.get(field)
    if source_field is not None and type(source_field) is not _BUILTIN_STR:
        return None
    return NumpyFilterSpec(
        field,
        source_field,
        comparison,
        literal,
    )


def _primitive_filter(
    operation: FilterOp,
    current_fields: dict[str, NumpyColumnSource],
) -> NumpyFilterSpec | NumpyConjunctionSpec | None:
    """Return one primitive comparison or a closed short-circuit conjunction."""
    if operation.negate or type(operation.predicate) is not RowExpr:
        return None
    root = operation.predicate._node
    if not isinstance(root, Binary) or root.kind != "and":
        return _primitive_comparison(root, current_fields)
    filters: list[NumpyFilterSpec] = []
    stack = [root]
    while stack:
        node = stack.pop()
        if isinstance(node, Binary) and node.kind == "and":
            stack.extend((node.right, node.left))
            continue
        primitive = _primitive_comparison(node, current_fields)
        if primitive is None:
            return None
        filters.append(primitive)
    return NumpyConjunctionSpec(_BUILTIN_TUPLE(filters))


def _direct_projection(
    operation: MapOp,
    current_fields: dict[str, NumpyColumnSource],
) -> NumpyProjectionSpec | None:
    """Return one trusted Rows.select projection over top-level string fields."""
    descriptor = _row_stage_descriptor(operation.function)
    if descriptor is None or descriptor.kind != "select":
        return None
    fields: list[NumpyProjectionField] = []
    for output, field in descriptor.selectors:
        if type(output) is not _BUILTIN_STR or type(field) is not _BUILTIN_STR or "." in field:
            return None
        fields.append(NumpyProjectionField(output, field, current_fields.get(field)))
    expected = _BUILTIN_TUPLE((field.output, field.field) for field in fields)
    if (
        _live_generated_spec(
            operation.function,
            "_materialized_select_spec",
        )
        != expected
    ):
        return None
    return NumpyProjectionSpec(_BUILTIN_TUPLE(fields)) if fields else None


def _direct_rename(
    operation: MapOp,
    current_fields: dict[str, NumpyColumnSource],
) -> NumpyRenameSpec | None:
    """Return one collision-free canonical Rows.rename schema transformation."""
    descriptor = _row_stage_descriptor(operation.function)
    if descriptor is None or descriptor.kind != "rename":
        return None
    renames: dict[str, str] = {}
    for source, target in descriptor.selectors:
        if type(source) is not _BUILTIN_STR or type(target) is not _BUILTIN_STR or not target:
            return None
        renames[source] = target
    live = _live_generated_spec(
        operation.function,
        "_materialized_rename_spec",
    )
    if (
        live is None
        or _BUILTIN_LEN(live) != _BUILTIN_LEN(renames)
        or _BUILTIN_DICT(live) != renames
    ):
        return None
    renamed: dict[str, NumpyColumnSource] = {}
    for name, source_field in current_fields.items():
        target = renames.get(name, name)
        if target in renamed:
            return None
        renamed[target] = source_field
    return NumpyRenameSpec(_BUILTIN_TUPLE(renamed.items())) if renamed else None


def _primitive_computed_expression(  # noqa: C901 - closed manifest compiler
    program: RowProgram,
    current_fields: dict[str, NumpyColumnSource],
) -> NumpyComputedSpec | None:
    """Recognize exact integer arithmetic without callbacks or fixed-width overflow."""
    manifest = program.graph_manifest
    if _BUILTIN_TYPE(manifest) is not _BUILTIN_TUPLE or _BUILTIN_LEN(manifest) != 2:
        return None
    manifest_nodes, entries = manifest
    if (
        _BUILTIN_TYPE(manifest_nodes) is not _BUILTIN_TUPLE
        or not manifest_nodes
        or _BUILTIN_TYPE(entries) is not _BUILTIN_TUPLE
    ):
        return None
    views: dict[int, tuple[type[object], dict[str, object]]] = {}
    for entry in entries:
        if _BUILTIN_TYPE(entry) is not _BUILTIN_TUPLE or _BUILTIN_LEN(entry) != 3:
            return None
        node, node_type, attributes = entry
        if (
            _BUILTIN_TYPE(node_type) is not _BUILTIN_TYPE
            or _BUILTIN_TYPE(attributes) is not _BUILTIN_TUPLE
        ):
            return None
        views[_BUILTIN_ID(node)] = (node_type, _BUILTIN_DICT(attributes))

    dependencies: dict[str, str | None] = {}
    instructions: list[tuple[str, object]] = []
    occurrences = 0
    stack: list[tuple[object, bool]] = [(manifest_nodes[0], False)]
    while stack:
        node, visited = stack.pop()
        view = views.get(_BUILTIN_ID(node))
        if view is None:
            return None
        node_type, attributes = view
        if visited:
            if node_type is Field:
                instructions.append(("field", attributes["name"]))
            elif node_type is RowLiteral:
                instructions.append(("literal", attributes["value"]))
            elif node_type is Binary or node_type is Unary:
                instructions.append((attributes["kind"], None))  # type: ignore[arg-type]
            else:
                return None
            continue
        occurrences += 1
        if occurrences > _MAX_COMPUTED_OCCURRENCES:
            return None
        stack.append((node, True))
        if node_type is Field:
            field = attributes.get("name")
            if type(field) is not _BUILTIN_STR or "." in field:
                return None
            source = current_fields.get(field)
            if source is not None and type(source) is not _BUILTIN_STR:
                return None
            dependencies.setdefault(field, source)
            continue
        if node_type is RowLiteral:
            if type(attributes.get("value")) not in {_BUILTIN_INT, _CANONICAL_BOOL}:
                return None
            continue
        if node_type is Binary and attributes.get("kind") in {"+", "-", "*"}:
            stack.extend(
                (
                    (attributes["right"], False),
                    (attributes["left"], False),
                )
            )
            continue
        if node_type is Unary and attributes.get("kind") in {"neg", "abs"}:
            stack.append((attributes["operand"], False))
            continue
        return None
    return NumpyComputedSpec(
        _BUILTIN_TUPLE(instructions),
        _BUILTIN_TUPLE(dependencies.items()),
    )


def _direct_with_columns(
    operation: MapOp,
    current_fields: dict[str, NumpyColumnSource],
) -> NumpyWithColumnsSpec | None:
    """Return a live, pure computed-column stage over exact integer expressions."""
    descriptor = _row_stage_descriptor(operation.function)
    if descriptor is None or descriptor.kind != "with_columns" or not descriptor.selectors:
        return None
    live = _live_generated_spec(
        operation.function,
        "_materialized_with_columns_spec",
    )
    if live is None or _BUILTIN_LEN(live) != _BUILTIN_LEN(descriptor.selectors):
        return None
    fields: list[tuple[str, NumpyComputedSpec]] = []
    for (name, selector), (live_name, live_selector) in _BUILTIN_ZIP(
        descriptor.selectors,
        live,
        strict=True,
    ):
        if (
            type(name) is not _BUILTIN_STR
            or live_name != name
            or live_selector is not selector
            or type(selector) is not RowExpr
        ):
            return None
        evaluator = selector._evaluate
        if type(evaluator) is not LazyRowEvaluator:
            return None
        program = evaluator._program
        if type(program) is not RowProgram:
            return None
        computed = _primitive_computed_expression(program, current_fields)
        if computed is None:
            return None
        fields.append((name, computed))
    return NumpyWithColumnsSpec(_BUILTIN_TUPLE(fields))


def _supports_integer_kernels(array: object) -> bool:
    """Return whether one homogeneous matrix has exact built-in integer scalar values."""
    dtype = _BUILTIN_GETATTR(array, "dtype", None)
    return (
        _BUILTIN_GETATTR(dtype, "kind", None) in {"b", "i", "u"}
        and 1
        <= _BUILTIN_GETATTR(
            dtype,
            "itemsize",
            0,
        )
        <= 8
    )


def _finish_numpy_prefix(
    stages: list[NumpyStageSpec],
    current_fields: dict[str, NumpyColumnSource],
    computed_fields: list[NumpyComputedSpec],
) -> NumpyPrefixPlan | None:
    """Close one plan only when every pure computed expression remains observable."""
    if not stages:
        return None
    retained_computed = {
        _BUILTIN_ID(source)
        for source in current_fields.values()
        if isinstance(source, NumpyComputedSpec)
    }
    if _BUILTIN_ANY(
        _BUILTIN_ID(expression) not in retained_computed for expression in computed_fields
    ):
        return None
    return NumpyPrefixPlan(
        _BUILTIN_LEN(stages),
        _BUILTIN_TUPLE(stages),
        _BUILTIN_TUPLE(current_fields.items()),
    )


def plan_numpy_prefix(plan: Pipeline) -> NumpyPrefixPlan | None:
    """Return one complete safe NumPy filter/projection plan, or ``None``.

    Runtime failpoint, ownership, and mutable-array guards remain executor responsibilities.
    Planning only recognizes a closed structure without opening the source.
    """
    if plan.engine != "auto" or plan.parallel is not None or not plan.operations:
        return None
    from ..tabular.numpy import NumpyRowSource

    descriptor = plan.source.native_data
    if type(descriptor) is not NumpyRowSource or _BUILTIN_ANY(
        type(name) is not _BUILTIN_STR for name in descriptor.columns
    ):
        return None
    current_fields: dict[str, NumpyColumnSource] = {name: name for name in descriptor.columns}
    stages: list[NumpyStageSpec] = []
    computed = False
    computed_fields: list[NumpyComputedSpec] = []
    for operation in plan.operations:
        if isinstance(operation, FilterOp):
            if computed:
                return None
            if not _supports_integer_kernels(descriptor.array):
                return None
            filter_stage = _primitive_filter(operation, current_fields)
            if filter_stage is None:
                return None
            stages.append(filter_stage)
            continue
        if isinstance(operation, MapOp):
            projection = _direct_projection(operation, current_fields)
            if projection is not None:
                stages.append(projection)
                current_fields = {field.output: field.source_field for field in projection.fields}
                continue
            renamed = _direct_rename(operation, current_fields)
            if renamed is not None:
                stages.append(renamed)
                current_fields = _BUILTIN_DICT(renamed.fields)
                continue
            with_columns = _direct_with_columns(operation, current_fields)
            if with_columns is None or computed:
                return None
            if not _supports_integer_kernels(descriptor.array):
                return None
            stages.append(with_columns)
            for name, expression in with_columns.fields:
                current_fields[name] = expression
                computed_fields.append(expression)
            computed = True
            continue
        return None
    return _finish_numpy_prefix(stages, current_fields, computed_fields)
