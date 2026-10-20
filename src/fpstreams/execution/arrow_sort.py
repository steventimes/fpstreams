"""Guarded retained Arrow sorting and canonical result conversion."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any, Protocol

from ..planning.arrow_source import ArrowBatchSource as _ArrowBatchSource


class ArrowSortHooks(Protocol):
    """Keep retained sorting on the facade's current guards and row conversion."""

    ArrowBatchSource: type[_ArrowBatchSource]
    _EXPECTED_ARROW_ERRORS: tuple[type[Exception], ...]
    _arrow_modules: Callable[[], tuple[Any, Any]]
    _has_primitive_builtin_schema: Callable[[Any, Any], bool]
    _retained_arrow_stable_sort_result: Callable[[Any], tuple[Any, _ArrowBatchSource] | None]
    _retained_arrow_sorted_batches: Callable[[Any, _ArrowBatchSource], Iterator[Any]]
    batch_to_rows: Callable[[Any], list[dict[str, Any]]]


def _retained_arrow_stable_sort_result(
    plan: Any, *, hooks: ArrowSortHooks
) -> tuple[Any, _ArrowBatchSource] | None:
    """Return one guarded stable Arrow sort result before Python row conversion."""
    from ..expressions.selectors import _direct_field
    from ..physical.plan import SortPhysicalNode, SortStrategy
    from ..runtime.failpoints import has_active_failpoints, hit

    if (
        plan.root is not None
        or plan.engine != "auto"
        or plan.parallel is not None
        or len(plan.nodes) != 1
        or not isinstance(node := plan.nodes[0], SortPhysicalNode)
        or node.strategy is not SortStrategy.ARROW_STABLE
        or node.operation.buffer_size is not None
        or type(node.operation.reverse) is not bool
        or (field := _direct_field(node.operation.key)) is None
        or has_active_failpoints()
        or not plan.source.capabilities.reiterable
    ):
        return None
    descriptor = plan.source.native_data
    if (
        not isinstance(descriptor, hooks.ArrowBatchSource)
        or descriptor.kind not in {"table", "record_batch"}
        or descriptor.materialized_data is None
    ):
        return None

    pa, pc = hooks._arrow_modules()
    retained = descriptor.materialized_data
    if descriptor.kind == "table":
        if not isinstance(retained, pa.Table):
            return None
    elif not isinstance(retained, pa.RecordBatch):
        return None
    schema = retained.schema
    names = tuple(schema.names)
    if names.count(field) != 1 or not hooks._has_primitive_builtin_schema(pa, schema):
        return None
    field_index = names.index(field)
    key_values = retained.column(field_index)
    types = pa.types
    if key_values.null_count or not (
        types.is_boolean(key_values.type)
        or types.is_integer(key_values.type)
        or types.is_string(key_values.type)
        or types.is_large_string(key_values.type)
        or types.is_binary(key_values.type)
        or types.is_large_binary(key_values.type)
        or types.is_fixed_size_binary(key_values.type)
    ):
        return None

    try:
        retained.validate(full=True)
    except hooks._EXPECTED_ARROW_ERRORS:
        return None

    plan.source.open_native(hooks.ArrowBatchSource)
    hit("source.open.after")
    order = "descending" if node.operation.reverse else "ascending"
    try:
        indices = pc.sort_indices(retained, sort_keys=[(field, order)])
        ordered = pc.take(retained, indices)
    except hooks._EXPECTED_ARROW_ERRORS:
        return None
    return ordered, descriptor


def _retained_arrow_sorted_batches(
    ordered: Any, descriptor: _ArrowBatchSource, *, hooks: ArrowSortHooks
) -> Iterator[Any]:
    """Yield one ordered retained result in the adapter's canonical batch bounds."""
    if descriptor.kind == "table":
        yield from ordered.to_batches(max_chunksize=descriptor.batch_size)
        return
    for offset in range(0, ordered.num_rows, descriptor.batch_size):
        yield ordered.slice(offset, descriptor.batch_size)


def materialize_retained_arrow_stable_sort(plan: Any, *, hooks: ArrowSortHooks) -> list[Any] | None:
    """Collect a guarded retained Arrow sort without per-row generator forwarding."""
    result = hooks._retained_arrow_stable_sort_result(plan)
    if result is None:
        return None
    ordered, descriptor = result
    rows: list[Any] = []
    for batch in hooks._retained_arrow_sorted_batches(ordered, descriptor):
        rows.extend(hooks.batch_to_rows(batch))
    return rows


def try_retained_arrow_stable_sort(plan: Any, *, hooks: ArrowSortHooks) -> Iterator[Any] | None:
    """Sort one proven retained Arrow source without first constructing row dictionaries."""
    result = hooks._retained_arrow_stable_sort_result(plan)
    if result is None:
        return None
    ordered, descriptor = result

    def rows() -> Iterator[Any]:
        """Convert the already ordered primitive Arrow values in canonical batch bounds."""
        for batch in hooks._retained_arrow_sorted_batches(ordered, descriptor):
            yield from hooks.batch_to_rows(batch)

    return rows()
