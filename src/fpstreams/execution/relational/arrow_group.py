"""Arrow group aggregation execution across retained and streaming sources."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass, replace
from itertools import chain
from typing import Any

from ...collecting.aggregation import _MISSING
from ...physical.plan import PhysicalNode
from ...physical.relational import (
    ArrowGroupAggregateSpec,
    ArrowGroupSumSpec,
    GroupAggregatePhysicalNode,
    PipelinePhysicalNode,
    SourcePhysicalNode,
)
from ...planning.arrow import ArrowPrefixPlan, plan_arrow_table_prefix
from ...planning.arrow_source import ArrowBatchSource, batch_to_rows
from ...planning.logical import Pipeline
from ...runtime.iterators import close_iterators
from .arrow_global import _source_arrow_field
from .arrow_group_rows import _arrow_group_lane_columns

ModuleImporter = Callable[[str], Any]
ArrowPlanningOperations = Callable[[tuple[PhysicalNode, ...]], tuple[Any, ...] | None]
ArrowGroupTable = Callable[[Any, ArrowBatchSource], Any | None]
AuthoritativeGroup = Callable[[Iterator[Any], GroupAggregatePhysicalNode], Iterator[dict[str, Any]]]
ObserveBatch = Callable[[Any], None]
BatchTotals = Callable[[Any, Any, int, Any, Any], tuple[list[Any], list[Any]]]
TryFileGroup = Callable[
    [GroupAggregatePhysicalNode, ArrowBatchSource, Any, Any], list[dict[str, Any]] | None
]
TryRetainedGroup = Callable[
    [Any, ArrowGroupAggregateSpec, str, Any, Any], list[dict[str, Any]] | None
]
MaterializeGroup = Callable[..., list[dict[str, Any]] | None]


@dataclass(frozen=True, slots=True)
class ArrowGroupHooks:
    """Live owner seams captured once before Arrow group dispatch."""

    import_module: ModuleImporter
    arrow_planning_operations: ArrowPlanningOperations
    arrow_group_table: ArrowGroupTable
    execute_authoritative_group: AuthoritativeGroup
    observe_arrow_batch_rows: ObserveBatch
    arrow_group_batch_totals: BatchTotals
    try_arrow_file_group_sum: TryFileGroup
    try_arrow_retained_group_aggregate: TryRetainedGroup
    materialize_arrow_group_rows: MaterializeGroup
    batch_scalar_max_rows: int
    file_min_rows: int
    reader_multi_min_rows: int
    reader_cardinality_sample_rows: int
    reader_max_distinct_ratio: float
    csv_min_bytes: int


def _is_supported_arrow_group_key_type(types: Any, key_type: Any) -> bool:
    """Return whether Arrow keys have the same scalar equality as Python grouping."""
    logical_key_type = key_type.value_type if types.is_dictionary(key_type) else key_type
    return bool(
        types.is_null(logical_key_type)
        or types.is_boolean(logical_key_type)
        or types.is_integer(logical_key_type)
        or types.is_string(logical_key_type)
        or types.is_large_string(logical_key_type)
        or types.is_binary(logical_key_type)
        or types.is_large_binary(logical_key_type)
    )


def _prepare_arrow_group_keys(
    key_values: Any,
    types: Any,
    pc: Any,
) -> tuple[Any, Any] | None:
    """Canonicalize dictionary keys and retain first-seen order as an Arrow array."""
    if not types.is_dictionary(key_values.type):
        return key_values, pc.unique(key_values)
    key_values = key_values.unify_dictionaries()
    if key_values.num_chunks == 0:
        return None
    dictionary = key_values.chunk(0).dictionary
    if dictionary.null_count or pc.count_distinct(dictionary, mode="all").as_py() != len(
        dictionary
    ):
        # Arrow groups dictionary indices. Duplicate or null dictionary values can
        # therefore split values that Python regards as one logical key.
        return None
    return key_values, pc.unique(key_values)


def _merge_arrow_group_totals(
    groups: dict[Any, int],
    keys: list[Any],
    totals: list[Any],
) -> None:
    """Merge one complete batch result into insertion-ordered Python integer state."""
    for key, subtotal in zip(keys, totals, strict=True):
        value = int(subtotal)
        if key in groups:
            groups[key] += value
        else:
            groups[key] = 0 + value


def _consume_arrow_group_batch_as_scalars(
    groups: dict[Any, int],
    key_values: Any,
    sum_values: Any,
) -> None:
    """Continue a claimed reader through canonical scalar selection and addition."""
    keys = key_values.to_pylist()
    values = sum_values.to_pylist()
    for key, selected in zip(keys, values, strict=True):
        if key in groups:
            groups[key] = groups[key] + selected
        else:
            groups[key] = 0 + selected


def _arrow_group_batch_totals(
    key_values: Any,
    sum_values: Any,
    row_count: int,
    pa: Any,
    pc: Any,
) -> tuple[list[Any], list[Any]]:
    """Compute one all-or-nothing stable batch partial for incremental reader merge."""
    # Arrow's unique kernel explicitly preserves original order.  Keep that first-seen
    # oracle separate: a single-thread group is stable but can still expose hash order.
    encounter_order = pc.unique(key_values).to_pylist()
    bounds = pc.min_max(sum_values).as_py()
    maximum_absolute = max(abs(bounds["min"]), abs(bounds["max"]))
    wide_totals = maximum_absolute * row_count > 2**63 - 1
    if wide_totals:
        sum_values = pc.cast(sum_values, pa.decimal128(38, 0))
    grouped = (
        pa.table(
            {
                "__fpstreams_group_key": key_values,
                "__fpstreams_group_value": sum_values,
            }
        )
        .group_by("__fpstreams_group_key", use_threads=False)
        .aggregate([("__fpstreams_group_value", "sum")])
    )
    totals_by_key = dict(
        zip(grouped.column(0).to_pylist(), grouped.column(1).to_pylist(), strict=True)
    )
    return encounter_order, [totals_by_key[key] for key in encounter_order]


def _consume_arrow_group_batches(
    batches: Iterator[Any],
    *,
    key_index: int,
    value_index: int,
    pa: Any,
    pc: Any,
    hooks: ArrowGroupHooks,
    groups: dict[Any, int] | None = None,
) -> dict[Any, int]:
    """Merge stable Arrow partials, switching a claimed stream to scalar folding on decline."""
    if groups is None:
        groups = {}
    use_arrow = True
    for batch in batches:
        hooks.observe_arrow_batch_rows(batch)
        row_count = int(batch.num_rows)
        if row_count == 0:
            continue
        key_values = batch.column(key_index)
        sum_values = batch.column(value_index)
        if row_count <= hooks.batch_scalar_max_rows or sum_values.null_count or not use_arrow:
            _consume_arrow_group_batch_as_scalars(groups, key_values, sum_values)
            continue
        try:
            keys, totals = hooks.arrow_group_batch_totals(
                key_values,
                sum_values,
                row_count,
                pa,
                pc,
            )
        except (ArithmeticError, NotImplementedError, TypeError, ValueError):
            use_arrow = False
            _consume_arrow_group_batch_as_scalars(groups, key_values, sum_values)
        else:
            _merge_arrow_group_totals(groups, keys, totals)
    return groups


def _consume_arrow_file_group_batches(
    first: Any,
    batches: Iterator[Any],
    *,
    key_index: int,
    value_index: int,
    pa: Any,
    pc: Any,
    hooks: ArrowGroupHooks,
) -> dict[Any, int]:
    """Fold a measured small-file prefix scalarly, then switch the same stream to Arrow."""
    groups: dict[Any, int] = {}
    scalar_rows_left = hooks.file_min_rows
    current = first
    while True:
        row_count = int(current.num_rows)
        scalar_count = min(row_count, scalar_rows_left)
        if scalar_count:
            _consume_arrow_group_batch_as_scalars(
                groups,
                current.column(key_index).slice(0, scalar_count),
                current.column(value_index).slice(0, scalar_count),
            )
            scalar_rows_left -= scalar_count
        if scalar_count < row_count:
            remainder = current.slice(scalar_count)
            return _consume_arrow_group_batches(
                chain((remainder,), batches),
                key_index=key_index,
                value_index=value_index,
                pa=pa,
                pc=pc,
                hooks=hooks,
                groups=groups,
            )
        try:
            current = next(batches)
        except StopIteration:
            return groups
        if current.num_rows == 0:
            continue


def _try_arrow_reader_group_sum(
    node: GroupAggregatePhysicalNode,
    descriptor: ArrowBatchSource,
    pa: Any,
    pc: Any,
    hooks: ArrowGroupHooks,
) -> list[dict[str, Any]] | None:
    """Incrementally aggregate one proven one-shot reader without row dictionaries."""
    spec = node.arrow_i64_sum
    if (
        not isinstance(spec, ArrowGroupSumSpec)
        or not isinstance(node.input, SourcePhysicalNode)
        or descriptor.kind != "reader"
        or descriptor.reiterable
        or node.input.source.capabilities.reiterable
    ):
        return None
    schema = descriptor.schema_hint
    if schema is None:
        return None
    from ..arrow import arrow_schema_has_primitive_rows

    if not arrow_schema_has_primitive_rows(pa, schema):
        return None
    names = tuple(schema.names)
    if names.count(spec.key_field) != 1 or names.count(spec.value_field) != 1:
        return None
    key_index = names.index(spec.key_field)
    value_index = names.index(spec.value_field)
    key_type = schema.field(key_index).type
    types = pa.types
    if types.is_dictionary(key_type) or not _is_supported_arrow_group_key_type(types, key_type):
        return None
    if not types.is_int64(schema.field(value_index).type):
        return None

    # Every rejection above is deliberately pre-claim.  From this point onward the reader
    # cannot be reopened, so recoverable compute declines continue from the current batch.
    node.input.source.open_native(ArrowBatchSource)
    batches = descriptor.open_batches()
    active_error: BaseException | None = None
    try:
        groups = _consume_arrow_group_batches(
            batches,
            key_index=key_index,
            value_index=value_index,
            pa=pa,
            pc=pc,
            hooks=hooks,
        )
    except BaseException as error:
        active_error = error
        raise
    finally:
        close_iterators((batches,), active_error=active_error)

    key_name = node.key_names[0]
    return [{key_name: key, spec.output_name: total} for key, total in groups.items()]


def _new_arrow_group_multi_state(spec: ArrowGroupAggregateSpec) -> list[Any]:
    """Allocate one canonical count/sum/min/max state for a newly encountered key."""
    return [0 if lane.kind in {"count", "sum"} else _MISSING for lane in spec.lanes]


def _merge_arrow_group_multi_values(
    state: list[Any],
    values: Iterator[Any],
    spec: ArrowGroupAggregateSpec,
) -> None:
    """Merge one row or one batch partial in declared aggregate-lane order."""
    for position, (lane, value) in enumerate(zip(spec.lanes, values, strict=True)):
        current = state[position]
        if lane.kind in {"count", "sum"}:
            state[position] = current + value
        elif lane.kind == "min":
            if current is _MISSING or value < current:
                state[position] = value
        elif current is _MISSING or value > current:
            state[position] = value


def _merge_arrow_group_multi_partials(
    groups: dict[Any, list[Any]],
    rows: list[dict[str, Any]],
    spec: ArrowGroupAggregateSpec,
    key_name: str,
) -> None:
    """Merge stable per-batch Arrow rows into insertion-ordered Python states."""
    output_names = tuple(lane.output_name for lane in spec.lanes)
    for row in rows:
        key = row[key_name]
        if key in groups:
            state = groups[key]
        else:
            state = _new_arrow_group_multi_state(spec)
            groups[key] = state
        for position, (lane, output_name) in enumerate(zip(spec.lanes, output_names, strict=True)):
            value = row[output_name]
            current = state[position]
            if lane.kind in {"count", "sum"}:
                state[position] = current + value
            elif lane.kind == "min":
                if current is _MISSING or value < current:
                    state[position] = value
            elif current is _MISSING or value > current:
                state[position] = value


def _consume_arrow_group_multi_batch_as_scalars(
    groups: dict[Any, list[Any]],
    batch: Any,
    key_index: int,
    lane_indexes: tuple[int | None, ...],
    spec: ArrowGroupAggregateSpec,
) -> None:
    """Fold one claimed nullable batch in canonical row-major aggregate order."""
    keys = batch.column(key_index).to_pylist()
    materialized: dict[int, list[Any]] = {}
    lanes: list[list[Any] | None] = []
    for field_index in lane_indexes:
        if field_index is None:
            lanes.append(None)
            continue
        values = materialized.get(field_index)
        if values is None:
            values = batch.column(field_index).to_pylist()
            materialized[field_index] = values
        lanes.append(values)

    for row_position, key in enumerate(keys):
        try:
            hash(key)
            state = groups.get(key)
        except TypeError:
            raise TypeError("group_by keys must be hashable") from None
        if state is None:
            state = _new_arrow_group_multi_state(spec)
            groups[key] = state
        _merge_arrow_group_multi_values(
            state,
            (1 if values is None else values[row_position] for values in lanes),
            spec,
        )


def _arrow_reader_group_multi_layout(
    schema: Any,
    spec: ArrowGroupAggregateSpec,
    pa: Any,
) -> tuple[int, tuple[int | None, ...]] | None:
    """Resolve a fully proven reader schema without claiming its one-shot source."""
    from ..arrow import arrow_schema_has_primitive_rows

    if not arrow_schema_has_primitive_rows(pa, schema):
        return None
    names = tuple(schema.names)
    if names.count(spec.key_field) != 1:
        return None
    key_index = names.index(spec.key_field)
    key_type = schema.field(key_index).type
    types = pa.types
    if types.is_dictionary(key_type) or not _is_supported_arrow_group_key_type(types, key_type):
        return None
    lane_indexes: list[int | None] = []
    for lane in spec.lanes:
        value_field = lane.value_field
        if lane.kind == "count":
            lane_indexes.append(None)
            continue
        if value_field is None or names.count(value_field) != 1:
            return None
        field_index = names.index(value_field)
        if not types.is_int64(schema.field(field_index).type):
            return None
        lane_indexes.append(field_index)
    return key_index, tuple(lane_indexes)


def _arrow_reader_group_prefers_rows(
    key_values: Any,
    row_count: int,
    pc: Any,
    hooks: ArrowGroupHooks,
) -> bool:
    """Reject batch partials when their merge cardinality is predictably more expensive."""
    if row_count < hooks.reader_cardinality_sample_rows:
        return False
    try:
        distinct = int(pc.count_distinct(key_values, mode="all").as_py())
    except MemoryError:
        raise
    except (ArithmeticError, NotImplementedError, TypeError, ValueError):
        return True
    return distinct > row_count * hooks.reader_max_distinct_ratio


def _execute_claimed_arrow_reader_group_rows(
    first: Any,
    batches: Iterator[Any],
    node: GroupAggregatePhysicalNode,
    hooks: ArrowGroupHooks,
) -> list[dict[str, Any]]:
    """Resume canonical grouping from a reader batch already claimed for cardinality probing."""
    spec = node.closed_group
    if spec is None:
        raise RuntimeError("claimed Arrow reader group is missing its canonical fallback")
    from ...tabular.arrow import batch_to_rows as canonical_batch_to_rows

    def rows() -> Iterator[dict[str, Any]]:
        yield from canonical_batch_to_rows(first)
        for later in batches:
            yield from canonical_batch_to_rows(later)

    return list(hooks.execute_authoritative_group(rows(), node))


def _try_arrow_reader_group_aggregate(
    node: GroupAggregatePhysicalNode,
    descriptor: ArrowBatchSource,
    spec: ArrowGroupAggregateSpec,
    pa: Any,
    pc: Any,
    hooks: ArrowGroupHooks,
) -> list[dict[str, Any]] | None:
    """Incrementally reduce closed lanes from one proven one-shot Arrow reader."""
    if (
        not isinstance(node.input, SourcePhysicalNode)
        or descriptor.kind != "reader"
        or descriptor.reiterable
        or node.input.source.capabilities.reiterable
    ):
        return None
    schema = descriptor.schema_hint
    if schema is None:
        return None
    layout = _arrow_reader_group_multi_layout(schema, spec, pa)
    if layout is None:
        return None
    key_index, lane_indexes = layout

    # Every rejection above happens before the one-shot source is claimed. Recoverable
    # per-batch kernel declines below continue scalarly from that exact claimed batch.
    node.input.source.open_native(ArrowBatchSource)
    batches = descriptor.open_batches()
    groups: dict[Any, list[Any]] = {}
    key_name = node.key_names[0]
    use_arrow = True
    active_error: BaseException | None = None
    try:
        for batch in batches:
            hooks.observe_arrow_batch_rows(batch)
            row_count = int(batch.num_rows)
            if row_count == 0:
                continue
            if not groups:
                if row_count <= hooks.reader_multi_min_rows:
                    return _execute_claimed_arrow_reader_group_rows(
                        batch,
                        batches,
                        node,
                        hooks,
                    )
                if _arrow_reader_group_prefers_rows(
                    batch.column(key_index),
                    row_count,
                    pc,
                    hooks,
                ):
                    return _execute_claimed_arrow_reader_group_rows(
                        batch,
                        batches,
                        node,
                        hooks,
                    )
            nullable = any(
                field_index is not None and batch.column(field_index).null_count
                for field_index in lane_indexes
            )
            if nullable or not use_arrow:
                _consume_arrow_group_multi_batch_as_scalars(
                    groups,
                    batch,
                    key_index,
                    lane_indexes,
                    spec,
                )
                continue
            partials = hooks.try_arrow_retained_group_aggregate(
                batch,
                spec,
                key_name,
                pa,
                pc,
            )
            if partials is None:
                use_arrow = False
                _consume_arrow_group_multi_batch_as_scalars(
                    groups,
                    batch,
                    key_index,
                    lane_indexes,
                    spec,
                )
            else:
                _merge_arrow_group_multi_partials(groups, partials, spec, key_name)
    except BaseException as error:
        active_error = error
        raise
    finally:
        close_iterators((batches,), active_error=active_error)

    result: list[dict[str, Any]] = []
    for key, state in groups.items():
        row: dict[str, Any] = {key_name: key}
        for lane, value in zip(spec.lanes, state, strict=True):
            row[lane.output_name] = None if value is _MISSING else value
        result.append(row)
    return result


def _try_arrow_file_group_sum_impl(
    node: GroupAggregatePhysicalNode,
    descriptor: ArrowBatchSource,
    pa: Any,
    pc: Any,
    hooks: ArrowGroupHooks,
) -> list[dict[str, Any]] | None:
    """Incrementally aggregate direct CSV/Parquet fields without retaining a whole table."""
    spec = node.arrow_i64_sum
    if (
        not isinstance(spec, ArrowGroupSumSpec)
        or not isinstance(node.input, SourcePhysicalNode)
        or descriptor.kind not in {"csv", "parquet"}
        or not descriptor.reiterable
        or not node.input.source.capabilities.reiterable
    ):
        return None

    node.input.source.open_native(ArrowBatchSource)
    columns = tuple(dict.fromkeys((spec.key_field, spec.value_field)))
    # Default CSV projection is admitted only with large-file evidence before this function.
    # Custom parse/callback sources have no projection opener and retain their one base stream.
    use_projection = descriptor.kind == "parquet" or descriptor.projection_opener is not None
    batches = descriptor.open_batches(columns=columns if use_projection else None)

    def canonical_from(first: Any) -> list[dict[str, Any]]:
        """Resume the exact row grouping loop from an already-opened file batch stream."""
        simple_sum = node.simple_sum
        if simple_sum is None:
            raise RuntimeError("Arrow file group sum is missing its canonical fallback")

        def rows() -> Iterator[dict[str, Any]]:
            yield from first.to_pylist()
            for later in batches:
                yield from later.to_pylist()

        return list(hooks.execute_authoritative_group(rows(), node))

    active_error: BaseException | None = None
    try:
        for first in batches:
            if first.num_rows == 0:
                continue
            names = tuple(first.schema.names)
            if names.count(spec.key_field) != 1 or names.count(spec.value_field) != 1:
                return canonical_from(first)
            key_index = names.index(spec.key_field)
            value_index = names.index(spec.value_field)
            types = pa.types
            key_type = first.schema.field(key_index).type
            if types.is_dictionary(key_type) or not _is_supported_arrow_group_key_type(
                types, key_type
            ):
                return canonical_from(first)
            if not types.is_int64(first.schema.field(value_index).type):
                return canonical_from(first)
            groups = _consume_arrow_file_group_batches(
                first,
                batches,
                key_index=key_index,
                value_index=value_index,
                pa=pa,
                pc=pc,
                hooks=hooks,
            )
            key_name = node.key_names[0]
            return [{key_name: key, spec.output_name: total} for key, total in groups.items()]
        return []
    except BaseException as error:
        active_error = error
        raise
    finally:
        close_iterators((batches,), active_error=active_error)


def _try_arrow_retained_group_aggregate_impl(
    table: Any,
    spec: ArrowGroupAggregateSpec,
    key_name: str,
    pa: Any,
    pc: Any,
    hooks: ArrowGroupHooks,
) -> list[dict[str, Any]] | None:
    """Run closed int64 lanes on one reusable Arrow table without boxing input rows."""
    # Empty Python grouping never evaluates selectors, even when their fields are absent.
    if table.num_rows == 0:
        return []
    names = tuple(table.schema.names)
    if names.count(spec.key_field) != 1:
        return None
    key_values = table.column(names.index(spec.key_field))
    types = pa.types
    if not _is_supported_arrow_group_key_type(types, key_values.type):
        return None
    lane_columns = _arrow_group_lane_columns(table, names, spec, types)
    if lane_columns is None:
        return None

    try:
        prepared_keys = _prepare_arrow_group_keys(key_values, types, pc)
        if prepared_keys is None:
            return None
        prepared_key_values, encounter_order = prepared_keys
        compact_columns: dict[str, Any] = {
            "__fpstreams_group_key": prepared_key_values,
        }
        aggregate_requests: list[tuple[Any, str]] = []
        aggregate_fields: list[str] = []
        wide_sums: list[bool] = []
        count_requested = False
        for position, (lane, values) in enumerate(zip(spec.lanes, lane_columns, strict=True)):
            if lane.kind == "count":
                if not count_requested:
                    aggregate_requests.append(([], "count_all"))
                    count_requested = True
                aggregate_fields.append("count_all")
                wide_sums.append(False)
                continue
            assert values is not None
            wide_sum = False
            if lane.kind == "sum":
                bounds = pc.min_max(values).as_py()
                maximum_absolute = max(abs(bounds["min"]), abs(bounds["max"]))
                wide_sum = maximum_absolute * table.num_rows > 2**63 - 1
                if wide_sum:
                    values = pc.cast(values, pa.decimal128(38, 0))
            internal_name = f"__fpstreams_group_value_{position}"
            compact_columns[internal_name] = values
            aggregate_requests.append((internal_name, lane.kind))
            aggregate_fields.append(f"{internal_name}_{lane.kind}")
            wide_sums.append(wide_sum)
        grouped = (
            pa.table(compact_columns)
            .group_by("__fpstreams_group_key", use_threads=False)
            .aggregate(aggregate_requests)
        )
        grouped_names = tuple(grouped.schema.names)
        key_field = "__fpstreams_group_key"
        if grouped_names.count(key_field) != 1 or any(
            grouped_names.count(field) != 1 for field in aggregate_fields
        ):
            return None
        return hooks.materialize_arrow_group_rows(
            grouped,
            grouped_names,
            aggregate_fields,
            encounter_order,
            spec,
            key_name,
            wide_sums,
            pa,
            types,
            pc,
        )
    except (ArithmeticError, NotImplementedError, TypeError, ValueError):
        return None


def _source_arrow_group_spec(
    spec: ArrowGroupAggregateSpec,
    prefix: ArrowPrefixPlan,
) -> ArrowGroupAggregateSpec | None:
    """Map projected group fields back to the retained source table."""
    key_field = _source_arrow_field(spec.key_field, prefix)
    if key_field is None:
        return None
    lanes: list[Any] = []
    for lane in spec.lanes:
        if lane.value_field is None:
            lanes.append(lane)
            continue
        value_field = _source_arrow_field(lane.value_field, prefix)
        if value_field is None:
            return None
        lanes.append(replace(lane, value_field=value_field))
    return ArrowGroupAggregateSpec(key_field, tuple(lanes))


def _arrow_pipeline_group_is_total(
    table: Any,
    spec: ArrowGroupAggregateSpec,
    prefix: ArrowPrefixPlan,
    pa: Any,
) -> bool:
    """Prove grouping cannot fail before a later prefix row is evaluated."""
    if table.num_rows == 0:
        return True
    source_spec = _source_arrow_group_spec(spec, prefix)
    if source_spec is None:
        return False
    names = tuple(table.schema.names)
    if names.count(source_spec.key_field) != 1:
        return False
    key_values = table.column(names.index(source_spec.key_field))
    if not _is_supported_arrow_group_key_type(pa.types, key_values.type):
        return False
    return _arrow_group_lane_columns(table, names, source_spec, pa.types) is not None


def _reuse_eager_pandas_group_table(
    table: Any,
    node: GroupAggregatePhysicalNode,
    descriptor: ArrowBatchSource,
    hooks: ArrowGroupHooks,
) -> list[dict[str, Any]] | None:
    """Continue a pandas group decline from its already-converted table snapshot."""
    if descriptor.kind != "dataframe" or descriptor.columnar_opener is None:
        return None
    from ...tabular.arrow import batch_to_rows as canonical_batch_to_rows

    def rows() -> Iterator[dict[str, Any]]:
        for batch in table.to_batches(max_chunksize=descriptor.batch_size):
            yield from canonical_batch_to_rows(batch)

    values = rows()
    if node.simple_sum is not None or node.closed_group is not None:
        return list(hooks.execute_authoritative_group(values, node))
    return None


def _try_arrow_group_sum_impl(  # noqa: C901 - guarded source/backend dispatch
    node: GroupAggregatePhysicalNode,
    hooks: ArrowGroupHooks,
) -> list[dict[str, Any]] | None:
    """Aggregate a proven direct Arrow i64 field sum without boxing its input rows."""
    spec = node.arrow_i64_sum
    if spec is None:
        return None
    if isinstance(node.input, PipelinePhysicalNode):
        pipeline_input = node.input
        if not isinstance(spec, ArrowGroupAggregateSpec) or not isinstance(
            pipeline_input.input, SourcePhysicalNode
        ):
            return None
        source = pipeline_input.input.source
        native_data = source.native_data
        if not isinstance(native_data, ArrowBatchSource):
            return None
        from ..arrow import (
            retained_table_rows_are_canonical,
            try_arrow_table,
            try_validated_retained_arrow_table,
        )

        operations = hooks.arrow_planning_operations(pipeline_input.stages)
        if operations is None:
            return None
        pipeline = Pipeline(
            source,
            operations,
            pipeline_input.engine,
            pipeline_input.parallel,
        )
        prefix = plan_arrow_table_prefix(pipeline)
        if prefix is None:
            return None
        pa = hooks.import_module("pyarrow")
        pc = hooks.import_module("pyarrow.compute")
        raw_table = hooks.arrow_group_table(pa, native_data)
        if (
            raw_table is None
            or not retained_table_rows_are_canonical(raw_table)
            or not _arrow_pipeline_group_is_total(raw_table, spec, prefix, pa)
        ):
            return None
        handled, table = try_validated_retained_arrow_table(
            pipeline,
            prefix=prefix,
        )
        if not handled:
            handled, table = try_arrow_table(
                pipeline,
                batch_size=native_data.batch_size,
                preserve_source_schema=True,
            )
        if not handled:
            return None
        if table is None:
            raise RuntimeError("handled Arrow table materialization returned no table")

        group_rows = hooks.try_arrow_retained_group_aggregate(
            table,
            spec,
            node.key_names[0],
            pa,
            pc,
        )
        if group_rows is not None:
            return group_rows

        values = iter(batch_to_rows(table))
        if node.simple_sum is not None or node.closed_group is not None:
            return list(hooks.execute_authoritative_group(values, node))
        raise RuntimeError("claimed Arrow group input has no canonical aggregate fallback")

    if not isinstance(node.input, SourcePhysicalNode):
        return None
    native_data = node.input.source.native_data
    if not isinstance(native_data, ArrowBatchSource):
        return None

    pa = hooks.import_module("pyarrow")
    pc = hooks.import_module("pyarrow.compute")
    if isinstance(spec, ArrowGroupAggregateSpec):
        if native_data.kind == "reader":
            return _try_arrow_reader_group_aggregate(
                node,
                native_data,
                spec,
                pa,
                pc,
                hooks,
            )
        if not node.input.source.capabilities.reiterable or native_data.kind not in {
            "table",
            "record_batch",
            "dataframe",
            "polars",
        }:
            return None
        descriptor = node.input.source.open_native(ArrowBatchSource)
        table = hooks.arrow_group_table(pa, descriptor)
        if table is None:
            return None
        from ..arrow import retained_table_rows_are_valid

        if not retained_table_rows_are_valid(table):
            return _reuse_eager_pandas_group_table(table, node, native_data, hooks)
        group_rows = hooks.try_arrow_retained_group_aggregate(
            table,
            spec,
            node.key_names[0],
            pa,
            pc,
        )
        if group_rows is not None:
            return group_rows
        from ..arrow import retained_table_rows_are_canonical

        if not retained_table_rows_are_canonical(table) or node.closed_group is None:
            return _reuse_eager_pandas_group_table(table, node, native_data, hooks)
        from ...tabular.arrow import batch_to_rows as canonical_batch_to_rows

        return list(
            hooks.execute_authoritative_group(
                iter(canonical_batch_to_rows(table)),
                node,
            )
        )
    if not isinstance(spec, ArrowGroupSumSpec):
        return None

    if native_data.kind == "csv" and native_data.projection_opener is not None:
        size_opener = native_data.byte_size_opener
        if size_opener is None:
            return None
        size_bytes = size_opener()
        if type(size_bytes) is not int or size_bytes < hooks.csv_min_bytes:
            return None

    if native_data.kind == "reader":
        return _try_arrow_reader_group_sum(node, native_data, pa, pc, hooks)
    if native_data.kind in {"csv", "parquet"}:
        return hooks.try_arrow_file_group_sum(node, native_data, pa, pc)
    if not node.input.source.capabilities.reiterable or native_data.kind not in {
        "table",
        "record_batch",
        "dataframe",
        "polars",
    }:
        return None
    descriptor = node.input.source.open_native(ArrowBatchSource)
    table = hooks.arrow_group_table(pa, descriptor)
    if table is None:
        return None

    # Empty row execution never evaluates selectors, even if the retained schema does
    # not contain them. Preserve that timing before consulting field metadata.
    if table.num_rows == 0:
        return []
    from ..arrow import retained_table_rows_are_valid

    if not retained_table_rows_are_valid(table):
        return _reuse_eager_pandas_group_table(table, node, native_data, hooks)
    names = table.schema.names
    if names.count(spec.key_field) != 1 or names.count(spec.value_field) != 1:
        return _reuse_eager_pandas_group_table(table, node, native_data, hooks)
    key_values = table.column(names.index(spec.key_field))
    sum_values = table.column(names.index(spec.value_field))
    types = pa.types
    if not _is_supported_arrow_group_key_type(types, key_values.type):
        return _reuse_eager_pandas_group_table(table, node, native_data, hooks)
    if not types.is_int64(sum_values.type) or sum_values.null_count:
        return _reuse_eager_pandas_group_table(table, node, native_data, hooks)

    try:
        prepared_keys = _prepare_arrow_group_keys(key_values, types, pc)
        if prepared_keys is None:
            return _reuse_eager_pandas_group_table(table, node, native_data, hooks)
        key_values, encounter_order_values = prepared_keys
        bounds = pc.min_max(sum_values).as_py()
        maximum_absolute = max(abs(bounds["min"]), abs(bounds["max"]))
        wide_totals = maximum_absolute * table.num_rows > 2**63 - 1
        if wide_totals:
            sum_values = pc.cast(sum_values, pa.decimal128(38, 0))
        compact = pa.table(
            {
                "__fpstreams_group_key": key_values,
                "__fpstreams_group_value": sum_values,
            }
        )
        grouped = compact.group_by("__fpstreams_group_key", use_threads=False).aggregate(
            [("__fpstreams_group_value", "sum")]
        )
        grouped_names = tuple(grouped.schema.names)
        key_field = "__fpstreams_group_key"
        total_field = "__fpstreams_group_value_sum"
        if grouped_names.count(key_field) != 1 or grouped_names.count(total_field) != 1:
            return _reuse_eager_pandas_group_table(table, node, native_data, hooks)
        keys = grouped.column(grouped_names.index(key_field)).to_pylist()
        totals = grouped.column(grouped_names.index(total_field)).to_pylist()
        encounter_order = encounter_order_values.to_pylist()
    except (ArithmeticError, NotImplementedError, TypeError, ValueError):
        return _reuse_eager_pandas_group_table(table, node, native_data, hooks)

    totals_by_key = dict(zip(keys, totals, strict=True))
    keys = encounter_order
    totals = [totals_by_key[key] for key in keys]
    key_name = node.key_names[0]
    if not wide_totals:
        return [
            {key_name: key, spec.output_name: total}
            for key, total in zip(keys, totals, strict=True)
        ]
    result: list[dict[str, Any]] = []
    for key, total in zip(keys, totals, strict=True):
        row = {key_name: key}
        row[spec.output_name] = int(total)
        result.append(row)
    return result
