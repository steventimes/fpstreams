"""Global Arrow aggregation execution and materialization."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any

from ...collecting.aggregation import _MISSING
from ...collecting.program import (
    CollectorState,
    collector_lifecycle_is_current,
    collector_lifecycle_revisions,
    consume_collector_program,
    finish_collector_program,
    run_collector_program,
)
from ...physical.plan import PhysicalNode, PhysicalPlan
from ...physical.relational import (
    ArrowGlobalAggregateSpec,
    ArrowGlobalSumSpec,
    GlobalAggregatePhysicalNode,
    PipelinePhysicalNode,
    SourcePhysicalNode,
)
from ...planning.arrow import ArrowPrefixPlan, plan_arrow_table_prefix
from ...planning.arrow_source import ArrowBatchSource, batch_to_rows
from ...planning.logical import Pipeline
from ...runtime.iterators import close_iterators

ModuleImporter = Callable[[str], Any]
ArrowPlanningOperations = Callable[[tuple[PhysicalNode, ...]], tuple[Any, ...] | None]
ArrowGroupTable = Callable[[Any, ArrowBatchSource], Any | None]
ArrowTableReducer = Callable[[Any, ArrowGlobalSumSpec, Any, Any], dict[str, Any] | None]


@dataclass(frozen=True, slots=True)
class ArrowGlobalHooks:
    """Live owner-module dependencies whose replacement must remain observable."""

    import_module: ModuleImporter
    arrow_planning_operations: ArrowPlanningOperations
    arrow_group_table: ArrowGroupTable
    reduce_arrow_table: ArrowTableReducer


def _arrow_group_table(pa: Any, descriptor: ArrowBatchSource) -> Any | None:
    """Open one reusable columnar aggregate input without adapter batch slicing."""
    retained = descriptor.materialized_data
    if descriptor.kind == "table" and isinstance(retained, pa.Table):
        return retained
    if descriptor.kind == "record_batch" and isinstance(retained, pa.RecordBatch):
        try:
            return pa.Table.from_batches([retained], schema=descriptor.schema_hint)
        except (TypeError, ValueError):
            return None
    if descriptor.columnar_opener is not None:
        table = descriptor.columnar_opener()
        return table if isinstance(table, pa.Table) else None

    batches = descriptor.open_batches()
    active_error: BaseException | None = None
    try:
        materialized_batches = list(batches)
    except BaseException as error:
        active_error = error
        raise
    finally:
        close_iterators((batches,), active_error=active_error)
    try:
        return pa.Table.from_batches(materialized_batches, schema=descriptor.schema_hint)
    except (TypeError, ValueError):
        return None


def _try_arrow_global_count_impl(
    node: GlobalAggregatePhysicalNode,
    hooks: ArrowGlobalHooks,
) -> dict[str, Any] | None:
    """Count one direct Arrow source or a complete table-safe Arrow prefix."""
    name = node.arrow_count_name
    if name is None:
        return None
    if isinstance(node.input, PipelinePhysicalNode):
        pipeline_input = node.input
        if not isinstance(pipeline_input.input, SourcePhysicalNode):
            return None
        source = pipeline_input.input.source
        native_data = source.native_data
        if (
            not source.capabilities.reiterable
            or not isinstance(native_data, ArrowBatchSource)
            or native_data.kind not in {"table", "record_batch"}
            or native_data.materialized_data is None
        ):
            return None
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
        from ..arrow import (
            retained_table_rows_are_canonical,
            try_arrow_count,
            try_validated_retained_arrow_table,
        )

        pa = hooks.import_module("pyarrow")
        raw_table = hooks.arrow_group_table(pa, native_data)
        if raw_table is None or not retained_table_rows_are_canonical(raw_table):
            return None
        whole_handled, whole_table = try_validated_retained_arrow_table(
            pipeline,
            prefix=prefix,
        )
        if whole_handled:
            if whole_table is None:
                raise RuntimeError("handled retained Arrow prefix returned no table")
            return {name: int(whole_table.num_rows)}
        handled, count = try_arrow_count(pipeline, prefix=prefix)
        return {name: count} if handled else None
    if (
        not isinstance(node.input, SourcePhysicalNode)
        or not node.input.source.capabilities.reiterable
    ):
        return None
    source = node.input.source
    native_data = source.native_data
    if not isinstance(native_data, ArrowBatchSource) or native_data.count_opener is None:
        return None
    from ...tabular.arrow import guarded_parquet_count_opener

    count_opener = guarded_parquet_count_opener(source)
    if count_opener is None:
        return None
    program = node.aggregations.collectors
    state = program.initialize()
    revisions = collector_lifecycle_revisions(program)
    descriptor = source.open_native(ArrowBatchSource)
    if descriptor.count_opener is not count_opener:
        return finish_collector_program(source.open(), program, state)
    count = count_opener()
    if count is not None and collector_lifecycle_is_current(program, revisions):
        return {name: count}
    return finish_collector_program(source.open(), program, state)


def _observe_arrow_batch_rows(batch: Any) -> None:
    """Preserve the source's canonical row-conversion boundary for a consumed batch."""
    from ..arrow import observe_arrow_batch_rows

    observe_arrow_batch_rows(batch)


def _consume_claimed_arrow_batches(
    batches: Iterator[Any],
    program: Any,
    state: CollectorState,
    *,
    first_batch: Any = _MISSING,
) -> None:
    """Continue a claimed batch stream as canonical rows without reopening its source."""
    from ...tabular.arrow import batch_to_rows as canonical_batch_to_rows

    def rows() -> Iterator[dict[str, Any]]:
        if first_batch is not _MISSING:
            yield from canonical_batch_to_rows(first_batch)
        for batch in batches:
            yield from canonical_batch_to_rows(batch)

    consume_collector_program(rows(), program, state)


def _arrow_batches_first(batches: Iterator[Any], field_index: int) -> Any:
    """Return the first selected scalar, stopping before any later batch pull."""
    for batch in batches:
        _observe_arrow_batch_rows(batch)
        if batch.num_rows:
            return batch.column(field_index)[0].as_py()
    return None


def _arrow_batches_last(batches: Iterator[Any], field_index: int) -> Any:
    """Return the final selected scalar after exhausting all batches."""
    result: Any = _MISSING
    for batch in batches:
        _observe_arrow_batch_rows(batch)
        if batch.num_rows:
            result = batch.column(field_index)[batch.num_rows - 1].as_py()
    return None if result is _MISSING else result


def _reduce_arrow_batches(
    batches: Iterator[Any],
    field_index: int,
    spec: ArrowGlobalSumSpec,
    pa: Any,
    pc: Any,
) -> Any:
    """Reduce consumed Arrow batches while retaining canonical batch boundaries."""
    match spec.kind:
        case "first":
            result = _arrow_batches_first(batches, field_index)
        case "last":
            result = _arrow_batches_last(batches, field_index)
        case "min" | "max" | "sum":
            from ..arrow import reduce_arrow_i64_batches

            result = reduce_arrow_i64_batches(batches, field_index, spec.kind, pa, pc).value
    return result


def _try_arrow_reader_global_reduction(  # noqa: C901 - claimed-reader lifecycle handoff
    node: GlobalAggregatePhysicalNode,
    descriptor: ArrowBatchSource,
    pa: Any,
    pc: Any,
) -> dict[str, Any] | None:
    """Consume one claimed Arrow reader without constructing row dictionaries."""
    spec = node.arrow_i64_sum
    if not isinstance(spec, ArrowGlobalSumSpec) or not isinstance(node.input, SourcePhysicalNode):
        return None
    schema = descriptor.schema_hint
    if schema is None:
        return None
    from ..arrow import (
        arrow_i64_extreme,
        arrow_i64_sum,
        arrow_schema_has_primitive_rows,
        observe_arrow_batch_rows,
    )

    if not arrow_schema_has_primitive_rows(pa, schema):
        return None
    names = schema.names
    if spec.value_field not in names:
        return None
    field_index = names.index(spec.value_field)
    if not pa.types.is_int64(schema.field(field_index).type):
        return None

    program = node.aggregations.collectors
    state = program.initialize()
    revisions = collector_lifecycle_revisions(program)
    node.input.source.open_native(ArrowBatchSource)
    batches = descriptor.open_batches()
    current = state.values[0]
    continued_as_rows = False
    active_error: BaseException | None = None
    try:
        while True:
            try:
                batch = next(batches)
            except StopIteration:
                break
            if not collector_lifecycle_is_current(program, revisions):
                state.values[0] = current
                _consume_claimed_arrow_batches(
                    batches,
                    program,
                    state,
                    first_batch=batch,
                )
                continued_as_rows = True
                break
            observe_arrow_batch_rows(batch)
            row_count = int(batch.num_rows)
            if not row_count:
                continue
            values = batch.column(field_index)
            match spec.kind:
                case "sum":
                    current += arrow_i64_sum(values, row_count, pa, pc)
                case "min" | "max":
                    current = arrow_i64_extreme(
                        current,
                        values,
                        spec.kind,
                        pc,
                        missing=_MISSING,
                    )
                case "first":
                    current = values[0].as_py()
                    break
                case "last":
                    current = values[row_count - 1].as_py()
    except BaseException as error:
        active_error = error
        raise
    finally:
        close_iterators((batches,), active_error=active_error)

    if continued_as_rows:
        return program.finish(state)
    if not collector_lifecycle_is_current(program, revisions):
        state.values[0] = current
        return program.finish(state)
    result = None if current is _MISSING else current
    return {spec.output_name: result}


def _reduce_arrow_table_impl(
    table: Any,
    spec: ArrowGlobalSumSpec,
    pa: Any,
    pc: Any,
) -> dict[str, Any] | None:
    """Reduce a retained Arrow table after its source-level guards have passed."""
    from ..arrow import arrow_i64_extreme, arrow_i64_sum

    if table.num_rows == 0:
        identity = 0 if spec.kind == "sum" else None
        return {spec.output_name: identity}
    names = table.schema.names
    if names.count(spec.value_field) != 1:
        return None
    values = table.column(names.index(spec.value_field))
    if not pa.types.is_int64(values.type):
        return None

    match spec.kind:
        case "first":
            result = values[0].as_py()
        case "last":
            result = values[table.num_rows - 1].as_py()
        case "min" | "max":
            if values.null_count:
                result = arrow_i64_extreme(
                    _MISSING,
                    values,
                    spec.kind,
                    pc,
                    missing=_MISSING,
                )
            else:
                try:
                    result = arrow_i64_extreme(
                        _MISSING,
                        values,
                        spec.kind,
                        pc,
                        missing=_MISSING,
                    )
                except (ArithmeticError, NotImplementedError, TypeError, ValueError):
                    return None
        case "sum":
            if values.null_count:
                return None
            try:
                result = arrow_i64_sum(values, table.num_rows, pa, pc)
            except (ArithmeticError, NotImplementedError, TypeError, ValueError):
                return None
    return {spec.output_name: result}


def _reduce_arrow_global_multi_table(
    table: Any,
    spec: ArrowGlobalAggregateSpec,
    pa: Any,
    pc: Any,
) -> dict[str, Any] | None:
    """Reduce closed lanes over one validated Arrow table, caching duplicate work."""
    if table.num_rows == 0:
        return {
            lane.output_name: 0 if lane.kind in {"count", "sum"} else None for lane in spec.lanes
        }
    from ..arrow import retained_table_rows_are_valid

    if not retained_table_rows_are_valid(table):
        return None
    names = tuple(table.schema.names)
    columns: dict[str, Any] = {}
    requirements: dict[str, set[str]] = {}
    for lane in spec.lanes:
        value_field = lane.value_field
        if lane.kind == "count":
            continue
        if value_field is None or names.count(value_field) != 1:
            return None
        values = columns.setdefault(value_field, table.column(names.index(value_field)))
        if not pa.types.is_int64(values.type) or values.null_count:
            return None
        requirements.setdefault(value_field, set()).add(lane.kind)

    from ..arrow import arrow_i64_sum

    reduced_values: dict[tuple[str, str], Any] = {}
    for value_field, kinds in requirements.items():
        values = columns[value_field]
        try:
            bounds = pc.min_max(values).as_py()
            if "sum" in kinds:
                reduced_values[("sum", value_field)] = arrow_i64_sum(
                    values,
                    table.num_rows,
                    pa,
                    pc,
                    bounds=bounds,
                )
            if "min" in kinds:
                reduced_values[("min", value_field)] = bounds["min"]
            if "max" in kinds:
                reduced_values[("max", value_field)] = bounds["max"]
        except (ArithmeticError, NotImplementedError, TypeError, ValueError):
            return None
    result: dict[str, Any] = {}
    for lane in spec.lanes:
        if lane.kind == "count":
            result[lane.output_name] = int(table.num_rows)
            continue
        value_field = lane.value_field
        if value_field is None:
            return None
        result[lane.output_name] = reduced_values[(lane.kind, value_field)]
    return result


def _fold_arrow_global_multi_rows(
    columns: dict[str, list[Any]],
    row_count: int,
    lanes: tuple[tuple[str, str | None], ...],
    states: dict[tuple[str, str | None], Any],
) -> None:
    """Fold one nullable reader batch in canonical row-major lane order."""
    for position in range(row_count):
        for lane in lanes:
            kind, value_field = lane
            current = states.get(lane, _MISSING)
            if kind == "count":
                states[lane] = 1 if current is _MISSING else current + 1
                continue
            assert value_field is not None
            selected = columns[value_field][position]
            if kind == "sum":
                states[lane] = (0 if current is _MISSING else current) + selected
            elif kind == "min":
                if current is _MISSING or selected < current:
                    states[lane] = selected
            elif current is _MISSING or selected > current:
                states[lane] = selected


def _arrow_global_multi_batch_partials(
    batch: Any,
    field_indexes: dict[str, int],
    requirements: dict[str, set[str]],
    lanes: tuple[tuple[str, str | None], ...],
    pa: Any,
    pc: Any,
) -> dict[tuple[str, str | None], Any] | None:
    """Compute one non-null batch transactionally, declining to its row loop on failure."""
    from ..arrow import arrow_i64_sum

    row_count = int(batch.num_rows)
    partials: dict[tuple[str, str | None], Any] = {}
    try:
        for kind, value_field in lanes:
            if kind == "count":
                partials[(kind, value_field)] = row_count
        for value_field, kinds in requirements.items():
            values = batch.column(field_indexes[value_field])
            bounds = pc.min_max(values).as_py()
            if "sum" in kinds:
                partials[("sum", value_field)] = arrow_i64_sum(
                    values,
                    row_count,
                    pa,
                    pc,
                    bounds=bounds,
                )
            if "min" in kinds:
                partials[("min", value_field)] = bounds["min"]
            if "max" in kinds:
                partials[("max", value_field)] = bounds["max"]
    except MemoryError:
        raise
    except (ArithmeticError, NotImplementedError, TypeError, ValueError):
        return None
    return partials


def _merge_arrow_global_multi_partials(
    states: dict[tuple[str, str | None], Any],
    partials: dict[tuple[str, str | None], Any],
) -> None:
    """Merge one committed Arrow batch into unbounded Python aggregate states."""
    for lane, value in partials.items():
        kind = lane[0]
        current = states.get(lane, _MISSING)
        if kind in {"count", "sum"}:
            states[lane] = value if current is _MISSING else current + value
        elif kind == "min":
            states[lane] = value if current is _MISSING or value < current else current
        else:
            states[lane] = value if current is _MISSING or value > current else current


def _try_arrow_reader_global_multi_aggregate(  # noqa: C901 - batch/lifecycle handoff
    node: GlobalAggregatePhysicalNode,
    descriptor: ArrowBatchSource,
    spec: ArrowGlobalAggregateSpec,
    outer_plan: PhysicalPlan | None,
    pa: Any,
    pc: Any,
) -> dict[str, Any] | None:
    """Reduce one claimed Arrow reader batch-wise without constructing row dictionaries."""
    if not isinstance(node.input, SourcePhysicalNode) or descriptor.kind != "reader":
        return None
    schema = descriptor.schema_hint
    if schema is None:
        return None
    from ..arrow import arrow_schema_has_primitive_rows, observe_arrow_batch_rows

    if not arrow_schema_has_primitive_rows(pa, schema):
        return None
    names = tuple(schema.names)
    field_indexes: dict[str, int] = {}
    requirements: dict[str, set[str]] = {}
    for lane in spec.lanes:
        value_field = lane.value_field
        if lane.kind == "count":
            continue
        if value_field is None or names.count(value_field) != 1:
            return None
        field_index = names.index(value_field)
        if not pa.types.is_int64(schema.field(field_index).type):
            return None
        field_indexes[value_field] = field_index
        requirements.setdefault(value_field, set()).add(lane.kind)

    unique_lanes = tuple(dict.fromkeys((lane.kind, lane.value_field) for lane in spec.lanes))
    program = node.aggregations.collectors
    state = program.initialize()
    revisions = collector_lifecycle_revisions(program)
    states: dict[tuple[str, str | None], Any] = {}
    node.input.source.open_native(ArrowBatchSource)
    batches = descriptor.open_batches()
    continued_as_rows = False
    active_error: BaseException | None = None
    try:
        while True:
            try:
                batch = next(batches)
            except StopIteration:
                break
            if not collector_lifecycle_is_current(program, revisions):
                for index, lane in enumerate(spec.lanes):
                    state.values[index] = states.get(
                        (lane.kind, lane.value_field),
                        state.values[index],
                    )
                _consume_claimed_arrow_batches(
                    batches,
                    program,
                    state,
                    first_batch=batch,
                )
                continued_as_rows = True
                break
            observe_arrow_batch_rows(batch)
            row_count = int(batch.num_rows)
            if not row_count:
                continue
            selected = {field: batch.column(index) for field, index in field_indexes.items()}
            if any(values.null_count for values in selected.values()):
                _fold_arrow_global_multi_rows(
                    {field: values.to_pylist() for field, values in selected.items()},
                    row_count,
                    unique_lanes,
                    states,
                )
                continue
            partials = _arrow_global_multi_batch_partials(
                batch,
                field_indexes,
                requirements,
                unique_lanes,
                pa,
                pc,
            )
            if partials is None:
                _fold_arrow_global_multi_rows(
                    {field: values.to_pylist() for field, values in selected.items()},
                    row_count,
                    unique_lanes,
                    states,
                )
            else:
                _merge_arrow_global_multi_partials(states, partials)
    except BaseException as error:
        active_error = error
        raise
    finally:
        close_iterators((batches,), active_error=active_error)

    if continued_as_rows:
        return program.finish(state)
    if not collector_lifecycle_is_current(program, revisions):
        for index, lane in enumerate(spec.lanes):
            state.values[index] = states.get(
                (lane.kind, lane.value_field),
                state.values[index],
            )
        return program.finish(state)

    result: dict[str, Any] = {}
    for lane in spec.lanes:
        key = (lane.kind, lane.value_field)
        value = states.get(key, _MISSING)
        if value is _MISSING:
            value = 0 if lane.kind in {"count", "sum"} else None
        result[lane.output_name] = value
    from ...runtime.report import _record_direct_strategy

    _record_direct_strategy(
        outer_plan,
        "arrow_direct",
        "one-shot Arrow batches supplied global aggregation without row boxing",
    )
    return result


def _try_arrow_global_multi_aggregate_impl(
    node: GlobalAggregatePhysicalNode,
    spec: ArrowGlobalAggregateSpec,
    outer_plan: PhysicalPlan | None,
    hooks: ArrowGlobalHooks,
) -> dict[str, Any] | None:
    """Open one reusable table or one-shot reader and reduce all closed global lanes."""
    if not isinstance(node.input, SourcePhysicalNode):
        return None
    native_data = node.input.source.native_data
    if not isinstance(native_data, ArrowBatchSource):
        return None
    pa = hooks.import_module("pyarrow")
    pc = hooks.import_module("pyarrow.compute")
    if native_data.kind == "reader":
        return _try_arrow_reader_global_multi_aggregate(
            node,
            native_data,
            spec,
            outer_plan,
            pa,
            pc,
        )
    if not node.input.source.capabilities.reiterable:
        return None
    retained = native_data.kind in {"table", "record_batch"}
    reopened_columnar = native_data.kind in {"dataframe", "polars"} and (
        native_data.columnar_opener is not None
    )
    if not retained and not reopened_columnar:
        return None

    program = node.aggregations.collectors
    state = program.initialize() if reopened_columnar else None
    revisions = collector_lifecycle_revisions(program) if state is not None else None
    descriptor = node.input.source.open_native(ArrowBatchSource)
    table = hooks.arrow_group_table(pa, descriptor)
    if table is None:
        return None
    if (
        state is not None
        and revisions is not None
        and not collector_lifecycle_is_current(
            program,
            revisions,
        )
    ):
        return _reduce_eager_global_table(
            table,
            node,
            (),
            descriptor,
            state=state,
            full_rows=True,
        )
    result = _reduce_arrow_global_multi_table(table, spec, pa, pc)
    if (
        state is not None
        and revisions is not None
        and not collector_lifecycle_is_current(
            program,
            revisions,
        )
    ):
        return _reduce_eager_global_table(
            table,
            node,
            (),
            descriptor,
            state=state,
            full_rows=True,
        )
    if result is not None:
        from ...runtime.report import _record_direct_strategy

        _record_direct_strategy(
            outer_plan,
            "arrow_direct",
            "retained Arrow columns supplied global aggregation without row boxing",
        )
        return result
    fields = tuple(lane.value_field for lane in spec.lanes if lane.value_field is not None)
    return _reduce_eager_global_table(table, node, fields, descriptor, state=state)


def _reduce_eager_global_table(
    table: Any,
    node: GlobalAggregatePhysicalNode,
    fields: tuple[str, ...],
    descriptor: ArrowBatchSource,
    *,
    state: CollectorState | None = None,
    full_rows: bool = False,
) -> dict[str, Any] | None:
    """Reuse one eager conversion for a canonical collector fallback."""
    if descriptor.kind not in {"dataframe", "polars"} or descriptor.columnar_opener is None:
        return None
    names = tuple(table.schema.names)
    from ..arrow import retained_table_rows_are_valid

    selected_fields = tuple(dict.fromkeys(fields))
    if full_rows:
        collector_table = table
    elif any(names.count(field) != 1 for field in selected_fields):
        if descriptor.kind != "dataframe":
            return None
        collector_table = table
    elif retained_table_rows_are_valid(table):
        collector_table = table.select(selected_fields)
    elif descriptor.kind == "dataframe":
        # The pandas adapter's canonical path is this same Arrow conversion followed by
        # bounded full-row boxing. Reuse it even for nested schemas or conversion errors.
        collector_table = table
    else:
        # Polars owns distinct to_dicts semantics for nonprimitive and invalid values.
        return None
    from ...tabular.arrow import batch_to_rows as canonical_batch_to_rows

    def selected_rows() -> Iterator[dict[str, Any]]:
        for batch in collector_table.to_batches(max_chunksize=descriptor.batch_size):
            yield from canonical_batch_to_rows(batch)

    program = node.aggregations.collectors
    if state is None:
        return run_collector_program(selected_rows(), program)
    return finish_collector_program(selected_rows(), program, state)


def _reduce_eager_selected_table(
    table: Any,
    node: GlobalAggregatePhysicalNode,
    spec: ArrowGlobalSumSpec,
    descriptor: ArrowBatchSource,
    *,
    state: CollectorState | None = None,
) -> dict[str, Any] | None:
    """Reuse one eager conversion for the existing single-lane fallback."""
    return _reduce_eager_global_table(
        table,
        node,
        (spec.value_field,),
        descriptor,
        state=state,
    )


def _source_arrow_field(field: str, prefix: ArrowPrefixPlan) -> str | None:
    """Map one projected output field back to its retained source column."""
    projection = prefix.projection
    if projection is None:
        return field
    return next(
        (source for output, source in projection.selectors if output == field),
        None,
    )


def _arrow_pipeline_reduction_is_total(
    table: Any,
    spec: ArrowGlobalSumSpec,
    prefix: ArrowPrefixPlan,
    pa: Any,
) -> bool:
    """Prove a full prefix cannot overtake an earlier scalar-reduction error."""
    if table.num_rows == 0:
        return True
    source_field = _source_arrow_field(spec.value_field, prefix)
    if source_field is None:
        return False
    names = tuple(table.schema.names)
    if names.count(source_field) != 1:
        return False
    values = table.column(names.index(source_field))
    return bool(pa.types.is_int64(values.type) and values.null_count == 0)


def _try_arrow_global_reduction_impl(  # noqa: C901 - guarded source/backend dispatch
    node: GlobalAggregatePhysicalNode,
    outer_plan: PhysicalPlan | None,
    hooks: ArrowGlobalHooks,
) -> dict[str, Any] | None:
    """Reduce proven Arrow lanes without materializing their input rows."""
    spec = node.arrow_i64_sum
    if spec is None:
        return None
    if isinstance(spec, ArrowGlobalAggregateSpec):
        return _try_arrow_global_multi_aggregate_impl(node, spec, outer_plan, hooks)
    if isinstance(node.input, PipelinePhysicalNode):
        pipeline_input = node.input
        if spec.kind == "first" or not isinstance(pipeline_input.input, SourcePhysicalNode):
            return None
        source = pipeline_input.input.source
        native_data = source.native_data
        if (
            not source.capabilities.reiterable
            or not isinstance(native_data, ArrowBatchSource)
            or native_data.kind not in {"table", "record_batch"}
            or native_data.materialized_data is None
        ):
            return None
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
        if raw_table is None or not _arrow_pipeline_reduction_is_total(raw_table, spec, prefix, pa):
            return None
        from ..arrow import (
            retained_table_rows_are_canonical,
            try_arrow_table,
            try_validated_retained_arrow_table,
        )

        if not retained_table_rows_are_canonical(raw_table):
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
        reduced = hooks.reduce_arrow_table(table, spec, pa, pc)
        if reduced is not None:
            return reduced
        return run_collector_program(iter(batch_to_rows(table)), node.aggregations.collectors)
    if not isinstance(node.input, SourcePhysicalNode):
        return None
    native_data = node.input.source.native_data
    if not isinstance(native_data, ArrowBatchSource) or native_data.kind not in {
        "table",
        "record_batch",
        "reader",
        "dataframe",
        "polars",
    }:
        return None

    pa = hooks.import_module("pyarrow")
    pc = hooks.import_module("pyarrow.compute")
    if native_data.kind == "reader":
        return _try_arrow_reader_global_reduction(node, native_data, pa, pc)
    if not node.input.source.capabilities.reiterable:
        return None
    reopened_columnar = native_data.kind in {"dataframe", "polars"} and (
        native_data.columnar_opener is not None
    )
    program = node.aggregations.collectors
    state = program.initialize() if reopened_columnar else None
    revisions = collector_lifecycle_revisions(program) if state is not None else None
    descriptor = node.input.source.open_native(ArrowBatchSource)
    table = hooks.arrow_group_table(pa, descriptor)
    if table is None:
        return None
    if (
        state is not None
        and revisions is not None
        and not collector_lifecycle_is_current(
            program,
            revisions,
        )
    ):
        return _reduce_eager_global_table(
            table,
            node,
            (),
            descriptor,
            state=state,
            full_rows=True,
        )
    if table.num_rows == 0:
        identity = 0 if spec.kind == "sum" else None
        return {spec.output_name: identity}
    names = tuple(table.schema.names)
    if names.count(spec.value_field) != 1:
        return _reduce_eager_selected_table(table, node, spec, descriptor, state=state)
    field_index = names.index(spec.value_field)
    from ..arrow import arrow_schema_has_primitive_rows

    if arrow_schema_has_primitive_rows(pa, table.schema) and pa.types.is_int64(
        table.schema.field(field_index).type
    ):
        batches = iter(table.to_batches(max_chunksize=native_data.batch_size))
        selected_values = table.column(field_index)
        if selected_values.null_count and spec.kind in {"sum", "min", "max"}:
            result = _reduce_arrow_batches(batches, field_index, spec, pa, pc)
            if (
                state is not None
                and revisions is not None
                and not collector_lifecycle_is_current(program, revisions)
            ):
                return _reduce_eager_global_table(
                    table,
                    node,
                    (),
                    descriptor,
                    state=state,
                    full_rows=True,
                )
            return {spec.output_name: result}
        try:
            result = _reduce_arrow_batches(batches, field_index, spec, pa, pc)
        except (ArithmeticError, NotImplementedError, TypeError, ValueError):
            pass
        else:
            if (
                state is not None
                and revisions is not None
                and not collector_lifecycle_is_current(program, revisions)
            ):
                return _reduce_eager_global_table(
                    table,
                    node,
                    (),
                    descriptor,
                    state=state,
                    full_rows=True,
                )
            return {spec.output_name: result}
    if (
        state is not None
        and revisions is not None
        and not collector_lifecycle_is_current(
            program,
            revisions,
        )
    ):
        return _reduce_eager_global_table(
            table,
            node,
            (),
            descriptor,
            state=state,
            full_rows=True,
        )
    return _reduce_eager_selected_table(table, node, spec, descriptor, state=state)
