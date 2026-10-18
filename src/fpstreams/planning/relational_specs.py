"""Source-safe backend candidate recognition for relational plans."""

from __future__ import annotations

from typing import TypeGuard, cast

from ..collecting.aggregation import (
    AggregationItems,
    native_group_aggregation,
    project_count_aggregation,
)
from ..expressions.selectors import (
    _direct_field,
    _normalize_direct_row_selector,
    compile_selector,
)
from ..physical.plan import (
    CompiledExpressionPhysicalNode,
    RowPhysicalNode,
    SortPhysicalNode,
)
from ..physical.relational import (
    ArrowGlobalAggregateSpec,
    ArrowGlobalSumSpec,
    ArrowGroupAggregateSpec,
    ArrowGroupLaneSpec,
    ArrowGroupSumSpec,
    ArrowUniqueJoinSpec,
    ClosedGroupSpec,
    CompiledJoinSpec,
    CompositeCountSumSpec,
    GroupLane,
    NativeFixedI64GroupSpec,
    NativeGlobalI64AggregateSpec,
    NativeGlobalI64LaneSpec,
    NativeGroupSumSpec,
    NativeMultiI64GroupLaneSpec,
    NativeMultiI64GroupSpec,
    NativePairI64ExprGroupSumSpec,
    NativeRecordGlobalSumSpec,
    NativeRecordGroupSumSpec,
    NativeRecordJoinSpec,
    NumpyGlobalAggregateSpec,
    NumpyGlobalLaneSpec,
    NumpyGroupAggregateSpec,
    NumpyGroupLaneSpec,
    PhysicalRelNode,
    PipelinePhysicalNode,
    SimpleGroupSumSpec,
    SourcePhysicalNode,
    SpillCountSpec,
)
from .arrow import (
    plan_arrow_table_prefix,
)
from .arrow_source import ArrowBatchSource
from .logical import (
    GroupAggregateSpec,
    JoinNode,
    LogicalNode,
    Pipeline,
    Query,
)
from .numpy import NumpyColumnSource, NumpyPrefixPlan, plan_numpy_prefix
from .pair_i64_expression import lower_pair_i64_group_key, lower_pair_i64_group_value
from .source import (
    _CANONICAL_RETAINED_SEQUENCE,
    _CANONICAL_SOURCE_NATIVE_DATA,
    Source,
)
from .sync import Operation

_ARROW_UNIQUE_JOIN_MIN_ROWS = 128
_NATIVE_PAIR_I64_EXPR_GROUP_MIN_ROWS = 32
_NATIVE_GLOBAL_I64_MIN_ROWS = 128
_NUMPY_GLOBAL_MIN_ROWS = 32


def _is_exact_int(value: object) -> TypeGuard[int]:
    """Narrow an exact integer while rejecting ``bool`` and custom integer subclasses."""
    return type(value) is int


def _is_exact_field_selector(value: object) -> TypeGuard[str | int]:
    """Accept only built-in field/index selectors, excluding bool and subclasses."""
    return type(value) is str or type(value) is int


def _simple_group_sum_spec(spec: GroupAggregateSpec) -> SimpleGroupSumSpec | None:
    """Recognize one non-spilling selected sum without evaluating either selector."""
    if spec.partitions is not None or len(spec.keys) != 1 or len(spec.aggregations) != 1:
        return None
    _key_name, raw_key_selector = spec.keys[0]
    key_selector = _normalize_direct_row_selector(raw_key_selector)
    output_name, aggregation = spec.aggregations[0]
    native = native_group_aggregation(aggregation)
    if native is None or native.kind != "sum" or native.selector is None:
        return None
    value_selector = _normalize_direct_row_selector(native.selector)
    if not (callable(key_selector) or _is_exact_field_selector(key_selector)) or not (
        callable(value_selector) or _is_exact_field_selector(value_selector)
    ):
        return None
    return SimpleGroupSumSpec(
        key_selector,
        value_selector,
        compile_selector(value_selector),
        output_name,
    )


def _closed_group_spec(spec: GroupAggregateSpec) -> ClosedGroupSpec | None:
    """Compile trusted fixed-size grouped aggregations into contiguous state lanes."""
    if spec.partitions is not None or len(spec.keys) != 1:
        return None
    _key_name, raw_key_selector = spec.keys[0]
    key_selector = _normalize_direct_row_selector(raw_key_selector)
    if not (callable(key_selector) or _is_exact_field_selector(key_selector)):
        return None

    lanes: list[GroupLane] = []
    for output_name, aggregation in spec.aggregations:
        if project_count_aggregation(aggregation):
            lanes.append(GroupLane(output_name, "count", None, None))
            continue
        native = native_group_aggregation(aggregation)
        # ``mean`` carries a marker so direct columnar global aggregation can prove its
        # lifecycle, but grouped mean still requires the ordinary collector state.
        if native is None or native.kind == "mean":
            return None
        selector = (
            None if native.selector is None else _normalize_direct_row_selector(native.selector)
        )
        if selector is not None and not (callable(selector) or _is_exact_field_selector(selector)):
            return None
        lanes.append(
            GroupLane(
                output_name,
                native.kind,
                selector,
                None if selector is None else compile_selector(selector),
            )
        )
    return ClosedGroupSpec(key_selector, tuple(lanes))


def _composite_count_sum_spec(spec: GroupAggregateSpec) -> CompositeCountSumSpec | None:
    """Recognize two direct keys with the common count-then-sum aggregation shape."""
    if spec.partitions is not None or len(spec.keys) != 2 or len(spec.aggregations) != 2:
        return None
    key_selectors = tuple(_normalize_direct_row_selector(selector) for _name, selector in spec.keys)
    if any(
        not _is_exact_field_selector(selector) or (type(selector) is str and "." in selector)
        for selector in key_selectors
    ):
        return None
    count_name, count_aggregation = spec.aggregations[0]
    sum_name, sum_aggregation = spec.aggregations[1]
    native_sum = native_group_aggregation(sum_aggregation)
    if native_sum is None or native_sum.kind != "sum" or native_sum.selector is None:
        return None
    value_selector = _normalize_direct_row_selector(native_sum.selector)
    if (
        not project_count_aggregation(count_aggregation)
        or not _is_exact_field_selector(value_selector)
        or (type(value_selector) is str and "." in value_selector)
    ):
        return None
    direct_keys = cast(tuple[str | int, str | int], key_selectors)
    return CompositeCountSumSpec(
        direct_keys,
        value_selector,
        compile_selector(value_selector),
        count_name,
        sum_name,
    )


def _native_fixed_i64_group_spec(
    spec: ClosedGroupSpec | None,
) -> NativeFixedI64GroupSpec | NativeMultiI64GroupSpec | None:
    """Narrow exact direct lanes to the fixed or generic i64 record ABI."""
    if spec is None or not _is_exact_field_selector(spec.key_selector):
        return None
    key_selector = spec.key_selector
    if type(key_selector) is str and "." in key_selector:
        return None
    signature = tuple(lane.kind for lane in spec.lanes)
    if signature == ("count",):
        count_lane = spec.lanes[0]
        if type(count_lane.output_name) is not str:
            return None
        return NativeFixedI64GroupSpec(
            "tuple" if type(key_selector) is int else "dict",
            key_selector,
            None,
            count_lane.output_name,
            None,
        )
    if signature == ("count", "sum"):
        count_lane, sum_lane = spec.lanes
        value_selector = sum_lane.selector
        if (
            type(count_lane.output_name) is str
            and type(sum_lane.output_name) is str
            and _is_exact_field_selector(value_selector)
            and type(value_selector) is type(key_selector)
            and not (type(value_selector) is str and "." in value_selector)
        ):
            return NativeFixedI64GroupSpec(
                "tuple" if type(key_selector) is int else "dict",
                key_selector,
                value_selector,
                count_lane.output_name,
                sum_lane.output_name,
            )

    # The existing single-sum ABI is narrower and faster than the generic lane engine.
    if not spec.lanes or signature == ("sum",):
        return None
    lanes: list[NativeMultiI64GroupLaneSpec] = []
    for lane in spec.lanes:
        if type(lane.output_name) is not str:
            return None
        if lane.kind == "count" and lane.selector is None:
            lanes.append(NativeMultiI64GroupLaneSpec(lane.output_name, "count", None))
            continue
        value_selector = lane.selector
        if (
            lane.kind not in {"sum", "min", "max"}
            or not _is_exact_field_selector(value_selector)
            or type(value_selector) is not type(key_selector)
            or (type(value_selector) is str and "." in value_selector)
        ):
            return None
        lanes.append(NativeMultiI64GroupLaneSpec(lane.output_name, lane.kind, value_selector))
    return NativeMultiI64GroupSpec(
        "tuple" if type(key_selector) is int else "dict",
        key_selector,
        tuple(lanes),
    )


def _spill_count_spec(spec: GroupAggregateSpec) -> SpillCountSpec | None:
    """Recognize the one closed count shape whose partial state is proven mergeable."""
    if spec.partitions is None or len(spec.keys) != 1 or len(spec.aggregations) != 1:
        return None
    _key_name, raw_key_selector = spec.keys[0]
    key_selector = _normalize_direct_row_selector(raw_key_selector)
    output_name, aggregation = spec.aggregations[0]
    if (
        type(key_selector) is not str
        or type(output_name) is not str
        or "." in key_selector
        or not project_count_aggregation(aggregation)
    ):
        return None
    return SpillCountSpec(key_selector, output_name)


def _native_group_sum_spec(
    spec: SimpleGroupSumSpec | None,
) -> NativeGroupSumSpec | None:
    """Narrow a simple group sum to the existing exact index-based i64 kernel."""
    if spec is None or not _is_exact_int(spec.key_selector):
        return None
    if not _is_exact_int(spec.value_selector):
        return None
    return NativeGroupSumSpec(spec.key_selector, spec.value_selector, spec.output_name)


def _native_record_group_sum_spec(
    spec: SimpleGroupSumSpec | None,
) -> NativeRecordGroupSumSpec | None:
    """Narrow a simple group sum to exact, non-dotted built-in record fields."""
    if spec is None or type(spec.key_selector) is not str or "." in spec.key_selector:
        return None
    if type(spec.value_selector) is not str or "." in spec.value_selector:
        return None
    return NativeRecordGroupSumSpec(
        spec.key_selector,
        spec.value_selector,
        spec.output_name,
    )


def _native_pair_i64_expr_group_sum_spec(
    spec: SimpleGroupSumSpec | None,
    input_node: PhysicalRelNode,
    query: Query,
) -> NativePairI64ExprGroupSumSpec | None:
    """Recognize two canonical pair expressions over one retained exact sequence."""
    if (
        spec is None
        or query.logical.engine != "auto"
        or query.logical.parallel is not None
        or type(input_node) is not SourcePhysicalNode
        or type(spec.output_name) is not str
    ):
        return None
    source = input_node.source
    if (
        type(source) is not Source
        or not source.capabilities.reiterable
        or not source.capabilities.ordered
        or Source.__dict__.get("retained_sequence") is not _CANONICAL_RETAINED_SEQUENCE
        or Source.__dict__.get("native_data") is not _CANONICAL_SOURCE_NATIVE_DATA
    ):
        return None
    retained = source.retained_sequence()
    if (
        type(retained) not in (list, tuple)
        or retained is not source.native_data
        or len(retained) < _NATIVE_PAIR_I64_EXPR_GROUP_MIN_ROWS
    ):
        return None
    key_program = lower_pair_i64_group_key(spec.key_selector)
    value_program = lower_pair_i64_group_value(spec.value_selector)
    if key_program is None or value_program is None:
        return None
    return NativePairI64ExprGroupSumSpec(
        key_program,
        value_program,
        spec.output_name,
    )


def _arrow_group_sum_spec(
    spec: SimpleGroupSumSpec | None,
    input_node: PhysicalRelNode,
    query: Query,
) -> ArrowGroupSumSpec | None:
    """Mark a direct Arrow field sum whose source can be specialized safely."""
    if (
        spec is None
        or query.logical.engine != "auto"
        or not isinstance(input_node, SourcePhysicalNode)
        or type(spec.key_selector) is not str
        or "." in spec.key_selector
        or type(spec.value_selector) is not str
        or "." in spec.value_selector
    ):
        return None
    descriptor = input_node.source.native_data
    if not isinstance(descriptor, ArrowBatchSource):
        return None
    retained_arrow = descriptor.kind in {"table", "record_batch"}
    reopenable_columnar = descriptor.kind in {"dataframe", "polars"} and (
        descriptor.columnar_opener is not None
    )
    reopenable_batches = descriptor.kind in {"csv", "parquet"} and descriptor.reiterable
    if retained_arrow or reopenable_columnar or reopenable_batches:
        if not input_node.source.capabilities.reiterable:
            return None
        return ArrowGroupSumSpec(spec.key_selector, spec.value_selector, spec.output_name)
    if (
        descriptor.kind != "reader"
        or descriptor.reiterable
        or input_node.source.capabilities.reiterable
        or descriptor.schema_hint is None
    ):
        return None

    # A one-shot source cannot be reopened after a speculative type rejection.  Prove its
    # complete batch schema while planning so every runtime decline still happens pre-claim.
    schema = descriptor.schema_hint
    names = tuple(getattr(schema, "names", ()))
    if names.count(spec.key_selector) != 1 or names.count(spec.value_selector) != 1:
        return None
    try:
        from pyarrow import types  # type: ignore[import-untyped]

        key_type = schema.field(names.index(spec.key_selector)).type
        value_type = schema.field(names.index(spec.value_selector)).type
    except (AttributeError, ImportError, IndexError, TypeError, ValueError):
        return None
    if types.is_dictionary(key_type) or not (
        types.is_null(key_type)
        or types.is_boolean(key_type)
        or types.is_integer(key_type)
        or types.is_string(key_type)
        or types.is_large_string(key_type)
        or types.is_binary(key_type)
        or types.is_large_binary(key_type)
    ):
        return None
    if not types.is_int64(value_type):
        return None
    return ArrowGroupSumSpec(spec.key_selector, spec.value_selector, spec.output_name)


def _closed_arrow_group_spec(closed: ClosedGroupSpec | None) -> ArrowGroupAggregateSpec | None:
    """Narrow one direct-field closed group to the retained Arrow lane vocabulary."""
    if closed is None or type(closed.key_selector) is not str or "." in closed.key_selector:
        return None
    lanes: list[ArrowGroupLaneSpec] = []
    for lane in closed.lanes:
        if type(lane.output_name) is not str:
            return None
        if lane.kind == "count" and lane.selector is None:
            lanes.append(ArrowGroupLaneSpec(lane.output_name, "count", None))
            continue
        if (
            lane.kind not in {"sum", "min", "max"}
            or type(lane.selector) is not str
            or "." in lane.selector
        ):
            return None
        lanes.append(ArrowGroupLaneSpec(lane.output_name, lane.kind, lane.selector))
    return ArrowGroupAggregateSpec(closed.key_selector, tuple(lanes)) if lanes else None


def _arrow_reader_group_aggregate_spec(
    spec: ArrowGroupAggregateSpec,
    input_node: PhysicalRelNode,
) -> ArrowGroupAggregateSpec | None:
    """Prove a one-shot reader's complete grouped-aggregate schema before claiming it."""
    if not isinstance(input_node, SourcePhysicalNode):
        return None
    descriptor = input_node.source.native_data
    if (
        not isinstance(descriptor, ArrowBatchSource)
        or descriptor.kind != "reader"
        or descriptor.reiterable
        or input_node.source.capabilities.reiterable
        or descriptor.schema_hint is None
    ):
        return None
    schema = descriptor.schema_hint
    names = tuple(getattr(schema, "names", ()))
    if names.count(spec.key_field) != 1:
        return None
    try:
        from pyarrow import types

        key_type = schema.field(names.index(spec.key_field)).type
    except (AttributeError, ImportError, IndexError, TypeError, ValueError):
        return None
    if types.is_dictionary(key_type) or not (
        types.is_null(key_type)
        or types.is_boolean(key_type)
        or types.is_integer(key_type)
        or types.is_string(key_type)
        or types.is_large_string(key_type)
        or types.is_binary(key_type)
        or types.is_large_binary(key_type)
    ):
        return None
    for lane in spec.lanes:
        value_field = lane.value_field
        if lane.kind == "count":
            continue
        if value_field is None or names.count(value_field) != 1:
            return None
        try:
            value_type = schema.field(names.index(value_field)).type
        except (AttributeError, IndexError, TypeError, ValueError):
            return None
        if not types.is_int64(value_type):
            return None
    return spec


def _numpy_group_source_prefix(
    input_node: PhysicalRelNode,
) -> tuple[SourcePhysicalNode, NumpyPrefixPlan | None] | None:
    """Recover one direct source and an optional complete safe physical prefix."""
    source_node: SourcePhysicalNode
    prefix: NumpyPrefixPlan | None
    if isinstance(input_node, SourcePhysicalNode):
        source_node = input_node
        prefix = None
    elif (
        isinstance(input_node, PipelinePhysicalNode)
        and input_node.parallel is None
        and isinstance(input_node.input, SourcePhysicalNode)
    ):
        source_node = input_node.input
        prefix_operations: list[Operation] = []
        for stage in input_node.stages:
            if not isinstance(stage, (RowPhysicalNode, CompiledExpressionPhysicalNode)):
                return None
            prefix_operations.append(stage.operation)
        prefix = plan_numpy_prefix(
            Pipeline(
                source_node.source,
                tuple(prefix_operations),
                input_node.engine,
                input_node.parallel,
            )
        )
        if prefix is None or prefix.operation_count != len(input_node.stages):
            return None
    else:
        return None
    return source_node, prefix


def _numpy_group_aggregate_spec(
    closed: ClosedGroupSpec | None,
    input_node: PhysicalRelNode,
    query: Query,
    key_name: object,
) -> NumpyGroupAggregateSpec | None:
    """Narrow a direct or safe-prefix integer matrix group to the NumPy vocabulary."""
    if (
        closed is None
        or query.logical.engine != "auto"
        or query.logical.parallel is not None
        or type(key_name) is not str
        or type(closed.key_selector) is not str
        or "." in closed.key_selector
    ):
        return None
    source_prefix = _numpy_group_source_prefix(input_node)
    if source_prefix is None:
        return None
    source_node, prefix = source_prefix
    if not source_node.source.capabilities.reiterable:
        return None
    from ..tabular.numpy import NumpyRowSource

    descriptor = source_node.source.native_data
    if type(descriptor) is not NumpyRowSource or any(
        type(name) is not str for name in descriptor.columns
    ):
        return None
    dtype = getattr(descriptor.array, "dtype", None)
    if (
        getattr(dtype, "kind", None) not in {"b", "i", "u"}
        or not 1 <= getattr(dtype, "itemsize", 0) <= 8
    ):
        return None

    key_field: str | None
    if prefix is None:
        key_field = closed.key_selector
        field_mapping: dict[str, NumpyColumnSource] | None = None
    else:
        field_mapping = dict(prefix.output_fields)
        mapped_key_field = field_mapping.get(closed.key_selector)
        if type(mapped_key_field) is not str:
            return None
        key_field = mapped_key_field

    lanes: list[NumpyGroupLaneSpec] = []
    for lane in closed.lanes:
        if type(lane.output_name) is not str:
            return None
        if lane.kind == "count" and lane.selector is None:
            lanes.append(NumpyGroupLaneSpec(lane.output_name, "count", None))
            continue
        if (
            lane.kind not in {"sum", "min", "max"}
            or type(lane.selector) is not str
            or "." in lane.selector
        ):
            return None
        value_field = lane.selector if field_mapping is None else field_mapping.get(lane.selector)
        if type(value_field) is not str:
            return None
        lanes.append(NumpyGroupLaneSpec(lane.output_name, lane.kind, value_field))
    return NumpyGroupAggregateSpec(key_field, tuple(lanes), prefix) if lanes else None


def _numpy_global_aggregate_spec(
    aggregations: AggregationItems,
    input_node: PhysicalRelNode,
    query: Query,
) -> NumpyGlobalAggregateSpec | None:
    """Narrow direct retained numeric-matrix reductions to the NumPy lane vocabulary."""
    if (
        query.logical.engine != "auto"
        or query.logical.parallel is not None
        or not isinstance(input_node, SourcePhysicalNode)
        or not input_node.source.capabilities.reiterable
    ):
        return None
    from ..tabular.numpy import NumpyRowSource

    descriptor = input_node.source.native_data
    if type(descriptor) is not NumpyRowSource or any(
        type(name) is not str for name in descriptor.columns
    ):
        return None
    row_count = len(descriptor)
    dtype = getattr(descriptor.array, "dtype", None)
    dtype_kind = getattr(dtype, "kind", None)
    itemsize = getattr(dtype, "itemsize", 0)
    integer_dtype = dtype_kind in {"b", "i", "u"} and 1 <= itemsize <= 8
    float64_dtype = dtype_kind == "f" and itemsize == 8 and bool(getattr(dtype, "isnative", False))
    if not (integer_dtype or float64_dtype) or row_count < _NUMPY_GLOBAL_MIN_ROWS:
        return None

    lanes: list[NumpyGlobalLaneSpec] = []
    for output_name, aggregation in aggregations:
        if type(output_name) is not str:
            return None
        if project_count_aggregation(aggregation):
            lanes.append(NumpyGlobalLaneSpec(output_name, "count", None))
            continue
        native = native_group_aggregation(aggregation)
        if native is None or type(native.selector) is not str or "." in native.selector:
            return None
        if native.kind == "mean":
            if not (
                float64_dtype
                or (dtype_kind == "i" and itemsize == 8 and bool(getattr(dtype, "isnative", False)))
            ):
                return None
        elif native.kind == "sum":
            if not (integer_dtype or float64_dtype):
                return None
        elif native.kind not in {"min", "max"} or not integer_dtype:
            return None
        lanes.append(NumpyGlobalLaneSpec(output_name, native.kind, native.selector))
    return NumpyGlobalAggregateSpec(tuple(lanes)) if lanes else None


def _has_stable_arrow_table_prefix(input_node: PhysicalRelNode, query: Query) -> bool:
    """Recognize a complete table-safe unary prefix over one replayable columnar source."""
    if (
        query.logical.engine != "auto"
        or query.logical.parallel is not None
        or not isinstance(input_node, PipelinePhysicalNode)
        or input_node.parallel is not None
        or not isinstance(input_node.input, SourcePhysicalNode)
    ):
        return False
    source = input_node.input.source
    if not source.capabilities.reiterable:
        return False
    descriptor = source.native_data
    if not isinstance(descriptor, ArrowBatchSource):
        return False
    if descriptor.kind not in {"table", "record_batch"} or descriptor.materialized_data is None:
        return False
    operations: list[Operation] = []
    for stage in input_node.stages:
        if not isinstance(
            stage,
            (RowPhysicalNode, CompiledExpressionPhysicalNode, SortPhysicalNode),
        ):
            return False
        operations.append(stage.operation)
    pipeline = Pipeline(
        source,
        tuple(operations),
        input_node.engine,
        input_node.parallel,
    )
    return plan_arrow_table_prefix(pipeline) is not None


def _arrow_group_aggregate_spec(
    closed: ClosedGroupSpec | None,
    simple_sum: SimpleGroupSumSpec | None,
    input_node: PhysicalRelNode,
    query: Query,
    key_name: object,
) -> ArrowGroupSumSpec | ArrowGroupAggregateSpec | None:
    """Select a direct-source or table-prefix Arrow grouped aggregate."""
    if type(key_name) is not str:
        return None
    closed_spec = _closed_arrow_group_spec(closed)
    if closed_spec is None:
        return None
    sum_spec = _arrow_group_sum_spec(simple_sum, input_node, query)
    if sum_spec is not None:
        return sum_spec
    if _has_stable_arrow_table_prefix(input_node, query):
        return closed_spec
    if query.logical.engine != "auto" or not isinstance(input_node, SourcePhysicalNode):
        return None
    descriptor = input_node.source.native_data
    if not isinstance(descriptor, ArrowBatchSource):
        return None
    if descriptor.kind == "reader":
        return _arrow_reader_group_aggregate_spec(closed_spec, input_node)
    if not input_node.source.capabilities.reiterable:
        return None
    retained = descriptor.kind in {"table", "record_batch"}
    reopened_columnar = descriptor.kind in {"dataframe", "polars"} and (
        descriptor.columnar_opener is not None
    )
    return closed_spec if retained or reopened_columnar else None


def _arrow_global_reduction_spec(
    aggregations: AggregationItems,
    input_node: PhysicalRelNode,
    query: Query,
) -> ArrowGlobalSumSpec | None:
    """Mark one direct or table-prefix Arrow i64 scalar reduction."""
    if query.logical.engine != "auto" or len(aggregations) != 1:
        return None
    output_name, aggregation = aggregations[0]
    native = native_group_aggregation(aggregation)
    if (
        native is None
        or native.kind not in {"sum", "min", "max", "first", "last"}
        or type(native.selector) is not str
        or "." in native.selector
    ):
        return None
    if _has_stable_arrow_table_prefix(input_node, query):
        if native.kind == "first" or type(output_name) is not str:
            return None
        return ArrowGlobalSumSpec(native.selector, output_name, native.kind)
    if not isinstance(input_node, SourcePhysicalNode):
        return None
    descriptor = input_node.source.native_data
    if not isinstance(descriptor, ArrowBatchSource):
        return None
    retained_arrow = descriptor.kind in {"table", "record_batch"}
    reopenable_columnar = descriptor.kind in {"dataframe", "polars"} and (
        descriptor.columnar_opener is not None
    )
    one_shot_reader = descriptor.kind == "reader"
    if not retained_arrow and not reopenable_columnar and not one_shot_reader:
        return None
    return ArrowGlobalSumSpec(native.selector, output_name, native.kind)


def _arrow_global_multi_spec(
    aggregations: AggregationItems,
    input_node: PhysicalRelNode,
    query: Query,
) -> ArrowGlobalAggregateSpec | None:
    """Narrow direct retained or one-shot Arrow input to closed global lanes."""
    if (
        query.logical.engine != "auto"
        or len(aggregations) <= 1
        or not isinstance(input_node, SourcePhysicalNode)
    ):
        return None
    descriptor = input_node.source.native_data
    if not isinstance(descriptor, ArrowBatchSource):
        return None
    retained = descriptor.kind in {"table", "record_batch"}
    reopened_columnar = descriptor.kind in {"dataframe", "polars"} and (
        descriptor.columnar_opener is not None
    )
    one_shot_reader = (
        descriptor.kind == "reader"
        and not descriptor.reiterable
        and not input_node.source.capabilities.reiterable
    )
    if not one_shot_reader and (
        not input_node.source.capabilities.reiterable or (not retained and not reopened_columnar)
    ):
        return None

    lanes: list[ArrowGroupLaneSpec] = []
    for output_name, aggregation in aggregations:
        if type(output_name) is not str:
            return None
        if project_count_aggregation(aggregation):
            lanes.append(ArrowGroupLaneSpec(output_name, "count", None))
            continue
        native = native_group_aggregation(aggregation)
        if (
            native is None
            or native.kind not in {"sum", "min", "max"}
            or type(native.selector) is not str
            or "." in native.selector
        ):
            return None
        lanes.append(ArrowGroupLaneSpec(output_name, native.kind, native.selector))
    return ArrowGlobalAggregateSpec(tuple(lanes))


def _native_record_global_sum_spec(
    aggregations: AggregationItems,
    input_node: PhysicalRelNode,
    query: Query,
) -> NativeRecordGlobalSumSpec | None:
    """Mark one direct retained exact-record i64 sum for guarded native execution."""
    if (
        query.logical.engine != "auto"
        or len(aggregations) != 1
        or not isinstance(input_node, SourcePhysicalNode)
        or not input_node.source.capabilities.reiterable
        or type(input_node.source.native_data) not in (list, tuple)
    ):
        return None
    output_name, aggregation = aggregations[0]
    native = native_group_aggregation(aggregation)
    if (
        native is None
        or native.kind != "sum"
        or type(native.selector) is not str
        or "." in native.selector
    ):
        return None
    return NativeRecordGlobalSumSpec(native.selector, output_name)


def _native_global_i64_aggregate_spec(
    aggregations: AggregationItems,
    input_node: PhysicalRelNode,
    query: Query,
) -> NativeGlobalI64AggregateSpec | None:
    """Narrow direct exact-record reductions to one ordered native i64 scan."""
    if (
        query.logical.engine != "auto"
        or query.logical.parallel is not None
        or not aggregations
        or not isinstance(input_node, SourcePhysicalNode)
        or not input_node.source.capabilities.reiterable
    ):
        return None
    retained = input_node.source.native_data
    if type(retained) not in (list, tuple) or len(retained) < _NATIVE_GLOBAL_I64_MIN_ROWS:
        return None

    selector_type: type[int] | type[str] | None = None
    lanes: list[NativeGlobalI64LaneSpec] = []
    selected_kind: str | None = None
    for output_name, aggregation in aggregations:
        if type(output_name) is not str:
            return None
        if project_count_aggregation(aggregation):
            lanes.append(NativeGlobalI64LaneSpec(output_name, "count", None))
            continue
        native = native_group_aggregation(aggregation)
        if (
            native is None
            or native.kind not in {"sum", "min", "max"}
            or not _is_exact_field_selector(native.selector)
            or (type(native.selector) is str and "." in native.selector)
        ):
            return None
        current_type = type(native.selector)
        if selector_type is None:
            selector_type = current_type
        elif current_type is not selector_type:
            return None
        selected_kind = native.kind
        lanes.append(
            NativeGlobalI64LaneSpec(
                output_name,
                native.kind,
                native.selector,
            )
        )

    # Exact-size count already has an O(1) path. The established dictionary single-sum ABI is
    # narrower and faster than the generic lane parser, so keep it authoritative.
    if selector_type is None or (
        len(lanes) == 1 and selector_type is str and selected_kind == "sum"
    ):
        return None
    return NativeGlobalI64AggregateSpec(
        "tuple" if selector_type is int else "dict",
        tuple(lanes),
    )


def _exact_global_count_name(
    aggregations: AggregationItems,
    input_node: PhysicalRelNode,
    query: Query,
) -> str | None:
    """Mark one direct project-owned count over a trusted exact-size source."""
    if (
        query.logical.engine != "auto"
        or len(aggregations) != 1
        or not isinstance(input_node, SourcePhysicalNode)
        or not input_node.source.capabilities.reiterable
        or input_node.source.current_exact_size() is None
    ):
        return None
    output_name, aggregation = aggregations[0]
    return output_name if project_count_aggregation(aggregation) else None


def _arrow_global_count_name(
    aggregations: AggregationItems,
    input_node: PhysicalRelNode,
    query: Query,
) -> str | None:
    """Mark one direct or table-prefix count backed by guarded Arrow execution."""
    if query.logical.engine != "auto" or len(aggregations) != 1:
        return None
    output_name, aggregation = aggregations[0]
    if not project_count_aggregation(aggregation):
        return None
    if _has_stable_arrow_table_prefix(input_node, query):
        return output_name if type(output_name) is str else None
    if (
        not isinstance(input_node, SourcePhysicalNode)
        or not input_node.source.capabilities.reiterable
    ):
        return None
    descriptor = input_node.source.native_data
    if not isinstance(descriptor, ArrowBatchSource) or descriptor.count_opener is None:
        return None
    return output_name


def _native_direct_join_spec(
    logical: JoinNode,
    root: LogicalNode,
    query: Query,
    left: PhysicalRelNode,
    right: PhysicalRelNode,
    compiled: CompiledJoinSpec,
) -> NativeRecordJoinSpec | None:
    """Admit top-level direct physical fields without changing logical output naming."""
    spec = logical.spec
    left_field = _direct_field(compiled.left_key)
    right_field = _direct_field(compiled.right_key)
    if (
        query.logical.engine != "auto"
        or query.terminal.name != "list"
        or query.logical.root is not root
        or root is not logical
        or not isinstance(left, SourcePhysicalNode)
        or not isinstance(right, SourcePhysicalNode)
        or not left.source.capabilities.reiterable
        or not right.source.capabilities.reiterable
        or type(left.source.native_data) not in (list, tuple)
        or type(right.source.native_data) not in (list, tuple)
        or spec.partitions is not None
        or spec.how not in {"inner", "left"}
        or spec.validate not in {"m:m", "m:1"}
        or left_field is None
        or right_field is None
    ):
        return None
    return NativeRecordJoinSpec(left_field, right_field)


def _native_i64_join_spec(
    direct: NativeRecordJoinSpec | None,
    compiled: CompiledJoinSpec,
) -> NativeRecordJoinSpec | None:
    """Keep i64 v1 only when its implicit shared-key merge matches logical naming."""
    if direct is None:
        return None
    implicit_shared = (
        frozenset((direct.left_field,)) if direct.left_field == direct.right_field else frozenset()
    )
    return direct if compiled.shared_names == implicit_shared else None


def _arrow_unique_join_spec(
    logical: JoinNode,
    root: LogicalNode,
    query: Query,
    left: PhysicalRelNode,
    right: PhysicalRelNode,
) -> ArrowUniqueJoinSpec | None:
    """Recognize one top-level retained Arrow m:1 join without opening either source."""
    spec = logical.spec
    if (
        query.logical.engine != "auto"
        or query.logical.parallel is not None
        or query.terminal.name != "list"
        or query.logical.root is not root
        or root is not logical
        or not isinstance(left, SourcePhysicalNode)
        or not isinstance(right, SourcePhysicalNode)
        or not left.source.capabilities.reiterable
        or not right.source.capabilities.reiterable
        or (
            (left_size := left.source.current_exact_size()) is not None
            and (right_size := right.source.current_exact_size()) is not None
            and left_size + right_size < _ARROW_UNIQUE_JOIN_MIN_ROWS
        )
        or spec.partitions is not None
        or type(spec.how) is not str
        or spec.how not in {"inner", "left"}
        or type(spec.validate) is not str
        or spec.validate != "m:1"
        or type(spec.left_on) is not str
        or "." in spec.left_on
        or type(spec.right_on) is not str
        or "." in spec.right_on
        or type(spec.suffix) is not str
    ):
        return None
    descriptors = (left.source.native_data, right.source.native_data)
    if any(
        not isinstance(descriptor, ArrowBatchSource)
        or descriptor.kind not in {"table", "record_batch"}
        or descriptor.materialized_data is None
        for descriptor in descriptors
    ):
        return None
    return ArrowUniqueJoinSpec(spec.left_on, spec.right_on)


def _native_callable_join_validation(
    logical: JoinNode,
    root: LogicalNode,
    query: Query,
    left: PhysicalRelNode,
    right: PhysicalRelNode,
    compiled: CompiledJoinSpec,
) -> str | None:
    """Return the supported cardinality for one eager callable join admission."""
    spec = logical.spec
    if (
        query.logical.engine != "auto"
        or query.terminal.name != "list"
        or query.logical.root is not root
        or root is not logical
        or not isinstance(left, SourcePhysicalNode)
        or not isinstance(right, SourcePhysicalNode)
        or not left.source.capabilities.reiterable
        or not right.source.capabilities.reiterable
        or type(left.source.native_data) not in (list, tuple)
        or type(right.source.native_data) not in (list, tuple)
        or spec.partitions is not None
        or spec.how not in {"inner", "left"}
    ):
        return None
    if spec.validate == "m:1":
        validation = "m:1"
    elif spec.validate == "m:m":
        validation = "m:m"
    else:
        return None
    if (
        (
            _direct_field(compiled.left_key) is not None
            and _direct_field(compiled.right_key) is not None
        )
        or not callable(spec.left_on)
        or not callable(spec.right_on)
        or type(spec.suffix) is not str
    ):
        return None
    return validation
