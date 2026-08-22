"""Compile M1 logical queries to source-safe M2 physical plans."""

from __future__ import annotations

from hashlib import sha256
from typing import Any, TypeGuard

from ..collecting.aggregate_program import compile_aggregations
from ..collecting.aggregation import (
    AggregationItems,
    native_group_aggregation,
    project_count_aggregation,
)
from ..errors import NativeUnsupportedError
from ..expressions.program import ExprProgram, compile_expression
from ..expressions.selectors import _direct_field, compile_selector
from ..expressions.typed_ir import Effect, ExpressionSource, lower_expression
from ..physical.compiled import ProgramFingerprint
from ..physical.kernel_cache import KernelCache
from ..physical.plan import (
    BackendPayload,
    CompiledExpressionPhysicalNode,
    PhysicalPlan,
    PlanDecision,
    RowPhysicalNode,
    SortPhysicalNode,
    SortStrategy,
)
from ..physical.relational import (
    ArrowGlobalSumSpec,
    ArrowGroupSumSpec,
    ArrowUniqueJoinSpec,
    ClosedGroupSpec,
    CompiledJoinSpec,
    CompositeCountSumSpec,
    GlobalAggregatePhysicalNode,
    GroupAggregatePhysicalNode,
    GroupLane,
    JoinPhysicalNode,
    JoinStrategy,
    NativeCallableGroupSpec,
    NativeFixedI64GroupSpec,
    NativeGroupSumSpec,
    NativeRecordGroupSumSpec,
    NativeRecordJoinSpec,
    PhysicalRelNode,
    PipelinePhysicalNode,
    SimpleGroupSumSpec,
    SourcePhysicalNode,
    SpillCountSpec,
)
from .arrow import plan_arrow_first_prefix, plan_arrow_prefix
from .arrow_source import ArrowBatchSource
from .logical import (
    GlobalAggregateNode,
    GroupAggregateNode,
    GroupAggregateSpec,
    JoinNode,
    LogicalNode,
    LogicalPlan,
    Query,
    SourceNode,
    TerminalSpec,
    UnaryNode,
    linear_pipeline,
    unary_chain,
    walk_logical,
)
from .native import (
    EngineDecision,
    select_materializing_engine,
    select_terminal_engine,
    validate_terminal,
)
from .plan_cache import PhysicalPlanTemplate, PlanCache, PlanCacheKey
from .source import Source
from .sync import FilterOp, MapOp, Operation, SortOp

_NATIVE_TERMINALS = frozenset(
    {
        "count",
        "sum",
        "min",
        "max",
        "minmax",
        "last",
        "first",
        "any",
        "all",
        "statistics",
        "aggregate",
    }
)
_MATERIALIZING_TERMINALS = frozenset({"iterate", "list", "tuple", "set"})
_ARROW_PREFIX_TERMINALS = _MATERIALIZING_TERMINALS | {"count"}
_ARROW_UNIQUE_JOIN_MIN_ROWS = 128
_SINGLE_INPUT_RELATIONS = (
    PipelinePhysicalNode,
    GroupAggregatePhysicalNode,
    GlobalAggregatePhysicalNode,
)
_EXPRESSION_PROGRAM_CACHE = KernelCache()
_PHYSICAL_TEMPLATE_CACHE: PlanCache[
    tuple[
        tuple[Any, ...],
        PlanDecision,
        BackendPayload,
    ]
] = PlanCache()
_STREAMING_ITERATION_REASON = "streaming iteration uses low-latency Python row execution"
_ITERATE_TERMINAL = TerminalSpec("iterate")


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
    _key_name, key_selector = spec.keys[0]
    output_name, aggregation = spec.aggregations[0]
    native = native_group_aggregation(aggregation)
    if (
        not (callable(key_selector) or _is_exact_field_selector(key_selector))
        or native is None
        or native.kind != "sum"
        or not (callable(native.selector) or _is_exact_field_selector(native.selector))
    ):
        return None
    return SimpleGroupSumSpec(
        key_selector,
        native.selector,
        compile_selector(native.selector),
        output_name,
    )


def _closed_group_spec(spec: GroupAggregateSpec) -> ClosedGroupSpec | None:
    """Compile trusted fixed-size grouped aggregations into contiguous state lanes."""
    if spec.partitions is not None or len(spec.keys) != 1:
        return None
    _key_name, key_selector = spec.keys[0]
    if not (callable(key_selector) or _is_exact_field_selector(key_selector)):
        return None

    lanes: list[GroupLane] = []
    for output_name, aggregation in spec.aggregations:
        if project_count_aggregation(aggregation):
            lanes.append(GroupLane(output_name, "count", None, None))
            continue
        native = native_group_aggregation(aggregation)
        if native is None:
            return None
        selector = native.selector
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
    key_selectors = tuple(selector for _name, selector in spec.keys)
    if any(
        not _is_exact_field_selector(selector) or (type(selector) is str and "." in selector)
        for selector in key_selectors
    ):
        return None
    count_name, count_aggregation = spec.aggregations[0]
    sum_name, sum_aggregation = spec.aggregations[1]
    native_sum = native_group_aggregation(sum_aggregation)
    if (
        not project_count_aggregation(count_aggregation)
        or native_sum is None
        or native_sum.kind != "sum"
        or not _is_exact_field_selector(native_sum.selector)
        or (type(native_sum.selector) is str and "." in native_sum.selector)
    ):
        return None
    return CompositeCountSumSpec(
        (key_selectors[0], key_selectors[1]),
        native_sum.selector,
        compile_selector(native_sum.selector),
        count_name,
        sum_name,
    )


def _native_fixed_i64_group_spec(
    spec: ClosedGroupSpec | None,
) -> NativeFixedI64GroupSpec | None:
    """Narrow fixed count lanes to one exact tuple or record ABI description."""
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
    if signature != ("count", "sum"):
        return None
    count_lane, sum_lane = spec.lanes
    value_selector = sum_lane.selector
    if (
        type(count_lane.output_name) is not str
        or type(sum_lane.output_name) is not str
        or not _is_exact_field_selector(value_selector)
        or type(value_selector) is not type(key_selector)
        or (type(value_selector) is str and "." in value_selector)
    ):
        return None
    return NativeFixedI64GroupSpec(
        "tuple" if type(key_selector) is int else "dict",
        key_selector,
        value_selector,
        count_lane.output_name,
        sum_lane.output_name,
    )


def _native_callable_group_spec(
    spec: ClosedGroupSpec | None,
) -> NativeCallableGroupSpec | None:
    """Recognize exact-record count/sum with one callback and one direct field."""
    if spec is None or tuple(lane.kind for lane in spec.lanes) != ("count", "sum"):
        return None
    count_lane, sum_lane = spec.lanes
    value_selector = sum_lane.selector
    if type(count_lane.output_name) is not str or type(sum_lane.output_name) is not str:
        return None
    if callable(spec.key_selector) and type(value_selector) is str and "." not in value_selector:
        return NativeCallableGroupSpec(
            "key",
            value_selector,
            count_lane.output_name,
            sum_lane.output_name,
        )
    if type(spec.key_selector) is str and "." not in spec.key_selector and callable(value_selector):
        return NativeCallableGroupSpec(
            "value",
            spec.key_selector,
            count_lane.output_name,
            sum_lane.output_name,
        )
    return None


def _spill_count_spec(spec: GroupAggregateSpec) -> SpillCountSpec | None:
    """Recognize the one closed count shape whose partial state is proven mergeable."""
    if spec.partitions is None or len(spec.keys) != 1 or len(spec.aggregations) != 1:
        return None
    _key_name, key_selector = spec.keys[0]
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


def _arrow_global_reduction_spec(
    aggregations: AggregationItems,
    input_node: PhysicalRelNode,
    query: Query,
) -> ArrowGlobalSumSpec | None:
    """Mark one direct Arrow i64 scalar reduction for guarded columnar execution."""
    if (
        query.logical.engine != "auto"
        or len(aggregations) != 1
        or not isinstance(input_node, SourcePhysicalNode)
    ):
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
        or input_node.source.capabilities.exact_size is None
    ):
        return None
    output_name, aggregation = aggregations[0]
    return output_name if project_count_aggregation(aggregation) else None


def _arrow_global_count_name(
    aggregations: AggregationItems,
    input_node: PhysicalRelNode,
    query: Query,
) -> str | None:
    """Mark one direct count backed by a replayable Arrow source terminal."""
    if (
        query.logical.engine != "auto"
        or len(aggregations) != 1
        or not isinstance(input_node, SourcePhysicalNode)
        or not input_node.source.capabilities.reiterable
    ):
        return None
    descriptor = input_node.source.native_data
    if not isinstance(descriptor, ArrowBatchSource) or descriptor.count_opener is None:
        return None
    output_name, aggregation = aggregations[0]
    return output_name if project_count_aggregation(aggregation) else None


def _native_record_join_spec(
    logical: JoinNode,
    root: LogicalNode,
    query: Query,
    left: PhysicalRelNode,
    right: PhysicalRelNode,
) -> NativeRecordJoinSpec | None:
    """Recognize only a top-level eager auto join over two retained exact containers."""
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
        or spec.validate not in {"m:m", "m:1"}
        or type(spec.left_on) is not str
        or "." in spec.left_on
        or type(spec.right_on) is not str
        or "." in spec.right_on
    ):
        return None
    return NativeRecordJoinSpec(spec.left_on, spec.right_on)


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
            left.source.capabilities.exact_size is not None
            and right.source.capabilities.exact_size is not None
            and left.source.capabilities.exact_size + right.source.capabilities.exact_size
            < _ARROW_UNIQUE_JOIN_MIN_ROWS
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
    if not callable(spec.left_on) or not callable(spec.right_on) or type(spec.suffix) is not str:
        return None
    return validation


def _compile_streaming_iteration(logical: LogicalPlan) -> PhysicalPlan | None:
    """Compile linear Python iteration without materializing-backend analysis."""
    if logical.engine not in {"auto", "python"}:
        return None
    try:
        source, unary_nodes = unary_chain(logical.root)
    except TypeError:
        return None
    decision = EngineDecision("python", _STREAMING_ITERATION_REASON)
    return PhysicalPlan(
        source.source,
        tuple(
            RowPhysicalNode((index,), "python-row", node.operation)
            for index, node in enumerate(unary_nodes)
        ),
        _ITERATE_TERMINAL,
        PlanDecision(decision.engine, decision.reason),
        "python",
        logical.parallel,
        BackendPayload(decision),
        cache_reason="streaming iteration is compiled without materializing backends",
    )


def compile_iteration(query: Query) -> PhysicalPlan:
    """Compile one already-described iterate query through the low-latency row route."""
    if query.terminal.name != "iterate":
        raise ValueError("compile_iteration requires an iterate query")
    streaming = _compile_streaming_iteration(query.logical)
    if streaming is not None:
        return streaming
    return compile_query(query)


def compile_query(query: Query) -> PhysicalPlan:
    """Compile without opening the source and select each current backend exactly once."""
    if query.terminal.name == "iterate" and query.logical.engine == "python":
        streaming = _compile_streaming_iteration(query.logical)
        if streaming is not None:
            return streaming
    if _contains_relational(query.logical.root):
        # Forced native is an end-to-end contract, whereas the relational executor is
        # currently Python-owned even when auto selects a guarded native aggregate kernel.
        if query.logical.engine == "native":
            raise NativeUnsupportedError("relational queries are not native-compilable")
        return _compile_relational_query(query)
    source, unary_nodes = unary_chain(query.logical.root)
    pipeline = linear_pipeline(query.logical)
    terminal = query.terminal.name
    arrow_prefix = (
        plan_arrow_prefix(pipeline)
        if terminal in _ARROW_PREFIX_TERMINALS
        else plan_arrow_first_prefix(pipeline)
        if terminal == "first"
        else None
    )
    native_decision = (
        select_materializing_engine(pipeline)
        if terminal in _MATERIALIZING_TERMINALS
        else (
            select_terminal_engine(pipeline, validate_terminal(terminal))
            if terminal in _NATIVE_TERMINALS
            else None
        )
    )
    compiled_nodes = tuple(
        _compile_node(index, node.operation) for index, node in enumerate(unary_nodes)
    )
    nodes = _select_direct_arrow_sort(compiled_nodes, source.source, query)
    guards = (
        ("row_mode: low_latency_terminal",)
        if query.terminal.name in {"first", "any", "all", "none", "find", "nth"}
        else ()
    )
    decision = (
        PlanDecision(native_decision.engine, native_decision.reason, guards=guards)
        if native_decision is not None
        else PlanDecision(
            "python",
            "terminal is executed by the Python compatibility path",
            guards=guards,
        )
    )
    payload = BackendPayload(native_decision, arrow_prefix)
    cache_key, cache_reason = _physical_cache_key(
        query,
        compiled_nodes,
        nodes,
        decision,
        payload,
    )
    if cache_key is not None:
        template = _PHYSICAL_TEMPLATE_CACHE.get(cache_key)
        if template is not None:
            cached_nodes, cached_decision, cached_payload = template.payload
            return PhysicalPlan(
                source.source,
                cached_nodes,
                query.terminal,
                cached_decision,
                query.logical.engine,
                query.logical.parallel,
                cached_payload,
                cache_hit=True,
                cacheable=True,
                cache_reason="reused source-free physical template",
            )
    physical = PhysicalPlan(
        source.source,
        nodes,
        query.terminal,
        decision,
        query.logical.engine,
        query.logical.parallel,
        payload,
        cacheable=cache_key is not None,
        cache_reason=cache_reason,
    )
    if cache_key is not None:
        _PHYSICAL_TEMPLATE_CACHE.put(
            cache_key,
            PhysicalPlanTemplate((nodes, decision, payload)),
        )
    return physical


def _select_direct_arrow_sort(
    nodes: tuple[RowPhysicalNode | CompiledExpressionPhysicalNode | SortPhysicalNode, ...],
    source: Source[Any],
    query: Query,
) -> tuple[RowPhysicalNode | CompiledExpressionPhysicalNode | SortPhysicalNode, ...]:
    """Select one guarded retained-Arrow stable sort without inspecting source values."""
    if (
        query.logical.engine != "auto"
        or query.logical.parallel is not None
        or len(nodes) != 1
        or not isinstance(node := nodes[0], SortPhysicalNode)
        or node.strategy is not SortStrategy.IN_MEMORY
        or type(node.operation.reverse) is not bool
        or _direct_field(node.operation.key) is None
        or not source.capabilities.reiterable
    ):
        return nodes
    descriptor = source.native_data
    if (
        not isinstance(descriptor, ArrowBatchSource)
        or descriptor.kind not in {"table", "record_batch"}
        or descriptor.materialized_data is None
    ):
        return nodes
    return (
        SortPhysicalNode(
            node.logical_ids,
            "arrow",
            node.operation,
            SortStrategy.ARROW_STABLE,
        ),
    )


def _physical_cache_key(
    query: Query,
    compiled_nodes: tuple[RowPhysicalNode | CompiledExpressionPhysicalNode | SortPhysicalNode, ...],
    physical_nodes: tuple[RowPhysicalNode | CompiledExpressionPhysicalNode | SortPhysicalNode, ...],
    decision: PlanDecision,
    payload: BackendPayload,
) -> tuple[PlanCacheKey | None, str]:
    """Return a source-free cache key only for structurally fingerprintable Python templates."""
    if payload.native_decision is not None and payload.native_decision.program is not None:
        return None, "native program retains source data"
    node_descriptors: list[str] = []
    for node in compiled_nodes:
        if not isinstance(node, CompiledExpressionPhysicalNode):
            return None, f"{type(node).__name__} is not source-free cacheable"
        if node.expression.source is ExpressionSource.ROW:
            return (
                None,
                "row expressions retain selector, literal, and backend bindings",
            )
        try:
            expression_fingerprint = ProgramFingerprint.from_expression(node.program).value
        except ValueError:
            return None, "expression cannot be structurally fingerprinted"
        if isinstance(node.operation, MapOp):
            operation_kind = "map"
        elif isinstance(node.operation, FilterOp):
            operation_kind = f"filter:{node.operation.negate}"
        else:
            return (
                None,
                f"{type(node.operation).__name__} has no cacheable execution semantics",
            )
        node_descriptors.append(f"{operation_kind}:{expression_fingerprint}")
    if not node_descriptors:
        return None, "query has no cacheable compiled expressions"
    source = query.logical.root
    try:
        source_node, _unary = unary_chain(source)
    except TypeError:
        return None, "relational query templates are not cacheable yet"
    capabilities = source_node.source.capabilities
    exact_size = capabilities.exact_size
    size_bucket = (
        None
        if exact_size is None
        else (
            31
            if exact_size <= 31
            else 255
            if exact_size <= 255
            else 4095
            if exact_size <= 4095
            else 4096
        )
    )
    physical_shape = ",".join(f"{type(node).__name__}:{node.engine}" for node in physical_nodes)
    decision_signature = (
        f"decision_engine:{decision.selected_engine}",
        f"decision_reason:{decision.reason}",
        f"decision_estimated_rows:{decision.estimated_rows}",
        f"decision_estimated_bytes:{decision.estimated_bytes}",
        *(f"decision_guard:{guard}" for guard in decision.guards),
    )
    arrow_prefix_signature = _arrow_prefix_cache_signature(payload)
    fingerprint = sha256(
        "|".join(
            (
                query.logical.engine,
                query.terminal.name,
                f"physical_shape:{physical_shape}",
                *decision_signature,
                *arrow_prefix_signature,
                *node_descriptors,
            )
        ).encode()
    ).hexdigest()
    return (
        PlanCacheKey(
            fingerprint,
            query.terminal.name,
            (capabilities.reiterable, size_bucket, capabilities.ordered),
        ),
        "source-free compiled expression template",
    )


def _arrow_prefix_cache_signature(payload: BackendPayload) -> tuple[str, ...]:
    """Describe Arrow payload behavior without retaining operations or their source objects."""
    prefix = payload.arrow_prefix
    if prefix is None:
        return ("arrow_prefix:none",)
    return (
        "arrow_prefix:present",
        f"arrow_prefix_operation_count:{prefix.operation_count}",
        f"arrow_prefix_boundary_reason:{prefix.boundary_reason.value}",
        f"arrow_prefix_guarded:{prefix.guarded}",
        f"arrow_prefix_first_only:{prefix.first_only}",
    )


def _contains_relational(root: LogicalNode) -> bool:
    """Return whether a logical tree requires recursive relational compilation."""
    return any(
        isinstance(node, (JoinNode, GroupAggregateNode, GlobalAggregateNode))
        for node in walk_logical(root)
    )


def _compile_relational_query(query: Query) -> PhysicalPlan:
    """Compile a binary/aggregate tree without opening any branch source."""
    identifiers = {id(node): index for index, node in enumerate(walk_logical(query.logical.root))}
    root = _compile_relational_node(query.logical.root, query, identifiers)
    source = _leftmost_source(root)
    return PhysicalPlan(
        source,
        (),
        query.terminal,
        PlanDecision("python", "relational physical tree uses the Python record executor"),
        query.logical.engine,
        query.logical.parallel,
        None,
        root,
    )


def _compile_relational_node(
    root: LogicalNode,
    query: Query,
    identifiers: dict[int, int],
) -> PhysicalRelNode:
    """Recursively compile maximal unary chains around relational logical nodes."""
    reversed_unary: list[UnaryNode] = []
    current = root
    while isinstance(current, UnaryNode):
        reversed_unary.append(current)
        current = current.input
    if isinstance(current, SourceNode):
        physical: PhysicalRelNode = SourcePhysicalNode(
            (identifiers[id(current)],),
            query.logical.engine,
            query.logical.parallel,
            current.source,
        )
    elif isinstance(current, JoinNode):
        from ..tabular.join import _compile_join_selector, _shared_join_names

        left = _compile_relational_node(current.left, query, identifiers)
        right = _compile_relational_node(current.right, query, identifiers)
        spec = CompiledJoinSpec(
            current.spec,
            _compile_join_selector(current.spec.left_on),
            _compile_join_selector(current.spec.right_on),
            frozenset(_shared_join_names(current.spec.left_on, current.spec.right_on)),
        )
        if current.spec.partitions is not None:
            strategy = JoinStrategy.GRACE_HASH
            reason = "explicit partitions request bounded grace hash execution"
        elif current.spec.how in {"inner", "left"} and current.spec.validate in {
            "1:1",
            "m:1",
        }:
            strategy = JoinStrategy.UNIQUE_RIGHT
            reason = "validation guarantees a unique right-side index"
        else:
            strategy = JoinStrategy.HASH_RIGHT
            reason = "preserve left encounter order with a stable right hash index"
        callable_join_validation = _native_callable_join_validation(
            current, root, query, left, right
        )
        physical = JoinPhysicalNode(
            (identifiers[id(current)],),
            query.logical.engine,
            query.logical.parallel,
            left,
            right,
            spec,
            strategy,
            reason,
            _arrow_unique_join_spec(current, root, query, left, right),
            _native_record_join_spec(current, root, query, left, right),
            callable_join_validation == "m:1",
            callable_join_validation == "m:m",
        )
    elif isinstance(current, GroupAggregateNode):
        from ..tabular.join import _compile_join_selector, _compose_composite_selector

        input_node = _compile_relational_node(current.input, query, identifiers)
        simple_sum = _simple_group_sum_spec(current.spec)
        closed_group = _closed_group_spec(current.spec)
        native_group_allowed = (
            query.logical.engine == "auto"
            and isinstance(input_node, SourcePhysicalNode)
            and input_node.source.capabilities.reiterable
        )
        key_selectors = tuple(selector for _name, selector in current.spec.keys)
        keys = tuple(_compile_join_selector(selector) for selector in key_selectors)
        physical = GroupAggregatePhysicalNode(
            (identifiers[id(current)],),
            query.logical.engine,
            query.logical.parallel,
            input_node,
            tuple(name for name, _selector in current.spec.keys),
            keys,
            (keys[0] if len(keys) == 1 else _compose_composite_selector(key_selectors, keys)),
            compile_aggregations(current.spec.aggregations),
            _spill_count_spec(current.spec),
            simple_sum,
            closed_group,
            _composite_count_sum_spec(current.spec),
            (_native_fixed_i64_group_spec(closed_group) if native_group_allowed else None),
            (_native_callable_group_spec(closed_group) if native_group_allowed else None),
            _arrow_group_sum_spec(simple_sum, input_node, query),
            (_native_group_sum_spec(simple_sum) if native_group_allowed else None),
            (_native_record_group_sum_spec(simple_sum) if native_group_allowed else None),
            current.spec.partitions,
            None if current.spec.tempdir is None else str(current.spec.tempdir),
            current.spec.limits,
        )
    elif isinstance(current, GlobalAggregateNode):
        input_node = _compile_relational_node(current.input, query, identifiers)
        physical = GlobalAggregatePhysicalNode(
            (identifiers[id(current)],),
            query.logical.engine,
            query.logical.parallel,
            input_node,
            compile_aggregations(current.aggregations),
            _exact_global_count_name(current.aggregations, input_node, query),
            _arrow_global_count_name(current.aggregations, input_node, query),
            _arrow_global_reduction_spec(current.aggregations, input_node, query),
        )
    else:
        raise TypeError(f"unsupported logical node: {type(current).__name__}")

    if not reversed_unary:
        return physical
    unary = tuple(reversed(reversed_unary))
    stages = tuple(_compile_node(identifiers[id(node)], node.operation) for node in unary)
    return PipelinePhysicalNode(
        tuple(identifiers[id(node)] for node in unary),
        query.logical.engine,
        query.logical.parallel,
        physical,
        stages,
    )


def _leftmost_source(root: PhysicalRelNode) -> Source[Any]:
    """Locate the compatibility source retained on a relational PhysicalPlan."""
    current = root
    while not isinstance(current, SourcePhysicalNode):
        if isinstance(current, _SINGLE_INPUT_RELATIONS):
            current = current.input
        elif isinstance(current, JoinPhysicalNode):
            current = current.left
        else:
            raise TypeError(f"unsupported physical relation: {type(current).__name__}")
    return current.source


def _compile_node(
    index: int, operation: Operation
) -> RowPhysicalNode | CompiledExpressionPhysicalNode | SortPhysicalNode:
    """Compile only analyzable map/filter expressions; preserve opaque callbacks as barriers."""
    from ..runtime.failpoints import hit

    if isinstance(operation, (MapOp, FilterOp)):
        candidate = operation.function if isinstance(operation, MapOp) else operation.predicate
        try:
            hit("expression.guard.before")
            expression = lower_expression(candidate)
        except TypeError:
            pass
        else:
            if expression.effect is not Effect.PYTHON_CALLBACK:
                try:
                    fingerprint = ProgramFingerprint.from_expression(
                        ExprProgram(expression.root, {}, expression.effect)
                    )
                except ValueError:
                    program = compile_expression(expression)
                else:
                    if expression.source is ExpressionSource.ROW:
                        # Row programs close over selector callables and literal identity.  A
                        # structural cache would bind a later query to the first graph's values.
                        program = compile_expression(expression)
                    else:
                        program = _EXPRESSION_PROGRAM_CACHE.get_or_compile(
                            fingerprint,
                            lambda: compile_expression(expression),
                        )
                node = CompiledExpressionPhysicalNode(
                    (index,),
                    "compiled_expression",
                    operation,
                    expression,
                    program,
                )
                hit("backend.convert.after")
                return node
    if isinstance(operation, SortOp):
        if operation.buffer_size is None:
            strategy = SortStrategy.IN_MEMORY
        elif operation.key is None or operation.key.__module__ == "fpstreams.expressions.selectors":
            strategy = SortStrategy.CACHED_EXTERNAL_MERGE
        else:
            strategy = SortStrategy.OPAQUE_CALLBACK
        engine = "python" if strategy is SortStrategy.IN_MEMORY else "python-spill"
        return SortPhysicalNode((index,), engine, operation, strategy)
    return RowPhysicalNode((index,), "python-row", operation)
