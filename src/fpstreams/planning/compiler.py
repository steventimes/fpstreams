"""Compile M1 logical queries to source-safe M2 physical plans."""

from __future__ import annotations

from hashlib import sha256
from typing import Any

from ..collecting.aggregate_program import native_mean_only
from ..errors import NativeUnsupportedError
from ..expressions.program import ExprProgram, compile_expression
from ..expressions.selectors import (
    _direct_field,
)
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
from .arrow import (
    plan_arrow_first_prefix,
    plan_arrow_prefix,
    plan_arrow_reduction_prefix,
)
from .arrow_source import ArrowBatchSource
from .logical import (
    LogicalPlan,
    Query,
    TerminalSpec,
    linear_pipeline,
    unary_chain,
)
from .native import (
    EngineDecision,
    select_materializing_engine,
    select_terminal_engine,
    validate_terminal,
)
from .numpy import plan_numpy_prefix
from .plan_cache import PhysicalPlanTemplate, PlanCache, PlanCacheKey
from .relational import _compile_relational_query, _contains_relational
from .source import (
    Source,
)
from .sync import FilterOp, MapOp, Operation, SortOp

_NATIVE_TERMINALS = frozenset(
    {
        "count",
        "sum",
        "mean",
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
_ARROW_REDUCTION_TERMINALS = frozenset({"sum", "min", "max", "mean", "statistics"})
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
        return _compile_relational_query(query, _compile_relational_stage)
    source, unary_nodes = unary_chain(query.logical.root)
    pipeline = linear_pipeline(query.logical)
    terminal = query.terminal.name
    native_terminal = (
        "mean" if terminal == "aggregate" and native_mean_only(query.terminal.options) else terminal
    )
    arrow_reduction_allowed = terminal in _ARROW_REDUCTION_TERMINALS and (
        terminal != "sum"
        or (len(query.terminal.arguments) == 1 and type(query.terminal.arguments[0]) is int)
    )
    arrow_prefix = (
        plan_arrow_prefix(pipeline)
        if terminal in _ARROW_PREFIX_TERMINALS
        else plan_arrow_reduction_prefix(pipeline)
        if arrow_reduction_allowed
        else plan_arrow_first_prefix(pipeline)
        if terminal == "first"
        else None
    )
    numpy_prefix = plan_numpy_prefix(pipeline) if terminal == "list" else None
    native_decision = (
        select_materializing_engine(pipeline)
        if terminal in _MATERIALIZING_TERMINALS
        else (
            select_terminal_engine(pipeline, validate_terminal(native_terminal))
            if native_terminal in _NATIVE_TERMINALS
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
    payload = BackendPayload(native_decision, arrow_prefix, numpy_prefix)
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
    exact_size = source_node.source.current_exact_size()
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
    numpy_prefix_signature = _numpy_prefix_cache_signature(payload)
    fingerprint = sha256(
        "|".join(
            (
                query.logical.engine,
                query.terminal.name,
                f"physical_shape:{physical_shape}",
                *decision_signature,
                *arrow_prefix_signature,
                *numpy_prefix_signature,
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


def _numpy_prefix_cache_signature(payload: BackendPayload) -> tuple[str, ...]:
    """Describe NumPy payload behavior without retaining query-local prefix values."""
    prefix = payload.numpy_prefix
    if prefix is None:
        return ("numpy_prefix:none",)
    return (
        "numpy_prefix:present",
        f"numpy_prefix_operation_count:{prefix.operation_count}",
        f"numpy_prefix_guarded:{prefix.guarded}",
    )


def _compile_relational_stage(
    index: int, operation: Operation
) -> RowPhysicalNode | CompiledExpressionPhysicalNode | SortPhysicalNode:
    """Read the current stage compiler after each preceding compilation callback."""
    return _compile_node(index, operation)


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
