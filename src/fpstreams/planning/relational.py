"""Compile relational trees while delegating unary-stage compilation to the caller."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from ..collecting.aggregate_program import compile_aggregations
from ..expressions.selectors import (
    _normalize_direct_row_selector,
)
from ..physical.plan import (
    CompiledExpressionPhysicalNode,
    PhysicalPlan,
    PlanDecision,
    RowPhysicalNode,
    SortPhysicalNode,
)
from ..physical.relational import (
    CompiledJoinSpec,
    GlobalAggregatePhysicalNode,
    GroupAggregatePhysicalNode,
    JoinPhysicalNode,
    JoinStrategy,
    MergeSortedPhysicalNode,
    PhysicalRelNode,
    PipelinePhysicalNode,
    SortedGroupAggregatePhysicalNode,
    SortedJoinPhysicalNode,
    SourcePhysicalNode,
)
from .logical import (
    GlobalAggregateNode,
    GroupAggregateNode,
    JoinNode,
    LogicalNode,
    MergeSortedNode,
    Query,
    SortedGroupAggregateNode,
    SortedJoinNode,
    SourceNode,
    UnaryNode,
    walk_logical,
)
from .relational_specs import (
    _arrow_global_count_name,
    _arrow_global_multi_spec,
    _arrow_global_reduction_spec,
    _arrow_group_aggregate_spec,
    _arrow_unique_join_spec,
    _closed_group_spec,
    _composite_count_sum_spec,
    _exact_global_count_name,
    _native_callable_join_validation,
    _native_direct_join_spec,
    _native_fixed_i64_group_spec,
    _native_global_i64_aggregate_spec,
    _native_group_sum_spec,
    _native_i64_join_spec,
    _native_pair_i64_expr_group_sum_spec,
    _native_record_global_sum_spec,
    _native_record_group_sum_spec,
    _numpy_global_aggregate_spec,
    _numpy_group_aggregate_spec,
    _simple_group_sum_spec,
    _spill_count_spec,
)
from .source import (
    Source,
)
from .sync import Operation

CompileNode = Callable[
    [int, Operation], RowPhysicalNode | CompiledExpressionPhysicalNode | SortPhysicalNode
]

_SINGLE_INPUT_RELATIONS = (
    PipelinePhysicalNode,
    GroupAggregatePhysicalNode,
    SortedGroupAggregatePhysicalNode,
    GlobalAggregatePhysicalNode,
)


def _contains_relational(root: LogicalNode) -> bool:
    """Return whether a logical tree requires recursive relational compilation."""
    return any(
        isinstance(
            node,
            (
                JoinNode,
                MergeSortedNode,
                SortedJoinNode,
                GroupAggregateNode,
                SortedGroupAggregateNode,
                GlobalAggregateNode,
            ),
        )
        for node in walk_logical(root)
    )


def _compile_relational_query(query: Query, compile_node: CompileNode) -> PhysicalPlan:
    """Compile a binary/aggregate tree without opening any branch source."""
    identifiers = {id(node): index for index, node in enumerate(walk_logical(query.logical.root))}
    root = _compile_relational_node(query.logical.root, query, identifiers, compile_node)
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
    compile_node: CompileNode,
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
    elif isinstance(current, MergeSortedNode):
        from ..expressions.selectors import compile_selector

        child_query = Query(query.logical.with_engine("python"), query.terminal)
        physical = MergeSortedPhysicalNode(
            (identifiers[id(current)],),
            query.logical.engine,
            query.logical.parallel,
            _compile_relational_node(current.left, child_query, identifiers, compile_node),
            _compile_relational_node(current.right, child_query, identifiers, compile_node),
            compile_selector(current.key),
        )
    elif isinstance(current, SortedJoinNode):
        from ..tabular.join import _compile_join_selector, _shared_join_names

        child_query = Query(query.logical.with_engine("python"), query.terminal)
        physical = SortedJoinPhysicalNode(
            (identifiers[id(current)],),
            query.logical.engine,
            query.logical.parallel,
            _compile_relational_node(current.left, child_query, identifiers, compile_node),
            _compile_relational_node(current.right, child_query, identifiers, compile_node),
            CompiledJoinSpec(
                current.spec,
                _compile_join_selector(current.spec.left_on),
                _compile_join_selector(current.spec.right_on),
                frozenset(_shared_join_names(current.spec.left_on, current.spec.right_on)),
            ),
            current.max_right_group_rows,
            current.max_matches_per_left,
            current.max_output_rows,
        )
    elif isinstance(current, JoinNode):
        from ..tabular.join import _compile_join_selector, _shared_join_names

        left = _compile_relational_node(current.left, query, identifiers, compile_node)
        right = _compile_relational_node(current.right, query, identifiers, compile_node)
        spec = CompiledJoinSpec(
            current.spec,
            _compile_join_selector(current.spec.left_on),
            _compile_join_selector(current.spec.right_on),
            frozenset(_shared_join_names(current.spec.left_on, current.spec.right_on)),
        )
        direct_join = _native_direct_join_spec(current, root, query, left, right, spec)
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
            current, root, query, left, right, spec
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
            direct_join,
            _native_i64_join_spec(direct_join, spec),
            callable_join_validation == "m:1",
            callable_join_validation == "m:m",
        )
    elif isinstance(current, SortedGroupAggregateNode):
        from ..expressions.selectors import compile_selector

        # Keep upstream row reads lazy: auto materialization would consume future groups.
        child_query = Query(query.logical.with_engine("python"), query.terminal)
        input_node = _compile_relational_node(current.input, child_query, identifiers, compile_node)
        physical = SortedGroupAggregatePhysicalNode(
            (identifiers[id(current)],),
            query.logical.engine,
            query.logical.parallel,
            input_node,
            tuple((name, compile_selector(selector)) for name, selector in current.spec.keys),
            current.spec.aggregations,
        )
    elif isinstance(current, GroupAggregateNode):
        from ..tabular.join import _compile_join_selector, _compose_composite_selector

        input_node = _compile_relational_node(current.input, query, identifiers, compile_node)
        simple_sum = _simple_group_sum_spec(current.spec)
        closed_group = _closed_group_spec(current.spec)
        native_group_allowed = (
            query.logical.engine == "auto"
            and isinstance(input_node, SourcePhysicalNode)
            and input_node.source.capabilities.reiterable
        )
        key_selectors = tuple(
            _normalize_direct_row_selector(selector) for _name, selector in current.spec.keys
        )
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
            _arrow_group_aggregate_spec(
                closed_group,
                simple_sum,
                input_node,
                query,
                current.spec.keys[0][0] if len(current.spec.keys) == 1 else None,
            ),
            _numpy_group_aggregate_spec(
                closed_group,
                input_node,
                query,
                current.spec.keys[0][0] if len(current.spec.keys) == 1 else None,
            ),
            _native_pair_i64_expr_group_sum_spec(simple_sum, input_node, query),
            (_native_group_sum_spec(simple_sum) if native_group_allowed else None),
            (_native_record_group_sum_spec(simple_sum) if native_group_allowed else None),
            current.spec.partitions,
            None if current.spec.tempdir is None else str(current.spec.tempdir),
            current.spec.limits,
        )
    elif isinstance(current, GlobalAggregateNode):
        input_node = _compile_relational_node(current.input, query, identifiers, compile_node)
        physical = GlobalAggregatePhysicalNode(
            (identifiers[id(current)],),
            query.logical.engine,
            query.logical.parallel,
            input_node,
            compile_aggregations(current.aggregations),
            _exact_global_count_name(current.aggregations, input_node, query),
            _arrow_global_count_name(current.aggregations, input_node, query),
            _arrow_global_reduction_spec(current.aggregations, input_node, query)
            or _arrow_global_multi_spec(current.aggregations, input_node, query),
            _numpy_global_aggregate_spec(current.aggregations, input_node, query),
            _native_global_i64_aggregate_spec(current.aggregations, input_node, query),
            _native_record_global_sum_spec(current.aggregations, input_node, query),
        )
    else:
        raise TypeError(f"unsupported logical node: {type(current).__name__}")

    if not reversed_unary:
        return physical
    unary = tuple(reversed(reversed_unary))
    stages = tuple(compile_node(identifiers[id(node)], node.operation) for node in unary)
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
        elif isinstance(
            current, (JoinPhysicalNode, MergeSortedPhysicalNode, SortedJoinPhysicalNode)
        ):
            current = current.left
        else:
            raise TypeError(f"unsupported physical relation: {type(current).__name__}")
    return current.source
