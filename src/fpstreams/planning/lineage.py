"""Describe conservative logical field dependencies without opening record sources."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field, replace
from typing import Any, TypeAlias, TypeGuard

from ..expressions.row import RowExpr
from ..expressions.row_ir import Index, InputRow, RowExprAnalysis, _children, analyze_row_node
from .arrow import _row_stage_descriptor
from .logical import (
    GlobalAggregateNode,
    GroupAggregateNode,
    JoinNode,
    LogicalNode,
    MergeSortedNode,
    SortedGroupAggregateNode,
    SortedJoinNode,
    SourceNode,
    UnaryNode,
)
from .sync import DropOp, FilterOp, MapOp, TakeOp

Dependencies: TypeAlias = frozenset[tuple[str, str]] | None


def _live_expression(expression: Any) -> TypeGuard[RowExpr]:
    """Require the retained expression graph to match the callable that will execute."""
    from ..expressions.row_eval import LazyRowEvaluator
    from ..tabular.rows import _CANONICAL_ROW_EXPR_CALL, _CANONICAL_ROW_EXPR_CALL_CODE

    return (
        type(expression) is RowExpr
        and type(expression._evaluate) is LazyRowEvaluator
        and expression._evaluate.node is expression._node
        and RowExpr.__call__ is _CANONICAL_ROW_EXPR_CALL
        and _CANONICAL_ROW_EXPR_CALL.__code__ is _CANONICAL_ROW_EXPR_CALL_CODE
    )


def _union(parts: list[Dependencies]) -> Dependencies:
    if any(part is None for part in parts):
        return None
    return frozenset().union(*(part for part in parts if part is not None))


@dataclass(kw_only=True)
class _Fields:
    """Track explicit overrides plus an optional identity passthrough for unseen fields."""

    analyze: Callable[[Any], RowExprAnalysis] = field(repr=False)
    default: str | None = None
    values: dict[str, Dependencies] = field(default_factory=dict)
    complete: bool = False
    rows: Dependencies = frozenset()
    excluded: frozenset[str] = frozenset()

    def resolve(self, name: str) -> Dependencies:
        if name in self.excluded:
            return None
        if name in self.values:
            return self.values[name]
        return None if self.default is None else frozenset({(self.default, name)})

    def select(self, selector: Any) -> Dependencies:
        if type(selector) is str:
            return self.resolve(selector.split(".")[0])
        if not _live_expression(selector):
            return None
        pending = [selector._node]
        visited: set[int] = set()
        while pending:
            node = pending.pop()
            if id(node) in visited:
                continue
            visited.add(id(node))
            if type(node) in {InputRow, Index}:
                return None
            pending.extend(_children(node))
        fields = self.analyze(selector._node).fields
        return None if fields is None else _union([self.resolve(name) for name in fields])


def _computed_columns(function: Any) -> tuple[tuple[str, RowExpr], ...] | None:
    """Read live project closures, including expressions not yet lazily compiled."""
    from ..tabular.rows import (
        _CANONICAL_ROW_EXPR_CALL,
        _CANONICAL_ROW_EXPR_CALL_CODE,
        _CANONICAL_WITH_COLUMNS_ENRICHER_CODE,
    )

    if (
        function.__code__ is not _CANONICAL_WITH_COLUMNS_ENRICHER_CODE
        or function.__code__.co_freevars != ("selectors",)
        or function.__closure__ is None
        or RowExpr.__call__ is not _CANONICAL_ROW_EXPR_CALL
        or _CANONICAL_ROW_EXPR_CALL.__code__ is not _CANONICAL_ROW_EXPR_CALL_CODE
    ):
        return None
    try:
        selectors = function.__closure__[0].cell_contents
    except ValueError:
        return None
    if type(selectors) is not list:
        return None
    result = []
    for entry in selectors:
        if type(entry) is not tuple or len(entry) != 2:
            return None
        name, expression = entry
        if type(name) is not str or type(expression) is not RowExpr:
            return None
        if not _live_expression(expression):
            return None
        result.append((name, expression))
    return tuple(result)


def _map_fields(source: _Fields, operation: MapOp) -> _Fields:
    from ..tabular.rows import _materialized_rename_spec, _materialized_select_spec

    descriptor = _row_stage_descriptor(operation.function)
    if descriptor is None:
        return _Fields(analyze=source.analyze, rows=None)
    if descriptor.kind == "select":
        projection = _materialized_select_spec(operation.function)
        if projection is not None:
            return _Fields(
                values={name: source.select(selector) for name, selector in projection},
                complete=True,
                rows=source.rows,
                analyze=source.analyze,
            )
    elif descriptor.kind == "with_columns":
        columns = _computed_columns(operation.function)
        if columns is not None:
            if any(source.analyze(expression._node).opaque for _name, expression in columns):
                return _Fields(analyze=source.analyze, rows=None)
            return replace(
                source,
                values=source.values
                | {name: source.select(selector) for name, selector in columns},
                excluded=source.excluded - {name for name, _selector in columns},
            )
    elif descriptor.kind == "rename":
        renames = _materialized_rename_spec(operation.function)
        if renames is not None:
            mapping = {
                before: after
                for before, after in renames
                if not source.complete or before in source.values
            }
            values = {
                mapping.get(name, name): dependencies
                for name, dependencies in source.values.items()
            }
            for before, after in mapping.items():
                values[after] = source.resolve(before)
            excluded = (source.excluded | mapping.keys()) - set(mapping.values())
            return replace(source, values=values, excluded=frozenset(excluded))
    return _Fields(analyze=source.analyze, rows=None)


def _aggregate_fields(source: _Fields, keys: Any, aggregations: Any) -> _Fields:
    from ..collecting.aggregation import native_group_aggregation

    values = {name: source.select(selector) for name, selector in keys}
    for name, aggregation in aggregations:
        hint = native_group_aggregation(aggregation)
        values[name] = None if hint is None else source.select(hint.selector)
    rows = _union([source.rows, *(source.select(selector) for _name, selector in keys)])
    return _Fields(analyze=source.analyze, values=values, complete=True, rows=rows)


def _unary_fields(node: UnaryNode, source: _Fields) -> _Fields:
    operation = node.operation
    if type(operation) is MapOp:
        return _map_fields(source, operation)
    if type(operation) is FilterOp and _live_expression(operation.predicate):
        if not source.analyze(operation.predicate._node).opaque:
            return replace(
                source,
                values=source.values.copy(),
                rows=_union([source.rows, source.select(operation.predicate)]),
            )
    elif type(operation) in {TakeOp, DropOp}:
        return source
    return _Fields(analyze=source.analyze, rows=None)


def _join_fields(node: JoinNode | SortedJoinNode, left: _Fields, right: _Fields) -> _Fields:
    # Schemas can be learned from explicit projections, never by sampling rows.
    from ..tabular.join import _join_targets, _shared_join_names

    if not (
        left.complete
        and right.complete
        and type(node.spec.left_on) is str
        and type(node.spec.right_on) is str
        and type(node.spec.suffix) is str
    ):
        return _Fields(analyze=left.analyze, rows=None)
    shared = _shared_join_names(node.spec.left_on, node.spec.right_on)
    values = left.values.copy()
    try:
        targets = (
            ()
            if node.spec.how in {"semi", "anti"}
            else _join_targets(
                tuple(left.values),
                tuple(right.values),
                shared_names=shared,
                suffix=node.spec.suffix,
            )
        )
    except ValueError:
        return _Fields(analyze=left.analyze, rows=None)
    for name, target in targets:
        values[target] = right.values[name]
    for name in shared:
        if name in values:
            values[name] = _union([left.resolve(name), right.resolve(name)])
    return _Fields(
        values=values,
        complete=True,
        analyze=left.analyze,
        rows=_union(
            [
                left.rows,
                right.rows,
                left.select(node.spec.left_on),
                right.select(node.spec.right_on),
            ]
        ),
    )


def describe_lineage(root: LogicalNode) -> dict[str, Any]:
    """Return versioned static dependencies, distinguishing row selection from value inputs."""
    sources: list[str] = []
    recent: tuple[Any, RowExprAnalysis] | None = None

    def analyze(node: Any) -> RowExprAnalysis:
        # One export owns one retained root, including across unknown field states.
        nonlocal recent
        if recent is None or recent[0] is not node:
            recent = (node, analyze_row_node(node))
        return recent[1]

    results: dict[int, _Fields] = {}
    pending = [(root, False)]
    while pending:
        node, visited = pending.pop()
        if id(node) in results:
            continue
        if not visited:
            pending.append((node, True))
            if isinstance(
                node, (UnaryNode, GroupAggregateNode, SortedGroupAggregateNode, GlobalAggregateNode)
            ):
                pending.append((node.input, False))
            elif isinstance(node, (JoinNode, SortedJoinNode, MergeSortedNode)):
                pending.extend(((node.right, False), (node.left, False)))
            continue
        if isinstance(node, SourceNode):
            identifier = f"source_{len(sources)}"
            sources.append(identifier)
            result = _Fields(analyze=analyze, default=identifier)
        elif isinstance(node, UnaryNode):
            result = _unary_fields(node, results[id(node.input)])
        elif isinstance(node, (GroupAggregateNode, SortedGroupAggregateNode)):
            result = _aggregate_fields(
                results[id(node.input)], node.spec.keys, node.spec.aggregations
            )
        elif isinstance(node, GlobalAggregateNode):
            result = _aggregate_fields(results[id(node.input)], (), node.aggregations)
        elif isinstance(node, (JoinNode, SortedJoinNode)):
            result = _join_fields(node, results[id(node.left)], results[id(node.right)])
        else:
            result = _Fields(analyze=analyze, rows=None)
        results[id(node)] = result

    def serialize(dependencies: Dependencies) -> list[dict[str, str]] | None:
        return (
            None
            if dependencies is None
            else [{"source": source, "field": name} for source, name in sorted(dependencies)]
        )

    final = results[id(root)]
    return {
        "schema_version": 1,
        "kind": "fpstreams.static_field_dependencies",
        "sources": sources,
        "fields": {name: serialize(value) for name, value in final.values.items()},
        "passthrough_source": final.default,
        "passthrough_excludes": sorted(final.excluded),
        "complete_schema": final.complete,
        "row_dependencies": serialize(final.rows),
    }
