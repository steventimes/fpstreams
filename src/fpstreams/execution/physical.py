"""Execute compiled synchronous physical plans without plan reconstruction."""

from __future__ import annotations

from collections.abc import Callable, Generator, Iterator
from types import FunctionType
from typing import Any, Literal, TypeVar, cast

from ..expressions.typed_ir import ExpressionSource
from ..physical.plan import (
    BackendPayload,
    CompiledExpressionPhysicalNode,
    PhysicalNode,
    PhysicalPlan,
    RowPhysicalNode,
    SortPhysicalNode,
    SortStrategy,
)
from ..planning.arrow import RowStageDescriptor, _register_row_stage
from ..planning.logical import Pipeline
from ..planning.sync import FilterOp, MapOp, Operation
from ..runtime.query import QueryRuntime
from . import execute
from ._pair_dict import (
    PairDictConsumer,
    consume_pair_filter_to_dict,
    consume_pair_map_to_dict,
    consume_pair_side_filter_to_dict,
    is_canonical_pair_dict_consumer,
)
from .sync import execute_operations, open_operations

R = TypeVar("R")


class PhysicalExecutionError(RuntimeError):
    """Raised when a physical node has no safe canonical executor."""


_RUNTIME_READY = object()


def _iterate_runtime_values(
    values: Callable[[], Iterator[Any]],
    runtime: QueryRuntime,
) -> Generator[Any, None, None]:
    """Enter runtime ownership before opening values and preserve the primary error."""
    active_error: BaseException | None = None
    try:
        # The factory consumes this private handshake immediately.  It activates the
        # generator's cleanup scope without calling ``values`` or pulling the source.
        yield _RUNTIME_READY
        yield from values()
    except BaseException as error:
        active_error = error
        runtime.close(None if isinstance(error, GeneratorExit) else error)
        raise
    finally:
        if active_error is None:
            runtime.close()


def _close_runtime_with_values(
    values: Callable[[], Iterator[Any]],
    runtime: QueryRuntime,
) -> Iterator[Any]:
    """Activate cleanup ownership without opening or pulling the physical value source."""
    iterator = _iterate_runtime_values(values, runtime)
    ready = next(iterator)
    if ready is not _RUNTIME_READY:  # pragma: no cover - closed internal invariant
        raise RuntimeError("runtime iterator failed to enter its cleanup scope")
    return iterator


def operations_from_physical_nodes(
    nodes: tuple[PhysicalNode, ...],
) -> tuple[Operation, ...]:
    """Materialize row-operation payloads from compiled physical stages."""
    operations: list[Operation] = []
    for node in nodes:
        if isinstance(node, RowPhysicalNode):
            operations.append(node.operation)
        elif isinstance(node, CompiledExpressionPhysicalNode):
            evaluator = node.program.evaluator()
            if isinstance(node.operation, MapOp):
                operations.append(MapOp(evaluator, node.operation.name))
            elif isinstance(node.operation, FilterOp):
                predicate = (
                    _register_row_stage(
                        evaluator,
                        RowStageDescriptor("where", predicate=node.expression.root),
                    )
                    if node.expression.source is ExpressionSource.ROW
                    and type(evaluator) is FunctionType
                    else evaluator
                )
                operations.append(FilterOp(predicate, node.operation.negate, node.operation.name))
            else:
                raise PhysicalExecutionError(
                    f"unsupported compiled operation {type(node.operation).__name__}"
                )
        elif isinstance(node, SortPhysicalNode):
            operations.append(node.operation)
        else:
            raise PhysicalExecutionError(f"unknown physical node {type(node).__name__}")
    return tuple(operations)


def _is_direct_streaming_python_plan(plan: PhysicalPlan) -> bool:
    """Return whether a compiled iteration plan can be consumed in the caller's stack."""
    payload = plan.backend_payload
    return (
        plan.terminal.name == "iterate"
        and plan.root is None
        and plan.engine == "python"
        and plan.decision.selected_engine == "python"
        and isinstance(payload, BackendPayload)
        and payload.arrow_prefix is None
        and payload.native_decision is not None
        and payload.native_decision.engine == "python"
        and all(
            type(node) is RowPhysicalNode and node.engine == "python-row" for node in plan.nodes
        )
    )


def consume_physical(  # noqa: C901 - terminal fast paths share one ownership stack
    plan: PhysicalPlan,
    consumer: Callable[[Iterator[Any]], R],
    pipeline: Pipeline | None = None,
) -> R:
    """Consume a physical plan while keeping sink and iterator cleanup in one stack."""
    pair_consumer = consumer if type(consumer) is PairDictConsumer else None
    if pair_consumer is not None and pipeline is not None and plan.root is None:
        from ._pair_dict import (
            try_consume_pair_unique_to_dict,
            try_consume_pair_value_filter_to_dict,
        )
        from ._pair_row_filter import try_consume_pair_row_filter_to_dict

        operations = operations_from_physical_nodes(plan.nodes)
        handled, result = try_consume_pair_unique_to_dict(
            plan,
            pipeline,
            operations,
            pair_consumer,
            open_operations,
        )
        if handled:
            return cast(R, result)
        handled, result = try_consume_pair_row_filter_to_dict(
            plan,
            pipeline,
            operations,
            pair_consumer,
            open_operations,
        )
        if handled:
            return cast(R, result)
        handled, result = try_consume_pair_value_filter_to_dict(
            plan,
            pipeline,
            operations,
            pair_consumer,
            open_operations,
        )
        if handled:
            return cast(R, result)
    if _is_direct_streaming_python_plan(plan):
        runtime = QueryRuntime()
        active_error: BaseException | None = None
        try:
            operations = operations_from_physical_nodes(plan.nodes)
            from ._pair_dict import prepare_pair_value_map_to_dict

            pair_value_map_snapshot = prepare_pair_value_map_to_dict(
                plan,
                pipeline,
                operations,
                pair_consumer,
                open_operations,
            )
            try:
                source_iterator = plan.source.open()
            except StopIteration as error:
                raise RuntimeError("generator raised StopIteration") from error
            pair_filter = None
            pair_map = None
            if (
                pair_consumer is not None
                and operations
                and is_canonical_pair_dict_consumer(pair_consumer)
            ):
                from ..planning._pair_stages import PairFilterDescriptor, PairMapDescriptor
                from ..runtime.failpoints import has_active_failpoints

                tail = operations[-1]
                if not has_active_failpoints():
                    if (
                        type(tail) is MapOp
                        and type(tail.function) is PairMapDescriptor
                        and (
                            tail.function.side in {"key", "value"}
                            or (
                                tail.function.side == "pair"
                                and pair_consumer.policy in {"first", "last"}
                            )
                        )
                    ):
                        pair_map = tail.function
                    elif (
                        type(tail) is FilterOp
                        and type(tail.predicate) is PairFilterDescriptor
                        and (
                            tail.predicate.target in {"pair", "row"}
                            or (
                                tail.predicate.target in {"key", "value"}
                                and pair_consumer.policy in {"first", "last"}
                            )
                        )
                        and tail.negate is False
                    ):
                        pair_filter = tail.predicate
            prefix = (
                operations[:-1] if pair_map is not None or pair_filter is not None else operations
            )
            with open_operations(source_iterator, prefix, runtime=runtime) as iterator:
                if pair_map is not None and pair_consumer is not None:
                    from ._pair_dict import try_consume_pair_value_map_to_dict_opened

                    handled, mapped = try_consume_pair_value_map_to_dict_opened(
                        plan,
                        pipeline,
                        operations,
                        pair_consumer,
                        pair_map,
                        iterator,
                        pair_value_map_snapshot,
                    )
                    if handled:
                        return cast(R, mapped)
                    return cast(
                        R,
                        consume_pair_map_to_dict(iterator, pair_map, pair_consumer.policy),
                    )
                if pair_filter is not None and pair_consumer is not None:
                    if pair_filter.target == "row":
                        from ._pair_row_filter import consume_pair_row_filter_to_dict

                        return cast(
                            R,
                            consume_pair_row_filter_to_dict(
                                iterator,
                                pair_filter,
                                pair_consumer.policy,
                            ),
                        )
                    return cast(
                        R,
                        (
                            consume_pair_filter_to_dict(
                                iterator,
                                pair_filter,
                                pair_consumer.policy,
                            )
                            if pair_filter.target == "pair"
                            else consume_pair_side_filter_to_dict(
                                iterator,
                                pair_filter,
                                cast(Literal["first", "last"], pair_consumer.policy),
                            )
                        ),
                    )
                return consumer(iterator)
        except BaseException as error:
            active_error = error
            runtime.close(None if isinstance(error, GeneratorExit) else error)
            raise
        finally:
            if active_error is None:
                runtime.close()

    iterator = execute_physical(plan)
    from ..runtime.iterators import closing_iterators

    with closing_iterators((iterator,)):
        return consumer(iterator)


def execute_physical(plan: PhysicalPlan, runtime: QueryRuntime | None = None) -> Iterator[Any]:
    """Execute one already-selected physical plan and close its query runtime."""
    active_runtime = runtime or QueryRuntime()
    relation_values: Callable[[], Iterator[Any]] | None = None
    operations: tuple[Operation, ...] = ()
    try:
        payload = plan.backend_payload
        if payload is not None and not isinstance(payload, BackendPayload):
            raise PhysicalExecutionError(f"unknown backend payload {type(payload).__name__}")
        if plan.root is not None:
            from .relational import execute_relational

            def open_relation_values() -> Iterator[Any]:
                """Open the selected relational executor under runtime ownership."""
                return execute_relational(cast(Any, plan.root), active_runtime, plan)

            relation_values = open_relation_values
        else:
            operations = operations_from_physical_nodes(plan.nodes)
    except BaseException as error:
        active_runtime.close(error)
        raise
    if relation_values is not None:
        return _close_runtime_with_values(relation_values, active_runtime)

    def values() -> Iterator[Any]:
        """Execute the selected linear backend."""
        if (
            len(plan.nodes) == 1
            and isinstance(node := plan.nodes[0], SortPhysicalNode)
            and node.strategy is SortStrategy.ARROW_STABLE
        ):
            from .arrow import try_retained_arrow_stable_sort

            arrow_sorted = try_retained_arrow_stable_sort(plan)
            if arrow_sorted is not None:
                yield from arrow_sorted
                return
        if payload is None or (
            payload.native_decision is not None
            and payload.native_decision.engine == "python"
            and payload.arrow_prefix is None
        ):
            yield from execute_operations(
                plan.source.open(),
                operations,
                runtime=active_runtime,
            )
        else:
            yield from execute(
                Pipeline(plan.source, operations, plan.engine, plan.parallel),
                decision=None if payload is None else payload.native_decision,
                arrow_prefix=None if payload is None else payload.arrow_prefix,
                runtime=active_runtime,
            )

    return _close_runtime_with_values(values, active_runtime)
