"""Execute compiled synchronous physical plans without plan reconstruction."""

from __future__ import annotations

from collections.abc import Callable, Generator, Iterator
from typing import Any, cast

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
from ..planning.arrow import PlannedRowCallable, RowStageDescriptor
from ..planning.logical import Pipeline
from ..planning.sync import FilterOp, MapOp, Operation
from ..runtime.query import QueryRuntime
from . import execute
from .sync import execute_operations


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
                    PlannedRowCallable(
                        evaluator,
                        role="compiled_rows_predicate",
                        descriptor=RowStageDescriptor("where", predicate=node.expression.root),
                    )
                    if node.expression.source is ExpressionSource.ROW
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
                return execute_relational(cast(Any, plan.root), active_runtime)

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
