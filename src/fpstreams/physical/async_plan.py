"""Source-safe physical representation of immutable async flow plans."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..planning.async_ import (
    AsyncLogicalPlan,
    AsyncOperation,
    _AsyncSource,
    _BufferTimeout,
    _CombineLatest,
    _Debounce,
    _Delay,
    _Filter,
    _MapAsync,
    _Merge,
    _MergeMap,
    _Prefetch,
    _SessionWindow,
    _SwitchMap,
    _Tap,
    _Throttle,
    _Timeout,
)
from ..planning.semantics import AsyncTerminalName


@dataclass(frozen=True, slots=True)
class AsyncQuery:
    """An async logical plan paired with the public terminal being evaluated."""

    plan: AsyncLogicalPlan[Any]
    terminal: AsyncTerminalName


@dataclass(frozen=True, slots=True)
class AsyncPhysicalNode:
    """Base value for a source-safe async execution stage."""

    logical_ids: tuple[int, ...]
    name: str


@dataclass(frozen=True, slots=True)
class AsyncSerialStage(AsyncPhysicalNode):
    """A contiguous serial map/filter/tap run."""

    operations: tuple[AsyncOperation, ...]


@dataclass(frozen=True, slots=True)
class AsyncMapNode(AsyncPhysicalNode):
    """Bounded concurrent map operation."""

    operation: _MapAsync


@dataclass(frozen=True, slots=True)
class AsyncMergeNode(AsyncPhysicalNode):
    """Concurrent interleaving of the primary and additional sources."""

    operation: _Merge


@dataclass(frozen=True, slots=True)
class AsyncCombineLatestNode(AsyncPhysicalNode):
    """Latest-value combination across several independently pulled sources."""

    operation: _CombineLatest


@dataclass(frozen=True, slots=True)
class AsyncMergeMapNode(AsyncPhysicalNode):
    """Bounded concurrent mapping whose active inner streams are interleaved."""

    operation: _MergeMap


@dataclass(frozen=True, slots=True)
class AsyncSwitchMapNode(AsyncPhysicalNode):
    """Latest-only mapping that cancels and closes every superseded inner."""

    operation: _SwitchMap


@dataclass(frozen=True, slots=True)
class AsyncTimerNode(AsyncPhysicalNode):
    """One timer-driven operation; timer ownership is supplied by its executor."""

    operation: _Timeout | _Debounce | _BufferTimeout | _SessionWindow | _Delay | _Throttle


@dataclass(frozen=True, slots=True)
class AsyncPrefetchNode(AsyncPhysicalNode):
    """One explicitly bounded asynchronous pull-ahead operation."""

    operation: _Prefetch


@dataclass(frozen=True, slots=True)
class AsyncSerialOperationNode(AsyncPhysicalNode):
    """One non-concurrent serial operation executed by the physical scheduler."""

    operation: AsyncOperation


@dataclass(frozen=True, slots=True)
class AsyncPhysicalPlan:
    """Fully source-safe async physical plan ready for one evaluation."""

    source: _AsyncSource[Any]
    nodes: tuple[AsyncPhysicalNode, ...]
    terminal: AsyncTerminalName


def _is_serial(operation: AsyncOperation) -> bool:
    """Return whether an operation can join a callback-order-preserving serial stage."""
    return isinstance(operation, (_Filter, _Tap)) or (
        isinstance(operation, _MapAsync)
        and operation.concurrency == 1
        and operation.timeout is None
    )


def compile_async_query(query: AsyncQuery) -> AsyncPhysicalPlan:
    """Map immutable operations to explicit nodes without opening the source."""

    nodes: list[AsyncPhysicalNode] = []
    operations = query.plan.operations
    index = 0
    while index < len(operations):
        operation = operations[index]
        if _is_serial(operation):
            end = index + 1
            while end < len(operations) and _is_serial(operations[end]):
                end += 1
            nodes.append(
                AsyncSerialStage(
                    tuple(range(index, end)),
                    "serial",
                    operations[index:end],
                )
            )
            index = end
            continue
        if isinstance(operation, _MapAsync):
            node: AsyncPhysicalNode = AsyncMapNode((index,), "map_concurrent", operation)
        elif isinstance(operation, _Merge):
            node = AsyncMergeNode((index,), "merge", operation)
        elif isinstance(operation, _CombineLatest):
            node = AsyncCombineLatestNode((index,), "combine_latest", operation)
        elif isinstance(operation, _MergeMap):
            node = AsyncMergeMapNode((index,), "merge_map", operation)
        elif isinstance(operation, _SwitchMap):
            node = AsyncSwitchMapNode((index,), "switch_map", operation)
        elif isinstance(operation, _Prefetch):
            node = AsyncPrefetchNode((index,), "prefetch", operation)
        elif isinstance(operation, _SessionWindow):
            node = AsyncTimerNode((index,), "session_window", operation)
        elif isinstance(operation, (_Timeout, _Debounce, _BufferTimeout, _Delay, _Throttle)):
            node = AsyncTimerNode((index,), type(operation).__name__.removeprefix("_"), operation)
        else:
            node = AsyncSerialOperationNode(
                (index,), type(operation).__name__.removeprefix("_"), operation
            )
        nodes.append(node)
        index += 1
    return AsyncPhysicalPlan(query.plan.source, tuple(nodes), query.terminal)
