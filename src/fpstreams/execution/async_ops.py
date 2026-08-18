"""Map each asynchronous plan node to its lazy iterator implementation."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from typing import Any

from ..planning.async_ import (
    _Append,
    _AsyncOperation,
    _BatchBySize,
    _BufferTimeout,
    _Chunk,
    _Collapse,
    _CombineLatest,
    _Concat,
    _Cross,
    _Debounce,
    _Delay,
    _Drop,
    _DropWhile,
    _Enumerate,
    _Filter,
    _FlatMap,
    _Fold,
    _GroupRuns,
    _Intersperse,
    _MapAsync,
    _MapFirst,
    _MapLast,
    _Merge,
    _MergeMap,
    _Pairwise,
    _Prepend,
    _Scan,
    _ScanRight,
    _SwitchMap,
    _Take,
    _TakeWhile,
    _TakeWhileInclusive,
    _Tap,
    _Throttle,
    _Timeout,
    _Unique,
    _Window,
    _Zip,
    _ZipLongest,
)
from ..planning.async_utils import close_async_iterators as close_async_iterators
from . import async_iterators as _iterators
from .async_concurrency import (
    buffer_timeout,
    combine_latest,
    debounce,
    delay,
    map_concurrent,
    merge,
    merge_map,
    switch_map,
    throttle,
    timeout,
)


def _pairwise(source: AsyncIterator[Any], _operation: _Pairwise) -> AsyncIterator[Any]:
    """Adapt the argument-free pairwise iterator to the operation-handler signature."""
    return _iterators._pairwise(source)


AsyncOperationHandler = Callable[..., AsyncIterator[Any]]
ASYNC_OPERATION_HANDLERS: dict[type[object], AsyncOperationHandler] = {
    _MapAsync: map_concurrent,
    _Filter: _iterators._filter,
    _Tap: _iterators._tap,
    _FlatMap: _iterators._flat_map,
    _Merge: merge,
    _MergeMap: merge_map,
    _SwitchMap: switch_map,
    _CombineLatest: combine_latest,
    _Timeout: timeout,
    _Debounce: debounce,
    _BufferTimeout: buffer_timeout,
    _Delay: delay,
    _Throttle: throttle,
    _Take: _iterators._take,
    _Drop: _iterators._drop,
    _TakeWhile: _iterators._take_while,
    _TakeWhileInclusive: _iterators._take_while_inclusive,
    _DropWhile: _iterators._drop_while,
    _Chunk: _iterators._chunk,
    _BatchBySize: _iterators._batch_by_size,
    _Window: _iterators._window,
    _Pairwise: _pairwise,
    _GroupRuns: _iterators._group_runs,
    _Fold: _iterators._fold,
    _Unique: _iterators._unique,
    _Enumerate: _iterators._enumerate,
    _Zip: _iterators._zip,
    _ZipLongest: _iterators._zip_longest,
    _Intersperse: _iterators._intersperse,
    _Concat: _iterators._concat,
    _Cross: _iterators._cross,
    _Scan: _iterators._scan,
    _ScanRight: _iterators._scan_right,
    _Prepend: _iterators._prepend,
    _Append: _iterators._append,
    _MapFirst: _iterators._map_first,
    _MapLast: _iterators._map_last,
    _Collapse: _iterators._collapse,
}
SUPPORTED_ASYNC_OPERATION_TYPES: tuple[type[object], ...] = tuple(ASYNC_OPERATION_HANDLERS)


def apply_async_operation(
    source: AsyncIterator[Any], operation: _AsyncOperation
) -> AsyncIterator[Any]:
    """Construct the iterator layer registered for the operation's exact type.

    Unsupported subclasses are rejected rather than inheriting another node's
    handler implicitly.
    """
    handler = ASYNC_OPERATION_HANDLERS.get(type(operation))
    if handler is None:
        raise TypeError(f"unsupported asynchronous operation: {type(operation).__name__}")
    return handler(source, operation)
