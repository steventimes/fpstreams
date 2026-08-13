"""Exhaustive dispatch for individual asynchronous flow operations."""

from __future__ import annotations

from collections.abc import AsyncIterator
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

SUPPORTED_ASYNC_OPERATION_TYPES: tuple[type[object], ...] = (
    _MapAsync,
    _Filter,
    _Tap,
    _Throttle,
    _FlatMap,
    _Merge,
    _MergeMap,
    _SwitchMap,
    _CombineLatest,
    _Timeout,
    _Debounce,
    _Delay,
    _BufferTimeout,
    _Delay,
    _Throttle,
    _Take,
    _Drop,
    _TakeWhile,
    _TakeWhileInclusive,
    _DropWhile,
    _Chunk,
    _BatchBySize,
    _Window,
    _Pairwise,
    _GroupRuns,
    _Fold,
    _Unique,
    _Enumerate,
    _Zip,
    _ZipLongest,
    _Intersperse,
    _Concat,
    _Cross,
    _Scan,
    _ScanRight,
    _SwitchMap,
    _Prepend,
    _Append,
    _MapFirst,
    _MapLast,
    _Collapse,
)


def apply_async_operation(
    source: AsyncIterator[Any], operation: _AsyncOperation
) -> AsyncIterator[Any]:
    if isinstance(operation, _MapAsync):
        return map_concurrent(source, operation)
    if isinstance(operation, _Filter):
        return _iterators._filter(source, operation)
    if isinstance(operation, _Tap):
        return _iterators._tap(source, operation)
    if isinstance(operation, _FlatMap):
        return _iterators._flat_map(source, operation)
    if isinstance(operation, _Merge):
        return merge(source, operation)
    if isinstance(operation, _MergeMap):
        return merge_map(source, operation)
    if isinstance(operation, _SwitchMap):
        return switch_map(source, operation)
    if isinstance(operation, _CombineLatest):
        return combine_latest(source, operation)
    if isinstance(operation, _Timeout):
        return timeout(source, operation)
    if isinstance(operation, _Debounce):
        return debounce(source, operation)
    if isinstance(operation, _BufferTimeout):
        return buffer_timeout(source, operation)
    if isinstance(operation, _Delay):
        return delay(source, operation)
    if isinstance(operation, _Throttle):
        return throttle(source, operation)
    if isinstance(operation, _Take):
        return _iterators._take(source, operation)
    if isinstance(operation, _Drop):
        return _iterators._drop(source, operation)
    if isinstance(operation, _TakeWhile):
        return _iterators._take_while(source, operation)
    if isinstance(operation, _TakeWhileInclusive):
        return _iterators._take_while_inclusive(source, operation)
    if isinstance(operation, _DropWhile):
        return _iterators._drop_while(source, operation)
    if isinstance(operation, _Chunk):
        return _iterators._chunk(source, operation)
    if isinstance(operation, _BatchBySize):
        return _iterators._batch_by_size(source, operation)
    if isinstance(operation, _Window):
        return _iterators._window(source, operation)
    if isinstance(operation, _Pairwise):
        return _iterators._pairwise(source)
    if isinstance(operation, _GroupRuns):
        return _iterators._group_runs(source, operation)
    if isinstance(operation, _Fold):
        return _iterators._fold(source, operation)
    if isinstance(operation, _Unique):
        return _iterators._unique(source, operation)
    if isinstance(operation, _Enumerate):
        return _iterators._enumerate(source, operation)
    if isinstance(operation, _Zip):
        return _iterators._zip(source, operation)
    if isinstance(operation, _ZipLongest):
        return _iterators._zip_longest(source, operation)
    if isinstance(operation, _Intersperse):
        return _iterators._intersperse(source, operation)
    if isinstance(operation, _Concat):
        return _iterators._concat(source, operation)
    if isinstance(operation, _Cross):
        return _iterators._cross(source, operation)
    if isinstance(operation, _Scan):
        return _iterators._scan(source, operation)
    if isinstance(operation, _ScanRight):
        return _iterators._scan_right(source, operation)
    if isinstance(operation, _Prepend):
        return _iterators._prepend(source, operation)
    if isinstance(operation, _Append):
        return _iterators._append(source, operation)
    if isinstance(operation, _MapFirst):
        return _iterators._map_first(source, operation)
    if isinstance(operation, _MapLast):
        return _iterators._map_last(source, operation)
    if isinstance(operation, _Collapse):
        return _iterators._collapse(source, operation)
    raise TypeError(f"unsupported asynchronous operation: {type(operation).__name__}")
