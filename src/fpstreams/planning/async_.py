"""Immutable operation nodes and sources for asynchronous flow plans."""

from __future__ import annotations

from collections.abc import AsyncIterable, AsyncIterator, Callable, Iterable, Iterator
from dataclasses import dataclass
from threading import Lock
from typing import Any, Generic, TypeVar

from ..errors import FlowConsumedError

T = TypeVar("T")


async def _from_sync(iterator: Iterator[T]) -> AsyncIterator[T]:
    try:
        for item in iterator:
            yield item
    finally:
        close = getattr(iterator, "close", None)
        if callable(close):
            close()


def _to_async_iterator(source: AsyncIterable[T] | Iterable[T]) -> AsyncIterator[T]:
    if isinstance(source, AsyncIterable):
        return source.__aiter__()
    return _from_sync(iter(source))


class _AsyncSource(Generic[T]):
    __slots__ = ("_claimed", "_lock", "_opener", "reiterable")

    def __init__(
        self,
        opener: Callable[[], AsyncIterator[T]],
        *,
        reiterable: bool,
    ) -> None:
        self._opener = opener
        self.reiterable = reiterable
        self._claimed = False
        self._lock = Lock()

    @classmethod
    def from_value(cls, source: AsyncIterable[T] | Iterable[T]) -> _AsyncSource[T]:
        one_shot = isinstance(source, (AsyncIterator, Iterator))
        return cls(lambda: _to_async_iterator(source), reiterable=not one_shot)

    @classmethod
    def defer(cls, factory: Callable[[], AsyncIterable[T] | Iterable[T]]) -> _AsyncSource[T]:
        return cls(lambda: _to_async_iterator(factory()), reiterable=True)

    def open(self) -> AsyncIterator[T]:
        if not self.reiterable:
            with self._lock:
                if self._claimed:
                    raise FlowConsumedError("async source has already been consumed")
                self._claimed = True
        return self._opener()


@dataclass(frozen=True, slots=True)
class _MapAsync:
    function: Callable[[Any], Any]
    concurrency: int
    ordered: bool
    timeout: float | None


@dataclass(frozen=True, slots=True)
class _Filter:
    predicate: Callable[[Any], Any]


@dataclass(frozen=True, slots=True)
class _Tap:
    action: Callable[[Any], Any]


@dataclass(frozen=True, slots=True)
class _FlatMap:
    function: Callable[[Any], Any]


@dataclass(frozen=True, slots=True)
class _Merge:
    sources: tuple[_AsyncSource[Any], ...]


@dataclass(frozen=True, slots=True)
class _MergeMap:
    function: Callable[[Any], Any]
    concurrency: int


@dataclass(frozen=True, slots=True)
class _SwitchMap:
    function: Callable[[Any], Any]


@dataclass(frozen=True, slots=True)
class _CombineLatest:
    sources: tuple[_AsyncSource[Any], ...]


@dataclass(frozen=True, slots=True)
class _Timeout:
    seconds: float


@dataclass(frozen=True, slots=True)
class _Debounce:
    seconds: float


@dataclass(frozen=True, slots=True)
class _BufferTimeout:
    max_count: int
    seconds: float


@dataclass(frozen=True, slots=True)
class _Delay:
    seconds: float


@dataclass(frozen=True, slots=True)
class _Throttle:
    max_count: int
    per: float


@dataclass(frozen=True, slots=True)
class _Take:
    count: int


@dataclass(frozen=True, slots=True)
class _Drop:
    count: int


@dataclass(frozen=True, slots=True)
class _TakeWhile:
    predicate: Callable[[Any], Any]


@dataclass(frozen=True, slots=True)
class _TakeWhileInclusive:
    predicate: Callable[[Any], Any]


@dataclass(frozen=True, slots=True)
class _DropWhile:
    predicate: Callable[[Any], Any]


@dataclass(frozen=True, slots=True)
class _Chunk:
    size: int


@dataclass(frozen=True, slots=True)
class _BatchBySize:
    max_size: int
    max_count: int | None
    get_size: Callable[[Any], Any]
    strict: bool


@dataclass(frozen=True, slots=True)
class _Window:
    size: int
    step: int


@dataclass(frozen=True, slots=True)
class _Pairwise:
    pass


@dataclass(frozen=True, slots=True)
class _GroupRuns:
    key: Callable[[Any], Any]


@dataclass(frozen=True, slots=True)
class _Fold:
    initializer: Callable[[], Any]
    function: Callable[[Any, Any], Any]


@dataclass(frozen=True, slots=True)
class _Unique:
    key: Callable[[Any], Any]


@dataclass(frozen=True, slots=True)
class _Enumerate:
    start: int


@dataclass(frozen=True, slots=True)
class _Zip:
    source: _AsyncSource[Any]
    strict: bool


@dataclass(frozen=True, slots=True)
class _ZipLongest:
    source: _AsyncSource[Any]
    fillvalue: Any


@dataclass(frozen=True, slots=True)
class _Intersperse:
    separator: Any


@dataclass(frozen=True, slots=True)
class _Concat:
    sources: tuple[_AsyncSource[Any], ...]


@dataclass(frozen=True, slots=True)
class _Cross:
    source: _AsyncSource[Any]
    max_right: int | None


@dataclass(frozen=True, slots=True)
class _Scan:
    initial: Any
    function: Callable[[Any, Any], Any]


@dataclass(frozen=True, slots=True)
class _ScanRight:
    initial: Any
    function: Callable[[Any, Any], Any]
    max_items: int | None


@dataclass(frozen=True, slots=True)
class _Prepend:
    values: tuple[Any, ...]


@dataclass(frozen=True, slots=True)
class _Append:
    values: tuple[Any, ...]


@dataclass(frozen=True, slots=True)
class _MapFirst:
    function: Callable[[Any], Any]


@dataclass(frozen=True, slots=True)
class _MapLast:
    function: Callable[[Any], Any]


@dataclass(frozen=True, slots=True)
class _Collapse:
    collapsible: Callable[[Any, Any], Any]
    merger: Callable[[Any, Any], Any]


_AsyncOperation = (
    _MapAsync
    | _Filter
    | _Tap
    | _FlatMap
    | _Merge
    | _MergeMap
    | _SwitchMap
    | _CombineLatest
    | _Timeout
    | _Debounce
    | _BufferTimeout
    | _Delay
    | _Throttle
    | _Take
    | _Drop
    | _TakeWhile
    | _TakeWhileInclusive
    | _DropWhile
    | _Chunk
    | _BatchBySize
    | _Window
    | _Pairwise
    | _GroupRuns
    | _Fold
    | _Unique
    | _Enumerate
    | _Zip
    | _ZipLongest
    | _Intersperse
    | _Concat
    | _Cross
    | _Scan
    | _ScanRight
    | _Prepend
    | _Append
    | _MapFirst
    | _MapLast
    | _Collapse
)


@dataclass(frozen=True, slots=True)
class _AsyncPlan(Generic[T]):
    source: _AsyncSource[Any]
    operations: tuple[_AsyncOperation, ...] = ()
