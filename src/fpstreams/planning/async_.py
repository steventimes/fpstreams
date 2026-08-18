"""Own asynchronous sources and define the immutable nodes consumed by async executors."""

from __future__ import annotations

from collections.abc import AsyncIterable, AsyncIterator, Callable, Iterable, Iterator
from dataclasses import dataclass
from threading import Lock
from typing import Any, Generic, TypeVar

from ..errors import FlowConsumedError
from .semantics import StreamFacts, facts_from_capabilities

T = TypeVar("T")


async def _from_sync(iterator: Iterator[T]) -> AsyncIterator[T]:
    """Adapt a synchronous iterator to async iteration and close it on every exit path."""
    try:
        for item in iterator:
            yield item
    finally:
        close = getattr(iterator, "close", None)
        if callable(close):
            close()


def _to_async_iterator(source: AsyncIterable[T] | Iterable[T]) -> AsyncIterator[T]:
    """Open an async iterable directly or wrap a synchronous iterable in the async adapter."""
    if isinstance(source, AsyncIterable):
        return source.__aiter__()
    return _from_sync(iter(source))


class _AsyncSource(Generic[T]):
    """Own an async opener, semantic facts, and the claim state for one-shot sources."""

    __slots__ = ("_claimed", "_lock", "_opener", "facts", "reiterable")

    def __init__(
        self,
        opener: Callable[[], AsyncIterator[T]],
        *,
        reiterable: bool,
        facts: StreamFacts | None = None,
    ) -> None:
        """Store the opener and derive conservative ordered-source facts when none are supplied."""
        self._opener = opener
        self.reiterable = reiterable
        self._claimed = False
        self._lock = Lock()
        self.facts = facts or facts_from_capabilities(
            reiterable=reiterable,
            exact_size=None,
            ordered=True,
        )

    @classmethod
    def from_value(cls, source: AsyncIterable[T] | Iterable[T]) -> _AsyncSource[T]:
        """Wrap a sync or async iterable and infer one-shot, exact-size, and ordering facts."""
        one_shot = isinstance(source, (AsyncIterator, Iterator))
        exact_size = len(source) if type(source) in (list, tuple, range, str, bytes, dict) else None  # type: ignore[arg-type]
        ordered = not isinstance(source, (set, frozenset))
        facts = facts_from_capabilities(
            reiterable=not one_shot,
            exact_size=exact_size,
            ordered=ordered,
        )
        return cls(lambda: _to_async_iterator(source), reiterable=not one_shot, facts=facts)

    @classmethod
    def defer(
        cls,
        factory: Callable[[], AsyncIterable[T] | Iterable[T]],
        *,
        facts: StreamFacts | None = None,
    ) -> _AsyncSource[T]:
        """Create a reopenable async source by invoking ``factory`` for each evaluation."""
        return cls(
            lambda: _to_async_iterator(factory()),
            reiterable=True,
            facts=facts
            or facts_from_capabilities(
                reiterable=True,
                exact_size=None,
                ordered=True,
                reopenable=True,
            ),
        )

    def open(self) -> AsyncIterator[T]:
        """Atomically claim a one-shot source, then call its async opener."""
        if not self.reiterable:
            with self._lock:
                if self._claimed:
                    raise FlowConsumedError("async source has already been consumed")
                self._claimed = True
        return self._opener()


@dataclass(frozen=True, slots=True)
class _MapAsync:
    """Map items with bounded concurrency, optional ordering, and an optional per-call timeout."""

    function: Callable[[Any], Any]
    concurrency: int
    ordered: bool
    timeout: float | None


@dataclass(frozen=True, slots=True)
class _Filter:
    """Keep items whose synchronous or awaitable predicate result is truthy."""

    predicate: Callable[[Any], Any]


@dataclass(frozen=True, slots=True)
class _Tap:
    """Invoke and optionally await an action for each item, then emit the unchanged item."""

    action: Callable[[Any], Any]


@dataclass(frozen=True, slots=True)
class _FlatMap:
    """Expand each item into a synchronous or asynchronous iterable returned by ``function``."""

    function: Callable[[Any], Any]


@dataclass(frozen=True, slots=True)
class _Merge:
    """Interleave the primary stream and additional sources as their next items become ready."""

    sources: tuple[_AsyncSource[Any], ...]


@dataclass(frozen=True, slots=True)
class _MergeMap:
    """Map items to inner streams and merge them under one shared concurrency limit."""

    function: Callable[[Any], Any]
    concurrency: int


@dataclass(frozen=True, slots=True)
class _SwitchMap:
    """Map each item to a stream while cancelling and closing the previous inner stream."""

    function: Callable[[Any], Any]


@dataclass(frozen=True, slots=True)
class _CombineLatest:
    """Emit the latest tuple after every source has produced once and any source updates."""

    sources: tuple[_AsyncSource[Any], ...]


@dataclass(frozen=True, slots=True)
class _Timeout:
    """Limit the wait for each next upstream item to ``seconds``."""

    seconds: float


@dataclass(frozen=True, slots=True)
class _Debounce:
    """Emit only the latest item after ``seconds`` of quiet, flushing it at source end."""

    seconds: float


@dataclass(frozen=True, slots=True)
class _BufferTimeout:
    """Emit tuple batches when ``max_count`` is reached or the first-item timer expires."""

    max_count: int
    seconds: float


@dataclass(frozen=True, slots=True)
class _Delay:
    """Delay the first upstream pull by ``seconds`` without spacing later items."""

    seconds: float


@dataclass(frozen=True, slots=True)
class _Throttle:
    """Rate-limit output to ``max_count`` items in each rolling ``per``-second window."""

    max_count: int
    per: float


@dataclass(frozen=True, slots=True)
class _Take:
    """Emit at most the first ``count`` items, then close upstream."""

    count: int


@dataclass(frozen=True, slots=True)
class _Drop:
    """Suppress the first ``count`` items and emit the remainder."""

    count: int


@dataclass(frozen=True, slots=True)
class _TakeWhile:
    """Emit a prefix while the optional awaitable predicate is truthy, excluding the failure."""

    predicate: Callable[[Any], Any]


@dataclass(frozen=True, slots=True)
class _TakeWhileInclusive:
    """Emit a prefix through and including the first item whose predicate is false."""

    predicate: Callable[[Any], Any]


@dataclass(frozen=True, slots=True)
class _DropWhile:
    """Drop the leading items with truthy predicate results, then emit every later item."""

    predicate: Callable[[Any], Any]


@dataclass(frozen=True, slots=True)
class _Chunk:
    """Group consecutive items into non-overlapping tuples of at most ``size`` values."""

    size: int


@dataclass(frozen=True, slots=True)
class _BatchBySize:
    """Batch by accumulated async size and an optional count cap.

    In strict mode, an individual item larger than ``max_size`` raises instead of forming an
    oversized singleton batch.
    """

    max_size: int
    max_count: int | None
    get_size: Callable[[Any], Any]
    strict: bool


@dataclass(frozen=True, slots=True)
class _Window:
    """Emit full sliding windows separated by ``step``, or one initially partial window."""

    size: int
    step: int


@dataclass(frozen=True, slots=True)
class _Pairwise:
    """Emit overlapping pairs of adjacent input items."""

    pass


@dataclass(frozen=True, slots=True)
class _GroupRuns:
    """Group contiguous items with equal synchronous or awaitable keys into tuples."""

    key: Callable[[Any], Any]


@dataclass(frozen=True, slots=True)
class _Fold:
    """Await initialization and reductions, then emit the single final accumulator state."""

    initializer: Callable[[], Any]
    function: Callable[[Any, Any], Any]


@dataclass(frozen=True, slots=True)
class _Unique:
    """Emit the first item for each distinct synchronous or awaitable key."""

    key: Callable[[Any], Any]


@dataclass(frozen=True, slots=True)
class _Enumerate:
    """Pair each item with a consecutive index beginning at ``start``."""

    start: int


@dataclass(frozen=True, slots=True)
class _Zip:
    """Pair the primary stream with another async source, optionally requiring equal lengths."""

    source: _AsyncSource[Any]
    strict: bool


@dataclass(frozen=True, slots=True)
class _ZipLongest:
    """Pair two streams through the longer input, filling missing positions with ``fillvalue``."""

    source: _AsyncSource[Any]
    fillvalue: Any


@dataclass(frozen=True, slots=True)
class _Intersperse:
    """Insert ``separator`` between consecutive input items."""

    separator: Any


@dataclass(frozen=True, slots=True)
class _Concat:
    """Emit the primary stream followed by each additional async source in order."""

    sources: tuple[_AsyncSource[Any], ...]


@dataclass(frozen=True, slots=True)
class _Cross:
    """Form a Cartesian product after buffering the right source under an optional cap."""

    source: _AsyncSource[Any]
    max_right: int | None


@dataclass(frozen=True, slots=True)
class _Scan:
    """Emit each awaited left-to-right accumulator state after consuming an item."""

    initial: Any
    function: Callable[[Any, Any], Any]


@dataclass(frozen=True, slots=True)
class _ScanRight:
    """Buffer input under the optional ``max_items`` cap and emit awaited right-fold states."""

    initial: Any
    function: Callable[[Any, Any], Any]
    max_items: int | None


@dataclass(frozen=True, slots=True)
class _Prepend:
    """Emit configured synchronous values before the upstream stream."""

    values: tuple[Any, ...]


@dataclass(frozen=True, slots=True)
class _Append:
    """Emit configured synchronous values after the upstream stream completes."""

    values: tuple[Any, ...]


@dataclass(frozen=True, slots=True)
class _MapFirst:
    """Apply a synchronous or awaitable function only to the first item."""

    function: Callable[[Any], Any]


@dataclass(frozen=True, slots=True)
class _MapLast:
    """Hold one item back and apply a synchronous or awaitable function only to the last."""

    function: Callable[[Any], Any]


@dataclass(frozen=True, slots=True)
class _Collapse:
    """Merge adjacent collapsible items into awaited aggregates for each contiguous run."""

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
    """Bind an async source to an immutable sequence of asynchronous operation nodes."""

    source: _AsyncSource[Any]
    operations: tuple[_AsyncOperation, ...] = ()
