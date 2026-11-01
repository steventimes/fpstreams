"""Own asynchronous sources and define the immutable nodes consumed by async executors."""

from __future__ import annotations

from collections.abc import AsyncIterable, AsyncIterator, Callable, Iterable, Iterator
from dataclasses import dataclass, replace
from threading import Lock
from types import FunctionType
from typing import Any, Generic, TypeVar, cast

from ..errors import FlowConsumedError
from ..runtime.iterators import closing_iterators
from .semantics import (
    Cardinality,
    OrderingGuarantee,
    Replayability,
    StreamFacts,
    TerminationEvidence,
    facts_from_capabilities,
)
from .source import _range_size

T = TypeVar("T")
_NO_RETAINED_SEQUENCE = object()


class _SyncAsyncIterator(AsyncIterator[T]):
    """Own a synchronous iterator before its first asynchronous pull."""

    __slots__ = ("_active", "_iterator", "_owns_iterator")

    def __init__(self, iterator: Iterator[T]) -> None:
        self._iterator: Iterator[T] | None = iterator
        self._active: Iterator[T] | None = None
        self._owns_iterator = True

    def __aiter__(self) -> _SyncAsyncIterator[T]:
        return self

    async def __anext__(self) -> T:
        iterator = self._iterator
        if iterator is None:
            raise StopAsyncIteration
        try:
            if self._active is None:
                self._active = iter(iterator)
            try:
                return next(self._active)
            except StopIteration:
                pass
        except BaseException as error:
            self._close(error)
            raise
        self._close()
        del iterator
        raise StopAsyncIteration

    def _close(self, active_error: BaseException | None = None) -> None:
        iterator = self._iterator
        if iterator is None and active_error is None:
            return
        self._iterator = None
        self._active = None
        try:
            # Retain the generator context's conversion of a close-hook StopIteration.
            with closing_iterators(
                () if iterator is None or not self._owns_iterator else (iterator,)
            ):
                if active_error is not None:
                    raise active_error
        except StopIteration as error:
            raise RuntimeError("async generator raised StopIteration") from error
        except StopAsyncIteration as error:
            # The former async generator could not expose this as normal exhaustion.
            raise RuntimeError("async generator raised StopAsyncIteration") from error

    async def aclose(self) -> None:
        """Close once, including when no item has been requested."""
        try:
            self._close()
        except GeneratorExit:
            return


def _to_async_iterator(source: AsyncIterable[T] | Iterable[T]) -> AsyncIterator[T]:
    """Open an async iterable directly or wrap a synchronous iterable in the async adapter."""
    if isinstance(source, AsyncIterable):
        return source.__aiter__()
    return _SyncAsyncIterator(iter(source))


class _AsyncSource(Generic[T]):
    """Own an async opener, semantic facts, and the claim state for one-shot sources."""

    __slots__ = (
        "_claimed",
        "_live_size_data",
        "_lock",
        "_opener",
        "_retained_opener",
        "_retained_sequence",
        "facts",
        "reiterable",
    )

    def __init__(
        self,
        opener: Callable[[], AsyncIterator[T]],
        *,
        reiterable: bool,
        facts: StreamFacts | None = None,
    ) -> None:
        """Store the opener and derive conservative ordered-source facts when none are supplied."""
        self._opener = opener
        self._retained_opener: Callable[[], AsyncIterator[T]] | None = None
        self._retained_sequence: object = _NO_RETAINED_SEQUENCE
        self._live_size_data: object = _NO_RETAINED_SEQUENCE
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
        source_type = type(source)
        native_sequence = source_type is list or source_type is tuple or source_type is range
        safely_sized = native_sequence or any(
            source_type is kind for kind in (str, bytes, dict, set, frozenset)
        )
        try:
            exact_size = len(source) if safely_sized else None  # type: ignore[arg-type]
        except OverflowError:
            if source_type is not range:
                raise
            exact_size = _range_size(cast(range, source))
        ordered = not isinstance(source, (set, frozenset))
        facts = facts_from_capabilities(
            reiterable=not one_shot,
            exact_size=exact_size,
            ordered=ordered,
        )

        def opener() -> AsyncIterator[T]:
            """Adapt the retained construction-time source on each evaluation."""
            return _to_async_iterator(source)

        if one_shot:
            opener.__dict__["_one_shot_value"] = source
        result = cls(opener, reiterable=not one_shot, facts=facts)
        if not one_shot and not safely_sized:
            result.facts = replace(result.facts, replayability=Replayability.UNKNOWN)
        if safely_sized:
            result._retained_opener = opener
            result._live_size_data = source
        if native_sequence:
            result._retained_sequence = source
        return result

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

    def one_shot_value(self) -> object:
        """Return a one-shot value retained by the current canonical opener, if any."""
        opener = self._opener
        if type(opener) is FunctionType:
            return opener.__dict__.get("_one_shot_value", _NO_RETAINED_SEQUENCE)
        return _NO_RETAINED_SEQUENCE

    def retained_sequence(self) -> list[Any] | tuple[Any, ...] | range | None:
        """Return a still-canonical exact synchronous sequence retained by this source."""
        retained = self._retained_sequence
        if self._opener is not self._retained_opener:
            return None
        retained_type = type(retained)
        if retained_type is list or retained_type is tuple or retained_type is range:
            return cast(list[Any] | tuple[Any, ...] | range, retained)
        return None

    def current_exact_size(self) -> int | None:
        """Read the live length of a retained exact sequence without opening it."""
        retained = self.retained_sequence()
        try:
            return None if retained is None else len(retained)
        except OverflowError:
            if type(retained) is not range:
                raise
            return _range_size(retained)

    def current_facts(self) -> StreamFacts:
        """Refresh trusted built-in sizes without consuming or opening the source."""
        if self._retained_opener is not None and self._opener is not self._retained_opener:
            return replace(
                self.facts,
                termination=TerminationEvidence.UNKNOWN,
                cardinality=Cardinality.unknown(),
                ordering=OrderingGuarantee.UNKNOWN,
            )
        if self._live_size_data is _NO_RETAINED_SEQUENCE:
            return self.facts
        try:
            size = len(self._live_size_data)  # type: ignore[arg-type]
        except OverflowError:
            if type(self._live_size_data) is not range:
                raise
            size = _range_size(self._live_size_data)
        return replace(
            self.facts,
            termination=TerminationEvidence.PROVEN_FINITE,
            cardinality=Cardinality.exact(size),
        )


@dataclass(frozen=True, slots=True)
class _MapAsync:
    """Map items with bounded concurrency, buffering, ordering, and an optional timeout."""

    function: Callable[[Any], Any]
    concurrency: int
    ordered: bool
    timeout: float | None
    buffer: int


@dataclass(frozen=True, slots=True)
class _Filter:
    """Keep items whose synchronous or awaitable predicate result is truthy."""

    predicate: Callable[[Any], Any]


@dataclass(frozen=True, slots=True)
class _Tap:
    """Invoke and optionally await an action for each item, then emit the unchanged item."""

    action: Callable[[Any], Any]


@dataclass(frozen=True, slots=True)
class _FilterMap:
    """Map each input once, await its result if needed, and discard ``None``."""

    function: Callable[[Any], Any]


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
class _SessionWindow:
    """Emit bounded tuples separated by ``idle_for`` seconds without a new item."""

    idle_for: float
    max_count: int


@dataclass(frozen=True, slots=True)
class _Prefetch:
    """Pull ahead by at most ``capacity`` accepted, not-yet-emitted values."""

    capacity: int


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
    strict: bool = False


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
class _WindowReduce:
    """Reduce stepped windows with an associative, nonmutating synchronous callback."""

    size: int
    step: int
    combine: Callable[[Any, Any], Any]


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
    """Reduce adjacent runs with an awaitable binary merger or synchronous Collector."""

    collapsible: Callable[[Any, Any], Any]
    merger: Callable[..., Any]


AsyncOperation = (
    _MapAsync
    | _Filter
    | _FilterMap
    | _Tap
    | _FlatMap
    | _Merge
    | _MergeMap
    | _SwitchMap
    | _CombineLatest
    | _Timeout
    | _Debounce
    | _BufferTimeout
    | _SessionWindow
    | _Prefetch
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
    | _WindowReduce
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
class AsyncLogicalPlan(Generic[T]):
    """Bind an async source to an immutable sequence of asynchronous operation nodes."""

    source: _AsyncSource[Any]
    operations: tuple[AsyncOperation, ...] = ()
