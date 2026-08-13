"""Lazy synchronous pipelines and terminal operations."""

from __future__ import annotations

import operator
import os
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Generic, TypeVar, cast

from ..execution import (
    execute,
)
from ..expressions.selectors import Selector, compile_selector
from ..planning.explain import PlanExplanation
from ..planning.gather import Gatherer
from ..planning.source import Source
from ..planning.sync import (
    AppendOp,
    ChunkOp,
    CollapseOp,
    ConcatOp,
    CrossOp,
    DropOp,
    DropWhileOp,
    Engine,
    EnumerateOp,
    FilterOp,
    FlatMapOp,
    GatherOp,
    GroupRunsOp,
    IntersperseOp,
    MapFirstOp,
    MapLastOp,
    MapOp,
    Operation,
    PairwiseOp,
    ParallelBackend,
    ParallelMapOp,
    ParallelSettings,
    Plan,
    PrependOp,
    ScanOp,
    ScanRightOp,
    SortOp,
    TakeOp,
    TakeWhileInclusiveOp,
    TakeWhileOp,
    TapOp,
    UniqueOp,
    WindowOp,
    ZipLongestOp,
    ZipOp,
)
from ..primitives.result import Err, Ok, Result
from .flow_terminals import FlowTerminalsMixin

T = TypeVar("T")
R = TypeVar("R")
C = TypeVar("C")
U = TypeVar("U")
_MISSING = object()


def _default_item_size(value: Any) -> int:
    return len(value)


@dataclass(slots=True)
class _BatchState(Generic[T]):
    items: list[T]
    size: int = 0


@dataclass(slots=True)
class _FoldState(Generic[R]):
    value: R


class Flow(FlowTerminalsMixin[T], Generic[T]):
    """A lazy description of work over an iterable source."""

    __slots__ = ("_plan",)

    def __init__(self, source: Iterable[T] | Source[T] | Plan) -> None:
        if isinstance(source, Plan):
            self._plan = source
        else:
            owned = source if isinstance(source, Source) else Source.from_iterable(source)
            self._plan = Plan(owned)

    @staticmethod
    def of(*items: R) -> Flow[R]:
        """Create a flow from positional items.

        Args:
            *items: Items emitted by the new pipeline.

        Returns:
            A new lazy `Flow` over the supplied source or values.
        """
        return Flow(items)

    @staticmethod
    def from_iterable(source: Iterable[R]) -> Flow[R]:
        """Create a flow over an iterable source.

        Args:
            source: The iterable, async iterable, or data source to read lazily.

        Returns:
            A new lazy `Flow` over the supplied source or values.
        """
        return Flow(source)

    @staticmethod
    def empty() -> Flow[Any]:
        """Create a flow that emits no items.

        Returns:
            A new lazy `Flow` over the supplied source or values.
        """
        return Flow(())

    @staticmethod
    def of_nullable(value: R | None) -> Flow[R]:
        """Create an empty flow for None, otherwise emit the value once.

        Args:
            value: The value consumed by this operation.

        Returns:
            A new lazy `Flow` over the supplied source or values.
        """
        return Flow(()) if value is None else Flow((value,))

    @staticmethod
    def iterate(seed: R, function: Callable[[R], R]) -> Flow[R]:
        """Emit seed, then repeatedly apply function to the previous value.

        Args:
            seed: The first value emitted or used to initialize the sequence.
            function: The callable applied by this operation.

        Returns:
            A new lazy `Flow` over the supplied source or values.
        """

        def values() -> Iterator[R]:
            current = seed
            while True:
                yield current
                current = function(current)

        return Flow(Source.defer(values))

    @staticmethod
    def generate(supplier: Callable[[], R]) -> Flow[R]:
        """Create an infinite flow by calling supplier for each item.

        Args:
            supplier: A zero-argument callable that supplies a value or iterable.

        Returns:
            A new lazy `Flow` over the supplied source or values.
        """

        def values() -> Iterator[R]:
            while True:
                yield supplier()

        return Flow(Source.defer(values))

    def __iter__(self) -> Iterator[T]:
        return execute(self._plan, auto_native=False)

    def _append(self, operation: Operation) -> Flow[Any]:
        return Flow(self._plan.append(operation))

    def map(self, function: Callable[[T], R]) -> Flow[R]:
        """Apply function to each item lazily.

        `map` is lazy: the callable runs only when a terminal operation or iteration consumes
        the flow.

        Args:
            function: The callable applied by this operation.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        # Store the transform in the immutable plan; no item is mapped yet.
        if self._plan.parallel is not None:
            settings = self._plan.parallel
            return self.map_parallel(
                function,
                workers=settings.workers,
                backend=settings.backend,
                ordered=settings.ordered,
                buffer=settings.buffer,
            )
        return self._append(MapOp(function))

    def map_parallel(
        self,
        function: Callable[[T], R],
        *,
        workers: int | None = None,
        backend: ParallelBackend = "thread",
        ordered: bool = True,
        buffer: int | None = None,
    ) -> Flow[R]:
        """Map items in a bounded thread or process pool.

        Only a bounded number of tasks are submitted at once, preventing a slow consumer from
        creating unbounded work.

        Args:
            function: The callable applied by this operation.
            workers: The number of worker threads or processes. None chooses a runtime default.
            backend: The parallel backend: thread or process.
            ordered: Whether results must preserve source encounter order.
            buffer: The maximum number of submitted tasks awaiting consumption.

        Returns:
            A new lazy `Flow` representing this operation.

        Raises:
            ValueError: If workers or buffer is less than one, or backend is unsupported.
        """
        # The buffer caps submitted work so a slow consumer cannot create unbounded futures.
        if workers is not None and workers < 1:
            raise ValueError("workers must be at least 1")
        if backend not in ("thread", "process"):
            raise ValueError("backend must be 'thread' or 'process'")
        if buffer is None:
            buffer = 2 * (workers or (os.cpu_count() or 1))
        if buffer < 1:
            raise ValueError("buffer must be at least 1")
        return self._append(ParallelMapOp(function, workers, backend, ordered, buffer))

    parallel_map = map_parallel

    def parallel(
        self,
        *,
        workers: int | None = None,
        backend: ParallelBackend = "process",
        ordered: bool = True,
        buffer: int | None = None,
    ) -> Flow[T]:
        """Apply parallel settings to map operations added after this call.

        Args:
            workers: The number of worker threads or processes. None chooses a runtime default.
            backend: The parallel backend: thread or process.
            ordered: Whether results must preserve source encounter order.
            buffer: The maximum number of submitted tasks awaiting consumption.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        # Parallel settings are immutable plan metadata; existing operations are unchanged.
        if workers is not None and workers < 1:
            raise ValueError("workers must be at least 1")
        if backend not in ("thread", "process"):
            raise ValueError("backend must be 'thread' or 'process'")
        if buffer is None:
            buffer = 2 * (workers or (os.cpu_count() or 1))
        if buffer < 1:
            raise ValueError("buffer must be at least 1")
        return Flow(self._plan.with_parallel(ParallelSettings(workers, backend, ordered, buffer)))

    def sequential(self) -> Flow[T]:
        """Return a flow whose following maps run sequentially.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        return Flow(self._plan.with_parallel(None))

    def tap(self, function: Callable[[T], None]) -> Flow[T]:
        """Run a side effect for each item while passing the item through.

        Args:
            function: The callable applied by this operation.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        return self._append(TapOp(function))

    peek = tap

    def filter(self, predicate: Callable[[T], Any]) -> Flow[T]:
        """Keep items for which predicate returns a truthy value.

        `filter` is lazy and preserves encounter order; the predicate runs as items are
        requested.

        Args:
            predicate: A callable that decides whether an item matches.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        # Filtering stays lazy by recording a predicate node in the plan.
        return self._append(FilterOp(predicate))

    where = filter

    def reject(self, predicate: Callable[[T], Any]) -> Flow[T]:
        """Drop items for which predicate returns a truthy value.

        Args:
            predicate: A callable that decides whether an item matches.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        return self._append(FilterOp(predicate, negate=True))

    def compact(self, selector: Selector | None = None) -> Flow[T]:
        """Drop None values, optionally selected from each item.

        Args:
            selector: A callable, field name, index, path, or expression used to select a value.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        if selector is None:
            return self.filter(lambda item: item is not None)
        select = compile_selector(selector)
        return self.filter(lambda item: select(item) is not None)

    filter_none = compact

    def flat_map(self, function: Callable[[T], Iterable[R]]) -> Flow[R]:
        """Map each item to an iterable and emit the iterable contents.

        Each input may emit zero or more output items. Nested iterables are consumed lazily in
        encounter order.

        Args:
            function: The callable applied by this operation.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        # Flattening happens during iteration, so inner iterables are not collected first.
        return self._append(FlatMapOp(function))

    def filter_map(self, function: Callable[[T], R | None]) -> Flow[R]:
        """Map items and discard results equal to None.

        Args:
            function: The callable applied by this operation.

        Returns:
            A new lazy `Flow` representing this operation.
        """

        def transform(item: T) -> tuple[R, ...]:
            result = function(item)
            return () if result is None else (result,)

        return self.flat_map(transform)

    def pluck(self, selector: Selector) -> Flow[Any]:
        """Select one field, index, attribute, or nested path from each item.

        Args:
            selector: A callable, field name, index, path, or expression used to select a value.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        return self.map(compile_selector(selector))

    pick = pluck

    def unique(self) -> Flow[T]:
        """Keep the first occurrence of each value in encounter order.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        return self._append(UniqueOp())

    def distinct(self) -> Flow[T]:
        """Keep the first occurrence of each value in encounter order.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        return self.unique()

    def unique_by(self, selector: Selector) -> Flow[T]:
        """Keep the first item for each selected key.

        Args:
            selector: A callable, field name, index, path, or expression used to select a value.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        return self._append(UniqueOp(compile_selector(selector)))

    distinct_by = unique_by

    def sort_by(
        self,
        selector: Selector,
        *,
        reverse: bool = False,
        buffer_size: int | None = None,
        tempdir: str | os.PathLike[str] | None = None,
    ) -> Flow[T]:
        """Sort items by a selector, optionally using bounded external storage.

        Args:
            selector: A callable, field name, index, path, or expression used to select a value.
            reverse: If true, produce values in descending order.
            buffer_size: The maximum number of items held in an in-memory buffer or batch.
            tempdir: The directory used for temporary spill files.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        return self.sorted(
            key=compile_selector(selector),
            reverse=reverse,
            buffer_size=buffer_size,
            tempdir=tempdir,
        )

    def sorted(
        self,
        *,
        key: Callable[[T], Any] | None = None,
        reverse: bool = False,
        buffer_size: int | None = None,
        tempdir: str | os.PathLike[str] | None = None,
    ) -> Flow[T]:
        """Sort the flow, optionally using bounded external runs.

        Args:
            key: The callable or selector used to derive a key.
            reverse: If true, produce values in descending order.
            buffer_size: The maximum number of items held in an in-memory buffer or batch.
            tempdir: The directory used for temporary spill files.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        if buffer_size is not None:
            buffer_size = operator.index(buffer_size)
            if buffer_size <= 0:
                raise ValueError("sort buffer_size must be positive")
        return self._append(
            SortOp(
                key,
                reverse,
                buffer_size,
                tempdir,
                "external_sort" if buffer_size is not None else "sort",
            )
        )

    def external_sort(
        self,
        *,
        key: Callable[[T], Any] | None = None,
        reverse: bool = False,
        buffer_size: int = 100_000,
        tempdir: str | os.PathLike[str] | None = None,
    ) -> Flow[T]:
        """Sort with a bounded in-memory buffer and temporary files.

        Sorted runs are written to temporary files and merged lazily, keeping peak in-memory
        items bounded.

        Args:
            key: The callable or selector used to derive a key.
            reverse: If true, produce values in descending order.
            buffer_size: The maximum number of items held in an in-memory buffer or batch.
            tempdir: The directory used for temporary spill files.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        return self.sorted(
            key=key,
            reverse=reverse,
            buffer_size=buffer_size,
            tempdir=tempdir,
        )

    def external_sort_by(
        self,
        selector: Selector,
        *,
        reverse: bool = False,
        buffer_size: int = 100_000,
        tempdir: str | os.PathLike[str] | None = None,
    ) -> Flow[T]:
        """Sort by a selector using bounded memory and temporary files.

        Sorted runs are written to temporary files and merged lazily, keeping peak in-memory
        items bounded.

        Args:
            selector: A callable, field name, index, path, or expression used to select a value.
            reverse: If true, produce values in descending order.
            buffer_size: The maximum number of items held in an in-memory buffer or batch.
            tempdir: The directory used for temporary spill files.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        return self.sort_by(
            selector,
            reverse=reverse,
            buffer_size=buffer_size,
            tempdir=tempdir,
        )

    def chunk(self, size: int) -> Flow[tuple[T, ...]]:
        """Group consecutive items into fixed-size tuples.

        Args:
            size: The requested window, chunk, or batch size.

        Returns:
            A new lazy `Flow` representing this operation.

        Raises:
            ValueError: If size is less than one.
        """
        if size <= 0:
            raise ValueError("chunk size must be positive")
        return self._append(ChunkOp(size))

    batch = chunk

    def batch_by_size(
        self,
        max_size: int,
        *,
        max_count: int | None = None,
        get_size: Callable[[T], int] = _default_item_size,
        strict: bool = True,
    ) -> Flow[tuple[T, ...]]:
        """Build batches constrained by item count and total measured size.

        Args:
            max_size: The maximum measured size allowed in one batch.
            max_count: The maximum number of items emitted in one batch.
            get_size: A callable that returns the measured size of one item.
            strict: Whether invalid or empty input should raise instead of returning a fallback.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        if max_size <= 0:
            raise ValueError("max_size must be positive")
        if max_count is not None and max_count <= 0:
            raise ValueError("max_count must be positive")
        if not callable(get_size):
            raise TypeError("get_size must be callable")

        def integrate(state: _BatchState[T], item: T) -> tuple[tuple[T, ...], ...]:
            raw_size = get_size(item)
            try:
                item_size = operator.index(raw_size)
            except TypeError:
                raise TypeError("get_size must return an integer") from None
            if item_size < 0:
                raise ValueError("item sizes must be non-negative")
            if strict and item_size > max_size:
                raise ValueError(f"item size {item_size} exceeds max_size {max_size}")

            output: tuple[T, ...] | None = None
            size_exceeded = state.size + item_size > max_size
            count_reached = max_count is not None and len(state.items) >= max_count
            if state.items and (size_exceeded or count_reached):
                output = tuple(state.items)
                state.items.clear()
                state.size = 0

            state.items.append(item)
            state.size += item_size
            return () if output is None else (output,)

        def finish(state: _BatchState[T]) -> tuple[tuple[T, ...], ...]:
            return (tuple(state.items),) if state.items else ()

        return self.gather(Gatherer(lambda: _BatchState([]), integrate, finish))

    constrained_batches = batch_by_size

    def window(self, size: int, *, step: int = 1) -> Flow[tuple[T, ...]]:
        """Emit sliding tuples of size with the requested step.

        Args:
            size: The requested window, chunk, or batch size.
            step: The distance between consecutive windows or numeric increments.

        Returns:
            A new lazy `Flow` representing this operation.

        Raises:
            ValueError: If size or step is less than one.
        """
        if size <= 0:
            raise ValueError("window size must be positive")
        if step <= 0:
            raise ValueError("window step must be positive")
        return self._append(WindowOp(size, step))

    def group_runs(self, key: Selector | None = None) -> Flow[tuple[T, ...]]:
        """Group consecutive items that share the same key.

        Args:
            key: The callable or selector used to derive a key.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        select = (lambda item: item) if key is None else compile_selector(key)
        return self._append(GroupRunsOp(select))

    chunk_by = group_runs

    def pairwise(self) -> Flow[tuple[T, T]]:
        """Emit each adjacent pair of items.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        return self._append(PairwiseOp())

    def pair_map(self, function: Callable[[T, T], R]) -> Flow[R]:
        """Apply a two-argument function to each adjacent pair.

        Args:
            function: The callable applied by this operation.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        return self.pairwise().map(lambda pair: function(pair[0], pair[1]))

    def enumerate(self, start: int = 0) -> Flow[tuple[int, T]]:
        """Pair each item with a consecutive index starting at start.

        Args:
            start: The first index, numeric value, or additive identity to use.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        return self._append(EnumerateOp(start))

    zip_with_index = enumerate

    def zip(self, other: Iterable[U], *, strict: bool = False) -> Flow[tuple[T, U]]:
        """Pair items with another iterable until one side ends.

        Args:
            other: The other iterable, flow, value, or fallback used by the operation.
            strict: Whether invalid or empty input should raise instead of returning a fallback.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        return self._append(ZipOp(Source.from_iterable(other), strict))

    def zip_longest(
        self, other: Iterable[U], *, fillvalue: Any = None
    ) -> Flow[tuple[T | Any, U | Any]]:
        """Pair with another iterable until both sides end, filling missing values.

        Args:
            other: The other iterable, flow, value, or fallback used by the operation.
            fillvalue: The value used when one side of a longest zip is exhausted.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        return self._append(ZipLongestOp(Source.from_iterable(other), fillvalue))

    def intersperse(self, separator: T) -> Flow[T]:
        """Insert separator between consecutive items.

        Args:
            separator: The string inserted between adjacent string representations.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        return self._append(IntersperseOp(separator))

    def concat(self, *others: Iterable[T]) -> Flow[T]:
        """Emit this flow followed by each supplied iterable.

        Args:
            *others: Additional sources combined with this pipeline.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        return self._append(ConcatOp(tuple(Source.from_iterable(other) for other in others)))

    def cross(self, other: Iterable[U], *, max_right: int | None = None) -> Flow[tuple[T, U]]:
        """Emit the Cartesian product with a bounded or reiterable right side.

        Args:
            other: The other iterable, flow, value, or fallback used by the operation.
            max_right: The maximum right-side size allowed when buffering is required.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        if max_right is not None:
            max_right = operator.index(max_right)
            if max_right < 0:
                raise ValueError("max_right must be non-negative")
        return self._append(CrossOp(Source.from_iterable(other), max_right))

    cartesian = cross

    def scan(self, initial: R, function: Callable[[R, T], R]) -> Flow[R]:
        """Emit each left-to-right accumulator state.

        Unlike `reduce`, `scan` emits every intermediate accumulator state.

        Args:
            initial: The initial accumulator value. When omitted, the first item is used where
                supported.
            function: The callable applied by this operation.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        return self._append(ScanOp(initial, function))

    def scan_right(
        self,
        initial: R,
        function: Callable[[T, R], R],
        *,
        max_items: int | None = None,
    ) -> Flow[R]:
        """Emit accumulator states while combining items from right to left.

        Args:
            initial: The initial accumulator value. When omitted, the first item is used where
                supported.
            function: The callable applied by this operation.
            max_items: The maximum number of source items allowed in the right-side buffer.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        if not callable(function):
            raise TypeError("function must be callable")
        if max_items is not None:
            max_items = operator.index(max_items)
            if max_items < 0:
                raise ValueError("max_items must be non-negative")
        return self._append(ScanRightOp(initial, function, max_items))

    def gather(self, gatherer: Gatherer[T, Any, R]) -> Flow[R]:
        """Apply a stateful Gatherer that may emit zero or more values per item.

        A gatherer may retain state and emit zero, one, or many outputs for each input item.

        Args:
            gatherer: The stateful gatherer applied to this pipeline.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        return self._append(GatherOp(gatherer))

    def fold(
        self,
        initializer: Callable[[], R],
        function: Callable[[R, T], R],
    ) -> Flow[R]:
        """Consume all items into one emitted value using fresh state per iteration.

        Args:
            initializer: A zero-argument callable that creates fresh mutable state.
            function: The callable applied by this operation.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        if not callable(initializer):
            raise TypeError("initializer must be callable")
        if not callable(function):
            raise TypeError("function must be callable")

        def integrate(state: _FoldState[R], item: T) -> tuple[()]:
            state.value = function(state.value, item)
            return ()

        def finish(state: _FoldState[R]) -> tuple[R]:
            return (state.value,)

        return self.gather(Gatherer(lambda: _FoldState(initializer()), integrate, finish))

    def prepend(self, *values: T) -> Flow[T]:
        """Emit values before the items from this flow.

        Args:
            *values: Values supplied to this operation in encounter order.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        return self._append(PrependOp(values))

    def append(self, *values: T) -> Flow[T]:
        """Emit values after the items from this flow.

        Args:
            *values: Values supplied to this operation in encounter order.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        return self._append(AppendOp(values))

    def map_first(self, function: Callable[[T], T]) -> Flow[T]:
        """Transform only the first item, if one exists.

        Args:
            function: The callable applied by this operation.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        return self._append(MapFirstOp(function))

    def map_last(self, function: Callable[[T], T]) -> Flow[T]:
        """Transform only the last item, if one exists.

        Args:
            function: The callable applied by this operation.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        return self._append(MapLastOp(function))

    def collapse(
        self,
        collapsible: Callable[[T, T], bool],
        merger: Callable[[T, T], T],
    ) -> Flow[T]:
        """Merge adjacent items while collapsible returns true.

        Args:
            collapsible: A callable deciding whether two adjacent items should be combined.
            merger: A callable that merges two downstream results.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        return self._append(CollapseOp(collapsible, merger))

    def attempt(self, function: Callable[[T], R]) -> Flow[Result[R]]:
        """Map each item and wrap success or failure in a Result.

        Args:
            function: The callable applied by this operation.

        Returns:
            A new lazy `Flow` representing this operation.
        """

        def capture(item: T) -> Result[R]:
            try:
                return Ok(function(item))
            except Exception as error:
                return Err(error)

        return self.map(capture)

    def with_engine(self, engine: Engine) -> Flow[T]:
        """Request automatic, Python, or native execution for this plan.

        Selecting an engine changes execution policy without consuming the source.

        Args:
            engine: The execution engine requested for this pipeline.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        if engine not in ("auto", "python", "native"):
            raise ValueError("engine must be 'auto', 'python', or 'native'")
        return Flow(self._plan.with_engine(engine))

    def explain(self) -> PlanExplanation:
        """Describe engine selection, stages, and fused operations without executing.

        This inspection method does not consume or execute the source.

        Returns:
            A structured explanation of the selected engine and planned stages.
        """
        return PlanExplanation(self._plan)

    def pairs(self) -> Any:
        """View a flow of two-tuples as a key/value Pairs pipeline.

        Returns:
            A lazy `Pairs` view over this flow.
        """
        from .pairs import Pairs

        return Pairs(cast(Flow[tuple[Any, Any]], self))

    def take(self, count: int) -> Flow[T]:
        """Emit at most count items, then close the upstream iterator.

        Args:
            count: The requested number of items.

        Returns:
            A new lazy `Flow` representing this operation.

        Raises:
            ValueError: If count is negative.
        """
        if count < 0:
            raise ValueError("take count must be non-negative")
        return self._append(TakeOp(count))

    def limit(self, count: int) -> Flow[T]:
        """Emit at most count items; alias of take.

        Args:
            count: The requested number of items.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        return self.take(count)

    def drop(self, count: int) -> Flow[T]:
        """Skip count items before yielding the remainder.

        Args:
            count: The requested number of items.

        Returns:
            A new lazy `Flow` representing this operation.

        Raises:
            ValueError: If count is negative.
        """
        if count < 0:
            raise ValueError("drop count must be non-negative")
        return self._append(DropOp(count))

    def skip(self, count: int) -> Flow[T]:
        """Skip count items; alias of drop.

        Args:
            count: The requested number of items.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        return self.drop(count)

    def take_while(self, predicate: Callable[[T], bool]) -> Flow[T]:
        """Emit the longest prefix that satisfies predicate.

        Args:
            predicate: A callable that decides whether an item matches.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        return self._append(TakeWhileOp(predicate))

    def take_while_inclusive(self, predicate: Callable[[T], bool]) -> Flow[T]:
        """Emit through the first item that fails predicate.

        Args:
            predicate: A callable that decides whether an item matches.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        return self._append(TakeWhileInclusiveOp(predicate))

    def drop_while(self, predicate: Callable[[T], bool]) -> Flow[T]:
        """Skip the longest prefix that satisfies predicate.

        Args:
            predicate: A callable that decides whether an item matches.

        Returns:
            A new lazy `Flow` representing this operation.
        """
        return self._append(DropWhileOp(predicate))

    @contextmanager
    def _open(self) -> Iterator[Iterator[T]]:
        iterator = iter(self)
        try:
            yield iterator
        finally:
            close = getattr(iterator, "close", None)
            if close is not None:
                close()


class _FlowFactory:
    """Callable factory for creating synchronous flows and deferred sources."""

    __slots__ = ()

    def __call__(self, source: Iterable[T]) -> Flow[T]:
        """Create a lazy flow over an iterable source.

        Args:
            source: The iterable consumed when the flow is evaluated.

        Returns:
            A new lazy `Flow`.
        """
        return Flow(source)

    def defer(self, factory: Callable[[], Iterable[T]]) -> Flow[T]:
        """Create a reusable flow that calls factory for each iteration.

        Args:
            factory: A callable that opens a fresh source for every iteration.

        Returns:
            A new reusable lazy `Flow`.
        """
        return Flow(Source.defer(factory))

    def empty(self) -> Flow[Any]:
        """Create a flow that emits no items.

        Returns:
            A new reusable lazy `Flow`.
        """
        return Flow.empty()

    def of_nullable(self, value: T | None) -> Flow[T]:
        """Create an empty flow for None, otherwise emit the value once.

        Args:
            value: The value consumed by this operation.

        Returns:
            A new reusable lazy `Flow`.
        """
        return Flow.of_nullable(value)

    def generate(self, supplier: Callable[[], T]) -> Flow[T]:
        """Create an infinite flow by calling supplier for each item.

        Args:
            supplier: A zero-argument callable that supplies a value or iterable.

        Returns:
            A new reusable lazy `Flow`.
        """
        return Flow.generate(supplier)

    def concat(self, *sources: Iterable[T]) -> Flow[T]:
        """Create a flow that emits each source in order.

        Args:
            *sources: Sources emitted sequentially by the new pipeline.

        Returns:
            A new reusable lazy `Flow`.
        """
        if not sources:
            return Flow(())
        first, *rest = sources
        return Flow(first).concat(*rest)

    def iterate(self, seed: T, function: Callable[[T], T]) -> Flow[T]:
        """Emit seed, then repeatedly apply function to the previous value.

        Args:
            seed: The first value emitted or used to initialize the sequence.
            function: The callable applied by this operation.

        Returns:
            A new reusable lazy `Flow`.
        """
        return Flow.iterate(seed, function)


flow = _FlowFactory()
