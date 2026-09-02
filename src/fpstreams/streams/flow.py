"""Lazy synchronous pipelines and terminal operations."""

from __future__ import annotations

import operator
import os
import sys
from collections.abc import Callable, Generator, Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from importlib import import_module
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Literal,
    Protocol,
    TypeVar,
    cast,
    overload,
)

import fpstreams

if TYPE_CHECKING:
    import polars as pl

from ..expressions.selectors import Selector, compile_selector
from ..planning.explain import PlanExplanation, explain_query
from ..planning.gather import Gatherer
from ..planning.logical import (
    LogicalPlan,
    Pipeline,
    Query,
    SourceNode,
    TerminalSpec,
    linear_pipeline,
)
from ..planning.native import TerminalName
from ..planning.semantics import (
    Cardinality,
    OrderingGuarantee,
    Replayability,
    StreamFacts,
    TerminationEvidence,
)
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
from ..runtime.iterators import closing_iterators
from .flow_terminals import FlowTerminalsMixin

T = TypeVar("T")
R = TypeVar("R")
C = TypeVar("C")
U = TypeVar("U")
_MISSING = object()
_TABULAR_MODULE_ROOTS = frozenset(("pandas", "polars", "pyarrow"))


class _ArrowCStreamProvider(Protocol):
    """Static shape of the Arrow PyCapsule stream protocol."""

    def __arrow_c_stream__(self, requested_schema: Any = None) -> Any: ...


class _DataFrameProvider(Protocol):
    """Static shape of the dataframe interchange protocol."""

    def __dataframe__(
        self,
        nan_as_null: bool = False,
        allow_copy: bool = True,
    ) -> Any: ...


def _tabular_module_root(source_type: type[Any]) -> str | None:
    """Return a recognized vendor root, including for application-defined subclasses."""
    module_root = source_type.__module__.partition(".")[0]
    if module_root in _TABULAR_MODULE_ROOTS:
        return module_root
    return next(
        (
            root
            for base in source_type.__mro__[1:]
            if (root := base.__module__.partition(".")[0]) in _TABULAR_MODULE_ROOTS
        ),
        None,
    )


def _default_item_size(value: Any) -> int:
    """Measure an item with `len()` for size-constrained batching."""
    return len(value)


@dataclass(slots=True)
class _BatchState(Generic[T]):
    """Hold the current size-constrained batch and its accumulated size."""

    items: list[T]
    size: int = 0


@dataclass(slots=True)
class _FoldState(Generic[R]):
    """Hold the mutable accumulator used by `Flow.fold`."""

    value: R


class Flow(FlowTerminalsMixin[T], Generic[T]):
    """A synchronous pipeline that opens its source only when iterated or consumed."""

    __slots__ = ("_logical_plan",)

    def __init__(self, source: Iterable[T] | Source[T]) -> None:
        """Wrap an iterable or owned source, or reuse an internal logical pipeline."""
        owned = source if isinstance(source, Source) else Source.from_iterable(source)
        self._logical_plan = LogicalPlan(SourceNode(owned))

    @classmethod
    def _from_logical(cls, logical: LogicalPlan) -> Flow[Any]:
        """Create a flow from an already-validated internal logical tree."""
        instance = object.__new__(cls)
        instance._logical_plan = logical
        return instance

    @property
    def _pipeline(self) -> Pipeline:
        """Return the canonical unopened linear view for backend selection."""
        return linear_pipeline(self._logical_plan)

    def _uncompiled_exact_count(self) -> int | None:
        """Read trusted identity-source cardinality before backend compilation.

        Forced native requests still compile so extension availability and source support remain
        part of that explicit contract. Automatic and Python identity plans can return metadata
        recorded from exact built-in containers without opening or inspecting their contents.
        """
        logical = self._logical_plan
        if logical.engine == "native" or not isinstance(logical.root, SourceNode):
            return None
        capabilities = logical.root.source.capabilities
        if not capabilities.reiterable:
            return None
        return logical.root.source.current_exact_size()

    def _uncompiled_python_identity_pipeline(self, query: Query) -> Pipeline | None:
        """Expose an operation-free auto/Python source without selecting a backend."""
        logical = query.logical
        if logical.engine not in {"auto", "python"} or not isinstance(logical.root, SourceNode):
            return None
        return linear_pipeline(logical)

    def _query(self, name: str, *arguments: Any, **options: Any) -> Query:
        """Describe one terminal request without executing or opening the pipeline."""
        return Query(
            self._logical_plan,
            TerminalSpec(name, arguments, tuple(options.items())),
        )

    @staticmethod
    def of(*items: R) -> Flow[R]:
        """Create a flow from positional items.

        Args:
            *items: Positional values to emit in argument order.

        Returns:
            A reusable flow that emits `items` in argument order.
        """
        return Flow(items)

    @staticmethod
    def from_iterable(source: Iterable[R]) -> Flow[R]:
        """Create a flow over an iterable source.

        Args:
            source: A synchronous iterable opened when the flow is consumed. Iterator instances
                remain one-shot; other iterables can be evaluated repeatedly.

        Returns:
            A flow that emits each item from `source` in its iteration order.
        """
        return Flow(source)

    @staticmethod
    def empty() -> Flow[Any]:
        """Create a flow that emits no items.

        Returns:
            A reusable flow that always completes without emitting an item.
        """
        return Flow(())

    @staticmethod
    def of_nullable(value: R | None) -> Flow[R]:
        """Create an empty flow for None, otherwise emit the value once.

        Args:
            value: The optional item to emit; `None` produces an empty flow.

        Returns:
            A flow containing `value` once, or no items when `value` is `None`.
        """
        return Flow(()) if value is None else Flow((value,))

    @staticmethod
    def iterate(seed: R, function: Callable[[R], R]) -> Flow[R]:
        """Emit seed, then repeatedly apply function to the previous value.

        Args:
            seed: The first value emitted or used to initialize the sequence.
            function: Called with the previously emitted value to produce the next value.

        Returns:
            A reusable, infinite flow beginning with `seed`.
        """

        def values() -> Iterator[R]:
            """Yield the seed followed by successive applications of `function`."""
            current = seed
            while True:
                yield current
                current = function(current)

        return Flow(
            Source.defer(
                values,
                facts=StreamFacts(
                    TerminationEvidence.PROVEN_INFINITE,
                    Cardinality.unknown(),
                    Replayability.REOPENABLE,
                    OrderingGuarantee.ORDERED,
                ),
            )
        )

    @staticmethod
    def generate(supplier: Callable[[], R]) -> Flow[R]:
        """Create an infinite flow by calling supplier for each item.

        Args:
            supplier: Called once for each requested item to produce that item.

        Returns:
            A reusable, infinite flow of values returned by `supplier`.
        """

        def values() -> Iterator[R]:
            """Call `supplier` for every requested item and yield its return value."""
            while True:
                yield supplier()

        return Flow(
            Source.defer(
                values,
                facts=StreamFacts(
                    TerminationEvidence.PROVEN_INFINITE,
                    Cardinality.unknown(),
                    Replayability.REOPENABLE,
                    OrderingGuarantee.ORDERED,
                ),
            )
        )

    def __iter__(self) -> Iterator[T]:
        """Compile and execute every public iteration through the physical pipeline."""
        from ..execution.physical import execute_physical
        from ..planning.compiler import compile_iteration
        from ..runtime.report import _record_sync_plan

        physical = compile_iteration(self._query("iterate"))
        _record_sync_plan(physical)
        return execute_physical(physical)

    def _append(self, operation: Operation) -> Flow[Any]:
        """Return a new Flow whose immutable plan includes `operation`."""
        return self._from_logical(self._logical_plan.append(operation))

    def map(self, function: Callable[[T], R]) -> Flow[R]:
        """Apply function to each item lazily.

        `map` is lazy: the callable runs only when a terminal operation or iteration consumes
        the flow.

        Args:
            function: Receives each source item and returns its replacement value.

        Returns:
            A flow of mapped values; current plan-level parallel settings, if any, apply to this
            map.
        """
        # Store the transform in the immutable plan; no item is mapped yet.
        if self._logical_plan.parallel is not None:
            settings = self._logical_plan.parallel
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
            function: Receives one source item in a worker and returns its mapped value.
            workers: Worker count, or `None` to use the executor's default.
            backend: Run callbacks in a `thread` or spawn-based `process` pool.
            ordered: Emit in source order when true, otherwise in completion order.
            buffer: Maximum submitted futures retained before the pipeline waits for a result.

        Returns:
            A flow that submits bounded mapping work when consumed.

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
            workers: Worker count, or `None` to use the executor's default.
            backend: Run callbacks in a `thread` or spawn-based `process` pool.
            ordered: Preserve source order for subsequent maps when true.
            buffer: Maximum in-flight results retained by each subsequent map.

        Returns:
            A flow sharing this pipeline with parallel defaults for maps appended afterward.
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
        return self._from_logical(
            self._logical_plan.with_parallel(ParallelSettings(workers, backend, ordered, buffer))
        )

    def sequential(self) -> Flow[T]:
        """Return a flow whose following maps run sequentially.

        Returns:
            A flow sharing this pipeline with parallel defaults cleared for later maps.
        """
        return self._from_logical(self._logical_plan.with_parallel(None))

    def tap(self, function: Callable[[T], None]) -> Flow[T]:
        """Run a side effect for each item while passing the item through.

        Args:
            function: Called for its side effect before each original item is emitted.

        Returns:
            A flow that emits every original item unchanged after calling `function`.
        """
        return self._append(TapOp(function))

    peek = tap

    def filter(self, predicate: Callable[[T], Any]) -> Flow[T]:
        """Keep items for which predicate returns a truthy value.

        `filter` is lazy and preserves encounter order; the predicate runs as items are
        requested.

        Args:
            predicate: Called for each item; truthy results retain that item.

        Returns:
            A flow containing only source items whose predicate result is truthy.
        """
        # Filtering stays lazy by recording a predicate node in the plan.
        return self._append(FilterOp(predicate))

    where = filter

    def reject(self, predicate: Callable[[T], Any]) -> Flow[T]:
        """Drop items for which predicate returns a truthy value.

        Args:
            predicate: Called for each item; truthy results drop that item.

        Returns:
            A flow containing only source items whose predicate result is falsey.
        """
        return self._append(FilterOp(predicate, negate=True))

    def compact(self, selector: Selector | None = None) -> Flow[T]:
        """Drop None values, optionally selected from each item.

        Args:
            selector: Optional callable, field name, index, path, or expression whose selected
                value is checked for `None`; without one, each item is checked directly.

        Returns:
            A flow that omits items whose selected value is `None` while retaining other falsey
            values.
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
            function: Maps each source item to the iterable whose contents are emitted.

        Returns:
            A flow that lazily emits every mapped iterable in source order.
        """
        # Flattening happens during iteration, so inner iterables are not collected first.
        return self._append(FlatMapOp(function))

    def filter_map(self, function: Callable[[T], R | None]) -> Flow[R]:
        """Map items and discard results equal to None.

        Args:
            function: Maps each source item to one output value or `None`.

        Returns:
            A flow of non-`None` mapped results; falsey values such as `0` are retained.
        """

        def transform(item: T) -> tuple[R, ...]:
            """Return no values for a None result, otherwise return one mapped value."""
            result = function(item)
            return () if result is None else (result,)

        return self.flat_map(transform)

    def pluck(self, selector: Selector) -> Flow[Any]:
        """Select one field, index, attribute, or nested path from each item.

        Args:
            selector: Callable, field name, index, path, or expression evaluated for each output.

        Returns:
            A flow containing the value selected from each source item.
        """
        return self.map(compile_selector(selector))

    pick = pluck

    def unique(self) -> Flow[T]:
        """Keep the first occurrence of each value in encounter order.

        Returns:
            A flow containing the first occurrence of each distinct value.
        """
        return self._append(UniqueOp())

    def distinct(self) -> Flow[T]:
        """Keep the first occurrence of each value in encounter order.

        Returns:
            The same lazy de-duplication pipeline produced by `unique()`.
        """
        return self.unique()

    def unique_by(self, selector: Selector) -> Flow[T]:
        """Keep the first item for each selected key.

        Args:
            selector: Callable, field name, index, path, or expression producing each uniqueness
                key.

        Returns:
            A flow containing the first source item for each distinct selected key.
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
            selector: Callable, field name, index, path, or expression producing comparison keys.
            reverse: Emit items in descending selected-key order when true.
            buffer_size: Items per sorted in-memory run; `None` performs one in-memory sort.
            tempdir: Directory for temporary run files when `buffer_size` is set.

        Returns:
            A flow that emits every source item ordered by its selected value.
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
            key: Optional callable used to derive each item's comparison key.
            reverse: Emit items in descending comparison order when true.
            buffer_size: Items per sorted in-memory run; `None` performs one in-memory sort.
            tempdir: Directory for temporary run files when `buffer_size` is set.

        Returns:
            A flow that globally orders the source by `key` or by the items themselves.
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
            key: Optional callable used to derive each item's comparison key.
            reverse: Emit items in descending comparison order when true.
            buffer_size: Maximum items sorted in memory for each temporary run.
            tempdir: Directory in which temporary sorted runs are created.

        Returns:
            A globally sorted flow produced by lazily merging bounded temporary runs.
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
            selector: Callable, field name, index, path, or expression producing comparison keys.
            reverse: Emit items in descending selected-key order when true.
            buffer_size: Maximum items sorted in memory for each temporary run.
            tempdir: Directory in which temporary sorted runs are created.

        Returns:
            A flow ordered by the selected value using bounded temporary runs.
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
            size: Maximum number of consecutive items in each tuple.

        Returns:
            A flow of non-overlapping tuples, including a final shorter tuple when needed.

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
            max_size: Maximum sum of item sizes in a normal batch.
            max_count: Optional maximum item count per batch.
            get_size: Returns a non-negative integer size for each item; defaults to `len`.
            strict: Raise when one item exceeds `max_size`; when false, emit it in an oversized
                singleton batch.

        Returns:
            A flow of non-empty tuples packed without exceeding either configured limit, except
            for non-strict oversized singleton items.
        """
        if max_size <= 0:
            raise ValueError("max_size must be positive")
        if max_count is not None and max_count <= 0:
            raise ValueError("max_count must be positive")
        if not callable(get_size):
            raise TypeError("get_size must be callable")

        def integrate(state: _BatchState[T], item: T) -> tuple[tuple[T, ...], ...]:
            """Add an item to the current batch, emitting the previous batch at a limit."""
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
            """Emit the final non-empty size-constrained batch."""
            return (tuple(state.items),) if state.items else ()

        return self.gather(Gatherer(lambda: _BatchState([]), integrate, finish))

    constrained_batches = batch_by_size

    def window(self, size: int, *, step: int = 1) -> Flow[tuple[T, ...]]:
        """Emit sliding tuples of size with the requested step.

        Args:
            size: Number of items in each full window.
            step: Number of source items consumed between successive windows.

        Returns:
            A flow of full sliding windows; a non-empty source shorter than `size` produces one
            partial window, but no trailing partial window is emitted otherwise.

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
            key: Optional selector for run identity; adjacent items themselves are compared when
                omitted.

        Returns:
            A flow of non-empty tuples, one for each contiguous run of equal keys.
        """
        select = (lambda item: item) if key is None else compile_selector(key)
        return self._append(GroupRunsOp(select))

    chunk_by = group_runs

    def pairwise(self) -> Flow[tuple[T, T]]:
        """Emit each adjacent pair of items.

        Returns:
            A flow of overlapping `(previous, current)` pairs; fewer than two items emit nothing.
        """
        return self._append(PairwiseOp())

    def pair_map(self, function: Callable[[T, T], R]) -> Flow[R]:
        """Apply a two-argument function to each adjacent pair.

        Args:
            function: Called as `function(previous, current)` for each adjacent pair.

        Returns:
            A flow containing one mapped result per adjacent source pair.
        """
        return self.pairwise().map(lambda pair: function(pair[0], pair[1]))

    def enumerate(self, start: int = 0) -> Flow[tuple[int, T]]:
        """Pair each item with a consecutive index starting at start.

        Args:
            start: Integer index paired with the first source item.

        Returns:
            A flow of `(index, item)` pairs with consecutive integer indices.
        """
        return self._append(EnumerateOp(start))

    zip_with_index = enumerate

    def zip(self, other: Iterable[U], *, strict: bool = False) -> Flow[tuple[T, U]]:
        """Pair items with another iterable until one side ends.

        Args:
            other: Synchronous iterable providing the right-hand item in each pair.
            strict: Raise `ValueError` during consumption when the two inputs have different
                lengths.

        Returns:
            A flow of pairs ending with the shorter input unless `strict` is true.
        """
        return self._append(ZipOp(Source.from_iterable(other), strict))

    def zip_longest(
        self, other: Iterable[U], *, fillvalue: Any = None
    ) -> Flow[tuple[T | Any, U | Any]]:
        """Pair with another iterable until both sides end, filling missing values.

        Args:
            other: Synchronous iterable providing right-hand values.
            fillvalue: Substitute used on whichever input is exhausted first.

        Returns:
            A flow of pairs whose length matches the longer input.
        """
        return self._append(ZipLongestOp(Source.from_iterable(other), fillvalue))

    def intersperse(self, separator: T) -> Flow[T]:
        """Insert separator between consecutive items.

        Args:
            separator: Item emitted once between each pair of adjacent source items.

        Returns:
            A flow with `separator` between source items and never at either boundary.
        """
        return self._append(IntersperseOp(separator))

    def concat(self, *others: Iterable[T]) -> Flow[T]:
        """Emit this flow followed by each supplied iterable.

        Args:
            *others: Synchronous iterables opened and emitted after this flow, in argument order.

        Returns:
            A flow that drains this source and then each additional iterable in order.
        """
        return self._append(ConcatOp(tuple(Source.from_iterable(other) for other in others)))

    def cross(self, other: Iterable[U], *, max_right: int | None = None) -> Flow[tuple[T, U]]:
        """Buffer another iterable once and emit a left-major Cartesian product.

        Args:
            other: Synchronous iterable buffered as the right side after the first left item.
            max_right: Optional maximum number of right-side items that may be buffered.

        Returns:
            A flow of `(left, right)` pairs with every right item repeated for each left item.

        Raises:
            BufferLimitError: During consumption if `other` contains more than `max_right` items.
        """
        if max_right is not None:
            max_right = operator.index(max_right)
            if max_right < 0:
                raise ValueError("max_right must be non-negative")
        return self._append(CrossOp(Source.from_iterable(other), max_right))

    cartesian = cross

    def scan(self, initial: R, function: Callable[[R, T], R]) -> Flow[R]:
        """Emit each left-to-right accumulator state after consuming one item.

        Unlike `reduce`, `scan` emits every intermediate accumulator state.

        Args:
            initial: Accumulator passed to the first callback; it is not emitted by itself.
            function: Called as `function(state, item)` to produce each next state.

        Returns:
            A flow with one accumulated state for every source item.
        """
        return self._append(ScanOp(initial, function))

    def scan_right(
        self,
        initial: R,
        function: Callable[[T, R], R],
        *,
        max_items: int | None = None,
    ) -> Flow[R]:
        """Buffer the source, accumulate from right to left, and emit states in source order.

        Args:
            initial: Accumulator passed to the rightmost callback; it is not emitted by itself.
            function: Called as `function(item, state)` from the rightmost item to the leftmost.
            max_items: Optional maximum number of source items that may be buffered.

        Returns:
            A flow with one right-fold state per source item, ordered like the original items.

        Raises:
            BufferLimitError: During consumption if the source exceeds `max_items`.
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
            A flow of values emitted by `gatherer` while it integrates and finishes the source.
        """
        return self._append(GatherOp(gatherer))

    def fold(
        self,
        initializer: Callable[[], R],
        function: Callable[[R, T], R],
    ) -> Flow[R]:
        """Consume all items into one emitted value using fresh state per iteration.

        Args:
            initializer: Called once per evaluation to create the initial accumulator.
            function: Called as `function(state, item)` and returns the replacement accumulator.

        Returns:
            A flow that emits exactly one final accumulator, including for an empty source.
        """
        if not callable(initializer):
            raise TypeError("initializer must be callable")
        if not callable(function):
            raise TypeError("function must be callable")

        def integrate(state: _FoldState[R], item: T) -> tuple[()]:
            """Replace the accumulator with `function(state.value, item)` without emitting."""
            state.value = function(state.value, item)
            return ()

        def finish(state: _FoldState[R]) -> tuple[R]:
            """Emit the final accumulator after all source items have been integrated."""
            return (state.value,)

        return self.gather(Gatherer(lambda: _FoldState(initializer()), integrate, finish))

    def prepend(self, *values: T) -> Flow[T]:
        """Emit values before the items from this flow.

        Args:
            *values: Items to emit before the first source item, in argument order.

        Returns:
            A flow containing `values` followed by every source item.
        """
        return self._append(PrependOp(values))

    def append(self, *values: T) -> Flow[T]:
        """Emit values after the items from this flow.

        Args:
            *values: Items to emit after the source completes, in argument order.

        Returns:
            A flow containing every source item followed by `values`.
        """
        return self._append(AppendOp(values))

    def map_first(self, function: Callable[[T], T]) -> Flow[T]:
        """Transform only the first item, if one exists.

        Args:
            function: Maps the first item; it is never called for an empty source.

        Returns:
            A flow with only its first item replaced by `function(first)`.
        """
        return self._append(MapFirstOp(function))

    def map_last(self, function: Callable[[T], T]) -> Flow[T]:
        """Transform only the last item, if one exists.

        Args:
            function: Maps the final item; it is never called for an empty source.

        Returns:
            A flow with only its final item replaced by `function(last)`.
        """
        return self._append(MapLastOp(function))

    def collapse(
        self,
        collapsible: Callable[[T, T], bool],
        merger: Callable[[T, T], T],
    ) -> Flow[T]:
        """Merge adjacent items while collapsible returns true.

        Args:
            collapsible: Called on neighboring original items to decide whether a run continues.
            merger: Combines the current run aggregate with the next item.

        Returns:
            A flow containing one merged aggregate for each contiguous collapsible run.
        """
        return self._append(CollapseOp(collapsible, merger))

    def attempt(self, function: Callable[[T], R]) -> Flow[Result[R]]:
        """Map each item and wrap success or failure in a Result.

        Args:
            function: Maps one source item and may raise an `Exception`.

        Returns:
            A flow of `Ok` mapped values and `Err` objects for raised exceptions.
        """

        def capture(item: T) -> Result[R]:
            """Wrap the mapped value in Ok or convert a raised exception to Err."""
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
            An equivalent lazy flow whose plan requests `engine` during execution.
        """
        if engine not in ("auto", "python", "native"):
            raise ValueError("engine must be 'auto', 'python', or 'native'")
        return self._from_logical(self._logical_plan.with_engine(engine))

    def explain(self, terminal: TerminalName = "iterate") -> PlanExplanation:
        """Describe engine selection, stages, and fused operations without executing.

        This inspection method does not consume or execute the source.

        Args:
            terminal: Terminal operation to include when validating and selecting the engine.

        Returns:
            A structured explanation of the selected engine and planned stages.
        """
        return explain_query(self._query(terminal))

    def pairs(self) -> Any:
        """View a flow of two-tuples as a key/value Pairs pipeline.

        Returns:
            A lazy `Pairs` view over this flow.
        """
        # Pairs depends on Flow for its public factory and annotations. Resolve the
        # adapter only at this API boundary so neither module can observe the other
        # while it is still being initialized.
        pairs_type = cast(Any, import_module(".pairs", __package__)).Pairs
        return pairs_type(cast(Flow[tuple[Any, Any]], self))

    def rows(self) -> fpstreams.Rows[T]:
        """View this flow as a lazy record pipeline without inspecting its items.

        Returns:
            A Rows view that shares this flow's plan and source ownership.
        """
        from ..tabular.rows import Rows

        return Rows(self)

    def select(self, *selectors: str | int, **named: Selector) -> fpstreams.Rows[dict[str, Any]]:
        """Project record fields through a lazy Rows view of this flow."""
        return self.rows().select(*selectors, **named)

    def with_columns(self, **columns: Selector) -> fpstreams.Rows[dict[str, Any]]:
        """Add or replace record fields through a lazy Rows view of this flow."""
        return self.rows().with_columns(**columns)

    def rename(self, **columns: str) -> fpstreams.Rows[dict[str, Any]]:
        """Rename record fields through a lazy Rows view of this flow."""
        return self.rows().rename(**columns)

    def cast(self, **columns: Callable[[Any], Any]) -> fpstreams.Rows[dict[str, Any]]:
        """Convert named record fields through a lazy Rows view of this flow."""
        return self.rows().cast(**columns)

    def fill_nulls(self, **replacements: object) -> fpstreams.Rows[dict[str, Any]]:
        """Replace missing or None record fields through a lazy Rows view."""
        return self.rows().fill_nulls(**replacements)

    def drop_nulls(
        self,
        *selectors: Selector,
        how: Literal["any", "all"] = "any",
    ) -> fpstreams.Rows[T]:
        """Drop records according to selected None values through a Rows view."""
        return self.rows().drop_nulls(*selectors, how=how)

    def explode(
        self,
        selector: Selector,
        *,
        into: str | None = None,
        outer: bool = False,
    ) -> fpstreams.Rows[dict[str, Any]]:
        """Expand one selected iterable through a lazy Rows view."""
        return self.rows().explode(selector, into=into, outer=outer)

    def unnest(self, column: str, *, prefix: str = "") -> fpstreams.Rows[dict[str, Any]]:
        """Promote fields from one nested record through a lazy Rows view."""
        return self.rows().unnest(column, prefix=prefix)

    def unpivot(
        self,
        *columns: str,
        names_to: str = "variable",
        values_to: str = "value",
    ) -> fpstreams.Rows[dict[str, Any]]:
        """Reshape wide records into name/value records through a Rows view."""
        return self.rows().unpivot(*columns, names_to=names_to, values_to=values_to)

    def pivot(
        self,
        *,
        index: Selector | tuple[Selector, ...],
        columns: Selector,
        values: Selector,
        aggregate: str | Callable[[Any, Any], Any] = "error",
        fill: Any = None,
    ) -> fpstreams.Rows[dict[str, Any]]:
        """Reshape long records into wide records through a lazy Rows view."""
        return self.rows().pivot(
            index=index,
            columns=columns,
            values=values,
            aggregate=aggregate,
            fill=fill,
        )

    def group_by(self, *selectors: Selector, **named: Selector) -> fpstreams.tabular.GroupedRows[T]:
        """Describe grouped aggregation through a lazy Rows view of this flow."""
        return self.rows().group_by(*selectors, **named)

    def take(self, count: int) -> Flow[T]:
        """Emit at most count items, then close the upstream iterator.

        Args:
            count: Maximum number of leading items to emit.

        Returns:
            A flow containing only the first `count` source items.

        Raises:
            ValueError: If count is negative.
        """
        if count < 0:
            raise ValueError("take count must be non-negative")
        return self._append(TakeOp(count))

    def limit(self, count: int) -> Flow[T]:
        """Emit at most count items; alias of take.

        Args:
            count: Maximum number of leading items to emit.

        Returns:
            The same bounded pipeline produced by `take(count)`.
        """
        return self.take(count)

    def drop(self, count: int) -> Flow[T]:
        """Skip count items before yielding the remainder.

        Args:
            count: Number of leading items to consume without emitting.

        Returns:
            A flow containing every source item after the first `count`.

        Raises:
            ValueError: If count is negative.
        """
        if count < 0:
            raise ValueError("drop count must be non-negative")
        return self._append(DropOp(count))

    def skip(self, count: int) -> Flow[T]:
        """Skip count items; alias of drop.

        Args:
            count: Number of leading items to consume without emitting.

        Returns:
            The same suffix pipeline produced by `drop(count)`.
        """
        return self.drop(count)

    def take_while(self, predicate: Callable[[T], bool]) -> Flow[T]:
        """Emit the longest prefix that satisfies predicate.

        Args:
            predicate: Called on leading items until its first falsey result.

        Returns:
            A flow ending before the first source item whose predicate result is falsey.
        """
        return self._append(TakeWhileOp(predicate))

    def take_while_inclusive(self, predicate: Callable[[T], bool]) -> Flow[T]:
        """Emit through the first item that fails predicate.

        Args:
            predicate: Called on leading items through its first falsey result.

        Returns:
            A flow ending after emitting the first item whose predicate result is falsey.
        """
        return self._append(TakeWhileInclusiveOp(predicate))

    def drop_while(self, predicate: Callable[[T], bool]) -> Flow[T]:
        """Skip the longest prefix that satisfies predicate.

        Args:
            predicate: Called only while leading items produce truthy results.

        Returns:
            A flow beginning with the first item whose predicate result is falsey; later items are
            emitted without further predicate calls.
        """
        return self._append(DropWhileOp(predicate))

    @contextmanager
    def _open(self) -> Generator[Iterator[T], None, None]:
        """Yield one pipeline iterator and close it when the context exits."""
        from ..execution.physical import execute_physical
        from ..planning.compiler import compile_iteration
        from ..runtime.report import _record_sync_plan

        physical = compile_iteration(self._query("iterate"))
        _record_sync_plan(physical)
        iterator = execute_physical(physical)
        with closing_iterators((iterator,)):
            yield iterator

    def _consume(self, consumer: Callable[[Iterator[T]], R]) -> R:
        """Consume one compiled iteration plan inside the terminal's call stack."""
        from ..execution.physical import consume_physical
        from ..planning.compiler import compile_iteration
        from ..runtime.report import _record_sync_plan

        physical = compile_iteration(self._query("iterate"))
        _record_sync_plan(physical)
        return consume_physical(
            physical,
            cast(Callable[[Iterator[Any]], R], consumer),
            self._pipeline if physical.root is None else None,
        )


_CANONICAL_FLOW_DROP = Flow.drop
_CANONICAL_FLOW_FIRST = Flow.first
_CANONICAL_FLOW_LAST = Flow.last
_CANONICAL_FLOW_QUERY = Flow._query
_CANONICAL_FLOW_QUERY_CODE = Flow._query.__code__
_CANONICAL_FLOW_TERMINAL_CONTEXT = Flow._terminal_context
_CANONICAL_FLOW_TERMINAL_CONTEXT_CODE = Flow._terminal_context.__code__
_CANONICAL_FLOW_COMPILED_TERMINAL_CONTEXT = Flow._compiled_terminal_context
_CANONICAL_FLOW_COMPILED_TERMINAL_CONTEXT_CODE = Flow._compiled_terminal_context.__code__
_CANONICAL_FLOW_NATIVE_DECISION = Flow._native_decision
_CANONICAL_FLOW_NATIVE_DECISION_CODE = Flow._native_decision.__code__


class _FlowFactory:
    """Callable factory for creating synchronous flows and deferred sources."""

    __slots__ = ()

    def _from_loaded_tabular_type(self, source: Any, module_root: str) -> Flow[Any] | None:
        """Use a vendor adapter only after confirming the genuine loaded runtime type."""
        if module_root == "polars":
            polars = cast(Any, sys.modules.get("polars"))
            if polars is not None and isinstance(source, (polars.DataFrame, polars.LazyFrame)):
                return self.from_polars(source)
        elif module_root == "pyarrow":
            pyarrow = cast(Any, sys.modules.get("pyarrow"))
            if pyarrow is not None and isinstance(
                source,
                (pyarrow.Table, pyarrow.RecordBatch, pyarrow.RecordBatchReader),
            ):
                return self.from_arrow(source)
        elif module_root == "pandas":
            pandas = cast(Any, sys.modules.get("pandas"))
            if pandas is not None and isinstance(source, pandas.DataFrame):
                return self.from_dataframe(source)
        return None

    if TYPE_CHECKING:

        @overload
        def __call__(
            self,
            source: pl.Series,
        ) -> Flow[Any]: ...

    @overload
    def __call__(self, source: Flow[T]) -> Flow[T]: ...

    @overload
    # Rows also exports Arrow, but runtime dispatch deliberately preserves its existing Flow[T]
    # before considering structural tabular protocols.
    def __call__(self, source: fpstreams.Rows[T]) -> Flow[T]: ...  # type: ignore[overload-overlap]

    @overload
    def __call__(
        self,
        source: _ArrowCStreamProvider | _DataFrameProvider,
    ) -> Flow[dict[str, Any]]: ...

    @overload
    def __call__(self, source: Iterable[T]) -> Flow[T]: ...

    @overload
    def __call__(self, source: Any) -> Flow[Any]: ...

    def __call__(self, source: Any) -> Flow[Any]:
        """Create a lazy flow, retaining recognized tabular source protocols.

        Args:
            source: A Flow, Rows view, iterable, or supported tabular protocol provider.

        Returns:
            A flow that emits items or record dictionaries when consumed.
        """
        source_type = type(source)
        # Exact built-ins include the common containers and iterator implementations (including
        # generator). They cannot acquire either tabular protocol, so avoid all adapter probes.
        if source_type.__module__ == "builtins":
            return Flow(source)
        if isinstance(source, Flow):
            return source
        rows_type = getattr(fpstreams, "Rows", ())
        if isinstance(source, rows_type):
            return cast(Flow[Any], source._flow)

        tabular_root = _tabular_module_root(source_type)
        if tabular_root is not None:
            adapted = self._from_loaded_tabular_type(source, tabular_root)
            if adapted is not None:
                return adapted
            # Series, Arrow arrays, and other one-dimensional vendor objects can
            # expose Arrow/dataframe protocols whose schema is not a record batch.
            # They keep their ordinary iterable semantics instead of being forced
            # through the record-oriented adapters below.
            return Flow(source)

        arrow_protocol = callable(getattr(source, "__arrow_c_stream__", None))
        dataframe_protocol = callable(getattr(source, "__dataframe__", None))
        # A custom dual-protocol provider chooses Arrow's standard stream. Concrete pandas,
        # Polars, and PyArrow objects were routed above to preserve their adapter semantics.
        if arrow_protocol:
            return self.from_arrow(source)
        if dataframe_protocol:
            return self.from_dataframe(source)
        return Flow(source)

    def from_arrow(self, source: Any, *, batch_size: int = 65_536) -> Flow[dict[str, Any]]:
        """Adapt Arrow data through the established Rows source implementation."""
        from ..tabular.rows import Rows

        return Rows.from_arrow(source, batch_size=batch_size)._flow

    def from_columns(
        self,
        columns: Mapping[str, Any],
        *,
        batch_size: int = 65_536,
    ) -> Flow[dict[str, Any]]:
        """Adapt an explicit mapping of independent columns through Arrow."""
        from ..tabular.rows import Rows

        return Rows.from_columns(columns, batch_size=batch_size)._flow

    def from_numpy(
        self,
        array: Any,
        *,
        columns: Iterable[str] | None = None,
    ) -> Flow[Any]:
        """Adapt a one-dimensional NumPy array to scalars or a two-dimensional one to rows."""
        from ..tabular.numpy import numpy_module, numpy_scalar_source
        from ..tabular.rows import Rows

        values = numpy_module("from_numpy()").asarray(array)
        if values.ndim == 1:
            return Flow(numpy_scalar_source(values, columns=columns))
        return Rows.from_numpy(values, columns=columns)._flow

    def from_dataframe(
        self,
        frame: Any,
        *,
        batch_size: int = 65_536,
        allow_copy: bool = True,
    ) -> Flow[dict[str, Any]]:
        """Adapt a dataframe interchange provider through the Rows implementation."""
        from ..tabular.rows import Rows

        return Rows.from_dataframe(
            frame,
            batch_size=batch_size,
            allow_copy=allow_copy,
        )._flow

    from_pandas = from_dataframe

    def from_polars(
        self,
        frame: Any,
        *,
        batch_size: int = 65_536,
        maintain_order: bool = True,
        engine: Any = "auto",
    ) -> Flow[dict[str, Any]]:
        """Adapt a Polars DataFrame or LazyFrame through the Rows implementation."""
        from ..tabular.rows import Rows

        return Rows.from_polars(
            frame,
            batch_size=batch_size,
            maintain_order=maintain_order,
            engine=engine,
        )._flow

    def scan_csv(
        self,
        path: str | os.PathLike[str],
        *,
        batch_size: int = 65_536,
        read_options: Any = None,
        parse_options: Any = None,
        convert_options: Any = None,
        memory_pool: Any = None,
    ) -> Flow[dict[str, Any]]:
        """Scan typed CSV batches through the established Rows adapter."""
        from ..tabular.rows import Rows

        return Rows.scan_csv(
            path,
            batch_size=batch_size,
            read_options=read_options,
            parse_options=parse_options,
            convert_options=convert_options,
            memory_pool=memory_pool,
        )._flow

    def from_parquet(
        self,
        source: Any,
        *,
        columns: Iterable[str] | None = None,
        filter: Any = None,
        batch_size: int = 65_536,
        use_threads: bool = True,
        filesystem: Any = None,
        partitioning: Any = None,
    ) -> Flow[dict[str, Any]]:
        """Scan Parquet data through the established Rows adapter."""
        from ..tabular.rows import Rows

        return Rows.from_parquet(
            source,
            columns=columns,
            filter=filter,
            batch_size=batch_size,
            use_threads=use_threads,
            filesystem=filesystem,
            partitioning=partitioning,
        )._flow

    def defer(self, factory: Callable[[], Iterable[T]]) -> Flow[T]:
        """Create a reusable flow that calls factory for each iteration.

        Args:
            factory: Called for each evaluation and returns that evaluation's synchronous iterable.

        Returns:
            A reusable flow that invokes `factory` separately for each evaluation.
        """
        return Flow(Source.defer(factory))

    def empty(self) -> Flow[Any]:
        """Create a flow that emits no items.

        Returns:
            A reusable flow that emits no items.
        """
        return Flow.empty()

    def of_nullable(self, value: T | None) -> Flow[T]:
        """Create an empty flow for None, otherwise emit the value once.

        Args:
            value: The optional item to emit; `None` produces an empty flow.

        Returns:
            A flow containing `value` once, or no items when `value` is `None`.
        """
        return Flow.of_nullable(value)

    def generate(self, supplier: Callable[[], T]) -> Flow[T]:
        """Create an infinite flow by calling supplier for each item.

        Args:
            supplier: Called once for each requested item to produce that item.

        Returns:
            A reusable, infinite flow of values returned by `supplier`.
        """
        return Flow.generate(supplier)

    def concat(self, *sources: Iterable[T]) -> Flow[T]:
        """Create a flow that emits each source in order.

        Args:
            *sources: Sources emitted sequentially by the new pipeline.

        Returns:
            A flow that drains each supplied source in argument order.
        """
        if not sources:
            return Flow(())
        first, *rest = sources
        return Flow(first).concat(*rest)

    def iterate(self, seed: T, function: Callable[[T], T]) -> Flow[T]:
        """Emit seed, then repeatedly apply function to the previous value.

        Args:
            seed: The first value emitted or used to initialize the sequence.
            function: Called with the previously emitted value to produce the next value.

        Returns:
            A reusable, infinite flow beginning with `seed`.
        """
        return Flow.iterate(seed, function)


flow = _FlowFactory()
