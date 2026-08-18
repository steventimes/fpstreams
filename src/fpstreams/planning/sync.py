"""Immutable nodes that describe synchronous stream operations without executing them."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from os import PathLike
from typing import Any, Literal

from .gather import Gatherer
from .source import Source

Engine = Literal["auto", "python", "native"]
ParallelBackend = Literal["thread", "process"]


@dataclass(frozen=True, slots=True)
class ParallelSettings:
    """Configure workers, backend, output ordering, and in-flight buffering for parallel maps."""

    workers: int | None
    backend: ParallelBackend
    ordered: bool
    buffer: int


@dataclass(frozen=True, slots=True)
class MapOp:
    """Replace each input item with the result of ``function``."""

    function: Callable[[Any], Any]
    name: str = "map"


@dataclass(frozen=True, slots=True)
class ParallelMapOp:
    """Map items in a bounded thread or process pool, optionally preserving input order."""

    function: Callable[[Any], Any]
    workers: int | None
    backend: ParallelBackend
    ordered: bool
    buffer: int
    name: str = "map_parallel"


@dataclass(frozen=True, slots=True)
class TapOp:
    """Call ``function`` for its side effect, then emit the original item."""

    function: Callable[[Any], None]
    name: str = "tap"


@dataclass(frozen=True, slots=True)
class FilterOp:
    """Keep items whose predicate truth value differs from ``negate``."""

    predicate: Callable[[Any], Any]
    negate: bool = False
    name: str = "filter"


@dataclass(frozen=True, slots=True)
class FlatMapOp:
    """Replace each input item with all values from the iterable returned by ``function``."""

    function: Callable[[Any], Any]
    name: str = "flat_map"


@dataclass(frozen=True, slots=True)
class TakeOp:
    """Emit at most the first ``count`` input items."""

    count: int
    name: str = "take"


@dataclass(frozen=True, slots=True)
class DropOp:
    """Suppress the first ``count`` input items and emit the remainder."""

    count: int
    name: str = "drop"


@dataclass(frozen=True, slots=True)
class TakeWhileOp:
    """Emit the leading items that satisfy ``predicate``, excluding the first failure."""

    predicate: Callable[[Any], bool]
    name: str = "take_while"


@dataclass(frozen=True, slots=True)
class TakeWhileInclusiveOp:
    """Emit through the first item that fails ``predicate``, including that boundary item."""

    predicate: Callable[[Any], bool]
    name: str = "take_while_inclusive"


@dataclass(frozen=True, slots=True)
class DropWhileOp:
    """Suppress the leading items that satisfy ``predicate``, then emit every later item."""

    predicate: Callable[[Any], bool]
    name: str = "drop_while"


@dataclass(frozen=True, slots=True)
class UniqueOp:
    """Emit the first item for each distinct item or derived key."""

    key: Callable[[Any], Any] | None = None
    name: str = "unique"


@dataclass(frozen=True, slots=True)
class ChunkOp:
    """Group consecutive items into non-overlapping tuples of at most ``size`` values."""

    size: int
    name: str = "chunk"


@dataclass(frozen=True, slots=True)
class WindowOp:
    """Emit full sliding windows separated by ``step`` items.

    When the entire input is shorter than ``size``, the executor emits one partial window.
    """

    size: int
    step: int
    name: str = "window"


@dataclass(frozen=True, slots=True)
class GroupRunsOp:
    """Group contiguous items with equal derived keys into tuples."""

    key: Callable[[Any], Any]
    name: str = "group_runs"


@dataclass(frozen=True, slots=True)
class PairwiseOp:
    """Emit overlapping pairs of adjacent input items."""

    name: str = "pairwise"


@dataclass(frozen=True, slots=True)
class EnumerateOp:
    """Pair each item with a consecutive index beginning at ``start``."""

    start: int
    name: str = "enumerate"


@dataclass(frozen=True, slots=True)
class ZipOp:
    """Pair the primary stream with another source, optionally requiring equal lengths."""

    source: Source[Any]
    strict: bool
    name: str = "zip"


@dataclass(frozen=True, slots=True)
class ZipLongestOp:
    """Pair two streams through the longer input, filling missing positions with ``fillvalue``."""

    source: Source[Any]
    fillvalue: Any
    name: str = "zip_longest"


@dataclass(frozen=True, slots=True)
class IntersperseOp:
    """Insert ``separator`` between consecutive input items."""

    separator: Any
    name: str = "intersperse"


@dataclass(frozen=True, slots=True)
class ConcatOp:
    """Emit the primary stream followed by each additional source in order."""

    sources: tuple[Source[Any], ...]
    name: str = "concat"


@dataclass(frozen=True, slots=True)
class CrossOp:
    """Form a Cartesian product after buffering the right source, subject to ``max_right``."""

    source: Source[Any]
    max_right: int | None
    name: str = "cross"


@dataclass(frozen=True, slots=True)
class ScanOp:
    """Emit each left-to-right accumulator state after consuming an item."""

    initial: Any
    function: Callable[[Any, Any], Any]
    name: str = "scan"


@dataclass(frozen=True, slots=True)
class ScanRightOp:
    """Buffer input and emit right-to-left accumulator states in original item order.

    ``max_items`` limits buffering and raises a buffer-limit error when exceeded.
    """

    initial: Any
    function: Callable[[Any, Any], Any]
    max_items: int | None
    name: str = "scan_right"


@dataclass(frozen=True, slots=True)
class SortOp:
    """Globally sort input in memory or via bounded external runs when ``buffer_size`` is set."""

    key: Callable[[Any], Any] | None
    reverse: bool
    buffer_size: int | None = None
    tempdir: str | PathLike[str] | None = None
    name: str = "sort"


@dataclass(frozen=True, slots=True)
class GatherOp:
    """Apply a custom stateful gatherer that may emit zero or more values per input."""

    gatherer: Gatherer[Any, Any, Any]
    name: str = "gather"


@dataclass(frozen=True, slots=True)
class PrependOp:
    """Emit configured values before the upstream stream."""

    values: tuple[Any, ...]
    name: str = "prepend"


@dataclass(frozen=True, slots=True)
class AppendOp:
    """Emit configured values after the upstream stream completes."""

    values: tuple[Any, ...]
    name: str = "append"


@dataclass(frozen=True, slots=True)
class MapFirstOp:
    """Transform only the first item, leaving an empty stream empty."""

    function: Callable[[Any], Any]
    name: str = "map_first"


@dataclass(frozen=True, slots=True)
class MapLastOp:
    """Hold one item back so only the final item is transformed."""

    function: Callable[[Any], Any]
    name: str = "map_last"


@dataclass(frozen=True, slots=True)
class CollapseOp:
    """Merge adjacent collapsible items into one aggregate per contiguous run."""

    collapsible: Callable[[Any, Any], bool]
    merger: Callable[[Any, Any], Any]
    name: str = "collapse"


Operation = (
    MapOp
    | ParallelMapOp
    | TapOp
    | FilterOp
    | FlatMapOp
    | TakeOp
    | DropOp
    | TakeWhileOp
    | TakeWhileInclusiveOp
    | DropWhileOp
    | UniqueOp
    | ChunkOp
    | WindowOp
    | GroupRunsOp
    | PairwiseOp
    | EnumerateOp
    | ZipOp
    | ZipLongestOp
    | IntersperseOp
    | ConcatOp
    | CrossOp
    | ScanOp
    | ScanRightOp
    | SortOp
    | GatherOp
    | PrependOp
    | AppendOp
    | MapFirstOp
    | MapLastOp
    | CollapseOp
)


@dataclass(frozen=True, slots=True)
class Plan:
    """Bind a source to an immutable operation sequence and execution preferences."""

    source: Source[Any]
    operations: tuple[Operation, ...] = ()
    engine: Engine = "auto"
    parallel: ParallelSettings | None = None

    def append(self, operation: Operation) -> Plan:
        """Return a new plan with ``operation`` appended to the lazy pipeline."""
        return Plan(
            self.source,
            (*self.operations, operation),
            self.engine,
            self.parallel,
        )

    def with_engine(self, engine: Engine) -> Plan:
        """Return a copy requesting automatic, Python, or native execution."""
        return Plan(self.source, self.operations, engine, self.parallel)

    def with_parallel(self, settings: ParallelSettings | None) -> Plan:
        """Return a copy with plan-level parallel settings replaced or cleared."""
        return Plan(self.source, self.operations, self.engine, settings)
