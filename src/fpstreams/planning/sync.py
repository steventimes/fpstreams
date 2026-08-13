"""Immutable operation nodes for synchronous flow plans."""

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
    workers: int | None
    backend: ParallelBackend
    ordered: bool
    buffer: int


@dataclass(frozen=True, slots=True)
class MapOp:
    function: Callable[[Any], Any]
    name: str = "map"


@dataclass(frozen=True, slots=True)
class ParallelMapOp:
    function: Callable[[Any], Any]
    workers: int | None
    backend: ParallelBackend
    ordered: bool
    buffer: int
    name: str = "map_parallel"


@dataclass(frozen=True, slots=True)
class TapOp:
    function: Callable[[Any], None]
    name: str = "tap"


@dataclass(frozen=True, slots=True)
class FilterOp:
    predicate: Callable[[Any], Any]
    negate: bool = False
    name: str = "filter"


@dataclass(frozen=True, slots=True)
class FlatMapOp:
    function: Callable[[Any], Any]
    name: str = "flat_map"


@dataclass(frozen=True, slots=True)
class TakeOp:
    count: int
    name: str = "take"


@dataclass(frozen=True, slots=True)
class DropOp:
    count: int
    name: str = "drop"


@dataclass(frozen=True, slots=True)
class TakeWhileOp:
    predicate: Callable[[Any], bool]
    name: str = "take_while"


@dataclass(frozen=True, slots=True)
class TakeWhileInclusiveOp:
    predicate: Callable[[Any], bool]
    name: str = "take_while_inclusive"


@dataclass(frozen=True, slots=True)
class DropWhileOp:
    predicate: Callable[[Any], bool]
    name: str = "drop_while"


@dataclass(frozen=True, slots=True)
class UniqueOp:
    key: Callable[[Any], Any] | None = None
    name: str = "unique"


@dataclass(frozen=True, slots=True)
class ChunkOp:
    size: int
    name: str = "chunk"


@dataclass(frozen=True, slots=True)
class WindowOp:
    size: int
    step: int
    name: str = "window"


@dataclass(frozen=True, slots=True)
class GroupRunsOp:
    key: Callable[[Any], Any]
    name: str = "group_runs"


@dataclass(frozen=True, slots=True)
class PairwiseOp:
    name: str = "pairwise"


@dataclass(frozen=True, slots=True)
class EnumerateOp:
    start: int
    name: str = "enumerate"


@dataclass(frozen=True, slots=True)
class ZipOp:
    source: Source[Any]
    strict: bool
    name: str = "zip"


@dataclass(frozen=True, slots=True)
class ZipLongestOp:
    source: Source[Any]
    fillvalue: Any
    name: str = "zip_longest"


@dataclass(frozen=True, slots=True)
class IntersperseOp:
    separator: Any
    name: str = "intersperse"


@dataclass(frozen=True, slots=True)
class ConcatOp:
    sources: tuple[Source[Any], ...]
    name: str = "concat"


@dataclass(frozen=True, slots=True)
class CrossOp:
    source: Source[Any]
    max_right: int | None
    name: str = "cross"


@dataclass(frozen=True, slots=True)
class ScanOp:
    initial: Any
    function: Callable[[Any, Any], Any]
    name: str = "scan"


@dataclass(frozen=True, slots=True)
class ScanRightOp:
    initial: Any
    function: Callable[[Any, Any], Any]
    max_items: int | None
    name: str = "scan_right"


@dataclass(frozen=True, slots=True)
class SortOp:
    key: Callable[[Any], Any] | None
    reverse: bool
    buffer_size: int | None = None
    tempdir: str | PathLike[str] | None = None
    name: str = "sort"


@dataclass(frozen=True, slots=True)
class GatherOp:
    gatherer: Gatherer[Any, Any, Any]
    name: str = "gather"


@dataclass(frozen=True, slots=True)
class PrependOp:
    values: tuple[Any, ...]
    name: str = "prepend"


@dataclass(frozen=True, slots=True)
class AppendOp:
    values: tuple[Any, ...]
    name: str = "append"


@dataclass(frozen=True, slots=True)
class MapFirstOp:
    function: Callable[[Any], Any]
    name: str = "map_first"


@dataclass(frozen=True, slots=True)
class MapLastOp:
    function: Callable[[Any], Any]
    name: str = "map_last"


@dataclass(frozen=True, slots=True)
class CollapseOp:
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
    source: Source[Any]
    operations: tuple[Operation, ...] = ()
    engine: Engine = "auto"
    parallel: ParallelSettings | None = None

    def append(self, operation: Operation) -> Plan:
        return Plan(
            self.source,
            (*self.operations, operation),
            self.engine,
            self.parallel,
        )

    def with_engine(self, engine: Engine) -> Plan:
        return Plan(self.source, self.operations, engine, self.parallel)

    def with_parallel(self, settings: ParallelSettings | None) -> Plan:
        return Plan(self.source, self.operations, self.engine, settings)
