"""Physical tree values for record joins and aggregations."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Literal, TypeAlias

from ..collecting.aggregate_program import AggregationProgram
from ..planning.logical import JoinSpec
from ..planning.source import Source
from ..planning.sync import Engine, ParallelSettings
from .plan import PhysicalNode

if TYPE_CHECKING:
    from ..tabular.spill_limits import SpillLimits


@dataclass(frozen=True, slots=True)
class PhysicalRelNode:
    """Base class for a recursively executable relational physical tree."""

    logical_ids: tuple[int, ...]
    engine: Engine
    parallel: ParallelSettings | None


@dataclass(frozen=True, slots=True)
class SourcePhysicalNode(PhysicalRelNode):
    """An unopened relational leaf source."""

    source: Source[Any]


@dataclass(frozen=True, slots=True)
class PipelinePhysicalNode(PhysicalRelNode):
    """A unary physical stage sequence applied to one relational input."""

    input: PhysicalRelNode
    stages: tuple[PhysicalNode, ...]


class JoinStrategy(StrEnum):
    """Legal record-join execution strategies."""

    HASH_RIGHT = "hash_right"
    UNIQUE_RIGHT = "unique_right"
    GRACE_HASH = "grace_hash"


@dataclass(frozen=True, slots=True)
class CompiledJoinSpec:
    """Join metadata with selectors compiled once during physical planning."""

    logical: JoinSpec
    left_key: Callable[[Any], Any]
    right_key: Callable[[Any], Any]
    shared_names: frozenset[str]


@dataclass(frozen=True, slots=True)
class NativeGroupSumSpec:
    """Exact tuple indexes and output name for the guarded i64 group-sum kernel."""

    key_index: int
    value_index: int
    output_name: str


@dataclass(frozen=True, slots=True)
class NativeRecordGroupSumSpec:
    """Exact record fields and output name for the guarded i64 group-sum kernel."""

    key_field: str
    value_field: str
    output_name: str


@dataclass(frozen=True, slots=True)
class ArrowGroupSumSpec:
    """Exact Arrow fields accepted by the guarded columnar i64 group-sum path."""

    key_field: str
    value_field: str
    output_name: str


@dataclass(frozen=True, slots=True)
class ArrowGlobalSumSpec:
    """Exact Arrow field and output name for one guarded columnar reduction.

    The legacy class and physical-node field names remain stable because they are
    internal plan-inspection surfaces already exercised by downstream tests.
    """

    value_field: str
    output_name: str
    kind: Literal["sum", "min", "max", "first", "last"] = "sum"


@dataclass(frozen=True, slots=True)
class NativeRecordJoinSpec:
    """Exact record fields accepted by the guarded eager unique-right join ABI."""

    left_field: str
    right_field: str


@dataclass(frozen=True, slots=True)
class ArrowUniqueJoinSpec:
    """Exact Arrow fields accepted by the guarded retained m:1 join path."""

    left_field: str
    right_field: str


@dataclass(frozen=True, slots=True)
class SimpleGroupSumSpec:
    """Raw field selectors and compiled value access for one selected group sum.

    The raw selectors let the Python executor use exact-dict subscription without
    weakening the full selector protocol used for mappings, objects, dotted paths,
    and indexable rows.
    """

    key_selector: Callable[[Any], Any] | str | int
    value_selector: Callable[[Any], Any] | str | int
    select_value: Callable[[Any], Any]
    output_name: str


GroupLaneKind: TypeAlias = Literal["count", "sum", "min", "max", "first", "last"]


@dataclass(frozen=True, slots=True)
class GroupLane:
    """One closed aggregation stored as a contiguous per-group state lane."""

    output_name: str
    kind: GroupLaneKind
    selector: Callable[[Any], Any] | str | int | None
    select_value: Callable[[Any], Any] | None


@dataclass(frozen=True, slots=True)
class ClosedGroupSpec:
    """A one-key group whose project-owned aggregations need no collector objects."""

    key_selector: Callable[[Any], Any] | str | int
    lanes: tuple[GroupLane, ...]


@dataclass(frozen=True, slots=True)
class CompositeCountSumSpec:
    """Two direct group keys and one direct value for a count-plus-sum loop."""

    key_selectors: tuple[str | int, str | int]
    value_selector: str | int
    select_value: Callable[[Any], Any]
    count_name: str
    sum_name: str


@dataclass(frozen=True, slots=True)
class NativeFixedI64GroupSpec:
    """One exact tuple/dict count shape accepted by the guarded fixed-group ABI."""

    row_kind: Literal["tuple", "dict"]
    key_selector: int | str
    value_selector: int | str | None
    count_name: str
    sum_name: str | None


@dataclass(frozen=True, slots=True)
class NativeCallableGroupSpec:
    """One exact-record count/sum shape with exactly one opaque callback lane."""

    callback_side: Literal["key", "value"]
    direct_field: str
    count_name: str
    sum_name: str


@dataclass(frozen=True, slots=True)
class SpillCountSpec:
    """Closed explicit-spill shape eligible for bounded count preaggregation."""

    key_field: str
    output_name: str


@dataclass(frozen=True, slots=True)
class JoinPhysicalNode(PhysicalRelNode):
    """A binary join with a preselected legal strategy."""

    left: PhysicalRelNode
    right: PhysicalRelNode
    spec: CompiledJoinSpec
    strategy: JoinStrategy
    reason: str
    arrow_unique: ArrowUniqueJoinSpec | None
    native_record_i64: NativeRecordJoinSpec | None
    native_callable_unique: bool
    native_callable_many: bool


@dataclass(frozen=True, slots=True)
class GroupAggregatePhysicalNode(PhysicalRelNode):
    """A grouped aggregation over one recursively executable input."""

    input: PhysicalRelNode
    key_names: tuple[str, ...]
    keys: tuple[Callable[[Any], Any], ...]
    select_key: Callable[[Any], Any]
    aggregations: AggregationProgram
    spill_count: SpillCountSpec | None
    simple_sum: SimpleGroupSumSpec | None
    closed_group: ClosedGroupSpec | None
    composite_count_sum: CompositeCountSumSpec | None
    native_fixed_i64_group: NativeFixedI64GroupSpec | None
    native_callable_group: NativeCallableGroupSpec | None
    arrow_i64_sum: ArrowGroupSumSpec | None
    native_i64_sum: NativeGroupSumSpec | None
    native_record_i64_sum: NativeRecordGroupSumSpec | None
    partitions: int | None
    tempdir: str | None
    limits: SpillLimits | None


@dataclass(frozen=True, slots=True)
class GlobalAggregatePhysicalNode(PhysicalRelNode):
    """A global one-row aggregation over one recursively executable input."""

    input: PhysicalRelNode
    aggregations: AggregationProgram
    exact_count_name: str | None
    arrow_count_name: str | None
    arrow_i64_sum: ArrowGlobalSumSpec | None
