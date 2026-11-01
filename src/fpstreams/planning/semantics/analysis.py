"""Register conservative fact, progress, state, and completion rules for every plan node."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ..async_ import (
    AsyncLogicalPlan,
    _Append,
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
    _FilterMap,
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
    _Prefetch,
    _Prepend,
    _Scan,
    _ScanRight,
    _SessionWindow,
    _SwitchMap,
    _Take,
    _TakeWhile,
    _TakeWhileInclusive,
    _Tap,
    _Throttle,
    _Timeout,
    _Unique,
    _Window,
    _WindowReduce,
    _Zip,
    _ZipLongest,
)
from ..logical import Pipeline
from ..sync import (
    AppendOp,
    ChunkOp,
    CollapseOp,
    ConcatOp,
    CrossOp,
    DropOp,
    DropWhileOp,
    EnumerateOp,
    FilterMapOp,
    FilterOp,
    FlatMapOp,
    GatherOp,
    GroupRunsOp,
    IntersperseOp,
    MapFirstOp,
    MapLastOp,
    MapOp,
    PairwiseOp,
    ParallelMapOp,
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
    WindowReduceOp,
    ZipLongestOp,
    ZipOp,
)
from . import (
    AsyncTerminalName,
    Cardinality,
    CardinalityKind,
    CompletionCondition,
    CompletionDependency,
    DiagnosticSeverity,
    OperatorAnalysis,
    OrderingGuarantee,
    PlanDiagnostic,
    PlanSemantics,
    ProgressKind,
    StateKind,
    StateProfile,
    StreamFacts,
    TerminationEvidence,
)

FactsTransfer = Callable[[StreamFacts, Any], StreamFacts]
StateResolver = Callable[[Any], StateProfile]
DependencyResolver = Callable[[StreamFacts, Any], tuple[CompletionDependency, ...]]


@dataclass(frozen=True, slots=True)
class OperatorRule:
    """Define how one node transforms facts and reports progress, state, and dependencies."""

    name: str
    progress: ProgressKind
    state: StateResolver
    transfer: FactsTransfer
    completion_dependencies: DependencyResolver
    requires_order: bool = False


def _facts(
    source: StreamFacts,
    *,
    termination: TerminationEvidence | None = None,
    cardinality: Cardinality | None = None,
    ordering: OrderingGuarantee | None = None,
) -> StreamFacts:
    """Copy source facts while replacing selected termination, cardinality, or ordering fields."""
    return StreamFacts(
        termination or source.termination,
        cardinality or source.cardinality,
        source.replayability,
        ordering or source.ordering,
    )


def _source_end(source: StreamFacts, _operation: Any) -> tuple[CompletionDependency, ...]:
    """Declare that completion requires exhaustion of the upstream stream."""
    return (CompletionDependency("upstream", source),)


def _none(_source: StreamFacts, _operation: Any) -> tuple[CompletionDependency, ...]:
    """Declare that an operator has no whole-input completion dependency."""
    return ()


def _preserve(source: StreamFacts, _operation: Any) -> StreamFacts:
    """Pass every upstream semantic fact through unchanged."""
    return source


def _filter(source: StreamFacts, _operation: Any) -> StreamFacts:
    """Downgrade a positive exact count to an upper bound while preserving exact emptiness."""
    cardinality = source.cardinality
    if cardinality.kind is CardinalityKind.EXACT and cardinality.value == 0:
        output = cardinality
    elif cardinality.kind is CardinalityKind.EXACT:
        output = Cardinality.upper_bound(cardinality.value or 0)
    else:
        output = cardinality
    termination = (
        TerminationEvidence.UNKNOWN
        if source.termination is TerminationEvidence.PROVEN_INFINITE
        else source.termination
    )
    return _facts(source, cardinality=output, termination=termination)


def _surround(source: StreamFacts, operation: Any) -> StreamFacts:
    cardinality = source.cardinality
    if cardinality.kind is CardinalityKind.EXACT:
        cardinality = Cardinality.exact((cardinality.value or 0) + len(operation.values))
    elif cardinality.kind is CardinalityKind.UPPER_BOUND:
        cardinality = Cardinality.upper_bound((cardinality.value or 0) + len(operation.values))
    return _facts(source, cardinality=cardinality)


def _intersperse(source: StreamFacts, _operation: Any) -> StreamFacts:
    cardinality = source.cardinality
    if cardinality.kind is CardinalityKind.EXACT:
        cardinality = Cardinality.exact(max(0, 2 * (cardinality.value or 0) - 1))
    elif cardinality.kind is CardinalityKind.UPPER_BOUND:
        cardinality = Cardinality.upper_bound(max(0, 2 * (cardinality.value or 0) - 1))
    return _facts(source, cardinality=cardinality)


def _flat_map(source: StreamFacts, _operation: Any) -> StreamFacts:
    """Preserve known emptiness; otherwise forget termination and cardinality after expansion."""
    if source.cardinality.kind is CardinalityKind.EXACT and source.cardinality.value == 0:
        return _facts(source, cardinality=Cardinality.exact(0))
    return _facts(
        source,
        termination=TerminationEvidence.UNKNOWN,
        cardinality=Cardinality.unknown(),
    )


def _take(source: StreamFacts, operation: Any) -> StreamFacts:
    """Cap cardinality at ``count``; zero is finite, and positive counts use upstream evidence."""
    count = operation.count
    if count == 0:
        return _facts(
            source,
            termination=TerminationEvidence.PROVEN_FINITE,
            cardinality=Cardinality.exact(0),
        )
    cardinality = source.cardinality
    if cardinality.kind is CardinalityKind.EXACT:
        output_cardinality = Cardinality.exact(min(count, cardinality.value or 0))
    elif cardinality.kind is CardinalityKind.UPPER_BOUND:
        output_cardinality = Cardinality.upper_bound(min(count, cardinality.value or 0))
    else:
        output_cardinality = Cardinality.upper_bound(count)
    termination = source.termination
    if source.termination in {
        TerminationEvidence.PROVEN_FINITE,
        TerminationEvidence.PROVEN_INFINITE,
    }:
        termination = TerminationEvidence.PROVEN_FINITE
    return _facts(source, termination=termination, cardinality=output_cardinality)


def _take_while(source: StreamFacts, _operation: Any) -> StreamFacts:
    """Preserve proven finiteness but otherwise make termination and cardinality unknown."""
    termination = (
        TerminationEvidence.PROVEN_FINITE
        if source.termination is TerminationEvidence.PROVEN_FINITE
        else TerminationEvidence.UNKNOWN
    )
    return _facts(source, termination=termination, cardinality=Cardinality.unknown())


def _drop(source: StreamFacts, operation: Any) -> StreamFacts:
    """Subtract ``count`` from exact or upper-bound cardinality without going below zero."""
    count = operation.count
    cardinality = source.cardinality
    if cardinality.kind is CardinalityKind.EXACT:
        cardinality = Cardinality.exact(max(0, (cardinality.value or 0) - count))
    elif cardinality.kind is CardinalityKind.UPPER_BOUND:
        cardinality = Cardinality.upper_bound(max(0, (cardinality.value or 0) - count))
    return _facts(source, cardinality=cardinality)


def _pairwise(source: StreamFacts, _operation: Any) -> StreamFacts:
    """Reduce exact or bounded cardinality by one for adjacent output pairs."""
    cardinality = source.cardinality
    if cardinality.kind is CardinalityKind.EXACT:
        cardinality = Cardinality.exact(max(0, (cardinality.value or 0) - 1))
    elif cardinality.kind is CardinalityKind.UPPER_BOUND:
        cardinality = Cardinality.upper_bound(max(0, (cardinality.value or 0) - 1))
    return _facts(source, cardinality=cardinality)


def _chunk(source: StreamFacts, operation: Any) -> StreamFacts:
    """Bound chunk counts, accounting for a strict incomplete final chunk."""
    cardinality = source.cardinality
    if cardinality.kind is CardinalityKind.EXACT:
        count = cardinality.value or 0
        if operation.strict and count % operation.size:
            cardinality = Cardinality.upper_bound(count // operation.size)
        else:
            cardinality = Cardinality.exact((count + operation.size - 1) // operation.size)
    elif cardinality.kind is CardinalityKind.UPPER_BOUND:
        count = cardinality.value or 0
        cardinality = Cardinality.upper_bound(
            count // operation.size
            if operation.strict
            else (count + operation.size - 1) // operation.size
        )
    return _facts(source, cardinality=cardinality)


def _window(source: StreamFacts, operation: Any) -> StreamFacts:
    """Compute exact stepped-window output only when the input cardinality is exact."""
    if source.cardinality.kind is CardinalityKind.EXACT:
        count = source.cardinality.value or 0
        output = (
            max(0, ((count - operation.size) // operation.step) + 1)
            if count >= operation.size
            else (1 if count else 0)
        )
        cardinality = Cardinality.exact(output)
    else:
        cardinality = Cardinality.unknown()
    return _facts(source, cardinality=cardinality)


def _concat(source: StreamFacts, operation: Any) -> StreamFacts:
    """Sum all proven-finite exact inputs, otherwise conservatively classify concatenation."""
    sources = (source, *(item.current_facts() for item in operation.sources))
    total = 0
    exact = True
    for item in sources:
        if item.termination is TerminationEvidence.PROVEN_INFINITE:
            return _facts(
                source,
                termination=TerminationEvidence.PROVEN_INFINITE,
                cardinality=Cardinality.unknown(),
            )
        if item.termination is not TerminationEvidence.PROVEN_FINITE:
            exact = False
            break
        if item.cardinality.kind is CardinalityKind.EXACT:
            total += item.cardinality.value or 0
        else:
            exact = False
    if exact:
        return _facts(
            source,
            termination=TerminationEvidence.PROVEN_FINITE,
            cardinality=Cardinality.exact(total),
        )
    return _facts(
        source,
        termination=TerminationEvidence.UNKNOWN,
        cardinality=Cardinality.unknown(),
    )


def _zip(source: StreamFacts, operation: Any) -> StreamFacts:
    """Derive shortest-input termination and the minimum available cardinality bound."""
    right = operation.source.current_facts()
    if (
        source.termination is not TerminationEvidence.UNKNOWN
        and right.termination is not TerminationEvidence.UNKNOWN
        and (
            source.termination is TerminationEvidence.PROVEN_FINITE
            or right.termination is TerminationEvidence.PROVEN_FINITE
        )
    ):
        termination = TerminationEvidence.PROVEN_FINITE
    elif (
        source.termination is TerminationEvidence.PROVEN_INFINITE
        and right.termination is TerminationEvidence.PROVEN_INFINITE
    ):
        termination = TerminationEvidence.PROVEN_INFINITE
    else:
        termination = TerminationEvidence.UNKNOWN
    cardinality = Cardinality.unknown()
    if (
        source.cardinality.kind is CardinalityKind.EXACT
        and right.cardinality.kind is CardinalityKind.EXACT
    ):
        left_size = source.cardinality.value or 0
        right_size = right.cardinality.value or 0
        count = min(left_size, right_size)
        cardinality = (
            Cardinality.upper_bound(count)
            if operation.strict and left_size != right_size
            else Cardinality.exact(count)
        )
    elif (
        source.cardinality.kind is not CardinalityKind.UNKNOWN
        and right.cardinality.kind is not CardinalityKind.UNKNOWN
    ):
        cardinality = Cardinality.upper_bound(
            min(source.cardinality.value or 0, right.cardinality.value or 0)
        )
    return _facts(source, termination=termination, cardinality=cardinality)


def _zip_longest(source: StreamFacts, operation: Any) -> StreamFacts:
    """Derive longest-input termination while leaving output cardinality unknown."""
    right = operation.source.current_facts()
    if (
        source.termination is not TerminationEvidence.UNKNOWN
        and right.termination is not TerminationEvidence.UNKNOWN
        and (
            source.termination is TerminationEvidence.PROVEN_INFINITE
            or right.termination is TerminationEvidence.PROVEN_INFINITE
        )
    ):
        termination = TerminationEvidence.PROVEN_INFINITE
    elif (
        source.termination is TerminationEvidence.PROVEN_FINITE
        and right.termination is TerminationEvidence.PROVEN_FINITE
    ):
        termination = TerminationEvidence.PROVEN_FINITE
    else:
        termination = TerminationEvidence.UNKNOWN
    return _facts(source, termination=termination, cardinality=Cardinality.unknown())


def _cross(source: StreamFacts, operation: Any) -> StreamFacts:
    """Derive Cartesian-product finiteness and multiply two exact finite counts."""
    right = operation.source.current_facts()
    if source.cardinality.kind is CardinalityKind.EXACT and source.cardinality.value == 0:
        return _facts(
            source,
            termination=TerminationEvidence.PROVEN_FINITE,
            cardinality=Cardinality.exact(0),
        )
    if (
        operation.max_right is not None
        and right.cardinality.kind is CardinalityKind.EXACT
        and (right.cardinality.value or 0) > operation.max_right
    ):
        return _facts(
            source,
            termination=(
                TerminationEvidence.PROVEN_FINITE
                if source.termination is TerminationEvidence.PROVEN_FINITE
                else TerminationEvidence.UNKNOWN
            ),
            cardinality=Cardinality.upper_bound(0),
        )
    if (
        source.termination is TerminationEvidence.PROVEN_FINITE
        and right.termination is TerminationEvidence.PROVEN_FINITE
    ):
        if (
            source.cardinality.kind is CardinalityKind.EXACT
            and right.cardinality.kind is CardinalityKind.EXACT
        ):
            return _facts(
                source,
                termination=TerminationEvidence.PROVEN_FINITE,
                cardinality=Cardinality.exact(
                    (source.cardinality.value or 0) * (right.cardinality.value or 0)
                ),
            )
        return _facts(
            source,
            termination=TerminationEvidence.PROVEN_FINITE,
            cardinality=Cardinality.unknown(),
        )
    if (
        operation.max_right is None
        and right.termination is TerminationEvidence.PROVEN_INFINITE
        and source.termination is TerminationEvidence.PROVEN_FINITE
        and source.cardinality.kind is CardinalityKind.EXACT
        and (source.cardinality.value or 0) > 0
    ):
        return _facts(
            source,
            termination=TerminationEvidence.PROVEN_INFINITE,
            cardinality=Cardinality.unknown(),
        )
    return _facts(
        source,
        termination=TerminationEvidence.UNKNOWN,
        cardinality=Cardinality.unknown(),
    )


def _cross_dependencies(source: StreamFacts, operation: Any) -> tuple[CompletionDependency, ...]:
    """The right side is exhausted only after a left value, unless a cap fails first."""
    if operation.max_right is not None or (
        source.cardinality.kind is CardinalityKind.EXACT and source.cardinality.value == 0
    ):
        return ()
    right = operation.source.current_facts()
    if (
        source.cardinality.kind is not CardinalityKind.EXACT
        and right.termination is TerminationEvidence.PROVEN_INFINITE
    ):
        right = _facts(right, termination=TerminationEvidence.UNKNOWN)
    return (CompletionDependency("right (if left is nonempty)", right),)


def _merge(source: StreamFacts, operation: Any) -> StreamFacts:
    """Combine concurrent-source termination and mark cardinality and encounter order unknown."""
    facts = (source, *(item.current_facts() for item in operation.sources))
    if any(item.termination is TerminationEvidence.PROVEN_INFINITE for item in facts):
        termination = TerminationEvidence.PROVEN_INFINITE
    elif all(item.termination is TerminationEvidence.PROVEN_FINITE for item in facts):
        termination = TerminationEvidence.PROVEN_FINITE
    else:
        termination = TerminationEvidence.UNKNOWN
    return _facts(
        source,
        termination=termination,
        cardinality=Cardinality.unknown(),
        ordering=OrderingGuarantee.UNKNOWN,
    )


def _combine_latest(source: StreamFacts, operation: Any) -> StreamFacts:
    """Classify latest-value combination, including the finite-empty case for any empty input."""
    facts = (source, *(item.current_facts() for item in operation.sources))
    if any(
        item.cardinality.kind is CardinalityKind.EXACT and item.cardinality.value == 0
        for item in facts
    ):
        return _facts(
            source,
            termination=TerminationEvidence.PROVEN_FINITE,
            cardinality=Cardinality.exact(0),
        )
    if any(item.termination is TerminationEvidence.PROVEN_INFINITE for item in facts):
        termination = TerminationEvidence.PROVEN_INFINITE
    elif all(item.termination is TerminationEvidence.PROVEN_FINITE for item in facts):
        termination = TerminationEvidence.PROVEN_FINITE
    else:
        termination = TerminationEvidence.UNKNOWN
    return _facts(
        source,
        termination=termination,
        cardinality=Cardinality.unknown(),
        ordering=OrderingGuarantee.UNKNOWN,
    )


def _state_stateless(_operation: Any) -> StateProfile:
    """Resolve an operator to a stateless memory profile."""
    return StateProfile.stateless()


def _state_bounded(operation: Any) -> StateProfile:
    """Infer a non-negative state bound from the first configured buffer or concurrency field."""
    bound = (
        getattr(operation, "buffer", None)
        or getattr(operation, "size", None)
        or getattr(operation, "max_items", None)
        or getattr(operation, "max_count", None)
        or getattr(operation, "capacity", None)
        or getattr(operation, "concurrency", None)
    )
    return StateProfile.bounded(max(0, int(bound or 0)))


def _window_reduce_state(operation: Any) -> StateProfile:
    """Bound cached entries, including the back aggregate, rather than their byte size."""
    return StateProfile.bounded(operation.size + 1)


def _state_unique(_operation: Any) -> StateProfile:
    """Classify distinct-value tracking as state that grows with observed keys."""
    return StateProfile.grows_with_keys()


def _state_input(_operation: Any) -> StateProfile:
    """Classify an operator as potentially retaining its entire input."""
    return StateProfile.grows_with_input()


def _cross_state(operation: Any) -> StateProfile:
    return (
        StateProfile.bounded(operation.max_right)
        if operation.max_right is not None
        else StateProfile.grows_with_input()
    )


def _state_unknown(_operation: Any) -> StateProfile:
    """Return an unknown state profile when the node exposes no usable memory contract."""
    return StateProfile.unknown()


def _dependencies_for(progress: ProgressKind) -> DependencyResolver:
    """Require upstream exhaustion only for global-final and side-input-final progress modes."""
    return (
        _source_end
        if progress in {ProgressKind.GLOBAL_FINAL, ProgressKind.SIDE_INPUT_FINAL}
        else _none
    )


def _rule(
    cls: type[Any],
    *,
    progress: ProgressKind = ProgressKind.PIPELINED,
    state: StateResolver = _state_stateless,
    transfer: FactsTransfer = _preserve,
    requires_order: bool = False,
    dependencies: DependencyResolver | None = None,
) -> tuple[type[Any], OperatorRule]:
    """Build a registry entry for a node class, filling dependency behavior from progress."""
    return cls, OperatorRule(
        getattr(cls, "__name__", str(cls)),
        progress,
        state,
        transfer,
        dependencies or _dependencies_for(progress),
        requires_order,
    )


_SYNC_RULE_ITEMS = [
    _rule(MapOp),
    _rule(TapOp),
    _rule(EnumerateOp),
    _rule(
        ParallelMapOp,
        state=_state_bounded,
        transfer=lambda s, o: _facts(
            s,
            ordering=(OrderingGuarantee.ORDERED if o.ordered else OrderingGuarantee.UNORDERED),
        ),
    ),
    _rule(FilterOp, transfer=_filter),
    _rule(FilterMapOp, transfer=_filter),
    _rule(DropWhileOp, transfer=_filter),
    _rule(FlatMapOp, transfer=_flat_map),
    _rule(TakeOp, progress=ProgressKind.PREFIX_EMITTING, transfer=_take),
    _rule(TakeWhileOp, progress=ProgressKind.PREFIX_EMITTING, transfer=_take_while),
    _rule(
        TakeWhileInclusiveOp,
        progress=ProgressKind.PREFIX_EMITTING,
        transfer=_take_while,
    ),
    _rule(DropOp, transfer=_drop),
    _rule(UniqueOp, state=_state_unique, transfer=_filter),
    _rule(
        ChunkOp,
        progress=ProgressKind.PREFIX_EMITTING,
        state=_state_bounded,
        transfer=_chunk,
    ),
    _rule(
        WindowOp,
        progress=ProgressKind.PREFIX_EMITTING,
        state=_state_bounded,
        transfer=_window,
    ),
    _rule(
        WindowReduceOp,
        progress=ProgressKind.PREFIX_EMITTING,
        state=_window_reduce_state,
        transfer=_window,
        requires_order=True,
    ),
    _rule(GroupRunsOp, progress=ProgressKind.PREFIX_EMITTING, state=_state_input, transfer=_filter),
    _rule(PairwiseOp, progress=ProgressKind.PREFIX_EMITTING, transfer=_pairwise),
    _rule(ZipOp, transfer=_zip),
    _rule(ZipLongestOp, transfer=_zip_longest),
    _rule(IntersperseOp, transfer=_intersperse),
    _rule(ConcatOp, transfer=_concat),
    _rule(
        CrossOp,
        progress=ProgressKind.SIDE_INPUT_FINAL,
        state=_cross_state,
        transfer=_cross,
        dependencies=_cross_dependencies,
    ),
    _rule(ScanOp, progress=ProgressKind.PREFIX_EMITTING, requires_order=True),
    _rule(
        ScanRightOp,
        progress=ProgressKind.GLOBAL_FINAL,
        state=_state_input,
        requires_order=True,
    ),
    _rule(
        SortOp,
        progress=ProgressKind.GLOBAL_FINAL,
        state=lambda o: StateProfile.grows_with_input(spillable=o.buffer_size is not None),
        requires_order=True,
    ),
    _rule(
        GatherOp,
        state=_state_unknown,
        transfer=lambda s, _o: _facts(
            s, termination=TerminationEvidence.UNKNOWN, cardinality=Cardinality.unknown()
        ),
    ),
    _rule(PrependOp, transfer=_surround),
    _rule(AppendOp, progress=ProgressKind.PREFIX_EMITTING, transfer=_surround),
    _rule(MapFirstOp, progress=ProgressKind.PREFIX_EMITTING, requires_order=True),
    _rule(MapLastOp, progress=ProgressKind.PREFIX_EMITTING, requires_order=True),
    _rule(
        CollapseOp,
        progress=ProgressKind.PREFIX_EMITTING,
        state=_state_unknown,
        transfer=lambda s, _o: _facts(s, cardinality=Cardinality.unknown()),
        requires_order=True,
    ),
]
SYNC_OPERATOR_RULES: dict[type[Any], OperatorRule] = dict(_SYNC_RULE_ITEMS)


_ASYNC_NAMES: dict[type[Any], str] = {
    _MapAsync: "map_async",
    _Filter: "filter",
    _FilterMap: "filter_map",
    _Tap: "tap",
    _FlatMap: "flat_map",
    _Merge: "merge",
    _MergeMap: "merge_map",
    _SwitchMap: "switch_map",
    _CombineLatest: "combine_latest",
    _Timeout: "timeout",
    _Debounce: "debounce",
    _BufferTimeout: "buffer_timeout",
    _SessionWindow: "session_window",
    _Prefetch: "prefetch",
    _Delay: "delay",
    _Throttle: "throttle",
    _Take: "take",
    _Drop: "drop",
    _TakeWhile: "take_while",
    _TakeWhileInclusive: "take_while_inclusive",
    _DropWhile: "drop_while",
    _Chunk: "chunk",
    _BatchBySize: "batch_by_size",
    _Window: "window",
    _WindowReduce: "window_reduce",
    _Pairwise: "pairwise",
    _GroupRuns: "group_runs",
    _Fold: "fold",
    _Unique: "unique",
    _Enumerate: "enumerate",
    _Zip: "zip",
    _ZipLongest: "zip_longest",
    _Intersperse: "intersperse",
    _Concat: "concat",
    _Cross: "cross",
    _Scan: "scan",
    _ScanRight: "scan_right",
    _Prepend: "prepend",
    _Append: "append",
    _MapFirst: "map_first",
    _MapLast: "map_last",
    _Collapse: "collapse",
}


_ASYNC_RULE_ITEMS: list[tuple[type[Any], OperatorRule]] = []


def _async_map_transfer(source: StreamFacts, operation: Any) -> StreamFacts:
    """Preserve async-map facts except for ordered versus completion-order output."""
    return _facts(
        source,
        ordering=(OrderingGuarantee.ORDERED if operation.ordered else OrderingGuarantee.UNORDERED),
    )


def _bounded_sources(operation: Any) -> StateProfile:
    """Bound concurrent multi-source state by the primary source plus additional sources."""
    return StateProfile.bounded(len(operation.sources) + 1)


def _switch_transfer(source: StreamFacts, _operation: Any) -> StreamFacts:
    """Forget termination, cardinality, and ordering after switching among unknown inner streams."""
    return _facts(
        source,
        termination=TerminationEvidence.UNKNOWN,
        cardinality=Cardinality.unknown(),
        ordering=OrderingGuarantee.UNKNOWN,
    )


def _batch_state(operation: Any) -> StateProfile:
    """Use ``max_count`` as a batch memory bound, or report unknown state without that cap."""
    return (
        StateProfile.bounded(operation.max_count)
        if operation.max_count is not None
        else StateProfile.unknown()
    )


def _unknown_cardinality(source: StreamFacts, _operation: Any) -> StreamFacts:
    """Preserve all source facts except output cardinality, which becomes unknown."""
    return _facts(source, cardinality=Cardinality.unknown())


def _fold_cardinality(source: StreamFacts, _operation: Any) -> StreamFacts:
    return _facts(source, cardinality=Cardinality.upper_bound(1))


for _class, _name in _ASYNC_NAMES.items():
    _transfer: FactsTransfer = _preserve
    _progress = ProgressKind.PIPELINED
    _state: StateResolver = _state_stateless
    _requires_order = False
    if _class is _MapAsync:
        _state = _state_bounded
        _transfer = _async_map_transfer
    elif _class is _Prefetch:
        _state = _state_bounded
    elif _class is _SessionWindow:
        _progress = ProgressKind.PREFIX_EMITTING
        _state = _state_bounded
        _transfer = _unknown_cardinality
    elif _class in {_Filter, _FilterMap}:
        _transfer = _filter
    elif _class is _FlatMap:
        _transfer = _flat_map
    elif _class is _Merge:
        _transfer = _merge
        _state = _bounded_sources
        _requires_order = False
    elif _class is _MergeMap:
        _transfer = _flat_map
        _state = _state_bounded
    elif _class is _SwitchMap:
        _transfer = _switch_transfer
    elif _class is _CombineLatest:
        _transfer = _combine_latest
        _state = _bounded_sources
    elif _class in {
        _Take,
    }:
        _progress = ProgressKind.PREFIX_EMITTING
        _transfer = _take
    elif _class in {_TakeWhile, _TakeWhileInclusive}:
        _progress = ProgressKind.PREFIX_EMITTING
        _transfer = _take_while
    elif _class is _Drop:
        _transfer = _drop
    elif _class is _DropWhile:
        _transfer = _filter
    elif _class is _Chunk:
        _progress = ProgressKind.PREFIX_EMITTING
        _state = _state_bounded
        _transfer = _chunk
    elif _class is _BatchBySize:
        _progress = ProgressKind.PREFIX_EMITTING
        _state = _batch_state
        _transfer = _filter
    elif _class is _Window:
        _progress = ProgressKind.PREFIX_EMITTING
        _state = _state_bounded
        _transfer = _window
    elif _class is _WindowReduce:
        _progress = ProgressKind.PREFIX_EMITTING
        _state = _window_reduce_state
        _transfer = _window
        _requires_order = True
    elif _class is _Pairwise:
        _progress = ProgressKind.PREFIX_EMITTING
        _transfer = _pairwise
        _requires_order = True
    elif _class is _GroupRuns:
        _progress = ProgressKind.PREFIX_EMITTING
        _state = _state_input
        _transfer = _filter
        _requires_order = True
    elif _class is _Fold:
        _progress = ProgressKind.GLOBAL_FINAL
        _state = _state_unknown
        _transfer = _fold_cardinality
    elif _class in {_Prepend, _Append}:
        _transfer = _surround
    elif _class is _Intersperse:
        _transfer = _intersperse
    elif _class is _Unique:
        _state = _state_unique
        _transfer = _filter
    elif _class in {_Zip, _ZipLongest, _Concat, _Cross}:
        _transfer = {
            _Zip: _zip,
            _ZipLongest: _zip_longest,
            _Concat: _concat,
            _Cross: _cross,
        }[_class]
        if _class is _Cross:
            _progress = ProgressKind.SIDE_INPUT_FINAL
            _state = _cross_state
    elif _class is _ScanRight:
        _progress = ProgressKind.GLOBAL_FINAL
        _state = _state_input
        _requires_order = True
    elif _class is _Scan or _class in {_MapFirst, _MapLast}:
        _progress = ProgressKind.PREFIX_EMITTING
        _requires_order = True
    elif _class is _Collapse:
        _progress = ProgressKind.PREFIX_EMITTING
        _state = _state_unknown
        _transfer = _unknown_cardinality
        _requires_order = True
    _ASYNC_RULE_ITEMS.append(
        (
            _class,
            OperatorRule(
                _name,
                _progress,
                _state,
                _transfer,
                _cross_dependencies if _class is _Cross else _dependencies_for(_progress),
                _requires_order,
            ),
        )
    )

ASYNC_OPERATOR_RULES: dict[type[Any], OperatorRule] = dict(_ASYNC_RULE_ITEMS)


def _terminal_condition(terminal: str) -> CompletionCondition:
    """Map a terminal to the event that lets it finish or short-circuit."""
    if terminal in {"first", "any"}:
        return CompletionCondition.FIRST_ITEM_OR_SOURCE_END
    if terminal == "all":
        return CompletionCondition.WITNESS_OR_SOURCE_END
    if terminal == "iterate":
        return CompletionCondition.CONSUMER_STOP
    return CompletionCondition.SOURCE_END


def _analyse(
    source: StreamFacts,
    operations: tuple[object, ...],
    rules: dict[type[Any], OperatorRule],
) -> tuple[tuple[OperatorAnalysis, ...], StreamFacts]:
    """Apply exact-type rules in plan order and capture each input-to-output fact transition."""
    current = source
    analyses: list[OperatorAnalysis] = []
    for index, operation in enumerate(operations):
        rule = rules.get(type(operation))
        if rule is None:
            raise TypeError(f"no semantic rule registered for {type(operation).__name__}")
        output = rule.transfer(current, operation)
        analyses.append(
            OperatorAnalysis(
                index=index,
                name=str(getattr(operation, "name", rule.name)),
                input=current,
                output=output,
                progress=rule.progress,
                state=rule.state(operation),
                completion_dependencies=rule.completion_dependencies(current, operation),
                requires_order=rule.requires_order,
            )
        )
        current = output
    return tuple(analyses), current


def _diagnostics(
    operations: tuple[OperatorAnalysis, ...], output: StreamFacts, terminal: str
) -> tuple[PlanDiagnostic, ...]:
    """Report unordered inputs, potentially unbounded state, and unproven completion.

    Proven-infinite dependencies and source-ending terminals are errors; unknown termination is
    a warning because the analyzer cannot prove that evaluation completes.
    """
    diagnostics: list[PlanDiagnostic] = []
    for operation in operations:
        if operation.requires_order and operation.input.ordering.value == "unordered":
            diagnostics.append(
                PlanDiagnostic(
                    "ORDER_NOT_PRESERVED",
                    DiagnosticSeverity.WARNING,
                    "operator "
                    f"{operation.name} requires ordered input, but upstream ordering is unordered",
                    operation.index,
                )
            )
        state_input: StreamFacts | None = operation.input
        if operation.progress is ProgressKind.SIDE_INPUT_FINAL:
            state_input = (
                operation.completion_dependencies[0].facts
                if operation.completion_dependencies
                else None
            )
        if (
            operation.state.kind
            in {
                StateKind.GROWS_WITH_INPUT,
                StateKind.GROWS_WITH_KEYS,
                StateKind.UNKNOWN,
            }
            and state_input is not None
            and state_input.termination is not TerminationEvidence.PROVEN_FINITE
        ):
            diagnostics.append(
                PlanDiagnostic(
                    "STATE_MAY_GROW",
                    DiagnosticSeverity.WARNING,
                    f"operator {operation.name} may retain unbounded state until input completion",
                    operation.index,
                )
            )
        for dependency in operation.completion_dependencies:
            if dependency.condition is not CompletionCondition.SOURCE_END:
                continue
            if dependency.facts.termination is TerminationEvidence.PROVEN_INFINITE:
                diagnostics.append(
                    PlanDiagnostic(
                        "NON_TERMINATING_PLAN",
                        DiagnosticSeverity.ERROR,
                        "operator "
                        f"{operation.name} waits for {dependency.label}, which is proven infinite",
                        operation.index,
                    )
                )
            elif dependency.facts.termination is not TerminationEvidence.PROVEN_FINITE:
                diagnostics.append(
                    PlanDiagnostic(
                        "COMPLETION_NOT_PROVEN",
                        DiagnosticSeverity.WARNING,
                        "operator "
                        f"{operation.name} completion depends on an input with unknown termination",
                        operation.index,
                    )
                )
    condition = _terminal_condition(terminal)
    if condition is CompletionCondition.SOURCE_END:
        if output.termination is TerminationEvidence.PROVEN_INFINITE:
            diagnostics.append(
                PlanDiagnostic(
                    "NON_TERMINATING_PLAN",
                    DiagnosticSeverity.ERROR,
                    f"terminal {terminal} waits for source end",
                )
            )
        elif output.termination is not TerminationEvidence.PROVEN_FINITE:
            diagnostics.append(
                PlanDiagnostic(
                    "COMPLETION_NOT_PROVEN",
                    DiagnosticSeverity.WARNING,
                    f"terminal {terminal} completion is not proven",
                )
            )
    return tuple(diagnostics)


def analyze_sync_plan(plan: Pipeline, terminal: str = "iterate") -> PlanSemantics:
    """Analyze a synchronous plan with the sync rule registry and terminal diagnostics."""
    source = plan.source.current_facts()
    operations, output = _analyse(source, plan.operations, SYNC_OPERATOR_RULES)
    return PlanSemantics(
        source,
        operations,
        output,
        _terminal_condition(terminal),
        _diagnostics(operations, output, terminal),
    )


def analyze_async_plan(
    plan: AsyncLogicalPlan[object], terminal: AsyncTerminalName = "iterate"
) -> PlanSemantics:
    """Analyze an asynchronous plan with the async rule registry and terminal diagnostics."""
    source = plan.source.current_facts()
    operations, output = _analyse(source, plan.operations, ASYNC_OPERATOR_RULES)
    return PlanSemantics(
        source,
        operations,
        output,
        _terminal_condition(terminal),
        _diagnostics(operations, output, terminal),
    )
