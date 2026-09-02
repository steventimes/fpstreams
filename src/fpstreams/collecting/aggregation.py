"""Build named single-pass aggregators and finalize fused native snapshots."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from types import CodeType, FunctionType, MappingProxyType
from typing import Any, Literal, cast

from ..expressions.selectors import Selector, compile_selector
from . import _collector_base
from ._collector_base import Aggregator as Aggregator
from .collector import (
    finish_collectors,
    initialize_collectors,
    step_collectors,
)
from .reducer import COUNT_LAWS, ReducerAggregator
from .statistics import OnlineStatistics, mean_from, std_from, validate_ddof, variance_from

_BUILTIN_GETATTR = getattr
_BUILTIN_LEN = len
_BUILTIN_TYPE = type
_BUILTIN_LIST_TYPE = list
_BUILTIN_STR_TYPE = str
_BUILTIN_STR_SPLIT = str.split

_MISSING = object()
_CANONICAL_MISSING = _MISSING
_CANONICAL_ONLINE_STATISTICS = OnlineStatistics
_CANONICAL_ONLINE_INIT = OnlineStatistics.__init__
_CANONICAL_ONLINE_INIT_CODE = OnlineStatistics.__init__.__code__
_CANONICAL_ONLINE_INIT_DEFAULTS = OnlineStatistics.__init__.__defaults__
_CANONICAL_ONLINE_INIT_KWDEFAULTS = OnlineStatistics.__init__.__kwdefaults__
_CANONICAL_ONLINE_NEW = OnlineStatistics.__new__
_CANONICAL_ONLINE_GETATTRIBUTE = OnlineStatistics.__getattribute__
_CANONICAL_ONLINE_SETATTR = OnlineStatistics.__setattr__
_CANONICAL_ONLINE_COUNT_DESCRIPTOR = OnlineStatistics.__dict__["count"]
_CANONICAL_ONLINE_TOTAL_DESCRIPTOR = OnlineStatistics.__dict__["total"]
_CANONICAL_ONLINE_COMPENSATION_DESCRIPTOR = OnlineStatistics.__dict__["compensation"]
_CANONICAL_ONLINE_ROLLING_MEAN_DESCRIPTOR = OnlineStatistics.__dict__["rolling_mean"]
_CANONICAL_ONLINE_SQUARED_DEVIATIONS_DESCRIPTOR = OnlineStatistics.__dict__["squared_deviations"]
_CANONICAL_MEAN_FROM = mean_from
_CANONICAL_MEAN_FROM_CODE = mean_from.__code__
_CANONICAL_STD_FROM = std_from
_CANONICAL_STD_FROM_CODE = std_from.__code__
_CANONICAL_VALIDATE_DDOF = validate_ddof
_CANONICAL_VALIDATE_DDOF_CODE = validate_ddof.__code__
_CANONICAL_VARIANCE_FROM = variance_from
_CANONICAL_VARIANCE_FROM_CODE = variance_from.__code__
_CANONICAL_ONLINE_ACCEPT = OnlineStatistics.accept
_CANONICAL_ONLINE_ACCEPT_CODE = OnlineStatistics.accept.__code__
_CANONICAL_ONLINE_SNAPSHOT = OnlineStatistics.snapshot
_CANONICAL_ONLINE_SNAPSHOT_CODE = OnlineStatistics.snapshot.__code__
_CANONICAL_ONLINE_MATH = _CANONICAL_ONLINE_ACCEPT.__globals__["math"]
_CANONICAL_ONLINE_ISFINITE = _BUILTIN_GETATTR(_CANONICAL_ONLINE_MATH, "isfinite")

NativeAggregationKind = Literal[
    "count", "sum", "min", "max", "first", "last", "mean", "variance", "std"
]
NativeGroupAggregationKind = Literal["sum", "min", "max", "first", "last", "mean"]
NativeAggregateSnapshot = tuple[
    int,
    int | float,
    int | float | None,
    int | float | None,
    int | float | None,
    int | float | None,
    float,
    float,
]


def _count_initializer() -> int:
    """Create the project-owned constant-size count identity."""
    return 0


def _count_step(count: int, _row: Any) -> int:
    """Advance the project-owned whole-row count state."""
    return count + 1


def _count_merge(left: int, right: int) -> int:
    """Merge two project-owned count states."""
    return left + right


class _ProjectCountAggregator(ReducerAggregator):
    """Closed collector type constructed only by the project count factory."""

    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(
            _count_initializer,
            _count_step,
            merge=_count_merge,
            laws=COUNT_LAWS,
            native=NativeAggregation("count"),
        )


def _identity(value: Any) -> Any:
    """Select an entire input row when an aggregation has no explicit selector."""
    return value


_CANONICAL_IDENTITY = _identity
_CANONICAL_IDENTITY_CODE = _identity.__code__


@dataclass(frozen=True, slots=True)
class NativeAggregation:
    """Identify a whole-value aggregation supported by the fused native snapshot.

    `ddof` is consulted only for variance and standard-deviation kinds.
    """

    kind: NativeAggregationKind
    ddof: int = 0


@dataclass(frozen=True, slots=True)
class _DirectFunctionProvenance:
    """Freeze one directly captured Python function without recursing further."""

    code: CodeType
    closure: tuple[Any, ...]
    globals: tuple[tuple[str, Any], ...]


@dataclass(frozen=True, slots=True)
class _FunctionProvenance:
    """Freeze one lifecycle function and its directly captured functions."""

    code: CodeType
    closure: tuple[Any, ...]
    globals: tuple[tuple[str, Any], ...]
    captured_functions: tuple[_DirectFunctionProvenance | None, ...]


@dataclass(frozen=True, slots=True)
class NativeGroupAggregation:
    """Retain one project-owned grouped aggregation and its exact lifecycle."""

    kind: NativeGroupAggregationKind
    selector: Selector | None
    initializer: Callable[[], Any]
    step: Callable[[Any, Any], Any]
    finish: Callable[[Any], Any]
    combine: Callable[[Any, Any], Any] | None
    done: Callable[[Any], bool]
    lifecycle_provenance: tuple[_FunctionProvenance | None, ...]


@dataclass(frozen=True, slots=True)
class _NativeAggregationProvenance:
    """Retain one project-created whole-value native aggregation lifecycle."""

    native: NativeAggregation
    kind: NativeAggregationKind
    ddof: int
    initializer: Callable[[], Any]
    step: Callable[[Any, Any], Any]
    finish: Callable[[Any], Any]
    combine: Callable[[Any, Any], Any] | None
    done: Callable[[Any], bool]
    lifecycle_provenance: tuple[_FunctionProvenance | None, ...]


_GROUP_AGGREGATION_MARKER = "_fpstreams_group_aggregation"
_GROUP_AGGREGATION_TOKEN = object()
_NATIVE_AGGREGATION_MARKER = "_fpstreams_native_aggregation"
_NATIVE_AGGREGATION_TOKEN = object()
_EMPTY_CLOSURE_CELL = object()
_MISSING_GLOBAL = object()

_GROUP_STEP_KINDS: MappingProxyType[CodeType, NativeGroupAggregationKind]
_CANONICAL_GROUP_STEP_KINDS: MappingProxyType[CodeType, NativeGroupAggregationKind]
_GENERATED_SELECTOR_KINDS: MappingProxyType[CodeType, Literal["index", "field", "path"]]
_CANONICAL_GENERATED_SELECTOR_KINDS: MappingProxyType[CodeType, Literal["index", "field", "path"]]


def _closure_values(function: Callable[..., Any]) -> tuple[Any, ...]:
    """Snapshot closure identities without invoking user equality protocols."""
    values: list[Any] = []
    for cell in function.__closure__ or ():
        try:
            values.append(cell.cell_contents)
        except ValueError:
            values.append(_EMPTY_CLOSURE_CELL)
    return tuple(values)


def _global_values(function: FunctionType) -> tuple[tuple[str, Any], ...]:
    """Snapshot effective identities for names that the function code may resolve globally."""
    namespace = function.__globals__
    builtins_namespace = cast(dict[str, Any], _BUILTIN_GETATTR(function, "__builtins__"))
    return tuple(
        (
            name,
            namespace[name] if name in namespace else builtins_namespace.get(name, _MISSING_GLOBAL),
        )
        for name in function.__code__.co_names
    )


def _matches_global_values(
    function: FunctionType,
    expected: tuple[tuple[str, Any], ...],
) -> bool:
    """Compare effective globals without invoking user equality protocols."""
    namespace = function.__globals__
    builtins_namespace = cast(dict[str, Any], _BUILTIN_GETATTR(function, "__builtins__"))
    for name, value in expected:
        live = (
            namespace[name] if name in namespace else builtins_namespace.get(name, _MISSING_GLOBAL)
        )
        if live is not value:
            return False
    return True


def _function_provenance(function: Any) -> _FunctionProvenance | None:
    """Snapshot one exact Python function and one level of captured functions."""
    if _BUILTIN_TYPE(function) is not FunctionType:
        return None
    closure = _closure_values(function)
    return _FunctionProvenance(
        function.__code__,
        closure,
        _global_values(function),
        tuple(
            _DirectFunctionProvenance(
                value.__code__,
                _closure_values(value),
                _global_values(value),
            )
            if _BUILTIN_TYPE(value) is FunctionType
            else None
            for value in closure
        ),
    )


def _lifecycle_provenance(
    lifecycle: tuple[Any, ...],
) -> tuple[_FunctionProvenance | None, ...]:
    """Snapshot the fixed collector lifecycle without traversing arbitrary objects."""
    return tuple(_function_provenance(function) for function in lifecycle)


def _matches_function_provenance(
    function: Any,
    provenance: _FunctionProvenance | None,
) -> bool:
    """Match shallow provenance without allocating closure snapshots on the hot path."""
    if provenance is None:
        return _BUILTIN_TYPE(function) is not FunctionType
    if (
        _BUILTIN_TYPE(function) is not FunctionType
        or function.__code__ is not provenance.code
        or not _matches_global_values(function, provenance.globals)
    ):
        return False
    cells = function.__closure__ or ()
    expected = provenance.closure
    if _BUILTIN_LEN(cells) != _BUILTIN_LEN(expected):
        return False
    index = 0
    while index < _BUILTIN_LEN(cells):
        try:
            value = cells[index].cell_contents
        except ValueError:
            value = _EMPTY_CLOSURE_CELL
        if value is not expected[index]:
            return False
        captured = provenance.captured_functions[index]
        if captured is not None:
            if (
                _BUILTIN_TYPE(value) is not FunctionType
                or value.__code__ is not captured.code
                or not _matches_global_values(value, captured.globals)
            ):
                return False
            captured_cells = value.__closure__ or ()
            captured_expected = captured.closure
            if _BUILTIN_LEN(captured_cells) != _BUILTIN_LEN(captured_expected):
                return False
            captured_index = 0
            while captured_index < _BUILTIN_LEN(captured_cells):
                try:
                    captured_value = captured_cells[captured_index].cell_contents
                except ValueError:
                    captured_value = _EMPTY_CLOSURE_CELL
                if captured_value is not captured_expected[captured_index]:
                    return False
                captured_index += 1
        index += 1
    return True


def _matches_lifecycle_provenance(
    lifecycle: tuple[Any, ...],
    provenance: tuple[_FunctionProvenance | None, ...],
) -> bool:
    """Match all five collector lifecycle slots against their factory snapshots."""
    if _BUILTIN_LEN(lifecycle) != _BUILTIN_LEN(provenance):
        return False
    index = 0
    while index < _BUILTIN_LEN(lifecycle):
        if not _matches_function_provenance(lifecycle[index], provenance[index]):
            return False
        index += 1
    return True


_CANONICAL_ONLINE_ACCEPT_PROVENANCE = _function_provenance(_CANONICAL_ONLINE_ACCEPT)
_CANONICAL_ONLINE_SNAPSHOT_PROVENANCE = _function_provenance(_CANONICAL_ONLINE_SNAPSHOT)
_CANONICAL_MEAN_FROM_PROVENANCE = _function_provenance(_CANONICAL_MEAN_FROM)


_PROJECT_COUNT_LIFECYCLE = (
    _count_initializer,
    _count_step,
    _collector_base._identity,
    _count_merge,
    _collector_base._never_done,
)
_PROJECT_COUNT_LIFECYCLE_CODES = tuple(function.__code__ for function in _PROJECT_COUNT_LIFECYCLE)


def _native_support_is_canonical(native: NativeAggregation) -> bool:
    """Require the project functions whose semantics a fused snapshot replaces."""
    if _identity is not _CANONICAL_IDENTITY or _identity.__code__ is not _CANONICAL_IDENTITY_CODE:
        return False
    if native.kind in {"min", "max", "first", "last"} and _MISSING is not _CANONICAL_MISSING:
        return False
    if native.kind not in {"mean", "variance", "std"}:
        return True
    return bool(
        OnlineStatistics is _CANONICAL_ONLINE_STATISTICS
        and OnlineStatistics.__dict__.get("__init__") is _CANONICAL_ONLINE_INIT
        and _CANONICAL_ONLINE_INIT.__code__ is _CANONICAL_ONLINE_INIT_CODE
        and _CANONICAL_ONLINE_INIT.__defaults__ is _CANONICAL_ONLINE_INIT_DEFAULTS
        and _CANONICAL_ONLINE_INIT.__kwdefaults__ is _CANONICAL_ONLINE_INIT_KWDEFAULTS
        and OnlineStatistics.__new__ is _CANONICAL_ONLINE_NEW
        and OnlineStatistics.__getattribute__ is _CANONICAL_ONLINE_GETATTRIBUTE
        and OnlineStatistics.__setattr__ is _CANONICAL_ONLINE_SETATTR
        and OnlineStatistics.__dict__.get("count") is _CANONICAL_ONLINE_COUNT_DESCRIPTOR
        and OnlineStatistics.__dict__.get("total") is _CANONICAL_ONLINE_TOTAL_DESCRIPTOR
        and OnlineStatistics.__dict__.get("compensation")
        is _CANONICAL_ONLINE_COMPENSATION_DESCRIPTOR
        and OnlineStatistics.__dict__.get("rolling_mean")
        is _CANONICAL_ONLINE_ROLLING_MEAN_DESCRIPTOR
        and OnlineStatistics.__dict__.get("squared_deviations")
        is _CANONICAL_ONLINE_SQUARED_DEVIATIONS_DESCRIPTOR
        and OnlineStatistics.__dict__.get("accept") is _CANONICAL_ONLINE_ACCEPT
        and _CANONICAL_ONLINE_ACCEPT.__code__ is _CANONICAL_ONLINE_ACCEPT_CODE
        and _matches_function_provenance(
            _CANONICAL_ONLINE_ACCEPT,
            _CANONICAL_ONLINE_ACCEPT_PROVENANCE,
        )
        and _BUILTIN_GETATTR(_CANONICAL_ONLINE_MATH, "isfinite", None) is _CANONICAL_ONLINE_ISFINITE
        and OnlineStatistics.__dict__.get("snapshot") is _CANONICAL_ONLINE_SNAPSHOT
        and _CANONICAL_ONLINE_SNAPSHOT.__code__ is _CANONICAL_ONLINE_SNAPSHOT_CODE
        and _matches_function_provenance(
            _CANONICAL_ONLINE_SNAPSHOT,
            _CANONICAL_ONLINE_SNAPSHOT_PROVENANCE,
        )
        and mean_from is _CANONICAL_MEAN_FROM
        and _CANONICAL_MEAN_FROM.__code__ is _CANONICAL_MEAN_FROM_CODE
        and _matches_function_provenance(
            _CANONICAL_MEAN_FROM,
            _CANONICAL_MEAN_FROM_PROVENANCE,
        )
        and std_from is _CANONICAL_STD_FROM
        and _CANONICAL_STD_FROM.__code__ is _CANONICAL_STD_FROM_CODE
        and validate_ddof is _CANONICAL_VALIDATE_DDOF
        and _CANONICAL_VALIDATE_DDOF.__code__ is _CANONICAL_VALIDATE_DDOF_CODE
        and variance_from is _CANONICAL_VARIANCE_FROM
        and _CANONICAL_VARIANCE_FROM.__code__ is _CANONICAL_VARIANCE_FROM_CODE
    )


def _mark_group_aggregation(
    aggregation: Aggregator,
    kind: Literal["sum", "min", "max", "first", "last", "mean"],
    selector: Selector | None,
) -> Aggregator:
    """Brand an immutable factory result with its original lifecycle identities."""
    if (
        selector is None
        and (
            _identity is not _CANONICAL_IDENTITY
            or _identity.__code__ is not _CANONICAL_IDENTITY_CODE
        )
    ) or (kind in {"min", "max", "first", "last"} and _MISSING is not _CANONICAL_MISSING):
        return aggregation
    if kind == "mean" and not _native_support_is_canonical(NativeAggregation("mean")):
        return aggregation
    lifecycle = (
        aggregation.initializer,
        aggregation.step,
        aggregation.finish,
        aggregation.combine,
        aggregation.done,
    )
    hint = NativeGroupAggregation(
        kind,
        selector,
        aggregation.initializer,
        aggregation.step,
        aggregation.finish,
        aggregation.combine,
        aggregation.done,
        _lifecycle_provenance(lifecycle),
    )
    aggregation.step.__dict__[_GROUP_AGGREGATION_MARKER] = (
        _GROUP_AGGREGATION_TOKEN,
        hint,
    )
    return _mark_native_aggregation(aggregation)


def _mark_native_aggregation(aggregation: Aggregator) -> Aggregator:
    """Brand one canonical whole-value factory result for fused linear execution."""
    native = aggregation.native
    if _BUILTIN_TYPE(native) is not NativeAggregation or not _native_support_is_canonical(native):
        return aggregation
    lifecycle = (
        aggregation.initializer,
        aggregation.step,
        aggregation.finish,
        aggregation.combine,
        aggregation.done,
    )
    hint = _NativeAggregationProvenance(
        native,
        native.kind,
        native.ddof,
        aggregation.initializer,
        aggregation.step,
        aggregation.finish,
        aggregation.combine,
        aggregation.done,
        _lifecycle_provenance(lifecycle),
    )
    aggregation.step.__dict__[_NATIVE_AGGREGATION_MARKER] = (
        _NATIVE_AGGREGATION_TOKEN,
        hint,
    )
    return aggregation


def _path_parts_match_selector(parts: Any, selector: Any) -> bool:
    """Compare a generated dotted-path closure using exact built-in strings only."""
    if (
        _BUILTIN_TYPE(selector) is not _BUILTIN_STR_TYPE
        or _BUILTIN_TYPE(parts) is not _BUILTIN_LIST_TYPE
    ):
        return False
    expected_parts = _BUILTIN_STR_SPLIT(selector, ".")
    if _BUILTIN_LEN(parts) != _BUILTIN_LEN(expected_parts):
        return False
    index = 0
    while index < _BUILTIN_LEN(expected_parts):
        part = parts[index]
        expected = expected_parts[index]
        if _BUILTIN_TYPE(part) is not _BUILTIN_STR_TYPE or part != expected:
            return False
        index += 1
    return True


_CANONICAL_PATH_PARTS_MATCH_SELECTOR = _path_parts_match_selector
_CANONICAL_PATH_PARTS_MATCH_SELECTOR_CODE = _path_parts_match_selector.__code__


def _generated_selector_matches_hint(
    select: FunctionType,
    selector: Any,
    generated_kind: Literal["index", "field", "path"],
) -> bool:
    """Bind one canonical generated selector closure to its retained public token."""
    if (
        _path_parts_match_selector is not _CANONICAL_PATH_PARTS_MATCH_SELECTOR
        or _CANONICAL_PATH_PARTS_MATCH_SELECTOR.__code__
        is not _CANONICAL_PATH_PARTS_MATCH_SELECTOR_CODE
    ):
        return False
    select_cells = select.__closure__ or ()
    freevars = select.__code__.co_freevars
    if _BUILTIN_LEN(select_cells) != _BUILTIN_LEN(freevars):
        return False

    captured_selector: Any = _EMPTY_CLOSURE_CELL
    captured_parts: Any = _EMPTY_CLOSURE_CELL
    index = 0
    while index < _BUILTIN_LEN(freevars):
        try:
            value = select_cells[index].cell_contents
        except ValueError:
            return False
        name = freevars[index]
        if name == "selector":
            captured_selector = value
        elif name == "parts":
            captured_parts = value
        index += 1
    if captured_selector is not selector:
        return False
    if generated_kind != "path":
        return captured_parts is _EMPTY_CLOSURE_CELL
    return _CANONICAL_PATH_PARTS_MATCH_SELECTOR(captured_parts, selector)


_CANONICAL_GENERATED_SELECTOR_MATCHES_HINT = _generated_selector_matches_hint
_CANONICAL_GENERATED_SELECTOR_MATCHES_HINT_CODE = _generated_selector_matches_hint.__code__


def _group_selector_matches_step(
    step: Callable[[Any, Any], Any],
    hint: NativeGroupAggregation,
) -> bool:
    """Bind planner selector metadata to the canonical selector captured by ``step``."""
    if (
        _GENERATED_SELECTOR_KINDS is not _CANONICAL_GENERATED_SELECTOR_KINDS
        or _generated_selector_matches_hint is not _CANONICAL_GENERATED_SELECTOR_MATCHES_HINT
        or _CANONICAL_GENERATED_SELECTOR_MATCHES_HINT.__code__
        is not _CANONICAL_GENERATED_SELECTOR_MATCHES_HINT_CODE
        or _BUILTIN_TYPE(step) is not FunctionType
        or step.__code__.co_freevars != ("select",)
    ):
        return False
    cells = step.__closure__ or ()
    if _BUILTIN_LEN(cells) != 1:
        return False
    try:
        select = cells[0].cell_contents
    except ValueError:
        return False

    selector = hint.selector
    if selector is None:
        return select is _CANONICAL_IDENTITY and _identity is _CANONICAL_IDENTITY
    if _BUILTIN_TYPE(select) is FunctionType:
        generated_kind = _GENERATED_SELECTOR_KINDS.get(select.__code__)
        if generated_kind is not None:
            return _CANONICAL_GENERATED_SELECTOR_MATCHES_HINT(
                select,
                selector,
                generated_kind,
            )
    return select is selector


_CANONICAL_GROUP_SELECTOR_MATCHES_STEP = _group_selector_matches_step
_CANONICAL_GROUP_SELECTOR_MATCHES_STEP_CODE = _group_selector_matches_step.__code__


def native_group_aggregation(aggregation: Aggregator) -> NativeGroupAggregation | None:
    """Recognize a factory result without trusting user-constructible native metadata."""
    if (
        _GROUP_STEP_KINDS is not _CANONICAL_GROUP_STEP_KINDS
        or _GENERATED_SELECTOR_KINDS is not _CANONICAL_GENERATED_SELECTOR_KINDS
        or _group_selector_matches_step is not _CANONICAL_GROUP_SELECTOR_MATCHES_STEP
        or _CANONICAL_GROUP_SELECTOR_MATCHES_STEP.__code__
        is not _CANONICAL_GROUP_SELECTOR_MATCHES_STEP_CODE
        or _BUILTIN_TYPE(aggregation) is not Aggregator
        or _BUILTIN_TYPE(aggregation.step) is not FunctionType
    ):
        return None
    expected_kind = _GROUP_STEP_KINDS.get(aggregation.step.__code__)
    marker = _BUILTIN_GETATTR(aggregation.step, _GROUP_AGGREGATION_MARKER, None)
    if (
        expected_kind is None
        or _BUILTIN_TYPE(marker) is not tuple
        or _BUILTIN_LEN(marker) != 2
        or marker[0] is not _GROUP_AGGREGATION_TOKEN
        or _BUILTIN_TYPE(marker[1]) is not NativeGroupAggregation
    ):
        return None
    hint = marker[1]
    native = aggregation.native
    if hint.selector is None:
        if (
            _BUILTIN_TYPE(native) is not NativeAggregation
            or native.kind is not expected_kind
            or _BUILTIN_TYPE(native.ddof) is not int
            or native.ddof != 0
        ):
            return None
    elif native is not None:
        return None
    lifecycle = (
        aggregation.initializer,
        aggregation.step,
        aggregation.finish,
        aggregation.combine,
        aggregation.done,
    )
    return (
        hint
        if aggregation.initializer is hint.initializer
        and aggregation.step is hint.step
        and aggregation.finish is hint.finish
        and aggregation.combine is hint.combine
        and aggregation.done is hint.done
        and hint.kind is expected_kind
        and _CANONICAL_GROUP_SELECTOR_MATCHES_STEP(aggregation.step, hint)
        and _native_support_is_canonical(NativeAggregation(expected_kind))
        and _matches_lifecycle_provenance(
            lifecycle,
            hint.lifecycle_provenance,
        )
        else None
    )


def project_count_aggregation(aggregation: Aggregator) -> bool:
    """Recognize only the constant-size count reducer built by :meth:`agg.count`.

    Native metadata and reducer laws are user-constructible, so neither is enough
    to authorize a count-specific state merge.  The private lifecycle identities
    close that gap while the singleton laws prove constant, project-owned state.
    """
    native = _BUILTIN_GETATTR(aggregation, "native", None)
    return (
        _BUILTIN_TYPE(aggregation) is _ProjectCountAggregator
        and aggregation.initializer is _count_initializer
        and _count_initializer.__code__ is _PROJECT_COUNT_LIFECYCLE_CODES[0]
        and aggregation.step is _count_step
        and _count_step.__code__ is _PROJECT_COUNT_LIFECYCLE_CODES[1]
        and aggregation.finish is _collector_base._identity
        and _collector_base._identity.__code__ is _PROJECT_COUNT_LIFECYCLE_CODES[2]
        and aggregation.combine is _count_merge
        and _count_merge.__code__ is _PROJECT_COUNT_LIFECYCLE_CODES[3]
        and aggregation.done is _collector_base._never_done
        and _collector_base._never_done.__code__ is _PROJECT_COUNT_LIFECYCLE_CODES[4]
        and _BUILTIN_GETATTR(aggregation, "laws", None) is COUNT_LAWS
        and _BUILTIN_TYPE(native) is NativeAggregation
        and native.kind == "count"
        and native.ddof == 0
    )


def native_aggregation_is_live(aggregation: Aggregator) -> bool:
    """Recognize an unchanged project-created whole-value native aggregation."""
    if project_count_aggregation(aggregation):
        return True
    marker = _BUILTIN_GETATTR(aggregation.step, _NATIVE_AGGREGATION_MARKER, None)
    if (
        _BUILTIN_TYPE(marker) is not tuple
        or _BUILTIN_LEN(marker) != 2
        or marker[0] is not _NATIVE_AGGREGATION_TOKEN
        or _BUILTIN_TYPE(marker[1]) is not _NativeAggregationProvenance
        or _BUILTIN_TYPE(aggregation) is not Aggregator
    ):
        return False
    hint = marker[1]
    lifecycle = (
        aggregation.initializer,
        aggregation.step,
        aggregation.finish,
        aggregation.combine,
        aggregation.done,
    )
    return bool(
        aggregation.native is hint.native
        and _BUILTIN_TYPE(hint.native.kind) is str
        and _BUILTIN_TYPE(hint.native.ddof) is int
        and hint.native.kind == hint.kind
        and hint.native.ddof == hint.ddof
        and _native_support_is_canonical(hint.native)
        and aggregation.initializer is hint.initializer
        and aggregation.step is hint.step
        and aggregation.finish is hint.finish
        and aggregation.combine is hint.combine
        and aggregation.done is hint.done
        and _matches_lifecycle_provenance(lifecycle, hint.lifecycle_provenance)
    )


class _DistinctState:
    """Track distinct hashable values in a set and unhashable values in a list."""

    __slots__ = ("hashable", "unhashable")

    def __init__(self) -> None:
        """Initialize empty storage for both hashability categories."""
        self.hashable: set[Any] = set()
        self.unhashable: list[Any] = []

    def add(self, value: Any) -> None:
        """Add a value once, falling back to equality-based list membership if unhashable."""
        try:
            self.hashable.add(value)
        except TypeError:
            if value not in self.unhashable:
                self.unhashable.append(value)

    def count(self) -> int:
        """Return the combined number of hashable and unhashable distinct values."""
        return len(self.hashable) + len(self.unhashable)


AggregationItems = tuple[tuple[str, Aggregator], ...]


def prepare_aggregations(aggregations: Mapping[str, Aggregator]) -> AggregationItems:
    """Validate named aggregators and freeze mapping order as `(name, aggregator)` pairs.

    The mapping must be nonempty, names must be truthy, and every value must be an
    :class:`Aggregator` rather than an arbitrary collector.
    """
    if not aggregations:
        raise ValueError("aggregate requires at least one named aggregation")
    items = tuple(aggregations.items())
    for name, aggregation in items:
        if not name:
            raise ValueError("aggregate names cannot be empty")
        if not isinstance(aggregation, Aggregator):
            raise TypeError(f"aggregate {name!r} must be an Aggregator")
    return items


def initialize_aggregations(items: AggregationItems) -> dict[str, Any]:
    """Initialize one independent state per named aggregator."""
    return initialize_collectors(items)


def step_aggregations(states: dict[str, Any], items: AggregationItems, value: Any) -> None:
    """Offer one input value to every named aggregator that is not complete."""
    step_collectors(states, items, value)


def finish_aggregations(states: Mapping[str, Any], items: AggregationItems) -> dict[str, Any]:
    """Finish named aggregation states into an insertion-ordered result dictionary."""
    return finish_collectors(states, items)


def run_aggregations(values: Iterable[Any], items: AggregationItems) -> dict[str, Any]:
    """Run the canonical compiled aggregation program in one traversal."""
    from .program import compile_collectors, run_collector_program

    return run_collector_program(values, compile_collectors(items))


def native_aggregation_items(items: AggregationItems) -> bool:
    """Return whether every aggregator carries fused-native metadata."""
    return all(isinstance(aggregation.native, NativeAggregation) for _name, aggregation in items)


def native_first_only(items: AggregationItems) -> str | None:
    """Return the result name for exactly one native `first` aggregation, else `None`."""
    if len(items) != 1:
        return None
    name, aggregation = items[0]
    native = aggregation.native
    return name if isinstance(native, NativeAggregation) and native.kind == "first" else None


def finish_native_aggregations(
    items: AggregationItems, snapshot: NativeAggregateSnapshot
) -> dict[str, Any]:
    """Project one fused native snapshot into each named aggregation result.

    Count, sum, extrema, endpoints, and mean are read directly from the snapshot. Variance and
    standard deviation use each aggregator's `ddof`. Missing native metadata raises
    `RuntimeError` because the caller selected a native-only finalization path.
    """
    count, total, minimum, maximum, first, last, mean, squared_deviations = snapshot
    statistics = (count, mean, squared_deviations)
    values: dict[NativeAggregationKind, Any] = {
        "count": count,
        "sum": total,
        "min": minimum,
        "max": maximum,
        "first": first,
        "last": last,
        "mean": mean_from(statistics),
    }
    result: dict[str, Any] = {}
    for name, aggregation in items:
        native = aggregation.native
        if not isinstance(native, NativeAggregation):
            raise RuntimeError("native aggregation metadata is missing")
        if native.kind == "variance":
            result[name] = variance_from(statistics, native.ddof)
        elif native.kind == "std":
            result[name] = std_from(statistics, native.ddof)
        else:
            result[name] = values[native.kind]
    return result


class _AggFactory:
    """Construct built-in aggregators for `Flow.aggregate` and tabular grouping."""

    __slots__ = ()

    def count(self) -> Aggregator:
        """Build a mergeable whole-value counter with native metadata.

        Returns:
            An aggregator returning zero for empty input.
        """
        return _ProjectCountAggregator()

    def count_where(self, predicate: Selector) -> Aggregator:
        """Build an aggregator that counts truthy selector results.

        Args:
            predicate: Selector evaluated once per input item and converted to `bool`.

        Returns:
            An aggregator returning zero for empty input.
        """
        test = compile_selector(predicate)
        return Aggregator(
            lambda: 0,
            lambda count, row: count + bool(test(row)),
        )

    def any(self, predicate: Selector | None = None) -> Aggregator:
        """Build an aggregator that stops after the first truthy selected value.

        With no predicate, each whole item is tested for truth. Empty input returns `False`.

        Args:
            predicate: Optional selector whose result is converted to `bool`.

        Returns:
            An OR-combinable, early-stopping Boolean aggregator.
        """
        test = bool if predicate is None else compile_selector(predicate)
        return Aggregator(
            lambda: False,
            lambda matched, row: matched or bool(test(row)),
            combine=lambda left, right: left or right,
            done=bool,
        )

    def all(self, predicate: Selector | None = None) -> Aggregator:
        """Build an aggregator that stops after the first false selected value.

        With no predicate, each whole item is tested for truth. Empty input returns `True`.

        Args:
            predicate: Optional selector whose result is converted to `bool`.

        Returns:
            An AND-combinable, early-stopping Boolean aggregator.
        """
        test = bool if predicate is None else compile_selector(predicate)
        return Aggregator(
            lambda: True,
            lambda matched, row: matched and bool(test(row)),
            combine=lambda left, right: left and right,
            done=lambda matched: not matched,
        )

    def sum(self, selector: Selector | None = None) -> Aggregator:
        """Build an aggregator that adds selected values from a zero identity.

        Whole-value sums carry native metadata; selected sums execute through Python selector
        evaluation. Empty input returns zero.

        Args:
            selector: Value selector; `None` adds each whole input item.

        Returns:
            An addition-combinable aggregator.
        """
        select = _identity if selector is None else compile_selector(selector)

        def step(total: Any, row: Any) -> Any:
            """Add one selected value to the current total."""
            return total + select(row)

        return _mark_group_aggregation(
            Aggregator(
                lambda: 0,
                step,
                combine=lambda left, right: left + right,
                native=NativeAggregation("sum") if selector is None else None,
            ),
            "sum",
            selector,
        )

    def mean(self, selector: Selector | None = None) -> Aggregator:
        """Build an aggregator for a compensated one-pass arithmetic mean.

        Selected values must be real numbers accepted by :class:`OnlineStatistics`. Empty
        input returns `None`; whole-value means carry native metadata.

        Args:
            selector: Value selector; `None` averages each whole input item.

        Returns:
            An aggregator finishing online statistics as `float` or `None`.
        """
        select = _identity if selector is None else compile_selector(selector)

        def step(state: OnlineStatistics, row: Any) -> OnlineStatistics:
            """Select one real value and update the mutable online-statistics state."""
            state.accept(select(row))
            return state

        return _mark_group_aggregation(
            Aggregator(
                OnlineStatistics,
                step,
                lambda state: mean_from(state.snapshot()),
                native=NativeAggregation("mean") if selector is None else None,
            ),
            "mean",
            selector,
        )

    def variance(self, selector: Selector | None = None, *, ddof: int = 1) -> Aggregator:
        """Build an aggregator for one-pass variance with divisor `count - ddof`.

        The default computes sample variance. Results are `None` when `count <= ddof`.

        Args:
            selector: Value selector; `None` consumes each whole input item.
            ddof: Non-negative delta degrees of freedom.

        Returns:
            An online-statistics aggregator returning variance or `None`.

        Raises:
            ValueError: If `ddof` is negative.
        """
        return self._variance(selector, ddof=ddof, square_root=False)

    def std(self, selector: Selector | None = None, *, ddof: int = 1) -> Aggregator:
        """Build an aggregator for the square root of one-pass variance.

        The default computes sample standard deviation. Results are `None` when
        `count <= ddof`.

        Args:
            selector: Value selector; `None` consumes each whole input item.
            ddof: Non-negative delta degrees of freedom.

        Returns:
            An online-statistics aggregator returning standard deviation or `None`.

        Raises:
            ValueError: If `ddof` is negative.
        """
        return self._variance(selector, ddof=ddof, square_root=True)

    def _variance(self, selector: Selector | None, *, ddof: int, square_root: bool) -> Aggregator:
        """Construct the shared online state machine for variance or standard deviation."""
        if ddof < 0:
            raise ValueError("ddof must be non-negative")
        select = _identity if selector is None else compile_selector(selector)

        def step(state: OnlineStatistics, row: Any) -> OnlineStatistics:
            """Select one real value and update the mutable online-statistics state."""
            state.accept(select(row))
            return state

        def finish(state: OnlineStatistics) -> float | None:
            """Finish the state as variance or its square root under the captured `ddof`."""
            snapshot = state.snapshot()
            return std_from(snapshot, ddof) if square_root else variance_from(snapshot, ddof)

        kind: Literal["std", "variance"] = "std" if square_root else "variance"
        return _mark_native_aggregation(
            Aggregator(
                OnlineStatistics,
                step,
                finish,
                native=NativeAggregation(kind, ddof) if selector is None else None,
            )
        )

    def count_distinct(self, selector: Selector | None = None) -> Aggregator:
        """Build an aggregator that counts distinct selected values.

        Hashable values use set semantics. Unhashable values are retained once according to
        equality-based list membership. Empty input returns zero.

        Args:
            selector: Value selector; `None` compares each whole input item.

        Returns:
            An aggregator backed by separate hashable and unhashable state.
        """
        select = _identity if selector is None else compile_selector(selector)

        def step(state: _DistinctState, row: Any) -> _DistinctState:
            """Select one value, add it if distinct, and preserve the mutable state."""
            state.add(select(row))
            return state

        return Aggregator(_DistinctState, step, _DistinctState.count)

    def min(self, selector: Selector | None = None) -> Aggregator:
        """Build an aggregator that retains the first smallest selected value.

        Values are compared with `<`; equal values keep the earlier representative. Empty
        input returns `None`, and whole-value minima carry native metadata.

        Args:
            selector: Value selector; `None` compares each whole input item.

        Returns:
            An aggregator returning the minimum selected value or `None`.
        """
        select = _identity if selector is None else compile_selector(selector)

        def step(current: Any, row: Any) -> Any:
            """Replace missing or larger state with the newly selected value."""
            value = select(row)
            return value if current is _MISSING or value < current else current

        return _mark_group_aggregation(
            Aggregator(
                lambda: _MISSING,
                step,
                lambda value: None if value is _MISSING else value,
                native=NativeAggregation("min") if selector is None else None,
            ),
            "min",
            selector,
        )

    def max(self, selector: Selector | None = None) -> Aggregator:
        """Build an aggregator that retains the first largest selected value.

        Values are compared with `>`; equal values keep the earlier representative. Empty
        input returns `None`, and whole-value maxima carry native metadata.

        Args:
            selector: Value selector; `None` compares each whole input item.

        Returns:
            An aggregator returning the maximum selected value or `None`.
        """
        select = _identity if selector is None else compile_selector(selector)

        def step(current: Any, row: Any) -> Any:
            """Replace missing or smaller state with the newly selected value."""
            value = select(row)
            return value if current is _MISSING or value > current else current

        return _mark_group_aggregation(
            Aggregator(
                lambda: _MISSING,
                step,
                lambda value: None if value is _MISSING else value,
                native=NativeAggregation("max") if selector is None else None,
            ),
            "max",
            selector,
        )

    def first(self, selector: Selector | None = None) -> Aggregator:
        """Build an aggregator that stops after the first selected value.

        Empty input returns `None`. A selected `None` is still a completed first value, and a
        whole-value aggregation carries native short-circuit metadata.

        Args:
            selector: Value selector; `None` returns the whole first item.

        Returns:
            An aggregator consuming at most one item.
        """
        select = _identity if selector is None else compile_selector(selector)

        def step(current: Any, row: Any) -> Any:
            """Select only while the project-owned missing state remains."""
            return select(row) if current is _MISSING else current

        return _mark_group_aggregation(
            Aggregator(
                lambda: _MISSING,
                step,
                lambda value: None if value is _MISSING else value,
                done=lambda value: value is not _MISSING,
                native=NativeAggregation("first") if selector is None else None,
            ),
            "first",
            selector,
        )

    def last(self, selector: Selector | None = None) -> Aggregator:
        """Build an aggregator that consumes all input and retains its last selected value.

        Empty input returns `None`; whole-value aggregation carries native metadata.

        Args:
            selector: Value selector; `None` returns the whole last item.

        Returns:
            An aggregator retaining one current value.
        """
        select = _identity if selector is None else compile_selector(selector)

        def step(_current: Any, row: Any) -> Any:
            """Replace the current value with the newly selected value."""
            return select(row)

        return _mark_group_aggregation(
            Aggregator(
                lambda: _MISSING,
                step,
                lambda value: None if value is _MISSING else value,
                native=NativeAggregation("last") if selector is None else None,
            ),
            "last",
            selector,
        )

    def collect(
        self, selector: Selector | None = None, *, into: Callable[[Any], Any] = list
    ) -> Aggregator:
        """Build an aggregator that buffers selected values and calls `into` at finish.

        `list` and `tuple` finishers produce lawful mergeable reducer aggregators. Other
        callable finishers still receive the full encounter-ordered list but do not declare a
        state merger. This factory does not execute a flow immediately.

        Args:
            selector: Value selector; `None` collects each whole input item.
            into: Callable invoked once with the accumulated list.

        Returns:
            An aggregator returning `into(selected_values)`.

        Raises:
            TypeError: If `into` is not callable.
        """
        select = _identity if selector is None else compile_selector(selector)

        def append(values: list[Any], row: Any) -> list[Any]:
            """Append one selected value and preserve the mutable list state."""
            values.append(select(row))
            return values

        from .reducer import LIST_LAWS, ReducerAggregator

        if into in (list, tuple):
            return ReducerAggregator(
                list,
                append,
                into,
                merge=lambda left, right: left.extend(right) or left,
                laws=LIST_LAWS,
            )
        return Aggregator(
            list,
            append,
            into,
        )


def _nested_code(function: Callable[..., Any], name: str) -> CodeType:
    """Resolve one compiler-owned nested function code object during module setup."""
    owner = cast(FunctionType, function)
    for constant in owner.__code__.co_consts:
        if _BUILTIN_TYPE(constant) is CodeType and constant.co_name == name:
            return constant
    raise RuntimeError(f"missing nested code {name!r} in {owner.__qualname__}")


_group_step_kinds: dict[CodeType, NativeGroupAggregationKind] = {
    _nested_code(_AggFactory.sum, "step"): "sum",
    _nested_code(_AggFactory.min, "step"): "min",
    _nested_code(_AggFactory.max, "step"): "max",
    _nested_code(_AggFactory.first, "step"): "first",
    _nested_code(_AggFactory.last, "step"): "last",
    _nested_code(_AggFactory.mean, "step"): "mean",
}
_GROUP_STEP_KINDS = MappingProxyType(_group_step_kinds)
_CANONICAL_GROUP_STEP_KINDS = _GROUP_STEP_KINDS
del _group_step_kinds

_generated_selector_kinds: dict[CodeType, Literal["index", "field", "path"]] = {
    _nested_code(compile_selector, "select_index"): "index",
    _nested_code(compile_selector, "select_field"): "field",
    _nested_code(compile_selector, "select_path"): "path",
}
_GENERATED_SELECTOR_KINDS = MappingProxyType(_generated_selector_kinds)
_CANONICAL_GENERATED_SELECTOR_KINDS = _GENERATED_SELECTOR_KINDS
del _generated_selector_kinds


agg = _AggFactory()
