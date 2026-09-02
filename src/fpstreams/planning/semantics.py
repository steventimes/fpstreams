"""Represent conservative stream semantics in immutable, serializable value objects."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Literal


class TerminationEvidence(StrEnum):
    """Express whether source exhaustion is proven finite, proven impossible, or unknown."""

    PROVEN_FINITE = "proven_finite"
    PROVEN_INFINITE = "proven_infinite"
    UNKNOWN = "unknown"


class CardinalityKind(StrEnum):
    """Distinguish exact output counts, upper bounds, and unavailable count information."""

    EXACT = "exact"
    UPPER_BOUND = "upper_bound"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class Cardinality:
    """Pair a cardinality evidence kind with its optional non-negative count or bound."""

    kind: CardinalityKind
    value: int | None = None

    def __post_init__(self) -> None:
        """Enforce that unknown counts have no value and known counts are non-negative."""
        if self.kind is CardinalityKind.UNKNOWN:
            if self.value is not None:
                raise ValueError("unknown cardinality cannot carry a value")
        elif self.value is None or self.value < 0:
            raise ValueError("cardinality bounds must be non-negative")

    @classmethod
    def exact(cls, value: int) -> Cardinality:
        """Construct evidence that output contains exactly ``value`` items."""
        return cls(CardinalityKind.EXACT, value)

    @classmethod
    def upper_bound(cls, value: int) -> Cardinality:
        """Construct evidence that output contains no more than ``value`` items."""
        return cls(CardinalityKind.UPPER_BOUND, value)

    @classmethod
    def unknown(cls) -> Cardinality:
        """Construct cardinality evidence with no known count or bound."""
        return cls(CardinalityKind.UNKNOWN)

    def to_dict(self) -> dict[str, Any]:
        """Serialize the cardinality kind and optional numeric value."""
        return {"kind": self.kind.value, "value": self.value}


class Replayability(StrEnum):
    """Describe whether evaluation is one-shot, factory-reopenable, or directly reiterable."""

    ONE_SHOT = "one_shot"
    REOPENABLE = "reopenable"
    REITERABLE = "reiterable"


class OrderingGuarantee(StrEnum):
    """Describe whether encounter order is preserved, absent, or not known."""

    ORDERED = "ordered"
    UNORDERED = "unordered"
    UNKNOWN = "unknown"


class ProgressKind(StrEnum):
    """Classify when an operator can emit relative to prefixes, segments, and input completion."""

    PIPELINED = "pipelined"
    PREFIX_EMITTING = "prefix_emitting"
    SEGMENT_FINAL = "segment_final"
    SIDE_INPUT_FINAL = "side_input_final"
    GLOBAL_FINAL = "global_final"


class StateKind(StrEnum):
    """Classify operator memory as fixed, explicitly bounded, input-growing, or unknown."""

    STATELESS = "stateless"
    CONSTANT = "constant"
    BOUNDED = "bounded"
    GROWS_WITH_KEYS = "grows_with_keys"
    GROWS_WITH_INPUT = "grows_with_input"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class StateProfile:
    """Describe an operator memory class, optional bound, and ability to spill."""

    kind: StateKind
    bound: int | None = None
    spillable: bool = False

    def __post_init__(self) -> None:
        """Require a non-negative bound only for ``BOUNDED`` state profiles."""
        if self.kind is StateKind.BOUNDED:
            if self.bound is None or self.bound < 0:
                raise ValueError("bounded state requires a non-negative bound")
        elif self.bound is not None:
            raise ValueError("only bounded state may carry a bound")

    @classmethod
    def stateless(cls) -> StateProfile:
        """Construct a profile for an operator that retains no stream state."""
        return cls(StateKind.STATELESS)

    @classmethod
    def constant(cls) -> StateProfile:
        """Construct a profile whose retained state does not scale with input."""
        return cls(StateKind.CONSTANT)

    @classmethod
    def bounded(cls, bound: int, *, spillable: bool = False) -> StateProfile:
        """Construct an explicitly bounded state profile with optional spill support."""
        return cls(StateKind.BOUNDED, bound, spillable)

    @classmethod
    def grows_with_keys(cls, *, spillable: bool = False) -> StateProfile:
        """Construct a profile whose state scales with the number of distinct keys."""
        return cls(StateKind.GROWS_WITH_KEYS, spillable=spillable)

    @classmethod
    def grows_with_input(cls, *, spillable: bool = False) -> StateProfile:
        """Construct a profile whose state may retain the full input."""
        return cls(StateKind.GROWS_WITH_INPUT, spillable=spillable)

    @classmethod
    def unknown(cls) -> StateProfile:
        """Construct a profile for an operator with an unspecified memory contract."""
        return cls(StateKind.UNKNOWN)

    def to_dict(self) -> dict[str, Any]:
        """Serialize state kind, numeric bound, and spillability."""
        return {
            "kind": self.kind.value,
            "bound": self.bound,
            "spillable": self.spillable,
        }


class CompletionCondition(StrEnum):
    """Identify the event a terminal or dependency must observe before it can complete."""

    CONSUMER_STOP = "consumer_stop"
    SOURCE_END = "source_end"
    FIRST_ITEM_OR_SOURCE_END = "first_item_or_source_end"
    WITNESS_OR_SOURCE_END = "witness_or_source_end"


class DiagnosticSeverity(StrEnum):
    """Rank semantic diagnostics as warnings or errors."""

    WARNING = "warning"
    ERROR = "error"


@dataclass(frozen=True, slots=True)
class StreamFacts:
    """Collect termination, cardinality, replayability, and ordering facts for one stream edge."""

    termination: TerminationEvidence
    cardinality: Cardinality
    replayability: Replayability
    ordering: OrderingGuarantee

    def to_dict(self) -> dict[str, Any]:
        """Serialize all facts attached to a stream edge."""
        return {
            "termination": self.termination.value,
            "cardinality": self.cardinality.to_dict(),
            "replayability": self.replayability.value,
            "ordering": self.ordering.value,
        }


@dataclass(frozen=True, slots=True)
class CompletionDependency:
    """Name an input and the event on that input required for an operator to complete."""

    label: str
    facts: StreamFacts
    condition: CompletionCondition = CompletionCondition.SOURCE_END

    def to_dict(self) -> dict[str, Any]:
        """Serialize a labeled dependency, its input facts, and completion condition."""
        return {
            "label": self.label,
            "facts": self.facts.to_dict(),
            "condition": self.condition.value,
        }


@dataclass(frozen=True, slots=True)
class OperatorAnalysis:
    """Capture one operation semantic transition, progress mode, state, and dependencies."""

    index: int
    name: str
    input: StreamFacts
    output: StreamFacts
    progress: ProgressKind
    state: StateProfile
    completion_dependencies: tuple[CompletionDependency, ...]
    requires_order: bool

    def to_dict(self) -> dict[str, Any]:
        """Serialize the complete semantic analysis of one operation node."""
        return {
            "index": self.index,
            "name": self.name,
            "input": self.input.to_dict(),
            "output": self.output.to_dict(),
            "progress": self.progress.value,
            "state": self.state.to_dict(),
            "completion_dependencies": [
                dependency.to_dict() for dependency in self.completion_dependencies
            ],
            "requires_order": self.requires_order,
        }


@dataclass(frozen=True, slots=True)
class PlanDiagnostic:
    """Report a coded semantic warning or error, optionally tied to an operation index."""

    code: str
    severity: DiagnosticSeverity
    message: str
    operation_index: int | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize a diagnostic for explain output or external tooling."""
        return {
            "code": self.code,
            "severity": self.severity.value,
            "message": self.message,
            "operation_index": self.operation_index,
        }


@dataclass(frozen=True, slots=True)
class PlanSemantics:
    """Aggregate source facts, per-operation transitions, output facts, and diagnostics."""

    source: StreamFacts
    operations: tuple[OperatorAnalysis, ...]
    output: StreamFacts
    completion: CompletionCondition
    diagnostics: tuple[PlanDiagnostic, ...]

    def to_dict(self, *, include_diagnostics: bool = True) -> dict[str, Any]:
        """Serialize plan semantics, optionally embedding the diagnostic list."""
        result: dict[str, Any] = {
            "source": self.source.to_dict(),
            "operations": [operation.to_dict() for operation in self.operations],
            "output": self.output.to_dict(),
            "completion": self.completion.value,
        }
        if include_diagnostics:
            result["diagnostics"] = [diagnostic.to_dict() for diagnostic in self.diagnostics]
        return result


AsyncTerminalName = Literal[
    "iterate",
    "list",
    "count",
    "sum",
    "min",
    "max",
    "minmax",
    "statistics",
    "aggregate",
    "first",
    "last",
    "any",
    "all",
]


def facts_from_capabilities(
    *,
    reiterable: bool,
    exact_size: int | None,
    ordered: bool,
    reopenable: bool = False,
) -> StreamFacts:
    """Convert concrete source capabilities into conservative planner facts.

    A known exact size proves finiteness. Replayability distinguishes reusable iterables from
    factories that reopen resources and from one-shot iterators.
    """
    termination = (
        TerminationEvidence.PROVEN_FINITE if exact_size is not None else TerminationEvidence.UNKNOWN
    )
    cardinality = Cardinality.exact(exact_size) if exact_size is not None else Cardinality.unknown()
    replayability = (
        Replayability.REITERABLE
        if reiterable and not reopenable
        else Replayability.REOPENABLE
        if reopenable
        else Replayability.ONE_SHOT
    )
    ordering_value = OrderingGuarantee.ORDERED if ordered else OrderingGuarantee.UNORDERED
    return StreamFacts(termination, cardinality, replayability, ordering_value)
