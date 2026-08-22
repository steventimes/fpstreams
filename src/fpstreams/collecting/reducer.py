"""Attach declared algebraic laws to mergeable collectors and merge their states."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Generic, Literal, TypeVar

from ..planning.semantics import StateProfile
from ._collector_base import Aggregator, Collector, _identity, _never_done

T = TypeVar("T")
S = TypeVar("S")
R = TypeVar("R")


class LawProvenance(StrEnum):
    """Identify whether reducer laws were project-verified or asserted by a user."""

    PROJECT_VERIFIED = "project_verified"
    USER_ASSERTED = "user_asserted"


class EmptyInputPolicy(StrEnum):
    """Describe what a reducer's finisher does with an untouched identity state."""

    FINISH_IDENTITY = "finish_identity"
    RETURNS_NONE = "returns_none"
    RAISES = "raises"


@dataclass(frozen=True, slots=True)
class ReducerLaws:
    """Declare merge laws, order requirements, empty behavior, and state growth.

    Reducers require a true associative merge and a true identity. Commutative reducers
    cannot also be marked order-sensitive. `state` tells planners whether partial state is
    constant-sized or grows with input, while `provenance` records who established the laws.
    """

    associative: Literal[True]
    commutative: bool
    order_sensitive: bool
    identity: Literal[True]
    empty_input: EmptyInputPolicy
    state: StateProfile
    provenance: LawProvenance

    def __post_init__(self) -> None:
        """Validate mandatory laws, compatible flags, and enum/profile field types."""
        if self.associative is not True or self.identity is not True:
            raise ValueError("Reducer laws require associative=True and identity=True")
        if self.commutative and self.order_sensitive:
            raise ValueError("commutative reducers cannot be order-sensitive")
        if not isinstance(self.empty_input, EmptyInputPolicy):
            raise TypeError("empty_input must be an EmptyInputPolicy")
        if not isinstance(self.provenance, LawProvenance):
            raise TypeError("provenance must be a LawProvenance")
        if not isinstance(self.state, StateProfile):
            raise TypeError("state must be a StateProfile")

    def to_dict(self) -> dict[str, Any]:
        """Serialize law flags and nested state metadata to plain Python values."""
        return {
            "associative": self.associative,
            "commutative": self.commutative,
            "order_sensitive": self.order_sensitive,
            "identity": self.identity,
            "empty_input": self.empty_input.value,
            "state": self.state.to_dict(),
            "provenance": self.provenance.value,
        }


class ReducerLawError(ValueError):
    """Raised when reducer state cannot be merged as the requested mode requires."""


class Reducer(Collector[T, S, R], Generic[T, S, R]):
    """A collector with a mandatory state merger and validated algebraic laws.

    The inherited initializer, step, finish, early-completion predicate, and native metadata
    define sequential collection. `combine` and `laws` additionally authorize partitioned
    state reduction.
    """

    __slots__ = ("laws",)
    laws: ReducerLaws

    def __init__(
        self,
        initializer: Callable[[], S],
        step: Callable[[S, T], S],
        finish: Callable[[S], R] = _identity,
        *,
        merge: Callable[[S, S], S],
        laws: ReducerLaws,
        done: Callable[[S], bool] = _never_done,
        native: Any | None = None,
    ) -> None:
        """Initialize a reducer after validating its merge callable and law declaration."""
        if not callable(merge):
            raise TypeError("Reducer merge must be callable")
        if not isinstance(laws, ReducerLaws):
            raise TypeError("laws must be a ReducerLaws")
        super().__init__(initializer, step, finish, merge, done, native)
        self.laws: ReducerLaws
        object.__setattr__(self, "laws", laws)

    def reduce(self, values: Iterable[T]) -> R:
        """Run the inherited collector state machine over `values` and finish its state."""
        return self(values)


class ReducerAggregator(Aggregator):
    """A named aggregator whose partial states may be merged under declared laws."""

    __slots__ = ("laws",)
    laws: ReducerLaws

    def __init__(
        self,
        initializer: Callable[[], Any],
        step: Callable[[Any, Any], Any],
        finish: Callable[[Any], Any] = _identity,
        *,
        merge: Callable[[Any, Any], Any],
        laws: ReducerLaws,
        done: Callable[[Any], bool] = _never_done,
        native: Any | None = None,
    ) -> None:
        """Initialize a mergeable aggregator after validating its merger and laws."""
        if not callable(merge):
            raise TypeError("ReducerAggregator merge must be callable")
        if not isinstance(laws, ReducerLaws):
            raise TypeError("laws must be a ReducerLaws")
        super().__init__(initializer, step, finish, merge, done, native)
        self.laws: ReducerLaws
        object.__setattr__(self, "laws", laws)


@dataclass(frozen=True, slots=True)
class ReductionExplanation:
    """Summarize whether a collector has trusted laws and a declared state combiner."""

    mergeable: bool
    combine_declared: bool
    laws: ReducerLaws | None

    def to_dict(self) -> dict[str, Any]:
        """Serialize mergeability, combiner presence, and optional reducer laws."""
        return {
            "mergeable": self.mergeable,
            "combine_declared": self.combine_declared,
            "laws": None if self.laws is None else self.laws.to_dict(),
        }


def explain_reduction(collector: Collector[Any, Any, Any]) -> ReductionExplanation:
    """Inspect a collector's declared support for partitioned state merging.

    Only :class:`Reducer` and :class:`ReducerAggregator` instances contribute laws and are
    therefore reported as mergeable. `combine_declared` independently reports whether any
    collector exposes a combiner. Non-collector inputs raise `TypeError`.
    """
    if not isinstance(collector, Collector):
        raise TypeError("collector must be a Collector")
    laws = (
        getattr(collector, "laws", None)
        if isinstance(collector, (Reducer, ReducerAggregator))
        else None
    )
    return ReductionExplanation(laws is not None, collector.combine is not None, laws)


def merge_reducer_states(states: Sequence[S], reducer: Reducer[Any, S, Any]) -> S:
    """Combine partial states in a deterministic, balanced pairwise tree.

    An empty sequence returns a fresh initializer state. A single state is returned unchanged;
    larger sequences merge adjacent pairs until one state remains. This function does not run
    the reducer finisher.
    """
    if not isinstance(reducer, Reducer):
        raise TypeError("reducer must be a Reducer")
    if not states:
        return reducer.initializer()
    merge = reducer.combine
    if merge is None:
        raise ReducerLawError("reducer does not provide a merge function")
    current = list(states)
    while len(current) > 1:
        current = [
            merge(current[i], current[i + 1]) if i + 1 < len(current) else current[i]
            for i in range(0, len(current), 2)
        ]
    return current[0]


def run_partitioned_reducer(
    values: Iterable[T], reducer: Reducer[T, S, R], *, partition_size: int
) -> R:
    """Reduce fixed-size sequential partitions, pairwise-merge them, and finish once.

    The input is fully consumed without consulting the reducer's early-completion predicate.
    Empty input still contributes one identity state. `partition_size` must be positive and
    `reducer` must be a :class:`Reducer`.
    """
    if partition_size <= 0:
        raise ValueError("partition_size must be positive")
    if not isinstance(reducer, Reducer):
        raise TypeError("reducer must be a Reducer")
    states: list[S] = []
    state = reducer.initializer()
    count = 0
    for value in values:
        state = reducer.step(state, value)
        count += 1
        if count == partition_size:
            states.append(state)
            state = reducer.initializer()
            count = 0
    if count or not states:
        states.append(state)
    return reducer.finish(merge_reducer_states(states, reducer))


COUNT_LAWS = ReducerLaws(
    True,
    True,
    False,
    True,
    EmptyInputPolicy.FINISH_IDENTITY,
    StateProfile.constant(),
    LawProvenance.PROJECT_VERIFIED,
)
LIST_LAWS = ReducerLaws(
    True,
    False,
    True,
    True,
    EmptyInputPolicy.FINISH_IDENTITY,
    StateProfile.grows_with_input(),
    LawProvenance.PROJECT_VERIFIED,
)
