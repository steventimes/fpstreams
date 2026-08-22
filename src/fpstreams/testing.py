"""Developer checks for custom reducers."""

from __future__ import annotations

import operator
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from .collecting.reducer import Reducer, merge_reducer_states


@dataclass(frozen=True, slots=True)
class ReducerLawReport:
    """Record sampled associativity, left identity, and partition-equivalence checks."""

    associative: bool
    identity: bool
    partition_equivalent: bool
    checked_partitions: tuple[tuple[int, ...], ...]


def check_reducer_laws(
    reducer: Reducer[Any, Any, Any],
    values: Iterable[Any],
    *,
    partitions: Iterable[tuple[int, ...]],
    equivalent: Callable[[Any, Any], bool] = operator.eq,
) -> ReducerLawReport:
    """Evaluate reducer laws against materialized example values."""
    if not isinstance(reducer, Reducer):
        raise TypeError("reducer must be a Reducer")
    data = tuple(values)
    reference = reducer.reduce(data)
    merge = reducer.combine
    if merge is None:
        raise ValueError("reducer must have a merge function")

    identity_state = reducer.initializer()
    sample_state = reducer.initializer()
    for value in data:
        sample_state = reducer.step(sample_state, value)
    identity = equivalent(
        reducer.finish(merge(identity_state, sample_state)),
        reducer.finish(sample_state),
    )

    checked: list[tuple[int, ...]] = []
    partition_equivalent = True
    for layout in partitions:
        normalized = tuple(layout)
        checked.append(normalized)
        if any(size < 0 for size in normalized) or sum(normalized) != len(data):
            raise ValueError("partition sizes must be non-negative and sum to input length")
        states: list[Any] = []
        offset = 0
        for size in normalized:
            state = reducer.initializer()
            for value in data[offset : offset + size]:
                state = reducer.step(state, value)
            states.append(state)
            offset += size
        result = reducer.finish(merge_reducer_states(states, reducer))
        if not equivalent(reference, result):
            partition_equivalent = False

    associative = True
    if len(data) >= 3:
        states = []
        for value in data[:3]:
            state = reducer.initializer()
            states.append(reducer.step(state, value))
        left, middle, right = states
        associative = equivalent(
            merge(merge(left, middle), right),
            merge(left, merge(middle, right)),
        )

    return ReducerLawReport(associative, identity, partition_equivalent, tuple(checked))


def assert_reducer_laws(
    reducer: Reducer[Any, Any, Any],
    values: Iterable[Any],
    *,
    partitions: Iterable[tuple[int, ...]],
    equivalent: Callable[[Any, Any], bool] = operator.eq,
) -> None:
    """Run :func:`check_reducer_laws` and raise for its first failed property."""
    report = check_reducer_laws(reducer, values, partitions=partitions, equivalent=equivalent)
    if not report.associative:
        raise AssertionError("reducer merge is not associative")
    if not report.identity:
        raise AssertionError("reducer identity does not satisfy the declared empty-input policy")
    if not report.partition_equivalent:
        raise AssertionError(
            f"reducer is not partition-equivalent; checked {report.checked_partitions!r}"
        )


__all__ = [
    "ReducerLawReport",
    "assert_reducer_laws",
    "check_reducer_laws",
]
