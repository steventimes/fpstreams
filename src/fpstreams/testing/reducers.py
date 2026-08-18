from __future__ import annotations

import operator
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from ..collecting.reducer import Reducer, merge_reducer_states


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
    """Evaluate selected reducer laws against materialized example values.

    The reference result comes from normal sequential reduction. Each partition layout must
    contain non-negative sizes summing to the input length; its independently stepped states
    are pairwise merged and compared with that reference. Left identity is checked using the
    full sample state. Associativity is sampled from the first three input-derived states and
    is reported as true without a comparison when fewer than three values are available.

    Args:
        reducer: Mergeable reducer to exercise.
        values: Finite example input, materialized once as a tuple.
        partitions: Partition-size layouts to compare with sequential reduction.
        equivalent: Equality predicate for finished results and sampled merged states.

    Returns:
        A report containing each Boolean outcome and the normalized layouts checked.

    Raises:
        TypeError: If `reducer` is not a :class:`Reducer`.
        ValueError: If no merger exists or a partition layout is invalid.
    """
    if not isinstance(reducer, Reducer):
        raise TypeError("reducer must be a Reducer")
    data = tuple(values)
    reference = reducer.reduce(data)
    merge = reducer.combine
    if merge is None:
        raise ValueError("reducer must have a merge function")
    associative = True
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
        checked.append(tuple(layout))
        if any(size < 0 for size in layout) or sum(layout) != len(data):
            raise ValueError("partition sizes must be non-negative and sum to input length")
        states: list[Any] = []
        offset = 0
        for size in layout:
            state = reducer.initializer()
            for value in data[offset : offset + size]:
                state = reducer.step(state, value)
            states.append(state)
            offset += size
        result = reducer.finish(merge_reducer_states(states, reducer))
        if not equivalent(reference, result):
            partition_equivalent = False
    if len(data) >= 3:
        left = reducer.initializer()
        middle = reducer.initializer()
        right = reducer.initializer()
        for value in data[:1]:
            left = reducer.step(left, value)
        for value in data[1:2]:
            middle = reducer.step(middle, value)
        for value in data[2:3]:
            right = reducer.step(right, value)
        lhs = merge(merge(left, middle), right)
        rhs = merge(left, merge(middle, right))
        associative = equivalent(lhs, rhs)
    return ReducerLawReport(associative, identity, partition_equivalent, tuple(checked))


def assert_reducer_laws(
    reducer: Reducer[Any, Any, Any],
    values: Iterable[Any],
    *,
    partitions: Iterable[tuple[int, ...]],
    equivalent: Callable[[Any, Any], bool] = operator.eq,
) -> None:
    """Run :func:`check_reducer_laws` and raise for its first failed property.

    Failures are checked in associativity, identity, then partition-equivalence order. Invalid
    inputs retain the `TypeError` or `ValueError` behavior of the underlying check.
    """
    report = check_reducer_laws(reducer, values, partitions=partitions, equivalent=equivalent)
    if not report.associative:
        raise AssertionError("reducer merge is not associative")
    if not report.identity:
        raise AssertionError("reducer identity does not satisfy the declared empty-input policy")
    if not report.partition_equivalent:
        raise AssertionError(
            f"reducer is not partition-equivalent; checked {report.checked_partitions!r}"
        )
