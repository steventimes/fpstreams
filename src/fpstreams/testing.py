"""Developer checks for custom reducers."""

from __future__ import annotations

import operator
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any, SupportsIndex

from .collecting.reducer import Reducer, merge_reducer_states
from .runtime.iterators import closing_iterators


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
    partitions: Iterable[tuple[SupportsIndex, ...]],
    equivalent: Callable[[Any, Any], bool] = operator.eq,
) -> ReducerLawReport:
    """Sample reducer laws using independent states for each merge expression.

    Materialize and close the input before replaying examples. Each identity and associativity
    branch starts from fresh initializer/step results, so in-place merging cannot alter the
    other branch. `equivalent` compares raw states for associativity and finished outputs for
    identity and partition equivalence. Sampled checks cannot prove laws for arbitrary inputs.
    Initializers must produce independent state, and replay must not mutate the input examples.
    Partition sizes support `__index__`; checked layouts contain normalized Python integers.
    """
    if not isinstance(reducer, Reducer):
        raise TypeError("reducer must be a Reducer")
    materialized: list[Any] = []
    iterator = iter(values)
    with closing_iterators((iterator,)):
        while True:
            try:
                materialized.append(next(iterator))
            except StopIteration:
                break
    data = tuple(materialized)
    del materialized

    def accumulate(sample: Iterable[Any]) -> Any:
        state = reducer.initializer()
        for value in sample:
            state = reducer.step(state, value)
        return state

    reference = reducer.reduce(data)
    merge = reducer.combine
    if merge is None:
        raise ValueError("reducer must have a merge function")

    identity = equivalent(
        reducer.finish(merge(reducer.initializer(), accumulate(data))),
        reference,
    )

    checked: list[tuple[int, ...]] = []
    partition_equivalent = True
    for layout in partitions:
        normalized = tuple(operator.index(size) for size in layout)
        checked.append(normalized)
        if any(size < 0 for size in normalized) or sum(normalized) != len(data):
            raise ValueError("partition sizes must be non-negative and sum to input length")
        states: list[Any] = []
        offset = 0
        for size in normalized:
            states.append(accumulate(data[offset : offset + size]))
            offset += size
        result = reducer.finish(merge_reducer_states(states, reducer))
        if not equivalent(reference, result):
            partition_equivalent = False

    associative = True
    if len(data) >= 3:
        left, middle, right = (accumulate((value,)) for value in data[:3])
        other_left, other_middle, other_right = (accumulate((value,)) for value in data[:3])
        associative = equivalent(
            merge(merge(left, middle), right),
            merge(other_left, merge(other_middle, other_right)),
        )

    return ReducerLawReport(associative, identity, partition_equivalent, tuple(checked))


def assert_reducer_laws(
    reducer: Reducer[Any, Any, Any],
    values: Iterable[Any],
    *,
    partitions: Iterable[tuple[SupportsIndex, ...]],
    equivalent: Callable[[Any, Any], bool] = operator.eq,
) -> None:
    """Run :func:`check_reducer_laws` and raise for its first failed property."""
    report = check_reducer_laws(
        reducer,
        values,
        partitions=partitions,
        equivalent=equivalent,
    )
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
