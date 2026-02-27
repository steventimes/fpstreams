"""Property-based tests for stream/list semantics.

These complement example-based QA tests by validating invariants across a wide
input space, which is useful for larger codebases and regression prevention.
"""

from __future__ import annotations

import pytest

hypothesis = pytest.importorskip("hypothesis")
from hypothesis import given
from hypothesis import strategies as st

from fpstreams import Option, Stream
import fpstreams.rust_ops as rust_ops


@given(st.lists(st.integers(), max_size=100), st.integers(min_value=0, max_value=120))
def test_limit_list_matches_slice_invariant(values: list[int], limit: int) -> None:
    assert rust_ops.limit_list(values, limit) == values[:limit]


@given(st.lists(st.integers(), max_size=100), st.integers(min_value=0, max_value=120))
def test_skip_list_matches_slice_invariant(values: list[int], skip: int) -> None:
    assert rust_ops.skip_list(values, skip) == values[skip:]


@given(st.lists(st.integers(), max_size=150))
def test_distinct_list_matches_first_occurrence_invariant(values: list[int]) -> None:
    result = rust_ops.distinct_list(values)
    seen: set[int] = set()
    expected: list[int] = []
    for item in values:
        if item not in seen:
            seen.add(item)
            expected.append(item)
    assert result == expected


@given(st.one_of(st.none(), st.text()), st.text())
def test_option_or_else_nullable_invariant(value: str | None, fallback: str) -> None:
    assert Option.of_nullable(value).or_else(fallback) == (value if value is not None else fallback)


@given(st.integers(min_value=0, max_value=1_000), st.integers(min_value=1, max_value=50))
def test_stream_iterate_arithmetic_progression_invariant(seed: int, step: int) -> None:
    values = Stream.iterate(seed, lambda x: x + step).limit(8).to_list()
    assert len(values) == 8
    for left, right in zip(values, values[1:]):
        assert right - left == step
