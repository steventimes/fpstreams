"""Property-based tests: random valid inputs verifying invariants."""

from __future__ import annotations

import random

from fpstreams import Stream


def test_property_distinct_preserves_first_occurrence_order() -> None:
    rng = random.Random(101)
    for _ in range(300):
        values = [rng.randint(-20, 20) for _ in range(rng.randint(0, 120))]
        out = Stream(values).distinct().to_list()
        expected: list[int] = []
        seen: set[int] = set()
        for v in values:
            if v not in seen:
                seen.add(v)
                expected.append(v)
        assert out == expected


def test_property_batch_flattens_back_to_original() -> None:
    rng = random.Random(102)
    for _ in range(250):
        values = [rng.randint(-1_000, 1_000) for _ in range(rng.randint(0, 300))]
        size = rng.randint(1, 25)
        batches = Stream(values).batch(size).to_list()
        flattened = [x for b in batches for x in b]
        assert flattened == values


def test_property_limit_prefix_law() -> None:
    rng = random.Random(103)
    for _ in range(250):
        values = [rng.randint(-50, 50) for _ in range(rng.randint(0, 200))]
        n = rng.randint(0, 100)
        out = Stream(values).limit(n).to_list()
        assert len(out) <= n
        assert out == values[:n]
