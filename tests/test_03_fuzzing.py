"""Fuzz-style tests: random input stress for crash detection and oracle comparison."""

from __future__ import annotations

import random

from fpstreams import Stream


def test_fuzz_random_numeric_pipelines_are_crash_free_and_match_oracle() -> None:
    rng = random.Random(20260101)

    for _ in range(400):
        size = rng.randint(0, 200)
        data = [rng.randint(-10_000, 10_000) for _ in range(size)]

        skip_n = rng.randint(0, 10)
        limit_n = rng.randint(0, 100)

        result = (
            Stream(data)
            .map(lambda x: x * 3 - 1)
            .filter(lambda x: x % 7 != 0)
            .skip(skip_n)
            .limit(limit_n)
            .to_list()
        )

        expected = [x * 3 - 1 for x in data]
        expected = [x for x in expected if x % 7 != 0]
        expected = expected[skip_n:][:limit_n]

        assert result == expected
        assert all((x + 1) % 3 == 0 for x in result)


def test_fuzz_string_joining_never_crashes() -> None:
    rng = random.Random(7)
    for _ in range(300):
        words = ["".join(chr(rng.randint(97, 122)) for _ in range(rng.randint(0, 8))) for _ in range(rng.randint(0, 60))]
        out = Stream(words).join("|")
        assert isinstance(out, str)
