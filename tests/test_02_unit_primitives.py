"""Unit tests for Option/Result/functional helpers with concrete examples."""

from __future__ import annotations

import pytest

from fpstreams import Option, Result, curry, pipe, retry


def test_option_paths_present_and_empty() -> None:
    assert Option.of_nullable(10).filter(lambda x: x > 3).or_else(0) == 10
    assert Option.empty().map(lambda x: x).or_else(99) == 99


def test_option_of_none_is_rejected() -> None:
    with pytest.raises(ValueError):
        Option.of(None)  # type: ignore[arg-type]


def test_result_success_and_failure_paths() -> None:
    assert Result.success(5).map(lambda x: x + 1).get_or_else(0) == 6
    failure = Result.of(lambda: 1 / 0)
    assert failure.is_failure()


def test_pipe_and_curry_examples() -> None:
    assert pipe(3, lambda x: x + 2, lambda x: x * 4) == 20

    @curry
    def add3(a: int, b: int, c: int) -> int:
        return a + b + c

    assert add3(1)(2)(3) == 6
    assert add3(1, 2, 3) == 6


def test_retry_recovers_transient_failure() -> None:
    state = {"attempt": 0}

    @retry(attempts=3, backoff=1.0, jitter=False)
    async def flaky() -> int:
        state["attempt"] += 1
        if state["attempt"] < 2:
            raise RuntimeError("transient")
        return 42

    import asyncio

    assert asyncio.run(flaky()) == 42
    assert state["attempt"] == 2
