"""Option, Result, and functional-helper laws and examples."""

from __future__ import annotations

from typing import Any, cast

import pytest

from fpstreams import Err, Option, Result, Stream, curry, pipe, retry


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


def test_err_requires_an_exception_argument() -> None:
    with pytest.raises(TypeError):
        cast(Any, Err)()


def test_pipe_and_curry_examples() -> None:
    assert pipe(3, lambda x: x + 2, lambda x: x * 4) == 20

    @curry
    def add3(a: int, b: int, c: int) -> int:
        return a + b + c

    assert add3(1)(2)(3) == 6
    assert add3(1, 2, 3) == 6


def test_curry_calls_after_all_required_parameters_are_bound() -> None:
    def scale(value: int, factor: int = 2) -> int:
        return value * factor

    assert curry(scale)(3) == 6


def test_curry_accepts_callables_without_code_objects() -> None:
    assert curry(pow)(2)(3) == 8


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


def test_theorem_option_functor_identity_and_composition_on_finite_domain() -> None:
    domain = [None, -2, -1, 0, 1, 2]

    def f(value: int) -> int:
        return value + 3

    def g(value: int) -> int:
        return value * 2

    for v in domain:
        opt = Option.of_nullable(v)
        # Identity: map(id) == self
        assert opt.map(lambda x: x).or_else(None) == opt.or_else(None)
        # Composition: map(f).map(g) == map(g∘f)
        lhs = opt.map(f).map(g).or_else(None)
        rhs = opt.map(lambda x: g(f(x))).or_else(None)
        assert lhs == rhs


def test_theorem_result_monad_left_and_right_identity_finite_domain() -> None:
    domain = [-2, -1, 0, 1, 2]

    def f(x: int) -> Result[int]:
        return Result.success(x * 10)

    for x in domain:
        # Left identity: return x >>= f == f x
        assert Result.success(x).flat_map(f).get_or_else(0) == f(x).get_or_else(0)

        # Right identity: m >>= return == m
        m = Result.success(x)
        assert m.flat_map(Result.success).get_or_else(0) == m.get_or_else(0)


def test_theorem_stream_sum_homomorphism_under_concatenation() -> None:
    left = [1, 2, 3]
    right = [4, 5]
    assert Stream(left + right).sum() == Stream(left).sum() + Stream(right).sum()
