"""Theorem-style tests: algebraic law checks (proof by finite-domain exhaustion)."""

from __future__ import annotations

from fpstreams import Option, Result, Stream


def test_theorem_option_functor_identity_and_composition_on_finite_domain() -> None:
    domain = [None, -2, -1, 0, 1, 2]
    f = lambda x: x + 3
    g = lambda x: x * 2

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
