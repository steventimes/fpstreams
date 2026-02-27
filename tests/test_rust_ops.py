"""QA-style tests for Rust-accelerated list operations.

These tests validate both the functional contract and the dispatch behavior
between Rust-backed implementations and pure-Python fallbacks.
"""

from __future__ import annotations

import pytest

import fpstreams.rust_ops as rust_ops


class TestRustOpsFunctionalParity:
    """Behavior-focused tests for public rust_ops helpers."""

    def test_batch_window_examples_match_expected_shapes(self) -> None:
        data = [1, 2, 3, 4, 5]

        assert rust_ops.batch_list(data, 2) == [[1, 2], [3, 4], [5]]
        assert rust_ops.window_list(data, 3, 1) == [[1, 2, 3], [2, 3, 4], [3, 4, 5]]

    def test_sorted_min_max_empty_edge_cases(self) -> None:
        assert rust_ops.sorted_list([3, 1, 2], reverse=True) == [3, 2, 1]
        assert rust_ops.min_list([]) is None
        assert rust_ops.max_list([]) is None

    def test_distinct_list_preserves_first_seen_order(self) -> None:
        assert rust_ops.distinct_list([3, 1, 3, 2, 1]) == [3, 1, 2]

    def test_limit_skip_and_sum_examples(self) -> None:
        values = [10, 11, 12, 13]
        assert rust_ops.limit_list(values, 2) == [10, 11]
        assert rust_ops.skip_list(values, 2) == [12, 13]
        assert rust_ops.sum_list([1, 2, 3]) == 6


class TestRustOpsDispatchBehavior:
    """Dispatch tests ensure predictable fallback semantics for integration."""

    def test_call_rust_returns_none_when_rust_not_available(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(rust_ops, "_RUST_AVAILABLE", False)

        called = False

        def _fake(*_: object) -> object:
            nonlocal called
            called = True
            return "should-not-run"

        assert rust_ops._call_rust(_fake, 1, 2, 3) is None
        assert called is False

    def test_call_rust_executes_function_when_rust_available(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(rust_ops, "_RUST_AVAILABLE", True)

        def _fake(a: int, b: int) -> int:
            return a + b

        assert rust_ops._call_rust(_fake, 2, 3) == 5

    def test_distinct_list_falls_back_to_python_when_rust_function_missing(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(rust_ops, "_RUST_AVAILABLE", True)
        monkeypatch.setattr(rust_ops, "_distinct_list", None)

        assert rust_ops.distinct_list([1, 1, 2, 1]) == [1, 2]
