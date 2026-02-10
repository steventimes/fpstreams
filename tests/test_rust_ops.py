import pytest

import fpstreams.rust_ops as rust_ops


def test_distinct_list_matches_expected_order():
    data = [3, 1, 3, 2, 1]
    assert rust_ops.distinct_list(data) == [3, 1, 2]


def test_batch_list_produces_chunks():
    data = list(range(7))
    assert rust_ops.batch_list(data, 3) == [[0, 1, 2], [3, 4, 5], [6]]


def test_limit_list_truncates_sequence():
    data = [10, 11, 12, 13]
    assert rust_ops.limit_list(data, 2) == [10, 11]


def test_skip_list_drops_prefix():
    data = [10, 11, 12, 13]
    assert rust_ops.skip_list(data, 2) == [12, 13]


def test_window_list_respects_step():
    data = [1, 2, 3, 4, 5]
    assert rust_ops.window_list(data, 3, 1) == [[1, 2, 3], [2, 3, 4], [3, 4, 5]]
    assert rust_ops.window_list(data, 2, 2) == [[1, 2], [3, 4]]


def test_sorted_list_supports_reverse():
    data = [3, 1, 2]
    assert rust_ops.sorted_list(data) == [1, 2, 3]
    assert rust_ops.sorted_list(data, reverse=True) == [3, 2, 1]


def test_min_max_list_handles_empty():
    assert rust_ops.min_list([]) is None
    assert rust_ops.max_list([]) is None


def test_sum_list_matches_python_sum():
    data = [1, 2, 3]
    assert rust_ops.sum_list(data) == 6
