import pytest
from fpstreams import Stream

def test_sum():
    assert Stream([1, 2, 3]).sum() == 6

def test_min_max():
    data = [10, 1, 5]
    assert Stream(data).min().or_else(0) == 1
    assert Stream(data).max().or_else(0) == 10

def test_min_max_with_key():
    data = [{"v": 10}, {"v": 1}, {"v": 5}]
    min_val = Stream(data).min(key=lambda x: x["v"]).or_else({})
    assert min_val == {"v": 1}

def test_describe_numeric():
    stats = Stream([1, 2, 3, 4, 5]).describe()
    assert stats["count"] == 5
    assert stats["sum"] == 15
    assert stats["mean"] == 3.0
    assert stats["min"] == 1
    assert stats["max"] == 5
    assert "std" in stats

def test_describe_mixed_safe():
    # Should not crash on non-numeric data
    stats = Stream(["a", "b"]).describe()
    assert stats["count"] == 2
    assert "mean" not in stats

def test_describe_empty():
    assert Stream([]).describe() == {}