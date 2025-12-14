import pytest
from fpstreams import Stream

def test_sum_sequential():
    data = [1, 2, 3, 4]
    assert Stream(data).sum() == 10

def test_min_max_sequential():
    data = [10, 5, 20]
    assert Stream(data).min().or_else(0) == 5
    assert Stream(data).max().or_else(0) == 20

def test_sum_parallel():
    data = list(range(100))
    assert Stream(data).parallel(processes=2).sum() == 4950

def test_max_parallel():
    data = list(range(1000))
    assert Stream(data).parallel(processes=2).max().or_else(0) == 999