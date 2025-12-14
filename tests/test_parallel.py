import pytest
from fpstreams import Stream

def heavy_calc(x):
    return x * x

def heavy_filter(x):
    return x % 2 == 0

def add_values(acc, x):
    return acc + x

def filter_is_even(x):
    return x % 2 == 0

def test_parallel_map():
    data = list(range(100))
    expected = [x * x for x in data]

    result = Stream(data).parallel(processes=2).map(heavy_calc).to_list()
    
    assert sorted(result) == sorted(expected)

def test_parallel_filter():
    data = list(range(100))
    result = (
        Stream(data)
        .parallel(processes=2)
        .filter(filter_is_even)
        .to_list()
    )
    assert len(result) == 50

def test_parallel_reduce():
    data = list(range(1, 5)) # 1, 2, 3, 4
    result = Stream(data).parallel(processes=2).reduce(0, add_values)
    assert result == 10

def test_parallel_pick_filter_none():
    data = [{"id": 1}, {"id": None}, {"id": 2}]
    result = (
        Stream(data)
        .parallel(processes=2)
        .pick("id")
        .filter_none()
        .to_list()
    )
    assert sorted(result) == [1, 2]