import pytest
from fpstreams import Stream

# --- Top-level helpers for pickling ---
def heavy_calc(x):
    return x * x

def filter_is_even(x):
    return x % 2 == 0

def add_values(acc, x):
    return acc + x

def increment(x):
    return x + 1

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

# --- v0.4.0 Parallel Structure Tests ---

def test_parallel_batch():
    data = list(range(20))
    result = (
        Stream(data)
        .parallel(processes=2)
        .batch(5)
        .to_list()
    )
    assert len(result) == 4
    for batch in result:
        assert len(batch) == 5

def test_parallel_window():
    data = list(range(10))
    result = (
        Stream(data)
        .parallel(processes=2)
        .window(2)
        .to_list()
    )

    for win in result:
        assert len(win) == 2

def test_parallel_large_dataset_no_crash():
    count = (
        Stream(range(1000))
        .parallel()
        .map(increment) 
        .count()
    )
    assert count == 1000