import pytest
import math
from fpstreams import Stream

def heavy_calc(x):
    return x * x

def heavy_filter(x):
    return x % 2 == 0

def add_one(x):
    return x + 1

def test_parallel_map_correctness():
    """Verify parallel map returns same results as sequential."""
    data = list(range(1000))
    
    # sequential
    expected = [x * x for x in data]
    
    # Parallel
    result = (
        Stream(data)
        .parallel(processes=2) # Force 2 processes
        .map(heavy_calc)
        .to_list()
    )

    assert result == expected

def test_parallel_filter_correctness():
    """Verify parallel filter works."""
    data = list(range(1000))
    expected = [x for x in data if x % 2 == 0]
    
    result = (
        Stream(data)
        .parallel(processes=2)
        .filter(heavy_filter)
        .to_list()
    )
    
    assert result == expected

def test_parallel_pipeline_mixed():
    """Verify a chain of map -> filter -> map works in parallel."""
    data = list(range(100))
    
    expected = [(x*x) + 1 for x in data if (x*x) % 2 == 0]
    
    result = (
        Stream(data)
        .parallel(processes=2)
        .map(heavy_calc)       # Square
        .filter(heavy_filter)  # Keep evens
        .map(add_one)  # Add 1
        .to_list()
    )
    
    assert result == expected

def test_parallel_large_dataset():
    """Stress test with a larger dataset to ensure no hanging."""
    data = range(10_000)
    
    count = (
        Stream(data)
        .parallel()
        .map(heavy_calc)
        .count()
    )
    
    assert count == 10_000