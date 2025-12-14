import pytest
from fpstreams import Stream, Collectors

def test_grouping_by():
    data = ["apple", "apricot", "banana"]
    result = Stream(data).collect(Collectors.grouping_by(lambda s: s[0]))
    
    assert result == {
        "a": ["apple", "apricot"],
        "b": ["banana"]
    }

def test_partitioning_by():
    data = [1, 2, 3, 4]
    result = Stream(data).collect(Collectors.partitioning_by(lambda x: x % 2 == 0))
    
    assert result[True] == [2, 4]
    assert result[False] == [1, 3]

def test_to_columns():
    data = [{"a": 1, "b": 2}, {"a": 3}]
    result = Stream(data).collect(Collectors.to_columns())
    
    assert result["a"] == [1, 3]
    assert result["b"] == [2, None]

def test_joining_collector():
    data = ["x", "y", "z"]
    result = Stream(data).collect(Collectors.joining(","))
    assert result == "x,y,z"