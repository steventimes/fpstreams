import pytest
from fpstreams import Stream, Collectors

def test_grouping_by_simple():
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

def test_summarizing():
    data = [1, 2, 3, 4, 5]
    stats = Stream(data).collect(Collectors.summarizing(lambda x: float(x)))
    
    assert stats.count == 5
    assert stats.sum == 15.0
    assert stats.min == 1.0
    assert stats.max == 5.0
    assert stats.average == 3.0

def test_averaging():
    data = [10, 20, 30]
    avg = Stream(data).collect(Collectors.averaging(lambda x: x))
    assert avg == 20.0

def test_grouping_by_counting():
    words = ["a", "bb", "ccc", "d", "ee"]
    groups = Stream(words).collect(
        Collectors.grouping_by(
            lambda x: len(x),
            downstream=Collectors.counting()
        )
    )
    
    assert groups[1] == 2 # "a", "d"
    assert groups[2] == 2 # "bb", "ee"
    assert groups[3] == 1 # "ccc"

def test_grouping_by_summing():
    items = [
        {"cat": "A", "val": 10},
        {"cat": "B", "val": 20},
        {"cat": "A", "val": 5}
    ]
    groups = Stream(items).collect(
        Collectors.grouping_by(
            lambda x: x["cat"],
            downstream=Collectors.summing(lambda x: x["val"])
        )
    )
    
    assert groups["A"] == 15
    assert groups["B"] == 20