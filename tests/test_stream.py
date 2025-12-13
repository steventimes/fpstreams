import pytest
from pyflow import Stream, Collectors

def test_stream_map_filter_collect():
    data = [1, 2, 3, 4, 5]
    result = (
        Stream(data)
        .filter(lambda x: x % 2 == 0)  # [2, 4]
        .map(lambda x: x * 10)         # [20, 40]
        .to_list()
    )
    assert result == [20, 40]

def test_stream_lazy_evaluation():
    # test if lazy
    def infinite_numbers():
        n = 0
        while True:
            yield n
            n += 1

    result = Stream(infinite_numbers()).limit(3).to_list()
    assert result == [0, 1, 2]

def test_collectors_grouping():
    data = ["apple", "apricot", "banana", "blueberry", "cherry"]
    result = (
        Stream(data)
        .collect(Collectors.grouping_by(lambda s: s[0]))
    )
    assert result == {
        "a": ["apple", "apricot"],
        "b": ["banana", "blueberry"],
        "c": ["cherry"]
    }

def test_stream_sorted_complex():
    data = [{"name": "Bob", "age": 30}, {"name": "Alice", "age": 25}]
    
    # Sort by age
    result = (
        Stream(data)
        .sorted(key=lambda x: x["age"])
        .map(lambda x: x["name"])
        .to_list()
    )
    assert result == ["Alice", "Bob"]
    
def test_take_while():
    data = [1, 2, 3, 10, 4, 5]
    # Should stop at 10
    result = Stream(data).take_while(lambda x: x < 10).to_list()
    assert result == [1, 2, 3]

def test_drop_while():
    data = [1, 2, 3, 10, 4, 5]
    # Should drop 1, 2, 3 and start taking from 10
    result = Stream(data).drop_while(lambda x: x < 10).to_list()
    assert result == [10, 4, 5]

def test_zip():
    names = ["Alice", "Bob", "Charlie"]
    scores = [100, 85, 95]
    
    result = Stream(names).zip(scores).to_list()
    
    assert result == [("Alice", 100), ("Bob", 85), ("Charlie", 95)]

def test_zip_mismatch_length():
    # stop at the shortest list
    list1 = [1, 2, 3]
    list2 = ["a", "b"]
    
    result = Stream(list1).zip(list2).to_list()
    assert result == [(1, "a"), (2, "b")]

def test_zip_with_index():
    data = ["a", "b", "c"]
    result = Stream(data).zip_with_index().to_list()
    assert result == [(0, "a"), (1, "b"), (2, "c")]

def test_partitioning_by():
    # Split numbers into Even (True) and Odd (False)
    data = [1, 2, 3, 4, 5, 6]
    
    result = (
        Stream(data)
        .collect(Collectors.partitioning_by(lambda x: x % 2 == 0))
    )
    
    assert result[True] == [2, 4, 6]  # Evens
    assert result[False] == [1, 3, 5] # Odds