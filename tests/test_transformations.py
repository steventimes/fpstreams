import pytest
from fpstreams import Stream

def test_map_filter():
    data = [1, 2, 3, 4, 5]
    result = (
        Stream(data)
        .filter(lambda x: x % 2 == 0)  # [2, 4]
        .map(lambda x: x * 10)         # [20, 40]
        .to_list()
    )
    assert result == [20, 40]

def test_flat_map():
    data = [[1, 2], [3, 4]]
    result = Stream(data).flat_map(lambda x: x).to_list()
    assert result == [1, 2, 3, 4]

def test_pick_key():
    users = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": None}
    ]
    names = Stream(users).pick("name").to_list()
    assert names == ["Alice", "Bob", None]

def test_pick_index():
    data = [[1, 2], [3, 4]]
    col2 = Stream(data).pick(1).to_list()
    assert col2 == [2, 4]

def test_filter_none():
    data = [1, None, 2, None, 3]
    result = Stream(data).filter_none().to_list()
    assert result == [1, 2, 3]

def test_filter_none_with_key():
    data = [
        {"val": 1},
        {"val": None},
        {"val": 2},
        {"other": 3} # Missing key treated as None
    ]
    # Should keep only items where item['val'] is not None
    result = Stream(data).filter_none(key="val").to_list()
    assert result == [{"val": 1}, {"val": 2}]

def test_distinct():
    data = [1, 2, 2, 3, 3, 3]
    result = Stream(data).distinct().to_list()
    assert result == [1, 2, 3]

def test_sorted():
    data = [3, 1, 4, 2]
    result = Stream(data).sorted().to_list()
    assert result == [1, 2, 3, 4]

def test_sorted_custom_key():
    data = [{"a": 2}, {"a": 1}]
    result = Stream(data).sorted(key=lambda x: x["a"]).to_list()
    assert result == [{"a": 1}, {"a": 2}]

def test_limit_skip():
    data = range(10)
    result = Stream(data).skip(2).limit(3).to_list()
    assert result == [2, 3, 4]

def test_take_while():
    data = [1, 2, 3, 10, 4, 5]
    result = Stream(data).take_while(lambda x: x < 10).to_list()
    assert result == [1, 2, 3]

def test_drop_while():
    data = [1, 2, 3, 10, 4, 5]
    result = Stream(data).drop_while(lambda x: x < 10).to_list()
    assert result == [10, 4, 5]

def test_zip():
    names = ["Alice", "Bob"]
    scores = [100, 85, 90] # 90 ignored
    result = Stream(names).zip(scores).to_list()
    assert result == [("Alice", 100), ("Bob", 85)]

def test_zip_with_index():
    data = ["a", "b"]
    result = Stream(data).zip_with_index(start=1).to_list()
    assert result == [(1, "a"), (2, "b")]

def test_chain():
    s1 = Stream([1, 2])
    s2 = Stream([3, 4])
    result = Stream.chain(s1, s2).to_list()
    assert result == [1, 2, 3, 4]

def test_lazy_evaluation():
    def infinite():
        n = 0
        while True:
            yield n
            n += 1
    
    result = Stream(infinite()).limit(5).to_list()
    assert result == [0, 1, 2, 3, 4]