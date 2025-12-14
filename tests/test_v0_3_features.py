import pytest
import os
import json
import csv
from fpstreams import Stream

users = [
    {"id": 1, "name": "Alice", "role": "admin"},
    {"id": 2, "name": "Bob", "role": None},
    {"id": 3, "name": None, "role": "user"},
    {"id": 4, "name": "Dave", "role": "user"},
]

numeric_data = [1, 2, 3, 4, 5, 100]


def test_pluck():
    # extract the names
    names = Stream(users).pluck("name").to_list()
    assert names == ["Alice", "Bob", None, "Dave"]

def test_drop_none_basic():
    data = [1, None, 2, None, 3]
    result = Stream(data).drop_none().to_list()
    assert result == [1, 2, 3]

def test_drop_none_key():
    # Drop users where 'name' is None
    valid_users = Stream(users).drop_none(key="name").to_list()
    assert len(valid_users) == 3
    assert valid_users[2]["name"] == "Dave"

def test_join():
    data = ["a", "b", "c"]
    assert Stream(data).join("-") == "a-b-c"
    assert Stream(data).join("") == "abc"

def test_concat():
    s1 = Stream([1, 2])
    s2 = Stream([3, 4])
    result = Stream.concat(s1, s2).to_list()
    assert result == [1, 2, 3, 4]

def test_describe_numeric():
    stats = Stream([1, 2, 3, 4, 5]).describe()
    assert stats["count"] == 5
    assert stats["sum"] == 15
    assert stats["min"] == 1
    assert stats["max"] == 5
    assert stats["mean"] == 3.0

def test_describe_empty():
    stats = Stream([]).describe()
    assert stats == {}


def test_to_csv(tmp_path):
    file = tmp_path / "output.csv"
    
    data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    Stream(data).to_csv(str(file), header=["a", "b"])
    
    with open(file) as f:
        content = f.read().strip().split("\n")
        assert content[0] == "a,b"
        assert content[1] == "1,2"
        assert content[2] == "3,4"

def test_to_json(tmp_path):
    file = tmp_path / "output.json"
    
    data = [{"id": 1}, {"id": 2}]
    Stream(data).to_json(str(file))
    
    with open(file) as f:
        loaded = json.load(f)
        assert loaded == data

def test_to_np():
    try:
        import numpy as np
    except ImportError:
        pytest.skip("Numpy not installed")

    arr = Stream([1, 2, 3]).to_np()
    assert isinstance(arr, np.ndarray)
    assert np.array_equal(arr, np.array([1, 2, 3]))

def test_to_df():
    try:
        import pandas as pd
    except ImportError:
        pytest.skip("Pandas not installed")

    data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    df = Stream(data).to_df()
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert df.iloc[0]["a"] == 1