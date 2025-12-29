import pytest
from fpstreams import Stream, ParallelStream
import math


def test_zip_longest_sequential():
    s1 = [1, 2]
    s2 = ["a", "b", "c"]
    
    result = Stream(s1).zip_longest(s2, fillvalue=-1).to_list()
    
    assert result == [(1, "a"), (2, "b"), (-1, "c")]

def test_scan_sequential():
    # Scan is like reduce but keeps intermediate results
    # 1 -> 1
    # 2 -> 1+2 = 3
    # 3 -> 3+3 = 6
    # 4 -> 6+4 = 10
    data = [1, 2, 3, 4]
    result = Stream(data).scan(0, lambda acc, x: acc + x).to_list()
    assert result == [0, 1, 3, 6, 10]

def test_parallel_scan_fallback():
    data = [1, 2, 3, 4]
    result = ParallelStream(data).scan(0, lambda acc, x: acc + x).to_list()
    assert result == [0, 1, 3, 6, 10]


def test_describe_numeric():
    data = [1, 2, 3, 4, 5, 100]
    stats = Stream(data).describe()
    
    assert stats["count"] == 6
    assert stats["min"] == 1
    assert stats["max"] == 100
    assert stats["sum"] == 115
    assert "mean" in stats
    assert "std" in stats

def test_describe_empty():
    stats = Stream([]).describe()
    assert stats == {}

def test_to_numpy():
    try:
        import numpy as np
    except ImportError:
        pytest.skip("Numpy not installed")
        
    data = [1, 2, 3]
    arr = Stream(data).to_np()
    
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
    assert df.shape == (2, 2)
    assert df.iloc[0]["a"] == 1

def test_to_csv(tmp_path):
    f = tmp_path / "test.csv"
    data = [{"name": "alice", "age": 30}, {"name": "bob", "age": 25}]
    
    Stream(data).to_csv(str(f), header=["name", "age"])
    
    content = f.read_text(encoding="utf-8").strip().splitlines()
    assert content[0] == "name,age"
    assert "alice,30" in content[1]
    assert "bob,25" in content[2]

def test_to_json(tmp_path):
    import json
    f = tmp_path / "test.json"
    data = [1, 2, 3]
    
    Stream(data).to_json(str(f))
    
    loaded = json.loads(f.read_text(encoding="utf-8"))
    assert loaded == [1, 2, 3]

def test_parallel_batch_consistency():
    """
    Verifies the fix for 'Iterable vs Iterator' in ParallelStream.batch
    """
    data = list(range(100))
    # Batch size 10 -> 10 batches of 10 items
    batches = (
        ParallelStream(data)
        .batch(10)
        .to_list()
    )
    
    assert len(batches) == 10
    assert len(batches[0]) == 10
    # Ensure no data loss
    flattened = [x for batch in batches for x in batch]
    assert sorted(flattened) == data