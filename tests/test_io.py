import pytest
import json
import csv
from fpstreams import Stream

# --- CSV & JSON ---

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

# --- Pandas & Numpy (Skipped if libraries not installed) ---

def test_to_df():
    try:
        import pandas as pd
    except ImportError:
        pytest.skip("Pandas not installed")
        
    data = [{"col1": 1, "col2": 2}, {"col1": 3, "col2": 4}]
    df = Stream(data).to_df()
    
    assert df.shape == (2, 2)
    assert df.iloc[0]["col1"] == 1

def test_to_np():
    try:
        import numpy as np
    except ImportError:
        pytest.skip("Numpy not installed")
        
    data = [1, 2, 3]
    arr = Stream(data).to_np()
    
    assert arr.sum() == 6