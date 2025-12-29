import pytest
import time
from fpstreams import Stream, Result
from fpstreams.exceptions import StreamEmptyError

def test_window_by_time():
    # Mock data: (timestamp, value)
    data = [
        (100, "a"), (101, "b"), # Window 1 (start 100, len 5)
        (106, "c"),             # Window 2 (start 106)
        (120, "d")              # Window 3
    ]
    
    batches = (
        Stream(data)
        .window_by_time(time_extractor=lambda x: x[0], seconds=5)
        .to_list()
    )
    
    assert len(batches) == 3
    assert batches[0] == [(100, "a"), (101, "b")]
    assert batches[1] == [(106, "c")]

def test_flat_map_result():
    data = [
        Result.success(1),
        Result.failure(ValueError("fail")),
        Result.success(2)
    ]
    
    # Should only get [1, 2]
    result = Stream(data).flat_map_result().to_list()
    assert result == [1, 2]

def test_partition_results():
    data = [
        Result.success(1),
        Result.failure(ValueError("bad")),
        Result.success(2)
    ]
    
    ok, errs = Stream(data).partition_results()
    
    assert ok == [1, 2]
    assert len(errs) == 1
    assert str(errs[0]) == "bad"


def test_reduce_empty_is_safe():
    result = Stream([]).reduce(0, lambda a,b: a+b)
    assert result == 0

def test_min_empty_returns_empty_option():
    assert Stream([]).min().is_empty()

import os
def test_py_typed_exists():
    import fpstreams
    pkg_path = os.path.dirname(fpstreams.__file__)
    assert os.path.exists(os.path.join(pkg_path, "py.typed"))