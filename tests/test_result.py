import pytest
from fpstreams import Result

def test_result_success():
    res = Result.success(10)
    assert res.is_success()
    assert res.get_or_else(0) == 10

def test_result_failure():
    res = Result.failure(ValueError("oops"))
    assert res.is_failure()
    assert res.get_or_else(0) == 0

def test_result_try_catch_wrapper():
    def fails():
        raise ValueError("boom")
    
    res = Result.of(fails)
    assert res.is_failure()
    assert "boom" in str(res.error)