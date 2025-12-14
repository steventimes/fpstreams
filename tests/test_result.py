import pytest
from fpstreams import Result

def test_result_success():
    res = Result.success(10)
    assert res.is_success()
    assert res.get_or_else(0) == 10

def test_result_failure_handling():
    error = ValueError("Something broke")
    res = Result.failure(error)
    
    assert res.is_failure()
    assert res.get_or_else(0) == 0

def test_result_chaining_success():
    # A success chain
    val = (
        Result.success(5)
        .map(lambda x: x * 2)
        .map(lambda x: x + 1)
        .get_or_throw()
    )
    assert val == 11

def test_result_chaining_failure():
    # an exception happens in the middle
    def risky_operation():
        raise ValueError("Wrong")

    result = (
        Result.of(risky_operation)
        .map(lambda x: x * 2)       # skip
        .map_error(lambda e: Exception("Wrapped Error"))
    )
    
    assert result.is_failure()
    with pytest.raises(Exception) as excinfo:
        result.get_or_throw()
    assert "Wrapped Error" in str(excinfo.value)