import pytest
from fpstreams.functional import pipe, curry

def test_pipe():
    # 5 -> +1 -> *2 -> str
    result = pipe(5, lambda x: x + 1, lambda x: x * 2, str)
    assert result == "12"

def test_curry():
    @curry
    def add(a, b):
        return a + b
    
    assert add(1)(2) == 3
    assert add(1, 2) == 3