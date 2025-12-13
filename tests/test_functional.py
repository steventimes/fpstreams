import pytest
from pyflow.functional import pipe, curry

def test_pipe_flow():
    # 5 -> +1 -> *2 -> str
    result = pipe(5, lambda x: x + 1, lambda x: x * 2, str)
    assert result == "12"

def test_curry_simple():
    @curry
    def add(a, b, c):
        return a + b + c

    step1 = add(1)
    step2 = step1(2)
    result = step2(3)
    
    assert result == 6

def test_curry_mixed_calls():
    @curry
    def multiply(a, b, c, d):
        return a * b * c * d

    # multiply(2)(3, 4)(5)
    result = multiply(2)(3, 4)(5)
    assert result == 120

def test_curry_normal_call():
    @curry
    def sub(a, b):
        return a - b
    
    assert sub(10, 3) == 7