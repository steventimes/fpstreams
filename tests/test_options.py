import pytest
from fpstreams import Option

def test_option_of_valid():
    opt = Option.of("hello")
    assert opt.is_present()
    assert opt.or_else("default") == "hello"

def test_option_empty_handling():
    opt = Option.empty()
    assert opt.is_empty()
    assert opt.or_else("default") == "default"

def test_option_chaining():
    # value exists
    val = (
        Option.of(10)
        .map(lambda x: x * 2)
        .filter(lambda x: x > 15)
        .or_else(0)
    )
    assert val == 20

def test_option_chaining_empty():
    # fails midway
    val = (
        Option.of(10)
        .map(lambda x: x * 2)
        .filter(lambda x: x > 50)  # 20 is not > 50, so it becomes Empty
        .or_else(0)
    )
    assert val == 0