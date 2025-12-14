import pytest
from fpstreams import Option

def test_option_basic():
    opt = Option.of("hello")
    assert opt.is_present()
    assert opt.or_else("default") == "hello"

def test_option_empty():
    opt = Option.empty()
    assert opt.is_empty()
    assert opt.or_else("default") == "default"

def test_option_map_chain():
    val = (
        Option.of(10)
        .map(lambda x: x * 2)
        .filter(lambda x: x > 15)
        .or_else(0)
    )
    assert val == 20