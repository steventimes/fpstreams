import pytest
from fpstreams import Stream

def test_to_list_set():
    data = [1, 2, 2, 3]
    assert Stream(data).to_list() == [1, 2, 2, 3]
    assert Stream(data).to_set() == {1, 2, 3}

def test_count():
    assert Stream([1, 2, 3]).count() == 3

def test_find_first():
    opt = Stream([1, 2, 3]).find_first()
    assert opt.is_present()
    assert opt.or_else(0) == 1

def test_find_first_empty():
    opt = Stream([]).find_first()
    assert opt.is_empty()

def test_matching():
    data = [2, 4, 6]
    assert Stream(data).all_match(lambda x: x % 2 == 0) is True
    assert Stream(data).any_match(lambda x: x > 5) is True
    assert Stream(data).none_match(lambda x: x % 2 != 0) is True

def test_reduce():
    # Sum
    result = Stream([1, 2, 3, 4]).reduce(0, lambda acc, x: acc + x)
    assert result == 10

def test_reduce_string():
    result = Stream(["a", "b", "c"]).reduce("", lambda acc, x: acc + x)
    assert result == "abc"

def test_join():
    assert Stream(["a", "b", "c"]).join("-") == "a-b-c"