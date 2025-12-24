import pytest
from fpstreams import Stream

def test_stream_of():
    # Stream.of(1, 2, 3) -> [1, 2, 3]
    result = Stream.of(1, 2, 3, 4, 5).to_list()
    assert result == [1, 2, 3, 4, 5]

def test_stream_generate():
    # Infinite stream: 1, 1, 1...
    # limit it to test it
    result = Stream.generate(lambda: 1).limit(5).to_list()
    assert result == [1, 1, 1, 1, 1]

def test_stream_iterate():
    # Stream.iterate(seed, func) -> seed, func(seed), func(func(seed))...
    # 1 -> 2 -> 4 -> 8 -> 16
    result = Stream.iterate(1, lambda x: x * 2).limit(5).to_list()
    assert result == [1, 2, 4, 8, 16]