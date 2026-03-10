"""Model-checking style tests: exhaustive exploration of tiny state spaces."""

from __future__ import annotations

import itertools

from fpstreams import Stream


def _spec_pipeline(values: list[int], do_map: bool, do_filter: bool, skip_n: int, limit_n: int) -> list[int]:
    out = list(values)
    if do_map:
        out = [x + 1 for x in out]
    if do_filter:
        out = [x for x in out if x % 2 == 0]
    out = out[skip_n:]
    out = out[:limit_n]
    return out


def test_model_check_small_pipeline_state_space() -> None:
    universe = [0, 1, 2]
    for length in range(0, 5):
        for tup in itertools.product(universe, repeat=length):
            values = list(tup)
            for do_map, do_filter in itertools.product([False, True], repeat=2):
                for skip_n in range(0, 3):
                    for limit_n in range(0, 4):
                        stream = Stream(values)
                        if do_map:
                            stream = stream.map(lambda x: x + 1)
                        if do_filter:
                            stream = stream.filter(lambda x: x % 2 == 0)
                        out = stream.skip(skip_n).limit(limit_n).to_list()
                        assert out == _spec_pipeline(values, do_map, do_filter, skip_n, limit_n)
