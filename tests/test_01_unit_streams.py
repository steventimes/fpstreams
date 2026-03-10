"""Unit tests: partitioned examples for Stream/ParallelStream/Collectors behavior."""

from __future__ import annotations

from fpstreams import Collectors, ParallelStream, Stream


def _double(x: int) -> int:
    return x * 2


def _div_by_three(x: int) -> bool:
    return x % 3 == 0


def test_unit_transformations_cover_empty_single_many() -> None:
    assert Stream([]).map(lambda x: x).to_list() == []
    assert Stream([5]).filter(lambda x: x > 0).to_list() == [5]
    assert Stream([1, 2, 3, 4]).flat_map(lambda x: [x, -x]).to_list() == [1, -1, 2, -2, 3, -3, 4, -4]


def test_unit_limit_skip_and_window_edges() -> None:
    assert Stream([1, 2, 3]).limit(0).to_list() == []
    assert Stream([1, 2, 3]).skip(10).to_list() == []
    assert Stream([1, 2, 3, 4]).window(2).to_list() == [[1, 2], [2, 3], [3, 4]]


def test_unit_terminal_operations() -> None:
    s = Stream([3, 1, 2])
    assert s.sorted().to_list() == [1, 2, 3]
    assert Stream([3, 1, 2]).min().or_else(-1) == 1
    assert Stream([3, 1, 2]).max().or_else(-1) == 3
    assert Stream([3, 1, 2]).sum() == 6


def test_unit_collectors_grouping_partitioning_and_columns() -> None:
    grouped = Stream(["aa", "b", "cc"]).collect(Collectors.grouping_by(len))
    assert grouped == {2: ["aa", "cc"], 1: ["b"]}

    partitioned = Stream([1, 2, 3, 4]).collect(Collectors.partitioning_by(lambda x: x % 2 == 0))
    assert partitioned[True] == [2, 4]
    assert partitioned[False] == [1, 3]

    columns = Stream([{"a": 1}, {"a": 2, "b": 3}]).collect(Collectors.to_columns())
    assert columns["a"] == [1, 2]
    assert columns["b"] == [None, 3]


def test_unit_parallel_matches_sequential_for_pure_pipeline() -> None:
    data = list(range(200))
    seq = Stream(data).map(_double).filter(_div_by_three).to_list()
    par = ParallelStream(data).map(_double).filter(_div_by_three).to_list()
    assert sorted(par) == sorted(seq)
