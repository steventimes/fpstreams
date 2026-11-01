"""Rows grouped and global aggregation, collector, and numeric contracts."""

from __future__ import annotations

import random
from collections.abc import Callable, Iterator, Mapping
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

import fpstreams

from ._tabular_support import (
    _run_inline_python as _run_inline_python,
)

PROJECT_ROOT = Path(__file__).parents[2]


def test_composite_selector_fast_path_is_exact_and_keeps_canonical_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only two or three exact direct parts may bypass their compiled selectors."""
    from fpstreams.tabular import join as join_module

    canonical_compile = join_module.compile_selector

    class FieldName(str):
        pass

    class Composite(tuple[object, ...]):
        pass

    def select_a(row: Mapping[str, int]) -> int:
        return row["a"]

    def tracking_compiler(call_log: list[object]):
        def tracked_compile(part: object):
            select = canonical_compile(part)

            def tracked_select(value: object) -> object:
                call_log.append(part)
                return select(value)

            return tracked_select

        return tracked_compile

    cases = (
        (("a", "b"), {"a": 1, "b": 2}, (1, 2), False),
        ((0, 1, 2), {0: "a", 1: "b", 2: "c"}, ("a", "b", "c"), False),
        (
            ("a", "b", "c", "d"),
            {"a": 1, "b": 2, "c": 3, "d": 4},
            (1, 2, 3, 4),
            True,
        ),
        (("nested.a", "b"), {"nested": {"a": 1}, "b": 2}, (1, 2), True),
        ((FieldName("a"), "b"), {"a": 1, "b": 2}, (1, 2), True),
        ((True, "b"), {True: 1, "b": 2}, (1, 2), True),
        (("a", 0), {"a": 1, 0: 2}, (1, 2), False),
        ((select_a, "b"), {"a": 1, "b": 2}, (1, 2), True),
        (Composite(("a", "b")), {"a": 1, "b": 2}, (1, 2), True),
    )
    for parts, row, expected, uses_fallback in cases:
        calls: list[object] = []
        monkeypatch.setattr(join_module, "compile_selector", tracking_compiler(calls))
        select_key = join_module._compile_join_selector(parts)

        assert select_key(row) == expected
        assert calls == (list(parts) if uses_fallback else [])

    class LoggedMapping(Mapping[str, int]):
        def __init__(self) -> None:
            self.values = {"a": 1, "b": 2}

        def __getitem__(self, key: str) -> int:
            return self.values[key]

        def __iter__(self) -> Iterator[str]:
            return iter(self.values)

        def __len__(self) -> int:
            return len(self.values)

    calls = []

    monkeypatch.setattr(join_module, "compile_selector", tracking_compiler(calls))
    select_mapping_key = join_module._compile_join_selector(("a", "b"))

    assert select_mapping_key(LoggedMapping()) == (1, 2)
    assert calls == ["a", "b"]


@pytest.mark.parametrize(
    ("part", "error_type", "wrapped"),
    [
        ("missing", AttributeError, True),
        ("missing", IndexError, False),
        (7, AttributeError, False),
        (7, IndexError, True),
        ("missing", KeyError, True),
        (7, TypeError, True),
    ],
)
def test_composite_direct_selector_matches_component_exception_boundaries(
    part: str | int,
    error_type: type[Exception],
    wrapped: bool,
) -> None:
    """A direct part retains the exact exception set, message, and cause of its selector."""
    from fpstreams.expressions.selectors import compile_selector
    from fpstreams.tabular.join import _compile_join_selector

    first = 101 if type(part) is str else "first"

    class CollidingKey:
        def __hash__(self) -> int:
            return hash(part)

        def __eq__(self, other: object) -> bool:
            if other == part:
                raise error_type("collision")
            return False

    def record() -> dict[object, object]:
        return {first: "first", CollidingKey(): "collision"}

    component_selectors = (compile_selector(first), compile_selector(part))

    def canonical(row: object) -> tuple[object, ...]:
        return tuple(select(row) for select in component_selectors)

    direct = _compile_join_selector((first, part))

    def capture(select: object) -> tuple[type[BaseException], str, object, object]:
        try:
            select(record())  # type: ignore[operator]
        except BaseException as error:
            cause = error.__cause__
            return (
                type(error),
                str(error),
                None if cause is None else type(cause),
                None if cause is None else str(cause),
            )
        raise AssertionError("selector unexpectedly succeeded")

    expected_type = fpstreams.SelectionError if wrapped else error_type
    canonical_error = capture(canonical)
    direct_error = capture(direct)

    assert canonical_error[0] is expected_type
    assert direct_error == canonical_error


def test_generic_multi_key_group_uses_one_composite_selector_and_keeps_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The generic in-memory loop reads exact dict keys without component callables."""
    from fpstreams.physical.relational import GroupAggregatePhysicalNode
    from fpstreams.planning.compiler import compile_query
    from fpstreams.tabular import join as join_module

    canonical_compile = join_module.compile_selector
    calls: list[object] = []

    def tracked_compile(part: object):
        select = canonical_compile(part)

        def tracked_select(value: object) -> object:
            calls.append(part)
            return select(value)

        return tracked_select

    monkeypatch.setattr(join_module, "compile_selector", tracked_compile)
    first_key = int("1000000")
    equal_key = int("1000000")
    first_band = "".join(("band", "-x"))
    equal_band = "".join(("band", "-x"))
    assert first_key == equal_key and first_key is not equal_key
    assert first_band == equal_band and first_band is not equal_band
    count = fpstreams.Aggregator(lambda: 0, lambda state, _row: state + 1)
    grouped = (
        fpstreams.rows(
            [
                {"key": first_key, "band": first_band},
                {"key": equal_key, "band": equal_band},
                {"key": 2000, "band": "band-y"},
            ]
        )
        .with_engine("python")
        .group_by("key", "band")
        .aggregate(rows=count)
    )

    physical = compile_query(grouped._flow._query("list"))
    assert isinstance(physical.root, GroupAggregatePhysicalNode)
    assert physical.root.select_key({"key": 1, "band": "x"}) == (1, "x")
    calls.clear()

    result = grouped.to_list()

    assert calls == []
    assert result == [
        {"key": first_key, "band": first_band, "rows": 2},
        {"key": 2000, "band": "band-y", "rows": 1},
    ]
    assert result[0]["key"] is first_key
    assert result[0]["band"] is first_band

    class LoggedMapping(Mapping[str, object]):
        def __getitem__(self, key: str) -> object:
            return {"key": 1, "band": "mapped"}[key]

        def __iter__(self) -> Iterator[str]:
            return iter(("key", "band"))

        def __len__(self) -> int:
            return 2

    calls.clear()
    assert fpstreams.rows([LoggedMapping()]).with_engine("python").group_by(
        "key", "band"
    ).aggregate(rows=count).to_list() == [{"key": 1, "band": "mapped", "rows": 1}]
    assert calls == ["key", "band"]


@pytest.mark.parametrize("metaclass_result", [False, True, "raise"])
@pytest.mark.parametrize("builtin_prefix", [False, True])
def test_composite_group_type_guard_does_not_compare_metaclasses(
    metaclass_result: bool | str,
    builtin_prefix: bool,
) -> None:
    """Recognizing exact keys must not call class equality or skip custom key hashes."""
    class_comparisons: list[object] = []
    key_calls: list[str] = []

    class Meta(type):
        def __eq__(cls, other: object) -> bool:
            class_comparisons.append(other)
            if metaclass_result == "raise":
                raise RuntimeError("type guard called user code")
            return bool(metaclass_result)

        __hash__ = type.__hash__

    class Key(metaclass=Meta):
        def __hash__(self) -> int:
            key_calls.append("hash")
            return 1

        def __eq__(self, other: object) -> bool:
            key_calls.append("eq")
            return self is other

    key = Key()
    records = ([(0, 2, 1)] if builtin_prefix else []) + [(key, 2, 3), (key, 2, 4)]
    query = (
        fpstreams.rows(records)
        .group_by(first=0, second=1)
        .aggregate(count=fpstreams.agg.count(), total=fpstreams.agg.sum(2))
    )
    reference = (
        fpstreams.rows(records)
        .group_by(first=0, second=1)
        .aggregate(
            count=fpstreams.Aggregator(lambda: 0, lambda state, _row: state + 1),
            total=fpstreams.agg.sum(2),
        )
    )
    expected = reference.to_list()
    expected_calls = key_calls.copy()
    key_calls.clear()
    class_comparisons.clear()

    result = query.to_list()

    assert class_comparisons == []
    assert key_calls == expected_calls
    assert result == expected
    assert result[-1]["first"] is key
    assert result[-1]["count"] == 2
    assert result[-1]["total"] == 7


def test_generic_composite_group_closes_one_shot_on_selection_and_failpoint() -> None:
    """The composite key path keeps canonical source ownership and state transitions."""
    from fpstreams.runtime.failpoints import failpoint

    count = fpstreams.Aggregator(lambda: 0, lambda state, _row: state + 1)
    events: list[str] = []

    def missing_key() -> Iterator[dict[str, int]]:
        events.append("open:missing")
        try:
            yield {"left": 1}
        finally:
            events.append("close:missing")

    with pytest.raises(fpstreams.SelectionError) as captured:
        (
            fpstreams.rows(missing_key())
            .with_engine("python")
            .group_by("left", "right")
            .aggregate(rows=count)
            .to_list()
        )
    assert isinstance(captured.value.__cause__, KeyError)
    assert events == ["open:missing", "close:missing"]

    def instrumented() -> Iterator[dict[str, int]]:
        events.append("open:failpoint")
        try:
            yield {"left": 1, "right": 2}
        finally:
            events.append("close:failpoint")

    with (
        failpoint("group.state.create.after", RuntimeError("group transition")),
        pytest.raises(RuntimeError, match="group transition"),
    ):
        (
            fpstreams.rows(instrumented())
            .with_engine("python")
            .group_by("left", "right")
            .aggregate(rows=count)
            .to_list()
        )
    assert events == [
        "open:missing",
        "close:missing",
        "open:failpoint",
        "close:failpoint",
    ]


def test_rows_min_max_fixed_lane_preserves_protocol_order() -> None:
    """Two extrema lanes keep distinct reads and canonical protocol ordering."""

    events: list[str] = []

    class Key:
        def __init__(self, label: str) -> None:
            self.label = label

        def __hash__(self) -> int:
            events.append(f"hash:{self.label}")
            return 1

        def __eq__(self, other: object) -> bool:
            events.append(f"eq:{self.label}:{getattr(other, 'label', '?')}")
            return isinstance(other, Key)

    class Value:
        def __init__(self, label: str, number: int) -> None:
            self.label = label
            self.number = number

        def __lt__(self, other: Value) -> bool:
            events.append(f"lt:{self.label}:{other.label}")
            return self.number < other.number

        def __gt__(self, other: Value) -> bool:
            events.append(f"gt:{self.label}:{other.label}")
            return self.number > other.number

    class LoggedRow(Mapping[str, object]):
        def __init__(self, label: str, key: Key, value: Value) -> None:
            self.label = label
            self.values = {"key": key, "value": value}

        def __getitem__(self, name: str) -> object:
            events.append(f"get:{self.label}:{name}")
            return self.values[name]

        def __iter__(self) -> Iterator[str]:
            return iter(self.values)

        def __len__(self) -> int:
            return len(self.values)

    first_key = Key("k1")
    equal_key = Key("k2")
    first_value = Value("v1", 5)
    second_value = Value("v2", 3)
    result = (
        fpstreams.rows(
            [
                LoggedRow("r1", first_key, first_value),
                LoggedRow("r2", equal_key, second_value),
            ]
        )
        .with_engine("python")
        .group_by("key")
        .aggregate(low=fpstreams.agg.min("value"), high=fpstreams.agg.max("value"))
        .to_list()
    )

    assert len(result) == 1
    assert result[0]["key"] is first_key
    assert result[0]["low"] is second_value
    assert result[0]["high"] is first_value
    assert events == [
        "get:r1:key",
        "hash:k1",
        "hash:k1",
        "hash:k1",
        "get:r1:value",
        "get:r1:value",
        "get:r2:key",
        "hash:k2",
        "hash:k2",
        "eq:k1:k2",
        "get:r2:value",
        "lt:v2:v1",
        "get:r2:value",
        "gt:v2:v1",
    ]


@pytest.mark.parametrize("lane_order", ["min_max", "max_min"])
def test_rows_min_max_fixed_lane_is_independent_of_keyword_order(
    lane_order: str,
) -> None:
    """Equivalent extrema requests retain results and field order."""
    aggregations = (
        {
            "low": fpstreams.agg.min(1),
            "high": fpstreams.agg.max(1),
        }
        if lane_order == "min_max"
        else {
            "high": fpstreams.agg.max(1),
            "low": fpstreams.agg.min(1),
        }
    )

    result = (
        fpstreams.rows([(1, 5), (1, 3), (2, 7)])
        .with_engine("python")
        .group_by(0)
        .aggregate(**aggregations)
        .to_list()
    )

    assert result == [
        {"key_0": 1, "low": 3, "high": 5},
        {"key_0": 2, "low": 7, "high": 7},
    ]
    assert [*result[0]] == ["key_0", *aggregations]


def test_rows_max_min_fixed_lane_preserves_requested_comparison_order() -> None:
    """The reverse keyword order must evaluate maximum before minimum on every row."""
    events: list[str] = []

    class Value:
        def __init__(self, label: str, number: int) -> None:
            self.label = label
            self.number = number

        def __lt__(self, other: Value) -> bool:
            events.append(f"lt:{self.label}:{other.label}")
            return self.number < other.number

        def __gt__(self, other: Value) -> bool:
            events.append(f"gt:{self.label}:{other.label}")
            return self.number > other.number

    class Row(Mapping[str, object]):
        def __init__(self, label: str, value: Value) -> None:
            self.label = label
            self.value = value

        def __getitem__(self, name: str) -> object:
            events.append(f"get:{self.label}:{name}")
            return 1 if name == "key" else self.value

        def __iter__(self) -> Iterator[str]:
            return iter(("key", "value"))

        def __len__(self) -> int:
            return 2

    first = Value("first", 5)
    second = Value("second", 3)
    result = (
        fpstreams.rows([Row("first", first), Row("second", second)])
        .with_engine("python")
        .group_by("key")
        .aggregate(high=fpstreams.agg.max("value"), low=fpstreams.agg.min("value"))
        .to_list()
    )

    assert result == [{"key": 1, "high": first, "low": second}]
    assert events == [
        "get:first:key",
        "get:first:value",
        "get:first:value",
        "get:second:key",
        "get:second:value",
        "gt:second:first",
        "get:second:value",
        "lt:second:first",
    ]


def test_rows_min_max_fixed_lane_closes_on_error_and_failpoint() -> None:
    """Comparison failure and state failpoints stop later reads and close the source."""
    from fpstreams.runtime.failpoints import failpoint

    events: list[str] = []

    class Value:
        def __init__(self, label: str, *, fail: bool = False) -> None:
            self.label = label
            self.fail = fail

        def __lt__(self, other: Value) -> bool:
            events.append(f"lt:{self.label}:{other.label}")
            if self.fail:
                raise RuntimeError("min comparison")
            return False

        def __gt__(self, other: Value) -> bool:
            events.append(f"gt:{self.label}:{other.label}")
            return False

    class LoggedRow(Mapping[str, object]):
        def __init__(self, label: str, value: Value) -> None:
            self.label = label
            self.value = value

        def __getitem__(self, name: str) -> object:
            events.append(f"get:{self.label}:{name}")
            return 1 if name == "key" else self.value

        def __iter__(self) -> Iterator[str]:
            return iter(("key", "value"))

        def __len__(self) -> int:
            return 2

    def failing_comparison() -> Iterator[Mapping[str, object]]:
        events.append("open:comparison")
        try:
            yield LoggedRow("first", Value("first"))
            yield LoggedRow("second", Value("second", fail=True))
        finally:
            events.append("close:comparison")

    with pytest.raises(RuntimeError, match="min comparison"):
        (
            fpstreams.rows(failing_comparison())
            .with_engine("python")
            .group_by("key")
            .aggregate(low=fpstreams.agg.min("value"), high=fpstreams.agg.max("value"))
            .to_list()
        )
    assert events == [
        "open:comparison",
        "get:first:key",
        "get:first:value",
        "get:first:value",
        "get:second:key",
        "get:second:value",
        "lt:second:first",
        "close:comparison",
    ]

    events.clear()

    def instrumented() -> Iterator[Mapping[str, object]]:
        events.append("open:failpoint")
        try:
            yield LoggedRow("instrumented", Value("unused"))
        finally:
            events.append("close:failpoint")

    with (
        failpoint("group.state.create.after", RuntimeError("group transition")),
        pytest.raises(RuntimeError, match="group transition"),
    ):
        (
            fpstreams.rows(instrumented())
            .with_engine("python")
            .group_by("key")
            .aggregate(low=fpstreams.agg.min("value"), high=fpstreams.agg.max("value"))
            .to_list()
        )
    assert events == [
        "open:failpoint",
        "get:instrumented:key",
        "close:failpoint",
    ]


@pytest.mark.parametrize("reverse", [False, True])
def test_rows_count_distinct_compares_mixed_hashability(reverse):
    values = [{1}, frozenset({1})]
    if reverse:
        values.reverse()
    records = [{"team": "a", "tags": value} for value in values]
    result = (
        fpstreams.rows(records)
        .group_by("team")
        .aggregate(distinct_tags=fpstreams.agg.count_distinct("tags"))
        .to_list()
    )
    assert result == [{"team": "a", "distinct_tags": 1}]


def test_rows_computes_stable_online_statistics_and_distinct_counts() -> None:
    records = [
        {"team": "a", "score": 1_000_000_000_001.0, "tags": ["x"]},
        {"team": "a", "score": 1_000_000_000_002.0, "tags": ["x"]},
        {"team": "a", "score": 1_000_000_000_003.0, "tags": ["y"]},
        {"team": "b", "score": 7.0, "tags": ["z"]},
    ]

    summary = (
        fpstreams.rows(records)
        .group_by("team")
        .aggregate(
            sample_variance=fpstreams.agg.variance("score"),
            population_variance=fpstreams.agg.variance("score", ddof=0),
            sample_std=fpstreams.agg.std("score"),
            distinct_tags=fpstreams.agg.count_distinct("tags"),
        )
        .to_list()
    )

    assert summary == [
        {
            "team": "a",
            "sample_variance": 1.0,
            "population_variance": pytest.approx(2 / 3),
            "sample_std": 1.0,
            "distinct_tags": 2,
        },
        {
            "team": "b",
            "sample_variance": None,
            "population_variance": 0.0,
            "sample_std": None,
            "distinct_tags": 1,
        },
    ]
    with pytest.raises(ValueError, match="ddof"):
        fpstreams.agg.variance("score", ddof=-1)


def test_grouped_rows_initializes_aggregations_once_per_group() -> None:
    initializer_calls = 0

    def initializer() -> int:
        nonlocal initializer_calls
        initializer_calls += 1
        return 0

    total = fpstreams.Aggregator(
        initializer,
        lambda state, row: state + row["score"],
    )
    result = (
        fpstreams.rows(
            [
                {"team": "a", "score": 1},
                {"team": "a", "score": 2},
                {"team": "b", "score": 3},
            ]
        )
        .group_by("team")
        .aggregate(total=total)
        .to_list()
    )

    assert result == [{"team": "a", "total": 3}, {"team": "b", "total": 3}]
    assert initializer_calls == 2


@pytest.mark.parametrize("position", ["key", "value"])
@pytest.mark.parametrize(
    ("selector", "error_type", "wrapped"),
    [
        (0, AttributeError, False),
        (0, IndexError, True),
        ("field", IndexError, False),
        ("field", AttributeError, True),
    ],
)
def test_selected_group_sum_keeps_selector_exception_boundaries(
    position: str,
    selector: str | int,
    error_type: type[Exception],
    wrapped: bool,
) -> None:
    """Direct lookup translates exactly the exception set of each canonical selector."""

    class CollidingKey:
        def __hash__(self) -> int:
            return hash(selector)

        def __eq__(self, _other: object) -> bool:
            raise error_type("collision equality")

    if position == "key":
        record = {CollidingKey(): "unrelated", "safe_value": 4}
        grouped = (
            fpstreams.rows([record])
            .group_by(key=selector)
            .aggregate(total=fpstreams.agg.sum("safe_value"))
        )
    else:
        record = {"safe_key": "a", CollidingKey(): "unrelated"}
        grouped = (
            fpstreams.rows([record])
            .group_by("safe_key")
            .aggregate(total=fpstreams.agg.sum(selector))
        )

    expected_error = fpstreams.SelectionError if wrapped else error_type
    with pytest.raises(
        expected_error, match="collision equality" if not wrapped else None
    ) as captured:
        grouped.to_list()
    if wrapped:
        assert isinstance(captured.value.__cause__, error_type)
    else:
        assert captured.value.__cause__ is None


def test_selected_group_sum_matches_builtin_and_callable_oracles() -> None:
    """Both engines preserve ordered dict grouping across a deterministic random sample."""
    generator = random.Random(0xF57EA)
    records = [
        {"key": generator.randrange(41), "value": generator.randrange(-100, 101)}
        for _ in range(4_000)
    ]
    totals: dict[int, int] = {}
    for record in records:
        key = record["key"]
        totals[key] = totals.get(key, 0) + record["value"]
    expected = [{"key": key, "total": total} for key, total in totals.items()]

    callable_oracle = (
        fpstreams.rows(records)
        .with_engine("python")
        .group_by(key=lambda row: row["key"])
        .aggregate(total=fpstreams.agg.sum(lambda row: row["value"]))
        .to_list()
    )
    assert callable_oracle == expected
    for engine in ("auto", "python"):
        actual = (
            fpstreams.rows(records)
            .with_engine(engine)
            .group_by("key")
            .aggregate(total=fpstreams.agg.sum("value"))
            .to_list()
        )
        assert actual == expected
        assert (
            fpstreams.rows([])
            .with_engine(engine)
            .group_by("key")
            .aggregate(total=fpstreams.agg.sum("value"))
            .to_list()
            == []
        )


@pytest.mark.parametrize("scenario", ["pair_reversed", "group_enumerate"])
def test_closed_kernels_reject_preimport_builtin_pollution(scenario: str) -> None:
    """A polluted implementation primitive must keep execution on Python semantics."""
    if scenario == "pair_reversed":
        body = """
import builtins
import numpy  # Load optional dependency internals before applying the narrow pollution.

builtins.reversed = lambda values: values
import fpstreams
from fpstreams.planning.pairs.i64_expression import lower_pair_i64_row_filter

values = [(index, index % 4) for index in range(128)]
expression = (fpstreams.col(1) - 1) == 1
assert lower_pair_i64_row_filter(expression) is None
automatic = fpstreams.pairs(values).filter_pairs(expression).to_dict(on_duplicate="last")
canonical = (
    fpstreams.pairs(values)
    .filter_pairs(expression)
    .with_engine("python")
    .to_dict(on_duplicate="last")
)
assert automatic == canonical
"""
    else:
        body = """
import builtins
import numpy as np

original_enumerate = builtins.enumerate

def selective_enumerate(values, start=0):
    if (
        type(values) is tuple
        and values
        and type(values[0]).__module__.startswith("fpstreams.collecting.")
    ):
        return original_enumerate((), start)
    return original_enumerate(values, start)

builtins.enumerate = selective_enumerate
import fpstreams

query = (
    fpstreams.rows.from_numpy(
        np.asarray([[1, 10], [1, 20], [2, 30]], dtype=np.int64),
        columns=("key", "value"),
    )
    .group_by("key")
    .aggregate(rows=fpstreams.agg.count(), total=fpstreams.agg.sum("value"))
)
automatic = query.run_with_report("to_list")
canonical = query.with_engine("python").to_list()
assert automatic.report.strategy == "planned:python"
assert automatic.value == canonical == [
    {"key": 1, "rows": 0, "total": 0},
    {"key": 2, "rows": 0, "total": 0},
]
"""
    completed = _run_inline_python(body, cwd=PROJECT_ROOT)

    assert completed.returncode == 0, completed.stderr


def test_selected_tuple_group_sum_native_decline_replays_protocol_rows_once() -> None:
    """A late exact-shape decline adds no selector, hash, or addition observations."""

    def evaluate(engine: str) -> tuple[list[dict[str, object]], list[str]]:
        events: list[str] = []

        class Integer(int):
            def __hash__(self) -> int:
                events.append("hash")
                return int.__hash__(self)

        class Number:
            def __init__(self, value: int) -> None:
                self.value = value

            def __radd__(self, left: object) -> int:
                events.append(f"radd:{left!r}+{self.value}")
                assert type(left) is int
                return left + self.value

        class Row(tuple[object, ...]):
            def __getitem__(self, index: object) -> object:
                events.append(f"get:{index!r}")
                return super().__getitem__(index)  # type: ignore[index]

        query = (
            fpstreams.rows([(1, 2), Row((Integer(1), Number(3)))])
            .with_engine(engine)
            .group_by(key=0)
            .aggregate(total=fpstreams.agg.sum(1))
        )
        return query.to_list(), events

    expected, expected_events = evaluate("python")
    actual, actual_events = evaluate("auto")

    assert actual == expected == [{"key": 1, "total": 5}]
    assert (
        actual_events
        == expected_events
        == [
            "get:0",
            "hash",
            "hash",
            "get:1",
            "radd:2+3",
        ]
    )


def test_selected_tuple_group_sum_short_row_replays_and_closes_once() -> None:
    """A late short exact tuple enters one canonical selection-error lifecycle."""
    from fpstreams.planning.source import Source, SourceCapabilities
    from fpstreams.streams.flow import Flow

    retained = [(1, 2), (1,)]
    events: list[str] = []

    def open_rows() -> Iterator[tuple[int, ...]]:
        events.append("open")
        try:
            for index, row in enumerate(retained):
                events.append(f"pull:{index}")
                yield row
        finally:
            events.append("close")

    source = Source(
        open_rows,
        SourceCapabilities(reiterable=True, exact_size=2, ordered=True),
        native_data=retained,
    )
    grouped = fpstreams.Rows(Flow(source)).group_by(key=0).aggregate(total=fpstreams.agg.sum(1))

    with pytest.raises(fpstreams.SelectionError) as captured:
        grouped.to_list()

    assert isinstance(captured.value.__cause__, IndexError)
    assert events == ["open", "pull:0", "pull:1", "close"]


def test_selected_tuple_group_sum_rejects_nonexact_index_selectors_before_native(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bool and int-subclass selectors retain Python selection and report policy."""
    from fpstreams import _native

    class Index(int):
        calls = 0

        def __index__(self) -> int:
            type(self).calls += 1
            return int(self)

    def unexpected_native(*_arguments: object) -> object:
        raise AssertionError("a nonexact selector entered fixed-index native grouping")

    monkeypatch.setattr(_native, "group_sum_i64_rows_v1", unexpected_native)
    candidates = (
        (True, 0, [{"key": 1, "total": 30}]),
        (Index(0), Index(1), [{"key": 10, "total": 1}, {"key": 20, "total": 1}]),
    )
    for key_selector, value_selector, expected in candidates:
        query = (
            fpstreams.rows(((10, 1), (20, 1)))
            .group_by(key=key_selector)
            .aggregate(total=fpstreams.agg.sum(value_selector))
        )
        execution = query.run_with_report("to_list")
        assert execution.value == expected
        assert execution.report.strategy == "planned:python"
    assert Index.calls == 0


def test_selected_group_sum_rechecks_the_exact_fast_shape_per_row() -> None:
    """A built-in first key never weakens later mapping or custom-key protocols."""
    events: list[str] = []

    class CustomKey:
        def __hash__(self) -> int:
            events.append("hash")
            return 101

    class LoggedMapping(Mapping[str, object]):
        def __getitem__(self, key: str) -> object:
            events.append(f"mapping:{key}")
            return {"key": "safe", "value": 4}[key]

        def __iter__(self) -> Iterator[str]:
            return iter(("key", "value"))

        def __len__(self) -> int:
            return 2

    custom = CustomKey()
    result = (
        fpstreams.rows(
            [
                {"key": "safe", "value": 1},
                {"key": custom, "value": 2},
                {"key": custom, "value": 3},
                LoggedMapping(),
            ]
        )
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
        .to_list()
    )

    assert result[0] == {"key": "safe", "total": 5}
    assert result[1]["key"] is custom
    assert result[1]["total"] == 5
    # First custom insertion performs explicit hash, lookup, and insertion;
    # its second row performs explicit hash and lookup, exactly as before.
    assert events == ["hash"] * 5 + ["mapping:key", "mapping:value"]


def test_selected_group_sum_preserves_fast_lookup_collision_type_error() -> None:
    """A later built-in key preserves the original custom equality failure."""

    failure = TypeError("equality-owned")

    class CollidingKey:
        def __hash__(self) -> int:
            return hash(1)

        def __eq__(self, other: object) -> bool:
            if other == 1:
                raise failure
            return self is other

    records = [
        {"key": 0, "value": 1},
        {"key": CollidingKey(), "value": 2},
        {"key": 1, "value": 3},
    ]

    with pytest.raises(TypeError, match="equality-owned") as captured:
        fpstreams.rows(records).group_by("key").aggregate(
            total=fpstreams.agg.sum("value")
        ).to_list()
    assert captured.value is failure


@pytest.mark.parametrize("engine", ["python", "auto"])
@pytest.mark.parametrize("collectors", [1, 2])
def test_group_by_preserves_key_equality_failure(engine: str, collectors: int) -> None:
    failure = TypeError("group equality failed")
    closed: list[bool] = []

    class Key:
        def __hash__(self) -> int:
            return 0

        def __eq__(self, other: object) -> bool:
            raise failure

    def source():
        try:
            yield {"key": Key(), "value": 1}
            yield {"key": Key(), "value": 2}
        finally:
            closed.append(True)

    aggregations = {"total": fpstreams.agg.sum("value")}
    if collectors == 2:
        aggregations["count"] = fpstreams.agg.count()
    query = fpstreams.rows(source()).with_engine(engine).group_by("key").aggregate(**aggregations)
    with pytest.raises(TypeError) as captured:
        query.to_list()
    assert captured.value is failure
    assert closed == [True]


def test_selected_group_sum_preserves_mapping_addition_and_failure_order() -> None:
    """Fallback protocols, ``0 + value``, dotted paths, and close remain observable."""
    events: list[str] = []

    class LoggedDict(dict[str, object]):
        def __getitem__(self, key: str) -> object:
            events.append(f"dict:{key}")
            return super().__getitem__(key)

    class LoggedMapping(Mapping[str, object]):
        def __init__(self, **values: object) -> None:
            self.values = values

        def __getitem__(self, key: str) -> object:
            events.append(f"mapping:{key}")
            return self.values[key]

        def __iter__(self) -> Iterator[str]:
            return iter(self.values)

        def __len__(self) -> int:
            return len(self.values)

    class StatefulNumber:
        def __init__(self, value: int) -> None:
            self.value = value

        def __radd__(self, left: object) -> StatefulNumber:
            events.append(f"radd:{left!r}+{self.value}")
            assert left == 0
            return StatefulNumber(self.value)

        def __add__(self, right: object) -> StatefulNumber:
            assert isinstance(right, StatefulNumber)
            events.append(f"add:{self.value}+{right.value}")
            return StatefulNumber(self.value + right.value)

    result = (
        fpstreams.rows(
            [
                LoggedDict(key="a", value=StatefulNumber(2)),
                LoggedMapping(key="a", value=StatefulNumber(3)),
            ]
        )
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
        .to_list()
    )
    assert result[0]["key"] == "a"
    assert result[0]["total"].value == 5
    assert events == [
        "dict:key",
        "dict:value",
        "radd:0+2",
        "mapping:key",
        "mapping:value",
        "add:2+3",
    ]
    assert fpstreams.rows([{"outer": {"key": "x"}, "payload": {"value": 4}}]).group_by(
        "outer.key"
    ).aggregate(total=fpstreams.agg.sum("payload.value")).to_list() == [{"key": "x", "total": 4}]

    closed: list[bool] = []

    class ExplodingNumber:
        def __radd__(self, _left: object) -> object:
            raise RuntimeError("addition failed")

    def values() -> Iterator[dict[str, object]]:
        try:
            yield {"key": "x", "value": ExplodingNumber()}
        finally:
            closed.append(True)

    with pytest.raises(RuntimeError, match="addition failed"):
        fpstreams.rows(values()).group_by("key").aggregate(
            total=fpstreams.agg.sum("value")
        ).to_list()
    assert closed == [True]

    lookup_events: list[str] = []

    class UnhashableKeyRow(dict[str, object]):
        def __getitem__(self, key: str) -> object:
            lookup_events.append(key)
            return super().__getitem__(key)

    with pytest.raises(TypeError, match="hashable"):
        fpstreams.rows([UnhashableKeyRow(key=[], value=1)]).group_by("key").aggregate(
            total=fpstreams.agg.sum("value")
        ).to_list()
    assert lookup_events == ["key"]

    nan_result = (
        fpstreams.rows([{"key": "x", "value": float("nan")}])
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
        .first()["total"]
    )
    assert nan_result != nan_result
    with pytest.raises(TypeError):
        fpstreams.rows([{"key": "x", "value": None}]).group_by("key").aggregate(
            total=fpstreams.agg.sum("value")
        ).to_list()


def test_rows_names_computed_group_keys_and_supports_boolean_aggregations() -> None:
    records = [
        {"team": "a", "amount": 10, "paid": True},
        {"team": "a", "amount": 15, "paid": False},
        {"team": "a", "amount": 30, "paid": True},
        {"team": "b", "amount": 5, "paid": True},
    ]
    result = (
        fpstreams.rows(records)
        .group_by("team", band=lambda row: "high" if row["amount"] >= 20 else "low")
        .aggregate(
            rows=fpstreams.agg.count(),
            paid=fpstreams.agg.count_where("paid"),
            any_large=fpstreams.agg.any(lambda row: row["amount"] >= 30),
            all_positive=fpstreams.agg.all(lambda row: row["amount"] > 0),
        )
        .to_list()
    )

    assert result == [
        {
            "team": "a",
            "band": "low",
            "rows": 2,
            "paid": 1,
            "any_large": False,
            "all_positive": True,
        },
        {
            "team": "a",
            "band": "high",
            "rows": 1,
            "paid": 1,
            "any_large": True,
            "all_positive": True,
        },
        {
            "team": "b",
            "band": "low",
            "rows": 1,
            "paid": 1,
            "any_large": False,
            "all_positive": True,
        },
    ]
    assert fpstreams.rows([]).aggregate(
        any_paid=fpstreams.agg.any("paid"),
        all_paid=fpstreams.agg.all("paid"),
        paid=fpstreams.agg.count_where("paid"),
    ).first() == {"any_paid": False, "all_paid": True, "paid": 0}
    with pytest.raises(fpstreams.DuplicateKeyError):
        fpstreams.rows(records).group_by("team", team="paid")
    with pytest.raises(TypeError, match="hashable"):
        fpstreams.rows([{"tags": ["x"]}]).group_by("tags").aggregate(
            rows=fpstreams.agg.count()
        ).to_list()


def test_rows_global_aggregate_is_chainable_and_handles_empty_input() -> None:
    summary = fpstreams.rows(
        [
            {"score": 1.0, "tag": ["x"]},
            {"score": 2.0, "tag": ["x"]},
            {"score": 3.0, "tag": ["y"]},
        ]
    ).aggregate(
        rows=fpstreams.agg.count(),
        total=fpstreams.agg.sum("score"),
        variance=fpstreams.agg.variance("score"),
        tags=fpstreams.agg.count_distinct("tag"),
    )

    expected = [{"rows": 3, "total": 6.0, "variance": 1.0, "tags": 2}]
    assert summary.to_list() == expected
    assert summary.to_list() == expected
    assert fpstreams.rows([]).aggregate(
        rows=fpstreams.agg.count(), mean=fpstreams.agg.mean("score")
    ).to_list() == [{"rows": 0, "mean": None}]
    with pytest.raises(ValueError, match="aggregate"):
        fpstreams.rows([]).aggregate()


def test_rows_rejects_duplicate_derived_group_and_pivot_keys() -> None:
    records = [{"left": {"id": 1}, "right": {"id": 2}, "value": 3}]

    with pytest.raises(fpstreams.DuplicateKeyError):
        fpstreams.rows(records).group_by("left.id", "right.id").aggregate(
            total=fpstreams.agg.sum("value")
        )

    with pytest.raises(fpstreams.DuplicateKeyError):
        fpstreams.rows(records).pivot(
            index=("left.id", "right.id"),
            columns="value",
            values="value",
        )


@pytest.mark.parametrize("source_kind", ["list", "tuple", "arrow", "record_batch"])
def test_direct_exact_size_global_count_never_opens_its_source(
    monkeypatch: pytest.MonkeyPatch, source_kind: str
) -> None:
    """A direct project-owned count may use a trusted exact source cardinality."""
    from fpstreams.physical.relational import GlobalAggregatePhysicalNode, SourcePhysicalNode
    from fpstreams.planning.compiler import compile_query
    from fpstreams.planning.source import Source

    records = [{"value": 1}, {"value": 2}, {"value": 3}]
    if source_kind == "list":
        source = fpstreams.rows(records)
    elif source_kind == "tuple":
        source = fpstreams.rows(tuple(records))
    elif source_kind == "arrow":
        source = fpstreams.rows.from_arrow(pa.table({"value": [1, 2, 3]}))
    else:
        source = fpstreams.rows.from_arrow(pa.record_batch({"value": [1, 2, 3]}))
    aggregated = source.aggregate(rows=fpstreams.agg.count())
    physical = compile_query(aggregated._flow._query("list"))
    assert isinstance(physical.root, GlobalAggregatePhysicalNode)
    assert isinstance(physical.root.input, SourcePhysicalNode)
    assert physical.root.exact_count_name == "rows"
    relation = aggregated._flow.explain("list").to_dict()["relations"]
    assert relation["candidate"] == "exact_size"
    assert relation["guarded"] is True
    input_source = physical.root.input.source
    open_source = Source.open

    def reject_source(candidate: Source[object]) -> Iterator[object]:
        if candidate is input_source:
            raise AssertionError("exact global count must not open its source")
        return open_source(candidate)

    monkeypatch.setattr(Source, "open", reject_source)

    assert aggregated.to_list() == [{"rows": 3}]


@pytest.mark.parametrize("source_kind", ["list", "tuple"])
def test_direct_native_record_global_sum_never_opens_python_rows(
    monkeypatch: pytest.MonkeyPatch, source_kind: str
) -> None:
    """A direct exact-record i64 sum should reduce its retained source in native code."""
    from fpstreams.physical.relational import GlobalAggregatePhysicalNode, SourcePhysicalNode
    from fpstreams.planning.compiler import compile_query
    from fpstreams.planning.source import Source

    records = [
        {"gross_total": 2, "label": "a"},
        {"gross_total": 3, "label": "b"},
        {"gross_total": -1, "label": "c"},
    ]
    source = fpstreams.rows(records if source_kind == "list" else tuple(records))
    aggregated = source.aggregate(total=fpstreams.agg.sum("gross_total"))
    physical = compile_query(aggregated._flow._query("list"))

    assert isinstance(physical.root, GlobalAggregatePhysicalNode)
    assert isinstance(physical.root.input, SourcePhysicalNode)
    assert physical.root.native_record_i64_sum is not None
    relation = aggregated._flow.explain("list").to_dict()["relations"]
    assert relation["candidate"] == "native_reduce"
    assert relation["guarded"] is True
    input_source = physical.root.input.source
    open_source = Source.open

    def reject_row_source(candidate: Source[object]) -> Iterator[object]:
        if candidate is input_source:
            raise AssertionError("native global sum must not open Python rows")
        return open_source(candidate)

    monkeypatch.setattr(Source, "open", reject_row_source)

    assert aggregated.to_list() == [{"total": 4}]


@pytest.mark.parametrize("source_kind", ["list", "tuple"])
@pytest.mark.parametrize("row_kind", ["dict", "tuple"])
def test_direct_native_multi_global_aggregate_never_opens_python_rows(
    monkeypatch: pytest.MonkeyPatch,
    source_kind: str,
    row_kind: str,
) -> None:
    """Closed exact-i64 lanes should reduce one retained source in one native scan."""
    from fpstreams.physical.relational import GlobalAggregatePhysicalNode, SourcePhysicalNode
    from fpstreams.planning.compiler import compile_query
    from fpstreams.planning.source import Source

    if row_kind == "dict":
        records: list[object] = [
            {"left": 2, "right": 9},
            {"left": 3, "right": -4},
            {"left": -1, "right": 7},
        ] * 64
        sum_field = bytes((108, 101, 102, 116)).decode()
        max_field = bytes((108, 101, 102, 116)).decode()
        assert sum_field == max_field == "left" and sum_field is not max_field
        total = fpstreams.agg.sum(sum_field)
        low = fpstreams.agg.min("right")
        high = fpstreams.agg.max(max_field)
    else:
        records = [(2, 9), (3, -4), (-1, 7)] * 64
        total = fpstreams.agg.sum(0)
        low = fpstreams.agg.min(1)
        high = fpstreams.agg.max(0)
    retained = records if source_kind == "list" else tuple(records)
    aggregated = fpstreams.rows(retained).aggregate(
        rows=fpstreams.agg.count(),
        total=total,
        low=low,
        high=high,
    )
    physical = compile_query(aggregated._flow._query("list"))

    assert isinstance(physical.root, GlobalAggregatePhysicalNode)
    assert isinstance(physical.root.input, SourcePhysicalNode)
    marker = physical.root.native_multi_i64
    assert marker is not None
    assert marker.row_kind == row_kind
    assert tuple(lane.kind for lane in marker.lanes) == ("count", "sum", "min", "max")
    assert physical.root.native_record_i64_sum is None
    relation = aggregated.explain("list").to_dict()["relations"]
    assert relation["candidate"] == "native_multi_reduce"
    assert relation["guarded"] is True
    input_source = physical.root.input.source
    open_source = Source.open

    def reject_row_source(candidate: Source[object]) -> Iterator[object]:
        if candidate is input_source:
            raise AssertionError("native global aggregate must not open Python rows")
        return open_source(candidate)

    monkeypatch.setattr(Source, "open", reject_row_source)

    execution = aggregated.run_with_report("to_list")

    assert execution.value == [{"rows": 192, "total": 256, "low": -4, "high": 3}]
    assert execution.report.compiler_engine == "python"
    assert execution.report.strategy == "rust_direct"


@pytest.mark.parametrize("size", [127, 128])
def test_native_multi_global_aggregate_uses_a_measured_small_source_threshold(size: int) -> None:
    """Auto planning pays the native fixed cost only after its generic crossover."""
    from fpstreams.physical.relational import GlobalAggregatePhysicalNode
    from fpstreams.planning.compiler import compile_query

    records = [(index, -index) for index in range(size)]
    aggregated = fpstreams.rows(records).aggregate(
        rows=fpstreams.agg.count(),
        total=fpstreams.agg.sum(0),
        low=fpstreams.agg.min(1),
    )
    physical = compile_query(aggregated._flow._query("list"))

    assert isinstance(physical.root, GlobalAggregatePhysicalNode)
    assert (physical.root.native_multi_i64 is not None) is (size == 128)
    assert aggregated.to_list() == [{"rows": size, "total": sum(range(size)), "low": -(size - 1)}]


def test_native_multi_global_aggregate_abi_preserves_closed_lane_semantics() -> None:
    """The optional ABI keeps lane order, wide sums, extrema identity, and empty values."""
    from fpstreams import _native

    first_low = int("-1000")
    equal_low = int("-1000")
    assert first_low is not equal_low
    maximum = 2**63 - 1
    dict_lanes = (
        (0, None, "rows"),
        (1, "amount", "total"),
        (2, "low", "low"),
        (3, "high", "high"),
    )
    dict_result = _native.global_multi_i64_dict_rows_v1(
        [
            {"amount": maximum, "low": first_low, "high": -5},
            {"amount": maximum, "low": equal_low, "high": 8},
        ],
        dict_lanes,
    )
    assert dict_result == {
        "rows": 2,
        "total": maximum * 2,
        "low": first_low,
        "high": 8,
    }
    assert dict_result["low"] is first_low
    assert _native.global_multi_i64_dict_rows_v1([], dict_lanes) == {
        "rows": 0,
        "total": 0,
        "low": None,
        "high": None,
    }

    tuple_lanes = (
        (0, None, "rows"),
        (1, -2, "total"),
        (2, -1, "low"),
        (3, 0, "high"),
    )
    assert _native.global_multi_i64_rows_v1([(2, 9), (3, -4), (-1, 7)], tuple_lanes) == {
        "rows": 3,
        "total": 4,
        "low": -4,
        "high": 3,
    }
    assert _native.global_multi_i64_rows_v1((), tuple_lanes) == {
        "rows": 0,
        "total": 0,
        "low": None,
        "high": None,
    }


def test_native_multi_global_aggregate_abi_declines_without_protocol_dispatch() -> None:
    """Speculative exact-record rejection must not call user equality or integer hooks."""
    from fpstreams import _native

    class Trap:
        calls = 0

        def __hash__(self) -> int:
            return hash("value")

        def __eq__(self, _other: object) -> bool:
            type(self).calls += 1
            raise AssertionError("speculative lookup touched custom equality")

    class Record(dict[str, object]):
        pass

    dict_lanes = ((0, None, "rows"), (1, "value", "total"))
    trap = Trap()
    assert _native.global_multi_i64_dict_rows_v1([{trap: 1}], dict_lanes) is None
    assert Trap.calls == 0
    for source in (
        [{"value": True}],
        [{"value": 2**100}],
        [{"missing": 1}],
        [Record(value=1)],
    ):
        assert _native.global_multi_i64_dict_rows_v1(source, dict_lanes) is None

    class Integer(int):
        pass

    tuple_lanes = ((0, None, "rows"), (1, 0, "total"))
    for source in ([(True,)], [(2**100,)], [(Integer(1),)], [object()]):
        assert _native.global_multi_i64_rows_v1(source, tuple_lanes) is None


def test_native_multi_global_aggregate_decline_reopens_one_canonical_scan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A native type decline leaves the retained input untouched for the Python result."""
    from fpstreams import _native

    records = [{"value": True}, {"value": 2**100}] * 64
    calls: list[tuple[object, object]] = []

    def decline(source: object, lanes: object) -> None:
        calls.append((source, lanes))
        return None

    monkeypatch.setattr(
        _native,
        "global_multi_i64_dict_rows_v1",
        decline,
        raising=False,
    )

    assert fpstreams.rows(records).aggregate(
        rows=fpstreams.agg.count(),
        total=fpstreams.agg.sum("value"),
        low=fpstreams.agg.min("value"),
    ).to_list() == [{"rows": 128, "total": 64 * (1 + 2**100), "low": True}]
    assert len(calls) == 1
    assert calls[0][0] is records


def test_native_multi_global_aggregate_keeps_the_single_sum_abi(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The narrower established single-sum kernel remains the preferred control path."""
    from fpstreams import _native

    records = [{"value": 2}, {"value": 3}]
    calls: list[object] = []
    old_kernel = _native.global_sum_i64_dict_rows_v1

    def tracked_single(source: object, field: str) -> int | None:
        calls.append(source)
        return old_kernel(source, field)

    def unexpected_multi(_source: object, _lanes: object) -> object:
        raise AssertionError("single sum reached the generic global aggregate kernel")

    monkeypatch.setattr(_native, "global_sum_i64_dict_rows_v1", tracked_single)
    monkeypatch.setattr(
        _native,
        "global_multi_i64_dict_rows_v1",
        unexpected_multi,
        raising=False,
    )

    assert fpstreams.rows(records).aggregate(total=fpstreams.agg.sum("value")).to_list() == [
        {"total": 5}
    ]
    assert calls == [records]


def test_native_multi_global_aggregate_revalidates_live_collectors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A compiled shortcut must yield to a collector whose transition code changed."""
    from fpstreams import _native
    from fpstreams.execution.physical import execute_physical
    from fpstreams.planning.compiler import compile_query

    rows = fpstreams.agg.count()
    total = fpstreams.agg.sum("value")
    aggregated = fpstreams.rows([{"value": 2}, {"value": 3}] * 64).aggregate(
        rows=rows,
        total=total,
    )
    physical = compile_query(aggregated._flow._query("list"))

    def step_factory() -> Callable[[int, dict[str, int]], int]:
        def select(row: dict[str, int]) -> int:
            return row["value"]

        def step(current: int, row: dict[str, int]) -> int:
            return current + select(row) * 9

        return step

    def unexpected_kernel(_source: object, _lanes: object) -> object:
        raise AssertionError("a stale global aggregate reached native execution")

    monkeypatch.setattr(total.step, "__code__", step_factory().__code__)
    monkeypatch.setattr(_native, "global_multi_i64_dict_rows_v1", unexpected_kernel)

    assert list(execute_physical(physical)) == [{"rows": 128, "total": 2_880}]


def test_python_global_aggregate_observes_done_mutated_on_first_pull() -> None:
    """The no-early-stop fast path must notice a done hook installed by its source."""
    events: list[str] = []
    total = fpstreams.agg.sum("value")

    def replacement_done(state: int) -> bool:
        events.append(f"done:{state}")
        return state >= 2

    def records() -> Iterator[dict[str, int]]:
        try:
            events.append("mutate:done")
            object.__setattr__(total, "done", replacement_done)
            events.append("pull:2")
            yield {"value": 2}
            events.append("pull:3")
            yield {"value": 3}
        finally:
            events.append("close")

    aggregated = fpstreams.rows(records()).with_engine("python").aggregate(total=total)

    assert aggregated.to_list() == [{"total": 2}]
    assert events == ["mutate:done", "pull:2", "done:2", "close"]


def test_python_global_aggregate_keeps_primary_error_when_close_fails() -> None:
    """Iterator cleanup must annotate rather than replace an active collector failure."""

    class Values(Iterator[dict[str, int]]):
        def __init__(self) -> None:
            self.pulled = False
            self.close_calls = 0

        def __iter__(self) -> Values:
            return self

        def __next__(self) -> dict[str, int]:
            if self.pulled:
                raise StopIteration
            self.pulled = True
            return {"value": 2}

        def close(self) -> None:
            self.close_calls += 1
            raise OSError("secondary close")

    def failing_step(_state: int, _row: object) -> int:
        raise ValueError("primary step")

    values = Values()
    aggregated = (
        fpstreams.rows(values)
        .with_engine("python")
        .aggregate(total=fpstreams.Aggregator(lambda: 0, failing_step))
    )

    with pytest.raises(ValueError, match="primary step") as captured:
        aggregated.to_list()
    assert captured.value.__notes__ == ["cleanup failed with OSError: secondary close"]
    assert values.close_calls == 1


def test_native_multi_global_aggregate_preserves_failpoint_observation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Instrumented execution stays on the canonical source transition path."""
    from fpstreams import _native
    from fpstreams.runtime.failpoints import failpoint

    def unexpected_kernel(_source: object, _lanes: object) -> object:
        raise AssertionError("an instrumented global aggregate reached native execution")

    monkeypatch.setattr(_native, "global_multi_i64_dict_rows_v1", unexpected_kernel)
    aggregated = fpstreams.rows([{"value": 2}, {"value": 3}] * 64).aggregate(
        rows=fpstreams.agg.count(),
        total=fpstreams.agg.sum("value"),
    )

    with (
        failpoint("source.open.after", RuntimeError("canonical global multi aggregate")),
        pytest.raises(RuntimeError, match="canonical global multi aggregate"),
    ):
        aggregated.to_list()


@pytest.mark.parametrize("mutation", ["kind", "selector"])
def test_group_aggregation_marker_metadata_is_bound_to_its_step(
    mutation: str,
) -> None:
    """Frozen-marker bypasses cannot relabel the operator or its captured selector."""
    np = pytest.importorskip("numpy")
    from fpstreams.collecting.aggregation import native_group_aggregation

    aggregation = fpstreams.agg.mean("value") if mutation == "kind" else fpstreams.agg.sum("value")
    hint = native_group_aggregation(aggregation)
    assert hint is not None
    object.__setattr__(hint, mutation, "max" if mutation == "kind" else "other")

    source = fpstreams.rows.from_numpy(
        np.asarray([[1, 1, 10], [1, 2, 20], [1, 4, 40], [1, 5, 50]] * 8),
        columns=("key", "value", "other"),
    )
    automatic = source.group_by("key").aggregate(result=aggregation)
    canonical = source.with_engine("python").group_by("key").aggregate(result=aggregation)
    expected = [{"key": 1, "result": 3.0 if mutation == "kind" else 96}]

    assert native_group_aggregation(aggregation) is None
    assert canonical.to_list() == expected
    assert automatic.to_list() == expected


@pytest.mark.parametrize(
    ("lifecycle", "expected_global", "expected_grouped"),
    [
        ("initializer", 42, 26),
        ("step", 64, 32),
        ("finish", 96, 48),
        ("combine", 32, 16),
        ("done", 32, 16),
    ],
)
def test_group_sum_deopts_after_any_lifecycle_code_change(
    monkeypatch: pytest.MonkeyPatch,
    lifecycle: str,
    expected_global: int,
    expected_grouped: int,
) -> None:
    """All five factory lifecycle functions must retain their original Python code."""
    np = pytest.importorskip("numpy")
    from fpstreams.collecting.aggregation import native_group_aggregation

    total = fpstreams.agg.sum("value")

    def initializer() -> int:
        return 10

    def step_factory() -> Callable[[int, dict[str, int]], int]:
        def select(row: dict[str, int]) -> int:
            return row["value"]

        def step(current: int, row: dict[str, int]) -> int:
            return current + select(row) * 2

        return step

    def finish(current: int) -> int:
        return current * 3

    def combine(left: int, right: int) -> int:
        return left + right + 100

    def done(_current: int) -> bool:
        return True

    replacement = {
        "initializer": initializer,
        "step": step_factory(),
        "finish": finish,
        "combine": combine,
        "done": done,
    }[lifecycle]
    target = getattr(total, lifecycle)
    monkeypatch.setattr(target, "__code__", replacement.__code__)

    matrix = np.asarray([[index % 2, 1] for index in range(32)], dtype=np.int64)
    source = fpstreams.rows.from_numpy(matrix, columns=("key", "value"))
    automatic_global = source.aggregate(total=total)
    canonical_global = source.with_engine("python").aggregate(total=total)
    automatic_grouped = source.group_by("key").aggregate(total=total)
    canonical_grouped = source.with_engine("python").group_by("key").aggregate(total=total)

    assert native_group_aggregation(total) is None
    assert "candidate" not in automatic_grouped.explain("list").to_dict()["relations"]
    assert canonical_global.to_list() == [{"total": expected_global}]
    assert automatic_global.to_list() == [{"total": expected_global}]
    expected_groups = [
        {"key": 0, "total": expected_grouped},
        {"key": 1, "total": expected_grouped},
    ]
    assert canonical_grouped.to_list() == expected_groups
    assert automatic_grouped.to_list() == expected_groups


@pytest.mark.parametrize("engine", ["auto", "python"])
@pytest.mark.parametrize("phase", ["source", "key"])
@pytest.mark.parametrize(
    "mutation", ["step_code", "selector_cell", "initializer_code", "finish_code"]
)
def test_group_sum_observes_lifecycle_mutations_during_traversal(
    monkeypatch: pytest.MonkeyPatch,
    engine: str,
    phase: str,
    mutation: str,
) -> None:
    """Callbacks can change a factory function without replacing its collector slot."""
    total = fpstreams.agg.sum("value")
    events: list[tuple[str, int]] = []

    def replacement_factory() -> Callable[[int, dict[str, int]], int]:
        def select(row: dict[str, int]) -> int:
            return row["value"]

        def step(state: int, row: dict[str, int]) -> int:
            return state + select(row) * 10

        return step

    def initializer() -> int:
        return 100

    def finish(state: int) -> int:
        return state * 10

    def mutate() -> None:
        if mutation == "step_code":
            monkeypatch.setattr(total.step, "__code__", replacement_factory().__code__)
        elif mutation == "selector_cell":
            assert total.step.__closure__ is not None
            monkeypatch.setattr(
                total.step.__closure__[0], "cell_contents", lambda row: row["value"] * 10
            )
        elif mutation == "initializer_code":
            monkeypatch.setattr(total.initializer, "__code__", initializer.__code__)
        else:
            monkeypatch.setattr(total.finish, "__code__", finish.__code__)

    def source() -> Iterator[dict[str, int]]:
        try:
            for value in (1, 2, 3):
                events.append(("source", value))
                if phase == "source" and value == 2:
                    mutate()
                yield {"key": value % 2, "value": value}
        finally:
            events.append(("close", 0))

    def key(row: dict[str, int]) -> int:
        events.append(("key", row["value"]))
        if phase == "key" and row["value"] == 2:
            mutate()
        return row["key"]

    result = (
        fpstreams.rows(source())
        .with_engine(engine)
        .group_by(key=key)
        .aggregate(total=total)
        .to_list()
    )
    expected = {
        "step_code": (31, 20),
        "selector_cell": (31, 20),
        "initializer_code": (4, 102),
        "finish_code": (40, 20),
    }[mutation]
    assert result == [{"key": 1, "total": expected[0]}, {"key": 0, "total": expected[1]}]
    assert events == [
        ("source", 1),
        ("key", 1),
        ("source", 2),
        ("key", 2),
        ("source", 3),
        ("key", 3),
        ("close", 0),
    ]


@pytest.mark.parametrize("single", [True, False])
@pytest.mark.parametrize("accessor", ["getattribute", "property"])
def test_group_aggregation_reads_dynamic_step_like_the_general_collector_program(
    single: bool, accessor: str
) -> None:
    reads: list[int] = []

    def read_step() -> Any:
        factor = len(reads) + 1
        reads.append(factor)
        return lambda state, row: state + row["value"] * factor

    class DynamicAggregator(fpstreams.Aggregator):
        __slots__ = ()

        def __getattribute__(self, name: str) -> Any:
            if name == "step":
                return read_step()
            return super().__getattribute__(name)

    class PropertyAggregator(fpstreams.Aggregator):
        __slots__ = ("_step",)

        @property
        def step(self) -> Any:
            return read_step()

        @step.setter
        def step(self, value: Any) -> None:
            object.__setattr__(self, "_step", value)

    constructor = DynamicAggregator if accessor == "getattribute" else PropertyAggregator
    aggregation = constructor(lambda: 0, lambda state, row: state)
    items = {"total": aggregation}
    if not single:
        items["count"] = fpstreams.agg.count()
    query = (
        fpstreams.rows([{"key": 0, "value": value} for value in (1, 2, 3)])
        .group_by(key=lambda row: row["key"])
        .aggregate(**items)
    )
    reads.clear()
    expected = {"key": 0, "total": 14}
    if not single:
        expected["count"] = 3
    assert query.to_list() == [expected]
    assert reads == [1, 2, 3]


def test_group_collector_guard_does_not_read_custom_metaclass_namespaces() -> None:
    checking = False

    class Meta(type):
        def __getattribute__(cls, name: str) -> Any:
            if checking and name in ("__mro__", "__dict__"):
                raise AssertionError("metaclass namespace read")
            return super().__getattribute__(name)

    class CustomAggregator(fpstreams.Aggregator, metaclass=Meta):
        __slots__ = ()

    total = CustomAggregator(lambda: 0, lambda state, row: state + row["value"])
    query = (
        fpstreams.rows([{"value": value} for value in (1, 2, 3)])
        .group_by(key=lambda row: 0)
        .aggregate(total=total)
    )
    checking = True
    assert query.to_list() == [{"key": 0, "total": 6}]


def test_group_sum_reads_finisher_code_between_output_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    total = fpstreams.agg.sum("value")
    result = iter(
        fpstreams.rows([{"key": 1, "value": 2}, {"key": 2, "value": 3}])
        .with_engine("python")
        .group_by(key=lambda row: row["key"])
        .aggregate(total=total)
    )
    assert next(result) == {"key": 1, "total": 2}

    def finish(state: int) -> int:
        return state * 10

    monkeypatch.setattr(total.finish, "__code__", finish.__code__)
    assert list(result) == [{"key": 2, "total": 30}]


@pytest.mark.parametrize(
    ("lifecycle", "expected"),
    [
        ("initializer", 42),
        ("step", 64),
        ("finish", 96),
        ("combine", 32),
        ("done", 32),
    ],
)
def test_project_count_deopts_after_any_lifecycle_code_change(
    monkeypatch: pytest.MonkeyPatch,
    lifecycle: str,
    expected: int,
) -> None:
    """Every project-owned count lifecycle must retain its factory code identity."""
    np = pytest.importorskip("numpy")

    rows = fpstreams.agg.count()

    def initializer() -> int:
        return 10

    def step(current: int, _row: object) -> int:
        return current + 2

    def finish(current: int) -> int:
        return current * 3

    def combine(left: int, right: int) -> int:
        return left + right + 100

    def done(_current: int) -> bool:
        return True

    replacement = {
        "initializer": initializer,
        "step": step,
        "finish": finish,
        "combine": combine,
        "done": done,
    }[lifecycle]
    target = getattr(rows, lifecycle)
    monkeypatch.setattr(target, "__code__", replacement.__code__)

    matrix = np.ones((32, 1), dtype=np.int64)
    source = fpstreams.rows.from_numpy(matrix, columns=("value",))
    automatic = source.aggregate(rows=rows)
    canonical = source.with_engine("python").aggregate(rows=rows)
    relation = automatic.explain("list").to_dict()["relations"]

    assert "candidate" not in relation
    assert canonical.to_list() == [{"rows": expected}]
    assert automatic.to_list() == [{"rows": expected}]


def test_native_record_global_sum_decline_reopens_canonical_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A semantics guard rejection must leave the retained source clean for Python fallback."""
    from fpstreams import _native

    calls: list[tuple[object, str]] = []

    def decline(source: object, field: str) -> None:
        calls.append((source, field))
        return None

    records = [{"value": True}, {"value": 2**100}]
    monkeypatch.setattr(_native, "global_sum_i64_dict_rows_v1", decline, raising=False)

    assert fpstreams.rows(records).aggregate(total=fpstreams.agg.sum("value")).to_list() == [
        {"total": 1 + 2**100}
    ]
    assert calls == [(records, "value")]


def test_native_record_global_sum_abi_declines_protocol_sensitive_values() -> None:
    """The optional ABI must reject unsafe shapes before invoking Python protocols."""
    from fpstreams import _native

    class Record(dict[str, object]):
        pass

    class Field(str):
        pass

    class RecordMapping(Mapping[str, object]):
        def __getitem__(self, key: str) -> object:
            return 1

        def __iter__(self) -> Iterator[str]:
            return iter(("value",))

        def __len__(self) -> int:
            return 1

    class Trap:
        calls = 0

        def __hash__(self) -> int:
            return hash("value")

        def __eq__(self, _other: object) -> bool:
            type(self).calls += 1
            raise AssertionError("speculative lookup touched custom equality")

    kernel = _native.global_sum_i64_dict_rows_v1
    maximum = 2**63 - 1
    assert kernel([{"value": 2}, {"value": 3}, {"value": -1}], "value") == 4
    assert kernel(({"value": maximum}, {"value": maximum}), "value") == maximum * 2
    for source in (
        [{"value": True}],
        [{"value": 2**100}],
        [{"value": None}],
        [{"missing": 1}],
        [Record(value=1)],
        [RecordMapping()],
        ({"value": 1} for _ in range(1)),
    ):
        assert kernel(source, "value") is None
    assert kernel([{"value": 1}], Field("value")) is None

    trap = Trap()
    assert kernel([{trap: 1}], "value") is None
    assert Trap.calls == 0


def test_native_record_global_sum_fallback_runs_mapping_protocols_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Speculative rejection must not duplicate Mapping or dict-subclass effects."""
    from fpstreams import _native

    effects: list[tuple[str, str]] = []

    class Record(dict[str, int]):
        def __getitem__(self, field: str) -> int:
            effects.append(("dict", field))
            return super().__getitem__(field)

    class RecordMapping(Mapping[str, int]):
        def __getitem__(self, field: str) -> int:
            effects.append(("mapping", field))
            return 3

        def __iter__(self) -> Iterator[str]:
            return iter(("value",))

        def __len__(self) -> int:
            return 1

    kernel = _native.global_sum_i64_dict_rows_v1
    kernel_calls: list[object] = []

    def tracked_kernel(source: object, field: str) -> int | None:
        kernel_calls.append(source)
        return kernel(source, field)

    monkeypatch.setattr(_native, "global_sum_i64_dict_rows_v1", tracked_kernel)
    dict_records = [Record(value=2)]
    mapping_records = [RecordMapping()]

    assert fpstreams.rows(dict_records).aggregate(total=fpstreams.agg.sum("value")).to_list() == [
        {"total": 2}
    ]
    assert fpstreams.rows(mapping_records).aggregate(
        total=fpstreams.agg.sum("value")
    ).to_list() == [{"total": 3}]
    assert kernel_calls == [dict_records, mapping_records]
    assert effects == [("dict", "value"), ("mapping", "value")]


def test_native_record_global_sum_declines_one_shot_and_instrumented_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One-shot ownership and failpoint observation stay on the canonical row executor."""
    from fpstreams import _native
    from fpstreams.runtime.failpoints import failpoint

    def unexpected_kernel(_source: object, _field: str) -> int | None:
        raise AssertionError("protocol-sensitive source reached the native global sum")

    monkeypatch.setattr(_native, "global_sum_i64_dict_rows_v1", unexpected_kernel)
    one_shot = fpstreams.rows(iter(({"value": 2}, {"value": 3}))).aggregate(
        total=fpstreams.agg.sum("value")
    )
    assert one_shot.to_list() == [{"total": 5}]
    with pytest.raises(fpstreams.FlowConsumedError):
        one_shot.to_list()

    automatic = fpstreams.rows([{"value": 2}, {"value": 3}]).aggregate(
        total=fpstreams.agg.sum("value")
    )
    with (
        failpoint("source.open.after", RuntimeError("canonical record global sum")),
        pytest.raises(RuntimeError, match="canonical record global sum"),
    ):
        automatic.to_list()


def test_exact_size_global_count_declines_transforms_forced_python_and_failpoints() -> None:
    """Only a direct automatic count may bypass canonical source observation."""
    from fpstreams.physical.relational import GlobalAggregatePhysicalNode
    from fpstreams.planning.compiler import compile_query
    from fpstreams.runtime.failpoints import failpoint

    source = fpstreams.rows([1, 2, 3])
    candidates = (
        source.with_engine("python").aggregate(rows=fpstreams.agg.count()),
        source.where(lambda _value: True).aggregate(rows=fpstreams.agg.count()),
        source.aggregate(rows=fpstreams.agg.count(), total=fpstreams.agg.sum()),
    )
    for candidate in candidates:
        physical = compile_query(candidate._flow._query("list"))
        assert isinstance(physical.root, GlobalAggregatePhysicalNode)
        assert physical.root.exact_count_name is None

    automatic = source.aggregate(rows=fpstreams.agg.count())
    with (
        failpoint("source.open.after", RuntimeError("canonical aggregate count")),
        pytest.raises(RuntimeError, match="canonical aggregate count"),
    ):
        automatic.to_list()


@pytest.mark.parametrize("adapter", ["dataframe", "polars"])
def test_guarded_eager_frame_identity_count_never_opens_python_rows(
    monkeypatch: pytest.MonkeyPatch, adapter: str
) -> None:
    """Unknown adapter size may be counted from zero-operation batches without boxing."""
    from fpstreams.planning.source import Source

    data = {"value": [1, 2, 3, 4]}
    source = (
        fpstreams.rows.from_dataframe(pd.DataFrame(data), batch_size=1)
        if adapter == "dataframe"
        else fpstreams.rows.from_polars(pl.DataFrame(data), batch_size=1)
    )
    input_source = source._flow._pipeline.source
    open_source = Source.open

    def reject_row_source(candidate: Source[object]) -> Iterator[object]:
        if candidate is input_source:
            raise AssertionError("identity columnar count must not open Python rows")
        return open_source(candidate)

    monkeypatch.setattr(Source, "open", reject_row_source)

    assert source.count() == 4


@pytest.mark.parametrize("engine", ["python", "auto"])
def test_group_by_sorted_public_contract(engine) -> None:
    from fpstreams import NativeUnsupportedError, agg, flow
    from fpstreams.planning.logical import SortedGroupAggregateNode, walk_logical

    opened = []
    pulled = []

    def records():
        opened.append(True)
        for row in [("a", 2), ("a", 3), ("b", 7), ("c", 9)]:
            pulled.append(row)
            yield {"region": row[0], "amount": row[1]}

    grouped = flow.defer(records).with_engine(engine).rows().group_by_sorted("region")
    result = grouped.aggregate(count=agg.count(), total=agg.sum("amount"))
    assert opened == pulled == []
    explanation = result.explain().to_dict()
    assert explanation["relations"]["node"] == "sorted_group_aggregate"
    assert explanation["relations"]["lookahead_rows"] == 1
    assert opened == []
    assert isinstance(result._flow._logical_plan.root, SortedGroupAggregateNode)
    assert len(walk_logical(result._flow._logical_plan.root)) == 2
    assert result.take(1).to_list() == [{"region": "a", "count": 2, "total": 5}]
    assert pulled == [("a", 2), ("a", 3), ("b", 7)]
    with pytest.raises(ValueError, match="sorted"):
        grouped.spill()
    assert opened == [True]
    assert flow([]).rows().group_by_sorted("region").aggregate(count=agg.count()).to_list() == []
    assert flow([(1, "a", 2), (1, "a", 3), (1, "b", 7)]).rows().group_by_sorted(
        0, label=1
    ).aggregate(total=agg.sum(2)).to_list() == [
        {"key_0": 1, "label": "a", "total": 5},
        {"key_0": 1, "label": "b", "total": 7},
    ]
    with pytest.raises(ValueError):
        flow([]).rows().group_by_sorted()
    with pytest.raises(TypeError):
        flow([]).rows().group_by_sorted(object())
    with pytest.raises(ValueError, match=r"input.*index 2"):
        flow([{"k": 1}, {"k": 3}, {"k": 2}]).rows().group_by_sorted("k").aggregate(
            count=agg.count()
        ).to_list()
    native_opened = []
    native = (
        flow.defer(lambda: native_opened.append(True) or iter([{"k": 1}]))
        .with_engine("native")
        .rows()
        .group_by_sorted("k")
        .aggregate(count=agg.count())
    )
    with pytest.raises(NativeUnsupportedError):
        native.to_list()
    assert native_opened == []


@pytest.mark.parametrize("engine", ["auto", "python"])
@pytest.mark.parametrize("mapping", [False, True])
@pytest.mark.parametrize("generator", [False, True])
def test_feedback_weighted_groups_keep_valid_denominators_and_unknown_values(
    engine, mapping, generator
):
    from types import MappingProxyType

    original = [
        {"player": 1, "minutes": 45, "observed": 60, "speed": 3.0, "runs": 0},
        {"player": 1, "minutes": 30, "observed": 120, "speed": 6.0, "runs": None},
        {"player": 2, "minutes": 90, "observed": 0, "speed": None, "runs": None},
        {"player": 3, "minutes": 10, "observed": 0.5, "speed": 1e12, "runs": 1},
    ]
    before = [row.copy() for row in original]
    records = [MappingProxyType(row) for row in original] if mapping else original
    source = (row for row in records) if generator else records
    summaries = (
        fpstreams.flow(source)
        .with_engine(engine)
        .with_columns(
            speed_num=lambda row: (
                row["speed"] * row["observed"] if row["speed"] is not None else 0.0
            ),
            speed_den=lambda row: row["observed"] if row["speed"] is not None else 0.0,
            run_num=lambda row: row["runs"] if row["runs"] is not None else 0,
            run_den=lambda row: row["minutes"] if row["runs"] is not None else 0,
        )
        .group_by("player")
        .aggregate(
            matches=fpstreams.agg.count(),
            minutes=fpstreams.agg.sum("minutes"),
            speed_num=fpstreams.agg.sum("speed_num"),
            speed_den=fpstreams.agg.sum("speed_den"),
            run_num=fpstreams.agg.sum("run_num"),
            run_den=fpstreams.agg.sum("run_den"),
        )
        .with_columns(
            speed=lambda row: row["speed_num"] / row["speed_den"] if row["speed_den"] else None,
            runs_per90=lambda row: row["run_num"] / row["run_den"] * 90 if row["run_den"] else None,
        )
        .sort_by("player")
        .to_list()
    )
    assert [row["matches"] for row in summaries] == [2, 1, 1]
    assert [row["minutes"] for row in summaries] == [75, 90, 10]
    assert summaries[0]["speed"] == pytest.approx(5.0, rel=1e-10, abs=1e-8)
    assert summaries[0]["run_den"] == 45
    assert summaries[0]["runs_per90"] == 0
    assert summaries[1]["speed"] is None
    assert summaries[1]["runs_per90"] is None
    assert summaries[2]["speed"] == pytest.approx(1e12, rel=1e-10, abs=1e-8)
    assert summaries[2]["runs_per90"] == 9
    assert original == before


def test_sorted_group_selector_stopiteration_is_wrapped_before_source_cleanup() -> None:
    import sys

    failure = StopIteration("sorted selector stopped")
    closed_with = []

    class Source:
        def __iter__(self):
            return self

        def __next__(self):
            return {"key": 1}

        def close(self):
            closed_with.append(sys.exception())

    def key(row):
        raise failure

    with pytest.raises(RuntimeError, match="generator raised StopIteration") as caught:
        fpstreams.rows(Source()).group_by_sorted(key=key).aggregate(
            rows=fpstreams.agg.count()
        ).to_list()
    assert caught.value.__cause__ is failure
    assert caught.value.__context__ is failure
    assert closed_with == [caught.value]


@pytest.mark.parametrize("builtin", ["tuple", "len"])
def test_sorted_group_key_wrapper_observes_constructor_replacements(builtin, monkeypatch) -> None:
    from fpstreams.execution import sorted_streams

    events = []
    original = tuple if builtin == "tuple" else len

    def replacement(value):
        events.append(builtin)
        return original(value)

    def key(row):
        if row["key"] == 1:
            monkeypatch.setattr(sorted_streams, builtin, replacement, raising=False)
        return row["key"]

    result = (
        fpstreams.rows([{"key": 1}, {"key": 2}, {"key": 3}])
        .group_by_sorted(key=key)
        .aggregate(rows=fpstreams.agg.count())
        .to_list()
    )
    assert result == [{"key": 1, "rows": 1}, {"key": 2, "rows": 1}, {"key": 3, "rows": 1}]
    assert events.count(builtin) == (2 if builtin == "tuple" else 6)
