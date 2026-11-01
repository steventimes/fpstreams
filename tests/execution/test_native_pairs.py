"""execution native pairs regression contracts."""

from __future__ import annotations

import sys
from collections.abc import Iterator
from datetime import datetime
from typing import Any

import pytest

import fpstreams
from fpstreams import agg, col, flow, lit
from fpstreams.planning.source import Source, SourceCapabilities
from fpstreams.runtime.failpoints import failpoint


def _dictionary_array(indices: list[int | None], values: list[object]) -> Any:
    """Build one dictionary array with an intentionally explicit physical dictionary."""
    pa = pytest.importorskip("pyarrow")
    return pa.DictionaryArray.from_arrays(
        pa.array(indices, type=pa.int8()),
        pa.array(values),
    )


def _supported_table(*, differing_dictionaries: bool) -> Any:
    """Return logical keys a, b, a, null, c in one or two physical dictionaries."""
    pa = pytest.importorskip("pyarrow")
    first = _dictionary_array([1, 0, 1], ["b", "a", "c"])
    if not differing_dictionaries:
        keys = _dictionary_array([1, 0, 1, None, 2], ["b", "a", "c"])
    else:
        second = _dictionary_array([None, 0], ["c", "a", "b"])
        keys = pa.chunked_array([first, second])
    return pa.table({"key": keys, "value": [1, 2, 3, 4, 5]})


def _group_sum(table: Any) -> list[dict[str, object]]:
    return (
        fpstreams.rows.from_arrow(table)
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
        .to_list()
    )


@pytest.mark.parametrize("differing_dictionaries", [False, True])
def test_arrow_dictionary_group_sum_stays_columnar_across_dictionary_layouts(
    monkeypatch: pytest.MonkeyPatch,
    differing_dictionaries: bool,
) -> None:
    """Canonical dictionaries, including null indices, must never box input rows."""
    from fpstreams.tabular import arrow as arrow_adapter

    def forbidden_rows(_batch: object) -> list[dict[str, object]]:
        raise AssertionError("canonical dictionary grouping must stay columnar")

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", forbidden_rows)

    assert _group_sum(_supported_table(differing_dictionaries=differing_dictionaries)) == [
        {"key": "a", "total": 4},
        {"key": "b", "total": 2},
        {"key": None, "total": 4},
        {"key": "c", "total": 5},
    ]


@pytest.mark.parametrize(
    ("dictionary_case", "values", "expected"),
    [
        pytest.param(
            "duplicate",
            [1, 2, 3, 4],
            [{"key": "same", "total": 10}],
            id="duplicate-dictionary-values",
        ),
        pytest.param(
            "null",
            [1, 2, 3, 4, 5],
            [{"key": None, "total": 12}, {"key": "value", "total": 3}],
            id="dictionary-null-value",
        ),
        pytest.param(
            "timestamp",
            [1, 2, 3],
            [
                {"key": datetime(2024, 1, 2), "total": 4},
                {"key": datetime(2024, 1, 1), "total": 2},
            ],
            id="unsupported-dictionary-value-type",
        ),
    ],
)
def test_arrow_dictionary_group_sum_declines_unsafe_dictionary_values(
    monkeypatch: pytest.MonkeyPatch,
    dictionary_case: str,
    values: list[int],
    expected: list[dict[str, object]],
) -> None:
    """Noncanonical or unsupported dictionaries retain canonical Python grouping."""
    from fpstreams.tabular import arrow as arrow_adapter

    pa = pytest.importorskip("pyarrow")
    if dictionary_case == "duplicate":
        keys = _dictionary_array([0, 1, 0, 1], ["same", "same"])
    elif dictionary_case == "null":
        keys = _dictionary_array([0, None, 1, 0, None], [None, "value"])
    else:
        keys = pa.DictionaryArray.from_arrays(
            pa.array([1, 0, 1], type=pa.int8()),
            pa.array([datetime(2024, 1, 1), datetime(2024, 1, 2)], type=pa.timestamp("s")),
        )
    converted: list[int] = []
    convert_rows = arrow_adapter.batch_to_rows

    def tracked(batch: Any) -> list[dict[str, object]]:
        converted.append(batch.num_rows)
        return convert_rows(batch)

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", tracked)

    assert _group_sum(pa.table({"key": keys, "value": values})) == expected
    assert converted == [len(values)]


def test_arrow_dictionary_group_sum_active_failpoint_uses_canonical_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Instrumentation disables the dictionary specialization without changing results."""
    from fpstreams.runtime.failpoints import failpoint
    from fpstreams.tabular import arrow as arrow_adapter

    converted: list[int] = []
    convert_rows = arrow_adapter.batch_to_rows

    def tracked(batch: Any) -> list[dict[str, object]]:
        converted.append(batch.num_rows)
        return convert_rows(batch)

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", tracked)
    with failpoint("unrelated.transition", RuntimeError("unused")):
        result = _group_sum(_supported_table(differing_dictionaries=False))

    assert result == [
        {"key": "a", "total": 4},
        {"key": "b", "total": 2},
        {"key": None, "total": 4},
        {"key": "c", "total": 5},
    ]
    assert converted == [5]


def test_arrow_dictionary_guard_decline_does_not_reopen_native_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A speculative dictionary decline performs one native open before row fallback."""
    from fpstreams.planning.source import Source

    pa = pytest.importorskip("pyarrow")
    native_opens = 0
    open_native = Source.open_native

    def tracked(self: Source[Any], expected_type: type[Any]) -> Any:
        nonlocal native_opens
        native_opens += 1
        return open_native(self, expected_type)

    monkeypatch.setattr(Source, "open_native", tracked)
    duplicate_keys = _dictionary_array([0, 1, 0, 1], ["same", "same"])

    assert _group_sum(pa.table({"key": duplicate_keys, "value": [1, 2, 3, 4]})) == [
        {"key": "same", "total": 10}
    ]
    assert native_opens == 1


@pytest.mark.parametrize("container", [list, tuple])
def test_direct_pair_sum_uses_strict_native_abi_without_opening_source(
    monkeypatch: pytest.MonkeyPatch,
    container: type[list[tuple[int, int]]] | type[tuple[tuple[int, int], ...]],
) -> None:
    """Deleting this fast path must expose an unexpected canonical source open."""
    first_key = int("1000000")
    equal_key = int("1000000")
    assert type(first_key) is type(equal_key) is int
    later_key = int("-7")
    assert first_key == equal_key and first_key is not equal_key
    source = container([(first_key, 2), (later_key, 5), (equal_key, 3)])

    def unexpected_open(*_args: object) -> Iterator[Any]:
        raise AssertionError("native pair sum opened its Python source")

    def unexpected_open_native(*_args: object) -> object:
        raise AssertionError("native pair sum entered the source lifecycle")

    monkeypatch.setattr(Source, "open", unexpected_open)
    monkeypatch.setattr(Source, "open_native", unexpected_open_native)

    result = fpstreams.pairs(source).aggregate_values(total=fpstreams.agg.sum())

    keys = list(result)
    assert keys == [1_000_000, -7]
    assert keys[0] is first_key
    assert result == {1_000_000: {"total": 5}, -7: {"total": 5}}


def test_direct_pair_sum_accepts_a_final_native_dictionary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Large native outputs must not be rebuilt from intermediate pairs in Python."""
    from fpstreams import _native

    source = [(1, 2), (1, 3)]
    final = {1: {"total": 5}}

    def final_dictionary(rows: object, output_name: object) -> tuple[bool, object]:
        assert rows is source
        assert output_name == "total"
        return True, final

    def unexpected_legacy_kernel(*_args: object) -> object:
        raise AssertionError("a handled v2 result reached the legacy pair kernel")

    monkeypatch.setattr(
        _native,
        "group_sum_i64_exact_pairs_v2",
        final_dictionary,
        raising=False,
    )
    monkeypatch.setattr(_native, "group_sum_i64_exact_pairs_v1", unexpected_legacy_kernel)

    result = fpstreams.pairs(source).aggregate_values(total=fpstreams.agg.sum())

    assert result is final


def test_direct_pair_multi_aggregate_reuses_native_group_kernel_without_opening_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Canonical whole-value lanes should reuse the exact-record Rust group kernel."""
    from fpstreams import _native

    first_key = int("1000000")
    equal_key = int("1000000")
    assert type(first_key) is type(equal_key) is int
    assert first_key == equal_key and first_key is not equal_key
    source = [(first_key, 7), (-3, 4), (equal_key, 2)]
    native_kernel = _native.group_multi_i64_exact_pairs_v1
    calls = 0

    def tracked_kernel(
        rows: object,
        key_name: str,
        lanes: tuple[tuple[int, int | None, str], ...],
    ) -> object:
        nonlocal calls
        calls += 1
        assert rows is source
        assert type(key_name) is str
        assert key_name not in {"count", "total", "low", "high"}
        assert lanes == (
            (0, None, "count"),
            (1, 1, "total"),
            (2, 1, "low"),
            (3, 1, "high"),
        )
        return native_kernel(rows, key_name, lanes)

    def unexpected_open(*_args: object) -> Iterator[Any]:
        raise AssertionError("native pair aggregation opened its Python source")

    monkeypatch.setattr(_native, "group_multi_i64_exact_pairs_v1", tracked_kernel)
    monkeypatch.setattr(Source, "open", unexpected_open)

    result = fpstreams.pairs(source).aggregate_values(
        count=fpstreams.agg.count(),
        total=fpstreams.agg.sum(),
        low=fpstreams.agg.min(),
        high=fpstreams.agg.max(),
    )

    assert calls == 1
    keys = list(result)
    assert keys == [1_000_000, -3]
    assert keys[0] is first_key
    assert result == {
        1_000_000: {"count": 2, "total": 9, "low": 2, "high": 7},
        -3: {"count": 1, "total": 4, "low": 4, "high": 4},
    }


def test_direct_pair_multi_native_decline_opens_only_the_canonical_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An incompatible exact container must remain untouched for one Python fallback."""
    from fpstreams import _native

    events: list[str] = []
    original_open = Source.open

    def decline(*_args: object) -> None:
        events.append("native")
        return None

    def tracked_open(source: Source[Any]) -> Iterator[Any]:
        events.append("open")
        return original_open(source)

    monkeypatch.setattr(_native, "group_multi_i64_exact_pairs_v1", decline)
    monkeypatch.setattr(Source, "open", tracked_open)

    result = fpstreams.pairs([(1, 2), (1, 3)]).aggregate_values(
        count=fpstreams.agg.count(),
        minimum=fpstreams.agg.min(),
    )

    assert result == {1: {"count": 2, "minimum": 2}}
    assert events == ["native", "open"]


@pytest.mark.parametrize(
    ("source", "message"),
    [
        ([(1,)], "not enough values to unpack"),
        ([(1, 2, 3)], "too many values to unpack"),
    ],
)
def test_direct_pair_multi_native_declines_non_pair_tuple_width(
    source: list[tuple[int, ...]],
    message: str,
) -> None:
    """The shared record kernel must retain the strict public pair boundary."""
    with pytest.raises(ValueError, match=message):
        fpstreams.pairs(source).aggregate_values(
            count=fpstreams.agg.count(),
            total=fpstreams.agg.sum(),
        )


def test_direct_pair_native_honors_replaced_public_aggregation_helper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Public collector helpers remain authoritative over every native lane shape."""
    pairs_module = sys.modules["fpstreams.streams.pairs"]

    def replacement(
        states: dict[str, int],
        items: tuple[tuple[str, object], ...],
        value: int,
    ) -> None:
        for name, _aggregation in items:
            states[name] += 1 if name == "count" else value * 10

    monkeypatch.setattr(pairs_module, "step_aggregations", replacement)

    assert fpstreams.pairs([(1, 2), (1, 3)]).aggregate_values(
        count=fpstreams.agg.count(),
        total=fpstreams.agg.sum(),
    ) == {1: {"count": 2, "total": 50}}


@pytest.mark.parametrize("source", [[], ()])
def test_direct_empty_pair_sum_is_a_handled_native_result(
    monkeypatch: pytest.MonkeyPatch,
    source: list[tuple[int, int]] | tuple[tuple[int, int], ...],
) -> None:
    """An empty native dictionary must not be confused with the fallback sentinel."""

    def unexpected_open(*_args: object) -> Iterator[Any]:
        raise AssertionError("empty native pair sum opened its Python source")

    monkeypatch.setattr(Source, "open", unexpected_open)

    assert fpstreams.pairs(source).aggregate_values(total=fpstreams.agg.sum()) == {}


@pytest.mark.parametrize("native_mode", ["missing", "noncallable", "decline"])
def test_pair_sum_native_unavailability_opens_only_the_canonical_fallback(
    monkeypatch: pytest.MonkeyPatch,
    native_mode: str,
) -> None:
    """Missing and ``None`` ABI outcomes leave the source unopened until fallback."""
    from fpstreams import _native

    events: list[str] = []
    original_open = Source.open

    def tracked_open(source: Source[Any]) -> Iterator[Any]:
        events.append("open")
        return original_open(source)

    monkeypatch.setattr(Source, "open", tracked_open)
    if native_mode == "missing":
        monkeypatch.delattr(_native, "group_sum_i64_exact_pairs_v2")
        monkeypatch.delattr(_native, "group_sum_i64_exact_pairs_v1")
    elif native_mode == "noncallable":
        monkeypatch.setattr(_native, "group_sum_i64_exact_pairs_v2", None)
        monkeypatch.setattr(_native, "group_sum_i64_exact_pairs_v1", None)
    else:

        def decline(_source: object, _output_name: object) -> None:
            assert events == []
            events.append("native")
            return None

        monkeypatch.setattr(_native, "group_sum_i64_exact_pairs_v2", decline)

    result = fpstreams.pairs([(1, 2), (1, 3)]).aggregate_values(total=fpstreams.agg.sum())

    assert result == {1: {"total": 5}}
    assert events == (["native", "open"] if native_mode == "decline" else ["open"])


def test_pair_sum_native_error_propagates_without_opening_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Catching a real ABI failure would incorrectly turn it into Python fallback."""
    from fpstreams import _native

    events: list[str] = []

    def fail(_source: object, _output_name: object) -> None:
        events.append("native")
        raise MemoryError("native grouping allocation failed")

    def unexpected_open(*_args: object) -> Iterator[Any]:
        events.append("open")
        raise AssertionError("native failure opened the source")

    monkeypatch.setattr(_native, "group_sum_i64_exact_pairs_v2", fail)
    monkeypatch.setattr(Source, "open", unexpected_open)

    with pytest.raises(MemoryError, match="native grouping allocation failed"):
        fpstreams.pairs([(1, 2)]).aggregate_values(total=fpstreams.agg.sum())
    assert events == ["native"]


def test_pair_sum_native_requires_a_direct_replayable_exact_container(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Removing any source-shape guard must invoke the rejecting kernel below."""
    from fpstreams import _native

    def unexpected_native(*_args: object) -> None:
        raise AssertionError("ineligible pair source reached native execution")

    monkeypatch.setattr(_native, "group_sum_i64_exact_pairs_v2", unexpected_native)

    def one_shot() -> Iterator[tuple[int, int]]:
        yield 1, 2
        yield 1, 3

    class PairList(list[tuple[int, int]]):
        pass

    assert fpstreams.pairs(one_shot()).aggregate_values(total=fpstreams.agg.sum()) == {
        1: {"total": 5}
    }
    assert fpstreams.pairs(PairList([(1, 2)])).aggregate_values(total=fpstreams.agg.sum()) == {
        1: {"total": 2}
    }
    assert fpstreams.pairs([(1, 2)]).map_values(lambda value: value + 3).aggregate_values(
        total=fpstreams.agg.sum()
    ) == {1: {"total": 5}}


@pytest.mark.parametrize(
    "capabilities",
    [
        SourceCapabilities(reiterable=False, exact_size=2, ordered=True),
        SourceCapabilities(reiterable=True, exact_size=2, ordered=False),
    ],
)
def test_pair_sum_native_checks_retained_source_replayability_and_order(
    monkeypatch: pytest.MonkeyPatch,
    capabilities: SourceCapabilities,
) -> None:
    """Retained rows alone cannot authorize native execution without both source guarantees."""
    from fpstreams import _native

    rows = [(1, 2), (1, 3)]
    source = Source(lambda: iter(rows), capabilities, native_data=rows)

    def unexpected_native(*_args: object) -> None:
        raise AssertionError("unproven pair source reached native execution")

    monkeypatch.setattr(_native, "group_sum_i64_exact_pairs_v2", unexpected_native)

    assert fpstreams.Flow(source).pairs().aggregate_values(total=fpstreams.agg.sum()) == {
        1: {"total": 5}
    }


def test_pair_sum_native_honors_forced_python_and_active_failpoints(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Execution policy and instrumentation must retain the canonical Python boundaries."""
    from fpstreams import _native

    def unexpected_native(*_args: object) -> None:
        raise AssertionError("execution policy allowed native pair grouping")

    monkeypatch.setattr(_native, "group_sum_i64_exact_pairs_v2", unexpected_native)
    forced_python = fpstreams.pairs([(1, 2), (1, 3)]).to_flow().with_engine("python").pairs()

    assert forced_python.aggregate_values(total=fpstreams.agg.sum()) == {1: {"total": 5}}
    with failpoint("unrelated.transition", RuntimeError("unused")):
        assert fpstreams.pairs([(1, 2)]).aggregate_values(total=fpstreams.agg.sum()) == {
            1: {"total": 2}
        }


def test_narrow_pair_sum_kernel_requires_one_builtin_whole_value_sum(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Other lane shapes must bypass the specialized single-sum ABI."""
    from fpstreams import _native

    def unexpected_native(*_args: object) -> None:
        raise AssertionError("noncanonical aggregation reached native pair grouping")

    monkeypatch.setattr(_native, "group_sum_i64_exact_pairs_v2", unexpected_native)
    source = [(1, 2), (1, 3)]

    assert fpstreams.pairs(source).aggregate_values(
        total=fpstreams.agg.sum(lambda value: value)
    ) == {1: {"total": 5}}
    assert fpstreams.pairs(source).aggregate_values(
        total=fpstreams.agg.sum(), count=fpstreams.agg.count()
    ) == {1: {"total": 5, "count": 2}}
    assert fpstreams.pairs(source).aggregate_values(minimum=fpstreams.agg.min()) == {
        1: {"minimum": 2}
    }

    branded = fpstreams.agg.sum()

    def replacement(total: int, _value: int) -> int:
        return total + 100

    replacement.__dict__.update(branded.step.__dict__)
    counterfeit = fpstreams.Aggregator(
        branded.initializer,
        replacement,
        branded.finish,
        branded.combine,
        branded.done,
        branded.native,
    )
    assert fpstreams.pairs(source).aggregate_values(total=counterfeit) == {1: {"total": 200}}


def test_pair_sum_native_requires_an_exact_output_name(monkeypatch: pytest.MonkeyPatch) -> None:
    """A string subclass can have observable hashing and therefore cannot enter native output."""
    from fpstreams import _native

    class OutputName(str):
        pass

    def unexpected_native(*_args: object) -> None:
        raise AssertionError("nonexact output name reached native pair grouping")

    monkeypatch.setattr(_native, "group_sum_i64_exact_pairs_v2", unexpected_native)
    output_name = OutputName("total")

    result = fpstreams.pairs([(1, 2)]).aggregate_values(**{output_name: fpstreams.agg.sum()})

    assert result == {1: {"total": 2}}
    assert next(iter(result[1])) is output_name


@pytest.mark.parametrize(
    ("source", "message"),
    [
        ([(1,)], "not enough values to unpack"),
        ([(1, 2), (1, 2, 3)], "too many values to unpack"),
    ],
)
def test_pair_sum_native_decline_preserves_pair_unpack_errors(
    monkeypatch: pytest.MonkeyPatch,
    source: list[Any],
    message: str,
) -> None:
    """Strict width rejection must replay the canonical loop and its exact failure class."""
    from fpstreams import _native

    calls = 0
    strict_kernel = _native.group_sum_i64_exact_pairs_v2

    def tracked_kernel(rows: object, output_name: object) -> object:
        nonlocal calls
        calls += 1
        return strict_kernel(rows, output_name)

    monkeypatch.setattr(_native, "group_sum_i64_exact_pairs_v2", tracked_kernel)

    with pytest.raises(ValueError, match=message):
        fpstreams.pairs(source).aggregate_values(total=fpstreams.agg.sum())
    assert calls == 1


def test_pair_aggregation_preserves_builtin_keys_and_multiple_lanes() -> None:
    """The public Python path preserves names, key order, and canonical lane values."""
    result = (
        fpstreams.pairs([("a", 3), ("b", 4), ("a", 2)])
        .with_engine("python")
        .aggregate_values(
            count=fpstreams.agg.count(),
            total=fpstreams.agg.sum(),
            low=fpstreams.agg.min(),
            high=fpstreams.agg.max(),
        )
    )

    assert result == {
        "a": {"count": 2, "total": 5, "low": 2, "high": 3},
        "b": {"count": 1, "total": 4, "low": 4, "high": 4},
    }
    assert list(result) == ["a", "b"]
    assert list(result["a"]) == ["count", "total", "low", "high"]


def test_pair_aggregation_finish_mutated_during_consumption_remains_authoritative() -> None:
    """A value callback can still replace a finisher after aggregation starts."""
    aggregation = fpstreams.agg.sum()

    class Value:
        def __radd__(self, total: int) -> int:
            object.__setattr__(aggregation, "finish", lambda state: state * 10)
            return total + 2

    result = (
        fpstreams.pairs([("key", Value())])
        .with_engine("python")
        .aggregate_values(total=aggregation)
    )

    assert result == {"key": {"total": 20}}


def test_pair_row_expression_group_sum_attempts_native_before_clean_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Eligible retained i64 pair expressions try Rust without consuming Python fallback."""
    from fpstreams import _native

    source = [(3, 7), (1, 11), (4, 13), (8, 17)] * 8
    calls: list[tuple[object, object, object, object, object]] = []

    def decline(
        rows: object,
        key_program: object,
        value_program: object,
        key_name: object,
        output_name: object,
    ) -> None:
        calls.append((rows, key_program, value_program, key_name, output_name))
        return None

    monkeypatch.setattr(_native, "group_sum_i64_pair_expr_rows_v1", decline, raising=False)
    grouped = (
        fpstreams.rows(source)
        .group_by(key=col(0) % 3)
        .aggregate(total=fpstreams.agg.sum(col(1) * 2 - 1))
    )
    constant = (
        fpstreams.rows(source).group_by(key=col(0) % 3).aggregate(total=fpstreams.agg.sum(lit(3)))
    )

    assert grouped.to_list() == [
        {"key": 0, "total": 104},
        {"key": 1, "total": 368},
        {"key": 2, "total": 264},
    ]
    assert constant.to_list() == [
        {"key": 0, "total": 24},
        {"key": 1, "total": 48},
        {"key": 2, "total": 24},
    ]
    assert len(calls) == 2
    for native_source, key_program, value_program, key_name, output_name in calls:
        assert native_source is source
        assert key_program and value_program
        assert (key_name, output_name) == ("key", "total")


def test_pair_row_expression_group_sum_keeps_sub_crossover_sources_in_python(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A retained source below the measured crossover must avoid native startup overhead."""
    from fpstreams import _native

    def unexpected_native(*_arguments: object) -> object:
        raise AssertionError("a sub-crossover RowExpr group reached native execution")

    monkeypatch.setattr(
        _native,
        "group_sum_i64_pair_expr_rows_v1",
        unexpected_native,
        raising=False,
    )
    source = [(value, value + 1) for value in range(31)]

    result = (
        fpstreams.rows(source)
        .group_by(key=col(0) % 2)
        .aggregate(total=fpstreams.agg.sum(col(1) + 1))
        .to_list()
    )

    assert result == [{"key": 0, "total": 272}, {"key": 1, "total": 255}]


@pytest.mark.parametrize("replacement", ["open", "claim", "rowexpr_getattribute"])
def test_pair_row_expression_group_sum_observes_replaced_python_boundaries(
    monkeypatch: pytest.MonkeyPatch,
    replacement: str,
) -> None:
    """Replaced source and expression boundaries remain observable through Python fallback."""
    from fpstreams import _native
    from fpstreams.expressions.row import RowExpr

    source = [(value, value + 1) for value in range(32)]
    key = col(0) % 2
    value = col(1) + 1
    grouped = fpstreams.rows(source).group_by(key=key).aggregate(total=fpstreams.agg.sum(value))
    events: list[str] = []

    def unexpected_native(*_arguments: object) -> object:
        raise AssertionError("a replaced Python boundary reached native execution")

    if replacement == "open":
        original_open = Source.open

        def observed_open(owner: Source[Any]) -> Iterator[Any]:
            events.append("open")
            return original_open(owner)

        monkeypatch.setattr(Source, "open", observed_open)
    elif replacement == "claim":
        original_claim = Source._claim

        def observed_claim(owner: Source[Any]) -> None:
            events.append("claim")
            original_claim(owner)

        monkeypatch.setattr(Source, "_claim", observed_claim)
    else:
        original_getattribute = RowExpr.__getattribute__

        def observed_getattribute(expression: RowExpr, name: str) -> object:
            attribute = original_getattribute(expression, name)
            if name != "_evaluate":
                return attribute

            def observed_evaluate(row: object) -> object:
                events.append("evaluate")
                return attribute(row)

            return observed_evaluate

        monkeypatch.setattr(RowExpr, "__getattribute__", observed_getattribute)
    monkeypatch.setattr(
        _native,
        "group_sum_i64_pair_expr_rows_v1",
        unexpected_native,
        raising=False,
    )

    result = grouped.to_list()

    assert result == [{"key": 0, "total": 272}, {"key": 1, "total": 288}]
    assert events == ([replacement] if replacement != "rowexpr_getattribute" else ["evaluate"] * 64)


def test_pair_row_expression_group_sum_matches_python_negative_floor_division_and_modulo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The native arithmetic vocabulary follows Python for negative // and % operands."""
    from fpstreams import _native

    source = [(-7, -7), (-8, -8), (-1, -1), (7, 7), (8, 8), (1, 1)] * 6
    expected = [
        {"key": -3, "total": -18},
        {"key": -1, "total": -6},
        {"key": 2, "total": -18},
        {"key": 0, "total": -12},
    ]
    native_calls = 0
    native_kernel = _native.group_sum_i64_pair_expr_rows_v1

    def tracked_native(*arguments: object) -> object:
        nonlocal native_calls
        native_calls += 1
        return native_kernel(*arguments)

    monkeypatch.setattr(_native, "group_sum_i64_pair_expr_rows_v1", tracked_native)

    def run(engine: str) -> list[dict[str, int]]:
        return (
            fpstreams.rows(source)
            .with_engine(engine)
            .group_by(key=col(0) // 3)
            .aggregate(total=fpstreams.agg.sum(col(1) % -3))
            .to_list()
        )

    assert run("python") == expected
    assert run("auto") == expected
    assert native_calls == 1


@pytest.mark.parametrize("late_row", [[1, 5], {0: 1, 1: 5}])
def test_pair_row_expression_group_sum_replays_after_a_late_non_tuple_row(
    monkeypatch: pytest.MonkeyPatch,
    late_row: object,
) -> None:
    """A whole-container shape rejection restarts without losing prior groups or order."""
    from fpstreams import _native

    source = [(1, 2), (2, 3)] * 15 + [(1, 2), late_row]
    native_calls = 0
    native_kernel = _native.group_sum_i64_pair_expr_rows_v1

    def tracked_native(*arguments: object) -> object:
        nonlocal native_calls
        native_calls += 1
        return native_kernel(*arguments)

    monkeypatch.setattr(_native, "group_sum_i64_pair_expr_rows_v1", tracked_native)

    assert fpstreams.rows(source).group_by(key=col(0) % 2).aggregate(
        total=fpstreams.agg.sum(col(1) * 2)
    ).to_list() == [{"key": 1, "total": 74}, {"key": 0, "total": 90}]
    assert native_calls == 1


def test_pair_row_expression_group_sum_keeps_generators_and_python_engine_off_native(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One-shot sources and forced Python execution never enter the retained-sequence ABI."""
    from fpstreams import _native

    def unexpected_native(*_arguments: object) -> object:
        raise AssertionError("an ineligible RowExpr group reached native execution")

    monkeypatch.setattr(
        _native,
        "group_sum_i64_pair_expr_rows_v1",
        unexpected_native,
        raising=False,
    )
    generated = ((value, value + 1) for value in range(32))
    generator_result = (
        fpstreams.rows(generated)
        .group_by(key=col(0) % 2)
        .aggregate(total=fpstreams.agg.sum(col(1) + 1))
        .to_list()
    )
    python_result = (
        fpstreams.rows([(value, value + 1) for value in range(32)])
        .with_engine("python")
        .group_by(key=col(0) % 2)
        .aggregate(total=fpstreams.agg.sum(col(1) + 1))
        .to_list()
    )

    assert generator_result == [{"key": 0, "total": 272}, {"key": 1, "total": 288}]
    assert python_result == generator_result


def test_pair_row_expression_group_sum_declines_active_failpoints(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Active failpoints retain the observable Python execution path."""
    from fpstreams import _native
    from fpstreams.runtime.failpoints import failpoint

    def unexpected_native(*_arguments: object) -> object:
        raise AssertionError("an observed RowExpr group reached native execution")

    monkeypatch.setattr(
        _native,
        "group_sum_i64_pair_expr_rows_v1",
        unexpected_native,
        raising=False,
    )
    grouped = (
        fpstreams.rows([(value, value + 1) for value in range(32)])
        .group_by(key=col(0) % 2)
        .aggregate(total=fpstreams.agg.sum(col(1) + 1))
    )
    with failpoint("unrelated.rowexpr-group.transition", RuntimeError("unused")):
        result = grouped.to_list()

    assert result == [{"key": 0, "total": 272}, {"key": 1, "total": 288}]


@pytest.mark.parametrize("boolean_side", ["key", "value"])
def test_pair_row_expression_group_sum_keeps_boolean_roots_in_python(
    monkeypatch: pytest.MonkeyPatch,
    boolean_side: str,
) -> None:
    """Comparison roots retain bool keys/results instead of entering the integer ABI."""
    from fpstreams import _native

    def unexpected_native(*_arguments: object) -> object:
        raise AssertionError("a Boolean RowExpr root reached the i64 arithmetic kernel")

    monkeypatch.setattr(
        _native,
        "group_sum_i64_pair_expr_rows_v1",
        unexpected_native,
        raising=False,
    )
    source = [(-1, -2), (1, 3), (-2, 4)] * 11
    if boolean_side == "key":
        result = (
            fpstreams.rows(source)
            .group_by(key=col(0) > 0)
            .aggregate(total=fpstreams.agg.sum(col(1) + 0))
            .to_list()
        )
        assert result == [{"key": False, "total": 22}, {"key": True, "total": 33}]
        assert all(type(row["key"]) is bool for row in result)
    else:
        result = (
            fpstreams.rows(source)
            .group_by(key=col(0) + 0)
            .aggregate(total=fpstreams.agg.sum(col(1) > 0))
            .to_list()
        )
        assert result == [
            {"key": -1, "total": 0},
            {"key": 1, "total": 11},
            {"key": -2, "total": 11},
        ]
        assert all(type(row["total"]) is int for row in result)


def test_pair_row_expression_group_sum_preserves_non_arithmetic_key_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Direct indexes use the identity-safe kernel, while literal keys remain Python objects."""
    from fpstreams import _native

    def unexpected_expr_native(*_arguments: object) -> object:
        raise AssertionError("a direct or literal RowExpr key reached the arithmetic ABI")

    monkeypatch.setattr(
        _native,
        "group_sum_i64_pair_expr_rows_v1",
        unexpected_expr_native,
        raising=False,
    )
    first_key = int("5000")
    equal_key = int("5000")
    assert first_key is not equal_key
    direct = (
        fpstreams.rows([(first_key, 2), (equal_key, 3)] * 16)
        .group_by(key=col(0))
        .aggregate(total=fpstreams.agg.sum(col(1)))
        .to_list()
    )
    literal_key = int("4000000000000000000")
    literal = (
        fpstreams.rows([(1, 2), (2, 3)] * 16)
        .group_by(key=lit(literal_key))
        .aggregate(total=fpstreams.agg.sum(col(1) + 0))
        .to_list()
    )

    assert direct == [{"key": first_key, "total": 80}]
    assert direct[0]["key"] is first_key
    assert literal == [{"key": literal_key, "total": 80}]
    assert literal[0]["key"] is literal_key


def test_pair_row_expression_group_sum_preserves_modulo_dividend_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Modulo bucketing retains the first source integer when CPython returns it directly."""
    from fpstreams import _native

    first_key = int("5000")
    equal_key = int("5000")
    assert first_key is not equal_key
    source = [(first_key, 2), (equal_key, 3)] * 16
    native_calls = 0
    native_kernel = _native.group_sum_i64_pair_expr_rows_v1

    def tracked_native(*arguments: object) -> object:
        nonlocal native_calls
        native_calls += 1
        return native_kernel(*arguments)

    monkeypatch.setattr(_native, "group_sum_i64_pair_expr_rows_v1", tracked_native)

    def run(engine: str) -> list[dict[str, int]]:
        return (
            fpstreams.rows(source)
            .with_engine(engine)
            .group_by(key=col(0) % 1_000_000_000_000)
            .aggregate(total=fpstreams.agg.sum(col(1) + 0))
            .to_list()
        )

    expected = run("python")
    result = run("auto")

    assert result == expected == [{"key": first_key, "total": 80}]
    assert expected[0]["key"] is first_key
    assert result[0]["key"] is first_key
    assert native_calls == 1


def test_pair_row_expression_group_sum_respects_a_replaced_lazy_program(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A live LazyRowEvaluator cache remains authoritative over its original IR node."""
    from fpstreams import _native

    def unexpected_native(*_arguments: object) -> object:
        raise AssertionError("a replaced RowExpr evaluator program reached native execution")

    def constant_key(_row: object) -> int:
        return 99

    def constant_value(_row: object) -> int:
        return 10

    key = col(0) % 2
    value = col(1) + 0
    monkeypatch.setattr(key._evaluate, "_program", constant_key)
    monkeypatch.setattr(value._evaluate, "_program", constant_value)
    monkeypatch.setattr(
        _native,
        "group_sum_i64_pair_expr_rows_v1",
        unexpected_native,
        raising=False,
    )

    result = (
        fpstreams.rows([(1, 2), (2, 3), (3, 4)] * 11)
        .group_by(key=key)
        .aggregate(total=fpstreams.agg.sum(value))
        .to_list()
    )

    assert result == [{"key": 99, "total": 330}]


def test_pair_row_expression_group_sum_cleanly_replays_python_integer_edges(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bool subclasses, custom ints, bigints, and checked overflow keep Python semantics."""
    from fpstreams import _native

    calls: list[str] = []
    custom_key = object()
    native_calls = 0
    native_kernel = _native.group_sum_i64_pair_expr_rows_v1

    def tracked_native(*arguments: object) -> object:
        nonlocal native_calls
        native_calls += 1
        return native_kernel(*arguments)

    monkeypatch.setattr(_native, "group_sum_i64_pair_expr_rows_v1", tracked_native)

    class CustomInt(int):
        def __mod__(self, _other: object) -> object:
            calls.append("mod")
            return custom_key

    custom_result = (
        fpstreams.rows([(CustomInt(5), 2), (1, 3)] * 16)
        .group_by(key=col(0) % 2)
        .aggregate(total=fpstreams.agg.sum(col(1) + 0))
        .to_list()
    )
    huge = 1 << 80
    huge_result = (
        fpstreams.rows([(huge, huge)] * 32)
        .group_by(key=col(0) + 1)
        .aggregate(total=fpstreams.agg.sum(col(1) * 2))
        .to_list()
    )
    i64_max = (1 << 63) - 1
    overflow_result = (
        fpstreams.rows([(i64_max, 7)] * 32)
        .group_by(key=(col(0) + 1) - 1)
        .aggregate(total=fpstreams.agg.sum(col(1) + 0))
        .to_list()
    )
    bool_result = (
        fpstreams.rows([(True, 2), (1, 3)] * 16)
        .group_by(key=col(0) + 0)
        .aggregate(total=fpstreams.agg.sum(col(1) + 0))
        .to_list()
    )

    assert custom_result == [{"key": custom_key, "total": 32}, {"key": 1, "total": 48}]
    assert custom_result[0]["key"] is custom_key
    assert calls == ["mod"] * 16
    assert huge_result == [{"key": huge + 1, "total": huge * 64}]
    assert overflow_result == [{"key": i64_max, "total": 224}]
    assert bool_result == [{"key": 1, "total": 80}]
    assert type(bool_result[0]["key"]) is int
    assert native_calls == 4


def test_pair_row_expression_group_sum_preserves_python_error_after_native_decline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A declined checked program replays the current Python runtime exception."""
    from fpstreams import _native

    native_calls = 0
    native_kernel = _native.group_sum_i64_pair_expr_rows_v1

    def tracked_native(*arguments: object) -> object:
        nonlocal native_calls
        native_calls += 1
        return native_kernel(*arguments)

    monkeypatch.setattr(_native, "group_sum_i64_pair_expr_rows_v1", tracked_native)
    canonical = (
        fpstreams.rows([(0, 0)] * 32)
        .with_engine("python")
        .group_by(key=col(0) % 0)
        .aggregate(total=fpstreams.agg.sum(1 // col(1)))
    )
    automatic = (
        fpstreams.rows([(0, 0)] * 32)
        .group_by(key=col(0) % 0)
        .aggregate(total=fpstreams.agg.sum(1 // col(1)))
    )

    with pytest.raises(ZeroDivisionError) as expected:
        canonical.to_list()
    with pytest.raises(ZeroDivisionError) as captured:
        automatic.to_list()
    assert captured.value.args == expected.value.args
    assert native_calls == 1


def test_pair_row_expression_group_sum_keeps_opaque_callbacks_in_python(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Auto execution preserves opaque key-before-value callbacks exactly once per row."""
    from fpstreams import _native
    from fpstreams.expressions.row import RowExpr

    def unexpected_native(*_arguments: object) -> object:
        raise AssertionError("an opaque RowExpr group reached native execution")

    monkeypatch.setattr(
        _native,
        "group_sum_i64_pair_expr_rows_v1",
        unexpected_native,
        raising=False,
    )
    calls: list[tuple[str, int]] = []
    key = RowExpr(lambda row: calls.append(("key", row[0])) or row[0] % 2, "key")
    value = RowExpr(lambda row: calls.append(("value", row[1])) or row[1] * 2, "value")

    result = (
        fpstreams.rows([(1, 2), (2, 3), (3, 4)] * 11)
        .group_by(key=key)
        .aggregate(total=fpstreams.agg.sum(value))
        .to_list()
    )

    assert result == [{"key": 1, "total": 132}, {"key": 0, "total": 66}]
    assert (
        calls
        == [
            ("key", 1),
            ("value", 2),
            ("key", 2),
            ("value", 3),
            ("key", 3),
            ("value", 4),
        ]
        * 11
    )


@pytest.mark.parametrize("kind", ["group", "merge", "join"])
def test_sorted_execution_report_routes(kind, monkeypatch) -> None:
    from fpstreams.execution import sorted_streams

    calls = []
    if kind == "group":
        query = (
            flow([{"id": 1}, {"id": 1}, {"id": 2}])
            .rows()
            .group_by_sorted("id")
            .aggregate(n=agg.count())
        )
        name = "aggregate_sorted_rows"
    elif kind == "merge":
        query = flow([1, 3]).merge_sorted([2, 4])
        name = "merge_sorted_values"
    else:
        query = flow([{"id": 1}]).rows().join_sorted([{"id": 1}], on="id")
        name = "join_sorted_rows"
    real = getattr(sorted_streams, name)

    def forwarded(*args, **kwargs):
        calls.append("entered")
        yield from real(*args, **kwargs)
        calls.append("returned")

    monkeypatch.setattr(sorted_streams, name, forwarded)
    query.explain()
    assert calls == []
    execution = query.run_with_report("to_list")
    assert calls == ["entered", "returned"]
    assert execution.report.strategy == f"python_sorted_{kind}"
    assert execution.report.compiler_engine == "python"
    assert execution.value


def test_optional_backend_fallback_contract(monkeypatch) -> None:
    from fpstreams import _native
    from fpstreams.execution import relational

    monkeypatch.delattr(_native, "group_fixed_i64_rows_v1", raising=False)
    monkeypatch.delattr(_native, "group_fixed_i64_dict_rows_v1", raising=False)
    monkeypatch.delattr(_native, "group_multi_i64_rows_v1", raising=False)
    monkeypatch.delattr(_native, "group_multi_i64_dict_rows_v1", raising=False)
    query = fpstreams.rows([(1, 3), (1, 4)]).group_by(key=0).aggregate(total=fpstreams.agg.sum(1))
    assert query.to_list() == [{"key": 1, "total": 7}]
    failure = MemoryError("user key allocation")
    calls = []

    def key(row):
        calls.append(row)
        raise failure

    monkeypatch.setattr(relational, "_try_native_group_sum", lambda node: None)
    with pytest.raises(MemoryError) as caught:
        fpstreams.rows(iter([(1, 3), (1, 4)])).group_by(key=key).aggregate(
            total=fpstreams.agg.sum(1)
        ).to_list()
    assert caught.value is failure
    assert calls == [(1, 3)]
