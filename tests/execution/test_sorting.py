"""execution sorting regression contracts."""

from __future__ import annotations

import sys
from collections.abc import Mapping
from dataclasses import replace

import pytest

import fpstreams
from fpstreams import flow
from fpstreams.execution.sorting import SortRecord, sort_records
from fpstreams.physical.plan import (
    SortPhysicalNode,
    SortStrategy,
)
from fpstreams.planning.compiler import compile_query
from fpstreams.planning.source import Source
from fpstreams.runtime.failpoints import failpoint


class _LessThanOnlySortKey:
    """Pickleable test key whose valid ordering deliberately omits equality."""

    def __init__(self, value: int) -> None:
        self.value = value

    def __lt__(self, other: object) -> bool:
        assert isinstance(other, _LessThanOnlySortKey)
        return self.value < other.value

    def __eq__(self, other: object) -> bool:
        raise AssertionError("sorting must not compare keys for equality")


class _IdentityLessThanOnlyValue:
    """Pickleable identity value exposing ordering, stability tags, and no equality."""

    def __init__(self, key: int, position: int) -> None:
        self.key = key
        self.position = position

    def __lt__(self, other: object) -> bool:
        assert isinstance(other, _IdentityLessThanOnlyValue)
        return self.key < other.key

    def __eq__(self, other: object) -> bool:
        raise AssertionError("identity external sort must not compare values for equality")


class _FalseySortKey:
    """Callable key whose truth value must not replace it with identity."""

    def __call__(self, value: int) -> int:
        return -value

    def __bool__(self) -> bool:
        return False


class _UnpickleableSortKey:
    """A valid ordering key whose process-local result cannot enter a spill record."""

    def __init__(self, value: int) -> None:
        self.value = value

    def __lt__(self, other: object) -> bool:
        assert isinstance(other, _UnpickleableSortKey)
        return self.value < other.value

    def __reduce_ex__(self, _protocol: int) -> object:
        raise TypeError("sort key result is process-local")


class _LateUnpickleableSortKey:
    """A comparable key whose later records alone fail spill serialization."""

    def __init__(self, value: int) -> None:
        self.value = value

    def __lt__(self, other: object) -> bool:
        assert isinstance(other, _LateUnpickleableSortKey)
        return self.value < other.value

    def __reduce_ex__(self, protocol: int) -> object:
        if self.value == 0:
            return (type(self), (self.value,))
        raise TypeError("later sort key result is process-local")


_sort_merge_error: ValueError | None = None


class _ExplodingMergeValue:
    """Pickleable value that fails only when separate identity runs are merged."""

    def __lt__(self, _other: object) -> bool:
        if _sort_merge_error is None:
            raise AssertionError("merge error must be configured by the test")
        raise _sort_merge_error


def test_equal_keys_keep_global_input_position() -> None:
    """Record position preserves stable ties even when reverse key order is requested."""
    records = [SortRecord(1, position, {"position": position}) for position in range(100)]

    result = sort_records(records, reverse=True)

    assert [record.value["position"] for record in result] == list(range(100))


def test_external_sort_evaluates_key_once_per_value(tmp_path) -> None:
    calls = 0

    def key(value: dict[str, int]) -> int:
        nonlocal calls
        calls += 1
        return value["key"]

    values = [{"key": value} for value in range(64, -1, -1)]

    assert flow(values).external_sort(key=key, buffer_size=1, tempdir=tmp_path).to_list() == sorted(
        values,
        key=lambda value: value["key"],
    )
    assert calls == len(values)


def test_external_sort_preserves_stability_across_one_record_runs(tmp_path) -> None:
    """Global encounter positions must advance before each short-lived run closes."""
    values = [{"key": 0, "position": position} for position in range(80)]

    result = flow(values).external_sort_by("key", buffer_size=1, tempdir=tmp_path).to_list()

    assert result == values


def test_external_sort_uses_only_less_than_for_cross_run_keys(tmp_path) -> None:
    """Valid ordering keys need not provide equality just to enter the merge heap."""
    values = [3, 1, 2, 1]
    for reverse, expected in ((False, [1, 1, 2, 3]), (True, [3, 2, 1, 1])):
        result = (
            flow(values)
            .external_sort(
                key=_LessThanOnlySortKey,
                reverse=reverse,
                buffer_size=1,
                tempdir=tmp_path,
            )
            .to_list()
        )

        assert result == expected


@pytest.mark.parametrize("reverse", [False, True])
def test_external_identity_sort_preserves_less_than_only_stable_ties(
    tmp_path, reverse: bool
) -> None:
    """Raw-value compaction keeps global stability without adding equality probes."""
    values = [_IdentityLessThanOnlyValue(position % 3, position) for position in range(80)]

    result = flow(values).external_sort(reverse=reverse, buffer_size=1, tempdir=tmp_path).to_list()

    expected = sorted(range(80), key=lambda position: position % 3, reverse=reverse)
    assert [value.position for value in result] == expected


def test_external_sort_preserves_a_falsey_callable_key(tmp_path) -> None:
    result = (
        flow([1, 3, 2])
        .external_sort(key=_FalseySortKey(), buffer_size=1, tempdir=tmp_path)
        .to_list()
    )

    assert result == [3, 2, 1]


def test_external_identity_sort_spills_each_value_without_a_duplicate_cached_key(
    tmp_path, monkeypatch
) -> None:
    """Identity runs use the value itself instead of serializing key/value duplicates."""
    from fpstreams.storage.codec import SpillCodec

    stored_types: set[type[object]] = set()
    write_records = SpillCodec.write_records

    def tracked_write_records(self, handle, values):
        stored_types.update(type(value) for value in values)
        return write_records(self, handle, values)

    monkeypatch.setattr(SpillCodec, "write_records", tracked_write_records)

    assert flow([3, 1, 2]).external_sort(buffer_size=1, tempdir=tmp_path).to_list() == [1, 2, 3]
    assert stored_types == {int}


def test_external_sort_falls_back_when_only_cached_key_results_are_unpickleable(tmp_path) -> None:
    values = [3, 1, 2]

    assert flow(values).external_sort(
        key=_UnpickleableSortKey,
        buffer_size=1,
        tempdir=tmp_path,
    ).to_list() == [1, 2, 3]
    assert list(tmp_path.iterdir()) == []


def test_external_sort_value_only_fallback_rewrites_completed_keyed_runs(tmp_path) -> None:
    values = [0, 2, 1]

    assert flow(values).external_sort(
        key=_LateUnpickleableSortKey,
        buffer_size=1,
        tempdir=tmp_path,
    ).to_list() == [0, 1, 2]
    assert list(tmp_path.iterdir()) == []


def test_external_sort_keeps_the_cached_key_path_for_an_unpickleable_callable(tmp_path) -> None:
    calls = 0

    def key(value: int) -> int:
        nonlocal calls
        calls += 1
        return value

    assert flow([3, 1, 2]).external_sort(key=key, buffer_size=1, tempdir=tmp_path).to_list() == [
        1,
        2,
        3,
    ]
    assert calls == 3


@pytest.mark.parametrize("reverse", [False, True])
def test_external_sort_value_only_fallback_preserves_multi_run_stability_and_cleanup(
    tmp_path, reverse: bool
) -> None:
    opened = 0
    values = [{"key": value % 3, "position": value} for value in range(80, 0, -1)]

    def source():
        nonlocal opened
        opened += 1
        yield from values

    def key(value: dict[str, int]) -> _UnpickleableSortKey:
        return _UnpickleableSortKey(value["key"])

    result = (
        flow(source())
        .external_sort(
            key=key,
            reverse=reverse,
            buffer_size=1,
            tempdir=tmp_path,
        )
        .to_list()
    )

    assert result == sorted(values, key=lambda value: value["key"], reverse=reverse)
    assert opened == 1
    assert list(tmp_path.iterdir()) == []


def test_external_sort_value_serialization_error_keeps_the_codec_as_its_cause(tmp_path) -> None:
    from fpstreams.storage import SpillSerializationError

    values = [(2, lambda: None), (1, lambda: None)]

    with pytest.raises(TypeError, match="values must be picklable") as captured:
        result = flow(values).external_sort(
            key=lambda value: value[0],
            buffer_size=1,
            tempdir=tmp_path,
        )
        result.to_list()

    assert isinstance(captured.value.__cause__, SpillSerializationError)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("transition", ["spill.mkdir.after", "resource.register.after"])
def test_external_sort_cleans_up_when_store_initialization_fails(tmp_path, transition: str) -> None:
    with (
        failpoint(transition, RuntimeError("store initialization failed")),
        pytest.raises(RuntimeError, match="store initialization failed"),
    ):
        flow([3, 2, 1]).external_sort(buffer_size=1, tempdir=tmp_path).to_list()

    assert list(tmp_path.iterdir()) == []


def test_external_sort_keeps_a_primary_error_when_cleanup_also_fails(tmp_path) -> None:
    def key(value: int) -> int:
        if value == 1:
            raise ValueError("primary sort failure")
        return value

    with (
        failpoint("resource.close.before", OSError("cleanup failure")),
        pytest.raises(ValueError, match="primary sort failure") as captured,
    ):
        flow([3, 2, 1]).external_sort(key=key, buffer_size=1, tempdir=tmp_path).to_list()

    assert captured.value.__notes__ == ["cleanup failed: OSError: cleanup failure"]


def test_external_sort_reader_cleanup_is_not_allowed_to_mask_comparison(
    tmp_path, monkeypatch
) -> None:
    """Every reader closes, while its failures become notes on the comparison error."""
    from fpstreams.runtime import TrackedBinaryFile

    global _sort_merge_error

    primary = ValueError("comparison primary")
    _sort_merge_error = primary
    close_calls = 0
    original_close = TrackedBinaryFile._close_owned

    def fail_after_reader_close(handle: TrackedBinaryFile) -> None:
        nonlocal close_calls
        is_reader = handle.readable()
        original_close(handle)
        if is_reader:
            close_calls += 1
            raise OSError(f"reader close failure {close_calls}")

    monkeypatch.setattr(TrackedBinaryFile, "_close_owned", fail_after_reader_close)
    try:
        with pytest.raises(ValueError, match="comparison primary") as captured:
            flow([_ExplodingMergeValue() for _position in range(3)]).external_sort(
                buffer_size=1,
                tempdir=tmp_path,
            ).to_list()
    finally:
        _sort_merge_error = None

    assert captured.value is primary
    assert captured.value.__notes__ == [
        "cleanup failed: OSError: reader close failure 1",
        "cleanup failed: OSError: reader close failure 2",
    ]
    assert close_calls == 2
    assert list(tmp_path.iterdir()) == []


def test_external_sort_does_not_relabel_a_user_type_error(tmp_path) -> None:
    primary = TypeError("spill record is not serializable: raised by user key")

    def key(value: int) -> int:
        if value == 1:
            raise primary
        return value

    with pytest.raises(TypeError) as captured:
        flow([3, 2, 1]).external_sort(key=key, buffer_size=1, tempdir=tmp_path).to_list()

    assert captured.value is primary


def test_external_sort_does_not_relabel_a_user_spill_error_from_the_source(tmp_path) -> None:
    """The exported codec error remains user-owned when a source deliberately raises it."""
    from fpstreams.storage import SpillSerializationError

    primary = SpillSerializationError("raised by source")

    def source():
        yield 3
        yield 2
        raise primary

    with pytest.raises(SpillSerializationError) as captured:
        flow(source()).external_sort(buffer_size=1, tempdir=tmp_path).to_list()

    assert captured.value is primary


def test_external_sort_does_not_relabel_a_user_spill_error_from_the_key(tmp_path) -> None:
    """Only errors created by serialization itself may enter codec recovery."""
    from fpstreams.storage import SpillSerializationError

    primary = SpillSerializationError("raised by key")

    def key(value: int) -> int:
        if value == 1:
            raise primary
        return value

    with pytest.raises(SpillSerializationError) as captured:
        flow([3, 2, 1]).external_sort(key=key, buffer_size=1, tempdir=tmp_path).to_list()

    assert captured.value is primary


def test_external_sort_does_not_relabel_a_user_spill_error_from_comparison(tmp_path) -> None:
    """Comparison failures keep their identity even inside a spill-run construction."""
    from fpstreams.storage import SpillSerializationError

    primary = SpillSerializationError("raised by comparison")

    class ExplodingComparison:
        def __lt__(self, _other: object) -> bool:
            raise primary

    values = [ExplodingComparison(), ExplodingComparison(), ExplodingComparison()]
    with pytest.raises(SpillSerializationError) as captured:
        flow(values).external_sort(buffer_size=2, tempdir=tmp_path).to_list()

    assert captured.value is primary


def test_numeric_runs_match_stable_python_order() -> None:
    cases = [
        [3, 1, 2, 1, 0],
        [-(2**63), 9, 0, 2**63 - 1, -1] * 1000,
    ]
    for keys in cases:
        records = [SortRecord(key, position, position) for position, key in enumerate(keys)]
        expected = sorted(records, key=lambda record: (record.key, record.position))
        assert sort_records(records, reverse=False) == expected


def test_external_sort_compiles_an_explicit_physical_node(tmp_path) -> None:
    query = flow([3, 1, 2]).external_sort(buffer_size=1, tempdir=tmp_path)._query("list")
    physical = compile_query(query)

    assert isinstance(physical.nodes[0], SortPhysicalNode)
    assert physical.nodes[0].strategy is SortStrategy.CACHED_EXTERNAL_MERGE


def test_external_selector_sort_explains_cached_strategy(tmp_path) -> None:
    explanation = (
        flow([{"score": 2}, {"score": 1}])
        .external_sort_by("score", buffer_size=1, tempdir=tmp_path)
        .explain()
        .to_dict()
    )

    assert explanation["stages"] == [
        {
            "engine": "python-spill",
            "operations": ["external_sort"],
            "fused": False,
        }
    ]


def test_repeated_external_sort_explain_uses_physical_node_order(tmp_path) -> None:
    """Equal operation names must not send every physical engine to the first stage."""

    from fpstreams.planning.explain import explain_physical

    query = (
        flow([3, 1, 2])
        .external_sort(buffer_size=1, tempdir=tmp_path)
        .external_sort(reverse=True, buffer_size=1, tempdir=tmp_path)
        ._query("list")
    )
    physical = compile_query(query)
    first, second = physical.nodes
    assert isinstance(first, SortPhysicalNode)
    assert isinstance(second, SortPhysicalNode)
    assert first.operation.name == second.operation.name == "external_sort"

    distinct_engines = replace(
        physical,
        nodes=(
            replace(first, engine="first-sort-engine"),
            replace(second, engine="second-sort-engine"),
        ),
    )

    assert explain_physical(distinct_engines).to_dict()["stages"] == [
        {
            "engine": "first-sort-engine",
            "operations": ["external_sort"],
            "fused": False,
        },
        {
            "engine": "second-sort-engine",
            "operations": ["external_sort"],
            "fused": False,
        },
    ]


def test_retained_arrow_direct_sort_compiles_a_stable_columnar_strategy() -> None:
    """Removing source-aware sort selection would put both retained sources back on rows."""
    pa = pytest.importorskip("pyarrow")

    sources = (
        pa.table({"key": [2, 1], "position": [0, 1]}),
        pa.record_batch({"key": [2, 1], "position": [0, 1]}),
    )
    for source in sources:
        query = fpstreams.rows.from_arrow(source).sort_by("key")._flow
        physical = compile_query(query._query("list"))

        assert len(physical.nodes) == 1
        assert isinstance(physical.nodes[0], SortPhysicalNode)
        assert physical.nodes[0].strategy is SortStrategy.ARROW_STABLE
        assert physical.nodes[0].engine == "arrow"


@pytest.mark.parametrize("as_batch", [False, True])
@pytest.mark.parametrize(
    ("reverse", "expected"),
    [
        (False, ["b", "d", "a", "c"]),
        (True, ["a", "c", "b", "d"]),
    ],
)
def test_retained_arrow_direct_sort_keeps_python_tie_order(
    as_batch: bool,
    reverse: bool,
    expected: list[str],
) -> None:
    """A descending key must never reverse records that compare equal."""
    pa = pytest.importorskip("pyarrow")
    table = pa.table(
        {
            "key": pa.array([2, 1, 2, 1], type=pa.int64()),
            "id": ["a", "b", "c", "d"],
        }
    )
    source = table.to_batches()[0] if as_batch else table

    result = fpstreams.rows.from_arrow(source).sort_by("key", reverse=reverse).to_list()

    assert [row["id"] for row in result] == expected


@pytest.mark.parametrize(
    ("reverse", "expected"),
    [
        (False, ["b", "d", "a", "c"]),
        (True, ["a", "c", "b", "d"]),
    ],
)
def test_retained_arrow_direct_sort_keeps_ties_stable_across_chunks(
    reverse: bool,
    expected: list[str],
) -> None:
    """The stable order proof must cover Arrow's cross-chunk merge, not only one batch."""
    pa = pytest.importorskip("pyarrow")
    table = pa.table(
        {
            "key": pa.chunked_array([[2, 1], [2, 1]], type=pa.int64()),
            "id": pa.chunked_array([["a", "b"], ["c", "d"]], type=pa.string()),
        }
    )

    result = fpstreams.rows.from_arrow(table).sort_by("key", reverse=reverse).to_list()

    assert [row["id"] for row in result] == expected


@pytest.mark.parametrize(
    ("key_type", "keys", "expected_positions"),
    [
        ("bool", [True, False, True, False], [1, 3, 0, 2]),
        ("int64", [2, 1, 2, 1], [1, 3, 0, 2]),
        ("uint64", [2**64 - 1, 0, 2**64 - 1, 0], [1, 3, 0, 2]),
        ("string", ["b", "a", "b", "a"], [1, 3, 0, 2]),
        ("large_string", ["b", "a", "b", "a"], [1, 3, 0, 2]),
        ("binary", [b"b", b"a", b"b", b"a"], [1, 3, 0, 2]),
        ("large_binary", [b"b", b"a", b"b", b"a"], [1, 3, 0, 2]),
    ],
)
def test_retained_arrow_direct_sort_accepts_only_proven_builtin_key_orders(
    key_type: str,
    keys: list[object],
    expected_positions: list[int],
) -> None:
    """Each admitted Arrow key family must have the same order as builtin Python scalars."""
    pa = pytest.importorskip("pyarrow")
    table = pa.table(
        {
            "key": pa.array(keys, type=pa.type_for_alias(key_type)),
            "position": range(len(keys)),
        }
    )

    result = fpstreams.rows.from_arrow(table).sort_by("key").to_list()

    assert [row["position"] for row in result] == expected_positions


def test_retained_arrow_direct_sort_accepts_fixed_size_binary_without_row_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fixed-width byte keys have the same lexicographic order and need no row path."""
    pa = pytest.importorskip("pyarrow")
    from fpstreams.planning.arrow.source import ArrowBatchSource
    from fpstreams.planning.source import Source

    open_rows = Source.open

    def reject_rows(source: Source[object]):
        if isinstance(source.native_data, ArrowBatchSource):
            raise AssertionError("a proven fixed-size binary key reopened Python rows")
        return open_rows(source)

    monkeypatch.setattr(Source, "open", reject_rows)
    table = pa.table(
        {
            "key": pa.array([b"bb", b"aa", b"bb", b"aa"], type=pa.binary(2)),
            "position": [0, 1, 2, 3],
        }
    )

    result = fpstreams.rows.from_arrow(table).sort_by("key").to_list()

    assert [row["position"] for row in result] == [1, 3, 0, 2]


def test_retained_arrow_sort_list_avoids_physical_row_forwarding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The list terminal should extend converted batches instead of yielding every row."""
    pa = pytest.importorskip("pyarrow")
    from fpstreams.streams import flow_terminals

    def reject_physical_rows(_plan):
        raise AssertionError("direct Arrow sort list entered the row-forwarding executor")

    monkeypatch.setattr(flow_terminals, "execute_physical", reject_physical_rows)
    table = pa.table({"key": [2, 1], "position": [0, 1]})

    assert fpstreams.rows.from_arrow(table).sort_by("key").to_list() == [
        {"key": 1, "position": 1},
        {"key": 2, "position": 0},
    ]


def test_ordinary_list_does_not_load_the_arrow_sort_terminal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-sort list must reject the specialization before importing its Arrow executor."""

    monkeypatch.setitem(sys.modules, "fpstreams.execution.arrow", None)

    assert fpstreams.flow([2, 1]).to_list() == [2, 1]


def test_retained_arrow_direct_sort_falls_back_for_null_and_nan_semantics() -> None:
    """Arrow null/NaN placement must not replace Python's comparisons."""
    pa = pytest.importorskip("pyarrow")

    nullable = pa.table({"key": [2, None, 1], "position": [0, 1, 2]})
    with pytest.raises(TypeError) as canonical:
        fpstreams.rows.from_arrow(nullable).with_engine("python").sort_by("key").to_list()
    with pytest.raises(TypeError) as automatic:
        fpstreams.rows.from_arrow(nullable).sort_by("key").to_list()
    assert str(automatic.value) == str(canonical.value)
    assert automatic.value.__cause__ is None
    assert automatic.value.__context__ is None

    nan = float("nan")
    floating = pa.table({"key": [nan, 2.0, 1.0, nan, -1.0], "position": [0, 1, 2, 3, 4]})
    canonical_result = (
        fpstreams.rows.from_arrow(floating).with_engine("python").sort_by("key").to_list()
    )
    result = fpstreams.rows.from_arrow(floating).sort_by("key").to_list()
    assert [row["position"] for row in result] == [row["position"] for row in canonical_result]


def test_retained_arrow_direct_sort_keeps_missing_and_nested_comparison_errors() -> None:
    """Schema rejection must re-enter selection and comparison at the Python boundary."""
    pa = pytest.importorskip("pyarrow")

    nonempty = fpstreams.rows.from_arrow(pa.table({"present": [2, 1]}))
    with pytest.raises(fpstreams.SelectionError) as captured:
        nonempty.sort_by("missing").to_list()
    assert str(captured.value) == "Could not resolve selector 'missing'; failed at 'missing'"
    assert type(captured.value.__cause__) is KeyError
    assert captured.value.__cause__.args == ("missing",)

    empty = pa.table({"present": pa.array([], type=pa.int64())})
    assert fpstreams.rows.from_arrow(empty).sort_by("missing").to_list() == []

    nested = pa.table(
        {
            "key": pa.array(
                [{"value": 2}, {"value": 1}],
                type=pa.struct([("value", pa.int64())]),
            )
        }
    )
    with pytest.raises(TypeError) as canonical:
        fpstreams.rows.from_arrow(nested).with_engine("python").sort_by("key").to_list()
    with pytest.raises(TypeError) as automatic:
        fpstreams.rows.from_arrow(nested).sort_by("key").to_list()
    assert str(automatic.value) == str(canonical.value)


def test_retained_arrow_direct_sort_validates_before_reordering_row_conversion_errors() -> None:
    """Malformed primitive buffers must fail before sorting changes the first observed error."""
    pa = pytest.importorskip("pyarrow")
    offsets = pa.array([0, 1, 2], type=pa.int32()).buffers()[1]
    malformed = pa.Array.from_buffers(
        pa.string(),
        2,
        [None, offsets, pa.py_buffer(b"\xffa")],
    )
    table = pa.table({"key": [2, 1], "payload": malformed})

    with pytest.raises(UnicodeDecodeError) as canonical:
        (
            fpstreams.rows.from_arrow(table, batch_size=1)
            .with_engine("python")
            .sort_by("key")
            ._flow.to_set()
        )
    with pytest.raises(UnicodeDecodeError) as automatic:
        fpstreams.rows.from_arrow(table, batch_size=1).sort_by("key")._flow.to_set()

    assert automatic.value.args == canonical.value.args


def test_retained_arrow_direct_sort_rejects_forced_callback_pipeline_and_failpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only an uninstrumented auto plan with one exact direct selector may skip row keys."""
    pa = pytest.importorskip("pyarrow")
    from fpstreams.runtime.failpoints import failpoint

    table = pa.table({"key": [2, 1], "position": [0, 1]})
    forced = fpstreams.rows.from_arrow(table).with_engine("python").sort_by("key")._flow
    forced_plan = compile_query(forced._query("list"))
    assert isinstance(forced_plan.nodes[0], SortPhysicalNode)
    assert forced_plan.nodes[0].strategy is SortStrategy.IN_MEMORY

    selected: list[int] = []

    def select(row: dict[str, int]) -> int:
        selected.append(row["key"])
        return row["key"]

    assert fpstreams.rows.from_arrow(table).sort_by(select).to_list() == [
        {"key": 1, "position": 1},
        {"key": 2, "position": 0},
    ]
    assert selected == [2, 1]

    retained: list[int] = []

    def keep(row: dict[str, int]) -> bool:
        retained.append(row["position"])
        return True

    pipeline = fpstreams.rows.from_arrow(table).filter(keep).sort_by("key")
    assert pipeline.to_list() == [
        {"key": 1, "position": 1},
        {"key": 2, "position": 0},
    ]
    assert retained == [0, 1]
    physical = compile_query(pipeline._flow._query("list"))
    assert len(physical.nodes) == 2
    assert isinstance(physical.nodes[-1], SortPhysicalNode)
    assert physical.nodes[-1].strategy is SortStrategy.IN_MEMORY

    primary = RuntimeError("canonical Arrow source open")
    with (
        failpoint("source.open.after", primary),
        pytest.raises(RuntimeError) as captured,
    ):
        fpstreams.rows.from_arrow(table).sort_by("key").to_list()
    assert captured.value is primary

    class Field(str):
        pass

    subclass = fpstreams.rows.from_arrow(table).sort_by(Field("key"))._flow
    subclass_plan = compile_query(subclass._query("list"))
    assert isinstance(subclass_plan.nodes[0], SortPhysicalNode)
    assert subclass_plan.nodes[0].strategy is SortStrategy.IN_MEMORY


def test_retained_arrow_direct_sort_keeps_reverse_protocol_after_row_boxing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A protocol-bearing reverse option must retain canonical evaluation order."""
    pa = pytest.importorskip("pyarrow")
    from fpstreams.tabular import arrow as arrow_adapter

    events: list[str] = []
    canonical_batch_to_rows = arrow_adapter.batch_to_rows

    def tracked_batch_to_rows(batch):
        events.append("rows")
        return canonical_batch_to_rows(batch)

    class Reverse:
        def __bool__(self) -> bool:
            events.append("reverse")
            return True

        def __index__(self) -> int:
            events.append("reverse")
            return 1

    monkeypatch.setattr(arrow_adapter, "batch_to_rows", tracked_batch_to_rows)
    table = pa.table({"key": [2, 1], "position": [0, 1]})

    result = fpstreams.rows.from_arrow(table).sort_by("key", reverse=Reverse()).to_list()  # type: ignore[arg-type]

    assert result == [{"key": 2, "position": 0}, {"key": 1, "position": 1}]
    assert events == ["rows", "reverse"]


def test_retained_arrow_direct_sort_falls_back_after_expected_kernel_decline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A replayable retained source can safely restart through canonical rows."""
    pa = pytest.importorskip("pyarrow")
    pc = pytest.importorskip("pyarrow.compute")
    table = pa.table({"key": [2, 1], "position": [0, 1]})

    def decline(*_args, **_kwargs):
        raise ValueError("unsupported sort kernel")

    monkeypatch.setattr(pc, "sort_indices", decline)

    assert fpstreams.rows.from_arrow(table).sort_by("key").to_list() == [
        {"key": 1, "position": 1},
        {"key": 2, "position": 0},
    ]


def test_retained_arrow_direct_sort_propagates_memory_error_and_remains_replayable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Resource exhaustion propagates, while successful retained sources remain reusable."""
    pa = pytest.importorskip("pyarrow")
    pc = pytest.importorskip("pyarrow.compute")
    table = pa.table({"key": [2, 1], "position": [0, 1]})
    rows = fpstreams.rows.from_arrow(table)
    primary = MemoryError("sort allocation")

    with monkeypatch.context() as patch:
        patch.setattr(pc, "sort_indices", lambda *_args, **_kwargs: (_ for _ in ()).throw(primary))
        with pytest.raises(MemoryError) as captured:
            rows.sort_by("key").to_list()
        assert captured.value is primary

    expected = [{"key": 1, "position": 1}, {"key": 2, "position": 0}]
    assert rows.sort_by("key").to_list() == expected
    assert rows.with_engine("python").sort_by("key").to_list() == expected


@pytest.mark.parametrize(
    ("reverse", "expected_ids"),
    [(False, ["b", "d", "a", "c"]), (True, ["a", "c", "b", "d"])],
)
def test_in_memory_direct_field_sort_uses_exact_dict_guard_and_keeps_stable_ties(
    monkeypatch: pytest.MonkeyPatch,
    reverse: bool,
    expected_ids: list[str],
) -> None:
    """Removing the guarded native key lane would leave both native ABIs unused."""
    from operator import itemgetter

    from fpstreams import _native

    records = [
        {"key": 2, "id": "a"},
        {"key": 1, "id": "b"},
        {"key": 2, "id": "c"},
        {"key": 1, "id": "d"},
    ]
    guarded: list[tuple[int, ...]] = []
    keyed: list[tuple[str, type[BaseException]]] = []

    def exact_dict_rows(source: list[object]) -> bool:
        guarded.append(tuple(id(row) for row in source))
        return all(type(row) is dict for row in source)

    def direct_key(field: str, error_type: type[BaseException]):
        keyed.append((field, error_type))
        return itemgetter(field)

    monkeypatch.setattr(_native, "all_exact_dict_rows_v1", exact_dict_rows, raising=False)
    monkeypatch.setattr(_native, "direct_dict_field_key_v1", direct_key, raising=False)

    result = fpstreams.rows(records).sort_by("key", reverse=reverse).to_list()

    assert [row["id"] for row in result] == expected_ids
    assert guarded == [tuple(id(row) for row in records)]
    assert keyed == [("key", fpstreams.SelectionError)]


def test_in_memory_direct_field_sort_preserves_selection_error_timing_and_cause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed direct lookup still consumes the source first and raises the canonical error."""
    from fpstreams import _native

    pulls: list[int] = []
    guard_calls = 0

    def source():
        for index, row in enumerate([{"key": 2}, {"missing": 0}, {"key": 1}]):
            pulls.append(index)
            yield row

    def exact_dict_rows(_source: list[object]) -> bool:
        nonlocal guard_calls
        guard_calls += 1
        return True

    monkeypatch.setattr(_native, "all_exact_dict_rows_v1", exact_dict_rows, raising=False)

    with pytest.raises(fpstreams.SelectionError) as captured:
        fpstreams.rows(source()).sort_by("key").to_list()

    assert str(captured.value) == "Could not resolve selector 'key'; failed at 'key'"
    assert type(captured.value.__cause__) is KeyError
    assert captured.value.__cause__.args == ("key",)
    assert captured.value.__context__ is captured.value.__cause__
    assert captured.value.__suppress_context__
    assert pulls == [0, 1, 2]
    assert guard_calls == 1


def test_in_memory_direct_field_sort_keeps_comparison_type_error_raw() -> None:
    """The native key lane must not relabel a comparison failure as field selection."""
    records = [{"key": 1}, {"key": "two"}]

    with pytest.raises(TypeError) as canonical:
        fpstreams.rows(records).sort_by(lambda row: row["key"]).to_list()
    with pytest.raises(TypeError) as direct:
        fpstreams.rows(records).sort_by("key").to_list()

    assert str(direct.value) == str(canonical.value)
    assert direct.value.__cause__ is None
    assert direct.value.__context__ is None


def test_in_memory_direct_field_sort_keeps_lookup_type_error_at_selection_boundary() -> None:
    """A colliding custom key still wraps its lookup TypeError inside the native callback."""
    primary = TypeError("lookup equality failed")

    class Collision:
        def __hash__(self) -> int:
            return hash("key")

        def __eq__(self, _other: object) -> bool:
            raise primary

    with pytest.raises(fpstreams.SelectionError) as captured:
        fpstreams.rows([{Collision(): 1}]).sort_by("key").to_list()

    assert str(captured.value) == "Could not resolve selector 'key'; failed at 'key'"
    assert captured.value.__cause__ is primary
    assert captured.value.__context__ is primary
    assert captured.value.__suppress_context__


def test_in_memory_direct_field_sort_declines_mixed_rows_before_selector_effects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed exact-dict proof must retain Mapping and attribute lookup order."""

    from fpstreams import _native

    effects: list[tuple[object, ...]] = []
    guard_calls = 0

    class LoggedMapping(Mapping[str, int]):
        def __init__(self, value: int) -> None:
            self.value = value

        def __getitem__(self, field: str) -> int:
            effects.append(("mapping", self.value, field))
            return self.value

        def __iter__(self):
            return iter(("key",))

        def __len__(self) -> int:
            return 1

    class LoggedAttribute:
        def __init__(self, value: int) -> None:
            self._value = value

        @property
        def key(self) -> int:
            effects.append(("attribute", self._value))
            return self._value

    def decline(_source: list[object]) -> bool:
        nonlocal guard_calls
        guard_calls += 1
        return False

    mapping = LoggedMapping(1)
    attribute = LoggedAttribute(2)
    exact = {"key": 3}
    monkeypatch.setattr(_native, "all_exact_dict_rows_v1", decline, raising=False)

    result = fpstreams.rows([exact, mapping, attribute]).sort_by("key").to_list()

    assert result == [mapping, attribute, exact]
    assert effects == [("mapping", 1, "key"), ("attribute", 2)]
    assert guard_calls == 1


def test_in_memory_direct_field_sort_declines_dict_subclasses_before_getitem(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A dict subclass keeps the generated selector and its observable protocol calls."""
    from fpstreams import _native

    effects: list[tuple[int, str]] = []
    guard_calls = 0

    class Record(dict[str, int]):
        def __getitem__(self, field: str) -> int:
            value = super().__getitem__(field)
            effects.append((value, field))
            return value

    native_guard = _native.all_exact_dict_rows_v1

    def tracked_guard(source: object) -> bool:
        nonlocal guard_calls
        guard_calls += 1
        return native_guard(source)

    monkeypatch.setattr(_native, "all_exact_dict_rows_v1", tracked_guard)
    records = [Record(key=2), Record(key=1)]

    assert fpstreams.rows(records).sort_by("key").to_list() == [records[1], records[0]]
    assert effects == [(2, "key"), (1, "key")]
    assert guard_calls == 1


def test_direct_field_sort_rejects_string_subclasses_and_forged_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only an exact string compiled with the private identity token may enter the lane."""
    from fpstreams import _native

    class Field(str):
        pass

    def unexpected_guard(_source: object) -> bool:
        raise AssertionError("untrusted selector metadata reached the native guard")

    monkeypatch.setattr(_native, "all_exact_dict_rows_v1", unexpected_guard)
    records = [{"key": 2}, {"key": 1}]
    assert fpstreams.rows(records).sort_by(Field("key")).to_list() == [records[1], records[0]]

    effects: list[int] = []

    def forged(row: dict[str, int]) -> int:
        effects.append(row["key"])
        return row["key"]

    forged.__fpstreams_direct_field_v1__ = (object(), "key")  # type: ignore[attr-defined]
    assert fpstreams.rows(records).sort_by(forged).to_list() == [records[1], records[0]]
    assert effects == [2, 1]


def test_native_exact_dict_sort_guard_checks_types_without_protocol_dispatch() -> None:
    """The speculative ABI proves only exact container types without row protocol calls."""
    from fpstreams import _native

    class Record(dict[str, int]):
        def __getitem__(self, key: str) -> int:
            raise AssertionError(f"record protocol dispatched for {key}")

    class RecordList(list[dict[str, int]]):
        def __iter__(self):
            raise AssertionError("list protocol dispatched")

    assert _native.all_exact_dict_rows_v1([{}, {"key": 1}])
    assert not _native.all_exact_dict_rows_v1(({},))
    assert not _native.all_exact_dict_rows_v1([Record(key=1)])
    assert not _native.all_exact_dict_rows_v1(RecordList([{"key": 1}]))
    assert _native.all_exact_dict_rows_v1([{"key": 1, 2: 3}])


def test_in_memory_direct_field_sort_keeps_old_wheel_and_opaque_selector_fallbacks(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """The optional ABI cannot become required or leak into callable and external sorts."""
    from fpstreams import _native

    records = [{"key": 2}, {"key": 1}]
    for missing_abi in ("all_exact_dict_rows_v1", "direct_dict_field_key_v1"):
        with monkeypatch.context() as patch:
            patch.delattr(_native, missing_abi, raising=False)
            assert fpstreams.rows(records).sort_by("key").to_list() == [records[1], records[0]]

    def unexpected_guard(_source: object) -> bool:
        raise AssertionError("opaque and external sorts must not request an exact-dict proof")

    def unexpected_key(_field: object, _error_type: object) -> object:
        raise AssertionError("opaque and external sorts must not request a direct-field key")

    monkeypatch.setattr(_native, "all_exact_dict_rows_v1", unexpected_guard, raising=False)
    monkeypatch.setattr(_native, "direct_dict_field_key_v1", unexpected_key, raising=False)
    assert fpstreams.rows(records).sort_by(lambda row: row["key"]).to_list() == [
        records[1],
        records[0],
    ]
    assert fpstreams.rows(records).external_sort_by(
        "key", buffer_size=1, tempdir=tmp_path
    ).to_list() == [records[1], records[0]]
