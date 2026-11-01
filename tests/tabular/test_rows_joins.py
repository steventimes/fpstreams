"""Rows hash and sorted join ordering, snapshots, and protocol contracts."""

from __future__ import annotations

import gc
import weakref
from collections.abc import Iterator, Mapping
from typing import Any

import pytest

import fpstreams
from fpstreams import flow


def test_rows_join_and_select_replace_nested_lookup_loops() -> None:
    orders = [
        {"order_id": 10, "user_id": 1, "amount": 50},
        {"order_id": 11, "user_id": 3, "amount": 20},
    ]
    users = [
        {"user_id": 1, "name": "Ada"},
        {"user_id": 2, "name": "Lin"},
    ]

    joined = (
        fpstreams.rows(orders)
        .join(users, on="user_id", how="left")
        .select("order_id", "name", total="amount")
        .to_list()
    )

    assert joined == [
        {"order_id": 10, "name": "Ada", "total": 50},
        {"order_id": 11, "name": None, "total": 20},
    ]


def test_common_rows_joins_stream_the_left_side() -> None:
    opened: list[int] = []

    def left_rows():
        for value in range(100):
            opened.append(value)
            yield {"id": value}

    joined = fpstreams.rows(left_rows()).join([{"id": 0, "name": "zero"}], on="id")
    iterator = iter(joined)

    assert next(iterator) == {"id": 0, "name": "zero"}
    assert opened == [0]
    iterator.close()


def test_join_keeps_a_right_flow_root_and_its_one_shot_native_source() -> None:
    from fpstreams.planning.logical import JoinNode, SourceNode
    from fpstreams.planning.source import Source, SourceCapabilities

    right_records = [{"id": 1, "right": "A"}]
    native_source = Source(
        lambda: iter(right_records),
        SourceCapabilities(reiterable=False, exact_size=1),
        native_data=right_records,
    )
    right = fpstreams.Flow(native_source)
    right_root = right._logical_plan.root

    joined = fpstreams.rows([{"id": 1, "left": "a"}]).join(right, on="id")
    join_root = joined._flow._logical_plan.root

    assert isinstance(join_root, JoinNode)
    assert join_root.right is right_root
    assert isinstance(join_root.right, SourceNode)
    assert join_root.right.source is native_source
    assert join_root.right.source.native_data is right_records
    assert joined.to_list() == [{"id": 1, "left": "a", "right": "A"}]
    with pytest.raises(fpstreams.FlowConsumedError):
        right.to_list()
    with pytest.raises(fpstreams.FlowConsumedError):
        joined.to_list()


def test_semi_join_indexes_only_keys_without_retaining_right_payloads() -> None:
    class Payload:
        pass

    references: list[weakref.ReferenceType[Payload]] = []

    def right_rows():
        for value in range(20):
            payload = Payload()
            references.append(weakref.ref(payload))
            yield {"id": value, "payload": payload}

    joined = fpstreams.rows([{"id": 0}]).join(right_rows(), on="id", how="semi")
    iterator = iter(joined)
    assert next(iterator) == {"id": 0}
    gc.collect()
    assert all(reference() is None for reference in references)
    iterator.close()


def test_join_modes_preserve_duplicate_key_order_and_cardinality() -> None:
    left = [
        {"id": 1, "left": "a"},
        {"id": 1, "left": "b"},
        {"id": 3, "left": "c"},
    ]
    right = [
        {"id": 1, "right": "x"},
        {"id": 1, "right": "y"},
        {"id": 2, "right": "z"},
    ]
    matched = [
        {"id": 1, "left": "a", "right": "x"},
        {"id": 1, "left": "a", "right": "y"},
        {"id": 1, "left": "b", "right": "x"},
        {"id": 1, "left": "b", "right": "y"},
    ]

    def join(how: str):
        return fpstreams.rows(left).join(right, on="id", how=how).to_list()

    assert join("inner") == matched
    assert join("left") == [*matched, {"id": 3, "left": "c", "right": None}]
    assert join("right") == [*matched, {"id": 2, "left": None, "right": "z"}]
    assert join("full") == [
        *matched,
        {"id": 3, "left": "c", "right": None},
        {"id": 2, "left": None, "right": "z"},
    ]
    assert join("semi") == left[:2]
    assert join("anti") == left[2:]


def test_left_driven_join_outputs_own_independent_snapshots() -> None:
    """Snapshot reuse must never expose an input dict or alias duplicate outputs."""
    payload: list[int] = [1]
    source = {"id": 1, "left": payload}

    singleton = fpstreams.rows([source]).join([{"id": 1, "right": "x"}], on="id").to_list()
    duplicates = (
        fpstreams.rows([source])
        .join(
            [{"id": 1, "right": "x"}, {"id": 1, "right": "y"}],
            on="id",
        )
        .to_list()
    )
    semi = fpstreams.rows([source]).join([{"id": 1}], on="id", how="semi").to_list()

    assert singleton == [{"id": 1, "left": payload, "right": "x"}]
    assert singleton[0] is not source
    assert singleton[0]["left"] is payload
    assert duplicates[0] is not duplicates[1]
    duplicates[0]["right"] = "changed"
    assert duplicates[1]["right"] == "y"
    assert semi == [source]
    assert semi[0] is not source
    assert source == {"id": 1, "left": payload}


@pytest.mark.parametrize("engine", ["python", "auto"])
@pytest.mark.parametrize("safe_names", [False, True])
def test_general_left_join_retains_its_snapshot_after_output_mutation(
    engine: str, safe_names: bool
) -> None:
    """The next source row sees values retained by the unmatched row's private snapshot."""

    class Marker:
        pass

    class FieldName(str):
        pass

    payload_name = "payload" if safe_names else FieldName("payload")
    references = []
    closed = []

    def left_source():
        try:
            row = {"id": 1, payload_name: Marker()}
            references.append(weakref.ref(row[payload_name]))
            yield row
            yield {"id": 3, "previous_payload_alive": references[0]() is not None}
        finally:
            closed.append("left")

    def right_source():
        try:
            yield {"id": 2, "right": True}
        finally:
            closed.append("right")

    def select_left(row):
        row.pop(payload_name, None)
        return row["id"]

    iterator = iter(
        fpstreams.rows(left_source())
        .with_engine(engine)
        .join(
            right_source(),
            left_on=select_left,
            right_on="id",
            how="left",
            validate="m:m",
        )
    )
    try:
        first = next(iterator)
        assert closed == ["right"]
        first.pop(payload_name)
        assert references[0]() is not None
        second = next(iterator)
        assert second == {
            "id": 3,
            "previous_payload_alive": True,
            "id_right": None,
            "right": None,
        }
        assert references[0]() is None
    finally:
        iterator.close()
    assert closed == ["right", "left"]


def test_mapping_proxy_records_keep_dict_conversion_protocol_and_owned_join_outputs() -> None:
    """The exact proxy fast path remains equivalent to dict(proxy), including callbacks."""
    from types import MappingProxyType

    from fpstreams.tabular.records import _as_record

    events: list[tuple[str, object]] = []

    class TracedMapping(Mapping[str, object]):
        def __init__(self, values: dict[str, object]) -> None:
            self.values = values

        def __iter__(self) -> Iterator[str]:
            events.append(("iter", None))
            return iter(self.values)

        def __len__(self) -> int:
            events.append(("len", None))
            return len(self.values)

        def __getitem__(self, key: str) -> object:
            events.append(("getitem", key))
            return self.values[key]

    traced = MappingProxyType(TracedMapping({"id": 1, "left": "L"}))
    events.clear()
    expected = dict(traced)
    expected_events = list(events)
    events.clear()

    assert _as_record(traced) == expected
    assert events == expected_events

    left_values = {"id": 1, "left": "L"}
    right_values = {"id": 1, "right": "R"}
    left = MappingProxyType(left_values)
    result = (
        fpstreams.rows([left])
        .join(
            [MappingProxyType(right_values)],
            left_on=lambda row: row["id"],
            right_on=lambda row: row["id"],
        )
        .to_list()
    )

    assert result == [{"id": 1, "left": "L", "id_right": 1, "right": "R"}]
    assert type(result[0]) is dict
    result[0]["left"] = "changed"
    assert left_values == {"id": 1, "left": "L"}


def test_join_mapping_classification_cache_keeps_abc_guard_boundaries(  # noqa: C901
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only an ordinary non-virtual Mapping class may skip repeated ABC classification."""
    from abc import ABCMeta

    from fpstreams.tabular import join as join_module

    class DirectMapping(Mapping[str, object]):
        def __init__(self, identifier: int) -> None:
            self.values = {"id": identifier, "payload": identifier}

        def __iter__(self) -> Iterator[str]:
            return iter(self.values)

        def __len__(self) -> int:
            return len(self.values)

        def __getitem__(self, key: str) -> object:
            return self.values[key]

    class VirtualMapping:
        def __init__(self, identifier: int) -> None:
            self.values = {"id": identifier, "payload": identifier}

        def __iter__(self) -> Iterator[str]:
            return iter(self.values)

        def __len__(self) -> int:
            return len(self.values)

        def __getitem__(self, key: str) -> object:
            return self.values[key]

        def keys(self) -> object:
            return self.values.keys()

    Mapping.register(VirtualMapping)

    class CustomMeta(ABCMeta):
        pass

    class CustomMetaMapping(Mapping[str, object], metaclass=CustomMeta):
        def __init__(self, identifier: int) -> None:
            self.values = {"id": identifier, "payload": identifier}

        def __iter__(self) -> Iterator[str]:
            return iter(self.values)

        def __len__(self) -> int:
            return len(self.values)

        def __getitem__(self, key: str) -> object:
            return self.values[key]

    canonical = join_module._as_record
    classified: list[type[object]] = []

    def tracked(row: object) -> dict[str, Any]:
        classified.append(type(row))
        return canonical(row)

    monkeypatch.setattr(join_module, "_as_record", tracked)

    direct = [DirectMapping(index) for index in range(3)]
    _columns, _index, slots = join_module._join_record_index(
        direct, lambda row: row["id"], validate="m:m"
    )
    assert classified == [DirectMapping]
    assert [record["payload"] for record in slots] == [0, 1, 2]

    for row_type in (VirtualMapping, CustomMetaMapping):
        classified.clear()
        values = [row_type(index) for index in range(3)]
        join_module._join_record_index(values, lambda row: row["id"], validate="m:m")
        assert classified == [row_type, row_type, row_type]


def test_plain_field_join_reuses_proven_nominal_mapping_classification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A proven nominal Mapping should not repeat the selector's Mapping ABC check."""
    from fpstreams.expressions import selectors

    class DirectMapping(Mapping[str, object]):
        def __init__(self, **values: object) -> None:
            self.values = values

        def __iter__(self) -> Iterator[str]:
            return iter(self.values)

        def __len__(self) -> int:
            return len(self.values)

        def __getitem__(self, key: str) -> object:
            return self.values[key]

    class RejectingMeta(type):
        def __instancecheck__(cls, instance: object) -> bool:
            raise AssertionError(f"repeated Mapping check for {type(instance).__name__}")

    class RejectingMapping(metaclass=RejectingMeta):
        pass

    left = [DirectMapping(id=1, left="a"), DirectMapping(id=2, left="b")]
    right = [DirectMapping(id=1, right="x"), DirectMapping(id=2, right="y")]
    joined = fpstreams.rows(left).join(right, on="id", how="inner").with_engine("python")
    monkeypatch.setattr(selectors, "Mapping", RejectingMapping)

    assert joined.to_list() == [
        {"id": 1, "left": "a", "right": "x"},
        {"id": 2, "left": "b", "right": "y"},
    ]


def test_unique_right_plain_field_join_reuses_proven_nominal_mapping_classification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The m:1 strategy should share the proven nominal-Mapping field fast path."""
    from fpstreams.expressions import selectors

    class DirectMapping(Mapping[str, object]):
        def __init__(self, **values: object) -> None:
            self.values = values

        def __iter__(self) -> Iterator[str]:
            return iter(self.values)

        def __len__(self) -> int:
            return len(self.values)

        def __getitem__(self, key: str) -> object:
            return self.values[key]

    class RejectingMeta(type):
        def __instancecheck__(cls, instance: object) -> bool:
            raise AssertionError(f"repeated Mapping check for {type(instance).__name__}")

    class RejectingMapping(metaclass=RejectingMeta):
        pass

    left = [DirectMapping(id=1, left="a"), DirectMapping(id=2, left="b")]
    right = [DirectMapping(id=1, right="x"), DirectMapping(id=2, right="y")]
    joined = (
        fpstreams.rows(left).join(right, on="id", how="inner", validate="m:1").with_engine("python")
    )
    monkeypatch.setattr(selectors, "Mapping", RejectingMapping)

    assert joined.to_list() == [
        {"id": 1, "left": "a", "right": "x"},
        {"id": 2, "left": "b", "right": "y"},
    ]


def test_join_target_cache_preserves_original_and_generated_key_identity() -> None:
    """Only original right names may be shared; generated suffix keys remain per output."""
    safe_name = "".join(("right", "_payload"))
    safe = (
        fpstreams.rows([{"id": 1}, {"id": 1}])
        .join(
            [{"id": 1, safe_name: True}],
            on="id",
        )
        .to_list()
    )
    safe_keys = [next(name for name in row if name == safe_name) for row in safe]
    assert safe_keys[0] is safe_name
    assert safe_keys[1] is safe_name

    collided = (
        fpstreams.rows([{"id": 1, "value": "a"}, {"id": 1, "value": "b"}])
        .join(
            [{"id": 1, "value": "right"}],
            on="id",
        )
        .to_list()
    )
    generated = [next(name for name in row if name == "value_right") for row in collided]
    assert generated[0] is not generated[1]


@pytest.mark.parametrize("validate", ["m:1", "m:m"])
@pytest.mark.parametrize("engine", ["python", "auto"])
def test_join_target_cache_handles_repeated_changes_in_layout_width(
    validate: str, engine: str, monkeypatch
) -> None:
    """Switching between cached narrow and wide shapes retains the canonical outputs."""
    from fpstreams.tabular.join import _JoinTargetCache

    names = tuple(f"field_{index}" for index in range(31))
    widths = (2, 8, 3, 8, 32, 4, 32, 2, 9, 5, 9)
    left = [
        {"id": 1, **{name: row_index for name in names[: width - 1]}}
        for row_index, width in enumerate(widths)
    ]
    right = [{"id": 1, "right": "first"}]
    if validate == "m:m":
        right.append({"id": 1, "right": "second"})

    def execute():
        return (
            fpstreams.rows(iter(left))
            .with_engine(engine)
            .join(right, on="id", validate=validate)
            .to_list()
        )

    actual = execute()
    monkeypatch.setattr(_JoinTargetCache, "target_plan", lambda *args, **kwargs: None)
    expected = execute()
    assert actual == expected
    assert len(actual) == len(left) * len(right)
    for row, reference in zip(actual, expected, strict=True):
        assert list(row) == list(reference)
        assert all(name is other for name, other in zip(row, reference, strict=True))


@pytest.mark.parametrize("width", [2, 8, 32])
@pytest.mark.parametrize("reset_cache", [False, True])
def test_join_target_cache_keeps_dictionary_subclass_iteration(
    width: int, reset_cache: bool
) -> None:
    """A custom names iterator can reset the cache before the snapshot is inspected."""
    from fpstreams.tabular.join import _JoinTargetCache

    cache = _JoinTargetCache()
    names = {f"left_{index}": index for index in range(width)}
    right_name = "right_payload"
    expected = ((right_name, right_name),)
    events = []

    class Names(dict):
        def __iter__(self):
            events.append("iter")
            if reset_cache:
                cache._disable()
            return super().__iter__()

        def __len__(self):
            events.append("len")
            return super().__len__()

    tuple(Names(names))
    expected_events = list(events)
    events.clear()
    cache = _JoinTargetCache()
    assert cache.target_plan(names, (right_name,), shared_names=set(), suffix="_right") == expected
    assert (
        cache.target_plan(Names(names), (right_name,), shared_names=set(), suffix="_right")
        == expected
    )
    assert events == expected_events


@pytest.mark.parametrize("validate", ["m:1", "m:m"])
@pytest.mark.parametrize("engine", ["python", "auto"])
def test_join_target_cache_keeps_a_replaced_field_snapshot_constructor(
    validate: str, engine: str, monkeypatch
) -> None:
    """A source can replace the tuple constructor after a wide layout has warmed the cache."""
    import fpstreams.tabular.join as joins

    names = tuple(f"field_{index}" for index in range(7))
    calls = []

    def snapshot(value):
        calls.append(type(value))
        return tuple(value)

    def source():
        yield {"id": 1, **dict.fromkeys(names, "first")}
        monkeypatch.setattr(joins, "tuple", snapshot, raising=False)
        yield {"id": 1, **dict.fromkeys(names, "second")}

    result = (
        fpstreams.rows(source())
        .with_engine(engine)
        .join([{"id": 1, "right": True}], on="id", validate=validate)
        .to_list()
    )
    assert result == [
        {"id": 1, **dict.fromkeys(names, value), "right": True} for value in ("first", "second")
    ]
    assert calls == [dict]


def test_join_target_plan_reuses_one_generated_key_for_duplicate_matches() -> None:
    """All outputs from one left row retain the same per-row generated suffix key."""

    def select_id(row: dict[str, Any]) -> Any:
        return row["id"]

    result = (
        fpstreams.rows([{"id": 1, "value": "left"}])
        .join(
            [{"id": 1, "value": "a"}, {"id": 1, "value": "b"}],
            left_on=select_id,
            right_on=select_id,
        )
        .to_list()
    )
    generated = [next(name for name in row if name == "value_right") for row in result]

    assert result == [
        {"id": 1, "value": "left", "id_right": 1, "value_right": "a"},
        {"id": 1, "value": "left", "id_right": 1, "value_right": "b"},
    ]
    assert generated[0] is generated[1]


def test_join_target_cache_does_not_rehash_protocol_sensitive_field_names() -> None:
    """Unsafe field shapes fall back through the exact-dict canonical path."""
    from fpstreams.tabular.join import _JoinTargetCache

    hash_calls: list[str] = []

    class FieldName(str):
        def __hash__(self) -> int:
            hash_calls.append(str(self))
            return super().__hash__()

    field = FieldName("payload")
    left = [{"id": 1, field: "a"}, {"id": 2, field: "b"}]
    right = [{"id": 1, "right": "x"}, {"id": 2, "right": "y"}]
    expected = [
        {"id": 1, field: "a", "right": "x"},
        {"id": 2, field: "b", "right": "y"},
    ]
    hash_calls.clear()

    result = fpstreams.rows(left).join(right, on="id").to_list()
    assert hash_calls == []
    assert result == expected

    cache = _JoinTargetCache()
    cache.target_plan(left[0], ("id", "right"), shared_names={"id"}, suffix="_right")
    assert not cache.enabled

    collided_cache = _JoinTargetCache()
    collided_cache.target_plan(
        {"id": 1, "value": "left"},
        ("id", "value"),
        shared_names={"id"},
        suffix="_right",
    )
    assert collided_cache.enabled


@pytest.mark.parametrize("width", [8, 32])
@pytest.mark.parametrize("layout", ["repeated", "equal_names", "capacity", "unsafe"])
@pytest.mark.parametrize("validate", ["m:1", "m:m"])
@pytest.mark.parametrize("how", ["inner", "left"])
def test_wide_join_layout_cache_matches_uncached_output_and_protocol(
    monkeypatch: pytest.MonkeyPatch, width: int, layout: str, validate: str, how: str
) -> None:
    from fpstreams.tabular.join import _JoinTargetCache

    events = []

    class FieldName(str):
        def __hash__(self):
            events.append(("field_hash", str(self)))
            return super().__hash__()

        def __eq__(self, other):
            events.append(("field_equal", str(self)))
            return super().__eq__(other)

    names = tuple(f"left_field_{offset}" for offset in range(width - 2))
    count = 67 if layout == "capacity" else 4
    records = []
    for index in range(count):
        row_names = list(names)
        if layout == "equal_names":
            row_names = [name.encode().decode() for name in names]
        elif layout == "capacity":
            row_names[-1] = f"shape_{index}"
        elif layout == "unsafe" and index == 2:
            row_names[-1] = FieldName(row_names[-1])
        records.append(
            {
                "id": 2 if index == count - 1 else 1,
                "value": index,
                **dict.fromkeys(row_names, index),
            }
        )
    right_name = "".join(("right", "_payload"))
    right = [
        {"id": 1, "value": label, right_name: label}
        for label in ("a", "b")[: 2 if validate == "m:m" else 1]
    ]

    def execute():
        events.clear()

        def source():
            try:
                for row in records:
                    events.append(("pull", row["value"]))
                    yield row
            finally:
                events.append(("close",))

        def select_left(row):
            events.append(("left_key", row["value"]))
            return row["id"]

        def select_right(row):
            events.append(("right_key", row["value"]))
            return row["id"]

        result = (
            fpstreams.rows(source())
            .with_engine("python")
            .join(right, left_on=select_left, right_on=select_right, how=how, validate=validate)
            .to_list()
        )
        protocol = list(events)
        assert protocol.count(("close",)) == 1
        assert len({id(row) for row in result}) == len(result)
        generated = {}
        for row in result:
            original = records[row["value"]]
            assert all(
                actual is expected
                for actual, expected in zip(tuple(row)[:width], original, strict=True)
            )
            assert next(name for name in row if name == right_name) is right_name
            generated.setdefault(row["value"], []).append(
                next(name for name in row if name == "value_right")
            )
        for keys in generated.values():
            assert all(name is keys[0] for name in keys)
        assert len({id(keys[0]) for keys in generated.values()}) == len(generated)
        return [list(row.items()) for row in result], protocol

    actual, actual_protocol = execute()
    monkeypatch.setattr(_JoinTargetCache, "target_plan", lambda *args, **kwargs: None)
    expected, expected_protocol = execute()

    assert actual == expected
    assert actual_protocol == expected_protocol
    assert len(actual) == (count - 1) * len(right) + (how == "left")


@pytest.mark.parametrize("validate", ["m:1", "m:m"])
def test_wide_join_layout_change_reports_suffix_collision_after_first_output(validate: str) -> None:
    from fpstreams.errors import DuplicateKeyError

    fields = {f"field_{index}": index for index in range(8)}
    records = [
        {"id": 1, "value": "first", **fields},
        {"id": 1, "value": "second", **fields, "value_right": "collision"},
    ]
    closed = []

    def source():
        try:
            yield from records
        finally:
            closed.append(True)

    iterator = iter(
        fpstreams.rows(source())
        .with_engine("python")
        .join([{"id": 1, "value": "right"}], on="id", validate=validate)
    )
    assert next(iterator)["value_right"] == "right"
    with pytest.raises(DuplicateKeyError, match="value_right"):
        next(iterator)
    assert closed == [True]


def test_hash_join_bucket_promotion_does_not_repeat_key_protocols() -> None:
    """Promoting a repeated right key must not perform another index assignment."""
    from fpstreams.tabular.join import _join_record_index

    events: list[str] = []

    class Key:
        def __init__(self, label: str) -> None:
            self.label = label

        def __hash__(self) -> int:
            events.append(f"hash:{self.label}")
            return 0

        def __eq__(self, other: object) -> bool:
            assert isinstance(other, Key)
            events.append(f"eq:{self.label}:{other.label}")
            return True

    first, second, third = Key("first"), Key("second"), Key("third")
    rows = [
        {"key": first, "value": "a"},
        {"key": second, "value": "b"},
        {"key": third, "value": "c"},
    ]
    events.clear()

    _columns, index, slots = _join_record_index(rows, lambda row: row["key"], validate="m:m")

    assert events == [
        "hash:first",
        "hash:first",
        "hash:second",
        "eq:first:second",
        "hash:third",
        "eq:first:third",
    ]
    bucket = slots[next(iter(index.values()))]
    assert isinstance(bucket, list)
    assert [row["value"] for row in bucket] == [
        "a",
        "b",
        "c",
    ]

    mixed_events: list[str] = []

    class EqualToOne:
        def __hash__(self) -> int:
            mixed_events.append("hash")
            return hash(1)

        def __eq__(self, other: object) -> bool:
            mixed_events.append(f"eq:{other!r}")
            return other == 1

    mixed_key = EqualToOne()
    _columns, mixed_index, mixed_slots = _join_record_index(
        [{"key": 1, "value": "first"}, {"key": mixed_key, "value": "second"}],
        lambda row: row["key"],
        validate="m:m",
    )
    assert mixed_events == ["hash", "eq:1"]
    mixed_bucket = mixed_slots[next(iter(mixed_index.values()))]
    assert isinstance(mixed_bucket, list)
    assert [row["value"] for row in mixed_bucket] == [
        "first",
        "second",
    ]

    metaclass_events: list[str] = []

    class KeyType(type):
        def __eq__(cls, other: object) -> bool:
            metaclass_events.append(f"eq:{other!r}")
            return False

        __hash__ = type.__hash__

    class MetaclassKey(metaclass=KeyType):
        pass

    _join_record_index(
        [{"key": MetaclassKey(), "value": 1}],
        lambda row: row["key"],
        validate="m:m",
    )
    assert metaclass_events == []


def test_hash_join_slots_promote_dense_keys_without_reindexing() -> None:
    """Dense keys should promote their stable slots without rewriting hash entries."""
    from fpstreams.tabular.join import _join_record_index

    rows = [{"key": index % 8, "value": index} for index in range(80)]

    _columns, index, slots = _join_record_index(
        rows,
        lambda row: row["key"],
        validate="m:m",
    )

    assert all(isinstance(slots[position], list) for position in index.values())
    key_three = slots[index[3]]
    assert isinstance(key_three, list)
    assert [row["value"] for row in key_three] == list(range(3, 80, 8))


@pytest.mark.parametrize(
    ("how", "right", "expected"),
    [
        (
            "inner",
            [{"id": 1, "right": "matched"}],
            [{"id": 1, "left": "before", "id_right": 1, "right": "matched"}],
        ),
        (
            "left",
            [{"id": 2, "right": "unmatched"}],
            [{"id": 1, "left": "before", "id_right": None, "right": None}],
        ),
        ("semi", [{"id": 1}], [{"id": 1, "left": "before"}]),
        ("anti", [{"id": 2}], [{"id": 1, "left": "before"}]),
    ],
)
def test_callable_join_key_mutation_does_not_change_left_snapshot(
    how: str,
    right: list[dict[str, Any]],
    expected: list[dict[str, Any]],
) -> None:
    """Join output snapshots a left dictionary before invoking an opaque key callback."""
    source = {"id": 1, "left": "before"}

    def mutating_key(row: dict[str, Any]) -> int:
        key = row["id"]
        row["left"] = "after"
        row["added"] = True
        return key

    result = (
        fpstreams.rows([source])
        .join(
            right,
            left_on=mutating_key,
            right_on="id",
            how=how,
        )
        .to_list()
    )

    assert result == expected
    assert source == {"id": 1, "left": "after", "added": True}


def test_unique_right_join_snapshots_left_before_callable_key_mutation() -> None:
    """The unique-right fast path keeps the same pre-callback snapshot contract."""
    source = {"id": 1, "left": "before"}

    def mutating_key(row: dict[str, Any]) -> int:
        key = row["id"]
        row["left"] = "after"
        row["added"] = True
        return key

    result = (
        fpstreams.rows([source])
        .join(
            [{"id": 1, "right": "matched"}],
            left_on=mutating_key,
            right_on="id",
            validate="m:1",
        )
        .to_list()
    )

    assert result == [{"id": 1, "left": "before", "id_right": 1, "right": "matched"}]
    assert source == {"id": 1, "left": "after", "added": True}


def test_unique_right_inner_skips_target_collisions_for_an_unmatched_left_row() -> None:
    """An inner join must establish that a row matches before validating its output layout."""
    left = [{"id": 2, "value": "left", "value_right": "occupied"}]
    right = [{"id": 1, "value": "right"}]

    def select_id(row: dict[str, Any]) -> Any:
        return row["id"]

    assert (
        fpstreams.rows(left)
        .join(
            right,
            left_on=select_id,
            right_on=select_id,
            validate="m:1",
        )
        .to_list()
        == []
    )


def test_unique_right_probe_reports_unhashable_key_before_output_collision() -> None:
    """Probe errors retain the hash-join ordering even when the row layout also collides."""
    left = [{"id": [], "value": "left", "value_right": "occupied"}]
    right = [{"id": 1, "value": "right"}]

    def select_id(row: dict[str, Any]) -> Any:
        return row["id"]

    with pytest.raises(TypeError, match="join keys must be hashable"):
        fpstreams.rows(left).join(
            right,
            left_on=select_id,
            right_on=select_id,
            validate="m:1",
        ).to_list()


def test_unique_right_join_matches_canonical_key_hash_trace() -> None:
    """The m:1 specialization must not add speculative user hash calls."""
    events: list[str] = []

    class Key:
        def __init__(self, label: str) -> None:
            self.label = label

        def __hash__(self) -> int:
            events.append(f"hash:{self.label}")
            return 0

        def __eq__(self, other: object) -> bool:
            assert isinstance(other, Key)
            events.append(f"eq:{self.label}:{other.label}")
            return True

    left_key = Key("left")
    right_key = Key("right")
    result = (
        fpstreams.rows([{"id": left_key, "left": True}])
        .join(
            [{"id": right_key, "right": True}],
            on="id",
            validate="m:1",
        )
        .with_engine("python")
        .to_list()
    )

    assert result == [{"id": left_key, "left": True, "right": True}]
    assert events == [
        "hash:right",
        "hash:right",
        "hash:left",
        "eq:right:left",
    ]


def test_unique_right_join_discovers_columns_before_hashing_the_key() -> None:
    """Right-column discovery retains the canonical pre-index protocol order."""
    events: list[str] = []

    class Field(str):
        def __hash__(self) -> int:
            events.append("field")
            return super().__hash__()

    class Key:
        def __hash__(self) -> int:
            events.append("key")
            return 1

    payload = Field("payload")
    right = {"id": Key(), payload: True}
    events.clear()

    assert (
        fpstreams.rows([{"id": 2}])
        .join([right], on="id", validate="m:1")
        .with_engine("python")
        .to_list()
        == []
    )
    assert events.index("field") < events.index("key")


@pytest.mark.parametrize(
    ("how", "validate"),
    [("inner", "m:m"), ("semi", "m:m"), ("inner", "m:1")],
)
def test_exact_field_join_snapshots_before_colliding_key_equality(how: str, validate: str) -> None:
    """A string lookup may run a colliding key's ``__eq__`` and mutate its source."""

    class CollidingKey:
        def __init__(self, source: dict[Any, Any]) -> None:
            self.source = source

        def __hash__(self) -> int:
            return hash("id")

        def __eq__(self, other: object) -> bool:
            self.source["value"] = "after-equality"
            return False

    source: dict[Any, Any] = {"value": "placeholder"}
    source[CollidingKey(source)] = "collision"
    source["id"] = 1
    # Inserting the real field may already compare it with the colliding key.
    source["value"] = "before"

    result = (
        fpstreams.rows([source])
        .join(
            [{"id": 1, "right": True}],
            on="id",
            how=how,
            validate=validate,
        )
        .to_list()
    )

    assert result[0]["value"] == "before"
    assert source["value"] == "after-equality"


def test_join_validation_is_checked_before_sources_are_opened() -> None:
    opened = False

    def records():
        nonlocal opened
        opened = True
        yield {"id": 1}

    with pytest.raises(ValueError, match="validate must be one of"):
        fpstreams.rows(records()).join(records(), on="id", validate="one-to-one")

    assert not opened


def test_join_cardinality_contracts_accept_matching_inputs() -> None:
    left_unique = [{"id": 1, "left": "a"}]
    left_duplicate = [{"id": 1, "left": "a"}, {"id": 1, "left": "b"}]
    right_unique = [{"id": 1, "right": "x"}]
    right_duplicate = [{"id": 1, "right": "x"}, {"id": 1, "right": "y"}]

    assert (
        len(fpstreams.rows(left_unique).join(right_duplicate, on="id", validate="1:m").to_list())
        == 2
    )
    assert (
        len(fpstreams.rows(left_duplicate).join(right_unique, on="id", validate="m:1").to_list())
        == 2
    )
    assert (
        len(fpstreams.rows(left_unique).join(right_unique, on="id", validate="1:1").to_list()) == 1
    )
    assert (
        len(fpstreams.rows(left_duplicate).join(right_duplicate, on="id", validate="m:m").to_list())
        == 4
    )


@pytest.mark.parametrize(
    ("validate", "duplicate_side"),
    [("1:m", "left"), ("1:1", "left"), ("m:1", "right"), ("1:1", "right")],
)
def test_join_cardinality_contracts_reject_duplicate_keys(
    validate: str, duplicate_side: str
) -> None:
    unique = [{"id": 1}]
    duplicate = [{"id": 1}, {"id": 1}]
    left = duplicate if duplicate_side == "left" else unique
    right = duplicate if duplicate_side == "right" else unique

    with pytest.raises(
        ValueError,
        match=rf"validate={validate!r} requires unique {duplicate_side} keys.*1",
    ):
        fpstreams.rows(left).join(right, on="id", validate=validate).to_list()


@pytest.mark.parametrize(
    ("how", "validate", "duplicate_side"),
    [("semi", "1:m", "left"), ("anti", "m:1", "right")],
)
def test_semi_and_anti_joins_honor_cardinality_contracts(
    how: str, validate: str, duplicate_side: str
) -> None:
    unique = [{"id": 1}]
    duplicate = [{"id": 1}, {"id": 1}]
    left = duplicate if duplicate_side == "left" else unique
    right = duplicate if duplicate_side == "right" else unique

    with pytest.raises(ValueError, match=rf"unique {duplicate_side} keys"):
        fpstreams.rows(left).join(
            right,
            on="id",
            how=how,
            validate=validate,
        ).to_list()


def test_rows_supports_relational_join_modes_and_different_keys() -> None:
    left = [{"left_id": 1, "value": "a"}, {"left_id": 3, "value": "c"}]
    right = [{"right_id": 1, "value": "A"}, {"right_id": 2, "value": "B"}]

    assert fpstreams.rows(left).join(
        right, left_on="left_id", right_on="right_id", how="full"
    ).to_list() == [
        {"left_id": 1, "value": "a", "right_id": 1, "value_right": "A"},
        {"left_id": 3, "value": "c", "right_id": None, "value_right": None},
        {"left_id": None, "value": None, "right_id": 2, "value_right": "B"},
    ]
    assert fpstreams.rows(left).join(
        right, left_on="left_id", right_on="right_id", how="semi"
    ).to_list() == [left[0]]
    assert fpstreams.rows(left).join(
        right, left_on="left_id", right_on="right_id", how="anti"
    ).to_list() == [left[1]]


def test_rows_join_accepts_readable_composite_keys() -> None:
    events = [
        {"tenant": "a", "user_id": 1, "event": "open"},
        {"tenant": "b", "user_id": 1, "event": "click"},
    ]
    users = [
        {"tenant": "a", "id": 1, "name": "Ada"},
        {"tenant": "b", "id": 2, "name": "Lin"},
    ]

    assert fpstreams.rows(events).join(
        users,
        left_on=("tenant", "user_id"),
        right_on=("tenant", "id"),
        how="left",
    ).select("event", "name").to_list() == [
        {"event": "open", "name": "Ada"},
        {"event": "click", "name": None},
    ]


@pytest.mark.parametrize("engine", ["python", "auto"])
@pytest.mark.parametrize("validate", ["m:m", "1:1", "m:1", "1:m"])
def test_join_preserves_key_equality_failure(engine: str, validate: str) -> None:
    failure = TypeError("join equality failed")
    closed: list[str] = []

    class Key:
        def __hash__(self) -> int:
            return 0

        def __eq__(self, other: object) -> bool:
            raise failure

    def rows(side: str):
        try:
            yield {"key": Key(), "value": side}
            yield {"key": Key(), "value": side}
        finally:
            closed.append(side)

    query = (
        fpstreams.rows(rows("left"))
        .with_engine(engine)
        .join(rows("right"), left_on="key", right_on="key", validate=validate)
    )
    with pytest.raises(TypeError) as captured:
        query.to_list()
    assert captured.value is failure
    assert "right" in closed


def test_in_memory_join_keeps_source_failure_primary_and_closes_that_source() -> None:
    primary = ValueError("left source failed")

    class Source(Iterator[dict[str, int]]):
        def __init__(self) -> None:
            self.pulls = 0
            self.close_calls = 0

        def __next__(self) -> dict[str, int]:
            self.pulls += 1
            if self.pulls == 1:
                return {"id": 1}
            raise primary

        def close(self) -> None:
            self.close_calls += 1
            raise OSError("left source close failed")

    source = Source()
    with pytest.raises(ValueError) as captured:
        fpstreams.rows(source).join([{"id": 1}], on="id").to_list()

    assert source.pulls == 2
    assert captured.value is primary
    assert captured.value.__notes__ == ["cleanup failed with OSError: left source close failed"]
    assert source.close_calls == 1


@pytest.mark.parametrize("failing_side", ["left", "right"])
def test_unique_right_join_keeps_key_failure_primary_when_source_close_fails(
    failing_side: str,
) -> None:
    primary = ValueError(f"{failing_side} key failed")

    class Source(Iterator[dict[str, int]]):
        def __init__(self) -> None:
            self.emitted = False
            self.close_calls = 0

        def __next__(self) -> dict[str, int]:
            if self.emitted:
                raise StopIteration
            self.emitted = True
            return {"id": 1}

        def close(self) -> None:
            self.close_calls += 1
            raise OSError(f"{failing_side} source close failed")

    def fail_key(_row: object) -> int:
        raise primary

    def direct_key(row: dict[str, int]) -> int:
        return row["id"]

    source = Source()
    joined = (
        fpstreams.rows(source).join(
            [{"id": 1}],
            left_on=fail_key,
            right_on=direct_key,
            validate="m:1",
        )
        if failing_side == "left"
        else fpstreams.rows([{"id": 1}]).join(
            source,
            left_on=direct_key,
            right_on=fail_key,
            validate="m:1",
        )
    ).with_engine("python")

    with pytest.raises(ValueError) as captured:
        joined.to_list()

    assert captured.value is primary
    assert captured.value.__notes__ == [
        f"cleanup failed with OSError: {failing_side} source close failed"
    ]
    assert source.close_calls == 1


@pytest.mark.parametrize("how", ["inner", "left", "semi", "anti"])
def test_sorted_join_many_to_one_contract(how) -> None:
    from fpstreams import flow
    from fpstreams.execution.sorted_streams import join_sorted_rows
    from fpstreams.tabular.join import _compile_join_selector, _shared_join_names

    left = [
        {"id": 0, "value": "l0"},
        {"id": 1, "value": "l1"},
        {"id": 1, "value": "l2"},
        {"id": 3, "value": "l3"},
    ]
    right = [{"id": 1, "value": "r1", "tag": "a"}, {"id": 3, "value": "r3"}]
    key = _compile_join_selector("id")
    options = {
        "left_key": key,
        "right_key": key,
        "shared_names": _shared_join_names("id", "id"),
        "how": how,
        "suffix": "_rhs",
        "validate": "m:1",
    }
    expected = (
        flow(left).rows().join(right, on="id", how=how, suffix="_rhs", validate="m:1").to_list()
    )
    actual = list(join_sorted_rows(left, right, **options))
    assert actual == expected
    assert all(row is not original for row in actual for original in left)
    assert len({id(row) for row in actual}) == len(actual)
    assert (
        list(join_sorted_rows(left, [], **options))
        == flow(left).rows().join([], on="id", how=how, suffix="_rhs", validate="m:1").to_list()
    )
    assert list(join_sorted_rows([], right, **options)) == []
    duplicate = join_sorted_rows([{"id": 1}], [{"id": 1}, {"id": 1}], **options)
    with pytest.raises(ValueError, match="right"):
        next(duplicate)
    if how in {"inner", "left"}:
        with pytest.raises(ValueError, match="column"):
            list(join_sorted_rows(left, [{"id": 1}, {"id": 3, "new": 1}], **options))
    else:
        scalar_right = [1, 3]
        options["right_key"] = lambda value: value
        options["shared_names"] = set()
        from fpstreams import SelectionError

        with pytest.raises(SelectionError):
            list(join_sorted_rows(left, scalar_right, **options))
        with pytest.raises(SelectionError):
            flow(left).rows().join(
                scalar_right,
                left_on="id",
                right_on=lambda value: value,
                how=how,
                validate="m:1",
            ).to_list()


def test_sorted_join_snapshots_before_key_callbacks() -> None:
    from fpstreams.execution.sorted_streams import join_sorted_rows

    left = [{"id": 1, "value": "left original"}]
    right = [{"id": 1, "value": "right original"}]
    calls = []

    def key(row):
        calls.append(row["value"])
        row["value"] = "changed"
        return row["id"]

    result = list(
        join_sorted_rows(
            left,
            right,
            left_key=key,
            right_key=key,
            shared_names={"id"},
            how="inner",
            suffix="_right",
            validate="m:1",
        )
    )
    assert result == [{"id": 1, "value": "left original", "value_right": "right original"}]
    assert sorted(calls) == ["left original", "right original"]


def test_sorted_join_paired_open_failure_and_alias() -> None:
    from fpstreams.execution.sorted_streams import join_sorted_rows
    from fpstreams.planning.source import Source

    events = []

    class Records:
        def __iter__(self):
            return self

        def __next__(self):
            events.append("pull")
            return {"id": 1}

        def close(self):
            events.append("close")

    iterator = Records()

    def left():
        events.append("left")
        return iterator

    def right():
        events.append("right")
        return iterator

    options = dict(
        left_key=lambda row: row["id"], right_key=lambda row: row["id"], shared_names={"id"}
    )
    with pytest.raises(ValueError, match="same iterator"):
        next(join_sorted_rows(Source.defer(left), Source.defer(right), **options))
    assert events == ["left", "right", "close"]
    events.clear()
    error = RuntimeError("right open")

    def broken():
        events.append("right")
        raise error

    with pytest.raises(RuntimeError) as caught:
        next(join_sorted_rows(Source.defer(left), Source.defer(broken), **options))
    assert caught.value is error
    assert events == ["left", "right", "close"]


@pytest.mark.parametrize("engine", ["python", "auto"])
def test_join_sorted_public_contract(engine) -> None:
    left = [{"id": 1, "l": 1}, {"id": 1, "l": 2}, {"id": 3, "l": 3}]
    right = [{"id": 1, "r": 4}, {"id": 2, "r": 5}]
    for how in ("inner", "left", "semi", "anti"):
        result = (
            flow(left)
            .with_engine(engine)
            .rows()
            .join_sorted(right, on="id", how=how, validate="m:1")
        )
        assert (
            result.to_list()
            == flow(left).rows().join(right, on="id", how=how, validate="m:1").to_list()
        )
        assert "sorted_join" in str(result.explain())
    assert flow(left).rows().join_sorted(right, on="id", validate="m:1").take(0).to_list() == []
    with pytest.raises(ValueError, match=r"sorted|order"):
        flow([{"id": 2}, {"id": 1}]).rows().join_sorted(right, on="id", validate="m:1").to_list()
    for invalid in (
        {"how": "full"},
        {"validate": "unknown"},
        {"max_output_rows": 0},
        {"max_matches_per_left": 1.5},
        {"max_right_group_rows": None},
    ):
        with pytest.raises((ValueError, TypeError)):
            flow(left).rows().join_sorted(right, on="id", **invalid)
    from fpstreams import NativeUnsupportedError

    with pytest.raises(NativeUnsupportedError):
        flow(left).with_engine("native").rows().join_sorted(
            right, on="id", validate="m:1"
        ).to_list()


@pytest.mark.parametrize("how", ["inner", "left", "semi", "anti"])
def test_sorted_join_many_to_many_limits(how) -> None:
    from fpstreams import BufferLimitError

    left = [{"id": 0, "l": 0}, {"id": 1, "l": 1}, {"id": 1, "l": 2}]
    right = [{"id": 1, "r": i} for i in range(3)]
    query = flow(left).rows().join_sorted(right, on="id", how=how)
    actual = query.to_list()
    assert actual == flow(left).rows().join(right, on="id", how=how).to_list()
    assert len({id(row) for row in actual}) == len(actual)
    for validation in ("m:1", "1:1", "1:m"):
        with pytest.raises(ValueError, match=r"left|right"):
            flow(left).rows().join_sorted(right, on="id", how=how, validate=validation).to_list()
    with pytest.raises(BufferLimitError, match="right"):
        flow(left).rows().join_sorted(right, on="id", how=how, max_right_group_rows=2).to_list()
    if how in {"inner", "left"}:
        with pytest.raises(BufferLimitError, match="matches"):
            flow(left).rows().join_sorted(right, on="id", how=how, max_matches_per_left=2).to_list()
    output_limit = 1
    limited = iter(
        flow(left).rows().join_sorted(right, on="id", how=how, max_output_rows=output_limit)
    )
    first = next(limited)
    assert first == actual[0]
    if len(actual) > output_limit:
        with pytest.raises(BufferLimitError, match="output"):
            next(limited)
    else:
        assert list(limited) == []
