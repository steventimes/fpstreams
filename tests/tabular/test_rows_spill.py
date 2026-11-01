"""Rows partitioned join and grouping limits, ordering, and cleanup contracts."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

import fpstreams


@dataclass(frozen=True)
class _CollidingKey:
    value: int

    def __hash__(self) -> int:
        return 1


@pytest.mark.parametrize("how", ["inner", "left", "right", "full", "semi", "anti"])
def test_partitioned_join_matches_stable_in_memory_semantics(how: str, tmp_path) -> None:
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

    expected = fpstreams.rows(left).join(right, on="id", how=how).to_list()
    actual = (
        fpstreams.rows(left)
        .join(
            right,
            on="id",
            how=how,
            partitions=3,
            tempdir=tmp_path,
        )
        .to_list()
    )

    assert actual == expected
    assert list(tmp_path.iterdir()) == []


def test_partitioned_join_preserves_composite_keys_suffixes_and_sparse_columns() -> None:
    left = [
        {"tenant": "a", "user": 1, "value": "left-a"},
        {"tenant": "b", "user": 1, "value": "left-b", "extra": 9},
    ]
    right = [
        {"tenant": "a", "id": 1, "value": "right-a"},
        {"tenant": "b", "id": 2, "value": "right-b"},
    ]

    regular = (
        fpstreams.rows(left)
        .join(
            right,
            left_on=("tenant", "user"),
            right_on=("tenant", "id"),
            how="full",
            suffix="_lookup",
        )
        .to_list()
    )
    spilled = (
        fpstreams.rows(left)
        .join(
            right,
            left_on=("tenant", "user"),
            right_on=("tenant", "id"),
            how="full",
            suffix="_lookup",
            partitions=4,
        )
        .to_list()
    )

    assert spilled == regular


@pytest.mark.parametrize(
    ("validate", "duplicate_side"),
    [("1:m", "left"), ("m:1", "right")],
)
def test_partitioned_join_validates_cardinality_and_cleans_up(
    validate: str, duplicate_side: str, tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fpstreams.tabular import spill

    unique = [{"id": 1}]
    duplicate = [{"id": 1}, {"id": 1}]
    left = duplicate if duplicate_side == "left" else unique
    right = duplicate if duplicate_side == "right" else unique
    retained_readers: list[Iterator[Any]] = []
    closed_paths: list[Path] = []
    original_read = spill._read

    def tracked_read(path: Path, *, store=None) -> Iterator[Any]:
        """Retain each reader so only explicit executor ownership can close it."""
        upstream = original_read(path, store=store)

        def values() -> Iterator[Any]:
            try:
                yield from upstream
            finally:
                upstream.close()
                closed_paths.append(path)

        reader = values()
        retained_readers.append(reader)
        return reader

    monkeypatch.setattr(spill, "_read", tracked_read)

    with pytest.raises(ValueError, match=rf"unique {duplicate_side} keys"):
        fpstreams.rows(left).join(
            right,
            on="id",
            validate=validate,
            partitions=3,
            tempdir=tmp_path,
        ).to_list()

    assert retained_readers
    assert len(closed_paths) == len(retained_readers)
    assert list(tmp_path.iterdir()) == []


def test_spilled_group_by_preserves_first_group_and_row_order(tmp_path) -> None:
    records = [
        {"team": "b", "band": 1, "score": 4},
        {"team": "a", "band": 1, "score": 3},
        {"team": "b", "band": 1, "score": 2},
        {"team": "a", "band": 2, "score": 8},
        {"team": "a", "band": 1, "score": 1},
    ]

    expected = (
        fpstreams.rows(records)
        .group_by("team", "band")
        .aggregate(
            count=fpstreams.agg.count(),
            total=fpstreams.agg.sum("score"),
            scores=fpstreams.agg.collect("score", into=tuple),
        )
        .to_list()
    )
    actual = (
        fpstreams.rows(records)
        .group_by("team", "band")
        .spill(3, tempdir=tmp_path)
        .aggregate(
            count=fpstreams.agg.count(),
            total=fpstreams.agg.sum("score"),
            scores=fpstreams.agg.collect("score", into=tuple),
        )
        .to_list()
    )

    assert actual == expected
    assert list(tmp_path.iterdir()) == []


def test_spill_closes_sources_and_temp_files_after_short_circuit(tmp_path) -> None:
    left_closed = right_closed = False

    def left() -> Iterator[dict[str, int]]:
        nonlocal left_closed
        try:
            for value in range(20):
                yield {"id": value}
        finally:
            left_closed = True

    def right() -> Iterator[dict[str, int]]:
        nonlocal right_closed
        try:
            for value in range(20):
                yield {"id": value}
        finally:
            right_closed = True

    iterator = iter(fpstreams.rows(left()).join(right(), on="id", partitions=3, tempdir=tmp_path))
    assert next(iterator) == {"id": 0}
    assert left_closed and right_closed
    assert list(tmp_path.iterdir())
    iterator.close()
    assert list(tmp_path.iterdir()) == []


def test_spill_errors_are_clear_and_cleanup_is_transactional(tmp_path) -> None:
    with pytest.raises(TypeError, match="hashable"):
        (
            fpstreams.rows([{"key": [1], "value": 2}])
            .group_by("key")
            .spill(2, tempdir=tmp_path)
            .aggregate(total=fpstreams.agg.sum("value"))
            .to_list()
        )
    assert list(tmp_path.iterdir()) == []

    assert fpstreams.rows([{"id": 1}]).join(
        [{"id": 1, "payload": lambda: None}],
        on="id",
        how="semi",
        partitions=2,
        tempdir=tmp_path,
    ).to_list() == [{"id": 1}]
    assert list(tmp_path.iterdir()) == []

    with pytest.raises(TypeError, match="picklable"):
        fpstreams.rows([{"id": 1, "value": lambda: None}]).join(
            [{"id": 1}],
            on="id",
            partitions=2,
            tempdir=tmp_path,
        ).to_list()
    assert list(tmp_path.iterdir()) == []


def test_spill_partition_configuration_is_validated() -> None:
    grouped = fpstreams.rows([{"id": 1}]).group_by("id")
    with pytest.raises(ValueError, match="between"):
        grouped.spill(1)
    with pytest.raises(TypeError, match="integer"):
        grouped.spill(2.5)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="tempdir requires"):
        fpstreams.rows([{"id": 1}]).join([{"id": 1}], on="id", tempdir="unused")


def test_spill_limits_are_validated_before_sources_are_consumed() -> None:
    with pytest.raises(ValueError, match="max_partition_rows"):
        fpstreams.SpillLimits(max_partition_rows=0)
    with pytest.raises(TypeError, match="max_output_rows"):
        fpstreams.SpillLimits(max_output_rows=2.5)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="limits requires partitions"):
        fpstreams.rows([{"id": 1}]).join(
            [{"id": 1}],
            on="id",
            limits=fpstreams.SpillLimits(),
        )


def test_spilled_join_rejects_an_oversized_skewed_partition_and_cleans_up(tmp_path) -> None:
    limits = fpstreams.SpillLimits(
        max_partition_rows=2,
        max_partition_bytes=64 * 1024,
        max_matches_per_key=10,
        max_output_rows=10,
        max_repartition_depth=1,
    )

    with pytest.raises(fpstreams.BufferLimitError, match="max_partition_rows"):
        fpstreams.rows([{"id": 1}]).join(
            [{"id": 1, "right": value} for value in range(3)],
            on="id",
            partitions=2,
            tempdir=tmp_path,
            limits=limits,
        ).to_list()

    assert list(tmp_path.iterdir()) == []


def test_spilled_group_rejects_distinct_colliding_keys_without_unbounded_load(tmp_path) -> None:
    limits = fpstreams.SpillLimits(
        max_partition_rows=2,
        max_partition_bytes=64 * 1024,
        max_matches_per_key=10,
        max_output_rows=10,
        max_repartition_depth=1,
    )
    records = [{"key": _CollidingKey(value)} for value in range(3)]

    with pytest.raises(fpstreams.BufferLimitError, match="max_partition_rows"):
        (
            fpstreams.rows(records)
            .group_by("key")
            .spill(2, tempdir=tmp_path, limits=limits)
            .aggregate(count=fpstreams.agg.count())
            .to_list()
        )

    assert list(tmp_path.iterdir()) == []


def test_spill_write_failure_closes_source_and_removes_temporary_files(
    tmp_path, monkeypatch
) -> None:
    from fpstreams.tabular import spill_io

    closed = False

    def records() -> Iterator[dict[str, int]]:
        nonlocal closed
        try:
            yield {"key": 1}
        finally:
            closed = True

    def fail_write(*_args, **_kwargs) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(spill_io.SpillWriter, "write_encoded", fail_write)
    with pytest.raises(OSError, match="disk full"):
        (
            fpstreams.rows(records())
            .group_by("key")
            .spill(2, tempdir=tmp_path)
            .aggregate(count=fpstreams.agg.count())
            .to_list()
        )

    assert closed
    assert list(tmp_path.iterdir()) == []


def test_spilled_group_rejects_an_oversized_serialized_partition(tmp_path) -> None:
    limits = fpstreams.SpillLimits(
        max_partition_rows=10,
        max_partition_bytes=512,
        max_matches_per_key=10,
        max_output_rows=10,
        max_repartition_depth=0,
    )

    with pytest.raises(fpstreams.BufferLimitError, match="max_partition_bytes"):
        (
            fpstreams.rows([{"key": 1, "payload": "x" * 5_000}])
            .group_by("key")
            .spill(2, tempdir=tmp_path, limits=limits)
            .aggregate(count=fpstreams.agg.count())
            .to_list()
        )

    assert list(tmp_path.iterdir()) == []


def test_spilled_join_guards_matches_and_total_output(tmp_path) -> None:
    matches_limited = fpstreams.SpillLimits(
        max_partition_rows=10,
        max_partition_bytes=64 * 1024,
        max_matches_per_key=2,
        max_output_rows=10,
        max_repartition_depth=0,
    )
    with pytest.raises(fpstreams.BufferLimitError, match="max_matches_per_key"):
        fpstreams.rows([{"id": 1, "left": "a"}]).join(
            [{"id": 1, "right": value} for value in range(3)],
            on="id",
            partitions=2,
            tempdir=tmp_path,
            limits=matches_limited,
        ).to_list()
    assert list(tmp_path.iterdir()) == []

    output_limited = fpstreams.SpillLimits(
        max_partition_rows=10,
        max_partition_bytes=64 * 1024,
        max_matches_per_key=10,
        max_output_rows=3,
        max_repartition_depth=0,
    )
    left = [{"id": 1, "left": value} for value in range(2)]
    right = [{"id": 1, "right": value} for value in range(2)]
    with pytest.raises(fpstreams.BufferLimitError, match="max_output_rows"):
        fpstreams.rows(left).join(
            right,
            on="id",
            partitions=2,
            tempdir=tmp_path,
            limits=output_limited,
        ).to_list()
    assert list(tmp_path.iterdir()) == []

    exact_limit = fpstreams.SpillLimits(
        max_partition_rows=10,
        max_partition_bytes=64 * 1024,
        max_matches_per_key=10,
        max_output_rows=4,
        max_repartition_depth=0,
    )
    assert (
        len(
            fpstreams.rows(left)
            .join(
                right,
                on="id",
                partitions=2,
                tempdir=tmp_path,
                limits=exact_limit,
            )
            .to_list()
        )
        == 4
    )
    assert list(tmp_path.iterdir()) == []


class _RestoredHashFailureKey:
    def __init__(self, restored=False):
        self.restored = restored

    def __hash__(self):
        if self.restored:
            self.restored = False
            raise KeyError("restored key hash failed")
        return 0

    def __eq__(self, other):
        return isinstance(other, _RestoredHashFailureKey)

    def __reduce__(self):
        return _RestoredHashFailureKey, (True,)


def test_spilled_group_lookup_propagates_restored_key_error(tmp_path):
    with pytest.raises(KeyError, match="restored key hash failed"):
        (
            fpstreams.rows([{"key": _RestoredHashFailureKey(), "value": 1}])
            .group_by("key")
            .spill(2, tempdir=tmp_path)
            .aggregate(total=fpstreams.agg.sum("value"))
            .to_list()
        )
    assert list(tmp_path.iterdir()) == []
