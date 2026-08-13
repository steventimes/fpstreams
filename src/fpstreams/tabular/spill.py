"""Partitioned external-memory joins and grouped aggregations."""

from __future__ import annotations

import heapq
import operator
import os
import pickle
import tempfile
from collections import OrderedDict
from collections.abc import Callable, Iterable, Iterator, Mapping
from pathlib import Path
from typing import Any, BinaryIO

from ..collecting.aggregation import (
    AggregationItems,
    finish_aggregations,
    initialize_aggregations,
    step_aggregations,
)

_MAX_OPEN_WRITERS = 32
_MAX_PARTITIONS = 256


def validate_partitions(partitions: int) -> int:
    try:
        count = operator.index(partitions)
    except TypeError:
        raise TypeError("partitions must be an integer") from None
    if count < 2 or count > _MAX_PARTITIONS:
        raise ValueError(f"partitions must be between 2 and {_MAX_PARTITIONS}")
    return count


def _dump(handle: BinaryIO, value: Any, *, operation: str) -> None:
    try:
        pickle.dump(value, handle, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception as error:
        raise TypeError(f"{operation} spill data must be picklable") from error


def _partition(key: Any, count: int, *, operation: str) -> int:
    try:
        return hash(key) % count
    except TypeError:
        raise TypeError(f"{operation} keys must be hashable") from None


class _PartitionWriters:
    __slots__ = ("_handles", "_operation", "paths")

    def __init__(self, directory: Path, prefix: str, count: int, *, operation: str) -> None:
        self.paths = tuple(directory / f"{prefix}-{position}.bin" for position in range(count))
        self._operation = operation
        self._handles: OrderedDict[int, BinaryIO] = OrderedDict()

    def dump(self, position: int, value: Any) -> None:
        handle = self._handles.pop(position, None)
        if handle is None:
            if len(self._handles) >= _MAX_OPEN_WRITERS:
                _old_position, old_handle = self._handles.popitem(last=False)
                old_handle.close()
            handle = self.paths[position].open("ab")
        self._handles[position] = handle
        _dump(handle, value, operation=self._operation)

    def close(self) -> None:
        while self._handles:
            _position, handle = self._handles.popitem(last=False)
            handle.close()


class _LazyWriter:
    __slots__ = ("_handle", "_operation", "_path")

    def __init__(self, path: Path, *, operation: str) -> None:
        self._path = path
        self._operation = operation
        self._handle: BinaryIO | None = None

    def dump(self, value: Any) -> None:
        if self._handle is None:
            self._handle = self._path.open("wb")
        _dump(self._handle, value, operation=self._operation)

    def close(self) -> None:
        if self._handle is not None:
            self._handle.close()
            self._handle = None


def _read(path: Path) -> Iterator[Any]:
    if not path.exists():
        return
    with path.open("rb") as handle:
        while True:
            try:
                yield pickle.load(handle)
            except EOFError:
                return


def _merge_ordered(paths: Iterable[Path]) -> Iterator[Any]:
    readers: list[Iterator[Any]] = []
    heap: list[tuple[Any, int, Any, Iterator[Any]]] = []
    try:
        for serial, path in enumerate(paths):
            reader = _read(path)
            readers.append(reader)
            try:
                order, value = next(reader)
            except StopIteration:
                continue
            heapq.heappush(heap, (order, serial, value, reader))

        while heap:
            _order, serial, value, reader = heapq.heappop(heap)
            yield value
            try:
                order, next_value = next(reader)
            except StopIteration:
                continue
            heapq.heappush(heap, (order, serial, next_value, reader))
    finally:
        for reader in readers:
            close = getattr(reader, "close", None)
            if callable(close):
                close()


def spilled_group_aggregate(
    source: Iterable[Any],
    *,
    key_names: tuple[str, ...],
    keys: tuple[Callable[[Any], Any], ...],
    aggregation_items: AggregationItems,
    partitions: int,
    tempdir: str | os.PathLike[str] | None,
) -> Iterator[dict[str, Any]]:
    multiple_keys = len(keys) > 1
    with tempfile.TemporaryDirectory(prefix="fpstreams-group-", dir=tempdir) as directory_name:
        directory = Path(directory_name)
        inputs = _PartitionWriters(directory, "input", partitions, operation="group_by")
        iterator = iter(source)
        try:
            for position, row in enumerate(iterator):
                key = tuple(select(row) for select in keys) if multiple_keys else keys[0](row)
                bucket = _partition(key, partitions, operation="group_by")
                inputs.dump(bucket, (position, key, row))
        finally:
            inputs.close()
            close = getattr(iterator, "close", None)
            if callable(close):
                close()

        outputs = tuple(directory / f"result-{position}.bin" for position in range(partitions))
        for position, input_path in enumerate(inputs.paths):
            groups: dict[Any, tuple[int, dict[str, Any]]] = {}
            for first_position, key, row in _read(input_path):
                try:
                    first, states = groups[key]
                except KeyError:
                    first = first_position
                    states = initialize_aggregations(aggregation_items)
                    groups[key] = first, states
                step_aggregations(states, aggregation_items, row)

            output = _LazyWriter(outputs[position], operation="group_by")
            try:
                for key, (first, states) in groups.items():
                    key_values = key if multiple_keys else (key,)
                    result = dict(zip(key_names, key_values, strict=True))
                    result.update(finish_aggregations(states, aggregation_items))
                    output.dump((first, result))
            finally:
                output.close()

        yield from _merge_ordered(outputs)


def _partition_join_side(
    source: Iterable[Any],
    *,
    select: Callable[[Any], Any],
    writers: _PartitionWriters,
    partitions: int,
    as_record: Callable[[Any], dict[str, Any]],
    remember_columns: Callable[[Mapping[str, Any], list[str], set[str]], None],
    keys_only: bool = False,
) -> tuple[str, ...]:
    columns: list[str] = []
    seen: set[str] = set()
    iterator = iter(source)
    try:
        for position, row in enumerate(iterator):
            record = as_record(row)
            key = select(row)
            if not keys_only:
                remember_columns(record, columns, seen)
            bucket = _partition(key, partitions, operation="join")
            writers.dump(bucket, (position, key, None if keys_only else record))
    finally:
        writers.close()
        close = getattr(iterator, "close", None)
        if callable(close):
            close()
    return tuple(columns)


def _validated_join_rows(
    rows: Iterable[tuple[int, Any, Any]],
    *,
    validate: str,
    side: str,
) -> Iterator[tuple[int, Any, Any]]:
    """Yield partition rows while enforcing the requested side's uniqueness."""
    requires_unique = validate in {"1:1", "1:m"} if side == "left" else validate in {"1:1", "m:1"}
    if not requires_unique:
        yield from rows
        return

    seen: set[Any] = set()
    for item in rows:
        key = item[1]
        if key in seen:
            raise ValueError(
                f"join validate={validate!r} requires unique {side} keys; found duplicate {key!r}"
            )
        seen.add(key)
        yield item


def spilled_join(
    left_source: Iterable[Any],
    right_source: Iterable[Any],
    *,
    left_key: Callable[[Any], Any],
    right_key: Callable[[Any], Any],
    how: str,
    shared_names: set[str],
    suffix: str,
    validate: str,
    partitions: int,
    tempdir: str | os.PathLike[str] | None,
    as_record: Callable[[Any], dict[str, Any]],
    remember_columns: Callable[[Mapping[str, Any], list[str], set[str]], None],
    join_targets: Callable[..., tuple[tuple[str, str], ...]],
    merge_records: Callable[
        [dict[str, Any], dict[str, Any], tuple[tuple[str, str], ...], set[str]],
        dict[str, Any],
    ],
) -> Iterator[dict[str, Any]]:
    """Join hash partitions and merge their position-tagged output in stable order."""
    with tempfile.TemporaryDirectory(prefix="fpstreams-join-", dir=tempdir) as directory_name:
        directory = Path(directory_name)
        right_inputs = _PartitionWriters(directory, "right", partitions, operation="join")
        right_columns = _partition_join_side(
            right_source,
            select=right_key,
            writers=right_inputs,
            partitions=partitions,
            as_record=as_record,
            remember_columns=remember_columns,
            keys_only=how in {"semi", "anti"},
        )
        left_inputs = _PartitionWriters(directory, "left", partitions, operation="join")
        left_columns = _partition_join_side(
            left_source,
            select=left_key,
            writers=left_inputs,
            partitions=partitions,
            as_record=as_record,
            remember_columns=remember_columns,
        )

        left_outputs = tuple(directory / f"left-result-{i}.bin" for i in range(partitions))
        right_outputs = tuple(directory / f"right-result-{i}.bin" for i in range(partitions))
        global_targets = (
            join_targets(
                left_columns,
                right_columns,
                shared_names=shared_names,
                suffix=suffix,
            )
            if how in {"right", "full"}
            else ()
        )

        # Process partitions independently, then merge position-tagged outputs for stable order.
        for partition_position in range(partitions):
            right_rows = list(
                _validated_join_rows(
                    _read(right_inputs.paths[partition_position]),
                    validate=validate,
                    side="right",
                )
            )
            left_output = _LazyWriter(left_outputs[partition_position], operation="join")
            right_output = _LazyWriter(right_outputs[partition_position], operation="join")
            left_rows = _validated_join_rows(
                _read(left_inputs.paths[partition_position]),
                validate=validate,
                side="left",
            )
            try:
                if how in {"semi", "anti"}:
                    right_keys = {key for _position, key, _record in right_rows}
                    for left_position, key, left in left_rows:
                        matched = key in right_keys
                        if matched == (how == "semi"):
                            left_output.dump(((left_position, 0), left))
                    continue

                index: dict[Any, list[int]] = {}
                for local_position, (_right_position, key, _right) in enumerate(right_rows):
                    index.setdefault(key, []).append(local_position)
                matched_right = bytearray(len(right_rows))

                for left_position, key, left in left_rows:
                    matches = index.get(key, ())
                    if matches:
                        targets = (
                            global_targets
                            if how in {"right", "full"}
                            else join_targets(
                                left,
                                right_columns,
                                shared_names=shared_names,
                                suffix=suffix,
                            )
                        )
                        for ordinal, local_position in enumerate(matches):
                            matched_right[local_position] = 1
                            right = right_rows[local_position][2]
                            result = merge_records(left, right, targets, shared_names)
                            left_output.dump(((left_position, ordinal), result))
                    elif how in {"left", "full"}:
                        targets = (
                            global_targets
                            if how == "full"
                            else join_targets(
                                left,
                                right_columns,
                                shared_names=shared_names,
                                suffix=suffix,
                            )
                        )
                        merged = left.copy()
                        for name, target in targets:
                            if name not in shared_names:
                                merged[target] = None
                        left_output.dump(((left_position, 0), merged))

                if how in {"right", "full"}:
                    for local_position, (right_position, _key, right) in enumerate(right_rows):
                        if matched_right[local_position]:
                            continue
                        merged = {name: None for name in left_columns}
                        for name, target in global_targets:
                            if name in right:
                                merged[target] = right[name]
                        right_output.dump((right_position, merged))
            finally:
                left_output.close()
                right_output.close()

        yield from _merge_ordered(left_outputs)
        if how in {"right", "full"}:
            yield from _merge_ordered(right_outputs)
