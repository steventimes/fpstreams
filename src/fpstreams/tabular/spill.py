"""Partitioned external-memory joins and grouped aggregations."""

from __future__ import annotations

import operator
import os
import tempfile
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..collecting.aggregation import (
    AggregationItems,
    finish_aggregations,
    initialize_aggregations,
    step_aggregations,
)
from .spill_io import (
    LazyWriter as _LazyWriter,
)
from .spill_io import (
    PartitionFile,
    repartition,
)
from .spill_io import (
    PartitionWriters as _PartitionWriters,
)
from .spill_io import (
    merge_ordered as _merge_ordered,
)
from .spill_io import (
    partition as _partition,
)
from .spill_io import (
    read as _read,
)
from .spill_limits import SpillBudget, SpillLimits, raise_spill_limit

_MAX_PARTITIONS = 256
JoinRow = tuple[int, Any, dict[str, Any] | None]
JoinTargets = tuple[tuple[str, str], ...]
JoinTargetBuilder = Callable[..., JoinTargets]
MergeRecords = Callable[
    [dict[str, Any], dict[str, Any], JoinTargets, set[str]],
    dict[str, Any],
]


def validate_partitions(partitions: int) -> int:
    """Coerce and validate a partition count between two and the implementation cap."""
    try:
        count = operator.index(partitions)
    except TypeError:
        raise TypeError("partitions must be an integer") from None
    if count < 2 or count > _MAX_PARTITIONS:
        raise ValueError(f"partitions must be between 2 and {_MAX_PARTITIONS}")
    return count


def _partition_issue(
    file: PartitionFile,
    limits: SpillLimits,
) -> tuple[str, int, str, int] | None:
    """Return the first row-count or byte-size limit exceeded by a partition."""
    if file.rows > limits.max_partition_rows:
        return ("rows", file.rows, "max_partition_rows", limits.max_partition_rows)
    if file.bytes > limits.max_partition_bytes:
        return ("bytes", file.bytes, "max_partition_bytes", limits.max_partition_bytes)
    return None


def _repartition_salt(depth: int) -> int:
    """Derive a deterministic nonzero hash salt for one repartition depth."""
    return 0x9E3779B1 * depth


def _bounded_group_partitions(
    source: PartitionFile,
    *,
    directory: Path,
    partitions: int,
    limits: SpillLimits,
    depth: int = 0,
) -> Iterator[PartitionFile]:
    """Recursively repartition group data until every non-empty leaf fits its limits."""
    issue = _partition_issue(source, limits)
    if issue is None:
        if source.rows:
            yield source
        return
    if depth >= limits.max_repartition_depth:
        measurement, actual, field, allowed = issue
        raise_spill_limit(
            "group_by",
            f"partition {measurement}",
            actual,
            field,
            allowed,
            depth=depth,
        )
    children = repartition(
        source,
        directory,
        f"group-depth-{depth + 1}-{source.path.stem}",
        partitions,
        operation="group_by",
        salt=_repartition_salt(depth + 1),
    )
    for child in children:
        yield from _bounded_group_partitions(
            child,
            directory=directory,
            partitions=partitions,
            limits=limits,
            depth=depth + 1,
        )


def _bounded_join_partitions(
    left: PartitionFile,
    right: PartitionFile,
    *,
    directory: Path,
    partitions: int,
    limits: SpillLimits,
    depth: int = 0,
) -> Iterator[tuple[PartitionFile, PartitionFile]]:
    """Recursively repartition matching join sides until both leaves fit their limits."""
    left_issue = _partition_issue(left, limits)
    right_issue = _partition_issue(right, limits)
    if left_issue is None and right_issue is None:
        if left.rows or right.rows:
            yield left, right
        return
    if depth >= limits.max_repartition_depth:
        side, issue = ("left", left_issue) if left_issue is not None else ("right", right_issue)
        if issue is None:
            raise RuntimeError("oversized join partition is missing limit details")
        measurement, actual, field, allowed = issue
        raise_spill_limit(
            "join",
            f"{side} partition {measurement}",
            actual,
            field,
            allowed,
            depth=depth,
        )
    next_depth = depth + 1
    salt = _repartition_salt(next_depth)
    left_children = repartition(
        left,
        directory,
        f"left-depth-{next_depth}-{left.path.stem}",
        partitions,
        operation="join",
        salt=salt,
    )
    right_children = repartition(
        right,
        directory,
        f"right-depth-{next_depth}-{right.path.stem}",
        partitions,
        operation="join",
        salt=salt,
    )
    for left_child, right_child in zip(left_children, right_children, strict=True):
        yield from _bounded_join_partitions(
            left_child,
            right_child,
            directory=directory,
            partitions=partitions,
            limits=limits,
            depth=next_depth,
        )


def spilled_group_aggregate(
    source: Iterable[Any],
    *,
    key_names: tuple[str, ...],
    keys: tuple[Callable[[Any], Any], ...],
    aggregation_items: AggregationItems,
    partitions: int,
    tempdir: str | os.PathLike[str] | None,
    limits: SpillLimits,
) -> Iterator[dict[str, Any]]:
    """Partition rows by group key, aggregate bounded leaves, and restore first-seen order."""
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

        output_paths: list[Path] = []
        budget = SpillBudget("group_by", limits)
        for initial in inputs.files():
            for input_file in _bounded_group_partitions(
                initial,
                directory=directory,
                partitions=partitions,
                limits=limits,
            ):
                groups: dict[Any, tuple[int, dict[str, Any]]] = {}
                for first_position, key, row in _read(input_file.path):
                    try:
                        first, states = groups[key]
                    except KeyError:
                        if len(groups) >= limits.max_partition_rows:
                            raise_spill_limit(
                                "group_by",
                                "live group states",
                                len(groups) + 1,
                                "max_partition_rows",
                                limits.max_partition_rows,
                            )
                        first = first_position
                        states = initialize_aggregations(aggregation_items)
                        groups[key] = first, states
                    step_aggregations(states, aggregation_items, row)

                output_path = directory / f"result-{len(output_paths)}.bin"
                output_paths.append(output_path)
                output = _LazyWriter(output_path, operation="group_by")
                try:
                    for key, (first, states) in groups.items():
                        budget.add_output()
                        key_values = key if multiple_keys else (key,)
                        result = dict(zip(key_names, key_values, strict=True))
                        result.update(finish_aggregations(states, aggregation_items))
                        output.dump((first, result))
                finally:
                    output.close()

        yield from _merge_ordered(output_paths)


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
    """Stream one join side into keyed partitions while recording its output columns."""
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


@dataclass(frozen=True, slots=True)
class _JoinLeafConfig:
    """Bundle join mode, schema mapping, validation, and record-merging policy for a leaf."""

    how: str
    shared_names: set[str]
    suffix: str
    validate: str
    left_columns: tuple[str, ...]
    right_columns: tuple[str, ...]
    global_targets: JoinTargets
    join_targets: JoinTargetBuilder
    merge_records: MergeRecords


def _join_leaf_pairs(
    left_files: tuple[PartitionFile, ...],
    right_files: tuple[PartitionFile, ...],
    *,
    directory: Path,
    partitions: int,
    limits: SpillLimits,
) -> Iterator[tuple[PartitionFile, PartitionFile]]:
    """Pair corresponding partitions and yield recursively bounded join leaves."""
    for left_file, right_file in zip(left_files, right_files, strict=True):
        yield from _bounded_join_partitions(
            left_file,
            right_file,
            directory=directory,
            partitions=partitions,
            limits=limits,
        )


def _require_join_record(record: dict[str, Any] | None) -> dict[str, Any]:
    """Return a materialized join record or flag an invalid internal keys-only row."""
    if record is None:
        raise RuntimeError("regular spilled join row is missing its record")
    return record


def _write_semi_or_anti_leaf(
    left_rows: Iterable[JoinRow],
    right_rows: Iterable[JoinRow],
    *,
    how: str,
    output: _LazyWriter,
    budget: SpillBudget,
) -> None:
    """Emit left rows selected by semi/anti key membership in the right leaf."""
    right_keys = {key for _position, key, _record in right_rows}
    for left_position, key, left_record in left_rows:
        if (key in right_keys) == (how == "semi"):
            budget.add_output()
            output.dump(((left_position, 0), _require_join_record(left_record)))


def _index_right_rows(right_rows: list[JoinRow]) -> dict[Any, list[int]]:
    """Map each right-side key to all matching local row positions."""
    index: dict[Any, list[int]] = {}
    for local_position, (_right_position, key, _right) in enumerate(right_rows):
        index.setdefault(key, []).append(local_position)
    return index


def _left_targets(config: _JoinLeafConfig, left: dict[str, Any]) -> JoinTargets:
    """Resolve right-column output names, using global names when unmatched rights may emit."""
    if config.how in {"right", "full"}:
        return config.global_targets
    return config.join_targets(
        left,
        config.right_columns,
        shared_names=config.shared_names,
        suffix=config.suffix,
    )


def _write_left_matches(
    left_position: int,
    left: dict[str, Any],
    matches: list[int] | tuple[()],
    right_rows: list[JoinRow],
    matched_right: bytearray,
    *,
    config: _JoinLeafConfig,
    output: _LazyWriter,
    budget: SpillBudget,
) -> bool:
    """Emit every right match for one left row and mark matched right positions."""
    if not matches:
        return False
    budget.check_matches(len(matches))
    targets = _left_targets(config, left)
    for ordinal, local_position in enumerate(matches):
        budget.add_output()
        matched_right[local_position] = 1
        right = _require_join_record(right_rows[local_position][2])
        result = config.merge_records(left, right, targets, config.shared_names)
        output.dump(((left_position, ordinal), result))
    return True


def _write_unmatched_left(
    left_position: int,
    left: dict[str, Any],
    *,
    config: _JoinLeafConfig,
    output: _LazyWriter,
    budget: SpillBudget,
) -> None:
    """Emit an unmatched left row with None-filled right-side columns."""
    budget.add_output()
    targets = (
        config.global_targets
        if config.how == "full"
        else config.join_targets(
            left,
            config.right_columns,
            shared_names=config.shared_names,
            suffix=config.suffix,
        )
    )
    merged = left.copy()
    for name, target in targets:
        if name not in config.shared_names:
            merged[target] = None
    output.dump(((left_position, 0), merged))


def _write_regular_left_rows(
    left_rows: Iterable[JoinRow],
    right_rows: list[JoinRow],
    *,
    config: _JoinLeafConfig,
    output: _LazyWriter,
    budget: SpillBudget,
) -> bytearray:
    """Drive a regular join from left rows and return flags for matched right rows."""
    index = _index_right_rows(right_rows)
    matched_right = bytearray(len(right_rows))
    for left_position, key, left_record in left_rows:
        left = _require_join_record(left_record)
        matched = _write_left_matches(
            left_position,
            left,
            index.get(key, ()),
            right_rows,
            matched_right,
            config=config,
            output=output,
            budget=budget,
        )
        if not matched and config.how in {"left", "full"}:
            _write_unmatched_left(
                left_position,
                left,
                config=config,
                output=output,
                budget=budget,
            )
    return matched_right


def _write_unmatched_right_rows(
    right_rows: list[JoinRow],
    matched_right: bytearray,
    *,
    config: _JoinLeafConfig,
    output: _LazyWriter,
    budget: SpillBudget,
) -> None:
    """Emit unmatched right rows with None-filled left-side columns."""
    for local_position, (right_position, _key, right_record) in enumerate(right_rows):
        if matched_right[local_position]:
            continue
        budget.add_output()
        right = _require_join_record(right_record)
        merged = {name: None for name in config.left_columns}
        for name, target in config.global_targets:
            if name in right:
                merged[target] = right[name]
        output.dump((right_position, merged))


def _process_join_leaf(
    left_input: PartitionFile,
    right_input: PartitionFile,
    *,
    left_output_path: Path,
    right_output_path: Path,
    config: _JoinLeafConfig,
    budget: SpillBudget,
) -> None:
    """Validate and join one bounded partition pair into ordered temporary outputs."""
    right_rows = list(
        _validated_join_rows(
            _read(right_input.path),
            validate=config.validate,
            side="right",
        )
    )
    left_rows = _validated_join_rows(
        _read(left_input.path),
        validate=config.validate,
        side="left",
    )
    left_output = _LazyWriter(left_output_path, operation="join")
    right_output = _LazyWriter(right_output_path, operation="join")
    try:
        if config.how in {"semi", "anti"}:
            _write_semi_or_anti_leaf(
                left_rows,
                right_rows,
                how=config.how,
                output=left_output,
                budget=budget,
            )
            return
        matched_right = _write_regular_left_rows(
            left_rows,
            right_rows,
            config=config,
            output=left_output,
            budget=budget,
        )
        if config.how in {"right", "full"}:
            _write_unmatched_right_rows(
                right_rows,
                matched_right,
                config=config,
                output=right_output,
                budget=budget,
            )
    finally:
        left_output.close()
        right_output.close()


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
    limits: SpillLimits,
    as_record: Callable[[Any], dict[str, Any]],
    remember_columns: Callable[[Mapping[str, Any], list[str], set[str]], None],
    join_targets: JoinTargetBuilder,
    merge_records: MergeRecords,
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

        left_outputs: list[Path] = []
        right_outputs: list[Path] = []
        budget = SpillBudget("join", limits)
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

        config = _JoinLeafConfig(
            how,
            shared_names,
            suffix,
            validate,
            left_columns,
            right_columns,
            global_targets,
            join_targets,
            merge_records,
        )
        leaf_pairs = _join_leaf_pairs(
            left_inputs.files(),
            right_inputs.files(),
            directory=directory,
            partitions=partitions,
            limits=limits,
        )
        for serial, (left_input, right_input) in enumerate(leaf_pairs):
            left_output_path = directory / f"left-result-{serial}.bin"
            right_output_path = directory / f"right-result-{serial}.bin"
            left_outputs.append(left_output_path)
            right_outputs.append(right_output_path)
            _process_join_leaf(
                left_input,
                right_input,
                left_output_path=left_output_path,
                right_output_path=right_output_path,
                config=config,
                budget=budget,
            )

        yield from _merge_ordered(left_outputs)
        if how in {"right", "full"}:
            yield from _merge_ordered(right_outputs)
