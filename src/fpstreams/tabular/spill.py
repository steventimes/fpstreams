"""Partitioned external-memory joins and grouped aggregations."""

from __future__ import annotations

import operator
import os
import tempfile
from collections.abc import Callable, Generator, Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..collecting.aggregation import (
    AggregationItems,
    finish_aggregations,
    initialize_aggregations,
    step_aggregations,
)
from ..runtime.query import QueryRuntime
from ..storage import SpillStore
from .spill_io import (
    PartitionFile,
    group_count_partial,
    group_count_partial_stats,
    repartition,
)
from .spill_io import (
    SpillLazyWriter as _SpillLazyWriter,
)
from .spill_io import (
    SpillPartitionWriters as _SpillPartitionWriters,
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
_MIN_SPILL_FILES = 3
# Count compaction keeps exact frequency evidence and retained states bounded.
# Two 256-row windows reject persistently cold streams. Stronger recurrence may
# earn up to eight windows to reach admission, while a dominant key activates
# immediately; an active window must then retain at least one row in eight. This
# admits sustained low-cardinality/skewed streams while retiring to canonical raw
# spilling when a prefix signal goes stale. Exact keys are tracked without
# collision-based estimates, and the flat-row proof scans at most 127 fields.
# String limits are measured in characters; bytes and integers use bytes.
_HOT_COUNT_ADMISSION = 8
_HOT_COUNT_ADMISSION_CAPACITY = 8_192
_HOT_COUNT_CACHE_SIZE = 512
_HOT_COUNT_MAX_KEY_BYTES = 4 * 1_024
_HOT_COUNT_SAMPLE_ROWS = 256
_HOT_COUNT_COLD_RECHECK_WINDOWS = 2
_HOT_COUNT_EXTENSION_WINDOWS = 4
_HOT_COUNT_MAX_SAMPLE_WINDOWS = _HOT_COUNT_ADMISSION
_HOT_COUNT_DOMINANT_DIVISOR = 2
_HOT_COUNT_PRODUCTIVITY_DIVISOR = 8
_PICKLE_PURE_SCAN_LIMIT = 256
_PICKLE_PURE_VALUES = frozenset({type(None), bool, int, float, str, bytes})
JoinRow = tuple[int, Any, dict[str, Any] | None]
JoinTargets = tuple[tuple[str, str], ...]
JoinTargetBuilder = Callable[..., JoinTargets]
MergeRecords = Callable[
    [dict[str, Any], dict[str, Any], JoinTargets, set[str]],
    dict[str, Any],
]


class _AdmissionSketch:
    """Bounded exact frequency evidence; saturation merely declines new keys."""

    __slots__ = ("_counts",)

    def __init__(self) -> None:
        self._counts: dict[Any, int] = {}

    def add(self, key: Any) -> int:
        """Increment a tracked exact key or report one for an untracked saturated key."""
        count = self._counts.get(key)
        if count is not None:
            count += 1
            self._counts[key] = count
            return count
        if len(self._counts) < _HOT_COUNT_ADMISSION_CAPACITY:
            self._counts[key] = 1
        return 1

    @property
    def tracked_keys(self) -> int:
        """Return the number of exact keys currently retaining frequency evidence."""
        return len(self._counts)


def _sample_predicts_compaction(
    sketch: _AdmissionSketch,
    *,
    observed_rows: int,
    window_peak: int,
    window_rows: int,
) -> bool:
    """Require amortizable recurrence or a dominant key before retaining rows."""
    tracked = sketch.tracked_keys
    amortizable = tracked > 0 and observed_rows >= tracked * _HOT_COUNT_ADMISSION
    dominant = window_peak * _HOT_COUNT_DOMINANT_DIVISOR > window_rows
    return amortizable or dominant


def _sampling_can_continue(
    sketch: _AdmissionSketch,
    *,
    observed_rows: int,
    observed_windows: int,
) -> bool:
    """Spend more proof work only while recurrence can plausibly reach admission."""
    if observed_windows >= _HOT_COUNT_MAX_SAMPLE_WINDOWS:
        return False
    if observed_windows < _HOT_COUNT_COLD_RECHECK_WINDOWS:
        return True
    tracked = sketch.tracked_keys
    if tracked == 0 or observed_rows < tracked * _HOT_COUNT_DOMINANT_DIVISOR:
        return False
    if observed_windows < _HOT_COUNT_EXTENSION_WINDOWS:
        return True
    return observed_rows >= tracked * (_HOT_COUNT_ADMISSION // 2)


def _active_window_is_productive(*, retained_rows: int, observed_rows: int) -> bool:
    """Keep an active cache only while its measured retention can repay the proof work."""
    return retained_rows * _HOT_COUNT_PRODUCTIVITY_DIVISOR >= observed_rows


def _exact_primitive_hot_key(key: Any) -> bool:
    """Accept bounded protocol-free keys while leaving non-reflexive floats raw."""
    key_type = type(key)
    if key_type is bool or key is None:
        return True
    if key_type is int:
        return bool(key.bit_length() <= _HOT_COUNT_MAX_KEY_BYTES * 8)
    if key_type is float:
        return bool(key == key)
    if key_type is str or key_type is bytes:
        return len(key) <= _HOT_COUNT_MAX_KEY_BYTES
    return False


def _pickle_pure_row(row: Any, key: Any) -> bool:
    """Accept a bounded flat row whose pickle frame size survives roundtrips."""
    if type(row) is not dict:
        return False
    if 1 + (2 * len(row)) > _PICKLE_PURE_SCAN_LIMIT:
        return False
    key_type = type(key)
    has_string_value = key_type is str
    first_bytes = key if key_type is bytes else None
    multiple_bytes = False
    for field, value in row.items():
        value_type = type(value)
        if type(field) is not str or value_type not in _PICKLE_PURE_VALUES:
            return False
        if value_type is str:
            has_string_value = True
        elif value_type is bytes:
            if first_bytes is None:
                first_bytes = value
            else:
                multiple_bytes = True
    if not has_string_value and not multiple_bytes:
        return True

    string_identities: dict[str, int] = {}
    bytes_identities: dict[bytes, int] = {}
    values = (key, *row.keys(), *row.values())
    for value in values:
        value_type = type(value)
        if value_type is str:
            identity = id(value)
            previous = string_identities.get(value)
            if previous is not None and previous != identity:
                return False
            string_identities[value] = identity
        elif value_type is bytes:
            identity = id(value)
            previous = bytes_identities.get(value)
            if previous is not None and previous != identity:
                return False
            bytes_identities[value] = identity
    return True


def _flush_count_partials(
    hot: dict[Any, list[int]],
    *,
    writers: _SpillPartitionWriters,
    partitions: int,
) -> None:
    """Write retained states before a protocol-bearing row and at end of input."""
    for key, (first, count, virtual_rows, virtual_frame_bytes) in hot.items():
        bucket = _partition(key, partitions, operation="group_by")
        writers.dump(
            bucket,
            group_count_partial(
                first,
                key,
                count,
                virtual_rows,
                virtual_frame_bytes,
            ),
            virtual_rows=virtual_rows,
            virtual_frame_bytes=virtual_frame_bytes,
        )
    hot.clear()


def _partition_count_rows(
    iterator: Iterator[Any],
    *,
    select_key: Callable[[Any], Any],
    writers: _SpillPartitionWriters,
    partitions: int,
) -> None:
    """Adaptively write cold rows verbatim and retain only productive hot count states."""
    sketch = _AdmissionSketch()
    window_sketch = _AdmissionSketch()
    # Values are [first position, count, canonical rows, canonical frame bytes].
    hot: dict[Any, list[int]] = {}
    sampling = True
    sample_rows = 0
    sample_windows = 0
    window_rows = 0
    window_peak = 0
    retained_rows = 0
    for position, row in enumerate(iterator):
        key = select_key(row)
        bucket = _partition(key, partitions, operation="group_by")
        raw = (position, key, row)
        eligible = _exact_primitive_hot_key(key) and _pickle_pure_row(row, key)
        if not eligible:
            # A later custom equality or pickle protocol can observe grouping and
            # serialization order.  Materialize every earlier state before this
            # row, then stay byte-for-byte canonical for the rest of the source.
            _flush_count_partials(hot, writers=writers, partitions=partitions)
            writers.dump(bucket, raw)
            _partition_raw_group_rows(
                iterator,
                start_position=position + 1,
                select_key=select_key,
                writers=writers,
                partitions=partitions,
            )
            return
        if sampling:
            sketch.add(key)
            window_peak = max(window_peak, window_sketch.add(key))
            writers.dump(bucket, raw)
            sample_rows += 1
            window_rows += 1
            if window_rows < _HOT_COUNT_SAMPLE_ROWS:
                continue
            if _sample_predicts_compaction(
                sketch,
                observed_rows=sample_rows,
                window_peak=window_peak,
                window_rows=window_rows,
            ):
                sampling = False
                window_rows = 0
                retained_rows = 0
                continue
            sample_windows += 1
            if not _sampling_can_continue(
                sketch,
                observed_rows=sample_rows,
                observed_windows=sample_windows,
            ):
                _partition_raw_group_rows(
                    iterator,
                    start_position=position + 1,
                    select_key=select_key,
                    writers=writers,
                    partitions=partitions,
                )
                return
            window_sketch = _AdmissionSketch()
            window_rows = 0
            window_peak = 0
            continue
        entry = hot.get(key)
        if entry is not None:
            raw_size = writers.encoded_size(raw)
            entry[1] += 1
            entry[2] += 1
            entry[3] += raw_size
            retained_rows += 1
        else:
            frame = writers.encode(raw)
            estimate = sketch.add(key)
            if estimate < _HOT_COUNT_ADMISSION or len(hot) >= _HOT_COUNT_CACHE_SIZE:
                writers.dump_encoded(bucket, frame)
            else:
                entry = [position, 1, 1, len(frame)]
                hot[key] = entry
                retained_rows += 1
        window_rows += 1
        if window_rows == _HOT_COUNT_SAMPLE_ROWS:
            if not _active_window_is_productive(
                retained_rows=retained_rows,
                observed_rows=window_rows,
            ):
                _flush_count_partials(hot, writers=writers, partitions=partitions)
                _partition_raw_group_rows(
                    iterator,
                    start_position=position + 1,
                    select_key=select_key,
                    writers=writers,
                    partitions=partitions,
                )
                return
            window_rows = 0
            retained_rows = 0

    _flush_count_partials(hot, writers=writers, partitions=partitions)


def _partition_raw_group_rows(
    iterator: Iterator[Any],
    *,
    start_position: int,
    select_key: Callable[[Any], Any],
    writers: _SpillPartitionWriters,
    partitions: int,
) -> None:
    """Finish an auto-cold stream through the canonical raw partition loop."""
    for position, row in enumerate(iterator, start=start_position):
        key = select_key(row)
        bucket = _partition(key, partitions, operation="group_by")
        writers.dump(bucket, (position, key, row))


def require_spill_file_budget(runtime: QueryRuntime | None, operation: str) -> None:
    """Reject an unusable injected budget before either one-shot source is opened."""
    if runtime is None:
        return
    limit = runtime.limits.max_open_files
    current = runtime.metrics.open_files
    available = limit - current
    if available < _MIN_SPILL_FILES:
        raise RuntimeError(
            f"spilled {operation} requires at least {_MIN_SPILL_FILES} available file slots: "
            f"available={available}, current={current}, limit={limit}"
        )


@contextmanager
def _owned_spill_store(
    parent: Path,
    operation: str,
    runtime: QueryRuntime | None,
) -> Generator[SpillStore, None, None]:
    """Reuse a query runtime or own one for a spill generator's full lifetime."""
    owned = runtime is None
    active = QueryRuntime() if runtime is None else runtime
    try:
        # These partitions are consumed and deleted by this query. Flushing is
        # required before reopening, but crash durability would only slow them.
        store = SpillStore(active, parent=parent, operation=operation, durable=False)
        yield store
    except BaseException as error:
        if owned:
            active.close(error)
        raise
    else:
        if owned:
            active.close()


@contextmanager
def _closing_iterator(iterator: Iterator[Any]) -> Generator[Iterator[Any], None, None]:
    """Close a partition reader deterministically, including validation failures."""
    from ..runtime.iterators import closing_iterators

    with closing_iterators((iterator,)):
        yield iterator


def _close_partition_input(
    writers: _SpillPartitionWriters,
    iterator: Iterator[Any],
    active_error: BaseException | None,
) -> None:
    """Close writers and source while retaining the first active pipeline failure."""
    from ..runtime.resources import _add_cleanup_failure

    errors: list[BaseException] = []
    try:
        writers.close(active_error)
    except BaseException as error:
        errors.append(error)
    close = getattr(iterator, "close", None)
    if callable(close):
        try:
            close()
        except BaseException as error:
            errors.append(error)
    _add_cleanup_failure(active_error, errors)


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
    store: SpillStore,
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
        store=store,
    )
    for child in children:
        yield from _bounded_group_partitions(
            child,
            directory=directory,
            partitions=partitions,
            limits=limits,
            store=store,
            depth=depth + 1,
        )


def _bounded_join_partitions(
    left: PartitionFile,
    right: PartitionFile,
    *,
    directory: Path,
    partitions: int,
    limits: SpillLimits,
    store: SpillStore,
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
        store=store,
    )
    right_children = repartition(
        right,
        directory,
        f"right-depth-{next_depth}-{right.path.stem}",
        partitions,
        operation="join",
        salt=salt,
        store=store,
    )
    for left_child, right_child in zip(left_children, right_children, strict=True):
        yield from _bounded_join_partitions(
            left_child,
            right_child,
            directory=directory,
            partitions=partitions,
            limits=limits,
            store=store,
            depth=next_depth,
        )


def _partition_group_inputs(
    iterator: Iterator[Any],
    *,
    keys: tuple[Callable[[Any], Any], ...],
    writers: _SpillPartitionWriters,
    partitions: int,
    count_spec: tuple[str, str] | None,
) -> None:
    """Write canonical rows or the closed count-only hot/cold representation."""
    if count_spec is not None:
        _partition_count_rows(
            iterator,
            select_key=keys[0],
            writers=writers,
            partitions=partitions,
        )
        return
    multiple_keys = len(keys) > 1
    for position, row in enumerate(iterator):
        key = tuple(select(row) for select in keys) if multiple_keys else keys[0](row)
        bucket = _partition(key, partitions, operation="group_by")
        writers.dump(bucket, (position, key, row))


def spilled_group_aggregate(
    source: Iterable[Any],
    *,
    key_names: tuple[str, ...],
    keys: tuple[Callable[[Any], Any], ...],
    aggregation_items: AggregationItems,
    partitions: int,
    tempdir: str | os.PathLike[str] | None,
    limits: SpillLimits,
    runtime: QueryRuntime | None = None,
    count_spec: tuple[str, str] | None = None,
) -> Iterator[dict[str, Any]]:
    """Partition rows by group key, aggregate bounded leaves, and restore first-seen order."""
    require_spill_file_budget(runtime, "group_by")
    if not keys:
        # Public ``Rows.group_by`` rejects this shape, but the executor keeps the
        # invariant explicit so malformed internal plans fail before opening a source.
        raise ValueError("spilled group aggregation requires at least one key")
    multiple_keys = len(keys) > 1
    with (
        tempfile.TemporaryDirectory(prefix="fpstreams-group-", dir=tempdir) as directory_name,
        _owned_spill_store(Path(directory_name), "group_by", runtime) as store,
    ):
        inputs = _SpillPartitionWriters(
            store.directory, "input", partitions, operation="group_by", store=store
        )
        iterator = iter(source)
        active_error: BaseException | None = None
        try:
            _partition_group_inputs(
                iterator,
                keys=keys,
                writers=inputs,
                partitions=partitions,
                count_spec=count_spec,
            )
        except BaseException as error:
            active_error = error
            raise
        finally:
            _close_partition_input(inputs, iterator, active_error)

        output_paths: list[Path] = []
        budget = SpillBudget("group_by", limits)
        for initial in inputs.files():
            for input_file in _bounded_group_partitions(
                initial,
                directory=store.directory,
                partitions=partitions,
                limits=limits,
                store=store,
            ):
                groups: dict[Any, list[Any]] = {}
                with _closing_iterator(_read(input_file.path, store=store)) as rows:
                    for spilled in rows:
                        first_position, key = spilled[0], spilled[1]
                        try:
                            entry = groups[key]
                        except KeyError:
                            if len(groups) >= limits.max_partition_rows:
                                raise_spill_limit(
                                    "group_by",
                                    "live group states",
                                    len(groups) + 1,
                                    "max_partition_rows",
                                    limits.max_partition_rows,
                                )
                            states = (
                                0
                                if count_spec is not None
                                else initialize_aggregations(aggregation_items)
                            )
                            entry = [first_position, states]
                            groups[key] = entry
                        if count_spec is None:
                            step_aggregations(entry[1], aggregation_items, spilled[2])
                        else:
                            partial = group_count_partial_stats(spilled)
                            increment = 1 if partial is None else spilled[3]
                            if first_position < entry[0]:
                                entry[0] = first_position
                            entry[1] += increment

                output_path = store.directory / f"result-{len(output_paths)}.bin"
                output_paths.append(output_path)
                output = _SpillLazyWriter(output_path, operation="group_by", store=store)
                active_error = None
                try:
                    group_items: Iterable[tuple[Any, list[Any]]] = groups.items()
                    if count_spec is not None:
                        # Retained states are flushed after raw rows, so carried
                        # positions restore canonical first-seen output order.
                        group_items = iter(sorted(group_items, key=lambda item: item[1][0]))
                    for key, (first, states) in group_items:
                        budget.add_output()
                        key_values = key if multiple_keys else (key,)
                        result = dict(zip(key_names, key_values, strict=True))
                        if count_spec is None:
                            result.update(finish_aggregations(states, aggregation_items))
                        else:
                            _key_field, output_name = count_spec
                            result[output_name] = states
                        output.dump((first, result))
                except BaseException as error:
                    active_error = error
                    raise
                finally:
                    output.close(active_error)

        yield from _merge_ordered(output_paths, store=store)


def _partition_join_side(
    source: Iterable[Any],
    *,
    select: Callable[[Any], Any],
    writers: _SpillPartitionWriters,
    partitions: int,
    as_record: Callable[[Any], dict[str, Any]],
    remember_columns: Callable[[Mapping[str, Any], list[str], set[str]], None],
    keys_only: bool = False,
) -> tuple[str, ...]:
    """Stream one join side into keyed partitions while recording its output columns."""
    columns: list[str] = []
    seen: set[str] = set()
    iterator = iter(source)
    active_error: BaseException | None = None
    try:
        for position, row in enumerate(iterator):
            record = as_record(row)
            key = select(row)
            if not keys_only:
                remember_columns(record, columns, seen)
            bucket = _partition(key, partitions, operation="join")
            writers.dump(bucket, (position, key, None if keys_only else record))
    except BaseException as error:
        active_error = error
        raise
    finally:
        _close_partition_input(writers, iterator, active_error)
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
    store: SpillStore,
) -> Iterator[tuple[PartitionFile, PartitionFile]]:
    """Pair corresponding partitions and yield recursively bounded join leaves."""
    for left_file, right_file in zip(left_files, right_files, strict=True):
        yield from _bounded_join_partitions(
            left_file,
            right_file,
            directory=directory,
            partitions=partitions,
            limits=limits,
            store=store,
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
    output: _SpillLazyWriter,
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
    output: _SpillLazyWriter,
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
    output: _SpillLazyWriter,
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
    output: _SpillLazyWriter,
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
    output: _SpillLazyWriter,
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
    store: SpillStore,
) -> None:
    """Validate and join one bounded partition pair into ordered temporary outputs."""
    # Validation can stop in the middle of either generator. Keep both the raw
    # file reader and its validating wrapper under explicit ownership rather than
    # relying on implementation-specific generator garbage collection.
    with (
        _closing_iterator(_read(right_input.path, store=store)) as right_reader,
        _closing_iterator(
            _validated_join_rows(right_reader, validate=config.validate, side="right")
        ) as validated_right,
    ):
        right_rows = list(validated_right)
    with (
        _closing_iterator(_read(left_input.path, store=store)) as left_reader,
        _closing_iterator(
            _validated_join_rows(left_reader, validate=config.validate, side="left")
        ) as left_rows,
    ):
        left_output = _SpillLazyWriter(left_output_path, operation="join", store=store)
        right_output = _SpillLazyWriter(right_output_path, operation="join", store=store)
        active_error: BaseException | None = None
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
        except BaseException as error:
            active_error = error
            raise
        finally:
            from ..runtime.resources import _add_cleanup_failure

            errors: list[BaseException] = []
            for output in (left_output, right_output):
                try:
                    output.close(active_error)
                except BaseException as error:
                    errors.append(error)
            _add_cleanup_failure(active_error, errors)


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
    runtime: QueryRuntime | None = None,
) -> Iterator[dict[str, Any]]:
    """Join hash partitions and merge their position-tagged output in stable order."""
    require_spill_file_budget(runtime, "join")
    with (
        tempfile.TemporaryDirectory(prefix="fpstreams-join-", dir=tempdir) as directory_name,
        _owned_spill_store(Path(directory_name), "join", runtime) as store,
    ):
        right_inputs = _SpillPartitionWriters(
            store.directory, "right", partitions, operation="join", store=store
        )
        right_columns = _partition_join_side(
            right_source,
            select=right_key,
            writers=right_inputs,
            partitions=partitions,
            as_record=as_record,
            remember_columns=remember_columns,
            keys_only=how in {"semi", "anti"},
        )
        left_inputs = _SpillPartitionWriters(
            store.directory, "left", partitions, operation="join", store=store
        )
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
            directory=store.directory,
            partitions=partitions,
            limits=limits,
            store=store,
        )
        for serial, (left_input, right_input) in enumerate(leaf_pairs):
            left_output_path = store.directory / f"left-result-{serial}.bin"
            right_output_path = store.directory / f"right-result-{serial}.bin"
            left_outputs.append(left_output_path)
            right_outputs.append(right_output_path)
            _process_join_leaf(
                left_input,
                right_input,
                left_output_path=left_output_path,
                right_output_path=right_output_path,
                config=config,
                budget=budget,
                store=store,
            )

        yield from _merge_ordered(left_outputs, store=store)
        if how in {"right", "full"}:
            yield from _merge_ordered(right_outputs, store=store)
