"""Join planning and in-memory execution for record pipelines."""

from __future__ import annotations

import os
from collections.abc import Callable, Iterable, Iterator
from typing import Any, Literal, TypeAlias

from ..expressions.selectors import Selector, compile_selector
from .records import _as_record, _remember_columns
from .spill import spilled_join, validate_partitions
from .spill_limits import SpillLimits

JoinSelector: TypeAlias = Selector | tuple[Selector, ...]
JoinValidation: TypeAlias = Literal["m:m", "1:1", "1:m", "m:1"]

_JOIN_MODES = frozenset({"inner", "left", "right", "full", "semi", "anti"})
_JOIN_VALIDATIONS = frozenset({"m:m", "1:1", "1:m", "m:1"})


def _close_iterator(iterator: Iterator[Any]) -> None:
    """Close a source iterator when it exposes an explicit close hook."""
    close = getattr(iterator, "close", None)
    if callable(close):
        close()


def _compile_join_selector(selector: JoinSelector) -> Callable[[Any], Any]:
    """Compile one selector or a tuple of selectors into a join-key function."""
    if not isinstance(selector, tuple):
        return compile_selector(selector)
    if not selector:
        raise ValueError("composite join keys cannot be empty")
    selectors = tuple(compile_selector(part) for part in selector)

    def select_composite_key(row: Any) -> tuple[Any, ...]:
        """Return the ordered values that form a composite join key."""
        return tuple(select(row) for select in selectors)

    return select_composite_key


def _normalize_join_selectors(
    *,
    on: JoinSelector | None,
    left_on: JoinSelector | None,
    right_on: JoinSelector | None,
) -> tuple[JoinSelector, JoinSelector]:
    """Resolve shared and side-specific selectors into a left/right pair."""
    if on is not None:
        if left_on is not None or right_on is not None:
            raise ValueError("use either on or left_on/right_on")
        left_on = right_on = on
    elif left_on is None or right_on is None:
        raise ValueError("join requires on or both left_on and right_on")

    if isinstance(left_on, tuple) and isinstance(right_on, tuple) and len(left_on) != len(right_on):
        raise ValueError("left_on and right_on must contain the same number of keys")
    return left_on, right_on


def _shared_join_names(
    left_on: JoinSelector,
    right_on: JoinSelector,
) -> set[str]:
    """Return same-name scalar fields that should appear only once in output."""
    if isinstance(left_on, str) and left_on == right_on and "." not in left_on:
        return {left_on}
    if isinstance(left_on, tuple) and isinstance(right_on, tuple):
        return {
            left
            for left, right in zip(left_on, right_on, strict=True)
            if isinstance(left, str) and left == right and "." not in left
        }
    return set()


def _requires_unique_left(validate: str) -> bool:
    """Return whether a validation contract requires unique left keys."""
    return validate in {"1:1", "1:m"}


def _requires_unique_right(validate: str) -> bool:
    """Return whether a validation contract requires unique right keys."""
    return validate in {"1:1", "m:1"}


def _check_unique_key(
    seen: set[Any],
    key: Any,
    *,
    validate: str,
    side: Literal["left", "right"],
) -> None:
    """Record a key or raise when it violates the requested cardinality."""
    if key in seen:
        raise ValueError(
            f"join validate={validate!r} requires unique {side} keys; found duplicate {key!r}"
        )
    seen.add(key)


def _join_key_set(
    source: Iterable[Any],
    select: Callable[[Any], Any],
    *,
    validate: str,
) -> set[Any]:
    """Index only right-side keys for a semi or anti join."""
    keys: set[Any] = set()
    unique = _requires_unique_right(validate)
    iterator = iter(source)
    try:
        for row in iterator:
            _as_record(row)
            key = select(row)
            if unique and key in keys:
                raise ValueError(
                    f"join validate={validate!r} requires unique right keys; "
                    f"found duplicate {key!r}"
                )
            keys.add(key)
    finally:
        _close_iterator(iterator)
    return keys


def _join_record_index(
    source: Iterable[Any],
    select: Callable[[Any], Any],
    *,
    validate: str,
) -> tuple[tuple[str, ...], dict[Any, list[dict[str, Any]]]]:
    """Build the right-side record index used by left-driven joins."""
    columns: list[str] = []
    seen_columns: set[str] = set()
    index: dict[Any, list[dict[str, Any]]] = {}
    unique = _requires_unique_right(validate)
    iterator = iter(source)
    try:
        for row in iterator:
            record = _as_record(row)
            key = select(row)
            _remember_columns(record, columns, seen_columns)
            matches = index.get(key)
            if matches is None:
                index[key] = [record]
            elif unique:
                raise ValueError(
                    f"join validate={validate!r} requires unique right keys; "
                    f"found duplicate {key!r}"
                )
            else:
                matches.append(record)
    finally:
        _close_iterator(iterator)
    return tuple(columns), index


def _join_position_index(
    source: Iterable[Any],
    select: Callable[[Any], Any],
    *,
    validate: str,
) -> tuple[list[dict[str, Any]], tuple[str, ...], dict[Any, list[int]]]:
    """Materialize right rows and map each key to stable row positions."""
    records: list[dict[str, Any]] = []
    columns: list[str] = []
    seen_columns: set[str] = set()
    index: dict[Any, list[int]] = {}
    unique = _requires_unique_right(validate)
    iterator = iter(source)
    try:
        for row in iterator:
            record = _as_record(row)
            key = select(row)
            position = len(records)
            records.append(record)
            _remember_columns(record, columns, seen_columns)
            positions = index.get(key)
            if positions is None:
                index[key] = [position]
            elif unique:
                raise ValueError(
                    f"join validate={validate!r} requires unique right keys; "
                    f"found duplicate {key!r}"
                )
            else:
                positions.append(position)
    finally:
        _close_iterator(iterator)
    return records, tuple(columns), index


def _materialize_join_rows(
    source: Iterable[Any],
    select: Callable[[Any], Any],
    *,
    validate: str,
) -> tuple[list[tuple[dict[str, Any], Any]], tuple[str, ...]]:
    """Materialize left rows for right/full joins and optionally validate keys."""
    records: list[tuple[dict[str, Any], Any]] = []
    columns: list[str] = []
    seen_columns: set[str] = set()
    seen_keys: set[Any] | None = set() if _requires_unique_left(validate) else None
    iterator = iter(source)
    try:
        for row in iterator:
            record = _as_record(row)
            key = select(row)
            if seen_keys is not None:
                _check_unique_key(seen_keys, key, validate=validate, side="left")
            records.append((record, key))
            _remember_columns(record, columns, seen_columns)
    finally:
        _close_iterator(iterator)
    return records, tuple(columns)


def _merge_join_records(
    left: dict[str, Any],
    right: dict[str, Any],
    targets: tuple[tuple[str, str], ...],
    shared_names: set[str],
) -> dict[str, Any]:
    """Merge a matched pair without overwriting shared join fields."""
    merged = left.copy()
    for name, target in targets:
        if name in shared_names and target in merged:
            continue
        if name in right:
            merged[target] = right[name]
    return merged


def _join_targets(
    left_names: Iterable[str],
    right_names: Iterable[str],
    *,
    shared_names: set[str],
    suffix: str,
) -> tuple[tuple[str, str], ...]:
    """Map right-side fields to collision-free output field names."""
    left = set(left_names)
    used = set(left)
    targets: list[tuple[str, str]] = []
    for name in right_names:
        if name in shared_names:
            targets.append((name, name))
            continue
        target = f"{name}{suffix}" if name in left else name
        if target in used:
            from ..errors import DuplicateKeyError

            raise DuplicateKeyError(
                f"join maps right column {name!r} to existing output column {target!r}"
            )
        used.add(target)
        targets.append((name, target))
    return tuple(targets)


def _semi_or_anti_join(
    left_source: Iterable[Any],
    right_source: Iterable[Any],
    *,
    left_key: Callable[[Any], Any],
    right_key: Callable[[Any], Any],
    how: str,
    validate: str,
) -> Iterator[dict[str, Any]]:
    """Execute a key-only semi or anti join while streaming the left side."""
    keys = _join_key_set(right_source, right_key, validate=validate)
    seen_left: set[Any] | None = set() if _requires_unique_left(validate) else None
    iterator = iter(left_source)
    try:
        for row in iterator:
            left = _as_record(row)
            key = left_key(row)
            if seen_left is not None:
                _check_unique_key(seen_left, key, validate=validate, side="left")
            matched = key in keys
            if matched == (how == "semi"):
                yield left
    finally:
        _close_iterator(iterator)


def _left_driven_join(
    left_source: Iterable[Any],
    right_source: Iterable[Any],
    *,
    left_key: Callable[[Any], Any],
    right_key: Callable[[Any], Any],
    how: str,
    shared_names: set[str],
    suffix: str,
    validate: str,
) -> Iterator[dict[str, Any]]:
    """Execute an inner/left join using a right index and a streaming left side."""
    right_columns, record_index = _join_record_index(
        right_source,
        right_key,
        validate=validate,
    )
    seen_left: set[Any] | None = set() if _requires_unique_left(validate) else None
    iterator = iter(left_source)
    try:
        for row in iterator:
            left = _as_record(row)
            key = left_key(row)
            if seen_left is not None:
                _check_unique_key(seen_left, key, validate=validate, side="left")
            record_matches = record_index.get(key, ())
            if record_matches:
                targets = _join_targets(
                    left,
                    right_columns,
                    shared_names=shared_names,
                    suffix=suffix,
                )
                for right in record_matches:
                    yield _merge_join_records(left, right, targets, shared_names)
            elif how == "left":
                targets = _join_targets(
                    left,
                    right_columns,
                    shared_names=shared_names,
                    suffix=suffix,
                )
                merged = left.copy()
                for name, target in targets:
                    if name not in shared_names:
                        merged[target] = None
                yield merged
    finally:
        _close_iterator(iterator)


def _right_or_full_join(
    left_source: Iterable[Any],
    right_source: Iterable[Any],
    *,
    left_key: Callable[[Any], Any],
    right_key: Callable[[Any], Any],
    how: str,
    shared_names: set[str],
    suffix: str,
    validate: str,
) -> Iterator[dict[str, Any]]:
    """Execute a stable right/full join after validating both materialized sides."""
    right_records, right_columns, position_index = _join_position_index(
        right_source,
        right_key,
        validate=validate,
    )
    left_records, left_columns = _materialize_join_rows(
        left_source,
        left_key,
        validate=validate,
    )
    targets = _join_targets(
        left_columns,
        right_columns,
        shared_names=shared_names,
        suffix=suffix,
    )
    matched_right = bytearray(len(right_records))
    for left, key in left_records:
        position_matches = position_index.get(key, ())
        if position_matches:
            for right_position in position_matches:
                matched_right[right_position] = 1
                yield _merge_join_records(
                    left,
                    right_records[right_position],
                    targets,
                    shared_names,
                )
        elif how == "full":
            merged = left.copy()
            for name, target in targets:
                if name not in shared_names:
                    merged[target] = None
            yield merged

    # Right-only rows follow left-driven output, preserving existing stable order.
    for right_position, right in enumerate(right_records):
        if matched_right[right_position]:
            continue
        merged = {name: None for name in left_columns}
        for name, target in targets:
            if name in right:
                merged[target] = right[name]
        yield merged


def _build_join(
    left_source: Iterable[Any],
    right_source: Iterable[Any],
    *,
    on: JoinSelector | None,
    left_on: JoinSelector | None,
    right_on: JoinSelector | None,
    how: str,
    suffix: str,
    validate: str,
    partitions: int | None,
    tempdir: str | os.PathLike[str] | None,
    limits: SpillLimits | None,
) -> Callable[[], Iterator[dict[str, Any]]]:
    """Validate a join plan and return its deferred iterator factory."""
    if how not in _JOIN_MODES:
        raise ValueError(f"how must be one of {sorted(_JOIN_MODES)!r}")
    if validate not in _JOIN_VALIDATIONS:
        raise ValueError(f"validate must be one of {sorted(_JOIN_VALIDATIONS)!r}")
    normalized_left, normalized_right = _normalize_join_selectors(
        on=on,
        left_on=left_on,
        right_on=right_on,
    )
    partition_count = None if partitions is None else validate_partitions(partitions)
    if tempdir is not None and partition_count is None:
        raise ValueError("tempdir requires partitions")
    if limits is not None and partition_count is None:
        raise ValueError("limits requires partitions")
    spill_limits = limits or SpillLimits()

    left_key = _compile_join_selector(normalized_left)
    right_key = _compile_join_selector(normalized_right)
    shared_names = _shared_join_names(normalized_left, normalized_right)

    def evaluate() -> Iterator[dict[str, Any]]:
        """Execute the validated join plan when the returned Rows is consumed."""
        if partition_count is not None:
            yield from spilled_join(
                left_source,
                right_source,
                left_key=left_key,
                right_key=right_key,
                how=how,
                shared_names=shared_names,
                suffix=suffix,
                validate=validate,
                partitions=partition_count,
                tempdir=tempdir,
                limits=spill_limits,
                as_record=_as_record,
                remember_columns=_remember_columns,
                join_targets=_join_targets,
                merge_records=_merge_join_records,
            )
        elif how in {"semi", "anti"}:
            yield from _semi_or_anti_join(
                left_source,
                right_source,
                left_key=left_key,
                right_key=right_key,
                how=how,
                validate=validate,
            )
        elif how in {"inner", "left"}:
            yield from _left_driven_join(
                left_source,
                right_source,
                left_key=left_key,
                right_key=right_key,
                how=how,
                shared_names=shared_names,
                suffix=suffix,
                validate=validate,
            )
        else:
            yield from _right_or_full_join(
                left_source,
                right_source,
                left_key=left_key,
                right_key=right_key,
                how=how,
                shared_names=shared_names,
                suffix=suffix,
                validate=validate,
            )

    return evaluate
