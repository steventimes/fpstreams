"""Checked, lazy cursors for explicitly sorted Python inputs."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from typing import Any, cast

from ..collecting.aggregation import (
    AggregationItems,
    finish_aggregations,
    initialize_aggregations,
    step_aggregations,
)
from ..planning.source import Source
from ..runtime.iterators import close_iterators, closing_iterators


class _End:
    __slots__ = ()


_EOF = _End()
_UNSET = object()


class _KeyShape:
    """Share an exact builtin key shape between input cursors."""

    def __init__(self) -> None:
        self._signature: type[Any] | tuple[type[Any], ...] | None = None

    def check(self, key: Any, *, side: str, index: int) -> None:
        key_type = type(key)
        signature: type[Any] | tuple[type[Any], ...]
        if key_type is int or key_type is str or key_type is bytes:
            signature = key_type
        else:
            parts = key if key_type is tuple else ()
            if not parts or any(
                not (type(part) is int or type(part) is str or type(part) is bytes)
                for part in parts
            ):
                raise TypeError(
                    f"{side} sorted key at index {index} must be an exact int, str, bytes "
                    "or nonempty flat tuple of these types"
                )
            signature = tuple(type(part) for part in parts)
        if self._signature is None:
            self._signature = signature
        elif self._signature != signature:
            raise TypeError(f"{side} sorted key changes type or shape at index {index}")


class _SortedCursor(Iterator[Any]):
    """Own an unopened source and at most one checked lookahead record."""

    def __init__(
        self,
        source: Source[Any],
        selector: Callable[[Any], Any],
        *,
        side: str,
        shape: _KeyShape | None = None,
        snapshot: Callable[[Any], Any] | None = None,
        unique: bool = False,
    ) -> None:
        self._source = source
        self._selector = selector
        self._side = side
        self._shape = shape if shape is not None else _KeyShape()
        self._snapshot = snapshot
        self._unique = unique
        self._iterator: Iterator[Any] | None = None
        self._cached: Any = _UNSET
        self._previous: Any = _UNSET
        self._index = 0
        self._closed = False

    def open(self) -> Iterator[Any] | None:
        """Open without pulling, allowing a pair to check iterator identity."""
        if not self._closed and self._iterator is None:
            self._iterator = self._source.open()
        return self._iterator

    def peek(self) -> tuple[Any, Any] | _End:
        if self._closed:
            return _EOF
        if self._cached is not _UNSET:
            return cast("tuple[Any, Any] | _End", self._cached)
        try:
            iterator = self.open()
            assert iterator is not None
            try:
                value = next(iterator)
            except StopIteration:
                self._cached = _EOF
                return _EOF
            # Record joins take their snapshot before invoking the key on the original row.
            snapshot = value if self._snapshot is None else self._snapshot(value)
            key = self._selector(value)
            self._shape.check(key, side=self._side, index=self._index)
            if self._previous is not _UNSET and key < self._previous:
                raise ValueError(f"{self._side} input is not sorted at index {self._index}")
            if self._unique and self._previous is not _UNSET and key == self._previous:
                raise ValueError(f"sorted join requires unique {self._side} keys")
            self._previous = key
            self._index += 1
            entry = (key, snapshot)
            self._cached = entry
            return entry
        except BaseException as error:
            self.close(active_error=error)
            raise

    def pop(self) -> tuple[Any, Any] | _End:
        item = self.peek()
        if item is not _EOF:
            self._cached = _UNSET
        return item

    def close(self, *, active_error: BaseException | None = None) -> None:
        if self._closed:
            return
        self._closed = True
        iterator, self._iterator = self._iterator, None
        self._cached = _EOF
        self._previous = _UNSET
        if iterator is not None:
            close_iterators((iterator,), active_error=active_error)

    def __iter__(self) -> _SortedCursor:
        return self

    def __next__(self) -> Any:
        item = self.pop()
        if isinstance(item, _End):
            raise StopIteration
        return item[1]


def aggregate_sorted_rows(
    values: Iterable[Any] | Source[Any],
    keys: tuple[tuple[str, Callable[[Any], Any]], ...],
    aggregations: AggregationItems,
) -> Iterator[dict[str, Any]]:
    """Finish adjacent groups without buffering their rows or future groups."""
    source = values if isinstance(values, Source) else Source.from_iterable(values)

    def select_key(row: Any) -> Any:
        selected = tuple(selector(row) for _name, selector in keys)
        return selected[0] if len(selected) == 1 else selected

    cursor = _SortedCursor(source, select_key, side="input")
    with closing_iterators((cursor,)):
        while True:
            head = cursor.peek()
            if isinstance(head, _End):
                return
            group_key = head[0]
            del head
            states = initialize_aggregations(aggregations)
            while True:
                head = cursor.peek()
                if isinstance(head, _End) or head[0] != group_key:
                    del head
                    break
                cursor.pop()
                step_aggregations(states, aggregations, head[1])
                del head
            selected_keys = (group_key,) if len(keys) == 1 else group_key
            result = {name: key for (name, _selector), key in zip(keys, selected_keys, strict=True)}
            result.update(finish_aggregations(states, aggregations))
            del states, group_key, selected_keys
            yield result
            del result


def _open_pair(left: _SortedCursor, right: _SortedCursor) -> None:
    """Open left then right, rejecting aliasing before either iterator is pulled."""
    left_iterator = left.open()
    if left._source is right._source and not left._source.capabilities.reiterable:
        raise ValueError("sorted inputs wrap the same one-shot source")
    right_iterator = right.open()
    if left_iterator is right_iterator:
        # The left cursor owns this one resource; the right cursor must not close it again.
        right._iterator = None
        right._closed = True
        raise ValueError("sorted inputs opened the same iterator")


def merge_sorted_values(
    left: Source[Any],
    right: Source[Any],
    key: Callable[[Any], Any],
    *,
    shared_one_shot: bool = False,
) -> Iterator[Any]:
    """Merge checked lookaheads, preserving left ties and original item identity."""
    shape = _KeyShape()
    left_cursor = _SortedCursor(left, key, side="left", shape=shape)
    right_cursor = _SortedCursor(right, key, side="right", shape=shape)
    with closing_iterators((left_cursor, right_cursor)):
        _open_pair(left_cursor, right_cursor)
        if shared_one_shot:
            raise ValueError("sorted branches share the same one-shot source")
        while True:
            left_head, right_head = left_cursor.peek(), right_cursor.peek()
            if isinstance(left_head, _End) and isinstance(right_head, _End):
                return
            if isinstance(right_head, _End) or (
                not isinstance(left_head, _End) and left_head[0] <= right_head[0]
            ):
                assert not isinstance(left_head, _End)
                value = left_head[1]
                left_cursor.pop()
            else:
                value = right_head[1]
                right_cursor.pop()
            del left_head, right_head
            yield value
            del value


def _read_right_group(cursor: _SortedCursor, *, unique: bool, limit: int) -> tuple[Any, list[Any]]:
    """Read one adjacent right group, including its boundary lookahead."""
    head = cursor.peek()
    assert not isinstance(head, _End)
    key = head[0]
    del head
    from ..errors import BufferLimitError

    records: list[Any] = []
    while True:
        head = cursor.peek()
        if isinstance(head, _End) or head[0] != key:
            break
        if unique and records:
            raise ValueError("sorted join requires unique right keys")
        if len(records) >= limit:
            raise BufferLimitError("sorted join right group exceeds max_right_group_rows")
        cursor.pop()
        records.append(head[1])
        del head
    return key, records


def join_sorted_rows(
    left: Iterable[Any] | Source[Any],
    right: Iterable[Any] | Source[Any],
    *,
    left_key: Callable[[Any], Any],
    right_key: Callable[[Any], Any],
    shared_names: set[str],
    how: str = "inner",
    suffix: str = "_right",
    validate: str = "m:1",
    max_right_group_rows: int = 100_000,
    max_matches_per_left: int = 100_000,
    max_output_rows: int = 1_000_000,
    shared_one_shot: bool = False,
) -> Iterator[dict[str, Any]]:
    """Stream sorted record joins with bounded current-group and output state."""
    from ..tabular.records import _as_record

    if how not in {"inner", "left", "semi", "anti"} or validate not in {"m:m", "m:1", "1:m", "1:1"}:
        raise ValueError("invalid sorted join mode or cardinality")
    left_source = left if isinstance(left, Source) else Source.from_iterable(left)
    right_source = right if isinstance(right, Source) else Source.from_iterable(right)
    right_names: tuple[str, ...] | None = None

    def snapshot_right(row: Any) -> dict[str, Any]:
        nonlocal right_names
        record = _as_record(row)
        if right_names is None:
            right_names = tuple(record)
        elif any(name not in right_names for name in record):
            raise ValueError("sorted join right record introduces a new column")
        return record

    shape = _KeyShape()
    left_cursor = _SortedCursor(
        left_source,
        left_key,
        side="left",
        shape=shape,
        snapshot=_as_record,
        unique=validate in {"1:m", "1:1"},
    )
    right_cursor = _SortedCursor(
        right_source,
        right_key,
        side="right",
        shape=shape,
        snapshot=snapshot_right if how in {"inner", "left"} else _as_record,
    )
    group_key: Any = _UNSET
    right_group: list[Any] = []
    output_budget = _JoinOutputBudget(max_matches_per_left, max_output_rows)
    with closing_iterators((left_cursor, right_cursor)):
        _open_pair(left_cursor, right_cursor)
        if shared_one_shot:
            raise ValueError("sorted branches share the same one-shot source")
        while True:
            left_head = left_cursor.peek()
            if isinstance(left_head, _End):
                return
            key, row = left_head
            del left_head
            if group_key is not _UNSET and group_key < key:
                right_group.clear()
                group_key = _UNSET
            right_head = right_cursor.peek()
            while not isinstance(right_head, _End) and right_head[0] < key:
                _discarded_key, discarded = _read_right_group(
                    right_cursor, unique=validate in {"m:1", "1:1"}, limit=max_right_group_rows
                )
                discarded.clear()
                del discarded, _discarded_key, right_head
                right_head = right_cursor.peek()
            if group_key is _UNSET and not isinstance(right_head, _End) and right_head[0] == key:
                group_key, right_group = _read_right_group(
                    right_cursor, unique=validate in {"m:1", "1:1"}, limit=max_right_group_rows
                )
            del right_head
            matched = group_key is not _UNSET and group_key == key
            left_cursor.pop()
            yield from output_budget.outputs(
                row,
                right_group,
                matched,
                how=how,
                right_names=right_names or (),
                shared_names=shared_names,
                suffix=suffix,
            )
            del row, key


def _sorted_join_outputs(
    row: dict[str, Any],
    right_group: list[Any],
    matched: bool,
    *,
    how: str,
    right_names: tuple[str, ...],
    shared_names: set[str],
    suffix: str,
) -> Iterator[dict[str, Any]]:
    from ..tabular.join import _join_targets, _merge_join_records

    if how == "semi":
        if matched:
            yield row
    elif how == "anti":
        if not matched:
            yield row
    else:
        targets = _join_targets(row, right_names, shared_names=shared_names, suffix=suffix)
        if matched:
            for right_row in right_group:
                yield _merge_join_records(row, right_row, targets, shared_names)
        elif how == "left":
            result = dict(row)
            for name, target in targets:
                if name not in shared_names:
                    result[target] = None
            yield result


class _JoinOutputBudget:
    """Count actual outputs across left rows without retaining them."""

    def __init__(self, max_matches: int, max_outputs: int) -> None:
        self._max_matches = max_matches
        self._max_outputs = max_outputs
        self._outputs = 0

    def outputs(
        self,
        row: dict[str, Any],
        right_group: list[Any],
        matched: bool,
        *,
        how: str,
        right_names: tuple[str, ...],
        shared_names: set[str],
        suffix: str,
    ) -> Iterator[dict[str, Any]]:
        from ..errors import BufferLimitError

        matches = len(right_group) if how in {"inner", "left"} else int(matched and how == "semi")
        if matches > self._max_matches:
            raise BufferLimitError("sorted join matches exceed max_matches_per_left")
        for result in _sorted_join_outputs(
            row,
            right_group,
            matched,
            how=how,
            right_names=right_names,
            shared_names=shared_names,
            suffix=suffix,
        ):
            if self._outputs >= self._max_outputs:
                raise BufferLimitError("sorted join output exceeds max_output_rows")
            self._outputs += 1
            yield result
            del result
