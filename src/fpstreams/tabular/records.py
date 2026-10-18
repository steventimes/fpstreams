"""Record conversion helpers for tabular pipelines."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import asdict, is_dataclass
from types import MappingProxyType
from typing import Any

from ..errors import DuplicateKeyError, SelectionError


def _as_record(row: Any) -> dict[str, Any]:
    """Copy a supported record-like object into a mutable dictionary."""
    # Exact dictionaries dominate row pipelines. Avoid the comparatively costly
    # ``Mapping`` ABC check while retaining protocol dispatch for subclasses.
    if type(row) is dict:
        return row.copy()
    # ``mappingproxy`` is a final built-in Mapping on every supported CPython. Keep the
    # established ``dict(row)`` conversion (including wrapped custom-Mapping callbacks) while
    # skipping the comparatively expensive ABC instance walk for this exact type.
    if type(row) is MappingProxyType:
        return dict(row)
    continuations = _RECORD_CONTINUATIONS
    if isinstance(row, Mapping):
        return continuations[0](row)
    return continuations[1](row)


def _as_record_after_mapping(row: Any) -> dict[str, Any]:
    """Continue canonical record conversion after ``Mapping`` returned false."""
    if is_dataclass(row) and not isinstance(row, type):
        return asdict(row)
    as_dict = getattr(row, "_asdict", None)
    if callable(as_dict):
        return dict(as_dict())
    attributes = getattr(row, "__dict__", None)
    if attributes is not None:
        return dict(attributes)
    raise SelectionError(f"{type(row).__name__} cannot be represented as a record")


def _mapping_record(row: Any) -> dict[str, Any]:
    """Convert one already-proven Mapping without repeating its ABC hook."""
    return dict(row)


_RECORD_CONTINUATIONS = (_mapping_record, _as_record_after_mapping)


def _record_view(row: Any) -> Mapping[str, Any]:
    """Return a mapping directly, converting other supported records when needed."""
    return row if isinstance(row, Mapping) else _as_record(row)


def _explode_owned_record(
    record: dict[str, Any], values: Any, output_name: str, outer: bool
) -> Iterator[dict[str, Any]]:
    """Expand an already owned snapshot without converting or selecting again."""
    if values is None:
        if outer:
            record[output_name] = None
            yield record
        return
    if isinstance(values, (str, bytes, bytearray, Mapping)) or not isinstance(values, Iterable):
        raise TypeError("explode selector must return a non-string iterable or None")
    emitted = False
    for value in values:
        emitted = True
        expanded = record.copy()
        expanded[output_name] = value
        yield expanded
    if outer and not emitted:
        record[output_name] = None
        yield record


def _unpivot_owned_record(
    record: dict[str, Any], columns: tuple[str, ...], names_to: str, values_to: str
) -> Iterator[dict[str, Any]]:
    """Expand owned fields in caller order, preserving collision and missing-field errors."""
    missing = [name for name in columns if name not in record]
    if missing:
        raise SelectionError(f"missing columns for unpivot: {missing!r}")
    base = {name: value for name, value in record.items() if name not in columns}
    if names_to in base or values_to in base:
        raise DuplicateKeyError("unpivot output names collide with existing columns")
    for name in columns:
        yield {**base, names_to: name, values_to: record[name]}


def _require_unique_names(names: Iterable[str], *, operation: str) -> None:
    """Raise when an operation would create duplicate output field names."""
    seen: set[str] = set()
    for name in names:
        if name in seen:
            raise DuplicateKeyError(f"{operation} creates duplicate output column {name!r}")
        seen.add(name)


def _remember_columns(record: Mapping[str, Any], names: list[str], seen: set[str]) -> None:
    """Append newly observed record fields while preserving discovery order."""
    for name in record:
        if name not in seen:
            seen.add(name)
            names.append(name)
