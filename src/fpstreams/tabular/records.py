"""Record conversion helpers for tabular pipelines."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
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
