"""Small opt-in safety helpers shared by stream and tabular I/O."""

from __future__ import annotations

import operator
from typing import Any

_SPREADSHEET_PREFIXES = frozenset({"=", "+", "-", "@"})


def spreadsheet_safe_cell(value: Any) -> Any:
    """Neutralize strings that spreadsheet programs may interpret as formulas."""
    if isinstance(value, str) and value.lstrip()[:1] in _SPREADSHEET_PREFIXES:
        return f"'{value}"
    return value


def validate_max_record_bytes(value: int | None) -> int | None:
    """Validate an optional positive byte bound before constructing a lazy source."""
    if value is None:
        return None
    try:
        limit = operator.index(value)
    except TypeError:
        raise TypeError("max_record_bytes must be an integer or None") from None
    if limit <= 0:
        raise ValueError("max_record_bytes must be greater than zero")
    return limit
