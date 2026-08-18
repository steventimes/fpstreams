"""Validate record-size limits and neutralize formula-like spreadsheet cells."""

from __future__ import annotations

import operator
from typing import Any

_SPREADSHEET_PREFIXES = frozenset({"=", "+", "-", "@"})


def spreadsheet_safe_cell(value: Any) -> Any:
    """Prefix formula-like strings with an apostrophe and leave other values unchanged.

    Detection ignores leading whitespace and treats strings beginning with ``=``, ``+``,
    ``-``, or ``@`` as potentially executable spreadsheet formulas. The apostrophe is added
    before the original string, preserving its whitespace and content.
    """
    if isinstance(value, str) and value.lstrip()[:1] in _SPREADSHEET_PREFIXES:
        return f"'{value}"
    return value


def validate_max_record_bytes(value: int | None) -> int | None:
    """Return a positive integer record limit, or preserve an unlimited `None` value.

    Objects implementing the integer index protocol are normalized to `int`. Non-integer
    values raise `TypeError`, and zero or negative limits raise `ValueError` immediately so a
    lazy source cannot defer configuration errors until iteration.
    """
    if value is None:
        return None
    try:
        limit = operator.index(value)
    except TypeError:
        raise TypeError("max_record_bytes must be an integer or None") from None
    if limit <= 0:
        raise ValueError("max_record_bytes must be greater than zero")
    return limit
