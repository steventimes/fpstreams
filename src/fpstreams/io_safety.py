"""Validate record-size limits and neutralize formula-like spreadsheet cells."""

from __future__ import annotations

import operator
import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Literal

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


@contextmanager
def atomic_output_path(
    target: str | os.PathLike[str], *, if_exists: Literal["replace", "error"]
) -> Iterator[Path]:
    """Publish a closed, same-directory temporary file without following symlinks.

    The caller owns the writer and must close it before leaving the context.
    This guarantees atomic visibility, not durability after a power failure.
    """
    from .runtime.resources import _add_cleanup_failure

    if if_exists not in {"replace", "error"}:
        raise ValueError("if_exists must be replace or error")
    destination = Path(target).absolute()
    if destination.is_symlink():
        raise ValueError("atomic output target must not be a symlink")
    if if_exists == "error" and destination.exists():
        raise FileExistsError(destination)
    descriptor, name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent
    )
    temporary = Path(name)
    active_error: BaseException | None = None
    try:
        os.close(descriptor)
        yield temporary
        if if_exists == "error":
            os.link(temporary, destination)
        else:
            os.replace(temporary, destination)
    except BaseException as error:
        active_error = error
        raise
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except BaseException as error:
            _add_cleanup_failure(active_error, [error])


@contextmanager
def output_path(
    target: str | os.PathLike[str], *, atomic: bool, if_exists: Literal["replace", "error"]
) -> Iterator[str | os.PathLike[str]]:
    """Choose explicit atomic publication or preserve the existing direct path."""
    if atomic:
        with atomic_output_path(target, if_exists=if_exists) as temporary:
            yield temporary
    else:
        if if_exists != "replace":
            raise ValueError("if_exists requires atomic=True unless set to replace")
        yield target
