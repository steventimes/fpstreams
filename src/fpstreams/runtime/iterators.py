"""Close synchronous iterators without hiding pipeline failures."""

from __future__ import annotations

import itertools
from collections.abc import Generator, Iterable, Iterator
from contextlib import contextmanager
from typing import Any
from weakref import ReferenceType, ref

from ..errors import _cleanup_error_summary

_NON_CLOSING_BUILTIN_ITERATORS: tuple[type[Any], ...] = (
    type(iter([])),
    type(iter(())),
    type(iter(range(0))),
    type(iter("")),
    type(iter(b"")),
    type(iter(bytearray())),
    type(iter({})),
    type(iter(set())),
    enumerate,
    filter,
    map,
    zip,
)
_NON_CLOSING_ITERTOOLS_TYPES = frozenset(
    value
    for value in vars(itertools).values()
    if isinstance(value, type)
    and value.__module__ == "itertools"
    and not hasattr(value, "close")
    and not hasattr(value, "aclose")
)
_NON_CLOSING_TYPE_IDENTITIES = {
    id(kind): kind for kind in (*_NON_CLOSING_BUILTIN_ITERATORS, *_NON_CLOSING_ITERTOOLS_TYPES)
}


class _IteratorIdentities:
    """Track owned cursors without retaining those that support weak references."""

    def __init__(self) -> None:
        self._values: dict[int, tuple[ReferenceType[Any] | None, object | None]] = {}

    def contains(self, value: object) -> bool:
        entry = self._values.get(id(value))
        if entry is None:
            return False
        reference, strong = entry
        return (reference() if reference is not None else strong) is value

    def add(self, value: object) -> None:
        # Exact standard types have no close hook; subclasses can add one.
        value_type = type(value)
        if _NON_CLOSING_TYPE_IDENTITIES.get(id(value_type)) is value_type:
            return
        try:
            self._values[id(value)] = (ref(value), None)
        except TypeError:
            self._values[id(value)] = (None, value)

    def clear(self) -> None:
        self._values.clear()


def close_iterator(iterator: Iterator[Any]) -> None:
    """Call an iterator's optional synchronous close hook."""
    close = getattr(iterator, "close", None)
    if callable(close):
        close()


def close_iterators(
    iterators: Iterable[Iterator[Any]],
    *,
    active_error: BaseException | None = None,
) -> None:
    """Close every iterator while preserving an active pipeline error."""
    if isinstance(active_error, GeneratorExit):
        active_error = None
    first_cleanup_error: BaseException | None = None

    for iterator in iterators:
        try:
            close_iterator(iterator)
        except BaseException as error:
            note = _cleanup_error_summary(error, prefix="cleanup failed with")
            nested_notes = tuple(getattr(error, "__notes__", ()) or ())
            target = active_error if active_error is not None else first_cleanup_error
            if target is None:
                first_cleanup_error = error
            else:
                target.add_note(note)
                if target is not error:
                    for nested_note in nested_notes:
                        target.add_note(nested_note)

    if first_cleanup_error is not None:
        raise first_cleanup_error


@contextmanager
def closing_iterators(iterators: Iterable[Iterator[Any]]) -> Generator[None, None, None]:
    """Own iterators for one block and distinguish its error from ambient handlers."""
    try:
        yield
    except BaseException as error:
        close_iterators(tuple(iterators), active_error=error)
        raise
    else:
        close_iterators(tuple(iterators))


__all__ = ["close_iterator", "close_iterators", "closing_iterators"]
