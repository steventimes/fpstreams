"""Close synchronous iterators without hiding pipeline failures."""

from __future__ import annotations

from collections.abc import Generator, Iterable, Iterator
from contextlib import contextmanager
from typing import Any


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
            note = f"cleanup failed with {type(error).__name__}: {error}"
            if active_error is not None:
                active_error.add_note(note)
            elif first_cleanup_error is None:
                first_cleanup_error = error
            else:
                first_cleanup_error.add_note(note)

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
