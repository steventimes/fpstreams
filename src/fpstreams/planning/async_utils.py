"""Resolve optionally awaitable results and close owned asynchronous iterators safely."""

from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Iterable
from typing import Any

_MISSING = object()


async def _resolve(value: Any) -> Any:
    """Await an awaitable value, or return a synchronous value unchanged."""
    return await value if inspect.isawaitable(value) else value


async def _close(iterator: AsyncIterator[Any]) -> None:
    """Call and, when necessary, await an iterator's optional ``aclose`` method."""
    close = getattr(iterator, "aclose", None)
    if not callable(close):
        return
    result = close()
    if inspect.isawaitable(result):
        await result


async def close_async_iterators(
    iterators: Iterable[AsyncIterator[Any]],
    *,
    active_error: BaseException | None = None,
) -> None:
    """Close every iterator without letting one cleanup failure skip the remaining iterators.

    Cleanup failures are attached as notes to ``active_error``. Without an active error, the
    first cleanup failure is raised after later failures have been added to it as notes.
    """
    first_cleanup_error: BaseException | None = None
    for iterator in iterators:
        try:
            await _close(iterator)
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
