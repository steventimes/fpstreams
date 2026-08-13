"""Awaitable resolution and asynchronous iterator cleanup helpers."""

from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Iterable
from typing import Any

_MISSING = object()


async def _resolve(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


async def _close(iterator: AsyncIterator[Any]) -> None:
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
