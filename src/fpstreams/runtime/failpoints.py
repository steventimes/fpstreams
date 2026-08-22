"""Scoped internal failpoints with task and thread isolation."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar

_ACTIVE: ContextVar[tuple[tuple[str, BaseException], ...]] = ContextVar(
    "fpstreams_failpoints", default=()
)


@contextmanager
def failpoint(name: str, error: BaseException) -> Generator[None, None, None]:
    """Temporarily raise ``error`` when the named transition calls :func:`hit`."""
    if not name:
        raise ValueError("failpoint name cannot be empty")
    token = _ACTIVE.set((*_ACTIVE.get(), (name, error)))
    try:
        yield
    finally:
        _ACTIVE.reset(token)


def hit(name: str) -> None:
    """Raise the innermost active error for ``name``, or remain a no-op by default."""
    for active_name, error in reversed(_ACTIVE.get()):
        if active_name == name:
            raise error


def has_active_failpoints() -> bool:
    """Return whether this task or thread requires instrumented execution boundaries."""
    return bool(_ACTIVE.get())
