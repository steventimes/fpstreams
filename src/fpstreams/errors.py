"""Exceptions for flow consumption, selection, collection, and execution failures."""

from contextvars import ContextVar


class FlowError(Exception):
    """Base class for errors raised by fpstreams operations."""


class FlowConsumedError(FlowError):
    """Raised when code tries to evaluate an already-consumed one-shot flow."""


class EmptyFlowError(FlowError):
    """Raised when an empty flow cannot satisfy an element-requiring terminal."""


class SelectionError(FlowError, LookupError):
    """Raised when a field, index, path, or expression selector cannot resolve a value."""


_NO_USER_SELECTION_TRACKING = object()
_user_selection_error: ContextVar[object] = ContextVar(
    "fpstreams_user_selection_error", default=_NO_USER_SELECTION_TRACKING
)


class _SelectionTracking:
    def __init__(self) -> None:
        self.depth = 0
        self.leaf: object = None
        self.missing: tuple[SelectionError, int] | None = None
        self.user: SelectionError | None = None


def _record_user_selection_error(error: SelectionError) -> None:
    """Record a user failure only within an active drop-null evaluation."""
    tracking = _user_selection_error.get()
    if isinstance(tracking, _SelectionTracking):
        tracking.user = error


def _record_missing_selection_error(error: SelectionError, origin: object) -> None:
    tracking = _user_selection_error.get()
    if isinstance(tracking, _SelectionTracking) and tracking.leaf is origin:
        tracking.missing = (error, tracking.depth)


class DuplicateKeyError(FlowError, ValueError):
    """Raised when dictionary collection encounters a key with no overwrite policy."""


class NativeUnsupportedError(FlowError):
    """Raised when a plan forced to the native engine contains an unsupported operation."""


class BufferLimitError(FlowError):
    """Raised when a bounded record or buffer exceeds its configured byte or item limit."""


# Lazy execution backends must share the exception class bound by eager selector compilation.
_CANONICAL_SELECTION_ERROR = SelectionError


def _cleanup_error_summary(error: BaseException, *, prefix: str = "cleanup failed:") -> str:
    """Describe cleanup failures even when their exception message cannot be formatted."""
    try:
        message = format(error)
    except BaseException:
        # Diagnostics must not replace the operation failure or stop later closers.
        message = "<exception message unavailable>"
    return f"{prefix} {type(error).__name__}: {message}"
