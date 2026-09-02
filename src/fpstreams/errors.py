"""Exceptions for flow consumption, selection, collection, and execution failures."""


class FlowError(Exception):
    """Base class for errors raised by fpstreams operations."""


class FlowConsumedError(FlowError):
    """Raised when code tries to evaluate an already-consumed one-shot flow."""


class EmptyFlowError(FlowError):
    """Raised when an empty flow cannot satisfy an element-requiring terminal."""


class SelectionError(FlowError, LookupError):
    """Raised when a field, index, path, or expression selector cannot resolve a value."""


class DuplicateKeyError(FlowError, ValueError):
    """Raised when dictionary collection encounters a key with no overwrite policy."""


class NativeUnsupportedError(FlowError):
    """Raised when a plan forced to the native engine contains an unsupported operation."""


class BufferLimitError(FlowError):
    """Raised when a bounded record or buffer exceeds its configured byte or item limit."""


# Lazy execution backends must share the exception class bound by eager selector compilation.
_CANONICAL_SELECTION_ERROR = SelectionError
