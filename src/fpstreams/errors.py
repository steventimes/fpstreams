"""Exception hierarchy shared by every fpstreams domain."""


class FlowError(Exception):
    """Base class for fpstreams pipeline failures."""


class FlowConsumedError(FlowError):
    """Raised when a one-shot source is evaluated more than once."""


class EmptyFlowError(FlowError):
    """Raised when a terminal requires an element from an empty flow."""


class SelectionError(FlowError, LookupError):
    """Raised when a selector cannot resolve a value."""


class DuplicateKeyError(FlowError, ValueError):
    """Raised when collecting pairs would overwrite an existing key."""


class NativeUnsupportedError(FlowError):
    """Raised when forced native execution cannot execute a plan."""


class BufferLimitError(FlowError):
    """Raised when a bounded buffering operation exceeds its configured limit."""
