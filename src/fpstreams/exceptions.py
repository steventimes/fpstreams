class StreamError(Exception):
    """Base class for exceptions in fpstreams."""
    pass

class StreamEmptyError(StreamError):
    """Raised when an operation (like reduce) is performed on an empty stream without a default."""
    pass

class ParallelExecutionError(StreamError):
    """Raised when a parallel worker fails unexpectedly."""
    pass

class DependencyMissingError(StreamError, ImportError):
    """Raised when a feature (like to_df) is called without the required package installed."""
    pass