from .errors import EmptyFlowError, FlowError

StreamError = FlowError
StreamEmptyError = EmptyFlowError


class ParallelExecutionError(FlowError):
    """Signal that a parallel execution strategy could not complete the flow."""

    pass


class DependencyMissingError(FlowError, ImportError):
    """Report that an operation requires an optional dependency that is unavailable."""

    pass
