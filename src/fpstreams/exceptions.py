from .errors import EmptyFlowError, FlowError

StreamError = FlowError
StreamEmptyError = EmptyFlowError


class ParallelExecutionError(FlowError):
    pass


class DependencyMissingError(FlowError, ImportError):
    pass
