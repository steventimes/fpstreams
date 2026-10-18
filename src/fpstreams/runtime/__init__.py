"""Internal query-runtime ownership, accounting, and cleanup primitives."""

from .files import FileLimitError, FileManager, TrackedBinaryFile
from .limits import QueryLimits
from .metrics import QueryMetrics
from .query import QueryRuntime
from .report import ExecutionEvent, ExecutionReport, ExecutionResult
from .resources import ResourceRegistry
from .spill import SpillFileRegistry
from .tasks import TaskRole, TaskRuntime

__all__ = [
    "ExecutionEvent",
    "ExecutionReport",
    "ExecutionResult",
    "FileLimitError",
    "FileManager",
    "QueryLimits",
    "QueryMetrics",
    "QueryRuntime",
    "ResourceRegistry",
    "SpillFileRegistry",
    "TaskRole",
    "TaskRuntime",
    "TrackedBinaryFile",
]
