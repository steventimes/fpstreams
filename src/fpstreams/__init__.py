"""Expose the fpstreams v2 flow, expression, collection, and value-type APIs."""

from .collecting import (
    Aggregator,
    Collector,
    Collectors,
    EmptyInputPolicy,
    LawProvenance,
    Reducer,
    ReducerAggregator,
    ReducerLawError,
    ReducerLaws,
    ReductionExplanation,
    SummaryStatistics,
    agg,
    explain_reduction,
)
from .errors import (
    BufferLimitError,
    DuplicateKeyError,
    EmptyFlowError,
    FlowConsumedError,
    FlowError,
    NativeUnsupportedError,
    SelectionError,
)
from .expressions import Expr, FExpr, RowExpr, coalesce, col, fitem, item, lit, when
from .functional import curry, pipe, retry
from .primitives import Err, Ok, Option, Result
from .runtime.executor import BoundedExecutor
from .runtime.report import ExecutionEvent, ExecutionReport, ExecutionResult
from .storage.checkpoint import FileCheckpoint
from .streams import (
    AsyncFlow,
    AsyncStream,
    Downstream,
    Flow,
    Gatherer,
    Pairs,
    aflow,
    flow,
    pairs,
)
from .tabular import Rows, SpillLimits
from .tabular.factory import rows as rows

__version__ = "2.4.0"
Stream = Flow
ParallelStream = Flow

__all__ = [
    "Aggregator",
    "AsyncFlow",
    "AsyncStream",
    "BoundedExecutor",
    "BufferLimitError",
    "Collector",
    "Collectors",
    "Downstream",
    "DuplicateKeyError",
    "EmptyFlowError",
    "EmptyInputPolicy",
    "Err",
    "ExecutionEvent",
    "ExecutionReport",
    "ExecutionResult",
    "Expr",
    "FExpr",
    "FileCheckpoint",
    "Flow",
    "FlowConsumedError",
    "FlowError",
    "Gatherer",
    "LawProvenance",
    "NativeUnsupportedError",
    "Ok",
    "Option",
    "Pairs",
    "ParallelStream",
    "Reducer",
    "ReducerAggregator",
    "ReducerLawError",
    "ReducerLaws",
    "ReductionExplanation",
    "Result",
    "RowExpr",
    "Rows",
    "SelectionError",
    "SpillLimits",
    "Stream",
    "SummaryStatistics",
    "__version__",
    "aflow",
    "agg",
    "coalesce",
    "col",
    "curry",
    "explain_reduction",
    "fitem",
    "flow",
    "item",
    "lit",
    "pairs",
    "pipe",
    "retry",
    "rows",
    "when",
]
