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
from .tabular import Rows, SpillLimits, rows

__version__ = "2.0.0"
Stream = Flow
ParallelStream = Flow

__all__ = [
    "Aggregator",
    "AsyncFlow",
    "AsyncStream",
    "BufferLimitError",
    "Collector",
    "Collectors",
    "Downstream",
    "DuplicateKeyError",
    "EmptyFlowError",
    "EmptyInputPolicy",
    "Err",
    "Expr",
    "FExpr",
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
