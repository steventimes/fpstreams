"""Stable public API for fpstreams v2."""

from .collecting import Aggregator, Collector, Collectors, SummaryStatistics, agg
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
from .tabular import Rows, rows

__version__ = "2.0.0a1"
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
    "Err",
    "Expr",
    "FExpr",
    "Flow",
    "FlowConsumedError",
    "FlowError",
    "Gatherer",
    "NativeUnsupportedError",
    "Ok",
    "Option",
    "Pairs",
    "ParallelStream",
    "Result",
    "RowExpr",
    "Rows",
    "SelectionError",
    "Stream",
    "SummaryStatistics",
    "__version__",
    "aflow",
    "agg",
    "coalesce",
    "col",
    "curry",
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
