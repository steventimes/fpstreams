"""Record-oriented pipelines and tabular interoperability."""

from .factory import rows as _rows_factory
from .grouped import GroupedRows
from .rows import Rows
from .spill_limits import SpillLimits

rows = _rows_factory

__all__ = ["GroupedRows", "Rows", "SpillLimits", "rows"]
