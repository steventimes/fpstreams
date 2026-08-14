"""Record-oriented pipelines and tabular interoperability."""

from .factory import rows
from .grouped import GroupedRows
from .rows import Rows
from .spill_limits import SpillLimits

__all__ = ["GroupedRows", "Rows", "SpillLimits", "rows"]
