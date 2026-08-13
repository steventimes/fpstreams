"""Record-oriented pipelines and tabular interoperability."""

from .factory import rows
from .grouped import GroupedRows
from .rows import Rows

__all__ = ["GroupedRows", "Rows", "rows"]
