"""Re-export row-expression builders for column-oriented compatibility imports."""

from fpstreams.expressions.row import RowExpr, coalesce, col, lit, when

__all__ = ["RowExpr", "coalesce", "col", "lit", "when"]
