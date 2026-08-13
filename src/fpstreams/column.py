"""Compatibility facade for row expressions."""

from .expressions.row import RowExpr, coalesce, col, lit, when

__all__ = ["RowExpr", "coalesce", "col", "lit", "when"]
