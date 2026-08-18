"""Re-export scalar expression types and item selectors for legacy imports."""

from .expressions.scalar import Expr, FExpr, fitem, item

__all__ = ["Expr", "FExpr", "fitem", "item"]
