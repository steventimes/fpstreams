"""Compile callables, integer indexes, and dotted strings into row accessors."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from types import FunctionType
from typing import Any, TypeAlias

from ..errors import SelectionError

Selector: TypeAlias = Callable[[Any], Any] | str | int
_DIRECT_FIELD_TOKEN = object()
_DIRECT_FIELD_ATTRIBUTE = "__fpstreams_direct_field_v1__"


def _direct_field(selector: Callable[[Any], Any] | None) -> str | None:
    """Return the direct exact-string field carried by one generated selector."""
    if type(selector) is not FunctionType:
        return None
    metadata = getattr(selector, _DIRECT_FIELD_ATTRIBUTE, None)
    if (
        type(metadata) is tuple
        and len(metadata) == 2
        and metadata[0] is _DIRECT_FIELD_TOKEN
        and type(metadata[1]) is str
    ):
        return metadata[1]
    return None


def _normalize_direct_row_selector(selector: Selector) -> Selector:
    """Lower one inspectable direct RowExpr leaf to its public selector token.

    Relation planning can then reuse the existing field, index, and dotted-path
    machinery without interpreting arbitrary callable bytecode.  Exact type checks
    deliberately leave directly constructed PythonUDF expressions, subclasses, and
    malformed third-party IR untouched.
    """
    from .row import RowExpr
    from .row_ir import Field, Index, Path

    if type(selector) is not RowExpr:
        return selector
    node = selector._node
    if type(node) is Field and type(node.name) is str:
        return node.name
    if type(node) is Index and type(node.index) is int:
        return node.index
    if type(node) is Path and all(type(part) is str for part in node.parts):
        path = ".".join(node.parts)
        if node.selector is None or (type(node.selector) is str and node.selector == path):
            return path
    return selector


def compile_selector(selector: Selector) -> Callable[[Any], Any]:
    """Return a callable implementing the supplied selector.

    Callable selectors are returned unchanged. An integer reads value[selector].
    A dotted string walks each segment as a mapping key when the current value is a
    Mapping and as an attribute otherwise. Lookup and type failures from generated
    accessors are chained into SelectionError with selector context.
    """
    if callable(selector):
        return selector

    if isinstance(selector, int):

        def select_index(value: Any) -> Any:
            """Read the captured integer index and translate lookup/type failures.

            IndexError, KeyError, and TypeError become SelectionError; other exceptions from
            custom __getitem__ implementations propagate unchanged.
            """
            try:
                return value[selector]
            except (IndexError, KeyError, TypeError) as error:
                raise SelectionError(
                    f"Could not resolve index selector {selector!r} on {type(value).__name__}"
                ) from error

        return select_index

    parts = selector.split(".")

    if len(parts) == 1:

        def select_field(value: Any) -> Any:
            """Read one field with a fast exact-dict path and full protocol fallback."""
            try:
                if type(value) is dict:
                    return value[selector]
                return value[selector] if isinstance(value, Mapping) else getattr(value, selector)
            except (AttributeError, KeyError, TypeError) as error:
                raise SelectionError(
                    f"Could not resolve selector {selector!r}; failed at {selector!r}"
                ) from error

        if type(selector) is str:
            setattr(select_field, _DIRECT_FIELD_ATTRIBUTE, (_DIRECT_FIELD_TOKEN, selector))
        return select_field

    def select_path(value: Any) -> Any:
        """Walk the captured dotted path through mapping keys or object attributes.

        The first failing segment is named in a SelectionError chained from AttributeError,
        KeyError, or TypeError.
        """
        current = value
        for part in parts:
            try:
                if type(current) is dict:
                    current = current[part]
                else:
                    current = (
                        current[part] if isinstance(current, Mapping) else getattr(current, part)
                    )
            except (AttributeError, KeyError, TypeError) as error:
                raise SelectionError(
                    f"Could not resolve selector {selector!r}; failed at {part!r}"
                ) from error
        return current

    return select_path
