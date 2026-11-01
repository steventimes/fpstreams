"""Compile callables, integer indexes, and dotted strings into row accessors."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from types import FunctionType
from typing import Any, NoReturn, TypeAlias, cast

from ..errors import (
    SelectionError,
    _record_missing_selection_error,
    _record_user_selection_error,
    _SelectionTracking,
    _user_selection_error,
)

Selector: TypeAlias = Callable[[Any], Any] | str | int
_DIRECT_FIELD_TOKEN = object()
_DIRECT_FIELD_ATTRIBUTE = "__fpstreams_direct_field_v1__"
_TRACKING_ATTRIBUTE = "__fpstreams_selection_origin_v1__"


def _raise_lookup_error(
    error: Exception, selector: str, part: str, *, attribute_access: bool, origin: object
) -> NoReturn:
    """Preserve user selection errors and translate generated lookup failures."""
    if isinstance(error, SelectionError):
        if attribute_access:
            _record_user_selection_error(error)
        else:
            _record_missing_selection_error(error, origin)
        raise
    failure = SelectionError(f"Could not resolve selector {selector!r}; failed at {part!r}")
    if attribute_access and isinstance(error, TypeError):
        cast(Any, failure)._attribute_type_error = True
    else:
        _record_missing_selection_error(failure, origin)
    raise failure from error


def _call_row_selector(selector: Callable[[Any], Any], row: Any) -> Any:
    tracking = _user_selection_error.get()
    if not isinstance(tracking, _SelectionTracking):
        return selector(row)
    old = tracking.leaf
    tracking.leaf = (
        getattr(selector, _TRACKING_ATTRIBUTE, None) if type(selector) is FunctionType else None
    )
    try:
        return selector(row)
    finally:
        tracking.leaf = old


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
                failure = SelectionError(
                    f"Could not resolve index selector {selector!r} on {type(value).__name__}"
                )
                _record_missing_selection_error(failure, selector)
                raise failure from error
            except SelectionError as error:
                _record_missing_selection_error(error, selector)
                raise

        setattr(select_index, _TRACKING_ATTRIBUTE, selector)
        return select_index

    parts = selector.split(".")

    if len(parts) == 1:

        def select_field(value: Any) -> Any:
            """Read one field with a fast exact-dict path and full protocol fallback."""
            attribute_access = False
            try:
                attribute_access = type(value) is not dict and not isinstance(value, Mapping)
                return getattr(value, selector) if attribute_access else value[selector]
            except (AttributeError, KeyError, TypeError, SelectionError) as error:
                _raise_lookup_error(
                    error, selector, selector, attribute_access=attribute_access, origin=selector
                )

        if type(selector) is str:
            setattr(select_field, _DIRECT_FIELD_ATTRIBUTE, (_DIRECT_FIELD_TOKEN, selector))
        setattr(select_field, _TRACKING_ATTRIBUTE, selector)
        return select_field

    def select_path(value: Any) -> Any:
        """Walk the captured dotted path through mapping keys or object attributes.

        The first failing segment is named in a SelectionError chained from AttributeError,
        KeyError, or TypeError.
        """
        current = value
        for part in parts:
            attribute_access = False
            try:
                if type(current) is dict or isinstance(current, Mapping):
                    current = current[part]
                else:
                    attribute_access = True
                    current = getattr(current, part)
            except (AttributeError, KeyError, TypeError, SelectionError) as error:
                _raise_lookup_error(
                    error, selector, part, attribute_access=attribute_access, origin=selector
                )
        return current

    setattr(select_path, _TRACKING_ATTRIBUTE, selector)
    return select_path
