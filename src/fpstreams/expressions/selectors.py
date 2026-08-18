"""Compile callables, integer indexes, and dotted strings into row accessors."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, TypeAlias

from ..errors import SelectionError

Selector: TypeAlias = Callable[[Any], Any] | str | int


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

    def select_path(value: Any) -> Any:
        """Walk the captured dotted path through mapping keys or object attributes.

        The first failing segment is named in a SelectionError chained from AttributeError,
        KeyError, or TypeError.
        """
        current = value
        for part in parts:
            try:
                current = current[part] if isinstance(current, Mapping) else getattr(current, part)
            except (AttributeError, KeyError, TypeError) as error:
                raise SelectionError(
                    f"Could not resolve selector {selector!r}; failed at {part!r}"
                ) from error
        return current

    return select_path
