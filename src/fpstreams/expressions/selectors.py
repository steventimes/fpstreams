"""Normalize callable, mapping-key, and attribute selectors."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, TypeAlias

from ..errors import SelectionError

Selector: TypeAlias = Callable[[Any], Any] | str | int


def compile_selector(selector: Selector) -> Callable[[Any], Any]:
    if callable(selector):
        return selector

    if isinstance(selector, int):

        def select_index(value: Any) -> Any:
            try:
                return value[selector]
            except (IndexError, KeyError, TypeError) as error:
                raise SelectionError(
                    f"Could not resolve index selector {selector!r} on {type(value).__name__}"
                ) from error

        return select_index

    parts = selector.split(".")

    def select_path(value: Any) -> Any:
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
