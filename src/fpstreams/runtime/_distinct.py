"""Distinguish unhashable keys from equality failures without extra hash calls."""

from __future__ import annotations

from typing import Any


class HashFailure(Exception):
    """A key's hash protocol raised TypeError; equality errors remain untouched."""


class DistinctKey:
    """Forward set protocols while marking errors raised specifically by hashing.

    Callers keep exact integers and strings as raw keys: their hashes cannot
    raise TypeError. Subclasses and other values still require this wrapper.
    """

    __slots__ = ("value",)

    def __init__(self, value: Any) -> None:
        self.value = value

    def __hash__(self) -> int:
        try:
            return hash(self.value)
        except TypeError as error:
            raise HashFailure from error

    def __eq__(self, other: Any) -> Any:
        if isinstance(other, DistinctKey):
            return self.value is other.value or self.value == other.value
        # Native prefixes retain exact builtin keys, which compare first before
        # reflected equality on a custom suffix value.
        return other is self.value or other == self.value
