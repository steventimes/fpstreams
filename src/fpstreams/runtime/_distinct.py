"""Distinguish unhashable keys from equality failures without extra hash calls."""

from __future__ import annotations

from typing import Any, NoReturn


def reraise_key_type_error(key: Any, message: str) -> NoReturn:
    """Translate provably unhashable keys without repeating hash or equality calls."""
    for base in type.__getattribute__(type(key), "__mro__"):
        namespace = type.__getattribute__(base, "__dict__")
        if "__hash__" in namespace:
            if namespace["__hash__"] is None:
                raise TypeError(message) from None
            break
    raise


class HashFailure(Exception):
    """A key's hash protocol raised TypeError; equality errors remain untouched."""


class DistinctKey:
    """Forward set protocols while marking errors raised specifically by hashing.

    Callers keep exact integers and strings as raw keys: their hashes cannot
    raise TypeError. Subclasses and other values still require this wrapper.
    """

    __slots__ = ("_precomputed_hash", "value")

    def __init__(self, value: Any, precomputed_hash: int | None = None) -> None:
        self.value = value
        self._precomputed_hash = precomputed_hash

    def __hash__(self) -> int:
        if self._precomputed_hash is not None:
            return self._precomputed_hash
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
