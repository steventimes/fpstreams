"""Distinguish unhashable keys from equality failures without extra hash calls."""

from __future__ import annotations

import sys
from collections.abc import Callable
from typing import Any, NoReturn, cast

_TYPE_MRO = type.__dict__["__mro__"]
_TYPE_NAMESPACE = type.__dict__["__dict__"]

dict_get: Callable[[dict[Any, Any], Any, Any], Any] = cast(Any, dict.get)
dict_set: Callable[[dict[Any, Any], Any, Any], None] = cast(Any, dict.__setitem__)

if sys.implementation.name == "cpython" and sys.version_info >= (3, 14):
    import ctypes

    # CPython 3.14+ replaces a hash TypeError in its ordinary dict entry points.
    # These exported known-hash functions use the hash already computed here.
    _hash_key = ctypes.pythonapi["PyObject_Hash"]
    _hash_key.argtypes = (ctypes.py_object,)
    _hash_key.restype = ctypes.c_ssize_t
    _get_known_hash = ctypes.pythonapi["_PyDict_GetItem_KnownHash"]
    _get_known_hash.argtypes = (ctypes.py_object, ctypes.py_object, ctypes.c_ssize_t)
    _get_known_hash.restype = ctypes.c_void_p
    _set_known_hash = ctypes.pythonapi["_PyDict_SetItem_KnownHash"]
    _set_known_hash.argtypes = (
        ctypes.py_object,
        ctypes.py_object,
        ctypes.py_object,
        ctypes.c_ssize_t,
    )
    _set_known_hash.restype = ctypes.c_int

    def _get_owned_table(mapping: dict[Any, Any], key: Any, default: Any) -> Any:
        """Read a terminal-owned table without replacing the caller's hash error.

        The table is private to its synchronous consumer or async task. It owns
        the borrowed value until the cast acquires a Python reference. Only the
        owning consumer mutates this table, outside the read operation.
        PyDLL propagates pending errors, so NULL without an error means missing.
        """
        kind = type(key)
        if (
            kind is int
            or kind is str
            or kind is float
            or kind is bool
            or kind is bytes
            or _hash_is_disabled(kind)
        ):
            return mapping.get(key, default)
        pointer = _get_known_hash(mapping, key, _hash_key(key))
        return ctypes.cast(pointer, ctypes.py_object).value if pointer else default

    def _set_owned_table(mapping: dict[Any, Any], key: Any, value: Any) -> None:
        """Update a private table, computing exactly one hash before insertion."""
        kind = type(key)
        if (
            kind is int
            or kind is str
            or kind is float
            or kind is bool
            or kind is bytes
            or _hash_is_disabled(kind)
        ):
            mapping[key] = value
        else:
            _set_known_hash(mapping, key, value, _hash_key(key))

    dict_get = _get_owned_table
    dict_set = _set_owned_table


def _hash_is_disabled(kind: type[Any]) -> bool:
    """Recognize disabled hash slots without calling user metaclass hooks.

    Ordinary dict operations retain interpreter-specific errors for these keys.
    """
    for base in _TYPE_MRO.__get__(kind):
        namespace = _TYPE_NAMESPACE.__get__(base)
        if "__hash__" in namespace:
            return namespace["__hash__"] is None
    return False


def reraise_key_type_error(key: Any, message: str) -> NoReturn:
    """Translate provably unhashable keys without repeating hash or equality calls."""
    if _hash_is_disabled(type(key)):
        raise TypeError(message) from None
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
