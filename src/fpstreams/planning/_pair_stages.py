"""Trusted query-local structure for internal pair callbacks."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from operator import itemgetter
from typing import Any, Literal, cast

PAIR_KEY_SELECTOR = itemgetter(0)


@dataclass(frozen=True, slots=True)
class PairMapDescriptor:
    """Describe and canonically execute one public pair mapping operation."""

    side: Literal["key", "value", "pair"]
    callback: Callable[..., Any]

    def __call__(self, pair: Any) -> tuple[Any, Any]:
        """Retain the established wrapper behavior on non-specialized execution paths."""
        if self.side == "pair":
            return cast(tuple[Any, Any], self.callback(pair[0], pair[1]))
        if self.side == "value":
            return pair[0], self.callback(pair[1])
        return self.callback(pair[0]), pair[1]


@dataclass(frozen=True, slots=True)
class PairFlatMapDescriptor:
    """Describe and canonically execute a two-argument pair flat-map."""

    callback: Callable[..., Iterable[Any]]

    def __call__(self, pair: Any) -> Iterable[Any]:
        """Retain the established adapter behavior outside specialized execution."""
        return self.callback(pair[0], pair[1])


@dataclass(frozen=True, slots=True)
class PairFilterDescriptor:
    """Describe and canonically execute a predicate over selected pair fields."""

    target: Literal["pair", "row", "key", "value"]
    callback: Callable[..., Any]

    def __call__(self, pair: Any) -> Any:
        """Retain the established adapter behavior outside specialized execution."""
        if self.target == "pair":
            return self.callback(pair[0], pair[1])
        if self.target == "row":
            return self.callback(pair)
        if self.target == "key":
            return self.callback(pair[0])
        return self.callback(pair[1])
