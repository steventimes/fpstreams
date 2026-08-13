"""Source ownership and repeatability metadata for synchronous plans."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from threading import Lock
from typing import Any, Generic, TypeVar, cast

from ..errors import FlowConsumedError

T = TypeVar("T")
_SAFE_SIZED_TYPES = (list, tuple, range, str, bytes, dict, set, frozenset)
_NATIVE_SOURCE_TYPES = (list, tuple, range)


@dataclass(frozen=True, slots=True)
class SourceCapabilities:
    reiterable: bool
    exact_size: int | None
    ordered: bool = True


class Source(Generic[T]):
    __slots__ = ("_claimed", "_factory", "_lock", "capabilities", "native_data")

    def __init__(
        self,
        factory: Callable[[], Iterator[T]],
        capabilities: SourceCapabilities,
        native_data: Any = None,
    ) -> None:
        self._factory = factory
        self.capabilities = capabilities
        self._claimed = False
        self._lock = Lock()
        self.native_data = native_data

    @classmethod
    def from_iterable(cls, value: Iterable[T]) -> Source[T]:
        exact_size = len(cast(Any, value)) if type(value) in _SAFE_SIZED_TYPES else None
        ordered = not isinstance(value, (set, frozenset))
        if not isinstance(value, Iterator):
            return cls(
                lambda: iter(value),
                SourceCapabilities(
                    reiterable=True,
                    exact_size=exact_size,
                    ordered=ordered,
                ),
                native_data=value if type(value) in _NATIVE_SOURCE_TYPES else None,
            )
        iterator = iter(value)
        return cls(
            lambda: iterator,
            SourceCapabilities(reiterable=False, exact_size=exact_size),
        )

    @classmethod
    def defer(cls, factory: Callable[[], Iterable[T]]) -> Source[T]:
        return cls(
            lambda: iter(factory()),
            SourceCapabilities(reiterable=True, exact_size=None),
        )

    def open(self) -> Iterator[T]:
        if self.capabilities.reiterable:
            return self._factory()
        # Claim one-shot iterators atomically so concurrent terminals cannot double-consume them.
        with self._lock:
            if self._claimed:
                raise FlowConsumedError(
                    "This flow wraps a one-shot source that has already been consumed. "
                    "Use flow.defer(factory) to create a fresh source per evaluation."
                )
            self._claimed = True
        return self._factory()
