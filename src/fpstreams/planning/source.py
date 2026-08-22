"""Own synchronous source openers and enforce their replayability contracts."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from threading import Lock
from typing import Any, Generic, TypeVar, cast

from ..errors import FlowConsumedError
from .semantics import StreamFacts, facts_from_capabilities

T = TypeVar("T")
_SAFE_SIZED_TYPES = (list, tuple, range, str, bytes, dict, set, frozenset)
_NATIVE_SOURCE_TYPES = (list, tuple, range)


@dataclass(frozen=True, slots=True)
class SourceCapabilities:
    """Record whether a source is reiterable, its safe exact size, and its ordering."""

    reiterable: bool
    exact_size: int | None
    ordered: bool = True


class Source(Generic[T]):
    """Open a synchronous source while atomically enforcing one-shot consumption.

    The source also retains conservative semantic facts and, for supported containers, the
    original data needed to cross into a native execution engine without first opening Python
    iteration.
    """

    __slots__ = (
        "_claimed",
        "_factory",
        "_lock",
        "capabilities",
        "facts",
        "native_data",
    )

    def __init__(
        self,
        factory: Callable[[], Iterator[T]],
        capabilities: SourceCapabilities,
        native_data: Any = None,
        *,
        facts: StreamFacts | None = None,
    ) -> None:
        """Store the opener and derive semantic facts when callers do not provide them."""
        self._factory = factory
        self.capabilities = capabilities
        self._claimed = False
        self._lock = Lock()
        self.native_data = native_data
        self.facts = facts or facts_from_capabilities(
            reiterable=capabilities.reiterable,
            exact_size=capabilities.exact_size,
            ordered=capabilities.ordered,
        )

    @classmethod
    def from_iterable(cls, value: Iterable[T]) -> Source[T]:
        """Describe an iterable, treating iterator instances as atomically claimed one-shots.

        Exact size is trusted only for built-in containers with side-effect-free ``len``;
        lists, tuples, and ranges are additionally retained as native-engine inputs.
        """
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
    def defer(
        cls, factory: Callable[[], Iterable[T]], *, facts: StreamFacts | None = None
    ) -> Source[T]:
        """Create a reopenable source whose factory is invoked for each evaluation."""
        return cls(
            lambda: iter(factory()),
            SourceCapabilities(reiterable=True, exact_size=None),
            facts=facts
            or facts_from_capabilities(
                reiterable=True,
                exact_size=None,
                ordered=True,
                reopenable=True,
            ),
        )

    def open(self) -> Iterator[T]:
        """Claim the source when necessary, then create its Python iterator."""
        from ..runtime.failpoints import hit

        self._claim()
        iterator = self._factory()
        try:
            hit("source.open.after")
        except BaseException:
            close = getattr(iterator, "close", None)
            if callable(close):
                close()
            raise
        return iterator

    def _claim(self) -> None:
        """Atomically reject a second evaluation of a non-reiterable source."""
        if self.capabilities.reiterable:
            return
        with self._lock:
            if self._claimed:
                raise FlowConsumedError(
                    "This flow wraps a one-shot source that has already been consumed. "
                    "Use flow.defer(factory) to create a fresh source per evaluation."
                )
            self._claimed = True

    def open_native(self, expected_type: type[Any]) -> Any:
        """Claim and return retained native data after validating its expected container type."""
        if not isinstance(self.native_data, expected_type):
            raise TypeError(f"source does not provide {expected_type.__name__} native data")
        self._claim()
        return self.native_data
