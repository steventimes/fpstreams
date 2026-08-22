"""Cycle-free base state machines shared by collectors and aggregators."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

T = TypeVar("T")
R = TypeVar("R")
S = TypeVar("S")


def _identity(value: Any) -> Any:
    """Return collector state unchanged when no explicit finisher is required."""
    return value


def _never_done(_state: Any) -> bool:
    """Mark a collector as unable to finish before its input is exhausted."""
    return False


@dataclass(frozen=True, slots=True)
class Collector(Generic[T, S, R]):
    """Describe an immutable streaming reduction from input items to a result.

    ``initializer`` creates independent state, ``step`` consumes one item, and
    ``finish`` converts final state to the public result. An optional ``combine``
    merges partial states. ``done`` enables source short-circuiting, and ``native``
    carries planner metadata without changing Python execution.
    """

    initializer: Callable[[], S]
    step: Callable[[S, T], S]
    finish: Callable[[S], R] = _identity
    combine: Callable[[S, S], S] | None = None
    done: Callable[[S], bool] = _never_done
    native: Any | None = None

    def __post_init__(self) -> None:
        """Require callable lifecycle hooks and an optional callable state combiner."""
        for name in ("initializer", "step", "finish", "done"):
            if not callable(getattr(self, name)):
                raise TypeError(f"Collector {name} must be callable")
        if self.combine is not None and not callable(self.combine):
            raise TypeError("Collector combine must be callable or None")

    def __call__(self, values: Iterable[T]) -> R:
        """Initialize, step, and finish over one traversal with deterministic close.

        The completion predicate is evaluated once for each newly produced state.
        Closing in ``finally`` preserves the public early-stop and failure behavior
        for generator sources without importing the higher-level program module.
        """
        state = self.initializer()
        completed = self.done(state) if self.done is not _never_done else False
        iterator = iter(values)
        try:
            while not completed:
                try:
                    value = next(iterator)
                except StopIteration:
                    break
                state = self.step(state, value)
                if self.done is not _never_done:
                    completed = self.done(state)
        finally:
            close = getattr(iterator, "close", None)
            if callable(close):
                close()
        return self.finish(state)


CollectorItems = tuple[tuple[str, Collector[Any, Any, Any]], ...]


class Aggregator(Collector[Any, Any, Any]):
    """A collector accepted by named aggregate terminals and native fusion."""

    __slots__ = ()


# These classes historically lived in the public modules below.  Retain their
# introspection and pickle identity while keeping the dependency leaf cycle-free.
Collector.__module__ = "fpstreams.collecting.collector"
Aggregator.__module__ = "fpstreams.collecting.aggregation"
