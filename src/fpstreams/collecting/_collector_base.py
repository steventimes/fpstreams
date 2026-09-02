"""Cycle-free base state machines shared by collectors and aggregators."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

from ..runtime.iterators import closing_iterators

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
    _lifecycle_revision: int = field(init=False, default=0, repr=False, compare=False)

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
        with closing_iterators((iterator,)):
            while not completed:
                try:
                    value = next(iterator)
                except StopIteration:
                    break
                state = self.step(state, value)
                if self.done is not _never_done:
                    completed = self.done(state)
        return self.finish(state)


class _LifecycleSlot:
    """Increment a private revision when an explicit lifecycle slot is replaced.

    Normal assignment remains rejected by the frozen dataclass. ``object.__setattr__`` is
    intentionally observable in Python, though, and is used by compatibility tests and advanced
    callers. Wrapping the generated member descriptor lets optimized consumers notice that rare
    escape hatch with one integer comparison per row instead of re-validating every function.
    """

    __slots__ = ("_revision", "_slot")

    def __init__(self, slot: Any, revision: Any) -> None:
        self._slot = slot
        self._revision = revision

    def __get__(self, instance: object | None, owner: type[object] | None = None) -> Any:
        if instance is None:
            return self
        return self._slot.__get__(instance, owner)

    def __set__(self, instance: object, value: Any) -> None:
        self._slot.__set__(instance, value)
        try:
            revision = self._revision.__get__(instance, type(instance))
        except AttributeError:
            revision = 0
        self._revision.__set__(instance, revision + 1)


# Dataclass initialization and explicit ``object.__setattr__`` both use these descriptors. The
# generated initializer writes the revision's default last, so newly constructed collectors start
# at zero while subsequent lifecycle replacement increments monotonically.
_COLLECTOR_REVISION_SLOT = Collector.__dict__["_lifecycle_revision"]
for _lifecycle_name in ("initializer", "step", "finish", "combine", "done"):
    _lifecycle_slot = Collector.__dict__[_lifecycle_name]
    setattr(
        Collector,
        _lifecycle_name,
        _LifecycleSlot(_lifecycle_slot, _COLLECTOR_REVISION_SLOT),
    )


CollectorItems = tuple[tuple[str, Collector[Any, Any, Any]], ...]


class Aggregator(Collector[Any, Any, Any]):
    """A collector accepted by named aggregate terminals and native fusion."""

    __slots__ = ()


# These classes historically lived in the public modules below.  Retain their
# introspection and pickle identity while keeping the dependency leaf cycle-free.
Collector.__module__ = "fpstreams.collecting.collector"
Aggregator.__module__ = "fpstreams.collecting.aggregation"
