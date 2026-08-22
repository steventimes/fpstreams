"""Fixed-layout state programs for existing collectors."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ._collector_base import Collector, CollectorItems, _never_done


@dataclass(frozen=True, slots=True)
class CollectorLayout:
    """Immutable collector names and instances in observable declaration order."""

    names: tuple[str, ...]
    collectors: tuple[Collector[Any, Any, Any], ...]


@dataclass(slots=True)
class CollectorState:
    """Mutable indexed values and completion flags for one program invocation."""

    values: list[Any]
    completed: list[bool]


@dataclass(frozen=True, slots=True)
class CollectorProgram:
    """Reusable compiled collector layout with array-indexed execution."""

    layout: CollectorLayout
    single: bool

    def initialize(self) -> CollectorState:
        """Create independent indexed states and cache their initial completion flags."""
        values = [collector.initializer() for collector in self.layout.collectors]
        # Cache completion with each newly created state. The hot step path can then
        # trust this bit instead of calling an arbitrary predicate twice per input.
        completed = [
            collector.done(value) if collector.done is not _never_done else False
            for collector, value in zip(self.layout.collectors, values, strict=True)
        ]
        return CollectorState(values, completed)

    def step(self, state: CollectorState, value: Any) -> None:
        """Offer one value to unfinished collectors in declaration order."""
        if self.single:
            collector = self.layout.collectors[0]
            if collector.done is not _never_done and state.completed[0]:
                return
            state.values[0] = collector.step(state.values[0], value)
            if collector.done is not _never_done:
                state.completed[0] = collector.done(state.values[0])
            return

        for index, collector in enumerate(self.layout.collectors):
            if collector.done is not _never_done and state.completed[index]:
                continue
            state.values[index] = collector.step(state.values[index], value)
            if collector.done is not _never_done:
                state.completed[index] = collector.done(state.values[index])

    def done(self, state: CollectorState) -> bool:
        """Return whether each collector has completed at its current indexed state."""
        return all(state.completed)

    def finish(self, state: CollectorState) -> dict[str, Any]:
        """Finish every state in fixed declaration order."""
        return {
            name: collector.finish(state.values[index])
            for index, (name, collector) in enumerate(
                zip(self.layout.names, self.layout.collectors, strict=True)
            )
        }

    def merge(self, left: CollectorState, right: CollectorState) -> None:
        """Validate all combiners first, then replace left values atomically."""
        merged: list[Any] = []
        for name, collector, left_value, right_value in zip(
            self.layout.names,
            self.layout.collectors,
            left.values,
            right.values,
            strict=True,
        ):
            if collector.combine is None:
                raise ValueError(f"collector {name!r} is not mergeable")
            merged.append(collector.combine(left_value, right_value))
        left.values[:] = merged
        left.completed[:] = [
            collector.done(value) if collector.done is not _never_done else False
            for collector, value in zip(self.layout.collectors, merged, strict=True)
        ]


def compile_collectors(items: CollectorItems) -> CollectorProgram:
    """Freeze already validated collector items into an immutable array layout."""
    return CollectorProgram(
        CollectorLayout(
            tuple(name for name, _collector in items),
            tuple(collector for _, collector in items),
        ),
        len(items) == 1,
    )


def run_collector_program(values: Any, program: CollectorProgram) -> dict[str, Any]:
    """Run a program in one pass and close an iterator at its existing semantic boundary."""
    state = program.initialize()
    iterator = iter(values)
    try:
        while not program.done(state):
            try:
                value = next(iterator)
            except StopIteration:
                break
            program.step(state, value)
    finally:
        close = getattr(iterator, "close", None)
        if callable(close):
            close()
    return program.finish(state)
