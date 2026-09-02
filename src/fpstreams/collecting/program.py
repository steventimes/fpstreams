"""Fixed-layout state programs for existing collectors."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from ._collector_base import Collector, CollectorItems, _never_done

_COLLECTOR_PROGRAM_TRUST_TOKEN = object()


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
    _fast_path_token: object | None = field(default=None, repr=False, compare=False)
    _fast_path_provenance: tuple[object, ...] | None = field(
        default=None,
        repr=False,
        compare=False,
    )

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


_CANONICAL_COLLECTOR_PROGRAM = CollectorProgram
_CANONICAL_COLLECTOR_LAYOUT = CollectorLayout


def collector_program_fast_path_is_live(program: object) -> bool:
    """Accept only an internally compiled program whose frozen layout still matches."""
    if type(program) is not _CANONICAL_COLLECTOR_PROGRAM:
        return False
    provenance = program._fast_path_provenance
    if (
        program._fast_path_token is not _COLLECTOR_PROGRAM_TRUST_TOKEN
        or type(provenance) is not tuple
        or len(provenance) != 4
    ):
        return False
    trusted_layout, trusted_names, trusted_collectors, trusted_single = provenance
    return not (
        program.layout is not trusted_layout
        or program.single is not trusted_single
        or type(program.single) is not bool
        or type(trusted_layout) is not _CANONICAL_COLLECTOR_LAYOUT
        or trusted_layout.names is not trusted_names
        or trusted_layout.collectors is not trusted_collectors
        or type(trusted_names) is not tuple
        or type(trusted_collectors) is not tuple
        or program.single is not (len(trusted_collectors) == 1)
    )


def compile_collectors(items: CollectorItems) -> CollectorProgram:
    """Freeze already validated collector items into an immutable array layout."""
    layout = CollectorLayout(
        tuple(name for name, _collector in items),
        tuple(collector for _, collector in items),
    )
    single = len(items) == 1
    return CollectorProgram(
        layout,
        single,
        _COLLECTOR_PROGRAM_TRUST_TOKEN,
        (layout, layout.names, layout.collectors, single),
    )


def collector_lifecycle_revisions(program: CollectorProgram) -> tuple[int, ...]:
    """Snapshot the observable lifecycle revision of every retained collector."""
    return tuple(collector._lifecycle_revision for collector in program.layout.collectors)


def collector_lifecycle_is_current(
    program: CollectorProgram,
    revisions: tuple[int, ...],
) -> bool:
    """Return whether every lifecycle still matches a previously captured revision."""
    collectors = program.layout.collectors
    if len(collectors) != len(revisions):
        return False
    for index, collector in enumerate(collectors):
        if collector._lifecycle_revision != revisions[index]:
            return False
    return True


def _consume_single_unbounded(
    iterator: Any,
    collector: Collector[Any, Any, Any],
    state: CollectorState,
    revision: int,
) -> bool:
    """Run one never-done lane until exhaustion or a live done hook appears."""
    step = collector.step
    done: Callable[[Any], bool] = _never_done
    current = state.values[0]
    state.values[0] = None
    while True:
        try:
            value = next(iterator)
        except StopIteration:
            state.values[0] = current
            return True
        live_revision = collector._lifecycle_revision
        if live_revision != revision:
            revision = live_revision
            step = collector.step
            done = collector.done
        current = step(current, value)
        live_revision = collector._lifecycle_revision
        if live_revision != revision:
            revision = live_revision
            step = collector.step
            done = collector.done
        if done is _never_done:
            continue
        state.values[0] = current
        state.completed[0] = done(current)
        return False


def _consume_multi_unbounded(
    iterator: Any,
    collectors: tuple[Collector[Any, Any, Any], ...],
    state: CollectorState,
    revisions: tuple[int, ...],
) -> bool:
    """Run never-done lanes until exhaustion or one live done hook appears."""
    live_revisions = list(revisions)
    steps = [collector.step for collector in collectors]
    dones: list[Callable[[Any], bool]] = [_never_done] * len(collectors)
    while True:
        try:
            value = next(iterator)
        except StopIteration:
            return True
        needs_done_loop = False
        for index, collector in enumerate(collectors):
            live_revision = collector._lifecycle_revision
            if live_revision != live_revisions[index]:
                live_revisions[index] = live_revision
                steps[index] = collector.step
                dones[index] = collector.done
            state.values[index] = steps[index](state.values[index], value)
            live_revision = collector._lifecycle_revision
            if live_revision != live_revisions[index]:
                live_revisions[index] = live_revision
                steps[index] = collector.step
                dones[index] = collector.done
            done = dones[index]
            if done is not _never_done:
                state.completed[index] = done(state.values[index])
                needs_done_loop = True
        if needs_done_loop:
            return False


def _consume_collector_program(
    iterator: Any,
    program: CollectorProgram,
    state: CollectorState,
) -> None:
    """Advance an initialized state while retaining dynamic lifecycle replacements."""
    collectors = program.layout.collectors
    revisions = collector_lifecycle_revisions(program)
    if collectors and all(collector.done is _never_done for collector in collectors):
        if program.single:
            if _consume_single_unbounded(iterator, collectors[0], state, revisions[0]):
                return
        elif _consume_multi_unbounded(iterator, collectors, state, revisions):
            return

    while not program.done(state):
        try:
            value = next(iterator)
        except StopIteration:
            break
        program.step(state, value)


def consume_collector_program(
    values: Any,
    program: CollectorProgram,
    state: CollectorState,
) -> None:
    """Continue one initialized program and close its input before returning."""
    iterator = iter(values)
    # Import lazily to keep the collecting leaf independent during module initialization.
    from ..runtime.iterators import closing_iterators

    with closing_iterators((iterator,)):
        _consume_collector_program(iterator, program, state)


def finish_collector_program(
    values: Any,
    program: CollectorProgram,
    state: CollectorState,
) -> dict[str, Any]:
    """Continue one initialized program, close its input, and apply live finishers."""
    consume_collector_program(values, program, state)
    return program.finish(state)


def run_collector_program(values: Any, program: CollectorProgram) -> dict[str, Any]:
    """Run a program in one pass and close an iterator at its existing semantic boundary."""
    return finish_collector_program(values, program, program.initialize())
