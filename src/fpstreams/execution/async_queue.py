"""Bounded completion primitives used by async physical schedulers."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

_MISSING = object()


@dataclass(frozen=True, slots=True)
class Completion:
    """The value or error associated with one logical sequence number."""

    sequence: int
    value: object = _MISSING
    error: BaseException | None = None

    @classmethod
    def from_task(cls, sequence: int, task: asyncio.Task[Any]) -> Completion:
        """Capture a completed task outcome without raising from a done callback."""
        if task.cancelled():
            return cls(sequence, error=asyncio.CancelledError())
        try:
            return cls(sequence, value=task.result())
        except BaseException as error:
            return cls(sequence, error=error)

    @classmethod
    def value_of(cls, sequence: int, value: object) -> Completion:
        """Create a successful completion without allocating a temporary task."""
        return cls(sequence, value=value)

    def result(self) -> object:
        """Return the completed value or raise its captured error."""
        if self.error is not None:
            raise self.error
        return self.value


class OrderedResultRing:
    """Fixed-capacity sequence slots that release only the next logical completion."""

    def __init__(self, capacity: int) -> None:
        if capacity < 1:
            raise ValueError("ordered ring capacity must be at least 1")
        self._slots: list[Completion | None] = [None] * capacity
        self._next = 0

    def put(self, sequence: int, completion: Completion) -> None:
        """Store a completion unless its fixed slot is still occupied by another sequence."""
        if sequence != completion.sequence:
            raise ValueError("completion sequence does not match slot sequence")
        slot = sequence % len(self._slots)
        occupied = self._slots[slot]
        if occupied is not None:
            raise RuntimeError(f"ordered result ring slot {slot} is occupied")
        self._slots[slot] = completion

    def pop_next(self) -> Completion | None:
        """Release exactly the next sequence when it has completed."""
        slot = self._next % len(self._slots)
        completion = self._slots[slot]
        if completion is None or completion.sequence != self._next:
            return None
        self._slots[slot] = None
        self._next += 1
        return completion


class CompletionQueue:
    """An unbounded completion-order queue populated by task done callbacks."""

    def __init__(self) -> None:
        self._queue: asyncio.Queue[Completion] = asyncio.Queue()

    def publish(self, completion: Completion) -> None:
        """Enqueue a captured task outcome without blocking its done callback."""
        self._queue.put_nowait(completion)

    def watch(self, task: asyncio.Task[Any], *, sequence: int) -> None:
        """Publish one captured outcome when task settles without invoking user callbacks."""
        task.add_done_callback(
            lambda settled: self.publish(Completion.from_task(sequence, settled))
        )

    async def get(self) -> Completion:
        """Wait for the next completion in physical finish order."""
        return await self._queue.get()

    def empty(self) -> bool:
        """Report whether a completion is already available without waiting."""
        return self._queue.empty()
