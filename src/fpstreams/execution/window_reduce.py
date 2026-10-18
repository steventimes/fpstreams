"""Share ordered partial aggregates between adjacent count-based windows.

The front stack stores suffix aggregates, the back stack raw values plus one
aggregate. This uses the storage insight from Two-Stacks Lite (Tangwongsan et
al., https://arxiv.org/abs/2009.13768), with Python lists as the two stacks.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterator
from itertools import islice
from sys import maxsize
from typing import Generic, TypeVar, cast

T = TypeVar("T")

# Bound C-only skip loops so Python can process interrupts between blocks.
_SKIP_BLOCK_SIZE = 4096


def reduce_disjoint_windows(
    iterator: Iterator[T], size: int, step: int, combine: Callable[[T, T], T]
) -> Iterator[T]:
    """Fold nonoverlapping windows without retaining their raw input values.

    The outer islice obtains the input iterator once, as the queue's for loop
    does. Further slices only iterate that adapter. Counts may exceed maxsize;
    each individual C slice remains representable. Partial trailing windows are
    still folded so their callback errors and upstream errors retain their order.
    """
    cursor = islice(iterator, None)
    if size == 1 and step <= _SKIP_BLOCK_SIZE:
        for item in islice(cursor, 0, None, step):
            yield item
        return

    emitted = False
    while True:
        try:
            total = next(cursor)
        except StopIteration:
            return
        count = 1
        while count < size:
            target = min(size, count + maxsize)
            for item in islice(cursor, target - count):
                total = combine(total, item)
                count += 1
            if count < target:
                if not emitted:
                    yield total
                return
        emitted = True
        yield total
        del total
        if count > 1:
            del item

        remaining = step - size
        while remaining:
            block = min(remaining, _SKIP_BLOCK_SIZE)
            last = deque(enumerate(islice(cursor, block), 1), maxlen=1)
            consumed = last[0][0] if last else 0
            last.clear()
            if consumed < block:
                return
            remaining -= block


class WindowReducer(Generic[T]):
    """Maintain at most ``size + 1`` cached entries for a lawful combine function.

    Combine must be associative and must not mutate its arguments. Values and
    returned aggregates must remain unchanged while the query is being consumed.
    Stack transfers take linear work; combine calls are amortized constant per
    input item. Aggregate sizes themselves are not bounded by this entry count.
    """

    def __init__(self, size: int, step: int, combine: Callable[[T, T], T]) -> None:
        self.size = size
        self.step = step
        self.combine = combine
        self.front: list[T] = []
        self.back: list[T] = []
        self.back_total: T | None = None
        self.remaining = size
        self.gap = 0
        self.reset_pending = False
        self.emitted = False

    def push(self, value: T) -> bool:
        """Accept one source item and report whether a full window is ready."""
        if self.reset_pending:
            self.front.clear()
            self.back.clear()
            self.back_total = None
            self.reset_pending = False
        if self.gap:
            self.gap -= 1
            return False
        if len(self.front) + len(self.back) == self.size:
            if not self.front:
                while self.back:
                    item = self.back.pop()
                    self.front.append(self.combine(item, self.front[-1]) if self.front else item)
                self.back_total = None
            self.front.pop()
        self.back_total = self.combine(cast(T, self.back_total), value) if self.back else value
        self.back.append(value)
        self.remaining -= 1
        if self.remaining:
            return False
        self.emitted = True
        if self.step >= self.size:
            self.reset_pending = True
            self.gap = self.step - self.size
            self.remaining = self.size
        else:
            self.remaining = self.step
        return True

    def result(self) -> T:
        """Read a nonempty window without modifying either stack."""
        if not self.front:
            return cast(T, self.back_total)
        if not self.back:
            return self.front[-1]
        return self.combine(self.front[-1], cast(T, self.back_total))

    def finish(self) -> tuple[T, ...]:
        """Emit only an initially short window, never an incomplete trailing one."""
        if not self.emitted and self.back:
            return (self.result(),)
        return ()
