"""Share a submission budget without taking ownership of the underlying executor."""

from __future__ import annotations

import operator
from collections import deque
from collections.abc import Callable
from concurrent.futures import Executor, Future
from threading import Condition
from typing import Any, ParamSpec, TypeVar

P = ParamSpec("P")
T = TypeVar("T")


class BoundedExecutor(Executor):
    """Limit unfinished submissions across callers sharing one borrowed executor.

    Submitters wait in arrival order when the budget is full. Completed or cancelled
    futures release their slots; retained results remain subject to each flow's buffer.
    Submit through the same wrapper to share its budget. Tasks submitted directly to
    the underlying executor are outside that budget.

    Shutdown closes this wrapper and optionally cancels or waits for its own work.
    It never shuts down the underlying executor. As with a thread pool, callbacks
    must not wait for nested submissions that require their own occupied slots.

    Args:
        executor: Caller-owned thread, process, or other concurrent executor.
        max_pending: Maximum number of unfinished submitted calls, including running calls.

    Raises:
        TypeError: If executor is not an Executor or max_pending is not an integer.
        ValueError: If max_pending is less than one.
    """

    def __init__(self, executor: Executor, *, max_pending: int) -> None:
        if not isinstance(executor, Executor):
            raise TypeError("executor must be a concurrent.futures.Executor")
        limit = operator.index(max_pending)
        if limit < 1:
            raise ValueError("max_pending must be positive")
        self._executor = executor
        self._max_pending = limit
        self._condition = Condition()
        self._waiters: deque[object] = deque()
        self._futures: set[Future[Any]] = set()
        self._admitting = 0
        self._shutdown = False
        self._cancel_on_shutdown = False

    @property
    def max_pending(self) -> int:
        """Return the shared capacity, including running calls."""
        return self._max_pending

    @property
    def pending(self) -> int:
        """Return the current number of unfinished or being-submitted calls."""
        with self._condition:
            return self._admitting + len(self._futures)

    def _reserve(self) -> None:
        with self._condition:
            ticket = object()
            self._waiters.append(ticket)
            try:
                while True:
                    if self._shutdown:
                        raise RuntimeError("cannot schedule new futures after shutdown")
                    if (
                        self._waiters[0] is ticket
                        and self._admitting + len(self._futures) < self._max_pending
                    ):
                        self._admitting += 1
                        return
                    self._condition.wait()
            finally:
                self._waiters.remove(ticket)
                self._condition.notify_all()

    def submit(self, fn: Callable[P, T], /, *args: P.args, **kwargs: P.kwargs) -> Future[T]:
        """Submit a call, blocking for shared capacity without changing its Future."""
        self._reserve()
        try:
            future = self._executor.submit(fn, *args, **kwargs)
        except BaseException:
            with self._condition:
                self._admitting -= 1
                self._condition.notify_all()
            raise
        with self._condition:
            self._futures.add(future)
            self._admitting -= 1
            cancel = self._cancel_on_shutdown
        future.add_done_callback(self._release)
        if cancel:
            future.cancel()
        return future

    def _release(self, future: Future[Any]) -> None:
        with self._condition:
            self._futures.discard(future)
            self._condition.notify_all()

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        """Close admissions and clean up this wrapper's work, keeping the pool open."""
        with self._condition:
            self._shutdown = True
            self._cancel_on_shutdown |= cancel_futures
            futures = tuple(self._futures) if cancel_futures else ()
            self._condition.notify_all()
        for future in futures:
            future.cancel()
        if wait:
            with self._condition:
                while self._admitting or self._futures:
                    self._condition.wait()
