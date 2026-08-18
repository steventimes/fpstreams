"""Function composition, staged argument binding, and asynchronous retries."""

from __future__ import annotations

import asyncio
import functools
import inspect
import random
from collections.abc import Awaitable, Callable
from typing import Any, ParamSpec, TypeVar

T = TypeVar("T")
P = ParamSpec("P")


def pipe(value: T, *functions: Callable[[Any], Any]) -> Any:
    """Pass a value through a left-to-right sequence of callables.

    Each callable receives the preceding callable's return value. With no callables, the
    original value is returned unchanged.

    Args:
        value: The first callable's input.
        *functions: Unary callables to invoke in order.

    Returns:
        The final callable's return value, or `value` when `functions` is empty.
    """
    current: Any = value
    for function in functions:
        current = function(current)
    return current


def curry(function: Callable[..., T]) -> Callable[..., Any]:
    """Wrap a callable so its arguments can be supplied across multiple calls.

    The wrapped callable executes as soon as every non-variadic parameter without a default
    has been bound. Arguments may still be supplied all at once, and invalid or duplicate
    arguments raise the same binding errors produced by :func:`inspect.signature`.

    Args:
        function: A callable whose signature can be inspected.

    Returns:
        A metadata-preserving callable that either executes `function` or returns another
        argument-accepting stage.
    """
    signature = inspect.signature(function)
    required = tuple(
        name
        for name, parameter in signature.parameters.items()
        if parameter.default is inspect.Parameter.empty
        and parameter.kind not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
    )

    @functools.wraps(function)
    def curried(*args: Any, **kwargs: Any) -> Any:
        """Bind one argument stage and execute once all required parameters are present."""
        bound = signature.bind_partial(*args, **kwargs)
        if all(name in bound.arguments for name in required):
            return function(*args, **kwargs)
        return lambda *more, **named: curried(*args, *more, **kwargs, **named)

    return curried


def retry(
    attempts: int = 3,
    backoff: float = 2.0,
    jitter: bool = True,
    exceptions: tuple[type[Exception], ...] = (Exception,),
    *,
    delay: float = 0.0,
) -> Callable[[Callable[P, Awaitable[T]]], Callable[P, Awaitable[T]]]:
    """Decorate an async callable with bounded retries and exponential delay.

    `attempts` includes the initial call. Only `exceptions` are retried; all other exceptions
    propagate immediately. Before each retry, the current delay is optionally increased by a
    random value of up to ten percent, then multiplied by `backoff` for the next retry.

    Args:
        attempts: Maximum calls, including the initial call; must be at least one.
        backoff: Non-negative multiplier applied after each retry delay.
        jitter: Whether to add up to ten percent random jitter to nonzero delays.
        exceptions: Exception classes that trigger another attempt.
        delay: Non-negative seconds to wait before the first retry.

    Returns:
        A decorator whose wrapper retries the asynchronous callable under this policy.

    Raises:
        ValueError: If `attempts` is less than one or a delay parameter is negative.
    """
    if attempts < 1:
        raise ValueError("attempts must be at least 1")
    if delay < 0 or backoff < 0:
        raise ValueError("delay and backoff cannot be negative")

    def decorate(function: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
        """Wrap an async callable with the configured retry policy."""

        @functools.wraps(function)
        async def wrapped(*args: P.args, **kwargs: P.kwargs) -> T:
            """Return the first successful result or re-raise the final retryable error."""
            current_delay = delay
            for attempt in range(attempts):
                try:
                    return await function(*args, **kwargs)
                except exceptions:
                    if attempt + 1 == attempts:
                        raise
                    wait = current_delay
                    if jitter and wait:
                        wait += random.uniform(0, wait * 0.1)
                    if wait:
                        await asyncio.sleep(wait)
                    current_delay *= backoff
            raise RuntimeError("unreachable retry state")

        return wrapped

    return decorate
