"""Small functional helpers that are independent of pipeline execution."""

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
    """Pass value through functions from left to right.

    The first callable receives `value`; each later callable receives the previous result.

    Args:
        value: The value consumed by this operation.
        *functions: Callables applied from left to right.

    Returns:
        The result returned by the final callable, or `value` when no callables are supplied.
    """
    current: Any = value
    for function in functions:
        current = function(current)
    return current


def curry(function: Callable[..., T]) -> Callable[..., Any]:
    """Return a callable that accepts the original arguments in stages.

    Required parameters are detected from `inspect.signature`, so defaults, builtins, and
    callable objects are supported.

    Args:
        function: The callable applied by this operation.

    Returns:
        A callable implementing the described behavior.
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
    """Retry an async function with configurable delay and backoff.

    Only the configured exception types are retried; other exceptions propagate immediately.

    Args:
        attempts: The maximum number of calls, including the first attempt.
        backoff: The multiplier applied to the delay after each failed attempt.
        jitter: Random delay added to each retry interval.
        exceptions: The exception type or tuple of types that should trigger a retry.
        delay: The initial delay in seconds before retrying.

    Returns:
        A callable implementing the described behavior.
    """
    if attempts < 1:
        raise ValueError("attempts must be at least 1")
    if delay < 0 or backoff < 0:
        raise ValueError("delay and backoff cannot be negative")

    def decorate(function: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
        @functools.wraps(function)
        async def wrapped(*args: P.args, **kwargs: P.kwargs) -> T:
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
