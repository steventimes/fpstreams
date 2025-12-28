from typing import TypeVar, Callable, Any, Awaitable, Tuple, Type
import functools
import random
import asyncio

T = TypeVar("T")
R = TypeVar("R")

def pipe(value: T, *functions: Callable[[Any], Any]) -> Any: # type: ignore
    """
    Passes a value through a sequence of functions.
    pipe(x, f, g, h) is equivalent to h(g(f(x)))
    """
    return functools.reduce(lambda val, func: func(val), functions, value)

def curry(func: Callable) -> Callable:
    """
    Transforms a function that takes multiple arguments into a chain of functions.
    @curry
    def add(a, b): return a + b
    
    add(1)(2) # returns 3
    """
    @functools.wraps(func)
    def curried(*args):
        if len(args) >= func.__code__.co_argcount:
            return func(*args)
        return lambda *more: curried(*(args + more))
    return curried

def retry(
    attempts: int = 3, 
    backoff: float = 1.5, 
    jitter: bool = True,
    exceptions: Tuple[Type[Exception], ...] = (Exception,)
) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
    """
    Decorator to retry an async function upon failure.
    
    Usage:
        @retry(attempts=3, backoff=2.0)
        async def fetch(url): ...
        
        ### Or inline in a stream:
        stream.map_async(retry(attempts=3)(fetch_func))
    """
    def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            current_delay = 1.0
            last_exception = None
            
            for attempt in range(attempts):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt == attempts - 1:
                        raise e
                    
                    # Calculate wait time
                    wait = current_delay
                    if jitter:
                        wait += random.uniform(0, 0.1 * wait)
                    
                    await asyncio.sleep(wait)
                    current_delay *= backoff

            raise last_exception  # type: ignore
        return wrapper
    return decorator