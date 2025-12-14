from typing import TypeVar, Callable, Any
import functools

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