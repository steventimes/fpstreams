# pyright: strict
"""Executable consumers of reduce and explicit fold accumulator contracts."""

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, assert_type

from fpstreams import Flow, flow


def text_step(accumulator: str, item: int) -> str:
    return accumulator + str(item)


def reduce_text(values: Flow[int], step: Callable[[str, int], str], initial: str) -> str:
    """A local, concrete boundary; not a proposed generic library declaration."""
    result: object = values.reduce(step, initial)
    if not isinstance(result, str):
        raise TypeError("text reducer returned a non-string")
    return result


def nullable_step(accumulator: int | None, item: int) -> int | None:
    return item if accumulator is None else accumulator + item


def nested_step(accumulator: list[Any], item: int) -> list[Any]:
    return [*accumulator, item]


def invalid_transition(accumulator: int, item: int) -> str:
    return str(accumulator + item)


def untyped_return(accumulator: str, item: int) -> Any:
    return item


# Callable parameter annotations check closure independently of Flow.reduce's Any.
text_callback: Callable[[str, int], str] = text_step
assert_type(reduce_text(flow([1, 2]), text_callback, ""), str)
assert reduce_text(flow([1, 2]), text_callback, "") == "12"
assert reduce_text(flow([1, 2]), lambda accumulator, item: accumulator + str(item), "") == "12"

if TYPE_CHECKING:
    # Current API limitation: annotations on callbacks do not refine these returns.
    assert_type(flow([1, 2]).reduce(lambda a, b: a + b), Any)
    assert_type(flow([1, 2]).reduce(lambda a, b: a + str(b), ""), Any)
    assert_type(flow([1, 2]).reduce(text_callback, ""), Any)
    assert_type(flow([1, 2]).reduce(nullable_step, None), Any)
    assert_type(flow([1, 2]).reduce(nested_step, [0]), Any)
    # This invalid transition is accepted by the current public Any signature.
    assert_type(flow([1, 2, 3]).reduce(invalid_transition), Any)

integer_result: object = flow([1, 2]).reduce(lambda a, b: a + b)
if not isinstance(integer_result, int):
    raise TypeError("integer reducer returned a non-integer")
assert_type(integer_result, int)
assert integer_result == 3

nullable_result: object = flow([1, 2]).reduce(nullable_step, None)
if nullable_result is not None and not isinstance(nullable_result, int):
    raise TypeError("nullable reducer returned an invalid value")
assert_type(nullable_result, int | None)
assert nullable_result == 3
assert Flow[int]([]).reduce(nullable_step, None) is None

# Nested Any is deliberately not relabelled list[int].
dynamic_seed: list[Any] = ["unknown"]
assert flow([1, 2]).reduce(nested_step, dynamic_seed) == ["unknown", 1, 2]
assert Flow[int]([]).reduce(nested_step, dynamic_seed) is dynamic_seed
assert flow([dynamic_seed]).reduce(lambda a, b: a + b) is dynamic_seed

try:
    flow([1, 2, 3]).reduce(invalid_transition)
except TypeError:
    pass
else:
    raise AssertionError("non-closed accumulator unexpectedly succeeded")

# Any bypasses callback checking; the concrete boundary still checks its result.
try:
    reduce_text(flow([1]), untyped_return, "")
except TypeError:
    pass
else:
    raise AssertionError("dynamic callback bypassed runtime result validation")


# Explicit migration: the result annotation declares the accumulator contract.
def text_initial() -> str:
    return ""


def nullable_initial() -> int | None:
    return None


def nested_initial() -> list[Any]:
    return dynamic_seed


text_fold: Flow[str] = flow([1, 2]).fold(
    text_initial, lambda state, item: assert_type(state, str) + str(assert_type(item, int))
)
assert_type(text_fold, Flow[str])
assert_type(text_fold.first(), str)
assert text_fold.first() == "12"
assert_type(flow([1, 2]).fold(nullable_initial, nullable_step).first(), int | None)
assert_type(flow([1, 2]).fold(nested_initial, nested_step).first(), list[Any])
assert Flow[int]([]).fold(nested_initial, nested_step).first() is dynamic_seed


async def explicit_async_examples() -> None:
    import asyncio
    from collections.abc import Awaitable

    from fpstreams import AsyncFlow, aflow

    async def initial() -> str:
        return ""

    async def append(state: str, item: int) -> str:
        return state + str(item)

    query: AsyncFlow[str] = aflow([1, 2]).fold_async(
        initial, lambda state, item: append(assert_type(state, str), assert_type(item, int))
    )
    assert_type(query, AsyncFlow[str])
    assert_type(await query.first(), str)
    assert await query.first() == "12"

    state: asyncio.Future[int] = asyncio.get_running_loop().create_future()
    state.set_result(7)
    sync_future: Flow[asyncio.Future[int]] = flow([1]).fold(
        lambda: state, lambda accumulator, item: accumulator
    )
    assert sync_future.first() is state

    async def future_initial() -> asyncio.Future[int]:
        return state

    async def future_step(accumulator: asyncio.Future[int], item: int) -> asyncio.Future[int]:
        assert accumulator is state
        return accumulator

    empty = AsyncFlow[int]([]).fold_async(future_initial, future_step)
    singleton = aflow([1]).fold_async(
        future_initial, lambda accumulator, item: future_step(accumulator, item)
    )
    assert_type(await empty.first(), asyncio.Future[int])
    assert_type(await singleton.first(), asyncio.Future[int])
    assert await empty.first() is state
    assert await singleton.first() is state

    async def awaitable_initial() -> Awaitable[int]:
        return state

    async def awaitable_step(accumulator: Awaitable[int], item: int) -> Awaitable[int]:
        return accumulator

    assert_type(
        await aflow([1]).fold_async(awaitable_initial, awaitable_step).first(),
        Awaitable[int],
    )


if __name__ == "__main__":
    import asyncio

    asyncio.run(explicit_async_examples())
