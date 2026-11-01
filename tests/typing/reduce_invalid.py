# pyright: strict
"""Static negative examples: marked expected errors; do not execute this file."""

from collections.abc import Callable

from fpstreams import AsyncFlow, Flow, aflow, flow
from tests.typing.reduce_examples import invalid_transition, reduce_text, text_initial

wrong: Callable[[str, int], str] = invalid_transition  # error: assignment
reduce_text(flow([1, 2, 3]), invalid_transition, "")  # error: arg-type

# An explicit result annotation gives lambdas a concrete accumulator type.
bad_lambda: Flow[str] = flow([1, 2]).fold(
    text_initial,
    lambda state, item: state.missing(),  # error: attr-defined
)
bad_fold: Flow[str] = flow([1, 2]).fold(text_initial, invalid_transition)  # error: arg-type


async def async_initial() -> str:
    return ""


async def wrong_step(state: int, item: int) -> str:
    return str(state + item)


bad_async_fold: AsyncFlow[str] = aflow([1, 2]).fold_async(
    async_initial,
    wrong_step,  # error: arg-type
)
