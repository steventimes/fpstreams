"""Static consumer assertions; checked by mypy and Pyright, never executed."""

from asyncio import Future
from collections import deque
from collections.abc import Awaitable, Callable, Iterable
from decimal import Decimal
from fractions import Fraction
from typing import Any, Never, TypeVar, assert_type

from fpstreams import AsyncFlow, Collector, Collectors, Flow, Pairs, Rows, aflow, flow, rows

assert_type(flow([1, 2]), Flow[int])
assert_type(flow([1, 2]).to_async(), AsyncFlow[int])
assert_type(flow([1, 2]).to_async().map(str), AsyncFlow[str])
entries: list[tuple[str, int]] = [("a", 1)]
assert_type(flow(entries).pairs(), Pairs[str, int])
assert_type(flow(entries).pairs().map_values(str), Pairs[str, str])
assert_type(flow([["a", 1]]).pairs(), Pairs[Any, Any])
assert_type(rows([{"value": 1}]), Rows[dict[str, int]])
assert_type(flow(flow([1, 2])), Flow[int])
assert_type(flow([1, 2]).map_batches(lambda batch: batch[0] + 1, size=2), Flow[int])
assert_type(flow([1, 2]).chunk(2, strict=True), Flow[tuple[int, ...]])
assert_type(aflow([1, 2]).batch(2, strict=True), AsyncFlow[tuple[int, ...]])


def unfold_step(state: int) -> tuple[str, int] | None:
    return (str(state), state - 1) if state else None


async def async_unfold_step(state: int) -> tuple[str, int] | None:
    return unfold_step(state)


def mixed_unfold_step(state: int) -> tuple[str, int] | Awaitable[tuple[str, int] | None] | None:
    return unfold_step(state) if state % 2 else async_unfold_step(state)


def accum_step(state: int, value: int) -> tuple[int, str]:
    return state + value, str(state)


async def async_accum_step(state: int, value: int) -> tuple[int, str]:
    return accum_step(state, value)


def mixed_accum_step(state: int, value: int) -> tuple[int, str] | Awaitable[tuple[int, str]]:
    return accum_step(state, value) if value % 2 else async_accum_step(state, value)


def awaitable_accum_output(state: int, value: int) -> tuple[int, Awaitable[str]]:
    raise NotImplementedError


async def async_awaitable_accum_output(state: int, value: int) -> tuple[int, Awaitable[str]]:
    raise NotImplementedError


def mixed_accum_output(state: int, value: int) -> tuple[int, str | Awaitable[str]]:
    raise NotImplementedError


def mixed_awaitable_transition(
    state: int, value: int
) -> tuple[int, Awaitable[str]] | Awaitable[tuple[int, Awaitable[str]]]:
    raise NotImplementedError


def nested_accum_output(state: int, value: int) -> tuple[int, Awaitable[Awaitable[str]]]:
    raise NotImplementedError


def future_accum_output(state: int, value: int) -> tuple[int, Future[str]]:
    raise NotImplementedError


def dynamic_accum_output(state: int, value: int) -> tuple[int, Any]:
    raise NotImplementedError


assert_type(flow.unfold(2, unfold_step), Flow[str])
assert_type(Flow.unfold(2, unfold_step), Flow[str])
assert_type(flow([1, 2]).map_accum(0, accum_step), Flow[str])
assert_type(aflow.unfold(2, unfold_step), AsyncFlow[str])
assert_type(aflow.unfold(2, async_unfold_step), AsyncFlow[str])
assert_type(aflow.unfold(2, mixed_unfold_step), AsyncFlow[str])
assert_type(AsyncFlow.unfold(2, unfold_step), AsyncFlow[str])
assert_type(AsyncFlow.unfold(2, async_unfold_step), AsyncFlow[str])
assert_type(AsyncFlow.unfold(2, mixed_unfold_step), AsyncFlow[str])
assert_type(aflow([1, 2]).map_accum(0, accum_step), AsyncFlow[str])
assert_type(aflow([1, 2]).map_accum(0, async_accum_step), AsyncFlow[str])
assert_type(aflow([1, 2]).map_accum(0, mixed_accum_step), AsyncFlow[str])
assert_type(aflow([1, 2]).map_accum(0, awaitable_accum_output), AsyncFlow[str])
assert_type(aflow([1, 2]).map_accum(0, async_awaitable_accum_output), AsyncFlow[str])
assert_type(aflow([1, 2]).map_accum(0, mixed_accum_output), AsyncFlow[str])
assert_type(aflow([1, 2]).map_accum(0, mixed_awaitable_transition), AsyncFlow[str])
assert_type(aflow([1, 2]).map_accum(0, nested_accum_output), AsyncFlow[Awaitable[str]])
assert_type(aflow([1, 2]).map_accum(0, future_accum_output), AsyncFlow[str])
assert_type(aflow([1, 2]).map_accum(0, dynamic_accum_output), AsyncFlow[Any])
assert_type(
    aflow([1, 2]).map_accum(0, lambda state, value: (state + value, str(value))), AsyncFlow[str]
)
assert_type(rows(rows([{"value": 1}])), Rows[dict[str, int]])


async def async_item(value: int) -> int:
    return value + 1


async def async_batch(batch: tuple[int, ...]) -> int:
    return sum(batch)


def mixed_item(value: int) -> int | Awaitable[int]:
    return value if value % 2 else async_item(value)


def mixed_batch(batch: tuple[int, ...]) -> int | Awaitable[int]:
    return sum(batch) if batch[0] % 2 else async_batch(batch)


assert_type(aflow([1, 2]).map(async_item), AsyncFlow[int])
assert_type(aflow([1, 2]).map_async(async_item), AsyncFlow[int])
assert_type(aflow([1, 2]).map_batches(async_batch, size=2), AsyncFlow[int])
assert_type(aflow([1, 2]).map(mixed_item), AsyncFlow[int])
assert_type(aflow([1, 2]).map_async(mixed_item), AsyncFlow[int])
assert_type(aflow([1, 2]).map_batches(mixed_batch, size=2), AsyncFlow[int])
assert_type(aflow([1, 2]).map_batches(lambda batch: str(batch), size=2), AsyncFlow[str])
listed: Collector[int, list[int], list[int]] = Collectors.to_list()
counted: Collector[int, int, int] = Collectors.counting()
absent: Collector[int, None, None] = Collector(lambda: None, lambda state, value: None)


def adjacent(left: int, right: int) -> bool:
    return left == right


async def async_adjacent(left: int, right: int) -> bool:
    return left == right


assert_type(flow([1, 2]).collapse(adjacent, listed), Flow[list[int]])
assert_type(flow([1, 2]).collapse(adjacent, Collectors.to_list()), Flow[list[int]])
assert_type(flow([1, 2]).collapse(adjacent, counted), Flow[int])
assert_type(aflow([1, 2]).collapse(async_adjacent, listed), AsyncFlow[list[int]])
assert_type(aflow([1, 2]).collapse(async_adjacent, Collectors.to_list()), AsyncFlow[list[int]])
dictionary: Collector[int, dict[int, str], dict[int, str]] = Collectors.to_dict(
    lambda value: value, lambda value: str(value)
)
assert_type(flow([1, 2]).collect(listed), list[int])
assert_type(flow([1, 2]).collect(Collectors.to_list()), list[int])
assert_type(flow([1, 2]).collect(Collectors.to_set()), set[int])
assert_type(flow([1, 2]).collect(Collectors.to_tuple()), tuple[int, ...])
assert_type(flow([1, 2]).collect(Collectors.first()), int | None)
assert_type(flow([1, 2]).collect(Collectors.head(1)), list[int])
assert_type(flow(entries).pairs().collect_values(Collectors.to_list()), dict[str, list[int]])
assert_type(Collectors[int].to_list(), Collector[int, list[int], list[int]])
assert_type(Collectors[int].counting(), Collector[int, int, int])
assert_type(flow([1, 2]).collect(Collectors.frequencies()), dict[int, int])
assert_type(Collectors[int].frequencies(), Collector[int, dict[int, int], dict[int, int]])
assert_type(flow([1, 2]).collect(Collectors.mode()), int | None)
assert_type(Collectors[int].mode(), Collector[int, Any, int | None])


async def mode_type_contract() -> None:
    assert_type(await aflow([1, 2]).collect(Collectors.mode()), int | None)
    assert_type(await aflow([1, 2]).collect(Collectors.frequencies()), dict[int, int])


assert_type(Collectors[Any].to_list(), Collector[Any, list[Any], list[Any]])
assert_type(Collectors[Never].to_list(), Collector[Never, list[Never], list[Never]])
assert_type(flow([1, 2]).collect(counted), int)
assert_type(flow([1, 2]).collect(absent), None)
assert_type(flow([1, 2]).collect(dictionary), dict[int, str])
assert_type(flow([1, 2]).collect(size=counted), dict[str, Any])
assert_type(flow([1, 2]).collect(None, size=counted), dict[str, Any])
assert_type(flow([1, 2]).collect(collector=counted), dict[str, Any])


def collect_text(values: Iterable[int]) -> str:
    return str(sum(values))


assert_type(flow([1, 2]).collect(collect_text), str)


def forward_collect(
    collector: Collector[int, Any, int] | None, named: dict[str, Collector[int, Any, Any]]
) -> int | dict[str, Any]:
    result = flow([1, 2]).collect(collector, **named)
    assert_type(result, int | dict[str, Any])
    return result


async def finish_async(values: list[int]) -> str:
    return str(sum(values))


payload: Collector[int, list[int], Awaitable[str]] = Collector(
    list, lambda state, value: [*state, value], finish_async
)
assert_type(flow([1, 2]).collect(payload), Awaitable[str])
assert_type(flow([1, 2]).collapse(adjacent, payload), Flow[Awaitable[str]])
assert_type(aflow([1, 2]).collapse(async_adjacent, payload), AsyncFlow[Awaitable[str]])


async def collect_typed() -> None:
    assert_type(await aflow([1, 2]).collect(listed), list[int])
    assert_type(await aflow([1, 2]).collect(Collectors.to_list()), list[int])
    assert_type(await aflow([1, 2]).collect(counted), int)
    assert_type(await aflow([1, 2]).collect(absent), None)
    assert_type(await aflow([1, 2]).collect(dictionary), dict[int, str])
    assert_type(await aflow([1, 2]).collect(size=counted), dict[str, Any])
    assert_type(await aflow([1, 2]).collect(None, size=counted), dict[str, Any])
    assert_type(await aflow([1, 2]).collect(collector=counted), dict[str, Any])
    assert_type(await aflow([1, 2]).collect(payload), Awaitable[str])


async def forward_async_collect(
    collector: Collector[int, Any, int] | None, named: dict[str, Collector[int, Any, Any]]
) -> int | dict[str, Any]:
    result = await aflow([1, 2]).collect(collector, **named)
    assert_type(result, int | dict[str, Any])
    return result


assert_type(Collectors[object].to_list(), Collector[object, list[object], list[object]])
assert_type(Collectors[int | str].to_list(), Collector[int | str, list[int | str], list[int | str]])

assert_type(Collectors[Any].counting(), Collector[Any, int, int])
assert_type(Collectors[Never].counting(), Collector[Never, int, int])
Element = TypeVar("Element")


def generic_factory(value: Element) -> Collector[Element, list[Element], list[Element]]:
    result = Collectors[Element].to_list()
    assert_type(result, Collector[Element, list[Element], list[Element]])
    return result


assert_type(flow([1, 2]).collect(Collectors.tail(1)), list[int])
assert_type(flow([1, 2]).collect(Collectors.last()), int | None)
assert_type(flow([1, 2]).collect(Collectors.only()), int | None)
assert_type(Collectors[int].to_set(), Collector[int, set[int], set[int]])
assert_type(Collectors[int].to_tuple(), Collector[int, list[int], tuple[int, ...]])
assert_type(Collectors[int].head(1), Collector[int, list[int], list[int]])
assert_type(Collectors[int].tail(1), Collector[int, deque[int], list[int]])
assert_type(flow([1, 2]).collect(Collectors[int].to_list()), list[int])


def broad_step(state: int, value: object) -> int:
    return state + 1


broad: Collector[object, int, int] = Collector(lambda: 0, broad_step)
assert_type(flow([1, 2]).collect(broad), int)


async def collect_more_types() -> None:
    assert_type(await aflow([1, 2]).collect(Collectors.to_set()), set[int])
    assert_type(await aflow([1, 2]).collect(Collectors.to_tuple()), tuple[int, ...])
    assert_type(await aflow([1, 2]).collect(Collectors.first()), int | None)
    assert_type(await aflow([1, 2]).collect(Collectors[int].to_list()), list[int])
    assert_type(await aflow([1, 2]).collect(broad), int)


gradual: Collector[Any, list[Any], list[Any]] = Collectors[Any].to_list()
assert_type(flow([1, 2]).collect(gradual), list[Any])
gradual_payload: Collector[Any, list[int], Awaitable[str]] = Collector(
    list, lambda state, value: [*state, int(value)], finish_async
)
assert_type(flow([1, 2]).collect(gradual_payload), Awaitable[str])
object_payload: Collector[object, list[int], Awaitable[str]] = Collector(
    list, lambda state, value: [*state, len(str(value))], finish_async
)


async def async_collect_text(values: Iterable[int]) -> str:
    return str(sum(values))


async def collect_gradual_types() -> None:
    assert_type(await aflow([1, 2]).collect(gradual), list[Any])
    assert_type(await aflow([1, 2]).collect(collect_text), str)
    assert_type(await aflow([1, 2]).collect(async_collect_text), str)
    assert_type(await aflow([1, 2]).collect(object_payload), Awaitable[str])


def forward_callable(
    collector: Collector[int, Any, str] | Callable[[Iterable[int]], str] | None,
    named: dict[str, Collector[int, Any, Any]],
) -> str | dict[str, Any]:
    result = flow([1, 2]).collect(collector, **named)
    assert_type(result, str | dict[str, Any])
    return result


async def forward_async_callable(
    collector: Collector[int, Any, str] | Callable[[Iterable[int]], str | Awaitable[str]] | None,
    named: dict[str, Collector[int, Any, Any]],
) -> str | dict[str, Any]:
    result = await aflow([1, 2]).collect(collector, **named)
    assert_type(result, str | dict[str, Any])
    return result


# A supplied default contributes its type; an omitted default adds no Any.
assert_type(flow([1, 2]).first(), int)
assert_type(flow([1, 2]).first(default=None), int | None)
assert_type(flow([1, 2]).first(default="missing"), int | str)
assert_type(flow([1, 2]).last(), int)
assert_type(flow([1, 2]).last(default=None), int | None)
assert_type(flow([1, 2]).last(default="missing"), int | str)
assert_type(flow([1, 2]).find(lambda value: value > 0), int)
assert_type(flow([1, 2]).find(lambda value: value > 0, default=None), int | None)
assert_type(flow([1, 2]).find(lambda value: value > 0, default="missing"), int | str)
assert_type(flow([1, 2]).nth(0), int)
assert_type(flow([1, 2]).nth(0, default=None), int | None)
assert_type(flow([1, 2]).nth(0, default="missing"), int | str)


Fallback = TypeVar("Fallback")


def forward_terminal_default(values: Flow[Element], fallback: Fallback) -> Element | Fallback:
    return values.first(fallback)


async def terminal_default_types() -> None:
    assert_type(await aflow([1, 2]).first(), int)
    assert_type(await aflow([1, 2]).first(default=None), int | None)
    assert_type(await aflow([1, 2]).first(default="missing"), int | str)
    assert_type(await aflow([1, 2]).last(), int)
    assert_type(await aflow([1, 2]).last(default=None), int | None)
    assert_type(await aflow([1, 2]).last(default="missing"), int | str)
    assert_type(await aflow([1, 2]).find(lambda value: value > 0), int)
    assert_type(await aflow([1, 2]).find(lambda value: value > 0, default=None), int | None)
    assert_type(await aflow([1, 2]).find(lambda value: value > 0, default="missing"), int | str)
    assert_type(await aflow([1, 2]).nth(-1), int)
    assert_type(await aflow([1, 2]).nth(-1, default=None), int | None)
    assert_type(await aflow([1, 2]).nth(-1, default="missing"), int | str)


async def forward_async_terminal_default(
    values: AsyncFlow[Element], fallback: Fallback
) -> Element | Fallback:
    return await values.nth(0, fallback)


# Explicit averaging keeps input, accumulator, and result types independent.


def decimal_item(value: Decimal) -> Decimal:
    return value


def decimal_add(total: Decimal, value: Decimal) -> Decimal:
    return total + value


def decimal_divide(total: Decimal, count: int) -> Decimal:
    return total / count


def decimal_empty() -> Decimal:
    return Decimal(0)


def decimal_row(row: dict[str, Decimal]) -> Decimal:
    return row["value"]


explicit_average = Collectors.averaging_with(
    decimal_item,
    zero=Decimal,
    add=decimal_add,
    divide=decimal_divide,
    empty=decimal_empty,
    combine=decimal_add,
)
assert_type(explicit_average([Decimal("1.1")]), Decimal)
assert_type(explicit_average([]), Decimal)
assert_type(flow([Decimal("1.1")]).collect(explicit_average), Decimal)
assert_type(
    flow([Decimal("1.1")]).collect(Collectors.grouping_by(str, explicit_average)),
    dict[Any, Decimal | list[Decimal]],
)
assert_type(
    flow([Decimal("1.1")]).collect(Collectors.collecting_and_then(explicit_average, str)), str
)
row_average = Collectors.averaging_with(
    decimal_row, zero=Decimal, add=decimal_add, divide=decimal_divide, empty=decimal_empty
)
assert_type(row_average([{"value": Decimal("1.1")}]), Decimal)
assert_type(Collectors[Decimal].averaging()([Decimal("1.1")]), object)
assert_type(Collectors[Fraction].averaging()([Fraction(1, 3)]), object)
assert_type(Collectors[complex].averaging()([1j]), object)


def optional_decimal_divide(total: Decimal, count: int) -> Decimal | None:
    return total / count


def no_average() -> Decimal | None:
    return None


optional_average = Collectors.averaging_with(
    decimal_item, zero=Decimal, add=decimal_add, divide=optional_decimal_divide, empty=no_average
)
assert_type(optional_average([]), Decimal | None)


class AverageToken:
    def __truediv__(self, count: int) -> str:
        return str(count)


def token_item(value: AverageToken) -> AverageToken:
    return value


def token_add(total: AverageToken, value: AverageToken) -> AverageToken:
    return total


def token_divide(total: AverageToken, count: int) -> str:
    return total / count


text_average = Collectors.averaging_with(
    token_item, zero=AverageToken, add=token_add, divide=token_divide, empty=str
)
assert_type(text_average([AverageToken()]), str)
assert_type(flow([AverageToken()]).collect(text_average), str)


async def explicit_average_async() -> None:
    assert_type(await aflow([Decimal("1.1")]).collect(explicit_average), Decimal)
    assert_type(await aflow([{"value": Decimal("1.1")}]).collect(row_average), Decimal)
    assert_type(await aflow([AverageToken()]).collect(text_average), str)
    assert_type(await aflow([Decimal("1.1")]).collect(optional_average), Decimal | None)


def fraction_item(value: Fraction) -> Fraction:
    return value


def fraction_add(total: Fraction, value: Fraction) -> Fraction:
    return total + value


def fraction_divide(total: Fraction, count: int) -> Fraction:
    return total / count


fraction_average = Collectors.averaging_with(
    fraction_item, zero=Fraction, add=fraction_add, divide=fraction_divide, empty=Fraction
)
assert_type(fraction_average([Fraction(1, 3)]), Fraction)


def complex_item(value: complex) -> complex:
    return value


def complex_add(total: complex, value: complex) -> complex:
    return total + value


def complex_divide(total: complex, count: int) -> complex:
    return total / count


complex_average = Collectors.averaging_with(
    complex_item, zero=complex, add=complex_add, divide=complex_divide, empty=complex
)
assert_type(complex_average([1j]), complex)
