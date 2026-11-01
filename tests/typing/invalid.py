"""Each marked line must be rejected with the matching diagnostic rule."""

from collections.abc import Awaitable
from decimal import Decimal
from typing import Any

from fpstreams import AsyncFlow, Collector, Collectors, aflow, flow

Bad = Collector[str, Any, Any]
Int = Collectors[int]
Text = Collectors[str]


def merge(count: int, values: list[int]) -> int:
    return count


def future_output(state: int, value: int) -> tuple[int, Awaitable[str]]:
    raise NotImplementedError


wrong_0: Bad = Int.statistics()  # error: assignment
wrong_1: Bad = Int.approx_quantiles([0.5])  # error: assignment
wrong_2: Bad = Int.approx_count_distinct()  # error: assignment
wrong_3: Bad = Int.to_list()  # error: assignment
wrong_4: Bad = Int.to_set()  # error: assignment
wrong_5: Bad = Int.to_tuple()  # error: assignment
wrong_6: Bad = Int.grouping_by("group")  # error: assignment
wrong_7: Bad = Int.partitioning_by("active")  # error: assignment
wrong_8: Bad = Int.mapping(str, Text.to_list())  # error: assignment
wrong_9: Bad = Int.filtering(bool, Int.to_list())  # error: assignment
wrong_10: Bad = Int.flat_mapping(str, Text.to_list())  # error: assignment
wrong_11: Bad = Int.collecting_and_then(Int.to_list(), len)  # error: assignment
wrong_12: Bad = Int.teeing(Int.counting(), Int.to_list(), merge)  # error: assignment
wrong_13: Bad = Int.counting()  # error: assignment
wrong_14: Bad = Int.summing()  # error: assignment
wrong_15: Bad = Int.averaging()  # error: assignment
wrong_16: Bad = Int.summarizing()  # error: assignment
wrong_17: Bad = Int.first()  # error: assignment
wrong_18: Bad = Int.last()  # error: assignment
wrong_19: Bad = Int.head(2)  # error: assignment
wrong_20: Bad = Int.tail(2)  # error: assignment
wrong_21: Bad = Int.only()  # error: assignment
wrong_22: Bad = Int.to_dict(str, str)  # error: assignment
wrong_mode: Bad = Int.mode()  # error: assignment
wrong_frequencies: Bad = Int.frequencies()  # error: assignment

wrong_result: str = flow([1, 2]).collect(Collectors.to_list())  # error: assignment
accum_flow = aflow([1]).map_accum(0, lambda state, value: (state + value, value))
wrong_accum: str = accum_flow  # error: assignment
resolved_output = aflow([1]).map_accum(0, future_output)
bad_future_output: AsyncFlow[Awaitable[str]] = resolved_output  # error: assignment
wrong_elements: list[str] = flow([1, 2]).map(lambda value: value + 1).to_list()  # error: assignment
wrong_collector: Collector[int, int, int] = Int.to_list()  # error: assignment
wrong_bound: Collector[str, list[str], list[str]] = Int.to_list()  # error: assignment
wrong_inputs: Collector[object, int, int] = Int.counting()  # error: assignment


async def invalid_async() -> None:
    await flow([1]).to_async().missing_method()  # error: attr-defined


flow([("a", 1)]).pairs().map_values(lambda value: value.upper()).to_dict()  # error: attr-defined

flow([1, 2]).collect(Collectors[str].to_list())  # error: arg-type
flow([1, 2]).collect(size=Collectors[str].counting())  # error: arg-type


async def wrong_async_input() -> None:
    await aflow([1, 2]).collect(Collectors[str].to_list())  # error: arg-type


first_with_default = flow([1, 2]).first(default="missing")
bad_first_default: int = first_with_default  # error: assignment
last_with_default = flow([1, 2]).last(default="missing")
bad_last_default: int = last_with_default  # error: assignment
find_with_default = flow([1, 2]).find(bool, default="missing")
bad_find_default: int = find_with_default  # error: assignment
nth_with_default = flow([1, 2]).nth(0, default="missing")
bad_nth_default: int = nth_with_default  # error: assignment


async def invalid_terminal_defaults() -> None:
    first_with_default = await aflow([1, 2]).first(default="missing")
    _bad_first_default: int = first_with_default  # error: assignment
    last_with_default = await aflow([1, 2]).last(default="missing")
    _bad_last_default: int = last_with_default  # error: assignment
    find_with_default = await aflow([1, 2]).find(bool, default="missing")
    _bad_find_default: int = find_with_default  # error: assignment
    nth_with_default = await aflow([1, 2]).nth(0, default="missing")
    _bad_nth_default: int = nth_with_default  # error: assignment


# An explicit average must receive the selected input type and expose its real result.


def average_item(value: Decimal) -> Decimal:
    return value


def average_add(total: Decimal, value: Decimal) -> Decimal:
    return total + value


def average_divide(total: Decimal, count: int) -> Decimal:
    return total / count


average = Collectors.averaging_with(
    average_item, zero=Decimal, add=average_add, divide=average_divide, empty=Decimal
)
wrong_average_result: float = average([Decimal(1)])  # error: assignment
wrong_old_average: float = Collectors[Decimal].averaging()([Decimal(1)])  # error: assignment
average_bad_values = ["bad"]
average(average_bad_values)  # error: arg-type
flow(["bad"]).collect(average)  # error: arg-type


async def invalid_average_async() -> None:
    await aflow(["bad"]).collect(average)  # error: arg-type
