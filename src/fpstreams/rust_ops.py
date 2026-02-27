from __future__ import annotations

from typing import Callable, Sequence, TypeVar, List, cast

T = TypeVar("T")

try:
    from fpstreams_rust import batch_list as _batch_list
    from fpstreams_rust import distinct_list as _distinct_list
    from fpstreams_rust import limit_list as _limit_list
    from fpstreams_rust import skip_list as _skip_list
    from fpstreams_rust import window_list as _window_list
    from fpstreams_rust import sorted_list as _sorted_list
    from fpstreams_rust import min_list as _min_list
    from fpstreams_rust import max_list as _max_list
    from fpstreams_rust import sum_list as _sum_list

    _RUST_AVAILABLE = True
except Exception:
    _RUST_AVAILABLE = False
    _batch_list = None
    _distinct_list = None
    _limit_list = None
    _skip_list = None
    _window_list = None
    _sorted_list = None
    _min_list = None
    _max_list = None
    _sum_list = None


def _call_rust(fn: Callable[..., object] | None, *args: object) -> object | None:
    if not _RUST_AVAILABLE or fn is None:
        return None
    return fn(*args)


def rust_available() -> bool:
    return _RUST_AVAILABLE


def distinct_list(values: Sequence[T]) -> List[T]:
    rust_result = _call_rust(_distinct_list, values)
    if rust_result is not None:
        return cast(List[T], rust_result)

    seen: set[T] = set()
    output: List[T] = []
    for item in values:
        if item not in seen:
            seen.add(item)
            output.append(item)
    return output


def batch_list(values: Sequence[T], size: int) -> List[List[T]]:
    rust_result = _call_rust(_batch_list, values, size)
    if rust_result is not None:
        return cast(List[List[T]], rust_result)

    batches: List[List[T]] = []
    current: List[T] = []
    for item in values:
        current.append(item)
        if len(current) >= size:
            batches.append(current)
            current = []
    if current:
        batches.append(current)
    return batches


def limit_list(values: Sequence[T], max_size: int) -> List[T]:
    rust_result = _call_rust(_limit_list, values, max_size)
    if rust_result is not None:
        return cast(List[T], rust_result)
    return list(values[:max_size])


def skip_list(values: Sequence[T], n: int) -> List[T]:
    rust_result = _call_rust(_skip_list, values, n)
    if rust_result is not None:
        return cast(List[T], rust_result)
    return list(values[n:])


def window_list(values: Sequence[T], size: int, step: int = 1) -> List[List[T]]:
    rust_result = _call_rust(_window_list, values, size, step)
    if rust_result is not None:
        return cast(List[List[T]], rust_result)
    if size <= 0:
        return []
    if step <= 0:
        step = 1
    output: List[List[T]] = []
    for index in range(0, max(len(values) - size + 1, 0), step):
        output.append(list(values[index:index + size]))
    return output


def sorted_list(values: Sequence[T], reverse: bool = False) -> List[T]:
    rust_result = _call_rust(_sorted_list, values, reverse)
    if rust_result is not None:
        return cast(List[T], rust_result)
    return sorted(values, reverse=reverse)


def min_list(values: Sequence[T]) -> T | None:
    rust_result = _call_rust(_min_list, values)
    if rust_result is not None:
        return cast(T, rust_result)
    if not values:
        return None
    return min(values)


def max_list(values: Sequence[T]) -> T | None:
    rust_result = _call_rust(_max_list, values)
    if rust_result is not None:
        return cast(T, rust_result)
    if not values:
        return None
    return max(values)


def sum_list(values: Sequence[T]) -> T:
    rust_result = _call_rust(_sum_list, values)
    if rust_result is not None:
        return cast(T, rust_result)
    return sum(values)  # type: ignore
