from __future__ import annotations

from typing import Sequence, TypeVar, List

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


def rust_available() -> bool:
    return _RUST_AVAILABLE


def distinct_list(values: Sequence[T]) -> List[T]:
    if _RUST_AVAILABLE:
        return _distinct_list(values)  # type: ignore

    seen: set[T] = set()
    output: List[T] = []
    for item in values:
        if item not in seen:
            seen.add(item)
            output.append(item)
    return output


def batch_list(values: Sequence[T], size: int) -> List[List[T]]:
    if _RUST_AVAILABLE:
        return _batch_list(values, size)  # type: ignore

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
    if _RUST_AVAILABLE:
        return _limit_list(values, max_size)  # type: ignore
    return list(values[:max_size])


def skip_list(values: Sequence[T], n: int) -> List[T]:
    if _RUST_AVAILABLE:
        return _skip_list(values, n)  # type: ignore
    return list(values[n:])


def window_list(values: Sequence[T], size: int, step: int = 1) -> List[List[T]]:
    if _RUST_AVAILABLE:
        return _window_list(values, size, step)  # type: ignore
    if size <= 0:
        return []
    if step <= 0:
        step = 1
    output: List[List[T]] = []
    for index in range(0, max(len(values) - size + 1, 0), step):
        output.append(list(values[index:index + size]))
    return output


def sorted_list(values: Sequence[T], reverse: bool = False) -> List[T]:
    if _RUST_AVAILABLE:
        return _sorted_list(values, reverse)  # type: ignore
    return sorted(values, reverse=reverse)


def min_list(values: Sequence[T]) -> T | None:
    if _RUST_AVAILABLE:
        return _min_list(values)  # type: ignore
    if not values:
        return None
    return min(values)


def max_list(values: Sequence[T]) -> T | None:
    if _RUST_AVAILABLE:
        return _max_list(values)  # type: ignore
    if not values:
        return None
    return max(values)


def sum_list(values: Sequence[T]) -> T:
    if _RUST_AVAILABLE:
        return _sum_list(values)  # type: ignore
    return sum(values)  # type: ignore
