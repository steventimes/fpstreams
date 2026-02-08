from __future__ import annotations

from typing import Sequence, TypeVar, List

T = TypeVar("T")

try:
    from fpstreams_rust import batch_list as _batch_list
    from fpstreams_rust import distinct_list as _distinct_list

    _RUST_AVAILABLE = True
except Exception:
    _RUST_AVAILABLE = False
    _batch_list = None
    _distinct_list = None


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
