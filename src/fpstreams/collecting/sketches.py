"""Adapt optional DataSketches kernels to the existing Collector protocol."""

from __future__ import annotations

import hashlib
import importlib
import math
import operator
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, SupportsIndex

from ..expressions.selectors import Selector
from .collector import Collector, _selector
from .statistics import finite_number


def _backend() -> Any:
    """Import the optional C++ adapters only for an explicit sketch factory call."""
    try:
        return importlib.import_module("datasketches")
    except ModuleNotFoundError as error:
        if error.name != "datasketches":
            raise
        raise ImportError("approximate collectors require fpstreams[sketch]") from error


@dataclass(slots=True)
class _SketchState:
    """Keep the adapter format and precision alongside one independently owned kernel."""

    signature: tuple[str, int]
    kernel: Any


def _check_merge(left: _SketchState, right: _SketchState) -> None:
    """Reject incompatible or aliased partial states before touching either kernel."""
    if type(left) is not _SketchState or type(right) is not _SketchState:
        raise TypeError("sketch merge requires sketch states")
    if left.signature != right.signature:
        raise ValueError("sketch states must use the same algorithm and precision")
    if left is right or left.kernel is right.kernel:
        raise ValueError("sketch merge requires independent partial states")


def quantiles(
    ranks: Iterable[float], selector: Selector | None, k: SupportsIndex
) -> Collector[Any, Any, tuple[float | None, ...]]:
    """Build a double-precision KLL collector after freezing its query configuration."""
    precision = operator.index(k)
    if not 8 <= precision <= 65535:
        raise ValueError("KLL k must be between 8 and 65535")
    probabilities = tuple(finite_number(rank) for rank in ranks)
    if any(not 0 <= rank <= 1 for rank in probabilities):
        raise ValueError("quantile ranks must be between zero and one")
    select = _selector(selector)
    backend = _backend()

    def initialize() -> _SketchState:
        return _SketchState(("kll-doubles-v1", precision), backend.kll_doubles_sketch(precision))

    def step(state: _SketchState, item: Any) -> _SketchState:
        value = select(item)
        if value is not None:
            state.kernel.update(finite_number(value))
        return state

    def finish(state: _SketchState) -> tuple[float | None, ...]:
        if state.kernel.is_empty():
            return (None,) * len(probabilities)
        return tuple(state.kernel.get_quantiles(probabilities))

    def merge(left: _SketchState, right: _SketchState) -> _SketchState:
        _check_merge(left, right)
        left.kernel.merge(right.kernel)
        return left

    return Collector(initialize, step, finish, merge)


def _cardinality_token(value: Any) -> str:
    """Encode the finite builtin scalar domain without Python's randomized hash."""
    kind = type(value)
    if kind is bool or kind is int:
        raw = f"n{int(value):x}/1".encode("ascii")
    elif kind is float:
        if not math.isfinite(value):
            raise ValueError("approximate distinct values must be finite")
        numerator, denominator = value.as_integer_ratio()
        raw = f"n{numerator:x}/{denominator:x}".encode("ascii")
    elif kind is str:
        raw = b"s" + value.encode("utf-8", "surrogatepass")
    elif kind is bytes:
        raw = b"b" + value
    else:
        raise TypeError("approximate distinct requires builtin bool, int, float, str, or bytes")
    return hashlib.blake2b(raw, digest_size=16, person=b"fpstreams-hll-v1").hexdigest()


def distinct_count(selector: Selector | None, lg_k: SupportsIndex) -> Collector[Any, Any, float]:
    """Build a mergeable HLL union over explicitly encoded scalar identities."""
    precision = operator.index(lg_k)
    if not 7 <= precision <= 21:
        raise ValueError("HLL lg_k must be between 7 and 21")
    select = _selector(selector)
    backend = _backend()

    def initialize() -> _SketchState:
        return _SketchState(("hll-scalar-v1", precision), backend.hll_union(precision))

    def step(state: _SketchState, item: Any) -> _SketchState:
        value = select(item)
        if value is not None:
            state.kernel.update(_cardinality_token(value))
        return state

    def merge(left: _SketchState, right: _SketchState) -> _SketchState:
        _check_merge(left, right)
        left.kernel.update(right.kernel.get_result())
        return left

    return Collector(initialize, step, lambda state: float(state.kernel.get_estimate()), merge)
