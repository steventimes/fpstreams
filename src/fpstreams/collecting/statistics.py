"""Compute compensated means and Welford variance state in one pass."""

from __future__ import annotations

import math
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from numbers import Number
from typing import Any, TypeAlias, cast

StatisticsSnapshot: TypeAlias = tuple[int, float, float]


@dataclass(slots=True)
class OnlineStatistics:
    """Accumulate count, compensated sum, running mean, and squared deviations.

    `total` and `compensation` implement a compensated floating-point sum used by
    :meth:`snapshot` for the reported mean. `rolling_mean` and `squared_deviations` implement
    Welford's online variance update.
    """

    count: int = 0
    total: float = 0.0
    compensation: float = 0.0
    rolling_mean: float = 0.0
    squared_deviations: float = 0.0

    def accept(self, item: Any) -> None:
        """Convert one real number to `float` and update every statistic in place.

        Non-numeric values and complex numbers raise `TypeError`. Non-finite values are still
        accepted, but reset sum compensation because compensated arithmetic is not meaningful
        for that update.
        """
        if not isinstance(item, Number) or isinstance(item, complex):
            raise TypeError("statistics require real numeric values")
        value = float(cast(Any, item))
        self.count += 1

        combined = self.total + value
        if math.isfinite(self.total) and math.isfinite(value) and math.isfinite(combined):
            self.compensation += (
                (self.total - combined) + value
                if abs(self.total) >= abs(value)
                else (value - combined) + self.total
            )
        else:
            self.compensation = 0.0
        self.total = combined

        delta = value - self.rolling_mean
        self.rolling_mean += delta / self.count
        self.squared_deviations += delta * (value - self.rolling_mean)

    def snapshot(self) -> StatisticsSnapshot:
        """Return `(count, compensated_mean, squared_deviations)` for finalizers.

        Empty state uses `0.0` as its internal mean; public finalizers decide whether that
        represents a value or an empty result.
        """
        mean = (self.total + self.compensation) / self.count if self.count else 0.0
        return self.count, mean, self.squared_deviations


def compensated_mean(values: Iterable[Any]) -> float | None:
    """Return the mean without maintaining variance-only Welford state.

    Exact built-in numeric values take the common branch. Other values retain
    ``OnlineStatistics.accept`` semantics, including support for registered
    ``Number`` implementations and rejection of complex or float-only objects.
    """
    count = 0
    total = 0.0
    compensation = 0.0
    isfinite = math.isfinite
    for item in values:
        item_type = type(item)
        if item_type is int or item_type is float or item_type is bool:
            value = float(item)
        else:
            if not isinstance(item, Number) or isinstance(item, complex):
                raise TypeError("statistics require real numeric values")
            value = float(cast(Any, item))
        count += 1

        combined = total + value
        if isfinite(total) and isfinite(value) and isfinite(combined):
            compensation += (
                (total - combined) + value
                if abs(total) >= abs(value)
                else (value - combined) + total
            )
        else:
            compensation = 0.0
        total = combined

    return (total + compensation) / count if count else None


def _continue_compensated_mean(
    values: Iterator[Any],
    count: int,
    total: float,
    compensation: float,
    isfinite: Callable[[float], bool],
    boundary_holder: list[Any] | None = None,
) -> float | None:
    """Continue the canonical mean loop after a callback-free native exact prefix.

    The native iterator reducer returns before invoking protocols on a subclass or custom
    ``Number``. This continuation deliberately resolves the same module globals and builtins as
    :func:`compensated_mean` for every remaining item, so callbacks can still replace ``float``,
    ``Number``, ``cast``, or the other live dependencies between iterations. ``isfinite`` remains
    the value captured when the logical compensated-mean call began.
    """
    if boundary_holder is not None:
        item = boundary_holder.pop()
        del boundary_holder
        item_type = type(item)
        if item_type is int or item_type is float or item_type is bool:
            value = float(item)
        else:
            if not isinstance(item, Number) or isinstance(item, complex):
                raise TypeError("statistics require real numeric values")
            value = float(cast(Any, item))
        count += 1

        combined = total + value
        if isfinite(total) and isfinite(value) and isfinite(combined):
            compensation += (
                (total - combined) + value
                if abs(total) >= abs(value)
                else (value - combined) + total
            )
        else:
            compensation = 0.0
        total = combined

    for item in values:
        item_type = type(item)
        if item_type is int or item_type is float or item_type is bool:
            value = float(item)
        else:
            if not isinstance(item, Number) or isinstance(item, complex):
                raise TypeError("statistics require real numeric values")
            value = float(cast(Any, item))
        count += 1

        combined = total + value
        if isfinite(total) and isfinite(value) and isfinite(combined):
            compensation += (
                (total - combined) + value
                if abs(total) >= abs(value)
                else (value - combined) + total
            )
        else:
            compensation = 0.0
        total = combined

    return (total + compensation) / count if count else None


def mean_from(snapshot: StatisticsSnapshot) -> float | None:
    """Return a snapshot's mean, or `None` when its count is zero."""
    count, mean, _squared_deviations = snapshot
    return mean if count else None


def validate_ddof(ddof: int) -> None:
    """Reject a negative delta degrees of freedom with `ValueError`."""
    if ddof < 0:
        raise ValueError("ddof must be non-negative")


def variance_from(snapshot: StatisticsSnapshot, ddof: int) -> float | None:
    """Compute `squared_deviations / (count - ddof)` from a snapshot.

    A negative `ddof` raises `ValueError`; a count no greater than `ddof` has no defined
    result and returns `None`. Tiny negative results caused by floating-point roundoff are
    clamped to zero.
    """
    validate_ddof(ddof)
    count, _mean, squared_deviations = snapshot
    if count <= ddof:
        return None
    variance = squared_deviations / (count - ddof)
    return 0.0 if variance < 0.0 else variance


def std_from(snapshot: StatisticsSnapshot, ddof: int) -> float | None:
    """Return the square root of :func:`variance_from`, preserving `None`."""
    variance = variance_from(snapshot, ddof)
    return None if variance is None else math.sqrt(variance)
