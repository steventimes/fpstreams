"""Numerically stable one-pass statistics shared by reductions."""

from __future__ import annotations

import math
from dataclasses import dataclass
from numbers import Number
from typing import Any, TypeAlias, cast

StatisticsSnapshot: TypeAlias = tuple[int, float, float]


@dataclass(slots=True)
class OnlineStatistics:
    count: int = 0
    total: float = 0.0
    compensation: float = 0.0
    rolling_mean: float = 0.0
    squared_deviations: float = 0.0

    def accept(self, item: Any) -> None:
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
        mean = (self.total + self.compensation) / self.count if self.count else 0.0
        return self.count, mean, self.squared_deviations


def mean_from(snapshot: StatisticsSnapshot) -> float | None:
    count, mean, _squared_deviations = snapshot
    return mean if count else None


def validate_ddof(ddof: int) -> None:
    if ddof < 0:
        raise ValueError("ddof must be non-negative")


def variance_from(snapshot: StatisticsSnapshot, ddof: int) -> float | None:
    validate_ddof(ddof)
    count, _mean, squared_deviations = snapshot
    if count <= ddof:
        return None
    variance = squared_deviations / (count - ddof)
    return 0.0 if variance < 0.0 else variance


def std_from(snapshot: StatisticsSnapshot, ddof: int) -> float | None:
    variance = variance_from(snapshot, ddof)
    return None if variance is None else math.sqrt(variance)
