"""Validated resource budgets for partitioned tabular operations."""

from __future__ import annotations

import operator
from dataclasses import dataclass

from ..errors import BufferLimitError


def _positive_integer(name: str, value: int) -> int:
    """Coerce an integer-like limit and require it to be greater than zero."""
    try:
        result = operator.index(value)
    except TypeError:
        raise TypeError(f"{name} must be an integer") from None
    if result <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return result


def _nonnegative_integer(name: str, value: int) -> int:
    """Coerce an integer-like limit and reject negative values."""
    try:
        result = operator.index(value)
    except TypeError:
        raise TypeError(f"{name} must be an integer") from None
    if result < 0:
        raise ValueError(f"{name} cannot be negative")
    return result


@dataclass(frozen=True, slots=True)
class SpillLimits:
    """Bound partition rows and bytes, join fan-out, output rows, and repartition depth."""

    max_partition_rows: int = 100_000
    max_partition_bytes: int = 64 * 1024 * 1024
    max_matches_per_key: int = 100_000
    max_output_rows: int = 1_000_000
    max_repartition_depth: int = 3

    def __post_init__(self) -> None:
        """Validate and normalize every configured spill limit on construction."""
        object.__setattr__(
            self,
            "max_partition_rows",
            _positive_integer("max_partition_rows", self.max_partition_rows),
        )
        object.__setattr__(
            self,
            "max_partition_bytes",
            _positive_integer("max_partition_bytes", self.max_partition_bytes),
        )
        object.__setattr__(
            self,
            "max_matches_per_key",
            _positive_integer("max_matches_per_key", self.max_matches_per_key),
        )
        object.__setattr__(
            self,
            "max_output_rows",
            _positive_integer("max_output_rows", self.max_output_rows),
        )
        object.__setattr__(
            self,
            "max_repartition_depth",
            _nonnegative_integer("max_repartition_depth", self.max_repartition_depth),
        )


def raise_spill_limit(
    operation: str,
    measurement: str,
    actual: int,
    field: str,
    allowed: int,
    *,
    depth: int | None = None,
) -> None:
    """Raise a BufferLimitError containing the measured value and configured budget."""
    depth_text = "" if depth is None else f" after {depth} repartition levels"
    raise BufferLimitError(
        f"{operation} {measurement} {actual} exceed {field}={allowed}{depth_text}"
    )


class SpillBudget:
    """Track join expansion without retaining emitted records."""

    __slots__ = ("_limits", "_operation", "outputs")

    def __init__(self, operation: str, limits: SpillLimits) -> None:
        """Start an output counter governed by the supplied spill limits."""
        self._operation = operation
        self._limits = limits
        self.outputs = 0

    def check_matches(self, count: int) -> None:
        """Reject a join key whose match fan-out exceeds the configured maximum."""
        if count > self._limits.max_matches_per_key:
            raise_spill_limit(
                self._operation,
                "matches for one key",
                count,
                "max_matches_per_key",
                self._limits.max_matches_per_key,
            )

    def add_output(self) -> None:
        """Reserve one output row, raising before the total exceeds its budget."""
        output = self.outputs + 1
        if output > self._limits.max_output_rows:
            raise_spill_limit(
                self._operation,
                "output rows",
                output,
                "max_output_rows",
                self._limits.max_output_rows,
            )
        self.outputs = output
