"""Deferred in-memory and partitioned grouped aggregation."""

from __future__ import annotations

import os
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Generic, TypeAlias, TypeVar

from ..collecting.aggregation import (
    Aggregator,
    finish_aggregations,
    initialize_aggregations,
    prepare_aggregations,
    step_aggregations,
)
from ..errors import DuplicateKeyError
from ..expressions.selectors import Selector, compile_selector
from ..streams.flow import flow
from .spill import spilled_group_aggregate, validate_partitions
from .spill_limits import SpillLimits

if TYPE_CHECKING:
    from .rows import Rows

T = TypeVar("T")
JoinSelector: TypeAlias = Selector | tuple[Selector, ...]


class GroupedRows(Generic[T]):
    """A deferred grouping that chooses in-memory or partitioned aggregation."""

    __slots__ = ("_keys", "_limits", "_partitions", "_rows", "_tempdir")

    def __init__(
        self,
        source: Rows[T],
        keys: tuple[tuple[str, Selector], ...],
        *,
        partitions: int | None = None,
        tempdir: str | os.PathLike[str] | None = None,
        limits: SpillLimits | None = None,
    ) -> None:
        self._rows = source
        self._keys = keys
        self._partitions = partitions
        self._tempdir = tempdir
        self._limits = limits

    def spill(
        self,
        partitions: int = 32,
        *,
        tempdir: str | os.PathLike[str] | None = None,
        limits: SpillLimits | None = None,
    ) -> GroupedRows[T]:
        """Configure partitioned temporary storage for grouped aggregation.

        Args:
            partitions: The number of spill partitions used for bounded-memory processing.
            tempdir: The directory used for temporary spill files.
            limits: Finite partition, state, match, and output budgets.

        Returns:
            A new lazy `Rows` pipeline representing this operation.
        """
        return GroupedRows(
            self._rows,
            self._keys,
            partitions=validate_partitions(partitions),
            tempdir=tempdir,
            limits=limits or SpillLimits(),
        )

    def aggregate(self, **aggregations: Aggregator) -> Rows[dict[str, Any]]:
        """Compute named aggregations independently for each group.

        Grouping and aggregation remain deferred until the returned `Rows` pipeline is consumed.

        Args:
            **aggregations: Named aggregators evaluated during the same traversal.

        Returns:
            A lazy row pipeline containing one aggregate record per group.
        """
        aggregation_items = prepare_aggregations(aggregations)
        key_names = tuple(name for name, _selector in self._keys)
        keys = tuple(compile_selector(selector) for _name, selector in self._keys)
        multiple_keys = len(keys) > 1
        overlap = set(key_names) & aggregations.keys()
        if overlap:
            name = next(name for name in key_names if name in overlap)
            raise DuplicateKeyError(f"aggregate output column {name!r} collides with a group key")

        def evaluate() -> Iterator[dict[str, Any]]:
            if self._partitions is not None:
                yield from spilled_group_aggregate(
                    self._rows,
                    key_names=key_names,
                    keys=keys,
                    aggregation_items=aggregation_items,
                    partitions=self._partitions,
                    tempdir=self._tempdir,
                    limits=self._limits or SpillLimits(),
                )
                return
            groups: dict[Any, dict[str, Any]] = {}
            for row in self._rows:
                key = tuple(select(row) for select in keys) if multiple_keys else keys[0](row)
                try:
                    states = groups[key]
                except KeyError:
                    states = initialize_aggregations(aggregation_items)
                    groups[key] = states
                except TypeError:
                    raise TypeError("group_by keys must be hashable") from None
                step_aggregations(states, aggregation_items, row)

            for key, states in groups.items():
                key_values = key if multiple_keys else (key,)
                result = dict(zip(key_names, key_values, strict=True))
                result.update(finish_aggregations(states, aggregation_items))
                yield result

        from .rows import Rows

        return Rows(flow.defer(evaluate))
