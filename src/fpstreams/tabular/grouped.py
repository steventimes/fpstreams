"""Deferred in-memory and partitioned grouped aggregation."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, Generic, TypeAlias, TypeVar

from ..collecting.aggregation import (
    Aggregator,
    prepare_aggregations,
)
from ..errors import DuplicateKeyError
from ..expressions.selectors import Selector
from ..planning.logical import GroupAggregateNode, GroupAggregateSpec
from ..streams.flow import Flow
from .spill import validate_partitions
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
        """Store a deferred grouping configuration without consuming the row source."""
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
        """Return grouping configured to aggregate through bounded temporary partitions.

        Args:
            partitions: Hash-partition count from 2 through 256.
            tempdir: Parent directory for automatically removed spill files.
            limits: Partition, group-state, output, and repartition budgets.

        Returns:
            A new GroupedRows configuration; call aggregate() to obtain a lazy pipeline.
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
        overlap = set(key_names) & aggregations.keys()
        if overlap:
            name = next(name for name in key_names if name in overlap)
            raise DuplicateKeyError(f"aggregate output column {name!r} collides with a group key")

        from .rows import Rows

        logical = self._rows._flow._logical_plan
        return Rows(
            Flow._from_logical(
                logical.with_root(
                    GroupAggregateNode(
                        logical.root,
                        GroupAggregateSpec(
                            self._keys,
                            aggregation_items,
                            self._partitions,
                            self._tempdir,
                            self._limits,
                        ),
                    )
                )
            )
        )
