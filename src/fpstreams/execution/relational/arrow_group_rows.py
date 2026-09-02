"""Arrow group-result shaping isolated from relational dispatch."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

from ...physical.relational import ArrowGroupAggregateSpec

LaneArrays = Callable[[Any, tuple[str, ...], list[str], Any, Any], list[Any]]
LaneLists = Callable[
    [Any, tuple[str, ...], list[str], Any, Any | None],
    list[list[Any]],
]
OrderedRows = Callable[
    [list[Any], list[list[Any]], ArrowGroupAggregateSpec, str, list[bool]],
    list[dict[str, Any]],
]


def _arrow_group_lane_columns(
    table: Any,
    names: tuple[str, ...],
    spec: ArrowGroupAggregateSpec,
    types: Any,
) -> list[Any | None] | None:
    """Preflight every selected value before any Arrow compute kernel runs."""
    lane_columns: list[Any | None] = []
    for lane in spec.lanes:
        if lane.kind == "count":
            lane_columns.append(None)
            continue
        value_field = lane.value_field
        if value_field is None or names.count(value_field) != 1:
            return None
        values = table.column(names.index(value_field))
        if not types.is_int64(values.type) or values.null_count:
            return None
        lane_columns.append(values)
    return lane_columns


def _arrow_group_lane_lists(
    grouped: Any,
    grouped_names: tuple[str, ...],
    aggregate_fields: list[str],
    pc: Any,
    order: Any | None = None,
) -> list[list[Any]]:
    """Materialize each distinct aggregate field once, optionally after Arrow reordering."""
    materialized: dict[str, list[Any]] = {}
    lanes: list[list[Any]] = []
    for field in aggregate_fields:
        values = materialized.get(field)
        if values is None:
            column = grouped.column(grouped_names.index(field))
            if order is not None:
                column = pc.take(column, order)
            values = column.to_pylist()
            materialized[field] = values
        lanes.append(values)
    return lanes


def _arrow_group_lane_arrays(
    grouped: Any,
    grouped_names: tuple[str, ...],
    aggregate_fields: list[str],
    pc: Any,
    order: Any,
) -> list[Any]:
    """Reorder each distinct aggregate field once while retaining Arrow arrays."""
    materialized: dict[str, Any] = {}
    lanes: list[Any] = []
    for field in aggregate_fields:
        values = materialized.get(field)
        if values is None:
            values = pc.take(grouped.column(grouped_names.index(field)), order)
            materialized[field] = values
        lanes.append(values)
    return lanes


def _ordered_arrow_group_rows(
    keys: list[Any],
    lanes: list[list[Any]],
    spec: ArrowGroupAggregateSpec,
    key_name: str,
    wide_sums: list[bool],
) -> list[dict[str, Any]]:
    """Build canonical rows from already first-seen-ordered scalar columns."""
    result: list[dict[str, Any]] = []
    for position, key in enumerate(keys):
        row: dict[str, Any] = {key_name: key}
        for lane, values, wide_sum in zip(spec.lanes, lanes, wide_sums, strict=True):
            value = values[position]
            row[lane.output_name] = int(value) if wide_sum else value
        result.append(row)
    return result


def _materialize_arrow_group_rows(
    grouped: Any,
    grouped_names: tuple[str, ...],
    aggregate_fields: list[str],
    encounter_order: Any,
    spec: ArrowGroupAggregateSpec,
    key_name: str,
    wide_sums: list[bool],
    pa: Any,
    types: Any,
    pc: Any,
    *,
    table_min_groups: int,
    take_min_groups: int,
    lane_arrays: LaneArrays,
    lane_lists: LaneLists,
    ordered_rows: OrderedRows,
) -> list[dict[str, Any]] | None:
    """Restore first-seen order and batch-box sufficiently large grouped outputs."""
    key_field = "__fpstreams_group_key"
    grouped_key_values = grouped.column(grouped_names.index(key_field))
    if (
        grouped.num_rows >= table_min_groups
        and not types.is_dictionary(grouped_key_values.type)
        and not any(wide_sums)
        and type(key_name) is str
        and all(type(lane.output_name) is str for lane in spec.lanes)
    ):
        order = pc.index_in(encounter_order, value_set=grouped_key_values)
        if order.null_count:
            return None
        lanes = lane_arrays(
            grouped,
            grouped_names,
            aggregate_fields,
            pc,
            order,
        )
        rows = cast(
            list[dict[str, Any]],
            pa.Table.from_arrays(
                [encounter_order, *lanes],
                names=[key_name, *(lane.output_name for lane in spec.lanes)],
            ).to_pylist(),
        )
        return rows

    if grouped.num_rows >= take_min_groups and not types.is_dictionary(grouped_key_values.type):
        order = pc.index_in(encounter_order, value_set=grouped_key_values)
        if order.null_count:
            return None
        keys = encounter_order.to_pylist()
        lanes = lane_lists(
            grouped,
            grouped_names,
            aggregate_fields,
            pc,
            order,
        )
        return ordered_rows(keys, lanes, spec, key_name, wide_sums)

    grouped_keys = grouped_key_values.to_pylist()
    grouped_lanes = lane_lists(
        grouped,
        grouped_names,
        aggregate_fields,
        pc,
        None,
    )
    rows_by_key: dict[Any, dict[str, Any]] = {}
    for group_position, key in enumerate(grouped_keys):
        row: dict[str, Any] = {key_name: key}
        for lane, values, wide_sum in zip(
            spec.lanes,
            grouped_lanes,
            wide_sums,
            strict=True,
        ):
            value = values[group_position]
            row[lane.output_name] = int(value) if wide_sum else value
        rows_by_key[key] = row
    return [rows_by_key[key] for key in encounter_order.to_pylist()]
