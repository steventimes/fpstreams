"""Compatibility facade for named aggregations."""

from .collecting.aggregation import (
    AggregationItems,
    Aggregator,
    NativeAggregateSnapshot,
    NativeAggregation,
    NativeAggregationKind,
    agg,
    finish_aggregations,
    finish_native_aggregations,
    initialize_aggregations,
    native_aggregation_items,
    native_first_only,
    prepare_aggregations,
    run_aggregations,
    step_aggregations,
)

__all__ = [
    "AggregationItems",
    "Aggregator",
    "NativeAggregateSnapshot",
    "NativeAggregation",
    "NativeAggregationKind",
    "agg",
    "finish_aggregations",
    "finish_native_aggregations",
    "initialize_aggregations",
    "native_aggregation_items",
    "native_first_only",
    "prepare_aggregations",
    "run_aggregations",
    "step_aggregations",
]
