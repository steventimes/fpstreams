"""Compatibility facade for streaming collectors."""

from .collecting.collector import (
    Collector,
    CollectorItems,
    Collectors,
    SummaryStatistics,
    collectors_done,
    finish_collectors,
    initialize_collectors,
    prepare_collectors,
    run_collectors,
    step_collectors,
)

__all__ = [
    "Collector",
    "CollectorItems",
    "Collectors",
    "SummaryStatistics",
    "collectors_done",
    "finish_collectors",
    "initialize_collectors",
    "prepare_collectors",
    "run_collectors",
    "step_collectors",
]
