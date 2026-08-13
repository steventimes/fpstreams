# v2 status and roadmap

fpstreams 2 replaces the v1 implementation. The current stable version is `2.0.0`.

## Included in 2.0

- Domain-oriented package layout with small compatibility facades.
- Lazy `Flow`, `AsyncFlow`, `Rows`, and `Pairs` APIs.
- Placeholder and row expression systems.
- Collectors, named aggregators, and grouped aggregation.
- Fused synchronous and asynchronous Python execution.
- Native Rust numeric plans plus automatic hybrid planning.
- Bounded concurrent async mapping, merge operations, timeouts, debounce, and
  time-windowed buffering.
- CSV, JSONL, SQLite, DB-API, Arrow, Parquet, pandas, and Polars adapters.
- External sorting and partitioned spill paths for joins and grouping.
- One-shot source enforcement and deterministic resource cleanup.
- Strict typing, Python/Rust parity tests, and wheel/sdist packaging.

## Stability commitments

### Freeze public behavior

Public names, signatures, exceptions, source-consumption rules, and engine fallback
behavior are stable within the v2 release line. Breaking changes belong in a future
major release. v2 does not add an alias for every v1 method.

### Add native operations only with parity tests

Add native operations only when they preserve Python ordering, equality,
overflow, error, and cleanup semantics. Every new kernel needs Python/native
parity tests and an explicit unsupported path.

### Make spill behavior easier to inspect

Improve spill diagnostics, partition selection, and observability for external
sorts, joins, and grouped aggregation. Materializing operations should remain
obvious in API documentation and plan explanations.

### Keep adapters current

Keep Arrow as the preferred columnar interchange path and validate adapter
behavior across supported pandas, PyArrow, and Polars releases. Third-party data
packages remain optional dependencies.

## Possible work after 2.0

- Additional streaming joins and merge operations for already-sorted inputs.
- More approximate or bounded statistical aggregators.
- Richer plan diagnostics and structured execution metrics.
- Additional `Option`/`Result` traversal helpers.
- Narrow extension hooks for custom sources, aggregators, and native kernels.

These ideas stay out of 2.0 unless they can be added without weakening API
consistency, memory bounds, or Python/Rust parity.

## What fpstreams will not do

fpstreams will not replace NumPy, pandas, Polars, or a distributed query engine.
It is a pipeline layer that passes data to those systems when appropriate. Ordinary
transforms are not distributed automatically, and unbounded inputs are not silently
materialized.

## Release validation

The v2 release gate covers:

- no known Python/native semantic divergence in supported plans;
- clean tests, lint, strict typing, Rust formatting, clippy, and package builds;
- documented behavior for one-shot sources, cancellation, spilling, and fallbacks;
- successful installation and import from built wheels;
- a migration path for supported v1 entry-point aliases.
