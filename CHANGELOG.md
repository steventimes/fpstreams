# Changelog

This file records user-visible and compatibility-relevant changes in fpstreams 2,
including changed defaults.

## 2.1.0 - 2026-09-01

### Added

- `flow()` can enter record operations directly, while `Rows` remains available
  as an explicit relational view. New column and NumPy factories make it possible
  to keep columnar inputs columnar until execution.
- `ExecutionReport` and `run_with_report()` expose the strategy used by a terminal
  without changing the terminal result.
- Standard `__arrow_c_stream__` and `__dataframe__` providers can be routed through
  `flow()`.
- Arrow-backed CSV scanning supports typed incremental reads and query projection
  under PyArrow's parsing and error contract.
- `AsyncFlow` now includes queue sources, bounded prefetch, session windows, numeric
  terminals, and execution reports.
- `Pairs` accepts explicit engine selection and row expressions for pair filtering.

### Changed

- Retained NumPy matrices can execute guarded identity, projection, filter,
  computed-column, aggregate, and grouped-aggregate paths without first building
  one Python dictionary per input row.
- Native Rust execution now handles additional scalar, pair, reshape, join,
  group, and global aggregation plans. Unsupported or data-semantics-sensitive
  cases still use the Python path.
- Record joins and grouped aggregation use narrower shape checks and bounded native
  kernels where they preserve Python ordering, identity, errors, and cleanup.
- `rows.from_csv()` and `rows.from_jsonl()` now accept caller-owned open handles
  and replayable zero-argument opener functions in addition to paths.
- The benchmark runner now compares fpstreams with Python, NumPy, and pandas and
  reports the percentage difference for each comparable case.

### Fixed

- Fast paths now revalidate cached row expressions, collector programs, NumPy
  adapters, and implementation primitives before bypassing Python execution.
- One-shot sources, iterators, async tasks, database resources, spill files, and
  retained tabular readers keep their cleanup behavior on early return and errors.
- Cleanup attempts every owned resource, preserves the operation error as primary,
  and reports independent close failures without inheriting an unrelated outer
  exception handler.
- Path and opener CSV/JSONL sources open on execution. Handles returned by an
  opener are closed by fpstreams; caller-owned handles remain open. Arrow C
  streams, explicit column mappings, and NumPy inputs keep their documented
  construction-time import or conversion behavior.

## 2.0.0

fpstreams 2 replaced the v1 implementation with typed lazy plans, a primary
`Flow` API, explicit `Rows`, `AsyncFlow`, and `Pairs` views, and optional Rust and
Arrow execution.
