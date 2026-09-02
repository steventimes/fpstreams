# v2 status and roadmap

fpstreams 2 replaces the v1 implementation. This source tree targets `2.1.0`.

## Included in 2.0

- Domain-oriented package layout with small compatibility facades.
- A primary synchronous `Flow` entry point, explicit relational `Rows` views,
  and lazy `AsyncFlow` and `Pairs` APIs.
- Placeholder and row expression systems.
- Collectors, named aggregators, and grouped aggregation.
- Fused synchronous and asynchronous Python execution.
- Native Rust scalar kernels and automatic Python/native planning.
- Bounded concurrent async mapping, merge operations, timeouts, debounce, and
  time-windowed buffering.
- CSV, JSONL, SQLite, DB-API, Arrow, Parquet, pandas, and Polars adapters.
- External sorting and partitioned spill paths for joins and grouping.
- One-shot source enforcement and cleanup of iterators, tasks, files, and
  connections owned by fpstreams.
- Strict typing, Python/Rust parity tests, and wheel/sdist packaging.

## 2.0 stabilization completed

- Terminal-aware execution explanations and exact-size count routing.
- Cached flat scalar-expression evaluators and linear native-prefix planning.
- Single registries for synchronous and asynchronous operation dispatch.
- Skew-aware spill repartitioning with finite partition, match, state, and output limits.
- Spreadsheet-safe CSV as an opt-in mode and bounded JSONL records by default.
- Patched development dependencies, SHA-pinned Actions, automated dependency updates,
  clean-install smoke tests for built wheels and the sdist, and SHA-256 manifests.
- Machine-readable release benchmarks and branch-coverage gates for high-risk modules.

## Included in 2.1

- Record operations available from the primary `flow()` entry point, while
  preserving `Rows` as an explicit view and compatibility namespace.
- Column and NumPy construction APIs, NumPy output, Rows concatenation, and
  standard Arrow C stream/dataframe protocol routing.
- Structured execution reports for synchronous, asynchronous, and relational
  terminals.
- Async queue sources, bounded prefetch, session windows, and numeric terminals.
- Wider guarded Rust and NumPy execution for scalar, pair, record, join, group,
  reshape, and global aggregation plans.
- Cross-library benchmark output with Python, NumPy, and pandas comparisons.

## Stability commitments

### Freeze public behavior

Starting with 2.1, public names, signatures, exceptions, source-consumption
rules, and engine fallback behavior remain compatible within the v2 release
line. A documented safety limit may be tightened only with a changelog entry
and an explicit opt-out. Other breaking changes belong in a future major
release. v2 does not add an alias for every v1 method.

### Add native operations only with parity tests

Add native operations only when they preserve Python ordering, equality,
overflow, error, and cleanup semantics. Every new kernel needs Python/native
parity tests and an explicit unsupported path.

### Keep spill behavior inspectable

Keep spill diagnostics, partition selection, and resource limits visible for
external sorts, joins, and grouped aggregation. Materializing operations should
remain obvious in API documentation and plan explanations.

### Keep adapters current

Keep Arrow as the preferred columnar interchange path and validate adapter
behavior across supported pandas, PyArrow, and Polars releases. Third-party data
packages remain optional dependencies.

## Possible work after 2.1

- Additional streaming joins and merge operations for already-sorted inputs.
- More approximate or bounded statistical aggregators.
- Richer plan diagnostics and structured execution metrics.
- Additional `Option`/`Result` traversal helpers.
- Narrow extension hooks for custom sources, aggregators, and native kernels.

These ideas are outside 2.1. Later work must preserve API consistency, memory
bounds, and Python/Rust parity. Longer-term research includes bounded/unbounded
capability typing, mergeable aggregator algebra, incremental Rows, broader Arrow
or Substrait pushdown, and an async cancel-scope redesign.

Free-threaded Python has a narrower boundary. An experimental, non-blocking job
builds the native extension on a CPython 3.14t interpreter, then runs static
auditing, targeted native snapshot stress tests, and threaded smoke checks. It
is not a release-wheel target or a claim of complete free-threaded performance
parity. Fast paths that cannot meet the free-threaded safety contract are
disabled and use their canonical fallback. Standard CPython 3.11 through 3.14
remains the release-tested matrix.

## Scope boundaries

fpstreams is a local pipeline layer. It passes columnar and dataframe work to
NumPy, pandas, Polars, or Arrow when appropriate and does not provide distributed
execution. Unbounded inputs are not silently materialized.

## Repository validation

CI checks source changes. Before building release artifacts, the publish workflow
also validates the tag, rebuilds the native extension, and runs the complete
Python and Rust test suites on Linux. Together, the repository workflows provide:

- Python/native parity tests for supported plan families;
- tests, lint, strict typing, Rust formatting, clippy, and package builds;
- contract tests for one-shot sources, cancellation, spilling, and fallbacks;
- clean-install and native/Python smoke tests for every wheel and the sdist;
- source tests on standard CPython 3.11 through 3.14, with the separate CPython
  3.14t job remaining experimental and non-blocking;
- repository and focus-module branch-coverage thresholds;
- SHA-pinned CI actions, OIDC-based PyPI publishing, and SHA-256 artifact manifests;
- a documented migration path for supported v1 entry-point aliases.
