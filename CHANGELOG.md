# Changelog

This file records user-visible and compatibility-relevant changes in fpstreams 2,
including changed defaults.

## Unreleased

## 2.4.1 - 2026-10-18

### Changed

- Separate relational candidate recognition and tree compilation from the general
  compiler. Native grouping, Collector grouping, and synchronous terminal helpers
  now live in domain modules; public interfaces and execution rules stay the same.

## 2.4.0 - 2026-10-18

### Added

- `ExecutionReport.to_dict()` and `ExecutionEvent.to_dict()` export plain
  metadata. Report and plan dictionaries carry an independent schema version;
  reports include metric units, query-owned resource scope, and event limits.

- `Flow.collapse(predicate, collector)` and its async equivalent reduce each
  adjacent run with existing Collector state. Completion stops further steps
  within a run while predicates continue to find its boundary. Collector hooks
  remain synchronous, and awaitable finisher results are emitted as data.

- `Flow.window_reduce(size, combine, step=1)` and `AsyncFlow.window_reduce(...)`
  reuse ordered partial aggregates across count-based windows. The synchronous
  combine function must be associative and nonmutating. Cached entries are
  bounded by `size + 1`; byte sizes and individual stack transfers are not.
  Nonoverlapping windows use a running fold without retaining a raw window buffer.

- `flow.unfold(seed, function)` and `aflow.unfold(...)` generate values lazily
  from `(value, next_state)` transitions until the callback returns `None`.
  Class constructors expose the same operation; async transitions may be awaitable.
- `Flow.map_accum(initial, function)` and `AsyncFlow.map_accum(...)` thread
  state through ordered callbacks returning `(next_state, output)`, emitting
  one output per input through the existing gather/scan execution paths.
- `agg.min_by(selector)` and `agg.max_by(selector)` retain a whole winning
  item in one pass. Ties keep the first item and empty input returns `None`;
  the factories reuse existing grouped, async, and spill aggregation.
- `agg.top(count, key=...)` and `agg.bottom(count, key=...)` retain a bounded
  list of whole items per group, ordered by key with stable ties. They compose
  with the existing global, async, sorted, and spill aggregation paths.
- `run_with_report()` includes up to 64 immutable plan and route events in
  `report.events`, with `events_truncated` marking further observations. Child
  plans keep their own labels without replacing the outer route. Flow, Rows,
  Pairs, and AsyncFlow use the same recorder.
- `BoundedExecutor(executor, max_pending=...)` shares one unfinished-task budget
  across independent queries. It accepts existing thread or process pools,
  releases capacity on completion or cancellation, and keeps the borrowed pool open.
- `FileCheckpoint(directory, version=...)` stores verified outputs for stable
  file partitions. Each successful callback publishes an output file and its
  metadata together; later runs verify input and output hashes before reuse.

### Changed

- `filter_map()` avoids intermediate tuple adaptation in synchronous pipelines
  and the extra predicate callback in async serial fusion. It calls the mapper
  once per input and keeps every result except `None`.
- Proven primitive Arrow comparison chains can share a selection mask,
  filtering payload once or counting directly from the mask. Offline Optuna
  tuning favored nonempty batches with at least one column in the measured shapes.
- Spill decoding releases encoded buffers before yielding decoded records
  and releases completed decoded frames before reading the next frame.

- `Collectors.head()` and `tail()` accept integer-index objects, with matching
  `SupportsIndex` annotations. Partitioned reducer execution and reducer-law
  layouts use the same integer protocol.
- `Collectors.teeing()` can combine partial states when both downstream
  collectors provide combiners. It keeps their live callbacks and carries no
  inferred reducer laws or native metadata.
- Named `aggregate()` and `Pairs.aggregate_values()` accept any `Collector`,
  including composed `Collectors` and existing `agg` factory results. Global,
  async, grouped, sorted, and spill paths share the existing collector lifecycle.
- Multi-collector Python aggregation reuses its state-list references within
  each row while retaining live callbacks and their declaration order.
- Retained primitive Arrow tables and record batches can keep their native
  buffers when `arrow_batches(schema=...)` receives the matching output schema,
  including metadata. Safe direct-field projections and primitive filters can
  execute columnarly. Casts, Python callbacks, and unsupported sources retain
  the existing conversion path.
- Arrow execution accepts chains of safe direct comparisons followed by a
  direct projection. Parquet keeps only the leading comparison as an automatic
  scan hint; all filters remain in the ordered residual program.
- Default local Parquet scans with one fragment can omit safe unused fields
  from validation scans, retaining query dependencies and fields whose Python
  conversion can fail. Sources with possible scanner casts retain the full
  caller-visible projection before automatic filtering and projection.

### Fixed

- Query-wide task/file limits and task-scope limits reject noninteger values,
  including NaN, before resource acquisition. Integer-index values are
  normalized once during construction.

- `Collectors.first()` and `head(count)` preserve completed state when used
  inside `grouping_by` or `partitioning_by`. Previously, `first()` returned the
  last group item and `head()` could retain the whole group, even with a zero
  count. Grouping adapters continue to read all input and step custom collectors.
- `Flow` and `AsyncFlow` validate `take`, `limit`, `drop`, and `skip` counts
  through the integer index protocol before building the operation. Noninteger
  values, including NaN and infinity, raise `TypeError` before opening the source.
  This prevents async prefix operations from silently rounding counts or losing
  their limit.
- `Flow.collect()` and `AsyncFlow.collect()` preserve a single streaming
  collector's finished result type in type checkers. Named collectors return
  a dictionary, while optional or dynamically forwarded arguments retain the
  general result union. Runtime dispatch and validation are unchanged.
- `Collectors.head()` and `tail()` reject floats, including NaN and infinity,
  at construction. Partitioned reducer execution validates `partition_size`
  before creating state or opening input, preventing non-integer values from
  silently removing partition boundaries.
- Partitioned reducer execution closes its input iterator on exhaustion and
  failure, before merging states or finishing. Cleanup failures preserve an
  active source or callback exception.
- Collector programs reject missing combiners before invoking any merge
  callback, preventing avoidable partial mutations on invalid merge requests.
- Reducer law checks build independent states for each merge expression,
  preventing in-place combiners from producing false identity or associativity
  results. The input iterator closes before the examples are replayed.
- Unsafe Arrow batches evaluate filters in row order and stop when downstream
  consumers stop, preserving Python exception order and early-exit behavior.
- Arrow prefix fallbacks reuse converted rows, so custom extension scalars
  cross the Python conversion boundary once per input batch.
- Automatic Parquet hints preserve conversion errors in discarded fields and
  rows, including casts across fragments and supplied dataset schemas.
  Types whose Python conversion can fail are validated before automatic
  filtering or projection; explicit source `columns` and `filter` still define
  which values the source exposes.
- `scan_csv()` validates every caller-visible column before query projection,
  preserving conversion errors in later CSV blocks. It opens the typed source
  once; explicit source conversion options still control included columns.
- An explicit zero-column Arrow schema preserves the input row count in
  `arrow_batches()` and `to_arrow()`, including partial final batches.
- Polars batch exports preserve the height of zero-column inputs. Parquet
  writes reject nonempty rows without columns, preserving an existing target
  instead of publishing a file that loses the input row count.

## 2.3.0 - 2026-10-15

### Fixed

- Async callback return types resolve correctly for `AsyncFlow.map()`,
  `map_async()`, and `map_batches()`. Mixed immediate and awaitable returns
  retain the resolved element type.

### Changed

- Supported single-stage Python `with_columns()` and `select()` queries reuse a
  loop template with separate callback bindings for each execution, reducing
  fixed setup cost while retaining computed-column live selectors and projection
  snapshots for each row.
- Explicit sorted grouping with one key avoids a temporary selector generator
  and wrapper tuple per row while preserving live callbacks, key validation,
  lookahead, and callback error behavior.

### Added

- `Flow.map_batches(function, size=...)` and `AsyncFlow.map_batches(...)` call a
  mapper once per tuple batch, including a shorter final batch. Each call emits
  one result. Sync batches inherit `parallel()` settings; async batches support
  concurrency, ordering, per-batch timeouts, and bounded buffering.
- `Flow.map_parallel()` and `Flow.parallel()` accept a caller-owned `executor`.
  Queries cancel their queued work and wait for their own running mappers while
  leaving the shared pool open. Existing worker/backend defaults remain in place
  when no executor is supplied.
- Streaming analytics examples show generator input, valid weighted denominators,
  parameterized SQLite selection, and atomic writes across multiple batches.

## 2.2.1 - 2026-10-07

### Fixed

- Editors resolve the public `rows` factory directly, preserving method completion,
  parameter hints, and docstrings instead of treating it as the record module.
- Core `flow()` and `rows()` factories retain iterable element types without optional
  data adapters installed. A shared series protocol preserves one-dimensional series
  typing without importing Polars into the core type declarations.

### Changed

- Automatic planning skips NumPy buffer probes for exact list, tuple, and range
  sources. Python identity terminal plans on exact containers also avoid a temporary
  decision object, reducing the fixed cost of small workloads.

## 2.2.0 - 2026-10-06

### Added

- Add bounded `Rows.join_sorted()` for explicitly ordered records, with consumed-prefix
  validation and first-right-record schema rules.
- Report explicit sorted group, merge, and join execution through existing report fields.
- Add optional atomic path output to Flow CSV/JSON/JSONL and Rows CSV/JSONL sinks,
  including no-overwrite publication. Existing direct output remains the default.
- `Flow.to_jsonl()` streams arbitrary JSON values as one value per line.
  `Rows.to_jsonl()` now accepts `default` for custom serialization.

- `Flow.merge_sorted()` stably merges two ascending inputs without sorting or
  materializing them. Ties prefer the left input and outputs retain their identity.

- `Rows.group_by_sorted()` aggregates adjacent ascending keys in Python with
  current group state and one lookahead row. Consumed keys are checked for exact
  builtin types and order; this explicit mode does not sort or support spill.

- `Pairs.run_with_report()` executes a pair terminal once and returns its value
  with an execution report. It supports `to_dict`, `group_values`,
  `collect_values`, and `aggregate_values`.

### Fixed

- Rust source builds support the declared 1.85 minimum. Native kernels no longer
  require Rust 1.88 through let-chain syntax; CI checks the minimum compiler.
- SQLite sinks match existing table, view, and column names using SQLite's ASCII
  case rules. Duplicate column aliases raise `DuplicateKeyError` before inserts.
  Fail mode and view rejection check the destination before reading rows.
- Sync and async flows accept built-in ranges longer than `sys.maxsize`. Bounded
  reads and exact counts no longer fail during source size detection.

- `spreadsheet_safe=True` also neutralizes formula-like CSV headers, including
  inferred record field names and explicitly named empty output. Record lookup
  still uses the original names; the raw-output default is unchanged.
- Relative Parquet output paths stay anchored to the initial working directory
  if a source or conversion callback changes directories during the write.

- Scalar keys in explicit sorted operations avoid temporary shape tuples while
  retaining per-row type and ordering checks.

- Atomic CSV and JSON output keeps its original destination when a source or
  serializer changes the working directory during execution.

- CSV and SQLite record sinks propagate `StopIteration` from first-record
  conversion instead of treating it as empty input. SQLite replacement leaves
  the existing table intact when that conversion fails.

- Sync and async `chunk`, `window`, and `batch_by_size` validate integer bounds
  before execution, including their aliases. Floats, NaN, and infinity now raise
  `TypeError` instead of failing after source consumption or leaving async batches
  unbounded. Objects implementing `__index__` are normalized once per bound.

- `unique()`, `unique_by()`, and `Pairs.unique_keys()` propagate equality errors
  instead of treating them as unhashable keys. Hash callbacks keep their existing
  lookup and insertion counts.
- Async uniqueness, `agg.count_distinct()`, and native pair-uniqueness continuation
  also propagate equality errors without extra hash calls. Iterator cleanup retains
  nested exception notes when several owned resources fail to close.
- Distinct operations avoid key-wrapper allocations for exact built-in strings.
  String subclasses and custom keys retain guarded hash and equality handling.
- Grouped reductions and frequency counts propagate `KeyError` raised by a key's
  hash or equality method instead of treating it as a missing group. This includes
  async reductions, grouping collectors, Pairs, and spilled grouping.
- Parquet `if_exists="error"` publishes with an atomic no-overwrite hard link.
  A concurrent creator or dangling symlink cannot be overwritten. Filesystems
  without hard-link support report an error; `replace` still uses atomic rename.
- Owned Arrow resources report close failures on successful queries and attach
  cleanup diagnostics to an existing query error. Cleanup attempts every owned
  resource. This also changes Arrow `first()`, which previously ignored close errors.

- Engine benchmarks calibrate warmed timing blocks for short tasks and first-row
  latency, retaining elapsed time and call counts in their reports. Rebuild
  single-call baselines before comparing. Timing and resource regression limits
  are unchanged; scheduled CI now uploads the three raw reference reports.

- Benchmark report schema 6 records Python and glibc allocator environment
  settings. Baseline creation and comparison reject missing or mismatched
  settings; regenerate older reports. The runners do not change allocator defaults.

- NumPy frequency benchmarks check key types, integer counts, first-key order,
  and floating-point bit patterns. Separate NaN entries, their signs, and their
  payloads remain distinct when comparing independently computed outputs.
- Generated Python `with_columns` loops call the live enrichment function.
  Changes to captured accessors, expression evaluators, or the selector list
  remain visible after plan caching and while reading the source. The transform
  still copies the row before invoking selectors against the original input.

- Arrow file projections keep selector changes made by custom scan openers
  visible. Parquet rechecks the projection after creating its dataset, including
  with an explicit source filter. CSV keeps full fields when its public reader
  hooks differ from the extension entrypoints. Normal CSV projection retains
  the bounded schema probe and the default data reader.
- `Rows.select()` rechecks captured field accessors before using Rust, NumPy,
  or Arrow projection metadata. The generated Python loop calls the live
  projection, so changes made during input reads remain visible. Custom Arrow
  batch sources keep fields available for fallback; changed projections can
  infer their output dtype instead of forcing the former field's schema.
  NumPy fallback finishes the already-opened source without reopening it.
- Pivot calls the live index, column, and value selectors on its Python path.
  Direct dict lookups had ignored changes to selector code, closure bindings,
  and globals. Native admission now checks those bindings before execution;
  its fallback shares the general selector loop.
- Native pivot falls back when Rows/Flow iteration or source hooks change,
  including changes to function code. Replacement results, exceptions, and
  iterator cleanup remain observable.
- `frequencies(key=...)` uses the Python pipeline for sequential `auto` plans,
  so key callbacks can affect subsequent input reads. Native materialization
  could hide those changes. Reports identify this route as `python_frequency`.
- Benchmark speedup requirements for composite count/sum groups and NamedTuple
  callable joins now apply from 1,000 rows. Smaller inputs still emit timing and
  allocation data for cross-run checks. The original ratios and CI workloads
  are unchanged; small runs no longer inherit speedup requirements validated
  for larger inputs.
- The engine benchmark CLI lists scenarios without running their tasks, timing,
  allocation tracking, or execution observation. Listing and measurement share
  filtering and fixture cleanup, including when a later scenario builder fails.
- Single-collector grouping preserves the general collector program's state
  release order, including when output is closed early. Unused input keys are
  released before reading the current finisher, so changes made by their release
  callbacks take effect.
- Single-collector grouping uses the current step after truth-testing a custom
  completion result. It also releases replaced completion values before pulling
  another row, matching the general collector program's callback behavior.
- Python grouping calls the live key selector for each row. A field shortcut
  could keep using an old field after a source or callback changed the selector's
  code or closure, merging distinct groups. Selector globals and error types
  now follow the same function calls as general grouped aggregation.
- Scalar caches no longer confuse numerically equal constants of different
  types. Directly constructed expressions retain their Python result types,
  wide integer values, and custom constant identities. Expressions with
  nonstandard operand types use Python in `auto` mode; forcing `native` raises
  `NativeUnsupportedError` instead of changing their numeric representation.
- Float expressions retain the sign of `-0.0` in their instructions and evaluator
  cache. Compiling a positive-zero expression first no longer changes a later
  negative-zero result. Scalar and Pairs execution keep their existing routes.
- Python grouped sums call the live collector lifecycle throughout traversal
  and output. Changes to function code or selector closure cells made by a source,
  key callback, or output consumer are no longer hidden by an inlined sum loop.
- Two-key count/sum groups also use the live collector program. Their separate
  loop skipped changes to initializer and step code or the sum selector closure
  made while reading the source, even though the same collectors worked correctly
  in other group layouts.
- Grouped aggregation uses the general collector program for custom lifecycle
  getters. A dynamic `step` property or `__getattribute__` override is read for
  each step instead of being cached as a fixed function.
- Single-collector grouping no longer caches a temporary lifecycle hook between
  key selection and lookup. If hashing replaces that hook, its release callback
  runs before the next hash call, matching the general collector program.
- Exact builtin checks no longer use custom metaclass equality to infer source
  size or select grouping, spill, range lookup, and float-expression shortcuts.
  Custom iterables are counted by traversal; map/filter fusion does not request
  their length hints. Grouping retains custom key and serialization calls.
- Execution reports distinguish successful top-level Rust and Arrow record
  joins from Python joins. Native pair aggregation also records its direct route.
- Benchmark comparisons reject missing provenance and mismatched workloads.
  Both suites record dependencies, Git state, code and workload fingerprints,
  and an untimed observation of the task. Median baselines retain each run's
  provenance. Older reports need to be regenerated for report schema 6.
- Reports include CPU affinity, NumPy CPU dispatch, and an allowlist of runtime
  settings. Comparisons reject mismatched configurations; both runners also
  reject configuration changes during measurement.
- Join benchmarks honor the requested engine. Twelve Python controls separate
  dict and Mapping records, field and callable keys, and inner, left, and repeated
  matches. References preserve callable-key columns and snapshots taken before
  key selection, including unmatched left rows.
- NumPy frequency benchmarks cover integer and float arrays, with and without
  a callable key, at low and high cardinality. All references preserve first-key
  order; Python array-to-list conversion is included in its timed task.
- Competitive samples warm each task for at least 1ms after GC and record the
  number of warmup calls. One warmup call left inconsistent timings for short
  NumPy tasks in local measurements.
- Competitive benchmarks now record peak Python allocation in a separate,
  untimed call for every implementation, using the engine suite's shared helper.
  Schema 6 rejects missing or invalid resource measurements, and baseline
  creation rejects inconsistent resource sets instead of substituting zero.
  Earlier competitive reports contain no allocation evidence.
- Regression checks accept zero allocated bytes when both runs report zero.
- `frequencies()` on retained lists and tuples falls back to Python when the
  optional Rust extension is unavailable, including in the browser wheel.
- Frequency benchmarks use bulk conversion for NumPy dictionaries and pandas'
  `to_dict()`, avoiding per-key normalization overhead in the reference tasks.

### Changed

- Automatic NumPy `frequencies()` without a key can count a selected native
  stream in Rust through its existing iterator. Source fallback and query cleanup
  stay in the physical executor. Custom keys return to Python before hashing;
  old wheels and free-threaded CPython retain the prior counting path.
- Python `Rows.select()` keeps its per-row selector snapshot as a list, avoiding
  an intermediate tuple conversion. Selector edits still affect subsequent rows,
  and replaced selectors stay alive until the current row finishes.

- Python scalar iteration over an exact NumPy ndarray checks its live size
  without creating a shape tuple for every value. Dimension and length checks,
  lazy reads, and the fallback for custom array objects remain in place.
- Python joins reduce layout-checking overhead for left records with more than
  four fields. Repeated private dict snapshots need no temporary field-name tuple
  or Python comparison generator. Field identity, short-circuiting, suffix keys,
  snapshot lifetimes, and the bounded layout cache retain their existing behavior.
- Two-key count/sum groups can run in Rust with `engine="auto"` when a retained
  list or tuple contains exact tuple rows and both keys and the selected values
  are plain signed 64-bit integers. The path preserves first-key identities,
  encounter order, and wider sums. Changed functions, one-shot sources, and
  unsupported types use the Python collector program; older extensions can
  decline the optional entry point.
- `frequencies()` can continue counting in Rust after its bounded integer
  prefix. The continuation supports exact builtin integer, boolean, string,
  bytes, float, and `None` keys in retained lists and tuples on GIL-enabled
  CPython. It updates the output dictionary directly and hands custom keys
  back to Python before calling their hash or equality methods.
- Frequency execution reports record a completed native count as `rust_direct`
  and a count completed by Python after a native prefix as `python_frequency`.

### Internal

- Browser wheels record checkout identity and working-tree state. Release labels
  require a clean checkout matching the version tag; dirty or unknown builds
  remain development builds. The playground displays this provenance.
- Release smoke checks exercise paid-order aggregation and bounded async mapping
  in addition to Python/native integer sums.

- Single-collector groups write step results directly to their stored entries
  and avoid reading stored state for completed groups. Group benchmarks now
  include `agg.first()` with repeated and distinct keys.
- Group benchmarks now cover custom completion predicates with repeated and
  distinct keys, alongside collectors that cannot finish early.
- Generated scalar expressions, row expressions, and fused loops share a smaller
  AST location pass. It preserves traversal order and the existing synthetic
  source positions used in tracebacks.
- Scalar program fingerprints use fewer intermediate records while retaining
  the binary framing format, arbitrary-size integers, and float payload bits.
- Scalar fingerprint encoding reuses fixed instruction headers instead of
  rebuilding their bytes for every query. Expression reads and type checks are
  unchanged; the table retains no expressions or source data.
- Planning benchmarks separate compilation from execution for integer and float
  expressions and a callable control, using the same pipeline for each pair.
- Python grouped output uses one fewer forwarding generator. The collector
  iterator still starts lazily and retains its existing cleanup path.
- Collector lifecycle properties read their original slots directly, removing
  an extra Python wrapper. Frozen assignment, explicit replacement, and deletion
  errors are preserved. The API manifest now classifies those existing fields
  as properties; their names and constructor signatures are unchanged.
- Add Python dict-group benchmarks for field and callable selectors with repeated
  and distinct keys. Execution observations record the case's requested engine.
- Include nominal Mapping and mappingproxy field-group cases with repeated and
  distinct keys when measuring Python selector changes.
- Extend the two-key count/sum benchmarks to repeated keys. Keep the original
  direct-versus-callable timing threshold and report any regression against it.
- Move the narrow integer-key record-join ABI adapter into the existing join
  executor module. Kernel order, shape guards, and fallback behavior are unchanged.

### Documentation

- Repair split return descriptions and unresolved API references in generated
  pages. The two `partition_results()` methods now describe their success and
  exception lists separately.
- Correct the DB-API example's `batch_size` argument and the distinction between
  `Rows.skip()` and `Rows.drop()` in the API index.
- Explain the difference between a compiled outer plan and a recorded direct
  terminal route, including the limits of reports for compound queries.
- Revise the website's introductory and performance text and document the next
  benchmark and refactoring steps in the roadmap.

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
