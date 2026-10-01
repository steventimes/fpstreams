# v2 status and roadmap

The `2.1.0` release contains the features listed below. Source changes were reviewed
against the local checkout on 2026-09-30 and are not included in that release.
No release date has been assigned to the remaining candidates.

The current source adds correctness repairs, guarded native paths, and more
traceable reports. Some repairs increased Python execution costs; the local
measurements below include those regressions.

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

## Source changes since 2.1.0

These changes are in the current checkout and are listed in
[Unreleased](https://github.com/steventimes/fpstreams/blob/master/CHANGELOG.md).
They require a newer build than the published 2.1.0 wheel.

- **Errors and cleanup:** uniqueness and grouping propagate user hash/equality
  errors. Arrow reports owned-resource close failures. Parquet's
  `if_exists="error"` publishes without overwriting a concurrent creator.
- **Live callbacks:** grouping, pivot, projection, and computed columns preserve
  changes to selectors and collectors during consumption. File-scan projection
  rechecks selectors after custom openers. Exact builtin checks avoid invoking
  metaclass equality.
- **Scalar evaluation:** caches retain constant types, arbitrary-size integers,
  object identity, and signed zero. Generated expressions share their location
  pass and reuse fixed fingerprint headers.
- **Guarded native execution:** two-key integer count/sum grouping and retained
  list/tuple frequency continuation can use Rust. Unsupported inputs, changed
  functions, old extensions, and free-threaded execution keep their documented
  fallbacks. Keyed NumPy counting reads values between callbacks.
- **Reports:** Pairs adds `run_with_report()` for its four eager terminals.
  Top-level record joins distinguish Rust, Arrow, and Python routes. Reports
  describe the outer query; they do not trace every stage.
- **Benchmark evidence:** schema 6 records provenance, allocator settings, warmed
  blocks, and separate Python allocation measurements. Comparisons reject
  missing or mismatched evidence. CLI listing does not execute benchmark tasks.
- **Batch bounds:** sync and async batching normalize integer bounds before
  consumption. CSV and SQLite retain conversion failures on the first record.

The browser playground runs a pure-Python wheel built from this checkout.
Its status shows the package version, commit, working-tree state, and engine.
Only a clean checkout matching the release tag is labelled a release build.
See [browser scope](playground.md#browser-scope) and
[execution reports](user-guide/execution-reports.md).

## Local measurements and retained regressions

The following results summarize separate historical experiments, mostly at
1,000, 10,000, and 100,000 prepared inputs. Each belongs to its recorded source,
native build, workload matrix, and interpreter. They cannot be combined into a
single claim about this checkout. The raw reports, including failed comparisons,
remain in the local, ignored `artifacts/` directory; they are not distributed
with the package or used as a CI historical baseline.

| Experiment | Recorded result | Local evidence directory |
| --- | --- | --- |
| Live pivot selectors | Python direct-dict pivot took 62%–67% longer; fix retained | `artifacts/pivot-selector-review/` |
| Live group keys | mappingproxy repeated keys took 43%–53% longer; distinct keys 27%–33% longer | `artifacts/group-key-binding-review/` |
| Collector lifecycle | Correctness repairs retain callback and state-release order; some grouping regressions remain | `artifacts/lifecycle-review/`, `artifacts/group-value-state-review/` |
| Two integer keys in Rust | Supported tuple-row cases took 89%–97% less time than the corrected Python program; two baselines checked | `artifacts/composite-native-review/` |
| Scalar planning | Fixed fingerprint headers reduced repeated compilation time by 14%–16%; large-input execution stayed within about 2% | `artifacts/scalar-framing-review/` |
| Wide joins | Removing a comparison generator reduced wide-dict times by 20%–33%; avoiding a temporary tuple then reduced them a further 7%–11% | `artifacts/join-layout-review/`, `artifacts/join-overhead-review/` |
| Keyed NumPy frequency reads | Correct live reads took 1.60–2.11 times as long as the old materializing route; later size checks recovered about 5%–7% | `artifacts/pivot-boundary-review/`, `artifacts/numpy-scalar-review/` |
| Unkeyed NumPy frequency continuation | Distinct float64 counting at 100,000 items took about 22% less time; tracked allocation savings were only 72–140 bytes | `artifacts/numpy-generator-frequency-review/` |
| Projection repair | Direct-dict Python projection took 2.02–2.35 times as long at the two larger sizes; failures retained | `artifacts/projection-selector-review/` |
| Per-row selector snapshot | Python dict projection took about 5% less time while retaining selector lifetimes | `artifacts/selector-snapshot-review/` |
| Live computed columns | Expressions took 44%–119% longer, direct fields 6%–42%, callables 8%–27%; fix retained | `artifacts/with-columns-binding-review/` |
| File-scan opener checks | Nine timing/allocation comparisons passed on patched CPython 3.12.13; measured tasks stayed within about 2% | `artifacts/scan-opener-review/` |
| Executor forwarding prototype | Five of 126 comparisons failed across 252 reports; prototype rejected | `artifacts/isolated-forwarding-review/` |
| Allocator diagnostics | Allocation history affected faults and system CPU time; environment fields do not reconstruct that history | `artifacts/allocator-evidence-review/`, `artifacts/allocator-stability-review/` |

Allocation measurements exclude prepared inputs and untracked native allocations;
they are not process RSS. CPython 3.12.3 also exposed a tracing race reproduced
without fpstreams. Subsequent allocation comparisons used patched 3.12.13 on
both sides. See [CPython #128679](https://github.com/python/cpython/issues/128679).

## Next development steps

The current source includes explicit `Rows.group_by_sorted()`, `Flow.merge_sorted()`,
and bounded `Rows.join_sorted()` operations. They run in Python and check ordering
while consuming the input. Sorted joins use the first right record's column list
and enforce row budgets. These APIs are absent from published 2.1.0.

Flow CSV/JSON/JSONL and Rows CSV/JSONL sinks offer optional atomic path output.
Publication waits for writer and source cleanup, with a no-overwrite mode.
Flow gains JSONL output for arbitrary values; Rows JSONL gains a custom serializer.
Spreadsheet-safe CSV now protects headers as well as values, and relative Parquet
destinations stay fixed if a callback changes directories. These changes are
unreleased.

Further performance work starts with profiles of the current source. Computed
columns, projection, and collector loops still carry costs from correctness
repairs. Keep live callbacks, row snapshots, object lifetimes, and the failed
comparisons in the acceptance criteria. The discarded map-based projection and
executor-forwarding shortcuts must not be restored merely because outputs compare
equal on static data.

There is no reviewed `benchmarks/baselines/engine-v2.json` in this checkout.
The scheduled workflow's median of three same-checkout runs checks consistency
within a revision. A historical baseline needs review in its target CI runner;
local reports cannot establish that result.

Additional diagnostics, bounded statistics, and extension hooks remain candidates
without a release date. Free-threaded CPython 3.14t stays experimental and
non-blocking; standard CPython 3.11–3.14 is the supported release matrix.

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
