# v2 status and roadmap

[Version 2.3.0](https://github.com/steventimes/fpstreams/releases/tag/v2.3.0)
adds batch mapping and caller-owned mapping executors, and fixes async callback
type inference. It builds on 2.2.1's editor and planning fixes and 2.2.0's sorted
operations, atomic file exports, correctness repairs, and execution reports.

The 2.3.0 measurements use `v2.2.1` as a separate release reference. Optimizations
reduce setup cost for supported single-stage Python computed-column and
projection queries. Sorted single-key grouping also avoids per-row wrapper
allocation, and parallel maps can use a caller-owned executor. Single-collector
grouping and further submission experiments remain candidates below. Some earlier
correctness repairs increased execution costs, and their recorded regressions
remain in the measurement history.

## 2.3.0 candidate checkpoint

The local candidate verification on October 12 covered caller-owned mapping
executors and explicit `map_batches()` on synchronous and asynchronous flows. Each batch produces one
result; async callbacks keep the resolved return type, including mixed immediate
and awaitable returns. Batch sizes, buffering, ordering, pool ownership, and
cooperative cancellation remain explicit.

The final Python source passed 4,307 tests with 10 skips and 88.05%
branch coverage. Planning, native execution, spilling, and async execution passed
their existing focus thresholds. All 211 Rust tests, lint, formatting, typing,
version checks, and the offline dependency-lock check passed. Native and browser
wheels and the source archive built successfully. Clean-install API checks passed
on standard CPython 3.11 through 3.14. Installed-wheel checks also executed selected
consumer code from the football, coursework, and data-quality projects using
synthetic inputs; they establish compatibility, not application acceleration.

Fresh measurements used CPython 3.12.13, CPU 2, one thread per numeric library,
hash seed 0, release native builds, and the existing allocation tracing. The full
integer/float engine matrix passed at 1,000, 10,000, and 100,000 items with 210
records per size. Three fresh processes per side and size, seven samples per
process, and alternating invocation order supplied each comparison. All nine
time/allocation checks passed against the prior development reference, and all
nine passed in a separate comparison with released 2.2.1 Python source.

The separate release comparison measured these reductions in median elapsed time:

| Query shape | 1,000 rows | 10,000 rows | 100,000 rows |
| --- | --- | --- | --- |
| Dict field `with_columns()` | 32.9% | 5.1% | Within noise |
| Mapping field `with_columns()` | 14.4% | Within noise | Within noise |
| Sorted grouping, integer/string/bytes key | 19.5%–20.7% | 19.9%–20.8% | 18.2%–18.5% |
| Sorted grouping, one tuple-valued key | 11.9% | 13.4% | 12.7% |

These results describe local workloads and exclude import and input preparation.
Mapping projection showed no measured benefit, and larger computed-column inputs
did not establish a throughput gain. Tracked allocation peaks exclude prepared
inputs and native heaps; they are not RSS measurements.

Evidence is retained in the local `artifacts/rc230-20261012/` directory. The final
source/native fingerprints are `2665994d`/`916e3454`, and the shared matrix is
`e464e9d0`. The development reference is `7e9609bd`/`cc8bd052`; the separate release
reference is Python `a6f8cdc8` with locally built release native `cc8bd052`. Those
reports retain their original identities and are not merged into one baseline.
Candidate validation does not publish the package or establish remote CI results.

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

## Included in 2.2

These changes require version 2.2.0 or newer. See the
[changelog](https://github.com/steventimes/fpstreams/blob/master/CHANGELOG.md)
for the full list.

- **Sorted operations:** `Rows.group_by_sorted()`, `Flow.merge_sorted()`, and
  bounded `Rows.join_sorted()` run in Python and validate consumed keys. They
  preserve encounter order and enforce the sorted join's row budgets.
- **File exports:** Flow CSV/JSON/JSONL and Rows CSV/JSONL sinks offer optional
  atomic path output and no-overwrite publication after cleanup. Flow adds JSONL
  output for arbitrary values; Rows JSONL accepts a custom serializer. CSV header
  protection and stable relative output paths retain their documented defaults.
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
- **SQLite and ranges:** SQLite destination names follow ASCII case rules and
  duplicate column aliases fail before inserts. Sync and async flows support
  exact counts and bounded reads of ranges longer than `sys.maxsize`.

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
| Identity terminal planning in 2.2.1 | At 1,000 items, automatic list sums took 8.8% less time and tuple sums 7.7% less; tracked allocation peaks were unchanged | `artifacts/auto-sum-optimization-20261007/` |

Allocation measurements exclude prepared inputs and untracked native allocations;
they are not process RSS. CPython 3.12.3 also exposed a tracing race reproduced
without fpstreams. Subsequent allocation comparisons used patched 3.12.13 on
both sides. See [CPython #128679](https://github.com/python/cpython/issues/128679).

## 2.2.1 preparation and verification

The completed 2.2.1 preparation round used released 2.2.0 source as its local
benchmark reference. It covered 1,000, 10,000, and 100,000 inputs, plus callback
mutation, bounded streaming, and async backpressure experiments. Raw reports and
the diagnostic profiles of that 2.2.0 source are kept in
`artifacts/next-version-review-20261007/`.
These measurements do not create a CI historical baseline.

The released-source reference exposed failures in the existing same-run limits
for 1,000-item automatic list and tuple sums. The two larger input sizes passed,
and the 100,000-item holdout passed comparison with three reference runs.

A follow-up using saved and optimized 2.2.1 source reduced the median time for
those small sums by 8.8% for lists and 7.7% for tuples. Each side used three runs
with seven samples per scenario. The optimized auto/Python ratios were
1.05–1.07, within the original 1.1 limit. All nine timing and allocation
comparisons at 1,000, 10,000, and 100,000 inputs passed. Tracked peak allocation
was unchanged for these sums. Raw reports, including failed intermediate
candidates, are in `artifacts/auto-sum-optimization-20261007/`; this remains local
evidence rather than a CI historical baseline.

The released commit passed 4,217 Python tests with 10 skips, the branch-coverage
gates, and 211 Rust tests. Release builds produced eight native wheels and one
sdist; their hashes match the GitHub attachments, SHA-256 manifest, and PyPI
files. CPython 3.11–3.14 installations from PyPI passed native, Python, async,
and missing-extra smoke checks. This packaging evidence is separate from the
local performance measurements. Local publication records are in
`artifacts/release-2.2.1-20261007/`.

## Computed-column setup after 2.2.1

Fresh comparisons used released 2.2.1 source as the reference. Both sides ran on
CPython 3.12.13 with the same release native extension, CPU 2 affinity, single
thread settings, hash seed, and default allocator settings. Each side had three
independent process runs at each size, with seven samples per implementation.

The matrix now has 16 computed-column cases, including dict and MappingProxyType
inputs, five- and 32-column records, explicit Python controls, and a NumPy control.
Five projection cases ran alongside them. All nine timing and tracked-allocation
comparisons passed the existing regression limits. Verified aggregates from that
selection are retained; its original temporary report files are unavailable.
A fresh retained selection of the three dict selector cases also passed all nine
comparisons. The table below uses those retained reports.

For supported single-stage queries, execution now binds each query's callback
and fallback to a shared loop template. This removes repeated AST construction
and compilation. The original enrichment function still copies each row before
reading its live selector list against the original input. Multi-stage loops use
the existing compiler.

| Prepared dict rows | Field selector time saved | Row expression time saved | Callable time saved |
| --- | --- | --- | --- |
| 1,000 | 33.6% | 21.9% | 33.3% |
| 10,000 | 6.9% | 3.3% | 5.8% |
| 100,000 | Within 2% | Within 2% | Within 2% |

These results concern fixed setup cost in the measured workloads. At 100,000
rows, differences stayed within 2% across the computed-column cases. The
`dict.copy()` replacement probe was also within that range and was discarded.
Tracked allocation excludes prepared inputs and native heaps; it does not
establish a process RSS reduction. Retained subset reports and the verified
expanded-matrix and rejected-probe summaries are kept in
`artifacts/with-columns-optimization-20261008/`, outside the package and
reviewed CI baselines.

## Projection setup after computed columns

This round used the already verified computed-column source as its reference.
Both sides used the same release native extension and an expanded matrix of
11 projection cases plus three computed-column controls. At 1,000, 10,000, and
100,000 rows, each side ran three independent processes with seven samples per
implementation. All 18 reports are retained, and all nine timing/allocation
comparisons passed the existing gates.

A supported single-stage Python projection now reuses the existing row-map loop
template. The projection function still snapshots its positional and named
selectors for each row. Input-row retention, selector lifetime, callback errors,
and source cleanup keep their original boundaries. Small direct-field inputs
keep the existing route below 2,048 rows; the crossover was not changed.

| Prepared rows | Python field projection time saved | Expression projection time saved | Callable projection time saved |
| --- | --- | --- | --- |
| 1,000 | Within 2% | 22.2% | 33.0% |
| 10,000 | 6.0% | 2.1% | 5.4% |
| 100,000 | Within 2% | Within 2% | Within 2% |

The forced-Python wide-input case saved 5.6% at 10,000 rows. NumPy/native and
Arrow controls are separate from these Python results. This change reduces
setup work; it does not establish higher throughput on large inputs or lower
process RSS. Raw profiles, reports, and comparison baselines are in
`artifacts/select-optimization-20261008/`. These are local measurements, not
reviewed CI historical baselines.

## Analytics feedback and the next feature scope

The soccer analytics usage report supplied prepared-list team aggregation,
generator-fed cohort counts, and weighted player metrics as concrete workloads.
The benchmark now includes those input shapes, and Mapping projection controls
use the same prepared Mapping rows on both sides. Small first-query costs and
warmed throughput are recorded separately; normalization, deduplication, sorting,
and database reads retain their own costs.

The current sorted single-key candidate reduced complete-consumption time by
18%–20% for integer keys, 18%–20% for string/bytes keys, and 11%–13% for tuple-valued
keys across 1,000, 10,000, and 100,000 prepared rows. All nine time/allocation
comparisons passed. Merge and join controls stayed within about 1%. This result
concerns explicitly sorted grouping, not the report's ordinary hash groups.
See [sorted measurements](user-guide/performance.md#choosing-explicit-sorted-operations-current-source).

Caller-owned executors provide explicit pool reuse while preserving query-owned
cancellation and cleanup. A small synthetic spawn diagnostic separated startup,
first output, submission, computation, and shutdown; it supports checking startup
cost separately, without establishing a general batching or application speedup.
The existing `chunk().map_parallel().flat_map()` composition remains available.
The new [analytics recipes](user-guide/analytics.md) use existing v2 interfaces.

The final source passed 4,288 Python tests with 10 skips and all branch-coverage,
lint, formatting, and strict-type gates. Each of the three engine matrices
contained 210 passing records. Installed wheel checks passed on standard
CPython 3.11–3.14, and the browser wheel and strict documentation build passed.
A local consumer check called the application's actual team, player-weighted,
and cohort functions with synthetic inputs through saved release, preceding
development, and current wheels. All three agreed with the application's Python
reference; application source and the supplied report were unchanged. This is
functional compatibility evidence, not a full application run or speed claim.
Records are retained in `artifacts/next-api-feedback-20261011/`.

## Consumer feedback and engine references reviewed on October 12

Two additional consumer reports narrow the next experiments. The COSI116
consumer used installed 2.2.1 for a read-only SQLite query, six numeric sums
plus a count, and a validated one-to-one adjacent-season join. Its saved
32,574-row/60-group and 10,670-pair results agree with independent references;
the report contains no formal performance comparison. A quality-worker report
uses pinned 2.1.0 with a file iterator and one stateful `reduce` callback. Its
Decimal checks, deadlines and original-row evidence require sequential callback
semantics. These are consumer reports, not acceptance of this development tree.

The proprietary references are
[Photon](https://docs.databricks.com/aws/en/spark/faq) and the separately licensed
q core behind [PyKX](https://github.com/KxSystems/pykx/blob/main/LICENSE.txt).
[Pathway](https://github.com/pathwaycom/pathway/blob/main/LICENSE.txt) has public
BSL source, while [Bodo](https://github.com/bodo-ai/Bodo/blob/main/LICENSE) has an
Apache-2.0 core; neither belongs in an opaque-binary comparison by default.
REA inspected the official PyKX 4.1.0 wheel's archive and public wrapper API.
The recorded package digest matches PyPI. Native engine algorithms and vendor
runtime performance were not established by that inspection.

[Photon's published architecture](https://www.databricks.com/de/wp-content/uploads/2022/07/Photon-A-Fast-Query-Engine-for-Lakehouse-Systems.pdf)
motivates measuring conversion boundaries before adding kernels. Compare an
existing Arrow/NumPy pipeline with its first Python-row boundary, recording
planning, reading, conversion and aggregation separately. Preserve a supported
columnar segment where safe; one-shot sources must keep their streaming fallback.
[PyKX's null-conversion documentation](https://code.kx.com/pykx/4.1/user-guide/fundamentals/nulls_and_infinities.html)
also shows why skipping validation can change the values a user receives.
Compare nullable and null-free numeric inputs with the original semantics.

The next profiles should cover multiple collectors and generic stateful reduce
before revisiting the existing single-collector path. Use prepared dict/Mapping
records, fresh generators, and filtered SQLite inputs as separate workloads.
Retain strict join cardinality, empty-input behavior, callback errors, source
cleanup, and the original-row evidence order. A cheap integer-reduce probe alone
cannot establish a bottleneck in a Decimal-based worker.

Current `run_with_report("reduce", function, initial)` already observes a reduce
terminal. In unreleased code, the same report includes up to 64 events from
existing plan and direct-route observation points, including child plans.
The addition preserves the outer report and leaves ordinary execution free of
per-row instrumentation and source replay. It does not provide a complete kernel
trace or stage timings. Consumption counts and cancellation accounting still
need their own definitions; they are not existing report fields. See
[execution reports](user-guide/execution-reports.md) for the current diagnostic
scope. No runtime speedup has been established by this reporting addition.

### Execution controls and file recovery after 2.3.0

The unreleased work following the Kestra review also addresses data movement,
shared executor pressure, and file-partition recovery. These are separate from
execution reporting. [Kestra's storage guidance](https://kestra.io/docs/concepts/storage)
and [partitioned processing examples](https://kestra.io/docs/best-practices/loop)
provided the design comparisons; fpstreams retains its existing lazy operators.

`BoundedExecutor` supplies one unfinished-call budget to independent queries
sharing a caller-owned pool. Each flow keeps its own result buffer and cleanup
ownership. The wrapper leaves the underlying pool open. Saturated local probes
use 3, 8, and 16 queries with the same worker and per-query buffer settings,
recording aggregate pending calls and Python allocations separately from speed.

`FileCheckpoint.run()` accepts a stable input file and a callback that writes
one output file. It returns the verified output path, allowing subsequent
stages to pass file references. Input bytes, resolved paths, and explicit
processing versions identify partitions. Completed entries include output
hashes; corrupt entries fail without silently replaying callbacks. This does
not make external callback effects transactional or replay arbitrary iterators.

The initial matching-schema Arrow path covered unchanged retained tables and
record batches. Follow-up work also admits safe direct-field projections and
primitive filters when the explicit schema matches their output, including
metadata. Each batch is revalidated; changed column order or types, modified
conversion hooks, and unsupported plans retain the row converter. Local probes
cover narrow, wide, and nullable tables at 1,000, 10,000, and 100,000 rows, with
unchanged-schema, inferred-schema, and cast controls measured separately.

Three independent paired rounds with 21 samples each measured 98.4%–98.6% less
time for direct projection and 94.7%–95.7% less for filtering plus projection
at 100,000 rows. Controls stayed within 3%. Input preparation is outside timing;
Python allocation peaks exclude prepared data and native heaps. These are local
export-path measurements, not application speedups or a reviewed historical
baseline. Zero-column Arrow and Polars output preserves row counts; Parquet
writes reject nonempty rows without columns before publishing a destination.

### Bounded ranking and Python collector dispatch (unreleased)

`agg.top(count, key=...)` and `agg.bottom(count, key=...)` retain at most K whole
items per state, keep equal keys in input order, and compose with existing
global, async, hash, sorted, and spill aggregation. A heap entry is allocated
only for a retained candidate. Keys run once per item, and payloads are never
compared. The [ranking recipe](user-guide/analytics.md#rank-records-within-each-group-unreleased)
shows their use alongside a count.

Three alternating local rounds at K=5 measured 92%–99% lower Python allocation
peaks than collect-then-rank on 100,000 input records split into one or 60
groups. The bounded implementation took 13%–36% more time. At 1,000 records and
60 groups, its allocation peak was about 35% higher. Bounded state is useful
for large groups; it is not a promise of lower latency or allocation for every
shape. Avoiding temporary entries for discarded candidates reduced time by
35%–38% relative to the first bounded implementation at 100,000 records.

Profiling six sums plus count and weighted metrics identified Python collector
dispatch as a shared cost. Reusing the internal state lists within one row
reduced time by 2.6%–4.5% and 2.2%–4.4%, respectively, at 100,000 dictionary
records across three paired rounds. Mapping and unique-group controls remain
separate; no application or unique-group speedup is claimed. Live collector
getters, steps, completion checks, and finishers retain their order.

Owned-record expansion helpers now live with record conversion. `Rows` retains
the planning, snapshot, and materialized-path proofs; public interfaces remain
unchanged apart from the two new aggregator factories.

### Further source comparisons

[Trino's pushdown documentation](https://trino.io/docs/current/optimizer/pushdown.html)
reinforces doing projection and filtering at the source. fpstreams already
supports explicit Parquet scan projection/filtering and parameterized database
queries with bounded fetches. Arbitrary Python callbacks still run in Python;
moving them into a connector can change types, errors, and side effects.

The unreleased `agg.min_by` and `agg.max_by` select a whole record by a separate
key. They cover batch latest-record selection and ranked records without
collecting and sorting each group. [Trino's keyed aggregations](https://trino.io/docs/current/functions/aggregate.html#max_by)
and [Hudi's record-merging model](https://hudi.apache.org/docs/record_merger/)
supplied comparisons. fpstreams uses Python comparisons and keeps the first
item on ties. It provides no persistent table, partial-update reconciliation,
or event-time watermark. The [versioned-record recipe](user-guide/analytics.md#select-records-by-version-unreleased)
applies deletion markers after selecting the winning version.

[Beam's accumulator lifecycle](https://beam.apache.org/documentation/programming-guide/)
maps to existing collectors and reducers. Further partial aggregation needs
explicit merge laws and measurements; arbitrary Python keys do not establish
associativity. Changing when user callbacks run also requires preserving the
v2 contract. [ReactPHP streams](https://reactphp.org/stream/) and
[StreamSaver](https://github.com/jimmywarting/StreamSaver.js) emphasize bounded
transfer and sink completion, which fit existing buffered maps, owned source
cleanup, and streaming file writers.

[Allegro's cache case study](https://blog.allegro.tech/2016/03/writing-fast-cache-service-in-go.html)
motivates measuring retained objects and lock contention. Current plan and
kernel caches are bounded to 128 descriptors; their source-free, full-key
lookup must remain intact. A Go-specific heap layout is not evidence for
replacing these Python caches. [StreamDiffusion](https://github.com/cumulo-autumn/StreamDiffusion)
offers batch and queue comparisons; similarity-based frame skipping is an
application policy. Existing `map_batches`, filters, and shared executors
provide explicit composition points.

DBX, StreamPark, Java Stream, and IronMQ operate at database-client, job-platform,
or message-delivery boundaries. Their useful comparisons are capability checks,
recovery records, batching, and acknowledging work after success. Broker
reservations and acknowledgments need a concrete connector contract before
fpstreams can manage them. Functional-Light JavaScript, the FP jargon glossary,
FunctionalPlus API search, and the Ironhack exercises inform naming and small
examples. Existing `pipe`, `curry`, collectors, and the API index
already provide those entry points. The awesome-bigdata list is a discovery
index; each linked project's actual documentation and source remain the
evidence for an implementation decision.

### Second source pass: composition boundaries

Further reading of [Beam's batch-size estimator](https://github.com/apache/beam/blob/cddfa4b6e2e2fa54f13c73d29b598e3de40a3e93/sdks/python/apache_beam/transforms/util.py)
and [Trino's remaining-filter contract](https://github.com/trinodb/trino/blob/78cbf077f084fd5fea3d23c131f87266b7f89c52/core/trino-spi/src/main/java/io/trino/spi/connector/ConstraintApplicationResult.java)
narrows the next work to existing composition boundaries.

Parquet already has automatic projection and exact single-comparison scan
hints, in addition to explicit source options. A synthetic 4,000-row, 20-column
probe passed 10 rows and one column from the scanner to the executor for one
range filter followed by a direct select. Adding a second range filter before
the select retained correct output but passed all 4,000 rows and 20 columns.
An explicit source-filter control passed 10 rows and one column. These are
observed scanner outputs, not physical read counts or latency measurements.

Unreleased execution now carries the leading proven comparison through a longer
guarded filter/projection prefix while retaining the original residual operations.
It does not combine later filters: a later false predicate could remove a null
row before an earlier comparison raises its Python `TypeError`. Unsafe batches
reuse the lazy Python row-operation chain, preserving exception order and early
exit. Parquet validates fallible source values before automatic hints discard
them; explicit source options still define the visible input. This can require
reading unused string, temporal, or nested columns. Repeated paired measurements
compare the wider prefix against a baseline containing the same correctness fixes.

The local comparison uses 1,000, 10,000, and 100,000 rows, narrow integer and
wide mixed/nullable inputs, Parquet row output and retained Arrow batch output.
Three alternating rounds contain 216 profiles with 21 samples each. At 100,000
rows, the two-filter projection takes 87.3%–88.3% less time for narrow Parquet
and 80.2%–80.7% less for wide Parquet. Retained Arrow batch export takes
87.8%–88.2% and 92.2%–92.3% less time, respectively. Python allocation peaks
fall by about 16% and 55% for Parquet, and 98%–99% for retained Arrow.
The existing timing, allocation, and group gates pass; individual control time
ratios range from 0.903 to 1.224 across all sizes and rounds.

Preserving conversion errors has a separate cost. Against the earlier rules,
the valid wide-text single-filter Parquet control takes 2.18–3.64 times as
long because it validates values in columns and row groups that automatic hints
previously discarded. Narrow integer controls remain near their earlier time.
The next scan candidate is to read the required query columns and every field
whose conversion can fail, retaining their original error order. These are
local workload measurements; prepared inputs and native heaps are excluded
from Python allocation, and physical I/O, RSS, and application timing were not
measured. The Parquet and retained Arrow measurement paths are unchanged by
the later CSV fixes. CSV now uses one full typed scan before query
projection, preserving errors in later blocks and invalid-row callback counts.

A subsequent refinement omits safe unused fields from validation scan requests
when a default local Parquet source has one fragment. Required query and scan
predicate fields remain, and fallible fields retain their source order. A
multi-fragment or supplied-schema scan can cast an integer column from physical
strings, so it validates the full caller-visible projection before automatic
query hints discard values. Counting fragments stops after two entries; it
does not decode additional physical schemas or invoke their extension hooks.

The refinement's three alternating rounds contain 486 profiles, each with
21 samples, across three row counts and numeric, mixed/nullable, all-string,
nested, and two-file inputs. Its baseline contains the same cast and error
fixes with safe-field omission disabled. At 100,000 rows, mixed-input single
and double-filter projection takes about 19%–23% less time, and Table output
takes about 25%–26% less. Python allocation peaks remain close to the baseline;
the existing timing, allocation, and per-shape/operation group gates pass.
A separate valid-input comparison to the preceding checkpoint records the
cast-preservation cost: the numeric two-file query takes 2.77–2.79 times as
long. Explicit source `columns` and `filter` still define an earlier boundary
when that is the intended input. These measurements retain the same allocation
and application-level limits described above.

`batch_by_size` already supports measured sizes and item-count bounds, and
`buffer_timeout` already supplies async count/time flushing. Reuse those APIs
before adding an adaptive batching interface. Beam's estimator measures
downstream cost and adjusts batch size; fpstreams callbacks can observe those
boundaries, so adaptation cannot silently replace fixed `map_batches` behavior.
Start with fixed-size comparisons and a payload-size control. See
[batching bounds](user-guide/performance.md#choose-a-batching-bound).

Allegro's cache comparison led to a separate contention profile. Three local
CPython 3.12.13 processes each ran 144 combinations of integer/float expressions,
short/long programs, 1/32/1,024 input values, 1/4/8 workers, and cold/warm caches
with repeated/distinct keys. Each combination retains 21 uninstrumented timing
samples of 128 queries. The source was `ee6d9781`, with native `916e3454`; these
are separate workloads from the 210-record engine matrix.

Separate instrumented observations found blocked kernel-cache acquisitions in
42 of 108 cold/distinct-key trials and 8 of 216 warm-cache trials. Same-key cold
batches compiled once, and warm batches compiled zero times. Some scenarios
varied substantially across processes. The raw timing samples, lock observations,
and additional compile CPU diagnostics remain in `artifacts/cache-contention-20261015/`.
Summed wait time spans concurrent callers and does not estimate a possible speedup.

The lock-waiting behavior is reproduced; an application bottleneck or improvement
has not been established. The next comparison should mix warm hits with unrelated
cold compilations. Any lock change must keep source-free bounded entries, full
fingerprints, same-key compilation, recursive calls, and error propagation.

The second pass rechecked the hashes of the prior 32 pinned source files and
read four additional files. Route, batching, and whole-record selection probes
are in `artifacts/link-followup-20261015/`. Whole-record `max_by` correctly does
not merge partial updates; Hudi-style merging needs an explicit policy for
missing fields, nulls, deletions, and versions. No new runtime behavior or
performance gain is claimed by this source review.

### ClickHouse: defer payload work after selection

[ClickHouse's lazy materialization](https://clickhouse.com/blog/clickhouse-gets-lazier-and-faster-introducing-lazy-materialization)
adds a useful comparison for wide records: retain positions through filtering
and ranking, then obtain the payload columns for the selected rows. Its
[pinned optimizer](https://github.com/ClickHouse/ClickHouse/blob/39dfa8c8bead0da6c2adbc1ec3cfa92fffe25aec/src/Processors/QueryPlan/Optimizations/optimizeLazyMaterialization.cpp)
checks supported readers, small limits, and intervening expressions; it declines
row-expanding shapes. This supports extending existing guarded paths rather
than adding a general SQL engine to fpstreams.

Current `Flow.top` and `bottom` bound retained candidates with `heapq`, but
still obtain Python rows from Arrow inputs before ranking. A separate candidate
is direct primitive-key ranking over retained Arrow data, followed by conversion
of the winning rows. Stable ties, nullable keys, dropped-column validation,
mutable buffers, conversion hooks, and early-exit behavior must remain intact.
File-backed late reads need an additional stable-source and row-position
contract; they do not follow automatically from a retained-table experiment.

The subsequent retained-ranking prototype failed the existing Python control
timing gate and was withdrawn. Its preserved results are in
`artifacts/arrow-ranking-20261015/`; no ranking fast path remains from that trial.
A renewed trial must account for control variability and the native position
buffer's growth with input size.

ClickHouse's [chunk representation](https://github.com/ClickHouse/ClickHouse/blob/39dfa8c8bead0da6c2adbc1ec3cfa92fffe25aec/src/Processors/Chunk.h)
also tracks row count independently of columns, consistent with the corrected
zero-column Arrow exports. Its [aggregation settings](https://github.com/ClickHouse/ClickHouse/blob/39dfa8c8bead0da6c2adbc1ec3cfa92fffe25aec/src/Interpreters/Aggregator.h)
distinguish group-count and byte thresholds. Existing `SpillLimits` already
bound partition rows and encoded bytes; those are not live Python collector
state or RSS bounds. No additional memory guarantee, partition-merging law,
or fpstreams speedup is established by this comparison. Selected source files
and decisions are retained in `artifacts/clickhouse-research-20261015/`.

### References queued on October 17

These repositories were initially saved for later review. The October 18
follow-up completed scoped, read-only analysis of their relevant architecture
and public interfaces. Experiments and implementation remain deferred.

- [ThingsBoard](https://github.com/thingsboard/thingsboard)
- [StarRocks](https://github.com/StarRocks/starrocks)
- [Debezium](https://github.com/debezium/debezium)
- [Bytebase](https://github.com/bytebase/bytebase)
- [xerj](https://github.com/xerj-org/xerj)
- [Sogou Workflow](https://github.com/sogou/workflow)

The separate [73-reference catalog](research-references.md) preserves the supplied
paper and project list, proposed uses, and extension markers. Review status is
recorded separately from the list.

## Next development steps

### Completed research follow-up on October 18 (unreleased)

The [reference review](research-references.md#october-18-completed-review-and-adopted-changes-unreleased)
is complete for all 25 papers and the selected documentation and implementation
scope of the non-paper references. The adopted changes are implemented and
verified:

- integer-index validation before acquiring task and file resources;
- direct synchronous `filter_map()` execution and integration with async serial
  fusion, preserving one mapper call per input and all results except `None`;
- versioned plan dictionaries and [report export](user-guide/execution-reports.md#metadata-export-unreleased),
  with reports recording metric units, resource scope, and event limits;
- a shared selection mask for proven primitive Arrow comparison chains;
- earlier release of encoded buffers and completed decoded frames during spilling.

The final source passed 4,769 Python checks with 10 skips and 88.28% branch
coverage, the existing engine and targeted performance gates, package builds,
and installed-wheel checks on standard Linux CPython 3.11–3.14. These are local
development results, not a new release or Windows/macOS acceptance. The stable
package remains 2.3.0.

The synchronous cross-node fusion prototype was withdrawn after a performance
regression; the direct `filter_map()` implementation keeps the original iterator
boundaries. The earlier Arrow ranking prototype also remains withdrawn. Neither
prototype is part of the verified changes. DABA, event-time and distributed
execution, approximate aggregates, and transparent callback reordering were not
adopted. A completed reference review does not schedule those capabilities.

Further optimization candidates remain below. Start from the verified source
and profile a concrete workload before choosing another implementation change.

### Functional-programming source follow-up (unreleased)

The [FP jargon glossary's unfold example](https://github.com/hemanth/functional-programming-jargon/blob/a362b3a6117eeaa6042fc21e032afa30bab8b5d6/readme.md#anamorphism)
motivates `flow.unfold` and `aflow.unfold`: a callback returns `(value, next_state)`
or `None` to stop. fpstreams uses a lazy iterative source, permits `None` values
inside a transition, and keeps callback-controlled termination explicit.

`map_accum(initial, function)` provides stateful mapping over an existing source.
Callbacks return `(next_state, output)`, and the flow emits one output per
consumed item. This is an explicit streaming counterpart to the
[map-accumulator operation](https://ramdajs.com/docs/#mapAccum), without a
materialized output list or returned final state. Sync reuses a gatherer;
async reuses serial scan and map. No optimizer node, dependency, or new root
namespace is required. State starts from the supplied object on each execution;
mutable objects are not cloned. These APIs add capability, with no speedup claim.

FunctionalPlus's [median implementation](https://github.com/Dobiasd/FunctionalPlus/blob/462d2a2508360e982f08cda7b37a90617189d636/include/fplus/container_properties.hpp#L433)
and the consumer report's speed-percentile use case motivate the
[exact grouped-percentile recipe](user-guide/analytics.md#exact-grouped-percentiles-unreleased).
It composes existing collectors with the standard library, calculates p50 and
p90 together, and defines missing-value and small-group policies. Exact results
retain the valid observations in each active group.

Start each further candidate from the verified development source after the
measured changes above. Use released `v2.2.1` for a separate comparison of the
combined changes through 2.3.0. For subsequent work, use released `v2.3.0` as
the separate release reference. Build a release extension for each side and collect fresh
reports with the same harness, interpreter, dependencies, runtime settings,
input shapes, and invocation order. Reproduce any cost suggested by older
profiles on the chosen reference before changing code.
Verify `FPSTREAMS_BENCHMARK_SOURCE` and the imported source fingerprint when
selecting the reference checkout.

| Order | Candidate | Initial comparison | Required behavior |
| --- | --- | --- | --- |
| 1 | Python `with_columns()` setup, measured | Fields, row expressions, and callables; dict and Mapping rows; narrow and wide records | Read the live selector list and evaluate against the original row; preserve row copies, callback order, and errors |
| 2 | Python `select()` setup, measured | Fields, aliases, and expressions at different widths | Keep a selector snapshot for each row, output field order, object lifetimes, and source cleanup |
| 3 | Sorted single-key grouping, measured | Integer, string, bytes, and tuple-valued keys; repeated and skewed groups; early and full consumption | Keep key validation, live callbacks, lookahead bounds, and close order |
| 4 | Caller-owned executor, implemented | Repeated thread and spawn-process queries, early exit, callback failures, and interrupted waits | Cancel and await only query-owned work; preserve pool ownership and source cleanup |
| 5 | Multi-collector dispatch improved locally, unreleased | Six-metric totals and weighted metrics; dict/Mapping, generator input, unique-group controls, and selected SQLite consumer checks | Keep dynamic getters, collector step/finish functions, valid denominators, key identity and release order |
| 6 | Generic stateful reduce | File iterators with Decimal checks, deadlines and bounded evidence; cheap callback controls | Keep one pass, left-to-right callbacks, empty-input behavior, exception identity and source cleanup |
| 7 | Existing single-collector path | Low and high key cardinality; dict, Mapping and callable selectors | Profile the existing implementation before changing its hash/equality calls or state lifetimes |
| 8 | Bounded events integrated into execution reports, unreleased | Child plan choices and observed direct routes | Preserve the outer report, cap each report at 64 events, and avoid replay or default per-row instrumentation |
| 9 | Shared executor budget, unreleased | Independent queries using one pool, completion, cancellation, shutdown, and interrupted admission | Bound unfinished submissions through one wrapper; keep per-query cleanup and borrowed-pool ownership |
| 10 | File partition recovery, unreleased | Reopened stores, injected partial writes, process termination, input changes, and corrupt outputs | Reuse only verified completed outputs; retain explicit versions, file ownership, and callback errors |
| 11 | Matching explicit Arrow schemas and safe prefixes, unreleased | Retained tables and batches; narrow, wide, nullable, per-batch schema changes, casts, metadata, and callback controls | Preserve schema and bounded batches; keep unsupported inputs on the existing converter |
| 12 | Further process submission and source reuse | One item versus explicit batches under the same spawn policy; prepared inputs versus repeated file scans | Keep bounded submission, ordering, callback errors, source ownership, and early-exit cleanup |
| 13 | Whole-item keyed extrema, unreleased | Latest versions, equal keys, deletion markers, callback failures, sorted and spilled grouping | Select once per item, keep the first tie, preserve Python comparisons and source cleanup |
| 14 | Bounded per-group ranking, unreleased | Stable ties, identity, zero count, callback failures, async and spill; allocation and latency compared separately | Retain at most K items per state and preserve input order for equal keys |
| 15 | Longer guarded Arrow/Parquet prefixes and validation-column pruning, unreleased | Three sizes, narrow and wide/nullable inputs; paired profiles with single-filter, callback, and scanner-cast controls | Keep residual and conversion-error order, live selectors, and one source open; retain complete validation where casts cannot be ruled out |
| 16 | Explicit batch-size tuning | Fixed sizes versus existing measured-size/count and async timeout bounds | Keep boundary-sensitive callback behavior explicit; distinguish pending calls, retained results, and payload bytes |
| 17 | Cache contention profiled locally | Three processes, 144 combinations each, 21 samples per combination; separate lock and CPU observations | Next test mixed warm hits/cold compilation; retain bounds, fingerprints, same-key compilation, recursion and errors |
| 18 | Arrow ranking prototype withdrawn | The original Python control timing gate failed; candidate and reports retained locally | Resolve control variability and native position-buffer costs before a renewed trial; preserve ties, conversion errors, hooks and source identity |
| 19 | Exact percentile composition documented, unreleased | Finite observations with missing/zero values and small groups; installed-wheel checks on Python 3.11–3.14 | Normalize numeric inputs, keep inclusive interpolation explicit, and account for retained per-group observations |

Start row comparisons at 1,000, 10,000, and 100,000 items with the existing
`rows.with_columns*`, `rows.select*`, `rows.group_sum.python.*`, and `sorted.*`
competitive cases. Include callable/Python controls and inspect the observed
route before attributing a gain to the Python loop. For small planning changes, also check
short inputs and either side of the existing native crossover.

Each candidate needs a fresh profile, a correctness-checked reference, one
implementation change, and repeated timing/allocation comparisons. Add negative
regressions to the closest existing test module and then run the complete
Python checks for an execution change. Extend the matrix before measurement
when a required shape is missing, and regenerate both sides after any matrix
change. Keep a candidate only when it beats run-to-run noise and passes the
existing gates. Preserve failures and revert neutral candidates.

Single-collector grouping already has a dedicated execution path. Profile that
path before changing it; do not add another implementation. The soccer report's
team and weighted summaries use multiple collectors, so their measured cost
cannot establish a single-collector bottleneck.

The earlier COSI107 assignment comparison using pre-change and optimized 2.2.1
wheels took about 168.48 and 168.00 seconds in one run each, with equal outputs.
That establishes application compatibility, not an application speedup.
Process experiments must separate submission cost, startup, callback computation,
first-result latency, and final cleanup. Begin with the existing explicit
`chunk(...).map_parallel(...).flat_map(...)` composition. Batch size changes the
unit of buffering and can change callback/error timing; compare those semantics
when evaluating a new batching option or executor reuse. Deferred sources keep
their reopening behavior, and the process backend keeps `spawn`.

There is no reviewed `benchmarks/baselines/engine-v2.json` in this checkout.
The scheduled workflow's median of three same-checkout runs checks consistency
within a revision. A historical baseline needs review in its target CI runner;
local reports cannot establish that result.

Additional diagnostics, bounded statistics, and extension hooks remain candidates
without a release date. Free-threaded CPython 3.14t stays experimental and
non-blocking; standard CPython 3.11–3.14 is the supported release matrix.

Compatible fixes and internal performance changes are maintenance candidates.
The caller-owned executor and explicit `map_batches()` additions ship in
2.3.0. Batch mapping uses the existing chunk and map paths,
produces one result per batch, and supports sync pool defaults or async map
controls. Replay interfaces remain proposals; `reduce_by()` already covers
per-key state reduction. Dependency updates also get their own
lock, adapter, and platform validation; they are reviewed separately from
performance changes. The stable release is 2.3.0. The local candidate results
above retain their original measurement dates and scope.

The map-based projection, executor-forwarding removal, `Counter(map(...))`,
frozen-field inlining, and shared Mapping-check shortcuts remain rejected.
Their failed callback, lifetime, or timing cases are regression requirements
for future work.

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
