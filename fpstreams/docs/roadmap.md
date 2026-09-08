# v2 status and roadmap

The `2.1.0` release contains the features listed below. Working changes were checked
against the local source on 2026-09-07 and are not included in that release.
No release date has been assigned to the remaining candidates.

Working changes fix grouped-result and scalar-cache errors and add Rust grouping
for two integer keys. They also reduce expression compilation costs and speed up
wide Python joins. The remaining Python grouping regression is documented below.

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

## Implemented in the working tree

### Pivot iteration and frequency key callbacks

Native pivot now checks that iteration and source hooks still have their
original methods and code. Replacing an opener had made the Python path read
a value of 30 while `auto` returned the old value of 2 and skipped the opener.
The fallback now preserves the replacement's result, error, and cleanup.

The Python pivot fallback also uses one shared selector loop. Direct dict
lookups had ignored changes to the compiled selectors: changing a value
selector's captured field made a mappingproxy yield 30 while a dict still
yielded 2. Index and column selectors had the same problem. The native path
now checks selector code, closure bindings, and globals before admission.
Forty regressions cover changes before execution and during source reads,
including exceptions from selector globals.

At 1,000, 10,000, and 100,000 input rows, forcing Python for the direct dict
pivot took about 62%–67% longer than the old shortcut. All nine comparisons
flagged that task; auto, mappingproxy, and callable pivot controls stayed
within about 2%. The Python task's traced allocation peak increased by
24,208–96,064 bytes. These measurements exclude prepared inputs and native
allocations. The fix is retained for consistent selector behavior, with the
failed timing reports available for further work.

For sequential linear `auto` plans, `frequencies(key=...)` uses Python iteration
between key calls. A key callback that changed the next NumPy value to 9 had
still received 2 after native materialization. The execution report records
`python_frequency` for this path. Keyless counting, explicit
native requests, and relational inputs retain their existing routes.

The new controls use prepared int64 and float64 arrays, with and without a
callable key, at low and high cardinality. They check complete counts and
first-key order. At 1,000, 10,000, and 100,000 items, keyed NumPy counting took
1.60–2.11 times as long as the old route. All nine comparisons exceeded the
existing timing limits for those four tasks. Pivot and the other controls
stayed within about 2.2% of their baselines.

Measured Python allocation peaks fell for keyed counting; this excludes prepared
input arrays and untracked native allocations. The correctness fixes are retained,
along with the failed timing reports. Further work starts from the corrected
scalar iterator and must keep reads between key callbacks.

The next scalar-iterator change uses `size` for the per-value length check on
exact ndarrays, avoiding a temporary shape tuple. It still checks dimensions
and length before each read; custom array objects keep their shape protocol.
Against the corrected iterator, keyed counting took about 5%–7% less time at
the same three sizes. All nine timing and allocation comparisons passed, with
other controls within about 3.2%. The four keyed tasks each used 16 more bytes
at their measured Python allocation peak. This recovers part of the repair's
cost; it does not restore the earlier materializing route's speed.

### Live group key selection

Python grouping now calls the key selector for every row. The old field shortcut
could keep reading an earlier field after the source or a callback changed the
selector. One reproduced case merged two groups with totals of 1 and 2 into a
single total of 3. The fix also preserves changes to selector code, globals, and
error types, along with first-key identity and one-shot cleanup.

The fix has a measurable cost. Compared with the version that still had the bug,
local mappingproxy tasks took 43%–53% longer with repeated keys and 27%–33%
longer with distinct keys. Dict field tasks took about 2%–10% longer and Mapping
field tasks about 8%–20% longer; callable controls stayed within 3%.
All nine comparisons exceeded the existing mappingproxy timing limit.

Those runs covered 1,000, 10,000, and 100,000 records on a release native build,
with three runs per side and seven samples per run. The failed reports remain
available, and the thresholds are unchanged. Further optimization must preserve
selector calls and Mapping protocol hooks.

### Scalar constant types

Scalar caches now distinguish equal constants with different types. Previously,
compiling a float expression first could turn the integer result
`9007199254740993` into `9007199254740992.0`. Reversing the order could change
a float result into an integer.

Nonstandard operand objects retain their identity in evaluator caches. Structural
caching and native execution decline them without conversion: `auto` uses Python,
and forcing `native` raises `NativeUnsupportedError`. Ordinary constant factories,
cache limits, and Pairs evaluator-identity checks retain their existing behavior.
Regression coverage includes both warmup orders, short and long expressions,
custom objects, and inspection before errors.

### Generated-code locations

Scalar expressions, row expressions, and their fused loops now share a pass that
assigns locations to generated AST nodes. It preserves synthetic line numbers
and traversal order, including object inspection before compilation errors.

In the local comparison for this change, 1,000-value Rows `with_columns()` tasks
took about 9%–11% less time, and small NumPy filter/projection tasks about 4%
less. At 100,000 values, timings stayed within 2% of the baseline. These results
support lower planning costs on small inputs; they do not establish a gain in
large-input execution.

### Scalar fingerprints and signed zero

Scalar instructions and evaluator caches now preserve the sign of `-0.0`.
Previously, instruction generation replaced it with `0.0`, and the cache treated
programs containing those values as equal. Pairs uses the corrected key too.

Fingerprint encoding also reuses fixed headers for opcodes from 0 through 255.
The roughly 18KB table holds only bytes. Instructions are still read and checked
on every call, and negative or wider opcodes use the general encoder. The binary
format preserves arbitrary-size integers and every float payload bit.

Compared with the version containing both scalar-cache fixes, repeated expression
compilation took about 14%–16% less time. Complete map/filter tasks took about
5%–11% less at 10 inputs and 3%–9% less at 1,000. At 100,000 inputs, execution
stayed within about 2% of the baseline. All nine cross-version comparisons and the engine suite's same-run reference
checks passed.

These measurements used CPython 3.12.3 with a release native build, three runs
per side and seven samples per run. They describe this change in isolation;
compile-only timings do not prove that a native kernel ran.

### Live grouped aggregation functions

Grouped aggregation now calls the live collector functions. The former inlined
sum loop could miss function-code or closure changes during traversal and return
6 where the current function returned 51. A separate two-key count/sum loop could
return 5 instead of 50 after a source callback changed its step.

Both loops and their partial recovery machinery have been removed. Single
collectors use the shared loop; multiple collectors use the collector program.
They preserve source consumption, callback order, cleanup, and finisher changes
between output rows. The shared loop also honors step changes made while
truth-testing a custom completion result and releases replaced completion values
before pulling another row. Removing the inlined loops increased Python execution costs;
the Rust path below recovers that cost for supported two-key integer inputs.

The single-collector loop no longer keeps an extra reference to the latest state.
It preserves the general collector program's release order, including when an
output iterator is closed early. Unused input keys are also released before the
finisher is read. In one reproduced case, a key's release callback replaced the
finisher: the corrected results are 40 and 20; the old loop returned 4 and 2.

The state-lifetime fix passed all nine local timing and Python allocation
comparisons at 1,000, 10,000, and 100,000 records, with three runs per side and
seven samples per run. First-value grouping took about 1%–4% less time with
repeated keys and 2%–4% more with distinct keys. The changed Python paths used
16–72 additional peak tracked bytes. These results support retaining the fix;
they do not establish a general speed or memory improvement.

### Two integer keys in Rust

With `engine="auto"`, a retained list or tuple of exact tuple rows can now use
Rust for count/sum grouping with two direct integer-index keys. The keys and
selected values must be plain signed 64-bit integers. Results preserve encounter
order and the first key objects; sums can exceed 64 bits.

One-shot sources, changed functions, custom types, and unsupported values use the
Python collector program. Older extensions without the new entry point also fall
back. Native allocation errors propagate, and successful native execution reports
`rust_direct`.

Local comparisons at 1,000, 10,000, and 100,000 rows measured 54%–89% less time
than the old inlined loop and 89%–97% less than the corrected Python version.
Both distinct-key and 16-group cases were included. All 18 comparisons against
the two baselines passed the existing timing and Python allocation gates.
Each version ran three times per size with seven samples per run on CPython
3.12.3 with a release native build. Python allocation measurements exclude Rust
allocations, and these results cover only the supported integer-tuple cases.

### Collector field reads

Collector lifecycle properties now read their original slots directly, removing
one Python wrapper per access. Explicit field replacement, frozen assignment,
deletion errors, and function identity retain their existing behavior. Custom
getters use the general collector program; this also fixes a dynamic `step`
getter returning 6 where that program returned 14.

The group loop refreshes cached functions after key lookup. This lets temporary
hooks installed by a selector and replaced by hashing release at the expected
point, including when lookup raises. The group entry also returns its existing
lazy iterator directly instead of forwarding every output through another
generator. Collector checks still happen when consumption starts.

These changes passed their local comparisons against the preceding correct
versions. They recover part of the Python grouping cost; the remaining key
selection regression is described above.

### Exact type checks without user callbacks

Source metadata and execution guards now compare builtin types by identity.
Previously, a custom metaclass comparing itself equal to `list` could make
`count()` trust an unrelated length: a source yielding three items could report
99. Synchronous and asynchronous counting now traverse that source.

The correction also covers iterator length hints, grouped keys, spill
preaggregation, range `index_of()`, and float constants. Regression checks retain
callback counts, one-shot consumption, and cleanup.

### Traceable benchmark reports

Both benchmark suites record dependency versions, Git revision and dirty state,
source and native fingerprints, and workload identity. They recheck this evidence
after measurement. Report schema 6 requires it, and median baselines retain the
metadata of each original run. Older reports must be regenerated.

The same workload can compare different commits and package versions. Changes in
input size, domain, quick mode, dependencies, native profile, or runtime settings
make reports incompatible. Reports include CPU affinity, NumPy dispatch settings,
and warmup counts. Use `FPSTREAMS_BENCHMARK_SOURCE` to measure another checkout
with the same harness; see the [benchmark workflow](user-guide/performance.md#a-practical-tuning-loop).

Both suites now measure peak Python allocation in one separate task call after
timing. This includes temporary objects and the task's output, but excludes
prepared inputs and native allocations not tracked by Python. Missing or invalid
resource measurements are rejected; a baseline cannot fill a missing value with
zero. Earlier competitive reports contained timings only and cannot establish
that an allocation limit passed. The engine-suite reports for two-key Rust
grouping did include Python allocation measurements.

### Separate benchmark listing from measurement

The engine CLI now lists scenarios without running benchmark tasks or collecting
timing, allocation, and execution reports. Listing shares domain, quick-mode, and
include filtering with normal runs, preserving order and optional-backend
availability. It creates small fixtures and releases them before returning.
If a later scenario builder fails, fixtures from earlier builders are released too.

### Benchmark thresholds for small inputs

Composite count/sum groups and NamedTuple callable joins now apply their extra
same-run speedup requirements from 1,000 rows. The ratios and the 300,000-row CI
workload are unchanged. Smaller inputs still emit timing and allocation data
for comparisons across runs.

Three local runs at each of eight sizes reproduced threshold failures at 1, 16,
and 64 rows. All measured sizes from 256 through 300,000 passed. At 1,000 rows,
the original limits also passed three runs each on CPython 3.11–3.14, using a
release native build. Task bodies and sampling were unchanged.

### Python join controls

Join benchmarks now apply each case's requested engine. Twelve Python controls
cover dict and Mapping records with field and callable keys. They include inner
and left joins with unique right keys, plus inner joins with repeated right keys.
Their reference tasks preserve output columns, unmatched rows, and snapshots
taken before key callbacks.

Six further cases use eight or 32 left fields. Each task checks complete outputs
before timing and records the observed route separately. Python controls report
`python_join`; the five auto-engine controls can run in Rust.

### Wide join layouts

Python joins compare field-name identities when reusing an output layout. For
left records with more than four fields, `map(operator.is_, ...)` performs these
checks without a Python generator. Repeated private dict snapshots are compared
directly with the cached names, avoiding a temporary tuple. Dictionary subclasses
and replaced tuple constructors keep the original path. Length checks,
short-circuiting, snapshot lifetimes, suffix-key identities, changing layouts,
and the 64-shape cache limit retain their existing behavior.

At 1,000, 10,000, and 100,000 rows, the eight- and 32-field dict tasks took about
20%–33% less time; the Mapping tasks with repeated matches took about 6%–7% less.
The two-field Python controls stayed within about 2% of the baseline. Those
measurements cover replacing the comparison generator.

Avoiding the temporary tuple reduced wide dict times by a further 7%–11% and
wide Mapping times by 1%–3%, measured against the version with the generator
already removed. Two-field Python controls ranged from about 1% faster to 2%
slower. Measured Python allocation peaks changed by -96 to +48 bytes; this does
not establish a general memory improvement.

Each change passed all nine comparisons of timing and Python allocation. Both
used CPython 3.12.3, a release native build, three runs per version and seven
samples per run at the three sizes. Prepared inputs were outside timing; these
results apply to the measured Python join tasks.

### Execution observations and pair reports

Benchmark tasks execute their original pipeline and terminal once outside the
timed samples to observe the route. One-shot tasks build a fresh source each time.
When no plan is recorded, the observation is explicitly `unknown`.

Top-level record joins now distinguish successful Rust and Arrow shortcuts from
Python execution. `Pairs.run_with_report()` supports `to_dict`, `group_values`,
`collect_values`, and `aggregate_values`, including direct native aggregation.
Reports describe the outer query and preserve a single terminal execution;
per-stage tracing remains future work. See [execution reports](user-guide/execution-reports.md).

### A smaller join execution entry point

The narrow integer-key record-join adapter now lives in the existing
`execution/relational/join.py`. The package entry point still selects the route
and supplies its field limit. Kernel order, fallback, and NamedTuple protocol
guards retain their existing behavior. This refactoring passed its local
comparisons without establishing an algorithmic speedup.

### Frequency counting beyond the integer prefix

Identity `frequencies()` can now continue counting in Rust after its bounded
integer prefix fills. Retained lists and tuples support exact builtin strings,
bytes, large integers, floats, booleans, and `None`, while keeping the first key
object and encounter order. Custom keys return to Python before their hash or
equality methods run. The continuation does not allocate a second growing table.

Other sources and transformed pipelines retain their existing routes. The new
continuation is disabled on free-threaded CPython. A missing native extension
uses Python; the source tree also fixes that fallback for browser-wheel lists
and tuples. Reports distinguish `rust_direct` from `python_frequency`.

Local CPython 3.12.3 comparisons measured about 10%–20% less time at 10,000 through
250,000 items in cases using the continuation. Inputs were prepared in advance,
and outputs were dictionaries. Counter remained faster for small inputs, and
NumPy had a large advantage on repeated numeric values. Eight array-input cases
now cover int64 and float64 counting, with and without a key, at low and high
cardinality. List-input tasks must include conversion costs. Check the complete
result, including first-key order, signed zero, and distinct NaN keys; sorted
unique values alone do not preserve the frequency dictionary's behavior.

The NumPy frequency benchmark comparator now checks those representations
directly. Ordinary dictionary equality accepted integer/float substitutions and
opposite signs of zero; it also rejected equivalent outputs with independently
boxed NaNs. Fifteen regression cases cover these boundaries, including count
types and separate NaN entries. Floating-point keys are compared by their bits:
`float.hex()` cannot distinguish NaN signs or payloads. Python and auto execution
both preserve those bits in the retained-array probe.

Automatic NumPy counting without a key now has an optional Rust iterator entry
point. It consumes the existing generator, preserving automatic source fallback
and QueryRuntime cleanup. It owns the counting dictionary and returns custom
keys to Python before hashing them. Older extensions and free-threaded CPython
keep the previous path. A direct bypass of the physical executor was rejected:
it produced equal counts but skipped runtime cleanup.

The 23 new checks cover source and counting errors, custom keys, fallback, old
wheels, reports, and floating-point bits. The complete suite passed 3,823 tests
with 10 skips; 211 Rust tests also passed. All nine local timing and allocation
comparisons passed at 1,000, 10,000, and 100,000 items. At 100,000 items, counting
distinct float64 values took about 22% less time. Int64 cases at 10,000 and
100,000 items took about 2%–5% less. Small inputs and repeated float64 keys were
largely unchanged. Python allocation peaks fell by only 72–140 bytes in the
unkeyed NumPy cases, which is not a substantial memory saving.

A later forwarding prototype passed its compatibility checks but was not
retained. Three local measurement batches, including one with fixed CPU
affinity, failed timing gates and showed large changes in control or reference
tasks. The existing executor remains in place.

Local diagnostics showed that allocation history can change minor page faults
and system CPU time. Explicit allocator settings and fresh processes reduced
some variation, but did not establish stable results for every workload.
Schema 6 records allocator environment settings and rejects missing or
mismatched values; those fields do not reconstruct process allocation history.

The final forwarding trial used one case per fresh process, three input sizes,
and alternating baseline and candidate runs. Five of 126 comparisons failed
the existing timing gates across 252 reports, including failures in fpstreams
and reference implementations. The prototype remains rejected. Do not keep
retrying it under new settings or remove failed samples. Any later executor
change must preserve source-opener exception conversion and delegate lifetimes.

`Counter(map(key, values))` is not a drop-in replacement for the keyed loop.
A local probe counted one hash call where the current lookup-then-assignment
loop made two, suppressing an exception from the second call. Regression checks
now cover both keyed and identity counting in Python and auto modes.

### Projection selector consistency

`Rows.select()` now checks captured field accessors before using cached
projection metadata. The generated Python loop calls the current projection
for each row. This fixes cases where NumPy, Arrow, or large dict inputs kept
reading an old field after the selector changed.

NumPy rechecks after opening its source; Arrow rechecks at that boundary and
between batches. Unknown batch openers retain the full input fields for Python
fallback, even when they support repeated reads. A changed selector can also
infer its output type instead of being forced into the old field's schema.

The repair has a measured cost. In three local runs at each size, forced-Python
dict projection took about 2.02 times as long at 10,000 rows and 2.35 times as long
at 100,000 rows. Those six comparisons failed the existing timing limit. All
three comparisons at 1,000 rows passed. The larger Python runs used 2,110 fewer
bytes at the measured Python allocation peak; this excludes prepared input and
untracked native allocations. Automatic dict projection and Mapping controls
stayed within about 2% of the baseline. Small NumPy projection tasks took about
7%–9% longer; the larger NumPy controls stayed within about 2%.

### File scan callbacks

CSV and Parquet projections now preserve selector changes made by scan openers,
including wrappers installed before a query is built. CSV retains all fields
when its public reader hooks differ from the extension entrypoints. Ordinary
readers still use the bounded schema probe and default data reader.

Parquet checks the live projection after opening the dataset and before
creating its scanner. The check also applies when the source has an explicit
filter. Changes to a retained dataset factory's function code are detected too.
Fallback continues from the opened source; it does not call the opener again.
Regression checks cover full materialization, batches, count, first, and the
original exception object from a changed selector.

All 85 new semantic checks fail on the retained baseline and pass with the
repair. The full suite passed 3,728 tests with 10 skips. Separate repeated file
benchmarks exposed a CPython 3.12.3 `tracemalloc` race. A native-thread probe
reproduces it without importing fpstreams or Arrow; it completes 100 rounds on
3.12.13, which also passes 20 rounds of the file benchmarks. See
[CPython #128679](https://github.com/python/cpython/issues/128679). Further
allocation comparisons use the patched interpreter for both baseline and
candidate.

On CPython 3.12.13, all nine paired timing and allocation comparisons passed
across 1,000, 10,000, and 100,000 rows. The four fpstreams tasks stayed within
about 2% of the baseline in median time. The release suite also passed all
297 checks on that interpreter.

### Selector snapshots

Python projection now iterates its temporary selector list directly, avoiding
the conversion to a tuple. It still reads the positional and alias lists for
each row. A callback can change the next row's selectors while the current row
keeps its original snapshot, including selector lifetimes and exceptions.

On CPython 3.12.13, 18 paired reports across 1,000, 10,000, and 100,000 rows
passed all nine timing and allocation comparisons. Python dict projection took
about 5% less time, expression projection 2%–3% less, and Mapping projection
1%–2.5% less. Automatic Arrow and NumPy paths stayed within about 2% of the
baseline. Peak Python allocation did not show a consistent reduction.
The complete suite passed 3,731 tests with 10 skips. Snapshot replacement and
exception-lifetime probes also passed on CPython 3.11–3.14.

## Next development steps

### Reduce the cost of live computed columns

The generated Python `with_columns` loop now calls the original enrichment
function. Captured accessors, expression evaluators, and sibling-list changes
remain visible in cached plans and during source consumption. The transform
copies the row first and evaluates selectors against the original input. NumPy
keeps its existing live checks and uses the same fallback semantics.

The 54 new regression cases include 52 failures on the retained baseline. They
cover dict, Arrow, and NumPy sources, materialization and column terminals,
source-open callbacks, adaptive compilation, and exceptions that stop further
source reads. The complete suite passed 3,785 tests with 10 skips; direct-field
binding probes also passed on CPython 3.11–3.14.

This repair has a measured cost. Across 1,000, 10,000, and 100,000 rows,
expression workloads took about 44%–119% longer, direct fields 6%–42% longer,
and callable columns 8%–27% longer. All nine timing comparisons flagged a
regression; the failed reports are retained. Mapping and NumPy controls changed
less. In new 100,000-row profiles, enrichment has the highest own time among
package functions for dict fields, expressions, and callables; record conversion
leads the Mapping case. Keep selector calls, row copying, and mutation visibility
in the acceptance criteria for further optimization.

### Preserve projection lifetimes while reducing overhead

Replacing every single-stage `select` loop with `map` changes when dropped
values are released. A generator source can observe that change when it resumes.
The counterexample reproduces on CPython 3.11–3.14, so the replacement was not
retained. The unreachable inline-select AST branch has been removed separately;
select stages still call the original projection.

Any further loop simplification must preserve both input-row and previous-result
lifetimes, including the adaptive transition and early close. Keep expression
selectors, Mapping inputs, and multi-stage fusion in the controls. The snapshot
change above retains these loops; it does not resolve the lifetime difference
between a generator and `map`.

### Review remaining join and row-builder costs

Start further join work with a profile of the current source. Wide layout checks
now avoid both the comparison generator and repeated temporary name tuples.
Earlier profiles include costs that have since been removed.
Check record snapshot order before moving allocations: Mapping keys, iteration,
and item access can invoke user code, including mutations during key selection.

For structural changes, review the remaining native join adapters and Python
group state in `execution/relational/__init__.py`, then the private projection
and reshape builders in `tabular/rows.py`. The row builders and their native
guards share function, closure, globals, and builtin identities. Trace those
bindings before moving a builder; extracting it needs to improve maintenance
while preserving its live hooks.

Validate structural and algorithm changes separately. Core execution changes need
the full Python checks; Rust changes need Rust checks too. Keep a performance
change only when comparable measurements support it.

### Reduce the cost of the shared collector loop

Start with the correct version that calls the key selector for every row. Use
the twelve Python record-group cases to compare dict fields, dict callables,
custom completion predicates, Mapping fields, mappingproxy fields, and first-value
groups with repeated and distinct keys. Earlier 100,000-row profiles showed
two field-selector calls per row; both must retain their normal protocol behavior.

The current loop has been profiled across those twelve cases. Keep guard checks
at iterator consumption and create the next output's key dictionary before
reading its finisher: releasing a previous output can replace that function.
Two attempts to move these steps changed results in local probes and were discarded.

Further changes must preserve live functions, completion flags, temporary-hook
release, first-key identity, lazy dispatch, timers, and cleanup. Keep the original
failed reports when assessing how much of the regression remains. A timing
threshold passing on its own is insufficient reason to retain a more complex
representation.

The selected key belongs in the group state: a custom key can compare unequal
during lookup and equal during insertion. The dictionary may then retain an
older key object than the new state. Taking output keys from that dictionary
would change identity.

### Separate planning from record output

Further planning changes need a profile from the current source. Scalar cache
fixes, header reuse, and shared AST location handling have changed those costs.
Any cache reuse must preserve changes to expression programs and callback
bindings between compilation steps.

Measure complete outputs when working on NumPy and Arrow boundaries. Small inputs
can spend much of their time in planning; larger ones may spend it creating
records. The existing measurements do not justify changing NumPy batching or
record conversion yet.

### Review the historical baseline in its target environment

This checkout has no reviewed `benchmarks/baselines/engine-v2.json`. Without one,
the scheduled workflow compares a fourth run with the median of three runs from
the same checkout. That checks consistency within a revision.

Build and review a historical baseline in the CI runner's environment. Local
reports retain unreviewed provenance and must not be copied into that role.
Earlier full-matrix runs contained reference-task outliers; isolated replays
passed, and a minimum 1ms warmup reduced one observed source of variation.
The original flagged reports remain available. Those replays neither explain
every outlier nor establish a product speedup.

### Later candidates

- Additional streaming joins and merge operations for already-sorted inputs.
- More approximate or bounded statistical aggregators.
- Richer plan diagnostics and structured execution metrics.
- Additional `Option`/`Result` traversal helpers.
- Narrow extension hooks for custom sources, aggregators, and native kernels.

These ideas remain unassigned. Later work must preserve API consistency, memory
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
