# Performance and execution

Start with `auto`. It can keep small workloads and Python callbacks in Python,
run supported numeric work in Rust, and preserve columnar execution in Arrow or
NumPy. Forcing `native` raises `NativeUnsupportedError` if the requested plan
cannot run there.

## Inspect before changing code

```python
from fpstreams import flow, item

pipeline = flow(range(1_000_000)).map(item * 3 + 1).filter(item % 2 == 0)
print(pipeline.explain(terminal="sum"))
```

Use the explanation to check:

- Is the source reiterable, exact-sized, ordered, or known infinite?
- Which engine handles streaming and which handles materialization?
- Does execution scan or copy the source at a backend boundary?
- Which expression or relational stages were compiled?
- Where does an Arrow plan hand records back to Python?
- Does a terminal need all input, only a prefix, or no scan at all?

The explanation describes planning decisions. An
[`execution report`](execution-reports.md) adds terminal and resource
observations, but neither is a complete trace of internal kernels and runtime
fallbacks. Confirm the relevant route before attributing a timing to a backend.

An O(1) `count()` is valid only when source metadata proves the exact output
cardinality and no operation invalidates it. A one-shot iterator is consumed in
the normal way.

## Source shape matters

The same values can carry different execution guarantees.

| Source | Replayability | Useful information | Typical consequence |
| --- | --- | --- | --- |
| `range` | Reiterable | Exact size and numeric structure | Strong native specialization without a Python list |
| exact `list` or `tuple` | Reiterable | Exact size and stable built-in layout | May use native kernels when conversion cost is justified |
| generator or iterator | One-shot | No safe replay | Avoid speculative paths that would pull twice |
| Arrow table or batch | Reiterable | Schema and column buffers | Retain compatible column operations in Arrow |
| Arrow stream/reader | Usually one-shot | Typed batches | Stream batches without pretending they can reopen |
| NumPy array | Reiterable | Live dimensions, dtype, and strides | Use compatible numeric kernels; read scalar values lazily on Python paths |
| dataframe protocol provider | Adapter-defined | Column metadata | Conversion is deferred where the protocol permits it |

Do not convert an iterator to a list merely to make it “optimizable” unless the
workload already requires full materialization. The copy can dominate the
operation and changes memory behavior.

## Expressions, direct selectors, and callables

Direct field/index selectors and `item`, `fitem`, or `col` expressions are
inspectable. The planner may lower them to a typed operation. Arbitrary callables
remain Python programs.

```python
# Inspectable
flow(values).map(item * 2).filter(item > 10)
flow(records).group_by("region")

# Opaque but fully supported
flow(values).map(custom_transform)
flow(records).group_by(lambda row: normalize(row["region"]))
```

Use an expression when it expresses the operation clearly. Keep a callable for
logic that would become harder to read as an expression.

In the unreleased working tree, `auto` can run a two-key count/sum group in Rust
when its input is a retained list or tuple of exact tuple rows. Both keys and
the selected values must be plain signed 64-bit integers. The result keeps the
first key objects and encounter order, and the sum may exceed 64 bits. Iterator
sources, custom types, changed collector functions, and unsupported layouts use
the Python collector program. This path is not in the published 2.1.0 wheel.

## Keep pipelines fused

Each terminal opens and executes a plan. Keep adjacent transformations in one
pipeline when they form one result:

```python
total = flow(values).map(transform).filter(keep).sum()
```

Materializing between stages creates extra allocation and prevents cross-stage
planning:

```python
intermediate = flow(values).map(transform).to_list()
total = flow(intermediate).filter(keep).sum()
```

Materialize deliberately when you need reuse, random access, a snapshot boundary,
or interaction with an API that requires a container.

## Select the right terminal

Use the narrowest terminal that expresses the answer:

- `first`, `any`, `all`, `find`, and `nth` can short-circuit; use the lazy
  `take` transformation to bound how many results a later terminal requests;
- `count`, `sum`, `min`, `max`, `mean`, `variance`, and `std` avoid building an
  output container;
- `summarize()` and `aggregate(...)` can compute several reductions during one
  traversal;
- `to_list()` and `to_tuple()` retain every result;
- exact sort, pivot, and most exact groups necessarily observe the whole input.

Calling separate scalar terminals on a one-shot source is invalid, and on a
reiterable source scans it repeatedly. Use a combined aggregation when the
statistics belong to one pass.

`frequencies()` retains one count per distinct key. The unreleased implementation
can count exact builtin values in retained lists and tuples in Rust, including
strings, bytes, floats, large integers, booleans, and `None`.

It keeps the first key object and encounter order. Custom keys are counted in
Python without extra hash or equality calls. Transformed pipelines keep their
existing execution paths. The new continuation is disabled on free-threaded
CPython.

For an untransformed NumPy source without a key, an automatic plan that selects
native execution can also count in Rust. It consumes the existing iterator one
item at a time, preserving source fallback and query cleanup. This removes the
Python counting loop; the selected numeric executor still materializes its
output before iteration. A missing iterator-counting entry point keeps the
previous path.

When the Rust extension is unavailable, `auto` counts in Python. The current
source tree also fixes this fallback for lists and tuples in the browser wheel.

For a NumPy source, a key callback may change values that have not been read yet.
The unreleased `auto` implementation preserves those edits by reading between
key calls on sequential linear pipelines. This route can cost more than copying
the array up front. Its execution report records `python_frequency`.

## Records and tabular data

Record operations have two different costs: extracting fields and creating owned
output records. A columnar source can avoid part of that work only while its
operations remain column-compatible. A Python callable or record-specific
protocol can create a boundary back to row execution.

For large typed CSV or Parquet data, prefer `scan_csv` or `from_parquet` with
projection and filtering. Use compatibility `from_csv` when its string-cell
semantics are what the application needs.

Joins and groups are sensitive to:

- key cardinality and skew;
- one-to-one, many-to-one, or many-to-many validation;
- output fan-out, not just input size;
- direct fields versus Python callbacks;
- whether an input can be safely partitioned or replayed.

Set `validate` on joins when the relationship is known. fpstreams raises when a
key violates it. A streaming consumer may already have received earlier rows
when a duplicate is encountered, so validation does not make downstream side
effects atomic.

## Bound large global work

Use external sorting or spill-enabled relational operations when the in-memory
working set is not acceptable. Configure limits from an operational budget:

```python
from fpstreams import SpillLimits, rows

limits = SpillLimits(
    max_partition_rows=250_000,
    max_partition_bytes=128 * 1024 * 1024,
    max_matches_per_key=20_000,
    max_output_rows=5_000_000,
    max_repartition_depth=4,
)

joined = rows(left).join(
    rows(right),
    on="id",
    partitions=32,
    limits=limits,
)
```

If a limit is exceeded, inspect key skew, row size, and join fan-out before
raising it. Spill processing can still fail when a single partition remains
too large after repartitioning.

## Async throughput

For I/O-bound work, `map_async` exposes concurrency and ordering separately.
Higher concurrency is useful only until the upstream service, connection pool,
CPU, or memory becomes the bottleneck.

```python
results = await aflow(urls).map_async(fetch, concurrency=16, ordered=False).to_list()
```

`ordered=False` can return completed work sooner. Bounded buffers and explicit
timeouts protect memory and latency. Short-circuiting or failure cancels owned
tasks; user-created background tasks are outside that ownership boundary.

## Measure representative work

A useful benchmark includes construction and conversions that the user actually
pays for, proves equivalent outputs, and samples more than one size and data
shape. At minimum record:

- Python and dependency versions, platform, and processor architecture;
- Git commit, dirty status, source and native-extension fingerprints;
- source type, input size, cardinality, skew, and null distribution;
- requested engine and observed execution route, including gaps in that evidence;
- warmup policy and multiple timing samples;
- peak memory or bounded-resource counters where relevant;
- output equivalence and exception behavior;
- both common and adversarial shapes.

Compare tasks with equivalent results and source-consumption rules.
Pandas and NumPy begin with columnar or contiguous storage; an fpstreams pipeline
may begin with arbitrary Python objects. Include input preparation consistently
or state that the comparison begins after preparation.

The frequency comparison cases start with prepared inputs: lists for fpstreams
and `collections.Counter`, arrays for NumPy, and Series for pandas. Every timed
task returns a dictionary; NumPy converts its result arrays in bulk, and pandas
uses `to_dict()`. The cases cover low and high cardinality for integers,
strings, large integers, and floats, plus a string distribution in which one key
accounts for 90% of the input. They compare counts using dictionary equality;
fpstreams' key identity and encounter order are checked separately in parity
tests. Input conversion is outside timing, and other distributions can change
the ranking.

Eight additional frequency cases start from NumPy int64 or float64 arrays. They
separate identity counting from a callable key, with repeated and distinct keys
in each group. All implementations return complete dictionaries in first-key
order. The Python reference converts the array to a list inside its timed task;
the NumPy and pandas references use equivalent array operations for the key.

The `io.arrow.csv.select` and `io.arrow.parquet.select` cases start with files
and query objects created outside timing. Each measured call reads the file,
selects two columns, and returns Python records. The pandas reference reads the
same file independently. These cases measure file reading and projection;
fixture creation and query construction are excluded.

The join cases also start with prepared records and return complete lists.
Twelve Python controls vary record type (dict or Mapping) and key selection
(field or callable). They cover inner and left joins with unique right keys,
plus inner joins with repeated right keys. Use these controls to assess changes
to Python execution, and check the observed route: auto-engine cases can run
in Rust.
Six further cases use eight or 32 left fields to measure wider record layouts.
Callable-key comparisons retain both key columns and copy records before calling
the selector. Left joins include unmatched rows and their missing right values.

## A practical tuning loop

Native, Arrow, and NumPy execution does not emit the same per-row Python trace
or profile events as the Python engine. When a debugger or Python profiler needs
those frames, run the pipeline with `with_engine("python")` while investigating
it.

1. Use `explain()` and a profiler to find the dominant stage.
2. Record a correctness-checked baseline across representative shapes.
3. Change one mechanism shared by the workloads that show the bottleneck.
4. Re-run correctness, resource, and performance gates.
5. Keep the change only when the improvement is stable and the complexity is
   justified; revert neutral or noisy changes.

The repository runner compares engine paths and offers a separate Python,
NumPy, and pandas comparison suite. List the available scenarios before choosing
what to measure:

```bash
python benchmark.py --list-scenarios --quick --include 'fpstreams_operation/sync/*'
python benchmark.py --competitive --list-scenarios --include 'rows.join.*'
```

Both suites support `--quick` and `--include` when listing. For the engine suite,
`--domain` selects integer workloads, floating-point workloads, or both. Engine
listing creates small fixtures and releases them before returning; it does not
run benchmark tasks or collect timing, allocation, or execution reports.

Both suites emit schema 6 reports with dependency versions, Git state, source
and native hashes, and a workload fingerprint. They also record CPU affinity,
NumPy CPU features and a fingerprint of selected kernel targets, plus SIMD,
thread-count, Python hash-seed, and allocator settings.
The allocator fields include `PYTHONMALLOC`, `PYTHONMALLOCSTATS`, `GLIBC_TUNABLES`,
and an allowlist of `MALLOC_*` environment controls. These are requested settings;
the report does not prove which allocator is active or reconstruct its allocation
history. The runners recheck that evidence after measurement and reject a run if it changes.
An unavailable Git repository is recorded explicitly with `git_available=false`
and null revision and dirty fields. Runtime configuration must match on both
sides; older reports need to be regenerated.

The engine suite can enforce ratios against its Python references from the same
run. Composite count/sum groups and NamedTuple callable joins apply their extra
speedup requirements from 1,000 rows. Smaller inputs still produce timing and
allocation records, with `maximum_ratio=null` for these cases.

Competitive reports describe cross-library timings without enforcing those
ratios. For comparisons across versions, `benchmarks/regression.py` checks each
task's timing and allocation against the baseline; this includes reference tasks
and small-input cases without a same-run speedup requirement.

The engine runner warms each task while calibrating a block to at least 5ms,
then uses the same call count for every timed sample. Sample times are block
elapsed time divided by the call count. Reports retain raw blocks and separate
calibration records for both full execution and first-row latency. These are
warmed measurements of the existing tasks. Identity cases reuse prepared
pipelines; operation cases retain their construction and cleanup work. They do
not measure first-call startup. The runner does not disable GC. Different sampling methods
cannot share a baseline, so regenerate references made with single-call timing.
The scheduled workflow also uploads all three reference runs for diagnosis.

The competitive runner rotates peer order, collects garbage, then warms the
next task for at least 1ms before timing one call. Each result records the number
of warmup calls. This measures warmed execution; first-call latency needs a
separate measurement. Local checks found that one warmup call left short NumPy
tasks with two timing ranges. Longer warmup removed that pattern, though other
full-matrix outliers still require investigation. Matching allocator settings
also does not reproduce process allocation history: local same-code checks
still found different timing bands under fixed mmap and trim thresholds. Check
stability with independent runs before evaluating a candidate. Keep failed runs;
a passing replay does not invalidate them, and hidden priming allocations can
change what the benchmark measures.

For sensitive cases, use an exact `--include` case ID and launch the CLI in a
new process for each run. This avoids allocations left by other cases in the
same suite process. Local checks of five affected tasks passed at 100,000 items
with this approach and matching allocator settings. Later candidate comparisons
still failed at smaller sizes, sometimes across several implementations in the
same run. Isolation alone does not prove stability. Check other sizes and
compare reports produced with the same invocation pattern; do not mix isolated
case runs with results from a shared suite process.

After timing, each implementation runs once more under `tracemalloc` to record
`resources.peak_allocation_bytes`. One-shot tasks create a fresh source for this
call too. The measurement includes temporary allocations and the result, while
prepared inputs remain outside its scope. Rust and other native allocations may
be invisible to `tracemalloc`; this is not process RSS or total memory usage.

Run these measurements on a patched interpreter. Older CPython builds can crash
when tracing stops while a native thread initializes; see
[CPython #128679](https://github.com/python/cpython/issues/128679). Local file-scan
stress checks passed on CPython 3.12.13 with the same dependencies that reproduced
the crash on 3.12.3. Record the full version and rerun both sides of a comparison
after changing interpreters.

Reports with missing or invalid resource values are rejected. Schema 4 competitive
reports did not measure allocation, so their timing results cannot establish a
memory gain or a passed allocation threshold.

NumPy's [runtime CPU dispatch](https://numpy.org/doc/stable/reference/simd/how-it-works.html)
can select different kernels on different processors or with different feature
settings. Do not disable those features just to make a comparison pass. Record
the actual configuration and compare equivalent runs.

Each fpstreams task runs once outside timing to observe its execution route.
This uses the same callable as the timed task; one-shot cases create a fresh
source for every call. `execution.status="unknown"` means that the task did not
record a plan. An `observed` outer route still has the limits described in
[execution reports](execution-reports.md).

To compare another checkout, point `FPSTREAMS_BENCHMARK_SOURCE` at its `src`
directory. Use the same runner and Python environment for both sides. Build
the release extension for each checkout first, and keep size, filters, domain,
and quick mode identical:

```bash
FPSTREAMS_BENCHMARK_SOURCE=/path/to/base/src .venv/bin/python benchmark.py \
  --competitive --size 10000 --repeats 5 --include 'rows.join.*' \
  --json artifacts/base-10000-run-1.json
.venv/bin/python benchmark.py \
  --competitive --size 10000 --repeats 5 --include 'rows.join.*' \
  --json artifacts/current-10000-run-1.json
```

Repeat both commands with `run-2` and `run-3` filenames. Keep the raw reports and
write the baseline to a separate file:

```bash
.venv/bin/python benchmarks/regression.py --create-baseline \
  --provenance local_one_shot_unreviewed \
  --output artifacts/base-10000-median.json \
  artifacts/base-10000-run-1.json artifacts/base-10000-run-2.json \
  artifacts/base-10000-run-3.json
.venv/bin/python benchmarks/regression.py \
  artifacts/base-10000-median.json artifacts/current-10000-run-1.json
```

Compare each current run against that baseline, then repeat at another size.
The baseline retains each run's provenance. Code hashes, Git revisions, and
fpstreams versions may differ between the two sides;
workload settings and other dependency versions must match. Old reports that
lack the required fields need to be regenerated.

Cross-commit checks in CI still need a reviewed baseline that matches the
runner's environment. Without one, the scheduled workflow compares three runs
with a fourth from the same checkout. That checks timing consistency within
one revision. The [roadmap](../roadmap.md) tracks the remaining baseline work.
