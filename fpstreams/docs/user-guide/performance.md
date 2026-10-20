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

Arrow's Parquet execution plan can read, cast, and fail before delivering a
batch. `first()` and limited queries stop once they receive enough output, but
cannot guarantee a result before a native source error. Source `columns` defines
which fields are visible; a later query projection does not remove errors from
other visible source fields. Batch size bounds batches, not the backend's I/O
or error timing.

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

Version 2.2.1 reduces planning overhead for untransformed exact
lists and tuples. Small sums keep their Python route, and compatible integer
containers use the same native crossover as before. Terminal timings still
include planning, so a direct Python operation can be faster for small inputs.

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

Since 2.2.0, `auto` can run a two-key count/sum group in Rust
when its input is a retained list or tuple of exact tuple rows. Both keys and
the selected values must be plain signed 64-bit integers. The result keeps the
first key objects and encounter order, and the sum may exceed 64 bits. Iterator
sources, custom types, changed collector functions, and unsupported layouts use
the Python collector program.

## Computed-column setup in development

Supported single-stage Python `with_columns()` queries now reuse a loop template
with separate callback bindings for each execution. The selectors stay in a live
list and receive the original row after its shallow copy is made. This development
change reduces fixed setup cost, which matters most for smaller inputs. See the
[local measurements](../roadmap.md#computed-column-setup-after-221) for the tested
sizes and limits.

## Projection setup in development

Supported single-stage Python `select()` queries use the same loop template as
computed columns, with separate callback bindings for each execution. Each row
still takes its selector snapshot before running the first selector. The
existing small direct-field route and native/Arrow routes keep their boundaries.
See the [local projection measurements](../roadmap.md#projection-setup-after-computed-columns)
for the measured input sizes and controls.

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

`frequencies()` retains one count per distinct key. Since 2.2.0, the implementation
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
The `auto` implementation preserves those edits by reading between
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

## Share a submission budget (2.4.0) {#share-a-submission-budget-unreleased}

Several queries can reuse one executor while keeping their own `buffer` limits.
To cap their combined unfinished work, give them the same `BoundedExecutor`:

```python
from concurrent.futures import ThreadPoolExecutor
from fpstreams import BoundedExecutor, flow

with ThreadPoolExecutor(max_workers=2) as pool:
    with BoundedExecutor(pool, max_pending=4) as budget:
        result = flow(range(10)).map_parallel(
            abs, executor=budget, buffer=8,
        ).to_list()
    assert pool.submit(abs, -1).result() == 1
```

Run independent queries from separate caller threads and pass `budget` to each.
Calls blocked in `submit()` wait for capacity in arrival order. Completion or
cancellation releases a slot. Shutdown wakes blocked submitters with
`RuntimeError`; `cancel_futures=True` cancels this wrapper's queued calls.
Running callbacks must finish cooperatively. Work submitted directly to `pool`
does not count against the wrapper's budget.

The shared limit counts tasks, not bytes or completed results. A blocked
submitter can still retain the item it is submitting. Keep per-query buffers
and batch sizes within the application's memory budget. Avoid waiting for
nested work that needs the slots occupied by its own parent callbacks.

## Preserve Arrow batches

Use `Rows.arrow_batches()` to pass columnar data to a batch consumer. In
2.4.0, a primitive Arrow table or record batch can keep its buffers
when the supplied schema exactly matches the output, including field and
schema metadata. Safe direct-field projections and primitive filters can run
columnarly; filters may allocate output buffers. The output still respects
`batch_size`, and an explicit zero-column schema preserves empty-record counts.

```python
import pyarrow as pa
from fpstreams import col, rows

table = pa.table({"id": [1, 2, 3], "value": [2.0, None, 4.0]})
batches = rows.from_arrow(table).arrow_batches(
    batch_size=2, schema=table.schema,
).to_list()
assert [batch.num_rows for batch in batches] == [2, 1]

projected = (
    rows.from_arrow(table)
    .where(col("id") > 1)
    .select(amount="value")
    .arrow_batches(batch_size=2, schema=pa.schema([("amount", pa.float64())]))
    .to_list()
)
assert projected[0].to_pylist() == [{"amount": None}, {"amount": 4.0}]
```

A cast, changed metadata, Python callback, or unsupported source uses the
existing conversion path. Buffer reuse can keep the source allocation alive;
make an explicit copy when independent storage is required. Measure the entire
consumer pipeline before treating a cheaper export as an application speedup.

Version 2.4.0 also supports multiple safe direct comparisons followed
by a direct projection. Parquet uses only the leading comparison as a scan
hint and retains every filter in its original order. Automatic hints must not
hide source conversion errors: visible UTF-8, temporal, and nested fields are
validated before automatic filtering or projection. These fields can require
reading columns that the query later discards. Explicit source `columns` and
`filter` define the source boundary before this validation.

Unreleased changes also recognize list, large-list, and fixed-size-list fields
whose children contain only supported builtin scalars or lists. Full buffer
validation still precedes automatic filtering or projection, including validation
of unused list fields. Extension, temporal, struct, and map values retain the
conversion path. A Python list result still owns independent row dictionaries
and selected list values.

For primitive batches, supported comparison chains can share a boolean
selection mask and filter the payload once. A count can use the mask directly.
This path requires the existing source-conversion and type guards; empty,
nullable, or unsupported inputs keep their existing execution paths.
The tradeoff is evaluating each supported comparison on the original batch
instead of filtering after every comparison. Offline Optuna tuning of row and
column thresholds favored sharing on all nonempty supported batches in the
measured shapes; this does not change the source boundary or establish an
application speedup.

A default local source with one Parquet fragment can omit safe unused fields
from its scan request, retaining filter dependencies and every field whose
conversion needs validation. Multiple fragments or a supplied dataset schema
can require Arrow casts even for an integer column. Those scans retain the
complete caller-visible projection before applying automatic query hints, so
an unused column's cast failure remains observable. Fragment counting stops
after two entries and does not inspect extra physical schemas.

`scan_csv()` reads and validates all caller-visible typed columns before query
projection. Inferring a type from the first block does not prove that later
values fit it. To exclude a column at the source boundary, supply PyArrow
`ConvertOptions(include_columns=[...])` explicitly.

`polars_batches()` also preserves the height of zero-column batches. A Parquet
write rejects nonempty rows without columns to avoid losing their row count;
an existing output file is preserved when conversion fails.

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

Computed-column cases separate field, row-expression, and callable selectors.
They cover five- and 32-column records, dict and MappingProxyType inputs, and
explicit Python controls. Every task returns a complete list of copied records;
NumPy remains a separate control. Query and input construction are outside
competitive timing, while terminal planning and execution are included.

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

Further local candidates start from the verified development source; a separate
comparison with released `v2.2.1` checks the combined changes. Earlier profiles
suggest collector loops and process submission, but those costs need to be
reproduced on the chosen reference source. The
[roadmap](../roadmap.md#next-development-steps) records the candidate order and
the callback, lifetime, and cleanup requirements for each stage.

The repository runner compares engine paths and offers a separate Python,
NumPy, and pandas comparison suite. List the available scenarios before choosing
what to measure:

```bash
python benchmark.py --list-scenarios --quick --include 'fpstreams_operation/sync/*'
python benchmark.py --competitive --list-scenarios --include 'rows.join.*'
python benchmark.py --competitive --list-scenarios --include 'rows.with_columns*'
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

Keep first-call startup separate from the warmed runner. Before measuring a
candidate, stop other heavy checks and record both reference and candidate runs
with the same filters, shapes, and process layout. Retain at least three
independent reference runs and repeat the candidate with the existing seven-sample
comparison setup. After a shared-loop change, include the full engine matrix and
the unaffected reference tasks. A local comparison does not create a reviewed
CI historical baseline.

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
one revision. These four engine runs collect 21 samples per scenario. Engine
timing blocks also record process CPU time for diagnosis; regression checks
continue to use wall time and the existing limits. The
[roadmap](../roadmap.md) tracks the remaining baseline work.

## Caller-owned executors

Version 2.3.0 accepts `executor=` in `map_parallel()` and `parallel()`.
An external pool can serve repeated queries without restarting its workers:

```python
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context

from fpstreams import flow


def square(value):
    return value * value


if __name__ == "__main__":
    with ProcessPoolExecutor(max_workers=2, mp_context=get_context("spawn")) as pool:
        query = flow(range(100)).map_parallel(square, executor=pool, buffer=4)
        first = query.to_list()
        second = query.to_list()
        assert first == second
```

The caller creates and closes the external pool. Each query cancels its own
queued futures and waits for its own running mappers on early exit or failure.
It leaves unrelated pool work untouched. Supplying `workers` with `executor`
raises `ValueError`; the supplied pool determines its backend. Set `buffer`
explicitly for the workload. Its default remains twice the CPU count when
`workers` is omitted, without inspecting private executor attributes.

`parallel(executor=pool, buffer=4)` applies the same settings to following
maps. `sequential()` clears those defaults for subsequent maps. Existing calls
without an external executor keep their worker/backend defaults and create
owned pools. Reuse can reduce worker startup cost; it does not make CPU-bound
callbacks faster by itself.

## Choose a batching bound

| Workload | Existing operation | Bound |
| --- | --- | --- |
| A callback accepts a fixed number of items | `map_batches(function, size=...)` | Items per callback, with a shorter final batch |
| Payload sizes vary | `batch_by_size(max_size, max_count=..., get_size=...).map(function)` | Sum of measured sizes and items per batch |
| An async source may pause between items | `buffer_timeout(max_count, seconds).map(function)` | Item count or time from the first buffered item |
| Several queries share a pool | `BoundedExecutor(pool, max_pending=...)` (2.4.0) | Unfinished calls submitted through that wrapper |

Set `max_count` alongside a size bound when individual items can have zero
measured size. Choose `get_size` for the payload or service limit being enforced;
it does not measure Python object overhead or process memory. For example:

```python
from fpstreams import flow

payloads = [b"a", b"12345678", b"ab", b"1234"]
batches = flow(payloads).batch_by_size(10, max_count=3).to_list()
assert [sum(map(len, batch)) for batch in batches] == [9, 6]
assert [item for batch in batches for item in batch] == payloads
```

Changing batch boundaries changes callback invocations and can change results.
Keep that choice explicit. When tuning an expensive callback, compare several
fixed sizes with the same input and worker policy, checking output, first-result
latency, complete execution time, and retained payload separately. A task-count
budget does not bound the byte size of results retained by a slow consumer.

## Process mapping and explicit batches

Without `executor=`, `map_parallel()` creates a worker pool for each execution,
and its process backend uses `spawn`. A supplied pool keeps the caller's start
method. Mapping submits one source item per future. `buffer` bounds submitted
futures, while `ordered=True` emits results in source order. Closing early
cancels queued work and waits for running callbacks to finish, so measure both
first-result latency and the time until the terminal returns.

Process callbacks must be importable and picklable. Keep them at module scope,
and launch a script's pipeline under an `if __name__ == "__main__"` guard.
The following example uses `map_batches()`, added in 2.3.0.
Installed 2.2.1 can use `chunk(4).map_parallel(...)` instead.
Each batch callback receives a tuple and emits one result. Expand iterable
results explicitly with `flat_map(iter)`:

```python
from fpstreams import flow


def square_batch(batch):
    return tuple(value * value for value in batch)


if __name__ == "__main__":
    result = (
        flow(range(17))
        .parallel(backend="process", workers=2, buffer=4)
        .map_batches(square_batch, size=4)
        .flat_map(iter)
        .to_list()
    )
    assert result == [value * value for value in range(17)]
```

With batch mapping, `buffer` counts batches, each holding up to `size` original
items. A batch callback can process items beyond the first result a downstream
consumer needs. It can also fail before returning earlier results from that
batch. Use batches only when that execution unit fits the application;
it does not preserve all the timing boundaries of item-wise mapping.

Compare item-wise and batched work under the same worker count, spawn policy,
and input. Match the maximum submitted original-item budget and record the
future count separately: batches of four with `buffer=4` hold up to 16 original
items, so the item-wise control needs `buffer=16`. Include cheap, medium-cost,
and expensive callbacks, full consumption, early exit, and callback failure.
Record pool startup, warmed throughput, first-result latency, and final cleanup
separately when evaluating executor reuse or batch mapping. The new method
composes the existing chunk and map operations; it introduces no automatic
batch-size selection or separate scheduling engine.

Async batch mapping also accepts synchronous and asynchronous callbacks. It
defaults to one call in flight. Explicit concurrency changes the unit of work
to a batch. `timeout` is cooperative: callbacks run on the event loop, so a
callback that blocks that loop cannot be interrupted by timeout or cancellation.
Use asynchronous I/O in async callbacks:

```python
import asyncio
from fpstreams import aflow


async def total_batch(batch):
    await asyncio.sleep(0)
    return sum(batch)


async def summarize():
    result = await (
        aflow(range(7))
        .map_batches(total_batch, size=3, concurrency=2, buffer=2)
        .to_list()
    )
    assert result == [3, 12, 6]


if __name__ == "__main__":
    asyncio.run(summarize())
```

Both forms include a shorter final batch and leave callback results intact.
Empty sources do not call the mapper. Early exit can skip some submitted
batches; callbacks already running in a sync pool finish before query cleanup,
while async callbacks are cancelled and awaited.

For `flow.defer(factory)`, each terminal calls the factory again. When repeated
file reads dominate, compare a file-backed pipeline with an explicitly prepared
input as separate workloads. Keep the input's freshness and memory requirements
visible; automatic replay or materialization would change the source contract.

## Choosing explicit sorted operations {#choosing-explicit-sorted-operations-current-source}

Use `group_by_sorted()`, `merge_sorted()`, and `join_sorted()` when inputs are
already in ascending key order and you need incremental output or bounded state.
They do not sort the input for you. Added in 2.2.0, they execute in Python.

Version 2.3.0 avoids the selector generator and wrapper tuple for
one-key grouping. The selected key still retains its identity, callbacks remain
live, and type/order validation and lookahead are unchanged. A tuple-valued
single key uses this route; grouping by multiple selectors keeps the existing
path. Callback `StopIteration` retains its `RuntimeError` wrapper and cause.

Local October 11 comparisons used CPython 3.12.13, the same release native
extension, CPU 2, and single-thread settings. Each side ran three independent
processes at each size, with seven samples per implementation. Nine comparisons
passed the existing time and Python-allocation limits.

| Prepared rows | Integer key time saved | String key time saved | Bytes key time saved | Tuple-valued key time saved |
| --- | --- | --- | --- | --- |
| 1,000 | 20.0% | 19.5% | 19.4% | 10.8% |
| 10,000 | 19.2% | 18.5% | 20.2% | 13.1% |
| 100,000 | 18.0% | 18.3% | 18.6% | 12.4% |

These are complete-consumption sorted-group measurements against the preceding
development implementation. Merge and join controls stayed within about 1%.
They do not establish an advantage over ordinary hash grouping or an application
speedup. Prepared inputs and native allocations are excluded from Python
allocation tracing; the measurements are not RSS or a reviewed CI baseline.
Raw reports and comparisons are in `artifacts/next-api-feedback-20261011/`.

The sorted group holds current collector state and one lookahead row; collectors
that retain values can still grow. A merge holds one lookahead per input. A join
holds the current right key group and lookahead, with finite row budgets. Rows
can contain large objects, so row limits are not byte limits. Large duplicate
right groups delay the first joined row. Checks apply to the consumed prefix;
short-circuiting does not validate an unread tail.

The September 29 measurements on CPython 3.12.13 compared these APIs with ordinary grouping,
concatenation plus stable sorting, and hash joins in the same checkout, using the
Python engine. Inputs were prepared records in lists or fresh generator views,
with duplicate and skewed keys, at 1,000, 10,000, and 100,000 rows. Three sequential
workers recorded seven samples per task. Data construction, route observation,
complete-content assertions, and allocation tracing were outside timing. Each
sample ran the actual Flow/Rows operation, including construction and planning.

Across these cases, complete consumption took about 4.93 times as long for sorted
grouping, 12.38 times for sorted merging, and 3.71 times for sorted joins. First-row
and early-stop cases were faster overall, but skewed joins could still regress.
The worst individual case was a complete sorted merge at about 14.94 times its
ordinary counterpart. All three workers agreed on the overall direction within
each operation/consumption group. These results do not establish a universal
speed advantage. Prefer ordinary operations when full-consumption throughput is
the priority, and measure your actual data before choosing a sorted path.

Measured Python allocation peaks were lower in aggregate. Those peaks exclude
prepared input objects and untracked native allocations, and are not RSS. The
raw local reports and worker summaries are under
`artifacts/next-releases/20260929T210456Z/D03/`; generated reports are ignored and
are not published as a CI baseline.

The competitive runner includes 54 `sorted.*` cases for full consumption, first
output, and stopping after 16 outputs. It validates complete ordered values
before timing, including full-consumption tasks whose timed terminal is `count()`.
For larger benchmark sizes, join row budgets explicitly scale to cover the
fixture; library defaults remain unchanged. To reproduce this local comparison:

```bash
uv run python benchmark.py --competitive --include 'sorted.*' \
  --size 10000 --repeats 7 --json artifacts/sorted-10000.json
```
