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

Version 2.5.0 uses `itertools.batched` on Python 3.12+ for eligible
retained-list chunks and exact generator, map, and filter cursors. Custom cursors
and Python 3.11 keep the canonical iterator path. Strict batches still reject a
short tail during consumption; early termination retains source cleanup.

Rolling statistics can use a Rust window state while selectors execute in Python.
Non-null values are converted once before Rust updates the state.
`with_engine("python")` selects the reference state. Unsupported native
capabilities and configuration values retain the Python implementation.

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

Version 2.5.0 skips structural fingerprints for row expressions,
whose compiled programs retain query-specific selector and literal bindings.
This reduces setup work and lets deep expressions reach the iterative evaluator
when direct AST compilation exceeds its bounds. Effect analysis visits shared IR
nodes once; evaluation still runs every occurrence, including repeated selectors
and callbacks. Scalar stages reuse their validated fingerprint across the
compiled-program and physical-template caches. Cache keys, capacity, and locking
are unchanged; repeated queries still bind their current source and terminal.

NumPy filter lowering accepts only exact built-in integer and Boolean literals.
Custom numeric types use Python comparison semantics without invoking their
metaclasses' hashing or equality during that eligibility check.

Since 2.2.0, `auto` can run a two-key count/sum group in Rust
when its input is a retained list or tuple of exact tuple rows. Both keys and
the selected values must be plain signed 64-bit integers. The result keeps the
first key objects and encounter order, and the sum may exceed 64 bits. Iterator
sources, custom types, changed collector functions, and unsupported layouts use
the Python collector program.

## Computed-column setup in development

Supported single-stage Python `with_columns()` queries reuse a loop template
with separate callback bindings for each execution. Selectors remain live and
receive the original row after its shallow copy is made.

## Projection setup in development

Supported single-stage Python `select()` queries use the same loop template as
computed columns, with separate callback bindings for each execution. Each row
still takes its selector snapshot before running the first selector. The
existing small direct-field route and native/Arrow routes keep their boundaries.

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

For large typed CSV or Parquet data, consider `scan_csv` or `from_parquet`.
CSV queries convert all caller-visible columns before applying a projection;
`select()` does not skip conversion errors in unused columns. Pass Arrow CSV
`ConvertOptions(include_columns=[...])` to restrict the reader explicitly.
Parquet scans can push supported projection and filtering into the reader.
Use compatibility `from_csv` when its string-cell semantics are what the
application needs.

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

When evaluating a replayable Flow from multiple threads, keep shared inputs
unchanged and use callbacks that are safe to call concurrently. Each evaluation
creates its own Collector state through the initializer; a custom initializer
must return independent state. A one-shot source still permits only one
evaluation; competing callers receive `FlowConsumedError`.

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

2.5.0 changes also recognize list, large-list, and fixed-size-list fields
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

Synchronous callbacks still run on the event loop. Increasing async concurrency
does not make a CPU-bound callback run in parallel, and a callback that blocks
the loop also delays cooperative timeouts and cancellation.

In the 2.5.0 source, async file cancellation waits for an in-flight
filesystem operation to finish before closing its handle. A stalled filesystem
call can therefore delay cancellation indefinitely. Measure the complete query,
including cleanup, and measure cancellation until the cancelled task finishes.
Keep Python allocation peaks, native allocations, and process RSS separate;
bounded item counts alone do not establish a byte-level memory bound.

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

The sorted group holds current collector state and one lookahead row; collectors
that retain values can still grow. A merge holds one lookahead per input. A join
holds the current right key group and lookahead, with finite row budgets. Rows
can contain large objects, so row limits are not byte limits. Large duplicate
right groups delay the first joined row. Checks apply to the consumed prefix;
short-circuiting does not validate an unread tail.

## 2.5.0 materialization changes

For retained primitive Arrow Tables and RecordBatches, `to_list()` can collect a
complete compatible columnar prefix directly. Streaming, unsupported conversion,
and incomplete-prefix plans retain their existing paths and cleanup contracts.

Opt-in field-dependency export reuses computed-column analyses within each step.
