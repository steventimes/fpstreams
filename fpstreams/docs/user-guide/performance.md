# Performance and execution

Start with `auto`. It can keep small or opaque work in Python, fuse numeric work
into Rust, retain an Arrow prefix, or combine backends at a guarded boundary.
Forcing `native` does not make an unsupported plan faster; it turns a normal
fallback into `NativeUnsupportedError`.

## Inspect before changing code

```python
from fpstreams import flow, item

pipeline = flow(range(1_000_000)).map(item * 3 + 1).filter(item % 2 == 0)
print(pipeline.explain(terminal="sum"))
```

The explanation answers the questions that matter for performance:

- Is the source reiterable, exact-sized, ordered, or known infinite?
- Which engine handles streaming and which handles materialization?
- Does execution scan or copy the source at a backend boundary?
- Which expression or relational stages were compiled?
- Where does an Arrow plan hand records back to Python?
- Does a terminal need all input, only a prefix, or no scan at all?

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

Choose the form that communicates your operation. Replacing a readable callable
with an expression is worthwhile when the expression model fits exactly; do not
encode complex business logic just to chase a backend.

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

- `first`, `any`, `all`, `find`, `take`, and `nth` can short-circuit;
- `count`, `sum`, `min`, `max`, `mean`, `variance`, and `std` avoid building an
  output container;
- `summarize()` and `aggregate(...)` can compute several reductions during one
  traversal;
- `to_list()` and `to_tuple()` retain every result;
- exact sort, pivot, and most exact groups necessarily observe the whole input.

Calling separate scalar terminals on a one-shot source is invalid, and on a
reiterable source scans it repeatedly. Use a combined aggregation when the
statistics belong to one pass.

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

Set `validate` on joins when the relationship is known. It documents the data
contract and lets fpstreams reject invalid multiplicity before producing a
misleading result.

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

Limits prevent a pathological key or underestimated row from turning a bounded
operation into an unbounded one. Raising the limit without inspecting the data
only moves the failure.

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
- source type, input size, cardinality, skew, and null distribution;
- warmup policy and multiple timing samples;
- peak memory or bounded-resource counters where relevant;
- output equivalence and exception behavior;
- both common and adversarial shapes.

Compare end-to-end user tasks, not internal kernels with different semantics.
Pandas and NumPy begin with columnar or contiguous storage; an fpstreams pipeline
may begin with arbitrary Python objects. Include input preparation consistently
or state that the comparison begins after preparation.

## A practical tuning loop

Native, Arrow, and NumPy execution does not emit the same per-row Python trace
or profile events as the Python engine. When a debugger or Python profiler needs
those frames, run the pipeline with `with_engine("python")` while investigating
it.

1. Use `explain()` and a profiler to find the dominant stage.
2. Record a correctness-checked baseline across representative shapes.
3. Change one general mechanism, not one named benchmark scenario.
4. Re-run correctness, resource, and performance gates.
5. Keep the change only when the improvement is stable and the complexity is
   justified; revert neutral or noisy changes.

The repository benchmark follows this policy. It is a diagnostic and regression
guard, not part of production dispatch.
