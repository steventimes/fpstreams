# Frequently asked questions

## When should I use fpstreams instead of a comprehension?

Use a comprehension for a short, local transformation that already reads
clearly. fpstreams is most useful when a pipeline needs reusable lazy plans,
short-circuiting terminals, bounded async work, record operations, resource
cleanup, or optional native and columnar execution.

Small generic pipelines can be slower than a comprehension because planning and
dispatch have a fixed cost. Measure the complete workload rather than assuming
that every chain should use a native kernel.

## Should record data start with `flow()` or `rows()`?

Start new synchronous code with `flow()`. Nonconflicting record methods such as
`select()`, `with_columns()`, and `group_by()` enter a Rows view automatically.
Use `rows()` when you want an explicit relational view, record-specific I/O, or
a method whose Flow spelling already has another meaning.

## What does the `auto` engine do?

`auto` chooses a legal Python, Rust, Arrow, NumPy, or hybrid path for the source,
operations, and terminal. It falls back when an optimization cannot preserve
Python behavior. Use `explain(terminal=...)` before execution or
`run_with_report(...)` to inspect what actually ran.

Forcing `native` is mainly useful for parity checks and diagnosis. It raises
`NativeUnsupportedError` when the complete requested plan is unsupported.

## Why are `from_csv()` and `scan_csv()` separate?

`rows.from_csv()` follows Python's `csv.DictReader` behavior and produces string
cells. `rows.scan_csv()` uses PyArrow for typed, incremental reading and can push
selected columns into conversion. Keeping two names makes their parsing and
error contracts explicit.

## Can I run a pipeline more than once?

Lists, tuples, paths, and deferred factories are normally replayable. Iterators,
open handles, async iterators, Arrow readers, and imported Arrow C streams are
one-shot. Use `flow.defer(factory)` or a documented opener function when each
execution should acquire a fresh resource.

## Is fpstreams a distributed or GPU engine?

No. fpstreams is a local pipeline library. It can retain Arrow and NumPy data and
use guarded Rust kernels, but it does not silently move work to a cluster or GPU.
Use explicit interoperability boundaries when another system should own that
execution.

## How should I report a performance problem?

Include the fpstreams and Python versions, CPU and operating system, input size,
data types, key cardinality where relevant, the complete pipeline, and the
terminal. A result across several sizes is more useful than one timing. The
repository's `run_benchmark.sh` exposes the standard comparison matrix without
requiring CLI arguments.
