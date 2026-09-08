# Execution reports

`explain()` describes a plan without running it. `run_with_report()` executes
one eager terminal and returns its value, the recorded execution route, and
measurements of resources owned by the query.

```python
from fpstreams import flow

observed = flow([1, 2, 3, 4]).run_with_report("sum")

assert observed.value == 10
print(observed.report.compiler_engine)
print(observed.report.strategy)
print(observed.report.elapsed_ns)
```

The terminal runs once. Calling `run_with_report("sum")` does not first run an
explanation or repeat the source to collect metrics. If execution raises, the
same exception is propagated and no `ExecutionResult` is returned.

`Flow.run_with_report()` accepts its reportable eager terminals and forwards
additional positional and keyword arguments to the named method. `Rows` supports
`to_list`, `count`, `first`, and `last`. The current source tree adds Pairs
reporting for `to_dict`, `group_values`, `collect_values`, and `aggregate_values`.
This Pairs method is not included in the published 2.1.0 release:

```python
from fpstreams import flow

observed = flow([("a", 1), ("a", 2), ("b", 4)]).pairs().run_with_report("group_values")
assert observed.value == {"a": [1, 2], "b": [4]}
```

The AsyncFlow form is awaited:

```python
from fpstreams import aflow

observed = await aflow([1, 2, 3]).run_with_report("to_list")
assert observed.value == [1, 2, 3]
```

An unsupported name raises `ValueError`. Lazy transformations and plain
iteration are not terminals for this API.

## Report fields

| Field | Meaning |
| --- | --- |
| `terminal` | Eager method that was executed |
| `requested_engine` | Engine requested by the pipeline, such as `auto`, `python`, `native`, or `async` |
| `compiler_engine` | Engine selected by the compiler; metadata-only answers may report `not_compiled` |
| `strategy` | Recorded outer plan or direct terminal route, such as `planned:python`, `metadata`, or `async_scheduler` |
| `reason` | Explanation attached to that plan or recorded direct route |
| `elapsed_ns` | Wall-clock nanoseconds spent inside the terminal call, including query cleanup |
| `peak_owned_async_tasks` | Highest number of asyncio tasks owned by this query |
| `peak_spill_files` | Highest number of spill files held open by this query |
| `spill_bytes_written` | Cumulative framed spill bytes written by this query |

Resource counts cover tasks and spill files owned by the query. Use a system
profiler to measure process RSS, CPU usage, or resources opened by application
callbacks.

## Interpreting the route

The report records the outer plan and direct terminal paths. A top-level record
join that returns through a Rust shortcut reports `strategy="rust_direct"`;
the Python join executor reports `python_join`, including its spill path.
An Arrow join materialized directly from retained columns reports `arrow_direct`.
The outer relational plan can still have `compiler_engine="python"`; use
`strategy` to distinguish these terminal routes.

`Pairs.aggregate_values()` also records a successful Rust shortcut. Child plans
do not replace the outer route, and the report does not trace every kernel or
fallback inside a compound query. The direct join route labels above are also
unreleased; published 2.1.0 records the outer plan for those shortcuts.

For `frequencies()` on retained lists and tuples, the current source tree records
`rust_direct` when native counting completes. If Python completes the count after
a native prefix, it records `python_frequency`. The latter can include both
native and Python work; it does not measure the time spent in each.
Untransformed NumPy sources without a key use the same labels when an automatic
native plan uses iterator counting. Here `rust_direct` describes the counting
terminal; it does not prove that every upstream step avoided Python fallback.
Sequential linear `auto` plans with `frequencies(key=...)` also record
`python_frequency`: the Python pipeline preserves key callbacks before later
input reads. These labels are also unreleased.

Use the report with `explain()` and a profiler when investigating a particular
backend. A `planned:*` value alone cannot establish which backend handled all
of the work. The route labels can also change with input shape, installed
extensions, or fpstreams version; use them for diagnosis, not application
control flow.

## Runtime value types

`ExecutionResult` contains `value` and `report`. Both it and `ExecutionReport`
are immutable after the terminal finishes.

::: fpstreams.ExecutionResult
    options:
      show_root_heading: true
      members: true

::: fpstreams.ExecutionReport
    options:
      show_root_heading: true
      members: true
