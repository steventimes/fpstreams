# Execution reports

`explain()` describes a plan without running it. `run_with_report()` runs one
eager terminal and returns its normal value together with the strategy and
query-owned resource measurements observed during that call.

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
`to_list`, `count`, `first`, and `last`. The AsyncFlow form is awaited:

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
| `strategy` | Concrete route used by the terminal, including direct, planned, metadata, or scheduler paths |
| `reason` | Short explanation for the selected route |
| `elapsed_ns` | Wall-clock nanoseconds spent inside the terminal call, including query cleanup |
| `peak_owned_async_tasks` | Highest number of asyncio tasks owned by this query |
| `peak_spill_files` | Highest number of spill files held open by this query |
| `spill_bytes_written` | Cumulative framed spill bytes written by this query |

The resource fields are deliberately narrow. They do not sample process RSS,
CPU usage, unrelated asyncio tasks, or files opened by application callbacks.
Use a system profiler when process-wide measurements are required.

`strategy` and `reason` tell you what happened in this execution. They can
change when the input shape, requested engine, installed native extension, or
fpstreams version changes. Treat them as diagnostics rather than application
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
