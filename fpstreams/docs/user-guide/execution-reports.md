# Execution reports

## Static field dependencies (Unreleased)

`Rows.explain().to_dict(include_field_dependencies=True)["field_dependencies"]` describes logical input fields
without reading a row or evaluating a selector. It exports its own schema version,
source identifiers, explicit output fields, and any identity passthrough. A
dependency is a source/field pair; `None` means unknown and an empty list means
no declared field input. `complete_schema` distinguishes an explicit projection
from a source whose full schema is unknown. `passthrough_excludes` records renamed
fields that no longer pass through under their old names.

The analysis follows direct top-level projections, RowExpr computed columns,
renames, and closed RowExpr filters. It reports value dependencies separately
from `row_dependencies`, which affect which rows reach the result. Distinct logical
sources have separate identifiers. Join output fields are tracked when both input
schemas are established by projections and join keys are direct strings; shared
keys conservatively include both origins. Grouped keys and recognized aggregation
inputs are tracked; unsupported collectors remain unknown.

Opaque callbacks, unsupported transformations, and unrecognized join schemas
stop value inference. Nested selectors and some expression projections also
remain unknown. This metadata describes declared logical field inputs, rather
than per-record evidence or a proof about side effects in custom value objects.
It does not authorize optimization, establish purity, inspect records, or retain
source values. Runtime `ExecutionReport` observations keep their existing scope.

This analysis uses the shared synchronous explanation entry point. A Rows view
and its underlying Flow therefore produce the same explanation dictionary.
The default output keeps its existing fields and order; source analysis runs
only when explicitly requested.

Each export owns its expression-analysis cache, retaining only the most recent
root. Field states share that cache within the export, including after an
unknown transformation. Later exports analyze their current expressions again.

## Plan dictionary reference (Unreleased)

The synchronous Flow and Rows explanation objects share this method.
`AsyncFlow.explain().to_dict()` keeps its existing signature and does not accept
the field-dependency flag.

::: fpstreams.planning.explain.PlanExplanation.to_dict
    options:
      show_root_heading: true
      show_source: false

## Runtime observations

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
`to_list`, `count`, `first`, and `last`. Version 2.2.0 adds Pairs
reporting for `to_dict`, `group_values`, `collect_values`, and `aggregate_values`.
For example:

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

## Related inspection tools

| Tool | Information returned | Executes the input pipeline |
| --- | --- | --- |
| `explain(terminal=...).to_dict()` | Planned engines, stages, data movement, and static semantic `diagnostics` | No |
| `run_with_report(terminal, ...)` | Terminal value, observed route, resource counts, and bounded events (events added in 2.4.0) | Yes, once |
| `explain_reduction(collector)` | Collector declarations for combining partial states and reducer laws | No |

Static `diagnostics` describe completion, ordering, and state risks in the plan.
The execution report records observations from the terminal that ran. Reading
its events does not call `explain()` or execute the query again.

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
| `events` | Immutable tuple containing the first 64 plan and route observations; 2.4.0 |
| `events_truncated` | Whether further observations exceeded the 64-event limit; 2.4.0 |

Resource counts cover tasks and spill files owned by the query. Use a system
profiler to measure process RSS, CPU usage, or resources opened by application
callbacks.

## Metadata export (2.4.0) {#metadata-export-unreleased}

Use `report.to_dict()` to copy an execution report into plain metadata for
JSON storage or a caller-managed integration:

```python
import json
from fpstreams import flow

observed = flow([1, 2, 3]).run_with_report("sum")
metadata = observed.report.to_dict()
assert metadata["schema_version"] == 1
assert metadata["kind"] == "fpstreams.execution_report"
serialized = json.dumps(metadata)
```

The exported schema has its own version, independent of the package version.
It includes the report fields above, `metric_units`, `resource_scope`, and the
event limit and truncation flag. Each event becomes a new dictionary. The
terminal value remains in `observed.value`; export reads only the recorded
metadata and performs no query execution or network I/O.

Plan explanations also export `schema_version=1`,
`kind="fpstreams.plan_explanation"`, and a `mode` of `sync` or `async`.
These describe planned execution. Execution reports contain observations from
one completed terminal call.

An OpenLineage adapter can use this metadata as part of a custom facet. The
calling application must supply its Job, Run, and Dataset identities and own
the lifecycle events. The exported dictionary is an fpstreams report schema;
it does not infer those identities or form an OpenLineage RunEvent.

## Interpreting the route

The report records the outer plan and direct terminal paths. A top-level record
join that returns through a Rust shortcut reports `strategy="rust_direct"`;
the Python join executor reports `python_join`, including its spill path.
An Arrow join materialized directly from retained columns reports `arrow_direct`.
The outer relational plan can still have `compiler_engine="python"`; use
`strategy` to distinguish these terminal routes.

`Pairs.aggregate_values()` also records a successful Rust shortcut. Child plans
do not replace the outer route, and the report does not trace every kernel or
fallback inside a compound query. The direct join route labels above were added
in 2.2.0; version 2.1.0 records the outer plan for those shortcuts.

For `frequencies()` on retained lists and tuples, version 2.2.0 records
`rust_direct` when native counting completes. If Python completes the count after
a native prefix, it records `python_frequency`. The latter can include both
native and Python work; it does not measure the time spent in each.
Untransformed NumPy sources without a key use the same labels when an automatic
native plan uses iterator counting. Here `rust_direct` describes the counting
terminal; it does not prove that every upstream step avoided Python fallback.
Sequential linear `auto` plans with `frequencies(key=...)` also record
`python_frequency`: the Python pipeline preserves key callbacks before later
input reads. These labels were added in 2.2.0.

Use the report with `explain()` and a profiler when investigating a particular
backend. A `planned:*` value alone cannot establish which backend handled all
of the work. The route labels can also change with input shape, installed
extensions, or fpstreams version; use them for diagnosis, not application
control flow.

## Plan and route events (2.4.0) {#plan-and-route-events-unreleased}

`run_with_report()` collects bounded events from the existing plan and
direct-route hooks. Read `report.events` when a compound query's outer report
leaves a backend choice unclear. Child plans appear in encounter order while
the report's outer `compiler_engine`, `strategy`, and `reason` keep their usual
meaning.

```python
from fpstreams import flow

child = flow([2]).map(lambda value: value * 10)
observed = flow([1]).concat(child).run_with_report("to_list")

assert observed.value == [1, 20]
assert observed.report.events
for event in observed.report.events:
    print(event.kind, event.compiler_engine, event.strategy, event.reason)
```

Each immutable `ExecutionEvent` contains `kind`, `compiler_engine`, `strategy`,
and `reason`. The kinds are `plan` for a compiler or scheduler choice,
`strategy` for a recorded direct route, and `metadata` for an answer that avoids
compilation. A compiler choice alone does not prove which kernels processed
the input. A `None` compiler engine means the route hook supplied no independent
compiler decision. Repeated observations can appear,
and events do not form a complete execution tree or measure stage durations.

Reports keep the first 64 observations. When another event would exceed that
limit, the report sets `events_truncated=True`. Events retain diagnostic labels
and reasons, with no references to plans, callbacks, or input objects. Collection
does not replay the source or count individual input rows.

Only `run_with_report()` calls collect events; ordinary terminals need no
recorder. Each nested or concurrent report has its own event budget and result,
including awaited AsyncFlow reports. Terminal exceptions propagate unchanged,
without an `ExecutionResult`. All terminal keyword arguments remain available,
including aggregation names such as `diagnostics` or `max_events`.

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

::: fpstreams.ExecutionEvent
    options:
      show_root_heading: true
      members: true

Explicit sorted operations report `python_sorted_group`,
`python_sorted_merge`, or `python_sorted_join` when the outer query executes that
strategy. `explain()` describes a plan without executing it. Reports summarize
an outer query; they do not trace every nested stage. These route names were added
in 2.2.0.
