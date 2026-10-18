# Flow

`Flow[T]` is the primary synchronous lazy pipeline for both ordinary values and
records. Transformations return a new pipeline; terminal methods execute its
plan.

Use `flow(source)` for an existing source and `flow.defer(factory)` when every
execution must create a fresh iterable. Besides ordinary iterables,
`flow(source)` retains supported PyArrow, pandas, Polars, Arrow C stream, and
dataframe-interchange inputs.

## State-driven generation and mapping (unreleased)

`unfold` builds a source from state, while `map_accum` transforms an existing
source with state. They use different tuple orders:

| Operation | Callback result | Emitted value |
| --- | --- | --- |
| `flow.unfold(seed, function)` | `(value, next_state)` or `None` to stop | `value` |
| `query.map_accum(initial, function)` | `(next_state, output)` | `output` |

```python
from fpstreams import flow

countdown = flow.unfold(3, lambda n: (n, n - 1) if n else None)
assert countdown.to_list() == [3, 2, 1]
assert countdown.take(1).to_list() == [3]
```

The unfold seed is not emitted by itself. Returning `(None, next_state)` emits
an actual `None`; only returning `None` ends generation. The callback controls
termination, so bound an infinite transition with `take` or another stopping
operation. Construction and `take(0)` do not run the callback.

```python
from fpstreams import flow


def running_total(total, value):
    next_total = total + value
    return next_total, {"value": value, "total": next_total}


query = flow([2, 3, 5]).map_accum(0, running_total)
assert query.to_list() == [
    {"value": 2, "total": 2},
    {"value": 3, "total": 5},
    {"value": 5, "total": 10},
]
```

`map_accum` calls the function once per consumed item in encounter order. It
emits nothing for an empty source and does not emit its final state. Each
evaluation starts from the supplied initial state; mutable seed objects are
reused by reference, following `scan`. Use `flow.defer` to allocate fresh
mutable state for each evaluation. The operation keeps the source's replay
and cleanup rules and runs its state transitions sequentially even with
parallel map defaults.

## Reuse sliding-window aggregates (unreleased)

`window_reduce(size, combine, step=1)` combines each count-based window without
building a tuple and folding every overlapping value again:

```python
from operator import add
from fpstreams import flow

assert flow([2, 3, 5, 7]).window_reduce(3, add).to_list() == [10, 15]
assert flow([2, 3]).window_reduce(3, add).to_list() == [5]
```

`combine` must be associative and must not modify either argument. Source values
and returned aggregates must remain unchanged during iteration because later
windows may reuse them. Encounter order is preserved, so a noncommutative
associative function is supported. Floating-point addition is not associative;
use `window(...).map(...)` when a left fold or arbitrary callback behavior matters.
There is no automatic conversion of existing window pipelines.

Empty input emits nothing. Initially short input emits one partial result;
after a full window, incomplete trailing windows are omitted. `step` may exceed
`size`; values in the gaps are consumed without being combined. Overlapping
windows can group calls differently from a left fold. A combine error can arise
while building a trailing window that would never be emitted. Stopping downstream
closes the owned iterator chain without pulling another item.

Overlapping windows use two aggregate stacks, following the storage insight in
[Two-Stacks Lite](https://arxiv.org/abs/2009.13768). They cache at most `size + 1`
entries. When `step >= size`, a running left fold replaces the stacks and keeps
no raw window buffer. Both paths preserve input order and fold incomplete tails,
even when those tails produce no output. Cached entries do not bound bytes:
an aggregate can grow with its input.
Combine calls are amortized constant per input item; transferring a stack can
take linear work in `size`. This does not provide DABA's worst-case constant
operation bound. Small windows and windows without overlap may cost more than
an ordinary fold; compare against `window(...).map(...)` for those workloads.

## Reduce adjacent runs with a Collector (unreleased)

`collapse(predicate, collector)` feeds each nonempty adjacent run into fresh
[Collector](collecting.md) state. The predicate sees neighboring input items,
including when it is not transitive:

```python
from fpstreams import Collectors, flow

query = flow([1, 2, 4, 9, 11, 1]).collapse(
    lambda left, right: 0 <= right - left <= 2,
    Collectors.counting(),
)
assert query.to_list() == [3, 2, 1]
```

Each run has its own initializer, steps, completion state, and finisher. Empty
input emits nothing and initializes no collector. A completed collector stops
receiving steps, while adjacent predicates keep running until the run boundary.
For example, `Collectors.head(1)` keeps only the first item of each run; it
cannot finish the whole query after seeing the first item.

A boundary consumes one item of the next run before emitting the current
result. The next collector is initialized when consumption resumes, so `take(1)`
does not initialize an unused next run. The last run is finished after normal
source exhaustion. Source errors and early termination do not flush a pending
run. An infinite run still needs a boundary or source exhaustion to emit.

The operation keeps collector state and adjacent input references without a
raw run buffer. `Collectors.counting()` keeps one running integer;
`Collectors.to_list()` retains every run member. A growing collector state and
the final `to_list()` result can still grow with input size. Collector hooks run
while reading input, so callback errors can appear earlier than with
`group_runs(...).map(collector)`. This is an explicit overload, not an automatic
rewrite of buffered pipelines.

The adjacent predicate and the state program add work as input arrives.
When grouping by equal keys and the groups fit in memory, compare this overload
with `group_runs(...).map(collector)`. Buffered execution can be faster.

Hooks follow the synchronous Collector protocol, including `done`. A finisher
returning an awaitable emits that object as data. Collector `combine` and
`native` metadata are unused by this sequential reduction. The binary merger
form of `collapse(predicate, merger)` remains available.

## Record operations and the Rows view

`Flow.rows()` creates a lazy relational view without inspecting any item. The
view shares the same plan and source ownership, so it does not make a one-shot
input reusable.

Record operations whose names do not conflict with Flow semantics can be called
directly. `select()`, `with_columns()`, `rename()`, `cast()`, `fill_nulls()`,
`drop_nulls()`, `explode()`, `unnest()`, `unpivot()`, and `pivot()` return Rows;
`group_by()` returns GroupedRows.

Four relation-building names retain their established Flow meanings. Enter the
Rows view first for these relational forms:

| Flow call | Flow meaning | Explicit Rows call | Rows meaning |
| --- | --- | --- | --- |
| `drop(count)` | Skip leading items | `rows().drop(*columns)` | Remove record fields |
| `join(separator)` | Join string representations | `rows().join(other, ...)` | Relational join |
| `aggregate(...)` | Execute and return a dictionary | `rows().aggregate(...)` | Build a lazy one-row relation |
| `where(predicate)` | Alias of `filter` | `rows().where(predicate, **equalities)` | Filter records, including field equalities |

This table covers relation-building operations, not every shared method name.
Flow keeps its own output signatures too. Enter `.rows()` before `to_csv()`,
`to_pandas()`, or `to_df()` when you need Rows-specific writer or materializer
options.

`Rows.map()` and `Rows.flat_map()` return an ordinary Flow because their output
may have any shape. If those functions produce records, a later nonconflicting
record method can enter Rows again.

## Tabular source routing

When the corresponding package is already loaded, `flow(source)` recognizes
concrete PyArrow `Table`, `RecordBatch`, and `RecordBatchReader` objects, pandas
DataFrames, and Polars DataFrame or LazyFrame objects. It does not import those
packages merely to probe an arbitrary object. A custom object with
`__arrow_c_stream__` or `__dataframe__` is routed through the matching adapter;
Arrow wins when both protocols are present. Pandas conversion emits data columns
only, not the dataframe index.

This routing does not sample ordinary iterable contents. Built-in containers,
generators, NumPy arrays, and plain two-dimensional lists are not classified by
their contents. Record methods still work when their items support the requested
selectors. File paths are not guessed as CSV or Parquet inputs; use an explicit
factory.

Conversion timing follows the selected protocol:

- an ordinary generator is not opened during construction;
- generic dataframe conversion and Polars LazyFrame collection are deferred
  until execution;
- a custom Arrow C stream is imported once during construction and is one-shot;
- a PyArrow `RecordBatchReader` is one-shot, while retained tables and record
  batches are reiterable.

## Explaining terminal execution

`explain()` defaults to ordinary iteration. Pass a terminal name when you want
to inspect `to_list()`, `count()`, `sum()`, statistics, aggregation, or a
short-circuiting terminal. The same planner is used by the explanation and the
terminal itself.

```python
from fpstreams import flow

explanation = flow([1, 2, 3]).explain(terminal="count").to_dict()

assert explanation["selected_engine"] == "python"
assert explanation["complexity"] == "O(1)"
assert explanation["semantics"]["output"]["cardinality"] == {
    "kind": "exact",
    "value": 3,
}
assert explanation["diagnostics"] == []
assert explanation["arrow_prefix"] is None
assert explanation["boundaries"] == []
```

An Arrow-capable plan reports its retained prefix and any guarded transition to
Python rows in `arrow_prefix` and `boundaries`. Relational plans additionally
report their selected tree and strategy in `relations`.

[`run_with_report()`](../user-guide/execution-reports.md) returns the terminal
value, its recorded route, and query-owned resource counts in one execution.
The report covers the outer plan and some direct paths; internal kernels and
fallbacks are not all recorded.

An identity list or tuple remains in Python under `auto` when a terminal would
otherwise scan and copy it. An identity range can still use native numeric
reduction. Exact-size `count()` is O(1) only for an unchanged, safely reiterable
source; operations and one-shot inputs are consumed normally.

## CSV safety

Flow `to_csv(..., spreadsheet_safe=False)` writes arbitrary scalar, sequence, or
mapping items and accepts an optional `header`. Set `spreadsheet_safe=True` when
untrusted strings will be opened in Excel, Sheets, or similar software. Suspect
formula prefixes are neutralized with a leading single quote. The method writes
the file and returns `None`.

After entering Rows, `to_csv()` is the record writer instead and exposes
`fieldnames`, `include_header`, and `extrasaction` options.

## Creating a flow

`flow(range(...))` and `aflow(range(...))` accept ranges longer than `sys.maxsize`.
Bounded reads remain lazy, and an identity `count()` returns the exact cardinality.

| Call | Behavior |
| --- | --- |
| `flow(source)` | Wrap an ordinary iterable, reuse a Flow/Rows plan, or route a supported tabular source |
| `flow.defer(factory)` | Call the factory for each execution |
| `flow.from_arrow(source)` | Adapt an Arrow table, batch, reader, or C stream provider |
| `flow.from_columns(columns)` | Build and retain an Arrow table from equal-length named columns |
| `flow.from_numpy(array, columns=...)` | Adapt a one-dimensional array to scalars or a two-dimensional array to named records |
| `flow.from_dataframe(frame)` | Use the dataframe interchange protocol; `from_pandas` is an alias |
| `flow.from_polars(frame)` | Retain a Polars DataFrame or LazyFrame |
| `flow.scan_csv(path)` | Scan typed Arrow CSV batches with query column pruning |
| `flow.from_parquet(source)` | Scan Parquet with optional projection and filtering |
| `flow.empty()` | Create a flow with no items |
| `flow.of_nullable(value)` | Emit one value, or nothing when it is `None` |
| `flow.iterate(seed, function)` | Repeatedly derive the next value from the previous one |
| `flow.generate(supplier)` | Call a supplier for every emitted value |
| `flow.concat(*sources)` | Read several sources in order |

## Methods

::: fpstreams.Flow
    options:
      members_order: source
      show_root_heading: true
      show_source: false

## Merging sorted inputs {#merging-sorted-inputs-current-source}

`merge_sorted(other, key=None)` checks ascending keys while consuming two inputs.
Keys must be exact int, str, bytes, or nonempty flat tuples with one fixed type
per position across both inputs. Equal keys from the left come first; output
objects are the original inputs. Each side has one lookahead row.

```python
from fpstreams import flow

assert flow([1, 3]).merge_sorted([1, 2]).to_list() == [1, 1, 2, 3]
```

Added in 2.2.0. It uses Python in `auto` mode;
forcing native is unsupported. Iterator aliasing is rejected before the first
pull, and both owned inputs are closed on errors or early termination. Nested
merges keep one pair of cursors per merge; they are not a heap-based k-way merge.

Sorted paired operations acquire their branch source iterators left to right,
without pulling rows, before either input runs. This includes source leaves in
composed branches, so a shared underlying iterator can be rejected before it is
read. Each acquired iterator closes once, even if a branch never yields a row.
An outer `take(0)` does not start this acquisition phase.

## File exports {#file-exports-current-source}

`to_jsonl()` writes one JSON value per line, including scalars and records. Use
`default` for custom serialization and `ensure_ascii` to control Unicode escaping.
The source is consumed once and the output is written incrementally.

`to_csv()`, `to_json()`, and `to_jsonl()` accept `atomic=True` to preserve the old
target until writing and source cleanup succeed. `if_exists="error"` requires
atomic output and prevents overwriting a concurrently created target. See
[atomic file output](io.md#atomic-flow-file-output-current-source) for examples
and filesystem limits. These additions were introduced in 2.2.0.
