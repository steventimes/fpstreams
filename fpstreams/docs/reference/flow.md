# Flow

`Flow[T]` is the primary synchronous lazy pipeline for both ordinary values and
records. Transformations return a new pipeline; terminal methods execute its
plan.

Use `flow(source)` for an existing source and `flow.defer(factory)` when every
execution must create a fresh iterable. Besides ordinary iterables,
`flow(source)` retains supported PyArrow, pandas, Polars, Arrow C stream, and
dataframe-interchange inputs.

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
