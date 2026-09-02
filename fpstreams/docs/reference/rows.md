# Rows

`Rows[T]` is the explicit relational and compatibility view over a lazy Flow. It
adds record selectors, expressions, joins, grouping, reshape operations, and
record-oriented data-system adapters.

Enter it with `flow(source).rows()`, construct it with `rows(source)`, or use one
of the adapter factories. A Rows view shares its Flow's plan and source
ownership; it does not inspect the source or make a one-shot input reusable.

## Entering and leaving Rows

Nonconflicting record methods can be called directly on Flow. `select()`,
`with_columns()`, `rename()`, `cast()`, `fill_nulls()`, `drop_nulls()`,
`explode()`, `unnest()`, `unpivot()`, and `pivot()` return Rows, while
`group_by()` returns GroupedRows.

Four relation-building names already have Flow meanings. Use `.rows()` before
them when you intend these relational forms:

| Flow call | Flow meaning | Explicit Rows call | Rows meaning |
| --- | --- | --- | --- |
| `drop(count)` | Skip leading items | `rows().drop(*columns)` | Remove record fields |
| `join(separator)` | Join string representations | `rows().join(other, ...)` | Relational join |
| `aggregate(...)` | Execute and return a dictionary | `rows().aggregate(...)` | Build a lazy one-row relation |
| `where(predicate)` | Alias of `filter` | `rows().where(predicate, **equalities)` | Filter records, including field equalities |

Flow also has same-named output methods. Enter `.rows()` before `to_csv()`,
`to_pandas()`, or `to_df()` when you need the record writer or Rows-specific
materialization options.

`Rows.map()` and `Rows.flat_map()` return Flow because their result may no
longer be record-shaped. If they do emit records, calling a nonconflicting record
method enters Rows again.

`Rows.to_flow()` returns the exact underlying Flow without adding an operation or
copying data. `Rows.explain()` delegates to that Flow and does not consume the
source, so relational plans can be inspected without using private attributes.

`Rows.concat(*sources)` drains each source in order and stays lazy. It preserves
records exactly as received; unlike a dataframe union, it does not align fields,
fill missing columns, or infer a common dtype.

## Creating rows

| Call | Source |
| --- | --- |
| `rows(source)` | Records, a `Flow`, supported tabular objects, or standard tabular-protocol providers |
| `rows.from_csv(path)` | Compatible CSV rows from a path, text handle, or opener |
| `rows.scan_csv(path)` | Typed, streaming Arrow CSV batches with query column pruning |
| `rows.from_jsonl(path)` | JSON objects from a path, text/binary handle, or opener |
| `rows.from_arrow(source)` | An Arrow `Table`, `RecordBatch`, `RecordBatchReader`, or `__arrow_c_stream__` provider |
| `rows.from_columns(columns)` | Equal-length named columns retained as an Arrow table |
| `rows.from_numpy(array, columns=...)` | An explicit two-dimensional array converted lazily to named records |
| `rows.from_dataframe(frame)` | The dataframe interchange protocol |
| `rows.from_polars(frame)` | A Polars `DataFrame` or `LazyFrame` |
| `rows.from_parquet(source)` | Parquet data with optional projection and filtering |
| `rows.from_db(connect, query)` | A DB-API query using a connection factory |
| `rows.from_sqlite(database, query)` | A SQLite query |

Path, opener, and database adapters open their resources only when the pipeline executes.
An already-open CSV or JSONL handle is caller-owned and one-shot; fpstreams neither rewinds nor
closes it. A zero-argument opener is replayable, and fpstreams closes each handle it returns.
Dataframe interchange conversion and Polars LazyFrame collection are also
deferred. An Arrow C stream provider is the exception: its stream is imported
once at construction and treated as one-shot. Adapter docstrings list the exact
options and return types.

Use `from_csv()` when Python `csv.DictReader` compatibility and string-valued
cells matter. Use `scan_csv()` for typed inference and wide analytical scans;
direct `select()` queries are pushed into Arrow CSV conversion. Arrow's
incremental reader is single-threaded and freezes inferred types after its first
byte block, so pass `ReadOptions` or `ConvertOptions.column_types` when the
default inference is not appropriate.

The primary `flow(source)` entry automatically recognizes PyArrow, pandas, and
Polars objects and standard `__arrow_c_stream__` or `__dataframe__` providers.
`rows(source)` uses the same dispatch path, including Arrow priority for an
object that implements both protocols. The named factories remain useful when
you want to make the adapter or its options explicit.
`flow.scan_csv()` and `flow.from_parquet()` return Flow, while the corresponding
Rows factories return the explicit relational view.

## Bounded JSONL and spreadsheet-safe CSV

`rows.from_jsonl()` limits each physical line to 8 MiB by default. An oversized
record raises `BufferLimitError` before JSON parsing; binary inputs are checked
before decoding as well. Adjust the limit with `max_record_bytes`; use `None`
only when unlimited records from a trusted local file are intentional.

CSV writers preserve raw cells by default. Use
`to_csv(..., spreadsheet_safe=True)` for untrusted text that will be opened in a
spreadsheet. A string whose first non-whitespace character is `=`, `+`, `-`, or
`@` receives a leading single quote. Non-string values are unchanged. CSV and
JSON/JSONL writers return `None`.

Rows `to_csv()` is the record writer and exposes `fieldnames`, `include_header`,
and `extrasaction`. Flow `to_csv()` instead writes arbitrary items and accepts an
optional `header`.

## Arrow and dataframe output

Rows accepts Arrow inputs and can also produce Arrow and dataframe outputs:

- `arrow_batches()` emits bounded PyArrow RecordBatches lazily;
- `to_arrow()` materializes a PyArrow Table;
- `__arrow_c_stream__()` exports the standard Arrow PyCapsule stream protocol;
- `to_pandas()` and `to_polars()` materialize their respective dataframe types;
- `to_parquet()` writes bounded row groups and publishes the local file
  atomically.

Direct retained Arrow sources can reuse native batches. Joins and aggregates are
evaluated first because their output has no equivalent linear source view.

## Safer joins

A duplicate lookup key can multiply records without warning. Declare the
relationship you expect when duplicates would indicate bad data:

```python
customers = [
    {"customer_id": 1, "name": "Ada"},
    {"customer_id": 2, "name": "Lin"},
]

enriched = (
    rows(orders)
    .join(
        customers,
        on="customer_id",
        how="left",
        validate="m:1",
    )
    .to_list()
)
```

This many-to-one join permits several orders per customer and requires unique
customer keys. Use `1:m` for a unique left side, `1:1` when both sides must
be unique, or `m:m` to permit duplicates on both sides. Partitioned joins use
the same checks, and errors name the duplicate side and key.

Partitioning is governed by `SpillLimits`. Its defaults are:

| Limit | Default |
| --- | ---: |
| Rows in one loaded partition | 100,000 |
| Serialized bytes in one loaded partition | 64 MiB |
| Matches for one join key | 100,000 |
| Total output rows | 1,000,000 |
| Recursive repartition levels | 3 |

Oversized partitions are repartitioned with a new deterministic salt. If skew
still exceeds the configured depth, or match, group-state, or output expansion
exceeds another limit, the operation raises `BufferLimitError` before loading an
unbounded bucket. Temporary files are removed. Pass a custom `SpillLimits` to
`join(..., partitions=..., limits=...)` or `group_by(...).spill(limits=...)`.

This is bounded processing up to configured limits, not guaranteed completion
for every skewed or many-to-many input.

## Methods

::: fpstreams.Rows
    options:
      members_order: source
      show_root_heading: true
      show_source: false

## GroupedRows

`group_by()` returns a grouped plan. Call `aggregate()` directly, or call
`spill()` first to use partitioned temporary storage.

::: fpstreams.tabular.GroupedRows
    options:
      members_order: source
      show_root_heading: true
      show_source: false
