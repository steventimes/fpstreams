# Rows

`Rows[T]` adds record selectors, expressions, joins, grouping, reshape
operations, and data-system adapters to a lazy flow.

Create a pipeline with `rows(source)` or one of the adapter factories.

## Creating rows

| Call | Source |
| --- | --- |
| `rows(source)` | Records, a `Flow`, pandas, or Polars data |
| `rows.from_csv(path)` | CSV rows |
| `rows.from_jsonl(path)` | JSON objects stored one per line |
| `rows.from_arrow(source)` | Arrow tables, batches, readers, datasets, or streams |
| `rows.from_dataframe(frame)` | The dataframe interchange protocol |
| `rows.from_polars(frame)` | A Polars `DataFrame` or `LazyFrame` |
| `rows.from_parquet(source)` | Parquet data with optional projection and filtering |
| `rows.from_db(connect, query)` | A DB-API query using a connection factory |
| `rows.from_sqlite(database, query)` | A SQLite query |

Adapters open files and connections only when the pipeline executes. Their method
docstrings list supported options and return types.

## Bounded JSONL and spreadsheet-safe CSV

`rows.from_jsonl()` limits each physical line to 8 MiB by default. An oversized
record raises `BufferLimitError` before text decoding or JSON parsing. Adjust the
limit with `max_record_bytes`; use `None` only when unlimited records from a
trusted local file are intentional.

CSV writers preserve raw cells by default. Use
`to_csv(..., spreadsheet_safe=True)` for untrusted text that will be opened in a
spreadsheet. A string whose first non-whitespace character is `=`, `+`, `-`, or
`@` receives a leading single quote. Non-string values are unchanged. CSV and
JSON/JSONL writers return `None`.

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
still exceeds the configured depth—or match, group-state, or output expansion
exceeds another limit—the operation raises `BufferLimitError` before loading an
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
