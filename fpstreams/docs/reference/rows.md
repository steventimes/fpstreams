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
