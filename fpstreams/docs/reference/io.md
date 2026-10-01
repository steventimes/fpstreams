# Data input and output

Use an explicit factory for files, named columns, and NumPy arrays.
`flow(source)` recognizes supported tabular objects and protocols, but treats
ordinary iterables as supplied. It does not infer a file format from a path or
sample nested lists to decide whether they are tables.

Owned Arrow readers and iterators report `close()` failures. If a query has
already failed, its original exception is preserved and cleanup failures are
attached as exception notes, including diagnostics from nested cleanup. A successful
read or write can therefore raise while closing its resources, including after an
early `first()` result.

Parquet writes publish a completed temporary file in the destination directory.
With `if_exists="error"`, publication uses a hard link that fails if the destination
exists, including when another process creates it during the write. The filesystem
must support hard links. `if_exists="replace"` uses an atomic replacement instead.

## Input matrix

| Entry point | Input | Output | Evaluation | Replayability | Extra |
| --- | --- | --- | --- | --- | --- |
| `flow(source)` | Iterable, Flow/Rows, recognized tabular object or protocol provider | `Flow` | Lazy for ordinary iterables | Follows source | None for core |
| `rows(source)` | Records, Flow, recognized tabular input | `Rows` | Lazy view | Follows source | None for core |
| `rows.from_csv(path)` | CSV path, text handle, or opener | string-valued dictionaries | Opens or reads on execution | Path/opener replayable; handle one-shot | None |
| `rows.scan_csv(path)` | CSV file | typed dictionaries from Arrow batches | File opens on execution | Reopens by path | `arrow` |
| `rows.from_jsonl(path)` | JSONL path, text/binary handle, or opener | dictionaries | Opens or reads on execution | Path/opener replayable; handle one-shot | None |
| `flow.from_columns` / `rows.from_columns` | Explicit mapping of equal-length columns | named record pipeline | Arrow table built at construction | Replays the retained table | `arrow` |
| `flow.from_numpy` | Explicit one- or two-dimensional array | Python scalars for 1D; named dictionaries for 2D | `numpy.asarray` at construction; values converted on execution | Replays the retained array | `data` |
| `rows.from_numpy` | Explicit two-dimensional array | dictionaries with named columns | `numpy.asarray` at construction; rows converted on execution | Replays the retained array | `data` |
| `flow.from_arrow` / `rows.from_arrow` | Arrow table, batch, reader or C stream provider | record pipeline | Adapter-dependent | Table/batch reusable; reader/stream one-shot | `arrow` |
| `flow.from_dataframe` / `rows.from_dataframe` | `__dataframe__` provider | record pipeline | Conversion deferred | Provider-dependent | `data` |
| `flow.from_polars` / `rows.from_polars` | Polars DataFrame/LazyFrame | record pipeline | LazyFrame collection deferred | Provider-dependent | `polars` |
| `flow.from_parquet` / `rows.from_parquet` | Parquet path/dataset | typed records | Scan opens on execution | Reopens by source | `arrow` |
| `rows.from_db(connect, query, ...)` | DB-API connection factory | dictionaries | Connects on execution | Reconnects through factory | Driver supplied by app |
| `rows.from_sqlite(database, query, ...)` | SQLite path or URI | dictionaries | Connects on execution | Reconnects by path | Standard library |

`flow(source)` recognizes concrete PyArrow, pandas, and Polars types only when
their package is already loaded. It does not import all optional packages to
probe every arbitrary object. Standard `__arrow_c_stream__` and `__dataframe__`
providers are recognized directly; Arrow wins when an object exposes both.

## CSV: compatibility and typed scan

`from_csv()` reads string cells with Python's CSV parser. `scan_csv()` uses
Arrow's typed batch reader.

### `rows.from_csv`

Use this reader for string cells and Python `csv` dialect options.

```python
from fpstreams import rows

paid = rows.from_csv("orders.csv", encoding="utf-8").where(status="paid").to_list()
```

- the header becomes dictionary keys;
- cells remain strings unless a later `cast` or `parse` converts them;
- a path opens only when the pipeline executes and reopens on later executions;
- an already-open text handle is caller-owned, starts at its current position, is not closed by
  fpstreams, and makes the pipeline one-shot;
- a zero-argument opener is called for each execution, and fpstreams closes every handle it
  returns;
- duplicate header names raise `DuplicateKeyError` before the first row is emitted.

Pass an open handle to read it once, or an opener to read it again on each execution:

```python
from io import StringIO

uploaded = StringIO("id,name\n1,Ada\n")
records = rows.from_csv(uploaded).to_list()
assert not uploaded.closed

# An opener makes the source replayable. Each returned handle is library-owned.
records = rows.from_csv(lambda: bucket.open_text("orders.csv"))
```

### `rows.scan_csv`

Use the Arrow-backed scanner for typed, batched work and query projection.

```python
from fpstreams import col, rows

paid = rows.scan_csv("orders.csv").where(col("status") == "paid").select("region", "amount")
```

Arrow owns CSV type inference and parsing rules. Projection can avoid materializing
unused columns. Install with `pip install "fpstreams[arrow]"`.

## JSON Lines

`rows.from_jsonl` reads one JSON value per physical line and requires each value
to be an object. The default maximum record size protects against a single
unbounded line; tune it explicitly for trusted larger records.

```python
events = rows.from_jsonl(
    "events.jsonl",
    max_record_bytes=4 * 1024 * 1024,
)
```

Duplicate object keys are rejected rather than silently choosing one value.
`max_record_bytes=None` disables the byte limit for trusted input.

Paths, handles, and openers follow the same ownership contract as `from_csv`. JSONL handles may
yield `bytes` or `str`. `encoding` decodes paths and binary handles; it also defines encoded-byte
accounting for a text handle when `max_record_bytes` is active.

## Column mappings

Use `from_columns` when data already consists of independent named columns. The
mapping itself is not reinterpreted by `flow(columns)` or `rows(columns)`; the
explicit factory constructs and retains a PyArrow table immediately.

```python
from fpstreams import flow

records = flow.from_columns(
    {"id": [1, 2], "status": ["open", "closed"]},
    batch_size=1_024,
)
```

Column names must be unique, non-empty strings and all columns must have the
same length. The retained table is replayable. Install with
`pip install "fpstreams[arrow]"`.

## NumPy arrays

NumPy input is explicit. Ordinary `flow(array)` keeps an ndarray's normal
iterable behavior, and plain two-dimensional lists are never inspected to guess
that they are tables. `flow.from_numpy` accepts a one-dimensional array as a
scalar source or a two-dimensional array as named records. `rows.from_numpy`
accepts only the record form:

```python
import numpy as np

from fpstreams import flow, rows

values = flow.from_numpy(np.asarray([1, 2, 3]))
assert values.to_list() == [1, 2, 3]

measurements = rows.from_numpy(
    np.asarray([[1, 20.5], [2, 21.0]]),
    columns=["sensor_id", "temperature"],
)

assert measurements.select("sensor_id").to_list() == [
    {"sensor_id": 1},
    {"sensor_id": 2},
]
```

`from_numpy` calls `numpy.asarray` once when the adapter is constructed. The
exact ndarray returned by `asarray` is retained and the conversion is not
repeated on later executions. Whether that array owns or shares its storage
follows NumPy's rules: mutations to the original input are visible only when
the retained result shares that storage. The source is replayable; Python
scalars or dictionary rows are produced only as each execution pulls them.
`columns` is invalid for a one-dimensional array. For a two-dimensional array,
omit it to use the string names `"0"`, `"1"`, and so on. Explicit names must be
unique, non-empty strings matching the array width.

`Rows.to_numpy(*selectors, dtype=None, copy=None)` always returns a
two-dimensional ndarray. Selectors use the normal Rows field, path, index,
expression, callable, and `SelectionError` rules. Without selectors, record
fields follow first-seen order and missing fields become `None`. Empty output
has shape `(0, number_of_selected_columns)`; an empty source with no known
columns has shape `(0, 0)`.

The `copy` parameter follows the installed NumPy version: `None` copies only when
needed, and `True` requests a distinct result. `False` is a strict no-copy request
on NumPy 2.x, which raises when the shape or dtype requires allocation; NumPy
1.x treats it as a best-effort preference. fpstreams does not promise zero-copy
conversion in general. Selectors, record alignment, dtype conversion, and
non-NumPy sources can all require allocation.

## Arrow and the C stream protocol

Accepted Arrow sources include tables, record batches, record-batch readers,
and providers of `__arrow_c_stream__`. Use `from_parquet` for an Arrow Dataset.

- Tables and record batches are retained and can normally be evaluated again.
- A `RecordBatchReader` is one-shot.
- A custom C stream is imported once at construction and is one-shot.
- Column-compatible filters, projections, casts, and aggregations may remain in
  Arrow until a row-only operation requires Python-visible records;
  `Rows.explain()` reports the retained prefix and its boundary.
- Crossing into arbitrary Python callbacks materializes Python-visible values as
  required by the callback contract.

The C stream protocol lets libraries exchange Arrow data. Schema conversion,
unsupported data types, and conversion to Python rows can still allocate or copy
data.

## Dataframe interchange and pandas

`from_dataframe` accepts the standard `__dataframe__` protocol. Conversion is
deferred until execution where the provider permits it. Pandas indices are not
emitted as data columns; reset or copy the index into a column first when it is
part of the dataset.

```python
pipeline = flow.from_dataframe(frame).select("customer_id", "amount")
```

`from_pandas` is an alias of `from_dataframe`. Install the data integration with
`pip install "fpstreams[data]"`.

## Polars

`from_polars` accepts a Polars `DataFrame` or `LazyFrame`. A LazyFrame is retained
until execution rather than collected during construction. Conversion uses Arrow
interoperability and therefore requires the `polars` extra.

Use `to_polars_batches` or `polars_batches` when downstream code can consume
batches and a complete DataFrame is unnecessary.

## Parquet

Parquet adapters accept explicit projection and filtering supported by the Arrow
dataset layer. Select only required columns before a Python callback so the
scanner can avoid unnecessary I/O and decoding.

```python
pipeline = rows.from_parquet(
    "warehouse/orders/",
    columns=["region", "status", "amount"],
)
```

Directory and dataset replayability follows the underlying path or dataset
object. Files are opened during execution, not when the plan is created.

## Databases and SQLite

`rows.from_db` accepts a connection factory rather than an already-open global
connection. Each execution calls the factory, executes the query, derives field
names from the cursor description, and closes resources it owns.

```python
import sqlite3
from fpstreams import rows

orders = rows.from_db(
    lambda: sqlite3.connect("shop.db"),
    "select id, amount from orders where status = ?",
    parameters=("paid",),
    batch_size=1_000,
)
```

`rows.from_sqlite` is the convenience adapter for a database path or URI. Query
parameters must use the database driver's binding mechanism; never interpolate
untrusted values into SQL text.

## Output matrix

| Method | Result or effect | Materialization | Extra |
| --- | --- | --- | --- |
| `Flow.to_list`, `Flow.to_tuple`, `Flow.to_set` | Python container | Full result | None |
| `Rows.to_list` | List of records | Full result | None |
| `Rows.to_columns` | dictionary of column lists | Full result | None |
| `Rows.to_numpy` | two-dimensional NumPy array, optionally selected | Full result | `data` |
| `Flow.to_json` | JSON array file | Streams values to the destination | None |
| `Flow.to_jsonl` (unreleased) | One JSON value per line | Streams values to the destination | None |
| `Flow.to_csv` | scalar/sequence/mapping CSV file | Streams rows to file | None |
| `Rows.to_csv` | record CSV file | Streams rows; schema/header policy is explicit | None |
| `Rows.to_jsonl` | JSON object lines | Streams rows | None |
| `to_arrow` | Arrow table | Full result | `arrow` |
| `to_arrow_batches` | Arrow record batches | Batch materialization | `arrow` |
| `to_pandas` | pandas DataFrame | Full result | `data` |
| `to_polars` | Polars DataFrame | Full result | `polars` |
| `to_parquet` | Local Parquet file | Streams bounded row groups, then atomically publishes the file | `arrow` |
| `to_db` / `to_sqlite` | Inserted database rows | Batched side effect | Driver / standard library |

Rows-specific output signatures are available only after entering the Rows view.
For example, `flow(records).rows().to_csv(...)` exposes `fieldnames`, header, and
extra-field policies; `Flow.to_csv(...)` accepts arbitrary value shapes.

Record conversion failures propagate, including `StopIteration` from a record’s
`_asdict()` method. They do not mean that the source is empty. SQLite validates
the first record before replacing an existing table, so a conversion failure
leaves that table intact. CSV and JSONL write directly by default; use the
unreleased `atomic=True` option below when a failed export must preserve the old file.

## Spreadsheet safety

CSV intended for Excel, Sheets, or similar applications can treat leading
characters such as `=`, `+`, `-`, and `@` as formulas. Set
`spreadsheet_safe=True` for untrusted text. fpstreams prefixes suspect strings with
a single quote, including strings with leading whitespace. The current source
also protects header cells, whether supplied explicitly or inferred from record
keys. Header protection is not in the published 2.1.0 wheel. It changes the written
labels, while record lookup still uses the original keys. Numeric values are
unchanged. Leave this option disabled when a consumer needs the original strings.

## Files, errors, and partial effects

Read adapters close the files and connections they open on completion, early
termination, and failure. Writers may stream directly, replace a completed file,
or use a database transaction; check the method's contract. A failed streaming
write may leave partial output.

Common adapter failures include:

- `ImportError` with the required optional extra when an integration is absent;
- `SelectionError` for a missing field or invalid selector;
- `DuplicateKeyError` for duplicate JSON keys or a duplicate key under an
  error-on-duplicate dictionary policy;
- `BufferLimitError` when a configured record, batch, spill, fan-out, or output
  budget is exceeded;
- the underlying parser, filesystem, Arrow, dataframe, or database exception
  when that boundary owns the failure.

## Browser playground

The [browser playground](../playground.md) installs a pure-Python wheel into a
Pyodide worker. It is meant for in-memory core examples. Browser security does
not expose arbitrary local paths, normal process pools, or the CPython/Rust
extension. Use the installed package for production I/O and native execution.

## JSON Lines output (current source)

Use `Flow.to_jsonl()` for arbitrary JSON values and `Rows.to_jsonl()` for records.
Both write one value per line, consume the source once, and write an empty file
for an empty input. Neither collects the whole result in memory. Flow's method
is new; Rows now also accepts `default` to serialize dates and other custom values:

```python
from datetime import date
from fpstreams import rows

rows([{"id": 1, "day": date(2026, 9, 30)}]).to_jsonl(
    "events.jsonl",
    default=date.isoformat,
    atomic=True,
    if_exists="error",
)
# events.jsonl contains: {"id": 1, "day": "2026-09-30"}
```

`ensure_ascii=False` preserves Unicode text; `encoding` defaults to UTF-8.
`rows.from_jsonl()` reads object records, so it cannot read scalar lines written
by Flow. These additions are absent from the published 2.1.0 wheel.

## Atomic file output (current source) {#atomic-flow-file-output-current-source}

`Flow.to_csv()`, `Flow.to_json()`, `Flow.to_jsonl()`, `Rows.to_csv()`, and
`Rows.to_jsonl()` accept `atomic=True`. They write a temporary
file in the destination directory and publish it only after the writer and source
close successfully. A serialization or cleanup error preserves the old target.
Relative destinations are anchored to the working directory before the source
opens, so a callback changing directories cannot redirect publication.
Their default remains `atomic=False`, with the existing direct-write behavior.

For example, export a report without replacing the last successful result if
conversion fails halfway through:

```python
from fpstreams import rows

rows([{"region": "eu", "revenue": 48}]).to_csv(
    "report.csv", atomic=True, spreadsheet_safe=True,
)
```

With atomic output, `if_exists="replace"` replaces the destination directory entry.
`if_exists="error"` uses an atomic no-overwrite operation and raises
`FileExistsError` if another writer creates the target first. Filesystems that do
not support this operation fail explicitly. Existing symlinks, including dangling
ones, are rejected. `if_exists="error"` requires `atomic=True`.

This is an atomic visibility guarantee for a file path, not a promise of durability
after power loss or a transaction across a network filesystem. File handles are
not accepted. The temporary file's permissions become the output permissions;
replacement does not preserve the old file's mode or ownership. Destination
directories must be trusted: anchoring a path does not protect against another
process renaming its parent directories. These options are in the checkout and
are absent from published 2.1.0.
