# Data input and output

fpstreams separates source selection from query construction. File formats are
never guessed from a string path, and a two-dimensional Python object is not
silently reclassified by sampling its contents. Use an explicit adapter when the
source contract matters.

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

Two CSV adapters intentionally expose different contracts.

### `rows.from_csv`

Use the standard-library-compatible reader when string cells and `csv` dialect
options are desired.

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

The ownership distinction makes upload streams and remote-storage clients usable without a
temporary file:

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

The C stream protocol provides interoperability, not a promise of zero copies in
every downstream operation. Schema conversion, unsupported data types, or a
Python row boundary can require allocation.

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
    fetch_size=1_000,
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

## Spreadsheet safety

CSV intended for Excel, Sheets, or similar applications can treat leading
characters such as `=`, `+`, `-`, and `@` as formulas. Set
`spreadsheet_safe=True` for untrusted text. fpstreams prefixes suspect cells with
a single quote. Leave it disabled for machine interchange where byte-level value
preservation is required.

## Files, errors, and partial effects

Read adapters own files and connections they open and close them on completion,
early termination, and failure. Writer methods document whether they stream
directly or use a replace/transaction boundary. A raised error means callers
must not assume an external file or database accepted no rows unless the adapter
explicitly provides atomic behavior.

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
