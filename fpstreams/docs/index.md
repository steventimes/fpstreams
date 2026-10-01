# fpstreams v2

fpstreams lets you filter, transform, and aggregate data in lazy Python
pipelines. Start with `flow()` for synchronous values and records, including
supported Arrow tables and dataframes. Use `aflow()` for async work:

| Entry point | Use it for |
| --- | --- |
| `flow(source)` | Synchronous value or record pipelines, including supported tabular sources |
| `aflow(source)` | Asynchronous I/O, merging, time-based operators, bounded concurrency |
| `rows(source)` | Record operations, relational joins, and record-specific I/O |
| `pairs(source)` | Key/value transformations and per-key aggregation |

The examples use the `2.1` API. Sections marked unreleased describe changes
available in the source tree that are not yet included in the PyPI release.

The [2.1 changelog](https://github.com/steventimes/fpstreams/blob/master/CHANGELOG.md)
summarizes the new public APIs and execution changes.

## Installation

~~~bash
pip install fpstreams
~~~

Python 3.11 or newer is required. Release testing covers standard CPython 3.11
through 3.14. Free-threaded 3.14t support is experimental and has no release
wheels. Install optional integrations as needed:

~~~bash
pip install "fpstreams[async]"   # aiofiles for async file adapters
pip install "fpstreams[arrow]"
pip install "fpstreams[data]"
pip install "fpstreams[polars]"
~~~

## Work with records

Filter paid orders, then count orders and total revenue for each region.
Use string selectors to read record fields and `col()` to build expressions.
Methods such as `select()` and `group_by()` enter a Rows view from `flow()`:

~~~python
from fpstreams import agg, col, flow

orders = [
    {"region": "eu", "status": "paid", "amount": 24},
    {"region": "us", "status": "paid", "amount": 20},
    {"region": "eu", "status": "cancelled", "amount": 99},
    {"region": "eu", "status": "paid", "amount": 24},
]

result = (
    flow(orders)
    .filter(col("status") == "paid")
    .group_by("region")
    .aggregate(
        orders=agg.count(),
        revenue=agg.sum("amount"),
    )
    .sort_by("region")
    .to_list()
)

print(result)
# [{'region': 'eu', 'orders': 2, 'revenue': 48},
#  {'region': 'us', 'orders': 1, 'revenue': 20}]
~~~

Use `flow(records).rows()` when you need a method whose Flow meaning is already
established: Flow `drop(count)` skips items, `join(separator)` builds a string,
`aggregate(...)` executes to a dictionary, and `where(predicate)` aliases
`filter`. Their Rows counterparts remove columns, perform a relational join,
build a lazy one-row relation, and accept record equalities.
Flow also keeps its own output signatures; enter `.rows()` before `to_csv()`,
`to_pandas()`, or `to_df()` when you need Rows-specific options.

`flow(source)` automatically retains concrete PyArrow, pandas, and Polars inputs
and recognizes standard `__arrow_c_stream__` and `__dataframe__` providers.
Explicit Flow factories cover Arrow, dataframe, Polars, typed CSV, and Parquet.
The `rows` namespace also supplies compatibility CSV, JSONL, SQLite, DB-API, and
record-oriented output methods. Optional third-party packages are imported only
when the corresponding adapter is used.

## When to use fpstreams

A comprehension is usually enough for a short transformation. Reach for a
pipeline when you need grouped aggregation, early termination, resource cleanup,
or async tasks with a concurrency limit. Small pipelines can be slower because
planning and dispatch add overhead.

Lazy execution does not make every operation constant-memory. Sorting, grouping,
and joins retain state; choose explicit limits or spill to disk for larger inputs.

## Your first flow

~~~python
from fpstreams import flow, item

result = (
    flow(range(1, 10))
    .filter(item % 2 == 0)  # Keep even values.
    .map(item * item)  # Square each remaining value.
    .take(3)  # Stop after three results.
    .to_list()
)

print(result)
# [4, 16, 36]
~~~

A pipeline has three parts:

1. A **source**, such as a list, generator, deferred factory, database cursor, or
   async iterator.
2. Zero or more lazy **transformations**, such as `map`, `filter`, `window`, or
   `group_by`.
3. A **terminal operation**, such as `to_list`, `count`, `first`, `collect`, or
   `aggregate`, which executes the plan.

Lists and other reiterable sources can execute repeatedly. Iterators are one-shot.
Use `flow.defer(factory)` when every execution must open a fresh source.

## Aggregate once

Named aggregations share a single traversal:

~~~python
from fpstreams import agg, flow

summary = flow([1, 2, 3, 4]).aggregate(
    count=agg.count(),
    total=agg.sum(),
    mean=agg.mean(),
)

print(summary)
# {'count': 4, 'total': 10, 'mean': 2.5}
~~~

Use a `Collector` when the result is a general container or reduction. Use an
`Aggregator` for composable statistics, especially named and grouped aggregation.

## Bound asynchronous concurrency

This example uses only the core package; the `async` extra is for file adapters.

~~~python
import asyncio

from fpstreams import aflow


async def request(value: int) -> int:
    await asyncio.sleep(0.01)
    return value * 10


async def main() -> None:
    values = await (
        aflow([1, 2, 3, 4])
        .map_async(request, concurrency=2, ordered=True)  # At most two requests run.
        .timeout(1.0)  # Fail instead of waiting forever.
        .to_list()
    )
    print(values)
    # [10, 20, 30, 40]


asyncio.run(main())
~~~

Concurrency is bounded. On completion, short-circuit, timeout, or failure,
fpstreams cancels outstanding tasks and closes the owned async iterator.

## Understand execution

The default engine, `auto`, selects Python, Rust, Arrow, NumPy, or a combination
based on the source, operations, and terminal. It uses an optimized path only
when that path can preserve the pipeline's Python behavior.

~~~python
from fpstreams import flow, item

pipeline = flow([1, 2, 3])
explanation = pipeline.explain(terminal="count").to_dict()

assert explanation["selected_engine"] == "python"
assert explanation["complexity"] == "O(1)"
assert explanation["data_movement"]["copies_source"] is False
~~~

Call `with_engine("python")` or `with_engine("native")` to test a specific
engine. A forced native plan fails clearly when it is unsupported; `auto` can
fall back or split the pipeline into stages.

Identity list and tuple materialization stays in Python. Large, supported integer
sums may use native execution in `auto` mode; the route depends on the terminal
and input. An unchanged reiterable source with a known exact size can answer
`count()` without opening the source.

## Keep memory bounded

`map`, `filter`, and `take` stream values. Sorting, joins, and grouping need
more state; use these options to limit their in-memory working set:

- `external_sort(buffer_size=..., tempdir=...)` writes sorted runs to temporary files.
- `Rows.join(..., partitions=..., tempdir=..., limits=...)` partitions large joins.
- `Rows.group_by(...).spill(partitions=..., tempdir=..., limits=...)` partitions grouping.
- Buffer-sensitive operations raise `BufferLimitError` instead of growing without limit.

`SpillLimits` has finite defaults for partition rows and bytes, per-key matches,
total output, and repartition depth. Highly skewed or expanding inputs can fail
at those limits. Temporary resources are cleaned up on normal completion, errors,
limit failures, and early exit.

CSV is written raw by default. Enable `spreadsheet_safe=True` for untrusted text
that will be opened in spreadsheet software. JSONL input has an 8 MiB per-record
default; `max_record_bytes=None` disables it for trusted input.

## Moving from v1

`Stream`, `AsyncStream`, and `ParallelStream` remain import aliases to ease
migration. New synchronous code should start with `flow`; use `.rows()` or the
`rows` namespace for an explicit relational view or a record-specific adapter.
Use `aflow` and `pairs` for their respective domains.

The old `core` and standalone `ParallelStream` implementations were removed.
The import alias and `Flow.parallel()` compatibility strategy remain. New code can
use `map_parallel()` for thread or process mapping and `map_async()` for async work.
Because v2 changes terminal and error semantics, test existing pipelines before
upgrading production code.
