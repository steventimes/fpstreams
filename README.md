# fpstreams

[![Tests](https://github.com/steventimes/fpstreams/actions/workflows/test.yml/badge.svg)](https://github.com/steventimes/fpstreams/actions/workflows/test.yml)
[![PyPI](https://img.shields.io/pypi/v/fpstreams.svg)](https://pypi.org/project/fpstreams/)
[![Python](https://img.shields.io/pypi/pyversions/fpstreams.svg)](https://pypi.org/project/fpstreams/)
[![License: MIT](https://img.shields.io/badge/license-MIT-yellow.svg)](LICENSE)

Typed, lazy data pipelines for Python, with synchronous streams, structured
asynchronous concurrency, record-oriented transforms, and optional Rust execution.

> fpstreams 2 is the stable, ground-up replacement for the v1 implementation.

## What is in v2

- `Flow[T]`: lazy, reiterable or one-shot synchronous pipelines.
- `AsyncFlow[T]`: asynchronous transforms with bounded concurrency, ordering,
  timeouts, merging, debouncing, and cancellation-safe cleanup.
- `Rows[T]`: expressions, joins, grouping, reshape operations, CSV/JSONL/SQL,
  Arrow, Parquet, pandas, and Polars interoperability.
- `Pairs[K, V]`: key/value transforms and per-key collection or aggregation.
- `Collector` and `Aggregator`: single-pass reductions, including named
  multi-aggregation.
- `Option` and `Result`: typed value and error containers.
- Automatic execution planning: fused Python loops, native Rust kernels for
  supported numeric plans, and hybrid execution when only part of a plan is native.
- Bounded-memory operations such as external sort and partitioned joins/grouping.

Python 3.11 or newer is required.

## Installation

Install the latest stable release:

~~~bash
pip install fpstreams
~~~

Install optional integrations only when needed:

~~~bash
pip install "fpstreams[async]"   # aiofiles
pip install "fpstreams[arrow]"   # PyArrow and Parquet
pip install "fpstreams[data]"    # NumPy, pandas, and PyArrow
pip install "fpstreams[polars]"  # Polars and PyArrow
~~~

## Quick start

Pipelines are lazy. Transformations build a plan; terminal operations such as
`to_list()`, `aggregate()`, `first()`, and `count()` execute it.

~~~python
from fpstreams import flow, item

squares = (
    flow(range(1, 10))
    .filter(item % 2 == 0)  # Keep even values.
    .map(item * item)  # Square each value.
    .take(3)  # Stop after three results.
    .to_list()
)

assert squares == [4, 16, 36]
~~~

The placeholder expression above is equivalent to two lambdas, but it can also be
compiled by the native engine when the complete plan is supported.

### Named single-pass aggregation

~~~python
from fpstreams import agg, flow

summary = flow([1, 2, 3, 4]).aggregate(
    count=agg.count(),
    total=agg.sum(),
    mean=agg.mean(),
)

assert summary == {"count": 4, "total": 10, "mean": 2.5}
~~~

All named aggregations share one traversal of the source.

### Record pipelines

`Rows` accepts dictionaries, dataclasses, named tuples, and objects with attributes.

~~~python
from fpstreams import agg, col, rows

orders = [
    {"region": "eu", "status": "paid", "price": 12, "quantity": 2},
    {"region": "us", "status": "paid", "price": 20, "quantity": 1},
    {"region": "eu", "status": "cancelled", "price": 99, "quantity": 1},
    {"region": "eu", "status": "paid", "price": 8, "quantity": 3},
]

revenue = (
    rows(orders)
    .where(col("status") == "paid")
    .with_columns(revenue=col("price") * col("quantity"))
    .group_by("region")
    .aggregate(
        orders=agg.count(),
        revenue=agg.sum("revenue"),
    )
    .sort_by("region")
    .to_list()
)

assert revenue == [
    {"region": "eu", "orders": 2, "revenue": 48},
    {"region": "us", "orders": 1, "revenue": 20},
]
~~~

`Rows` also reads and writes Arrow, Parquet, pandas, Polars, SQLite, and
DB-API sources:

~~~python
from fpstreams import col, rows

active = (
    rows.from_parquet("accounts.parquet", columns=["id", "status", "balance"])
    .where(col("status") == "active")
    .select("id", "balance")
)

active.to_parquet("active-accounts.parquet")
~~~

### Asynchronous pipelines

`map_async` accepts synchronous or asynchronous callables. `concurrency` bounds
in-flight tasks, while `ordered=True` preserves input order.

~~~python
import asyncio

from fpstreams import aflow


async def fetch(value: int) -> int:
    await asyncio.sleep(0.01)
    return value * 10


async def main() -> None:
    result = await (
        aflow([1, 2, 3, 4])
        .map_async(fetch, concurrency=2, ordered=True)
        .filter(lambda value: value >= 20)
        .to_list()
    )
    assert result == [20, 30, 40]


asyncio.run(main())
~~~

Async iterators and outstanding tasks are closed or cancelled when a pipeline
finishes, errors, times out, or short-circuits.

### Key/value pipelines

~~~python
from fpstreams import agg, pairs

totals = pairs([("a", 2), ("b", 5), ("a", 3)]).aggregate_values(
    total=agg.sum(),
    average=agg.mean(),
)

assert totals == {
    "a": {"total": 5, "average": 2.5},
    "b": {"total": 5, "average": 5.0},
}
~~~

## Execution engines

The default `auto` engine chooses between Python, native Rust, and hybrid execution.
Use `explain()` before execution to inspect that decision:

~~~python
from fpstreams import flow, item

pipeline = flow([1, 2, 3]).map(item + 1).filter(item > 2)
plan = pipeline.explain().to_dict()

assert plan["selected_engine"] == "python"
assert plan["stages"][0]["fused"] is True
~~~

You can request an engine explicitly when testing parity or diagnosing a plan:

~~~python
python_result = pipeline.with_engine("python").to_list()
native_result = pipeline.with_engine("native").to_list()
~~~

A forced native plan raises `NativeUnsupportedError` if its types or operations
cannot run natively. `auto` falls back safely.

For data larger than memory, use `external_sort(..., buffer_size=...)`,
`Rows.join(..., partitions=...)`, or `Rows.group_by(...).spill(...)` instead of
materializing the entire input.

## Source and resource semantics

- Reiterable inputs such as lists can execute more than once.
- Iterators and async iterators are one-shot and raise `FlowConsumedError` on a
  second execution.
- `flow.defer(factory)` opens a fresh source for every execution.
- Terminal operations close owned iterators, database cursors, temporary files,
  and asynchronous tasks, including on errors and early termination.

## v1 compatibility

`Stream` remains an alias of `Flow`, `AsyncStream` remains an alias of
`AsyncFlow`, and `ParallelStream` remains an alias of `Flow` to ease imports.
New v2 code should use `flow`, `aflow`, `rows`, and `pairs` directly.

v2 breaks parts of the v1 API. The standalone `core` and `ParallelStream`
implementations are gone. `ParallelStream` remains an alias, and `Flow.parallel()`
remains as a compatibility strategy for following maps. New code can call
`parallel_map()` directly or use `map_async()` for asynchronous work.

## Development

~~~bash
uv sync --extra arrow --extra data --extra polars \
  --group build --group test --group lint --group type --group docs

uv run pytest
uv run ruff check .
uv run mypy
cargo test --manifest-path rust/Cargo.toml
uv run mkdocs build --strict -f fpstreams/mkdocs.yml
~~~

The source tree is organized by domain under `src/fpstreams/`: `streams`,
`planning`, `execution`, `collecting`, `tabular`, `expressions`, and `primitives`.
Small top-level modules are compatibility facades, not duplicate implementations.

## License

MIT. See [LICENSE](LICENSE).
