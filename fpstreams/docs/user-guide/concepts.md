# Core concepts

A pipeline stores a source and its transformations. Building the pipeline does
not pull items from an ordinary iterable; iteration or a terminal such as
`to_list()` starts execution.

```python
from fpstreams import flow, item

pipeline = flow(range(10)).map(item * 2).filter(item > 8)

# No source item has been pulled yet.
values = pipeline.to_list()
assert values == [10, 12, 14, 16, 18]
```

## Pipelines and terminals

Transformations return a new pipeline and leave the previous one unchanged.
Common transformations include `map`, `filter`, `flat_map`, `take`, `unique`,
`sort_by`, `select`, and `group_by`. Terminals execute a plan and return a Python
value, write output, or perform a side effect.

| Kind | Examples | Result |
| --- | --- | --- |
| Lazy transformation | `map`, `filter`, `take`, `select` | A `Flow` or `Rows` plan |
| Lazy relational operation | `group_by`, `Rows.join`, `pivot` | A relational plan |
| Materializing terminal | `to_list`, `to_dict`, `partition` | A container in memory |
| Scalar terminal | `count`, `sum`, `first`, `variance` | One scalar or optional value |
| Effect terminal | `for_each`, `to_csv`, `to_jsonl` | Performs the requested effect |

Iteration is also execution. `for value in pipeline` opens and owns one source
iterator in the same way as a terminal.

## Source replayability

A plan can run again if its source can be reopened.

```python
from fpstreams import flow

reusable = flow([1, 2, 3])
assert reusable.sum() == 6
assert reusable.to_tuple() == (1, 2, 3)

one_shot = flow(iter([1, 2, 3]))
assert one_shot.sum() == 6
# A second execution raises FlowConsumedError.
```

Use `flow.defer(factory)` when each execution should open a fresh resource or
iterator:

```python
pipeline = flow.defer(lambda: iter([1, 2, 3]))
assert pipeline.sum() == pipeline.sum() == 6
```

A generator is consumed once. To reuse its results, materialize them explicitly;
this retains the values in memory and runs upstream callbacks at that point.

A custom iterable's `__len__()` does not prove how many items it will yield.
`count()` traverses such a source. Only trusted source metadata, such as the
current length of an exact builtin list, can avoid that traversal.

## One synchronous entry point

`flow()` is the normal synchronous constructor for both scalar and record data.
Record-specific methods whose names do not conflict with Flow semantics enter a
`Rows` view automatically.

```python
from fpstreams import flow

records = flow([{"id": 1, "score": 8}, {"id": 2, "score": 5}])
selected = records.select("id").to_list()
assert selected == [{"id": 1}, {"id": 2}]
```

Use `.rows()` when a name already has a scalar Flow meaning:

| Scalar Flow | Relational Rows |
| --- | --- |
| `drop(3)` skips three items | `rows().drop("column")` removes a field |
| `join(",")` joins strings | `rows().join(other, on="id")` joins relations |
| `aggregate(...)` returns a dictionary | `rows().aggregate(...)` remains lazy |
| `where(predicate)` aliases `filter` | `rows().where(active=True)` accepts equalities |

`Rows.map()` and `Rows.flat_map()` return a normal Flow because an arbitrary
callable may stop producing records. You can enter a Rows view again later.

## Selectors and expressions

A selector describes how to read a value. Public APIs accept direct names,
indexes, paths, callables, or inspectable expressions depending on the
operation.

```python
from fpstreams import col, flow, item

numbers = flow(range(8)).map(item * 3).filter(item % 2 == 0)
records = flow([{"price": 10, "quantity": 3}]).with_columns(total=col("price") * col("quantity"))
```

Prefer an expression or direct field selector when it describes the operation
clearly. The planner can inspect these forms and may compile them. Use a callable
for arbitrary Python behavior; callable exceptions, side effects, and encounter
order remain part of the program's observable semantics.

Boolean expression operators use `&`, `|`, and `~`, with parentheses around
each comparison:

```python
paid_large = (col("status") == "paid") & (col("amount") >= 100)
```

Python's `and`, `or`, and `not` cannot be overloaded into expression trees.

## Four execution domains

| Entry point | Domain | Typical work |
| --- | --- | --- |
| `flow(source)` | Synchronous values and records | Transform, reduce, collect, relational views |
| `aflow(source)` | Async iterables | Concurrent I/O, merge, time and cancellation operators |
| `rows(source)` | Explicit relational view | Record I/O, joins, grouping and reshape |
| `pairs(source)` | Key/value data | Per-key transforms and aggregation |

These entry points construct plans and share source ownership when you switch
views. Each adapter defines when it imports or converts its input; see the
[I/O matrix](../reference/io.md).

## Planning and engines

The default `auto` engine chooses among Python iteration, Rust kernels,
Arrow and NumPy operations, and plans that combine them. Engine selection
must preserve values, exceptions, ordering, one-shot behavior, and callback
effects.

```python
from fpstreams import flow, item

pipeline = flow(range(1_000_000)).map(item * 2).filter(item % 3 == 0)
explanation = pipeline.explain(terminal="sum")
print(explanation)
```

Use `explain()` to inspect the selected engine, stages, data movement,
materialization boundaries, complexity, and diagnostics. Force an engine only
for debugging or controlled deployment requirements:

```python
pipeline.with_engine("python").sum()
pipeline.with_engine("native").sum()  # raises if the exact plan is unsupported
```

If `auto` falls back to Python, streaming operations still pull values lazily.

## Streaming, buffering, and materialization

Operations fall into three broad memory shapes:

- streaming operators such as `map`, `filter`, and `take` hold constant or
  bounded local state;
- bounded operators such as `window`, `chunk`, buffered async operators, and
  external sorting retain an explicit amount of state;
- global operators such as in-memory sorting, exact grouping, pivoting, and
  `to_list` must retain data proportional to the input or output.

`chunk(size)`, `window(size, step=...)`, and `batch_by_size(max_size,
max_count=...)` require positive integer bounds in both sync and async pipelines.
They accept objects with `__index__`, normalize each bound when you build the
pipeline, and reject floats (including NaN and infinity) before opening the source.
For `batch_by_size`, `max_count=None` leaves the item count unlimited. If
`get_size` can return zero, set `max_count` to bound the number of retained items;
`max_size` measures your callback’s units, not the batch’s memory usage.

Large global operations can use spill settings. Exceeding a configured
partition, fan-out, output, or byte budget raises `BufferLimitError` and ends
the query.

## Resource ownership

The executor closes the source iterator and resources it opens. Early
termination, callback failure, cancellation, and terminal failure still enter
the same cleanup path. A user-supplied object that fpstreams did not open remains
owned according to the adapter's documented contract.

For async concurrency, fpstreams also owns scheduled tasks. A short-circuiting
terminal cancels outstanding work before it returns or propagates an error.

## Stable semantics before acceleration

An optimized path is eligible only when it can preserve the canonical behavior.
Important boundaries include:

- record and container subclasses may override Python protocols;
- custom metaclasses may override class equality, so equality with `list` or
  `int` cannot establish an exact builtin type;
- mapping lookup can invoke custom hash and equality code;
- a selector or aggregation callable can mutate live input;
- an iterator cannot be replayed after a speculative executor has pulled it;
- integer range and floating-point behavior must match the documented terminal.

These constraints explain why two operations that look similar can select
different physical paths. They also keep engine choice from changing user code.

## Next steps

- Browse the [API index](../reference/index.md) by task.
- Review [data input and output](../reference/io.md) before connecting another
  dataframe or storage system.
- Read [performance and execution](performance.md) before tuning a workload.
- Use the [browser playground](../playground.md) to try core APIs immediately.

## Inputs that are already sorted (current source)

These APIs are available in the checkout, but are not part of published 2.1.0.
They consume ascending inputs without building a hash index over the whole source.
Keys must be exact built-in integers, strings, bytes, or nonempty flat tuples of
these types. Each key position must keep the same type across both inputs.

Group paid orders that already arrive in region order:

```python
from fpstreams import agg, flow

orders = [{"region": "eu", "amount": 12}, {"region": "eu", "amount": 36},
          {"region": "us", "amount": 20}]
print(flow(orders).rows().group_by_sorted("region").aggregate(
    orders=agg.count(), revenue=agg.sum("amount")
).to_list())
# [{'region': 'eu', 'orders': 2, 'revenue': 48},
#  {'region': 'us', 'orders': 1, 'revenue': 20}]
```

Merge two chronological logs. Equal timestamps keep the left input first:

```python
from fpstreams import flow

left = [{"time": 1, "event": "start"}, {"time": 3, "event": "end"}]
right = [{"time": 2, "event": "update"}]
print(flow(left).merge_sorted(right, key="time").to_list())
# [{'time': 1, 'event': 'start'}, {'time': 2, 'event': 'update'},
#  {'time': 3, 'event': 'end'}]
```

Join sorted order details to a sorted customer table:

```python
from fpstreams import flow

orders = [{"customer": 1, "amount": 12}, {"customer": 1, "amount": 36}]
customers = [{"customer": 1, "name": "Ana"}]
print(flow(orders).rows().join_sorted(
    customers, on="customer", validate="m:1"
).to_list())
# [{'customer': 1, 'amount': 12, 'name': 'Ana'},
#  {'customer': 1, 'amount': 36, 'name': 'Ana'}]
```

Ordering and cardinality checks apply only to consumed rows. `take()` does not
scan the remainder for errors. Grouping keeps current collector state, which can
still grow if a collector stores every value. A sorted join buffers the current
right key group before its first match; a large duplicate group delays that result.
Join limits count rows, not bytes, so one large record can still use substantial
memory. These operations currently run in Python.
