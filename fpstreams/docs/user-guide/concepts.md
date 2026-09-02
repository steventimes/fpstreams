# Core concepts

fpstreams describes work before it performs it. A pipeline contains a source,
ordered transformations, and enough facts for the planner to select a safe
executor. A terminal turns that description into values.

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
| Lazy relational operation | `group_by`, `join`, `pivot` | A relational plan |
| Materializing terminal | `to_list`, `to_dict`, `partition` | A container in memory |
| Scalar terminal | `count`, `sum`, `first`, `variance` | One scalar or optional value |
| Effect terminal | `for_each`, `to_csv`, `to_jsonl` | Performs the requested effect |

Iteration is also execution. `for value in pipeline` opens and owns one source
iterator in the same way as a terminal.

## Source replayability

The source, not the pipeline syntax, determines whether a plan can be run again.

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

fpstreams does not silently cache a generator. That would change memory use,
resource lifetime, callback timing, and visibility of upstream changes.

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

These are views over a small number of execution models, not independent data
containers that eagerly copy inputs.

## Planning and engines

The default `auto` engine chooses among canonical Python iteration, compiled
Rust kernels, Arrow-native prefixes, and guarded hybrid plans. Engine selection
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

`auto` falling back to Python is normal. It is not an error and does not mean
the whole library is running eagerly.

## Streaming, buffering, and materialization

Operations fall into three broad memory shapes:

- streaming operators such as `map`, `filter`, and `take` hold constant or
  bounded local state;
- bounded operators such as `window`, `chunk`, buffered async operators, and
  external sorting retain an explicit amount of state;
- global operators such as in-memory sorting, exact grouping, pivoting, and
  `to_list` must retain data proportional to the input or output.

Large global work can use explicit spill settings. Limits are correctness and
operational controls, not tuning hints: exceeding a configured partition,
fan-out, output, or byte budget raises `BufferLimitError` instead of quietly
using unbounded memory.

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
