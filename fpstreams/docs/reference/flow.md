# Flow

`Flow[T]` is the primary synchronous lazy pipeline for both ordinary values and
records. Transformations return a new pipeline; terminal methods execute its
plan.

Use `flow(source)` for an existing source and `flow.defer(factory)` when every
execution must create a fresh iterable. Besides ordinary iterables,
`flow(source)` retains supported PyArrow, pandas, Polars, Arrow C stream, and
dataframe-interchange inputs.

`chunk(size, strict=True)` (2.5.0) rejects an incomplete final batch when
the flow reaches it. The default continues to emit a shorter final batch.
`batch` is an alias; `AsyncFlow.chunk` provides the same option.
In strict mode, the short tail has already been read when `ValueError` is
raised; it is not emitted. To inspect a short tail yourself, use the default
mode and check the tuple length. Stopping before that tail does not validate it.

In the 2.5.0 source, `map_parallel` and `parallel` normalize non-`None`
`workers` and `buffer` values
with the integer `__index__` protocol when configuring the flow, before opening
its source. Values must be positive. Floating-point values, including NaN and
infinity, raise `TypeError`.

## Consumption, batching, and cleanup

In the 2.5.0 Python uniqueness path, `distinct` and `distinct_by` also
compare hashable keys with previously seen unhashable keys, and vice versa.
The first accepted equal key wins. Mixed categories can invoke additional
equality callbacks and propagate their errors. State retains distinct keys;
with H hashable and U unhashable keys, cross-category comparisons may take
O(H × U) time and state uses O(H + U) references. Existing same-category
comparison rules remain unchanged. For a new hashable key, each previously
accepted unhashable key is compared as `seen == key`; for a new unhashable key,
accepted hashable keys are compared as `key == seen` in encounter order.

`group_runs` emits each contiguous group as a complete tuple. To detect a
group boundary, it reads the first item of the next group and computes its key
before emitting the previous group. If that key computation fails, the previous
group has not yet been emitted. It holds the whole current group in memory;
the tuple contains the original item references, without deep copies. An
infinite run with no key change cannot produce a completed group.

In the 2.5.0 source, `concat` opens each additional source only when it
reaches it and closes an opened iterator on completion or early exit. Passing
the same one-shot iterator as the input and as an additional source does not
give the additional source a second ownership of that iterator.

In the 2.5.0 Python grouping path, `reduce_by` and `frequencies`
propagate errors raised by a key's custom hash or equality operation unchanged.
An unhashable key raises `TypeError`.

`zip` pulls the left input before the right input. If the right input ends
first, a left item can already have been consumed without appearing in a pair.
This also matters when the inputs have side effects. `strict=True` checks for
unequal lengths during consumption, so a downstream `take` cannot validate an
unconsumed tail. `zip_longest` uses `None` for padding by default, which is
indistinguishable from an input `None`. Pass a unique sentinel when the
distinction matters. A mutable `fillvalue` is reused, not copied.

`intersperse` reads the next input item before emitting its separator.
Consequently, `intersperse(...).take(2)` can consume two source items even
though only the first item and a separator are returned. A successful `first`
or `find` only checks the consumed prefix; consume the entire source if all
items and source errors must be checked.

`batch_by_size` checks the next item's size before emitting a pending batch,
including when `max_count` has already been reached. An oversized next item
in strict mode or an error from the source or size function can therefore stop
the flow before that pending batch is emitted. Normal exhaustion emits the
remaining nonempty batch. The operation does not copy the items in its tuples.

With `map_parallel(..., ordered=False)`, results arrive in completion order;
following it with `take(n)` does not select the first `n` inputs. Work already
submitted can have started even when its result is never returned.

Terminal operations own their iteration and close it when they stop. When
iterating directly, keep and explicitly close the iterator if you stop early;
`break` alone does not close a retained iterator. In the Python path, direct
iteration of `take(1)` can yield its last item while the source remains open;
cleanup occurs when iteration resumes or the iterator is explicitly closed.
For an iterator with a
`close` method, `contextlib.closing` can express this ownership:

```python
from contextlib import closing
from fpstreams import flow

with closing(iter(flow(range(10)))) as values:
    for value in values:
        if value == 2:
            break
```

For a direct `flow.defer(factory)` source, `take(0).to_list()` opens and closes
the factory's iterator without pulling an item, while
`collect(Collectors.head(0))` does not call the factory. Factory errors in the
first case still propagate. Custom iterators need not implement `close`.

## Flattening mapped iterables

`flat_map()` expands one iterable level in order. A string is iterable and
therefore yields its characters; a dictionary yields its keys. Wrap either in
another iterable to keep it as one value.

In the synchronous Python path, a mapped iterator is delegated with
`yield from`. It is not explicitly closed when it is exhausted normally or when its
own `__next__` raises. Closing an active delegated iterator after downstream
early termination calls its `close()` if available. A generator may also run its
own cleanup as it finishes; these details do not imply that every iterable
provides a close hook.

## Scan state and Gatherer completion

`scan(initial, function)` emits one state per input and emits nothing for an
empty input. It does not copy the initial state or results. If a callback
mutates and returns one object, previously emitted references can observe
later mutations; the supplied seed is also reused on another evaluation. Return
fresh values for snapshots, and create a fresh pipeline and seed with `defer`
when each execution needs independent state:

```python
from fpstreams import flow

def add_key(counts, key):
    return {**counts, key: counts.get(key, 0) + 1}

counts = flow.defer(lambda: flow(["a", "b", "a"]).scan({}, add_key))
expected = [{"a": 1}, {"a": 1, "b": 1}, {"a": 2, "b": 1}]
assert counts.to_list() == expected
assert counts.to_list() == expected
```

`scan_right` buffers the full source and computes the right fold before its
first output; it emits nothing for an empty source. With `max_items`, it must
read the next item to detect an exceeded limit, so the limit bounds buffered
items rather than all source reads.

For the synchronous `Gatherer`, the finisher runs on normal exhaustion and
when the gatherer stops input. Downstream early close can also run it with a
rejecting output channel. It must not infer that the source was exhausted from
the number of items seen. Source or integration failures do not turn the
finisher into a `finally` hook; manage separately owned resources explicitly.
In a composed gatherer, an initializer or finisher failure can prevent later
hooks from running.

## Reduce values and check their types {#reduce-types}

`reduce(function, initial=...)` combines values from left to right. Without an
initial value, the first element is the accumulator; an empty flow raises
`EmptyFlowError`. With an initial value, an empty flow returns that exact object.
The callback must accept its own previous result as the next accumulator.

The current declaration accepts `Callable[[Any, T], Any]` and returns `Any`,
with or without an initial value. Runtime results can be more specific, but
neither an ordinary lambda nor a named, annotated callback refines that public
return type:

```python
from typing import Any, assert_type
from fpstreams import flow

assert_type(flow([1, 2]).reduce(lambda a, b: a + b), Any)  # runtime: 3
assert_type(flow([1, 2]).reduce(lambda a, b: a + str(b), ""), Any)  # runtime: "12"
```

`Any` can also let an invalid callback pass static checking. For example,
`def step(a: int, b: int) -> str: return str(a + b)` works for `[1, 2]`
without an initial value, but fails with `TypeError` for `[1, 2, 3]`: its next
accumulator is a string, which the callback cannot add to an integer.

For an explicit accumulator contract, use the existing lazy `fold` operation and
annotate its result. This gives ordinary lambdas concrete parameter types in both
mypy and Pyright. The initializer creates state, and `.first()` returns the one
emitted result:

```python
from fpstreams import Flow, flow


def initial_text() -> str:
    return ""


query: Flow[str] = flow([1, 2]).fold(
    initial_text, lambda state, item: state + str(item)
)
assert query.first() == "12"
assert Flow[int]([]).fold(initial_text, lambda state, item: state).first() == ""
```

This is a migration from `reduce`, whose return declaration remains `Any`.
`fold` requires an initializer and calls the step for every input item, including
the first. It emits the initializer's exact result on empty input and never
awaits state values. To migrate an unseeded reduction, choose an explicit seed
that suits the operation; a singleton now also runs the step. The initializer
runs once per evaluation, so use a factory when each evaluation needs fresh
mutable state. A callback or seed containing `Any` can bypass the declared
contract. These annotations add no runtime state checks and do not turn
`list[Any]` into `list[int]`. For untrusted dynamic results, keep the runtime
check described below.

For a concrete result, receive it as `object` and check it before use:

```python
from fpstreams import flow

result: object = flow([1, 2]).reduce(lambda a, b: a + b)
if not isinstance(result, int):
    raise TypeError("expected an integer accumulator")
assert result == 3  # narrowed to int
```

When several calls share a specific accumulator contract, a local boundary can
check the callback signature and the returned value:

```python
from collections.abc import Callable
from fpstreams import Flow, flow


def text_step(accumulator: str, item: int) -> str:
    return accumulator + str(item)


def reduce_text(values: Flow[int], step: Callable[[str, int], str], initial: str) -> str:
    result: object = values.reduce(step, initial)
    if not isinstance(result, str):
        raise TypeError("text reducer returned a non-string")
    return result


callback: Callable[[str, int], str] = text_step
assert reduce_text(flow([1, 2]), callback, "") == "12"
```

This is a caller-owned string example, not an additional fpstreams API or a
promise of arbitrary generic safety. A callback returning `Any` can still bypass
static signature checks; the explicit result check above catches a non-string
result if the reduction returns. It does not validate every intermediate value.
A nullable accumulator must accept `None`; nested `Any`, such as `list[Any]`,
still carries unknown element types. Neither should be relabelled as a more
precise type without evidence.

In a source checkout, `tests/typing/reduce_examples.py` contains the runnable
examples, nullable and nested-`Any` cases, identity checks, and a runtime witness
for the invalid callback. `tests/typing/reduce_invalid.py` contains marked expected
static errors and must not be executed. From the checkout root, the unified
`uv run python scripts/check_typing.py` command checks both example pairs with
mypy and Pyright, one invocation per checker. See the repository's CONTRIBUTING
"Run the checks" section for setup and target selection. No optional data backend
is needed to run the reduce examples.

## Statistical pipelines (2.5.0)

Use `collect(Collectors.statistics(...))` for one summary over the whole input,
or `rolling_statistics(size, selector=..., ddof=0)` for one result per input item:

```python
from fpstreams import Collectors, flow

assert flow([2, None, 4, 6]).collect(Collectors.statistics())["mean"] == 4.0
assert flow([2, None, 4, 6]).rolling_statistics(2).to_list() == [
    (1, 2.0, 0.0), (1, 2.0, 0.0), (1, 4.0, 0.0), (2, 5.0, 1.0)
]
```

Rolling results are `(count, mean, variance)` tuples, including warmup windows.
Selected `None` occupies a window position and contributes no observation.
Size and `ddof` use the integer index protocol: invalid types raise `TypeError`,
and a nonpositive size or negative `ddof` raises `ValueError` at construction.
Non-real inputs raise `TypeError` during consumption; nonfinite rolling inputs
raise `ValueError`. Each evaluation starts with fresh window state.

See [shared and approximate statistics](collecting.md#shared-and-approximate-statistics-250)
for Collector input policies and optional sketches, and the
[rolling recipe](../user-guide/analytics.md#rolling-numerical-statistics-250)
for state bounds and rebuilding costs. These additions require the source version.

## State-driven generation and mapping (2.4.0) {#state-driven-generation-and-mapping-unreleased}

`unfold` builds a source from state, while `map_accum` transforms an existing
source with state. They use different tuple orders:

| Operation | Callback result | Emitted value |
| --- | --- | --- |
| `flow.unfold(seed, function)` | `(value, next_state)` or `None` to stop | `value` |
| `query.map_accum(initial, function)` | `(next_state, output)` | `output` |

```python
from fpstreams import flow

countdown = flow.unfold(3, lambda n: (n, n - 1) if n else None)
assert countdown.to_list() == [3, 2, 1]
assert countdown.take(1).to_list() == [3]
```

The unfold seed is not emitted by itself. Returning `(None, next_state)` emits
an actual `None`; only returning `None` ends generation. The callback controls
termination, so bound an infinite transition with `take` or another stopping
operation. Construction and `take(0)` do not run the callback.

```python
from fpstreams import flow


def running_total(total, value):
    next_total = total + value
    return next_total, {"value": value, "total": next_total}


query = flow([2, 3, 5]).map_accum(0, running_total)
assert query.to_list() == [
    {"value": 2, "total": 2},
    {"value": 3, "total": 5},
    {"value": 5, "total": 10},
]
```

`map_accum` calls the function once per consumed item in encounter order. It
emits nothing for an empty source and does not emit its final state. Each
evaluation starts from the supplied initial state; mutable seed objects are
reused by reference, following `scan`. Use `flow.defer` to allocate fresh
mutable state for each evaluation. The operation keeps the source's replay
and cleanup rules and runs its state transitions sequentially even with
parallel map defaults.

## Reuse sliding-window aggregates (2.4.0) {#reuse-sliding-window-aggregates-unreleased}

`window_reduce(size, combine, step=1)` combines each count-based window without
building a tuple and folding every overlapping value again:

```python
from operator import add
from fpstreams import flow

assert flow([2, 3, 5, 7]).window_reduce(3, add).to_list() == [10, 15]
assert flow([2, 3]).window_reduce(3, add).to_list() == [5]
```

`combine` must be associative and must not modify either argument. Source values
and returned aggregates must remain unchanged during iteration because later
windows may reuse them. Encounter order is preserved, so a noncommutative
associative function is supported. Floating-point addition is not associative;
use `window(...).map(...)` when a left fold or arbitrary callback behavior matters.
There is no automatic conversion of existing window pipelines.

Empty input emits nothing. Initially short input emits one partial result;
after a full window, incomplete trailing windows are omitted. `step` may exceed
`size`; values in the gaps are consumed without being combined. Overlapping
windows can group calls differently from a left fold. A combine error can arise
while building a trailing window that would never be emitted. Stopping downstream
closes the owned iterator chain without pulling another item.

Overlapping windows use two aggregate stacks, following the storage insight in
[Two-Stacks Lite](https://arxiv.org/abs/2009.13768). They cache at most `size + 1`
entries. When `step >= size`, a running left fold replaces the stacks and keeps
no raw window buffer. Both paths preserve input order and fold incomplete tails,
even when those tails produce no output. Cached entries do not bound bytes:
an aggregate can grow with its input.
Combine calls are amortized constant per input item; transferring a stack can
take linear work in `size`. This does not provide DABA's worst-case constant
operation bound. Small windows and windows without overlap may cost more than
an ordinary fold; compare against `window(...).map(...)` for those workloads.

## Reduce adjacent runs with a Collector (2.4.0) {#reduce-adjacent-runs-with-a-collector-unreleased}

`collapse(predicate, collector)` feeds each nonempty adjacent run into fresh
[Collector](collecting.md) state. The predicate sees neighboring input items,
including when it is not transitive:

```python
from fpstreams import Collectors, flow

query = flow([1, 2, 4, 9, 11, 1]).collapse(
    lambda left, right: 0 <= right - left <= 2,
    Collectors.counting(),
)
assert query.to_list() == [3, 2, 1]
```

Each run has its own initializer, steps, completion state, and finisher. Empty
input emits nothing and initializes no collector. A completed collector stops
receiving steps, while adjacent predicates keep running until the run boundary.
For example, `Collectors.head(1)` keeps only the first item of each run; it
cannot finish the whole query after seeing the first item.

A boundary consumes one item of the next run before emitting the current
result. The next collector is initialized when consumption resumes, so `take(1)`
does not initialize an unused next run. The last run is finished after normal
source exhaustion. Source errors and early termination do not flush a pending
run. An infinite run still needs a boundary or source exhaustion to emit.

The operation keeps collector state and adjacent input references without a
raw run buffer. `Collectors.counting()` keeps one running integer;
`Collectors.to_list()` retains every run member. A growing collector state and
the final `to_list()` result can still grow with input size. Collector hooks run
while reading input, so callback errors can appear earlier than with
`group_runs(...).map(collector)`. This is an explicit overload, not an automatic
rewrite of buffered pipelines.

The adjacent predicate and the state program add work as input arrives.
When grouping by equal keys and the groups fit in memory, compare this overload
with `group_runs(...).map(collector)`. Buffered execution can be faster.

Hooks follow the synchronous Collector protocol, including `done`. A finisher
returning an awaitable emits that object as data. Collector `combine` and
`native` metadata are unused by this sequential reduction. The binary merger
form of `collapse(predicate, merger)` remains available.

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

An `exact` cardinality describes the number of outputs on normal, complete
consumption under the source facts available when `explain()` runs. It does not
guarantee that callbacks succeed or finish, and a referenced mutable source can
change before execution. `upper_bound` limits the outputs under the same source
facts; it does not guarantee normal completion. When a strict zip or chunk is
known to fail on a length mismatch, the planner reports a conservative upper
bound rather than an exact count. A downstream `take` may stop before that
failure is reached.

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
| `flow.scan_csv(path)` | Scan typed Arrow CSV batches; reader column selection requires explicit conversion options |
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
