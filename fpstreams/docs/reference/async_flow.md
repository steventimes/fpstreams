# AsyncFlow

`AsyncFlow[T]` combines async iteration with bounded concurrent mapping, merging,
rate control, latest-request switching, timeouts, and buffer-by-time operations.

Use `aflow(source)` with an iterable or async iterable.

## Consumption and cleanup

In the 2.5.0 async uniqueness path, `distinct` and `distinct_by` compare
hashable and unhashable keys across categories. The first accepted equal key
wins; additional equality callbacks and their errors are observable. State uses
O(H + U) references and cross-category comparisons can take O(H × U) time for
H hashable and U unhashable keys. A new hashable key is compared as `seen == key`
against prior unhashable keys; a new unhashable key is compared as `key == seen`
against prior hashable keys in encounter order.

Async `group_runs` reads the first item of the next group and resolves its key
before emitting the previous group as a complete tuple. A key failure at that
boundary occurs before the previous group is emitted. It holds the whole
current group in memory and keeps original item references in the tuple. An
infinite run with no key change cannot produce a completed group.

In the 2.5.0 async `concat` path, an additional source is opened only
when reached and is closed on completion or early exit. A directly supplied
one-shot iterator shared with the input keeps one owner, including when it is a
synchronous iterator adapted for async iteration.

In the 2.5.0 async grouping path, `reduce_by` and `frequencies` propagate
errors raised by a key's custom hash or equality operation unchanged. An
unhashable key raises `TypeError`.

In the 2.5.0 source, async `chunk(size, strict=True)` reads an
incomplete tail before raising `ValueError` and does not emit that tail. The
default emits the shorter tuple. Stopping before the tail does not validate it;
when the error occurs, the operation closes the owned source when it supports
closing. Async `batch_by_size` checks the next item
and its size before emitting a pending batch, including when `max_count` has
been reached. A source error, size error, or strict oversized item can prevent
that pending batch from being emitted. Normal exhaustion emits a nonempty tail.

Async `zip` and `zip_longest` await their left and right inputs in order, not
concurrently. A left input that waits for the right input to start can stall.
The right input can end after the left has already produced an extra item.
Strict zip validates unequal lengths only as far as it is consumed.
`zip_longest` defaults to `None` padding; use a unique sentinel to distinguish
padding from a real `None`. A mutable fill value is reused by reference.
`combine_latest` cannot emit until every source has a value; when a source
ends empty, the other pending sources are cancelled and closed.

Direct async iteration keeps the output iterator open after `break` if the
caller retains it. Close it explicitly when stopping early:

```python
from contextlib import aclosing
from fpstreams import aflow

async def first_three():
    async with aclosing(aiter(aflow(range(10)))) as values:
        async for value in values:
            if value == 2:
                break
```

When a downstream `take(1)` yields its last item through direct iteration,
cleanup may wait until the iterator is resumed or explicitly closed. Terminal
methods manage their iterator when they finish. An `aflow` backed directly by
an async generator closes that owned generator when `first()` stops. A later
concat source that has not been opened is not closed merely because an earlier
source short-circuits. `merge_map` inner sources can outlive the outer source;
manage any external resources they depend on for the needed lifetime.

Async `scan` has the same [state reference and replay rules](flow.md#scan-state-and-gatherer-completion)
as synchronous `scan`; an empty source emits no state. `scan_right` buffers and
folds the full source before emitting. Its `max_items` check reads one more
item when detecting an overflow, just as in the synchronous path. Use
`aflow.defer` to create the pipeline and mutable seed anew for each execution.

For a direct `aflow.defer(factory)` source, `take(0).to_list()` opens and closes
the iterator without pulling it, whereas `collect(Collectors.head(0))` does
not call the factory. A factory error still propagates in the first case.
`intersperse` reads the next item before emitting its separator. `delay` opens
the source before its initial wait but does not pull during that wait;
`throttle` may already have pulled an item while waiting to emit it. A prefix
result does not validate unread items or errors.

## Flattening mapped iterables

`flat_map()` expands each mapped iterable in order, accepting either a
synchronous or asynchronous iterable. A string expands into characters and a
dictionary into keys; wrap the value in another iterable to keep it intact.

The serial implementation manages each acquired inner iterator and attempts
its `close()` or `aclose()` when supported, including on exhaustion, failure,
or downstream early termination. If a mapper returns an iterator object that
returns itself from `__iter__` or `__aiter__`, the current implementation calls
that method once to acquire it and again when entering the iteration loop.
Side effects in these methods can therefore be observed twice.

## Statistics and rolling windows (2.5.0)

`rolling_statistics(size, selector=..., ddof=0)` returns an AsyncFlow of
`(count, mean, variance)` tuples. The selector remains synchronous. Missing
values, warmup output, configuration errors, and fresh-state rules match
[Flow statistics](flow.md#statistical-pipelines-250).

```python
import asyncio
from fpstreams import Collectors, aflow


async def summarize():
    summary = await aflow([2, None, 4, 6]).collect(Collectors.statistics())
    windows = await aflow([2, None, 4, 6]).rolling_statistics(2).to_list()
    assert summary["mean"] == 4.0
    assert windows[-1] == (2, 5.0, 1.0)


asyncio.run(summarize())
```

Collector state callbacks follow their synchronous contract; await the async
terminal that consumes the flow. Cancellation closes the owned upstream
iterator. The optional sketches use the same `collect()` entry point; see
[Collector statistics](collecting.md#shared-and-approximate-statistics-250).
These additions require the source version.

## Creating an async flow

| Call | Behavior |
| --- | --- |
| `aflow(source)` | Wrap a synchronous or asynchronous iterable |
| `aflow.defer(factory)` | Call the source factory for each execution |
| `aflow.unfold(seed, function)` (2.4.0) | Resolve `(value, next_state)` transitions until `None` is returned |
| `aflow.from_queue(queue, stop=...)` | Read a caller-owned `asyncio.Queue` once, optionally until an identity sentinel |
| `aflow.from_file(path)` | Read text lines without blocking the event loop |
| `aflow.interval(seconds)` | Emit increasing integers on a timer |
| `aflow.paginate(fetch_page)` | Fetch pages until the returned cursor is `None` |

Queue sources do not call `task_done()` and do not own the producer or queue.
Avoid `prefetch()` when `Queue.join()` or per-item acknowledgements must track
each `get()` exactly.

File sources own their handles and use the optional `async` extra (`aiofiles`).
In the 2.5.0 source, cancellation waits for in-flight file operations and
any required close attempt before propagating. If close fails, cancellation is
preserved and the close failure is added to its notes. Cancelling an await does
not stop a filesystem call running in a thread: a stalled call can delay
cancellation indefinitely. Closing the file does not shut down the event loop's
shared executor.

## Reduce values and check their types

`await flow.reduce(function, initial)` reduces from left to right. The callback
may return a value or an awaitable; an awaitable callback result is awaited
once. An empty flow returns the supplied initial object unchanged. Without an
initial value, a singleton returns its item unchanged, even if that item is
awaitable; an empty flow raises `EmptyFlowError`.

`reduce_right` passes `(item, accumulator)` to the callback and optionally
limits buffering with `max_items`; `fold_right` is its alias. These terminal
methods currently return `Any` to type checkers after awaiting, so an annotated
callback alone does not narrow the result. See the
[synchronous reduce type examples](flow.md#reduce-types) for a caller-owned
runtime check. `fold`, by contrast, returns a lazy `AsyncFlow` and resolves its
initializer factory result once when the flow is consumed.

```python
import asyncio
from fpstreams import aflow


async def main():
    seed = asyncio.get_running_loop().create_future()
    seed.set_result(10)
    assert await aflow([]).reduce(lambda total, item: total + item, seed) is seed
    assert await aflow([1]).reduce(lambda total, item: seed, 0) == 10


asyncio.run(main())
```

### Explicit asynchronous accumulator contract (2.5.0)

`fold_async(initializer, function)` returns a lazy `AsyncFlow[R]`. Both hooks
must return awaitables under this typed contract. Each hook result is awaited
exactly once, and its
resolved value becomes the state. Annotate the resulting flow to declare the
state contract and provide context for ordinary lambda callbacks:

```python
import asyncio
from fpstreams import AsyncFlow, aflow


async def initial_text() -> str:
    return ""


async def append_text(state: str, item: int) -> str:
    return state + str(item)


async def main():
    query: AsyncFlow[str] = aflow([1, 2]).fold_async(
        initial_text, lambda state, item: append_text(state, item)
    )
    assert await query.first() == "12"


asyncio.run(main())
```

This explicit path keeps the existing `reduce` and mixed-hook `fold` interfaces.
It does not improve the old `reduce` return declaration. An empty source emits
the resolved initializer state. Every source item invokes the step, including a
singleton, so migration from unseeded `reduce` requires an explicit initial
state. Each evaluation calls the initializer again.

The state `R` may itself be awaitable. For example, an `async def` initializer
annotated `-> asyncio.Future[int]` can return a Future as state, and an
`async def` step with the same return annotation can retain it. The hook
coroutines are awaited once; the Future is neither awaited again nor replaced,
even on empty or singleton inputs. A synchronous initializer returning the
Future directly belongs to the existing mixed-hook `fold`, which resolves that
Future and uses its value as state. Wrap the initializer and step in async
functions when the Future object itself is the intended state.

As with other Python type annotations, `Any` can bypass this explicit contract.
Execution reuses `fold`; it does not add runtime checks for hook awaitability
or state types, and nested `Any` remains unknown. Use a
caller-owned runtime check when accepting untrusted dynamic callback results.

## Thread state through async callbacks (2.4.0) {#thread-state-through-async-callbacks-unreleased}

`AsyncFlow.unfold` and `aflow.unfold` accept synchronous, asynchronous, or mixed
callbacks. Each call resolves before the next transition starts. Returning
`(None, next_state)` emits `None`; returning `None` ends the source.

`map_accum(initial, function)` consumes an existing source in encounter order.
The callback resolves to `(next_state, output)`, and only the output is emitted:
an awaitable transition is awaited once, then an awaitable output is also
awaited once. The next state is passed through without awaiting it. Synchronous
`Flow.map_accum()` instead emits its output unchanged, including an awaitable.
When a task is cancelled while awaiting a native Future output, that Future is
normally cancelled too; `asyncio.shield()` can protect an underlying Future
from this cancellation. Other cancellation sources can still cancel it.

```python
import asyncio
from fpstreams import aflow


async def countdown(state: int) -> tuple[int, int] | None:
    await asyncio.sleep(0)
    return (state, state - 1) if state else None


async def running(total: int, value: int) -> tuple[int, int]:
    await asyncio.sleep(0)
    next_total = total + value
    return next_total, next_total


async def main():
    query = aflow.unfold(3, countdown).map_accum(0, running)
    assert await query.to_list() == [3, 5, 6]
```

Neither operation starts background work or prefetches the next transition.
Cancellation propagates to the active callback and closes the owned iterator
chain. Each evaluation begins with the supplied seed by reference; use
`aflow.defer` when mutable state needs a new instance. See the
[synchronous state examples](flow.md#state-driven-generation-and-mapping-unreleased)
for tuple ordering and empty-input behavior.

## Reuse sliding-window aggregates (2.4.0) {#reuse-sliding-window-aggregates-unreleased}

`AsyncFlow.window_reduce(size, combine, step=1)` uses the same ordered aggregation
strategies and [algebraic contract](flow.md#reuse-sliding-window-aggregates-unreleased)
as the synchronous operation. The combine function is synchronous. It must be
associative and must not modify its arguments; source values and emitted
aggregates must remain unchanged during iteration. Awaitable combine functions
are not supported.

```python
import asyncio
from operator import add
from fpstreams import aflow

async def main():
    assert await aflow([2, 3, 5, 7]).window_reduce(3, add).to_list() == [10, 15]
```

Nonoverlapping windows use a running fold without a raw window buffer.
The operation creates no background task or prefetch buffer. Cancellation while
waiting for the next source item propagates upstream and closes the iterator
chain. A stack transfer runs synchronously and can take linear work in the
window size; this operation adds no scheduling checkpoint inside a combine call.

## Reduce adjacent runs with a Collector (2.4.0) {#reduce-adjacent-runs-with-a-collector-unreleased}

`AsyncFlow.collapse(predicate, collector)` follows the same
[run and Collector rules](flow.md#reduce-adjacent-runs-with-a-collector-unreleased).
The adjacent predicate may be synchronous or awaitable:

```python
import asyncio
from fpstreams import Collectors, aflow

async def adjacent(left, right):
    await asyncio.sleep(0)
    return 0 <= right - left <= 2

async def main():
    result = await aflow([1, 2, 4, 9, 11, 1]).collapse(
        adjacent, Collectors.head(1)
    ).to_list()
    assert result == [[1], [9], [1]]
```

Collector hooks retain their synchronous protocol. An awaitable returned by
the finisher is emitted as data, without an implicit await. No background task
or prefetch buffer is added. Cancellation while reading the source or waiting
for the predicate closes upstream without finishing the pending run.

## Bound pull-ahead work

`prefetch(capacity)` lets a producer pull ahead while retaining at most
`capacity` accepted values. It preserves encounter order and cancels its owned
producer task when downstream stops or fails.

~~~python
values = await aflow(source).prefetch(32).map_async(transform, concurrency=8).to_list()
~~~

Prefetch is useful when upstream latency and downstream work overlap. It is not
a concurrency setting for `map_async`, and increasing the capacity also
increases the maximum retained input.

On normal completion, accepted values are emitted in order. If the producer
fails, values already accepted by the buffer are emitted before its error.
An early downstream stop or external cancellation cleans up the producer, but
does not promise delivery of every in-flight value.

With `map_async(..., ordered=False)`, results arrive in completion order.
Following it with `take(n)` need not return the first `n` inputs, and callbacks
whose results are not returned may already have started. With ordered output,
a later callback can fail while an earlier result is still pending; a full
consumer can receive that error, while `take(1)` may end after the first result
without receiving it. Mapper timeouts use cooperative cancellation: a callback
that suppresses cancellation can return a value, and waiting for cleanup can
exceed the timeout.

If a task factory rejects a scope's cleanup task, the scope waits for its existing
tasks to stop before upstream resources close. The caller that encountered the
rejection still receives that submission error. No replacement task is created
for this fallback; later close calls can observe that cleanup has finished.

In the 2.5.0 source, cancelling a caller that is already waiting for a
cancelled child to stop does not cancel that child again. The wait finishes
before caller cancellation propagates, including when a timeout has cancelled
an upstream pull. A child that never finishes can delay this wait indefinitely.

`switch_map` also closes its owned outer iterator if its first pull task cannot
be admitted. The mapper is not called when this submission fails.
Query ownership also closes an acquired root iterator when an intermediate
stage never starts, such as `map(...).take(0)`, without pulling an item.
Synchronous iterators adapted by `aflow` follow the same ownership rule.

## Build bounded sessions

`session_window(idle_for, max_count=...)` groups consecutive values until the
source stays quiet for `idle_for` seconds. `max_count` is a required hard cap;
reaching it flushes the session even if the idle timer has not fired. Source
completion flushes the final non-empty session.

## Control request rates

Place `throttle` before concurrent work when an API accepts only a fixed number of requests per
time window. The first window may start with a burst; later items wait without filling a background
queue.

~~~python
results = await (
    aflow(requests)
    .throttle(5, per=1.0)  # Start at most five requests per second.
    .map_async(send_request, concurrency=3)  # Keep at most three requests in flight.
    .to_list()
)
~~~

Use `spaceout(seconds)` when every pair of emissions needs a minimum gap. `delay(seconds)` waits
once, before the first upstream item is requested.

In the 2.5.0 source, NaN durations raise `ValueError` when configuring
`interval`, `delay`, `throttle`, `spaceout`, `timeout`, `debounce`,
`buffer_timeout` (also `batch_timeout`), `map_async`, and `map_batches`.
`session_window` already rejects NaN. Zero remains valid for `interval` and
`debounce`; the other durations must be positive. Positive infinity remains
accepted and can leave timed work waiting indefinitely. Validation does not
open the source or start a mapper.

## Keep only the latest work

`switch_map` is useful for search boxes, live filters, and other inputs where a newer value makes
older work irrelevant. When a new outer item arrives, fpstreams cancels and closes the previous
inner source. The last inner source can finish after the outer source ends.

~~~python
matches = await (
    aflow(query_changes)
    .switch_map(search_pages)  # Switch away from the previous result stream.
    .to_list()
)
~~~

`search_pages` may return an iterable, an async iterable, or an awaitable containing either one.
Inner iterators used by `merge_map` and `switch_map` do not need an `aclose` hook.
If an inner exposes a callable hook, it is selected when the inner opens and
called once during cleanup. Its result is awaited only when it is awaitable.
Cancellation and cleanup apply to work owned by the pipeline. Whether an
external request, thread, or independently created task actually stops depends
on that work's implementation.

In the 2.5.0 source, the concurrency limits of `map_async` and `merge_map`,
and the result buffer of `map_async`, must be positive integers.
Integer-like values use `__index__` and
are normalized when configuring the flow, before it opens its source.
Floating-point values, including NaN and infinity, raise `TypeError`.

## Reduce without materializing

`sum`, `min`, `max`, and `minmax` consume the source once without retaining its
input items; accumulator size still follows the result type. `sum` accepts the
same `start` argument and rejects string or bytes starts in the same way as
Python's built-in `sum`.

`min`, `max`, and `minmax` accept a callable, field name, integer index, dotted path, or row
expression as `key`. A callable key may return a value directly or return an awaitable. Equal keys
retain the first item encountered.

~~~python
lowest, highest = await aflow(events).minmax(key="metrics.latency_ms")
total = await aflow(amounts).sum(start=opening_balance)
~~~

The extreme-value terminals raise `EmptyFlowError` on an empty source. `sum` returns `start` for an
empty source. Exceptions and cancellation close the owned upstream iterator before propagating.

## Methods

::: fpstreams.AsyncFlow
    options:
      members_order: source
      show_root_heading: true
      show_source: false
