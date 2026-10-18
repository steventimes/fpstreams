# AsyncFlow

`AsyncFlow[T]` combines async iteration with bounded concurrent mapping, merging,
rate control, latest-request switching, timeouts, and buffer-by-time operations.

Use `aflow(source)` with an iterable or async iterable.

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

## Thread state through async callbacks (2.4.0) {#thread-state-through-async-callbacks-unreleased}

`AsyncFlow.unfold` and `aflow.unfold` accept synchronous, asynchronous, or mixed
callbacks. Each call resolves before the next transition starts. Returning
`(None, next_state)` emits `None`; returning `None` ends the source.

`map_accum(initial, function)` consumes an existing source in encounter order.
The callback resolves to `(next_state, output)`, and only the output is emitted:

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

## Keep only the latest work

`switch_map` is useful for search boxes, live filters, and other inputs where a newer value makes
older work irrelevant. When a new outer item arrives, fpstreams cancels and closes the previous
inner source. The last inner source can finish after the outer source ends.

~~~python
matches = await (
    aflow(query_changes)
    .switch_map(search_pages)  # Cancel the previous search when the query changes.
    .to_list()
)
~~~

`search_pages` may return an iterable, an async iterable, or an awaitable containing either one.

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
