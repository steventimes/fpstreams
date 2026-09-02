# AsyncFlow

`AsyncFlow[T]` combines async iteration with bounded concurrent mapping, merging,
rate control, latest-request switching, timeouts, and buffer-by-time operations.

Use `aflow(source)` with an iterable or async iterable.

## Creating an async flow

| Call | Behavior |
| --- | --- |
| `aflow(source)` | Wrap a synchronous or asynchronous iterable |
| `aflow.defer(factory)` | Call the source factory for each execution |
| `aflow.from_queue(queue, stop=...)` | Read a caller-owned `asyncio.Queue` once, optionally until an identity sentinel |
| `aflow.from_file(path)` | Read text lines without blocking the event loop |
| `aflow.interval(seconds)` | Emit increasing integers on a timer |
| `aflow.paginate(fetch_page)` | Fetch pages until the returned cursor is `None` |

Queue sources do not call `task_done()` and do not own the producer or queue.
Avoid `prefetch()` when `Queue.join()` or per-item acknowledgements must track
each `get()` exactly.

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
