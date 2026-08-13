# AsyncFlow

`AsyncFlow[T]` combines async iteration with bounded concurrent mapping, merging,
rate control, latest-request switching, timeouts, and buffer-by-time operations.

Use `aflow(source)` with an iterable or async iterable.

## Creating an async flow

| Call | Behavior |
| --- | --- |
| `aflow(source)` | Wrap a synchronous or asynchronous iterable |
| `aflow.defer(factory)` | Call the source factory for each execution |
| `aflow.from_file(path)` | Read text lines without blocking the event loop |
| `aflow.interval(seconds)` | Emit increasing integers on a timer |
| `aflow.paginate(fetch_page)` | Fetch pages until the returned cursor is `None` |

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

## Methods

::: fpstreams.AsyncFlow
    options:
      members_order: source
      show_root_heading: true
      show_source: false
