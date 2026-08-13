# Market-informed async flow-control design

## Decision

fpstreams will add pull-based asynchronous flow-control and latest-inner switching without
changing its iterator model. The public additions are `AsyncFlow.delay`, `AsyncFlow.throttle`,
`AsyncFlow.spaceout`, and `AsyncFlow.switch_map`.

This direction combines the most relevant strengths found in active peer projects:

- streamable exposes bounded sliding-window throttling for sync/async iterables;
- aiostream exposes delay, output spacing, and switch-to-latest mapping;
- Streamz demonstrates the value of rate control and backpressure in continuous pipelines;
- RxPY validates switch-to-latest and recovery as important event-stream primitives;
- Polars reinforces explicit lazy plans and explainable execution, which fpstreams already uses.

Push-based branching graphs, scheduler abstractions, implicit caching, and a general optimizer are
not part of this tranche. They would change the ownership and consumption model rather than extend
it.

## Public API

`delay(seconds)` waits before requesting the first upstream item on each evaluation. It preserves
all values and their order.

`throttle(max_count, per=seconds)` permits at most `max_count` emissions in a monotonic sliding
window. It may emit an initial burst up to the limit, then waits before later emissions. It keeps
only emission timestamps, so memory is bounded by `max_count`.

`spaceout(seconds)` is the one-item throttle convenience. The first item is immediate; later items
are separated by at least `seconds` according to the event loop's monotonic clock.

`switch_map(function)` maps each outer item to a synchronous or asynchronous iterable, accepting an
awaitable mapper result. A new outer item cancels the pending mapper/pull for the previous item,
closes the previous inner iterator, and becomes the only active inner. Once the outer source ends,
the latest inner is allowed to finish.

All methods remain lazy and return `AsyncFlow`. Counts must support `operator.index`; durations must
be positive; mapper validation happens before any source is opened.

## Execution and ownership

New immutable nodes live in `planning/async_.py`. Timing and switching mechanics live in
`execution/async_concurrency.py`; the exhaustive dispatcher in `execution/async_ops.py` selects
them. No new per-item plan branching is added to the orchestration entry point.

Throttle keeps a deque of at most `max_count` timestamps and uses `asyncio.get_running_loop().time`
to avoid wall-clock changes. Delay and throttle never prefetch more than the item currently being
processed.

Switch-map owns at most one outer pull, one mapper task, one inner iterator, and one inner pull.
When outer and inner completions race, a real new outer value wins and the stale inner result is
discarded. Normal outer exhaustion does not discard the latest inner.

Every cancellation is awaited. Every owned iterator is closed through the exception-aware cleanup
helper. Cleanup failures become notes on an active pipeline error and otherwise surface after all
close attempts.

## Documentation and compatibility

Each public method receives an English Google-style source docstring with precise timing,
arguments, return type, validation errors, and cancellation behavior. MkDocs renders those same
docstrings, so VS Code hover help and the API site remain synchronized. No function-by-function
material is added to the README.

Existing imports, signatures, timing operators, aliases, and operation semantics remain unchanged.
No dependency is added.

## Verification

Tests will cover validation before source opening, delay-before-first-pull, sliding-window bursts,
spaceout separation, latest-inner replacement, awaitable mappers, race behavior, early termination,
mapper failures, cancellation, source/inner cleanup, dispatcher exhaustiveness, inline docstrings,
and strict static checks. Full Python, Rust, MkDocs, and wheel-install verification follows.
