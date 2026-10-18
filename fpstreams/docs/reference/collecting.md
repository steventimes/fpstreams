# Collecting and aggregation

Collectors reduce a flow into a result. `agg` supplies factories for common
named and grouped aggregations; each result is also a `Collector`.

In the development version, named `aggregate()` and `Pairs.aggregate_values()`
accept any `Collector`. This includes compositions made with
`Collectors.filtering`, `mapping`, `teeing`, and `collecting_and_then`. The same
collector can run over a Flow, an AsyncFlow, or each row group, including sorted
and spill groups. Every group receives fresh state from its initializer.

`aggregate()` requires named collectors. For a single unnamed result, use
`collect(collector)`. Collector callbacks keep their usual synchronous contract
when used with AsyncFlow. Composing a collector does not grant native execution
or reducer laws; those still require their existing proofs and declarations.
See the [weighted collector recipe](../user-guide/analytics.md#compose-a-weighted-collector-unreleased).

In the development version, editor hints and type checkers retain the finished
result type of a single streaming collector for both Flow and AsyncFlow.
Named collector calls return `dict[str, Any]`. Calls that forward a possibly
`None` collector together with named arguments keep the general result union.

AsyncFlow uses a streaming Collector's synchronous state-machine contract.
An awaitable returned by its finisher remains a result value for the caller
to await. For a non-Collector callable, AsyncFlow awaits an awaitable result
before returning it. These runtime rules are unchanged by the type declarations.

## Collector

::: fpstreams.Collector
    options:
      members_order: source
      show_root_heading: true
      show_source: false

## Built-in collectors

In the development version, `head(count)` and `tail(count)` accept integers and
objects implementing `__index__`. Each factory normalizes the count once, so
the same collector can be reused without consulting the count provider again.
Negative integer counts raise `ValueError`; floats, NaN, infinity, and other non-index
values raise `TypeError` at construction. A zero head still pulls no items,
while a zero tail consumes the source and returns an empty list.

The development version also keeps completed `first()` and `head(count)` states
stable when a grouping adapter continues offering input. For example,
`Collectors.grouping_by(key, Collectors.first())` retains the first item per key,
and a downstream `head(0)` retains an empty list for each encountered group.
`grouping_by` and `partitioning_by` still read the full source and call downstream
steps for every item without consulting its `done` hook. These built-in steps
keep their completed result unchanged. Custom downstream callbacks continue
receiving every input item.

In the development version, `Collectors.teeing()` provides a state combiner
when both downstream collectors declare one at construction. It combines the
left states, then the right states. Downstream finishers and the result merger
run only when the combined state is finished. Missing combiners leave
`combine=None`; a live combiner removed later raises `ValueError` when needed.

An available combiner does not establish associativity or authorize parallel
execution. `explain_reduction()` reports `combine_declared` separately from
reducer laws. User combiners can mutate their input states; mutations cannot be
rolled back if a later callback fails.

For custom reducers, `fpstreams.testing.check_reducer_laws()` samples identity,
associativity, and partition equivalence. It closes the input before replaying
examples and initializes independent states for the two associativity trees.
Its `equivalent` callback compares raw states for associativity and finished
outputs for the other checks. Passing sampled examples does not prove the laws
for every input or permit reordering arbitrary callbacks.
Partition layouts also accept integer-index objects; `checked_partitions`
records their normalized Python integer values.

::: fpstreams.Collectors
    options:
      members_order: source
      show_root_heading: true
      show_source: false

## Aggregators

`agg` creates named, single-pass aggregations:

| Call | Result |
| --- | --- |
| `agg.count()` | Number of input items |
| `agg.count_where(predicate)` | Number of matching items |
| `agg.any(predicate)` / `agg.all(predicate)` | Boolean checks |
| `agg.sum(selector)` / `agg.mean(selector)` | Sum or arithmetic mean |
| `agg.variance(selector, ddof=1)` | Variance |
| `agg.std(selector, ddof=1)` | Standard deviation |
| `agg.count_distinct(selector)` | Number of distinct values |
| `agg.min(selector)` / `agg.max(selector)` | Smallest or largest value |
| `agg.min_by(selector)` / `agg.max_by(selector)` | Whole item with the smallest or largest key (unreleased) |
| `agg.top(count, key=...)` / `agg.bottom(count, key=...)` | Up to `count` whole items in descending or ascending key order (unreleased) |
| `agg.first(selector)` / `agg.last(selector)` | Boundary values |
| `agg.collect(selector, into=list)` | Values collected with a constructor |

Omit `selector` to aggregate the items themselves. A string selector reads a
record field; expression and callable selectors are also accepted.

`min_by` and `max_by` require a selector and return the original winning item.
They select a key once per item, keep the first item on ties, and return `None`
for empty input. Each state retains one key and one item. Keys use Python's
`<` or `>` comparison, including its errors and NaN behavior. Apply a key
normalization yourself when missing or incomparable values need a policy.

These factories work with `Flow.aggregate`, `AsyncFlow.aggregate`, and global,
grouped, sorted, or spilled row aggregation. Spill restores serialized records
under its existing contract. The factories do not declare merge laws for
arbitrary Python keys. See the [latest-record recipe](../user-guide/analytics.md#select-records-by-version-unreleased).

`top` and `bottom` return lists, retain at most `count` items per state, and
keep input order for equal keys. The key runs once per item; payloads are never
compared. Count must be a non-negative integer. Empty input and zero count
return `[]`; zero count skips key calls and does not pull an independent input.
Grouped aggregation still reads the source to discover groups. Use consistently
ordered keys: Python heap and sorting comparisons determine the result and
propagate comparison errors. These aggregators declare no merge laws. See the
[per-group ranking recipe](../user-guide/analytics.md#rank-records-within-each-group-unreleased).

::: fpstreams.Aggregator
    options:
      members_order: source
      show_root_heading: true
      show_source: false
