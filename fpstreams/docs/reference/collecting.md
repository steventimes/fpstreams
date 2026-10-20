# Collecting and aggregation

Collectors reduce a flow into a result. `agg` supplies factories for common
named and grouped aggregations; each result is also a `Collector`.

Since 2.4.0, named `aggregate()` and `Pairs.aggregate_values()`
accept any `Collector`. This includes compositions made with
`Collectors.filtering`, `mapping`, `teeing`, and `collecting_and_then`. The same
collector can run over a Flow, an AsyncFlow, or each row group, including sorted
and spill groups. Every group receives fresh state from its initializer.

`aggregate()` requires named collectors. For a single unnamed result, use
`collect(collector)`. Collector callbacks keep their usual synchronous contract
when used with AsyncFlow. Composing a collector does not grant native execution
or reducer laws; those still require their existing proofs and declarations.
See the [weighted collector recipe](../user-guide/analytics.md#compose-a-weighted-collector-unreleased).

Since 2.4.0, editor hints and type checkers retain the finished
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

### Shared and approximate statistics (Unreleased)

`Collectors.statistics("value", ddof=1)` returns `count`, `mean`, `variance`,
and `std` in one dictionary. It selects and converts each nonmissing value once.
Selected `None` values are skipped; an undefined mean, variance, or standard
deviation is `None`. This explicit collector shares its statistical state without
changing the callback behavior of existing collectors. It has no state combiner.

These collectors are not included in PyPI 2.4.1. From a source checkout, install
the `sketch` extra to use the optional Apache DataSketches C++ kernels:

```python
from fpstreams import Collectors, flow

result = flow([1, 2, 3, None, 4]).collect(
    distribution=Collectors.approx_quantiles([0, 0.5, 1], k=200),
    cardinality=Collectors.approx_count_distinct(lg_k=12),
)
```

`approx_quantiles` preserves rank order in its result tuple. It skips `None`
and converts real numeric inputs to finite double-precision values; conversion
can round large integers. Empty input returns one `None` per requested rank.
Complex values, including NumPy complex scalars with a zero imaginary part,
are rejected before conversion. Rank parameters use the same real-number check.
The KLL parameter `k` ranges from 8 to 65535. Estimates are stochastic and have
rank error, which does not bound error in the returned numeric value. The
[DataSketches KLL documentation](https://apache.github.io/datasketches-python/main/quantiles/kll.html)
describes its empirical error model. Do not infer a fixed byte limit from `k`.

`approx_count_distinct` returns a floating-point HLL estimate, including `0.0`
for empty input. Its `lg_k` ranges from 7 to 21. It accepts exact builtin bool,
int, finite float, str, and bytes values; `None` is skipped. Equal numeric values
such as `True`, `1`, and `1.0` share an identity. Text and bytes have different
identities, including empty values. Other objects and nonfinite floats are
rejected. The adapter encodes scalars with type framing and exact integer ratios,
then applies a versioned BLAKE2b digest before native hashing. This adds work per
value and does not provide general Python object equality semantics.

Both factories import the optional dependency only when called. Their states
support compatible partial merges: the left state is updated and the right is
preserved. Algorithm and precision must match, and states must be independent.
State serialization is not a public persistence contract. Merge order can affect
estimates, so byte equality and exact reducer laws are not promised. Existing
grouping creates fresh state per group; many groups can still consume substantial
memory. These sketches do not certify uniqueness or replace exact collectors.

DataSketches 5.2.0 publishes wheels for CPython 3.11 through 3.13, but no 3.14
wheel. Installing the sketch extra on 3.14 may require a source build and its
toolchain. The base library has no DataSketches dependency.

### Collector composition and completion (2.4.0)

`head(count)` and `tail(count)` accept integers and
objects implementing `__index__`. Each factory normalizes the count once, so
the same collector can be reused without consulting the count provider again.
Negative integer counts raise `ValueError`; floats, NaN, infinity, and other non-index
values raise `TypeError` at construction. A zero head still pulls no items,
while a zero tail consumes the source and returns an empty list.

Completed `first()` and `head(count)` states remain
stable when a grouping adapter continues offering input. For example,
`Collectors.grouping_by(key, Collectors.first())` retains the first item per key,
and a downstream `head(0)` retains an empty list for each encountered group.
`grouping_by` and `partitioning_by` still read the full source and call downstream
steps for every item without consulting its `done` hook. These built-in steps
keep their completed result unchanged. Custom downstream callbacks continue
receiving every input item.

`Collectors.teeing()` provides a state combiner
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
| `agg.min_by(selector)` / `agg.max_by(selector)` | Whole item with the smallest or largest key (2.4.0) |
| `agg.top(count, key=...)` / `agg.bottom(count, key=...)` | Up to `count` whole items in descending or ascending key order (2.4.0) |
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
