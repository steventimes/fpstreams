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
See the [weighted collector recipe](../user-guide/analytics.md#compose-a-weighted-collector-250).

`grouping_by` groups all matching keys, even when they are not adjacent;
`group_runs` instead emits consecutive runs. The grouping classifier receives
each item, including `None`, and `None` itself can also be a group key. A group's
initializer runs when its key first appears; a state of `None` is a valid state,
not a request to initialize it again. `partitioning_by` initializes and finishes
both Boolean partitions, including for empty input.

`reduce_by` also initializes once per encountered key, even when the
initializer and subsequent steps return `None`. If a callback needs to tell
the first item apart from a legitimate `None` state, use a distinct marker:

```python
from fpstreams import flow

items = [("a", None), ("a", 7)]
initialized = []

def initial_none():
    initialized.append(True)
    return None

assert flow(items).reduce_by(
    lambda pair: pair[0], lambda state, pair: None, initializer=initial_none
) == {"a": None}
assert initialized == [True]

unseen = object()
assert flow(items).reduce_by(
    lambda pair: pair[0],
    lambda state, pair: pair[1] if state is unseen else state,
    initializer=lambda: unseen,
) == {"a": None}
```

The same per-key rule applies to `await aflow(items).reduce_by(...)`; its
selector, initializer, and step can also return awaitables.

`grouping_by` and `partitioning_by` offer every selected item to their downstream
collector even if its `done` predicate is true. `teeing` stops stepping a finished
downstream but stops the source only when both downstream collectors are done.
Choose downstream callbacks that support the lifecycle of the chosen adapter.
For AsyncFlow, collector callbacks remain synchronous: a classifier's returned
Future is a key, whereas `AsyncFlow.reduce_by` resolves an awaitable selector result.

`chunk` and `group_runs` return tuples containing the original item references;
they do not deep-copy mutable items. Arrow batch slices may share the source value
buffer. Retaining a slice can therefore retain that buffer, and a row-count limit
alone is not a byte-memory limit.

`Collectors.frequencies()` (2.5.0) returns a dictionary counting each
hashable input, in first-encounter order. It uses one entry per distinct value
and can be combined with `mapping` for grouped field counts:

```python
from fpstreams import Collectors, flow

assert flow(["left", "right", "left"]).collect(Collectors.frequencies()) == {
    "left": 2, "right": 1
}
```

For example, count positions separately for each team:

```python
from fpstreams import Collectors, rows

positions = Collectors.mapping("position", Collectors.frequencies())
result = rows([
    {"team": "a", "position": "left"},
    {"team": "a", "position": "right"},
    {"team": "a", "position": "left"},
    {"team": "b", "position": "right"},
    {"team": "b", "position": "left"},
]).group_by("team").aggregate(counts=positions).to_list()

assert [group["team"] for group in result] == ["a", "b"]
assert list(result[0]["counts"].items()) == [("left", 2), ("right", 1)]
assert list(result[1]["counts"].items()) == [("right", 1), ("left", 1)]
```

`Collectors.mode()` (2.5.0) returns the most frequent hashable value; ties
go to the value first encountered. Empty input returns `None`, which is also a
valid input value. The collector retains one entry per distinct value and does
not provide a partial-state combiner. Compose it with `mapping` and `filtering`
to select a field or exclude missing values:

```python
from fpstreams import Collectors, rows

positions = Collectors.mapping("position", Collectors.mode())
result = rows([
    {"team": "a", "position": "left"},
    {"team": "a", "position": "right"},
    {"team": "a", "position": "right"},
]).group_by("team").aggregate(position=positions).to_list()
assert result == [{"team": "a", "position": "right"}]
```

Since 2.4.0, editor hints and type checkers retain the finished
result type of a single streaming collector for both Flow and AsyncFlow.
Named collector calls return `dict[str, Any]`. Calls that forward a possibly
`None` collector together with named arguments keep the general result union.

AsyncFlow uses a streaming Collector's synchronous state-machine contract.
An awaitable returned by its finisher remains a result value for the caller
to await. For a non-Collector callable, AsyncFlow awaits an awaitable result
before returning it. These runtime rules are unchanged by the type declarations.

### Collector type inference (2.5.0)

Container factories infer their element type from the consuming pipeline or an
explicit annotation. `Collectors[int]` remains available when constructing a
collector separately:

```python
from typing import assert_type

from fpstreams import Collector, Collectors, flow

assert_type(flow([1, 2]).collect(Collectors.to_list()), list[int])
assert_type(flow([1, 2]).collect(Collectors.to_tuple()), tuple[int, ...])
integers: Collector[int, list[int], list[int]] = Collectors[int].to_list()
```

Unannotated standalone factories and some inline `Collectors[Any]` calls remain
checker dependent. Use a concrete `Collectors[T]` namespace or store the result
with an explicit `Collector` annotation before consuming it.

The same result inference applies to `AsyncFlow.collect()` and
`Pairs.collect_values()`. A collector consumes its input type; its mutable state
and result types remain invariant. In particular, an integer list collector does
not become a string list collector. An awaitable finisher remains a value in
both the synchronous and asynchronous Collector contracts described above.

A separately annotated `Collector[Any, list[Any], list[Any]]` retains its
`list[Any]` result in both Flow and AsyncFlow. Forwarding a possibly `None`
collector still retains the result union with `dict[str, Any]` for named
collection.

For an async Collector with an awaitable finisher, use a concrete input type;
`object` is suitable if its step accepts arbitrary objects. Combining `Any`
with the callable overloads can still erase this result type. The runtime
continues to preserve the finisher's awaitable as a value.

Source installations use `typing-extensions>=4.12` on Python 3.11 and 3.12 for
type parameter defaults; Python 3.13 and newer use the standard library.
This typing change is not included in the published 2.4.1 release.

## Collector

::: fpstreams.Collector
    options:
      members_order: source
      show_root_heading: true
      show_source: false

## Built-in collectors

### Shared and approximate statistics (2.5.0)

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
or 3.15 wheel. Installing the sketch extra on these versions requires a source build and its
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
Identity is sampled on both sides using separate states. Each sampled partition
stops stepping when `done` is true. This differs from `run_partitioned_reducer`,
which consumes and steps every input. A passing partition sample therefore does
not establish that this execution function can run the reducer successfully.
`associativity_checked` is false for fewer
than three inputs; `partition_equivalence_checked` is false without layouts.
The corresponding boolean results remain true when no example was checked for
backward compatibility, and are not evidence that the property holds. The new
checked flags do not participate in `ReducerLawReport` equality, preserving the
previous comparison contract; inspect the flags explicitly when coverage matters.
Its `equivalent` callback compares raw states for associativity and finished
outputs for the other checks, so a finisher that hides state differences can
also hide an identity failure. Passing sampled examples does not prove the laws
for every input or permit reordering arbitrary callbacks.
For example, floating-point addition may pass one associativity sample but
produce a different result under another partition layout due to rounding.
Partition layouts also accept integer-index objects; `checked_partitions`
records their normalized Python integer values.

`Flow.mean()` returns `None` on an empty flow and uses compensated floating-point
summation. `Collectors.averaging()` returns `0.0` on empty input and uses ordinary
addition followed by division; rounding and result types depend on the operands.
In 2.5.0, its result annotation is `object`: nonempty Decimal and Fraction inputs preserve
those division result types, complex inputs return complex, and custom division
can return any type. This annotation replaces the inaccurate `float` declaration;
the runtime arithmetic and empty `0.0` are unchanged. Existing typed callers must
narrow that result or migrate to the explicit interface below.
For example, `[1e16, 1.0, -1e16]` yields `1/3` from `mean()` but `0.0` from
`averaging()` in the Python paths. These operations do not automatically skip
`None`. A collector's merge order can also affect floating-point rounding.

### Explicit averaging operations (2.5.0)

`Collectors.averaging_with()` requires a callable selector and explicit `zero`,
`add`, `divide`, and `empty` callbacks. The selector determines the input and
selected-value types; accumulator and result types can differ. No result is
converted to float. `zero()` creates fresh accumulator state per collection,
`divide(total, count)` runs only for nonempty input, and `empty()` supplies the
empty result. Both finish callbacks must return the declared result type,
which can include `None` or another explicit alternative.

```python
from decimal import Decimal
from fpstreams import Collectors, flow


def value(item: Decimal) -> Decimal:
    return item


def add(total: Decimal, item: Decimal) -> Decimal:
    return total + item


def divide(total: Decimal, count: int) -> Decimal:
    return total / count


average = Collectors.averaging_with(
    value, zero=Decimal, add=add, divide=divide, empty=Decimal,
)
assert average([Decimal("1.1"), Decimal("2.2")]) == Decimal("1.65")
assert average([]) == Decimal(0)
assert flow([Decimal("2")]).collect(average) == Decimal("2")
```

For records, supply an annotated selector such as
`def value(row: dict[str, Decimal]) -> Decimal: return row["amount"]`.
String and integer selectors remain available on legacy `averaging()`; the
explicit interface uses callables so its selected-value type is known.
A custom division result can also have a different type from its accumulator:

```python
class Number:
    def __init__(self, value: int = 0) -> None:
        self.value = value

    def __add__(self, other: "Number") -> "Number":
        return Number(self.value + other.value)

    def __truediv__(self, count: int) -> str:
        return f"{self.value}/{count}"


def number(item: Number) -> Number:
    return item


def add_number(total: Number, item: Number) -> Number:
    return total + item


def divide_number(total: Number, count: int) -> str:
    return total / count


text_average = Collectors.averaging_with(
    number, zero=Number, add=add_number, divide=divide_number, empty=str,
)
assert text_average([Number(3), Number(4)]) == "7/2"
assert text_average([]) == ""
```

The resulting Collector supports direct calls, `Flow.collect`, `AsyncFlow.collect`,
and existing Collector composition. `collecting_and_then` preserves its result
type; the existing `grouping_by` annotation still includes its default list
alternative in the result values. Partial-state merging is disabled unless
`combine(total_left, total_right)` is supplied; counts are then added separately.
The caller owns the callbacks' arithmetic, errors, precision, and merge laws.
This interface provides an explicit typed migration; it does not infer the
legacy factory's arbitrary operator results.

In the Python `summarizing()` path, an empty summary reports zero for its extrema.
A summary containing only NaN has count 1 and NaN sum, while its minimum and
maximum remain positive and negative infinity. With `[NaN, 2.0]`, its extrema
are both `2.0`, while `Flow.min()` and `Flow.max()` return NaN for that order.
With `[2.0, NaN]`, the summary extrema and both Flow methods return `2.0`.
These are implementation-specific examples, not a guarantee for every backend
or numeric type.
For the Python paths tested with `[float('inf'), float('-inf')]`, `Flow.mean()`
and `Collectors.averaging()` return NaN; Python's `statistics.fmean()` raises
`ValueError` for the same input. Select the contract your application requires.

In a tested Arrow int64 row aggregation, an empty sum is `0`, a `None` value
raises `TypeError`, and `[2**63 - 1, 1]` sums to the Python integer `2**63`.
For the same Arrow arrays, `pyarrow.compute.sum` returns null for empty input,
skips nulls, and wraps that integer sum to `-2**63`. A tested NumPy int64
group sum also returns `2**63`, including on the `numpy_direct` route, while
`numpy.sum` wraps the fixed-width values. These examples do not guarantee the
same behavior for every source, dtype, aggregation, or backend.

For the tested float64 Rows minimum, `[NaN, 1.0]` returns NaN and `[1.0, NaN]`
returns `1.0`; equal signed zeros retain the first input's sign. In the same
samples, `numpy.min` returns NaN for either NaN order and the last zero's sign.
Both `python` and `auto` reported `planned:python` for those examples; they do
not demonstrate a NumPy-native extrema kernel.

::: fpstreams.Collectors
    options:
      members_order: source
      show_root_heading: true
      show_source: false
      show_overloads: false
      show_bases: false

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

In the 2.5.0 Python path, `agg.count_distinct` checks equality across
hashable and unhashable values. It retains distinct values, may call equality
across categories, and can take O(H × U) cross-category comparisons for H
hashable and U unhashable values, with O(H + U) retained references. The Python
path compares previously accepted values to the incoming value using list
membership semantics. A hash `TypeError` takes the unhashable path; other hash
errors and comparison errors propagate.

`min_by` and `max_by` require a selector and return the original winning item.
They select a key once per item, keep the first item on ties, and return `None`
for empty input. Each state retains one key and one item. Keys use Python's
`<` or `>` comparison, including its errors and NaN behavior. Apply a key
normalization yourself when missing or incomparable values need a policy.

These factories work with `Flow.aggregate`, `AsyncFlow.aggregate`, and global,
grouped, sorted, or spilled row aggregation. Spill restores serialized records
under its existing contract. The factories do not declare merge laws for
arbitrary Python keys. See the [latest-record recipe](../user-guide/analytics.md#select-records-by-version-250).

`top` and `bottom` return lists, retain at most `count` items per state, and
keep input order for equal keys. The key runs once per item; payloads are never
compared. Count must be a non-negative integer. Empty input and zero count
return `[]`; zero count skips key calls and does not pull an independent input.
Grouped aggregation still reads the source to discover groups. Use consistently
ordered keys: Python heap and sorting comparisons determine the result and
propagate comparison errors. These aggregators declare no merge laws. See the
[per-group ranking recipe](../user-guide/analytics.md#rank-records-within-each-group-250).

::: fpstreams.Aggregator
    options:
      members_order: source
      show_root_heading: true
      show_source: false
