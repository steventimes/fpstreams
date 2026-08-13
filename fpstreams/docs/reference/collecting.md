# Collecting and aggregation

Collectors reduce a flow into a result. Aggregators are composable state
machines used by named and grouped aggregation.

## Collector

::: fpstreams.Collector
    options:
      members_order: source
      show_root_heading: true
      show_source: false

## Built-in collectors

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
| `agg.first(selector)` / `agg.last(selector)` | Boundary values |
| `agg.collect(selector, into=list)` | Values collected with a constructor |

Omit `selector` to aggregate the items themselves. A string selector reads a
record field; expression and callable selectors are also accepted.

::: fpstreams.Aggregator
    options:
      members_order: source
      show_root_heading: true
      show_source: false
