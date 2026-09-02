# Errors and runtime values

The root-level pipeline errors listed below inherit from `FlowError`. Exceptions
raised by user callbacks, Python protocols, parsers, optional libraries, the
filesystem, and database drivers keep their original type unless an adapter
documents a more specific boundary. Some public subpackages define additional
errors for their own storage or runtime contracts.

## Error hierarchy

| Error | Raised when |
| --- | --- |
| `FlowError` | Base class for fpstreams operation errors |
| `FlowConsumedError` | A one-shot source is evaluated after it has already been consumed |
| `EmptyFlowError` | An element-requiring terminal such as `first()` receives no item |
| `SelectionError` | A field, index, path, or expression cannot select a value |
| `DuplicateKeyError` | An output field, JSON object key, pivot cell, or join suffix would be ambiguous |
| `NativeUnsupportedError` | A plan forced to `native` cannot be represented by the native engine |
| `BufferLimitError` | A configured record, buffer, partition, fan-out, or output budget is exceeded |

Catch the narrow error that your application can recover from. Catching
`FlowError` is useful at a pipeline boundary, but it does not include arbitrary
callback or third-party-library exceptions.

```python
from fpstreams import EmptyFlowError, flow

try:
    value = flow([]).first()
except EmptyFlowError:
    value = None
```

When an optional adapter is missing, install the extra named in its `ImportError`
message. A missing optional integration is not converted to `FlowError`.

## SpillLimits

`SpillLimits` is an immutable set of hard relational budgets. It protects the
application from skew, underestimated rows, explosive many-to-many joins, and
unbounded repartition attempts.

::: fpstreams.SpillLimits
    options:
      show_root_heading: true
      members: false

| Field | Default | Meaning |
| --- | ---: | --- |
| `max_partition_rows` | 100,000 | Maximum rows retained in one partition |
| `max_partition_bytes` | 64 MiB | Maximum estimated bytes in one partition |
| `max_matches_per_key` | 100,000 | Maximum join fan-out for one key |
| `max_output_rows` | 1,000,000 | Maximum total records emitted by the bounded operation |
| `max_repartition_depth` | 3 | Maximum recursive repartition attempts |

Limits must be positive integers except repartition depth, which may be zero.
Exceeding a budget raises before the operation silently becomes unbounded.

## SummaryStatistics

`flow(values).collect(Collectors.summarizing())` returns one mutable statistics
value containing count, sum, minimum, maximum, and a derived average.

::: fpstreams.SummaryStatistics
    options:
      show_root_heading: true
      members: true

Empty input is normalized to count and sum zero, minimum and maximum `0.0`, and
average `0.0`. `Flow.describe()` is a separate convenience terminal that returns
a dictionary and may include sample standard deviation for numeric input.

## Reducer laws

Parallel or tree reduction can change grouping. fpstreams does not infer that an
arbitrary function is associative, commutative, or has a valid identity. Reducer
metadata makes those requirements explicit.

::: fpstreams.Reducer
    options:
      show_root_heading: true
      members: true

::: fpstreams.ReducerLaws
    options:
      show_root_heading: true
      members: true

::: fpstreams.ReductionExplanation
    options:
      show_root_heading: true
      members: true

`ReducerLawError` reports a missing or contradicted law. `LawProvenance` records
whether a law was declared, derived, or verified according to the reducer API.

## Root error module

::: fpstreams.errors
    options:
      show_root_heading: false
      members: true
