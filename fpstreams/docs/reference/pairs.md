# Pairs

`Pairs[K, V]` is a key/value view over a lazy flow of two-tuples. It provides
key-, value-, and pair-aware transforms plus per-key collection and aggregation.

In the 2.5.0 Python uniqueness path, `unique_keys` compares hashable and
unhashable keys across categories, keeping the first accepted equal key and its
value. Cross-category comparisons may run user equality callbacks and take
O(H × U) time for H hashable and U unhashable keys. The operation retains
distinct keys; `to_dict` still requires every emitted key to be hashable.

In the 2.5.0 Python path, `collect_values` and `aggregate_values` propagate
errors raised by a key's custom hash or equality operation unchanged. An
unhashable key raises `TypeError`.

For a streaming collector, `collect_values` creates one state per key and skips
further `step` calls for that key after its `done` predicate is true. Other keys
can still receive values. A state of `None` remains a state and does not cause
another initialization. This differs from `Collectors.grouping_by`, which offers
every item to its downstream collector regardless of `done`.

`with_engine()` keeps the Pairs view while changing the underlying Flow policy.
`to_flow()` returns that Flow without copying values. Inspect the returned Flow
when it will be consumed as a Flow; pair terminals such as `to_dict()` may use a
pair-specific direct path and do not expose a separate `explain()` API.

Use `run_with_report()` to execute `to_dict`, `group_values`, `collect_values`,
or `aggregate_values` and inspect the route. The terminal runs once; extra
arguments are forwarded to it. The result contains the terminal's value and an
immutable [execution report](../user-guide/execution-reports.md).
This reporting method was added in 2.2.0.

```python
from fpstreams import flow

result = flow([("red", 2), ("red", 3)]).pairs().run_with_report("group_values")
assert result.value == {"red": [2, 3]}
```

::: fpstreams.Pairs
    options:
      members_order: source
      show_root_heading: true
      show_source: false
