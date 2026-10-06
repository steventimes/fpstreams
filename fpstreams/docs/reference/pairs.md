# Pairs

`Pairs[K, V]` is a key/value view over a lazy flow of two-tuples. It provides
key-, value-, and pair-aware transforms plus per-key collection and aggregation.

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
