# Pairs

`Pairs[K, V]` is a key/value view over a lazy flow of two-tuples. It provides
key-, value-, and pair-aware transforms plus per-key collection and aggregation.

`with_engine()` keeps the Pairs view while changing the underlying Flow policy.
`to_flow()` returns that Flow without copying values. Inspect the returned Flow
when it will be consumed as a Flow; pair terminals such as `to_dict()` may use a
pair-specific direct path and do not currently expose a separate `explain()` API.

::: fpstreams.Pairs
    options:
      members_order: source
      show_root_heading: true
      show_source: false
