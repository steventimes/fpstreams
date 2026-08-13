# Rows join validation and extraction design

## Decision

Move join-specific planning and execution out of `tabular/rows.py` into `tabular/join.py`, and add
`validate="m:m" | "1:1" | "1:m" | "m:1"` to `Rows.join`.

The validation vocabulary follows established dataframe practice: `1:m` requires unique left keys,
`m:1` requires unique right keys, `1:1` requires both, and `m:m` keeps current many-to-many
behavior. The default is `m:m`, so existing pipelines are unchanged.

## Boundaries

`rows.py` retains the public method and its Google-style docstring. `join.py` owns selector
normalization, shared-column planning, in-memory algorithms, output merging, and iterator cleanup.
`spill.py` retains partition files and stable output merging, with one new validation argument.
`records.py` keeps general record conversion; join-only helpers move to `join.py`.

No dataframe dependency is introduced. Existing inner, left, right, full, semi, anti, composite-key,
suffix, stable-order, lazy-left, and bounded-spill behavior remains intact.

## Validation semantics

Invalid validation modes fail while the plan is built, before either source opens. Right-key
uniqueness is checked while constructing the existing right-side index. Left-key uniqueness is
checked as the left source streams for inner, left, semi, and anti joins; right/full joins already
materialize both sides and validate before emitting.

Spilled joins validate inside each hash partition. Equal keys always map to the same partition, so
per-partition duplicate detection is globally correct without an extra unbounded global set. A
validation failure occurs before spilled output is merged to the caller, and the temporary directory
is removed normally.

Duplicate keys raise `ValueError` naming the validation contract, side, and key. Unhashable keys
retain the existing `TypeError` behavior.

## Verification

Tests cover valid and invalid modes, all cardinality contracts, semi/anti joins, partitioned parity,
cleanup, stable order, lazy left-side streaming, public hover docs, and unchanged default behavior.
Full Python, Rust, MkDocs, wheel, and Git guardrail checks follow.
