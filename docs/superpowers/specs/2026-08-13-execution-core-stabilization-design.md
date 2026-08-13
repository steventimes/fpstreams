# Execution core stabilization design

## Decision

The next v2 tranche prioritizes stability and maintainability. Public imports, method names,
signatures, exceptions, laziness, encounter order, short-circuiting, and cleanup behavior remain
unchanged. Performance work and new public features follow only after the execution core is easier
to reason about and test.

## Scope

This tranche restructures the pure-Python synchronous and asynchronous execution engines. It does
not change planning operation data classes or the public `Flow`, `AsyncFlow`, `Rows`, or `Pairs`
interfaces.

The synchronous engine will separate plan traversal from single-operation dispatch. Adjacent
`map`/`filter`/`tap` fusion and adjacent gatherer composition remain explicit orchestration rules.
Individual operations continue to return lazy iterator layers.

The asynchronous engine will use the same conceptual boundary: orchestration identifies fusable
runs, then a focused dispatcher builds the next async iterator layer. Timing and concurrent
operators remain in `execution/async_concurrency.py`; the core dispatcher only selects them.

## Module boundaries

- `execution/sync.py` owns plan traversal, fusion, iterator ownership, and synchronous dispatch.
- Focused synchronous operation helpers remain private and may move to a private sibling module
  when that makes the orchestration file materially smaller.
- `execution/async_.py` owns async traversal, fusion, and async dispatch.
- `execution/async_concurrency.py` continues to own task groups, cancellation, timeouts, debounce,
  merge, and concurrent mapping.
- `planning/` remains a declarative operation model and must not import execution code.

No registry based on user-visible strings will be introduced. Dispatch remains typed and exhaustive
so a new operation cannot silently execute as a no-op.

## Data and resource flow

A plan opens its source once per evaluation. Each operation wraps the current iterator without
materializing it unless that operation is already documented as materializing or bounded-buffering.
The engine records every closeable iterator it owns and closes them in reverse construction order.
Async layers close upstream iterators in `finally` blocks and cancellation propagates through the
same ownership chain.

Short-circuiting operations must not request one extra source item. Fused stages must preserve the
same ordering of mapping, side effects, and predicates as their unfused equivalents.

## Errors

Validation remains at public API construction boundaries. Runtime callable errors propagate
unchanged. Buffer limits continue to raise `BufferLimitError`; forced native incompatibility
continues to raise `NativeUnsupportedError`. Cleanup errors must not hide an exception already
raised by the pipeline.

The dispatcher will fail explicitly for an unknown operation instead of returning the input
iterator unchanged. This is an internal invariant and does not add a public exception type.

## Testing

Tests will characterize behavior before structural changes:

- every synchronous and asynchronous operation is recognized by its dispatcher;
- fused and deliberately unfused equivalent pipelines return the same values and side effects;
- early termination closes sources and does not pull an unnecessary tail;
- callable failures, buffer failures, cancellation, and timeout paths close owned resources;
- Python, hybrid, and native materializing paths retain existing parity.

Tests remain grouped by domain. A small execution-contract test module may be extracted only if it
reduces the size and mixed responsibilities of the current execution test file.

## Delivery order

1. Add missing execution-contract tests and an exhaustive operation-recognition assertion.
2. Extract synchronous dispatch while preserving existing helpers and fusion rules.
3. Extract asynchronous dispatch while preserving concurrency helper ownership.
4. Remove unreachable branches or duplicate helpers proven unnecessary by tests.
5. Run Python tests, Ruff, strict mypy, Rust tests/fmt/clippy, strict MkDocs, and wheel-install smoke.
6. Re-audit file and function complexity before choosing the tabular join/SQLite tranche.

## Out of scope

This tranche does not add public methods, native kernels, execution metrics, streaming joins, new
adapters, or deprecations. It does not rename compatibility facades or change v1 aliases.

## Success criteria

- The public API and all current behavior tests remain unchanged.
- The main synchronous and asynchronous execution functions become short orchestration routines.
- Operation dispatch is explicit and exhaustively tested.
- Source cleanup and short-circuit semantics have direct regression tests.
- The complete repository verification suite and isolated wheel smoke test pass.
