# Synchronous Execution Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Do not use subagents for this repository.

**Goal:** Make synchronous plan execution a short orchestration routine with explicit, exhaustively checked single-operation dispatch while preserving all public behavior.

**Architecture:** Move individual iterator transformations and their type dispatch from `execution/sync.py` into a private `execution/sync_ops.py` module. Keep plan traversal, stateless fusion, gatherer composition, iterator ownership, and reverse-order cleanup in `execution/sync.py`.

**Tech Stack:** Python 3.11+, pytest, Ruff, strict mypy, Maturin/PyO3, MkDocs.

## Global Constraints

- Do not change public imports, names, signatures, aliases, exceptions, or lazy evaluation behavior.
- Preserve encounter order, short-circuiting, fusion order, and reverse-order resource cleanup.
- Unknown internal operations must fail explicitly instead of silently passing through.
- Do not add dependencies.
- Do not commit, stage, push, publish, or deploy documentation.
- Use `.venv/bin/...` for Python tooling.
- Keep tests in the existing consolidated domain files.

---

## File structure

- Create `src/fpstreams/execution/sync_ops.py`: synchronous iterator transformations, the explicit supported-operation type tuple, and one-operation dispatch.
- Modify `src/fpstreams/execution/sync.py`: plan traversal, map/filter/tap fusion, gatherer composition, iterator ownership, and `execute()` only.
- Modify `tests/test_invariants.py`: dispatch exhaustiveness, unknown-operation failure, and fused side-effect ordering contracts.

The asynchronous executor is intentionally deferred to a separate plan after this independently testable refactor passes.

### Task 1: Characterize synchronous dispatch and fusion contracts

**Files:**
- Modify: `tests/test_invariants.py`
- Test: `tests/test_invariants.py`

**Interfaces:**
- Consumes: `Operation`, `MapOp`, and the existing public `flow()` pipeline.
- Produces: contract tests for `SUPPORTED_OPERATION_TYPES: tuple[type[object], ...]` and `apply_operation(iterator: Iterator[Any], operation: Operation) -> Iterator[Any]`.

- [ ] **Step 1: Add imports for the internal dispatch contract**

```python
from typing import cast, get_args

import pytest

from fpstreams import Stream, flow
from fpstreams.execution.sync_ops import SUPPORTED_OPERATION_TYPES, apply_operation
from fpstreams.planning.sync import MapOp, Operation
```

- [ ] **Step 2: Add the exhaustive and explicit-failure test**

```python
def test_sync_operation_dispatch_is_exhaustive_and_rejects_unknown_types() -> None:
    assert set(SUPPORTED_OPERATION_TYPES) == set(get_args(Operation))
    assert list(apply_operation(iter([1, 2]), MapOp(lambda value: value + 1))) == [2, 3]

    with pytest.raises(TypeError, match="unsupported synchronous operation: object"):
        apply_operation(iter(()), cast(Operation, object()))
```

- [ ] **Step 3: Add the fused ordering test**

```python
def test_sync_fusion_preserves_callable_and_side_effect_order() -> None:
    events: list[tuple[str, int]] = []

    def mapped(value: int) -> int:
        events.append(("map", value))
        return value + 10

    def tapped(value: int) -> None:
        events.append(("tap", value))

    def accepted(value: int) -> bool:
        events.append(("filter", value))
        return value % 2 == 1

    assert flow([1, 2, 3]).map(mapped).tap(tapped).filter(accepted).to_list() == [11, 13]
    assert events == [
        ("map", 1),
        ("tap", 11),
        ("filter", 11),
        ("map", 2),
        ("tap", 12),
        ("filter", 12),
        ("map", 3),
        ("tap", 13),
        ("filter", 13),
    ]
```

- [ ] **Step 4: Run the focused test and confirm the expected red state**

Run `.venv/bin/pytest tests/test_invariants.py -q`.

Expected: collection fails because `fpstreams.execution.sync_ops` does not exist. This proves the new contract is not already satisfied accidentally.

- [ ] **Step 5: Check the local diff without staging**

Run `git diff -- tests/test_invariants.py` and `git diff --cached --name-only`.

Expected: only the intended test changes appear, and the staged-file command prints nothing.

### Task 2: Extract synchronous operation implementations and dispatch

**Files:**
- Create: `src/fpstreams/execution/sync_ops.py`
- Modify: `src/fpstreams/execution/sync.py`
- Test: `tests/test_invariants.py`

**Interfaces:**
- Consumes: every class in `fpstreams.planning.sync.Operation` and `external_sort()`.
- Produces: `SUPPORTED_OPERATION_TYPES` and `apply_operation()` for `execution.sync.execute()`.

- [ ] **Step 1: Create the focused operation module and move helpers without behavioral edits**

Start the module with the synchronous iterator/concurrency imports currently used by the operation helpers, every operation class from `planning.sync`, `Downstream`, `BufferLimitError`, and `external_sort`.

Move these exact helper definitions from `sync.py` with their bodies unchanged:

```text
_map, _take_while_inclusive, _map_parallel, _tap, _filter, _flat_map,
_unique, _chunk, _window, _group_runs, _close_iterator, _zip,
_zip_longest, _intersperse, _concat, _scan, _scan_right, _cross,
_gather, _prepend, _append, _map_first, _map_last, _collapse
```

Rename `_close_iterator` to `close_iterator`; update only its calls inside the moved helpers. Leave `_fused`, `_remember_iterator`, and `execute` in `sync.py`.

- [ ] **Step 2: Declare the explicit operation coverage tuple**

```python
SUPPORTED_OPERATION_TYPES: tuple[type[object], ...] = (
    MapOp,
    ParallelMapOp,
    TapOp,
    FilterOp,
    FlatMapOp,
    TakeOp,
    DropOp,
    TakeWhileOp,
    TakeWhileInclusiveOp,
    DropWhileOp,
    UniqueOp,
    ChunkOp,
    WindowOp,
    GroupRunsOp,
    PairwiseOp,
    EnumerateOp,
    ZipOp,
    ZipLongestOp,
    IntersperseOp,
    ConcatOp,
    CrossOp,
    ScanOp,
    ScanRightOp,
    SortOp,
    GatherOp,
    PrependOp,
    AppendOp,
    MapFirstOp,
    MapLastOp,
    CollapseOp,
)
```

- [ ] **Step 3: Add explicit one-operation dispatch**

Implement `apply_operation(iterator: Iterator[Any], operation: Operation) -> Iterator[Any]` with the exact existing `isinstance` order. Return the moved helper for map, parallel map, tap, filter, flat map, inclusive take, unique, chunk, window, grouped runs, zip variants, intersperse, concat, cross, scans, gather, prepend/append, first/last mapping, and collapse. Return the existing `itertools` layers for take, drop, take-while, drop-while, pairwise, and enumerate. Preserve the current in-memory/external `SortOp` branch. End with:

```python
    raise TypeError(f"unsupported synchronous operation: {type(operation).__name__}")
```

- [ ] **Step 4: Replace the long dispatch chain in `execute()`**

In `sync.py`, import `apply_operation` and `close_iterator`. Remove imports now owned only by `sync_ops.py`. Preserve the map/filter/tap fusion branch and adjacent gatherer composition branch exactly. Replace the remaining `isinstance` chain with:

```python
        iterator = apply_operation(iterator, operation)
        _remember_iterator(managed_iterators, iterator)
        index += 1
```

Update `_remember_iterator` and the final cleanup loop to call `close_iterator`.

- [ ] **Step 5: Run focused tests and static checks**

```bash
.venv/bin/pytest tests/test_invariants.py tests/test_flow_api.py tests/test_stream_extensions.py -q
.venv/bin/ruff check src/fpstreams/execution tests/test_invariants.py
.venv/bin/ruff format --check src/fpstreams/execution tests/test_invariants.py
.venv/bin/mypy src/fpstreams/execution src/fpstreams/planning
```

Expected: all selected tests and checks pass.

- [ ] **Step 6: Inspect the refactor boundary without staging**

Run `git diff --stat` for the three task files, `git diff --check`, and `git diff --cached --name-only`.

Expected: orchestration shrinks in `sync.py`, operation code appears in `sync_ops.py`, whitespace is clean, and nothing is staged.

### Task 3: Verify execution behavior across public and native paths

**Files:**
- Modify only if a regression is found: `src/fpstreams/execution/sync.py`
- Modify only if a regression is found: `src/fpstreams/execution/sync_ops.py`
- Test: `tests/test_execution_engines.py`
- Test: `tests/test_collecting_api.py`
- Test: `tests/test_rows_api.py`
- Test: `tests/test_pairs_api.py`

**Interfaces:**
- Consumes: the refactored `execute(plan)` entry point.
- Produces: a behaviorally identical synchronous path for Flow, Rows, Pairs, collectors, hybrid plans, and forced native plans.

- [ ] **Step 1: Run every synchronous consumer test group**

```bash
.venv/bin/pytest tests/test_flow_api.py tests/test_stream_extensions.py   tests/test_execution_engines.py tests/test_collecting_api.py   tests/test_rows_api.py tests/test_pairs_api.py -q
```

Expected: all tests pass. If one fails, restore semantic identity with the pre-refactor helper body before changing an expectation.

- [ ] **Step 2: Verify source closure and short-circuit cases directly**

```bash
.venv/bin/pytest   tests/test_flow_api.py::test_short_circuit_closes_the_upstream_iterator   tests/test_stream_extensions.py::test_integrator_short_circuits_source_and_still_finishes   tests/test_stream_extensions.py::test_take_while_inclusive_emits_the_boundary_then_closes   tests/test_execution_engines.py::test_native_short_circuit_terminals_do_not_evaluate_the_tail -q
```

Expected: four tests pass without an extra source pull or leaked iterator.

- [ ] **Step 3: Recalculate structural metrics**

Use the repository AST metric to require `execute()` to stay below 65 physical lines and below 12 branch nodes. Confirm `sync_ops.py` contains operation mechanics but no plan traversal loop.

- [ ] **Step 4: Check the no-public-change invariant**

Run a Python process that records `inspect.signature(Flow.map)`, `Flow.filter`, and `Flow.take` before and after the extraction and asserts the strings are unchanged.

### Task 4: Complete repository verification and handoff

**Files:**
- Verify: the full repository.
- Do not create release artifacts inside the repository root.

**Interfaces:**
- Consumes: all changes from Tasks 1–3.
- Produces: a verified, uncommitted synchronous execution refactor ready for review.

- [ ] **Step 1: Run the complete Python suite**

```bash
.venv/bin/pytest -q
.venv/bin/ruff check src tests
.venv/bin/ruff format --check src tests
.venv/bin/mypy src/fpstreams
.venv/bin/uv lock --check
```

Expected: 210 existing tests plus the new contract tests pass, one optional test remains skipped, and all quality checks pass.

- [ ] **Step 2: Run Rust and documentation checks**

```bash
cargo fmt --manifest-path rust/Cargo.toml -- --check
cargo test --manifest-path rust/Cargo.toml
cargo clippy --manifest-path rust/Cargo.toml --all-targets -- -D warnings
.venv/bin/mkdocs build --strict --config-file fpstreams/mkdocs.yml --site-dir /tmp/fpstreams-site
```

Expected: 11 Rust tests pass, formatting and clippy are clean, and MkDocs builds successfully.

- [ ] **Step 3: Build and smoke-test an isolated wheel**

Build the wheel into a fresh `/tmp/fpstreams-wheel-verify.*` directory, install it into a fresh virtual environment, and assert a mapped and filtered `flow(range(5))` produces `[1, 3, 5]`. Also confirm `fpstreams/py.typed`, license metadata, and source docstrings remain installed.

- [ ] **Step 4: Enforce version-control guardrails**

```bash
test "$(git rev-parse HEAD)" = "13912419175ecd351d96ecb3cf14a4e112da2ce0"
test -z "$(git diff --cached --name-only)"
git diff --check
```

Expected: HEAD is unchanged, the staged-file count is zero, and all diffs are whitespace-clean.

- [ ] **Step 5: Report the tranche and select the next design**

Report changed module boundaries, test totals, structural metrics, and behavior-preservation evidence. The next design cycle targets asynchronous dispatch; the tabular join/SQLite tranche follows after both execution engines are stable.
