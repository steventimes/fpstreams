# Asynchronous Execution Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Do not use subagents for this repository.

**Goal:** Turn asynchronous plan execution into a compact orchestration routine with exhaustive single-operation dispatch and exception-safe cleanup, without changing public behavior.

**Architecture:** Move non-fused async iterator transformations and typed dispatch from `execution/async_.py` into a private `execution/async_ops.py` module. Keep plan traversal and map/filter/tap fusion in `execution/async_.py`; keep task scheduling, cancellation, realtime behavior, and concurrent operators in `execution/async_concurrency.py`.

**Tech Stack:** Python 3.11+, asyncio, pytest, Ruff, strict mypy, Maturin/PyO3, MkDocs.

## Global Constraints

- Do not change public imports, names, signatures, aliases, exception types, timing semantics, or laziness.
- Preserve encounter order, awaitable resolution, fusion order, short-circuiting, cancellation, and source ownership.
- Unknown internal operations must fail explicitly instead of silently passing through.
- Cleanup failures must not hide an active pipeline exception, and one failed close must not prevent later owned iterators from being closed.
- Do not add dependencies.
- Do not commit, stage, push, publish, or deploy documentation.
- Use `.venv/bin/...` for Python tooling.
- Keep tests in the existing consolidated domain files.

---

## File structure

- Create `src/fpstreams/execution/async_ops.py`: non-fused async iterator transformations, explicit operation coverage, single-operation dispatch, and multi-iterator cleanup.
- Modify `src/fpstreams/execution/async_.py`: plan traversal and map/filter/tap fusion only.
- Modify `tests/test_invariants.py`: operation coverage, direct dispatch, explicit unknown-operation failure, and unfused callable ordering.
- Modify `tests/test_async_api.py`: cleanup exception precedence and exhaustive close behavior.

### Task 1: Characterize asynchronous dispatch contracts

**Files:**
- Modify: `tests/test_invariants.py`
- Test: `tests/test_invariants.py`

- [ ] **Step 1: Require the focused dispatch module and exhaustive coverage**

Add a test that discovers `fpstreams.execution.async_ops`, imports `SUPPORTED_ASYNC_OPERATION_TYPES`, and compares it with `typing.get_args(_AsyncOperation)`.

- [ ] **Step 2: Require direct dispatch for a simple operation**

Build a tiny async generator, dispatch `_Take(2)`, consume it with a local async collector, and require `[1, 2]`.

- [ ] **Step 3: Require explicit rejection of unknown operation objects**

Call `apply_async_operation(source, cast(_AsyncOperation, object()))` and require `TypeError("unsupported asynchronous operation: object")`.

- [ ] **Step 4: Require direct stateless callable ordering**

Dispatch `_MapAsync(..., concurrency=1, ordered=True, timeout=None)`, `_Tap`, and `_Filter` separately. Assert both values and exact map/tap/filter event order.

- [ ] **Step 5: Observe each focused RED state before implementing it**

Run only the new test after each addition. The failure must identify the missing module, entry point, branch, or explicit error rather than an unrelated setup problem.

### Task 2: Extract async operation helpers and exhaustive dispatch

**Files:**
- Create: `src/fpstreams/execution/async_ops.py`
- Modify: `src/fpstreams/execution/async_.py`
- Test: `tests/test_invariants.py`

- [ ] **Step 1: Move non-fused helpers without semantic edits**

Move `_flat_map`, `_take`, `_drop`, `_take_while`, `_take_while_inclusive`, `_drop_while`, `_chunk`, `_batch_by_size`, `_window`, `_pairwise`, `_group_runs`, `_fold`, `_unique`, `_enumerate`, `_zip`, `_zip_longest`, `_intersperse`, `_concat`, `_cross`, `_scan`, `_scan_right`, `_prepend`, `_append`, `_map_first`, `_map_last`, and `_collapse` into `async_ops.py`.

- [ ] **Step 2: Add standalone fused-operation helpers for direct dispatch**

Add focused `_filter` and `_tap` async generators that use `_resolve` and close their upstream source in `finally`. `_MapAsync` dispatch continues to use `map_concurrent`, including its sequential configuration.

- [ ] **Step 3: Declare exhaustive operation coverage**

Define `SUPPORTED_ASYNC_OPERATION_TYPES: tuple[type[object], ...]` containing every class in `_AsyncOperation`, in union order.

- [ ] **Step 4: Add typed one-operation dispatch**

Implement `apply_async_operation(source, operation)` with explicit `isinstance` branches. Select timing/concurrent helpers from `async_concurrency.py`; select focused helpers for all other operations. End with an explicit unsupported-operation `TypeError`.

- [ ] **Step 5: Reduce `_execute()` to orchestration**

Keep only source opening, index traversal, adjacent fusable-run detection, `_fused()` construction, dispatcher invocation, yielding, and cleanup in `async_.py`.

- [ ] **Step 6: Run focused behavior and static checks**

Run `tests/test_invariants.py`, `tests/test_async_api.py`, and `tests/test_execution_engines.py`, followed by Ruff and strict mypy for execution/planning modules.

### Task 3: Make asynchronous cleanup exception-safe

**Files:**
- Modify: `src/fpstreams/execution/async_ops.py`
- Modify: `src/fpstreams/execution/async_.py`
- Modify: `tests/test_async_api.py`

- [ ] **Step 1: Add a failing active-exception preservation test**

Use an async source whose `finally` raises a cleanup error and a mapping callable that raises a distinct pipeline error. Require the pipeline error to remain primary and the cleanup failure to be recorded as an exception note.

- [ ] **Step 2: Add a failing exhaustive-close test**

Use two owned async iterators where the first close raises. Require the second close to still run and the first cleanup error to be raised only after all close attempts.

- [ ] **Step 3: Implement ordered async cleanup**

Add `close_async_iterators(iterators)` that checks `sys.exception()`, awaits every close in order, attaches cleanup failures to an active exception, and otherwise raises the first cleanup error after all attempts.

- [ ] **Step 4: Route `_execute()` finalization through the helper**

Close the final iterator when distinct from the root, then the root, without duplicating the same object. Preserve the current ownership order.

- [ ] **Step 5: Verify cancellation and timeout paths**

Run direct tests for cancellation, timeout, debounce, merge, merge-map, and early `take` termination. Confirm no pending-task warnings or leaked sources.

### Task 4: Complete repository verification and review

**Files:**
- Verify: the full repository.
- Do not create release artifacts inside the repository root.

- [ ] **Step 1: Recalculate structure and public signatures**

Require `_execute()` to remain below 65 physical lines and 12 branch nodes. Confirm representative `AsyncFlow` signatures and docstrings are unchanged.

- [ ] **Step 2: Run all Python, Rust, and documentation checks**

Run the complete pytest suite, Ruff lint/format, strict mypy, lock check, Cargo fmt/test/clippy, and strict MkDocs build.

- [ ] **Step 3: Build and smoke-test a fresh wheel**

Build into `/tmp`, install into a fresh virtual environment, run an async mapped/filtered pipeline, and verify installed inline API docstrings plus typing/license files.

- [ ] **Step 4: Enforce version-control guardrails**

Require HEAD `13912419175ecd351d96ecb3cf14a4e112da2ce0`, an empty staged-file list, and a clean `git diff --check` result.

- [ ] **Step 5: Perform a focused code review**

Review dispatch exhaustiveness, iterator ownership, error paths, timing behavior, cancellation, readability, and avoidable per-item overhead. Fix only validated issues and rerun the affected verification.

- [ ] **Step 6: Select the next repository tranche**

After both execution engines are stable, evaluate tabular joins/SQLite integration, duplicated compatibility layers, public API ergonomics, dependency hygiene, and benchmark coverage before choosing the next implementation scope.
