# Market-informed AsyncFlow controls implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans task by task. Do not use subagents for this repository.

**Goal:** Add bounded rate control, delayed consumption, and latest-inner switching to `AsyncFlow`
while preserving pull-based backpressure and exception-safe cleanup.

**Architecture:** Add declarative async operation nodes, public planning methods with hover
docstrings, and focused execution in `async_concurrency.py`. Keep `async_.py` orchestration unchanged
and extend the exhaustive dispatcher.

**Constraints:** No dependency, public break, commit, stage, push, publish, or docs deployment. Keep
tests in consolidated files and use `.venv/bin/...` tooling.

### Task 1: Characterize public validation and timing semantics

- [ ] Add tests in `tests/test_async_api.py` for `delay`, `throttle`, and `spaceout` before methods exist.
- [ ] Require validation to happen without opening the source.
- [ ] Require delay before the first pull, an initial throttle burst followed by a sliding-window
      wait, and minimum output spacing.
- [ ] Run each focused test and observe the expected RED failure.

### Task 2: Add timing operation nodes and execution

- [ ] Add `_Delay` and `_Throttle` immutable nodes to `planning/async_.py` and `_AsyncOperation`.
- [ ] Add public `AsyncFlow.delay`, `throttle`, and `spaceout` with English Google-style docstrings.
- [ ] Add `delay` and `throttle` generators to `execution/async_concurrency.py` using monotonic event
      loop time and bounded timestamp storage.
- [ ] Extend `SUPPORTED_ASYNC_OPERATION_TYPES` and `apply_async_operation` explicitly.
- [ ] Run focused timing, dispatch-coverage, Ruff, and strict mypy checks.

### Task 3: Characterize switch-to-latest behavior

- [ ] Add tests for latest-inner replacement, awaitable mapper results, deterministic replacement
      when outer and inner pulls race, and completion of the latest inner after outer exhaustion.
- [ ] Add tests for early downstream termination and mapper errors closing outer/current inner
      resources and cancelling pending tasks.
- [ ] Observe focused RED failures before implementation.

### Task 4: Implement switch-map with explicit ownership

- [ ] Add `_SwitchMap` to the plan union and `AsyncFlow.switch_map` with a complete hover docstring.
- [ ] Implement one-outer/one-mapper/one-inner task ownership in `async_concurrency.py`.
- [ ] Prioritize a newly produced outer value over a simultaneously completed stale inner pull.
- [ ] Await all cancellation and route every iterator through exception-safe cleanup.
- [ ] Extend exhaustive dispatch, then run all async and execution-engine tests plus static checks.

### Task 5: Documentation consistency and repository verification

- [ ] Correct AsyncFlow hover return text that says `Flow` instead of `AsyncFlow`.
- [ ] Confirm MkDocs renders all four methods from source docstrings without README duplication.
- [ ] Run full pytest, Ruff lint/format, strict mypy, lock check, Cargo fmt/test/clippy, and strict
      MkDocs build.
- [ ] Build/install a wheel in `/tmp` and exercise the new public API from the installed package.
- [ ] Confirm HEAD is unchanged, staged files are zero, workflow dispatch entries remain, and
      `git diff --check` passes.
