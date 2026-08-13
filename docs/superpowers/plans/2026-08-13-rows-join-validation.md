# Rows join validation and extraction implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans task by task. Do not use subagents for this repository.

**Goal:** Add explicit join-cardinality contracts and reduce the largest Rows complexity hotspot
without changing default join behavior.

**Constraints:** No new dependency, commit, stage, push, publish, or docs deployment. Keep tests in
`tests/test_rows_api.py` and use `.venv/bin/...`.

### Task 1: Characterize public validation

- [ ] Add RED tests for invalid mode before source opening, accepted `m:m`, left/right/both duplicate
      failures, and successful unique contracts.
- [ ] Add RED parity tests for semi/anti and partitioned execution with temp cleanup.

### Task 2: Extract in-memory join execution

- [ ] Create `tabular/join.py` with selector normalization, join-only record helpers, uniqueness
      checks, and a deferred iterator factory.
- [ ] Reduce `Rows.join` to its public docstring, arguments, and delegation.
- [ ] Remove join-only helpers/imports from `records.py` and `rows.py`.
- [ ] Keep no-validation paths allocation-equivalent to the current implementation.

### Task 3: Extend spilled joins

- [ ] Pass validation mode to `spilled_join`.
- [ ] Check right and left uniqueness per partition only when required.
- [ ] Ensure failures precede output merge and temporary files are removed.

### Task 4: Verify and review

- [ ] Run all Rows/data adapter tests, Ruff, strict mypy, and complexity metrics.
- [ ] Verify the new source docstring renders in MkDocs and installed wheel hover help.
- [ ] Run the complete repository suite and version-control guardrails.
