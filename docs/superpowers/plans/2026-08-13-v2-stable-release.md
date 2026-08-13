# fpstreams 2.0.0 Stable Release Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Freeze and verify the complete public Python API, remove any release-blocking semantic defects, and prepare version `2.0.0` without pushing or publishing it.

**Architecture:** Treat `fpstreams.__all__`, its exported classes, and their public methods as the stable contract. Exercise that contract through real Python calls on every supported interpreter, preserve Python/native semantics, and keep release metadata synchronized across Python, Rust, documentation, and the lockfiles.

**Tech Stack:** Python 3.11–3.14, pytest, mypy, Ruff, PyO3, Rust 2024, Cargo, maturin, MkDocs, GitHub Actions, PyPI Trusted Publishing.

## Global Constraints

- The stable version is exactly `2.0.0` in Python, Rust, documentation, and built distribution metadata.
- Python 3.11, 3.12, 3.13, and 3.14 remain supported.
- Public APIs require English runtime docstrings suitable for editor hover and MkDocs rendering.
- Python and native engines must preserve runtime-specific Python behavior.
- No remote push, GitHub Release, or PyPI publication is part of this plan.
- Keep the consolidated test layout; add regression coverage to an existing test module.

---

### Task 1: Audit and freeze the root public API

**Files:**
- Modify if required: `src/fpstreams/__init__.py`
- Modify if required: `src/fpstreams/flow.py`
- Modify if required: `src/fpstreams/rows.py`
- Modify if required: `src/fpstreams/pairs.py`
- Test: `tests/test_compatibility.py`

**Interfaces:**
- Consumes: `fpstreams.__all__`, root factory functions, compatibility aliases, and public class signatures.
- Produces: A root API whose exported objects remain callable and correctly bound after supported imports.

- [x] **Step 1: Generate the complete root-export and public-method inventory**

Run an introspection script that records every `fpstreams.__all__` export, signature, defining module, docstring presence, and every non-private method on exported classes.

- [x] **Step 2: Exercise every public factory and compatibility import boundary**

Run real smoke calls for `flow`, `aflow`, `rows`, `pairs`, expression helpers, collectors, aggregators, containers, and functional utilities. Import compatibility modules before repeating the root factory calls so package-attribute collisions cannot hide.

- [x] **Step 3: Add a failing regression test for each confirmed contract defect**

Add each regression to `tests/test_compatibility.py`; the expected behavior must be a literal public result and the test must fail for the diagnosed defect before production code changes.

- [x] **Step 4: Apply the smallest API correction**

Change only the module or export responsible for the confirmed defect. Remove a colliding compatibility facade rather than adding import-hook or callable-module behavior when no supported stable contract requires that facade.

- [x] **Step 5: Run the focused compatibility suite**

Run: `uv run pytest -q tests/test_compatibility.py`

Expected: all compatibility and root API tests pass.

### Task 2: Audit behavior, signatures, typing, and documentation

**Files:**
- Modify if required: `src/fpstreams/streams/flow.py`
- Modify if required: `src/fpstreams/streams/flow_terminals.py`
- Modify if required: `src/fpstreams/streams/async_flow.py`
- Modify if required: `src/fpstreams/streams/async_terminals.py`
- Modify if required: `src/fpstreams/tabular/rows.py`
- Modify if required: `src/fpstreams/streams/pairs.py`
- Modify if required: `src/fpstreams/collecting/collector.py`
- Modify if required: `src/fpstreams/collecting/aggregation.py`
- Modify if required: `src/fpstreams/primitives/option.py`
- Modify if required: `src/fpstreams/primitives/result.py`
- Test: one existing domain test module matching each confirmed defect

**Interfaces:**
- Consumes: The frozen root exports from Task 1.
- Produces: Consistent signatures, exceptions, laziness, source ownership, engine parity, type hints, and editor-visible docstrings.

- [x] **Step 1: Compare all exported signatures and method families**

Check constructors, factory methods, transformations, terminals, adapter methods, collectors, aggregators, and containers for inconsistent defaults, misleading annotations, or duplicate names with different meanings.

- [x] **Step 2: Run domain-focused behavior and parity suites**

Run the existing Flow, AsyncFlow, Rows, Pairs, primitive, collector, execution-engine, invariant, and adapter tests separately so failures identify the owning API family.

- [x] **Step 3: Validate public documentation coverage**

Require every root-exported function/class and every public method on exported classes to have a runtime docstring. Build MkDocs with `--strict` so every API reference resolves.

- [x] **Step 4: Fix only confirmed release blockers with red-green tests**

For each defect, add the smallest real-behavior regression to the owning consolidated test file, verify the red state, implement the correction, and verify the focused green state.

### Task 3: Make the release pipeline reproducible

**Files:**
- Modify: `.github/workflows/publish.yml`
- Test: local YAML parsing and local execution of version-validation logic

**Interfaces:**
- Consumes: A Git tag and package version.
- Produces: Release-triggered PyPI publishing plus a safe manual build-only path for validating the complete wheel matrix before consuming version `2.0.0`.

- [x] **Step 1: Preserve `release: published` as the only automatic publish trigger**

Keep GitHub Release publication as the production event and retain job-level `id-token: write` only on the PyPI upload job.

- [x] **Step 2: Add a build-only manual dispatch path**

Allow `workflow_dispatch` to run version validation and all wheel/sdist build jobs, but guard the PyPI upload job with `github.event_name == 'release'` so a manual validation cannot publish.

- [x] **Step 3: Validate workflow structure and tag/version checks locally**

Parse the workflow as YAML and execute the embedded version comparison with both matching and mismatching tags.

### Task 4: Synchronize stable release metadata

**Files:**
- Modify: `pyproject.toml`
- Modify: `rust/Cargo.toml`
- Modify: `rust/Cargo.lock`
- Modify: `src/fpstreams/__init__.py`
- Modify: `README.md`
- Modify: `fpstreams/docs/roadmap.md`
- Modify: `uv.lock`

**Interfaces:**
- Consumes: The verified API and release workflow from Tasks 1–3.
- Produces: Python, Rust, documentation, lockfile, wheel, and sdist metadata that all report `2.0.0`.

- [x] **Step 1: Replace prerelease metadata with `2.0.0`**

Set the project version to `2.0.0`, the Rust crate version to `2.0.0`, and `fpstreams.__version__` to `2.0.0`. Rewrite current-version prose so it no longer tells stable users to install a prerelease.

- [x] **Step 2: Refresh lockfiles**

Run: `uv lock`

Run: `cargo check --manifest-path rust/Cargo.toml`

Expected: `uv.lock` and `rust/Cargo.lock` both record version `2.0.0`.

- [x] **Step 3: Build and inspect release artifacts**

Build a locked PyPI-compatible abi3 wheel and sdist with maturin. Inspect filenames, `METADATA`, `Requires-Python`, native extension presence, typing marker, license, README, Rust sources, and Cargo lockfile.

### Task 5: Execute the stable release gate and commit

**Files:**
- Verify: all changed files from Tasks 1–4

**Interfaces:**
- Consumes: The complete candidate tree.
- Produces: One reviewed local release-preparation commit on `master`.

- [x] **Step 1: Run supported-Python tests**

Run all tests on Python 3.11, 3.12, 3.13, and 3.14 with a freshly built native extension for each interpreter.

- [x] **Step 2: Run quality and Rust gates**

Run Ruff lint/format, strict mypy, lock checks, Cargo fmt, all Rust tests, and clippy with warnings denied.

- [x] **Step 3: Run docs and isolated-install gates**

Build MkDocs strictly, install the wheel into a clean Python environment outside the repository, import every root export, and exercise representative Flow, AsyncFlow, Rows, Pairs, collector, and container calls.

- [x] **Step 4: Review the final diff and commit locally**

Require a clean diff check and no unrelated paths, then commit with the English message `release: prepare fpstreams 2.0.0`. Do not push or create a Release.

## Self-Review

- The plan covers every root-exported API family and the release metadata named by the repository.
- Conditional code changes require a confirmed failing behavior test before implementation.
- Release publication remains gated to GitHub Release events; manual dispatch is build-only.
- The plan contains no placeholder implementation steps or undefined interfaces.
