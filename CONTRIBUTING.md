# Contributing to fpstreams

Thank you for taking the time to improve fpstreams. Small, focused changes are
easier to review and safer to release than broad rewrites.

## Set up the repository

Python 3.11 or newer, Rust, and [uv](https://docs.astral.sh/uv/) are required for
the complete development environment.

```bash
uv sync --extra arrow --extra data --extra polars --extra sketch \
  --group build --group test --group lint --group type --group docs
uv run maturin develop --release
```

The sketch extra needs Apache DataSketches. Its 5.2.0 release has no CPython
3.14 or 3.15 wheel. Source builds need CMake 3.16 or newer, a C++17 compiler,
and access to the upstream DataSketches C++ repository. Python 3.11–3.13 can
use prebuilt wheels. The base library and other checks remain available without this extra.

## Make a change

- Keep public behavior compatible within the current major release line. Describe
  intentional incompatibilities and migration paths in the changelog. The 2.5.0
  release includes the documented static averaging type correction.
  A local build alone does not establish publication.
- Preserve ordering, object identity, errors, source ownership, and cleanup when
  adding an optimized path. Unsupported shapes must retain the canonical fallback.
- Keep modules organized by one clear responsibility. Prefer an existing domain
  package over adding another top-level facade or one-file helper.
- Add regression coverage to the closest existing test module. Avoid creating a
  new test file for a single case.
- Write public docstrings with Google sections and Markdown references. Indent
  continuation lines within a `Returns:` item by four extra spaces; name tuple
  elements when describing them separately. Check the rendered tables after editing.
- Do not add a fast path that recognizes benchmark fixtures or only wins at one
  fixed size, type, or cardinality.

## Internal execution code

`tabular/rows.py` retains public validation and live materialization hooks.
`planning/logical.py` assembles validated binary join plans for both join modes.

`planning/compiler.py` owns unary-stage compilation and source-free plan caches.
`planning/relational/__init__.py` builds relation trees using that same stage compiler;
`planning/relational/specs.py` recognizes backend candidates without opening inputs.

`execution/relational/` keeps dispatch and live source/selector proofs at its
package entry point. `native_group.py` owns optional native grouping ABIs, and
`collector_group.py` owns first-seen Collector state and cleanup. Its hash lookup
must observe replacements made by key and Collector callbacks.

`streams/flow_terminals.py` keeps public methods and canonical identity guards.
Its helpers in `streams/_terminals/` accept the live terminal owner through
explicit protocols. `reductions.py` groups numeric and frequency reductions;
`structural.py` and `unique.py` retain their separate optimization guards.
Keep a binding live when callbacks can replace it;
copying that binding into a helper module can silently bypass the replacement.
Regression coverage belongs in the existing contract, engine, and flow tests.

`execution/arrow/__init__.py` owns batch dispatch, safety proofs, and the live bindings
used by optimized paths. `numeric.py` implements exact column reductions
and borrowed-buffer means; `sort.py` implements retained-column sorting.
Their hooks read the owner module at each boundary. Keep source claims,
conversion callbacks, fallback, and buffer release in the same order when
moving execution code.

`execution/asynchronous/` owns the async physical executor: its package entry
point composes nodes, `operators.py` contains serial iterators and dispatch,
`concurrent.py` owns map/prefetch queues, and `merge.py` and `timers.py` manage
nested sources and timed work. `execution/pairs/` groups key/value execution.

`planning/arrow/` keeps prefix planning at its package entry point and source
adaptation in `source.py`. `planning/pairs/` owns pair-stage recognition and
its guarded integer-expression implementation in `i64_expression.py`.

`planning/semantics/` keeps stable fact types at its package entry point and
rules with their analyzer in `analysis.py`. `tabular/io/` groups output methods,
text sources, and database sources/sinks. Internal module paths may change;
use the documented `fpstreams` imports for application code.

## Run the checks

Ordinary master pushes and pull requests keep a Python 3.11 test lane, the
separate Python 3.12 coverage/quality environment, and the minimal installation
check. Changed interpreter, resource, platform, or adapter boundaries expand the
relevant matrices. Package, lock, build, Rust, and workflow changes request the
complete matrix. A manual `tests` suite and reusable release qualification keep
all supported interpreter, platform, and adapter checks. Pages settings are a
separate authorization; these local workflow changes do not enable any workflow.

Performance measurements and their tools remain local. GitHub Actions has no
benchmark job, schedule, or manual benchmark suite. The manual `all` suite runs
all non-benchmark checks, including the experimental free-threaded lane.

Choose checks based on what the change affects; CI defines the complete matrix.
For a local internal Python or Rust change, check the affected contracts and use
the relevant lint, format, and type checks. Expand to the relevant type
checker or interpreter matrix when public types or interpreter compatibility
change. Expand performance checks when the change affects a measured performance
path or makes a performance claim. Preserve cancellation, cleanup, and transaction
error contracts when simplifying code.

Guidance or progress-record edits alone need consistency and link checks, not
full test suites, matrices, benchmarks, or package builds. Reuse evidence for
unchanged code within its original scope; record updates do not invalidate it.

The publishing workflow calls the reusable CI workflow for release qualification
before building distributions. Release jobs run smoke checks in isolated Python
and compare the imported package entry point and native extension with installed
`RECORD` hashes. Qualification includes the existing frozen-lock
checks, platform checks, and an adapter compatibility matrix. The adapter jobs
resolve the declared direct dependencies with `lowest-direct` and `highest` on
Python 3.11; they supplement the frozen-lock lane. Windows and macOS smoke checks
also select async cancellation, native execution, spawned workers, and borrowed
executor paths. Their smoke selection includes concurrent cache compilation and
cleanup diagnostics. The experimental free-threaded matrix uses Python 3.14t and
3.15t with the `test-core` dependency group. It checks native import without
re-enabling the GIL, cache coordination, concurrent native snapshots, and Flow
smoke contracts, plus shared row-expression evaluation. The regular `test` group
includes `test-core` and the adapter
dependencies; adapter coverage remains in the other jobs.

Report local workflow review and static validation separately from hosted CI
runs. A configured job is not a passing run. The new compatibility and expanded
platform lanes still need a historical baseline; retain failures and record the
resolved dependency versions when assessing their results.

The CI workflow runs on pull requests and pushes to `master`; feature branches
without a pull request can use a manual run. New push and pull request runs
cancel older runs for the same branch or pull request. Manual runs use separate
groups, and release qualification uses a separate non-cancelling group. Docs
deployment on `master` runs only for documentation, package-source or build-input
changes; manual deployment remains available.

Tests follow the source domains under `tests/streams/`, `tests/tabular/`,
`tests/execution/`, and `tests/contracts/`. Keep statistics and sketches in
`tests/streams/test_collecting.py`, rolling Flow behavior in
`tests/streams/test_flow.py`, and async cancellation in
`tests/streams/test_async.py`. Field dependencies belong in
`tests/tabular/test_rows.py`; Arrow projection belongs in
`tests/tabular/test_arrow.py`. Add cases to the relevant module and reuse its
helpers. `tests/typing/` holds consumer type-checking fixtures.
Parameterize genuinely shared scenarios; keep checks for distinct failure,
ownership, and backend contracts even when they use similar input.

The commands below are available checks, not a required sequence for every edit.
Select those that cover the affected code, dependencies, docs, or packaging.
Release qualification still follows the publishing workflow.

```bash
uv lock --check
uv run pytest -W error --cov=src/fpstreams --cov-branch \
  --cov-report=term-missing
uv run coverage report
uv run ruff check src tests scripts
uv run ruff format --check src tests scripts
uv run mypy src/fpstreams
cargo fmt --manifest-path rust/Cargo.toml -- --check
cargo test --manifest-path rust/Cargo.toml
cargo clippy --manifest-path rust/Cargo.toml --all-targets -- -D warnings
uv run python scripts/build_browser_wheel.py
uv run mkdocs build --strict --config-file fpstreams/mkdocs.yml
```

Public type assertions and invalid-call fixtures live in `tests/typing/`.
The existing pytest regression checks them with mypy when the `type` group is
installed. The quality job also checks them with pinned Microsoft Pyright for
Python 3.11–3.15 language targets. These static targets do not replace runtime
tests on each Python version. Pyright is a development tool; Node.js is not
needed to install or use fpstreams.

With Node.js 24.22.1 and the `type` dependency group installed, run from the
repository root:

```bash
npm ci --prefix tests/typing --ignore-scripts --omit=optional
uv run python scripts/check_typing.py
uv run python scripts/check_typing.py --python-version 3.15
```

The [Flow reduce guide](fpstreams/docs/reference/flow.md) explains
its current `Any` return type. The runnable
[reduce examples](tests/typing/reduce_examples.py) demonstrate annotated callbacks
and concrete result checks. The [negative examples](tests/typing/reduce_invalid.py)
are static inputs: expect rejections at the marked lines for incompatible
assignments, callbacks, and accumulator operations. Do not execute that file.

Run the positive examples with:

```bash
uv run python tests/typing/reduce_examples.py
```

The `scripts/check_typing.py` commands above check both the original
`valid.py`/`invalid.py` pair and the reduce example pair. Each checker receives
all four files in one invocation, so the default command starts two checker
processes. The 120-second limit applies to each checker invocation. The runner
succeeds only when positive assertions pass and negative fixtures produce their
expected rejections at the marked lines. Extra Unknown diagnostics are allowed
only on marked negative lines. Errors in positive or external files, missing
diagnostics, unexpected locations, and tool failures cause failure. A successful
runner exit means the checkers rejected the marked invalid calls. These examples
do not repair the public `reduce` signature.

The default target is Python 3.11; use `--checker mypy` or `--checker pyright`
to run one checker. Add successful uses with `assert_type` in `valid.py`.
In `invalid.py`, mark each rejected expression with its expected error category.
The checker must report that category on that line; an unrelated failure or
missing tool cannot satisfy a negative case. These fixtures are static inputs,
not programs to execute.

Measure locally when the affected performance path or a performance claim needs
evidence. Select workloads that cover that impact. The benchmark tools and raw
reports are not included in the public source distribution. Before an
expensive formal performance confirmation, run known low-cost compatibility
counterexamples for that path, such as foreign-loop rejection for async task
changes, together with its cancellation and cleanup contracts. Resolve failures
before timing. For the selected workloads, compare more than one input size,
and include different data types or key cardinalities where they change the
algorithm. Report the environment and raw result rather than a standalone
speedup claim.

Before an optimization experiment, fix its workload, sample budget, and retention
rule. Measure complete consumption and cleanup, and keep the raw results with
the loaded source and native-extension identities. Record rejected attempts so
later work can reuse the decision. A paused or unverified prototype belongs in
recoverable work records; its changelog entry should wait until validation.

Use a patched interpreter for allocation measurements. CPython 3.12.3 can
segfault when `tracemalloc.stop()` overlaps native thread initialization
([CPython #128679](https://github.com/python/cpython/issues/128679)). The local
file-scan stress check passes on CPython 3.12.13 with the same dependencies.
Keep tracing enabled, record the full Python version, and rerun both baseline
and candidate after changing interpreters.

## Open a pull request

Describe the user-visible problem, the chosen boundary, and how you verified the
change. Keep generated benchmark output and local build artifacts out of the
commit. By participating, you agree to communicate respectfully and focus review
comments on the work.
