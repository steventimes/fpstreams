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
3.14 wheel, so use 3.11–3.13 for sketch development or provide the source-build
toolchain. The base library and other checks remain available without this extra.

## Make a change

- Keep public behavior compatible within the v2 release line. Call out any
  intentional exception in the changelog.
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

`planning/compiler.py` owns unary-stage compilation and source-free plan caches.
`planning/relational.py` builds relation trees using that same stage compiler;
`planning/relational_specs.py` recognizes backend candidates without opening inputs.

`execution/relational/` keeps dispatch and live source/selector proofs at its
package entry point. `native_group.py` owns optional native grouping ABIs, and
`collector_group.py` owns first-seen Collector state and cleanup. Its hash lookup
must observe replacements made by key and Collector callbacks.

`streams/flow_terminals.py` keeps public methods and canonical identity guards.
Its numeric, frequency, and materialization helpers accept the live terminal
owner through explicit protocols. Keep a binding live when callbacks can replace it;
copying that binding into a helper module can silently bypass the replacement.
Regression coverage belongs in the existing contract, engine, and flow tests.

`execution/arrow.py` owns batch dispatch, safety proofs, and the live bindings
used by optimized paths. `arrow_numeric.py` implements exact column reductions
and borrowed-buffer means; `arrow_sort.py` implements retained-column sorting.
Their hooks read the owner module at each boundary. Keep source claims,
conversion callbacks, fallback, and buffer release in the same order when
moving execution code.

## Run the checks

Run the checks that cover your change; CI runs the complete matrix. Core Python
or Rust execution changes should run their full language-specific checks before
review.

Tests live in nine domain modules. Keep statistics and sketches in
`test_collecting.py`, rolling Flow behavior in `test_flow.py`, async cancellation
in `test_async.py`, and field dependencies and Arrow projection in
`test_tabular.py`. Add cases to the relevant group and reuse its helpers.
Parameterize genuinely shared scenarios; keep checks for distinct failure,
ownership, and backend contracts even when they use similar input.

```bash
uv lock --check
uv run pytest -W error --cov=src/fpstreams --cov-branch \
  --cov-report=term-missing
uv run coverage report
uv run ruff check src tests scripts benchmarks benchmark.py
uv run ruff format --check src tests scripts benchmarks benchmark.py
uv run mypy src/fpstreams
cargo fmt --manifest-path rust/Cargo.toml -- --check
cargo test --manifest-path rust/Cargo.toml
cargo clippy --manifest-path rust/Cargo.toml --all-targets -- -D warnings
uv run python scripts/build_browser_wheel.py
uv run mkdocs build --strict --config-file fpstreams/mkdocs.yml
```

Run `./run_benchmark.sh` when a change can affect execution. Compare more than
one input size, and include different data types or key cardinalities where they
change the algorithm. Report the environment and raw result rather than a
standalone speedup claim.

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
