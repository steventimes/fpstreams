# Contributing to fpstreams

Thank you for taking the time to improve fpstreams. Small, focused changes are
easier to review and safer to release than broad rewrites.

## Set up the repository

Python 3.11 or newer, Rust, and [uv](https://docs.astral.sh/uv/) are required for
the complete development environment.

```bash
uv sync --extra arrow --extra data --extra polars \
  --group build --group test --group lint --group type --group docs
uv run maturin develop --release
```

## Make a change

- Keep public behavior compatible within the v2 release line. Call out any
  intentional exception in the changelog.
- Preserve ordering, object identity, errors, source ownership, and cleanup when
  adding an optimized path. Unsupported shapes must retain the canonical fallback.
- Keep modules organized by one clear responsibility. Prefer an existing domain
  package over adding another top-level facade or one-file helper.
- Add regression coverage to the closest existing test module. Avoid creating a
  new test file for a single case.
- Do not add a fast path that recognizes benchmark fixtures or only wins at one
  fixed size, type, or cardinality.

## Run the checks

Run the checks that cover your change; CI runs the complete matrix. Core Python
or Rust execution changes should run their full language-specific checks before
review.

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

## Open a pull request

Describe the user-visible problem, the chosen boundary, and how you verified the
change. Keep generated benchmark output and local build artifacts out of the
commit. By participating, you agree to communicate respectfully and focus review
comments on the work.
