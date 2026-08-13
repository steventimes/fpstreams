# dev 提交清单

## 主仓库 `fpstreams`

下面按当前目录结构和改动内容分组；每个三级标题都是建议使用的英文 commit message。

### `refactor: rewrite Flow, AsyncFlow, and core APIs`

- [ ] `src/fpstreams/__init__.py`
- [ ] `src/fpstreams/async_flow.py`
- [ ] `src/fpstreams/core/__init__.py`
- [ ] `src/fpstreams/core/async_ops.py`
- [ ] `src/fpstreams/core/async_stream.py`
- [ ] `src/fpstreams/core/collectors.py`
- [ ] `src/fpstreams/core/common.py`
- [ ] `src/fpstreams/core/ops.py`
- [ ] `src/fpstreams/core/parallel.py`
- [ ] `src/fpstreams/core/sequential.py`
- [ ] `src/fpstreams/core/stream_interface.py`
- [ ] `src/fpstreams/errors.py`
- [ ] `src/fpstreams/exceptions.py`
- [ ] `src/fpstreams/expr.py`
- [ ] `src/fpstreams/expressions/__init__.py`
- [ ] `src/fpstreams/expressions/row.py`
- [ ] `src/fpstreams/expressions/scalar.py`
- [ ] `src/fpstreams/expressions/selectors.py`
- [ ] `src/fpstreams/flow.py`
- [ ] `src/fpstreams/functional.py`
- [ ] `src/fpstreams/option.py`
- [ ] `src/fpstreams/primitives/__init__.py`
- [ ] `src/fpstreams/primitives/option.py`
- [ ] `src/fpstreams/primitives/result.py`
- [ ] `src/fpstreams/result.py`
- [ ] `src/fpstreams/streams/__init__.py`
- [ ] `src/fpstreams/streams/async_flow.py`
- [ ] `src/fpstreams/streams/async_terminals.py`
- [ ] `src/fpstreams/streams/flow.py`
- [ ] `src/fpstreams/streams/flow_terminals.py`

### `feat: add execution planning, collectors, and aggregations`

- `src/fpstreams/aggregate.py`
- `src/fpstreams/collecting/__init__.py`
- `src/fpstreams/collecting/aggregation.py`
- `src/fpstreams/collecting/collector.py`
- `src/fpstreams/collecting/statistics.py`
- `src/fpstreams/collectors.py`
- `src/fpstreams/execution/__init__.py`
- `src/fpstreams/execution/async_.py`
- `src/fpstreams/execution/async_concurrency.py`
- `src/fpstreams/execution/async_iterators.py`
- `src/fpstreams/execution/async_ops.py`
- `src/fpstreams/execution/native.py`
- `src/fpstreams/execution/sorting.py`
- `src/fpstreams/execution/sync.py`
- `src/fpstreams/execution/sync_ops.py`
- `src/fpstreams/gather.py`
- `src/fpstreams/planning/__init__.py`
- `src/fpstreams/planning/async_.py`
- `src/fpstreams/planning/async_utils.py`
- `src/fpstreams/planning/explain.py`
- `src/fpstreams/planning/gather.py`
- `src/fpstreams/planning/native.py`
- `src/fpstreams/planning/source.py`
- `src/fpstreams/planning/sync.py`

### `feat: add Rows, Pairs, and data adapters`

- `src/fpstreams/column.py`
- `src/fpstreams/pairs.py`
- `src/fpstreams/rows.py`
- `src/fpstreams/streams/pairs.py`
- `src/fpstreams/tabular/__init__.py`
- `src/fpstreams/tabular/arrow.py`
- `src/fpstreams/tabular/dataframe.py`
- `src/fpstreams/tabular/factory.py`
- `src/fpstreams/tabular/grouped.py`
- `src/fpstreams/tabular/io.py`
- `src/fpstreams/tabular/join.py`
- `src/fpstreams/tabular/polars.py`
- `src/fpstreams/tabular/records.py`
- `src/fpstreams/tabular/rows.py`
- `src/fpstreams/tabular/spill.py`
- `src/fpstreams/tabular/sql.py`
- `src/fpstreams/tabular/sqlite_sink.py`

### `feat: split and extend the native Rust engine`

- `rust/Cargo.lock`
- `rust/Cargo.toml`
- `rust/src/common.rs`
- `rust/src/float.rs`
- `rust/src/integer.rs`
- `rust/src/lib.rs`
- `rust/src/list_ops.rs`
- `rust/src/tests.rs`
- `src/fpstreams/_native.pyi`
- `src/fpstreams/rust_ops.py`

### `test: consolidate the v2 test suite`

- `tests/test_00_smoke.py`
- `tests/test_01_unit_streams.py`
- `tests/test_02_unit_primitives.py`
- `tests/test_03_fuzzing.py`
- `tests/test_04_property_based.py`
- `tests/test_05_integration.py`
- `tests/test_06_model_checking.py`
- `tests/test_07_theorem_style.py`
- `tests/test_08_coverage_check.py`
- `tests/test_async_api.py`
- `tests/test_collecting_api.py`
- `tests/test_compatibility.py`
- `tests/test_data_adapters.py`
- `tests/test_execution_engines.py`
- `tests/test_flow_api.py`
- `tests/test_invariants.py`
- `tests/test_pairs_api.py`
- `tests/test_primitives.py`
- `tests/test_rows_api.py`
- `tests/test_stream_extensions.py`

### `docs: rewrite the v2 README and MkDocs API reference`

- `README.md`
- `fpstreams/docs/index.md`
- `fpstreams/docs/reference/async_flow.md`
- `fpstreams/docs/reference/async_stream.md`
- `fpstreams/docs/reference/collecting.md`
- `fpstreams/docs/reference/collectors.md`
- `fpstreams/docs/reference/containers.md`
- `fpstreams/docs/reference/expressions.md`
- `fpstreams/docs/reference/flow.md`
- `fpstreams/docs/reference/functional.md`
- `fpstreams/docs/reference/gatherers.md`
- `fpstreams/docs/reference/option.md`
- `fpstreams/docs/reference/pairs.md`
- `fpstreams/docs/reference/parallel.md`
- `fpstreams/docs/reference/result.md`
- `fpstreams/docs/reference/rows.md`
- `fpstreams/docs/reference/stream.md`
- `fpstreams/docs/roadmap.md`
- `fpstreams/mkdocs.yml`

### `docs: record v2 refactor designs and implementation plans`

- `DEV_COMMIT_CHECKLIST.md`
- `docs/superpowers/plans/2026-08-13-asynchronous-execution-refactor.md`
- `docs/superpowers/plans/2026-08-13-market-informed-async-flow-control.md`
- `docs/superpowers/plans/2026-08-13-rows-join-validation.md`
- `docs/superpowers/plans/2026-08-13-synchronous-execution-refactor.md`
- `docs/superpowers/specs/2026-08-13-execution-core-stabilization-design.md`
- `docs/superpowers/specs/2026-08-13-market-informed-async-flow-control-design.md`
- `docs/superpowers/specs/2026-08-13-rows-join-validation-design.md`

### `build: update v2 packaging, dependencies, and licensing`

- `.gitignore`
- `LICENSE`
- `MANIFEST.in`
- `pyproject.toml`
- `requirements-dev.txt`
- `uv.lock`

### `ci: update test, release, and gh-pages workflows`

- `.github/workflows/docs.yml`
- `.github/workflows/publish.yml`
- `.github/workflows/test.yml`

### `perf: update v2 benchmarks`

- `benchmark.py`
