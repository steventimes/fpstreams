# Functional Coverage & Roadmap

This document captures how `fpstreams` applies functional programming concepts today, what gaps are worth filling next, and how Rust could accelerate expensive workloads while preserving the Python-first API.

## Current Functional Programming Coverage

`fpstreams` already embodies several FP staples:

- **Composable pipelines** via `Stream`/`ParallelStream` transformations such as `map`, `filter`, `flat_map`, `zip`, `scan`, `batch`, and `window`.
- **Lazy evaluation** in stream pipelines, with terminal operations like `collect`, `reduce`, `to_list`, and `count`.
- **Functional helpers** like `pipe`, `curry`, and `retry` for composition, currying, and robust async retries.
- **Container types** (`Option`, `Result`) that encode nullability and error handling in a functional style.
- **Collectors** that provide grouping, summarizing, partitioning, and mapping into aggregate results.
- **Async and parallel variants** to keep functional pipelines consistent across sync/async/CPU-bound workloads.

## Potential Functional Additions

If you want deeper FP ergonomics, these additions would keep the API aligned with the existing stream/collector style:

1. **Stream combinators**
   - `partition(predicate)` → returns `(matches, non_matches)` without forcing collectors.
   - `chunk_by(key_fn)` → starts a new chunk when the key changes (useful for run-length-like grouping).
   - `distinct_by(key_fn)` → distinct with projection, complementing `distinct()`.
   - `take_until(predicate)` / `drop_until(predicate)` → common FP flow-control operations.
   - `merge_sorted(other, key=None)` → stream-friendly merges for pre-sorted inputs.

2. **Collector extensions**
   - `median`, `percentile`, and `histogram` collectors for richer stats.
   - `top_n` / `bottom_n` collectors to avoid full sorts for large datasets.

3. **Option/Result ergonomics**
   - `zip`, `sequence`, and `traverse` helpers to combine `Option`/`Result` values across collections.

4. **Type-focused affordances**
   - `map_typed` / `filter_typed` variants or overloads that narrow types for better IDE guidance.

These are additive and can remain optional, preserving the current API surface while giving power users more functional vocabulary.

## Rust Acceleration Plan

Certain operations are CPU-heavy or memory-sensitive and are prime candidates for a Rust extension module. The key is to keep Python ergonomics while enabling a fast-path for large data or numeric workloads.

### Candidate hotspots

- **Numeric collectors**: `summarizing`, `summing`, `averaging`, quantiles.
- **High-volume transforms**: `map`/`filter`/`flat_map` on numeric streams.
- **Windowing/scan**: especially on large sequences of numeric data.
- **Group-by and distinct** for large datasets (hashing overhead in Python can be high).
- **Parallel operations**: a Rust-backed `parallel()` pipeline using `rayon` for consistent throughput.

### Proposed approach

1. **Optional extension module**
   - Build a `fpstreams_rust` extension via `pyo3` + `maturin`.
   - Ship as an extra (e.g., `pip install fpstreams[fast]`) that preserves the pure-Python fallback.

2. **Stable Python API**
   - Keep the public classes and method signatures unchanged.
   - Route to Rust fast-paths when the stream contains Rust-friendly types (e.g., numeric lists, NumPy arrays, or buffer protocol inputs).

3. **Interoperability strategy**
   - Support Python iterables for compatibility, but add an optimized path for lists/tuples/arrays.
   - Use `PyBuffer`/NumPy views for zero-copy operations where possible.

4. **Incremental rollout**
   - Start with collectors (`summarizing`, `summing`, `averaging`) and window/scan operations.
   - Add parallel map/filter/reduce after functional parity is proven.
   - Gate by benchmarks to validate real-world wins.

5. **Testing & CI**
   - Add Python/Rust parity tests.
   - Build wheels for major platforms in CI to keep installation friction low.

This plan keeps `fpstreams` easy to install while unlocking a high-performance option for complex workloads.
