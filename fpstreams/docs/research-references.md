# Research references

This catalog records the 73 references supplied on October 17, 2026, with their
proposed uses for [fpstreams](https://github.com/steventimes/fpstreams). Topics
include lazy pipelines, logical and physical plans, mixed Python/Rust/Arrow/NumPy
execution, spilling groups and joins, bounded async execution, and semantic parity.

Titles, categories, and links below follow the supplied list. Inclusion does not
mean that a source has been verified, reviewed, implemented, or benchmarked.
**Extension** marks a proposed capability that requires new functionality. It
does not mean that the capability is available or scheduled. Review notes below
identify the limited work actually completed against these references.

## Papers, 1–25

| No. | Reference | Proposed use |
| --- | --- | --- |
| 1 | [Stream Fusion: From Lists to Streams to Nothing at All](https://www.cs.tufts.edu/~nr/cs257/archive/duncan-coutts/stream-fusion.pdf) | Fuse map, filter, zip, and fold while reducing intermediate objects. |
| 2 | [Stream Fusion, to Completeness](https://arxiv.org/abs/1612.06668) | Study fusion of nested streams, zip, and early termination. |
| 3 | [Complete Fusion for Stateful Streams](https://arxiv.org/abs/2412.15768) | Establish verifiable transformation rules for stateful stream fusion. |
| 4 | [MonetDB/X100: Hyper-Pipelining Query Execution](https://www.cidrdb.org/cidr2005/papers/P19.pdf) | Measure cache-friendly batch sizes and amortize per-row dispatch. |
| 5 | [Efficiently Compiling Efficient Query Plans for Modern Hardware](https://www.vldb.org/pvldb/vol4/p539-neumann.pdf) | Compare data-centric code generation and pipeline fusion boundaries. |
| 6 | [Vectorization vs. Compilation in Query Execution](https://ir.cwi.nl/pub/18550) | Decide which operators suit batch vectorization or compiled fusion. |
| 7 | [Everything You Always Wanted to Know About Compiled and Vectorized Queries But Were Afraid to Ask](https://www.vldb.org/pvldb/vol11/p2209-kersten.pdf) | Calibrate mixed execution against computation and memory-access patterns. |
| 8 | [Weld: A Common Runtime for High Performance Data Analytics](https://dawn.cs.stanford.edu/publications/weld/weld-common-runtime-high-performance-data-analytics) | Reduce data movement across Python, NumPy, and Arrow. |
| 9 | [Tuplex: Data Science in Python at Native Code Speed](https://cs.brown.edu/people/lspiegel/files/Tuplex_Preprint2020.pdf) | Study common-type native paths and exceptional Python fallback. |
| 10 | [Volcano—An Extensible and Parallel Query Evaluation System](https://15799.courses.cs.cmu.edu/spring2025/papers/04-volcano/graefe-icde1994.pdf) | Compare iterator execution, operator lifecycles, and parallel exchange boundaries. |
| 11 | [The Cascades Framework for Query Optimization](https://15721.courses.cs.cmu.edu/spring2016/papers/graefe-ieee1995.pdf) | Separate optimization rules, cost estimates, and physical strategy selection. |
| 12 | [Morsel-Driven Parallelism](https://ir.cwi.nl/pub/22547) | Balance fine-grained batch scheduling, NUMA, and skew. |
| 13 | [Eddies: Continuously Adaptive Query Processing](https://dsf.berkeley.edu/papers/sigmod00-eddy.pdf) | Explore runtime adaptation only for pure operators that can safely be reordered. |
| 14 | [How Good Are Query Optimizers, Really?](https://ir.cwi.nl/pub/24379) | Test join selection with cardinality errors and skewed data. |
| 15 | [Robust External Hash Aggregation in the Solid State Age](https://duckdb.org/pdf/ICDE2024-kuiper-boncz-muehleisen-out-of-core.pdf) | Improve spill layout and performance under memory pressure. |
| 16 | [Simple, Efficient, and Robust Hash Tables for Join Processing](https://db.in.tum.de/~birler/papers/hashtable.pdf) | Study join-specific hash tables, negative-probe filtering, and skew. |
| 17 | [To Partition, or Not to Partition, That is the Join Question in a Real System](https://www-db.cs.tum.edu/~bandle/papers/bandle-partitionVsNonPartition.pdf) | Measure partitioning costs across join input sizes. |
| 18 | [Global Hash Tables Strike Back!](https://arxiv.org/abs/2505.04153) | Compare shared and partitioned aggregation under memory pressure and skew. |
| 19 | [In-Order Sliding-Window Aggregation in Worst-Case Constant Time](https://arxiv.org/abs/2009.13768) | Evaluate DABA to avoid repeating window computations. |
| 20 | [The Dataflow Model](https://research.google/pubs/the-dataflow-model-a-practical-approach-to-balancing-correctness-latency-and-cost-in-massive-scale-unbounded-out-of-order-data-processing/) | **Extension.** Define event-time windows, watermarks, late data, and result revisions. |
| 21 | [Naiad: A Timely Dataflow System](https://research.google/pubs/naiad-a-timely-dataflow-system/) | **Extension.** Study progress tracking and cyclic incremental dataflow. |
| 22 | [Umbra: A Disk-Based System with In-Memory Performance](https://vldb.org/cidrdb/papers/2020/p29-neumann-cidr20.pdf) | Compare buffer management across memory and disk execution. |
| 23 | [Spark SQL: Relational Data Processing in Spark](https://amplab.cs.berkeley.edu/publication/spark-sql-relational-data-processing-in-spark/) | Study a shared optimizer for relational expressions and functional pipelines. |
| 24 | [Velox: Meta’s Unified Execution Engine](https://vldb.org/pvldb/vol15/p3372-pedreira.pdf) | Separate reusable kernels, connectors, and resource management. |
| 25 | [Adaptive Execution of Compiled Queries](https://zenodo.org/records/2157816) | Study switching between interpreted and compiled execution for short queries. |

## Open-source projects and specifications, 26–63

| No. | Reference | Proposed use |
| --- | --- | --- |
| 26 | [DuckDB](https://github.com/duckdb/duckdb) | Compare vectorized operators, external sorting, and external hash aggregation. |
| 27 | [Polars](https://github.com/pola-rs/polars) | Compare streaming plans, expression optimization, and columnar grouping. |
| 28 | [Apache DataFusion](https://github.com/apache/datafusion) | Study Rust execution plans, optimizer rules, and shared spill budgets. |
| 29 | [Apache Arrow Acero](https://arrow.apache.org/docs/cpp/acero/overview.html) | Compare batch dataflow, backpressure, and Arrow operator composition. |
| 30 | [Velox](https://github.com/facebookincubator/velox) | Study encoded vectors, lazy materialization, and executor memory management. |
| 31 | [Substrait, an open specification](https://github.com/substrait-io/substrait) | Explore serializable and inspectable relational plans across backends. |
| 32 | [Ibis](https://github.com/ibis-project/ibis) | Compare compilation boundaries from one expression API to multiple backends. |
| 33 | [Narwhals](https://github.com/narwhals-dev/narwhals) | Improve DataFrame adaptation, static typing, and optional dependency isolation. |
| 34 | [Daft](https://github.com/Eventual-Inc/Daft) | Compare pipelines combining Python UDFs and Rust columnar execution. |
| 35 | [Dask](https://github.com/dask/dask) | Study task fusion, chunk granularity, and scheduler overhead. |
| 36 | [Ray Data](https://github.com/ray-project/ray) | Compare batch mapping, concurrency budgets, and producer/consumer backpressure. |
| 37 | [Vaex](https://github.com/vaexio/vaex) | Study memory mapping and virtual columns to reduce array copies. |
| 38 | [PyO3](https://github.com/PyO3/pyo3) | Check batch FFI, object borrowing, and GIL release boundaries. |
| 39 | [rust-numpy](https://github.com/PyO3/rust-numpy) | Check shared NumPy buffer lifetimes, strides, and writability. |
| 40 | [NumExpr](https://github.com/pydata/numexpr) | Compare chunked expression evaluation and temporary array costs. |
| 41 | [Numba](https://github.com/numba/numba) | Compare numeric specialization, compilation caches, and parallel reduction. |
| 42 | [cytoolz](https://github.com/pytoolz/cytoolz) | Compare functional grouping, joins, and reduction internals. |
| 43 | [more-itertools](https://github.com/more-itertools/more-itertools) | Check lazy windows, chunking, grouping, and iterator boundaries. |
| 44 | [aiostream](https://github.com/vxgmichel/aiostream) | Compare ordered concurrent maps, task limits, and safe closing. |
| 45 | [asyncstdlib](https://asyncstdlib.readthedocs.io/en/stable/index.html) | Compare async iterator borrowing, scoped closing, and exception propagation. |
| 46 | [ReactiveX for Python / RxPY](https://github.com/ReactiveX/RxPY) | Compare debounce, switch, merge, and time-operator semantics. |
| 47 | [AnyIO](https://github.com/agronholm/anyio) | Study task groups, cancellation scopes, and bounded object streams. |
| 48 | [Trio](https://github.com/python-trio/trio) | Compare structured concurrency, cancellation, and task cleanup. |
| 49 | [Streamz](https://github.com/python-streamz/streamz) | Compare branching dataflow, feedback, and slow-consumer backpressure. |
| 50 | [Apache Flink](https://github.com/apache/flink) | **Extension.** Study windows, state backends, and backpressure diagnostics. |
| 51 | [Apache Beam](https://github.com/apache/beam) | **Extension.** Study composable Combine, window triggers, and batch/stream semantics. |
| 52 | [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow) | **Extension.** Explore incremental joins, grouping, and indexed intermediate state. |
| 53 | [Strymonas, a research implementation](https://github.com/strymonas/strymonas-ocaml) | Inspect generated loops for complex stream combinations without intermediate objects. |
| 54 | [hashbrown](https://github.com/rust-lang/hashbrown) | Compare SIMD probing and compact hash layouts for native grouping. |
| 55 | [Rayon](https://github.com/rayon-rs/rayon) | Explore native parallel iteration, reduction, and thread-pool reuse. |
| 56 | [pyperf](https://github.com/psf/pyperf) | Improve process warmup, environment capture, and regression comparison. |
| 57 | [Hypothesis](https://github.com/HypothesisWorks/hypothesis) | Generate boundary inputs for Python/native semantic equivalence checks. |
| 58 | [IBM Sliding Window Aggregators](https://github.com/IBM/sliding-window-aggregators) | Compare DABA and related window algorithms and benchmarks. |
| 59 | [ConnectorX](https://github.com/sfu-db/connector-x) | Study database-to-Arrow batch reads with fewer copies. |
| 60 | [Apache Arrow nanoarrow](https://github.com/apache/arrow-nanoarrow) | Compare Arrow C Data/C Stream interoperability and lifetimes. |
| 61 | [Apache DataSketches](https://github.com/apache/datasketches) | Evaluate optional bounded-state approximate cardinality and quantile collectors. |
| 62 | [orjson](https://github.com/ijl/orjson) | Compare Rust/Python JSON encoding, decoding, and output types. |
| 63 | [simdjson](https://github.com/simdjson/simdjson) | Explore batched JSONL parsing and field access on demand. |

## Commercial engines and services, 64–73

These entries are references to public documentation. They do not establish
access to proprietary implementations or authorize use of a hosted service.

| No. | Reference | Proposed use |
| --- | --- | --- |
| 64 | [Databricks Photon, a proprietary engine](https://docs.databricks.com/gcp/en/compute/photon) | Compare documented vectorization coverage, fallback reasons, and diagnostics. |
| 65 | [Tableau Hyper, a proprietary engine](https://www.tableau.com/developer/learning/tableau-hyper-api) | Compare batch inserts, type conversion, and embedded analytics interfaces. |
| 66 | [Snowflake / Snowpark, a commercial backend with open-source clients](https://docs.snowflake.com/en/developer-guide/snowpark/python/pandas-on-snowflake) | Compare lazy pushdown, UDF costs, and local/backend switching. |
| 67 | [MotherDuck, a commercial service based on DuckDB](https://motherduck.com/blog/announcing-motherduck-duckdb-in-the-cloud/) | Study hybrid local/remote plans and input transfer costs. |
| 68 | [Coiled, a commercial service based on Dask](https://docs.coiled.io/blog/fine-performance-metrics.html) | Compare separate compute, transfer, and spill times in reports. |
| 69 | [Anyscale, a commercial service based on Ray](https://docs.anyscale.com/monitoring/workload-debugging/data-dashboard) | Compare backpressure, memory-failure diagnostics, and batch task status. |
| 70 | [Polars Cloud, a commercial service with an open-source Polars core](https://docs.cloud.pola.rs/polars-cloud/run/query-profile/) | Compare stage graphs and CPU, I/O, and shuffle diagnostics. |
| 71 | [SingleStore, a proprietary engine](https://docs.singlestore.com/cloud/query-data/advanced-query-topics/code-generation/) | Study interpretation before compilation and parameterized plan caching. |
| 72 | [Actian Analytics Engine, formerly Vector, a proprietary engine](https://www.actian.com/databases/analytics-engine/) | Compare cache-friendly vectors, column compression, and block pruning. |
| 73 | [CedarDB, a commercial engine](https://cedardb.com/docs/technology/) | Study tiered compilation, buffer management, and specialized hash joins. |

## Additional reference: StreamEx

[StreamEx](https://github.com/amaembo/streamex) extends Java's Stream API.
The initial comparison used its
[cheatsheet](https://github.com/amaembo/streamex/blob/streamex-0.8.4/wiki/CHEATSHEET.md)
and selected implementations in the `streamex-0.8.4` tag. This supplements the
original 73 entries without renumbering them. Selected code was read in
[`StreamEx`](https://github.com/amaembo/streamex/blob/streamex-0.8.4/src/main/java/one/util/streamex/StreamEx.java)
and [`MoreCollectors`](https://github.com/amaembo/streamex/blob/streamex-0.8.4/src/main/java/one/util/streamex/MoreCollectors.java).
No upstream code was executed or copied into fpstreams.
The [0.9.0 release notes](https://github.com/amaembo/streamex/releases/tag/streamex-0.9.0)
were also checked. They add nullness annotations and null-terminated helpers,
and report a fix for filtering entries with null keys or values. The source
comparison below remains pinned to 0.8.4.

| StreamEx feature | fpstreams comparison and next question |
| --- | --- |
| `pairMap`, `mapFirst`, `mapLast`, `scanLeft` | Existing `pairwise().map(...)`, `map_first`, `map_last`, and `scan` cover related uses. Check callback order and allocation costs before adding shortcuts. |
| `groupRuns`, `collapse`, `runLengths` | `group_runs(key)` groups equal adjacent keys; `collapse(predicate, merger)` supports pair predicates. These have different contracts. The unreleased Collector overload reduces runs without a raw run buffer. |
| `collapse(predicate, collector)` | Existing `Rows.group_by_sorted(...).aggregate(...)` covers ascending supported keys. The unreleased `collapse` overload covers arbitrary adjacent predicates and async pipelines through the same Collector state program. Memory depends on the Collector's state; a run needs a boundary or source exhaustion to emit. |
| `MoreCollectors.reducingWithZero` | Compare with the existing `Collector.done` protocol. An absorbing value requires an explicit user contract; ordinary reducers cannot safely infer early termination. |
| `headTail` | Study iterator ownership, replay, and cleanup before exposing a live tail. Existing `unfold`, `map_accum`, and Gatherer already cover many stateful transformations. |

For example,
[`scanLeft(seed, accumulator)`](https://github.com/amaembo/streamex/blob/streamex-0.8.4/src/main/java/one/util/streamex/AbstractStreamEx.java#L1647-L1684)
is a terminal operation that includes the seed in its result list. fpstreams
`scan(initial, function)` is lazy and emits only states produced from input items.
Copying the Java name would obscure that difference.

These are API comparisons, not a claim of semantic equivalence or measured
performance. The [unreleased Collector overload](reference/flow.md#reduce-adjacent-runs-with-a-collector-unreleased)
preserves the existing binary merger form without adding a new method name. Java
parallel streams do not establish fpstreams support for distributed execution,
event-time windows, or watermarks.

## Review notes

The [roadmap](roadmap.md) records earlier comparisons and experiments separately.
An existing related feature does not mean that every source in this catalog has
been reviewed. The [six previously queued repositories](roadmap.md#references-queued-on-october-17)
remain deferred.

### October 17: prefix counts and iterator boundaries

Initial reading of reference 43's [iterator recipes](https://more-itertools.readthedocs.io/en/stable/api.html#more_itertools.take)
led to a check of fpstreams prefix operations. The Python path rejected floats
only during execution, while the async path accepted them: `take(2.5)` emitted
three items and `take(float("nan"))` emitted the entire finite input.

The unreleased fix normalizes `take`, `limit`, `drop`, and `skip` counts once
through the [integer index protocol](https://docs.python.org/3.12/library/operator.html#operator.index)
when constructing the operation. Both APIs accept integer-index objects and
reject floats before opening a source. Integer behavior, zero-count boundaries,
iterator cleanup, and existing execution paths remain subject to regression
checks. No upstream implementation was copied or executed.

Reference 45's [iterator-scoping guide](https://asyncstdlib.readthedocs.io/en/stable/source/notes/iter_scope.html)
distinguishes owning an iterator from borrowing it for several operations.
fpstreams already closes owned async iterators on early termination. This
comparison preserves that default; a borrowing API remains a separate candidate.
The initially attempted iterator API URL was unavailable, so the guide was
reached through the documentation index instead.

### Versioned paper follow-up

Metadata and abstracts were checked for references
[2](https://arxiv.org/abs/1612.06668v1),
[3](https://arxiv.org/abs/2412.15768v2),
[18](https://arxiv.org/abs/2505.04153v2), and
[19](https://arxiv.org/abs/2009.13768v1). Their supplied titles match the papers,
including abbreviated titles for 3 and 18. These checks are not full-paper reviews.

The streaming model and scope sections of reference 3 describe an acyclic,
single-consumer compilation model with declared state. Its allocation guarantee
depends on individual operators avoiding calls and allocations. For fpstreams,
our inference is to evaluate fusion within proven expression paths first, while
preserving observable Python callbacks, exceptions, and iterator cleanup.
The paper does not establish a speedup or unrestricted fusion for this library.

Reference 19 assumes an associative aggregate operation. A local probe using the
existing reducer law checker accepted a small exact-integer sum sample and
rejected the corresponding floating-point counterexample. This does not prove
integer laws for arbitrary user reducers. DABA remains a candidate requiring an
explicit algebraic contract; it cannot silently replace arbitrary window
callbacks or change initial partial-window behavior.
