# API index

Use this page to find an operation by task. Detailed signatures, parameter
defaults, return types, and exceptions are rendered on the linked class pages.
The leftmost name in each row is the canonical spelling; aliases are listed
separately so search results do not imply several different behaviors.

## Construct a pipeline

| Task | Entry point | Notes |
| --- | --- | --- |
| Synchronous values or records | `flow(source)` | Primary synchronous constructor; routes supported tabular inputs |
| Fresh source per execution | `flow.defer(factory)` | Use for reopenable files, queries, or iterator factories |
| Positional values | `flow.of(*items)` | Reusable tuple-backed source |
| Optional single value | `flow.of_nullable(value)` | Empty for `None`, otherwise one item |
| Repeated state transition | `flow.iterate(seed, function)` | Infinite until bounded downstream |
| Repeated supplier | `flow.generate(supplier)` | Infinite until bounded downstream |
| Async values | `aflow(source)` | Accepts async or supported synchronous sources |
| Key/value values | `pairs(source)` | Explicit pair pipeline |
| Explicit record view | `rows(source)` | Relational and record-I/O namespace |
| Explicit named columns | `flow.from_columns` / `rows.from_columns` | Equal-length columns retained through Arrow |
| Explicit NumPy values | `flow.from_numpy` | One-dimensional scalars or two-dimensional named records |
| Explicit NumPy records | `rows.from_numpy` | Two-dimensional array retained as named, replayable records |

Tabular constructors are listed in [data input and output](io.md).

## Transform and filter Flow values

| Category | Canonical methods |
| --- | --- |
| One-to-one transform | `map`, `map_first`, `map_last`, `pair_map`, `tap` |
| Zero-or-one transform | `filter`, `reject`, `filter_map`, `filter_none`, `compact` |
| One-to-many transform | `flat_map`, `collapse` |
| Prefix/suffix bounds | `take`, `drop`, `take_while`, `take_while_inclusive`, `drop_while` |
| Stateful transform | `scan`, `scan_right`, `fold_by`, `reduce_by`, `gather` |
| De-duplication | `unique`, `unique_by`, `distinct`, `distinct_by` |
| Error/value partitioning | `attempt`, `partition_results` |
| Engine/concurrency view | `with_engine`, `parallel`, `sequential`, `map_parallel` |

See [Flow](flow.md) for complete signatures.

## Batch, window, and order

| Category | Canonical methods |
| --- | --- |
| Fixed grouping | `chunk`, `window`, `pairwise`, `batch` |
| Conditional grouping | `chunk_by`, `batch_by_size`, `constrained_batches`, `group_runs`, `collapse` |
| Stable order | `sort_by`, `sorted`, `external_sort_by`, `top`, `bottom` |
| Position | `enumerate`, `zip_with_index`, `map_first`, `map_last` |
| Separation | `intersperse` |

## Combine pipelines

| Shape | Methods |
| --- | --- |
| Sequential | `append`, `prepend`, `concat` |
| Position-wise | `zip`, `zip_longest` |
| Cartesian | `cross`, `cartesian` |
| String terminal | `join(separator)` |

AsyncFlow adds `merge`, `combine_latest`, `flat_map_merge`, `merge_map`, and
`switch_map` for concurrent sources.

## Scalar and collection terminals

| Category | Terminals |
| --- | --- |
| Cardinality/truth | `count`, `count_by`, `any`, `all`, `none` |
| Element lookup | `first`, `last`, `nth`, `find`, `find_index`, `index_of` |
| Numeric | `sum`, `min`, `max`, `minmax`, `mean`, `average`, `variance`, `std` |
| General reduction | `fold`, `fold_right`, `reduce`, `reduce_right`, `aggregate`, `summarize`, `collect` |
| Distribution | `frequencies`, `partition`, `describe` |
| Containers | `to_list`, `to_tuple`, `to_set` |
| Data systems | `to_numpy`, `to_pandas`, `to_csv` |
| File/effect output | `to_json`, `for_each`, iteration |
| Planning and observation | `explain`, `run_with_report` |

Terminals that require an element document their empty-input policy. Statistical
terminals return `None` where the statistic is undefined rather than inventing a
value.

## Work with records

| Task | Rows methods |
| --- | --- |
| Choose or derive columns | `select`, `with_columns`, `rename`, `drop`, `cast`, `parse` |
| Filter | `filter`, `where` |
| Null handling | `fill_nulls`, `drop_nulls` |
| Group | `group_by(...).aggregate(...)`, `aggregate` |
| Join | `join` |
| Concatenate sources | `concat` |
| Ordering/deduplication | `sort_by`, `external_sort_by`, `unique_by`, `distinct_by` |
| Reshape | `explode`, `unnest`, `unpivot`, `pivot` |
| Row materialization | `to_list`, `to_columns`, `first`, `last`, `count` |
| Flow view and planning | `to_flow`, `explain` |
| NumPy/Arrow/dataframes | `from_columns`, `from_numpy`, `to_numpy`, `arrow_batches`, `polars_batches`, `to_arrow`, `to_pandas`, `to_polars` |

See [Rows](rows.md), [expressions](expressions.md), and [I/O](io.md).

## Aggregate once

`agg` creates aggregators for `Rows.aggregate`, `GroupedRows.aggregate`, and
multi-reduction programs.

| Result | Factories |
| --- | --- |
| Counts | `agg.count`, `agg.count_where`, `agg.count_distinct` |
| Numeric | `agg.sum`, `agg.mean`, `agg.variance`, `agg.std`, `agg.min`, `agg.max` |
| Encounter order | `agg.first`, `agg.last` |
| Truth | `agg.any`, `agg.all` |
| Collection | `agg.collect` |

Collector factories in `Collectors` cover general iterable reduction, mapping,
filtering, partitioning, grouping, joining, and downstream composition. Reducer
objects describe associative laws when parallel or tree reduction needs explicit
proof.

## Transform pairs

| Task | Pairs methods |
| --- | --- |
| Transform | `map_keys`, `map_values`, `map_pairs`, `flat_map_pairs`, `tap` |
| Filter | `filter_keys`, `filter_values`, `filter_pairs` |
| De-duplicate/order | `unique_keys`, `sort_by_key`, `sort_by_value` |
| Per-key values | `group_values`, `collect_values`, `aggregate_values` |
| Views and execution policy | `keys`, `values`, `items`, `invert`, `to_flow`, `with_engine` |
| Terminal | `to_dict` |

See [Pairs](pairs.md).

## Async operations

AsyncFlow shares most synchronous transformations and the count, truth,
lookup, fold, frequency, sum, extreme-value, mean, variance, and
standard-deviation terminals. It does not currently expose `describe`. Its
async-only groups are:

| Task | Methods |
| --- | --- |
| Concurrent callbacks | `map_async`, `flat_map_merge`, `merge_map`, `switch_map` |
| Multiple async sources | `merge`, `combine_latest` |
| Rate and time | `delay`, `interval`, `spaceout`, `throttle`, `debounce`, `timeout` |
| Time-bounded buffers | `batch_timeout`, `buffer_timeout` |
| External sources | `from_file`, `paginate` |

Concurrency, ordering, cancellation, and buffer bounds are documented with each
method on [AsyncFlow](async_flow.md).

## Expressions

| Namespace | Purpose |
| --- | --- |
| `item` | Integer/scalar expression with Python integer semantics |
| `fitem` | Floating expression and native floating kernels |
| `col(name)` | Read a record column or path |
| `lit(value)` | Embed a literal in a row expression |
| `when(condition, value)` | Conditional row expression builder |
| `coalesce(*values)` | First non-`None` row expression |

See the full operator and precedence notes under [expressions](expressions.md).

## Values, errors, and runtime controls

| Area | Public names |
| --- | --- |
| Optional/result values | `Option`, `Result`, `Ok`, `Err` |
| Statistics | `SummaryStatistics` |
| Execution reports | `ExecutionResult`, `ExecutionReport` |
| Spill budgets | `SpillLimits` |
| Collection laws | `Reducer`, `ReducerAggregator`, `ReducerLaws`, `ReducerLawError`, `LawProvenance`, `ReductionExplanation` |
| Error base | `FlowError` |
| User-facing errors | `FlowConsumedError`, `EmptyFlowError`, `SelectionError`, `DuplicateKeyError`, `NativeUnsupportedError`, `BufferLimitError` |

See [Option and Result](containers.md) and [errors and runtime values](errors.md).

## Canonical names and aliases

Aliases exist for migration or familiarity. New documentation uses the canonical
name in the left column.

| Canonical | Aliases |
| --- | --- |
| `take` | `limit`, `head` (Rows) |
| `drop` | `skip`, `offset` (Rows) |
| `filter` | `where`; `reject` negates the predicate |
| `unique` / `unique_by` | `distinct` / `distinct_by` |
| `mean` | `average` |
| `map_parallel` | `parallel_map` |
| `to_numpy` | `to_np` |
| `to_pandas` | `to_df` |
| `fill_nulls` | `fillna` (Rows) |
| `drop_nulls` | `dropna` (Rows) |
| `from_dataframe` | `from_pandas` |
| `Flow` | `Stream`; `ParallelStream` is a compatibility alias |
| `AsyncFlow` | `AsyncStream` |

Aliases have the same execution semantics unless their signature explicitly
narrows the canonical method.
