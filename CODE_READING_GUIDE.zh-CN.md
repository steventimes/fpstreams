# fpstreams 代码阅读指南

这份指南不按目录逐个介绍文件，而是跟着一条查询真正经过的路径来读。同步代码先从 `Flow` 开始；记录数据仍由 `Rows` 实现关系操作，但用户不必先换入口。读懂这条主线以后，再看异步和 Rust。直接从 `rust/` 或外排代码开始，很容易只看到局部循环，不知道它们在整个查询里负责什么。

## 先记住一张图

```text
用户调用
  │
  ▼
Flow / AsyncFlow                    只描述“想做什么”
  │
  ├─ Rows view                     为记录数据补充关系操作
  │
  ▼
LogicalPlan                         保存尚未执行的逻辑树
  │
  ├─ semantic analyzer             检查无限流、顺序、基数等约束
  │
  ▼
compiler                            按查询形状和数据能力选执行办法
  │
  ▼
PhysicalPlan                        记录 Python / Rust / Arrow / relation 策略
  │
  ▼
executor                            打开数据源并执行；这里才真正读取数据
  │
  ▼
terminal                            to_list、count、sum、first……
```

最重要的边界是：构建逻辑计划时不能偷读数据源。`flow(iterator)` 可能是一次性的，编译器如果为了猜类型先取一项，用户真正执行时就会少一项。

## 第一次阅读：跟一条普通 Flow 走到底

先用这段查询当路标：

```python
from fpstreams import flow, item

query = flow(range(10)).filter(item % 2 == 0).map(item * 3).take(2)
result = query.to_list()
```

按下面的顺序打开文件。

1. `src/fpstreams/__init__.py`

   这里只看公开名字。先知道用户能拿到 `flow`、`rows`、`aflow`、`item`、`col`、`agg` 等什么东西，不必追每一个兼容别名。

2. `src/fpstreams/streams/flow.py`

   找 `Flow.__init__`、`_append`、`map`、`filter` 和 `take`。这些方法不会遍历数据，而是返回带有新逻辑节点的 `Flow`。留意 `__iter__`：真正迭代时，它会编译并执行计划。

3. `src/fpstreams/planning/sync.py`

   这里定义 `MapOp`、`FilterOp`、`TakeOp` 等操作值。它们是“操作的描述”，不是执行循环。读到这里应该能分清：`Flow.map()` 负责建计划，`execution` 才负责逐项调用 map 函数。

4. `src/fpstreams/planning/source.py`

   重点看 `Source.from_iterable()`、`open()` 和 source capabilities。列表可以重复打开，迭代器只能消费一次，`range` 还带有精确长度和原生数据能力。很多优化是否安全，取决于这些事实。

5. `src/fpstreams/planning/logical.py`

   先读 `SourceNode`、`UnaryNode`、`LogicalPlan` 和 `Query`。普通流水线是一棵从 source 往外包的不可变树。`linear_pipeline()` 会把只有一条链的树摊平成 `source + operations`；join 和 group 是分叉或关系节点，不能这样摊平。

6. `src/fpstreams/planning/compiler.py`

   从 `compile_query()` 开始，不要立刻钻进所有辅助函数。先看它怎样区分线性查询和关系查询，再看 `_compile_node()` 怎样把表达式、排序和普通 Python 操作变成 physical node。计划缓存也在这一层，实现在 `planning/plan_cache.py`。

7. `src/fpstreams/physical/plan.py`

   这里的类是编译结果。`RowPhysicalNode` 表示普通 Python 行操作，`CompiledExpressionPhysicalNode` 带着编译好的表达式，`SortPhysicalNode` 保存排序策略。`PhysicalPlan` 还保存后端选择理由，供执行器和 `explain()` 使用。

8. `src/fpstreams/execution/physical.py`

   看 `execute_physical()`。它是同步执行的总入口：关系树交给 relational executor，线性计划按编译器已经选好的 payload 执行。它还创建并关闭 `QueryRuntime`，所以异常和提前结束时资源也要从这里收回来。

9. `src/fpstreams/execution/sync.py` 和 `sync_ops.py`

   这里终于能看到数据逐项流动。`sync.py` 负责相邻操作的执行和融合，`sync_ops.py` 放具体 iterator 实现。读 `take` 时注意它怎样停止拉取上游；这正是流式库相对一次性数组计算很有价值的地方。

10. `src/fpstreams/streams/flow_terminals.py`

    最后看 `to_list()`、`count()`、`sum()`、`first()` 等终端。终端会把“这次到底要算什么”交给 compiler。线性计划可以走精确长度或 native 快路，关系计划必须执行完整 physical root，不能退化成只读左侧 source。

走完这十步，再回头看 `query.explain(terminal="list").to_dict()`。此时输出里的 selected engine、复杂度、数据搬运和拒绝原因会和代码对应起来。

## 第二次阅读：表达式为什么比 lambda 更容易优化

用这两种写法比较：

```python
flow(values).map(lambda x: x * 3 + 1)
flow(values).map(item * 3 + 1)
```

lambda 是不透明的 Python 回调。`item * 3 + 1` 则是一棵可以检查和编译的表达式树。

建议顺序：

1. `expressions/scalar.py`：`item` 和 `fitem` 怎样用运算符重载建立整数、浮点表达式。
2. `expressions/row.py` 与 `row_ir.py`：`col("price") * col("count")` 的记录表达式节点。
3. `expressions/typed_ir.py`：编译前保存的保守 effect 和来源信息。遇到 Python UDF 时，优化器必须承认它可能有副作用。
4. `expressions/program.py`：把公开表达式变成可执行 program。
5. `expressions/_row_codegen.py` 与 `execution/_rows_fusion.py`：对安全的 exact-dict 记录链生成并融合 Python 代码；遇到自定义 Mapping、Path、UDF 等边界时回到规范解释路径。

这里不要把“相同表达式”想成普通字符串替换。literal 可能保存有身份的 Python 对象，callback 也可能有状态。缓存必须避免把上一次查询绑定的对象带进下一次查询。

## 第三次阅读：Flow 的记录视图、join 和 group

同步入口仍然是 `flow(...)`。普通值继续使用 `map`、`filter` 和终端；记录值可以直接调用 `select`、`with_columns`、`group_by` 等方法。这些方法会建立一个共享原计划和数据源所有权的 `Rows` 视图，不会检查或预读第一行：

```python
from fpstreams import col, flow

paid = (
    flow([{"customer": "A", "amount": 12}, {"customer": "B", "amount": 5}])
    .where(col("amount") >= 10)
    .select("customer", "amount")
)
```

`Rows` 仍是公开 API。`flow(records).rows()` 可以显式取得关系视图，原来的 `rows(...)` 工厂也继续兼容旧代码。下面四个关系操作由接收者决定语义：

| 调用 | `Flow` 语义 | 显式 `.rows()` 语义 |
| --- | --- | --- |
| `drop` | 跳过前 N 项 | 删除记录字段 |
| `join` | 把值连接成字符串并立即返回 | 对两个记录源做关系 join |
| `aggregate` | 立即返回 collector 结果字典 | 返回惰性、仅一行的 `Rows` |
| `where` | 只接收 predicate，是 `filter` 的别名 | 还可接收字段相等条件 |

这张表不是全部同名方法清单。`Flow` 也保留自己的输出接口；如果要传 Rows 版本的 `fieldnames`、`batch_size` 或 `schema` 等参数，先进入 `.rows()`，再调用 `to_csv()`、`to_pandas()` 或 `to_df()`。

例如关系 join 要写成 `flow(left).rows().join(right, on="id")`，字段条件筛选可写成 `flow(records).rows().where(status="paid")`。这种显式写法比按运行时参数猜含义更稳，也不需要消费一次性迭代器。

`flow(...)` 会自动识别已经加载的 pandas、Polars 和 PyArrow 对象，也接受实现标准 `__arrow_c_stream__` 或 `__dataframe__` 协议的对象。一个对象同时实现两种协议时优先 Arrow。普通生成器和内建容器不会被协议探测；二维 `list` 仍可以直接调用记录方法，例如 `flow([[1, "A"]]).select(0, 1)`。

实现从 `src/fpstreams/tabular/rows.py` 开始读。`Rows` 是面向记录的关系视图，底下仍持有同一个 `Flow` 和逻辑计划。

接着读：

- `tabular/records.py`：字典、dataclass、named tuple 和对象怎样变成记录。join 会在调用用户 key 前做浅拷贝，避免 key 的副作用污染已经确定的输出快照。
- `expressions/selectors.py`：字符串字段、整数下标、路径和 callable 如何统一成 selector。exact dict 的常见字段读取有快路，其他对象保留原协议行为。
- `tabular/join.py`：内存 join 的稳定输出顺序、字段冲突和 cardinality validation。右侧
  hash index 保存的是 slot 位置；同一个 key 第二次出现时，只在 slot 数组里把单条记录
  换成列表，不会重新向 hash index 写入用户的 key。这个细节是为了避免额外调用自定义
  `__hash__` 或 `__eq__`。固定 exact-string schema 的字段冲突分析会缓存成蓝图；真正生成的
  suffix 字符串仍然逐输出创建，所以键对象身份没有被缓存偷换。
- `tabular/grouped.py`：`group_by(...).aggregate(...)` 怎样建立关系节点。
- `physical/relational.py`：join/group 的物理策略值。
- `execution/relational/`：`__init__.py` 负责关系树和通用 group state，`join.py` 负责
  内存 hash join，`arrow_group.py` 与 `arrow_global.py` 处理列式聚合。精确字段的单键循环和
  Rust tuple/record 快路也从这个包分派；记录太宽、字段形状复杂或类型混杂时会回到 Python。

`Rows.to_list()` 还有一条很窄的 record join 快路：两边必须是可重复打开的 exact
list/tuple，记录是窄 exact dict，连接字段是 exact integer，而且右键实际唯一。Rust 会先完整
证明这些条件，再生成结果；任一条件不成立就从未打开的 source 走原 Python join。普通迭代、
`first()` 和提前关闭仍使用流式执行，不会被 eager 快路接管。

Arrow 也采用同样的保守边界。`Rows.from_dataframe()` 和 `Rows.from_polars()` 会同时保留
规范 row opener 与惰性 `ArrowBatchSource`；普通迭代和强制 Python 仍走前者，自动物化终端
才能选择后者。单独一个 exact 直接字段 `select`，以及一个严格闭合的“直接 primitive 字段
比较 → 直接 select”，可以在 `planning/arrow.py` 得到批计划，由 `execution/arrow.py` 先过滤、
裁列，再转成 Python 记录。算术、null、literal 子类、路径 selector、Mapping 或用户回调仍在
打开 source 前回到规范路径。重点读这里的“证明与拒绝条件”；Arrow compute 调用本身很短。

读 join 时可以一直问三个问题：build 哪一侧、输出顺序是否稳定、用户 selector 在哪一刻被调用。只看 hash 表代码会漏掉后两个语义约束。

## 第四次阅读：外排、资源上限和 LSM 思路

数据放不进内存时，入口在 `tabular/spill.py` 和 `execution/sorting.py`。推荐按这个顺序：

1. `tabular/spill_limits.py`：分区、单 key 匹配数、输出行数和 repartition 深度的硬上限。
2. `runtime/limits.py`、`files.py`、`resources.py`：任务和文件描述符如何记账，为什么 close 必须幂等，而且清理异常不能盖住真正的计算异常。
3. `storage/codec.py` 与 `spill_store.py`：磁盘 frame、读写、临时目录和 failpoint 边界。
4. `tabular/spill_io.py`：分区 writer、受文件数限制的多代合并。
5. `tabular/spill.py`：Grace hash join、spill group 和稳定结果合并。
6. `execution/sorting.py`：run 生成、缓存 key、稳定 k-way merge，以及 key 不可序列化时的兼容路径。

显式 `.spill(...).aggregate(count=agg.count())` 还有一个有界的热/冷分流。重复很多次、形状可以
静态证明的扁平记录在内存里只保留 count；冷键和带自定义协议的记录仍按原 frame 写盘。热状态
也会携带原始行数、原始帧字节数和首次位置，所以 repartition 上限与输出顺序不因压缩而改变。
任何 failpoint、非标准 count、嵌套对象或 pickle 身份不稳定的值都会禁用这条快路。

这里借的是 LSM tree 的有序 run 和分代合并思路：文件过多时分代合并，并限制每次
merge 的 fan-in。它只服务于一次查询的临时外排，没有 WAL、后台 compaction、Bloom
filter 或长期层级。

## 第五次阅读：native Rust 是怎样接进来的

先读 Python 侧，再读 Rust：

1. `planning/native.py`：哪些操作能组成完整 native program，什么时候选 Python、native 或 hybrid。这里的检查必须是 O(1) 或并入 Rust 提取，不能为了选后端先用 Python 扫完整个列表。
2. `execution/native.py`：Python physical plan 怎样调用扩展，旧 wheel 缺少新符号时如何安全回退。
3. `src/fpstreams/_native.pyi`：Python 看到的 native ABI；它比直接翻 Rust 文件更适合先建立全局印象。
4. `rust/src/integer.rs`、`float.rs`、`relational.rs`、`relational_fixed.rs` 及其同名子目录：
   数值终端、短路 probe、materialize、聚合、固定 schema group 和窄 record join kernel。
   顶层文件登记 PyO3 接口和共享分派，较长的执行族放在子目录里。
5. `rust/src/lib.rs`：PyO3 导出边界。

native 也有转换成本。把 Python list 转成 Rust `Vec` 需要时间，小数据、原样 list/count
或含 Python 回调的计划通常留在 Python。optimizer 会按查询选择后端。

## 第六次阅读：异步执行

异步部分最好在同步主线读懂后再看：

1. `streams/async_flow.py`：公开 API 和逻辑操作。
2. `planning/async_.py` 与 `physical/async_plan.py`：异步 logical operation 怎样编译成 physical node。
3. `execution/async_scheduler.py`：总调度入口和最外层 iterator 的所有权。
4. `execution/async_map.py`：有界并发、ordered result 和取消。
5. `execution/async_merge.py`：merge、combine_latest、merge_map、switch_map 的 inner 生命周期。
6. `runtime/tasks.py` 与 `resources.py`：task scope、一次关闭和 query 结束时的兜底回收。

异步代码的主要风险是 task/iterator 的所有权，以及正常耗尽、异常、取消和短路时的关闭
责任。阅读每个 `finally` 比只读主循环更重要。

## Collectors 和聚合单独怎么读

如果只想研究一次遍历完成多个统计量，按这个顺序：

1. `collecting/collector.py`：通用 Collector 接口。
2. `collecting/aggregation.py`：`agg.sum()`、`agg.mean()` 等公开聚合器。
3. `collecting/program.py` 与 `aggregate_program.py`：多个 collector 共用一次遍历，以及 native mask 怎样只计算需要的槽位。
4. `collecting/statistics.py`：在线均值、方差和数值稳定性。

`aggregate(count=..., total=..., mean=...)` 不是执行三遍。compiler 先得到 collector program，执行时每个输入只推进一次。

## 测试应该按什么顺序读

测试按职责收在 9 个顶层文件里。不要从 `test_release.py` 开始。

1. `tests/test_core.py`：Option、Result、基础函数式工具。
2. `tests/test_flow.py`：同步 API、终端、表达式和 Python/native 语义。
3. `tests/test_tabular.py`：Rows、selector、join/group、Arrow/SQL 与记录语义。
4. `tests/test_collecting.py`：Collector、Aggregator 和统计。
5. `tests/test_async.py`：公开异步行为。
6. `tests/test_engine.py`：logical/physical/compiler、缓存、后端选择和关系策略。
7. `tests/test_runtime.py`：查询资源、任务、异步调度和 spill store 的生命周期。
8. `tests/test_contracts.py`：同步/异步语义、逻辑计划到物理计划的一致性、公开 API 快照和版本化契约。
9. `tests/test_release.py`：wheel、兼容性、基准与发布门禁。

定位 bug 时，先在前五个文件找公开语义，再去 engine、runtime 和 contracts 看内部约束。release tests 是最后一道边界，不适合拿来学习基本调用流程。

## 三次学习安排

如果一次读不完，可以拆成三次：

- 第一次只跟 `flow(range(...)).filter(...).map(...).to_list()`，读到 `execution/sync.py` 为止。
- 第二次跟 README 里的订单记录示例，重点看 Flow 如何进入 Rows 视图，以及表达式融合、group 和 collector program。
- 第三次再读 spill、async 和 Rust，每次选一个方向，不要三条支线一起展开。

每次都可以用下面几条命令验证理解：

```bash
uv run python -c "from fpstreams import flow; print(flow(range(3)).to_list())"
uv run pytest -q tests/test_flow.py
uv run pytest -q tests/test_tabular.py
uv run pytest -q tests/test_engine.py
uv run pytest -q tests/test_runtime.py tests/test_contracts.py
```

修改 optimizer 或 executor 后，常用的本地检查如下；CI 还会覆盖多 Python
版本、打包和干净环境安装：

```bash
uv run pytest -W error --cov=src/fpstreams --cov-branch --cov-report=term-missing
uv run coverage report
uv lock --check
uv run ruff check src tests scripts benchmarks benchmark.py
uv run ruff format --check src tests scripts benchmarks benchmark.py
uv run mypy src/fpstreams
cargo fmt --manifest-path rust/Cargo.toml -- --check
cargo test --manifest-path rust/Cargo.toml
cargo clippy --manifest-path rust/Cargo.toml --all-targets -- -D warnings
uv run python scripts/build_browser_wheel.py
uv run mkdocs build --strict --config-file fpstreams/mkdocs.yml
```

有一个实用办法：先写出你认为某条查询会生成的 logical nodes、physical nodes 和最终 executor，再用 `explain()` 对答案。能准确说出“什么时候打开 source、谁负责 close”，基本就读懂了这条路径。
