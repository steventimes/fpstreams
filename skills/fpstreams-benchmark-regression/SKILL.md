---
name: fpstreams-benchmark-regression
description: Validate fpstreams benchmark comparability and replay release regression checks before evaluating a performance change. Use for benchmark baselines, native build failures, and measured optimization in this repository.
---

# fpstreams benchmark 回归

从仓库根运行。先看 `AGENTS.md` 的当前检查点和 `CONTRIBUTING.md` 的兼容性
边界；不把前次结果当作本次 baseline。

先确认本地 native 扩展可导入；已安装构建环境但缺扩展时：

```bash
.venv/bin/python -m maturin develop --release --offline
.venv/bin/python -m pytest -q tests/test_release.py
```

缺依赖时按 CONTRIBUTING.md 恢复锁定环境；不要跳过 native 用例或用 debug
扩展的结果声称 release 性能。性能实验前停止其他重型检查，避免机器负载污染。

分配测量使用已修补的解释器。本机 CPython 3.12.3 会触发
[tracemalloc.stop() 与原生线程初始化的竞争](https://github.com/python/cpython/issues/128679)；
无 fpstreams/Arrow 的独立探针也会崩溃。CPython 3.12.13 的独立探针及文件扫描
压力检查通过。保留 tracing 和原门槛，记录完整解释器版本；换解释器后 base/head
一起重跑，不能与旧环境的报告混比。

比较前核对报告 schema、场景集合、规模、domain、quick、matrix 和 native
profile，以及 CPU affinity、NumPy CPU dispatch 和报告中的运行设置。当前报告
schema 为 6，competitive 样本须保留预热次数；不要给旧报告补字段冒充新测量。
schema 6 要求记录 Python/glibc 分配器环境设置；旧报告须重测，不能补空值。
环境字段记录请求值，不证明实际启用的分配器或相同的分配历史。不要为通过门槛
偷偷预分配内存或筛掉慢样本。
提交 SHA、包版本和代码 hash 用于溯源，不要求 base/head 代码相同。
两套报告现在都要求 `resources.peak_allocation_bytes`，来自计时后的独立
`tracemalloc` 调用。它不包括预先构造的输入和未被 Python 追踪的原生分配，
不能等同于 RSS。旧 schema 4 competitive 没有该指标，不能据其比较通过声称
内存门槛也通过；需要重跑。baseline 不能把缺失值补成零。
无可比较的历史基线时明确标注本次局部实验，不能声称解决跨版本回归。

沿用 `run_benchmark.sh` 的参数和现有 `benchmarks/regression.py`，输出写到
被忽略的 artifacts；不要临时改默认参数后遗留。优化以一个共享瓶颈为单位，
保留现有 fallback 与 source consumption 语义；在相关输入形状和多个规模上复测。
负向覆盖放进现有 `tests/test_release.py`，不为每个案例增加顶层测试文件。

只有本次回归和可比较 benchmark 都提供证据时才保留性能修改。发布、推送与
正式 baseline 审批是独立任务，不由一次本地性能工作自动授权。
