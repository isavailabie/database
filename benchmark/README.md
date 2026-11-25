# 性能评测

本目录提供时序数据库课程项目的可复现性能评测。`runner.py` 复制一份源码快照、构建工作负载程序，
并按 profile 在独立进程中逐个场景运行，结果写入 CSV 和 JSON。
混合场景通过 `--keep` 复用对应的均匀/热点导入库；不同规模、线程变体和轮次隔离。
只选择 mixed 时会自动预载 N 点，导入失败则不运行该 mixed。

| 目录 | 用途 |
| --- | --- |
| `benchmark/` | 评测运行器，负责构建、执行、汇总 |
| `benchmarks/` | 工作负载程序（`tsdb_bench`、`tsdb_mixed_bench`、`tsdb_recover`、`tsdb_design_bench`） |
| `src/`、`include/` | 引擎实现与接口 |

## 运行

使用 Linux 或 WSL2，安装 CMake 3.16+、支持 C++20 的编译器和 Python 3.9+。

```bash
python3 benchmark/runner.py --source "$PWD" --output "$PWD/build/eval-quick" --profile quick
```

每次必须使用新的输出目录，已有结果不会被覆盖。构建采用 Release。

| 档位 | 数据点 N | 混合读写时长 | 重复次数 |
| --- | ---: | ---: | ---: |
| smoke | 100,000 | 10 s | 1 |
| quick | 1,000,000 | 60 s | 1 |
| standard | 10,000,000 | 600 s | 3 |
| full | 100,000,000 | 3600 s | 1 |

`full` 对应作业的容量与持续时间要求，耗时较长（导入 + 混合 + 恢复，通常数小时），
建议挂到后台执行。档位参数可用 `--points`、`--seconds`、`--repeat`、`--threads` 覆盖。

## 场景

| 场景 | 检查或测量的内容 |
| --- | --- |
| `perf_ingest` | 10000 条 Series 顺序追加导入；吞吐、峰值 RSS、磁盘占用、最终刷盘与重开恢复耗时 |
| `perf_ingest_hot` | 8 条热点 Series 承担 50% 写入时的导入表现 |
| `perf_mixed` | 已有 N 点上 4 写 16 读；读写吞吐及 p50/p95/p99、维护计数、过程采样 |
| `perf_mixed_hot` | 每个写线程只写 2 条 Series，压单分片热点路径 |
| `perf_recover` | 写入后不 `Close` 直接退出，再测量重新打开耗时、重放 WAL 字节与恢复点数 |

`--matrix` 会对性能场景额外跑 N/10 规模，并对混合场景额外跑单写线程，用于观察规模与并发的影响。

局部调试：

```bash
python3 benchmark/runner.py --source "$PWD" --output "$PWD/build/eval-one" \
  --profile quick --cases perf_ingest,perf_recover
```

## 查看结果

| 输出文件 | 用途 |
| --- | --- |
| `summary.json` | 整体状态、配置、每个 run 的记录（状态、耗时、日志路径） |
| `metrics.csv` | 每个 (场景, 阶段, 重复) 一行：ops、ops/s、p50/p95/p99、RSS、磁盘、WAL |
| `aggregate.json` | 按 (场景, 阶段, 规模, 线程数) 汇总的中位数与极值 |
| `<case>-p<N>-t<T>-r<R>/run.log` | 单次运行的命令与原始输出 |
| `configure.log`、`build.log` | 构建日志，定位配置或编译失败 |
| 混合场景目录内 `samples.csv` | 当前/峰值 RSS、存活/待回收 SSTable、磁盘与 WAL 的过程采样 |

`--sample-interval` 设置采样间隔（秒，默认 10）；结束时即时唤醒采样线程。
混合输出 `initial_points` 与 `historical_read_points`，证明预载规模及实际读取历史数据。
恢复输出校验所有配置序列，`--expected-points` 要求精确匹配恢复点数；空库和错误返回非零。
读写延迟分位来自每线程最多 50000 个全时段蓄水池样本，采用 nearest-rank。
混合结果合并各线程样本，是近似估计；线程样本上限不表示累计操作数。
`rss_bytes` 在历史 metrics 中仍表示整进程峰值，采样中的 `current_rss_bytes`
才是当前 RSS；驱动与引擎同进程，不能据此宣称严格分离了驱动内存。

报告应给出多轮重复的中位数和波动。吞吐包含驱动侧的数据生成与校验，峰值 RSS 为引擎进程整体
（包括驱动批次、读取校验及混合延迟样本），磁盘占用为数据库目录下全部文件之和。

## 设计级 A/B 对照

`compare.py` 面向基线与候选 Release 二进制，用相同驱动、输入与同步持久化语义执行
AB / BA / AB 三轮交替。不会复用已有输出或数据目录，也不会用失败、缺指标、非有限值、
恢复丢点或混合读写错误的结果生成成功摘要。每次的命令、状态、原始输出和多轮中位数
保存在 `results.json` 与对应日志中。默认七种场景（uniform / hot / append /
compact / scan / narrow / mixed），可用 `--cases` 选择子集。

```bash
python3 benchmark/compare.py \
  --baseline /tmp/tsdb-baseline-build --candidate /tmp/tsdb-candidate-build \
  --output "$PWD/build/perf-ab" --data-root /tmp/tsdb-perf-ab \
  --points 10000000 --hot-points 3000000 --seconds 30 --repeat 3
```

**数据目录必须位于被测 Linux 文件系统**；先用 `stat -f -c %T /tmp` 验证，不能假设
`/tmp` 总是磁盘，也不要把宿主共享挂载与 Linux 容器数据层混在同一组比较中。
测试时不并行构建、跑回归或启动其他负载。交替顺序降低但不能完全消除磁盘缓存、
fsync、后台维护和温度漂移的影响；需要同时报告每轮结果，不宣称统计显著性。

| 场景 | 驱动与口径 |
| --- | --- |
| uniform / hot | 单写线程、1000 点/批、10000 Series；`seconds` 包括写入及最终 drain；热点为 8 Series 占 50% |
| append | 单热点驻留追加，最多 100 万点，禁后台合并；`append_seconds` 只测 WriteBatch 循环 |
| compact | 单热点每 10 万点 Flush、禁后台合并；单独计时显式 Compact，随后逐点校验完整扫描和重开 |
| scan | 单热点 Flush 后完整扫描三遍；不是点查，也不是冷缓存随机 I/O |
| narrow | 单热点一次性 Flush 成单个 SST / 单个 series chunk，执行 800 次独立的 128 点尾部窄窗口查询；用于把页索引的定位成本从噪声中分离出来 |
| mixed | 每版本先预载 N 点，再在同库 4 写 16 读，逐条扫描指定 Series；报告读写吞吐与 p95/p99 |

`narrow` 的查询次数由 `compare.py` 的 `kNarrowQueries = 800` 固定；每次查询逐点
校验 Series、timestamp、value 且必须恰好命中 128 点，`scan_points` 期望值为
`repeats × 128`，不是 `points × repeats`。窗口起点沿系列后半段按 997 点步长推进，
因此它测的是「长 series chunk 里跳过大量不可能命中的页」，而不是磁盘 I/O 吞吐。
`compare.py` 会把该场景的主指标记为 `scan_seconds`。

`tsdb_design_bench append|compact|scan|narrow points path repeats` 拒绝已有数据库，
正常与恢复扫描均校验每点的 Series、timestamp、value 和总点数；`narrow` 关闭后重开
还会逐点校验全部点数。`rss_bytes` / `rss_scope`
表示含写入、扫描、关闭和恢复阶段的整进程峰值，不是合并步骤单独的净内存。
`recover_seconds` 只测 Open；导入驱动的 `recovery_seconds` 包含 Open 和全库恢复扫描，
两者不能直接比较。`speedup_percent` 为基线耗时/候选耗时减一，或候选吞吐/基线吞吐
减一；**速度提升百分比不等于耗时减少百分比**。

设计级驱动必须用同一份 `benchmarks/design.cpp` 分别链接两个版本的库。基线源码没有
该新驱动时，可在其构建后手动编译，具体命令见
[`doc/性能优化-2026-11-25.md`](../doc/性能优化-2026-11-25.md)。该报告及其原始指标为
上一轮数据，不能与历史 WSL2 或 macOS 报告拼接成同一实验。

`c99b8ab` 之后的持续迭代（含 `narrow` 场景的引入、三轮 AB/BA、五轮复测、
1 亿点库 60 分钟混合验收与收敛判据）见
[`doc/性能优化-迭代-2026-11-25.md`](../doc/性能优化-迭代-2026-11-25.md)，
原始指标为 `performance-results-round1-*.json`。每轮实验各自独立，不与上一轮混算。
