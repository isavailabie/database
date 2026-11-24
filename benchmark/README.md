# 性能评测

本目录提供时序数据库课程项目的可复现性能评测。`runner.py` 复制一份源码快照、构建工作负载程序，
并按 profile 在独立进程中逐个场景运行，结果写入 CSV 和 JSON。
混合场景通过 `--keep` 复用对应的均匀/热点导入库；不同规模、线程变体和轮次隔离。
只选择 mixed 时会自动预载 N 点，导入失败则不运行该 mixed。

| 目录 | 用途 |
| --- | --- |
| `benchmark/` | 评测运行器，负责构建、执行、汇总 |
| `benchmarks/` | 工作负载程序（`tsdb_bench`、`tsdb_mixed_bench`、`tsdb_recover`） |
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
恢复输出校验所有配置序列，`--expected-points` 可要求最低恢复点数；空库和错误返回非零。
读写延迟分位来自每线程最多 50000 个全时段蓄水池样本，采用 nearest-rank。
混合结果合并各线程样本，是近似估计；线程样本上限不表示累计操作数。
`rss_bytes` 在历史 metrics 中仍表示整进程峰值，采样中的 `current_rss_bytes`
才是当前 RSS；驱动与引擎同进程，不能据此宣称严格分离了驱动内存。

报告应给出多轮重复的中位数和波动。吞吐包含驱动侧的数据生成与校验，峰值 RSS 为引擎进程整体
（驱动只保留一个小批次），磁盘占用为数据库目录下全部文件之和。
