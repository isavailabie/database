# 时序数据库课程项目

## 构建

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel 4
ctest --test-dir build --output-on-failure
```

未指定 `CMAKE_BUILD_TYPE` 时默认使用 Release（见 `CMakeLists.txt`），基准数字
一定要对应 Release 构建。

## 测试

`ctest` 运行两组用例：`tsdb_tests`（功能、崩溃恢复、并发压缩与 GC）和
`tsdb_fault_test`（50 次文件级故障注入）。

```bash
./build/tsdb_tests              # 全部用例，逐条打印名称与耗时
./build/tsdb_tests compaction   # 只跑名字包含 compaction 的
./build/tsdb_tests 13           # 只跑第 13 个
```

## 基准

可复现的评测由 `benchmark/runner.py` 驱动，目录结构与课程 benchmark 一致：
复制源码快照、Release 构建、每个场景在独立进程与独立数据库中运行，结果写入
`metrics.csv`、`aggregate.json` 和 `summary.json`。

```bash
bash scripts/run_bench.sh quick     # 100 万点，约 1–2 分钟
bash scripts/run_bench.sh full      # 1 亿点 + 60 分钟混合，数小时
```

也可以直接调用运行器：

```bash
python3 benchmark/runner.py --source "$PWD" --output build/eval-quick --profile quick
```

| 档位 | 数据点 N | 混合读写时长 | 重复次数 |
| --- | ---: | ---: | ---: |
| smoke | 100,000 | 10 s | 1 |
| quick | 1,000,000 | 60 s | 1 |
| standard | 10,000,000 | 600 s | 3 |
| full | 100,000,000 | 3600 s | 1 |

场景包括均匀导入、热点导入（8 条 Series 占 50% 写入）、4 写 16 读混合、
热点混合和崩溃恢复；`--matrix` 会额外跑 N/10 规模与单写线程。各场景的指标口径
见 `benchmark/README.md`。

工作负载程序也可以单独运行：

```bash
./build/tsdb_bench 1000000 --series 10000
./build/tsdb_bench 1000000 --hot-series 8 --hot-fraction 0.5
./build/tsdb_bench 100000000 --crash --path /tmp/crash-db
./build/tsdb_recover /tmp/crash-db

./build/tsdb_mixed_bench 60 --writers 4 --readers 16
./build/tsdb_mixed_bench 60 --hot
```

## 当前状态

已实现：

- `Put`、`WriteBatch`、`Scan`、窗口 `Aggregate`、`ExpireBefore`
- Series 分片和 Shard MemTable（有界内存，Flush 后清空）
- WAL 校验、持久 fd 和小批量 Group Commit
- 增量 Flush、SSTable、Manifest 和尾部 WAL 重放
- 无损 timestamp/value 编码（zigzag varint + 原始 float64 位）
- SSTable SeriesIndex（含字节长度）和按需磁盘分页读取
- 有界流式 Compaction：磁盘多 SSTable 归并 + 过期过滤
- Size-tiered Compaction：按层级归并，避免 O(N²) 写放大
- Compaction 归并 I/O 期间释放全局锁，不阻塞前台写入
- 后台 Flush、阈值触发 Compaction 和后台 GC
- 引用计数 GC：读取并发下也能回收过期 SSTable 文件
- ReadView、过期视图和物理过期清理
- 热点 Series 每 Flush 窗口限流
- 工作线程绑定到 `cpu_budget` CPU 配额
- Covered Commit Version 写入 Manifest
- 50 次文件级故障注入；WAL 残缺尾部、Expire+GC 后崩溃的恢复用例
- 导入、混合、恢复三类工作负载程序，以及 `benchmark/runner.py` 评测运行器

验证结果（作者机器，NVMe SSD，WSL2；可用 `bash scripts/run_bench.sh full` 复现）：

- 1000 万点导入 smoke：约 109k 点/秒，峰值 RSS 约 32 MiB，恢复约 1.9 秒。
- 1 亿点完整导入：约 103.6k 点/秒（16.1 分钟），峰值 RSS 约 42 MiB，
  恢复约 19.9 秒，WAL 重放 0 字节。
- 60 分钟混合读写：约 1481 万点写入、34.6 万次扫描、0 扫描错误、
  0 待回收 SSTable、峰值 RSS 约 113 MiB、20 次合并、148 次刷盘。
- 1 亿点之上的 60 分钟混合读写（严格验收）：约 1133 万点追加写入、
  26.4 万次扫描、0 扫描错误、峰值 RSS 约 125 MiB、16 次合并、114 次刷盘。
