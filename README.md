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
`tsdb_fault_test`（9 个运行阶段 × 两种 WAL × 10 轮，共 180 次真实进程崩溃，
另加 50 次文件损坏注入）。安装 Python 3 时还会运行 `benchmark_runner_tests`。
真实崩溃测试必须命中指定挂点并退出，恢复后校验已确认提交、整批原子性、
过期状态以及继续写入后二次恢复，不能以未注入的轮次计数。

```bash
./build/tsdb_tests              # 全部用例，逐条打印名称与耗时
./build/tsdb_tests compaction   # 只跑名字包含 compaction 的
./build/tsdb_tests 13           # 只跑第 13 个
```

## 本轮功能修复验证（2026-11-25）

验收目标为 Linux。本轮使用独立 Ubuntu 22.04 ARM64 Docker 容器、GCC 11.4、
4 CPU 配额验证；最终再以 cpuset 0–3 和 512 MiB 内存上限运行功能与混合负载。
不是直接复用本机旧 `build/` 二进制。
Release、Debug、ASan/UBSan 均通过：51 个功能用例、180 次真实进程崩溃、
50 次文件损坏注入与 10 个运行器用例；Release 功能套件另连续通过 10 轮。
新增覆盖容量背压、不可合并 SST 的前进性、关闭唤醒、持久化失败和统计接口。
百万点 quick 流程已验证同库均匀/热点各 60 秒混合读写及异常恢复，未报读写错误。
过程统计和原始输出保留在本地 `build/linux-validation-20261125/`（不提交）。

**边界**：本轮是功能回归，并未重新完成 1 亿点 + 60 分钟完整验收；
下方历史性能不是本轮结果。真实进程退出测试不等同于硬件掉电实验。
尚未实施 vector MemTable、锁外 WAL/Flush、严格有界流式 Compaction 等性能改造。

## 基准

可复现的评测由 `benchmark/runner.py` 驱动，目录结构与课程 benchmark 一致：
复制源码快照、Release 构建，各轮次在独立进程中运行；混合场景通过 `--keep`
复用对应均匀/热点导入的数据库，单独选 mixed 时自动预载。结果写入
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
- Compaction：按 Series 归并磁盘多 SSTable，并过滤过期点；当前仍物化单条 Series，
  尚未完成热点 Series 的严格有界流式归并
- Size-tiered Compaction：按层级归并，避免 O(N²) 写放大
- Compaction 归并 I/O 期间释放全局锁，不阻塞前台写入
- 后台 Flush、阈值触发 Compaction 和后台 GC
- 引用计数 GC：读取并发下也能回收过期 SSTable 文件
- ReadView、过期视图和物理过期清理
- 队列、MemTable 和热点容量触发阻塞背压；等待后重新校验批次，单批超限直接拒绝
- SST 压力下强制低阈值合并；不可再归并的文件集合采用软上限，避免永久等待
- Linux 协调/刷盘/合并线程进行亲和绑定；验收用容器 `--cpus=4` 限制全部线程
- Covered Commit Version 写入 Manifest
- 180 次阶段级真实崩溃 + 50 次文件损坏；WAL 残缺尾部、Expire+GC 的恢复用例
- 持久化失败进入故障状态：拒绝新读写/维护/GC，既有游标保持视图，关闭仍释放资源；须重开修复
- COUNT 的精确整型结果位于 `AggregateResult::count`，`result` 保留浮点兼容投影
- 当前 RSS 与峰值 RSS 分开统计，混合基准记录写入延迟分位和周期 `samples.csv`
- 导入、混合、恢复三类工作负载程序，以及 `benchmark/runner.py` 评测运行器

验证结果（作者机器，NVMe SSD，WSL2；可用 `bash scripts/run_bench.sh full` 复现）：

> **读数注意**：下面四条来自 **页级时间索引（11e2cd0）与写背压（75b758b）之前**
> 的版本，其中“1 亿点 42 MiB”已不代表当前代码。
> 当前代码在同类环境（4 vCPU WSL2、ext4 on vhdx、数据目录放真实磁盘而非 tmpfs）
> 上实测 1 亿点导入：**320 s（约 312k 点/秒）、峰值 RSS 约 240 MB、
> 恢复 12.3 s、磁盘 1.5 GB**，仍满足 512 MiB 预算。
> RSS 上升来自常驻索引：56~80 个存活 SSTable 上约 20 万条 Series 索引项和
> 91 万条页索引项（每个 Series 索引项自带一个页 vector）。

- 1000 万点导入 smoke：约 109k 点/秒，峰值 RSS 约 32 MiB，恢复约 1.9 秒。
- 1 亿点完整导入：约 103.6k 点/秒（16.1 分钟），峰值 RSS 约 42 MiB，
  恢复约 19.9 秒，WAL 重放 0 字节。
- 60 分钟混合读写：约 1481 万点写入、34.6 万次扫描、0 扫描错误、
  0 待回收 SSTable、峰值 RSS 约 113 MiB、20 次合并、148 次刷盘。
- 1 亿点之上的 60 分钟混合读写（严格验收）：约 1133 万点追加写入、
  26.4 万次扫描、0 扫描错误、峰值 RSS 约 125 MiB、16 次合并、114 次刷盘。

单盘优化补充（macOS 实机；上方作者机器的数据保留）：

- **写路径**：无快照持有时原地应用 fragment，省去整片/整 Series 的 COW。
  批量导入 2M 点 0.93s→0.48s、10M 点 5.7s→2.9s（约 2×）。
- **峰值内存**：对活跃 SSTable 数做写者背压（`Options::max_live_sstables`，默认
  256）。1 亿点导入峰值 RSS **978 MiB→229 MiB**，耗时 58.7s→103.7s（仍远低于
  60 分钟上限）。原因：快速导入会跑赢 Compaction，堆积上千个 SSTable，而每个
  SSTable 都带一份全 Series 索引。
- **WAL 独立盘**：`Options::wal_dir` 可把 WAL 段放到与 SSTable 不同的磁盘；
  Manifest 记录 `WAL_DIR`，目录变更时拒绝启动以免丢未刷盘段。单盘上分片并发
  WAL 仍更慢（15%），默认关闭；多盘场景见 `doc/sharding.md` §6。
- **选择性查询**：SSTable Series Chunk 增加 128 点/页的 `[min,max]` 时间索引，
  窄范围查询约 5.7×（87µs→15µs），全量扫描不变。
