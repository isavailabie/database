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
以上为功能修复阶段的验证；随后实施的性能改造及独立对照结果见下一节。

## 性能优化验证（2026-11-25）

相对已推送功能基线 `a2cb1e0`，保留写入准入投影、vector MemTable、流式多路
Compaction 和 8 KiB 查询解码；WAL 锁外流水线因混合吞吐退化未合入。
在 Linux 4 CPU、512 MiB、无交换空间下，六场景各三轮 AB/BA 交替，中位结果：

| 场景 | 基线 → 优化 | 收益 |
| --- | --- | --- |
| 千万点均匀导入，含 drain | 10.119 → 7.817 s | 耗时 -22.75% |
| 千万点热点导入，含 drain | 9.154 → 7.471 s | 耗时 -18.39% |
| 300 万点单热点合并 | 0.824 → 0.180 s | 4.58 倍；进程峰值 RSS 244.10 → 16.13 MiB |
| 100 万点单热点驻留追加 | 0.494 → 0.316 s | 耗时 -35.99%；RSS -54.22% |
| 300 万点磁盘扫描三遍 | 0.525 → 0.253 s | 2.08 倍；RSS +0.64% |
| 已有千万点库，4 写 16 读 | 写 68.8k → 159.6k 点/s；读 31.8k → 63.9k 次/s | 写 2.32 倍、读 2.01 倍；零读写错误 |

最终 Release、Debug、ASan/UBSan 均通过 63 个功能用例、180 次真实崩溃、50 次文件
损坏、38 个 Python 协议用例及 5 个设计烟测；Release 全功能另连续通过 10 轮。
详细环境、逐轮数据、尾延迟、设计取舍、容量验证和未完成的 60 分钟验收边界见
[`doc/性能优化-2026-11-25.md`](doc/性能优化-2026-11-25.md)，
[三轮原始指标 JSON](doc/performance-results-2026-11-25.json) 随仓库保存。
最终版本另在相同限额下验证 1 亿点均匀/热点导入：71.24 / 76.70 秒，峰值 RSS
152.08 / 168.11 MiB；同库各 60 秒混合零读写错误，峰值最高 184.36 MiB。
异常退出后 WAL 重放与全库校验均少于 8 秒，精确恢复全部已确认点。
**该轮尚未完成亿点库上的 60 分钟持续混合**；热点一次扫描最大延迟约 1.968 秒，详见报告。
以上为 Docker Linux overlayfs 数据层测量，不是历史 WSL2 或裸机 SSD 数据。
60 分钟验收在后续第 1 轮迭代补齐，见下一节；两轮实验不混算。

## 性能迭代验证（2026-11-25，第 1 轮）

基线为上一轮已推送的 `c99b8ab`。本轮保留的确定项是**资源生命周期与并发正确性修复**，
唯一可重复证明的吞吐收益来自长序列尾部窄范围查询：

| 项目 | 结果 |
| --- | --- |
| 300 万点单 chunk、800 次 128 点尾部窄查询 | 三轮 AB/BA **+78.29%**，独立五轮复测 **+67.36%**，八轮方向一致 |
| 其余六场景（导入、合并、追加、全扫描、mixed） | 在噪声范围内持平，不宣称提速 |
| 100 万点驻留追加 | 三轮 -11.15% / 五轮 +35.31%，方向反转，判定为 0.3 秒级短操作噪声，两次原始结果均保留 |
| 1 亿点均匀 / 热点导入（512 MiB、无交换空间） | 83.40 / 69.86 秒，峰值 RSS 182,411,264 / 204,124,160 B，恢复 11.02 / 9.20 秒，精确恢复 1 亿点 |

四项保留改动：Expire 后闲置 vector capacity 按阈值回收且过期时不再无条件复制
shard 外层 map；页索引改用 `lower_bound` 跳过不可能命中的页并提前终止；
`Scan` 先在 16 名额 gate 排队再取 ReadView，避免等待者固定旧快照；
`Close` 唤醒 gate 等待者，消除关闭后查询永久阻塞。

回归：Release / Debug / ASan+UBSan 各自 CTest 5/5，68 个功能用例、
180 次真实进程崩溃、50 次文件损坏、42 个 Python 协议用例、6 个设计烟测，
Release 功能套件连续 10 轮通过。新增 `scan_gate_close_wakeup_stress` 覆盖
`Close` 与查询准入等待之间的丢失唤醒竞态。完整逐轮数据、噪声判读、边界与收敛判据见
[`doc/性能优化-迭代-2026-11-25.md`](doc/性能优化-迭代-2026-11-25.md)，
原始指标见 `doc/performance-results-round1-*.json`。

**1 亿点库 60 分钟混合验收已补齐**：均匀 / 热点库各连续 3600 秒，
两组 `scan_errors=0`、`write_errors=0`，峰值 RSS 分别 296.20 / 299.44 MiB，
分别追加 221,834,000 / 296,278,000 点、完成 11,070,223 / 10,354,781 次扫描。
过程采样与完整指标见 `doc/性能优化-迭代-2026-11-25.md` §1.4。

## 第 2 轮候选评估与收敛（2026-11-25）

在隔离源码树上评估了三个查询路径候选：解码快路径（varint / double 缓冲内直读 +
显式堆比较器）、选择性查询建源前页索引剪枝、SST 常驻共享 fd。
四个 A/B 会话共 134 条记录，**没有一项达到保留门槛**：

- 共享 fd 破坏两条既有保证（空闲时不持有 SST fd、文件被改名后必须报打开错误），
  在最终 68 用例功能回归中失败 2 项（`query_releases_descriptors`、
  `query_truncated_read_is_error`），不进入 A/B；
- 解码快路径在三轮七场景中无稳定正收益，narrow -29.69% 在五轮复测里只剩
  -1.93%，mixed 写吞吐两组中位约 -7.5% 但逐轮方向不一致；
- 源剪枝相对基线的 narrow 收益仅 +1.41% 且方向翻转，而验收混合查询是全范围，
  该分支根本不触发。

其余候选（Flush 锁外、分块 COW、流式 Aggregate、Compaction fan-in、WAL 流式恢复、
FD 预算、低干扰统计、持久化索引）逐项复核后也因风险/复杂度不成比例或无可测收益
而不保留。**结论：迭代收敛，主树不再引入新的性能优化项**；这不等于理论上不存在
更激进的架构重构。完整数据、噪声判读与收敛论证见
[`doc/性能优化-迭代-2026-11-25.md`](doc/性能优化-迭代-2026-11-25.md) §4–§5，
原始指标见 `doc/performance-results-round2-*.json`。最终推送前在正式
4 核、512 MiB、无交换空间下重跑主树 Release / Debug / ASan+UBSan 三套 CTest
（各 5/5 通过）与 Release 功能连续 10 轮，隔离变体源码树也重跑完整 CTest。
本轮仅提交文档与原始数据，未改动引擎代码。

## 第 3 轮复查修复（2026-11-25）

收敛后对全量代码复查，修复 1 个真实资源泄漏和 4 个次要问题：

- **孤儿 SST 永久泄漏**：待删除列表只在内存中，`Close` 不做 GC，`Open` 只清理
  `.tmp`。热点验收库磁盘上 103 个 SST 只有 55 个登记在 MANIFEST。现在 `Open`
  先持久化 MANIFEST 再删除未登记的 `.sst`，`Close` 在最终 Flush 后执行一次 GC；
  在该库副本上回收 48 个文件、27.3 MB，恢复点数与旧引擎一致；
- CURRENT 存在而 MANIFEST 缺失时拒绝打开（以前会按空库打开）；
- `GetStats` 改用 `error_code` 版目录遍历，并把文件系统和内存采样移到全局锁外；
- 写协调线程改用专用条件变量：空闲 5 s 的 CPU 从约 136 ms 降到 0.3 ms 以内，
  自愿上下文切换从约 13,000 次降到个位数；
- `last_timestamps` / `hot_series_points` 在过期和重启后收缩。

协调线程的改动同时修掉了组提交等待中的丢失唤醒：千万点 mixed 写吞吐两组独立实验
**+33.45% / +32.40%**（8/8 轮同向），写入 p50 下降 33–39%，扫描吞吐 −1.8% 到 −3.4%；
导入与 append 持平。功能用例 68 → 73，新增的 5 个用例中有 4 个在旧引擎上失败；
故障注入在每次恢复后都核对磁盘 SST 与 MANIFEST 一致。Release / Debug / ASan+UBSan
各自 CTest 5/5，Release 连续 10 轮通过。详见
[`doc/性能优化-迭代-2026-11-25.md`](doc/性能优化-迭代-2026-11-25.md) §6，
原始指标见 `doc/performance-results-round3-*.json`。

## 第 4 轮：锁外 Flush 实测（2026-11-25）

把「Flush 全程持全局锁」拆成「冻结 / 锁外落盘 / 发布」：冻结时先切到新 WAL 段，
旧段等 MANIFEST 发布后再删除；covered version 只推进到冻结点。方案已完整实现，
TSan、新增的 3 个崩溃与并发用例、三套 CTest 全部通过，但千万点 A/B 两组会话
（3 + 5 轮）的主指标没有可重复收益：mixed 写吞吐 −0.56%，uniform 导入 −1.57%
（两组方向相反），hot 导入 −7.26% 但被离群值主导。只有导入写入 p99 稳定下降
40–49%。原因是单盘上锁外写 SST 与 WAL fsync 争用磁盘：Flush 总耗时 +12%、
WAL 同步 +13%；把锁外阶段改成单线程反而更慢。按既定口径**不合入**，补丁保存在
`doc/candidates/round4-unlocked-flush.patch`，留待 WAL 独立盘环境复测。

同轮修复并提交一个基线就有的问题：`shard_stopping` 原为 `vector<bool>`，`Close` 不持
分片锁就写入。这是数据竞争（TSan 报 24 条），也可能丢失唤醒，使 Close 卡住。
现在改为逐字节存储并在 `shard_mutex[i]` 下写入：TSan 零警告，三套 CTest 各 5/5，
Release 连续 10 轮通过，性能在噪声内。详见
[`doc/性能优化-迭代-2026-11-25.md`](doc/性能优化-迭代-2026-11-25.md) §7。

## 写入校验验收（2026-11-26）

课程要求「不满足时间戳顺序或过期边界要求的写入返回错误；批次校验失败时整批不生效」。
校验在入队前持锁执行，同时检查批内、排队中批次、已提交数据与 cutoff；失败批次不分配
版本、不写 WAL、不改任何状态。新增用例 `write_validation_contract` 覆盖跨批 / 批内 /
重复时间戳、cutoff 边界、失败批次不占版本、重启后（SST 与仅 WAL 两种来源）校验仍成立、
4 写者并发写同一 Series。故意改坏三处校验后该用例均失败；其中「不检查排队中批次」
原有用例无一能发现。三套构建 CTest 各 5/5，Release 连续 10 轮通过。设计说明见
设计文档 §4.1，讨论材料见 [`doc/课程讨论-架构设计与写入校验-2026-11-26.md`](doc/课程讨论-架构设计与写入校验-2026-11-26.md)。

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
- Series 分片和连续 vector MemTable（递增追加摊销 O(1)，逻辑容量背压，Flush 后清空；
  逻辑点数额度不等同于实际 vector capacity 或 RSS 硬上限）
- WAL 校验、持久 fd 和小批量 Group Commit；写协调线程使用专用条件变量，空闲时不轮询
- 增量 Flush、SSTable、Manifest 和尾部 WAL 重放
- 无损 timestamp/value 编码（zigzag varint + 原始 float64 位）
- SSTable SeriesIndex（含字节长度）和按需磁盘分页读取；页索引用 `lower_bound` 跳过
  `max_timestamp < max(start, cutoff)` 的页并在越过 `end` 时提前终止；每查询源固定
  8 KiB pread 缓冲，Open 校验保留，游标 Close/EOF/异常即时释放快照与 fd，
  聚合读取错误通过 Status 返回
- Compaction：按 Series 索引堆与时间戳点堆流式归并、过滤过期点；每输入 SST 使用
  64 KiB 缓冲，点归并临时空间为 O(fan-in)，不物化整条热点 Series；输出索引仍随规模增长
- Size-tiered Compaction：按层级归并，避免 O(N²) 写放大
- Compaction 归并 I/O 期间释放全局锁，不阻塞前台写入
- 后台 Flush、阈值触发 Compaction 和后台 GC
- 引用计数 GC：读取并发下也能回收过期 SSTable 文件
- 孤儿 SST 回收：`Open` 先持久化 MANIFEST 再删除未登记的 `.sst`，`Close` 收尾 GC；
  CURRENT 存在而 MANIFEST 缺失时拒绝打开
- ReadView、过期视图和物理过期清理；`ExpireBefore` 先只读探测再按需 COW，
  前缀删除后按阈值重建闲置 vector capacity，避免逻辑额度已归还但实际 RSS 滞留
- 查询先在 16 名额 gate 取得准入再取 ReadView，等待者不固定旧快照；
  `Close` 唤醒 gate 等待者并返回错误，避免关闭后永久阻塞
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
