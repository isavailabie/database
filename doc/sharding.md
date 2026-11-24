# 分片设计（内存 / 磁盘 / 写入路径）

> 面向 1 亿点、1 万条 Series、4 写 16 读、4 核 512 MiB 的运行要求。

## 1. 为什么按 Series 哈希分片，而不按时间

```c
shard_id = splitmix64(series_id) % 4
```

数据模型保证**同一 Series 的时间戳严格递增**。按 Series 分片后：

1. 单 Series 的顺序校验、追加写、查询定位都落在同一个分片内，不需要跨分片协调；
2. 时间维度不参与路由，而是由 SSTable / Series Chunk 的 `[min_ts, max_ts]` 做区间裁剪；
3. 时间分片会让"同一 Series 的连续写入"跨多个分片，破坏顺序校验和追加局部性。

实测哈希分布（`splitmix64(series_id) % 4`）：

| Series 数量 | shard0 / 1 / 2 / 3 | 最大/最小 |
| --- | --- | --- |
| 1,000 | 245 / 258 / 236 / 261 | 1.11 |
| 10,000 | 2410 / 2561 / 2456 / 2573 | **1.07** |

1 万条 Series 时偏差 7%，可以接受。注意：连续的小 `series_id` 有局部偏差（0..15 时 shard0 占 8 个），
因为取模只用低位；必要时上线前对 `series_id` 加盐。

## 2. 内存分片

每个分片一份**独立的 MemTable**，写时复制（Copy-on-Write）到 Series 粒度：

```
ShardData
└─ map< series_id, shared_ptr< vector<{point, commit_version}> > >
```

- 无快照时递增写入直接追加 vector；共享时仅克隆本批涉及的 Series，复制成本仍与该 Series 驻留点数有关；
- shard 独占并不表示其全部 Series 独占：先写 A 克隆 shard 后，B 仍可能被旧快照共享，必须再次检查 Series 的 COW；
- 恢复允许乱序二分定位后插入、重复 timestamp 取最高版本；唯一 timestamp 才增加点数额度，正常追加为摊销 O(1)，大量逆序恢复最坏 O(n²)；
- 快照（ReadView）持有旧指针，旧查询继续读旧数据；
- 全局累计 100k 点触发一次 Flush，Flush 后整片清空，内存不随数据总量增长；
- 全局保存 `series_id → last_timestamp` 水位表，顺序校验结合 pending 队列；写入准入只投影候选批次涉及的热点计数，不复制完整活跃 Series 表。

示例：`WriteBatch[(101,1000,10.0), (205,1500,20.0), (306,1200,30.0)]`

| series | 实测 shard | MemTable 落点 |
| --- | --- | --- |
| 101 | shard 3 | `shard-3: 101 → [(1000, 10.0, V)]` |
| 205 | shard 0 | `shard-0: 205 → [(1500, 20.0, V)]` |
| 306 | shard 2 | `shard-2: 306 → [(1200, 30.0, V)]` |

同一批次共享 CommitVersion `V`；三个分片全部应用完成后 `visible_version` 才推进到 `V`，
读者看不到半批。

## 3. 磁盘分片

```
db/
├── CURRENT / MANIFEST            # cutoff、next_version、covered 版本、文件表（校验和）
├── wal/000001.log                # 追加日志（分段，Flush 后轮转）
└── sst/
    ├── shard-03-101.sst          # L0：一次 Flush 一片一个
    ├── shard-00-102.sst
    └── shard-03-118.sst          # L1：同片 8 个 L0 归并而来
```

SSTable 内部按 `(series_id, timestamp)` 排序，Series 连续存放成 Chunk：

```
shard-03-101.sst
├─ series 101  chunk: 1000..3000   offset=12   len=96
├─ series 105  chunk: 1100..2100   offset=108  len=64
└─ series 109  chunk: 1500..2500   offset=172  len=64
```

加载时流式重建 SeriesIndex 到内存（不加载点数据）；查询按 `offset + byte_length` 按需 `pread`。
Compaction 是**分片内、分层**的：同片同层 ≥8 个文件归并成一个下一层文件，归并时按
`commit_version` 去重、丢弃 `ts < cutoff` 的点。不同分片互不影响。

当前归并以 Series 索引堆选择下一条序列，再用时间戳点堆归并；每输入 SST 复用一个
fd 和 64 KiB 缓冲。点归并临时空间随输入文件数 k 增长，不随热点序列的点数增长，
但输出 Series/page 索引仍随输出规模增长。查询使用独立的 8 KiB 缓冲，每个
(series, SST) 源占一个 fd；16 并发游标上限不代表查询源数或 fd 总量已经严格受限。

## 4. 写入路径（分片并行）

```
4 个写入线程
     │  Put / WriteBatch（校验：单 Series 时间戳严格递增、cutoff、配额）
     ▼
全局写队列 ──► 协调线程
                │ ① 分配 CommitVersion
                │ ② 追加全局 WAL 并 fsync（组提交：多写者合批，实测 3.4 请求/次）
                │ ③ 按 shard 拆 fragment
                ▼
   ┌────────────┬────────────┬────────────┬────────────┐
   ▼            ▼            ▼            ▼
 Shard 0      Shard 1      Shard 2      Shard 3
 队列         队列         队列         队列
 worker       worker       worker       worker
 （各自的锁、各自的 MemTable，COW 应用，互不等待）
   └────────────┴────────────┴────────────┘
                │ ④ 全部 ack
                ▼
        visible_version → V（本批对读可见）→ 返回客户端
```

- **并行的是分片执行**：各 shard apply，以及一次 Flush 内的分片 SST 写入可以并行；Compaction 归并 I/O 在全局锁外。当前 Flush 仍由全局锁串行协调，会阻塞前台写入，并未实现 immutable MemTable 锁外 Flush；
- **串行的是"提交顺序"**：一个协调线程负责版本分配与 WAL 追加，保证跨分片批次的原子性；
- 读路径：锁内只取快照（分片指针 + SSTable 引用计数），物化/归并在锁外，长查询不阻塞写入。

## 5. 规模示例（1 亿点 / 1 万 Series / 4 分片）

| 指标 | 单分片 | 全局 |
| --- | ---: | ---: |
| Series | 2,500 | 10,000 |
| 数据点 | 2,500 万 | 1 亿 |
| 平均每 Series 点数 | 1 万 | 1 万 |
| MemTable | 独立（全局 100k 点窗口 → 每片约 25k） | 有界，Flush 后清空 |
| Flush 次数 | 约 1,000 次（每 100k 点一次） | 每次每片一个 L0 文件 |
| 稳态文件数 | 每层 ≤8 个，几百个量级 | — |
| 磁盘 | — | 约 13 B/点 → **1.3 GB** |
| 峰值 RSS | — | 历史测量曾为 48–80 MiB；不能代表当前亿点库，2026-11-25 实测与边界见性能报告 |

## 6. WAL 的取舍：全局 vs 分片

| | 全局 WAL（当前） | 每分片 WAL + commit.log |
| --- | --- | --- |
| 一次跨分片提交的 fsync | **1 次**（覆盖所有分片） | 4 次并行 + 1 次 commit ≈ 单盘 4–5× 延迟 |
| 单分片提交 | 1 次 | 1 次（可自提交） |
| 崩溃恢复 | 单日 志按序重放 | 需 commit.log 交叉引用 + 按 offset 截断 |
| 单盘吞吐 | 基准 | 预计下降（fsync 次数增加） |
| 多盘吞吐 | 受单盘限制 | 可随盘数线性扩展 |

结论：**单盘用全局 WAL（分片并行执行 + 全局提交顺序，RocksDB 也是这样）；
多盘部署时再把 WAL 拆到各分片**，此时 fsync 才能真正并行。

### 6.1 实测 A/B（单盘，4 写 16 读，各 15 s）

| 指标 | 全局 WAL（默认） | 分片 WAL（`--sharded-wal`） |
| --- | ---: | ---: |
| 写入吞吐 | **200k 点/s** | 170k 点/s（−15%） |
| fsync 次数/秒 | **646** | 2892（4.5×） |
| 每次 commit 的 fsync | 1 | 每分片 1 次 + commit 1 次 |
| 峰值 RSS | 69 MiB | 64 MiB |

单盘上分片 WAL 的并行 fsync 都落在同一块盘上，只是把 1 次 fsync 拆成 4–5 次，
因此**更慢**；这验证了「单盘用全局 WAL」的结论。

分片 WAL 要真正收益，需要同时满足：

1. **每分片 WAL 落在不同物理盘**（当前实现都在 `db/wal/shard-N/`，同一块盘）；
2. **单分片事务自提交**（`kSelfCommit`，跳过 commit.log 的那次 fsync），
   当前 v1 实现对所有提交都写版本区间，尚未实现自提交；
3. 多盘并行时 fsync 才能重叠。

因此在单盘（课程验收环境）上，`Options::sharded_wal` 保持默认 `false`。

### 6.2 独立 WAL 盘：`Options::wal_dir`

WAL 段目录默认是 `<db>/wal`，可通过 `Options::wal_dir` 指到**另一块盘**，
让 WAL fsync 不再和 SSTable 的刷盘/合并抢同一块盘：

```cpp
tsdb::Options opts;
opts.wal_dir = "/mnt/wal";   // SSTable 仍在 db/sst，WAL 在独立盘
opts.sharded_wal = true;     // 可选：分片 WAL
```

- Manifest 记录 `WAL_DIR`；若下次启动时该目录变了，`Open` 直接报错
  `WAL directory changed since last open`，避免把未刷盘段丢在旧盘；
- 分片 WAL 复用同一覆盖：`wal_dir/shard-N/` + `wal_dir/commit/`；
- 基准和恢复程序都支持 `--wal-dir DIR`：
  `./tsdb_mixed_bench 60 --wal-dir /mnt/wal [--sharded-wal]`、
  `./tsdb_bench 1000000 --wal-dir /mnt/wal`、`./tsdb_recover DIR --wal-dir /mnt/wal`。

> 说明：本机只有一块物理盘，同盘不同目录的 A/B 不会带来吞吐提升
> （实测全局 237k 点/s、分片 220k 点/s，仍是 fsync 次数差异）。
> 独立盘的价值要放到**两个真实设备**上才能体现；分片 fsync 要真正并行，
> 还需要每分片 WAL 落在各自独立的盘上 + 单分片自提交（见 6.1 第 2 点）。

## 7. 实测（1M 点，磁盘，非 tmpfs）

| 场景 | 结果 |
| --- | --- |
| 导入（10k Series 均匀） | 118k 点/s，RSS 27 MiB，磁盘 13 MB |
| 导入（8 条热点占 50%） | 124k 点/s，RSS 31 MiB |
| 混合 4 写 16 读 | 写 14.7k 点/s，扫 14.4k 次/s，p99 25 ms，RSS 48 MiB |
| 热点混合 | 写 17.9k 点/s，扫 1.3k 次/s，p99 171 ms，RSS 79 MiB |
| 崩溃恢复 | 0.43 s |

> 注意：`/tmp` 在 WSL2 上是 tmpfs，fsync 几乎免费，导入会显示 250k 点/s。
> **所有对外数字必须在真实磁盘上测**。
