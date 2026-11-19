# 分片 WAL 与跨分片提交协议（设计稿）

> 目标：把唯一还没有分片的持久化组件——WAL——拆成每个 Shard 一份，
> 同时保持 `WriteBatch` 的跨分片原子性与崩溃恢复的正确性。

## 1. 范围

改造前（当前实现）：

- 一个全局 `wal/000001.log`，一条记录承载整个批次；
- 一次组提交一次 fsync，覆盖所有分片；
- 恢复时按序重放，残缺尾部截断。

改造后：

- 每个 Shard 一个独立 WAL（`wal/shard-N/`），各自追加、各自 fsync；
- 一个全局 Commit WAL（`wal/commit/`），只记录"这个事务到底提交了没有"和过期边界；
- 全局协调器仍然存在，但它不再把数据写进同一个文件。

不变的部分：分片路由、COW MemTable、Shard Worker、ReadView、`visible_version`、
SSTable/Compaction/GC 全部保持现状。

## 2. 目录结构

```
db/
├── MANIFEST, CURRENT
├── wal/
│   ├── shard-0/000001.log     # 只有 shard 0 的点数据
│   ├── shard-1/000001.log
│   ├── shard-2/000001.log
│   ├── shard-3/000001.log
│   └── commit/000001.log      # 提交决定 + 过期边界（全局，量很小）
└── sst/...
```

## 3. 记录格式

### 3.1 Shard WAL

```
[magic u32][type u8][txn u64][version u64][shard u8][count u32]
[ (series_id uvarint, ts zigzag-varint, value f64) × count ]
[crc u64]
```

| type | 含义 |
| --- | --- |
| 1 `kFragment` | 跨分片事务的一个片段，**需要 Commit 记录确认**才生效 |
| 2 `kSelfCommit` | 单分片事务，记录完整即提交，不需要 Commit 记录 |

### 3.2 Commit WAL

```
[magic u32][type u8][version u64][txn u64][n u32]
[ (shard u8, end_offset u64) × n ][crc u64]
```

| type | 含义 |
| --- | --- |
| 1 `kCommit` | 事务 txn 提交；每个参与分片的片段结束于 `end_offset` |
| 2 `kExpire` | 全局过期边界推进到 `version`，payload 为 `cutoff` |

`end_offset` 是该分片 WAL 中该片段最后一个字节的位置，恢复时按它精确截断。

## 4. 提交流程

### 4.1 单分片批次（快路径）

```
协调器: 分配 version V 与 txn T，拆出唯一 fragment
Shard i: append kSelfCommit{T,V,points} → fsync      ← 唯一一次 fsync
Shard i: apply → ack
协调器: visible_version = V → 返回
```

### 4.2 跨分片批次（两阶段）

```
协调器: 分配 V, T，拆成 N 个 fragment
  ┌─ Shard 0: append kFragment{T,V,...} → fsync ┐
  ├─ Shard 2: append kFragment{T,V,...} → fsync ├─ 并行，各自记下 end_offset
  └─ Shard 3: append kFragment{T,V,...} → fsync ┘
协调器: 若全部成功 → append kCommit{V,T,[(shard,end_offset)...]} 到 Commit WAL → fsync
        若有任一失败 → 不写 Commit，事务作废
各分片: apply → ack
协调器: visible_version = V → 返回
```

单盘上一次跨分片提交是 **N 次并行 fsync + 1 次 commit fsync**；单分片只有 1 次。

## 5. 崩溃点与恢复语义

| 崩溃位置 | 磁盘状态 | 恢复行为 | 客户端 |
| --- | --- | --- | --- |
| 任一 fragment fsync 前 | 分片 WAL 尾部残缺 | 截断到最后一个完整记录；无 Commit → 丢弃 | 未收到成功 |
| fragment 全部 fsync 后、Commit 前 | 有完整 fragment，无 Commit | 丢弃这些 fragment | 未收到成功 |
| Commit fsync 前（写到一半） | Commit 记录残缺 | 视为未提交，丢弃全部 fragment | 未收到成功 |
| Commit fsync 后、apply 前 | 完整 fragment + Commit | 重放全部 fragment，重新 apply | 已收到成功（重放保证不丢） |
| apply 中途 | 同上 | 重放后重新 apply，幂等（同 ts 同 version 覆盖） | 已收到成功 |

**关键不变式**：只有 Commit 记录落盘的事务才会被恢复；没有 Commit 的 fragment 一律丢弃。

## 6. 恢复算法

```
读取 MANIFEST（校验和、cutoff、next_version、covered 版本、SSTable 列表）
读取 Commit WAL:
    committed[txn] = (version, [(shard, end_offset)], )
    记录已提交的最大 version
对每个 shard:
    顺序读 WAL 记录：
      kSelfCommit  → 应用
      kFragment    → 若 txn ∈ committed 且当前偏移 <= committed[txn].end_offset → 应用
                     否则跳过（未提交）
    把 WAL 截断到"最后一个已应用的记录末尾"
    （已提交但偏移超过 end_offset 的，说明是别的事务，正常继续）
visible_version = 已提交的连续最大 version
next_version    = max(MANIFEST.next_version, 已见最大 version + 1)
重建各 SSTable 的 SeriesIndex，重建 last_timestamps
```

## 7. 日志回收

- 每个 Shard 独立轮转：自己的 MemTable 被 Flush 成 SSTable、Manifest 发布后，
  才能删除该分片已覆盖的段；
- Commit WAL 只保留"还有未覆盖事务"的部分，按 covered 版本轮转；
- 单分片事务的段回收只看该分片自己的 covered 版本。

## 8. 可见性与原子性

- `visible_version` 仍是唯一可见性闸门，且只推进到**连续已提交完成 apply** 的版本；
- 读路径不变：`ReadView.version = visible_version`，MemTable/ SSTable 均按版本过滤。

## 9. 开关与 A/B

新增 `Options::sharded_wal`（默认 `true`，可通过环境/命令行关闭）：

- `false`：走现有全局 WAL 路径（保留作为回退）；
- `true`：走本文档描述的分片 WAL 路径。

两套路径共用同一份 MemTable / Apply / ReadView / SSTable 代码，差异只在上面的
"写日志 + 恢复"两处，便于对比与回退。

## 10. 性能预期

| 批次类型 | 全局 WAL（现状） | 分片 WAL |
| --- | --- | --- |
| 单分片 | 1 fsync | **1 fsync** |
| 跨分片 | 1 fsync | N 并行 fsync + 1 commit fsync |

导入批次几乎都跨分片，**单盘上预计吞吐下降 20–40%**；缓解手段：
每个分片内对多个事务做组提交（一次 fsync 覆盖多批），Commit WAL 同样组提交。
收益在日志架构与分片级生命周期，不在单盘吞吐。

## 11. 实现步骤与验收

| 阶段 | 内容 | 验收 |
| --- | --- | --- |
| 1 | `ShardWAL` / `CommitWAL` 读写 + 单分片快路径 | 现有 23 个测试全绿 |
| 2 | 跨分片两阶段提交 + 恢复 | 上述崩溃矩阵逐条覆盖 |
| 3 | 日志回收按分片推进 | Flush 后各分片段能独立清理 |
| 4 | 故障注入 ≥50 次（写入/刷盘/发布/合并/回收） | 每次"要么数据完好、要么干净失败" |
| 5 | 开关打开做 A/B 基准 | 单分片/跨分片分别报告 |
