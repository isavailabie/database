# 分片 WAL 与跨分片提交协议（设计稿 v2）

> v1 评审结论：架构方向通过，恢复与回收规则不通过。本版按评审逐条修订。

## 0. 评审意见闭合

| # | 评审意见 | 本版处理 |
| --- | --- | --- |
| 1 | `end_offset` 不能代表"之前的事务都已提交"；提交序列允许空洞 | 改为 **FragmentRef 精确定位 + 按 txn 精确匹配**；恢复不再按偏移判断是否提交（§3、§7） |
| 2 | 单分片 SelfCommit 与跨分片 Commit 的版本顺序、Abort 语义不完整 | 明确事务状态机、**连续版本闸门**与 **Abort 不得留洞**（§4） |
| 3 | CommitWAL `fsync` 报错时状态不确定，不能直接判未提交 | 明确错误语义：进入**降级状态**，由恢复决定结果（§6.2） |
| 4 | Expire 与普通提交的版本顺序、Apply 完成条件不完整 | Expire 作为**带版本的全局控制事务**，同样走状态机（§4.3、§5） |
| 5 | 记录缺少 Segment ID | 记录头带 `segment_id`，FragmentRef 带 `(segment, start, end)`（§3） |
| 6 | 回收条件不充分 | 回收以 **Manifest 覆盖 + 全部参与分片已发布** 为前提（§8） |
| 7 | 开关默认应为 false，格式需由 Manifest 标记 | `sharded_wal` 默认 **false**；Manifest 记录 `wal_format`，禁止混用（§9） |
| 8 | 性能不应写成结论 | 改为待测假设，并给出组提交优化方向（§10） |

## 1. 不变式

1. **单事务在途**：协调器同一时刻只推进一组事务（组内可含多个 txn），组内全部 Prepare/Commit 完成前不分配新版本。
   → 版本号**连续无空洞**；Abort 只需把 `next_version` 回退（现有实现已如此）。
2. **可见性闸门**：`visible_version` 只推进到"已提交 **且** 全部参与分片已 Apply"的连续版本。
3. **提交即持久**：只有 CommitWAL 中的决策落盘，事务才算提交；没有决策的 Fragment 一律不恢复。
4. **不确定性不猜测**：无法确定提交结果时（fsync 报错/掉电），不得擅自判 Abort，交由恢复判定（§6.2）。

## 2. 目录与段

```
db/wal/
├── shard-0/000001.log
├── shard-1/000001.log
├── shard-2/000001.log
├── shard-3/000001.log
└── commit/000001.log
```

每个 WAL 目录独立分段（`%06llu.log`），独立轮转。段号单调整增，记录头带 `segment_id`
以便 Commit 记录跨段引用。

## 3. 记录格式

### 3.1 Shard WAL

```
[magic u32][type u8][segment_id u64][txn u64][version u64][shard u8][count u32]
[ (series uvarint, ts zigzag-varint, value f64) × count ][crc u64]
```

| type | 含义 |
| --- | --- |
| `kFragment` | 跨分片事务的片段，**需 Commit 决策**才生效 |
| `kSelfCommit` | 单分片事务，记录完整即提交 |

### 3.2 Commit WAL

```
kCommitRange 记录:  [magic u32][type u8][segment_id u64][first_version u64][last_version u64][crc u64]
kExpireRecord 记录: [magic u32][type u8][segment_id u64][version u64][cutoff u64][crc u64]
```

> 实现说明（v1）：提交决策记录的是**版本区间**而不是逐 fragment 的 `(segment,start,end)`。
> 因为 §1.1 保证同一时刻只有一个事务组在途、组内版本连续，所以"记录 version 落在某个
> 已提交区间内"与"按 txn 精确匹配"等价，恢复不做任何偏移推断。
> 逐 fragment 的精确坐标留到需要**子段截断/精确回收**时再引入（当前回收以整段为单位，
> 见 §8），届时 Commit 记录升版本号，旧库按 §9 的格式标记拒绝混用。

## 4. 事务状态机与版本

### 4.1 状态

```
ALLOCATED → PREPARING → (COMMITTED | ABORTED) → APPLYING → APPLIED → VISIBLE
```

- `ALLOCATED`：协调器分配 version/txn；
- `PREPARING`：各分片追加 Fragment 并 fsync；
- `COMMITTED`：CommitWAL 决策落盘（单分片事务则由 `kSelfCommit` 记录自身代表）；
- `APPLYING/APPLIED`：分片应用到 MemTable 并 ack；
- `VISIBLE`：`visible_version` 越过该版本。

### 4.2 版本闸门

```
visible_version = max{ V | V 连续、V 已 COMMITTED 且所有参与分片 APPLIED }
```

- 由于 §1.1 保证只有一个事务组在途，**不存在"未提交版本成为永久空洞"**；
- `ABORTED`：协调器回退 `next_version`（组内 txn 数），下一组重新使用这些版本号；
- 因此闸门实现为"组内全部 APPLIED → 推进到组内最大版本"。

### 4.3 Expire

`ExpireBefore(cutoff)` 是一次**全局控制事务**：分配 version，写 `kExpireBatch`，
fsync 后更新 `cutoff`，再由各分片清理；`visible_version` 必须等它完成后才能越过该版本。

## 5. 提交流程（含组提交）

### 5.1 单分片快路径

```
协调器: 分配 V, T
Shard i: append kSelfCommit{T,V} → 与同段其它记录一起 fsync（组提交）
Shard i: apply → ack
协调器: visible_version = V → 返回
```

### 5.2 跨分片

```
协调器: 分配 V, T，拆 N 个 fragment
各分片: append kFragment{T,V}，记下 (segment, start, end)；与其它记录一起 fsync（组提交）
协调器: 全部成功后 append kCommitBatch{[(T,V,[refs])]} → fsync（一次覆盖多个 txn）
各分片: apply → ack
协调器: visible_version = 组内最大 V → 返回
```

组提交是本设计的性能关键：**同一分片的多条 Fragment / SelfCommit 共用一次 fsync；
CommitWAL 一次 fsync 记录多个 txn 的决策**。

## 6. 崩溃与错误语义

### 6.1 崩溃点矩阵

| 崩溃位置 | 磁盘状态 | 恢复 | 客户端 |
| --- | --- | --- | --- |
| Fragment fsync 前 | 段尾残缺 | 截断尾部，丢弃 | 未成功 |
| Fragment 完成、Commit 前 | 完整 Fragment，无决策 | 丢弃 | 未成功 |
| Commit 记录写到一半 | 决策残缺 | 视为未提交，丢弃全部 Fragment | 未成功 |
| Commit 落盘后、Apply 前 | 完整决策 | 全部重放并 Apply | 已成功，不丢 |
| Apply 中途 | 同上 | 重放后重新 Apply（幂等） | 已成功，不丢 |

### 6.2 fsync 报错（意见 3）

`fsync` 返回错误时，数据可能已持久化也可能没有，**不能判定为失败**。规则：

1. 重试一次；
2. 仍失败 → 引擎进入 **DEGRADED**：拒绝新的写入与过期操作（返回错误），查询仍可读；
3. 只有重新 `Open`（走恢复）才能确定该事务的实际结果；
4. 恢复时若读到完整 Commit 决策 → 该事务**已提交**，正常重放；否则未提交。

**不允许**在 fsync 失败后直接把事务标记为 Aborted 并继续服务。

### 6.3 恢复与截断（意见 1）

- **只截断段尾不完整记录**；
- 段中间"未提交的 Fragment"**不删除、不截断**——它在逻辑上被忽略，物理空间由 §8 的段级回收处理；
- 因此恢复后段内可能同时存在"已提交/未提交"记录，这是正常状态，不是错误。

## 7. 恢复算法

```
1. 读 MANIFEST（校验和、cutoff、next_version、covered 版本、SSTable 列表、wal_format）
   wal_format 不匹配 → 拒绝打开（§9）
2. 读 Commit WAL（所有段，按段号顺序）：
     committed[txn] = (version, [FragmentRef],)
     committed_versions = 已提交版本集合
     max_committed = max(committed_versions)
3. 对每个 shard：
     顺序读段内记录：
       kSelfCommit            → 应用
       kFragment(txn, ref)    → 仅当 txn ∈ committed **且** ref 与当前记录位置完全匹配 → 应用
                                否则跳过（保留在段内，不截断）
     段尾不完整记录 → 截断到最后一个完整记录末尾
4. 重放 ExpireBatch：按版本顺序恢复 cutoff（只认已提交的）
5. visible_version = 连续已提交且已应用的版本
   next_version    = max(MANIFEST.next_version, 已见最大 version + 1)
6. 重建 SeriesIndex 与 last_timestamps
```

> 匹配语义：`FragmentRef` 的 `(shard, segment_id, start_offset, end_offset)` 必须与
> 实际记录位置一致，否则视为不一致状态（记录损坏），按损坏处理并拒绝打开。

## 8. WAL 回收（意见 6）

一个段可以删除，当且仅当：

1. 该段内所有记录对应的 txn **要么已提交并 Apply，要么已确认 Abort**；
2. 该段涉及的所有分片数据已被 **Manifest 覆盖**（`covered_version >= 段内最大 version`）；
3. 段的删除与 Manifest 发布顺序一致：**先发布 Manifest，再删除段**（崩溃只会留下可重放的旧段）。

CommitWAL 段的回收额外要求：该段内所有 txn 都满足上面两条。

## 9. 开关与格式标记（意见 7）

- `Options::sharded_wal` **默认 false**；打开后启用分片 WAL；
- MANIFEST 增加 `wal_format` 字段（`1=global`、`2=sharded`）；
- 打开已有库时：格式不匹配 → 拒绝打开并提示迁移；**禁止运行时混用**；
- 迁移工具（后续）：把 `wal/*.log` 逐条转成 `wal/shard-N/*.log` 与 `wal/commit/*.log`。

## 10. 性能（意见 8）

预期（待实测，不作为结论）：跨分片事务需等待所有参与分片的日志持久化，并额外等待
Commit 决策持久化，同步开销可能高于当前全局 WAL。实际影响取决于：

- 磁盘并发 fsync 行为；
- 组提交批次大小（本设计已把 Fragment / SelfCommit / Commit 决策都做成可批量）；
- 跨分片批次占比。

验证方式：`sharded_wal` 开/关各跑 `perf_ingest`、`perf_ingest_hot`、`perf_mixed`，
分别报告单分片与跨分片场景。

## 11. 实现顺序（修订后）

| 阶段 | 内容 | 验收 |
| --- | --- | --- |
| 1 | 段管理 + 记录编解码 + `kSelfCommit` 快路径（开关默认关） | 现有 23 测试全绿 |
| 2 | `kFragment` + `kCommitBatch` + 组提交 | 跨分片测试通过 |
| 3 | 恢复：精确匹配、只截尾、Expire 重放 | §6.1 矩阵逐条覆盖 |
| 4 | 段级回收（§8 三个条件） | Flush/Compaction 后段可独立删除 |
| 5 | 故障注入 ≥50 次 | 要么数据完好，要么干净失败 |
| 6 | 开关打开做 A/B | 单分片/跨分片分别报数 |
