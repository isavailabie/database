# LevelDB 实验报告：TTL 与统计元数据

姓名：马舒童  10235501462 宋琪语10235501465　　

分工：
宋琪语：TTL 与统计元数据的实现；报告撰写；
马舒童：故障恢复与一致性（MANIFEST 检查点与滚动、重开时的三条重建路径、崩溃点注入与恢复验证、WAL 尾部损坏处理、ENOSPC 场景），测试补充

课程：CDMS.XuanZhou.2026Fall.Dase · Project1　

---

### 1. 实验要求

在同一份 LevelDB 源码中同时实现 TTL 与统计元数据，两者必须协同工作：TTL 负责数据的生命周期，统计负责描述数据库的物理规模，二者共享同一套版本与序列号机制。

### 1.1 TTL

| 要求 | 具体内容 |
|---|---|
| 写入有效期 | 接口 `Put(options, key, value, ttl)`，单位秒，用 `Options::env->NowMicros()` 取时间并保存**绝对到期时间**；普通 Put 永不过期 |
| 过滤过期 | 读取时间 `>=` 到期时间即 `NotFound`，迭代器跳过该记录；快照使用其创建时的序列号与时间 |
| 清理 | compaction 删除可回收的过期记录，同时保护活跃快照与仍被读取的视图 |
| 恢复 | 刷盘、合并、重开后保留原到期时间，不重新计算 |
| 边界 | `ttl==0` 写入一条立即过期的新版本并正常分配序列号；时间计算溢出返回 `InvalidArgument`，不写数据、不推进序列号 |

### 1.2 统计元数据

| 要求 | 具体内容 |
|---|---|
| 三个字段 | `total_entries`（未回收的内部记录数）、`distinct_keys`（按用户键去重）、`seq`（最新已发布序列号） |
| 统计范围 | 当前 memtable、immutable memtable、当前版本中的有效 SST；WAL 副本不重复计数 |
| 维护 | 写入、覆盖、删除、批量写入后更新；compaction 回收后扣减 |
| 一致性 | 一次查询返回的三个字段属于同一时刻；并发写入与 WriteBatch 不能让查询读到半个批次 |
| 恢复 | 重开后统计正确；可保存统计，也可由持久化记录重建 |
| 接口约束 | 忽略 `ReadOptions::snapshot`；`stats` 为空指针返回 `InvalidArgument` |

------

### 2. 实验思路

TTL 需要知道"这条记录什么时候失效"，这是**记录级**的属性；统计需要知道"当前版本里一共有多少条记录、涉及多少键"，这是**版本级**的属性。而 LevelDB 恰好已经把这两层分好了：记录级的东西编码在值里，版本级的东西记录在 `VersionEdit` 里。

于是整体方案自然分成两条线，共用同一套 WAL → memtable → SST → compaction 的通道：

- **到期时间写进值里**：新增一个值类型，值的前 8 字节是绝对到期时间，后面是用户数据。刷盘和重启不重新计算期限；compaction 仍会按 TTL、快照和更深层旧版本判断是否回收。
- **SST 统计变化写进 `VersionEdit` 里**：flush/compaction 对 SST 统计的增减与文件变更写进同一条记录，一起提交、一起生效。内存记录的计数在写入时更新，恢复时从 WAL 重放得到。

#### TTL 的思路

- **到期时间存进值里**（新增一个值类型，前 8 字节是绝对时间戳）——这样 WAL、memtable、SST 的现有通道全部复用，刷盘、合并、重启都只是字节搬运；
- **过期在读取时等同于删除**——一旦命中过期版本就终止查找，不向更旧的数据源回退；
- **回收由视图时间决定**——compaction 只在"所有活跃视图都看不到它"之后才真正删除。

#### **统计表的思路：**

- **一张表，两列计数**。每个用户键占一行，行内两列：`sst_count` 记这个键在当前版本 SST 里有多少条记录，`runtime_count` 记它在 memtable 和 immutable 里有多少条。行数天然就是去重键数，两列之和就是记录总数——同时得到 `distinct_keys` 和 `total_entries`。
- **flush 只做同一行内的转移，只有 compaction 才真正减少**。flush 是把记录从内存搬到磁盘，条数没变，所以只是 `runtime_count −n`、`sst_count +n`；只有 compaction 丢弃记录时总数才减少。
- **SST 统计挂在版本变更上，而不是单独落盘**。flush/compaction 对 SST 统计的变化作为 `VersionEdit` 的一部分，与文件增删写进同一条 MANIFEST 记录。内存侧计数随写入更新，并在恢复时从 WAL 重建。
- **内存与磁盘分开保存**。高频变化的逐键表只放内存；磁盘上不存这张表，只存重建它所需的输入——SST 侧的逐键条数（检查点 + 增量）和 WAL 里的原始记录。重开时两路合并：MANIFEST 回放填 `sst_count` 列，WAL 重放后扫一遍内存表填 `runtime_count` 列，表就重新构建完成。

---

### 3. 实现过程

#### 3.1  TTL 值类型

**记录格式**

`db/dbformat.h`

```cpp
enum ValueType {
  kTypeDeletion = 0x0,
  kTypeValue    = 0x1,
  kTypeTTLValue = 0x2      // 只有这个类型带 8 字节前缀
};
static const ValueType kValueTypeForSeek = kTypeTTLValue;

inline std::string EncodeTTLValue(const Slice& value, uint64_t expiration_micros) {
  std::string encoded;
  PutFixed64(&encoded, expiration_micros);     // 前 8 字节：绝对到期微秒
  encoded.append(value.data(), value.size());  // 后面：用户值
  return encoded;
}

inline bool ParseInternalKey(const Slice& internal_key, ParsedInternalKey* result) {
  const size_t n = internal_key.size();
  if (n < 8) return false;
  uint64_t num = DecodeFixed64(internal_key.data() + n - 8);
  uint8_t c = num & 0xff;
  result->sequence = num >> 8;
  result->type = static_cast<ValueType>(c);
  result->user_key = Slice(internal_key.data(), n - 8);
  return (c <= static_cast<uint8_t>(kTypeTTLValue));
}
```

- `kValueTypeForSeek` 一起改：内部键按 `(sequence, type)` 降序排，seek 要用最大的 type 才能命中同序列号的 TTL 版本。

统一判据函数，三处读取共用：

```cpp
inline Status DecodeValueForRead(ValueType type, Slice* value, uint64_t read_time_micros) {
  if (type == kTypeDeletion) return Status::NotFound(Slice());
  if (type == kTypeValue)    return Status::OK();
  if (type != kTypeTTLValue || value->size() < 8) return Status::Corruption("invalid TTL value");
  const uint64_t expiration_micros = DecodeFixed64(value->data());
  value->remove_prefix(8);                       // 对用户隐藏前缀
  return read_time_micros >= expiration_micros ? Status::NotFound(Slice()) : Status::OK();
}
```

 **写入路径：复用原有 Write 通道**

`db/db_impl.cc`

```cpp
Status DBImpl::Put(const WriteOptions& options, const Slice& key,
                   const Slice& value, uint64_t ttl) {
  const uint64_t now = env_->NowMicros();
  // 乘法与加法溢出一次覆盖；失败时不写 WAL、不占序列号
  if (ttl > (std::numeric_limits<uint64_t>::max() - now) / 1000000) {
    return Status::InvalidArgument("TTL deadline overflow");
  }
  WriteBatch batch;
  WriteBatchInternal::PutWithExpiration(&batch, key, value, now + ttl * 1000000);
  return Write(options, &batch);      // 复用原有 Write
}
```

- 保证 `now + ttl*1e6` 不溢出，等价于 `ttl <= (UINT64_MAX - now) / 1e6`（整除向下取整）。

- `ttl == 0` 时产生一条 `到期时间 = now` 的记录，序列号照常分配，读的时候立刻判定过期。

`db/write_batch.cc`

新增批次记录类型与处理器回调：存储侧的 handler 保留到期时间，普通 handler 只看到用户值。

```cpp
void WriteBatchInternal::PutWithExpiration(WriteBatch* batch, const Slice& key,
                                           const Slice& value, uint64_t expiration_micros) {
  SetCount(batch, Count(batch) + 1);
  batch->rep_.push_back(static_cast<char>(kTypeTTLValue));
  PutLengthPrefixedSlice(&batch->rep_, key);
  PutLengthPrefixedSlice(&batch->rep_, EncodeTTLValue(value, expiration_micros));
}
```

**读取路径：过期等于删除**

**memtable**（`db/memtable.cc`）：命中即返回，不再查 imm 与 SST。

```cpp
Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
*s = DecodeValueForRead(static_cast<ValueType>(tag & 0xff), &v, read_time_micros);
if (s->ok()) value->assign(v.data(), v.size());
return true;      // 关键：过期也算"命中"，阻止继续查 imm 和 SST
```

**SST**（`db/version_set.cc`）：把过期映射成 `kDeleted` 状态，遍历立即停止。

```cpp
Slice value = v;
const Status status = DecodeValueForRead(parsed_key.type, &value, s->read_time_micros);
s->state = status.ok() ? kFound : status.IsNotFound() ? kDeleted : kCorrupt;
if (s->state == kFound) s->value->assign(value.data(), value.size());
```

- `kDeleted` 与真正的删除标记走同一条分支，所以"过期"和"删除"在下游是同一个语义。

**迭代器**（`db/db_iter.cc`）：正向把该键加入 `skip` 集合；反向清除已保存的候选值

- 正向遍历**从新到旧**，先遇到最新版本，一旦它过期就记下这个键、跳过其余版本：

```
if (!skipping || user_comparator_->Compare(ikey.user_key, *skip) > 0) {
  Slice value = iter_->value();
  const Status decoded = DecodeValueForRead(ikey.type, &value, read_time_micros_);
  if (decoded.ok()) { valid_ = true; saved_key_.clear(); return; }
  if (!decoded.IsNotFound()) status_ = decoded;
  SaveKey(ikey.user_key, skip);    // 记下这个键，跳过它所有更旧的版本
  skipping = true;
}
```

- 反向遍历**从旧到新**，顺序相反：先遇到旧版本并可能把它存成候选值，等走到更新的过期版本时，必须把候选值清掉：

```
value_type = decoded.ok() ? kTypeValue : kTypeDeletion;
if (value_type == kTypeDeletion) {     // 过期与删除同等对待
  saved_key_.clear();
  ClearSavedValue();
}
```

最后输出给用户前还要把 8 字节前缀剥掉——正向按内部键的类型位判断，反向保存时已剥离过：

```
Slice value() const override {
  if (direction_ == kReverse) return saved_value_;
  Slice value = iter_->value();
  if ((DecodeFixed64(iter_->key().data() + iter_->key().size() - 8) & 0xff) == kTypeTTLValue)
    value.remove_prefix(8);
  return value;
}
```

**视图冻结：快照与迭代器**

`db/snapshot.h`

```cpp
class SnapshotImpl : public Snapshot {
  const SequenceNumber sequence_number_;
  const uint64_t read_time_micros_;     // 新增：创建时的物理时间
};
```

- `GetSnapshot()` 同时记下当时的序列号和 `NowMicros()`；
- `NewIterator()` 时，如果带了快照就用快照的时间，否则取一次 `NowMicros()` 存进 `DBIter`，整个遍历期不变。

**compaction **

`db/db_impl.cc` 的 `DoCompactionWork`：

```cpp
compact->expiration_cutoff_micros = snapshots_.OldestReadTime(start_micros);
// cutoff = min(当前时间, 所有活跃快照创建时间的最小值)
```

删除条件（三者同时满足）：

```
} else if ((ikey.type == kTypeDeletion ||
            (ikey.type == kTypeTTLValue && input->value().size() >= 8 &&
             DecodeFixed64(input->value().data()) <= compact->expiration_cutoff_micros)) &&
           ikey.sequence <= compact->smallest_snapshot &&
           compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
  drop = true;
}
```

- 到期时间 <= cutoff：保证所有还在读的视图都已经看不见它；
- sequence <= smallest_snapshot：没有更新的快照还需要这个版本
- IsBaseLevelForKey：更深层没有同键旧数据——否则这条过期记录要留着当墓碑，删了旧值会复活

**L6优化**

常规 compaction 是 L→L+1，最深层没有 L+1，记录永远等不到下一次合并，无法及时清理过期值。所以给 `Compaction` 加了 `output_level_`，支持同层重写：

```cpp
Compaction::Compaction(const Options* options, int level, bool rewrite)
    : level_(level),
      output_level_(rewrite || level == config::kNumLevels - 1 ? level : level + 1),
      ...
```

`CompactRange` 在逐层压缩之后，找出最深的重叠层再重写一次：

```
int rewrite_level = -1;
for (int level = 0; level < config::kNumLevels; ++level)
  if (versions_->current()->OverlapInLevel(level, begin, end)) rewrite_level = level;
if (rewrite_level >= 0) TEST_CompactRange(rewrite_level, begin, end, true);
```

**时钟回拨的保护。** 合并期间如果墙上时钟倒退，可能冒出"比 cutoff 还早"的新快照，此时安装结果会破坏它的视图，所以安装前再复核一次，不满足就丢弃本次输出：

```
if (snapshots_.OldestReadTime(env_->NowMicros()) < compact->expiration_cutoff_micros) {
  Log(options_.info_log, "discarding compaction after TTL clock rollback");
} else {
  status = InstallCompactionResults(compact);
}
```

**WAL恢复**

`db/write_batch.cc`， 恢复时调用

```
void PutWithExpiration(const Slice& key, const Slice& value,
                       uint64_t expiration_micros) override {
  mem_->Add(sequence_, kTypeTTLValue, key, EncodeTTLValue(value, expiration_micros));
  sequence_++;
}
```

- **WAL 里存的是最终编码**（8 字节绝对时间 + 用户值），恢复时原样写回 memtable，不做任何转换；
- **flush 与 compaction 只是字节搬运**：memtable → SST、SST → 新 SST，到期时间字段不动；
- **关库期间的时间也计入**：不存在“重开时重新计算”的代码，不会写错。

**WAL 尾部损坏的处理**

`reuse_logs` 打开时，数据库重开会**复用最后一个 WAL 文件**继续写。但如果这个文件的尾部本来就是残缺的（进程被 kill 时写了半条记录、CRC 校验不通过、块尾不完整），直接复用就会在垃圾数据后面追加，下次重放时可能把新记录也判成损坏。所以需要知道“这条日志的尾部干不干净”。

`db/log_reader.h`

```
// 读取过程中是否遇到过损坏或不完整的尾部
bool HasCorruptionOrTruncatedTail() const { return has_corruption_or_truncated_tail_; }
```

它在三处被置位：`ReportCorruption`、`ReportDrop`（CRC 或长度异常）、以及读到块尾长度不足/字节数不够时的 EOF 分支。

`db/db_impl.cc` 的 `RecoverLogFile` 用它决定能否复用：

```
const bool clean_log_tail = !reader.HasCorruptionOrTruncatedTail();
delete file;

// 只有尾部干净时，才允许复用这个日志文件继续追加
if (status.ok() && clean_log_tail && options_.reuse_logs && last_log && compactions == 0) {
  assert(logfile_ == nullptr);
  ...
}
```

**尾部有问题的日志不复用，换一个新的**。已经成功重放的前缀仍然有效，只是不再往这个文件里写，避免“旧垃圾 + 新记录”混在同一条日志里。

#### 3.2 统计数据

统计由四个部分组成：逐键明细（两列计数）、SST 总数、内存总数、以及版本序列号。前三个在 `VersionSet` 里，第四个属于版本属性。

**数据结构**

`db/version_set.h` 新增了一个条目结构和一张映射：

```cc
struct StatsEntry {
  uint64_t sst_count = 0;      // 当前 Version 的 SST 里，这个键有多少条
  uint64_t runtime_count = 0;  // memtable + immutable 里，这个键有多少条
};

// 用数据库自己的用户比较器排序，支持透明查找（可直接传 Slice，避免构造临时 string）
struct StatsKeyLess {
  using is_transparent = void;
  const Comparator* comparator;
  bool operator()(const std::string& a, const std::string& b) const { ... }
  bool operator()(const std::string& a, const Slice& b) const { ... }
  bool operator()(const Slice& a, const std::string& b) const { ... }
};

using StatsCounts  = std::map<std::string, StatsEntry, StatsKeyLess>;
using PendingStats = std::map<std::string, uint64_t, StatsKeyLess>;
```

`VersionSet` 里新增的成员：

```cc
StatsCounts stats_counts_;     // 逐键明细
uint64_t    stats_total_;      // 所有 SST 记录总数
uint64_t    runtime_total_ = 0;// 内存中记录总数
bool        has_stats_snapshot_;// 映射是否有效（无效则需重建）
```

三个访问器：

```
uint64_t StatsTotal()        const { return stats_total_ + runtime_total_; }
uint64_t SstStatsTotal()     const { return stats_total_; }       // 写检查点用
uint64_t DistinctStatsKeys() const { return stats_counts_.size(); }
```

`DBImpl` 侧还加了一个 `stats_ready_` 标志，确保“表已经建好”之后才允许保存缓存。

**写入时的记账**

`DBImpl::Write` 在执行完 WAL 与 memtable 之后，回到锁内先发布序列号、再累加计数：

```cc
if (valid_batch && status.ok() &&
    WriteBatchInternal::Count(write_batch) > 0) {
  versions_->SetLastSequence(last_sequence);      // 先发布数据
  for (const Slice& key : batch_keys.keys) {
    Status stats_status = AddStatsEntry(key);      // 再累加计数
    if (!stats_status.ok()) {
      // 计数异常时整体重建一次，避免局部不一致
      status = RebuildStats();
      if (!status.ok()) RecordBackgroundError(status);
      break;
    }
  }
}
```

`AddStatsEntry` 只是转到 `VersionSet::AddRuntimeStats`，后者做了三件事：查键、溢出检查、更新两个数（`runtime_count` 与 `runtime_total_`）。

下面展示的是基本记账逻辑；最终代码对按比较器递增的新键使用了插入位置提示，优化过程见 6.3 节。

```
Status VersionSet::AddRuntimeStats(const Slice& key, uint64_t count) {
  if (count == 0) return Status::OK();
  auto found = stats_counts_.find(key);
  if (count > std::numeric_limits<uint64_t>::max() - runtime_total_ ||
      (found != stats_counts_.end() &&
       count > std::numeric_limits<uint64_t>::max() - found->second.runtime_count)) {
    return Status::Corruption("runtime stats count overflow");
  }
  if (found == stats_counts_.end()) {
    stats_counts_.emplace(key.ToString(), StatsEntry{0, count});  // 新键建行
  } else {
    found->second.runtime_count += count;                         // 老键加数
  }
  runtime_total_ += count;
  return Status::OK();
}
```

键的收集是在解锁期间完成的——`WriteBatch::Iterate(&batch_keys)` 同时格式校验，坏批次不会进入 WAL。

**flush 时的转移**

`WriteLevel0Table` 里要把 memtable 的逐键条数算出来。因为 memtable 迭代器按用户键有序，同一个键的所有版本相邻，所以**顺序扫描 + 计数连续段**即可，不需要中间 map：

```
std::vector<std::pair<std::string, uint64_t>> counts;
std::string current_key;
uint64_t current_count = 0;
for (stats_iter->SeekToFirst(); stats_iter->Valid(); stats_iter->Next()) {
  ParsedInternalKey key;
  if (!ParseInternalKey(stats_iter->key(), &key)) { ... }
  if (current_count == 0 ||
      user_comparator()->Compare(key.user_key, current_key) != 0) {
    if (current_count != 0) counts.emplace_back(std::move(current_key), current_count);
    current_key = key.user_key.ToString();
    current_count = 1;
  } else {
    ++current_count;
  }
}
```

算完之后写进 VersionEdit，与文件变更同批提交：

```
edit->MarkStatsDelta();
for (const auto& entry : counts) {
  edit->AddStatsDelta(entry.first, entry.second, /*add=*/true);   // sst_count += n
}
```

`LogAndApply` 成功之后，再扣掉内存侧（这一步在 `CompactMemTable` 里）：

```
for (const auto& transfer : runtime_transfers) {
  versions_->RemoveRuntimeStats(transfer.first, transfer.second);
}
```

```
void VersionSet::RemoveRuntimeStats(const Slice& key, uint64_t count) {
  auto found = stats_counts_.find(key);
  assert(found != stats_counts_.end());
  found->second.runtime_count -= count;
  runtime_total_ -= count;
  if (found->second.runtime_count == 0 && found->second.sst_count == 0) {
    stats_counts_.erase(found);        // 两列都归零才删行
  }
}
```

**compaction 时的扣减**

合并过程中被丢弃的每条记录按用户键累加到 `CompactionState::dropped_entries`，新版本时统一提交减量：

```
if (drop) {
  ++compact->dropped_entries[ikey.user_key];
}
...
compact->compaction->edit()->MarkStatsDelta();
for (const auto& dropped : compact->dropped_entries) {
  compact->compaction->edit()->AddStatsDelta(dropped.first, dropped.second, /*add=*/false);
}
```

compaction 不碰 `runtime_count`——它的输入只有 SST 文件。

**落盘：VersionEdit 的新字段**

`db/version_edit.cc` 新增 5 个 tag：

```
kFormatVersion    = 10,
kStatsSnapshotTotal = 11,   // 检查点总条数
kStatsSnapshotEntry = 12,   // 检查点条目：(key, count)
kStatsDelta         = 13,   // 增量：(key, count, add)
kStatsDeltaMarker   = 14    // 增量标记：文件变了但计数没变
```

```
if (has_stats_snapshot_) {
  PutVarint32(dst, kStatsSnapshotTotal);
  PutVarint64(dst, stats_snapshot_total_);
  for (const auto& entry : stats_snapshot_) {
    PutVarint32(dst, kStatsSnapshotEntry);
    PutLengthPrefixedSlice(dst, entry.first);
    PutVarint64(dst, entry.second);
  }
}
for (const StatsDelta& delta : stats_deltas_) {
  PutVarint32(dst, kStatsDelta);
  PutLengthPrefixedSlice(dst, delta.key);
  PutVarint32(dst, delta.add ? 1 : 0);
  PutVarint64(dst, delta.count);
}
if (has_stats_delta_ && stats_deltas_.empty()) {
  PutVarint32(dst, kStatsDeltaMarker);
}
```

- 逐键记录里不带序列号；序列号是整条 VersionEdit 的全局字段（tag 4），由 `LogAndApply` 统一写入。

- 解码侧做了严格校验：键必须严格递增、计数非零、累加和必须与声明的总数一致，否则判为损坏：

```
if (entry.second == 0 ||
    (have_previous &&
     icmp_.user_comparator()->Compare(previous, entry.first) >= 0) ||
    entry.second > std::numeric_limits<uint64_t>::max() - restored_total) {
  return Status::Corruption("invalid stats snapshot ordering or count");
}
```

**重开重建**

`DB::Open` 中统计按三条路径处理：

```
// WAL 恢复与它的 SST 提交都完成之后
if (s.ok() && !impl->LoadStatsCache()) {
  s = impl->versions_->HasStatsSnapshot()
          ? impl->RestoreStatsFromManifest()   // 路径 A：正常
          : impl->RebuildStats();              // 路径 B：兜底
}
if (s.ok() && !impl->versions_->HasStatsSnapshot()) {
  VersionEdit stats_snapshot;
  s = impl->BuildStatsSnapshot(&stats_snapshot);
  if (s.ok()) s = impl->versions_->LogAndApply(&stats_snapshot, &impl->mutex_);
}
```

- **A 正常路径**：`VersionSet::Recover()` 回放 MANIFEST 得到 `sst_count` 列；`RestoreStatsFromManifest()` 只扫 mem/imm 补 `runtime_count` 列，**不碰任何 SST 数据块**。

```
Status DBImpl::RestoreStatsFromManifest() {
  std::vector<MemTable*> memtables;
  if (mem_ != nullptr) memtables.push_back(mem_);
  if (imm_ != nullptr) memtables.push_back(imm_);
  for (MemTable* mem : memtables) {
    std::unique_ptr<Iterator> iter(mem->NewIterator());
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ParsedInternalKey key;
      ParseInternalKey(iter->key(), &key);
      Status status = versions_->AddRuntimeStats(key.user_key);
      if (!status.ok()) return status;
    }
  }
  return Status::OK();
}
```

- **B 兜底路径**：没有检查点（老格式库）或回放校验失败时，`RebuildStats()` 清空映射，用内部迭代器遍历 mem、imm、以及当前版本的所有 SST，逐条重建。代价是 O(全部内部记录)。
- **C 旧库缓存**：`db/stats_cache.cc` 里的 `LoadStatsCache()` 只在缺少检查点时尝试，读取后立即删除文件；校验 magic、CRC32C、指纹（比较器名 + 序列号 + 每层文件元数据）、键顺序与总数，任一失败即退回 A/B。

路径 C 用到的缓存文件需要在文件名解析里注册一个新类型，否则它会被当成未知文件，在清理旧文件时被误删：

```
// db/filename.cc
} else if (rest == "STATS" || rest == "STATS.tmp") {
  *number = 0;
  *type = kStatsCacheFile;
}
```

```
// db/db_impl.cc RemoveObsoleteFiles
case kStatsCacheFile:
  keep = true;
  break;
```

写入发生在数据库关闭时，且是**尽力而为**（失败只记日志，不影响正确性）：

```
void DBImpl::SaveStatsCache() {
  if (!stats_ready_ || !StatsCacheEligible()) return;   // 表没建好、或内存非空则不写
  std::string encoded;
  PutLengthPrefixedSlice(&encoded, "LDBSTATS1");
  PutLengthPrefixedSlice(&encoded, StatsFingerprint());   // 比较器名 + 序列号 + 每层文件元数据
  PutFixed64(&encoded, versions_->StatsTotal());
  PutFixed64(&encoded, versions_->DistinctStatsKeys());
  // ... 逐键条目 ...
  PutFixed32(&encoded, crc32c::Value(encoded.data(), encoded.size()));
  const std::string temporary = dbname_ + "/STATS.tmp";
  Status status = WriteStringToFileSync(env_, encoded, temporary);
  if (status.ok()) status = env_->RenameFile(temporary, dbname_ + "/STATS");
}
```

读取时校验 magic 与 CRC、并把指纹与当前文件集合比对，任一不符就丢弃；而且**读完立刻删除文件**，避免下次误用。它只是一条加速路径，最坏情况是退回 MANIFEST 回放或全量重建。

**MANIFEST 滚动与检查点**

统计检查点不是每次 flush 都写，而是只在 **MANIFEST 被重写（滚动）** 时写一次；平时只在文件变更的 `VersionEdit` 里追加增量。这样写放大与“变化的键数”成正比，而不是与“全部键数”成正比。

但检查点本身随去重键数增长（100k 键约 1.2 MB），如果沿用原来的滚动阈值（`TargetFileSize`），检查点一大就会**每次 flush 都触发重写**。所以阈值改成与检查点大小挂钩：

```
// 阈值 = 检查点大小 + max(检查点大小, 目标文件大小)
static uint64_t ManifestRolloverThreshold(uint64_t snapshot_size, uint64_t target_size) {
  const uint64_t growth = std::max(snapshot_size, target_size);
  if (snapshot_size > std::numeric_limits<uint64_t>::max() - growth) {
    return std::numeric_limits<uint64_t>::max();
  }
  return snapshot_size + growth;
}
```

滚动的执行顺序是“**先建新文件，最后才切 CURRENT**”，保证任何时刻都有一个可用的旧恢复点：

```
if (create_new_manifest) {
  new_manifest_file = DescriptorFileName(dbname_, new_manifest_number);
  s = env_->NewWritableFile(new_manifest_file, &new_descriptor_file);
  if (s.ok()) {
    new_descriptor_log = new log::Writer(new_descriptor_file);
    s = WriteSnapshot(new_descriptor_log);        // 全量：文件元数据 + 统计检查点
  }
}
...
if (s.ok() && create_new_manifest) {
  s = SetCurrentFile(env_, dbname_, new_manifest_number);   // 成功之后才切换
}
```

失败时删除新文件、保留旧恢复点：

```
} else {
  delete v;
  if (create_new_manifest) {
    delete new_descriptor_log;
    delete new_descriptor_file;
    env_->RemoveFile(new_manifest_file);
  }
}
```

全量检查点的内容由 `WriteSnapshot` 写入：

```
edit.SetFormatVersion(kCurrentDatabaseFormatVersion);
if (has_stats_snapshot_) {
  std::vector<std::pair<std::string, uint64_t>> entries;
  for (const auto& entry : stats_counts_) {
    if (entry.second.sst_count != 0) entries.emplace_back(entry.first, entry.second.sst_count);
  }
  edit.SetStatsSnapshot(stats_total_, std::move(entries));
}
```

这里只导出 `sst_count != 0` 的键——只有内存记录、还没刷盘的键不需要进检查点，它们的计数由 WAL 重放恢复。

**故障恢复**

SST 统计增量与文件增删在 flush/compaction 时写进同一条 `VersionEdit`，同步 MANIFEST 后才安装版本并更新内存统计。WAL 中的运行时记录则在写入时更新内存计数，恢复时重新统计。统计字段的语义校验失败时可以丢弃辅助统计状态并扫描数据重建；如果 MANIFEST 记录本身无法解码或校验和损坏，恢复仍会失败，因为该记录同时包含版本文件元数据。

下面按崩溃点分别说明。

崩溃点一：WAL 追加或 fsync 失败。此时记录可能只写了一部分，WAL 位置不确定，所以**既不发布序列号，也不累加计数**，并把数据库置为错误状态：

```
if (status.ok() && batch_count > 0) {
  WriteBatchInternal::SetSequence(write_batch, last_sequence + 1);
  last_sequence += batch_count;
  status = log_->AddRecord(WriteBatchInternal::Contents(write_batch));
  log_error = !status.ok();
}
...
mutex_.Lock();
if (sync_error || log_error) {
  RecordBackgroundError(status);      // 之后所有写都被拒绝，直到重开恢复日志
}
```

后续写入会在 `MakeRoomForWrite` 里直接返回 `bg_error_`，不会在一个已知损坏的日志上继续写。重开后 WAL 按实际能读出的记录重放，统计也只按这些记录重建——两边一致。

崩溃点二：SST 已落盘，MANIFEST 还没提交。flush 先生成 SST，再把“新增文件 + 统计增量”写进 MANIFEST。如果进程在两者之间挂掉，那个新 SST 就变成孤儿文件：MANIFEST 里没有它，统计增量也没提交。重开时 `Recover()` 只按 MANIFEST 认定的文件构建版本，孤儿文件不在其中；随后 `RemoveObsoleteFiles()` 会把它删掉。所以统计与文件集合仍然一致，不需要特殊处理。

崩溃点三：MANIFEST 已写成功，但 CURRENT 还没切换（滚动场景）。MANIFEST 滚动时先创建新文件、写入全量检查点，最后才 `SetCurrentFile` 切换 CURRENT。若在切换前崩溃，CURRENT 仍指向旧 MANIFEST——新文件是孤儿，恢复用的是旧的检查点 + 旧增量，统计回到滚动前的状态，与当时有效的文件集合一致。

崩溃点四：MANIFEST 中的统计字段语义不一致。回放时会校验键顺序、计数、总数和增量边界。此类统计状态无效时，代码会丢弃辅助统计状态并退回扫描重建。若整条 MANIFEST 记录无法解码或校验和损坏，则恢复失败，不会仅靠统计重建继续打开数据库。

```
Status stats_status = ApplyStatsEdit(edit, &stats_counts_, &stats_total_, &has_stats_snapshot_);
if (!stats_status.ok()) {
  Log(options_->info_log, "Ignoring invalid MANIFEST statistics and rebuilding: %s\n",
      stats_status.ToString().c_str());
  stats_counts_.clear();
  stats_total_ = 0;
  runtime_total_ = 0;
  has_stats_snapshot_ = false;      // 后续走 RebuildStats()
}
```

同一条逻辑还覆盖“文件变更没带增量标记”这种不一致（`file edit is missing its stats delta marker`）——此时也丢弃统计、重建。

崩溃点五：统计增量与文件对不上且老格式库没有检查点。调用`RebuildStats()`：清空映射，用内部迭代器遍历 mem、imm 和当前版本的所有 SST，逐条重数。

**查询接口**

```
Status DBImpl::GetCurrentStats(const ReadOptions&, Stats* stats) {
  if (stats == nullptr) return Status::InvalidArgument("null stats");
  MutexLock lock(&mutex_);
  stats->total_entries = versions_->StatsTotal();         // 两列总数相加
  stats->distinct_keys = versions_->DistinctStatsKeys();  // 映射的行数
  stats->seq           = versions_->LastSequence();       // 最新已发布序列号
  return Status::OK();
}
```

`options` 被完全忽略（不读 `snapshot`），三个字段在同一临界区内读取，因此一定属于同一时刻。

**一致性约束与实现代价**

**约束：统计不能掉队。** `VersionSet::LogAndApply` 里加了一道检查——凡是改动了文件集合的 edit，必须携带增量标记，否则拒绝安装：

```
const bool changes_files = !edit->deleted_files_.empty() || !edit->new_files_.empty();
if (s.ok() && has_stats_snapshot_ && changes_files && !edit->has_stats_delta_) {
  s = Status::Corruption("file edit is missing its stats delta marker");
}
```

**两处针对性的优化：**

- **flush 顺序计数**：利用 memtable 按键有序，把“逐条插局部 map”改成“扫描计数连续段”；
- **检查点回放用 `emplace_hint`**：检查点按键有序写入且回放时已校验严格递增，插入位置总是末尾，省掉每键一次树搜索。

### 4. 测试用例增加

#### 4.1 测试组织

在课程提供的 `ttl_test`（2 个）与 `metadata_test`（3 个）之外，新增 5 个测试文件：

| 文件 | 用例数 | 覆盖重点 |
|---|---:|---|
| `test/ttl_read_view_test.cc` | 14 | 读语义、边界、快照与迭代器视图 |
| `test/ttl_gc_test.cc` | 14 | 物理回收、最深一层、与统计的联动 |
| `test/metadata_consistency_test.cc` | 17 | 三字段逐步核对、并发一致性、恢复 |
| `test/stats_cache_test.cc` | 25 | MANIFEST 检查点、滚动、故障注入 |
| `test/compaction_recovery_test.cc` | 3 | 合并过程与 WAL 崩溃恢复 |
| **合计** | **73** | |

Linux 且存在 `/dev/shm` 时，CMake 会为 `stats_cache_test` 设置专用 `TEST_TMPDIR`，使真实 ENOSPC 用例通过常规 CTest 自动运行。当前 Ubuntu 22 容器的 `stats_cache_test` 25/25 通过，ENOSPC 用例未跳过；不具备有界 tmpfs 的环境会跳过该用例。

#### 4.2 用例与验证点的对应

| 课程要求的验证点 | 对应用例 |
|---|---|
| 覆盖/删除组合，新值过期不回退旧值 | `ExpiredOverwriteNeverFallsBackAcrossSources`、`TTLOverwriteDeletesAndBatchLeaveExactSurvivors` |
| 零 TTL、时间溢出、恰好到期 | `ExactDeadlineAndOverflowSequence`、`ZeroEpochAndMaximumDeadline` |
| 快照、Seek、正反向遍历 | `SnapshotAndLazyIteratorFreezeTimeAndSequence`、`SeekAndDirectionChangesSkipExpiredNewVersions` |
| 刷盘/Compaction/重开后有效期不变 | `FlushingAndCompactionDoNotExtendTTL`、`DeepestLevelRewritesAllSelectedFilesAndIsRepeatable` |
| 回收后核对内部记录被清除 | `PublicCompactRangeCollectsNewlyFlushedRecords`、`ExpiredBarrierSurvivesUntilDeeperOldValueIsMerged` |
| 多线程与异常退出恢复 | `ConcurrentWritesReadsAndIterators`、`SyncWALSurvivesAbruptProcessExitAndRemainsCollectible` |
| 关库期间时间计入 | `ClosedTimeCountsTowardDeadlineAndIsNotRenewed` |
| 统计：逐步计算期望值 | `StepwiseOperationsBatchAndIgnoredSnapshot` |
| 统计：跨 mem/SST 去重与回收扣减 | `KeysAreDeduplicatedAcrossMemoryAndSSTs`、`ImmutableFlushTransferDoesNotDoubleCount` |
| 统计：并发一致性 | `ConcurrentQueriesNeverSeeHalfABatch` |
| 统计：与 TTL 联合 | `TTLExpiryAndInvalidDeadlineOnlyChangeOnGC` |
| 统计：重开与异常恢复 | `ManifestStatsTrackFlushCompactionAndWALRecovery`、`LegacyManifestIsUpgradedOnOpen` |

### 5. 测试结果

#### 5.1 课程 base 测试

```
$ ./build/release/ttl_test
[  PASSED  ] 2 tests.        (41.8 s)

$ ./build/release/metadata_test
[  PASSED  ] 3 tests.
```

#### 5.2 全部用例

```
$ ctest --test-dir build/release
100% tests passed, 0 tests failed out of 8
Total Test time (real) = 79.9 s
```

![image-20261104233111174](C:\Users\ASUS\AppData\Roaming\Typora\typora-user-images\image-20261104233111174.png)

#### 5.3 公开 benchmark

最终代码在 `--profile standard --matrix` 下运行公开 benchmark，每个配置重复三次取中位数，共 504 次执行，**168 个配置全部通过**；同批次的原生回归 8/8 通过。运行输出为

```bash
python3 benchmark/runner.py --kind combined --source "$PWD" \
  --output "$HOME/data/eval-quick-..." --profile quick --jobs 2
{"status": "pass", "passed": 22, "cases": 22}

python3 benchmark/runner.py --kind combined --source "$PWD" \
  --output "$HOME/data/eval-standard-..." --profile standard --matrix --jobs 2
{"status": "pass", "passed": 168, "cases": 168}
```

`full_suite` 均为 `true`，`regression.status` 均为 `pass`（固定原生回归、C 回归、POSIX Env 回归三组全过）。

| 场景                   |            吞吐 | p99 延迟 | 说明                     |
| ---------------------- | --------------: | -------: | ------------------------ |
| 普通写 `put`           |   115,604 ops/s |  10.3 µs | 基线 190,463，**−39.3%** |
| TTL 写 `ttl_put`       |   112,782 ops/s |   7.5 µs | 比普通写 **−2.4%**       |
| 读命中 `get_hit`       |   542,240 ops/s |   3.2 µs | 不经过新增写入路径       |
| 读未命中 `get_miss`    | 2,901,070 ops/s |   0.4 µs | 布隆过滤器命中           |
| 扫描 `scan`            | 2,343,840 ops/s |  <0.1 µs | —                        |
| 统计查询 `stats_query` |    2,641,330 /s |   0.1 µs | 单次约 **0.38 µs**       |
| 开库 `open`            |      76.9 ms/次 |  76.2 ms | 100k 键检查点解析        |

- 写入是唯一明显退化的场景：115,604 ops/s，相当于单次 8.65 µs，比基线的 5.25 µs 多 3.40 µs。这部分开销来自每条记录在统计映射中的一次插入，是精确去重的必然成本。

- TTL 写比普通写低 2.4%，说明 TTL 本身几乎不增加写入成本——两者走的是同一条写路径，区别只是值的编码多 8 字节。

- 读、扫描与统计查询不经过新增的写入维护逻辑，与基线持平。统计查询单次约 0.38 µs，主要成本是取锁和拷贝三个字段。

- 开库耗时 76.9 ms，主要来自解析 MANIFEST 里的统计检查点；该成本随去重键数线性增长。

![image-20261105012332826](C:\Users\ASUS\AppData\Roaming\Typora\typora-user-images\image-20261105012332826.png)

![image-20261105014350682](C:\Users\ASUS\AppData\Roaming\Typora\typora-user-images\image-20261105014350682.png)

#### 5.5 与基线对比

用未修改的 base 提交（`775e7db`）跑相同场景：

| 场景 | 基线 | 本实现 | 差距 |
|---|---:|---:|---:|
| `perf_write`（N=100k） | 190,463 | 115,604 | −39.3% |
| `perf_write`（N=10k） | 188,500 | 134,431 | −29% |
| `parallel_put` 8 线程（N=100k） | 44,972 | 34,546 | −23% |

多线程写入吞吐低于单线程在基线中同样存在（基线 8 线程为单线程的 1/13.8），属于 LevelDB 单写者互斥锁的固有行为，不是本实验引入的。**单线程写入慢 29%~39% 是本实现的主要性能代价**，来源与优化过程见 6.1。

---

### 6. 实验遇到的问题

#### 6.1 问题一：写入时间明显增加

| 场景   |    基线 |  优化前 |                           优化后 |
| ------ | ------: | ------: | -------------------------------: |
| N=10k  | 188,500 | 109,024 |                  134,431（+23%） |
| N=100k | 190,463 | 112,242 | 121,355（+8%，最终复测 115,604） |

换算成单次写：基线 5.25 µs，本实现 **8.65 µs / 多花 3.40 µs**。

**定位过程：写探针，控制变量逐层隔离**

独立的探针程序：**打开数据库、连续写 N 条 256 字节记录、计时**，并把 `write_buffer_size` 作为可控变量。

思路是：写 100k × 256 B = 25 MB 数据。

- 缓冲设 4 MB → 触发约 6 次 flush；
- 缓冲设 64 MB / 256 MB → 全程不触发 flush。

两组一减，就把 **flush 的成本**与**每次写的成本**分离开了。

| 缓冲 | 基线 | 本实现 | 每次写差距 |
|---|---:|---:|---:|
| 4 MB（有 flush） | 553,882 | 276,106 | 1.82 µs |
| 64 MB（无 flush） | 557,512 | 402,176 | 0.69 µs |
| 256 MB（完全不 flush） | 556,418 | 407,499 | 0.66 µs |

- **基线的三种配置几乎一致**（flush 在后台，不占用写路径）；
- 本实现把 flush 关掉后，4 MB 与 256 MB 配置的差距从 1.82 µs 降到 0.66 µs；这组数据表明约 64% 的差距与 flush 配置相关，约 0.66 µs 的差距在无 flush 配置下仍存在；
- 再做一组"100k 次写但只有 1000 个不同键"的对照，差距进一步缩到 0.25 µs，说明每次写的那部分主要花在**统计映射随键数变大**上。

**瓶颈在 flush 时对 memtable 的二次遍历与统计记账。**

flush 路径原来是这样：遍历 memtable，把 1.6 万条记录逐条插进一个局部的 `std::map`，再逐键生成增量。每条记录一次节点分配、一次树插入。

但 memtable 的迭代器本身就是**按用户键排序**的（InternalKeyComparator：先比 user key 升序，再比 sequence 降序），同一个键的所有版本必然相邻。所以根本不需要 map：

```cpp
// 改前：每条记录一次 map 插入
auto found = counts.find(key.user_key);
if (found == counts.end()) counts.emplace(key.user_key.ToString(), 1);
else                      ++found->second;

// 改后：只比较相邻键，顺序计数连续段
if (current_count == 0 || user_comparator()->Compare(key.user_key, current_key) != 0) {
  if (current_count != 0) counts.emplace_back(std::move(current_key), current_count);
  current_key = key.user_key.ToString();
  current_count = 1;
} else {
  ++current_count;
}
```

一次顺序扫描，只在**每个去重键**上分配一次字符串，而不是每条记录一次。

**效果**：

| 指标 | 优化前 | 优化后 |
|---|---:|---:|
| 探针 4 MB 缓冲 | 246,180 | 276,106（**+12%**） |
| 探针 64 MB 缓冲（对照组） | 394,644 | 402,176（+2%，说明测量可信） |
| `perf_write` N=10k | 109,024 | 134,431（+23%） |
| `perf_write` N=100k | 112,242 | 121,355（+8%） |

与基线的差距由 −42% 收敛到 −29%～−39%。

---

#### 6.2 问题二：统计表关闭后存储

统计需要回答三个字段，其中 `distinct_keys` 要求**精确去重**——必须知道每个键在 memtable、immutable、各层 SST 里分别有多少条记录，才能判断某个键是否已被完全回收。一个全局总数支撑不了这件事，所以持久化结构必须是**逐键**的。

但逐键数据有两个互相矛盾的诉求：

- 写入是高频的，每秒上万次，**不能每次都落盘**；
- 崩溃/重开后必须能恢复，**不能只放内存**。

解决方案：**内存与磁盘分开，各管一段**

把统计拆成两个层次，各自承担不同的频率与责任：

**内存层（高频、可重建）**：一张 `std::map<user_key, {sst_count, runtime_count}>`。写入时统计计数只更新内存表，不额外持久化；用户数据仍按正常路径写入 WAL。

**磁盘层（低频、随版本提交）**：不单独建文件，而是把统计**挂在已有的 MANIFEST 上**，以"检查点 + 增量"的形式存在：

- 检查点：MANIFEST 重写时写入一次全量逐键快照；
- 增量：每次 flush / compaction 的 VersionEdit 里追加若干条 `键 + 条数 + 加减方向`。

| 数据 | 存在哪 | 谁负责恢复 |
|---|---|---|
| SST 里各键的条数 | MANIFEST（检查点 + 增量） | 回放 MANIFEST |
| memtable 里各键的条数 | **不持久化**，只存在于 WAL 的记录里 | 重放 WAL 时重新数 |

1. **统计计数不额外增加写入**：高频计数更新在内存完成；SST 统计增量随 flush/compaction 写入 MANIFEST，用户数据仍写入 WAL。
2. **崩溃后可以重建**：内存那部分虽然丢了，但 WAL 里本来就有这些记录，重放时数一遍即可——**统计是派生数据，不需要自己承担持久化责任**。
3. **与版本严格同步**：增量与文件变更写在同一条 VersionEdit 记录里，一起提交、一起生效，不会出现"文件变了统计没变"。
4. **恢复不需要扫数据块**：SST 侧直接来自 MANIFEST，内存侧只扫未刷盘的少量记录。只有老格式库或校验失败时才退化为全库重建。

代价是 MANIFEST 会变大（此前 100k 键测量约 1.2 MB），打开数据库时要解析它——此前的 100k 键开库记录为 81.7 ms。

------

#### 6.3问题三：进一步优化与扩展验证

写入路径的统计更新改为先用一次 `lower_bound` 找到插入位置，再以该位置作为 `std::map` 插入提示；对于按比较器递增的新键，直接在末尾提示插入，避免每条新记录重复查找树位置。

Ubuntu 22 容器中使用公开运行器的 `perf_write`，100k 条、单线程、256 字节 value、各运行 3 次：

| 状态   |      中位吞吐 |              三次范围 | 中位 p99 |
|---|---:|---:|---:|
| 优化前 | 295,040 ops/s | 244,662–347,086 ops/s |   6.5 µs |
| 优化后 | 351,544 ops/s | 340,759–356,968 ops/s |   5.0 µs |

该次配对测量中位吞吐提升约 19.1%。下表是本轮 Ubuntu 22 容器中的公开 `perf_reopen` 单次测量，开库计时不包括建库和 compaction；它与 5.4 节此前不同批次的开库数据不直接比较。用例会校验重开后的逐键统计和全部键值：

|  不同键数 |       MANIFEST 文件大小 | 重开耗时 |
|---:|---:|---:|
|   100,000 |   1,902,085 B(1.81 MiB) |  36.1 ms |
|   500,000 |   9,509,637 B(9.07 MiB) |  89.7 ms |
| 1,000,000 | 19,019,020 B(18.14 MiB) | 184.2 ms |

扩展压力用例覆盖 4 个写线程并发写入 40,000 个唯一键，同时运行统计查询、小写缓冲区触发的 flush、后台自动 compaction 和 6 次显式全范围 compaction；结束后核对内部记录、统计、回收和重开结果。另增加 5 次独立子进程测试：每次在 WAL `Sync` 成功后立即异常退出，随后重开数据库并核对已确认写入及统计。完整 Ubuntu 22 CTest 为 8/8 通过，ENOSPC 用例在专用 tmpfs 上实际执行。

---

### 7. 附：AI 编程助手使用说明

| 环节 | 参与程度 |
|---|---|
| 方案设计 | 讨论与评审，最终方案由本人确定 |
| 代码实现 | AI辅助代码实现 |
| 性能问题定位 | 协助编写探针、设计隔离实验、分析数据 |
