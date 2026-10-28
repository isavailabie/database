# database

基于课程 LevelDB Project1，在同一份源码中实现 TTL、统计元数据及协同回收。保留上游许可证、依赖源码和公开 benchmark。

## 实现

- TTL：`DB::Put(options, key, value, ttl)`，单位为秒，使用 `Options::env->NowMicros()` 保存绝对到期时间。普通 Put 永不过期；零 TTL 发布立即过期版本；时间溢出返回 InvalidArgument 且不写入、不消耗序列号。
- 读取：Get 和双向迭代过滤过期版本且不回退到同键旧值；快照固定创建时的序列号和时间；无快照迭代器固定自身创建视图。
- 回收：Compaction 保护活跃快照和被读取视图引用的数据源，保留仍用于遮蔽深层旧值的过期记录；CompactRange 支持最深层 SST 重写，回收不推进序列号。
- 统计：GetCurrentStats 原子返回当前 mem、imm 和有效 SST 中的内部记录数、去重用户键数和最新已发布序列号。删除标记和尚未回收的过期记录计数，WAL 副本不重复计数，查询忽略快照。
- 恢复优化：MANIFEST 持久化 SST 逐键统计检查点和随 flush/compaction 原子提交的增量；恢复时重放 MANIFEST 统计状态，再扫描 WAL 恢复出的内存记录。旧格式或统计状态语义校验失败时回退权威记录扫描。正常关闭时仍可保存可丢弃的 STATS 加速缓存，但它不再是快速恢复的唯一来源。

## 项目实施过程与每一步的思路

以下按实际实施顺序记录，而不是只列接口功能。两个实验始终修改同一份源码，没有维护两套分离实现。

### 第一步：建立原始代码的构建与测试基线（M1）

**目标：** 先区分环境问题和未实现功能，保留修改前的可复现结果。

- 在 macOS 上准备 C++17 编译器、Python 和项目本地 CMake，编译原始 LevelDB。
- 单独运行课程 `ttl_test`、`metadata_test`，并运行原生回归。TTL 接口未实现，统计返回零；这些是功能起点，不当作环境故障。
- 注意 `PersistAndRecover` 初始可以通过：重开前后都为零也能满足它的相等断言。因此“测试通过”不一定证明统计正确，后续必须独立算预期值。
- 原版评测脚本读取 `/proc/cpuinfo` 和 `/proc/meminfo`，不能直接在 macOS 完成评测。准备 Ubuntu 24.04 的 Colima/Docker 环境，将同一份源码共享到 Linux，在独立构建目录中编译。
- 容器和编辑器配置只用于本机开发，不作为课程源码依赖提交。

**阶段核验：** 原生回归通过，原版功能用例的失败点明确，Linux 完整 combined quick 能真正执行而非停在环境检测。

### 第二步：决定 TTL 编码并接入写入路径（M2）

**思路：** 到期时间必须只计算一次，并作为记录的一部分进入 WAL、MemTable 和 SST；不能在刷盘或重开时重新计算。

- 在 `db/dbformat.h` 增加 `kTypeTTLValue`。仅 TTL 类型的 value 为“8 字节 fixed64 绝对到期微秒 + 用户值”，普通 value 和删除标记保持原编码。
- 使用独立类型，而不是给所有值加前缀，避免普通二进制值被误判为 TTL，也保持普通记录编码兼容。
- 更新内部键解析和 `kValueTypeForSeek`，确保相同序列号的 TTL 版本不会因类型排序而被查找跳过。
- `DBImpl::Put(..., ttl)` 使用 `env_->NowMicros()` 计算截止时刻。先检查 `ttl > (UINT64_MAX - now) / 1000000`，再进入写入队列，同时覆盖乘法和加法溢出。
- `ttl == 0` 仍编码并提交一条立即过期记录，正常分配序列号；溢出则不进入 WAL、不写记录、不推进序列号。
- 在 `db/write_batch.cc` 和 `db/write_batch_internal.h` 增加带绝对到期时间的内部批次记录及 Handler 回调。存储回调保留期限，已有普通 Handler 默认收到不带前缀的用户值。

**持久化链路：** TTL Put → 编码后的 WriteBatch → WAL → MemTable → SST → Compaction 输出。恢复解析同一份绝对截止时间，不重新给记录续期。

### 第三步：让点查、快照和迭代器使用正确读取视图（M2）

**核心问题：** “最新版本过期”不等于“继续找旧版本”。过期版本必须像删除标记一样遮住旧值，否则会出现旧值复活。

- `db/memtable.cc` 和 `db/version_set.cc` 对第一条符合序列号的版本统一解码，达到或超过截止时间就返回 NotFound，终止向更旧数据源查找。
- 普通 Get 在锁内捕获一次序列号和读取时间，整个内存表及 SST 查询复用该时间。
- `db/snapshot.h` 保存快照创建时的序列号和时间；快照不会因真实时间流逝改变 TTL 可见性。
- `db/db_iter.cc` 固定迭代器创建视图。正向遍历遇到过期最新版本时跳过该键旧版本；反向遍历先遇到旧版本时，后续较新过期版本会清除旧候选值。
- 返回给用户的 value 去掉 TTL 前缀；内部迭代器仍保留物理记录，便于回收和独立验证。

**测试：** 新增 `test/ttl_read_view_test.cc`，覆盖零时刻、最大截止时间、溢出与序列号、二进制/空值、跨 SST 覆盖、懒初始化迭代器、Seek 和方向切换、日志复用、重开及并发。最终 14/14 在 Linux 和 macOS 上通过。

**阶段结果：** TTL quick 9/10，通过读取功能，尚未实现的物理回收项仍失败。原始真实时钟用例曾受到 Linux 虚拟机墙钟漂移影响：等待 20 秒时墙钟只前进约 19.89 秒。没有提前判定过期或修改课程用例，而是用可控时钟验证边界，并在 macOS 单独运行原始 TTL 用例通过。

### 第四步：安全回收过期记录，并处理最深层 SST（M3）

**思路：** 读取过滤和物理清理是两件事。回收不仅要看当前时间，还要看读取视图和深层旧版本。

- 合并开始时，在数据库锁内固定最早活跃快照序列号，以及当前时间和所有活跃快照时间的最小值。
- 当 TTL 截止时间不大于该时间界限、记录序列号不大于最早快照序列号，且输出层下面没有需要遮蔽的同键旧值时，才按删除屏障规则安全丢弃。
- 较旧版本继续遵循原生快照回收规则。对于深层仍有旧值的过期记录，先保留屏障，等相关版本合并后再删除。
- 活跃快照保护当前版本所需记录；迭代器及在途 Get 通过持有旧 Version/MemTable 引用保护自己的视图。当前统计不把旧 Version 的残留文件算作当前记录。
- 在 `db/version_set.h/.cc` 引入实际 `output_level()`，允许层 1 及以下同层重写，最深层不需要不存在的下一层。
- `CompactRange` 完成原有合并后，重写最深重叠层；同层任务一次流式处理选中输入，避免包含边界的游标反复选中刚生成的文件而无法结束。

**解决的边界问题：**

1. L0 点查按文件编号确定文件新旧。把旧 L0 输入原层重写为更大编号可能打乱版本关系，所以 L0 始终正常合并到 L1，不原层重写。
2. 合并解锁期间若墙钟回拨或创建了时间更早的快照，安装前重新检查时间界限，必要时放弃本次输出，保留原版本和读取视图。
3. 没有快照也不能马上删除被迭代器引用的旧 SST。测试既核对当前内部记录已经归零，也核对旧迭代器仍能读取，释放后旧文件可清理。

**阶段核验：** `test/ttl_gc_test.cc` 13/13，通过多文件最深层重写、部分范围、屏障、快照/迭代器、并发和异常退出恢复；TTL quick 10/10，原生回归通过。原始 ReadTTL 和 CompactionTTL 在 macOS 分别通过。

### 第五步：维护精确统计与批次原子发布（M4）

**思路：** 统计的是物理内部记录，而不是 Get 能看到的活跃键；同一键多个版本、删除标记和未回收 TTL 都要计数。

- 在 `db/db_impl.h/.cc` 维护全局有序映射“用户键 → 当前数据源中的内部记录引用数”，以及总记录数。去重依据用户比较器，支持比较器等价但字节不同的键。
- 一条成功写入增加一个引用，总记录数增加一；同键覆盖增加版本数但不增加去重键数；删除不存在的键也写入删除标记并计数。
- 在 WAL 和内存插入前完整验证 WriteBatch，收集批次键；成功插入后在同一把数据库锁内更新所有计数和已发布序列号，查询不会看到半个批次。
- MemTable 刷盘只是改变记录归属，从内存移到 SST，不增减全局引用，不重复计算 WAL 副本。
- `GetCurrentStats` 在锁内一次返回 `total_entries`、映射大小和 `LastSequence()`。它始终查询当前状态，忽略读取快照；空指针返回 InvalidArgument。

**开销取舍：** 查询为常数时间；增量维护需要有序映射查找，额外内存随去重键数增长。不在每次统计查询时扫描数据库。

### 第六步：使回收和恢复统计与当前版本一致（M5）

**思路：** 后台合并计算出要丢弃的记录，不代表这些记录已经从当前数据库消失；只有版本成功安装后才可扣计数。

- 合并状态暂存实际丢弃记录的用户键及数量，不在合并扫描阶段修改全局计数。
- `InstallCompactionResults` 成功完成 `LogAndApply` 后，在持锁状态下扣除贡献，引用数归零才删除该用户键。安装失败或因回拨取消输出均保持旧计数。
- 最初的恢复方案在 WAL 重放和最终版本安装完成后，扫描当前 MemTable、Immutable MemTable 和 SST 重建引用，完成后才发布数据库并启动后台任务。
- 序列号始终来自数据库已发布写入进度；TTL 到期、物理回收、刷盘和统计恢复本身不推进它。

**验证方法：** `test/metadata_consistency_test.cc` 按操作逐步计算预期值，再用内部迭代器独立核对。11/11 在 Linux、macOS 均通过，覆盖跨 mem/imm/SST 去重、自定义比较器、无效批次、日志复用、SST/MANIFEST 失败和并发三条记录批次。TTL 回收测试同步增加统计 oracle，验证读取与计数协同。

**阶段结果：** 原始 metadata_test 3/3，metadata quick 9/9，combined quick 22/22，原生回归通过。

### 第七步：补齐异常退出、并发及内存安全验证（M6）

**思路：** 正常关闭后重开不足以证明异常恢复正确，失败提交也不能仅用接口返回值互相比较。

- WAL 异常退出测试先确认 `sync=true` 的 Put/Delete 成功，再让独立子进程不执行 DB 析构而退出，父进程重开后核对读取、独立物理记录和三个统计字段。
- 新增 `test/compaction_recovery_test.cc`，分别在目标 SST Sync 成功但尚未提交 MANIFEST，以及 MANIFEST Sync 成功但尚未删除旧文件时终止子进程。
- 前者恢复旧输入版本，后者恢复已提交的新版本；两者完成后再次回收、重开都得到同一正确最终状态。
- 注入点直接针对准备好的最深层重写，避免把公开 CompactRange 前置日志轮换的 MANIFEST 提交误当成目标 GC 提交。
- 并发批次统计、TTL 并发回收、回拨快照保护分别重复 20 轮。使用公开运行器的 ASan/UBSan 模式检查内存安全和未定义行为，不用诊断构建做性能比较。

**阶段核验：** 新增两个提交边界测试在 Linux、macOS 均通过；压力重复通过，完整 sanitizer combined quick 与固定原生回归通过。

### 第八步：运行标准矩阵，并与固定原生基线对照（M7）

**思路：** 既测功能正确性，也报告真实开销，不用 quick 的数字代替正式性能数据。

- 依据 `benchmark/README.md` 获取指定 Google LevelDB 提交作为性能基线；不修改课程 benchmark，也不把固定原生回归版本误当成性能基线。
- 同一 Linux 容器中顺序运行 baseline 和 combined 的 `--profile standard --matrix`。每组由运行器默认重复三次，保留全部原始样本、范围和中位数。
- 数据库和输出放在原生 Linux 文件系统，避免共享 macOS 源码目录影响测量。使用新的输出目录，不覆盖已有结果。
- 覆盖一万/十万规模、适用场景的 1/8 线程和 0/50/100% 过期比例。4 个虚拟 CPU 下的 8 线程是超订阅，报告中保留该限制。
- 使用官方 `benchmark/compare.py` 对比原生 baseline；优化前后两个 combined 结果直接比较原始 aggregate，不能伪造 kind 去绕过官方工具的输入限制。

**实际结果：** baseline standard 36/36、combined standard 168/168，均通过固定原生回归。首次十万条记录重开中位耗时约 155.8 ms，原生基线约 3.54 ms，明确暴露启动扫描的性能问题，而不是把正确性通过当作性能优秀。

### 第九步：优化统计重建，避免普通重开重复扫描 SST

**思路：** 缓存只是加速层，WAL、MANIFEST 和当前物理记录仍是恢复的权威来源；缓存坏了不能让数据库无法打开或统计悄悄出错。

- 在 `db/stats_cache.cc` 实现可丢弃 STATS 文件。只有成功打开、无后台错误且 mem/imm 没有尚待刷盘记录的正常关闭，才保存缓存；不为了缓存额外强制刷盘。
- 缓存保存有序用户键及引用数、总数、CRC，以及比较器名称、已发布序列号、各层 SST 编号/大小/最小最大内部键的精确指纹。
- Open 完成权威恢复后校验缓存。成功时只读取键计数并使用有序插入提示重建映射，避开 SST 用户 value 扫描和逐条记录的有序查找。
- 缓存缺失、CRC 损坏、截断、零引用或非法计数、指纹失配时回退原始扫描。GC 不推进序列号，所以必须同时校验 SST 集合，不能只比较 seq。
- 有 WAL 内存记录时不直接采用旧缓存。加载前消费/删除缓存，配合精确指纹防止后续异常退出使用更早状态。
- 临时文件同步写入后重命名；失败只放弃辅助缓存，不改变 DB 权威状态。文件名解析识别 STATS/STATS.tmp，DestroyDB 能完全清理。

**发现并修复的问题：** sanitizer 的原生自定义比较器用例暴露析构时调用已失效比较器的错误。改为构造 DB 时复制比较器名称，关闭缓存路径不再调用它；保留失败日志并重新运行完整 sanitizer，而不是绕过用例。

**专项核验：** 新增 `test/stats_cache_test.cc` 12/12 在 Linux、macOS 均通过。除了检查统计，还观察缓存命中时实际没有打开 SST 数据文件；失效时则确实重新扫描。覆盖日志复用、时间到期、同 seq 合并后旧缓存、可选写入失败和 DestroyDB。

**优化后重测：** 完整 standard matrix 168/168、sanitizer combined 22/22、原生回归均通过。三次重开测量的中位数如下：

| 数据规模 | 优化前 | 优化后 | 提速 | 原生基线 |
| --- | ---: | ---: | ---: | ---: |
| 10,000 条 | 14.32 ms | 5.77 ms | 2.48 倍 | 3.39 ms |
| 100,000 条 | 155.78 ms | 31.90 ms | 4.88 倍 | 3.54 ms |

仍保留限制：键计数恢复有读取/分配成本，十万条仍约慢 9 倍；WAL、缓存失效仍走扫描；关闭写缓存的开销不包含在公开 perf_reopen 的打开计时中。性能结果不等同于老师统一机器或隐藏评测成绩。

### 第十步：整理可独立构建的提交内容

- 将本机容器、编辑器、助手状态、构建与测量产物排除，保留 LevelDB 源码、课程依赖、补充测试和通用构建说明。
- 从准确暂存内容导出不含本机配置的源码，按通用 Linux 命令全新编译，8/8 原生及扩展测试程序通过后再创建提交。
- 保留课程仓库历史和许可证，使用提供的作者署名；不强推、不改动课程测试源码、测试断言或评测 workload。为让固定旧版回归测试能与新增的内部 `kTypeTTLValue` 一起编译，公开评测构建仅对两份旧格式化测试源将 `-Wswitch` 从 error 降为 warning，不改测试内容或行为。
- 公开功能和本机验证已完成，但课程姓名/学号/分工、本人独立核验截图、正式实验报告、统一机器及不公开评测仍需要后续完成。本 README 记录实施过程，不替代这些要求。

## 依赖与构建

课程运行环境为 Linux 或 WSL2，需要 CMake 3.22+、支持 C++17 的编译器和 Python 3.9+。依赖源码已包含，无需初始化 Git 子模块。

```bash
cmake -S . -B build/release \
  -DCMAKE_BUILD_TYPE=Release \
  -DLEVELDB_BUILD_TESTS=ON \
  -DLEVELDB_BUILD_BENCHMARKS=OFF
cmake --build build/release -j 2
ctest --test-dir build/release --output-on-failure
```

课程两个原始测试程序未注册到 CTest，单独运行；TTL 两项使用不同目录，避免其共用 testdb 相互影响：

```bash
mkdir -p build/manual/{ttl-read,ttl-compaction,metadata}
(cd build/manual/ttl-read && ../../release/ttl_test --gtest_filter=TestTTL.ReadTTL)
(cd build/manual/ttl-compaction && ../../release/ttl_test --gtest_filter=TestTTL.CompactionTTL)
(cd build/manual/metadata && ../../release/metadata_test)
```

扩展测试已注册到 CTest，也可以单独执行：

```bash
./build/release/ttl_read_view_test
./build/release/ttl_gc_test
./build/release/metadata_consistency_test
./build/release/compaction_recovery_test
./build/release/stats_cache_test
```

测试包括可控时钟、零 TTL、溢出、覆盖与删除、正反向遍历、快照保护、最深层回收、独立物理计数、并发批次一致性、sync WAL 异常退出、SST/MANIFEST 提交边界崩溃、缓存命中和失效回退。

## 可重复回收验证

停止写入，释放快照及迭代器，再调用 `db->CompactRange(nullptr, nullptr)`。该方法先完成内存刷盘及目标合并，并重写最深重叠层；测试 Env 等待后台任务完成后，用 `TEST_NewInternalIterator()` 独立核对当前物理记录。`ttl_gc_test` 还检查旧 SST 被读取视图引用时保持可读、释放后可清理，以及多次回收和重开后序列号不变。

## benchmark

必须在 Linux 环境运行原版脚本，每次使用新的输出目录：

```bash
python3 benchmark/runner.py --kind combined --source "$PWD" \
  --output "$PWD/build/evaluation-quick-$(date +%Y%m%d-%H%M%S)" \
  --profile quick --jobs 2

python3 benchmark/runner.py --kind combined --source "$PWD" \
  --output "$PWD/build/evaluation-standard-$(date +%Y%m%d-%H%M%S)" \
  --profile standard --matrix --jobs 2
```

standard 默认每组重复三次。性能测量建议将输出放在原生 Linux 文件系统，避免 macOS 共享目录的 I/O 干扰。基线按 `benchmark/README.md` 指定的 Google LevelDB 提交 `7ee830d02b623e8ffe0b95d59a74db1e58da04c5` 单独运行 `--kind baseline`，使用 `benchmark/compare.py` 比较。公开评测附带的固定原生回归另有自己的版本，不与性能基线混淆。

## 已验证结果及限制

**本轮复验（2026-10-29，Apple M5 / macOS 26.6，AppleClang 21，CMake 4.4.5）：**

- 原生构建及 CTest：8/8 通过；MANIFEST rollover 定向故障测试：5/5 通过。
- 公开运行器 combined quick 的 ASan/UBSan：22/22 场景通过；固定原生回归、C 回归和 POSIX Env 回归通过（原生回归 209 通过、2 个压缩用例按预期跳过）。
- combined standard matrix：168/168 场景通过，三次重复及固定原生回归通过。100,000 键 `perf_reopen` 的 open 阶段中位数约 54.5 ms；这是本机诊断数据，不与历史 Intel/Linux 数字直接比较，也不作为课程机器的性能成绩。
- benchmark runner 的硬件信息收集现支持 Linux `/proc` 与 macOS `sysctl`。本轮在 macOS 原生环境执行；课程正式验收仍需在 Linux/WSL2 环境按要求复跑。没有可用性能基线源码，因此本轮未运行 baseline 对比。

以下 2026-10-19 的结果是在 Intel Mac 的 Ubuntu 24.04 容器中（4 CPU、6 GiB、GCC 13.3、CMake 3.28.3、Python 3.12.3）对较早实现版本取得的历史数据，不是本轮 MANIFEST 统计增量实现的重新验收：

- 原生及扩展 CTest：8/8 程序通过；缓存专项测试 12/12，在 Linux 与 macOS 均通过。
- 完整 ASan/UBSan combined quick：22/22，固定原生回归通过。
- 优化后完整 combined standard matrix：168/168，每组重复三次，固定原生回归通过。
- 十万条记录重开三次测量的中位耗时由约 155.8 ms 降至 31.9 ms，约提速 4.9 倍；原生性能基线约 3.54 ms，因此仍约慢 9 倍。一万条重开由约 14.3 ms 降至 5.77 ms。

旧的 STATS 缓存性能测量记录的是 MANIFEST 统计增量实现之前的方案，不能直接代表当前版本的恢复性能。当前仍有按去重键规模增长的统计映射内存；WAL 恢复只需扫描未刷盘的内存记录，但旧 MANIFEST、语义校验失败和缺失检查点会扫描 SST 回退。公开重开性能场景计时不包含关闭操作，不能把历史收益理解为当前完整打开/关闭总开销。以上历史数据不代表当前实现的验收，也不代表统一机器成绩或不公开评测结果。原始课程测试和公开 benchmark workload 未修改。

## 系统设计审视与后续改造

本节区分已实现能力与仍待验证的风险。LevelDB 的 WAL、MANIFEST 和 SST 是数据恢复的权威来源。数据库格式版本 2 在 MANIFEST 中持久化 SST 逐键统计状态；`STATS` 仍是可丢弃缓存，不是恢复正确性的前提。

### MANIFEST 统计检查点与增量

课程统计口径保持不变。实现采用“完整逐键检查点 + 有界增量”，直接复用 MANIFEST，而不另造并行日志：

- memtable flush 的逐键正增量、compaction 丢弃记录的逐键负增量，与对应 SST 添加/删除放在同一个 MANIFEST edit 中同步提交。
- 持久化映射只描述当前已提交 SST。WAL 中尚未刷盘的数据仍由 WAL 恢复，恢复后只扫描内存记录并合并到运行时统计。
- MANIFEST 达到 `max_file_size` 后，在下一次版本提交时先写入包含当前 SST 版本及完整统计映射的新 MANIFEST，再追加本次 edit、同步并切换 `CURRENT`。切换成功后旧 MANIFEST 随 obsolete-file 回收；未成功切换时旧 MANIFEST 仍是恢复入口。
- 旧 MANIFEST 没有统计检查点、计数增量 underflow/overflow 或统计版本校验失败时，放弃该辅助状态，扫描恢复后的 SST/MemTable 重建并写入新检查点。数据文件恢复不依赖统计元数据。
- 新格式版本 2 的增量会校验正负计数和总数边界；未知未来格式与版本回退明确拒绝。MANIFEST 日志记录本身损坏仍按元数据恢复错误处理，不能把不可解析的文件版本变更当作有效状态。

测试覆盖旧格式升级、无 STATS 缓存的 MANIFEST 恢复、WAL 记录合并、flush/compaction 计数变化、语义计数 underflow/overflow 扫描回退、rollover 后检查点恢复及旧 MANIFEST 回收。现另覆盖 rollover 的新 MANIFEST 创建失败、本次 edit 的 header/payload append 失败、Sync 失败和 CURRENT rename 失败；每种情况都检查旧恢复入口仍有效，并在移除注入后重开验证 WAL 数据与统计。真实磁盘空间耗尽、断电/进程崩溃切点与目录元数据持久性仍待验证；超大键映射的快照空间、内存及写放大也未测量。

### 建议实施顺序

1. 建立并记录现有 Linux 基线，分别运行课程 TTL、统计测试、combined quick、原生回归和已有故障恢复用例；当前 macOS runner 也可执行，但不能替代课程指定的 Linux/WSL2 环境验收。
2. 固化统计和 TTL 不变量，补齐 WAL、SST、MANIFEST、统计辅助文件之间的提交/回退故障测试。
3. 已实现 MANIFEST 统计增量、完整快照与阈值 rollover；I/O 返回错误的 rollover 故障注入和 10 万键标准矩阵已覆盖，真实磁盘空间耗尽、rollover 进程崩溃切点仍待验证。
4. TTL 使用绝对墙上时钟截止时间；正常前进/回拨、快照固定读取时间，以及 Compaction 解锁期间回拨的安装保护已有可控时钟测试。真实系统调时和休眠/恢复后的行为仍需在目标操作系统确认。
5. 已为 MANIFEST 增加格式版本标记：缺少标记的旧格式仍可读取、扫描统计并升级；未来版本及版本回退会明确拒绝。旧二进制回滚到新格式数据库不支持。
6. 已验证部分文件/元数据 I/O 错误和旧 SST 被活跃迭代器/快照引用时的行为；继续验证真实 ENOSPC、断电后的目录元数据持久性和后台 Compaction 持续失败时的反馈及写入影响。
7. 已有 10 万键课程矩阵及性能场景；仍需专门量化统计映射峰值内存、MANIFEST 快照字节/增长曲线、关闭写放大，以及更大键基数下的恢复耗时。

上述边界审视是待验证的风险清单，不代表已确认每项都存在缺陷。每个实现阶段都应更新其测试结果和剩余限制，避免把设计目标写成已经完成的功能。

本轮初始审计确认一处发布状态错误：WriteBatch 格式校验通过、但 WAL 追加失败时，原实现仍会推进最新序列号，造成 `seq` 指向未发布的写入。现已改为仅在 WAL 写入和 MemTable 插入均成功后发布序列号；WAL 追加或同步失败后也会拒绝后续写入，直到重开恢复日志状态。新增了 WAL 负载追加中途失败的注入测试，检查失败批次不改变统计/序列号、数据库在重开恢复前拒绝新写，以及恢复后仍可继续写。另增加 MANIFEST 格式回退/未来版本拒绝、旧格式升级、统计增量恢复/回退、rollover 故障切点和旧 MANIFEST 回收测试。2026-10-29 在 macOS 上使用 CMake 4.4.5、AppleClang 21 和 SDK 26.5 完成全量构建；CTest 8/8、rollover 定向注入 5/5、ASan/UBSan combined quick 22/22 和 combined standard matrix 168/168 均通过；固定原生回归 209 项通过、2 个压缩用例按预期跳过，C 回归和 POSIX Env 回归通过。macOS 结果不能替代课程指定的 Linux/WSL2 正式验收或基线比较；本轮未运行 baseline 对比。SDK 27.0 不能由当前 linker 解析，使用系统已有的 SDK 26.5 完成验证，没有替换系统开发工具。真实磁盘空间耗尽、rollover 进程崩溃切点、断电/目录元数据持久性，以及超出 10 万键规模的统计映射内存与写放大仍待验证。

## 作者与核验

Git 提交作者：qqhppnw。课程姓名、学号、正式分工由本人补充，不从账号推断。

Coding agent 参与源码实现、测试补充、回归执行和本机性能测量。本人仍需复核设计和代码、独立运行验收、整理真实截图及实验报告，并按要求披露参与范围。

本机容器、编辑器和助手配置以及构建产物不提交；可用上述通用命令在 Linux/WSL2 构建。
