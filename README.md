# database

基于课程 LevelDB Project1，在同一份源码中实现 TTL、统计元数据及协同回收。保留上游许可证、依赖源码和公开 benchmark。

## 实现

- TTL：`DB::Put(options, key, value, ttl)`，单位为秒，使用 `Options::env->NowMicros()` 保存绝对到期时间。普通 Put 永不过期；零 TTL 发布立即过期版本；时间溢出返回 InvalidArgument 且不写入、不消耗序列号。
- 读取：Get 和双向迭代过滤过期版本且不回退到同键旧值；快照固定创建时的序列号和时间；无快照迭代器固定自身创建视图。
- 回收：Compaction 保护活跃快照和被读取视图引用的数据源，保留仍用于遮蔽深层旧值的过期记录；CompactRange 支持最深层 SST 重写，回收不推进序列号。
- 统计：GetCurrentStats 原子返回当前 mem、imm 和有效 SST 中的内部记录数、去重用户键数和最新已发布序列号。删除标记和尚未回收的过期记录计数，WAL 副本不重复计数，查询忽略快照。
- 恢复优化：正常关闭且内存记录已全部刷盘时保存可丢弃 STATS 缓存；校验 CRC、比较器名称、序列号及精确 SST 集合后按序恢复键引用计数。缓存缺失、损坏、失配或有 WAL 内存记录时回退到权威记录扫描。无需缓存也能正常恢复数据库。

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

2026-10-19，在 Intel Mac 的 Ubuntu 24.04 容器中（4 CPU、6 GiB、GCC 13.3、CMake 3.28.3、Python 3.12.3）验证：

- 原生及扩展 CTest：8/8 程序通过；缓存专项测试 12/12，在 Linux 与 macOS 均通过。
- 完整 ASan/UBSan combined quick：22/22，固定原生回归通过。
- 优化后完整 combined standard matrix：168/168，每组重复三次，固定原生回归通过。
- 十万条记录重开三次测量的中位耗时由约 155.8 ms 降至 31.9 ms，约提速 4.9 倍；原生性能基线约 3.54 ms，因此仍约慢 9 倍。一万条重开由约 14.3 ms 降至 5.77 ms。

缓存按去重键规模保存并重建有序键计数，仍有启动、额外内存与关闭写缓存开销；WAL 恢复和失效缓存仍须扫描。公开重开性能场景计时不包含关闭操作，不能把收益理解为所有打开/关闭总开销都下降。以上是本机测试，不代表统一机器成绩或不公开评测结果。原始课程测试和公开 benchmark 未修改。

## 作者与核验

Git 提交作者：qqhppnw。课程姓名、学号、正式分工由本人补充，不从账号推断。

Coding agent 参与源码实现、测试补充、回归执行和本机性能测量。本人仍需复核设计和代码、独立运行验收、整理真实截图及实验报告，并按要求披露参与范围。

本机容器、编辑器和助手配置以及构建产物不提交；可用上述通用命令在 Linux/WSL2 构建。
