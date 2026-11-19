# 时序数据库课程项目

## 构建

```bash
cmake -S . -B build \
  -DCMAKE_OSX_SYSROOT=/Library/Developer/CommandLineTools/SDKs/MacOSX26.5.sdk
cmake --build build --parallel 4
ctest --test-dir build --output-on-failure
```

## 基准

批量导入：

```bash
./build/tsdb_bench 100000
./build/tsdb_bench 1000000
```

混合读写：

```bash
./build/tsdb_mixed_bench 60
```

混合基准固定使用 4 个写线程和 16 个查询线程，并在结束时报告写入点数、范围扫描次数、扫描错误、提交版本、WAL 大小及维护计数。

导入基准还会关闭并重新打开数据库，报告恢复耗时、重放 WAL 字节和目标 Series 的恢复点数。

故障注入：

```bash
./build/tsdb_fault_test
ctest --test-dir build --output-on-failure
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
- 后台 Flush、阈值触发 Compaction 和后台 GC
- 引用计数 GC：读取并发下也能回收过期 SSTable 文件
- ReadView、过期视图和物理过期清理
- 热点 Series 每 Flush 窗口限流
- 工作线程绑定到 `cpu_budget` CPU 配额
- Covered Commit Version 写入 Manifest
- 50 次文件级故障注入
- 导入和混合基准会报告吞吐、峰值 RSS、恢复耗时

验证结果：

- 1000 万点导入 smoke：约 109k 点/秒，峰值 RSS 约 32 MiB，
  恢复约 1.9 秒。
- 1 亿点完整导入：约 103.6k 点/秒（16.1 分钟），峰值 RSS 约 42 MiB，
  恢复约 19.9 秒，WAL 重放 0 字节。
- 60 分钟混合读写：约 1481 万点写入、34.6 万次扫描、0 扫描错误、
  0 待回收 SSTable、峰值 RSS 约 113 MiB、20 次合并、148 次刷盘。

