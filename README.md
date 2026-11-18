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
- 后台 Flush、阈值触发 Compaction 和后台 GC
- ReadView、过期视图和物理过期清理
- 热点 Series 每 Flush 窗口限流
- 工作线程绑定到 `cpu_budget` CPU 配额
- Covered Commit Version 写入 Manifest
- 50 次文件级故障注入
- 导入和混合基准会报告吞吐、峰值 RSS、恢复耗时

验证结果：

- 1000 万点导入 smoke：约 44.7k 点/秒，峰值 RSS 约 178 MiB，
  恢复约 4.5 秒，WAL 重放 0 字节。

尚未完成课程满负载验收：

- 完整的 1 亿点、60 分钟读写和 60 秒恢复基准
- 512 MiB RSS 在 1 亿点满负载下的最终确认

