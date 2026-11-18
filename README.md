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

当前实现包含：

- `Put`、`WriteBatch`
- Series 分片和 Shard MemTable
- WAL 校验、持久 fd 和 Group Commit
- SSTable、Manifest 和尾部 WAL 重放
- 流式 `Scan`
- 窗口聚合
- ReadView 和过期视图
- 后台 Flush 请求、基础 Compaction 和状态统计

以下内容仍是下一阶段：

- 磁盘外 SSTable 分页读取
- Page 编码和压缩
- 自动持续 Compaction 和过期 GC
- 完整的 1 亿点、60 分钟读写和恢复基准
