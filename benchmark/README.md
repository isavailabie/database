# 课程公开 benchmark

本目录提供 TTL 和统计元数据实验的公开评测。实验要求以课程平台发布的文档为准。

| 目录 | 用途 |
| --- | --- |
| `benchmark/` | 课程评测使用本目录检查实验正确性、执行原生回归并测量性能。 |
| `benchmarks/` | LevelDB 原有的性能测试源码位于此目录，包括 `db_bench` 等程序。 |
| `third_party/benchmark/` | LevelDB 原有性能测试使用的 Google Benchmark 依赖库位于此目录。 |

课程实验使用 `benchmark/runner.py`。构建选项 `LEVELDB_BUILD_BENCHMARKS=OFF` 关闭 LevelDB 原有的性能测试程序，不影响课程运行器。课程运行器会自行配置和构建测试，无需先编译 `db_bench`。

## 运行公开测试

使用 Linux 或 Windows 的 WSL2 环境，安装 CMake 3.22+、支持 C++17 的编译器和 Python 3.9+。仓库已包含所需依赖源码，无需初始化 Git 子模块。

在 `master` 分支的仓库根目录分别执行：

```bash
python3 benchmark/runner.py --kind ttl --source "$PWD" \
  --output "$PWD/build/evaluation-ttl-quick" --profile quick --jobs 2
python3 benchmark/runner.py --kind metadata --source "$PWD" \
  --output "$PWD/build/evaluation-metadata-quick" --profile quick --jobs 2
```

初始 base 可以构建，但尚未实现两个实验的功能，相关功能测试会失败。学生完成实现后，应通过对应实验的测试。

| 档位 | 规模参数 N | 写线程数 | 重复次数 |
| --- | ---: | ---: | ---: |
| quick | 10,000 | 4 | 1 |
| standard | 100,000 | 8 | 3 |

`quick` 适合开发自测，`standard --matrix` 用于规模和性能测量。N 按场景表示键数或操作数。矩阵测试对性能场景比较 N/10 和 N 两种规模，并对适用场景比较单线程与多线程、不同 TTL 过期比例。

运行器会复制源码后构建，每次必须使用新的输出目录。公开测试检查实验接口和结果，测量普通读写、扫描、打开数据库及实验功能的开销，并默认执行固定版本的原生回归测试。

`--cases` 可以选择场景进行局部调试；`--keys`、`--threads`、`--repeat` 和 `--seed` 可以调整参数。`python3 benchmark/runner.py --help` 列出全部参数。

## 查看结果

| 输出文件 | 用途 |
| --- | --- |
| `summary.json` | 文件记录整体状态及各场景的测试结果。 |
| `configure.log`、`build.log` | 日志记录配置和编译过程，帮助定位构建失败。 |
| 场景目录中的 `run.log` | 日志记录具体场景的执行情况，帮助定位功能错误或超时。 |
| `metrics.csv`、`aggregate.json` | 文件记录已产生的性能指标和重复运行的汇总结果。 |

## 与原生 LevelDB 比较

课程使用 Google LevelDB 的固定提交 `7ee830d02b623e8ffe0b95d59a74db1e58da04c5` 作为普通读写的对照基线。基线引擎源码与课程原有对照版本一致。

在仓库根目录下载基线源码；此步骤只需执行一次。运行器使用课程仓库中已包含的依赖，无需初始化基线仓库的子模块：

```bash
git clone https://github.com/google/leveldb.git ../leveldb-baseline
git -C ../leveldb-baseline checkout --detach 7ee830d02b623e8ffe0b95d59a74db1e58da04c5
```

随后在同一机器上依次运行基线和两个实验，避免同时运行影响性能测量：

```bash
python3 benchmark/runner.py --kind baseline --source ../leveldb-baseline \
  --output "$PWD/build/evaluation-baseline" --profile standard --matrix --jobs 2

python3 benchmark/runner.py --kind ttl --source "$PWD" \
  --output "$PWD/build/evaluation-ttl-standard" --profile standard --matrix --jobs 2

python3 benchmark/runner.py --kind metadata --source "$PWD" \
  --output "$PWD/build/evaluation-metadata-standard" --profile standard --matrix --jobs 2
```

每组评测全部通过后，分别比较两个实验与基线的共同性能场景：

```bash
python3 benchmark/compare.py build/evaluation-baseline build/evaluation-ttl-standard \
  --output build/comparison-ttl.json
python3 benchmark/compare.py build/evaluation-baseline build/evaluation-metadata-standard \
  --output build/comparison-metadata.json
```

如果当前只完成一个实验，只需运行该实验及其基线对照。比较工具要求两侧评测通过、环境和测量参数匹配，并且输出文件尚不存在。工具给出吞吐比值，不自动给出成绩。

下载源码 ZIP 的用户可以从 Google LevelDB 仓库按上述固定提交下载基线源码，将其解压到仓库之外，再将 `--source` 指向该目录。运行器本身不依赖 Git 历史。

报告应给出三轮测量的中位数和波动。吞吐包含驱动中的数据生成和校验，峰值 RSS 包含测试模型和缓存。

`regression-source/` 仅保存固定版本的原生回归测试文件；`package.json` 提供其校验信息和依赖版本。
