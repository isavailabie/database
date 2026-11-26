#include "tsdb.h"
#include "../benchmarks/bench_util.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <limits>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

#include "check.h"

#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>

using namespace tsdb;

namespace {

static_assert(std::is_same_v<decltype(AggregateResult::count), uint64_t>);
static_assert(std::is_same_v<decltype(AggregateResult::result), double>);

// 子进程兜底限制死锁回归的执行时间，不让整个测试集永久挂起。
void RunWithTimeout(const std::function<void()>& run) {
    const pid_t pid = fork();
    CHECK(pid >= 0);
    if (pid == 0) {
        try {
            run();
            _exit(0);
        } catch (const std::exception& error) {
            std::cerr << '\n' << error.what() << std::endl;
            _exit(1);
        }
    }
    int child_status = 0;
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::seconds(15);
    while (std::chrono::steady_clock::now() < deadline) {
        const auto result = waitpid(pid, &child_status, WNOHANG);
        if (result == pid) {
            CHECK(WIFEXITED(child_status) && WEXITSTATUS(child_status) == 0);
            return;
        }
        if (result < 0) {
            ::kill(pid, SIGKILL);
            waitpid(pid, &child_status, 0);
            CHECK(result >= 0);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ::kill(pid, SIGKILL);
    waitpid(pid, &child_status, 0);
    throw test_support::CheckFailure("并发测试超过有界超时");
}

std::filesystem::path TestPath() {
    auto pattern = (std::filesystem::temp_directory_path() / "tsdb-course-test-XXXXXX").string();
    std::vector<char> buffer(pattern.begin(), pattern.end());
    buffer.push_back('\0');
    const auto path = ::mkdtemp(buffer.data());
    if (!path) throw std::runtime_error("cannot create isolated test directory");
    return path;
}

void AssertOk(Status status) {
    if (!status.ok) {
        std::cerr << status.message << '\n';
        std::abort();
    }
}

std::vector<DataPoint> Read(Cursor* cursor) {
    std::vector<DataPoint> result;
    while (auto point = cursor->NextPoint()) result.push_back(*point);
    return result;
}

// WAL segments are numbered; the newest one is the active log.
std::filesystem::path NewestWalSegment(const std::filesystem::path& path) {
    std::filesystem::path newest;
    uint64_t best = 0;
    std::error_code ec;
    for (const auto& entry :
         std::filesystem::directory_iterator(path / "wal", ec)) {
        if (!entry.is_regular_file()) continue;
        if (entry.path().extension() != ".log") continue;
        uint64_t number = 0;
        try {
            number = std::stoull(entry.path().stem().string());
        } catch (...) {
            continue;
        }
        if (newest.empty() || number > best) {
            best = number;
            newest = entry.path();
        }
    }
    return newest;
}

void TestWriteScanAndAtomicValidation(const std::filesystem::path& path) {
    Status status;
    auto engine = Engine::Open(path, Options{4, 1000}, &status);
    AssertOk(status);
    CommitVersion version = 0;
    AssertOk(engine->WriteBatch({{1, 1000, 1.0}, {2, 500, 2.0}}, &version));
    CHECK(version == 1);
    auto bad = engine->WriteBatch({{1, 2000, 3.0}, {1, 1500, 4.0}});
    CHECK(!bad.ok);
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({1, 2}, 0, 2000, &cursor));
    const auto points = Read(cursor.get());
    CHECK(points.size() == 2);
    CHECK(points[0].series_id == 2 && points[1].series_id == 1);
    AssertOk(engine->Close());
}

// 课程要求 F1：「不满足时间戳顺序或过期边界要求的写入返回错误；批次校验失败时
// 整批不生效」。集中覆盖：批内 / 跨批 / 重复时间戳；过期边界（含恰好等于
// cutoff）；失败批次中合法序列的点同样不可见、不消耗版本号、不更新递增跟踪、
// 不进入 WAL；重启后（数据分别位于 SST 与仅位于 WAL）校验状态仍成立；
// 多写者并发写同一序列时，按提交版本排序的时间戳仍严格递增。
void TestWriteValidationContract(const std::filesystem::path& path) {
    const Options options{.shard_count = 4, .memtable_max_points = 1000000,
                          .enable_background_compaction = false};
    auto visible = [](Engine* engine) {
        std::unique_ptr<Cursor> cursor;
        AssertOk(engine->Scan({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, 0, 100000,
                              &cursor));
        return Read(cursor.get());
    };
    const auto db = path / "db";
    {
        Status status;
        auto engine = Engine::Open(db, options, &status);
        AssertOk(status);
        CommitVersion version = 0;
        AssertOk(engine->WriteBatch({{1, 100, 1.0}, {2, 100, 1.0}}, &version));
        CHECK(version == 1);

        // 跨批：更小、相等（重复的 (series_id, timestamp)）都拒绝。
        CHECK(!engine->Put({1, 50, 0.0}).ok);
        CHECK(!engine->Put({1, 100, 0.0}).ok);
        // 批内：同一序列不严格递增（相等或回退）拒绝。
        CHECK(!engine->WriteBatch({{1, 200, 0.0}, {1, 200, 0.0}}).ok);
        CHECK(!engine->WriteBatch({{1, 300, 0.0}, {1, 250, 0.0}}).ok);
        // 混合批次：前三个点合法，最后一个违规，整批都不可见。
        CHECK(!engine->WriteBatch(
                   {{3, 10, 0.0}, {4, 10, 0.0}, {2, 500, 0.0}, {1, 90, 0.0}})
                   .ok);
        CHECK(visible(engine.get()).size() == 2);
        // 失败批次既不消耗版本号，也不推进递增跟踪：2 仍可写 300（< 500）。
        AssertOk(engine->Put({2, 300, 1.0}, &version));
        CHECK(version == 2);
        AssertOk(engine->Put({3, 10, 1.0}, &version));
        CHECK(version == 3);
        // 递增只针对同一序列：批内不同序列之间的时间戳无顺序要求。
        AssertOk(engine->WriteBatch({{5, 1000, 1.0}, {6, 10, 1.0}}, &version));
        CHECK(version == 4);

        // 过期边界：小于 cutoff 拒绝，恰好等于 cutoff 允许。
        AssertOk(engine->ExpireBefore(150, &version));
        CHECK(version == 5);
        CHECK(!engine->Put({7, 149, 0.0}).ok);
        AssertOk(engine->Put({7, 150, 1.0}, &version));
        CHECK(version == 6);
        // 批内一个点早于 cutoff，整批拒绝，合法序列 8 也不生效。
        CHECK(!engine->WriteBatch({{8, 200, 0.0}, {9, 100, 0.0}}).ok);
        AssertOk(engine->Put({8, 160, 1.0}, &version));
        CHECK(version == 7);
        // 过期边界只能前进；相同边界幂等。
        CHECK(!engine->ExpireBefore(100).ok);
        AssertOk(engine->ExpireBefore(150));
        AssertOk(engine->Flush());  // 以上数据进入 SST
        AssertOk(engine->Close());
    }

    // 子进程只写 WAL 后异常退出：重启时这两条只能从 WAL 重放得到。
    const pid_t pid = fork();
    CHECK(pid >= 0);
    if (pid == 0) {
        Status status;
        auto engine = Engine::Open(db, options, &status);
        if (!status.ok) _exit(2);
        if (!engine->Put({2, 400, 1.0}).ok) _exit(3);
        if (!engine->Put({11, 1000, 1.0}).ok) _exit(4);
        if (engine->Put({11, 999, 0.0}).ok) _exit(5);
        _exit(0);
    }
    int child_status = 0;
    waitpid(pid, &child_status, 0);
    CHECK(WIFEXITED(child_status) && WEXITSTATUS(child_status) == 0);

    {
        Status status;
        auto engine = Engine::Open(db, options, &status);
        AssertOk(status);
        CHECK(!engine->Put({2, 400, 0.0}).ok);    // 来自 WAL 重放
        CHECK(!engine->Put({2, 350, 0.0}).ok);
        CHECK(!engine->Put({11, 1000, 0.0}).ok);  // 来自 WAL 重放
        CHECK(!engine->Put({8, 160, 0.0}).ok);    // 来自 SST 索引
        CHECK(!engine->Put({5, 1000, 0.0}).ok);   // 来自 SST 索引
        CHECK(!engine->Put({7, 149, 0.0}).ok);    // cutoff 持久化
        CHECK(!engine->WriteBatch({{2, 500, 0.0}, {11, 999, 0.0}}).ok);
        AssertOk(engine->Put({2, 401, 1.0}));
        const auto points = visible(engine.get());
        const std::vector<std::pair<int64_t, uint64_t>> expected{
            {150, 7}, {160, 8}, {300, 2}, {400, 2}, {401, 2}, {1000, 5}, {1000, 11}};
        CHECK(points.size() == expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
            CHECK(points[i].timestamp_ms == expected[i].first &&
                  points[i].series_id == expected[i].second);
        }
        AssertOk(engine->Close());
    }

    // 4 个写者交错写同一序列：校验同时看已提交与排队中的批次，因此被接受的
    // 写入按提交版本排序后时间戳严格递增，且全部可见、没有重复。
    {
        Status status;
        auto engine = Engine::Open(path / "concurrent", options, &status);
        AssertOk(status);
        std::mutex mutex;
        std::vector<std::pair<CommitVersion, int64_t>> accepted;
        std::atomic<size_t> rejected{0};
        std::vector<std::thread> writers;
        for (int64_t writer = 0; writer < 4; ++writer) {
            writers.emplace_back([&, writer] {
                for (int64_t i = 0; i < 200; ++i) {
                    const int64_t timestamp = i * 4 + writer;
                    CommitVersion version = 0;
                    if (engine->Put({42, timestamp, 1.0}, &version).ok) {
                        std::lock_guard lock(mutex);
                        accepted.emplace_back(version, timestamp);
                    } else {
                        ++rejected;
                    }
                }
            });
        }
        for (auto& writer : writers) writer.join();
        CHECK(!accepted.empty());
        CHECK(accepted.size() + rejected.load() == 800);
        std::sort(accepted.begin(), accepted.end());
        for (size_t i = 1; i < accepted.size(); ++i) {
            CHECK(accepted[i].first > accepted[i - 1].first);
            CHECK(accepted[i].second > accepted[i - 1].second);
        }
        std::unique_ptr<Cursor> cursor;
        AssertOk(engine->Scan({42}, 0, 100000, &cursor));
        const auto points = Read(cursor.get());
        CHECK(points.size() == accepted.size());
        for (size_t i = 0; i < points.size(); ++i) {
            CHECK(points[i].timestamp_ms == accepted[i].second);
        }
        AssertOk(engine->Close());
    }
}

void TestReadViewAndExpiration(const std::filesystem::path& path) {
    Status status;
    auto engine = Engine::Open(path, Options{4, 1000}, &status);
    AssertOk(status);
    AssertOk(engine->Put({1, 1000, 1.0}));
    std::unique_ptr<Cursor> old_view;
    AssertOk(engine->Scan({1}, 0, 2000, &old_view));
    AssertOk(engine->Put({1, 1500, 2.0}));
    AssertOk(engine->ExpireBefore(1200));
    auto old_points = Read(old_view.get());
    CHECK(old_points.size() == 1);
    std::unique_ptr<Cursor> current;
    AssertOk(engine->Scan({1}, 0, 2000, &current));
    const auto current_points = Read(current.get());
    CHECK(current_points.size() == 1 && current_points[0].timestamp_ms == 1500);
    std::unique_ptr<Cursor> empty;
    AssertOk(engine->Scan({1}, 1500, 1500, &empty));
    CHECK(Read(empty.get()).empty());
    std::unique_ptr<Cursor> reversed;
    auto reversed_status = engine->Scan({1}, 1501, 1500, &reversed);
    CHECK(!reversed_status.ok);
    AssertOk(engine->Compact());
    std::unique_ptr<Cursor> compacted;
    AssertOk(engine->Scan({1}, 0, 2000, &compacted));
    const auto compacted_points = Read(compacted.get());
    CHECK(compacted_points.size() == 1 && compacted_points[0].timestamp_ms == 1500);
    CHECK(old_points.size() == 1);
    AssertOk(engine->GarbageCollect());
    AssertOk(engine->Close());
}

void TestRecovery(const std::filesystem::path& path) {
    {
        Status status;
        auto engine = Engine::Open(path, Options{4, 2}, &status);
        AssertOk(status);
        AssertOk(engine->WriteBatch({{10, 100, 4.0}, {11, 200, 5.0}}));
        AssertOk(engine->Put({10, 300, 6.0}));
        AssertOk(engine->Close());
    }

    Status status;
    auto engine = Engine::Open(path, Options{4, 1000}, &status);
    AssertOk(status);
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({10, 11}, 0, 1000, &cursor));
    auto recovered = Read(cursor.get());
    CHECK(recovered.size() == 3);
    std::unique_ptr<Cursor> aggregate;
    AssertOk(engine->Aggregate({10}, 0, 1000, 200, AggregateOp::Count, &aggregate));
    auto result = aggregate->NextAggregate();
    CHECK(result && result->count == 1);
    AssertOk(engine->Close());
}

void TestConcurrentGroupCommit(const std::filesystem::path& path) {
    Status status;
    auto engine = Engine::Open(path, Options{4, 100000}, &status);
    AssertOk(status);
    std::vector<std::thread> writers;
    std::vector<CommitVersion> versions(4);
    for (uint64_t thread_id = 0; thread_id < 4; ++thread_id) {
        writers.emplace_back([engine = engine.get(), thread_id, &versions] {
            for (int i = 0; i < 40; ++i) {
                Status write_status = engine->WriteBatch(
                    {{100 + thread_id, static_cast<int64_t>(thread_id * 1000 + i),
                      static_cast<double>(i)}},
                    &versions[thread_id]);
                CHECK(write_status.ok);
            }
        });
    }
    for (auto& writer : writers) writer.join();
    CHECK(versions[0] > 0 && versions[1] > 0 &&
           versions[2] > 0 && versions[3] > 0);
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({100, 101, 102, 103}, 0, 5000, &cursor));
    CHECK(Read(cursor.get()).size() == 160);
    AssertOk(engine->Close());
}

void TestQueueBackpressure(const std::filesystem::path& path) {
    Status status;
    auto engine = Engine::Open(
        path, Options{.shard_count = 4, .memtable_max_points = 100000,
                      .max_pending_batches = 1},
        &status);
    AssertOk(status);
    std::mutex start_mutex;
    std::condition_variable start_cv;
    bool start = false;
    std::vector<Status> outcomes(4);
    std::vector<CommitVersion> versions(4);
    std::vector<std::thread> writers;
    for (uint64_t writer_id = 0; writer_id < 4; ++writer_id) {
        writers.emplace_back([&, writer_id] {
            {
                std::unique_lock lock(start_mutex);
                start_cv.wait(lock, [&] { return start; });
            }
            for (int i = 0; i < 64; ++i) {
                outcomes[writer_id] = engine->WriteBatch(
                    {{300 + writer_id, i, static_cast<double>(i)}},
                    &versions[writer_id]);
                if (!outcomes[writer_id].ok) break;
            }
        });
    }
    {
        std::lock_guard lock(start_mutex);
        start = true;
    }
    start_cv.notify_all();
    for (auto& writer : writers) writer.join();
    for (size_t i = 0; i < outcomes.size(); ++i) {
        CHECK(outcomes[i].ok);
        CHECK(versions[i] > 0);
    }
    EngineStats stats;
    AssertOk(engine->GetStats(&stats));
    CHECK(stats.pending_batches == 0);
    CHECK(stats.visible_version == 4 * 64);
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({300, 301, 302, 303}, 0, 64, &cursor));
    CHECK(Read(cursor.get()).size() == 4 * 64);
    CHECK(!engine->Put({300, 62, 0.0}).ok);
    AssertOk(engine->Close());
}

void TestZeroQueueCapacityRejected(const std::filesystem::path& path) {
    Status status;
    auto engine = Engine::Open(
        path, Options{.max_pending_batches = 0}, &status);
    CHECK(!status.ok);
    CHECK(engine == nullptr);
}

void TestMemtableBackpressureAutoFlush(const std::filesystem::path& path) {
    const Options options{.shard_count = 4,
                          .memtable_max_points = 1000000,
                          .enable_background_compaction = false,
                          .max_memtable_bytes = 2 * 32};
    Status status;
    auto engine = Engine::Open(path, options, &status);
    AssertOk(status);
    AssertOk(engine->Put({310, 100, 1.0}));
    // 下一批自身能容纳，但必须先刷掉现存的一个点才能提交完整批次。
    AssertOk(engine->WriteBatch({{310, 200, 2.0}, {311, 200, 3.0}}));
    EngineStats stats;
    AssertOk(engine->GetStats(&stats));
    CHECK(stats.flush_count >= 1);
    CHECK(stats.mutable_points <= 2);
    CHECK(stats.memtable_bytes == stats.mutable_points * 32);
    CHECK(stats.memtable_bytes <= options.max_memtable_bytes);
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({310, 311}, 0, 300, &cursor));
    CHECK(Read(cursor.get()).size() == 3);
    cursor->Close();
    AssertOk(engine->Close());
    engine.reset();
    auto reopened = Engine::Open(path, options, &status);
    AssertOk(status);
    AssertOk(reopened->Scan({310, 311}, 0, 300, &cursor));
    CHECK(Read(cursor.get()).size() == 3);
    AssertOk(reopened->Close());
}

void TestHotSeriesBackpressureAutoFlush(const std::filesystem::path& path) {
    Status status;
    auto engine = Engine::Open(
        path, Options{.shard_count = 4, .memtable_max_points = 1000000,
                      .enable_background_compaction = false,
                      .hot_series_point_limit = 2}, &status);
    AssertOk(status);
    for (int batch = 0; batch < 3; ++batch) {
        AssertOk(engine->WriteBatch({{320, batch * 2, 1.0},
                                    {320, batch * 2 + 1, 2.0}}));
    }
    EngineStats stats;
    AssertOk(engine->GetStats(&stats));
    CHECK(stats.flush_count >= 2);
    CHECK(stats.mutable_points <= 2);
    CHECK(stats.memtable_bytes == stats.mutable_points * 32);
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({320}, 0, 6, &cursor));
    const auto points = Read(cursor.get());
    CHECK(points.size() == 6);
    for (size_t i = 0; i < points.size(); ++i) {
        CHECK(points[i].timestamp_ms == static_cast<int64_t>(i));
    }
    AssertOk(engine->Close());
}

void TestOversizedHotSeriesBatchAtomic(const std::filesystem::path& path) {
    Status status;
    auto engine = Engine::Open(
        path, Options{.memtable_max_points = 1000000,
                      .enable_background_compaction = false,
                      .hot_series_point_limit = 2}, &status);
    AssertOk(status);
    CommitVersion version = 99;
    const auto started = std::chrono::steady_clock::now();
    const auto rejected = engine->WriteBatch(
        {{331, 100, 1.0}, {330, 100, 1.0}, {330, 200, 2.0},
         {330, 300, 3.0}}, &version);
    CHECK(!rejected.ok);
    CHECK(std::chrono::steady_clock::now() - started < std::chrono::seconds(2));
    CHECK(version == 99);
    EngineStats stats;
    AssertOk(engine->GetStats(&stats));
    CHECK(stats.visible_version == 0);
    CHECK(stats.mutable_points == 0 && stats.memtable_bytes == 0);
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({330, 331}, 0, 400, &cursor));
    CHECK(Read(cursor.get()).empty());
    AssertOk(engine->WriteBatch({{330, 100, 4.0}, {331, 100, 5.0}}, &version));
    CHECK(version == 1);
    AssertOk(engine->Close());
}

void TestOversizedMemoryBatchRejected(const std::filesystem::path& path) {
    Status status;
    auto engine = Engine::Open(
        path, Options{.memtable_max_points = 1000000,
                      .enable_background_compaction = false,
                      .max_memtable_bytes = 2 * 32}, &status);
    AssertOk(status);
    AssertOk(engine->Put({340, 100, 1.0}));
    CommitVersion version = 99;
    const auto started = std::chrono::steady_clock::now();
    CHECK(!engine->WriteBatch({{340, 200, 2.0}, {341, 200, 3.0},
                              {342, 200, 4.0}}, &version).ok);
    CHECK(std::chrono::steady_clock::now() - started < std::chrono::seconds(2));
    CHECK(version == 99);
    EngineStats stats;
    AssertOk(engine->GetStats(&stats));
    CHECK(stats.visible_version == 1);
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({340, 341, 342}, 0, 300, &cursor));
    const auto points = Read(cursor.get());
    CHECK(points.size() == 1 && points[0].timestamp_ms == 100);
    AssertOk(engine->Put({341, 200, 5.0}, &version));
    CHECK(version == 2);
    AssertOk(engine->Close());
}

void TestCurrentRssStats(const std::filesystem::path& path) {
    Status status;
    auto engine = Engine::Open(path, {}, &status);
    AssertOk(status);
    AssertOk(engine->Put({350, 100, 1.0}));
    EngineStats stats;
    AssertOk(engine->GetStats(&stats));
    CHECK(stats.rss_bytes > 0);
    CHECK(stats.rss_bytes < (uint64_t{1} << 40));
#if defined(__linux__)
    uint64_t virtual_pages = 0;
    uint64_t resident_pages = 0;
    std::ifstream statm("/proc/self/statm");
    CHECK(static_cast<bool>(statm >> virtual_pages >> resident_pages));
    const long page_size = ::sysconf(_SC_PAGESIZE);
    CHECK(page_size > 0 && resident_pages > 0);
    const auto current_rss = resident_pages * static_cast<uint64_t>(page_size);
    // /proc 与 rusage 不是同一种采样，不要求 peak >= current 或精确相等。
    CHECK(stats.rss_bytes >= current_rss / 4);
    CHECK(stats.rss_bytes <= current_rss * 4);
#endif
    AssertOk(engine->Close());
}

void TestAggregateCountExactType(const std::filesystem::path& path) {
    Status status;
    auto engine = Engine::Open(path, Options{4, 100000}, &status);
    AssertOk(status);
    AssertOk(engine->WriteBatch({{360, 100, 1.0}, {360, 101, 2.0},
                                 {361, 100, 4.0}}));
    AssertOk(engine->Flush());
    AssertOk(engine->Put({360, 102, 3.0}));
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Aggregate({360, 360, 361}, 0, 200, 200,
                               AggregateOp::Count, &cursor));
    uint64_t total = 0;
    size_t buckets = 0;
    while (auto result = cursor->NextAggregate()) {
        CHECK(result->count == (result->series_id == 360 ? 3 : 1));
        CHECK(result->result == static_cast<double>(result->count));
        total += result->count;
        ++buckets;
    }
    CHECK(buckets == 2 && total == 4);
    // 无需构造数千万亿个点，也能检查现有结果游标不把 count 经 double 转换。
    constexpr uint64_t exact_count = (uint64_t{1} << 53) + 1;
    Cursor exact(std::vector<AggregateResult>{
        {360, 0, 200, static_cast<double>(exact_count), exact_count}});
    const auto result = exact.NextAggregate();
    CHECK(result && result->count == exact_count);
    CHECK(result->result == static_cast<double>(exact_count));
    AssertOk(engine->Close());
}

void TestExpirationVersionIdempotencyAndAccounting(
    const std::filesystem::path& path) {
    Status status;
    auto engine = Engine::Open(
        path, Options{.memtable_max_points = 1000000,
                      .enable_background_compaction = false,
                      .hot_series_point_limit = 2}, &status);
    AssertOk(status);
    CommitVersion written = 0;
    AssertOk(engine->WriteBatch({{370, 100, 1.0}, {370, 200, 2.0}}, &written));
    EngineStats before;
    AssertOk(engine->GetStats(&before));
    CHECK(before.mutable_points == 2 && before.memtable_bytes == 2 * 32);
    std::unique_ptr<Cursor> old_view;
    AssertOk(engine->Scan({370}, 0, 500, &old_view));
    CommitVersion expired = 0;
    AssertOk(engine->ExpireBefore(200, &expired));
    EngineStats after;
    AssertOk(engine->GetStats(&after));
    CHECK(expired > written);
    CHECK(after.visible_version == expired);
    CHECK(after.expiration_cutoff == 200);
    CHECK(after.mutable_points == 1 && after.memtable_bytes == 32);
    CommitVersion duplicate = 0;
    AssertOk(engine->ExpireBefore(200, &duplicate));
    EngineStats repeated;
    AssertOk(engine->GetStats(&repeated));
    CHECK(duplicate == expired && repeated.visible_version == expired);
    CHECK(repeated.wal_syncs == after.wal_syncs);
    CHECK(repeated.wal_bytes == after.wal_bytes);
    CHECK(repeated.mutable_points == after.mutable_points);
    CHECK(repeated.memtable_bytes == after.memtable_bytes);
    AssertOk(engine->Put({370, 300, 3.0}));
    EngineStats appended;
    AssertOk(engine->GetStats(&appended));
    CHECK(appended.mutable_points == 2 && appended.memtable_bytes == 2 * 32);
    CHECK(appended.flush_count == 0);
    CHECK(Read(old_view.get()).size() == 2);
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({370}, 0, 500, &cursor));
    const auto points = Read(cursor.get());
    CHECK(points.size() == 2 && points.front().timestamp_ms == 200 &&
          points.back().timestamp_ms == 300);
    AssertOk(engine->ExpireBefore(400));
    EngineStats empty;
    AssertOk(engine->GetStats(&empty));
    CHECK(empty.mutable_points == 0 && empty.memtable_bytes == 0);
    AssertOk(engine->Put({370, 400, 4.0}));
    AssertOk(engine->GetStats(&appended));
    CHECK(appended.mutable_points == 1 && appended.memtable_bytes == 32);
    CHECK(appended.flush_count == 0);
    CHECK(!engine->ExpireBefore(399).ok);
    AssertOk(engine->Close());
}

void TestManifestFailureStickyAndRecoverable(const std::filesystem::path& path) {
    const Options options{.memtable_max_points = 1000000,
                          .enable_background_compaction = false,
                          .enable_background_gc = false};
    Status status;
    auto engine = Engine::Open(path, options, &status);
    AssertOk(status);
    AssertOk(engine->Put({380, 100, 1.0}));
    AssertOk(engine->Flush());
    AssertOk(engine->WriteBatch({{380, 200, 2.0}, {381, 200, 3.0}}));
    CHECK(std::filesystem::create_directory(path / "MANIFEST.tmp"));
    CHECK(!engine->Flush().ok);
    EngineStats failed;
    AssertOk(engine->GetStats(&failed));
    // 移除故障源仍然必须保持 storage_error，不能在下一次操作中重试 IO。
    CHECK(std::filesystem::remove(path / "MANIFEST.tmp"));
    CommitVersion rejected_version = 99;
    CHECK(!engine->WriteBatch({{382, 300, 4.0}}, &rejected_version).ok);
    CHECK(rejected_version == 99);
    CHECK(!engine->GarbageCollect().ok);
    CHECK(!engine->ExpireBefore(failed.expiration_cutoff).ok);
    CHECK(!engine->ExpireBefore(500).ok);
    CHECK(!engine->Flush().ok);
    CHECK(!engine->Compact().ok);
    EngineStats rejected;
    AssertOk(engine->GetStats(&rejected));
    CHECK(rejected.visible_version == failed.visible_version);
    CHECK(rejected.expiration_cutoff == failed.expiration_cutoff);
    CHECK(rejected.wal_syncs == failed.wal_syncs);
    CHECK(rejected.wal_bytes == failed.wal_bytes);
    CHECK(!engine->Close().ok);
    // 重复关闭和析构均须完成，不能保留处于 closing 状态的线程。
    (void)engine->Close();
    engine.reset();
    auto reopened = Engine::Open(path, options, &status);
    AssertOk(status);
    std::unique_ptr<Cursor> cursor;
    AssertOk(reopened->Scan({380, 381, 382}, 0, 1000, &cursor));
    const auto points = Read(cursor.get());
    CHECK(points.size() == 3);
    CHECK(points[0].series_id == 380 && points[0].timestamp_ms == 100);
    CHECK(points[1].series_id == 380 && points[1].timestamp_ms == 200);
    CHECK(points[2].series_id == 381 && points[2].timestamp_ms == 200);
    AssertOk(reopened->Put({382, 300, 4.0}));
    AssertOk(reopened->Close());
}

void TestBackgroundFlushFailureWakesWriter(const std::filesystem::path& path) {
    const Options options{.memtable_max_points = 1000000,
                          .enable_background_compaction = false,
                          .max_memtable_bytes = 2 * 32,
                          .enable_background_gc = false};
    Status status;
    auto engine = Engine::Open(path, options, &status);
    AssertOk(status);
    AssertOk(engine->Put({390, 100, 1.0}));
    CHECK(std::filesystem::create_directory(path / "MANIFEST.tmp"));
    CommitVersion version = 99;
    const auto started = std::chrono::steady_clock::now();
    CHECK(!engine->WriteBatch({{390, 200, 2.0}, {391, 200, 3.0}}, &version).ok);
    CHECK(std::chrono::steady_clock::now() - started < std::chrono::seconds(5));
    CHECK(version == 99);
    CHECK(std::filesystem::remove(path / "MANIFEST.tmp"));
    CHECK(!engine->Put({391, 300, 4.0}).ok);
    CHECK(!engine->GarbageCollect().ok);
    CHECK(!engine->Close().ok);
    engine.reset();
    auto reopened = Engine::Open(path, options, &status);
    AssertOk(status);
    std::unique_ptr<Cursor> cursor;
    AssertOk(reopened->Scan({390, 391}, 0, 1000, &cursor));
    const auto points = Read(cursor.get());
    CHECK(points.size() == 1 && points[0].timestamp_ms == 100);
    AssertOk(reopened->Close());
}

void TestCloseWakesQueueBackpressureWriter(const std::filesystem::path& path) {
    const Options options{.shard_count = 4,
                          .memtable_max_points = 1000000,
                          .max_pending_batches = 1,
                          .compaction_sstable_threshold = 1000000,
                          .max_live_sstables = 1,
                          .enable_background_gc = false};
    Status status;
    auto engine = Engine::Open(path, options, &status);
    AssertOk(status);
    AssertOk(engine->Put({4000, 100, 1.0}));
    AssertOk(engine->Flush());
    AssertOk(engine->Put({4000, 200, 2.0}));
    AssertOk(engine->Flush());
    EngineStats stats;
    AssertOk(engine->GetStats(&stats));
    CHECK(stats.live_sstable_count == 2 && stats.compaction_count == 0);

    std::mutex hook_mutex;
    std::condition_variable hook_cv;
    bool entered_merge = false;
    bool release_merge = false;
    bool entered_wait = false;
    bool entered_close = false;
    bool waiting_done = false;
    testing::SetFaultHook([&](const char* stage) {
        if (std::strcmp(stage, "compaction_merge_begin") == 0) {
            // 合并 hook 不持引擎全局锁，coordinator 可以进入 SST 背压等待。
            std::unique_lock lock(hook_mutex);
            entered_merge = true;
            hook_cv.notify_all();
            hook_cv.wait(lock, [&] { return release_merge; });
        } else if (std::strcmp(stage, "write_queue_wait") == 0) {
            std::lock_guard lock(hook_mutex);
            entered_wait = true;
            hook_cv.notify_all();
        } else if (std::strcmp(stage, "close_begin") == 0) {
            std::lock_guard lock(hook_mutex);
            entered_close = true;
            hook_cv.notify_all();
        }
    });
    Status compact_status;
    std::thread compactor([&] { compact_status = engine->Compact(); });
    bool merge_reached = false;
    {
        std::unique_lock lock(hook_mutex);
        merge_reached = hook_cv.wait_for(lock, std::chrono::seconds(2), [&] {
            return entered_merge;
        });
    }
    if (!merge_reached) {
        {
            std::lock_guard lock(hook_mutex);
            release_merge = true;
        }
        hook_cv.notify_all();
        compactor.join();
        testing::SetFaultHook({});
        (void)engine->Close();
        CHECK(merge_reached);
    }

    Status queued_status;
    std::thread queued([&] { queued_status = engine->Put({4002, 300, 3.0}); });
    bool queue_full = false;
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        status = engine->GetStats(&stats);
        if (!status.ok) break;
        if (stats.pending_batches == 1) {
            queue_full = true;
            break;
        }
        std::this_thread::yield();
    }
    if (!queue_full) {
        {
            std::lock_guard lock(hook_mutex);
            release_merge = true;
        }
        hook_cv.notify_all();
        compactor.join();
        (void)engine->Close();
        queued.join();
        testing::SetFaultHook({});
        CHECK(queue_full);
    }

    Status waiting_status;
    CommitVersion waiting_version = 99;
    std::thread waiting([&] {
        waiting_status = engine->Put({4003, 300, 4.0}, &waiting_version);
        std::lock_guard lock(hook_mutex);
        waiting_done = true;
        hook_cv.notify_all();
    });
    bool wait_reached = false;
    {
        std::unique_lock lock(hook_mutex);
        wait_reached = hook_cv.wait_for(lock, std::chrono::seconds(2), [&] {
            return entered_wait;
        });
    }
    const auto close_started = std::chrono::steady_clock::now();
    Status close_status;
    std::thread closer([&] { close_status = engine->Close(); });
    bool close_reached = false;
    bool waiting_woke = false;
    {
        std::unique_lock lock(hook_mutex);
        close_reached = hook_cv.wait_for(lock, std::chrono::seconds(2), [&] {
            return entered_close;
        });
        // 未入队写者必须由 Close 唤醒，而不是靠释放合并 gate 腾出队列。
        waiting_woke = hook_cv.wait_for(lock, std::chrono::seconds(2), [&] {
            return waiting_done;
        });
        release_merge = true;
    }
    hook_cv.notify_all();
    // 所有线程释放并 join 后再断言，普通失败不会留下悬挂的 hook 或线程。
    compactor.join();
    queued.join();
    waiting.join();
    closer.join();
    testing::SetFaultHook({});
    CHECK(wait_reached && close_reached && waiting_woke);
    CHECK(std::chrono::steady_clock::now() - close_started <
          std::chrono::seconds(5));
    CHECK(compact_status.ok && close_status.ok && queued_status.ok);
    CHECK(!waiting_status.ok);
    CHECK(waiting_version == 99);
    engine.reset();
    auto reopened = Engine::Open(path, options, &status);
    AssertOk(status);
    std::unique_ptr<Cursor> cursor;
    AssertOk(reopened->Scan({4000, 4002, 4003}, 0, 1000, &cursor));
    const auto points = Read(cursor.get());
    CHECK(points.size() == 3);
    CHECK(points[0].series_id == 4000 && points[0].timestamp_ms == 100);
    CHECK(points[1].series_id == 4000 && points[1].timestamp_ms == 200);
    CHECK(points[2].series_id == 4002 && points[2].timestamp_ms == 300);
    for (const auto& point : points) CHECK(point.series_id != 4003);
    AssertOk(reopened->Close());
}

void TestIrreducibleSstablePressureMakesProgress(const std::filesystem::path& path) {
    const Options options{.shard_count = 4,
                          .memtable_max_points = 1000000,
                          .max_pending_batches = 1,
                          .compaction_sstable_threshold = 1000000,
                          .max_live_sstables = 1,
                          .enable_background_gc = false};
    Status status;
    auto engine = Engine::Open(path, options, &status);
    AssertOk(status);
    AssertOk(engine->WriteBatch({{4010, 100, 1.0}, {4012, 100, 2.0}}));
    AssertOk(engine->Flush());
    EngineStats stats;
    AssertOk(engine->GetStats(&stats));
    // 每个分片只有一个文件，合法软上限不能要求这个文件集合继续缩小。
    CHECK(stats.live_sstable_count == 2 && stats.compaction_count == 0);
    const auto started = std::chrono::steady_clock::now();
    CommitVersion version = 99;
    AssertOk(engine->WriteBatch({{4010, 200, 3.0}, {4012, 200, 4.0}}, &version));
    CHECK(version == 2);
    AssertOk(engine->Put({4010, 300, 5.0}, &version));
    CHECK(version == 3);
    CHECK(std::chrono::steady_clock::now() - started < std::chrono::seconds(5));
    AssertOk(engine->GetStats(&stats));
    CHECK(stats.pending_batches == 0 && stats.visible_version == 3);
    CHECK(stats.live_sstable_count == 2 && stats.compaction_count == 0);
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({4010, 4012}, 0, 1000, &cursor));
    CHECK(Read(cursor.get()).size() == 5);
    AssertOk(engine->Close());
    engine.reset();
    auto reopened = Engine::Open(path, options, &status);
    AssertOk(status);
    AssertOk(reopened->Scan({4010, 4012}, 0, 1000, &cursor));
    CHECK(Read(cursor.get()).size() == 5);
    AssertOk(reopened->Put({4012, 300, 6.0}, &version));
    CHECK(version == 4);
    AssertOk(reopened->Close());
}

void TestSstablePressureForcesHighThresholdCompaction(
    const std::filesystem::path& path) {
    const Options options{.shard_count = 1,
                          .memtable_max_points = 1000000,
                          .max_pending_batches = 1,
                          .compaction_sstable_threshold = 1000000,
                          .max_live_sstables = 1,
                          .enable_background_gc = false};
    Status status;
    auto engine = Engine::Open(path, options, &status);
    AssertOk(status);
    AssertOk(engine->Put({4020, 100, 1.0}));
    AssertOk(engine->Flush());
    AssertOk(engine->Put({4020, 200, 2.0}));
    AssertOk(engine->Flush());
    EngineStats stats;
    AssertOk(engine->GetStats(&stats));
    CHECK(stats.live_sstable_count == 2 && stats.compaction_count == 0);
    std::atomic<bool> entered_merge{false};
    testing::SetFaultHook([&](const char* stage) {
        if (std::strcmp(stage, "compaction_merge_begin") == 0) {
            entered_merge.store(true);
        }
    });
    const auto started = std::chrono::steady_clock::now();
    CommitVersion version = 99;
    const auto write_status = engine->Put({4020, 300, 3.0}, &version);
    testing::SetFaultHook({});
    AssertOk(write_status);
    CHECK(std::chrono::steady_clock::now() - started < std::chrono::seconds(5));
    CHECK(entered_merge.load());
    CHECK(version == 3);
    AssertOk(engine->GetStats(&stats));
    // 没有调用 Compact；coordinator 压力请求必须越过正常的百万文件阈值。
    CHECK(stats.compaction_count == 1 && stats.live_sstable_count == 1);
    CHECK(stats.pending_batches == 0 && stats.visible_version == 3);
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({4020}, 0, 1000, &cursor));
    const auto points = Read(cursor.get());
    CHECK(points.size() == 3);
    for (size_t i = 0; i < points.size(); ++i) {
        CHECK(points[i].timestamp_ms == static_cast<int64_t>((i + 1) * 100));
        CHECK(points[i].value == static_cast<double>(i + 1));
    }
    AssertOk(engine->Close());
    engine.reset();
    auto reopened = Engine::Open(path, options, &status);
    AssertOk(status);
    AssertOk(reopened->Scan({4020}, 0, 1000, &cursor));
    CHECK(Read(cursor.get()).size() == 3);
    AssertOk(reopened->Close());
}

void TestPhysicalExpirationAndGC(const std::filesystem::path& path) {
    Status status;
    auto engine = Engine::Open(path, Options{4, 100000}, &status);
    AssertOk(status);
    AssertOk(engine->WriteBatch({{600, 100, 1.0}, {600, 200, 2.0}}));
    AssertOk(engine->Flush());
    std::unique_ptr<Cursor> old_view;
    AssertOk(engine->Scan({600}, 0, 300, &old_view));
    AssertOk(engine->ExpireBefore(300));
    AssertOk(engine->GarbageCollect());
    CHECK(old_view->NextPoint().has_value());
    old_view->Close();
    AssertOk(engine->GarbageCollect());
    std::unique_ptr<Cursor> new_view;
    AssertOk(engine->Scan({600}, 0, 300, &new_view));
    CHECK(Read(new_view.get()).empty());
    AssertOk(engine->Close());
}

void TestMultipleIncrementalFlushes(const std::filesystem::path& path) {
    {
        Status status;
        auto engine = Engine::Open(path, Options{4, 100000}, &status);
        AssertOk(status);
        AssertOk(engine->WriteBatch({{500, 100, 1.0}, {501, 100, 2.0}}));
        AssertOk(engine->Flush());
        AssertOk(engine->WriteBatch({{500, 200, 3.0}, {501, 200, 4.0}}));
        AssertOk(engine->Close());
    }
    Status status;
    auto engine = Engine::Open(path, Options{4, 100000}, &status);
    AssertOk(status);
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({500, 501}, 0, 300, &cursor));
    auto points = Read(cursor.get());
    CHECK(points.size() == 4);
    CHECK(points[0].timestamp_ms == 100);
    CHECK(points[2].timestamp_ms == 200);
    AssertOk(engine->Close());
}

void TestTimestampEncodingRoundTrip() {
    const auto path =
        std::filesystem::temp_directory_path() / "tsdb-encoding-test";
    std::filesystem::remove_all(path);
    Status status;
    auto engine = Engine::Open(path, Options{4, 100000}, &status);
    AssertOk(status);
    AssertOk(engine->WriteBatch({
        {700, -1600, 1.0},
        {700, -1500, 2.0},
        {700, -1499, 3.0},
        {700, 1000, 4.0},
        {700, 2000, 5.0}}));
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({700}, -2000, 3000, &cursor));
    auto points = Read(cursor.get());
    CHECK(points.size() == 5);
    CHECK(points[0].timestamp_ms == -1600);
    CHECK(points.back().timestamp_ms == 2000);
    AssertOk(engine->Close());
}

void TestNegativeWindowsAndDuplicateSeries(const std::filesystem::path& path) {
    Status status;
    auto engine = Engine::Open(path, Options{4, 100000}, &status);
    AssertOk(status);
    AssertOk(engine->WriteBatch({{400, -1501, 1.0}, {400, -1500, 2.0},
                                 {400, -1499, 3.0}}));
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({400, 400}, -1600, -1400, &cursor));
    CHECK(Read(cursor.get()).size() == 3);
    std::unique_ptr<Cursor> aggregate;
    AssertOk(engine->Aggregate({400, 400}, -1600, -1400, 200,
                               AggregateOp::Count, &aggregate));
    uint64_t count = 0;
    while (auto result = aggregate->NextAggregate()) count += result->count;
    CHECK(count == 3);
    AssertOk(engine->Close());
}

void TestLargeSeriesPaging(const std::filesystem::path& path) {
    Status status;
    auto engine = Engine::Open(path, Options{4, 100000}, &status);
    AssertOk(status);
    std::vector<DataPoint> batch;
    batch.reserve(10000);
    for (int i = 0; i < 10000; ++i) {
        batch.push_back({900, static_cast<int64_t>(i * 10),
                         static_cast<double>(i)});
    }
    AssertOk(engine->WriteBatch(batch));
    AssertOk(engine->Flush());
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({900}, 0, 1000000, &cursor));
    CHECK(Read(cursor.get()).size() == 10000);
    AssertOk(engine->Close());
}

void TestFlushClearsMemtable(const std::filesystem::path& path) {
    Status status;
    auto engine = Engine::Open(path, Options{4, 100000}, &status);
    AssertOk(status);
    AssertOk(engine->WriteBatch({{800, 100, 1.0}, {801, 100, 2.0}}));
    EngineStats before;
    AssertOk(engine->GetStats(&before));
    CHECK(before.mutable_points == 2);
    AssertOk(engine->Flush());
    EngineStats after;
    AssertOk(engine->GetStats(&after));
    CHECK(after.mutable_points == 0);
    CHECK(after.memtable_bytes == 0);
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({800, 801}, 0, 200, &cursor));
    CHECK(Read(cursor.get()).size() == 2);
    AssertOk(engine->Close());
}

void TestCompactionMergesSstables(const std::filesystem::path& path) {
    Status status;
    auto engine = Engine::Open(path, Options{4, 100000}, &status);
    AssertOk(status);
    for (int flush = 0; flush < 3; ++flush) {
        std::vector<DataPoint> batch;
        for (int i = 0; i < 100; ++i) {
            batch.push_back({static_cast<uint64_t>(700 + (i % 4)),
                             static_cast<int64_t>(flush * 1000 + i),
                             static_cast<double>(i)});
        }
        AssertOk(engine->WriteBatch(batch));
        AssertOk(engine->Flush());
    }
    AssertOk(engine->Compact());
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({700, 701, 702, 703}, 0, 10000, &cursor));
    CHECK(Read(cursor.get()).size() == 300);
    AssertOk(engine->Close());

    auto reopened = Engine::Open(path, Options{4, 100000}, &status);
    AssertOk(status);
    AssertOk(reopened->Scan({700, 701, 702, 703}, 0, 10000, &cursor));
    CHECK(Read(cursor.get()).size() == 300);
    AssertOk(reopened->Close());
}

void TestCompactionLargeHotSeries(const std::filesystem::path& path) {
    const Options options{.shard_count = 1,
                          .memtable_max_points = 100000,
                          .enable_background_compaction = false,
                          .enable_background_gc = false};
    constexpr uint64_t series_id = std::numeric_limits<uint64_t>::max();
    constexpr int64_t per_file = 60001;
    constexpr int64_t total = 8 * per_file;
    constexpr int64_t base = -400000;
    constexpr int64_t expired = total / 3;
    Status status;
    auto engine = Engine::Open(path, options, &status);
    AssertOk(status);
    for (int64_t file = 0; file < 8; ++file) {
        std::vector<DataPoint> batch;
        batch.reserve(per_file);
        for (int64_t i = file * per_file; i < (file + 1) * per_file; ++i) {
            batch.push_back({series_id, base + i, static_cast<double>(i)});
        }
        AssertOk(engine->WriteBatch(batch));
        AssertOk(engine->Flush());
    }
    EngineStats stats;
    AssertOk(engine->GetStats(&stats));
    CHECK(stats.live_sstable_count == 8);
    std::unique_ptr<Cursor> held;
    AssertOk(engine->Scan({series_id}, base, base + total, &held));
    AssertOk(engine->ExpireBefore(base + expired));
    AssertOk(engine->Compact());
    AssertOk(engine->GarbageCollect());
    AssertOk(engine->GetStats(&stats));
    CHECK(stats.live_sstable_count == 1 && stats.compaction_count == 1);
    CHECK(stats.obsolete_sstable_count == 8 && stats.active_read_views == 1);
    const auto check_points = [&](Cursor* cursor, int64_t first) {
        for (int64_t i = first; i < total; ++i) {
            const auto point = cursor->NextPoint();
            CHECK(point && point->series_id == series_id);
            CHECK(point->timestamp_ms == base + i);
            CHECK(point->value == static_cast<double>(i));
        }
        CHECK(!cursor->NextPoint());
    };
    // 合并和过期之后才继续读取旧视图，包含未预取且已过期的全部输入页。
    check_points(held.get(), 0);
    held->Close();
    AssertOk(engine->GarbageCollect());
    AssertOk(engine->GetStats(&stats));
    CHECK(stats.obsolete_sstable_count == 0 && stats.sstable_count == 1);
    std::unique_ptr<Cursor> current;
    AssertOk(engine->Scan({series_id}, base, base + total, &current));
    check_points(current.get(), expired);
    AssertOk(engine->Close());
    engine = Engine::Open(path, options, &status);
    AssertOk(status);
    AssertOk(engine->Scan({series_id}, base, base + total, &current));
    check_points(current.get(), expired);
    AssertOk(engine->Close());
}

void TestCompactionDropsExpired(const std::filesystem::path& path) {
    Status status;
    auto engine = Engine::Open(path, Options{4, 100000}, &status);
    AssertOk(status);
    AssertOk(engine->WriteBatch(
        {{900, 100, 1.0}, {900, 200, 2.0}, {900, 300, 3.0}}));
    AssertOk(engine->Flush());
    AssertOk(engine->ExpireBefore(250));
    AssertOk(engine->Compact());
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({900}, 0, 1000, &cursor));
    const auto points = Read(cursor.get());
    CHECK(points.size() == 1 && points[0].timestamp_ms == 300);
    AssertOk(engine->Close());
}

void TestMultiLevelCompaction(const std::filesystem::path& path) {
    Status status;
    auto engine = Engine::Open(path, Options{4, 100000}, &status);
    AssertOk(status);
    // Two phases of 8 flushes each. The second Compact() cascades level-0
    // into level-1 and then merges the two level-1 SSTables into level-2.
    const int total_flushes = 16;
    for (int flush = 0; flush < total_flushes; ++flush) {
        std::vector<DataPoint> batch;
        for (int i = 0; i < 50; ++i) {
            batch.push_back({static_cast<uint64_t>(770 + (i % 4)),
                             static_cast<int64_t>(flush * 1000 + i),
                             static_cast<double>(i)});
        }
        AssertOk(engine->WriteBatch(batch));
        AssertOk(engine->Flush());
        if (flush == 7) AssertOk(engine->Compact());
    }
    AssertOk(engine->Compact());
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({770, 771, 772, 773}, 0, 20000, &cursor));
    CHECK(Read(cursor.get()).size() == 16 * 50);
    AssertOk(engine->Close());

    auto reopened = Engine::Open(path, Options{4, 100000}, &status);
    AssertOk(status);
    AssertOk(reopened->Scan({770, 771, 772, 773}, 0, 20000, &cursor));
    CHECK(Read(cursor.get()).size() == 16 * 50);
    AssertOk(reopened->Close());
}

void TestNegativeTimestampRecovery(const std::filesystem::path& path) {
    {
        Status status;
        auto engine = Engine::Open(path, Options{4, 100000}, &status);
        AssertOk(status);
        AssertOk(engine->WriteBatch({{950, -1000, 1.0}, {950, -500, 2.0}}));
        AssertOk(engine->Flush());
        AssertOk(engine->Close());
    }
    Status status;
    auto engine = Engine::Open(path, Options{4, 100000}, &status);
    AssertOk(status);
    AssertOk(engine->Put({950, 0, 3.0}));
    CHECK(!engine->Put({950, -500, 4.0}).ok);
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({950}, -2000, 2000, &cursor));
    CHECK(Read(cursor.get()).size() == 3);
    AssertOk(engine->Close());
}

void TestGcReclaimsWhileViewActive(const std::filesystem::path& path) {
    Status status;
    auto engine = Engine::Open(path, Options{4, 100000}, &status);
    AssertOk(status);
    AssertOk(engine->WriteBatch({{960, 100, 1.0}}));
    AssertOk(engine->Flush());
    std::unique_ptr<Cursor> held;
    AssertOk(engine->Scan({960}, 0, 200, &held));
    AssertOk(engine->WriteBatch({{960, 200, 2.0}}));
    AssertOk(engine->Flush());
    AssertOk(engine->Compact());
    AssertOk(engine->GarbageCollect());
    CHECK(held->NextPoint().has_value());
    held->Close();
    AssertOk(engine->GarbageCollect());
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({960}, 0, 300, &cursor));
    CHECK(Read(cursor.get()).size() == 2);
    AssertOk(engine->Close());
}

void TestSstableChecksum(const std::filesystem::path& path) {
    {
        Status status;
        auto engine = Engine::Open(path, Options{4, 1000}, &status);
        AssertOk(status);
        AssertOk(engine->Put({20, 100, 7.0}));
        AssertOk(engine->Close());
    }
    std::filesystem::path sstable;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(path)) {
        if (entry.path().extension() == ".sst") sstable = entry.path();
    }
    CHECK(!sstable.empty());
    std::fstream file(sstable, std::ios::binary | std::ios::in | std::ios::out);
    file.seekg(-1, std::ios::end);
    char byte = 0;
    file.read(&byte, 1);
    byte ^= 0xff;
    file.seekp(-1, std::ios::end);
    file.write(&byte, 1);
    file.close();
    Status status;
    auto engine = Engine::Open(path, Options{4, 1000}, &status);
    CHECK(!status.ok);
    CHECK(engine == nullptr);
}

// Exercises the optional per-shard WAL path: a cross-shard batch, an expire
// record, a clean reopen and a crash-recovery reopen.
void TestShardedWal(const std::filesystem::path& path) {
    const auto options = Options{.shard_count = 4,
                                 .memtable_max_points = 1000,
                                 .sharded_wal = true};
    {
        Status status;
        auto engine = Engine::Open(path, options, &status);
        AssertOk(status);
        AssertOk(engine->WriteBatch({{1, 100, 1.0}, {2, 200, 2.0}, {3, 300, 3.0}}));
        AssertOk(engine->ExpireBefore(150));
        AssertOk(engine->Close());
    }
    {
        Status status;
        auto engine = Engine::Open(path, options, &status);
        AssertOk(status);
        std::unique_ptr<Cursor> cursor;
        AssertOk(engine->Scan({1, 2, 3}, 0, 1000, &cursor));
        CHECK(Read(cursor.get()).size() == 2);
        AssertOk(engine->Close());
    }

    const pid_t pid = fork();
    CHECK(pid >= 0);
    if (pid == 0) {
        Status status;
        auto engine = Engine::Open(path, options, &status);
        if (!status.ok) _exit(2);
        if (!engine->Put({7, 400, 4.0}).ok) _exit(3);
        _exit(0);
    }
    int child_status = 0;
    waitpid(pid, &child_status, 0);
    CHECK(WIFEXITED(child_status) && WEXITSTATUS(child_status) == 0);

    Status status;
    auto engine = Engine::Open(path, options, &status);
    AssertOk(status);
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({7}, 0, 1000, &cursor));
    CHECK(Read(cursor.get()).size() == 1);
    AssertOk(engine->Close());
}
void TestWalOnSeparateDir(const std::filesystem::path& path) {
    const auto wal_dir = path / "wal-separate";
    const Options options{.shard_count = 4,
                          .memtable_max_points = 1000000,
                          .wal_dir = wal_dir};
    // Crash (no Close) so the WAL on the separate directory must be replayed.
    const pid_t pid = fork();
    CHECK(pid >= 0);
    if (pid == 0) {
        Status status;
        auto engine = Engine::Open(path, options, &status);
        if (!status.ok) _exit(2);
        if (!engine->WriteBatch({{7, 100, 1.0}, {7, 200, 2.0}}).ok) _exit(3);
        _exit(0);
    }
    int child_status = 0;
    waitpid(pid, &child_status, 0);
    CHECK(WIFEXITED(child_status) && WEXITSTATUS(child_status) == 0);
    CHECK(std::filesystem::exists(wal_dir));

    // Reopen with the same WAL directory: records are replayed from it.
    Status status;
    auto engine = Engine::Open(path, options, &status);
    AssertOk(status);
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({7}, 0, 1000, &cursor));
    CHECK(Read(cursor.get()).size() == 2);
    // Flush records the WAL directory in the manifest.
    AssertOk(engine->Close());

    // A different WAL directory would orphan unflushed records: refuse to open.
    auto wrong = Engine::Open(
        path,
        Options{.shard_count = 4, .memtable_max_points = 1000000,
                .wal_dir = path / "wal-other"},
        &status);
    CHECK(wrong == nullptr);
    CHECK(!status.ok);

    // The sharded (concurrent) WAL honours the same override.
    const auto sharded_dir = path / "wal-sharded";
    const auto sharded_path = path / "sharded-db";
    {
        Status sharded_status;
        auto sharded = Engine::Open(
            sharded_path,
            Options{.shard_count = 4, .memtable_max_points = 1000000,
                    .wal_dir = sharded_dir, .sharded_wal = true},
            &sharded_status);
        AssertOk(sharded_status);
        AssertOk(sharded->Put({9, 100, 1.0}));
        AssertOk(sharded->Close());
    }
    CHECK(std::filesystem::exists(sharded_dir / "shard-0"));
    CHECK(std::filesystem::exists(sharded_dir / "commit"));
}

void TestCursorOutlivesEngine(const std::filesystem::path& path) {
    std::unique_ptr<Cursor> cursor;
    {
        Status status;
        auto engine = Engine::Open(path, Options{4, 1000}, &status);
        AssertOk(status);
        AssertOk(engine->Put({42, 100, 1.0}));
        AssertOk(engine->Scan({42}, 0, 200, &cursor));
        AssertOk(engine->Close());
    }
    auto point = cursor->NextPoint();
    CHECK(point && point->timestamp_ms == 100);
    cursor->Close();
}

void TestLargeSeriesStreaming(const std::filesystem::path& path) {
    Status status;
    auto engine = Engine::Open(path, Options{4, 100000}, &status);
    AssertOk(status);
    std::vector<DataPoint> batch;
    batch.reserve(100000);
    for (int i = 0; i < 100000; ++i) {
        batch.push_back({910, static_cast<int64_t>(i * 10),
                         static_cast<double>(i)});
    }
    AssertOk(engine->WriteBatch(batch));
    AssertOk(engine->Flush());
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({910}, 0, 2000000, &cursor));
    CHECK(Read(cursor.get()).size() == 100000);
    AssertOk(engine->Close());
}

void TestPageIndexSelectiveScan(const std::filesystem::path& path) {
    Status status;
    auto engine = Engine::Open(path, Options{4, 100000}, &status);
    AssertOk(status);
    // 1000 points => ~8 pages of 128 points for the single series.
    std::vector<DataPoint> batch;
    batch.reserve(1000);
    for (int i = 0; i < 1000; ++i) {
        batch.push_back({920, static_cast<int64_t>(i), static_cast<double>(i)});
    }
    AssertOk(engine->WriteBatch(batch));
    AssertOk(engine->Flush());

    const auto scan = [&](int64_t lo, int64_t hi) {
        std::unique_ptr<Cursor> cursor;
        AssertOk(engine->Scan({920}, lo, hi, &cursor));
        return Read(cursor.get());
    };
    // Ranges that start/end inside pages and span page boundaries.
    auto points = scan(0, 10);
    CHECK(points.size() == 10 && points.front().timestamp_ms == 0);
    points = scan(500, 520);
    CHECK(points.size() == 20 && points.front().timestamp_ms == 500);
    points = scan(999, 1000);
    CHECK(points.size() == 1 && points.front().timestamp_ms == 999);
    points = scan(128, 256);
    CHECK(points.size() == 128 && points.front().timestamp_ms == 128);
    points = scan(0, 1000);
    CHECK(points.size() == 1000);
    points = scan(1000, 2000);
    CHECK(points.empty());
    // Expiration must also prune pages.
    AssertOk(engine->ExpireBefore(600));
    points = scan(0, 1000);
    CHECK(points.size() == 400 && points.front().timestamp_ms == 600);
    AssertOk(engine->Close());
}

// 过期释放的是逻辑点数额度；vector::erase 不缩 capacity，多条 series 各留下
// 少量点时会累积闲置分配。这里验证显著闲置时确实重建且旧快照不受影响。
void TestExpireReclaimsIdleCapacity(const std::filesystem::path& path) {
    Options options;
    options.shard_count = 1;
    options.memtable_max_points = 100000;
    options.hot_series_point_limit = 100000;
    options.enable_background_compaction = false;
    options.enable_background_gc = false;
    Status status;
    auto engine = Engine::Open(path, options, &status);
    AssertOk(status);
    constexpr int64_t kPoints = 20000;
    std::vector<DataPoint> batch;
    batch.reserve(1000);
    for (int64_t i = 0; i < kPoints; ++i) {
        batch.push_back({501, i, static_cast<double>(i % 97)});
        if (batch.size() == 1000) {
            AssertOk(engine->WriteBatch(batch));
            batch.clear();
        }
    }
    if (!batch.empty()) AssertOk(engine->WriteBatch(batch));
    const auto before = testing::InspectSeriesBuffer(*engine, 501);
    CHECK(before.point_count == static_cast<size_t>(kPoints) &&
          before.capacity.has_value());
    CHECK(*before.capacity >= 4096);

    // 旧游标固定整个 shard：过期必须先 COW，且不能改变旧视图的点。
    std::unique_ptr<Cursor> snapshot;
    AssertOk(engine->Scan({501}, 0, kPoints, &snapshot));
    AssertOk(engine->ExpireBefore(kPoints - 1));
    const auto after = testing::InspectSeriesBuffer(*engine, 501);
    CHECK(after.point_count == 1 && after.capacity.has_value());
    CHECK(*after.capacity >= 1 && *after.capacity * 2 < *before.capacity);
    CHECK(Read(snapshot.get()).size() == static_cast<size_t>(kPoints));

    EngineStats stats;
    AssertOk(engine->GetStats(&stats));
    CHECK(stats.mutable_points == 1);
    CHECK(stats.memtable_bytes == sizeof(DataPoint) + sizeof(CommitVersion));
    AssertOk(engine->Put({501, kPoints, 7.0}));
    std::unique_ptr<Cursor> fresh;
    AssertOk(engine->Scan({501}, 0, kPoints + 1, &fresh));
    CHECK(Read(fresh.get()).size() == 2);
    AssertOk(engine->Close());

    engine = Engine::Open(path, options, &status);
    AssertOk(status);
    std::unique_ptr<Cursor> reopened;
    AssertOk(engine->Scan({501}, 0, kPoints + 1, &reopened));
    const auto recovered = Read(reopened.get());
    CHECK(recovered.size() == 2 && recovered[0].timestamp_ms == kPoints - 1);
    AssertOk(engine->Close());
}

// 单 series 多页时，尾部窄范围查询不应线性遍历前面的页索引；重开后页索引
// 由磁盘重建，必须覆盖同一路径。
void TestPageIndexTailNarrowScan(const std::filesystem::path& path) {
    Options options;
    options.shard_count = 1;
    options.memtable_max_points = 20000;
    options.hot_series_point_limit = 20000;
    options.enable_background_compaction = false;
    options.enable_background_gc = false;
    Status status;
    auto engine = Engine::Open(path, options, &status);
    AssertOk(status);
    constexpr int64_t kPoints = 8000;
    std::vector<DataPoint> batch;
    batch.reserve(1000);
    for (int64_t i = 0; i < kPoints; ++i) {
        batch.push_back({777, i, static_cast<double>(i % 13)});
        if (batch.size() == 1000) {
            AssertOk(engine->WriteBatch(batch));
            batch.clear();
        }
    }
    if (!batch.empty()) AssertOk(engine->WriteBatch(batch));
    AssertOk(engine->Flush());
    const auto narrow = [&](int64_t lo) {
        std::unique_ptr<Cursor> cursor;
        AssertOk(engine->Scan({777}, lo, lo + 128, &cursor));
        const auto points = Read(cursor.get());
        CHECK(points.size() == 128);
        for (size_t j = 0; j < points.size(); ++j) {
            CHECK(points[j].timestamp_ms == lo + static_cast<int64_t>(j));
            CHECK(points[j].value ==
                  static_cast<double>((lo + static_cast<int64_t>(j)) % 13));
        }
    };
    narrow(0);
    narrow(127);
    narrow(4096);
    narrow(7800);
    narrow(kPoints - 128);
    std::unique_ptr<Cursor> beyond;
    AssertOk(engine->Scan({777}, kPoints, kPoints + 128, &beyond));
    CHECK(Read(beyond.get()).empty());
    AssertOk(engine->ExpireBefore(6000));
    std::unique_ptr<Cursor> expired;
    AssertOk(engine->Scan({777}, 7800, 7928, &expired));
    CHECK(Read(expired.get()).size() == 128);
    AssertOk(engine->Close());

    engine = Engine::Open(path, options, &status);
    AssertOk(status);
    std::unique_ptr<Cursor> reopened;
    AssertOk(engine->Scan({777}, 7800, 7928, &reopened));
    CHECK(Read(reopened.get()).size() == 128);
    AssertOk(engine->Close());
}

// gate 占满时，等待中的 Scan 不能提前固定 ReadView；名额释放后正常成功。
void TestScanGateWaiterHoldsNoView(const std::filesystem::path& path) {
    Options options;
    options.shard_count = 1;
    options.enable_background_compaction = false;
    options.enable_background_gc = false;
    Status status;
    auto engine = Engine::Open(path, options, &status);
    AssertOk(status);
    AssertOk(engine->WriteBatch({{1, 10, 1.0}, {1, 20, 2.0}}));
    std::vector<std::unique_ptr<Cursor>> held;
    for (size_t i = 0; i < 16; ++i) {
        std::unique_ptr<Cursor> cursor;
        AssertOk(engine->Scan({1}, 0, 100, &cursor));
        held.push_back(std::move(cursor));
    }
    std::atomic<bool> done{false};
    std::atomic<bool> failed{false};
    std::thread waiter([&] {
        std::unique_ptr<Cursor> cursor;
        failed.store(!engine->Scan({1}, 0, 100, &cursor).ok);
        done.store(true);
    });
    for (int i = 0; i < 100 && !done.load(); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    EngineStats stats;
    AssertOk(engine->GetStats(&stats));
    CHECK(stats.active_read_views == 16);
    held.back()->Close();
    waiter.join();
    CHECK(done.load() && !failed.load());
    AssertOk(engine->GetStats(&stats));
    // Close() 归还了一个名额和一份 view，waiter 的游标析构归还另一份。
    CHECK(stats.active_read_views == 15);
    held.clear();
    AssertOk(engine->Close());
}

// Close 必须唤醒阻塞在查询准入 gate 的 Scan，且等待者返回错误而非半个快照。
void TestScanGateCloseWakesWaiter(const std::filesystem::path& path) {
    Options options;
    options.shard_count = 1;
    options.enable_background_compaction = false;
    options.enable_background_gc = false;
    Status status;
    auto engine = Engine::Open(path, options, &status);
    AssertOk(status);
    AssertOk(engine->WriteBatch({{1, 10, 1.0}, {1, 20, 2.0}}));
    std::vector<std::unique_ptr<Cursor>> held;
    for (size_t i = 0; i < 16; ++i) {
        std::unique_ptr<Cursor> cursor;
        AssertOk(engine->Scan({1}, 0, 100, &cursor));
        held.push_back(std::move(cursor));
    }
    std::atomic<bool> done{false};
    std::atomic<bool> failed{false};
    std::thread waiter([&] {
        std::unique_ptr<Cursor> cursor;
        failed.store(!engine->Scan({1}, 0, 100, &cursor).ok);
        done.store(true);
    });
    for (int i = 0; i < 100 && !done.load(); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    EngineStats stats;
    AssertOk(engine->GetStats(&stats));
    CHECK(stats.active_read_views == 16);
    // 不释放任何名额直接关闭；修复前 waiter 永不醒来，join 会挂住。
    AssertOk(engine->Close());
    waiter.join();
    CHECK(done.load() && failed.load());
    held.clear();
}

// Close 与「刚判定谓词为假、尚未进入 condition_variable::wait」的查询反复竞争。
// 修复前 notify_all 可能落在两者之间而丢失；这里用多轮 + 多等待者放大窗口，
// 由 RunWithTimeout 保证真正丢失唤醒时测试集失败而不是挂住。
void TestScanGateCloseWakeupStress(const std::filesystem::path& path) {
    Options options;
    options.shard_count = 1;
    options.enable_background_compaction = false;
    options.enable_background_gc = false;
    constexpr int kRounds = 24;
    constexpr int kWaiters = 3;
    for (int round = 0; round < kRounds; ++round) {
        const auto db = path / ("round_" + std::to_string(round));
        Status status;
        auto engine = Engine::Open(db, options, &status);
        AssertOk(status);
        AssertOk(engine->WriteBatch({{1, 10, 1.0}, {1, 20, 2.0}}));
        std::vector<std::unique_ptr<Cursor>> held;
        for (size_t i = 0; i < 16; ++i) {
            std::unique_ptr<Cursor> cursor;
            AssertOk(engine->Scan({1}, 0, 100, &cursor));
            held.push_back(std::move(cursor));
        }
        std::atomic<int> returned{0};
        std::vector<std::thread> waiters;
        for (int i = 0; i < kWaiters; ++i) {
            waiters.emplace_back([&] {
                std::unique_ptr<Cursor> cursor;
                if (!engine->Scan({1}, 0, 100, &cursor).ok) ++returned;
            });
        }
        AssertOk(engine->Close());
        for (auto& thread : waiters) thread.join();
        CHECK(returned.load() == kWaiters);
        held.clear();
    }
}

void TestCrashRecovery(const std::filesystem::path& path) {
    const pid_t pid = fork();
    CHECK(pid >= 0);
    if (pid == 0) {
        // Child: write and crash without flushing/Close, so the WAL keeps
        // the committed (fsynced) records for replay.
        Status status;
        auto engine = Engine::Open(path, Options{4, 1000000}, &status);
        if (!status.ok) _exit(2);
        if (!engine->WriteBatch({{5, 100, 1.0}, {5, 200, 2.0}}).ok) _exit(3);
        _exit(0);
    }
    int child_status = 0;
    waitpid(pid, &child_status, 0);
    CHECK(WIFEXITED(child_status) && WEXITSTATUS(child_status) == 0);

    Status status;
    auto engine = Engine::Open(path, Options{4, 1000000}, &status);
    AssertOk(status);
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({5}, 0, 1000, &cursor));
    const auto points = Read(cursor.get());
    CHECK(points.size() == 2);
    CHECK(points[0].timestamp_ms == 100 && points[1].timestamp_ms == 200);
    AssertOk(engine->Close());
}

void TestCrashRecoveryExpire(const std::filesystem::path& path) {
    const pid_t pid = fork();
    CHECK(pid >= 0);
    if (pid == 0) {
        Status status;
        auto engine = Engine::Open(path, Options{4, 1000000}, &status);
        if (!status.ok) _exit(2);
        if (!engine->WriteBatch({{6, 100, 1.0}, {6, 500, 2.0}}).ok) _exit(3);
        if (!engine->ExpireBefore(300).ok) _exit(4);
        _exit(0);
    }
    int child_status = 0;
    waitpid(pid, &child_status, 0);
    CHECK(WIFEXITED(child_status) && WEXITSTATUS(child_status) == 0);

    Status status;
    auto engine = Engine::Open(path, Options{4, 1000000}, &status);
    AssertOk(status);
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({6}, 0, 1000, &cursor));
    const auto points = Read(cursor.get());
    CHECK(points.size() == 1 && points[0].timestamp_ms == 500);
    CHECK(!engine->Put({6, 200, 3.0}).ok);
    AssertOk(engine->Close());
}

// A torn WAL tail must be dropped on recovery. Otherwise the next crash would
// append valid records behind the garbage, and every later open would fail its
// magic/checksum check (the log would be unreadable forever).
void TestTornWalTailRecovery(const std::filesystem::path& path) {
    auto crash_write = [&path](uint64_t series_id, int64_t timestamp) {
        const pid_t pid = fork();
        CHECK(pid >= 0);
        if (pid == 0) {
            Status status;
            auto engine = Engine::Open(path, Options{4, 1000}, &status);
            if (!status.ok) _exit(2);
            if (!engine->Put({series_id, timestamp, 1.0}).ok) _exit(3);
            _exit(0);
        }
        int child_status = 0;
        waitpid(pid, &child_status, 0);
        CHECK(WIFEXITED(child_status) && WEXITSTATUS(child_status) == 0);
    };

    crash_write(31, 100);
    {
        std::ofstream wal(NewestWalSegment(path),
                          std::ios::binary | std::ios::app);
        wal << 'T';  // torn record tail, as a crash mid-write would leave
    }
    crash_write(31, 200);  // opens (must truncate), commits, crashes again

    Status status;
    auto engine = Engine::Open(path, Options{4, 1000}, &status);
    AssertOk(status);
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({31}, 0, 1000, &cursor));
    CHECK(Read(cursor.get()).size() == 2);
    AssertOk(engine->Close());
}

void TestConcurrentCompactionStress(const std::filesystem::path& path) {
    Status status;
    auto engine = Engine::Open(path, Options{4, 5000}, &status);
    AssertOk(status);
    std::atomic<bool> stop{false};
    std::vector<std::thread> threads;
    for (int writer_id = 0; writer_id < 4; ++writer_id) {
        threads.emplace_back([&, writer_id] {
            uint64_t ts = writer_id * 1000000;
            while (!stop.load(std::memory_order_relaxed)) {
                std::vector<DataPoint> batch;
                batch.reserve(100);
                for (int i = 0; i < 100; ++i) {
                    batch.push_back({static_cast<uint64_t>(4000 + writer_id),
                                     static_cast<int64_t>(ts++),
                                     static_cast<double>(i)});
                }
                (void)engine->WriteBatch(batch);
            }
        });
    }
    threads.emplace_back([&] {
        int64_t cutoff = 1000;
        while (!stop.load(std::memory_order_relaxed)) {
            (void)engine->ExpireBefore(cutoff);
            (void)engine->Compact();
            cutoff += 5000;
        }
    });
    std::this_thread::sleep_for(std::chrono::seconds(2));
    stop.store(true);
    for (auto& thread : threads) thread.join();
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({4000, 4001, 4002, 4003}, 0,
                          std::numeric_limits<int64_t>::max(), &cursor));
    while (cursor->NextPoint()) {}
    AssertOk(engine->Close());
}

// --- synthesized WAL records -------------------------------------------------
// The write path never emits descending timestamps or versions below the
// covered range, so these helpers build the on-disk bytes directly and mirror
// the framing in src/tsdb.cpp (WAL magic, FNV-1a checksum, varint payload).

constexpr uint32_t kWalMagicBytes = 0x54534442;
constexpr uint32_t kEncodedPayloadMagic = 0x57564c45;

uint64_t Fnv1a(const std::string& payload) {
    uint64_t hash = 1469598103934665603ULL;
    for (unsigned char byte : payload) {
        hash ^= byte;
        hash *= 1099511628211ULL;
    }
    return hash;
}

template <typename T>
void AppendRaw(std::string* out, T value) {
    out->append(reinterpret_cast<const char*>(&value), sizeof(T));
}

std::string EncodeUvarint(uint64_t value) {
    std::string out;
    while (value >= 0x80) {
        out.push_back(static_cast<char>((value & 0x7f) | 0x80));
        value >>= 7;
    }
    out.push_back(static_cast<char>(value));
    return out;
}

uint64_t EncodeZigzag(int64_t value) {
    return (static_cast<uint64_t>(value) << 1) ^
           (static_cast<uint64_t>(value) >> 63);
}

// One kTxnRecord holding a single point.
std::string TxnRecordBytes(CommitVersion version, uint64_t series_id,
                           int64_t timestamp, double value) {
    std::string payload;
    AppendRaw(&payload, kEncodedPayloadMagic);
    AppendRaw(&payload, version);
    AppendRaw(&payload, static_cast<uint32_t>(1));
    payload += EncodeUvarint(series_id);
    payload += EncodeUvarint(EncodeZigzag(timestamp));
    AppendRaw(&payload, value);

    std::string record;
    AppendRaw(&record, kWalMagicBytes);
    record.push_back(static_cast<char>(1));  // kTxnRecord
    AppendRaw(&record, static_cast<uint64_t>(payload.size()));
    AppendRaw(&record, Fnv1a(payload));
    record += payload;
    return record;
}

// Drops a segment into wal/; the engine must be closed while this runs.
void WriteWalSegment(const std::filesystem::path& db_path, uint64_t number,
                     const std::string& bytes) {
    char name[32];
    std::snprintf(name, sizeof(name), "%06llu.log",
                  static_cast<unsigned long long>(number));
    std::filesystem::create_directories(db_path / "wal");
    std::ofstream out(db_path / "wal" / name,
                      std::ios::binary | std::ios::trunc);
    CHECK(out);
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
}

using CompactionTestPoint = std::pair<DataPoint, CommitVersion>;

// 公共写接口禁止旧时间戳；直接构造带有效校验和的 SST 来覆盖跨文件版本重叠。
void WriteCompactionSstable(const std::filesystem::path& path, size_t number,
                            const std::vector<CompactionTestPoint>& points,
                            bool raw) {
    std::filesystem::create_directories(path / "sst");
    std::string payload;
    for (const auto& [point, version] : points) {
        if (raw) {
            AppendRaw(&payload, point.series_id);
            AppendRaw(&payload, point.timestamp_ms);
        } else {
            payload += EncodeUvarint(point.series_id);
            const uint64_t zigzag =
                (static_cast<uint64_t>(point.timestamp_ms) << 1) ^
                static_cast<uint64_t>(point.timestamp_ms >> 63);
            payload += EncodeUvarint(zigzag);
        }
        AppendRaw(&payload, point.value);
        if (raw) AppendRaw(&payload, version);
        else payload += EncodeUvarint(version);
    }
    std::string bytes;
    AppendRaw(&bytes, raw ? kWalMagicBytes : uint32_t{0x53535445});
    AppendRaw(&bytes, static_cast<uint64_t>(points.size()));
    bytes += payload;
    AppendRaw(&bytes, Fnv1a(payload));
    std::ofstream out(path / "sst" / ("shard-00-" + std::to_string(number) + ".sst"),
                      std::ios::binary | std::ios::trunc);
    CHECK(out);
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    out.close();
    CHECK(out);
}

void WriteCompactionManifest(const std::filesystem::path& path, size_t files,
                             CommitVersion next_version) {
    std::string payload = std::to_string(std::numeric_limits<int64_t>::min()) +
                          " " + std::to_string(next_version) + " " +
                          std::to_string(files + 1) + "\n";
    for (size_t i = 1; i <= files; ++i) {
        payload += "0 sst/shard-00-" + std::to_string(i) + ".sst\n";
    }
    std::ofstream out(path / "MANIFEST");
    CHECK(out);
    out << payload << "CHECKSUM " << Fnv1a(payload) << '\n';
    out.close();
    CHECK(out);
}

void TestCompactionOverlappingVersions(const std::filesystem::path& path) {
    constexpr uint64_t hot_series = std::numeric_limits<uint64_t>::max();
    constexpr CommitVersion version_base = uint64_t{1} << 63;
    constexpr int64_t count = 7001;
    constexpr int64_t base = -3500;
    for (size_t source = 0; source < 8; ++source) {
        std::vector<CompactionTestPoint> points;
        points.push_back({{7 + source, 0, static_cast<double>(source)}, 1});
        if (source % 2 == 0) points.push_back({{100, 1, 100.0}, 2});
        for (int64_t i = 0; i < count; ++i) {
            const auto version = version_base + (i + source) % 8 + 1;
            points.push_back({{hot_series, base + i,
                               static_cast<double>(source * 100000 + i)}, version});
            if (i % 128 == 0) {
                // 同一输入里的重复时间戳也必须参与最大版本去重。
                points.push_back({{hot_series, base + i, -1.0}, 0});
            }
        }
        WriteCompactionSstable(path, source + 1, points, source % 3 == 0);
    }
    WriteCompactionManifest(path, 8, version_base + 9);
    const Options options{.shard_count = 1,
                          .enable_background_compaction = false,
                          .enable_background_gc = false};
    Status status;
    auto engine = Engine::Open(path, options, &status);
    AssertOk(status);
    const auto check_hot = [&](Cursor* cursor, int64_t first) {
        for (int64_t i = first; i < count; ++i) {
            const auto point = cursor->NextPoint();
            const int64_t source = 7 - i % 8;
            CHECK(point && point->series_id == hot_series);
            CHECK(point->timestamp_ms == base + i);
            CHECK(point->value == static_cast<double>(source * 100000 + i));
        }
        CHECK(!cursor->NextPoint());
    };
    std::unique_ptr<Cursor> held;
    AssertOk(engine->Scan({hot_series}, base, base + count, &held));
    constexpr int64_t cutoff = -37;
    AssertOk(engine->ExpireBefore(cutoff));
    AssertOk(engine->Compact());
    AssertOk(engine->GarbageCollect());
    check_hot(held.get(), 0);
    held->Close();
    AssertOk(engine->GarbageCollect());
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({hot_series}, base, base + count, &cursor));
    check_hot(cursor.get(), cutoff - base);
    AssertOk(engine->Scan({7, 8, 9, 10, 11, 12, 13, 14, 100}, -1, 2, &cursor));
    const auto cold = Read(cursor.get());
    CHECK(cold.size() == 9);
    for (size_t i = 0; i < 8; ++i) {
        CHECK(cold[i].series_id == 7 + i && cold[i].timestamp_ms == 0);
        CHECK(cold[i].value == static_cast<double>(i));
    }
    CHECK(cold.back().series_id == 100 && cold.back().value == 100.0);
    AssertOk(engine->Close());
    engine = Engine::Open(path, options, &status);
    AssertOk(status);
    AssertOk(engine->Scan({hot_series}, base, base + count, &cursor));
    check_hot(cursor.get(), cutoff - base);
    AssertOk(engine->Close());
}

void TestCompactionReadFailure(const std::filesystem::path& path) {
    for (size_t mode = 0; mode < 4; ++mode) {
        const auto db = path / std::to_string(mode);
        std::vector<CompactionTestPoint> points;
        for (int64_t i = 0; i < 5001; ++i) {
            points.push_back({{std::numeric_limits<uint64_t>::max(), i,
                               static_cast<double>(i)}, 1});
        }
        const bool raw = mode == 1;
        WriteCompactionSstable(db, 1, points, raw);
        WriteCompactionSstable(db, 2, points, raw);
        WriteCompactionManifest(db, 2, 2);
        const Options options{.shard_count = 1,
                              .enable_background_compaction = false,
                              .enable_background_gc = false};
        Status status;
        auto engine = Engine::Open(db, options, &status);
        AssertOk(status);
        std::ifstream manifest_before(db / "MANIFEST");
        const std::string before(std::istreambuf_iterator<char>(manifest_before), {});
        manifest_before.close();
        const auto source = db / "sst" / "shard-00-1.sst";
        const auto backup = source.string() + ".backup";
        std::filesystem::rename(source, backup);
        if (mode < 2) {
            std::filesystem::copy_file(backup, source);
            // 第一缓冲完整可读，第二次 refill 后遇到截断，而不是只测打开失败。
            std::filesystem::resize_file(source, 12 + 64 * 1024 + 1);
        } else if (mode == 3) {
            // open 目录会成功，但 pread 返回负值，不能转换为无符号的记录数。
            CHECK(std::filesystem::create_directory(source));
        }
        const auto compact_status = engine->Compact();
        CHECK(!compact_status.ok);
        CHECK(compact_status.message.find(mode == 2 ? "compaction open failed" :
                                                     "compaction read failed") !=
              std::string::npos);
        std::ifstream manifest_after(db / "MANIFEST");
        const std::string after(std::istreambuf_iterator<char>(manifest_after), {});
        CHECK(before == after);
        for (const auto& file : std::filesystem::directory_iterator(db / "sst")) {
            CHECK(file.path().extension() != ".tmp");
        }
        if (mode == 3) CHECK(std::filesystem::remove(source));
        std::filesystem::rename(backup, source);
        CHECK(!engine->Close().ok);
        engine.reset();
        engine = Engine::Open(db, options, &status);
        AssertOk(status);
        AssertOk(engine->Compact());
        std::unique_ptr<Cursor> cursor;
        AssertOk(engine->Scan({std::numeric_limits<uint64_t>::max()}, 0, 5001,
                              &cursor));
        const auto recovered = Read(cursor.get());
        CHECK(recovered.size() == points.size());
        for (size_t i = 0; i < recovered.size(); ++i) {
            CHECK(recovered[i].timestamp_ms == static_cast<int64_t>(i));
            CHECK(recovered[i].value == static_cast<double>(i));
        }
        AssertOk(engine->Close());
    }
}

// Normal append keeps the per-series buffer time-ordered, including across the
// vector growth boundary and with two series interleaved in one batch.
void TestVectorAppendOrder(const std::filesystem::path& path) {
    Status status;
    auto engine = Engine::Open(path, Options{4, 100000}, &status);
    AssertOk(status);
    std::vector<DataPoint> batch;
    batch.reserve(600);
    for (int i = 0; i < 300; ++i) {
        batch.push_back({1, 1000 + i, static_cast<double>(i)});
        batch.push_back({2, 1000 + i, static_cast<double>(i) * 2.0});
    }
    AssertOk(engine->WriteBatch(batch));
    std::vector<DataPoint> more;
    more.reserve(800);
    for (int i = 300; i < 700; ++i) {
        more.push_back({1, 1000 + i, static_cast<double>(i)});
        more.push_back({2, 1000 + i, static_cast<double>(i) * 2.0});
    }
    AssertOk(engine->WriteBatch(more));

    for (const uint64_t series_id : {uint64_t{1}, uint64_t{2}}) {
        std::unique_ptr<Cursor> cursor;
        AssertOk(engine->Scan({series_id}, 0, 5000, &cursor));
        const auto points = Read(cursor.get());
        CHECK(points.size() == 700);
        for (size_t i = 0; i < points.size(); ++i) {
            CHECK(points[i].series_id == series_id);
            CHECK(points[i].timestamp_ms == 1000 + static_cast<int64_t>(i));
            const double expected = series_id == 1
                                        ? static_cast<double>(i)
                                        : static_cast<double>(i) * 2.0;
            CHECK(points[i].value == expected);
        }
    }
    AssertOk(engine->Close());
}

// WAL replay can hand the buffer timestamps that are not ascending; the append
// must fall back to an ordered insert and stay sorted through the flush.
void TestVectorReplayOrder(const std::filesystem::path& path) {
    {
        Status status;
        auto engine = Engine::Open(path, Options{4, 100000}, &status);
        AssertOk(status);
        AssertOk(engine->Put({7, 300, 3.0}));  // flushed by Close, version 1
        AssertOk(engine->Close());
    }
    // Two records for the same series in descending order: 200 is appended
    // before 100, so the buffer has to insert in the middle.
    WriteWalSegment(path, 1, TxnRecordBytes(2, 7, 200, 2.0) +
                                 TxnRecordBytes(3, 7, 100, 1.0));

    Status status;
    auto engine = Engine::Open(path, Options{4, 100000}, &status);
    AssertOk(status);
    // The crafted segment must actually have been replayed; otherwise this test
    // would pass even if the replay path ignored it entirely.
    EngineStats replayed;
    AssertOk(engine->GetStats(&replayed));
    CHECK(replayed.wal_bytes_replayed > 0);
    // SST 中的最大时间是 300，乱序恢复不能把写入水位退到 100。
    CHECK(!engine->Put({7, 250, 25.0}).ok);
    CHECK(!engine->Put({7, 300, 30.0}).ok);
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({7}, 0, 1000, &cursor));
    const auto points = Read(cursor.get());
    CHECK(points.size() == 3);
    CHECK(points[0].timestamp_ms == 100 && points[0].value == 1.0);
    CHECK(points[1].timestamp_ms == 200 && points[1].value == 2.0);
    CHECK(points[2].timestamp_ms == 300 && points[2].value == 3.0);

    // The order must survive the flush: the SSTable writer requires ascending
    // timestamps inside a series chunk.
    AssertOk(engine->Flush());
    std::unique_ptr<Cursor> flushed;
    AssertOk(engine->Scan({7}, 0, 1000, &flushed));
    const auto flushed_points = Read(flushed.get());
    CHECK(flushed_points.size() == 3);
    for (size_t i = 0; i < flushed_points.size(); ++i) {
        CHECK(flushed_points[i].timestamp_ms ==
               100 + static_cast<int64_t>(i) * 100);
    }
    AssertOk(engine->Close());
}

// 同一 memtable 内乱序恢复重复 timestamp，必须保留最高 version 且只计一个点。
void TestVectorReplayDedupAndAccounting(const std::filesystem::path& path) {
    constexpr size_t kPointBytes = sizeof(DataPoint) + sizeof(CommitVersion);
    const Options options{.shard_count = 1,
                          .memtable_max_points = 100000,
                          .enable_background_compaction = false,
                          .max_memtable_bytes = 5 * kPointBytes,
                          .hot_series_point_limit = 5};
    Status status;
    {
        auto engine = Engine::Open(path, options, &status);
        AssertOk(status);
        AssertOk(engine->Close());
    }
    WriteWalSegment(path, 1, TxnRecordBytes(1, 7, 300, 3.0) +
                                 TxnRecordBytes(4, 7, 100, 4.0) +
                                 TxnRecordBytes(5, 7, 300, 5.0) +
                                 TxnRecordBytes(2, 7, 100, 2.0) +
                                 TxnRecordBytes(3, 7, 200, 3.0) +
                                 TxnRecordBytes(4, 7, 300, 4.0) +
                                 TxnRecordBytes(7, 7, 200, 7.0) +
                                 TxnRecordBytes(6, 7, 50, 0.6) +
                                 TxnRecordBytes(6, 7, 50, 60.0));
    auto engine = Engine::Open(path, options, &status);
    AssertOk(status);
    EngineStats stats;
    AssertOk(engine->GetStats(&stats));
    CHECK(stats.wal_bytes_replayed > 0);
    CHECK(stats.visible_version == 7);
    CHECK(stats.mutable_points == 4);
    CHECK(stats.memtable_bytes == 4 * kPointBytes);
    CHECK(stats.flush_count == 0);
    const auto buffer = testing::InspectSeriesBuffer(*engine, 7);
    CHECK(buffer.point_count == 4);
    CHECK(buffer.capacity.has_value() && *buffer.capacity >= 4);

    std::unique_ptr<Cursor> all;
    AssertOk(engine->Scan({7}, 0, 400, &all));
    const auto points = Read(all.get());
    CHECK(points.size() == 4);
    CHECK(points[0].timestamp_ms == 50 && points[0].value == 0.6);
    CHECK(points[1].timestamp_ms == 100 && points[1].value == 4.0);
    CHECK(points[2].timestamp_ms == 200 && points[2].value == 7.0);
    CHECK(points[3].timestamp_ms == 300 && points[3].value == 5.0);
    CHECK(!engine->Put({7, 250, 25.0}).ok);
    CHECK(!engine->Put({7, 300, 30.0}).ok);
    // 四个唯一点尚有一个位置，重复 WAL 不能制造内存或热点背压。
    AssertOk(engine->Put({7, 301, 301.0}));
    AssertOk(engine->GetStats(&stats));
    CHECK(stats.mutable_points == 5 && stats.memtable_bytes == 5 * kPointBytes);
    CHECK(stats.flush_count == 0);

    std::unique_ptr<Cursor> snapshot;
    AssertOk(engine->Scan({7}, 100, 301, &snapshot));
    AssertOk(engine->ExpireBefore(200));
    AssertOk(engine->GetStats(&stats));
    CHECK(stats.mutable_points == 3 && stats.memtable_bytes == 3 * kPointBytes);
    const auto retained_snapshot = Read(snapshot.get());
    CHECK(retained_snapshot.size() == 3);
    CHECK(retained_snapshot[0].timestamp_ms == 100 &&
          retained_snapshot[0].value == 4.0);
    CHECK(retained_snapshot[1].timestamp_ms == 200 &&
          retained_snapshot[1].value == 7.0);
    CHECK(retained_snapshot[2].timestamp_ms == 300 &&
          retained_snapshot[2].value == 5.0);
    std::unique_ptr<Cursor> narrowed;
    AssertOk(engine->Scan({7}, 100, 301, &narrowed));
    const auto narrowed_points = Read(narrowed.get());
    CHECK(narrowed_points.size() == 2);
    CHECK(narrowed_points[0].timestamp_ms == 200);
    CHECK(narrowed_points[1].timestamp_ms == 300);
    AssertOk(engine->Scan({7}, 0, 100, &narrowed));
    CHECK(Read(narrowed.get()).empty());

    // 前缀删除也要释放逻辑计数，填满后再添一个点才触发自动 flush。
    AssertOk(engine->Put({7, 302, 302.0}));
    AssertOk(engine->Put({7, 303, 303.0}));
    AssertOk(engine->GetStats(&stats));
    CHECK(stats.mutable_points == 5 && stats.memtable_bytes == 5 * kPointBytes);
    CHECK(stats.flush_count == 0);
    AssertOk(engine->Put({7, 304, 304.0}));
    AssertOk(engine->GetStats(&stats));
    CHECK(stats.flush_count == 1);
    CHECK(stats.mutable_points == 1 && stats.memtable_bytes == kPointBytes);
    AssertOk(engine->Close());

    auto reopened = Engine::Open(path, options, &status);
    AssertOk(status);
    std::unique_ptr<Cursor> flushed;
    AssertOk(reopened->Scan({7}, 0, 400, &flushed));
    const auto flushed_points = Read(flushed.get());
    CHECK(flushed_points.size() == 6);
    CHECK(flushed_points[0].timestamp_ms == 200 && flushed_points[0].value == 7.0);
    CHECK(flushed_points[1].timestamp_ms == 300 && flushed_points[1].value == 5.0);
    for (size_t i = 2; i < flushed_points.size(); ++i) {
        CHECK(flushed_points[i].timestamp_ms == 299 + static_cast<int64_t>(i));
        CHECK(flushed_points[i].value == static_cast<double>(flushed_points[i].timestamp_ms));
    }
    AssertOk(reopened->Close());
}

// 恢复得到的唯一点必须计入 memtable_max_points 的自动 flush 阈值。
void TestVectorReplayFlushThreshold(const std::filesystem::path& path) {
    const Options options{.shard_count = 1,
                          .memtable_max_points = 4,
                          .enable_background_compaction = false};
    Status status;
    {
        auto engine = Engine::Open(path, options, &status);
        AssertOk(status);
        AssertOk(engine->Close());
    }
    WriteWalSegment(path, 1, TxnRecordBytes(1, 8, 400, 4.0) +
                                 TxnRecordBytes(2, 8, 100, 1.0) +
                                 TxnRecordBytes(3, 8, 300, 3.0) +
                                 TxnRecordBytes(4, 8, 200, 2.0) +
                                 TxnRecordBytes(5, 8, 100, 5.0));
    auto engine = Engine::Open(path, options, &status);
    AssertOk(status);
    EngineStats stats;
    AssertOk(engine->GetStats(&stats));
    CHECK(stats.mutable_points == 4 && stats.flush_count == 0);
    std::mutex hook_mutex;
    std::condition_variable hook_cv;
    bool flush_reached = false;
    testing::SetFaultHook([&](const char* stage) {
        if (std::strcmp(stage, "sstable_before_rename") != 0) return;
        std::lock_guard lock(hook_mutex);
        flush_reached = true;
        hook_cv.notify_one();
    });
    AssertOk(engine->Put({8, 401, 401.0}));
    bool notified = false;
    {
        std::unique_lock lock(hook_mutex);
        notified = hook_cv.wait_for(lock, std::chrono::seconds(2), [&] {
            return flush_reached;
        });
    }
    testing::SetFaultHook({});
    AssertOk(engine->GetStats(&stats));
    CHECK(notified);
    CHECK(stats.flush_count >= 1);
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({8}, 0, 500, &cursor));
    const auto points = Read(cursor.get());
    CHECK(points.size() == 5);
    CHECK(points[0].timestamp_ms == 100 && points[0].value == 5.0);
    CHECK(points.back().timestamp_ms == 401);
    AssertOk(engine->Close());
}

// A replayed record for a key that already exists in an SSTable must not
// produce a duplicate point, and the higher commit version wins.
void TestDuplicateVersionDedup(const std::filesystem::path& path) {
    {
        Status status;
        auto engine = Engine::Open(path, Options{4, 100000}, &status);
        AssertOk(status);
        AssertOk(engine->Put({9, 500, 5.0}));
        AssertOk(engine->Flush());  // (9,500) now lives in an SSTable at version 1
        AssertOk(engine->Close());
    }
    // Stale record for the same key: version 0 is below the covered version, so
    // the SSTable copy must win and the key must be returned exactly once.
    WriteWalSegment(path, 1, TxnRecordBytes(0, 9, 500, 9.0));

    Status status;
    auto engine = Engine::Open(path, Options{4, 100000}, &status);
    AssertOk(status);
    EngineStats replayed;
    AssertOk(engine->GetStats(&replayed));
    CHECK(replayed.wal_bytes_replayed > 0);
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({9}, 0, 1000, &cursor));
    const auto points = Read(cursor.get());
    CHECK(points.size() == 1);
    CHECK(points[0].timestamp_ms == 500);
    CHECK(points[0].value == 5.0);

    // The stale record is below the covered version, so the flush must skip it
    // rather than writing it into a second SSTable.
    AssertOk(engine->Flush());
    std::unique_ptr<Cursor> flushed;
    AssertOk(engine->Scan({9}, 0, 1000, &flushed));
    const auto flushed_points = Read(flushed.get());
    CHECK(flushed_points.size() == 1);
    CHECK(flushed_points[0].value == 5.0);
    AssertOk(engine->Close());
}

// A cursor opened before later writes keeps its own read view: the commit has to
// copy the series buffer (COW) instead of appending to the snapshot's vector.
void TestCowReadViewIsolation(const std::filesystem::path& path) {
    Status status;
    auto engine = Engine::Open(path, Options{4, 100000}, &status);
    AssertOk(status);
    std::vector<DataPoint> first;
    first.reserve(100);
    for (int i = 0; i < 100; ++i) {
        first.push_back({42, i, static_cast<double>(i)});
    }
    AssertOk(engine->WriteBatch(first));

    std::unique_ptr<Cursor> old_view;
    AssertOk(engine->Scan({42}, 0, 1000, &old_view));

    std::vector<DataPoint> second;
    second.reserve(100);
    for (int i = 100; i < 200; ++i) {
        second.push_back({42, i, static_cast<double>(i)});
    }
    AssertOk(engine->WriteBatch(second));

    // The old cursor must still see exactly the first 100 points.
    const auto old_points = Read(old_view.get());
    CHECK(old_points.size() == 100);
    for (size_t i = 0; i < old_points.size(); ++i) {
        CHECK(old_points[i].timestamp_ms == static_cast<int64_t>(i));
        CHECK(old_points[i].value == static_cast<double>(i));
    }

    std::unique_ptr<Cursor> fresh;
    AssertOk(engine->Scan({42}, 0, 1000, &fresh));
    const auto fresh_points = Read(fresh.get());
    CHECK(fresh_points.size() == 200);
    CHECK(fresh_points.back().timestamp_ms == 199);

    // Same rule across a flush: a snapshot taken after the flush ignores the
    // write that follows it.
    AssertOk(engine->Flush());
    std::unique_ptr<Cursor> after_flush;
    AssertOk(engine->Scan({42}, 0, 1000, &after_flush));
    AssertOk(engine->Put({42, 200, 200.0}));
    const auto after_flush_points = Read(after_flush.get());
    CHECK(after_flush_points.size() == 200);
    CHECK(after_flush_points.back().timestamp_ms == 199);
    AssertOk(engine->Close());
}

// Empty scans and aggregates, including a series that was never written.
void TestEmptyDataset(const std::filesystem::path& path) {
    Status status;
    auto engine = Engine::Open(path, Options{4, 100000}, &status);
    AssertOk(status);
    std::unique_ptr<Cursor> cursor;

    AssertOk(engine->Scan({}, 0, 1000, &cursor));
    CHECK(Read(cursor.get()).empty());

    AssertOk(engine->Scan({123}, 0, 1000, &cursor));
    CHECK(Read(cursor.get()).empty());

    AssertOk(engine->Scan({123}, 500, 500, &cursor));
    CHECK(Read(cursor.get()).empty());

    AssertOk(engine->Aggregate({123}, 0, 1000, 10, AggregateOp::Sum, &cursor));
    CHECK(!cursor->NextAggregate().has_value());

    AssertOk(engine->Aggregate({}, 0, 1000, 10, AggregateOp::Count, &cursor));
    CHECK(!cursor->NextAggregate().has_value());

    // An engine that has only ever been flushed is still empty.
    AssertOk(engine->Flush());
    AssertOk(engine->Scan({123}, 0, 1000, &cursor));
    CHECK(Read(cursor.get()).empty());
    AssertOk(engine->Close());
}

// Window boundaries with negative timestamps, and empty buckets must not be
// reported.
void TestAggregateBuckets(const std::filesystem::path& path) {
    Status status;
    auto engine = Engine::Open(path, Options{4, 100000}, &status);
    AssertOk(status);
    AssertOk(engine->WriteBatch({{1, -250, 2.0},
                                 {1, -240, 3.0},
                                 {1, -100, 7.0},
                                 {1, 100, 11.0}}));
    // window 100 starting at -300: buckets -300 (two points), -100 and 100;
    // -200 and 0 are empty and must be skipped.
    const auto run = [&](AggregateOp op) {
        std::unique_ptr<Cursor> cursor;
        AssertOk(engine->Aggregate({1}, -300, 200, 100, op, &cursor));
        std::vector<AggregateResult> out;
        while (auto bucket = cursor->NextAggregate()) out.push_back(*bucket);
        return out;
    };

    const auto sums = run(AggregateOp::Sum);
    CHECK(sums.size() == 3);
    CHECK(sums[0].bucket_start == -300 && sums[0].bucket_end == -200);
    CHECK(sums[0].result == 5.0 && sums[0].count == 2);
    CHECK(sums[1].bucket_start == -100 && sums[1].result == 7.0);
    CHECK(sums[2].bucket_start == 100 && sums[2].bucket_end == 200);
    CHECK(sums[2].result == 11.0);

    const auto counts = run(AggregateOp::Count);
    CHECK(counts.size() == 3);
    CHECK(counts[0].count == 2 && counts[0].result == 2.0);
    CHECK(counts[1].count == 1 && counts[2].count == 1);

    const auto averages = run(AggregateOp::Avg);
    CHECK(averages.size() == 3 && averages[0].result == 2.5);

    const auto minimums = run(AggregateOp::Min);
    CHECK(minimums.size() == 3 && minimums[0].result == 2.0);
    CHECK(minimums[1].result == 7.0 && minimums[2].result == 11.0);

    const auto maximums = run(AggregateOp::Max);
    CHECK(maximums.size() == 3 && maximums[0].result == 3.0);
    CHECK(maximums[2].result == 11.0);
    AssertOk(engine->Close());
}

// Timestamps at the edges of int64 must round-trip through WAL and SSTable.
void TestExtremeTimestamps(const std::filesystem::path& path) {
    constexpr int64_t kMin = std::numeric_limits<int64_t>::min();
    constexpr int64_t kMax = std::numeric_limits<int64_t>::max();
    Status status;
    auto engine = Engine::Open(path, Options{4, 100000}, &status);
    AssertOk(status);
    AssertOk(engine->Put({1, kMin, 1.0}));
    AssertOk(engine->Put({1, kMin + 1, 3.0}));
    AssertOk(engine->Put({2, kMax, 2.0}));

    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({1}, kMin, kMax, &cursor));
    const auto points = Read(cursor.get());
    CHECK(points.size() == 2);
    CHECK(points[0].timestamp_ms == kMin);
    CHECK(points[1].timestamp_ms == kMin + 1);

    // [start, end) is half-open, so the largest timestamp cannot be reached by
    // a range query; writing it is legal and it stays in the database.
    std::unique_ptr<Cursor> max_cursor;
    AssertOk(engine->Scan({2}, kMin, kMax, &max_cursor));
    CHECK(Read(max_cursor.get()).empty());

    AssertOk(engine->Flush());
    AssertOk(engine->Close());
    auto reopened = Engine::Open(path, Options{4, 100000}, &status);
    AssertOk(status);
    std::unique_ptr<Cursor> again;
    AssertOk(reopened->Scan({1}, kMin, kMax, &again));
    const auto again_points = Read(again.get());
    CHECK(again_points.size() == 2);
    CHECK(again_points[0].timestamp_ms == kMin && again_points[0].value == 1.0);
    CHECK(again_points[1].timestamp_ms == kMin + 1 &&
           again_points[1].value == 3.0);
    AssertOk(reopened->Close());
}

// Appending after a snapshot must reallocate the per-series vector and the
// snapshot must survive it. The capacity is measured through the test hook, so
// the "this write must reallocate" claim is grounded in the actual buffer rather
// than in an assumed growth factor: growing past capacity forces a reallocation,
// and without copy-on-write the cursor's iterators dangle (ASan reports it).
void TestCowVectorReallocation(const std::filesystem::path& path) {
    Status status;
    auto engine = Engine::Open(path, Options{4, 100000}, &status);
    AssertOk(status);
    constexpr int kInitial = 100;
    std::vector<DataPoint> first;
    first.reserve(kInitial);
    for (int i = 0; i < kInitial; ++i) {
        first.push_back({77, i, static_cast<double>(i)});
    }
    AssertOk(engine->WriteBatch(first));

    // The snapshot pins the shard, so the next commit has to copy the buffer.
    std::unique_ptr<Cursor> old_view;
    AssertOk(engine->Scan({77}, 0, 100000, &old_view));

    // The cursor and the live shard share this buffer instance right now, so its
    // capacity is exactly the one an in-place append would have to grow.
    const auto before = testing::InspectSeriesBuffer(*engine, 77);
    CHECK(before.point_count == static_cast<size_t>(kInitial));

    // 必须测得真实 vector capacity，不能用 map 路径绕过重分配验证。
    CHECK(before.capacity.has_value());
    CHECK(*before.capacity >= before.point_count);
    const size_t target = *before.capacity + 1;
    CHECK(target > before.point_count);
    std::vector<DataPoint> second;
    second.reserve(target - before.point_count);
    for (size_t i = static_cast<size_t>(kInitial); i < target; ++i) {
        second.push_back({77, static_cast<int64_t>(i), static_cast<double>(i)});
    }
    CHECK(!second.empty());
    AssertOk(engine->WriteBatch(second));

    const auto after = testing::InspectSeriesBuffer(*engine, 77);
    CHECK(after.point_count == target);
    CHECK(after.point_count > *before.capacity);
    CHECK(after.capacity.has_value());
    CHECK(*after.capacity >= after.point_count);

    const auto old_points = Read(old_view.get());
    CHECK(old_points.size() == static_cast<size_t>(kInitial));
    for (size_t i = 0; i < old_points.size(); ++i) {
        CHECK(old_points[i].timestamp_ms == static_cast<int64_t>(i));
        CHECK(old_points[i].value == static_cast<double>(i));
    }

    std::unique_ptr<Cursor> fresh;
    AssertOk(engine->Scan({77}, 0, 100000, &fresh));
    const auto fresh_points = Read(fresh.get());
    CHECK(fresh_points.size() == target);
    CHECK(fresh_points.back().timestamp_ms == static_cast<int64_t>(target - 1));

    // The reallocated buffer must also survive a flush and land in the SSTable
    // in timestamp order.
    AssertOk(engine->Flush());
    std::unique_ptr<Cursor> flushed;
    AssertOk(engine->Scan({77}, 0, 100000, &flushed));
    const auto flushed_points = Read(flushed.get());
    CHECK(flushed_points.size() == target);
    for (size_t i = 0; i < flushed_points.size(); ++i) {
        CHECK(flushed_points[i].timestamp_ms == static_cast<int64_t>(i));
    }
    AssertOk(engine->Close());
}

void TestCowSameShardSharedSeries(const std::filesystem::path& path) {
    const Options options{.shard_count = 1,
                          .memtable_max_points = 100000,
                          .enable_background_compaction = false,
                          .enable_background_gc = false};
    constexpr uint64_t series_a = 77;
    constexpr uint64_t series_b = 78;
    constexpr size_t initial = 100;
    Status status;
    auto engine = Engine::Open(path, options, &status);
    AssertOk(status);
    std::vector<DataPoint> first;
    for (size_t i = 0; i < initial; ++i) {
        first.push_back({series_a, static_cast<int64_t>(i), static_cast<double>(i)});
        first.push_back({series_b, static_cast<int64_t>(i), 1000.0 + i});
    }
    AssertOk(engine->WriteBatch(first));
    const auto before_a = testing::InspectSeriesBuffer(*engine, series_a);
    const auto before_b = testing::InspectSeriesBuffer(*engine, series_b);
    CHECK(before_a.point_count == initial && before_b.point_count == initial);
    CHECK(before_a.capacity.has_value() && before_b.capacity.has_value());
    CHECK(*before_a.capacity >= initial && *before_b.capacity >= initial);

    // A cursor 只选 A，但 view pin 整个 shard；B cursor 独立捕获同一旧 shard。
    std::unique_ptr<Cursor> old_a;
    std::unique_ptr<Cursor> old_b;
    AssertOk(engine->Scan({series_a}, 0, 100000, &old_a));
    AssertOk(engine->Scan({series_b}, 0, 100000, &old_b));
    const auto grow = [&](uint64_t series, size_t target, double base) {
        std::vector<DataPoint> batch;
        for (size_t i = initial; i < target; ++i) {
            batch.push_back({series, static_cast<int64_t>(i), base + i});
        }
        AssertOk(engine->WriteBatch(batch));
    };
    const size_t target_a = *before_a.capacity + 1;
    grow(series_a, target_a, 0.0);
    const auto after_a = testing::InspectSeriesBuffer(*engine, series_a);
    CHECK(after_a.point_count == target_a && after_a.capacity.has_value());
    CHECK(*after_a.capacity > *before_a.capacity);

    // 写 A 已克隆外层 shard。期间不捕获新 view，live shard 独占而 B still shared。
    const auto untouched_b = testing::InspectSeriesBuffer(*engine, series_b);
    CHECK(untouched_b.point_count == before_b.point_count);
    CHECK(untouched_b.capacity == before_b.capacity);
    const size_t target_b = *untouched_b.capacity + 1;
    grow(series_b, target_b, 1000.0);
    const auto after_b = testing::InspectSeriesBuffer(*engine, series_b);
    CHECK(after_b.point_count == target_b && after_b.capacity.has_value());
    CHECK(after_b.point_count > *untouched_b.capacity);
    CHECK(*after_b.capacity > *untouched_b.capacity);

    // 后续前缀删除也不能改变两个旧 cursor 的点和值。
    constexpr size_t cutoff = 50;
    AssertOk(engine->ExpireBefore(cutoff));
    const auto check = [&](Cursor* cursor, uint64_t series, size_t lo,
                           size_t hi, double base) {
        const auto points = Read(cursor);
        CHECK(points.size() == hi - lo);
        for (size_t i = 0; i < points.size(); ++i) {
            CHECK(points[i].series_id == series);
            CHECK(points[i].timestamp_ms == static_cast<int64_t>(lo + i));
            CHECK(points[i].value == base + lo + i);
        }
        CHECK(cursor->closed());
    };
    check(old_a.get(), series_a, 0, initial, 0.0);
    check(old_b.get(), series_b, 0, initial, 1000.0);
    EngineStats stats;
    AssertOk(engine->GetStats(&stats));
    CHECK(stats.active_read_views == 0);
    CHECK(stats.mutable_points == target_a + target_b - 2 * cutoff);
    std::unique_ptr<Cursor> fresh;
    AssertOk(engine->Scan({series_a}, 0, 100000, &fresh));
    check(fresh.get(), series_a, cutoff, target_a, 0.0);
    AssertOk(engine->Scan({series_b}, 0, 100000, &fresh));
    check(fresh.get(), series_b, cutoff, target_b, 1000.0);
    AssertOk(engine->Close());
}

// A range wider than INT64_MAX: timestamp - start reaches 2^64 - 2, so the
// bucket math has to be done in 128 bits. Without that this is signed overflow
// (UB), which the UBSan build reports.
void TestAggregateExtremeRange(const std::filesystem::path& path) {
    constexpr int64_t kMin = std::numeric_limits<int64_t>::min();
    constexpr int64_t kMax = std::numeric_limits<int64_t>::max();
    Status status;
    auto engine = Engine::Open(path, Options{4, 100000}, &status);
    AssertOk(status);
    AssertOk(engine->WriteBatch({{1, kMin, 1.0},
                                 {1, -1, 2.0},
                                 {1, 0, 3.0},
                                 {1, kMax - 1, 4.0}}));

    // One full-width window: [INT64_MIN, -1), [-1, INT64_MAX-1),
    // [INT64_MAX-1, INT64_MAX).
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Aggregate({1}, kMin, kMax, kMax, AggregateOp::Count, &cursor));
    std::vector<AggregateResult> buckets;
    while (auto bucket = cursor->NextAggregate()) buckets.push_back(*bucket);
    CHECK(buckets.size() == 3);
    CHECK(buckets[0].bucket_start == kMin && buckets[0].count == 1);
    CHECK(buckets[1].bucket_start == -1 && buckets[1].count == 2);
    CHECK(buckets[2].bucket_start == kMax - 1 && buckets[2].count == 1);
    CHECK(buckets[2].bucket_end == kMax);

    // window == 1 over the same span: one bucket per point, sums preserved.
    std::unique_ptr<Cursor> per_point;
    AssertOk(engine->Aggregate({1}, kMin, kMax, 1, AggregateOp::Sum, &per_point));
    std::vector<AggregateResult> points;
    while (auto bucket = per_point->NextAggregate()) points.push_back(*bucket);
    CHECK(points.size() == 4);
    CHECK(points[0].bucket_start == kMin && points[0].result == 1.0);
    CHECK(points[1].bucket_start == -1 && points[1].result == 2.0);
    CHECK(points[2].bucket_start == 0 && points[2].result == 3.0);
    CHECK(points[3].bucket_start == kMax - 1 && points[3].result == 4.0);

    // Parameter validation must reject these instead of computing a bucket.
    std::unique_ptr<Cursor> invalid;
    CHECK(!engine->Aggregate({1}, 0, 1000, 0, AggregateOp::Sum, &invalid).ok);
    CHECK(!engine->Aggregate({1}, 0, 1000, -1, AggregateOp::Sum, &invalid).ok);
    CHECK(!engine->Aggregate({1}, 1000, 0, 10, AggregateOp::Sum, &invalid).ok);
    AssertOk(engine->Close());
}

// Zigzag boundaries through both on-disk paths: the first reopen replays the WAL,
// the second reads an SSTable. INT64_MAX itself is not covered because a
// half-open [start, end) cannot include it; INT64_MAX - 1 exercises the same
// 10-byte varint branch.
void TestZigzagBoundaries(const std::filesystem::path& path) {
    constexpr int64_t kMin = std::numeric_limits<int64_t>::min();
    constexpr int64_t kMax = std::numeric_limits<int64_t>::max();
    const std::vector<int64_t> values{kMin, kMin + 1, -1, 0, 1, kMax - 1};

    const pid_t pid = fork();
    CHECK(pid >= 0);
    if (pid == 0) {
        Status child_status;
        auto engine = Engine::Open(path, Options{4, 100000}, &child_status);
        if (!child_status.ok) _exit(2);
        for (size_t i = 0; i < values.size(); ++i) {
            if (!engine->Put({1000 + i, values[i],
                              static_cast<double>(i)})
                     .ok) {
                _exit(3);
            }
        }
        _exit(0);  // crash without Close: the records stay in the WAL
    }
    int child_status = 0;
    waitpid(pid, &child_status, 0);
    CHECK(WIFEXITED(child_status) && WEXITSTATUS(child_status) == 0);

    const auto check_all = [&](Engine* engine) {
        for (size_t i = 0; i < values.size(); ++i) {
            std::unique_ptr<Cursor> cursor;
            AssertOk(engine->Scan({1000 + i}, kMin, kMax, &cursor));
            const auto points = Read(cursor.get());
            CHECK(points.size() == 1);
            CHECK(points[0].timestamp_ms == values[i]);
            CHECK(points[0].value == static_cast<double>(i));
        }
    };

    Status status;
    auto replayed = Engine::Open(path, Options{4, 100000}, &status);
    AssertOk(status);
    EngineStats replayed_stats;
    AssertOk(replayed->GetStats(&replayed_stats));
    CHECK(replayed_stats.wal_bytes_replayed > 0);  // the values came from the WAL
    check_all(replayed.get());
    AssertOk(replayed->Flush());
    AssertOk(replayed->Close());

    auto reopened = Engine::Open(path, Options{4, 100000}, &status);
    AssertOk(status);
    check_all(reopened.get());  // now from an SSTable
    AssertOk(reopened->Close());
}

void TestAggregateInvalidOutput(const std::filesystem::path& path) {
    Status status;
    auto engine = Engine::Open(path, {}, &status);
    AssertOk(status);
    CHECK(!engine->Aggregate({1}, 0, 10, 1, AggregateOp::Count, nullptr).ok);
    std::unique_ptr<Cursor> cursor;
    CHECK(!engine->Aggregate({1}, 0, 10, 1, static_cast<AggregateOp>(99), &cursor).ok);
    AssertOk(engine->Close());
}

void TestQueryBufferBoundaries(const std::filesystem::path& path) {
    constexpr uint64_t series = std::numeric_limits<uint64_t>::max();
    constexpr int64_t first = std::numeric_limits<int64_t>::min();
    constexpr size_t count = 4096;
    const double values[] = {-0.0, std::numeric_limits<double>::max(),
                             std::numeric_limits<double>::lowest(),
                             std::numeric_limits<double>::denorm_min(), 0.125};
    std::vector<DataPoint> batch;
    bool crossed[3] = {};
    size_t offset = 0;
    for (size_t i = 0; i < count; ++i) {
        batch.push_back({series, first + static_cast<int64_t>(i), values[i % 5]});
        // 每条记录：10 字节 series、10 字节负时间戳、double、1 字节版本。
        const size_t lengths[] = {10, 10, sizeof(double), 1};
        for (size_t field = 0; field < 4; ++field) {
            if (field < 3 && offset / 8192 != (offset + lengths[field] - 1) / 8192) {
                crossed[field] = true;
            }
            offset += lengths[field];
        }
    }
    CHECK(crossed[0] && crossed[1] && crossed[2]);
    Status status;
    const Options options{.shard_count = 1, .memtable_max_points = 100000,
                          .enable_background_compaction = false,
                          .enable_background_gc = false};
    auto engine = Engine::Open(path, options, &status);
    AssertOk(status);
    AssertOk(engine->WriteBatch(batch));
    AssertOk(engine->Flush());
    const auto check = [&](Engine* db, size_t lo, size_t hi) {
        std::unique_ptr<Cursor> cursor;
        AssertOk(db->Scan({series}, first + static_cast<int64_t>(lo),
                          first + static_cast<int64_t>(hi), &cursor));
        const auto points = Read(cursor.get());
        CHECK(points.size() == hi - lo);
        for (size_t i = 0; i < points.size(); ++i) {
            CHECK(points[i].series_id == series);
            CHECK(points[i].timestamp_ms == batch[lo + i].timestamp_ms);
            CHECK(std::memcmp(&points[i].value, &batch[lo + i].value,
                              sizeof(double)) == 0);
        }
    };
    check(engine.get(), 0, count);
    check(engine.get(), 127, 513);
    check(engine.get(), 511, 1537);
    check(engine.get(), 2049, 2050);
    check(engine.get(), count - 1, count);
    AssertOk(engine->Close());
    engine.reset();
    engine = Engine::Open(path, options, &status);
    AssertOk(status);
    check(engine.get(), 0, count);
    check(engine.get(), 255, 1025);
    AssertOk(engine->Close());
}

void TestQueryLegacySequential(const std::filesystem::path& path) {
    constexpr uint64_t count = 800;
    const Options options{.shard_count = 1, .memtable_max_points = 100000,
                          .enable_background_compaction = false,
                          .enable_background_gc = false};
    std::vector<DataPoint> batch;
    std::string payload;
    for (uint64_t series = 1; series <= 2; ++series) {
        for (uint64_t i = 0; i < count; ++i) {
            const int64_t timestamp = static_cast<int64_t>(i) - 400;
            const double value = static_cast<double>(series * 1000 + i) + 0.25;
            batch.push_back({series, timestamp, value});
            AppendRaw(&payload, series);
            AppendRaw(&payload, timestamp);
            AppendRaw(&payload, value);
            AppendRaw(&payload, CommitVersion{1});
        }
    }
    Status status;
    auto engine = Engine::Open(path, options, &status);
    AssertOk(status);
    AssertOk(engine->WriteBatch(batch));
    AssertOk(engine->Close());
    engine.reset();
    std::filesystem::path sstable;
    for (const auto& entry : std::filesystem::directory_iterator(path / "sst")) {
        if (entry.path().extension() == ".sst") {
            CHECK(sstable.empty());
            sstable = entry.path();
        }
    }
    CHECK(!sstable.empty());
    std::string bytes;
    AppendRaw(&bytes, kWalMagicBytes);
    AppendRaw(&bytes, count * 2);
    bytes += payload;
    AppendRaw(&bytes, Fnv1a(payload));
    {
        std::ofstream out(sstable, std::ios::binary | std::ios::trunc);
        CHECK(out);
        out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        CHECK(out);
    }
    engine = Engine::Open(path, options, &status);
    AssertOk(status);
    for (const auto range : {std::pair<int64_t, int64_t>{-400, 400},
                             {-117, 123}, {398, 400}}) {
        std::unique_ptr<Cursor> cursor;
        AssertOk(engine->Scan({2, 1}, range.first, range.second, &cursor));
        const auto points = Read(cursor.get());
        CHECK(points.size() == static_cast<size_t>(range.second - range.first) * 2);
        for (size_t i = 0; i < points.size(); ++i) {
            const uint64_t series = i % 2 + 1;
            const int64_t timestamp = range.first + static_cast<int64_t>(i / 2);
            CHECK(points[i].series_id == series);
            CHECK(points[i].timestamp_ms == timestamp);
            CHECK(points[i].value == static_cast<double>(series * 1000 + timestamp + 400) + 0.25);
        }
    }
    AssertOk(engine->Close());
}

void TestCursorReleasesCaptures() {
    for (int mode = 0; mode < 3; ++mode) {
        int callbacks = 0;
        auto points = std::make_shared<std::vector<DataPoint>>(1, DataPoint{1, 2, 3.0});
        auto callback_resource = std::make_shared<int>(42);
        std::weak_ptr<std::vector<DataPoint>> weak_points = points;
        std::weak_ptr<int> weak_callback = callback_resource;
        {
            Cursor cursor(
                [points, mode, first = true]() mutable -> std::optional<DataPoint> {
                    if (mode == 2) throw std::runtime_error("测试生成器错误");
                    if (!first) return std::nullopt;
                    first = false;
                    return points->front();
                },
                [callback_resource, &callbacks] { ++callbacks; });
            points.reset();
            callback_resource.reset();
            CHECK(!weak_points.expired() && !weak_callback.expired());
            if (mode == 0) {
                cursor.Close();
            } else if (mode == 1) {
                CHECK(cursor.NextPoint().has_value());
                CHECK(!cursor.NextPoint().has_value());
            } else {
                bool failed = false;
                try { cursor.NextPoint(); }
                catch (const std::runtime_error&) { failed = true; }
                CHECK(failed);
            }
            CHECK(cursor.closed());
            CHECK(weak_points.expired() && weak_callback.expired());
            CHECK(callbacks == 1);
            cursor.Close();
            CHECK(!cursor.NextPoint().has_value());
            CHECK(callbacks == 1);
        }
        CHECK(callbacks == 1);
    }
    auto resource = std::make_shared<int>(1);
    std::weak_ptr<int> weak = resource;
    Cursor no_callback([resource]() -> std::optional<DataPoint> { return std::nullopt; });
    resource.reset();
    CHECK(!no_callback.NextPoint().has_value());
    CHECK(weak.expired());
}

void TestQueryReleasesDescriptors(const std::filesystem::path& path) {
#if defined(__linux__)
    const auto descriptors = [&] {
        size_t count = 0;
        for (const auto& entry : std::filesystem::directory_iterator("/proc/self/fd")) {
            std::error_code ec;
            const auto target = std::filesystem::read_symlink(entry.path(), ec);
            if (!ec && target.string().find(path.string()) == 0 &&
                target.extension() == ".sst") ++count;
        }
        return count;
    };
    Status status;
    auto engine = Engine::Open(path, Options{.shard_count = 1,
        .memtable_max_points = 100000, .enable_background_compaction = false,
        .enable_background_gc = false}, &status);
    AssertOk(status);
    AssertOk(engine->Put({1, 100, 1.0}));
    AssertOk(engine->Flush());
    AssertOk(engine->Put({1, 200, 2.0}));
    AssertOk(engine->Flush());
    AssertOk(engine->Put({1, 300, 3.0}));
    CHECK(descriptors() == 0);
    for (bool eof : {false, true}) {
        std::unique_ptr<Cursor> cursor;
        AssertOk(engine->Scan({1}, 0, 400, &cursor));
        CHECK(descriptors() == 2);
        if (eof) CHECK(Read(cursor.get()).size() == 3);
        else cursor->Close();
        CHECK(cursor != nullptr && cursor->closed());
        CHECK(descriptors() == 0);
        EngineStats stats;
        AssertOk(engine->GetStats(&stats));
        CHECK(stats.active_read_views == 0);
    }
    AssertOk(engine->Close());
#else
    (void)path;
#endif
}

void TestQueryTruncatedReadIsError(const std::filesystem::path& path) {
    Status status;
    auto engine = Engine::Open(path, Options{.shard_count = 1,
        .memtable_max_points = 100000, .enable_background_compaction = false,
        .enable_background_gc = false}, &status);
    AssertOk(status);
    constexpr uint64_t series = std::numeric_limits<uint64_t>::max();
    constexpr int64_t first = std::numeric_limits<int64_t>::min();
    std::vector<DataPoint> batch;
    for (int i = 0; i < 2048; ++i) batch.push_back({series, first + i, i + 0.25});
    AssertOk(engine->WriteBatch(batch));
    AssertOk(engine->Flush());
    std::filesystem::path sstable;
    for (const auto& entry : std::filesystem::directory_iterator(path / "sst")) {
        if (entry.path().extension() == ".sst") sstable = entry.path();
    }
    CHECK(!sstable.empty());
    // 截断发生于 metadata 校验后：完整第一页仍可被窄查询读取。
    std::filesystem::resize_file(sstable, 12 + 128 * 29);
    std::unique_ptr<Cursor> narrow;
    AssertOk(engine->Scan({series}, first + 127, first + 128, &narrow));
    const auto point = narrow->NextPoint();
    CHECK(point && point->timestamp_ms == first + 127);
    CHECK(!narrow->NextPoint().has_value());
    // 初次 pread 返回正数短读，后续 refill 的 0 必须报截断，不能正常 EOF。
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({series}, first, first + 2048, &cursor));
    size_t decoded = 0;
    bool failed = false;
    try { while (cursor->NextPoint()) ++decoded; }
    catch (const std::runtime_error&) { failed = true; }
    CHECK(failed && decoded < 2048 && cursor->closed());
    EngineStats stats;
    AssertOk(engine->GetStats(&stats));
    CHECK(stats.active_read_views == 0);
    // 多次首条解码失败不得泄漏 gate；次数超过并发上限。
    for (int i = 0; i < 32; ++i) {
        std::unique_ptr<Cursor> broken;
        const auto result = engine->Scan({series}, first + 1000, first + 1001, &broken);
        CHECK(!result.ok && result.message.find("truncated") != std::string::npos);
        CHECK(!broken);
        AssertOk(engine->GetStats(&stats));
        CHECK(stats.active_read_views == 0);
    }
    const auto saved = sstable.string() + ".saved";
    std::filesystem::rename(sstable, saved);
    std::unique_ptr<Cursor> missing;
    const auto open_error = engine->Scan({series}, first, first + 2048, &missing);
    CHECK(!open_error.ok && open_error.message.find("open SST query") != std::string::npos);
    CHECK(!missing);
    // open 目录成功，但 pread 必须报告 EISDIR 而不是空结果。
    std::filesystem::create_directory(sstable);
    std::unique_ptr<Cursor> unreadable;
    const auto read_error = engine->Scan({series}, first, first + 2048, &unreadable);
    CHECK(!read_error.ok && read_error.message.find("pread SST query") != std::string::npos);
    CHECK(!unreadable);
    std::filesystem::remove(sstable);
    std::filesystem::rename(saved, sstable);
    AssertOk(engine->GetStats(&stats));
    CHECK(stats.active_read_views == 0);
    AssertOk(engine->Close());
}

void TestAggregateTruncatedReadIsError(const std::filesystem::path& path) {
    Status status;
    auto engine = Engine::Open(path, Options{.shard_count = 1,
        .memtable_max_points = 100000, .enable_background_compaction = false,
        .enable_background_gc = false}, &status);
    AssertOk(status);
    constexpr uint64_t series = std::numeric_limits<uint64_t>::max();
    constexpr int64_t first = std::numeric_limits<int64_t>::min();
    constexpr int64_t count = 2048;
    std::vector<DataPoint> batch;
    for (int64_t i = 0; i < count; ++i) batch.push_back({series, first + i, i + 0.25});
    AssertOk(engine->WriteBatch(batch));
    AssertOk(engine->Flush());
    std::filesystem::path sstable;
    for (const auto& entry : std::filesystem::directory_iterator(path / "sst")) {
        if (entry.path().extension() == ".sst") {
            CHECK(sstable.empty());
            sstable = entry.path();
        }
    }
    CHECK(!sstable.empty());
    // 保留头部和完整 8 KiB 缓冲，首条解码成功，后续 refill 才遇到截断。
    constexpr uint64_t retained = sizeof(uint32_t) + sizeof(uint64_t) + 8 * 1024;
    CHECK(std::filesystem::file_size(sstable) > retained);
    std::filesystem::resize_file(sstable, retained);
    std::unique_ptr<Cursor> scan;
    AssertOk(engine->Scan({series}, first, first + count, &scan));
    const auto point = scan->NextPoint();
    CHECK(point && point->timestamp_ms == first && point->value == 0.25);
    scan->Close();
    EngineStats stats;
    AssertOk(engine->GetStats(&stats));
    CHECK(stats.active_read_views == 0);
    for (const auto operation : {AggregateOp::Count, AggregateOp::Sum}) {
        for (const bool reuse_output : {false, true}) {
            std::unique_ptr<Cursor> cursor;
            if (reuse_output) {
                cursor = std::make_unique<Cursor>(std::vector<AggregateResult>{
                    {series, first, first + 1, -1.0, 1}});
            }
            const auto* original = cursor.get();
            Status result;
            bool threw = false;
            try {
                result = engine->Aggregate({series}, first, first + count, 128,
                                           operation, &cursor);
            } catch (...) {
                threw = true;
            }
            CHECK(!threw);
            CHECK(!result.ok && result.message.find("truncated") != std::string::npos);
            // 失败不发布已累积的桶；空输出仍为空，已有游标及其结果保持原样。
            CHECK(cursor.get() == original);
            if (cursor) {
                const auto previous = cursor->NextAggregate();
                CHECK(previous && previous->series_id == series &&
                      previous->bucket_start == first && previous->bucket_end == first + 1 &&
                      previous->result == -1.0 && previous->count == 1);
                CHECK(!cursor->NextAggregate().has_value());
            }
            AssertOk(engine->GetStats(&stats));
            CHECK(stats.active_read_views == 0);
        }
    }
    AssertOk(engine->Close());
}

void TestLatencySampling() {
    std::vector<double> values{1, 2, 3, 4};
    CHECK(bench::Percentile(&values, 0.50) == 2);
    CHECK(bench::Percentile(&values, 0.95) == 4);
    std::vector<double> samples;
    bench::LatencyReservoir reservoir;
    for (int i = 0; i < 100000; ++i) reservoir.Add(samples, i < 50000 ? 1.0 : 100.0);
    CHECK(samples.size() == 50000);
    CHECK(reservoir.seen == 100000);
    const auto late = std::count(samples.begin(), samples.end(), 100.0);
    CHECK(late > 20000 && late < 30000);
    CHECK(bench::Percentile(&samples, 0.95) == 100.0);
}

std::set<std::string> SstFilesOnDisk(const std::filesystem::path& db) {
    std::set<std::string> names;
    for (const auto& entry : std::filesystem::directory_iterator(db / "sst")) {
        if (entry.is_regular_file() && entry.path().extension() == ".sst") {
            names.insert(entry.path().filename().string());
        }
    }
    return names;
}

std::set<std::string> SstFilesInManifest(const std::filesystem::path& db) {
    std::ifstream in(db / "MANIFEST");
    CHECK(in.is_open());
    std::set<std::string> names;
    std::string line;
    std::getline(in, line);  // cutoff / next_version / next_file_number
    while (std::getline(in, line)) {
        if (line.rfind("WAL_", 0) == 0 || line.rfind("COVERED ", 0) == 0 ||
            line.rfind("CHECKSUM ", 0) == 0) {
            continue;
        }
        std::istringstream row(line);
        size_t index = 0;
        std::string file;
        CHECK(static_cast<bool>(row >> index >> file));
        names.insert(std::filesystem::path(file).filename().string());
    }
    return names;
}

// 查询持有旧文件时合并：被替换的输入当时无法 GC，原实现 Close 不做 GC、
// 待删除列表只在内存中、Open 只清 *.tmp，这些文件会永久留在磁盘上。
void TestOrphanSstablesReclaimed(const std::filesystem::path& path) {
    const Options options{.shard_count = 1,
                          .enable_background_compaction = false,
                          .enable_background_gc = false};
    Status status;
    auto engine = Engine::Open(path, options, &status);
    AssertOk(status);
    for (int64_t file = 0; file < 3; ++file) {
        std::vector<DataPoint> batch;
        for (int64_t i = 0; i < 100; ++i) {
            const int64_t timestamp = file * 1000 + i;
            batch.push_back({1, timestamp, static_cast<double>(timestamp)});
        }
        AssertOk(engine->WriteBatch(batch));
        AssertOk(engine->Flush());
    }
    std::unique_ptr<Cursor> held;
    AssertOk(engine->Scan({1}, 0, 100000, &held));
    AssertOk(engine->Compact());
    AssertOk(engine->GarbageCollect());
    CHECK(SstFilesOnDisk(path).size() == 4);
    CHECK(SstFilesInManifest(path).size() == 1);
    CHECK(Read(held.get()).size() == 300);
    held.reset();
    AssertOk(engine->Close());
    engine.reset();
    // Close 在最终 Flush 后回收已无引用的旧文件。
    CHECK(SstFilesOnDisk(path) == SstFilesInManifest(path));

    // 模拟崩溃遗留：已改名就位却未登记进 Manifest 的输出；非 .sst 文件不动。
    const auto live = *SstFilesInManifest(path).begin();
    std::filesystem::copy_file(path / "sst" / live, path / "sst" / "shard-00-999.sst");
    std::filesystem::copy_file(path / "sst" / live, path / "sst" / "keep.backup");
    engine = Engine::Open(path, options, &status);
    AssertOk(status);
    CHECK(SstFilesOnDisk(path) == SstFilesInManifest(path));
    CHECK(std::filesystem::exists(path / "sst" / "keep.backup"));
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({1}, 0, 100000, &cursor));
    const auto points = Read(cursor.get());
    CHECK(points.size() == 300);
    for (size_t i = 0; i < points.size(); ++i) {
        const int64_t expected = static_cast<int64_t>(i / 100) * 1000 +
                                 static_cast<int64_t>(i % 100);
        CHECK(points[i].timestamp_ms == expected);
        CHECK(points[i].value == static_cast<double>(expected));
    }
    cursor.reset();
    EngineStats stats;
    AssertOk(engine->GetStats(&stats));
    CHECK(stats.sstable_count == stats.live_sstable_count);
    AssertOk(engine->Close());
}

// CURRENT 存在而 MANIFEST 丢失只能是外部损坏：不能按空库打开，更不能把 SST
// 当作孤儿删掉。
void TestMissingManifestRejected(const std::filesystem::path& path) {
    const Options options{.shard_count = 1,
                          .enable_background_compaction = false,
                          .enable_background_gc = false};
    Status status;
    auto engine = Engine::Open(path, options, &status);
    AssertOk(status);
    AssertOk(engine->Put({1, 10, 1.0}));
    AssertOk(engine->Flush());
    AssertOk(engine->Close());
    engine.reset();
    const auto before = SstFilesOnDisk(path);
    CHECK(!before.empty());
    CHECK(std::filesystem::exists(path / "CURRENT"));
    CHECK(std::filesystem::remove(path / "MANIFEST"));
    engine = Engine::Open(path, options, &status);
    CHECK(engine == nullptr && !status.ok);
    CHECK(status.message.find("Manifest missing") != std::string::npos);
    CHECK(SstFilesOnDisk(path) == before);
}

// 过期不仅回收数据，也要回收只增不减的写入跟踪表；严格递增与 cutoff 校验不变。
void TestExpirePrunesWriteTracking(const std::filesystem::path& path) {
    const Options options{.shard_count = 2,
                          .enable_background_compaction = false,
                          .enable_background_gc = false};
    Status status;
    auto engine = Engine::Open(path, options, &status);
    AssertOk(status);
    std::vector<DataPoint> batch;
    for (uint64_t series = 0; series < 1000; ++series) {
        batch.push_back({series, static_cast<int64_t>(series), 1.0});
    }
    AssertOk(engine->WriteBatch(batch));
    auto tracking = testing::InspectWriteTracking(*engine);
    CHECK(tracking.last_timestamp_entries == 1000);
    CHECK(tracking.hot_series_entries == 1000);
    AssertOk(engine->ExpireBefore(500));
    tracking = testing::InspectWriteTracking(*engine);
    CHECK(tracking.last_timestamp_entries == 500);
    CHECK(tracking.hot_series_entries == 500);
    AssertOk(engine->Put({3, 500, 2.0}));     // 已过期 series 可从 cutoff 继续写
    CHECK(!engine->Put({4, 499, 2.0}).ok);    // cutoff 之前仍拒绝
    CHECK(!engine->Put({700, 700, 2.0}).ok);  // 存活 series 仍要求严格递增
    AssertOk(engine->Put({700, 701, 2.0}));

    // SST 仍覆盖到 999，因而保留；重启时从 SST 恢复的跟踪项也要按 cutoff 裁剪。
    AssertOk(engine->Flush());
    AssertOk(engine->ExpireBefore(800));
    tracking = testing::InspectWriteTracking(*engine);
    CHECK(tracking.last_timestamp_entries == 200);
    CHECK(tracking.hot_series_entries == 0);
    AssertOk(engine->Close());
    engine.reset();
    engine = Engine::Open(path, options, &status);
    AssertOk(status);
    tracking = testing::InspectWriteTracking(*engine);
    CHECK(tracking.last_timestamp_entries == 200);
    AssertOk(engine->Put({600, 800, 3.0}));
    CHECK(!engine->Put({900, 900, 3.0}).ok);
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({600, 900}, 0, 2000, &cursor));
    const auto points = Read(cursor.get());
    CHECK(points.size() == 2);
    CHECK(points[0].series_id == 600 && points[0].timestamp_ms == 800);
    CHECK(points[1].series_id == 900 && points[1].timestamp_ms == 900);
    cursor.reset();
    AssertOk(engine->Close());
}

// 统计采样与 Flush / Compaction / GC 并发：目录项被并发改名删除时不能抛出，
// 文件大小读取失败也不能把 (uintmax_t)-1 累加进 sstable_bytes。
void TestStatsDuringChurn(const std::filesystem::path& path) {
    Options options;
    options.shard_count = 2;
    options.compaction_sstable_threshold = 2;
    Status status;
    auto engine = Engine::Open(path, options, &status);
    AssertOk(status);
    std::atomic<bool> stop{false};
    std::atomic<bool> bad{false};
    std::atomic<size_t> calls{0};
    std::thread sampler([&] {
        while (!stop.load()) {
            EngineStats stats;
            const auto result = engine->GetStats(&stats);
            if (!result.ok || stats.sstable_bytes > (uint64_t{1} << 40)) bad = true;
            ++calls;
        }
    });
    for (int64_t round = 0; round < 40; ++round) {
        std::vector<DataPoint> points;
        for (int64_t i = 0; i < 200; ++i) {
            points.push_back({static_cast<uint64_t>(i % 4), round * 1000 + i, 1.0});
        }
        AssertOk(engine->WriteBatch(points));
        AssertOk(engine->Flush());
    }
    AssertOk(engine->Compact());
    stop = true;
    sampler.join();
    CHECK(!bad.load() && calls.load() > 0);
    AssertOk(engine->Close());
}

// 空闲引擎不应周期性唤醒：原写协调线程每 200 µs 超时一次并争抢全局锁。
void TestIdleCoordinatorSleeps(const std::filesystem::path& path) {
    Status status;
    auto engine = Engine::Open(path, Options{}, &status);
    AssertOk(status);
    AssertOk(engine->Put({1, 1, 1.0}));  // 确认协调线程已就绪并完成过一组
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    rusage before{};
    rusage after{};
    CHECK(::getrusage(RUSAGE_SELF, &before) == 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    CHECK(::getrusage(RUSAGE_SELF, &after) == 0);
    const long switches = after.ru_nvcsw - before.ru_nvcsw;
    if (switches >= 50) std::cerr << "idle voluntary switches=" << switches << '\n';
    CHECK(switches < 50);
    // 唤醒仍然及时：空闲之后的单次写入立即完成。
    const auto started = std::chrono::steady_clock::now();
    AssertOk(engine->Put({1, 2, 2.0}));
    CHECK(std::chrono::steady_clock::now() - started < std::chrono::seconds(2));
    AssertOk(engine->Close());
}

}  // namespace

int main(int argc, char** argv) {
    const auto root = TestPath();
    struct Case {
        const char* name;
        std::function<void()> run;
    };
    const std::vector<Case> cases = {
        {"latency_sampling", [&] { TestLatencySampling(); }},
        {"aggregate_invalid_output", [&] { TestAggregateInvalidOutput(root / "aggregate_invalid"); }},
        {"timestamp_encoding", [&] { TestTimestampEncodingRoundTrip(); }},
        {"write_scan_atomic", [&] { TestWriteScanAndAtomicValidation(root / "atomic"); }},
        {"write_validation_contract", [&] { RunWithTimeout([&] { TestWriteValidationContract(root / "write_validation"); }); }},
        {"readview_expiration", [&] { TestReadViewAndExpiration(root / "views"); }},
        {"recovery", [&] { TestRecovery(root / "recovery"); }},
        {"concurrent_group_commit", [&] { TestConcurrentGroupCommit(root / "group_commit"); }},
        {"queue_backpressure", [&] { RunWithTimeout([&] { TestQueueBackpressure(root / "queue"); }); }},
        {"zero_queue_capacity", [&] { TestZeroQueueCapacityRejected(root / "zero_queue_capacity"); }},
        {"memtable_backpressure", [&] { RunWithTimeout([&] { TestMemtableBackpressureAutoFlush(root / "memtable_backpressure"); }); }},
        {"hot_series_backpressure", [&] { RunWithTimeout([&] { TestHotSeriesBackpressureAutoFlush(root / "hot_series_backpressure"); }); }},
        {"oversized_hot_series_batch", [&] { RunWithTimeout([&] { TestOversizedHotSeriesBatchAtomic(root / "oversized_hot_series_batch"); }); }},
        {"oversized_memory_batch", [&] { RunWithTimeout([&] { TestOversizedMemoryBatchRejected(root / "oversized_memory_batch"); }); }},
        {"current_rss_stats", [&] { TestCurrentRssStats(root / "current_rss_stats"); }},
        {"aggregate_count_exact_type", [&] { TestAggregateCountExactType(root / "aggregate_count_exact_type"); }},
        {"expiration_version_accounting", [&] { RunWithTimeout([&] { TestExpirationVersionIdempotencyAndAccounting(root / "expiration_version_accounting"); }); }},
        {"manifest_failure_sticky", [&] { RunWithTimeout([&] { TestManifestFailureStickyAndRecoverable(root / "manifest_failure_sticky"); }); }},
        {"background_flush_failure_wakeup", [&] { RunWithTimeout([&] { TestBackgroundFlushFailureWakesWriter(root / "background_flush_failure_wakeup"); }); }},
        {"close_queue_backpressure_wakeup", [&] { RunWithTimeout([&] { TestCloseWakesQueueBackpressureWriter(root / "close_queue_backpressure_wakeup"); }); }},
        {"irreducible_sstable_pressure", [&] { RunWithTimeout([&] { TestIrreducibleSstablePressureMakesProgress(root / "irreducible_sstable_pressure"); }); }},
        {"sstable_pressure_high_threshold", [&] { RunWithTimeout([&] { TestSstablePressureForcesHighThresholdCompaction(root / "sstable_pressure_high_threshold"); }); }},
        {"negative_windows", [&] { TestNegativeWindowsAndDuplicateSeries(root / "negative"); }},
        {"incremental_flush", [&] { TestMultipleIncrementalFlushes(root / "incremental"); }},
        {"physical_expiration_gc", [&] { TestPhysicalExpirationAndGC(root / "physical_expiration"); }},
        {"large_series_paging", [&] { TestLargeSeriesPaging(root / "large_series_paging"); }},
        {"large_series_streaming", [&] { TestLargeSeriesStreaming(root / "large_series_streaming"); }},        {"flush_clears_memtable", [&] { TestFlushClearsMemtable(root / "flush_clears_memtable"); }},
        {"compaction_merges", [&] { TestCompactionMergesSstables(root / "compaction_merges"); }},
        {"compaction_large_hot_series", [&] { TestCompactionLargeHotSeries(root / "compaction_large_hot_series"); }},
        {"compaction_overlapping_versions", [&] { TestCompactionOverlappingVersions(root / "compaction_overlapping_versions"); }},
        {"compaction_read_failure", [&] { TestCompactionReadFailure(root / "compaction_read_failure"); }},
        {"compaction_drops_expired", [&] { TestCompactionDropsExpired(root / "compaction_drops_expired"); }},
        {"multi_level_compaction", [&] { TestMultiLevelCompaction(root / "multi_level_compaction"); }},
        {"negative_ts_recovery", [&] { TestNegativeTimestampRecovery(root / "negative_ts_recovery"); }},
        {"gc_refcount", [&] { TestGcReclaimsWhileViewActive(root / "gc_refcount"); }},
        {"crash_recovery", [&] { TestCrashRecovery(root / "crash_recovery"); }},
        {"crash_recovery_expire", [&] { TestCrashRecoveryExpire(root / "crash_recovery_expire"); }},
        {"torn_wal_tail", [&] { TestTornWalTailRecovery(root / "torn_wal_tail"); }},
        {"concurrent_compaction_stress", [&] { TestConcurrentCompactionStress(root / "concurrent_compaction_stress"); }},
        {"page_index_selective_scan", [&] { TestPageIndexSelectiveScan(root / "page_index_selective"); }},
        {"expire_reclaims_idle_capacity", [&] { TestExpireReclaimsIdleCapacity(root / "expire_capacity"); }},
        {"page_index_tail_narrow_scan", [&] { TestPageIndexTailNarrowScan(root / "page_tail"); }},
        {"scan_gate_waiter_holds_no_view", [&] { RunWithTimeout([&] { TestScanGateWaiterHoldsNoView(root / "scan_gate_view"); }); }},
        {"scan_gate_close_wakes_waiter", [&] { RunWithTimeout([&] { TestScanGateCloseWakesWaiter(root / "scan_gate_close"); }); }},
        {"scan_gate_close_wakeup_stress", [&] { RunWithTimeout([&] { TestScanGateCloseWakeupStress(root / "scan_gate_stress"); }); }},
        {"sstable_checksum", [&] { TestSstableChecksum(root / "checksum"); }},
        {"cursor_outlives_engine", [&] { TestCursorOutlivesEngine(root / "cursor_lifetime"); }},
        {"sharded_wal", [&] { TestShardedWal(root / "sharded_wal"); }},
        {"wal_separate_dir", [&] { TestWalOnSeparateDir(root / "wal_separate"); }},
        {"vector_append_order", [&] { TestVectorAppendOrder(root / "vector_append"); }},
        {"vector_replay_order", [&] { TestVectorReplayOrder(root / "vector_replay"); }},
        {"vector_replay_dedup_accounting", [&] { RunWithTimeout([&] { TestVectorReplayDedupAndAccounting(root / "vector_replay_dedup"); }); }},
        {"vector_replay_flush_threshold", [&] { RunWithTimeout([&] { TestVectorReplayFlushThreshold(root / "vector_replay_flush"); }); }},
        {"duplicate_version_dedup", [&] { TestDuplicateVersionDedup(root / "version_dedup"); }},
        {"cow_read_view_isolation", [&] { TestCowReadViewIsolation(root / "cow_view"); }},
        {"empty_dataset", [&] { TestEmptyDataset(root / "empty_dataset"); }},
        {"aggregate_buckets", [&] { TestAggregateBuckets(root / "aggregate_buckets"); }},
        {"extreme_timestamps", [&] { TestExtremeTimestamps(root / "extreme_ts"); }},
        {"cow_vector_reallocation", [&] { TestCowVectorReallocation(root / "cow_realloc"); }},
        {"aggregate_extreme_range", [&] { TestAggregateExtremeRange(root / "aggregate_extreme"); }},
        {"zigzag_boundaries", [&] { TestZigzagBoundaries(root / "zigzag"); }},
        {"query_buffer_boundaries", [&] { TestQueryBufferBoundaries(root / "query_buffer"); }},
        {"query_legacy_sequential", [&] { TestQueryLegacySequential(root / "query_legacy"); }},
        {"cursor_releases_captures", [&] { TestCursorReleasesCaptures(); }},
        {"query_releases_descriptors", [&] { TestQueryReleasesDescriptors(root / "query_descriptors"); }},
        {"query_truncated_read_is_error", [&] { RunWithTimeout([&] { TestQueryTruncatedReadIsError(root / "query_truncated"); }); }},
        {"aggregate_truncated_read_is_error", [&] { RunWithTimeout([&] { TestAggregateTruncatedReadIsError(root / "aggregate_truncated"); }); }},
        {"cow_same_shard_shared_series", [&] { TestCowSameShardSharedSeries(root / "cow_same_shard"); }},
        {"orphan_sstables_reclaimed", [&] { RunWithTimeout([&] { TestOrphanSstablesReclaimed(root / "orphan_sst"); }); }},
        {"missing_manifest_rejected", [&] { RunWithTimeout([&] { TestMissingManifestRejected(root / "missing_manifest"); }); }},
        {"expire_prunes_write_tracking", [&] { RunWithTimeout([&] { TestExpirePrunesWriteTracking(root / "expire_tracking"); }); }},
        {"stats_during_churn", [&] { RunWithTimeout([&] { TestStatsDuringChurn(root / "stats_churn"); }); }},
        {"idle_coordinator_sleeps", [&] { RunWithTimeout([&] { TestIdleCoordinatorSleeps(root / "idle_coordinator"); }); }},
    };
    const std::string filter = argc > 1 ? argv[1] : std::string();
    size_t ran = 0;
    size_t failed = 0;
    for (size_t i = 0; i < cases.size(); ++i) {
        if (!filter.empty() && filter != std::to_string(i + 1) &&
            std::string(cases[i].name).find(filter) == std::string::npos) {
            continue;
        }
        std::cout << "[" << (i + 1) << "/" << cases.size() << "] "
                  << cases[i].name << " ... " << std::flush;
        const auto started = std::chrono::steady_clock::now();
        try {
            cases[i].run();
        } catch (const std::exception& error) {
            // CHECK carries the expression, file and line; keep going so one
            // failure does not hide the rest of the suite.
            std::cout << "FAILED\n  " << error.what() << std::endl;
            ++failed;
            ++ran;
            continue;
        }
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - started);
        std::cout << "ok (" << elapsed.count() << " ms)" << std::endl;
        ++ran;
    }
    if (ran == 0) {
        std::cerr << "no test matched '" << filter << "'\n";
        return 2;
    }
    if (failed != 0) {
        std::cerr << failed << " of " << ran << " tests failed\n";
        return 1;
    }
    std::filesystem::remove_all(root);
    std::cout << "all tests passed\n";
    return 0;
}
