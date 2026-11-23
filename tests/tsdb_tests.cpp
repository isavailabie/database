#include "tsdb.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <limits>
#include <string>
#include <thread>
#include <vector>

#include "check.h"

#include <sys/wait.h>
#include <unistd.h>

using namespace tsdb;

namespace {

std::filesystem::path TestPath() {
    const auto path = std::filesystem::temp_directory_path() / "tsdb-course-test";
    std::filesystem::remove_all(path);
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
    CommitVersion version = 0;
    AssertOk(engine->Put({300, 100, 1.0}, &version));
    AssertOk(engine->Put({300, 200, 2.0}, &version));
    auto out_of_order = engine->Put({300, 150, 3.0});
    CHECK(!out_of_order.ok);
    AssertOk(engine->Close());
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

    // The vector layout reports the capacity the snapshot buffer actually has, so
    // growing one past it is what forces the reallocation. The ordered-map layout
    // has no capacity; there the write only has to be large enough to exercise
    // the COW clone.
    const bool measurable = before.capacity.has_value();
    size_t target = static_cast<size_t>(2 * kInitial + 1);
    if (measurable) {
        CHECK(*before.capacity >= before.point_count);
        target = *before.capacity + 1;
    }
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
    if (measurable) {
        // Proof that the write outgrew the buffer the snapshot holds.
        CHECK(after.point_count > *before.capacity);
        CHECK(after.capacity.has_value());
        CHECK(*after.capacity >= after.point_count);
    }

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

}  // namespace

int main(int argc, char** argv) {
    const auto root = TestPath();
    struct Case {
        const char* name;
        std::function<void()> run;
    };
    const std::vector<Case> cases = {
        {"timestamp_encoding", [&] { TestTimestampEncodingRoundTrip(); }},
        {"write_scan_atomic", [&] { TestWriteScanAndAtomicValidation(root / "atomic"); }},
        {"readview_expiration", [&] { TestReadViewAndExpiration(root / "views"); }},
        {"recovery", [&] { TestRecovery(root / "recovery"); }},
        {"concurrent_group_commit", [&] { TestConcurrentGroupCommit(root / "group_commit"); }},
        {"queue_backpressure", [&] { TestQueueBackpressure(root / "queue"); }},
        {"negative_windows", [&] { TestNegativeWindowsAndDuplicateSeries(root / "negative"); }},
        {"incremental_flush", [&] { TestMultipleIncrementalFlushes(root / "incremental"); }},
        {"physical_expiration_gc", [&] { TestPhysicalExpirationAndGC(root / "physical_expiration"); }},
        {"large_series_paging", [&] { TestLargeSeriesPaging(root / "large_series_paging"); }},
        {"large_series_streaming", [&] { TestLargeSeriesStreaming(root / "large_series_streaming"); }},        {"flush_clears_memtable", [&] { TestFlushClearsMemtable(root / "flush_clears_memtable"); }},
        {"compaction_merges", [&] { TestCompactionMergesSstables(root / "compaction_merges"); }},
        {"compaction_drops_expired", [&] { TestCompactionDropsExpired(root / "compaction_drops_expired"); }},
        {"multi_level_compaction", [&] { TestMultiLevelCompaction(root / "multi_level_compaction"); }},
        {"negative_ts_recovery", [&] { TestNegativeTimestampRecovery(root / "negative_ts_recovery"); }},
        {"gc_refcount", [&] { TestGcReclaimsWhileViewActive(root / "gc_refcount"); }},
        {"crash_recovery", [&] { TestCrashRecovery(root / "crash_recovery"); }},
        {"crash_recovery_expire", [&] { TestCrashRecoveryExpire(root / "crash_recovery_expire"); }},
        {"torn_wal_tail", [&] { TestTornWalTailRecovery(root / "torn_wal_tail"); }},
        {"concurrent_compaction_stress", [&] { TestConcurrentCompactionStress(root / "concurrent_compaction_stress"); }},
        {"page_index_selective_scan", [&] { TestPageIndexSelectiveScan(root / "page_index_selective"); }},
        {"sstable_checksum", [&] { TestSstableChecksum(root / "checksum"); }},
        {"cursor_outlives_engine", [&] { TestCursorOutlivesEngine(root / "cursor_lifetime"); }},
        {"sharded_wal", [&] { TestShardedWal(root / "sharded_wal"); }},
        {"wal_separate_dir", [&] { TestWalOnSeparateDir(root / "wal_separate"); }},
        {"vector_append_order", [&] { TestVectorAppendOrder(root / "vector_append"); }},
        {"vector_replay_order", [&] { TestVectorReplayOrder(root / "vector_replay"); }},
        {"duplicate_version_dedup", [&] { TestDuplicateVersionDedup(root / "version_dedup"); }},
        {"cow_read_view_isolation", [&] { TestCowReadViewIsolation(root / "cow_view"); }},
        {"empty_dataset", [&] { TestEmptyDataset(root / "empty_dataset"); }},
        {"aggregate_buckets", [&] { TestAggregateBuckets(root / "aggregate_buckets"); }},
        {"extreme_timestamps", [&] { TestExtremeTimestamps(root / "extreme_ts"); }},
        {"cow_vector_reallocation", [&] { TestCowVectorReallocation(root / "cow_realloc"); }},
        {"aggregate_extreme_range", [&] { TestAggregateExtremeRange(root / "aggregate_extreme"); }},
        {"zigzag_boundaries", [&] { TestZigzagBoundaries(root / "zigzag"); }},
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
    std::cout << "all tests passed\n";
    return 0;
}
