#include "tsdb.h"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <thread>
#include <vector>

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

void TestWriteScanAndAtomicValidation(const std::filesystem::path& path) {
    Status status;
    auto engine = Engine::Open(path, Options{4, 1000}, &status);
    AssertOk(status);
    CommitVersion version = 0;
    AssertOk(engine->WriteBatch({{1, 1000, 1.0}, {2, 500, 2.0}}, &version));
    assert(version == 1);
    auto bad = engine->WriteBatch({{1, 2000, 3.0}, {1, 1500, 4.0}});
    assert(!bad.ok);
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({1, 2}, 0, 2000, &cursor));
    const auto points = Read(cursor.get());
    assert(points.size() == 2);
    assert(points[0].series_id == 2 && points[1].series_id == 1);
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
    assert(old_points.size() == 1);
    std::unique_ptr<Cursor> current;
    AssertOk(engine->Scan({1}, 0, 2000, &current));
    const auto current_points = Read(current.get());
    assert(current_points.size() == 1 && current_points[0].timestamp_ms == 1500);
    std::unique_ptr<Cursor> empty;
    AssertOk(engine->Scan({1}, 1500, 1500, &empty));
    assert(Read(empty.get()).empty());
    std::unique_ptr<Cursor> reversed;
    auto reversed_status = engine->Scan({1}, 1501, 1500, &reversed);
    assert(!reversed_status.ok);
    AssertOk(engine->Compact());
    std::unique_ptr<Cursor> compacted;
    AssertOk(engine->Scan({1}, 0, 2000, &compacted));
    const auto compacted_points = Read(compacted.get());
    assert(compacted_points.size() == 1 && compacted_points[0].timestamp_ms == 1500);
    assert(old_points.size() == 1);
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
    assert(recovered.size() == 3);
    std::unique_ptr<Cursor> aggregate;
    AssertOk(engine->Aggregate({10}, 0, 1000, 200, AggregateOp::Count, &aggregate));
    auto result = aggregate->NextAggregate();
    assert(result && result->count == 1);
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
                assert(write_status.ok);
            }
        });
    }
    for (auto& writer : writers) writer.join();
    assert(versions[0] > 0 && versions[1] > 0 &&
           versions[2] > 0 && versions[3] > 0);
    std::unique_ptr<Cursor> cursor;
    AssertOk(engine->Scan({100, 101, 102, 103}, 0, 5000, &cursor));
    assert(Read(cursor.get()).size() == 160);
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
    assert(!out_of_order.ok);
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
    assert(old_view->NextPoint().has_value());
    old_view->Close();
    AssertOk(engine->GarbageCollect());
    std::unique_ptr<Cursor> new_view;
    AssertOk(engine->Scan({600}, 0, 300, &new_view));
    assert(Read(new_view.get()).empty());
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
    assert(points.size() == 4);
    assert(points[0].timestamp_ms == 100);
    assert(points[2].timestamp_ms == 200);
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
    assert(points.size() == 5);
    assert(points[0].timestamp_ms == -1600);
    assert(points.back().timestamp_ms == 2000);
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
    assert(Read(cursor.get()).size() == 3);
    std::unique_ptr<Cursor> aggregate;
    AssertOk(engine->Aggregate({400, 400}, -1600, -1400, 200,
                               AggregateOp::Count, &aggregate));
    uint64_t count = 0;
    while (auto result = aggregate->NextAggregate()) count += result->count;
    assert(count == 3);
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
    assert(!sstable.empty());
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
    assert(!status.ok);
    assert(engine == nullptr);
}

}  // namespace

int main() {
    const auto root = TestPath();
    TestTimestampEncodingRoundTrip();
    TestWriteScanAndAtomicValidation(root / "atomic");
    TestReadViewAndExpiration(root / "views");
    TestRecovery(root / "recovery");
    TestConcurrentGroupCommit(root / "group_commit");
    TestQueueBackpressure(root / "queue");
    TestNegativeWindowsAndDuplicateSeries(root / "negative");
    TestMultipleIncrementalFlushes(root / "incremental");
    TestPhysicalExpirationAndGC(root / "physical_expiration");
    TestSstableChecksum(root / "checksum");
    std::cout << "all tests passed\n";
}
