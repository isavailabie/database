#include "tsdb.h"

#include <chrono>
#include <filesystem>
#include <iostream>
#include <limits>
#include <vector>

int main(int argc, char** argv) {
    size_t points = 100000;
    if (argc > 1) points = std::stoull(argv[1]);

    const auto path =
        std::filesystem::temp_directory_path() / "tsdb-ingest-benchmark";
    std::filesystem::remove_all(path);

    tsdb::Status status;
    auto engine = tsdb::Engine::Open(
        path, tsdb::Options{.shard_count = 4, .memtable_max_points = 100000},
        &status);
    if (!status.ok) {
        std::cerr << status.message << '\n';
        return 1;
    }

    std::vector<tsdb::DataPoint> batch;
    batch.reserve(1000);
    const auto start = std::chrono::steady_clock::now();
    size_t submitted = 0;
    while (submitted < points) {
        batch.clear();
        for (size_t i = 0; i < 1000 && submitted < points; ++i, ++submitted) {
            batch.push_back({1000 + (submitted % 10000),
                             static_cast<int64_t>(submitted),
                             static_cast<double>(submitted % 1000)});
        }
        tsdb::CommitVersion version = 0;
        status = engine->WriteBatch(batch, &version);
        if (!status.ok) {
            std::cerr << status.message << '\n';
            return 2;
        }
    }
    status = engine->Close();
    if (!status.ok) {
        std::cerr << status.message << '\n';
        return 3;
    }
    const auto elapsed = std::chrono::steady_clock::now() - start;
    const double seconds =
        std::chrono::duration_cast<std::chrono::duration<double>>(elapsed).count();
    const auto recovery_start = std::chrono::steady_clock::now();
    status = tsdb::Status::Ok();
    auto reopened = tsdb::Engine::Open(path, tsdb::Options{
        .shard_count = 4, .memtable_max_points = 100000,
        .max_pending_batches = 64}, &status);
    if (!status.ok) {
        std::cerr << "recovery: " << status.message << '\n';
        return 4;
    }
    std::unique_ptr<tsdb::Cursor> cursor;
    status = reopened->Scan({1000}, 0,
                            std::numeric_limits<int64_t>::max(), &cursor);
    if (!status.ok) return 5;
    uint64_t recovered = 0;
    while (cursor->NextPoint()) ++recovered;
    const auto recovery_elapsed = std::chrono::steady_clock::now() -
                                  recovery_start;
    tsdb::EngineStats stats;
    if (!reopened->GetStats(&stats).ok) return 6;
    reopened->Close();
    const double recovery_seconds =
        std::chrono::duration_cast<std::chrono::duration<double>>(
            recovery_elapsed).count();
    std::cout << "points=" << points
              << " seconds=" << seconds
              << " throughput=" << static_cast<double>(points) / seconds
              << " points/s"
              << " sstables=" << stats.sstable_count
              << " sstable_bytes=" << stats.sstable_bytes
              << " recovery_seconds=" << recovery_seconds
              << " wal_bytes_replayed=" << stats.wal_bytes_replayed
              << " recovered_series_1000_points=" << recovered
              << " peak_rss_bytes=" << stats.peak_rss_bytes << '\n';
}
