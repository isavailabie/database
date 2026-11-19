#include "tsdb.h"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <limits>
#include <thread>
#include <vector>

int main(int argc, char** argv) {
    size_t seconds_limit = 10;
    if (argc > 1) seconds_limit = std::stoull(argv[1]);
    const bool keep = argc > 2 && std::string(argv[2]) == "--keep";

    const auto path = std::filesystem::temp_directory_path() /
                      (keep ? "tsdb-ingest-benchmark" : "tsdb-mixed-benchmark");
    if (!keep) std::filesystem::remove_all(path);

    tsdb::Status status;
    auto engine = tsdb::Engine::Open(
        path, tsdb::Options{.shard_count = 4, .memtable_max_points = 100000},
        &status);
    if (!status.ok) {
        std::cerr << status.message << '\n';
        return 1;
    }

    std::atomic<bool> stop{false};
    std::atomic<uint64_t> written{0};
    std::atomic<uint64_t> scans{0};
    std::atomic<uint64_t> scan_errors{0};
    std::vector<std::thread> threads;

    for (uint64_t writer_id = 0; writer_id < 4; ++writer_id) {
        threads.emplace_back([&, writer_id] {
            const uint64_t timestamp_base =
                keep ? 1000000000000ULL : 0;
            uint64_t timestamp = timestamp_base + writer_id * 100000000ULL;
            while (!stop.load(std::memory_order_relaxed)) {
                std::vector<tsdb::DataPoint> batch;
                batch.reserve(100);
                for (size_t i = 0; i < 100; ++i) {
                    batch.push_back({
                        1000 + writer_id * 100 + (i % 100),
                        static_cast<int64_t>(timestamp++),
                        static_cast<double>((timestamp + i) % 1000)});
                }
                tsdb::CommitVersion version = 0;
                tsdb::Status write_status =
                    engine->WriteBatch(batch, &version);
                if (!write_status.ok) {
                    std::cerr << "write: " << write_status.message << '\n';
                    stop.store(true);
                    break;
                }
                written.fetch_add(batch.size(), std::memory_order_relaxed);
            }
        });
    }

    for (uint64_t reader_id = 0; reader_id < 16; ++reader_id) {
        threads.emplace_back([&, reader_id] {
            while (!stop.load(std::memory_order_relaxed)) {
                std::unique_ptr<tsdb::Cursor> cursor;
                tsdb::Status scan_status =
                    engine->Scan({1000 + reader_id % 400},
                                 0, std::numeric_limits<int64_t>::max(),
                                 &cursor);
                if (!scan_status.ok) {
                    scan_errors.fetch_add(1, std::memory_order_relaxed);
                    break;
                }
                while (cursor->NextPoint()) {}
                scans.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    std::this_thread::sleep_for(std::chrono::seconds(seconds_limit));
    stop.store(true);
    for (auto& thread : threads) thread.join();

    tsdb::Status close_status = engine->Close();
    if (!close_status.ok) {
        std::cerr << "close: " << close_status.message << '\n';
        return 2;
    }
    tsdb::EngineStats stats;
    if (!engine->GetStats(&stats).ok) return 3;

    std::cout << "seconds=" << seconds_limit
              << " written=" << written.load()
              << " scans=" << scans.load()
              << " scan_errors=" << scan_errors.load()
              << " visible_version=" << stats.visible_version
              << " wal_bytes=" << stats.wal_bytes
              << " flush_count=" << stats.flush_count
              << " compaction_count=" << stats.compaction_count
              << " active_read_views=" << stats.active_read_views
              << " obsolete_sstables=" << stats.obsolete_sstable_count
              << " peak_rss_bytes=" << stats.peak_rss_bytes
              << " engine_threads=" << stats.engine_threads << '\n';
}
