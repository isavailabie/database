// Mixed read/write benchmark: 4 writer threads and 16 reader threads against a
// live engine (background flush, compaction and GC all enabled). Reports write
// throughput, scan throughput and scan latency percentiles, plus peak RSS and
// on-disk size.
//
//   tsdb_mixed_bench [seconds] [--writers N] [--readers N]
//                    [--series-per-writer N] [--hot] [--path DIR]
//                    [--csv FILE] [--keep]
//
// --keep runs against an existing database produced by an earlier import (the
// writers offset their timestamps past the imported range).
// --hot concentrates every writer on a couple of series, which exercises the
// single-shard hotspot path.

#include "tsdb.h"

#include "bench_util.h"

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <string>
#include <thread>
#include <vector>

namespace {

struct Args {
    size_t seconds{60};
    size_t writers{4};
    size_t readers{16};
    uint64_t series_per_writer{100};
    std::filesystem::path path;
    std::string csv;
    bool keep{false};
    bool sharded_wal{false};
};

Args ParseArgs(int argc, char** argv) {
    Args args;
    args.path = std::filesystem::temp_directory_path() / "tsdb-mixed-benchmark";
    int index = 1;
    if (index < argc && argv[index][0] != '-') {
        args.seconds = std::stoull(argv[index]);
        ++index;
    }
    const auto value = [&](int* position) -> std::string {
        if (*position + 1 >= argc) {
            std::cerr << "missing value for " << argv[*position] << '\n';
            std::exit(2);
        }
        ++(*position);
        return argv[*position];
    };
    for (; index < argc; ++index) {
        const std::string flag = argv[index];
        if (flag == "--writers") {
            args.writers = std::stoull(value(&index));
        } else if (flag == "--readers") {
            args.readers = std::stoull(value(&index));
        } else if (flag == "--series-per-writer") {
            args.series_per_writer = std::stoull(value(&index));
        } else if (flag == "--hot") {
            args.series_per_writer = 2;
        } else if (flag == "--path") {
            args.path = value(&index);
        } else if (flag == "--csv") {
            args.csv = value(&index);
        } else if (flag == "--keep") {
            args.keep = true;
        } else if (flag == "--sharded-wal") {
            args.sharded_wal = true;
        } else {
            std::cerr << "unknown flag " << flag << '\n';
            std::exit(2);
        }
    }
    if (args.writers == 0 || args.readers == 0 || args.series_per_writer == 0) {
        std::cerr << "writers, readers and series-per-writer must be positive\n";
        std::exit(2);
    }
    return args;
}

}  // namespace

int main(int argc, char** argv) {
    const Args args = ParseArgs(argc, argv);
    if (!args.keep) {
        std::error_code ec;
        std::filesystem::remove_all(args.path, ec);
    }

    tsdb::Status status;
    auto engine = tsdb::Engine::Open(
        args.path, tsdb::Options{.shard_count = 4, .memtable_max_points = 100000, .sharded_wal = args.sharded_wal},
        &status);
    if (!status.ok || !engine) {
        std::cerr << status.message << '\n';
        return 1;
    }

    std::atomic<bool> stop{false};
    std::atomic<uint64_t> written{0};
    std::atomic<uint64_t> scans{0};
    std::atomic<uint64_t> scan_errors{0};
    std::vector<std::vector<double>> latencies_us(args.readers);
    std::vector<std::thread> threads;

    const auto started = bench::Clock::now();

    for (size_t writer_id = 0; writer_id < args.writers; ++writer_id) {
        threads.emplace_back([&, writer_id] {
            const uint64_t series_base = 1000 + writer_id * args.series_per_writer;
            int64_t timestamp =
                static_cast<int64_t>(writer_id) * 1000000000LL +
                (args.keep ? 1000000000000LL : 0);
            std::vector<tsdb::DataPoint> batch;
            batch.reserve(100);
            while (!stop.load(std::memory_order_relaxed)) {
                batch.clear();
                for (uint64_t i = 0; i < 100; ++i) {
                    batch.push_back(
                        {series_base + (i % args.series_per_writer),
                         timestamp++,
                         static_cast<double>(i % 1000)});
                }
                tsdb::CommitVersion version = 0;
                const tsdb::Status write_status = engine->WriteBatch(batch, &version);
                if (!write_status.ok) {
                    std::cerr << "write: " << write_status.message << '\n';
                    stop.store(true);
                    break;
                }
                written.fetch_add(batch.size(), std::memory_order_relaxed);
            }
        });
    }

    const uint64_t total_series = args.writers * args.series_per_writer;
    for (size_t reader_id = 0; reader_id < args.readers; ++reader_id) {
        threads.emplace_back([&, reader_id] {
            auto& samples = latencies_us[reader_id];
            uint64_t turn = reader_id;
            while (!stop.load(std::memory_order_relaxed)) {
                const uint64_t series_id = 1000 + (turn % total_series);
                ++turn;
                std::unique_ptr<tsdb::Cursor> cursor;
                const auto scan_started = bench::Clock::now();
                const tsdb::Status scan_status =
                    engine->Scan({series_id}, 0, std::numeric_limits<int64_t>::max(),
                                 &cursor);
                if (!scan_status.ok) {
                    scan_errors.fetch_add(1, std::memory_order_relaxed);
                    break;
                }
                while (cursor->NextPoint()) {
                }
                const double elapsed_us =
                    bench::Seconds(bench::Clock::now() - scan_started) * 1e6;
                // Keep the sample bounded: the driver's buffer counts towards
                // the process RSS, which is what we report.
                if (samples.size() < 50000) samples.push_back(elapsed_us);
                scans.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    std::this_thread::sleep_for(std::chrono::seconds(args.seconds));
    stop.store(true);
    for (auto& thread : threads) thread.join();
    const double elapsed = bench::Seconds(bench::Clock::now() - started);

    if (!engine->Close().ok) {
        std::cerr << "close failed\n";
        return 2;
    }
    tsdb::EngineStats stats;
    if (!engine->GetStats(&stats).ok) return 3;

    std::vector<double> merged;
    size_t total_samples = 0;
    for (const auto& samples : latencies_us) total_samples += samples.size();
    merged.reserve(total_samples);
    for (const auto& samples : latencies_us) {
        merged.insert(merged.end(), samples.begin(), samples.end());
    }
    const double p50 = bench::Percentile(&merged, 0.50);
    const double p95 = bench::Percentile(&merged, 0.95);
    const double p99 = bench::Percentile(&merged, 0.99);
    const double worst = merged.empty() ? 0.0 : merged.back();
    const uint64_t written_count = written.load();
    const uint64_t scan_count = scans.load();
    const uint64_t disk_bytes = bench::DirectoryBytes(args.path);
    const double write_throughput = static_cast<double>(written_count) / elapsed;
    const double scan_throughput = static_cast<double>(scan_count) / elapsed;

    std::cout << "seconds=" << elapsed
              << " writers=" << args.writers
              << " readers=" << args.readers
              << " series_per_writer=" << args.series_per_writer
              << " written=" << written_count
              << " write_throughput=" << write_throughput
              << " scans=" << scan_count
              << " scan_throughput=" << scan_throughput
              << " scan_errors=" << scan_errors.load()
              << " scan_p50_us=" << p50
              << " scan_p95_us=" << p95
              << " scan_p99_us=" << p99
              << " scan_max_us=" << worst
              << " rss_bytes=" << bench::PeakRssBytes()
              << " disk_bytes=" << disk_bytes
              << " flush_count=" << stats.flush_count
              << " compaction_count=" << stats.compaction_count
              << " obsolete_sstables=" << stats.obsolete_sstable_count
              << " wal_bytes=" << stats.wal_bytes
              << " visible_version=" << stats.visible_version
              << " commits=" << stats.visible_version
              << " wal_syncs=" << stats.wal_syncs
              << " syncs_per_second="
              << (elapsed > 0 ? static_cast<double>(stats.wal_syncs) / elapsed
                              : 0.0)
              << " points_per_commit="
              << (stats.visible_version
                      ? static_cast<double>(written_count) /
                            static_cast<double>(stats.visible_version)
                      : 0.0)
              << " engine_threads=" << stats.engine_threads << '\n';

    bench::AppendCsvRow(
        args.csv,
        "scenario,seconds,writers,readers,series_per_writer,written,"
        "write_throughput,scans,scan_throughput,scan_errors,scan_p50_us,"
        "scan_p95_us,scan_p99_us,scan_max_us,rss_bytes,disk_bytes,flush_count,"
        "compaction_count,obsolete_sstables,wal_bytes,visible_version",
        "mixed," + std::to_string(elapsed) + "," + std::to_string(args.writers) +
            "," + std::to_string(args.readers) + "," +
            std::to_string(args.series_per_writer) + "," +
            std::to_string(written_count) + "," +
            std::to_string(write_throughput) + "," + std::to_string(scan_count) +
            "," + std::to_string(scan_throughput) + "," +
            std::to_string(scan_errors.load()) + "," + std::to_string(p50) + "," +
            std::to_string(p95) + "," + std::to_string(p99) + "," +
            std::to_string(worst) + "," +
            std::to_string(bench::PeakRssBytes()) + "," +
            std::to_string(disk_bytes) + "," + std::to_string(stats.flush_count) +
            "," + std::to_string(stats.compaction_count) + "," +
            std::to_string(stats.obsolete_sstable_count) + "," +
            std::to_string(stats.wal_bytes) + "," +
            std::to_string(stats.visible_version));
    return 0;
}
