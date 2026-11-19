// Crash-recovery benchmark: opens an existing database directory (typically one
// left behind by `tsdb_bench N --crash`) and measures how long it takes to
// become serviceable, reporting the WAL that had to be replayed.
//
//   tsdb_recover <db-path> [--wal-dir DIR] [--csv FILE] [--sharded-wal]
//
// The open is timed separately from the first probe scan so that the recovery
// budget can be judged against the 60 s requirement.

#include "tsdb.h"

#include "bench_util.h"

#include <cstdlib>
#include <iostream>
#include <limits>
#include <string>

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "usage: tsdb_recover <db-path> [--csv FILE]\n";
        return 2;
    }
    const std::filesystem::path path = argv[1];
    std::string csv;
    std::filesystem::path wal_dir;
    bool sharded_wal = false;
    for (int index = 2; index < argc; ++index) {
        const std::string flag = argv[index];
        if (flag == "--csv" && index + 1 < argc) {
            csv = argv[++index];
        } else if (flag == "--wal-dir" && index + 1 < argc) {
            wal_dir = argv[++index];
        } else if (flag == "--sharded-wal") {
            sharded_wal = true;
        } else {
            std::cerr << "unknown flag " << flag << '\n';
            return 2;
        }
    }
    const std::filesystem::path effective_wal_dir =
        wal_dir.empty() ? (path / "wal") : wal_dir;

    uint64_t wal_bytes = 0;
    {
        std::error_code ec;
        for (const auto& entry :
             std::filesystem::directory_iterator(effective_wal_dir, ec)) {
            if (!entry.is_regular_file()) continue;
            std::error_code size_ec;
            const auto size =
                std::filesystem::file_size(entry.path(), size_ec);
            if (!size_ec) wal_bytes += static_cast<uint64_t>(size);
        }
    }

    const auto started = bench::Clock::now();
    tsdb::Status status;
    auto engine = tsdb::Engine::Open(
        path, tsdb::Options{.shard_count = 4, .memtable_max_points = 100000, .wal_dir = wal_dir, .sharded_wal = sharded_wal},
        &status);
    if (!status.ok || !engine) {
        std::cerr << "open failed: " << status.message << '\n';
        return 1;
    }
    const double open_seconds = bench::Seconds(bench::Clock::now() - started);

    tsdb::EngineStats stats;
    if (!engine->GetStats(&stats).ok) return 3;

    uint64_t recovered = 0;
    {
        std::unique_ptr<tsdb::Cursor> cursor;
        const tsdb::Status scan_status =
            engine->Scan({0, 1000}, 0, std::numeric_limits<int64_t>::max(),
                         &cursor);
        if (!scan_status.ok) {
            std::cerr << "probe scan failed: " << scan_status.message << '\n';
            return 4;
        }
        while (cursor->NextPoint()) ++recovered;
    }
    const double total_seconds = bench::Seconds(bench::Clock::now() - started);
    const uint64_t disk_bytes = bench::DirectoryBytes(path);
    const uint64_t rss_bytes = bench::PeakRssBytes();
    engine->Close();

    std::cout << "open_seconds=" << open_seconds
              << " total_seconds=" << total_seconds
              << " wal_bytes=" << wal_bytes
              << " wal_bytes_replayed=" << stats.wal_bytes_replayed
              << " sstables=" << stats.sstable_count
              << " recovered_points=" << recovered
              << " disk_bytes=" << disk_bytes
              << " rss_bytes=" << rss_bytes << '\n';

    bench::AppendCsvRow(
        csv,
        "scenario,open_seconds,total_seconds,wal_bytes,wal_bytes_replayed,"
        "sstables,recovered_points,disk_bytes,rss_bytes",
        "recover," + std::to_string(open_seconds) + "," +
            std::to_string(total_seconds) + "," + std::to_string(wal_bytes) +
            "," + std::to_string(stats.wal_bytes_replayed) + "," +
            std::to_string(stats.sstable_count) + "," +
            std::to_string(recovered) + "," + std::to_string(disk_bytes) + "," +
            std::to_string(rss_bytes));
    return 0;
}
