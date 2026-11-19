// Import benchmark: writes N points with strictly increasing timestamps, then
// closes, reopens and probes the recovered data. Reports throughput, peak RSS,
// on-disk size, final flush cost and recovery time.
//
//   tsdb_bench [points] [--series N] [--hot-series N] [--hot-fraction F]
//              [--path DIR] [--csv FILE] [--keep] [--crash] [--compact]
//
// --crash writes the points and exits without flushing, so the next
// `tsdb_recover DIR` measures a real crash recovery including WAL replay.

#include "tsdb.h"

#include "bench_util.h"

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

#include <unistd.h>

namespace {

struct Args {
    size_t points{100000};
    uint64_t series{10000};
    uint64_t hot_series{0};
    double hot_fraction{0.0};
    std::filesystem::path path;
    std::string csv;
    bool keep{false};
    bool crash{false};
    bool compact{false};
};

Args ParseArgs(int argc, char** argv) {
    Args args;
    args.path = std::filesystem::temp_directory_path() / "tsdb-ingest-benchmark";
    int index = 1;
    if (index < argc && argv[index][0] != '-') {
        args.points = std::stoull(argv[index]);
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
        if (flag == "--series") {
            args.series = std::stoull(value(&index));
        } else if (flag == "--hot-series") {
            args.hot_series = std::stoull(value(&index));
        } else if (flag == "--hot-fraction") {
            args.hot_fraction = std::stod(value(&index));
        } else if (flag == "--path") {
            args.path = value(&index);
        } else if (flag == "--csv") {
            args.csv = value(&index);
        } else if (flag == "--keep") {
            args.keep = true;
        } else if (flag == "--crash") {
            args.crash = true;
        } else if (flag == "--compact") {
            args.compact = true;
        } else {
            std::cerr << "unknown flag " << flag << '\n';
            std::exit(2);
        }
    }
    return args;
}

}  // namespace

int main(int argc, char** argv) {
    const Args args = ParseArgs(argc, argv);
    if (args.series == 0) {
        std::cerr << "--series must be positive\n";
        return 2;
    }
    if (args.points == 0) {
        std::cerr << "points must be positive\n";
        return 2;
    }
    if (!args.keep) {
        std::error_code ec;
        std::filesystem::remove_all(args.path, ec);
    }

    tsdb::Status status;
    auto engine = tsdb::Engine::Open(
        args.path, tsdb::Options{.shard_count = 4, .memtable_max_points = 100000},
        &status);
    if (!status.ok || !engine) {
        std::cerr << status.message << '\n';
        return 1;
    }

    // Uniform series live at 1000 + slot, hot series at 0 + slot, so the two
    // sets never overlap. Every slot keeps its own monotonically rising clock.
    const uint64_t slots = args.series + args.hot_series;
    std::vector<int64_t> last_timestamp(slots, 0);
    std::vector<tsdb::DataPoint> batch;
    batch.reserve(1000);
    uint64_t rng = 0x9e3779b97f4a7c15ULL;

    const auto started = bench::Clock::now();
    size_t submitted = 0;
    while (submitted < args.points) {
        const size_t count = std::min<size_t>(1000, args.points - submitted);
        batch.clear();
        for (size_t i = 0; i < count; ++i) {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            const bool hot =
                args.hot_series > 0 && args.hot_fraction > 0.0 &&
                static_cast<double>(rng % 10000) < args.hot_fraction * 10000.0;
            // A second draw for the slot, otherwise the hot decision and the
            // slot share one modulus and half of the uniform series stay empty.
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            const uint64_t slot =
                hot ? (rng % args.hot_series) : (rng % args.series);
            const uint64_t series_id = hot ? slot : 1000 + slot;
            const size_t clock_slot = hot ? args.series + slot : slot;
            batch.push_back({series_id, last_timestamp[clock_slot]++,
                             static_cast<double>(submitted % 1000)});
            ++submitted;
        }
        status = engine->WriteBatch(batch, nullptr);
        if (!status.ok) {
            std::cerr << "write failed: " << status.message << '\n';
            return 2;
        }
    }
    const double write_seconds = bench::Seconds(bench::Clock::now() - started);

    if (args.crash) {
        // No Close(): the committed records stay in the WAL for tsdb_recover.
        std::cout << "points=" << args.points << " series=" << args.series
                  << " hot_series=" << args.hot_series
                  << " seconds=" << write_seconds
                  << " throughput="
                  << static_cast<double>(args.points) / write_seconds
                  << " crash=1 path=" << args.path.string() << '\n';
        _exit(0);
    }

    double compaction_seconds = 0.0;
    if (args.compact) {
        const auto compact_started = bench::Clock::now();
        status = engine->Compact();
        if (!status.ok) {
            std::cerr << "compact failed: " << status.message << '\n';
            return 3;
        }
        compaction_seconds = bench::Seconds(bench::Clock::now() - compact_started);
    }
    // Capture the write-phase counters before Close(), which flushes and would
    // otherwise attribute the final flush to the reopened engine instead.
    tsdb::EngineStats write_stats;
    (void)engine->GetStats(&write_stats);
    if (!engine->Close().ok) {
        std::cerr << "close failed\n";
        return 4;
    }

    uint64_t wal_bytes = 0;
    {
        std::error_code ec;
        for (const auto& entry :
             std::filesystem::directory_iterator(args.path / "wal", ec)) {
            if (!entry.is_regular_file()) continue;
            std::error_code size_ec;
            const auto size =
                std::filesystem::file_size(entry.path(), size_ec);
            if (!size_ec) wal_bytes += static_cast<uint64_t>(size);
        }
    }

    const auto recovery_started = bench::Clock::now();
    status = tsdb::Status::Ok();
    auto reopened = tsdb::Engine::Open(
        args.path, tsdb::Options{.shard_count = 4, .memtable_max_points = 100000},
        &status);
    if (!status.ok || !reopened) {
        std::cerr << "recovery failed: " << status.message << '\n';
        return 5;
    }
    uint64_t recovered = 0;
    {
        std::unique_ptr<tsdb::Cursor> cursor;
        status = reopened->Scan({0, 1000}, 0,
                                std::numeric_limits<int64_t>::max(), &cursor);
        if (!status.ok) {
            std::cerr << "probe scan failed: " << status.message << '\n';
            return 6;
        }
        while (cursor->NextPoint()) ++recovered;
    }
    tsdb::EngineStats stats;
    if (!reopened->GetStats(&stats).ok) return 7;
    const double recovery_seconds =
        bench::Seconds(bench::Clock::now() - recovery_started);
    const uint64_t disk_bytes = bench::DirectoryBytes(args.path);
    reopened->Close();

    const double throughput = static_cast<double>(args.points) / write_seconds;
    std::cout << "points=" << args.points << " series=" << args.series
              << " hot_series=" << args.hot_series
              << " seconds=" << write_seconds << " throughput=" << throughput
              << " compact_seconds=" << compaction_seconds
              << " rss_bytes=" << bench::PeakRssBytes()
              << " disk_bytes=" << disk_bytes
              << " sstables=" << stats.sstable_count
              << " sstable_bytes=" << stats.sstable_bytes
              << " recovery_seconds=" << recovery_seconds
              << " flush_count=" << write_stats.flush_count
              << " wal_bytes=" << wal_bytes
              << " wal_bytes_replayed=" << stats.wal_bytes_replayed
              << " recovered_points=" << recovered << '\n';

    bench::AppendCsvRow(
        args.csv,
        "scenario,points,series,hot_series,seconds,throughput,compact_seconds,"
        "rss_bytes,disk_bytes,sstables,sstable_bytes,recovery_seconds,wal_bytes,"
        "wal_bytes_replayed,recovered_points",
        "ingest," + std::to_string(args.points) + "," +
            std::to_string(args.series) + "," +
            std::to_string(args.hot_series) + "," +
            std::to_string(write_seconds) + "," + std::to_string(throughput) +
            "," + std::to_string(compaction_seconds) + "," +
            std::to_string(bench::PeakRssBytes()) + "," +
            std::to_string(disk_bytes) + "," +
            std::to_string(stats.sstable_count) + "," +
            std::to_string(stats.sstable_bytes) + "," +
            std::to_string(recovery_seconds) + "," + std::to_string(wal_bytes) +
            "," + std::to_string(stats.wal_bytes_replayed) + "," +
            std::to_string(recovered));
    return 0;
}
