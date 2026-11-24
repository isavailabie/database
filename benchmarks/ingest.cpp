// Import benchmark: writes N points with strictly increasing timestamps, then
// closes, reopens and probes the recovered data. Reports throughput, peak RSS,
// on-disk size, final flush cost and recovery time.
//
//   tsdb_bench [points] [--series N] [--hot-series N] [--hot-fraction F]
//              [--path DIR] [--wal-dir DIR] [--csv FILE] [--keep] [--crash]
//              [--compact] [--sharded-wal]
//
// --crash writes the points and exits without flushing, so the next
// `tsdb_recover DIR` measures a real crash recovery including WAL replay.

#include "tsdb.h"

#include "bench_util.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <stdexcept>
#include <thread>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

#include <unistd.h>

namespace {

uint64_t Unsigned(const std::string& text) {
    if (text.empty() || text.find_first_not_of("0123456789") != std::string::npos)
        throw std::invalid_argument("需要非负整数: " + text);
    return std::stoull(text);
}

struct Args {
    size_t points{100000};
    uint64_t series{10000};
    uint64_t hot_series{0};
    double hot_fraction{0.0};
    std::filesystem::path path;
    std::filesystem::path wal_dir;
    std::string csv;
    bool keep{false};
    bool crash{false};
    bool compact{false};
    bool sharded_wal{false};
};

Args ParseArgs(int argc, char** argv) {
    Args args;
    args.path = std::filesystem::temp_directory_path() / "tsdb-ingest-benchmark";
    int index = 1;
    if (index < argc && argv[index][0] != '-') {
        args.points = Unsigned(argv[index]);
        ++index;
    }
    const auto value = [&](int* position) -> std::string {
        if (*position + 1 >= argc || std::string(argv[*position + 1]).starts_with("--"))
            throw std::invalid_argument(std::string("缺少参数值: ") + argv[*position]);
        ++(*position);
        return argv[*position];
    };
    for (; index < argc; ++index) {
        const std::string flag = argv[index];
        if (flag == "--series") {
            args.series = Unsigned(value(&index));
        } else if (flag == "--hot-series") {
            args.hot_series = Unsigned(value(&index));
        } else if (flag == "--hot-fraction") {
            const auto text = value(&index);
            size_t used = 0;
            args.hot_fraction = std::stod(text, &used);
            if (used != text.size()) throw std::invalid_argument("热点比例格式错误");
        } else if (flag == "--path") {
            args.path = value(&index);
        } else if (flag == "--wal-dir") {
            args.wal_dir = value(&index);
        } else if (flag == "--csv") {
            args.csv = value(&index);
        } else if (flag == "--keep") {
            args.keep = true;
        } else if (flag == "--crash") {
            args.crash = true;
        } else if (flag == "--compact") {
            args.compact = true;
        } else if (flag == "--sharded-wal") {
            args.sharded_wal = true;
        } else {
            std::cerr << "unknown flag " << flag << '\n';
            std::exit(2);
        }
    }
    if (!std::isfinite(args.hot_fraction) || args.hot_fraction < 0.0 || args.hot_fraction > 1.0 ||
        (args.hot_fraction > 0.0 && args.hot_series == 0))
        throw std::invalid_argument("热点比例必须在 [0, 1] 内，非零比例需要热点序列");
    if (args.hot_series > 1000 || args.series > std::numeric_limits<uint64_t>::max() - 1000 ||
        args.series > std::numeric_limits<size_t>::max() - args.hot_series)
        throw std::invalid_argument("序列数超出范围或热点与普通序列重叠");
    return args;
}

int Run(const Args& args) {
    if (args.series == 0) {
        std::cerr << "--series must be positive\n";
        return 2;
    }
    if (args.points == 0) {
        std::cerr << "points must be positive\n";
        return 2;
    }
    if (args.keep) {
        if (!std::filesystem::is_directory(args.path))
            throw std::runtime_error("--keep 需要已有数据库目录");
    } else {
        std::filesystem::remove_all(args.path);
    }

    tsdb::Status status;
    auto engine = tsdb::Engine::Open(
        args.path, tsdb::Options{.shard_count = 4, .memtable_max_points = 100000, .wal_dir = args.wal_dir, .sharded_wal = args.sharded_wal},
        &status);
    if (!status.ok || !engine) {
        std::cerr << status.message << '\n';
        return 1;
    }

    // Uniform series live at 1000 + slot, hot series at 0 + slot, so the two
    // sets never overlap. Every slot keeps its own monotonically rising clock.
    const uint64_t slots = args.series + args.hot_series;
    tsdb::EngineStats initial_stats;
    status = engine->GetStats(&initial_stats);
    if (!status.ok) throw std::runtime_error("stats: " + status.message);
    std::vector<int64_t> last_timestamp(slots, std::max<int64_t>(0, initial_stats.expiration_cutoff));
    uint64_t initial_points = 0;
    if (args.keep) {
        for (uint64_t slot = 0; slot < slots; ++slot) {
            const auto series_id = slot < args.series ? 1000 + slot : slot - args.series;
            std::unique_ptr<tsdb::Cursor> cursor;
            status = engine->Scan({series_id}, std::numeric_limits<int64_t>::min(),
                                  std::numeric_limits<int64_t>::max(), &cursor);
            if (!status.ok || !cursor) throw std::runtime_error("history scan: " + status.message);
            while (const auto point = cursor->NextPoint()) {
                ++initial_points;
                if (point->timestamp_ms >= std::numeric_limits<int64_t>::max() - 1)
                    throw std::runtime_error("历史时间戳已无追加空间");
                last_timestamp[slot] = std::max(last_timestamp[slot], point->timestamp_ms + 1);
            }
        }
        if (!initial_points) throw std::runtime_error("已有库为空或没有所选序列数据");
    }
    std::vector<double> write_latencies_us;
    bench::LatencyReservoir write_reservoir;
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
            if (last_timestamp[clock_slot] == std::numeric_limits<int64_t>::max())
                throw std::runtime_error("写入时间戳溢出");
            batch.push_back({series_id, last_timestamp[clock_slot]++,
                             static_cast<double>(submitted % 1000)});
            ++submitted;
        }
        const auto batch_started = bench::Clock::now();
        status = engine->WriteBatch(batch, nullptr);
        const double batch_us = bench::Seconds(bench::Clock::now() - batch_started) * 1e6;
        write_reservoir.Add(write_latencies_us, batch_us);
        if (!status.ok) {
            std::cerr << "write failed: " << status.message << '\n';
            return 2;
        }
    }
    const double write_seconds = bench::Seconds(bench::Clock::now() - started);
    const double write_p50 = bench::Percentile(&write_latencies_us, 0.50);
    const double write_p95 = bench::Percentile(&write_latencies_us, 0.95);
    const double write_p99 = bench::Percentile(&write_latencies_us, 0.99);

    if (args.crash) {
        // No Close(): the committed records stay in the WAL for tsdb_recover.
        std::cout << "points=" << args.points << " series=" << args.series
                  << " hot_series=" << args.hot_series
                  << " seconds=" << write_seconds
                  << " throughput="
                  << static_cast<double>(args.points) / write_seconds
                  << " write_p50_us=" << write_p50 << " write_p95_us=" << write_p95
                  << " write_p99_us=" << write_p99
                  << " crash=1 path=" << args.path.string() << '\n';
        std::cout.flush();
        _exit(0);
    }

    // The acceptance number has to include the final flush and the compaction
    // the import produced, so drain explicitly and time that separately from the
    // write loop. By default we wait for the background compaction to settle
    // (no change in its counter or in the live file count for ~1s) instead of
    // forcing Compact(), which would rewrite the whole database.
    const auto drain_started = bench::Clock::now();
    status = engine->Flush();
    if (!status.ok) {
        std::cerr << "flush failed: " << status.message << '\n';
        return 3;
    }
    double drain_compact_seconds = 0.0;
    if (args.compact) {
        const auto compact_started = bench::Clock::now();
        status = engine->Compact();
        if (!status.ok) {
            std::cerr << "compact failed: " << status.message << '\n';
            return 3;
        }
        drain_compact_seconds =
            bench::Seconds(bench::Clock::now() - compact_started);
    } else {
        int stable_rounds = 0;
        uint64_t last_compactions = ~0ULL;
        uint64_t last_files = ~0ULL;
        while (stable_rounds < 5) {
            tsdb::EngineStats snapshot;
            if (!engine->GetStats(&snapshot).ok)
                throw std::runtime_error("合并期间读取统计失败");
            if (snapshot.compaction_count == last_compactions &&
                snapshot.sstable_count == last_files) {
                ++stable_rounds;
            } else {
                stable_rounds = 0;
                last_compactions = snapshot.compaction_count;
                last_files = snapshot.sstable_count;
            }
            if (bench::Seconds(bench::Clock::now() - drain_started) > 900.0) {
                throw std::runtime_error("合并在 900 秒内未稳定");
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
    }
    if (!engine->Close().ok) {
        std::cerr << "close failed\n";
        return 4;
    }
    const double drain_seconds = bench::Seconds(bench::Clock::now() - drain_started);
    const double total_seconds = write_seconds + drain_seconds;

    tsdb::EngineStats stats;
    if (!engine->GetStats(&stats).ok) return 4;

    const auto recovery_started = bench::Clock::now();
    status = tsdb::Status::Ok();
    auto reopened = tsdb::Engine::Open(
        args.path, tsdb::Options{.shard_count = 4, .memtable_max_points = 100000, .wal_dir = args.wal_dir, .sharded_wal = args.sharded_wal},
        &status);
    if (!status.ok || !reopened) {
        std::cerr << "recovery failed: " << status.message << '\n';
        return 5;
    }
    uint64_t recovered = 0;
    for (uint64_t slot = 0; slot < slots; ++slot) {
        const auto series_id = slot < args.series ? 1000 + slot : slot - args.series;
        std::unique_ptr<tsdb::Cursor> cursor;
        status = reopened->Scan({series_id}, std::numeric_limits<int64_t>::min(),
                                std::numeric_limits<int64_t>::max(), &cursor);
        if (!status.ok) {
            std::cerr << "probe scan failed: " << status.message << '\n';
            return 6;
        }
        while (cursor->NextPoint()) ++recovered;
    }
    const double recovery_seconds =
        bench::Seconds(bench::Clock::now() - recovery_started);
    const uint64_t disk_bytes = bench::DirectoryBytes(args.path);
    if (!reopened->Close().ok) throw std::runtime_error("恢复校验后关闭失败");
    if (recovered != initial_points + args.points)
        throw std::runtime_error("恢复点数与已提交点数不一致");

    const double throughput = static_cast<double>(args.points) / total_seconds;
    const double write_throughput =
        static_cast<double>(args.points) / write_seconds;
    std::cout << "points=" << args.points << " series=" << args.series
              << " hot_series=" << args.hot_series
              << " seconds=" << total_seconds << " throughput=" << throughput
              << " write_seconds=" << write_seconds
              << " write_throughput=" << write_throughput
              << " drain_seconds=" << drain_seconds
              << " drain_compact_seconds=" << drain_compact_seconds
              << " rss_bytes=" << bench::PeakRssBytes()
              << " disk_bytes=" << disk_bytes
              << " sstables=" << stats.sstable_count
              << " series_index_entries=" << stats.series_index_entries
              << " page_index_entries=" << stats.page_index_entries
              << " obsolete_sstables=" << stats.obsolete_sstable_count
              << " memtable_bytes=" << stats.memtable_bytes
              << " sstable_bytes=" << stats.sstable_bytes
              << " recovery_seconds=" << recovery_seconds
              << " flush_count=" << stats.flush_count
              << " flush_ms=" << stats.flush_ns / 1000000.0
              << " compaction_count=" << stats.compaction_count
              << " compaction_ms=" << stats.compaction_ns / 1000000.0
              << " wal_syncs=" << stats.wal_syncs
              << " wal_sync_ms=" << stats.wal_sync_ns / 1000000.0
              << " wal_bytes_written=" << stats.wal_bytes_written
              << " wal_bytes_replayed=" << stats.wal_bytes_replayed
              << " recovered_points=" << recovered
              << " initial_points=" << initial_points
              << " write_p50_us=" << write_p50 << " write_p95_us=" << write_p95
              << " write_p99_us=" << write_p99 << '\n';

    bench::AppendCsvRow(
        args.csv,
        "scenario,points,series,hot_series,seconds,throughput,write_seconds,"
        "write_throughput,drain_seconds,rss_bytes,disk_bytes,sstables,"
        "sstable_bytes,series_index_entries,page_index_entries,obsolete_sstables,"
        "memtable_bytes,recovery_seconds,flush_count,flush_ms,compaction_count,"
        "compaction_ms,wal_syncs,wal_sync_ms,wal_bytes_written,"
        "wal_bytes_replayed,recovered_points,write_p50_us,write_p95_us,write_p99_us",
        "ingest," + std::to_string(args.points) + "," +
            std::to_string(args.series) + "," +
            std::to_string(args.hot_series) + "," +
            std::to_string(total_seconds) + "," + std::to_string(throughput) +
            "," + std::to_string(write_seconds) + "," +
            std::to_string(write_throughput) + "," +
            std::to_string(drain_seconds) + "," +
            std::to_string(bench::PeakRssBytes()) + "," +
            std::to_string(disk_bytes) + "," +
            std::to_string(stats.sstable_count) + "," +
            std::to_string(stats.sstable_bytes) + "," +
            std::to_string(stats.series_index_entries) + "," +
            std::to_string(stats.page_index_entries) + "," +
            std::to_string(stats.obsolete_sstable_count) + "," +
            std::to_string(stats.memtable_bytes) + "," +
            std::to_string(recovery_seconds) + "," +
            std::to_string(stats.flush_count) + "," +
            std::to_string(stats.flush_ns / 1000000.0) + "," +
            std::to_string(stats.compaction_count) + "," +
            std::to_string(stats.compaction_ns / 1000000.0) + "," +
            std::to_string(stats.wal_syncs) + "," +
            std::to_string(stats.wal_sync_ns / 1000000.0) + "," +
            std::to_string(stats.wal_bytes_written) + "," +
            std::to_string(stats.wal_bytes_replayed) + "," +
            std::to_string(recovered) + "," + std::to_string(write_p50) + "," +
            std::to_string(write_p95) + "," + std::to_string(write_p99));
    return 0;
}
}  // namespace

int main(int argc, char** argv) {
    try {
        return Run(ParseArgs(argc, argv));
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 2;
    }
}
