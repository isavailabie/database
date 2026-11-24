// 异常恢复基准：打开已有库，计时 Open，并流式校验全部所选序列。
// tsdb_recover <db-path> [--wal-dir DIR] [--csv FILE] [--sharded-wal]
//   [--series N] [--hot-series N] [--expected-points N]
#include "tsdb.h"
#include "bench_util.h"

#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>

namespace {
uint64_t Unsigned(const std::string& text) {
    if (text.empty() || text.find_first_not_of("0123456789") != std::string::npos)
        throw std::invalid_argument("需要非负整数: " + text);
    return std::stoull(text);
}

int Run(int argc, char** argv) {
    if (argc < 2 || std::string(argv[1]).starts_with("--"))
        throw std::invalid_argument("usage: tsdb_recover <db-path> [--csv FILE]");
    const std::filesystem::path path = argv[1];
    std::string csv;
    std::filesystem::path wal_dir;
    bool sharded_wal = false;
    uint64_t series = 10000, hot_series = 8, expected_points = 0;
    const auto value = [&](int& index) -> std::string {
        if (index + 1 >= argc || std::string(argv[index + 1]).starts_with("--"))
            throw std::invalid_argument(std::string("缺少参数值: ") + argv[index]);
        return argv[++index];
    };
    for (int index = 2; index < argc; ++index) {
        const std::string flag = argv[index];
        if (flag == "--csv") csv = value(index);
        else if (flag == "--wal-dir") wal_dir = value(index);
        else if (flag == "--series") series = Unsigned(value(index));
        else if (flag == "--hot-series") hot_series = Unsigned(value(index));
        else if (flag == "--expected-points") expected_points = Unsigned(value(index));
        else if (flag == "--sharded-wal") sharded_wal = true;
        else throw std::invalid_argument("未知参数: " + flag);
    }
    if (!series || series > std::numeric_limits<uint64_t>::max() - 1000 || hot_series > 1000)
        throw std::invalid_argument("序列数必须有效且热点与普通序列不能重叠");
    if (!std::filesystem::is_directory(path))
        throw std::runtime_error("恢复需要已有数据库目录");
    const auto effective_wal_dir = wal_dir.empty() ? path / "wal" : wal_dir;
    uint64_t wal_bytes = 0;
    std::error_code ec;
    if (std::filesystem::exists(effective_wal_dir)) {
        for (const auto& entry : std::filesystem::recursive_directory_iterator(effective_wal_dir)) {
            if (!entry.is_regular_file()) continue;
            const auto size = std::filesystem::file_size(entry.path(), ec);
            if (ec) throw std::runtime_error("无法读取 WAL 大小: " + ec.message());
            wal_bytes += static_cast<uint64_t>(size);
        }
    }
    const auto started = bench::Clock::now();
    tsdb::Status status;
    auto engine = tsdb::Engine::Open(
        path, tsdb::Options{.shard_count = 4, .memtable_max_points = 100000,
                           .wal_dir = wal_dir, .sharded_wal = sharded_wal}, &status);
    if (!status.ok || !engine) throw std::runtime_error("open: " + status.message);
    const double open_seconds = bench::Seconds(bench::Clock::now() - started);
    tsdb::EngineStats stats;
    status = engine->GetStats(&stats);
    if (!status.ok) throw std::runtime_error("stats: " + status.message);
    uint64_t recovered = 0;
    for (uint64_t slot = 0; slot < series + hot_series; ++slot) {
        const auto series_id = slot < series ? 1000 + slot : slot - series;
        std::unique_ptr<tsdb::Cursor> cursor;
        status = engine->Scan({series_id}, std::numeric_limits<int64_t>::min(),
                              std::numeric_limits<int64_t>::max(), &cursor);
        if (!status.ok || !cursor) throw std::runtime_error("probe scan: " + status.message);
        while (cursor->NextPoint()) ++recovered;
    }
    if (!recovered || (expected_points && recovered != expected_points))
        throw std::runtime_error("恢复库为空或恢复点数与 --expected-points 不一致");
    const double total_seconds = bench::Seconds(bench::Clock::now() - started);
    const uint64_t disk_bytes = bench::DirectoryBytes(path);
    const uint64_t rss_bytes = bench::PeakRssBytes();
    status = engine->Close();
    if (!status.ok) throw std::runtime_error("close: " + status.message);

    std::cout << "open_seconds=" << open_seconds << " total_seconds=" << total_seconds
              << " wal_bytes=" << wal_bytes << " wal_bytes_replayed=" << stats.wal_bytes_replayed
              << " sstables=" << stats.sstable_count << " recovered_points=" << recovered
              << " disk_bytes=" << disk_bytes << " rss_bytes=" << rss_bytes << '\n';
    bench::AppendCsvRow(
        csv, "scenario,open_seconds,total_seconds,wal_bytes,wal_bytes_replayed,"
             "sstables,recovered_points,disk_bytes,rss_bytes",
        "recover," + std::to_string(open_seconds) + "," + std::to_string(total_seconds) + "," +
        std::to_string(wal_bytes) + "," + std::to_string(stats.wal_bytes_replayed) + "," +
        std::to_string(stats.sstable_count) + "," + std::to_string(recovered) + "," +
        std::to_string(disk_bytes) + "," + std::to_string(rss_bytes));
    return 0;
}
}  // namespace

int main(int argc, char** argv) {
    try {
        return Run(argc, argv);
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 2;
    }
}
