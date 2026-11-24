// 混合读写基准：--keep 复用导入库，写入从已有数据末尾继续。
// tsdb_mixed_bench [seconds] [--writers N] [--readers N]
//   [--series-per-writer N] [--series N] [--hot] [--path DIR]
//   [--wal-dir DIR] [--csv FILE] [--keep] [--sharded-wal]
//   [--expected-points N] [--sample-csv FILE] [--sample-interval SECONDS]
#include "tsdb.h"
#include "bench_util.h"

#include <algorithm>
#include <atomic>
#include <cmath>
#include <condition_variable>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

uint64_t Unsigned(const std::string& text) {
    if (text.empty() || text.find_first_not_of("0123456789") != std::string::npos)
        throw std::invalid_argument("需要非负整数: " + text);
    return std::stoull(text);
}

struct Args {
    size_t seconds{60};
    size_t writers{4};
    size_t readers{16};
    uint64_t series_per_writer{100};
    uint64_t series{10000};
    uint64_t expected_points{0};
    double sample_interval{10.0};
    std::filesystem::path path;
    std::filesystem::path wal_dir;
    std::string csv;
    std::string sample_csv;
    bool hot{false};
    bool keep{false};
    bool sharded_wal{false};
};

Args ParseArgs(int argc, char** argv) {
    Args args;
    args.path = std::filesystem::temp_directory_path() / "tsdb-mixed-benchmark";
    int index = 1;
    if (index < argc && argv[index][0] != '-') args.seconds = Unsigned(argv[index++]);
    const auto value = [&](int* position) -> std::string {
        if (*position + 1 >= argc || std::string(argv[*position + 1]).starts_with("--"))
            throw std::invalid_argument(std::string("缺少参数值: ") + argv[*position]);
        return argv[++(*position)];
    };
    for (; index < argc; ++index) {
        const std::string flag = argv[index];
        if (flag == "--writers") args.writers = Unsigned(value(&index));
        else if (flag == "--readers") args.readers = Unsigned(value(&index));
        else if (flag == "--series-per-writer") args.series_per_writer = Unsigned(value(&index));
        else if (flag == "--series") args.series = Unsigned(value(&index));
        else if (flag == "--expected-points") args.expected_points = Unsigned(value(&index));
        else if (flag == "--hot") args.hot = true;
        else if (flag == "--path") args.path = value(&index);
        else if (flag == "--wal-dir") args.wal_dir = value(&index);
        else if (flag == "--csv") args.csv = value(&index);
        else if (flag == "--sample-csv") args.sample_csv = value(&index);
        else if (flag == "--sample-interval") {
            const auto text = value(&index);
            size_t used = 0;
            args.sample_interval = std::stod(text, &used);
            if (used != text.size()) throw std::invalid_argument("采样间隔格式错误");
        } else if (flag == "--keep") args.keep = true;
        else if (flag == "--sharded-wal") args.sharded_wal = true;
        else throw std::invalid_argument("未知参数: " + flag);
    }
    if (args.hot) args.series_per_writer = 2;
    if (!args.seconds || !args.writers || !args.readers || !args.series_per_writer || !args.series)
        throw std::invalid_argument("时长、线程数和序列数必须为正数");
    if (!std::isfinite(args.sample_interval) || args.sample_interval < 0.001 ||
        args.sample_interval > 86400.0)
        throw std::invalid_argument("采样间隔必须在 0.001 到 86400 秒之间");
    if (args.seconds > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) / 1000000000ULL ||
        args.series > std::numeric_limits<uint64_t>::max() - 1000 ||
        args.writers > (std::numeric_limits<uint64_t>::max() - 1000) / args.series_per_writer)
        throw std::invalid_argument("参数超出可表示范围");
    if (args.expected_points && !args.keep)
        throw std::invalid_argument("--expected-points 需要 --keep");
    if (!args.csv.empty() && args.csv == args.sample_csv)
        throw std::invalid_argument("汇总 CSV 和采样 CSV 必须使用不同文件");
    return args;
}

// wait_until 使用独立通知，退出不等待下一个采样间隔。
class Sampler {
public:
    Sampler(tsdb::Engine& engine, const Args& args, bench::Clock::time_point started)
        : engine_(engine), args_(args), started_(started) {
        if (args.sample_csv.empty()) return;
        out_.open(args.sample_csv, std::ios::trunc);
        if (!out_) throw std::runtime_error("无法写入采样 CSV: " + args.sample_csv);
        out_ << "elapsed_seconds,current_rss_bytes,peak_rss_bytes,live_sstables,"
                "obsolete_sstables,disk_bytes,wal_bytes\n";
        Sample();
        thread_ = std::thread([this] {
            const auto interval = std::chrono::duration_cast<bench::Clock::duration>(
                std::chrono::duration<double>(args_.sample_interval));
            auto deadline = started_ + interval;
            std::unique_lock lock(mutex_);
            while (!cv_.wait_until(lock, deadline, [this] { return done_; })) {
                lock.unlock();
                Sample();
                lock.lock();
                deadline += interval;
                while (deadline <= bench::Clock::now()) deadline += interval;
            }
        });
    }
    ~Sampler() { Stop(); }
    void Stop() {
        {
            std::lock_guard lock(mutex_);
            done_ = true;
        }
        cv_.notify_all();
        if (thread_.joinable()) {
            thread_.join();
            Sample();
        }
    }
    bool ok() const { return !failed_; }

private:
    void Sample() {
        try {
            tsdb::EngineStats stats;
            const auto status = engine_.GetStats(&stats);
            if (!status.ok) throw std::runtime_error(status.message);
            const auto live = stats.live_sstable_count;
            uint64_t disk = bench::DirectoryBytes(args_.path);
            if (!args_.wal_dir.empty()) {
                const auto wal = std::filesystem::weakly_canonical(args_.wal_dir);
                const auto root = std::filesystem::weakly_canonical(args_.path);
                const auto relative = wal.lexically_relative(root);
                if (relative.empty() || *relative.begin() == "..") disk += bench::DirectoryBytes(wal);
            }
            out_ << bench::Seconds(bench::Clock::now() - started_) << ','
                 << stats.rss_bytes << ',' << stats.peak_rss_bytes << ',' << live << ','
                 << stats.obsolete_sstable_count << ',' << disk << ',' << stats.wal_bytes << '\n';
            out_.flush();
            if (!out_) throw std::runtime_error("采样 CSV 写入失败");
        } catch (const std::exception& error) {
            failed_ = true;
            std::cerr << "sample: " << error.what() << '\n';
        }
    }
    tsdb::Engine& engine_;
    const Args& args_;
    bench::Clock::time_point started_;
    std::ofstream out_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool done_{false};
    bool failed_{false};
    std::thread thread_;
};

int Run(const Args& args) {
    if (args.keep) {
        if (!std::filesystem::is_directory(args.path))
            throw std::runtime_error("--keep 需要已有数据库目录");
    } else {
        std::filesystem::remove_all(args.path);
    }
    tsdb::Status status;
    auto engine = tsdb::Engine::Open(
        args.path, tsdb::Options{.shard_count = 4, .memtable_max_points = 100000,
                               .wal_dir = args.wal_dir, .sharded_wal = args.sharded_wal}, &status);
    if (!status.ok || !engine) throw std::runtime_error("open: " + status.message);
    tsdb::EngineStats initial_stats;
    status = engine->GetStats(&initial_stats);
    if (!status.ok) throw std::runtime_error("stats: " + status.message);

    const uint64_t total_series = args.writers * args.series_per_writer;
    std::vector<uint64_t> query_series;
    uint64_t initial_points = 0;
    int64_t write_start = std::max<int64_t>(0, initial_stats.expiration_cutoff);
    if (args.keep) {
        std::vector<uint64_t> candidates;
        for (uint64_t i = 0; i < std::max(args.series, args.hot ? uint64_t{0} : total_series); ++i)
            candidates.push_back(1000 + i);
        for (uint64_t i = 0; i < std::max<uint64_t>(8, args.hot ? total_series : 0); ++i)
            candidates.push_back(i);
        // 逐序列流式校验，既获得真实历史末尾，也避免一次持有所有序列的解码器。
        for (const auto series_id : candidates) {
            std::unique_ptr<tsdb::Cursor> cursor;
            status = engine->Scan({series_id}, std::numeric_limits<int64_t>::min(),
                                  std::numeric_limits<int64_t>::max(), &cursor);
            if (!status.ok || !cursor) throw std::runtime_error("history scan: " + status.message);
            bool populated = false;
            while (const auto point = cursor->NextPoint()) {
                ++initial_points;
                populated = true;
                if (point->timestamp_ms >= std::numeric_limits<int64_t>::max() - 100)
                    throw std::runtime_error("历史时间戳已无追加空间");
                write_start = std::max(write_start, point->timestamp_ms + 1);
            }
            if (populated) query_series.push_back(series_id);
        }
        if (!initial_points || initial_points < args.expected_points)
            throw std::runtime_error("已有库为空或恢复点数小于 --expected-points");
    }
    for (uint64_t i = 0; i < total_series; ++i) query_series.push_back((args.hot ? 0 : 1000) + i);
    std::sort(query_series.begin(), query_series.end());
    query_series.erase(std::unique(query_series.begin(), query_series.end()), query_series.end());

    std::atomic<bool> stop{false};
    std::atomic<uint64_t> written{0}, scans{0}, scan_errors{0}, write_errors{0};
    std::atomic<uint64_t> scanned_points{0}, historical_read_points{0};
    std::mutex stop_mutex;
    std::condition_variable stop_cv;
    const auto request_stop = [&] {
        {
            std::lock_guard lock(stop_mutex);
            stop.store(true);
        }
        stop_cv.notify_all();
    };
    std::vector<std::vector<double>> latencies_us(args.readers), write_latencies_us(args.writers);
    std::vector<std::thread> threads;
    const auto started = bench::Clock::now();
    Sampler sampler(*engine, args, started);

    for (size_t writer_id = 0; writer_id < args.writers; ++writer_id) {
        threads.emplace_back([&, writer_id] {
            try {
                const uint64_t series_base = (args.hot ? 0 : 1000) + writer_id * args.series_per_writer;
                int64_t timestamp = write_start;
                std::vector<tsdb::DataPoint> batch;
                batch.reserve(100);
                auto& samples = write_latencies_us[writer_id];
                bench::LatencyReservoir reservoir(writer_id + 1);
                while (!stop.load(std::memory_order_relaxed)) {
                    if (timestamp > std::numeric_limits<int64_t>::max() - 100)
                        throw std::runtime_error("写入时间戳溢出");
                    batch.clear();
                    for (uint64_t i = 0; i < 100; ++i)
                        batch.push_back({series_base + (i % args.series_per_writer), timestamp++, static_cast<double>(i)});
                    tsdb::CommitVersion version = 0;
                    const auto write_started = bench::Clock::now();
                    const auto write_status = engine->WriteBatch(batch, &version);
                    const double elapsed_us = bench::Seconds(bench::Clock::now() - write_started) * 1e6;
                    if (!write_status.ok) throw std::runtime_error(write_status.message);
                    reservoir.Add(samples, elapsed_us);
                    written.fetch_add(batch.size(), std::memory_order_relaxed);
                }
            } catch (const std::exception& error) {
                std::cerr << "write: " << error.what() << '\n';
                write_errors.fetch_add(1);
                request_stop();
            }
        });
    }
    for (size_t reader_id = 0; reader_id < args.readers; ++reader_id) {
        threads.emplace_back([&, reader_id] {
            try {
                auto& samples = latencies_us[reader_id];
                bench::LatencyReservoir reservoir(reader_id + 100);
                uint64_t turn = reader_id;
                while (!stop.load(std::memory_order_relaxed)) {
                    const uint64_t series_id = query_series[turn++ % query_series.size()];
                    std::unique_ptr<tsdb::Cursor> cursor;
                    const auto scan_started = bench::Clock::now();
                    const auto scan_status = engine->Scan({series_id}, std::numeric_limits<int64_t>::min(),
                                                          std::numeric_limits<int64_t>::max(), &cursor);
                    if (!scan_status.ok || !cursor) throw std::runtime_error(scan_status.message);
                    uint64_t points = 0, historical = 0;
                    while (const auto point = cursor->NextPoint()) {
                        ++points;
                        if (point->timestamp_ms < write_start) ++historical;
                    }
                    const double elapsed_us = bench::Seconds(bench::Clock::now() - scan_started) * 1e6;
                    reservoir.Add(samples, elapsed_us);
                    scanned_points.fetch_add(points, std::memory_order_relaxed);
                    historical_read_points.fetch_add(historical, std::memory_order_relaxed);
                    scans.fetch_add(1, std::memory_order_relaxed);
                }
            } catch (const std::exception& error) {
                std::cerr << "scan: " << error.what() << '\n';
                scan_errors.fetch_add(1);
                request_stop();
            }
        });
    }
    {
        std::unique_lock lock(stop_mutex);
        stop_cv.wait_for(lock, std::chrono::seconds(args.seconds), [&] { return stop.load(); });
    }
    request_stop();
    for (auto& thread : threads) thread.join();
    const double elapsed = bench::Seconds(bench::Clock::now() - started);
    sampler.Stop();
    if (!sampler.ok() || write_errors.load() || scan_errors.load() || !written.load() || !scans.load() ||
        (args.keep && !historical_read_points.load()))
        throw std::runtime_error("混合负载失败或未读取已有数据");
    status = engine->Close();
    if (!status.ok) throw std::runtime_error("close: " + status.message);
    tsdb::EngineStats stats;
    status = engine->GetStats(&stats);
    if (!status.ok) throw std::runtime_error("stats: " + status.message);

    std::vector<double> merged, write_merged;
    for (const auto& samples : latencies_us) merged.insert(merged.end(), samples.begin(), samples.end());
    for (const auto& samples : write_latencies_us) write_merged.insert(write_merged.end(), samples.begin(), samples.end());
    const double p50 = bench::Percentile(&merged, 0.50), p95 = bench::Percentile(&merged, 0.95),
                 p99 = bench::Percentile(&merged, 0.99);
    const double write_p50 = bench::Percentile(&write_merged, 0.50),
                 write_p95 = bench::Percentile(&write_merged, 0.95),
                 write_p99 = bench::Percentile(&write_merged, 0.99);
    const double worst = merged.empty() ? 0.0 : merged.back();
    const uint64_t written_count = written.load(), scan_count = scans.load();
    const uint64_t disk_bytes = bench::DirectoryBytes(args.path);
    const double write_throughput = static_cast<double>(written_count) / elapsed;
    const double scan_throughput = static_cast<double>(scan_count) / elapsed;
    const auto commits = stats.visible_version - initial_stats.visible_version;

    std::cout << "seconds=" << elapsed << " writers=" << args.writers << " readers=" << args.readers
              << " series_per_writer=" << args.series_per_writer << " written=" << written_count
              << " write_throughput=" << write_throughput << " scans=" << scan_count
              << " scan_throughput=" << scan_throughput << " scan_errors=" << scan_errors.load()
              << " scan_p50_us=" << p50 << " scan_p95_us=" << p95 << " scan_p99_us=" << p99
              << " scan_max_us=" << worst << " rss_bytes=" << bench::PeakRssBytes()
              << " disk_bytes=" << disk_bytes << " flush_count=" << stats.flush_count
              << " compaction_count=" << stats.compaction_count
              << " obsolete_sstables=" << stats.obsolete_sstable_count << " wal_bytes=" << stats.wal_bytes
              << " visible_version=" << stats.visible_version << " commits=" << commits
              << " wal_syncs=" << stats.wal_syncs
              << " syncs_per_second=" << static_cast<double>(stats.wal_syncs) / elapsed
              << " points_per_commit=" << (commits ? static_cast<double>(written_count) / commits : 0.0)
              << " engine_threads=" << stats.engine_threads << " write_errors=" << write_errors.load()
              << " write_p50_us=" << write_p50 << " write_p95_us=" << write_p95 << " write_p99_us=" << write_p99
              << " initial_points=" << initial_points << " write_start_timestamp=" << write_start
              << " scanned_points=" << scanned_points.load() << " historical_read_points=" << historical_read_points.load()
              << " current_rss_bytes=" << stats.rss_bytes << " peak_rss_bytes=" << stats.peak_rss_bytes << '\n';

    bench::AppendCsvRow(
        args.csv,
        "scenario,seconds,writers,readers,series_per_writer,written,write_throughput,scans,"
        "scan_throughput,scan_errors,scan_p50_us,scan_p95_us,scan_p99_us,scan_max_us,rss_bytes,"
        "disk_bytes,flush_count,compaction_count,obsolete_sstables,wal_bytes,visible_version,"
        "write_p50_us,write_p95_us,write_p99_us,initial_points,write_start_timestamp,historical_read_points",
        "mixed," + std::to_string(elapsed) + "," + std::to_string(args.writers) + "," + std::to_string(args.readers) +
        "," + std::to_string(args.series_per_writer) + "," + std::to_string(written_count) + "," +
        std::to_string(write_throughput) + "," + std::to_string(scan_count) + "," + std::to_string(scan_throughput) +
        "," + std::to_string(scan_errors.load()) + "," + std::to_string(p50) + "," + std::to_string(p95) + "," +
        std::to_string(p99) + "," + std::to_string(worst) + "," + std::to_string(bench::PeakRssBytes()) + "," +
        std::to_string(disk_bytes) + "," + std::to_string(stats.flush_count) + "," + std::to_string(stats.compaction_count) +
        "," + std::to_string(stats.obsolete_sstable_count) + "," + std::to_string(stats.wal_bytes) + "," +
        std::to_string(stats.visible_version) + "," + std::to_string(write_p50) + "," + std::to_string(write_p95) + "," +
        std::to_string(write_p99) + "," + std::to_string(initial_points) + "," + std::to_string(write_start) + "," +
        std::to_string(historical_read_points.load()));
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
