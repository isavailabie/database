#include "tsdb.h"
#include "bench_util.h"

#include <iostream>
#include <limits>
#include <stdexcept>

namespace {
constexpr uint64_t kNarrowWindow = 128;
constexpr uint64_t kNarrowStride = 997;
}  // namespace

// Deterministic design-level benchmark. Link this exact driver against both
// revisions: input, API calls, fsync semantics and validation stay identical.
int main(int argc, char** argv) {
    try {
        if (argc != 5)
            throw std::runtime_error("usage: tsdb_design_bench append|compact|scan|narrow points path repeats");
        const auto positive_integer = [](const std::string& text) {
            if (text.empty() || text.find_first_not_of("0123456789") != std::string::npos)
                throw std::runtime_error("invalid positive integer: " + text);
            const uint64_t value = std::stoull(text);
            if (!value) throw std::runtime_error("integer must be positive");
            return value;
        };
        const std::string mode = argv[1];
        const uint64_t points = positive_integer(argv[2]);
        const std::filesystem::path path = argv[3];
        const uint64_t repeats = positive_integer(argv[4]);
        const bool narrow = mode == "narrow";
        if ((!narrow && mode != "append" && mode != "compact" && mode != "scan") || path.empty() ||
            points > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ||
            repeats > std::numeric_limits<uint64_t>::max() / points)
            throw std::runtime_error("invalid arguments");
        // 每个窄查询窗口 [lo, lo + kNarrowWindow) 必须落在 [0, points) 内。
        if (narrow) {
            const uint64_t half = points - points / 2;
            if (half < kNarrowWindow ||
                (repeats - 1) > (half - kNarrowWindow) / kNarrowStride)
                throw std::runtime_error("invalid arguments");
        }
        // 不删除已有数据库；悬空链接也不视为新路径。
        if (std::filesystem::exists(path)) throw std::runtime_error("path must be new");
        for (auto part = std::filesystem::absolute(path);; part = part.parent_path()) {
            if (std::filesystem::is_symlink(part) && !std::filesystem::exists(part))
                throw std::runtime_error("path must not contain a dangling symlink");
            if (part == part.parent_path()) break;
        }
        tsdb::Options options;
        options.memtable_max_points = (mode == "append" || narrow) ? points + 1 : 100000;
        options.enable_background_compaction = false;
        options.enable_background_gc = false;
        options.hot_series_point_limit = points + 1;
        tsdb::Status status;
        auto engine = tsdb::Engine::Open(path, options, &status);
        if (!status.ok || !engine) throw std::runtime_error(status.message);
        const auto require = [](tsdb::Status s) { if (!s.ok) throw std::runtime_error(s.message); };
        const auto start = bench::Clock::now();
        std::vector<tsdb::DataPoint> batch;
        batch.reserve(1000);
        for (uint64_t i = 0; i < points;) {
            batch.clear();
            for (uint64_t j = 0; j < 1000 && i < points; ++j, ++i)
                batch.push_back({1, static_cast<int64_t>(i), static_cast<double>(i % 97)});
            require(engine->WriteBatch(batch));
            // 仅 compact 分段 Flush；narrow 依赖末尾单次 Flush 形成单个 SST。
            if (mode == "compact" && i % 100000 == 0) require(engine->Flush());
        }
        const double append_seconds = bench::Seconds(bench::Clock::now() - start);
        double compact_seconds = 0;
        if (mode != "append") require(engine->Flush());
        if (mode == "compact") {
            const auto begin = bench::Clock::now();
            require(engine->Compact());
            compact_seconds = bench::Seconds(bench::Clock::now() - begin);
        }
        const auto scan_begin = bench::Clock::now();
        uint64_t scanned = 0;
        for (uint64_t iteration = 0; iteration < repeats; ++iteration) {
            // narrow 在单个 SST/单个 series chunk 上做独立窄范围查询，lo 随迭代线性前移；
            // 其余模式做整段全扫描。两者都逐点校验 series/timestamp/value 与点数。
            const int64_t lo = narrow
                ? static_cast<int64_t>(points / 2 + iteration * kNarrowStride) : 0;
            const uint64_t expected = narrow ? kNarrowWindow : points;
            std::unique_ptr<tsdb::Cursor> cursor;
            require(engine->Scan({1}, lo, lo + static_cast<int64_t>(expected), &cursor));
            uint64_t count = 0;
            while (const auto point = cursor->NextPoint()) {
                const int64_t timestamp = lo + static_cast<int64_t>(count);
                if (point->series_id != 1 || point->timestamp_ms != timestamp ||
                    point->value != static_cast<double>(timestamp % 97))
                    throw std::runtime_error("scan data mismatch");
                ++count;
            }
            if (count != expected) throw std::runtime_error("missing points");
            scanned += count;
        }
        const double scan_seconds = bench::Seconds(bench::Clock::now() - scan_begin);
        tsdb::EngineStats stats;
        require(engine->GetStats(&stats));
        require(engine->Close());
        const auto recover_begin = bench::Clock::now();
        engine = tsdb::Engine::Open(path, options, &status);
        if (!engine || !status.ok) throw std::runtime_error(status.message);
        const double recover_seconds = bench::Seconds(bench::Clock::now() - recover_begin);
        std::unique_ptr<tsdb::Cursor> recovered;
        require(engine->Scan({1}, 0, static_cast<int64_t>(points), &recovered));
        uint64_t count = 0;
        while (const auto point = recovered->NextPoint()) {
            if (point->series_id != 1 || point->timestamp_ms != static_cast<int64_t>(count) ||
                point->value != static_cast<double>(count % 97))
                throw std::runtime_error("recovery data mismatch");
            ++count;
        }
        if (count != points) throw std::runtime_error("recovery point mismatch");
        require(engine->Close());
        // 字节单位的进程峰值 RSS，包含驱动缓冲区、写入、扫描和恢复校验。
        const auto rss = bench::PeakRssBytes();
        if (!rss || append_seconds <= 0 || scan_seconds <= 0 || recover_seconds <= 0 ||
            (mode == "compact" && compact_seconds <= 0))
            throw std::runtime_error("invalid timing or RSS measurement");
        std::cout << "mode=" << mode << " points=" << points << " repeats=" << repeats
                  << " append_seconds=" << append_seconds << " compact_seconds=" << compact_seconds
                  << " scan_seconds=" << scan_seconds << " scan_points=" << scanned
                  << " scan_points_per_second=" << scanned / scan_seconds
                  << " rss_bytes=" << rss << " rss_scope=process_peak_bytes"
                  << " compaction_count=" << stats.compaction_count
                  << " flush_count=" << stats.flush_count << " recover_seconds=" << recover_seconds
                  << " recovered_points=" << count << '\n';
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
