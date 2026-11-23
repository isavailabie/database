#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace tsdb {

struct DataPoint {
    uint64_t series_id{};
    int64_t timestamp_ms{};
    double value{};
};

using CommitVersion = uint64_t;

enum class AggregateOp { Count, Sum, Min, Max, Avg };

struct AggregateResult {
    uint64_t series_id{};
    int64_t bucket_start{};
    int64_t bucket_end{};
    double result{};
    uint64_t count{};
};

struct Status {
    bool ok{true};
    std::string message;

    static Status Ok() { return {}; }
    static Status Error(std::string message) { return {false, std::move(message)}; }
};

class Cursor {
public:
    Cursor() = default;
    explicit Cursor(std::vector<DataPoint> points);
    explicit Cursor(std::function<std::optional<DataPoint>()> generator);
    explicit Cursor(std::function<std::optional<DataPoint>()> generator,
                    std::function<void()> close_callback);
    explicit Cursor(std::vector<AggregateResult> results);

    std::optional<DataPoint> NextPoint();
    std::optional<AggregateResult> NextAggregate();
    void Close();
    ~Cursor();
    bool closed() const { return closed_; }

private:
    std::vector<DataPoint> points_;
    std::function<std::optional<DataPoint>()> point_generator_;
    std::function<void()> close_callback_;
    std::vector<AggregateResult> aggregates_;
    size_t index_{0};
    bool aggregate_{false};
    bool closed_{false};
    bool close_callback_called_{false};
};

struct Options {
    size_t shard_count{4};
    size_t memtable_max_points{100000};
    size_t max_pending_batches{64};
    bool enable_background_compaction{true};
    size_t max_memtable_bytes{160 * 1024 * 1024};
    size_t cpu_budget{4};
    uint64_t hot_series_point_limit{10000000};
    size_t compaction_sstable_threshold{8};
    // Backpressure: pause writers when more than this many SSTables are live,
    // so the per-series index of a burst of flushes cannot blow the RSS budget.
    // 0 disables the limit.
    size_t max_live_sstables{256};
    bool enable_background_gc{true};
    // Directory for WAL segments; empty means <db>/wal. Pointing it at a
    // separate disk keeps WAL fsyncs from contending with SSTable I/O.
    std::filesystem::path wal_dir;
    // Per-shard WAL + global commit log. Off by default: the single global WAL
    // stays the production path until the sharded protocol has been soaked.
    bool sharded_wal{false};
};

struct EngineStats {
    CommitVersion visible_version{};
    int64_t expiration_cutoff{};
    size_t mutable_points{};
    size_t immutable_points{};
    uint64_t wal_bytes{};
    uint64_t wal_bytes_replayed{};
    uint64_t wal_syncs{};
    uint64_t compaction_count{};
    uint64_t flush_count{};
    size_t pending_batches{};
    uint64_t sstable_count{};
    uint64_t sstable_bytes{};
    size_t active_read_views{};
    uint64_t obsolete_sstable_count{};
    uint64_t rss_bytes{};
    uint64_t peak_rss_bytes{};
    size_t engine_threads{};
    size_t memtable_bytes{};
};

class Engine;

namespace testing {

// Test-only introspection: reports the in-memory buffer of one series so the
// suite can prove that a write outgrew (and therefore reallocated) it instead of
// inferring that from a vector growth model. Not part of the storage contract;
// the engine itself never calls it.
struct SeriesBufferInfo {
    size_t point_count{};
    // Present when the memtable keeps the series in a contiguous buffer whose
    // capacity is meaningful (the vector layout); empty for the ordered-map
    // layout, which has no capacity to grow.
    std::optional<size_t> capacity;
};

SeriesBufferInfo InspectSeriesBuffer(const Engine& engine, uint64_t series_id);

}  // namespace testing

class Engine {
public:
    static std::unique_ptr<Engine> Open(const std::filesystem::path& path,
                                        const Options& options = {},
                                        Status* status = nullptr);
    ~Engine();

    Status Put(const DataPoint& point, CommitVersion* version = nullptr);
    Status WriteBatch(const std::vector<DataPoint>& points,
                      CommitVersion* version = nullptr);
    Status Scan(const std::vector<uint64_t>& series_ids,
                int64_t start, int64_t end,
                std::unique_ptr<Cursor>* cursor) const;
    Status Aggregate(const std::vector<uint64_t>& series_ids,
                     int64_t start, int64_t end, int64_t window_ms,
                     AggregateOp operation,
                     std::unique_ptr<Cursor>* cursor) const;
    Status ExpireBefore(int64_t cutoff, CommitVersion* version = nullptr);
    Status Flush();
    Status Compact();
    Status Close();
    Status GarbageCollect();
    Status GetStats(EngineStats* stats) const;

private:
    struct Impl;
    explicit Engine(std::shared_ptr<Impl> impl);
    Status FlushLocked(bool incremental = true);
    Status CompactRoundLocked(std::unique_lock<std::mutex>& lock,
                              size_t threshold);
    // Shared so that a Cursor created by Scan can keep the engine state alive
    // until the cursor is destroyed, even if the Engine object is gone.
    std::shared_ptr<Impl> impl_;

    // The test hook above needs the impl to reach the memtable buffer.
    friend testing::SeriesBufferInfo testing::InspectSeriesBuffer(
        const Engine& engine, uint64_t series_id);
};

}  // namespace tsdb
