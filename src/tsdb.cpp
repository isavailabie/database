#include "tsdb.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <limits>
#include <map>
#include <mutex>
#include <numeric>
#include <queue>
#include <set>
#include <sstream>
#include <sys/stat.h>
#include <tuple>
#include <unistd.h>
#include <sys/resource.h>
#include <condition_variable>
#include <thread>
#include <deque>
#include <unordered_map>
#include <unordered_set>

#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif

namespace tsdb {
namespace {

struct SstableIndexEntry {
    uint64_t series_id{};
    uint64_t offset{};
    uint64_t byte_length{};
    uint64_t point_count{};
    int64_t min_timestamp{std::numeric_limits<int64_t>::max()};
    int64_t max_timestamp{std::numeric_limits<int64_t>::min()};
};

struct SstableMetadata {
    std::filesystem::path path;
    size_t shard_id{};
    size_t level{0};
    uint64_t point_count{};
    uint32_t encoding{0};
    int64_t min_timestamp{std::numeric_limits<int64_t>::max()};
    int64_t max_timestamp{std::numeric_limits<int64_t>::min()};
    std::map<uint64_t, SstableIndexEntry> series;
    mutable size_t refs{0};
};

constexpr uint32_t kWalMagic = 0x54534442;
constexpr uint8_t kTxnRecord = 1;
constexpr uint8_t kExpireRecord = 2;
constexpr uint8_t kCommitRangeRecord = 3;
constexpr uint64_t kDiskPageRecords = 128;
constexpr uint32_t kEncodedSstableMagic = 0x53535445;
constexpr uint32_t kEncodedWalPayloadMagic = 0x57564c45;

uint64_t Checksum(const std::string& payload) {
    uint64_t hash = 1469598103934665603ULL;
    for (unsigned char byte : payload) {
        hash ^= byte;
        hash *= 1099511628211ULL;
    }
    return hash;
}

uint64_t HashRecord(uint64_t hash, const char* bytes, size_t length) {
    for (size_t i = 0; i < length; ++i) {
        hash ^= static_cast<unsigned char>(bytes[i]);
        hash *= 1099511628211ULL;
    }
    return hash;
}


bool SyncDirectory(const std::filesystem::path& path) {
    const int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return false;
    const bool ok = ::fsync(fd) == 0;
    ::close(fd);
    return ok;
}

template <typename T>
void Append(std::string& out, T value) {
    const auto* bytes = reinterpret_cast<const char*>(&value);
    out.append(bytes, sizeof(T));
}

template <typename T>
bool Read(const std::string& data, size_t& offset, T* value) {
    if (offset + sizeof(T) > data.size()) return false;
    std::memcpy(value, data.data() + offset, sizeof(T));
    offset += sizeof(T);
    return true;
}

void AppendUvarint(std::string& output, uint64_t value) {
    while (value >= 0x80) {
        output.push_back(static_cast<char>((value & 0x7f) | 0x80));
        value >>= 7;
    }
    output.push_back(static_cast<char>(value));
}

bool ReadUvarint(const std::string& input, size_t& offset, uint64_t* value) {
    uint64_t result = 0;
    unsigned shift = 0;
    while (offset < input.size() && shift < 64) {
        const uint8_t byte =
            static_cast<uint8_t>(static_cast<unsigned char>(input[offset++]));
        result |= static_cast<uint64_t>(byte & 0x7f) << shift;
        if ((byte & 0x80) == 0) {
            *value = result;
            return true;
        }
        shift += 7;
    }
    return false;
}

uint64_t ZigzagEncode(int64_t value) {
    // Go through unsigned: left shifting a negative int64_t is undefined.
    return (static_cast<uint64_t>(value) << 1) ^
           (static_cast<uint64_t>(value) >> 63);
}

int64_t ZigzagDecode(uint64_t value) {
    return static_cast<int64_t>((value >> 1) ^
                                -static_cast<int64_t>(value & 1));
}


bool SyncFile(const std::filesystem::path& path) {
    const int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return false;
    const bool ok = ::fsync(fd) == 0;
    ::close(fd);
    return ok;
}

// WAL segments live in db/wal/000001.log, 000002.log, ... and are replayed in
// numeric order. Rotation lets a flush retire the segments it has already
// covered instead of truncating one shared file.
std::filesystem::path WalSegmentPath(const std::filesystem::path& dir,
                                     uint64_t number) {
    char name[32];
    std::snprintf(name, sizeof(name), "%06llu.log",
                  static_cast<unsigned long long>(number));
    return dir / name;
}

std::vector<uint64_t> ListSegmentNumbers(const std::filesystem::path& dir) {
    std::vector<uint64_t> segments;
    std::error_code ec;
    if (!std::filesystem::exists(dir, ec)) return segments;
    for (const auto& entry : std::filesystem::directory_iterator(dir, ec)) {
        if (!entry.is_regular_file()) continue;
        if (entry.path().extension() != ".log") continue;
        const std::string stem = entry.path().stem().string();
        if (stem.empty()) continue;
        bool digits = true;
        for (const char c : stem) {
            if (std::isdigit(static_cast<unsigned char>(c)) == 0) {
                digits = false;
                break;
            }
        }
        if (!digits) continue;
        try {
            segments.push_back(std::stoull(stem));
        } catch (...) {
            continue;
        }
    }
    std::sort(segments.begin(), segments.end());
    return segments;
}

// Best-effort rollback of a partially written WAL record. A failure here is not
// actionable: the caller reports the original write error to the client anyway.
void TruncateWalBestEffort(int fd, off_t size) {
    const int truncated = ::ftruncate(fd, size);
    const int synced = ::fsync(fd);
    (void)truncated;
    (void)synced;
}

void PinCurrentThread(size_t cpu_budget, size_t index) {
    (void)cpu_budget;
    (void)index;
#if defined(__linux__)
    if (cpu_budget == 0) return;
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(index % cpu_budget, &set);
    pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
#endif
}

struct VersionedPoint {
    DataPoint point;
    CommitVersion version{};
};

using SeriesPoints = std::map<int64_t, VersionedPoint>;

struct ShardData {
    // Snapshots share the per-series maps: a commit clones only the series it
    // actually writes, never the whole shard. Copying the outer map stays cheap
    // (a few thousand shared_ptr) regardless of how many points are resident.
    std::map<uint64_t, std::shared_ptr<SeriesPoints>> series;
    size_t point_count() const {
        size_t count = 0;
        for (const auto& [_, points] : series) count += points->size();
        return count;
    }
};

struct ReadView {
    CommitVersion version{};
    int64_t cutoff{std::numeric_limits<int64_t>::min()};
    std::vector<std::shared_ptr<const ShardData>> shards;
    std::vector<std::shared_ptr<const SstableMetadata>> sstables;
};

std::string WalPayloadTxn(CommitVersion version,
                          const std::vector<DataPoint>& points) {
    std::string payload;
    Append(payload, kEncodedWalPayloadMagic);
    Append(payload, version);
    const uint32_t count = static_cast<uint32_t>(points.size());
    Append(payload, count);
    for (const auto& point : points) {
        AppendUvarint(payload, point.series_id);
        AppendUvarint(payload, ZigzagEncode(point.timestamp_ms));
        Append(payload, point.value);
    }
    return payload;
}

std::string WalPayloadExpire(CommitVersion version, int64_t cutoff) {
    std::string payload;
    Append(payload, version);
    Append(payload, cutoff);
    return payload;
}

bool AppendWalRecord(int fd, uint8_t type,
                     const std::string& payload) {
    if (fd < 0) return false;
    off_t record_start = ::lseek(fd, 0, SEEK_END);
    if (record_start < 0) return false;
    const uint32_t magic = kWalMagic;
    const uint64_t length = payload.size();
    const uint64_t checksum = Checksum(payload);
    std::string record;
    Append(record, magic);
    Append(record, type);
    Append(record, length);
    Append(record, checksum);
    record.append(payload);
    size_t written = 0;
    while (written < record.size()) {
        const ssize_t result =
            ::write(fd, record.data() + written, record.size() - written);
        if (result <= 0) {
            TruncateWalBestEffort(fd, record_start);
            return false;
        }
        written += static_cast<size_t>(result);
    }
    if (::fsync(fd) == 0) return true;
    TruncateWalBestEffort(fd, record_start);
    return false;
}

bool AppendWalRecordNoSync(int fd, uint8_t type,
                           const std::string& payload) {
    if (fd < 0) return false;
    off_t record_start = ::lseek(fd, 0, SEEK_END);
    if (record_start < 0) return false;
    const uint32_t magic = kWalMagic;
    const uint64_t length = payload.size();
    const uint64_t checksum = Checksum(payload);
    std::string record;
    Append(record, magic);
    Append(record, type);
    Append(record, length);
    Append(record, checksum);
    record.append(payload);
    size_t written = 0;
    while (written < record.size()) {
        const ssize_t result =
            ::write(fd, record.data() + written, record.size() - written);
        if (result <= 0) {
            TruncateWalBestEffort(fd, record_start);
            return false;
        }
        written += static_cast<size_t>(result);
    }
    return true;
}

bool ReadFile(const std::filesystem::path& path, std::string* data) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return false;
    *data = std::string(std::istreambuf_iterator<char>(in), {});
    return true;
}

struct SstableWriter {
    std::ofstream out;
    uint64_t hash{1469598103934665603ULL};
    uint64_t count{0};
    uint64_t payload_bytes{0};
    SstableMetadata metadata;
    uint32_t magic{kEncodedSstableMagic};

    bool Open(const std::filesystem::path& temp) {
        out.open(temp, std::ios::binary | std::ios::trunc);
        if (!out) return false;
        out.write(reinterpret_cast<const char*>(&magic), sizeof(magic));
        const uint64_t zero = 0;
        out.write(reinterpret_cast<const char*>(&zero), sizeof(zero));
        return true;
    }

    void Append(uint64_t series_id, int64_t timestamp, double value,
                CommitVersion version) {
        auto& entry = metadata.series[series_id];
        entry.series_id = series_id;
        if (entry.point_count == 0) {
            entry.offset = sizeof(uint32_t) + sizeof(uint64_t) + payload_bytes;
        }
        char buf[10];
        size_t n = 0;
        uint64_t v = series_id;
        while (v >= 0x80) {
            buf[n++] = static_cast<char>((v & 0x7f) | 0x80);
            v >>= 7;
        }
        buf[n++] = static_cast<char>(v);
        WriteAndHash(buf, n);
        v = ZigzagEncode(timestamp);
        n = 0;
        while (v >= 0x80) {
            buf[n++] = static_cast<char>((v & 0x7f) | 0x80);
            v >>= 7;
        }
        buf[n++] = static_cast<char>(v);
        WriteAndHash(buf, n);
        WriteAndHash(reinterpret_cast<const char*>(&value), sizeof(value));
        v = version;
        n = 0;
        while (v >= 0x80) {
            buf[n++] = static_cast<char>((v & 0x7f) | 0x80);
            v >>= 7;
        }
        buf[n++] = static_cast<char>(v);
        WriteAndHash(buf, n);
        ++entry.point_count;
        entry.byte_length = sizeof(uint32_t) + sizeof(uint64_t) + payload_bytes -
                            entry.offset;
        entry.min_timestamp = std::min(entry.min_timestamp, timestamp);
        entry.max_timestamp = std::max(entry.max_timestamp, timestamp);
        metadata.min_timestamp = std::min(metadata.min_timestamp, timestamp);
        metadata.max_timestamp = std::max(metadata.max_timestamp, timestamp);
        ++count;
    }

    void WriteAndHash(const char* bytes, size_t length) {
        out.write(bytes, static_cast<std::streamsize>(length));
        hash = HashRecord(hash, bytes, length);
        payload_bytes += length;
    }

    bool Finish() {
        out.write(reinterpret_cast<const char*>(&hash), sizeof(hash));
        out.flush();
        out.seekp(sizeof(uint32_t), std::ios::beg);
        out.write(reinterpret_cast<const char*>(&count), sizeof(count));
        out.flush();
        out.close();
        metadata.point_count = count;
        metadata.encoding = magic;
        return !out.fail();
    }

    void Discard() {
        out.close();
    }
};

struct StreamingReader {
    std::ifstream in;
    uint64_t hash{1469598103934665603ULL};
    uint64_t bytes{0};

    explicit StreamingReader(const std::filesystem::path& path)
        : in(path, std::ios::binary) {}

    bool ReadUvarint(uint64_t* value) {
        uint64_t result = 0;
        unsigned shift = 0;
        while (shift < 64) {
            char raw = 0;
            in.get(raw);
            if (!in) return false;
            hash = HashRecord(hash, &raw, 1);
            ++bytes;
            const uint8_t byte = static_cast<uint8_t>(raw);
            result |= static_cast<uint64_t>(byte & 0x7f) << shift;
            if ((byte & 0x80) == 0) {
                *value = result;
                return true;
            }
            shift += 7;
        }
        return false;
    }

    bool ReadDouble(double* value) {
        char raw[sizeof(double)];
        in.read(raw, sizeof(raw));
        if (!in) return false;
        std::memcpy(value, raw, sizeof(raw));
        hash = HashRecord(hash, raw, sizeof(raw));
        bytes += sizeof(raw);
        return true;
    }
};

constexpr size_t kInlineReadBytes = 1 * 1024 * 1024;

bool ReadSeriesPoints(const SstableMetadata& sstable, uint64_t series_id,
                      std::vector<VersionedPoint>* out) {
    const auto it = sstable.series.find(series_id);
    if (it == sstable.series.end()) return true;
    const auto& entry = it->second;
    out->reserve(entry.point_count);
    if (sstable.encoding == kEncodedSstableMagic) {
        if (entry.byte_length <= kInlineReadBytes) {
            const int fd = ::open(sstable.path.c_str(), O_RDONLY);
            if (fd < 0) return false;
            std::vector<char> buffer(entry.byte_length);
            const ssize_t bytes =
                ::pread(fd, buffer.data(), buffer.size(), entry.offset);
            ::close(fd);
            if (bytes <= 0) return false;
            std::string payload(buffer.data(), static_cast<size_t>(bytes));
            size_t offset = 0;
            for (uint64_t i = 0; i < entry.point_count; ++i) {
                uint64_t sid = 0;
                uint64_t zigzag_timestamp = 0;
                double value = 0.0;
                uint64_t version = 0;
                if (!ReadUvarint(payload, offset, &sid) ||
                    !ReadUvarint(payload, offset, &zigzag_timestamp) ||
                    !Read(payload, offset, &value) ||
                    !ReadUvarint(payload, offset, &version)) {
                    return false;
                }
                out->push_back({{sid, ZigzagDecode(zigzag_timestamp), value},
                                static_cast<CommitVersion>(version)});
            }
            return true;
        }
        StreamingReader reader(sstable.path);
        reader.in.seekg(entry.offset, std::ios::beg);
        if (!reader.in) return false;
        for (uint64_t i = 0; i < entry.point_count; ++i) {
            uint64_t sid = 0;
            uint64_t zigzag_timestamp = 0;
            double value = 0.0;
            uint64_t version = 0;
            if (!reader.ReadUvarint(&sid) ||
                !reader.ReadUvarint(&zigzag_timestamp) ||
                !reader.ReadDouble(&value) ||
                !reader.ReadUvarint(&version)) {
                return false;
            }
            out->push_back({{sid, ZigzagDecode(zigzag_timestamp), value},
                            static_cast<CommitVersion>(version)});
        }
        return true;
    }
    const int fd = ::open(sstable.path.c_str(), O_RDONLY);
    if (fd < 0) return false;
    const size_t record_size = sizeof(uint64_t) + sizeof(int64_t) +
                               sizeof(double) + sizeof(CommitVersion);
    std::vector<char> buffer(entry.point_count * record_size);
    const ssize_t bytes =
        ::pread(fd, buffer.data(), buffer.size(), entry.offset);
    const size_t complete = static_cast<size_t>(bytes) / record_size;
    for (size_t j = 0; j < complete; ++j) {
        const char* record = buffer.data() + j * record_size;
        uint64_t sid = 0;
        int64_t timestamp = 0;
        double value = 0.0;
        CommitVersion version = 0;
        std::memcpy(&sid, record, sizeof(uint64_t));
        std::memcpy(&timestamp, record + sizeof(uint64_t), sizeof(int64_t));
        std::memcpy(&value,
                    record + sizeof(uint64_t) + sizeof(int64_t),
                    sizeof(double));
        std::memcpy(&version,
                    record + sizeof(uint64_t) + sizeof(int64_t) +
                        sizeof(double),
                    sizeof(CommitVersion));
        out->push_back({{sid, timestamp, value}, version});
    }
    ::close(fd);
    return complete == entry.point_count;
}

}  // namespace

Cursor::Cursor(std::vector<DataPoint> points) : points_(std::move(points)) {}

Cursor::Cursor(std::function<std::optional<DataPoint>()> generator)
    : point_generator_(std::move(generator)) {}

Cursor::Cursor(std::function<std::optional<DataPoint>()> generator,
               std::function<void()> close_callback)
    : point_generator_(std::move(generator)),
      close_callback_(std::move(close_callback)) {}

Cursor::Cursor(std::vector<AggregateResult> results)
    : aggregates_(std::move(results)), aggregate_(true) {}

std::optional<DataPoint> Cursor::NextPoint() {
    if (closed_ || aggregate_) return std::nullopt;
    if (point_generator_) {
        auto point = point_generator_();
        if (!point && !close_callback_called_) {
            close_callback_called_=true;
            if (close_callback_) close_callback_();
        }
        return point;
    }
    if (index_ >= points_.size()) return std::nullopt;
    return points_[index_++];
}

std::optional<AggregateResult> Cursor::NextAggregate() {
    if (closed_ || !aggregate_ || index_ >= aggregates_.size()) return std::nullopt;
    return aggregates_[index_++];
}

void Cursor::Close() {
    if (!close_callback_called_) {
        close_callback_called_=true;
        if (close_callback_) close_callback_();
    }
    closed_ = true;
    points_.clear();
    aggregates_.clear();
    point_generator_ = {};
}

Cursor::~Cursor() {
    Close();
}

struct PendingWrite {
    std::vector<DataPoint> points;
    CommitVersion version{0};
    off_t wal_start{0};
    off_t wal_end{0};
    Status status{Status::Ok()};
    bool done{false};
};

// One commit split by shard: the unit of work of a shard worker.
struct ShardFragment {
    std::vector<VersionedPoint> points;
};

struct Engine::Impl {
    explicit Impl(std::filesystem::path root, Options opts)
        : root(std::move(root)), options(opts) {}

    std::filesystem::path root;
    Options options;
    std::filesystem::path wal_dir;
    uint64_t wal_segment{1};
    // Sharded WAL state (Options::sharded_wal). Each shard appends to its own
    // log; a separate commit log records which versions are durable.
    bool sharded_wal{false};
    std::filesystem::path shard_wal_dir;
    std::filesystem::path commit_wal_dir;
    std::vector<int> shard_wal_fd;
    int commit_wal_fd{-1};
    std::vector<uint64_t> shard_wal_segment;
    uint64_t commit_wal_segment{1};
    std::vector<std::pair<CommitVersion, CommitVersion>> committed_ranges;
    uint32_t wal_format{1};
    std::filesystem::path manifest_path;
    mutable std::mutex mutex;
    std::condition_variable flush_cv;
    std::thread flush_worker;
    std::condition_variable compaction_cv;
    std::thread compaction_worker;
    std::condition_variable write_cv;
    std::thread write_worker;
    std::deque<std::shared_ptr<PendingWrite>> write_queue;
    bool stopping_write{false};
    bool flush_requested{false};
    bool stopping_flush{false};
    bool compaction_requested{false};
    bool stopping_compaction{false};
    std::condition_variable close_cv;
    Engine* owner{nullptr};
    std::vector<std::shared_ptr<ShardData>> shards;
    // Per-shard execution: each shard owns a mutex, a queue and a worker, so a
    // commit is applied by the shards in parallel instead of by one thread.
    std::vector<std::unique_ptr<std::mutex>> shard_mutex;
    std::vector<std::unique_ptr<std::condition_variable>> shard_cv;
    std::vector<std::deque<std::shared_ptr<ShardFragment>>> shard_queue;
    std::vector<std::thread> shard_worker;
    std::vector<bool> shard_stopping;
    std::mutex ack_mutex;
    std::condition_variable ack_cv;
    size_t inflight_fragments{0};
    // Highest commit version whose fragments have all been applied; readers
    // never observe a partially applied batch.
    CommitVersion visible_version{0};
    CommitVersion next_version{1};
    int64_t cutoff{std::numeric_limits<int64_t>::min()};
    bool closed{false};
    bool closing{false};
    size_t points_since_flush{0};
    std::vector<std::filesystem::path> current_files;
    std::vector<std::shared_ptr<SstableMetadata>> sstable_metadata;
    uint64_t wal_bytes_replayed{0};
    uint64_t wal_valid_bytes{0};
    uint64_t wal_syncs{0};
    size_t active_writers{0};
    uint64_t next_file_number{1};
    int wal_fd{-1};
    uint64_t compaction_count{0};
    uint64_t flush_count{0};
    size_t memtable_bytes{0};
    std::unordered_map<uint64_t, uint64_t> hot_series_points;
    std::vector<CommitVersion> covered_commit_versions;
    CommitVersion metadata_covered_commit_version{0};
    size_t active_read_views{0};
    std::vector<std::shared_ptr<SstableMetadata>> obsolete_sstables;
    CommitVersion last_flushed_version{0};
    std::unordered_map<uint64_t, int64_t> last_timestamps;

    // Each streaming cursor holds one decoder per (series, SSTable) pair it
    // touches, so this cap bounds open descriptors rather than result memory.
    static constexpr size_t kMaxConcurrentScans = 16;
    std::mutex scan_mu;
    std::condition_variable scan_cv;
    size_t active_scans{0};

    size_t ShardFor(uint64_t series_id) const {
        uint64_t value = series_id;
        value ^= value >> 30;
        value *= 0xbf58476d1ce4e5b9ULL;
        value ^= value >> 27;
        value *= 0x94d049bb133111ebULL;
        value ^= value >> 31;
        return value % options.shard_count;
    }

    ReadView ViewLocked() const {
        ReadView view;
        // Only fully applied batches are visible: a reader never observes half
        // of a cross-shard commit.
        view.version = visible_version;
        view.cutoff = cutoff;
        view.shards.reserve(shards.size());
        for (size_t i = 0; i < shards.size(); ++i) {
            std::lock_guard lock(*shard_mutex[i]);
            view.shards.push_back(shards[i]);
        }
        for (const auto& sstable : sstable_metadata) {
            view.sstables.push_back(sstable);
            ++sstable->refs;
        }
        return view;
    }

    Status ValidateBatch(const std::vector<DataPoint>& points) const {
        if (points.empty()) return Status::Error("empty batch");
        std::map<uint64_t, int64_t> last;
        for (const auto& pending : write_queue) {
            for (const auto& point : pending->points) {
                auto it = last.find(point.series_id);
                if (it == last.end()) {
                    last.emplace(point.series_id, point.timestamp_ms);
                } else if (point.timestamp_ms > it->second) {
                    it->second = point.timestamp_ms;
                }
            }
        }
        for (const auto& point : points) {
            if (point.timestamp_ms < cutoff) {
                return Status::Error("point is before expiration cutoff");
            }
            auto [it, inserted] = last.emplace(point.series_id, point.timestamp_ms);
            if (!inserted) {
                if (point.timestamp_ms <= it->second) {
                    return Status::Error("batch timestamps are not strictly increasing");
                }
                it->second = point.timestamp_ms;
            }
            const auto committed = last_timestamps.find(point.series_id);
            if (committed != last_timestamps.end() &&
                point.timestamp_ms <= committed->second) {
                return Status::Error("timestamp is not greater than last committed timestamp");
            }
        }
        return Status::Ok();
    }

    // Appends one point to the (already copy-on-write cloned) shard. The series
    // map is cloned lazily on first write, so a 10-point batch touching three
    // series copies three series maps instead of the whole shard.
    void AppendToMemtable(uint64_t series_id, int64_t timestamp,
                          const DataPoint& point, CommitVersion version) {
        auto& slot = shards[ShardFor(series_id)]->series[series_id];
        if (!slot) {
            slot = std::make_shared<SeriesPoints>();
        } else if (!slot.unique()) {
            slot = std::make_shared<SeriesPoints>(*slot);
        }
        (*slot)[timestamp] = VersionedPoint{point, version};
        last_timestamps[series_id] = timestamp;
        memtable_bytes += sizeof(DataPoint) + sizeof(CommitVersion);
        ++hot_series_points[series_id];
    }

    void ApplyPoints(const std::vector<DataPoint>& points, CommitVersion version) {
        std::set<size_t> touched;
        for (const auto& point : points) touched.insert(ShardFor(point.series_id));
        for (const auto shard_id : touched) {
            shards[shard_id] = std::make_shared<ShardData>(*shards[shard_id]);
        }
        for (const auto& point : points) {
            AppendToMemtable(point.series_id, point.timestamp_ms, point, version);
        }
    }

    void ClearMemtableLocked() {
        for (size_t i = 0; i < shards.size(); ++i) {
            std::lock_guard lock(*shard_mutex[i]);
            shards[i] = std::make_shared<ShardData>();
        }
        memtable_bytes = 0;
    }

    // Splits a committed group by shard, hands every fragment to that shard's
    // worker and waits until all of them have been applied. Called with the
    // global mutex held, so no flush or expire can interleave with a commit.
    void DispatchGroup(const std::vector<std::shared_ptr<PendingWrite>>& group) {
        std::vector<std::shared_ptr<ShardFragment>> fragments(shards.size());
        for (const auto& request : group) {
            for (const auto& point : request->points) {
                const size_t shard_id = ShardFor(point.series_id);
                if (!fragments[shard_id]) {
                    fragments[shard_id] = std::make_shared<ShardFragment>();
                }
                fragments[shard_id]->points.push_back(
                    VersionedPoint{point, request->version});
                last_timestamps[point.series_id] = point.timestamp_ms;
                memtable_bytes += sizeof(DataPoint) + sizeof(CommitVersion);
                ++hot_series_points[point.series_id];
            }
        }
        size_t pending = 0;
        for (const auto& fragment : fragments) {
            if (fragment) ++pending;
        }
        if (pending == 0) return;
        {
            std::lock_guard guard(ack_mutex);
            inflight_fragments = pending;
        }
        for (size_t i = 0; i < fragments.size(); ++i) {
            if (!fragments[i]) continue;
            {
                std::lock_guard lock(*shard_mutex[i]);
                shard_queue[i].push_back(fragments[i]);
            }
            shard_cv[i]->notify_one();
        }
        std::unique_lock ack_lock(ack_mutex);
        ack_cv.wait(ack_lock, [&] { return inflight_fragments == 0; });
    }

    void ApplyFragment(size_t shard_id, const ShardFragment& fragment) {
        std::lock_guard lock(*shard_mutex[shard_id]);
        auto next = std::make_shared<ShardData>(*shards[shard_id]);
        for (const auto& point : fragment.points) {
            auto& slot = next->series[point.point.series_id];
            if (!slot) {
                slot = std::make_shared<SeriesPoints>();
            } else if (!slot.unique()) {
                slot = std::make_shared<SeriesPoints>(*slot);
            }
            (*slot)[point.point.timestamp_ms] = point;
        }
        shards[shard_id] = next;
    }

    Status WriteMergedShard(
        const std::vector<std::shared_ptr<SstableMetadata>>& sources,
        SstableWriter* writer) {
        std::set<uint64_t> series_ids;
        for (const auto& sstable : sources) {
            for (const auto& [series_id, _] : sstable->series) {
                series_ids.insert(series_id);
            }
        }
        for (const uint64_t series_id : series_ids) {
            std::map<int64_t, VersionedPoint> merged;
            const auto insert = [&](int64_t timestamp,
                                    const VersionedPoint& point) {
                if (timestamp < cutoff) return;
                auto [it, ok] = merged.emplace(timestamp, point);
                if (!ok && point.version > it->second.version) {
                    it->second = point;
                }
            };
            for (const auto& sstable : sources) {
                if (sstable->series.find(series_id) ==
                    sstable->series.end()) {
                    continue;
                }
                std::vector<VersionedPoint> points;
                if (!ReadSeriesPoints(*sstable, series_id, &points)) {
                    return Status::Error("compaction read failed");
                }
                for (const auto& point : points) {
                    insert(point.point.timestamp_ms, point);
                }
            }
            for (const auto& [timestamp, point] : merged) {
                if (point.version > next_version - 1) continue;
                writer->Append(series_id, timestamp, point.point.value,
                               point.version);
            }
        }
        return Status::Ok();
    }

    bool ShouldCompactLocked(size_t threshold) const {
        for (size_t shard_id = 0; shard_id < shards.size(); ++shard_id) {
            std::map<size_t, size_t> counts;
            for (const auto& sstable : sstable_metadata) {
                if (sstable->shard_id == shard_id) {
                    ++counts[sstable->level];
                }
            }
            for (const auto& [level, count] : counts) {
                if (count >= threshold) return true;
            }
        }
        return false;
    }

    struct CompactionJob {
        size_t shard_id{};
        size_t level{};
        std::vector<std::shared_ptr<SstableMetadata>> sources;
        std::filesystem::path file;
        std::filesystem::path temp;
    };

    std::vector<CompactionJob> PlanCompactionLocked(size_t threshold) {
        std::vector<CompactionJob> jobs;
        const size_t kNoLevel = std::numeric_limits<size_t>::max();
        std::vector<size_t> compact_level(shards.size(), kNoLevel);
        for (size_t shard_id = 0; shard_id < shards.size(); ++shard_id) {
            std::map<size_t, size_t> counts;
            for (const auto& sstable : sstable_metadata) {
                if (sstable->shard_id == shard_id) ++counts[sstable->level];
            }
            for (const auto& [level, count] : counts) {
                if (count >= threshold) {
                    compact_level[shard_id] = level;
                    break;
                }
            }
        }
        for (size_t shard_id = 0; shard_id < shards.size(); ++shard_id) {
            if (compact_level[shard_id] == kNoLevel) continue;
            CompactionJob job;
            job.shard_id = shard_id;
            job.level = compact_level[shard_id];
            for (const auto& sstable : sstable_metadata) {
                if (sstable->shard_id == shard_id &&
                    sstable->level == job.level) {
                    job.sources.push_back(sstable);
                    ++sstable->refs;
                }
            }
            std::string filename = "shard-";
            filename += shard_id < 10 ? "0" : "";
            filename += std::to_string(shard_id);
            filename += "-" + std::to_string(next_file_number) + ".sst";
            job.file = root / "sst" / filename;
            job.temp = job.file.string() + ".tmp";
            ++next_file_number;
            jobs.push_back(std::move(job));
        }
        return jobs;
    }

    Status MergeCompactionJob(const CompactionJob& job,
                              std::shared_ptr<SstableMetadata>* out) {
        SstableWriter writer;
        if (!writer.Open(job.temp)) return Status::Error("cannot create SSTable");
        auto status = WriteMergedShard(job.sources, &writer);
        if (!status.ok) {
            writer.Discard();
            std::error_code ec;
            std::filesystem::remove(job.temp, ec);
            return status;
        }
        if (writer.count == 0) {
            writer.Discard();
            std::error_code ec;
            std::filesystem::remove(job.temp, ec);
            *out = nullptr;
            return Status::Ok();
        }
        if (!writer.Finish()) {
            std::error_code ec;
            std::filesystem::remove(job.temp, ec);
            return Status::Error("SSTable write failed");
        }
        if (!SyncFile(job.temp)) return Status::Error("SSTable sync failed");
        std::error_code ec;
        std::filesystem::rename(job.temp, job.file, ec);
        if (ec) return Status::Error("SSTable publish failed");
        if (!SyncDirectory(job.file.parent_path())) {
            return Status::Error("SSTable directory sync failed");
        }
        auto metadata =
            std::make_shared<SstableMetadata>(std::move(writer.metadata));
        metadata->path = job.file;
        metadata->shard_id = job.shard_id;
        metadata->level = job.level + 1;
        *out = std::move(metadata);
        return Status::Ok();
    }

    Status PublishCompactionLocked(
        const std::vector<CompactionJob>& jobs,
        const std::vector<std::shared_ptr<SstableMetadata>>& published) {
        for (const auto& job : jobs) {
            for (const auto& source : job.sources) {
                if (source->refs > 0) --source->refs;
            }
        }
        std::unordered_set<const SstableMetadata*> source_ptrs;
        for (const auto& job : jobs) {
            for (const auto& source : job.sources) {
                source_ptrs.insert(source.get());
            }
        }
        std::vector<std::shared_ptr<SstableMetadata>> retained;
        retained.reserve(sstable_metadata.size());
        for (const auto& sstable : sstable_metadata) {
            if (source_ptrs.count(sstable.get()) != 0) {
                obsolete_sstables.push_back(sstable);
            } else {
                retained.push_back(sstable);
            }
        }
        sstable_metadata = std::move(retained);
        sstable_metadata.insert(sstable_metadata.end(), published.begin(),
                                published.end());
        current_files.clear();
        current_files.reserve(sstable_metadata.size());
        for (const auto& sstable : sstable_metadata) {
            current_files.push_back(sstable->path);
        }
        auto status = PublishManifestLocked(current_files);
        if (!status.ok) return status;
        ++compaction_count;
        return Status::Ok();
    }

    Status GarbageCollectLocked() {
        bool deleted = false;
        std::vector<std::shared_ptr<SstableMetadata>> retained;
        retained.reserve(obsolete_sstables.size());
        for (const auto& obsolete : obsolete_sstables) {
            if (obsolete->refs == 0) {
                std::error_code ec;
                std::filesystem::remove(obsolete->path, ec);
                deleted = true;
            } else {
                retained.push_back(obsolete);
            }
        }
        obsolete_sstables = std::move(retained);
        if (deleted && !SyncDirectory(root / "sst")) {
            return Status::Error("GC directory sync failed");
        }
        return Status::Ok();
    }

    Status PublishManifestLocked(
        const std::vector<std::filesystem::path>& files) {
        const auto manifest_tmp = manifest_path.string() + ".tmp";
        {
            std::ofstream manifest(manifest_tmp, std::ios::trunc);
            if (!manifest) return Status::Error("cannot create Manifest");
            std::string manifest_text;
            manifest_text += std::to_string(cutoff) + " " +
                             std::to_string(next_version) + " " +
                             std::to_string(next_file_number) + "\n";
            manifest_text +=
                std::string("WAL_FORMAT ") + (sharded_wal ? "2" : "1") + "\n";
            for (size_t shard_id = 0;
                 shard_id < covered_commit_versions.size(); ++shard_id) {
                manifest_text += "COVERED " + std::to_string(shard_id) + " " +
                                 std::to_string(
                                     covered_commit_versions[shard_id]) +
                                 "\n";
            }
            for (size_t i = 0; i < files.size(); ++i) {
                manifest_text +=
                    std::to_string(i) + " " +
                    std::filesystem::relative(files[i], root).string() + "\n";
            }
            manifest << manifest_text;
            manifest << "CHECKSUM " << Checksum(manifest_text) << '\n';
        }
        if (!SyncFile(manifest_tmp)) return Status::Error("Manifest sync failed");
        std::error_code ec;
        std::filesystem::rename(manifest_tmp, manifest_path, ec);
        if (ec) return Status::Error("Manifest publish failed");
        if (!SyncDirectory(root)) {
            return Status::Error("Manifest directory sync failed");
        }
        const auto current_tmp = root / "CURRENT.tmp";
        {
            std::ofstream current(current_tmp, std::ios::trunc);
            if (!current) return Status::Error("cannot create CURRENT");
            current << "MANIFEST\n";
            current.close();
        }
        if (!SyncFile(current_tmp)) return Status::Error("CURRENT sync failed");
        std::error_code current_ec;
        std::filesystem::rename(current_tmp, root / "CURRENT", current_ec);
        if (current_ec) return Status::Error("CURRENT publish failed");
        if (!SyncDirectory(root)) {
            return Status::Error("CURRENT directory sync failed");
        }
        return Status::Ok();
    }

    Status ReadSstableMetadata(const std::filesystem::path& path,
                               size_t shard_id,
                               std::shared_ptr<SstableMetadata>* metadata) {
        std::ifstream in(path, std::ios::binary);
        if (!in) return Status::Error("cannot read SSTable metadata");
        uint32_t magic = 0;
        uint64_t count = 0;
        in.read(reinterpret_cast<char*>(&magic), sizeof(magic));
        in.read(reinterpret_cast<char*>(&count), sizeof(count));
        if (!in || (magic != kWalMagic && magic != kEncodedSstableMagic)) {
            return Status::Error("invalid SSTable metadata");
        }
        auto result = std::make_shared<SstableMetadata>();
        result->path = path;
        result->shard_id = shard_id;
        result->point_count = count;
        result->encoding = magic;
        if (magic == kEncodedSstableMagic) {
            StreamingReader reader(path);
            reader.in.seekg(sizeof(uint32_t) + sizeof(uint64_t),
                            std::ios::beg);
            if (!reader.in) return Status::Error("cannot read SSTable");
            for (uint64_t i = 0; i < count; ++i) {
                const uint64_t record_offset =
                    sizeof(uint32_t) + sizeof(uint64_t) + reader.bytes;
                uint64_t series_id = 0;
                uint64_t zigzag_timestamp = 0;
                double value = 0.0;
                uint64_t version = 0;
                if (!reader.ReadUvarint(&series_id) ||
                    !reader.ReadUvarint(&zigzag_timestamp) ||
                    !reader.ReadDouble(&value) ||
                    !reader.ReadUvarint(&version)) {
                    return Status::Error("truncated encoded SSTable");
                }
                const int64_t timestamp = ZigzagDecode(zigzag_timestamp);
                auto& entry = result->series[series_id];
                entry.series_id = series_id;
                if (entry.point_count == 0) entry.offset = record_offset;
                ++entry.point_count;
                entry.byte_length = sizeof(uint32_t) + sizeof(uint64_t) +
                                    reader.bytes - entry.offset;
                entry.min_timestamp = std::min(entry.min_timestamp, timestamp);
                entry.max_timestamp = std::max(entry.max_timestamp, timestamp);
                result->min_timestamp =
                    std::min(result->min_timestamp, timestamp);
                result->max_timestamp =
                    std::max(result->max_timestamp, timestamp);
            }
            uint64_t recorded_checksum = 0;
            reader.in.read(reinterpret_cast<char*>(&recorded_checksum),
                           sizeof(recorded_checksum));
            if (!reader.in || reader.hash != recorded_checksum) {
                return Status::Error("SSTable checksum mismatch");
            }
            *metadata = std::move(result);
            return Status::Ok();
        }
        uint64_t hash = 1469598103934665603ULL;
        for (uint64_t i = 0; i < count; ++i) {
            uint64_t series_id = 0;
            int64_t timestamp = 0;
            double value = 0.0;
            CommitVersion version = 0;
            in.read(reinterpret_cast<char*>(&series_id), sizeof(uint64_t));
            in.read(reinterpret_cast<char*>(&timestamp), sizeof(int64_t));
            in.read(reinterpret_cast<char*>(&value), sizeof(double));
            in.read(reinterpret_cast<char*>(&version), sizeof(CommitVersion));
            if (!in) return Status::Error("truncated SSTable metadata");
            char raw[sizeof(uint64_t) + sizeof(int64_t) +
                     sizeof(double) + sizeof(CommitVersion)];
            std::memcpy(raw, &series_id, sizeof(uint64_t));
            std::memcpy(raw + sizeof(uint64_t), &timestamp, sizeof(int64_t));
            std::memcpy(raw + sizeof(uint64_t) + sizeof(int64_t),
                        &value, sizeof(double));
            std::memcpy(raw + sizeof(uint64_t) + sizeof(int64_t) +
                            sizeof(double),
                        &version, sizeof(CommitVersion));
            hash = HashRecord(hash, raw, sizeof(raw));
            auto& entry = result->series[series_id];
            entry.series_id = series_id;
            if (entry.point_count == 0) {
                entry.offset =
                    sizeof(uint32_t) + sizeof(uint64_t) +
                    i * (sizeof(uint64_t) + sizeof(int64_t) +
                         sizeof(double) + sizeof(CommitVersion));
            }
            ++entry.point_count;
            entry.byte_length = entry.point_count *
                                (sizeof(uint64_t) + sizeof(int64_t) +
                                 sizeof(double) + sizeof(CommitVersion));
            entry.min_timestamp = std::min(entry.min_timestamp, timestamp);
            entry.max_timestamp = std::max(entry.max_timestamp, timestamp);
            result->min_timestamp =
                std::min(result->min_timestamp, timestamp);
            result->max_timestamp =
                std::max(result->max_timestamp, timestamp);
        }
        uint64_t checksum = 0;
        in.read(reinterpret_cast<char*>(&checksum), sizeof(checksum));
        if (!in || checksum != hash) {
            return Status::Error("SSTable metadata checksum mismatch");
        }
        *metadata = std::move(result);
        return Status::Ok();
    }

    Status LoadManifest() {
        std::filesystem::path active_manifest = manifest_path;
        const auto current_path = root / "CURRENT";
        if (std::filesystem::exists(current_path)) {
            std::string name;
            std::ifstream current(current_path);
            std::getline(current, name);
            if (name == "MANIFEST") active_manifest = manifest_path;
        }
        if (!std::filesystem::exists(active_manifest)) return Status::Ok();
        std::ifstream in(active_manifest);
        if (!in) return Status::Error("cannot read Manifest");
        size_t shard_id = 0;
        std::string file;
        std::string header;
        std::getline(in, header);
        std::istringstream header_in(header);
        header_in >> cutoff >> next_version;
        if (!(header_in >> next_file_number)) next_file_number = 1;
        std::string manifest_payload = header + "\n";
        std::string line;
        uint64_t recorded_checksum = 0;
        bool checksum_seen = false;
        while (std::getline(in, line)) {
            if (line.rfind("CHECKSUM ", 0) == 0) {
                if (checksum_seen) return Status::Error("duplicate Manifest checksum");
                std::istringstream checksum_line(line);
                std::string label;
                checksum_line >> label >> recorded_checksum;
                checksum_seen = true;
                continue;
            }
            if (checksum_seen) return Status::Error("data after Manifest checksum");
            manifest_payload += line + "\n";
            if (line.rfind("WAL_FORMAT ", 0) == 0) {
                std::istringstream format_line(line);
                std::string label;
                uint32_t format = 1;
                format_line >> label >> format;
                wal_format = format;
                continue;
            }
            if (line.rfind("COVERED ", 0) == 0) {
                size_t shard_id_line = 0;
                CommitVersion covered = 0;
                std::istringstream covered_line(line);
                std::string label;
                covered_line >> label >> shard_id_line >> covered;
                if (shard_id_line < covered_commit_versions.size()) {
                    covered_commit_versions[shard_id_line] = covered;
                }
                continue;
            }
            std::istringstream row(line);
            if (!(row >> shard_id >> file)) return Status::Error("invalid Manifest row");
            const std::string filename = std::filesystem::path(file).filename().string();
            const std::string prefix = "shard-";
            size_t parsed_shard = shard_id;
            if (filename.rfind(prefix, 0) == 0) {
                try {
                    parsed_shard = std::stoull(filename.substr(prefix.size(), 2));
                } catch (...) {
                    return Status::Error("invalid SSTable shard name");
                }
            }
            if (parsed_shard >= shards.size()) return Status::Error("invalid shard in Manifest");
            std::shared_ptr<SstableMetadata> metadata;
            auto status = ReadSstableMetadata(root / file, parsed_shard, &metadata);
            if (!status.ok) return status;
            sstable_metadata.push_back(metadata);
            for (const auto& [series_id, entry] : metadata->series) {
                auto it = last_timestamps.find(series_id);
                if (it == last_timestamps.end() ||
                    entry.max_timestamp > it->second) {
                    last_timestamps[series_id] = entry.max_timestamp;
                }
            }
            current_files.push_back(root / file);
        }
        CommitVersion min_covered = 0;
        if (!covered_commit_versions.empty()) {
            min_covered = covered_commit_versions[0];
            for (const CommitVersion covered : covered_commit_versions) {
                min_covered = std::min(min_covered, covered);
            }
        }
        last_flushed_version = min_covered;
        if (!checksum_seen ||
            recorded_checksum == 0 ||
            recorded_checksum != Checksum(manifest_payload)) {
            return Status::Error("Manifest checksum mismatch");
        }
        return Status::Ok();
    }

    // Parses one WAL transaction payload and applies it. Returns false when the
    // payload is malformed (a torn tail), in which case nothing is applied.
    bool ApplyWalTxn(const std::string& payload, CommitVersion* version) {
        size_t payload_offset = 0;
        uint32_t encoded_magic = 0;
        if (Read(payload, payload_offset, &encoded_magic) &&
            encoded_magic == kEncodedWalPayloadMagic) {
            if (!Read(payload, payload_offset, version)) return false;
            uint32_t count = 0;
            if (!Read(payload, payload_offset, &count)) return false;
            if (count > payload.size()) return false;
            std::vector<DataPoint> points(count);
            for (auto& point : points) {
                uint64_t series_id = 0;
                uint64_t zigzag_timestamp = 0;
                if (!ReadUvarint(payload, payload_offset, &series_id) ||
                    !ReadUvarint(payload, payload_offset, &zigzag_timestamp) ||
                    !Read(payload, payload_offset, &point.value)) {
                    return false;
                }
                point.series_id = series_id;
                point.timestamp_ms = ZigzagDecode(zigzag_timestamp);
            }
            ApplyPoints(points, *version);
            return true;
        }
        payload_offset = 0;
        if (!Read(payload, payload_offset, version)) return false;
        uint32_t count = 0;
        if (!Read(payload, payload_offset, &count)) return false;
        if (count > payload.size()) return false;
        std::vector<DataPoint> points(count);
        for (auto& point : points) {
            if (!Read(payload, payload_offset, &point.series_id) ||
                !Read(payload, payload_offset, &point.timestamp_ms) ||
                !Read(payload, payload_offset, &point.value)) {
                return false;
            }
        }
        ApplyPoints(points, *version);
        return true;
    }

    std::vector<uint64_t> ListWalSegments() const {
        return ListSegmentNumbers(wal_dir);
    }

    // Replays complete records only. A torn tail (crash between the write and
    // the fsync, or a partially written record) is only legal in the newest
    // segment: it is truncated there, while older segments must be intact.
    bool ReplaySegment(const std::string& data, size_t* valid_bytes) {
        *valid_bytes = 0;
        size_t offset = 0;
        while (offset < data.size()) {
            const size_t record_start = offset;
            uint32_t magic = 0;
            uint8_t type = 0;
            uint64_t length = 0;
            uint64_t checksum = 0;
            if (!Read(data, offset, &magic) || !Read(data, offset, &type) ||
                !Read(data, offset, &length)) {
                break;
            }
            if (magic != kWalMagic) break;
            if (length > data.size() - offset ||
                offset + sizeof(checksum) + length > data.size()) {
                break;
            }
            if (!Read(data, offset, &checksum)) break;
            std::string payload = data.substr(offset, length);
            offset += length;
            if (checksum != Checksum(payload)) break;

            const size_t record_end = offset;
            size_t payload_offset = 0;
            CommitVersion version = 0;
            bool applied = false;
            if (type == kTxnRecord) {
                applied = ApplyWalTxn(payload, &version);
            } else if (type == kExpireRecord) {
                applied = Read(payload, payload_offset, &version) &&
                          Read(payload, payload_offset, &cutoff);
            }
            if (!applied) break;

            wal_bytes_replayed += record_end - record_start;
            next_version = std::max(next_version, version + 1);
            *valid_bytes = record_end;
        }
        return *valid_bytes == data.size();
    }

    Status ReplayWal() {
        const auto segments = ListWalSegments();
        for (size_t i = 0; i < segments.size(); ++i) {
            const auto path = WalSegmentPath(wal_dir, segments[i]);
            std::string data;
            if (!ReadFile(path, &data)) return Status::Error("cannot read WAL");
            size_t valid = 0;
            const bool complete = ReplaySegment(data, &valid);
            if (!complete && i + 1 != segments.size()) {
                return Status::Error("corrupt WAL segment");
            }
            wal_valid_bytes = valid;
            wal_segment = segments[i];
        }
        return Status::Ok();
    }

    // Called by a flush while it holds the global lock and after it published a
    // manifest covering every record written so far: the older segments can be
    // dropped, and later commits continue in a fresh one.
    bool RotateWalLocked() {
        const uint64_t next = wal_segment + 1;
        const auto path = WalSegmentPath(wal_dir, next);
        const int fd =
            ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_APPEND, 0644);
        if (fd < 0) return false;
        if (::fsync(fd) != 0) {
            ::close(fd);
            return false;
        }
        if (wal_fd >= 0) ::close(wal_fd);
        wal_fd = fd;
        wal_segment = next;
        // Deleting the retired segments does not depend on the directory sync
        // succeeding: the manifest already covers them either way.
        const bool synced = SyncDirectory(wal_dir);
        for (const uint64_t number : ListWalSegments()) {
            if (number >= wal_segment) continue;
            std::error_code ec;
            std::filesystem::remove(WalSegmentPath(wal_dir, number), ec);
        }
        return synced;
    }

    // ---- sharded WAL -------------------------------------------------------

    std::filesystem::path ShardWalDir(size_t shard_id) const {
        return shard_wal_dir / ("shard-" + std::to_string(shard_id));
    }

    bool ContainsCommitted(CommitVersion version) const {
        for (const auto& range : committed_ranges) {
            if (version >= range.first && version <= range.second) return true;
        }
        return false;
    }

    // Appends every request's points to the WAL of the shard they belong to,
    // fsyncs each touched shard once, then records one commit range covering
    // all versions of the group.
    bool AppendGroupSharded(
        const std::vector<std::shared_ptr<PendingWrite>>& group,
        CommitVersion first_version) {
        std::vector<std::vector<const PendingWrite*>> per_shard(shards.size());
        for (const auto& request : group) {
            std::set<size_t> touched;
            for (const auto& point : request->points) {
                touched.insert(ShardFor(point.series_id));
            }
            for (const size_t shard_id : touched) {
                per_shard[shard_id].push_back(request.get());
            }
        }
        bool ok = true;
        for (size_t shard_id = 0; shard_id < per_shard.size() && ok; ++shard_id) {
            if (per_shard[shard_id].empty()) continue;
            for (const PendingWrite* request : per_shard[shard_id]) {
                std::vector<DataPoint> points;
                for (const auto& point : request->points) {
                    if (ShardFor(point.series_id) == shard_id) points.push_back(point);
                }
                if (!AppendWalRecordNoSync(shard_wal_fd[shard_id], kTxnRecord,
                                           WalPayloadTxn(request->version, points))) {
                    ok = false;
                    break;
                }
            }
            if (ok) ok = ::fsync(shard_wal_fd[shard_id]) == 0;
            if (ok) ++wal_syncs;
        }
        if (!ok) return false;
        std::string payload;
        Append(payload, first_version);
        Append(payload, next_version - 1);
        if (!AppendWalRecordNoSync(commit_wal_fd, kCommitRangeRecord, payload)) {
            return false;
        }
        if (::fsync(commit_wal_fd) != 0) return false;
        ++wal_syncs;
        return true;
    }

    Status ReplayCommitWal() {
        const auto segments = ListSegmentNumbers(commit_wal_dir);
        if (segments.empty()) return Status::Ok();
        for (size_t i = 0; i < segments.size(); ++i) {
            const auto path = WalSegmentPath(commit_wal_dir, segments[i]);
            std::string data;
            if (!ReadFile(path, &data)) return Status::Error("cannot read commit WAL");
            size_t offset = 0;
            size_t valid = 0;
            while (offset < data.size()) {
                uint32_t magic = 0;
                uint8_t type = 0;
                uint64_t length = 0;
                uint64_t checksum = 0;
                if (!Read(data, offset, &magic) || !Read(data, offset, &type) ||
                    !Read(data, offset, &length)) {
                    break;
                }
                if (magic != kWalMagic) break;
                if (length > data.size() - offset ||
                    offset + sizeof(checksum) + length > data.size()) {
                    break;
                }
                if (!Read(data, offset, &checksum)) break;
                std::string payload = data.substr(offset, length);
                offset += length;
                if (checksum != Checksum(payload)) break;
                size_t payload_offset = 0;
                if (type == kCommitRangeRecord) {
                    CommitVersion first = 0;
                    CommitVersion last = 0;
                    if (!Read(payload, payload_offset, &first) ||
                        !Read(payload, payload_offset, &last)) {
                        break;
                    }
                    committed_ranges.emplace_back(first, last);
                } else if (type == kExpireRecord) {
                    CommitVersion version = 0;
                    if (!Read(payload, payload_offset, &version) ||
                        !Read(payload, payload_offset, &cutoff)) {
                        break;
                    }
                    next_version = std::max(next_version, version + 1);
                } else {
                    break;
                }
                valid = offset;
            }
            const bool last = (i + 1 == segments.size());
            if (last) {
                std::error_code ec;
                const auto size = std::filesystem::file_size(path, ec);
                if (!ec && size > valid) {
                    (void)::truncate(path.c_str(), static_cast<off_t>(valid));
                }
                commit_wal_segment = segments[i];
            } else if (valid != data.size()) {
                return Status::Error("corrupt commit WAL segment");
            }
        }
        return Status::Ok();
    }

    Status ReplayShardWal(size_t shard_id) {
        const auto dir = ShardWalDir(shard_id);
        const auto segments = ListSegmentNumbers(dir);
        if (segments.empty()) {
            shard_wal_segment[shard_id] = 1;
            return Status::Ok();
        }
        for (size_t i = 0; i < segments.size(); ++i) {
            const auto path = WalSegmentPath(dir, segments[i]);
            std::string data;
            if (!ReadFile(path, &data)) return Status::Error("cannot read shard WAL");
            size_t offset = 0;
            size_t valid = 0;
            while (offset < data.size()) {
                uint32_t magic = 0;
                uint8_t type = 0;
                uint64_t length = 0;
                uint64_t checksum = 0;
                if (!Read(data, offset, &magic) || !Read(data, offset, &type) ||
                    !Read(data, offset, &length)) {
                    break;
                }
                if (magic != kWalMagic) break;
                if (length > data.size() - offset ||
                    offset + sizeof(checksum) + length > data.size()) {
                    break;
                }
                if (!Read(data, offset, &checksum)) break;
                std::string payload = data.substr(offset, length);
                offset += length;
                if (checksum != Checksum(payload)) break;
                size_t payload_offset = 0;
                uint32_t payload_magic = 0;
                CommitVersion version = 0;
                if (type != kTxnRecord) break;
                if (!Read(payload, payload_offset, &payload_magic) ||
                    payload_magic != kEncodedWalPayloadMagic ||
                    !Read(payload, payload_offset, &version)) {
                    break;
                }
                next_version = std::max(next_version, version + 1);
                // Only committed versions are replayed; uncommitted fragments
                // stay in the log but are skipped.
                if (ContainsCommitted(version)) {
                    if (!ApplyWalTxn(payload, &version)) break;
                }
                valid = offset;
            }
            const bool last = (i + 1 == segments.size());
            if (last) {
                std::error_code ec;
                const auto size = std::filesystem::file_size(path, ec);
                if (!ec && size > valid) {
                    (void)::truncate(path.c_str(), static_cast<off_t>(valid));
                }
            } else if (valid != data.size()) {
                return Status::Error("corrupt shard WAL segment");
            }
            shard_wal_segment[shard_id] = segments[i];
        }
        return Status::Ok();
    }

    Status ReplayShardedWal() {
        auto status = ReplayCommitWal();
        if (!status.ok) return status;
        for (size_t i = 0; i < shards.size(); ++i) {
            status = ReplayShardWal(i);
            if (!status.ok) return status;
        }
        return Status::Ok();
    }

    bool OpenShardedWalFds() {
        for (size_t i = 0; i < shards.size(); ++i) {
            const auto dir = ShardWalDir(i);
            std::error_code ec;
            std::filesystem::create_directories(dir, ec);
            if (ec) return false;
            const auto path = WalSegmentPath(dir, shard_wal_segment[i]);
            shard_wal_fd[i] = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
            if (shard_wal_fd[i] < 0) return false;
        }
        std::error_code ec;
        std::filesystem::create_directories(commit_wal_dir, ec);
        if (ec) return false;
        const auto path = WalSegmentPath(commit_wal_dir, commit_wal_segment);
        commit_wal_fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
        return commit_wal_fd >= 0;
    }

    // Called by a flush while holding the lock and after the manifest covered
    // every record written so far.
    bool RotateShardedWalLocked() {
        auto rotate = [](const std::filesystem::path& dir, uint64_t* segment,
                         int* fd) {
            const uint64_t next = *segment + 1;
            const auto path = WalSegmentPath(dir, next);
            const int new_fd =
                ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_APPEND, 0644);
            if (new_fd < 0) return false;
            if (::fsync(new_fd) != 0) {
                ::close(new_fd);
                return false;
            }
            if (*fd >= 0) ::close(*fd);
            *fd = new_fd;
            *segment = next;
            for (const uint64_t number : ListSegmentNumbers(dir)) {
                if (number >= *segment) continue;
                std::error_code ec;
                std::filesystem::remove(WalSegmentPath(dir, number), ec);
            }
            return true;
        };
        for (size_t i = 0; i < shards.size(); ++i) {
            if (!rotate(ShardWalDir(i), &shard_wal_segment[i], &shard_wal_fd[i])) {
                return false;
            }
        }
        return rotate(commit_wal_dir, &commit_wal_segment, &commit_wal_fd);
    }
};

Engine::Engine(std::shared_ptr<Impl> impl) : impl_(std::move(impl)) {
    impl_->owner = this;
    impl_->write_worker = std::thread([this] {
        PinCurrentThread(impl_->options.cpu_budget, 0);
        while (true) {
            std::unique_lock lock(impl_->mutex);
            impl_->write_cv.wait_for(lock, std::chrono::microseconds(200), [&] {
                return !impl_->write_queue.empty() || impl_->stopping_write;
            });
            if (impl_->stopping_write && impl_->write_queue.empty()) return;
            if (impl_->write_queue.empty()) continue;

            // Collect a group before committing. When several writers are
            // already waiting, block until they have all queued their batch so
            // that one WAL fsync covers all of them; with a single writer there
            // is nothing to wait for, so the fast path pays no extra latency.
            // The lock is released while waiting, so the writers can enqueue.
            const size_t target = std::min<size_t>(impl_->active_writers, 8);
            if (target > 1) {
                impl_->write_cv.wait_for(lock, std::chrono::milliseconds(2), [&] {
                    return impl_->write_queue.size() >= target ||
                           impl_->stopping_write;
                });
            }

            std::vector<std::shared_ptr<PendingWrite>> group;
            size_t queued_bytes = 0;
            while (!impl_->write_queue.empty() &&
                   group.size() < 64 &&
                   queued_bytes < 256 * 1024) {
                auto request = impl_->write_queue.front();
                impl_->write_queue.pop_front();
                queued_bytes += request->points.size() * sizeof(DataPoint);
                group.push_back(std::move(request));
            }

            bool ok = true;
            for (const auto& request : group) {
                request->version = impl_->next_version++;
            }
            const CommitVersion first_version = group.front()->version;
            if (impl_->sharded_wal) {
                ok = impl_->AppendGroupSharded(group, first_version);
            } else {
                off_t wal_record_start = ::lseek(impl_->wal_fd, 0, SEEK_END);
                for (const auto& request : group) {
                    request->wal_start = ::lseek(impl_->wal_fd, 0, SEEK_END);
                    if (!AppendWalRecordNoSync(
                            impl_->wal_fd, kTxnRecord,
                            WalPayloadTxn(request->version, request->points))) {
                        ok = false;
                        break;
                    }
                    request->wal_end = ::lseek(impl_->wal_fd, 0, SEEK_END);
                }
                if (ok) ok = ::fsync(impl_->wal_fd) == 0;
                if (ok) ++impl_->wal_syncs;
                if (!ok && wal_record_start >= 0) {
                    TruncateWalBestEffort(impl_->wal_fd, wal_record_start);
                }
            }
            if (ok) {
                // Hand the batch to the shard workers and wait for every shard
                // to apply it; only then does the commit become visible.
                impl_->DispatchGroup(group);
                impl_->visible_version = impl_->next_version - 1;
                for (const auto& request : group) {
                    impl_->points_since_flush += request->points.size();
                    request->status = Status::Ok();
                    request->done = true;
                }
            } else {
                impl_->next_version -= group.size();
                for (const auto& request : group) {
                    request->status = Status::Error("WAL sync failed");
                    request->done = true;
                }
            }
            impl_->write_cv.notify_all();
        }
    });
    impl_->flush_worker = std::thread([this] {
        PinCurrentThread(impl_->options.cpu_budget, 1);
        while (true) {
            std::unique_lock lock(impl_->mutex);
            impl_->flush_cv.wait(lock, [&] {
                return impl_->flush_requested || impl_->stopping_flush;
            });
            if (impl_->stopping_flush) return;
            impl_->flush_requested = false;
            const bool flushed = FlushLocked(true).ok;
            if (flushed && impl_->options.enable_background_gc) {
                (void)impl_->GarbageCollectLocked();
            }
            if (flushed && impl_->options.enable_background_compaction &&
                impl_->ShouldCompactLocked(
                    impl_->options.compaction_sstable_threshold)) {
                impl_->compaction_requested = true;
                impl_->compaction_cv.notify_one();
            }
        }
    });
    if (impl_->options.enable_background_compaction) {
        impl_->compaction_worker = std::thread([this] {
            PinCurrentThread(impl_->options.cpu_budget, 2);
            while (true) {
                std::unique_lock lock(impl_->mutex);
                impl_->compaction_cv.wait(lock, [&] {
                    return impl_->compaction_requested ||
                           impl_->stopping_compaction;
                });
                if (impl_->stopping_compaction) return;
                impl_->compaction_requested = false;
                // Bounded per wakeup: foreground flushing must not be starved by
                // a merge loop that is refilled as fast as it drains.
                for (size_t round = 0;
                     round < 32 &&
                     impl_->ShouldCompactLocked(
                         impl_->options.compaction_sstable_threshold);
                     ++round) {
                    if (!CompactRoundLocked(
                             lock,
                             impl_->options.compaction_sstable_threshold)
                             .ok) {
                        break;
                    }
                }
                if (impl_->options.enable_background_gc) {
                    (void)impl_->GarbageCollectLocked();
                }
            }
        });
    }
    for (size_t i = 0; i < impl_->shards.size(); ++i) {
        impl_->shard_worker.emplace_back([this, i] {
            // Shard workers are bursty (they sleep between commits); pinning
            // them to the same cores as the coordinator and readers only adds
            // contention, so they stay unpinned inside the 4-core budget.
            while (true) {
                std::shared_ptr<ShardFragment> fragment;
                {
                    std::unique_lock lock(*impl_->shard_mutex[i]);
                    impl_->shard_cv[i]->wait(lock, [&] {
                        return impl_->shard_stopping[i] ||
                               !impl_->shard_queue[i].empty();
                    });
                    if (impl_->shard_queue[i].empty()) {
                        if (impl_->shard_stopping[i]) return;
                        continue;
                    }
                    fragment = impl_->shard_queue[i].front();
                    impl_->shard_queue[i].pop_front();
                }
                impl_->ApplyFragment(i, *fragment);
                {
                    std::lock_guard guard(impl_->ack_mutex);
                    if (impl_->inflight_fragments > 0) --impl_->inflight_fragments;
                }
                impl_->ack_cv.notify_all();
            }
        });
    }
}

std::unique_ptr<Engine> Engine::Open(const std::filesystem::path& path,
                                     const Options& options, Status* status) {
    auto fail = [&](Status value) -> std::unique_ptr<Engine> {
        if (status) *status = std::move(value);
        return nullptr;
    };
    if (options.shard_count == 0) return fail(Status::Error("shard_count must be positive"));
    if (options.cpu_budget == 0 || options.cpu_budget > 4) {
        return fail(Status::Error("cpu_budget must be between 1 and 4"));
    }
    std::error_code ec;
    std::filesystem::create_directories(path / "sst", ec);
    std::filesystem::create_directories(path / "tmp", ec);
    if (ec) return fail(Status::Error("cannot create database directory"));
    std::error_code cleanup_ec;
    const auto sst_dir = path / "sst";
    if (std::filesystem::exists(sst_dir, cleanup_ec)) {
        for (const auto& entry :
             std::filesystem::directory_iterator(sst_dir, cleanup_ec)) {
            if (entry.is_regular_file() &&
                entry.path().extension() == ".tmp") {
                std::filesystem::remove(entry.path(), cleanup_ec);
            }
        }
    }
    auto impl = std::make_shared<Impl>(path, options);
    impl->wal_dir = path / "wal";
    impl->manifest_path = path / "MANIFEST";
    std::filesystem::create_directories(impl->wal_dir, ec);
    if (ec) return fail(Status::Error("cannot create WAL directory"));
    impl->sharded_wal = options.sharded_wal;
    impl->shard_wal_dir = impl->wal_dir;
    impl->commit_wal_dir = impl->wal_dir / "commit";
    impl->shards.resize(options.shard_count);
    for (auto& shard : impl->shards) shard = std::make_shared<ShardData>();
    impl->shard_mutex.resize(options.shard_count);
    impl->shard_cv.resize(options.shard_count);
    impl->shard_queue.resize(options.shard_count);
    impl->shard_stopping.assign(options.shard_count, false);
    for (size_t i = 0; i < options.shard_count; ++i) {
        impl->shard_mutex[i] = std::make_unique<std::mutex>();
        impl->shard_cv[i] = std::make_unique<std::condition_variable>();
    }
    impl->covered_commit_versions.assign(options.shard_count, 0);
    auto load_status = impl->LoadManifest();
    if (!load_status.ok) return fail(load_status);
    if (std::filesystem::exists(impl->manifest_path) &&
        impl->wal_format != (options.sharded_wal ? 2u : 1u)) {
        return fail(Status::Error("WAL format mismatch"));
    }
    if (options.sharded_wal) {
        impl->shard_wal_fd.assign(options.shard_count, -1);
        impl->shard_wal_segment.assign(options.shard_count, 1);
        load_status = impl->ReplayShardedWal();
        if (!load_status.ok) return fail(load_status);
        if (!impl->OpenShardedWalFds()) {
            return fail(Status::Error("cannot open shard WAL"));
        }
        impl->visible_version = impl->next_version - 1;
        if (status) *status = Status::Ok();
        return std::unique_ptr<Engine>(new Engine(std::move(impl)));
    }
    load_status = impl->ReplayWal();
    if (!load_status.ok) return fail(load_status);
    impl->visible_version = impl->next_version - 1;
    // Drop a torn tail in the newest segment before appending again: otherwise
    // the next crash would leave a valid record behind a malformed one and the
    // log could never be replayed again.
    const auto active_path = WalSegmentPath(impl->wal_dir, impl->wal_segment);
    if (std::filesystem::exists(active_path)) {
        std::error_code size_ec;
        const auto wal_size = std::filesystem::file_size(active_path, size_ec);
        if (!size_ec && wal_size > impl->wal_valid_bytes &&
            ::truncate(active_path.c_str(),
                       static_cast<off_t>(impl->wal_valid_bytes)) != 0) {
            return fail(Status::Error("cannot truncate WAL tail"));
        }
    }
    impl->wal_fd = ::open(active_path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (impl->wal_fd < 0) return fail(Status::Error("cannot open WAL"));
    if (status) *status = Status::Ok();
    return std::unique_ptr<Engine>(new Engine(std::move(impl)));
}

Engine::~Engine() {
    if (impl_ && !impl_->closed) Close();
}

Status Engine::Put(const DataPoint& point, CommitVersion* version) {
    return WriteBatch({point}, version);
}

Status Engine::WriteBatch(const std::vector<DataPoint>& points,
                          CommitVersion* version) {
    std::unique_lock lock(impl_->mutex);
    if (impl_->closed || impl_->closing) return Status::Error("engine is closed");
    if (impl_->write_queue.size() >= impl_->options.max_pending_batches) {
        return Status::Error("write queue pressure");
    }
    const size_t batch_bytes =
        points.size() * (sizeof(DataPoint) + sizeof(CommitVersion));
    for (const auto& point : points) {
        const uint64_t projected =
            impl_->hot_series_points[point.series_id] + 1;
        if (projected > impl_->options.hot_series_point_limit) {
            impl_->flush_requested = true;
            impl_->flush_cv.notify_one();
            return Status::Error("hot series pressure");
        }
    }
    if (impl_->memtable_bytes + batch_bytes >
        impl_->options.max_memtable_bytes) {
        impl_->flush_requested = true;
        impl_->flush_cv.notify_one();
        return Status::Error("memtable pressure");
    }
    auto status = impl_->ValidateBatch(points);
    if (!status.ok) return status;
    if (impl_->points_since_flush + points.size() > impl_->options.memtable_max_points) {
        impl_->flush_requested = true;
        impl_->flush_cv.notify_one();
    }
    auto request = std::make_shared<PendingWrite>();
    request->points = points;
    impl_->write_queue.push_back(request);
    // Lets the coordinator know how many writers a group can contain, so it
    // waits for them instead of committing one request per WAL fsync.
    ++impl_->active_writers;
    impl_->write_cv.notify_one();
    impl_->write_cv.wait(lock, [&] { return request->done; });
    --impl_->active_writers;
    if (request->status.ok && version) *version = request->version;
    return request->status;
}

Status Engine::Scan(const std::vector<uint64_t>& series_ids, int64_t start,
                    int64_t end, std::unique_ptr<Cursor>* cursor) const {
    if (!cursor) return Status::Error("cursor output is null");
    if (start > end) return Status::Error("start must not exceed end");
    std::unique_lock lock(impl_->mutex);
    if (impl_->closed || impl_->closing) return Status::Error("engine is closed");
    const auto view = impl_->ViewLocked();
    ++impl_->active_read_views;
    // The view (shard snapshots + referenced SSTables) is immutable, so the
    // cursor decodes and merges without the global lock: a long scan no longer
    // blocks writers, and its memory does not grow with the result size.
    lock.unlock();

    // Bound the number of cursors that can hold decoder state at once.
    {
        std::unique_lock gate(impl_->scan_mu);
        impl_->scan_cv.wait(gate, [&] {
            return impl_->active_scans < Impl::kMaxConcurrentScans;
        });
        ++impl_->active_scans;
    }

    // Streaming scan: one source per (series, SSTable) plus one per series for
    // the memtable snapshot. Sources decode on demand and are merged by
    // (timestamp, series_id), so memory is proportional to the number of
    // sources rather than to the number of points returned.
    struct Source {
        uint64_t series_id{};
        std::shared_ptr<const SeriesPoints> memtable;
        SeriesPoints::const_iterator mem_it;
        SeriesPoints::const_iterator mem_end;
        std::unique_ptr<StreamingReader> file;
        uint64_t remaining{0};
        // Legacy raw (fixed record) SSTables are still readable.
        bool raw{false};
        std::unique_ptr<std::ifstream> raw_file;
        std::streamoff raw_offset{0};
        uint64_t raw_index{0};
    };
    struct Item {
        int64_t timestamp{};
        uint64_t series_id{};
        double value{};
        CommitVersion version{};
        size_t source{};
    };
    struct ItemGreater {
        bool operator()(const Item& left, const Item& right) const {
            return std::tie(left.timestamp, left.series_id) >
                   std::tie(right.timestamp, right.series_id);
        }
    };
    struct StreamState {
        ReadView view;
        std::vector<std::shared_ptr<Source>> sources;
        std::priority_queue<Item, std::vector<Item>, ItemGreater> heap;
        int64_t start{};
        int64_t end{};
    };

    auto state = std::make_shared<StreamState>();
    state->view = view;
    state->start = start;
    state->end = end;

    std::set<uint64_t> unique_series(series_ids.begin(), series_ids.end());
    for (const auto series_id : unique_series) {
        const size_t shard_id = impl_->ShardFor(series_id);
        const auto& shard_series = view.shards[shard_id]->series;
        const auto series_it = shard_series.find(series_id);
        if (series_it != shard_series.end() && series_it->second) {
            auto source = std::make_shared<Source>();
            source->series_id = series_id;
            source->memtable = series_it->second;
            source->mem_it = series_it->second->begin();
            source->mem_end = series_it->second->end();
            state->sources.push_back(std::move(source));
        }
        for (const auto& sstable : view.sstables) {
            if (sstable->shard_id != shard_id) continue;
            const auto entry_it = sstable->series.find(series_id);
            if (entry_it == sstable->series.end()) continue;
            const auto& entry = entry_it->second;
            if (entry.max_timestamp < start || entry.min_timestamp >= end) continue;
            auto source = std::make_shared<Source>();
            source->series_id = series_id;
            if (sstable->encoding == kEncodedSstableMagic) {
                source->file = std::make_unique<StreamingReader>(sstable->path);
                if (!source->file->in) continue;
                source->file->in.seekg(entry.offset, std::ios::beg);
                if (!source->file->in) continue;
                source->remaining = entry.point_count;
            } else {
                source->raw = true;
                source->raw_file = std::make_unique<std::ifstream>(
                    sstable->path, std::ios::binary);
                if (!*source->raw_file) continue;
                source->raw_offset = static_cast<std::streamoff>(entry.offset);
                source->remaining = entry.point_count;
            }
            state->sources.push_back(std::move(source));
        }
    }

    auto advance = [state](size_t index) {
        auto& source = *state->sources[index];
        const int64_t cutoff = state->view.cutoff;
        const CommitVersion visible = state->view.version;
        while (true) {
            Item item;
            item.source = index;
            item.series_id = source.series_id;
            if (source.memtable) {
                while (source.mem_it != source.mem_end &&
                       (source.mem_it->first < state->start ||
                        source.mem_it->first >= state->end ||
                        source.mem_it->first < cutoff ||
                        source.mem_it->second.version > visible)) {
                    ++source.mem_it;
                }
                if (source.mem_it == source.mem_end) return;
                item.timestamp = source.mem_it->first;
                item.value = source.mem_it->second.point.value;
                item.version = source.mem_it->second.version;
                ++source.mem_it;
                state->heap.push(item);
                return;
            }
            if (source.remaining == 0) return;
            if (source.raw) {
                constexpr size_t kRecord = sizeof(uint64_t) + sizeof(int64_t) +
                                           sizeof(double) + sizeof(CommitVersion);
                char record[kRecord];
                source.raw_file->seekg(
                    source.raw_offset +
                        static_cast<std::streamoff>(source.raw_index * kRecord),
                    std::ios::beg);
                source.raw_file->read(record, kRecord);
                if (!*source.raw_file) {
                    source.remaining = 0;
                    return;
                }
                ++source.raw_index;
                --source.remaining;
                uint64_t sid = 0;
                int64_t timestamp = 0;
                double value = 0.0;
                CommitVersion version = 0;
                std::memcpy(&sid, record, sizeof(uint64_t));
                std::memcpy(&timestamp, record + sizeof(uint64_t),
                            sizeof(int64_t));
                std::memcpy(&value,
                            record + sizeof(uint64_t) + sizeof(int64_t),
                            sizeof(double));
                std::memcpy(&version,
                            record + sizeof(uint64_t) + sizeof(int64_t) +
                                sizeof(double),
                            sizeof(CommitVersion));
                if (timestamp >= state->end) {
                    source.remaining = 0;
                    return;
                }
                if (timestamp < state->start || timestamp < cutoff ||
                    version > visible) {
                    continue;
                }
                item.timestamp = timestamp;
                item.value = value;
                item.version = version;
                state->heap.push(item);
                return;
            }
            uint64_t disk_series = 0;
            uint64_t zigzag_timestamp = 0;
            double disk_value = 0.0;
            uint64_t disk_version = 0;
            if (!source.file->ReadUvarint(&disk_series) ||
                !source.file->ReadUvarint(&zigzag_timestamp) ||
                !source.file->ReadDouble(&disk_value) ||
                !source.file->ReadUvarint(&disk_version)) {
                source.remaining = 0;
                return;
            }
            --source.remaining;
            const int64_t timestamp = ZigzagDecode(zigzag_timestamp);
            if (timestamp >= state->end) {
                source.remaining = 0;
                return;
            }
            if (timestamp < state->start || timestamp < cutoff ||
                disk_version > visible) {
                continue;
            }
            item.timestamp = timestamp;
            item.value = disk_value;
            item.version = static_cast<CommitVersion>(disk_version);
            state->heap.push(item);
            return;
        }
    };
    for (size_t i = 0; i < state->sources.size(); ++i) advance(i);

    *cursor = std::make_unique<Cursor>(
        [state, advance]() mutable -> std::optional<DataPoint> {
            if (state->heap.empty()) return std::nullopt;
            Item best = state->heap.top();
            state->heap.pop();
            advance(best.source);
            // Duplicate timestamps of one series may live in several SSTables;
            // emit a single point, keeping the newest commit version.
            while (!state->heap.empty() &&
                   state->heap.top().timestamp == best.timestamp &&
                   state->heap.top().series_id == best.series_id) {
                const Item duplicate = state->heap.top();
                state->heap.pop();
                if (duplicate.version > best.version) best = duplicate;
                advance(duplicate.source);
            }
            return DataPoint{best.series_id, best.timestamp, best.value};
        },
        [impl = impl_, state] {
            {
                std::lock_guard lock(impl->mutex);
                if (impl->active_read_views > 0) --impl->active_read_views;
                for (const auto& sstable : state->view.sstables) {
                    if (sstable->refs > 0) --sstable->refs;
                }
            }
            {
                std::lock_guard gate(impl->scan_mu);
                if (impl->active_scans > 0) --impl->active_scans;
            }
            impl->scan_cv.notify_one();
        });
    return Status::Ok();
}

Status Engine::Aggregate(const std::vector<uint64_t>& series_ids, int64_t start,
                         int64_t end, int64_t window_ms, AggregateOp operation,
                         std::unique_ptr<Cursor>* cursor) const {
    if (window_ms <= 0) return Status::Error("window_ms must be positive");
    if (start > end) return Status::Error("start must not exceed end");
    std::unique_ptr<Cursor> scan;
    auto status = Scan(series_ids, start, end, &scan);
    if (!status.ok) return status;
    struct Bucket {
        uint64_t count{0};
        double sum{0.0};
        double min{std::numeric_limits<double>::infinity()};
        double max{-std::numeric_limits<double>::infinity()};
    };
    std::map<std::pair<int64_t, uint64_t>, Bucket> buckets;
    while (auto point = scan->NextPoint()) {
        const int64_t offset = point->timestamp_ms - start;
        const int64_t quotient =
            offset >= 0 ? offset / window_ms
                        : -(((-offset) + window_ms - 1) / window_ms);
        const int64_t bucket = start + quotient * window_ms;
        auto& bucket_state = buckets[{bucket, point->series_id}];
        ++bucket_state.count;
        bucket_state.sum += point->value;
        bucket_state.min = std::min(bucket_state.min, point->value);
        bucket_state.max = std::max(bucket_state.max, point->value);
    }
    std::vector<AggregateResult> results;
    for (auto& [key, bucket_state] : buckets) {
        double result = 0.0;
        if (operation == AggregateOp::Count) {
            result = static_cast<double>(bucket_state.count);
        } else if (operation == AggregateOp::Sum || operation == AggregateOp::Avg) {
            result = bucket_state.sum;
            if (operation == AggregateOp::Avg) result /= bucket_state.count;
        } else if (operation == AggregateOp::Min) {
            result = bucket_state.min;
        } else {
            result = bucket_state.max;
        }
        results.push_back({key.second, key.first,
                           std::min(end, key.first + window_ms), result,
                           bucket_state.count});
    }
    *cursor = std::make_unique<Cursor>(std::move(results));
    return Status::Ok();
}

Status Engine::ExpireBefore(int64_t cutoff, CommitVersion* version) {
    std::unique_lock lock(impl_->mutex);
    if (impl_->closed || impl_->closing) return Status::Error("engine is closed");
    if (cutoff < impl_->cutoff) return Status::Error("cutoff must not move backwards");
    if (cutoff == impl_->cutoff) {
        if (version) *version = impl_->next_version - 1;
        return Status::Ok();
    }
    impl_->write_cv.wait(lock, [&] {
        return impl_->write_queue.empty();
    });
    const CommitVersion commit = impl_->next_version++;
    const int expire_fd =
        impl_->sharded_wal ? impl_->commit_wal_fd : impl_->wal_fd;
    if (!AppendWalRecord(expire_fd, kExpireRecord,
                         WalPayloadExpire(commit, cutoff))) {
        --impl_->next_version;
        return Status::Error("WAL sync failed");
    }
    ++impl_->wal_syncs;
    impl_->cutoff = cutoff;
    bool removed_sstable = false;
    std::vector<std::shared_ptr<SstableMetadata>> retained_sstables;
    for (const auto& sstable : impl_->sstable_metadata) {
        if (sstable->max_timestamp < cutoff) {
            removed_sstable = true;
            impl_->obsolete_sstables.push_back(sstable);
            for (auto it = impl_->current_files.begin();
                 it != impl_->current_files.end();) {
                if (*it == sstable->path) {
                    it = impl_->current_files.erase(it);
                } else {
                    ++it;
                }
            }
        } else {
            retained_sstables.push_back(sstable);
        }
    }
    impl_->sstable_metadata = std::move(retained_sstables);
    if (removed_sstable) {
        // Publish the shrunken file set before GC may unlink those SSTables: a
        // crash that leaves the old Manifest on disk would otherwise point at
        // files that are already gone and the engine could not reopen.
        // The expire itself is already durable in the WAL, so reporting the
        // error is enough - a retry with the same cutoff is a no-op.
        auto publish_status = impl_->PublishManifestLocked(impl_->current_files);
        if (!publish_status.ok) return publish_status;
    }
    if (impl_->options.enable_background_compaction) {
        impl_->compaction_requested = true;
        impl_->compaction_cv.notify_one();
    }
    constexpr size_t kRecordBytes = sizeof(DataPoint) + sizeof(CommitVersion);
    for (size_t shard_id = 0; shard_id < impl_->shards.size(); ++shard_id) {
        std::lock_guard shard_lock(*impl_->shard_mutex[shard_id]);
        impl_->shards[shard_id] =
            std::make_shared<ShardData>(*impl_->shards[shard_id]);
        auto& points_by_series = impl_->shards[shard_id]->series;
        for (auto series_it = points_by_series.begin();
             series_it != points_by_series.end();) {
            // Probe first: only clone the series that actually lose a point.
            bool expiring = false;
            for (const auto& [timestamp, _value] : *series_it->second) {
                if (timestamp < cutoff) {
                    expiring = true;
                    break;
                }
            }
            if (!expiring) {
                ++series_it;
                continue;
            }
            if (!series_it->second.unique()) {
                series_it->second =
                    std::make_shared<SeriesPoints>(*series_it->second);
            }
            auto& points = *series_it->second;
            size_t removed = 0;
            for (auto point_it = points.begin(); point_it != points.end();) {
                if (point_it->first < cutoff) {
                    point_it = points.erase(point_it);
                    ++removed;
                } else {
                    ++point_it;
                }
            }
            if (removed > 0) {
                impl_->memtable_bytes -= removed * kRecordBytes;
            }
            if (points.empty()) {
                series_it = points_by_series.erase(series_it);
            } else {
                ++series_it;
            }
        }
    }
    if (version) *version = commit;
    return Status::Ok();
}

Status Engine::FlushLocked(bool incremental) {
    (void)incremental;
    if (impl_->closed) return Status::Error("engine is closed");

    struct Job {
        size_t shard_id{};
        std::shared_ptr<ShardData> data;
        std::filesystem::path file;
        std::filesystem::path temp;
        bool written{false};
        Status status{Status::Ok()};
        std::shared_ptr<SstableMetadata> metadata;
    };

    std::vector<std::shared_ptr<Job>> jobs;
    for (size_t shard_id = 0; shard_id < impl_->shards.size(); ++shard_id) {
        auto job = std::make_shared<Job>();
        job->shard_id = shard_id;
        {
            std::lock_guard shard_lock(*impl_->shard_mutex[shard_id]);
            job->data = impl_->shards[shard_id];
        }
        std::string filename = "shard-";
        filename += shard_id < 10 ? "0" : "";
        filename += std::to_string(shard_id);
        filename += "-" + std::to_string(impl_->next_file_number) + ".sst";
        ++impl_->next_file_number;
        job->file = impl_->root / "sst" / filename;
        job->temp = job->file.string() + ".tmp";
        jobs.push_back(std::move(job));
    }

    // The per-shard SSTables are independent, so the four writes run
    // concurrently instead of rotating through one shard at a time.
    std::vector<std::thread> writers;
    for (const auto& job : jobs) {
        writers.emplace_back([this, job] {
            SstableWriter writer;
            if (!writer.Open(job->temp)) {
                job->status = Status::Error("cannot create SSTable");
                return;
            }
            for (const auto& [_, points] : job->data->series) {
                for (const auto& [__, value] : *points) {
                    if (value.version <= impl_->last_flushed_version) continue;
                    writer.Append(value.point.series_id,
                                  value.point.timestamp_ms, value.point.value,
                                  value.version);
                }
            }
            if (writer.count == 0) {
                writer.Discard();
                std::error_code discard_ec;
                std::filesystem::remove(job->temp, discard_ec);
                return;
            }
            if (!writer.Finish()) {
                std::error_code discard_ec;
                std::filesystem::remove(job->temp, discard_ec);
                job->status = Status::Error("SSTable write failed");
                return;
            }
            if (!SyncFile(job->temp)) {
                job->status = Status::Error("SSTable sync failed");
                return;
            }
            std::error_code ec;
            std::filesystem::rename(job->temp, job->file, ec);
            if (ec) {
                job->status = Status::Error("SSTable publish failed");
                return;
            }
            if (!SyncDirectory(job->file.parent_path())) {
                job->status = Status::Error("SSTable directory sync failed");
                return;
            }
            auto metadata =
                std::make_shared<SstableMetadata>(std::move(writer.metadata));
            metadata->path = job->file;
            metadata->shard_id = job->shard_id;
            metadata->level = 0;
            job->metadata = std::move(metadata);
            job->written = true;
        });
    }
    for (auto& writer : writers) writer.join();

    std::vector<std::filesystem::path> new_files;
    std::vector<std::shared_ptr<SstableMetadata>> published_metadata;
    for (const auto& job : jobs) {
        if (!job->status.ok) return job->status;
        if (!job->written) continue;
        new_files.push_back(job->file);
        published_metadata.push_back(job->metadata);
    }
    for (auto& covered : impl_->covered_commit_versions) {
        covered = impl_->next_version - 1;
    }
    impl_->metadata_covered_commit_version = impl_->next_version - 1;
    impl_->current_files.insert(impl_->current_files.end(),
                                new_files.begin(), new_files.end());
    auto status = impl_->PublishManifestLocked(impl_->current_files);
    if (!status.ok) return status;
    impl_->sstable_metadata.insert(impl_->sstable_metadata.end(),
                                   published_metadata.begin(),
                                   published_metadata.end());
    impl_->last_flushed_version = impl_->next_version - 1;
    impl_->points_since_flush = 0;
    impl_->hot_series_points.clear();
    impl_->ClearMemtableLocked();
    ++impl_->flush_count;
    // Best effort, and only after the in-memory state above is committed: the
    // manifest already covers every record written so far, so a failed rotation
    // just leaves a stale segment to be replayed (and dropped by the next one).
    if (impl_->sharded_wal) {
        (void)impl_->RotateShardedWalLocked();
    } else {
        (void)impl_->RotateWalLocked();
    }
    return Status::Ok();
}
Status Engine::CompactRoundLocked(std::unique_lock<std::mutex>& lock,
                                  size_t threshold) {
    auto jobs = impl_->PlanCompactionLocked(threshold);
    if (jobs.empty()) return Status::Ok();
    lock.unlock();
    std::vector<std::shared_ptr<SstableMetadata>> published;
    std::vector<std::filesystem::path> written_files;
    Status merge_status = Status::Ok();
    for (const auto& job : jobs) {
        std::shared_ptr<SstableMetadata> out;
        auto status = impl_->MergeCompactionJob(job, &out);
        if (!status.ok) {
            merge_status = status;
            break;
        }
        if (out) {
            published.push_back(out);
            written_files.push_back(out->path);
        }
    }
    lock.lock();
    if (!merge_status.ok) {
        for (const auto& file : written_files) {
            std::error_code ec;
            std::filesystem::remove(file, ec);
        }
        for (const auto& job : jobs) {
            for (const auto& source : job.sources) {
                if (source->refs > 0) --source->refs;
            }
        }
        return merge_status;
    }
    return impl_->PublishCompactionLocked(jobs, published);
}

Status Engine::Flush() {
    std::lock_guard lock(impl_->mutex);
    return FlushLocked(true);
}

Status Engine::Compact() {
    std::unique_lock lock(impl_->mutex);
    if (impl_->closed) return Status::Error("engine is closed");
    // Bounded: with writers flushing continuously a shard can refill its level
    // as fast as it is merged, which would keep this loop spinning forever.
    for (size_t round = 0; round < 64 && impl_->ShouldCompactLocked(2); ++round) {
        auto status = CompactRoundLocked(lock, 2);
        if (!status.ok) return status;
    }
    return Status::Ok();
}


Status Engine::GarbageCollect() {
    std::lock_guard lock(impl_->mutex);
    return impl_->GarbageCollectLocked();
}

Status Engine::Close() {
    {
        std::lock_guard lock(impl_->mutex);
        if (impl_->closed) return Status::Ok();
        if (impl_->closing) return Status::Error("close already in progress");
        impl_->closing = true;
        impl_->stopping_write = true;
        impl_->write_cv.notify_all();
        impl_->stopping_flush = true;
        impl_->flush_cv.notify_all();
        impl_->stopping_compaction = true;
        impl_->compaction_cv.notify_all();
    }
    // The coordinator drains the queue first; only after it is gone may the
    // shard workers be told to stop, otherwise a fragment could be queued for a
    // worker that has already exited.
    if (impl_->write_worker.joinable()) impl_->write_worker.join();
    {
        std::lock_guard lock(impl_->mutex);
        for (size_t i = 0; i < impl_->shard_stopping.size(); ++i) {
            impl_->shard_stopping[i] = true;
            impl_->shard_cv[i]->notify_all();
        }
    }
    for (auto& worker : impl_->shard_worker) {
        if (worker.joinable()) worker.join();
    }
    if (impl_->flush_worker.joinable()) impl_->flush_worker.join();
    if (impl_->compaction_worker.joinable()) impl_->compaction_worker.join();
    std::lock_guard lock(impl_->mutex);
    auto status = FlushLocked(true);
    if (!status.ok) return status;
    impl_->closed = true;
    if (impl_->wal_fd >= 0) {
        ::close(impl_->wal_fd);
        impl_->wal_fd = -1;
    }
    for (int& fd : impl_->shard_wal_fd) {
        if (fd >= 0) {
            ::close(fd);
            fd = -1;
        }
    }
    if (impl_->commit_wal_fd >= 0) {
        ::close(impl_->commit_wal_fd);
        impl_->commit_wal_fd = -1;
    }
    impl_->closing = false;
    impl_->close_cv.notify_all();
    return Status::Ok();
}

Status Engine::GetStats(EngineStats* stats) const {
    if (!stats) return Status::Error("stats output is null");
    std::lock_guard lock(impl_->mutex);
    stats->visible_version = impl_->visible_version;
    stats->expiration_cutoff = impl_->cutoff;
    stats->mutable_points = 0;
    for (size_t i = 0; i < impl_->shards.size(); ++i) {
        std::lock_guard shard_lock(*impl_->shard_mutex[i]);
        stats->mutable_points += impl_->shards[i]->point_count();
    }
    stats->immutable_points = 0;
    uint64_t wal_bytes = 0;
    auto add_dir_bytes = [&wal_bytes](const std::filesystem::path& dir) {
        for (const uint64_t number : ListSegmentNumbers(dir)) {
            std::error_code ec;
            const auto size = std::filesystem::file_size(
                WalSegmentPath(dir, number), ec);
            if (!ec) wal_bytes += static_cast<uint64_t>(size);
        }
    };
    if (impl_->sharded_wal) {
        for (size_t i = 0; i < impl_->shards.size(); ++i) {
            add_dir_bytes(impl_->ShardWalDir(i));
        }
        add_dir_bytes(impl_->commit_wal_dir);
    } else {
        add_dir_bytes(impl_->wal_dir);
    }
    stats->wal_bytes = wal_bytes;
    stats->wal_bytes_replayed = impl_->wal_bytes_replayed;
    stats->wal_syncs = impl_->wal_syncs;
    stats->compaction_count = impl_->compaction_count;
    stats->flush_count = impl_->flush_count;
    stats->pending_batches = impl_->write_queue.size();
    stats->active_read_views = impl_->active_read_views;
    stats->obsolete_sstable_count = impl_->obsolete_sstables.size();
    stats->sstable_count = 0;
    stats->sstable_bytes = 0;
    std::error_code dir_error;
    const auto sstable_dir = impl_->root / "sst";
    if (std::filesystem::exists(sstable_dir, dir_error)) {
        for (const auto& entry :
             std::filesystem::recursive_directory_iterator(sstable_dir)) {
            if (!entry.is_regular_file()) continue;
            if (entry.path().extension() == ".sst") {
                ++stats->sstable_count;
                std::error_code size_error;
                stats->sstable_bytes +=
                    std::filesystem::file_size(entry.path(), size_error);
            }
        }
    }
    struct rusage usage {};
    if (::getrusage(RUSAGE_SELF, &usage) == 0) {
#if defined(__APPLE__)
        stats->peak_rss_bytes =
            static_cast<uint64_t>(usage.ru_maxrss);
#else
        stats->peak_rss_bytes =
            static_cast<uint64_t>(usage.ru_maxrss) * 1024ULL;
#endif
    }
    stats->rss_bytes = stats->peak_rss_bytes;
    stats->engine_threads = 2 +
                            (impl_->options.enable_background_compaction ? 1 : 0) +
                            impl_->shards.size();
    stats->memtable_bytes = impl_->memtable_bytes;
    return Status::Ok();
}

}  // namespace tsdb
