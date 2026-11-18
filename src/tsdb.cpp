#include "tsdb.h"

#include <algorithm>
#include <cmath>
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
    uint64_t point_count{};
    uint32_t encoding{0};
    int64_t min_timestamp{std::numeric_limits<int64_t>::max()};
    int64_t max_timestamp{std::numeric_limits<int64_t>::min()};
    std::map<uint64_t, SstableIndexEntry> series;
};

constexpr uint32_t kWalMagic = 0x54534442;
constexpr uint8_t kTxnRecord = 1;
constexpr uint8_t kExpireRecord = 2;
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
    return static_cast<uint64_t>((value << 1) ^ (value >> 63));
}

int64_t ZigzagDecode(uint64_t value) {
    return static_cast<int64_t>((value >> 1) ^
                                -static_cast<int64_t>(value & 1));
}

bool ReadUvarintStream(std::istream& input, uint64_t* value) {
    uint64_t result = 0;
    unsigned shift = 0;
    while (shift < 64) {
        char raw = 0;
        input.get(raw);
        if (!input) return false;
        const uint8_t byte =
            static_cast<uint8_t>(static_cast<unsigned char>(raw));
        result |= static_cast<uint64_t>(byte & 0x7f) << shift;
        if ((byte & 0x80) == 0) {
            *value = result;
            return true;
        }
        shift += 7;
    }
    return false;
}


bool SyncFile(const std::filesystem::path& path) {
    const int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return false;
    const bool ok = ::fsync(fd) == 0;
    ::close(fd);
    return ok;
}

struct VersionedPoint {
    DataPoint point;
    CommitVersion version{};
};

struct ShardData {
    std::map<uint64_t, std::map<int64_t, VersionedPoint>> series;
    std::unordered_map<uint64_t, size_t> block_bytes;
    std::unordered_map<uint64_t, size_t> block_count;
    std::unordered_map<uint64_t, std::vector<std::vector<VersionedPoint>>> blocks;
    size_t point_count() const {
        size_t count = 0;
        for (const auto& [_, points] : series) count += points.size();
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
            ::ftruncate(fd, record_start);
            ::fsync(fd);
            return false;
        }
        written += static_cast<size_t>(result);
    }
    if (::fsync(fd) == 0) return true;
    ::ftruncate(fd, record_start);
    ::fsync(fd);
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
            ::ftruncate(fd, record_start);
            ::fsync(fd);
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

struct Engine::Impl {
    explicit Impl(std::filesystem::path root, Options opts)
        : root(std::move(root)), options(opts) {}

    std::filesystem::path root;
    Options options;
    std::filesystem::path wal_path;
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
    CommitVersion next_version{1};
    int64_t cutoff{std::numeric_limits<int64_t>::min()};
    bool closed{false};
    bool closing{false};
    size_t points_since_flush{0};
    std::vector<std::filesystem::path> current_files;
    std::vector<std::shared_ptr<SstableMetadata>> sstable_metadata;
    uint64_t wal_bytes_replayed{0};
    uint64_t next_file_number{1};
    int wal_fd{-1};
    uint64_t compaction_count{0};
    uint64_t flush_count{0};
    size_t memtable_bytes{0};
    std::unordered_map<uint64_t, uint64_t> hot_series_points;
    std::vector<CommitVersion> covered_commit_versions;
    CommitVersion metadata_covered_commit_version{0};
    size_t active_read_views{0};
    std::vector<std::filesystem::path> obsolete_sstables;
    CommitVersion last_flushed_version{0};

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
        view.version = next_version - 1;
        view.cutoff = cutoff;
        view.shards.reserve(shards.size());
        for (const auto& shard : shards) view.shards.push_back(shard);
        for (const auto& sstable : sstable_metadata) view.sstables.push_back(sstable);
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
            const auto& series = shards[ShardFor(point.series_id)]->series;
            const auto series_it = series.find(point.series_id);
            if (series_it != series.end() && !series_it->second.empty()) {
                if (point.timestamp_ms <= series_it->second.rbegin()->first) {
                    return Status::Error("timestamp is not greater than last committed timestamp");
                }
            }
        }
        return Status::Ok();
    }

    void ApplyPoints(const std::vector<DataPoint>& points, CommitVersion version) {
        std::set<size_t> touched;
        for (const auto& point : points) touched.insert(ShardFor(point.series_id));
        for (const auto shard_id : touched) {
            shards[shard_id] = std::make_shared<ShardData>(*shards[shard_id]);
        }
        for (const auto& point : points) {
            shards[ShardFor(point.series_id)]->series[point.series_id]
                [point.timestamp_ms] = VersionedPoint{point, version};
            const size_t bytes = sizeof(DataPoint) + sizeof(CommitVersion);
            auto& block_chain =
                shards[ShardFor(point.series_id)]
                    ->blocks[point.series_id];
            if (block_chain.empty()) block_chain.emplace_back();
            block_chain.back().push_back(VersionedPoint{point, version});
            if (block_chain.back().size() * bytes >
                options.memtable_block_bytes) {
                block_chain.emplace_back();
            }
            shards[ShardFor(point.series_id)]->block_bytes
                [point.series_id] += bytes;
            auto& shard = *shards[ShardFor(point.series_id)];
            shard.block_count[point.series_id] =
                (shard.block_bytes[point.series_id] +
                 options.memtable_block_bytes - 1) /
                options.memtable_block_bytes;
            memtable_bytes += bytes;
            ++hot_series_points[point.series_id];
        }
    }

    void CompactShardLocked(size_t shard_id) {
        auto compacted = std::make_shared<ShardData>();
        const auto& old = *shards[shard_id];
        size_t batch_count = 0;
        for (const auto& [series_id, points] : old.series) {
            std::vector<VersionedPoint> batch;
            batch.reserve(kDiskPageRecords);
            for (const auto& [timestamp, value] : points) {
                if (timestamp < cutoff ||
                    value.version > next_version - 1) {
                    continue;
                }
                compacted->series[series_id][timestamp] = value;
                batch.push_back(value);
                ++batch_count;
                if (batch.size() == kDiskPageRecords) {
                    compacted->blocks[series_id].push_back(batch);
                    batch.clear();
                }
            }
            if (!batch.empty()) {
                compacted->blocks[series_id].push_back(batch);
            }
            compacted->block_bytes[series_id] =
                compacted->series[series_id].size() *
                (sizeof(DataPoint) + sizeof(CommitVersion));
            compacted->block_count[series_id] =
                compacted->blocks[series_id].size();
        }
        (void)batch_count;
        shards[shard_id] = std::move(compacted);
    }

    void ApplyGroup(const std::vector<std::shared_ptr<PendingWrite>>& group) {
        std::set<size_t> touched;
        for (const auto& request : group) {
            for (const auto& point : request->points) {
                touched.insert(ShardFor(point.series_id));
            }
        }
        for (size_t shard_id : touched) {
            shards[shard_id] = std::make_shared<ShardData>(*shards[shard_id]);
        }
        for (const auto& request : group) {
            for (const auto& point : request->points) {
                shards[ShardFor(point.series_id)]->series[point.series_id]
                    [point.timestamp_ms] = VersionedPoint{point, request->version};
                const size_t block_bytes =
                    sizeof(DataPoint) + sizeof(CommitVersion);
                auto& block_chain =
                    shards[ShardFor(point.series_id)]
                        ->blocks[point.series_id];
                if (block_chain.empty()) block_chain.emplace_back();
                block_chain.back().push_back(
                    VersionedPoint{point, request->version});
                if (block_chain.back().size() * block_bytes >
                    options.memtable_block_bytes) {
                    block_chain.emplace_back();
                }
                shards[ShardFor(point.series_id)]->block_bytes
                    [point.series_id] += block_bytes;
                auto& shard = *shards[ShardFor(point.series_id)];
                shard.block_count[point.series_id] =
                    (shard.block_bytes[point.series_id] +
                     options.memtable_block_bytes - 1) /
                    options.memtable_block_bytes;
                memtable_bytes += block_bytes;
                ++hot_series_points[point.series_id];
            }
        }
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
            std::string data;
            if (!ReadFile(path, &data)) return Status::Error("cannot read SSTable");
            if (data.size() < sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint64_t)) {
                return Status::Error("truncated encoded SSTable");
            }
            const uint64_t checksum_offset = data.size() - sizeof(uint64_t);
            uint64_t recorded_checksum = 0;
            std::memcpy(&recorded_checksum, data.data() + checksum_offset,
                        sizeof(uint64_t));
            const size_t payload_offset =
                sizeof(uint32_t) + sizeof(uint64_t);
            const std::string payload =
                data.substr(payload_offset, checksum_offset - payload_offset);
            if (Checksum(payload) != recorded_checksum) {
                return Status::Error("SSTable checksum mismatch");
            }
            size_t encoded_offset = 0;
            for (uint64_t i = 0; i < count; ++i) {
                const size_t record_offset = payload_offset + encoded_offset;
                uint64_t series_id = 0;
                uint64_t zigzag_timestamp = 0;
                double value = 0.0;
                uint64_t version = 0;
                if (!ReadUvarint(payload, encoded_offset, &series_id) ||
                    !ReadUvarint(payload, encoded_offset, &zigzag_timestamp) ||
                    !Read(payload, encoded_offset, &value) ||
                    !ReadUvarint(payload, encoded_offset, &version)) {
                    return Status::Error("truncated encoded SSTable");
                }
                const int64_t timestamp = ZigzagDecode(zigzag_timestamp);
                auto& entry = result->series[series_id];
                entry.series_id = series_id;
                if (entry.point_count == 0) entry.offset = record_offset;
                ++entry.point_count;
                entry.byte_length = payload_offset + encoded_offset -
                                    entry.offset;
                entry.min_timestamp = std::min(entry.min_timestamp, timestamp);
                entry.max_timestamp = std::max(entry.max_timestamp, timestamp);
                result->min_timestamp =
                    std::min(result->min_timestamp, timestamp);
                result->max_timestamp =
                    std::max(result->max_timestamp, timestamp);
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

    Status LoadSstable(const std::filesystem::path& path, size_t shard_id) {
        std::ifstream in(path, std::ios::binary);
        if (!in) return Status::Error("cannot read SSTable: " + path.string());
        uint32_t magic = 0;
        uint64_t count = 0;
        in.read(reinterpret_cast<char*>(&magic), sizeof(magic));
        in.read(reinterpret_cast<char*>(&count), sizeof(count));
        if (!in ||
            (magic != kWalMagic && magic != kEncodedSstableMagic)) {
            return Status::Error("invalid SSTable");
        }
        auto metadata = std::make_shared<SstableMetadata>();
        metadata->path = path;
        metadata->shard_id = shard_id;
        metadata->point_count = count;
        metadata->encoding = magic;
        if (magic == kEncodedSstableMagic) {
            std::string data;
            if (!ReadFile(path, &data)) return Status::Error("cannot read SSTable");
            if (data.size() < sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint64_t)) {
                return Status::Error("truncated encoded SSTable");
            }
            const uint64_t checksum_offset = data.size() - sizeof(uint64_t);
            uint64_t recorded_checksum = 0;
            std::memcpy(&recorded_checksum, data.data() + checksum_offset,
                        sizeof(uint64_t));
            const size_t payload_offset =
                sizeof(uint32_t) + sizeof(uint64_t);
            const std::string payload =
                data.substr(payload_offset, checksum_offset - payload_offset);
            if (Checksum(payload) != recorded_checksum) {
                return Status::Error("SSTable checksum mismatch");
            }
            size_t encoded_offset = 0;
            for (uint64_t i = 0; i < count; ++i) {
                const size_t record_offset = payload_offset + encoded_offset;
                uint64_t series_id = 0;
                uint64_t zigzag_timestamp = 0;
                double value = 0.0;
                uint64_t version = 0;
                if (!ReadUvarint(payload, encoded_offset, &series_id) ||
                    !ReadUvarint(payload, encoded_offset, &zigzag_timestamp) ||
                    !Read(payload, encoded_offset, &value) ||
                    !ReadUvarint(payload, encoded_offset, &version)) {
                    return Status::Error("truncated encoded SSTable");
                }
                const int64_t timestamp = ZigzagDecode(zigzag_timestamp);
                const VersionedPoint point{
                    {series_id, timestamp, value},
                    static_cast<CommitVersion>(version)};
                shards[shard_id]->series[series_id][timestamp] = point;
                auto& entry = metadata->series[series_id];
                entry.series_id = series_id;
                if (entry.point_count == 0) entry.offset = record_offset;
                ++entry.point_count;
                entry.min_timestamp = std::min(entry.min_timestamp, timestamp);
                entry.max_timestamp = std::max(entry.max_timestamp, timestamp);
                metadata->min_timestamp =
                    std::min(metadata->min_timestamp, timestamp);
                metadata->max_timestamp =
                    std::max(metadata->max_timestamp, timestamp);
                next_version = std::max(next_version, version + 1);
            }
            sstable_metadata.push_back(metadata);
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
            if (!in) return Status::Error("truncated SSTable");
            char raw[sizeof(uint64_t) + sizeof(int64_t) +
                     sizeof(double) + sizeof(CommitVersion)];
            std::memcpy(raw, &series_id, sizeof(uint64_t));
            std::memcpy(raw + sizeof(uint64_t), &timestamp, sizeof(int64_t));
            std::memcpy(raw + sizeof(uint64_t) + sizeof(int64_t),
                        &value, sizeof(double));
            std::memcpy(raw + sizeof(uint64_t) + sizeof(int64_t) + sizeof(double),
                        &version, sizeof(CommitVersion));
            hash = HashRecord(hash, raw, sizeof(raw));
            auto& entry = metadata->series[series_id];
            entry.series_id = series_id;
            if (entry.point_count == 0) {
                entry.offset = sizeof(uint32_t) + sizeof(uint64_t) +
                               i * (sizeof(uint64_t) + sizeof(int64_t) +
                                    sizeof(double) + sizeof(CommitVersion));
            }
            ++entry.point_count;
            entry.min_timestamp = std::min(entry.min_timestamp, timestamp);
            entry.max_timestamp = std::max(entry.max_timestamp, timestamp);
            metadata->min_timestamp = std::min(metadata->min_timestamp, timestamp);
            metadata->max_timestamp = std::max(metadata->max_timestamp, timestamp);
            next_version = std::max(next_version, version + 1);
        }
        uint64_t checksum = 0;
        in.read(reinterpret_cast<char*>(&checksum), sizeof(checksum));
        if (!in || checksum != hash) {
            return Status::Error("SSTable checksum mismatch");
        }
        sstable_metadata.push_back(metadata);
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
        if (next_version > 0) last_flushed_version = next_version - 1;
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
            auto status = LoadSstable(root / file, parsed_shard);
            if (!status.ok) return status;
            current_files.push_back(root / file);
        }
        if (!checksum_seen ||
            recorded_checksum == 0 ||
            recorded_checksum != Checksum(manifest_payload)) {
            return Status::Error("Manifest checksum mismatch");
        }
        return Status::Ok();
    }

    Status ReplayWal() {
        if (!std::filesystem::exists(wal_path)) return Status::Ok();
        std::string data;
        if (!ReadFile(wal_path, &data)) return Status::Error("cannot read WAL");
        size_t offset = 0;
        while (offset < data.size()) {
            uint32_t magic = 0;
            uint8_t type = 0;
            uint64_t length = 0;
            uint64_t checksum = 0;
            const size_t record_start = offset;
            if (!Read(data, offset, &magic) || !Read(data, offset, &type) ||
                !Read(data, offset, &length)) {
                break;
            }
            if (magic != kWalMagic) return Status::Error("corrupt WAL magic");
            if (offset + sizeof(checksum) + length > data.size()) {
                break;
            }
            if (!Read(data, offset, &checksum)) break;
            std::string payload = data.substr(offset, length);
            offset += length;
            if (checksum != Checksum(payload)) return Status::Error("corrupt WAL checksum");
            wal_bytes_replayed += offset - record_start;
            size_t payload_offset = 0;
            CommitVersion version = 0;
            if (type == kTxnRecord) {
                uint32_t encoded_magic = 0;
                if (payload.size() >= sizeof(uint32_t) &&
                    Read(payload, payload_offset, &encoded_magic) &&
                    encoded_magic == kEncodedWalPayloadMagic) {
                    if (!Read(payload, payload_offset, &version)) {
                        return Status::Error("invalid encoded WAL");
                    }
                    uint32_t count = 0;
                    if (!Read(payload, payload_offset, &count)) {
                        return Status::Error("invalid encoded WAL");
                    }
                    std::vector<DataPoint> points(count);
                    for (auto& point : points) {
                        uint64_t series_id = 0;
                        uint64_t zigzag_timestamp = 0;
                        if (!ReadUvarint(payload, payload_offset, &series_id) ||
                            !ReadUvarint(payload, payload_offset,
                                         &zigzag_timestamp) ||
                            !Read(payload, payload_offset, &point.value)) {
                            return Status::Error("invalid encoded WAL");
                        }
                        point.series_id = series_id;
                        point.timestamp_ms = ZigzagDecode(zigzag_timestamp);
                    }
                    ApplyPoints(points, version);
                } else {
                    payload_offset = 0;
                    if (!Read(payload, payload_offset, &version)) return Status::Error("invalid WAL");
                    uint32_t count = 0;
                    if (!Read(payload, payload_offset, &count)) return Status::Error("invalid WAL");
                    std::vector<DataPoint> points(count);
                    for (auto& point : points) {
                        if (!Read(payload, payload_offset, &point.series_id) ||
                            !Read(payload, payload_offset, &point.timestamp_ms) ||
                            !Read(payload, payload_offset, &point.value)) {
                            return Status::Error("invalid WAL");
                        }
                    }
                    ApplyPoints(points, version);
                }
            } else if (type == kExpireRecord) {
                if (!Read(payload, payload_offset, &cutoff)) return Status::Error("invalid WAL");
            } else {
                return Status::Error("unknown WAL record");
            }
            next_version = std::max(next_version, version + 1);
        }
        return Status::Ok();
    }
};

Engine::Engine(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {
    impl_->owner = this;
    impl_->write_worker = std::thread([this] {
        while (true) {
            std::unique_lock lock(impl_->mutex);
            impl_->write_cv.wait_for(lock, std::chrono::microseconds(200), [&] {
                return !impl_->write_queue.empty() || impl_->stopping_write;
            });
            if (impl_->stopping_write && impl_->write_queue.empty()) return;
            if (impl_->write_queue.empty()) continue;

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
            off_t wal_record_start = ::lseek(impl_->wal_fd, 0, SEEK_END);
            for (const auto& request : group) {
                request->version = impl_->next_version++;
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
            if (!ok && wal_record_start >= 0) {
                ::ftruncate(impl_->wal_fd, wal_record_start);
                ::fsync(impl_->wal_fd);
            }
            if (ok) {
                impl_->ApplyGroup(group);
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
        while (true) {
            std::unique_lock lock(impl_->mutex);
            impl_->flush_cv.wait(lock, [&] {
                return impl_->flush_requested || impl_->stopping_flush;
            });
            if (impl_->stopping_flush) return;
            impl_->flush_requested = false;
            if (FlushLocked(true).ok &&
                impl_->options.enable_background_compaction &&
                impl_->flush_count >= 4 &&
                impl_->flush_count % 4 == 0) {
                impl_->compaction_requested = true;
                impl_->compaction_cv.notify_one();
            }
        }
    });
    if (impl_->options.enable_background_compaction) {
        impl_->compaction_worker = std::thread([this] {
            while (true) {
                std::unique_lock lock(impl_->mutex);
                impl_->compaction_cv.wait(lock, [&] {
                    return impl_->compaction_requested ||
                           impl_->stopping_compaction;
                });
                if (impl_->stopping_compaction) return;
                impl_->compaction_requested = false;
                for (size_t shard_id = 0;
                     shard_id < impl_->shards.size(); ++shard_id) {
                    impl_->CompactShardLocked(shard_id);
                }
                impl_->points_since_flush = 0;
                for (const auto& shard : impl_->shards) {
                    impl_->points_since_flush += shard->point_count();
                }
                ++impl_->compaction_count;
                (void)FlushLocked(false);
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
    auto impl = std::make_unique<Impl>(path, options);
    impl->wal_path = path / "wal.log";
    impl->manifest_path = path / "MANIFEST";
    impl->shards.resize(options.shard_count);
    for (auto& shard : impl->shards) shard = std::make_shared<ShardData>();
    impl->covered_commit_versions.assign(options.shard_count, 0);
    auto load_status = impl->LoadManifest();
    if (!load_status.ok) return fail(load_status);
    load_status = impl->ReplayWal();
    if (!load_status.ok) return fail(load_status);
    impl->wal_fd = ::open(impl->wal_path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
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
    impl_->write_cv.notify_one();
    impl_->write_cv.wait(lock, [&] { return request->done; });
    if (request->status.ok && version) *version = request->version;
    return request->status;
}

Status Engine::Scan(const std::vector<uint64_t>& series_ids, int64_t start,
                    int64_t end, std::unique_ptr<Cursor>* cursor) const {
    if (!cursor) return Status::Error("cursor output is null");
    if (start > end) return Status::Error("start must not exceed end");
    std::lock_guard lock(impl_->mutex);
    if (impl_->closed || impl_->closing) return Status::Error("engine is closed");
    const auto view = impl_->ViewLocked();
    ++impl_->active_read_views;

    using SeriesMap = std::map<int64_t, VersionedPoint>;
    struct Source {
        std::shared_ptr<SeriesMap> owner;
        SeriesMap::const_iterator it;
        SeriesMap::const_iterator end;
        uint64_t series_id{};
    };
    struct HeapItem {
        size_t source{};
        DataPoint point;
    };
    struct Compare {
        bool operator()(const HeapItem& left, const HeapItem& right) const {
            return std::tie(left.point.timestamp_ms, left.point.series_id) >
                   std::tie(right.point.timestamp_ms, right.point.series_id);
        }
    };
    struct StreamState {
        ReadView view;
        std::vector<Source> sources;
        std::priority_queue<HeapItem, std::vector<HeapItem>, Compare> heap;
        int64_t start{};
        int64_t end{};
    };

    auto state = std::make_shared<StreamState>();
    state->view = view;
    state->start = start;
    state->end = end;
    std::set<uint64_t> unique_series(series_ids.begin(), series_ids.end());
    for (const auto series_id : unique_series) {
        auto merged_owner = std::make_shared<SeriesMap>();
        auto& merged = *merged_owner;
        const auto& series =
            view.shards[impl_->ShardFor(series_id)]->series;
        const auto series_it = series.find(series_id);
        if (series_it != series.end()) {
            for (const auto& [timestamp, value] : series_it->second) {
                merged[timestamp] = value;
            }
        }
        for (const auto& sstable : view.sstables) {
            if (sstable->shard_id != impl_->ShardFor(series_id)) continue;
            const auto entry_it = sstable->series.find(series_id);
            if (entry_it == sstable->series.end()) continue;
            const auto& entry = entry_it->second;
            if (entry.max_timestamp < start || entry.min_timestamp >= end) continue;
            const int fd = ::open(sstable->path.c_str(), O_RDONLY);
            if (fd < 0) continue;
            if (sstable->encoding == kEncodedSstableMagic) {
                std::vector<char> encoded_page(entry.byte_length);
                const ssize_t bytes =
                    ::pread(fd, encoded_page.data(), encoded_page.size(),
                            entry.offset);
                if (bytes > 0) {
                    std::string payload(
                        encoded_page.data(), static_cast<size_t>(bytes));
                    size_t encoded_offset = 0;
                    for (uint64_t i = 0; i < entry.point_count; ++i) {
                        uint64_t disk_series = 0;
                        uint64_t zigzag_timestamp = 0;
                        double disk_value = 0.0;
                        uint64_t disk_version = 0;
                        if (!ReadUvarint(payload, encoded_offset, &disk_series) ||
                            !ReadUvarint(payload, encoded_offset,
                                         &zigzag_timestamp) ||
                            !Read(payload, encoded_offset, &disk_value) ||
                            !ReadUvarint(payload, encoded_offset,
                                         &disk_version)) {
                            break;
                        }
                        const int64_t disk_timestamp =
                            ZigzagDecode(zigzag_timestamp);
                        if (disk_timestamp >= start && disk_timestamp < end &&
                            disk_timestamp >= view.cutoff &&
                            disk_version <= view.version) {
                            const auto existing = merged.find(disk_timestamp);
                            if (existing == merged.end() ||
                                disk_version >= existing->second.version) {
                                merged[disk_timestamp] =
                                    VersionedPoint{
                                        {disk_series, disk_timestamp, disk_value},
                                        static_cast<CommitVersion>(disk_version)};
                            }
                        }
                    }
                }
                ::close(fd);
                continue;
            }
            const size_t record_size =
                sizeof(uint64_t) + sizeof(int64_t) +
                sizeof(double) + sizeof(CommitVersion);
            for (uint64_t i = 0; i < entry.point_count;
                 i += kDiskPageRecords) {
                const uint64_t batch =
                    std::min(kDiskPageRecords, entry.point_count - i);
                std::vector<char> page(batch * record_size);
                const ssize_t bytes =
                    ::pread(fd, page.data(), page.size(),
                            entry.offset + i * record_size);
                if (bytes < 0) break;
                const size_t complete =
                    static_cast<size_t>(bytes) / record_size;
                for (size_t j = 0; j < complete; ++j) {
                    const char* record = page.data() + j * record_size;
                    uint64_t disk_series = 0;
                    int64_t disk_timestamp = 0;
                    double disk_value = 0.0;
                    CommitVersion disk_version = 0;
                    std::memcpy(&disk_series, record, sizeof(uint64_t));
                    std::memcpy(&disk_timestamp,
                                record + sizeof(uint64_t), sizeof(int64_t));
                    std::memcpy(&disk_value,
                                record + sizeof(uint64_t) + sizeof(int64_t),
                                sizeof(double));
                    std::memcpy(&disk_version,
                                record + sizeof(uint64_t) + sizeof(int64_t) +
                                    sizeof(double),
                                sizeof(CommitVersion));
                    if (disk_timestamp >= start && disk_timestamp < end &&
                        disk_timestamp >= view.cutoff &&
                        disk_version <= view.version) {
                        const auto existing = merged.find(disk_timestamp);
                        if (existing == merged.end() ||
                            disk_version >= existing->second.version) {
                            merged[disk_timestamp] =
                                VersionedPoint{
                                    {disk_series, disk_timestamp, disk_value},
                                    disk_version};
                        }
                    }
                }
            }
            ::close(fd);
        }
        if (merged.empty()) continue;
        state->sources.push_back(
            {merged_owner, merged.begin(), merged.end(), series_id});
    }

    auto advance = [state](size_t source_index) {
        auto& source = state->sources[source_index];
        while (source.it != source.end &&
               (source.it->first < state->start ||
                source.it->first >= state->end ||
                source.it->first < state->view.cutoff ||
                source.it->second.version > state->view.version)) {
            ++source.it;
        }
        if (source.it != source.end && source.it->first < state->end) {
            state->heap.push({source_index, source.it->second.point});
        }
    };
    for (size_t i = 0; i < state->sources.size(); ++i) advance(i);

    *cursor = std::make_unique<Cursor>(
        [state, advance]() mutable -> std::optional<DataPoint> {
            if (state->heap.empty()) return std::nullopt;
            const auto item = state->heap.top();
            state->heap.pop();
            auto& source = state->sources[item.source];
            ++source.it;
            advance(item.source);
            return item.point;
        },
        [this] {
            std::lock_guard lock(impl_->mutex);
            if (impl_->active_read_views > 0) --impl_->active_read_views;
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
    if (!AppendWalRecord(impl_->wal_fd, kExpireRecord,
                         WalPayloadExpire(commit, cutoff))) {
        --impl_->next_version;
        return Status::Error("WAL sync failed");
    }
    impl_->cutoff = cutoff;
    std::vector<std::shared_ptr<SstableMetadata>> retained_sstables;
    for (const auto& sstable : impl_->sstable_metadata) {
        if (sstable->max_timestamp < cutoff) {
            impl_->obsolete_sstables.push_back(sstable->path);
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
    if (impl_->options.enable_background_compaction) {
        impl_->compaction_requested = true;
        impl_->compaction_cv.notify_one();
    }
    constexpr size_t kRecordBytes = sizeof(DataPoint) + sizeof(CommitVersion);
    for (size_t shard_id = 0; shard_id < impl_->shards.size(); ++shard_id) {
        impl_->shards[shard_id] =
            std::make_shared<ShardData>(*impl_->shards[shard_id]);
        auto& points_by_series = impl_->shards[shard_id]->series;
        auto& blocks_by_series = impl_->shards[shard_id]->blocks;
        for (auto series_it = points_by_series.begin();
             series_it != points_by_series.end();) {
            size_t removed = 0;
            for (auto point_it = series_it->second.begin();
                 point_it != series_it->second.end();) {
                if (point_it->first < cutoff) {
                    point_it = series_it->second.erase(point_it);
                    ++removed;
                } else {
                    ++point_it;
                }
            }
            if (removed > 0) {
                impl_->memtable_bytes -= removed * kRecordBytes;
                blocks_by_series.erase(series_it->first);
                impl_->shards[shard_id]->block_bytes.erase(series_it->first);
                impl_->shards[shard_id]->block_count.erase(series_it->first);
            }
            if (series_it->second.empty()) {
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
    if (impl_->closed) return Status::Error("engine is closed");
    std::vector<std::filesystem::path> new_files;
    std::vector<std::shared_ptr<SstableMetadata>> published_metadata;
    std::vector<std::filesystem::path> manifest_files =
        incremental ? impl_->current_files : std::vector<std::filesystem::path>{};
    for (size_t shard_id = 0; shard_id < impl_->shards.size(); ++shard_id) {
        std::string filename = "shard-";
        filename += shard_id < 10 ? "0" : "";
        filename += std::to_string(shard_id);
        filename += "-" + std::to_string(impl_->next_file_number) + ".sst";
        const auto file = impl_->root / "sst" / filename;
        const auto temp = file.string() + ".tmp";
        std::ofstream out(temp, std::ios::binary | std::ios::trunc);
        if (!out) return Status::Error("cannot create SSTable");
        const auto& data = *impl_->shards[shard_id];
        const uint32_t magic = kEncodedSstableMagic;
        std::string payload;
        uint64_t count = 0;
        for (const auto& [_, points] : data.series) {
            for (const auto& [__, value] : points) {
                if (incremental &&
                    value.version <= impl_->last_flushed_version) {
                    continue;
                }
                AppendUvarint(payload, value.point.series_id);
                AppendUvarint(payload, ZigzagEncode(value.point.timestamp_ms));
                Append(payload, value.point.value);
                AppendUvarint(payload, value.version);
                ++count;
            }
        }
        if (incremental && count == 0) continue;
        out.write(reinterpret_cast<const char*>(&magic), sizeof(magic));
        out.write(reinterpret_cast<const char*>(&count), sizeof(count));
        const uint64_t checksum = Checksum(payload);
        out.write(payload.data(), static_cast<std::streamsize>(payload.size()));
        out.write(reinterpret_cast<const char*>(&checksum), sizeof(checksum));
        out.flush();
        out.close();
        if (!SyncFile(temp)) return Status::Error("SSTable sync failed");
        std::error_code ec;
        std::filesystem::rename(temp, file, ec);
        if (ec) return Status::Error("SSTable publish failed");
        if (!SyncDirectory(file.parent_path())) {
            return Status::Error("SSTable directory sync failed");
        }
        std::shared_ptr<SstableMetadata> metadata;
        auto metadata_status =
            impl_->ReadSstableMetadata(file, shard_id, &metadata);
        if (!metadata_status.ok) return metadata_status;
        published_metadata.push_back(metadata);
        new_files.push_back(file);
        manifest_files.push_back(file);
        ++impl_->next_file_number;
    }
    for (auto& covered : impl_->covered_commit_versions) {
        covered = impl_->next_version - 1;
    }
    impl_->metadata_covered_commit_version = impl_->next_version - 1;
    const auto manifest_tmp = impl_->manifest_path.string() + ".tmp";
    {
        std::ofstream manifest(manifest_tmp, std::ios::trunc);
        if (!manifest) return Status::Error("cannot create Manifest");
        std::string manifest_text;
        manifest_text += std::to_string(impl_->cutoff) + " " +
                         std::to_string(impl_->next_version) + " " +
                         std::to_string(impl_->next_file_number) + "\n";
        for (size_t shard_id = 0;
             shard_id < impl_->covered_commit_versions.size(); ++shard_id) {
            manifest_text += "COVERED " + std::to_string(shard_id) + " " +
                             std::to_string(
                                 impl_->covered_commit_versions[shard_id]) +
                             "\n";
        }
        for (size_t i = 0; i < manifest_files.size(); ++i) {
            manifest_text += std::to_string(i) + " " +
                             std::filesystem::relative(manifest_files[i], impl_->root).string() +
                             "\n";
        }
        manifest << manifest_text;
        manifest << "CHECKSUM " << Checksum(manifest_text) << '\n';
    }
    if (!SyncFile(manifest_tmp)) return Status::Error("Manifest sync failed");
    std::error_code ec;
    std::filesystem::rename(manifest_tmp, impl_->manifest_path, ec);
    if (ec) return Status::Error("Manifest publish failed");
    if (!SyncDirectory(impl_->root)) return Status::Error("Manifest directory sync failed");
    {
        const auto current_tmp = impl_->root / "CURRENT.tmp";
        std::ofstream current(current_tmp, std::ios::trunc);
        if (!current) return Status::Error("cannot create CURRENT");
        current << "MANIFEST\n";
        current.close();
        if (!SyncFile(current_tmp)) return Status::Error("CURRENT sync failed");
        std::error_code current_ec;
        std::filesystem::rename(current_tmp, impl_->root / "CURRENT", current_ec);
        if (current_ec) return Status::Error("CURRENT publish failed");
        if (!SyncDirectory(impl_->root)) return Status::Error("CURRENT directory sync failed");
    }
    if (!incremental) {
        for (const auto& obsolete : impl_->current_files) {
            impl_->obsolete_sstables.push_back(obsolete);
        }
    }
    if (!incremental && !impl_->current_files.empty() &&
        !SyncDirectory(impl_->root / "sst")) {
        return Status::Error("obsolete SSTable directory sync failed");
    }
    if (::ftruncate(impl_->wal_fd, 0) != 0 ||
        ::fsync(impl_->wal_fd) != 0) {
        return Status::Error("WAL truncate sync failed");
    }
    impl_->current_files =
        incremental ? std::move(manifest_files) : std::move(new_files);
    if (incremental) {
        impl_->sstable_metadata.insert(
            impl_->sstable_metadata.end(),
            published_metadata.begin(),
            published_metadata.end());
    } else {
        impl_->sstable_metadata = std::move(published_metadata);
    }
    impl_->last_flushed_version = impl_->next_version - 1;
    impl_->points_since_flush = 0;
    impl_->hot_series_points.clear();
    ++impl_->flush_count;
    return Status::Ok();
}

Status Engine::Flush() {
    std::lock_guard lock(impl_->mutex);
    return FlushLocked(true);
}

Status Engine::Compact() {
    std::lock_guard lock(impl_->mutex);
    if (impl_->closed) return Status::Error("engine is closed");
    for (size_t shard_id = 0; shard_id < impl_->shards.size(); ++shard_id) {
        impl_->CompactShardLocked(shard_id);
    }
    impl_->points_since_flush = 0;
    for (const auto& shard : impl_->shards) {
        impl_->points_since_flush += shard->point_count();
    }
    ++impl_->compaction_count;
    return FlushLocked(false);
}

Status Engine::GarbageCollect() {
    std::lock_guard lock(impl_->mutex);
    if (impl_->active_read_views != 0) {
        return Status::Ok();
    }
    for (const auto& obsolete : impl_->obsolete_sstables) {
        std::error_code ec;
        std::filesystem::remove(obsolete, ec);
    }
    impl_->obsolete_sstables.clear();
    if (!SyncDirectory(impl_->root / "sst")) {
        return Status::Error("GC directory sync failed");
    }
    return Status::Ok();
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
    if (impl_->write_worker.joinable()) impl_->write_worker.join();
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
    impl_->closing = false;
    impl_->close_cv.notify_all();
    return Status::Ok();
}

Status Engine::GetStats(EngineStats* stats) const {
    if (!stats) return Status::Error("stats output is null");
    std::lock_guard lock(impl_->mutex);
    stats->visible_version = impl_->next_version - 1;
    stats->expiration_cutoff = impl_->cutoff;
    stats->mutable_points = 0;
    for (const auto& shard : impl_->shards) {
        stats->mutable_points += shard->point_count();
    }
    stats->immutable_points = impl_->points_since_flush;
    std::error_code ec;
    stats->wal_bytes = std::filesystem::exists(impl_->wal_path, ec)
                           ? std::filesystem::file_size(impl_->wal_path, ec)
                           : 0;
    stats->wal_bytes_replayed = impl_->wal_bytes_replayed;
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
    stats->engine_threads = 3;
    stats->memtable_bytes = impl_->memtable_bytes;
    return Status::Ok();
}

}  // namespace tsdb
