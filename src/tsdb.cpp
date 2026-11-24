#include "tsdb.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cerrno>
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
#include <stdexcept>
#include <system_error>
#include <sys/stat.h>
#include <tuple>
#include <unistd.h>
#include <sys/resource.h>
#include <condition_variable>
#include <thread>
#include <deque>
#include <unordered_map>
#include <unordered_set>

#if defined(__APPLE__)
#include <mach/mach.h>
#endif

#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif

namespace tsdb {
namespace {

std::mutex fault_hook_mutex;
std::function<void(const char*)> fault_hook;

void FaultPoint(const char* stage) {
    std::function<void(const char*)> hook;
    {
        std::lock_guard lock(fault_hook_mutex);
        hook = fault_hook;
    }
    if (hook) hook(stage);
}

bool SyncData(int fd) {
#if defined(__APPLE__)
    // fsync alone may leave acknowledged data in the drive's volatile cache.
    return ::fcntl(fd, F_FULLFSYNC) == 0;
#else
    return ::fsync(fd) == 0;
#endif
}

// One 128-point page inside a series chunk. Records are time-sorted within a
// chunk, so a page's [min,max] lets a selective scan seek past pages that do
// not intersect the query range instead of decoding the whole chunk.
struct SstablePageIndexEntry {
    uint64_t offset{};
    uint64_t point_count{};
    int64_t min_timestamp{std::numeric_limits<int64_t>::max()};
    int64_t max_timestamp{std::numeric_limits<int64_t>::min()};
};

struct SstableIndexEntry {
    uint64_t series_id{};
    uint64_t offset{};
    uint64_t byte_length{};
    uint64_t point_count{};
    int64_t min_timestamp{std::numeric_limits<int64_t>::max()};
    int64_t max_timestamp{std::numeric_limits<int64_t>::min()};
    std::vector<SstablePageIndexEntry> pages;
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

// C++20 defines >> on a negative signed value as an arithmetic shift; before
// that it was implementation-defined. ZigzagEncode depends on it (see below),
// and the build sets CMAKE_CXX_STANDARD 20. If that ever drops to 17, use
// static_cast<uint64_t>(-(value < 0)) instead of the sign fill.
static_assert(__cplusplus >= 202002L,
              "ZigzagEncode requires C++20 arithmetic right shift");

uint64_t ZigzagEncode(int64_t value) {
    // Go through unsigned for the left shift, but the sign fill has to come from
    // an arithmetic right shift on the signed value: a logical shift yields 1
    // instead of all ones and encodes every negative timestamp as ~2^64, which
    // then decodes to garbage and becomes unreadable once it hits the WAL or an
    // SSTable.
    return (static_cast<uint64_t>(value) << 1) ^
           static_cast<uint64_t>(value >> 63);
}

int64_t ZigzagDecode(uint64_t value) {
    return static_cast<int64_t>((value >> 1) ^
                                -static_cast<int64_t>(value & 1));
}


bool SyncFile(const std::filesystem::path& path) {
    const int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return false;
    const bool ok = SyncData(fd);
    ::close(fd);
    return ok;
}

// Absolute, lexically-normalized path used to detect a changed WAL directory
// between runs (changing it would orphan unflushed WAL segments).
std::filesystem::path NormalizePath(const std::filesystem::path& path) {
    std::error_code ec;
    const auto absolute = std::filesystem::absolute(path, ec);
    if (ec) return path.lexically_normal();
    return absolute.lexically_normal();
}

// Sync the whole ancestor chain: create_directories may have introduced more
// than one directory, and an external WAL root need not share the DB parent.
bool SyncDirectoryChain(const std::filesystem::path& path) {
    auto directory = NormalizePath(path);
    while (true) {
        if (!SyncDirectory(directory)) return false;
        const auto parent = directory.parent_path();
        if (parent.empty() || parent == directory) return true;
        directory = parent;
    }
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

// Timestamp arithmetic needs a wider type than int64_t: [start, end) can span
// more than INT64_MAX, so `timestamp - start` does not fit. __extension__ keeps
// -Wpedantic quiet about the built-in type; GCC and Clang both provide it.
#if defined(__SIZEOF_INT128__)
__extension__ typedef __int128 Int128;
#else
#error "the engine requires 128-bit integer support"
#endif

// Adds the lifetime of the enclosing scope to a nanosecond counter. Used to
// report where the import time goes; it also covers early returns.
struct NsTimer {
    std::chrono::steady_clock::time_point started{
        std::chrono::steady_clock::now()};
    uint64_t* total;
    explicit NsTimer(uint64_t* counter) : total(counter) {}
    ~NsTimer() {
        *total += static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - started)
                .count());
    }
    NsTimer(const NsTimer&) = delete;
    NsTimer& operator=(const NsTimer&) = delete;
};

struct VersionedPoint {
    DataPoint point;
    CommitVersion version{};
};

class SeriesPoints {
public:
    size_t size() const { return points_.size(); }
    size_t capacity() const { return points_.capacity(); }
    bool empty() const { return points_.empty(); }
    const VersionedPoint& front() const { return points_.front(); }
    const VersionedPoint& operator[](size_t index) const { return points_[index]; }
    auto begin() const { return points_.begin(); }
    auto end() const { return points_.end(); }

    size_t LowerBoundIndex(int64_t timestamp) const {
        return static_cast<size_t>(std::lower_bound(
            points_.begin(), points_.end(), timestamp,
            [](const VersionedPoint& point, int64_t value) {
                return point.point.timestamp_ms < value;
            }) - points_.begin());
    }

    bool Upsert(const VersionedPoint& point) {
        const int64_t timestamp = point.point.timestamp_ms;
        // 正常写入严格递增，追加为摊销 O(1)，仅恢复乱序记录时二分插入。
        if (points_.empty() || points_.back().point.timestamp_ms < timestamp) {
            points_.push_back(point);
            return true;
        }
        const size_t index = LowerBoundIndex(timestamp);
        if (points_[index].point.timestamp_ms == timestamp) {
            if (point.version > points_[index].version) points_[index] = point;
            return false;
        }
        points_.insert(points_.begin() + index, point);
        return true;
    }

    size_t ExpireBefore(int64_t cutoff) {
        const size_t removed = LowerBoundIndex(cutoff);
        points_.erase(points_.begin(), points_.begin() + removed);
        return removed;
    }

private:
    std::vector<VersionedPoint> points_;
};

struct ShardData {
    // 快照共享每条 series 的 buffer；提交只复制实际写入且仍被共享的 series。
    // 外层 map 仅保存 shared_ptr，复制成本不随驻留点数增长。
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
    FaultPoint("wal_before_sync");
    if (SyncData(fd)) {
        FaultPoint("wal_after_sync");
        return true;
    }
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
    // Page index being accumulated for the current series.
    SstablePageIndexEntry page;
    uint64_t page_series{0};
    bool have_page{false};

    void FlushPage() {
        if (!have_page) return;
        metadata.series[page_series].pages.push_back(page);
        have_page = false;
    }

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
        const uint64_t record_offset =
            sizeof(uint32_t) + sizeof(uint64_t) + payload_bytes;
        auto& entry = metadata.series[series_id];
        entry.series_id = series_id;
        if (entry.point_count == 0) {
            entry.offset = record_offset;
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
        if (!have_page || page_series != series_id ||
            page.point_count == kDiskPageRecords) {
            FlushPage();
            page = SstablePageIndexEntry{};
            page.offset = record_offset;
            page_series = series_id;
            have_page = true;
        }
        ++page.point_count;
        page.min_timestamp = std::min(page.min_timestamp, timestamp);
        page.max_timestamp = std::max(page.max_timestamp, timestamp);
        ++count;
    }

    void WriteAndHash(const char* bytes, size_t length) {
        out.write(bytes, static_cast<std::streamsize>(length));
        hash = HashRecord(hash, bytes, length);
        payload_bytes += length;
    }

    bool Finish() {
        FlushPage();
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

// 查询只解码已在 Open 时校验过的 SST；每源固定 8 KiB，不再逐字节计算 hash。
class QueryReader {
public:
    QueryReader(const std::filesystem::path& path, uint64_t offset, uint64_t end)
        : offset_(offset), end_(end) {
        fd_ = ::open(path.c_str(), O_RDONLY);
        if (fd_ < 0) {
            throw std::system_error(errno, std::generic_category(), "open SST query");
        }
    }

    ~QueryReader() { ::close(fd_); }
    QueryReader(const QueryReader&) = delete;
    QueryReader& operator=(const QueryReader&) = delete;

    void Seek(uint64_t offset, uint64_t end) {
        end_ = end;
        if (offset >= offset_ && offset - offset_ <= size_) {
            position_ = static_cast<size_t>(offset - offset_);
        } else {
            offset_ = offset;
            position_ = size_ = 0;
        }
    }

    uint64_t ReadUvarint() {
        uint64_t value = 0;
        for (unsigned shift = 0; shift < 64; shift += 7) {
            if (position_ == size_) Refill();
            const uint8_t byte = static_cast<uint8_t>(buffer_[position_++]);
            if (shift == 63 && byte > 1) {
                throw std::runtime_error("invalid SST query varint");
            }
            value |= static_cast<uint64_t>(byte & 0x7f) << shift;
            if ((byte & 0x80) == 0) return value;
        }
        throw std::runtime_error("invalid SST query varint");
    }

    double ReadDouble() {
        double value;
        ReadBytes(&value, sizeof(value));
        return value;
    }

    void ReadBytes(void* output, size_t length) {
        auto* target = static_cast<char*>(output);
        while (length != 0) {
            if (position_ == size_) Refill();
            const size_t count = std::min(length, size_ - position_);
            std::memcpy(target, buffer_.data() + position_, count);
            position_ += count;
            target += count;
            length -= count;
        }
    }

private:
    void Refill() {
        offset_ += position_;
        position_ = size_ = 0;
        if (offset_ >= end_) {
            throw std::runtime_error("truncated SST query record");
        }
        const size_t length = static_cast<size_t>(
            std::min<uint64_t>(buffer_.size(), end_ - offset_));
        ssize_t count;
        do {
            count = ::pread(fd_, buffer_.data(), length,
                            static_cast<off_t>(offset_));
        } while (count < 0 && errno == EINTR);
        if (count < 0) {
            throw std::system_error(errno, std::generic_category(), "pread SST query");
        }
        if (count == 0) {
            throw std::runtime_error("truncated SST query record");
        }
        // 正数短读仍可继续从实际读取的末尾 refill，只有 0 才表示截断。
        size_ = static_cast<size_t>(count);
    }

    int fd_{-1};
    uint64_t offset_;
    uint64_t end_;
    size_t position_{0};
    size_t size_{0};
    std::array<char, 8 * 1024> buffer_;
};

// 每个输入 SST 只持有固定缓冲和当前点；跨 series 复用文件描述符。
class SeriesReader {
public:
    explicit SeriesReader(const SstableMetadata& sstable)
        : fd_(::open(sstable.path.c_str(), O_RDONLY)),
          raw_(sstable.encoding == kWalMagic) {}
    ~SeriesReader() {
        if (fd_ >= 0) ::close(fd_);
    }
    SeriesReader(const SeriesReader&) = delete;
    SeriesReader& operator=(const SeriesReader&) = delete;

    bool IsOpen() const { return fd_ >= 0; }
    void Start(const SstableIndexEntry& entry) {
        series_id_ = entry.series_id;
        offset_ = entry.offset;
        unread_bytes_ = entry.byte_length;
        remaining = entry.point_count;
        position_ = size_ = 0;
        have_timestamp_ = false;
    }

    bool Next(VersionedPoint* point) {
        uint64_t series_id = 0;
        int64_t timestamp = 0;
        double value = 0.0;
        CommitVersion version = 0;
        if (raw_) {
            if (!ReadBytes(&series_id, sizeof(series_id)) ||
                !ReadBytes(&timestamp, sizeof(timestamp)) ||
                !ReadBytes(&value, sizeof(value)) ||
                !ReadBytes(&version, sizeof(version))) return false;
        } else {
            uint64_t zigzag_timestamp = 0;
            if (!ReadVarint(&series_id) || !ReadVarint(&zigzag_timestamp) ||
                !ReadBytes(&value, sizeof(value)) || !ReadVarint(&version)) {
                return false;
            }
            timestamp = ZigzagDecode(zigzag_timestamp);
        }
        if (series_id != series_id_ ||
            (have_timestamp_ && timestamp < last_timestamp_)) return false;
        last_timestamp_ = timestamp;
        have_timestamp_ = true;
        --remaining;
        // 必须恰好消费索引中的整个 chunk，截断/错误长度不能当作 EOF。
        if (remaining == 0 && (unread_bytes_ != 0 || position_ != size_)) {
            return false;
        }
        *point = {{series_id, timestamp, value}, version};
        return true;
    }

    uint64_t remaining{0};

private:
    bool Refill() {
        const size_t length = static_cast<size_t>(
            std::min<uint64_t>(unread_bytes_, sizeof(buffer_)));
        if (length == 0) return false;
        ssize_t bytes;
        do {
            bytes = ::pread(fd_, buffer_, length, static_cast<off_t>(offset_));
        } while (bytes < 0 && errno == EINTR);
        if (bytes <= 0) return false;
        size_ = static_cast<size_t>(bytes);
        position_ = 0;
        offset_ += size_;
        unread_bytes_ -= size_;
        return true;
    }

    bool ReadBytes(void* output, size_t length) {
        auto* bytes = static_cast<char*>(output);
        while (length != 0) {
            if (position_ == size_ && !Refill()) return false;
            const size_t count = std::min(length, size_ - position_);
            std::memcpy(bytes, buffer_ + position_, count);
            bytes += count;
            length -= count;
            position_ += count;
        }
        return true;
    }

    bool ReadVarint(uint64_t* value) {
        uint64_t result = 0;
        for (unsigned shift = 0; shift < 64; shift += 7) {
            if (position_ == size_ && !Refill()) return false;
            const auto byte = static_cast<uint8_t>(buffer_[position_++]);
            if (shift == 63 && byte > 1) return false;
            result |= static_cast<uint64_t>(byte & 0x7f) << shift;
            if ((byte & 0x80) == 0) {
                *value = result;
                return true;
            }
        }
        return false;
    }

    int fd_;
    bool raw_;
    char buffer_[64 * 1024];
    uint64_t series_id_{0};
    uint64_t offset_{0};
    uint64_t unread_bytes_{0};
    size_t position_{0};
    size_t size_{0};
    int64_t last_timestamp_{0};
    bool have_timestamp_{false};
};

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
        try {
            auto point = point_generator_();
            if (!point) Close();
            return point;
        } catch (...) {
            Close();
            throw;
        }
    }
    if (index_ >= points_.size()) return std::nullopt;
    return points_[index_++];
}

std::optional<AggregateResult> Cursor::NextAggregate() {
    if (closed_ || !aggregate_ || index_ >= aggregates_.size()) return std::nullopt;
    return aggregates_[index_++];
}

void Cursor::Close() {
    closed_ = true;
    auto callback = std::move(close_callback_);
    close_callback_ = {};
    points_.clear();
    aggregates_.clear();
    point_generator_ = {};
    if (!close_callback_called_) {
        close_callback_called_ = true;
        if (callback) callback();
    }
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
    ~Impl() {
        if (wal_fd >= 0) ::close(wal_fd);
        if (commit_wal_fd >= 0) ::close(commit_wal_fd);
        for (const int fd : shard_wal_fd) if (fd >= 0) ::close(fd);
    }

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
    // Signalled by the compaction worker whenever the live SSTable count drops,
    // so writers waiting on max_live_sstables can make progress.
    std::condition_variable compaction_done_cv;
    std::thread compaction_worker;
    std::condition_variable write_cv;
    std::thread write_worker;
    std::deque<std::shared_ptr<PendingWrite>> write_queue;
    bool stopping_write{false};
    bool flush_requested{false};
    bool stopping_flush{false};
    bool compaction_requested{false};
    bool compacting{false};
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
    // Instrumentation for the performance report: time spent inside the WAL
    // fsync, inside a whole flush and inside a whole compaction round, plus the
    // bytes actually appended to the WAL (wal_bytes above only reflects the
    // segments still on disk, which is 0 after a clean close).
    uint64_t wal_sync_ns{0};
    uint64_t flush_ns{0};
    uint64_t compaction_ns{0};
    uint64_t wal_bytes_written{0};
    size_t active_writers{0};
    uint64_t next_file_number{1};
    int wal_fd{-1};
    uint64_t compaction_count{0};
    uint64_t flush_count{0};
    size_t memtable_bytes{0};
    Status storage_error{Status::Ok()};
    // Persistence failure may happen after rename: the durable file set is then
    // ambiguous. Never continue writes or unlink files; reopen to reconcile.
    void RecordStorageError(Status status) {
        if (storage_error.ok) storage_error = std::move(status);
        write_cv.notify_all();
        compaction_done_cv.notify_all();
    }
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
        std::unordered_map<uint64_t, int64_t> last;
        last.reserve(points.size());
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

    // 恢复时按需复制 series，乱序插入或重复版本只在新增 timestamp 时计数。
    void AppendToMemtable(uint64_t series_id, int64_t timestamp,
                          const DataPoint& point, CommitVersion version) {
        auto& slot = shards[ShardFor(series_id)]->series[series_id];
        if (!slot) {
            slot = std::make_shared<SeriesPoints>();
        } else if (slot.use_count() != 1) {
            slot = std::make_shared<SeriesPoints>(*slot);
        }
        const bool inserted = slot->Upsert(VersionedPoint{point, version});
        auto [last, first] = last_timestamps.emplace(series_id, timestamp);
        if (!first) last->second = std::max(last->second, timestamp);
        if (inserted) {
            memtable_bytes += sizeof(DataPoint) + sizeof(CommitVersion);
            ++hot_series_points[series_id];
            ++points_since_flush;
        }
    }

    void ApplyPoints(const std::vector<DataPoint>& points, CommitVersion version) {
        std::set<size_t> touched;
        for (const auto& point : points) touched.insert(ShardFor(point.series_id));
        for (const auto shard_id : touched) {
            if (shards[shard_id].use_count() != 1) {
                shards[shard_id] = std::make_shared<ShardData>(*shards[shard_id]);
            }
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
        if (shards[shard_id].use_count() != 1) {
            shards[shard_id] = std::make_shared<ShardData>(*shards[shard_id]);
        }
        for (const auto& point : fragment.points) {
            auto& slot = shards[shard_id]->series[point.point.series_id];
            if (!slot) {
                slot = std::make_shared<SeriesPoints>();
            } else if (slot.use_count() != 1) {
                // shard 已独占时，未修改过的 series 仍可能被更早的快照共享。
                slot = std::make_shared<SeriesPoints>(*slot);
            }
            slot->Upsert(point);
        }
    }

    Status WriteMergedShard(
        const std::vector<std::shared_ptr<SstableMetadata>>& sources,
        SstableWriter* writer, int64_t merge_cutoff, CommitVersion merge_version) {
        using SeriesIterator = decltype(SstableMetadata::series)::const_iterator;
        using SeriesItem = std::pair<uint64_t, size_t>;
        std::vector<SeriesIterator> positions;
        std::vector<std::unique_ptr<SeriesReader>> readers(sources.size());
        std::priority_queue<SeriesItem, std::vector<SeriesItem>,
                            std::greater<SeriesItem>> series_heap;
        positions.reserve(sources.size());
        for (size_t i = 0; i < sources.size(); ++i) {
            positions.push_back(sources[i]->series.begin());
            if (positions[i] == sources[i]->series.end()) continue;
            readers[i] = std::make_unique<SeriesReader>(*sources[i]);
            if (!readers[i]->IsOpen()) {
                return Status::Error("compaction open failed: " +
                                     sources[i]->path.string());
            }
            series_heap.emplace(positions[i]->first, i);
        }
        struct Item {
            VersionedPoint point;
            size_t source;
        };
        struct ItemGreater {
            bool operator()(const Item& left, const Item& right) const {
                return std::tie(left.point.point.timestamp_ms, left.source) >
                       std::tie(right.point.point.timestamp_ms, right.source);
            }
        };
        std::priority_queue<Item, std::vector<Item>, ItemGreater> point_heap;
        const auto advance = [&](size_t i) {
            if (readers[i]->remaining == 0) return true;
            VersionedPoint point;
            if (!readers[i]->Next(&point)) return false;
            point_heap.push({point, i});
            return true;
        };
        const auto read_error = [&](size_t i) {
            return Status::Error("compaction read failed: " +
                                 sources[i]->path.string());
        };
        // 两个堆均至多 k 项，不复制完整 series 集合或热点的所有点。
        while (!series_heap.empty()) {
            const uint64_t series_id = series_heap.top().first;
            do {
                const size_t i = series_heap.top().second;
                series_heap.pop();
                readers[i]->Start(positions[i]->second);
                if (!advance(i)) return read_error(i);
                if (++positions[i] != sources[i]->series.end()) {
                    series_heap.emplace(positions[i]->first, i);
                }
            } while (!series_heap.empty() &&
                     series_heap.top().first == series_id);
            while (!point_heap.empty()) {
                Item best = point_heap.top();
                point_heap.pop();
                if (!advance(best.source)) return read_error(best.source);
                while (!point_heap.empty() &&
                       point_heap.top().point.point.timestamp_ms ==
                           best.point.point.timestamp_ms) {
                    const Item duplicate = point_heap.top();
                    point_heap.pop();
                    if (duplicate.point.version > best.point.version) {
                        best = duplicate;
                    }
                    if (!advance(duplicate.source)) {
                        return read_error(duplicate.source);
                    }
                }
                // 与原实现一致：先取最大版本，再应用合并视图的版本上界。
                if (best.point.point.timestamp_ms < merge_cutoff ||
                    best.point.version > merge_version) continue;
                writer->Append(series_id, best.point.point.timestamp_ms,
                               best.point.point.value, best.point.version);
                if (!writer->out) return Status::Error("SSTable write failed");
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
        int64_t cutoff{};
        CommitVersion version{};
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
            job.cutoff = cutoff;
            job.version = visible_version;
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
        auto status = WriteMergedShard(job.sources, &writer, job.cutoff, job.version);
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
        FaultPoint("compaction_before_manifest");
        auto status = PublishManifestLocked(current_files);
        if (!status.ok) {
            RecordStorageError(status);
            return status;
        }
        FaultPoint("compaction_after_manifest");
        ++compaction_count;
        return Status::Ok();
    }

    Status GarbageCollectLocked() {
        if (!storage_error.ok) return storage_error;
        bool deleted = false;
        Status result = Status::Ok();
        std::vector<std::shared_ptr<SstableMetadata>> retained;
        retained.reserve(obsolete_sstables.size());
        for (const auto& obsolete : obsolete_sstables) {
            if (obsolete->refs == 0) {
                std::error_code ec;
                FaultPoint("gc_before_delete");
                const bool removed = std::filesystem::remove(obsolete->path, ec);
                if (ec) {
                    retained.push_back(obsolete);
                    result = Status::Error("GC file removal failed: " + ec.message());
                } else {
                    deleted = deleted || removed;
                    FaultPoint("gc_after_delete");
                }
            } else {
                retained.push_back(obsolete);
            }
        }
        obsolete_sstables = std::move(retained);
        if (deleted && !SyncDirectory(root / "sst")) {
            return Status::Error("GC directory sync failed");
        }
        return result;
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
            manifest_text += "WAL_DIR " + NormalizePath(wal_dir).string() + "\n";
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
            // Records arrive grouped by series and time-sorted within a series,
            // so pages are simply every kDiskPageRecords consecutive records.
            SstablePageIndexEntry page;
            uint64_t page_series = 0;
            bool have_page = false;
            const auto flush_page = [&]() {
                if (!have_page) return;
                result->series[page_series].pages.push_back(page);
                have_page = false;
            };
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

                if (!have_page || page_series != series_id ||
                    page.point_count == kDiskPageRecords) {
                    flush_page();
                    page = SstablePageIndexEntry{};
                    page.offset = record_offset;
                    page_series = series_id;
                    have_page = true;
                }
                ++page.point_count;
                page.min_timestamp = std::min(page.min_timestamp, timestamp);
                page.max_timestamp = std::max(page.max_timestamp, timestamp);
            }
            flush_page();
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
            if (line.rfind("WAL_DIR ", 0) == 0) {
                // A different WAL directory would orphan unflushed segments,
                // silently losing data, so refuse to open.
                const std::string recorded = line.substr(8);
                if (NormalizePath(recorded) != NormalizePath(wal_dir)) {
                    return Status::Error(
                        "WAL directory changed since last open");
                }
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
        if (!SyncData(fd) || !SyncDirectory(wal_dir)) {
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
        std::vector<char> shard_ok(per_shard.size(), 0);
        std::vector<std::thread> writers;
        for (size_t shard_id = 0; shard_id < per_shard.size(); ++shard_id) {
            if (per_shard[shard_id].empty()) continue;
            shard_ok[shard_id] = 1;
            writers.emplace_back([this, shard_id, &per_shard, &shard_ok] {
                for (const PendingWrite* request : per_shard[shard_id]) {
                    std::vector<DataPoint> points;
                    for (const auto& point : request->points) {
                        if (ShardFor(point.series_id) == shard_id) {
                            points.push_back(point);
                        }
                    }
                    if (!AppendWalRecordNoSync(
                            shard_wal_fd[shard_id], kTxnRecord,
                            WalPayloadTxn(request->version, points))) {
                        shard_ok[shard_id] = 0;
                        return;
                    }
                }
                if (!SyncData(shard_wal_fd[shard_id])) shard_ok[shard_id] = 0;
            });
        }
        for (auto& writer : writers) writer.join();
        for (size_t shard_id = 0; shard_id < per_shard.size(); ++shard_id) {
            if (per_shard[shard_id].empty()) continue;
            if (shard_ok[shard_id] == 0) {
                ok = false;
            } else {
                ++wal_syncs;
            }
        }
        if (!ok) return false;
        std::string payload;
        Append(payload, first_version);
        Append(payload, next_version - 1);
        if (!AppendWalRecordNoSync(commit_wal_fd, kCommitRangeRecord, payload)) {
            return false;
        }
        FaultPoint("wal_before_sync");
        if (!SyncData(commit_wal_fd)) return false;
        FaultPoint("wal_after_sync");
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
                    if (::truncate(path.c_str(), static_cast<off_t>(valid)) != 0) {
                        return Status::Error("cannot truncate WAL tail");
                    }
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
                    if (::truncate(path.c_str(), static_cast<off_t>(valid)) != 0) {
                        return Status::Error("cannot truncate WAL tail");
                    }
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
        if (commit_wal_fd < 0) return false;
        for (size_t i = 0; i < shards.size(); ++i) {
            if (!SyncData(shard_wal_fd[i]) || !SyncDirectory(ShardWalDir(i))) return false;
        }
        return SyncData(commit_wal_fd) && SyncDirectory(commit_wal_dir) &&
               SyncDirectoryChain(wal_dir) && SyncDirectoryChain(root);
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
            if (!SyncData(new_fd) || !SyncDirectory(dir)) {
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
            if (!impl_->storage_error.ok) {
                for (const auto& request : impl_->write_queue) {
                    request->status = impl_->storage_error;
                    request->done = true;
                }
                impl_->write_queue.clear();
                impl_->write_cv.notify_all();
                continue;
            }

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

            // Backpressure: a burst of flushes can outrun compaction, and every
            // live SSTable keeps a full per-series index. Pause the group until
            // compaction drains the backlog so peak RSS stays bounded.
            if (impl_->options.max_live_sstables > 0 &&
                impl_->options.enable_background_compaction) {
                while (impl_->sstable_metadata.size() >
                           impl_->options.max_live_sstables &&
                       !impl_->stopping_write &&
                       !impl_->stopping_compaction && impl_->storage_error.ok) {
                    // The normal size-tier threshold may never be reached.
                    // Force a two-file round under pressure. A file set with
                    // one file per shard/level cannot shrink; treat the limit as
                    // soft in that irreducible state instead of deadlocking.
                    if (!impl_->compacting && !impl_->ShouldCompactLocked(2)) break;
                    impl_->compaction_requested = true;
                    impl_->compaction_cv.notify_one();
                    impl_->compaction_done_cv.wait_for(
                        lock, std::chrono::milliseconds(5));
                }
            }

            if (!impl_->storage_error.ok) continue;
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
                if (ok) {
                    const auto sync_started =
                        std::chrono::steady_clock::now();
                    FaultPoint("wal_before_sync");
                    ok = SyncData(impl_->wal_fd);
                    if (ok) FaultPoint("wal_after_sync");
                    impl_->wal_sync_ns += static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - sync_started)
                            .count());
                }
                if (ok) ++impl_->wal_syncs;
                if (ok) {
                    for (const auto& request : group) {
                        impl_->wal_bytes_written += static_cast<uint64_t>(
                            request->wal_end - request->wal_start);
                    }
                }
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
                // Failed fsync is indeterminate, especially for shard fragments:
                // reusing these versions can resurrect an old failed batch.
                impl_->RecordStorageError(Status::Error("WAL sync failed; reopen required"));
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
            const auto flush_status = FlushLocked(true);
            const bool flushed = flush_status.ok;
            if (!flushed) impl_->RecordStorageError(flush_status);
            impl_->write_cv.notify_all();
            if (flushed && impl_->options.enable_background_gc) {
                const auto gc_status = impl_->GarbageCollectLocked();
                if (!gc_status.ok) impl_->RecordStorageError(gc_status);
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
                for (size_t round = 0; round < 32; ++round) {
                    const bool pressure = impl_->options.max_live_sstables > 0 &&
                        impl_->sstable_metadata.size() > impl_->options.max_live_sstables;
                    const size_t threshold = pressure ? 2 :
                        impl_->options.compaction_sstable_threshold;
                    if (impl_->stopping_compaction || !impl_->ShouldCompactLocked(threshold)) break;
                    const auto compact_status = CompactRoundLocked(lock, threshold);
                    if (!compact_status.ok) {
                        impl_->RecordStorageError(compact_status);
                        break;
                    }
                }
                if (impl_->options.enable_background_gc) {
                    (void)impl_->GarbageCollectLocked();
                }
                // Wake writers paused by max_live_sstables backpressure.
                impl_->compaction_done_cv.notify_all();
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
    if (options.shard_count == 0 || options.shard_count > 99) {
        return fail(Status::Error("shard_count must be between 1 and 99"));
    }
    if (options.max_pending_batches == 0 || options.memtable_max_points == 0 ||
        options.hot_series_point_limit == 0 ||
        options.max_memtable_bytes < sizeof(DataPoint) + sizeof(CommitVersion)) {
        return fail(Status::Error("write capacity limits must be positive"));
    }
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
    // WAL segments may live on a separate disk: Options::wal_dir overrides the
    // default <db>/wal location.
    impl->wal_dir = options.wal_dir.empty() ? (path / "wal") : options.wal_dir;
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
    if (!SyncData(impl->wal_fd) || !SyncDirectoryChain(impl->wal_dir) ||
        !SyncDirectoryChain(path)) {
        return fail(Status::Error("cannot persist WAL directory"));
    }
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
    if (!impl_->storage_error.ok) return impl_->storage_error;
    constexpr size_t kPointBytes = sizeof(DataPoint) + sizeof(CommitVersion);
    if (points.size() > impl_->options.max_memtable_bytes / kPointBytes) {
        return Status::Error("batch exceeds memtable capacity");
    }
    const size_t batch_bytes = points.size() * kPointBytes;
    std::unordered_map<uint64_t, uint64_t> counts;
    for (const auto& point : points) {
        if (++counts[point.series_id] > impl_->options.hot_series_point_limit) {
            return Status::Error("batch exceeds hot series capacity");
        }
    }
    while (true) {
        if (impl_->closed || impl_->closing) return Status::Error("engine is closed");
        if (!impl_->storage_error.ok) return impl_->storage_error;
        // Waits release the mutex: another writer or ExpireBefore can invalidate
        // this batch. Validate again before reserving capacity, not just once.
        auto status = impl_->ValidateBatch(points);
        if (!status.ok) return status;
        if (impl_->write_queue.size() >= impl_->options.max_pending_batches) {
            FaultPoint("write_queue_wait");
            impl_->write_cv.wait(lock);
            continue;
        }
        size_t reserved_bytes = 0;
        // Only the candidate's series can violate its hot-series reservation.
        // Copying the entire live-series map here costs O(series_count) per
        // batch (10000 allocations in the normal uniform workload).
        std::unordered_map<uint64_t, uint64_t> projected;
        projected.reserve(counts.size());
        for (const auto& [series, count] : counts) {
            (void)count;
            const auto current = impl_->hot_series_points.find(series);
            projected.emplace(series, current == impl_->hot_series_points.end()
                                      ? 0 : current->second);
        }
        for (const auto& pending : impl_->write_queue) {
            reserved_bytes += pending->points.size() * kPointBytes;
            for (const auto& point : pending->points) {
                const auto found = projected.find(point.series_id);
                if (found != projected.end()) ++found->second;
            }
        }
        bool pressure = impl_->memtable_bytes >
                            impl_->options.max_memtable_bytes - batch_bytes ||
                        reserved_bytes > impl_->options.max_memtable_bytes -
                            batch_bytes - impl_->memtable_bytes;
        for (const auto& [series, count] : counts) {
            if (projected[series] > impl_->options.hot_series_point_limit - count) {
                pressure = true;
            }
        }
        if (!pressure) break;
        impl_->flush_requested = true;
        impl_->flush_cv.notify_one();
        FaultPoint("write_pressure_wait");
        impl_->write_cv.wait(lock);
    }
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
                    int64_t end, std::unique_ptr<Cursor>* cursor) const try {
    if (!cursor) return Status::Error("cursor output is null");
    if (start > end) return Status::Error("start must not exceed end");
    // 查询打开/首条解码失败也必须归还 view 和 gate；获取顺序保持不变。
    struct ScanLease {
        std::shared_ptr<Impl> impl;
        ReadView view;
        bool active_view{false};
        bool active_scan{false};

        ~ScanLease() {
            if (active_view) {
                std::lock_guard lock(impl->mutex);
                --impl->active_read_views;
                for (const auto& sstable : view.sstables) --sstable->refs;
            }
            view = {};
            if (active_scan) {
                {
                    std::lock_guard gate(impl->scan_mu);
                    --impl->active_scans;
                }
                impl->scan_cv.notify_one();
            }
        }
    };
    auto lease = std::make_shared<ScanLease>();
    lease->impl = impl_;
    std::unique_lock lock(impl_->mutex);
    if (impl_->closed || impl_->closing) return Status::Error("engine is closed");
    if (!impl_->storage_error.ok) return impl_->storage_error;
    lease->view = impl_->ViewLocked();
    const auto& view = lease->view;
    ++impl_->active_read_views;
    lease->active_view = true;
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
        lease->active_scan = true;
    }

    // Streaming scan: one source per (series, SSTable) plus one per series for
    // the memtable snapshot. Sources decode on demand and are merged by
    // (timestamp, series_id), so memory is proportional to the number of
    // sources rather than to the number of points returned.
    struct Source {
        uint64_t series_id{};
        std::shared_ptr<const SeriesPoints> memtable;
        size_t mem_index{0};
        size_t mem_end{0};
        std::unique_ptr<QueryReader> file;
        uint64_t remaining{0};
        // Page index of the series chunk (encoded SSTables only). Drives
        // seeking past pages whose time range misses the query.
        const std::vector<SstablePageIndexEntry>* pages{nullptr};
        size_t page_index{0};
        uint64_t file_end{0};
        // Legacy raw (fixed record) SSTables are still readable.
        bool raw{false};
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
            source->mem_index = source->memtable->LowerBoundIndex(
                std::max(start, view.cutoff));
            source->mem_end = source->memtable->LowerBoundIndex(end);
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
            source->file_end = entry.offset + entry.byte_length;
            source->file = std::make_unique<QueryReader>(
                sstable->path, entry.offset, source->file_end);
            if (sstable->encoding == kEncodedSstableMagic) {
                // A range that covers the whole chunk decodes sequentially; only
                // selective ranges pay for page traversal/seeking.
                const bool full_range =
                    start <= entry.min_timestamp && end > entry.max_timestamp;
                if (full_range) {
                    source->remaining = entry.point_count;
                    source->pages = nullptr;
                } else {
                    source->remaining = 0;
                    source->pages = &entry.pages;
                    source->page_index = 0;
                }
            } else {
                source->raw = true;
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
                while (source.mem_index < source.mem_end &&
                       (*source.memtable)[source.mem_index].version > visible) {
                    ++source.mem_index;
                }
                if (source.mem_index >= source.mem_end) return;
                const auto& point = (*source.memtable)[source.mem_index++];
                item.timestamp = point.point.timestamp_ms;
                item.value = point.point.value;
                item.version = point.version;
                state->heap.push(item);
                return;
            }
            if (source.remaining == 0) {
                // Encoded sources advance page by page, seeking past pages that
                // do not intersect [start, end) (or are fully expired).
                if (source.raw || !source.pages) return;
                bool found = false;
                while (source.page_index < source.pages->size()) {
                    const auto& page = (*source.pages)[source.page_index];
                    ++source.page_index;
                    if (page.max_timestamp < state->start ||
                        page.min_timestamp >= state->end ||
                        page.max_timestamp < cutoff) {
                        continue;
                    }
                    // 窄查询预读截止于选中页，不把后续未命中页带入缓冲。
                    const uint64_t page_end =
                        source.page_index < source.pages->size()
                            ? (*source.pages)[source.page_index].offset
                            : source.file_end;
                    source.file->Seek(page.offset, page_end);
                    source.remaining = page.point_count;
                    found = true;
                    break;
                }
                if (!found) return;
            }
            if (source.raw) {
                constexpr size_t kRecord = sizeof(uint64_t) + sizeof(int64_t) +
                                           sizeof(double) + sizeof(CommitVersion);
                char record[kRecord];
                source.file->ReadBytes(record, kRecord);
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
            source.file->ReadUvarint();  // series_id 由已校验的索引确定。
            const uint64_t zigzag_timestamp = source.file->ReadUvarint();
            const double disk_value = source.file->ReadDouble();
            const uint64_t disk_version = source.file->ReadUvarint();
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
        [lease = std::move(lease), state]() mutable {
            // Cursor 先销毁 generator，再释放 decoder/view，最后归还 gate。
            state.reset();
            lease.reset();
        });
    return Status::Ok();
} catch (const std::system_error& error) {
    return Status::Error(error.what());
} catch (const std::runtime_error& error) {
    return Status::Error(error.what());
}

Status Engine::Aggregate(const std::vector<uint64_t>& series_ids, int64_t start,
                         int64_t end, int64_t window_ms, AggregateOp operation,
                         std::unique_ptr<Cursor>* cursor) const try {
    if (!cursor) return Status::Error("cursor output is null");
    if (operation != AggregateOp::Count && operation != AggregateOp::Sum &&
        operation != AggregateOp::Min && operation != AggregateOp::Max &&
        operation != AggregateOp::Avg) return Status::Error("invalid aggregate operation");
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
        // 128-bit intermediates: [start, end) can span more than INT64_MAX, so
        // timestamp - start, quotient * window and the bucket start all leave the
        // 64-bit range before being clamped back into it.
        const Int128 offset = static_cast<Int128>(point->timestamp_ms) - start;
        Int128 quotient = offset / window_ms;
        if (offset < 0 && offset % window_ms != 0) --quotient;  // floor
        const Int128 bucket_start =
            static_cast<Int128>(start) + quotient * window_ms;
        if (bucket_start < static_cast<Int128>(
                               std::numeric_limits<int64_t>::min()) ||
            bucket_start > static_cast<Int128>(
                               std::numeric_limits<int64_t>::max())) {
            return Status::Error("aggregate bucket range overflow");
        }
        auto& bucket_state = buckets[{static_cast<int64_t>(bucket_start),
                                      point->series_id}];
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
        // The last bucket is truncated at `end`, and key.first + window can
        // overflow int64_t, so the clamp runs in 128 bits.
        const Int128 bucket_end = static_cast<Int128>(key.first) + window_ms;
        const int64_t clamped_end =
            bucket_end > static_cast<Int128>(end)
                ? end
                : static_cast<int64_t>(bucket_end);
        results.push_back({key.second, key.first, clamped_end, result,
                           bucket_state.count});
    }
    *cursor = std::make_unique<Cursor>(std::move(results));
    return Status::Ok();
} catch (const std::system_error& error) {
    return Status::Error(error.what());
} catch (const std::runtime_error& error) {
    return Status::Error(error.what());
}

Status Engine::ExpireBefore(int64_t cutoff, CommitVersion* version) {
    std::unique_lock lock(impl_->mutex);
    if (impl_->closed || impl_->closing) return Status::Error("engine is closed");
    if (!impl_->storage_error.ok) return impl_->storage_error;
    if (cutoff < impl_->cutoff) return Status::Error("cutoff must not move backwards");
    if (cutoff == impl_->cutoff) {
        if (version) *version = impl_->next_version - 1;
        return Status::Ok();
    }
    impl_->write_cv.wait(lock, [&] {
        return impl_->write_queue.empty() || impl_->closing || !impl_->storage_error.ok;
    });
    if (impl_->closed || impl_->closing) return Status::Error("engine is closed");
    if (!impl_->storage_error.ok) return impl_->storage_error;
    // Another expiration may have advanced cutoff while this wait released lock.
    if (cutoff < impl_->cutoff) return Status::Error("cutoff must not move backwards");
    if (cutoff == impl_->cutoff) {
        if (version) *version = impl_->visible_version;
        return Status::Ok();
    }
    const CommitVersion commit = impl_->next_version++;
    const int expire_fd =
        impl_->sharded_wal ? impl_->commit_wal_fd : impl_->wal_fd;
    if (!AppendWalRecord(expire_fd, kExpireRecord,
                         WalPayloadExpire(commit, cutoff))) {
        impl_->RecordStorageError(Status::Error("WAL sync failed; reopen required"));
        return impl_->storage_error;
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
        if (!publish_status.ok) {
            impl_->RecordStorageError(publish_status);
            return publish_status;
        }
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
            // 有序 buffer 只需检查首点；仅复制确实需要删除前缀的 series。
            if (series_it->second->empty() ||
                series_it->second->front().point.timestamp_ms >= cutoff) {
                ++series_it;
                continue;
            }
            if (series_it->second.use_count() != 1) {
                series_it->second =
                    std::make_shared<SeriesPoints>(*series_it->second);
            }
            auto& points = *series_it->second;
            const size_t removed = points.ExpireBefore(cutoff);
            if (removed > 0) {
                impl_->memtable_bytes -= removed * kRecordBytes;
                impl_->hot_series_points[series_it->first] -= removed;
                impl_->points_since_flush -= std::min(impl_->points_since_flush, removed);
            }
            if (points.empty()) {
                series_it = points_by_series.erase(series_it);
            } else {
                ++series_it;
            }
        }
    }
    impl_->visible_version = commit;
    impl_->write_cv.notify_all();
    if (version) *version = commit;
    return Status::Ok();
}

Status Engine::FlushLocked(bool incremental) {
    const NsTimer flush_timer(&impl_->flush_ns);
    (void)incremental;
    if (impl_->closed) return Status::Error("engine is closed");
    if (!impl_->storage_error.ok) return impl_->storage_error;

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
                for (const auto& value : *points) {
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
            FaultPoint("sstable_before_rename");
            std::filesystem::rename(job->temp, job->file, ec);
            if (ec) {
                job->status = Status::Error("SSTable publish failed");
                return;
            }
            if (!SyncDirectory(job->file.parent_path())) {
                job->status = Status::Error("SSTable directory sync failed");
                return;
            }
            FaultPoint("sstable_after_rename");
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
    if (!status.ok) {
        impl_->RecordStorageError(status);
        return status;
    }
    FaultPoint("flush_after_manifest");
    impl_->sstable_metadata.insert(impl_->sstable_metadata.end(),
                                   published_metadata.begin(),
                                   published_metadata.end());
    impl_->last_flushed_version = impl_->next_version - 1;
    impl_->points_since_flush = 0;
    impl_->hot_series_points.clear();
    impl_->ClearMemtableLocked();
    ++impl_->flush_count;
    impl_->write_cv.notify_all();
    // A failed directory sync can leave the next WAL name non-durable. Do not
    // acknowledge further writes to that fd even though this flush is durable.
    const bool rotated = impl_->sharded_wal ? impl_->RotateShardedWalLocked()
                                            : impl_->RotateWalLocked();
    if (!rotated) {
        auto error = Status::Error("WAL rotation failed; reopen required");
        impl_->RecordStorageError(error);
        return error;
    }
    return Status::Ok();
}
Status Engine::CompactRoundLocked(std::unique_lock<std::mutex>& lock,
                                  size_t threshold) {
    const NsTimer compaction_timer(&impl_->compaction_ns);
    if (!impl_->storage_error.ok) return impl_->storage_error;
    impl_->compaction_done_cv.wait(lock, [&] {
        return !impl_->compacting || !impl_->storage_error.ok;
    });
    if (!impl_->storage_error.ok) return impl_->storage_error;
    auto jobs = impl_->PlanCompactionLocked(threshold);
    if (jobs.empty()) return Status::Ok();
    impl_->compacting = true;
    lock.unlock();
    FaultPoint("compaction_merge_begin");
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
    impl_->compacting = false;
    impl_->compaction_done_cv.notify_all();
    if (!impl_->storage_error.ok) merge_status = impl_->storage_error;
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
    const auto status = FlushLocked(true);
    if (!status.ok) impl_->RecordStorageError(status);
    impl_->write_cv.notify_all();
    return status;
}

Status Engine::Compact() {
    std::unique_lock lock(impl_->mutex);
    if (impl_->closed || impl_->closing) return Status::Error("engine is closed");
    if (!impl_->storage_error.ok) return impl_->storage_error;
    // Bounded: with writers flushing continuously a shard can refill its level
    // as fast as it is merged, which would keep this loop spinning forever.
    for (size_t round = 0; round < 64 && impl_->ShouldCompactLocked(2); ++round) {
        auto status = CompactRoundLocked(lock, 2);
        if (!status.ok) {
            impl_->RecordStorageError(status);
            return status;
        }
    }
    return Status::Ok();
}


Status Engine::GarbageCollect() {
    std::lock_guard lock(impl_->mutex);
    if (impl_->closed || impl_->closing) return Status::Error("engine is closed");
    return impl_->GarbageCollectLocked();
}

Status Engine::Close() {
    {
        std::lock_guard lock(impl_->mutex);
        if (impl_->closed) return Status::Ok();
        if (impl_->closing) return Status::Error("close already in progress");
        impl_->closing = true;
        FaultPoint("close_begin");
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
    // Always release descriptors and mark closed, even if the final flush fails.
    // Otherwise destruction leaves a permanently closing, half-stopped engine.
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
    return status;
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
    stats->wal_sync_ns = impl_->wal_sync_ns;
    stats->wal_bytes_written = impl_->wal_bytes_written;
    stats->flush_ns = impl_->flush_ns;
    stats->compaction_ns = impl_->compaction_ns;
    uint64_t series_entries = 0;
    uint64_t page_entries = 0;
    for (const auto& sstable : impl_->sstable_metadata) {
        series_entries += sstable->series.size();
        for (const auto& [series_id, entry] : sstable->series) {
            (void)series_id;
            page_entries += entry.pages.size();
        }
    }
    stats->series_index_entries = series_entries;
    stats->page_index_entries = page_entries;
    stats->compaction_count = impl_->compaction_count;
    stats->flush_count = impl_->flush_count;
    stats->pending_batches = impl_->write_queue.size();
    stats->active_read_views = impl_->active_read_views;
    stats->obsolete_sstable_count = impl_->obsolete_sstables.size();
    stats->live_sstable_count = impl_->sstable_metadata.size();
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
    stats->rss_bytes = 0;
#if defined(__linux__)
    // statm reports current resident pages, not the lifetime ru_maxrss peak.
    std::ifstream statm("/proc/self/statm");
    uint64_t virtual_pages = 0, resident_pages = 0;
    const long page_size = ::sysconf(_SC_PAGESIZE);
    if (page_size > 0 && (statm >> virtual_pages >> resident_pages)) {
        stats->rss_bytes = resident_pages * static_cast<uint64_t>(page_size);
    }
#elif defined(__APPLE__)
    mach_task_basic_info_data_t info{};
    mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
    if (::task_info(mach_task_self(), MACH_TASK_BASIC_INFO,
                    reinterpret_cast<task_info_t>(&info), &count) == KERN_SUCCESS) {
        stats->rss_bytes = info.resident_size;
    }
#endif
    stats->engine_threads = 2 +
                            (impl_->options.enable_background_compaction ? 1 : 0) +
                            impl_->shards.size();
    stats->memtable_bytes = impl_->memtable_bytes;
    return Status::Ok();
}

namespace testing {

void SetFaultHook(std::function<void(const char*)> hook) {
    std::lock_guard lock(fault_hook_mutex);
    fault_hook = std::move(hook);
}

SeriesBufferInfo InspectSeriesBuffer(const Engine& engine, uint64_t series_id) {
    const auto& impl = *engine.impl_;
    std::lock_guard lock(impl.mutex);
    const size_t shard_id = impl.ShardFor(series_id);
    std::lock_guard shard_lock(*impl.shard_mutex[shard_id]);
    const auto& series = impl.shards[shard_id]->series;
    const auto it = series.find(series_id);
    if (it == series.end() || !it->second) return {};
    return {it->second->size(), it->second->capacity()};
}

}  // namespace testing

}  // namespace tsdb
