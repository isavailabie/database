#include "tsdb.h"

#include "check.h"

#include <array>
#include <atomic>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {

constexpr int kCrashExit = 86;
constexpr int kChildFailureExit = 87;
constexpr int kHookMissExit = 88;
constexpr size_t kRoundsPerStage = 10;
constexpr int64_t kExpirationCutoff = 300;
constexpr int64_t kRecoveryExpirationCutoff = kExpirationCutoff + 1;
constexpr tsdb::CommitVersion kAcknowledgedVersion = 5;
constexpr tsdb::CommitVersion kCandidateVersion = 6;

struct FaultStage {
    const char* name;
    enum Operation { Wal, Flush, Compaction, GC } operation;
};

constexpr std::array<FaultStage, 9> kStages{{
    {"wal_before_sync", FaultStage::Wal},
    {"wal_after_sync", FaultStage::Wal},
    {"sstable_before_rename", FaultStage::Flush},
    {"sstable_after_rename", FaultStage::Flush},
    {"flush_after_manifest", FaultStage::Flush},
    {"compaction_before_manifest", FaultStage::Compaction},
    {"compaction_after_manifest", FaultStage::Compaction},
    {"gc_before_delete", FaultStage::GC},
    {"gc_after_delete", FaultStage::GC},
}};

struct TempRoot {
    std::filesystem::path path;

    TempRoot() {
        auto name = (std::filesystem::temp_directory_path() /
                     "tsdb-fault-XXXXXX").string();
        std::vector<char> buffer(name.begin(), name.end());
        buffer.push_back('\0');
        const char* result = ::mkdtemp(buffer.data());
        CHECK(result != nullptr);
        path = result;
    }

    ~TempRoot() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
};

tsdb::Options TestOptions(bool sharded_wal = false) {
    tsdb::Options options;
    options.shard_count = 4;
    options.memtable_max_points = 100000;
    options.enable_background_compaction = false;
    options.enable_background_gc = false;
    options.sharded_wal = sharded_wal;
    return options;
}

std::unique_ptr<tsdb::Engine> OpenEngine(const std::filesystem::path& path,
                                        bool sharded_wal) {
    tsdb::Status status;
    auto engine = tsdb::Engine::Open(path, TestOptions(sharded_wal), &status);
    CHECK(status.ok);
    CHECK(engine != nullptr);
    return engine;
}

using PointKey = std::pair<uint64_t, int64_t>;
using Points = std::map<PointKey, double>;

std::vector<uint64_t> SeriesIds() {
    std::vector<uint64_t> ids;
    for (uint64_t series = 0; series < 16; ++series) ids.push_back(series);
    return ids;
}

std::vector<tsdb::DataPoint> Batch(size_t batch, size_t round,
                                 tsdb::CommitVersion version) {
    std::vector<tsdb::DataPoint> points;
    for (uint64_t series : SeriesIds()) {
        for (int64_t offset = 0; offset < 8; ++offset) {
            int64_t timestamp = 0;
            switch (batch) {
                case 0: timestamp = 10 + offset; break;
                case 1: timestamp = 120 + offset; break;
                case 2: timestamp = 200 + offset; break;
                case 3: timestamp = kExpirationCutoff + offset; break;
                case 4: timestamp = 400 + offset; break;
                default: timestamp = static_cast<int64_t>(batch) * 100 + offset;
            }
            const double value = static_cast<double>(version * 100000 +
                round * 1000 + series * 10 + static_cast<uint64_t>(offset)) + 0.25;
            points.push_back({series, timestamp, value});
        }
    }
    return points;
}

void Apply(Points* expected, const std::vector<tsdb::DataPoint>& batch) {
    for (const auto& point : batch) {
        (*expected)[{point.series_id, point.timestamp_ms}] = point.value;
    }
}

void Expire(Points* expected, int64_t cutoff) {
    for (auto it = expected->begin(); it != expected->end();) {
        if (it->first.second < cutoff) it = expected->erase(it);
        else ++it;
    }
}

Points ReadPoints(tsdb::Engine& engine) {
    std::unique_ptr<tsdb::Cursor> cursor;
    CHECK(engine.Scan(SeriesIds(), 0, 1000, &cursor).ok);
    CHECK(cursor != nullptr);
    Points result;
    while (auto point = cursor->NextPoint()) {
        CHECK(result.emplace(PointKey{point->series_id, point->timestamp_ms},
                             point->value).second);
    }
    cursor->Close();
    cursor.reset();
    return result;
}

void CheckState(tsdb::Engine& engine, const Points& expected,
                tsdb::CommitVersion version, int64_t cutoff) {
    CHECK(ReadPoints(engine) == expected);
    tsdb::EngineStats stats;
    CHECK(engine.GetStats(&stats).ok);
    CHECK(stats.visible_version == version);
    CHECK(stats.expiration_cutoff == cutoff);
    CHECK(stats.active_read_views == 0);
}

struct Acknowledgement {
    enum Kind : uint64_t { Write, Expiration } kind;
    uint64_t batch;
    tsdb::CommitVersion version;
    int64_t cutoff;
};

void SendAcknowledgement(int fd, const Acknowledgement& ack) {
    const auto* bytes = reinterpret_cast<const char*>(&ack);
    size_t sent = 0;
    while (sent < sizeof(ack)) {
        const ssize_t count = ::write(fd, bytes + sent, sizeof(ack) - sent);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) ::_exit(kChildFailureExit);
        sent += static_cast<size_t>(count);
    }
}

std::vector<Acknowledgement> ReadAcknowledgements(int fd) {
    std::vector<Acknowledgement> result;
    Acknowledgement ack{};
    auto* bytes = reinterpret_cast<char*>(&ack);
    size_t received = 0;
    while (true) {
        const ssize_t count = ::read(fd, bytes + received, sizeof(ack) - received);
        if (count < 0 && errno == EINTR) continue;
        CHECK(count >= 0);
        if (count == 0) {
            CHECK(received == 0);
            return result;
        }
        received += static_cast<size_t>(count);
        if (received == sizeof(ack)) {
            result.push_back(ack);
            received = 0;
        }
    }
}

void CrashWorkload(const std::filesystem::path& path, bool sharded_wal,
                   const FaultStage& stage, size_t round, int ack_fd) {
    auto engine = OpenEngine(path, sharded_wal);
    for (size_t batch = 0; batch < 3; ++batch) {
        tsdb::CommitVersion version = 0;
        CHECK(engine->WriteBatch(Batch(batch, round, batch + 1), &version).ok);
        CHECK(version == batch + 1);
        SendAcknowledgement(ack_fd, {Acknowledgement::Write, batch, version, 0});
        // 同一组 series 的多次 Flush 保证每个有数据的 shard 有多个 SST。
        CHECK(engine->Flush().ok);
    }
    tsdb::CommitVersion version = 0;
    CHECK(engine->ExpireBefore(kExpirationCutoff, &version).ok);
    CHECK(version == 4);
    SendAcknowledgement(ack_fd,
                        {Acknowledgement::Expiration, 0, version, kExpirationCutoff});
    CHECK(engine->WriteBatch(Batch(3, round, kAcknowledgedVersion), &version).ok);
    CHECK(version == kAcknowledgedVersion);
    SendAcknowledgement(ack_fd, {Acknowledgement::Write, 3, version, 0});
    // Expiration removes all three old file sets. Keep the acknowledged batch
    // in its own SSTable so the candidate creates a second live file per shard.
    if (stage.operation == FaultStage::Compaction || stage.operation == FaultStage::GC) {
        CHECK(engine->Flush().ok);
    }

    // GC 的不同轮次在不同文件删除处退出，覆盖部分 OBSOLETE 已被删除的情况。
    size_t crash_on_hit = 1;
    if (stage.operation != FaultStage::Wal) {
        CHECK(engine->WriteBatch(Batch(4, round, kCandidateVersion), &version).ok);
        CHECK(version == kCandidateVersion);
        // 此批次故意不发送 ack，恢复时允许全有或全无，但禁止部分提交。
        if (stage.operation == FaultStage::Compaction ||
            stage.operation == FaultStage::GC) {
            CHECK(engine->Flush().ok);
            tsdb::EngineStats stats;
            CHECK(engine->GetStats(&stats).ok);
            CHECK(stats.sstable_count >= 2);
            if (stage.operation == FaultStage::GC) {
                CHECK(engine->Compact().ok);
                CHECK(engine->GetStats(&stats).ok);
                CHECK(stats.compaction_count > 0);
                CHECK(stats.obsolete_sstable_count >= 2);
                CHECK(stats.active_read_views == 0);
                crash_on_hit = 1 + round % stats.obsolete_sstable_count;
            }
        }
    }

    std::atomic<size_t> hits{0};
    tsdb::testing::SetFaultHook([&hits, name = stage.name, crash_on_hit](const char* name_seen) {
        if (std::strcmp(name_seen, name) == 0 &&
            hits.fetch_add(1, std::memory_order_relaxed) + 1 == crash_on_hit) {
            ::_exit(kCrashExit);
        }
    });

    // 安装 hook 后不能抛异常展开 Engine 析构；否则 Close 可能误命中 hook。
    tsdb::Status status;
    switch (stage.operation) {
        case FaultStage::Wal:
            status = engine->WriteBatch(Batch(4, round, kCandidateVersion), &version);
            break;
        case FaultStage::Flush: status = engine->Flush(); break;
        case FaultStage::Compaction: status = engine->Compact(); break;
        case FaultStage::GC: status = engine->GarbageCollect(); break;
    }
    ::_exit(status.ok ? kHookMissExit : kChildFailureExit);
}

int WaitFor(pid_t child) {
    int status = 0;
    pid_t waited;
    do {
        waited = ::waitpid(child, &status, 0);
    } while (waited < 0 && errno == EINTR);
    CHECK(waited == child);
    return status;
}

Points AcknowledgedPoints(const std::vector<Acknowledgement>& acks, size_t round) {
    CHECK(acks.size() == 5);
    Points expected;
    for (size_t i = 0; i < acks.size(); ++i) {
        const auto& ack = acks[i];
        CHECK(ack.version == i + 1);
        if (i == 3) {
            CHECK(ack.kind == Acknowledgement::Expiration);
            CHECK(ack.cutoff == kExpirationCutoff);
            Expire(&expected, ack.cutoff);
        } else {
            CHECK(ack.kind == Acknowledgement::Write);
            CHECK(ack.batch == (i == 4 ? 3 : i));
            CHECK(ack.cutoff == 0);
            Apply(&expected, Batch(ack.batch, round, ack.version));
        }
    }
    return expected;
}

// 磁盘上的 SST 必须恰好是 Manifest 登记的集合：崩溃留下的未发布输出、GC 中途
// 退出残留的已替换输入，都应在恢复打开时回收，正常关闭后也不得遗留。
void CheckDiskMatchesManifest(const std::filesystem::path& path) {
    std::set<std::string> on_disk;
    for (const auto& entry : std::filesystem::directory_iterator(path / "sst")) {
        if (entry.is_regular_file() && entry.path().extension() == ".sst") {
            on_disk.insert(entry.path().filename().string());
        }
    }
    std::ifstream in(path / "MANIFEST");
    CHECK(in.is_open());
    std::set<std::string> listed;
    std::string line;
    std::getline(in, line);
    while (std::getline(in, line)) {
        if (line.rfind("WAL_", 0) == 0 || line.rfind("COVERED ", 0) == 0 ||
            line.rfind("CHECKSUM ", 0) == 0) {
            continue;
        }
        std::istringstream row(line);
        size_t index = 0;
        std::string file;
        CHECK(static_cast<bool>(row >> index >> file));
        listed.insert(std::filesystem::path(file).filename().string());
    }
    CHECK(on_disk == listed);
}

void VerifyRecovery(const std::filesystem::path& path, bool sharded_wal,
                    size_t round, Points acknowledged) {
    auto engine = OpenEngine(path, sharded_wal);
    CheckDiskMatchesManifest(path);
    Points with_candidate = acknowledged;
    Apply(&with_candidate, Batch(4, round, kCandidateVersion));
    const Points recovered = ReadPoints(*engine);
    // 精确比较整个键值集合，检查跨 shard 新插入批次的原子性。
    CHECK(recovered == acknowledged || recovered == with_candidate);
    tsdb::CommitVersion version = recovered == with_candidate
        ? kCandidateVersion : kAcknowledgedVersion;
    Points expected = recovered;
    CheckState(*engine, expected, version, kExpirationCutoff);

    auto next_batch = Batch(5, round, version + 1);
    tsdb::CommitVersion next = 0;
    CHECK(engine->WriteBatch(next_batch, &next).ok);
    CHECK(next == ++version);
    Apply(&expected, next_batch);
    CHECK(engine->Flush().ok);
    CHECK(engine->ExpireBefore(kRecoveryExpirationCutoff, &next).ok);
    CHECK(next == ++version);
    Expire(&expected, kRecoveryExpirationCutoff);
    next_batch = Batch(6, round, version + 1);
    CHECK(engine->WriteBatch(next_batch, &next).ok);
    CHECK(next == ++version);
    Apply(&expected, next_batch);
    CHECK(engine->Flush().ok);
    CheckState(*engine, expected, version, kRecoveryExpirationCutoff);
    CHECK(engine->Close().ok);
    engine.reset();
    CheckDiskMatchesManifest(path);

    engine = OpenEngine(path, sharded_wal);
    CheckState(*engine, expected, version, kRecoveryExpirationCutoff);
    next_batch = Batch(7, round, version + 1);
    CHECK(engine->WriteBatch(next_batch, &next).ok);
    CHECK(next == ++version);
    Apply(&expected, next_batch);
    CHECK(engine->Flush().ok);
    CHECK(engine->Close().ok);
    engine.reset();

    engine = OpenEngine(path, sharded_wal);
    CheckState(*engine, expected, version, kRecoveryExpirationCutoff);
    CHECK(engine->GarbageCollect().ok);
    CHECK(engine->Close().ok);
    engine.reset();
    CheckDiskMatchesManifest(path);
}

void RunCrashRound(const std::filesystem::path& path, bool sharded_wal,
                   const FaultStage& stage, size_t round) {
    std::filesystem::create_directory(path);
    int ack_pipe[2];
    CHECK(::pipe(ack_pipe) == 0);
    const pid_t writer = ::fork();
    if (writer < 0) {
        ::close(ack_pipe[0]);
        ::close(ack_pipe[1]);
        CHECK(writer >= 0);
    }
    if (writer == 0) {
        ::close(ack_pipe[0]);
        ::alarm(30);
        // 写入线程只在 fork 后启动；ack 只在 WriteBatch/ExpireBefore 成功后发送。
        std::thread workload([&] {
            try {
                CrashWorkload(path, sharded_wal, stage, round, ack_pipe[1]);
            } catch (const std::exception& error) {
                std::cerr << "crash setup failed: " << error.what() << '\n';
                ::_exit(kChildFailureExit);
            }
        });
        workload.join();
        ::_exit(kHookMissExit);
    }
    ::close(ack_pipe[1]);
    const auto acknowledgements = ReadAcknowledgements(ack_pipe[0]);
    ::close(ack_pipe[0]);
    const int writer_status = WaitFor(writer);
    if (!WIFEXITED(writer_status) || WEXITSTATUS(writer_status) != kCrashExit) {
        std::cerr << "hook " << stage.name << " not hit, wait_status="
                  << writer_status << '\n';
    }
    CHECK(WIFEXITED(writer_status));
    CHECK(WEXITSTATUS(writer_status) == kCrashExit);
    const auto expected = AcknowledgedPoints(acknowledgements, round);

    // 父进程始终只监督，不创建 Engine；恢复也在独立子进程中进行。
    // 因而所有 fork 均发生在父进程持有任何 Engine 或工作线程之前。
    const pid_t verifier = ::fork();
    CHECK(verifier >= 0);
    if (verifier == 0) {
        ::alarm(30);
        try {
            VerifyRecovery(path, sharded_wal, round, expected);
            ::_exit(0);
        } catch (const std::exception& error) {
            std::cerr << "recovery failed: " << error.what() << '\n';
            ::_exit(kChildFailureExit);
        }
    }
    const int verifier_status = WaitFor(verifier);
    CHECK(WIFEXITED(verifier_status));
    CHECK(WEXITSTATUS(verifier_status) == 0);
    std::filesystem::remove_all(path);
}

// WAL 分段编号递增，最新段是允许存在不完整尾部的活动日志。
std::filesystem::path NewestWalSegment(const std::filesystem::path& path) {
    std::filesystem::path newest;
    uint64_t best = 0;
    for (const auto& entry : std::filesystem::directory_iterator(path / "wal")) {
        if (!entry.is_regular_file() || entry.path().extension() != ".log") continue;
        uint64_t number = 0;
        try {
            number = std::stoull(entry.path().stem().string());
        } catch (...) {
            continue;
        }
        if (newest.empty() || number > best) {
            best = number;
            newest = entry.path();
        }
    }
    CHECK(!newest.empty());
    return newest;
}

std::filesystem::path FirstSstable(const std::filesystem::path& path) {
    for (const auto& entry : std::filesystem::recursive_directory_iterator(path / "sst")) {
        if (entry.is_regular_file() && entry.path().extension() == ".sst") {
            return entry.path();
        }
    }
    CHECK(false);
    return {};
}

void StaticDamageRound(const std::filesystem::path& path, size_t round) {
    auto engine = OpenEngine(path, false);
    tsdb::CommitVersion version = 0;
    CHECK(engine->WriteBatch({{1, static_cast<int64_t>(round), 1.25}}, &version).ok);
    CHECK(version == 1);
    CHECK(engine->Flush().ok);
    CHECK(engine->Close().ok);
    engine.reset();

    switch (round % 5) {
        case 0: {
            std::ofstream damaged(path / "MANIFEST", std::ios::binary | std::ios::app);
            CHECK(damaged.is_open());
            damaged << "BROKEN_LINE\n";
            damaged.close();
            CHECK(damaged.good());
            break;
        }
        case 1: {
            std::ofstream tmp(path / "sst" / "orphan.sst.tmp", std::ios::binary);
            CHECK(tmp.is_open());
            tmp << "orphan";
            tmp.close();
            CHECK(tmp.good());
            break;
        }
        case 2: {
            std::ofstream wal(NewestWalSegment(path), std::ios::binary | std::ios::app);
            CHECK(wal.is_open());
            wal << "T";
            wal.close();
            CHECK(wal.good());
            break;
        }
        case 3: {
            std::fstream file(FirstSstable(path),
                              std::ios::binary | std::ios::in | std::ios::out);
            CHECK(file.is_open());
            file.seekg(-1, std::ios::end);
            char byte = 0;
            file.read(&byte, 1);
            CHECK(file.good());
            byte ^= 0xff;
            file.seekp(-1, std::ios::end);
            file.write(&byte, 1);
            file.close();
            CHECK(file.good());
            break;
        }
        case 4: {
            // 原实现 i % 5 == 4 未注入；现在截断真实 SST 的记录/校验和。
            const auto sstable = FirstSstable(path);
            const auto size = std::filesystem::file_size(sstable);
            CHECK(size > 8);
            std::filesystem::resize_file(sstable, size - 8);
            break;
        }
    }

    tsdb::Status status;
    engine = tsdb::Engine::Open(path, TestOptions(), &status);
    if (round % 5 == 0 || round % 5 == 3 || round % 5 == 4) {
        CHECK(engine == nullptr);
        CHECK(!status.ok);
    } else {
        CHECK(status.ok);
        CHECK(engine != nullptr);
        Points expected{{{1, static_cast<int64_t>(round)}, 1.25}};
        CheckState(*engine, expected, 1, std::numeric_limits<int64_t>::min());
        CHECK(engine->WriteBatch({{1, 900, 2.5}}, &version).ok);
        CHECK(version == 2);
        CHECK(engine->Flush().ok);
        CHECK(engine->Close().ok);
        engine.reset();
        engine = OpenEngine(path, false);
        expected[{1, 900}] = 2.5;
        CheckState(*engine, expected, 2, std::numeric_limits<int64_t>::min());
        CHECK(engine->Close().ok);
        engine.reset();
    }
    std::filesystem::remove_all(path);
}

}  // namespace

int main() {
    size_t actual_crashes = 0;
    size_t static_injections = 0;
    std::string context;
    try {
        TempRoot root;
        // 必须先完成所有 fork，再在父进程运行会创建 Engine 的静态损坏测试。
        for (bool sharded_wal : {false, true}) {
            const std::string mode = sharded_wal ? "sharded_wal" : "global_wal";
            for (const auto& stage : kStages) {
                size_t stage_crashes = 0;
                for (size_t round = 0; round < kRoundsPerStage; ++round) {
                    context = mode + "/" + stage.name + "/" + std::to_string(round);
                    const auto path = root.path /
                        (mode + "-" + stage.name + "-" + std::to_string(round));
                    RunCrashRound(path, sharded_wal, stage, round);
                    ++stage_crashes;
                    ++actual_crashes;
                }
                std::cout << "fault_stage=" << stage.name << " mode=" << mode
                          << " real_crashes=" << stage_crashes << std::endl;
            }
        }
        CHECK(actual_crashes == kStages.size() * kRoundsPerStage * 2);
        for (size_t round = 0; round < 50; ++round) {
            context = "static_damage/" + std::to_string(round);
            StaticDamageRound(root.path / ("static-" + std::to_string(round)), round);
            ++static_injections;
        }
    } catch (const std::exception& error) {
        std::cerr << "fault injection " << context << " failed: "
                  << error.what() << '\n';
        return 1;
    }

    std::cout << "static_fault_injections=" << static_injections
              << " real_process_crashes=" << actual_crashes
              << " fault_injections=" << static_injections + actual_crashes << '\n';
    return 0;
}
