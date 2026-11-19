#include "tsdb.h"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <vector>

namespace {

// WAL segments are numbered; the newest one is the active log.
std::filesystem::path NewestWalSegment(const std::filesystem::path& path) {
    std::filesystem::path newest;
    uint64_t best = 0;
    std::error_code ec;
    for (const auto& entry :
         std::filesystem::directory_iterator(path / "wal", ec)) {
        if (!entry.is_regular_file()) continue;
        if (entry.path().extension() != ".log") continue;
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
    return newest;
}

}  // namespace

int main() {
    size_t injected = 0;

    for (size_t i = 0; i < 50; ++i) {
        const auto path =
            std::filesystem::temp_directory_path() /
            ("tsdb-fault-" + std::to_string(i));
        std::filesystem::remove_all(path);

        tsdb::Status status;
        auto engine = tsdb::Engine::Open(
            path, tsdb::Options{.shard_count = 4,
                                .memtable_max_points = 1000},
            &status);
        assert(status.ok);
        tsdb::CommitVersion version = 0;
        assert(engine->WriteBatch({{1, static_cast<int64_t>(i), 1.25}},
                                  &version).ok);
        assert(engine->Flush().ok);
        assert(engine->Close().ok);

        const auto manifest = path / "MANIFEST";
        if (i % 5 == 0) {
            std::ofstream damaged(manifest, std::ios::binary |
                                           std::ios::app);
            damaged << "BROKEN_LINE\n";
            damaged.close();
        } else if (i % 5 == 1) {
            std::ofstream tmp(path / "sst" / "orphan.sst.tmp",
                              std::ios::binary);
            tmp << "orphan";
        } else if (i % 5 == 2) {
            std::ofstream wal(NewestWalSegment(path),
                              std::ios::binary | std::ios::app);
            wal << "T";
        } else if (i % 5 == 3) {
            for (const auto& entry :
                 std::filesystem::recursive_directory_iterator(path / "sst")) {
                if (entry.path().extension() == ".sst") {
                    std::fstream file(entry.path(),
                                      std::ios::binary |
                                      std::ios::in |
                                      std::ios::out);
                    file.seekp(-1, std::ios::end);
                    char byte = 0;
                    file.read(&byte, 1);
                    byte ^= 0xff;
                    file.seekp(-1, std::ios::end);
                    file.write(&byte, 1);
                    break;
                }
            }
        }

        auto reopened = tsdb::Engine::Open(
            path, tsdb::Options{.shard_count = 4,
                                .memtable_max_points = 1000},
            &status);
        if (i % 5 == 0 || i % 5 == 3) {
            assert(reopened == nullptr);
        } else {
            assert(reopened != nullptr);
            std::unique_ptr<tsdb::Cursor> cursor;
            assert(reopened->Scan({1}, 0, 1000, &cursor).ok);
            assert(reopened->Close().ok);
        }
        ++injected;
        std::filesystem::remove_all(path);
    }

    std::cout << "fault_injections=" << injected << '\n';
    return 0;
}
