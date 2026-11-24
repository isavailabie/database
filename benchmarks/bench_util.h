#pragma once

// Helpers shared by the benchmark drivers. Drivers currently link the engine
// in-process: their buffers are included in process RSS and must stay bounded.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cmath>
#include <random>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include <sys/resource.h>

namespace bench {

using Clock = std::chrono::steady_clock;

inline double Seconds(Clock::duration duration) {
    return std::chrono::duration<double>(duration).count();
}

// Peak resident set size of this process, as reported by the kernel.
inline uint64_t PeakRssBytes() {
    struct rusage usage {};
    if (::getrusage(RUSAGE_SELF, &usage) != 0) return 0;
#if defined(__APPLE__)
    return static_cast<uint64_t>(usage.ru_maxrss);
#else
    return static_cast<uint64_t>(usage.ru_maxrss) * 1024ULL;
#endif
}

// Sum of all regular files under `dir` (SSTables, WAL, Manifest, ...).
inline uint64_t DirectoryBytes(const std::filesystem::path& dir) {
    std::error_code ec;
    if (!std::filesystem::exists(dir, ec)) return 0;
    uint64_t total = 0;
    for (const auto& entry :
         std::filesystem::recursive_directory_iterator(dir, ec)) {
        if (!entry.is_regular_file(ec)) continue;
        std::error_code size_ec;
        const auto size = std::filesystem::file_size(entry.path(), size_ec);
        if (!size_ec) total += static_cast<uint64_t>(size);
    }
    return total;
}

// Bounded Algorithm R samples span the complete run, rather than just the
// first 50000 operations. One instance is owned by each workload thread.
struct LatencyReservoir {
    uint64_t seen{0};
    std::mt19937_64 random;
    explicit LatencyReservoir(uint64_t seed = 1) : random(seed) {}
    void Add(std::vector<double>& samples, double latency) {
        ++seen;
        constexpr size_t kLimit = 50000;
        if (samples.size() < kLimit) {
            samples.push_back(latency);
        } else {
            const auto index = std::uniform_int_distribution<uint64_t>(0, seen - 1)(random);
            if (index < kLimit) samples[static_cast<size_t>(index)] = latency;
        }
    }
};

// Nearest-rank percentile over an unsorted sample; 0.0 for an empty sample.
inline double Percentile(std::vector<double>* values, double fraction) {
    if (values->empty()) return 0.0;
    std::sort(values->begin(), values->end());
    const double rank = std::ceil(std::clamp(fraction, 0.0, 1.0) * values->size());
    const size_t index = rank > 0 ? static_cast<size_t>(rank) - 1 : 0;
    return (*values)[index];
}

// Appends one row to a CSV file, writing the header when the file is new.
inline void AppendCsvRow(const std::string& path, const std::string& header,
                         const std::string& row) {
    if (path.empty()) return;
    std::error_code ec;
    const bool fresh = !std::filesystem::exists(path, ec);
    std::ofstream out(path, std::ios::app);
    if (!out) return;
    if (fresh) out << header << '\n';
    out << row << '\n';
}

}  // namespace bench
