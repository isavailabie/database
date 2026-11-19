#pragma once

// Small helpers shared by the benchmark drivers. Everything here runs in the
// driver process, never inside the engine, so it does not count against the
// engine's RSS budget.

#include <algorithm>
#include <chrono>
#include <cstdint>
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

// Nearest-rank percentile over an unsorted sample; 0.0 for an empty sample.
inline double Percentile(std::vector<double>* values, double fraction) {
    if (values->empty()) return 0.0;
    std::sort(values->begin(), values->end());
    const double scaled = fraction * static_cast<double>(values->size() - 1);
    size_t index = static_cast<size_t>(scaled);
    if (index >= values->size()) index = values->size() - 1;
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
