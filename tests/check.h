#pragma once

// Test assertions must not depend on NDEBUG: a Release build defines it, which
// turns assert() into a no-op and makes the suite pass without checking
// anything. CHECK throws instead, and the test runner reports the expression,
// file and line for the failing case.

#include <stdexcept>
#include <string>

namespace testing {

class CheckFailure : public std::runtime_error {
public:
    explicit CheckFailure(const std::string& message)
        : std::runtime_error(message) {}
};

}  // namespace testing

#define CHECK(condition)                                                \
    do {                                                                \
        if (!(condition)) {                                             \
            throw ::testing::CheckFailure(                              \
                std::string("CHECK failed: ") + #condition + " at " +   \
                __FILE__ + ":" + std::to_string(__LINE__));             \
        }                                                               \
    } while (false)
