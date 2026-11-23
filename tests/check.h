#pragma once

// Test assertions must not depend on NDEBUG: a Release build defines it, which
// turns assert() into a no-op and makes the suite pass without checking
// anything. CHECK throws instead, and the test runner reports the expression,
// file and line for the failing case.

#include <stdexcept>
#include <string>

// Deliberately not named `testing`: the engine exposes tsdb::testing for its own
// test hooks and `using namespace tsdb` in the suite would make the name
// ambiguous.
namespace test_support {

class CheckFailure : public std::runtime_error {
public:
    explicit CheckFailure(const std::string& message)
        : std::runtime_error(message) {}
};

}  // namespace test_support

#define CHECK(condition)                                                \
    do {                                                                \
        if (!(condition)) {                                             \
            throw ::test_support::CheckFailure(                         \
                std::string("CHECK failed: ") + #condition + " at " +   \
                __FILE__ + ":" + std::to_string(__LINE__));             \
        }                                                               \
    } while (false)
