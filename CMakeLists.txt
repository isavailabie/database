cmake_minimum_required(VERSION 3.16)
project(tsdb LANGUAGES CXX)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(tsdb src/tsdb.cpp)
target_include_directories(tsdb PUBLIC include)
target_compile_options(tsdb PRIVATE -Wall -Wextra -Wpedantic)

add_executable(tsdb_tests tests/tsdb_tests.cpp)
target_link_libraries(tsdb_tests PRIVATE tsdb)

add_executable(tsdb_bench benchmarks/ingest.cpp)
target_link_libraries(tsdb_bench PRIVATE tsdb)

add_executable(tsdb_mixed_bench benchmarks/mixed.cpp)
target_link_libraries(tsdb_mixed_bench PRIVATE tsdb)

add_executable(tsdb_recover benchmarks/recover.cpp)
target_link_libraries(tsdb_recover PRIVATE tsdb)

add_executable(tsdb_fault_test tests/fault_injection.cpp)
target_link_libraries(tsdb_fault_test PRIVATE tsdb)

enable_testing()
add_test(NAME tsdb_tests COMMAND tsdb_tests)
add_test(NAME tsdb_fault_test COMMAND tsdb_fault_test)
