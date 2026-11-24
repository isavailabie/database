cmake_minimum_required(VERSION 3.16)
project(tsdb LANGUAGES CXX)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Sanitizer build for the test suite:
#   cmake -S . -B build-asan -DTSDB_SANITIZE=ON
#   ctest --test-dir build-asan --output-on-failure
# AddressSanitizer catches the dangling iterators a missing copy-on-write would
# leave behind, UBSan catches the timestamp arithmetic.
option(TSDB_SANITIZE "Build with AddressSanitizer and UBSan" OFF)
if(TSDB_SANITIZE)
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer -g)
  add_link_options(-fsanitize=address,undefined)
endif()

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

add_executable(tsdb_design_bench benchmarks/design.cpp)
target_link_libraries(tsdb_design_bench PRIVATE tsdb)

add_executable(tsdb_fault_test tests/fault_injection.cpp)
target_link_libraries(tsdb_fault_test PRIVATE tsdb)

enable_testing()
add_test(NAME tsdb_tests COMMAND tsdb_tests)
add_test(NAME tsdb_fault_test COMMAND tsdb_fault_test)
set_tests_properties(tsdb_tests tsdb_fault_test PROPERTIES TIMEOUT 180)
find_package(Python3 COMPONENTS Interpreter QUIET)
if(Python3_Interpreter_FOUND)
  add_test(NAME benchmark_runner_tests
    COMMAND ${Python3_EXECUTABLE} -m unittest discover
      -s ${CMAKE_CURRENT_SOURCE_DIR}/tests -p test_benchmark_runner.py -v)
  add_test(NAME benchmark_compare_tests
    COMMAND ${Python3_EXECUTABLE} -m unittest discover
      -s ${CMAKE_CURRENT_SOURCE_DIR}/tests -p test_benchmark_compare.py -v)
  add_test(NAME benchmark_design_smoke
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_benchmark_compare.py
      --design-binary $<TARGET_FILE:tsdb_design_bench>)
  set_tests_properties(benchmark_compare_tests PROPERTIES TIMEOUT 30)
  set_tests_properties(benchmark_design_smoke PROPERTIES TIMEOUT 60 RUN_SERIAL TRUE)
endif()
