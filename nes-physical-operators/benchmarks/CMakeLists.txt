# Standalone build for the slice-store micro-benchmark. NOT wired into the
# parent project's CMake on purpose - configure this directory directly:
#
#   cmake -S nes-physical-operators/benchmarks -B build/slice-bench \
#         -DCMAKE_BUILD_TYPE=Release
#   cmake --build build/slice-bench -j
#   ./build/slice-bench/slice-store-benchmark

cmake_minimum_required(VERSION 3.20)
project(slice-store-benchmark CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# Pull a release build of Google Benchmark via FetchContent. The Debian/Ubuntu
# package ships a CMAKE_BUILD_TYPE=None build with NDEBUG undefined, which
# trips libbenchmark's "***WARNING*** Library was built as DEBUG" diagnostic
# and leaves assertions live in the timing loop. Building from source here
# guarantees an honest release configuration.
include(FetchContent)
set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "" FORCE)
set(BENCHMARK_INSTALL_DOCS OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
    googlebenchmark
    GIT_REPOSITORY https://github.com/google/benchmark.git
    GIT_TAG v1.9.1
    GIT_SHALLOW TRUE
)
FetchContent_MakeAvailable(googlebenchmark)

find_package(Threads REQUIRED)

add_executable(slice-store-benchmark SliceStoreBenchmark.cpp)
target_link_libraries(slice-store-benchmark PRIVATE benchmark::benchmark Threads::Threads)
target_compile_options(slice-store-benchmark PRIVATE -Wall -Wextra -Wpedantic)
