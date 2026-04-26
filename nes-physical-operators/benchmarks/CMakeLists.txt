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

find_package(benchmark REQUIRED)
find_package(Threads REQUIRED)

add_executable(slice-store-benchmark SliceStoreBenchmark.cpp)
target_link_libraries(slice-store-benchmark PRIVATE benchmark::benchmark Threads::Threads)
target_compile_options(slice-store-benchmark PRIVATE -Wall -Wextra -Wpedantic)
