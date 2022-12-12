
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(ExternalProject)

add_library(nes-runtime-benchmark INTERFACE)
target_include_directories(nes-runtime-benchmark INTERFACE "../tests/include/")
target_link_libraries(nes-runtime-benchmark INTERFACE nes-runtime)
target_link_libraries(nes-runtime-benchmark INTERFACE benchmark::benchmark)


set(BENCHMARK_ENABLE_GTEST_TESTS OFF)
set(BENCHMARK_ENABLE_TESTING OFF)
FetchContent_Declare(googlebenchmark
        GIT_REPOSITORY https://github.com/google/benchmark.git
        GIT_TAG main) # need master for benchmark::benchmark

FetchContent_MakeAvailable(
        googlebenchmark)

function(add_nes_benchmarks TARGET_NAME FILE_PATH)
    add_executable(${TARGET_NAME} ${FILE_PATH})
    target_link_libraries(${TARGET_NAME} nes-runtime-benchmark)
    message(STATUS "Added benchmark ${TARGET_NAME}")
endfunction()

add_nes_benchmarks(nautilus-tracing-benchmark "Nautilus/BenchmarkTracing.cpp")


