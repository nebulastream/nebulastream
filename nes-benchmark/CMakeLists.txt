# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

message("---Building benchmarks...")


function(add_nes_benchmarks TARGET_NAME FILE_PATH COMMAND_LINE_ARGS)
    add_executable(${TARGET_NAME} ${FILE_PATH})
    target_link_libraries(${TARGET_NAME} nes nes-benchmark ${GBENCHMARK_LIBRARIES})
    add_test(NAME ${TARGET_NAME} COMMAND ${TARGET_NAME} ${COMMAND_LINE_ARGS})
    message(STATUS "Added benchmark ${TARGET_NAME}")
endfunction()


# include dirs
include_directories("include/")

# add src
add_subdirectory(src)

# Get all source files
get_source(nes-benchmark NES_BENCHMARK_SOURCE_FILES)

# Add nes-benchmark library
add_library(nes-benchmark SHARED ${NES_BENCHMARK_SOURCE_FILES})
target_link_libraries(nes-benchmark PUBLIC nes nes-common ${LIBRARIES} cpr::cpr ${CURL_LIBRARIES})

# add E2E benchmark runner
add_executable(e2e-benchmark-runner "E2ERunner.cpp")
target_link_libraries(e2e-benchmark-runner PUBLIC nes-benchmark)



#-------------------------------##################------------------------------
#-------------------------------#Micro Benchmarks#-----------------------------
include(ExternalProject)
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/include/")

### Query Merger Benchmark ###
add_nes_benchmarks(query-merger-benchmark "src/MicroBenchmarks/QueryMerger/BenchmarkQueryMerger.cpp" "--benchmark_out=BenchmarkQueryMerger.log --benchmark_out_format=json")

### Incremental Placement Benchmark ###
add_nes_benchmarks(incremental-placement-benchmark "src/MicroBenchmarks/IncrementalPlacement/BenchmarkIncrementalPlacement.cpp" "--benchmark_out=BenchmarkIncrementalPlacement.log --benchmark_out_format=json")

#add_executable(output-channel-benchmark "src/MicroBenchmarks/BenchmarkOutputChannel.cpp")
add_nes_benchmarks(output-channel-benchmark "src/MicroBenchmarks/BenchmarkOutputChannel.cpp" "--benchmark_out=BenchmarkOutputChannel.log --benchmark_out_format=json")


message("---Done building benchmarks")