
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# Google Benchmark Framework ----------------------------------------------------

if (NES_BUILD_BENCHMARKS)
    message("---Building benchmarks...")
    include(ExternalProject)

    include_directories("${CMAKE_CURRENT_SOURCE_DIR}/include/")

    #-------------------------------##################------------------------------
    #-------------------------------# Micro Benchmarks#-----------------------------

    ### Query Merger Benchmark ###
    add_executable(query-merger-benchmark "src/MicroBenchmarks/QueryMerger/BenchmarkQueryMerger.cpp")
    target_link_libraries(query-merger-benchmark nes ${GBENCHMARK_LIBRARIES})
    add_test(NAME query-merger-benchmark COMMAND query-merger-benchmark --benchmark_out=BenchmarkQueryMerger.log --benchmark_out_format=json)

    ### Incremental Placement Benchmark ###
    add_executable(incremental-placement-benchmark "src/MicroBenchmarks/IncrementalPlacement/BenchmarkIncrementalPlacement.cpp")
    target_link_libraries(incremental-placement-benchmark nes ${GBENCHMARK_LIBRARIES})
    add_test(NAME incremental-placement-benchmark COMMAND incremental-placement-benchmark --benchmark_out=BenchmarkIncrementalPlacement.log --benchmark_out_format=json)

    add_executable(output-channel-benchmark "src/MicroBenchmarks/BenchmarkOutputChannel.cpp")
    target_link_libraries(output-channel-benchmark nes ${GBENCHMARK_LIBRARIES})
    add_test(NAME output-channel-benchmark COMMAND output-channel-benchmark --benchmark_out=BenchmarkOutputChannel.log --benchmark_out_format=json)

    message("---Done building benchmarks")
else (NES_BUILD_BENCHMARKS)
    message("---Not building benchmarks!")
endif ()