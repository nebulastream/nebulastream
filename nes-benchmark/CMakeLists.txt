# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

message(STATUS "Building benchmarks...")

function(add_nes_benchmarks TARGET_NAME FILE_PATH)
    add_executable(${TARGET_NAME} ${FILE_PATH})
    target_link_libraries(${TARGET_NAME} nes nes-benchmark ${GBENCHMARK_LIBRARIES} nes-tests)
    message(STATUS "Added benchmark ${TARGET_NAME}")
endfunction()


# Include dirs
include_directories("include/")

# Add src
add_subdirectory(src)

# Get all source files
get_source(nes-benchmark NES_BENCHMARK_SOURCE_FILES)

# Add nes-benchmark library
add_library(nes-benchmark SHARED ${NES_BENCHMARK_SOURCE_FILES})
target_link_libraries(nes-benchmark PUBLIC nes nes-common ${LIBRARIES} cpr::cpr ${CURL_LIBRARIES})

# Add E2E benchmark runner with custom e2e test config
add_nes_benchmarks(e2e-benchmark-runner "E2ERunner.cpp")
add_test(NAME e2e-benchmark-runner COMMAND e2e-benchmark-runner --configPath=../../nes-benchmark/config-examples/E2EConfigs/e2e_test_config.yaml)
message(STATUS "Added test e2e-benchmark-runner")

# Add E2E benchmark runner with custom e2e test config
#add_nes_benchmarks(ysb-kafka-data-generator "YSBKafkaDataGenerator.cpp")
#message(STATUS "Added test ysb-kafka-data-generator")

#-------------------------------##################------------------------------
#-------------------------------#Micro Benchmarks#------------------------------
include(ExternalProject)
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/include/")

### Query Merger Benchmark ###
add_nes_benchmarks(query-merger-benchmark "src/MicroBenchmarks/QueryMerger/BenchmarkQueryMerger.cpp")

### Incremental Placement Benchmark ###
add_nes_benchmarks(incremental-placement-benchmark "src/MicroBenchmarks/IncrementalPlacement/BenchmarkIncrementalPlacement.cpp")

### Output Channel Benchmark ###
add_nes_benchmarks(output-channel-benchmark "src/MicroBenchmarks/BenchmarkOutputChannel.cpp")


message(STATUS "Done building benchmarks")