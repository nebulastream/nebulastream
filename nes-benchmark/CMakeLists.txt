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
    target_link_libraries(${TARGET_NAME} nes-coordinator  nes-benchmark ${GBENCHMARK_LIBRARIES} nes-coordinator-test-util)
    message(STATUS "Added benchmark ${TARGET_NAME}")
endfunction()


add_compile_definitions(TEST_DATA_DIRECTORY="${CMAKE_CURRENT_BINARY_DIR}/testData/")
message(STATUS "Test Data Directory: ${TEST_DATA_DIRECTORY}/testData")

# Copy Benchmark Data --------------------------------------------------------------
file(COPY configs/enginePaper/benchmark-data DESTINATION .)
add_compile_definitions(BENCHMARK_DATA_DIRECTORY="${CMAKE_CURRENT_BINARY_DIR}/benchmark-data/")
message(STATUS "Benchmark Data Directory: ${BENCHMARK_DATA_DIRECTORY}/benchmark-data")

# Include dirs
include_directories("include/")

# Add src
add_subdirectory(src)

# Get all source files
get_source(nes-benchmark NES_BENCHMARK_SOURCE_FILES)

# Add nes-benchmark library
add_library(nes-benchmark ${NES_BENCHMARK_SOURCE_FILES})
target_link_libraries(nes-benchmark PUBLIC ${LIBRARIES} cpr::cpr ${CURL_LIBRARIES})
target_link_libraries(nes-benchmark  PUBLIC nes-test-util nes-coordinator)
add_nes_benchmarks(e2e-benchmark-main "src/E2E/E2EBenchmarkMain.cpp")


#-------------------------------##################------------------------------
#-------------------------------#Micro Benchmarks#------------------------------
include(ExternalProject)
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/include/")

### Incremental Stream Query Merger Benchmark (ISQM) ###
add_nes_benchmarks(isqm-benchmark "src/MicroBenchmarks/ISQMBenchmark/ISQMBenchmark.cpp")

### Incremental Stream Query Placement Benchmark (ISQP) ###
add_nes_benchmarks(isqp-benchmark "src/MicroBenchmarks/ISQPBenchmark/ISQPBenchmark.cpp")

### Incremental Stream Query Placement Benchmark (ISQP) with topology changes ###
add_nes_benchmarks(isqp-benchmark-topology-change "src/MicroBenchmarks/ISQPBenchmark/ISQPBenchmarkTopologyChanges.cpp")

### Output Channel Benchmark ###
add_nes_benchmarks(output-channel-benchmark "src/MicroBenchmarks/BenchmarkOutputChannel.cpp")

#YSB Kafka data generator
add_nes_benchmarks(ysb-kafka-data-generator "src/DataGeneration/YSBKafkaDataGenerator.cpp")

add_nes_benchmarks(state-recreation-benchmark "src/MicroBenchmarks//Reconfiguration/StateRecreationBenchmark.cpp")

# Adding benchmark tests
add_subdirectory(test)