# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Self-contained std::chrono micro-benchmark (no external benchmark library). It compiles the SIMD
# kernel directly and links only nes-common (for ErrorHandling), so it does NOT depend on the full
# nes-input-formatters library / plugin registry — keeping it buildable and fast to link.
# EXCLUDE_FROM_ALL: built on demand via
#   cmake --build {build} --target csv-indexer-benchmark
add_executable(csv-indexer-benchmark EXCLUDE_FROM_ALL
        CSVIndexerBenchmark.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../src/SIMDCSVKernel.cpp)
target_link_libraries(csv-indexer-benchmark PRIVATE nes-common)
target_include_directories(csv-indexer-benchmark PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../private)

# Reference: vendored simdjson full-parse throughput, measured the same way (no NES deps).
find_package(simdjson CONFIG REQUIRED)
add_executable(simdjson-indexing-benchmark EXCLUDE_FROM_ALL SimdjsonIndexingBenchmark.cpp)
target_link_libraries(simdjson-indexing-benchmark PRIVATE simdjson::simdjson)
