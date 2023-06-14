
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
target_link_libraries(nes-runtime-benchmark INTERFACE nes-runtime nes-runtime-test-util)

find_package(benchmark CONFIG REQUIRED)
if (NOT ${benchmark_FOUND})
    message(FATAL_ERROR "Unable to find google benchmark")
endif ()

target_link_libraries(nes-runtime-benchmark INTERFACE benchmark::benchmark)

find_package(benchmark)

function(add_nes_benchmarks TARGET_NAME FILE_PATH)
    add_executable(${TARGET_NAME} ${FILE_PATH})
    target_link_libraries(${TARGET_NAME} nes-runtime-benchmark benchmark::benchmark)
    message(STATUS "Added benchmark ${TARGET_NAME}")
endfunction()

add_nes_benchmarks(nautilus-backend-benchmark "Nautilus/BackendCompilationBenchmark.cpp")
target_include_directories(nautilus-backend-benchmark INTERFACE ${CMAKE_SOURCE_DIR}/Nautilus)
add_nes_benchmarks(nautilus-tracing-benchmark "Nautilus/BenchmarkTracing.cpp")
add_executable(tpch-benchmark "TPCH/TPCHBenchmark.cpp")
target_link_libraries(tpch-benchmark PUBLIC tpch-dbgen nes-runtime-benchmark)

add_executable(stream-benchmark "Stream/StreamBenchmark.cpp")
target_link_libraries(stream-benchmark PUBLIC tpch-dbgen nes-runtime-benchmark)

add_executable(udf-benchmark "UDFs/UDFBenchmark.cpp")
target_link_libraries(udf-benchmark PUBLIC nes-runtime-benchmark)
add_compile_definitions(TEST_DATA_DIRECTORY="${CMAKE_CURRENT_BINARY_DIR}/../tests/testData/")

set(SHELL_SCRIPT_SOURCE_DIR "${CMAKE_SOURCE_DIR}/path/to/shell/file")

# Set the destination directory where you want to copy the shell file
set(SHELL_SCRIPT_DEST_DIR "${CMAKE_BINARY_DIR}/path/to/build/directory")

# Use configure_file to copy the shell file
file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/scripts" DESTINATION "${CMAKE_CURRENT_BINARY_DIR}")

