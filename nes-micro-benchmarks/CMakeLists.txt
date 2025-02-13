# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#set(CMAKE_VERBOSE_MAKEFILE on)

include_directories(tests/include)

# Set Sources and Headers
add_subdirectory(src)
get_source(nes-micro-benchmarks NES_MICRO_BENCHMARKS)

#
find_package(benchmark REQUIRED)

# Add Library
add_library(nes-micro-benchmarks ${NES_MICRO_BENCHMARKS})
target_link_libraries(nes-micro-benchmarks PUBLIC nes-common nes-operators nes-data-types nes-memory nes-sources nes-query-engine nes-execution nes-nautilus benchmark::benchmark nes-execution-registry)

target_include_directories(nes-micro-benchmarks PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

# folly
include(CMakeFindDependencyMacro)
find_package(folly CONFIG REQUIRED)
target_link_libraries(nes-micro-benchmarks PRIVATE folly::folly)
target_include_directories(nes-micro-benchmarks PRIVATE ${FOLLY_INCLUDE_DIR})


if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-micro-benchmarks REUSE_FROM nes-common)
endif ()

target_compile_options(nes-micro-benchmarks PUBLIC "-Wno-bitwise-instead-of-logical")

# Add Private Dependencies
if (NES_ENABLES_TESTS AND NOT (CMAKE_HOST_SYSTEM_NAME STREQUAL "Darwin"))
    # this allows to mock read calls
    target_compile_definitions(nes-micro-benchmarks PRIVATE WRAP_READ_CALL)
    target_link_libraries(nes-micro-benchmarks PRIVATE  "-Wl,--wrap,read")
endif ()

add_subdirectory(benchmark-executables)
