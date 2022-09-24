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

find_package(OpenMP)
if (OPENMP_FOUND)
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OpenMP_EXE_LINKER_FLAGS}")
endif()

# include dirs
include_directories("include/")


# add src
add_subdirectory(src)

# Get all source files
get_source(nes-benchmark NES_BENCHMARK_SOURCE_FILES)

# Add nes-benchmark library
add_library(nes-benchmark SHARED ${NES_BENCHMARK_SOURCE_FILES})
target_link_libraries(nes-benchmark PUBLIC nes nes-common ${LIBRARIES})

# add E2E benchmark runner
add_executable(e2e-benchmark-runner "E2ERunner.cpp")
target_link_libraries(e2e-benchmark-runner PUBLIC nes-benchmark)


message("---Done building benchmarks")