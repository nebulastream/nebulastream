# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

find_package(benchmark QUIET)
if (benchmark_FOUND)
    add_executable(hash-function-benchmark HashFunctionBenchmark.cpp)
    target_link_libraries(hash-function-benchmark PRIVATE nes-nautilus benchmark::benchmark)
    message(STATUS "Added hash-function-benchmark (Google Benchmark)")
else()
    add_executable(hash-function-benchmark HashFunctionBenchmarkStandalone.cpp)
    target_link_libraries(hash-function-benchmark PRIVATE nes-nautilus)
    message(STATUS "Added hash-function-benchmark (standalone, Google Benchmark not found)")
endif()
