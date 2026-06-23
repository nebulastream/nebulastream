# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# nes-bench-runner: a concurrent, profile-clean throughput harness built on the same embedded
# worker stack as systest. It links nes-systest-lib (defined in the sibling systest/ subdir, added
# before this one) to reuse the query binder, QuerySubmitter, and the embedded QueryManager.
find_package(argparse CONFIG REQUIRED)

add_executable(nes-bench-runner BenchRunner.cpp)
set_property(TARGET nes-bench-runner PROPERTY ENABLE_EXPORTS ON)
target_link_libraries(nes-bench-runner PUBLIC nes-systest-lib argparse::argparse nes-query-optimizer)
target_include_directories(nes-bench-runner PUBLIC $<INSTALL_INTERFACE:/include/nebulastream>)
