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
include(GoogleTest)

function(add_nes_execution_test)
    add_nes_test(${ARGN})
    set(TARGET_NAME ${ARGV0})
    target_link_libraries(${TARGET_NAME} nes-execution nes-common nes-data-types nes-operators nes-grpc nes-runtime nes-test-util nes-configurations nes-nautilus nes-window-types)
endfunction()

add_subdirectory(UnitTests)

add_nes_execution_test(micro-benchmarks-tests "MicroBenchmarks.cpp")
