# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

function(add_nes_physical_operator_test)
    add_nes_test(${ARGN})
    set(TARGET_NAME ${ARGV0})
    target_link_libraries(${TARGET_NAME} nes-data-types nes-physical-operators nes-memory-test-utils nes-test-util)
endfunction()

add_nes_physical_operator_test(EmitPhysicalOperatorTest EmitPhysicalOperatorTest.cpp)
add_nes_physical_operator_test(SliceAssignerTest SliceAssignerTest.cpp)

add_executable(slice-micro-benchmark microbenchmark/SliceMicroBenchmark.cpp)
target_link_libraries(slice-micro-benchmark nes-data-types nes-physical-operators nes-memory-test-utils nes-test-util)