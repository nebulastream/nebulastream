# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_library(nes-memory-test-utils TestUtils/MemoryTestUtils.cpp)
target_include_directories(nes-memory-test-utils PUBLIC TestUtils)
target_link_libraries(nes-memory-test-utils PUBLIC nes-memory)


function(add_nes_memory_test)
    add_nes_test(${ARGN})
    target_link_libraries(${ARGV0} nes-memory)
    target_include_directories(${ARGV0} PRIVATE ..)
endfunction()

add_nes_memory_test(buffer_manager_test "BufferManagerTest.cpp")
add_nes_memory_test(buffer_test "BufferTest.cpp")