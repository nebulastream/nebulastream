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

add_nes_test(buffer-test BufferTests.cpp)
target_link_libraries(buffer-test nes-memory nes-memory-test-utils)

add_nes_test(unpooled-buffer-test UnpooledBufferTests.cpp)
target_link_libraries(unpooled-buffer-test nes-memory nes-memory-test-utils)
