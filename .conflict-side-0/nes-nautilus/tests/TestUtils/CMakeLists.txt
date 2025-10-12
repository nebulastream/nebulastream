# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_library(nes-nautilus-test-util STATIC
        src/NautilusTestUtils.cpp
        src/PagedVectorTestUtils.cpp
        src/ChainedHashMapTestUtils.cpp
        src/ChainedHashMapCustomValueTestUtils.cpp
)

target_include_directories(nes-nautilus-test-util PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(nes-nautilus-test-util PUBLIC
        nes-memory
        nes-grpc
        nes-nautilus
        nes-test-util
        ${GTEST_LIBRARIES}
)
