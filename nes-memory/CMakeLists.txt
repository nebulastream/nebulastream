# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_subdirectory(MemoryLayout)

add_library(nes-memory
        BufferManager.cpp
        TupleBufferImpl.cpp
        TupleBuffer.cpp
        NesDefaultMemoryAllocator.cpp
        FixedSizeBufferPool.cpp
        TaggedPointer.cpp
        TestTupleBuffer.cpp
        ${MEMORY_LAYOUT_SOURCES}
)

target_include_directories(nes-memory
        PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>
        PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)
find_package(folly REQUIRED)
target_link_libraries(nes-memory PUBLIC nes-common nes-data-types PRIVATE folly::folly)
add_tests_if_enabled(tests)
