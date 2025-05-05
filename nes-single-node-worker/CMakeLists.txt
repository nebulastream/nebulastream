# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Add Source Code
add_subdirectory(src)
include(GNUInstallDirs)

# Add nes-worker library
get_source(nes-single-node-worker NES_WORKER_SOURCE_FILES)
add_library(nes-single-node-worker-lib ${NES_WORKER_SOURCE_FILES})

target_include_directories(nes-single-node-worker-lib PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

target_link_libraries(nes-single-node-worker-lib
        PUBLIC
        nes-grpc
        nes-query-compiler
        nes-query-optimizer
)

add_executable(nes-single-node-worker src/SingleNodeWorkerStarter.cpp)
target_link_libraries(nes-single-node-worker PRIVATE nes-single-node-worker-lib)

add_tests_if_enabled(tests)
