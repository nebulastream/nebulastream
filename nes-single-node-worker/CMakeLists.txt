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
add_executable(single-node-worker ${NES_WORKER_SOURCE_FILES})

target_include_directories(single-node-worker PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

target_link_libraries(single-node-worker
        PRIVATE
        nes-grpc
        nes-runtime
        nes-execution
)

if (NES_ENABLES_TESTS)
    # Add tests with command
    add_subdirectory(tests)
    message(STATUS "Tests are enabled for single-node-worker")
else ()
    message(STATUS "Tests are disabled for single-node-worker")
endif ()

#Add the nes lib and headers to cpack
install(TARGETS single-node-worker
        EXPORT NebulaStreamTargets
        LIBRARY
        COMPONENT nesLibrary)
