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
        nes-runtime
        nes-execution
)

add_executable(single-node src/SingleNodeWorkerStarter.cpp)
target_link_libraries(single-node PRIVATE nes-single-node-worker-lib)

if (NES_ENABLES_TESTS)
    # Add tests with command
    add_subdirectory(tests)
    message(STATUS "Tests are enabled for single-node-worker")

    # TODO 294-P
    # This test, in combination with tests/MoldStripBug.cpp,
    # documents that the single-node worker (and the smaller MoldStripBug.cpp)
    # break when linking with mold and then stripping with llvm-strip-18
    if (MOLD_EXECUTABLE AND NES_USE_MOLD_IF_AVAILABLE)
        add_custom_command(
                OUTPUT mold-strip-bug
                COMMAND llvm-strip-18 -o mold-strip-bug $<TARGET_FILE:single-node>
                DEPENDS single-node
                COMMENT "Breaking single-node by stripping with llvm-strip-18"
        )

        add_custom_target(mold-strip-bug-single-node_broken ALL DEPENDS mold-strip-bug)

        # Expects the program to fail by failing if the program exits correctly
        add_test(NAME mold-strip-bug-single-node_broken COMMAND sh -c "./mold-strip-bug && exit 1 || exit 0")
    endif ()
else ()
    message(STATUS "Tests are disabled for single-node-worker")
endif ()

#Add the nes lib and headers to cpack
install(TARGETS nes-single-node-worker-lib
        EXPORT NebulaStreamTargets
        LIBRARY
        COMPONENT nesLibrary)
