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

add_library(nes-single-node-worker-interface interface/WorkerStatus.cpp interface/SingleNodeWorkerConfiguration.cpp)
target_include_directories(nes-single-node-worker-interface PUBLIC interface)
target_link_libraries(nes-single-node-worker-interface PUBLIC nes-common nes-grpc nes-configurations nes-runtime-interface nes-logical-operators)

# Add nes-worker library
get_source(nes-single-node-worker NES_WORKER_SOURCE_FILES)
add_library(nes-single-node-worker-lib ${NES_WORKER_SOURCE_FILES})

target_include_directories(nes-single-node-worker-lib PUBLIC include)

target_link_libraries(nes-single-node-worker-lib
        PUBLIC
        nes-grpc
        nes-query-compiler
        nes-query-optimizer
        nes-single-node-worker-interface
)

add_executable(nes-single-node-worker src/SingleNodeWorkerStarter.cpp)
find_package(argparse CONFIG REQUIRED)
target_link_libraries(nes-single-node-worker PRIVATE nes-single-node-worker-lib argparse::argparse)

# UdfBridgeRegistry resolves BRIDGE clauses relative to the worker's executable dir, so stage the built
# bridge there via symlink. nes-udf/bridge-cpython is configured after this dir, so re-check for
# Python's embed dev files instead of `if (TARGET nes-cpython-udf-bridge)`.
find_package(Python3 COMPONENTS Development.Embed QUIET)
if (Python3_FOUND)
    add_custom_command(TARGET nes-single-node-worker POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E make_directory "$<TARGET_FILE_DIR:nes-single-node-worker>/nes-udf-bridges"
            COMMAND ${CMAKE_COMMAND} -E create_symlink "$<TARGET_FILE:nes-cpython-udf-bridge>" "$<TARGET_FILE_DIR:nes-single-node-worker>/nes-udf-bridges/$<TARGET_FILE_NAME:nes-cpython-udf-bridge>")
endif ()

# Same staging, for the PyPy bridge (see nes-udf/bridge-pypy) -- independently re-detected here for
# the same reason as the Python3 check above.
include(${PROJECT_SOURCE_DIR}/cmake/FindNesPyPy.cmake)
nes_find_pypy()
if (NES_PYPY_FOUND)
    add_custom_command(TARGET nes-single-node-worker POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E make_directory "$<TARGET_FILE_DIR:nes-single-node-worker>/nes-udf-bridges"
            COMMAND ${CMAKE_COMMAND} -E create_symlink "$<TARGET_FILE:nes-pypy-udf-bridge>" "$<TARGET_FILE_DIR:nes-single-node-worker>/nes-udf-bridges/$<TARGET_FILE_NAME:nes-pypy-udf-bridge>")
endif ()

if (ENABLE_BATS_TESTS)
    add_test(
            NAME worker-offline-test
            COMMAND
            env NES_WORKER=$<TARGET_FILE:nes-single-node-worker>
            env NES_WORKER_TESTDATA=${CMAKE_CURRENT_SOURCE_DIR}/tests
            ${BATS} -x --verbose-run ${CMAKE_CURRENT_SOURCE_DIR}/tests/offline.bats
    )
endif ()
