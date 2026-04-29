# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# nebucli is a Rust staticlib with a thin C++ main() wrapper.
# This lets CMake resolve the full C++ dependency graph (sql-planner,
# nes-single-node-worker-lib, etc.) that the coordinator bridge needs.

# Workaround: both nes_rust_bindings and coordinator_bridge are separate Rust staticlibs
# that each embed an identical copy of the cxx runtime. Allow the linker to deduplicate.
# TODO: merge into a single Rust staticlib to avoid this.
set(CXX_MULDEFS_FLAG "LINKER:-z,muldefs")

add_executable(nes-cli main.cpp)
target_link_libraries(nes-cli PRIVATE nebucli nes-coordinator-bridge)
target_link_options(nes-cli PRIVATE ${CXX_MULDEFS_FLAG})

# nebucli E2E tests driven by bats

if (ENABLE_BATS_TESTS)
    add_test(
            NAME offline-cli-test
            COMMAND
            env NES_CLI=$<TARGET_FILE:nes-cli>
            env NES_CLI_TESTDATA=${CMAKE_CURRENT_SOURCE_DIR}/tests
            env NES_TEST_TMP_DIR=${CMAKE_BINARY_DIR}/test-tmp
            env NEBULASTREAM=$<TARGET_FILE:nes-single-node-worker>
            ${BATS} --verbose-run --timing ${CMAKE_CURRENT_SOURCE_DIR}/tests/offline.bats
    )
endif ()

if (ENABLE_DOCKER_TESTS)
    if (ENABLE_BATS_TESTS)
        add_test(
                NAME distributed-cli-test
                COMMAND
                env NES_CLI=$<TARGET_FILE:nes-cli>
                env NES_CLI_TESTDATA=${CMAKE_CURRENT_SOURCE_DIR}/tests
                env NEBULASTREAM=$<TARGET_FILE:nes-single-node-worker>
                env NES_TEST_TMP_DIR=${CMAKE_BINARY_DIR}/test-tmp
                env NES_RUNTIME_BASE_IMAGE=${NES_RUNTIME_BASE_IMAGE}
                ${BATS} --jobs 19 --verbose-run --timing ${CMAKE_CURRENT_SOURCE_DIR}/tests/distributed.bats
        )
        set_tests_properties(distributed-cli-test PROPERTIES LABELS "DockerCompose" FIXTURES_REQUIRED RuntimeBaseImage)
    endif ()
endif ()
