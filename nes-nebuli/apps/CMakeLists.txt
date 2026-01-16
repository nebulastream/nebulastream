# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# nes-repl is our REPL based frontend it comes in two flavours. The embedded version contains the single-node-worker and
# can thus execute queries locally. The non-embedded version always requires the '-s' flag to deploy remote queries.

# nes-repl-embedded
add_executable(nes-repl-embedded repl/ReplStarter.cpp repl/Repl.cpp)
target_link_libraries(nes-repl-embedded PUBLIC nes-nebuli-embedded-lib argparse::argparse replxx::replxx)
target_include_directories(nes-repl-embedded
        PUBLIC repl
)

# nes-repl
add_executable(nes-repl repl/ReplStarter.cpp repl/Repl.cpp)
target_link_libraries(nes-repl PUBLIC nes-nebuli-lib argparse::argparse replxx::replxx)

target_include_directories(nes-repl
        PUBLIC repl
)

# nes-cli is the one-shot controller for the nes-single-node-worker, it is completely stateless and intended to deploy
# and control queries based on a topology specification which contains all state required to perform placement and
# optimizations.

add_executable(nes-cli cli/CLIStarter.cpp cli/QueryStateBackend.cpp)
target_link_libraries(nes-cli PUBLIC nes-nebuli-lib argparse::argparse yaml-cpp::yaml-cpp)

target_include_directories(nes-cli
        PRIVATE cli
)

# These are E2E Tests for both the CLI tool and the repl.
# Because the REPL has both a interactive and non-interactive mode, the REPL is tested in both ways.
# We use bats to drive the non-interactive tests for the cli and repl and expect to drive the interactive test for the repl

# Tests are gated behind the ENABLE_DOCKER_TESTS, which is disabled by default to prevent accidental Docker-in-Docker Tests.
# Important: If you enable the flag make sure you invoke the ctest from outside the development container!!
# This also tests the embedded version of the repl, which does not require a docker compose setup.

find_program(BATS bats)
set(ENABLE_BATS_TESTS ON)
if (${BATS} STREQUAL BATS-NOTFOUND)
    set(ENABLE_BATS_TESTS OFF)
    message(WARNING "Bats not found. Disabling Bats based e2e tests. You can install Bats via apt install bats")
endif ()

find_program(EXPECT expect)
set(ENABLE_EXPECT_TESTS ON)
if (${EXPECT} STREQUAL EXPECT-NOTFOUND)
    set(ENABLE_EXPECT_TESTS OFF)
    message(WARNING "Expect not found. Disabling Expect based e2e tests. You can install Expect via apt install expect")
endif ()

if (ENABLE_BATS_TESTS)

    add_test(
            NAME embedded-repl-test
            COMMAND
            env NES_REPL=$<TARGET_FILE:nes-repl-embedded>
            env NES_REPL_TESTDATA=${CMAKE_CURRENT_SOURCE_DIR}/repl/tests
            env SYSTEST_TESTDATA=${CMAKE_CURRENT_BINARY_DIR}/../../nes-systests/testdata
            ${BATS} -x --verbose-run ${CMAKE_CURRENT_SOURCE_DIR}/repl/tests/sql-file-tests/embedded.bats
    )

    add_test(
            NAME offline-cli-test
            COMMAND
            env NES_CLI=$<TARGET_FILE:nes-cli>
            env NES_CLI_TESTDATA=${CMAKE_CURRENT_SOURCE_DIR}/cli/tests
            env NEBULASTREAM=$<TARGET_FILE:nes-single-node-worker>
            ${BATS} -x --verbose-run ${CMAKE_CURRENT_SOURCE_DIR}/cli/tests/offline.bats
    )
endif ()

if (ENABLE_EXPECT_TESTS)
    # Automatically discover and add all embedded expect tests
    file(GLOB EMBEDDED_REPL_TESTS
            "${CMAKE_CURRENT_SOURCE_DIR}/repl/tests/expect-tests/embedded/*.exp"
    )
    foreach (TEST_FILE ${EMBEDDED_REPL_TESTS})
        get_filename_component(TEST_NAME ${TEST_FILE} NAME_WE)
        add_test(
                NAME "embedded-repl-test-${TEST_NAME}"
                COMMAND
                ${TEST_FILE} $<TARGET_FILE:nes-repl-embedded>
        )
    endforeach ()
endif ()

if (ENABLE_DOCKER_TESTS)
    if (ENABLE_BATS_TESTS)
        add_test(
                NAME distributed-cli-test
                COMMAND
                env NES_CLI=$<TARGET_FILE:nes-cli>
                env NES_CLI_TESTDATA=${CMAKE_CURRENT_SOURCE_DIR}/cli/tests
                env NEBULASTREAM=$<TARGET_FILE:nes-single-node-worker>
                ${BATS} -x --verbose-run ${CMAKE_CURRENT_SOURCE_DIR}/cli/tests/distributed.bats
        )
        set_tests_properties(distributed-cli-test PROPERTIES LABELS "DockerCompose")

        add_test(
                NAME distributed-repl-tests
                COMMAND
                env NES_REPL=$<TARGET_FILE:nes-repl>
                env NES_REPL_TESTDATA=${CMAKE_CURRENT_SOURCE_DIR}/repl/tests
                env NEBULASTREAM=$<TARGET_FILE:nes-single-node-worker>
                ${BATS} -x --verbose-run ${CMAKE_CURRENT_SOURCE_DIR}/repl/tests/sql-file-tests/distributed.bats
        )
        set_tests_properties(distributed-repl-tests PROPERTIES LABELS "DockerCompose")
    endif ()

    if (ENABLE_EXPECT_TESTS)
        # Automatically discover and add all distributed expect tests
        file(GLOB DISTRIBUTED_REPL_TESTS
                "${CMAKE_CURRENT_SOURCE_DIR}/repl/tests/expect-tests/distributed/*.exp"
        )
        foreach (TEST_FILE ${DISTRIBUTED_REPL_TESTS})
            get_filename_component(TEST_NAME ${TEST_FILE} NAME_WE)
            add_test(
                    NAME "distributed-repl-test-${TEST_NAME}"
                    COMMAND
                    env NES_REPL=$<TARGET_FILE:nes-repl>
                    env NES_REPL_TESTDATA=${CMAKE_CURRENT_SOURCE_DIR}/repl/tests
                    env NEBULASTREAM=$<TARGET_FILE:nes-single-node-worker>
                    ${TEST_FILE}
            )
            set_tests_properties("distributed-repl-test-${TEST_NAME}" PROPERTIES LABELS "DockerCompose")
        endforeach ()
    endif ()
endif ()
