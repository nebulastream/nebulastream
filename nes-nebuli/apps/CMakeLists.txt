# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Nebuli is our REPL based frontend it comes in two flavours. The embedded version contains the single-node-worker and
# can thus execute queries locally. The non-embedded version always requires the '-s' flag to deploy remote queries.

# nebuli-embedded
add_executable(nebuli-embedded repl/ReplStarter.cpp repl/Repl.cpp)
target_link_libraries(nebuli-embedded PUBLIC nes-nebuli-embedded-lib argparse::argparse replxx::replxx)
target_include_directories(nebuli-embedded
        PUBLIC repl
)
add_test(
        NAME "repl-test"
        COMMAND
        env NEBULI=$<TARGET_FILE:nebuli-embedded>
        env NEBULI_TESTDATA=${CMAKE_CURRENT_SOURCE_DIR}/repl/tests
        env SYSTEST_TESTDATA=${CMAKE_CURRENT_BINARY_DIR}/../../nes-systests/testdata
        bats -x --verbose-run ${CMAKE_CURRENT_SOURCE_DIR}/repl/tests/nebuli_invocation_test.bats
)

# nebuli
add_executable(nebuli repl/ReplStarter.cpp repl/Repl.cpp)
target_link_libraries(nebuli PUBLIC nes-nebuli-lib argparse::argparse replxx::replxx)

target_include_directories(nebuli
        PUBLIC repl
)


# NebuCLI is the one-shot controller for the nes-single-node-worker, it is completely stateless and intended to deploy
# and control queries based on a topology specification which contains all state required to perform placement and
# optimizations.

add_executable(nebucli nebucli/NebuLIStarter.cpp)
target_link_libraries(nebucli PUBLIC nes-nebuli-lib argparse::argparse yaml-cpp::yaml-cpp)

target_include_directories(nebucli
        PRIVATE nebucli
)

include(FetchContent)
set(VERSION "v0.4.40")
set(BINARY_NAME "grpc_health_probe")
set(BINARY_SUFFIX "linux-amd64")
FetchContent_Declare(
        grpc_health_probe
        URL https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${VERSION}/${BINARY_NAME}-${BINARY_SUFFIX}
        URL_HASH SHA256=36f1e2bade1672ba7f3ddb4a14e5035c06856cc5fd4e9e538dd1d4878871edeb
        DOWNLOAD_NO_EXTRACT TRUE
)
FetchContent_MakeAvailable(grpc_health_probe)
file(CHMOD ${grpc_health_probe_SOURCE_DIR}/${BINARY_NAME}-${BINARY_SUFFIX}
        PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE)

add_test(
        NAME nebucli-test
        COMMAND
        env NEBUCLI=$<TARGET_FILE:nebucli>
        env NEBUCLI_TESTDATA=${CMAKE_CURRENT_SOURCE_DIR}/nebucli/tests
        env NEBULASTREAM=$<TARGET_FILE:nes-single-node-worker>
        env HEALTH_PROBE=${grpc_health_probe_SOURCE_DIR}/${BINARY_NAME}-${BINARY_SUFFIX}
        bats --filter-tags !docker -x --verbose-run ${CMAKE_CURRENT_SOURCE_DIR}/nebucli/tests/nebucli_invocation_test.bats
)

add_test(
        NAME nebucli-test-docker
        COMMAND
        env NEBUCLI=$<TARGET_FILE:nebucli>
        env NEBUCLI_TESTDATA=${CMAKE_CURRENT_SOURCE_DIR}/nebucli/tests
        env NEBULASTREAM=$<TARGET_FILE:nes-single-node-worker>
        env HEALTH_PROBE=${grpc_health_probe_SOURCE_DIR}/${BINARY_NAME}-${BINARY_SUFFIX}
        bats --filter-tags docker -x --verbose-run ${CMAKE_CURRENT_SOURCE_DIR}/nebucli/tests/nebucli_invocation_test.bats
)
