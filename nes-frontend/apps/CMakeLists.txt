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
target_link_libraries(nes-repl-embedded PUBLIC nes-frontend-embedded-lib argparse::argparse replxx::replxx)
target_include_directories(nes-repl-embedded
        PUBLIC repl
)

# nes-repl
add_executable(nes-repl repl/ReplStarter.cpp repl/Repl.cpp)
target_link_libraries(nes-repl PUBLIC nes-frontend-lib argparse::argparse replxx::replxx)

target_include_directories(nes-repl
        PUBLIC repl
)

# nes-cli is the one-shot controller for the nes-single-node-worker, it is completely stateless and intended to deploy
# and control queries based on a topology specification which contains all state required to perform placement and
# optimizations.

add_executable(nes-cli cli/CLIStarter.cpp cli/QueryStateBackend.cpp)
target_link_libraries(nes-cli PUBLIC nes-frontend-lib argparse::argparse yaml-cpp::yaml-cpp)

target_include_directories(nes-cli
        PRIVATE cli
)

# These are E2E Tests for both the CLI tool and the repl.
# Because the REPL has both a interactive and non-interactive mode, the REPL is tested in both ways.
# We use bats to drive the non-interactive tests for the cli and repl
# This also tests the embedded version of the repl, which does not require a docker compose setup.

add_e2e_test(
    NAME embedded-repl-test
    BATS_FILE repl/tests/embedded.bats
)

add_e2e_test(
    NAME offline-cli-test
    BATS_FILE cli/tests/offline.bats
)

# These tests use docker CLI to run docker-compose from within the development container.
add_e2e_test(
    NAME distributed-cli-test
    BATS_FILE cli/tests/distributed.bats
    DOCKER_COMPOSE
    JOBS 4
)

add_e2e_test(
    NAME distributed-repl-tests
    BATS_FILE repl/tests/distributed.bats
    DOCKER_COMPOSE
)
