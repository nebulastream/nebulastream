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

# Workaround: both nes_rust_bindings and coordinator_bridge are separate Rust staticlibs
# that each embed an identical copy of the cxx runtime. Allow the linker to deduplicate.

# TODO: merge into a single Rust staticlib to avoid this.
set(CXX_MULDEFS_FLAG "LINKER:-z,muldefs")

# nes-repl-embedded
add_executable(nes-repl-embedded repl/ReplStarter.cpp repl/Repl.cpp)
target_link_libraries(nes-repl-embedded PUBLIC nes-frontend-lib nes-coordinator-bridge sql-planner argparse::argparse replxx::replxx)
target_link_options(nes-repl-embedded PRIVATE ${CXX_MULDEFS_FLAG})
target_include_directories(nes-repl-embedded
        PUBLIC repl
)

# nes-repl
add_executable(nes-repl repl/ReplStarter.cpp repl/Repl.cpp)
target_link_libraries(nes-repl PUBLIC nes-frontend-lib nes-coordinator-bridge sql-planner argparse::argparse replxx::replxx)
target_link_options(nes-repl PRIVATE ${CXX_MULDEFS_FLAG})

target_include_directories(nes-repl
        PUBLIC repl
)

# E2E Tests for the REPL. The REPL has both interactive and non-interactive mode.
# We use bats to drive the non-interactive tests.
# The embedded version does not require a docker compose setup.

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
