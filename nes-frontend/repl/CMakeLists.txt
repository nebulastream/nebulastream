# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# nes-repl is our REPL based frontend. It comes in two flavours: the embedded version contains the single-node-worker and
# can thus execute queries locally, while the non-embedded version deploys queries to remote workers.

# nes-repl-embedded
add_executable(nes-repl-embedded ReplStarter.cpp Repl.cpp)
# Runs the single-node worker in-process and registers a local worker at startup.
target_compile_definitions(nes-repl-embedded PRIVATE EMBED_ENGINE)
target_link_libraries(nes-repl-embedded PUBLIC nes-coordinator-bridge sql-planner argparse::argparse replxx::replxx)
target_include_directories(nes-repl-embedded PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# nes-repl
add_executable(nes-repl ReplStarter.cpp Repl.cpp)
target_link_libraries(nes-repl PUBLIC nes-coordinator-bridge sql-planner argparse::argparse replxx::replxx)
target_include_directories(nes-repl PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# E2E Tests for the REPL. The REPL has both interactive and non-interactive mode.
# We use bats to drive the non-interactive tests.
# The embedded version does not require a docker compose setup.

add_e2e_test(
    NAME embedded-repl-test
    BATS_FILE tests/embedded.bats
)

add_e2e_test(
    NAME distributed-repl-tests
    BATS_FILE tests/distributed.bats
    DOCKER_COMPOSE
)
