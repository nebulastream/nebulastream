# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_rust_cargo_test(nebucli)

add_executable(nes-cli main.cpp)
target_link_libraries(nes-cli PRIVATE nes-common)
target_link_rust_lib(nes-cli nebucli)

add_e2e_test(
    NAME offline-cli-test
    BATS_FILE tests/offline.bats
)

add_e2e_test(
    NAME distributed-cli-test
    BATS_FILE tests/distributed.bats
    DOCKER_COMPOSE
    JOBS 19
)
