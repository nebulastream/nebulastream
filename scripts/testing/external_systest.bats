#!/usr/bin/env bats

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Generic runner for "systest with an external dependency" — one .test DSL
# file plus a plugin-local compose.snippet.yaml. No per-plugin bats file is
# needed; CMake's add_external_systest_profile() points every profile at this
# same .bats and parameterises it via PROFILE_DIR + PROFILE_NAME + TEST_FILE
# env vars.
#
# All real work lives in the nes_external_systest_* preset in
# distributed_bats_lib.bash.

source "$NES_BATS_LIB"

setup_file()    { nes_external_systest_setup_file; }
teardown_file() { nes_external_systest_teardown_file; }
setup()         { nes_external_systest_setup; }
teardown()      { nes_external_systest_teardown; }

@test "external-systest profile=${PROFILE_NAME}" {
  nes_external_systest_compose_up
  run nes_external_systest_run
  [ "$status" -eq 0 ]
}
