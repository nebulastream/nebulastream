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

source "$NES_BATS_LIB"

setup_file() {
  nes_require_env NES_SYSTEST
  nes_require_env NES_SERVER
  nes_require_env NES_DIR
  nes_require_env NES_TEST_TMP_DIR
  nes_require_env DATADIR
  nes_require_executable "$NES_SYSTEST"
  nes_require_executable "$NES_SERVER"
  echo "# Using NES_SYSTEST: $NES_SYSTEST" >&3
  echo "# Using NES_SERVER: $NES_SERVER" >&3
  echo "# Using DATADIR: $DATADIR" >&3
}

teardown_file() {
  nes_offline_teardown_file
}

setup() {
  nes_offline_setup
  nes_server_start --worker-mode embedded
}

teardown() {
  if [ -n "${NES_SERVER_PID:-}" ]; then
    nes_server_stop 10 || true
  fi
}

EXCLUDE_GROUPS=(large tcp)
if [ "${ENABLE_INFERENCE_TESTS:-OFF}" != "ON" ]; then
  EXCLUDE_GROUPS+=(Inference)
fi

systest_through_server() {
  "$NES_SYSTEST" --coordinator "$NES_SERVER_URL" --clusterConfig "$1" \
    --log-path "$TMP_DIR/systest.log" --workingDir "$TMP_DIR/workdir" --data "$DATADIR" \
    -e "${EXCLUDE_GROUPS[@]}" >&3
}

@test "two node systest through the server" {
  run systest_through_server "$NES_DIR/nes-systests/configs/topologies/two-node-with-interpreter.yaml"
  [ "$status" -eq 0 ]
}

@test "8 node systest through the server" {
  run systest_through_server "$NES_DIR/nes-systests/configs/topologies/8-node.yaml"
  [ "$status" -eq 0 ]
}
