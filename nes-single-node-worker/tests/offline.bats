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

# This File contains all offline tests for worker. Offline tests are tests which do not require a worker and are
# completely local to worker

setup_file() {
  if [ -z "$NES_WORKER" ]; then
    echo "ERROR: NES_WORKER environment variable must be set" >&2
    echo "Usage: NES_WORKER=/path/to/nebucli bats nebucli.bats" >&2
    exit 1
  fi

  if [ -z "$NES_WORKER_TESTDATA" ]; then
    echo "ERROR: NES_WORKER_TESTDATA environment variable must be set" >&2
    echo "Usage: NES_WORKER_TESTDATA=/path/to/cli/testdata" >&2
    exit 1
  fi

  if [ ! -f "$NES_WORKER" ]; then
    echo "ERROR: NES_WORKER file does not exist: $NES_WORKER" >&2
    exit 1
  fi

  if [ ! -x "$NES_WORKER" ]; then
    echo "ERROR: NES_WORKER file is not executable: $NES_WORKER" >&2
    exit 1
  fi

  # Print environment info for debugging
  echo "# Using NES_WORKER: $NES_WORKER" >&3
  echo "# Using NES_WORKER_TESTDATA: $NES_WORKER_TESTDATA" >&3
}

teardown_file() {
  # Clean up any global resources if needed
  echo "# Test suite completed" >&3
}

setup() {
  export TMP_DIR=$(mktemp -d)

  cp -r "$NES_WORKER_TESTDATA" "$TMP_DIR"
  cd "$TMP_DIR" || exit

  echo "# Using TEST_DIR: $TMP_DIR" >&3
}

teardown() {
  # Clean up temporary directory
  if [ -n "$TMP_DIR" ] && [ -d "$TMP_DIR" ]; then
    rm -rf "$TMP_DIR"
  fi
}

@test "worker shows help" {
  run $NES_WORKER --help
  [ "$status" -eq 0 ]

  # Randomly pick words from the expected output
  echo "$output" | grep -q "worker"
  echo "$output" | grep -q "number_of_buffers_in_global_buffer_manager"
  echo "$output" | grep -q "query_engine"
  echo "$output" | grep -q "INTERPRETER"
}

@test "worker launches and stays alive" {
  run timeout 5 $NES_WORKER
  [ "$status" -eq 124 ] # killed by timeout
  grep "Starting SingleNodeWorker" singleNodeWorker.log
}

@test "worker accepts grpc address" {
  run timeout 5 $NES_WORKER --grpc=localhost:55555
  [ "$status" -eq 124 ] # killed by timeout
  grep "localhost:55555" singleNodeWorker.log

  run timeout 5 $NES_WORKER --grpc=0.0.0.0:55555
  [ "$status" -eq 124 ] # killed by timeout
  grep "0.0.0.0:55555" singleNodeWorker.log
}

@test "worker rejects bad grpc address" {
  # DNS Name is Valid, but cannot be resolved
  run timeout 10 $NES_WORKER --grpc=asdf.asdf.asdf:55555

  echo $output | grep "Name or service not known"
  grep "Failed to start GRPC Server" singleNodeWorker.log

  # DNS Name is Invalid
  run timeout 10 $NES_WORKER --grpc=wow!:55555
  grep "Invalid hostname: 'wow!'" singleNodeWorker.log
  [ "$status" -ne 0 ]
}

@test "worker accepts valid configs" {
  run timeout 5 $NES_WORKER --configPath=tests/good/config.yaml
  [ "$status" -eq 124 ] # killed by timeout
}

@test "worker warns when CLI overrides YAML config value" {
  # The YAML config sets number_of_worker_threads=4. Override it via CLI to trigger the warning.
  run timeout 5 $NES_WORKER --configPath=tests/good/config.yaml --worker.query_engine.number_of_worker_threads=8
  [ "$status" -eq 124 ] # killed by timeout

  # The log should contain the override warning
  grep "was already set" singleNodeWorker.log
  grep "overridden by a command-line argument" singleNodeWorker.log
}

@test "worker does not warn when CLI sets a value not in YAML" {
  # The YAML config does not set buffer_size_in_bytes. Setting it via CLI should not trigger a warning.
  run timeout 5 $NES_WORKER --configPath=tests/good/config.yaml --worker.buffer_size_in_bytes=8192
  [ "$status" -eq 124 ] # killed by timeout

  # The log should NOT contain the override warning for this key
  ! grep "buffer_size_in_bytes.*was already set" singleNodeWorker.log
}


