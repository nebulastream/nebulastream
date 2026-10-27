#!/usr/bin/env bash

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# TEMPORARY (do not merge): repeats the TCP sink reconnect test until it fails to catch the flaky stall with diagnostics.
# Usage: tmp_loop_tcp_reconnect.sh <build-dir> <iterations>
set -euo pipefail

build_dir=$1
iterations=$2

# The first regular run also sets up the ctest fixtures (binaries and runtime image) the bats suite depends on.
[ -n "${SKIP_FIRST_RUN:-}" ] || ctest --test-dir "$build_dir" --no-tests=error -j 1 -R '^tcp-sink-test$' --output-on-failure --timeout 2400 ${ADDITIONAL_CTEST_ARGS:-}

# Reuses the exact ctest command (env and bats flags) and restricts bats to the reconnect test.
ctest --test-dir "$build_dir" -R '^tcp-sink-test$' --show-only=json-v1 > "$build_dir/tcp-sink-test.json"
mapfile -d '' -t command < <(python3 -c '
import json, sys
test = next(t for t in json.load(open(sys.argv[1]))["tests"] if t["name"] == "tcp-sink-test")
command = test["command"]
command = command[:-1] + ["--filter", "^reconnects after the TCP peer restarts$", command[-1]]
directory = next((p["value"] for p in test.get("properties", []) if p["name"] == "WORKING_DIRECTORY"), ".")
sys.stdout.write("\0".join([directory] + command) + "\0")
' "$build_dir/tcp-sink-test.json")
working_directory=${command[0]}
command=("${command[@]:1}")

cd "$working_directory"
for iteration in $(seq 1 "$iterations"); do
  echo "=== reconnect iteration $iteration/$iterations ($(date -u +%H:%M:%S))"
  if ! "${command[@]}"; then
    echo "=== reconnect test FAILED in iteration $iteration"
    exit 1
  fi
done
echo "=== reconnect test passed $iterations iterations"
