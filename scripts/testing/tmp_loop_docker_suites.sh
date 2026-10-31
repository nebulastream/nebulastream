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

# TEMPORARY (do not merge): repeats the docker compose suites in CI order until one fails, to catch the flaky
# TCP sink reconnect stall, which never showed up when the reconnect test ran on its own.
# systest-remote-test runs after tcp-sink-test and is left out to fit more passes into a job.
# Usage: tmp_loop_docker_suites.sh <build-dir> <passes>
set -euo pipefail

build_dir=$(realpath "$1")
passes=$2
disk_log="$build_dir/disk-usage.log"

# Logs free disk space and what docker and the test directories occupy, to find what fills the runner's disk.
# The build directory lives on the runner's root filesystem, which also holds /var/lib/docker.
report_disk() {
  local label=$1
  {
    echo "=== disk $label ($(date -u +%H:%M:%S)): $(df -h --output=avail,used,pcent "$build_dir" | tail -1 | xargs)" \
      "free/used/use%, test-tmp $(du -sh "$build_dir/test-tmp" 2>/dev/null | cut -f1)"
    docker system df --format '    docker {{.Type}}: {{.Size}} ({{.Reclaimable}} reclaimable)'
  } | tee -a "$disk_log"
}

# Every bats suite builds its images from a fresh context, which leaves build cache the size of the copied NES binaries
# behind. GitHub-hosted runners get a fresh machine per job, so all of it can go. Self-hosted runners share the docker
# daemon between jobs, so only the COPY layers of the test images are removed there.
prune_test_image_cache() {
  local filter=()
  [ "${RUNNER_ENVIRONMENT:-}" = github-hosted ] || filter=(--filter 'description~=COPY.nes-')
  docker builder prune -af "${filter[@]}" | tail -1 || echo "=== build cache prune failed"
}

report_disk "before first pass"
for pass in $(seq 1 "$passes"); do
  echo "=== docker suites pass $pass/$passes ($(date -u +%H:%M:%S))"
  if ! ctest --test-dir "$build_dir" --no-tests=error -j 1 -L DockerCompose -E '^systest-remote-test$' \
    --output-on-failure --output-junit "$build_dir/junit-docker-pass-$pass.xml" --timeout 2400 ${ADDITIONAL_CTEST_ARGS:-}; then
    echo "=== docker suites FAILED in pass $pass"
    report_disk "after failed pass $pass"
    exit 1
  fi
  prune_test_image_cache
  report_disk "after pass $pass"
done
echo "=== docker suites passed $passes passes"
