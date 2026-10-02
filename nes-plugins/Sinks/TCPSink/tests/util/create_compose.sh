#!/bin/bash

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eo pipefail

if [ $# -ne 1 ] || [ ! -f "$1" ]; then
  echo "Usage: $0 <topology-file>" >&2
  exit 1
fi

for required_variable in WORKER_IMAGE CLI_IMAGE TEST_DIR; do
  if [ -z "${!required_variable}" ]; then
    echo "ERROR: $required_variable is not set" >&2
    exit 1
  fi
done

cat <<EOF
services:
  tcp-client:
    image: busybox:1.38.0
    working_dir: /workdir
    entrypoint: ["/bin/sh", "-c"]
    command: ["exec nc -l -p 9000 >> /workdir/results.csv"]
    stop_grace_period: 0s
    volumes:
      - type: bind
        source: "$TEST_DIR"
        target: /workdir

  nes-cli:
    image: $CLI_IMAGE
    pull_policy: never
    environment:
      NES_TOPOLOGY_FILE: $1
      XDG_STATE_HOME: /workdir/.xdg-state
    stop_grace_period: 0s
    working_dir: /workdir
    command: ["sleep", "infinity"]
    volumes:
      - type: bind
        source: "$TEST_DIR"
        target: /workdir

  worker-1:
    image: $WORKER_IMAGE
    pull_policy: never
    working_dir: /workdir/worker-1
    depends_on:
      tcp-client:
        condition: service_started
    healthcheck:
      test: ["CMD", "/bin/grpc_health_probe", "-addr=worker-1:8080", "-connect-timeout", "5s"]
      interval: 1s
      timeout: 5s
      retries: 10
      start_interval: 100ms
      start_period: 60s
    command: [
      "--",
      "--grpc=worker-1:8080",
      "--data_address=worker-1:9090",
      "--worker.default_query_execution.execution_mode=COMPILER",
      "--worker.default_query_execution.operator_buffer_size=4096",
      "--worker.query_engine.number_of_worker_threads=4",
    ]
    volumes:
      - type: bind
        source: "$TEST_DIR"
        target: /workdir

networks:
  default:
    labels:
      nes-test: ${NES_BATS_TEST_LABEL:-distributed-tcp-sink}
EOF
