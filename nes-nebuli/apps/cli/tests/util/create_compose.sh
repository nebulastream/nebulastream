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

if [ $# -ne 1 ]; then
  echo "Error: Exactly one argument required"
  echo "Usage: $0 <filename>"
  exit 1
fi

# Check if the argument is an existing file
if [ ! -f "$1" ]; then
  echo "Error: '$1' is not a valid file or does not exist"
  exit 1
fi

# Check if yq is installed
if ! command -v yq &>/dev/null; then
  echo "Error: yq is required. Install with: sudo snap install yq"
  exit 1
fi

WORKERS_FILE=$1

if [ ! -f "$WORKERS_FILE" ]; then
  echo "$WORKERS_FILE does not exist"
  exit 1
fi

# Start building the compose file
cat <<EOF
services:
  nes-cli:
    image: nes-cli-image
    pull_policy: never
    environment:
      NES_WORKER_GRPC_ADDR: worker-node:8080
      NES_TOPOLOGY_FILE: $WORKERS_FILE
      XDG_STATE_HOME: $(pwd)/.xdg-state
    stop_grace_period: 0s
    command: ["sleep", "infinity"]
    working_dir: $(pwd)
    volumes:
      - $(pwd):$(pwd)
EOF

# Read workers and generate services
WORKER_COUNT=$(yq '.workers | length' "$WORKERS_FILE")

for i in $(seq 0 $((WORKER_COUNT - 1))); do
  # Extract worker data
  HOST=$(yq -r ".workers[$i].host" "$WORKERS_FILE")
  HOST_NAME=$(echo $HOST | cut -d':' -f1)
  GRPC=$(yq -r ".workers[$i].grpc" "$WORKERS_FILE")
  GRPC_PORT=$(echo $GRPC | cut -d':' -f2)

  # Generate service definition
  cat <<EOF
  $HOST_NAME:
    image: worker-image
    pull_policy: never
    working_dir: $(pwd)/$HOST_NAME
    healthcheck:
      test: ["CMD", "/bin/grpc_health_probe", "-addr=$HOST_NAME:$GRPC_PORT", "-connect-timeout", "5s" ]
      interval: 1s
      timeout: 5s
      retries: 3
      start_period: 0s
    command: [
      "--grpc=$HOST_NAME:$GRPC_PORT",
      "--connection=$HOST",
      "--worker.default_query_execution.execution_mode=INTERPRETER",
    ]
    volumes:
      - $(pwd):$(pwd)
EOF

done
