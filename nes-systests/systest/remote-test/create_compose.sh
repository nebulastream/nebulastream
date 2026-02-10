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

if [ -z "$WORKER_IMAGE" ]; then
  echo "ERROR: WORKER_IMAGE is not set"
  exit 1
fi

if [ -z "$SYSTEST_IMAGE" ]; then
  echo "ERROR: SYSTEST_IMAGE is not set"
  exit 1
fi

if [ -z "$NES_DIR" ]; then
  echo "ERROR: NES_DIR is not set"
  exit 1
fi

if [ -z "$CONTAINER_WORKDIR" ]; then
  echo "ERROR: CONTAINER_WORKDIR is not set"
  exit 1
fi

if [ -z "$TEST_VOLUME" ]; then
  echo "ERROR: TEST_VOLUME is not set"
  exit 1
fi

if [ -z "$TESTDATA_VOLUME" ]; then
  echo "ERROR: TESTDATA_VOLUME is not set"
  exit 1
fi

if [ -z "$TESTCONFIG_VOLUME" ]; then
  echo "ERROR: TESTCONFIG_VOLUME is not set"
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

# Check if worker config is valid (not null, empty object, or whitespace)
is_valid_config() {
  local config="$1"
  if [ "$config" = "null" ] || [ "$config" = "{}" ] || [ -z "$config" ]; then
    return 1
  fi
  return 0
}

WORKERS_FILE=$1

if [ ! -f "$WORKERS_FILE" ]; then
  echo "$WORKERS_FILE does not exist"
  exit 1
fi

# Start building the compose file
# Volume mounts:
#   TESTDATA_VOLUME:   test input data -> /data
#   TESTCONFIG_VOLUME: contains /nes-systests/* -> $NES_DIR (reconstructs $NES_DIR/nes-systests/*)
#   TEST_VOLUME:       test working directory -> $CONTAINER_WORKDIR
cat <<EOF
services:
  systest:
    image: $SYSTEST_IMAGE
    pull_policy: never
    stop_grace_period: 0s
    command: ["sleep", "infinity"]
    working_dir: $CONTAINER_WORKDIR
    volumes:
      - $TESTDATA_VOLUME:/data
      - $TESTCONFIG_VOLUME:$NES_DIR
      - $TEST_VOLUME:$CONTAINER_WORKDIR
EOF

# Read workers and generate services
WORKER_COUNT=$(yq '.workers | length' "$WORKERS_FILE")

for i in $(seq 0 $((WORKER_COUNT - 1))); do
  # Extract worker data
  HOST=$(yq -r ".workers[$i].host" "$WORKERS_FILE")
  HOST_NAME=$(echo $HOST | cut -d':' -f1)
  GRPC=$(yq -r ".workers[$i].grpc" "$WORKERS_FILE")
  GRPC_PORT=$(echo $GRPC | cut -d':' -f2)

  # Check if worker has config
  HAS_CONFIG=$(yq ".workers[$i] | has(\"config\")" "$WORKERS_FILE")

  if [ "$HAS_CONFIG" = "true" ]; then
    CONFIG_CONTENT=$(yq ".workers[$i].config" "$WORKERS_FILE")

    if is_valid_config "$CONFIG_CONTENT"; then
      # Generate service with config file creation
      cat <<EOF
  $HOST_NAME:
    image: $WORKER_IMAGE
    pull_policy: never
    working_dir: $CONTAINER_WORKDIR/$HOST_NAME
    healthcheck:
      test: ["CMD", "/bin/grpc_health_probe", "-addr=$HOST_NAME:$GRPC_PORT", "-connect-timeout", "5s" ]
      interval: 1s
      timeout: 5s
      retries: 3
      start_period: 0s
    command:
      - /bin/bash
      - -c
      - |
        set -e
        mkdir -p $CONTAINER_WORKDIR/configs
        cat > $CONTAINER_WORKDIR/configs/$HOST_NAME.yaml <<'NES_CONFIG_EOF'
$(yq ".workers[$i].config" "$WORKERS_FILE")
        NES_CONFIG_EOF
        exec nes-single-node-worker --grpc=$HOST_NAME:$GRPC_PORT --connection=$HOST --configPath=$CONTAINER_WORKDIR/configs/$HOST_NAME.yaml
    volumes:
      - $TESTDATA_VOLUME:/data
      - $TEST_VOLUME:$CONTAINER_WORKDIR
      - $TESTCONFIG_VOLUME:$NES_DIR
EOF
      continue
    fi
  fi

  # Worker has no config or invalid config - use original command
  cat <<EOF
  $HOST_NAME:
    image: $WORKER_IMAGE
    pull_policy: never
    working_dir: $CONTAINER_WORKDIR/$HOST_NAME
    healthcheck:
      test: ["CMD", "/bin/grpc_health_probe", "-addr=$HOST_NAME:$GRPC_PORT", "-connect-timeout", "5s" ]
      interval: 1s
      timeout: 5s
      retries: 3
      start_period: 0s
    command: [
      "--grpc=$HOST_NAME:$GRPC_PORT",
      "--connection=$HOST",
    ]
    volumes:
      - $TESTDATA_VOLUME:/data
      - $TEST_VOLUME:$CONTAINER_WORKDIR
      - $TESTCONFIG_VOLUME:$NES_DIR
EOF

done

cat <<EOF
volumes:
  $TESTDATA_VOLUME:
    external: true
  $TESTCONFIG_VOLUME:
    external: true
  $TEST_VOLUME:
    external: true
EOF
