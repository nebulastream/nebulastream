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

run_yq() {
  if command -v yq &>/dev/null; then
    yq "$@"
    return
  fi

  if command -v nix &>/dev/null; then
    nix develop "$NES_DIR" -c yq "$@"
    return
  fi

  echo "Error: yq is required"
  exit 1
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
NIX_STORE_MOUNT=''
if [ -d /nix/store ]; then
  NIX_STORE_MOUNT='      - /nix/store:/nix/store:ro'
fi
cat <<EOF
services:
  systest:
    image: $SYSTEST_IMAGE
    pull_policy: never
    stop_grace_period: 0s
    command: ["sleep", "infinity"]
    working_dir: $CONTAINER_WORKDIR
    environment:
      NES_SYSTEST_INLINE_EVENT_HOST: systest
    volumes:
      - $TESTDATA_VOLUME:/data
$NIX_STORE_MOUNT
      - $TESTCONFIG_VOLUME:$NES_DIR
      - $TEST_VOLUME:$CONTAINER_WORKDIR
EOF

# Read workers and generate services
WORKER_COUNT=$(run_yq '.workers | length' "$WORKERS_FILE")

for i in $(seq 0 $((WORKER_COUNT - 1))); do
  GRPC=$(run_yq -r ".workers[$i].host" "$WORKERS_FILE")
  HOST_NAME=$(echo $GRPC | cut -d':' -f1)
  GRPC_PORT=$(echo $GRPC | cut -d':' -f2)
  DATA=$(run_yq -r ".workers[$i].data" "$WORKERS_FILE")

  HAS_CONFIG=$(run_yq ".workers[$i] | has(\"config\")" "$WORKERS_FILE")
  COMMAND_ARGS=$(printf '      "--grpc=%s",\n      "--data=%s"' "$HOST_NAME:$GRPC_PORT" "$DATA")
  if [ "$HAS_CONFIG" = "true" ]; then
    COMMAND_ARGS=$(printf '%s,\n      "--configPath=%s/configs/%s.yaml"' "$COMMAND_ARGS" "$CONTAINER_WORKDIR" "$HOST_NAME")
  fi

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
$COMMAND_ARGS
    ]
    volumes:
      - $TESTDATA_VOLUME:/data
$NIX_STORE_MOUNT
      - $TEST_VOLUME:$CONTAINER_WORKDIR
      - $TESTCONFIG_VOLUME:$NES_DIR
EOF

done

cat <<EOF
networks:
  default:
    labels:
      nes-test: systest-remote
volumes:
  $TESTDATA_VOLUME:
    external: true
  $TESTCONFIG_VOLUME:
    external: true
  $TEST_VOLUME:
    external: true
EOF
