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

if [ -z "$TEST_DIR" ]; then
  echo "ERROR: TEST_DIR is not set"
  exit 1
fi

if [ -z "$TESTDATA_DIR" ]; then
  echo "ERROR: TESTDATA_DIR is not set"
  exit 1
fi

if [ -z "$TESTCONFIG_DIR" ]; then
  echo "ERROR: TESTCONFIG_DIR is not set"
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

# ExternalData entries may be symlinks into a store outside TESTDATA_DIR.
# CMake has already verified this optional mount with the host Docker daemon.
TESTDATA_CACHE_MOUNT=
TESTDATA_CACHE_VOLUME=
if [ -n "$NES_DOCKER_EXTERNAL_DATA_MOUNT_TYPE" ]; then
  TESTDATA_CACHE_MOUNT="      - type: $NES_DOCKER_EXTERNAL_DATA_MOUNT_TYPE
        source: \"$NES_DOCKER_EXTERNAL_DATA_SOURCE\"
        target: \"$NES_DOCKER_EXTERNAL_DATA_TARGET\"
        read_only: true"
  if [ "$NES_DOCKER_EXTERNAL_DATA_MOUNT_TYPE" = volume ]; then
    TESTDATA_CACHE_VOLUME="volumes:
  $NES_DOCKER_EXTERNAL_DATA_SOURCE:
    external: true"
  fi
fi

# Start building the compose file
# Volume mounts:
#   TESTDATA_DIR:   test input data -> /data
#   TESTCONFIG_DIR: repository checkout -> $NES_DIR
#   TEST_DIR:       test working directory -> $CONTAINER_WORKDIR
cat <<EOF
services:
  systest:
    image: $SYSTEST_IMAGE
    pull_policy: never
    stop_grace_period: 0s
    command: ["sleep", "infinity"]
    working_dir: $CONTAINER_WORKDIR
    volumes:
      - type: bind
        source: "$TESTDATA_DIR"
        target: /data
$TESTDATA_CACHE_MOUNT
      - type: bind
        source: "$TESTCONFIG_DIR"
        target: "$NES_DIR"
      - type: bind
        source: "$TEST_DIR"
        target: "$CONTAINER_WORKDIR"
EOF

# Read workers and generate services
WORKER_COUNT=$(yq '.workers | length' "$WORKERS_FILE")

for i in $(seq 0 $((WORKER_COUNT - 1))); do
  GRPC=$(yq -r ".workers[$i].host" "$WORKERS_FILE")
  HOST_NAME=$(echo $GRPC | cut -d':' -f1)
  GRPC_PORT=$(echo $GRPC | cut -d':' -f2)
  DATA=$(yq -r ".workers[$i].data_address" "$WORKERS_FILE")

  HAS_CONFIG=$(yq ".workers[$i] | has(\"config\")" "$WORKERS_FILE")
  CONFIG_ARG=""
  if [ "$HAS_CONFIG" = "true" ]; then
    CONFIG_ARG="\"--configPath=$CONTAINER_WORKDIR/configs/$HOST_NAME.yaml\","
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
      "--grpc=$HOST_NAME:$GRPC_PORT",
      "--data_address=$DATA",
      $CONFIG_ARG
    ]
    volumes:
      - type: bind
        source: "$TESTDATA_DIR"
        target: /data
$TESTDATA_CACHE_MOUNT
      - type: bind
        source: "$TEST_DIR"
        target: "$CONTAINER_WORKDIR"
      - type: bind
        source: "$TESTCONFIG_DIR"
        target: "$NES_DIR"
EOF

done

cat <<EOF
networks:
  default:
    labels:
      nes-test: systest-remote
$TESTDATA_CACHE_VOLUME
EOF
