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

if [ -z "$CLI_IMAGE" ]; then
  echo "ERROR: CLI_IMAGE is not set"
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

# Check if worker config is valid (not null, empty object, or whitespace)
is_valid_config() {
  local config="$1"
  if [ "$config" = "null" ] || [ "$config" = "{}" ] || [ -z "$config" ]; then
    return 1
  fi
  return 0
}

# NOTE: $WORKER_IMAGE must already have the PostgreSQL ODBC driver
# installed. The bats setup_file hook extends the base worker image
# once per suite (re-tagging back to $WORKER_IMAGE) so we don't pay
# the rebuild cost on every test.

# Start building the compose file
cat <<EOF
services:
  postgres:
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: nes
      POSTGRES_PASSWORD: nes
      POSTGRES_DB: nesdb
      POSTGRES_INITDB_ARGS: "--no-sync"
    command:
      - "postgres"
      - "-c"
      - "fsync=off"
      - "-c"
      - "synchronous_commit=off"
      - "-c"
      - "full_page_writes=off"
      - "-c"
      - "checkpoint_timeout=30min"
      - "-c"
      - "max_wal_size=4GB"
      - "-c"
      - "shared_buffers=256MB"
      - "-c"
      - "log_statement=none"
    tmpfs:
      - /var/lib/postgresql/data:rw,size=512m
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U nes -d nesdb"]
      interval: 2s
      timeout: 3s
      retries: 15
      start_period: 5s

  nes-cli:
    image: $CLI_IMAGE
    pull_policy: never
    environment:
      NES_TOPOLOGY_FILE: $WORKERS_FILE
      XDG_STATE_HOME: /workdir/.xdg-state
    stop_grace_period: 0s
    working_dir: /workdir
    command: ["sleep", "infinity"]
    volumes:
      - $TEST_VOLUME:/workdir
EOF

# Read workers and generate services
WORKER_COUNT=$(yq '.workers | length' "$WORKERS_FILE")

for i in $(seq 0 $((WORKER_COUNT - 1))); do
  # Extract worker data
  HOST=$(yq -r ".workers[$i].host" "$WORKERS_FILE")
  HOST_NAME=$(echo $HOST | cut -d':' -f1)
  HOST_PORT=$(echo $HOST | cut -d':' -f2)
  DATA=$(yq -r ".workers[$i].data_address" "$WORKERS_FILE")

  # Check if worker has config
  HAS_CONFIG=$(yq ".workers[$i] | has(\"config\")" "$WORKERS_FILE")

  if [ "$HAS_CONFIG" = "true" ]; then
    CONFIG_CONTENT=$(yq ".workers[$i].config" "$WORKERS_FILE")

    if is_valid_config "$CONFIG_CONTENT"; then
      # Base64-encode the config to avoid YAML-in-YAML embedding issues.
      CONFIG_B64=$(echo "$CONFIG_CONTENT" | base64 -w0)
      # Generate service with config file creation.
      # Override the entrypoint since the worker image uses ENTRYPOINT ["nes-single-node-worker"].
      cat <<EOF
  $HOST_NAME:
    image: $WORKER_IMAGE
    pull_policy: never
    working_dir: /workdir/$HOST_NAME
    depends_on:
      postgres:
        condition: service_healthy
    # The worker statically links unixodbc from vcpkg, which hardcodes a
    # vcpkg-prefix sysconfdir. Override via ODBCSYSINI so the driver
    # manager reads the odbcinst.ini we mount below.
    environment:
      ODBCSYSINI: /etc
    configs:
      - source: odbcinst_ini
        target: /etc/odbcinst.ini
    healthcheck:
      test: ["CMD", "/bin/grpc_health_probe", "-addr=$HOST_NAME:$HOST_PORT", "-connect-timeout", "5s" ]
      interval: 1s
      timeout: 5s
      retries: 10
      start_period: 60s
    entrypoint: ["/bin/bash", "-c"]
    command:
      - |
        set -e
        mkdir -p /workdir/configs
        echo '$CONFIG_B64' | base64 -d > /workdir/configs/$HOST_NAME.yaml
        exec nes-single-node-worker --grpc=$HOST_NAME:$HOST_PORT --data_address=$DATA --worker.default_query_execution.execution_mode=INTERPRETER --worker.query_engine.number_of_worker_threads=1 --configPath=/workdir/configs/$HOST_NAME.yaml
    volumes:
      - $TEST_VOLUME:/workdir
EOF
      continue
    fi
  fi

  # Worker has no config or invalid config - use simple command
  cat <<EOF
  $HOST_NAME:
    image: $WORKER_IMAGE
    pull_policy: never
    working_dir: /workdir/$HOST_NAME
    depends_on:
      postgres:
        condition: service_healthy
    # The worker statically links unixodbc from vcpkg, which hardcodes a
    # vcpkg-prefix sysconfdir. Override via ODBCSYSINI so the driver
    # manager reads the odbcinst.ini we mount below.
    environment:
      ODBCSYSINI: /etc
    configs:
      - source: odbcinst_ini
        target: /etc/odbcinst.ini
    healthcheck:
      test: ["CMD", "/bin/grpc_health_probe", "-addr=$HOST_NAME:$HOST_PORT", "-connect-timeout", "5s" ]
      interval: 1s
      timeout: 5s
      retries: 10
      start_period: 60s
    command: [
      "--grpc=$HOST_NAME:$HOST_PORT",
      "--data_address=$DATA",
      "--worker.default_query_execution.execution_mode=INTERPRETER",
      "--worker.query_engine.number_of_worker_threads=1",
    ]
    volumes:
      - $TEST_VOLUME:/workdir
EOF

done

cat <<EOF
networks:
  default:
    labels:
      nes-test: distributed-cli
volumes:
  $TEST_VOLUME:
    external: true
configs:
  odbcinst_ini:
    content: |
      [PostgreSQL]
      Description=PostgreSQL ODBC Driver (Unicode)
      Driver=/usr/lib/x86_64-linux-gnu/odbc/psqlodbcw.so
      UsageCount=1
EOF
