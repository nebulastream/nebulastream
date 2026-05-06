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

setup_file() {
  # Clean up leaked containers and networks from previous crashed runs
  for net in $(docker network ls --filter label=nes-test=distributed-cli -q 2>/dev/null); do
    docker network inspect "$net" -f '{{range .Containers}}{{.Name}} {{end}}' 2>/dev/null | xargs -r docker rm -f 2>/dev/null || true
  done
  docker network prune -f --filter label=nes-test=distributed-cli 2>/dev/null || true
  # Remove all containers (running or stopped) referencing test images
  # from previous runs, so those images can later be deleted
  for img in $(docker images --filter reference='nes-worker-cli-test-*' --filter reference='nes-cli-image-*' -q 2>/dev/null); do
    docker ps -aq --filter ancestor="$img" 2>/dev/null | xargs -r docker rm -f 2>/dev/null || true
  done
  docker images --filter reference='nes-worker-cli-test-*' --filter reference='nes-cli-image-*' -q | xargs -r docker image rm -f 2>/dev/null || true

  # Validate environment variables
  if [ -z "$NES_CLI" ]; then
    echo "ERROR: NES_CLI environment variable must be set" >&2
    echo "Usage: NES_CLI=/path/to/nebucli bats nebucli.bats" >&2
    exit 1
  fi

  if [ -z "$NEBULASTREAM" ]; then
    echo "ERROR: NEBULASTREAM environment variable must be set" >&2
    echo "Usage: NEBULASTREAM=/path/to/nes-single-node-worker bats nebucli.bats" >&2
    exit 1
  fi

  if [ -z "$NES_CLI_TESTDATA" ]; then
    echo "ERROR: NES_CLI_TESTDATA environment variable must be set" >&2
    echo "Usage: NES_CLI_TESTDATA=/path/to/cli/testdata" >&2
    exit 1
  fi

  if [ -z "$NES_TEST_TMP_DIR" ]; then
    echo "ERROR: NES_TEST_TMP_DIR environment variable must be set" >&2
    echo "Usage: NES_TEST_TMP_DIR=/path/to/build/test-tmp" >&2
    exit 1
  fi

  if [ ! -f "$NES_CLI" ]; then
    echo "ERROR: NES_CLI file does not exist: $NES_CLI" >&2
    exit 1
  fi

  if [ ! -f "$NEBULASTREAM" ]; then
    echo "ERROR: NEBULASTREAM file does not exist: $NEBULASTREAM" >&2
    exit 1
  fi

  if [ ! -x "$NES_CLI" ]; then
    echo "ERROR: NES_CLI file is not executable: $NES_CLI" >&2
    exit 1
  fi

  if [ ! -x "$NEBULASTREAM" ]; then
    echo "ERROR: NEBULASTREAM file is not executable: $NEBULASTREAM" >&2
    exit 1
  fi

  if [ -z "$NES_RUNTIME_BASE_IMAGE" ]; then
    echo "ERROR: NES_RUNTIME_BASE_IMAGE environment variable must be set" >&2
    exit 1
  fi

  # Build Docker images with unique tags to avoid collisions when test suites run in parallel
  local suffix=$(head -c 8 /dev/urandom | od -An -tx1 | tr -d ' \n')
  export WORKER_IMAGE="nes-worker-cli-test-${suffix}"
  local worker_ctx=$(mktemp -d)
  cp $(realpath $NEBULASTREAM) "$worker_ctx/nes-single-node-worker"
  docker build --load -t $WORKER_IMAGE -f - "$worker_ctx" <<EOF
    FROM $NES_RUNTIME_BASE_IMAGE
    COPY nes-single-node-worker /usr/bin
    ENTRYPOINT ["nes-single-node-worker"]
EOF
  rm -rf "$worker_ctx"
  export CLI_IMAGE="nes-cli-image-${suffix}"
  local cli_ctx=$(mktemp -d)
  cp $(realpath $NES_CLI) "$cli_ctx/nes-cli"
  docker build --load -t $CLI_IMAGE -f - "$cli_ctx" <<EOF
    FROM $NES_RUNTIME_BASE_IMAGE
    COPY nes-cli /usr/bin
EOF
  rm -rf "$cli_ctx"

  # Print environment info for debugging
  echo "# Using NES_CLI: $NES_CLI" >&3
  echo "# Using NEBULASTREAM: $NEBULASTREAM" >&3
  echo "# Using WORKER_IMAGE: $WORKER_IMAGE" >&3
  echo "# Using CLI_IMAGE: $CLI_IMAGE" >&3
}

teardown_file() {
  echo "# Test suite completed" >&3
  docker rmi $WORKER_IMAGE || true
  docker rmi $CLI_IMAGE || true
}

setup() {
  # Create temp directory within the mounted workspace (not /tmp)
  # so it's accessible from docker-compose containers running on the host
  mkdir -p "$NES_TEST_TMP_DIR"
  export TMP_DIR=$(mktemp -d -p "$NES_TEST_TMP_DIR")
  cp -r "$NES_CLI_TESTDATA" "$TMP_DIR"
  cd "$TMP_DIR" || exit
  echo "# Using TEST_DIR: $TMP_DIR" >&3

  volume=$(docker volume create)
  volume_host_container=$(docker run -d --rm -v $volume:/data alpine sleep infinite)
  docker cp . $volume_host_container:/data
  docker stop -t0 $volume_host_container
  export TEST_VOLUME=$volume
  echo "# Using test volume: $TEST_VOLUME" >&3
}

sync_workdir() {
  volume_host_container=$(docker run -d --rm -v $TEST_VOLUME:/data alpine sleep infinite)
  docker cp $volume_host_container:/data/. .
  docker stop -t0 $volume_host_container
}

teardown() {
  sync_workdir || true
  docker compose down -v || true
  docker volume rm $TEST_VOLUME || true
}

function setup_distributed() {
  tests/util/create_compose.sh "$1" > docker-compose.yaml
  local compose_output exit_code=0
  compose_output=$(docker compose up -d --wait 2>&1) || exit_code=$?
  if [ "$exit_code" -ne 0 ]; then
    echo "# [docker compose up] (status=$exit_code):" >&3
    while IFS= read -r line; do echo "#   $line" >&3; done <<< "$compose_output"
  fi
  return $exit_code
}

DOCKER_NES_CLI() {
  # docker compose exec v2 disconnects the session when its stdin reaches EOF
  # (docker/compose#10418). In bats subshells stdin is closed, so we pipe from
  # tail to keep the connection alive.
  tail -f /dev/null | docker compose exec -T nes-cli nes-cli "$@"
}


DOCKER_MQTT_PRODUCE() {
  TOPIC=$1
  shift
  docker compose exec mqtt-client mosquitto_pub -h mqtt-broker -t $TOPIC -m "$@"
}

DOCKER_MQTT_SUBSCRIBE() {
  TOPIC=$1
  shift
  docker compose exec mqtt-client mosquitto_sub -h mqtt-broker -t $TOPIC >&3
}

assert_json_equal() {
  local expected="$1"
  local actual="$2"

  diff <(echo "$expected" | jq --sort-keys .) \
    <(echo "$actual" | jq --sort-keys .)
}

assert_json_contains() {
  local expected="$1"
  local actual="$2"

  local result=$(echo "$actual" | jq --argjson exp "$expected" 'contains($exp)')

  if [ "$result" != "true" ]; then
    echo "JSON subset check failed"
    echo "Expected (subset): $expected"
    echo "Actual: $actual"
    return 1
  fi
}

@test "launch query from topology" {
  setup_distributed tests/good/example.yaml
  run DOCKER_NES_CLI -t tests/good/example.yaml start
  [ "$status" -eq 0 ]
  query_id=$output

  sleep 1
  DOCKER_MQTT_PRODUCE mqtt-source-test $'32.0\n'
  DOCKER_MQTT_PRODUCE mqtt-source-test $'32.0\n'
  sleep 1

  run DOCKER_NES_CLI -t tests/good/single-worker-with-4k-buffers.yaml stop $query_id
  sleep 1

  sync_workdir
  grep "2,410" worker-1/results.csv
}

@test "launch query from commandline" {
  setup_distributed tests/good/single-worker-with-4k-buffers.yaml
  run DOCKER_NES_CLI -t tests/good/single-worker-with-4k-buffers.yaml start "$(cat <<'EOF'
    SELECT * FROM MQTT(
        "worker-1:8080" as `SOURCE`.`HOST`,
        'mqtt-source-test' AS `SOURCE`.TOPIC,
        'mqtt-broker' AS `SOURCE`.SERVER_URI,
        100 AS `SOURCE`.FLUSH_INTERVAL_MS,
        2 AS `SOURCE`.QOS,
        'CSV' AS PARSER.`TYPE`,
        SCHEMA(id UINT64 NOT NULL) AS `SOURCE`.`SCHEMA`
    ) INTO CHECKSUM("worker-1:8080" as `SINK`.`HOST`, "results.csv" as `SINK`.`FILE_PATH`)
EOF
)"

  [ "$status" -eq 0 ]
  query_id=$output

  sleep 1
  DOCKER_MQTT_PRODUCE mqtt-source-test $'32\n'
  DOCKER_MQTT_PRODUCE mqtt-source-test $'32\n'
  sleep 1

  run DOCKER_NES_CLI -t tests/good/single-worker-with-4k-buffers.yaml stop $query_id
  [ "$status" -eq 0 ]
  sleep 1

  sync_workdir
  grep "2,222" worker-1/results.csv
}

@test "long flushing interval" {
  setup_distributed tests/good/single-worker-with-4k-buffers.yaml
  run DOCKER_NES_CLI -t tests/good/single-worker-with-4k-buffers.yaml start "$(cat <<'EOF'
    SELECT * FROM MQTT(
        "worker-1:8080" as `SOURCE`.`HOST`,
        'mqtt-source-test' AS `SOURCE`.TOPIC,
        'mqtt-broker' AS `SOURCE`.SERVER_URI,
        5000 AS `SOURCE`.FLUSH_INTERVAL_MS,
        2 AS `SOURCE`.QOS,
        'CSV' AS PARSER.`TYPE`,
        SCHEMA(id UINT64 NOT NULL) AS `SOURCE`.`SCHEMA`
    ) INTO FILE("CSV" AS `SINK`.OUTPUT_FORMAT, "worker-1:8080" as `SINK`.`HOST`, "results.csv" as `SINK`.`FILE_PATH`)
EOF
)"

  [ "$status" -eq 0 ]
  query_id=$output

  sleep 1
  DOCKER_MQTT_PRODUCE mqtt-source-test $'32\n'
  DOCKER_MQTT_PRODUCE mqtt-source-test $'32\n'
  sleep 1

  sync_workdir
  [ "$(cat worker-1/results.csv | wc -l)" -eq 0 ]

  sleep 6
  sync_workdir
  # header + 2 rows
  [ "$(cat worker-1/results.csv | wc -l)" -eq 3 ]
}


@test "long flushing interval flush on buffer full" {

  # configures the buffer to use 4096 bytes.
  # the source receives raw ascii csv. the data is always ['3', '2', '\n'] (3 bytes)

  setup_distributed tests/good/single-worker-with-4k-buffers.yaml
  run DOCKER_NES_CLI -t tests/good/single-worker-with-4k-buffers.yaml start "$(cat <<'EOF'
    SELECT * FROM MQTT(
        "worker-1:8080" as `SOURCE`.`HOST`,
        'mqtt-source-test' AS `SOURCE`.TOPIC,
        'mqtt-broker' AS `SOURCE`.SERVER_URI,
        1000000 AS `SOURCE`.FLUSH_INTERVAL_MS,
        2 AS `SOURCE`.QOS,
        'CSV' AS PARSER.`TYPE`,
        SCHEMA(id UINT64 NOT NULL) AS `SOURCE`.`SCHEMA`
    ) INTO FILE("CSV" AS `SINK`.OUTPUT_FORMAT, "worker-1:8080" as `SINK`.`HOST`, "results.csv" as `SINK`.`FILE_PATH`)
EOF
)"

  [ "$status" -eq 0 ]
  query_id=$output

  sleep 1
  #
  for i in {1..68}; do (DOCKER_MQTT_PRODUCE mqtt-source-test $'32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n'&); done
  wait
  sleep 1

  sync_workdir
  [ "$(cat worker-1/results.csv | wc -l)" -eq 0 ]

  DOCKER_MQTT_PRODUCE mqtt-source-test $'32\n32\n32\n32\n32\n32\n'

  sleep 1
  sync_workdir

  # header + 1365 rows (4096 / 3)
  [ "$(cat worker-1/results.csv | wc -l)" -eq 1366 ]
}

@test "more data" {
  setup_distributed tests/good/single-worker-with-4k-buffers.yaml
  run DOCKER_NES_CLI -t tests/good/single-worker-with-4k-buffers.yaml start "$(cat <<'EOF'
    SELECT * FROM MQTT(
        "worker-1:8080" as `SOURCE`.`HOST`,
        'mqtt-source-test' AS `SOURCE`.TOPIC,
        'mqtt-broker' AS `SOURCE`.SERVER_URI,
        100 AS `SOURCE`.FLUSH_INTERVAL_MS,
        2 AS `SOURCE`.QOS,
        'CSV' AS PARSER.`TYPE`,
        SCHEMA(id UINT64 NOT NULL) AS `SOURCE`.`SCHEMA`
    ) INTO CHECKSUM("CSV" AS `SINK`.OUTPUT_FORMAT, "worker-1:8080" as `SINK`.`HOST`, "results.csv" as `SINK`.`FILE_PATH`)
EOF
)"

  [ "$status" -eq 0 ]
  query_id=$output

  sleep 1

  # 30000 tuples
  # 2 producers
  for i in {1..2}; do
    (
        # 250 messages
        for j in {1..250}; do
            # 60 per message
            DOCKER_MQTT_PRODUCE mqtt-source-test $'32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n32\n'
        done
    )&
  done

  # 500 tuples
  # 2 producers
  for i in {1..2}; do
    (
        # 250 messages
        for j in {1..250}; do
            # 1 per message
            DOCKER_MQTT_PRODUCE mqtt-source-test $'32\n'
        done
    )&
  done
  wait

  sleep 1
  run DOCKER_NES_CLI -t tests/good/single-worker-with-4k-buffers.yaml stop $query_id

  sync_workdir
  grep "30500" worker-1/results.csv
}
