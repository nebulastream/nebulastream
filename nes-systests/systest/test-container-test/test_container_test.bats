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
  for net in $(docker network ls --filter label=nes-test=test-container -q 2>/dev/null); do
    docker network inspect "$net" -f '{{range .Containers}}{{.Name}} {{end}}' 2>/dev/null | xargs -r docker rm -f 2>/dev/null || true
  done
  docker network prune -f --filter label=nes-test=test-container 2>/dev/null || true

  if [ -z "$SYSTEST" ]; then
    echo "ERROR: SYSTEST environment variable must be set" >&2
    exit 1
  fi

  if [ -z "$NES_DIR" ]; then
    echo "ERROR: NES_DIR environment variable must be set" >&2
    exit 1
  fi

  if [ -z "$NES_TEST_TMP_DIR" ]; then
    echo "ERROR: NES_TEST_TMP_DIR environment variable must be set" >&2
    exit 1
  fi

  if [ -z "$NES_RUNTIME_BASE_IMAGE" ]; then
    echo "ERROR: NES_RUNTIME_BASE_IMAGE environment variable must be set" >&2
    exit 1
  fi

  if [ ! -x "$SYSTEST" ]; then
    echo "ERROR: SYSTEST file is not executable: $SYSTEST" >&2
    exit 1
  fi

  # Build the systest Docker image with a unique tag to avoid collisions when test suites run in parallel.
  # We build on top of the pre-built runtime base image so that we do not need any network access here.
  # Use a minimal build context with only the required binary to avoid sending whole build directories
  # to the Docker daemon.
  local suffix=$(head -c 8 /dev/urandom | od -An -tx1 | tr -d ' \n')
  export SYSTEST_IMAGE="nes-systest-testcontainer-${suffix}"
  local systest_ctx=$(mktemp -d)
  cp "$(realpath "$SYSTEST")" "$systest_ctx/systest"
  docker build --load -t "$SYSTEST_IMAGE" -f - "$systest_ctx" <<EOF
    FROM $NES_RUNTIME_BASE_IMAGE
    COPY systest /usr/bin
EOF
  rm -rf "$systest_ctx"

  export CONTAINER_WORKDIR="/$(cat /proc/sys/kernel/random/uuid)"

  # Volume containing the systest discovery directory ($NES_DIR/nes-systests/*).
  # We cannot bind-mount as we are potentially running in a container using Docker out of Docker.
  export TESTCONFIG_VOLUME=$(docker volume create)
  local volume_host_container=$(docker run -d --rm -v "$TESTCONFIG_VOLUME":/config alpine sleep infinity)
  docker exec "$volume_host_container" sh -c "mkdir -p /config/nes-systests"
  # Dereference symlinks via tar and pipe directly into the volume container
  tar -chf - -C "${NES_DIR}/nes-systests" . \
    | docker exec -i "$volume_host_container" tar -xf - -C /config/nes-systests
  docker stop -t0 "$volume_host_container"

  echo "# Using SYSTEST: $SYSTEST" >&3
  echo "# Using NES_DIR: $NES_DIR" >&3
  echo "# Using SYSTEST_IMAGE: $SYSTEST_IMAGE" >&3
  echo "# Using TESTCONFIG_VOLUME: $TESTCONFIG_VOLUME" >&3
  echo "# Using CONTAINER_WORKDIR: $CONTAINER_WORKDIR" >&3
}

teardown_file() {
  docker volume rm "$TESTCONFIG_VOLUME" || true
  docker rmi "$SYSTEST_IMAGE" || true
  echo "# Test suite completed" >&3
}

setup() {
  mkdir -p "$NES_TEST_TMP_DIR"
  export TMP_DIR=$(mktemp -d -p "$NES_TEST_TMP_DIR")
  cd "$TMP_DIR" || exit
  echo "# Using TEST_DIR: $TMP_DIR" >&3

  export TEST_VOLUME=$(docker volume create)
  local volume_host_container=$(docker run -d --rm -v "$TEST_VOLUME":/data alpine sleep infinity)
  docker stop -t0 "$volume_host_container"
  echo "# Using TEST_VOLUME: $TEST_VOLUME" >&3
}

teardown() {
  docker compose cp systest:$CONTAINER_WORKDIR/. . || true
  docker compose down -v || true
  docker volume rm "$TEST_VOLUME" || true
  [ -n "$MQTT_CONFIG_VOLUME" ] && docker volume rm "$MQTT_CONFIG_VOLUME" || true
}

function setup_mqtt() {
  # The mosquitto config and the producer payload have to be available to the broker/producer
  # containers. We cannot bind-mount as we are potentially running in a container using Docker out of
  # Docker, so we copy them into a volume.
  export MQTT_CONFIG_VOLUME=$(docker volume create)
  local volume_host=$(docker run -d --rm -v "$MQTT_CONFIG_VOLUME":/cfg alpine sleep infinity)
  tar -chf - -C "$NES_DIR/nes-systests/systest/test-container-test" mosquitto.conf mqtt-data.jsonl \
    | docker exec -i "$volume_host" tar -xf - -C /cfg
  docker stop -t0 "$volume_host"

  "$NES_DIR/nes-systests/systest/test-container-test/create_test_containers.sh" >docker-compose.yaml
  local compose_output exit_code=0
  compose_output=$(docker compose up -d --wait 2>&1) || exit_code=$?
  if [ "$exit_code" -ne 0 ]; then
    echo "# [docker compose up] (status=$exit_code):" >&3
    while IFS= read -r line; do echo "#   $line" >&3; done <<<"$compose_output"
  fi
  return $exit_code
}

DOCKER_SYSTEST() {
  # Forward ASAN_OPTIONS (CI sets detect_leaks=0 for sanitizer builds, like the rest of the test
  # suite) into the container, which would otherwise run the systest binary with leak detection on.
  docker compose exec -e ASAN_OPTIONS systest systest --workingDir "$CONTAINER_WORKDIR/systest-workdir" "$@" >&3
}

@test "mqtt source test" {
  setup_mqtt
  run DOCKER_SYSTEST --groups TestContainer
  [ "$status" -eq 0 ]
}
