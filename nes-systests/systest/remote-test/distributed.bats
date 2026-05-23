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

source "$NES_BATS_LIB"

setup_file() {
  nes_distributed_setup_file systest
  nes_require_env NES_DIR DATADIR

  export CONTAINER_WORKDIR="/$(cat /proc/sys/kernel/random/uuid)"
  # Will contain the test data
  export TESTDATA_VOLUME=$(docker volume create)

  # Will contain the tests itself, as well as the configuration files
  export TESTCONFIG_VOLUME=$(docker volume create)

  # Prepare Volumes with Testdata. We cannot mount as we are potentially running in a container using Docker out of Docker.
  # Systest is a little picky about where it finds the testdata. We can freely place input data anywhere and use the ---data flag
  # Systest discovery and configuration are hard coded and look in the $NES_DIR/nes-systests directory. We have to replicate this in docker compose
  #
  # Volume structure:
  #   TESTCONFIG_VOLUME: contains /nes-systests/* and will be mounted at $NES_DIR in containers
  #                      This reconstructs the expected path $NES_DIR/nes-systests/* inside containers
  #   TESTDATA_VOLUME:   contains test input data, mounted at /data in containers

  volume_host_container=$(docker run -d --rm -v $TESTCONFIG_VOLUME:/config -v $TESTDATA_VOLUME:/data alpine sleep infinite)
  docker exec $volume_host_container sh -c "mkdir -p /config/nes-systests"
  # Dereference symlinks via tar and pipe directly into the volume container
  tar -chf - -C "${DATADIR}" . \
    | docker exec -i $volume_host_container tar -xf - -C /data
  tar -chf - -C "${NES_DIR}/nes-systests" . \
    | docker exec -i $volume_host_container tar -xf - -C /config/nes-systests
  docker stop -t0 $volume_host_container

  echo "# Using NES_DIR: $NES_DIR" >&3
  echo "# Using WORKER_IMAGE: $WORKER_IMAGE" >&3
  echo "# Using SYSTEST_IMAGE: $SYSTEST_IMAGE" >&3
  echo "# Using TESTDATA_VOLUME: $TESTDATA_VOLUME" >&3
  echo "# Using TESTCONFIG_VOLUME: $TESTCONFIG_VOLUME" >&3
  echo "# Using CONTAINER_WORKDIR: $CONTAINER_WORKDIR" >&3
}

teardown_file() {
  docker volume rm $TESTDATA_VOLUME || true
  docker volume rm $TESTCONFIG_VOLUME || true
  nes_distributed_teardown_file
}

INSTANCE_PID=0
setup() {
  # Deterministic per-test working dir (see distributed_bats_lib.bash for the
  # rationale — coverage harvest relies on stable paths across reruns).
  mkdir -p "$NES_TEST_TMP_DIR"
  export TMP_DIR="$NES_TEST_TMP_DIR/${NES_CTEST_NAME}/${BATS_TEST_NAME}"
  rm -rf "$TMP_DIR"
  mkdir -p "$TMP_DIR"

  # Copy NES_DIR contents to temp directory for DOOD compatibility
  cd "$TMP_DIR" || exit

  echo "# Using TEST_DIR: $TMP_DIR" >&3

  volume=$(docker volume create)
  volume_host_container=$(docker run -d --rm -v $volume:/data alpine sleep infinite)
  docker stop -t0 $volume_host_container
  export TEST_VOLUME=$volume
  echo "Using test volume: $TEST_VOLUME" >&3
}

teardown() {
  docker compose cp systest:$CONTAINER_WORKDIR/. . || true
  docker compose down -v || true
  docker volume rm $TEST_VOLUME || true
}

docker_systest() {
  docker compose exec systest systest --log-path $CONTAINER_WORKDIR/systest.log --data /data  --workingDir $CONTAINER_WORKDIR/systest-workdir "$@" >&3
}

# Inference systests load ONNX models via the IREE toolchain. When the IREE tools are not
# available in the worker image (ENABLE_IREE_TESTS=OFF), exclude the Inference group too.
EXTRA_EXCLUDE_GROUPS=()
if [ "$ENABLE_IREE_TESTS" != "ON" ]; then
  EXTRA_EXCLUDE_GROUPS+=(Inference)
fi

@test "two node systest" {
  setup_distributed systest $NES_DIR/nes-systests/configs/topologies/two-node-with-interpreter.yaml
  run docker_systest -e large tcp "${EXTRA_EXCLUDE_GROUPS[@]}" --clusterConfig $NES_DIR/nes-systests/configs/topologies/two-node-with-interpreter.yaml --remote
  [ "$status" -eq 0 ]
}

@test "8 node systest" {
  setup_distributed systest $NES_DIR/nes-systests/configs/topologies/8-node.yaml
  run docker_systest -e large tcp "${EXTRA_EXCLUDE_GROUPS[@]}" --clusterConfig $NES_DIR/nes-systests/configs/topologies/8-node.yaml --remote
  [ "$status" -eq 0 ]
}

@test "large scale tests on two nodes" {
  if [ "$ENABLE_LARGE_TESTS" != "ON" ]; then
    skip "Large tests disabled (ENABLE_LARGE_TESTS=$ENABLE_LARGE_TESTS)"
  fi
  setup_distributed systest $NES_DIR/nes-systests/configs/topologies/two-node-more-capacity.yaml
  run docker_systest -g large -e tcp "${EXTRA_EXCLUDE_GROUPS[@]}" --clusterConfig $NES_DIR/nes-systests/configs/topologies/two-node.yaml --remote
  [ "$status" -eq 0 ]
}
