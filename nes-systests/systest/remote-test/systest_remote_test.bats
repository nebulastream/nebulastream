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


# Convert cmake boolean to bash boolean
to_bool() {
    case "${1^^}" in
        1|ON|YES|TRUE|Y) echo "true" ;;
        *) echo "false" ;;
    esac
}

setup_file() {
  # Validate SYSTEST environment variable once for all tests
  if [ -z "$SYSTEST" ]; then
    echo "ERROR: SYSTEST environment variable must be set" >&2
    echo "Usage: SYSTEST=/path/to/nebucli bats nebucli.bats" >&2
    exit 1
  fi

  if [ -z "$NEBULASTREAM" ]; then
    echo "ERROR: NEBULASTREAM environment variable must be set" >&2
    echo "Usage: NEBULASTREAM=/path/to/nes-single-node-worker bats nebucli.bats" >&2
    exit 1
  fi

  if [ -z "$ENABLE_LARGE_TESTS" ]; then
    echo "ERROR: ENABLE_LARGE_TESTS environment variable must be set" >&2
    echo "Usage: ENABLE_LARGE_TESTS=TRUE" >&2
    exit 1
  fi

  if [ -z "$NES_DIR" ]; then
    echo "ERROR: NES_DIR environment variable must be set" >&2
    echo "Usage: NES_DIR=/path/to/nebulastream" >&2
    exit 1
  fi

  if [ -z "$NES_TEST_TMP_DIR" ]; then
    echo "ERROR: NES_TEST_TMP_DIR environment variable must be set" >&2
    echo "Usage: NES_TEST_TMP_DIR=/path/to/build/test-tmp" >&2
    exit 1
  fi

  if [ -z "$DATADIR" ]; then
    echo "ERROR: DATADIR environment variable must be set" >&2
    echo "Usage: DATADIR=/path/to/testdata bats systest_remote_test.bats" >&2
    exit 1
  fi

  if [ ! -f "$SYSTEST" ]; then
    echo "ERROR: SYSTEST file does not exist: $SYSTEST" >&2
    exit 1
  fi

  if [ ! -f "$NEBULASTREAM" ]; then
    echo "ERROR: NEBULASTREAM file does not exist: $NEBULASTREAM" >&2
    exit 1
  fi

  if [ ! -x "$SYSTEST" ]; then
    echo "ERROR: SYSTEST file is not executable: $SYSTEST" >&2
    exit 1
  fi

  if [ ! -x "$NEBULASTREAM" ]; then
    echo "ERROR: NEBULASTREAM file is not executable: $NEBULASTREAM" >&2
    exit 1
  fi

  export WORKER_IMAGE=$(docker build -q -f - $(dirname $(realpath $NEBULASTREAM)) <<EOF
    FROM ubuntu:24.04 AS app
    ENV LLVM_TOOLCHAIN_VERSION=19
    RUN apt update -y && apt install curl wget gpg -y
    RUN curl -fsSL https://apt.llvm.org/llvm-snapshot.gpg.key | gpg --dearmor -o /etc/apt/keyrings/llvm-snapshot.gpg \
    && chmod a+r /etc/apt/keyrings/llvm-snapshot.gpg \
    && echo "deb [arch="\$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"/ llvm-toolchain-"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"-\${LLVM_TOOLCHAIN_VERSION} main" > /etc/apt/sources.list.d/llvm-snapshot.list \
    && echo "deb-src [arch="\$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"/ llvm-toolchain-"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"-\${LLVM_TOOLCHAIN_VERSION} main" >> /etc/apt/sources.list.d/llvm-snapshot.list \
    && apt update -y \
    && apt install -y libc++1-\${LLVM_TOOLCHAIN_VERSION} libc++abi1-\${LLVM_TOOLCHAIN_VERSION} tree

    RUN GRPC_HEALTH_PROBE_VERSION=v0.4.40 && \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/\${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-\$(dpkg --print-architecture) && \
    chmod +x /bin/grpc_health_probe

    COPY nes-single-node-worker /usr/bin
    ENTRYPOINT ["nes-single-node-worker"]
EOF
)
  export SYSTEST_IMAGE=$(docker build -q -f - $(dirname $(realpath $SYSTEST)) <<EOF
    FROM ubuntu:24.04 AS app
    ENV LLVM_TOOLCHAIN_VERSION=19
    RUN apt update -y && apt install curl wget gpg -y
    RUN curl -fsSL https://apt.llvm.org/llvm-snapshot.gpg.key | gpg --dearmor -o /etc/apt/keyrings/llvm-snapshot.gpg \
    && chmod a+r /etc/apt/keyrings/llvm-snapshot.gpg \
    && echo "deb [arch="\$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"/ llvm-toolchain-"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"-\${LLVM_TOOLCHAIN_VERSION} main" > /etc/apt/sources.list.d/llvm-snapshot.list \
    && echo "deb-src [arch="\$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"/ llvm-toolchain-"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"-\${LLVM_TOOLCHAIN_VERSION} main" >> /etc/apt/sources.list.d/llvm-snapshot.list \
    && apt update -y \
    && apt install -y libc++1-\${LLVM_TOOLCHAIN_VERSION} libc++abi1-\${LLVM_TOOLCHAIN_VERSION} tree

    COPY systest /usr/bin
EOF
)
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



  if [ "$(to_bool "${ENABLE_LARGE_TESTS:-OFF}")" = "true" ]; then
    export NES_EXCLUDE_TEST_GROUPS="-e tcp"
  else
    export NES_EXCLUDE_TEST_GROUPS="-e tcp large"
  fi

  echo "# Using SYSTEST: $SYSTEST" >&3
  echo "# Using NEBULASTREAM: $NEBULASTREAM" >&3
  echo "# Using NES_DIR: $NES_DIR" >&3
  echo "# Using WORKER_IMAGE: $WORKER_IMAGE" >&3
  echo "# Using SYSTEST_IMAGE: $SYSTEST_IMAGE" >&3
  echo "# Using TESTDATA_VOLUME: $TESTDATA_VOLUME" >&3
  echo "# Using TESTCONFIG_VOLUME: $TESTCONFIG_VOLUME" >&3
  echo "# Using CONTAINER_WORKDIR: $CONTAINER_WORKDIR" >&3
  echo "# Excluding test groups: $NES_EXCLUDE_TEST_GROUPS" >&3
}

teardown_file() {
  # Clean up any global resources if needed
  echo "# Test suite completed" >&3

  docker volume rm $TESTDATA_VOLUME || true
  docker volume rm $TESTCONFIG_VOLUME || true
  docker rmi $WORKER_IMAGE || true
  docker rmi $SYSTEST_IMAGE || true
}

INSTANCE_PID=0
setup() {
  mkdir -p "$NES_TEST_TMP_DIR"
  export TMP_DIR=$(mktemp -d -p "$NES_TEST_TMP_DIR")

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
  docker compose cp systest:/ . || true
  docker compose down -v || true
  docker volume rm $TEST_VOLUME || true
}

function setup_distributed() {
  $NES_DIR/nes-systests/systest/remote-test/create_compose.sh "$1" >docker-compose.yaml
  docker compose up -d --wait
}

DOCKER_SYSTEST() {
  docker compose exec systest systest --log-path $CONTAINER_WORKDIR/systest.log --data /data  --workingDir $CONTAINER_WORKDIR/systest-workdir "$@" >&3
}

@test "two node systest" {
  setup_distributed $NES_DIR/nes-systests/configs/topologies/two-node.yaml
  run DOCKER_SYSTEST ${NES_EXCLUDE_TEST_GROUPS} --clusterConfig $NES_DIR/nes-systests/configs/topologies/two-node.yaml --remote
  [ "$status" -eq 0 ]
}

@test "8 node systest" {
  setup_distributed $NES_DIR/nes-systests/configs/topologies/8-node.yaml
  run DOCKER_SYSTEST ${NES_EXCLUDE_TEST_GROUPS} --clusterConfig $NES_DIR/nes-systests/configs/topologies/8-node.yaml --remote
  [ "$status" -eq 0 ]
}
