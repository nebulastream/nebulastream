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

  if [ -z "$NES_DIR" ]; then
    echo "ERROR: NES_DIR environment variable must be set" >&2
    echo "Usage: NES_DIR=/path/to/nebulastream" >&2
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

  # Print environment info for debugging
  echo "# Using SYSTEST: $SYSTEST" >&3
  echo "# Using NEBULASTREAM: $NEBULASTREAM" >&3
  echo "# Using NES_DIR: $NES_DIR" >&3
}

teardown_file() {
  # Clean up any global resources if needed
  echo "# Test suite completed" >&3
}
setup_file() {
  docker build -t worker-image -f - $(dirname $(realpath $NEBULASTREAM)) <<EOF
    FROM ubuntu:24.04 AS app
    ENV LLVM_TOOLCHAIN_VERSION=19
    RUN apt update -y && apt install curl wget gpg -y
    RUN curl -fsSL https://apt.llvm.org/llvm-snapshot.gpg.key | gpg --dearmor -o /etc/apt/keyrings/llvm-snapshot.gpg \
    && chmod a+r /etc/apt/keyrings/llvm-snapshot.gpg \
    && echo "deb [arch="\$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"/ llvm-toolchain-"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"-\${LLVM_TOOLCHAIN_VERSION} main" > /etc/apt/sources.list.d/llvm-snapshot.list \
    && echo "deb-src [arch="\$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"/ llvm-toolchain-"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"-\${LLVM_TOOLCHAIN_VERSION} main" >> /etc/apt/sources.list.d/llvm-snapshot.list \
    && apt update -y \
    && apt install -y libc++1-\${LLVM_TOOLCHAIN_VERSION} libc++abi1-\${LLVM_TOOLCHAIN_VERSION}

    RUN GRPC_HEALTH_PROBE_VERSION=v0.4.40 && \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/\${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-\$(dpkg --print-architecture) && \
    chmod +x /bin/grpc_health_probe

    COPY nes-single-node-worker /usr/bin
    ENTRYPOINT ["nes-single-node-worker"]
EOF
  docker build -t systest-image -f - $(dirname $(realpath $SYSTEST)) <<EOF
    FROM ubuntu:24.04 AS app
    ENV LLVM_TOOLCHAIN_VERSION=19
    RUN apt update -y && apt install curl wget gpg -y
    RUN curl -fsSL https://apt.llvm.org/llvm-snapshot.gpg.key | gpg --dearmor -o /etc/apt/keyrings/llvm-snapshot.gpg \
    && chmod a+r /etc/apt/keyrings/llvm-snapshot.gpg \
    && echo "deb [arch="\$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"/ llvm-toolchain-"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"-\${LLVM_TOOLCHAIN_VERSION} main" > /etc/apt/sources.list.d/llvm-snapshot.list \
    && echo "deb-src [arch="\$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"/ llvm-toolchain-"\$(. /etc/os-release && echo "\$VERSION_CODENAME")"-\${LLVM_TOOLCHAIN_VERSION} main" >> /etc/apt/sources.list.d/llvm-snapshot.list \
    && apt update -y \
    && apt install -y libc++1-\${LLVM_TOOLCHAIN_VERSION} libc++abi1-\${LLVM_TOOLCHAIN_VERSION}
    RUN curl -sSL -O https://packages.microsoft.com/config/ubuntu/24.04/packages-microsoft-prod.deb && \
                dpkg -i packages-microsoft-prod.deb && \
                rm packages-microsoft-prod.deb && \
                apt-get update && \
                ACCEPT_EULA=Y apt-get install -y msodbcsql18

    COPY systest /usr/bin
EOF
}

INSTANCE_PID=0
setup() {
  export TMP_DIR=$(mktemp -d)

  cd "$TMP_DIR" || exit

  echo "Using TEST_DIR: $TMP_DIR" >&3
}

teardown() {
  docker compose down -v 2>/dev/null || true
}

function setup_distributed() {
  # setup the MQTT container
  cat $NES_DIR/nes-systests/systest/test-container-test/mosquitto.conf > mosquitto.conf
  cat $NES_DIR/nes-systests/systest/test-container-test/mqtt-data.jsonl > mqtt-data.jsonl

  # setup MSSQL Server container
  cat $NES_DIR/nes-systests/systest/test-container-test/init-db.sql > init-db.sql

  $NES_DIR/nes-systests/systest/test-container-test/create_test_containers.sh > docker-compose.yaml
  echo "Running docker compose up" >&3
  docker compose up -d --wait 2>/dev/null
}

DOCKER_SYSTEST() {
  echo "Starting: $BATS_TEST_NAME" >&3
  docker compose run -q --rm systest systest --workingDir $(pwd)/workdir "$@" 2>&3
}

@test "test container tests" {
  setup_distributed
  run DOCKER_SYSTEST --groups TestContainer

    [ "$status" -eq 0 ]
}
