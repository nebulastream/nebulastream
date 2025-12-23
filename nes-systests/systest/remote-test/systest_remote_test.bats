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

    COPY systest /usr/bin
EOF
}

INSTANCE_PID=0
setup() {
  export TMP_DIR=$(mktemp -d)

  cd "$TMP_DIR" || exit

  echo "# Using TEST_DIR: $TMP_DIR" >&3
}

teardown() {
  docker compose down -v || true
}

limit_container_bandwidth() {
  local svc="$1"
  local veth="$2"
  local rate="$3"

  # Clean existing qdisc if present
  sudo tc qdisc del dev "$veth" root 2>/dev/null || true

  # HTB root
  sudo tc qdisc add dev "$veth" root handle 1: htb default 10

  # Bandwidth class
  sudo tc class add dev "$veth" parent 1: classid 1:10 htb \
    rate "$rate" ceil "$rate"

  echo "#   tc: $svc limited to $rate on $veth" >&3
}

# Help function that displays the veths for each container to enable sending rate tracking via btop
print_container_veths() {
  local RATE="$1"
  echo "# Container → veth mapping" >&3

  for svc in $(docker compose ps --services); do
    CID=$(docker compose ps -q "$svc")
    [ -z "$CID" ] && continue

    PID=$(docker inspect -f '{{.State.Pid}}' "$CID")
    [ "$PID" = "0" ] && continue

    # Enter netns and extract ifindex after @if
    IFIDX=$(sudo nsenter -t "$PID" -n ip -o link \
      | awk -F'@if' '/eth0/ {print $2}' | cut -d':' -f1)

    if [ -z "$IFIDX" ]; then
      echo "#   $svc → <no ifindex>" >&3
      continue
    fi

    VETH=$(ip -o link \
      | awk -F': ' -v ifidx="$IFIDX" '$1 == ifidx {print $2}' \
      | cut -d'@' -f1)

    if [ -z "$VETH" ]; then
      echo "#   $svc → <no veth>" >&3
      continue
    fi

    echo "#   $svc → $VETH" >&3
    if [ "$svc" != "systest" ]; then
      limit_container_bandwidth "$svc" "$VETH" "$RATE"
    fi
  done
}

function setup_distributed() {
  $NES_DIR/nes-systests/systest/remote-test/create_compose.sh "$1" >docker-compose.yaml
  docker compose up -d --wait
  print_container_veths "$2"
}

DOCKER_SYSTEST() {
  docker compose run --rm systest systest --workingDir $(pwd)/workdir "$@"
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
@test "2 node systest" {
  setup_distributed $NES_DIR/nes-systests/configs/topologies/two-node.yaml "5mbit"
  run DOCKER_SYSTEST -t "nes-systests/benchmark/ClusterMonitoring.test:1" --clusterConfig nes-systests/configs/topologies/two-node.yaml --remote -- 
  [ "$status" -eq 0 ]
}
    
@test "my cool test" {
    setup_distributed $NES_DIR/nes-systests/configs/topologies/two-node.yaml "50mbits"
    run DOCKER_SYSTEST -e LARGE source --clusterConfig nes-systests/configs/topologies/two-node.yaml --remote 
    [ "$status" -eq 0 ]
}
