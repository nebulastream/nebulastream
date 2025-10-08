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

if [ ! -f "$NEBULASTREAM" ]; then
  echo "You have to build the nes-single-node-worker binary first for cmake target ${CMAKE_BUILD_DIR}"
  exit 1
fi

# Start building the compose file
cat <<EOF
services:
  nebucli-image:
    profiles: [ build-only ]
    image: nebucli-image
    pull_policy: never
    build:
      context: $(dirname $(realpath $NEBUCLI))
      dockerfile_inline: |
        FROM ubuntu:24.04 AS app
        ENV LLVM_TOOLCHAIN_VERSION=19
        RUN apt update -y && apt install curl wget gpg -y
        RUN curl -fsSL https://apt.llvm.org/llvm-snapshot.gpg.key | gpg --dearmor -o /etc/apt/keyrings/llvm-snapshot.gpg \
        && chmod a+r /etc/apt/keyrings/llvm-snapshot.gpg \
        && echo "deb [arch="\$\$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"\$(. /etc/os-release && echo "\$\$VERSION_CODENAME")"/ llvm-toolchain-"\$(. /etc/os-release && echo "\$\$VERSION_CODENAME")"-\$\${LLVM_TOOLCHAIN_VERSION} main" > /etc/apt/sources.list.d/llvm-snapshot.list \
        && echo "deb-src [arch="\$\$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"\$(. /etc/os-release && echo "\$\$VERSION_CODENAME")"/ llvm-toolchain-"\$(. /etc/os-release && echo "\$\$VERSION_CODENAME")"-\$\${LLVM_TOOLCHAIN_VERSION} main" >> /etc/apt/sources.list.d/llvm-snapshot.list \
        && apt update -y \
        && apt install -y libc++1-\$\${LLVM_TOOLCHAIN_VERSION} libc++abi1-\$\${LLVM_TOOLCHAIN_VERSION}

        COPY nebucli /usr/bin

  worker-image:
    profiles: [ build-only ]
    image: worker-image
    pull_policy: never
    build:
      context: $(dirname $(realpath $NEBULASTREAM))
      dockerfile_inline: |
        FROM ubuntu:24.04 AS app
        ENV LLVM_TOOLCHAIN_VERSION=19
        RUN apt update -y && apt install curl wget gpg -y
        RUN curl -fsSL https://apt.llvm.org/llvm-snapshot.gpg.key | gpg --dearmor -o /etc/apt/keyrings/llvm-snapshot.gpg \
        && chmod a+r /etc/apt/keyrings/llvm-snapshot.gpg \
        && echo "deb [arch="\$\$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"\$(. /etc/os-release && echo "\$\$VERSION_CODENAME")"/ llvm-toolchain-"\$(. /etc/os-release && echo "\$\$VERSION_CODENAME")"-\$\${LLVM_TOOLCHAIN_VERSION} main" > /etc/apt/sources.list.d/llvm-snapshot.list \
        && echo "deb-src [arch="\$\$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"\$(. /etc/os-release && echo "\$\$VERSION_CODENAME")"/ llvm-toolchain-"\$(. /etc/os-release && echo "\$\$VERSION_CODENAME")"-\$\${LLVM_TOOLCHAIN_VERSION} main" >> /etc/apt/sources.list.d/llvm-snapshot.list \
        && apt update -y \
        && apt install -y libc++1-\$\${LLVM_TOOLCHAIN_VERSION} libc++abi1-\$\${LLVM_TOOLCHAIN_VERSION}

        RUN GRPC_HEALTH_PROBE_VERSION=v0.4.40 && \
        wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/\$\${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 && \
        chmod +x /bin/grpc_health_probe

        COPY nes-single-node-worker /usr/bin
        ENTRYPOINT ["nes-single-node-worker"]

  nebucli:
    image: nebucli-image
    pull_policy: never
    command: ["sleep", "infinity"]
    working_dir: $(pwd)
    volumes:
      - $(pwd):$(pwd)
EOF

# Read workers and generate services
WORKER_COUNT=$(yq '.workers | length' "$WORKERS_FILE")

for i in $(seq 0 $((WORKER_COUNT - 1))); do
  # Extract worker data
  HOST=$(yq -r ".workers[$i].host" "$WORKERS_FILE")
  HOST_NAME=$(echo $HOST | cut -d':' -f1)
  GRPC=$(yq -r ".workers[$i].grpc" "$WORKERS_FILE")
  GRPC_PORT=$(echo $GRPC | cut -d':' -f2)

  # Generate service definition
  cat <<EOF
  $HOST_NAME:
    image: worker-image
    pull_policy: never
    healthcheck:
      test: ["CMD", "/bin/grpc_health_probe", "-addr=:$GRPC_PORT"]
      interval: 1s
      timeout: 5s
      retries: 3
      start_period: 1s
    command: [
      "--grpc=0.0.0.0:$GRPC_PORT",
      "--connection=$HOST",
    ]
    volumes:
      - $(pwd):$(pwd)
EOF

done
