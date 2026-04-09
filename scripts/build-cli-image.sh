#!/bin/bash

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

# Enable Docker BuildKit
export DOCKER_BUILDKIT=1

NES_DIR="$(git rev-parse --show-toplevel)"
RUNTIME_BASE_IMAGE="${1:-nebulastream/nes-runtime-base:local}"
CLI_IMAGE_TAG="${2:-nes-cli:local}"

# Try to find the CLI binary in common locations
CLI_BINARY=""
for path in \
    "${NES_DIR}/build/nes-cli/nes-cli" \
    "${NES_DIR}/build-docker/nes-cli/nes-cli" \
    "${NES_DIR}/cmake-build-debug-nes/nes-frontend/apps/cli/nes-cli" \
    "${NES_DIR}/cmake-build-debug/nes-frontend/apps/cli/nes-cli" \
    "/tmp/nebulastream-public/cmake-build-debug-nes/nes-frontend/apps/cli/nes-cli" \
    "/tmp/nebulastream-public/build/nes-cli/nes-cli"
do
    if [ -f "$path" ] && [ -x "$path" ]; then
        CLI_BINARY="$path"
        break
    fi
done

# If not found in standard locations, search for it
if [ -z "$CLI_BINARY" ]; then
    echo "Searching for nes-cli binary..." >&2
    CLI_BINARY=$(find /tmp/nebulastream-public "${NES_DIR}" -name "nes-cli" -type f -executable 2>/dev/null | head -1)
fi

if [ -z "$CLI_BINARY" ]; then
    echo "ERROR: CLI binary not found in any standard build location"
    echo ""
    echo "Checked:"
    echo "  - ${NES_DIR}/build/nes-cli/nes-cli"
    echo "  - ${NES_DIR}/build-docker/nes-cli/nes-cli"
    echo "  - ${NES_DIR}/cmake-build-debug-nes/nes-frontend/apps/cli/nes-cli"
    echo "  - ${NES_DIR}/cmake-build-debug/nes-frontend/apps/cli/nes-cli"
    echo "  - /tmp/nebulastream-public/cmake-build-debug-nes/nes-frontend/apps/cli/nes-cli"
    echo "  - /tmp/nebulastream-public/build/nes-cli/nes-cli"
    echo ""
    echo "Please build the CLI first using your preferred build method:"
    echo "  Option 1 (standard):"
    echo "    cmake -B build -S . -DCMAKE_BUILD_TYPE=RelWithDebInfo -DNES_ENABLES_TESTS=0"
    echo "    cmake --build build --target nes-cli -j"
    echo ""
    echo "  Option 2 (CLion):"
    echo "    Build the nes-cli target in your CLion IDE"
    echo ""
    exit 1
fi

echo "Building CLI Docker image with minimal context..."
echo "  CLI Binary: $CLI_BINARY"
echo "  Runtime Base Image: $RUNTIME_BASE_IMAGE"
echo "  Image Tag: $CLI_IMAGE_TAG"

# Create a minimal build context with only the CLI binary
cli_ctx=$(mktemp -d)
cp "$(realpath "$CLI_BINARY")" "$cli_ctx/nes-cli"

# Build using minimal context
docker build --load -t "$CLI_IMAGE_TAG" -f - "$cli_ctx" <<EOF
FROM ${RUNTIME_BASE_IMAGE}
VOLUME /state
ENV XDG_STATE_HOME=/state
COPY nes-cli /usr/bin
ENTRYPOINT ["nes-cli"]
EOF

# Cleanup
rm -rf "$cli_ctx"

echo "✓ Successfully built $CLI_IMAGE_TAG"
echo ""
echo "To push to a registry:"
echo "  docker tag $CLI_IMAGE_TAG <your-registry>/nes-cli:latest"
echo "  docker push <your-registry>/nes-cli:latest"






