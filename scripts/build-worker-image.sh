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
WORKER_IMAGE_TAG="${2:-nes-worker:local}"

# Try to find the worker binary in common locations
WORKER_BINARY=""
for path in \
    "${NES_DIR}/build/nes-single-node-worker/nes-single-node-worker" \
    "${NES_DIR}/build-docker/nes-single-node-worker/nes-single-node-worker" \
    "${NES_DIR}/cmake-build-debug-nes/nes-single-node-worker/nes-single-node-worker" \
    "${NES_DIR}/cmake-build-debug/nes-single-node-worker/nes-single-node-worker" \
    "/tmp/nebulastream-public/cmake-build-debug-nes/nes-single-node-worker/nes-single-node-worker" \
    "/tmp/nebulastream-public/build/nes-single-node-worker/nes-single-node-worker"
do
    if [ -f "$path" ] && [ -x "$path" ]; then
        WORKER_BINARY="$path"
        break
    fi
done

# If not found in standard locations, search for it
if [ -z "$WORKER_BINARY" ]; then
    echo "Searching for nes-single-node-worker binary..." >&2
    WORKER_BINARY=$(find /tmp/nebulastream-public "${NES_DIR}" -name "nes-single-node-worker" -type f -executable 2>/dev/null | head -1)
fi

if [ -z "$WORKER_BINARY" ]; then
    echo "ERROR: Worker binary not found in any standard build location"
    echo ""
    echo "Checked:"
    echo "  - ${NES_DIR}/build/nes-single-node-worker/nes-single-node-worker"
    echo "  - ${NES_DIR}/build-docker/nes-single-node-worker/nes-single-node-worker"
    echo "  - ${NES_DIR}/cmake-build-debug-nes/nes-single-node-worker/nes-single-node-worker"
    echo "  - ${NES_DIR}/cmake-build-debug/nes-single-node-worker/nes-single-node-worker"
    echo "  - /tmp/nebulastream-public/cmake-build-debug-nes/nes-single-node-worker/nes-single-node-worker"
    echo "  - /tmp/nebulastream-public/build/nes-single-node-worker/nes-single-node-worker"
    echo ""
    echo "Please build the worker first using your preferred build method:"
    echo "  Option 1 (standard):"
    echo "    cmake -B build -S . -DCMAKE_BUILD_TYPE=RelWithDebInfo -DNES_ENABLES_TESTS=0"
    echo "    cmake --build build --target nes-single-node-worker -j"
    echo ""
    echo "  Option 2 (CLion):"
    echo "    Build the nes-single-node-worker target in your CLion IDE"
    echo ""
    exit 1
fi

echo "Building worker Docker image with minimal context..."
echo "  Worker Binary: $WORKER_BINARY"
echo "  Runtime Base Image: $RUNTIME_BASE_IMAGE"
echo "  Image Tag: $WORKER_IMAGE_TAG"

# Create a minimal build context with only the worker binary
worker_ctx=$(mktemp -d)
cp "$(realpath "$WORKER_BINARY")" "$worker_ctx/nes-single-node-worker"

# Build using minimal context
docker build --load -t "$WORKER_IMAGE_TAG" -f - "$worker_ctx" <<EOF
FROM ${RUNTIME_BASE_IMAGE}
COPY nes-single-node-worker /usr/bin
ENTRYPOINT ["nes-single-node-worker"]
EOF

# Cleanup
rm -rf "$worker_ctx"

echo "✓ Successfully built $WORKER_IMAGE_TAG"
echo ""
echo "To push to a registry:"
echo "  docker tag $WORKER_IMAGE_TAG <your-registry>/nes-worker:latest"
echo "  docker push <your-registry>/nes-worker:latest"

