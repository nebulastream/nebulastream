#!/usr/bin/env bash
# Run a selected test from nes-frontend/nebucli/tests/distributed.bats with debug output.
#
# Usage: scripts/run-bats-cli-test.sh [test-name-regex]
#   Omitted regex runs the entire distributed.bats file.
#
# Env vars:
#   BUILD_DIR              build directory (default: cmake-build-debug-bats)
#   DEV_IMAGE              dev container image (default: nebulastream/nes-development:local)
#   NES_RUNTIME_BASE_IMAGE runtime base image used by the bats tests
#                          (default: nes-runtime-base:test)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-cmake-build-debug-bats}"
DEV_IMAGE="${DEV_IMAGE:-nebulastream/nes-development:local}"
NES_RUNTIME_BASE_IMAGE="${NES_RUNTIME_BASE_IMAGE:-nes-runtime-base:test}"

FILTER="${1:-}"

BATS_FILE="$REPO_ROOT/nes-frontend/nebucli/tests/distributed.bats"
NES_CLI="$REPO_ROOT/$BUILD_DIR/nes-frontend/nebucli/nes-cli"
NEBULASTREAM="$REPO_ROOT/$BUILD_DIR/nes-single-node-worker/nes-single-node-worker"
NES_CLI_TESTDATA="$REPO_ROOT/nes-frontend/nebucli/tests"
NES_TEST_TMP_DIR="$REPO_ROOT/$BUILD_DIR/test-tmp"

echo ">>> Building nes-cli and nes-single-node-worker in $BUILD_DIR ..."
# The CMake cache in cmake-build-debug-bats is pinned to /tmp/nebulastream
# (its original configure path), so mount the repo there inside the container.
docker run --rm \
  --workdir /tmp/nebulastream \
  -v "$REPO_ROOT:/tmp/nebulastream" \
  "$DEV_IMAGE" \
  cmake --build "$BUILD_DIR" -j --target nes-cli nes-single-node-worker

if [ ! -x "$NES_CLI" ]; then
  echo "ERROR: nes-cli not found or not executable at $NES_CLI" >&2
  exit 1
fi
if [ ! -x "$NEBULASTREAM" ]; then
  echo "ERROR: nes-single-node-worker not found or not executable at $NEBULASTREAM" >&2
  exit 1
fi

mkdir -p "$NES_TEST_TMP_DIR"

echo ">>> Ensuring runtime base image $NES_RUNTIME_BASE_IMAGE exists ..."
if ! docker image inspect "$NES_RUNTIME_BASE_IMAGE" >/dev/null 2>&1; then
  docker build --load -t "$NES_RUNTIME_BASE_IMAGE" \
    -f "$REPO_ROOT/docker/runtime/RuntimeBase.dockerfile" \
    "$REPO_ROOT/docker/runtime"
fi

BATS_ARGS=(
  --verbose-run
  --timing
  --trace
  --print-output-on-failure
  --show-output-of-passing-tests
  --no-tempdir-cleanup
)
if [ -n "$FILTER" ]; then
  BATS_ARGS+=(-f "$FILTER")
fi

echo ">>> Running bats ${BATS_ARGS[*]} $BATS_FILE"
env \
  NES_CLI="$NES_CLI" \
  NES_CLI_TESTDATA="$NES_CLI_TESTDATA" \
  NES_TEST_TMP_DIR="$NES_TEST_TMP_DIR" \
  NEBULASTREAM="$NEBULASTREAM" \
  NES_RUNTIME_BASE_IMAGE="$NES_RUNTIME_BASE_IMAGE" \
  bats "${BATS_ARGS[@]}" "$BATS_FILE"
