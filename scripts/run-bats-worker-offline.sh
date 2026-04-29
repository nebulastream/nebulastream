#!/usr/bin/env bash
# Run a selected test from nes-single-node-worker/tests/offline.bats with debug output.
# Worker offline tests don't require a coordinator — the worker is invoked directly on the host.
#
# Usage: scripts/run-bats-worker-offline.sh [test-name-regex]
#   Omitted regex runs the entire offline.bats file.
#
# Env vars:
#   BUILD_DIR  build directory (default: cmake-build-debug-bats)
#   DEV_IMAGE  dev container image (default: nebulastream/nes-development:local)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-cmake-build-debug-bats}"
DEV_IMAGE="${DEV_IMAGE:-nebulastream/nes-development:local}"

FILTER="${1:-}"

BATS_FILE="$REPO_ROOT/nes-single-node-worker/tests/offline.bats"
NES_WORKER="$REPO_ROOT/$BUILD_DIR/nes-single-node-worker/nes-single-node-worker"
NES_WORKER_TESTDATA="$REPO_ROOT/nes-single-node-worker/tests"

echo ">>> Building nes-single-node-worker in $BUILD_DIR ..."
docker run --rm \
  --workdir /tmp/nebulastream \
  -v "$REPO_ROOT:/tmp/nebulastream" \
  "$DEV_IMAGE" \
  cmake --build "$BUILD_DIR" -j --target nes-single-node-worker

if [ ! -x "$NES_WORKER" ]; then
  echo "ERROR: nes-single-node-worker not found or not executable at $NES_WORKER" >&2
  exit 1
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
  NES_WORKER="$NES_WORKER" \
  NES_WORKER_TESTDATA="$NES_WORKER_TESTDATA" \
  bats "${BATS_ARGS[@]}" "$BATS_FILE"
