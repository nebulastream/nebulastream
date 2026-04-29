#!/usr/bin/env bash
# Run a selected test from nes-frontend/nebucli/tests/offline.bats with debug output.
# Offline tests don't require a worker — the cli is invoked directly on the host.
#
# Usage: scripts/run-bats-cli-offline.sh [test-name-regex]
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

BATS_FILE="$REPO_ROOT/nes-frontend/nebucli/tests/offline.bats"
NES_CLI="$REPO_ROOT/$BUILD_DIR/nes-frontend/nebucli/nes-cli"
NES_CLI_TESTDATA="$REPO_ROOT/nes-frontend/nebucli/tests"
NES_TEST_TMP_DIR="$REPO_ROOT/$BUILD_DIR/test-tmp"

echo ">>> Building nes-cli in $BUILD_DIR ..."
docker run --rm \
  --workdir /tmp/nebulastream \
  -v "$REPO_ROOT:/tmp/nebulastream" \
  "$DEV_IMAGE" \
  cmake --build "$BUILD_DIR" -j --target nes-cli

if [ ! -x "$NES_CLI" ]; then
  echo "ERROR: nes-cli not found or not executable at $NES_CLI" >&2
  exit 1
fi

mkdir -p "$NES_TEST_TMP_DIR"

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
  bats "${BATS_ARGS[@]}" "$BATS_FILE"
