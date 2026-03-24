#!/usr/bin/env bash
# Reproduces the config-override warning from Fix(#152).
#
# This script creates a minimal YAML config file, then starts the single-node-worker
# with both --configPath and a CLI flag that overrides a YAML value. The log output
# should contain:
#   "Configuration '...' was already set (e.g. via YAML config file) and is now being
#    overridden by a command-line argument."
#
# Usage:
#   # From the repo root, with the worker binary built:
#   bash scripts/reproduce-config-override-warning.sh
#
#   # Or inside Docker:
#   docker run --rm --workdir /src -v "$(pwd)":/src nebulastream/nes-development:local \
#     bash scripts/reproduce-config-override-warning.sh

set -e

WORKER_BIN="${1:-cmake-build-docker/nes-single-node-worker/nes-single-node-worker}"
TMPDIR=$(mktemp -d)
YAML_CONFIG="$TMPDIR/test-config.yaml"
LOG_FILE="$TMPDIR/worker.log"

trap 'rm -rf "$TMPDIR"' EXIT

if [ ! -x "$WORKER_BIN" ]; then
    echo "Error: Worker binary not found at $WORKER_BIN"
    echo "Build it first:  cmake --build cmake-build-docker -j --target nes-single-node-worker"
    echo "Or pass the path: $0 <path-to-worker-binary>"
    exit 1
fi

# Create a YAML config that explicitly sets the number of worker threads.
cat > "$YAML_CONFIG" <<'YAML'
worker:
  query_engine:
    number_of_worker_threads: 2
YAML

echo "=== Config file ($YAML_CONFIG) ==="
cat "$YAML_CONFIG"
echo ""

echo "=== Running worker with YAML config AND CLI override ==="
echo "Command: $WORKER_BIN --configPath=$YAML_CONFIG --worker.query_engine.number_of_worker_threads=8"
echo ""

# Run the worker briefly — it will start up, log the warning, then we kill it.
# We redirect stderr+stdout to the log and give it a few seconds to initialize.
timeout 5 "$WORKER_BIN" \
    "--configPath=$YAML_CONFIG" \
    "--worker.query_engine.number_of_worker_threads=8" \
    > "$LOG_FILE" 2>&1 || true

echo "=== Checking log for override warning ==="
if grep -q "was already set.*overridden by a command-line argument" "$LOG_FILE"; then
    echo "SUCCESS: Override warning found in log:"
    grep "was already set.*overridden by a command-line argument" "$LOG_FILE"
else
    echo "WARNING: Override warning not found in log output."
    echo "Log contents (last 20 lines):"
    tail -20 "$LOG_FILE"
    exit 1
fi
