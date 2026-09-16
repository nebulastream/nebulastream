#!/usr/bin/env bash
# Stops the local MQTT broker started by start.sh, and the Python baseline
# process too if start.sh was run with --python-baseline. Doesn't touch
# NES/nebulastream -- just this app's own broker container and, if present,
# the python-baseline process.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

if [[ -f python-baseline/.infer.pid ]]; then
  pid="$(cat python-baseline/.infer.pid)"
  if kill "$pid" 2>/dev/null; then
    echo "Stopped Python baseline (pid $pid)."
  fi
  rm -f python-baseline/.infer.pid
fi

docker compose down
