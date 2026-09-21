#!/usr/bin/env bash
# Starts the local MQTT broker this app depends on, then opens the app.
# This is entirely separate from NES/nebulastream -- NES only ever acts as
# an MQTT client publishing into this broker (hbw-state.yaml's MQTTSink).
# Run hbw-state.yaml/.sql yourself, same as any other NES query.
#
# --python-baseline (-p) additionally starts python-baseline/infer.py in the
# background -- the independent, non-NES pipeline that publishes to the
# "Python baseline" dashboard panel. Optional because it needs its own venv
# and reaches out to the real factory broker (192.168.0.10), which not every
# machine running this script can necessarily reach.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

WITH_PYTHON_BASELINE=0
for arg in "$@"; do
  case "$arg" in
    --python-baseline|-p) WITH_PYTHON_BASELINE=1 ;;
    *) echo "Unknown option: $arg" >&2; exit 1 ;;
  esac
done

echo "Starting local MQTT broker..."
docker compose up -d

if [[ "$WITH_PYTHON_BASELINE" -eq 1 ]]; then
  echo "Starting Python baseline (python-baseline/infer.py)..."
  cd python-baseline
  if [[ ! -x .venv/bin/python ]]; then
    echo "  no venv found, creating one and installing requirements..."
    python3 -m venv .venv
    .venv/bin/pip install -q -r requirements.txt
  fi
  nohup .venv/bin/python infer.py >infer.log 2>&1 &
  echo $! >.infer.pid
  cd ..
  echo "  running (pid $(cat python-baseline/.infer.pid)), logs at python-baseline/infer.log"
fi

xdg-open "file://$(pwd)/index.html" >/dev/null 2>&1 &
