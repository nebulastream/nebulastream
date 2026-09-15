#!/usr/bin/env bash
# Starts the local MQTT broker this app depends on, then opens the app.
# This is entirely separate from NES/nebulastream -- NES only ever acts as
# an MQTT client publishing into this broker (hbw-state.yaml's MQTTSink).
# Run hbw-state.yaml/.sql yourself, same as any other NES query.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

echo "Starting local MQTT broker..."
docker compose up -d

xdg-open "file://$(pwd)/index.html" >/dev/null 2>&1 &
