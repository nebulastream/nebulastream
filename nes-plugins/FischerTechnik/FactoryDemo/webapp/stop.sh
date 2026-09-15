#!/usr/bin/env bash
# Stops the local MQTT broker started by start.sh. Doesn't touch NES/nebulastream
# -- just tears down this app's own broker container.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
docker compose down
