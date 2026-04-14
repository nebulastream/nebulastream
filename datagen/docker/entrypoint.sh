#!/usr/bin/env sh
set -eu

CONFIG_PATH="${DATAGEN_CONFIG:-/app/config/config.yaml}"
BINARY_PATH="${DATAGEN_BIN:-/usr/local/bin/datagen}"

if [ "$#" -gt 0 ]; then
    exec "$BINARY_PATH" "$@"
fi

exec "$BINARY_PATH" "$CONFIG_PATH"
