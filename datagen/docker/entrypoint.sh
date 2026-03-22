#!/usr/bin/env sh
set -eu

CONFIG_PATH="${DATAGEN_CONFIG:-/app/config/config.yaml}"

if [ "$#" -gt 0 ]; then
    exec /usr/local/bin/datagen "$@"
fi

exec /usr/local/bin/datagen "$CONFIG_PATH"
