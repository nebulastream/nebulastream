#!/bin/bash
set -e

PROFILE="/etc/profile.d/nes-vcpkg-cache.sh"
ENV_FILE="/etc/environment"

write_vcpkg_env() {
    local key="$1"
    local value="$2"
    # Write to profile.d for login shells
    echo "export ${key}=\"${value}\"" >> "$PROFILE"
    # Write to /etc/environment for all processes (PAM, IDEs, cmake)
    # Remove any existing entry for this key first, then append
    sed -i "/^${key}=/d" "$ENV_FILE"
    echo "${key}=${value}" >> "$ENV_FILE"
}

# Clear profile for a fresh write
cat /dev/null > "$PROFILE"

# If R2 secrets are provided, configure vcpkg binary caching via S3-compatible API
if [[ -n "$R2_ACCOUNT_ID" && -n "$R2_KEY" && -n "$R2_SECRET" ]]; then
    write_vcpkg_env "AWS_ACCESS_KEY_ID"        "$R2_KEY"
    write_vcpkg_env "AWS_SECRET_ACCESS_KEY"    "$R2_SECRET"
    write_vcpkg_env "AWS_ENDPOINT_URL_S3"      "https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
    write_vcpkg_env "AWS_REGION"               "auto"
    write_vcpkg_env "VCPKG_BINARY_SOURCES"     "clear;x-aws,s3://my-vcpkg-cache/,readwrite"
    echo "[nes-entrypoint] R2 binary cache configured (readwrite)"
else
    write_vcpkg_env "VCPKG_BINARY_SOURCES" "clear;http,https://pub-162f068cdd4a45d192dedb5f886cd825.r2.dev/{sha}.zip,read"
    echo "[nes-entrypoint] No R2 secrets found, using public read-only binary cache"
fi

# If called with arguments (docker ENTRYPOINT mode), exec them
if [ $# -gt 0 ]; then
    # Source the profile so the current process also gets the env vars
    [ -f "$PROFILE" ] && . "$PROFILE"
    exec "$@"
fi
