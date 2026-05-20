#!/usr/bin/env bash
set -euo pipefail

# CLion's DevPod backend uses this user plugins directory (no /plugins suffix).
# Override the path for other backend layouts, or the version after an IDE upgrade.
plugins_dir="${NES_CLION_PLUGINS_DIR:-${HOME}/.local/share/JetBrains/CLion${NES_CLION_VERSION:-2026.2}}"
version=1.5
archive="NES-Systest-Runner-${version}-SNAPSHOT.zip"
checksum=71aeb8a3ee6ec2f3d0aa09bb443562635658a0ab6a4fbc8ffa481bafc251643f

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

curl --fail --location --silent --show-error --retry 3 \
    "https://github.com/nebulastream/systest-plugin/releases/download/v${version}/${archive}" \
    --output "$tmp_dir/plugin.zip"
printf '%s  %s\n' "$checksum" "$tmp_dir/plugin.zip" | sha256sum --check --status
unzip -q "$tmp_dir/plugin.zip" -d "$tmp_dir/extracted"

mkdir -p "$plugins_dir/NES-Systest-Runner"
cp -a "$tmp_dir/extracted/NES-Systest-Runner/." "$plugins_dir/NES-Systest-Runner/"
echo "Installed NES-Systest-Runner ${version} in $plugins_dir"
