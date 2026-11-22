#!/usr/bin/env bash

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
