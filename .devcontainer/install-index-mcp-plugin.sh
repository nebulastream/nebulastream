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

# Match the backend directory used by the systest plugin installer.
plugins_dir="${NES_CLION_PLUGINS_DIR:-${HOME}/.local/share/JetBrains/CLion${NES_CLION_VERSION:-2026.2}}"
config_dir="${NES_CLION_CONFIG_DIR:-${XDG_CONFIG_HOME:-${HOME}/.config}/JetBrains/CLion${NES_CLION_VERSION:-2026.2}}"
version=5.20.3
archive="jetbrains-index-mcp-plugin-${version}-signed.zip"
checksum=5a9aa89721817bf349e2d2f942af59c402feee9ab02f4fae2ad9361713fd1465

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

curl --fail --location --silent --show-error --retry 3 \
    "https://github.com/hechtcarmel/jetbrains-index-mcp-plugin/releases/download/${version}/${archive}" \
    --output "$tmp_dir/plugin.zip"
printf '%s  %s\n' "$checksum" "$tmp_dir/plugin.zip" | sha256sum --check --status
unzip -q "$tmp_dir/plugin.zip" -d "$tmp_dir/extracted"

# Replace the complete distribution so an upgrade cannot leave older JARs loaded.
mkdir -p "$plugins_dir"
rm -rf "$plugins_dir/jetbrains-index-mcp-plugin"
cp -a "$tmp_dir/extracted/jetbrains-index-mcp-plugin" "$plugins_dir/"
echo "Installed IDE Index MCP Server ${version} in $plugins_dir"

# Pin exposed tools for this plugin release without replacing other MCP preferences.
python3 - "$(dirname -- "${BASH_SOURCE[0]}")/mcp-plugin.xml" \
    "$config_dir/options/mcp-plugin.xml" "$tmp_dir/mcp-plugin.xml" <<'PY'
import sys
from pathlib import Path
import xml.etree.ElementTree as ET

template = ET.parse(sys.argv[1]).getroot().find("component")
settings_path = Path(sys.argv[2])
tree = ET.parse(settings_path) if settings_path.exists() else ET.ElementTree(ET.Element("application"))
component = tree.getroot().find("component[@name='McpPluginSettings']")
if component is None:
    component = ET.SubElement(tree.getroot(), "component", name="McpPluginSettings")

for option in template:
    existing = component.find(f"option[@name='{option.get('name')}']")
    if existing is not None:
        component.remove(existing)
    component.append(option)

ET.indent(tree, space="  ")
tree.write(sys.argv[3], encoding="unicode")
PY
install -D -m 644 "$tmp_dir/mcp-plugin.xml" "$config_dir/options/mcp-plugin.xml"
echo "Configured IDE Index MCP Server tools in $config_dir/options/mcp-plugin.xml"
