#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
agent_dir="${PI_CODING_AGENT_DIR:-$HOME/.pi/agent}"
destination="$agent_dir/skills/nes-notifications"
mkdir -p "$destination"
for file in SKILL.md relay.py; do
    install -m 644 "$script_dir/skills/nes-notifications/$file" "$destination/$file"
done
printf 'Installed NES notification skill in %s\n' "$destination"
