#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
    -s "$script_dir/skills/nes-notifications" -p 'test_*.py' -v
node --test "$script_dir"/skills/nes-notifications/*.test.mjs
bash -n "$script_dir/install-pi-skills.sh" "$script_dir/setup-pi.sh"
