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

# Reviewed revision: keep the full commit hash, not a moving branch/tag.
readonly source='git:https://github.com/awoaCrim/pi-openai-toolkit.git@37787051aa6e566dad0c028d49e20ded3ec72f25'

if ! command -v pi >/dev/null 2>&1; then
    echo 'Pi CLI is required to install pi-openai-toolkit.' >&2
    exit 1
fi

pi install "$source"

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
checkout="${PI_CODING_AGENT_DIR:-$HOME/.pi/agent}/git/github.com/awoaCrim/pi-openai-toolkit"
patch="$script_dir/patches/pi-openai-toolkit-hide-fast-status.patch"
if [[ "$(git -C "$checkout" rev-parse HEAD)" != '37787051aa6e566dad0c028d49e20ded3ec72f25' ]]; then
    echo 'Refusing to patch an unexpected pi-openai-toolkit revision.' >&2
    exit 1
fi
if git -C "$checkout" apply --reverse --check "$patch" 2>/dev/null; then
    echo 'Fast status indicator is already hidden.'
elif git -C "$checkout" apply --check "$patch"; then
    git -C "$checkout" apply "$patch"
    echo 'Hidden Fast status indicator; /fast remains available.'
else
    echo 'Cannot apply Fast-status patch; inspect local changes before retrying.' >&2
    exit 1
fi
