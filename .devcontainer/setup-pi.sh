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

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
checkout="$script_dir/pi"
repository="${PI_SETUP_REPOSITORY:-https://github.com/Artraxon/pi-setup.git}"
branch="${PI_SETUP_BRANCH:-main}"

if [[ ! -e "$checkout" ]]; then
    git clone --single-branch --branch "$branch" "$repository" "$checkout"
else
    # Checking .git explicitly prevents Git from silently finding the parent
    # workspace repository when this directory is not a standalone checkout.
    if [[ ! -e "$checkout/.git" ]] || [[ "$(git -C "$checkout" rev-parse --show-toplevel)" != "$checkout" ]]; then
        echo "Refusing to replace $checkout: it is not a standalone Git checkout." >&2
        exit 1
    fi
    if [[ -n "$(git -C "$checkout" status --porcelain)" ]]; then
        echo "Refusing to update $checkout: commit or stash local changes first." >&2
        exit 1
    fi
    current_branch="$(git -C "$checkout" branch --show-current)"
    if [[ "$current_branch" != "$branch" ]]; then
        echo "Refusing to update branch '$current_branch'; expected '$branch'." >&2
        exit 1
    fi
    if git -C "$checkout" remote get-url origin >/dev/null 2>&1; then
        if [[ "$(git -C "$checkout" remote get-url origin)" != "$repository" ]]; then
            echo "Origin differs from $repository; set PI_SETUP_REPOSITORY to use your existing origin." >&2
            exit 1
        fi
    else
        git -C "$checkout" remote add origin "$repository"
    fi
    git -C "$checkout" pull --ff-only origin "$branch"
fi

bash "$checkout/install.sh"
bash "$script_dir/install-pi-skills.sh"
bash "$script_dir/install-pi-openai-toolkit.sh"
