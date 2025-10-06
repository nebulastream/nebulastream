#!/usr/bin/env sh
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# based on: https://gist.github.com/pmenke-de/2fed80213c48c2fe80891678f4fa3b42

# If we are outside the dev shell, re-run this script inside the nix develop environment.
if [ -z "$IN_NIX_RUN" ]; then
  SCRIPT_PATH="$(readlink -f "$0")"
  exec "$(dirname "$0")"/nix-run.sh "$SCRIPT_PATH" "$@"
fi

# Use the cmakeFlags set by Nix - this doesn't work with --build
if printf '%s\n' "$@" | grep -q -- '--build'; then
  FLAGS=""
else
  FLAGS="$cmakeFlags"
fi

if [ -n "$FLAGS" ]; then
  set -- $FLAGS "$@"
else
  set -- "$@"
fi

exec cmake "$@"
