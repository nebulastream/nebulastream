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


# calculates a hash of the dependencies (vcpkg + stuff installed in dockerfiles like clang, mold, etc.)
# to enable pre-built images.

# exit if any command (even inside a pipe) fails or an undefined variable is used.
set -euo pipefail

# cd to root of git repo
cd "$(git rev-parse --show-toplevel)"

# paths of dirs or files that affect the dependency images
#
# Do not use trailing slashes on dirs since this leads to diverging hashes on macos.
HASH_PATHS=(
  vcpkg
  docker/dependency
)

# Find all files, convert CRLF to LF on-the-fly, then hash
find "${HASH_PATHS[@]}" -type f -exec sh -c ' printf "%s  %s\n" "$(tr -d "\r" < "$1" | sha256sum | cut -d " " -f1)" "$1"' sh {} \; \
  | LC_ALL=C sort -k 2 \
  | sha256sum \
  | cut -d ' ' -f1
