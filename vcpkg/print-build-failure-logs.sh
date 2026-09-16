#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# vcpkg keeps compiler errors in buildtree files instead of its install output. Print the failed ports' logs while
# the Docker build layer still exists, preserving the original install failure's exit status in the caller.
set -eu

install_log="$1"
buildtrees="$2"
mapfile -t failed_ports < <(sed -n 's/^error: building \([a-z0-9-]*\):.*/\1/p' "$install_log" | sort -u)
failed_ports+=(detect_compiler)
for port in "${failed_ports[@]}"; do
    if [[ ! -d "$buildtrees/$port" ]]; then
        continue
    fi
    while IFS= read -r -d '' log_file; do
        printf '\n--- vcpkg failure log: %s ---\n' "$log_file"
        cat -- "$log_file"
    done < <(find "$buildtrees/$port" -maxdepth 1 -type f \( -name '*.log' -o -name '*.vcpkg_abi_info.txt' \) -print0 | sort -z)
done
