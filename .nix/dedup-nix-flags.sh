# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Sourced by the devShell's shellHook (bash).
#
# The cc/bintools setup hooks add the include and library directories of a dependency once per path it is reached
# through, so NIX_CFLAGS_COMPILE and NIX_LDFLAGS contain the same directories several times. gcc passes all of its
# options to collect2 in the single COLLECT_GCC_OPTIONS environment variable, which fails with
# "posix_spawn: Argument list too long" once it exceeds the kernel's 128 KiB per-string limit (e.g. when cargo links
# the build scripts of cxxbridge). Repeated search directories have no effect, since only the first occurrence
# determines the search order, so we keep the first occurrence of each path option and leave all other flags as they
# are.
nesDedupPathFlags() {
  local var=$1
  local -a tokens out
  local -A seen
  local i=0 token key
  read -r -a tokens <<< "${!var}"
  while (( i < ${#tokens[@]} )); do
    token=${tokens[i]}
    case $token in
      -isystem | -idirafter | -iquote | -I | -L | -B | -rpath)
        if (( i + 1 < ${#tokens[@]} )); then
          key="$token ${tokens[i + 1]}"
          if [[ -z ${seen[$key]+x} ]]; then
            seen[$key]=1
            out+=("$token" "${tokens[i + 1]}")
          fi
          i=$((i + 2))
          continue
        fi
        ;;
      -I?* | -L?* | -B?*)
        if [[ -n ${seen[$token]+x} ]]; then
          i=$((i + 1))
          continue
        fi
        seen[$token]=1
        ;;
    esac
    out+=("$token")
    i=$((i + 1))
  done
  export "$var=${out[*]}"
}

for nesFlagsVar in NIX_CFLAGS_COMPILE NIX_CFLAGS_COMPILE_FOR_BUILD NIX_LDFLAGS NIX_LDFLAGS_FOR_BUILD; do
  if [[ -n ${!nesFlagsVar:-} ]]; then
    nesDedupPathFlags "$nesFlagsVar"
  fi
done
unset nesFlagsVar
unset -f nesDedupPathFlags
