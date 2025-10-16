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
# ----------------------------------------------------------
# Run any build tool inside the project’s dev‑shell (flake).
# ----------------------------------------------------------
# For every tool you need (gcc, g++, clang++, make, ninja …)
#   ln -s nix-run.sh .nix/g++
# ----------------------------------------------------------

# ---------- figure out which command we should run ----------
if [ "$(basename "$0")" = "nix-run.sh" ]; then
  # called as  “nix-run.sh <real‑cmd> …”
  CMD="$1"; shift
else
  # called through a symlink such as “g++”
  CMD="$(basename "$0")"
fi

if [ -z "$CMD" ]; then
  echo "nix-run.sh: missing command" >&2
  exit 1
fi

# ---------- avoid infinite recursion inside the dev‑shell ---
if [ -n "$IN_NIX_RUN" ]; then
  exec "$CMD" "$@"
fi

# ---------- locate project root & enter the dev‑shell -------
SCRIPT="$(readlink -f "$0")"          # follow symlinks
PROJECT_DIR="$(dirname "$(dirname "$SCRIPT")")"

SANITIZER_REQUEST="${NES_SANITIZER_REQUEST:-}"
if [ -z "$SANITIZER_REQUEST" ] && [ -n "${NES_SANITIZER:-}" ]; then
  SANITIZER_REQUEST="$NES_SANITIZER"
fi

STDLIB_REQUEST="${NES_STDLIB_REQUEST:-}"
if [ -z "$STDLIB_REQUEST" ] && [ -n "${NES_STDLIB:-}" ]; then
  STDLIB_REQUEST="$NES_STDLIB"
fi

SANITIZER_SELECTOR="none"
if [ -n "$SANITIZER_REQUEST" ]; then
  LOWER_REQUEST=$(printf '%s' "$SANITIZER_REQUEST" | tr '[:upper:]' '[:lower:]')
  case "$LOWER_REQUEST" in
    address|asan)
      SANITIZER_SELECTOR="asan"
      ;;
    thread|tsan)
      SANITIZER_SELECTOR="tsan"
      ;;
    undefined|ubsan)
      SANITIZER_SELECTOR="ubsan"
      ;;
    none|default)
      SANITIZER_SELECTOR="none"
      ;;
    *)
      printf 'nix-run.sh: warning: unrecognized sanitizer "%s", falling back to none\n' "$SANITIZER_REQUEST" >&2
      SANITIZER_SELECTOR="none"
      ;;
  esac
fi

STDLIB_SELECTOR="libstdcxx"
if [ -n "$STDLIB_REQUEST" ]; then
  LOWER_STDLIB=$(printf '%s' "$STDLIB_REQUEST" | tr '[:upper:]' '[:lower:]')
  LOWER_STDLIB=$(printf '%s' "$LOWER_STDLIB" | sed 's/++/xx/g')
  case "$LOWER_STDLIB" in
    libcxx|on|true|1|yes)
      STDLIB_SELECTOR="libcxx"
      ;;
    libstdcxx|off|false|0|no|default|local)
      STDLIB_SELECTOR="libstdcxx"
      ;;
    *)
      printf 'nix-run.sh: warning: unrecognized stdlib "%s", defaulting to libstdcxx\n' "$STDLIB_REQUEST" >&2
      STDLIB_SELECTOR="libstdcxx"
      ;;
  esac
fi

if [ "$SANITIZER_SELECTOR" = "none" ] && [ "$STDLIB_SELECTOR" = "libstdcxx" ]; then
  FLAKE_ATTR="default"
else
  FLAKE_ATTR="${SANITIZER_SELECTOR}-${STDLIB_SELECTOR}"
fi

# keep only the environment variables we actually need
KEEP_VARS="
HOME
USER
SHELL
TERM
SSH_AUTH_SOCK
GIT_CONFIG
CTEST_PARALLEL_LEVEL
"
KEEP_ARGS="--keep IN_NIX_RUN"
for var in $KEEP_VARS; do
  if [ -n "$(eval echo \${$var+x})" ]; then
    KEEP_ARGS="$KEEP_ARGS --keep $var"
  fi
done

IN_NIX_RUN=1 NIX_CONFIG="warn-dirty = false" \
  nix develop "${PROJECT_DIR}#${FLAKE_ATTR}" \
  --ignore-environment \
  $KEEP_ARGS \
  --command "$CMD" "$@"
