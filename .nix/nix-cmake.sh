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

  # Logic to enable building with -DUSE_SANITIZER=...
  REQUESTED_SANITIZER=""
  REQUESTED_STDLIB=""
  BUILD_DIR=""
  PREVIOUS_ARG=""
  for ARG in "$@"; do
    if [ "$PREVIOUS_ARG" = "-D" ]; then
      case "$ARG" in
        USE_SANITIZER=*)
          REQUESTED_SANITIZER="${ARG#USE_SANITIZER=}"
          ;;
        USE_LIBCXX_IF_AVAILABLE=*)
          REQUESTED_STDLIB="${ARG#USE_LIBCXX_IF_AVAILABLE=}"
          ;;
      esac
    elif [ "$PREVIOUS_ARG" = "--build" ]; then
      BUILD_DIR="$ARG"
    fi

    case "$ARG" in
      -DUSE_SANITIZER=*)
        REQUESTED_SANITIZER="${ARG#-DUSE_SANITIZER=}"
        ;;
      -DUSE_LIBCXX_IF_AVAILABLE=*)
        REQUESTED_STDLIB="${ARG#-DUSE_LIBCXX_IF_AVAILABLE=}"
        ;;
      --build)
        PREVIOUS_ARG="--build"
        continue
        ;;
      -D)
        PREVIOUS_ARG="-D"
        continue
        ;;
      --)
        PREVIOUS_ARG=""
        ;;
      *)
        PREVIOUS_ARG=""
        ;;
    esac
  done

  if [ -z "$REQUESTED_SANITIZER" ] && [ -n "$BUILD_DIR" ]; then
    CACHE_FILE="$BUILD_DIR/CMakeCache.txt"
    if [ -f "$CACHE_FILE" ]; then
      CACHE_SANITIZER=$(sed -n 's/^USE_SANITIZER:STRING=\(.*\)$/\1/p' "$CACHE_FILE" | head -n 1)
      if [ -n "$CACHE_SANITIZER" ]; then
        REQUESTED_SANITIZER="$CACHE_SANITIZER"
      fi
      if [ -z "$REQUESTED_STDLIB" ]; then
        CACHE_STDLIB=$(sed -n 's/^USE_LIBCXX_IF_AVAILABLE:BOOL=\(.*\)$/\1/p' "$CACHE_FILE" | head -n 1)
        if [ -n "$CACHE_STDLIB" ]; then
          REQUESTED_STDLIB="$CACHE_STDLIB"
        fi
      fi
    fi
  fi

  if [ -z "$REQUESTED_SANITIZER" ] && [ -n "${NES_SANITIZER:-}" ]; then
    REQUESTED_SANITIZER="$NES_SANITIZER"
  fi

  if [ -z "$REQUESTED_STDLIB" ] && [ -n "${NES_STDLIB:-}" ]; then
    REQUESTED_STDLIB="$NES_STDLIB"
  fi

  SANITIZER_SELECTOR=""
  if [ -n "$REQUESTED_SANITIZER" ]; then
    LOWER_REQUEST=$(printf '%s' "$REQUESTED_SANITIZER" | tr '[:upper:]' '[:lower:]')
    case "$LOWER_REQUEST" in
      asan|address)
        SANITIZER_SELECTOR="address"
        ;;
      tsan|thread)
        SANITIZER_SELECTOR="thread"
        ;;
      ubsan|undefined)
        SANITIZER_SELECTOR="undefined"
        ;;
      none|default)
        SANITIZER_SELECTOR="none"
        ;;
      *)
        printf 'nix-cmake.sh: warning: unrecognized sanitizer "%s", falling back to default\n' "$REQUESTED_SANITIZER" >&2
        SANITIZER_SELECTOR=""
        ;;
    esac
  fi

  STDLIB_SELECTOR=""
  if [ -n "$REQUESTED_STDLIB" ]; then
    LOWER_STDLIB=$(printf '%s' "$REQUESTED_STDLIB" | tr '[:upper:]' '[:lower:]')
    LOWER_STDLIB=$(printf '%s' "$LOWER_STDLIB" | sed 's/++/xx/g')
    case "$LOWER_STDLIB" in
      on|true|1|yes|libcxx)
        STDLIB_SELECTOR="libcxx"
        ;;
      off|false|0|no|libstdcxx|default|local)
        STDLIB_SELECTOR="libstdcxx"
        ;;
      *)
        printf 'nix-cmake.sh: warning: unrecognized stdlib "%s", defaulting to libstdcxx\n' "$REQUESTED_STDLIB" >&2
        STDLIB_SELECTOR="libstdcxx"
        ;;
    esac
  fi

  if [ -n "$SANITIZER_SELECTOR$STDLIB_SELECTOR" ]; then
    NES_SANITIZER_REQUEST="$SANITIZER_SELECTOR" \
      NES_STDLIB_REQUEST="$STDLIB_SELECTOR" \
      exec "$(dirname "$0")"/nix-run.sh "$SCRIPT_PATH" "$@"
  else
    exec "$(dirname "$0")"/nix-run.sh "$SCRIPT_PATH" "$@"
  fi
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
