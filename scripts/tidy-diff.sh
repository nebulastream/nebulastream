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

# Driver for the `tidy-diff` / `tidy-diff-fix` CMake targets.
#
# Runs clang-tidy over the local git diff via clang-tidy-diff(.py). The CMake
# module (cmake/ClangTidyDiff.cmake) discovers the tool paths and passes them in
# as arguments; everything here is plain bash so the logic is easy to read and
# stays identical between the local developer command and CI.
#
# Arguments (all required, supplied by the CMake target):
#   $1  CLANG_TIDY_DIFF   absolute path to clang-tidy-diff(.py)
#   $2  CLANG_TIDY_BINARY absolute path to the clang-tidy binary
#   $3  COMPILE_DB_DIR    directory holding compile_commands.json (build dir)
#   $4  REPORT_FILE       file to additionally write the (de-colored) output to
#   $5  MODE              "check" or "fix"
#
# Diff base:
#   Configurable via the NES_TIDY_DIFF_BASE environment variable. It holds the
#   arguments passed to `git diff` to select the base, e.g. "--cached" (default),
#   "origin/main", or "HEAD~3". We default to "--cached" to match the spirit of
#   the format target (operate on what is about to be committed).

set -eo pipefail

CLANG_TIDY_DIFF="$1"
CLANG_TIDY_BINARY="$2"
COMPILE_DB_DIR="$3"
REPORT_FILE="$4"
MODE="$5"

cd "$(git rev-parse --show-toplevel)"

# Diff base: default to the staged changes (`git diff --cached`).
NES_TIDY_DIFF_BASE="${NES_TIDY_DIFF_BASE:---cached}"

# Path exclusions mirror the CI invocation (.github/workflows/clang_tidy_diff.yml):
# generated/vendored sources are not ours to lint.
EXCLUDES=(':!*.inc' ':!*nes-rust-bindings' ':!vcpkg/**')

COLOR_BOLD="\e[1m"
COLOR_RESET="\e[0m"

# Build the `git diff` selector once, so the summary and the actual run cover
# exactly the same set of changes. -U0 keeps the diff to changed lines only,
# which is what clang-tidy-diff expects for line filtering.
GIT_DIFF_SELECTOR=(diff -U0 ${NES_TIDY_DIFF_BASE} -- "${EXCLUDES[@]}")

# --- Diff summary -----------------------------------------------------------
# Interpreting git diff ranges is non-intuitive, and clang-tidy-diff reports
# success when no lines are in scope. Print a short summary first so a "no-op
# success" is obvious instead of silent.
echo -e "${COLOR_BOLD}clang-tidy-diff: changes covered (base: 'git diff ${NES_TIDY_DIFF_BASE}')${COLOR_RESET}"
DIFF_STAT="$(git diff --stat ${NES_TIDY_DIFF_BASE} -- "${EXCLUDES[@]}" || true)"
if [ -z "$DIFF_STAT" ]; then
    echo "  (no changes in scope -- clang-tidy-diff will report success with nothing to do)"
else
    echo "$DIFF_STAT" | sed 's/^/  /'
fi
echo

# --- clang-tidy-diff invocation --------------------------------------------
# -p1            strip the leading 'a/' 'b/' path component from the diff
# -path          directory containing compile_commands.json
# -use-color     force colored diagnostics so stdout stays colored even though
#                we pipe through tee (clang-tidy would otherwise auto-disable
#                color when it detects a pipe)
TIDY_ARGS=(-clang-tidy-binary "$CLANG_TIDY_BINARY" -p1 -path "$COMPILE_DB_DIR" -use-color -j "$(nproc 2>/dev/null || echo 1)")
if [ "$MODE" = "fix" ]; then
    TIDY_ARGS+=(-fix)
fi

# Report file: we want color on stdout but a clean (ANSI-free) report on disk.
# Tee the raw colored stream to stdout, and pipe a copy through a sed filter
# that strips ANSI escape sequences before writing the report file.
#
# `set -o pipefail` ensures the exit status of clang-tidy-diff (the head of the
# pipe) propagates, so the target fails when findings are reported in check mode.
mkdir -p "$(dirname "$REPORT_FILE")"

git "${GIT_DIFF_SELECTOR[@]}" \
    | python3 "$CLANG_TIDY_DIFF" "${TIDY_ARGS[@]}" \
    | tee >(sed -r 's/\x1B\[[0-9;]*[mK]//g' > "$REPORT_FILE")

echo
echo -e "${COLOR_BOLD}clang-tidy-diff: report written to ${REPORT_FILE}${COLOR_RESET}"
