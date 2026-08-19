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

# Driver for the `tidy-diff` / `tidy-diff-fix` / `tidy-full` CMake targets.
#
# Runs clang-tidy either over the local git diff (via clang-tidy-diff(.py)) or
# over the whole compilation database (via run-clang-tidy). The CMake module
# (cmake/ClangTidy.cmake) discovers the tool paths and passes them in as
# arguments; everything here is plain bash so the logic is easy to read and
# stays identical between the local developer command and CI.
#
# Arguments (all required, supplied by the CMake target):
#   $1  TIDY_TOOL         absolute path to clang-tidy-diff(.py) (diff modes) or
#                         to run-clang-tidy (full mode)
#   $2  CLANG_TIDY_BINARY absolute path to the clang-tidy binary
#   $3  COMPILE_DB_DIR    directory holding compile_commands.json (build dir)
#   $4  REPORT_FILE       file to additionally write the (de-colored) output to
#   $5  MODE              "check", "fix" or "full"
#
# Diff base (diff modes only):
#   Configurable via the NES_TIDY_DIFF_BASE environment variable. It holds the
#   arguments passed to `git diff` to select the base, e.g. "HEAD" (default),
#   "--cached", "origin/main", or "HEAD~3". We default to "HEAD" so the check
#   covers every uncommitted change (staged and unstaged) since the last commit.
#   Note: when building via the Docker toolchain this variable is not forwarded
#   into the container automatically, so pass it in yourself (e.g. add
#   `-e NES_TIDY_DIFF_BASE=...` to the docker run invocation).
#
# Exported fixes (optional, used by CI):
#   If NES_TIDY_EXPORT_FIXES is set to a file path, it is passed through as
#   -export-fixes so the recorded fixes YAML can feed the PR review-comment bot.
#   When unset (the local developer flow) no fixes file is written and the
#   behavior is unchanged.
#
# Check configuration (optional, used by CI):
#   If NES_TIDY_CONFIG_FILE is set, it is forwarded as -config-file so a focused
#   configuration can replace the default .clang-tidy (e.g. the CI fast-fail
#   pre-check that only looks for duplicate includes).

set -eo pipefail

TIDY_TOOL="$1"
CLANG_TIDY_BINARY="$2"
COMPILE_DB_DIR="$3"
REPORT_FILE="$4"
MODE="$5"

# Assign first: `cd "$(git ...)"` would silently become a no-op `cd ""` outside a repository.
REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

COLOR_BOLD="\e[1m"
COLOR_RESET="\e[0m"

# Flags understood by both clang-tidy-diff and run-clang-tidy.
# -use-color     force colored diagnostics so stdout stays colored even though
#                we pipe through tee (clang-tidy would otherwise auto-disable
#                color when it detects a pipe)
TIDY_ARGS=(-clang-tidy-binary "$CLANG_TIDY_BINARY" -use-color -j "$(nproc 2>/dev/null || echo 1)")
if [ "$MODE" = "fix" ]; then
    TIDY_ARGS+=(-fix)
fi
# Optionally record the fixes YAML (see header comment); CI feeds this to the
# PR review-comment bot.
if [ -n "${NES_TIDY_EXPORT_FIXES:-}" ]; then
    mkdir -p "$(dirname "$NES_TIDY_EXPORT_FIXES")"
    TIDY_ARGS+=(-export-fixes "$NES_TIDY_EXPORT_FIXES")
fi
# Optionally replace the default .clang-tidy with a focused configuration (see header comment).
if [ -n "${NES_TIDY_CONFIG_FILE:-}" ]; then
    TIDY_ARGS+=(-config-file "$NES_TIDY_CONFIG_FILE")
fi

# Report file: we want color on stdout but a clean (ANSI-free) report on disk.
# Tee the raw colored stream to stdout, and pipe a copy through a sed filter
# that strips ANSI escape sequences before writing the report file.
#
# `set -o pipefail` ensures the exit status of the tidy tool (the head of the
# pipe) propagates, so the target fails when findings are reported.
mkdir -p "$(dirname "$REPORT_FILE")"
DECOLOR=(sed -r 's/\x1B\[[0-9;]*[mK]//g')

# Don't let a clang-tidy failure abort before we can inspect the report for the
# "missing generated header" symptom below.
set +e
if [ "$MODE" = "full" ]; then
    # Analyze every entry of the compilation database. Generated sources (any
    # '*_generated_src/' directory) and the Rust bindings are not ours to lint,
    # so they are filtered out via negative lookaheads -- the translation units
    # through the positional regex, their includes via -header-filter.
    HEADER_FILTER='^(?!.*nes-rust-bindings/).*'
    SOURCE_FILTER='^(?!.*_generated_src/)(?!.*nes-rust-bindings/).+$'

    echo -e "${COLOR_BOLD}clang-tidy: analyzing every translation unit in ${COMPILE_DB_DIR}${COLOR_RESET}"
    echo

    # run-clang-tidy reports its progress on stderr, which would bury the
    # findings; the diagnostics themselves go to stdout.
    "$TIDY_TOOL" -p "$COMPILE_DB_DIR" "${TIDY_ARGS[@]}" \
        -header-filter="$HEADER_FILTER" "$SOURCE_FILTER" 2>/dev/null \
        | tee >("${DECOLOR[@]}" > "$REPORT_FILE")
    STATUS=${PIPESTATUS[0]}
    grep -q "file not found" "$REPORT_FILE" && MISSING_GENERATED_HEADERS=1
else
    # Diff base: default to all uncommitted changes since the last commit (`git diff HEAD`).
    NES_TIDY_DIFF_BASE="${NES_TIDY_DIFF_BASE:-HEAD}"

    # Path exclusions mirror the CI invocation (.github/workflows/clang_tidy_diff.yml):
    # generated/vendored sources are not ours to lint.
    EXCLUDES=(':!*.inc' ':!*nes-rust-bindings' ':!vcpkg/**')

    # Build the `git diff` selector once, so the summary and the actual run cover
    # exactly the same set of changes. -U0 keeps the diff to changed lines only,
    # which is what clang-tidy-diff expects for line filtering.
    GIT_DIFF_SELECTOR=(diff -U0 ${NES_TIDY_DIFF_BASE} -- "${EXCLUDES[@]}")

    # --- Diff summary -------------------------------------------------------
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

    # Sources and headers are analyzed in two passes, because only a source file
    # has a compile command. For a header clang-tidy adapts the command of the
    # nearest source file in the compilation database; a header whose directory
    # tree holds no compiled source gets no include paths and fails on its first
    # include. That failure describes the reconstructed flags rather than the
    # change, so the header pass reports those headers as skipped. Every other
    # diagnostic, in either pass, still fails the run.
    #
    # -p1            strip the leading 'a/' 'b/' path component from the diff
    # -path          directory containing compile_commands.json
    # -iregex        select which of the changed files this pass analyzes
    SOURCE_PATTERN='.*\.(cpp|cc|c\+\+|cxx|c|cl|m|mm)'
    HEADER_PATTERN='.*\.(h|hh|hpp|hxx)'

    git "${GIT_DIFF_SELECTOR[@]}" \
        | python3 "$TIDY_TOOL" -p1 -path "$COMPILE_DB_DIR" -iregex "$SOURCE_PATTERN" "${TIDY_ARGS[@]}" \
        | tee >("${DECOLOR[@]}" > "$REPORT_FILE")
    STATUS=${PIPESTATUS[1]}

    # Only a source file can be missing a generated header, so the check for the
    # hint below runs before the header pass appends to the report.
    grep -q "file not found" "$REPORT_FILE" && MISSING_GENERATED_HEADERS=1

    # The header pass records its fixes separately, so the source pass keeps the
    # file name that CI uploads. Repeating the flag overrides the earlier one.
    HEADER_TIDY_ARGS=("${TIDY_ARGS[@]}")
    if [ -n "${NES_TIDY_EXPORT_FIXES:-}" ]; then
        HEADER_TIDY_ARGS+=(-export-fixes "${NES_TIDY_EXPORT_FIXES%.*}-headers.yml")
    fi

    HEADER_REPORT_FILE="${REPORT_FILE}.headers"
    git "${GIT_DIFF_SELECTOR[@]}" \
        | python3 "$TIDY_TOOL" -p1 -path "$COMPILE_DB_DIR" -iregex "$HEADER_PATTERN" "${HEADER_TIDY_ARGS[@]}" \
        | tee >("${DECOLOR[@]}" > "$HEADER_REPORT_FILE")
    HEADER_STATUS=${PIPESTATUS[1]}

    # A header pass that failed on nothing but missing includes says nothing about
    # the change, so the skipped headers are listed and the sources decide the run.
    ERROR_COUNT="$(grep -cE ': error: ' "$HEADER_REPORT_FILE" || true)"
    NOT_FOUND_COUNT="$(grep -cE ': error: .*file not found' "$HEADER_REPORT_FILE" || true)"
    if [ "$HEADER_STATUS" -ne 0 ] && [ "$NOT_FOUND_COUNT" -gt 0 ] && [ "$ERROR_COUNT" -eq "$NOT_FOUND_COUNT" ]; then
        echo
        echo -e "${COLOR_BOLD}Skipped ${NOT_FOUND_COUNT} changed header(s): no compile command could be reconstructed.${COLOR_RESET}"
        grep -E ': error: .*file not found' "$HEADER_REPORT_FILE" \
            | cut -d: -f1 | sed "s|^${REPO_ROOT}/||" | sort -u | sed 's/^/  /'
        echo "A header has no compile command of its own, and these sit in a directory tree with no"
        echo "compiled source to take one from. The tidy-full target analyzes them through the"
        echo "sources that include them."
        HEADER_STATUS=0
    fi

    cat "$HEADER_REPORT_FILE" >> "$REPORT_FILE"
    rm -f "$HEADER_REPORT_FILE"

    if [ "$STATUS" -eq 0 ]; then
        STATUS=$HEADER_STATUS
    fi
fi
set -e

# Generated headers (gRPC/protobuf stubs, ANTLR, cxxbridge) only exist after the
# nes-codegen target has run. When they're missing clang-tidy emits "'foo.h' file
# not found" fatal errors on the sources that include them, which look like real
# findings but aren't -- point the user at the target. A changed header without a
# compile command shows the same symptom for another reason, which the header
# pass reports as skipped.
if [ "${MISSING_GENERATED_HEADERS:-0}" -eq 1 ]; then
    echo
    echo -e "${COLOR_BOLD}Error: clang-tidy reported missing headers ('file not found').${COLOR_RESET}"
    echo "Some headers (e.g. gRPC/protobuf stubs) are generated during the build;"
    echo "without them clang-tidy cannot analyze the sources. Build the 'nes-codegen'"
    echo "target to produce them."
fi

echo
echo -e "${COLOR_BOLD}clang-tidy: report written to ${REPORT_FILE}${COLOR_RESET}"
exit "$STATUS"
