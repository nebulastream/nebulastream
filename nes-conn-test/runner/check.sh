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

# Component-local Python gate for the conn-test framework, invoked by
# scripts/format.sh (one line, beside the other helper-script checks). Python
# has no compiler, so the contract enforcement C++ gets from clang-tidy /
# -Werror lives here:
#
#   * ruff check   — lint (select = ALL minus a justified ignore list)
#   * ruff format  — formatting (the black/isort equivalent)
#   * mypy --strict — the type gate
#
# Scoping mirrors the C++ checks in format.sh: ruff/ruff-format run on the
# *changed* conn-test Python only (they are per-file-sound on any subset),
# while mypy is a whole-program checker and runs over the entire package — but
# only when at least one conn-test Python file changed (a PR that touches none
# does no work at all, not even a uv bootstrap). Like clang-format, `-i` fixes
# in place; without it, the gate only reports.
#
# Config (pyproject.toml) is passed explicitly because ruff/mypy discover it by
# walking *up* from each file, which would miss this directory's config for the
# out-of-tree plugin files under nes-plugins/*/conn-test/.

set -euo pipefail

# --- 1. mode + paths ---------------------------------------------------------
MODE_FIX=0
if [ "${1-}" = "-i" ]; then
    MODE_FIX=1
elif [ -n "${1-}" ]; then
    # An empty first arg is check mode: format.sh forwards "${1-}", which is the
    # empty string when format.sh itself was invoked without -i.
    echo "usage: $0 [-i]   (-i fixes in place, like clang-format -i)" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"   # .../nes-conn-test/runner
REPO_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)"
CFG="$SCRIPT_DIR/pyproject.toml"

# --- 2. changed-files scope (mirror format.sh) -------------------------------
# DISTANCE_MERGE_BASE is exported by format.sh / CI; recompute it locally the
# same way (format.sh:76) so check.sh is runnable standalone.
if [ -z "${DISTANCE_MERGE_BASE:-}" ]; then
    DISTANCE_MERGE_BASE="$(git -C "$REPO_ROOT" rev-list --count origin/main..HEAD)"
fi

# Added/modified/renamed conn-test Python (skip deletions, like format.sh:149).
# :(glob) magic so ** spans directories. Paths are repo-root-relative.
CHANGED=()
while IFS= read -r f; do
    [ -n "$f" ] && CHANGED+=("$f")
done < <(
    git -C "$REPO_ROOT" diff --name-only --diff-filter=RAM "HEAD~$DISTANCE_MERGE_BASE" -- \
        ':(glob)nes-conn-test/**/*.py' \
        ':(glob)nes-plugins/**/conn-test/*.py'
)

# --- 3. early no-op: nothing changed ⇒ exit BEFORE touching uv ----------------
if [ "${#CHANGED[@]}" -eq 0 ]; then
    exit 0
fi

# --- 4. uv bootstrap fallback (only reached when there is work) --------------
# CI and the dev image always ship uv; the host nes-format skill may not.
if ! command -v uv >/dev/null 2>&1; then
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.local/bin:$PATH"
fi
command -v uv >/dev/null 2>&1 || {
    echo "check.sh: uv is required but could not be installed" >&2
    exit 1
}

# cwd = runner (a uv workspace member): uv discovers the repo-root workspace by
# walking up, and running here makes `conntest_runner` importable so mypy
# resolves the plugins' imports.
cd "$SCRIPT_DIR"

# Platform-keyed uv environment. format.sh runs on the host (nes-format skill)
# AND inside the Linux dev image / CI, both mounting the same repo. A single
# shared `.venv` would be torn down and rebuilt on every host⇄container switch
# (different OS ⇒ uv deems the venv invalid), making the gate slow and flaky.
# Separate per-OS envs (`.venv-linux`, `.venv-darwin`) coexist; the committed
# root uv.lock stays the single version source-of-truth (still `--frozen`). The
# workspace `.venv` lives at the repo root, so the per-OS envs do too.
export UV_PROJECT_ENVIRONMENT="$REPO_ROOT/.venv-$(uname -s | tr '[:upper:]' '[:lower:]')"

# Install the workspace union once from the committed root lock, then run each
# gate tool with `--no-sync` (below) so an exact-sync from this member dir can't
# prune the other members' deps. `--frozen` keeps the lock authoritative.
if ! sync_out="$(uv sync --frozen --all-packages 2>&1)"; then
    printf 'check.sh: uv sync --frozen --all-packages failed:\n%s\n' "$sync_out" >&2
    exit 1
fi

# --- 5. absolutize the changed files for ruff --------------------------------
# Absolute paths so ruff relativizes per-file-ignores against this config's
# directory (the in-tree keys match) and the leading-slash globs reach the
# out-of-tree files (plugins under nes-plugins/, sv_fmt.py).
FILES=()
for f in "${CHANGED[@]}"; do
    FILES+=("$REPO_ROOT/$f")
done

RC=0

# Run a gate step quietly: capture its combined output and surface it — under a
# clear label, as plain text — only on failure. Silent on success, like the C++
# checks in format.sh. Capturing also forces non-color output (ruff/mypy drop
# color when stdout is not a TTY), so no escape codes leak when format.sh's
# output is itself captured.
run_step() {
    local label="$1" out
    shift
    if ! out="$("$@" 2>&1)"; then
        printf '\nconn-test python gate — %s:\n%s\n' "$label" "$out" >&2
        RC=1
    fi
}

# --- 6. ruff: lint + format on the changed files, mode-dependent -------------
# Like clang-format -i, `-i` rewrites in place; otherwise the gate only reports.
# sv_fmt.py and the plugin files ride along here — their relaxations live in
# pyproject's per-file-ignores (keyed by absolute glob), not in a second pass.
if [ "$MODE_FIX" -eq 1 ]; then
    run_step "ruff check --fix" uv run --frozen --no-sync ruff check --fix --config "$CFG" "${FILES[@]}"
    run_step "ruff format" uv run --frozen --no-sync ruff format --config "$CFG" "${FILES[@]}"
else
    run_step "ruff check" uv run --frozen --no-sync ruff check --config "$CFG" "${FILES[@]}"
    run_step "ruff format" uv run --frozen --no-sync ruff format --check --config "$CFG" "${FILES[@]}"
fi

# --- 7. mypy: production code, report-only, gated on "Python changed" --------
# Report-only in both modes (mypy cannot rewrite). Scope = the conntest_runner
# package + the plugin connectors (the production surface). tests/, conformance/
# and conftest.py are intentionally NOT type-gated: they are verified at runtime by
# pytest, and ruff already exempts them from the annotation bar (tests/** drops
# ANN), so requiring full type annotations on every test function here would be
# incoherent. The plugin files go one conn-test/ directory at a time: 4x
# loader.py / 4x scenarios.py share basenames with no package qualifier, so
# passing them together trips "duplicate module"; per-directory keeps each
# loader/scenarios pair cross-checked without the collision.
run_step "mypy (conntest_runner)" uv run --frozen --no-sync mypy --config-file "$CFG" src/conntest_runner

PLUGIN_DIRS=()
while IFS= read -r d; do
    [ -n "$d" ] && PLUGIN_DIRS+=("$REPO_ROOT/$d")
done < <(
    git -C "$REPO_ROOT" ls-files -- ':(glob)nes-plugins/**/conn-test/*.py' \
        | sed 's#/[^/]*$##' | sort -u
)
for d in ${PLUGIN_DIRS[@]+"${PLUGIN_DIRS[@]}"}; do
    run_step "mypy (${d#"$REPO_ROOT"/})" uv run --frozen --no-sync mypy --config-file "$CFG" "$d"
done

exit "$RC"
