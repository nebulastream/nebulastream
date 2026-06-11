# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""bash.py — the ``bash -c`` bodies executed inside the ``runner`` service.

Pytest argv arrive shlex-quoted via ``CONNTEST_PYTEST_ARGS`` /
``CONNTEST_META_PYTEST_ARGS`` and are ``eval``-ed; the argv-builders in
``battery.py`` / ``debug.py`` and these bodies are coupled by that convention,
so they live together.
"""

from __future__ import annotations


def _runner_bash_body() -> str:
    """The ``bash -c`` body executed inside the ``runner`` service.

    Order: libc++ loader workaround -> sanitizer symbolizer injection ->
    uv bootstrap (fallback only; the dev image ships uv) -> optional meta
    tests -> per-plugin conformance run. Pytest argv arrive shlex-quoted
    via CONNTEST_PYTEST_ARGS / CONNTEST_META_PYTEST_ARGS.
    """
    return r"""
set -e
mkdir -p "$HOME/.local/bin"
export PATH="$HOME/.local/bin:$PATH"

# Bug 2: the harness's DT_RUNPATH omits the toolchain libc++ on libcxx
# images, so the loader can't find libc++.so.1. We are in the same image
# the build linked against — locate it and prepend its dir. No-op on
# libstdcxx lanes and on images where the loader resolves it natively.
LIBCXX_FILE="$(find /usr /opt -name libc++.so.1 -type f 2>/dev/null | head -1)"
if [ -n "$LIBCXX_FILE" ]; then
    export LD_LIBRARY_PATH="$(dirname "$LIBCXX_FILE")${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
fi

# Symbolizer injection. *SAN_OPTIONS already carry the lane toggles (CI's
# detect_leaks) and the suppressions= path (set host-side by
# `_apply_plugin_suppressions` from each connector's conn-test/*-supp), but
# NOT external_symbolizer_path — llvm-symbolizer lives in THIS dev image, not
# on the host VM that runs `conntest ci`. Append it here (TSan wants
# space-separated options; the rest colon-separated). Without a symbolizer the
# per-frame connector suppressions never match.
SYMB="$(command -v llvm-symbolizer-19 2>/dev/null \
    || command -v llvm-symbolizer 2>/dev/null || true)"
if [ -z "$SYMB" ]; then
    SYMB="$(find /usr -name 'llvm-symbolizer*' -type f 2>/dev/null | head -1)"
fi
if [ -n "$SYMB" ]; then
    if [ -n "${ASAN_OPTIONS:-}" ]; then
        export ASAN_OPTIONS="${ASAN_OPTIONS}:external_symbolizer_path=${SYMB}"
    fi
    if [ -n "${LSAN_OPTIONS:-}" ]; then
        export LSAN_OPTIONS="${LSAN_OPTIONS}:external_symbolizer_path=${SYMB}"
    fi
    if [ -n "${UBSAN_OPTIONS:-}" ]; then
        export UBSAN_OPTIONS="${UBSAN_OPTIONS}:external_symbolizer_path=${SYMB}"
    fi
    if [ -n "${TSAN_OPTIONS:-}" ]; then
        export TSAN_OPTIONS="${TSAN_OPTIONS} external_symbolizer_path=${SYMB}"
    fi
fi

# uv: shipped in the dev image (docker/dependency/Development.dockerfile).
# Keep the astral bootstrap as a fallback for older local images that
# predate that layer.
if ! command -v uv >/dev/null; then
    curl -LsSf https://astral.sh/uv/install.sh | sh
fi

# Workspace env: install the UNION of all members' deps once (the runner +
# every plugin's conn-test deps), then run pytest against it without
# re-syncing. `uv sync` is exact by default and `uv run` from this member dir
# would otherwise prune the other plugins' deps — but discovery imports every
# loader at collection, so the env must hold the union. `--frozen` keeps the
# committed root uv.lock authoritative; `--no-sync` stops the run from pruning.
uv sync --frozen --all-packages

if [ -n "${CONNTEST_META_PYTEST_ARGS:-}" ]; then
    eval "uv run --frozen --no-sync pytest $CONNTEST_META_PYTEST_ARGS"
fi
eval "uv run --frozen --no-sync pytest $CONNTEST_PYTEST_ARGS"
""".strip()


def _debug_runner_bash() -> str:
    """``bash -c`` body for the debug exec.

    Pytest argv (including the ``--build-dir`` / ``--native-debug-port`` flags
    and the test selector) arrive shlex-quoted in ``CONNTEST_PYTEST_ARGS`` and
    are ``eval``-ed, the same convention the run path uses — so a ``-k``
    substring selector matches the parametrized ``test_scenario[...]`` ids (a
    bare ``::nodeid`` cannot).
    """
    return r"""
set -e
export PATH="$HOME/.local/bin:$PATH"
if ! command -v uv >/dev/null; then
    curl -LsSf https://astral.sh/uv/install.sh | sh >/dev/null 2>&1
fi
# Install the union of all workspace members' deps once (see _runner_bash_body),
# then exec pytest without re-syncing so the env keeps every plugin's loader deps.
uv sync --frozen --all-packages
eval "exec uv run --frozen --no-sync pytest $CONNTEST_PYTEST_ARGS"
""".strip()
