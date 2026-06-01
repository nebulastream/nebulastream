# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""conntest_runner.cli — the unified ``conntest`` CLI (docker-setup-v3).

One launcher that is BOTH the host orchestrator and (after a fallback
self-wrap) the in-container driver — the ClickHouse "one launcher, branch
on is_local" shape — so CI and local share one code path and v2's
CI-vs-local drift cannot recur.

Subcommands
-----------
  run [SELECTOR|-k EXPR] [-- pytest-args]   battery / one scenario
  ci                                        exactly what CI runs
  coverage                                  instrumented battery (ccov)
  debug [SCENARIO]                          C++ lldb + CLion attach
  list                                      plugins x scenarios
  install-clion                             (re)write the CLion run configs

The governing principle (§1): never ask "which container am I". The
orchestrator drives ``docker compose``; compose *creates* the ``runner``
service already on the plugin's project network, so the harness reaches
``<service>:<port>`` (e.g. ``broker:1883``) by DNS — no ``docker network
connect $HOSTNAME`` self-attach (the v2 mistake that broke CI).

Execution model (§5.2): on a host that has ``docker compose`` (Docker
Desktop, Colima+plugin, Linux, the GitHub runner VM) the orchestrator
runs right here and the ONLY dev container is the ``runner``. On a host
without it, ``conntest`` re-execs itself once inside a one-shot dev
container (which ships compose) and proceeds identically.
"""

from __future__ import annotations

import argparse
import ast
import contextlib
import os
import re
import shlex
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING
from xml.dom import minidom

if TYPE_CHECKING:
    from typing import NoReturn

# ---------------------------------------------------------------------------
# Static locations (relative to this file)
# ---------------------------------------------------------------------------
_RUNNER_DIR = Path(__file__).resolve().parents[2]  # .../nes-conn-test/runner
_REPO_ROOT_FALLBACK = _RUNNER_DIR.parents[1]  # .../<repo>
_RUNNER_COMPOSE = _RUNNER_DIR / "runner.compose.yaml"
_TOXIPROXY_COMPOSE = _RUNNER_DIR / "toxiproxy.compose.yaml"
_DEBUG_OVERRIDE = _RUNNER_DIR / "compose.debug.yaml"
_CONNTEST_EXE = _RUNNER_DIR / "conntest"

_PROJECT_PREFIX = "nebulastream-conntest"
_DEBUG_PROJECT_PREFIX = "nebulastream-conntest-debug"
_DEFAULT_IMAGE = "nebulastream/nes-development:local"
_DEFAULT_BUILD_DIR = "cmake-build-debug"
_CI_BUILD_DIR = "build"
_DEFAULT_DEBUG_PORT = 2345

# pytest's exit code for "no tests collected" — a -k selector / scenario-group
# restriction legitimately deselected everything, which is not a failure.
_PYTEST_EXIT_NO_TESTS = 5

# Services defined by the framework (runner.compose.yaml + the debug
# override), excluded from the data-service set the orchestrator waits on.
_FRAMEWORK_SERVICES = frozenset({"runner", "lldb-relay"})

# Short scenario name -> pytest -k substring. The generic battery is one
# parametrized test whose ids are "<scenario>-<plugin>-<config>", so a
# scenario's keyword is simply its name (matched as a substring of the id).
_SCENARIO_K = {
    "round_trip": "round_trip",
    "round-trip": "round_trip",
    "empty": "empty",
    "config": "config",
    "bad_endpoint": "bad_endpoint",
    "bad-endpoint": "bad_endpoint",
    "concurrent": "concurrent",
    "reconnect": "reconnect",
    "reconnect_after_death": "reconnect_after_death",
    "conformance": "",
    "": "",
}


def _log(msg: str) -> None:
    print(f"[conntest] {msg}", file=sys.stderr, flush=True)


def _die(msg: str, code: int = 2) -> NoReturn:
    print(f"conntest: {msg}", file=sys.stderr, flush=True)
    raise SystemExit(code)


# ---------------------------------------------------------------------------
# Host environment probing
# ---------------------------------------------------------------------------
def _repo_root() -> Path:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=str(_RUNNER_DIR),
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        if out:
            return Path(out)
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass
    return _REPO_ROOT_FALLBACK


def _in_container() -> bool:
    """True when we are already inside a container (so do not self-wrap)."""
    if os.environ.get("CONNTEST_IN_CONTAINER") == "1":
        return True
    return Path("/.dockerenv").exists()


def _host_has_docker_compose() -> bool:
    try:
        return (
            subprocess.run(
                ["docker", "compose", "version"],
                capture_output=True,
                check=False,
            ).returncode
            == 0
        )
    except FileNotFoundError:
        return False


def _locate_docker_sock() -> str | None:
    """Find the docker *client* endpoint (proves a daemon exists).

    Probe order mirrors the deleted shell scripts: $DOCKER_HOST, the active
    docker context, then the two common default paths. This returns the path
    the docker CLI uses to *connect*; it is not necessarily a valid bind-mount
    source (see ``_docker_sock_mount_source``).
    """
    dh = os.environ.get("DOCKER_HOST", "")
    if dh.startswith("unix://"):
        cand = dh[len("unix://") :]
        if Path(cand).is_socket():
            return cand
    try:
        ctx = subprocess.run(
            ["docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}"],
            capture_output=True,
            text=True,
            check=False,
        ).stdout.strip()
        if ctx.startswith("unix://"):
            cand = ctx[len("unix://") :]
            if Path(cand).is_socket():
                return cand
    except FileNotFoundError:
        pass
    for cand in ("/var/run/docker.sock", str(Path.home() / ".docker/run/docker.sock")):
        if Path(cand).is_socket():
            return cand
    return None


def _docker_sock_mount_source(sock: str) -> str:
    """Pick a bind-mount source for the docker socket the *daemon* can resolve.

    ``sock`` (from ``_locate_docker_sock``) is the client *connection* endpoint.
    On a VM-backed daemon (Colima / Docker Desktop) that is a host-only path the
    daemon's filesystem cannot see, so bind-mounting it fails with "operation not
    supported". The literal ``/var/run/docker.sock`` is resolved on the daemon's
    own side and works everywhere the daemon exposes it there (native Linux,
    Docker Desktop, Colima's VM).

    Prefer ``/var/run/docker.sock`` unless ``DOCKER_HOST`` is explicitly set — an
    explicit endpoint means the user deliberately picked a daemon, so honour it.
    Return the *unresolved* string (resolving the symlink would yield the
    unmountable host path).
    """
    if not os.environ.get("DOCKER_HOST") and Path("/var/run/docker.sock").is_socket():
        return "/var/run/docker.sock"
    return sock


def _docker_sock_gid(image: str, mount_src: str, fallback: int) -> int:
    """Group id of the bind-mounted socket *as the daemon exposes it*.

    The host ``stat`` gid is wrong on VM-backed daemons: inside the container the
    socket carries the VM's docker group, not the host file's group. Stat it from
    inside a throwaway container so ``--group-add`` matches. ``CONNTEST_DOCKER_GID``
    overrides the probe; any failure falls back to ``fallback``.
    """
    override = os.environ.get("CONNTEST_DOCKER_GID")
    if override and override.isdigit():
        return int(override)
    try:
        out = subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "-v",
                f"{mount_src}:/var/run/docker.sock",
                image,
                "stat",
                "-c",
                "%g",
                "/var/run/docker.sock",
            ],
            capture_output=True,
            text=True,
            check=False,
        ).stdout.strip()
        if out.isdigit():
            return int(out)
    except FileNotFoundError:
        pass
    return fallback


def _resolve_image(explicit: str | None, *, must_exist_locally: bool) -> str:
    """Resolve the dev image.

    Precedence: --image > NES_DEV_IMAGE > CONNTEST_DEV_IMAGE > first local
    ``nebulastream/nes-development:*`` > the canonical ``:local`` tag.

    For local subcommands (run/debug) ``must_exist_locally`` validates the
    image is present and fails with a build hint; CI passes the image
    explicitly and compose pulls it, so ci/coverage skip the check.
    """
    image = (
        explicit
        or os.environ.get("NES_DEV_IMAGE")
        or os.environ.get("CONNTEST_DEV_IMAGE")
        or _first_local_dev_image()
        or _DEFAULT_IMAGE
    )
    if must_exist_locally and not _image_present(image):
        _die(
            f"image {image!r} is not available locally. Build it with "
            "scripts/install-local-docker-environment.sh, or set NES_DEV_IMAGE "
            "to an existing nebulastream/nes-development:<tag>."
        )
    return image


def _first_local_dev_image() -> str | None:
    try:
        out = subprocess.run(
            [
                "docker",
                "images",
                "nebulastream/nes-development",
                "--format",
                "{{.Repository}}:{{.Tag}}",
            ],
            capture_output=True,
            text=True,
            check=False,
        ).stdout.splitlines()
    except FileNotFoundError:
        return None
    for line in out:
        stripped = line.strip()
        if stripped and not stripped.endswith(":<none>"):
            return stripped
    return None


def _image_present(image: str) -> bool:
    try:
        return (
            subprocess.run(
                ["docker", "image", "inspect", image],
                capture_output=True,
                check=False,
            ).returncode
            == 0
        )
    except FileNotFoundError:
        return False


def _autodetect_build_dir(repo_root: Path) -> str | None:
    """Find a configured CMake build dir under ``repo_root``.

    Returns its name relative to the repo root, or ``None`` if none is
    configured.

    The conventional ``cmake-build-debug`` (headless ``cmake -B`` recipe,
    what CI/README assume) wins if present. Otherwise we glob
    ``cmake-build-*`` for a directory holding a ``CMakeCache.txt`` — CLion's
    Docker toolchain names its dir ``cmake-build-<profile>`` (e.g.
    ``cmake-build-debug-docker-nes-development-local-libstd``), which the old
    hard-coded default missed entirely — preferring a ``CMAKE_BUILD_TYPE=Debug``
    build, then the most-recently-configured one.
    """
    conventional = repo_root / _DEFAULT_BUILD_DIR
    if (conventional / "CMakeCache.txt").is_file():
        return _DEFAULT_BUILD_DIR
    candidates: list[tuple[bool, float, str]] = []
    for d in repo_root.glob("cmake-build-*"):
        cache = d / "CMakeCache.txt"
        if not cache.is_file():
            continue
        m = re.search(
            r"^CMAKE_BUILD_TYPE:[^=]*=(.*)$",
            cache.read_text(encoding="utf-8", errors="replace"),
            re.MULTILINE,
        )
        is_debug = m is not None and m.group(1).strip().lower() == "debug"
        candidates.append((is_debug, cache.stat().st_mtime, d.name))
    if not candidates:
        return None
    # Debug-typed first, then most-recently-configured.
    candidates.sort(key=lambda t: (t[0], t[1]), reverse=True)
    return candidates[0][2]


def _resolve_build_dir(
    override: str | None = None,
    *,
    default: str | None = None,
    repo_root: Path | None = None,
) -> str:
    """Resolve the build-dir name.

    The single source of truth for the fallback, so no caller restates
    ``cmake-build-debug``.

    Precedence: an explicit ``override`` (the ``--build-dir`` flag) wins;
    then a caller-supplied ``default`` (CI's ``build``); otherwise autodetect
    a configured build dir under ``repo_root``, falling back to the
    conventional ``_DEFAULT_BUILD_DIR`` (so ``_build_context`` emits its
    helpful "not configured" error against a sensible path).
    """
    if override:
        return override
    if default is not None:
        return default
    return _autodetect_build_dir(repo_root or _repo_root()) or _DEFAULT_BUILD_DIR


def _read_container_repo_from_cache(build_dir: Path) -> str | None:
    """Extract ``CMAKE_HOME_DIRECTORY`` from the build's CMakeCache.txt.

    Build artifacts (harness binary, plugin .so) bake in the repo path
    CMake was configured against. The runner has to bind-mount the repo at
    that same path or dlopen / debug-info / profraw paths break. On CI that
    path equals ``$(pwd)``; locally it is typically ``/tmp/nebulastream``.
    """
    cache = build_dir / "CMakeCache.txt"
    if not cache.is_file():
        return None
    m = re.search(
        r"^CMAKE_HOME_DIRECTORY:[^=]*=(.*)$",
        cache.read_text(encoding="utf-8"),
        re.MULTILINE,
    )
    if not m:
        return None
    return m.group(1).strip() or None


# ---------------------------------------------------------------------------
# Run context — everything resolved once, reused per plugin
# ---------------------------------------------------------------------------
@dataclass
class RunContext:
    """Everything resolved once per invocation and reused for every plugin."""

    repo_root: Path
    image: str
    host_root: str  # bind-mount source (host-daemon-visible repo)
    container_repo: str  # bind-mount target == CMAKE_HOME_DIRECTORY
    host_build_dir: Path  # host path to the build dir
    container_build_dir: str  # build dir as seen inside the runner (pytest)
    env: dict[str, str]  # environment for `docker compose`
    k_expr: str = ""
    pytest_extra: list[str] = field(default_factory=list)


def _build_context(
    *,
    image: str,
    build_dir_name: str,
    k_expr: str,
    pytest_extra: list[str],
    extra_env: dict[str, str] | None = None,
) -> RunContext:
    repo_root = _repo_root()
    host_root = os.environ.get("HOST_NEBULASTREAM_ROOT", "").strip() or str(repo_root)
    host_build_dir = (repo_root / build_dir_name).resolve()
    if not (host_build_dir / "CMakeCache.txt").is_file():
        _die(
            f"build dir {host_build_dir} is not configured (no CMakeCache.txt). "
            "Configure it (CLion CMake profile, or `cmake -B <dir>`), or pass "
            "--build-dir to point at an existing one."
        )
    container_repo = _read_container_repo_from_cache(host_build_dir) or host_root
    # The runner sees the repo at container_repo; translate the build dir
    # into that view. On CI host_root == container_repo so this is identity;
    # locally it maps e.g. <repo>/cmake-build-debug -> /tmp/nebulastream/cmake-build-debug.
    try:
        rel = host_build_dir.relative_to(Path(host_root))
    except ValueError:
        rel = Path(host_build_dir.name)
    container_build_dir = f"{container_repo}/{rel.as_posix()}"

    env = os.environ.copy()
    env["HOST_NEBULASTREAM_ROOT"] = host_root
    env["CONNTEST_CONTAINER_REPO"] = container_repo
    env["CONNTEST_DEV_IMAGE"] = image
    env["CONNTEST_HOST_UID"] = str(os.getuid())
    env["CONNTEST_HOST_GID"] = str(os.getgid())
    env.setdefault("CONNTEST_READY_TIMEOUT_S", os.environ.get("CONNTEST_READY_TIMEOUT_S", "10"))
    if extra_env:
        env.update(extra_env)

    ctx = RunContext(
        repo_root=repo_root,
        image=image,
        host_root=host_root,
        container_repo=container_repo,
        host_build_dir=host_build_dir,
        container_build_dir=container_build_dir,
        env=env,
        k_expr=k_expr,
        pytest_extra=pytest_extra,
    )
    # Merge each connector's sanitizer suppressions and point the harness's
    # LSan/TSan at the result (no-op when no connector ships any).
    _apply_plugin_suppressions(ctx)
    return ctx


# ---------------------------------------------------------------------------
# Plugin discovery (host side — globs files, does NOT import loaders so the
# host python needs none of the runner's third-party deps)
# ---------------------------------------------------------------------------
def _find_plugin_composes(repo_root: Path) -> list[Path]:
    return sorted((repo_root / "nes-plugins").rglob("conn-test/compose.yaml"))


# ---------------------------------------------------------------------------
# Per-connector sanitizer suppressions (host side)
#
# conntest-harness is one binary that whole-archives every plugin, so a
# library's known-benign sanitizer findings are a property of that shared
# binary. Each connector declares the suppressions for the libraries IT links
# in its own conn-test/*.lsan-supp / *.tsan-supp — no central file. We discover
# them, dedup + merge into one document (a sanitizer's `suppressions=` takes a
# single file), and set LSAN_OPTIONS / TSAN_OPTIONS so the harness loads it.
# ---------------------------------------------------------------------------
# (file extension, env var, option separator). TSan parses space-separated
# options; LSan colon-separated — matching the symbolizer-injection split in
# `_runner_bash_body`, which appends to whatever this sets.
_SUPP_SANITIZERS = (
    ("lsan-supp", "LSAN_OPTIONS", ":"),
    ("tsan-supp", "TSAN_OPTIONS", " "),
)
# A directive is `<class>:<pattern>` (leak:, race:, mutex:, deadlock:, …).
# Comments (`#`) and blank lines are not directives.
_SUPP_DIRECTIVE = re.compile(r"^[a-z_]+:")


def _find_plugin_suppressions(repo_root: Path, ext: str) -> list[Path]:
    """Every connector's ``conn-test/*.<ext>`` file, sorted for a stable merge."""
    return sorted((repo_root / "nes-plugins").rglob(f"conn-test/*.{ext}"))


def _merge_suppressions(texts: list[str]) -> str:
    """Concatenate connector suppression files into one deduped document.

    A library shared by two connectors (paho, by both MQTT connectors) is
    declared once in each — self-contained — so the merge keeps each directive
    only once, first-seen order. Comments are dropped: the rationale lives in
    the per-connector source files; this generated artifact only needs the
    directives the sanitizer matches against.
    """
    seen: set[str] = set()
    lines = [
        "# Generated by conntest — merged connector sanitizer suppressions.",
        "# Source of truth: each connector's nes-plugins/*/*/conn-test/*-supp.",
    ]
    for text in texts:
        for raw in text.splitlines():
            line = raw.strip()
            if not _SUPP_DIRECTIVE.match(line) or line in seen:
                continue
            seen.add(line)
            lines.append(line)
    return "\n".join(lines) + "\n"


def _apply_plugin_suppressions(ctx: RunContext) -> None:
    """Merge per-connector suppression files and point the sanitizers at them.

    Writes one merged file per sanitizer under the (bind-mounted, gitignored)
    build dir and appends ``suppressions=<container path>`` to the matching
    ``*SAN_OPTIONS``. A no-op for a sanitizer no connector supplies. Harmless on
    a non-matching lane / non-sanitizer build — an uninstrumented binary just
    ignores the env var.
    """
    supp_dir = ctx.host_build_dir / "conntest-suppressions"
    for ext, var, sep in _SUPP_SANITIZERS:
        files = _find_plugin_suppressions(ctx.repo_root, ext)
        if not files:
            continue
        merged = _merge_suppressions([f.read_text(encoding="utf-8") for f in files])
        supp_dir.mkdir(parents=True, exist_ok=True)
        name = f"conntest.{ext}"
        (supp_dir / name).write_text(merged, encoding="utf-8")
        opt = f"suppressions={ctx.container_build_dir}/conntest-suppressions/{name}"
        existing = ctx.env.get(var, "")
        ctx.env[var] = f"{existing}{sep}{opt}" if existing else opt


def _plugin_name(compose_yaml: Path) -> str:
    # .../nes-plugins/<kind>/<Name>/conn-test/compose.yaml -> <Name>
    return compose_yaml.parent.parent.name


def _plugin_kind(compose_yaml: Path) -> str:
    # .../nes-plugins/<kind>/<Name>/conn-test/compose.yaml -> <kind>
    return compose_yaml.parent.parent.parent.name


def _compose_data_services(
    compose_files: list[Path], project: str, env: dict[str, str]
) -> list[str]:
    """Service names to wait on: every service in the merged stack minus the framework's own.

    Excludes ``runner`` / ``lldb-relay``. Resolved via ``docker compose config
    --services`` so we never hard-code connector-specifics like ``broker``
    """
    cmd = ["docker", "compose"]
    for f in compose_files:
        cmd += ["-f", str(f)]
    cmd += ["-p", project, "config", "--services"]
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True, check=False)
    services = [s.strip() for s in proc.stdout.splitlines() if s.strip()]
    data = [s for s in services if s not in _FRAMEWORK_SERVICES]
    if not data:
        _log(
            f"warning: no data services found in {[str(f) for f in compose_files]} "
            f"(config --services rc={proc.returncode}); falling back to `up --wait`"
        )
    return data


# ---------------------------------------------------------------------------
# The in-runner bash body
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# Build the harness inside the runner (local convenience: `conntest run --build`)
# ---------------------------------------------------------------------------
def _build_harness(ctx: RunContext) -> None:
    project = f"{_PROJECT_PREFIX}-build-{uuid.uuid4().hex}"
    base = ["docker", "compose", "-f", str(_RUNNER_COMPOSE), "-p", project]
    body = f"set -e; cmake --build {shlex.quote(ctx.container_build_dir)} --target conntest-harness"
    _log(f"building conntest-harness in the runner ({ctx.container_build_dir})")
    try:
        subprocess.run(
            [*base, "run", "--rm", "-T", "--no-deps", "--entrypoint", "bash", "runner", "-c", body],
            env=ctx.env,
            check=True,
        )
    finally:
        subprocess.run([*base, "down", "-v"], env=ctx.env, check=False)


# ---------------------------------------------------------------------------
# Run one plugin's battery inside its own compose project
# ---------------------------------------------------------------------------
def _setup_file_groups(plugin_compose: Path) -> list[tuple[str | None, str | None]]:
    """How to split a plugin's run into stack lifecycles.

    Returns ``(setup_file, scenario_keyword)`` pairs — one stack ``up``/``down``
    per pair. ``setup_file`` (or None) is the service start-up file to mount;
    ``scenario_keyword`` (or None for "no restriction") is a pytest ``-k``
    sub-expression scoping the run to that group's scenarios.

    A plugin whose scenarios declare no ``setup_file`` runs as a single default
    group — identical to the pre-grouping behaviour. Connectors that *do* pin
    setup files get one stack per distinct file, each started with that file
    mounted. The compose↔setup_file contract is enforced here so neither half
    can be configured without the other (no silently-ignored ``setup_file``,
    no dangling ``${CONNTEST_SETUP_FILE}``).
    """
    # Deferred: discovery pulls in the runner's third-party deps the host python may lack.
    from conntest_runner.discovery import read_plugin_scenarios  # noqa: PLC0415

    plugin = _plugin_name(plugin_compose)
    scenarios = read_plugin_scenarios(plugin_compose.parent)
    compose_uses_var = "CONNTEST_SETUP_FILE" in plugin_compose.read_text(encoding="utf-8")

    if scenarios is None:
        if compose_uses_var:
            _die(
                f"{plugin}: compose.yaml mounts ${{CONNTEST_SETUP_FILE}} but the "
                f"scenarios live on the loader class attribute, where setup_file "
                f"cannot be declared. Move them to conn-test/scenarios.py."
            )
        return [(None, None)]

    with_file = [s for s in scenarios if s.setup_file]
    if compose_uses_var and len(with_file) != len(scenarios):
        missing = sorted({s.name for s in scenarios if not s.setup_file})
        _die(
            f"{plugin}: compose.yaml mounts ${{CONNTEST_SETUP_FILE}}, so every "
            f"scenario must declare setup_file; missing on: {', '.join(missing)}"
        )
    if not compose_uses_var and with_file:
        named = sorted({s.name for s in with_file})
        _die(
            f"{plugin}: scenario(s) {', '.join(named)} declare setup_file but "
            f"compose.yaml hardcodes its setup mount. Switch the mount to "
            f"${{CONNTEST_SETUP_FILE}} or drop setup_file."
        )
    if not with_file:
        return [(None, None)]  # single default stack; no per-scenario restriction

    # Every declared setup_file must exist on disk. The compose mounts it as
    # ${CONNTEST_SETUP_FILE}; a missing source makes the docker daemon
    # auto-create a *directory* at that path, which then fails the bind with
    # the opaque "cannot mount a directory onto a file" OCI error at `up`
    # time (and litters the conn-test dir with an empty dir named after the
    # phantom file). Catch it here — where we already own the compose↔
    # setup_file contract — with a message that names the offending file.
    missing_files = sorted(
        {
            s.setup_file
            for s in with_file
            if s.setup_file is not None and not (plugin_compose.parent / s.setup_file).is_file()
        }
    )
    if missing_files:
        _die(
            f"{plugin}: scenario(s) declare setup_file(s) that do not exist "
            f"under {plugin_compose.parent}: {', '.join(missing_files)}. "
            f"Create the file(s) or fix the setup_file name in scenarios.py."
        )

    # One stack per distinct setup_file, each scoped to its scenarios' tests.
    by_file: dict[str | None, set[str]] = {}
    for s in scenarios:
        by_file.setdefault(s.setup_file, set()).add(s.name)
    groups: list[tuple[str | None, str | None]] = []
    for setup_file, names in by_file.items():
        funcs = sorted({_SCENARIO_K.get(n, n) for n in names})
        groups.append((setup_file, " or ".join(funcs)))
    return groups


def _debug_setup_file(plugin_compose: Path, scenario: str) -> str | None:
    """The setup_file the debug stack must mount for ``scenario``.

    A connector's conformance compose may mount ``${CONNTEST_SETUP_FILE}``
    The run path resolves one per setup_file
    group, but debug brings up a single scenario, so pin the file that
    scenario's group declares — without it the ``${...:?required}``
    interpolation aborts ``up`` before the broker even starts. Returns None
    when the connector pins no setup_file (its compose then has no
    ``${CONNTEST_SETUP_FILE}`` to satisfy). Reuses ``_setup_file_groups`` so
    the same compose<->setup_file contract is enforced and a bad/absent file
    is reported with the same message the run path gives.
    """
    func = _SCENARIO_K.get(scenario, scenario)
    groups = _setup_file_groups(plugin_compose)
    for setup_file, scenario_keyword in groups:
        if scenario_keyword is None or func in scenario_keyword.split(" or "):
            return setup_file
    # Scenario name didn't map to any group (e.g. a --selector-only run on a
    # connector that still pins a setup_file): fall back to the first group's
    # file so ${CONNTEST_SETUP_FILE} is satisfied rather than left unset.
    return groups[0][0] if groups else None


def _run_plugin(plugin_compose: Path, ctx: RunContext, *, run_meta: bool) -> int:
    """Run a plugin's battery, one stack lifecycle per setup_file group."""
    overall = 0
    for i, (setup_file, scenario_keyword) in enumerate(_setup_file_groups(plugin_compose)):
        rc = _run_plugin_group(
            plugin_compose,
            ctx,
            run_meta=(run_meta and i == 0),
            setup_file=setup_file,
            scenario_keyword=scenario_keyword,
        )
        if rc != 0:
            overall = rc
    return overall


def _plugin_needs_link(plugin_compose: Path) -> bool:
    """Whether the plugin declares a needs="link" (fault) scenario.

    Read host-side without importing the loader.
    """
    # Deferred: discovery pulls in the runner's third-party deps the host python may lack.
    from conntest_runner.discovery import read_plugin_scenarios  # noqa: PLC0415

    scenarios = read_plugin_scenarios(plugin_compose.parent)
    return scenarios is not None and any(getattr(s, "needs", None) == "link" for s in scenarios)


def _run_plugin_group(
    plugin_compose: Path,
    ctx: RunContext,
    *,
    run_meta: bool,
    setup_file: str | None,
    scenario_keyword: str | None,
) -> int:
    plugin = _plugin_name(plugin_compose)
    project = f"{_PROJECT_PREFIX}-{uuid.uuid4().hex}"
    compose_files = [plugin_compose, _RUNNER_COMPOSE]
    # Merge the toxiproxy sidecar only when this plugin declares a fault
    # (needs="link") scenario, so plugins without one bring up no extra service.
    if _plugin_needs_link(plugin_compose):
        compose_files.append(_TOXIPROXY_COMPOSE)
    base = ["docker", "compose"]
    for f in compose_files:
        base += ["-f", str(f)]
    base += ["-p", project]

    # One generic battery covers both kinds (source seed→fill→compare and
    # sink write→read_back→compare are dispatched inside the catalog by kind).
    conformance_test = "tests/test_conformance.py"
    junit_conformance = f"{ctx.container_build_dir}/junit-conntest-{plugin}.xml"
    pytest_args = [
        conformance_test,
        "-v",
        f"--build-dir={ctx.container_build_dir}",
        f"--junit-xml={junit_conformance}",
    ]
    # Scope by plugin, then (for a setup_file group) by that group's scenarios,
    # then by any user selector / -k.
    clauses = [f"({plugin})"]
    if scenario_keyword:
        clauses.append(f"({scenario_keyword})")
    if ctx.k_expr:
        clauses.append(f"({ctx.k_expr})")
    pytest_args += ["-k", " and ".join(clauses)]
    pytest_args += ctx.pytest_extra
    pytest_quoted = " ".join(shlex.quote(a) for a in pytest_args)

    meta_quoted = ""
    if run_meta:
        meta_args = [
            "tests/test_discovery.py",
            "tests/test_protocol.py",
            "tests/test_datamodel.py",
            "-v",
            f"--junit-xml={ctx.container_build_dir}/junit-conntest-meta.xml",
            *ctx.pytest_extra,
        ]
        meta_quoted = " ".join(shlex.quote(a) for a in meta_args)

    # A setup_file group starts the service with that file mounted (the
    # connector's compose.yaml maps ${CONNTEST_SETUP_FILE} to the right path).
    env = {**ctx.env, "CONNTEST_SETUP_FILE": setup_file} if setup_file else ctx.env
    data_services = _compose_data_services(compose_files, project, env)

    label = plugin if not setup_file else f"{plugin} [setup_file={setup_file}]"
    _log(f"{label}: up compose data service(s) {data_services or '(all)'} (project={project})")
    rc = 1
    try:
        # `up -d --wait` aborts when a data service exits or never turns
        # healthy — the framework's most common CI failure mode (a data
        # container dies during init). Capture a postmortem here, BEFORE the
        # `finally: down -v` destroys the container and its logs, instead of
        # letting `check=True` raise a bare CalledProcessError that strands us
        # with only docker's "exited (N)" line and no container stderr. Return
        # non-zero (don't raise) so the battery still exercises the remaining
        # plugins — a flaky db on one connector no longer masks the rest.
        up_rc = subprocess.run(
            [*base, "up", "-d", "--wait", *data_services],
            env=env,
            check=False,
        ).returncode
        if up_rc != 0:
            _log(
                f"{label}: data service(s) failed to come up (rc={up_rc}); "
                f"capturing postmortem before teardown"
            )
            _capture_postmortem(base, plugin, ctx, env=env, phase="up --wait")
            return up_rc or 1
        _log(f"{label}: pytest in a one-off runner on the project network")
        # Forward the active setup_file into the runner so the parametrization
        # collects only this group's scenarios (a scenario reused across setup
        # files otherwise produces a duplicate test in every group).
        setup_env = ["-e", f"CONNTEST_SETUP_FILE={setup_file}"] if setup_file else []
        run_cmd = [
            *base,
            "run",
            "--rm",
            "-T",
            "--no-deps",
            "--entrypoint",
            "bash",
            "-e",
            f"CONNTEST_PYTEST_ARGS={pytest_quoted}",
            "-e",
            f"CONNTEST_META_PYTEST_ARGS={meta_quoted}",
            *setup_env,
            "runner",
            "-c",
            _runner_bash_body(),
        ]
        rc = subprocess.run(run_cmd, env=env, check=False).returncode
        # pytest exit 5 == "no tests collected": a -k selector / scenario-group
        # restriction legitimately deselected every test here. Not a failure.
        if rc == _PYTEST_EXIT_NO_TESTS and (ctx.k_expr or scenario_keyword):
            _log(f"{label}: no tests matched the selector — skipping")
            return 0
        if rc != 0:
            _capture_postmortem(base, plugin, ctx, env=env, phase="pytest")
        return rc
    finally:
        _log(f"{label}: down -v")
        subprocess.run([*base, "down", "-v"], env=env, check=False)


def _capture_postmortem(
    base: list[str],
    plugin: str,
    ctx: RunContext,
    *,
    env: dict[str, str] | None = None,
    phase: str = "pytest",
) -> None:
    """Dump the project's container states + compose logs next to the junit (§5.4, host-side form).

    On a failing run this turns a CI flake into a readable artifact instead of
    "no idea why". Best-effort; never masks the real failure.

    Captured (before ``down -v`` destroys the project):
      * ``df -h`` of the build-dir filesystem — instant proof of a full disk,
        the leading cause of data-container init failures on the shared
        self-hosted runners ("No space left on device" during initdb).
      * ``docker compose ps -a`` — per-container status + exit code, which is
        the only place the *reason* ``up --wait`` aborted is visible (docker's
        "exited (N)" line names the code but not the container's own output).
      * ``docker compose logs`` — each service's stderr (e.g. postgres
        initdb / FATAL lines), which ``up --wait`` never prints.
      * ``docker system df`` — WHICH docker resource ate the disk (images /
        build cache / volumes), so the cleanup is targeted. Slow on a bloated
        store (it sizes every object), so it runs last under a timeout and a
        miss never costs us the cheaper sections above.

    Each probe is captured independently and the file is written in a
    ``finally`` so a slow/hung probe still yields a partial artifact rather
    than nothing. ``env`` must be the same environment the failing step ran
    with (it may carry ``CONNTEST_SETUP_FILE``, which the compose files
    interpolate); defaults to ``ctx.env``. ``phase`` records where we failed
    (``up --wait`` vs ``pytest``) so the artifact is self-describing.

    (The full §5.4 idea — an ``lldb -p <pid> thread backtrace all`` of a
    stuck harness — needs an in-runner hook at crash time; the harness is
    already reaped by the time pytest returns to this host process, so we
    capture what the host can still see.)
    """
    env = ctx.env if env is None else env
    out = ctx.host_build_dir / f"postmortem-conntest-{plugin}.log"
    sections = [f"=== conntest postmortem: {plugin} (failed at: {phase}) ==="]

    def _probe(title: str, cmd: list[str], timeout: int) -> None:
        try:
            p = subprocess.run(
                cmd, env=env, capture_output=True, text=True, timeout=timeout, check=False
            )
            sections.append(f"--- {title} ---\n" + p.stdout + p.stderr)
        except Exception as exc:  # noqa: BLE001 - a probe must never raise
            sections.append(f"--- {title} (capture failed: {exc}) ---")

    try:
        _probe("df -h (build-dir filesystem)", ["df", "-h", str(ctx.host_build_dir)], timeout=10)
        _probe("docker compose ps -a", [*base, "ps", "-a"], timeout=30)
        _probe("docker compose logs", [*base, "logs", "--no-color", "--timestamps"], timeout=30)
        # Slow on a bloated store; run last so a timeout can't strand the rest.
        _probe("docker system df", ["docker", "system", "df"], timeout=30)
    finally:
        try:
            out.write_text("\n\n".join(sections), encoding="utf-8")
            _log(f"{plugin}: wrote postmortem ({phase}) to {out}")
        except Exception as exc:  # noqa: BLE001 - postmortem must never raise
            _log(f"{plugin}: postmortem write failed: {exc}")


def _run_battery(ctx: RunContext, plugins: list[Path], *, with_meta: bool = True) -> int:
    if not plugins:
        _die("no plugin compose.yaml found under nes-plugins/", code=2)
    overall = 0
    for i, plugin_compose in enumerate(plugins):
        rc = _run_plugin(plugin_compose, ctx, run_meta=(with_meta and i == 0))
        if rc != 0:
            _log(f"{plugin_compose} FAILED (rc={rc})")
            overall = rc
    return overall


def _select_plugin_composes(repo_root: Path, plugin: str | None) -> list[Path]:
    """Discovered plugin composes, optionally narrowed to a single plugin by name.

    Narrowing here — *before* the battery loop means the other plugins' docker stacks
    are never brought up, rather than brought up only to have their tests deselected.
    """
    composes = _find_plugin_composes(repo_root)
    if plugin is None:
        return composes
    matched = [c for c in composes if _plugin_name(c) == plugin]
    if not matched:
        available = ", ".join(sorted(_plugin_name(c) for c in composes))
        _die(f"--plugin {plugin!r} matches no connector; available: {available}", code=2)
    return matched


# ---------------------------------------------------------------------------
# Selector handling (scenario short name or raw -k expr)
# ---------------------------------------------------------------------------
def _resolve_k_expr(selector: str, k_flag: str) -> str:
    if k_flag:
        return k_flag
    sel = selector.strip()
    if sel in ("discovery", "protocol"):
        _die(
            "discovery/protocol are meta tests; conntest runs them once per "
            "session before the conformance battery. Run without a selector "
            "(or with a per-test keyword)."
        )
    if sel in _SCENARIO_K:
        return _SCENARIO_K[sel]
    return sel  # treat as a raw pytest -k expression


# ---------------------------------------------------------------------------
# Self-wrap (fallback for hosts without `docker compose`)
# ---------------------------------------------------------------------------
def _maybe_self_wrap(raw_argv: list[str]) -> None:
    """Re-exec inside a one-shot dev container when the host lacks ``docker compose``.

    No-op on the common path (host has compose, or we are already in a
    container).
    """
    if _in_container() or _host_has_docker_compose():
        return
    sock = _locate_docker_sock()
    if sock is None:
        _die(
            "host has no `docker compose` and no docker socket was found to "
            "self-wrap into a dev container. Start your docker daemon (e.g. "
            "`colima start`) and confirm `docker context show`."
        )
    image = _resolve_image(None, must_exist_locally=True)
    repo = str(_repo_root())
    mount_src = _docker_sock_mount_source(sock)
    try:
        host_gid = Path(sock).stat().st_gid
    except OSError:
        host_gid = os.getgid()
    gid = _docker_sock_gid(image, mount_src, host_gid)
    _log(f"host lacks `docker compose`; self-wrapping into {image}")
    cmd = [
        "docker",
        "run",
        "--rm",
        "-v",
        f"{mount_src}:/var/run/docker.sock",
        "--group-add",
        str(gid),
        "-v",
        f"{repo}:{repo}",
        "-e",
        "CONNTEST_IN_CONTAINER=1",
        "-e",
        f"HOST_NEBULASTREAM_ROOT={repo}",
        "-e",
        f"HOME={repo}",
        "-u",
        f"{os.getuid()}:{os.getgid()}",
        "--workdir",
        str(Path(repo) / "nes-conn-test/runner"),
        image,
        str(Path(repo) / "nes-conn-test/runner/conntest"),
        *raw_argv,
    ]
    raise SystemExit(subprocess.run(cmd, check=False).returncode)


# ---------------------------------------------------------------------------
# Subcommands "run" and "ci"
# ---------------------------------------------------------------------------
def _cmd_run(args: argparse.Namespace, *, ci: bool) -> int:
    image = _resolve_image(args.image, must_exist_locally=not ci)
    build_dir_name = _resolve_build_dir(args.build_dir, default=_CI_BUILD_DIR if ci else None)
    k_expr = "" if ci else _resolve_k_expr(args.selector, args.k)
    ctx = _build_context(
        image=image,
        build_dir_name=build_dir_name,
        k_expr=k_expr,
        pytest_extra=args.pytest_extra,
    )
    _log(f"image={ctx.image}")
    _log(f"build-dir host={ctx.host_build_dir} container={ctx.container_build_dir}")
    if getattr(args, "build", False):
        _build_harness(ctx)
    plugin = getattr(args, "plugin", None)
    composes = _select_plugin_composes(ctx.repo_root, plugin)
    # A focused single-plugin run executes only that plugin's tests — the
    # framework meta tests (discovery/protocol) are not plugin-specific, so
    # skip them here; a full `conntest run` still runs them once.
    return _run_battery(ctx, composes, with_meta=(plugin is None))


# ---------------------------------------------------------------------------
# Subcommand: coverage (instrumented battery + ccov list wiring)
# ---------------------------------------------------------------------------
def _cmd_coverage(args: argparse.Namespace) -> int:
    image = _resolve_image(args.image, must_exist_locally=False)
    build_dir_name = _resolve_build_dir(args.build_dir, default=_CI_BUILD_DIR)
    ctx = _build_context(image=image, build_dir_name=build_dir_name, k_expr="", pytest_extra=[])

    profraw_dir = ctx.host_build_dir / "nes-conn-test" / "profraw"
    if profraw_dir.exists():
        for p in profraw_dir.glob("*.profraw"):
            p.unlink()
    profraw_dir.mkdir(parents=True, exist_ok=True)
    # The harness (in the runner) writes profraws here via the bind mount.
    # On the coverage lane host_root == container_repo, so the host and
    # container views of this path coincide.
    container_profraw = f"{ctx.container_build_dir}/nes-conn-test/profraw"
    ctx.env["LLVM_PROFILE_FILE"] = f"{container_profraw}/conntest.%p-%m.profraw"

    rc = _run_battery(ctx, _find_plugin_composes(ctx.repo_root))

    ccov_dir = Path(args.ccov_dir) if args.ccov_dir else ctx.host_build_dir / "ccov"
    ccov_dir.mkdir(parents=True, exist_ok=True)
    profraws = sorted(profraw_dir.glob("*.profraw"))
    if not profraws:
        _die(
            f"no .profraw files in {profraw_dir} after the battery. Was the "
            "build configured with -DCODE_COVERAGE=ON?",
            code=1,
        )
    with (ccov_dir / "profraw.list").open("a", encoding="utf-8") as fh:
        for p in profraws:
            fh.write(f"{p}\n")
    if args.harness:
        with (ccov_dir / "binaries.list").open("a", encoding="utf-8") as fh:
            fh.write(f"-object={args.harness}\n")
    _log(f"coverage: merged {len(profraws)} profraw(s) into {ccov_dir}")
    return rc


# ---------------------------------------------------------------------------
# Subcommand: debug (C++ lldb + CLion attach — preserves debug-listen.sh)
# ---------------------------------------------------------------------------
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


def _cleanup_stale_debug() -> None:
    try:
        out = subprocess.run(
            ["docker", "ps", "-aq", "--filter", f"name=^{_DEBUG_PROJECT_PREFIX}-"],
            capture_output=True,
            text=True,
            check=False,
        ).stdout.split()
    except FileNotFoundError:
        return
    if out:
        _log(f"removing stale debug container(s): {' '.join(out)}")
        subprocess.run(["docker", "rm", "-f", *out], capture_output=True, check=False)


def _harness_debug_info_mapped(ctx: RunContext) -> bool:
    """Whether the harness debug info uses HOST paths so CLion breakpoints can bind.

    True when built with -fdebug-prefix-map (host paths) or no remap is needed.
    When ``host_root == container_repo`` (CI / identity mount) no remap is
    needed → True. Otherwise -fdebug-prefix-map rewrites every DW_AT_comp_dir
    /name from the container path to ``host_root``; the host path can only
    appear in the binary if that rewrite happened, so a fixed-string scan for
    it is a reliable proxy. If grep is missing or the harness isn't built we
    don't block (return True) — the pytest fixture skips a missing binary.
    """
    if ctx.host_root == ctx.container_repo:
        return True
    harness = ctx.host_build_dir / "nes-conn-test" / "conntest-harness"
    if not harness.is_file():
        return True
    needle = f"{ctx.host_root}/nes-conn-test"
    try:
        return (
            subprocess.run(
                ["grep", "-a", "-F", "-q", "-m1", "-e", needle, str(harness)],
                check=False,
            ).returncode
            == 0
        )
    except FileNotFoundError:
        return True


def _assert_debug_paths_mapped(ctx: RunContext) -> None:
    """Refuse a debug run when the harness still carries container paths.

    Otherwise CLion breakpoints never bind, the inferior runs free, and CLion
    reports 'Debugger detached' the instant it continues.
    """
    if _harness_debug_info_mapped(ctx):
        return
    _die(
        "conntest debug: conntest-harness was built WITHOUT "
        "-fdebug-prefix-map, so its debug info uses container paths "
        f"({ctx.container_repo}/...). CLion breakpoints set on host paths "
        f"({ctx.host_root}/...) will NOT bind: the inferior runs free and "
        "CLion reports 'Debugger detached' immediately.\n"
        "Fix: set HOST_NEBULASTREAM_ROOT in your CLion CMake profile's "
        "Environment (Settings > Build, Execution, Deployment > CMake) to\n"
        f"    HOST_NEBULASTREAM_ROOT={ctx.host_root}\n"
        "then Reload CMake Project and rebuild conntest-harness "
        "(see nes-conn-test/README.md -> Debugging)."
    )


def _die_no_debug_target() -> NoReturn:
    """Fail with launcher setup instructions when no connector is selected.

    The launcher invoked ``conntest debug`` (--detach or not) without naming a
    connector. We can't pick a compose stack to bring up, so rather than crash
    deep in subprocess with a ``NoneType`` argv (the plugin flag threads
    straight into the foreground argv / pytest -k), fail up front with
    instructions for pointing the ConnTest Debug Launcher at a real target.
    """
    try:
        names = sorted(_plugin_name(c) for c in _find_plugin_composes(_repo_root()))
    except OSError:
        names = []
    available = ", ".join(names) if names else "(none discovered under nes-plugins/)"
    _die(
        "debug: no connector selected.\n"
        "  The ConnTest Debug Launcher ran `conntest debug --detach` with no\n"
        "  scenario or plugin, so there is no connector stack to bring up.\n"
        "\n"
        "  Point it at a specific scenario for a specific plugin. In CLion:\n"
        '  Edit Configurations -> "ConnTest Debug Launcher" -> Script text,\n'
        "  e.g.:\n"
        "\n"
        "      conntest debug round_trip --detach --plugin <PLUGIN_NAME>\n"
        "\n"
        "    scenario (1st positional): round_trip, empty, config, bad_endpoint,\n"
        "                               reconnect, … (connector-specific)\n"
        f"    --plugin <PLUGIN_NAME>:    one of {available}\n"
        "\n"
        "  Then set breakpoints in that connector's source and click Debug.\n"
        "  `conntest list` shows each plugin's available scenarios."
    )


def _cmd_debug(args: argparse.Namespace) -> int:
    port = args.port
    if not args.plugin and not args.selector:
        _die_no_debug_target()
    if args.detach:
        return _debug_detach(args, port)

    image = _resolve_image(args.image, must_exist_locally=True)
    scenario = args.scenario or "round_trip"
    k = _SCENARIO_K.get(scenario, scenario)

    # --plugin selects the connector whose compose stack we bring up and whose
    # tests we run. A raw --selector may instead pin the plugin via a [Name]
    # suffix (e.g. a hand-typed nodeid) — honour that for compose resolution.
    plugin = args.plugin
    if args.selector:
        m = re.search(r"\[([^\]]+)\]$", args.selector)
        if m:
            plugin = m.group(1)

    plugin_compose = _find_plugin_compose(plugin)
    if plugin_compose is None:
        _die(f"no conn-test/compose.yaml for plugin {plugin!r}")

    # The connector's conformance compose may mount ${CONNTEST_SETUP_FILE};
    # pin the file this scenario's group declares (None when the connector
    # uses none). It is needed at TWO points: the compose interpolation at
    # `up` (via ctx.env) and pytest's _cases() collection filter inside the
    # runner (via the exec env, where it also fixes the parametrized id).
    setup_file = _debug_setup_file(plugin_compose, scenario)

    extra_env = {"CONNTEST_DEBUG_PORT": str(port)}
    if setup_file:
        extra_env["CONNTEST_SETUP_FILE"] = setup_file
    ctx = _build_context(
        image=image,
        build_dir_name=_resolve_build_dir(args.build_dir),
        k_expr="",
        pytest_extra=[],
        extra_env=extra_env,
    )
    _assert_debug_paths_mapped(ctx)

    # Build the pytest argv host-side and forward it shlex-quoted. A -k
    # substring expression (plugin + scenario) is what actually selects the
    # parametrized test_scenario[...] case; --selector, when given, is passed
    # through verbatim as the user's own full selector.
    pytest_args = [
        "-s",
        f"--build-dir={ctx.container_build_dir}",
        f"--native-debug-port={port}",
        "-v",
    ]
    if args.selector:
        pytest_args.append(args.selector)
        sel_label = args.selector
    else:
        clauses = [f"({plugin})"] + ([f"({k})"] if k else [])
        kexpr = " and ".join(clauses)
        pytest_args += ["tests/test_conformance.py", "-k", kexpr]
        sel_label = f"-k {kexpr}"
    pytest_quoted = " ".join(shlex.quote(a) for a in pytest_args)

    _cleanup_stale_debug()
    project = f"{_DEBUG_PROJECT_PREFIX}-{port}"
    base = [
        "docker",
        "compose",
        "-f",
        str(plugin_compose),
        "-f",
        str(_RUNNER_COMPOSE),
        "-f",
        str(_DEBUG_OVERRIDE),
        "-p",
        project,
    ]
    _log(
        f"debug: plugin={plugin} scenario={scenario} select={sel_label} "
        f"setup_file={setup_file or '(none)'} port={port} project={project}"
    )
    try:
        # Bring up EVERYTHING (data + runner + lldb-relay): the debug flow
        # needs the runner long-lived with its published port + the socat
        # keepalive sidecar, so we `up`+`exec` (not the run-path's one-off
        # `run`). The relay shares the runner's netns and survives the
        # ~12s macOS/Colima idle-drop on a paused session.
        subprocess.run([*base, "up", "-d", "--wait"], env=ctx.env, check=True)
        # Forward the active setup_file into the runner so _cases() collects
        # only this group's scenarios and the id suffix matches the -k select.
        setup_env = ["-e", f"CONNTEST_SETUP_FILE={setup_file}"] if setup_file else []
        exec_cmd = [
            *base,
            "exec",
            "-T",
            "-e",
            f"CONNTEST_PYTEST_ARGS={pytest_quoted}",
            *setup_env,
            "runner",
            "bash",
            "-c",
            _debug_runner_bash(),
        ]
        return subprocess.run(exec_cmd, env=ctx.env, check=False).returncode
    finally:
        _log(f"debug: down -v ({project})")
        subprocess.run([*base, "down", "-v"], env=ctx.env, check=False)


def _debug_detach(args: argparse.Namespace, port: int) -> int:
    """Spawn the foreground debug flow detached, poll its log, exit 0 once attachable.

    Polls the spawned process's log for the lldb-server banner. This is the
    CLion 'before launch' hook — CLion SIGTERMs its own process group, so the
    child must be in a new session (start_new_session) to survive.
    """
    # A deterministic, port-keyed path the CLion run-config and `_banner_ready`
    # both read; not a randomized tempfile by design.
    log_file = Path(f"/tmp/conntest-debug-{port}.log")  # noqa: S108  # see comment above
    fg_argv = [
        sys.executable,
        str(_CONNTEST_EXE),
        "debug",
        args.scenario or "round_trip",
        "--plugin",
        args.plugin,
        "--port",
        str(port),
        "--build-dir",
        _resolve_build_dir(args.build_dir),
    ]
    if args.image:
        fg_argv += ["--image", args.image]
    if args.selector:
        fg_argv += ["--selector", args.selector]

    log = log_file.open("w", encoding="utf-8")
    proc = subprocess.Popen(
        fg_argv,
        stdin=subprocess.DEVNULL,
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    _log(f"debug: detached (pid={proc.pid} log={log_file})")
    _log("debug: waiting for lldb-server (cold start can take ~30s)…")
    for i in range(120):
        if proc.poll() is not None:
            _log("debug: background process exited before lldb-server started")
            _tail(log_file, 20)
            return proc.returncode or 1
        if _banner_ready(log_file, port):
            time.sleep(1)
            _log(f"debug: lldb-server ready on {port} after ~{i}s — CLion can attach")
            return 0
        time.sleep(1)
    _log(f"debug: port {port} did not come up within 120s")
    _tail(log_file, 30)
    proc.terminate()
    return 1


def _banner_ready(log_file: Path, port: int) -> bool:
    try:
        for line in log_file.read_text(encoding="utf-8", errors="replace").splitlines():
            if "gdbserver listening on" in line and line.rstrip().endswith(f":{port}"):
                return True
    except FileNotFoundError:
        pass
    return False


def _tail(path: Path, n: int) -> None:
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        for line in lines[-n:]:
            print(f"  {line}", file=sys.stderr)
    except FileNotFoundError:
        pass


def _find_plugin_compose(plugin: str) -> Path | None:
    repo = _repo_root()
    for kind in ("Sources", "Sinks"):
        cand = repo / "nes-plugins" / kind / plugin / "conn-test" / "compose.yaml"
        if cand.is_file():
            return cand
    return None


# ---------------------------------------------------------------------------
# Subcommand "list"
# ---------------------------------------------------------------------------
def _cmd_list(_args: argparse.Namespace) -> int:
    repo = _repo_root()
    composes = _find_plugin_composes(repo)
    loaders = sorted((repo / "nes-plugins").rglob("conn-test/loader.py"))
    by_dir = {c.parent: c for c in composes}
    if not loaders:
        _log("no conn-test plugins found under nes-plugins/")
        return 0
    for loader in loaders:
        ct = loader.parent
        name = ct.parent.name
        kind = ct.parent.parent.name
        scenarios = _scenarios_via_ast(ct)
        compose = by_dir.get(ct)
        if compose is not None:
            # Deferred: discovery pulls in the runner's third-party deps the host python may lack.
            from conntest_runner.discovery import _compose_service_names  # noqa: PLC0415

            services = sorted(_compose_service_names(compose))
            svc = f"services={services}"
        else:
            svc = "docker-free"
        print(f"{kind}/{name}: {svc} scenarios={scenarios}")
    return 0


def _scenarios_via_ast(conn_test_dir: Path) -> list[str]:  # noqa: C901  # single AST walk
    """Read the SCENARIOS list without importing the loader.

    Importing the loader would pull in the runner's third-party deps the host
    python may lack. Prefers a sibling scenarios.py, then a class/module-level
    SCENARIOS literal.
    """

    def _from_file(path: Path) -> list[str] | None:
        if not path.is_file():
            return None
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:
            return None
        for node in ast.walk(tree):
            # Plain `SCENARIOS = [...]` (ast.Assign) or annotated
            # `SCENARIOS: ClassVar[list[str]] = [...]` (ast.AnnAssign).
            targets = []
            value = None
            if isinstance(node, ast.Assign):
                targets, value = node.targets, node.value
            elif isinstance(node, ast.AnnAssign):
                targets, value = [node.target], node.value
            if value is None:
                continue
            for tgt in targets:
                if isinstance(tgt, ast.Name) and tgt.id == "SCENARIOS":
                    try:
                        return list(ast.literal_eval(value))
                    except (ValueError, SyntaxError):
                        return None
        return None

    return (
        _from_file(conn_test_dir / "scenarios.py") or _from_file(conn_test_dir / "loader.py") or []
    )


# ---------------------------------------------------------------------------
# Subcommand: install-clion (replaces setup-clion-debug.sh)
# ---------------------------------------------------------------------------
def _cmd_install_clion(args: argparse.Namespace) -> int:
    return _install_clion(quiet=args.quiet)


def _install_clion(*, quiet: bool) -> int:
    write_root = _repo_root()
    host_root = os.environ.get("HOST_NEBULASTREAM_ROOT", "").strip() or str(write_root)
    out_dir = write_root / ".idea" / "runConfigurations"
    debug_xml = out_dir / "ConnTest_Debug.xml"
    launcher_xml = out_dir / "ConnTest_Debug_Launcher.xml"
    run_xml = out_dir / "ConnTest.xml"

    conntest = f"{host_root}/nes-conn-test/runner/conntest"
    build_dir_name = _resolve_build_dir(repo_root=write_root)
    symbol_file = f"{host_root}/{build_dir_name}/nes-conn-test/conntest-harness"

    stale = [
        out_dir / "ConnTest_Debug_selfcheck.xml",
        # Pre-rename filenames (ConnTest Source Smoke / Debug ConnTest
        # (source) / before-launch helper); remove so CLion does not show
        # the old configs alongside the renamed ones.
        out_dir / "ConnTest_Debug_source.xml",
        out_dir / "ConnTest_Debug_BeforeLaunch.xml",
        out_dir / "ConnTest_Source_Smoke.xml",
        # Must stay last: _clean_stale rmdir's this entry's parent dir.
        write_root / ".idea" / "tools" / "External Tools.xml",
    ]

    debug_body = _CLION_DEBUG_XML.format(symbol_file=symbol_file)
    launcher_body = _CLION_LAUNCHER_XML.format(conntest=conntest)
    run_body = _CLION_RUN_XML.format(conntest=conntest)

    targets = (
        (debug_xml, debug_body),
        (launcher_xml, launcher_body),
        (run_xml, run_body),
    )
    # Idempotency: skip the write when every file already byte-matches what
    # we would generate AND is well-formed (CMake calls us on every
    # configure). Exact match auto-detects any template change — these
    # configs are generated and marked "do not edit", so we do not try to
    # preserve GUI edits to them. The wellformedness guard stops a
    # previously-broken file from being cached as "up to date" (CLion
    # silently drops malformed run-configs from its dropdown).
    if all(
        p.is_file() and p.read_text(encoding="utf-8") == body and _xml_wellformed(p)
        for p, body in targets
    ):
        _clean_stale(stale, quiet=quiet)
        if not quiet:
            print("install-clion: generated XMLs already up to date")
        return 0

    out_dir.mkdir(parents=True, exist_ok=True)
    for path, body in targets:
        path.write_text(body, encoding="utf-8")
        if not _xml_wellformed(path):
            _die(f"install-clion: generated XML is not well-formed: {path}")
        if not quiet:
            print(f"install-clion: wrote {path}")

    _clean_stale(stale, quiet=quiet)
    if not quiet:
        print(f"install-clion: host repo path = {host_root}")
    return 0


def _xml_wellformed(path: Path) -> bool:
    try:
        # Input is our own freshly-generated run-config (fixed template), not
        # untrusted external XML, so the XXE/billion-laughs concern S318 guards
        # against does not apply and defusedxml is not a project dependency.
        minidom.parseString(path.read_text(encoding="utf-8"))  # noqa: S318  # see comment above
    except Exception:  # noqa: BLE001
        return False
    else:
        return True


def _clean_stale(stale: list[Path], *, quiet: bool) -> None:
    for p in stale:
        if p.is_file():
            p.unlink()
            if not quiet:
                print(f"install-clion: removed stale {p}")
    tools = stale[-1].parent if stale else None
    if tools is not None and tools.is_dir():
        with contextlib.suppress(OSError):
            tools.rmdir()


# CLion run-config XML templates. Comments must NOT contain consecutive
# hyphens (IntelliJ silently drops run-configs whose XML fails to parse).
_CLION_DEBUG_XML = """<component name="ProjectRunConfigurationManager">
  <!--
    Conn-test remote debug (source or sink). GENERATED by
    `conntest install-clion`. Do not edit directly; edit the template in
    src/conntest_runner/cli.py and reconfigure cmake (which invokes the
    generator automatically).

    Click Debug here: the Before Launch step runs the sibling
    "ConnTest Debug Launcher" config, which invokes
    `conntest debug` in detach mode to spawn the dev container, broker, and
    lldb-server on tcp:127.0.0.1:2345, then returns once the listener is
    ready so CLion can attach. kind="LLDB" pairs the lldb client with
    lldb-server (the GDB client mishandles lldb-server's aarch64 register
    packet). The CMake build applies -fdebug-prefix-map so gutter
    breakpoints resolve with no path mapping.
    NB: XML comments must not contain double hyphens.
  -->
  <configuration default="false"
                 name="ConnTest Debug"
                 type="CLion_Remote"
                 version="1"
                 remoteCommand="connect://127.0.0.1:2345"
                 symbolFile="{symbol_file}"
                 sysroot=""
                 useSudo="false">
    <debugger kind="LLDB" isBundled="true" />
    <method v="2">
      <option name="RunConfigurationTask"
              enabled="true"
              run_configuration_name="ConnTest Debug Launcher"
              run_configuration_type="ShConfigurationType" />
    </method>
  </configuration>
</component>
"""

_CLION_LAUNCHER_XML = """<component name="ProjectRunConfigurationManager">
  <!--
    Conn-test debug Before Launch helper. GENERATED by
    `conntest install-clion`. Wraps `conntest debug` (detach mode) as a
    Shell Script run config so "ConnTest Debug" can chain it via
    RunConfigurationTask. The user typically does not run this directly.

    SCRIPT_TEXT below is intentionally generic (no scenario/plugin pinned):
    clicking Debug as-shipped fails fast with on-screen instructions rather
    than silently debugging one hardcoded connector. To target a real run,
    edit SCRIPT_TEXT the same way you would the "ConnTest" config: it takes
    the regular `conntest debug` args. The first positional is the scenario
    short name (round_trip, empty, config, bad_endpoint, reconnect, …) and
    the plugin flag selects the connector. The chosen connector's setup_file
    (e.g. mosquitto.conf) is mounted automatically. Remember to point
    breakpoints at that connector's source. A reconfigure regenerates this
    file from the template, so to keep a custom selection across configures
    duplicate the config in CLion and edit the copy.
    NB: XML comments must not contain consecutive hyphens; the flags
    themselves live in SCRIPT_TEXT below.
  -->
  <configuration default="false"
                 name="ConnTest Debug Launcher"
                 type="ShConfigurationType">
    <option name="SCRIPT_TEXT"
            value="exec &quot;{conntest}&quot; debug --detach" />
    <option name="INDEPENDENT_SCRIPT_PATH" value="true" />
    <option name="SCRIPT_PATH" value="" />
    <option name="SCRIPT_OPTIONS" value="" />
    <option name="INDEPENDENT_SCRIPT_WORKING_DIRECTORY" value="true" />
    <option name="SCRIPT_WORKING_DIRECTORY" value="$PROJECT_DIR$" />
    <option name="INDEPENDENT_INTERPRETER_PATH" value="true" />
    <option name="INTERPRETER_PATH" value="/bin/bash" />
    <option name="INTERPRETER_OPTIONS" value="" />
    <option name="EXECUTE_IN_TERMINAL" value="false" />
    <option name="EXECUTE_SCRIPT_FILE" value="false" />
    <envs />
    <method v="2" />
  </configuration>
</component>
"""

_CLION_RUN_XML = """<component name="ProjectRunConfigurationManager">
  <!--
    Conn-test battery runner. GENERATED by
    `conntest install-clion`. Click Run to build conntest-harness in the
    runner then execute the conformance battery (the `run` subcommand with
    the build flag, in SCRIPT_TEXT below). Edit that command (or duplicate
    the config in CLion) to append a scenario selector such as round_trip.
    NB: XML comments must not contain consecutive hyphens.
  -->
  <configuration default="false"
                 name="ConnTest"
                 type="ShConfigurationType">
    <option name="SCRIPT_TEXT" value="exec &quot;{conntest}&quot; run --build" />
    <option name="INDEPENDENT_SCRIPT_PATH" value="true" />
    <option name="SCRIPT_PATH" value="" />
    <option name="SCRIPT_OPTIONS" value="" />
    <option name="INDEPENDENT_SCRIPT_WORKING_DIRECTORY" value="true" />
    <option name="SCRIPT_WORKING_DIRECTORY" value="$PROJECT_DIR$" />
    <option name="INDEPENDENT_INTERPRETER_PATH" value="true" />
    <option name="INTERPRETER_PATH" value="/bin/bash" />
    <option name="INTERPRETER_OPTIONS" value="" />
    <option name="EXECUTE_IN_TERMINAL" value="true" />
    <option name="EXECUTE_SCRIPT_FILE" value="false" />
    <envs />
    <method v="2" />
  </configuration>
</component>
"""


# ---------------------------------------------------------------------------
# Argument parsing + dispatch
# ---------------------------------------------------------------------------
def _split_pytest_passthrough(argv: list[str]) -> tuple[list[str], list[str]]:
    """Split on the first bare ``--``: everything after it is forwarded to pytest.

    Mirrors the deleted clion-source-smoke.sh's behaviour.
    """
    if "--" in argv:
        i = argv.index("--")
        return argv[:i], argv[i + 1 :]
    return argv, []


def _make_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="conntest", description=__doc__.splitlines()[0])
    sub = p.add_subparsers(dest="cmd", required=True)

    run = sub.add_parser("run", help="run the battery (or one scenario)")
    run.add_argument(
        "selector",
        nargs="?",
        default="",
        help="scenario short name (round_trip, empty, …) or a raw pytest -k expression",
    )
    run.add_argument("-k", dest="k", default="", help="pytest -k expression")
    run.add_argument(
        "--plugin",
        default=None,
        help="run only this connector's stack + tests"
        "skips bringing up every other plugin's "
        "docker infrastructure",
    )
    run.add_argument(
        "--build", action="store_true", help="build conntest-harness in the runner before pytest"
    )
    run.add_argument("--build-dir", default=None, help="build dir name under the repo root")
    run.add_argument("--image", default=None, help="dev image for the runner")

    ci = sub.add_parser("ci", help="run exactly what CI runs (all plugins + meta)")
    ci.add_argument("--build-dir", default=None)
    ci.add_argument("--image", default=None)

    cov = sub.add_parser("coverage", help="instrumented battery + ccov list wiring")
    cov.add_argument("--build-dir", default=None)
    cov.add_argument("--image", default=None)
    cov.add_argument(
        "--ccov-dir", default=None, help="CMAKE_COVERAGE_OUTPUT_DIRECTORY (profraw/binaries lists)"
    )
    cov.add_argument("--harness", default=None, help="path to the conntest-harness binary")

    dbg = sub.add_parser("debug", help="run one test under lldb-server for CLion")
    dbg.add_argument("scenario", nargs="?", default="round_trip")
    dbg.add_argument(
        "--plugin",
        help="connector to debug (e.g. MQTTSource); required — the launcher "
        "errors with setup instructions when omitted",
    )
    dbg.add_argument(
        "--selector", default="", help="full pytest selector (overrides scenario/plugin)"
    )
    dbg.add_argument("--port", type=int, default=_DEFAULT_DEBUG_PORT)
    dbg.add_argument(
        "--detach",
        action="store_true",
        help="spawn detached, wait for the lldb banner, exit 0 (CLion before-launch)",
    )
    dbg.add_argument("--build-dir", default=None)
    dbg.add_argument("--image", default=None)

    sub.add_parser("list", help="list discovered plugins x scenarios")

    icl = sub.add_parser("install-clion", help="(re)write the CLion run configs")
    icl.add_argument("--quiet", action="store_true")

    return p


def main(argv: list[str] | None = None) -> int:
    """Parse argv and dispatch to the selected subcommand handler."""
    raw = list(sys.argv[1:] if argv is None else argv)
    pre, post = _split_pytest_passthrough(raw)
    args = _make_parser().parse_args(pre)
    args.pytest_extra = post

    # `list` and `install-clion` are pure host-side file ops — no compose,
    # so no self-wrap needed (and they must work with the host python).
    if args.cmd == "list":
        return _cmd_list(args)
    if args.cmd == "install-clion":
        return _cmd_install_clion(args)

    _maybe_self_wrap(raw)  # no-op unless the host lacks `docker compose`

    if args.cmd == "run":
        return _cmd_run(args, ci=False)
    if args.cmd == "ci":
        args.selector = ""
        args.k = ""
        args.build = False
        return _cmd_run(args, ci=True)
    if args.cmd == "coverage":
        return _cmd_coverage(args)
    if args.cmd == "debug":
        return _cmd_debug(args)
    _die(f"unknown command {args.cmd!r}")  # NoReturn — argparse already guards the command set
