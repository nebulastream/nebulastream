# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""env.py — host environment probing for the conntest CLI.

Docker daemon / socket discovery (used by the self-wrap fallback), dev-image
resolution, and build-dir autodetection + resolution. The single source of
truth for "where does this run and against which image / build dir".
"""

from __future__ import annotations

import os
import re
from pathlib import Path

from conntest_runner.cli._util import _capture, _die, _repo_root

_DEFAULT_IMAGE = "nebulastream/nes-development:local"
_DEFAULT_BUILD_DIR = "cmake-build-debug"


def _in_container() -> bool:
    """True when we are already inside a container (so do not self-wrap)."""
    if os.environ.get("CONNTEST_IN_CONTAINER") == "1":
        return True
    return Path("/.dockerenv").exists()


def _host_has_docker_compose() -> bool:
    proc = _capture(["docker", "compose", "version"])
    return proc is not None and proc.returncode == 0


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
    proc = _capture(["docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}"])
    if proc is not None:
        ctx = proc.stdout.strip()
        if ctx.startswith("unix://"):
            cand = ctx[len("unix://") :]
            if Path(cand).is_socket():
                return cand
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
    proc = _capture(
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
    )
    if proc is not None and (out := proc.stdout.strip()).isdigit():
        return int(out)
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
    proc = _capture(
        [
            "docker",
            "images",
            "nebulastream/nes-development",
            "--format",
            "{{.Repository}}:{{.Tag}}",
        ],
    )
    if proc is None:
        return None
    for line in proc.stdout.splitlines():
        stripped = line.strip()
        if stripped and not stripped.endswith(":<none>"):
            return stripped
    return None


def _image_present(image: str) -> bool:
    proc = _capture(["docker", "image", "inspect", image])
    return proc is not None and proc.returncode == 0


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
