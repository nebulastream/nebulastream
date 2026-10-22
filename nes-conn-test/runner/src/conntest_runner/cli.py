# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The ``conntest`` command.

``run`` starts each connector's compose stack, runs pytest in a runner container on the stack's network,
and tears the stack down.
A connector without a ``compose.yaml`` gets a stack of only the runner.
This module uses only the stdlib, because pytest and the connectors' dependencies are installed in the runner.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import uuid
from pathlib import Path
from typing import NoReturn

from conntest_runner.discovery import read_plugin_scenarios, registered_connectors

_RUNNER_DIR = Path(__file__).resolve().parents[2]
_REPO_ROOT = _RUNNER_DIR.parents[1]
_RUNNER_COMPOSE = _RUNNER_DIR / "runner.compose.yaml"

# pytest's exit code for "no tests collected".
# A user selector that deselects every test of one connector is not a failure.
_PYTEST_EXIT_NO_TESTS = 5

# uv runs pytest in the venv of the root pyproject.toml, which holds every connector's dependencies.
_RUN_PYTEST = ("uv", "run", "--frozen", "pytest")


def _log(msg: str) -> None:
    print(f"[conntest] {msg}", file=sys.stderr, flush=True)


def _die(msg: str) -> NoReturn:
    print(f"conntest: {msg}", file=sys.stderr, flush=True)
    raise SystemExit(2)


def _connectors(build_dir: Path) -> dict[str, Path]:
    try:
        return registered_connectors(build_dir, _REPO_ROOT)
    except FileNotFoundError as e:
        _die(str(e))


def _resolve_image(explicit: str | None) -> str:
    """Inside a development container, the runner reuses that container's image,
    so the harness runs against the toolchain that it was built with.
    The image is found through the hostname, as in cmake/CheckTestUtilities.cmake.
    """
    if image := explicit or os.environ.get("NES_DEV_IMAGE"):
        return image
    if Path("/.dockerenv").exists():
        proc = subprocess.run(
            ["docker", "inspect", "--format={{.Config.Image}}", os.uname().nodename],
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode == 0 and proc.stdout.strip():
            return proc.stdout.strip()
    _die("cannot determine the development image; pass --image or set NES_DEV_IMAGE")


def _configured_source_dir(build_dir: Path) -> str:
    """The repo path that the harness was built against."""
    cache = build_dir / "CMakeCache.txt"
    if not cache.is_file():
        _die(f"{build_dir} is not a configured CMake build dir (no CMakeCache.txt)")
    match = re.search(r"^CMAKE_HOME_DIRECTORY:[^=]*=(.*)$", cache.read_text(encoding="utf-8"), re.MULTILINE)
    if match is None:
        _die(f"no CMAKE_HOME_DIRECTORY in {cache}")
    return match.group(1).strip()


def _compose_env(image: str, build_dir: Path) -> tuple[dict[str, str], str]:
    """The runner mounts the repo at the path that the build was configured against, so compiled-in paths resolve.

    The mount source must be a path that the docker daemon can see.
    ``HOST_NEBULASTREAM_ROOT`` gives it when this container mounts the repo at a different path.
    """
    container_repo = _configured_source_dir(build_dir)
    try:
        build_rel = build_dir.resolve().relative_to(_REPO_ROOT)
    except ValueError:
        _die(f"build dir {build_dir} must be inside the repo {_REPO_ROOT}")
    env = os.environ.copy()
    env["HOST_NEBULASTREAM_ROOT"] = os.environ.get("HOST_NEBULASTREAM_ROOT") or str(_REPO_ROOT)
    env["CONNTEST_CONTAINER_REPO"] = container_repo
    env["CONNTEST_DEV_IMAGE"] = image
    env["CONNTEST_HOST_UID"] = str(os.getuid())
    env["CONNTEST_HOST_GID"] = str(os.getgid())
    return env, f"{container_repo}/{build_rel.as_posix()}"


def _data_services(base: list[str], env: dict[str, str]) -> list[str]:
    proc = subprocess.run([*base, "config", "--services"], env=env, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        _die(f"docker compose config failed:\n{proc.stderr}")
    return [s for s in proc.stdout.split() if s != "runner"]


def _run_connector(
    plugin: str,
    conn_test_dir: Path,
    *,
    env: dict[str, str],
    build_dir: str,
    pytest_args: list[str],
) -> int:
    container_conn_test_dir = f"{env['CONNTEST_CONTAINER_REPO']}/{conn_test_dir.relative_to(_REPO_ROOT).as_posix()}"
    # A connector without a compose.yaml has no service, and the stack is only the runner.
    composes = [f for f in (conn_test_dir / "compose.yaml", _RUNNER_COMPOSE) if f.is_file()]
    base = ["docker", "compose", *(a for f in composes for a in ("-f", str(f))), "-p", f"conntest-{uuid.uuid4().hex}"]
    try:
        rc = 0
        if services := _data_services(base, env):
            _log(f"{plugin}: starting the data service(s)")
            rc = subprocess.run([*base, "up", "-d", "--wait", *services], env=env, check=False).returncode
        if rc == 0:
            _log(f"{plugin}: running pytest in the runner")
            args = ["conformance/test_conformance.py", "-v", f"--build-dir={build_dir}", *pytest_args]
            run = [
                *base,
                *("run", "--rm", "-T", "--no-deps"),
                *("-e", f"CONNTEST_PLUGIN={plugin}"),
                *("-e", f"CONNTEST_DIR={container_conn_test_dir}"),
                "runner",
            ]
            rc = subprocess.run([*run, *_RUN_PYTEST, *args], env=env, check=False).returncode
            if rc == _PYTEST_EXIT_NO_TESTS and pytest_args:
                _log(f"{plugin}: no test matched the selector")
                return 0
        if rc != 0:
            _log(f"{plugin}: failed (rc={rc}); container states and logs follow")
            subprocess.run([*base, "ps", "-a"], env=env, check=False)
            subprocess.run([*base, "logs", "--no-color", "--timestamps"], env=env, check=False)
        return rc
    finally:
        subprocess.run([*base, "down", "-v"], env=env, check=False)


def _cmd_run(args: argparse.Namespace) -> int:
    connectors = _connectors(Path(args.build_dir))
    if args.plugin is not None:
        if args.plugin not in connectors:
            _die(f"--plugin {args.plugin!r} has no add_conn_test; registered: {', '.join(connectors) or 'none'}")
        connectors = {args.plugin: connectors[args.plugin]}
    image = _resolve_image(args.image)
    env, build_dir = _compose_env(image, Path(args.build_dir).absolute())
    _log(f"runner image {image}, build dir {build_dir}")
    failed = [
        plugin
        for plugin, conn_test_dir in connectors.items()
        if _run_connector(plugin, conn_test_dir, env=env, build_dir=build_dir, pytest_args=args.pytest_args) != 0
    ]
    if failed:
        _log(f"failed: {', '.join(failed)}")
        return 1
    return 0


def _cmd_list(args: argparse.Namespace) -> int:
    for plugin, conn_test_dir in _connectors(Path(args.build_dir)).items():
        names = dict.fromkeys(s.name for s in read_plugin_scenarios(conn_test_dir))
        print(f"{plugin}: {', '.join(names)}")
    return 0


def main(argv: list[str] | None = None) -> int:
    raw = list(sys.argv[1:] if argv is None else argv)
    own, pytest_args = (raw[: raw.index("--")], raw[raw.index("--") + 1 :]) if "--" in raw else (raw, [])

    parser = argparse.ArgumentParser(prog="conntest", description="run the connection-test battery")
    sub = parser.add_subparsers(dest="cmd", required=True)
    run = sub.add_parser("run", help="run the battery (all connectors, or one); `-- ARGS` go to pytest")
    run.add_argument("--build-dir", required=True, help="CMake build dir holding nes-conn-test/conntest-harness")
    run.add_argument("--plugin", default=None, help="only this connector, e.g. TCPSource")
    run.add_argument("--image", default=None, help="development image for the runner container")
    run.set_defaults(func=_cmd_run)
    listing = sub.add_parser("list", help="list the connectors and their scenarios")
    listing.add_argument("--build-dir", required=True, help="CMake build dir holding nes-conn-test/connectors.txt")
    listing.set_defaults(func=_cmd_list)

    args = parser.parse_args(own)
    args.pytest_args = pytest_args
    rc: int = args.func(args)
    return rc
