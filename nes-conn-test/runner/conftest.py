# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""conftest.py — pytest configuration for the conntest runner.

Registers
---------
* ``--build-dir`` CLI option — overrides the per-test build-dir selection
  for the legacy ``binary_paths`` fixture.

Build-dir resolution (``binary_paths`` fixture)
-----------------------------------------------
Two-tier selection: ``--build-dir`` flag wins; otherwise the test's
``build_kind`` parametrize value is mapped via ``BUILD_DIR_FOR_KIND``.
``test_conformance.py`` has its own ``harness_binary`` fixture and does
not use this one.

Other fixtures
--------------
* ``repo_root`` — repo root path, derived from this file's location.
* ``worker_id`` — pytest-xdist stub; returns ``"master"`` when xdist
  is not active.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

# Maps a parametrize ``build_kind`` to the dedicated build dir. ``debug``
# is the day-to-day dev build; ``coverage`` participates in ``ccov-all-export``.
BUILD_DIR_FOR_KIND: dict[str, str] = {
    "debug": "cmake-build-debug",
    "coverage": "cmake-build-conntest-coverage",
}


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--build-dir",
        action="store",
        default=None,
        metavar="DIR",
        help=(
            "CMake build directory to use for this invocation "
            "(e.g. cmake-build-debug). Overrides the per-test "
            "build_kind parametrize value when set."
        ),
    )
    parser.addoption(
        "--native-debug-port",
        action="store",
        type=int,
        default=None,
        metavar="PORT",
        help=(
            "Wrap conntest-harness under `lldb-server-19 gdbserver` "
            "listening on this TCP port. lldb-server holds the harness "
            "stopped until a debugger client (CLion Remote Debug LLDB, "
            "lldb gdb-remote) attaches and continues; the scenario watchdog "
            "is raised to 30 min automatically when this is set so a paused "
            "session is not killed. Only tests that opt in via the "
            "`native_debug` fixture honour this. Must NOT be set in CI."
        ),
    )
    parser.addoption(
        "--native-debug-host",
        action="store",
        default="0.0.0.0",  # noqa: S104  # published to host via -p; see help below
        metavar="HOST",
        help=(
            "Bind address for lldb-server (default 0.0.0.0). 0.0.0.0 is "
            "what gets published to the host via -p HOST:PORT; rarely "
            "needs changing."
        ),
    )
    parser.addoption(
        "--native-debug-bind-port",
        action="store",
        type=int,
        default=23450,
        metavar="PORT",
        help=(
            "Internal in-VM port lldb-server binds to (default 23450). "
            "Differs from --native-debug-port (the host-facing port the "
            "user attaches to) because compose.debug.yaml interposes a "
            "socat keepalive relay between them. The relay listens on "
            "--native-debug-port and forwards to this port with "
            "SO_KEEPALIVE enabled — needed because some macOS/Colima "
            "TCP forwarders drop published-port idle connections after "
            "~12s. Override only on collision with another in-runner "
            "service; must match the CONNTEST_DEBUG_BIND_PORT env "
            "compose.debug.yaml is rendered with."
        ),
    )


@dataclass
class BinaryPaths:
    build_dir: Path
    harness: Path


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _build_kind_from_params(node: pytest.Item) -> str | None:
    callspec = getattr(node, "callspec", None)
    if callspec is None:
        return None
    return callspec.params.get("build_kind")


@pytest.fixture
def binary_paths(request: pytest.FixtureRequest, repo_root: Path) -> BinaryPaths:
    """Resolve the build dir + harness binary path for the current test."""
    override = request.config.getoption("--build-dir")
    if override:
        build_dir = Path(override)
    else:
        kind = _build_kind_from_params(request.node)
        if not kind:
            pytest.skip(
                "no --build-dir given and the test is not parametrized over "
                "`build_kind`; pass --build-dir=<cmake-build-...> or "
                "parametrize the test over build_kind."
            )
        if kind not in BUILD_DIR_FOR_KIND:
            pytest.fail(
                f"unknown build_kind {kind!r}; expected one of {sorted(BUILD_DIR_FOR_KIND)}"
            )
        build_dir = repo_root / BUILD_DIR_FOR_KIND[kind]

    if not build_dir.exists():
        pytest.skip(
            f"build directory {build_dir} is not configured — "
            "see nes-conn-test/CLAUDE.md for the configure + build recipe"
        )

    harness = build_dir / "nes-conn-test" / "conntest-harness"
    if not harness.exists():
        pytest.skip(
            f"conntest-harness not built in {build_dir} — build the conntest-harness target"
        )
    return BinaryPaths(build_dir=build_dir, harness=harness)


@pytest.fixture
def native_debug(request: pytest.FixtureRequest):
    """Return a ``NativeDebugConfig`` when ``--native-debug-port`` is set, else ``None``.

    Tests opt in by accepting this fixture and forwarding the result to
    ``_run_with_seed`` / argv construction.

    The import is deferred so fixture collection works even when the
    conntest_runner package isn't importable yet (cold runner setup).
    """
    port = request.config.getoption("--native-debug-port")
    if port is None:
        return None
    from conntest_runner.harness import NativeDebugConfig  # noqa: PLC0415  # see docstring

    return NativeDebugConfig(
        host=request.config.getoption("--native-debug-host"),
        port=port,
        bind_port=request.config.getoption("--native-debug-bind-port"),
    )


@pytest.fixture
def worker_id(request: pytest.FixtureRequest) -> str:
    """Return the pytest-xdist worker id, or ``"master"`` when not parallelized."""
    workerinput = getattr(request.config, "workerinput", None)
    if workerinput is not None:
        return workerinput.get("workerid", "master")
    return "master"
