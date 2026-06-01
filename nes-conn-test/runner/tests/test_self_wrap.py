# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Framework self-test for the self-wrap socket plumbing (no docker).

Covers the two pieces that differ between native-Linux and VM-backed
daemons (Colima / Docker Desktop): the bind-mount *source* selection and
the ``CONNTEST_DOCKER_GID`` override for the socket group probe.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from conntest_runner import cli

if TYPE_CHECKING:
    import pytest


def test_mount_source_prefers_var_run_when_docker_host_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No DOCKER_HOST + /var/run/docker.sock present → daemon-resolvable path."""
    monkeypatch.delenv("DOCKER_HOST", raising=False)
    monkeypatch.setattr(Path, "is_socket", lambda self: str(self) == "/var/run/docker.sock")
    # The located client endpoint is the unmountable colima host path.
    assert (
        cli._docker_sock_mount_source("/Users/me/.colima/default/docker.sock")
        == "/var/run/docker.sock"
    )


def test_mount_source_honors_explicit_docker_host(monkeypatch: pytest.MonkeyPatch) -> None:
    """An explicit DOCKER_HOST means the user picked a daemon — keep its endpoint."""
    monkeypatch.setenv("DOCKER_HOST", "unix:///custom/docker.sock")
    monkeypatch.setattr(Path, "is_socket", lambda _self: True)
    assert cli._docker_sock_mount_source("/custom/docker.sock") == "/custom/docker.sock"


def test_mount_source_falls_back_without_var_run(monkeypatch: pytest.MonkeyPatch) -> None:
    """Rootless Linux: no /var/run/docker.sock → use the located endpoint."""
    monkeypatch.delenv("DOCKER_HOST", raising=False)
    monkeypatch.setattr(Path, "is_socket", lambda _self: False)
    endpoint = "/run/user/1000/docker.sock"
    assert cli._docker_sock_mount_source(endpoint) == endpoint


def test_gid_env_override_skips_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    """CONNTEST_DOCKER_GID short-circuits the docker probe entirely."""
    monkeypatch.setenv("CONNTEST_DOCKER_GID", "991")

    def _fail(*_a: object, **_k: object) -> None:
        raise AssertionError("probe must not run when the gid is overridden")

    monkeypatch.setattr("conntest_runner.cli.subprocess.run", _fail)
    assert cli._docker_sock_gid("img", "/var/run/docker.sock", fallback=20) == 991
