# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""selfwrap.py — the host-lacks-docker-compose fallback.

When the host has no ``docker compose`` (and we are not already in a container),
re-exec ``conntest`` once inside a one-shot dev container (which ships compose),
forwarding the docker socket as a daemon-resolvable bind mount with the right
group. A no-op on the common path.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

from conntest_runner.cli._util import _die, _log, _repo_root
from conntest_runner.cli.env import (
    _docker_sock_gid,
    _docker_sock_mount_source,
    _host_has_docker_compose,
    _in_container,
    _locate_docker_sock,
    _resolve_image,
)


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
