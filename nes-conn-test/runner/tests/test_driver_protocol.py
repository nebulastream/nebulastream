# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests of the error replies of the conntest-harness binary.

A connector error may be a scenario's declared outcome and a harness error never is, so the reply's origin must be right.
"""

from __future__ import annotations

import json
import socket
import subprocess
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self, cast

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import TracebackType

_TIMEOUT_S = 30.0
_EXIT_OK = 0


@pytest.fixture
def sock_dir() -> Iterator[Path]:
    # pytest's tmp_path can exceed sizeof(sun_path) (~108 bytes) on deep test ids.
    d = Path(tempfile.mkdtemp(prefix="ct-drv-", dir="/tmp"))  # noqa: S108
    yield d
    for child in d.iterdir():
        child.unlink(missing_ok=True)
    d.rmdir()


class Session:
    """The test listens on the control socket and the harness connects to it."""

    def __init__(self, harness: Path, sock_dir: Path, *, config: Path) -> None:
        sock_path = sock_dir / "control.sock"
        self._listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._listener.bind(str(sock_path))
        self._listener.listen(1)
        self.proc = subprocess.Popen(  # noqa: S603
            [str(harness), "--mode=source", f"--config={config}", f"--control-sock={sock_path}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self._listener.settimeout(_TIMEOUT_S)
        self._conn, _ = self._listener.accept()
        self._conn.settimeout(_TIMEOUT_S)
        self._reader = self._conn.makefile("rb")

    def command(self, line: str) -> dict[str, Any]:
        self._conn.sendall(line.encode("utf-8") + b"\n")
        reply = self._reader.readline()
        assert reply.endswith(b"\n"), f"control channel closed mid-reply: {reply!r}"
        return cast("dict[str, Any]", json.loads(reply))

    def end(self) -> int:
        """Closing the control connection ends the harness."""
        self._reader.close()
        self._conn.close()
        self._listener.close()
        self.proc.communicate(timeout=_TIMEOUT_S)
        return self.proc.returncode

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.communicate(timeout=_TIMEOUT_S)


def test_harness_error_is_replayed(harness_binary: Path, sock_dir: Path, tmp_path: Path) -> None:
    # The error is cached and replayed, not produced by reading the config again.
    missing = tmp_path / "never-written.nesql"
    with Session(harness_binary, sock_dir, config=missing) as session:
        first = session.command("BIND")
        assert first["ok"] is False, first
        assert first["origin"] == "harness", first
        assert first["name"] == "HarnessIO", first
        assert session.command("BIND") == first
        assert session.command("OPEN") == first
        assert session.end() == _EXIT_OK


def test_error_reply_is_one_json_line_whatever_the_message(
    harness_binary: Path, sock_dir: Path, tmp_path: Path
) -> None:
    # The bind error contains the config path, and parsing the reply line is the assertion.
    weird = tmp_path / 'we"ird\\path\b\f\n\r\t\x01.nesql'
    with Session(harness_binary, sock_dir, config=weird) as session:
        reply = session.command("BIND")
        assert reply["ok"] is False, reply
        for char in ('"', "\\", "\b", "\f", "\n", "\r", "\t", "\x01"):
            assert char in reply["message"], f"missing {char!r} in {reply['message']!r}"
        assert session.end() == _EXIT_OK
