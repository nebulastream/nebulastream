# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Runs the C++ harness and its command session over a unix-domain control socket.

A global deadline per scenario kills a harness that is too stuck to honor its own step timeout.
It is the only timer outside the harness.
"""

from __future__ import annotations

import contextlib
import socket
import subprocess
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Self

from conntest_runner.protocol import Got, HarnessError, read_reply

_ACCEPT_TIMEOUT_S = 30.0
_ACCEPT_POLL_S = 0.1
_DEFAULT_GLOBAL_DEADLINE_S = 120.0


@dataclass(frozen=True)
class FillResult:
    """``observed`` is the harness's ``--observed-path`` file, empty when no path was given."""

    got: Got
    observed: bytes = b""


class _HarnessProc:
    def __init__(self, argv: list[str], server: socket.socket, *, accept_timeout: float = _ACCEPT_TIMEOUT_S) -> None:
        self.proc = subprocess.Popen(argv)  # noqa: S603  # argv is runner-built, not untrusted input
        # The accept polls the process, so a harness that exits before connecting fails at once and with its exit code.
        server.settimeout(_ACCEPT_POLL_S)
        deadline = time.monotonic() + accept_timeout
        while True:
            try:
                self.conn, _ = server.accept()
                break
            except TimeoutError as e:
                if (rc := self.proc.poll()) is not None:
                    raise HarnessError(
                        name="SpawnFailed",
                        phase="spawn",
                        message=f"harness exited with {rc} before connecting",
                    ) from e
                if time.monotonic() >= deadline:
                    self.proc.kill()
                    self.proc.wait()
                    raise HarnessError(
                        name="SpawnTimeout",
                        phase="spawn",
                        message=f"harness did not connect within {accept_timeout}s",
                    ) from e
        self._buf = b""

    def command(self, line: str) -> Got:
        try:
            self.conn.sendall((line + "\n").encode())
        except OSError as e:
            raise HarnessError(name="ChannelClosed", phase="send", message=f"{line!r}: {e}") from e
        while b"\n" not in self._buf:
            try:
                chunk = self.conn.recv(65536)
            except OSError as e:
                raise HarnessError(name="ChannelClosed", phase="recv", message=str(e)) from e
            if not chunk:
                rc = self.proc.poll()
                raise HarnessError(
                    name="ChannelClosed",
                    phase="recv",
                    message=f"harness closed the channel awaiting reply to {line!r} (exit={rc})",
                )
            self._buf += chunk
        raw, self._buf = self._buf.split(b"\n", 1)
        return read_reply(raw.decode())

    def kill(self) -> None:
        self.proc.kill()
        with contextlib.suppress(OSError):
            self.conn.close()
        self.proc.wait(timeout=10)

    def end_session(self) -> None:
        """The harness's session loop returns on EOF, so the harness shuts down gracefully."""
        with contextlib.suppress(OSError):
            self.conn.close()
        self.proc.wait(timeout=10)


class _Stepper:
    def __init__(
        self,
        *,
        harness: Path,
        mode: str,
        config: Path,
        extra_args: list[str] | None = None,
        global_deadline_s: float = _DEFAULT_GLOBAL_DEADLINE_S,
    ) -> None:
        self._inferior_argv = [
            str(harness),
            f"--mode={mode}",
            f"--config={config}",
            *(extra_args or []),
        ]
        self._dir = Path(tempfile.mkdtemp(prefix="conntest-"))
        self._sock_path = self._dir / f"ctl-{uuid.uuid4().hex}.sock"
        self._server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._server.bind(str(self._sock_path))
        self._server.listen(1)
        self._proc: _HarnessProc | None = None
        self._deadline = threading.Timer(global_deadline_s, self._on_deadline)
        self._deadline.daemon = True
        self._deadline.start()

    def _on_deadline(self) -> None:
        if self._proc is not None:
            self._proc.kill()

    def _spawn(self) -> None:
        argv = [*self._inferior_argv, f"--control-sock={self._sock_path}"]
        self._proc = _HarnessProc(argv, self._server)

    def _command(self, line: str) -> Got:
        if self._proc is None:
            self._spawn()
        assert self._proc is not None  # noqa: S101  # _spawn() sets _proc or raises; narrows the type
        return self._proc.command(line)

    def kill(self) -> None:
        """Models an ungraceful worker death: the backend sees the connection drop without a CLOSE.

        The next command spawns a new process, which connects to the control socket that is still listening.
        """
        if self._proc is not None:
            self._proc.kill()
            self._proc = None

    def cleanup(self) -> None:
        self._deadline.cancel()
        if self._proc is not None:
            self._proc.end_session()
            self._proc = None
        try:
            self._server.close()
            self._sock_path.unlink()
        except OSError:
            pass

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.cleanup()


class Source(_Stepper):
    def __init__(self, *, observed_path: Path | None = None, **kw: Any) -> None:
        super().__init__(mode="source", **kw)
        self._observed_path = observed_path

    def open(self) -> Got:
        return self._command("OPEN")

    def fill(self, n_rows: int, n_bytes: int) -> FillResult:
        """FILL sends both the row count and the JSONL byte length,
        because the runner does not track whether the source counts tuples or bytes.
        """
        got = self._command(f"FILL {n_rows} {n_bytes}")
        return FillResult(got=got, observed=self._read_observed())

    def close(self) -> Got:
        return self._command("CLOSE")

    def _read_observed(self) -> bytes:
        if self._observed_path is None:
            return b""
        try:
            return Path(self._observed_path).read_bytes()
        except OSError:
            return b""
