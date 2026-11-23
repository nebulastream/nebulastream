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

A deadline per test kills a harness that is too stuck to honor its own step timeout.
It is the only timer outside the harness.
"""

from __future__ import annotations

import contextlib
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import IO, TYPE_CHECKING, Any, Self

from conntest_runner.protocol import Got, HarnessError, read_reply

if TYPE_CHECKING:
    from conntest_runner.datamodel import NativeColumn

_ACCEPT_TIMEOUT_S = 30.0
_ACCEPT_POLL_S = 0.1
# The harness's step timeout, a constant in nes-conn-test/driver/Reply.hpp, from which the timeouts below are derived.
# BIND fails when the harness reports another value, so a change to only one of them fails every test.
# It is in milliseconds, as the harness reports it, and the timeouts below are in seconds, as Python's waits take them.
_HARNESS_STEP_TIMEOUT_MS = 30_000
# Four times the step timeout, because a stuck OPEN takes twice the step timeout.
# So a stuck step replies with the harness's StepTimeout, which says which step, before the deadline kills the harness.
_TEST_DEADLINE_S = 4 * _HARNESS_STEP_TIMEOUT_MS / 1000
# Above the step timeout, because on EOF the harness stops a running source with that timeout.
_END_SESSION_TIMEOUT_S = _HARNESS_STEP_TIMEOUT_MS / 1000 + 10.0


@dataclass(frozen=True)
class FillResult:
    """``observed`` is the harness's ``--observed-path`` file, empty when no path was given."""

    got: Got
    observed: bytes = b""


class _HarnessProc:
    def __init__(
        self,
        argv: list[str],
        server: socket.socket,
        *,
        log: IO[bytes] | None = None,
        accept_timeout: float = _ACCEPT_TIMEOUT_S,
    ) -> None:
        # Without a log, the harness writes to the runner's stdout and stderr.
        stderr = subprocess.STDOUT if log is not None else None
        self.proc = subprocess.Popen(argv, stdout=log, stderr=stderr)  # noqa: S603  # argv is runner-built, not untrusted input
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
        """The harness's session loop returns on EOF, so the harness shuts down gracefully.

        A harness that is still running after the timeout is killed, and the error says so.
        """
        with contextlib.suppress(OSError):
            self.conn.close()
        try:
            self.proc.wait(timeout=_END_SESSION_TIMEOUT_S)
        except subprocess.TimeoutExpired as e:
            self.proc.kill()
            self.proc.wait()
            raise HarnessError(
                name="ExitTimeout",
                phase="end",
                message=f"the harness did not exit within {_END_SESSION_TIMEOUT_S} s after EOF and was killed",
            ) from e


class _Stepper:
    def __init__(
        self,
        *,
        harness: Path,
        mode: str,
        config: Path,
        extra_args: list[str] | None = None,
        log_path: Path | None = None,
    ) -> None:
        """``log_path`` collects the output of every harness process of the case, a respawned one included."""
        self._log = log_path.open("ab") if log_path is not None else None
        self._inferior_argv = [
            str(harness),
            f"--mode={mode}",
            f"--config={config}",
            *(extra_args or []),
        ]
        # The socket lives outside the test's tmp_path, which can exceed the 108-byte AF_UNIX path limit.
        # It is the directory's only file, so cleanup removes the directory.
        self._dir = Path(tempfile.mkdtemp(prefix="conntest-"))
        self._sock_path = self._dir / f"ctl-{uuid.uuid4().hex}.sock"
        self._server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._server.bind(str(self._sock_path))
        self._server.listen(1)
        self._proc: _HarnessProc | None = None
        self._step_timeout_ms: int | None = None
        # Whether a BIND succeeded, after which a respawned harness gets one before its first step.
        self._bound = False
        self._deadline = threading.Timer(_TEST_DEADLINE_S, self._on_deadline)
        self._deadline.daemon = True
        self._deadline.start()

    def _on_deadline(self) -> None:
        if self._proc is not None:
            self._proc.kill()

    def _spawn(self) -> None:
        argv = [*self._inferior_argv, f"--control-sock={self._sock_path}"]
        self._proc = _HarnessProc(argv, self._server, log=self._log)

    def _command(self, line: str) -> Got:
        proc = self._proc
        if proc is None:
            self._spawn()
            proc = self._proc
            assert proc is not None  # noqa: S101  # _spawn() sets _proc or raises; narrows the type
            # A respawned harness has a new session, which fails an OPEN or START before BIND.
            if self._bound and line != "BIND":
                proc.command("BIND")
        return proc.command(line)

    def bind(self) -> list[NativeColumn]:
        """The schema that the engine bound from the config, which the rows are generated for."""
        got = self._command("BIND")
        # The deadlines are derived from the runner's copy of the step timeout, so they are wrong for a harness with another value.
        if got.step_timeout_ms != _HARNESS_STEP_TIMEOUT_MS:
            raise HarnessError(
                name="HarnessProtocol",
                phase="bind",
                message=f"the harness's step timeout is {got.step_timeout_ms} ms, "
                f"but the runner's copy in harness.py is {_HARNESS_STEP_TIMEOUT_MS} ms; change both",
            )
        self._step_timeout_ms = got.step_timeout_ms
        schema = list(got.schema)
        # Every row of an empty schema is `{}`, so every comparison would pass without checking a value.
        if not schema:
            raise HarnessError(name="HarnessProtocol", phase="bind", message="the BIND reply has no schema")
        self._bound = True
        return schema

    @property
    def step_timeout_s(self) -> float:
        """The harness's step timeout, so that a step that the runner runs itself waits as long as one of the harness."""
        if self._step_timeout_ms is None:
            raise RuntimeError("the harness reports its step timeout at BIND, which has not run")
        return self._step_timeout_ms / 1000

    def kill(self) -> None:
        """Models an ungraceful worker death: the backend sees the connection drop without a CLOSE.

        The next command spawns a new process, which connects to the control socket that is still listening.
        """
        if self._proc is not None:
            self._proc.kill()
            self._proc = None

    def cleanup(self) -> None:
        self._deadline.cancel()
        try:
            if self._proc is not None:
                self._proc.end_session()
        finally:
            self._proc = None
            with contextlib.suppress(OSError):
                self._server.close()
            shutil.rmtree(self._dir, ignore_errors=True)
            if self._log is not None:
                self._log.close()

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


class Sink(_Stepper):
    def __init__(self, **kw: Any) -> None:
        super().__init__(mode="sink", **kw)

    def start(self) -> Got:
        """The reply's ``n_buffers`` is how many buffers the harness made from the ``--input-path`` records."""
        return self._command("START")

    def write(self, n_buffers: int) -> Got:
        """Hands the next ``n_buffers`` buffers to the sink, or as many as are left."""
        return self._command(f"WRITE {n_buffers}")

    def stop(self) -> Got:
        return self._command("STOP")
