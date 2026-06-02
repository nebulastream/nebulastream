# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""harness.py — the stepper: driving the harness over a control socket.

A `Source` / `Sink` owns the harness OS process and a unix-domain control
socket. It spawns the harness lazily on the first open()/start(), reuses
it across commands, and can `kill()` it (next open()/start() respawns —
this is how reconnect-after-death works). Each scenario is wrapped in a
single **global deadline**: a watchdog thread that SIGKILLs a process so
wedged it cannot honor its own per-step timeout. That deadline is the
only timer outside the harness (design §5).

The control socket is a path the runner listens on and the harness
``connect()``s to (``--control-sock``). Because the harness opens it
itself rather than inheriting it, it survives ``lldb-server`` (which
closes inherited non-stdio fds) — so the debug path keeps working.

`NativeDebugConfig` wraps the harness under ``lldb-server-19 gdbserver``
for a CLion Remote Debug session; the control socket reaches the inferior
unchanged.
"""

from __future__ import annotations

import contextlib
import os
import socket
import subprocess
import tempfile
import threading
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Self

from conntest_runner.protocol import Got, HarnessError, read_reply

_ACCEPT_TIMEOUT_S = 30.0
_DEFAULT_GLOBAL_DEADLINE_S = 120.0


@dataclass(frozen=True)
class NativeDebugConfig:
    """Wrap the harness under ``lldb-server-19 gdbserver host:bind_port``.

    A CLion Remote Debug (LLDB) client can attach mid-session. The control
    socket the harness opens itself reaches the inferior unchanged (unlike
    the old inherited fd3/fd4 pair, which lldb-server closed).
    """

    host: str = "0.0.0.0"  # noqa: S104  # dev lldb-server bind for remote CLion attach
    port: int = 2345
    bind_port: int = 23450
    wrapper_argv: list[str] = field(default_factory=lambda: ["lldb-server-19", "gdbserver"])

    def build_argv(self, inferior_argv: list[str], *, ready_pipe: Path | None = None) -> list[str]:
        """Wrap ``inferior_argv`` under the gdbserver argv.

        ``ready_pipe`` (when given) tees the bind-ready signal so the caller
        can emit the listener banner only once the port is accepting.
        """
        cmd = list(self.wrapper_argv)
        if ready_pipe is not None:
            cmd += ["--named-pipe", str(ready_pipe)]
        cmd += [f"{self.host}:{self.bind_port}", "--", *inferior_argv]
        return cmd


@dataclass(frozen=True)
class FillResult:
    """The result of a source FILL/DRAIN step.

    Carries the reply (`got`) plus the bytes observed during the step (read
    from the harness's --observed-path, empty when the data model did not
    request them).
    """

    got: Got
    observed: bytes = b""


class _HarnessProc:
    """One harness OS process plus its accepted control connection."""

    def __init__(
        self,
        argv: list[str],
        server: socket.socket,
        *,
        accept_timeout: float = _ACCEPT_TIMEOUT_S,
        ready_pipe: Path | None = None,
        banner: str | None = None,
    ) -> None:
        self.proc = subprocess.Popen(argv)  # noqa: S603  # argv is runner-built, not untrusted input
        if ready_pipe is not None:
            # Native-debug spawn: the harness is wrapped under
            # `lldb-server gdbserver`, which binds the port and writes it to
            # this fifo, then holds the inferior STOPPED at entry until a
            # client attaches. Block on the fifo (hard sync that the port is
            # accepting), THEN emit the banner `conntest debug --detach`
            # greps for — so CLion only attaches once lldb-server is truly
            # listening. The control-socket accept() below then waits for the
            # user to attach + continue (hence the bumped accept_timeout); the
            # inferior connect()s only after it starts running.
            with ready_pipe.open(encoding="utf-8") as fp:
                fp.read()
            if banner:
                print(banner, flush=True)  # noqa: T201  # operator-facing readiness banner
        server.settimeout(accept_timeout)
        try:
            self.conn, _ = server.accept()
        except TimeoutError as e:
            self.proc.kill()
            raise HarnessError(
                name="SpawnTimeout",
                phase="spawn",
                message=f"harness did not connect within {accept_timeout}s",
            ) from e
        self._buf = b""

    def command(self, line: str) -> Got:
        """Send one command line, parse the one-line reply.

        Raises ConnectorError/HarnessError on an ok:false reply, or
        HarnessError if the channel closes (the process died — e.g. a
        global-deadline kill or a poisoned step).
        """
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
        """Close the command channel so the harness sees EOF and exits.

        The harness's session loop returns on the EOF, so it shuts down
        gracefully rather than being killed.
        """
        with contextlib.suppress(OSError):
            self.conn.close()
        self.proc.wait(timeout=10)


class _Stepper:
    """Owns a listening control socket and the lazy (re)spawn of the harness.

    Subclassed by `Source` / `Sink`, which add the per-command verbs. The
    global deadline (a watchdog thread) SIGKILLs the process if a scenario
    runs past it, then is reset/cancelled at cleanup.
    """

    def __init__(
        self,
        *,
        harness: Path,
        mode: str,
        config: Path,
        extra_args: list[str] | None = None,
        global_deadline_s: float = _DEFAULT_GLOBAL_DEADLINE_S,
        native_debug: NativeDebugConfig | None = None,
    ) -> None:
        self._inferior_argv = [
            str(harness),
            f"--mode={mode}",
            f"--config={config}",
            *(extra_args or []),
        ]
        self._native_debug = native_debug
        # A paused debug session must not trip the watchdog: bump the deadline
        # to >= 30 min when wrapping under lldb-server (matches the harness's
        # old auto-raised wall-timeout). The same value caps how long the
        # control-socket accept() waits for the user to attach + continue.
        if native_debug is not None:
            global_deadline_s = max(global_deadline_s, 30 * 60.0)
        self._deadline_s = global_deadline_s
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
            self._proc.kill()  # backstop a wedged process

    def _spawn(self) -> None:
        argv = [*list(self._inferior_argv), f"--control-sock={self._sock_path}"]
        if self._native_debug is None:
            self._proc = _HarnessProc(argv, self._server)
            return
        # Wrap under lldb-server and route its bind-ready signal through a
        # fifo so we can emit the listener banner only once it's accepting.
        ready_pipe = self._dir / f"lldb-ready-{uuid.uuid4().hex}.pipe"
        os.mkfifo(str(ready_pipe))
        argv = self._native_debug.build_argv(argv, ready_pipe=ready_pipe)
        banner = (
            f"lldb-server-19 gdbserver listening on "
            f"{self._native_debug.host}:{self._native_debug.port}"
        )
        self._proc = _HarnessProc(
            argv,
            self._server,
            accept_timeout=self._deadline_s,
            ready_pipe=ready_pipe,
            banner=banner,
        )

    def _command(self, line: str) -> Got:
        if self._proc is None:
            self._spawn()
        assert self._proc is not None  # noqa: S101  # _spawn() sets _proc or raises; narrows the type
        return self._proc.command(line)

    def kill(self) -> None:
        """SIGKILL the harness and drop the handle.

        The next open()/start() respawns a fresh process
        (reconnect-after-death).
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
    """Source-mode stepper: VALIDATE | OPEN | FILL <n> | DRAIN | CLOSE."""

    def __init__(self, *, observed_path: Path | None = None, **kw: Any) -> None:
        super().__init__(mode="source", **kw)
        self._observed_path = observed_path

    def validate(self) -> Got:
        """Run the VALIDATE step (config parse / connector validation)."""
        return self._command("VALIDATE")

    def open(self) -> Got:
        """Run the OPEN step (open the source connection)."""
        return self._command("OPEN")

    def fill(self, quota: int) -> FillResult:
        """Run a FILL step for ``quota`` tuples, returning the reply + observed bytes."""
        got = self._command(f"FILL {quota}")
        return FillResult(got=got, observed=self._read_observed())

    def drain(self) -> FillResult:
        """Run the DRAIN step (flush remaining data), returning the reply + observed bytes."""
        got = self._command("DRAIN")
        return FillResult(got=got, observed=self._read_observed())

    def close(self) -> Got:
        """Run the CLOSE step (close the source connection)."""
        return self._command("CLOSE")

    def _read_observed(self) -> bytes:
        if self._observed_path is None:
            return b""
        try:
            return Path(self._observed_path).read_bytes()
        except OSError:
            return b""


class Sink(_Stepper):
    """Sink-mode stepper: VALIDATE | START | WRITE <n> | STOP."""

    def __init__(self, **kw: Any) -> None:
        super().__init__(mode="sink", **kw)

    def validate(self) -> Got:
        """Run the VALIDATE step (config parse / connector validation)."""
        return self._command("VALIDATE")

    def start(self) -> Got:
        """Run the START step (open the sink connection).

        The reply's ``n_buffers`` is how many buffers the harness materialized
        from the input — the count the scenario steps through with WRITE.
        """
        return self._command("START")

    def write(self, buffers: int) -> Got:
        """Run a WRITE step that drains the next ``buffers`` buffers to the sink.

        One TupleBuffer is the unit (the engine's sink interface). The reply's
        ``units_written`` is the record count in those buffers; ``eos`` is whether
        the buffer stream is now exhausted.
        """
        return self._command(f"WRITE {buffers}")

    def stop(self) -> Got:
        """Run the STOP step (flush and close the sink connection)."""
        return self._command("STOP")
