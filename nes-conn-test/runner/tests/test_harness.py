# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for the runner's side of a harness session, against a fake harness."""

from __future__ import annotations

import signal
import stat
import sys
from typing import TYPE_CHECKING

import pytest

from conntest_runner import harness
from conntest_runner.protocol import HarnessError

if TYPE_CHECKING:
    from pathlib import Path

# Replies to every command and never exits after EOF, like a harness whose stop at EOF does not end.
_NEVER_EXITS = """
import signal, socket, sys
path = next(arg.split("=", 1)[1] for arg in sys.argv if arg.startswith("--control-sock="))
conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
conn.connect(path)
for _ in conn.makefile("rb"):
    conn.sendall(b'{"ok":true,"phase":"open"}\\n')
signal.pause()
"""


# Replies to every command with a BIND reply whose step timeout is 60 s, and exits after EOF.
_STEP_TIMEOUT_60_S = """
import socket, sys
path = next(arg.split("=", 1)[1] for arg in sys.argv if arg.startswith("--control-sock="))
conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
conn.connect(path)
for _ in conn.makefile("rb"):
    conn.sendall(b'{"ok":true,"phase":"bind","schema":[{"name":"ID","type":"INT32","nullable":false}],"step_timeout_ms":60000}\\n')
"""


# Appends every command to `<config>.commands`, replies to BIND with a BIND reply and to any other command with an OK, and exits after EOF.
_LOGS_COMMANDS = """
import socket, sys
args = dict(arg.split("=", 1) for arg in sys.argv[1:] if "=" in arg)
conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
conn.connect(args["--control-sock"])
for line in conn.makefile("rb"):
    with open(args["--config"] + ".commands", "ab") as log:
        log.write(line)
    if line.startswith(b"BIND"):
        conn.sendall(b'{"ok":true,"phase":"bind","schema":[{"name":"ID","type":"INT32","nullable":false}],"step_timeout_ms":30000}\\n')
    else:
        conn.sendall(b'{"ok":true,"phase":"open"}\\n')
"""


def _fake_harness(tmp_path: Path, script: str) -> Path:
    fake = tmp_path / "fake-harness"
    fake.write_text(f"#!{sys.executable}\n{script}", encoding="utf-8")
    fake.chmod(fake.stat().st_mode | stat.S_IXUSR)
    return fake


def test_end_session_kills_a_harness_that_does_not_exit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Only the kill ends the fake harness, so the timeout's value changes how long the test takes, not its outcome.
    monkeypatch.setattr(harness, "_END_SESSION_TIMEOUT_S", 0.1)
    fake = _fake_harness(tmp_path, _NEVER_EXITS)
    source = harness.Source(harness=fake, config=tmp_path / "unused.nesql")
    source.open()
    proc = source._proc.proc  # noqa: SLF001
    sock_dir = source._dir  # noqa: SLF001
    with pytest.raises(HarnessError, match="ExitTimeout"):
        source.cleanup()
    # The rest of the cleanup ran although the harness had to be killed.
    assert proc.returncode == -signal.SIGKILL
    assert not sock_dir.exists()


def test_bind_fails_when_the_harness_has_another_step_timeout(tmp_path: Path) -> None:
    source = harness.Source(harness=_fake_harness(tmp_path, _STEP_TIMEOUT_60_S), config=tmp_path / "unused.nesql")
    with pytest.raises(HarnessError, match="step timeout is 60000 ms, but the runner's copy in harness.py is 30000 ms"):
        source.bind()
    source.cleanup()


def test_a_respawned_harness_gets_a_bind_before_its_first_step(tmp_path: Path) -> None:
    # crash_recovery kills the harness, and the new process's session fails an OPEN before BIND.
    config = tmp_path / "unused.nesql"
    source = harness.Source(harness=_fake_harness(tmp_path, _LOGS_COMMANDS), config=config)
    source.bind()
    source.kill()
    source.open()
    source.cleanup()
    assert (tmp_path / "unused.nesql.commands").read_text(encoding="utf-8").split() == ["BIND", "BIND", "OPEN"]
