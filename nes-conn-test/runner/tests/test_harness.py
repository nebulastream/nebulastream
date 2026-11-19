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


def test_end_session_kills_a_harness_that_does_not_exit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Only the kill ends the fake harness, so the timeout's value changes how long the test takes, not its outcome.
    monkeypatch.setattr(harness, "_END_SESSION_TIMEOUT_S", 0.1)
    fake = tmp_path / "fake-harness"
    fake.write_text(f"#!{sys.executable}\n{_NEVER_EXITS}", encoding="utf-8")
    fake.chmod(fake.stat().st_mode | stat.S_IXUSR)
    source = harness.Source(harness=fake, config=tmp_path / "unused.nesql")
    source.open()
    proc = source._proc.proc  # noqa: SLF001
    sock_dir = source._dir  # noqa: SLF001
    with pytest.raises(HarnessError, match="ExitTimeout"):
        source.cleanup()
    # The rest of the cleanup ran although the harness had to be killed.
    assert proc.returncode == -signal.SIGKILL
    assert not sock_dir.exists()
