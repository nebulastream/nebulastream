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
    from collections.abc import Iterator, Sequence
    from types import TracebackType

_TIMEOUT_S = 30.0
_EXIT_OK = 0
_EXIT_BAD_ARGUMENTS = 2


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

    def __init__(
        self, harness: Path, sock_dir: Path, *, config: Path, mode: str = "source", args: Sequence[str] = ()
    ) -> None:
        sock_path = sock_dir / "control.sock"
        self._listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._listener.bind(str(sock_path))
        self._listener.listen(1)
        self.proc = subprocess.Popen(  # noqa: S603
            [str(harness), f"--mode={mode}", f"--config={config}", f"--control-sock={sock_path}", *args],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.stderr = ""
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
        _, self.stderr = self.proc.communicate(timeout=_TIMEOUT_S)
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


_MAX_UINT32 = 2**32 - 1


@pytest.mark.parametrize(
    ("mode", "flag", "value", "message"),
    [
        pytest.param("source", "--buffer-size", 0, "the buffer size", id="source-buffer-size-0"),
        pytest.param("source", "--buffer-size", _MAX_UINT32 + 1, "the buffer size", id="source-buffer-size-2^32"),
        pytest.param("source", "--buffer-count", 0, "the buffer count", id="source-buffer-count-0"),
        pytest.param("source", "--buffer-count", _MAX_UINT32 + 1, "the buffer count", id="source-buffer-count-2^32"),
        pytest.param("sink", "--buffer-size", 0, "the buffer size", id="sink-buffer-size-0"),
        pytest.param("sink", "--buffer-size", _MAX_UINT32 + 1, "the buffer size", id="sink-buffer-size-2^32"),
        pytest.param("sink", "--threads", 0, "the number of worker threads", id="sink-threads-0"),
        pytest.param("sink", "--threads", _MAX_UINT32 + 1, "the number of worker threads", id="sink-threads-2^32"),
    ],
)
def test_harness_rejects_an_option_outside_its_bounds(
    harness_binary: Path, sock_dir: Path, tmp_path: Path, mode: str, flag: str, value: int, message: str
) -> None:
    # The pool takes 32-bit sizes and counts, and a buffer size of 0 divides by zero, so the harness exits before a session starts.
    config = tmp_path / "never-read.nesql"
    with Session(harness_binary, sock_dir, config=config, mode=mode, args=[f"{flag}={value}"]) as session:
        assert session.end() == _EXIT_BAD_ARGUMENTS
    assert f"error: {flag}: " in session.stderr, session.stderr
    assert f"{message} must be between 1 and {_MAX_UINT32}, got {value}" in session.stderr, session.stderr


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


_FILE_CONFIGS = {
    "source": 'CREATE LOGICAL SOURCE s (id TINYINT NOT NULL, "Label" VARSIZED);\n'
    "CREATE PHYSICAL SOURCE FOR s TYPE File SET ('/dev/null' AS \"SOURCE\".FILE_PATH, 'CSV' AS INPUT_FORMATTER.\"TYPE\");\n",
    "sink": 'CREATE SINK s (id TINYINT NOT NULL, "Label" VARSIZED) TYPE File SET '
    "('/dev/null' AS \"SINK\".FILE_PATH, 'JSON' AS \"SINK\".OUTPUT_FORMAT);\n",
}


@pytest.mark.parametrize("mode", sorted(_FILE_CONFIGS))
def test_bind_replies_with_the_schema_that_the_engine_bound(
    harness_binary: Path, sock_dir: Path, tmp_path: Path, mode: str
) -> None:
    # The alias TINYINT is bound as INT8, and the unquoted name as its canonical, uppercase form.
    config = tmp_path / "file.nesql"
    config.write_text(_FILE_CONFIGS[mode], encoding="utf-8")
    with Session(harness_binary, sock_dir, config=config, mode=mode) as session:
        reply = session.command("BIND")
        assert reply == {
            "ok": True,
            "phase": "bind",
            "schema": [
                {"name": "ID", "type": "INT8", "nullable": False},
                {"name": "Label", "type": "VARSIZED", "nullable": True},
            ],
            "step_timeout_ms": 30000,
        }, reply
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


_PHYSICAL_SOURCE = "CREATE PHYSICAL SOURCE FOR s TYPE File SET ('/dev/null' AS \"SOURCE\".FILE_PATH, 'CSV' AS INPUT_FORMATTER.\"TYPE\");\n"


def _bind_error(harness_binary: Path, sock_dir: Path, config: Path) -> tuple[object, object, object]:
    with Session(harness_binary, sock_dir, config=config) as session:
        reply = session.command("BIND")
        assert session.end() == _EXIT_OK
    return reply["ok"], reply["origin"], reply["code"]


# The binder creates the sources through the frontend's statement handlers, so it reports their error codes.
def test_bind_reports_a_logical_source_that_already_exists(harness_binary: Path, sock_dir: Path, tmp_path: Path) -> None:
    config = tmp_path / "file.nesql"
    config.write_text("CREATE LOGICAL SOURCE s (id INT32);\nCREATE LOGICAL SOURCE s (id INT32);\n" + _PHYSICAL_SOURCE, encoding="utf-8")
    assert _bind_error(harness_binary, sock_dir, config) == (False, "harness", 2031)


def test_bind_reports_a_physical_source_without_its_logical_source(harness_binary: Path, sock_dir: Path, tmp_path: Path) -> None:
    config = tmp_path / "file.nesql"
    config.write_text(_PHYSICAL_SOURCE, encoding="utf-8")
    assert _bind_error(harness_binary, sock_dir, config) == (False, "harness", 2030)


def test_sink_rejects_a_step_out_of_order(harness_binary: Path, sock_dir: Path, tmp_path: Path) -> None:
    # The order is checked before the config is read, so the config need not bind.
    config = tmp_path / "unused.nesql"
    config.write_text("-- not read\n", encoding="utf-8")
    with Session(harness_binary, sock_dir, config=config, mode="sink") as session:
        for line, phase in (("WRITE 1", "write"), ("WRITE", "write"), ("STOP", "stop")):
            reply = session.command(line)
            assert reply["ok"] is False, reply
            assert (reply["origin"], reply["name"], reply["phase"]) == ("harness", "HarnessProtocol", phase), reply
        assert session.end() == _EXIT_OK


def test_sink_gets_records_that_continue_in_child_buffers(harness_binary: Path, sock_dir: Path, tmp_path: Path) -> None:
    # With 64-byte buffers, the second record crosses into a child, and the third is larger than two buffers.
    records = [
        b'{"ID":1,"Label":"' + b"a" * 20 + b'"}\n',
        b'{"ID":2,"Label":"' + b"b" * 20 + b'"}\n',
        b'{"ID":3,"Label":"' + b"c" * 130 + b'"}\n',
        b'{"ID":4,"Label":""}\n',
    ]
    output = tmp_path / "out.jsonl"
    config = tmp_path / "file.nesql"
    config.write_text(
        f'CREATE SINK s (id INTEGER NOT NULL, "Label" VARSIZED) TYPE File SET '
        f"('{output}' AS \"SINK\".FILE_PATH, 'JSON' AS \"SINK\".OUTPUT_FORMAT);\n",
        encoding="utf-8",
    )
    records_path = tmp_path / "records.jsonl"
    records_path.write_bytes(b"".join(records))
    args = [f"--input-path={records_path}", "--buffer-size=64"]
    with Session(harness_binary, sock_dir, config=config, mode="sink", args=args) as session:
        start = session.command("START")
        assert start == {"ok": True, "phase": "start", "n_buffers": 3}, start
        assert session.command("WRITE 3")["ok"] is True
        assert session.command("STOP")["ok"] is True
        assert session.end() == _EXIT_OK
    # The file sink writes a header line, and the worker threads may write the buffers in any order.
    _, written = output.read_bytes().split(b"\n", 1)
    assert sorted(written.splitlines(keepends=True)) == records


_JSON_FILE_SINK_STATEMENT = (
    'CREATE SINK s (id INTEGER NOT NULL) TYPE File SET (\'/dev/null\' AS "SINK".FILE_PATH, \'JSON\' AS "SINK".OUTPUT_FORMAT)'
)
_JSON_FILE_SINK = _JSON_FILE_SINK_STATEMENT + ";\n"


@pytest.mark.parametrize(
    ("config_text", "message"),
    [
        pytest.param(_JSON_FILE_SINK.replace("'JSON'", "'CSV'"), "OUTPUT_FORMAT must be 'JSON', not 'CSV'", id="csv"),
        pytest.param(
            _JSON_FILE_SINK * 2,
            f"expected a single CREATE SINK statement, but the config has 2: {_JSON_FILE_SINK_STATEMENT}; {_JSON_FILE_SINK_STATEMENT}",
            id="two-statements",
        ),
        pytest.param("CREATE LOGICAL SOURCE s (id INTEGER NOT NULL);\n", "the statement is not a CREATE SINK", id="not-a-sink"),
    ],
)
def test_sink_bind_rejects_a_config_that_the_session_cannot_run(
    harness_binary: Path, sock_dir: Path, tmp_path: Path, config_text: str, message: str
) -> None:
    # Every rejection is an InvalidConfig, so the message tells them apart.
    config = tmp_path / "sink.nesql"
    config.write_text(config_text, encoding="utf-8")
    with Session(harness_binary, sock_dir, config=config, mode="sink") as session:
        reply = session.command("BIND")
        assert (reply["ok"], reply["origin"], reply["name"]) == (False, "harness", "InvalidConfig"), reply
        assert message in reply["message"], reply
        assert session.end() == _EXIT_OK
