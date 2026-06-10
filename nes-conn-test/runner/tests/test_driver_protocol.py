# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Adversarial black-box tests of the conntest-harness driver binary.

The conformance battery only ever speaks the protocol *correctly*: the
runner formats every flag from a Python int, listens before spawning,
writes config files before BIND, and sends well-formed verbs. The
driver's guards against a misbehaving counterpart — malformed argv,
unconnectable sockets, unknown verbs, commands out of order, unreadable
configs, config files that parse but violate the harness contract — are
therefore structurally unreachable from the battery. This file is the
misbehaving counterpart.

Unlike the battery these tests need no docker services: the harness binds
no connector backend before the guard under test fires (the only configs
that bind successfully use the engine's built-in File sink against tmp
paths). They do need the harness *binary*, so they skip on hosts without
a build (the ``binary_paths`` fixture) and run for real as part of the
battery's meta group inside the runner container — including the coverage
lane, where the spawned harness writes profraws like every other spawn.

Each spawn is one short-lived session: a listening unix socket the test
owns, the harness connecting to it, raw command lines down, one raw JSON
reply line back up (asserted on directly, not through ``read_reply``,
because malformed-input replies are exactly what is under test here).
"""

from __future__ import annotations

import json
import socket
import subprocess
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, Self, cast

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import TracebackType


class BinaryPaths(Protocol):
    """Structural twin of conftest.BinaryPaths (conftest is not importable here)."""

    build_dir: Path
    harness: Path


_ACCEPT_TIMEOUT_S = 30.0
_EXIT_TIMEOUT_S = 30.0

# ConntestStarter.cpp's documented exit codes.
_EXIT_OK = 0
_EXIT_CHANNEL_ERROR = 1
_EXIT_BAD_ARGS = 2


def _short_sock_dir() -> Path:
    """A freshly-created short directory for the control socket.

    pytest's tmp_path can exceed ``sizeof(sun_path)`` (~108 bytes) on deep
    test ids; AF_UNIX needs a short path, so allocate under /tmp directly.
    """
    return Path(tempfile.mkdtemp(prefix="ct-drv-", dir="/tmp"))  # AF_UNIX path-length cap


@pytest.fixture
def sock_dir() -> Iterator[Path]:
    d = _short_sock_dir()
    yield d
    for child in d.iterdir():
        child.unlink(missing_ok=True)
    d.rmdir()


def _spawn(
    harness: Path,
    *,
    mode: str,
    config: Path | str,
    control_sock: Path | str,
    extra: tuple[str, ...] = (),
) -> subprocess.Popen[str]:
    argv = [
        str(harness),
        f"--mode={mode}",
        f"--config={config}",
        f"--control-sock={control_sock}",
        *extra,
    ]
    # The argv is built entirely from this test's literals + fixture paths.
    return subprocess.Popen(  # noqa: S603
        argv,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


class Session:
    """One harness process connected to a control socket this test listens on.

    Mirrors the runner's stepper topology (runner listens, harness connects)
    but exposes the raw line protocol instead of the typed command verbs, so
    tests can send malformed input the production stepper cannot produce.
    """

    def __init__(
        self,
        harness: Path,
        sock_dir: Path,
        *,
        mode: str,
        config: Path | str,
        extra: tuple[str, ...] = (),
        accept: bool = True,
    ) -> None:
        self.sock_path = sock_dir / "control.sock"
        self._listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._listener.bind(str(self.sock_path))
        self._listener.listen(1)
        self.proc = _spawn(
            harness, mode=mode, config=config, control_sock=self.sock_path, extra=extra
        )
        self._conn: socket.socket | None = None
        self._reader = None
        self.stderr = ""
        if accept:
            self._listener.settimeout(_ACCEPT_TIMEOUT_S)
            self._conn, _ = self._listener.accept()
            self._conn.settimeout(_ACCEPT_TIMEOUT_S)
            self._reader = self._conn.makefile("rb")

    def send_raw(self, data: bytes) -> None:
        assert self._conn is not None
        self._conn.sendall(data)

    def reply_line(self) -> str:
        assert self._reader is not None
        line = self._reader.readline()
        assert line.endswith(b"\n"), f"control channel closed mid-reply: {line!r}"
        return line.decode("utf-8").rstrip("\n")

    def command(self, line: str) -> dict[str, Any]:
        """Send one command line, return the parsed JSON reply."""
        self.send_raw(line.encode("utf-8") + b"\n")
        reply = self.reply_line()
        return cast("dict[str, Any]", json.loads(reply))

    def end(self) -> int:
        """Close the control connection (EOF), reap the harness, return its exit code.

        communicate() (not bare wait()) drains and closes the stdout/stderr
        pipes; the captured stderr stays available as ``self.stderr``.
        """
        if self._reader is not None:
            self._reader.close()
            self._reader = None
        if self._conn is not None:
            self._conn.close()
            self._conn = None
        self._listener.close()
        _, self.stderr = self.proc.communicate(timeout=_EXIT_TIMEOUT_S)
        return self.proc.returncode

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        try:
            self.end()
        finally:
            if self.proc.poll() is None:
                self.proc.kill()
                self.proc.communicate(timeout=_EXIT_TIMEOUT_S)


def _harness_error(reply: dict[str, Any], *, phase: str, fragment: str) -> None:
    """Assert a reply is an origin=harness error in `phase` mentioning `fragment`."""
    assert reply["ok"] is False, reply
    assert reply["origin"] == "harness", reply
    assert reply["phase"] == phase, reply
    assert fragment in reply["message"], reply


# ── argv / spawn errors (ConntestStarter, ControlChannel::connect) ─────


def test_missing_required_flags_exits_2(binary_paths: BinaryPaths) -> None:
    # argparse throws on the missing required --mode/--config/--control-sock;
    # main catches, prints usage, exits 2 — before any socket work.
    proc = subprocess.run(  # noqa: S603
        [str(binary_paths.harness)],
        capture_output=True,
        text=True,
        timeout=_EXIT_TIMEOUT_S,
        check=False,
    )
    assert proc.returncode == _EXIT_BAD_ARGS, proc.stderr
    assert "--mode" in proc.stderr


def test_unknown_mode_choice_exits_2(binary_paths: BinaryPaths, sock_dir: Path) -> None:
    proc = subprocess.run(  # noqa: S603
        [
            str(binary_paths.harness),
            "--mode=frobnicate",
            "--config=/dev/null",
            f"--control-sock={sock_dir}/s",
        ],
        capture_output=True,
        text=True,
        timeout=_EXIT_TIMEOUT_S,
        check=False,
    )
    assert proc.returncode == _EXIT_BAD_ARGS, proc.stderr


def test_connect_failure_exits_1(binary_paths: BinaryPaths, sock_dir: Path) -> None:
    # Nobody listens on the socket path: ControlChannel::connect logs and
    # returns nullopt; main exits 1 without entering a session.
    proc = subprocess.run(  # noqa: S603
        [
            str(binary_paths.harness),
            "--mode=source",
            "--config=/dev/null",
            f"--control-sock={sock_dir}/nobody-listens.sock",
        ],
        capture_output=True,
        text=True,
        timeout=_EXIT_TIMEOUT_S,
        check=False,
    )
    assert proc.returncode == _EXIT_CHANNEL_ERROR, proc.stderr


def test_overlong_socket_path_exits_1(binary_paths: BinaryPaths) -> None:
    # sun_path caps an AF_UNIX path at ~108 bytes; the driver rejects longer
    # ones up front instead of silently truncating.
    too_long = "/tmp/" + "x" * 200  # noqa: S108  # never created; only the length matters
    proc = subprocess.run(  # noqa: S603
        [
            str(binary_paths.harness),
            "--mode=source",
            "--config=/dev/null",
            f"--control-sock={too_long}",
        ],
        capture_output=True,
        text=True,
        timeout=_EXIT_TIMEOUT_S,
        check=False,
    )
    assert proc.returncode == _EXIT_CHANNEL_ERROR, proc.stderr


@pytest.mark.parametrize(
    ("mode", "flag"),
    [
        ("source", "--buffer-size=abc"),
        ("source", "--step-timeout-ms=-5"),
        ("sink", "--threads=abc"),
    ],
)
def test_non_numeric_unsigned_flag_exits_2(
    binary_paths: BinaryPaths, sock_dir: Path, mode: str, flag: str
) -> None:
    # main connects the control channel BEFORE parsing the per-mode numeric
    # flags, so the bad flag is only diagnosed after a successful accept.
    with Session(
        binary_paths.harness,
        sock_dir,
        mode=mode,
        config="/dev/null",
        extra=(flag,),
    ) as session:
        assert session.end() == _EXIT_BAD_ARGS
        assert "expects an unsigned integer" in session.stderr


# ── control-protocol abuse (SessionCommand, Source/SinkSession dispatch) ──


@pytest.mark.parametrize("mode", ["source", "sink"])
def test_unknown_verb(binary_paths: BinaryPaths, sock_dir: Path, mode: str) -> None:
    with Session(binary_paths.harness, sock_dir, mode=mode, config="/dev/null") as session:
        reply = session.command("FROBNICATE")
        _harness_error(reply, phase="dispatch", fragment="unknown command")
        assert session.end() == _EXIT_OK


@pytest.mark.parametrize("line", ["WRITE", "WRITE abc", "WRITE 1.5"])
def test_write_needs_a_buffer_count(binary_paths: BinaryPaths, sock_dir: Path, line: str) -> None:
    with Session(binary_paths.harness, sock_dir, mode="sink", config="/dev/null") as session:
        reply = session.command(line)
        _harness_error(reply, phase="write", fragment="WRITE needs a buffer count")
        assert session.end() == _EXIT_OK


def test_write_before_start(binary_paths: BinaryPaths, sock_dir: Path) -> None:
    with Session(binary_paths.harness, sock_dir, mode="sink", config="/dev/null") as session:
        reply = session.command("WRITE 1")
        _harness_error(reply, phase="write", fragment="START before WRITE")
        assert session.end() == _EXIT_OK


def test_stop_before_start(binary_paths: BinaryPaths, sock_dir: Path) -> None:
    with Session(binary_paths.harness, sock_dir, mode="sink", config="/dev/null") as session:
        reply = session.command("STOP")
        _harness_error(reply, phase="stop", fragment="STOP without a started sink")
        assert session.end() == _EXIT_OK


@pytest.mark.parametrize("line", ["FILL", "FILL 10", "FILL ten bytes"])
def test_fill_needs_two_counts(binary_paths: BinaryPaths, sock_dir: Path, line: str) -> None:
    with Session(binary_paths.harness, sock_dir, mode="source", config="/dev/null") as session:
        reply = session.command(line)
        _harness_error(reply, phase="fill", fragment="FILL needs <n_rows> <n_bytes>")
        assert session.end() == _EXIT_OK


def test_fill_before_open(binary_paths: BinaryPaths, sock_dir: Path) -> None:
    with Session(binary_paths.harness, sock_dir, mode="source", config="/dev/null") as session:
        reply = session.command("FILL 1 1")
        _harness_error(reply, phase="fill", fragment="OPEN before FILL")
        assert session.end() == _EXIT_OK


def test_close_before_open(binary_paths: BinaryPaths, sock_dir: Path) -> None:
    with Session(binary_paths.harness, sock_dir, mode="source", config="/dev/null") as session:
        reply = session.command("CLOSE")
        _harness_error(reply, phase="close", fragment="CLOSE without an open source")
        assert session.end() == _EXIT_OK


@pytest.mark.parametrize("mode", ["source", "sink"])
def test_blank_line_is_a_keepalive(binary_paths: BinaryPaths, sock_dir: Path, mode: str) -> None:
    # A blank line produces NO reply (the session loop just continues); the
    # next real command must still get exactly one reply.
    with Session(binary_paths.harness, sock_dir, mode=mode, config="/dev/null") as session:
        session.send_raw(b"\n")
        reply = session.command("FROBNICATE")
        _harness_error(reply, phase="dispatch", fragment="unknown command")
        assert session.end() == _EXIT_OK


def test_crlf_framing_strips_the_cr(binary_paths: BinaryPaths, sock_dir: Path) -> None:
    # readLine strips a trailing '\r' so a CRLF-framing peer still speaks the
    # protocol; the echoed command in the error must not contain the CR.
    with Session(binary_paths.harness, sock_dir, mode="sink", config="/dev/null") as session:
        session.send_raw(b"FROBNICATE\r\n")
        reply = cast("dict[str, Any]", json.loads(session.reply_line()))
        _harness_error(reply, phase="dispatch", fragment="unknown command: FROBNICATE")
        assert "\r" not in reply["message"]
        assert session.end() == _EXIT_OK


def test_split_writes_are_one_command(binary_paths: BinaryPaths, sock_dir: Path) -> None:
    # The control channel reads until '\n'; a command arriving in two TCP-ish
    # fragments is still one command, not two.
    with Session(binary_paths.harness, sock_dir, mode="sink", config="/dev/null") as session:
        session.send_raw(b"FROBN")
        time.sleep(0.05)
        session.send_raw(b"ICATE\n")
        reply = cast("dict[str, Any]", json.loads(session.reply_line()))
        _harness_error(reply, phase="dispatch", fragment="unknown command: FROBNICATE")
        assert session.end() == _EXIT_OK


# ── bind failures (missing config, replay, jsonEscape) ─────────────────


@pytest.mark.parametrize(
    ("mode", "follow_up"),
    [("source", "OPEN"), ("sink", "START")],
)
def test_missing_config_error_and_replay(
    binary_paths: BinaryPaths, sock_dir: Path, tmp_path: Path, mode: str, follow_up: str
) -> None:
    # First BIND fails reading the config; the error is cached and replayed
    # verbatim by a second BIND and by the follow-up command, instead of
    # re-attempting the read.
    missing = tmp_path / "never-written.nesql"
    with Session(binary_paths.harness, sock_dir, mode=mode, config=missing) as session:
        first = session.command("BIND")
        _harness_error(first, phase="bind", fragment="cannot read config file")
        assert first["name"] == "HarnessIO"
        second = session.command("BIND")
        assert second == first
        third = session.command(follow_up)
        assert third == second
        assert session.end() == _EXIT_OK


def test_error_reply_json_escapes_special_characters(
    binary_paths: BinaryPaths, sock_dir: Path, tmp_path: Path
) -> None:
    # The bind error embeds the config path in the reply message; a path
    # exercising every jsonEscape arm (quote, backslash, \b, \f, \n, \r, \t,
    # and a raw control byte) must still produce one parseable JSON line.
    weird = tmp_path / 'we"ird\\path\b\f\n\r\t\x01.nesql'
    with Session(binary_paths.harness, sock_dir, mode="sink", config=weird) as session:
        reply = session.command("BIND")  # json.loads inside command() IS the assertion
        _harness_error(reply, phase="bind", fragment="cannot read config file")
        for char in ('"', "\\", "\b", "\f", "\n", "\r", "\t", "\x01"):
            assert char in reply["message"], f"missing {char!r} in {reply['message']!r}"
        assert session.end() == _EXIT_OK


# ── binder contract violations (Binder.cpp guards) ─────────────────────

_VALID_FILE_SINK = """\
CREATE SINK conntest_sink (value VARSIZED NOT NULL) TYPE File SET (
    '{out}' AS `SINK`.file_path,
    'JSON'  AS `SINK`.OUTPUT_FORMAT);
"""


def _write_config(tmp_path: Path, sql: str) -> Path:
    config = tmp_path / "config.nesql"
    config.write_text(sql, encoding="utf-8")
    return config


def _bind_error(
    binary_paths: BinaryPaths, sock_dir: Path, tmp_path: Path, *, mode: str, sql: str
) -> dict[str, Any]:
    config = _write_config(tmp_path, sql)
    with Session(binary_paths.harness, sock_dir, mode=mode, config=config) as session:
        reply = session.command("BIND")
        assert reply["ok"] is False, reply
        assert session.end() == _EXIT_OK
    return reply


@pytest.mark.parametrize("mode", ["source", "sink"])
def test_unparseable_config_is_a_parse_error(
    binary_paths: BinaryPaths, sock_dir: Path, tmp_path: Path, mode: str
) -> None:
    reply = _bind_error(
        binary_paths, sock_dir, tmp_path, mode=mode, sql="THIS IS NOT SQL AT ALL;\n"
    )
    assert reply["phase"] == "parse", reply


def test_sink_config_must_be_exactly_one_statement(
    binary_paths: BinaryPaths, sock_dir: Path, tmp_path: Path
) -> None:
    two = _VALID_FILE_SINK.format(out="/tmp/a.jsonl") + _VALID_FILE_SINK.format(  # noqa: S108
        out="/tmp/b.jsonl"  # noqa: S108  # literal in generated SQL, never opened
    ).replace("conntest_sink", "conntest_sink2")
    reply = _bind_error(binary_paths, sock_dir, tmp_path, mode="sink", sql=two)
    _harness_error(reply, phase="descriptor", fragment="exactly one CREATE SINK statement")


def test_sink_config_with_non_sink_statement(
    binary_paths: BinaryPaths, sock_dir: Path, tmp_path: Path
) -> None:
    reply = _bind_error(
        binary_paths,
        sock_dir,
        tmp_path,
        mode="sink",
        sql="CREATE LOGICAL SOURCE not_a_sink (x INT32);\n",
    )
    _harness_error(reply, phase="descriptor", fragment="not a CREATE SINK statement")


def test_unregistered_sink_type_is_unknown_sink_type(
    binary_paths: BinaryPaths, sock_dir: Path, tmp_path: Path
) -> None:
    sql = _VALID_FILE_SINK.format(out="/tmp/x.jsonl").replace(  # noqa: S108
        "TYPE File", "TYPE Frobnicator"
    )
    reply = _bind_error(binary_paths, sock_dir, tmp_path, mode="sink", sql=sql)
    assert reply["origin"] == "connector", reply
    assert reply["name"] == "UnknownSinkType", reply


def test_sink_descriptor_validation_failure(
    binary_paths: BinaryPaths, sock_dir: Path, tmp_path: Path
) -> None:
    # TYPE File without its required file_path: parses fine, fails descriptor
    # validation — a connector-origin error, not a harness one.
    sql = (
        "CREATE SINK conntest_sink (value VARSIZED NOT NULL) TYPE File SET (\n"
        "    'JSON' AS `SINK`.OUTPUT_FORMAT);\n"
    )
    reply = _bind_error(binary_paths, sock_dir, tmp_path, mode="sink", sql=sql)
    assert reply["origin"] == "connector", reply
    assert reply["phase"] == "descriptor", reply


def test_duplicate_logical_source(
    binary_paths: BinaryPaths, sock_dir: Path, tmp_path: Path
) -> None:
    sql = "CREATE LOGICAL SOURCE dup (x INT32);\nCREATE LOGICAL SOURCE dup (x INT32);\n"
    reply = _bind_error(binary_paths, sock_dir, tmp_path, mode="source", sql=sql)
    _harness_error(reply, phase="descriptor", fragment="duplicate logical source")


def test_physical_source_for_unknown_logical(
    binary_paths: BinaryPaths, sock_dir: Path, tmp_path: Path
) -> None:
    sql = (
        "CREATE PHYSICAL SOURCE FOR never_declared TYPE File"
        " SET ('JSON' AS INPUT_FORMATTER.`TYPE`);\n"
    )
    reply = _bind_error(binary_paths, sock_dir, tmp_path, mode="source", sql=sql)
    # Depending on where the engine's StatementBinder resolves the logical
    # source, this surfaces either as the Binder's own guard (origin=harness,
    # "unknown logical source") or as a binder error (origin=connector,
    # phase=parse). Both are correct rejections of this config.
    assert reply["phase"] in ("descriptor", "parse"), reply


def test_source_config_without_physical_statement(
    binary_paths: BinaryPaths, sock_dir: Path, tmp_path: Path
) -> None:
    sql = "CREATE LOGICAL SOURCE lonely (x INT32);\n"
    reply = _bind_error(binary_paths, sock_dir, tmp_path, mode="source", sql=sql)
    _harness_error(reply, phase="descriptor", fragment="no CREATE PHYSICAL SOURCE")


def test_source_descriptor_validation_failure(
    binary_paths: BinaryPaths, sock_dir: Path, tmp_path: Path
) -> None:
    # TYPE File without its required file_path: parses + binds, fails the
    # connector's descriptor validation.
    sql = (
        "CREATE LOGICAL SOURCE smoke (x INT32);\n"
        "CREATE PHYSICAL SOURCE FOR smoke TYPE File SET ('JSON' AS INPUT_FORMATTER.`TYPE`);\n"
    )
    reply = _bind_error(binary_paths, sock_dir, tmp_path, mode="source", sql=sql)
    assert reply["origin"] == "connector", reply
    assert reply["phase"] == "descriptor", reply


# ── bound File-sink sessions (start/stop ordering, empty input) ────────


@pytest.fixture
def file_sink_config(tmp_path: Path) -> Path:
    return _write_config(tmp_path, _VALID_FILE_SINK.format(out=tmp_path / "out.jsonl"))


def test_sink_session_happy_empty_input(
    binary_paths: BinaryPaths, sock_dir: Path, file_sink_config: Path
) -> None:
    # No --input-path is the supported `empty` shape: START materializes zero
    # buffers and a WRITE drains nothing, successfully.
    with Session(binary_paths.harness, sock_dir, mode="sink", config=file_sink_config) as session:
        bind = session.command("BIND")
        assert bind["ok"] is True, bind
        assert bind["row_width"] > 0
        start = session.command("START")
        assert start == {"ok": True, "phase": "start", "n_buffers": 0}
        write = session.command("WRITE 1")
        assert write == {"ok": True, "phase": "write", "units_written": 0}
        stop = session.command("STOP")
        assert stop == {"ok": True, "phase": "stop"}
        assert session.end() == _EXIT_OK


def test_double_start_is_rejected(
    binary_paths: BinaryPaths, sock_dir: Path, file_sink_config: Path
) -> None:
    with Session(binary_paths.harness, sock_dir, mode="sink", config=file_sink_config) as session:
        assert session.command("BIND")["ok"] is True
        assert session.command("START")["ok"] is True
        reply = session.command("START")
        _harness_error(reply, phase="start", fragment="sink already started")
        assert session.command("STOP")["ok"] is True
        assert session.end() == _EXIT_OK


def test_double_stop_is_rejected(
    binary_paths: BinaryPaths, sock_dir: Path, file_sink_config: Path
) -> None:
    with Session(binary_paths.harness, sock_dir, mode="sink", config=file_sink_config) as session:
        assert session.command("BIND")["ok"] is True
        assert session.command("START")["ok"] is True
        assert session.command("STOP")["ok"] is True
        reply = session.command("STOP")
        _harness_error(reply, phase="stop", fragment="STOP without a started sink")
        assert session.end() == _EXIT_OK


def test_start_offset_beyond_input(
    binary_paths: BinaryPaths, sock_dir: Path, file_sink_config: Path
) -> None:
    # A resume offset past the materialized buffer count is a protocol error
    # (the runner only replays offsets a prior session reported committed).
    with Session(binary_paths.harness, sock_dir, mode="sink", config=file_sink_config) as session:
        assert session.command("BIND")["ok"] is True
        reply = session.command("START 5")
        _harness_error(reply, phase="start", fragment="exceeds input buffer count")
        assert session.end() == _EXIT_OK


def test_eof_with_started_sink_still_exits_cleanly(
    binary_paths: BinaryPaths, sock_dir: Path, file_sink_config: Path
) -> None:
    # EOF without STOP: the session state's destructor must best-effort stop
    # the started sink (the ExecutablePipelineStage contract) and exit 0.
    with Session(binary_paths.harness, sock_dir, mode="sink", config=file_sink_config) as session:
        assert session.command("BIND")["ok"] is True
        assert session.command("START")["ok"] is True
        assert session.end() == _EXIT_OK
