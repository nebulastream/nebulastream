#!/usr/bin/env python3
"""Adapt a NebulaStream JSON File-sink stream to Pi's one-message socket protocol."""

import argparse
import json
import os
from pathlib import Path
import re
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time


SCHEMA = b'"type":VARSIZED:NOT_NULLABLE,"text":VARSIZED:NOT_NULLABLE'
MAX_FRAME_BYTES = 65536
SQL_SOCKET = "@NES_NOTIFY_SOCKET@"


class RelayError(Exception):
    pass


def parse_message(frame):
    try:
        message = json.loads(frame.decode("utf-8"))
        if (not isinstance(message, dict) or set(message) != {"type", "text"}
                or message["type"] not in ("notify", "status") or not isinstance(message["text"], str)):
            raise ValueError()
        limit = 8192 if message["type"] == "notify" else 1024
        if len(message["text"].encode("utf-8")) > limit:
            raise ValueError()
        return message
    except (ValueError, UnicodeError):
        # Do not echo query output (which may contain private data or terminal escapes).
        raise RelayError("Invalid notification JSON or text exceeds Pi's byte limit") from None


def forward(message, destination, timeout):
    frame = (json.dumps(message, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as peer:
        deadline = time.monotonic() + timeout
        peer.settimeout(timeout)
        peer.connect(destination)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RelayError("Pi request timed out")
        peer.settimeout(remaining)
        peer.sendall(frame)
        reply = bytearray()
        while b"\n" not in reply:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RelayError("Pi acknowledgement timed out")
            peer.settimeout(remaining)
            chunk = peer.recv(1024)
            if not chunk:
                raise RelayError("Pi closed the connection without an acknowledgement")
            reply.extend(chunk)
            if len(reply) > 1024:
                raise RelayError("Invalid Pi acknowledgement")
        try:
            ack = json.loads(reply.decode("utf-8"))
            if not isinstance(ack, dict) or not isinstance(ack.get("ok"), bool):
                raise ValueError()
        except (ValueError, UnicodeError):
            raise RelayError("Invalid Pi acknowledgement") from None
        if not ack["ok"]:
            error = re.sub(r"[^a-zA-Z0-9_-]", "?", str(ack.get("error", "unknown")))[:80]
            raise RelayError(f"Pi rejected notification: {error}")


def relay_connection(peer, destination, timeout, stopping, worker_done):
    """No idle deadline on NES's connection; each Pi connection has its own deadline."""
    peer.settimeout(0.2)
    buffer = bytearray()
    first = True
    forwarded = 0
    while not stopping():
        try:
            chunk = peer.recv(MAX_FRAME_BYTES)
        except socket.timeout:
            if worker_done():
                raise RelayError("NES worker exited without closing its output stream")
            continue
        if not chunk:
            if buffer:
                raise RelayError("Incomplete trailing notification frame")
            break
        buffer.extend(chunk)
        while (newline := buffer.find(b"\n")) >= 0:
            if stopping():
                return forwarded
            if newline + 1 > MAX_FRAME_BYTES:
                raise RelayError("Notification frame exceeds 64 KiB")
            frame = bytes(buffer[:newline])
            del buffer[:newline + 1]
            skip = first and frame == SCHEMA
            first = False
            if skip:
                continue
            forward(parse_message(frame), destination, timeout)
            forwarded += 1
        if len(buffer) >= MAX_FRAME_BYTES:
            raise RelayError("Notification frame exceeds 64 KiB")
    return forwarded


def start_worker(args, path):
    if args.sql is None:
        return None
    sql = args.sql.read_text(encoding="utf-8")
    if SQL_SOCKET not in sql:
        raise RelayError(f"SQL must contain {SQL_SOCKET} inside the File sink's quoted FILE_PATH")
    sql = sql.replace(SQL_SOCKET, path.replace("'", "''"))
    binary = args.binary.resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise RelayError("Build nes-repl-embedded first or supply --binary")
    # A file, not a PIPE requiring communicate(): the relay must serve sockets
    # concurrently with the worker, even when the SQL is larger than a pipe.
    with tempfile.TemporaryFile() as stream:
        stream.write(sql.encode("utf-8"))
        stream.seek(0)
        return subprocess.Popen([
            str(binary), "-f", "JSON", "--on-exit", "WAIT_FOR_QUERY_TERMINATION", "--",
            "--worker.default_query_execution.execution_mode=INTERPRETER",
        ], stdin=stream)


def stop_worker(worker):
    if worker is not None:
        if worker.poll() is None:
            worker.terminate()
            try:
                worker.wait(timeout=2)
            except subprocess.TimeoutExpired:
                worker.kill()
        worker.wait()


def run(args):
    destination = str(Path(args.pi_socket).absolute())
    if not stat.S_ISSOCK(os.stat(destination).st_mode):
        raise RelayError("Pi destination is not a Unix-domain socket")
    stopped = False

    def stop(_signal, _frame):
        nonlocal stopped
        stopped = True

    previous = {number: signal.signal(number, stop) for number in (signal.SIGINT, signal.SIGTERM)}
    worker = None
    forwarded = 0
    try:
        with tempfile.TemporaryDirectory(prefix="pi-nes-") as directory:
            path = str(Path(args.listen).absolute()) if args.listen else str(Path(directory) / "input.sock")
            if path == destination:
                raise RelayError("NES input and Pi destination must be different sockets")
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
                # Never unlink an existing destination to make bind succeed.
                listener.bind(path)
                identity = os.lstat(path)
                try:
                    os.chmod(path, 0o600)
                    listener.listen(1)
                    listener.settimeout(0.2)
                    worker = start_worker(args, path)
                    print(json.dumps({"event": "ready", "socket": path}), flush=True)
                    while not stopped:
                        try:
                            peer, _ = listener.accept()
                        except socket.timeout:
                            if worker is not None and worker.poll() is not None:
                                break
                            continue
                        with peer:
                            forwarded += relay_connection(peer, destination, args.timeout,
                                                          lambda: stopped,
                                                          lambda: worker is not None and worker.poll() is not None)
                        if args.once:
                            break
                    code = worker.poll() if worker is not None else 0
                    print(json.dumps({"event": "stopped", "forwarded": forwarded}), flush=True)
                    return (128 - code if code < 0 else code) if code is not None and not stopped else 0
                finally:
                    stop_worker(worker)
                    worker = None
                    try:
                        current = os.lstat(path)
                        if (current.st_dev, current.st_ino) == (identity.st_dev, identity.st_ino):
                            os.unlink(path)
                    except FileNotFoundError:
                        pass
    finally:
        stop_worker(worker)
        for number, handler in previous.items():
            signal.signal(number, handler)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pi-socket", default=os.environ.get("PI_NOTIFY_SOCKET"),
                        help="Pi destination; defaults to the managed launch's PI_NOTIFY_SOCKET")
    parser.add_argument("--listen", help="NES input socket; defaults to a private temporary directory")
    parser.add_argument("--timeout", type=float, default=3, help="Pi connect/send/ack deadline in seconds (0 < N <= 3)")
    parser.add_argument("--once", action="store_true", help="Exit after one NES connection reaches EOF")
    parser.add_argument("--sql", type=Path, help=f"Run a SQL file containing {SQL_SOCKET} in the sink FILE_PATH")
    parser.add_argument("--binary", type=Path, default=Path("cmake-build-debug/nes-frontend/apps/nes-repl-embedded"))
    args = parser.parse_args(argv)
    if not args.pi_socket:
        parser.error("run as a Pi-managed process or supply --pi-socket")
    if not 0 < args.timeout <= 3:
        parser.error("--timeout must be greater than zero and at most 3 seconds")
    try:
        return run(args)
    except (OSError, RelayError) as error:
        # Avoid printing payloads or arbitrary exception text from the query.
        detail = str(error) if isinstance(error, RelayError) else f"socket/file operation failed ({error.__class__.__name__})"
        print(f"nes-notifications: {detail}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
