# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""tcp_relay.py — the TCP "external system" for the TCP connector conn-tests.

Plain TCP has no broker: nothing exists for a loader to seed or read back
independently of the connector. This relay plays that role as the compose data
service. It runs on a stock python:alpine image (stdlib only) and is mounted
via the framework's ``setup_file`` mechanism — the same pattern as mounting
mosquitto.conf into the stock mosquitto image. It pipes bytes between the one
connector-side connection and the one loader-side connection, in both
directions (loader→connector seeds a source; connector→loader captures a
sink's writes).

Port contract (shared with the sibling loader.py and compose.yaml):

  CONNECTOR_PORT (9000)  the compose-``expose:``d port — the framework derives
                         the connector endpoint from it, and toxiproxy fronts
                         it for the needs_link fault scenarios.
  LOADER_PORT (9001)     CONNECTOR_PORT + 1 — the loader's seed/capture side;
                         not exposed and never faulted, so the oracle path
                         stays trustworthy while the connector's link degrades.
  HEALTH_PORT (9002)     accept-and-close, for the compose healthcheck. A
                         dedicated port so health probes never disturb a
                         session (a probe on 9000/9001 would be mistaken for a
                         connector/loader connection).

Lifecycle: one compose stack serves every test case of a battery group
sequentially, so the relay accepts and pairs connections repeatedly (this is
what rules out a one-shot ``socat`` bridge). The framework's no-races
sequencing (setup → connect → transfer → read_back) guarantees the loader
connects before the connector within a case, so:

  * a new LOADER connection marks a new test case: both directions reset and
    a leftover connector connection from the previous case is dropped;
  * a new CONNECTOR connection within a case (crash_recovery's respawned
    harness, a reconnecting source) replaces the old one and keeps receiving
    the still-undelivered loader bytes.

EOF propagates in ONE direction only, loader→connector: when the loader
half-closes, its remaining bytes are flushed and the connector's write half is
shut down — this is what makes "peer close = end-of-stream" testable for the
source. A connector EOF (or death) is deliberately NOT propagated to the
loader: the connector side is allowed to come and go (crash_recovery, warm
reconnects), and the sink loader's read_back terminates on record count / idle
timeout, never on EOF.

The relay never needs to store-and-replay across loader connections — the
sequencing guarantee means a destination is attached before its data flows;
the per-direction ``out`` buffers only smooth the moment between read and
write (and survive a connector respawn).
"""

from __future__ import annotations

import contextlib
import selectors
import socket
import sys


def _log(msg: str) -> None:
    """Trace a lifecycle event to stderr.

    Lands in `docker compose logs` and the runner's postmortem, where it pins
    which hop lost bytes when a scenario fails (it is how the capture-side
    socket-timeout bug was found).
    """
    sys.stderr.write(f"[relay] {msg}\n")
    sys.stderr.flush()


CONNECTOR_PORT = 9000
LOADER_PORT = 9001
HEALTH_PORT = 9002

_RECV_CHUNK = 65536


class _Side:
    """One relay side: the current connection and the bytes queued toward it."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.conn: socket.socket | None = None
        self.read_closed = False  # saw EOF from this side
        self.out = bytearray()  # bytes queued to write TO this side
        self.out_eof = False  # shutdown this side's write half once `out` drains

    def drop(self, *, clear_out: bool) -> None:
        if self.conn is not None:
            with contextlib.suppress(OSError):
                self.conn.close()
        self.conn = None
        self.read_closed = False
        if clear_out:
            self.out.clear()
            self.out_eof = False

    def attach(self, conn: socket.socket) -> None:
        self.drop(clear_out=False)
        conn.setblocking(False)  # noqa: FBT003  # stdlib positional-bool API
        self.conn = conn


def main() -> None:  # noqa: C901, PLR0915  # one poll loop over shared closure state; splitting it would scatter the invariants
    """Accept, pair, and pipe until the container stops (see module docstring)."""
    sel = selectors.DefaultSelector()
    ports = (("connector", CONNECTOR_PORT), ("loader", LOADER_PORT), ("health", HEALTH_PORT))
    for name, port in ports:
        # 0.0.0.0 is correct here: the relay serves the compose-internal
        # network only (nothing is published to the host).
        listener = socket.create_server(("0.0.0.0", port), backlog=8)  # noqa: S104
        listener.setblocking(False)  # noqa: FBT003  # stdlib positional-bool API
        sel.register(listener, selectors.EVENT_READ, ("listen", name))

    connector = _Side("connector")
    loader = _Side("loader")
    sides = {"connector": connector, "loader": loader}

    def other(side: _Side) -> _Side:
        return loader if side is connector else connector

    tracked: dict[socket.socket, int] = {}  # conn -> currently registered mask

    def sync_registrations() -> None:  # noqa: C901  # the mask derivation is one table, not branches to extract
        """Re-derive each connection's event mask from the relay state.

        A side is read only while the other side is attached — until then its
        bytes wait in the kernel socket buffer (natural backpressure), and a
        too-early EOF cannot be observed and mis-propagated.
        """
        live: dict[socket.socket, tuple[int, str]] = {}
        for side in sides.values():
            if side.conn is None:
                continue
            mask = 0
            if not side.read_closed and other(side).conn is not None:
                mask |= selectors.EVENT_READ
            if side.out or side.out_eof:
                mask |= selectors.EVENT_WRITE
            live[side.conn] = (mask, side.name)
        for conn in list(tracked):
            if conn not in live or live[conn][0] == 0:
                sel.unregister(conn)
                del tracked[conn]
        for conn, (mask, name) in live.items():
            if mask == 0:
                continue
            if conn not in tracked:
                sel.register(conn, mask, ("conn", name))
            elif tracked[conn] != mask:
                sel.modify(conn, mask, ("conn", name))
            tracked[conn] = mask

    def forget(conn: socket.socket | None) -> None:
        if conn is not None and conn in tracked:
            sel.unregister(conn)
            del tracked[conn]

    def handle_accept(listener: socket.socket, name: str) -> None:
        try:
            conn, _addr = listener.accept()
        except OSError:
            return
        if name == "health":
            conn.close()
            return
        _log(f"accept {name}")
        side = sides[name]
        forget(side.conn)
        if name == "loader":
            # A new loader connection is a new test case: reset both
            # directions and drop the previous case's connector leftovers (a
            # dead socket kept around would otherwise feed a phantom EOF into
            # the fresh session).
            forget(connector.conn)
            connector.drop(clear_out=True)
            loader.drop(clear_out=True)
        side.attach(conn)

    def handle_readable(side: _Side) -> None:
        conn = side.conn
        if conn is None:
            return
        try:
            chunk = conn.recv(_RECV_CHUNK)
        except OSError:
            chunk = b""  # an abortive close counts as this side being done
        if chunk:
            other(side).out.extend(chunk)
            return
        _log(f"eof {side.name}")
        if side is loader:
            # Loader done seeding: propagate end-of-stream to the connector
            # once everything seeded so far has been flushed.
            side.read_closed = True
            connector.out_eof = True
        else:
            # Connector gone (clean stop, crash, or cut link). Never
            # propagated to the loader; keep undelivered loader bytes queued
            # for a respawned connector (crash_recovery).
            forget(side.conn)
            side.drop(clear_out=False)

    def handle_writable(side: _Side) -> None:
        conn = side.conn
        if conn is None:
            return
        try:
            if side.out:
                # bytes() copy: a memoryview would block the bytearray resize below.
                sent = conn.send(bytes(side.out[:_RECV_CHUNK]))
                del side.out[:sent]
            if not side.out and side.out_eof:
                _log(f"shutdown-wr {side.name}")
                conn.shutdown(socket.SHUT_WR)
                side.out_eof = False
        except OSError as exc:
            _log(f"send-err {side.name} {exc}")
            forget(side.conn)
            # Writing to the loader failed => the loader is gone for good
            # (its queued bytes are garbage). Writing to the connector failed
            # => same as a connector death: keep nothing it already consumed,
            # but a respawn may still drain the queue.
            side.drop(clear_out=side is loader)

    while True:
        sync_registrations()
        for key, events in sel.select(timeout=1.0):
            kind, name = key.data
            if kind == "listen":
                handle_accept(key.fileobj, name)  # type: ignore[arg-type]
                continue
            side = sides[name]
            if key.fileobj is not side.conn:
                continue  # stale event for a connection replaced this batch
            if events & selectors.EVENT_READ:
                handle_readable(side)
            if events & selectors.EVENT_WRITE and side.conn is not None:
                handle_writable(side)


if __name__ == "__main__":
    main()
