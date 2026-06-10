# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""TCP sink TestDataLoader for the conn-test harness.

Discovered by the runner via the per-plugin ``conn-test/loader.py`` glob.
Sink-side flow (inverse of the TCP source): the harness slices the runner's
JSONL into TupleBuffers and drains them through TCPSink, which writes each
buffer's bytes to the relay (the sibling ``tcp_relay.py``, mounted via
``setup_file``); the relay pipes them to the loader's capture connection, and
``read_back`` parses the collected bytes back into typed rows (via the
framework's ``jsonl_to_rows``) for the order-insensitive multiset compare. No
formatting happens in the sink — it carries the bytes through.

Port contract (see tcp_relay.py): the loader-side capture port is the exposed
connector port + 1, derived from the compose endpoint passed to ``__init__``
(always the real relay service, never the toxiproxy/fault endpoint).

Capture uses a **single persistent connection + a drain thread**. The
framework keeps one loader instance per scenario and calls ``setup`` (before
the sink starts) then ``read_back`` (after it stops), so ``setup`` connects,
spawns a daemon thread that accumulates everything the relay forwards, and
``read_back`` waits on the accumulated record count. Draining concurrently —
rather than only in ``read_back`` — keeps the relay's outbound queue moving so
a large scenario never stalls on full kernel buffers. The thread terminates on
EOF or on the finalizer closing the socket; the relay deliberately does NOT
propagate a connector crash as EOF, so termination never races a respawning
harness (crash_recovery).
"""

from __future__ import annotations

import contextlib
import socket
import threading
import time
import weakref
from typing import Any

from conntest_runner.contracts import SinkLoader
from conntest_runner.datamodel import NativeColumn, jsonl_to_rows
from conntest_runner.endpoint import split_endpoint

_CONNECT_TIMEOUT_S = 10.0
_RECV_CHUNK = 65536
# Once at least one record has arrived, stop collecting after this long with no
# new bytes — the sink's writes are delivered back-to-back, so a short quiet
# window means "delivery is done".
_IDLE_TIMEOUT_S = 1.0
# Overall ceiling on the read_back drain (the framework does not pass one).
_READ_BACK_TIMEOUT_S = 30.0


def _close(sock: socket.socket) -> None:
    """Close the capture connection. Safe to call more than once."""
    with contextlib.suppress(OSError):
        sock.close()


class TcpSinkLoader(SinkLoader):
    """Per-plugin TestDataLoader for the TCP sink.

    The class name is conventional, not load-bearing: discovery picks the
    single duck-typed match in this module.
    """

    def __init__(self, endpoint: str) -> None:
        host, port = split_endpoint(endpoint)
        # Loader-side relay port = exposed connector port + 1 (tcp_relay.py).
        self._capture_address = (host, port + 1)

    def config_overrides(self, *, target: str, endpoint: str) -> dict[str, str]:  # noqa: ARG002  # target unused: TCP has no topic/table namespace
        """Config keys the framework swaps in by value.

        Only the endpoint is rewritten. ``target`` has no TCP equivalent — a
        bare byte stream has no topic/table namespace; per-case isolation comes
        from the relay pairing each case's fresh connections instead.
        """
        host, port = split_endpoint(endpoint)
        return {"socket_host": host, "socket_port": str(port)}

    def setup(self, *, target: str, schema: object = None) -> None:  # noqa: ARG002  # formatted sink ignores schema; target has no TCP equivalent
        """Connect the capture side and start draining it on a daemon thread.

        Connecting here pins the case boundary (the relay treats a new loader
        connection as "new test case") and guarantees the capture end exists
        before the sink writes its first byte — the framework sequences setup
        before the sink's START. ``schema`` is accepted (the framework passes
        it uniformly) but unused — a formatted sink does not key off it;
        ``read_back`` receives it again for parsing.
        """
        sock = socket.create_connection(self._capture_address, timeout=_CONNECT_TIMEOUT_S)
        # create_connection's timeout sticks to the socket and would turn into
        # a 10 s *read* deadline on the drain thread — which must idle through
        # arbitrarily long quiet stretches (the timeout scenario holds ~15 s of
        # link silence). Block forever instead; the finalizer's close() is what
        # ends the thread.
        sock.settimeout(None)
        captured = bytearray()
        lock = threading.Lock()

        # The thread captures the socket/buffer/lock, NOT self, so the
        # finalizer registered below can collect this loader (and close the
        # socket, which also ends the thread) promptly.
        def drain() -> None:
            while True:
                try:
                    chunk = sock.recv(_RECV_CHUNK)
                except OSError:
                    return
                if not chunk:
                    return
                with lock:
                    captured.extend(chunk)

        thread = threading.Thread(target=drain, name="tcp-sink-capture", daemon=True)
        thread.start()

        self._captured = captured
        self._lock = lock
        # Close even if read_back() is never called (the `empty` scenario, or a
        # mid-scenario failure); closing the socket also terminates the drain
        # thread. Capture the socket only — not self.
        self._finalizer = weakref.finalize(self, _close, sock)

    def read_back(
        self,
        *,
        target: str,  # noqa: ARG002  # capture connection already bound in setup()
        expect_records: int,
        schema: list[NativeColumn],
    ) -> list[tuple[Any, ...]]:
        r"""Return the records captured since setup(), parsed back into rows.

        A record is one newline-terminated JSONL line (the framework's wire
        form). Waits until ``expect_records`` complete records have arrived,
        else ``_IDLE_TIMEOUT_S`` with no new bytes (once at least one record
        arrived), else ``_READ_BACK_TIMEOUT_S``. Parsing goes through the
        framework's ``jsonl_to_rows`` (keyed by ``schema``) — the single
        deserializer, so the loader does no decoding of its own. Closes the
        capture connection on every path.
        """
        try:
            if expect_records == 0:
                return []

            deadline = time.monotonic() + _READ_BACK_TIMEOUT_S
            last_size = 0
            last_change = time.monotonic()
            while time.monotonic() < deadline:
                with self._lock:
                    size = len(self._captured)
                    records = self._captured.count(b"\n")
                if records >= expect_records:
                    break
                if size != last_size:
                    last_size = size
                    last_change = time.monotonic()
                elif records > 0 and (time.monotonic() - last_change) >= _IDLE_TIMEOUT_S:
                    break
                time.sleep(0.05)

            with self._lock:
                payload = bytes(self._captured)
            return list(jsonl_to_rows(schema, payload))
        finally:
            self._finalizer()  # idempotent: runs _close once
