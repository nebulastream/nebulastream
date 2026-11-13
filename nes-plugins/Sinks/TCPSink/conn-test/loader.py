# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Reads back what the TCP sink wrote.

The TCP sink opens the connection, so the loader listens as the server.

The sink's configs declare the JSON output format, so the records are JSONL.
"""

from __future__ import annotations

import contextlib
import select
import socket
import threading
import weakref
from typing import Any

from conntest_runner.contracts import SinkLoader
from conntest_runner.datamodel import NativeColumn, jsonl_to_rows
from conntest_runner.endpoint import split_endpoint

# How often the receiver checks whether the read-back has started, while no connection is waiting.
_POLL_S = 0.1
_RECV_CHUNK = 65536


class _Receiver:
    """Keeps what each of the sink's connections delivered.

    The TCP sink reconnects after a lost connection, so the data of one test can arrive on several connections.
    """

    def __init__(self, listener: socket.socket) -> None:
        self._listener = listener
        self.received: list[bytearray] = []
        self.done = threading.Event()
        self.thread = threading.Thread(target=self._run, name="tcp-sink-receiver", daemon=True)
        self.thread.start()

    def _run(self) -> None:
        # A closed listener ends the thread, which is how the loader's finalizer stops it.
        with contextlib.suppress(OSError, ValueError):
            while True:
                waiting, _, _ = select.select([self._listener], [], [], _POLL_S)
                if not waiting:
                    if self.done.is_set():
                        return
                    continue
                conn, _ = self._listener.accept()
                # A broken connection ends only itself, because the sink sends the rest on a new one.
                with conn, contextlib.suppress(OSError):
                    data = bytearray()
                    self.received.append(data)
                    while chunk := conn.recv(_RECV_CHUNK):
                        data += chunk


def _close(listener: socket.socket, done: threading.Event) -> None:
    done.set()
    with contextlib.suppress(OSError):
        listener.close()


class TcpSinkLoader(SinkLoader):
    """Discovery picks the only concrete loader class in this module, so the class can have any name."""

    def __init__(self, endpoint: str | None) -> None:  # noqa: ARG002  # TCP has no compose service, so endpoint is None
        # The harness runs in the runner's container and reaches the loader at the container's address,
        # which the fault proxy, a container of its own, can reach as well.
        # Each case has its own loader and port, so no two cases share a connection.
        listener = socket.create_server(("0.0.0.0", 0))  # noqa: S104  # only the compose network can reach it
        super().__init__(f"{socket.gethostbyname(socket.gethostname())}:{listener.getsockname()[1]}")
        self._receiver = _Receiver(listener)
        # The finalizer holds only the listener and the event, so it does not keep the loader alive.
        self._finalizer = weakref.finalize(self, _close, listener, self._receiver.done)

    def config_overrides(self, *, target: str, endpoint: str) -> dict[str, str | int]:  # noqa: ARG002  # TCP has no topic or table
        host, port = split_endpoint(endpoint)
        return {"SOCKET_HOST": host, "SOCKET_PORT": port}

    def setup(self, *, target: str, schema: list[NativeColumn] | None = None) -> None:
        """The listener is up from the start."""

    def read_back(
        self,
        *,
        target: str,  # noqa: ARG002  # one listener per case
        schema: list[NativeColumn],
        n_rows: int,  # noqa: ARG002  # the closed connection marks the end of the data
        timeout_s: float,
    ) -> list[tuple[Any, ...]]:
        """The sink closes its connection when it stops, so the receiver has everything once it has read each connection to its end.

        A connection that is still open after the step timeout means a hung sink.
        """
        self._receiver.done.set()
        self._receiver.thread.join(timeout_s)
        if self._receiver.thread.is_alive():
            raise TimeoutError(f"the sink's connection was still open {timeout_s} s after the sink stopped")
        return jsonl_to_rows(schema, b"".join(self._receiver.received))
