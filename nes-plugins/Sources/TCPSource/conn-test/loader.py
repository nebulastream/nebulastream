# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Seeds the TCP source, which is a client, by being the server that it connects to.

The source's configs use the JSON input formatter, so the rows are written as JSONL.
"""

from __future__ import annotations

import contextlib
import select
import socket
import weakref
from concurrent.futures import Future, ThreadPoolExecutor
from typing import TYPE_CHECKING, Any

from conntest_runner.contracts import SourceLoader
from conntest_runner.datamodel import NativeColumn, rows_to_jsonl
from conntest_runner.endpoint import split_endpoint

if TYPE_CHECKING:
    from collections.abc import Iterable

_ACCEPT_TIMEOUT_S = 10.0


def _close(sockets: list[socket.socket], sender: ThreadPoolExecutor) -> None:
    sender.shutdown(wait=False, cancel_futures=True)
    for sock in sockets:
        with contextlib.suppress(OSError):
            sock.close()


class TcpSourceLoader(SourceLoader):
    """Discovery takes the single concrete loader class in this module, so its name is free."""

    def __init__(self, endpoint: str | None) -> None:  # noqa: ARG002  # TCP has no compose service, so endpoint is None
        # The harness runs in the runner's container, so the source reaches the loader on loopback.
        # Each case has its own loader and port, so no two cases share a connection.
        self._listener = socket.create_server(("127.0.0.1", 0))
        self._listener.settimeout(_ACCEPT_TIMEOUT_S)
        super().__init__(f"127.0.0.1:{self._listener.getsockname()[1]}")
        self._sockets = [self._listener]
        self._conn: socket.socket | None = None
        # The source reads only during the FILL that follows a seed, and a large batch exceeds the socket buffers,
        # so the batch is sent in the background.
        # One worker keeps the batches in order.
        self._sender = ThreadPoolExecutor(max_workers=1)
        self._sent: Future[None] | None = None
        self._schema: list[NativeColumn] = []
        # The finalizer holds only the sockets and the sender, so it does not keep the loader alive.
        self._finalizer = weakref.finalize(self, _close, self._sockets, self._sender)

    def config_overrides(self, *, target: str, endpoint: str) -> dict[str, str | int]:  # noqa: ARG002  # TCP has no topic or table
        host, port = split_endpoint(endpoint)
        return {"SOCKET_HOST": host, "SOCKET_PORT": port}

    def setup(self, *, target: str, schema: list[NativeColumn] | None = None) -> None:  # noqa: ARG002  # the listener is already up
        self._schema = schema or []

    def seed_batch(self, rows: Iterable[tuple[Any, ...]], *, target: str) -> None:  # noqa: ARG002  # one connection per case
        if self._sent is not None:
            # Reports a failed send of the previous batch.
            self._sent.result()
        self._sent = self._sender.submit(self._connection().sendall, rows_to_jsonl(self._schema, rows))

    def _connection(self) -> socket.socket:
        """A respawned source connects again, and its connection replaces the dead one."""
        while self._conn is None or select.select([self._listener], [], [], 0)[0]:
            self._conn, _ = self._listener.accept()
            self._sockets.append(self._conn)
        return self._conn
