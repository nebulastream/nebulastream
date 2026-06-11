# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""TCP source TestDataLoader for the conn-test harness.

Discovered by the runner via the per-plugin ``conn-test/loader.py`` glob.
The data service is the sibling ``tcp_relay.py`` (mounted via ``setup_file``):
the loader connects to the relay's loader-side port and writes seed bytes; the
relay pipes them to whatever connector connection the harness opened on the
exposed port. The TCP source is configured for the JSON input formatter, so
its wire form is JSONL: the loader serializes the rows with the framework's
``rows_to_jsonl`` (the single serializer — it never hand-rolls a format) and
the engine's JSON formatter parses them back.

Port contract (see tcp_relay.py): the loader-side port is the exposed
connector port + 1. The loader derives it from the compose endpoint the
framework passes to ``__init__`` — which is always the real relay service,
never the toxiproxy/fault endpoint, so the seed path stays off the faulted
link (the oracle-bypasses-the-proxy invariant).

The seed connection is opened once in ``setup`` (the framework keeps one
loader instance per scenario and sequences setup before the connector's OPEN)
and stays open across every ``seed_batch`` of the case; a ``weakref.finalize``
closes it on every path, which the relay propagates to the connector as a
clean end-of-stream.
"""

from __future__ import annotations

import contextlib
import socket
import weakref
from typing import TYPE_CHECKING, Any

from conntest_runner.contracts import SourceLoader
from conntest_runner.datamodel import NativeColumn, rows_to_jsonl
from conntest_runner.endpoint import split_endpoint

if TYPE_CHECKING:
    from collections.abc import Iterable

_CONNECT_TIMEOUT_S = 10.0


def _close(sock: socket.socket) -> None:
    """Close the seed connection. Safe to call more than once."""
    with contextlib.suppress(OSError):
        sock.close()


class TcpSourceLoader(SourceLoader):
    """Per-plugin TestDataLoader for the TCP source.

    The class name is conventional, not load-bearing: discovery picks the
    single duck-typed match in this module.
    """

    def __init__(self, endpoint: str) -> None:
        host, port = split_endpoint(endpoint)
        # Loader-side relay port = exposed connector port + 1 (tcp_relay.py).
        self._seed_address = (host, port + 1)
        # The schema the framework parses from the config, cached by setup() so
        # seed_batch() can render the generated rows to JSONL.
        self._schema: list[NativeColumn] = []

    def config_overrides(self, *, target: str, endpoint: str) -> dict[str, str]:  # noqa: ARG002  # target unused: TCP has no topic/table namespace
        """Config keys the framework swaps in by value.

        Only the endpoint is rewritten. ``target`` has no TCP equivalent — a
        bare byte stream has no topic/table namespace; per-case isolation comes
        from the relay pairing each case's fresh connections instead.
        """
        host, port = split_endpoint(endpoint)
        return {"socket_host": host, "socket_port": str(port)}

    def setup(self, *, target: str, schema: list[NativeColumn] | None = None) -> None:  # noqa: ARG002  # target unused: no backend resource to create
        """Open the seed connection to the relay and cache the schema.

        Connecting here (not lazily in ``seed_batch``) pins the case boundary:
        the relay treats a new loader connection as "new test case" and resets
        its session state before the connector's OPEN arrives.
        """
        self._schema = schema or []
        sock = socket.create_connection(self._seed_address, timeout=_CONNECT_TIMEOUT_S)
        self._sock = sock
        # Close even on paths that never finish the scenario; the relay turns
        # the close into a clean end-of-stream toward the connector. Capture
        # the socket only — not self — so this finalizer does not keep the
        # loader alive.
        self._finalizer = weakref.finalize(self, _close, sock)

    def seed_batch(self, rows: Iterable[tuple[Any, ...]], *, target: str) -> None:  # noqa: ARG002  # target unused: the relay routes by connection, not name
        """Serialize the framework's generated ``rows`` to JSONL and write them.

        ``sendall`` returns once the kernel accepted every byte; the relay (and
        the framework's sequencing — the connector is connected before transfer
        starts) takes it from there.
        """
        self._sock.sendall(rows_to_jsonl(self._schema, rows))
