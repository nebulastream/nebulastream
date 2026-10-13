# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""MQTT sink TestDataLoader for the conn-test harness.

Discovered by the runner via the per-plugin ``conn-test/loader.py`` glob.
Sink-side flow (inverse of the MQTT source): the harness slices the runner's JSONL
into TupleBuffers and drains them through MQTTSink, which publishes each buffer's
bytes as one MQTT message; ``read_back`` collects the messages and parses them back
into typed rows (via the framework's ``jsonl_to_rows``) for the order-insensitive
multiset compare. No formatting happens in the sink — it carries the bytes through
(formatting is query-compiled separately in the real engine); the JSONL form is the
framework's, and the loader parses it with the framework's single deserializer.

Read-back uses a **single persistent subscriber**. The framework keeps one
loader instance per scenario and calls ``setup`` (before the sink starts) then
``read_back`` (after it stops) on it, so ``setup`` opens one client, subscribes,
and leaves it connected and buffering on ``self`` for ``read_back`` to drain.
The subscription is established before ``setup`` returns (it waits for the
SUBACK), which matters because MQTT does not retain ordinary messages — a
subscriber that is not yet live when the sink publishes would miss them. A
``weakref.finalize`` closes the client even on the paths that never reach
``read_back`` (the ``empty`` scenario, mid-scenario failures).

MQTTSink connects eagerly in ``start()``, so an unreachable broker surfaces as
CannotOpenSink (4004) at the open phase — the framework default for
``bad_endpoint`` — hence no ``BAD_ENDPOINT_ERROR_CODE`` override.
"""

from __future__ import annotations

import contextlib
import threading
import time
import weakref
from typing import Any

import paho.mqtt.client as paho_mqtt
from paho.mqtt.enums import CallbackAPIVersion

from conntest_runner.contracts import SinkLoader
from conntest_runner.datamodel import NativeColumn, jsonl_to_rows
from conntest_runner.endpoint import split_endpoint

_SUBSCRIBE_ACK_TIMEOUT_S = 10.0
# Once at least one message has arrived, stop collecting after this long with no
# new message — the sink's publishes are delivered back-to-back, so a short quiet
# window means "delivery is done".
_IDLE_TIMEOUT_S = 1.0
# Overall ceiling on the read_back drain (the framework does not pass one).
_READ_BACK_TIMEOUT_S = 30.0


def _count_records(messages: list[bytes]) -> int:
    r"""Total non-blank '\\n'-delimited records across all message payloads."""
    return sum(1 for m in messages for line in m.split(b"\n") if line.strip())


def _close_client(client: paho_mqtt.Client) -> None:
    """Stop the network loop and disconnect. Safe to call more than once."""
    # Teardown must not mask the scenario result, so swallow late-disconnect noise.
    with contextlib.suppress(Exception):
        client.loop_stop()
    with contextlib.suppress(Exception):
        client.disconnect()


class MqttSinkLoader(SinkLoader):
    """Per-plugin TestDataLoader for the MQTT sink.

    The class name is conventional, not load-bearing: discovery picks the single
    duck-typed match in this module.
    """

    def __init__(self, endpoint: str) -> None:
        self._endpoint = endpoint
        self._host, self._port = split_endpoint(endpoint)

    def config_overrides(self, *, target: str, endpoint: str) -> dict[str, str]:
        """Config keys the framework swaps in by value.

        Point it at ``endpoint`` (host:port) and the per-test ``target`` topic.
        """
        return {"server_uri": f"tcp://{endpoint}", "topic": target}

    def setup(self, *, target: str, schema: object = None) -> None:  # noqa: ARG002  # formatted sink ignores schema
        """Open one persistent QoS-1 subscriber and leave it connected.

        Establishes the subscription before returning (waits for the SUBACK), so
        every message the sink later publishes lands on a live subscription —
        MQTT does not retain ordinary messages, so a subscriber that is not yet
        live would miss them. The client, buffer, and lock are stashed on
        ``self`` and drained in ``read_back``. ``schema`` is accepted (the
        framework passes it uniformly) but unused — a formatted sink does not key
        off it.
        """
        messages: list[bytes] = []
        lock = threading.Lock()
        subscribed = threading.Event()

        # The callbacks capture the buffer/lock/event, NOT self, so the finalizer
        # registered below can collect this loader (and close the client) promptly.
        def on_subscribe(
            client: paho_mqtt.Client,  # noqa: ARG001
            userdata: object,  # noqa: ARG001
            mid: int,  # noqa: ARG001
            reason_code_list: object,  # noqa: ARG001
            properties: object,  # noqa: ARG001
        ) -> None:
            subscribed.set()

        def on_message(
            client: paho_mqtt.Client,  # noqa: ARG001
            userdata: object,  # noqa: ARG001
            msg: paho_mqtt.MQTTMessage,
        ) -> None:
            with lock:
                messages.append(bytes(msg.payload))

        client = paho_mqtt.Client(CallbackAPIVersion.VERSION2)
        client.on_subscribe = on_subscribe
        client.on_message = on_message
        client.connect(self._host, self._port, keepalive=60)
        client.loop_start()

        ok = False
        try:
            client.subscribe(target, qos=1)
            if not subscribed.wait(timeout=_SUBSCRIBE_ACK_TIMEOUT_S):
                raise TimeoutError(
                    f"MqttSinkLoader.setup: no SUBACK within {_SUBSCRIBE_ACK_TIMEOUT_S}s "
                    f"(topic={target!r})"
                )
            ok = True
        finally:
            if not ok:
                _close_client(client)  # a failed setup must not leak the loop thread

        self._client = client
        self._messages = messages
        self._lock = lock
        # Close even if read_back() is never called (the `empty` scenario, or a
        # mid-scenario failure). Capture the client only — not self — so this
        # finalizer does not keep the loader alive.
        self._finalizer = weakref.finalize(self, _close_client, client)

    def read_back(
        self,
        *,
        target: str,  # noqa: ARG002  # subscription already bound in setup()
        expect_records: int,
        schema: list[NativeColumn],
    ) -> list[tuple[Any, ...]]:
        """Return the records the persistent subscriber collected since setup(), as rows.

        Each collected message is a batch of whole JSONL records; this parses them back
        into typed rows with the framework's ``jsonl_to_rows`` (keyed by ``schema``) — the
        single deserializer, so the loader does no decoding of its own. Waits until
        ``expect_records`` records have arrived, else ``_IDLE_TIMEOUT_S`` with no new
        message (once at least one arrived), else ``_READ_BACK_TIMEOUT_S``. Closes the
        client on every path.
        """
        try:
            if expect_records == 0:
                return []

            deadline = time.monotonic() + _READ_BACK_TIMEOUT_S
            last_count = 0
            last_change = time.monotonic()
            while time.monotonic() < deadline:
                with self._lock:
                    count = len(self._messages)
                    records = _count_records(self._messages)
                if records >= expect_records:
                    break
                if count != last_count:
                    last_count = count
                    last_change = time.monotonic()
                elif count > 0 and (time.monotonic() - last_change) >= _IDLE_TIMEOUT_S:
                    break
                time.sleep(0.05)

            with self._lock:
                payloads = list(self._messages)
            rows: list[tuple[Any, ...]] = []
            for payload in payloads:
                rows.extend(jsonl_to_rows(schema, payload))
            return rows
        finally:
            self._finalizer()  # idempotent: runs _close_client once
