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
Sink-side flow (inverse of the MQTT source): the harness packs a formatted CSV
fixture into TupleBuffers and drains them through MQTTSink, which publishes each
buffer's bytes as one MQTT message; ``read_back`` collects the messages and the
battery flattens them to records for the order-insensitive multiset compare.

Read-back uses a **persistent session** (MQTT ``clean_session=False``):
``prepare`` registers a durable subscription under a per-target client id so the
broker queues the sink's QoS-1 publishes while the subscriber is offline, and
``read_back`` reconnects with the same id to drain the queue. The client id is
derived deterministically from the target topic, so prepare/read_back agree
without sharing loader state.

MQTTSink connects eagerly in ``start()``, so an unreachable broker surfaces as
CannotOpenSink (4004) at the open phase — the framework default for
``bad_endpoint`` — hence no ``BAD_ENDPOINT_ERROR_CODE`` override.
"""

from __future__ import annotations

import hashlib
import threading
import time
from typing import ClassVar

import paho.mqtt.client as paho_mqtt
from paho.mqtt.enums import CallbackAPIVersion

from conntest_runner.datamodel import ByteModel
from conntest_runner.naming import unique_token

_SUBSCRIBE_ACK_TIMEOUT_S = 10.0
# Once at least one message has arrived, stop collecting after this long with no
# new message — a queued burst is delivered back-to-back on reconnect, so a short
# quiet window means "the broker is done".
_IDLE_TIMEOUT_S = 1.0


def _count_records(messages: list[bytes]) -> int:
    r"""Total non-blank '\\n'-delimited records across all message payloads."""
    return sum(1 for m in messages for line in m.split(b"\n") if line.strip())


def _register_durable_subscription(
    host: str,
    port: int,
    topic: str,
    *,
    client_id: str,
    qos: int = 1,
    timeout_s: float = _SUBSCRIBE_ACK_TIMEOUT_S,
) -> None:
    """Register a durable (clean_session=False) subscription, then disconnect.

    The broker queues QoS>=1 messages for ``client_id`` until
    ``_subscribe_collect`` reconnects with the same id.

    Uses paho-mqtt's VERSION2 callback API; ``clean_session`` remains valid
    under the default MQTTv311 protocol.
    """
    subscribed = threading.Event()

    def on_subscribe(
        client: paho_mqtt.Client,  # noqa: ARG001
        userdata: object,  # noqa: ARG001
        mid: int,  # noqa: ARG001
        reason_code_list: object,  # noqa: ARG001
        properties: object,  # noqa: ARG001
    ) -> None:
        subscribed.set()

    client = paho_mqtt.Client(CallbackAPIVersion.VERSION2, client_id=client_id, clean_session=False)
    client.on_subscribe = on_subscribe
    client.connect(host, port, keepalive=60)
    client.loop_start()
    try:
        client.subscribe(topic, qos=qos)
        if not subscribed.wait(timeout=timeout_s):
            raise TimeoutError(
                f"MqttSinkLoader._register_durable_subscription: no SUBACK within "
                f"{timeout_s}s (topic={topic!r}, client_id={client_id!r})"
            )
    finally:
        client.loop_stop()
        client.disconnect()


def _subscribe_collect(
    host: str,
    port: int,
    topic: str,
    *,
    client_id: str,
    clean_session: bool = False,
    qos: int = 1,
    expect_records: int | None = None,
    timeout_s: float = 30.0,
    idle_timeout_s: float = _IDLE_TIMEOUT_S,
) -> list[bytes]:
    """Reconnect to the durable session and drain the payloads the broker queued.

    Returns the message payloads (each a batch of whole records). Stops once
    ``expect_records`` flattened records have arrived, else after ``idle_timeout_s``
    with no new message (once at least one arrived), else at ``timeout_s``.
    """
    if expect_records == 0:
        return []

    messages: list[bytes] = []
    lock = threading.Lock()

    def on_message(
        client: paho_mqtt.Client,  # noqa: ARG001
        userdata: object,  # noqa: ARG001
        msg: paho_mqtt.MQTTMessage,
    ) -> None:
        with lock:
            messages.append(bytes(msg.payload))

    client = paho_mqtt.Client(
        CallbackAPIVersion.VERSION2, client_id=client_id, clean_session=clean_session
    )
    client.on_message = on_message
    client.connect(host, port, keepalive=60)
    client.loop_start()
    try:
        # Harmless on a durable reconnect (queued messages arrive on connect
        # regardless); required for a fresh live subscription.
        client.subscribe(topic, qos=qos)
        deadline = time.monotonic() + timeout_s
        last_count = 0
        last_change = time.monotonic()
        while time.monotonic() < deadline:
            with lock:
                count = len(messages)
                records = _count_records(messages)
            if expect_records is not None and records >= expect_records:
                break
            if count != last_count:
                last_count = count
                last_change = time.monotonic()
            elif count > 0 and (time.monotonic() - last_change) >= idle_timeout_s:
                break
            time.sleep(0.05)
    finally:
        client.loop_stop()
        client.disconnect()

    with lock:
        return list(messages)


class MqttSinkLoader:
    """Per-plugin TestDataLoader for the MQTT sink.

    The class name is conventional, not load-bearing: discovery picks the single
    duck-typed match in this module.
    """

    system: ClassVar[str] = "mqtt"
    compose_service: ClassVar[str] = "broker"
    compose_port: ClassVar[int] = 1883

    # The sink writes CSV records through; ByteModel owns the input quota
    # (record count) and the order-insensitive record-multiset read-back compare.
    data_model: ClassVar[ByteModel] = ByteModel()

    def __init__(self, endpoint: str) -> None:
        self._endpoint = endpoint
        host, _, port = endpoint.rpartition(":")
        if not host or not port:
            raise ValueError(f"MqttSinkLoader: endpoint must be 'host:port', got {endpoint!r}")
        self._host, self._port = host, int(port)

    # ---- framework hooks --------------------------------------------------
    def make_target(self, *, test_id: str, scenario: str, worker_id: str) -> str:
        """Per-test MQTT topic.

        Path-like, ``conntest/sink/...`` prefixed to keep it distinct from the
        MQTT *source* topics and easy to spot. Uniqueness comes from the central
        ``unique_token``.
        """
        return "conntest/sink/" + unique_token(test_id, scenario, worker_id)

    def config_overrides(self, *, target: str, endpoint: str) -> dict[str, str]:
        """Config keys the framework swaps in by value.

        Point it at ``endpoint`` (host:port) and the per-test ``target`` topic.
        """
        return {"server_uri": f"tcp://{endpoint}", "topic": target}

    def _client_id(self, target: str) -> str:
        """Stable per-target durable-session client id.

        MQTT v3.1.1 allows long ids; mosquitto accepts these. Deterministic so
        prepare() and read_back() name the same session without sharing instance
        state.
        """
        return "cts-" + hashlib.sha1(target.encode("utf-8"), usedforsecurity=False).hexdigest()[:20]

    def setup(self, *, target: str) -> None:
        """Register a durable (clean_session=False) subscription for read_back.

        The broker queues the sink's QoS-1 publishes. Runs before the harness
        starts, so every message the sink publishes is retained for the session.
        """
        _register_durable_subscription(
            self._host, self._port, target, client_id=self._client_id(target), qos=1
        )

    def read_back(
        self, *, target: str, expect_records: int, timeout_s: float = 30.0
    ) -> list[bytes]:
        """Drain the durable session's queued messages.

        Returns the message payloads (each a batch of whole records); the battery
        flattens them. Stops early once ``expect_records`` records have arrived.
        """
        return _subscribe_collect(
            self._host,
            self._port,
            target,
            client_id=self._client_id(target),
            clean_session=False,
            qos=1,
            expect_records=expect_records,
            timeout_s=timeout_s,
        )
