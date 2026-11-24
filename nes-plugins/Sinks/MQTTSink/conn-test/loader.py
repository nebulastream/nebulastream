# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Loader for the MQTT sink: it subscribes to each test's topic before the sink starts and collects what the sink publishes.

The sink publishes one message for each buffer, which includes the buffer's child buffers,
so each message holds whole JSONL records and the loader parses each message on its own.
"""

from __future__ import annotations

import contextlib
import threading
import uuid
import weakref
from typing import Any

import paho.mqtt.client as mqtt

from conntest_runner.contracts import SinkLoader
from conntest_runner.datamodel import NativeColumn, jsonl_to_rows
from conntest_runner.endpoint import split_endpoint

_CONNECT_TIMEOUT_S = 10.0
_SUBSCRIBE_TIMEOUT_S = 10.0
# The broker delivers at the lower of the publish and the subscription QoS, so the loader receives every message with at most QoS 1.
# The client reports a QoS 2 message only after its PUBREL, so with a QoS 2 subscription it could report the marker first.
_QOS = 1


def _close(client: mqtt.Client) -> None:
    with contextlib.suppress(Exception):
        client.disconnect()
    client.loop_stop()


class MqttSinkLoader(SinkLoader):
    """Discovery picks the only concrete loader class in this module, so the class can have any name."""

    def __init__(self, endpoint: str | None) -> None:
        if endpoint is None:
            raise ValueError("the MQTT loader needs the broker endpoint from conn-test/compose.yaml")
        super().__init__(endpoint)
        host, port = split_endpoint(endpoint)
        # The finalizer holds the client and its callbacks, so a callback that uses self would keep the loader alive.
        # Guards the collected messages and the acknowledged subscriptions, which the client's network thread adds to.
        changed = self._changed = threading.Condition()
        # The messages of each subscribed topic, in the order in which they arrived.
        messages: dict[str, list[bytes]] = {}
        # The id of each acknowledged subscribe request, and whether the broker granted it.
        subscriptions: dict[int, bool] = {}
        self._messages, self._subscriptions = messages, subscriptions
        connected = threading.Event()
        self._client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"conntest-loader-{uuid.uuid4().hex}")

        def on_connect(_client: mqtt.Client, _userdata: object, _flags: object, reason: Any, _props: object) -> None:
            if not reason.is_failure:
                connected.set()

        def on_subscribe(_client: mqtt.Client, _userdata: object, mid: int, reasons: list[Any], _props: object) -> None:
            with changed:
                subscriptions[mid] = not any(reason.is_failure for reason in reasons)
                changed.notify_all()

        def on_message(_client: mqtt.Client, _userdata: object, message: mqtt.MQTTMessage) -> None:
            with changed:
                # A topic whose read-back has ended is no longer collected.
                if message.topic in messages:
                    messages[message.topic].append(message.payload)
                    changed.notify_all()

        self._client.on_connect = on_connect
        self._client.on_subscribe = on_subscribe
        self._client.on_message = on_message
        self._client.connect(host, port)
        self._client.loop_start()
        self._finalizer = weakref.finalize(self, _close, self._client)
        if not connected.wait(_CONNECT_TIMEOUT_S):
            raise TimeoutError(f"no CONNACK from the broker at {endpoint} within {_CONNECT_TIMEOUT_S} s")

    def config_overrides(self, *, target: str, endpoint: str) -> dict[str, str | int]:
        return {"SERVER_URI": f"tcp://{endpoint}", "TOPIC": target}

    def setup(self, *, target: str, schema: list[NativeColumn] | None = None) -> None:  # noqa: ARG002  # a topic has no schema
        """Subscribes to the topic, because the broker delivers a message only to the subscriptions that exist when it arrives."""
        with self._changed:
            self._messages[target] = []
        result, mid = self._client.subscribe(target, qos=_QOS)
        if result != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"the loader could not subscribe to topic {target!r}: {mqtt.error_string(result)}")
        with self._changed:
            if not self._changed.wait_for(lambda: mid in self._subscriptions, timeout=_SUBSCRIBE_TIMEOUT_S):
                raise TimeoutError(f"no SUBACK for topic {target!r} within {_SUBSCRIBE_TIMEOUT_S} s")
            if not self._subscriptions[mid]:
                raise RuntimeError(f"the broker refused the subscription to topic {target!r}")

    def read_back(
        self,
        *,
        target: str,
        schema: list[NativeColumn],
        n_rows: int,  # noqa: ARG002  # the end marker marks the end of the data
        timeout_s: float,
    ) -> list[tuple[Any, ...]]:
        # A row count cannot end the read-back, because a case without rows must still see a wrong message.
        # mosquitto queues a message for its subscribers before it sends the PUBACK or PUBCOMP.
        # The sink stops only once it has received one of them for every message, so mosquitto queues the marker last.
        marker = f"conntest-end-{uuid.uuid4().hex}".encode()
        self._client.publish(target, marker, qos=_QOS)
        with self._changed:
            if not self._changed.wait_for(lambda: marker in self._messages[target], timeout=timeout_s):
                raise TimeoutError(f"the loader did not receive its end marker on topic {target!r} within {timeout_s} s")
            messages = self._messages.pop(target)
        self._client.unsubscribe(target)
        rows: list[tuple[Any, ...]] = []
        for index, payload in enumerate(messages[: messages.index(marker)]):
            # Every record ends with a newline, but the codec also accepts an empty payload or a missing final newline.
            if not payload.endswith(b"\n"):
                raise AssertionError(f"message {index} on the topic does not end with a newline: {payload[-80:]!r}")
            rows += jsonl_to_rows(schema, payload)
        return rows
