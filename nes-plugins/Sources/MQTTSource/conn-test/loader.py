# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""MQTT TestDataLoader for the conn-test harness.

Discovered by the runner via the per-plugin ``conn-test/loader.py`` glob
(see ``nes-conn-test/runner/conntest_runner/discovery.py``). Topology (the
broker service + port) is framework-derived from the sibling compose.yaml, and
the per-test topic is framework-assigned; the loader maps the framework's dynamic
values onto its config keys (``config_overrides``) and publishes the framework's
generated rows (``seed_batch``). The MQTT source is configured for the JSON input
formatter, so its wire form is JSONL: the loader serializes the rows with the
framework's ``rows_to_jsonl`` (the single serializer — it never hand-rolls a format)
and publishes the bytes; the engine's JSON formatter parses them back.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import paho.mqtt.client as paho_mqtt
from paho.mqtt.enums import CallbackAPIVersion

from conntest_runner.contracts import SourceLoader
from conntest_runner.datamodel import NativeColumn, rows_to_jsonl
from conntest_runner.endpoint import split_endpoint

if TYPE_CHECKING:
    from collections.abc import Iterable

_PUBLISH_TIMEOUT_S = 10.0


def _publish(
    host: str,
    port: int,
    topic: str,
    payload: bytes,
    *,
    qos: int = 1,
    retain: bool = False,
    timeout_s: float = _PUBLISH_TIMEOUT_S,
) -> None:
    """Publish a single message and block until the broker acks it (QoS>=1).

    Uses paho-mqtt's VERSION2 callback API (no callbacks are registered here,
    so the version only governs the constructor's contract).
    """
    client = paho_mqtt.Client(CallbackAPIVersion.VERSION2)
    client.connect(host, port, keepalive=60)
    client.loop_start()
    try:
        info = client.publish(topic, payload=payload, qos=qos, retain=retain)
        info.wait_for_publish(timeout=timeout_s)
        if not info.is_published():
            raise TimeoutError(
                f"MqttLoader._publish: broker did not ack PUBLISH within "
                f"{timeout_s}s (topic={topic!r})"
            )
    finally:
        client.loop_stop()
        client.disconnect()


class MqttLoader(SourceLoader):
    """Per-plugin TestDataLoader for the MQTT source.

    The class name is conventional, not load-bearing: discovery picks the
    single duck-typed match in this module.
    """

    # Scenarios are declared in the sibling conn-test/scenarios.py (read by the
    # runner host without importing this loader's paho dependency).

    def __init__(self, endpoint: str) -> None:
        self._endpoint = endpoint
        self._host, self._port = split_endpoint(endpoint)
        # The schema the framework parses from the config, cached by setup() so
        # seed_batch() can render the generated rows to JSONL.
        self._schema: list[NativeColumn] = []

    def config_overrides(self, *, target: str, endpoint: str) -> dict[str, str]:
        """Config keys the framework swaps in by value.

        Point it at ``endpoint`` (host:port) and the per-test ``target`` topic, and
        bind ``client_id`` to the same per-test token so each scenario gets its own
        MQTT session. A persistent-session config (clean_session=false) keeps its
        subscription and un-consumed backlog on the broker; a fixed, shared client_id
        would let one scenario's residue (e.g. a deliberately lossy batch) leak into
        the next that reopens that session — non-isolated and order-dependent. The
        token is unique per scenario yet stable within one, so reconnect/crash-resume
        still resume the same session.
        """
        return {"server_uri": f"tcp://{endpoint}", "topic": target, "client_id": target}

    def setup(self, *, target: str, schema: list[NativeColumn] | None = None) -> None:  # noqa: ARG002  # topic needs no pre-creation; setup only caches the schema for seed_batch
        """Cache the framework-parsed schema for ``seed_batch``; no backend to prepare.

        ``setup`` is part of the loader contract so the framework can call it
        uniformly. MQTT publishes to a topic that needs no pre-creation and the harness
        wires its own subscription, so the only work here is caching ``schema`` so
        ``seed_batch`` can render rows to JSONL (it is ``None`` for the non-data
        scenarios that never seed).
        """
        self._schema = schema or []

    def seed_batch(self, rows: Iterable[tuple[Any, ...]], *, target: str, qos: int = 1) -> None:
        """Serialize the framework's generated ``rows`` to JSONL and publish to ``target``.

        The MQTT source is configured for the JSON input formatter, so its wire form is
        JSONL: this loader renders the rows with the framework's ``rows_to_jsonl`` (the
        single serializer — it never hand-rolls a format) and publishes the bytes as one
        message; the engine parses them back. Called by the catalog after OPEN, so the
        message lands on the harness's already-wired subscription (open() returns
        post-SUBACK). ``qos`` defaults to 1 (blocks until acked); the outage scenarios
        pass qos=0 to exercise the broker's offline QoS-0 queuing policy
        (queue_qos0_messages).
        """
        _publish(self._host, self._port, target, rows_to_jsonl(self._schema, rows), qos=qos)
