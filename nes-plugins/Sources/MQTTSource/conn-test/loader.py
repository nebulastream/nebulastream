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
(see ``nes-conn-test/runner/conntest_runner/discovery.py``). The class
constants (``system``, ``compose_service``, ``compose_port``) drive
ComposeSystem; ``make_target`` / ``config_overrides`` let the framework
render a pristine ``.nesql`` into a runnable config; ``seed`` pushes the
fixture into the broker under a per-test topic.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

import paho.mqtt.client as paho_mqtt
from paho.mqtt.enums import CallbackAPIVersion

from conntest_runner.datamodel import ByteModel
from conntest_runner.naming import unique_token

if TYPE_CHECKING:
    from conntest_runner.datamodel import ByteData

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


class MqttLoader:
    """Per-plugin TestDataLoader for the MQTT source.

    The class name is conventional, not load-bearing: discovery picks the
    single duck-typed match in this module.
    """

    system: ClassVar[str] = "mqtt"
    compose_service: ClassVar[str] = "broker"
    compose_port: ClassVar[int] = 1883

    # MQTT is a byte stream: the framework's ByteModel owns load/quota/compare
    # (source round-trip verified by the reply SHA).
    data_model: ClassVar[ByteModel] = ByteModel()

    # Scenarios are declared in the sibling conn-test/scenarios.py (read by the
    # runner host without importing this loader's paho dependency).

    def __init__(self, endpoint: str) -> None:
        self._endpoint = endpoint
        host, _, port = endpoint.rpartition(":")
        if not host or not port:
            raise ValueError(f"MqttLoader: endpoint must be 'host:port', got {endpoint!r}")
        self._host, self._port = host, int(port)

    def make_target(self, *, test_id: str, scenario: str, worker_id: str) -> str:
        """Per-test MQTT topic.

        Path-like to keep MQTT brokers happy with ACL hierarchies and to make
        leaked retained messages easy to spot in monitoring. Uniqueness comes
        from the central ``unique_token``.
        """
        return "conntest/" + unique_token(test_id, scenario, worker_id)

    def config_overrides(self, *, target: str, endpoint: str) -> dict[str, str]:
        """Config keys the framework swaps in by value.

        Point it at ``endpoint`` (host:port) and the per-test ``target`` topic.
        """
        return {"serveruri": f"tcp://{endpoint}", "topic": target}

    def seed(self, data: ByteData, *, target: str, qos: int = 1) -> None:
        """Publish the dataset's bytes to ``target``.

        Called by the catalog after OPEN, so the message lands on the harness's
        already-wired subscription (open() returns post-SUBACK). ``qos`` defaults
        to 1 (blocks until acked); the outage scenarios pass qos=0 to exercise the
        broker's offline QoS-0 queuing policy (queue_qos0_messages).
        """
        _publish(self._host, self._port, target, data.data, qos=qos)
