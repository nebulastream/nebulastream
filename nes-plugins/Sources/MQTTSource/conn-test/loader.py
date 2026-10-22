# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Seeds the MQTT source by publishing to the topic that it subscribes to.

Each row is one message, as a device would send it, and the source appends its delimiter to each,
so the source delivers exactly the JSONL that the framework compares against.
"""

from __future__ import annotations

import contextlib
import threading
import uuid
import weakref
from typing import TYPE_CHECKING, Any

import paho.mqtt.client as mqtt

from conntest_runner.contracts import SourceLoader
from conntest_runner.datamodel import NativeColumn, rows_to_jsonl
from conntest_runner.endpoint import split_endpoint

if TYPE_CHECKING:
    from collections.abc import Iterable

_CONNECT_TIMEOUT_S = 10.0
_PUBLISH_TIMEOUT_S = 60.0
_QOS = 1


def _close(client: mqtt.Client) -> None:
    with contextlib.suppress(Exception):
        client.disconnect()
    client.loop_stop()


class MqttSourceLoader(SourceLoader):
    def __init__(self, endpoint: str | None) -> None:
        if endpoint is None:
            raise ValueError("the MQTT loader needs the broker endpoint from conn-test/compose.yaml")
        super().__init__(endpoint)
        host, port = split_endpoint(endpoint)
        self._schema: list[NativeColumn] = []
        connected = threading.Event()
        self._client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"conntest-loader-{uuid.uuid4().hex}")

        def on_connect(_client: mqtt.Client, _userdata: object, _flags: object, reason: Any, _props: object) -> None:
            if not reason.is_failure:
                connected.set()

        self._client.on_connect = on_connect
        self._client.connect(host, port)
        self._client.loop_start()
        # The finalizer holds only the client, so it does not keep the loader alive.
        self._finalizer = weakref.finalize(self, _close, self._client)
        # A publish before the CONNACK is rejected, not queued.
        if not connected.wait(_CONNECT_TIMEOUT_S):
            raise TimeoutError(f"no CONNACK from the broker at {endpoint} within {_CONNECT_TIMEOUT_S}s")

    def config_overrides(self, *, target: str, endpoint: str) -> dict[str, str]:
        return {"SERVER_URI": f"tcp://{endpoint}", "TOPIC": target}

    def setup(self, *, target: str, schema: list[NativeColumn] | None = None) -> None:  # noqa: ARG002  # a topic needs no creation
        self._schema = schema or []

    def seed_batch(self, rows: Iterable[tuple[Any, ...]], *, target: str) -> None:
        """Returns once the broker has acknowledged every message, so a following FILL can read all of them."""
        messages = [
            self._client.publish(target, line, qos=_QOS) for line in rows_to_jsonl(self._schema, rows).splitlines()
        ]
        for message in messages:
            message.wait_for_publish(_PUBLISH_TIMEOUT_S)
            if not message.is_published():
                raise TimeoutError(f"the broker did not acknowledge a message on {target!r} within {_PUBLISH_TIMEOUT_S}s")
