# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Loader for the Kafka sink: it creates each test's topic and reads back what the sink produced to it.

The sink produces one message for each buffer, which includes the buffer's child buffers,
so each message holds whole JSONL records and the loader parses each message on its own.
The sink's configs declare the JSON output format.
"""

from __future__ import annotations

import time
import uuid
from typing import Any

from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from conntest_runner.contracts import SinkLoader
from conntest_runner.datamodel import NativeColumn, jsonl_to_rows
from conntest_runner.endpoint import split_endpoint

# The broker's listener that gives out the fault proxy's address, see conn-test/compose.yaml.
_PROXIED_PORT = 19092
_ADMIN_TIMEOUT_S = 30.0
# The loader creates each test's topic with one partition, so every message that the sink produces is in partition 0.
_PARTITION = 0
# How long one poll waits, so that the read-back checks its deadline in between.
_POLL_S = 1.0


class KafkaSinkLoader(SinkLoader):
    """Discovery picks the only concrete loader class in this module, so the class can have any name."""

    def __init__(self, endpoint: str | None) -> None:
        if endpoint is None:
            raise ValueError("the Kafka loader needs the broker endpoint from conn-test/compose.yaml")
        super().__init__(endpoint)
        host, _ = split_endpoint(endpoint)
        self.link_endpoint = f"{host}:{_PROXIED_PORT}"
        self._admin = AdminClient({"bootstrap.servers": endpoint})

    def config_overrides(self, *, target: str, endpoint: str) -> dict[str, str | int]:
        return {"BROKERS": endpoint, "TOPIC": target}

    def setup(self, *, target: str, schema: list[NativeColumn] | None = None) -> None:  # noqa: ARG002  # a topic has no schema
        """Creates the topic, because the broker creates none for a producer, so a sink that produces to another topic fails."""
        created = self._admin.create_topics([NewTopic(target, num_partitions=1, replication_factor=1)], operation_timeout=_ADMIN_TIMEOUT_S)
        created[target].result(_ADMIN_TIMEOUT_S)

    def read_back(
        self,
        *,
        target: str,
        schema: list[NativeColumn],
        n_rows: int,  # noqa: ARG002  # the partition's end marks the end of the data
        timeout_s: float,
    ) -> list[tuple[Any, ...]]:
        """Reads the topic's partition from its first message to its end.

        The sink stops only once every message is delivered or has failed, so the partition's end does not move after it has stopped.
        A message that failed is missing from the topic, and the case fails with missing rows.
        An empty message is the sink's output for a buffer without tuples, an empty record that a sink must not deliver.
        """
        deadline = time.monotonic() + timeout_s
        # The client requires a group id, although the consumer assigns its partition and commits nothing.
        # The group makes the broker create its offsets topic, which conn-test/compose.yaml does before the first test.
        # With partition EOF, the consumer reports when it has read the partition to its end, also when the partition is empty.
        consumer = Consumer(
            {
                "bootstrap.servers": self.endpoint,
                "group.id": f"conntest-loader-{uuid.uuid4().hex}",
                "enable.auto.commit": False,
                "enable.partition.eof": True,
            }
        )
        try:
            consumer.assign([TopicPartition(target, _PARTITION, OFFSET_BEGINNING)])
            rows: list[tuple[Any, ...]] = []
            while True:
                remaining_s = deadline - time.monotonic()
                if remaining_s <= 0:
                    raise TimeoutError(f"the loader did not read topic {target!r} to its end within {timeout_s} s")
                message = consumer.poll(min(remaining_s, _POLL_S))
                if message is None:
                    continue
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        return rows
                    raise KafkaException(message.error())
                if not message.value():
                    raise AssertionError(f"the sink produced an empty message at offset {message.offset()}, which is an empty record")
                rows += jsonl_to_rows(schema, message.value())
        finally:
            consumer.close()
