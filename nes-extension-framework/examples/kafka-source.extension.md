---
type: source
name: KafkaSource
plugin: true
---

## Description

A source that reads records from an Apache Kafka topic using the librdkafka C++ client library.
Records are delivered as raw bytes into TupleBuffers and parsed by the configured InputFormatter.
The source reconnects automatically if the broker connection drops.

## Config Parameters

- BROKER_LIST (string, required): Comma-separated list of Kafka broker addresses (e.g. "localhost:9092,broker2:9092")
- TOPIC (string, required): Name of the Kafka topic to consume from
- GROUP_ID (string, optional, default=nebulastream): Consumer group identifier
- PARTITION (int, optional, default=0): Partition index to consume from
- START_OFFSET (string, optional, default=latest): Where to start consuming — "earliest" or "latest"

## Behavior

`open()`: Create the librdkafka consumer handle using BROKER_LIST and GROUP_ID. Subscribe to TOPIC at PARTITION starting from START_OFFSET. Throw `InvalidConfigParameter` if the broker is unreachable after a timeout.

`fillTupleBuffer()`: Poll for messages up to the available buffer capacity. Copy each message payload into the TupleBuffer using `getAvailableMemoryArea<char>()`. Return `FillTupleBufferResult::withBytes(n)` after each successful fill, or `FillTupleBufferResult::eos()` when the stop token is set.

`close()`: Unsubscribe and destroy the consumer handle.

## Examples

- Kafka message `{"id": 1, "value": 42.0}` → raw bytes copied into TupleBuffer, parsed as JSON by InputFormatter
- Stop token set while polling → return eos() immediately without waiting for next message
