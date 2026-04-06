#!/usr/bin/env python3
"""Publish CSV rows to multiple MQTT topics from concurrent clients.

Usage:
    mqtt_multi_topic_publish.py --host HOST --port PORT
                                --topics TOPIC1,TOPIC2,...
                                --clients-per-topic N --rows-per-client M
                                [--batch-size B]

Each topic gets `clients-per-topic` independent clients, each publishing
`rows-per-client` rows. Row IDs are globally unique: topic 0 client 0 gets
rows 1..M, topic 0 client 1 gets M+1..2M, topic 1 client 0 gets 2M+1..3M, etc.

Row format: <row_id>,<row_id * 10>,<row_id * 1000>
"""

import argparse
import threading
import sys

import paho.mqtt.client as mqtt


def publish_client(client_id, host, port, topic, start_row, num_rows, batch_size):
    """Connect, publish rows in batches, wait for all to be sent, disconnect."""
    c = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=client_id,
        protocol=mqtt.MQTTv311,
    )
    c.connect(host, port, keepalive=60)
    c.loop_start()

    inflight = []
    row = start_row
    end = start_row + num_rows
    while row < end:
        batch_end = min(row + batch_size, end)
        lines = []
        for r in range(row, batch_end):
            lines.append(f"{r},{r * 10},{r * 1000}")
        payload = "\n".join(lines) + "\n"
        msg_info = c.publish(topic, payload.encode(), qos=0)
        inflight.append(msg_info)
        row = batch_end

    for msg_info in inflight:
        msg_info.wait_for_publish()

    c.disconnect()
    c.loop_stop()


def main():
    p = argparse.ArgumentParser(description="Multi-topic MQTT CSV publisher")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--topics", required=True, help="Comma-separated list of topics")
    p.add_argument("--clients-per-topic", type=int, required=True)
    p.add_argument("--rows-per-client", type=int, required=True)
    p.add_argument("--batch-size", type=int, default=100, help="Rows per MQTT message")
    args = p.parse_args()

    topics = [t.strip() for t in args.topics.split(",")]
    total_clients = len(topics) * args.clients_per_topic
    total_rows = total_clients * args.rows_per_client

    print(f"Publishing {total_rows} rows across {len(topics)} topics "
          f"({args.clients_per_topic} clients/topic, "
          f"{args.rows_per_client} rows/client, batch {args.batch_size})",
          file=sys.stderr)

    threads = []
    global_client_idx = 0
    for topic in topics:
        for i in range(args.clients_per_topic):
            cid = f"pub-{topic.replace('/', '-')}-{i}"
            start = 1 + global_client_idx * args.rows_per_client
            t = threading.Thread(
                target=publish_client,
                args=(cid, args.host, args.port, topic,
                      start, args.rows_per_client, args.batch_size),
            )
            threads.append(t)
            global_client_idx += 1

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    print(f"All {total_clients} clients finished across {len(topics)} topics",
          file=sys.stderr)


if __name__ == "__main__":
    main()
