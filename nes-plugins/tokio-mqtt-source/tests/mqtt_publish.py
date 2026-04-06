#!/usr/bin/env python3
"""Publish CSV rows to an MQTT broker from one or more concurrent clients.

Usage:
    mqtt_publish.py --host HOST --port PORT --topic TOPIC
                    --clients N --rows-per-client M
                    [--batch-size B] [--client-prefix PREFIX]

Each client connects independently and publishes `rows-per-client` rows in
batches of `batch-size` rows per MQTT message. All clients run concurrently
in separate threads. The script exits once every client has published and
disconnected.

Row format: <global_row_id>,<global_row_id * 10>,<global_row_id * 1000>
Row IDs are globally unique across all clients (client 0 gets 1..M, client 1
gets M+1..2M, etc.).
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

    # Wait for all queued messages to be written to the socket.
    for msg_info in inflight:
        msg_info.wait_for_publish()

    c.disconnect()
    c.loop_stop()


def main():
    p = argparse.ArgumentParser(description="Multi-client MQTT CSV publisher")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--topic", required=True)
    p.add_argument("--clients", type=int, required=True, help="Number of concurrent clients")
    p.add_argument("--rows-per-client", type=int, required=True)
    p.add_argument("--batch-size", type=int, default=100, help="Rows per MQTT message")
    p.add_argument("--client-prefix", default="pub")
    args = p.parse_args()

    total = args.clients * args.rows_per_client
    print(f"Publishing {total} rows from {args.clients} clients "
          f"({args.rows_per_client} rows each, batch size {args.batch_size})",
          file=sys.stderr)

    threads = []
    for i in range(args.clients):
        cid = f"{args.client_prefix}-{i}"
        start = 1 + i * args.rows_per_client
        t = threading.Thread(
            target=publish_client,
            args=(cid, args.host, args.port, args.topic,
                  start, args.rows_per_client, args.batch_size),
        )
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    print(f"All {args.clients} clients finished", file=sys.stderr)


if __name__ == "__main__":
    main()
