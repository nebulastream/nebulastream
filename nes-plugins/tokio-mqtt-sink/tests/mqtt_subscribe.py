#!/usr/bin/env python3
"""Subscribe to an MQTT topic and write received messages to a file.

Usage:
    mqtt_subscribe.py --port PORT --topic TOPIC --output FILE
                      [--timeout SECONDS] [--host HOST]

Retries connection until the broker is available, so it can be started
before the broker is up. Once connected and subscribed, writes every
received message payload to the output file (appending). Exits after
--timeout seconds of wall-clock time.
"""

import argparse
import sys
import time
import threading

import paho.mqtt.client as mqtt


def main():
    p = argparse.ArgumentParser(description="MQTT subscriber with connection retry")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--topic", required=True)
    p.add_argument("--output", required=True, help="Output file path")
    p.add_argument("--timeout", type=int, default=30, help="Wall-clock timeout in seconds")
    args = p.parse_args()

    deadline = time.monotonic() + args.timeout
    output_file = open(args.output, "wb")
    msg_count = 0
    lock = threading.Lock()

    def on_connect(client, userdata, flags, rc, properties=None):
        if rc == 0:
            client.subscribe(args.topic, qos=0)
            print(f"Connected and subscribed to {args.topic}", file=sys.stderr)
        else:
            print(f"Connect failed with rc={rc}", file=sys.stderr)

    def on_message(client, userdata, msg):
        nonlocal msg_count
        with lock:
            output_file.write(msg.payload)
            msg_count += 1

    c = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"py-sub-{int(time.time())}",
        protocol=mqtt.MQTTv311,
    )
    c.on_connect = on_connect
    c.on_message = on_message

    # Retry connection until broker is up or timeout.
    connected = False
    while time.monotonic() < deadline:
        try:
            c.connect(args.host, args.port, keepalive=60)
            connected = True
            break
        except (ConnectionRefusedError, OSError):
            time.sleep(0.1)

    if not connected:
        print(f"ERROR: could not connect to {args.host}:{args.port} "
              f"within {args.timeout}s", file=sys.stderr)
        sys.exit(1)

    c.loop_start()

    # Wait until deadline.
    remaining = deadline - time.monotonic()
    if remaining > 0:
        time.sleep(remaining)

    c.loop_stop()
    c.disconnect()
    output_file.close()

    with lock:
        print(f"Received {msg_count} messages", file=sys.stderr)


if __name__ == "__main__":
    main()
