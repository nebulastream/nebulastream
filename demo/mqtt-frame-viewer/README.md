# MQTT media viewer

Start the local broker with `docker compose up -d`, then serve this folder with any static web server and open `index.html`. Its default WebSocket URL, `ws://localhost:9001/mqtt`, connects to that broker. MQTT is also exposed on `localhost:1883` for publishers.

Topics are added to all three dropdowns as messages matching the discovery filter arrive. The two video panels and the audio waveform can each select a different topic.

Video topics use the existing JSON frame format:

```json
{"timestamp": 1720000000000, "image": "/9j/4AAQSkZJRgABAQ..."}
```

The audio topic uses CSV: one floating-point sample per line, or a comma-separated batch. Samples are expected in the normalized `[-1, 1]` range and the most recent 4,096 samples are shown.

```text
0.0
0.12
-0.08
```

The page loads the Eclipse Paho browser client from unpkg, so it needs internet access unless that script is hosted locally.

The local broker allows anonymous connections and is intended only for a development machine. Add authentication and TLS before exposing it beyond localhost.
