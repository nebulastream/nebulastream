# MQTT media viewer

Run `docker compose up -d`, then open <http://localhost:8081>. nginx serves the viewer, which connects directly from the browser to the broker's MQTT-over-WebSocket listener on port 9001. The viewer derives the broker hostname from the page URL, so it also works when opened using the Docker host's network name or IP address. MQTT is exposed on port 1883 for publishers.

Browsers cannot connect to the raw MQTT listener on port 1883; they require the WebSocket listener exposed on port 9001. The broker URL remains editable in the viewer.

The viewer subscribes directly to `rgb-image`, `thermal-image`, `audio`, `audio-noise-level`, `audio-kws`, and `memory`. RGB, thermal, and the single audio waveform share the first row.

Video topics use the existing JSON frame format:

```json
{"timestamp": 1720000000000, "image": "/9j/4AAQSkZJRgABAQ..."}
```

The audio topic uses JSON with millisecond timestamps and a base64-encoded, little-endian IEEE-754 float64 array. Samples are expected in the normalized `[-1, 1]` range. For a chunk with multiple samples, the first and last samples map to `timestamp_start` and `timestamp_end`. The viewer renders only the latest second of signal and shows the number of valid chunks received since connecting. Overlapping chunks align instead of being appended twice.

```json
{
  "timestamp_start": 1720000000000,
  "timestamp_end": 1720000000100,
  "chunk": "AAAAAAAA8D8AAAAAAAAAAA=="
}
```

The `memory` topic publishes the process metrics from `/proc/$pid/smaps_rollup` with lowercase identifiers. The viewer plots RSS and PSS values, which are expressed in KiB by the source, over the last five seconds:

```json
{"rss_kb": 123456, "pss_kb": 120000}
```

The page loads the Eclipse Paho browser client from unpkg, so it needs internet access unless that script is hosted locally.

The local broker allows anonymous connections and is intended only for a development machine. Add authentication and TLS before exposing it beyond localhost.
