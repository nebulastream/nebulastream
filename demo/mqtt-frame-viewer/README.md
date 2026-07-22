# MQTT media viewer

Start the local broker with `docker compose up -d`, then serve this folder with any static web server and open `index.html`. Its default WebSocket URL, `ws://localhost:9001/mqtt`, connects to that broker. MQTT is also exposed on `localhost:1883` for publishers.

The viewer subscribes directly to `rgb-image`, `thermal-image`, `audio`, `audio-noise-level`, and `audio-kws`. RGB, thermal, and the single audio waveform share the first row.

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

The page loads the Eclipse Paho browser client from unpkg, so it needs internet access unless that script is hosted locally.

The local broker allows anonymous connections and is intended only for a development machine. Add authentication and TLS before exposing it beyond localhost.
