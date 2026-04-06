# TokioMqtt Source — Known Limitations

## Data Path

- **Payloads must be newline-terminated CSV** — the NES CSV InputFormatter requires `\n` as the tuple delimiter. Raw binary or JSON payloads will not work without changing the parser config. This is expected behavior given how the NES parser works.

## Singleton Broker

- **No graceful shutdown** — `broker.start()` blocks on thread joins forever. The broker thread is detached and keeps running until process exit.
- **Port/broker settings not configurable per-source** — the broker always starts on the default port (1883) with hardcoded settings. Port and broker tuning should become worker-level configuration in the future, not per-source config.

## Shutdown

- **Source responds to the cancellation token** but the REPL's `WAIT_FOR_QUERY_TERMINATION` loop never checks the SIGTERM stop token — this is a REPL bug (`ReplStarter.cpp:316`), not a source bug.
- **No clean broker stop API in rumqttd 0.19** — we cannot tell the TCP listener to stop accepting connections.

## Operational

- **Port conflicts fail silently** — if the configured port is already in use, `broker.start()` fails on a background thread and the source hangs on `link_rx.next()` forever.
- **No health check** — no way to tell from NES whether the broker is actually accepting connections.
- **No metrics** — rumqttd supports meters and alerts but they are not exposed.
- **Hardcoded broker settings** — `connection_timeout_ms` (60 000), `max_inflight_count` (100), `max_segment_size` (100 KB), listen port (1883) are not configurable. These should become worker-level configuration in the future.

## Testing

- **Embedded-only tests** — no distributed Docker Compose coverage. The REPL returns before the worker deploys the source, making distributed tests unreliable without protocol changes.
- **Tests use `kill -9`** because SIGTERM does not terminate the REPL cleanly (see Shutdown above).

## rumqttd Patches

We carry a local patched copy of rumqttd 0.19 in `third-party/rumqttd/` with two additions:

- **`BrokerHandle`** — a cloneable handle obtained via `Broker::handle()` before `start()`. Allows creating new internal links dynamically after the broker thread is running.
- **`LinkTx::unsubscribe()`** — mirrors `subscribe()` but sends `Packet::Unsubscribe`. The router already supported unsubscribe for TCP clients; this just exposes it on the internal link API.

These should be upstreamed or removed if a newer rumqttd version provides equivalent functionality.

---

## Not Relevant Yet

- **MQTT v4 only** — rumqttd 0.19 does not serve v5 on the same port. v4 is sufficient for current use cases.
- **No TLS** — the broker listens on plain TCP only. rumqttd supports `TlsConfig` (Rustls or NativeTls) but cert/key path plumbing is not wired up.
- **No authentication** — anyone who can reach the port can publish. rumqttd supports static user/pass maps and custom `AuthHandler` callbacks, just not exposed.
- **QoS 0 only on the internal link** — the internal `LinkTx::subscribe()` API does not accept a QoS parameter; the router delivers to internal links as fire-and-forget (`AtMostOnce`). Messages can theoretically be lost under extreme load, but this is a rumqttd internal-link API limitation, not something we can change without patching rumqttd.
