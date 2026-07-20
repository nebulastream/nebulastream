# NES Coffee Machine Demo — Integration Test

Practical reference for bringing up the full demo: real coffee machine → NES →
MQTT → dashboard.

## Architecture

```
[ Raspberry Pi :2222 ]              real Eversys telemetry, 43-field CSV
        │  ssh tunnel
        ▼
[ localhost:2223 ]                  tunnel endpoint
        │
        ▼
[ pi-relay.py :2222 ]               fans one upstream conn out to N clients
        │                           (or: coffe-server-pi-schema.py on :2222)
        ▼
[ nes-dev container ]               worker dials host.docker.internal:2222
        │                           MQTT sink → host.docker.internal:1883
        ▼
[ mosquitto ]                       1883 native · 9001 websockets
        │
        ▼
[ dashboard :5173 ]                 ws://localhost:9001/mqtt
```

### Ports

| Port | What | Who connects |
|------|------|--------------|
| 2222 | machine telemetry (relay, **or** dummy server) | the NES worker dials **out** to it |
| 2223 | local end of the ssh tunnel to the Pi | the relay reads from it |
| 1883 | mosquitto native MQTT | NES MQTT sinks publish here |
| 9001 | mosquitto **websockets** | the browser — the only port the UI cares about |
| 8080 | NES worker gRPC | `nes-cli` |
| 5173 | vite dev server | you |

Three things that cause most of the confusion:

- **The NES TCP source is a client.** The worker dials *out* to
  `socket_host:socket_port`. It does not listen.
- **From inside the container, the Mac is `host.docker.internal`** — not
  `127.0.0.1`. This applies to both the telemetry port and mosquitto.
- **1883 vs 9001 are not interchangeable.** Sinks use native MQTT (1883); the
  browser can only speak websockets (9001). `DEFAULT_MQTT_PORT` in
  `src/AppEnvProvider.jsx` must match the `listener` line in `mosquitto.conf`.

### Repo layout

Commands below assume this layout and are written relative to `work/`:

```
work/
├── nebulastream/                    queries, relay, dummy server, tooling
└── coffe-ui/
    ├── nebuli-ui/                   MQTT/React library (built + linked)
    └── nes-coffee-dashboard/        the dashboard
```

## Bring-up

Each step in its own terminal, in this order.

### 1. Telemetry source

**Real Pi** — tunnel to **2223**, because the relay needs 2222:

```bash
ssh -N -o ServerAliveInterval=30 -o ServerAliveCountMax=3 \
  -L 2223:raspi-coffee.bifold.tu-berlin.de:2222 tim@needmi-jh.dima.tu-berlin.de
```

```bash
cd nebulastream
python3 scripts/pi-relay.py --upstream-port 2223 --listen-port 2222 -v
```

The relay is not optional with the real Pi. The Pi serves telemetry as
`tail -f | nc`, which has **one reader** and drops the connection every
~11–130 s. NES treats a closed source as End-of-Stream and **stops the query
permanently — it does not reconnect.** The relay holds the single upstream
connection, reconnects with backoff, and keeps downstream sockets open, so
several queries can run at once and survive Pi dropouts.

**Dummy machine** — no tunnel, no relay, same port:

```bash
cd nebulastream
python3 scripts/coffe-server-pi-schema.py --interval 8 --port 2222
```

Calibrated to 24 days of real data (~30 drinks/weekday, 06:00–17:00). Add
`--speedup 60` to compress a working day into minutes, `--no-faults` to stop it
injecting dropout rows and counter resets.

### 2. mosquitto

```bash
cd coffe-ui/nes-coffee-dashboard
mosquitto -c mosquitto.conf -v
```

### 3. NES worker and queries

First time only — set up the dev container and build NES. See
`docs/development/development.md`; the entry point is:

```bash
cd nebulastream
./scripts/install-local-docker-environment.sh
```

Everything below assumes a completed build in `cmake-build-debug-docker/`.

```bash
docker start nes-dev
docker exec -it nes-dev bash
./cmake-build-debug-docker/nes-single-node-worker/nes-single-node-worker
```

Register the queries (new terminal, also inside the container):

```bash
docker exec -it nes-dev bash
D=scripts/coffe_queries/tcp/testv2/dashboard
for q in water-usage bean-consumption; do
  ./cmake-build-debug-docker/nes-frontend/apps/nes-cli -t $D/$q.yaml start
done
```

### 4. Dashboard

```bash
cd coffe-ui/nes-coffee-dashboard
nvm use && npm run dev            # → http://localhost:5173
```

`nebuli-ui` must be built and linked first — see that repo's README.

## Verifying

**Is the machine actually publishing?** Row age near 0, new row every ~8 s:

```bash
nc 127.0.0.1 2222 | while read -r l; do \
  printf '%s  row age=%ss\n' "$(date +%T)" "$(( $(date +%s) - ${l%%,*} ))"; done
```

The relay replays the last row to each new client, so **one row is not proof of
life** — you need a second one, or the age check above.

**What is the machine doing?** Prints only counters that changed, and flags
dropout rows, counter resets, and any supposedly-dead field that moves:

```bash
cd nebulastream
python3 -u scripts/pi-watch-counters.py --port 2222
```

This bypasses NES and MQTT deliberately — if the dashboard disagrees with the
machine, it tells you which half is wrong.

**Is anything reaching MQTT?**

```bash
mosquitto_sub -h 127.0.0.1 -p 1883 -t '#' -v
```

**Can the container reach the host?**

```bash
docker exec nes-dev bash -lc 'timeout 3 bash -c "echo > /dev/tcp/host.docker.internal/2222" && echo TCP-OK  || echo TCP-FAIL'
docker exec nes-dev bash -lc 'timeout 3 bash -c "echo > /dev/tcp/host.docker.internal/1883" && echo MQTT-OK || echo MQTT-FAIL'
```

## Status

| Tile | Query | State |
|---|---|---|
| Water Usage | `water-usage.yaml` | working, confirmed on the real machine |
| Bean Consumption | `bean-consumption.yaml` | working, confirmed on the real machine |
| Hopper Quantity | `hopper-quantity.yaml` | query duplicates bean-consumption; UI renders mock |
| Today vs Yesterday | `products-today-yesterday.yaml` | receives data, discards it; renders hardcoded values |
| Cleaning vs Use | `cleaning-vs-use.yaml` | UI subscribes to the wrong topic; renders mock |
| Top Drinks | `top-drinks.yaml` | shape mismatch; renders mock |

## What the real data says

From a 24-day capture (4839 rows, 723 products) — details in
`scripts/coffe-server-pi-schema.py` and the dashboard's chart docs.

- **Two exact identities**, which the tiles depend on:
  `bean_total = left_rear + right_front + beans_3 + grinder_adjustment`, and
  `water_total = products + rinse + cleaning + a constant offset`. The offset
  does not accumulate — **there is no purge counter** on this machine.
- **18 of 43 fields are permanently zero.** Single-brewer, single-milk-system,
  no powder chute, no steam wand. They stay on the wire (the Pi's format is
  fixed) but must not be charted.
- **The machine is slow**: 5–80 drinks/day, and 89% of rows contain no product.
  Telemetry is on a timer, not per-drink.
- **The feed is not clean**: 35 all-zero dropout rows and 14 counter resets.
  Every consumer has to guard against both.

## Problems / next steps

**1. Decide per query what belongs in NES and what in JS.**

Today all six queries are passthroughs and the dashboard derives any "since X"
figure in the browser. That works, but it means the interesting computation is
in React, not in the engine — and two screens opened minutes apart show
different numbers from the same stream.

What is actually available:

- Time-based `TUMBLING`/`SLIDING` windows work. `MAX(x) - MIN(x)` over a window
  is a genuine engine-side delta.
- The Pi's timestamp is epoch **seconds** while NES reads window timestamps as
  **milliseconds**, so `SIZE 10 SEC` silently means ~2.8 h. Fix by multiplying
  in a subquery (`ts_ms * UINT64(1000)`), not by avoiding windows.
- **Count-based windows and `THRESHOLD` are grammar-only** — they appear in
  `AntlrSQL.g4` and nowhere else in the codebase. Do not build on them.
- **No `LAG`/`LEAD`/`OVER`.** You cannot compare a row to its predecessor, so
  per-event classification (single vs double shot) is not expressible in SQL.

The hard part is not windowing itself:

- **Counter resets poison `MAX - MIN`.** A window spanning one reports the whole
  pre-reset lifetime as if consumed in that window. Dropouts are filterable
  (`WHERE counter > 0`); resets are not, because the post-reset values are real.
- **A correct plot can still be a boring plot.** A windowed rate is honestly
  flat and zero for most of the day. Window sizes need to be ~20 min+ to
  usually contain a drink, and live tiles should be paired with dense static
  ones.

**2. Hopper Quantity vs Bean Consumption.** The two queries are currently
byte-identical. Proposal: Bean Consumption becomes the all-time leaderboard
("what does the office actually drink"), Hopper Quantity becomes the live view —
ideally a NES-windowed rate, which would also remove the browser-side baseline.

**3. Confirm `ML_PER_TICK`.** Provisional at 1.0 ml/tick. Dispense a known
volume of hot water and diff `total_water_quantity_ticks`; nothing else may run
during the measurement. It scales every number on the water tile.

**4. Migrate the remaining UI charts** onto real data.
