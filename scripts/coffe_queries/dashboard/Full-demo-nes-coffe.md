# NES Coffee Machine Demo — Integration Test

Practical reference for bringing up the full demo: real coffee machine → NES →
MQTT → dashboard.

## Architecture

```
[ Raspberry Pi telemetry ]          real Eversys telemetry, 43-field CSV
        │  ssh tunnel → localhost:2223
        ▼
[ pi-relay.py :2222 ]  (host)       fans one upstream conn out to N clients
        │                           (or: coffe-server-pi-schema.py on :2222)
        ▼   host.docker.internal:2222   ← the ONLY connection out of the stack
┌─ docker compose network `nes` ─────────────────────────────┐
│ [ nes-worker ]       dials the telemetry above; gRPC :8080  │
│       │              MQTT sink → mosquitto:1883             │
│       ▼                                                     │
│ [ mosquitto ]        1883 native · 9001 websockets          │
│ [ nes-cli ]          one-shot: registers the queries        │
└─────────────────┬──────────── 1883 + 9001 published ───────┘
                  ▼
[ dashboard :5173 ]                 Firefox → ws://<pi>:9001/mqtt
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
- **Only the telemetry source leaves the compose stack.** The worker dials it at
  `host.docker.internal:2222` (the Pi host, as seen from inside the container).
  mosquitto now runs *inside* the stack, so sinks publish to `mosquitto:1883`
  over the docker network — the query YAMLs no longer use `host.docker.internal`
  for MQTT.
- **1883 vs 9001 are not interchangeable.** Sinks use native MQTT (1883); the
  browser can only speak websockets (9001). Both are published to the host so
  Firefox can reach 9001. `DEFAULT_MQTT_PORT` in `src/AppEnvProvider.jsx` must
  match the `listener` line in `mosquitto.conf`.

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

### 2. NES stack (worker + mosquitto + queries) — docker compose

Everything except the telemetry source and the dashboard runs from one file:
`scripts/coffe_queries/dashboard/docker-compose.yml`. It starts three containers
on a private `nes` network — `mosquitto` (broker), `nes-worker` (engine), and
`nes-cli` (one-shot query registration) — and publishes the broker's 1883/9001
to the host.

Get the two NES images. Either take the nightly builds:

```bash
scripts/coffe_queries/dashboard/update-demo-images.sh --force
```

or build them yourself from the repo root, which is what you want while developing:

```bash
docker build -f docker/single-node-worker/SingleNodeWorker.dockerfile -t nes-worker .
docker build -f docker/frontend/cli.dockerfile                        -t nes-cli    .
```

Bring it up:

```bash
docker compose -f scripts/coffe_queries/dashboard/docker-compose.yml up
```

`nes-cli` waits — via healthchecks — until both the worker's gRPC (:8080) and
mosquitto (:1883) are ready, then registers `water-usage`, `bean-consumption`
and `machine-activity`. Change that set by editing the `for q in …` loop in the
compose file.

Step 1 must already be running: the worker's only connection out of the stack is
the telemetry source at `host.docker.internal:2222`, kept reachable by
`extra_hosts: host-gateway`. The sinks publish to `mosquitto:1883` inside the
network (the query YAMLs were repointed there from `host.docker.internal:1883`).

Register one more query later without restarting the stack:

```bash
docker compose -f scripts/coffe_queries/dashboard/docker-compose.yml \
  run --rm --entrypoint nes-cli nes-cli -t /queries/<name>.yaml start
```

### 3. Dashboard

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

**Can the worker reach telemetry (host) and the broker (network)?**

```bash
D=scripts/coffe_queries/dashboard
docker compose -f $D/docker-compose.yml exec nes-worker bash -lc 'timeout 3 bash -c "echo > /dev/tcp/host.docker.internal/2222" && echo TCP-OK  || echo TCP-FAIL'
docker compose -f $D/docker-compose.yml exec nes-worker bash -lc 'timeout 3 bash -c "echo > /dev/tcp/mosquitto/1883"            && echo MQTT-OK || echo MQTT-FAIL'
```

## Staying on the nightly build

`update-demo-images.sh` pulls the images CI publishes every night
(`nebulastream/worker:latest`, `nebulastream/nes-cli:latest` — note the worker is
published as `worker`, not `nes-worker`), retags them to the local names the
compose file uses, restarts the stack and checks that the demo still works:
`nes-cli` must exit 0, all three queries must report `Running`, and each topic
must publish two messages — one is not enough, since the relay replays its last
row to every new subscriber.

If any of that fails it puts the previous images back and restarts again, so a
broken nightly never leaves the demo dead. The `:rollback` tags hold the last
images that *passed* this check, not merely the previously deployed ones.

```bash
scripts/coffe_queries/dashboard/update-demo-images.sh            # nightly use
scripts/coffe_queries/dashboard/update-demo-images.sh --dry-run  # show, change nothing
scripts/coffe_queries/dashboard/update-demo-images.sh --force    # even if unchanged
```

From cron on the Pi (the script needs no TTY and takes a lock, so overlapping
runs are impossible):

```cron
0 5 * * * /home/pi/nebulastream-public/scripts/coffe_queries/dashboard/update-demo-images.sh >> /home/pi/nes-demo-update.log 2>&1
```

`tail -3` of that log answers "did last night work?" — the final line is
`updated`, `already up to date`, or `ROLLED BACK: <reason>`. The Pi user must be
in the `docker` group, since cron cannot answer a `sudo` prompt.

Two caveats worth knowing. The data check needs the telemetry relay to be up; if
`:2222` is unreachable the script says so and decides on the query states alone,
rather than rolling back because the coffee machine is unplugged. And a query
whose source dies stays dead — NES treats a closed source as end-of-stream — so a
rollback restart is also how you recover from that.

## Status

| Tile | Query | State |
|---|---|---|
| Water Usage | `water-usage.yaml` | working, confirmed on the real machine |
| Bean Consumption | `bean-consumption.yaml` | working, confirmed on the real machine |
| Hopper Quantity | `hopper-quantity.yaml` | query duplicates bean-consumption; UI renders mock |
| Today vs Yesterday | `products-today-yesterday.yaml` | receives data, discards it; renders hardcoded values |
| Cleaning vs Use | `cleaning-vs-use.yaml` | UI subscribes to the wrong topic; renders mock |
| Top Drinks | `top-drinks.yaml` | shape mismatch; renders mock |

## Which queries feed which dashboard version

All queries now live flat in `scripts/coffe_queries/dashboard/` (the old
`tcp/testv2/` nesting was removed; superseded/experimental queries were moved out
to `work/coffe-queries-backup/`). The two dashboard branches use different subsets
of the queries kept here — every query below stays in this folder so either branch
can be brought up without hunting for files.

**`live-dev` branch — the trimmed demo shown next week (3 tiles):**

| Tile | Query | Topic |
|---|---|---|
| Brew Analysis (3 live plots: beans/water/milk) | `machine-activity.yaml` | `machine-activity` |
| Water Usage (doughnut) | `water-usage.yaml` | `water-usage` |
| Bean Consumption (grams leaderboard) | `bean-consumption.yaml` | `bean-consumption` |

**The other (full-UI) branch — everything else, to be added after the demo:**

| Tile | Query | Topic |
|---|---|---|
| Bean Usage — All Time | `bean-usage-total.yaml` | `bean-usage-total` |
| Hopper Consumption Rate | `hopper-rate.yaml` | `hopper-rate` |
| Top Drinks (doughnut) | `top-drinks.yaml` | `top-drinks` |
| Top Drinks (radar) | `top-drinks-radar.yaml` | `top-drinks-radar` |
| Today vs Yesterday | `products-today-yesterday.yaml` | `products-today-yesterday` |
| Shots vs Cleaning | `cleaning-vs-use.yaml` | `cleaning-vs-use` |

`hopper-quantity.yaml` is the earlier passthrough that `hopper-rate.yaml`
superseded — kept for reference, not wired to a live tile.

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
