# HBW Slot Monitor

Live view of the fischertechnik HBW's 9 storage slots (A1..C3), fed by a
NebulaStream query that runs vision inference on the SSC camera. Green
bucket = slot occupied, red/dashed = slot reads empty.

This app itself has nothing to do with NebulaStream — it's a static HTML
page that subscribes to a local MQTT broker. NES is just what publishes
into that broker.

```
NES query (in the nebulastream repo)
   -> publishes JSON snapshots
      -> local mosquitto broker (this folder), port 1883
         -> same broker, websocket listener, port 9001
            -> index.html (open directly in a browser)
```

## 1. Start the broker + the app

Requires Docker.

```
cd nes-plugins/FischerTechnik/FactoryDemo/webapp
./start.sh
```

This starts the local broker (a throwaway one — not the factory's own
broker, not exposed outside your machine) and opens `index.html` in your
default browser. Leave the broker running; stop it later with `./stop.sh`.

## 2. Run the NES query

From the `nebulastream` repo root, with the project already built:

```
./scripts/nes-cli-compose.sh \
  -b ./cmake-build-debug-docker/nes-frontend/apps/nes-cli \
  -t nes-plugins/FischerTechnik/FactoryDemo/hbw-state.yaml \
  -q nes-plugins/FischerTechnik/FactoryDemo/hbw-state.sql
```

(Adjust the `-b` path if your build directory is named differently.)

This subscribes to the real factory broker (`192.168.0.10`) for the SSC
camera and pan/tilt feeds, runs the per-slot bucket-presence model, and
publishes one JSON snapshot per aligned frame to `localhost:1883` on
topic `nes/hbw/slot_state`.

The app itself needs no server — it's a static HTML page. It'll show
"Connecting..." then "Connected" once it reaches the broker from step 1,
and the grid fills in as soon as the query in step 2 starts publishing
frames.

## Notes

- **No data / grid stuck on "no data yet"**: the query only publishes
  when the SSC camera's pan/tilt matches the trained framing (it's a
  shared, joystick-controlled camera). Point it at the HBW rack and
  confirm alignment (this is the same gate `ssc-slot-anomaly-gated-batched.yaml`
  uses).
- **Grid greys out ("stale") after ~6s with no update**: same cause —
  the camera drifted off the HBW, or the query/broker isn't running.
- To stop the broker: `./stop.sh` in this folder.
- The broker and the app are entirely independent of NES — NES is just an
  MQTT client that publishes into the broker, same as any other MQTT
  publisher would. Nothing here is spawned by, or tied to, the NES process.
