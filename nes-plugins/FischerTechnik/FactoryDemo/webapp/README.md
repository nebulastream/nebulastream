# HBW Slot Monitor

Live view of the fischertechnik HBW's 9 storage slots (A1..C3), shown side
by side for two independent pipelines that both run vision inference on
the same SSC camera feed:

- **NebulaStream** — `../hbw-state.sql`, running inside the NES engine.
- **Python baseline** — `python-baseline/infer.py`, a plain MQTT + ONNX
  Runtime script with no NES involvement, added to compare NES's
  end-to-end latency against a from-scratch implementation of the same
  model and threshold.

Green bucket = slot occupied, red/dashed = slot reads empty. Each panel
has its own "offset" readout (local receive time minus the frame's own
timestamp), so the two pipelines' latency can be compared directly.

This app itself has nothing to do with NebulaStream — it's a static HTML
page that subscribes to a local MQTT broker. NES and the Python baseline
are just two independent publishers into that broker, on separate topics.

```
Factory broker (192.168.0.10, topic i/cam)
   |                                    |
   v                                    v
NES query (hbw-state.sql)      python-baseline/infer.py
   |                                    |
   -> nes/hbw/slot_state         -> py/hbw/slot_state
   |                                    |
   +-----------> local mosquitto broker (this folder), port 1883 <---+
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
camera feed, runs the per-slot bucket-presence model, and publishes one
JSON snapshot per frame to `localhost:1883` on topic `nes/hbw/slot_state`
— unlike `ssc-slot-anomaly-gated-batched.yaml`, this query does not gate
on pan/tilt alignment (see hbw-state.yaml's header comment for why), so it
publishes for every frame regardless of where the shared camera is pointed.

## 3. Run the Python baseline (optional)

Either let `start.sh` manage it:

```
./start.sh --python-baseline   # or -p
```

which creates `python-baseline/.venv` on first run, installs
`requirements.txt` into it, and runs `infer.py` in the background (logs at
`python-baseline/infer.log`); `./stop.sh` stops it along with the broker.

Or run it yourself — see `python-baseline/README.md`:

```
cd python-baseline
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python infer.py
```

This is entirely independent of the NES query in step 2 — both subscribe
to the same factory camera topic directly and run the same model, so you
can run either one alone, or both together to compare them live. It's kept
optional/opt-in because it needs its own Python env and direct reachability
to the real factory broker (`192.168.0.10`), which not every machine running
this webapp can necessarily reach.

The app itself needs no server — it's a static HTML page. It'll show
"Connecting..." then "Connected" once it reaches the broker from step 1,
and each panel fills in as soon as its pipeline (step 2 and/or step 3)
starts publishing frames.

## Notes

- **A panel stuck on "no data yet"**: that panel's pipeline isn't running
  (query or `infer.py`), or the SSC camera (shared, joystick-controlled)
  isn't pointed at the HBW rack — neither pipeline gates on alignment, so
  off-target frames still publish, just with meaningless slot readings.
- **A panel greys out ("stale") after ~6s with no update**: that panel's
  pipeline stopped, or the camera feed itself stalled.
- To stop the broker: `./stop.sh` in this folder.
- The broker and the app are entirely independent of NES — NES and
  `infer.py` are just two independent MQTT clients publishing into the
  broker. Nothing here is spawned by, or tied to, the NES process.
