# Python baseline

A plain-Python, NES-independent pipeline for the same HBW slot-state task:
subscribe to the factory's SSC camera feed, run the same 9-slot ONNX model
NES's `hbw-state.sql` uses, and publish the same JSON snapshot shape to the
local demo broker, on a separate topic, so `../index.html` can show it next
to the NES pipeline for a live latency comparison.

```
Factory broker (192.168.0.10, topic i/cam)
   -> infer.py (this script)
      -> decode JPEG, crop+resize the 9 fixed slot ROIs, run the ONNX model
         -> publishes JSON to localhost:1883, topic py/hbw/slot_state
            -> local mosquitto broker (../docker-compose.yml)
               -> ../index.html, "Python baseline" panel
```

## Setup

```
cd nes-plugins/FischerTechnik/FactoryDemo/webapp/python-baseline
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## Run

Requires the local demo broker from `../docker-compose.yml` to already be
running (`../start.sh`), and a machine that can reach the real factory
broker at `192.168.0.10` (see the FischerTechnik connection docs).

```
python infer.py
```

Runs until killed. Each camera frame is processed independently (no
batching, no alignment gating), same as `hbw-state.sql`.

## Parity with the NES pipeline

This script is meant to be a faithful, independent reimplementation of
`hbw-state.sql`'s inference path, not a from-scratch model:

- Same 9 slot ROI pixel rects, same order (A1..C3), as
  `FtPreprocessAllSlotsPhysicalFunction.cpp`.
- Same resize target (64x64) and normalization (`/255.0`, CHW).
- Same `bucket_slot_cnn_9slot.onnx` file, same `p_absent > 0.85` threshold.
- Same output JSON field names (`ts`, `p_absent_<slot>`, `empty_<slot>`,
  `num_empty`), so the dashboard can reuse one rendering path for both
  panels.

One known, unavoidable source of small numerical divergence: NES decodes
JPEG with `stb_image`, this script decodes with Pillow/libjpeg -- different
JPEG decoders can round IDCT/chroma-upsampling slightly differently. This
should not flip any `empty_*` classification in practice, but the two
`p_absent_*` scores are not guaranteed to be bit-identical.
