#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Independent (non-NES) baseline for the HBW slot-state demo.

Subscribes directly to the factory's own MQTT broker for raw SSC camera
frames, runs the exact same ONNX model NES's hbw-state.sql uses, and
publishes the same JSON snapshot shape to the local demo broker used by
webapp/index.html -- so the dashboard can show this plain-Python pipeline
side by side with the NES one and compare end-to-end latency.

This process has nothing to do with NebulaStream: it is a normal MQTT
subscriber + ONNX inference loop, independent of the NES query.
"""

import argparse
import base64
import datetime as dt
import io
import json
import os

import numpy as np
import onnxruntime as ort
import paho.mqtt.client as mqtt
from PIL import Image

FACTORY_BROKER = "192.168.0.10"
FACTORY_PORT = 1883
FACTORY_TOPIC = "i/cam"

LOCAL_BROKER = "localhost"
LOCAL_PORT = 1883
OUTPUT_TOPIC = "py/hbw/slot_state"

DEFAULT_MODEL_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "models", "bucket_slot_cnn_9slot.onnx"
)
THRESHOLD = 0.85
MODEL_EXTENT = 64

# Same 9 fixed slot ROIs (x, y, w, h) as slotRois in
# nes-plugins/Functions/FtPreprocessAllSlots/FtPreprocessAllSlotsPhysicalFunction.cpp.
# Order A1..C3 is also the model's batch/output order -- keep all three in sync.
SLOT_ROIS = {
    "A1": (86, 10, 55, 51),
    "A2": (155, 20, 57, 50),
    "A3": (240, 30, 76, 55),
    "B1": (88, 63, 56, 40),
    "B2": (149, 73, 69, 52),
    "B3": (237, 88, 78, 62),
    "C1": (92, 107, 51, 51),
    "C2": (150, 128, 72, 50),
    "C3": (226, 153, 88, 53),
}
SLOTS = list(SLOT_ROIS)


def parse_ts_to_unix_ms(ts: str) -> int:
    # Mirrors NES's CASTTOUNIXTS: ISO-8601 string -> unix milliseconds.
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return int(dt.datetime.fromisoformat(ts).timestamp() * 1000)


def frame_to_tensor(data_uri: str) -> np.ndarray:
    """data:<type>;base64,<...> JPEG -> [1, 9, 3, 64, 64] float32, matching
    FT_PREPROCESS_ALL_SLOTS: crop each fixed ROI, resize to 64x64 (Pillow's
    bilinear/triangle resample, which the NES C++ resize was written to
    match), normalize to [0, 1], CHW per slot."""
    _, b64 = data_uri.split(",", 1)
    jpeg_bytes = base64.b64decode(b64)
    image = Image.open(io.BytesIO(jpeg_bytes)).convert("RGB")

    tensor = np.empty((1, len(SLOTS), 3, MODEL_EXTENT, MODEL_EXTENT), dtype=np.float32)
    for i, slot in enumerate(SLOTS):
        x, y, w, h = SLOT_ROIS[slot]
        crop = image.crop((x, y, x + w, y + h)).resize(
            (MODEL_EXTENT, MODEL_EXTENT), Image.Resampling.BILINEAR
        )
        arr = np.asarray(crop, dtype=np.float32) / 255.0  # HWC
        tensor[0, i] = arr.transpose(2, 0, 1)  # CHW
    return tensor


class HbwBaseline:
    def __init__(self, model_path: str):
        self.session = ort.InferenceSession(model_path, providers=["CPUExecutionProvider"])
        self.input_name = self.session.get_inputs()[0].name
        self.output_name = self.session.get_outputs()[0].name

        self.sink = mqtt.Client(client_id="hbw-python-baseline-sink")
        self.sink.connect(LOCAL_BROKER, LOCAL_PORT)
        self.sink.loop_start()

        self.source = mqtt.Client(client_id="hbw-python-baseline-cam")
        self.source.on_connect = self._on_connect
        self.source.on_message = self._on_message

    def _on_connect(self, client, userdata, flags, rc):
        print(f"[baseline] connected to factory broker ({FACTORY_BROKER}), rc={rc}")
        client.subscribe(FACTORY_TOPIC)

    def _on_message(self, client, userdata, msg):
        try:
            frame = json.loads(msg.payload.decode("utf-8"))
            ts_ms = parse_ts_to_unix_ms(frame["ts"])
            tensor = frame_to_tensor(frame["data"])
            p_absent = self.session.run([self.output_name], {self.input_name: tensor})[0]

            out = {"ts": ts_ms}
            num_empty = 0
            for slot, score in zip(SLOTS, p_absent):
                empty = int(score > THRESHOLD)
                out[f"p_absent_{slot}"] = float(score)
                out[f"empty_{slot}"] = empty
                num_empty += empty
            out["num_empty"] = num_empty

            self.sink.publish(OUTPUT_TOPIC, json.dumps(out), qos=0)
        except Exception as e:
            print(f"[baseline] error processing frame: {e}")

    def run(self):
        self.source.connect(FACTORY_BROKER, FACTORY_PORT)
        self.source.loop_forever()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", default=DEFAULT_MODEL_PATH, help="Path to bucket_slot_cnn_9slot.onnx")
    args = parser.parse_args()
    print(f"[baseline] loading model from {args.model}")
    HbwBaseline(args.model).run()


if __name__ == "__main__":
    main()
