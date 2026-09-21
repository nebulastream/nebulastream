# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Runs testdata/model/iris.onnx via onnxruntime, one scalar UDF per output class."""

import os

import numpy as np
import onnxruntime as ort

_MODEL_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "testdata", "model", "iris.onnx")
_session = ort.InferenceSession(_MODEL_PATH)
_INPUT_NAME = _session.get_inputs()[0].name


def _predict(p1, p2, p3, p4):
    x = np.array([[p1, p2, p3, p4]], dtype=np.float32)
    return _session.run(None, {_INPUT_NAME: x})[0][0]


def setosa(p1, p2, p3, p4):
    return float(_predict(p1, p2, p3, p4)[0])


def versicolor(p1, p2, p3, p4):
    return float(_predict(p1, p2, p3, p4)[1])


def virginica(p1, p2, p3, p4):
    return float(_predict(p1, p2, p3, p4)[2])
