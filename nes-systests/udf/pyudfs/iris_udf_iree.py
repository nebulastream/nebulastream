# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Runs testdata/model/iris.onnx via IREE (import -> compile -> run), one scalar UDF per class."""

import os
import tempfile

import iree.compiler as ic
import iree.runtime as rt
import numpy as np
from iree.compiler.tools.import_onnx import __main__ as _onnx_importer

_MODEL_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "testdata", "model", "iris.onnx")

with tempfile.TemporaryDirectory() as _tmp:
    _mlir_path = os.path.join(_tmp, "iris.mlir")
    _onnx_importer.main(_onnx_importer.parse_arguments([_MODEL_PATH, "-o", _mlir_path]))
    _vmfb = ic.compile_file(
        _mlir_path, input_type=ic.InputType.ONNX, target_backends=["llvm-cpu"], extra_args=["--iree-llvmcpu-target-cpu=generic"]
    )

_ctx = rt.SystemContext(config=rt.Config("local-sync"))
_vm_module = rt.VmModule.copy_buffer(_ctx.instance, _vmfb)
_ctx.add_vm_module(_vm_module)
_func_name = next(n for n in _vm_module.function_names if not n.startswith("__") and not n.endswith("$async"))
_infer = _ctx.modules.module[_func_name]


def _predict(p1, p2, p3, p4):
    x = np.array([[p1, p2, p3, p4]], dtype=np.float32)
    return np.asarray(_infer(x))[0]


def setosa(p1, p2, p3, p4):
    return float(_predict(p1, p2, p3, p4)[0])


def versicolor(p1, p2, p3, p4):
    return float(_predict(p1, p2, p3, p4)[1])


def virginica(p1, p2, p3, p4):
    return float(_predict(p1, p2, p3, p4)[2])
