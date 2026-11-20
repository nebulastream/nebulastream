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
"""Compiles a UDFBench UDF with Numba into an object file with one C-ABI entry and no Python wrapper, and writes the inputs
and the CPython results for the C++ driver. Usage: build.py UDF OUTDIR"""
import os
import struct
import sys

import llvmlite.binding as llvm
import numba
import numpy as np
from numba import types
from numba.cpython.unicode import _empty_string, _get_code_point, _set_code_point

import nes_numba_ext
import udfs
from nes_numba_ext.utf8_ext import text_from_utf8, unwrap, utf8_size, write_utf8


@numba.njit
def decode(data, length):
    """A Numba string from UTF-8 bytes."""
    count, largest, i = 0, 0, 0
    while i < length:
        b = data[i]
        if b < 0x80:
            code, i = b, i + 1
        elif b < 0xE0:
            code, i = ((b & 0x1F) << 6) | (data[i + 1] & 0x3F), i + 2
        elif b < 0xF0:
            code, i = ((b & 0x0F) << 12) | ((data[i + 1] & 0x3F) << 6) | (data[i + 2] & 0x3F), i + 3
        else:
            code, i = ((b & 0x07) << 18) | ((data[i + 1] & 0x3F) << 12) | ((data[i + 2] & 0x3F) << 6) | (data[i + 3] & 0x3F), i + 4
        count += 1
        largest = max(largest, code)
    kind = 1 if largest < 256 else (2 if largest < 65536 else 4)
    result = _empty_string(kind, count, largest < 128)
    i, j = 0, 0
    while i < length:
        b = data[i]
        if b < 0x80:
            code, i = b, i + 1
        elif b < 0xE0:
            code, i = ((b & 0x1F) << 6) | (data[i + 1] & 0x3F), i + 2
        elif b < 0xF0:
            code, i = ((b & 0x0F) << 12) | ((data[i + 1] & 0x3F) << 6) | (data[i + 2] & 0x3F), i + 3
        else:
            code, i = ((b & 0x07) << 18) | ((data[i + 1] & 0x3F) << 12) | ((data[i + 2] & 0x3F) << 6) | (data[i + 3] & 0x3F), i + 4
        _set_code_point(result, j, code)
        j += 1
    return result


@numba.njit
def encode(text, out, capacity):
    """Writes `text` as UTF-8 to out; its length, or -2 if it does not fit."""
    n = 0
    for j in range(len(text)):
        code = _get_code_point(text, j)
        if n + 4 > capacity:
            return -2
        if code < 0x80:
            out[n] = code
            n += 1
        elif code < 0x800:
            out[n] = 0xC0 | (code >> 6)
            out[n + 1] = 0x80 | (code & 0x3F)
            n += 2
        elif code < 0x10000:
            out[n] = 0xE0 | (code >> 12)
            out[n + 1] = 0x80 | ((code >> 6) & 0x3F)
            out[n + 2] = 0x80 | (code & 0x3F)
            n += 3
        else:
            out[n] = 0xF0 | (code >> 18)
            out[n + 1] = 0x80 | ((code >> 12) & 0x3F)
            out[n + 2] = 0x80 | ((code >> 6) & 0x3F)
            out[n + 3] = 0x80 | (code & 0x3F)
            n += 4
    return n


def compare(outdir):
    """Compares outputs.bin of the driver with expected.bin of CPython, row by row."""
    def read(path):
        with open(path, "rb") as data:
            count, = struct.unpack("<q", data.read(8))
            values = []
            for _ in range(count):
                length, = struct.unpack("<q", data.read(8))
                values.append(data.read(length) if length >= 0 else length)
            return values
    expected, outputs = read(os.path.join(outdir, "expected.bin")), read(os.path.join(outdir, "outputs.bin"))
    differences = sum(1 for a, b in zip(expected, outputs) if a != b)
    print(f"{outdir}: rows {len(expected)} differences {differences}")


def main():
    if sys.argv[1] == "--compare":
        compare(sys.argv[2])
        return
    name, outdir = sys.argv[1], sys.argv[2]
    os.makedirs(outdir, exist_ok=True)
    function = udfs.function(name)
    udf = nes_numba_ext.njit(function)
    extra = ', "doi"' if name == "jsonparse" else (", 5" if name == "frequentterms" else "")
    source = (
        "def entry(data, length, out, capacity):\n"
        "    text = text_from_utf8(data, length)\n"
        f"    result = udf(None, text{extra})\n"
        "    if result is None:\n"
        "        return -1\n"
        "    text = unwrap(result)\n"
        "    if utf8_size(text) > capacity:\n"
        "        return -2\n"
        "    return write_utf8(text, out)\n")
    namespace = {"text_from_utf8": text_from_utf8, "unwrap": unwrap, "utf8_size": utf8_size, "write_utf8": write_utf8,
                 "udf": udf}
    exec(source, namespace)
    pointer = types.CPointer(types.uint8)
    signature = types.int64(pointer, types.int64, pointer, types.int64)
    entry = numba.njit(signature)(namespace["entry"])
    overload = entry.overloads[signature.args]
    core = overload.fndesc.mangled_name
    ir = overload.library.get_llvm_str().replace(core, "nes_numba_entry")
    module = llvm.parse_assembly(ir)
    for function_ in module.functions:
        if not function_.is_declaration and function_.name != "nes_numba_entry":
            function_.linkage = llvm.Linkage.internal
    for variable in module.global_variables:
        if not variable.is_declaration:
            variable.linkage = llvm.Linkage.internal
    target = llvm.Target.from_default_triple().create_target_machine(opt=3, reloc="pic", codemodel="default")
    builder = llvm.create_pass_builder(target, llvm.create_pipeline_tuning_options(speed_level=0))
    passes = llvm.create_new_module_pass_manager()
    passes.add_global_dead_code_eliminate_pass()
    passes.add_strip_dead_prototype_pass()
    passes.run(module, builder)
    module.verify()
    open(os.path.join(outdir, "entry.ll"), "w").write(str(module))
    open(os.path.join(outdir, "entry.o"), "wb").write(target.emit_object(module))
    rows = [row for row in udfs.inputs(name) if row[0] is not None]
    with open(os.path.join(outdir, "inputs.bin"), "wb") as inputs, open(os.path.join(outdir, "expected.bin"), "wb") as expected:
        inputs.write(struct.pack("<q", len(rows)))
        expected.write(struct.pack("<q", len(rows)))
        for row in rows:
            data = row[0].encode()
            inputs.write(struct.pack("<q", len(data)) + data)
            try:
                result = function(None, *row)
                value = -1 if result is None else result.encode()
            except Exception:  # noqa: BLE001
                value = -3
            if isinstance(value, bytes):
                expected.write(struct.pack("<q", len(value)) + value)
            else:
                expected.write(struct.pack("<q", value))
    print(name, "IR KB", len(str(module)) // 1024, "functions", sum(1 for f in module.functions if not f.is_declaration))
    print("undefined:", " ".join(sorted(f.name for f in module.functions if f.is_declaration and not f.name.startswith("llvm."))
                                 + sorted(g.name for g in module.global_variables if g.is_declaration)))


if __name__ == "__main__":
    main()
