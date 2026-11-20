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
"""Compiles an INLINE Python UDF with Numba for NES: NumbaUdfCompiler runs it at CREATE FUNCTION.

Usage: nes_numba_compile.py REQUEST.json OUTPUT.bc. The request names the entry point ("module.function") or the body with
its parameter names, the argument and return types (NES type names), which arguments the body takes as Optional, and the
import paths. The output is LLVM bitcode that defines the entry `symbol` with the ABI of NES's Codon entry
(PythonUdfCompiler.cpp, createPythonUdfSource), so that NES links and calls it the same way:

    i8 symbol(<per argument: value (VARSIZED: ptr, i64), i8 is_null>, <result: VARSIZED: ptr ptr, ptr size; else ptr>,
              ptr result_is_null, ptr error)

returning 0, or 1 with the error holder set. The UDF itself is the unchanged function, compiled through nes_numba_ext's
front end. A VARSIZED argument becomes a Numba string without a copy if it is ASCII (utf8_ext); a VARSIZED result is written
as UTF-8 into memory from seq_alloc, which NES serves from the arena of the current call and copies right after it. Numba's
own runtime (NRT, typed list and dict, Unicode tables) is resolved by the worker (NumbaRuntime.cpp).
"""

import hashlib
import importlib
import json
import os
import sys
import tempfile
import textwrap
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import llvmlite.binding as llvm  # noqa: E402
import numba  # noqa: E402
from numba import types  # noqa: E402
from numba.core import cgutils  # noqa: E402
from numba.core.extending import intrinsic  # noqa: E402

import nes_numba_ext  # noqa: E402
from nes_numba_ext.utf8_ext import text_from_utf8, unwrap, utf8_size, write_utf8  # noqa: E402

SCALARS = {"INT8": (types.int8, "i8"), "INT16": (types.int16, "i16"), "INT32": (types.int32, "i32"),
           "INT64": (types.int64, "i64"), "UINT8": (types.uint8, "i8"), "UINT16": (types.uint16, "i16"),
           "UINT32": (types.uint32, "i32"), "UINT64": (types.uint64, "i64"), "FLOAT32": (types.float32, "float"),
           "FLOAT64": (types.float64, "double"), "BOOLEAN": (types.uint8, "i8")}
ERROR_MESSAGE = "the Numba UDF raised an exception"

seq_alloc = types.ExternalFunction("seq_alloc", types.voidptr(types.int64))


@intrinsic
def _null_pointer(typingctx):
    def codegen(context, builder, signature, arguments):
        return context.get_constant_null(types.voidptr)
    return types.voidptr(), codegen


@intrinsic
def _bytes_pointer(typingctx, pointer):
    def codegen(context, builder, signature, arguments):
        return builder.bitcast(arguments[0], context.get_value_type(signature.return_type))
    return types.CPointer(types.uint8)(pointer), codegen


def load_function(request):
    for path in reversed(request["importPaths"]):
        sys.path.insert(0, path)
    if request["entrypoint"]:
        module, name = request["entrypoint"].rsplit(".", 1)
        return getattr(importlib.import_module(module), name)
    # A body becomes a module file, as the front end reads the function's source.
    parameters = ", ".join(request["parameterNames"])
    source = f"def nes_numba_body({parameters}):\n" + textwrap.indent(textwrap.dedent(request["body"]).strip("\n") + "\n", "    ")
    directory = os.path.join(tempfile.gettempdir(), "nes-numba-udf-bodies")
    os.makedirs(directory, exist_ok=True)
    name = "nes_numba_body_" + hashlib.sha256(source.encode()).hexdigest()[:16]
    with open(os.path.join(directory, name + ".py"), "w") as file:
        file.write(source)
    sys.path.insert(0, directory)
    return importlib.import_module(name).nes_numba_body


def entry_source(request):
    """The Python source of the jitted entry with NES's ABI (without the error holder), and its Numba signature."""
    parameters, signature, lines, values = [], [], [], []
    # A NULL result also sets the value slots, as the Codon entry does: NES reads the size of a VARSIZED result before it
    # looks at the NULL flag.
    null_result = ["r_null[0] = 1"] + (["r_ptr[0] = _null_pointer()", "r_size[0] = 0"] if request["returnType"] == "VARSIZED" else [])
    for index, (kind, optional) in enumerate(zip(request["argumentTypes"], request["bodyNullable"])):
        if kind == "VARSIZED":
            parameters += [f"a{index}_ptr", f"a{index}_size"]
            signature += [types.CPointer(types.uint8), types.int64]
            value = f"text_from_utf8(a{index}_ptr, a{index}_size)"
        elif kind in SCALARS:
            parameters.append(f"a{index}")
            signature.append(SCALARS[kind][0])
            value = f"a{index} != 0" if kind == "BOOLEAN" else f"a{index}"
        else:
            raise SystemExit(f"argument {index + 1}: type {kind} is not supported by the Numba compiler")
        parameters.append(f"a{index}_null")
        signature.append(types.uint8)
        if optional:
            lines.append(f"v{index} = None if a{index}_null != 0 else {value}")
        else:
            lines += [f"if a{index}_null != 0:", *[f"    {line}" for line in null_result], "    return 0", f"v{index} = {value}"]
        values.append(f"v{index}")
    result = request["returnType"]
    if result == "VARSIZED":
        parameters += ["r_ptr", "r_size"]
        signature += [types.CPointer(types.voidptr), types.CPointer(types.int64)]
        store = ["text = unwrap(result)", "size = utf8_size(text)", "buffer = seq_alloc(size)",
                 "write_utf8(text, _bytes_pointer(buffer))", "r_ptr[0] = buffer", "r_size[0] = size"]
    elif result in SCALARS:
        parameters.append("r")
        signature.append(types.CPointer(SCALARS[result][0]))
        store = ["r[0] = 1 if unwrap(result) else 0"] if result == "BOOLEAN" else ["r[0] = unwrap(result)"]
    else:
        raise SystemExit(f"return type {result} is not supported by the Numba compiler")
    parameters.append("r_null")
    signature.append(types.CPointer(types.uint8))
    lines += [f"result = udf({', '.join(values)})", "if result is None:", *[f"    {line}" for line in null_result], "    return 0",
              "r_null[0] = 0", *store, "return 0"]
    source = f"def nes_numba_entry({', '.join(parameters)}):\n" + "".join(f"    {line}\n" for line in lines)
    return source, signature


def llvm_type(numba_type):
    if isinstance(numba_type, (types.CPointer, types.RawPointer)):
        return "ptr"
    for _, (scalar, name) in SCALARS.items():
        if numba_type == scalar:
            return name
    raise AssertionError(numba_type)


def shim(symbol, core, signature, module):
    """`symbol` with NES's ABI: calls the jitted entry with Numba's calling convention (status, result, exception info) and
    turns a status other than 0 into status 1 with ERROR_MESSAGE in the error holder."""
    parameters = [llvm_type(argument) for argument in signature]
    names = [f"%p{index}" for index in range(len(parameters))]
    message = ERROR_MESSAGE.encode()
    return f'''
target triple = "{module.triple}"
target datalayout = "{module.data_layout}"
@nes_numba_error = private unnamed_addr constant [{len(message)} x i8] c"{message.decode()}"
declare i32 @"{core}"(ptr, ptr, {", ".join(parameters)})
define i8 @"{symbol}"({", ".join(f"{kind} {name}" for kind, name in zip(parameters, names))}, ptr %error) {{
entry:
  %ret = alloca i8
  %exc = alloca ptr
  store ptr null, ptr %exc
  %status = call i32 @"{core}"(ptr %ret, ptr %exc, {", ".join(f"{kind} {name}" for kind, name in zip(parameters, names))})
  %ok = icmp eq i32 %status, 0
  br i1 %ok, label %done, label %failed
done:
  %value = load i8, ptr %ret
  ret i8 %value
failed:
  store ptr @nes_numba_error, ptr %error
  %size = getelementptr inbounds {{ptr, i64}}, ptr %error, i32 0, i32 1
  store i64 {len(message)}, ptr %size
  ret i8 1
}}
'''


def main():
    started = time.perf_counter()
    request = json.load(open(sys.argv[1]))
    function = load_function(request)
    udf = nes_numba_ext.njit(function)
    source, signature = entry_source(request)
    namespace = {"udf": udf, "text_from_utf8": text_from_utf8, "unwrap": unwrap, "utf8_size": utf8_size,
                 "write_utf8": write_utf8, "seq_alloc": seq_alloc, "_bytes_pointer": _bytes_pointer, "_null_pointer": _null_pointer}
    exec(compile(source, "<nes_numba_entry>", "exec"), namespace)
    entry = numba.njit(types.int8(*signature))(namespace["nes_numba_entry"])
    overload = entry.overloads[tuple(signature)]
    compiled = time.perf_counter()
    module = llvm.parse_assembly(overload.library.get_llvm_str())
    module.link_in(llvm.parse_assembly(shim(request["symbol"], overload.fndesc.mangled_name, signature, module)))
    module.verify()
    with open(sys.argv[2], "wb") as output:
        output.write(module.as_bitcode())
    print(f"numba compile {(compiled - started) * 1000:.1f} ms, bitcode {(time.perf_counter() - compiled) * 1000:.1f} ms",
          file=sys.stderr)


if __name__ == "__main__":
    main()
