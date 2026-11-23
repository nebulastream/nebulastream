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

"""Compiles an INLINE UDF with HiPy (COMPILER HIPY) to HiPy's IR, which NES traces with Nautilus (HiPyPhysicalFunction).

Usage: nes_hipy_compile.py REQUEST.json OUTPUT.json. The request names the entry point ("module.function") or the body with
its parameter names, the argument and return types (NES type names), the import paths, and the HiPy checkout (hipyDir).

HiPy runs the UDF's code at compile time with abstract arguments of the declared types and records the operations as its IR
(hipy.compiler). Every function of the UDF's module is marked for HiPy: an unmarked function would become a CPython call.
HiPy's stand-ins for re, math, datetime, random, statistics, collections and urllib.parse are imported, as they replace those
modules only once loaded. The UDF is compiled with fallback: what HiPy cannot compile natively becomes CPython operations
(Py* operations, python.* builtins), which the engine runs through the CPython bridge. The output is
{"entry": <function name>, "pythonOps": <number of CPython operations>, "module": <HiPy IR as JSON (ir.Module.serialize)>,
"chain": <the UDF as a linear chain of native and Python stages (nes_hipy_split), for a plan that runs each stage as an
operator>}. The catalog stores this output with the UDF.
"""

import contextlib
import hashlib
import importlib
import inspect
import json
import os
import sys
import tempfile
import textwrap
import types

HERE = os.path.dirname(os.path.abspath(__file__))
INT_TYPES = {"INT8", "INT16", "INT32", "INT64", "UINT8", "UINT16", "UINT32", "UINT64"}


def load_function(request):
    for path in reversed(request["importPaths"]):
        sys.path.insert(0, path)
    if request["entrypoint"]:
        module_name, name = request["entrypoint"].rsplit(".", 1)
        return importlib.import_module(module_name), name
    # A body becomes a module file, as HiPy reads the function's source.
    parameters = ", ".join(request["parameterNames"])
    source = f"def nes_hipy_body({parameters}):\n" + textwrap.indent(textwrap.dedent(request["body"]).strip("\n") + "\n", "    ")
    module_name = "nes_hipy_body_" + hashlib.sha256(source.encode()).hexdigest()[:16]
    directory = tempfile.mkdtemp(prefix="nes-hipy-")
    with open(os.path.join(directory, module_name + ".py"), "w", encoding="utf-8") as file:
        file.write(source)
    sys.path.insert(0, directory)
    return importlib.import_module(module_name), "nes_hipy_body"


def hipy_type(kind):
    import hipy.ir as ir
    import hipy.lib.builtins as builtins
    from hipy.value import SimpleType

    if kind == "VARSIZED":
        return SimpleType(builtins.str, ir.string)
    if kind in INT_TYPES:
        return SimpleType(builtins.int, ir.int)
    if kind in ("FLOAT64", "FLOAT32"):
        return SimpleType(builtins.float, ir.f64)
    if kind == "BOOLEAN":
        return SimpleType(builtins.bool, ir.bool)
    raise ValueError(f"HiPy UDFs do not support arguments of type {kind}")


def python_operations(node, found):
    """The operations of the serialized module that run in CPython."""
    if isinstance(node, dict):
        kind = node.get("kind", "")
        if kind.startswith("Py") or kind == "PythonCall":
            found.append(kind)
        if kind == "CallBuiltin" and (node["name"].startswith("python.") or node["name"].endswith("_python")):
            found.append(node["name"])
        for value in node.values():
            python_operations(value, found)
    elif isinstance(node, list):
        for value in node:
            python_operations(value, found)
    return found


def keep_keywords_in_fallback():
    """HiPy drops the keyword arguments of a call when it falls back to CPython after one of its stand-ins raised
    NotImplementedError (Context.perform_call re-calls with `perform_call(self.to_python(original_fn), args)`, e.g. for
    datetime.timedelta(seconds=x)). The re-call passes the same `args` list, so the keywords of the outer call are passed on."""
    import hipy.context

    original = hipy.context.Context.perform_call
    calls = []

    def perform_call(self, fn, args=[], kwargs={}, _action_id=None):  # noqa: B006 (HiPy's signature)
        if not kwargs and calls and calls[-1][0] is args:
            kwargs = calls[-1][1]
        calls.append((args, kwargs))
        try:
            return original(self, fn, args, kwargs, _action_id=_action_id)
        finally:
            calls.pop()

    hipy.context.Context.perform_call = perform_call


def stage_chain(serialized, entry_function):
    """The stages of the UDF, with the Python source that computes each (stage_<i>(values) -> outputs). A split that fails
    leaves the UDF as one unit: it does not fail the registration."""
    import nes_hipy_split

    try:
        chain = nes_hipy_split.split(serialized, entry_function)
        chain["pythonSource"] = nes_hipy_split.module_source(serialized, chain, nes_hipy_split.python_setup(serialized))
        shape = " -> ".join(stage["kind"] for stage in chain["stages"])
        print(f"HiPy stages: {shape}", file=sys.stderr)
        return chain
    except Exception as error:  # noqa: BLE001 (any failure of the split keeps the unsplit UDF)
        print(f"HiPy stages: not split ({type(error).__name__}: {error})", file=sys.stderr)
        return {"error": f"{type(error).__name__}: {error}"}


def main():
    with open(sys.argv[1], encoding="utf-8") as file:
        request = json.load(file)
    sys.path.insert(0, request["hipyDir"])
    # Stand-ins for the parts of networkx and typeshed_client that HiPy uses, after the installed packages.
    sys.path.append(os.path.join(HERE, "shim"))
    import hipy
    import hipy.compiler

    keep_keywords_in_fallback()

    for library in ("re", "math", "datetime", "random", "statistics", "collections", "urllib.parse"):
        with contextlib.suppress(Exception):
            importlib.import_module("hipy.lib." + library)

    module, entry_name = load_function(request)
    for name, value in list(vars(module).items()):
        if isinstance(value, types.FunctionType) and value.__module__ == module.__name__ and value.__name__ != "<lambda>":
            setattr(module, name, hipy.compiled_function(value))
    entry = getattr(module, entry_name)
    if not isinstance(entry, hipy.function.HLCFunction):
        raise TypeError(f"the entry point {entry_name} is not a def function")

    argument_types = [hipy_type(kind) for kind in request["argumentTypes"]]
    # HiPy prints its diagnostics to stdout.
    with contextlib.redirect_stdout(sys.stderr):
        ir_module = hipy.compiler.compile(entry, arg_types=argument_types, fallback=True, debug=False)
    serialized = json.loads(json.dumps(ir_module.serialize(), default=lambda value: value.serialize()))
    in_python = python_operations(serialized, [])
    if in_python:
        print(f"HiPy left {len(in_python)} operations to CPython: " + ", ".join(sorted(set(in_python))), file=sys.stderr)
    entry_function = inspect.unwrap(entry.pyfunc).__name__ + hipy.config.function_suffix
    output = {"entry": entry_function, "pythonOps": len(in_python), "module": serialized, "chain": stage_chain(serialized, entry_function)}
    with open(sys.argv[2], "w", encoding="utf-8") as file:
        json.dump(output, file, default=lambda value: value.serialize())


if __name__ == "__main__":
    main()
