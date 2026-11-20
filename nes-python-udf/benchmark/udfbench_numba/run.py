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
"""Compiles the unchanged UDFBench scalar UDFs with Numba (nopython mode, with the nes_numba_ext extensions) and checks
them against CPython on the tiny StreamUDFBench data.

Usage: run.py [--adapted] [--compile-only | --loop ROUNDS] [UDF ...]. With --adapted, it compiles the functions of
udfbench_scalar_inline_adapted.py instead (addnoise by default) and checks them against the unchanged UDFBench functions under
CPython. Prints one JSON line per UDF: compile time (wall and CPU), rows checked, mismatches with the first ones, the time of
one pass over all checked rows with Numba and with CPython (the fastest of REPEATS passes, calls from Python included), and the
error if the UDF does not compile.

--compile-only compiles the UDFs one after the other in this process and prints only the compile times: in one process per
UDF the cold time, in one process for all the warm time, as later UDFs reuse what Numba compiled for earlier ones.
--loop ROUNDS times ROUNDS passes over the checked rows inside one jitted function (no call from Python per row), the
fastest of REPEATS, as ns per row, and one pass of the function under CPython (calls from Python), as ns per row. Rows with a NULL argument are skipped: the query result is NULL for them.
addnoise draws random numbers; its results are only checked to be floats.
"""

import json
import math
import sys
import time

import numba
from numba import types

import nes_numba_ext  # noqa: F401  # registers the extensions
import udfs

REPEATS = 3

TYPES = {"str": types.unicode_type, "int": types.int64, "float": types.float64}


def same(expected, got):
    if isinstance(expected, float) and isinstance(got, float):
        return expected == got or (math.isnan(expected) and math.isnan(got)) or math.isclose(expected, got, rel_tol=1e-12)
    return expected == got and type(expected) is type(got)


def check(name, adapted=False):
    source, columns, kinds = udfs.UDFS[name]
    signature = ([] if adapted else [types.none]) + [TYPES[kind] for kind in kinds]
    if name == "frequentterms":
        signature.append(types.int64)
    reference = udfs.function(name)
    function = udfs.adapted_function(name) if adapted else reference
    dispatcher = nes_numba_ext.njit(function)
    self_arguments = () if adapted else (None,)
    result = {"udf": name, "numba": numba.__version__, "adapted": adapted}
    wall, cpu = time.perf_counter(), time.process_time()
    try:
        dispatcher.compile(tuple(signature))
    except Exception as error:  # noqa: BLE001 - a UDF that does not compile is a result
        result["error"] = f"{type(error).__name__}: {' '.join(str(error).split())[:600]}"
        return result
    finally:
        result["compile_wall_ms"] = round((time.perf_counter() - wall) * 1000, 1)
        result["compile_cpu_ms"] = round((time.process_time() - cpu) * 1000, 1)
    checked, mismatches, examples = 0, 0, []
    for row in udfs.inputs(name):
        if any(value is None for value in row):
            continue
        checked += 1
        try:
            expected = reference(None, *row)
        except Exception as error:  # noqa: BLE001
            expected = f"raises {type(error).__name__}"
        try:
            got = dispatcher(*self_arguments, *row)
        except Exception as error:  # noqa: BLE001
            got = f"raises {type(error).__name__}"
        ok = isinstance(got, float) if name == "addnoise" and expected is not None else same(expected, got)
        if not ok:
            mismatches += 1
            if len(examples) < 3:
                examples.append({"input": [str(value)[:120] for value in row], "expected": str(expected)[:120], "got": str(got)[:120]})
    result.update(rows=checked, mismatches=mismatches, examples=examples)
    rows = [row for row in udfs.inputs(name) if all(value is not None for value in row)]
    for key, call in (("numba_pass_ms", dispatcher), ("cpython_pass_ms", function)):
        call = (lambda target: lambda *row: target(*self_arguments, *row))(call)
        best = math.inf
        for _ in range(REPEATS):
            start = time.perf_counter()
            for row in rows:
                try:
                    call(*row)
                except Exception:  # noqa: BLE001, S110 - a raising row costs time like any other row
                    pass
            best = min(best, time.perf_counter() - start)
        result[key] = round(best * 1000, 1)
    return result


def prepare(name, adapted):
    """The jitted UDF and its signature."""
    _, _, kinds = udfs.UDFS[name]
    signature = ([] if adapted else [types.none]) + [TYPES[kind] for kind in kinds]
    if name == "frequentterms":
        signature.append(types.int64)
    function = udfs.adapted_function(name) if adapted else udfs.function(name)
    return nes_numba_ext.njit(function), tuple(signature)


def compile_only(name, adapted=False):
    result = {"udf": name, "numba": numba.__version__, "adapted": adapted}
    wall = time.perf_counter()
    try:
        dispatcher, signature = prepare(name, adapted)
        dispatcher.compile(signature)
    except Exception as error:  # noqa: BLE001 - a UDF that does not compile is a result
        result["error"] = f"{type(error).__name__}: {' '.join(str(error).split())[:300]}"
    result["compile_wall_ms"] = round((time.perf_counter() - wall) * 1000, 1)
    return result


def loop(name, rounds, adapted=False):
    """ns per row of ROUNDS passes over the checked rows in one jitted function."""
    dispatcher, signature = prepare(name, adapted)
    dispatcher.compile(signature)
    rows = [row for row in udfs.inputs(name) if all(value is not None for value in row)]
    arguments = ", ".join(f"row[{index}]" for index in range(len(rows[0])))
    self_argument = "" if adapted else "None, "
    source = ("def drive(rows, rounds):\n"
              "    nulls = 0\n"
              "    for _ in range(rounds):\n"
              "        for row in rows:\n"
              "            try:\n"
              f"                if udf({self_argument}{arguments}) is None:\n"
              "                    nulls += 1\n"
              "            except Exception:\n"
              "                nulls += 1\n"
              "    return nulls\n")
    namespace = {"udf": dispatcher}
    exec(source, namespace)
    drive = numba.njit(namespace["drive"])
    typed_rows = numba.typed.List(rows)
    drive(typed_rows, 1)
    best = math.inf
    for _ in range(REPEATS):
        start = time.perf_counter()
        drive(typed_rows, rounds)
        best = min(best, time.perf_counter() - start)
    function = udfs.adapted_function(name) if adapted else udfs.function(name)
    self_arguments = () if adapted else (None,)
    cpython = math.inf
    for _ in range(REPEATS):
        start = time.perf_counter()
        for row in rows:
            try:
                function(*self_arguments, *row)
            except Exception:  # noqa: BLE001, S110 - a raising row costs time like any other row
                pass
        cpython = min(cpython, time.perf_counter() - start)
    return {"udf": name, "numba": numba.__version__, "adapted": adapted, "rows": len(rows), "rounds": rounds,
            "loop_seconds": round(best, 4), "ns_per_row": round(best / (len(rows) * rounds) * 1e9, 1),
            "cpython_ns_per_row": round(cpython / len(rows) * 1e9, 1)}


if __name__ == "__main__":
    arguments = sys.argv[1:]
    adapted = "--adapted" in arguments
    rounds = int(arguments[arguments.index("--loop") + 1]) if "--loop" in arguments else None
    skip = {"--adapted", "--compile-only", "--loop", str(rounds)}
    names = [argument for argument in arguments if argument not in skip]
    for udf in names or list(udfs.ADAPTED if adapted else udfs.UDFS):
        if "--compile-only" in arguments:
            print(json.dumps(compile_only(udf, adapted)), flush=True)
        elif rounds is not None:
            print(json.dumps(loop(udf, rounds, adapted)), flush=True)
        else:
            print(json.dumps(check(udf, adapted)), flush=True)
