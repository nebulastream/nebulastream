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

Usage: run.py [--adapted] [UDF ...]. With --adapted, it compiles the functions of udfbench_scalar_inline_adapted.py instead
(addnoise by default) and checks them against the unchanged UDFBench functions under CPython. Prints one JSON line per UDF: compile time (wall and CPU), rows checked, mismatches with the first
ones, the time of one pass over all checked rows with Numba and with CPython (the fastest of REPEATS passes, calls from
Python included), and the error if the UDF does not compile. Rows with a NULL argument are skipped: the query result is NULL for them.
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


if __name__ == "__main__":
    arguments = sys.argv[1:]
    adapted = "--adapted" in arguments
    names = [argument for argument in arguments if argument != "--adapted"]
    for udf in names or list(udfs.ADAPTED if adapted else udfs.UDFS):
        print(json.dumps(check(udf, adapted)), flush=True)
