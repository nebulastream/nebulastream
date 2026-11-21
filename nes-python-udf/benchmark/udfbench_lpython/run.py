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
"""Runs the UDFBench scalar UDFs with LPython, without NebulaStream, on the same tiny data as udfbench_runner.py (E78, E79).

LPython compiles only a typed subset of Python. udfs/ holds a port of each UDF that LPython can compile, with the smallest
changes it needs (each file names them). UNSUPPORTED gives the reason for each other UDF. For a port, the script compiles it
with `lpython --fast` to an object file, links it with driver.c, which calls the UDF in a loop over the rows held in memory
as nes-python-udf-runner does, and compares the results of the first round with CPython running the unchanged UDF.

Appends one JSON line per UDF to results/<experiment>.jsonl. Needs a clean working tree, so that the recorded commit says
what was measured. Usage: run.py --experiment E80-lpython --lpython <env>/bin/lpython --scratch DIR
"""

import argparse
import json
import math
import os
import pathlib
import subprocess
import sys
import time

HERE = pathlib.Path(__file__).resolve().parent
REPOSITORY = HERE.parents[2]
sys.path.insert(0, str(HERE.parent / "udfbench_numba"))

import udfs  # noqa: E402

# UDFs without a port, and why. The LPython messages are those of LPython 0.22.0.
UNSUPPORTED = {
    "addnoise": "random.gauss does not exist in LPython's random module (\"The symbol 'gauss' not found in the module 'random'\")",
    "clean": "imports json, which LPython does not have (\"Could not find the module 'json'\")",
    "extractprojectid": "imports re, which LPython does not have (\"Could not find the module 're'\")",
    "frequentterms": "imports collections.Counter and heapq, which LPython does not have (\"Could not find the module 'collections'\")",
    "jaccard": "imports json, which LPython does not have",
    "jpack": "imports json (json.dumps), which LPython does not have",
    "jsoncount": "imports json, which LPython does not have",
    "jsonparse": "imports json, which LPython does not have",
    "jsort": "imports json, which LPython does not have; list.sort and sorted are not implemented either",
    "jsortvalues": "imports json, which LPython does not have; sorted is not implemented either",
    "keywords": "imports re, which LPython does not have",
    "removeshortterms": "imports json, which LPython does not have",
    "stem": "imports re, which LPython does not have; it also uses nested functions, which LPython does not implement",
}

# Argument kinds and return kind of driver.c, per port.
SIGNATURES = {
    "cleandate": "s-s",
    "converttoeuro": "fs-f",
    "extractclass": "s-s",
    "extractcode": "s-s",
    "extractday": "s-i",
    "extractfunder": "s-s",
    "extractid": "s-s",
    "extractmonth": "s-i",
    "extractyear": "s-i",
    "filterstopwords": "s-s",
    "log_10": "f-f",
    "lowerize": "s-s",
}


def git(*arguments):
    return subprocess.run(["git", "-C", str(REPOSITORY), *arguments], capture_output=True, text=True, check=True).stdout.strip()


def reference(name, rows):
    """The results of CPython with the unchanged UDF, as text lines; a NULL argument gives NULL without a call."""
    function = udfs.function(name)
    lines = []
    for row in rows:
        result = None if any(value is None for value in row) else function(None, *row)
        lines.append("NULL" if result is None else str(result))
    return lines


def same(expected, actual, kind):
    if expected == "NULL" or actual == "NULL" or kind != "f":
        return expected == actual
    a, b = float(expected), float(actual)
    return a == b or (math.isnan(a) and math.isnan(b))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--experiment", required=True)
    parser.add_argument("--lpython", required=True, help="the lpython binary")
    parser.add_argument("--udfs", help="comma-separated UDF names (default: all)")
    parser.add_argument("--rounds", type=int, default=3, help="timed rounds after the first")
    parser.add_argument("--scratch", required=True, help="directory for objects, inputs and results")
    arguments = parser.parse_args()
    if git("status", "--porcelain", "--untracked-files=no"):
        raise SystemExit("There are uncommitted changes. Commit them first, so that the recorded commit says what was measured.")

    commit = git("rev-parse", "HEAD")
    version = subprocess.run([arguments.lpython, "--version"], capture_output=True, text=True).stdout.split("\n")[0]
    runtime = subprocess.run([arguments.lpython, "--get-rtl-dir"], capture_output=True, text=True).stdout.strip()
    scratch = pathlib.Path(arguments.scratch)
    scratch.mkdir(parents=True, exist_ok=True)
    subprocess.run(["clang-22", "-O2", "-c", str(HERE / "driver.c"), "-o", str(scratch / "driver.o")], check=True)
    results = HERE.parent / "results" / (arguments.experiment + ".jsonl")
    names = arguments.udfs.split(",") if arguments.udfs else sorted(udfs.UDFS)
    for name in names:
        record = {"experiment": arguments.experiment, "commit": commit, "udf": name, "source": udfs.UDFS[name][0],
                  "compiler": "LPython", "lpython": version}
        if name in UNSUPPORTED:
            record |= {"supported": False, "reason": UNSUPPORTED[name]}
        else:
            directory = scratch / name
            directory.mkdir(exist_ok=True)
            rows = udfs.inputs(name)
            with open(directory / "args.tsv", "w", encoding="utf-8") as file:
                for row in rows:
                    null = any(value is None for value in row)
                    file.write("\t".join(["1" if null else "0", *("" if null else repr(v) if isinstance(v, float) else str(v) for v in row)]) + "\n")
            started = time.monotonic()
            compiled = subprocess.run([arguments.lpython, "--fast", "-c", str(HERE / "udfs" / f"{name}.py"), "-o", str(directory / "udf.o")],
                                      capture_output=True, text=True)
            compile_ms = (time.monotonic() - started) * 1000
            if compiled.returncode != 0:
                record |= {"supported": False, "reason": "compile error: " + (compiled.stdout + compiled.stderr)[-500:]}
            else:
                subprocess.run(["objcopy", "--redefine-sym", "main=lpython_main", str(directory / "udf.o")], check=True)
                subprocess.run(["clang-22", "-o", str(directory / "driver"), str(scratch / "driver.o"), str(directory / "udf.o"),
                                f"-L{runtime}", "-llpython_runtime_static", "-lm"], check=True)
                command = [str(directory / "driver"), str(directory / "args.tsv"), SIGNATURES[name], str(arguments.rounds),
                           str(directory / "out.txt")]
                completed = subprocess.run(command, capture_output=True, text=True)
                lines = [line for line in completed.stdout.splitlines() if line.startswith("{")]
                record |= {"supported": True, "return_code": completed.returncode, "command": command, "compile_ms": round(compile_ms, 1)}
                record |= json.loads(lines[-1]) if lines else {"error": completed.stderr[-500:]}
                if completed.returncode == 0:
                    expected = reference(name, rows)
                    actual = open(directory / "out.txt", encoding="utf-8", errors="surrogateescape").read().split("\n")[:-1]
                    kind = SIGNATURES[name][-1]
                    differing = [i for i, (e, a) in enumerate(zip(expected, actual)) if not same(e, a, kind)]
                    record["same_results_as"] = {"CPython": len(expected) == len(actual) and not differing}
                    record["differing_rows"] = len(differing) + abs(len(expected) - len(actual))
                    record["differing_examples"] = [{"row": i, "cpython": expected[i][:200], "lpython": actual[i][:200]} for i in differing[:3]]
        with open(results, "a") as file:
            file.write(json.dumps(record, ensure_ascii=False) + "\n")
        state = "unsupported" if not record["supported"] else f"rc {record.get('return_code')}"
        print(f"{name:18} {'best %10.1f ns/row' % record['best_ns_per_row'] if 'best_ns_per_row' in record else state:22} "
              f"same as CPython: {record.get('same_results_as', {}).get('CPython', '-')} ({record.get('differing_rows', '-')} rows differ)",
              flush=True)


if __name__ == "__main__":
    main()
