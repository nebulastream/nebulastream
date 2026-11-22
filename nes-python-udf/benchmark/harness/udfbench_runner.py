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
"""Runs the UDFBench scalar UDFs with nes-python-udf-runner: the INLINE UDF without the query engine, on the tiny data held in
memory, with Codon and with Numba (needs NES_NUMBA_PYTHON). The signatures and entry points come from
benchmark/UdfBenchCodonNumba.test, the source and argument columns of each UDF from udfbench_numba/udfs.py.

Per UDF and compiler it appends the runner's JSON line (compile and JIT time, ns per row of the first round and the best and
median of the timed rounds) to results/<experiment>.jsonl, and writes the results of the first round to --scratch. When both
compilers run, each line says whether the results of the two are the same (addnoise draws random numbers, so they differ).
Needs a clean working tree, so that the recorded commit says what was measured.
"""

import argparse
import json
import os
import re
import subprocess
import sys

HARNESS = os.path.dirname(os.path.abspath(__file__))
REPOSITORY = os.path.abspath(os.path.join(HARNESS, "../../.."))
TEST = os.path.join(REPOSITORY, "nes-systests/benchmark/UdfBenchCodonNumba.test")
UDF_PATH = os.path.join(REPOSITORY, "nes-systests/udf/pyudfs")
sys.path.insert(0, os.path.join(HARNESS, "../udfbench_numba"))

import udfs  # noqa: E402


def git(*arguments):
    return subprocess.run(["git", "-C", REPOSITORY, *arguments], capture_output=True, text=True, check=True).stdout.strip()


def signatures():
    """UDF name -> (argument types, return type, entry point), from the CREATE FUNCTION statements of the test."""
    result = {}
    pattern = re.compile(r"^CREATE FUNCTION (\w+?)_(?:inline|adapted) \((.*?)\) RETURNS (\w+) .*ENTRYPOINT '([^']+)';", re.MULTILINE)
    for name, parameters, returns, entrypoint in pattern.findall(open(TEST).read()):
        types = [parameter.split()[1] for parameter in parameters.split(",")]
        result[name] = (types, returns, entrypoint)
    return result


def runner_arguments(name, types):
    source, columns, _ = udfs.UDFS[name]
    file_name, schema = udfs.SOURCES[source]
    arguments = []
    for column, kind in zip(columns, types):
        if column.startswith("='"):
            arguments += ["--arg", f"literal:{kind}:{column[2:-1]}"]
        else:
            arguments += ["--arg", f"column:{schema.index(column)}:{kind}"]
    if name == "frequentterms":
        arguments += ["--arg", f"literal:{types[1]}:{udfs.FREQUENTTERMS_N}"]
    return source, str(udfs.DATA / file_name), arguments


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment", required=True)
    parser.add_argument("--build", default=os.path.join(REPOSITORY, "cmake-build-relwithdebinfo-libstdcxx-none"))
    parser.add_argument("--compilers", default="Codon,Numba")
    parser.add_argument("--udfs", help="comma-separated UDF names (default: all)")
    parser.add_argument("--rounds", type=int, default=3, help="timed rounds after the first")
    parser.add_argument("--codon-py-numerics", action="store_true",
                        help="compile Codon with Python numerics (NES_CODON_PY_NUMERICS=1); recorded in each line")
    parser.add_argument("--scratch", required=True, help="directory for the results of each UDF")
    arguments = parser.parse_args()
    if git("status", "--porcelain", "--untracked-files=no"):
        raise SystemExit("There are uncommitted changes. Commit them first, so that the recorded commit says what was measured.")

    runner = os.path.join(arguments.build, "nes-python-udf/benchmark/runner/nes-python-udf-runner")
    commit = git("rev-parse", "HEAD")
    known = signatures()
    names = arguments.udfs.split(",") if arguments.udfs else sorted(known)
    compilers = arguments.compilers.split(",")
    os.makedirs(arguments.scratch, exist_ok=True)
    results = os.path.join(HARNESS, "../results", arguments.experiment + ".jsonl")
    for name in names:
        types, returns, entrypoint = known[name]
        source, data, udf_arguments = runner_arguments(name, types)
        outputs = {}
        for compiler in compilers:
            output = os.path.join(arguments.scratch, f"{name}-{compiler}.out")
            command = [runner, "--compiler", compiler, "--entrypoint", entrypoint, "--import-path", UDF_PATH, "--returns", returns,
                       *udf_arguments, "--input", data, "--rounds", str(arguments.rounds), "--output", output]
            environment = {**os.environ, "NES_CODON_PY_NUMERICS": "1" if arguments.codon_py_numerics else "0"}
            completed = subprocess.run(command, capture_output=True, text=True, env=environment)
            lines = [line for line in completed.stdout.splitlines() if line.startswith("{")]
            record = json.loads(lines[-1]) if lines else {"error": completed.stderr.strip()[-500:]}
            record = {"experiment": arguments.experiment, "commit": commit, "udf": name, "source": source,
                      "codon_py_numerics": arguments.codon_py_numerics, "return_code": completed.returncode,
                      "command": command, **record}
            outputs[compiler] = output
            if len(outputs) == 2:
                first, second = (open(path, "rb").read() for path in outputs.values())
                record["same_results_as"] = {compilers[0]: first == second}
            with open(results, "a") as file:
                file.write(json.dumps(record) + "\n")
            print(f"{name:18} {compiler:6} best {record.get('best_ns_per_row', float('nan')):12.1f} ns/row  "
                  f"compile {record.get('compile_ms', float('nan')):8.1f} ms  rc {completed.returncode}", flush=True)


if __name__ == "__main__":
    main()
