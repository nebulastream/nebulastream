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
"""Runs benchmark/UdfBenchCodonNumba.test (UDFBench scalar UDFs as INLINE, Codon), or with --test UdfBenchNumba.test the same
with Numba (COMPILER NUMBA; needs NES_NUMBA_PYTHON), and writes per UDF:

- the compile time of CREATE FUNCTION (the frontend's "Compiled ... with Codon in" line with its phases, or "... with Numba
  in"), in the order of the test, so the first Codon UDF also loads the Codon stdlib;
- the time of the query on the tiny data and on the x10 data (best of --rounds), and the cost per row:
  (time x10 - time tiny) / (rows x10 - rows tiny), which leaves out fixed costs such as the pipeline compilation.

One worker thread, single-node topology. Needs the x10 data (make_udfbench_x10.sh) and a clean working tree, so that the
recorded commit says what was measured. Appends to results/<experiment>.jsonl.
"""

import argparse
import glob
import json
import os
import re
import shutil
import subprocess
import time

HARNESS = os.path.dirname(os.path.abspath(__file__))
REPOSITORY = os.path.abspath(os.path.join(HARNESS, "../../.."))
TEST = os.path.join(REPOSITORY, "nes-systests/benchmark/UdfBenchCodonNumba.test")
DATA = "/dev/shm/streamudfbench-data"
FILES = {"artifacts": "artifacts_15M.csv", "artifact_abstracts": "artifact_abstracts_13M.csv",
         "artifact_authorlists": "artifact_authorlists_4M.csv", "artifact_charges": "artifact_charges_32K.csv",
         "projects_artifacts": "projects_artifacts_16M.csv", "query2json": "query2json_3M.csv"}


def git(*arguments):
    return subprocess.run(["git", "-C", REPOSITORY, *arguments], capture_output=True, text=True, check=True).stdout.strip()


def rows(path):
    with open(path, "rb") as data:
        return sum(1 for _ in data)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment", required=True)
    parser.add_argument("--build", default=os.path.join(REPOSITORY, "cmake-build-relwithdebinfo-libstdcxx-none"))
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument("--query-timeout", type=int, default=1200, help="seconds; stem on the x10 data takes minutes")
    parser.add_argument("--scratch", required=True, help="directory for the systest working directory and output")
    parser.add_argument("--test", default=TEST, help="the benchmark test (default: UdfBenchCodonNumba.test); for "
                        "UdfBenchBaseline.test, the record of each source is its baseline without a UDF")
    parser.add_argument("--udfs", help="comma-separated UDF names: run only their CREATE FUNCTION statements and queries")
    arguments = parser.parse_args()
    if git("status", "--porcelain", "--untracked-files=no"):
        raise SystemExit("There are uncommitted changes. Commit them first, so that the recorded commit says what was measured.")

    if arguments.udfs:
        arguments.test = filtered_test(arguments.test, arguments.udfs.split(","))
    try:
        run(arguments)
    finally:
        if arguments.udfs:
            os.remove(arguments.test)


def filtered_test(path, udfs):
    """A copy of the test, in the same directory (systest needs it in the repository), with only the UDFs in udfs."""
    test = open(path).read()
    keep = lambda line: not re.match(r"(CREATE FUNCTION|SELECT) ", line) or any(re.search(rf"\b{udf}_(inline|adapted|numba)\b", line) for udf in udfs)
    blocks = test.split("\n\n")
    # A query block (SELECT, ----, expected rows) goes as a whole; in other blocks, only the CREATE FUNCTION lines are filtered.
    kept = [block for block in blocks if not block.startswith("SELECT ") or keep(block.splitlines()[0])]
    kept = ["\n".join(line for line in block.splitlines() if keep(line)) for block in kept]
    filtered = os.path.join(os.path.dirname(path), "TmpFiltered_" + os.path.basename(path))
    with open(filtered, "w") as output:
        output.write("\n\n".join(block for block in kept if block.strip()) + "\n")
    return filtered


def run(arguments):
    run_dir = os.path.join(arguments.scratch, arguments.experiment)
    shutil.rmtree(run_dir, ignore_errors=True)
    os.makedirs(run_dir + "/work")
    command = [os.path.join(arguments.build, "nes-systests/systest/systest"), "-t", arguments.test, "--data", DATA,
               "--workingDir", run_dir + "/work", "--log-path", run_dir + "/log.txt", "-b", "--benchmark-rounds",
               str(arguments.rounds), "--query-timeout", str(arguments.query_timeout), "-c", os.path.join(HARNESS, "single-node.yaml"), "--",
               "--worker.query_engine.number_of_worker_threads=1"]
    started = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    with open(run_dir + "/out.txt", "w") as output:
        return_code = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT).returncode
    out = open(run_dir + "/out.txt").read()

    test = open(arguments.test).read()
    creates = re.findall(r"^CREATE FUNCTION (\w+)_(inline|adapted|numba) ", test, flags=re.MULTILINE)
    names = [name for name, _ in creates]
    suffixes = {name: suffix for name, suffix in creates}
    engine = "numba" if "COMPILER NUMBA" in test else "codon"
    baseline = not names
    if baseline:
        names = list(FILES)
    if engine == "numba":
        compiles = [(total, None, None, None) for total in re.findall(r"Compiled '__nes_numba_udf_registered_\d+' with Numba in ([0-9.]+) ms", out)]
    else:
        compiles = re.findall(r"Compiled '__nes_python_udf_registered_\d+' with Codon in ([0-9.]+) ms \(mutex wait: [0-9.]+ ms, "
                              r"parse: ([0-9.]+) ms, compile: ([0-9.]+) ms, LLVM optimize: ([0-9.]+) ms", out)
    statements = re.findall(r"^(SELECT .*?);\n----", test, flags=re.MULTILINE)
    times = {}
    for path in glob.glob(run_dir + "/work/**/BenchmarkResults.json", recursive=True):
        for entry in json.load(open(path)):
            times[int(entry["query name"].rsplit(":", 1)[1])] = entry["time"]

    common = {"experiment": arguments.experiment, "commit": git("rev-parse", "HEAD"), "started": started, "command": command,
              "return_code": return_code, "rounds": arguments.rounds}
    records = []
    for index, name in enumerate(names):
        record = dict(common, udf=None if baseline else name, source=name if baseline else None,
                      engine="nes-baseline" if baseline else engine)
        if index < len(compiles):
            total, parse, compile_, optimize = compiles[index]
            record["compile_ms"] = float(total)
            if parse is not None:
                record.update(parse_ms=float(parse), typecheck_ms=float(compile_), llvm_optimize_ms=float(optimize))
        for number, statement in enumerate(statements, start=1):
            if baseline:
                if re.search(r"FROM (\w+)", statement).group(1).removesuffix("_x10") != name:
                    continue
            elif f"{name}_{suffixes[name]}(" not in statement:
                continue
            source = re.search(r"FROM (\w+)", statement).group(1)
            key = "x10" if source.endswith("_x10") else "tiny"
            base = source.removesuffix("_x10")
            directory = "x10/streamudfbench" if key == "x10" else "large/streamudfbench/tiny"
            record[f"seconds_{key}"] = times.get(number)
            record[f"rows_{key}"] = rows(os.path.join(DATA, directory, FILES[base]))
        if record.get("seconds_tiny") is not None and record.get("seconds_x10") is not None:
            record["ns_per_row"] = (record["seconds_x10"] - record["seconds_tiny"]) / (record["rows_x10"] - record["rows_tiny"]) * 1e9
        records.append(record)
    with open(os.path.join(HARNESS, "../results", arguments.experiment + ".jsonl"), "a") as results:
        for record in records:
            results.write(json.dumps(record) + "\n")
    for record in records:
        print(f"{record['udf'] or record['source']:20} compile {record.get('compile_ms', float('nan')):8.0f} ms  tiny {record.get('seconds_tiny') or float('nan'):7.3f} s"
              f"  x10 {record.get('seconds_x10') or float('nan'):7.3f} s  {record.get('ns_per_row', float('nan')):10.0f} ns/row")
    print(f"return code {return_code}")


if __name__ == "__main__":
    main()
