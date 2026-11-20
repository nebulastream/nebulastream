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
"""Runs benchmark/UdfBenchCodonNumba.test (UDFBench scalar UDFs as INLINE, Codon) and writes per UDF:

- the Codon compile time of CREATE FUNCTION (the frontend's "Compiled ... with Codon in" line, with its phases), in the order
  of the test, so the first UDF also loads the Codon stdlib;
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
    parser.add_argument("--scratch", required=True, help="directory for the systest working directory and output")
    arguments = parser.parse_args()
    if git("status", "--porcelain", "--untracked-files=no"):
        raise SystemExit("There are uncommitted changes. Commit them first, so that the recorded commit says what was measured.")

    run_dir = os.path.join(arguments.scratch, arguments.experiment)
    shutil.rmtree(run_dir, ignore_errors=True)
    os.makedirs(run_dir + "/work")
    command = [os.path.join(arguments.build, "nes-systests/systest/systest"), "-t", TEST, "--data", DATA,
               "--workingDir", run_dir + "/work", "--log-path", run_dir + "/log.txt", "-b", "--benchmark-rounds",
               str(arguments.rounds), "-c", os.path.join(HARNESS, "single-node.yaml"), "--",
               "--worker.query_engine.number_of_worker_threads=1"]
    started = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    with open(run_dir + "/out.txt", "w") as output:
        return_code = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT).returncode
    out = open(run_dir + "/out.txt").read()

    test = open(TEST).read()
    creates = re.findall(r"^CREATE FUNCTION (\w+)_inline|^CREATE FUNCTION (\w+)_adapted", test, flags=re.MULTILINE)
    names = [inline or adapted for inline, adapted in creates]
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
        record = dict(common, udf=name, engine="codon")
        if index < len(compiles):
            total, parse, compile_, optimize = map(float, compiles[index])
            record.update(compile_ms=total, parse_ms=parse, typecheck_ms=compile_, llvm_optimize_ms=optimize)
        for number, statement in enumerate(statements, start=1):
            if f"{name}_inline(" not in statement and f"{name}_adapted(" not in statement:
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
        print(f"{record['udf']:18} compile {record.get('compile_ms', float('nan')):8.0f} ms  tiny {record.get('seconds_tiny') or float('nan'):7.3f} s"
              f"  x10 {record.get('seconds_x10') or float('nan'):7.3f} s  {record.get('ns_per_row', float('nan')):10.0f} ns/row")
    print(f"return code {return_code}")


if __name__ == "__main__":
    main()
