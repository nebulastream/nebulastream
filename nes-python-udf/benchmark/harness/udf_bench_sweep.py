#!/usr/bin/env python3
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
"""Runs all queries of a benchmark test file (default nes-systests/benchmark/PythonUdf.test) in benchmark mode (one query at a time) on one layout of
run_experiment.py and appends one line per query and round to ../results/<experiment>.jsonl: its time, its rows per second, the
commit and the full command line. The UDF processes run on the interpreter of UDF_BENCH_PYTHON through the venv of the harness.
Generate the inputs first, see nes-systests/benchmark/generate_udf_bench_input.py."""

import argparse
import glob
import json
import os
import re
import shutil
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import run_experiment  # noqa: E402

HARNESS = os.path.dirname(os.path.abspath(__file__))
REPOSITORY = os.path.abspath(os.path.join(HARNESS, "../../.."))
TEST = os.path.join(REPOSITORY, "nes-systests/benchmark/PythonUdf.test")
ROWS = {"rows300m": 300_000_000, "rows30m": 30_000_000, "rows3m": 3_000_000, "rows1m": 1_000_000}


def queries(test):
    """The query of each number, as systest numbers them: the statements that end with ----."""
    statements = re.findall(r"^(SELECT .*?);\n----", open(test).read(), flags=re.MULTILINE | re.DOTALL)
    return {number + 1: statement for number, statement in enumerate(statements)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment", required=True)
    parser.add_argument("--test", default=TEST, help="the benchmark test file (default: benchmark/PythonUdf.test)")
    parser.add_argument("--layout", default="T w16 p16")
    parser.add_argument("--rounds", type=int, default=1)
    parser.add_argument("--hybrid", action="store_true", help="optimizer.udf_process_layout=HYBRID")
    parser.add_argument("--protocol", default="SHARED_MEMORY_ZERO_COPY")
    parser.add_argument("--memory", type=int, default=8589934592)
    parser.add_argument("--python", default="/usr/bin/pypy3")
    parser.add_argument("--build", default=os.path.join(REPOSITORY, "cmake-build-relwithdebinfo-libstdcxx-none"))
    parser.add_argument("--data", default="/dev/shm/nes-udf-bench")
    parser.add_argument("--scratch", default="/tmp/nes-udf-experiments")
    parser.add_argument("--worker-argument", action="append", default=[], metavar="ARG")
    parser.add_argument("--environment", action="append", default=[], metavar="NAME=VALUE")
    parser.add_argument("--note", default="")
    arguments = parser.parse_args()

    if run_experiment.git("status", "--porcelain", "--untracked-files=no"):
        raise SystemExit("There are uncommitted changes. Commit them first, so that the recorded commit says what was measured.")
    layout = next((entry for entry in run_experiment.layouts() if entry["name"] == arguments.layout), None)
    if layout is None:
        raise SystemExit(f"unknown layout {arguments.layout}")

    run_dir = os.path.join(arguments.scratch, f"{arguments.experiment}-{layout['name'].replace(' ', '-')}")
    shutil.rmtree(run_dir, ignore_errors=True)
    os.makedirs(run_dir + "/work")
    environment = dict(os.environ, RUN_DIR=run_dir, BOOT=os.path.join(HARNESS, "boot.py"), UDF_BENCH_PYTHON=arguments.python)
    environment.update(setting.split("=", 1) for setting in arguments.environment)
    worker_arguments = [
        f"--worker.query_engine.number_of_worker_threads={layout['w']}",
        f"--worker.python_udf.processes={layout['p']}",
        f"--worker.python_udf.process_venv={os.path.join(HARNESS, 'venv')}",
        f"--worker.python_udf.process_protocol={arguments.protocol}",
        f"--worker.total_memory_in_bytes={arguments.memory}",
        "--worker.query_engine.cpu_pinning=true",
        "--worker.query_engine.performance_cpus=" + ",".join(map(str, layout["perf"])),
        "--worker.query_engine.misc_cpus=" + ",".join(map(str, layout["misc"])),
        "--worker.python_udf.process_cpus=" + ",".join(map(str, layout["py"])),
        *(["--optimizer.udf_process_layout=HYBRID"] if arguments.hybrid else []),
        *arguments.worker_argument,
    ]
    command = [
        os.path.join(arguments.build, "nes-systests/systest/systest"),
        "-t", arguments.test,
        "--data", arguments.data,
        "--workingDir", run_dir + "/work",
        "--log-path", run_dir + "/log.txt",
        "-b", "--benchmark-rounds", str(arguments.rounds),
        "-c", os.path.join(HARNESS, "single-node.yaml"),
        "--", *worker_arguments,
    ]
    started = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    with open(run_dir + "/out.txt", "w") as output:
        return_code = subprocess.run(command, env=environment, stdout=output, stderr=subprocess.STDOUT).returncode
    print(open(run_dir + "/out.txt").read()[-3000:])

    statements = queries(arguments.test)
    common = {
        "experiment": arguments.experiment,
        "test": os.path.relpath(arguments.test, REPOSITORY),
        "commit": run_experiment.git("rev-parse", "HEAD"),
        "commit_time": run_experiment.git("log", "-1", "--format=%cI"),
        "started": started,
        "command": command,
        "environment": arguments.environment,
        "layout": layout,
        "hybrid": arguments.hybrid,
        "protocol": arguments.protocol,
        "python": arguments.python,
        "return_code": return_code,
        "note": arguments.note,
    }
    records = []
    for path in glob.glob(run_dir + "/work/**/BenchmarkResults.json", recursive=True):
        for entry in json.load(open(path)):
            number = int(entry["query name"].rsplit(":", 1)[1])
            statement = statements[number]
            source = re.search(r"FROM (rows\w+)", statement).group(1)
            records.append(dict(common, query=number, statement=statement, rows=ROWS[source], seconds=entry["time"],
                                mrows_per_s=ROWS[source] / entry["time"] / 1e6))
    records.sort(key=lambda record: record["query"])
    with open(os.path.join(HARNESS, "../results", arguments.experiment + ".jsonl"), "a") as results:
        for record in records:
            results.write(json.dumps(record) + "\n")
    for record in records:
        print(f"{record['query']:3} {record['seconds']:8.2f} s {record['mrows_per_s']:8.2f} M rows/s  {record['statement'][:110]}")
    print(f"{len(records)} results of {len(statements)} queries, return code {return_code}")


if __name__ == "__main__":
    main()
