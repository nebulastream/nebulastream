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
"""Runs the queries of a benchmark test file for each variant and number of worker threads, in benchmark mode (one query at a time),
and appends one line per query to ../results/<experiment>.jsonl: its time, its rows per second, the commit and the full command
line. A variant is an IN_PROCESS UDF interpreter (SHARED, SUBINTERPRETER, DLMOPEN, FREE_THREADED) for
benchmark/PythonUdfInterpreters.test, or a way to run OUT_OF_PROCESS UDFs for benchmark/PythonUdfProcessThreads.test: one process per
worker thread (PROCESSES) or one process with one thread per worker thread (THREADS), each on the python of --gil-venv or, with the
suffix _FT, of --ft-venv. Generate the inputs first, see nes-systests/benchmark/generate_udf_bench_input.py."""

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
TEST = os.path.join(REPOSITORY, "nes-systests/benchmark/PythonUdfInterpreters.test")
ROWS = {"rows30m": 30_000_000, "rows3m": 3_000_000, "rows1m": 1_000_000}


def git(*arguments):
    return subprocess.run(["git", "-C", REPOSITORY, *arguments], capture_output=True, text=True, check=True).stdout.strip()


def queries(test):
    """The query of each number, as systest numbers them: the statements that end with ----."""
    statements = re.findall(r"^(SELECT .*?);\n----", open(test).read(), flags=re.MULTILINE | re.DOTALL)
    return {number + 1: statement for number, statement in enumerate(statements)}


def variant_settings(variant, gil_venv, ft_venv, protocol):
    """The environment and worker arguments of a variant."""
    if variant in ("SHARED", "SUBINTERPRETER", "DLMOPEN", "FREE_THREADED"):
        venv = ft_venv if variant == "FREE_THREADED" else gil_venv
        return {}, [f"--worker.default_query_execution.in_process_udf_interpreter={variant}",
                    *([f"--worker.python_udf.venv={venv}"] if venv else [])]
    kind, _, flavor = variant.partition("_")
    if kind not in ("PROCESSES", "THREADS") or flavor not in ("", "FT"):
        raise SystemExit(f"unknown variant {variant}")
    venv = ft_venv if flavor == "FT" else gil_venv
    if not venv:
        raise SystemExit(f"{variant} needs {'--ft-venv' if flavor == 'FT' else '--gil-venv'}")
    # Only the shared memory protocols let one host process serve several UDF operators, see PythonProcesses::start
    if kind == "THREADS" and not protocol.startswith("SHARED_MEMORY"):
        raise SystemExit(f"{variant} needs a SHARED_MEMORY protocol")
    return ({"NES_UDF_SHARED_PROCESSES": "threads"} if kind == "THREADS" else {}), [
        f"--worker.python_udf.process_venv={venv}", f"--worker.python_udf.process_protocol={protocol}"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment", required=True)
    parser.add_argument("--test", default=TEST)
    parser.add_argument("--variants", default="SHARED,SUBINTERPRETER,DLMOPEN,FREE_THREADED")
    parser.add_argument("--threads", default="1,2,4,8,16")
    parser.add_argument("--calls", default="ROW", help="worker.default_query_execution.in_process_udf_calls")
    parser.add_argument("--rounds", type=int, default=1)
    parser.add_argument("--build", default=os.path.join(REPOSITORY, "cmake-build-relwithdebinfo-libstdcxx-none"))
    parser.add_argument("--protocol", default="SHARED_MEMORY_ZERO_COPY", help="worker.python_udf.process_protocol of OUT_OF_PROCESS variants")
    parser.add_argument("--gil-venv", default="", help="venv of a Python with a GIL")
    parser.add_argument("--ft-venv", default="", help="venv of a free-threaded Python, e.g. <build>/udf-ft-venv")
    parser.add_argument("--data", default="/dev/shm/nes-udf-bench")
    parser.add_argument("--scratch", default="/tmp/nes-udf-experiments")
    parser.add_argument("--environment", action="append", default=[], metavar="NAME=VALUE")
    parser.add_argument("--note", default="")
    arguments = parser.parse_args()
    if git("status", "--porcelain", "--untracked-files=no"):
        raise SystemExit("There are uncommitted changes. Commit them first, so that the recorded commit says what was measured.")

    statements = queries(arguments.test)
    results_path = os.path.join(HARNESS, "../results", arguments.experiment + ".jsonl")
    for interpreter in arguments.variants.split(","):
        variant_environment, variant_arguments = variant_settings(interpreter, arguments.gil_venv, arguments.ft_venv, arguments.protocol)
        for threads in map(int, arguments.threads.split(",")):
            run_dir = os.path.join(arguments.scratch, f"{arguments.experiment}-{interpreter}-w{threads}")
            shutil.rmtree(run_dir, ignore_errors=True)
            os.makedirs(run_dir + "/work")
            environment = dict(os.environ)
            environment.update(variant_environment)
            environment.update(setting.split("=", 1) for setting in arguments.environment)
            worker_arguments = [
                f"--worker.query_engine.number_of_worker_threads={threads}",
                f"--worker.default_query_execution.in_process_udf_calls={arguments.calls}",
                *variant_arguments,
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
            common = {
                "experiment": arguments.experiment,
                "test": os.path.relpath(arguments.test, REPOSITORY),
                "commit": git("rev-parse", "HEAD"),
                "started": started,
                "command": command,
                "environment": arguments.environment + [f"{name}={value}" for name, value in variant_environment.items()],
                "interpreter": interpreter,
                "threads": threads,
                "calls": arguments.calls,
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
            with open(results_path, "a") as results:
                for record in records:
                    results.write(json.dumps(record) + "\n")
            for record in records:
                print(f"{interpreter:15} w{threads:<3} q{record['query']} {record['seconds']:8.2f} s {record['mrows_per_s']:8.2f} M rows/s"
                      f"  {record['statement'][:70]}", flush=True)
            if len(records) != len(statements):
                print(f"{interpreter} w{threads}: {len(records)} results of {len(statements)} queries, return code {return_code}, "
                      f"see {run_dir}/out.txt", flush=True)


if __name__ == "__main__":
    main()
