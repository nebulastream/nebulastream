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
"""Runs the UDFBench scalar UDFs in Babelfish (the port to GraalVM 25, branch graalvm-25 of TU-Berlin-DIMA/babelfish),
without NebulaStream, on the same tiny data as udfbench_runner.py (E78, E79).

For each UDF, Babelfish's UdfBenchRunner loads the argument columns into a Babelfish table, runs the unchanged UDFBench
file as a Python scalar UDF (GraalPy), collects the results of the first round, warms up and times 3 rounds. A row with a
NULL argument is not loaded, because the engine gives NULL for it without a call; best_ns_per_row counts the loaded rows,
best_ns_per_all_rows all rows of the file (as the runner of E78/E79 does). The script compares the results with CPython
running the unchanged UDF and appends one JSON line per UDF to results/<experiment>.jsonl.

Needs clean working trees here and in Babelfish, so that the recorded commits say what was measured.
With --graal-plugins, Babelfish's Graal compiler plugins are loaded (E82).
Usage: run.py --experiment E81-babelfish --babelfish DIR --java GRAALVM/bin/java --scratch DIR [--graal-plugins]
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

JVM_OPTIONS = ["--sun-misc-unsafe-memory-access=allow", "--add-opens=java.base/java.nio=ALL-UNNAMED", "--enable-native-access=ALL-UNNAMED"]
# addnoise draws random numbers, so its results are not compared.
RANDOM = {"addnoise"}


def graal_plugin_options(babelfish):
    """JVM options that load graal-plugins: an automatic module on the module path (Graal loads its services from the
    boot layer only), the packages of graal-exports.txt exported to it, the Java-based Graal compiler (libgraal cannot
    load plugins) and the compiler configuration luth."""
    exports = [line.strip().replace("=ALL-UNNAMED", "=babelfish.graal.plugins")
               for line in (babelfish / "graal-exports.txt").read_text().splitlines() if line.strip()]
    return [f"--module-path={babelfish / 'graal-plugins/target/graal-plugins-1.0-SNAPSHOT.jar'}", "--add-modules=babelfish.graal.plugins",
            "-XX:+UnlockExperimentalVMOptions", "-XX:+EnableJVMCI", "-XX:-UseJVMCINativeLibrary",
            "-Djdk.graal.CompilerConfiguration=luth", *sorted(set(exports))]


def git(directory, *arguments):
    return subprocess.run(["git", "-C", str(directory), *arguments], capture_output=True, text=True, check=True).stdout.strip()


def reference(name):
    """CPython's results of the unchanged UDF for the rows without a NULL argument, as text lines."""
    function = udfs.function(name)
    lines = []
    for row in udfs.inputs(name):
        if any(value is None for value in row):
            continue
        result = function(None, *row)
        lines.append("NULL" if result is None else str(result))
    return lines


def same(expected, actual):
    if expected == actual:
        return True
    try:
        a, b = float(expected), float(actual)
    except ValueError:
        return False
    return a == b or (math.isnan(a) and math.isnan(b))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--experiment", required=True)
    parser.add_argument("--babelfish", required=True, help="the Babelfish checkout, built with mvn install")
    parser.add_argument("--java", required=True, help="java of the GraalVM 25 JDK")
    parser.add_argument("--udfs", help="comma-separated UDF names (default: all)")
    parser.add_argument("--rounds", type=int, default=3, help="timed rounds after the warm-up")
    parser.add_argument("--warmup-seconds", type=float, default=20)
    parser.add_argument("--timeout", type=float, default=900, help="seconds per UDF")
    parser.add_argument("--scratch", required=True, help="directory for results and the importer's working files")
    parser.add_argument("--graal-plugins", action="store_true",
                        help="load Babelfish's Graal compiler plugins (module graal-plugins, Java-based Graal compiler)")
    arguments = parser.parse_args()
    babelfish = pathlib.Path(arguments.babelfish)
    for directory in (REPOSITORY, babelfish):
        if git(directory, "status", "--porcelain", "--untracked-files=no"):
            raise SystemExit(f"There are uncommitted changes in {directory}. Commit them first.")

    commit, babelfish_commit = git(REPOSITORY, "rev-parse", "HEAD"), git(babelfish, "rev-parse", "HEAD")
    version = subprocess.run([arguments.java, "-version"], capture_output=True, text=True).stderr.splitlines()[1]
    jars = [line for line in (babelfish / "benchmark/target/classpath.txt").read_text().split(":") if line.endswith(".jar")]
    classpath = ":".join([str(babelfish / "benchmark/target/classes"), *jars])
    jvm_options = list(JVM_OPTIONS)
    if arguments.graal_plugins:
        jvm_options += graal_plugin_options(babelfish)
    scratch = pathlib.Path(arguments.scratch)
    scratch.mkdir(parents=True, exist_ok=True)
    results = HERE.parent / "results" / (arguments.experiment + ".jsonl")
    names = arguments.udfs.split(",") if arguments.udfs else sorted(udfs.UDFS)
    for name in names:
        output = scratch / f"{name}.out"
        command = [arguments.java, *jvm_options, "-cp", classpath, "de.tub.dima.babelfish.port.UdfBenchRunner", str(udfs.DATA),
                   str(REPOSITORY / "nes-systests/udf/pyudfs"), name, str(output), str(arguments.rounds), str(arguments.warmup_seconds)]
        started = time.monotonic()
        try:
            completed = subprocess.run(command, capture_output=True, text=True, cwd=scratch, timeout=arguments.timeout)
            return_code, stdout, stderr = completed.returncode, completed.stdout, completed.stderr
        except subprocess.TimeoutExpired as expired:
            return_code, stdout, stderr = "timeout", expired.stdout or "", expired.stderr or ""
            stdout = stdout.decode() if isinstance(stdout, bytes) else stdout
            stderr = stderr.decode() if isinstance(stderr, bytes) else stderr
        lines = [line for line in stdout.splitlines() if line.startswith('{"udf"')]
        record = {"experiment": arguments.experiment, "commit": commit, "babelfish_commit": babelfish_commit, "udf": name,
                  "source": udfs.UDFS[name][0], "compiler": "Babelfish", "graal_plugins": arguments.graal_plugins, "java": version,
                  "return_code": return_code,
                  "wall_s": round(time.monotonic() - started, 1), "command": command}
        if lines:
            record |= json.loads(lines[-1])
        else:
            errors = [line for line in (stdout + "\n" + stderr).splitlines() if "Error" in line or "Exception" in line]
            record["error"] = "\n".join(errors[:5])[-1500:] or (stdout + stderr)[-1500:]
        if lines and name not in RANDOM:
            expected = reference(name)
            actual = output.read_text(encoding="utf-8", errors="surrogateescape").split("\n")[:-1]
            differing = [i for i, (e, a) in enumerate(zip(expected, actual)) if not same(e, a)]
            record["same_results_as"] = {"CPython": len(expected) == len(actual) and not differing}
            record["differing_rows"] = len(differing) + abs(len(expected) - len(actual))
            record["differing_examples"] = [{"row": i, "cpython": expected[i][:200], "babelfish": actual[i][:200]} for i in differing[:3]]
        with open(results, "a") as file:
            file.write(json.dumps(record, ensure_ascii=False) + "\n")
        best = record.get("best_ns_per_row")
        print(f"{name:18} {'best %10.1f ns/row' % best if best else 'rc %s' % return_code:24} "
              f"same as CPython: {record.get('same_results_as', {}).get('CPython', '-')} ({record.get('differing_rows', '-')} rows differ) "
              f"{record.get('error', '')[:150]}", flush=True)


if __name__ == "__main__":
    main()
